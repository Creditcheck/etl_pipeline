package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/html"
)

// Job represents a URL to fetch and its depth relative to the starting URL.
type Job struct {
	URL   string
	Depth int
}

// extractLinks is now a thin orchestrator:
// - parse base
// - tokenize HTML
// - for each <a>, extract href + class
// - resolve + filter to same-host http(s)
// - return absolute URLs
func extractLinks(htmlBody, baseURL, className string) []string {
	base, ok := parseBaseURL(baseURL)
	if !ok {
		return nil
	}

	tok := html.NewTokenizer(strings.NewReader(htmlBody))

	var out []string
	for {
		tt := tok.Next()
		if tt == html.ErrorToken {
			return out
		}

		// Only consider start/self-closing tags; ignore text/comments/end tags.
		if tt != html.StartTagToken && tt != html.SelfClosingTagToken {
			continue
		}

		// Only consider <a ...> tags.
		tag, ok := readAnchorToken(tok)
		if !ok {
			continue
		}

		// Pull href and check for required class token.
		href, hasClass := anchorHrefAndClass(tag, className)
		if !hasClass || href == "" {
			continue
		}

		// Resolve + enforce http(s) + same host. If acceptable, append.
		if abs, ok := resolveAndFilterSameHost(base, href); ok {
			out = append(out, abs)
		}
	}
}

// parseBaseURL parses and validates the base URL once.
// We require a non-empty Host so that "same-host" filtering is meaningful.
func parseBaseURL(baseURL string) (*url.URL, bool) {
	base, err := url.Parse(baseURL)
	if err != nil || base == nil || base.Host == "" {
		return nil, false
	}
	return base, true
}

// readAnchorToken returns the token if the tokenizer is currently on an <a> tag.
// Single purpose: "is this token an anchor?"
func readAnchorToken(tok *html.Tokenizer) (html.Token, bool) {
	t := tok.Token()
	if t.Data != "a" {
		return html.Token{}, false
	}
	return t, true
}

// anchorHrefAndClass extracts:
// - href attribute (trimmed)
// - whether the anchor contains className as an EXACT class token
//
// Single purpose: "read attributes and determine match".
func anchorHrefAndClass(a html.Token, className string) (href string, hasClass bool) {
	for _, attr := range a.Attr {
		switch attr.Key {
		case "href":
			// Keep the raw href for url.Parse; just trim whitespace.
			href = strings.TrimSpace(attr.Val)

		case "class":
			// HTML class attribute is a whitespace-separated list.
			// We match the requested class as an exact token.
			if classListContains(attr.Val, className) {
				hasClass = true
			}
		}
	}
	return href, hasClass
}

// classListContains checks whether classAttr (e.g. "a b  c") contains className as an exact token.
// Single purpose: "class token membership".
func classListContains(classAttr, className string) bool {
	if className == "" {
		return false
	}
	for _, c := range strings.Fields(classAttr) {
		if c == className {
			return true
		}
	}
	return false
}

// resolveAndFilterSameHost resolves href against base and enforces crawl constraints:
// - scheme must be http or https
// - host must exactly match base.Host (includes port if present)
// - fragments are stripped to reduce duplicate URLs
//
// Single purpose: "turn an href into an acceptable absolute URL".
func resolveAndFilterSameHost(base *url.URL, href string) (string, bool) {
	u, err := url.Parse(href)
	if err != nil {
		return "", false
	}

	resolved := base.ResolveReference(u)

	// Only actual web pages. This drops mailto:, javascript:, tel:, data:, etc.
	if resolved.Scheme != "http" && resolved.Scheme != "https" {
		return "", false
	}

	// Strict same-host enforcement.
	// NOTE: Host includes port if present. If you want to ignore port differences,
	// compare resolved.Hostname() instead.
	if resolved.Host != base.Host {
		return "", false
	}

	// Normalize: drop fragment (#section) so we don't crawl the same document repeatedly.
	resolved.Fragment = ""

	return resolved.String(), true
}

// readURLsFromFile loads URLs (one per line).
func readURLsFromFile(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var urls []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			urls = append(urls, line)
		}
	}
	return urls, scanner.Err()
}

// worker consumes jobs from `jobs`, fetches the page, prints the URL,
// extracts matching links, and emits them to `discovered`.
//
// Concurrency / accounting notes:
//   - This worker does NOT decide uniqueness; the dispatcher owns the visited-set.
//   - Every Job the worker receives corresponds to "one unit of work" already counted
//     in the global wg. Therefore the worker MUST call wg.Done() exactly once per job.
//   - For each newly discovered link, the worker increments wg BEFORE sending the Job
//     to discovered. That way, even if the dispatcher discards it (duplicate/too deep),
//     the dispatcher can balance it with wg.Done(). This eliminates races where wg hits
//     zero while discovered jobs are still in-flight.
func worker(
	jobs <-chan Job,
	discovered chan<- Job,
	client *http.Client,
	className string,
	maxDepth int,
	wg *sync.WaitGroup,
	single bool,
) {
	for job := range jobs {
		// Ensure the current job is always marked done, even on early returns/continues.
		func() {
			defer wg.Done()

			// Depth guard (defensive; dispatcher should already filter too).
			if job.Depth > maxDepth {
				return
			}

			resp, err := client.Get(job.URL)
			if err != nil {
				return
			}
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			if err != nil {
				return
			}

			// Print visited URL.
			fmt.Println(job.URL)

			// Discover next-level links.
			links := extractLinks(string(body), job.URL, className)
			if single {
				for _, link := range links {
					fmt.Println(link)
				}
				return
			}
			nextDepth := job.Depth + 1

			// If we've already reached maxDepth, we can skip emitting children.
			if nextDepth > maxDepth {
				return
			}

			for _, link := range links {
				if single {
					break
				}
				// IMPORTANT:
				// Count this new candidate BEFORE sending it to the dispatcher.
				// If we counted after, wg could reach zero prematurely.
				wg.Add(1)

				// This send can block if the discovered buffer is full.
				// We intentionally allow backpressure rather than dropping URLs.
				// Deadlock is avoided because the dispatcher never blocks forever
				// on sending to `jobs` (it uses an internal queue).
				discovered <- Job{URL: link, Depth: nextDepth}
			}
		}()
	}
}

// dispatcher is the single goroutine responsible for:
// - de-duplication (visited set)
// - filtering invalid/too-deep URLs
// - feeding accepted jobs into `jobs`
//
// Key design point: it maintains an internal FIFO queue so it can keep draining
// `discovered` even if `jobs` is temporarily full. This prevents cyclic backpressure:
// workers -> discovered -> dispatcher -> jobs -> workers.
//
// Accounting:
//   - Every Job arriving on `discovered` has already been wg.Add(1)'d by the producer.
//   - If dispatcher discards the job (duplicate/invalid/too deep), dispatcher calls wg.Done().
//   - If dispatcher accepts the job, it forwards it to `jobs` WITHOUT changing wg.
//     The worker will eventually call wg.Done() when it finishes processing that job.
func dispatcher(
	discovered <-chan Job,
	jobs chan<- Job,
	maxDepth int,
	wg *sync.WaitGroup,
	dispatcherDone chan<- struct{},
) {
	defer close(dispatcherDone)

	visited := make(map[string]struct{})
	var vmu sync.Mutex

	// Internal FIFO queue of accepted jobs waiting to be sent to workers.
	queue := make([]Job, 0, 1024)

	// Helper: validate + dedupe + enqueue.
	accept := func(job Job) {
		// Filter by depth.
		if job.Depth > maxDepth {
			// This job was counted by wg.Add(1) at the producer; we discard it here.
			wg.Done()
			return
		}

		// Parse/normalize URL. If invalid, discard.
		parsed, err := url.Parse(job.URL)
		if err != nil {
			wg.Done()
			return
		}
		normalized := parsed.String()

		// De-duplicate.
		vmu.Lock()
		_, seen := visited[normalized]
		if seen {
			vmu.Unlock()
			wg.Done()
			return
		}
		visited[normalized] = struct{}{}
		vmu.Unlock()

		// Replace URL with normalized form.
		job.URL = normalized

		// Accepted: queue it for sending to workers.
		// NOTE: wg is NOT modified here; the job is still "live" and will be completed by a worker.
		queue = append(queue, job)
	}

	// We need a way to stop cleanly:
	// The main goroutine will wait for wg.Wait() == 0, which means:
	// - No workers are processing jobs, AND
	// - No new candidates are pending in discovered, AND
	// - No queued/accepted jobs are outstanding (because those are also counted in wg).
	//
	// At that moment, main will close `stop`, and dispatcher will:
	// - close(jobs) to stop workers
	// - return
	stop := make(chan struct{})

	// A small goroutine that closes stop when wg reaches zero.
	// This avoids the dispatcher having to poll or reason about wg state.
	go func() {
		wg.Wait()
		close(stop)
	}()

	for {
		// If we have something to send, prepare the send case.
		var (
			nextJob Job
			out     chan<- Job
		)
		if len(queue) > 0 {
			nextJob = queue[0]
			out = jobs
		}

		select {
		case <-stop:
			// No outstanding work anywhere.
			// Safe to close jobs: no worker will attempt to send more discovered work
			// because that would have required wg > 0.
			close(jobs)
			return

		case job := <-discovered:
			// NOTE: discovered is never closed in this design.
			// We terminate based on wg reaching zero instead.
			accept(job)

		case out <- nextJob:
			// Successfully handed a job to a worker; pop from queue.
			queue[0] = Job{}  // help GC
			queue = queue[1:] // advance
		}
	}
}

func main() {
	// Command-line flags
	workers := flag.Int("n", 5, "number of concurrent workers")
	inputFile := flag.String("i", "", "path to file containing URLs (one per line)")
	startURL := flag.String("url", "", "single start URL")
	className := flag.String("class", "", "CSS class to search for in <a> tags")
	maxDepth := flag.Int("depth", 1, "maximum crawl depth")
	single := flag.Bool("single", false, "fetch only the start URL and exit")
	flag.Parse()

	// Validate inputs
	if *inputFile == "" && *startURL == "" {
		fmt.Fprintf(os.Stderr, "Error: either -i or -url must be specified\n")
		flag.Usage()
		os.Exit(1)
	}
	if *className == "" {
		fmt.Fprintf(os.Stderr, "Error: -class must be specified\n")
		flag.Usage()
		os.Exit(1)
	}

	// Gather start URLs
	var startURLs []string
	if *inputFile != "" {
		urls, err := readURLsFromFile(*inputFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error reading URLs from file: %v\n", err)
			os.Exit(1)
		}
		startURLs = urls
	} else {
		startURLs = []string{*startURL}
	}

	// HTTP client shared across workers (safe for concurrent use).
	client := &http.Client{Timeout: 10 * time.Second}

	// Channel carrying accepted work to workers.
	// Buffer gives some decoupling but is not relied on for correctness.
	jobs := make(chan Job, 1024)

	// Channel carrying newly discovered candidate jobs back to dispatcher.
	// Buffer helps reduce contention; correctness is still ensured by the dispatcher queue.
	discovered := make(chan Job, 4096)

	// Global WaitGroup tracks *all* work in the system:
	// - jobs waiting in dispatcher queue
	// - jobs being processed by workers
	// - newly discovered candidates in-flight to dispatcher
	//
	// Invariant: every wg.Add(1) is matched by exactly one wg.Done().
	var wg sync.WaitGroup

	// Start dispatcher (single owner of visited + enqueuing).
	dispatcherDone := make(chan struct{})
	go dispatcher(discovered, jobs, *maxDepth, &wg, dispatcherDone)

	// Start worker goroutines.
	for i := 0; i < *workers; i++ {
		go worker(jobs, discovered, client, *className, *maxDepth, &wg, *single)
	}

	// Seed initial URLs by sending them into the same discovered pipeline.
	// IMPORTANT: each seeded job must be counted first.
	for _, u := range startURLs {
		parsed, err := url.Parse(u)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Invalid start URL: %v\n", u)
			continue
		}

		wg.Add(1)
		discovered <- Job{URL: parsed.String(), Depth: 0}
	}

	// Wait for dispatcher to close jobs and exit.
	<-dispatcherDone

	// At this point, wg is zero (stop condition), jobs is closed, workers will naturally exit.
	// We intentionally do not close(discovered) because:
	// - There are no senders left once wg == 0 and jobs is closed.
	// - Closing discovered from multiple potential senders is dangerous.
	//
	// If you want to be extra explicit, you'd need a separate worker WaitGroup and then close it
	// after all workers exit; but it's not required for correctness here.

	fmt.Fprintln(os.Stderr)
}
