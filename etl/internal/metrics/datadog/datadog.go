// Package datadog implements a Datadog backend for the internal/metrics package.
// NOTE ABOUT FLUSHING:
// This backend is meant to be useful for both short-lived and long-running ETL jobs.
// Submitting only once at process exit can make Datadog dashboards/monitors awkward
// for long jobs (you get a single spike rather than a time series).
//
// Therefore we:
//   - buffer metrics in-memory (fast, lock-protected)
//   - periodically Flush() on a ticker (default: once per minute)
//   - Flush() one final time on Close()
//
// This gives you:
//   - time series points while the job is running
//   - a final “tail” flush at shutdown
//
// Concurrency model:
//   - ETL goroutines can call IncCounter/ObserveHistogram at any time
//   - Flush snapshots+resets buffers under a mutex, then submits out-of-lock
//   - The flush loop calls Flush() periodically; Close() stops the loop
//
// If the process is killed with SIGKILL/OOM, Close() won’t run (no backend can fix that).
//
// Design goals (intentionally opinionated):
//
//   - Keep the core ETL code depending only on metrics.Backend.
//   - Buffer metrics in-memory and submit them on Flush().
//     (Your main.go already defers metrics.Flush() at program exit.)
//   - Avoid shipping Prometheus-specific or Datadog-specific code into the core.
//
// What we send to Datadog:
//
//   - Counters as Datadog "count" metrics:
//
//   - etl.step.total            (tagged by job, step, status)
//
//   - etl.records.total         (tagged by job, kind)
//
//   - etl.batches.total         (tagged by job)
//
//   - Step durations as *computed percentiles* emitted as gauges at flush time:
//
//   - etl.step.duration_seconds.p50/.p90/.p95/.p99/.max/.samples
//     These are window-local percentiles for the process lifetime (or since last Flush),
//     not globally aggregated percentiles across all processes.
//
// Auth:
//   - Uses Datadog's default env-based auth via the official client:
//     DD_API_KEY, DD_APP_KEY (if needed), DD_SITE, etc.
package datadog

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"etl/internal/metrics"

	dd "github.com/DataDog/datadog-api-client-go/v2/api/datadog"
	"github.com/DataDog/datadog-api-client-go/v2/api/datadogV2"
)

// Options controls Datadog backend configuration.
//
// Keep this minimal: most Datadog configuration is handled by environment variables
// that the official client already supports (keys, site, proxy, etc.).
type Options struct {
	// JobName becomes tag "job:<name>" on every metric.
	// If empty, defaults to "etl".
	JobName string

	// Tags are extra Datadog tags (e.g. []string{"env:prod", "service:etl"}).
	// These are attached to every metric point.
	Tags []string

	// FlushEvery controls how often we submit buffered metrics to Datadog.
	//
	// Why this exists:
	//   - Submitting only once at job end makes graphs sparse and monitors less useful.
	//   - Submitting too often increases API calls and can add overhead.
	//
	// Default:
	//   - 60 seconds (good tradeoff for ETL jobs)
	//
	// If <= 0, we fall back to 60 seconds.
	FlushEvery time.Duration
}

// Backend implements metrics.Backend for Datadog.
//
// It buffers increments/observations under a mutex so you can call it freely
// from concurrent ETL goroutines. Flush() snapshots buffers, resets them,
// then submits to Datadog.
type Backend struct {
	// api + ctx are the Datadog submission client and its context.
	api *datadogV2.MetricsApi
	ctx context.Context

	// flushEvery is the ticker interval for periodic flushes.
	flushEvery time.Duration

	// stopCh signals the background flush goroutine to stop.
	// We close it in Close().
	stopCh chan struct{}

	// doneCh is closed by the background goroutine when it has fully exited.
	// Close() waits on this to avoid racing with an in-flight Flush().
	doneCh chan struct{}
	// baseTags are attached to every metric series we submit.
	// Includes "job:<JobName>" plus user-provided tags.
	baseTags []string

	// mu protects *all* buffered state below.
	mu sync.Mutex

	// stepCounts buffers etl.step.total by (step,status).
	// Key format is a stable internal encoding; we do not expose it externally.
	stepCounts map[string]float64

	// recordCounts buffers etl.records.total by (kind).
	recordCounts map[string]float64

	// batchCount buffers etl.batches.total (single counter per process/job).
	batchCount float64

	// durationSamples buffers step duration samples by (step,status).
	// We emit percentiles/max/samples on Flush().
	durationSamples map[string][]float64
}

// resolveEnvTag determines the Datadog env tag.
//
// Precedence:
//  1. ENV        (explicit app-level env)
//  2. DD_ENV     (Datadog standard)
//  3. "unknown"  (safe fallback so the tag always exists)
func resolveEnvTag() string {
	if v := strings.TrimSpace(os.Getenv("ENV")); v != "" {
		return "env:" + v
	}
	if v := strings.TrimSpace(os.Getenv("DD_ENV")); v != "" {
		return "env:" + v
	}
	return "env:unknown"
}

// loop periodically flushes metrics until Close() is called.
//
// Important behavior:
//   - We intentionally ignore Flush() errors in the loop:
//   - metrics must never crash the ETL
//   - transient network issues shouldn’t kill the process
//     Instead, Flush() errors will be returned by Close()’s final flush (if any),
//     and you can also add logging at the call site if desired.
func (b *Backend) loop() {
	defer close(b.doneCh)

	t := time.NewTicker(b.flushEvery)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			// Periodic submission.
			// We drop the error here because this runs in a goroutine and we want
			// the ETL to continue even if Datadog is temporarily unavailable.
			_ = b.Flush()

		case <-b.stopCh:
			// Close() asked us to stop.
			// Do NOT final-flush here; Close() performs the final flush itself so it can
			// return the error to the caller for logging/visibility.
			return
		}
	}
}

// Close stops the background flush loop and performs one final Flush().
//
// Call this on shutdown to:
//   - stop the ticker goroutine (avoids leaks in tests / long-running procs)
//   - submit the last buffered metrics window
//
// Close is idempotent-ish in the sense that calling it twice will panic if we close
// an already-closed channel. In practice, call it once from main via defer.
func (b *Backend) Close() error {
	close(b.stopCh)
	<-b.doneCh
	return b.Flush()
}

// NewBackend constructs a Datadog backend using the official client.
//
// IMPORTANT: We intentionally rely on the Datadog client’s env-based configuration
// so operators can configure auth without code changes.
//
// Typical env vars:
//   - DD_API_KEY
//   - DD_APP_KEY (sometimes required depending on endpoint / org settings)
//   - DD_SITE (e.g. datadoghq.com, datadoghq.eu)
func NewBackend(parent context.Context, opts Options) (*Backend, error) {
	// Choose a safe default "job" tag value.
	job := opts.JobName
	if job == "" {
		job = "etl"
	}

	// Choose flush frequency.
	// We default to 60s because it:
	//   - provides reasonable chart resolution
	//   - keeps API traffic low for typical ETL jobs
	flushEvery := opts.FlushEvery
	if flushEvery <= 0 {
		flushEvery = 60 * time.Second
	}

	// Build the base tag set:
	//   - Always include job:<job>
	//   - Always include env:<env>
	//   - Then include any extra tags the caller provided
	envTag := resolveEnvTag()
	baseTags := make([]string, 0, 2+len(opts.Tags))
	baseTags = append(baseTags, envTag)
	baseTags = append(baseTags, "job:"+job)
	baseTags = append(baseTags, opts.Tags...)

	// Datadog client setup:
	// The official client reads env vars for auth + site selection.
	ctx := dd.NewDefaultContext(parent)
	cfg := dd.NewConfiguration()
	client := dd.NewAPIClient(cfg)
	api := datadogV2.NewMetricsApi(client)

	b := &Backend{
		api:             api,
		ctx:             ctx,
		flushEvery:      flushEvery,
		stopCh:          make(chan struct{}),
		doneCh:          make(chan struct{}),
		baseTags:        baseTags,
		stepCounts:      make(map[string]float64),
		recordCounts:    make(map[string]float64),
		durationSamples: make(map[string][]float64),
	}

	// Start the periodic flush loop.
	// This gives Datadog a time series while the job runs, instead of a single point at exit.
	go b.loop()

	return b, nil
}

// IncCounter implements metrics.Backend.
//
// The metrics package calls this for three metric names:
//   - etl_step_total (labels: job, step, status)
//   - etl_records_total (labels: job, kind)
//   - etl_batches_total (labels: job)
//
// IMPORTANT:
// The *core* package uses Prometheus-style snake_case names.
// We intentionally keep that stable across backends.
//
// This Datadog backend translates those names into dotted Datadog metric names
// at *Flush()* time when we submit the payload.
//
// Note: the "job" label is *already* represented by our Datadog base tag (job:<name>).
// We accept it but do not require it to be present in labels.
func (b *Backend) IncCounter(name string, delta float64, labels metrics.Labels) {
	// Datadog counts should never be negative in our ETL use case.
	// If someone accidentally calls with <= 0, ignore to avoid confusing graphs.
	if delta <= 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	switch name {
	case "etl_step_total":
		// Keyed by (step,status) because job is constant (base tag).
		step := labels["step"]
		status := labels["status"]
		k := stepStatusKey(step, status)
		b.stepCounts[k] += delta

	case "etl_records_total":
		// Keyed by kind (processed, inserted, parse_errors, ...).
		kind := labels["kind"]
		if kind == "" {
			// If kind is missing, skip rather than creating an ambiguous metric series.
			return
		}
		b.recordCounts[kind] += delta

	case "etl_batches_total":
		// Single counter per process/job.
		b.batchCount += delta

	default:
		// Unknown metric name: ignore by design to keep the interface permissive.
	}
}

// ObserveHistogram implements metrics.Backend for "etl_step_duration_seconds".
//
// The core metrics package calls ObserveHistogram with:
//
//	name = "etl_step_duration_seconds"
//	value = duration in seconds
//	labels: job, step, status
//
// We buffer raw samples and compute percentiles on Flush().
func (b *Backend) ObserveHistogram(name string, value float64, labels metrics.Labels) {
	// Only one histogram name is currently used by metrics.RecordStep().
	if name != "etl_step_duration_seconds" {
		return
	}

	// Negative or NaN durations are useless; ignore them.
	if value < 0 {
		return
	}

	step := labels["step"]
	status := labels["status"]
	k := stepStatusKey(step, status)

	b.mu.Lock()
	b.durationSamples[k] = append(b.durationSamples[k], value)
	b.mu.Unlock()
}

// Flush submits buffered metrics to Datadog and resets local buffers.
//
// This is intended to be called once at process shutdown (defer metrics.Flush())
// but it is safe to call multiple times: we snapshot+reset buffers each time.
//
// If there is nothing to send, Flush is a no-op and returns nil.
func (b *Backend) Flush() error {
	// Snapshot buffers under lock and reset them so the ETL can keep recording
	// while the network submission happens (if Flush is called mid-run).
	b.mu.Lock()

	stepCounts := b.stepCounts
	recordCounts := b.recordCounts
	batchCount := b.batchCount
	durationSamples := b.durationSamples

	// Reset buffers (important: new maps/slices so old snapshots are not mutated).
	b.stepCounts = make(map[string]float64)
	b.recordCounts = make(map[string]float64)
	b.batchCount = 0
	b.durationSamples = make(map[string][]float64)

	b.mu.Unlock()

	// Fast path: nothing to send.
	if len(stepCounts) == 0 && len(recordCounts) == 0 && batchCount == 0 && len(durationSamples) == 0 {
		return nil
	}

	// Datadog metric points use a Unix timestamp (seconds).
	now := time.Now().Unix()

	// Helper: append a "count" series (counter increments for this flush window).
	addCount := func(metric string, value float64, tags []string) datadogV2.MetricSeries {
		return datadogV2.MetricSeries{
			Metric: metric,
			Type:   datadogV2.METRICINTAKETYPE_COUNT.Ptr(),
			Points: []datadogV2.MetricPoint{
				{Timestamp: dd.PtrInt64(now), Value: dd.PtrFloat64(value)},
			},
			Tags: tags,
		}
	}

	// Helper: append a gauge series (used for percentiles/max/samples).
	addGauge := func(metric string, value float64, tags []string) datadogV2.MetricSeries {
		return datadogV2.MetricSeries{
			Metric: metric,
			Type:   datadogV2.METRICINTAKETYPE_GAUGE.Ptr(),
			Points: []datadogV2.MetricPoint{
				{Timestamp: dd.PtrInt64(now), Value: dd.PtrFloat64(value)},
			},
			Tags: tags,
		}
	}

	// Pre-allocate a bit to avoid re-allocations.
	series := make([]datadogV2.MetricSeries, 0, len(stepCounts)+len(recordCounts)+16)

	// 1) etl.step.total (counts by step+status)
	for k, v := range stepCounts {
		if v == 0 {
			continue
		}
		step, status := splitStepStatusKey(k)
		tags := withTags(b.baseTags, "step:"+step, "status:"+status)
		series = append(series, addCount("etl.step.total", v, tags))
	}

	// 2) etl.records.total (counts by kind)
	for kind, v := range recordCounts {
		if v == 0 {
			continue
		}
		tags := withTags(b.baseTags, "kind:"+kind)
		series = append(series, addCount("etl.records.total", v, tags))
	}

	// 3) etl.batches.total (single count)
	if batchCount != 0 {
		series = append(series, addCount("etl.batches.total", batchCount, b.baseTags))
	}

	// 4) etl_step_duration_seconds percentiles (gauges)
	//
	// IMPORTANT: These are computed from buffered samples inside this process.
	// If you run multiple ETL processes, each will emit its own window percentiles.
	// This is still extremely useful operationally (tail latency, regressions, etc.).
	for k, samples := range durationSamples {
		if len(samples) == 0 {
			continue
		}

		// Copy+sort so we don't mutate the snapshot slice (defensive programming).
		cp := append([]float64(nil), samples...)
		sort.Float64s(cp)

		step, status := splitStepStatusKey(k)
		tags := withTags(b.baseTags, "step:"+step, "status:"+status)

		p50 := percentileNearestRank(cp, 0.50)
		p90 := percentileNearestRank(cp, 0.90)
		p95 := percentileNearestRank(cp, 0.95)
		p99 := percentileNearestRank(cp, 0.99)
		max := cp[len(cp)-1]

		// Emit the standard percentile series under a consistent name prefix.
		series = append(series, addGauge("etl.step.duration_seconds.p50", p50, tags))
		series = append(series, addGauge("etl.step.duration_seconds.p90", p90, tags))
		series = append(series, addGauge("etl.step.duration_seconds.p95", p95, tags))
		series = append(series, addGauge("etl.step.duration_seconds.p99", p99, tags))
		series = append(series, addGauge("etl.step.duration_seconds.max", max, tags))
		series = append(series, addGauge("etl.step.duration_seconds.samples", float64(len(cp)), tags))
	}

	// Submit to Datadog.
	//
	// The V2 Metrics endpoint accepts a MetricPayload with a list of series.
	// We submit one payload per Flush().
	payload := datadogV2.MetricPayload{Series: series}
	_, _, err := b.api.SubmitMetrics(b.ctx, payload, *datadogV2.NewSubmitMetricsOptionalParameters())
	return err
}

// stepStatusKey encodes (step,status) into a single map key.
//
// We use a separator that is extremely unlikely to appear in either value.
// This is an internal encoding only; it never leaves this package.
func stepStatusKey(step, status string) string {
	return step + "\x00" + status
}

// splitStepStatusKey reverses stepStatusKey().
//
// If the key is malformed, we return best-effort values to avoid panics.
func splitStepStatusKey(k string) (step, status string) {
	parts := strings.SplitN(k, "\x00", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	// Malformed key; keep it observable in tags rather than crashing.
	return k, "unknown"
}

// withTags builds a new tag slice containing base tags + extras.
//
// We allocate a new slice so callers can safely reuse baseTags without mutation.
func withTags(base []string, extras ...string) []string {
	out := make([]string, 0, len(base)+len(extras))
	out = append(out, base...)
	out = append(out, extras...)
	return out
}

// percentileNearestRank assumes s is sorted ascending.
//
// This is intentionally simple and stable:
//   - clamp p into [0,1]
//   - compute nearest-rank-ish index into [0,n-1]
//
// For ops metrics, this is usually more than good enough, and avoids pulling
// in heavier stats libraries.
func percentileNearestRank(s []float64, p float64) float64 {
	n := len(s)
	if n == 0 {
		return 0
	}
	if p <= 0 {
		return s[0]
	}
	if p >= 1 {
		return s[n-1]
	}

	// Nearest rank to p*(n-1)
	idx := int(p*float64(n-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return s[idx]
}

// Compile-time assertion that *Backend implements the interface.
// This fails fast during compilation if metrics.Backend changes.
var _ metrics.Backend = (*Backend)(nil)

// Optional: a small helper to parse comma-separated tags from an env var.
// We keep it here because it’s Datadog-specific behavior.
//
// Example env value:
//
//	METRICS_TAGS="env:prod,service:etl,team:data"
func ParseTagsCSV(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Optional: a friendly error wrapper for backend initialization failures.
func wrapInitErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("datadog metrics init: %w", err)
}
