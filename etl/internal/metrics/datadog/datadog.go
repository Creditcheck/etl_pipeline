// Package datadog implements a Datadog backend for the internal/metrics package.
//
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
//   - Avoid shipping Prometheus-specific or Datadog-specific code into the core.
package datadog

import (
	"context"
	"fmt"
	"net/http"
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
type Options struct {
	// JobName becomes tag "job:<name>" on every metric.
	// If empty, defaults to "etl".
	JobName string

	// Tags are extra Datadog tags (e.g. []string{"env:prod", "service:etl"}).
	Tags []string

	// FlushEvery controls how often we submit buffered metrics to Datadog.
	// If <= 0, defaults to 60 seconds.
	FlushEvery time.Duration

	// The following fields are unexported test seams.
	//
	// They are intentionally kept private to preserve the public API surface.
	// Production code will never set them; unit tests can set them to avoid:
	//   - real network submission
	//   - nondeterministic clocks/tickers
	//
	// NOTE: Being unexported means this change is backwards-compatible for all
	// external callers, but enables deterministic unit tests in this package.
	now       func() time.Time
	newTicker func(d time.Duration) *time.Ticker
	submitter metricsSubmitter
}

// metricsSubmitter is the minimal interface needed to submit metrics.
//
// Why this exists:
//   - The Datadog SDK exposes a concrete *datadogV2.MetricsApi, which makes unit
//     testing difficult (we cannot stub it without doing real HTTP).
//   - Backend depends on this interface instead of the concrete type, enabling
//     deterministic tests with a fake submitter.
//
// This interface is intentionally tiny and private to keep refactors low-risk.
type metricsSubmitter interface {
	SubmitMetrics(ctx context.Context, body datadogV2.MetricPayload, params ...datadogV2.SubmitMetricsOptionalParameters) (datadogV2.IntakePayloadAccepted, *http.Response, error)
}

// Backend implements metrics.Backend for Datadog.
type Backend struct {
	api metricsSubmitter
	ctx context.Context

	flushEvery time.Duration
	stopCh     chan struct{}
	doneCh     chan struct{}

	baseTags []string

	// now is injected for deterministic tests. Production uses time.Now.
	now func() time.Time

	// newTicker is injected for deterministic tests. Production uses time.NewTicker.
	newTicker func(d time.Duration) *time.Ticker

	mu sync.Mutex

	stepCounts      map[string]float64
	recordCounts    map[string]float64
	batchCount      float64
	durationSamples map[string][]float64

	// HTTP metrics (crawler/extractor oriented).
	httpReqCounts map[string]float64 // status -> count
	httpErrCounts map[string]float64 // status -> count
	httpReqDur    map[string][]float64
	httpRespDur   map[string][]float64
	httpDownloadB map[string][]float64
}

func resolveEnvTag() string {
	if v := strings.TrimSpace(os.Getenv("ENV")); v != "" {
		return "env:" + v
	}
	if v := strings.TrimSpace(os.Getenv("DD_ENV")); v != "" {
		return "env:" + v
	}
	return "env:unknown"
}

func (b *Backend) loop() {
	defer close(b.doneCh)

	// newTicker is a seam to allow tests to run with very small tick durations
	// while still keeping the production behavior identical.
	t := b.newTicker(b.flushEvery)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			_ = b.Flush()
		case <-b.stopCh:
			return
		}
	}
}

// Close stops the background flush loop and performs one final Flush().
//
// Errors:
//   - Returns any error from the final Flush() submission.
//   - If Close is called multiple times, the behavior is undefined (it will panic
//     because stopCh is closed twice). This mirrors typical Go "Close once"
//     semantics and is acceptable for process-lifetime backends.
func (b *Backend) Close() error {
	close(b.stopCh)
	<-b.doneCh
	return b.Flush()
}

// NewBackend constructs a Datadog backend using the official client.
//
// When to use:
//   - Configure this backend when you want Datadog metrics for ETL runs.
//   - Suitable for both long-running pipelines (periodic flush) and short-lived
//     commands (final flush on Close).
//
// Edge cases:
//   - If opts.FlushEvery <= 0, defaults to 60s.
//   - If opts.JobName is empty, defaults to "etl".
//   - Environment tag selection uses ENV then DD_ENV, otherwise env:unknown.
//
// Errors:
//   - Returns an error only if internal initialization fails.
//   - Datadog client construction itself is not expected to fail under normal
//     conditions; network errors occur during Flush().
func NewBackend(parent context.Context, opts Options) (*Backend, error) {
	job := opts.JobName
	if job == "" {
		job = "etl"
	}

	flushEvery := opts.FlushEvery
	if flushEvery <= 0 {
		flushEvery = 60 * time.Second
	}

	envTag := resolveEnvTag()
	baseTags := make([]string, 0, 2+len(opts.Tags))
	baseTags = append(baseTags, envTag, "job:"+job)
	baseTags = append(baseTags, opts.Tags...)

	// Clock / ticker seams.
	nowFn := opts.now
	if nowFn == nil {
		nowFn = time.Now
	}
	newTicker := opts.newTicker
	if newTicker == nil {
		newTicker = time.NewTicker
	}

	// Submitter seam.
	submitter := opts.submitter
	if submitter == nil {
		ctx := dd.NewDefaultContext(parent)
		cfg := dd.NewConfiguration()
		client := dd.NewAPIClient(cfg)
		api := datadogV2.NewMetricsApi(client)

		// Wrap the concrete API in our tiny interface.
		submitter = api
		_ = ctx // ctx stored below
	}

	ctx := dd.NewDefaultContext(parent)

	b := &Backend{
		api:        submitter,
		ctx:        ctx,
		flushEvery: flushEvery,
		stopCh:     make(chan struct{}),
		doneCh:     make(chan struct{}),

		baseTags: baseTags,

		now:       nowFn,
		newTicker: newTicker,

		stepCounts:      make(map[string]float64),
		recordCounts:    make(map[string]float64),
		durationSamples: make(map[string][]float64),

		httpReqCounts: make(map[string]float64),
		httpErrCounts: make(map[string]float64),
		httpReqDur:    make(map[string][]float64),
		httpRespDur:   make(map[string][]float64),
		httpDownloadB: make(map[string][]float64),
	}

	go b.loop()
	return b, nil
}

// IncCounter implements metrics.Backend.
func (b *Backend) IncCounter(name string, delta float64, labels metrics.Labels) {
	if delta <= 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	switch name {
	case "etl_step_total":
		step := labels["step"]
		status := labels["status"]
		k := stepStatusKey(step, status)
		b.stepCounts[k] += delta

	case "etl_records_total":
		kind := labels["kind"]
		if kind == "" {
			return
		}
		b.recordCounts[kind] += delta

	case "etl_batches_total":
		b.batchCount += delta

	case "etl_http_requests_total":
		status := labels["status"]
		if status == "" {
			status = "unknown"
		}
		b.httpReqCounts[status] += delta

	case "etl_http_errors_total":
		status := labels["status"]
		if status == "" {
			status = "unknown"
		}
		b.httpErrCounts[status] += delta

	default:
		// Ignore unknown metrics by design.
	}
}

// ObserveHistogram implements metrics.Backend.
func (b *Backend) ObserveHistogram(name string, value float64, labels metrics.Labels) {
	if value < 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	switch name {
	case "etl_step_duration_seconds":
		step := labels["step"]
		status := labels["status"]
		k := stepStatusKey(step, status)
		b.durationSamples[k] = append(b.durationSamples[k], value)

	case "etl_http_request_duration_seconds":
		status := labels["status"]
		if status == "" {
			status = "unknown"
		}
		b.httpReqDur[status] = append(b.httpReqDur[status], value)

	case "etl_http_response_duration_seconds":
		status := labels["status"]
		if status == "" {
			status = "unknown"
		}
		b.httpRespDur[status] = append(b.httpRespDur[status], value)

	case "etl_http_download_bytes":
		status := labels["status"]
		if status == "" {
			status = "unknown"
		}
		b.httpDownloadB[status] = append(b.httpDownloadB[status], value)

	default:
		// Ignore unknown histograms by design.
	}
}

// snapshot is the immutable set of buffered metric state used to build a flush payload.
//
// Why this exists:
//   - Flush() must reset buffers under a lock, but must submit out-of-lock.
//   - snapshot allows a clean separation between (1) collect+reset and
//     (2) payload building+submission.
//
// This structure is intentionally simple (maps and slices) to avoid adding
// conversion overhead to the hot path.
type snapshot struct {
	stepCounts      map[string]float64
	recordCounts    map[string]float64
	batchCount      float64
	durationSamples map[string][]float64

	httpReqCounts map[string]float64
	httpErrCounts map[string]float64
	httpReqDur    map[string][]float64
	httpRespDur   map[string][]float64
	httpDownloadB map[string][]float64
}

// snapshotAndReset grabs current buffered metrics and resets internal buffers.
//
// Concurrency:
//   - Must be called with no lock held.
//   - Takes the lock internally and returns detached maps/slices.
func (b *Backend) snapshotAndReset() snapshot {
	b.mu.Lock()
	defer b.mu.Unlock()

	s := snapshot{
		stepCounts:      b.stepCounts,
		recordCounts:    b.recordCounts,
		batchCount:      b.batchCount,
		durationSamples: b.durationSamples,

		httpReqCounts: b.httpReqCounts,
		httpErrCounts: b.httpErrCounts,
		httpReqDur:    b.httpReqDur,
		httpRespDur:   b.httpRespDur,
		httpDownloadB: b.httpDownloadB,
	}

	// Reset buffers for the next collection window.
	b.stepCounts = make(map[string]float64)
	b.recordCounts = make(map[string]float64)
	b.batchCount = 0
	b.durationSamples = make(map[string][]float64)

	b.httpReqCounts = make(map[string]float64)
	b.httpErrCounts = make(map[string]float64)
	b.httpReqDur = make(map[string][]float64)
	b.httpRespDur = make(map[string][]float64)
	b.httpDownloadB = make(map[string][]float64)

	return s
}

// isEmpty returns true if the snapshot contains no data to submit.
func (s snapshot) isEmpty() bool {
	return len(s.stepCounts) == 0 &&
		len(s.recordCounts) == 0 &&
		s.batchCount == 0 &&
		len(s.durationSamples) == 0 &&
		len(s.httpReqCounts) == 0 &&
		len(s.httpErrCounts) == 0 &&
		len(s.httpReqDur) == 0 &&
		len(s.httpRespDur) == 0 &&
		len(s.httpDownloadB) == 0
}

// Flush submits buffered metrics to Datadog and resets local buffers.
//
// Errors:
//   - Returns any error from Datadog submission.
//   - Returns nil if there is nothing to submit.
//
// Edge cases:
//   - Flush is safe to call concurrently with IncCounter/ObserveHistogram.
//   - Flush resets buffers even if submission fails (by design, to keep the ETL
//     fast and avoid blocking future writes). If you need "at least once" delivery,
//     that is a different architecture.
func (b *Backend) Flush() error {
	snap := b.snapshotAndReset()
	if snap.isEmpty() {
		return nil
	}

	nowUnix := b.now().Unix()

	series := b.buildSeries(snap, nowUnix)
	payload := datadogV2.MetricPayload{Series: series}

	_, _, err := b.api.SubmitMetrics(b.ctx, payload, *datadogV2.NewSubmitMetricsOptionalParameters())
	return err
}

// buildSeries constructs Datadog series for a snapshot at a fixed timestamp.
//
// Why this exists:
//   - It is pure (no locks, no network, no clocks), making it easy to unit test.
//   - It centralizes naming/tagging behavior, which is an operational contract.
//
// The returned series slice is suitable for SubmitMetrics.
func (b *Backend) buildSeries(s snapshot, nowUnix int64) []datadogV2.MetricSeries {
	addCount := func(metric string, value float64, tags []string) datadogV2.MetricSeries {
		return datadogV2.MetricSeries{
			Metric: metric,
			Type:   datadogV2.METRICINTAKETYPE_COUNT.Ptr(),
			Points: []datadogV2.MetricPoint{
				{Timestamp: dd.PtrInt64(nowUnix), Value: dd.PtrFloat64(value)},
			},
			Tags: tags,
		}
	}

	addGauge := func(metric string, value float64, tags []string) datadogV2.MetricSeries {
		return datadogV2.MetricSeries{
			Metric: metric,
			Type:   datadogV2.METRICINTAKETYPE_GAUGE.Ptr(),
			Points: []datadogV2.MetricPoint{
				{Timestamp: dd.PtrInt64(nowUnix), Value: dd.PtrFloat64(value)},
			},
			Tags: tags,
		}
	}

	series := make([]datadogV2.MetricSeries, 0, len(s.stepCounts)+len(s.recordCounts)+64)

	// Step counters.
	for k, v := range s.stepCounts {
		if v == 0 {
			continue
		}
		step, status := splitStepStatusKey(k)
		tags := withTags(b.baseTags, "step:"+step, "status:"+status)
		series = append(series, addCount("etl.step.total", v, tags))
	}

	// Record counters.
	for kind, v := range s.recordCounts {
		if v == 0 {
			continue
		}
		tags := withTags(b.baseTags, "kind:"+kind)
		series = append(series, addCount("etl.records.total", v, tags))
	}

	// Batch counter.
	if s.batchCount != 0 {
		series = append(series, addCount("etl.batches.total", s.batchCount, b.baseTags))
	}

	// Step duration percentiles.
	for k, samples := range s.durationSamples {
		addPercentiles(&series, b.baseTags, "etl.step.duration_seconds", "step_status", k, samples, nowUnix)
	}

	// HTTP counts.
	for status, v := range s.httpReqCounts {
		if v == 0 {
			continue
		}
		tags := withTags(b.baseTags, "status:"+status)
		series = append(series, addCount("etl.http.requests.total", v, tags))
	}
	for status, v := range s.httpErrCounts {
		if v == 0 {
			continue
		}
		tags := withTags(b.baseTags, "status:"+status)
		series = append(series, addCount("etl.http.errors.total", v, tags))
	}

	// HTTP percentiles.
	for status, samples := range s.httpReqDur {
		addPercentilesWithStatus(&series, b.baseTags, "etl.http.request_duration_seconds", status, samples, addGauge, nowUnix)
	}
	for status, samples := range s.httpRespDur {
		addPercentilesWithStatus(&series, b.baseTags, "etl.http.response_duration_seconds", status, samples, addGauge, nowUnix)
	}
	for status, samples := range s.httpDownloadB {
		addPercentilesWithStatus(&series, b.baseTags, "etl.http.download_bytes", status, samples, addGauge, nowUnix)
	}

	return series
}

// addPercentiles appends a fixed set of percentile gauges for a sample set.
//
// When to use:
//   - Used by Flush() to publish percentiles for step durations.
//
// Edge cases:
//   - If samples is empty, it does nothing.
//   - It sorts a copy of samples (does not mutate input).
func addPercentiles(series *[]datadogV2.MetricSeries, baseTags []string, metricPrefix, keyKind, key string, samples []float64, nowUnix int64) {
	if len(samples) == 0 {
		return
	}
	cp := append([]float64(nil), samples...)
	sort.Float64s(cp)

	step, status := splitStepStatusKey(key)
	tags := withTags(baseTags, "step:"+step, "status:"+status)

	*series = append(*series, gaugeSeries(metricPrefix+".p50", percentileNearestRank(cp, 0.50), tags, nowUnix))
	*series = append(*series, gaugeSeries(metricPrefix+".p90", percentileNearestRank(cp, 0.90), tags, nowUnix))
	*series = append(*series, gaugeSeries(metricPrefix+".p95", percentileNearestRank(cp, 0.95), tags, nowUnix))
	*series = append(*series, gaugeSeries(metricPrefix+".p99", percentileNearestRank(cp, 0.99), tags, nowUnix))
	*series = append(*series, gaugeSeries(metricPrefix+".max", cp[len(cp)-1], tags, nowUnix))
	*series = append(*series, gaugeSeries(metricPrefix+".samples", float64(len(cp)), tags, nowUnix))

	_ = keyKind // reserved for future key schemas
}

func addPercentilesWithStatus(
	series *[]datadogV2.MetricSeries,
	baseTags []string,
	metricPrefix string,
	status string,
	samples []float64,
	addGauge func(metric string, value float64, tags []string) datadogV2.MetricSeries,
	nowUnix int64,
) {
	if len(samples) == 0 {
		return
	}
	cp := append([]float64(nil), samples...)
	sort.Float64s(cp)

	tags := withTags(baseTags, "status:"+status)
	*series = append(*series, addGauge(metricPrefix+".p50", percentileNearestRank(cp, 0.50), tags))
	*series = append(*series, addGauge(metricPrefix+".p90", percentileNearestRank(cp, 0.90), tags))
	*series = append(*series, addGauge(metricPrefix+".p95", percentileNearestRank(cp, 0.95), tags))
	*series = append(*series, addGauge(metricPrefix+".p99", percentileNearestRank(cp, 0.99), tags))
	*series = append(*series, addGauge(metricPrefix+".max", cp[len(cp)-1], tags))
	*series = append(*series, addGauge(metricPrefix+".samples", float64(len(cp)), tags))

	_ = nowUnix // reserved for future if addGauge is replaced by gaugeSeries
}

func gaugeSeries(metric string, value float64, tags []string, nowUnix int64) datadogV2.MetricSeries {
	return datadogV2.MetricSeries{
		Metric: metric,
		Type:   datadogV2.METRICINTAKETYPE_GAUGE.Ptr(),
		Points: []datadogV2.MetricPoint{
			{Timestamp: dd.PtrInt64(nowUnix), Value: dd.PtrFloat64(value)},
		},
		Tags: tags,
	}
}

func stepStatusKey(step, status string) string {
	return step + "\x00" + status
}

func splitStepStatusKey(k string) (step, status string) {
	parts := strings.SplitN(k, "\x00", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return k, "unknown"
}

func withTags(base []string, extras ...string) []string {
	out := make([]string, 0, len(base)+len(extras))
	out = append(out, base...)
	out = append(out, extras...)
	return out
}

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
	idx := int(p*float64(n-1) + 0.5)
	if idx < 0 {
		idx = 0
	}
	if idx >= n {
		idx = n - 1
	}
	return s[idx]
}

var _ metrics.Backend = (*Backend)(nil)

// ParseTagsCSV parses comma-separated tags like "env:prod,service:etl".
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

func wrapInitErr(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("datadog metrics init: %w", err)
}
