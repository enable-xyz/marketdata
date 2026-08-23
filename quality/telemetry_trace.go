package quality

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

var ErrTraceExporterShutdown = errors.New("quality: trace exporter is shut down")

type Counter interface {
	Inc()
}

type AsyncTraceConfig struct {
	QueueCapacity int
	MaxBatchSpans int
	ExportTimeout time.Duration
	Dropped       Counter
	Sources       []string
	Channels      []string
	Families      []string
}

type TraceQueueStats struct {
	Accepted    uint64
	Dropped     uint64
	Exported    uint64
	ExportError uint64
	Queued      int64
	Capacity    int
}

type traceBatch struct {
	spans []sdktrace.ReadOnlySpan
}

// AsyncTraceExporter is a bounded nonblocking wrapper around an OTLP-compatible
// SpanExporter. ExportSpans never waits for the downstream exporter. A full or
// oversized queue drops the entire immutable batch and increments Dropped.
type AsyncTraceExporter struct {
	next           sdktrace.SpanExporter
	queue          chan traceBatch
	slots          chan struct{}
	stop           chan struct{}
	done           chan struct{}
	exportTimeout  time.Duration
	maxBatchSpans  int
	droppedCounter Counter
	sources        map[string]string
	channels       map[string]string
	families       map[string]string
	outcomes       map[string]string
	emptyResource  *resource.Resource
	admit          sync.RWMutex
	shutdownOnce   sync.Once
	closed         atomic.Bool
	accepted       atomic.Uint64
	dropped        atomic.Uint64
	exported       atomic.Uint64
	exportError    atomic.Uint64
	queued         atomic.Int64
}

func NewAsyncTraceExporter(next sdktrace.SpanExporter, config AsyncTraceConfig) (*AsyncTraceExporter, error) {
	if next == nil {
		return nil, telemetryError("trace exporter is required")
	}
	if config.QueueCapacity < 1 || config.QueueCapacity > 4_096 || config.MaxBatchSpans < 1 || config.MaxBatchSpans > 512 || config.ExportTimeout <= 0 {
		return nil, telemetryError("trace queue must not exceed 4096 batches or 512 spans per batch and requires a positive export timeout")
	}
	sources, err := traceAllowlist("source", config.Sources)
	if err != nil {
		return nil, err
	}
	channels, err := traceAllowlist("channel", config.Channels)
	if err != nil {
		return nil, err
	}
	families, err := traceAllowlist("family", config.Families)
	if err != nil {
		return nil, err
	}
	exporter := &AsyncTraceExporter{
		next: next, queue: make(chan traceBatch, config.QueueCapacity), slots: make(chan struct{}, config.QueueCapacity),
		stop: make(chan struct{}), done: make(chan struct{}), exportTimeout: config.ExportTimeout,
		maxBatchSpans: config.MaxBatchSpans, droppedCounter: config.Dropped,
		sources: sources, channels: channels, families: families, outcomes: canonicalStrings(fixedOutcomes), emptyResource: resource.Empty(),
	}
	go exporter.run()
	return exporter, nil
}

func (e *AsyncTraceExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if e == nil {
		return ErrTraceExporterShutdown
	}
	e.admit.RLock()
	defer e.admit.RUnlock()
	if e.closed.Load() {
		return ErrTraceExporterShutdown
	}
	if len(spans) == 0 {
		return nil
	}
	if len(spans) > e.maxBatchSpans {
		e.recordDrop()
		return nil
	}
	select {
	case e.slots <- struct{}{}:
	default:
		e.recordDrop()
		return nil
	}
	batch := traceBatch{spans: make([]sdktrace.ReadOnlySpan, len(spans))}
	for index, span := range spans {
		batch.spans[index] = e.redactSpan(span)
	}
	e.queued.Add(1)
	e.queue <- batch
	e.accepted.Add(uint64(len(spans)))
	return nil
}

func (e *AsyncTraceExporter) Shutdown(ctx context.Context) error {
	if e == nil {
		return nil
	}
	e.shutdownOnce.Do(func() {
		e.admit.Lock()
		e.closed.Store(true)
		close(e.stop)
		e.admit.Unlock()
	})
	select {
	case <-e.done:
		return e.next.Shutdown(ctx)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ForceFlush waits for the queue to become empty. It never waits for queue
// admission and honors the caller's cancellation.
func (e *AsyncTraceExporter) ForceFlush(ctx context.Context) error {
	if e == nil {
		return nil
	}
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for e.queued.Load() != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
	return nil
}

func (e *AsyncTraceExporter) Stats() TraceQueueStats {
	if e == nil {
		return TraceQueueStats{}
	}
	return TraceQueueStats{
		Accepted: e.accepted.Load(), Dropped: e.dropped.Load(), Exported: e.exported.Load(), ExportError: e.exportError.Load(),
		Queued: e.queued.Load(), Capacity: cap(e.queue),
	}
}

func (e *AsyncTraceExporter) recordDrop() {
	e.dropped.Add(1)
	if e.droppedCounter != nil {
		e.droppedCounter.Inc()
	}
}

func (e *AsyncTraceExporter) run() {
	defer close(e.done)
	for {
		select {
		case batch := <-e.queue:
			e.export(batch)
		case <-e.stop:
			for {
				select {
				case batch := <-e.queue:
					e.export(batch)
				default:
					return
				}
			}
		}
	}
}

func (e *AsyncTraceExporter) export(batch traceBatch) {
	ctx, cancel := context.WithTimeout(context.Background(), e.exportTimeout)
	err := e.next.ExportSpans(ctx, batch.spans)
	cancel()
	e.queued.Add(-1)
	<-e.slots
	if err != nil {
		e.exportError.Add(1)
		return
	}
	e.exported.Add(uint64(len(batch.spans)))
}

type redactedSpan struct {
	sdktrace.ReadOnlySpan
	name                 string
	spanContext          oteltrace.SpanContext
	parent               oteltrace.SpanContext
	spanKind             oteltrace.SpanKind
	startTime            time.Time
	endTime              time.Time
	attributes           []attribute.KeyValue
	links                []sdktrace.Link
	events               []sdktrace.Event
	status               sdktrace.Status
	droppedAttributes    int
	droppedLinks         int
	droppedEvents        int
	childSpanCount       int
	resource             *resource.Resource
	instrumentationScope instrumentation.Scope
}

func (s redactedSpan) Name() string                                    { return s.name }
func (s redactedSpan) SpanContext() oteltrace.SpanContext              { return s.spanContext }
func (s redactedSpan) Parent() oteltrace.SpanContext                   { return s.parent }
func (s redactedSpan) SpanKind() oteltrace.SpanKind                    { return s.spanKind }
func (s redactedSpan) StartTime() time.Time                            { return s.startTime }
func (s redactedSpan) EndTime() time.Time                              { return s.endTime }
func (s redactedSpan) Attributes() []attribute.KeyValue                { return s.attributes }
func (s redactedSpan) Links() []sdktrace.Link                          { return s.links }
func (s redactedSpan) Events() []sdktrace.Event                        { return s.events }
func (s redactedSpan) Status() sdktrace.Status                         { return s.status }
func (s redactedSpan) InstrumentationScope() instrumentation.Scope     { return s.instrumentationScope }
func (s redactedSpan) InstrumentationLibrary() instrumentation.Library { return s.instrumentationScope }
func (s redactedSpan) Resource() *resource.Resource                    { return s.resource }
func (s redactedSpan) DroppedAttributes() int                          { return s.droppedAttributes }
func (s redactedSpan) DroppedLinks() int                               { return s.droppedLinks }
func (s redactedSpan) DroppedEvents() int                              { return s.droppedEvents }
func (s redactedSpan) ChildSpanCount() int                             { return s.childSpanCount }

func (e *AsyncTraceExporter) redactSpan(span sdktrace.ReadOnlySpan) sdktrace.ReadOnlySpan {
	redacted := redactedSpan{name: "trace.redacted", resource: e.emptyResource}
	if span == nil {
		return redacted
	}
	if traceNameAllowed(span.Name()) {
		redacted.name = span.Name()
	}
	redacted.spanContext = redactSpanContext(span.SpanContext())
	redacted.parent = redactSpanContext(span.Parent())
	redacted.spanKind = span.SpanKind()
	redacted.startTime = span.StartTime()
	redacted.endTime = span.EndTime()
	attributes := span.Attributes()
	redacted.attributes = e.redactAttributes(attributes)
	redacted.droppedAttributes = span.DroppedAttributes() + max(0, len(attributes)-len(redacted.attributes))
	redacted.childSpanCount = span.ChildSpanCount()
	status := span.Status()
	status.Description = ""
	redacted.status = status

	links := span.Links()
	redacted.links = make([]sdktrace.Link, 0, min(len(links), 32))
	for _, link := range links[:min(len(links), 32)] {
		attributes := e.redactAttributes(link.Attributes)
		redacted.links = append(redacted.links, sdktrace.Link{
			SpanContext: redactSpanContext(link.SpanContext), Attributes: attributes,
			DroppedAttributeCount: link.DroppedAttributeCount + max(0, len(link.Attributes)-len(attributes)),
		})
	}
	redacted.droppedLinks = span.DroppedLinks() + max(0, len(links)-len(redacted.links))

	events := span.Events()
	redacted.events = make([]sdktrace.Event, 0, min(len(events), 32))
	for _, event := range events[:min(len(events), 32)] {
		name := "trace.redacted"
		if traceNameAllowed(event.Name) {
			name = event.Name
		}
		attributes := e.redactAttributes(event.Attributes)
		redacted.events = append(redacted.events, sdktrace.Event{
			Name: name, Attributes: attributes, Time: event.Time,
			DroppedAttributeCount: event.DroppedAttributeCount + max(0, len(event.Attributes)-len(attributes)),
		})
	}
	redacted.droppedEvents = span.DroppedEvents() + max(0, len(events)-len(redacted.events))
	return redacted
}

func (e *AsyncTraceExporter) redactAttributes(attributes []attribute.KeyValue) []attribute.KeyValue {
	result := make([]attribute.KeyValue, 0, min(len(attributes), 4))
	var seen [4]bool
	for _, keyValue := range attributes[:min(len(attributes), 64)] {
		if keyValue.Value.Type() != attribute.STRING {
			continue
		}
		var index int
		var allowlist map[string]string
		switch string(keyValue.Key) {
		case metricLabelSource:
			index, allowlist = 0, e.sources
		case metricLabelChannel:
			index, allowlist = 1, e.channels
		case metricLabelFamily:
			index, allowlist = 2, e.families
		case metricLabelOutcome:
			index, allowlist = 3, e.outcomes
		default:
			continue
		}
		if seen[index] {
			continue
		}
		seen[index] = true
		value, exists := allowlist[keyValue.Value.AsString()]
		if !exists {
			value = metricUnknown
		}
		result = append(result, attribute.String(string(keyValue.Key), value))
	}
	return result
}

func redactSpanContext(spanContext oteltrace.SpanContext) oteltrace.SpanContext {
	return oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: spanContext.TraceID(), SpanID: spanContext.SpanID(), TraceFlags: spanContext.TraceFlags(), Remote: spanContext.IsRemote(),
	})
}

func traceAllowlist(name string, values []string) (map[string]string, error) {
	allowed, err := metricAllowlist(name, values)
	if err != nil {
		return nil, err
	}
	return canonicalStrings(allowed), nil
}

func canonicalStrings(values []string) map[string]string {
	canonical := make(map[string]string, len(values))
	for _, value := range values {
		canonical[value] = value
	}
	return canonical
}

func traceNameAllowed(name string) bool {
	if len(name) > 64 {
		return false
	}
	switch name {
	case "dns.lookup", "tls.handshake", "connection.open", "connection.close", "authentication.check",
		"subscription.ack", "heartbeat.check", "channel.receive", "rest.request", "writer.flush",
		"segment.commit", "dataset.commit", "replay.request", "query.request":
		return true
	default:
		return false
	}
}

var _ sdktrace.ReadOnlySpan = redactedSpan{}
var _ sdktrace.SpanExporter = (*AsyncTraceExporter)(nil)
