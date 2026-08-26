package quality

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

type adversarialLogValue struct{ secret string }

func (v adversarialLogValue) LogValue() slog.Value {
	return slog.GroupValue(slog.String("payload", v.secret), slog.Any("error", errors.New(v.secret)))
}

type capturingSpanExporter struct{ spans chan []sdktrace.ReadOnlySpan }

func (e *capturingSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.spans <- spans
	return nil
}

func (*capturingSpanExporter) Shutdown(context.Context) error { return nil }

func TestRedaction(t *testing.T) {
	const secret = "ELMD023-never-emit-this-bearer-secret"
	var output bytes.Buffer
	logger := slog.New(NewRedactingHandler(slog.NewJSONHandler(&output, nil)))
	segmentDigest := sha256.Sum256([]byte("segment-0001"))
	reference, err := NewRawReference([]byte(secret), RawCoordinate{SegmentSHA256: segmentDigest, EpochOrdinal: 42})
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse("https://user:" + secret + "@example.test/data?token=" + secret)
	if err != nil {
		t.Fatal(err)
	}
	headers := http.Header{"Authorization": {"Bearer " + secret}, "X-Arbitrary": {secret}}
	logger.WithGroup("headers").Info("connection.opened", "X-Arbitrary", secret)
	logger.LogAttrs(t.Context(), slog.LevelInfo, "segment.committed",
		slog.String("payload", secret),
		slog.String("authorization", "Bearer "+secret),
		slog.String("tls_key", secret),
		slog.String("paging_token", secret),
		slog.String("venue_credentials", secret),
		slog.String("token_hash", secret),
		slog.String("source", "sk_live_ABC123"),
		slog.Any("state", adversarialLogValue{secret: secret}),
		slog.Any("status", errors.Join(errors.New(secret), errors.New("nested: "+secret))),
		slog.Any("support", slog.GroupValue(slog.Any("headers", headers), slog.Any("endpoint", parsed))),
		slog.Any("state", reference),
		slog.Any("authorization", reference),
	)
	encoded := output.String()
	for _, forbidden := range []string{secret, "Bearer", "Authorization", "user:" + secret, "X-Arbitrary", "sk_live_ABC123"} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("redacted log contains forbidden material %q: %s", forbidden, encoded)
		}
	}
	if !strings.Contains(encoded, digestHex(segmentDigest)) || !strings.Contains(encoded, `"epoch_ordinal":42`) {
		t.Fatalf("structural raw coordinate is absent: %s", encoded)
	}
	wantDigest := sha256.Sum256([]byte(secret))
	if !strings.Contains(encoded, digestHex(wantDigest)) {
		t.Fatalf("redacted raw digest is absent: %s", encoded)
	}
	if strings.Count(encoded, digestHex(wantDigest)) != 1 {
		t.Fatalf("sensitive-key raw reference bypassed redaction: %s", encoded)
	}

	spanSink := &capturingSpanExporter{spans: make(chan []sdktrace.ReadOnlySpan, 1)}
	traceExporter, err := NewAsyncTraceExporter(spanSink, AsyncTraceConfig{
		QueueCapacity: 1, MaxBatchSpans: 1, ExportTimeout: time.Second,
		Sources: []string{"source-a"}, Channels: []string{"trades"}, Families: []string{"trade"},
	})
	if err != nil {
		t.Fatal(err)
	}
	traceState, err := oteltrace.ParseTraceState("vendor=must-not-cross-boundary")
	if err != nil {
		t.Fatal(err)
	}
	spanContext := oteltrace.NewSpanContext(oteltrace.SpanContextConfig{
		TraceID: oteltrace.TraceID{1}, SpanID: oteltrace.SpanID{1}, TraceState: traceState,
	})
	input := redactedSpan{
		name: secret, spanContext: spanContext, resource: resource.NewSchemaless(attribute.String("authorization", secret)),
		attributes: []attribute.KeyValue{
			attribute.String("source", "source-a"), attribute.String("outcome", "observed"),
			attribute.String("payload", secret), attribute.String("request_id", secret),
		},
		events: []sdktrace.Event{{Name: secret, Attributes: []attribute.KeyValue{attribute.String("error", secret)}}},
		links:  []sdktrace.Link{{SpanContext: spanContext, Attributes: []attribute.KeyValue{attribute.String("headers", secret)}}},
		status: sdktrace.Status{Description: secret},
	}
	if err := traceExporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{input}); err != nil {
		t.Fatal(err)
	}
	exported := (<-spanSink.spans)[0]
	if exported.Name() != "trace.redacted" || exported.Status().Description != "" || exported.Resource().Len() != 0 ||
		exported.SpanContext().TraceState().Len() != 0 {
		t.Fatalf("trace boundary retained unsafe name, status, resource, or trace state")
	}
	if got := exported.Attributes(); len(got) != 2 || got[0].Value.AsString() == secret || got[1].Value.AsString() == secret {
		t.Fatalf("trace attributes were not bounded and redacted: %+v", got)
	}
	if events := exported.Events(); len(events) != 1 || events[0].Name != "trace.redacted" || len(events[0].Attributes) != 0 {
		t.Fatalf("trace events were not redacted: %+v", events)
	}
	if links := exported.Links(); len(links) != 1 || len(links[0].Attributes) != 0 || links[0].SpanContext.TraceState().Len() != 0 {
		t.Fatalf("trace links were not redacted: %+v", links)
	}
	if err := traceExporter.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCardinality(t *testing.T) {
	metrics, err := NewMetrics(MetricConfig{
		Sources: []string{"source-a"}, Channels: []string{"trades"}, Families: []string{"trade"}, MaxSeries: 10_000,
	})
	if err != nil {
		t.Fatal(err)
	}
	attributes := MetricAttributes{
		"source": "source-a", "channel": "trades", "family": "trade", "outcome": "observed",
	}
	for index := range 1_000_000 {
		identity := strconv.Itoa(index)
		attributes["instrument_uid"] = "instrument-" + identity
		attributes["symbol"] = "symbol-" + identity
		attributes["trade_id"] = "trade-" + identity
		attributes["object_key"] = "object-" + identity
		attributes["request_id"] = "request-" + identity
		attributes["payload"] = "payload-" + identity
		if err := metrics.Record(SignalChannelMessages, 1, attributes); err != nil {
			t.Fatal(err)
		}
	}
	families, err := metrics.Registry().Gather()
	if err != nil {
		t.Fatal(err)
	}
	series := 0
	for _, family := range families {
		series += len(family.Metric)
	}
	if series != 1 {
		t.Fatalf("one million forbidden identities produced %d metric series, want 1", series)
	}
	if metrics.SeriesBound() > 10_000 {
		t.Fatalf("declared series bound = %d, want <= 10000", metrics.SeriesBound())
	}
}

type blackholeSpanExporter struct {
	entered chan struct{}
	release chan struct{}
}

func (e *blackholeSpanExporter) ExportSpans(context.Context, []sdktrace.ReadOnlySpan) error {
	select {
	case e.entered <- struct{}{}:
	default:
	}
	<-e.release
	return nil
}

func (e *blackholeSpanExporter) Shutdown(context.Context) error { return nil }

type atomicCounter struct{ count atomic.Uint64 }

func (c *atomicCounter) Inc() { c.count.Add(1) }

type blackholeRawBoundary struct{ count atomic.Uint64 }

func (b *blackholeRawBoundary) WriteRaw(context.Context, capture.RawMessage) error {
	b.count.Add(1)
	return nil
}

func (*blackholeRawBoundary) FlushCommit(context.Context) (capture.DurableCommit, error) {
	return capture.DurableCommit{SegmentID: "unused-segment", LastCoordinate: "unused-coordinate"}, nil
}

func TestTelemetryBlackhole(t *testing.T) {
	blackhole := &blackholeSpanExporter{entered: make(chan struct{}, 1), release: make(chan struct{})}
	dropped := new(atomicCounter)
	exporter, err := NewAsyncTraceExporter(blackhole, AsyncTraceConfig{
		QueueCapacity: 4, MaxBatchSpans: 1, ExportTimeout: time.Second, Dropped: dropped,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{nil}); err != nil {
		t.Fatal(err)
	}
	<-blackhole.entered
	for range 64 {
		if err := exporter.ExportSpans(t.Context(), []sdktrace.ReadOnlySpan{nil}); err != nil {
			t.Fatal(err)
		}
	}

	clock, err := capture.NewManualClock(0, "telemetry-blackhole-clock")
	if err != nil {
		t.Fatal(err)
	}
	raw := new(blackholeRawBoundary)
	writer, err := capture.NewWriterPressure(capture.WriterPressureConfig{
		Transport: capture.PressureTransportREST, DecodeQueueCapacity: 4, DurableQueueCapacity: 4,
		DecodeHighWater: 3, DurableHighWater: 3, DecodeLowWater: 0, DurableLowWater: 0,
		MaxRawMessageBytes: 32, PendingRESTCapacity: 1,
	}, clock, raw, capture.PressureHooks{
		RecordRESTOutcome: func(context.Context, string, capture.PressureOutcome) error { return nil },
		ResumeREST:        func(context.Context) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := range 100_000 {
		message := capture.RawMessage{
			Stream: "canonical", Coordinate: "raw-" + strconv.Itoa(index), Payload: []byte("complete"), FrameComplete: true,
		}
		if err := writer.EnqueueDecoded(t.Context(), message); err != nil {
			t.Fatal(err)
		}
		if err := writer.AdvanceDecode(t.Context()); err != nil {
			t.Fatal(err)
		}
		if err := writer.CommitOne(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if raw.count.Load() != 100_000 {
		t.Fatalf("raw durability progress = %d, want 100000", raw.count.Load())
	}
	stats := exporter.Stats()
	if stats.Queued > int64(stats.Capacity) || stats.Capacity != 4 || stats.Dropped == 0 || dropped.count.Load() == 0 {
		t.Fatalf("bounded blackhole stats = %+v, drop counter = %d", stats, dropped.count.Load())
	}
	close(blackhole.release)
	shutdown, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := exporter.Shutdown(shutdown); err != nil {
		t.Fatal(err)
	}
}

func TestStartupEvidence(t *testing.T) {
	var output bytes.Buffer
	logger := NewBoundaryLogger(slog.New(slog.NewJSONHandler(&output, nil)))
	evidence := StartupEvidence{
		BuildVersion: "1.2.3", BuildCommit: "abcdef0", BuildDate: "2026-08-23T00:00:00Z",
		SchemaDigest: sha256.Sum256([]byte("schema-v1")), MapperDigest: sha256.Sum256([]byte("mapper-v1")),
		ConfigDigest: sha256.Sum256([]byte("secret-ref-must-not-appear")),
		Support:      []SupportContract{{SourceID: "source-a", API: "binance-spot", Channels: []string{"depth", "trades"}, Families: []string{"book", "trade"}}},
	}
	if err := logger.Startup(t.Context(), evidence); err != nil {
		t.Fatal(err)
	}
	encoded := output.String()
	for _, required := range []string{"process.startup", "1.2.3", "abcdef0", "source-a", "binance-spot", digestHex(evidence.ConfigDigest)} {
		if !strings.Contains(encoded, required) {
			t.Fatalf("startup evidence omits %q: %s", required, encoded)
		}
	}
	if strings.Contains(encoded, "secret-ref-must-not-appear") {
		t.Fatalf("startup evidence disclosed config material: %s", encoded)
	}
	maximum := evidence
	maximum.Support = make([]SupportContract, maxStartupSupport)
	for index := range maximum.Support {
		maximum.Support[index] = SupportContract{
			SourceID: "source-" + strconv.Itoa(index), API: "public-v1",
			Channels: []string{"trades"}, Families: []string{"trade"},
		}
	}
	output.Reset()
	if err := logger.Startup(t.Context(), maximum); err != nil {
		t.Fatal(err)
	}
	for index := range maximum.Support {
		if !strings.Contains(output.String(), `"source":"source-`+strconv.Itoa(index)+`"`) {
			t.Fatalf("startup evidence omitted validated contract %d", index)
		}
	}
	overflow := maximum
	overflow.Support = append(overflow.Support, SupportContract{
		SourceID: "source-overflow", API: "public-v1", Channels: []string{"trades"}, Families: []string{"trade"},
	})
	if err := logger.Startup(t.Context(), overflow); err == nil {
		t.Fatal("startup evidence accepted contracts beyond its emission bound")
	}
}

func digestHex(digest [sha256.Size]byte) string {
	const hexadecimal = "0123456789abcdef"
	encoded := make([]byte, sha256.Size*2)
	for index, value := range digest {
		encoded[index*2] = hexadecimal[value>>4]
		encoded[index*2+1] = hexadecimal[value&0x0f]
	}
	return string(encoded)
}
