package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"slices"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/cmd"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/quality"
	"github.com/enable-xyz/marketdata/serve"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

type runtimeComposition struct {
	boundaries *quality.BoundaryLogger
	metrics    *quality.Metrics
	traces     *sdktrace.TracerProvider
	pressure   config.CaptureConfig
}

func composeRuntime(ctx context.Context, _ string, cfg config.Config, build cmd.BuildInfo, output io.Writer) (cmd.Runtime, error) {
	if output == nil {
		output = io.Discard
	}
	level, err := slogLevel(cfg.Telemetry.LogLevel)
	if err != nil {
		return nil, err
	}
	configDigest, err := cfg.PublicDigest()
	if err != nil {
		return nil, err
	}
	sources, channels, families, support, err := declaredTelemetryDimensions(cfg.Sources)
	if err != nil {
		return nil, err
	}
	metrics, err := quality.NewMetrics(quality.MetricConfig{
		Sources: sources, Channels: channels, Families: families, MaxSeries: cfg.Telemetry.MaxSeries,
	})
	if err != nil {
		return nil, err
	}
	boundaries := quality.NewBoundaryLogger(slog.New(slog.NewJSONHandler(output, &slog.HandlerOptions{Level: level})))
	evidence := quality.StartupEvidence{
		BuildVersion: buildValue(build.Version, "dev"), BuildCommit: buildValue(build.Commit, "none"), BuildDate: buildValue(build.Date, "unknown"),
		SchemaDigest: sha256.Sum256([]byte(fmt.Sprintf("catalog-schema:%d:%d", catalog.MinimumSupportedSchemaVersion, catalog.MaximumSupportedSchemaVersion))),
		MapperDigest: sha256.Sum256([]byte("normalized-schema:" + catalog.NormalizedSchemaVersionV1)),
		ConfigDigest: configDigest, Support: support,
	}
	if err := evidence.Validate(); err != nil {
		return nil, err
	}

	var provider *sdktrace.TracerProvider
	if cfg.Telemetry.TraceExporterRef != "" {
		endpoint, err := resolveTraceEndpoint(ctx, cfg.Telemetry.TraceExporterRef)
		if err != nil {
			return nil, err
		}
		exporter, err := otlptracehttp.New(ctx, otlptracehttp.WithEndpointURL(endpoint))
		if err != nil {
			return nil, errors.New("constructing configured OTLP trace exporter failed")
		}
		bounded, err := quality.NewAsyncTraceExporter(exporter, quality.AsyncTraceConfig{
			QueueCapacity: cfg.Telemetry.TraceQueueCapacity, MaxBatchSpans: cfg.Telemetry.TraceBatchSpans,
			ExportTimeout: cfg.Telemetry.TraceExportTimeout, Dropped: metrics.DroppedTraceCounter(),
			Sources: sources, Channels: channels, Families: families,
		})
		if err != nil {
			_ = exporter.Shutdown(context.WithoutCancel(ctx))
			return nil, err
		}
		provider = sdktrace.NewTracerProvider(sdktrace.WithSyncer(bounded))
	}
	if err := boundaries.Startup(ctx, evidence); err != nil {
		if provider != nil {
			_ = provider.Shutdown(context.WithoutCancel(ctx))
		}
		return nil, err
	}
	return &runtimeComposition{boundaries: boundaries, metrics: metrics, traces: provider, pressure: cfg.Capture}, nil
}

func (r *runtimeComposition) Shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.boundaries.Event(ctx, slog.LevelInfo, quality.BoundaryProcessShutdown)
	if r.traces == nil {
		return nil
	}
	return r.traces.Shutdown(ctx)
}

func (r *runtimeComposition) Metrics() *quality.Metrics {
	if r == nil {
		return nil
	}
	return r.metrics
}

func (r *runtimeComposition) TracerProvider() *sdktrace.TracerProvider {
	if r == nil {
		return nil
	}
	return r.traces
}

// NewServe resolves the explicitly supplied serve configuration and private
// bindings but deliberately does not create or open a network listener.
func (r *runtimeComposition) NewServe(ctx context.Context, configuration serve.Config, resolver serve.SecretResolver, dependencies serve.Dependencies) (*serve.Server, error) {
	if r == nil {
		return nil, errors.New("runtime composition is unavailable")
	}
	if err := validateExplicitServeConfig(configuration); err != nil {
		return nil, err
	}
	return serve.New(ctx, configuration, resolver, dependencies)
}

func validateExplicitServeConfig(configuration serve.Config) error {
	if configuration.TLSCertRef == "" || configuration.TLSKeyRef == "" || configuration.PagingKeyRef == "" ||
		len(configuration.Principals) == 0 || configuration.MaxQueryInterval <= 0 ||
		configuration.DefaultPageRows < 1 || configuration.MaxPageRows < 1 || configuration.MaxResponseBytes < 1 ||
		configuration.PageTokenTTL <= 0 || configuration.ReadHeaderTimeout <= 0 || configuration.ReadTimeout <= 0 ||
		configuration.WriteTimeout <= 0 || configuration.IdleTimeout <= 0 || configuration.Now == nil {
		return errors.New("serve composition requires explicit destinations, bindings, limits, timeouts, and clock")
	}
	for _, limits := range []serve.RouteLimits{
		configuration.Catalog, configuration.Query, configuration.NativeReplay, configuration.NormalizedReplay,
	} {
		if limits.QueueDepth < 1 || limits.Concurrency < 1 || limits.Deadline <= 0 || limits.MaxDuration <= 0 ||
			limits.MaxBytes < 1 || limits.BufferBytes < 1 {
			return errors.New("serve composition requires every queue, concurrency, deadline, byte, and buffer bound")
		}
	}
	return nil
}

// NewWriterPressure binds the caller's durable frame/segment boundary and
// connection effects to the one validated root pressure policy.
func (r *runtimeComposition) NewWriterPressure(transport capture.PressureTransport, clock capture.Clock, durable capture.DurableBoundary, hooks capture.PressureHooks) (*capture.WriterPressure, error) {
	if r == nil || r.pressure.DecodeQueueCapacity == 0 {
		return nil, errors.New("capture pressure is not configured")
	}
	return capture.NewWriterPressure(capture.WriterPressureConfig{
		Transport: transport, DecodeQueueCapacity: r.pressure.DecodeQueueCapacity, DurableQueueCapacity: r.pressure.DurableQueueCapacity,
		DecodeHighWater: r.pressure.DecodeHighWater, DurableHighWater: r.pressure.DurableHighWater,
		DecodeLowWater: r.pressure.DecodeLowWater, DurableLowWater: r.pressure.DurableLowWater,
		MaxRawMessageBytes: r.pressure.MaxRawMessageBytes, PendingRESTCapacity: r.pressure.PendingRESTCapacity,
	}, clock, durable, hooks)
}

func (r *runtimeComposition) NewRateOwners(configs []capture.RateOwnerConfig, clock capture.Clock) (*capture.RateOwners, error) {
	return capture.NewRateOwners(configs, clock)
}

func resolveTraceEndpoint(ctx context.Context, reference string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	endpoint, exists := os.LookupEnv(reference)
	if !exists || endpoint == "" {
		return "", errors.New("configured OTLP endpoint binding is absent")
	}
	parsed, err := url.ParseRequestURI(endpoint)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" ||
		(parsed.Scheme != "http" && parsed.Scheme != "https") {
		return "", errors.New("configured OTLP endpoint binding is invalid")
	}
	return endpoint, nil
}

func declaredTelemetryDimensions(configured []config.SourceConfig) ([]string, []string, []string, []quality.SupportContract, error) {
	sources := make([]string, 0, len(configured))
	var channels []string
	var families []string
	support := make([]quality.SupportContract, 0, len(configured))
	for _, source := range configured {
		declaredChannels := slices.Clone(source.Channels)
		declaredFamilies := slices.Clone(source.Families)
		slices.Sort(declaredChannels)
		slices.Sort(declaredFamilies)
		if len(declaredChannels) == 0 || len(declaredFamilies) == 0 {
			return nil, nil, nil, nil, errors.New("declared telemetry support requires channel and family allowlists")
		}
		sources = append(sources, source.ID)
		channels = append(channels, declaredChannels...)
		families = append(families, declaredFamilies...)
		support = append(support, quality.SupportContract{
			SourceID: source.ID, API: source.API, Channels: declaredChannels, Families: declaredFamilies,
		})
	}
	slices.Sort(sources)
	slices.Sort(channels)
	slices.Sort(families)
	sources = slices.Compact(sources)
	channels = slices.Compact(channels)
	families = slices.Compact(families)
	slices.SortFunc(support, func(left, right quality.SupportContract) int {
		if left.SourceID < right.SourceID {
			return -1
		}
		if left.SourceID > right.SourceID {
			return 1
		}
		if left.API < right.API {
			return -1
		}
		if left.API > right.API {
			return 1
		}
		return 0
	})
	return sources, channels, families, support, nil
}

func slogLevel(value string) (slog.Level, error) {
	switch value {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, errors.New("configured log level is invalid")
	}
}

func buildValue(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

var _ cmd.Runtime = (*runtimeComposition)(nil)
