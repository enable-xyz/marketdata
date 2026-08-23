package quality

import (
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	metricLabelSource  = "source"
	metricLabelChannel = "channel"
	metricLabelFamily  = "family"
	metricLabelOutcome = "outcome"
	metricUnknown      = "unknown"
)

var (
	metricLabelNames = []string{metricLabelSource, metricLabelChannel, metricLabelFamily, metricLabelOutcome}
	histogramBuckets = []float64{0.0001, 0.001, 0.01, 0.1, 1, 10, 60, 300, 1800}
)

// Signal names one bounded collector. Signal is a metric name component rather
// than a label, so callers cannot create a fifth cardinality dimension.
type Signal string

const (
	SignalDNS                   Signal = "dns_health"
	SignalTLS                   Signal = "tls_health"
	SignalConnect               Signal = "connect_health"
	SignalAuthentication        Signal = "authentication_health"
	SignalSubscriptionAck       Signal = "subscription_ack_health"
	SignalHeartbeat             Signal = "heartbeat_health"
	SignalUsefulDataSilence     Signal = "useful_data_silence_seconds"
	SignalChannelMessages       Signal = "channel_messages_total"
	SignalChannelBytes          Signal = "channel_bytes_total"
	SignalInterArrival          Signal = "channel_interarrival_seconds"
	SignalExchangeLag           Signal = "exchange_lag_seconds"
	SignalClockUncertainty      Signal = "clock_uncertainty_seconds"
	SignalSequenceReset         Signal = "sequence_resets_total"
	SignalSnapshot              Signal = "snapshot_state"
	SignalChecksum              Signal = "checksum_failures_total"
	SignalReconstructionEpoch   Signal = "reconstruction_epoch"
	SignalDecompressionFailure  Signal = "decompression_failures_total"
	SignalSchemaQuarantine      Signal = "schema_quarantine_total"
	SignalRESTLatency           Signal = "rest_latency_seconds"
	SignalRESTStatus            Signal = "rest_responses_total"
	SignalRESTTimeout           Signal = "rest_timeouts_total"
	SignalRESTUsedWeight        Signal = "rest_used_weight"
	SignalBanBudget             Signal = "rate_ban_budget"
	SignalWriterQueue           Signal = "writer_queue_depth"
	SignalSpoolBytes            Signal = "spool_bytes"
	SignalSegmentCloseLag       Signal = "segment_close_lag_seconds"
	SignalSegmentUploadLag      Signal = "segment_upload_lag_seconds"
	SignalSegmentCommitLag      Signal = "segment_commit_lag_seconds"
	SignalGapState              Signal = "gap_state"
	SignalObjectVerification    Signal = "object_verification_total"
	SignalCatalogAge            Signal = "catalog_age_seconds"
	SignalInstrumentAssociation Signal = "instrument_association_failures_total"
	SignalDatasetLag            Signal = "dataset_projection_lag_seconds"
	SignalWarehouseLag          Signal = "warehouse_projection_lag_seconds"
	SignalReplayPressure        Signal = "replay_pressure"
	SignalQueryPressure         Signal = "query_pressure"
	SignalTelemetryDropped      Signal = "telemetry_dropped_total"
)

var signalKinds = map[Signal]collectorKind{
	SignalDNS: kindGauge, SignalTLS: kindGauge, SignalConnect: kindGauge, SignalAuthentication: kindGauge,
	SignalSubscriptionAck: kindGauge, SignalHeartbeat: kindGauge, SignalUsefulDataSilence: kindGauge,
	SignalChannelMessages: kindCounter, SignalChannelBytes: kindCounter, SignalInterArrival: kindHistogram,
	SignalExchangeLag: kindHistogram, SignalClockUncertainty: kindGauge, SignalSequenceReset: kindCounter,
	SignalSnapshot: kindGauge, SignalChecksum: kindCounter, SignalReconstructionEpoch: kindGauge,
	SignalDecompressionFailure: kindCounter, SignalSchemaQuarantine: kindCounter, SignalRESTLatency: kindHistogram,
	SignalRESTStatus: kindCounter, SignalRESTTimeout: kindCounter, SignalRESTUsedWeight: kindGauge,
	SignalBanBudget: kindGauge, SignalWriterQueue: kindGauge, SignalSpoolBytes: kindGauge,
	SignalSegmentCloseLag: kindHistogram, SignalSegmentUploadLag: kindHistogram, SignalSegmentCommitLag: kindHistogram,
	SignalGapState: kindGauge, SignalObjectVerification: kindCounter, SignalCatalogAge: kindGauge,
	SignalInstrumentAssociation: kindCounter, SignalDatasetLag: kindGauge, SignalWarehouseLag: kindGauge,
	SignalReplayPressure: kindGauge, SignalQueryPressure: kindGauge, SignalTelemetryDropped: kindCounter,
}

var fixedOutcomes = []string{
	"collector_failed", "intentionally_excluded", "malformed", "observed", "observed_unchanged", "rate_limited",
	"schema_rejected", "sequence_gap", "source_stale", "unknown", "venue_unavailable",
}

type MetricConfig struct {
	Registry  *prometheus.Registry
	Sources   []string
	Channels  []string
	Families  []string
	MaxSeries int
}

// MetricAttributes is intentionally open for boundary adapters, but Record
// reads only the four named keys. Instrument, symbol, trade, object, request,
// payload, and every other key are ignored without allocation or retention.
type MetricAttributes map[string]string

type collectorKind uint8

const (
	kindCounter collectorKind = iota + 1
	kindGauge
	kindHistogram
)

type signalCollector struct {
	counter   *prometheus.CounterVec
	gauge     *prometheus.GaugeVec
	histogram *prometheus.HistogramVec
}

type Metrics struct {
	registry   *prometheus.Registry
	collectors map[Signal]signalCollector
	sources    map[string]struct{}
	channels   map[string]struct{}
	families   map[string]struct{}
	outcomes   map[string]struct{}
	seriesMax  int
}

func NewMetrics(config MetricConfig) (*Metrics, error) {
	if config.MaxSeries < 1 {
		return nil, telemetryError("max series must be positive")
	}
	sources, err := metricAllowlist("source", config.Sources)
	if err != nil {
		return nil, err
	}
	channels, err := metricAllowlist("channel", config.Channels)
	if err != nil {
		return nil, err
	}
	families, err := metricAllowlist("family", config.Families)
	if err != nil {
		return nil, err
	}
	seriesMax, ok := boundedSeries(signalSeriesWeight(), len(sources), len(channels), len(families), len(fixedOutcomes))
	if !ok || seriesMax > config.MaxSeries {
		return nil, fmt.Errorf("quality: telemetry configured series bound %d exceeds maximum %d", seriesMax, config.MaxSeries)
	}
	registry := config.Registry
	if registry == nil {
		registry = prometheus.NewRegistry()
	}
	metrics := &Metrics{
		registry: registry, collectors: make(map[Signal]signalCollector, len(signalKinds)),
		sources: toSet(sources), channels: toSet(channels), families: toSet(families), outcomes: toSet(fixedOutcomes),
		seriesMax: seriesMax,
	}
	for signal, kind := range signalKinds {
		collector := newSignalCollector(signal, kind)
		var registered prometheus.Collector
		switch kind {
		case kindCounter:
			registered = collector.counter
		case kindGauge:
			registered = collector.gauge
		case kindHistogram:
			registered = collector.histogram
		}
		if err := registry.Register(registered); err != nil {
			return nil, fmt.Errorf("quality: registering metric %s: %w", signal, err)
		}
		metrics.collectors[signal] = collector
	}
	return metrics, nil
}

func newSignalCollector(signal Signal, kind collectorKind) signalCollector {
	name := "enable_market_" + string(signal)
	help := "Bounded market-data " + strings.ReplaceAll(string(signal), "_", " ") + "."
	switch kind {
	case kindCounter:
		return signalCollector{counter: prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, metricLabelNames)}
	case kindGauge:
		return signalCollector{gauge: prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, metricLabelNames)}
	default:
		return signalCollector{histogram: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: name, Help: help,
			Buckets: histogramBuckets,
		}, metricLabelNames)}
	}
}

// Record updates exactly one fixed collector. Counters add non-negative value,
// gauges set value, and histograms observe non-negative value.
func (m *Metrics) Record(signal Signal, value float64, attributes MetricAttributes) error {
	if m == nil || math.IsNaN(value) || math.IsInf(value, 0) {
		return telemetryError("metric observation is invalid")
	}
	collector, exists := m.collectors[signal]
	if !exists {
		return telemetryError("metric signal is unknown")
	}
	labels := []string{
		collapse(attributes[metricLabelSource], m.sources),
		collapse(attributes[metricLabelChannel], m.channels),
		collapse(attributes[metricLabelFamily], m.families),
		collapse(attributes[metricLabelOutcome], m.outcomes),
	}
	switch {
	case collector.counter != nil:
		if value < 0 {
			return telemetryError("counter observation cannot be negative")
		}
		collector.counter.WithLabelValues(labels...).Add(value)
	case collector.gauge != nil:
		collector.gauge.WithLabelValues(labels...).Set(value)
	case collector.histogram != nil:
		if value < 0 {
			return telemetryError("histogram observation cannot be negative")
		}
		collector.histogram.WithLabelValues(labels...).Observe(value)
	}
	return nil
}

func (m *Metrics) Registry() *prometheus.Registry {
	if m == nil {
		return nil
	}
	return m.registry
}

func (m *Metrics) Handler() http.Handler {
	if m == nil || m.registry == nil {
		return http.NotFoundHandler()
	}
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) SeriesBound() int {
	if m == nil {
		return 0
	}
	return m.seriesMax
}

// DroppedTraceCounter is suitable for AsyncTraceExporter. Its fixed label tuple
// never depends on a span or request identity.
func (m *Metrics) DroppedTraceCounter() prometheus.Counter {
	if m == nil {
		return nil
	}
	collector := m.collectors[SignalTelemetryDropped]
	return collector.counter.WithLabelValues(metricUnknown, metricUnknown, metricUnknown, "collector_failed")
}

func metricAllowlist(name string, values []string) ([]string, error) {
	if len(values) > 256 {
		return nil, telemetryError(name + " allowlist exceeds 256 entries")
	}
	values = slices.Clone(values)
	slices.Sort(values)
	for index, value := range values {
		if !validIdentity(value) || value == metricUnknown || (index > 0 && value == values[index-1]) {
			return nil, telemetryError(name + " allowlist contains an invalid or duplicate identity")
		}
	}
	return append(values, metricUnknown), nil
}

func toSet(values []string) map[string]struct{} {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func collapse(value string, allowlist map[string]struct{}) string {
	if _, exists := allowlist[value]; exists {
		return value
	}
	return metricUnknown
}

func signalSeriesWeight() int {
	weight := 0
	for _, kind := range signalKinds {
		if kind == kindHistogram {
			// Explicit buckets, +Inf, sum, and count are distinct Prometheus series.
			weight += len(histogramBuckets) + 3
			continue
		}
		weight++
	}
	return weight
}

func boundedSeries(dimensions ...int) (int, bool) {
	value := 1
	for _, dimension := range dimensions {
		if dimension < 1 || value > math.MaxInt/dimension {
			return 0, false
		}
		value *= dimension
	}
	return value, true
}
