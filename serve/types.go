// Package serve exposes the bounded read-only market-data HTTP boundary.
package serve

import (
	"context"
	"errors"
	"time"

	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/warehouse"
)

const (
	DefaultPageRows            = 1_000
	MaximumPageRows            = 10_000
	MaximumResponseBytes       = int64(16 << 20)
	MaximumRequestBytes        = int64(1 << 20)
	MaximumQueryInterval       = 24 * time.Hour
	MaximumPrincipals          = 256
	MaximumSecretBytes         = 64 << 10
	MinimumBearerBytes         = 16
	MaximumBearerBytes         = 8 << 10
	MaximumMetrics             = 10_000
	ReplayTrailerTruncated     = "X-Replay-Truncated"
	ReplayTrailerTerminalError = "X-Replay-Terminal-Error"
)

var (
	ErrConfiguration    = errors.New("serve: invalid configuration")
	ErrAuthentication   = errors.New("serve: authentication denied")
	ErrAuthorization    = errors.New("serve: authorization denied")
	ErrQueryRequest     = errors.New("serve: invalid declarative request")
	ErrPageToken        = errors.New("serve: page token denied")
	ErrQueueFull        = errors.New("serve: request queue full")
	ErrResponseTooLarge = errors.New("serve: response row exceeds byte bound")
	ErrShuttingDown     = errors.New("serve: shutting down")
)

type Scope string

const (
	ScopeCatalogRead      Scope = "catalog:read"
	ScopeCoverageRead     Scope = "coverage:read"
	ScopeQueryRead        Scope = "query:read"
	ScopeReplayNative     Scope = "replay:native"
	ScopeReplayNormalized Scope = "replay:normalized"
	ScopeMetricsRead      Scope = "metrics:read"
)

type Principal struct {
	ID       string
	TokenRef string
	Scopes   []Scope
}

// SecretResolver returns a fresh caller-owned byte slice on every call. The
// server clears that slice immediately after deriving its runtime state.
type SecretResolver interface {
	Resolve(context.Context, string) ([]byte, error)
}

type RouteLimits struct {
	QueueDepth  int
	Concurrency int
	Deadline    time.Duration
	MaxDuration time.Duration
	MaxBytes    int64
	BufferBytes int
}

type Config struct {
	TLSCertRef        string
	TLSKeyRef         string
	PagingKeyRef      string
	Principals        []Principal
	MaxQueryInterval  time.Duration
	DefaultPageRows   int
	MaxPageRows       int
	MaxResponseBytes  int64
	PageTokenTTL      time.Duration
	ReadHeaderTimeout time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	IdleTimeout       time.Duration
	Catalog           RouteLimits
	Query             RouteLimits
	NativeReplay      RouteLimits
	NormalizedReplay  RouteLimits
	Now               func() time.Time
}

func DefaultConfig() Config {
	return Config{
		MaxQueryInterval: MaximumQueryInterval, DefaultPageRows: DefaultPageRows, MaxPageRows: MaximumPageRows,
		MaxResponseBytes: MaximumResponseBytes, PageTokenTTL: 15 * time.Minute,
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 5 * time.Minute,
		IdleTimeout:      2 * time.Minute,
		Catalog:          RouteLimits{QueueDepth: 16, Concurrency: 4, Deadline: 10 * time.Second, MaxDuration: 10 * time.Second, MaxBytes: MaximumResponseBytes, BufferBytes: 32 << 10},
		Query:            RouteLimits{QueueDepth: 32, Concurrency: 8, Deadline: 30 * time.Second, MaxDuration: 30 * time.Second, MaxBytes: MaximumResponseBytes, BufferBytes: 64 << 10},
		NativeReplay:     RouteLimits{QueueDepth: 16, Concurrency: 4, Deadline: 5 * time.Minute, MaxDuration: 5 * time.Minute, MaxBytes: 256 << 20, BufferBytes: 64 << 10},
		NormalizedReplay: RouteLimits{QueueDepth: 16, Concurrency: 4, Deadline: 5 * time.Minute, MaxDuration: 5 * time.Minute, MaxBytes: 256 << 20, BufferBytes: 32 << 10},
		Now:              time.Now,
	}
}

type CatalogSource struct {
	SourceID      string `json:"source_id"`
	Venue         string `json:"venue"`
	ProductFamily string `json:"product_family"`
	APIFamily     string `json:"api_family"`
	Environment   string `json:"environment"`
	Lifecycle     string `json:"lifecycle"`
}

type CatalogInstrument struct {
	InstrumentUID     string `json:"instrument_uid"`
	SourceID          string `json:"source_id"`
	NativeID          string `json:"native_id"`
	ListingGeneration int64  `json:"listing_generation"`
	Lifecycle         string `json:"lifecycle"`
	BaseAsset         string `json:"base_asset"`
	QuoteAsset        string `json:"quote_asset"`
	MarginAsset       string `json:"margin_asset,omitzero"`
	SettlementAsset   string `json:"settlement_asset,omitzero"`
	Kind              string `json:"kind"`
	Multiplier        string `json:"multiplier"`
}

type Coverage struct {
	ID                  string          `json:"id"`
	Tuple               warehouse.Tuple `json:"tuple"`
	StartReceivedTimeNS int64           `json:"start_received_time_ns"`
	EndReceivedTimeNS   int64           `json:"end_received_time_ns"`
	State               string          `json:"state"`
	CatalogSnapshotID   string          `json:"catalog_snapshot_id"`
	DatasetID           string          `json:"dataset_id"`
}

type Incident struct {
	ID                  string          `json:"id"`
	Tuple               warehouse.Tuple `json:"tuple"`
	StartReceivedTimeNS int64           `json:"start_received_time_ns"`
	EndReceivedTimeNS   int64           `json:"end_received_time_ns"`
	Kind                string          `json:"kind"`
	GapRefID            string          `json:"gap_ref_id"`
	CatalogSnapshotID   string          `json:"catalog_snapshot_id"`
}

type CatalogDataset struct {
	DatasetID         string `json:"dataset_id"`
	Family            string `json:"family"`
	CatalogSnapshotID string `json:"catalog_snapshot_id"`
	SchemaName        string `json:"schema_name"`
	SchemaVersion     uint16 `json:"schema_version"`
	Committed         bool   `json:"committed"`
}

type MetadataReader interface {
	Sources(context.Context) ([]CatalogSource, error)
	Instruments(context.Context) ([]CatalogInstrument, error)
	Coverage(context.Context) ([]Coverage, error)
	Incidents(context.Context) ([]Incident, error)
	Datasets(context.Context) ([]CatalogDataset, error)
	Ready() bool
}

type DatasetResolver interface {
	Dataset(context.Context, string) (warehouse.Dataset, error)
	LatestDataset(context.Context, string) (warehouse.Dataset, error)
}

type QueryPager interface {
	Page(context.Context, warehouse.QuerySpec) (warehouse.Page, error)
}

type MetricType string

const (
	MetricCounter MetricType = "counter"
	MetricGauge   MetricType = "gauge"
)

type Metric struct {
	Name  string
	Help  string
	Type  MetricType
	Value float64
}

type MetricsReader interface {
	Metrics(context.Context) ([]Metric, error)
}

type Dependencies struct {
	Metadata   MetadataReader
	Datasets   DatasetResolver
	Query      QueryPager
	Native     replay.NativeOpener
	Normalized replay.NormalizedOpener
	Metrics    MetricsReader
}

type QueryRequest struct {
	DatasetID           string   `json:"dataset_id"`
	Family              string   `json:"family"`
	SourceIDs           []string `json:"source_ids"`
	ChannelIDs          []string `json:"channel_ids"`
	InstrumentUIDs      []string `json:"instrument_uids"`
	StartReceivedTimeNS int64    `json:"start_received_time_ns"`
	EndReceivedTimeNS   int64    `json:"end_received_time_ns"`
	Limit               int      `json:"limit"`
	PageToken           string   `json:"page_token"`
}

type ReplayRequest struct {
	DatasetID           string   `json:"dataset_id"`
	Family              string   `json:"family"`
	SourceIDs           []string `json:"source_ids"`
	ChannelIDs          []string `json:"channel_ids"`
	InstrumentUIDs      []string `json:"instrument_uids"`
	StartReceivedTimeNS int64    `json:"start_received_time_ns"`
	EndReceivedTimeNS   int64    `json:"end_received_time_ns"`
}

type QueryResponse struct {
	DatasetID         string                        `json:"dataset_id"`
	CatalogSnapshotID string                        `json:"catalog_snapshot_id"`
	SchemaName        string                        `json:"schema_name"`
	SchemaVersion     uint16                        `json:"schema_version"`
	Records           []warehouse.QueryRow          `json:"records"`
	Coverage          []warehouse.CoverageReference `json:"coverage"`
	Gaps              []warehouse.GapReference      `json:"gaps"`
	NextPageToken     string                        `json:"next_page_token,omitzero"`
}
