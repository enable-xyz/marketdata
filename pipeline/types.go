package pipeline

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/dataset"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/warehouse"
)

const (
	MaximumExportSegments   = 1_000_000
	MaximumExportRecords    = 4_000_000
	MaximumExportRows       = 4_000_000
	MaximumExportPartitions = 32_768
	MaximumLoadCoverage     = 32_768
)

var (
	ErrInvalidPipeline      = errors.New("pipeline: invalid input")
	ErrPipelineBound        = errors.New("pipeline: configured bound exceeded")
	ErrRawSelection         = errors.New("pipeline: committed raw selection mismatch")
	ErrEmptyExport          = errors.New("pipeline: selected range has no normalized records")
	ErrReplayDiscontinuity  = errors.New("pipeline: replay integrity discontinuity")
	ErrPublicationConflict  = errors.New("pipeline: dataset publication conflict")
	ErrDatasetNotCommitted  = errors.New("pipeline: dataset is not committed")
	ErrMaterializedConflict = errors.New("pipeline: local materialization conflict")
)

// RawCatalog is the sole raw-authority boundary consumed by Exporter. The
// concrete catalog.PublicationStore satisfies it.
type RawCatalog interface {
	StreamCommittedRawSegments(context.Context, func(catalog.RawSegmentPublication) error) error
}

// ExportCatalog owns the verified-to-committed dataset lifecycle. Objects are
// made immutable and verified before RecordVerifiedDataset is called.
type ExportCatalog interface {
	FindDataset(context.Context, string) (catalog.DatasetPublication, bool, error)
	RecordVerifiedDataset(context.Context, catalog.DatasetPublication) error
	CommitDataset(context.Context, string) error
}

// LoadCatalog exposes only the committed dataset and warehouse-generation
// transitions required by Loader.
type LoadCatalog interface {
	FindDataset(context.Context, string) (catalog.DatasetPublication, bool, error)
	CommitDatasetGeneration(context.Context, catalog.DatasetGenerationCommit) error
}

// Normalizer is deliberately defined by its consumer. normalize.Orchestrator
// satisfies it without learning about pipeline orchestration.
type Normalizer interface {
	Normalize([]normalize.RawRecord) (normalize.Batch, error)
}

// WarehouseLoader is the narrow existing generation lifecycle used by Loader.
// warehouse.Loader satisfies it.
type WarehouseLoader interface {
	Load(context.Context, warehouse.CommittedManifest) (warehouse.LoadReceipt, error)
}

type ExporterConfig struct {
	Replay            replay.Config
	Writer            dataset.WriterOptions
	Verify            objectstore.VerifyPolicy
	Reconciler        objectstore.ImmutableCreateReconciler
	DatasetPolicyID   normalize.Hash
	ReplayConfigID    normalize.Hash
	CatalogSnapshotID normalize.Hash
	MapperSetID       normalize.Hash
	MaxSegments       int
	MaxRecords        uint64
	MaxOutputRows     uint64
	NormalizeBatch    int
	MaxPartitions     int
}

type ExportRequest struct {
	SourceID            string
	SegmentIDs          []string
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
	BuildRoot           string
	ObjectPrefix        string
}

type RawInputReceipt struct {
	SegmentID      string
	SourceID       string
	ChannelID      string
	EpochID        string
	ObjectKey      string
	OrdinalStart   uint64
	OrdinalEnd     uint64
	ContentSHA256  [sha256.Size]byte
	ManifestSHA256 [sha256.Size]byte
}

type ReplayReceipt struct {
	Order              replay.OrderLabel
	LogicalHashVersion uint16
	LogicalHash        [sha256.Size]byte
	EventCount         uint64
	RecordCount        uint64
	NormalizedRows     uint64
	QuarantinedRows    uint64
	Discontinuities    []replay.Discontinuity
}

type ObjectReceipt struct {
	Key       string
	SHA256    [sha256.Size]byte
	Bytes     int64
	Recovered bool
}

type DatasetReceipt struct {
	DatasetID       string
	Family          string
	SchemaName      string
	SchemaVersion   uint16
	PartitionKey    string
	ManifestHash    [sha256.Size]byte
	PhysicalHash    [sha256.Size]byte
	ParquetObject   ObjectReceipt
	ManifestObject  ObjectReceipt
	CatalogState    catalog.DatasetState
	ReusedCommitted bool
}

type ExportReceipt struct {
	SourceID           string
	InputManifestSetID [sha256.Size]byte
	Inputs             []RawInputReceipt
	Replay             ReplayReceipt
	Datasets           []DatasetReceipt
	Complete           bool
}

type LoaderConfig struct {
	MaxParquetBytes int64
	MaxCoverage     int
}

type LoadRequest struct {
	DatasetID string
	WorkRoot  string
}

type MaterializationReceipt struct {
	Root           string
	ManifestPath   string
	ParquetPath    string
	ManifestObject ObjectReceipt
	ParquetObject  ObjectReceipt
}

type LoadReceipt struct {
	DatasetID       string
	Materialization MaterializationReceipt
	Warehouse       warehouse.LoadReceipt
	CoverageCount   int
	GenerationBound bool
	Complete        bool
}

func validateExporterConfig(config ExporterConfig) (ExporterConfig, error) {
	if config.DatasetPolicyID == (normalize.Hash{}) || config.ReplayConfigID == (normalize.Hash{}) ||
		config.CatalogSnapshotID == (normalize.Hash{}) || config.MapperSetID == (normalize.Hash{}) {
		return ExporterConfig{}, fmt.Errorf("%w: immutable dataset, replay, catalog, and mapper identities are required", ErrInvalidPipeline)
	}
	if config.MaxSegments < 1 || config.MaxSegments > MaximumExportSegments ||
		config.MaxRecords < 1 || config.MaxRecords > MaximumExportRecords ||
		config.MaxOutputRows < 1 || config.MaxOutputRows > MaximumExportRows ||
		config.NormalizeBatch < 1 || config.NormalizeBatch > normalize.MaxNormalizationBatch ||
		config.MaxPartitions < 1 || config.MaxPartitions > MaximumExportPartitions {
		return ExporterConfig{}, fmt.Errorf("%w: export limits", ErrInvalidPipeline)
	}
	defaults := dataset.DefaultWriterOptions(config.DatasetPolicyID, config.ReplayConfigID, normalize.Hash{1})
	if config.Writer.RowGroupTargetBytes == 0 {
		config.Writer.RowGroupTargetBytes = defaults.RowGroupTargetBytes
	}
	if config.Writer.PageBufferBytes == 0 {
		config.Writer.PageBufferBytes = defaults.PageBufferBytes
	}
	if config.Writer.Compression == "" {
		config.Writer.Compression = defaults.Compression
	}
	if config.Writer.MaxInputRows == 0 {
		config.Writer.MaxInputRows = defaults.MaxInputRows
	}
	if config.Writer.MaxParquetRows == 0 {
		config.Writer.MaxParquetRows = defaults.MaxParquetRows
	}
	config.Writer.DatasetPolicyID = config.DatasetPolicyID
	config.Writer.ReplayConfigID = config.ReplayConfigID
	return config, nil
}

func validateLoaderConfig(config LoaderConfig) (LoaderConfig, error) {
	if config.MaxParquetBytes < 1 || config.MaxParquetBytes > dataset.MaxPartitionFileBytes ||
		config.MaxCoverage < 1 || config.MaxCoverage > MaximumLoadCoverage {
		return LoaderConfig{}, fmt.Errorf("%w: load limits", ErrInvalidPipeline)
	}
	return config, nil
}
