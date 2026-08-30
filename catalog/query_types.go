package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	RequiredQuerySchemaVersion = int64(7)
	MaximumMetadataResults     = 100_000
	MaximumRawSegmentResults   = 1_000_000
	MaximumDatasetLineage      = 1_000_000
	MaximumDatasetCoverage     = 32_768
	MaximumCheckpointBytes     = 1 << 20
)

var (
	ErrInvalidQueryProjection = errors.New("catalog: invalid query projection")
	ErrQueryNotFound          = errors.New("catalog: query projection not found")
	ErrQueryConflict          = errors.New("catalog: query projection conflict")
	ErrQueryBound             = errors.New("catalog: query result exceeds bound")
	ErrQueryNotReady          = errors.New("catalog: query store not ready")
)

// QueryDatabase is implemented by pgx.Conn and pgxpool.Pool. A pool is
// recommended because all QueryStore reads are safe to execute concurrently.
type QueryDatabase interface {
	Begin(context.Context) (pgx.Tx, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type QueryStore struct {
	database         QueryDatabase
	readinessTimeout time.Duration
	ready            atomic.Bool
}

// NewQueryStore proves schema compatibility and access to every required
// projection before returning. readinessTimeout remains the explicit bound for
// subsequent Ready probes, whose interface cannot carry a caller context.
func NewQueryStore(ctx context.Context, database QueryDatabase, readinessTimeout time.Duration) (*QueryStore, error) {
	if database == nil || readinessTimeout <= 0 {
		return nil, fmt.Errorf("%w: database and positive readiness timeout are required", ErrInvalidQueryProjection)
	}
	store := &QueryStore{database: database, readinessTimeout: readinessTimeout}
	if err := store.CheckReady(ctx); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *QueryStore) Ready() bool {
	if s == nil || s.database == nil || s.readinessTimeout <= 0 {
		return false
	}
	ctx, cancel := context.WithTimeout(context.Background(), s.readinessTimeout)
	defer cancel()
	return s.CheckReady(ctx) == nil
}

func (s *QueryStore) CheckReady(ctx context.Context) error {
	if s == nil || s.database == nil {
		return ErrQueryNotReady
	}
	var version int64
	var storesAvailable bool
	err := s.database.QueryRow(ctx, `
SELECT version_id,
       to_regclass('source') IS NOT NULL
       AND to_regclass('instrument') IS NOT NULL
       AND to_regclass('raw_segment') IS NOT NULL
       AND to_regclass('raw_segment_manifest') IS NOT NULL
       AND to_regclass('dataset_partition') IS NOT NULL
       AND to_regclass('dataset_partition_segment') IS NOT NULL
       AND to_regclass('dataset_manifest') IS NOT NULL
       AND to_regclass('dataset_generation') IS NOT NULL
       AND to_regclass('dataset_coverage') IS NOT NULL
       AND to_regclass('runtime_checkpoint') IS NOT NULL
       AND to_regclass('gap') IS NOT NULL
       AND to_regclass('incident') IS NOT NULL
FROM goose_db_version
WHERE is_applied
ORDER BY id DESC
LIMIT 1
`).Scan(&version, &storesAvailable)
	if err != nil || version != RequiredQuerySchemaVersion || !storesAvailable {
		s.ready.Store(false)
		if err != nil {
			return fmt.Errorf("%w: schema probe: %v", ErrQueryNotReady, err)
		}
		return fmt.Errorf("%w: schema version %d or required stores", ErrQueryNotReady, version)
	}
	// These zero-row subqueries prove SELECT access rather than merely table
	// existence. They remain bounded and do not inspect caller data.
	var accessProbe int
	err = s.database.QueryRow(ctx, `
SELECT
    (SELECT count(*) FROM source WHERE false)
  + (SELECT count(*) FROM instrument WHERE false)
  + (SELECT count(*) FROM raw_segment WHERE false)
  + (SELECT count(*) FROM raw_segment_manifest WHERE false)
  + (SELECT count(*) FROM dataset_partition WHERE false)
  + (SELECT count(*) FROM dataset_partition_segment WHERE false)
  + (SELECT count(*) FROM dataset_manifest WHERE false)
  + (SELECT count(*) FROM dataset_generation WHERE false)
  + (SELECT count(*) FROM dataset_coverage WHERE false)
  + (SELECT count(*) FROM runtime_checkpoint WHERE false)
  + (SELECT count(*) FROM gap WHERE false)
  + (SELECT count(*) FROM incident WHERE false)
`).Scan(&accessProbe)
	if err != nil || accessProbe != 0 {
		s.ready.Store(false)
		if err != nil {
			return fmt.Errorf("%w: required store access: %v", ErrQueryNotReady, err)
		}
		return fmt.Errorf("%w: invalid required store probe", ErrQueryNotReady)
	}
	s.ready.Store(true)
	return nil
}

func (s *QueryStore) unavailable(operation string, err error) error {
	if s != nil {
		s.ready.Store(false)
	}
	return fmt.Errorf("catalog: %s: %w", operation, err)
}

type SourceProjection struct {
	SourceID      string `json:"source_id"`
	Venue         string `json:"venue"`
	ProductFamily string `json:"product_family"`
	APIFamily     string `json:"api_family"`
	Environment   string `json:"environment"`
	Lifecycle     string `json:"lifecycle"`
}

type InstrumentProjection struct {
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

type TupleProjection struct {
	SourceID      string `json:"source_id"`
	ChannelID     string `json:"channel_id"`
	InstrumentUID string `json:"instrument_uid"`
}

type CoverageProjection struct {
	ID                  string          `json:"id"`
	Tuple               TupleProjection `json:"tuple"`
	StartReceivedTimeNS int64           `json:"start_received_time_ns"`
	EndReceivedTimeNS   int64           `json:"end_received_time_ns"`
	State               string          `json:"state"`
	CatalogSnapshotID   string          `json:"catalog_snapshot_id"`
	DatasetID           string          `json:"dataset_id"`
}

type IncidentProjection struct {
	ID                  string          `json:"id"`
	Tuple               TupleProjection `json:"tuple"`
	StartReceivedTimeNS int64           `json:"start_received_time_ns"`
	EndReceivedTimeNS   int64           `json:"end_received_time_ns"`
	Kind                string          `json:"kind"`
	GapRefID            string          `json:"gap_ref_id"`
	CatalogSnapshotID   string          `json:"catalog_snapshot_id"`
}

type DatasetProjection struct {
	DatasetID         string `json:"dataset_id"`
	Family            string `json:"family"`
	CatalogSnapshotID string `json:"catalog_snapshot_id"`
	SchemaName        string `json:"schema_name"`
	SchemaVersion     uint16 `json:"schema_version"`
	Committed         bool   `json:"committed"`
}

type DatasetState string

const (
	DatasetPending     DatasetState = "pending"
	DatasetVerified    DatasetState = "verified"
	DatasetCommitted   DatasetState = "committed"
	DatasetQuarantined DatasetState = "quarantined"
	DatasetSuperseded  DatasetState = "superseded"
)

type DatasetPublication struct {
	DatasetID           string
	DatasetFamily       string
	DatasetVersion      string
	SourceID            string
	SchemaName          string
	SchemaVersion       uint16
	ManifestVersion     uint16
	PartitionKey        string
	RangeStartNS        int64
	RangeEndNS          int64
	InputSegmentSetHash [sha256.Size]byte
	CatalogSnapshotHash [sha256.Size]byte
	MapperSetHash       [sha256.Size]byte
	LogicalHash         [sha256.Size]byte
	PhysicalHash        [sha256.Size]byte
	ParquetObjectKey    string
	ManifestObjectKey   string
	ParquetBytes        int64
	ManifestHash        [sha256.Size]byte
	ManifestBytes       []byte
	State               DatasetState
	InputSegmentIDs     []string
	Coverage            []DatasetCoverage
}

func (p DatasetPublication) clone() DatasetPublication {
	p.ManifestBytes = bytes.Clone(p.ManifestBytes)
	p.InputSegmentIDs = slices.Clone(p.InputSegmentIDs)
	p.Coverage = slices.Clone(p.Coverage)
	return p
}

type DatasetIdentity struct {
	ID                [sha256.Size]byte
	Family            string
	CatalogSnapshotID [sha256.Size]byte
	SchemaName        string
	SchemaVersion     uint16
}

func (d DatasetIdentity) IDString() string { return fmt.Sprintf("%x", d.ID) }

func (d DatasetIdentity) CatalogSnapshotIDString() string {
	return fmt.Sprintf("%x", d.CatalogSnapshotID)
}

type DatasetManifest struct {
	Dataset        DatasetIdentity
	ManifestPath   string
	ManifestSHA256 [sha256.Size]byte
	State          DatasetState
}

type DatasetLineageEntry struct {
	DatasetID    string
	RawSegmentID string
	InputOrdinal int
}

type DatasetCoverage struct {
	ID                  string
	Tuple               TupleProjection
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
	State               string
}

type DatasetGenerationCommit struct {
	DatasetID          string
	GenerationID       [sha256.Size]byte
	ManifestHash       [sha256.Size]byte
	InputHash          [sha256.Size]byte
	ExpectedEventCount uint64
	ExpectedRowCount   uint64
	Family             string
	CatalogSnapshotID  [sha256.Size]byte
	SchemaName         string
	SchemaVersion      uint16
}

type RawSegmentFilter struct {
	DatasetID           string
	SourceIDs           []string
	ChannelIDs          []string
	InstrumentUIDs      []string
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
	Limit               int
	MaxManifestBytes    int64
}

type RuntimeCheckpoint struct {
	Key            string
	SourceID       string
	ChannelID      string
	InstrumentUID  string
	ReceivedTimeNS int64
	StreamEpochID  string
	ArrivalOrdinal uint64
	MessageOrdinal uint32
	StateSHA256    [sha256.Size]byte
	StateBytes     []byte
	UpdatedAt      time.Time
}

func (c RuntimeCheckpoint) clone() RuntimeCheckpoint {
	c.StateBytes = bytes.Clone(c.StateBytes)
	return c
}

type GapProjection struct {
	ID                  string
	Tuple               TupleProjection
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
	Kind                string
	State               string
	DetectedTimeNS      int64
	ResolvedTimeNS      int64
}

type ReferenceFilter struct {
	DatasetID           string
	SourceIDs           []string
	ChannelIDs          []string
	InstrumentUIDs      []string
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
	Limit               int
}

type CoverageReferenceProjection struct {
	ID                  string
	Tuple               TupleProjection
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
	State               string
}

type GapReferenceProjection struct {
	ID                  string
	Tuple               TupleProjection
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
	Kind                string
}

type GapFilter struct {
	SourceIDs           []string
	ChannelIDs          []string
	InstrumentUIDs      []string
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
	Limit               int
}

func validQueryText(value string) bool {
	return value != "" && len(value) <= 4096 && !strings.ContainsRune(value, 0)
}

func validateSortedQueryIDs(name string, values []string, required bool, maximum int) error {
	if (required && len(values) == 0) || len(values) > maximum || !slices.IsSorted(values) {
		return fmt.Errorf("%w: %s must be sorted and contain at most %d values", ErrInvalidQueryProjection, name, maximum)
	}
	for index, value := range values {
		if !validQueryText(value) || (index > 0 && value == values[index-1]) {
			return fmt.Errorf("%w: %s contains an invalid or duplicate value", ErrInvalidQueryProjection, name)
		}
	}
	return nil
}

func nonzeroHash(value [sha256.Size]byte) bool { return value != [sha256.Size]byte{} }

func rollbackQueryTx(ctx context.Context, tx pgx.Tx, errp *error) {
	if rollbackErr := tx.Rollback(context.WithoutCancel(ctx)); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
		*errp = errors.Join(*errp, rollbackErr)
	}
}

func queryConflict(operation string) error {
	return fmt.Errorf("%w: %s", ErrQueryConflict, operation)
}

func queryNotFound(operation string) error {
	return fmt.Errorf("%w: %s", ErrQueryNotFound, operation)
}

func commandChangedOne(command pgconn.CommandTag) bool { return command.RowsAffected() == 1 }
