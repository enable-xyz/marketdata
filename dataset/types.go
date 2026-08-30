package dataset

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

const (
	ManifestVersion            uint16 = 1
	QualitySchemaVersion       uint16 = 1
	DatasetVersion                    = "parquet-dataset-v1"
	ParquetWriterVersion              = "parquet-go/v0.32.0"
	ParquetFormatCompatibility        = "apache-parquet-format/2.13.0"
	DefaultRowGroupBytes       int64  = 256 << 20
	DefaultPageBufferBytes            = 256 << 10
	MaxPartitionInputRows      uint64 = 4_000_000
	MaxPartitionParquetRows    uint64 = 20_000_000
	MaxPartitionFileBytes      int64  = 256 << 30
	MaxManifestBytes                  = 1 << 20
	MaxDatasetStringBytes             = 4096
	MaxQualityFlags                   = 64
	MaxPageValues              int64  = 64 << 20
)

var (
	ErrInvalidInput      = errors.New("dataset: invalid input")
	ErrPartitionBoundary = errors.New("dataset: row crosses partition boundary")
	ErrCorruptDataset    = errors.New("dataset: corrupt parquet dataset")
	ErrManifestMismatch  = errors.New("dataset: manifest mismatch")
)

type Family string

const (
	FamilyTrade            Family = "trade"
	FamilyBookUpdate       Family = "book_update"
	FamilyQuote            Family = "quote"
	FamilyTicker           Family = "ticker"
	FamilySchemaQuarantine Family = "schema_quarantine"
	FamilyQuality          Family = "quality"
	FamilyOpportunity      Family = "opportunity"
)

type WriterOptions struct {
	RowGroupTargetBytes int64
	PageBufferBytes     int
	Compression         string
	Dictionary          bool
	BloomFilter         bool
	MaxInputRows        uint64
	MaxParquetRows      uint64
	DatasetPolicyID     normalize.Hash
	ReplayConfigID      normalize.Hash
	InputManifestSetID  normalize.Hash
	DirectorySync       func(string) error
}

func DefaultWriterOptions(datasetPolicyID, replayConfigID, inputManifestSetID normalize.Hash) WriterOptions {
	return WriterOptions{
		RowGroupTargetBytes: DefaultRowGroupBytes,
		PageBufferBytes:     DefaultPageBufferBytes,
		Compression:         "zstd",
		MaxInputRows:        MaxPartitionInputRows,
		MaxParquetRows:      MaxPartitionParquetRows,
		DatasetPolicyID:     datasetPolicyID,
		ReplayConfigID:      replayConfigID,
		InputManifestSetID:  inputManifestSetID,
		DirectorySync:       syncDirectory,
	}
}

func (o WriterOptions) validate() error {
	if o.RowGroupTargetBytes != 64<<20 && o.RowGroupTargetBytes != 256<<20 && o.RowGroupTargetBytes != 1024<<20 {
		return fmt.Errorf("%w: row-group target must be 64, 256, or 1024 MiB", ErrInvalidInput)
	}
	if o.PageBufferBytes < 64<<10 || o.PageBufferBytes > 4<<20 || o.Compression != "zstd" {
		return fmt.Errorf("%w: unsupported page/compression option", ErrInvalidInput)
	}
	if o.MaxInputRows == 0 || o.MaxInputRows > MaxPartitionInputRows || o.MaxParquetRows == 0 || o.MaxParquetRows > MaxPartitionParquetRows {
		return fmt.Errorf("%w: row bound", ErrInvalidInput)
	}
	if o.DatasetPolicyID == (normalize.Hash{}) || o.ReplayConfigID == (normalize.Hash{}) || o.InputManifestSetID == (normalize.Hash{}) {
		return fmt.Errorf("%w: immutable policy/config/input identity is required", ErrInvalidInput)
	}
	return nil
}

type ManifestOptions struct {
	RowGroupTargetBytes int64  `json:"row_group_target_bytes"`
	PageBufferBytes     int    `json:"page_buffer_bytes"`
	Compression         string `json:"compression"`
	Dictionary          bool   `json:"dictionary"`
	BloomFilter         bool   `json:"bloom_filter"`
}

type ManifestEpoch struct {
	Kind                string `json:"kind"`
	ID                  string `json:"id"`
	FirstReceivedTimeNS int64  `json:"first_received_time_ns"`
	FirstArrivalOrdinal uint64 `json:"first_arrival_ordinal"`
	LastArrivalOrdinal  uint64 `json:"last_arrival_ordinal"`
}

type Manifest struct {
	ManifestVersion            uint16          `json:"manifest_version"`
	DatasetVersion             string          `json:"dataset_version"`
	ParquetWriterVersion       string          `json:"parquet_writer_version"`
	ParquetFormatCompatibility string          `json:"parquet_format_compatibility"`
	SchemaName                 string          `json:"schema_name"`
	SchemaVersion              uint16          `json:"schema_version"`
	SchemaSHA256               string          `json:"schema_sha256"`
	Family                     Family          `json:"family"`
	SourceID                   string          `json:"source_id"`
	Epochs                     []ManifestEpoch `json:"epochs"`
	UTCDate                    string          `json:"utc_date"`
	UTCHour                    string          `json:"utc_hour"`
	FirstReceivedTimeNS        int64           `json:"first_received_time_ns"`
	LastReceivedTimeNS         int64           `json:"last_received_time_ns"`
	FirstArrivalOrdinal        uint64          `json:"first_arrival_ordinal"`
	LastArrivalOrdinal         uint64          `json:"last_arrival_ordinal"`
	InputRows                  uint64          `json:"input_rows"`
	ParquetRows                uint64          `json:"parquet_rows"`
	RowGroups                  uint64          `json:"row_groups"`
	Pages                      uint64          `json:"pages"`
	Values                     uint64          `json:"values"`
	LogicalSHA256              string          `json:"logical_sha256"`
	PhysicalSHA256             string          `json:"physical_sha256"`
	FileBytes                  int64           `json:"file_bytes"`
	ParquetFile                string          `json:"parquet_file"`
	BuildID                    string          `json:"build_id"`
	DatasetPolicyID            string          `json:"dataset_policy_id"`
	ReplayConfigID             string          `json:"replay_config_id"`
	InputManifestSetID         string          `json:"input_manifest_set_id"`
	Options                    ManifestOptions `json:"options"`
}

type BuildResult struct {
	Manifest     Manifest
	ManifestPath string
	ParquetPath  string
	ManifestHash [sha256.Size]byte
}

type Verification struct {
	Manifest       Manifest
	PhysicalSHA256 [sha256.Size]byte
	LogicalSHA256  [sha256.Size]byte
	SchemaSHA256   [sha256.Size]byte
	RowGroups      uint64
	Pages          uint64
	Values         uint64
}

type NormalizedSource interface {
	Next(context.Context) (normalize.Row, error)
}

type QuarantineSource interface {
	Next(context.Context) (normalize.SchemaQuarantineV1, error)
}

type QualitySource interface {
	Next(context.Context) (QualityRowV1, error)
}

type SliceNormalizedSource struct {
	Rows []normalize.Row
	next int
}

func (s *SliceNormalizedSource) Next(context.Context) (normalize.Row, error) {
	if s.next == len(s.Rows) {
		return normalize.Row{}, io.EOF
	}
	row := s.Rows[s.next]
	s.next++
	return row, nil
}

type SliceQuarantineSource struct {
	Rows []normalize.SchemaQuarantineV1
	next int
}

func (s *SliceQuarantineSource) Next(context.Context) (normalize.SchemaQuarantineV1, error) {
	if s.next == len(s.Rows) {
		return normalize.SchemaQuarantineV1{}, io.EOF
	}
	row := s.Rows[s.next]
	s.next++
	return row, nil
}

type SliceQualitySource struct {
	Rows []QualityRowV1
	next int
}

func (s *SliceQualitySource) Next(context.Context) (QualityRowV1, error) {
	if s.next == len(s.Rows) {
		return QualityRowV1{}, io.EOF
	}
	row := s.Rows[s.next]
	s.next++
	return row, nil
}

// QualityRowV1 is the bounded, payload-free portable quality event. It keeps
// raw lineage and immutable schema/policy identities without accepting an
// open-ended attributes map.
type QualityRowV1 struct {
	Version                 uint16
	QualityID               normalize.Hash
	Kind                    string
	Code                    string
	SourceState             normalize.SourceState
	SourceSchemaFingerprint normalize.Hash
	SchemaName              string
	SchemaVersion           uint16
	SourceID                string
	ChannelID               string
	InstrumentUID           string
	InstrumentUIDState      normalize.SourceState
	ReceivedTimeNS          int64
	Coordinate              normalize.RawCoordinate
	MapperVersion           string
	MapperBindingID         normalize.Hash
	CatalogSnapshotID       normalize.Hash
	PolicyID                normalize.Hash
	QualityFlags            []string
}

func (r QualityRowV1) Validate() error {
	if r.Version != QualitySchemaVersion || r.QualityID == (normalize.Hash{}) || r.SourceSchemaFingerprint == (normalize.Hash{}) ||
		r.MapperBindingID == (normalize.Hash{}) || r.CatalogSnapshotID == (normalize.Hash{}) || r.PolicyID == (normalize.Hash{}) ||
		!validSourceState(r.SourceState) || r.Kind == "" || r.Code == "" || r.SchemaName == "" || r.SchemaVersion == 0 || r.SourceID == "" || r.ChannelID == "" ||
		r.ReceivedTimeNS < 0 || r.Coordinate.SourceID != r.SourceID || r.Coordinate.ChannelID != r.ChannelID ||
		r.Coordinate.RawSegmentSHA256 == (normalize.Hash{}) || r.Coordinate.RawPayloadSHA256 == (normalize.Hash{}) ||
		r.Coordinate.ArrivalOrdinal == 0 || r.Coordinate.EpochID == ([16]byte{}) ||
		(r.Coordinate.EpochKind != normalize.ConnectionEpoch && r.Coordinate.EpochKind != normalize.PollCycleEpoch) ||
		(r.InstrumentUID == "" && r.InstrumentUIDState != normalize.SourceMissing) ||
		(r.InstrumentUID != "" && r.InstrumentUIDState != normalize.SourceValue) {
		return fmt.Errorf("%w: invalid quality row", ErrInvalidInput)
	}
	for _, value := range []string{r.Kind, r.Code, r.SchemaName, r.SourceID, r.ChannelID, r.InstrumentUID, r.MapperVersion} {
		if len(value) > MaxDatasetStringBytes || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%w: invalid quality string", ErrInvalidInput)
		}
	}
	if len(r.QualityFlags) > MaxQualityFlags || !slices.IsSorted(r.QualityFlags) {
		return fmt.Errorf("%w: quality flags must be bounded and sorted", ErrInvalidInput)
	}
	for i, flag := range r.QualityFlags {
		if flag == "" || len(flag) > MaxDatasetStringBytes || strings.IndexByte(flag, 0) >= 0 || (i > 0 && flag == r.QualityFlags[i-1]) {
			return fmt.Errorf("%w: invalid quality flag", ErrInvalidInput)
		}
	}
	return nil
}

func validSourceState(value normalize.SourceState) bool {
	switch value {
	case normalize.SourceMissing, normalize.SourceNull, normalize.SourceEmpty, normalize.SourceValue:
		return true
	default:
		return false
	}
}

func hashHex(value normalize.Hash) string { return hex.EncodeToString(value[:]) }

func decodeHash(text string) (normalize.Hash, error) {
	var value normalize.Hash
	decoded, err := hex.DecodeString(text)
	if err != nil || len(decoded) != len(value) {
		return value, fmt.Errorf("%w: invalid SHA-256", ErrManifestMismatch)
	}
	copy(value[:], decoded)
	return value, nil
}
