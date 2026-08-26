package warehouse

import (
	"cmp"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

const (
	SchemaVersion      uint16 = 1
	SchemaIdentity            = "enable-clickhouse-warehouse-v1"
	DefaultBatchRows          = 100_000
	MaxBatchRows              = 100_000
	MaxIdentityBytes          = 256
	MaxGenerationError        = 4_096
)

var (
	ErrInvalidWarehouseInput  = errors.New("warehouse: invalid input")
	ErrManifestNotCommitted   = errors.New("warehouse: parquet manifest is not committed")
	ErrGenerationConflict     = errors.New("warehouse: load generation conflicts with committed input")
	ErrWriteOutcomeUnknown    = errors.New("warehouse: write outcome unknown")
	ErrReconciliationFailed   = errors.New("warehouse: exact generation reconciliation failed")
	ErrMeasuredResultRequired = errors.New("warehouse: measured X5 result required")
)

type Hash [sha256.Size]byte

type EventID = Hash

type GenerationID = Hash

func ParseHash(value string) (Hash, error) {
	var result Hash
	if len(value) != hex.EncodedLen(len(result)) {
		return result, fmt.Errorf("%w: SHA-256 length", ErrInvalidWarehouseInput)
	}
	bytes, err := hex.DecodeString(value)
	if err != nil {
		return result, fmt.Errorf("%w: SHA-256 encoding", ErrInvalidWarehouseInput)
	}
	copy(result[:], bytes)
	return result, nil
}

func (h Hash) String() string {
	return hex.EncodeToString(h[:])
}

type ManifestState string

const (
	ManifestCommitted ManifestState = "committed"
)

// CommittedManifest is an explicit caller assertion about catalog visibility.
// The loader still verifies the manifest bytes, Parquet footer, physical hash,
// logical hash, schema, row counts, and the asserted manifest SHA-256 before it
// permits any ClickHouse write.
type CommittedManifest struct {
	Root           string
	ManifestPath   string
	ManifestSHA256 Hash
	State          ManifestState
}

func (m CommittedManifest) validate() error {
	if m.State != ManifestCommitted {
		return ErrManifestNotCommitted
	}
	if m.Root == "" || m.ManifestPath == "" || m.ManifestSHA256 == (Hash{}) {
		return fmt.Errorf("%w: committed manifest root, path, and hash are required", ErrInvalidWarehouseInput)
	}
	return nil
}

type PartitionLayout string

const (
	PartitionMonth PartitionLayout = "month"
	PartitionDate  PartitionLayout = "date"
)

func (l PartitionLayout) valid() bool {
	return l == PartitionMonth || l == PartitionDate
}

type Compression string

const (
	CompressionLZ4  Compression = "lz4"
	CompressionZstd Compression = "zstd"
)

func (c Compression) valid() bool {
	return c == CompressionLZ4 || c == CompressionZstd
}

type Config struct {
	BatchRows   int
	Compression Compression
	Layout      PartitionLayout
}

func DefaultConfig() Config {
	return Config{BatchRows: DefaultBatchRows, Compression: CompressionLZ4, Layout: PartitionMonth}
}

func (c Config) normalized() (Config, error) {
	defaults := DefaultConfig()
	c.BatchRows = cmp.Or(c.BatchRows, defaults.BatchRows)
	c.Compression = cmp.Or(c.Compression, defaults.Compression)
	c.Layout = cmp.Or(c.Layout, defaults.Layout)
	if c.BatchRows < 1 || c.BatchRows > MaxBatchRows {
		return Config{}, fmt.Errorf("%w: batch rows must be between 1 and %d", ErrInvalidWarehouseInput, MaxBatchRows)
	}
	if !c.Compression.valid() || !c.Layout.valid() {
		return Config{}, fmt.Errorf("%w: compression or partition layout", ErrInvalidWarehouseInput)
	}
	return c, nil
}

type GenerationState string

const (
	GenerationPending   GenerationState = "pending"
	GenerationUnknown   GenerationState = "unknown"
	GenerationCommitted GenerationState = "committed"
	GenerationFailed    GenerationState = "failed"
)

func (s GenerationState) valid() bool {
	switch s {
	case GenerationPending, GenerationUnknown, GenerationCommitted, GenerationFailed:
		return true
	default:
		return false
	}
}

// Decimal is the exact base-10 coefficient stored at one schema-fixed scale.
// It mirrors normalize.Decimal without coupling warehouse reconciliation to a
// mutable arbitrary-precision integer.
type Decimal struct {
	Coefficient string `json:"coefficient"`
	Scale       uint8  `json:"scale"`
}

func (d Decimal) validate(expectedScale uint8) error {
	if d.Scale != expectedScale || d.Coefficient == "" {
		return fmt.Errorf("%w: decimal scale or coefficient", ErrInvalidWarehouseInput)
	}
	start := 0
	if d.Coefficient[0] == '-' {
		start = 1
	}
	if start == len(d.Coefficient) || len(d.Coefficient)-start > 38 {
		return fmt.Errorf("%w: decimal coefficient width", ErrInvalidWarehouseInput)
	}
	for i := start; i < len(d.Coefficient); i++ {
		if d.Coefficient[i] < '0' || d.Coefficient[i] > '9' {
			return fmt.Errorf("%w: decimal coefficient", ErrInvalidWarehouseInput)
		}
	}
	return nil
}

type Row struct {
	GenerationID                GenerationID
	ManifestHash                Hash
	RowID                       Hash
	EventID                     EventID
	LogicalHash                 Hash
	Family                      string
	SourceID                    string
	ChannelID                   string
	InstrumentUID               string
	EpochKind                   string
	ConnectionEpoch             [16]byte
	ReceivedTimeNS              int64
	ArrivalOrdinal              uint64
	MessageOrdinal              uint32
	RawSegmentSHA256            Hash
	RawRecordOrdinal            uint64
	RawPayloadSHA256            Hash
	CatalogSnapshotID           Hash
	SchemaName                  string
	SchemaVersion               uint16
	DatasetPolicyID             Hash
	ReplayConfigID              Hash
	InputManifestSetID          Hash
	PhysicalOrdinal             uint64
	Price                       *Decimal
	Amount                      *Decimal
	BidPrice                    *Decimal
	BidAmount                   *Decimal
	AskPrice                    *Decimal
	AskAmount                   *Decimal
	PriceChange                 *Decimal
	PriceChangePercent          *Decimal
	WeightedAveragePrice        *Decimal
	FirstTradeBeforeWindowPrice *Decimal
	LastPrice                   *Decimal
	LastAmount                  *Decimal
	NativeBestBidPrice          *Decimal
	NativeBestBidAmount         *Decimal
	NativeBestAskPrice          *Decimal
	NativeBestAskAmount         *Decimal
	OpenPrice                   *Decimal
	HighPrice                   *Decimal
	LowPrice                    *Decimal
	BaseVolume                  *Decimal
	QuoteVolume                 *Decimal
}

func (r Row) validate() error {
	if r.GenerationID == (GenerationID{}) || r.ManifestHash == (Hash{}) || r.RowID == (Hash{}) || r.EventID == (EventID{}) ||
		r.LogicalHash == (Hash{}) || r.SourceID == "" || r.ChannelID == "" || r.Family == "" || r.EpochKind == "" ||
		r.ConnectionEpoch == ([16]byte{}) || r.ReceivedTimeNS <= 0 || r.RawSegmentSHA256 == (Hash{}) ||
		r.RawPayloadSHA256 == (Hash{}) || r.CatalogSnapshotID == (Hash{}) || r.SchemaName == "" || r.SchemaVersion == 0 ||
		r.DatasetPolicyID == (Hash{}) || r.ReplayConfigID == (Hash{}) || r.InputManifestSetID == (Hash{}) {
		return fmt.Errorf("%w: incomplete warehouse row identity", ErrInvalidWarehouseInput)
	}
	for _, value := range []*Decimal{r.Price, r.Amount, r.BidPrice, r.BidAmount, r.AskPrice, r.AskAmount,
		r.PriceChange, r.WeightedAveragePrice, r.FirstTradeBeforeWindowPrice, r.LastPrice, r.LastAmount,
		r.NativeBestBidPrice, r.NativeBestBidAmount, r.NativeBestAskPrice, r.NativeBestAskAmount, r.OpenPrice,
		r.HighPrice, r.LowPrice, r.BaseVolume, r.QuoteVolume} {
		if value != nil {
			if err := value.validate(18); err != nil {
				return err
			}
		}
	}
	if r.PriceChangePercent != nil {
		if err := r.PriceChangePercent.validate(8); err != nil {
			return err
		}
	}
	return nil
}

type Generation struct {
	ID                   GenerationID
	ServerDigest         string
	ManifestHash         Hash
	InputHash            Hash
	DatasetIdentity      Hash
	CatalogIdentity      Hash
	SchemaIdentity       Hash
	Family               string
	SourceID             string
	UTCDate              string
	PartitionValue       uint32
	Layout               PartitionLayout
	ExpectedEventIDs     []EventID
	ExpectedEventSetHash Hash
	ExpectedEventCount   uint64
	ExpectedRowCount     uint64
	State                GenerationState
	LastError            string
	CreatedAt            time.Time
	UpdatedAt            time.Time
}

func (g Generation) validate() error {
	if g.ID == (GenerationID{}) || g.ServerDigest == "" || len(g.ServerDigest) > MaxIdentityBytes || strings.IndexByte(g.ServerDigest, 0) >= 0 ||
		g.ManifestHash == (Hash{}) || g.InputHash == (Hash{}) || g.DatasetIdentity == (Hash{}) ||
		g.CatalogIdentity == (Hash{}) || g.SchemaIdentity == (Hash{}) || g.Family == "" || g.SourceID == "" ||
		g.UTCDate == "" || g.PartitionValue == 0 || !g.Layout.valid() || g.ExpectedEventSetHash == (Hash{}) ||
		g.ExpectedEventCount == 0 || g.ExpectedRowCount == 0 || !g.State.valid() || len(g.LastError) > MaxGenerationError {
		return fmt.Errorf("%w: incomplete load generation", ErrInvalidWarehouseInput)
	}
	if uint64(len(g.ExpectedEventIDs)) != g.ExpectedEventCount || !slices.IsSortedFunc(g.ExpectedEventIDs, compareHash) {
		return fmt.Errorf("%w: expected event-ID set order or count", ErrInvalidWarehouseInput)
	}
	for i := 1; i < len(g.ExpectedEventIDs); i++ {
		if g.ExpectedEventIDs[i] == g.ExpectedEventIDs[i-1] {
			return fmt.Errorf("%w: duplicate expected event ID", ErrInvalidWarehouseInput)
		}
	}
	if eventSetHash(g.ExpectedEventIDs) != g.ExpectedEventSetHash {
		return fmt.Errorf("%w: expected event-ID set hash", ErrInvalidWarehouseInput)
	}
	return nil
}

func (g Generation) record() Generation {
	g.ExpectedEventIDs = nil
	return g
}

type LoadReceipt struct {
	GenerationID       GenerationID
	ManifestHash       Hash
	InputHash          Hash
	ExpectedEventCount uint64
	ExpectedRowCount   uint64
	Rebuilt            bool
	ReconciledUnknown  bool
}

type Partition struct {
	Layout PartitionLayout
	Value  uint32
}

func (p Partition) validate() error {
	if !p.Layout.valid() || p.Value == 0 {
		return fmt.Errorf("%w: partition", ErrInvalidWarehouseInput)
	}
	if p.Layout == PartitionMonth && (p.Value < 197001 || p.Value > 999912) {
		return fmt.Errorf("%w: month partition", ErrInvalidWarehouseInput)
	}
	if p.Layout == PartitionDate && (p.Value < 19700101 || p.Value > 99991231) {
		return fmt.Errorf("%w: date partition", ErrInvalidWarehouseInput)
	}
	return nil
}

func compareHash(left, right Hash) int {
	return strings.Compare(string(left[:]), string(right[:]))
}

func eventSetHash(ids []EventID) Hash {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("warehouse-event-id-set-v1\x00"))
	for _, id := range ids {
		_, _ = hasher.Write(id[:])
	}
	var result Hash
	copy(result[:], hasher.Sum(nil))
	return result
}

func rowIdentity(eventID EventID, physicalOrdinal uint64) Hash {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("warehouse-row-v1\x00"))
	_, _ = hasher.Write(eventID[:])
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], physicalOrdinal)
	_, _ = hasher.Write(encoded[:])
	var result Hash
	copy(result[:], hasher.Sum(nil))
	return result
}
