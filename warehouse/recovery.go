package warehouse

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math/big"
	"slices"
	"strings"
)

var ErrFullRebuildMismatch = errors.New("warehouse: full rebuild evidence mismatch")

// RebuildEvidenceStore is discovered only by full disaster-recovery drills.
// Implementations must stream every physical serving row in the table, without
// filtering to the expected rebuild generations, and enumerate generation IDs
// persisted in any warehouse table.
type RebuildEvidenceStore interface {
	StreamRebuildRows(context.Context, func(Row) error) error
	PersistedGenerationIDs(context.Context) ([]GenerationID, error)
}

type RebuildSummary struct {
	EventIDs       []EventID
	EventSetSHA256 Hash
	// LogicalSHA256 is the multiset hash of independently encoded actual Row
	// values. The persisted Row.LogicalHash claim is deliberately excluded.
	LogicalSHA256   Hash
	AggregateSHA256 Hash
	EventCount      uint64
	RowCount        uint64
}

type FullRebuildReport struct {
	Receipts               []LoadReceipt
	ExpectedGenerationIDs  []GenerationID
	PersistedGenerationIDs []GenerationID
	Source                 RebuildSummary
	Target                 RebuildSummary
}

// FullRebuild holds the concrete table's process-wide exclusive fence from
// before truncate through source and target verification. It compares every
// serving value read back from the rebuilt store, every persisted generation,
// the row multiset, unique event set, physical row count, and query aggregates.
func (l *Loader) FullRebuild(ctx context.Context, inputs []CommittedManifest) (FullRebuildReport, error) {
	store, ok := l.store.(RebuildEvidenceStore)
	if !ok {
		return FullRebuildReport{}, fmt.Errorf("%w: store cannot stream complete rebuild evidence", ErrInvalidWarehouseInput)
	}
	plans, err := l.planFullRebuild(ctx, inputs)
	if err != nil {
		return FullRebuildReport{}, err
	}
	releaseStore := acquireStoreRebuildFence(l.storeID)
	defer releaseStore()
	releaseGenerations := acquireGenerationLocks(plans)
	defer releaseGenerations()

	receipts, err := l.rebuildAllPlanned(ctx, inputs, plans)
	if err != nil {
		return FullRebuildReport{}, err
	}
	if len(receipts) != len(inputs) {
		return FullRebuildReport{}, fmt.Errorf("%w: rebuild receipt count", ErrFullRebuildMismatch)
	}
	expectedGenerationIDs := make([]GenerationID, len(receipts))
	for i := range receipts {
		expectedGenerationIDs[i] = receipts[i].GenerationID
	}
	expectedGenerationIDs, err = normalizeRebuildGenerationIDs(expectedGenerationIDs)
	if err != nil {
		return FullRebuildReport{}, err
	}
	persistedGenerationIDs, err := store.PersistedGenerationIDs(ctx)
	if err != nil {
		return FullRebuildReport{}, fmt.Errorf("warehouse: enumerate rebuilt ClickHouse generations: %w", err)
	}
	persistedGenerationIDs, err = normalizeRebuildGenerationIDs(persistedGenerationIDs)
	if err != nil {
		return FullRebuildReport{}, err
	}

	sourceBuilder := newRebuildSummaryBuilder()
	for i := range inputs {
		if err := l.reader.Scan(ctx, inputs[i], receipts[i].GenerationID, sourceBuilder.add); err != nil {
			return FullRebuildReport{}, fmt.Errorf("warehouse: scan committed Parquet rebuild evidence: %w", err)
		}
	}
	source, err := sourceBuilder.finish()
	if err != nil {
		return FullRebuildReport{}, err
	}
	targetBuilder := newRebuildSummaryBuilder()
	if err := store.StreamRebuildRows(ctx, targetBuilder.add); err != nil {
		return FullRebuildReport{}, fmt.Errorf("warehouse: stream all rebuilt ClickHouse evidence: %w", err)
	}
	target, err := targetBuilder.finish()
	if err != nil {
		return FullRebuildReport{}, err
	}
	report := FullRebuildReport{
		Receipts: slices.Clone(receipts), ExpectedGenerationIDs: expectedGenerationIDs,
		PersistedGenerationIDs: persistedGenerationIDs, Source: source, Target: target,
	}
	if !slices.Equal(expectedGenerationIDs, persistedGenerationIDs) ||
		!slices.Equal(source.EventIDs, target.EventIDs) || source.EventSetSHA256 != target.EventSetSHA256 ||
		source.LogicalSHA256 != target.LogicalSHA256 || source.AggregateSHA256 != target.AggregateSHA256 ||
		source.EventCount != target.EventCount || source.RowCount != target.RowCount {
		return report, ErrFullRebuildMismatch
	}
	return report, nil
}

func normalizeRebuildGenerationIDs(ids []GenerationID) ([]GenerationID, error) {
	result := slices.Clone(ids)
	for _, id := range result {
		if id == (GenerationID{}) {
			return nil, fmt.Errorf("%w: zero persisted generation identity", ErrFullRebuildMismatch)
		}
	}
	slices.SortFunc(result, compareHash)
	for i := 1; i < len(result); i++ {
		if result[i] == result[i-1] {
			return nil, fmt.Errorf("%w: duplicate persisted generation identity", ErrFullRebuildMismatch)
		}
	}
	return result, nil
}

type rebuildAggregateKey struct {
	generationID  GenerationID
	family        string
	catalogID     Hash
	schemaName    string
	schemaVersion uint16
	sourceID      string
	channelID     string
	instrumentUID string
}

type rebuildDecimalAggregate struct {
	count uint64
	scale uint8
	sum   big.Int
}

type rebuildAggregate struct {
	key      rebuildAggregateKey
	count    uint64
	minimum  int64
	maximum  int64
	decimals [21]rebuildDecimalAggregate
}

type rebuildSummaryBuilder struct {
	rowHashes  []Hash
	eventIDs   []EventID
	aggregates map[rebuildAggregateKey]rebuildAggregate
	rowCount   uint64
}

func newRebuildSummaryBuilder() *rebuildSummaryBuilder {
	return &rebuildSummaryBuilder{aggregates: make(map[rebuildAggregateKey]rebuildAggregate)}
}

func (b *rebuildSummaryBuilder) add(row Row) error {
	rowHash, err := canonicalRebuildRowHash(row)
	if err != nil {
		return err
	}
	b.rowHashes = append(b.rowHashes, rowHash)
	b.eventIDs = append(b.eventIDs, row.EventID)
	b.rowCount++

	key := rebuildAggregateKey{
		generationID: row.GenerationID, family: row.Family, catalogID: row.CatalogSnapshotID,
		schemaName: row.SchemaName, schemaVersion: row.SchemaVersion, sourceID: row.SourceID,
		channelID: row.ChannelID, instrumentUID: row.InstrumentUID,
	}
	aggregate, found := b.aggregates[key]
	if !found {
		aggregate = rebuildAggregate{key: key, minimum: row.ReceivedTimeNS, maximum: row.ReceivedTimeNS}
	}
	aggregate.count++
	aggregate.minimum = min(aggregate.minimum, row.ReceivedTimeNS)
	aggregate.maximum = max(aggregate.maximum, row.ReceivedTimeNS)
	for i, value := range rebuildDecimalFields(row) {
		if value == nil {
			continue
		}
		coefficient, ok := new(big.Int).SetString(value.Coefficient, 10)
		if !ok {
			return fmt.Errorf("%w: decimal aggregate coefficient", ErrFullRebuildMismatch)
		}
		field := &aggregate.decimals[i]
		if field.count != 0 && field.scale != value.Scale {
			return fmt.Errorf("%w: decimal aggregate scale", ErrFullRebuildMismatch)
		}
		field.count++
		field.scale = value.Scale
		field.sum.Add(&field.sum, coefficient)
	}
	b.aggregates[key] = aggregate
	return nil
}

func canonicalRebuildRowHash(row Row) (Hash, error) {
	if err := row.validate(); err != nil {
		return Hash{}, fmt.Errorf("%w: invalid actual rebuild row: %v", ErrFullRebuildMismatch, err)
	}
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("warehouse-rebuild-actual-row-v2\x00"))
	writeRebuildHash(hasher, row.GenerationID)
	writeRebuildHash(hasher, row.ManifestHash)
	writeRebuildHash(hasher, row.RowID)
	writeRebuildHash(hasher, row.EventID)
	writeRebuildString(hasher, row.Family)
	writeRebuildString(hasher, row.SourceID)
	writeRebuildString(hasher, row.ChannelID)
	writeRebuildString(hasher, row.InstrumentUID)
	writeRebuildString(hasher, row.EpochKind)
	_, _ = hasher.Write(row.ConnectionEpoch[:])
	writeRebuildInt64(hasher, row.ReceivedTimeNS)
	writeRebuildUint64(hasher, row.ArrivalOrdinal)
	writeRebuildUint32(hasher, row.MessageOrdinal)
	writeRebuildHash(hasher, row.RawSegmentSHA256)
	writeRebuildUint64(hasher, row.RawRecordOrdinal)
	writeRebuildHash(hasher, row.RawPayloadSHA256)
	writeRebuildHash(hasher, row.CatalogSnapshotID)
	writeRebuildString(hasher, row.SchemaName)
	writeRebuildUint16(hasher, row.SchemaVersion)
	writeRebuildHash(hasher, row.DatasetPolicyID)
	writeRebuildHash(hasher, row.ReplayConfigID)
	writeRebuildHash(hasher, row.InputManifestSetID)
	writeRebuildUint64(hasher, row.PhysicalOrdinal)
	for _, value := range rebuildDecimalFields(row) {
		if value == nil {
			writeRebuildUint8(hasher, 0)
			continue
		}
		coefficient, ok := new(big.Int).SetString(value.Coefficient, 10)
		if !ok {
			return Hash{}, fmt.Errorf("%w: decimal row coefficient", ErrFullRebuildMismatch)
		}
		writeRebuildUint8(hasher, 1)
		writeRebuildUint8(hasher, value.Scale)
		writeRebuildString(hasher, coefficient.String())
	}
	var result Hash
	copy(result[:], hasher.Sum(nil))
	return result, nil
}

func rebuildDecimalFields(row Row) [21]*Decimal {
	return [21]*Decimal{
		row.Price, row.Amount, row.BidPrice, row.BidAmount, row.AskPrice, row.AskAmount,
		row.PriceChange, row.PriceChangePercent, row.WeightedAveragePrice, row.FirstTradeBeforeWindowPrice,
		row.LastPrice, row.LastAmount, row.NativeBestBidPrice, row.NativeBestBidAmount,
		row.NativeBestAskPrice, row.NativeBestAskAmount, row.OpenPrice, row.HighPrice, row.LowPrice,
		row.BaseVolume, row.QuoteVolume,
	}
}

func (b *rebuildSummaryBuilder) finish() (RebuildSummary, error) {
	if b.rowCount == 0 {
		return RebuildSummary{}, fmt.Errorf("%w: empty rebuild evidence", ErrFullRebuildMismatch)
	}
	slices.SortFunc(b.eventIDs, compareHash)
	b.eventIDs = slices.Compact(b.eventIDs)
	slices.SortFunc(b.rowHashes, compareHash)
	logicalHasher := sha256.New()
	_, _ = logicalHasher.Write([]byte("warehouse-rebuild-actual-row-multiset-v2\x00"))
	for _, rowHash := range b.rowHashes {
		writeRebuildHash(logicalHasher, rowHash)
	}
	aggregateRows := make([]rebuildAggregate, 0, len(b.aggregates))
	for _, aggregate := range b.aggregates {
		aggregateRows = append(aggregateRows, aggregate)
	}
	slices.SortFunc(aggregateRows, func(left, right rebuildAggregate) int {
		if order := compareHash(left.key.generationID, right.key.generationID); order != 0 {
			return order
		}
		if order := strings.Compare(left.key.family, right.key.family); order != 0 {
			return order
		}
		if order := compareHash(left.key.catalogID, right.key.catalogID); order != 0 {
			return order
		}
		if order := strings.Compare(left.key.schemaName, right.key.schemaName); order != 0 {
			return order
		}
		if left.key.schemaVersion != right.key.schemaVersion {
			if left.key.schemaVersion < right.key.schemaVersion {
				return -1
			}
			return 1
		}
		if order := strings.Compare(left.key.sourceID, right.key.sourceID); order != 0 {
			return order
		}
		if order := strings.Compare(left.key.channelID, right.key.channelID); order != 0 {
			return order
		}
		return strings.Compare(left.key.instrumentUID, right.key.instrumentUID)
	})
	aggregateHasher := sha256.New()
	_, _ = aggregateHasher.Write([]byte("warehouse-rebuild-query-aggregates-v2\x00"))
	for _, aggregate := range aggregateRows {
		writeRebuildHash(aggregateHasher, aggregate.key.generationID)
		writeRebuildString(aggregateHasher, aggregate.key.family)
		writeRebuildHash(aggregateHasher, aggregate.key.catalogID)
		writeRebuildString(aggregateHasher, aggregate.key.schemaName)
		writeRebuildUint16(aggregateHasher, aggregate.key.schemaVersion)
		writeRebuildString(aggregateHasher, aggregate.key.sourceID)
		writeRebuildString(aggregateHasher, aggregate.key.channelID)
		writeRebuildString(aggregateHasher, aggregate.key.instrumentUID)
		writeRebuildUint64(aggregateHasher, aggregate.count)
		writeRebuildInt64(aggregateHasher, aggregate.minimum)
		writeRebuildInt64(aggregateHasher, aggregate.maximum)
		for _, field := range aggregate.decimals {
			writeRebuildUint64(aggregateHasher, field.count)
			writeRebuildUint8(aggregateHasher, field.scale)
			writeRebuildString(aggregateHasher, field.sum.String())
		}
	}
	var logicalHash, aggregateHash Hash
	copy(logicalHash[:], logicalHasher.Sum(nil))
	copy(aggregateHash[:], aggregateHasher.Sum(nil))
	summary := RebuildSummary{
		EventIDs: b.eventIDs, EventSetSHA256: eventSetHash(b.eventIDs), LogicalSHA256: logicalHash,
		AggregateSHA256: aggregateHash, EventCount: uint64(len(b.eventIDs)), RowCount: b.rowCount,
	}
	b.rowHashes = nil
	b.eventIDs = nil
	b.aggregates = nil
	return summary, nil
}

func writeRebuildHash(writer hash.Hash, value Hash) {
	_, _ = writer.Write(value[:])
}

func writeRebuildString(writer hash.Hash, value string) {
	writeRebuildUint64(writer, uint64(len(value)))
	_, _ = writer.Write([]byte(value))
}

func writeRebuildInt64(writer hash.Hash, value int64) {
	writeRebuildUint64(writer, uint64(value))
}

func writeRebuildUint64(writer hash.Hash, value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeRebuildUint32(writer hash.Hash, value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeRebuildUint16(writer hash.Hash, value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	_, _ = writer.Write(encoded[:])
}

func writeRebuildUint8(writer hash.Hash, value uint8) {
	_, _ = writer.Write([]byte{value})
}
