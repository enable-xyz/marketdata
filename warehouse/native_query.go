package warehouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"

	"github.com/shopspring/decimal"
)

// NativeQueryReferenceReader supplies the bounded catalog evidence associated
// with a declarative warehouse query. Implementations must return references in
// ascending ID order and must apply the QuerySpec tuple and time bounds.
type NativeQueryReferenceReader interface {
	References(context.Context, QuerySpec) ([]CoverageReference, []GapReference, error)
}

// NativeQueryReader executes the fixed warehouse query grammar over the native
// ClickHouse connection owned by a NativeStore.
type NativeQueryReader struct {
	query      nativeQueryTransport
	table      string
	references NativeQueryReferenceReader
	acquire    func()
	release    func()
}

// NewNativeQueryReader constructs a ClickHouse-backed QueryReader. The store
// remains caller-owned and must outlive the reader and every open cursor.
func NewNativeQueryReader(store *NativeStore, references NativeQueryReferenceReader) (*NativeQueryReader, error) {
	if store == nil || store.conn == nil {
		return nil, fmt.Errorf("%w: native query store is required", ErrInvalidQuery)
	}
	if references == nil {
		return nil, fmt.Errorf("%w: native query reference reader is required", ErrInvalidQuery)
	}
	schema, err := store.schema.normalized()
	if err != nil {
		return nil, err
	}
	return &NativeQueryReader{
		query: func(ctx context.Context, query string, args ...any) (nativeQueryRows, error) {
			return store.conn.Query(ctx, query, args...)
		},
		table:      schema.EventsTable(),
		references: references,
		acquire:    store.operationMu.Lock,
		release:    store.operationMu.Unlock,
	}, nil
}

type nativeQueryRows interface {
	Next() bool
	Scan(...any) error
	Err() error
	Close() error
}

type nativeQueryTransport func(context.Context, string, ...any) (nativeQueryRows, error)

func newNativeQueryReaderForTransport(table string, query nativeQueryTransport, references NativeQueryReferenceReader) (*NativeQueryReader, error) {
	if query == nil || references == nil || table == "" {
		return nil, fmt.Errorf("%w: native query dependencies are required", ErrInvalidQuery)
	}
	return &NativeQueryReader{query: query, table: table, references: references, acquire: func() {}, release: func() {}}, nil
}

func (r *NativeQueryReader) OpenQuery(ctx context.Context, spec QuerySpec) (QueryCursor, error) {
	if r == nil || r.query == nil || r.references == nil || r.acquire == nil || r.release == nil {
		return nil, fmt.Errorf("%w: native query reader is not initialized", ErrInvalidQuery)
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	coverage, gaps, err := r.references.References(ctx, spec)
	if err != nil {
		return nil, fmt.Errorf("warehouse: read query references: %w", err)
	}
	if _, _, err := validateReferences(spec, coverage, gaps); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	query, args := nativeSelect(spec, r.table)
	r.acquire()
	if err := ctx.Err(); err != nil {
		r.release()
		return nil, err
	}
	rows, err := r.query(ctx, query, args...)
	if err != nil {
		r.release()
		return nil, fmt.Errorf("warehouse: open native query cursor: %w", err)
	}
	if rows == nil {
		r.release()
		return nil, fmt.Errorf("warehouse: open native query cursor: nil rows")
	}
	return &nativeQueryCursor{
		rows:       rows,
		coverage:   slices.Clone(coverage),
		gaps:       slices.Clone(gaps),
		coverageBy: indexCoverage(coverage),
		gapsBy:     indexGaps(gaps),
		release:    r.release,
	}, nil
}

const nativeQueryColumns = `load_generation_id, manifest_sha256, row_id, event_id, logical_hash,
    family, source_id, channel_id, instrument_uid, epoch_kind, connection_epoch, received_time_ns,
    arrival_ordinal, message_ordinal, raw_segment_sha256, raw_record_ordinal, raw_payload_sha256,
    catalog_snapshot_id, schema_name, schema_version, dataset_policy_id, replay_config_id,
    input_manifest_set_id, physical_ordinal, price, amount, bid_price, bid_amount, ask_price, ask_amount,
    price_change, price_change_percent, weighted_average_price, first_trade_before_window_price,
    last_price, last_amount, native_best_bid_price, native_best_bid_amount, native_best_ask_price,
    native_best_ask_amount, open_price, high_price, low_price, base_volume, quote_volume`

func nativeSelect(spec QuerySpec, table string) (string, []any) {
	var query strings.Builder
	query.Grow(len(nativeQueryColumns) + len(table) + 512)
	query.WriteString("SELECT ")
	query.WriteString(nativeQueryColumns)
	query.WriteString(" FROM ")
	query.WriteString(table)
	query.WriteString(" WHERE load_generation_id = ? AND catalog_snapshot_id = ? AND family = ?")
	args := make([]any, 0, 10+len(spec.SourceIDs)+len(spec.ChannelIDs)+len(spec.InstrumentUIDs))
	args = append(args, string(spec.Dataset.ID[:]), string(spec.Dataset.CatalogSnapshotID[:]), spec.Dataset.Family)
	appendStringSet(&query, &args, "source_id", spec.SourceIDs)
	if len(spec.ChannelIDs) != 0 {
		appendStringSet(&query, &args, "channel_id", spec.ChannelIDs)
	}
	if len(spec.InstrumentUIDs) != 0 {
		appendStringSet(&query, &args, "instrument_uid", spec.InstrumentUIDs)
	}
	query.WriteString(" AND received_time_ns >= ? AND received_time_ns < ?")
	args = append(args, spec.StartReceivedTimeNS, spec.EndReceivedTimeNS)
	if spec.After != nil {
		query.WriteString(" AND (source_id, instrument_uid, received_time_ns, connection_epoch, arrival_ordinal, message_ordinal, row_id) > (?, ?, ?, ?, ?, ?, ?)")
		args = append(args, spec.After.SourceID, spec.After.InstrumentUID, spec.After.ReceivedTimeNS,
			string(spec.After.ConnectionEpoch[:]), spec.After.ArrivalOrdinal, spec.After.MessageOrdinal, string(spec.After.RowID[:]))
	}
	query.WriteString(" ORDER BY source_id, instrument_uid, received_time_ns, connection_epoch, arrival_ordinal, message_ordinal, row_id LIMIT ?")
	args = append(args, spec.Limit+1)
	return query.String(), args
}

func appendStringSet(query *strings.Builder, args *[]any, column string, values []string) {
	query.WriteString(" AND ")
	query.WriteString(column)
	query.WriteString(" IN (")
	for i, value := range values {
		if i != 0 {
			query.WriteString(", ")
		}
		query.WriteByte('?')
		*args = append(*args, value)
	}
	query.WriteByte(')')
}

type nativeReferenceWindow struct {
	id    string
	start int64
	end   int64
}

type nativeQueryCursor struct {
	mu         sync.Mutex
	rows       nativeQueryRows
	coverage   []CoverageReference
	gaps       []GapReference
	coverageBy map[Tuple][]nativeReferenceWindow
	gapsBy     map[Tuple][]nativeReferenceWindow
	release    func()
	closed     bool
}

func (c *nativeQueryCursor) References() ([]CoverageReference, []GapReference, error) {
	if c == nil {
		return nil, nil, fmt.Errorf("%w: native query cursor is not initialized", ErrInvalidQuery)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.coverage), slices.Clone(c.gaps), nil
}

func (c *nativeQueryCursor) Next(ctx context.Context) (QueryCandidate, error) {
	if c == nil {
		return QueryCandidate{}, fmt.Errorf("%w: native query cursor is not initialized", ErrInvalidQuery)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return QueryCandidate{}, io.EOF
	}
	if err := ctx.Err(); err != nil {
		return QueryCandidate{}, errors.Join(err, c.closeLocked())
	}
	if !c.rows.Next() {
		err := c.rows.Err()
		closeErr := c.closeLocked()
		if err != nil {
			return QueryCandidate{}, errors.Join(fmt.Errorf("warehouse: advance native query cursor: %w", err), closeErr)
		}
		if closeErr != nil {
			return QueryCandidate{}, closeErr
		}
		return QueryCandidate{}, io.EOF
	}
	if err := ctx.Err(); err != nil {
		return QueryCandidate{}, errors.Join(err, c.closeLocked())
	}
	row, err := scanNativeQueryRow(c.rows)
	if err != nil {
		return QueryCandidate{}, errors.Join(fmt.Errorf("warehouse: scan native query row: %w", err), c.closeLocked())
	}
	tuple := Tuple{SourceID: row.SourceID, ChannelID: row.ChannelID, InstrumentUID: row.InstrumentUID}
	return QueryCandidate{Row: row, CoverageRefIDs: referenceIDs(c.coverageBy[tuple], row.ReceivedTimeNS),
		GapRefIDs: referenceIDs(c.gapsBy[tuple], row.ReceivedTimeNS)}, nil
}

func (c *nativeQueryCursor) Close() error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeLocked()
}

func (c *nativeQueryCursor) closeLocked() error {
	if c.closed {
		return nil
	}
	c.closed = true
	err := c.rows.Close()
	c.release()
	return err
}

func indexCoverage(references []CoverageReference) map[Tuple][]nativeReferenceWindow {
	result := make(map[Tuple][]nativeReferenceWindow)
	for _, ref := range references {
		result[ref.Tuple] = append(result[ref.Tuple], nativeReferenceWindow{id: ref.ID, start: ref.StartReceivedTimeNS, end: ref.EndReceivedTimeNS})
	}
	return result
}

func indexGaps(references []GapReference) map[Tuple][]nativeReferenceWindow {
	result := make(map[Tuple][]nativeReferenceWindow)
	for _, ref := range references {
		result[ref.Tuple] = append(result[ref.Tuple], nativeReferenceWindow{id: ref.ID, start: ref.StartReceivedTimeNS, end: ref.EndReceivedTimeNS})
	}
	return result
}

func referenceIDs(windows []nativeReferenceWindow, receivedTimeNS int64) []string {
	var ids []string
	for _, window := range windows {
		if receivedTimeNS >= window.start && receivedTimeNS < window.end {
			ids = append(ids, window.id)
		}
	}
	return ids
}

type nativeFixedFields struct {
	generationID      []byte
	manifestHash      []byte
	rowID             []byte
	eventID           []byte
	logicalHash       []byte
	connectionEpoch   []byte
	rawSegmentHash    []byte
	rawPayloadHash    []byte
	catalogSnapshotID []byte
	datasetPolicyID   []byte
	replayConfigID    []byte
	inputManifestID   []byte
}

func scanNativeQueryRow(rows nativeQueryRows) (Row, error) {
	var row Row
	var fixed nativeFixedFields
	decimals := make([]*decimal.Decimal, 21)
	destinations := []any{
		&fixed.generationID, &fixed.manifestHash, &fixed.rowID, &fixed.eventID, &fixed.logicalHash,
		&row.Family, &row.SourceID, &row.ChannelID, &row.InstrumentUID, &row.EpochKind, &fixed.connectionEpoch,
		&row.ReceivedTimeNS, &row.ArrivalOrdinal, &row.MessageOrdinal, &fixed.rawSegmentHash, &row.RawRecordOrdinal,
		&fixed.rawPayloadHash, &fixed.catalogSnapshotID, &row.SchemaName, &row.SchemaVersion, &fixed.datasetPolicyID,
		&fixed.replayConfigID, &fixed.inputManifestID, &row.PhysicalOrdinal,
	}
	for i := range decimals {
		destinations = append(destinations, &decimals[i])
	}
	if err := rows.Scan(destinations...); err != nil {
		return Row{}, err
	}
	for _, assignment := range []struct {
		destination *Hash
		encoded     []byte
	}{
		{&row.GenerationID, fixed.generationID}, {&row.ManifestHash, fixed.manifestHash}, {&row.RowID, fixed.rowID},
		{&row.EventID, fixed.eventID}, {&row.LogicalHash, fixed.logicalHash}, {&row.RawSegmentSHA256, fixed.rawSegmentHash},
		{&row.RawPayloadSHA256, fixed.rawPayloadHash}, {&row.CatalogSnapshotID, fixed.catalogSnapshotID},
		{&row.DatasetPolicyID, fixed.datasetPolicyID}, {&row.ReplayConfigID, fixed.replayConfigID},
		{&row.InputManifestSetID, fixed.inputManifestID},
	} {
		if err := assignFixedHash(assignment.destination, assignment.encoded); err != nil {
			return Row{}, err
		}
	}
	if len(fixed.connectionEpoch) != len(row.ConnectionEpoch) {
		return Row{}, fmt.Errorf("%w: FixedString(16) length", ErrGenerationConflict)
	}
	copy(row.ConnectionEpoch[:], fixed.connectionEpoch)
	decimalFields := []**Decimal{
		&row.Price, &row.Amount, &row.BidPrice, &row.BidAmount, &row.AskPrice, &row.AskAmount, &row.PriceChange,
		&row.PriceChangePercent, &row.WeightedAveragePrice, &row.FirstTradeBeforeWindowPrice, &row.LastPrice,
		&row.LastAmount, &row.NativeBestBidPrice, &row.NativeBestBidAmount, &row.NativeBestAskPrice,
		&row.NativeBestAskAmount, &row.OpenPrice, &row.HighPrice, &row.LowPrice, &row.BaseVolume, &row.QuoteVolume,
	}
	for i, value := range decimals {
		if value == nil {
			continue
		}
		converted := decimalFromClickHouse(*value)
		*decimalFields[i] = &converted
	}
	return row, nil
}

var _ QueryReader = (*NativeQueryReader)(nil)
var _ QueryCursor = (*nativeQueryCursor)(nil)
