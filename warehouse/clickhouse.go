package warehouse

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/shopspring/decimal"
)

type BatchKind string

const (
	BatchGeneration  BatchKind = "generation"
	BatchExpectedIDs BatchKind = "expected_event_ids"
	BatchEvents      BatchKind = "events"
)

type BatchObservation struct {
	Kind    BatchKind
	Ordinal uint64
	Rows    int
}

// AcknowledgementFault runs only after a native synchronous batch Send has
// succeeded. Returning an error simulates response loss and is always surfaced
// as ErrWriteOutcomeUnknown; it cannot alter the batch or issue another write.
type AcknowledgementFault interface {
	AfterSynchronousBatch(context.Context, BatchObservation) error
}

type AcknowledgementFaultFunc func(context.Context, BatchObservation) error

func (f AcknowledgementFaultFunc) AfterSynchronousBatch(ctx context.Context, observation BatchObservation) error {
	return f(ctx, observation)
}

type NativeConfig struct {
	DSN                  string
	Database             string
	ServerDigest         string
	TablePrefix          string
	BatchRows            int
	Compression          Compression
	Layout               PartitionLayout
	AcknowledgementFault AcknowledgementFault
}

func (c NativeConfig) normalized() (NativeConfig, *clickhouse.Options, error) {
	if c.DSN == "" || c.ServerDigest == "" || len(c.ServerDigest) > MaxIdentityBytes || strings.IndexByte(c.ServerDigest, 0) >= 0 {
		return NativeConfig{}, nil, fmt.Errorf("%w: explicit ClickHouse DSN and server digest are required", ErrInvalidWarehouseInput)
	}
	options, err := clickhouse.ParseDSN(c.DSN)
	if err != nil {
		return NativeConfig{}, nil, fmt.Errorf("%w: parse explicit ClickHouse DSN: %v", ErrInvalidWarehouseInput, err)
	}
	if options.Protocol != clickhouse.Native {
		return NativeConfig{}, nil, fmt.Errorf("%w: native ClickHouse protocol is required", ErrInvalidWarehouseInput)
	}
	if c.Database == "" {
		c.Database = options.Auth.Database
	}
	if c.Database == "" || !identifierPattern.MatchString(c.Database) {
		return NativeConfig{}, nil, fmt.Errorf("%w: database must be explicit in NativeConfig or DSN", ErrInvalidWarehouseInput)
	}
	options.Auth.Database = c.Database
	loaderConfig, err := (Config{BatchRows: c.BatchRows, Compression: c.Compression, Layout: c.Layout}).normalized()
	if err != nil {
		return NativeConfig{}, nil, err
	}
	c.BatchRows = loaderConfig.BatchRows
	c.Compression = loaderConfig.Compression
	c.Layout = loaderConfig.Layout
	if c.TablePrefix == "" {
		c.TablePrefix = defaultTablePrefix
	}
	if !identifierPattern.MatchString(c.TablePrefix) {
		return NativeConfig{}, nil, fmt.Errorf("%w: table prefix", ErrInvalidWarehouseInput)
	}
	method := clickhouse.CompressionLZ4
	if c.Compression == CompressionZstd {
		method = clickhouse.CompressionZSTD
	}
	options.Compression = &clickhouse.Compression{Method: method}
	options.MaxOpenConns = 4
	options.MaxIdleConns = 2
	return c, options, nil
}

type NativeStore struct {
	operationMu      sync.Mutex
	conn             clickhouse.Conn
	schema           Schema
	serverDigest     string
	batchRows        int
	fault            AcknowledgementFault
	normalizedConfig NativeConfig
	options          *clickhouse.Options
	batchOrdinal     atomic.Uint64
	reconnects       atomic.Uint64
}

func OpenNative(ctx context.Context, config NativeConfig) (*NativeStore, error) {
	normalized, options, err := config.normalized()
	if err != nil {
		return nil, err
	}
	conn, err := openNativeConnection(ctx, options)
	if err != nil {
		return nil, err
	}
	return &NativeStore{conn: conn, schema: Schema{Database: normalized.Database, Prefix: normalized.TablePrefix, Layout: normalized.Layout},
		serverDigest: normalized.ServerDigest, batchRows: normalized.BatchRows, fault: normalized.AcknowledgementFault,
		normalizedConfig: normalized, options: options}, nil
}

func openNativeConnection(ctx context.Context, options *clickhouse.Options) (clickhouse.Conn, error) {
	conn, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("warehouse: open explicit ClickHouse DSN: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("warehouse: ping explicit ClickHouse DSN: %w", err)
	}
	return conn, nil
}

func (s *NativeStore) ReconnectCount() uint64 {
	return s.reconnects.Load()
}

// replaceConnection closes the connection that delivered the batch, then
// opens and pings a replacement from the exact normalized caller options.
// Every caller holds operationMu, so reconciliation cannot observe or race the
// closed handle.
func (s *NativeStore) replaceConnection(ctx context.Context) error {
	old := s.conn
	closeErr := old.Close()
	replacement, openErr := openNativeConnection(ctx, s.options)
	if openErr != nil {
		return errors.Join(closeErr, openErr)
	}
	s.conn = replacement
	s.reconnects.Add(1)
	return closeErr
}

func (s *NativeStore) Close() error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	return s.conn.Close()
}

func (s *NativeStore) EnsureSchema(ctx context.Context) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	statements, err := s.schema.Statements()
	if err != nil {
		return err
	}
	for _, statement := range statements {
		if err := s.conn.Exec(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *NativeStore) BeginGeneration(ctx context.Context, generation Generation) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	return s.beginGeneration(ctx, generation)
}

func (s *NativeStore) beginGeneration(ctx context.Context, generation Generation) error {
	if generation.ServerDigest != s.serverDigest {
		return ErrGenerationConflict
	}
	if err := generation.validate(); err != nil {
		return err
	}
	existing, found, err := s.generation(ctx, generation.ID)
	if err != nil {
		return err
	}
	if found {
		if !sameGeneration(existing, generation) {
			return ErrGenerationConflict
		}
		expected, err := s.expectedEventIDs(ctx, generation.ID)
		if err != nil {
			return err
		}
		if !slices.Equal(expected, generation.ExpectedEventIDs) {
			return ErrGenerationConflict
		}
		return nil
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.schema.GenerationsTable())
	if err != nil {
		return err
	}
	defer batch.Close()
	if err := batch.Append(generation.ID[:], generation.ServerDigest, generation.ManifestHash[:], generation.InputHash[:],
		generation.DatasetIdentity[:], generation.CatalogIdentity[:], generation.SchemaIdentity[:], generation.Family,
		generation.SourceID, generation.UTCDate, generation.PartitionValue, string(generation.Layout), generation.ExpectedEventSetHash[:],
		generation.ExpectedEventCount, generation.ExpectedRowCount, string(generation.State), generation.LastError,
		generation.CreatedAt.UTC(), generation.UpdatedAt.UTC()); err != nil {
		return err
	}
	if err := s.send(ctx, batch, BatchGeneration, 1); err != nil {
		return err
	}
	for start := 0; start < len(generation.ExpectedEventIDs); start += s.batchRows {
		end := min(start+s.batchRows, len(generation.ExpectedEventIDs))
		expectedBatch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.schema.ExpectedEventsTable())
		if err != nil {
			return err
		}
		for _, id := range generation.ExpectedEventIDs[start:end] {
			if err := expectedBatch.Append(generation.ID[:], generation.PartitionValue, id[:]); err != nil {
				_ = expectedBatch.Close()
				return err
			}
		}
		if err := s.send(ctx, expectedBatch, BatchExpectedIDs, end-start); err != nil {
			_ = expectedBatch.Close()
			return err
		}
		if err := expectedBatch.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (s *NativeStore) Generation(ctx context.Context, id GenerationID) (Generation, bool, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	return s.generation(ctx, id)
}

func (s *NativeStore) generation(ctx context.Context, id GenerationID) (Generation, bool, error) {
	query := fmt.Sprintf(`SELECT server_digest, manifest_sha256, input_hash, dataset_identity, catalog_identity,
    schema_identity, family, source_id, utc_date, partition_value, toString(partition_layout), expected_event_set_sha256,
    expected_event_count, expected_row_count, toString(state), last_error, created_at, updated_at
FROM %s WHERE load_generation_id = ? LIMIT 2`, s.schema.GenerationsTable())
	rows, err := s.conn.Query(ctx, query, string(id[:]))
	if err != nil {
		return Generation{}, false, err
	}
	defer rows.Close()
	var result Generation
	var manifestHash, inputHash, datasetIdentity, catalogIdentity, schemaIdentity, eventSetHash []byte
	var state, layout string
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Generation{}, false, err
		}
		return Generation{}, false, nil
	}
	if err := rows.Scan(&result.ServerDigest, &manifestHash, &inputHash, &datasetIdentity, &catalogIdentity,
		&schemaIdentity, &result.Family, &result.SourceID, &result.UTCDate, &result.PartitionValue, &layout, &eventSetHash,
		&result.ExpectedEventCount, &result.ExpectedRowCount, &state, &result.LastError, &result.CreatedAt, &result.UpdatedAt); err != nil {
		return Generation{}, false, err
	}
	if rows.Next() {
		return Generation{}, false, ErrGenerationConflict
	}
	if err := rows.Err(); err != nil {
		return Generation{}, false, err
	}
	result.ID = id
	if err := assignFixedHash(&result.ManifestHash, manifestHash); err != nil {
		return Generation{}, false, err
	}
	if err := assignFixedHash(&result.InputHash, inputHash); err != nil {
		return Generation{}, false, err
	}
	if err := assignFixedHash(&result.DatasetIdentity, datasetIdentity); err != nil {
		return Generation{}, false, err
	}
	if err := assignFixedHash(&result.CatalogIdentity, catalogIdentity); err != nil {
		return Generation{}, false, err
	}
	if err := assignFixedHash(&result.SchemaIdentity, schemaIdentity); err != nil {
		return Generation{}, false, err
	}
	if err := assignFixedHash(&result.ExpectedEventSetHash, eventSetHash); err != nil {
		return Generation{}, false, err
	}
	result.Layout = PartitionLayout(layout)
	result.State = GenerationState(state)
	if !result.Layout.valid() || !result.State.valid() {
		return Generation{}, false, ErrGenerationConflict
	}
	return result, true, nil
}

func (s *NativeStore) ExpectedEventIDs(ctx context.Context, id GenerationID) ([]EventID, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	return s.expectedEventIDs(ctx, id)
}

func (s *NativeStore) expectedEventIDs(ctx context.Context, id GenerationID) ([]EventID, error) {
	query := "SELECT event_id FROM " + s.schema.ExpectedEventsTable() + " WHERE load_generation_id = ? ORDER BY event_id"
	return s.readEventIDs(ctx, query, id)
}

func (s *NativeStore) ActualEventIDs(ctx context.Context, id GenerationID) ([]EventID, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	return s.actualEventIDs(ctx, id)
}

func (s *NativeStore) actualEventIDs(ctx context.Context, id GenerationID) ([]EventID, error) {
	query := "SELECT DISTINCT event_id FROM " + s.schema.EventsTable() + " WHERE load_generation_id = ? ORDER BY event_id"
	return s.readEventIDs(ctx, query, id)
}

func (s *NativeStore) readEventIDs(ctx context.Context, query string, id GenerationID) ([]EventID, error) {
	rows, err := s.conn.Query(ctx, query, string(id[:]))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []EventID
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var eventID EventID
		if err := assignFixedHash(&eventID, encoded); err != nil {
			return nil, err
		}
		result = append(result, eventID)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *NativeStore) GenerationRowCount(ctx context.Context, id GenerationID) (uint64, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	return s.generationRowCount(ctx, id)
}

func (s *NativeStore) generationRowCount(ctx context.Context, id GenerationID) (uint64, error) {
	var count uint64
	err := s.conn.QueryRow(ctx, "SELECT count() FROM "+s.schema.EventsTable()+" WHERE load_generation_id = ?", string(id[:])).Scan(&count)
	return count, err
}

func (s *NativeStore) InsertRows(ctx context.Context, rows []Row) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if len(rows) == 0 || len(rows) > s.batchRows {
		return fmt.Errorf("%w: native batch row count", ErrInvalidWarehouseInput)
	}
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.schema.EventsTable())
	if err != nil {
		return err
	}
	defer batch.Close()
	for _, row := range rows {
		if err := row.validate(); err != nil {
			return err
		}
		values, err := rowValues(row)
		if err != nil {
			return err
		}
		if err := batch.Append(values...); err != nil {
			return err
		}
	}
	return s.send(ctx, batch, BatchEvents, len(rows))
}

func (s *NativeStore) SetGenerationState(ctx context.Context, id GenerationID, state GenerationState, lastError string, updatedAt time.Time) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if id == (GenerationID{}) || !state.valid() || len(lastError) > MaxGenerationError || updatedAt.IsZero() {
		return fmt.Errorf("%w: generation state transition", ErrInvalidWarehouseInput)
	}
	query := fmt.Sprintf("ALTER TABLE %s UPDATE state = ?, last_error = ?, updated_at = ? WHERE load_generation_id = ? SETTINGS mutations_sync = 2",
		s.schema.GenerationsTable())
	return s.conn.Exec(ctx, query, string(state), lastError, updatedAt.UTC(), string(id[:]))
}

func (s *NativeStore) DeleteGeneration(ctx context.Context, id GenerationID) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if id == (GenerationID{}) {
		return fmt.Errorf("%w: generation identity", ErrInvalidWarehouseInput)
	}
	for _, table := range []string{s.schema.EventsTable(), s.schema.ExpectedEventsTable(), s.schema.GenerationsTable()} {
		query := fmt.Sprintf("ALTER TABLE %s DELETE WHERE load_generation_id = ? SETTINGS mutations_sync = 2", table)
		if err := s.conn.Exec(ctx, query, string(id[:])); err != nil {
			return err
		}
	}
	return nil
}

func (s *NativeStore) DropPartition(ctx context.Context, partition Partition) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	if err := partition.validate(); err != nil {
		return err
	}
	if partition.Layout != s.schema.Layout {
		return ErrGenerationConflict
	}
	tables := []struct {
		qualified string
		name      string
	}{
		{qualified: s.schema.EventsTable(), name: s.schema.Prefix + "_events_v1"},
		{qualified: s.schema.ExpectedEventsTable(), name: s.schema.Prefix + "_generation_event_ids_v1"},
		{qualified: s.schema.GenerationsTable(), name: s.schema.Prefix + "_load_generations_v1"},
	}
	for _, table := range tables {
		var activeParts uint64
		if err := s.conn.QueryRow(ctx, `SELECT count() FROM system.parts
WHERE active AND database = ? AND table = ? AND partition_id = ?`,
			s.schema.Database, table.name, fmt.Sprint(partition.Value)).Scan(&activeParts); err != nil {
			return err
		}
		if activeParts == 0 {
			continue
		}
		query := fmt.Sprintf("ALTER TABLE %s DROP PARTITION %d", table.qualified, partition.Value)
		if err := s.conn.Exec(ctx, query); err != nil {
			return err
		}
	}
	return nil
}

func (s *NativeStore) Truncate(ctx context.Context) error {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	for _, table := range []string{s.schema.EventsTable(), s.schema.ExpectedEventsTable(), s.schema.GenerationsTable()} {
		if err := s.conn.Exec(ctx, "TRUNCATE TABLE "+table+" SYNC"); err != nil {
			return err
		}
	}
	return nil
}

func (s *NativeStore) DecimalFields(ctx context.Context, generationID GenerationID, eventID EventID) (map[string]Decimal, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	query := fmt.Sprintf(`SELECT price, amount, bid_price, bid_amount, ask_price, ask_amount, price_change,
    price_change_percent, weighted_average_price, first_trade_before_window_price, last_price, last_amount,
    native_best_bid_price, native_best_bid_amount, native_best_ask_price, native_best_ask_amount,
    open_price, high_price, low_price, base_volume, quote_volume
FROM %s WHERE load_generation_id = ? AND event_id = ? ORDER BY physical_ordinal LIMIT 1`, s.schema.EventsTable())
	values := make([]*decimal.Decimal, 21)
	destinations := make([]any, len(values))
	for i := range values {
		destinations[i] = &values[i]
	}
	if err := s.conn.QueryRow(ctx, query, string(generationID[:]), string(eventID[:])).Scan(destinations...); err != nil {
		return nil, err
	}
	names := []string{"price", "amount", "bid_price", "bid_amount", "ask_price", "ask_amount", "price_change",
		"price_change_percent", "weighted_average_price", "first_trade_before_window_price", "last_price", "last_amount",
		"native_best_bid_price", "native_best_bid_amount", "native_best_ask_price", "native_best_ask_amount",
		"open_price", "high_price", "low_price", "base_volume", "quote_volume"}
	result := make(map[string]Decimal)
	for i, value := range values {
		if value == nil {
			continue
		}
		result[names[i]] = decimalFromClickHouse(*value)
	}
	return result, nil
}

func (s *NativeStore) send(ctx context.Context, batch driver.Batch, kind BatchKind, rows int) error {
	if err := batch.Send(); err != nil {
		unknown := fmt.Errorf("%w: native synchronous %s batch: %v", ErrWriteOutcomeUnknown, kind, err)
		return errors.Join(unknown, s.replaceConnection(ctx))
	}
	ordinal := s.batchOrdinal.Add(1) - 1
	if s.fault != nil {
		if err := s.fault.AfterSynchronousBatch(ctx, BatchObservation{Kind: kind, Ordinal: ordinal, Rows: rows}); err != nil {
			unknown := fmt.Errorf("%w: native synchronous %s acknowledgement: %v", ErrWriteOutcomeUnknown, kind, err)
			return errors.Join(unknown, s.replaceConnection(ctx))
		}
	}
	return nil
}

func rowValues(row Row) ([]any, error) {
	decimals := []*Decimal{row.Price, row.Amount, row.BidPrice, row.BidAmount, row.AskPrice, row.AskAmount,
		row.PriceChange, row.PriceChangePercent, row.WeightedAveragePrice, row.FirstTradeBeforeWindowPrice,
		row.LastPrice, row.LastAmount, row.NativeBestBidPrice, row.NativeBestBidAmount, row.NativeBestAskPrice,
		row.NativeBestAskAmount, row.OpenPrice, row.HighPrice, row.LowPrice, row.BaseVolume, row.QuoteVolume}
	values := []any{row.GenerationID[:], row.ManifestHash[:], row.RowID[:], row.EventID[:], row.LogicalHash[:], row.Family,
		row.SourceID, row.ChannelID, row.InstrumentUID, row.EpochKind, row.ConnectionEpoch[:], row.ReceivedTimeNS,
		row.ArrivalOrdinal, row.MessageOrdinal, row.RawSegmentSHA256[:], row.RawRecordOrdinal, row.RawPayloadSHA256[:],
		row.CatalogSnapshotID[:], row.SchemaName, row.SchemaVersion, row.DatasetPolicyID[:], row.ReplayConfigID[:],
		row.InputManifestSetID[:], row.PhysicalOrdinal}
	for _, value := range decimals {
		converted, err := decimalForClickHouse(value)
		if err != nil {
			return nil, err
		}
		values = append(values, converted)
	}
	return values, nil
}

func decimalForClickHouse(value *Decimal) (any, error) {
	if value == nil {
		return nil, nil
	}
	if value.Scale != 18 && value.Scale != 8 {
		return nil, fmt.Errorf("%w: ClickHouse decimal scale", ErrInvalidWarehouseInput)
	}
	coefficient, ok := new(big.Int).SetString(value.Coefficient, 10)
	if !ok {
		return nil, fmt.Errorf("%w: ClickHouse decimal coefficient", ErrInvalidWarehouseInput)
	}
	return decimal.NewFromBigInt(coefficient, -int32(value.Scale)), nil
}

func decimalFromClickHouse(value decimal.Decimal) Decimal {
	return Decimal{Coefficient: value.Coefficient().String(), Scale: uint8(-value.Exponent())}
}

func assignFixedHash(destination *Hash, encoded []byte) error {
	if len(encoded) != len(destination) {
		return fmt.Errorf("%w: FixedString(32) length", ErrGenerationConflict)
	}
	copy(destination[:], encoded)
	return nil
}

var _ Store = (*NativeStore)(nil)
