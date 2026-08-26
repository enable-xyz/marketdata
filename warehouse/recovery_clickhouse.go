package warehouse

import (
	"context"
	"fmt"

	"github.com/shopspring/decimal"
)

var _ RebuildEvidenceStore = (*NativeStore)(nil)

func (s *NativeStore) StoreIdentity() StoreIdentity {
	return StoreIdentity{
		ServerDigest: s.serverDigest,
		Database:     s.schema.Database,
		TablePrefix:  s.schema.Prefix,
		Layout:       s.schema.Layout,
	}
}

// PersistedGenerationIDs enumerates identities referenced by any warehouse
// table, including incomplete generations and orphan expected/event rows.
func (s *NativeStore) PersistedGenerationIDs(ctx context.Context) ([]GenerationID, error) {
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	query := fmt.Sprintf(`SELECT DISTINCT load_generation_id
FROM (
    SELECT load_generation_id FROM %s
    UNION ALL
    SELECT load_generation_id FROM %s
    UNION ALL
    SELECT load_generation_id FROM %s
)
ORDER BY load_generation_id`,
		s.schema.EventsTable(), s.schema.ExpectedEventsTable(), s.schema.GenerationsTable())
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []GenerationID
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		var id GenerationID
		if err := assignFixedHash(&id, encoded); err != nil {
			return nil, err
		}
		result = append(result, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

// StreamRebuildRows selects and reconstructs every canonical serving column
// for every physical row in the table. logical_hash is read only to reconstruct
// Row and validate its shape; canonicalRebuildRowHash never encodes that claim.
func (s *NativeStore) StreamRebuildRows(ctx context.Context, visit func(Row) error) error {
	if visit == nil {
		return fmt.Errorf("%w: rebuild row visitor is required", ErrInvalidWarehouseInput)
	}
	s.operationMu.Lock()
	defer s.operationMu.Unlock()
	query := fmt.Sprintf(`SELECT
    load_generation_id, manifest_sha256, row_id, event_id, logical_hash,
    family, source_id, channel_id, instrument_uid, epoch_kind, connection_epoch,
    received_time_ns, arrival_ordinal, message_ordinal, raw_segment_sha256,
    raw_record_ordinal, raw_payload_sha256, catalog_snapshot_id, schema_name,
    schema_version, dataset_policy_id, replay_config_id, input_manifest_set_id,
    physical_ordinal, price, amount, bid_price, bid_amount, ask_price, ask_amount,
    price_change, price_change_percent, weighted_average_price,
    first_trade_before_window_price, last_price, last_amount,
    native_best_bid_price, native_best_bid_amount, native_best_ask_price,
    native_best_ask_amount, open_price, high_price, low_price, base_volume, quote_volume
FROM %s
ORDER BY load_generation_id, event_id, row_id, physical_ordinal`, s.schema.EventsTable())
	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		row, err := scanRebuildRow(rows)
		if err != nil {
			return err
		}
		if err := visit(row); err != nil {
			return err
		}
	}
	return rows.Err()
}

type rebuildRowScanner interface {
	Scan(...any) error
}

func scanRebuildRow(scanner rebuildRowScanner) (Row, error) {
	var row Row
	var generation, manifest, rowID, eventID, logicalHash, connectionEpoch, rawSegment, rawPayload,
		catalogSnapshot, datasetPolicy, replayConfig, inputManifestSet []byte
	decimalValues := make([]*decimal.Decimal, 21)
	destinations := []any{
		&generation, &manifest, &rowID, &eventID, &logicalHash,
		&row.Family, &row.SourceID, &row.ChannelID, &row.InstrumentUID, &row.EpochKind, &connectionEpoch,
		&row.ReceivedTimeNS, &row.ArrivalOrdinal, &row.MessageOrdinal, &rawSegment,
		&row.RawRecordOrdinal, &rawPayload, &catalogSnapshot, &row.SchemaName,
		&row.SchemaVersion, &datasetPolicy, &replayConfig, &inputManifestSet, &row.PhysicalOrdinal,
	}
	for i := range decimalValues {
		destinations = append(destinations, &decimalValues[i])
	}
	if err := scanner.Scan(destinations...); err != nil {
		return Row{}, err
	}
	hashes := []struct {
		destination *Hash
		encoded     []byte
	}{
		{&row.GenerationID, generation},
		{&row.ManifestHash, manifest},
		{&row.RowID, rowID},
		{&row.EventID, eventID},
		{&row.LogicalHash, logicalHash},
		{&row.RawSegmentSHA256, rawSegment},
		{&row.RawPayloadSHA256, rawPayload},
		{&row.CatalogSnapshotID, catalogSnapshot},
		{&row.DatasetPolicyID, datasetPolicy},
		{&row.ReplayConfigID, replayConfig},
		{&row.InputManifestSetID, inputManifestSet},
	}
	for _, item := range hashes {
		if err := assignFixedHash(item.destination, item.encoded); err != nil {
			return Row{}, err
		}
	}
	if len(connectionEpoch) != len(row.ConnectionEpoch) {
		return Row{}, fmt.Errorf("%w: FixedString(16) length", ErrGenerationConflict)
	}
	copy(row.ConnectionEpoch[:], connectionEpoch)
	targets := rebuildDecimalTargets(&row)
	for i, value := range decimalValues {
		if value == nil {
			continue
		}
		converted := decimalFromClickHouse(*value)
		*targets[i] = &converted
	}
	return row, nil
}

func rebuildDecimalTargets(row *Row) [21]**Decimal {
	return [21]**Decimal{
		&row.Price, &row.Amount, &row.BidPrice, &row.BidAmount, &row.AskPrice, &row.AskAmount,
		&row.PriceChange, &row.PriceChangePercent, &row.WeightedAveragePrice,
		&row.FirstTradeBeforeWindowPrice, &row.LastPrice, &row.LastAmount,
		&row.NativeBestBidPrice, &row.NativeBestBidAmount, &row.NativeBestAskPrice,
		&row.NativeBestAskAmount, &row.OpenPrice, &row.HighPrice, &row.LowPrice,
		&row.BaseVolume, &row.QuoteVolume,
	}
}
