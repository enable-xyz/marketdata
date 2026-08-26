package warehouse

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/dataset"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/pathologize"
)

type ManifestReader interface {
	Plan(context.Context, CommittedManifest, string, PartitionLayout) (Generation, error)
	Scan(context.Context, CommittedManifest, GenerationID, func(Row) error) error
}

type ParquetManifestReader struct{}

type preparedManifest struct {
	input       CommittedManifest
	manifest    dataset.Manifest
	parquetPath string
}

func (ParquetManifestReader) Plan(ctx context.Context, input CommittedManifest, serverDigest string, layout PartitionLayout) (Generation, error) {
	if serverDigest == "" || len(serverDigest) > MaxIdentityBytes || strings.IndexByte(serverDigest, 0) >= 0 || !layout.valid() {
		return Generation{}, fmt.Errorf("%w: server digest or layout", ErrInvalidWarehouseInput)
	}
	prepared, err := prepareManifest(ctx, input)
	if err != nil {
		return Generation{}, err
	}
	inputHash, err := ParseHash(prepared.manifest.InputManifestSetID)
	if err != nil {
		return Generation{}, fmt.Errorf("warehouse: parse input manifest-set identity: %w", err)
	}
	schemaIdentity, err := ParseHash(prepared.manifest.SchemaSHA256)
	if err != nil {
		return Generation{}, fmt.Errorf("warehouse: parse schema identity: %w", err)
	}
	partitionValue, err := manifestPartitionValue(prepared.manifest.UTCDate, layout)
	if err != nil {
		return Generation{}, err
	}
	var eventIDs []EventID
	catalogs := make(map[Hash]struct{})
	var rowCount uint64
	if err := scanPrepared(ctx, prepared, GenerationID{}, func(row Row) error {
		eventIDs = append(eventIDs, row.EventID)
		catalogs[row.CatalogSnapshotID] = struct{}{}
		rowCount++
		return nil
	}); err != nil {
		return Generation{}, err
	}
	slices.SortFunc(eventIDs, compareHash)
	eventIDs = slices.Compact(eventIDs)
	if rowCount != prepared.manifest.ParquetRows || len(eventIDs) == 0 || len(catalogs) == 0 {
		return Generation{}, fmt.Errorf("%w: Parquet row or event-ID count", ErrInvalidWarehouseInput)
	}
	catalogIDs := make([]Hash, 0, len(catalogs))
	for id := range catalogs {
		catalogIDs = append(catalogIDs, id)
	}
	slices.SortFunc(catalogIDs, compareHash)
	catalogIdentity := hashParts("warehouse-catalog-identity-set-v1", hashesToBytes(catalogIDs)...)
	datasetIdentity := hashParts("warehouse-dataset-identity-v1", []byte(prepared.manifest.DatasetVersion),
		[]byte(prepared.manifest.BuildID), []byte(prepared.manifest.DatasetPolicyID))
	generationID := hashParts("warehouse-load-generation-v1", input.ManifestSHA256[:], inputHash[:], datasetIdentity[:],
		catalogIdentity[:], schemaIdentity[:], []byte(serverDigest), []byte(layout))
	generation := Generation{
		ID: generationID, ServerDigest: serverDigest, ManifestHash: input.ManifestSHA256, InputHash: inputHash,
		DatasetIdentity: datasetIdentity, CatalogIdentity: catalogIdentity, SchemaIdentity: schemaIdentity,
		Family: string(prepared.manifest.Family), SourceID: prepared.manifest.SourceID, UTCDate: prepared.manifest.UTCDate,
		PartitionValue: partitionValue, Layout: layout, ExpectedEventIDs: eventIDs,
		ExpectedEventSetHash: eventSetHash(eventIDs), ExpectedEventCount: uint64(len(eventIDs)),
		ExpectedRowCount: rowCount, State: GenerationPending,
	}
	if err := generation.validate(); err != nil {
		return Generation{}, err
	}
	return generation, nil
}

func (ParquetManifestReader) Scan(ctx context.Context, input CommittedManifest, generationID GenerationID, consume func(Row) error) error {
	if generationID == (GenerationID{}) || consume == nil {
		return fmt.Errorf("%w: generation identity and row consumer are required", ErrInvalidWarehouseInput)
	}
	prepared, err := prepareManifest(ctx, input)
	if err != nil {
		return err
	}
	return scanPrepared(ctx, prepared, generationID, consume)
}

func prepareManifest(ctx context.Context, input CommittedManifest) (preparedManifest, error) {
	if err := input.validate(); err != nil {
		return preparedManifest{}, err
	}
	verification, err := dataset.VerifyManifest(ctx, input.Root, input.ManifestPath)
	if err != nil {
		return preparedManifest{}, fmt.Errorf("warehouse: verify committed Parquet manifest: %w", err)
	}
	manifestPath, err := resolveContainedPath(input.Root, input.ManifestPath)
	if err != nil {
		return preparedManifest{}, err
	}
	manifestHash, err := hashBoundedFile(ctx, manifestPath, dataset.MaxManifestBytes)
	if err != nil {
		return preparedManifest{}, err
	}
	if manifestHash != input.ManifestSHA256 {
		return preparedManifest{}, fmt.Errorf("%w: committed manifest SHA-256", ErrGenerationConflict)
	}
	parquetPath, err := resolveContainedPath(input.Root, verification.Manifest.ParquetFile)
	if err != nil {
		return preparedManifest{}, err
	}
	return preparedManifest{input: input, manifest: verification.Manifest, parquetPath: parquetPath}, nil
}

func resolveContainedPath(root, candidate string) (string, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if filepath.IsAbs(candidate) {
		candidate, err = filepath.Rel(absoluteRoot, candidate)
		if err != nil {
			return "", err
		}
	}
	candidate = filepath.ToSlash(candidate)
	if !filepath.IsLocal(candidate) || strings.Contains(candidate, "\\") {
		return "", fmt.Errorf("%w: non-local dataset path", ErrInvalidWarehouseInput)
	}
	parts := strings.Split(candidate, "/")
	for _, part := range parts {
		if part == "" || !pathologize.IsClean(part) {
			return "", fmt.Errorf("%w: unsafe dataset path", ErrInvalidWarehouseInput)
		}
	}
	resolved := pathologize.Join(absoluteRoot, parts...)
	relative, err := filepath.Rel(absoluteRoot, resolved)
	if err != nil || !filepath.IsLocal(relative) {
		return "", fmt.Errorf("%w: dataset path escapes root", ErrInvalidWarehouseInput)
	}
	return resolved, nil
}

func hashBoundedFile(ctx context.Context, path string, limit int) (Hash, error) {
	file, err := os.Open(path)
	if err != nil {
		return Hash{}, err
	}
	defer file.Close()
	hasher := sha256.New()
	written, err := io.Copy(hasher, io.LimitReader(&contextReader{ctx: ctx, reader: file}, int64(limit)+1))
	if err != nil {
		return Hash{}, err
	}
	if written <= 0 || written > int64(limit) {
		return Hash{}, fmt.Errorf("%w: manifest size", ErrInvalidWarehouseInput)
	}
	var result Hash
	copy(result[:], hasher.Sum(nil))
	return result, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(bytes []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(bytes)
}

type parquetCommon struct {
	EventID            [32]byte `parquet:"event_id"`
	LogicalHash        [32]byte `parquet:"logical_hash"`
	SchemaName         string   `parquet:"schema_name,enum"`
	SchemaVersion      uint32   `parquet:"schema_version,uint(16)"`
	SourceID           string   `parquet:"source_id"`
	ChannelID          string   `parquet:"channel_id"`
	InstrumentUID      string   `parquet:"instrument_uid"`
	EpochKind          string   `parquet:"epoch_kind,enum"`
	EpochID            [16]byte `parquet:"epoch_id"`
	ArrivalOrdinal     uint64   `parquet:"arrival_ordinal,uint(64)"`
	MessageOrdinal     uint32   `parquet:"message_ordinal,uint(32)"`
	ReceivedTimeNS     int64    `parquet:"received_time_ns,timestamp(nanosecond:utc)"`
	RawSegmentSHA256   [32]byte `parquet:"raw_segment_sha256"`
	RawRecordOrdinal   uint64   `parquet:"raw_record_ordinal,uint(64)"`
	RawPayloadSHA256   [32]byte `parquet:"raw_payload_sha256"`
	CatalogSnapshotID  [32]byte `parquet:"catalog_snapshot_id"`
	DatasetPolicyID    [32]byte `parquet:"dataset_policy_id"`
	ReplayConfigID     [32]byte `parquet:"replay_config_id"`
	InputManifestSetID [32]byte `parquet:"input_manifest_set_id"`
}

type tradeSourceRow struct {
	parquetCommon
	Price  [16]byte `parquet:"price,decimal(18:38)"`
	Amount [16]byte `parquet:"amount,decimal(18:38)"`
}

type bookSourceRow struct {
	parquetCommon
	HasLevel     bool     `parquet:"has_level"`
	LevelOrdinal uint32   `parquet:"level_ordinal,uint(32)"`
	SideOrdinal  uint32   `parquet:"side_ordinal,uint(32)"`
	Price        [16]byte `parquet:"price,decimal(18:38)"`
	Amount       [16]byte `parquet:"amount,decimal(18:38)"`
}

type quoteSourceRow struct {
	parquetCommon
	BidPrice  [16]byte `parquet:"bid_price,decimal(18:38)"`
	BidAmount [16]byte `parquet:"bid_amount,decimal(18:38)"`
	AskPrice  [16]byte `parquet:"ask_price,decimal(18:38)"`
	AskAmount [16]byte `parquet:"ask_amount,decimal(18:38)"`
}

type tickerSourceRow struct {
	parquetCommon
	PriceChange                 [16]byte `parquet:"price_change,decimal(18:38)"`
	PriceChangePercent          [16]byte `parquet:"price_change_percent,decimal(8:38)"`
	WeightedAveragePrice        [16]byte `parquet:"weighted_average_price,decimal(18:38)"`
	FirstTradeBeforeWindowPrice [16]byte `parquet:"first_trade_before_window_price,decimal(18:38)"`
	LastPrice                   [16]byte `parquet:"last_price,decimal(18:38)"`
	LastAmount                  [16]byte `parquet:"last_amount,decimal(18:38)"`
	NativeBestBidPrice          [16]byte `parquet:"native_best_bid_price,decimal(18:38)"`
	NativeBestBidAmount         [16]byte `parquet:"native_best_bid_amount,decimal(18:38)"`
	NativeBestAskPrice          [16]byte `parquet:"native_best_ask_price,decimal(18:38)"`
	NativeBestAskAmount         [16]byte `parquet:"native_best_ask_amount,decimal(18:38)"`
	OpenPrice                   [16]byte `parquet:"open_price,decimal(18:38)"`
	HighPrice                   [16]byte `parquet:"high_price,decimal(18:38)"`
	LowPrice                    [16]byte `parquet:"low_price,decimal(18:38)"`
	BaseVolume                  [16]byte `parquet:"base_volume,decimal(18:38)"`
	QuoteVolume                 [16]byte `parquet:"quote_volume,decimal(18:38)"`
}

type auxiliarySourceRow struct {
	RowLogicalHash     [32]byte `parquet:"row_logical_hash"`
	SourceID           string   `parquet:"source_id"`
	ChannelID          string   `parquet:"channel_id"`
	ReceivedTimeNS     int64    `parquet:"received_time_ns,timestamp(nanosecond:utc)"`
	EpochKind          string   `parquet:"epoch_kind,enum"`
	EpochID            [16]byte `parquet:"epoch_id"`
	ArrivalOrdinal     uint64   `parquet:"arrival_ordinal,uint(64)"`
	MessageOrdinal     uint32   `parquet:"message_ordinal,uint(32)"`
	RawSegmentSHA256   [32]byte `parquet:"raw_segment_sha256"`
	RawRecordOrdinal   uint64   `parquet:"raw_record_ordinal,uint(64)"`
	RawPayloadSHA256   [32]byte `parquet:"raw_payload_sha256"`
	CatalogSnapshotID  [32]byte `parquet:"catalog_snapshot_id"`
	DatasetPolicyID    [32]byte `parquet:"dataset_policy_id"`
	ReplayConfigID     [32]byte `parquet:"replay_config_id"`
	InputManifestSetID [32]byte `parquet:"input_manifest_set_id"`
}

type quarantineSourceRow struct {
	auxiliarySourceRow
	QuarantineID [32]byte `parquet:"quarantine_id"`
}

type qualitySourceRow struct {
	auxiliarySourceRow
	InstrumentUID string   `parquet:"instrument_uid"`
	QualityID     [32]byte `parquet:"quality_id"`
	SchemaName    string   `parquet:"schema_name"`
	SchemaVersion uint32   `parquet:"schema_version,uint(16)"`
}

func scanPrepared(ctx context.Context, prepared preparedManifest, generationID GenerationID, consume func(Row) error) error {
	file, err := os.Open(prepared.parquetPath)
	if err != nil {
		return err
	}
	defer file.Close()
	var scanErr error
	switch prepared.manifest.Family {
	case dataset.FamilyTrade:
		scanErr = scanRows(ctx, parquet.NewGenericReader[tradeSourceRow](file), func(value tradeSourceRow) (Row, error) {
			row := commonRow(prepared, generationID, value.parquetCommon, 0)
			row.Price = decimalFromBytes(value.Price, 18)
			row.Amount = decimalFromBytes(value.Amount, 18)
			return row, nil
		}, consume)
	case dataset.FamilyBookUpdate:
		scanErr = scanRows(ctx, parquet.NewGenericReader[bookSourceRow](file), func(value bookSourceRow) (Row, error) {
			physicalOrdinal := uint64(2) << 32
			if value.HasLevel {
				physicalOrdinal = uint64(value.SideOrdinal)<<32 | uint64(value.LevelOrdinal)
			}
			row := commonRow(prepared, generationID, value.parquetCommon, physicalOrdinal)
			if value.HasLevel {
				row.Price = decimalFromBytes(value.Price, 18)
				row.Amount = decimalFromBytes(value.Amount, 18)
			}
			return row, nil
		}, consume)
	case dataset.FamilyQuote:
		scanErr = scanRows(ctx, parquet.NewGenericReader[quoteSourceRow](file), func(value quoteSourceRow) (Row, error) {
			row := commonRow(prepared, generationID, value.parquetCommon, 0)
			row.BidPrice = decimalFromBytes(value.BidPrice, 18)
			row.BidAmount = decimalFromBytes(value.BidAmount, 18)
			row.AskPrice = decimalFromBytes(value.AskPrice, 18)
			row.AskAmount = decimalFromBytes(value.AskAmount, 18)
			return row, nil
		}, consume)
	case dataset.FamilyTicker:
		scanErr = scanRows(ctx, parquet.NewGenericReader[tickerSourceRow](file), func(value tickerSourceRow) (Row, error) {
			row := commonRow(prepared, generationID, value.parquetCommon, 0)
			row.PriceChange = decimalFromBytes(value.PriceChange, 18)
			row.PriceChangePercent = decimalFromBytes(value.PriceChangePercent, 8)
			row.WeightedAveragePrice = decimalFromBytes(value.WeightedAveragePrice, 18)
			row.FirstTradeBeforeWindowPrice = decimalFromBytes(value.FirstTradeBeforeWindowPrice, 18)
			row.LastPrice = decimalFromBytes(value.LastPrice, 18)
			row.LastAmount = decimalFromBytes(value.LastAmount, 18)
			row.NativeBestBidPrice = decimalFromBytes(value.NativeBestBidPrice, 18)
			row.NativeBestBidAmount = decimalFromBytes(value.NativeBestBidAmount, 18)
			row.NativeBestAskPrice = decimalFromBytes(value.NativeBestAskPrice, 18)
			row.NativeBestAskAmount = decimalFromBytes(value.NativeBestAskAmount, 18)
			row.OpenPrice = decimalFromBytes(value.OpenPrice, 18)
			row.HighPrice = decimalFromBytes(value.HighPrice, 18)
			row.LowPrice = decimalFromBytes(value.LowPrice, 18)
			row.BaseVolume = decimalFromBytes(value.BaseVolume, 18)
			row.QuoteVolume = decimalFromBytes(value.QuoteVolume, 18)
			return row, nil
		}, consume)
	case dataset.FamilySchemaQuarantine:
		scanErr = scanRows(ctx, parquet.NewGenericReader[quarantineSourceRow](file), func(value quarantineSourceRow) (Row, error) {
			return auxiliaryRow(prepared, generationID, value.auxiliarySourceRow, value.QuarantineID, "", 0), nil
		}, consume)
	case dataset.FamilyQuality:
		scanErr = scanRows(ctx, parquet.NewGenericReader[qualitySourceRow](file), func(value qualitySourceRow) (Row, error) {
			row := auxiliaryRow(prepared, generationID, value.auxiliarySourceRow, value.QualityID, value.InstrumentUID, 0)
			row.SchemaName = value.SchemaName
			row.SchemaVersion = uint16(value.SchemaVersion)
			return row, nil
		}, consume)
	default:
		return fmt.Errorf("%w: unsupported committed Parquet family %q", ErrInvalidWarehouseInput, prepared.manifest.Family)
	}
	if scanErr != nil {
		return fmt.Errorf("warehouse: scan committed Parquet rows: %w", scanErr)
	}
	return nil
}

type genericReader[T any] interface {
	Read([]T) (int, error)
	Close() error
}

func scanRows[T any](ctx context.Context, reader genericReader[T], convert func(T) (Row, error), consume func(Row) error) error {
	defer reader.Close()
	batch := make([]T, 256)
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		n, err := reader.Read(batch)
		for i := range n {
			row, convertErr := convert(batch[i])
			if convertErr != nil {
				return convertErr
			}
			if consumeErr := consume(row); consumeErr != nil {
				return consumeErr
			}
		}
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func commonRow(prepared preparedManifest, generationID GenerationID, value parquetCommon, physicalOrdinal uint64) Row {
	eventID := Hash(value.EventID)
	return Row{
		GenerationID: generationID, ManifestHash: prepared.input.ManifestSHA256, RowID: rowIdentity(eventID, physicalOrdinal),
		EventID: eventID, LogicalHash: Hash(value.LogicalHash), Family: string(prepared.manifest.Family),
		SourceID: value.SourceID, ChannelID: value.ChannelID, InstrumentUID: value.InstrumentUID,
		EpochKind: value.EpochKind, ConnectionEpoch: value.EpochID, ReceivedTimeNS: value.ReceivedTimeNS,
		ArrivalOrdinal: value.ArrivalOrdinal, MessageOrdinal: value.MessageOrdinal,
		RawSegmentSHA256: Hash(value.RawSegmentSHA256), RawRecordOrdinal: value.RawRecordOrdinal,
		RawPayloadSHA256: Hash(value.RawPayloadSHA256), CatalogSnapshotID: Hash(value.CatalogSnapshotID),
		SchemaName: value.SchemaName, SchemaVersion: uint16(value.SchemaVersion),
		DatasetPolicyID: Hash(value.DatasetPolicyID), ReplayConfigID: Hash(value.ReplayConfigID),
		InputManifestSetID: Hash(value.InputManifestSetID), PhysicalOrdinal: physicalOrdinal,
	}
}

func auxiliaryRow(prepared preparedManifest, generationID GenerationID, value auxiliarySourceRow, eventID [32]byte, instrumentUID string, physicalOrdinal uint64) Row {
	id := Hash(eventID)
	return Row{
		GenerationID: generationID, ManifestHash: prepared.input.ManifestSHA256, RowID: rowIdentity(id, physicalOrdinal),
		EventID: id, LogicalHash: Hash(value.RowLogicalHash), Family: string(prepared.manifest.Family),
		SourceID: value.SourceID, ChannelID: value.ChannelID, InstrumentUID: instrumentUID,
		EpochKind: value.EpochKind, ConnectionEpoch: value.EpochID, ReceivedTimeNS: value.ReceivedTimeNS,
		ArrivalOrdinal: value.ArrivalOrdinal, MessageOrdinal: value.MessageOrdinal,
		RawSegmentSHA256: Hash(value.RawSegmentSHA256), RawRecordOrdinal: value.RawRecordOrdinal,
		RawPayloadSHA256: Hash(value.RawPayloadSHA256), CatalogSnapshotID: Hash(value.CatalogSnapshotID),
		SchemaName: prepared.manifest.SchemaName, SchemaVersion: prepared.manifest.SchemaVersion,
		DatasetPolicyID: Hash(value.DatasetPolicyID), ReplayConfigID: Hash(value.ReplayConfigID),
		InputManifestSetID: Hash(value.InputManifestSetID), PhysicalOrdinal: physicalOrdinal,
	}
}

func decimalFromBytes(encoded [16]byte, scale uint8) *Decimal {
	coefficient := new(big.Int).SetBytes(encoded[:])
	if encoded[0]&0x80 != 0 {
		coefficient.Sub(coefficient, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	return &Decimal{Coefficient: coefficient.String(), Scale: scale}
}

func manifestPartitionValue(utcDate string, layout PartitionLayout) (uint32, error) {
	date, err := time.Parse("2006-01-02", utcDate)
	if err != nil {
		return 0, fmt.Errorf("%w: manifest UTC date", ErrInvalidWarehouseInput)
	}
	if layout == PartitionMonth {
		return uint32(date.Year()*100 + int(date.Month())), nil
	}
	if layout == PartitionDate {
		return uint32(date.Year()*10_000 + int(date.Month())*100 + date.Day()), nil
	}
	return 0, fmt.Errorf("%w: partition layout", ErrInvalidWarehouseInput)
}

func hashParts(domain string, parts ...[]byte) Hash {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(domain))
	_, _ = hasher.Write([]byte{0})
	var size [8]byte
	for _, part := range parts {
		binary.BigEndian.PutUint64(size[:], uint64(len(part)))
		_, _ = hasher.Write(size[:])
		_, _ = hasher.Write(part)
	}
	var result Hash
	copy(result[:], hasher.Sum(nil))
	return result
}

func hashesToBytes(values []Hash) [][]byte {
	result := make([][]byte, len(values))
	for i := range values {
		result[i] = values[i][:]
	}
	return result
}
