package dataset

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/normalize"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
	"github.com/spf13/fileflow"
	"github.com/spf13/pathologize"
)

type partitionCoordinates struct {
	family       Family
	sourceID     string
	date         string
	hour         string
	firstTimeNS  int64
	lastTimeNS   int64
	firstArrival uint64
	lastArrival  uint64
	epochs       []ManifestEpoch
}

type epochKey struct {
	kind string
	id   [16]byte
}

type epochOrder struct {
	key                 epochKey
	firstReceivedTimeNS int64
	firstArrivalOrdinal uint64
	firstMessageOrdinal uint32
	lastArrivalOrdinal  uint64
}

type partitionWriter interface {
	Write(any) (int, error)
	Flush() error
	Close() error
	Size() int64
	SetKeyValueMetadata(string, string)
}

type genericPartitionWriter[T any] struct {
	writer *parquet.GenericWriter[T]
}

func (w *genericPartitionWriter[T]) Write(value any) (int, error) {
	row, ok := value.(T)
	if !ok {
		return 0, fmt.Errorf("%w: writer row type", ErrInvalidInput)
	}
	return w.writer.Write([]T{row})
}
func (w *genericPartitionWriter[T]) Flush() error { return w.writer.Flush() }
func (w *genericPartitionWriter[T]) Close() error { return w.writer.Close() }
func (w *genericPartitionWriter[T]) Size() int64  { return w.writer.Size() }
func (w *genericPartitionWriter[T]) SetKeyValueMetadata(key, value string) {
	w.writer.SetKeyValueMetadata(key, value)
}

func BuildNormalizedPartition(ctx context.Context, root string, source NormalizedSource, options WriterOptions) (BuildResult, error) {
	if source == nil {
		return BuildResult{}, fmt.Errorf("%w: nil normalized source", ErrInvalidInput)
	}
	if err := options.validate(); err != nil {
		return BuildResult{}, err
	}
	rows, coordinates, err := collectNormalizedRows(ctx, source, options)
	if err != nil {
		return BuildResult{}, err
	}
	return buildNormalized(ctx, root, rows, coordinates, options)
}

func buildNormalized(ctx context.Context, root string, rows []normalize.Row, coordinates partitionCoordinates, options WriterOptions) (result BuildResult, err error) {
	staging, err := newStaging(root)
	if err != nil {
		return result, err
	}
	defer func() {
		if err != nil {
			_ = os.Remove(staging)
		}
	}()
	file, err := os.OpenFile(staging, os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return result, fmt.Errorf("dataset: open staging parquet: %w", err)
	}
	writer, schemaName, schemaVersion, schema, err := newNormalizedWriter(file, coordinates.family, options, filepath.Dir(staging))
	if err != nil {
		_ = file.Close()
		return result, err
	}
	closed := false
	defer func() {
		if !closed {
			_ = writer.Close()
			_ = file.Close()
		}
	}()
	schemaHash := schemaDigest(schemaName, schemaVersion, schema)
	setStaticMetadata(writer, schemaName, schemaVersion, schemaHash, options)
	logical := sha256.New()
	_, _ = logical.Write([]byte("dataset-normalized-partition-logical-v1\x00"))
	var parquetRows uint64
	lastFlushSize := int64(0)
	for ordinal := range rows {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		row := rows[ordinal]
		if _, err := logical.Write(row.LogicalHash[:]); err != nil {
			return result, err
		}
		written, err := writeNormalizedRow(writer, row, uint64(ordinal), options)
		if err != nil {
			return result, err
		}
		parquetRows += uint64(written)
		if parquetRows > options.MaxParquetRows {
			return result, fmt.Errorf("%w: flattened parquet row bound", ErrInvalidInput)
		}
		if writer.Size() > MaxPartitionFileBytes {
			return result, fmt.Errorf("%w: parquet file byte bound", ErrInvalidInput)
		}
		if writer.Size()-lastFlushSize >= options.RowGroupTargetBytes {
			if err := writer.Flush(); err != nil {
				return result, fmt.Errorf("dataset: flush row group: %w", err)
			}
			lastFlushSize = writer.Size()
		}
	}
	inputRows := uint64(len(rows))
	logicalHash := sumHash(logical)
	writer.SetKeyValueMetadata("enable.logical_sha256", hex.EncodeToString(logicalHash[:]))
	writer.SetKeyValueMetadata("enable.input_rows", fmt.Sprint(inputRows))
	writer.SetKeyValueMetadata("enable.parquet_rows", fmt.Sprint(parquetRows))
	if err := writer.Close(); err != nil {
		return result, fmt.Errorf("dataset: close parquet writer: %w", err)
	}
	closed = true
	if err := file.Sync(); err != nil {
		return result, fmt.Errorf("dataset: sync parquet staging file: %w", err)
	}
	if err := file.Close(); err != nil {
		return result, fmt.Errorf("dataset: close parquet staging file: %w", err)
	}
	return finalizePartition(ctx, root, staging, coordinates, schemaName, schemaVersion, schemaHash, logicalHash, inputRows, parquetRows, options)
}

func collectNormalizedRows(ctx context.Context, source NormalizedSource, options WriterOptions) ([]normalize.Row, partitionCoordinates, error) {
	rows := make([]normalize.Row, 0, min(options.MaxInputRows, 4096))
	var coordinates partitionCoordinates
	epochs := make(map[epochKey]epochOrder)
	for {
		if err := ctx.Err(); err != nil {
			return nil, coordinates, err
		}
		row, err := source.Next(ctx)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, coordinates, fmt.Errorf("dataset: read normalized row: %w", err)
		}
		if uint64(len(rows)) >= options.MaxInputRows {
			return nil, coordinates, fmt.Errorf("%w: normalized input row bound", ErrInvalidInput)
		}
		if err := row.Validate(); err != nil {
			return nil, coordinates, fmt.Errorf("%w: normalized row %d: %v", ErrInvalidInput, len(rows), err)
		}
		family, err := normalizedFamily(row)
		if err != nil {
			return nil, coordinates, err
		}
		metadata := row.Common()
		date, hour := utcParts(metadata.ReceivedTimeNS)
		if len(rows) == 0 {
			coordinates = partitionCoordinates{family: family, sourceID: metadata.SourceID, date: date, hour: hour,
				firstTimeNS: metadata.ReceivedTimeNS, lastTimeNS: metadata.ReceivedTimeNS,
				firstArrival: metadata.ArrivalOrdinal, lastArrival: metadata.ArrivalOrdinal}
		} else if family != coordinates.family || metadata.SourceID != coordinates.sourceID || date != coordinates.date || hour != coordinates.hour {
			return nil, coordinates, fmt.Errorf("%w at input row %d", ErrPartitionBoundary, len(rows))
		}
		coordinates.include(metadata.ReceivedTimeNS, metadata.ArrivalOrdinal)
		key := epochKey{kind: string(metadata.EpochKind), id: metadata.EpochID}
		group, ok := epochs[key]
		if !ok {
			group = epochOrder{key: key, firstReceivedTimeNS: metadata.ReceivedTimeNS,
				firstArrivalOrdinal: metadata.ArrivalOrdinal, firstMessageOrdinal: metadata.MessageOrdinal,
				lastArrivalOrdinal: metadata.ArrivalOrdinal}
		} else {
			if metadata.ArrivalOrdinal < group.firstArrivalOrdinal ||
				(metadata.ArrivalOrdinal == group.firstArrivalOrdinal && metadata.MessageOrdinal < group.firstMessageOrdinal) {
				group.firstArrivalOrdinal = metadata.ArrivalOrdinal
				group.firstMessageOrdinal = metadata.MessageOrdinal
				group.firstReceivedTimeNS = metadata.ReceivedTimeNS
			}
			group.lastArrivalOrdinal = max(group.lastArrivalOrdinal, metadata.ArrivalOrdinal)
		}
		epochs[key] = group
		rows = append(rows, row)
	}
	if len(rows) == 0 {
		return nil, coordinates, fmt.Errorf("%w: empty partition", ErrInvalidInput)
	}
	slices.SortFunc(rows, func(a, b normalize.Row) int {
		left, right := a.Common(), b.Common()
		if order := cmp.Compare(left.InstrumentUID, right.InstrumentUID); order != 0 {
			return order
		}
		leftGroup := epochs[epochKey{kind: string(left.EpochKind), id: left.EpochID}]
		rightGroup := epochs[epochKey{kind: string(right.EpochKind), id: right.EpochID}]
		if order := cmp.Compare(leftGroup.firstReceivedTimeNS, rightGroup.firstReceivedTimeNS); order != 0 {
			return order
		}
		if order := cmp.Compare(leftGroup.key.kind, rightGroup.key.kind); order != 0 {
			return order
		}
		if order := bytes.Compare(leftGroup.key.id[:], rightGroup.key.id[:]); order != 0 {
			return order
		}
		if order := cmp.Compare(left.ArrivalOrdinal, right.ArrivalOrdinal); order != 0 {
			return order
		}
		if order := cmp.Compare(left.MessageOrdinal, right.MessageOrdinal); order != 0 {
			return order
		}
		return bytes.Compare(left.EventID[:], right.EventID[:])
	})
	for index := 1; index < len(rows); index++ {
		previous, current := rows[index-1].Common(), rows[index].Common()
		if previous.InstrumentUID == current.InstrumentUID && previous.EpochKind == current.EpochKind && previous.EpochID == current.EpochID &&
			previous.ArrivalOrdinal == current.ArrivalOrdinal && previous.MessageOrdinal == current.MessageOrdinal {
			return nil, coordinates, fmt.Errorf("%w: duplicate source coordinate after instrument/epoch ordering", ErrInvalidInput)
		}
	}
	groups := make([]epochOrder, 0, len(epochs))
	for _, group := range epochs {
		groups = append(groups, group)
	}
	slices.SortFunc(groups, compareEpochOrder)
	coordinates.epochs = make([]ManifestEpoch, len(groups))
	for i, group := range groups {
		coordinates.epochs[i] = ManifestEpoch{Kind: group.key.kind, ID: hex.EncodeToString(group.key.id[:]),
			FirstReceivedTimeNS: group.firstReceivedTimeNS, FirstArrivalOrdinal: group.firstArrivalOrdinal,
			LastArrivalOrdinal: group.lastArrivalOrdinal}
	}
	return rows, coordinates, nil
}

func compareEpochOrder(left, right epochOrder) int {
	if order := cmp.Compare(left.firstReceivedTimeNS, right.firstReceivedTimeNS); order != 0 {
		return order
	}
	if order := cmp.Compare(left.key.kind, right.key.kind); order != 0 {
		return order
	}
	return bytes.Compare(left.key.id[:], right.key.id[:])
}

func normalizedFamily(row normalize.Row) (Family, error) {
	switch row.Kind {
	case normalize.EventTrade:
		return FamilyTrade, nil
	case normalize.EventBookUpdate:
		return FamilyBookUpdate, nil
	case normalize.EventQuote:
		return FamilyQuote, nil
	case normalize.EventTicker:
		return FamilyTicker, nil
	default:
		return "", fmt.Errorf("%w: unknown normalized family", ErrInvalidInput)
	}
}

func newNormalizedWriter(output io.Writer, family Family, options WriterOptions, scratch string) (partitionWriter, string, uint16, *parquet.Schema, error) {
	name, version, schema, err := familySchema(family)
	if err != nil {
		return nil, "", 0, nil, err
	}
	writerOptions := parquetWriterOptions(schema, family, options, scratch)
	switch family {
	case FamilyTrade:
		return &genericPartitionWriter[tradeParquetRow]{parquet.NewGenericWriter[tradeParquetRow](output, writerOptions...)}, name, version, schema, nil
	case FamilyBookUpdate:
		return &genericPartitionWriter[bookParquetRow]{parquet.NewGenericWriter[bookParquetRow](output, writerOptions...)}, name, version, schema, nil
	case FamilyQuote:
		return &genericPartitionWriter[quoteParquetRow]{parquet.NewGenericWriter[quoteParquetRow](output, writerOptions...)}, name, version, schema, nil
	case FamilyTicker:
		return &genericPartitionWriter[tickerParquetRow]{parquet.NewGenericWriter[tickerParquetRow](output, writerOptions...)}, name, version, schema, nil
	default:
		return nil, "", 0, nil, fmt.Errorf("%w: normalized writer family", ErrInvalidInput)
	}
}

func parquetWriterOptions(schema *parquet.Schema, family Family, options WriterOptions, scratch string) []parquet.WriterOption {
	result := []parquet.WriterOption{
		schema,
		parquet.CreatedBy("enable-market-dataset", "1", ParquetWriterVersion),
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault, Concurrency: 1}),
		parquet.PageBufferSize(options.PageBufferBytes),
		parquet.DataPageVersion(2),
		parquet.DataPageStatistics(true),
		parquet.ColumnPageBuffers(parquet.NewFileBufferPool(scratch, ".parquet-pages.*")),
	}
	if options.Dictionary {
		result = append(result, parquet.DefaultEncodingFor(parquet.ByteArray, &parquet.RLEDictionary), parquet.DictionaryMaxBytes(16<<20))
	}
	if options.BloomFilter {
		column := "event_id"
		switch family {
		case FamilySchemaQuarantine:
			column = "quarantine_id"
		case FamilyQuality:
			column = "quality_id"
		case FamilyOpportunity:
			column = "opportunity_id"
		}
		result = append(result, parquet.BloomFilters(parquet.SplitBlockFilter(10, column)))
	}
	return result
}

func writeNormalizedRow(writer partitionWriter, row normalize.Row, ordinal uint64, options WriterOptions) (int, error) {
	common, err := commonFromRow(row, ordinal, options)
	if err != nil {
		return 0, err
	}
	switch row.Kind {
	case normalize.EventTrade:
		event := row.Trade
		price, err := decimalBytes(event.Price.Decimal, normalize.CanonicalPriceScale)
		if err != nil {
			return 0, err
		}
		amount, err := decimalBytes(event.Amount.Decimal, normalize.CanonicalAmountScale)
		if err != nil {
			return 0, err
		}
		_, err = writer.Write(tradeParquetRow{commonColumns: common, NativeTradeID: event.NativeTradeID,
			AggressorSide: string(event.AggressorSide), BuyerIsMaker: event.BuyerIsMaker, NativeIgnoreFlag: event.NativeIgnoreFlag,
			Price: price, Amount: amount, BaseAssetID: event.Amount.Unit.AssetID, QuoteAssetID: event.Price.Unit.QuoteAssetID,
			AggregationKind: string(event.AggregationKind), NativeDuplicateState: string(event.NativeDuplicateStatus)})
		return 1, err
	case normalize.EventBookUpdate:
		return writeBookRows(writer, common, row.BookUpdate)
	case normalize.EventQuote:
		event := row.Quote
		bidPrice, err := decimalBytes(event.BidPrice.Decimal, normalize.CanonicalPriceScale)
		if err != nil {
			return 0, err
		}
		bidAmount, err := decimalBytes(event.BidAmount.Decimal, normalize.CanonicalAmountScale)
		if err != nil {
			return 0, err
		}
		askPrice, err := decimalBytes(event.AskPrice.Decimal, normalize.CanonicalPriceScale)
		if err != nil {
			return 0, err
		}
		askAmount, err := decimalBytes(event.AskAmount.Decimal, normalize.CanonicalAmountScale)
		if err != nil {
			return 0, err
		}
		sourceTime, sourceState := optionalInt64(event.SourceTimeNS)
		_, err = writer.Write(quoteParquetRow{commonColumns: common, NativeSourceRole: event.NativeSourceRole, UpdateID: event.UpdateID,
			BidPrice: bidPrice, BidAmount: bidAmount, AskPrice: askPrice, AskAmount: askAmount,
			BaseAssetID: event.BidAmount.Unit.AssetID, QuoteAssetID: event.BidPrice.Unit.QuoteAssetID,
			RPIInclusionState: string(event.RPIInclusionState), SourceTimeNS: sourceTime, SourceTimeState: sourceState})
		return 1, err
	case normalize.EventTicker:
		value, err := tickerRow(common, row.Ticker)
		if err != nil {
			return 0, err
		}
		_, err = writer.Write(value)
		return 1, err
	default:
		return 0, fmt.Errorf("%w: normalized row family", ErrInvalidInput)
	}
}

func writeBookRows(writer partitionWriter, common commonColumns, event *normalize.BookUpdateV1) (int, error) {
	previous, previousState := optionalUint64(event.PreviousSequence)
	base := bookParquetRow{commonColumns: common, UpdateKind: string(event.UpdateKind), DepthContract: event.DepthContract,
		AggregationContract: event.AggregationContract, FirstSequence: event.FirstSequence, LastSequence: event.LastSequence,
		PreviousSequence: previous, PreviousSequenceState: previousState, ChecksumState: string(event.Checksum),
		AmountSemantics: event.AmountSemantics, ReconstructionEligibility: event.ReconstructionEligibility,
		BidCount: uint32(len(event.Bids)), AskCount: uint32(len(event.Asks))}
	if len(event.Bids)+len(event.Asks) == 0 {
		_, err := writer.Write(base)
		return 1, err
	}
	written := 0
	for _, level := range event.Bids {
		row, err := bookLevelRow(base, level, uint32(written))
		if err != nil {
			return written, err
		}
		if _, err := writer.Write(row); err != nil {
			return written, err
		}
		written++
	}
	for _, level := range event.Asks {
		row, err := bookLevelRow(base, level, uint32(written))
		if err != nil {
			return written, err
		}
		if _, err := writer.Write(row); err != nil {
			return written, err
		}
		written++
	}
	return written, nil
}

func bookLevelRow(base bookParquetRow, level normalize.BookLevel, ordinal uint32) (bookParquetRow, error) {
	price, err := decimalBytes(level.Price.Decimal, normalize.CanonicalPriceScale)
	if err != nil {
		return bookParquetRow{}, err
	}
	amount, err := decimalBytes(level.Amount.Decimal, normalize.CanonicalAmountScale)
	if err != nil {
		return bookParquetRow{}, err
	}
	base.HasLevel = true
	base.LevelOrdinal = ordinal
	base.SideOrdinal = level.LevelOrdinal
	base.Side = string(level.Side)
	base.Action = string(level.Action)
	base.Price = price
	base.Amount = amount
	base.PriceBaseAssetID = level.Price.Unit.BaseAssetID
	base.PriceQuoteAssetID = level.Price.Unit.QuoteAssetID
	base.AmountAssetID = level.Amount.Unit.AssetID
	return base, nil
}

func tickerRow(common commonColumns, event *normalize.TickerV1) (tickerParquetRow, error) {
	price := func(value normalize.Numeric) ([16]byte, error) {
		return decimalBytes(value.Decimal, normalize.CanonicalPriceScale)
	}
	amount := func(value normalize.Numeric) ([16]byte, error) {
		return decimalBytes(value.Decimal, normalize.CanonicalAmountScale)
	}
	var row tickerParquetRow
	row.commonColumns = common
	row.NativeSourceRole, row.WindowKind = event.NativeSourceRole, string(event.WindowKind)
	row.WindowOpenSemantics, row.WindowCloseSemantics = event.WindowOpenSemantics, event.WindowCloseSemantics
	row.WindowOpenTimeNS, row.WindowCloseTimeNS = event.WindowOpenTimeNS, event.WindowCloseTimeNS
	row.WindowTimeResolution, row.NominalWindowDurationNS = string(event.WindowTimeResolution), event.NominalWindowDurationNS
	var err error
	if row.PriceChange, err = price(event.PriceChange); err != nil {
		return row, err
	}
	if row.PriceChangePercent, err = decimalBytes(event.PriceChangePercent.Decimal, normalize.CanonicalPercentScale); err != nil {
		return row, err
	}
	if row.WeightedAveragePrice, err = price(event.WeightedAveragePrice); err != nil {
		return row, err
	}
	if row.FirstTradeBeforeWindowPrice, err = price(event.FirstTradeBeforeWindowPrice); err != nil {
		return row, err
	}
	if row.LastPrice, err = price(event.LastPrice); err != nil {
		return row, err
	}
	if row.LastAmount, err = amount(event.LastAmount); err != nil {
		return row, err
	}
	if row.NativeBestBidPrice, err = price(event.NativeBestBidPrice); err != nil {
		return row, err
	}
	if row.NativeBestBidAmount, err = amount(event.NativeBestBidAmount); err != nil {
		return row, err
	}
	if row.NativeBestAskPrice, err = price(event.NativeBestAskPrice); err != nil {
		return row, err
	}
	if row.NativeBestAskAmount, err = amount(event.NativeBestAskAmount); err != nil {
		return row, err
	}
	if row.OpenPrice, err = price(event.OpenPrice); err != nil {
		return row, err
	}
	if row.HighPrice, err = price(event.HighPrice); err != nil {
		return row, err
	}
	if row.LowPrice, err = price(event.LowPrice); err != nil {
		return row, err
	}
	if row.BaseVolume, err = amount(event.BaseVolume); err != nil {
		return row, err
	}
	if row.QuoteVolume, err = amount(event.QuoteVolume); err != nil {
		return row, err
	}
	row.BaseAssetID, row.QuoteAssetID = event.BaseVolume.Unit.AssetID, event.QuoteVolume.Unit.AssetID
	row.FirstTradeID, row.LastTradeID, row.TradeCount = event.FirstTradeID, event.LastTradeID, event.TradeCount
	return row, nil
}

func (p *partitionCoordinates) include(receivedTimeNS int64, arrivalOrdinal uint64) {
	if receivedTimeNS < p.firstTimeNS {
		p.firstTimeNS = receivedTimeNS
	}
	if receivedTimeNS > p.lastTimeNS {
		p.lastTimeNS = receivedTimeNS
	}
	if arrivalOrdinal < p.firstArrival {
		p.firstArrival = arrivalOrdinal
	}
	if arrivalOrdinal > p.lastArrival {
		p.lastArrival = arrivalOrdinal
	}
}

func utcParts(ns int64) (string, string) {
	value := time.Unix(0, ns).UTC()
	return value.Format("2006-01-02"), value.Format("15")
}

func newStaging(root string) (string, error) {
	if root == "" {
		return "", fmt.Errorf("%w: empty output root", ErrInvalidInput)
	}
	info, err := os.Lstat(root)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", fmt.Errorf("%w: output root must already exist", ErrInvalidInput)
		}
		return "", fmt.Errorf("dataset: inspect output root: %w", err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("%w: output root must be a real directory", ErrInvalidInput)
	}
	file, err := os.CreateTemp(root, ".parquet-staging.*")
	if err != nil {
		return "", fmt.Errorf("dataset: create staging parquet: %w", err)
	}
	path := file.Name()
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return "", err
	}
	return path, nil
}

func setStaticMetadata(writer partitionWriter, schemaName string, schemaVersion uint16, schemaHash [32]byte, options WriterOptions) {
	values := [][2]string{
		{"enable.dataset_version", DatasetVersion}, {"enable.parquet_writer_version", ParquetWriterVersion},
		{"enable.parquet_format_compatibility", ParquetFormatCompatibility}, {"enable.schema_name", schemaName},
		{"enable.schema_version", fmt.Sprint(schemaVersion)}, {"enable.schema_sha256", hex.EncodeToString(schemaHash[:])},
		{"enable.dataset_policy_id", hashHex(options.DatasetPolicyID)}, {"enable.replay_config_id", hashHex(options.ReplayConfigID)},
		{"enable.input_manifest_set_id", hashHex(options.InputManifestSetID)},
	}
	for _, value := range values {
		writer.SetKeyValueMetadata(value[0], value[1])
	}
}

func sumHash(value hash.Hash) [32]byte {
	var result [32]byte
	copy(result[:], value.Sum(nil))
	return result
}

func finalizePartition(ctx context.Context, root, staging string, coordinates partitionCoordinates, schemaName string, schemaVersion uint16,
	schemaHash, logicalHash [32]byte, inputRows, parquetRows uint64, options WriterOptions) (BuildResult, error) {
	physicalHash, fileBytes, err := hashFileContext(ctx, staging)
	if err != nil {
		return BuildResult{}, err
	}
	partitionDir := pathologize.Join(root, "dataset-v1", "family="+string(coordinates.family), "source="+coordinates.sourceID,
		"date="+coordinates.date, "hour="+coordinates.hour)
	epochIdentity := manifestEpochIdentity(coordinates.epochs)
	parquetName := "part-epochs-" + epochIdentity[:16] + "-" + hex.EncodeToString(physicalHash[:]) + ".parquet"
	parquetPath := filepath.Join(partitionDir, parquetName)
	finalPath, err := fileflow.Move(staging, parquetPath)
	if err != nil {
		return BuildResult{}, fmt.Errorf("dataset: publish parquet file: %w", err)
	}
	if finalPath != parquetPath {
		return BuildResult{}, fmt.Errorf("dataset: content-address collision at %s", parquetPath)
	}
	if err := syncPublicationDirectories(root, partitionDir, options.DirectorySync); err != nil {
		return BuildResult{}, fmt.Errorf("dataset: sync published parquet directories: %w", err)
	}
	relative, err := filepath.Rel(root, parquetPath)
	if err != nil {
		return BuildResult{}, err
	}
	stats, err := inspectParquet(ctx, parquetPath)
	if err != nil {
		return BuildResult{}, err
	}
	manifest := Manifest{
		ManifestVersion: ManifestVersion, DatasetVersion: DatasetVersion, ParquetWriterVersion: ParquetWriterVersion,
		ParquetFormatCompatibility: ParquetFormatCompatibility, SchemaName: schemaName, SchemaVersion: schemaVersion,
		SchemaSHA256: hex.EncodeToString(schemaHash[:]), Family: coordinates.family, SourceID: coordinates.sourceID,
		Epochs: slices.Clone(coordinates.epochs), UTCDate: coordinates.date, UTCHour: coordinates.hour,
		FirstReceivedTimeNS: coordinates.firstTimeNS, LastReceivedTimeNS: coordinates.lastTimeNS,
		FirstArrivalOrdinal: coordinates.firstArrival, LastArrivalOrdinal: coordinates.lastArrival,
		InputRows: inputRows, ParquetRows: parquetRows, RowGroups: stats.rowGroups, Pages: stats.pages, Values: stats.values,
		LogicalSHA256: hex.EncodeToString(logicalHash[:]), PhysicalSHA256: hex.EncodeToString(physicalHash[:]), FileBytes: fileBytes,
		ParquetFile: filepath.ToSlash(relative), DatasetPolicyID: hashHex(options.DatasetPolicyID), ReplayConfigID: hashHex(options.ReplayConfigID),
		InputManifestSetID: hashHex(options.InputManifestSetID), Options: ManifestOptions{RowGroupTargetBytes: options.RowGroupTargetBytes,
			PageBufferBytes: options.PageBufferBytes, Compression: options.Compression, Dictionary: options.Dictionary, BloomFilter: options.BloomFilter},
	}
	manifest.BuildID = manifestBuildID(manifest)
	if _, err := verifyParquetFile(ctx, parquetPath, manifest); err != nil {
		return BuildResult{}, err
	}
	bytes, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return BuildResult{}, err
	}
	bytes = append(bytes, '\n')
	manifestHash := sha256.Sum256(bytes)
	manifestTemp, err := os.CreateTemp(root, ".manifest-staging.*")
	if err != nil {
		return BuildResult{}, err
	}
	manifestTempPath := manifestTemp.Name()
	if _, err := manifestTemp.Write(bytes); err != nil {
		_ = manifestTemp.Close()
		_ = os.Remove(manifestTempPath)
		return BuildResult{}, err
	}
	if err := manifestTemp.Sync(); err != nil {
		_ = manifestTemp.Close()
		_ = os.Remove(manifestTempPath)
		return BuildResult{}, err
	}
	if err := manifestTemp.Close(); err != nil {
		_ = os.Remove(manifestTempPath)
		return BuildResult{}, err
	}
	manifestPath := filepath.Join(partitionDir, "manifest-"+manifest.BuildID+".json")
	finalManifest, err := fileflow.Move(manifestTempPath, manifestPath)
	if err != nil {
		return BuildResult{}, fmt.Errorf("dataset: publish manifest last: %w", err)
	}
	if finalManifest != manifestPath {
		return BuildResult{}, fmt.Errorf("dataset: manifest identity collision")
	}
	if err := syncPublicationDirectories(root, partitionDir, options.DirectorySync); err != nil {
		return BuildResult{}, fmt.Errorf("dataset: sync published manifest directories: %w", err)
	}
	return BuildResult{Manifest: manifest, ManifestPath: manifestPath, ParquetPath: parquetPath, ManifestHash: manifestHash}, nil
}

func manifestBuildID(manifest Manifest) string {
	payload := strings.Join([]string{DatasetVersion, manifest.SchemaSHA256, string(manifest.Family), manifest.SourceID,
		manifestEpochIdentity(manifest.Epochs), manifest.UTCDate, manifest.UTCHour, manifest.LogicalSHA256, manifest.PhysicalSHA256,
		manifest.DatasetPolicyID, manifest.ReplayConfigID, manifest.InputManifestSetID,
		fmt.Sprint(manifest.Options.RowGroupTargetBytes), fmt.Sprint(manifest.Options.PageBufferBytes), manifest.Options.Compression,
		fmt.Sprint(manifest.Options.Dictionary), fmt.Sprint(manifest.Options.BloomFilter)}, "\x00")
	digest := sha256.Sum256([]byte(payload))
	return hex.EncodeToString(digest[:])
}

func manifestEpochIdentity(epochs []ManifestEpoch) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("dataset-manifest-epochs-v1\x00"))
	for _, epoch := range epochs {
		_, _ = hasher.Write([]byte(epoch.Kind))
		_, _ = hasher.Write([]byte{0})
		_, _ = hasher.Write([]byte(epoch.ID))
		_, _ = hasher.Write([]byte{0})
		_, _ = fmt.Fprintf(hasher, "%d\x00%d\x00%d\x00", epoch.FirstReceivedTimeNS, epoch.FirstArrivalOrdinal, epoch.LastArrivalOrdinal)
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

func syncDirectory(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func syncPublicationDirectories(root, leaf string, syncFn func(string) error) error {
	if syncFn == nil {
		syncFn = syncDirectory
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return err
	}
	current, err := filepath.Abs(leaf)
	if err != nil {
		return err
	}
	for {
		relative, err := filepath.Rel(absoluteRoot, current)
		if err != nil || (!filepath.IsLocal(relative) && relative != ".") {
			return fmt.Errorf("%w: publication directory escapes root", ErrInvalidInput)
		}
		if err := syncFn(current); err != nil {
			return err
		}
		if current == absoluteRoot {
			return nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			return fmt.Errorf("%w: publication directory root not reached", ErrInvalidInput)
		}
		current = parent
	}
}

func hashFile(path string) ([32]byte, int64, error) {
	return hashFileContext(context.Background(), path)
}

func hashFileContext(ctx context.Context, path string) ([32]byte, int64, error) {
	var result [32]byte
	file, err := os.Open(path)
	if err != nil {
		return result, 0, fmt.Errorf("dataset: open for SHA-256: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return result, 0, err
	}
	if info.Size() < 0 || info.Size() > MaxPartitionFileBytes {
		return result, 0, fmt.Errorf("%w: parquet file byte bound", ErrInvalidInput)
	}
	hasher := sha256.New()
	buffer := make([]byte, 256<<10)
	var written int64
	for {
		if err := ctx.Err(); err != nil {
			return result, written, err
		}
		n, readErr := file.Read(buffer)
		if n > 0 {
			if _, err := hasher.Write(buffer[:n]); err != nil {
				return result, written, err
			}
			written += int64(n)
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return result, written, fmt.Errorf("dataset: hash parquet: %w", readErr)
		}
		if n == 0 {
			return result, written, fmt.Errorf("dataset: zero-progress parquet hash")
		}
	}
	copy(result[:], hasher.Sum(nil))
	return result, written, nil
}
