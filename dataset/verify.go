package dataset

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
	"github.com/parquet-go/parquet-go"
	"github.com/spf13/pathologize"
)

type parquetInspection struct {
	rowGroups uint64
	rows      uint64
	pages     uint64
	values    uint64
}

func VerifyManifest(ctx context.Context, root, manifestPath string) (Verification, error) {
	resolvedManifest, err := containedPath(root, manifestPath)
	if err != nil {
		return Verification{}, err
	}
	info, err := os.Stat(resolvedManifest)
	if err != nil {
		return Verification{}, fmt.Errorf("dataset: stat manifest: %w", err)
	}
	if info.Size() <= 0 || info.Size() > MaxManifestBytes {
		return Verification{}, fmt.Errorf("%w: manifest size", ErrManifestMismatch)
	}
	file, err := os.Open(resolvedManifest)
	if err != nil {
		return Verification{}, err
	}
	decoder := json.NewDecoder(io.LimitReader(file, MaxManifestBytes+1))
	decoder.DisallowUnknownFields()
	var manifest Manifest
	if err := decoder.Decode(&manifest); err != nil {
		_ = file.Close()
		return Verification{}, fmt.Errorf("%w: decode manifest: %v", ErrManifestMismatch, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		_ = file.Close()
		return Verification{}, fmt.Errorf("%w: trailing manifest content", ErrManifestMismatch)
	}
	if err := file.Close(); err != nil {
		return Verification{}, err
	}
	if err := validateManifest(manifest); err != nil {
		return Verification{}, err
	}
	parquetPath, err := containedPath(root, manifest.ParquetFile)
	if err != nil {
		return Verification{}, err
	}
	return verifyParquetFile(ctx, parquetPath, manifest)
}

func validateManifest(manifest Manifest) error {
	if manifest.ManifestVersion != ManifestVersion || manifest.DatasetVersion != DatasetVersion ||
		manifest.ParquetWriterVersion != ParquetWriterVersion || manifest.ParquetFormatCompatibility != ParquetFormatCompatibility ||
		manifest.SchemaName == "" || manifest.SchemaVersion == 0 || manifest.SourceID == "" || len(manifest.Epochs) == 0 ||
		manifest.UTCDate == "" || manifest.UTCHour == "" || manifest.InputRows == 0 || manifest.ParquetRows == 0 ||
		manifest.InputRows > MaxPartitionInputRows || manifest.ParquetRows > MaxPartitionParquetRows || manifest.RowGroups == 0 ||
		manifest.FileBytes <= 0 || manifest.FileBytes > MaxPartitionFileBytes || manifest.ParquetFile == "" || manifest.FirstReceivedTimeNS > manifest.LastReceivedTimeNS ||
		manifest.FirstArrivalOrdinal == 0 || manifest.FirstArrivalOrdinal > manifest.LastArrivalOrdinal {
		return fmt.Errorf("%w: invalid manifest fields", ErrManifestMismatch)
	}
	if _, err := decodeHash(manifest.SchemaSHA256); err != nil {
		return err
	}
	if _, err := decodeHash(manifest.LogicalSHA256); err != nil {
		return err
	}
	if _, err := decodeHash(manifest.PhysicalSHA256); err != nil {
		return err
	}
	if _, err := decodeHash(manifest.DatasetPolicyID); err != nil {
		return err
	}
	if _, err := decodeHash(manifest.ReplayConfigID); err != nil {
		return err
	}
	if _, err := decodeHash(manifest.InputManifestSetID); err != nil {
		return err
	}
	if len(manifest.Epochs) > int(MaxPartitionInputRows) {
		return fmt.Errorf("%w: epoch count bound", ErrManifestMismatch)
	}
	for index, epoch := range manifest.Epochs {
		decoded, err := hex.DecodeString(epoch.ID)
		if err != nil || len(decoded) != 16 ||
			(epoch.Kind != string(normalize.ConnectionEpoch) && epoch.Kind != string(normalize.PollCycleEpoch)) ||
			epoch.FirstReceivedTimeNS < manifest.FirstReceivedTimeNS || epoch.FirstReceivedTimeNS > manifest.LastReceivedTimeNS ||
			epoch.FirstArrivalOrdinal == 0 || epoch.FirstArrivalOrdinal > epoch.LastArrivalOrdinal ||
			epoch.FirstArrivalOrdinal < manifest.FirstArrivalOrdinal || epoch.LastArrivalOrdinal > manifest.LastArrivalOrdinal {
			return fmt.Errorf("%w: epoch identity or bounds", ErrManifestMismatch)
		}
		if index > 0 && compareManifestEpoch(manifest.Epochs[index-1], epoch) >= 0 {
			return fmt.Errorf("%w: epochs are not uniquely ordered", ErrManifestMismatch)
		}
	}
	name, version, _, err := familySchema(manifest.Family)
	if err != nil || name != manifest.SchemaName || version != manifest.SchemaVersion {
		return fmt.Errorf("%w: schema identity", ErrManifestMismatch)
	}
	options := WriterOptions{RowGroupTargetBytes: manifest.Options.RowGroupTargetBytes, PageBufferBytes: manifest.Options.PageBufferBytes,
		Compression: manifest.Options.Compression, Dictionary: manifest.Options.Dictionary, BloomFilter: manifest.Options.BloomFilter,
		MaxInputRows: MaxPartitionInputRows, MaxParquetRows: MaxPartitionParquetRows,
		DatasetPolicyID: mustManifestHash(manifest.DatasetPolicyID), ReplayConfigID: mustManifestHash(manifest.ReplayConfigID),
		InputManifestSetID: mustManifestHash(manifest.InputManifestSetID)}
	if err := options.validate(); err != nil {
		return fmt.Errorf("%w: writer options", ErrManifestMismatch)
	}
	if manifest.BuildID != manifestBuildID(manifest) {
		return fmt.Errorf("%w: build ID", ErrManifestMismatch)
	}
	epochIdentity := manifestEpochIdentity(manifest.Epochs)
	expectedParquet := pathologize.Join("dataset-v1", "family="+string(manifest.Family), "source="+manifest.SourceID,
		"date="+manifest.UTCDate, "hour="+manifest.UTCHour,
		"part-epochs-"+epochIdentity[:16]+"-"+manifest.PhysicalSHA256+".parquet")
	if manifest.ParquetFile != filepath.ToSlash(expectedParquet) {
		return fmt.Errorf("%w: parquet partition path", ErrManifestMismatch)
	}
	return nil
}

func compareManifestEpoch(left, right ManifestEpoch) int {
	if order := cmp.Compare(left.FirstReceivedTimeNS, right.FirstReceivedTimeNS); order != 0 {
		return order
	}
	if order := cmp.Compare(left.Kind, right.Kind); order != 0 {
		return order
	}
	return cmp.Compare(left.ID, right.ID)
}

func containedPath(root, path string) (string, error) {
	if root == "" || path == "" {
		return "", fmt.Errorf("%w: empty path", ErrManifestMismatch)
	}
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	candidate := path
	if filepath.IsAbs(path) {
		candidate, err = filepath.Rel(absoluteRoot, path)
		if err != nil {
			return "", err
		}
	}
	candidate = filepath.ToSlash(candidate)
	if !filepath.IsLocal(candidate) || strings.Contains(candidate, "\\") {
		return "", fmt.Errorf("%w: non-local path", ErrManifestMismatch)
	}
	parts := strings.Split(candidate, "/")
	for _, part := range parts {
		if part == "" || !pathologize.IsClean(part) {
			return "", fmt.Errorf("%w: unsafe path segment", ErrManifestMismatch)
		}
	}
	joined := pathologize.Join(absoluteRoot, parts...)
	relative, err := filepath.Rel(absoluteRoot, joined)
	if err != nil || !filepath.IsLocal(relative) {
		return "", fmt.Errorf("%w: path escapes root", ErrManifestMismatch)
	}
	return joined, nil
}

func verifyParquetFile(ctx context.Context, path string, manifest Manifest) (Verification, error) {
	physical, size, err := hashFileContext(ctx, path)
	if err != nil {
		return Verification{}, err
	}
	expectedPhysical, err := decodeHash(manifest.PhysicalSHA256)
	if err != nil {
		return Verification{}, err
	}
	if physical != expectedPhysical || size != manifest.FileBytes {
		return Verification{}, fmt.Errorf("%w: physical SHA-256 or byte count", ErrCorruptDataset)
	}
	inspection, err := inspectParquet(ctx, path)
	if err != nil {
		return Verification{}, err
	}
	if inspection.rowGroups != manifest.RowGroups || inspection.rows != manifest.ParquetRows || inspection.pages != manifest.Pages || inspection.values != manifest.Values {
		return Verification{}, fmt.Errorf("%w: row-group/page/value counts", ErrManifestMismatch)
	}
	name, version, schema, err := familySchema(manifest.Family)
	if err != nil {
		return Verification{}, err
	}
	schemaHash := schemaDigest(name, version, schema)
	expectedSchema, err := decodeHash(manifest.SchemaSHA256)
	if err != nil {
		return Verification{}, err
	}
	if schemaHash != expectedSchema {
		return Verification{}, fmt.Errorf("%w: compiled schema hash", ErrManifestMismatch)
	}
	file, err := os.Open(path)
	if err != nil {
		return Verification{}, err
	}
	defer file.Close()
	opened, err := parquet.OpenFile(file, size)
	if err != nil {
		return Verification{}, fmt.Errorf("%w: open parquet footer: %v", ErrCorruptDataset, err)
	}
	actualSchema := schemaDigest(name, version, opened.Schema())
	if actualSchema != expectedSchema || !parquet.EqualNodes(schema, opened.Schema()) {
		return Verification{}, fmt.Errorf("%w: physical schema", ErrManifestMismatch)
	}
	for key, expected := range map[string]string{
		"enable.dataset_version": DatasetVersion, "enable.parquet_writer_version": ParquetWriterVersion,
		"enable.parquet_format_compatibility": ParquetFormatCompatibility, "enable.schema_name": manifest.SchemaName,
		"enable.schema_version": fmt.Sprint(manifest.SchemaVersion), "enable.schema_sha256": manifest.SchemaSHA256,
		"enable.logical_sha256": manifest.LogicalSHA256, "enable.input_rows": fmt.Sprint(manifest.InputRows),
		"enable.parquet_rows": fmt.Sprint(manifest.ParquetRows), "enable.dataset_policy_id": manifest.DatasetPolicyID,
		"enable.replay_config_id": manifest.ReplayConfigID, "enable.input_manifest_set_id": manifest.InputManifestSetID,
	} {
		actual, ok := opened.Lookup(key)
		if !ok || actual != expected {
			return Verification{}, fmt.Errorf("%w: parquet metadata %s", ErrManifestMismatch, key)
		}
	}
	logical, inputRows, bounds, err := verifyTypedRows(ctx, file, manifest)
	if err != nil {
		return Verification{}, err
	}
	expectedLogical, err := decodeHash(manifest.LogicalSHA256)
	if err != nil {
		return Verification{}, err
	}
	if logical != expectedLogical || inputRows != manifest.InputRows || bounds != manifestBounds(manifest) {
		return Verification{}, fmt.Errorf("%w: logical hash, row count, or bounds", ErrManifestMismatch)
	}
	return Verification{Manifest: manifest, PhysicalSHA256: physical, LogicalSHA256: logical, SchemaSHA256: schemaHash,
		RowGroups: inspection.rowGroups, Pages: inspection.pages, Values: inspection.values}, nil
}

func inspectParquet(ctx context.Context, path string) (parquetInspection, error) {
	var result parquetInspection
	file, err := os.Open(path)
	if err != nil {
		return result, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return result, err
	}
	opened, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return result, fmt.Errorf("%w: footer: %v", ErrCorruptDataset, err)
	}
	values := make([]parquet.Value, 1024)
	for _, rowGroup := range opened.RowGroups() {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		result.rowGroups++
		if rowGroup.NumRows() < 0 || uint64(rowGroup.NumRows()) > MaxPartitionParquetRows {
			return result, fmt.Errorf("%w: row-group row bound", ErrCorruptDataset)
		}
		result.rows += uint64(rowGroup.NumRows())
		for _, column := range rowGroup.ColumnChunks() {
			pages := column.Pages()
			for {
				page, pageErr := pages.ReadPage()
				if errors.Is(pageErr, io.EOF) {
					break
				}
				if pageErr != nil {
					_ = pages.Close()
					return result, fmt.Errorf("%w: read page: %v", ErrCorruptDataset, pageErr)
				}
				if page.NumValues() < 0 || page.NumValues() > MaxPageValues || page.Size() < 0 || page.Size() > 64<<20 {
					_ = pages.Close()
					return result, fmt.Errorf("%w: page bound", ErrCorruptDataset)
				}
				result.pages++
				result.values += uint64(page.NumValues())
				reader := page.Values()
				var readValues int64
				for {
					n, valueErr := reader.ReadValues(values)
					readValues += int64(n)
					if errors.Is(valueErr, io.EOF) {
						break
					}
					if valueErr != nil {
						_ = pages.Close()
						return result, fmt.Errorf("%w: decode page value: %v", ErrCorruptDataset, valueErr)
					}
					if n == 0 {
						_ = pages.Close()
						return result, fmt.Errorf("%w: zero-progress page reader", ErrCorruptDataset)
					}
				}
				if readValues != page.NumValues() {
					_ = pages.Close()
					return result, fmt.Errorf("%w: page value count", ErrCorruptDataset)
				}
			}
			if err := pages.Close(); err != nil {
				return result, fmt.Errorf("%w: close pages: %v", ErrCorruptDataset, err)
			}
		}
	}
	return result, nil
}

type rowBounds struct {
	firstTime, lastTime       int64
	firstArrival, lastArrival uint64
	epochIdentity             [32]byte
}

func manifestBounds(value Manifest) rowBounds {
	return rowBounds{
		firstTime: value.FirstReceivedTimeNS, lastTime: value.LastReceivedTimeNS,
		firstArrival: value.FirstArrivalOrdinal, lastArrival: value.LastArrivalOrdinal,
		epochIdentity: epochIdentityDigest(value.Epochs),
	}
}

type epochObservationTracker map[epochKey]epochOrder

func (t epochObservationTracker) include(kind string, id [16]byte, receivedTimeNS int64, arrivalOrdinal uint64, messageOrdinal uint32) {
	key := epochKey{kind: kind, id: id}
	observation, ok := t[key]
	if !ok {
		t[key] = epochOrder{
			key: key, firstReceivedTimeNS: receivedTimeNS,
			firstArrivalOrdinal: arrivalOrdinal, firstMessageOrdinal: messageOrdinal,
			lastArrivalOrdinal: arrivalOrdinal,
		}
		return
	}
	if arrivalOrdinal < observation.firstArrivalOrdinal ||
		(arrivalOrdinal == observation.firstArrivalOrdinal && messageOrdinal < observation.firstMessageOrdinal) {
		observation.firstReceivedTimeNS = receivedTimeNS
		observation.firstArrivalOrdinal = arrivalOrdinal
		observation.firstMessageOrdinal = messageOrdinal
	}
	observation.lastArrivalOrdinal = max(observation.lastArrivalOrdinal, arrivalOrdinal)
	t[key] = observation
}

func (t epochObservationTracker) identity() [32]byte {
	epochs := make([]ManifestEpoch, 0, len(t))
	for _, observation := range t {
		epochs = append(epochs, ManifestEpoch{
			Kind: observation.key.kind, ID: hex.EncodeToString(observation.key.id[:]),
			FirstReceivedTimeNS: observation.firstReceivedTimeNS,
			FirstArrivalOrdinal: observation.firstArrivalOrdinal, LastArrivalOrdinal: observation.lastArrivalOrdinal,
		})
	}
	slices.SortFunc(epochs, compareManifestEpoch)
	return epochIdentityDigest(epochs)
}

func epochIdentityDigest(epochs []ManifestEpoch) [32]byte {
	decoded, _ := hex.DecodeString(manifestEpochIdentity(epochs))
	var identity [32]byte
	copy(identity[:], decoded)
	return identity
}

func verifyTypedRows(ctx context.Context, file *os.File, manifest Manifest) ([32]byte, uint64, rowBounds, error) {
	switch manifest.Family {
	case FamilyTrade:
		return verifySimpleRows(ctx, parquet.NewGenericReader[tradeParquetRow](file), manifest, tradeFromParquet)
	case FamilyQuote:
		return verifySimpleRows(ctx, parquet.NewGenericReader[quoteParquetRow](file), manifest, quoteFromParquet)
	case FamilyTicker:
		return verifySimpleRows(ctx, parquet.NewGenericReader[tickerParquetRow](file), manifest, tickerFromParquet)
	case FamilyBookUpdate:
		return verifyBookRows(ctx, parquet.NewGenericReader[bookParquetRow](file), manifest)
	case FamilySchemaQuarantine:
		return verifyAuxRows(ctx, parquet.NewGenericReader[quarantineParquetRow](file), manifest, quarantineFromParquet, "dataset-schema-quarantine-logical-v1\x00")
	case FamilyQuality:
		return verifyAuxRows(ctx, parquet.NewGenericReader[qualityParquetRow](file), manifest, qualityFromParquet, "dataset-quality-logical-v1\x00")
	default:
		return [32]byte{}, 0, rowBounds{}, fmt.Errorf("%w: family", ErrManifestMismatch)
	}
}

type genericReader[T any] interface {
	Read([]T) (int, error)
	Close() error
}

func verifySimpleRows[T any](ctx context.Context, reader genericReader[T], manifest Manifest,
	convert func(T, Manifest) (normalize.Row, commonColumns, error)) ([32]byte, uint64, rowBounds, error) {
	defer reader.Close()
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("dataset-normalized-partition-logical-v1\x00"))
	batch := make([]T, 128)
	var count uint64
	var bounds rowBounds
	epochObservations := make(epochObservationTracker)
	var previous commonColumns
	havePrevious := false
	for {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, 0, bounds, err
		}
		n, err := reader.Read(batch)
		for i := range n {
			row, common, convertErr := convert(batch[i], manifest)
			if convertErr != nil {
				return [32]byte{}, 0, bounds, convertErr
			}
			if common.EventOrdinal != count || common.LogicalHash != row.LogicalHash {
				return [32]byte{}, 0, bounds, fmt.Errorf("%w: event order/hash", ErrCorruptDataset)
			}
			if partitionErr := checkManifestPartition(common, manifest); partitionErr != nil {
				return [32]byte{}, 0, bounds, partitionErr
			}
			if havePrevious && compareCommonOrder(previous, common, manifest) >= 0 {
				return [32]byte{}, 0, bounds, fmt.Errorf("%w: instrument/epoch/source order", ErrCorruptDataset)
			}
			previous, havePrevious = common, true
			_, _ = hasher.Write(row.LogicalHash[:])
			bounds.include(common.ReceivedTimeNS, common.ArrivalOrdinal, count)
			epochObservations.include(common.EpochKind, common.EpochID, common.ReceivedTimeNS, common.ArrivalOrdinal, common.MessageOrdinal)
			count++
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return [32]byte{}, 0, bounds, fmt.Errorf("%w: read typed row: %v", ErrCorruptDataset, err)
		}
	}
	bounds.epochIdentity = epochObservations.identity()
	return sumHash(hasher), count, bounds, nil
}

func tradeFromParquet(value tradeParquetRow, manifest Manifest) (normalize.Row, commonColumns, error) {
	metadata, err := metadataFromCommon(value.commonColumns, manifest)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	price, err := decimalValue(value.Price, normalize.CanonicalPriceScale)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	amount, err := decimalValue(value.Amount, normalize.CanonicalAmountScale)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	event := normalize.TradeV1{Metadata: metadata, NativeTradeID: value.NativeTradeID, AggressorSide: normalize.Side(value.AggressorSide),
		BuyerIsMaker: value.BuyerIsMaker, NativeIgnoreFlag: value.NativeIgnoreFlag,
		Price:           normalize.Numeric{Decimal: price, Unit: normalize.SpotPriceUnit(value.BaseAssetID, value.QuoteAssetID)},
		Amount:          normalize.Numeric{Decimal: amount, Unit: normalize.BaseAssetUnit(value.BaseAssetID)},
		AggregationKind: normalize.AggregationKind(value.AggregationKind), NativeDuplicateStatus: normalize.DuplicateStatus(value.NativeDuplicateState)}
	row, err := normalize.NewTradeRow(event)
	return row, value.commonColumns, corruption(err)
}

func quoteFromParquet(value quoteParquetRow, manifest Manifest) (normalize.Row, commonColumns, error) {
	metadata, err := metadataFromCommon(value.commonColumns, manifest)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	bidPrice, err := decimalValue(value.BidPrice, normalize.CanonicalPriceScale)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	bidAmount, err := decimalValue(value.BidAmount, normalize.CanonicalAmountScale)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	askPrice, err := decimalValue(value.AskPrice, normalize.CanonicalPriceScale)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	askAmount, err := decimalValue(value.AskAmount, normalize.CanonicalAmountScale)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	sourceTime, err := parquetOptionalInt64(value.SourceTimeNS, value.SourceTimeState)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	event := normalize.QuoteV1{Metadata: metadata, NativeSourceRole: value.NativeSourceRole, UpdateID: value.UpdateID,
		BidPrice:          normalize.Numeric{Decimal: bidPrice, Unit: normalize.SpotPriceUnit(value.BaseAssetID, value.QuoteAssetID)},
		BidAmount:         normalize.Numeric{Decimal: bidAmount, Unit: normalize.BaseAssetUnit(value.BaseAssetID)},
		AskPrice:          normalize.Numeric{Decimal: askPrice, Unit: normalize.SpotPriceUnit(value.BaseAssetID, value.QuoteAssetID)},
		AskAmount:         normalize.Numeric{Decimal: askAmount, Unit: normalize.BaseAssetUnit(value.BaseAssetID)},
		RPIInclusionState: normalize.RPIInclusionState(value.RPIInclusionState), SourceTimeNS: sourceTime}
	row, err := normalize.NewQuoteRow(event)
	return row, value.commonColumns, corruption(err)
}

func tickerFromParquet(value tickerParquetRow, manifest Manifest) (normalize.Row, commonColumns, error) {
	metadata, err := metadataFromCommon(value.commonColumns, manifest)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	priceUnit := normalize.SpotPriceUnit(value.BaseAssetID, value.QuoteAssetID)
	baseUnit := normalize.BaseAssetUnit(value.BaseAssetID)
	quoteUnit := normalize.QuoteAssetUnit(value.QuoteAssetID)
	price := func(raw [16]byte) (normalize.Numeric, error) {
		value, err := decimalValue(raw, normalize.CanonicalPriceScale)
		return normalize.Numeric{Decimal: value, Unit: priceUnit}, err
	}
	amount := func(raw [16]byte) (normalize.Numeric, error) {
		value, err := decimalValue(raw, normalize.CanonicalAmountScale)
		return normalize.Numeric{Decimal: value, Unit: baseUnit}, err
	}
	event := normalize.TickerV1{Metadata: metadata, NativeSourceRole: value.NativeSourceRole, WindowKind: normalize.WindowKind(value.WindowKind),
		WindowOpenSemantics: value.WindowOpenSemantics, WindowCloseSemantics: value.WindowCloseSemantics,
		WindowOpenTimeNS: value.WindowOpenTimeNS, WindowCloseTimeNS: value.WindowCloseTimeNS,
		WindowTimeResolution: normalize.TimeResolution(value.WindowTimeResolution), NominalWindowDurationNS: value.NominalWindowDurationNS,
		FirstTradeID: value.FirstTradeID, LastTradeID: value.LastTradeID, TradeCount: value.TradeCount}
	if event.PriceChange, err = price(value.PriceChange); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	percent, err := decimalValue(value.PriceChangePercent, normalize.CanonicalPercentScale)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	event.PriceChangePercent = normalize.Numeric{Decimal: percent, Unit: normalize.PercentUnit()}
	if event.WeightedAveragePrice, err = price(value.WeightedAveragePrice); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.FirstTradeBeforeWindowPrice, err = price(value.FirstTradeBeforeWindowPrice); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.LastPrice, err = price(value.LastPrice); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.LastAmount, err = amount(value.LastAmount); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.NativeBestBidPrice, err = price(value.NativeBestBidPrice); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.NativeBestBidAmount, err = amount(value.NativeBestBidAmount); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.NativeBestAskPrice, err = price(value.NativeBestAskPrice); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.NativeBestAskAmount, err = amount(value.NativeBestAskAmount); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.OpenPrice, err = price(value.OpenPrice); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.HighPrice, err = price(value.HighPrice); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.LowPrice, err = price(value.LowPrice); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	if event.BaseVolume, err = amount(value.BaseVolume); err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	quoteDecimal, err := decimalValue(value.QuoteVolume, normalize.CanonicalAmountScale)
	if err != nil {
		return normalize.Row{}, value.commonColumns, err
	}
	event.QuoteVolume = normalize.Numeric{Decimal: quoteDecimal, Unit: quoteUnit}
	row, err := normalize.NewTickerRow(event)
	return row, value.commonColumns, corruption(err)
}

func verifyBookRows(ctx context.Context, reader genericReader[bookParquetRow], manifest Manifest) ([32]byte, uint64, rowBounds, error) {
	defer reader.Close()
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("dataset-normalized-partition-logical-v1\x00"))
	batch := make([]bookParquetRow, 128)
	var pending []bookParquetRow
	var count uint64
	var bounds rowBounds
	epochObservations := make(epochObservationTracker)
	var previous commonColumns
	havePrevious := false
	finish := func() error {
		if len(pending) == 0 {
			return nil
		}
		row, common, err := bookEventFromParquet(pending, manifest)
		if err != nil {
			return err
		}
		if common.EventOrdinal != count || common.LogicalHash != row.LogicalHash {
			return fmt.Errorf("%w: book event order/hash", ErrCorruptDataset)
		}
		if err := checkManifestPartition(common, manifest); err != nil {
			return err
		}
		if havePrevious && compareCommonOrder(previous, common, manifest) >= 0 {
			return fmt.Errorf("%w: book instrument/epoch/source order", ErrCorruptDataset)
		}
		previous, havePrevious = common, true
		_, _ = hasher.Write(row.LogicalHash[:])
		bounds.include(common.ReceivedTimeNS, common.ArrivalOrdinal, count)
		epochObservations.include(common.EpochKind, common.EpochID, common.ReceivedTimeNS, common.ArrivalOrdinal, common.MessageOrdinal)
		count++
		pending = pending[:0]
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, 0, bounds, err
		}
		n, err := reader.Read(batch)
		for i := range n {
			value := batch[i]
			if len(pending) > 0 && value.EventOrdinal != pending[0].EventOrdinal {
				if finishErr := finish(); finishErr != nil {
					return [32]byte{}, 0, bounds, finishErr
				}
			}
			pending = append(pending, value)
			if uint64(len(pending)) > MaxPartitionParquetRows {
				return [32]byte{}, 0, bounds, fmt.Errorf("%w: book event level bound", ErrCorruptDataset)
			}
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return [32]byte{}, 0, bounds, fmt.Errorf("%w: read book rows: %v", ErrCorruptDataset, err)
		}
	}
	if err := finish(); err != nil {
		return [32]byte{}, 0, bounds, err
	}
	bounds.epochIdentity = epochObservations.identity()
	return sumHash(hasher), count, bounds, nil
}

func bookEventFromParquet(rows []bookParquetRow, manifest Manifest) (normalize.Row, commonColumns, error) {
	first := rows[0]
	metadata, err := metadataFromCommon(first.commonColumns, manifest)
	if err != nil {
		return normalize.Row{}, first.commonColumns, err
	}
	expected := int(first.BidCount + first.AskCount)
	if expected == 0 {
		if len(rows) != 1 || first.HasLevel {
			return normalize.Row{}, first.commonColumns, fmt.Errorf("%w: empty book sentinel", ErrCorruptDataset)
		}
	} else if len(rows) != expected {
		return normalize.Row{}, first.commonColumns, fmt.Errorf("%w: flattened book row count", ErrCorruptDataset)
	}
	previousSequence, err := parquetOptionalUint64(first.PreviousSequence, first.PreviousSequenceState)
	if err != nil {
		return normalize.Row{}, first.commonColumns, err
	}
	event := normalize.BookUpdateV1{Metadata: metadata, UpdateKind: normalize.UpdateKind(first.UpdateKind), DepthContract: first.DepthContract,
		AggregationContract: first.AggregationContract, FirstSequence: first.FirstSequence, LastSequence: first.LastSequence,
		PreviousSequence: previousSequence, Checksum: normalize.SourceState(first.ChecksumState),
		AmountSemantics: first.AmountSemantics, ReconstructionEligibility: first.ReconstructionEligibility,
		Bids: make([]normalize.BookLevel, 0, first.BidCount), Asks: make([]normalize.BookLevel, 0, first.AskCount)}
	for i, value := range rows {
		if !reflect.DeepEqual(value.commonColumns, first.commonColumns) || value.UpdateKind != first.UpdateKind || value.DepthContract != first.DepthContract ||
			value.AggregationContract != first.AggregationContract || value.FirstSequence != first.FirstSequence || value.LastSequence != first.LastSequence ||
			!reflect.DeepEqual(value.PreviousSequence, first.PreviousSequence) || value.PreviousSequenceState != first.PreviousSequenceState ||
			value.ChecksumState != first.ChecksumState || value.AmountSemantics != first.AmountSemantics ||
			value.ReconstructionEligibility != first.ReconstructionEligibility || value.BidCount != first.BidCount || value.AskCount != first.AskCount {
			return normalize.Row{}, first.commonColumns, fmt.Errorf("%w: inconsistent repeated book event columns", ErrCorruptDataset)
		}
		if expected == 0 {
			continue
		}
		if !value.HasLevel || value.LevelOrdinal != uint32(i) {
			return normalize.Row{}, first.commonColumns, fmt.Errorf("%w: flattened level ordinal", ErrCorruptDataset)
		}
		price, err := decimalValue(value.Price, normalize.CanonicalPriceScale)
		if err != nil {
			return normalize.Row{}, first.commonColumns, err
		}
		amount, err := decimalValue(value.Amount, normalize.CanonicalAmountScale)
		if err != nil {
			return normalize.Row{}, first.commonColumns, err
		}
		level := normalize.BookLevel{Side: normalize.Side(value.Side), LevelOrdinal: value.SideOrdinal, Action: normalize.LevelAction(value.Action),
			Price:  normalize.Numeric{Decimal: price, Unit: normalize.SpotPriceUnit(value.PriceBaseAssetID, value.PriceQuoteAssetID)},
			Amount: normalize.Numeric{Decimal: amount, Unit: normalize.BaseAssetUnit(value.AmountAssetID)}}
		if i < int(first.BidCount) {
			event.Bids = append(event.Bids, level)
		} else {
			event.Asks = append(event.Asks, level)
		}
	}
	row, err := normalize.NewBookUpdateRow(event)
	return row, first.commonColumns, corruption(err)
}

func verifyAuxRows[T, I any](ctx context.Context, reader genericReader[T], manifest Manifest,
	convert func(T, Manifest) (I, uint64, [32]byte, auxiliaryCoordinate, error), domain string) ([32]byte, uint64, rowBounds, error) {
	defer reader.Close()
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("dataset-auxiliary-partition-logical-v1\x00" + string(manifest.Family) + "\x00"))
	batch := make([]T, 128)
	var count uint64
	var bounds rowBounds
	epochObservations := make(epochObservationTracker)
	_ = domain
	var previous auxiliaryCoordinate
	havePrevious := false
	for {
		if err := ctx.Err(); err != nil {
			return [32]byte{}, 0, bounds, err
		}
		n, err := reader.Read(batch)
		for i := range n {
			_, ordinal, digest, meta, convertErr := convert(batch[i], manifest)
			if convertErr != nil {
				return [32]byte{}, 0, bounds, convertErr
			}
			if ordinal != count {
				return [32]byte{}, 0, bounds, fmt.Errorf("%w: auxiliary ordinal", ErrCorruptDataset)
			}
			if err := checkAuxPartition(meta, manifest); err != nil {
				return [32]byte{}, 0, bounds, err
			}
			if havePrevious && compareAuxOrder(previous, meta, manifest) >= 0 {
				return [32]byte{}, 0, bounds, fmt.Errorf("%w: auxiliary instrument/epoch/source order", ErrCorruptDataset)
			}
			previous, havePrevious = meta, true
			_, _ = hasher.Write(digest[:])
			bounds.include(meta.receivedTimeNS, meta.arrivalOrdinal, count)
			epochObservations.include(meta.epochKind, meta.epochID, meta.receivedTimeNS, meta.arrivalOrdinal, meta.messageOrdinal)
			count++
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return [32]byte{}, 0, bounds, err
		}
	}
	bounds.epochIdentity = epochObservations.identity()
	return sumHash(hasher), count, bounds, nil
}

func quarantineFromParquet(value quarantineParquetRow, manifest Manifest) (normalize.SchemaQuarantineV1, uint64, [32]byte, auxiliaryCoordinate, error) {
	if value.DatasetPolicyID != mustManifestHash(manifest.DatasetPolicyID) || value.ReplayConfigID != mustManifestHash(manifest.ReplayConfigID) || value.InputManifestSetID != mustManifestHash(manifest.InputManifestSetID) {
		return normalize.SchemaQuarantineV1{}, 0, [32]byte{}, auxiliaryCoordinate{}, fmt.Errorf("%w: quarantine policy IDs", ErrManifestMismatch)
	}
	coordinate := normalize.RawCoordinate{SourceID: value.SourceID, ChannelID: value.ChannelID, EpochKind: normalize.EpochKind(value.EpochKind), EpochID: value.EpochID,
		ArrivalOrdinal: value.ArrivalOrdinal, MessageOrdinal: value.MessageOrdinal, RawSegmentSHA256: value.RawSegmentSHA256,
		RawRecordOrdinal: value.RawRecordOrdinal, RawPayloadSHA256: value.RawPayloadSHA256}
	row := normalize.SchemaQuarantineV1{Version: uint16(value.Version), QuarantineID: value.QuarantineID, Code: normalize.QuarantineCode(value.Code),
		Field: value.Field, SourceState: normalize.SourceState(value.SourceState), FingerprintClass: normalize.FingerprintClass(value.FingerprintClass),
		SourceSchemaFingerprint: value.SourceSchemaFingerprint, SourceID: value.SourceID, ChannelID: value.ChannelID,
		ReceivedTimeNS: value.ReceivedTimeNS, Coordinate: coordinate, MapperVersion: value.MapperVersion,
		MapperBindingID: value.MapperBindingID, SourceTimeResolution: normalize.TimeResolution(value.SourceTimeResolution), CatalogSnapshotID: value.CatalogSnapshotID}
	if err := validateQuarantine(row); err != nil {
		return row, 0, [32]byte{}, auxiliaryCoordinate{}, corruption(err)
	}
	digest := quarantineHash(row)
	if digest != value.RowLogicalHash {
		return row, 0, digest, auxiliaryCoordinate{}, fmt.Errorf("%w: quarantine logical hash", ErrCorruptDataset)
	}
	meta := auxiliaryCoordinate{sourceID: value.SourceID, channelID: value.ChannelID, epochKind: value.EpochKind, epochID: value.EpochID,
		arrivalOrdinal: value.ArrivalOrdinal, messageOrdinal: value.MessageOrdinal, receivedTimeNS: value.ReceivedTimeNS}
	return row, value.RowOrdinal, digest, meta, nil
}

func qualityFromParquet(value qualityParquetRow, manifest Manifest) (QualityRowV1, uint64, [32]byte, auxiliaryCoordinate, error) {
	if value.DatasetPolicyID != mustManifestHash(manifest.DatasetPolicyID) || value.ReplayConfigID != mustManifestHash(manifest.ReplayConfigID) || value.InputManifestSetID != mustManifestHash(manifest.InputManifestSetID) {
		return QualityRowV1{}, 0, [32]byte{}, auxiliaryCoordinate{}, fmt.Errorf("%w: quality policy IDs", ErrManifestMismatch)
	}
	coordinate := normalize.RawCoordinate{SourceID: value.SourceID, ChannelID: value.ChannelID, EpochKind: normalize.EpochKind(value.EpochKind), EpochID: value.EpochID,
		ArrivalOrdinal: value.ArrivalOrdinal, MessageOrdinal: value.MessageOrdinal, RawSegmentSHA256: value.RawSegmentSHA256,
		RawRecordOrdinal: value.RawRecordOrdinal, RawPayloadSHA256: value.RawPayloadSHA256}
	row := QualityRowV1{Version: uint16(value.Version), QualityID: value.QualityID, Kind: value.Kind, Code: value.Code,
		SourceState: normalize.SourceState(value.SourceState), SourceSchemaFingerprint: value.SourceSchemaFingerprint,
		SchemaName: value.SchemaName, SchemaVersion: uint16(value.SchemaVersion), SourceID: value.SourceID, ChannelID: value.ChannelID,
		InstrumentUID: value.InstrumentUID, InstrumentUIDState: normalize.SourceState(value.InstrumentUIDState),
		ReceivedTimeNS: value.ReceivedTimeNS, Coordinate: coordinate, MapperVersion: value.MapperVersion,
		MapperBindingID: value.MapperBindingID, CatalogSnapshotID: value.CatalogSnapshotID, PolicyID: value.PolicyID,
		QualityFlags: append([]string(nil), value.QualityFlags...)}
	if err := row.Validate(); err != nil {
		return row, 0, [32]byte{}, auxiliaryCoordinate{}, corruption(err)
	}
	digest := qualityHash(row)
	if digest != value.RowLogicalHash {
		return row, 0, digest, auxiliaryCoordinate{}, fmt.Errorf("%w: quality logical hash", ErrCorruptDataset)
	}
	meta := auxiliaryCoordinate{instrumentUID: value.InstrumentUID, sourceID: value.SourceID, channelID: value.ChannelID,
		epochKind: value.EpochKind, epochID: value.EpochID, arrivalOrdinal: value.ArrivalOrdinal,
		messageOrdinal: value.MessageOrdinal, receivedTimeNS: value.ReceivedTimeNS}
	return row, value.RowOrdinal, digest, meta, nil
}

func checkManifestPartition(value commonColumns, manifest Manifest) error {
	date, hour := utcParts(value.ReceivedTimeNS)
	if value.SourceID != manifest.SourceID || date != manifest.UTCDate || hour != manifest.UTCHour ||
		!manifestContainsEpoch(manifest, value.EpochKind, value.EpochID) {
		return fmt.Errorf("%w: row partition", ErrManifestMismatch)
	}
	return validateCommonStates(value)
}

func checkAuxPartition(value auxiliaryCoordinate, manifest Manifest) error {
	date, hour := utcParts(value.receivedTimeNS)
	if value.sourceID != manifest.SourceID || date != manifest.UTCDate || hour != manifest.UTCHour ||
		!manifestContainsEpoch(manifest, value.epochKind, value.epochID) {
		return fmt.Errorf("%w: auxiliary row partition", ErrManifestMismatch)
	}
	return nil
}

func manifestContainsEpoch(manifest Manifest, kind string, id [16]byte) bool {
	encoded := hex.EncodeToString(id[:])
	for _, epoch := range manifest.Epochs {
		if epoch.Kind == kind && epoch.ID == encoded {
			return true
		}
	}
	return false
}

func compareCommonOrder(left, right commonColumns, manifest Manifest) int {
	if order := cmp.Compare(left.InstrumentUID, right.InstrumentUID); order != 0 {
		return order
	}
	if order := cmp.Compare(manifestEpochIndex(manifest, left.EpochKind, left.EpochID), manifestEpochIndex(manifest, right.EpochKind, right.EpochID)); order != 0 {
		return order
	}
	if order := cmp.Compare(left.ArrivalOrdinal, right.ArrivalOrdinal); order != 0 {
		return order
	}
	if order := cmp.Compare(left.MessageOrdinal, right.MessageOrdinal); order != 0 {
		return order
	}
	return 0
}

func compareAuxOrder(left, right auxiliaryCoordinate, manifest Manifest) int {
	if order := cmp.Compare(left.instrumentUID, right.instrumentUID); order != 0 {
		return order
	}
	if order := cmp.Compare(manifestEpochIndex(manifest, left.epochKind, left.epochID), manifestEpochIndex(manifest, right.epochKind, right.epochID)); order != 0 {
		return order
	}
	if order := cmp.Compare(left.arrivalOrdinal, right.arrivalOrdinal); order != 0 {
		return order
	}
	if order := cmp.Compare(left.messageOrdinal, right.messageOrdinal); order != 0 {
		return order
	}
	return 0
}

func manifestEpochIndex(manifest Manifest, kind string, id [16]byte) int {
	encoded := hex.EncodeToString(id[:])
	for index, epoch := range manifest.Epochs {
		if epoch.Kind == kind && epoch.ID == encoded {
			return index
		}
	}
	return len(manifest.Epochs)
}

func validateCommonStates(value commonColumns) error {
	valid := func(pointer *int64, state string) bool {
		return (state == string(normalize.SourceMissing) && (pointer == nil || *pointer == 0)) ||
			(pointer != nil && state == string(normalize.SourceValue))
	}
	if !valid(value.ExchangeTimeNS, value.ExchangeTimeState) || !valid(value.SourceEventTimeNS, value.SourceEventTimeState) ||
		len(value.QualityFlags) > normalize.MaxQualityFlags || !slicesSortedUnique(value.QualityFlags) {
		return fmt.Errorf("%w: optional source state or quality flags", ErrCorruptDataset)
	}
	return validateDatasetString(value.SchemaName, value.SourceID, value.ChannelID, value.InstrumentUID, value.MapperVersion)
}

func slicesSortedUnique(values []string) bool {
	for i, value := range values {
		if value == "" || (i > 0 && values[i-1] >= value) {
			return false
		}
	}
	return true
}

func (b *rowBounds) include(received int64, arrival uint64, ordinal uint64) {
	if ordinal == 0 {
		b.firstTime, b.lastTime, b.firstArrival, b.lastArrival = received, received, arrival, arrival
		return
	}
	if received < b.firstTime {
		b.firstTime = received
	}
	if received > b.lastTime {
		b.lastTime = received
	}
	if arrival < b.firstArrival {
		b.firstArrival = arrival
	}
	if arrival > b.lastArrival {
		b.lastArrival = arrival
	}
}

func corruption(err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w: %v", ErrCorruptDataset, err)
}
