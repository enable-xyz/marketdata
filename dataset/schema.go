package dataset

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
	"github.com/parquet-go/parquet-go"
)

const (
	tradeParquetSchemaName      = "enable.trade.parquet.v1"
	bookParquetSchemaName       = "enable.book_update_levels.parquet.v1"
	quoteParquetSchemaName      = "enable.quote.parquet.v1"
	tickerParquetSchemaName     = "enable.ticker.parquet.v1"
	quarantineParquetSchemaName = "enable.schema_quarantine.parquet.v1"
	qualityParquetSchemaName    = "enable.quality.parquet.v1"
)

type commonColumns struct {
	EventOrdinal            uint64   `parquet:"event_ordinal,uint(64)"`
	EventID                 [32]byte `parquet:"event_id"`
	EventIDEncodingVersion  uint32   `parquet:"event_id_encoding_version,uint(16)"`
	LogicalHash             [32]byte `parquet:"logical_hash"`
	LogicalEncodingVersion  uint32   `parquet:"logical_encoding_version,uint(16)"`
	SchemaName              string   `parquet:"schema_name,enum"`
	SchemaVersion           uint32   `parquet:"schema_version,uint(16)"`
	SourceID                string   `parquet:"source_id"`
	ChannelID               string   `parquet:"channel_id"`
	InstrumentUID           string   `parquet:"instrument_uid"`
	EpochKind               string   `parquet:"epoch_kind,enum"`
	EpochID                 [16]byte `parquet:"epoch_id"`
	ArrivalOrdinal          uint64   `parquet:"arrival_ordinal,uint(64)"`
	MessageOrdinal          uint32   `parquet:"message_ordinal,uint(32)"`
	ExchangeTimeNS          *int64   `parquet:"exchange_time_ns,optional,timestamp(nanosecond:utc)"`
	ExchangeTimeState       string   `parquet:"exchange_time_state,enum"`
	ExchangeTimeResolution  string   `parquet:"exchange_time_resolution,enum"`
	SourceEventTimeNS       *int64   `parquet:"source_event_time_ns,optional,timestamp(nanosecond:utc)"`
	SourceEventTimeState    string   `parquet:"source_event_time_state,enum"`
	SourceTimeResolution    string   `parquet:"source_time_resolution,enum"`
	ReceivedTimeNS          int64    `parquet:"received_time_ns,timestamp(nanosecond:utc)"`
	RawSegmentSHA256        [32]byte `parquet:"raw_segment_sha256"`
	RawRecordOrdinal        uint64   `parquet:"raw_record_ordinal,uint(64)"`
	RawPayloadSHA256        [32]byte `parquet:"raw_payload_sha256"`
	SourceSchemaFingerprint [32]byte `parquet:"source_schema_fingerprint"`
	MapperVersion           string   `parquet:"mapper_version"`
	MapperBindingID         [32]byte `parquet:"mapper_binding_id"`
	CatalogSnapshotID       [32]byte `parquet:"catalog_snapshot_id"`
	QualityFlags            []string `parquet:"quality_flags,list"`
	DatasetPolicyID         [32]byte `parquet:"dataset_policy_id"`
	ReplayConfigID          [32]byte `parquet:"replay_config_id"`
	InputManifestSetID      [32]byte `parquet:"input_manifest_set_id"`
}

type tradeParquetRow struct {
	commonColumns
	NativeTradeID        uint64   `parquet:"native_trade_id,uint(64)"`
	AggressorSide        string   `parquet:"aggressor_side,enum"`
	BuyerIsMaker         bool     `parquet:"buyer_is_maker"`
	NativeIgnoreFlag     bool     `parquet:"native_ignore_flag"`
	Price                [16]byte `parquet:"price,decimal(18:38)"`
	Amount               [16]byte `parquet:"amount,decimal(18:38)"`
	BaseAssetID          string   `parquet:"base_asset_id"`
	QuoteAssetID         string   `parquet:"quote_asset_id"`
	AggregationKind      string   `parquet:"aggregation_kind,enum"`
	NativeDuplicateState string   `parquet:"native_duplicate_status,enum"`
}

type bookParquetRow struct {
	commonColumns
	UpdateKind                string   `parquet:"update_kind,enum"`
	DepthContract             string   `parquet:"depth_contract"`
	AggregationContract       string   `parquet:"aggregation_contract"`
	FirstSequence             uint64   `parquet:"first_sequence,uint(64)"`
	LastSequence              uint64   `parquet:"last_sequence,uint(64)"`
	PreviousSequence          *uint64  `parquet:"previous_sequence,optional,uint(64)"`
	PreviousSequenceState     string   `parquet:"previous_sequence_state,enum"`
	ChecksumState             string   `parquet:"checksum_state,enum"`
	AmountSemantics           string   `parquet:"amount_semantics"`
	ReconstructionEligibility string   `parquet:"reconstruction_eligibility"`
	BidCount                  uint32   `parquet:"bid_count,uint(32)"`
	AskCount                  uint32   `parquet:"ask_count,uint(32)"`
	HasLevel                  bool     `parquet:"has_level"`
	LevelOrdinal              uint32   `parquet:"level_ordinal,uint(32)"`
	SideOrdinal               uint32   `parquet:"side_ordinal,uint(32)"`
	Side                      string   `parquet:"side,enum"`
	Action                    string   `parquet:"action,enum"`
	Price                     [16]byte `parquet:"price,decimal(18:38)"`
	Amount                    [16]byte `parquet:"amount,decimal(18:38)"`
	PriceBaseAssetID          string   `parquet:"price_base_asset_id"`
	PriceQuoteAssetID         string   `parquet:"price_quote_asset_id"`
	AmountAssetID             string   `parquet:"amount_asset_id"`
}

type quoteParquetRow struct {
	commonColumns
	NativeSourceRole  string   `parquet:"native_source_role"`
	UpdateID          uint64   `parquet:"update_id,uint(64)"`
	BidPrice          [16]byte `parquet:"bid_price,decimal(18:38)"`
	BidAmount         [16]byte `parquet:"bid_amount,decimal(18:38)"`
	AskPrice          [16]byte `parquet:"ask_price,decimal(18:38)"`
	AskAmount         [16]byte `parquet:"ask_amount,decimal(18:38)"`
	BaseAssetID       string   `parquet:"base_asset_id"`
	QuoteAssetID      string   `parquet:"quote_asset_id"`
	RPIInclusionState string   `parquet:"rpi_inclusion_state,enum"`
	SourceTimeNS      *int64   `parquet:"source_time_ns,optional,timestamp(nanosecond:utc)"`
	SourceTimeState   string   `parquet:"source_time_state,enum"`
}

type tickerParquetRow struct {
	commonColumns
	NativeSourceRole            string   `parquet:"native_source_role"`
	WindowKind                  string   `parquet:"window_kind,enum"`
	WindowOpenSemantics         string   `parquet:"window_open_semantics"`
	WindowCloseSemantics        string   `parquet:"window_close_semantics"`
	WindowOpenTimeNS            int64    `parquet:"window_open_time_ns,timestamp(nanosecond:utc)"`
	WindowCloseTimeNS           int64    `parquet:"window_close_time_ns,timestamp(nanosecond:utc)"`
	WindowTimeResolution        string   `parquet:"window_time_resolution,enum"`
	NominalWindowDurationNS     uint64   `parquet:"nominal_window_duration_ns,uint(64)"`
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
	BaseAssetID                 string   `parquet:"base_asset_id"`
	QuoteAssetID                string   `parquet:"quote_asset_id"`
	FirstTradeID                uint64   `parquet:"first_trade_id,uint(64)"`
	LastTradeID                 uint64   `parquet:"last_trade_id,uint(64)"`
	TradeCount                  uint64   `parquet:"trade_count,uint(64)"`
}

type quarantineParquetRow struct {
	RowOrdinal              uint64   `parquet:"row_ordinal,uint(64)"`
	RowLogicalHash          [32]byte `parquet:"row_logical_hash"`
	Version                 uint32   `parquet:"version,uint(16)"`
	QuarantineID            [32]byte `parquet:"quarantine_id"`
	Code                    string   `parquet:"code,enum"`
	Field                   string   `parquet:"field"`
	SourceState             string   `parquet:"source_state,enum"`
	FingerprintClass        string   `parquet:"fingerprint_class,enum"`
	SourceSchemaFingerprint [32]byte `parquet:"source_schema_fingerprint"`
	SourceID                string   `parquet:"source_id"`
	ChannelID               string   `parquet:"channel_id"`
	ReceivedTimeNS          int64    `parquet:"received_time_ns,timestamp(nanosecond:utc)"`
	EpochKind               string   `parquet:"epoch_kind,enum"`
	EpochID                 [16]byte `parquet:"epoch_id"`
	ArrivalOrdinal          uint64   `parquet:"arrival_ordinal,uint(64)"`
	MessageOrdinal          uint32   `parquet:"message_ordinal,uint(32)"`
	RawSegmentSHA256        [32]byte `parquet:"raw_segment_sha256"`
	RawRecordOrdinal        uint64   `parquet:"raw_record_ordinal,uint(64)"`
	RawPayloadSHA256        [32]byte `parquet:"raw_payload_sha256"`
	MapperVersion           string   `parquet:"mapper_version"`
	MapperBindingID         [32]byte `parquet:"mapper_binding_id"`
	SourceTimeResolution    string   `parquet:"source_time_resolution,enum"`
	CatalogSnapshotID       [32]byte `parquet:"catalog_snapshot_id"`
	DatasetPolicyID         [32]byte `parquet:"dataset_policy_id"`
	ReplayConfigID          [32]byte `parquet:"replay_config_id"`
	InputManifestSetID      [32]byte `parquet:"input_manifest_set_id"`
}

type qualityParquetRow struct {
	RowOrdinal              uint64   `parquet:"row_ordinal,uint(64)"`
	RowLogicalHash          [32]byte `parquet:"row_logical_hash"`
	Version                 uint32   `parquet:"version,uint(16)"`
	QualityID               [32]byte `parquet:"quality_id"`
	Kind                    string   `parquet:"kind,enum"`
	Code                    string   `parquet:"code,enum"`
	SourceState             string   `parquet:"source_state,enum"`
	SourceSchemaFingerprint [32]byte `parquet:"source_schema_fingerprint"`
	SchemaName              string   `parquet:"schema_name"`
	SchemaVersion           uint32   `parquet:"schema_version,uint(16)"`
	SourceID                string   `parquet:"source_id"`
	ChannelID               string   `parquet:"channel_id"`
	InstrumentUID           string   `parquet:"instrument_uid"`
	InstrumentUIDState      string   `parquet:"instrument_uid_state,enum"`
	ReceivedTimeNS          int64    `parquet:"received_time_ns,timestamp(nanosecond:utc)"`
	EpochKind               string   `parquet:"epoch_kind,enum"`
	EpochID                 [16]byte `parquet:"epoch_id"`
	ArrivalOrdinal          uint64   `parquet:"arrival_ordinal,uint(64)"`
	MessageOrdinal          uint32   `parquet:"message_ordinal,uint(32)"`
	RawSegmentSHA256        [32]byte `parquet:"raw_segment_sha256"`
	RawRecordOrdinal        uint64   `parquet:"raw_record_ordinal,uint(64)"`
	RawPayloadSHA256        [32]byte `parquet:"raw_payload_sha256"`
	MapperVersion           string   `parquet:"mapper_version"`
	MapperBindingID         [32]byte `parquet:"mapper_binding_id"`
	CatalogSnapshotID       [32]byte `parquet:"catalog_snapshot_id"`
	PolicyID                [32]byte `parquet:"policy_id"`
	QualityFlags            []string `parquet:"quality_flags,list"`
	DatasetPolicyID         [32]byte `parquet:"dataset_policy_id"`
	ReplayConfigID          [32]byte `parquet:"replay_config_id"`
	InputManifestSetID      [32]byte `parquet:"input_manifest_set_id"`
}

func familySchema(family Family) (string, uint16, *parquet.Schema, error) {
	switch family {
	case FamilyTrade:
		return tradeParquetSchemaName, 1, parquet.SchemaOf(new(tradeParquetRow)), nil
	case FamilyBookUpdate:
		return bookParquetSchemaName, 1, parquet.SchemaOf(new(bookParquetRow)), nil
	case FamilyQuote:
		return quoteParquetSchemaName, 1, parquet.SchemaOf(new(quoteParquetRow)), nil
	case FamilyTicker:
		return tickerParquetSchemaName, 1, parquet.SchemaOf(new(tickerParquetRow)), nil
	case FamilySchemaQuarantine:
		return quarantineParquetSchemaName, 1, parquet.SchemaOf(new(quarantineParquetRow)), nil
	case FamilyQuality:
		return qualityParquetSchemaName, 1, parquet.SchemaOf(new(qualityParquetRow)), nil
	default:
		return "", 0, nil, fmt.Errorf("%w: unsupported family %q", ErrInvalidInput, family)
	}
}

func schemaDigest(name string, version uint16, schema *parquet.Schema) [32]byte {
	payload := fmt.Sprintf("dataset-schema-v1\n%s\n%d\n%s\n%s", name, version, ParquetFormatCompatibility, schema.String())
	return sha256.Sum256([]byte(payload))
}

func decimalBytes(value normalize.Decimal, scale uint8) ([16]byte, error) {
	var encoded [16]byte
	if err := value.Validate(); err != nil || value.Scale != scale {
		return encoded, fmt.Errorf("%w: decimal scale", ErrInvalidInput)
	}
	wide, ok := new(big.Int).SetString(value.Coefficient, 10)
	if !ok {
		return encoded, fmt.Errorf("%w: decimal coefficient", ErrInvalidInput)
	}
	if wide.Sign() < 0 {
		wide.Add(wide, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	bytes := wide.Bytes()
	if len(bytes) > len(encoded) {
		return encoded, fmt.Errorf("%w: decimal coefficient width", ErrInvalidInput)
	}
	copy(encoded[len(encoded)-len(bytes):], bytes)
	return encoded, nil
}

func decimalValue(encoded [16]byte, scale uint8) (normalize.Decimal, error) {
	wide := new(big.Int).SetBytes(encoded[:])
	if encoded[0]&0x80 != 0 {
		wide.Sub(wide, new(big.Int).Lsh(big.NewInt(1), 128))
	}
	value := normalize.Decimal{Coefficient: wide.String(), Scale: scale}
	if err := value.Validate(); err != nil {
		return normalize.Decimal{}, fmt.Errorf("%w: invalid decimal bytes", ErrCorruptDataset)
	}
	return value, nil
}

func optionalInt64(value normalize.OptionalInt64) (*int64, string) {
	if !value.Valid {
		return nil, string(normalize.SourceMissing)
	}
	copy := value.Value
	return &copy, string(normalize.SourceValue)
}

func optionalUint64(value normalize.OptionalUint64) (*uint64, string) {
	if !value.Valid {
		return nil, string(normalize.SourceMissing)
	}
	copy := value.Value
	return &copy, string(normalize.SourceValue)
}

func commonFromRow(row normalize.Row, ordinal uint64, options WriterOptions) (commonColumns, error) {
	if err := row.Validate(); err != nil {
		return commonColumns{}, fmt.Errorf("%w: normalized row: %v", ErrInvalidInput, err)
	}
	metadata := row.Common()
	exchangeTime, exchangeState := optionalInt64(metadata.ExchangeTimeNS)
	sourceEventTime, sourceState := optionalInt64(metadata.SourceEventTimeNS)
	flags := make([]string, len(metadata.QualityFlags))
	for i, flag := range metadata.QualityFlags {
		flags[i] = string(flag)
	}
	return commonColumns{
		EventOrdinal: ordinal, EventID: metadata.EventID, EventIDEncodingVersion: uint32(metadata.EventIDEncodingVersion),
		LogicalHash: row.LogicalHash, LogicalEncodingVersion: uint32(row.LogicalEncodingVersion),
		SchemaName: metadata.SchemaName, SchemaVersion: uint32(metadata.SchemaVersion), SourceID: metadata.SourceID,
		ChannelID: metadata.ChannelID, InstrumentUID: metadata.InstrumentUID, EpochKind: string(metadata.EpochKind),
		EpochID: metadata.EpochID, ArrivalOrdinal: metadata.ArrivalOrdinal, MessageOrdinal: metadata.MessageOrdinal,
		ExchangeTimeNS: exchangeTime, ExchangeTimeState: exchangeState, ExchangeTimeResolution: string(metadata.ExchangeTimeResolution),
		SourceEventTimeNS: sourceEventTime, SourceEventTimeState: sourceState, SourceTimeResolution: string(metadata.SourceTimeResolution),
		ReceivedTimeNS: metadata.ReceivedTimeNS, RawSegmentSHA256: metadata.RawSegmentSHA256,
		RawRecordOrdinal: metadata.RawRecordOrdinal, RawPayloadSHA256: metadata.RawPayloadSHA256,
		SourceSchemaFingerprint: metadata.SourceSchemaFingerprint, MapperVersion: metadata.MapperVersion,
		MapperBindingID: metadata.MapperBindingID, CatalogSnapshotID: metadata.CatalogSnapshotID, QualityFlags: flags,
		DatasetPolicyID: options.DatasetPolicyID, ReplayConfigID: options.ReplayConfigID, InputManifestSetID: options.InputManifestSetID,
	}, nil
}

func metadataFromCommon(value commonColumns, manifest Manifest) (normalize.Metadata, error) {
	if value.DatasetPolicyID != mustManifestHash(manifest.DatasetPolicyID) || value.ReplayConfigID != mustManifestHash(manifest.ReplayConfigID) ||
		value.InputManifestSetID != mustManifestHash(manifest.InputManifestSetID) {
		return normalize.Metadata{}, fmt.Errorf("%w: row policy/config identity", ErrManifestMismatch)
	}
	flags := make([]normalize.QualityFlag, len(value.QualityFlags))
	for i, flag := range value.QualityFlags {
		flags[i] = normalize.QualityFlag(flag)
	}
	exchangeTime, err := parquetOptionalInt64(value.ExchangeTimeNS, value.ExchangeTimeState)
	if err != nil {
		return normalize.Metadata{}, err
	}
	sourceEventTime, err := parquetOptionalInt64(value.SourceEventTimeNS, value.SourceEventTimeState)
	if err != nil {
		return normalize.Metadata{}, err
	}
	metadata := normalize.Metadata{
		EventID: value.EventID, EventIDEncodingVersion: uint16(value.EventIDEncodingVersion), SchemaName: value.SchemaName,
		SchemaVersion: uint16(value.SchemaVersion), SourceID: value.SourceID, ChannelID: value.ChannelID,
		InstrumentUID: value.InstrumentUID, EpochKind: normalize.EpochKind(value.EpochKind), EpochID: value.EpochID,
		ArrivalOrdinal: value.ArrivalOrdinal, MessageOrdinal: value.MessageOrdinal,
		ExchangeTimeNS:         exchangeTime,
		ExchangeTimeResolution: normalize.TimeResolution(value.ExchangeTimeResolution),
		SourceEventTimeNS:      sourceEventTime,
		SourceTimeResolution:   normalize.TimeResolution(value.SourceTimeResolution), ReceivedTimeNS: value.ReceivedTimeNS,
		RawSegmentSHA256: value.RawSegmentSHA256, RawRecordOrdinal: value.RawRecordOrdinal,
		RawPayloadSHA256: value.RawPayloadSHA256, SourceSchemaFingerprint: value.SourceSchemaFingerprint,
		MapperVersion: value.MapperVersion, MapperBindingID: value.MapperBindingID,
		CatalogSnapshotID: value.CatalogSnapshotID, QualityFlags: flags,
	}
	if err := metadata.Validate(); err != nil {
		return normalize.Metadata{}, fmt.Errorf("%w: common metadata: %v", ErrCorruptDataset, err)
	}
	return metadata, nil
}

func parquetOptionalInt64(value *int64, state string) (normalize.OptionalInt64, error) {
	switch {
	case value == nil && state == string(normalize.SourceMissing):
		return normalize.OptionalInt64{}, nil
	case value != nil && state == string(normalize.SourceValue):
		return normalize.OptionalInt64{Value: *value, Valid: true}, nil
	default:
		return normalize.OptionalInt64{}, fmt.Errorf("%w: invalid optional int64 value/state pair", ErrCorruptDataset)
	}
}

func parquetOptionalUint64(value *uint64, state string) (normalize.OptionalUint64, error) {
	switch {
	case value == nil && state == string(normalize.SourceMissing):
		return normalize.OptionalUint64{}, nil
	case value != nil && state == string(normalize.SourceValue):
		return normalize.OptionalUint64{Value: *value, Valid: true}, nil
	default:
		return normalize.OptionalUint64{}, fmt.Errorf("%w: invalid optional uint64 value/state pair", ErrCorruptDataset)
	}
}

func mustManifestHash(text string) normalize.Hash {
	decoded, _ := hex.DecodeString(text)
	var value normalize.Hash
	copy(value[:], decoded)
	return value
}

func validateDatasetString(values ...string) error {
	for _, value := range values {
		if len(value) > MaxDatasetStringBytes || strings.IndexByte(value, 0) >= 0 {
			return fmt.Errorf("%w: string bound", ErrCorruptDataset)
		}
	}
	return nil
}

func quarantineHash(value normalize.SchemaQuarantineV1) [32]byte {
	encoded, _ := json.Marshal(value)
	return sha256.Sum256(append([]byte("dataset-schema-quarantine-logical-v1\x00"), encoded...))
}

func qualityHash(value QualityRowV1) [32]byte {
	encoded, _ := json.Marshal(value)
	return sha256.Sum256(append([]byte("dataset-quality-logical-v1\x00"), encoded...))
}

func validateQuarantine(value normalize.SchemaQuarantineV1) error {
	coordinate := value.Coordinate
	if value.Version != normalize.SchemaQuarantineVersion || value.QuarantineID == (normalize.Hash{}) || value.SourceSchemaFingerprint == (normalize.Hash{}) ||
		!validSourceState(value.SourceState) || !validQuarantineCode(value.Code) || !validFingerprintClass(value.FingerprintClass) ||
		!validTimeResolution(value.SourceTimeResolution) || value.SourceID == "" || value.ChannelID == "" || value.ReceivedTimeNS < 0 ||
		coordinate.SourceID != value.SourceID || coordinate.ChannelID != value.ChannelID || coordinate.EpochID == ([16]byte{}) ||
		(coordinate.EpochKind != normalize.ConnectionEpoch && coordinate.EpochKind != normalize.PollCycleEpoch) ||
		coordinate.ArrivalOrdinal == 0 || coordinate.RawSegmentSHA256 == (normalize.Hash{}) ||
		coordinate.RawPayloadSHA256 == (normalize.Hash{}) || value.MapperBindingID == (normalize.Hash{}) || value.CatalogSnapshotID == (normalize.Hash{}) {
		return fmt.Errorf("%w: invalid schema quarantine", ErrInvalidInput)
	}
	return validateDatasetString(value.Field, value.SourceID, value.ChannelID, value.MapperVersion)
}

func validQuarantineCode(value normalize.QuarantineCode) bool {
	switch value {
	case normalize.QuarantineBindingUnavailable, normalize.QuarantineSchemaMalformed, normalize.QuarantineSchemaUnknown,
		normalize.QuarantineSemanticChange, normalize.QuarantineTypeMeaningChange, normalize.QuarantineInvalidField,
		normalize.QuarantineBounds, normalize.QuarantineInstrument, normalize.QuarantineMapperOutput:
		return true
	default:
		return false
	}
}

func validFingerprintClass(value normalize.FingerprintClass) bool {
	switch value {
	case normalize.FingerprintExact, normalize.FingerprintAdditiveHarmless, normalize.FingerprintSemanticAdditive,
		normalize.FingerprintTypeOrMeaningChange, normalize.FingerprintUnknown:
		return true
	default:
		return false
	}
}

func validTimeResolution(value normalize.TimeResolution) bool {
	switch value {
	case normalize.ResolutionAbsent, normalize.ResolutionNanosecond, normalize.ResolutionMicrosecond,
		normalize.ResolutionMillisecond, normalize.ResolutionSecond:
		return true
	default:
		return false
	}
}

func sortedStrings(values []string) []string {
	copy := slices.Clone(values)
	slices.Sort(copy)
	return copy
}
