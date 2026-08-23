package binance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
)

const SpotMapperVersion = "binance.spot.mapper.v1"

var ErrSpotMap = errors.New("binance: Spot normalization rejected")

type SpotMapper struct {
	version string
}

func NewSpotMapper(version string) (*SpotMapper, error) {
	if version == "" || len(version) > normalize.MaxMapperVersionBytes {
		return nil, fmt.Errorf("%w: mapper version", ErrSpotMap)
	}
	return &SpotMapper{version: version}, nil
}

func (m *SpotMapper) Version() string {
	if m == nil {
		return ""
	}
	return m.version
}

func SpotMapperTimeResolution(config SpotWSConfig) normalize.TimeResolution {
	if config.MicrosecondTime {
		return normalize.ResolutionMicrosecond
	}
	return normalize.ResolutionMillisecond
}

func NewSpotMapperBinding(snapshotID normalize.Hash, version string, effectiveFromNS int64, effectiveUntilNS normalize.OptionalInt64, sourceTimeResolution normalize.TimeResolution, additional []normalize.FingerprintRule) (normalize.BoundMapper, error) {
	mapper, err := NewSpotMapper(version)
	if err != nil {
		return normalize.BoundMapper{}, err
	}
	rules, err := SpotFingerprintRules()
	if err != nil {
		return normalize.BoundMapper{}, err
	}
	rules = append(rules, additional...)
	return normalize.BoundMapper{
		Binding: normalize.MapperBinding{
			Version: normalize.MapperBindingVersion, SourceID: SpotSourceID, ChannelID: SpotRawChannel,
			EffectiveFromNS: effectiveFromNS, EffectiveUntilNS: effectiveUntilNS,
			MapperVersion: version, SourceTimeResolution: sourceTimeResolution,
			CatalogSnapshotID: snapshotID, FingerprintRules: rules,
		},
		Mapper: mapper,
	}, nil
}

func SpotFingerprintRules() ([]normalize.FingerprintRule, error) {
	samples := [][]byte{
		[]byte(`{"e":"trade","E":1,"s":"X","t":1,"p":"1","q":"1","T":1,"m":true,"M":true}`),
		[]byte(`{"e":"depthUpdate","E":1,"s":"X","U":1,"u":1,"b":[["1","1"]],"a":[["1","1"]]}`),
		[]byte(`{"e":"depthUpdate","E":1,"s":"X","U":1,"u":1,"b":[],"a":[["1","1"]]}`),
		[]byte(`{"e":"depthUpdate","E":1,"s":"X","U":1,"u":1,"b":[["1","1"]],"a":[]}`),
		[]byte(`{"e":"depthUpdate","E":1,"s":"X","U":1,"u":1,"b":[],"a":[]}`),
		[]byte(`{"u":1,"s":"X","b":"1","B":"1","a":"1","A":"1"}`),
		[]byte(`{"e":"24hrTicker","E":1,"s":"X","p":"1","P":"1","w":"1","x":"1","c":"1","Q":"1","b":"1","B":"1","a":"1","A":"1","o":"1","h":"1","l":"1","v":"1","q":"1","O":1,"C":1,"F":1,"L":1,"n":1}`),
	}
	rules := make([]normalize.FingerprintRule, 0, len(samples))
	seen := make(map[normalize.Hash]struct{}, len(samples))
	for _, sample := range samples {
		observation, err := normalize.StructuralFingerprint(sample)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[observation.Fingerprint]; exists {
			continue
		}
		seen[observation.Fingerprint] = struct{}{}
		rules = append(rules, normalize.FingerprintRule{Fingerprint: observation.Fingerprint, Class: normalize.FingerprintExact})
	}
	return rules, nil
}

func (m *SpotMapper) Map(input normalize.MappingInput) ([]normalize.Row, error) {
	if m == nil || input.Binding.MapperVersion != m.version || input.Catalog == nil ||
		input.Record.Coordinate.SourceID != SpotSourceID || input.Record.Coordinate.ChannelID != SpotRawChannel ||
		input.Record.Envelope.RecordKind != capture.RecordKindWebSocket || input.Record.Envelope.PayloadEncoding != capture.PayloadEncodingJSON {
		return nil, normalize.RejectProjection(normalize.QuarantineInvalidField, "raw_boundary", normalize.SourceValue)
	}
	object, err := decodeSpotObject(input.Record.Envelope.RawPayload)
	if err != nil {
		return nil, err
	}
	if raw, ok := object["e"]; ok {
		event, state, err := mappedSpotString(raw)
		if err != nil {
			return nil, normalize.RejectProjection(normalize.QuarantineInvalidField, "e", state)
		}
		switch event {
		case "trade":
			row, err := m.mapTrade(input, object)
			if err != nil {
				return nil, err
			}
			return []normalize.Row{row}, nil
		case "depthUpdate":
			row, err := m.mapDepth(input, object)
			if err != nil {
				return nil, err
			}
			return []normalize.Row{row}, nil
		case "24hrTicker":
			row, err := m.mapTicker(input, object)
			if err != nil {
				return nil, err
			}
			return []normalize.Row{row}, nil
		default:
			return nil, normalize.RejectProjection(normalize.QuarantineSchemaUnknown, "e", normalize.SourceValue)
		}
	}
	row, err := m.mapBookTicker(input, object)
	if err != nil {
		return nil, err
	}
	return []normalize.Row{row}, nil
}

var spotTradeFields = []string{"E", "M", "T", "e", "m", "p", "q", "s", "t"}
var spotDepthFields = []string{"E", "U", "a", "b", "e", "s", "u"}
var spotBookTickerFields = []string{"A", "B", "a", "b", "s", "u"}
var spotTickerFields = []string{"A", "B", "C", "E", "F", "L", "O", "P", "Q", "a", "b", "c", "e", "h", "l", "n", "o", "p", "q", "s", "v", "w", "x"}

func validateSpotFields(object map[string]json.RawMessage, required []string, rule normalize.FingerprintRule) error {
	allowed := make(map[string]struct{}, len(required)+len(rule.AllowedUnknownFields))
	for _, field := range required {
		allowed[field] = struct{}{}
	}
	for _, field := range rule.AllowedUnknownFields {
		allowed[field] = struct{}{}
	}
	if len(object) != len(allowed) {
		for _, field := range required {
			if _, ok := object[field]; !ok {
				return normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceMissing)
			}
		}
		return normalize.RejectProjection(normalize.QuarantineSchemaUnknown, "object_fields", normalize.SourceValue)
	}
	for field := range object {
		if _, ok := allowed[field]; !ok {
			return normalize.RejectProjection(normalize.QuarantineSchemaUnknown, "object_fields", normalize.SourceValue)
		}
	}
	for _, field := range required {
		if _, ok := object[field]; !ok {
			return normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceMissing)
		}
	}
	return nil
}

func (m *SpotMapper) mapTrade(input normalize.MappingInput, object map[string]json.RawMessage) (normalize.Row, error) {
	if err := validateSpotFields(object, spotTradeFields, input.Fingerprint); err != nil {
		return normalize.Row{}, err
	}
	instrument, err := spotInstrument(input, object)
	if err != nil {
		return normalize.Row{}, err
	}
	tradeID, err := spotUint(object, "t")
	if err != nil {
		return normalize.Row{}, err
	}
	eventTime, resolution, err := spotTime(input, object, "E")
	if err != nil {
		return normalize.Row{}, err
	}
	tradeTime, _, err := spotTime(input, object, "T")
	if err != nil {
		return normalize.Row{}, err
	}
	buyerMaker, err := spotBool(object, "m")
	if err != nil {
		return normalize.Row{}, err
	}
	ignore, err := spotBool(object, "M")
	if err != nil {
		return normalize.Row{}, err
	}
	price, err := spotNumeric(object, "p", normalize.CanonicalPriceScale, normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID), false)
	if err != nil {
		return normalize.Row{}, err
	}
	amount, err := spotNumeric(object, "q", normalize.CanonicalAmountScale, normalize.BaseAssetUnit(instrument.BaseAssetID), false)
	if err != nil {
		return normalize.Row{}, err
	}
	metadata, err := spotMetadata(input, instrument, normalize.TradeSchemaName, normalize.TradeSchemaVersion,
		normalize.OptionalInt64{Value: tradeTime, Valid: true}, resolution, normalize.OptionalInt64{Value: eventTime, Valid: true})
	if err != nil {
		return normalize.Row{}, err
	}
	side := normalize.SideBuy
	if buyerMaker {
		side = normalize.SideSell
	}
	return normalize.NewTradeRow(normalize.TradeV1{
		Metadata: metadata, NativeTradeID: tradeID, AggressorSide: side, BuyerIsMaker: buyerMaker,
		NativeIgnoreFlag: ignore, Price: price, Amount: amount,
		AggregationKind: normalize.AggregationSingleMatch, NativeDuplicateStatus: normalize.DuplicateUnassessed,
	})
}

func (m *SpotMapper) mapDepth(input normalize.MappingInput, object map[string]json.RawMessage) (normalize.Row, error) {
	if err := validateSpotFields(object, spotDepthFields, input.Fingerprint); err != nil {
		return normalize.Row{}, err
	}
	instrument, err := spotInstrument(input, object)
	if err != nil {
		return normalize.Row{}, err
	}
	first, err := spotUint(object, "U")
	if err != nil {
		return normalize.Row{}, err
	}
	last, err := spotUint(object, "u")
	if err != nil {
		return normalize.Row{}, err
	}
	if first > last {
		return normalize.Row{}, normalize.RejectProjection(normalize.QuarantineInvalidField, "U/u", normalize.SourceValue)
	}
	eventTime, resolution, err := spotTime(input, object, "E")
	if err != nil {
		return normalize.Row{}, err
	}
	bids, err := mappedSpotLevels(object, "b", normalize.SideBuy, instrument)
	if err != nil {
		return normalize.Row{}, err
	}
	asks, err := mappedSpotLevels(object, "a", normalize.SideSell, instrument)
	if err != nil {
		return normalize.Row{}, err
	}
	metadata, err := spotMetadata(input, instrument, normalize.BookUpdateSchemaName, normalize.BookUpdateSchemaVersion,
		normalize.OptionalInt64{Value: eventTime, Valid: true}, resolution, normalize.OptionalInt64{Value: eventTime, Valid: true})
	if err != nil {
		return normalize.Row{}, err
	}
	return normalize.NewBookUpdateRow(normalize.BookUpdateV1{
		Metadata: metadata, UpdateKind: normalize.UpdateDelta, DepthContract: "diff_depth", AggregationContract: "100ms",
		FirstSequence: first, LastSequence: last, Checksum: normalize.SourceMissing,
		Bids: bids, Asks: asks, AmountSemantics: "absolute_base_asset_quantity",
		ReconstructionEligibility: "eligible_with_rest_snapshot_bridge",
	})
}

func (m *SpotMapper) mapBookTicker(input normalize.MappingInput, object map[string]json.RawMessage) (normalize.Row, error) {
	if err := validateSpotFields(object, spotBookTickerFields, input.Fingerprint); err != nil {
		return normalize.Row{}, err
	}
	instrument, err := spotInstrument(input, object)
	if err != nil {
		return normalize.Row{}, err
	}
	updateID, err := spotUint(object, "u")
	if err != nil {
		return normalize.Row{}, err
	}
	priceUnit := normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)
	amountUnit := normalize.BaseAssetUnit(instrument.BaseAssetID)
	bidPrice, err := spotNumeric(object, "b", normalize.CanonicalPriceScale, priceUnit, false)
	if err != nil {
		return normalize.Row{}, err
	}
	bidAmount, err := spotNumeric(object, "B", normalize.CanonicalAmountScale, amountUnit, false)
	if err != nil {
		return normalize.Row{}, err
	}
	askPrice, err := spotNumeric(object, "a", normalize.CanonicalPriceScale, priceUnit, false)
	if err != nil {
		return normalize.Row{}, err
	}
	askAmount, err := spotNumeric(object, "A", normalize.CanonicalAmountScale, amountUnit, false)
	if err != nil {
		return normalize.Row{}, err
	}
	metadata, err := spotMetadata(input, instrument, normalize.QuoteSchemaName, normalize.QuoteSchemaVersion,
		normalize.OptionalInt64{}, normalize.ResolutionAbsent, normalize.OptionalInt64{})
	if err != nil {
		return normalize.Row{}, err
	}
	return normalize.NewQuoteRow(normalize.QuoteV1{
		Metadata: metadata, NativeSourceRole: "bookTicker_native_bbo", UpdateID: updateID,
		BidPrice: bidPrice, BidAmount: bidAmount, AskPrice: askPrice, AskAmount: askAmount,
		RPIInclusionState: normalize.RPINotApplicable,
	})
}

func (m *SpotMapper) mapTicker(input normalize.MappingInput, object map[string]json.RawMessage) (normalize.Row, error) {
	if err := validateSpotFields(object, spotTickerFields, input.Fingerprint); err != nil {
		return normalize.Row{}, err
	}
	instrument, err := spotInstrument(input, object)
	if err != nil {
		return normalize.Row{}, err
	}
	eventTime, resolution, err := spotTime(input, object, "E")
	if err != nil {
		return normalize.Row{}, err
	}
	openNative, err := spotUint(object, "O")
	if err != nil {
		return normalize.Row{}, err
	}
	closeNative, err := spotUint(object, "C")
	if err != nil {
		return normalize.Row{}, err
	}
	_, multiplier, windowUnits, err := spotTimeContract(input.Binding.SourceTimeResolution)
	if err != nil {
		return normalize.Row{}, err
	}
	if closeNative < openNative || closeNative-openNative != windowUnits {
		return normalize.Row{}, normalize.RejectProjection(normalize.QuarantineInvalidField, "O/C", normalize.SourceValue)
	}
	openTime, err := scaleSpotTime(input, openNative, multiplier, "O")
	if err != nil {
		return normalize.Row{}, err
	}
	closeTime, err := scaleSpotTime(input, closeNative, multiplier, "C")
	if err != nil {
		return normalize.Row{}, err
	}
	firstID, err := spotUint(object, "F")
	if err != nil {
		return normalize.Row{}, err
	}
	lastID, err := spotUint(object, "L")
	if err != nil {
		return normalize.Row{}, err
	}
	count, err := spotUint(object, "n")
	if err != nil {
		return normalize.Row{}, err
	}
	if firstID > lastID {
		return normalize.Row{}, normalize.RejectProjection(normalize.QuarantineInvalidField, "F/L", normalize.SourceValue)
	}
	priceUnit := normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)
	baseUnit := normalize.BaseAssetUnit(instrument.BaseAssetID)
	quoteUnit := normalize.QuoteAssetUnit(instrument.QuoteAssetID)
	price := func(field string, negative bool) (normalize.Numeric, error) {
		return spotNumeric(object, field, normalize.CanonicalPriceScale, priceUnit, negative)
	}
	amount := func(field string) (normalize.Numeric, error) {
		return spotNumeric(object, field, normalize.CanonicalAmountScale, baseUnit, false)
	}
	priceChange, err := price("p", true)
	if err != nil {
		return normalize.Row{}, err
	}
	percent, err := spotNumeric(object, "P", normalize.CanonicalPercentScale, normalize.PercentUnit(), true)
	if err != nil {
		return normalize.Row{}, err
	}
	weighted, err := price("w", false)
	if err != nil {
		return normalize.Row{}, err
	}
	before, err := price("x", false)
	if err != nil {
		return normalize.Row{}, err
	}
	lastPrice, err := price("c", false)
	if err != nil {
		return normalize.Row{}, err
	}
	lastAmount, err := amount("Q")
	if err != nil {
		return normalize.Row{}, err
	}
	bidPrice, err := price("b", false)
	if err != nil {
		return normalize.Row{}, err
	}
	bidAmount, err := amount("B")
	if err != nil {
		return normalize.Row{}, err
	}
	askPrice, err := price("a", false)
	if err != nil {
		return normalize.Row{}, err
	}
	askAmount, err := amount("A")
	if err != nil {
		return normalize.Row{}, err
	}
	openPrice, err := price("o", false)
	if err != nil {
		return normalize.Row{}, err
	}
	highPrice, err := price("h", false)
	if err != nil {
		return normalize.Row{}, err
	}
	lowPrice, err := price("l", false)
	if err != nil {
		return normalize.Row{}, err
	}
	baseVolume, err := amount("v")
	if err != nil {
		return normalize.Row{}, err
	}
	quoteVolume, err := spotNumeric(object, "q", normalize.CanonicalAmountScale, quoteUnit, false)
	if err != nil {
		return normalize.Row{}, err
	}
	metadata, err := spotMetadata(input, instrument, normalize.TickerSchemaName, normalize.TickerSchemaVersion,
		normalize.OptionalInt64{Value: eventTime, Valid: true}, resolution, normalize.OptionalInt64{Value: eventTime, Valid: true})
	if err != nil {
		return normalize.Row{}, err
	}
	return normalize.NewTickerRow(normalize.TickerV1{
		Metadata: metadata, NativeSourceRole: "24hrTicker_statistics_not_bbo", WindowKind: normalize.WindowRolling24Hours,
		WindowOpenSemantics: "native_statistics_open_time", WindowCloseSemantics: "native_statistics_close_time",
		WindowOpenTimeNS: openTime, WindowCloseTimeNS: closeTime, WindowTimeResolution: resolution,
		NominalWindowDurationNS: 86_400_000_000_000, PriceChange: priceChange, PriceChangePercent: percent,
		WeightedAveragePrice: weighted, FirstTradeBeforeWindowPrice: before, LastPrice: lastPrice,
		LastAmount: lastAmount, NativeBestBidPrice: bidPrice, NativeBestBidAmount: bidAmount,
		NativeBestAskPrice: askPrice, NativeBestAskAmount: askAmount, OpenPrice: openPrice,
		HighPrice: highPrice, LowPrice: lowPrice, BaseVolume: baseVolume, QuoteVolume: quoteVolume,
		FirstTradeID: firstID, LastTradeID: lastID, TradeCount: count,
	})
}

func spotMetadata(input normalize.MappingInput, instrument normalize.InstrumentIdentity, schema string, version uint16, exchange normalize.OptionalInt64, resolution normalize.TimeResolution, event normalize.OptionalInt64) (normalize.Metadata, error) {
	flags := []normalize.QualityFlag(nil)
	if input.Fingerprint.Class == normalize.FingerprintAdditiveHarmless {
		flags = append(flags, normalize.QualitySchemaAdditiveField)
	}
	return normalize.NewMetadata(normalize.MetadataInput{
		Record: input.Record, SchemaName: schema, SchemaVersion: version, InstrumentUID: instrument.InstrumentUID,
		ExchangeTimeNS: exchange, ExchangeTimeResolution: resolution, SourceEventTimeNS: event,
		SourceTimeResolution:    input.Binding.SourceTimeResolution,
		SourceSchemaFingerprint: input.Fingerprint.Fingerprint, MapperVersion: input.Binding.MapperVersion,
		MapperBindingID: input.Binding.BindingID, CatalogSnapshotID: input.Binding.CatalogSnapshotID,
		AdditionalQualityFlags: flags,
	})
}

func spotInstrument(input normalize.MappingInput, object map[string]json.RawMessage) (normalize.InstrumentIdentity, error) {
	raw, ok := object["s"]
	if !ok {
		return normalize.InstrumentIdentity{}, normalize.RejectProjection(normalize.QuarantineInvalidField, "s", normalize.SourceMissing)
	}
	symbol, state, err := mappedSpotString(raw)
	if err != nil {
		return normalize.InstrumentIdentity{}, normalize.RejectProjection(normalize.QuarantineInvalidField, "s", state)
	}
	instrument, ok := input.Catalog.Lookup(SpotSourceID, symbol)
	if !ok {
		return normalize.InstrumentIdentity{}, normalize.RejectProjection(normalize.QuarantineInstrument, "s", normalize.SourceValue)
	}
	if input.Record.Envelope.NativeSymbol.Valid && input.Record.Envelope.NativeSymbol.Value != symbol {
		return normalize.InstrumentIdentity{}, normalize.RejectProjection(normalize.QuarantineInstrument, "native_symbol", normalize.SourceValue)
	}
	if input.Record.Envelope.InstrumentUID.Valid && input.Record.Envelope.InstrumentUID.Value != instrument.InstrumentUID {
		return normalize.InstrumentIdentity{}, normalize.RejectProjection(normalize.QuarantineInstrument, "instrument_uid", normalize.SourceValue)
	}
	return instrument, nil
}

func spotNumeric(object map[string]json.RawMessage, field string, scale uint8, unit normalize.Unit, allowNegative bool) (normalize.Numeric, error) {
	raw, ok := object[field]
	if !ok {
		return normalize.Numeric{}, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceMissing)
	}
	text, state, err := mappedSpotString(raw)
	if err != nil {
		return normalize.Numeric{}, normalize.RejectProjection(normalize.QuarantineInvalidField, field, state)
	}
	decimal, err := normalize.ParseDecimal(text, scale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.Numeric{}, normalize.RejectProjection(normalize.QuarantineBounds, field, normalize.SourceValue)
	}
	if !allowNegative && strings.HasPrefix(decimal.Coefficient, "-") {
		return normalize.Numeric{}, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceValue)
	}
	return normalize.Numeric{Decimal: decimal, Unit: unit}, nil
}

func mappedSpotLevels(object map[string]json.RawMessage, field string, side normalize.Side, instrument normalize.InstrumentIdentity) ([]normalize.BookLevel, error) {
	raw, ok := object[field]
	if !ok {
		return nil, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceMissing)
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return nil, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceNull)
	}
	var values []json.RawMessage
	if err := json.Unmarshal(raw, &values); err != nil {
		return nil, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceValue)
	}
	if len(values) > normalize.MaxBookLevelsPerSide {
		return nil, normalize.RejectProjection(normalize.QuarantineBounds, field, normalize.SourceValue)
	}
	levels := make([]normalize.BookLevel, len(values))
	for i, rawLevel := range values {
		var tuple []json.RawMessage
		if err := json.Unmarshal(rawLevel, &tuple); err != nil || len(tuple) != 2 {
			return nil, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceValue)
		}
		priceText, state, err := mappedSpotString(tuple[0])
		if err != nil {
			return nil, normalize.RejectProjection(normalize.QuarantineInvalidField, field+".price", state)
		}
		amountText, state, err := mappedSpotString(tuple[1])
		if err != nil {
			return nil, normalize.RejectProjection(normalize.QuarantineInvalidField, field+".amount", state)
		}
		price, err := normalize.ParseDecimal(priceText, normalize.CanonicalPriceScale, normalize.DefaultDecimalBounds())
		if err != nil || strings.HasPrefix(price.Coefficient, "-") {
			return nil, normalize.RejectProjection(normalize.QuarantineBounds, field+".price", normalize.SourceValue)
		}
		amount, err := normalize.ParseDecimal(amountText, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
		if err != nil || strings.HasPrefix(amount.Coefficient, "-") {
			return nil, normalize.RejectProjection(normalize.QuarantineBounds, field+".amount", normalize.SourceValue)
		}
		action := normalize.LevelUpsert
		if amount.IsZero() {
			action = normalize.LevelDelete
		}
		levels[i] = normalize.BookLevel{
			Side: side, LevelOrdinal: uint32(i), Action: action,
			Price:  normalize.Numeric{Decimal: price, Unit: normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)},
			Amount: normalize.Numeric{Decimal: amount, Unit: normalize.BaseAssetUnit(instrument.BaseAssetID)},
		}
	}
	return levels, nil
}

func mappedSpotString(raw json.RawMessage) (string, normalize.SourceState, error) {
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return "", normalize.SourceNull, ErrSpotMap
	}
	var value string
	if err := json.Unmarshal(trimmed, &value); err != nil {
		return "", normalize.SourceValue, ErrSpotMap
	}
	if value == "" {
		return "", normalize.SourceEmpty, ErrSpotMap
	}
	if len(value) > normalize.MaxFingerprintStringBytes {
		return "", normalize.SourceValue, ErrSpotMap
	}
	return value, normalize.SourceValue, nil
}

func spotUint(object map[string]json.RawMessage, field string) (uint64, error) {
	raw, ok := object[field]
	if !ok {
		return 0, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceMissing)
	}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return 0, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceNull)
	}
	if len(trimmed) == 0 || len(trimmed) > normalize.MaxFingerprintNumberBytes || bytes.ContainsAny(trimmed, ".eE+-\"") {
		return 0, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceValue)
	}
	value, err := strconv.ParseUint(string(trimmed), 10, 64)
	if err != nil {
		return 0, normalize.RejectProjection(normalize.QuarantineBounds, field, normalize.SourceValue)
	}
	return value, nil
}

func spotBool(object map[string]json.RawMessage, field string) (bool, error) {
	raw, ok := object[field]
	if !ok {
		return false, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceMissing)
	}
	trimmed := bytes.TrimSpace(raw)
	if bytes.Equal(trimmed, []byte("null")) {
		return false, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceNull)
	}
	if bytes.Equal(trimmed, []byte("true")) {
		return true, nil
	}
	if bytes.Equal(trimmed, []byte("false")) {
		return false, nil
	}
	return false, normalize.RejectProjection(normalize.QuarantineInvalidField, field, normalize.SourceValue)
}

func spotTime(input normalize.MappingInput, object map[string]json.RawMessage, field string) (int64, normalize.TimeResolution, error) {
	value, err := spotUint(object, field)
	if err != nil {
		return 0, normalize.ResolutionAbsent, err
	}
	resolution, multiplier, _, err := spotTimeContract(input.Binding.SourceTimeResolution)
	if err != nil {
		return 0, normalize.ResolutionAbsent, err
	}
	converted, err := scaleSpotTime(input, value, multiplier, field)
	if err != nil {
		return 0, normalize.ResolutionAbsent, err
	}
	return converted, resolution, nil
}

func spotTimeContract(resolution normalize.TimeResolution) (normalize.TimeResolution, uint64, uint64, error) {
	switch resolution {
	case normalize.ResolutionMillisecond:
		return resolution, 1_000_000, 86_400_000, nil
	case normalize.ResolutionMicrosecond:
		return resolution, 1_000, 86_400_000_000, nil
	default:
		return normalize.ResolutionAbsent, 0, 0, normalize.RejectProjection(normalize.QuarantineInvalidField, "source_time_resolution", normalize.SourceValue)
	}
}

func scaleSpotTime(input normalize.MappingInput, value, multiplier uint64, field string) (int64, error) {
	if value > uint64(^uint64(0)>>1)/multiplier {
		return 0, normalize.RejectProjection(normalize.QuarantineBounds, field, normalize.SourceValue)
	}
	converted := int64(value * multiplier)
	if field == "E" && input.Record.Envelope.ExchangeTimeNS.Valid {
		captured := input.Record.Envelope.ExchangeTimeResolution
		matchesBinding := (captured == capture.ExchangeTimeMillisecond && input.Binding.SourceTimeResolution == normalize.ResolutionMillisecond) ||
			(captured == capture.ExchangeTimeMicrosecond && input.Binding.SourceTimeResolution == normalize.ResolutionMicrosecond)
		if !matchesBinding || input.Record.Envelope.ExchangeTimeNS.Value != converted {
			return 0, normalize.RejectProjection(normalize.QuarantineInvalidField, "exchange_time_ns", normalize.SourceValue)
		}
	}
	return converted, nil
}

func decodeSpotObject(payload []byte) (map[string]json.RawMessage, error) {
	if len(payload) == 0 || len(payload) > SpotMaxRawPayloadBytes {
		return nil, normalize.RejectProjection(normalize.QuarantineBounds, "payload", normalize.SourceValue)
	}
	if err := validateNoDuplicateJSONKeys(payload); err != nil {
		return nil, normalize.RejectProjection(normalize.QuarantineSchemaMalformed, "payload", normalize.SourceValue)
	}
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil || object == nil || len(object) > normalize.MaxFingerprintFields {
		return nil, normalize.RejectProjection(normalize.QuarantineSchemaMalformed, "payload", normalize.SourceValue)
	}
	return object, nil
}

func validateNoDuplicateJSONKeys(payload []byte) error {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := consumeJSONValue(decoder, 1); err != nil {
		return err
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return ErrSpotMap
	}
	return nil
}

func consumeJSONValue(decoder *json.Decoder, depth int) error {
	if depth > normalize.MaxFingerprintDepth {
		return ErrSpotMap
	}
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	delim, composite := token.(json.Delim)
	if !composite {
		return nil
	}
	switch delim {
	case '{':
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return err
			}
			key, ok := keyToken.(string)
			if !ok || len(key) > normalize.MaxFingerprintStringBytes {
				return ErrSpotMap
			}
			if _, duplicate := seen[key]; duplicate {
				return ErrSpotMap
			}
			seen[key] = struct{}{}
			if len(seen) > normalize.MaxFingerprintFields {
				return ErrSpotMap
			}
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim('}') {
			return ErrSpotMap
		}
	case '[':
		items := 0
		for decoder.More() {
			items++
			if items > normalize.MaxFingerprintArrayItems {
				return ErrSpotMap
			}
			if err := consumeJSONValue(decoder, depth+1); err != nil {
				return err
			}
		}
		end, err := decoder.Token()
		if err != nil || end != json.Delim(']') {
			return ErrSpotMap
		}
	default:
		return ErrSpotMap
	}
	return nil
}

var _ normalize.Mapper = (*SpotMapper)(nil)
