package binance

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

type CoinMMergedRoute string

const (
	CoinMMergedRouteUSDM     CoinMMergedRoute = "usdm"
	CoinMMergedRouteCoinM    CoinMMergedRoute = "coinm"
	CoinMMergedRouteRejected CoinMMergedRoute = "rejected"
)

type CoinMMergedRecord struct {
	Index            int
	Stream           string
	Raw              json.RawMessage
	Symbol           string
	Pair             string
	NativeSymbolType uint8
	Route            CoinMMergedRoute
	SourceID         string
	Rejection        string
}

// RouteCoinMMergedRecords routes the merged all-market universe exclusively by
// the native st field: 1 is USD-M and 2 is COIN-M. A record-level inconsistency
// is retained byte-for-byte with a rejected decision; it is never repaired.
func RouteCoinMMergedRecords(raw []byte) ([]CoinMMergedRecord, error) {
	var wrapper struct {
		Stream string          `json:"stream"`
		Data   json.RawMessage `json:"data"`
	}
	if err := coinMUnmarshalBoundedStrict(raw, &wrapper); err != nil {
		return nil, err
	}
	contract, err := CoinMStreamContractFor(wrapper.Stream)
	if err != nil || !contract.MergedUniverse {
		return nil, fmt.Errorf("%w: payload is not a declared merged stream", ErrCoinMInvalidRoute)
	}
	var records []json.RawMessage
	if err := json.Unmarshal(wrapper.Data, &records); err != nil || len(records) == 0 || len(records) > CoinMMaxMergedRecords {
		return nil, fmt.Errorf("%w: merged data must be a bounded non-empty array", ErrCoinMInvalidRoute)
	}
	decisions := make([]CoinMMergedRecord, len(records))
	for i, record := range records {
		decision := CoinMMergedRecord{Index: i, Stream: wrapper.Stream, Raw: slices.Clone(record), Route: CoinMMergedRouteRejected}
		var identity struct {
			EventType  string `json:"e"`
			Symbol     string `json:"s"`
			Pair       string `json:"ps"`
			SymbolType uint8  `json:"st"`
		}
		object, err := coinMDecodeObject(record, 512)
		if err != nil ||
			!coinMDecodeExactField(object, "e", &identity.EventType) ||
			!coinMDecodeExactField(object, "s", &identity.Symbol) ||
			!coinMDecodeExactField(object, "ps", &identity.Pair) ||
			!coinMDecodeExactField(object, "st", &identity.SymbolType) {
			decision.Rejection = "malformed_record"
			decisions[i] = decision
			continue
		}
		decision.Symbol = identity.Symbol
		decision.Pair = identity.Pair
		decision.NativeSymbolType = identity.SymbolType
		if identity.Symbol == "" || identity.Pair == "" {
			decision.Rejection = "missing_symbol_identity"
			decisions[i] = decision
			continue
		}
		if identity.EventType != coinMExpectedMergedEventType(wrapper.Stream) {
			decision.Rejection = "stream_event_type_inconsistency"
			decisions[i] = decision
			continue
		}
		switch identity.SymbolType {
		case 1:
			if coinMSymbolShape(identity.Symbol, identity.Pair) {
				decision.Rejection = "st_symbol_family_inconsistency"
				break
			}
			decision.Route = CoinMMergedRouteUSDM
			decision.SourceID = USDMSourceID
		case 2:
			if !coinMSymbolShape(identity.Symbol, identity.Pair) {
				decision.Rejection = "st_symbol_family_inconsistency"
				break
			}
			decision.Route = CoinMMergedRouteCoinM
			decision.SourceID = CoinMSourceID
		default:
			decision.Rejection = "unknown_native_symbol_type"
		}
		decisions[i] = decision
	}
	return decisions, nil
}
func coinMExpectedMergedEventType(stream string) string {
	switch stream {
	case "!ticker@arr":
		return "24hrTicker"
	case "!miniTicker@arr":
		return "24hrMiniTicker"
	case "!bookTicker":
		return "bookTicker"
	case "!forceOrder@arr":
		return "forceOrder"
	case "!contractInfo":
		return "contractInfo"
	case "!markPrice@arr", "!markPrice@arr@1s":
		return "markPriceUpdate"
	default:
		return ""
	}
}

func coinMDecodeExactField(object map[string]json.RawMessage, name string, destination any) bool {
	raw, ok := object[name]
	return ok && !bytes.Equal(bytes.TrimSpace(raw), []byte("null")) && json.Unmarshal(raw, destination) == nil
}

func coinMSymbolShape(symbol, pair string) bool {
	return strings.HasSuffix(pair, "USD") && strings.HasPrefix(symbol, pair+"_") && len(symbol) > len(pair)+1
}

type CoinMAggregateTrade struct {
	SourceID         string
	NativeSymbolType uint8
	EventType        string
	EventTimeMS      int64
	Symbol           string
	Pair             string
	AggregateID      uint64
	PriceText        string
	ContractsText    string
	FirstTradeID     uint64
	LastTradeID      uint64
	TradeTimeMS      int64
	BuyerIsMaker     bool
	Price            normalize.Numeric
	Contracts        normalize.NativeValue
}

func ParseCoinMAggregateTrade(raw []byte, instrument normalize.InstrumentIdentity) (CoinMAggregateTrade, error) {
	var wire struct {
		EventType    string `json:"e"`
		EventTimeMS  int64  `json:"E"`
		Symbol       string `json:"s"`
		Pair         string `json:"ps"`
		SymbolType   uint8  `json:"st"`
		AggregateID  uint64 `json:"a"`
		Price        string `json:"p"`
		Contracts    string `json:"q"`
		FirstTradeID uint64 `json:"f"`
		LastTradeID  uint64 `json:"l"`
		TradeTimeMS  int64  `json:"T"`
		BuyerIsMaker bool   `json:"m"`
	}
	object, err := coinMDecodeObject(raw, 64)
	if err != nil ||
		!coinMDecodeExactField(object, "e", &wire.EventType) ||
		!coinMDecodeExactField(object, "E", &wire.EventTimeMS) ||
		!coinMDecodeExactField(object, "s", &wire.Symbol) ||
		!coinMDecodeExactField(object, "ps", &wire.Pair) ||
		!coinMDecodeExactField(object, "st", &wire.SymbolType) ||
		!coinMDecodeExactField(object, "a", &wire.AggregateID) ||
		!coinMDecodeExactField(object, "p", &wire.Price) ||
		!coinMDecodeExactField(object, "q", &wire.Contracts) ||
		!coinMDecodeExactField(object, "f", &wire.FirstTradeID) ||
		!coinMDecodeExactField(object, "l", &wire.LastTradeID) ||
		!coinMDecodeExactField(object, "T", &wire.TradeTimeMS) ||
		!coinMDecodeExactField(object, "m", &wire.BuyerIsMaker) {
		return CoinMAggregateTrade{}, fmt.Errorf("%w: missing or mistyped aggregate-trade field", ErrCoinMInvalidMarketPayload)
	}
	if wire.EventType != "aggTrade" || wire.SymbolType != 2 || wire.Symbol != instrument.NativeID || !coinMSymbolShape(wire.Symbol, wire.Pair) ||
		wire.AggregateID == 0 || wire.FirstTradeID == 0 || wire.LastTradeID < wire.FirstTradeID || wire.EventTimeMS < 0 || wire.TradeTimeMS < 0 {
		return CoinMAggregateTrade{}, fmt.Errorf("%w: malformed aggregate-trade identity", ErrCoinMInvalidMarketPayload)
	}
	price, err := parseCoinMPositiveDecimal(wire.Price, normalize.CanonicalPriceScale, false)
	if err != nil {
		return CoinMAggregateTrade{}, err
	}
	contracts, err := parseCoinMPositiveDecimal(wire.Contracts, normalize.CanonicalAmountScale, false)
	if err != nil {
		return CoinMAggregateTrade{}, err
	}
	return CoinMAggregateTrade{
		SourceID:         CoinMSourceID,
		NativeSymbolType: wire.SymbolType,
		EventType:        wire.EventType,
		EventTimeMS:      wire.EventTimeMS,
		Symbol:           wire.Symbol,
		Pair:             wire.Pair,
		AggregateID:      wire.AggregateID,
		PriceText:        wire.Price,
		ContractsText:    wire.Contracts,
		FirstTradeID:     wire.FirstTradeID,
		LastTradeID:      wire.LastTradeID,
		TradeTimeMS:      wire.TradeTimeMS,
		BuyerIsMaker:     wire.BuyerIsMaker,
		Price:            normalize.Numeric{Decimal: price, Unit: normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)},
		Contracts:        normalize.NativeValue{Decimal: contracts, Unit: normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: instrument.InstrumentUID}},
	}, nil
}

func (e CoinMAggregateTrade) AggressorSide() normalize.Side {
	if e.BuyerIsMaker {
		return normalize.SideSell
	}
	return normalize.SideBuy
}

func (e CoinMAggregateTrade) ContractConversion(receivedTimeNS int64, terms normalize.CoinMContractTerms) (normalize.CoinMContractConversion, error) {
	if e.Contracts.Unit.Kind != normalize.NativeUnitContracts ||
		terms.InstrumentUID != e.Contracts.Unit.InstrumentUID ||
		e.Price.Unit.Kind != normalize.UnitQuotePerBase ||
		terms.BaseAssetID != e.Price.Unit.BaseAssetID ||
		terms.QuoteAssetID != e.Price.Unit.QuoteAssetID ||
		terms.Payoff != normalize.CoinMPayoffInverseQuote {
		return normalize.CoinMContractConversion{}, fmt.Errorf("%w: aggregate trade and temporal contract terms differ", normalize.ErrInvalidCoinMContract)
	}
	provenance, err := coinMFieldProvenance(e.TradeTimeMS, receivedTimeNS)
	if err != nil {
		return normalize.CoinMContractConversion{}, err
	}
	return normalize.ConvertCoinMContracts(e.Contracts.Decimal, e.Price.Decimal, receivedTimeNS, provenance, terms)
}

type CoinMBookTicker struct {
	SourceID          string
	NativeSymbolType  uint8
	EventType         string
	UpdateID          uint64
	EventTimeMS       int64
	TransactionTimeMS int64
	Symbol            string
	Pair              string
	BidPrice          normalize.Numeric
	BidContracts      normalize.NativeValue
	AskPrice          normalize.Numeric
	AskContracts      normalize.NativeValue
}

func ParseCoinMBookTicker(raw []byte, instrument normalize.InstrumentIdentity) (CoinMBookTicker, error) {
	var wire struct {
		EventType         string `json:"e"`
		UpdateID          uint64 `json:"u"`
		EventTimeMS       int64  `json:"E"`
		TransactionTimeMS int64  `json:"T"`
		Symbol            string `json:"s"`
		Pair              string `json:"ps"`
		SymbolType        uint8  `json:"st"`
		BidPrice          string `json:"b"`
		BidContracts      string `json:"B"`
		AskPrice          string `json:"a"`
		AskContracts      string `json:"A"`
	}
	if err := coinMUnmarshalBoundedStrict(raw, &wire); err != nil {
		return CoinMBookTicker{}, err
	}
	if wire.EventType != "bookTicker" || wire.SymbolType != 2 || wire.UpdateID == 0 || wire.EventTimeMS < 0 || wire.TransactionTimeMS < 0 || wire.Symbol != instrument.NativeID || !coinMSymbolShape(wire.Symbol, wire.Pair) {
		return CoinMBookTicker{}, fmt.Errorf("%w: malformed bookTicker identity", ErrCoinMInvalidMarketPayload)
	}
	bidPrice, err := parseCoinMPositiveDecimal(wire.BidPrice, normalize.CanonicalPriceScale, false)
	if err != nil {
		return CoinMBookTicker{}, err
	}
	askPrice, err := parseCoinMPositiveDecimal(wire.AskPrice, normalize.CanonicalPriceScale, false)
	if err != nil {
		return CoinMBookTicker{}, err
	}
	bidContracts, err := parseCoinMPositiveDecimal(wire.BidContracts, normalize.CanonicalAmountScale, true)
	if err != nil {
		return CoinMBookTicker{}, err
	}
	askContracts, err := parseCoinMPositiveDecimal(wire.AskContracts, normalize.CanonicalAmountScale, true)
	if err != nil {
		return CoinMBookTicker{}, err
	}
	return CoinMBookTicker{
		SourceID:          CoinMSourceID,
		NativeSymbolType:  wire.SymbolType,
		EventType:         wire.EventType,
		UpdateID:          wire.UpdateID,
		EventTimeMS:       wire.EventTimeMS,
		TransactionTimeMS: wire.TransactionTimeMS,
		Symbol:            wire.Symbol,
		Pair:              wire.Pair,
		BidPrice:          normalize.Numeric{Decimal: bidPrice, Unit: normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)},
		BidContracts:      normalize.NativeValue{Decimal: bidContracts, Unit: normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: instrument.InstrumentUID}},
		AskPrice:          normalize.Numeric{Decimal: askPrice, Unit: normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)},
		AskContracts:      normalize.NativeValue{Decimal: askContracts, Unit: normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: instrument.InstrumentUID}},
	}, nil
}

type CoinMTicker24h struct {
	SourceID          string
	NativeSymbolType  uint8
	EventType         string
	EventTimeMS       int64
	Symbol            string
	Pair              string
	LastPrice         normalize.Numeric
	LastContracts     normalize.NativeValue
	ContractVolume    normalize.NativeValue
	BaseAssetVolume   normalize.NativeValue
	WindowOpenTimeMS  int64
	WindowCloseTimeMS int64
	FirstTradeID      uint64
	LastTradeID       uint64
	TradeCount        uint64
}

func ParseCoinMTicker24h(raw []byte, instrument normalize.InstrumentIdentity) (CoinMTicker24h, error) {
	object, err := coinMDecodeObject(raw, 64)
	if err != nil {
		return CoinMTicker24h{}, err
	}
	var eventType, symbol, pair, lastPriceText, lastContractsText, contractVolumeText, baseVolumeText string
	var symbolType uint8
	var eventTimeMS, openTimeMS, closeTimeMS int64
	var firstTradeID, lastTradeID, tradeCount uint64
	for key, destination := range map[string]any{
		"e": &eventType, "E": &eventTimeMS, "s": &symbol, "ps": &pair, "st": &symbolType,
		"c": &lastPriceText, "Q": &lastContractsText, "v": &contractVolumeText, "q": &baseVolumeText,
		"O": &openTimeMS, "C": &closeTimeMS, "F": &firstTradeID, "L": &lastTradeID, "n": &tradeCount,
	} {
		if err := requireCoinMJSON(object, key, destination); err != nil {
			return CoinMTicker24h{}, err
		}
	}
	if eventType != "24hrTicker" || symbolType != 2 || symbol != instrument.NativeID || !coinMSymbolShape(symbol, pair) || eventTimeMS < 0 || openTimeMS < 0 || closeTimeMS < openTimeMS || firstTradeID == 0 || lastTradeID < firstTradeID || tradeCount == 0 {
		return CoinMTicker24h{}, fmt.Errorf("%w: malformed 24hrTicker identity", ErrCoinMInvalidMarketPayload)
	}
	lastPrice, err := parseCoinMPositiveDecimal(lastPriceText, normalize.CanonicalPriceScale, false)
	if err != nil {
		return CoinMTicker24h{}, err
	}
	lastContracts, err := parseCoinMPositiveDecimal(lastContractsText, normalize.CanonicalAmountScale, false)
	if err != nil {
		return CoinMTicker24h{}, err
	}
	contractVolume, err := parseCoinMPositiveDecimal(contractVolumeText, normalize.CanonicalAmountScale, true)
	if err != nil {
		return CoinMTicker24h{}, err
	}
	baseVolume, err := parseCoinMPositiveDecimal(baseVolumeText, normalize.CanonicalAmountScale, true)
	if err != nil {
		return CoinMTicker24h{}, err
	}
	contractUnit := normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: instrument.InstrumentUID}
	return CoinMTicker24h{
		SourceID:          CoinMSourceID,
		NativeSymbolType:  symbolType,
		EventType:         eventType,
		EventTimeMS:       eventTimeMS,
		Symbol:            symbol,
		Pair:              pair,
		LastPrice:         normalize.Numeric{Decimal: lastPrice, Unit: normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)},
		LastContracts:     normalize.NativeValue{Decimal: lastContracts, Unit: contractUnit},
		ContractVolume:    normalize.NativeValue{Decimal: contractVolume, Unit: contractUnit},
		BaseAssetVolume:   normalize.NativeValue{Decimal: baseVolume, Unit: normalize.NativeUnit{Kind: normalize.NativeUnitBaseAsset, AssetID: instrument.BaseAssetID}},
		WindowOpenTimeMS:  openTimeMS,
		WindowCloseTimeMS: closeTimeMS,
		FirstTradeID:      firstTradeID,
		LastTradeID:       lastTradeID,
		TradeCount:        tradeCount,
	}, nil
}

type CoinMDerivativeTicker struct {
	SourceID         string
	NativeSymbolType uint8
	Symbol           string
	Pair             string
	EventTimeMS      int64
	NativeSourceRole string
	MarkPrice        normalize.NumericField
	IndexPrice       normalize.NumericField
	FundingRate      normalize.NumericField
	NextFundingTime  normalize.TimeField
	SettlementPrice  normalize.NumericField
}

func ParseCoinMDerivativeTicker(raw []byte, receivedTimeNS int64, instrument normalize.InstrumentIdentity) (CoinMDerivativeTicker, error) {
	object, err := coinMDecodeObject(raw, 32)
	if err != nil {
		return CoinMDerivativeTicker{}, err
	}
	var eventType, symbol, pair string
	var symbolType uint8
	var eventTimeMS int64
	for key, destination := range map[string]any{"e": &eventType, "E": &eventTimeMS, "s": &symbol, "ps": &pair, "st": &symbolType} {
		if err := requireCoinMJSON(object, key, destination); err != nil {
			return CoinMDerivativeTicker{}, err
		}
	}
	if eventType != "markPriceUpdate" || symbolType != 2 || symbol != instrument.NativeID || !coinMSymbolShape(symbol, pair) || eventTimeMS < 0 {
		return CoinMDerivativeTicker{}, fmt.Errorf("%w: malformed mark-price identity", ErrCoinMInvalidMarketPayload)
	}
	provenance, err := coinMFieldProvenance(eventTimeMS, receivedTimeNS)
	if err != nil {
		return CoinMDerivativeTicker{}, err
	}
	priceUnit := normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)
	mark, err := parseCoinMNumericField(object, "p", normalize.CanonicalPriceScale, priceUnit, provenance)
	if err != nil {
		return CoinMDerivativeTicker{}, err
	}
	index, err := parseCoinMNumericField(object, "i", normalize.CanonicalPriceScale, priceUnit, provenance)
	if err != nil {
		return CoinMDerivativeTicker{}, err
	}
	funding, err := parseCoinMNumericField(object, "r", normalize.CanonicalAmountScale, normalize.RateUnit(), provenance)
	if err != nil {
		return CoinMDerivativeTicker{}, err
	}
	settlement, err := parseCoinMNumericField(object, "P", normalize.CanonicalPriceScale, priceUnit, provenance)
	if err != nil {
		return CoinMDerivativeTicker{}, err
	}
	nextFunding, err := parseCoinMTimeField(object, "T", provenance)
	if err != nil {
		return CoinMDerivativeTicker{}, err
	}
	return CoinMDerivativeTicker{
		SourceID:         CoinMSourceID,
		NativeSymbolType: symbolType,
		Symbol:           symbol,
		Pair:             pair,
		EventTimeMS:      eventTimeMS,
		NativeSourceRole: string(CoinMRoleMarkIndexFunding),
		MarkPrice:        mark,
		IndexPrice:       index,
		FundingRate:      funding,
		NextFundingTime:  nextFunding,
		SettlementPrice:  settlement,
	}, nil
}

func (e CoinMDerivativeTicker) Normalized(metadata normalize.Metadata) (normalize.DerivativeTickerV1, error) {
	event := normalize.DerivativeTickerV1{
		Metadata:         metadata,
		NativeSourceRole: e.NativeSourceRole,
		LastPrice:        missingCoinMNumericField(),
		MarkPrice:        e.MarkPrice,
		IndexPrice:       e.IndexPrice,
		FundingRate:      e.FundingRate,
		NextFundingTime:  e.NextFundingTime,
		SettlementPrice:  e.SettlementPrice,
		Basis:            missingCoinMNumericField(),
		Premium:          missingCoinMNumericField(),
	}
	if err := event.Validate(); err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	return event, nil
}

type coinMStringField struct {
	State normalize.SourceState
	Text  string
}

func parseCoinMNumericField(object map[string]json.RawMessage, key string, scale uint8, unit normalize.Unit, provenance normalize.FieldProvenance) (normalize.NumericField, error) {
	field, err := parseCoinMStringField(object, key)
	if err != nil {
		return normalize.NumericField{}, err
	}
	result := missingCoinMNumericField()
	result.State = field.State
	if field.State != normalize.SourceMissing {
		result.Provenance = provenance
	}
	if field.State != normalize.SourceValue {
		return result, nil
	}
	decimal, err := normalize.ParseDecimal(field.Text, scale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.NumericField{}, fmt.Errorf("%w: field %s decimal", ErrCoinMInvalidMarketPayload, key)
	}
	result.Value = normalize.Numeric{Decimal: decimal, Unit: unit}
	return result, nil
}

func parseCoinMTimeField(object map[string]json.RawMessage, key string, provenance normalize.FieldProvenance) (normalize.TimeField, error) {
	raw, ok := object[key]
	if !ok {
		return missingCoinMTimeField(), nil
	}
	result := normalize.TimeField{Provenance: provenance}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		result.State = normalize.SourceNull
		return result, nil
	}
	if len(raw) > 0 && raw[0] == '"' {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return normalize.TimeField{}, fmt.Errorf("%w: time field %s", ErrCoinMInvalidMarketPayload, key)
		}
		if text == "" {
			result.State = normalize.SourceEmpty
			return result, nil
		}
		return normalize.TimeField{}, fmt.Errorf("%w: time field %s must be integer milliseconds", ErrCoinMInvalidMarketPayload, key)
	}
	var valueMS int64
	if json.Unmarshal(raw, &valueMS) != nil || valueMS < 0 || valueMS > (1<<63-1)/1_000_000 {
		return normalize.TimeField{}, fmt.Errorf("%w: invalid time field %s", ErrCoinMInvalidMarketPayload, key)
	}
	result.State = normalize.SourceValue
	result.ValueNS = valueMS * 1_000_000
	result.Resolution = normalize.ResolutionMillisecond
	return result, nil
}

func parseCoinMStringField(object map[string]json.RawMessage, key string) (coinMStringField, error) {
	raw, ok := object[key]
	if !ok {
		return coinMStringField{State: normalize.SourceMissing}, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return coinMStringField{State: normalize.SourceNull}, nil
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return coinMStringField{}, fmt.Errorf("%w: field %s must be a string", ErrCoinMInvalidMarketPayload, key)
	}
	if text == "" {
		return coinMStringField{State: normalize.SourceEmpty}, nil
	}
	return coinMStringField{State: normalize.SourceValue, Text: text}, nil
}

func coinMFieldProvenance(sourceTimeMS, receivedTimeNS int64) (normalize.FieldProvenance, error) {
	if sourceTimeMS < 0 || sourceTimeMS > (1<<63-1)/1_000_000 || receivedTimeNS < sourceTimeMS*1_000_000 {
		return normalize.FieldProvenance{}, fmt.Errorf("%w: source time is after receive time", ErrCoinMInvalidMarketPayload)
	}
	sourceTimeNS := sourceTimeMS * 1_000_000
	return normalize.FieldProvenance{
		SourceTimeNS:         normalize.OptionalInt64{Valid: true, Value: sourceTimeNS},
		SourceTimeResolution: normalize.ResolutionMillisecond,
		AgeNS:                normalize.OptionalUint64{Valid: true, Value: uint64(receivedTimeNS - sourceTimeNS)},
	}, nil
}

func missingCoinMNumericField() normalize.NumericField {
	return normalize.NumericField{State: normalize.SourceMissing, Provenance: normalize.FieldProvenance{SourceTimeResolution: normalize.ResolutionAbsent}}
}

func missingCoinMTimeField() normalize.TimeField {
	return normalize.TimeField{State: normalize.SourceMissing, Resolution: normalize.ResolutionAbsent, Provenance: normalize.FieldProvenance{SourceTimeResolution: normalize.ResolutionAbsent}}
}

func parseCoinMPositiveDecimal(text string, scale uint8, allowZero bool) (normalize.Decimal, error) {
	decimal, err := normalize.ParseDecimal(text, scale, normalize.DefaultDecimalBounds())
	if err != nil || strings.HasPrefix(decimal.Coefficient, "-") || (!allowZero && decimal.IsZero()) {
		return normalize.Decimal{}, fmt.Errorf("%w: invalid positive decimal", ErrCoinMInvalidMarketPayload)
	}
	return decimal, nil
}

func requireCoinMJSON(object map[string]json.RawMessage, key string, destination any) error {
	raw, ok := object[key]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, destination) != nil {
		return fmt.Errorf("%w: required field %s", ErrCoinMInvalidMarketPayload, key)
	}
	return nil
}

func coinMDecodeObject(raw []byte, maxFields int) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := coinMUnmarshalBounded(raw, &object); err != nil {
		return nil, err
	}
	if len(object) == 0 || len(object) > maxFields {
		return nil, fmt.Errorf("%w: object field bound", ErrCoinMInvalidMarketPayload)
	}
	return object, nil
}

func coinMUnmarshalBounded(raw []byte, destination any) error {
	if len(raw) == 0 || len(raw) > CoinMMaxRawPayloadBytes || !json.Valid(raw) {
		return ErrCoinMInvalidMarketPayload
	}
	if err := json.Unmarshal(raw, destination); err != nil {
		return fmt.Errorf("%w: %v", ErrCoinMInvalidMarketPayload, err)
	}
	return nil
}

func coinMUnmarshalBoundedStrict(raw []byte, destination any) error {
	if len(raw) == 0 || len(raw) > CoinMMaxRawPayloadBytes {
		return ErrCoinMInvalidMarketPayload
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("%w: %v", ErrCoinMInvalidMarketPayload, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON value", ErrCoinMInvalidMarketPayload)
	}
	return nil
}
