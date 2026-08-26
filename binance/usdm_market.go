package binance

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

var ErrUSDMInvalidMarketPayload = errors.New("binance: invalid USD-M market payload")

type USDMStringField struct {
	State normalize.SourceState
	Text  string
}

type USDMAggregateTrade struct {
	EventType                   string
	EventTimeMS                 int64
	Symbol                      string
	AggregateTradeID            uint64
	Price                       string
	Quantity                    string
	NormalQuantityExcludingRPI  USDMStringField
	FirstTradeID                uint64
	LastTradeID                 uint64
	TradeTimeMS                 int64
	BuyerIsMaker                bool
	NativeSourceRole            USDMStreamRole
	AggregationCadenceCeilingNS uint64
}

func ParseUSDMAggregateTrade(raw []byte) (USDMAggregateTrade, error) {
	var object map[string]json.RawMessage
	if err := unmarshalUSDMBounded(raw, &object); err != nil {
		return USDMAggregateTrade{}, err
	}
	var event USDMAggregateTrade
	if err := requireUSDMJSON(object, "e", &event.EventType); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := requireUSDMJSON(object, "E", &event.EventTimeMS); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := requireUSDMJSON(object, "s", &event.Symbol); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := requireUSDMJSON(object, "a", &event.AggregateTradeID); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := requireUSDMJSON(object, "p", &event.Price); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := requireUSDMJSON(object, "q", &event.Quantity); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := requireUSDMJSON(object, "f", &event.FirstTradeID); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := requireUSDMJSON(object, "l", &event.LastTradeID); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := requireUSDMJSON(object, "T", &event.TradeTimeMS); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := requireUSDMJSON(object, "m", &event.BuyerIsMaker); err != nil {
		return USDMAggregateTrade{}, err
	}
	field, err := parseUSDMStringField(object, "nq")
	if err != nil {
		return USDMAggregateTrade{}, err
	}
	event.NormalQuantityExcludingRPI = field
	event.NativeSourceRole = USDMRoleAggregateTrade
	event.AggregationCadenceCeilingNS = USDMAggregateTradeCeilingNS
	if event.EventType != "aggTrade" || event.Symbol == "" || event.AggregateTradeID == 0 || event.FirstTradeID > event.LastTradeID || event.EventTimeMS < 0 || event.TradeTimeMS < 0 {
		return USDMAggregateTrade{}, fmt.Errorf("%w: malformed aggregate-trade identity", ErrUSDMInvalidMarketPayload)
	}
	if err := validateUSDMDecimalText(event.Price, normalize.CanonicalPriceScale, false); err != nil {
		return USDMAggregateTrade{}, err
	}
	if err := validateUSDMDecimalText(event.Quantity, normalize.CanonicalAmountScale, true); err != nil {
		return USDMAggregateTrade{}, err
	}
	if field.State == normalize.SourceValue {
		if err := validateUSDMDecimalText(field.Text, normalize.CanonicalAmountScale, true); err != nil {
			return USDMAggregateTrade{}, err
		}
	}
	return event, nil
}

func (e USDMAggregateTrade) AggressorSide() normalize.Side {
	if e.BuyerIsMaker {
		return normalize.SideSell
	}
	return normalize.SideBuy
}

type USDMBookTicker struct {
	EventType         string
	UpdateID          uint64
	EventTimeMS       int64
	TransactionTimeMS int64
	Symbol            string
	BidPrice          string
	BidQuantity       string
	AskPrice          string
	AskQuantity       string
	NativeSourceRole  USDMStreamRole
	RPIInclusion      string
}

func ParseUSDMBookTicker(raw []byte) (USDMBookTicker, error) {
	var event USDMBookTicker
	var wire struct {
		EventType         string `json:"e"`
		UpdateID          uint64 `json:"u"`
		EventTimeMS       int64  `json:"E"`
		TransactionTimeMS int64  `json:"T"`
		Symbol            string `json:"s"`
		BidPrice          string `json:"b"`
		BidQuantity       string `json:"B"`
		AskPrice          string `json:"a"`
		AskQuantity       string `json:"A"`
	}
	if err := unmarshalUSDMBounded(raw, &wire); err != nil {
		return event, err
	}
	event = USDMBookTicker{EventType: wire.EventType, UpdateID: wire.UpdateID, EventTimeMS: wire.EventTimeMS, TransactionTimeMS: wire.TransactionTimeMS, Symbol: wire.Symbol, BidPrice: wire.BidPrice, BidQuantity: wire.BidQuantity, AskPrice: wire.AskPrice, AskQuantity: wire.AskQuantity, NativeSourceRole: USDMRoleNativeBBO, RPIInclusion: "excluded"}
	if event.EventType != "bookTicker" || event.UpdateID == 0 || event.Symbol == "" || event.EventTimeMS < 0 || event.TransactionTimeMS < 0 {
		return USDMBookTicker{}, fmt.Errorf("%w: malformed BBO identity", ErrUSDMInvalidMarketPayload)
	}
	for _, value := range []struct {
		text  string
		scale uint8
	}{
		{event.BidPrice, normalize.CanonicalPriceScale}, {event.BidQuantity, normalize.CanonicalAmountScale},
		{event.AskPrice, normalize.CanonicalPriceScale}, {event.AskQuantity, normalize.CanonicalAmountScale},
	} {
		if err := validateUSDMDecimalText(value.text, value.scale, true); err != nil {
			return USDMBookTicker{}, err
		}
	}
	return event, nil
}

type USDMTicker24h struct {
	EventType            string
	EventTimeMS          int64
	Symbol               string
	PriceChange          string
	PriceChangePercent   string
	WeightedAveragePrice string
	LastPrice            string
	LastQuantity         string
	OpenPrice            string
	HighPrice            string
	LowPrice             string
	BaseVolume           string
	QuoteVolume          string
	WindowOpenTimeMS     int64
	WindowCloseTimeMS    int64
	FirstTradeID         uint64
	LastTradeID          uint64
	TradeCount           uint64
	NativeSourceRole     USDMStreamRole
}

func ParseUSDMTicker24h(raw []byte) (USDMTicker24h, error) {
	var wire struct {
		EventType            string `json:"e"`
		EventTimeMS          int64  `json:"E"`
		Symbol               string `json:"s"`
		PriceChange          string `json:"p"`
		PriceChangePercent   string `json:"P"`
		WeightedAveragePrice string `json:"w"`
		LastPrice            string `json:"c"`
		LastQuantity         string `json:"Q"`
		OpenPrice            string `json:"o"`
		HighPrice            string `json:"h"`
		LowPrice             string `json:"l"`
		BaseVolume           string `json:"v"`
		QuoteVolume          string `json:"q"`
		WindowOpenTimeMS     int64  `json:"O"`
		WindowCloseTimeMS    int64  `json:"C"`
		FirstTradeID         uint64 `json:"F"`
		LastTradeID          uint64 `json:"L"`
		TradeCount           uint64 `json:"n"`
	}
	if err := unmarshalUSDMBounded(raw, &wire); err != nil {
		return USDMTicker24h{}, err
	}
	event := USDMTicker24h{EventType: wire.EventType, EventTimeMS: wire.EventTimeMS, Symbol: wire.Symbol, PriceChange: wire.PriceChange, PriceChangePercent: wire.PriceChangePercent, WeightedAveragePrice: wire.WeightedAveragePrice, LastPrice: wire.LastPrice, LastQuantity: wire.LastQuantity, OpenPrice: wire.OpenPrice, HighPrice: wire.HighPrice, LowPrice: wire.LowPrice, BaseVolume: wire.BaseVolume, QuoteVolume: wire.QuoteVolume, WindowOpenTimeMS: wire.WindowOpenTimeMS, WindowCloseTimeMS: wire.WindowCloseTimeMS, FirstTradeID: wire.FirstTradeID, LastTradeID: wire.LastTradeID, TradeCount: wire.TradeCount, NativeSourceRole: USDMRoleGenericTicker}
	if event.EventType != "24hrTicker" || event.Symbol == "" || event.EventTimeMS < 0 || event.WindowOpenTimeMS < 0 || event.WindowCloseTimeMS < event.WindowOpenTimeMS || event.FirstTradeID > event.LastTradeID {
		return USDMTicker24h{}, fmt.Errorf("%w: malformed generic ticker", ErrUSDMInvalidMarketPayload)
	}
	for _, text := range []string{event.PriceChange, event.PriceChangePercent} {
		if err := validateUSDMSignedDecimalText(text, normalize.CanonicalPriceScale); err != nil {
			return USDMTicker24h{}, err
		}
	}
	for _, text := range []string{event.WeightedAveragePrice, event.LastPrice, event.LastQuantity, event.OpenPrice, event.HighPrice, event.LowPrice, event.BaseVolume, event.QuoteVolume} {
		if err := validateUSDMDecimalText(text, normalize.CanonicalPriceScale, true); err != nil {
			return USDMTicker24h{}, err
		}
	}
	return event, nil
}

type USDMDerivativeTicker struct {
	Symbol           string
	EventTimeMS      int64
	NativeSourceRole string
	LastPrice        normalize.NumericField
	MarkPrice        normalize.NumericField
	IndexPrice       normalize.NumericField
	FundingRate      normalize.NumericField
	NextFundingTime  normalize.TimeField
	OpenInterest     []normalize.OpenInterestObservation
	SettlementPrice  normalize.NumericField
	Basis            normalize.NumericField
	Premium          normalize.NumericField
}

// ParseUSDMIndexPriceUpdate parses the dedicated @indexPrice stream. The
// stream is independent of @markPrice, so fields not carried by this payload
// remain explicitly missing in the returned derivative ticker.
func ParseUSDMIndexPriceUpdate(raw []byte, receivedTimeNS int64, instrument normalize.InstrumentIdentity) (USDMDerivativeTicker, error) {
	var wire struct {
		EventType   string `json:"e"`
		EventTimeMS int64  `json:"E"`
		Symbol      string `json:"s"`
		IndexPrice  string `json:"p"`
	}
	if err := unmarshalUSDMBoundedStrict(raw, &wire); err != nil {
		return USDMDerivativeTicker{}, err
	}
	if (wire.EventType != "indexPriceUpdate" && wire.EventType != "IndexUpdate") || wire.Symbol == "" || wire.Symbol != instrument.NativeID || wire.EventTimeMS < 0 {
		return USDMDerivativeTicker{}, fmt.Errorf("%w: malformed index-price event identity", ErrUSDMInvalidMarketPayload)
	}
	provenance, err := usdmFieldProvenance(wire.EventTimeMS, receivedTimeNS)
	if err != nil {
		return USDMDerivativeTicker{}, err
	}
	decimal, err := normalize.ParseDecimal(wire.IndexPrice, normalize.CanonicalPriceScale, normalize.DefaultDecimalBounds())
	if err != nil || strings.HasPrefix(decimal.Coefficient, "-") || decimal.IsZero() {
		return USDMDerivativeTicker{}, fmt.Errorf("%w: invalid index price", ErrUSDMInvalidMarketPayload)
	}
	indexPrice := normalize.NumericField{
		State: normalize.SourceValue,
		Value: normalize.Numeric{
			Decimal: decimal,
			Unit:    normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID),
		},
		Provenance: provenance,
	}
	return USDMDerivativeTicker{
		Symbol: wire.Symbol, EventTimeMS: wire.EventTimeMS, NativeSourceRole: string(USDMRoleIndexPrice),
		LastPrice: missingUSDMNumericField(), MarkPrice: missingUSDMNumericField(), IndexPrice: indexPrice,
		FundingRate: missingUSDMNumericField(), NextFundingTime: missingUSDMTimeField(),
		SettlementPrice: missingUSDMNumericField(), Basis: missingUSDMNumericField(), Premium: missingUSDMNumericField(),
	}, nil
}

func ParseUSDMDerivativeTicker(raw []byte, receivedTimeNS int64, instrument normalize.InstrumentIdentity) (USDMDerivativeTicker, error) {
	var object map[string]json.RawMessage
	if err := unmarshalUSDMBounded(raw, &object); err != nil {
		return USDMDerivativeTicker{}, err
	}
	var eventType, symbol string
	var eventTimeMS int64
	if err := requireUSDMJSON(object, "e", &eventType); err != nil {
		return USDMDerivativeTicker{}, err
	}
	if err := requireUSDMJSON(object, "E", &eventTimeMS); err != nil {
		return USDMDerivativeTicker{}, err
	}
	if err := requireUSDMJSON(object, "s", &symbol); err != nil {
		return USDMDerivativeTicker{}, err
	}
	if eventType != "markPriceUpdate" || symbol == "" || symbol != instrument.NativeID || eventTimeMS < 0 {
		return USDMDerivativeTicker{}, fmt.Errorf("%w: malformed mark-price event identity", ErrUSDMInvalidMarketPayload)
	}
	provenance, err := usdmFieldProvenance(eventTimeMS, receivedTimeNS)
	if err != nil {
		return USDMDerivativeTicker{}, err
	}
	priceUnit := normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)
	mark, err := parseUSDMNumericField(object, "p", normalize.CanonicalPriceScale, priceUnit, provenance)
	if err != nil {
		return USDMDerivativeTicker{}, err
	}
	index, err := parseUSDMNumericField(object, "i", normalize.CanonicalPriceScale, priceUnit, provenance)
	if err != nil {
		return USDMDerivativeTicker{}, err
	}
	funding, err := parseUSDMNumericField(object, "r", normalize.CanonicalAmountScale, normalize.RateUnit(), provenance)
	if err != nil {
		return USDMDerivativeTicker{}, err
	}
	settlement, err := parseUSDMNumericField(object, "P", normalize.CanonicalPriceScale, priceUnit, provenance)
	if err != nil {
		return USDMDerivativeTicker{}, err
	}
	nextFunding, err := parseUSDMTimeField(object, "T", provenance)
	if err != nil {
		return USDMDerivativeTicker{}, err
	}
	ticker := USDMDerivativeTicker{
		Symbol: symbol, EventTimeMS: eventTimeMS, NativeSourceRole: string(USDMRoleMarkIndexFunding),
		LastPrice: missingUSDMNumericField(), MarkPrice: mark, IndexPrice: index, FundingRate: funding, NextFundingTime: nextFunding,
		SettlementPrice: settlement, Basis: missingUSDMNumericField(), Premium: missingUSDMNumericField(),
	}
	return ticker, nil
}

func (e USDMDerivativeTicker) Normalized(metadata normalize.Metadata) (normalize.DerivativeTickerV1, error) {
	event := normalize.DerivativeTickerV1{Metadata: metadata, NativeSourceRole: e.NativeSourceRole, LastPrice: e.LastPrice, MarkPrice: e.MarkPrice, IndexPrice: e.IndexPrice, FundingRate: e.FundingRate, NextFundingTime: e.NextFundingTime, OpenInterest: append([]normalize.OpenInterestObservation(nil), e.OpenInterest...), SettlementPrice: e.SettlementPrice, Basis: e.Basis, Premium: e.Premium}
	if err := event.Validate(); err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	return event, nil
}

type USDMLiquidation struct {
	EventTimeMS               int64
	Symbol                    string
	Side                      normalize.Side
	OriginalQuantity          string
	LastFilledQuantity        string
	AccumulatedFilledQuantity string
	OrderPrice                string
	AveragePrice              string
	TradeTimeMS               int64
	NativeSourceRole          string
	Completeness              normalize.LiquidationCompleteness
}

func ParseUSDMLiquidation(raw []byte) (USDMLiquidation, error) {
	var wire struct {
		EventType   string `json:"e"`
		EventTimeMS int64  `json:"E"`
		Order       struct {
			Symbol                    string `json:"s"`
			Side                      string `json:"S"`
			OriginalQuantity          string `json:"q"`
			OrderPrice                string `json:"p"`
			AveragePrice              string `json:"ap"`
			LastFilledQuantity        string `json:"l"`
			AccumulatedFilledQuantity string `json:"z"`
			TradeTimeMS               int64  `json:"T"`
		} `json:"o"`
	}
	if err := unmarshalUSDMBounded(raw, &wire); err != nil {
		return USDMLiquidation{}, err
	}
	if wire.EventType != "forceOrder" || wire.EventTimeMS < 0 || wire.Order.Symbol == "" || wire.Order.TradeTimeMS < 0 {
		return USDMLiquidation{}, fmt.Errorf("%w: malformed force-order identity", ErrUSDMInvalidMarketPayload)
	}
	side, err := parseUSDMSide(wire.Order.Side)
	if err != nil {
		return USDMLiquidation{}, err
	}
	for _, field := range []struct {
		text  string
		scale uint8
	}{
		{wire.Order.OriginalQuantity, normalize.CanonicalAmountScale}, {wire.Order.LastFilledQuantity, normalize.CanonicalAmountScale}, {wire.Order.AccumulatedFilledQuantity, normalize.CanonicalAmountScale},
		{wire.Order.OrderPrice, normalize.CanonicalPriceScale}, {wire.Order.AveragePrice, normalize.CanonicalPriceScale},
	} {
		if err := validateUSDMDecimalText(field.text, field.scale, true); err != nil {
			return USDMLiquidation{}, err
		}
	}
	return USDMLiquidation{EventTimeMS: wire.EventTimeMS, Symbol: wire.Order.Symbol, Side: side, OriginalQuantity: wire.Order.OriginalQuantity, LastFilledQuantity: wire.Order.LastFilledQuantity, AccumulatedFilledQuantity: wire.Order.AccumulatedFilledQuantity, OrderPrice: wire.Order.OrderPrice, AveragePrice: wire.Order.AveragePrice, TradeTimeMS: wire.Order.TradeTimeMS, NativeSourceRole: string(USDMRoleLargestLiquidationWindow), Completeness: normalize.LiquidationLargestInWindow}, nil
}

func (e USDMLiquidation) Normalized(metadata normalize.Metadata, receivedTimeNS int64, instrument normalize.InstrumentIdentity) (normalize.LiquidationV1, error) {
	if e.Symbol != instrument.NativeID {
		return normalize.LiquidationV1{}, fmt.Errorf("%w: liquidation symbol does not match instrument", ErrUSDMInvalidMarketPayload)
	}
	amount, err := normalize.ParseDecimal(e.OriginalQuantity, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.LiquidationV1{}, err
	}
	provenance, err := usdmFieldProvenance(e.EventTimeMS, receivedTimeNS)
	if err != nil {
		return normalize.LiquidationV1{}, err
	}
	price, err := normalize.ParseDecimal(e.AveragePrice, normalize.CanonicalPriceScale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.LiquidationV1{}, err
	}
	event := normalize.LiquidationV1{
		Metadata: metadata, NativeSourceRole: e.NativeSourceRole, NativeRole: normalize.LiquidationNativeSnapshot,
		Side: e.Side, SideSemantics: normalize.LiquidationOrderSide,
		Amount:    normalize.NativeValue{Decimal: amount, Unit: normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: "forceOrder.q"}},
		Price:     normalize.NumericField{State: normalize.SourceValue, Value: normalize.Numeric{Decimal: price, Unit: normalize.SpotPriceUnit(instrument.BaseAssetID, instrument.QuoteAssetID)}, Provenance: provenance},
		PriceType: normalize.LiquidationAverageFillPrice, Completeness: normalize.LiquidationLargestInWindow,
		Window: normalize.LiquidationWindow{DurationNS: USDMLiquidationWindowNS, Selection: normalize.LiquidationLargestPerSymbol, PerSymbol: true},
	}
	if err := event.Validate(); err != nil {
		return normalize.LiquidationV1{}, err
	}
	return event, nil
}

func parseUSDMNumericField(object map[string]json.RawMessage, key string, scale uint8, unit normalize.Unit, provenance normalize.FieldProvenance) (normalize.NumericField, error) {
	field, err := parseUSDMStringField(object, key)
	if err != nil {
		return normalize.NumericField{}, err
	}
	result := missingUSDMNumericField()
	result.State = field.State
	if field.State != normalize.SourceMissing {
		result.Provenance = provenance
	}
	if field.State != normalize.SourceValue {
		return result, nil
	}
	decimal, err := normalize.ParseDecimal(field.Text, scale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.NumericField{}, fmt.Errorf("%w: field %s decimal", ErrUSDMInvalidMarketPayload, key)
	}
	result.Value = normalize.Numeric{Decimal: decimal, Unit: unit}
	return result, nil
}

func parseUSDMTimeField(object map[string]json.RawMessage, key string, provenance normalize.FieldProvenance) (normalize.TimeField, error) {
	raw, ok := object[key]
	if !ok {
		return missingUSDMTimeField(), nil
	}
	result := normalize.TimeField{Provenance: provenance}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		result.State = normalize.SourceNull
		return result, nil
	}
	if len(raw) > 0 && raw[0] == '"' {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return normalize.TimeField{}, fmt.Errorf("%w: time field %s", ErrUSDMInvalidMarketPayload, key)
		}
		if text == "" {
			result.State = normalize.SourceEmpty
			return result, nil
		}
		return normalize.TimeField{}, fmt.Errorf("%w: time field %s must be integer milliseconds", ErrUSDMInvalidMarketPayload, key)
	}
	var valueMS int64
	if json.Unmarshal(raw, &valueMS) != nil || valueMS < 0 || valueMS > (1<<63-1)/1_000_000 {
		return normalize.TimeField{}, fmt.Errorf("%w: invalid time field %s", ErrUSDMInvalidMarketPayload, key)
	}
	result.State = normalize.SourceValue
	result.ValueNS = valueMS * 1_000_000
	result.Resolution = normalize.ResolutionMillisecond
	return result, nil
}

func parseUSDMStringField(object map[string]json.RawMessage, key string) (USDMStringField, error) {
	raw, ok := object[key]
	if !ok {
		return USDMStringField{State: normalize.SourceMissing}, nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return USDMStringField{State: normalize.SourceNull}, nil
	}
	var text string
	if json.Unmarshal(raw, &text) != nil {
		return USDMStringField{}, fmt.Errorf("%w: field %s must be a string", ErrUSDMInvalidMarketPayload, key)
	}
	if text == "" {
		return USDMStringField{State: normalize.SourceEmpty}, nil
	}
	return USDMStringField{State: normalize.SourceValue, Text: text}, nil
}

func usdmFieldProvenance(sourceTimeMS, receivedTimeNS int64) (normalize.FieldProvenance, error) {
	if sourceTimeMS < 0 || sourceTimeMS > (1<<63-1)/1_000_000 {
		return normalize.FieldProvenance{}, fmt.Errorf("%w: source time overflow", ErrUSDMInvalidMarketPayload)
	}
	sourceTimeNS := sourceTimeMS * 1_000_000
	if receivedTimeNS < sourceTimeNS {
		return normalize.FieldProvenance{}, fmt.Errorf("%w: receive time precedes source time", ErrUSDMInvalidMarketPayload)
	}
	return normalize.FieldProvenance{SourceTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond, AgeNS: normalize.OptionalUint64{Value: uint64(receivedTimeNS - sourceTimeNS), Valid: true}}, nil
}

func missingUSDMNumericField() normalize.NumericField {
	return normalize.NumericField{State: normalize.SourceMissing, Provenance: normalize.FieldProvenance{SourceTimeResolution: normalize.ResolutionAbsent}}
}

func missingUSDMTimeField() normalize.TimeField {
	return normalize.TimeField{State: normalize.SourceMissing, Resolution: normalize.ResolutionAbsent, Provenance: normalize.FieldProvenance{SourceTimeResolution: normalize.ResolutionAbsent}}
}

func missingUSDMDerivedValue() normalize.DerivedValue {
	return normalize.DerivedValue{Field: missingUSDMNumericField()}
}

func validateUSDMSignedDecimalText(text string, scale uint8) error {
	if _, err := normalize.ParseDecimal(text, scale, normalize.DefaultDecimalBounds()); err != nil {
		return fmt.Errorf("%w: invalid signed decimal", ErrUSDMInvalidMarketPayload)
	}
	return nil
}

func validateUSDMDecimalText(text string, scale uint8, allowZero bool) error {
	decimal, err := normalize.ParseDecimal(text, scale, normalize.DefaultDecimalBounds())
	if err != nil || strings.HasPrefix(decimal.Coefficient, "-") || (!allowZero && decimal.IsZero()) {
		return fmt.Errorf("%w: invalid decimal", ErrUSDMInvalidMarketPayload)
	}
	return nil
}

func parseUSDMSide(value string) (normalize.Side, error) {
	switch value {
	case "BUY":
		return normalize.SideBuy, nil
	case "SELL":
		return normalize.SideSell, nil
	default:
		return "", fmt.Errorf("%w: unknown side %q", ErrUSDMInvalidMarketPayload, value)
	}
}

func requireUSDMJSON(object map[string]json.RawMessage, key string, destination any) error {
	raw, ok := object[key]
	if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) || json.Unmarshal(raw, destination) != nil {
		return fmt.Errorf("%w: required field %s", ErrUSDMInvalidMarketPayload, key)
	}
	return nil
}

func unmarshalUSDMBounded(raw []byte, destination any) error {
	if len(raw) == 0 || len(raw) > USDMMaxRawPayloadBytes || !json.Valid(raw) {
		return ErrUSDMInvalidMarketPayload
	}
	if err := json.Unmarshal(raw, destination); err != nil {
		return fmt.Errorf("%w: %v", ErrUSDMInvalidMarketPayload, err)
	}
	return nil
}

func unmarshalUSDMBoundedStrict(raw []byte, destination any) error {
	if len(raw) == 0 || len(raw) > USDMMaxRawPayloadBytes {
		return ErrUSDMInvalidMarketPayload
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		return fmt.Errorf("%w: %v", ErrUSDMInvalidMarketPayload, err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("%w: trailing JSON value", ErrUSDMInvalidMarketPayload)
	}
	return nil
}
