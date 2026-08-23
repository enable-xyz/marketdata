package bybit

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/enable-xyz/marketdata/normalize"
)

type NativeField struct {
	State        normalize.SourceState
	Text         string
	SourceTimeNS int64
}

type DerivativeTickerFields struct {
	LastPrice               NativeField
	MarkPrice               NativeField
	IndexPrice              NativeField
	FundingRate             NativeField
	NextFundingTime         NativeField
	OpenInterest            NativeField
	OpenInterestValue       NativeField
	SingleOpenInterest      NativeField
	SingleOpenInterestValue NativeField
	PredictedDeliveryPrice  NativeField
	Basis                   NativeField
	BasisRate               NativeField
}

type TickerUnitContract struct {
	Price             normalize.Unit
	OpenInterestSize  normalize.NativeUnit
	OpenInterestValue normalize.NativeUnit
}

func (c TickerUnitContract) Validate() error {
	if err := c.Price.Validate(); err != nil {
		return err
	}
	if err := c.OpenInterestSize.Validate(); err != nil {
		return err
	}
	return c.OpenInterestValue.Validate()
}

type DerivativeTickerState struct {
	category        Category
	symbol          string
	connectionEpoch string
	seeded          bool
	lastMessageNS   int64
	fields          DerivativeTickerFields
}

func NewDerivativeTickerState(category Category, symbol, connectionEpoch string) (*DerivativeTickerState, error) {
	if category != Linear && category != Inverse {
		return nil, fmt.Errorf("%w: derivative ticker is Linear/Inverse only", ErrUnsupportedRole)
	}
	if !validSymbol(symbol) || connectionEpoch == "" || len(connectionEpoch) > 128 {
		return nil, ErrInvalidTopic
	}
	state := &DerivativeTickerState{category: category, symbol: symbol}
	if err := state.BeginConnection(connectionEpoch); err != nil {
		return nil, err
	}
	return state, nil
}

// BeginConnection is the reconnect boundary. A new epoch clears every cached
// sparse field before any delta can be accepted.
func (s *DerivativeTickerState) BeginConnection(connectionEpoch string) error {
	if s == nil || connectionEpoch == "" || len(connectionEpoch) > 128 {
		return ErrInvalidTopic
	}
	if s.connectionEpoch == connectionEpoch {
		return nil
	}
	s.connectionEpoch = connectionEpoch
	s.seeded = false
	s.lastMessageNS = 0
	s.fields = missingDerivativeTickerFields()
	return nil
}

func (s *DerivativeTickerState) Apply(payload []byte) error {
	if s == nil {
		return ErrInvalidPayload
	}
	message, err := parseTickerPayload(payload)
	if err != nil {
		return err
	}
	if message.Topic != "tickers."+s.symbol || message.Symbol != s.symbol || message.TSNS < 0 {
		return ErrInvalidPayload
	}
	nextFields := s.fields
	nextSeeded := s.seeded
	switch message.Type {
	case "snapshot":
		nextFields = missingDerivativeTickerFields()
		nextSeeded = true
	case "delta":
		if !s.seeded {
			return ErrUnseededTicker
		}
	default:
		return ErrInvalidPayload
	}
	if s.lastMessageNS != 0 && message.TSNS < s.lastMessageNS {
		return fmt.Errorf("%w: ticker source time regressed", ErrInvalidPayload)
	}
	if err := applyDerivativeTickerFields(message.Data, message.TSNS, &nextFields); err != nil {
		return err
	}
	s.fields = nextFields
	s.seeded = nextSeeded
	s.lastMessageNS = message.TSNS
	return nil
}

func (s *DerivativeTickerState) Seeded() bool { return s != nil && s.seeded }

func (s *DerivativeTickerState) Fields() DerivativeTickerFields {
	if s == nil {
		return missingDerivativeTickerFields()
	}
	return s.fields
}

func (s *DerivativeTickerState) OpenInterest(receivedTimeNS int64, units TickerUnitContract) ([]normalize.OpenInterestObservation, error) {
	if s == nil || !s.seeded {
		return nil, ErrUnseededTicker
	}
	if err := units.Validate(); err != nil {
		return nil, err
	}
	both, err := makeOpenInterestObservation("openInterest", s.fields.OpenInterest, normalize.OpenInterestBothSides, units.OpenInterestSize, receivedTimeNS)
	if err != nil {
		return nil, err
	}
	bothValue, err := makeOpenInterestObservation("openInterestValue", s.fields.OpenInterestValue, normalize.OpenInterestBothSides, units.OpenInterestValue, receivedTimeNS)
	if err != nil {
		return nil, err
	}
	single, err := makeOpenInterestObservation("singleOpenInterest", s.fields.SingleOpenInterest, normalize.OpenInterestSingleSide, units.OpenInterestSize, receivedTimeNS)
	if err != nil {
		return nil, err
	}
	singleValue, err := makeOpenInterestObservation("singleOpenInterestValue", s.fields.SingleOpenInterestValue, normalize.OpenInterestSingleSide, units.OpenInterestValue, receivedTimeNS)
	if err != nil {
		return nil, err
	}
	if both.State == normalize.SourceValue && bothValue.State == normalize.SourceValue {
		both.ReportedVariants = []normalize.ReportedValue{{Name: "openInterestValue", Value: bothValue.Native}}
	}
	if single.State == normalize.SourceValue && singleValue.State == normalize.SourceValue {
		single.ReportedVariants = []normalize.ReportedValue{{Name: "singleOpenInterestValue", Value: singleValue.Native}}
	}
	observations := []normalize.OpenInterestObservation{both, bothValue, single, singleValue}
	for _, observation := range observations {
		if err := observation.Validate(); err != nil {
			return nil, err
		}
	}
	return observations, nil
}

func (s *DerivativeTickerState) Normalized(metadata normalize.Metadata, units TickerUnitContract) (normalize.DerivativeTickerV1, error) {
	if s == nil || !s.seeded {
		return normalize.DerivativeTickerV1{}, ErrUnseededTicker
	}
	if err := units.Validate(); err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	oi, err := s.OpenInterest(metadata.ReceivedTimeNS, units)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	last, err := makeNumericField(s.fields.LastPrice, units.Price, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	mark, err := makeNumericField(s.fields.MarkPrice, units.Price, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	index, err := makeNumericField(s.fields.IndexPrice, units.Price, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	funding, err := makeNumericField(s.fields.FundingRate, normalize.RateUnit(), metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	nextFunding, err := makeTimeField(s.fields.NextFundingTime, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	settlement, err := makeNumericField(s.fields.PredictedDeliveryPrice, units.Price, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	basis, err := makeNumericField(s.fields.Basis, units.Price, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	event := normalize.DerivativeTickerV1{
		Metadata:         metadata,
		NativeSourceRole: "bybit_v5_derivative_ticker_sparse_state",
		LastPrice:        last,
		MarkPrice:        mark,
		IndexPrice:       index,
		FundingRate:      funding,
		NextFundingTime:  nextFunding,
		OpenInterest:     oi,
		SettlementPrice:  settlement,
		Basis:            basis,
		Premium:          missingNumericField(),
	}
	if err := event.Validate(); err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	return event, nil
}

type SpotTickerState struct {
	symbol          string
	connectionEpoch string
	seeded          bool
	fields          map[string]NativeField
}

func NewSpotTickerState(symbol, connectionEpoch string) (*SpotTickerState, error) {
	if !validSymbol(symbol) || connectionEpoch == "" {
		return nil, ErrInvalidTopic
	}
	return &SpotTickerState{symbol: symbol, connectionEpoch: connectionEpoch, fields: make(map[string]NativeField)}, nil
}

func (s *SpotTickerState) BeginConnection(connectionEpoch string) error {
	if s == nil || connectionEpoch == "" || len(connectionEpoch) > 128 {
		return ErrInvalidTopic
	}
	if s.connectionEpoch != connectionEpoch {
		s.connectionEpoch = connectionEpoch
		s.seeded = false
		s.fields = make(map[string]NativeField)
	}
	return nil
}

func (s *SpotTickerState) Apply(payload []byte) error {
	if s == nil {
		return ErrInvalidPayload
	}
	message, err := parseTickerPayload(payload)
	if err != nil {
		return err
	}
	if message.Type != "snapshot" || message.Topic != "tickers."+s.symbol || message.Symbol != s.symbol {
		return fmt.Errorf("%w: Spot ticker is snapshot-only", ErrInvalidPayload)
	}
	fields := make(map[string]NativeField, len(message.Data))
	for name := range message.Data {
		field := NativeField{State: normalize.SourceMissing}
		if err := applyTickerField(message.Data, name, message.TSNS, &field); err != nil {
			return err
		}
		fields[name] = field
	}
	s.fields = fields
	s.seeded = true
	return nil
}

func (s *SpotTickerState) Fields() map[string]NativeField {
	clone := make(map[string]NativeField)
	if s == nil {
		return clone
	}
	for name, field := range s.fields {
		clone[name] = field
	}
	return clone
}

type tickerPayload struct {
	Topic  string
	Type   string
	Symbol string
	TSNS   int64
	Data   map[string]json.RawMessage
}

func parseTickerPayload(payload []byte) (tickerPayload, error) {
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || !json.Valid(payload) {
		return tickerPayload{}, ErrInvalidPayload
	}
	var envelope struct {
		Topic string          `json:"topic"`
		Type  string          `json:"type"`
		TS    int64           `json:"ts"`
		Data  json.RawMessage `json:"data"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&envelope); err != nil || envelope.TS < 0 || envelope.TS > (1<<63-1)/int64(1e6) {
		return tickerPayload{}, ErrInvalidPayload
	}
	data, err := decodeTickerData(envelope.Data)
	if err != nil {
		return tickerPayload{}, err
	}
	if err := validateTickerData(data); err != nil {
		return tickerPayload{}, err
	}
	symbolRaw, ok := data["symbol"]
	if !ok {
		return tickerPayload{}, ErrInvalidPayload
	}
	var symbol string
	if err := json.Unmarshal(symbolRaw, &symbol); err != nil || !validSymbol(symbol) {
		return tickerPayload{}, ErrInvalidPayload
	}
	return tickerPayload{Topic: envelope.Topic, Type: envelope.Type, Symbol: symbol, TSNS: envelope.TS * int64(1e6), Data: data}, nil
}

func decodeTickerData(raw json.RawMessage) (map[string]json.RawMessage, error) {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(raw, &object); err == nil && object != nil {
		return object, nil
	}
	var array []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &array); err != nil || len(array) != 1 || array[0] == nil {
		return nil, ErrInvalidPayload
	}
	return array[0], nil
}

func validateTickerData(data map[string]json.RawMessage) error {
	for _, raw := range data {
		if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			continue
		}
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return ErrInvalidPayload
		}
	}
	return nil
}

func applyDerivativeTickerFields(data map[string]json.RawMessage, sourceTimeNS int64, fields *DerivativeTickerFields) error {
	destinations := [...]struct {
		name  string
		field *NativeField
	}{
		{"lastPrice", &fields.LastPrice},
		{"markPrice", &fields.MarkPrice},
		{"indexPrice", &fields.IndexPrice},
		{"fundingRate", &fields.FundingRate},
		{"nextFundingTime", &fields.NextFundingTime},
		{"openInterest", &fields.OpenInterest},
		{"openInterestValue", &fields.OpenInterestValue},
		{"singleOpenInterest", &fields.SingleOpenInterest},
		{"singleOpenInterestValue", &fields.SingleOpenInterestValue},
		{"predictedDeliveryPrice", &fields.PredictedDeliveryPrice},
		{"basis", &fields.Basis},
		{"basisRate", &fields.BasisRate},
	}
	for _, destination := range destinations {
		if err := applyTickerField(data, destination.name, sourceTimeNS, destination.field); err != nil {
			return err
		}
	}
	return nil
}

func applyTickerField(data map[string]json.RawMessage, name string, sourceTimeNS int64, destination *NativeField) error {
	raw, ok := data[name]
	if !ok {
		return nil
	}
	if bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		*destination = NativeField{State: normalize.SourceNull, SourceTimeNS: sourceTimeNS}
		return nil
	}
	var text string
	if err := json.Unmarshal(raw, &text); err != nil {
		return ErrInvalidPayload
	}
	if text == "" {
		*destination = NativeField{State: normalize.SourceEmpty, SourceTimeNS: sourceTimeNS}
		return nil
	}
	*destination = NativeField{State: normalize.SourceValue, Text: text, SourceTimeNS: sourceTimeNS}
	return nil
}

func missingDerivativeTickerFields() DerivativeTickerFields {
	missing := NativeField{State: normalize.SourceMissing}
	return DerivativeTickerFields{LastPrice: missing, MarkPrice: missing, IndexPrice: missing, FundingRate: missing, NextFundingTime: missing, OpenInterest: missing, OpenInterestValue: missing, SingleOpenInterest: missing, SingleOpenInterestValue: missing, PredictedDeliveryPrice: missing, Basis: missing, BasisRate: missing}
}

func makeNumericField(field NativeField, unit normalize.Unit, receivedTimeNS int64) (normalize.NumericField, error) {
	result := normalize.NumericField{State: field.State}
	if field.State != normalize.SourceMissing {
		provenance, err := makeFieldProvenance(field.SourceTimeNS, receivedTimeNS)
		if err != nil {
			return normalize.NumericField{}, err
		}
		result.Provenance = provenance
	}
	if field.State != normalize.SourceValue {
		return result, result.Validate()
	}
	decimal, err := normalize.ParseDecimal(field.Text, normalize.CanonicalPriceScale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.NumericField{}, err
	}
	result.Value = normalize.Numeric{Decimal: decimal, Unit: unit}
	return result, result.Validate()
}

func makeTimeField(field NativeField, receivedTimeNS int64) (normalize.TimeField, error) {
	result := normalize.TimeField{State: field.State, Resolution: normalize.ResolutionAbsent}
	if field.State != normalize.SourceMissing {
		provenance, err := makeFieldProvenance(field.SourceTimeNS, receivedTimeNS)
		if err != nil {
			return normalize.TimeField{}, err
		}
		result.Provenance = provenance
	}
	if field.State != normalize.SourceValue {
		return result, result.Validate()
	}
	milliseconds, err := strconv.ParseInt(field.Text, 10, 64)
	if err != nil || milliseconds < 0 || milliseconds > (1<<63-1)/int64(1e6) {
		return normalize.TimeField{}, ErrInvalidPayload
	}
	result.ValueNS = milliseconds * int64(1e6)
	result.Resolution = normalize.ResolutionMillisecond
	return result, result.Validate()
}

func makeOpenInterestObservation(variant string, field NativeField, sidedness normalize.OpenInterestSidedness, unit normalize.NativeUnit, receivedTimeNS int64) (normalize.OpenInterestObservation, error) {
	observation := normalize.OpenInterestObservation{
		State:        field.State,
		Variant:      variant,
		DerivedBase:  missingDerivedValue(),
		DerivedQuote: missingDerivedValue(),
		DerivedUSD:   missingDerivedValue(),
	}
	if field.State != normalize.SourceMissing {
		provenance, err := makeFieldProvenance(field.SourceTimeNS, receivedTimeNS)
		if err != nil {
			return normalize.OpenInterestObservation{}, err
		}
		observation.Provenance = provenance
	}
	if field.State != normalize.SourceValue {
		return observation, observation.Validate()
	}
	decimal, err := normalize.ParseDecimal(field.Text, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.OpenInterestObservation{}, err
	}
	observation.Native = normalize.NativeValue{Decimal: decimal, Unit: unit}
	observation.Sidedness = sidedness
	return observation, observation.Validate()
}

func makeFieldProvenance(sourceTimeNS, receivedTimeNS int64) (normalize.FieldProvenance, error) {
	if sourceTimeNS < 0 || receivedTimeNS < sourceTimeNS {
		return normalize.FieldProvenance{}, ErrInvalidPayload
	}
	return normalize.FieldProvenance{
		SourceTimeNS:         normalize.OptionalInt64{Value: sourceTimeNS, Valid: true},
		SourceTimeResolution: normalize.ResolutionMillisecond,
		AgeNS:                normalize.OptionalUint64{Value: uint64(receivedTimeNS - sourceTimeNS), Valid: true},
	}, nil
}

func missingNumericField() normalize.NumericField {
	return normalize.NumericField{State: normalize.SourceMissing}
}

func missingDerivedValue() normalize.DerivedValue {
	return normalize.DerivedValue{Field: missingNumericField()}
}
