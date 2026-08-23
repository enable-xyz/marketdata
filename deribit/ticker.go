package deribit

import (
	"encoding/json"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

type Quote struct {
	Channel        string
	InstrumentName string
	TimestampMS    int64
	BestBidPrice   SourceNumber
	BestBidAmount  SourceNumber
	BestAskPrice   SourceNumber
	BestAskAmount  SourceNumber
}

type MappedQuote struct {
	InstrumentUID string
	SourceTimeNS  int64
	BidPrice      MappedNumber
	BidAmount     MappedNativeNumber
	AskPrice      MappedNumber
	AskAmount     MappedNativeNumber
	UnitInference normalize.DeribitUnitInference
}

func ParseQuote(payload []byte) (Quote, error) {
	channel, raw, err := notificationData(payload)
	if err != nil || !strings.HasPrefix(channel, "quote.") {
		return Quote{}, ErrInvalidRPC
	}
	object, err := decodeObject(raw)
	if err != nil {
		return Quote{}, err
	}
	instrument, err := requiredString(object, "instrument_name")
	if err != nil || channel != "quote."+instrument {
		return Quote{}, ErrInvalidRPC
	}
	timestamp, err := requiredInt64(object, "timestamp")
	if err != nil {
		return Quote{}, err
	}
	fields := make([]SourceNumber, 4)
	for index, name := range []string{"best_bid_price", "best_bid_amount", "best_ask_price", "best_ask_amount"} {
		fields[index], err = sourceNumber(object, name)
		if err != nil {
			return Quote{}, err
		}
	}
	return Quote{
		Channel: channel, InstrumentName: instrument, TimestampMS: timestamp,
		BestBidPrice: fields[0], BestBidAmount: fields[1], BestAskPrice: fields[2], BestAskAmount: fields[3],
	}, nil
}

func (q Quote) Normalized(terms normalize.DeribitInstrumentTerms) (MappedQuote, error) {
	if q.InstrumentName != terms.InstrumentName {
		return MappedQuote{}, ErrInvalidRPC
	}
	timestampNS, err := millisecondsToNanoseconds(q.TimestampMS)
	if err != nil {
		return MappedQuote{}, err
	}
	inference, err := normalize.InferDeribitAmountUnit(terms, timestampNS)
	if err != nil {
		return MappedQuote{}, err
	}
	priceUnit, err := terms.PremiumPriceUnit()
	if err != nil {
		return MappedQuote{}, err
	}
	bidPrice, err := mappedNumeric(q.BestBidPrice, priceUnit)
	if err != nil {
		return MappedQuote{}, err
	}
	askPrice, err := mappedNumeric(q.BestAskPrice, priceUnit)
	if err != nil {
		return MappedQuote{}, err
	}
	bidAmount, err := mappedNative(q.BestBidAmount, inference)
	if err != nil {
		return MappedQuote{}, err
	}
	askAmount, err := mappedNative(q.BestAskAmount, inference)
	if err != nil {
		return MappedQuote{}, err
	}
	return MappedQuote{
		InstrumentUID: terms.InstrumentUID, SourceTimeNS: timestampNS,
		BidPrice: bidPrice, BidAmount: bidAmount, AskPrice: askPrice, AskAmount: askAmount,
		UnitInference: inference,
	}, nil
}

type Ticker struct {
	Channel         string
	Cadence         Cadence
	InstrumentName  string
	TimestampMS     int64
	NativeState     string
	BestBidPrice    SourceNumber
	BestBidAmount   SourceNumber
	BestAskPrice    SourceNumber
	BestAskAmount   SourceNumber
	LastPrice       SourceNumber
	MarkPrice       SourceNumber
	IndexPrice      SourceNumber
	OpenInterest    SourceNumber
	Volume          SourceNumber
	VolumeUSD       SourceNumber
	CurrentFunding  SourceNumber
	Funding8H       SourceNumber
	UnderlyingPrice SourceNumber
	InterestRate    SourceNumber
	BidIV           SourceNumber
	AskIV           SourceNumber
	MarkIV          SourceNumber
	Delta           SourceNumber
	Gamma           SourceNumber
	Vega            SourceNumber
	Theta           SourceNumber
	Rho             SourceNumber
}

type MappedTicker struct {
	Channel           string
	Cadence           Cadence
	SourceAggregation string
	InstrumentKind    normalize.DeribitInstrumentKind
	InstrumentUID     string
	SourceTimeNS      int64
	NativeState       string
	LifecycleState    normalize.InstrumentLifecycleState
	BestBidPrice      MappedNumber
	BestBidAmount     MappedNativeNumber
	BestAskPrice      MappedNumber
	BestAskAmount     MappedNativeNumber
	LastPrice         MappedNumber
	MarkPrice         MappedNumber
	IndexPrice        MappedNumber
	OpenInterest      MappedNativeNumber
	Volume            MappedNativeNumber
	VolumeUSD         MappedNativeNumber
	CurrentFunding    MappedNumber
	Funding8H         MappedNumber
	UnderlyingPrice   MappedNumber
	InterestRate      MappedNumber
	BidIV             MappedNumber
	AskIV             MappedNumber
	MarkIV            MappedNumber
	Delta             MappedNativeNumber
	Gamma             MappedNativeNumber
	Vega              MappedNativeNumber
	Theta             MappedNativeNumber
	Rho               MappedNativeNumber
	UnitInference     normalize.DeribitUnitInference
}

func ParseTicker(payload []byte) (Ticker, error) {
	channel, raw, err := notificationData(payload)
	if err != nil {
		return Ticker{}, ErrInvalidRPC
	}
	parts := strings.Split(channel, ".")
	if len(parts) != 3 || parts[0] != "ticker" {
		return Ticker{}, ErrInvalidRPC
	}
	cadence := Cadence(parts[2])
	if err := cadence.Validate(); err != nil {
		return Ticker{}, ErrInvalidRPC
	}
	object, err := decodeObject(raw)
	if err != nil {
		return Ticker{}, err
	}
	instrument, err := requiredString(object, "instrument_name")
	if err != nil || instrument != parts[1] {
		return Ticker{}, ErrInvalidRPC
	}
	timestamp, err := requiredInt64(object, "timestamp")
	if err != nil {
		return Ticker{}, err
	}
	state, err := requiredString(object, "state")
	if err != nil || !validBookState(state) {
		return Ticker{}, ErrInvalidRPC
	}
	get := func(name string) (SourceNumber, error) { return sourceNumber(object, name) }
	ticker := Ticker{Channel: channel, Cadence: cadence, InstrumentName: instrument, TimestampMS: timestamp, NativeState: state}
	destinations := []struct {
		name  string
		field *SourceNumber
	}{
		{"best_bid_price", &ticker.BestBidPrice}, {"best_bid_amount", &ticker.BestBidAmount},
		{"best_ask_price", &ticker.BestAskPrice}, {"best_ask_amount", &ticker.BestAskAmount},
		{"last_price", &ticker.LastPrice}, {"mark_price", &ticker.MarkPrice}, {"index_price", &ticker.IndexPrice},
		{"open_interest", &ticker.OpenInterest}, {"current_funding", &ticker.CurrentFunding}, {"funding_8h", &ticker.Funding8H},
		{"underlying_price", &ticker.UnderlyingPrice}, {"interest_rate", &ticker.InterestRate},
		{"bid_iv", &ticker.BidIV}, {"ask_iv", &ticker.AskIV}, {"mark_iv", &ticker.MarkIV},
	}
	for _, destination := range destinations {
		*destination.field, err = get(destination.name)
		if err != nil {
			return Ticker{}, err
		}
	}
	statsObject := map[string]json.RawMessage{}
	if rawStats, exists := object["stats"]; exists && string(rawStats) != "null" {
		statsObject, err = decodeObject(rawStats)
		if err != nil {
			return Ticker{}, err
		}
	}
	ticker.Volume, err = sourceNumber(statsObject, "volume")
	if err != nil {
		return Ticker{}, err
	}
	ticker.VolumeUSD, err = sourceNumber(statsObject, "volume_usd")
	if err != nil {
		return Ticker{}, err
	}
	greeksObject := map[string]json.RawMessage{}
	if rawGreeks, exists := object["greeks"]; exists && string(rawGreeks) != "null" {
		greeksObject, err = decodeObject(rawGreeks)
		if err != nil {
			return Ticker{}, err
		}
	}
	for _, destination := range []struct {
		name  string
		field *SourceNumber
	}{{"delta", &ticker.Delta}, {"gamma", &ticker.Gamma}, {"vega", &ticker.Vega}, {"theta", &ticker.Theta}, {"rho", &ticker.Rho}} {
		*destination.field, err = sourceNumber(greeksObject, destination.name)
		if err != nil {
			return Ticker{}, err
		}
	}
	return ticker, nil
}

func (t Ticker) Normalized(terms normalize.DeribitInstrumentTerms) (MappedTicker, error) {
	if t.InstrumentName != terms.InstrumentName || t.Cadence.Validate() != nil ||
		t.Channel != "ticker."+t.InstrumentName+"."+string(t.Cadence) {
		return MappedTicker{}, ErrInvalidRPC
	}
	timestampNS, err := millisecondsToNanoseconds(t.TimestampMS)
	if err != nil {
		return MappedTicker{}, err
	}
	inference, err := normalize.InferDeribitAmountUnit(terms, timestampNS)
	if err != nil {
		return MappedTicker{}, err
	}
	priceUnit, err := terms.PremiumPriceUnit()
	if err != nil {
		return MappedTicker{}, err
	}
	referenceUnit, err := terms.ReferencePriceUnit()
	if err != nil {
		return MappedTicker{}, err
	}
	state, err := normalizedBookState(t.NativeState)
	if err != nil {
		return MappedTicker{}, err
	}
	mapped := MappedTicker{
		Channel: t.Channel, Cadence: t.Cadence, SourceAggregation: sourceAggregationContract(t.Cadence),
		InstrumentKind: terms.Kind, InstrumentUID: terms.InstrumentUID, SourceTimeNS: timestampNS,
		NativeState: t.NativeState, LifecycleState: state, UnitInference: inference,
	}
	for _, field := range []struct {
		source SourceNumber
		dest   *MappedNumber
	}{{t.BestBidPrice, &mapped.BestBidPrice}, {t.BestAskPrice, &mapped.BestAskPrice},
		{t.LastPrice, &mapped.LastPrice}, {t.MarkPrice, &mapped.MarkPrice}} {
		*field.dest, err = mappedNumeric(field.source, priceUnit)
		if err != nil {
			return MappedTicker{}, err
		}
	}
	for _, field := range []struct {
		source SourceNumber
		dest   *MappedNumber
	}{{t.IndexPrice, &mapped.IndexPrice}, {t.UnderlyingPrice, &mapped.UnderlyingPrice}} {
		*field.dest, err = mappedNumeric(field.source, referenceUnit)
		if err != nil {
			return MappedTicker{}, err
		}
	}
	nativeFields := []struct {
		source SourceNumber
		dest   *MappedNativeNumber
	}{{t.BestBidAmount, &mapped.BestBidAmount}, {t.BestAskAmount, &mapped.BestAskAmount},
		{t.OpenInterest, &mapped.OpenInterest}, {t.Volume, &mapped.Volume}}
	for _, field := range nativeFields {
		*field.dest, err = mappedNative(field.source, inference)
		if err != nil {
			return MappedTicker{}, err
		}
	}
	mapped.VolumeUSD = MappedNativeNumber{State: t.VolumeUSD.State}
	if t.VolumeUSD.State == normalize.SourceValue {
		decimal, parseErr := t.VolumeUSD.Decimal(normalize.CanonicalAmountScale)
		if parseErr != nil || strings.HasPrefix(decimal.Coefficient, "-") {
			return MappedTicker{}, ErrInvalidRPC
		}
		mapped.VolumeUSD.Value = normalize.NativeValue{
			Decimal: decimal, Unit: normalize.NativeUnit{Kind: normalize.NativeUnitUSD, AssetID: "USD"},
		}
		if err := mapped.VolumeUSD.Value.Validate(); err != nil {
			return MappedTicker{}, ErrInvalidRPC
		}
	}
	for _, field := range []struct {
		source SourceNumber
		dest   *MappedNumber
		unit   normalize.Unit
	}{
		{t.CurrentFunding, &mapped.CurrentFunding, normalize.RateUnit()}, {t.Funding8H, &mapped.Funding8H, normalize.RateUnit()},
		{t.InterestRate, &mapped.InterestRate, normalize.RateUnit()},
		{t.BidIV, &mapped.BidIV, normalize.ImpliedVolatilityUnit()}, {t.AskIV, &mapped.AskIV, normalize.ImpliedVolatilityUnit()},
		{t.MarkIV, &mapped.MarkIV, normalize.ImpliedVolatilityUnit()},
	} {
		*field.dest, err = mappedRate(field.source, field.unit)
		if err != nil {
			return MappedTicker{}, err
		}
	}
	for _, field := range []struct {
		source SourceNumber
		dest   *MappedNativeNumber
		label  string
	}{
		{t.Delta, &mapped.Delta, "deribit_delta"}, {t.Gamma, &mapped.Gamma, "deribit_gamma"},
		{t.Vega, &mapped.Vega, "deribit_vega"}, {t.Theta, &mapped.Theta, "deribit_theta"},
		{t.Rho, &mapped.Rho, "deribit_rho"},
	} {
		*field.dest, err = mappedVenueNative(field.source, field.label)
		if err != nil {
			return MappedTicker{}, err
		}
	}
	return mapped, nil
}

type MappedOptionSummary struct {
	InstrumentUID  string
	InstrumentName string
	Underlying     string
	Index          string
	ExpiryTimeNS   int64
	Strike         normalize.Numeric
	CallPut        normalize.OptionKind
	Ticker         MappedTicker
}

func (t Ticker) NormalizedOption(instrument Instrument, terms normalize.DeribitInstrumentTerms) (MappedOptionSummary, error) {
	if instrument.Kind != normalize.DeribitInstrumentOption || instrument.Kind != terms.Kind ||
		instrument.InstrumentName != t.InstrumentName || instrument.InstrumentName != terms.InstrumentName ||
		instrument.BaseCurrency != terms.BaseAssetID || instrument.QuoteCurrency != terms.QuoteAssetID ||
		instrument.CounterCurrency != terms.CounterAssetID || instrument.PriceIndex == "" ||
		instrument.Strike.State != normalize.SourceValue {
		return MappedOptionSummary{}, ErrInvalidRPC
	}
	mappedTicker, err := t.Normalized(terms)
	if err != nil {
		return MappedOptionSummary{}, err
	}
	mappedTicker.IndexPrice, err = mappedPrice(t.IndexPrice, instrument.BaseCurrency, instrument.CounterCurrency)
	if err != nil {
		return MappedOptionSummary{}, err
	}
	mappedTicker.UnderlyingPrice, err = mappedPrice(t.UnderlyingPrice, instrument.BaseCurrency, instrument.CounterCurrency)
	if err != nil {
		return MappedOptionSummary{}, err
	}
	strike, err := mappedPrice(instrument.Strike, instrument.BaseCurrency, instrument.CounterCurrency)
	if err != nil || strike.State != normalize.SourceValue {
		return MappedOptionSummary{}, ErrInvalidRPC
	}
	expiryNS, err := millisecondsToNanoseconds(instrument.ExpirationTimestampMS)
	if err != nil {
		return MappedOptionSummary{}, err
	}
	callPut := normalize.OptionCall
	if instrument.OptionType == "put" {
		callPut = normalize.OptionPut
	}
	return MappedOptionSummary{
		InstrumentUID: terms.InstrumentUID, InstrumentName: instrument.InstrumentName,
		Underlying: instrument.BaseCurrency, Index: instrument.PriceIndex, ExpiryTimeNS: expiryNS,
		Strike: strike.Value, CallPut: callPut, Ticker: mappedTicker,
	}, nil
}

func (m MappedTicker) DerivativeTickerV1(metadata normalize.Metadata) (normalize.DerivativeTickerV1, error) {
	if m.InstrumentKind != normalize.DeribitInstrumentFuture ||
		m.UnitInference.State != normalize.DeribitInferenceFixtureProven ||
		metadata.SourceID != SourceID || metadata.ChannelID != m.Channel ||
		metadata.InstrumentUID != m.InstrumentUID || !metadata.SourceEventTimeNS.Valid ||
		metadata.SourceEventTimeNS.Value != m.SourceTimeNS {
		return normalize.DerivativeTickerV1{}, ErrInvalidRPC
	}
	last, err := deribitNumericField(m.LastPrice, m.SourceTimeNS, metadata.ReceivedTimeNS, normalize.ResolutionMillisecond)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	mark, err := deribitNumericField(m.MarkPrice, m.SourceTimeNS, metadata.ReceivedTimeNS, normalize.ResolutionMillisecond)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	index, err := deribitNumericField(m.IndexPrice, m.SourceTimeNS, metadata.ReceivedTimeNS, normalize.ResolutionMillisecond)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	funding, err := deribitNumericField(m.CurrentFunding, m.SourceTimeNS, metadata.ReceivedTimeNS, normalize.ResolutionMillisecond)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	openInterestProvenance, err := deribitFieldProvenance(m.OpenInterest.State, m.SourceTimeNS, metadata.ReceivedTimeNS, normalize.ResolutionMillisecond)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	missingDerived := normalize.DerivedValue{
		Field: normalize.NumericField{
			State:      normalize.SourceMissing,
			Provenance: normalize.FieldProvenance{SourceTimeResolution: normalize.ResolutionAbsent},
		},
	}
	openInterest := normalize.OpenInterestObservation{
		State: m.OpenInterest.State, Variant: "open_interest", Native: m.OpenInterest.Value,
		Provenance:  openInterestProvenance,
		DerivedBase: missingDerived, DerivedQuote: missingDerived, DerivedUSD: missingDerived,
	}
	if m.OpenInterest.State == normalize.SourceValue {
		openInterest.Sidedness = normalize.OpenInterestUnspecified
	}
	missingNumeric := normalize.NumericField{State: normalize.SourceMissing}
	event := normalize.DerivativeTickerV1{
		Metadata: metadata, NativeSourceRole: "deribit_ticker_derivative",
		LastPrice: last, MarkPrice: mark, IndexPrice: index, FundingRate: funding,
		NextFundingTime: normalize.TimeField{State: normalize.SourceMissing, Resolution: normalize.ResolutionAbsent},
		OpenInterest:    []normalize.OpenInterestObservation{openInterest},
		SettlementPrice: missingNumeric, Basis: missingNumeric, Premium: missingNumeric,
	}
	if err := event.Validate(); err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	return event, nil
}

func (m MappedOptionSummary) OptionSummaryV1(metadata normalize.Metadata, instrumentObservedTimeNS int64) (normalize.OptionSummaryV1, error) {
	if metadata.SourceID != SourceID || metadata.ChannelID != m.Ticker.Channel ||
		metadata.InstrumentUID != m.InstrumentUID || m.Ticker.InstrumentKind != normalize.DeribitInstrumentOption ||
		m.Ticker.UnitInference.State != normalize.DeribitInferenceFixtureProven ||
		!metadata.SourceEventTimeNS.Valid || metadata.SourceEventTimeNS.Value != m.Ticker.SourceTimeNS {
		return normalize.OptionSummaryV1{}, ErrInvalidRPC
	}
	staticProvenance, err := deribitFieldProvenance(
		normalize.SourceValue, instrumentObservedTimeNS, metadata.ReceivedTimeNS, normalize.ResolutionNanosecond,
	)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	tickerField := func(field MappedNumber) (normalize.NumericField, error) {
		return deribitNumericField(field, m.Ticker.SourceTimeNS, metadata.ReceivedTimeNS, normalize.ResolutionMillisecond)
	}
	tickerNativeField := func(field MappedNativeNumber) (normalize.NativeNumericField, error) {
		provenance, err := deribitFieldProvenance(
			field.State, m.Ticker.SourceTimeNS, metadata.ReceivedTimeNS, normalize.ResolutionMillisecond,
		)
		if err != nil {
			return normalize.NativeNumericField{}, err
		}
		return normalize.NativeNumericField{State: field.State, Value: field.Value, Provenance: provenance}, nil
	}
	numericSources := []MappedNumber{
		m.Ticker.BestBidPrice, m.Ticker.BestAskPrice, m.Ticker.LastPrice, m.Ticker.MarkPrice,
		m.Ticker.BidIV, m.Ticker.AskIV, m.Ticker.MarkIV, m.Ticker.UnderlyingPrice, m.Ticker.IndexPrice,
	}
	numericFields := make([]normalize.NumericField, len(numericSources))
	for index, source := range numericSources {
		numericFields[index], err = tickerField(source)
		if err != nil {
			return normalize.OptionSummaryV1{}, err
		}
	}
	nativeSources := []MappedNativeNumber{
		m.Ticker.Delta, m.Ticker.Gamma, m.Ticker.Vega, m.Ticker.Theta, m.Ticker.Rho,
		m.Ticker.OpenInterest, m.Ticker.Volume,
	}
	nativeFields := make([]normalize.NativeNumericField, len(nativeSources))
	for index, source := range nativeSources {
		nativeFields[index], err = tickerNativeField(source)
		if err != nil {
			return normalize.OptionSummaryV1{}, err
		}
	}
	event := normalize.OptionSummaryV1{
		Metadata: metadata, NativeSourceRole: "deribit_ticker_option",
		Instrument: normalize.TextField{State: normalize.SourceValue, Value: m.InstrumentUID, Provenance: staticProvenance},
		Underlying: normalize.TextField{State: normalize.SourceValue, Value: m.Underlying, Provenance: staticProvenance},
		Index:      normalize.TextField{State: normalize.SourceValue, Value: m.Index, Provenance: staticProvenance},
		Expiry: normalize.TimeField{
			State: normalize.SourceValue, ValueNS: m.ExpiryTimeNS,
			Resolution: normalize.ResolutionMillisecond, Provenance: staticProvenance,
		},
		Strike: normalize.NumericField{
			State: normalize.SourceValue, Value: m.Strike, Provenance: staticProvenance,
		},
		CallPut: normalize.OptionKindField{
			State: normalize.SourceValue, Value: m.CallPut, Provenance: staticProvenance,
		},
		BidPrice: numericFields[0], AskPrice: numericFields[1], LastPrice: numericFields[2], MarkPrice: numericFields[3],
		BidIV: numericFields[4], AskIV: numericFields[5], MarkIV: numericFields[6],
		Delta: nativeFields[0], Gamma: nativeFields[1], Vega: nativeFields[2], Theta: nativeFields[3], Rho: nativeFields[4],
		OpenInterest: nativeFields[5], Volume: nativeFields[6],
		ForwardPrice:    normalize.NumericField{State: normalize.SourceMissing},
		UnderlyingPrice: numericFields[7], IndexPrice: numericFields[8],
	}
	if err := event.Validate(); err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	return event, nil
}

func deribitNumericField(
	field MappedNumber,
	sourceTimeNS int64,
	receivedTimeNS int64,
	resolution normalize.TimeResolution,
) (normalize.NumericField, error) {
	provenance, err := deribitFieldProvenance(field.State, sourceTimeNS, receivedTimeNS, resolution)
	if err != nil {
		return normalize.NumericField{}, err
	}
	return normalize.NumericField{State: field.State, Value: field.Value, Provenance: provenance}, nil
}

func deribitFieldProvenance(
	state normalize.SourceState,
	sourceTimeNS int64,
	receivedTimeNS int64,
	resolution normalize.TimeResolution,
) (normalize.FieldProvenance, error) {
	switch state {
	case normalize.SourceMissing:
		return normalize.FieldProvenance{}, nil
	case normalize.SourceNull, normalize.SourceEmpty, normalize.SourceValue:
	default:
		return normalize.FieldProvenance{}, ErrInvalidRPC
	}
	if sourceTimeNS < 0 || receivedTimeNS < sourceTimeNS || resolution == normalize.ResolutionAbsent {
		return normalize.FieldProvenance{}, ErrInvalidRPC
	}
	return normalize.FieldProvenance{
		SourceTimeNS:         normalize.OptionalInt64{Value: sourceTimeNS, Valid: true},
		SourceTimeResolution: resolution,
		AgeNS:                normalize.OptionalUint64{Value: uint64(receivedTimeNS - sourceTimeNS), Valid: true},
	}, nil
}

type Funding struct {
	Channel        string
	Cadence        Cadence
	InstrumentName string
	TimestampMS    int64
	Interest       SourceNumber
	Interest8H     SourceNumber
}

type MappedFunding struct {
	Channel           string
	Cadence           Cadence
	SourceAggregation string
	InstrumentUID     string
	SourceTimeNS      int64
	Interest          MappedNumber
	Interest8H        MappedNumber
}

func ParseFunding(payload []byte) (Funding, error) {
	channel, raw, err := notificationData(payload)
	if err != nil {
		return Funding{}, ErrInvalidRPC
	}
	parts := strings.Split(channel, ".")
	if len(parts) != 3 || parts[0] != "perpetual" {
		return Funding{}, ErrInvalidRPC
	}
	cadence := Cadence(parts[2])
	if err := cadence.Validate(); err != nil {
		return Funding{}, ErrInvalidRPC
	}
	object, err := decodeObject(raw)
	if err != nil {
		return Funding{}, err
	}
	timestamp, err := requiredInt64(object, "timestamp")
	if err != nil {
		return Funding{}, err
	}
	interest, err := sourceNumber(object, "interest")
	if err != nil {
		return Funding{}, err
	}
	interest8H, err := sourceNumber(object, "interest_8h")
	if err != nil {
		return Funding{}, err
	}
	return Funding{
		Channel: channel, Cadence: cadence, InstrumentName: parts[1], TimestampMS: timestamp,
		Interest: interest, Interest8H: interest8H,
	}, nil
}

func (f Funding) Normalized(instrumentUID string) (MappedFunding, error) {
	timestampNS, err := millisecondsToNanoseconds(f.TimestampMS)
	if err != nil || instrumentUID == "" || f.Cadence.Validate() != nil ||
		f.Channel != "perpetual."+f.InstrumentName+"."+string(f.Cadence) {
		return MappedFunding{}, ErrInvalidRPC
	}
	interest, err := mappedRate(f.Interest, normalize.RateUnit())
	if err != nil {
		return MappedFunding{}, err
	}
	interest8H, err := mappedRate(f.Interest8H, normalize.RateUnit())
	if err != nil {
		return MappedFunding{}, err
	}
	return MappedFunding{
		Channel: f.Channel, Cadence: f.Cadence, SourceAggregation: sourceAggregationContract(f.Cadence),
		InstrumentUID: instrumentUID, SourceTimeNS: timestampNS, Interest: interest, Interest8H: interest8H,
	}, nil
}

type IndexObservation struct {
	Channel     string
	IndexName   string
	TimestampMS int64
	Price       SourceNumber
}

func ParseIndex(payload []byte) (IndexObservation, error) {
	channel, raw, err := notificationData(payload)
	const prefix = "deribit_price_index."
	if err != nil || !strings.HasPrefix(channel, prefix) || len(channel) == len(prefix) {
		return IndexObservation{}, ErrInvalidRPC
	}
	object, err := decodeObject(raw)
	if err != nil {
		return IndexObservation{}, err
	}
	timestamp, err := requiredInt64(object, "timestamp")
	if err != nil {
		return IndexObservation{}, err
	}
	price, err := requiredNumber(object, "price")
	if err != nil {
		return IndexObservation{}, err
	}
	return IndexObservation{Channel: channel, IndexName: strings.TrimPrefix(channel, prefix), TimestampMS: timestamp, Price: price}, nil
}

func (i IndexObservation) Normalized(baseAssetID, quoteAssetID string) (MappedNumber, int64, error) {
	timestampNS, err := millisecondsToNanoseconds(i.TimestampMS)
	if err != nil {
		return MappedNumber{}, 0, err
	}
	price, err := mappedPrice(i.Price, baseAssetID, quoteAssetID)
	return price, timestampNS, err
}
