package bybit

import (
	"fmt"
	"strings"

	"github.com/enable-xyz/marketdata/normalize"
)

type OptionTickerFields struct {
	BidPrice        NativeField
	AskPrice        NativeField
	LastPrice       NativeField
	MarkPrice       NativeField
	IndexPrice      NativeField
	BidIV           NativeField
	AskIV           NativeField
	MarkIV          NativeField
	Delta           NativeField
	Gamma           NativeField
	Vega            NativeField
	Theta           NativeField
	Rho             NativeField
	OpenInterest    NativeField
	Volume          NativeField
	ForwardPrice    NativeField
	UnderlyingPrice NativeField
}

type OptionTickerSnapshot struct {
	Topic                 string
	BaseCoin              string
	Symbol                string
	SystemTimeNS          int64
	ForwardPriceFieldName string
	Fields                OptionTickerFields
}

func ParseOptionTickerSnapshot(payload []byte) (OptionTickerSnapshot, error) {
	if err := validateOptionPayload(payload, optionWSPayloadPolicy()); err != nil {
		return OptionTickerSnapshot{}, err
	}
	message, err := parseTickerPayload(payload)
	if err != nil {
		return OptionTickerSnapshot{}, err
	}
	if message.Type != "snapshot" {
		return OptionTickerSnapshot{}, fmt.Errorf("%w: option ticker is snapshot-only", ErrInvalidPayload)
	}
	const prefix = "tickers."
	if !stringsHasPrefixExact(message.Topic, prefix) {
		return OptionTickerSnapshot{}, ErrInvalidPayload
	}
	baseCoin := message.Topic[len(prefix):]
	identity, ok := parseOptionSymbol(message.Symbol)
	if !validBaseCoin(baseCoin) || !ok || identity.base != baseCoin {
		return OptionTickerSnapshot{}, fmt.Errorf("%w: option ticker topic is not its base coin", ErrInvalidPayload)
	}
	missing := NativeField{State: normalize.SourceMissing}
	fields := OptionTickerFields{
		BidPrice: missing, AskPrice: missing, LastPrice: missing, MarkPrice: missing, IndexPrice: missing,
		BidIV: missing, AskIV: missing, MarkIV: missing,
		Delta: missing, Gamma: missing, Vega: missing, Theta: missing, Rho: missing,
		OpenInterest: missing, Volume: missing, ForwardPrice: missing, UnderlyingPrice: missing,
	}
	destinations := []struct {
		name  string
		field *NativeField
	}{
		{"bidPrice", &fields.BidPrice}, {"askPrice", &fields.AskPrice}, {"lastPrice", &fields.LastPrice},
		{"markPrice", &fields.MarkPrice}, {"indexPrice", &fields.IndexPrice},
		{"bidIv", &fields.BidIV}, {"askIv", &fields.AskIV}, {"markIv", &fields.MarkIV},
		{"delta", &fields.Delta}, {"gamma", &fields.Gamma}, {"vega", &fields.Vega}, {"theta", &fields.Theta}, {"rho", &fields.Rho},
		{"openInterest", &fields.OpenInterest}, {"volume24h", &fields.Volume}, {"underlyingPrice", &fields.UnderlyingPrice},
	}
	for _, destination := range destinations {
		if err := applyTickerField(message.Data, destination.name, message.TSNS, destination.field); err != nil {
			return OptionTickerSnapshot{}, fmt.Errorf("%w: option ticker field %s type drift", ErrInvalidPayload, destination.name)
		}
		if destination.field.State == normalize.SourceValue && !validDecimalText(destination.field.Text) {
			return OptionTickerSnapshot{}, fmt.Errorf("%w: option ticker field %s is not decimal", ErrInvalidPayload, destination.name)
		}
	}
	forwardFieldName := ""
	forwardPresent := false
	for _, name := range []string{"forwardPrice", "predictedDeliveryPrice"} {
		field := NativeField{State: normalize.SourceMissing}
		if err := applyTickerField(message.Data, name, message.TSNS, &field); err != nil {
			return OptionTickerSnapshot{}, fmt.Errorf("%w: option ticker field %s type drift", ErrInvalidPayload, name)
		}
		if field.State != normalize.SourceMissing {
			if forwardPresent || (field.State == normalize.SourceValue && !validDecimalText(field.Text)) {
				return OptionTickerSnapshot{}, fmt.Errorf("%w: ambiguous or malformed option forward price", ErrInvalidPayload)
			}
			forwardPresent = true
			forwardFieldName = name
			fields.ForwardPrice = field
		}
	}
	return OptionTickerSnapshot{
		Topic: message.Topic, BaseCoin: baseCoin, Symbol: message.Symbol, SystemTimeNS: message.TSNS,
		ForwardPriceFieldName: forwardFieldName, Fields: fields,
	}, nil
}

type OptionIdentities struct {
	InstrumentUID string
	UnderlyingID  string
	IndexID       string
}

func (i OptionIdentities) Validate() error {
	for _, identity := range []string{i.InstrumentUID, i.UnderlyingID, i.IndexID} {
		if identity == "" || len(identity) > normalize.MaxOptionIdentityBytes || strings.IndexByte(identity, 0) >= 0 {
			return ErrInvalidPayload
		}
	}
	return nil
}

type OptionUnitContract struct {
	PremiumPrice   normalize.Unit
	ReferencePrice normalize.Unit
	OpenInterest   normalize.NativeUnit
	Volume         normalize.NativeUnit
	Delta          normalize.NativeUnit
	Gamma          normalize.NativeUnit
	Vega           normalize.NativeUnit
	Theta          normalize.NativeUnit
	Rho            normalize.NativeUnit
}

func (c OptionUnitContract) Validate() error {
	if err := c.PremiumPrice.Validate(); err != nil {
		return err
	}
	if err := c.ReferencePrice.Validate(); err != nil {
		return err
	}
	for _, unit := range []normalize.NativeUnit{c.OpenInterest, c.Volume, c.Delta, c.Gamma, c.Vega, c.Theta, c.Rho} {
		if err := unit.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s OptionTickerSnapshot) Normalized(metadata normalize.Metadata, instrument OptionInstrument, instrumentObservedTimeNS int64, identities OptionIdentities, units OptionUnitContract) (normalize.OptionSummaryV1, error) {
	if s.Symbol == "" || s.Symbol != instrument.Symbol || s.BaseCoin != instrument.BaseCoin ||
		metadata.SourceID != OptionSourceID || metadata.ChannelID != s.Topic ||
		metadata.InstrumentUID != identities.InstrumentUID {
		return normalize.OptionSummaryV1{}, ErrInvalidPayload
	}
	if err := identities.Validate(); err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	if err := units.Validate(); err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	if units.OpenInterest.Kind != normalize.NativeUnitContracts || units.OpenInterest.InstrumentUID != identities.InstrumentUID ||
		units.Volume.Kind != normalize.NativeUnitContracts || units.Volume.InstrumentUID != identities.InstrumentUID {
		return normalize.OptionSummaryV1{}, ErrInvalidPayload
	}
	instrumentField, err := makeOptionTextField(identities.InstrumentUID, instrumentObservedTimeNS, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	underlyingField, err := makeOptionTextField(identities.UnderlyingID, instrumentObservedTimeNS, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	indexField, err := makeOptionTextField(identities.IndexID, instrumentObservedTimeNS, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	expiryNS, err := instrument.ExpiryTimeNS()
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	expiry, err := makeOptionTimeField(expiryNS, instrumentObservedTimeNS, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	strike, err := makeOptionStaticNumericField(instrument.StrikePrice, units.ReferencePrice, instrumentObservedTimeNS, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	callPut, err := makeOptionKindField(instrument.Kind(), instrumentObservedTimeNS, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	bid, err := makeNumericField(s.Fields.BidPrice, units.PremiumPrice, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	ask, err := makeNumericField(s.Fields.AskPrice, units.PremiumPrice, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	last, err := makeNumericField(s.Fields.LastPrice, units.PremiumPrice, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	mark, err := makeNumericField(s.Fields.MarkPrice, units.PremiumPrice, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	bidIV, err := makeNumericField(s.Fields.BidIV, normalize.RateUnit(), metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	askIV, err := makeNumericField(s.Fields.AskIV, normalize.RateUnit(), metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	markIV, err := makeNumericField(s.Fields.MarkIV, normalize.RateUnit(), metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	forward, err := makeNumericField(s.Fields.ForwardPrice, units.ReferencePrice, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	underlying, err := makeNumericField(s.Fields.UnderlyingPrice, units.ReferencePrice, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	indexPrice, err := makeNumericField(s.Fields.IndexPrice, units.ReferencePrice, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	delta, err := makeOptionNativeNumericField(s.Fields.Delta, units.Delta, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	gamma, err := makeOptionNativeNumericField(s.Fields.Gamma, units.Gamma, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	vega, err := makeOptionNativeNumericField(s.Fields.Vega, units.Vega, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	theta, err := makeOptionNativeNumericField(s.Fields.Theta, units.Theta, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	rho, err := makeOptionNativeNumericField(s.Fields.Rho, units.Rho, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	oi, err := makeOptionNativeNumericField(s.Fields.OpenInterest, units.OpenInterest, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	volume, err := makeOptionNativeNumericField(s.Fields.Volume, units.Volume, metadata.ReceivedTimeNS)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	event := normalize.OptionSummaryV1{
		Metadata: metadata, NativeSourceRole: "bybit_v5_option_ticker_snapshot",
		Instrument: instrumentField, Underlying: underlyingField, Index: indexField, Expiry: expiry, Strike: strike, CallPut: callPut,
		BidPrice: bid, AskPrice: ask, LastPrice: last, MarkPrice: mark, BidIV: bidIV, AskIV: askIV, MarkIV: markIV,
		Delta: delta, Gamma: gamma, Vega: vega, Theta: theta, Rho: rho, OpenInterest: oi, Volume: volume,
		ForwardPrice: forward, UnderlyingPrice: underlying, IndexPrice: indexPrice,
	}
	if err := event.Validate(); err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	return event, nil
}

func makeOptionTextField(value string, sourceTimeNS, receivedTimeNS int64) (normalize.TextField, error) {
	provenance, err := makeFieldProvenance(sourceTimeNS, receivedTimeNS)
	if err != nil {
		return normalize.TextField{}, err
	}
	field := normalize.TextField{State: normalize.SourceValue, Value: value, Provenance: provenance}
	return field, field.Validate()
}

func makeOptionTimeField(valueNS, sourceTimeNS, receivedTimeNS int64) (normalize.TimeField, error) {
	provenance, err := makeFieldProvenance(sourceTimeNS, receivedTimeNS)
	if err != nil {
		return normalize.TimeField{}, err
	}
	field := normalize.TimeField{State: normalize.SourceValue, ValueNS: valueNS, Resolution: normalize.ResolutionMillisecond, Provenance: provenance}
	return field, field.Validate()
}

func makeOptionStaticNumericField(text string, unit normalize.Unit, sourceTimeNS, receivedTimeNS int64) (normalize.NumericField, error) {
	provenance, err := makeFieldProvenance(sourceTimeNS, receivedTimeNS)
	if err != nil {
		return normalize.NumericField{}, err
	}
	decimal, err := normalize.ParseDecimal(text, normalize.CanonicalPriceScale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.NumericField{}, err
	}
	field := normalize.NumericField{State: normalize.SourceValue, Value: normalize.Numeric{Decimal: decimal, Unit: unit}, Provenance: provenance}
	return field, field.Validate()
}

func makeOptionKindField(kind normalize.OptionKind, sourceTimeNS, receivedTimeNS int64) (normalize.OptionKindField, error) {
	provenance, err := makeFieldProvenance(sourceTimeNS, receivedTimeNS)
	if err != nil {
		return normalize.OptionKindField{}, err
	}
	field := normalize.OptionKindField{State: normalize.SourceValue, Value: kind, Provenance: provenance}
	return field, field.Validate()
}

func makeOptionNativeNumericField(field NativeField, unit normalize.NativeUnit, receivedTimeNS int64) (normalize.NativeNumericField, error) {
	result := normalize.NativeNumericField{State: field.State}
	if field.State != normalize.SourceMissing {
		provenance, err := makeFieldProvenance(field.SourceTimeNS, receivedTimeNS)
		if err != nil {
			return normalize.NativeNumericField{}, err
		}
		result.Provenance = provenance
	}
	if field.State != normalize.SourceValue {
		return result, result.Validate()
	}
	decimal, err := normalize.ParseDecimal(field.Text, normalize.CanonicalAmountScale, normalize.DefaultDecimalBounds())
	if err != nil {
		return normalize.NativeNumericField{}, err
	}
	result.Value = normalize.NativeValue{Decimal: decimal, Unit: unit}
	return result, result.Validate()
}
