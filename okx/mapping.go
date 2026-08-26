package okx

import (
	"fmt"
	"strconv"

	"github.com/enable-xyz/marketdata/normalize"
)

func (t Trade) Normalized(metadata normalize.Metadata, priceUnit, amountUnit normalize.Unit) (normalize.TradeV1, error) {
	sourceTimeNS := t.TimestampMS * 1_000_000
	return normalize.MapOKXTrade(metadata, normalize.OKXTradeInput{TradeID: t.TradeID, Side: t.Side, Price: normalizedNativeField(NativeField{State: normalize.SourceValue, Text: t.Price}, sourceTimeNS), Amount: normalizedNativeField(NativeField{State: normalize.SourceValue, Text: t.Size}, sourceTimeNS), PriceUnit: priceUnit, AmountUnit: amountUnit, MakerMatchCount: normalizedNativeField(t.Count, sourceTimeNS)})
}

type SpotUnitContract struct {
	Price       normalize.Unit
	BaseAmount  normalize.Unit
	QuoteAmount normalize.Unit
}

func (o MarketObservation) NormalizedSpotTicker(metadata normalize.Metadata, units SpotUnitContract) (normalize.OKXSpotTickerV1, error) {
	if o.Channel != "tickers" {
		return normalize.OKXSpotTickerV1{}, ErrAmbiguousProjection
	}
	sourceTimeNS, err := o.sourceTimeNS(metadata)
	if err != nil {
		return normalize.OKXSpotTickerV1{}, err
	}
	value := func(name string) normalize.OKXField { return normalizedNativeField(o.Fields[name], sourceTimeNS) }
	return normalize.MapOKXSpotTicker(metadata, normalize.OKXSpotTickerInput{LastPrice: value("last"), BidPrice: value("bidPx"), BidAmount: value("bidSz"), AskPrice: value("askPx"), AskAmount: value("askSz"), Open24H: value("open24h"), High24H: value("high24h"), Low24H: value("low24h"), BaseVolume24H: value("vol24h"), QuoteVolume24H: value("volCcy24h"), PriceUnit: units.Price, BaseUnit: units.BaseAmount, QuoteUnit: units.QuoteAmount})
}

type DerivativeUnitContract struct {
	Price                 normalize.Unit
	OpenInterest          normalize.NativeUnit
	OpenInterestSidedness normalize.OpenInterestSidedness
}

func (u DerivativeUnitContract) Validate(instrumentUID string) error {
	if err := u.Price.Validate(); err != nil || errNativeUnit(u.OpenInterest) != nil || u.OpenInterestSidedness == "" {
		return ErrAmbiguousProjection
	}
	if u.OpenInterest.Kind == normalize.NativeUnitContracts && u.OpenInterest.InstrumentUID != instrumentUID {
		return ErrAmbiguousProjection
	}
	return nil
}

func (o MarketObservation) NormalizedDerivative(metadata normalize.Metadata, units DerivativeUnitContract) (normalize.DerivativeTickerV1, error) {
	if err := units.Validate(metadata.InstrumentUID); err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	sourceTimeNS, err := o.sourceTimeNS(metadata)
	if err != nil {
		return normalize.DerivativeTickerV1{}, err
	}
	value := func(name string) normalize.OKXField { return normalizedNativeField(o.Fields[name], sourceTimeNS) }
	return normalize.MapOKXDerivativeTicker(metadata, normalize.OKXDerivativeInput{NativeSourceRole: "okx_v5_" + o.Channel, LastPrice: value("last"), MarkPrice: value("markPx"), IndexPrice: value("idxPx"), FundingRate: value("fundingRate"), NextFundingTime: value("nextFundingTime"), OpenInterest: value("oi"), SettlementPrice: value("settPx"), Basis: value("basis"), Premium: value("premium"), PriceUnit: units.Price, OpenInterestUnit: units.OpenInterest, OISidedness: units.OpenInterestSidedness})
}

type OptionTerms struct {
	InstrumentUID string
	UnderlyingID  string
	IndexID       string
	ExpiryMS      string
	Strike        string
	CallPut       string
	ObservedAtNS  int64
}

type OptionUnitContract struct {
	Price        normalize.Unit
	Greek        normalize.NativeUnit
	OpenInterest normalize.NativeUnit
	Volume       normalize.NativeUnit
}

func (o MarketObservation) NormalizedOption(metadata normalize.Metadata, terms OptionTerms, units OptionUnitContract) (normalize.OptionSummaryV1, error) {
	if o.Channel != "opt-summary" || terms.InstrumentUID == "" || terms.InstrumentUID != metadata.InstrumentUID || terms.UnderlyingID == "" || terms.IndexID == "" || terms.ExpiryMS == "" || terms.Strike == "" || terms.CallPut == "" || terms.ObservedAtNS < 0 {
		return normalize.OptionSummaryV1{}, ErrAmbiguousProjection
	}
	if err := units.Price.Validate(); err != nil || errNativeUnit(units.Greek) != nil || errNativeUnit(units.OpenInterest) != nil || errNativeUnit(units.Volume) != nil {
		return normalize.OptionSummaryV1{}, ErrAmbiguousProjection
	}
	sourceTimeNS, err := o.sourceTimeNS(metadata)
	if err != nil {
		return normalize.OptionSummaryV1{}, err
	}
	value := func(name string) normalize.OKXField { return normalizedNativeField(o.Fields[name], sourceTimeNS) }
	static := func(text string) normalize.OKXField {
		return normalize.OKXField{State: normalize.SourceValue, Text: text, SourceTimeNS: normalize.OptionalInt64{Value: terms.ObservedAtNS, Valid: true}, SourceTimeResolution: normalize.ResolutionNanosecond}
	}
	return normalize.MapOKXOptionSummary(metadata, normalize.OKXOptionInput{NativeSourceRole: "okx_v5_opt_summary", Instrument: static(terms.InstrumentUID), Underlying: static(terms.UnderlyingID), Index: static(terms.IndexID), Expiry: static(terms.ExpiryMS), Strike: static(terms.Strike), CallPut: static(terms.CallPut), BidPrice: value("bidPx"), AskPrice: value("askPx"), LastPrice: value("last"), MarkPrice: value("markPx"), BidIV: value("bidVol"), AskIV: value("askVol"), MarkIV: value("markVol"), Delta: value("delta"), Gamma: value("gamma"), Vega: value("vega"), Theta: value("theta"), Rho: value("rho"), OpenInterest: value("oi"), Volume: value("vol24h"), ForwardPrice: value("fwdPx"), UnderlyingPrice: value("ulyPx"), IndexPrice: value("idxPx"), PriceUnit: units.Price, GreekUnit: units.Greek, OpenInterestUnit: units.OpenInterest, VolumeUnit: units.Volume})
}

func (d LiquidationDetail) Normalized(metadata normalize.Metadata, batchID string, priceUnit normalize.Unit, amountUnit normalize.NativeUnit) (normalize.LiquidationV1, error) {
	if d.TimestampMS.State != normalize.SourceValue {
		return normalize.LiquidationV1{}, ErrAmbiguousProjection
	}
	timestampMS, err := strconv.ParseInt(d.TimestampMS.Text, 10, 64)
	if err != nil || timestampMS < 0 || timestampMS > (1<<63-1)/1_000_000 {
		return normalize.LiquidationV1{}, ErrInvalidPayload
	}
	sourceTimeNS := timestampMS * 1_000_000
	return normalize.MapOKXLiquidation(metadata, normalize.OKXLiquidationInput{Side: normalizedNativeField(d.Side, sourceTimeNS), BankruptcyPrice: normalizedNativeField(d.BankruptcyPrice, sourceTimeNS), Amount: normalizedNativeField(d.Size, sourceTimeNS), PriceUnit: priceUnit, AmountUnit: amountUnit, BatchID: batchID})
}

func (o InstrumentObservation) LifecycleState() (normalize.InstrumentLifecycleState, NativeField, error) {
	state, ok := o.Fields["state"]
	if !ok {
		return "", NativeField{State: normalize.SourceMissing}, ErrAmbiguousProjection
	}
	if state.State != normalize.SourceValue {
		return "", state, ErrAmbiguousProjection
	}
	mapped, err := normalize.OKXInstrumentState(state.Text)
	if err != nil {
		return "", state, err
	}
	return mapped, state, nil
}

func normalizedNativeField(field NativeField, sourceTimeNS int64) normalize.OKXField {
	if field.State == "" {
		field.State = normalize.SourceMissing
	}
	result := normalize.OKXField{State: field.State}
	if field.State == normalize.SourceValue {
		result.Text = field.Text
	}
	if field.State != normalize.SourceMissing {
		result.SourceTimeNS = normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}
		result.SourceTimeResolution = normalize.ResolutionMillisecond
	}
	return result
}

func (o MarketObservation) sourceTimeNS(metadata normalize.Metadata) (int64, error) {
	if o.TimestampMS.State == normalize.SourceValue {
		milliseconds, err := strconv.ParseInt(o.TimestampMS.Text, 10, 64)
		if err != nil || milliseconds < 0 || milliseconds > (1<<63-1)/1_000_000 {
			return 0, ErrInvalidPayload
		}
		return milliseconds * 1_000_000, nil
	}
	if metadata.SourceEventTimeNS.Valid {
		return metadata.SourceEventTimeNS.Value, nil
	}
	return 0, fmt.Errorf("%w: market observation has no source timestamp", ErrAmbiguousProjection)
}

func errNativeUnit(unit normalize.NativeUnit) error {
	return unit.Validate()
}
