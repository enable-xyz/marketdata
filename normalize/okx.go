package normalize

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var ErrInvalidOKXProjection = errors.New("normalize: invalid OKX V5 projection")

const OKXChecksumCutoverTimeNS = int64(1782172800000000000)

type OKXField struct {
	State                SourceState
	Text                 string
	SourceTimeNS         OptionalInt64
	SourceTimeResolution TimeResolution
}

func (f OKXField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid source state", ErrInvalidOKXProjection)
	}
	if f.State == SourceMissing {
		if f.Text != "" || f.SourceTimeNS.Valid || (f.SourceTimeResolution != "" && f.SourceTimeResolution != ResolutionAbsent) {
			return fmt.Errorf("%w: missing field carries evidence", ErrInvalidOKXProjection)
		}
		return nil
	}
	if !f.SourceTimeNS.Valid || f.SourceTimeNS.Value < 0 || f.SourceTimeResolution == ResolutionAbsent || !validTimeResolution(f.SourceTimeResolution) {
		return fmt.Errorf("%w: observed field lacks source time", ErrInvalidOKXProjection)
	}
	if f.State == SourceValue {
		if f.Text == "" || len(f.Text) > MaxDecimalInputBytes*2 || strings.IndexByte(f.Text, 0) >= 0 {
			return fmt.Errorf("%w: invalid source value", ErrInvalidOKXProjection)
		}
	} else if f.Text != "" {
		return fmt.Errorf("%w: unavailable field carries text", ErrInvalidOKXProjection)
	}
	return nil
}

func (f OKXField) provenance(receivedTimeNS int64) (FieldProvenance, error) {
	if err := f.Validate(); err != nil {
		return FieldProvenance{}, err
	}
	if f.State == SourceMissing {
		return FieldProvenance{SourceTimeResolution: ResolutionAbsent}, nil
	}
	if receivedTimeNS < f.SourceTimeNS.Value {
		return FieldProvenance{}, fmt.Errorf("%w: source time is after receive time", ErrInvalidOKXProjection)
	}
	return FieldProvenance{SourceTimeNS: f.SourceTimeNS, SourceTimeResolution: f.SourceTimeResolution, AgeNS: OptionalUint64{Value: uint64(receivedTimeNS - f.SourceTimeNS.Value), Valid: true}}, nil
}

func okxNumericField(field OKXField, unit Unit, scale uint8, receivedTimeNS int64) (NumericField, error) {
	provenance, err := field.provenance(receivedTimeNS)
	if err != nil {
		return NumericField{}, err
	}
	result := NumericField{State: field.State, Provenance: provenance}
	if field.State == SourceValue {
		decimal, err := ParseDecimal(field.Text, scale, DefaultDecimalBounds())
		if err != nil {
			return NumericField{}, fmt.Errorf("%w: %v", ErrInvalidOKXProjection, err)
		}
		result.Value = Numeric{Decimal: decimal, Unit: unit}
	}
	if err := result.Validate(); err != nil {
		return NumericField{}, err
	}
	return result, nil
}

func okxNativeNumericField(field OKXField, unit NativeUnit, scale uint8, receivedTimeNS int64) (NativeNumericField, error) {
	provenance, err := field.provenance(receivedTimeNS)
	if err != nil {
		return NativeNumericField{}, err
	}
	result := NativeNumericField{State: field.State, Provenance: provenance}
	if field.State == SourceValue {
		decimal, err := ParseDecimal(field.Text, scale, DefaultDecimalBounds())
		if err != nil {
			return NativeNumericField{}, fmt.Errorf("%w: %v", ErrInvalidOKXProjection, err)
		}
		result.Value = NativeValue{Decimal: decimal, Unit: unit}
	}
	if err := result.Validate(); err != nil {
		return NativeNumericField{}, err
	}
	return result, nil
}

func okxTimeField(field OKXField, receivedTimeNS int64) (TimeField, error) {
	provenance, err := field.provenance(receivedTimeNS)
	if err != nil {
		return TimeField{}, err
	}
	result := TimeField{State: field.State, Resolution: ResolutionAbsent, Provenance: provenance}
	if field.State == SourceValue {
		milliseconds, err := strconv.ParseInt(field.Text, 10, 64)
		if err != nil || milliseconds < 0 || milliseconds > (1<<63-1)/1_000_000 {
			return TimeField{}, fmt.Errorf("%w: invalid millisecond timestamp", ErrInvalidOKXProjection)
		}
		result.ValueNS = milliseconds * 1_000_000
		result.Resolution = ResolutionMillisecond
	}
	if err := result.Validate(); err != nil {
		return TimeField{}, err
	}
	return result, nil
}

type OKXTradeInput struct {
	TradeID         string
	Side            string
	Price           OKXField
	Amount          OKXField
	PriceUnit       Unit
	AmountUnit      Unit
	MakerMatchCount OKXField
}

func MapOKXTrade(metadata Metadata, input OKXTradeInput) (TradeV1, error) {
	if err := validateSchema(metadata, TradeSchemaName, TradeSchemaVersion); err != nil {
		return TradeV1{}, err
	}
	if err := input.MakerMatchCount.Validate(); err != nil || input.MakerMatchCount.State != SourceValue || input.MakerMatchCount.Text != "1" {
		return TradeV1{}, fmt.Errorf("%w: trades-all is not proven as one maker match", ErrInvalidOKXProjection)
	}
	tradeID, err := strconv.ParseUint(input.TradeID, 10, 64)
	if err != nil {
		return TradeV1{}, fmt.Errorf("%w: invalid trade ID", ErrInvalidOKXProjection)
	}
	price, err := okxNumericField(input.Price, input.PriceUnit, CanonicalPriceScale, metadata.ReceivedTimeNS)
	if err != nil || price.State != SourceValue {
		return TradeV1{}, fmt.Errorf("%w: trade price", ErrInvalidOKXProjection)
	}
	amount, err := okxNumericField(input.Amount, input.AmountUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	if err != nil || amount.State != SourceValue {
		return TradeV1{}, fmt.Errorf("%w: trade amount", ErrInvalidOKXProjection)
	}
	side := Side(input.Side)
	if side != SideBuy && side != SideSell {
		return TradeV1{}, fmt.Errorf("%w: trade side", ErrInvalidOKXProjection)
	}
	event := TradeV1{Metadata: metadata, NativeTradeID: tradeID, AggressorSide: side, Price: price.Value, Amount: amount.Value, AggregationKind: AggregationSingleMatch, NativeDuplicateStatus: DuplicateUnassessed}
	if err := event.Validate(); err != nil {
		return TradeV1{}, err
	}
	return event, nil
}

type OKXBookLiquidityKind string

const (
	OKXBookRegularLiquidity OKXBookLiquidityKind = "regular"
	OKXBookRPILiquidity     OKXBookLiquidityKind = "rpi"
)

// OKXBookUpdateV1 is the OKX source projection for BookUpdateV1 semantics. The
// shared v1 Row validator remains Binance-Spot-specific, so this type refuses to
// masquerade as that contract while retaining signed reset sequences and raw checksum.
type OKXBookUpdateV1 struct {
	Metadata            Metadata
	UpdateKind          string
	DepthContract       string
	CadenceContract     string
	LiquidityKind       OKXBookLiquidityKind
	SourceTimeNS        int64
	PreviousSequence    int64
	Sequence            int64
	RawChecksum         int32
	ChecksumState       SourceState
	Bids                []BookLevel
	Asks                []BookLevel
	RPIIncluded         bool
	Reconstructable     bool
	SnapshotReplacement bool
}

type OKXBookLevelInput struct {
	Price  string
	Amount string
}

type OKXBookInput struct {
	Action           string
	DepthContract    string
	SourceTimeNS     int64
	PreviousSequence int64
	Sequence         int64
	RawChecksum      int32
	Bids             []OKXBookLevelInput
	Asks             []OKXBookLevelInput
	PriceUnit        Unit
	AmountUnit       Unit
}

func MapOKXBook(metadata Metadata, input OKXBookInput) (OKXBookUpdateV1, error) {
	if err := validateSchema(metadata, BookUpdateSchemaName, BookUpdateSchemaVersion); err != nil || (input.Action != "snapshot" && input.Action != "update") || input.SourceTimeNS < 0 || input.Sequence < 0 || input.PreviousSequence < -1 {
		return OKXBookUpdateV1{}, fmt.Errorf("%w: book identity", ErrInvalidOKXProjection)
	}
	cadence := ""
	switch input.DepthContract {
	case "books":
		cadence = "100ms"
	case "books5":
		if input.Action != "snapshot" || len(input.Bids) > 5 || len(input.Asks) > 5 {
			return OKXBookUpdateV1{}, fmt.Errorf("%w: books5 must be a five-level snapshot replacement", ErrInvalidOKXProjection)
		}
		cadence = "100ms_snapshot_replacement"
	case "books50-l2-tbt", "books-l2-tbt":
		cadence = "10ms_vip4"
	case "books-rpi-tbt":
		cadence = "native_rpi_separate"
	default:
		return OKXBookUpdateV1{}, fmt.Errorf("%w: unsupported book contract", ErrInvalidOKXProjection)
	}
	bids, err := mapOKXLevels(input.Bids, SideBuy, input.PriceUnit, input.AmountUnit)
	if err != nil {
		return OKXBookUpdateV1{}, err
	}
	asks, err := mapOKXLevels(input.Asks, SideSell, input.PriceUnit, input.AmountUnit)
	if err != nil {
		return OKXBookUpdateV1{}, err
	}
	checksumState := SourceValue
	if input.SourceTimeNS >= OKXChecksumCutoverTimeNS {
		if input.RawChecksum != 0 {
			return OKXBookUpdateV1{}, fmt.Errorf("%w: post-cutover checksum field is not documented zero", ErrInvalidOKXProjection)
		}
		checksumState = SourceMissing
	}
	reconstructable := input.DepthContract == "books" || input.DepthContract == "books50-l2-tbt" || input.DepthContract == "books-l2-tbt" || input.DepthContract == "books-rpi-tbt"
	liquidityKind := OKXBookRegularLiquidity
	if input.DepthContract == "books-rpi-tbt" {
		liquidityKind = OKXBookRPILiquidity
	}
	return OKXBookUpdateV1{Metadata: metadata, UpdateKind: input.Action, DepthContract: input.DepthContract, CadenceContract: cadence, LiquidityKind: liquidityKind, SourceTimeNS: input.SourceTimeNS, PreviousSequence: input.PreviousSequence, Sequence: input.Sequence, RawChecksum: input.RawChecksum, ChecksumState: checksumState, Bids: bids, Asks: asks, RPIIncluded: input.DepthContract == "books-rpi-tbt", Reconstructable: reconstructable, SnapshotReplacement: input.DepthContract == "books5"}, nil
}

func mapOKXLevels(inputs []OKXBookLevelInput, side Side, priceUnit, amountUnit Unit) ([]BookLevel, error) {
	if len(inputs) > 400 || priceUnit.Kind != UnitQuotePerBase || amountUnit.Kind != UnitBaseAssetAmount || priceUnit.BaseAssetID != amountUnit.AssetID {
		return nil, fmt.Errorf("%w: book units or depth", ErrInvalidOKXProjection)
	}
	levels := make([]BookLevel, len(inputs))
	for index, input := range inputs {
		price, err := ParseDecimal(input.Price, CanonicalPriceScale, DefaultDecimalBounds())
		if err != nil {
			return nil, err
		}
		amount, err := ParseDecimal(input.Amount, CanonicalAmountScale, DefaultDecimalBounds())
		if err != nil {
			return nil, err
		}
		action := LevelUpsert
		if amount.IsZero() {
			action = LevelDelete
		}
		levels[index] = BookLevel{Side: side, LevelOrdinal: uint32(index), Action: action, Price: Numeric{Decimal: price, Unit: priceUnit}, Amount: Numeric{Decimal: amount, Unit: amountUnit}}
	}
	return levels, nil
}

type OKXQuoteV1 struct {
	Metadata         Metadata
	NativeSourceRole string
	BidPrice         NumericField
	BidAmount        NumericField
	AskPrice         NumericField
	AskAmount        NumericField
	RPIState         SourceState
}

func MapOKXQuote(metadata Metadata, bidPrice, bidAmount, askPrice, askAmount OKXField, priceUnit, amountUnit Unit) (OKXQuoteV1, error) {
	if err := validateSchema(metadata, QuoteSchemaName, QuoteSchemaVersion); err != nil {
		return OKXQuoteV1{}, err
	}
	values := make([]NumericField, 4)
	var err error
	values[0], err = okxNumericField(bidPrice, priceUnit, CanonicalPriceScale, metadata.ReceivedTimeNS)
	if err == nil {
		values[1], err = okxNumericField(bidAmount, amountUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	}
	if err == nil {
		values[2], err = okxNumericField(askPrice, priceUnit, CanonicalPriceScale, metadata.ReceivedTimeNS)
	}
	if err == nil {
		values[3], err = okxNumericField(askAmount, amountUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	}
	if err != nil {
		return OKXQuoteV1{}, err
	}
	for _, value := range values {
		if value.State != SourceValue {
			return OKXQuoteV1{}, fmt.Errorf("%w: incomplete BBO", ErrInvalidOKXProjection)
		}
	}
	if priceUnit.Kind != UnitQuotePerBase || amountUnit.Kind != UnitBaseAssetAmount || priceUnit.BaseAssetID != amountUnit.AssetID {
		return OKXQuoteV1{}, fmt.Errorf("%w: BBO units", ErrInvalidOKXProjection)
	}
	return OKXQuoteV1{Metadata: metadata, NativeSourceRole: "okx_v5_bbo_tbt", BidPrice: values[0], BidAmount: values[1], AskPrice: values[2], AskAmount: values[3], RPIState: SourceMissing}, nil
}

type OKXSpotTickerV1 struct {
	Metadata         Metadata
	NativeSourceRole string
	LastPrice        NumericField
	BidPrice         NumericField
	BidAmount        NumericField
	AskPrice         NumericField
	AskAmount        NumericField
	Open24H          NumericField
	High24H          NumericField
	Low24H           NumericField
	BaseVolume24H    NumericField
	QuoteVolume24H   NumericField
}

type OKXSpotTickerInput struct {
	LastPrice      OKXField
	BidPrice       OKXField
	BidAmount      OKXField
	AskPrice       OKXField
	AskAmount      OKXField
	Open24H        OKXField
	High24H        OKXField
	Low24H         OKXField
	BaseVolume24H  OKXField
	QuoteVolume24H OKXField
	PriceUnit      Unit
	BaseUnit       Unit
	QuoteUnit      Unit
}

func MapOKXSpotTicker(metadata Metadata, input OKXSpotTickerInput) (OKXSpotTickerV1, error) {
	if err := validateSchema(metadata, TickerSchemaName, TickerSchemaVersion); err != nil {
		return OKXSpotTickerV1{}, err
	}
	if input.PriceUnit.Kind != UnitQuotePerBase || input.BaseUnit.Kind != UnitBaseAssetAmount || input.QuoteUnit.Kind != UnitQuoteAssetAmount ||
		input.PriceUnit.BaseAssetID != input.BaseUnit.AssetID || input.PriceUnit.QuoteAssetID != input.QuoteUnit.AssetID {
		return OKXSpotTickerV1{}, fmt.Errorf("%w: spot ticker units", ErrInvalidOKXProjection)
	}
	mapPrice := func(field OKXField) (NumericField, error) {
		return okxNumericField(field, input.PriceUnit, CanonicalPriceScale, metadata.ReceivedTimeNS)
	}
	last, err := mapPrice(input.LastPrice)
	if err != nil || last.State != SourceValue {
		return OKXSpotTickerV1{}, fmt.Errorf("%w: spot last price", ErrInvalidOKXProjection)
	}
	bid, err := mapPrice(input.BidPrice)
	if err != nil {
		return OKXSpotTickerV1{}, err
	}
	bidAmount, err := okxNumericField(input.BidAmount, input.BaseUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	if err != nil {
		return OKXSpotTickerV1{}, err
	}
	ask, err := mapPrice(input.AskPrice)
	if err != nil {
		return OKXSpotTickerV1{}, err
	}
	askAmount, err := okxNumericField(input.AskAmount, input.BaseUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	if err != nil {
		return OKXSpotTickerV1{}, err
	}
	open, err := mapPrice(input.Open24H)
	if err != nil {
		return OKXSpotTickerV1{}, err
	}
	high, err := mapPrice(input.High24H)
	if err != nil {
		return OKXSpotTickerV1{}, err
	}
	low, err := mapPrice(input.Low24H)
	if err != nil {
		return OKXSpotTickerV1{}, err
	}
	baseVolume, err := okxNumericField(input.BaseVolume24H, input.BaseUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	if err != nil {
		return OKXSpotTickerV1{}, err
	}
	quoteVolume, err := okxNumericField(input.QuoteVolume24H, input.QuoteUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	if err != nil {
		return OKXSpotTickerV1{}, err
	}
	return OKXSpotTickerV1{Metadata: metadata, NativeSourceRole: "okx_v5_tickers_spot_24h", LastPrice: last, BidPrice: bid, BidAmount: bidAmount, AskPrice: ask, AskAmount: askAmount, Open24H: open, High24H: high, Low24H: low, BaseVolume24H: baseVolume, QuoteVolume24H: quoteVolume}, nil
}

type OKXDerivativeInput struct {
	NativeSourceRole string
	LastPrice        OKXField
	MarkPrice        OKXField
	IndexPrice       OKXField
	FundingRate      OKXField
	NextFundingTime  OKXField
	OpenInterest     OKXField
	SettlementPrice  OKXField
	Basis            OKXField
	Premium          OKXField
	PriceUnit        Unit
	OpenInterestUnit NativeUnit
	OISidedness      OpenInterestSidedness
}

func MapOKXDerivativeTicker(metadata Metadata, input OKXDerivativeInput) (DerivativeTickerV1, error) {
	if err := validateSchema(metadata, DerivativeTickerSchemaName, DerivativeTickerSchemaVersion); err != nil || input.NativeSourceRole == "" {
		return DerivativeTickerV1{}, fmt.Errorf("%w: derivative identity", ErrInvalidOKXProjection)
	}
	mapPrice := func(field OKXField) (NumericField, error) {
		return okxNumericField(field, input.PriceUnit, CanonicalPriceScale, metadata.ReceivedTimeNS)
	}
	last, err := mapPrice(input.LastPrice)
	if err != nil {
		return DerivativeTickerV1{}, err
	}
	mark, err := mapPrice(input.MarkPrice)
	if err != nil {
		return DerivativeTickerV1{}, err
	}
	index, err := mapPrice(input.IndexPrice)
	if err != nil {
		return DerivativeTickerV1{}, err
	}
	funding, err := okxNumericField(input.FundingRate, RateUnit(), CanonicalPriceScale, metadata.ReceivedTimeNS)
	if err != nil {
		return DerivativeTickerV1{}, err
	}
	nextFunding, err := okxTimeField(input.NextFundingTime, metadata.ReceivedTimeNS)
	if err != nil {
		return DerivativeTickerV1{}, err
	}
	settlement, err := mapPrice(input.SettlementPrice)
	if err != nil {
		return DerivativeTickerV1{}, err
	}
	basis, err := mapPrice(input.Basis)
	if err != nil {
		return DerivativeTickerV1{}, err
	}
	premium, err := okxNumericField(input.Premium, RateUnit(), CanonicalPriceScale, metadata.ReceivedTimeNS)
	if err != nil {
		return DerivativeTickerV1{}, err
	}
	oi := make([]OpenInterestObservation, 0, 1)
	if input.OpenInterest.State != SourceMissing {
		field, err := okxNativeNumericField(input.OpenInterest, input.OpenInterestUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
		if err != nil {
			return DerivativeTickerV1{}, err
		}
		missingDerived := DerivedValue{Field: NumericField{State: SourceMissing, Provenance: FieldProvenance{SourceTimeResolution: ResolutionAbsent}}}
		observation := OpenInterestObservation{State: field.State, Variant: "oi", Sidedness: input.OISidedness, Provenance: field.Provenance, DerivedBase: missingDerived, DerivedQuote: missingDerived, DerivedUSD: missingDerived}
		if field.State == SourceValue {
			observation.Native = field.Value
		} else {
			observation.Sidedness = ""
		}
		oi = append(oi, observation)
	}
	event := DerivativeTickerV1{Metadata: metadata, NativeSourceRole: input.NativeSourceRole, LastPrice: last, MarkPrice: mark, IndexPrice: index, FundingRate: funding, NextFundingTime: nextFunding, OpenInterest: oi, SettlementPrice: settlement, Basis: basis, Premium: premium}
	if err := event.Validate(); err != nil {
		return DerivativeTickerV1{}, err
	}
	return event, nil
}

type OKXOptionInput struct {
	NativeSourceRole string
	Instrument       OKXField
	Underlying       OKXField
	Index            OKXField
	Expiry           OKXField
	Strike           OKXField
	CallPut          OKXField
	BidPrice         OKXField
	AskPrice         OKXField
	LastPrice        OKXField
	MarkPrice        OKXField
	BidIV            OKXField
	AskIV            OKXField
	MarkIV           OKXField
	Delta            OKXField
	Gamma            OKXField
	Vega             OKXField
	Theta            OKXField
	Rho              OKXField
	OpenInterest     OKXField
	Volume           OKXField
	ForwardPrice     OKXField
	UnderlyingPrice  OKXField
	IndexPrice       OKXField
	PriceUnit        Unit
	GreekUnit        NativeUnit
	OpenInterestUnit NativeUnit
	VolumeUnit       NativeUnit
}

func MapOKXOptionSummary(metadata Metadata, input OKXOptionInput) (OptionSummaryV1, error) {
	if err := validateSchema(metadata, OptionSummarySchemaName, OptionSummarySchemaVersion); err != nil || input.NativeSourceRole == "" {
		return OptionSummaryV1{}, fmt.Errorf("%w: option identity", ErrInvalidOKXProjection)
	}
	text := func(field OKXField) (TextField, error) {
		provenance, err := field.provenance(metadata.ReceivedTimeNS)
		if err != nil {
			return TextField{}, err
		}
		result := TextField{State: field.State, Value: field.Text, Provenance: provenance}
		if field.State != SourceValue {
			result.Value = ""
		}
		return result, result.Validate()
	}
	instrument, err := text(input.Instrument)
	if err != nil {
		return OptionSummaryV1{}, err
	}
	underlying, err := text(input.Underlying)
	if err != nil {
		return OptionSummaryV1{}, err
	}
	indexIdentity, err := text(input.Index)
	if err != nil {
		return OptionSummaryV1{}, err
	}
	expiry, err := okxTimeField(input.Expiry, metadata.ReceivedTimeNS)
	if err != nil {
		return OptionSummaryV1{}, err
	}
	price := func(field OKXField) (NumericField, error) {
		return okxNumericField(field, input.PriceUnit, CanonicalPriceScale, metadata.ReceivedTimeNS)
	}
	strike, err := price(input.Strike)
	if err != nil {
		return OptionSummaryV1{}, err
	}
	callPutProvenance, err := input.CallPut.provenance(metadata.ReceivedTimeNS)
	if err != nil {
		return OptionSummaryV1{}, err
	}
	callPut := OptionKindField{State: input.CallPut.State, Provenance: callPutProvenance}
	if input.CallPut.State == SourceValue {
		switch input.CallPut.Text {
		case "C", "call":
			callPut.Value = OptionCall
		case "P", "put":
			callPut.Value = OptionPut
		default:
			return OptionSummaryV1{}, fmt.Errorf("%w: option type", ErrInvalidOKXProjection)
		}
	}
	mappedPrices := make([]NumericField, 10)
	for ordinal, field := range []OKXField{input.BidPrice, input.AskPrice, input.LastPrice, input.MarkPrice, input.ForwardPrice, input.UnderlyingPrice, input.IndexPrice} {
		mappedPrices[ordinal], err = price(field)
		if err != nil {
			return OptionSummaryV1{}, err
		}
	}
	for ordinal, field := range []OKXField{input.BidIV, input.AskIV, input.MarkIV} {
		mappedPrices[7+ordinal], err = okxNumericField(field, ImpliedVolatilityUnit(), CanonicalPriceScale, metadata.ReceivedTimeNS)
		if err != nil {
			return OptionSummaryV1{}, err
		}
	}
	greeks := make([]NativeNumericField, 5)
	for ordinal, field := range []OKXField{input.Delta, input.Gamma, input.Vega, input.Theta, input.Rho} {
		greeks[ordinal], err = okxNativeNumericField(field, input.GreekUnit, CanonicalPriceScale, metadata.ReceivedTimeNS)
		if err != nil {
			return OptionSummaryV1{}, err
		}
	}
	oi, err := okxNativeNumericField(input.OpenInterest, input.OpenInterestUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	if err != nil {
		return OptionSummaryV1{}, err
	}
	volume, err := okxNativeNumericField(input.Volume, input.VolumeUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	if err != nil {
		return OptionSummaryV1{}, err
	}
	event := OptionSummaryV1{Metadata: metadata, NativeSourceRole: input.NativeSourceRole, Instrument: instrument, Underlying: underlying, Index: indexIdentity, Expiry: expiry, Strike: strike, CallPut: callPut,
		BidPrice: mappedPrices[0], AskPrice: mappedPrices[1], LastPrice: mappedPrices[2], MarkPrice: mappedPrices[3], ForwardPrice: mappedPrices[4], UnderlyingPrice: mappedPrices[5], IndexPrice: mappedPrices[6],
		BidIV: mappedPrices[7], AskIV: mappedPrices[8], MarkIV: mappedPrices[9], Delta: greeks[0], Gamma: greeks[1], Vega: greeks[2], Theta: greeks[3], Rho: greeks[4], OpenInterest: oi, Volume: volume}
	if err := event.Validate(); err != nil {
		return OptionSummaryV1{}, err
	}
	return event, nil
}

type OKXLiquidationInput struct {
	Side            OKXField
	BankruptcyPrice OKXField
	Amount          OKXField
	PriceUnit       Unit
	AmountUnit      NativeUnit
	BatchID         string
}

func MapOKXLiquidation(metadata Metadata, input OKXLiquidationInput) (LiquidationV1, error) {
	if err := validateSchema(metadata, LiquidationSchemaName, LiquidationSchemaVersion); err != nil || input.Side.State != SourceValue || input.BankruptcyPrice.State != SourceValue || input.Amount.State != SourceValue || input.BatchID == "" {
		return LiquidationV1{}, fmt.Errorf("%w: incomplete liquidation evidence", ErrInvalidOKXProjection)
	}
	var side Side
	switch input.Side.Text {
	case "buy":
		side = SideBuy
	case "sell":
		side = SideSell
	default:
		return LiquidationV1{}, fmt.Errorf("%w: liquidation side", ErrInvalidOKXProjection)
	}
	price, err := okxNumericField(input.BankruptcyPrice, input.PriceUnit, CanonicalPriceScale, metadata.ReceivedTimeNS)
	if err != nil {
		return LiquidationV1{}, err
	}
	amount, err := okxNativeNumericField(input.Amount, input.AmountUnit, CanonicalAmountScale, metadata.ReceivedTimeNS)
	if err != nil {
		return LiquidationV1{}, err
	}
	event := LiquidationV1{Metadata: metadata, NativeSourceRole: "okx_v5_liquidation_orders", NativeRole: LiquidationNativeEvent, Side: side, SideSemantics: LiquidationOrderSide, Amount: amount.Value, Price: price, PriceType: LiquidationBankruptcyPrice, Completeness: LiquidationPartialNonchronological, Window: LiquidationWindow{Selection: LiquidationWindowSelectionUnknown, BatchID: input.BatchID}}
	if err := event.Validate(); err != nil {
		return LiquidationV1{}, err
	}
	return event, nil
}

func OKXInstrumentState(native string) (InstrumentLifecycleState, error) {
	switch native {
	case "preopen":
		return InstrumentStatePreListing, nil
	case "live":
		return InstrumentStateContinuousTrading, nil
	case "suspend":
		return InstrumentStateSuspended, nil
	case "expired":
		return InstrumentStateExpired, nil
	default:
		return "", fmt.Errorf("%w: unknown native lifecycle state %q", ErrInvalidOKXProjection, native)
	}
}

type OKXLifecycleInput struct {
	MetadataGeneration Uint64Field
	NativeStateBefore  InstrumentStateField
	NativeStateAfter   InstrumentStateField
	ListingTime        TimeField
	ContinuousTime     TimeField
	ExpiryTime         TimeField
	DeliveryTime       TimeField
	DelistingTime      TimeField
	TickSize           NumericChange
	LotSize            NativeNumericChange
	ContractMultiplier NativeNumericChange
	Payoff             TextChange
	OldRawHash         HashField
	NewRawHash         HashField
	ResolutionStatus   InstrumentResolutionField
}

func MapOKXLifecycle(metadata Metadata, input OKXLifecycleInput) (InstrumentEventV1, error) {
	event := InstrumentEventV1{Metadata: metadata, MetadataGeneration: input.MetadataGeneration, NativeStateBefore: input.NativeStateBefore, NativeStateAfter: input.NativeStateAfter,
		ListingTime: input.ListingTime, ContinuousTradingTime: input.ContinuousTime, ExpiryTime: input.ExpiryTime, DeliveryTime: input.DeliveryTime, DelistingTime: input.DelistingTime,
		TickSize: input.TickSize, LotSize: input.LotSize, ContractMultiplier: input.ContractMultiplier, Payoff: input.Payoff, OldRawHash: input.OldRawHash, NewRawHash: input.NewRawHash, ResolutionStatus: input.ResolutionStatus}
	if err := event.Validate(); err != nil {
		return InstrumentEventV1{}, fmt.Errorf("%w: %v", ErrInvalidOKXProjection, err)
	}
	return event, nil
}
