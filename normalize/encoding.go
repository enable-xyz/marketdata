package normalize

import (
	"crypto/sha256"
	"encoding/binary"
)

type canonicalEncoder struct {
	bytes []byte
}

func (e *canonicalEncoder) u8(value uint8) { e.bytes = append(e.bytes, value) }
func (e *canonicalEncoder) bool(value bool) {
	if value {
		e.u8(1)
	} else {
		e.u8(0)
	}
}
func (e *canonicalEncoder) u16(value uint16) {
	e.bytes = binary.BigEndian.AppendUint16(e.bytes, value)
}
func (e *canonicalEncoder) u32(value uint32) {
	e.bytes = binary.BigEndian.AppendUint32(e.bytes, value)
}
func (e *canonicalEncoder) u64(value uint64) {
	e.bytes = binary.BigEndian.AppendUint64(e.bytes, value)
}
func (e *canonicalEncoder) i64(value int64) { e.u64(uint64(value)) }
func (e *canonicalEncoder) string(value string) {
	e.u32(uint32(len(value)))
	e.bytes = append(e.bytes, value...)
}
func (e *canonicalEncoder) hash(value Hash)      { e.bytes = append(e.bytes, value[:]...) }
func (e *canonicalEncoder) epoch(value [16]byte) { e.bytes = append(e.bytes, value[:]...) }
func (e *canonicalEncoder) optionalInt64(value OptionalInt64) {
	e.bool(value.Valid)
	if value.Valid {
		e.i64(value.Value)
	}
}
func (e *canonicalEncoder) optionalUint64(value OptionalUint64) {
	e.bool(value.Valid)
	if value.Valid {
		e.u64(value.Value)
	}
}
func (e *canonicalEncoder) decimal(value Decimal) {
	e.string(value.Coefficient)
	e.u8(value.Scale)
}
func (e *canonicalEncoder) unit(value Unit) {
	e.string(string(value.Kind))
	e.string(value.AssetID)
	e.string(value.BaseAssetID)
	e.string(value.QuoteAssetID)
}
func (e *canonicalEncoder) numeric(value Numeric) {
	e.decimal(value.Decimal)
	e.unit(value.Unit)
}

func eventID(metadata Metadata) Hash {
	var e canonicalEncoder
	e.string("normalized-event-id")
	e.u16(EventIDEncodingVersion)
	e.string(metadata.SourceID)
	e.string(metadata.ChannelID)
	e.string(string(metadata.EpochKind))
	e.epoch(metadata.EpochID)
	e.u64(metadata.ArrivalOrdinal)
	e.u32(metadata.MessageOrdinal)
	e.hash(metadata.RawSegmentSHA256)
	e.u64(metadata.RawRecordOrdinal)
	e.hash(metadata.RawPayloadSHA256)
	e.string(metadata.MapperVersion)
	e.string(string(metadata.SourceTimeResolution))
	e.string(metadata.SchemaName)
	e.u16(metadata.SchemaVersion)
	return Hash(sha256.Sum256(e.bytes))
}

func encodeMetadata(e *canonicalEncoder, metadata Metadata) {
	e.hash(metadata.EventID)
	e.u16(metadata.EventIDEncodingVersion)
	e.string(metadata.SchemaName)
	e.u16(metadata.SchemaVersion)
	e.string(metadata.SourceID)
	e.string(metadata.ChannelID)
	e.string(metadata.InstrumentUID)
	e.string(string(metadata.EpochKind))
	e.epoch(metadata.EpochID)
	e.u64(metadata.ArrivalOrdinal)
	e.u32(metadata.MessageOrdinal)
	e.optionalInt64(metadata.ExchangeTimeNS)
	e.string(string(metadata.ExchangeTimeResolution))
	e.optionalInt64(metadata.SourceEventTimeNS)
	e.string(string(metadata.SourceTimeResolution))
	e.i64(metadata.ReceivedTimeNS)
	e.hash(metadata.RawSegmentSHA256)
	e.u64(metadata.RawRecordOrdinal)
	e.hash(metadata.RawPayloadSHA256)
	e.hash(metadata.SourceSchemaFingerprint)
	e.string(metadata.MapperVersion)
	e.hash(metadata.MapperBindingID)
	e.hash(metadata.CatalogSnapshotID)
	e.u32(uint32(len(metadata.QualityFlags)))
	for _, flag := range metadata.QualityFlags {
		e.string(string(flag))
	}
}

func logicalHash(row Row) Hash {
	var e canonicalEncoder
	e.string("normalized-logical-row")
	e.u16(LogicalEncodingVersion)
	e.string(string(row.Kind))
	switch row.Kind {
	case EventTrade:
		encodeTrade(&e, *row.Trade)
	case EventBookUpdate:
		encodeBookUpdate(&e, *row.BookUpdate)
	case EventQuote:
		encodeQuote(&e, *row.Quote)
	case EventTicker:
		encodeTicker(&e, *row.Ticker)
	case EventDerivativeTicker:
		encodeDerivativeTicker(&e, *row.DerivativeTicker)
	case EventLiquidation:
		encodeLiquidation(&e, *row.Liquidation)
	case EventOptionSummary:
		encodeOptionSummary(&e, *row.OptionSummary)
	case EventInstrument:
		encodeInstrumentEvent(&e, *row.InstrumentEvent)
	case EventSourceHealth:
		encodeSourceHealth(&e, *row.SourceHealth)
	}
	return Hash(sha256.Sum256(e.bytes))
}

func encodeTrade(e *canonicalEncoder, event TradeV1) {
	encodeMetadata(e, event.Metadata)
	e.u64(event.NativeTradeID)
	e.string(string(event.AggressorSide))
	e.bool(event.BuyerIsMaker)
	e.bool(event.NativeIgnoreFlag)
	e.numeric(event.Price)
	e.numeric(event.Amount)
	e.string(string(event.AggregationKind))
	e.string(string(event.NativeDuplicateStatus))
}

func encodeBookLevel(e *canonicalEncoder, level BookLevel) {
	e.string(string(level.Side))
	e.u32(level.LevelOrdinal)
	e.string(string(level.Action))
	e.numeric(level.Price)
	e.numeric(level.Amount)
}

func encodeBookUpdate(e *canonicalEncoder, event BookUpdateV1) {
	encodeMetadata(e, event.Metadata)
	e.string(string(event.UpdateKind))
	e.string(event.DepthContract)
	e.string(event.AggregationContract)
	e.u64(event.FirstSequence)
	e.u64(event.LastSequence)
	e.optionalUint64(event.PreviousSequence)
	e.string(string(event.Checksum))
	e.u32(uint32(len(event.Bids)))
	for _, level := range event.Bids {
		encodeBookLevel(e, level)
	}
	e.u32(uint32(len(event.Asks)))
	for _, level := range event.Asks {
		encodeBookLevel(e, level)
	}
	e.string(event.AmountSemantics)
	e.string(event.ReconstructionEligibility)
}

func encodeQuote(e *canonicalEncoder, event QuoteV1) {
	encodeMetadata(e, event.Metadata)
	e.string(event.NativeSourceRole)
	e.u64(event.UpdateID)
	e.numeric(event.BidPrice)
	e.numeric(event.BidAmount)
	e.numeric(event.AskPrice)
	e.numeric(event.AskAmount)
	e.string(string(event.RPIInclusionState))
	e.optionalInt64(event.SourceTimeNS)
}

func encodeTicker(e *canonicalEncoder, event TickerV1) {
	encodeMetadata(e, event.Metadata)
	e.string(event.NativeSourceRole)
	e.string(string(event.WindowKind))
	e.string(event.WindowOpenSemantics)
	e.string(event.WindowCloseSemantics)
	e.i64(event.WindowOpenTimeNS)
	e.i64(event.WindowCloseTimeNS)
	e.string(string(event.WindowTimeResolution))
	e.u64(event.NominalWindowDurationNS)
	for _, value := range []Numeric{
		event.PriceChange, event.PriceChangePercent, event.WeightedAveragePrice,
		event.FirstTradeBeforeWindowPrice, event.LastPrice, event.LastAmount,
		event.NativeBestBidPrice, event.NativeBestBidAmount, event.NativeBestAskPrice,
		event.NativeBestAskAmount, event.OpenPrice, event.HighPrice, event.LowPrice,
		event.BaseVolume, event.QuoteVolume,
	} {
		e.numeric(value)
	}
	e.u64(event.FirstTradeID)
	e.u64(event.LastTradeID)
	e.u64(event.TradeCount)
}

func encodeFieldProvenance(e *canonicalEncoder, value FieldProvenance) {
	e.optionalInt64(value.SourceTimeNS)
	e.string(string(value.SourceTimeResolution))
	e.optionalUint64(value.AgeNS)
}

func encodeNumericField(e *canonicalEncoder, value NumericField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.numeric(value.Value)
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeTimeField(e *canonicalEncoder, value TimeField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.i64(value.ValueNS)
		e.string(string(value.Resolution))
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeNativeUnit(e *canonicalEncoder, value NativeUnit) {
	e.string(string(value.Kind))
	e.string(value.AssetID)
	e.string(value.InstrumentUID)
	e.string(value.VenueLabel)
}

func encodeNativeValue(e *canonicalEncoder, value NativeValue) {
	e.decimal(value.Decimal)
	encodeNativeUnit(e, value.Unit)
}

func encodeDerivedValue(e *canonicalEncoder, value DerivedValue) {
	encodeNumericField(e, value.Field)
	e.string(value.FormulaVersion)
	e.string(value.CatalogVersion)
	e.string(string(value.Confidence))
}

func encodeOpenInterest(e *canonicalEncoder, value OpenInterestObservation) {
	e.string(string(value.State))
	e.string(value.Variant)
	if value.State == SourceValue {
		encodeNativeValue(e, value.Native)
		e.string(string(value.Sidedness))
		e.u32(uint32(len(value.ReportedVariants)))
		for _, variant := range value.ReportedVariants {
			e.string(variant.Name)
			encodeNativeValue(e, variant.Value)
		}
		e.string(value.MultiplierCatalogVersion)
	}
	encodeDerivedValue(e, value.DerivedBase)
	encodeDerivedValue(e, value.DerivedQuote)
	encodeDerivedValue(e, value.DerivedUSD)
	encodeFieldProvenance(e, value.Provenance)
}

func encodeDerivativeTicker(e *canonicalEncoder, event DerivativeTickerV1) {
	encodeMetadata(e, event.Metadata)
	e.string(event.NativeSourceRole)
	encodeNumericField(e, event.LastPrice)
	encodeNumericField(e, event.MarkPrice)
	encodeNumericField(e, event.IndexPrice)
	encodeNumericField(e, event.FundingRate)
	encodeTimeField(e, event.NextFundingTime)
	e.u32(uint32(len(event.OpenInterest)))
	for _, value := range event.OpenInterest {
		encodeOpenInterest(e, value)
	}
	encodeNumericField(e, event.SettlementPrice)
	encodeNumericField(e, event.Basis)
	encodeNumericField(e, event.Premium)
}

func encodeLiquidation(e *canonicalEncoder, event LiquidationV1) {
	encodeMetadata(e, event.Metadata)
	e.string(event.NativeSourceRole)
	e.string(string(event.NativeRole))
	e.string(string(event.Side))
	e.string(string(event.SideSemantics))
	encodeNativeValue(e, event.Amount)
	encodeNumericField(e, event.Price)
	e.string(string(event.PriceType))
	e.string(string(event.Completeness))
	e.optionalInt64(event.Window.StartTimeNS)
	e.optionalInt64(event.Window.EndTimeNS)
	e.u64(event.Window.DurationNS)
	e.string(string(event.Window.Selection))
	e.bool(event.Window.PerSymbol)
	e.string(event.Window.BatchID)
}

func encodeTextField(e *canonicalEncoder, value TextField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.string(value.Value)
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeNativeNumericField(e *canonicalEncoder, value NativeNumericField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		encodeNativeValue(e, value.Value)
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeOptionKindField(e *canonicalEncoder, value OptionKindField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.string(string(value.Value))
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeOptionSummary(e *canonicalEncoder, event OptionSummaryV1) {
	encodeMetadata(e, event.Metadata)
	e.string(event.NativeSourceRole)
	encodeTextField(e, event.Instrument)
	encodeTextField(e, event.Underlying)
	encodeTextField(e, event.Index)
	encodeTimeField(e, event.Expiry)
	encodeNumericField(e, event.Strike)
	encodeOptionKindField(e, event.CallPut)
	for _, value := range []NumericField{
		event.BidPrice, event.AskPrice, event.LastPrice, event.MarkPrice,
		event.BidIV, event.AskIV, event.MarkIV,
	} {
		encodeNumericField(e, value)
	}
	for _, value := range []NativeNumericField{
		event.Delta, event.Gamma, event.Vega, event.Theta, event.Rho,
		event.OpenInterest, event.Volume,
	} {
		encodeNativeNumericField(e, value)
	}
	encodeNumericField(e, event.ForwardPrice)
	encodeNumericField(e, event.UnderlyingPrice)
	encodeNumericField(e, event.IndexPrice)
}

func encodeUint64Field(e *canonicalEncoder, value Uint64Field) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.u64(value.Value)
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeHashField(e *canonicalEncoder, value HashField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.hash(value.Value)
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeInstrumentStateField(e *canonicalEncoder, value InstrumentStateField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.string(string(value.Value))
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeInstrumentResolutionField(e *canonicalEncoder, value InstrumentResolutionField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.string(string(value.Value))
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeNumericChange(e *canonicalEncoder, value NumericChange) {
	encodeNumericField(e, value.Old)
	encodeNumericField(e, value.New)
}

func encodeNativeNumericChange(e *canonicalEncoder, value NativeNumericChange) {
	encodeNativeNumericField(e, value.Old)
	encodeNativeNumericField(e, value.New)
}

func encodeTextChange(e *canonicalEncoder, value TextChange) {
	encodeTextField(e, value.Old)
	encodeTextField(e, value.New)
}

func encodeInstrumentEvent(e *canonicalEncoder, event InstrumentEventV1) {
	encodeMetadata(e, event.Metadata)
	encodeUint64Field(e, event.MetadataGeneration)
	encodeInstrumentStateField(e, event.NativeStateBefore)
	encodeInstrumentStateField(e, event.NativeStateAfter)
	encodeTimeField(e, event.ListingTime)
	encodeTimeField(e, event.ContinuousTradingTime)
	encodeTimeField(e, event.ExpiryTime)
	encodeTimeField(e, event.DeliveryTime)
	encodeTimeField(e, event.DelistingTime)
	encodeNumericChange(e, event.TickSize)
	encodeNativeNumericChange(e, event.LotSize)
	encodeNativeNumericChange(e, event.ContractMultiplier)
	encodeTextChange(e, event.Payoff)
	encodeHashField(e, event.OldRawHash)
	encodeHashField(e, event.NewRawHash)
	encodeInstrumentResolutionField(e, event.ResolutionStatus)
}

func encodeHealthStatusField(e *canonicalEncoder, value HealthStatusField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.string(string(value.Value))
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeHealthTextField(e *canonicalEncoder, value HealthTextField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.string(value.Value)
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeHealthMeasurementField(e *canonicalEncoder, value HealthMeasurementField) {
	e.string(string(value.State))
	if value.State == SourceValue {
		e.decimal(value.Value.Decimal)
		e.string(string(value.Value.Unit))
	}
	encodeFieldProvenance(e, value.Provenance)
}

func encodeSourceHealth(e *canonicalEncoder, event SourceHealthV1) {
	encodeMetadata(e, event.Metadata)
	e.string(string(event.Dimension))
	e.string(string(event.Scope))
	e.string(event.Component)
	e.string(event.NativeRole)
	encodeHealthStatusField(e, event.PreviousStatus)
	encodeHealthStatusField(e, event.CurrentStatus)
	encodeHealthTextField(e, event.NativePreviousState)
	encodeHealthTextField(e, event.NativeCurrentState)
	encodeHealthMeasurementField(e, event.PreviousMeasurement)
	encodeHealthMeasurementField(e, event.CurrentMeasurement)
	encodeTimeField(e, event.WindowStart)
	encodeTimeField(e, event.WindowEnd)
	encodeHealthTextField(e, event.NativeCode)
	encodeHealthTextField(e, event.Detail)
}
