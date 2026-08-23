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
