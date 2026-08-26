package okx

import (
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/orderbook"
)

// ReconstructionUpdate projects the parsed native message without changing
// native price/size text, level order, sequence IDs, or checksum evidence.
func (m BookMessage) ReconstructionUpdate(receivedTimeNS int64) orderbook.OKXUpdate {
	return orderbook.OKXUpdate{Channel: m.Channel, InstrumentID: m.InstrumentID, Action: m.Action, SourceTimeNS: m.TimestampMS * 1_000_000, ReceivedTimeNS: receivedTimeNS, PreviousSeqID: m.PreviousSeq, SeqID: m.Sequence, Checksum: m.Checksum, Bids: reconstructionLevels(m.Bids), Asks: reconstructionLevels(m.Asks)}
}

func reconstructionLevels(levels []BookLevel) []orderbook.OKXLevel {
	result := make([]orderbook.OKXLevel, len(levels))
	for index, level := range levels {
		result[index] = orderbook.OKXLevel{Price: level.Price, Size: level.Size, DeprecatedOrders: level.DeprecatedOrders, OrderCount: level.OrderCount}
	}
	return result
}

func (m BookMessage) Normalized(metadata normalize.Metadata, priceUnit, amountUnit normalize.Unit) (normalize.OKXBookUpdateV1, error) {
	bids := make([]normalize.OKXBookLevelInput, len(m.Bids))
	for index, level := range m.Bids {
		bids[index] = normalize.OKXBookLevelInput{Price: level.Price, Amount: level.Size}
	}
	asks := make([]normalize.OKXBookLevelInput, len(m.Asks))
	for index, level := range m.Asks {
		asks[index] = normalize.OKXBookLevelInput{Price: level.Price, Amount: level.Size}
	}
	return normalize.MapOKXBook(metadata, normalize.OKXBookInput{Action: m.Action, DepthContract: m.Channel, SourceTimeNS: m.TimestampMS * 1_000_000, PreviousSequence: m.PreviousSeq, Sequence: m.Sequence, RawChecksum: m.Checksum, Bids: bids, Asks: asks, PriceUnit: priceUnit, AmountUnit: amountUnit})
}

func (m BookMessage) NormalizedQuote(metadata normalize.Metadata, priceUnit, amountUnit normalize.Unit) (normalize.OKXQuoteV1, error) {
	if m.Channel != "bbo-tbt" || len(m.Bids) != 1 || len(m.Asks) != 1 {
		return normalize.OKXQuoteV1{}, ErrAmbiguousProjection
	}
	sourceTimeNS := m.TimestampMS * 1_000_000
	return normalize.MapOKXQuote(metadata,
		normalizedNativeField(NativeField{State: normalize.SourceValue, Text: m.Bids[0].Price}, sourceTimeNS),
		normalizedNativeField(NativeField{State: normalize.SourceValue, Text: m.Bids[0].Size}, sourceTimeNS),
		normalizedNativeField(NativeField{State: normalize.SourceValue, Text: m.Asks[0].Price}, sourceTimeNS),
		normalizedNativeField(NativeField{State: normalize.SourceValue, Text: m.Asks[0].Size}, sourceTimeNS),
		priceUnit, amountUnit,
	)
}
