package segment

import (
	"encoding/binary"
	"fmt"
)

type CorpusFamily uint8

const (
	CorpusSpotTrades CorpusFamily = iota + 1
	CorpusPerpetualBook
	CorpusOptionsTicker
	CorpusEquityHistory
	CorpusFXHistory
	CorpusReconnectControl
)

var RepresentativeFamilies = [...]CorpusFamily{
	CorpusSpotTrades,
	CorpusPerpetualBook,
	CorpusOptionsTicker,
	CorpusEquityHistory,
	CorpusFXHistory,
	CorpusReconnectControl,
}

type SyntheticCorpus struct {
	Family CorpusFamily
	Seed   uint64
	next   uint64
}

func NewSyntheticCorpus(family CorpusFamily, seed uint64) (*SyntheticCorpus, error) {
	if family < CorpusSpotTrades || family > CorpusReconnectControl {
		return nil, fmt.Errorf("segment: unknown synthetic corpus family %d", family)
	}
	return &SyntheticCorpus{Family: family, Seed: seed}, nil
}

// Next returns a deterministic synthetic record with exactly payloadBytes raw
// bytes. The control family intentionally requires zero payload bytes.
func (c *SyntheticCorpus) Next(payloadBytes int) (Envelope, error) {
	if payloadBytes < 0 || payloadBytes > MaxPayloadBytes {
		return Envelope{}, fmt.Errorf("%w: synthetic payload has %d bytes", ErrBounds, payloadBytes)
	}
	if c.Family == CorpusReconnectControl && payloadBytes != 0 {
		return Envelope{}, fmt.Errorf("%w: reconnect control payload must be empty", ErrBounds)
	}
	index := c.next
	c.next++
	epoch := syntheticEpoch(c.Seed, index/1024, byte(c.Family))
	base := Envelope{
		Kind:                       RecordKindWebSocket,
		SourceID:                   corpusSource(c.Family),
		ChannelOrEndpoint:          corpusContract(c.Family),
		NativeSymbol:               OptionalString{Value: fmt.Sprintf("SYNTH-%04d", index%10000), Valid: true},
		InstrumentUID:              OptionalString{Value: fmt.Sprintf("instrument-%06d", index%20000), Valid: true},
		ConnectionEpoch:            OptionalEpoch{Value: epoch, Valid: true},
		ArrivalOrdinal:             index + 1,
		MessageOrdinal:             uint32(index % 32),
		ExchangeTimeNS:             OptionalInt64{Value: 1700000000000000000 + int64(index)*1_000_000, Valid: true},
		ExchangeTimeResolution:     TimeResolutionMicrosecond,
		ReceivedWallTimeNS:         1700000000000500000 + int64(index)*1_000_003,
		ClockEpochID:               "synthetic-clock-0001",
		MonotonicNSSinceClockEpoch: index * 1_000_003,
		ClockOffsetNS:              OptionalInt64{Value: -125_000, Valid: true},
		ClockUncertaintyNS:         OptionalInt64{Value: 50_000, Valid: true},
		PayloadEncoding:            PayloadEncodingBinary,
		TerminalOutcome:            OutcomeObserved,
		RecorderVersion:            "synthetic-corpus-v1",
	}

	switch c.Family {
	case CorpusEquityHistory, CorpusFXHistory:
		base.Kind = RecordKindREST
		base.ConnectionEpoch = OptionalEpoch{}
		base.PollCycleID = OptionalEpoch{Value: epoch, Valid: true}
		base.ScheduledAtNS = OptionalInt64{Value: base.ReceivedWallTimeNS - 2_000_000, Valid: true}
		base.RequestStartedAtNS = OptionalInt64{Value: base.ReceivedWallTimeNS - 1_500_000, Valid: true}
		base.RequestCompletedAtNS = OptionalInt64{Value: base.ReceivedWallTimeNS - 100_000, Valid: true}
		base.HTTPStatusOrWSState = OptionalString{Value: "200", Valid: true}
	case CorpusReconnectControl:
		base.Kind = RecordKindControl
		base.PayloadEncoding = PayloadEncodingNone
		base.ExchangeTimeNS = OptionalInt64{}
		base.ExchangeTimeResolution = TimeResolutionAbsent
		base.HTTPStatusOrWSState = OptionalString{Value: "reconnected", Valid: true}
	}
	base.RawPayload = syntheticPayload(c.Family, c.Seed, index, payloadBytes)
	return base, nil
}

func SyntheticRecords(family CorpusFamily, seed uint64, payloadSizes []int) ([]Envelope, error) {
	corpus, err := NewSyntheticCorpus(family, seed)
	if err != nil {
		return nil, err
	}
	records := make([]Envelope, 0, len(payloadSizes))
	for _, size := range payloadSizes {
		record, err := corpus.Next(size)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func syntheticPayload(family CorpusFamily, seed, index uint64, size int) []byte {
	payload := make([]byte, size)
	if size == 0 {
		return payload
	}
	var template string
	symbol := index % 10000
	price := 20_000 + (seed+index*17)%50_000
	quantity := 1 + ((seed>>8)+index*13)%10_000
	switch family {
	case CorpusSpotTrades:
		template = fmt.Sprintf(`{"type":"trade","symbol":"SYNTH-%04d","price":"%d.%02d","quantity":"%d.%06d","side":"buy","trade_id":"%d"}`+"\n", symbol, price, index%100, quantity, index%1_000_000, index)
	case CorpusPerpetualBook:
		template = fmt.Sprintf(`{"type":"book_delta","symbol":"SYNTH-%04d","sequence":%d,"bids":[["%d.10","12.500000"],["%d.00","8.250000"]],"asks":[["%d.20","4.750000"],["%d.30","16.000000"]]}`+"\n", symbol, index, price, price-1, price, price+1)
	case CorpusOptionsTicker:
		template = fmt.Sprintf(`{"type":"ticker","symbol":"SYNTH-%04d-C","mark":"%d.25","bid":null,"ask":"%d.50","open_interest":"%d","greeks":{"delta":"0.5000","gamma":"0.0010"}}`+"\n", symbol, price, price+1, quantity)
	case CorpusEquityHistory:
		template = fmt.Sprintf(`{"date":"2026-08-%02d","symbol":"SYNTH-%04d","open":"%d.00","high":"%d.50","low":"%d.75","close":"%d.25","volume":%d}`+"\n", 1+index%28, symbol, price, price+10, price-10, price+2, quantity*100)
	case CorpusFXHistory:
		template = fmt.Sprintf(`{"time":1700000000,"pair":"SYNTH%04d/USD","bid":"1.%06d","ask":"1.%06d","mid":"1.%06d"}`+"\n", symbol, price, price+12, price+6)
	default:
		template = `{"type":"control","state":"reconnected"}` + "\n"
	}
	pattern := []byte(template)
	for written := 0; written < len(payload); {
		written += copy(payload[written:], pattern)
	}
	return payload
}

func syntheticEpoch(seed, group uint64, family byte) [16]byte {
	var epoch [16]byte
	binary.LittleEndian.PutUint64(epoch[:8], seed^group)
	binary.LittleEndian.PutUint64(epoch[8:], uint64(family)<<56|group)
	return epoch
}

func corpusSource(family CorpusFamily) string {
	switch family {
	case CorpusSpotTrades:
		return "synthetic-spot"
	case CorpusPerpetualBook:
		return "synthetic-perpetual"
	case CorpusOptionsTicker:
		return "synthetic-options"
	case CorpusEquityHistory:
		return "synthetic-equity"
	case CorpusFXHistory:
		return "synthetic-fx"
	case CorpusReconnectControl:
		return "synthetic-control"
	default:
		return ""
	}
}

func corpusContract(family CorpusFamily) string {
	switch family {
	case CorpusSpotTrades:
		return "trades-v1"
	case CorpusPerpetualBook:
		return "book-delta-v1"
	case CorpusOptionsTicker:
		return "ticker-sparse-v1"
	case CorpusEquityHistory:
		return "equity-history-v1"
	case CorpusFXHistory:
		return "fx-history-v1"
	case CorpusReconnectControl:
		return "connection-state-v1"
	default:
		return ""
	}
}
