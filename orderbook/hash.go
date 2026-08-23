package orderbook

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"

	"github.com/enable-xyz/marketdata/normalize"
)

type digestEncoder struct {
	h hash.Hash
}

func newDigestEncoder(domain string, version uint16) *digestEncoder {
	encoder := &digestEncoder{h: sha256.New()}
	encoder.string(domain)
	encoder.uint16(version)
	return encoder
}

func (e *digestEncoder) bytes(value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = e.h.Write(length[:])
	_, _ = e.h.Write(value)
}

func (e *digestEncoder) string(value string) {
	e.bytes([]byte(value))
}

func (e *digestEncoder) bool(value bool) {
	if value {
		_, _ = e.h.Write([]byte{1})
		return
	}
	_, _ = e.h.Write([]byte{0})
}

func (e *digestEncoder) uint16(value uint16) {
	var encoded [2]byte
	binary.BigEndian.PutUint16(encoded[:], value)
	_, _ = e.h.Write(encoded[:])
}

func (e *digestEncoder) uint32(value uint32) {
	var encoded [4]byte
	binary.BigEndian.PutUint32(encoded[:], value)
	_, _ = e.h.Write(encoded[:])
}

func (e *digestEncoder) uint64(value uint64) {
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], value)
	_, _ = e.h.Write(encoded[:])
}

func (e *digestEncoder) int64(value int64) {
	e.uint64(uint64(value))
}

func (e *digestEncoder) fixed(value []byte) {
	_, _ = e.h.Write(value)
}

func (e *digestEncoder) sum() normalize.Hash {
	var result normalize.Hash
	copy(result[:], e.h.Sum(nil))
	return result
}

func (e *digestEncoder) rawCoordinate(coordinate normalize.RawCoordinate) {
	e.string(coordinate.SourceID)
	e.string(coordinate.ChannelID)
	e.string(string(coordinate.EpochKind))
	e.fixed(coordinate.EpochID[:])
	e.uint64(coordinate.ArrivalOrdinal)
	e.uint32(coordinate.MessageOrdinal)
	e.fixed(coordinate.RawSegmentSHA256[:])
	e.uint64(coordinate.RawRecordOrdinal)
	e.fixed(coordinate.RawPayloadSHA256[:])
}

func (e *digestEncoder) decimal(decimal normalize.Decimal) {
	e.string(decimal.Coefficient)
	_, _ = e.h.Write([]byte{decimal.Scale})
}

func (e *digestEncoder) unit(unit normalize.Unit) {
	e.string(string(unit.Kind))
	e.string(unit.AssetID)
	e.string(unit.BaseAssetID)
	e.string(unit.QuoteAssetID)
}

func (e *digestEncoder) numeric(numeric normalize.Numeric) {
	e.decimal(numeric.Decimal)
	e.unit(numeric.Unit)
}

func (e *digestEncoder) snapshotLevels(levels []SnapshotLevel) {
	e.uint64(uint64(len(levels)))
	for _, level := range levels {
		e.numeric(level.Price)
		e.numeric(level.Amount)
	}
}

func (e *digestEncoder) levels(levels []Level) {
	e.uint64(uint64(len(levels)))
	for _, level := range levels {
		e.decimal(level.Price)
		e.decimal(level.Amount)
	}
}

func snapshotIdentity(snapshot SnapshotObservation) normalize.Hash {
	encoder := newDigestEncoder("enable-labs/orderbook/snapshot-observation", SnapshotEncodingVersion)
	encoder.string(snapshot.SourceID)
	encoder.string(snapshot.ChannelID)
	encoder.string(snapshot.InstrumentUID)
	encoder.uint64(snapshot.LastSequence)
	encoder.rawCoordinate(snapshot.RawCoordinate)
	encoder.uint64(snapshot.PayloadBytes)
	encoder.snapshotLevels(snapshot.Bids)
	encoder.snapshotLevels(snapshot.Asks)
	return encoder.sum()
}

func policyIdentity(config Config, policy SequencePolicy) normalize.Hash {
	encoder := newDigestEncoder("enable-labs/orderbook/reconstruction-policy", PolicyEncodingVersion)
	encoder.string(config.SourceID)
	encoder.string(config.UpdateChannelID)
	encoder.string(config.SnapshotChannelID)
	encoder.string(config.Instrument.InstrumentUID)
	encoder.string(config.Instrument.BaseAssetID)
	encoder.string(config.Instrument.QuoteAssetID)
	encoder.string(policy.Name())
	encoder.string(policy.Version())
	encoder.bool(config.RejectCrossed)
	encoder.uint32(config.Bounds.MaxBufferedMessages)
	encoder.uint64(config.Bounds.MaxBufferedBytes)
	encoder.int64(config.Bounds.MaxBufferSpanNS)
	encoder.uint32(config.Bounds.MaxLevelsPerSide)
	encoder.uint64(config.Bounds.MaxSnapshotBytes)
	encoder.uint32(config.Bounds.MaxSnapshotFetches)
	encoder.uint32(config.Bounds.MaxOutputs)
	return encoder.sum()
}

func reconstructionEpochIdentity(policyID, parent normalize.Hash, reason CloseReason, first normalize.RawCoordinate) normalize.Hash {
	encoder := newDigestEncoder("enable-labs/orderbook/reconstruction-epoch", 1)
	encoder.fixed(policyID[:])
	encoder.fixed(parent[:])
	encoder.string(string(reason))
	encoder.rawCoordinate(first)
	return encoder.sum()
}

func projectionIdentity(snapshot BookSnapshotV1) normalize.Hash {
	encoder := newDigestEncoder("enable-labs/orderbook/book-snapshot-v1", ProjectionEncodingVersion)
	encoder.string(snapshot.SchemaName)
	encoder.uint16(snapshot.SchemaVersion)
	encoder.string(snapshot.SourceID)
	encoder.string(snapshot.UpdateChannelID)
	encoder.string(snapshot.SnapshotChannelID)
	encoder.string(snapshot.InstrumentUID)
	encoder.string(snapshot.BaseAssetID)
	encoder.string(snapshot.QuoteAssetID)
	encoder.fixed(snapshot.CatalogSnapshotID[:])
	encoder.string(snapshot.MapperVersion)
	encoder.fixed(snapshot.MapperBindingID[:])
	encoder.string(snapshot.PolicyName)
	encoder.string(snapshot.PolicyVersion)
	encoder.fixed(snapshot.PolicyID[:])
	encoder.fixed(snapshot.ReconstructionEpochID[:])
	encoder.fixed(snapshot.InitialSnapshotID[:])
	encoder.rawCoordinate(snapshot.InitialSnapshotCoordinate)
	encoder.uint64(snapshot.LastSequence)
	encoder.uint32(snapshot.OutputOrdinal)
	encoder.rawCoordinate(snapshot.InputRange.First)
	encoder.rawCoordinate(snapshot.InputRange.Last)
	encoder.uint64(snapshot.InputRange.Count)
	encoder.fixed(snapshot.InputRange.Hash[:])
	encoder.levels(snapshot.Bids)
	encoder.levels(snapshot.Asks)
	return encoder.sum()
}

func appendInputIdentity(encoder *digestEncoder, event normalize.BookUpdateV1) {
	encoder.fixed(event.Metadata.EventID[:])
	encoder.rawCoordinate(metadataCoordinate(event.Metadata))
	encoder.uint64(event.FirstSequence)
	encoder.uint64(event.LastSequence)
}

func metadataCoordinate(metadata normalize.Metadata) normalize.RawCoordinate {
	return normalize.RawCoordinate{
		SourceID:         metadata.SourceID,
		ChannelID:        metadata.ChannelID,
		EpochKind:        metadata.EpochKind,
		EpochID:          metadata.EpochID,
		ArrivalOrdinal:   metadata.ArrivalOrdinal,
		MessageOrdinal:   metadata.MessageOrdinal,
		RawSegmentSHA256: metadata.RawSegmentSHA256,
		RawRecordOrdinal: metadata.RawRecordOrdinal,
		RawPayloadSHA256: metadata.RawPayloadSHA256,
	}
}
