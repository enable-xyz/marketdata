package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"

	"github.com/enable-xyz/marketdata/segment"
)

var logicalHashDomainV1 = []byte("enable-marketdata/native-replay/logical-hash\x00")

type logicalHasher struct {
	h            hash.Hash
	recordBuffer []byte
	numberBuffer [8]byte
}

func newLogicalHasher(version uint16) (*logicalHasher, error) {
	if version != LogicalHashVersionV1 {
		return nil, fmt.Errorf("%w: %d", ErrUnsupportedHash, version)
	}
	hasher := &logicalHasher{h: sha256.New()}
	_, _ = hasher.h.Write(logicalHashDomainV1)
	binary.BigEndian.PutUint16(hasher.numberBuffer[:2], version)
	_, _ = hasher.h.Write(hasher.numberBuffer[:2])
	return hasher, nil
}

func (h *logicalHasher) writeEvent(event Event) error {
	_, _ = h.h.Write([]byte{byte(event.Kind)})
	if err := h.writeString(string(event.Order)); err != nil {
		return err
	}
	h.writeInt64(event.Coordinate.ReceivedWallTimeNS)
	if err := h.writeString(event.Coordinate.SourceID); err != nil {
		return err
	}
	h.writeInt64(event.Coordinate.EpochFirstReceivedWallTimeNS)
	if err := h.writeString(event.Coordinate.StreamEpochID); err != nil {
		return err
	}
	h.writeUint64(event.Coordinate.ArrivalOrdinal)
	h.writeUint32(event.Coordinate.MessageOrdinal)

	switch event.Kind {
	case EventRecord:
		var err error
		h.recordBuffer, err = segmentAppend(h.recordBuffer[:0], event)
		if err != nil {
			return fmt.Errorf("%w: canonical record encoding: %v", ErrIntegrity, err)
		}
		if err := h.writeBytes(h.recordBuffer); err != nil {
			return err
		}
	case EventDiscontinuity:
		_, _ = h.h.Write([]byte{byte(event.Discontinuity.Kind), byte(event.Discontinuity.Reason)})
		if err := h.writeString(event.Discontinuity.SegmentID); err != nil {
			return err
		}
		if err := h.writeString(event.Discontinuity.PreviousStreamEpochID); err != nil {
			return err
		}
		h.writeUint64(event.Discontinuity.FirstOrdinal)
		h.writeUint64(event.Discontinuity.LastOrdinal)
		h.writeUint32(event.Discontinuity.FrameOrdinal)
		h.writeUint64(event.Discontinuity.CompressedOffset)
	default:
		return fmt.Errorf("%w: unsupported event kind %d", ErrIntegrity, event.Kind)
	}
	return nil
}

func segmentAppend(dst []byte, event Event) ([]byte, error) {
	return segment.AppendEnvelope(dst, event.Record)
}

func (h *logicalHasher) writeBytes(value []byte) error {
	if uint64(len(value)) > uint64(^uint32(0)) {
		return fmt.Errorf("%w: logical hash field has %d bytes", ErrInputBound, len(value))
	}
	h.writeUint32(uint32(len(value)))
	_, _ = h.h.Write(value)
	return nil
}

func (h *logicalHasher) writeString(value string) error {
	return h.writeBytes([]byte(value))
}

func (h *logicalHasher) writeUint32(value uint32) {
	binary.BigEndian.PutUint32(h.numberBuffer[:4], value)
	_, _ = h.h.Write(h.numberBuffer[:4])
}

func (h *logicalHasher) writeUint64(value uint64) {
	binary.BigEndian.PutUint64(h.numberBuffer[:], value)
	_, _ = h.h.Write(h.numberBuffer[:])
}

func (h *logicalHasher) writeInt64(value int64) {
	h.writeUint64(uint64(value))
}

func (h *logicalHasher) sum() [32]byte {
	var result [32]byte
	copy(result[:], h.h.Sum(nil))
	return result
}
