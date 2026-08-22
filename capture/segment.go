package capture

import (
	"bytes"
	"fmt"

	"github.com/enable-xyz/marketdata/segment"
)

const controlExtensionHeaderSize = 8

var controlExtensionMagic = [4]byte{'E', 'L', 'C', '1'}

const (
	captureExtensionVersion byte = 1
	captureExtensionControl byte = 1
)

// ToSegment validates all semantic and size bounds before returning a framing
// record. Returned byte slices do not alias the envelope.
func (e EnvelopeV1) ToSegment() (segment.Envelope, error) {
	if err := e.Validate(); err != nil {
		return segment.Envelope{}, err
	}
	var extensions []byte
	if e.RecordKind == RecordKindControl {
		extensions = make([]byte, controlExtensionHeaderSize, controlExtensionHeaderSize+len(e.Extensions))
		copy(extensions[:4], controlExtensionMagic[:])
		extensions[4] = captureExtensionVersion
		extensions[5] = captureExtensionControl
		extensions[6] = byte(e.ControlKind.Value)
		extensions = append(extensions, e.Extensions...)
	} else {
		extensions = append([]byte(nil), e.Extensions...)
	}
	return segment.Envelope{
		Kind:                       e.RecordKind,
		SourceID:                   e.SourceID,
		ChannelOrEndpoint:          e.ChannelOrEndpoint,
		NativeSymbol:               e.NativeSymbol,
		InstrumentUID:              e.InstrumentUID,
		ConnectionEpoch:            e.ConnectionEpoch,
		PollCycleID:                e.PollCycleID,
		ArrivalOrdinal:             e.ArrivalOrdinal,
		MessageOrdinal:             e.MessageOrdinal,
		ScheduledAtNS:              e.ScheduledAtNS,
		RequestStartedAtNS:         e.RequestStartedAtNS,
		RequestCompletedAtNS:       e.RequestCompletedAtNS,
		ExchangeTimeNS:             e.ExchangeTimeNS,
		ExchangeTimeResolution:     e.ExchangeTimeResolution,
		ReceivedWallTimeNS:         e.ReceivedWallTimeNS,
		ClockEpochID:               e.ClockEpochID,
		MonotonicNSSinceClockEpoch: e.MonotonicNSSinceClockEpoch,
		ClockOffsetNS:              e.ClockOffsetNS,
		ClockUncertaintyNS:         e.ClockUncertaintyNS,
		SubscriptionOrRequestID:    e.SubscriptionOrRequestID,
		HTTPStatusOrWSState:        e.HTTPStatusOrWSState,
		PayloadEncoding:            e.PayloadEncoding,
		RawPayload:                 append([]byte(nil), e.RawPayload...),
		SchemaFingerprint:          e.SchemaFingerprint,
		TerminalOutcome:            e.TerminalOutcome,
		RecorderVersion:            e.RecorderVersion,
		Extensions:                 extensions,
	}, nil
}

// EnvelopeV1FromSegment raises the framing record into the stronger semantic
// model, including verification of the raw payload digest and typed control
// extension.
func EnvelopeV1FromSegment(record segment.Envelope) (EnvelopeV1, error) {
	extensions := append([]byte(nil), record.Extensions...)
	controlKind := OptionalControlKind{}
	if record.Kind == segment.RecordKindControl {
		var err error
		controlKind, extensions, err = decodeControlExtension(extensions)
		if err != nil {
			return EnvelopeV1{}, err
		}
	}
	envelope := EnvelopeV1{
		EnvelopeVersion:            EnvelopeVersion,
		RecordKind:                 record.Kind,
		SourceID:                   record.SourceID,
		ChannelOrEndpoint:          record.ChannelOrEndpoint,
		NativeSymbol:               record.NativeSymbol,
		InstrumentUID:              record.InstrumentUID,
		ConnectionEpoch:            record.ConnectionEpoch,
		PollCycleID:                record.PollCycleID,
		ArrivalOrdinal:             record.ArrivalOrdinal,
		MessageOrdinal:             record.MessageOrdinal,
		ScheduledAtNS:              record.ScheduledAtNS,
		RequestStartedAtNS:         record.RequestStartedAtNS,
		RequestCompletedAtNS:       record.RequestCompletedAtNS,
		ExchangeTimeNS:             record.ExchangeTimeNS,
		ExchangeTimeResolution:     record.ExchangeTimeResolution,
		ReceivedWallTimeNS:         record.ReceivedWallTimeNS,
		ClockEpochID:               record.ClockEpochID,
		MonotonicNSSinceClockEpoch: record.MonotonicNSSinceClockEpoch,
		ClockOffsetNS:              record.ClockOffsetNS,
		ClockUncertaintyNS:         record.ClockUncertaintyNS,
		SubscriptionOrRequestID:    record.SubscriptionOrRequestID,
		HTTPStatusOrWSState:        record.HTTPStatusOrWSState,
		PayloadEncoding:            record.PayloadEncoding,
		RawPayload:                 append([]byte(nil), record.RawPayload...),
		RawPayloadSHA256:           PayloadHash(record.RawPayload),
		SchemaFingerprint:          record.SchemaFingerprint,
		TerminalOutcome:            record.TerminalOutcome,
		RecorderVersion:            record.RecorderVersion,
		ControlKind:                controlKind,
		Extensions:                 extensions,
	}
	if err := envelope.Validate(); err != nil {
		return EnvelopeV1{}, err
	}
	return envelope, nil
}

func decodeControlExtension(extension []byte) (OptionalControlKind, []byte, error) {
	if len(extension) < controlExtensionHeaderSize {
		return OptionalControlKind{}, nil, controlError("extensions", "typed control header is missing")
	}
	if !bytes.Equal(extension[:4], controlExtensionMagic[:]) {
		return OptionalControlKind{}, nil, controlError("extensions", "typed control magic is invalid")
	}
	if extension[4] != captureExtensionVersion {
		return OptionalControlKind{}, nil, controlError("extensions", fmt.Sprintf("unsupported semantic extension version %d", extension[4]))
	}
	if extension[5] != captureExtensionControl {
		return OptionalControlKind{}, nil, controlError("extensions", fmt.Sprintf("unsupported semantic extension type %d", extension[5]))
	}
	if extension[7] != 0 {
		return OptionalControlKind{}, nil, controlError("extensions", "reserved control extension byte is nonzero")
	}
	kind := OptionalControlKind{Value: ControlKind(extension[6]), Valid: true}
	if err := validateOptionalControlKind(kind); err != nil {
		return OptionalControlKind{}, nil, err
	}
	return kind, append([]byte(nil), extension[controlExtensionHeaderSize:]...), nil
}

// MarshalEnvelopeV1 rejects semantic and framing bounds before encoding.
func MarshalEnvelopeV1(envelope EnvelopeV1) ([]byte, error) {
	record, err := envelope.ToSegment()
	if err != nil {
		return nil, err
	}
	return segment.MarshalEnvelope(record)
}

func UnmarshalEnvelopeV1(data []byte) (EnvelopeV1, error) {
	record, err := segment.UnmarshalEnvelope(data)
	if err != nil {
		return EnvelopeV1{}, err
	}
	return EnvelopeV1FromSegment(record)
}

func (r ControlRecord) ToSegment() (segment.Envelope, error) {
	if err := r.Validate(); err != nil {
		return segment.Envelope{}, err
	}
	return r.Envelope.ToSegment()
}

func ControlRecordFromSegment(record segment.Envelope) (ControlRecord, error) {
	envelope, err := EnvelopeV1FromSegment(record)
	if err != nil {
		return ControlRecord{}, err
	}
	control := ControlRecord{Envelope: envelope}
	if err := control.Validate(); err != nil {
		return ControlRecord{}, err
	}
	return control, nil
}
