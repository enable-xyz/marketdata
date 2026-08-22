package capture

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestEnvelope(t *testing.T) {
	t.Run("lossless raw and nullable round trip", func(t *testing.T) {
		want := validWebSocketEnvelope()
		encoded, err := MarshalEnvelopeV1(want)
		if err != nil {
			t.Fatalf("MarshalEnvelopeV1() error = %v", err)
		}
		got, err := UnmarshalEnvelopeV1(encoded)
		if err != nil {
			t.Fatalf("UnmarshalEnvelopeV1() error = %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("round trip mismatch\ngot:  %#v\nwant: %#v", got, want)
		}
		if !bytes.Equal(got.RawPayload, []byte{0x00, 0xff, '{', '}', 0x00}) {
			t.Fatalf("raw payload changed: %x", got.RawPayload)
		}
		if got.RawPayloadSHA256 != PayloadHash(want.RawPayload) {
			t.Fatal("raw payload SHA-256 changed")
		}
		if !got.NativeSymbol.Valid || got.NativeSymbol.Value != "" {
			t.Fatalf("present empty native symbol lost: %#v", got.NativeSymbol)
		}
		if got.InstrumentUID.Valid {
			t.Fatalf("absent instrument UID became present: %#v", got.InstrumentUID)
		}
	})

	t.Run("segment conversion does not alias bytes", func(t *testing.T) {
		envelope := validWebSocketEnvelope()
		record, err := envelope.ToSegment()
		if err != nil {
			t.Fatalf("ToSegment() error = %v", err)
		}
		record.RawPayload[0] ^= 0xff
		record.Extensions[0] ^= 0xff
		if envelope.RawPayload[0] != 0x00 {
			t.Fatal("segment payload aliases capture payload")
		}
		if envelope.Extensions[0] != 0x10 {
			t.Fatal("segment extensions alias capture extensions")
		}
	})

	t.Run("all exchange resolutions and clocks", func(t *testing.T) {
		resolutions := []ExchangeTimeResolution{
			ExchangeTimeAbsent,
			ExchangeTimeNanosecond,
			ExchangeTimeMicrosecond,
			ExchangeTimeMillisecond,
			ExchangeTimeSecond,
		}
		for _, resolution := range resolutions {
			envelope := validRESTEnvelope()
			envelope.ExchangeTimeResolution = resolution
			envelope.ExchangeTimeNS = OptionalInt64{}
			if resolution != ExchangeTimeAbsent {
				envelope.ExchangeTimeNS = OptionalInt64{Value: 1_700_000_000_123_456_789, Valid: true}
			}
			encoded, err := MarshalEnvelopeV1(envelope)
			if err != nil {
				t.Fatalf("resolution %d: MarshalEnvelopeV1() error = %v", resolution, err)
			}
			got, err := UnmarshalEnvelopeV1(encoded)
			if err != nil {
				t.Fatalf("resolution %d: UnmarshalEnvelopeV1() error = %v", resolution, err)
			}
			if !reflect.DeepEqual(got, envelope) {
				t.Fatalf("resolution %d round trip mismatch\ngot:  %#v\nwant: %#v", resolution, got, envelope)
			}
			if got.ScheduledAtNS != envelope.ScheduledAtNS ||
				got.RequestStartedAtNS != envelope.RequestStartedAtNS ||
				got.RequestCompletedAtNS != envelope.RequestCompletedAtNS ||
				got.ReceivedWallTimeNS != envelope.ReceivedWallTimeNS ||
				got.MonotonicNSSinceClockEpoch != envelope.MonotonicNSSinceClockEpoch ||
				got.ClockOffsetNS != envelope.ClockOffsetNS ||
				got.ClockUncertaintyNS != envelope.ClockUncertaintyNS {
				t.Fatalf("resolution %d lost clock fields", resolution)
			}
		}
	})

	t.Run("terminal outcomes round trip", func(t *testing.T) {
		outcomes := []TerminalOutcome{
			TerminalObserved,
			TerminalUnchanged,
			TerminalTimeout,
			TerminalRateLimited,
			TerminalMalformed,
			TerminalRejected,
			TerminalFailed,
			TerminalDisconnected,
		}
		for _, outcome := range outcomes {
			envelope := validRESTEnvelope()
			envelope.TerminalOutcome = outcome
			encoded, err := MarshalEnvelopeV1(envelope)
			if err != nil {
				t.Fatalf("outcome %d: MarshalEnvelopeV1() error = %v", outcome, err)
			}
			got, err := UnmarshalEnvelopeV1(encoded)
			if err != nil {
				t.Fatalf("outcome %d: UnmarshalEnvelopeV1() error = %v", outcome, err)
			}
			if got.TerminalOutcome != outcome {
				t.Fatalf("terminal outcome = %d, want %d", got.TerminalOutcome, outcome)
			}
		}
	})

	t.Run("typed controls preserve every terminal outcome", func(t *testing.T) {
		controls := []struct {
			kind ControlKind
			poll bool
		}{
			{ControlConnectAttempt, false},
			{ControlConnected, false},
			{ControlSubscribeRequest, false},
			{ControlAcknowledgement, false},
			{ControlHeartbeat, false},
			{ControlDisconnect, false},
			{ControlReconnect, false},
			{ControlPollScheduled, true},
			{ControlRateLimited, true},
			{ControlTimeout, true},
			{ControlParseQuarantine, false},
			{ControlShutdown, false},
		}
		outcomes := []TerminalOutcome{
			TerminalObserved,
			TerminalUnchanged,
			TerminalTimeout,
			TerminalRateLimited,
			TerminalMalformed,
			TerminalRejected,
			TerminalFailed,
			TerminalDisconnected,
		}
		for _, controlType := range controls {
			for _, outcome := range outcomes {
				envelope := validControlEnvelope(controlType.kind, outcome, controlType.poll)
				control, err := NewControlRecord(controlType.kind, envelope)
				if err != nil {
					t.Fatalf("%s outcome %d: NewControlRecord() error = %v", controlType.kind, outcome, err)
				}
				framed, err := control.ToSegment()
				if err != nil {
					t.Fatalf("%s outcome %d: ToSegment() error = %v", controlType.kind, outcome, err)
				}
				got, err := ControlRecordFromSegment(framed)
				if err != nil {
					t.Fatalf("%s outcome %d: ControlRecordFromSegment() error = %v", controlType.kind, outcome, err)
				}
				if got.Envelope.ControlKind != (OptionalControlKind{Value: controlType.kind, Valid: true}) {
					t.Fatalf("control kind = %#v, want %s", got.Envelope.ControlKind, controlType.kind)
				}
				if got.Envelope.TerminalOutcome != outcome {
					t.Fatalf("%s outcome = %d, want %d", controlType.kind, got.Envelope.TerminalOutcome, outcome)
				}
				if !bytes.Equal(got.Envelope.Extensions, envelope.Extensions) {
					t.Fatalf("%s outcome %d extension changed: got %x want %x", controlType.kind, outcome, got.Envelope.Extensions, envelope.Extensions)
				}
			}
		}
	})

	t.Run("invalid combinations and bounds fail before framing", func(t *testing.T) {
		tests := []struct {
			name   string
			mutate func(*EnvelopeV1)
			cause  error
		}{
			{
				name: "version",
				mutate: func(e *EnvelopeV1) {
					e.EnvelopeVersion = 2
				},
				cause: ErrInvalidEnvelope,
			},
			{
				name: "missing epoch",
				mutate: func(e *EnvelopeV1) {
					e.ConnectionEpoch = OptionalEpoch{}
				},
				cause: ErrInvalidEpoch,
			},
			{
				name: "both epochs",
				mutate: func(e *EnvelopeV1) {
					e.PollCycleID = OptionalEpoch{Value: epochValue(0x22), Valid: true}
				},
				cause: ErrInvalidEpoch,
			},
			{
				name: "all-zero epoch",
				mutate: func(e *EnvelopeV1) {
					e.ConnectionEpoch = OptionalEpoch{Valid: true}
				},
				cause: ErrInvalidEpoch,
			},
			{
				name: "unassigned ordinal",
				mutate: func(e *EnvelopeV1) {
					e.ArrivalOrdinal = 0
				},
				cause: ErrInvalidEnvelope,
			},
			{
				name: "hash mismatch",
				mutate: func(e *EnvelopeV1) {
					e.RawPayloadSHA256[0] ^= 0xff
				},
				cause: ErrPayloadHash,
			},
			{
				name: "exchange resolution without time",
				mutate: func(e *EnvelopeV1) {
					e.ExchangeTimeNS = OptionalInt64{}
				},
				cause: ErrInvalidEnvelope,
			},
			{
				name: "clock offset without uncertainty",
				mutate: func(e *EnvelopeV1) {
					e.ClockUncertaintyNS = OptionalInt64{}
				},
				cause: ErrInvalidEnvelope,
			},
			{
				name: "negative uncertainty",
				mutate: func(e *EnvelopeV1) {
					e.ClockUncertaintyNS.Value = -1
				},
				cause: ErrInvalidEnvelope,
			},
			{
				name: "none encoding with payload",
				mutate: func(e *EnvelopeV1) {
					e.PayloadEncoding = PayloadEncodingNone
				},
				cause: ErrInvalidEnvelope,
			},
			{
				name: "source bound",
				mutate: func(e *EnvelopeV1) {
					e.SourceID = strings.Repeat("s", MaxSourceIDBytes+1)
				},
				cause: ErrInvalidEnvelope,
			},
			{
				name: "extension bound",
				mutate: func(e *EnvelopeV1) {
					e.Extensions = make([]byte, MaxExtensionBytes+1)
				},
				cause: ErrInvalidEnvelope,
			},
			{
				name: "noncanonical null",
				mutate: func(e *EnvelopeV1) {
					e.InstrumentUID = OptionalString{Value: "discarded", Valid: false}
				},
				cause: ErrInvalidEnvelope,
			},
		}
		for _, test := range tests {
			envelope := validWebSocketEnvelope()
			test.mutate(&envelope)
			_, err := envelope.ToSegment()
			if !errors.Is(err, test.cause) {
				t.Fatalf("%s: ToSegment() error = %v, want cause %v", test.name, err, test.cause)
			}
		}
	})

	t.Run("typed control header is mandatory", func(t *testing.T) {
		envelope := validControlEnvelope(ControlHeartbeat, TerminalObserved, false)
		control, err := NewControlRecord(ControlHeartbeat, envelope)
		if err != nil {
			t.Fatalf("NewControlRecord() error = %v", err)
		}
		record, err := control.ToSegment()
		if err != nil {
			t.Fatalf("ToSegment() error = %v", err)
		}
		record.Extensions = append([]byte(nil), envelope.Extensions...)
		_, err = EnvelopeV1FromSegment(record)
		if !errors.Is(err, ErrInvalidControl) {
			t.Fatalf("EnvelopeV1FromSegment() error = %v, want ErrInvalidControl", err)
		}
	})
}

func validWebSocketEnvelope() EnvelopeV1 {
	envelope := EnvelopeV1{
		EnvelopeVersion:            EnvelopeVersion,
		RecordKind:                 RecordKindWebSocket,
		SourceID:                   "source-api-v1",
		ChannelOrEndpoint:          "trades-v3",
		NativeSymbol:               OptionalString{Value: "", Valid: true},
		ConnectionEpoch:            OptionalEpoch{Value: epochValue(0x11), Valid: true},
		ArrivalOrdinal:             7,
		MessageOrdinal:             2,
		ExchangeTimeNS:             OptionalInt64{Value: 1_700_000_000_123_456_789, Valid: true},
		ExchangeTimeResolution:     ExchangeTimeNanosecond,
		ReceivedWallTimeNS:         1_700_000_000_123_556_789,
		ClockEpochID:               "recorder-process-17",
		MonotonicNSSinceClockEpoch: 998_001,
		ClockOffsetNS:              OptionalInt64{Value: -125_000, Valid: true},
		ClockUncertaintyNS:         OptionalInt64{Value: 50_000, Valid: true},
		SubscriptionOrRequestID:    OptionalString{Value: "native-subscription-9", Valid: true},
		HTTPStatusOrWSState:        OptionalString{Value: "open", Valid: true},
		PayloadEncoding:            PayloadEncodingBinary,
		SchemaFingerprint:          OptionalHash{Value: PayloadHash([]byte("schema-v1")), Valid: true},
		TerminalOutcome:            TerminalObserved,
		RecorderVersion:            "recorder-build-abc123",
		Extensions:                 []byte{0x10, 0x00, 0xff},
	}
	envelope.SetRawPayload([]byte{0x00, 0xff, '{', '}', 0x00})
	return envelope
}

func validRESTEnvelope() EnvelopeV1 {
	envelope := validWebSocketEnvelope()
	envelope.RecordKind = RecordKindREST
	envelope.ConnectionEpoch = OptionalEpoch{}
	envelope.PollCycleID = OptionalEpoch{Value: epochValue(0x22), Valid: true}
	envelope.MessageOrdinal = 0
	envelope.ScheduledAtNS = OptionalInt64{Value: 1_700_000_000_120_000_000, Valid: true}
	envelope.RequestStartedAtNS = OptionalInt64{Value: 1_700_000_000_121_000_000, Valid: true}
	envelope.RequestCompletedAtNS = OptionalInt64{Value: 1_700_000_000_123_500_000, Valid: true}
	envelope.SubscriptionOrRequestID = OptionalString{Value: "native-request-44", Valid: true}
	envelope.HTTPStatusOrWSState = OptionalString{Value: "200", Valid: true}
	return envelope
}

func validControlEnvelope(kind ControlKind, outcome TerminalOutcome, poll bool) EnvelopeV1 {
	envelope := validWebSocketEnvelope()
	envelope.RecordKind = RecordKindControl
	envelope.ChannelOrEndpoint = "control-v1"
	envelope.MessageOrdinal = 0
	envelope.ExchangeTimeNS = OptionalInt64{}
	envelope.ExchangeTimeResolution = ExchangeTimeAbsent
	envelope.PayloadEncoding = PayloadEncodingNone
	envelope.SetRawPayload(nil)
	envelope.TerminalOutcome = outcome
	envelope.ControlKind = OptionalControlKind{Value: kind, Valid: true}
	if poll {
		envelope.ConnectionEpoch = OptionalEpoch{}
		envelope.PollCycleID = OptionalEpoch{Value: epochValue(0x33), Valid: true}
	}
	if kind == ControlPollScheduled {
		envelope.ScheduledAtNS = OptionalInt64{Value: 1_700_000_000_120_000_000, Valid: true}
	}
	return envelope
}

func epochValue(seed byte) [16]byte {
	var epoch [16]byte
	for i := range epoch {
		epoch[i] = seed + byte(i)
	}
	return epoch
}
