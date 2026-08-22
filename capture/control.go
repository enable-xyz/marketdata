package capture

import (
	"errors"
	"fmt"
)

var ErrInvalidControl = errors.New("capture: invalid control record")

type ControlKind uint8

const (
	ControlConnectAttempt ControlKind = iota + 1
	ControlConnected
	ControlSubscribeRequest
	ControlAcknowledgement
	ControlHeartbeat
	ControlDisconnect
	ControlReconnect
	ControlPollScheduled
	ControlRateLimited
	ControlTimeout
	ControlParseQuarantine
	ControlShutdown
)

type OptionalControlKind struct {
	Value ControlKind
	Valid bool
}

func (k ControlKind) String() string {
	switch k {
	case ControlConnectAttempt:
		return "connect_attempt"
	case ControlConnected:
		return "connected"
	case ControlSubscribeRequest:
		return "subscribe_request"
	case ControlAcknowledgement:
		return "acknowledgement"
	case ControlHeartbeat:
		return "heartbeat"
	case ControlDisconnect:
		return "disconnect"
	case ControlReconnect:
		return "reconnect"
	case ControlPollScheduled:
		return "poll_scheduled"
	case ControlRateLimited:
		return "rate_limited"
	case ControlTimeout:
		return "timeout"
	case ControlParseQuarantine:
		return "parse_quarantine"
	case ControlShutdown:
		return "shutdown"
	default:
		return fmt.Sprintf("control_kind(%d)", k)
	}
}

// ControlRecord prevents an untyped control envelope from entering the
// downstream stream.
type ControlRecord struct {
	Envelope EnvelopeV1
}

// NewControlRecord marks the envelope as a typed control record, computes the
// digest of its already-captured raw bytes, and validates the result.
func NewControlRecord(kind ControlKind, envelope EnvelopeV1) (ControlRecord, error) {
	envelope.EnvelopeVersion = EnvelopeVersion
	envelope.RecordKind = RecordKindControl
	envelope.ControlKind = OptionalControlKind{Value: kind, Valid: true}
	envelope.RawPayload = append([]byte(nil), envelope.RawPayload...)
	envelope.Extensions = append([]byte(nil), envelope.Extensions...)
	envelope.RawPayloadSHA256 = PayloadHash(envelope.RawPayload)
	record := ControlRecord{Envelope: envelope}
	if err := record.Validate(); err != nil {
		return ControlRecord{}, err
	}
	return record, nil
}

func (r ControlRecord) Validate() error {
	if r.Envelope.RecordKind != RecordKindControl {
		return controlError("record_kind", "must be control")
	}
	return r.Envelope.Validate()
}

func controlError(field, problem string) error {
	return &ValidationError{
		Field:   field,
		Problem: problem,
		Cause:   errors.Join(ErrInvalidEnvelope, ErrInvalidControl),
	}
}

func validateOptionalControlKind(value OptionalControlKind) error {
	if !value.Valid {
		if value.Value != 0 {
			return controlError("control_kind", "absent value must have canonical zero storage")
		}
		return nil
	}
	if value.Value < ControlConnectAttempt || value.Value > ControlShutdown {
		return controlError("control_kind", fmt.Sprintf("unsupported value %d", value.Value))
	}
	return nil
}

func validateControlEnvelope(e EnvelopeV1, epoch StreamEpoch) error {
	if err := validateOptionalControlKind(e.ControlKind); err != nil {
		return err
	}
	kind := e.ControlKind.Value
	requireConnection := kind == ControlConnectAttempt ||
		kind == ControlConnected ||
		kind == ControlSubscribeRequest ||
		kind == ControlAcknowledgement ||
		kind == ControlHeartbeat ||
		kind == ControlDisconnect ||
		kind == ControlReconnect
	if requireConnection && epoch.Kind != EpochConnection {
		return controlError("connection_epoch", kind.String()+" requires a connection epoch")
	}
	if kind == ControlPollScheduled {
		if epoch.Kind != EpochPollCycle {
			return controlError("poll_cycle_id", "poll_scheduled requires a poll cycle")
		}
		if !e.ScheduledAtNS.Valid {
			return controlError("scheduled_at_ns", "poll_scheduled requires its scheduled time")
		}
	}
	return nil
}
