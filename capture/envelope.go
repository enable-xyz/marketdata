package capture

import (
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/enable-xyz/marketdata/segment"
)

const EnvelopeVersion uint16 = segment.EnvelopeVersion

const (
	MaxPayloadBytes         = segment.MaxPayloadBytes
	MaxSourceIDBytes        = segment.MaxSourceIDBytes
	MaxContractIDBytes      = segment.MaxContractIDBytes
	MaxSymbolBytes          = segment.MaxSymbolBytes
	MaxIdentityBytes        = segment.MaxIdentityBytes
	MaxClockEpochIDBytes    = segment.MaxClockEpochIDBytes
	MaxRecorderVersionBytes = segment.MaxRecorderVersionBytes
	MaxExtensionBytes       = segment.MaxExtensionBytes
)

type RecordKind = segment.RecordKind

const (
	RecordKindWebSocket = segment.RecordKindWebSocket
	RecordKindREST      = segment.RecordKindREST
	RecordKindControl   = segment.RecordKindControl
)

type PayloadEncoding = segment.PayloadEncoding

const (
	PayloadEncodingJSON   = segment.PayloadEncodingJSON
	PayloadEncodingBinary = segment.PayloadEncodingBinary
	PayloadEncodingText   = segment.PayloadEncodingText
	PayloadEncodingNone   = segment.PayloadEncodingNone
)

type ExchangeTimeResolution = segment.TimeResolution

const (
	ExchangeTimeAbsent      = segment.TimeResolutionAbsent
	ExchangeTimeNanosecond  = segment.TimeResolutionNanosecond
	ExchangeTimeMicrosecond = segment.TimeResolutionMicrosecond
	ExchangeTimeMillisecond = segment.TimeResolutionMillisecond
	ExchangeTimeSecond      = segment.TimeResolutionSecond
)

type TerminalOutcome = segment.TerminalOutcome

const (
	TerminalObserved     = segment.OutcomeObserved
	TerminalUnchanged    = segment.OutcomeUnchanged
	TerminalTimeout      = segment.OutcomeTimeout
	TerminalRateLimited  = segment.OutcomeRateLimited
	TerminalMalformed    = segment.OutcomeMalformed
	TerminalRejected     = segment.OutcomeRejected
	TerminalFailed       = segment.OutcomeFailed
	TerminalDisconnected = segment.OutcomeDisconnected
)

type OptionalString = segment.OptionalString
type OptionalInt64 = segment.OptionalInt64
type OptionalEpoch = segment.OptionalEpoch
type OptionalHash = segment.OptionalHash

var (
	ErrInvalidEnvelope = errors.New("capture: invalid envelope")
	ErrPayloadHash     = errors.New("capture: raw payload SHA-256 mismatch")
	ErrInvalidEpoch    = errors.New("capture: invalid stream epoch")
)

// ValidationError identifies the semantic field rejected before the framing
// layer is allowed to encode or write a record.
type ValidationError struct {
	Field   string
	Problem string
	Cause   error
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("capture: invalid %s: %s", e.Field, e.Problem)
}

func (e *ValidationError) Unwrap() error { return e.Cause }

func envelopeError(field, problem string, causes ...error) error {
	all := make([]error, 0, len(causes)+1)
	all = append(all, ErrInvalidEnvelope)
	all = append(all, causes...)
	return &ValidationError{Field: field, Problem: problem, Cause: errors.Join(all...)}
}

// EnvelopeV1 is the semantic capture boundary. RawPayload contains the exact
// application bytes received from the transport, before parsing or
// reserialization. Nullable fields use Valid to distinguish absent from a
// present zero or empty value.
type EnvelopeV1 struct {
	EnvelopeVersion            uint16
	RecordKind                 RecordKind
	SourceID                   string
	ChannelOrEndpoint          string
	NativeSymbol               OptionalString
	InstrumentUID              OptionalString
	ConnectionEpoch            OptionalEpoch
	PollCycleID                OptionalEpoch
	ArrivalOrdinal             uint64
	MessageOrdinal             uint32
	ScheduledAtNS              OptionalInt64
	RequestStartedAtNS         OptionalInt64
	RequestCompletedAtNS       OptionalInt64
	ExchangeTimeNS             OptionalInt64
	ExchangeTimeResolution     ExchangeTimeResolution
	ReceivedWallTimeNS         int64
	ClockEpochID               string
	MonotonicNSSinceClockEpoch uint64
	ClockOffsetNS              OptionalInt64
	ClockUncertaintyNS         OptionalInt64
	SubscriptionOrRequestID    OptionalString
	HTTPStatusOrWSState        OptionalString
	PayloadEncoding            PayloadEncoding
	RawPayload                 []byte
	RawPayloadSHA256           [sha256.Size]byte
	SchemaFingerprint          OptionalHash
	TerminalOutcome            TerminalOutcome
	RecorderVersion            string
	ControlKind                OptionalControlKind
	Extensions                 []byte
}

// SetRawPayload takes ownership of no caller memory: it copies the exact bytes
// and records their digest without interpreting them.
func (e *EnvelopeV1) SetRawPayload(payload []byte) {
	e.RawPayload = append([]byte(nil), payload...)
	e.RawPayloadSHA256 = sha256.Sum256(e.RawPayload)
}

// PayloadHash computes the digest of exact application bytes without parsing.
func PayloadHash(payload []byte) [sha256.Size]byte {
	return sha256.Sum256(payload)
}

// StreamEpoch returns the sole epoch identity carried by the envelope. Epoch
// identities are opaque; callers must not order or compare ordinals across
// different returned identities.
func (e EnvelopeV1) StreamEpoch() (StreamEpoch, error) {
	if e.ConnectionEpoch.Valid == e.PollCycleID.Valid {
		return StreamEpoch{}, envelopeError("stream epoch", "exactly one of connection_epoch or poll_cycle_id must be present", ErrInvalidEpoch)
	}
	if e.ConnectionEpoch.Valid {
		return StreamEpoch{Kind: EpochConnection, ID: e.ConnectionEpoch.Value}, nil
	}
	return StreamEpoch{Kind: EpochPollCycle, ID: e.PollCycleID.Value}, nil
}

// Validate checks the complete semantic contract and every framing bound. It
// deliberately does not compare wall timestamps: wall-clock regressions are
// evidence and must remain recordable.
func (e EnvelopeV1) Validate() error {
	if e.EnvelopeVersion != EnvelopeVersion {
		return envelopeError("envelope_version", fmt.Sprintf("got %d, want %d", e.EnvelopeVersion, EnvelopeVersion))
	}
	if e.RecordKind < RecordKindWebSocket || e.RecordKind > RecordKindControl {
		return envelopeError("record_kind", fmt.Sprintf("unsupported value %d", e.RecordKind))
	}
	if err := validateRequiredString("source_id", e.SourceID, MaxSourceIDBytes); err != nil {
		return err
	}
	if err := validateRequiredString("channel_or_endpoint", e.ChannelOrEndpoint, MaxContractIDBytes); err != nil {
		return err
	}
	if err := validateOptionalString("native_symbol", e.NativeSymbol, MaxSymbolBytes); err != nil {
		return err
	}
	if err := validateOptionalString("instrument_uid", e.InstrumentUID, MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateOptionalEpoch("connection_epoch", e.ConnectionEpoch); err != nil {
		return err
	}
	if err := validateOptionalEpoch("poll_cycle_id", e.PollCycleID); err != nil {
		return err
	}
	epoch, err := e.StreamEpoch()
	if err != nil {
		return err
	}
	if e.ArrivalOrdinal == 0 {
		return envelopeError("arrival_ordinal", "must be assigned before downstream enqueue")
	}
	if err := validateOptionalInt64("scheduled_at_ns", e.ScheduledAtNS); err != nil {
		return err
	}
	if err := validateOptionalInt64("request_started_at_ns", e.RequestStartedAtNS); err != nil {
		return err
	}
	if err := validateOptionalInt64("request_completed_at_ns", e.RequestCompletedAtNS); err != nil {
		return err
	}
	if err := validateOptionalInt64("exchange_time_ns", e.ExchangeTimeNS); err != nil {
		return err
	}
	if e.ExchangeTimeResolution > ExchangeTimeSecond {
		return envelopeError("exchange_time_resolution", fmt.Sprintf("unsupported value %d", e.ExchangeTimeResolution))
	}
	if e.ExchangeTimeNS.Valid != (e.ExchangeTimeResolution != ExchangeTimeAbsent) {
		return envelopeError("exchange_time_resolution", "must be absent exactly when exchange_time_ns is absent")
	}
	if err := validateRequiredString("clock_epoch_id", e.ClockEpochID, MaxClockEpochIDBytes); err != nil {
		return err
	}
	if err := validateOptionalInt64("clock_offset_ns", e.ClockOffsetNS); err != nil {
		return err
	}
	if err := validateOptionalInt64("clock_uncertainty_ns", e.ClockUncertaintyNS); err != nil {
		return err
	}
	if e.ClockOffsetNS.Valid != e.ClockUncertaintyNS.Valid {
		return envelopeError("process_clock", "clock_offset_ns and clock_uncertainty_ns must be present together")
	}
	if e.ClockUncertaintyNS.Valid && e.ClockUncertaintyNS.Value < 0 {
		return envelopeError("clock_uncertainty_ns", "must be non-negative")
	}
	if err := validateOptionalString("subscription_or_request_id", e.SubscriptionOrRequestID, MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateOptionalString("http_status_or_ws_state", e.HTTPStatusOrWSState, MaxIdentityBytes); err != nil {
		return err
	}
	if e.PayloadEncoding < PayloadEncodingJSON || e.PayloadEncoding > PayloadEncodingNone {
		return envelopeError("payload_encoding", fmt.Sprintf("unsupported value %d", e.PayloadEncoding))
	}
	if len(e.RawPayload) > MaxPayloadBytes {
		return envelopeError("raw_payload", fmt.Sprintf("has %d bytes, maximum is %d", len(e.RawPayload), MaxPayloadBytes))
	}
	if e.PayloadEncoding == PayloadEncodingNone && len(e.RawPayload) != 0 {
		return envelopeError("raw_payload", "must be empty when payload_encoding is none")
	}
	if got := sha256.Sum256(e.RawPayload); got != e.RawPayloadSHA256 {
		return envelopeError("raw_payload_sha256", "does not match raw_payload", ErrPayloadHash)
	}
	if err := validateOptionalHash("schema_fingerprint", e.SchemaFingerprint); err != nil {
		return err
	}
	if e.TerminalOutcome < TerminalObserved || e.TerminalOutcome > TerminalDisconnected {
		return envelopeError("terminal_outcome", fmt.Sprintf("unsupported value %d", e.TerminalOutcome))
	}
	if err := validateRequiredString("recorder_version", e.RecorderVersion, MaxRecorderVersionBytes); err != nil {
		return err
	}
	if err := validateOptionalControlKind(e.ControlKind); err != nil {
		return err
	}
	if err := validateRecordSemantics(e, epoch); err != nil {
		return err
	}
	encodedExtensionBytes := len(e.Extensions)
	if e.RecordKind == RecordKindControl {
		encodedExtensionBytes += controlExtensionHeaderSize
	}
	if encodedExtensionBytes > MaxExtensionBytes {
		return envelopeError("extensions", fmt.Sprintf("encode to %d bytes, maximum is %d", encodedExtensionBytes, MaxExtensionBytes))
	}
	if bodyBytes := envelopeBodyBytes(e, encodedExtensionBytes); bodyBytes > segment.MaxRecordBytes-segment.RecordHeaderSize {
		return envelopeError("envelope", fmt.Sprintf("body has %d bytes, maximum is %d", bodyBytes, segment.MaxRecordBytes-segment.RecordHeaderSize))
	}
	return nil
}

func validateRecordSemantics(e EnvelopeV1, epoch StreamEpoch) error {
	switch e.RecordKind {
	case RecordKindWebSocket:
		if epoch.Kind != EpochConnection {
			return envelopeError("connection_epoch", "websocket records require a connection epoch", ErrInvalidEpoch)
		}
		if e.ScheduledAtNS.Valid || e.RequestStartedAtNS.Valid || e.RequestCompletedAtNS.Valid {
			return envelopeError("request clocks", "websocket data records cannot carry REST request clocks")
		}
		if e.ControlKind.Valid {
			return envelopeError("control_kind", "must be absent for websocket records", ErrInvalidControl)
		}
	case RecordKindREST:
		if epoch.Kind != EpochPollCycle {
			return envelopeError("poll_cycle_id", "REST records require a poll cycle", ErrInvalidEpoch)
		}
		if !e.RequestStartedAtNS.Valid || !e.RequestCompletedAtNS.Valid {
			return envelopeError("request clocks", "REST records require request start and completion times")
		}
		if e.ControlKind.Valid {
			return envelopeError("control_kind", "must be absent for REST records", ErrInvalidControl)
		}
	case RecordKindControl:
		if !e.ControlKind.Valid {
			return envelopeError("control_kind", "is required for control records", ErrInvalidControl)
		}
		if err := validateControlEnvelope(e, epoch); err != nil {
			return err
		}
	}
	return nil
}

func validateRequiredString(field, value string, maximum int) error {
	if value == "" {
		return envelopeError(field, "must not be empty")
	}
	if len(value) > maximum {
		return envelopeError(field, fmt.Sprintf("has %d bytes, maximum is %d", len(value), maximum))
	}
	return nil
}

func validateOptionalString(field string, value OptionalString, maximum int) error {
	if !value.Valid && value.Value != "" {
		return envelopeError(field, "absent value must have canonical zero storage")
	}
	if value.Valid && len(value.Value) > maximum {
		return envelopeError(field, fmt.Sprintf("has %d bytes, maximum is %d", len(value.Value), maximum))
	}
	return nil
}

func validateOptionalInt64(field string, value OptionalInt64) error {
	if !value.Valid && value.Value != 0 {
		return envelopeError(field, "absent value must have canonical zero storage")
	}
	return nil
}

func validateOptionalEpoch(field string, value OptionalEpoch) error {
	if !value.Valid && value.Value != [16]byte{} {
		return envelopeError(field, "absent value must have canonical zero storage", ErrInvalidEpoch)
	}
	if value.Valid && value.Value == [16]byte{} {
		return envelopeError(field, "present epoch must not be all zero", ErrInvalidEpoch)
	}
	return nil
}

func validateOptionalHash(field string, value OptionalHash) error {
	if !value.Valid && value.Value != [sha256.Size]byte{} {
		return envelopeError(field, "absent value must have canonical zero storage")
	}
	return nil
}

func envelopeBodyBytes(e EnvelopeV1, extensionBytes int) int {
	bytes := 40
	bytes += 2 + len(e.SourceID)
	bytes += 2 + len(e.ChannelOrEndpoint)
	bytes += 2 + len(e.ClockEpochID)
	bytes += 2 + len(e.RecorderVersion)
	if e.NativeSymbol.Valid {
		bytes += 2 + len(e.NativeSymbol.Value)
	}
	if e.InstrumentUID.Valid {
		bytes += 2 + len(e.InstrumentUID.Value)
	}
	if e.ConnectionEpoch.Valid {
		bytes += 16
	}
	if e.PollCycleID.Valid {
		bytes += 16
	}
	for _, value := range [...]OptionalInt64{
		e.ScheduledAtNS,
		e.RequestStartedAtNS,
		e.RequestCompletedAtNS,
		e.ExchangeTimeNS,
		e.ClockOffsetNS,
		e.ClockUncertaintyNS,
	} {
		if value.Valid {
			bytes += 8
		}
	}
	if e.SubscriptionOrRequestID.Valid {
		bytes += 2 + len(e.SubscriptionOrRequestID.Value)
	}
	if e.HTTPStatusOrWSState.Valid {
		bytes += 2 + len(e.HTTPStatusOrWSState.Value)
	}
	bytes += 4 + len(e.RawPayload) + sha256.Size
	if e.SchemaFingerprint.Valid {
		bytes += sha256.Size
	}
	bytes += 4 + extensionBytes
	return bytes
}
