package segment

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"time"
)

const (
	FormatVersion             uint16 = 1
	EnvelopeVersion           uint16 = 1
	RecordHeaderSize                 = 16
	MaxPayloadBytes                  = 16 << 20
	MaxExtensionBytes                = 64 << 10
	MaxRecordBytes                   = MaxPayloadBytes + MaxExtensionBytes + 4096
	minimumEncodedRecordBytes        = RecordHeaderSize + 40 + 4*(2+1) + 4 + sha256.Size + 4
	MaxSourceIDBytes                 = 128
	MaxContractIDBytes               = 128
	MaxSymbolBytes                   = 256
	MaxIdentityBytes                 = 256
	MaxClockEpochIDBytes             = 128
	MaxRecorderVersionBytes          = 128
)

var (
	recordMagic = [4]byte{'E', 'M', 'R', '1'}
	crc32cTable = crc32.MakeTable(crc32.Castagnoli)

	ErrBounds      = errors.New("segment: value exceeds format bound")
	ErrCorrupt     = errors.New("segment: corrupt data")
	ErrTruncated   = errors.New("segment: truncated data")
	ErrVersion     = errors.New("segment: unsupported format version")
	ErrPayloadHash = errors.New("segment: payload SHA-256 mismatch")
)

type RecordKind uint8

const (
	RecordKindWebSocket RecordKind = 1
	RecordKindREST      RecordKind = 2
	RecordKindControl   RecordKind = 3
)

type PayloadEncoding uint8

const (
	PayloadEncodingJSON   PayloadEncoding = 1
	PayloadEncodingBinary PayloadEncoding = 2
	PayloadEncodingText   PayloadEncoding = 3
	PayloadEncodingNone   PayloadEncoding = 4
)

type TimeResolution uint8

const (
	TimeResolutionAbsent TimeResolution = iota
	TimeResolutionNanosecond
	TimeResolutionMicrosecond
	TimeResolutionMillisecond
	TimeResolutionSecond
)

type TerminalOutcome uint8

const (
	OutcomeObserved TerminalOutcome = iota + 1
	OutcomeUnchanged
	OutcomeTimeout
	OutcomeRateLimited
	OutcomeMalformed
	OutcomeRejected
	OutcomeFailed
	OutcomeDisconnected
)

type OptionalString struct {
	Value string
	Valid bool
}

type OptionalInt64 struct {
	Value int64
	Valid bool
}

type OptionalEpoch struct {
	Value [16]byte
	Valid bool
}

type OptionalHash struct {
	Value [32]byte
	Valid bool
}

// Envelope is the exact logical v1 capture envelope. RawPayload is preserved
// byte-for-byte; its SHA-256 is computed by the framing implementation.
type Envelope struct {
	Kind                       RecordKind
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
	ExchangeTimeResolution     TimeResolution
	ReceivedWallTimeNS         int64
	ClockEpochID               string
	MonotonicNSSinceClockEpoch uint64
	ClockOffsetNS              OptionalInt64
	ClockUncertaintyNS         OptionalInt64
	SubscriptionOrRequestID    OptionalString
	HTTPStatusOrWSState        OptionalString
	PayloadEncoding            PayloadEncoding
	RawPayload                 []byte
	SchemaFingerprint          OptionalHash
	TerminalOutcome            TerminalOutcome
	RecorderVersion            string
	Extensions                 []byte
}

const (
	presenceNativeSymbol uint16 = 1 << iota
	presenceInstrumentUID
	presenceConnectionEpoch
	presencePollCycle
	presenceScheduled
	presenceRequestStarted
	presenceRequestCompleted
	presenceExchangeTime
	presenceClockOffset
	presenceClockUncertainty
	presenceSubscription
	presenceHTTPState
	presenceSchemaFingerprint
)

func MarshalEnvelope(record Envelope) ([]byte, error) {
	return encodeRecord(record)
}

// AppendEnvelope appends the canonical EnvelopeV1 framing to dst. Callers that
// hash or stream many records can reuse one bounded buffer without allocating a
// second record-sized slice for every record.
func AppendEnvelope(dst []byte, record Envelope) ([]byte, error) {
	size, err := encodedRecordSize(record)
	if err != nil {
		return dst, err
	}
	dst = slices.Grow(dst, size)
	return appendEncodedRecord(dst, record, nil), nil
}

func UnmarshalEnvelope(data []byte) (Envelope, error) {
	record, consumed, err := decodeRecord(data)
	if err != nil {
		return Envelope{}, err
	}
	if consumed != len(data) {
		return Envelope{}, fmt.Errorf("%w: %d trailing envelope bytes", ErrCorrupt, len(data)-consumed)
	}
	return record, nil
}

type recordMeasurements struct {
	payloadSHA256 time.Duration
	crc32c        time.Duration
	frameSHA256   time.Duration
}

func encodeRecord(record Envelope) ([]byte, error) {
	return encodeRecordMeasured(record, nil)
}

func encodeRecordMeasured(record Envelope, measurements *recordMeasurements) ([]byte, error) {
	size, err := encodedRecordSize(record)
	if err != nil {
		return nil, err
	}
	return appendEncodedRecord(make([]byte, 0, size), record, measurements), nil
}

// encodedRecordSize validates record and returns its exact framed byte length.
// Keeping this separate lets the spool reserve a bounded frame once and append
// directly into it without retaining a second record-sized allocation.
func encodedRecordSize(record Envelope) (int, error) {
	if err := validateEnvelope(record); err != nil {
		return 0, err
	}
	body := 40
	body += 2 + len(record.SourceID)
	body += 2 + len(record.ChannelOrEndpoint)
	if record.NativeSymbol.Valid {
		body += 2 + len(record.NativeSymbol.Value)
	}
	if record.InstrumentUID.Valid {
		body += 2 + len(record.InstrumentUID.Value)
	}
	if record.ConnectionEpoch.Valid {
		body += len(record.ConnectionEpoch.Value)
	}
	if record.PollCycleID.Valid {
		body += len(record.PollCycleID.Value)
	}
	for _, value := range []OptionalInt64{
		record.ScheduledAtNS,
		record.RequestStartedAtNS,
		record.RequestCompletedAtNS,
		record.ExchangeTimeNS,
	} {
		if value.Valid {
			body += 8
		}
	}
	body += 2 + len(record.ClockEpochID)
	if record.ClockOffsetNS.Valid {
		body += 8
	}
	if record.ClockUncertaintyNS.Valid {
		body += 8
	}
	if record.SubscriptionOrRequestID.Valid {
		body += 2 + len(record.SubscriptionOrRequestID.Value)
	}
	if record.HTTPStatusOrWSState.Valid {
		body += 2 + len(record.HTTPStatusOrWSState.Value)
	}
	body += 4 + len(record.RawPayload) + sha256.Size
	if record.SchemaFingerprint.Valid {
		body += sha256.Size
	}
	body += 2 + len(record.RecorderVersion)
	body += 4 + len(record.Extensions)
	if body > MaxRecordBytes-RecordHeaderSize {
		return 0, fmt.Errorf("%w: record body has %d bytes", ErrBounds, body)
	}
	return RecordHeaderSize + body, nil
}

// appendEncodedRecord appends the accepted EnvelopeV1 representation to dst.
// The caller must reserve encodedRecordSize bytes when allocations are bounded.
func appendEncodedRecord(dst []byte, record Envelope, measurements *recordMeasurements) []byte {
	start := len(dst)
	dst = append(dst, recordMagic[:]...)
	dst = binary.LittleEndian.AppendUint16(dst, EnvelopeVersion)
	dst = binary.LittleEndian.AppendUint16(dst, RecordHeaderSize)
	dst = binary.LittleEndian.AppendUint32(dst, 0)
	dst = binary.LittleEndian.AppendUint32(dst, 0)
	bodyStart := len(dst)

	dst = append(dst, byte(record.Kind), byte(record.PayloadEncoding), byte(record.ExchangeTimeResolution), byte(record.TerminalOutcome))
	presence := envelopePresence(record)
	dst = binary.LittleEndian.AppendUint16(dst, presence)
	dst = binary.LittleEndian.AppendUint16(dst, 0)
	dst = binary.LittleEndian.AppendUint64(dst, record.ArrivalOrdinal)
	dst = binary.LittleEndian.AppendUint32(dst, record.MessageOrdinal)
	dst = binary.LittleEndian.AppendUint32(dst, 0)
	dst = binary.LittleEndian.AppendUint64(dst, uint64(record.ReceivedWallTimeNS))
	dst = binary.LittleEndian.AppendUint64(dst, record.MonotonicNSSinceClockEpoch)
	dst = appendString(dst, record.SourceID)
	dst = appendString(dst, record.ChannelOrEndpoint)
	if record.NativeSymbol.Valid {
		dst = appendString(dst, record.NativeSymbol.Value)
	}
	if record.InstrumentUID.Valid {
		dst = appendString(dst, record.InstrumentUID.Value)
	}
	if record.ConnectionEpoch.Valid {
		dst = append(dst, record.ConnectionEpoch.Value[:]...)
	}
	if record.PollCycleID.Valid {
		dst = append(dst, record.PollCycleID.Value[:]...)
	}
	dst = appendOptionalInt64(dst, record.ScheduledAtNS)
	dst = appendOptionalInt64(dst, record.RequestStartedAtNS)
	dst = appendOptionalInt64(dst, record.RequestCompletedAtNS)
	dst = appendOptionalInt64(dst, record.ExchangeTimeNS)
	dst = appendString(dst, record.ClockEpochID)
	dst = appendOptionalInt64(dst, record.ClockOffsetNS)
	dst = appendOptionalInt64(dst, record.ClockUncertaintyNS)
	if record.SubscriptionOrRequestID.Valid {
		dst = appendString(dst, record.SubscriptionOrRequestID.Value)
	}
	if record.HTTPStatusOrWSState.Valid {
		dst = appendString(dst, record.HTTPStatusOrWSState.Value)
	}
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(record.RawPayload)))
	dst = append(dst, record.RawPayload...)
	hashStarted := time.Now()
	payloadHash := sha256.Sum256(record.RawPayload)
	if measurements != nil {
		measurements.payloadSHA256 += time.Since(hashStarted)
	}
	dst = append(dst, payloadHash[:]...)
	if record.SchemaFingerprint.Valid {
		dst = append(dst, record.SchemaFingerprint.Value[:]...)
	}
	dst = appendString(dst, record.RecorderVersion)
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(record.Extensions)))
	dst = append(dst, record.Extensions...)

	body := dst[bodyStart:]
	binary.LittleEndian.PutUint32(dst[start+8:start+12], uint32(len(body)))
	crcStarted := time.Now()
	checksum := crc32.Checksum(body, crc32cTable)
	if measurements != nil {
		measurements.crc32c += time.Since(crcStarted)
	}
	binary.LittleEndian.PutUint32(dst[start+12:start+16], checksum)
	return dst
}

func decodeRecord(src []byte) (Envelope, int, error) {
	var record Envelope
	if len(src) < RecordHeaderSize {
		return record, 0, fmt.Errorf("%w: record header needs %d bytes, got %d", ErrTruncated, RecordHeaderSize, len(src))
	}
	if string(src[:4]) != string(recordMagic[:]) {
		return record, 0, fmt.Errorf("%w: invalid record magic", ErrCorrupt)
	}
	version := binary.LittleEndian.Uint16(src[4:6])
	if version != EnvelopeVersion {
		return record, 0, fmt.Errorf("%w: envelope version %d", ErrVersion, version)
	}
	if size := binary.LittleEndian.Uint16(src[6:8]); size != RecordHeaderSize {
		return record, 0, fmt.Errorf("%w: record header size %d", ErrVersion, size)
	}
	bodyLen := int(binary.LittleEndian.Uint32(src[8:12]))
	if bodyLen > MaxRecordBytes-RecordHeaderSize {
		return record, 0, fmt.Errorf("%w: record body has %d bytes", ErrBounds, bodyLen)
	}
	total := RecordHeaderSize + bodyLen
	if len(src) < total {
		return record, 0, fmt.Errorf("%w: record needs %d bytes, got %d", ErrTruncated, total, len(src))
	}
	body := src[RecordHeaderSize:total]
	wantCRC := binary.LittleEndian.Uint32(src[12:16])
	if got := crc32.Checksum(body, crc32cTable); got != wantCRC {
		return record, 0, fmt.Errorf("%w: record CRC32C got %08x want %08x", ErrCorrupt, got, wantCRC)
	}

	cursor := byteCursor{data: body}
	kind, err := cursor.u8()
	if err != nil {
		return record, 0, err
	}
	encoding, err := cursor.u8()
	if err != nil {
		return record, 0, err
	}
	resolution, err := cursor.u8()
	if err != nil {
		return record, 0, err
	}
	outcome, err := cursor.u8()
	if err != nil {
		return record, 0, err
	}
	presence, err := cursor.u16()
	if err != nil {
		return record, 0, err
	}
	if presence & ^uint16((1<<13)-1) != 0 {
		return record, 0, fmt.Errorf("%w: unknown envelope presence bits %04x", ErrVersion, presence)
	}
	reserved, err := cursor.u16()
	if err != nil {
		return record, 0, err
	}
	if reserved != 0 {
		return record, 0, fmt.Errorf("%w: nonzero envelope reserved field", ErrVersion)
	}
	arrival, err := cursor.u64()
	if err != nil {
		return record, 0, err
	}
	message, err := cursor.u32()
	if err != nil {
		return record, 0, err
	}
	reserved32, err := cursor.u32()
	if err != nil {
		return record, 0, err
	}
	if reserved32 != 0 {
		return record, 0, fmt.Errorf("%w: nonzero envelope reserved field", ErrVersion)
	}
	received, err := cursor.i64()
	if err != nil {
		return record, 0, err
	}
	monotonic, err := cursor.u64()
	if err != nil {
		return record, 0, err
	}
	source, err := cursor.string(MaxSourceIDBytes)
	if err != nil {
		return record, 0, err
	}
	contract, err := cursor.string(MaxContractIDBytes)
	if err != nil {
		return record, 0, err
	}

	record.Kind = RecordKind(kind)
	record.PayloadEncoding = PayloadEncoding(encoding)
	record.ExchangeTimeResolution = TimeResolution(resolution)
	record.TerminalOutcome = TerminalOutcome(outcome)
	record.ArrivalOrdinal = arrival
	record.MessageOrdinal = message
	record.ReceivedWallTimeNS = received
	record.MonotonicNSSinceClockEpoch = monotonic
	record.SourceID = source
	record.ChannelOrEndpoint = contract
	if err := decodeOptionalFields(&cursor, presence, &record); err != nil {
		return Envelope{}, 0, err
	}
	if err := validateEnvelope(record); err != nil {
		return Envelope{}, 0, fmt.Errorf("%w: decoded envelope: %v", ErrCorrupt, err)
	}
	if cursor.remaining() != 0 {
		return Envelope{}, 0, fmt.Errorf("%w: %d trailing record bytes", ErrCorrupt, cursor.remaining())
	}
	return record, total, nil
}

func decodeOptionalFields(cursor *byteCursor, presence uint16, record *Envelope) error {
	var err error
	if presence&presenceNativeSymbol != 0 {
		record.NativeSymbol.Value, err = cursor.string(MaxSymbolBytes)
		record.NativeSymbol.Valid = err == nil
		if err != nil {
			return err
		}
	}
	if presence&presenceInstrumentUID != 0 {
		record.InstrumentUID.Value, err = cursor.string(MaxIdentityBytes)
		record.InstrumentUID.Valid = err == nil
		if err != nil {
			return err
		}
	}
	if presence&presenceConnectionEpoch != 0 {
		epoch, takeErr := cursor.take(16)
		if takeErr != nil {
			return takeErr
		}
		copy(record.ConnectionEpoch.Value[:], epoch)
		record.ConnectionEpoch.Valid = true
	}
	if presence&presencePollCycle != 0 {
		epoch, takeErr := cursor.take(16)
		if takeErr != nil {
			return takeErr
		}
		copy(record.PollCycleID.Value[:], epoch)
		record.PollCycleID.Valid = true
	}
	if record.ScheduledAtNS, err = cursor.optionalInt64(presence&presenceScheduled != 0); err != nil {
		return err
	}
	if record.RequestStartedAtNS, err = cursor.optionalInt64(presence&presenceRequestStarted != 0); err != nil {
		return err
	}
	if record.RequestCompletedAtNS, err = cursor.optionalInt64(presence&presenceRequestCompleted != 0); err != nil {
		return err
	}
	if record.ExchangeTimeNS, err = cursor.optionalInt64(presence&presenceExchangeTime != 0); err != nil {
		return err
	}
	if record.ClockEpochID, err = cursor.string(MaxClockEpochIDBytes); err != nil {
		return err
	}
	if record.ClockOffsetNS, err = cursor.optionalInt64(presence&presenceClockOffset != 0); err != nil {
		return err
	}
	if record.ClockUncertaintyNS, err = cursor.optionalInt64(presence&presenceClockUncertainty != 0); err != nil {
		return err
	}
	if presence&presenceSubscription != 0 {
		record.SubscriptionOrRequestID.Value, err = cursor.string(MaxIdentityBytes)
		record.SubscriptionOrRequestID.Valid = err == nil
		if err != nil {
			return err
		}
	}
	if presence&presenceHTTPState != 0 {
		record.HTTPStatusOrWSState.Value, err = cursor.string(MaxIdentityBytes)
		record.HTTPStatusOrWSState.Valid = err == nil
		if err != nil {
			return err
		}
	}
	payloadLen, err := cursor.u32()
	if err != nil {
		return err
	}
	if payloadLen > MaxPayloadBytes {
		return fmt.Errorf("%w: payload has %d bytes", ErrBounds, payloadLen)
	}
	payload, err := cursor.take(int(payloadLen))
	if err != nil {
		return err
	}
	record.RawPayload = append([]byte(nil), payload...)
	storedHash, err := cursor.take(sha256.Size)
	if err != nil {
		return err
	}
	actualHash := sha256.Sum256(payload)
	if string(storedHash) != string(actualHash[:]) {
		return ErrPayloadHash
	}
	if presence&presenceSchemaFingerprint != 0 {
		hash, takeErr := cursor.take(sha256.Size)
		if takeErr != nil {
			return takeErr
		}
		copy(record.SchemaFingerprint.Value[:], hash)
		record.SchemaFingerprint.Valid = true
	}
	if record.RecorderVersion, err = cursor.string(MaxRecorderVersionBytes); err != nil {
		return err
	}
	extensionLen, err := cursor.u32()
	if err != nil {
		return err
	}
	if extensionLen > MaxExtensionBytes {
		return fmt.Errorf("%w: extensions have %d bytes", ErrBounds, extensionLen)
	}
	extensions, err := cursor.take(int(extensionLen))
	if err != nil {
		return err
	}
	record.Extensions = append([]byte(nil), extensions...)
	return nil
}

func validateEnvelope(record Envelope) error {
	if record.Kind < RecordKindWebSocket || record.Kind > RecordKindControl {
		return fmt.Errorf("%w: record kind %d", ErrBounds, record.Kind)
	}
	if record.PayloadEncoding < PayloadEncodingJSON || record.PayloadEncoding > PayloadEncodingNone {
		return fmt.Errorf("%w: payload encoding %d", ErrBounds, record.PayloadEncoding)
	}
	if record.PayloadEncoding == PayloadEncodingNone && len(record.RawPayload) != 0 {
		return fmt.Errorf("%w: none payload encoding with %d payload bytes", ErrBounds, len(record.RawPayload))
	}
	if record.ExchangeTimeResolution > TimeResolutionSecond {
		return fmt.Errorf("%w: exchange time resolution %d", ErrBounds, record.ExchangeTimeResolution)
	}
	if record.ExchangeTimeNS.Valid != (record.ExchangeTimeResolution != TimeResolutionAbsent) {
		return fmt.Errorf("%w: exchange time and resolution presence differ", ErrBounds)
	}
	if record.TerminalOutcome < OutcomeObserved || record.TerminalOutcome > OutcomeDisconnected {
		return fmt.Errorf("%w: terminal outcome %d", ErrBounds, record.TerminalOutcome)
	}
	if record.SourceID == "" || len(record.SourceID) > MaxSourceIDBytes {
		return fmt.Errorf("%w: source ID has %d bytes", ErrBounds, len(record.SourceID))
	}
	if record.ChannelOrEndpoint == "" || len(record.ChannelOrEndpoint) > MaxContractIDBytes {
		return fmt.Errorf("%w: contract ID has %d bytes", ErrBounds, len(record.ChannelOrEndpoint))
	}
	if record.ClockEpochID == "" || len(record.ClockEpochID) > MaxClockEpochIDBytes {
		return fmt.Errorf("%w: clock epoch ID has %d bytes", ErrBounds, len(record.ClockEpochID))
	}
	if record.RecorderVersion == "" || len(record.RecorderVersion) > MaxRecorderVersionBytes {
		return fmt.Errorf("%w: recorder version has %d bytes", ErrBounds, len(record.RecorderVersion))
	}
	if len(record.RawPayload) > MaxPayloadBytes {
		return fmt.Errorf("%w: payload has %d bytes", ErrBounds, len(record.RawPayload))
	}
	if len(record.Extensions) > MaxExtensionBytes {
		return fmt.Errorf("%w: extensions have %d bytes", ErrBounds, len(record.Extensions))
	}
	if record.NativeSymbol.Valid && len(record.NativeSymbol.Value) > MaxSymbolBytes {
		return fmt.Errorf("%w: native symbol has %d bytes", ErrBounds, len(record.NativeSymbol.Value))
	}
	if record.InstrumentUID.Valid && len(record.InstrumentUID.Value) > MaxIdentityBytes {
		return fmt.Errorf("%w: instrument UID has %d bytes", ErrBounds, len(record.InstrumentUID.Value))
	}
	if record.SubscriptionOrRequestID.Valid && len(record.SubscriptionOrRequestID.Value) > MaxIdentityBytes {
		return fmt.Errorf("%w: subscription/request ID has %d bytes", ErrBounds, len(record.SubscriptionOrRequestID.Value))
	}
	if record.HTTPStatusOrWSState.Valid && len(record.HTTPStatusOrWSState.Value) > MaxIdentityBytes {
		return fmt.Errorf("%w: HTTP/WS state has %d bytes", ErrBounds, len(record.HTTPStatusOrWSState.Value))
	}
	return nil
}

func envelopePresence(record Envelope) uint16 {
	var presence uint16
	if record.NativeSymbol.Valid {
		presence |= presenceNativeSymbol
	}
	if record.InstrumentUID.Valid {
		presence |= presenceInstrumentUID
	}
	if record.ConnectionEpoch.Valid {
		presence |= presenceConnectionEpoch
	}
	if record.PollCycleID.Valid {
		presence |= presencePollCycle
	}
	if record.ScheduledAtNS.Valid {
		presence |= presenceScheduled
	}
	if record.RequestStartedAtNS.Valid {
		presence |= presenceRequestStarted
	}
	if record.RequestCompletedAtNS.Valid {
		presence |= presenceRequestCompleted
	}
	if record.ExchangeTimeNS.Valid {
		presence |= presenceExchangeTime
	}
	if record.ClockOffsetNS.Valid {
		presence |= presenceClockOffset
	}
	if record.ClockUncertaintyNS.Valid {
		presence |= presenceClockUncertainty
	}
	if record.SubscriptionOrRequestID.Valid {
		presence |= presenceSubscription
	}
	if record.HTTPStatusOrWSState.Valid {
		presence |= presenceHTTPState
	}
	if record.SchemaFingerprint.Valid {
		presence |= presenceSchemaFingerprint
	}
	return presence
}

func appendString(dst []byte, value string) []byte {
	dst = binary.LittleEndian.AppendUint16(dst, uint16(len(value)))
	return append(dst, value...)
}

func appendOptionalInt64(dst []byte, value OptionalInt64) []byte {
	if !value.Valid {
		return dst
	}
	return binary.LittleEndian.AppendUint64(dst, uint64(value.Value))
}

type byteCursor struct {
	data []byte
	off  int
}

func (c *byteCursor) remaining() int { return len(c.data) - c.off }

func (c *byteCursor) take(n int) ([]byte, error) {
	if n < 0 || n > c.remaining() {
		return nil, fmt.Errorf("%w: need %d bytes with %d remaining", ErrTruncated, n, c.remaining())
	}
	value := c.data[c.off : c.off+n]
	c.off += n
	return value, nil
}

func (c *byteCursor) u8() (uint8, error) {
	value, err := c.take(1)
	if err != nil {
		return 0, err
	}
	return value[0], nil
}

func (c *byteCursor) u16() (uint16, error) {
	value, err := c.take(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(value), nil
}

func (c *byteCursor) u32() (uint32, error) {
	value, err := c.take(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(value), nil
}

func (c *byteCursor) u64() (uint64, error) {
	value, err := c.take(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(value), nil
}

func (c *byteCursor) i64() (int64, error) {
	value, err := c.u64()
	return int64(value), err
}

func (c *byteCursor) string(maxBytes int) (string, error) {
	length, err := c.u16()
	if err != nil {
		return "", err
	}
	if int(length) > maxBytes {
		return "", fmt.Errorf("%w: string has %d bytes, maximum %d", ErrBounds, length, maxBytes)
	}
	value, err := c.take(int(length))
	if err != nil {
		return "", err
	}
	return string(value), nil
}

func (c *byteCursor) optionalInt64(valid bool) (OptionalInt64, error) {
	if !valid {
		return OptionalInt64{}, nil
	}
	value, err := c.i64()
	return OptionalInt64{Value: value, Valid: err == nil}, err
}
