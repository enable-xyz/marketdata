package quality

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"strings"
)

const (
	redactedText      = "[redacted]"
	maxHandlerAttrs   = 64
	maxStartupSupport = maxHandlerAttrs - 4
)

var (
	boundaryNamePattern = regexp.MustCompile(`^[a-z][a-z0-9]*(?:[._-][a-z0-9]+)*$`)
	identityPattern     = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/@+=-]{0,255}$`)
)

// BoundaryEvent is a fixed process, connection, segment, or dataset lifecycle event.
type BoundaryEvent string

const (
	BoundaryProcessStartup    BoundaryEvent = "process.startup"
	BoundaryProcessShutdown   BoundaryEvent = "process.shutdown"
	BoundaryConnectionOpened  BoundaryEvent = "connection.opened"
	BoundaryConnectionClosed  BoundaryEvent = "connection.closed"
	BoundarySegmentCommitted  BoundaryEvent = "segment.committed"
	BoundaryDatasetCommitted  BoundaryEvent = "dataset.committed"
	BoundaryTelemetryDegraded BoundaryEvent = "telemetry.degraded"
)

// RawCoordinate identifies one durable payload location without accepting
// free-form caller material.
type RawCoordinate struct {
	SegmentSHA256 [sha256.Size]byte
	EpochOrdinal  uint64
}

// RawReference is the only value accepted by the logging boundary for raw data.
// It contains payload and segment digests plus an ordinal, never source bytes.
type RawReference struct {
	payloadDigest [sha256.Size]byte
	coordinate    RawCoordinate
}

// NewRawReference reduces raw bytes to a SHA-256 digest plus a structural
// durable coordinate. The returned value owns no reference to payload.
func NewRawReference(payload []byte, coordinate RawCoordinate) (RawReference, error) {
	if coordinate.SegmentSHA256 == ([sha256.Size]byte{}) {
		return RawReference{}, telemetryError("raw coordinate is invalid")
	}
	return RawReference{payloadDigest: sha256.Sum256(payload), coordinate: coordinate}, nil
}

func (r RawReference) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("payload_sha256", hex.EncodeToString(r.payloadDigest[:])),
		slog.String("segment_sha256", hex.EncodeToString(r.coordinate.SegmentSHA256[:])),
		slog.Uint64("epoch_ordinal", r.coordinate.EpochOrdinal),
	)
}

type publicText string

func (v publicText) LogValue() slog.Value { return slog.StringValue(string(v)) }

type evidenceDigest [sha256.Size]byte

func (d evidenceDigest) LogValue() slog.Value {
	return slog.StringValue(hex.EncodeToString(d[:]))
}

// RedactedValue is an explicit value for callers that need to retain an
// attribute's shape while proving its material cannot cross the boundary.
type RedactedValue struct{}

func (RedactedValue) LogValue() slog.Value { return slog.StringValue(redactedText) }

// RedactingHandler applies a deny-by-default attribute policy. Unknown Any
// values, errors, URLs, headers, byte slices, secret-bearing keys, and nested
// LogValuers are replaced without evaluating String or Error methods.
type RedactingHandler struct {
	next           slog.Handler
	sensitiveGroup bool
}

func NewRedactingHandler(next slog.Handler) *RedactingHandler {
	if next == nil {
		next = slog.NewTextHandler(io.Discard, nil)
	}
	return &RedactingHandler{next: next}
}

func (h *RedactingHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.next.Enabled(ctx, level)
}

func (h *RedactingHandler) Handle(ctx context.Context, record slog.Record) error {
	message := record.Message
	if !validBoundaryName(message) {
		message = "boundary.redacted"
	}
	clean := slog.NewRecord(record.Time, record.Level, message, record.PC)
	count := 0
	record.Attrs(func(attr slog.Attr) bool {
		if count >= maxHandlerAttrs {
			return false
		}
		clean.AddAttrs(h.cleanAttr(attr, 0))
		count++
		return true
	})
	return h.next.Handle(ctx, clean)
}

func (h *RedactingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	clean := make([]slog.Attr, 0, min(len(attrs), maxHandlerAttrs))
	for _, attr := range attrs[:min(len(attrs), maxHandlerAttrs)] {
		clean = append(clean, h.cleanAttr(attr, 0))
	}
	return &RedactingHandler{next: h.next.WithAttrs(clean), sensitiveGroup: h.sensitiveGroup}
}

func (h *RedactingHandler) WithGroup(name string) slog.Handler {
	if sensitiveKey(name) || !safeGroupKey(name) {
		return &RedactingHandler{next: h.next.WithGroup("redacted"), sensitiveGroup: true}
	}
	return &RedactingHandler{next: h.next.WithGroup(name), sensitiveGroup: h.sensitiveGroup}
}

func (h *RedactingHandler) cleanAttr(attr slog.Attr, depth int) slog.Attr {
	if attr.Equal(slog.Attr{}) {
		return slog.Attr{}
	}
	if depth > 8 || h.sensitiveGroup {
		return slog.String("value", redactedText)
	}
	value := attr.Value
	isGroup := value.Kind() == slog.KindGroup
	if sensitiveKey(attr.Key) {
		return slog.String(safeAttrKey(attr.Key), redactedText)
	}
	if raw, ok := markerRawReference(value); ok {
		if !safeAttributeKey(attr.Key) {
			return slog.String(safeAttrKey(attr.Key), redactedText)
		}
		return slog.Any(attr.Key, raw)
	}
	if digest, ok := markerEvidenceDigest(value); ok {
		if attr.Key != "schema" && attr.Key != "mapper" && attr.Key != "config" {
			return slog.String(safeAttrKey(attr.Key), redactedText)
		}
		return slog.Any(attr.Key, digest)
	}
	if text, ok := markerPublicText(value); ok {
		if !safeAttributeKey(attr.Key) {
			return slog.String(safeAttrKey(attr.Key), redactedText)
		}
		return slog.String(attr.Key, string(text))
	}
	if _, ok := markerRedacted(value); ok {
		return slog.String(safeAttrKey(attr.Key), redactedText)
	}
	if !safeAttributeKey(attr.Key) && !(isGroup && safeGroupKey(attr.Key)) {
		return slog.String(safeAttrKey(attr.Key), redactedText)
	}
	switch value.Kind() {
	case slog.KindGroup:
		group := value.Group()
		clean := make([]slog.Attr, 0, min(len(group), maxHandlerAttrs))
		for _, child := range group[:min(len(group), maxHandlerAttrs)] {
			clean = append(clean, h.cleanAttr(child, depth+1))
		}
		return slog.Attr{Key: attr.Key, Value: slog.GroupValue(clean...)}
	case slog.KindString:
		if value.String() != redactedText {
			return slog.String(attr.Key, redactedText)
		}
		return attr
	case slog.KindBool, slog.KindDuration, slog.KindFloat64, slog.KindInt64, slog.KindTime, slog.KindUint64:
		return attr
	default:
		return slog.String(attr.Key, redactedText)
	}
}

func markerRawReference(value slog.Value) (RawReference, bool) {
	if value.Kind() != slog.KindLogValuer {
		return RawReference{}, false
	}
	raw, ok := value.Any().(RawReference)
	return raw, ok
}

func markerEvidenceDigest(value slog.Value) (evidenceDigest, bool) {
	if value.Kind() != slog.KindLogValuer {
		return evidenceDigest{}, false
	}
	digest, ok := value.Any().(evidenceDigest)
	return digest, ok
}
func markerPublicText(value slog.Value) (publicText, bool) {
	if value.Kind() != slog.KindLogValuer {
		return "", false
	}
	text, ok := value.Any().(publicText)
	return text, ok
}

func markerRedacted(value slog.Value) (RedactedValue, bool) {
	if value.Kind() != slog.KindLogValuer {
		return RedactedValue{}, false
	}
	redacted, ok := value.Any().(RedactedValue)
	return redacted, ok
}

func sensitiveKey(key string) bool {
	words := strings.FieldsFunc(strings.ToLower(key), func(r rune) bool {
		return r == '_' || r == '-' || r == '.' || r == '/' || r == ' '
	})
	for _, word := range words {
		switch word {
		case "raw", "payload", "body", "bearer", "authorization", "auth", "header", "headers", "cookie",
			"credential", "credentials", "secret", "token", "password", "tls", "certificate", "cert", "private",
			"key", "signature", "hash", "digest", "dsn", "paging", "url", "uri", "endpoint":
			return true
		}
	}
	return false
}

func safeAttributeKey(key string) bool {
	switch key {
	case "event", "source", "channel", "family", "outcome", "state", "phase", "transport", "status", "code",
		"api", "scope_kind", "operation", "attempt", "count", "bytes", "duration", "latency", "queue_depth",
		"capacity", "version", "commit", "build_date", "channels", "families", "complete", "dropped", "reason_code":
		return true
	default:
		return strings.HasPrefix(key, "contract_") && len(key) <= 32
	}
}

func safeGroupKey(key string) bool {
	return safeAttributeKey(key) || key == "build" || key == "support"
}

func safeAttrKey(key string) string {
	if boundaryNamePattern.MatchString(key) && len(key) <= 64 {
		return key
	}
	return "value"
}

func validIdentity(value string) bool {
	return identityPattern.MatchString(value) && !strings.Contains(strings.ToLower(value), "bearer")
}

func validBoundaryName(value string) bool {
	return len(value) <= 128 && boundaryNamePattern.MatchString(value)
}

// BoundaryLogger emits only fixed boundary names through a redacting handler.
type BoundaryLogger struct {
	logger *slog.Logger
}

func NewBoundaryLogger(logger *slog.Logger) *BoundaryLogger {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &BoundaryLogger{logger: slog.New(NewRedactingHandler(logger.Handler()))}
}

func (l *BoundaryLogger) Event(ctx context.Context, level slog.Level, event BoundaryEvent, attrs ...slog.Attr) {
	name := string(event)
	if !slices.Contains([]BoundaryEvent{
		BoundaryProcessStartup, BoundaryProcessShutdown, BoundaryConnectionOpened, BoundaryConnectionClosed,
		BoundarySegmentCommitted, BoundaryDatasetCommitted, BoundaryTelemetryDegraded,
	}, event) {
		name = "boundary.unknown"
	}
	l.logger.LogAttrs(ctx, level, name, attrs...)
}

// StartupEvidence is caller-composed immutable evidence. Its digest fields are
// emitted through internal marker values that cannot be used for token hashes.
type StartupEvidence struct {
	BuildVersion string
	BuildCommit  string
	BuildDate    string
	SchemaDigest [sha256.Size]byte
	MapperDigest [sha256.Size]byte
	ConfigDigest [sha256.Size]byte
	Support      []SupportContract
}

type SupportContract struct {
	SourceID string
	API      string
	Channels []string
	Families []string
}

func (e StartupEvidence) Validate() error {
	for name, value := range map[string]string{
		"build version": e.BuildVersion,
		"build commit":  e.BuildCommit,
		"build date":    e.BuildDate,
	} {
		if !validIdentity(value) {
			return telemetryError(name + " is invalid")
		}
	}
	for name, value := range map[string][sha256.Size]byte{
		"schema digest": e.SchemaDigest,
		"mapper digest": e.MapperDigest,
		"config digest": e.ConfigDigest,
	} {
		if value == ([sha256.Size]byte{}) {
			return telemetryError(name + " is required")
		}
	}
	if len(e.Support) > maxStartupSupport {
		return telemetryError("support contracts exceed startup emission bound")
	}
	seen := make(map[string]struct{}, len(e.Support))
	for _, contract := range e.Support {
		if !validIdentity(contract.SourceID) || !validIdentity(contract.API) || len(contract.Channels) == 0 || len(contract.Families) == 0 {
			return telemetryError("support contract is incomplete")
		}
		key := contract.SourceID + "\x00" + contract.API
		if _, exists := seen[key]; exists {
			return telemetryError("support contract is duplicated")
		}
		seen[key] = struct{}{}
		for _, values := range [][]string{contract.Channels, contract.Families} {
			if len(values) > 256 || !slices.IsSorted(values) {
				return telemetryError("support contract values must be bounded and sorted")
			}
			for index, value := range values {
				if !validIdentity(value) || (index > 0 && value == values[index-1]) {
					return telemetryError("support contract values must be unique identities")
				}
			}
		}
	}
	return nil
}

func (l *BoundaryLogger) Startup(ctx context.Context, evidence StartupEvidence) error {
	if err := evidence.Validate(); err != nil {
		return err
	}
	attrs := []slog.Attr{
		slog.Group("build",
			slog.Any("version", publicText(evidence.BuildVersion)),
			slog.Any("commit", publicText(evidence.BuildCommit)),
			slog.Any("build_date", publicText(evidence.BuildDate)),
		),
		slog.Any("schema", evidenceDigest(evidence.SchemaDigest)),
		slog.Any("mapper", evidenceDigest(evidence.MapperDigest)),
		slog.Any("config", evidenceDigest(evidence.ConfigDigest)),
	}
	for index, contract := range evidence.Support {
		attrs = append(attrs, slog.Group(
			"contract_"+decimal(index),
			slog.Any("source", publicText(contract.SourceID)),
			slog.Any("api", publicText(contract.API)),
			slog.Any("channels", publicText(strings.Join(contract.Channels, ","))),
			slog.Any("families", publicText(strings.Join(contract.Families, ","))),
		))
	}
	l.Event(ctx, slog.LevelInfo, BoundaryProcessStartup, attrs...)
	return nil
}

func decimal(value int) string {
	if value == 0 {
		return "0"
	}
	var buffer [20]byte
	position := len(buffer)
	for value > 0 {
		position--
		buffer[position] = byte('0' + value%10)
		value /= 10
	}
	return string(buffer[position:])
}

func telemetryError(message string) error {
	return &TelemetryError{message: message}
}

// TelemetryError deliberately returns only invariant text supplied by this
// package; it never retains rejected material.
type TelemetryError struct{ message string }

func (e *TelemetryError) Error() string { return "quality: telemetry " + e.message }

// Compile-time check that the handler retains the standard slog contract.
var _ slog.Handler = (*RedactingHandler)(nil)
