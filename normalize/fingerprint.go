package normalize

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	MaxFingerprintPayloadBytes = 1 << 20
	MaxFingerprintDepth        = 32
	MaxFingerprintFields       = 512
	MaxFingerprintArrayItems   = 20_000
	MaxFingerprintNodes        = 100_000
	MaxFingerprintStringBytes  = 4096
	MaxFingerprintNumberBytes  = 128
)

var ErrInvalidFingerprintInput = errors.New("normalize: invalid schema fingerprint input")

type FingerprintClass string

const (
	FingerprintExact               FingerprintClass = "exact"
	FingerprintAdditiveHarmless    FingerprintClass = "additive_harmless"
	FingerprintSemanticAdditive    FingerprintClass = "semantic_additive"
	FingerprintTypeOrMeaningChange FingerprintClass = "type_or_meaning_change"
	FingerprintUnknown             FingerprintClass = "unknown"
)

type FingerprintRule struct {
	Fingerprint          Hash
	Class                FingerprintClass
	AllowedUnknownFields []string
}

type FingerprintObservation struct {
	Fingerprint     Hash
	EncodingVersion uint16
}

const FingerprintEncodingVersion uint16 = 1

func StructuralFingerprint(payload []byte) (FingerprintObservation, error) {
	if len(payload) == 0 || len(payload) > MaxFingerprintPayloadBytes || !utf8.Valid(payload) {
		return FingerprintObservation{}, fmt.Errorf("%w: payload byte bound", ErrInvalidFingerprintInput)
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return FingerprintObservation{}, fmt.Errorf("%w: malformed JSON", ErrInvalidFingerprintInput)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return FingerprintObservation{}, fmt.Errorf("%w: trailing JSON value", ErrInvalidFingerprintInput)
	}
	nodes := 0
	shape, err := fingerprintShape(value, 1, &nodes)
	if err != nil {
		return FingerprintObservation{}, err
	}
	var framed canonicalEncoder
	framed.string("structural-schema-fingerprint")
	framed.u16(FingerprintEncodingVersion)
	framed.string(shape)
	return FingerprintObservation{Fingerprint: Hash(sha256.Sum256(framed.bytes)), EncodingVersion: FingerprintEncodingVersion}, nil
}

func fingerprintShape(value any, depth int, nodes *int) (string, error) {
	*nodes = *nodes + 1
	if *nodes > MaxFingerprintNodes || depth > MaxFingerprintDepth {
		return "", fmt.Errorf("%w: structural depth or node bound", ErrInvalidFingerprintInput)
	}
	switch value := value.(type) {
	case nil:
		return "null", nil
	case bool:
		return "boolean", nil
	case string:
		if len(value) > MaxFingerprintStringBytes {
			return "", fmt.Errorf("%w: string byte bound", ErrInvalidFingerprintInput)
		}
		return "string", nil
	case json.Number:
		text := string(value)
		if len(text) == 0 || len(text) > MaxFingerprintNumberBytes {
			return "", fmt.Errorf("%w: number byte bound", ErrInvalidFingerprintInput)
		}
		if strings.ContainsAny(text, "eE") {
			return "number:exponent", nil
		}
		if strings.Contains(text, ".") {
			return "number:decimal", nil
		}
		return "number:integer", nil
	case []any:
		if len(value) > MaxFingerprintArrayItems {
			return "", fmt.Errorf("%w: array item bound", ErrInvalidFingerprintInput)
		}
		if len(value) == 0 {
			return "array:empty", nil
		}
		shapes := make([]string, 0, len(value))
		for _, item := range value {
			shape, err := fingerprintShape(item, depth+1, nodes)
			if err != nil {
				return "", err
			}
			shapes = append(shapes, shape)
		}
		slices.Sort(shapes)
		shapes = slices.Compact(shapes)
		return "array[" + strings.Join(shapes, ",") + "]", nil
	case map[string]any:
		if len(value) > MaxFingerprintFields {
			return "", fmt.Errorf("%w: object field bound", ErrInvalidFingerprintInput)
		}
		keys := slices.Sorted(maps.Keys(value))
		var builder strings.Builder
		builder.WriteString("object{")
		for _, key := range keys {
			if key == "" || len(key) > MaxFingerprintStringBytes {
				return "", fmt.Errorf("%w: object key bound", ErrInvalidFingerprintInput)
			}
			shape, err := fingerprintShape(value[key], depth+1, nodes)
			if err != nil {
				return "", err
			}
			builder.WriteString(key)
			builder.WriteByte(':')
			builder.WriteString(shape)
			builder.WriteByte(';')
		}
		builder.WriteByte('}')
		return builder.String(), nil
	default:
		return "", fmt.Errorf("%w: unsupported JSON representation", ErrInvalidFingerprintInput)
	}
}
