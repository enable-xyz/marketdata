package quality

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
	"strings"
)

var ErrInvalidSchemaObservation = errors.New("quality: invalid schema observation")

type SchemaClassification string

const (
	SchemaAdditiveUnknownField       SchemaClassification = "additive_unknown_field"
	SchemaKnownOptionalFieldAbsent   SchemaClassification = "known_optional_field_absent"
	SchemaNonsemanticTypeShapeChange SchemaClassification = "nonsemantic_type_or_shape_change"
	SchemaSemanticChange             SchemaClassification = "semantic_field_change"
	SchemaUnknownMessageRole         SchemaClassification = "unknown_message_role"
)

type NormalizationDisposition string

const (
	NormalizationContinue   NormalizationDisposition = "continue"
	NormalizationTransition NormalizationDisposition = "transition_table"
	NormalizationQuarantine NormalizationDisposition = "quarantine"
	NormalizationFailClosed NormalizationDisposition = "fail_closed"
)

type SchemaAction struct {
	RawAccepted   bool
	Normalization NormalizationDisposition
}

// SchemaDriftAction keeps raw acceptance independent from normalization. Only
// an unsafe frame/storage condition outside this model may stop raw capture.
func SchemaDriftAction(classification SchemaClassification, parserCanPreserve, optionalAbsenceResolved bool) (SchemaAction, error) {
	action := SchemaAction{RawAccepted: true}
	switch classification {
	case SchemaAdditiveUnknownField:
		action.Normalization = NormalizationContinue
	case SchemaKnownOptionalFieldAbsent:
		if optionalAbsenceResolved {
			action.Normalization = NormalizationTransition
		} else {
			action.Normalization = NormalizationQuarantine
		}
	case SchemaNonsemanticTypeShapeChange:
		if parserCanPreserve {
			action.Normalization = NormalizationContinue
		} else {
			action.Normalization = NormalizationQuarantine
		}
	case SchemaSemanticChange:
		action.Normalization = NormalizationFailClosed
	case SchemaUnknownMessageRole:
		action.Normalization = NormalizationQuarantine
	default:
		return SchemaAction{}, fmt.Errorf("%w: classification %q", ErrInvalidSchemaObservation, classification)
	}
	return action, nil
}

type SchemaObservation struct {
	SourceID                string
	ChannelID               string
	Fingerprint             [sha256.Size]byte
	FirstSeenTimeNS         int64
	LastSeenTimeNS          int64
	ObservationCount        uint64
	Classification          SchemaClassification
	ParserCanPreserve       bool
	OptionalAbsenceResolved bool
	FirstRawCoordinate      json.RawMessage
	LastRawCoordinate       json.RawMessage
	RedactedSample          json.RawMessage
	MapperDisposition       NormalizationDisposition
	MapperReleaseID         string
}

func (o SchemaObservation) Validate() error {
	for _, field := range []struct {
		name, value string
		required    bool
	}{
		{"source_id", o.SourceID, true}, {"channel_id", o.ChannelID, true}, {"mapper_release_id", o.MapperReleaseID, false},
	} {
		if err := validateQualityString(field.name, field.value, field.required); err != nil {
			return errors.Join(ErrInvalidSchemaObservation, err)
		}
	}
	if o.Fingerprint == ([sha256.Size]byte{}) || o.FirstSeenTimeNS < 0 || o.LastSeenTimeNS < o.FirstSeenTimeNS || o.ObservationCount == 0 {
		return fmt.Errorf("%w: invalid identity or observation interval", ErrInvalidSchemaObservation)
	}
	action, err := SchemaDriftAction(o.Classification, o.ParserCanPreserve, o.OptionalAbsenceResolved)
	if err != nil {
		return err
	}
	if !validNormalizationDisposition(o.MapperDisposition) || o.MapperDisposition != action.Normalization {
		return fmt.Errorf("%w: disposition %q does not match evidence-backed action %q",
			ErrInvalidSchemaObservation, o.MapperDisposition, action.Normalization)
	}
	for name, value := range map[string]json.RawMessage{
		"first_raw_coordinate": o.FirstRawCoordinate,
		"last_raw_coordinate":  o.LastRawCoordinate,
	} {
		canonical, err := canonicalJSONObject(value)
		if err != nil || !bytes.Equal(canonical, value) {
			return fmt.Errorf("%w: %s must be a canonical JSON object", ErrInvalidSchemaObservation, name)
		}
	}
	var sample any
	if err := json.Unmarshal(o.RedactedSample, &sample); err != nil || len(o.RedactedSample) == 0 {
		return fmt.Errorf("%w: invalid redacted structural sample", ErrInvalidSchemaObservation)
	}
	return nil
}

func validNormalizationDisposition(value NormalizationDisposition) bool {
	switch value {
	case NormalizationContinue, NormalizationTransition, NormalizationQuarantine, NormalizationFailClosed:
		return true
	default:
		return false
	}
}

// StructuralFingerprint returns a value-free structural sample and its digest.
// Object key order is ignored; field names, JSON scalar representation,
// nullability, nested object shape, and array element shapes are preserved.
func StructuralFingerprint(payload []byte) ([sha256.Size]byte, json.RawMessage, error) {
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return [sha256.Size]byte{}, nil, fmt.Errorf("%w: decode sample: %v", ErrInvalidSchemaObservation, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return [sha256.Size]byte{}, nil, fmt.Errorf("%w: trailing sample content", ErrInvalidSchemaObservation)
	}
	shape, err := structuralShape(value)
	if err != nil {
		return [sha256.Size]byte{}, nil, err
	}
	encoded, err := json.Marshal(shape)
	if err != nil {
		return [sha256.Size]byte{}, nil, err
	}
	return sha256.Sum256(append([]byte("quality-structural-fingerprint-v1\x00"), encoded...)), json.RawMessage(encoded), nil
}

func structuralShape(value any) (any, error) {
	switch typed := value.(type) {
	case nil:
		return "null", nil
	case bool:
		return "boolean", nil
	case string:
		return "string", nil
	case json.Number:
		text := string(typed)
		switch {
		case strings.ContainsAny(text, "eE"):
			return "number:exponent", nil
		case strings.Contains(text, "."):
			return "number:decimal", nil
		default:
			return "number:integer", nil
		}
	case map[string]any:
		keys := make([]string, 0, len(typed))
		for key := range typed {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		fields := make([]any, 0, len(keys))
		for _, key := range keys {
			child, err := structuralShape(typed[key])
			if err != nil {
				return nil, err
			}
			fields = append(fields, []any{key, child})
		}
		return []any{"object", fields}, nil
	case []any:
		shapes := make([]json.RawMessage, 0, len(typed))
		seen := make(map[string]struct{}, len(typed))
		for _, item := range typed {
			shape, err := structuralShape(item)
			if err != nil {
				return nil, err
			}
			encoded, err := json.Marshal(shape)
			if err != nil {
				return nil, err
			}
			key := string(encoded)
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			shapes = append(shapes, encoded)
		}
		slices.SortFunc(shapes, func(left, right json.RawMessage) int { return bytes.Compare(left, right) })
		elements := make([]any, 0, len(shapes))
		for _, encoded := range shapes {
			var shape any
			if err := json.Unmarshal(encoded, &shape); err != nil {
				return nil, err
			}
			elements = append(elements, shape)
		}
		return []any{"array", elements}, nil
	default:
		return nil, fmt.Errorf("%w: unsupported decoded type %T", ErrInvalidSchemaObservation, value)
	}
}

func MergeSchemaObservation(existing, next SchemaObservation) (SchemaObservation, error) {
	if err := existing.Validate(); err != nil {
		return SchemaObservation{}, err
	}
	if err := next.Validate(); err != nil {
		return SchemaObservation{}, err
	}
	if existing.SourceID != next.SourceID || existing.ChannelID != next.ChannelID || existing.Fingerprint != next.Fingerprint ||
		existing.Classification != next.Classification || existing.ParserCanPreserve != next.ParserCanPreserve ||
		existing.OptionalAbsenceResolved != next.OptionalAbsenceResolved ||
		next.FirstSeenTimeNS != next.LastSeenTimeNS || next.ObservationCount != 1 ||
		next.LastSeenTimeNS < existing.LastSeenTimeNS {
		return SchemaObservation{}, fmt.Errorf("%w: conflicting observation merge", ErrInvalidSchemaObservation)
	}
	merged := existing
	merged.LastSeenTimeNS = next.LastSeenTimeNS
	merged.LastRawCoordinate = slices.Clone(next.LastRawCoordinate)
	merged.RedactedSample = slices.Clone(next.RedactedSample)
	merged.ObservationCount++
	merged.MapperDisposition = next.MapperDisposition
	merged.MapperReleaseID = next.MapperReleaseID
	return merged, merged.Validate()
}
