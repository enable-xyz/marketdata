package normalize

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/catalog"
)

const (
	MapperBindingVersion    uint16 = 1
	MaxMapperBindings              = 1024
	MaxFingerprintRules            = 256
	MaxAllowedUnknownFields        = 64
	MaxNormalizationBatch          = 100_000
)

var (
	ErrInvalidMapperBinding  = errors.New("normalize: invalid mapper binding")
	ErrNormalizationBoundary = errors.New("normalize: normalization boundary failed")
)

type QuarantineCode string

const (
	QuarantineBindingUnavailable QuarantineCode = "mapper_binding_unavailable"
	QuarantineSchemaMalformed    QuarantineCode = "schema_malformed"
	QuarantineSchemaUnknown      QuarantineCode = "schema_unknown"
	QuarantineSemanticChange     QuarantineCode = "schema_semantic_change"
	QuarantineTypeMeaningChange  QuarantineCode = "schema_type_or_meaning_change"
	QuarantineInvalidField       QuarantineCode = "invalid_field"
	QuarantineBounds             QuarantineCode = "bounds_exceeded"
	QuarantineInstrument         QuarantineCode = "instrument_identity_unresolved"
	QuarantineMapperOutput       QuarantineCode = "invalid_mapper_output"
)

type ProjectionError struct {
	Code  QuarantineCode
	Field string
	State SourceState
}

func (e *ProjectionError) Error() string {
	if e.Field == "" {
		return "normalize: projection rejected"
	}
	return "normalize: projection rejected field " + e.Field
}

func RejectProjection(code QuarantineCode, field string, state SourceState) error {
	if state == "" {
		state = SourceValue
	}
	return &ProjectionError{Code: code, Field: field, State: state}
}

type MapperBinding struct {
	Version              uint16
	BindingID            Hash
	SourceID             string
	ChannelID            string
	EffectiveFromNS      int64
	EffectiveUntilNS     OptionalInt64
	MapperVersion        string
	SourceTimeResolution TimeResolution
	CatalogSnapshotID    Hash
	FingerprintRules     []FingerprintRule
}

type MappingInput struct {
	Record      RawRecord
	Catalog     *CatalogView
	Binding     MapperBinding
	Fingerprint FingerprintRule
}

type Mapper interface {
	Version() string
	Map(MappingInput) ([]Row, error)
}

type BoundMapper struct {
	Binding MapperBinding
	Mapper  Mapper
}

type Orchestrator struct {
	catalog  *CatalogView
	bindings []BoundMapper
}

type SchemaQuarantineV1 struct {
	Version                 uint16
	QuarantineID            Hash
	Code                    QuarantineCode
	Field                   string
	SourceState             SourceState
	FingerprintClass        FingerprintClass
	SourceSchemaFingerprint Hash
	SourceID                string
	ChannelID               string
	ReceivedTimeNS          int64
	Coordinate              RawCoordinate
	MapperVersion           string
	MapperBindingID         Hash
	SourceTimeResolution    TimeResolution
	CatalogSnapshotID       Hash
}

type Batch struct {
	Rows        []Row
	Quarantines []SchemaQuarantineV1
}

func NewOrchestrator(snapshot catalog.Snapshot, bound []BoundMapper) (*Orchestrator, error) {
	view, err := NewCatalogView(snapshot)
	if err != nil {
		return nil, err
	}
	if len(bound) == 0 || len(bound) > MaxMapperBindings {
		return nil, fmt.Errorf("%w: binding count", ErrInvalidMapperBinding)
	}
	bindings := make([]BoundMapper, len(bound))
	for i, candidate := range bound {
		binding, err := canonicalBinding(candidate.Binding, candidate.Mapper, view)
		if err != nil {
			return nil, err
		}
		bindings[i] = BoundMapper{Binding: binding, Mapper: candidate.Mapper}
	}
	slices.SortFunc(bindings, compareBoundMapper)
	for i := 1; i < len(bindings); i++ {
		previous, current := bindings[i-1].Binding, bindings[i].Binding
		if previous.SourceID == current.SourceID && previous.ChannelID == current.ChannelID {
			previousEnd := int64(^uint64(0) >> 1)
			if previous.EffectiveUntilNS.Valid {
				previousEnd = previous.EffectiveUntilNS.Value
			}
			if current.EffectiveFromNS < previousEnd {
				return nil, fmt.Errorf("%w: overlapping effective intervals", ErrInvalidMapperBinding)
			}
		}
	}
	return &Orchestrator{catalog: view, bindings: bindings}, nil
}

func canonicalBinding(binding MapperBinding, mapper Mapper, view *CatalogView) (MapperBinding, error) {
	if mapper == nil || binding.Version != MapperBindingVersion || binding.SourceID == "" || binding.ChannelID == "" ||
		binding.EffectiveFromNS < 0 || binding.MapperVersion == "" || len(binding.MapperVersion) > MaxMapperVersionBytes ||
		mapper.Version() != binding.MapperVersion || binding.CatalogSnapshotID != view.SnapshotID() ||
		binding.SourceID != view.SourceID() || binding.SourceTimeResolution == ResolutionAbsent ||
		!validTimeResolution(binding.SourceTimeResolution) ||
		len(binding.FingerprintRules) == 0 || len(binding.FingerprintRules) > MaxFingerprintRules {
		return MapperBinding{}, fmt.Errorf("%w: identity or bounds", ErrInvalidMapperBinding)
	}
	if binding.EffectiveUntilNS.Valid && binding.EffectiveUntilNS.Value <= binding.EffectiveFromNS {
		return MapperBinding{}, fmt.Errorf("%w: invalid effective interval", ErrInvalidMapperBinding)
	}
	binding.FingerprintRules = slices.Clone(binding.FingerprintRules)
	for i := range binding.FingerprintRules {
		rule := &binding.FingerprintRules[i]
		if rule.Fingerprint == (Hash{}) || !validFingerprintClass(rule.Class) || len(rule.AllowedUnknownFields) > MaxAllowedUnknownFields {
			return MapperBinding{}, fmt.Errorf("%w: fingerprint rule", ErrInvalidMapperBinding)
		}
		rule.AllowedUnknownFields = slices.Clone(rule.AllowedUnknownFields)
		slices.Sort(rule.AllowedUnknownFields)
		if len(rule.AllowedUnknownFields) != len(slices.Compact(slices.Clone(rule.AllowedUnknownFields))) {
			return MapperBinding{}, fmt.Errorf("%w: duplicate additive field", ErrInvalidMapperBinding)
		}
		for _, field := range rule.AllowedUnknownFields {
			if field == "" || len(field) > MaxFingerprintStringBytes || strings.IndexByte(field, 0) >= 0 {
				return MapperBinding{}, fmt.Errorf("%w: additive field bound", ErrInvalidMapperBinding)
			}
		}
		if rule.Class != FingerprintAdditiveHarmless && len(rule.AllowedUnknownFields) != 0 {
			return MapperBinding{}, fmt.Errorf("%w: only additive harmless rules allow fields", ErrInvalidMapperBinding)
		}
	}
	slices.SortFunc(binding.FingerprintRules, func(a, b FingerprintRule) int {
		return bytes.Compare(a.Fingerprint[:], b.Fingerprint[:])
	})
	for i := 1; i < len(binding.FingerprintRules); i++ {
		if binding.FingerprintRules[i].Fingerprint == binding.FingerprintRules[i-1].Fingerprint {
			return MapperBinding{}, fmt.Errorf("%w: duplicate fingerprint", ErrInvalidMapperBinding)
		}
	}
	binding.BindingID = bindingHash(binding)
	return binding, nil
}

func validFingerprintClass(class FingerprintClass) bool {
	switch class {
	case FingerprintExact, FingerprintAdditiveHarmless, FingerprintSemanticAdditive, FingerprintTypeOrMeaningChange, FingerprintUnknown:
		return true
	default:
		return false
	}
}

func bindingHash(binding MapperBinding) Hash {
	var e canonicalEncoder
	e.string("mapper-binding")
	e.u16(binding.Version)
	e.string(binding.SourceID)
	e.string(binding.ChannelID)
	e.i64(binding.EffectiveFromNS)
	e.optionalInt64(binding.EffectiveUntilNS)
	e.string(binding.MapperVersion)
	e.string(string(binding.SourceTimeResolution))
	e.hash(binding.CatalogSnapshotID)
	e.u32(uint32(len(binding.FingerprintRules)))
	for _, rule := range binding.FingerprintRules {
		e.hash(rule.Fingerprint)
		e.string(string(rule.Class))
		e.u32(uint32(len(rule.AllowedUnknownFields)))
		for _, field := range rule.AllowedUnknownFields {
			e.string(field)
		}
	}
	return Hash(sha256.Sum256(e.bytes))
}

func compareBoundMapper(a, b BoundMapper) int {
	if n := strings.Compare(a.Binding.SourceID, b.Binding.SourceID); n != 0 {
		return n
	}
	if n := strings.Compare(a.Binding.ChannelID, b.Binding.ChannelID); n != 0 {
		return n
	}
	if a.Binding.EffectiveFromNS < b.Binding.EffectiveFromNS {
		return -1
	}
	if a.Binding.EffectiveFromNS > b.Binding.EffectiveFromNS {
		return 1
	}
	return bytes.Compare(a.Binding.BindingID[:], b.Binding.BindingID[:])
}

func (o *Orchestrator) Normalize(records []RawRecord) (Batch, error) {
	if o == nil || o.catalog == nil || len(records) > MaxNormalizationBatch {
		return Batch{}, fmt.Errorf("%w: batch bound", ErrNormalizationBoundary)
	}
	batch := Batch{}
	lastArrivalByStream := make(map[streamOrderKey]uint64, len(records))
	for _, supplied := range records {
		record := RawRecord{Envelope: cloneEnvelope(supplied.Envelope), Coordinate: supplied.Coordinate, QualityFlags: slices.Clone(supplied.QualityFlags)}
		if err := record.Validate(); err != nil {
			return Batch{}, fmt.Errorf("%w: invalid raw coordinate", ErrNormalizationBoundary)
		}
		key := streamOrderKey{SourceID: record.Coordinate.SourceID, EpochKind: record.Coordinate.EpochKind, EpochID: record.Coordinate.EpochID}
		currentArrival := record.Coordinate.ArrivalOrdinal
		if previousArrival, ok := lastArrivalByStream[key]; ok && currentArrival <= previousArrival {
			return Batch{}, fmt.Errorf("%w: source epoch arrival ordinal is not strictly increasing", ErrNormalizationBoundary)
		}
		lastArrivalByStream[key] = currentArrival
		binding, found := o.selectBinding(record)
		if !found {
			batch.Quarantines = append(batch.Quarantines, newQuarantine(record, MapperBinding{}, Hash{}, FingerprintUnknown, QuarantineBindingUnavailable, "mapper_binding", SourceMissing, o.catalog.SnapshotID()))
			continue
		}
		observation, err := StructuralFingerprint(record.Envelope.RawPayload)
		if err != nil {
			batch.Quarantines = append(batch.Quarantines, newQuarantine(record, binding.Binding, Hash{}, FingerprintUnknown, QuarantineSchemaMalformed, "payload", SourceValue, o.catalog.SnapshotID()))
			continue
		}
		rule, found := findFingerprintRule(binding.Binding.FingerprintRules, observation.Fingerprint)
		if !found {
			batch.Quarantines = append(batch.Quarantines, newQuarantine(record, binding.Binding, observation.Fingerprint, FingerprintUnknown, QuarantineSchemaUnknown, "payload", SourceValue, o.catalog.SnapshotID()))
			continue
		}
		switch rule.Class {
		case FingerprintExact, FingerprintAdditiveHarmless:
		case FingerprintSemanticAdditive:
			batch.Quarantines = append(batch.Quarantines, newQuarantine(record, binding.Binding, observation.Fingerprint, rule.Class, QuarantineSemanticChange, "payload", SourceValue, o.catalog.SnapshotID()))
			continue
		case FingerprintTypeOrMeaningChange:
			batch.Quarantines = append(batch.Quarantines, newQuarantine(record, binding.Binding, observation.Fingerprint, rule.Class, QuarantineTypeMeaningChange, "payload", SourceValue, o.catalog.SnapshotID()))
			continue
		default:
			batch.Quarantines = append(batch.Quarantines, newQuarantine(record, binding.Binding, observation.Fingerprint, rule.Class, QuarantineSchemaUnknown, "payload", SourceValue, o.catalog.SnapshotID()))
			continue
		}
		rows, err := binding.Mapper.Map(MappingInput{Record: record, Catalog: o.catalog, Binding: binding.Binding, Fingerprint: rule})
		if err != nil {
			code, field, state := QuarantineInvalidField, "payload", SourceValue
			var projection *ProjectionError
			if errors.As(err, &projection) {
				code, field, state = projection.Code, projection.Field, projection.State
			}
			batch.Quarantines = append(batch.Quarantines, newQuarantine(record, binding.Binding, observation.Fingerprint, rule.Class, code, field, state, o.catalog.SnapshotID()))
			continue
		}
		if len(rows) == 0 || len(rows) > 4 {
			batch.Quarantines = append(batch.Quarantines, newQuarantine(record, binding.Binding, observation.Fingerprint, rule.Class, QuarantineMapperOutput, "rows", SourceValue, o.catalog.SnapshotID()))
			continue
		}
		valid := true
		for _, row := range rows {
			metadata := row.Common()
			if err := row.Validate(); err != nil || metadata.SourceID != record.Coordinate.SourceID || metadata.ChannelID != record.Coordinate.ChannelID ||
				metadata.EpochID != record.Coordinate.EpochID || metadata.ArrivalOrdinal != record.Coordinate.ArrivalOrdinal ||
				metadata.MessageOrdinal != record.Coordinate.MessageOrdinal || metadata.RawSegmentSHA256 != record.Coordinate.RawSegmentSHA256 ||
				metadata.RawRecordOrdinal != record.Coordinate.RawRecordOrdinal || metadata.RawPayloadSHA256 != record.Coordinate.RawPayloadSHA256 ||
				metadata.SourceSchemaFingerprint != observation.Fingerprint || metadata.MapperBindingID != binding.Binding.BindingID ||
				metadata.MapperVersion != binding.Binding.MapperVersion || metadata.CatalogSnapshotID != o.catalog.SnapshotID() ||
				metadata.SourceTimeResolution != binding.Binding.SourceTimeResolution {
				valid = false
				break
			}
		}
		if !valid {
			batch.Quarantines = append(batch.Quarantines, newQuarantine(record, binding.Binding, observation.Fingerprint, rule.Class, QuarantineMapperOutput, "row", SourceValue, o.catalog.SnapshotID()))
			continue
		}
		batch.Rows = append(batch.Rows, rows...)
	}
	return batch, nil
}

func (o *Orchestrator) selectBinding(record RawRecord) (BoundMapper, bool) {
	for _, binding := range o.bindings {
		value := binding.Binding
		if value.SourceID != record.Coordinate.SourceID || value.ChannelID != record.Coordinate.ChannelID || record.Envelope.ReceivedWallTimeNS < value.EffectiveFromNS {
			continue
		}
		if value.EffectiveUntilNS.Valid && record.Envelope.ReceivedWallTimeNS >= value.EffectiveUntilNS.Value {
			continue
		}
		return binding, true
	}
	return BoundMapper{}, false
}

func findFingerprintRule(rules []FingerprintRule, fingerprint Hash) (FingerprintRule, bool) {
	index, found := slices.BinarySearchFunc(rules, fingerprint, func(rule FingerprintRule, target Hash) int {
		return bytes.Compare(rule.Fingerprint[:], target[:])
	})
	if !found {
		return FingerprintRule{}, false
	}
	return rules[index], true
}

func newQuarantine(record RawRecord, binding MapperBinding, fingerprint Hash, class FingerprintClass, code QuarantineCode, field string, state SourceState, catalogID Hash) SchemaQuarantineV1 {
	value := SchemaQuarantineV1{
		Version: SchemaQuarantineVersion, Code: code, Field: field, SourceState: state,
		FingerprintClass: class, SourceSchemaFingerprint: fingerprint,
		SourceID: record.Coordinate.SourceID, ChannelID: record.Coordinate.ChannelID,
		ReceivedTimeNS: record.Envelope.ReceivedWallTimeNS, Coordinate: record.Coordinate,
		MapperVersion: binding.MapperVersion, MapperBindingID: binding.BindingID, CatalogSnapshotID: catalogID,
		SourceTimeResolution: binding.SourceTimeResolution,
	}
	var e canonicalEncoder
	e.string("schema-quarantine")
	e.u16(value.Version)
	e.string(string(value.Code))
	e.string(value.Field)
	e.string(string(value.SourceState))
	e.string(string(value.FingerprintClass))
	e.hash(value.SourceSchemaFingerprint)
	e.string(value.SourceID)
	e.string(value.ChannelID)
	e.i64(value.ReceivedTimeNS)
	e.string(string(value.Coordinate.EpochKind))
	e.epoch(value.Coordinate.EpochID)
	e.u64(value.Coordinate.ArrivalOrdinal)
	e.u32(value.Coordinate.MessageOrdinal)
	e.hash(value.Coordinate.RawSegmentSHA256)
	e.u64(value.Coordinate.RawRecordOrdinal)
	e.hash(value.Coordinate.RawPayloadSHA256)
	e.string(value.MapperVersion)
	e.hash(value.MapperBindingID)
	e.string(string(value.SourceTimeResolution))
	e.hash(value.CatalogSnapshotID)
	value.QuarantineID = Hash(sha256.Sum256(e.bytes))
	return value
}

type streamOrderKey struct {
	SourceID  string
	EpochKind EpochKind
	EpochID   [16]byte
}
