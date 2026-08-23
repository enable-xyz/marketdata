package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"io"
	"reflect"

	"github.com/enable-xyz/marketdata/normalize"
)

var (
	normalizeMetadataType = reflect.TypeFor[normalize.Metadata]()
	normalizeRowType      = reflect.TypeFor[normalize.Row]()
)

// MapperAcceptedProjection is the release-neutral identity of one accepted
// normalized event. The full EventID and mapper provenance remain alongside it
// in AcceptedFieldIdentity, but do not decide dual-run equivalence.
type MapperAcceptedProjection struct {
	Record          MapperRecordIdentity
	RowOrdinal      uint32
	EventKind       normalize.EventKind
	SchemaName      string
	SchemaVersion   uint16
	SemanticEventID normalize.Hash
}

func (p MapperAcceptedProjection) String() string {
	return fmt.Sprintf("%08x/%08x/%s/%s@%d/%x", p.Record.CorpusIndex, p.RowOrdinal, p.EventKind, p.SchemaName, p.SchemaVersion, p.SemanticEventID)
}

// MapperRejectionProjection excludes mapper release provenance while retaining
// the exact rejected raw coordinate and classification that must compare equal.
type MapperRejectionProjection struct {
	Record                  MapperRecordIdentity
	Code                    normalize.QuarantineCode
	Field                   string
	SourceState             normalize.SourceState
	FingerprintClass        normalize.FingerprintClass
	SourceSchemaFingerprint normalize.Hash
}

func (p MapperRejectionProjection) String() string {
	var e mapperEvidenceEncoder
	e.string("enable-marketdata/mapper-rejection-projection/v1")
	e.record(p.Record)
	e.string(string(p.Code))
	e.string(p.Field)
	e.string(string(p.SourceState))
	e.string(string(p.FingerprintClass))
	e.hash(p.SourceSchemaFingerprint)
	return fmt.Sprintf("v1/%x", e.bytes)
}

func semanticEventID(metadata normalize.Metadata, kind normalize.EventKind) normalize.Hash {
	e := newMapperDigestEncoder("enable-marketdata/mapper-semantic-event-id/v1")
	e.string(string(kind))
	e.string(metadata.SchemaName)
	e.u16(metadata.SchemaVersion)
	e.string(metadata.SourceID)
	e.string(metadata.ChannelID)
	e.string(string(metadata.EpochKind))
	e.epoch(metadata.EpochID)
	e.u64(metadata.ArrivalOrdinal)
	e.u32(metadata.MessageOrdinal)
	e.hashValue(metadata.RawSegmentSHA256)
	e.u64(metadata.RawRecordOrdinal)
	e.hashValue(metadata.RawPayloadSHA256)
	e.string(string(metadata.SourceTimeResolution))
	return e.sum()
}

// semanticRowHash covers every normalized schema field in declaration order,
// except the four release-provenance hashes/identifiers named below. Struct
// field names and concrete types are encoded, so adding or changing a frozen
// field changes this digest rather than being silently ignored.
func semanticRowHash(row normalize.Row) (normalize.Hash, error) {
	if err := row.Validate(); err != nil {
		return normalize.Hash{}, err
	}
	e := newMapperDigestEncoder("enable-marketdata/mapper-semantic-row/v1")
	if err := appendSemanticValue(e, reflect.ValueOf(row)); err != nil {
		return normalize.Hash{}, err
	}
	return e.sum(), nil
}

type semanticValueEncoder interface {
	u8(uint8)
	bool(bool)
	u32(uint32)
	u64(uint64)
	i64(int64)
	string(string)
}

func appendSemanticValue(e semanticValueEncoder, value reflect.Value) error {
	return appendSemanticValueWithType(e, value, true)
}

func appendSemanticValueWithType(e semanticValueEncoder, value reflect.Value, includeType bool) error {
	typeOf := value.Type()
	if includeType {
		e.string(typeOf.PkgPath())
		e.string(typeOf.String())
	}
	e.u8(uint8(value.Kind()))
	switch value.Kind() {
	case reflect.Bool:
		e.bool(value.Bool())
	case reflect.String:
		e.string(value.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		e.i64(value.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		e.u64(value.Uint())
	case reflect.Array, reflect.Slice:
		e.u32(uint32(value.Len()))
		e.string(typeOf.Elem().PkgPath())
		e.string(typeOf.Elem().String())
		for i := range value.Len() {
			if err := appendSemanticValueWithType(e, value.Index(i), false); err != nil {
				return err
			}
		}
	case reflect.Pointer:
		e.bool(!value.IsNil())
		if !value.IsNil() {
			e.string(typeOf.Elem().PkgPath())
			e.string(typeOf.Elem().String())
			return appendSemanticValueWithType(e, value.Elem(), false)
		}
	case reflect.Struct:
		included := uint32(0)
		for i := range value.NumField() {
			if includeSemanticField(typeOf, typeOf.Field(i).Name) {
				included++
			}
		}
		e.u32(included)
		for i := range value.NumField() {
			field := typeOf.Field(i)
			if !includeSemanticField(typeOf, field.Name) {
				continue
			}
			e.string(field.Name)
			if err := appendSemanticValueWithType(e, value.Field(i), true); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("%w: unsupported semantic row field %s (%s)", ErrMapperEvidence, typeOf, value.Kind())
	}
	return nil
}

func includeSemanticField(owner reflect.Type, name string) bool {
	if owner == normalizeRowType && name == "LogicalHash" {
		return false
	}
	if owner != normalizeMetadataType {
		return true
	}
	switch name {
	case "EventID", "MapperVersion", "MapperBindingID":
		return false
	default:
		return true
	}
}

func orderedRejectionHash(projections []MapperRejectionProjection) normalize.Hash {
	e := newMapperDigestEncoder("enable-marketdata/mapper-ordered-rejections/v1")
	e.u32(uint32(len(projections)))
	for _, projection := range projections {
		e.record(projection.Record)
		e.string(string(projection.Code))
		e.string(projection.Field)
		e.string(string(projection.SourceState))
		e.string(string(projection.FingerprintClass))
		e.hashValue(projection.SourceSchemaFingerprint)
	}
	return e.sum()
}

type mapperDigestEncoder struct {
	hash   hash.Hash
	number [8]byte
}

func newMapperDigestEncoder(domain string) *mapperDigestEncoder {
	e := &mapperDigestEncoder{hash: sha256.New()}
	e.string(domain)
	return e
}

func (e *mapperDigestEncoder) u8(value uint8) {
	e.number[0] = value
	_, _ = e.hash.Write(e.number[:1])
}

func (e *mapperDigestEncoder) bool(value bool) {
	if value {
		e.u8(1)
		return
	}
	e.u8(0)
}

func (e *mapperDigestEncoder) u16(value uint16) {
	binary.BigEndian.PutUint16(e.number[:2], value)
	_, _ = e.hash.Write(e.number[:2])
}

func (e *mapperDigestEncoder) u32(value uint32) {
	binary.BigEndian.PutUint32(e.number[:4], value)
	_, _ = e.hash.Write(e.number[:4])
}

func (e *mapperDigestEncoder) u64(value uint64) {
	binary.BigEndian.PutUint64(e.number[:], value)
	_, _ = e.hash.Write(e.number[:])
}

func (e *mapperDigestEncoder) i64(value int64) { e.u64(uint64(value)) }

func (e *mapperDigestEncoder) string(value string) {
	e.u32(uint32(len(value)))
	_, _ = io.WriteString(e.hash, value)
}

func (e *mapperDigestEncoder) hashValue(value normalize.Hash) {
	_, _ = e.hash.Write(value[:])
}

func (e *mapperDigestEncoder) epoch(value [16]byte) {
	_, _ = e.hash.Write(value[:])
}

func (e *mapperDigestEncoder) coordinate(value normalize.RawCoordinate) {
	e.string(value.SourceID)
	e.string(value.ChannelID)
	e.string(string(value.EpochKind))
	e.epoch(value.EpochID)
	e.u64(value.ArrivalOrdinal)
	e.u32(value.MessageOrdinal)
	e.hashValue(value.RawSegmentSHA256)
	e.u64(value.RawRecordOrdinal)
	e.hashValue(value.RawPayloadSHA256)
}

func (e *mapperDigestEncoder) record(value MapperRecordIdentity) {
	e.u32(value.CorpusIndex)
	e.i64(value.ReceivedWallTimeNS)
	e.coordinate(value.Coordinate)
}

func (e *mapperDigestEncoder) sum() normalize.Hash {
	var result normalize.Hash
	copy(result[:], e.hash.Sum(nil))
	return result
}
