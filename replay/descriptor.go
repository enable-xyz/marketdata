package replay

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/segment"
)

const MaximumManifestBytes = 4 << 20

var (
	ErrInvalidInput    = errors.New("replay: invalid input")
	ErrInputBound      = errors.New("replay: input bound exceeded")
	ErrIntegrity       = errors.New("replay: segment integrity failure")
	ErrEmitter         = errors.New("replay: emitter failure")
	ErrUnsupportedHash = errors.New("replay: unsupported logical hash version")
)

// InputDescriptor is an immutable replay identity derived from one catalog-
// committed raw segment manifest. Its byte slices and parsed manifest are owned
// by this value and are never exposed to callers.
type InputDescriptor struct {
	segmentID       string
	sourceID        string
	channelID       string
	epochID         string
	epochBytes      [16]byte
	objectKey       string
	contentSHA256   [32]byte
	byteLength      int64
	receivedStartNS int64
	receivedEndNS   int64
	ordinalStart    uint64
	ordinalEnd      uint64
	manifestSHA256  [32]byte
	manifestBytes   []byte
	manifest        segment.ReadyManifest
}

// NewInputDescriptor accepts only a committed catalog row and binds every
// catalog identity field to the exact immutable manifest bytes before replay.
func NewInputDescriptor(publication catalog.RawSegmentPublication) (InputDescriptor, error) {
	if publication.State != catalog.RawSegmentCommitted {
		return InputDescriptor{}, fmt.Errorf("%w: segment %q is not committed", ErrInvalidInput, publication.SegmentID)
	}
	if publication.SegmentID == "" || publication.SourceID == "" || publication.ChannelID == "" || publication.EpochID == "" || publication.ObjectKey == "" || publication.ByteLength <= 0 {
		return InputDescriptor{}, fmt.Errorf("%w: committed segment identity is incomplete", ErrInvalidInput)
	}
	if len(publication.ManifestBytes) == 0 || len(publication.ManifestBytes) > MaximumManifestBytes {
		return InputDescriptor{}, fmt.Errorf("%w: manifest has %d bytes", ErrInputBound, len(publication.ManifestBytes))
	}
	manifestBytes := bytes.Clone(publication.ManifestBytes)
	if sha256.Sum256(manifestBytes) != publication.ManifestSHA256 {
		return InputDescriptor{}, fmt.Errorf("%w: manifest SHA-256 differs from exact catalog bytes", ErrIntegrity)
	}

	var manifest segment.ReadyManifest
	decoder := json.NewDecoder(bytes.NewReader(manifestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return InputDescriptor{}, fmt.Errorf("%w: decode committed manifest: %v", ErrIntegrity, err)
	}
	if err := consumeJSONEOF(decoder); err != nil {
		return InputDescriptor{}, err
	}

	segmentID, _, err := parseCanonicalUUID(publication.SegmentID)
	if err != nil || segmentID != publication.SegmentID {
		return InputDescriptor{}, fmt.Errorf("%w: catalog segment ID is not a canonical UUID", ErrInvalidInput)
	}
	sourceID, _, err := parseCanonicalUUID(publication.SourceID)
	if err != nil || sourceID != publication.SourceID {
		return InputDescriptor{}, fmt.Errorf("%w: catalog source ID is not a canonical UUID", ErrInvalidInput)
	}
	epochID, epochBytes, err := parseCanonicalUUID(publication.EpochID)
	if err != nil || epochID != publication.EpochID {
		return InputDescriptor{}, fmt.Errorf("%w: catalog epoch ID is not a canonical UUID", ErrInvalidInput)
	}
	manifestEpochID, manifestEpochBytes, err := parseCanonicalUUID(manifest.EpochID)
	if err != nil || manifestEpochID != epochID || manifestEpochBytes != epochBytes {
		return InputDescriptor{}, fmt.Errorf("%w: manifest epoch identity differs from catalog", ErrIntegrity)
	}

	if manifest.ManifestVersion != segment.SpoolManifestVersion || publication.ManifestVersion != manifest.ManifestVersion || manifest.Segment.FormatVersion != segment.FormatVersion {
		return InputDescriptor{}, fmt.Errorf("%w: unsupported or inconsistent manifest version", ErrIntegrity)
	}
	if manifest.SourceID != sourceID || manifest.ChannelID != publication.ChannelID || manifest.ObjectKey != publication.ObjectKey {
		return InputDescriptor{}, fmt.Errorf("%w: manifest source, channel, or object identity differs from catalog", ErrIntegrity)
	}
	if manifest.EpochKind != segment.EpochConnection && manifest.EpochKind != segment.EpochPoll {
		return InputDescriptor{}, fmt.Errorf("%w: unsupported stream epoch kind %q", ErrIntegrity, manifest.EpochKind)
	}
	if manifest.WriterVersion == "" || manifest.SegmentFile == "" || manifest.Segment.RecordCount == 0 || len(manifest.Segment.Frames) == 0 {
		return InputDescriptor{}, fmt.Errorf("%w: committed manifest is incomplete", ErrIntegrity)
	}
	if manifest.Segment.CompressedBytes > uint64(^uint64(0)>>1) || int64(manifest.Segment.CompressedBytes) != publication.ByteLength || manifest.Segment.CompressedSHA256 != publication.ContentSHA256 {
		return InputDescriptor{}, fmt.Errorf("%w: manifest object length or SHA-256 differs from catalog", ErrIntegrity)
	}
	if manifest.Segment.FirstReceivedAtNS != publication.ReceivedStartNS || manifest.Segment.LastReceivedAtNS != publication.ReceivedEndNS || manifest.Segment.FirstOrdinal != publication.OrdinalStart || manifest.Segment.LastOrdinal != publication.OrdinalEnd {
		return InputDescriptor{}, fmt.Errorf("%w: manifest receive-time or ordinal range differs from catalog", ErrIntegrity)
	}
	if publication.ReceivedStartNS < 0 || publication.ReceivedEndNS < publication.ReceivedStartNS || publication.OrdinalEnd < publication.OrdinalStart || publication.OrdinalEnd > uint64(1<<63-1) {
		return InputDescriptor{}, fmt.Errorf("%w: catalog segment bounds are invalid", ErrInvalidInput)
	}

	return InputDescriptor{
		segmentID:       segmentID,
		sourceID:        sourceID,
		channelID:       publication.ChannelID,
		epochID:         epochID,
		epochBytes:      epochBytes,
		objectKey:       publication.ObjectKey,
		contentSHA256:   publication.ContentSHA256,
		byteLength:      publication.ByteLength,
		receivedStartNS: publication.ReceivedStartNS,
		receivedEndNS:   publication.ReceivedEndNS,
		ordinalStart:    publication.OrdinalStart,
		ordinalEnd:      publication.OrdinalEnd,
		manifestSHA256:  publication.ManifestSHA256,
		manifestBytes:   manifestBytes,
		manifest:        manifest,
	}, nil
}

func consumeJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("%w: committed manifest contains multiple JSON values", ErrIntegrity)
		}
		return fmt.Errorf("%w: committed manifest trailing bytes: %v", ErrIntegrity, err)
	}
	return nil
}

func parseCanonicalUUID(value string) (string, [16]byte, error) {
	var raw string
	switch len(value) {
	case 32:
		raw = value
	case 36:
		if value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
			return "", [16]byte{}, ErrInvalidInput
		}
		raw = value[:8] + value[9:13] + value[14:18] + value[19:23] + value[24:]
	default:
		return "", [16]byte{}, ErrInvalidInput
	}
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != 16 {
		return "", [16]byte{}, ErrInvalidInput
	}
	var id [16]byte
	copy(id[:], decoded)
	encoded := hex.EncodeToString(id[:])
	canonical := encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:]
	return canonical, id, nil
}

func (d InputDescriptor) SegmentID() string        { return d.segmentID }
func (d InputDescriptor) SourceID() string         { return d.sourceID }
func (d InputDescriptor) ChannelID() string        { return d.channelID }
func (d InputDescriptor) StreamEpochID() string    { return d.epochID }
func (d InputDescriptor) ObjectKey() string        { return d.objectKey }
func (d InputDescriptor) ByteLength() int64        { return d.byteLength }
func (d InputDescriptor) RecordCount() uint64      { return d.manifest.Segment.RecordCount }
func (d InputDescriptor) FrameCount() int          { return len(d.manifest.Segment.Frames) }
func (d InputDescriptor) ReceivedStartNS() int64   { return d.receivedStartNS }
func (d InputDescriptor) ReceivedEndNS() int64     { return d.receivedEndNS }
func (d InputDescriptor) OrdinalStart() uint64     { return d.ordinalStart }
func (d InputDescriptor) OrdinalEnd() uint64       { return d.ordinalEnd }
func (d InputDescriptor) ManifestSHA256() [32]byte { return d.manifestSHA256 }

// CommittedManifestSource streams committed catalog publication rows.
type CommittedManifestSource interface {
	StreamCommittedRawSegments(context.Context, func(catalog.RawSegmentPublication) error) error
}

// LoadInputs materializes at most maxSegments immutable descriptors. The
// explicit limit makes catalog snapshot accumulation caller-bounded.
func LoadInputs(ctx context.Context, source CommittedManifestSource, maxSegments int) ([]InputDescriptor, error) {
	if source == nil || maxSegments < 1 {
		return nil, fmt.Errorf("%w: source and positive segment limit are required", ErrInvalidInput)
	}
	inputs := make([]InputDescriptor, 0, min(maxSegments, 1024))
	err := source.StreamCommittedRawSegments(ctx, func(publication catalog.RawSegmentPublication) error {
		if len(inputs) == maxSegments {
			return fmt.Errorf("%w: more than %d committed segments", ErrInputBound, maxSegments)
		}
		descriptor, err := NewInputDescriptor(publication)
		if err != nil {
			return err
		}
		inputs = append(inputs, descriptor)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return inputs, nil
}
