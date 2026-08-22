package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/segment"
)

type fakeStoredObject struct {
	data         []byte
	metadata     map[string]string
	etag         string
	lastModified time.Time
}

type fakeMultipart struct {
	key   string
	hash  [32]byte
	parts map[int32][]byte
}

type fakeClient struct {
	mu                     sync.Mutex
	objects                map[string]fakeStoredObject
	uploads                map[string]*fakeMultipart
	nextUpload             int
	conditionalUnsupported bool
	multipartUnsupported   bool
	dropNextCreate         bool
	dropNextComplete       bool
	corruptNextMetadata    bool
	failPart               int32
	clockSkew              time.Duration
	failClockReset         bool
	clockResetAttempts     int
	clockResetHadDeadline  bool
	rangeReads             int
	aborts                 int
	reconciliations        int
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		objects: make(map[string]fakeStoredObject),
		uploads: make(map[string]*fakeMultipart),
	}
}

func (f *fakeClient) Head(_ context.Context, key string) (ObjectInfo, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[key]
	if !ok {
		return ObjectInfo{}, errorWithKind("head", key, ErrorNotFound, errors.New("absent"))
	}
	return ObjectInfo{
		Key:          key,
		Size:         int64(len(object.data)),
		Metadata:     cloneMetadata(object.metadata),
		ETag:         object.etag,
		LastModified: object.lastModified,
	}, nil
}

func (f *fakeClient) PutIfAbsent(_ context.Context, object PutObject) error {
	if f.conditionalUnsupported {
		return errorWithKind("conditional put", object.Key, ErrorConditionalUnsupported, errors.New("not implemented"))
	}
	return f.create(object, true)
}

func (f *fakeClient) CreateImmutable(_ context.Context, object PutObject) error {
	return f.create(object, true)
}

func (f *fakeClient) create(object PutObject, conditional bool) error {
	if _, err := object.Body.Seek(0, io.SeekStart); err != nil {
		return err
	}
	data, err := io.ReadAll(io.LimitReader(object.Body, object.Size+1))
	if err != nil {
		return err
	}
	if int64(len(data)) != object.Size {
		return fmt.Errorf("fake: body size %d, want %d", len(data), object.Size)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, exists := f.objects[object.Key]; exists && conditional {
		return errorWithKind("conditional put", object.Key, ErrorPrecondition, errors.New("exists"))
	}
	metadataHash := object.SHA256
	if f.corruptNextMetadata {
		metadataHash[0] ^= 0xff
		f.corruptNextMetadata = false
	}
	f.objects[object.Key] = fakeStoredObject{
		data: data,
		metadata: map[string]string{
			ApplicationSHA256Metadata: fmt.Sprintf("%x", metadataHash),
		},
		etag:         "deliberately-wrong-etag",
		lastModified: time.Unix(1_700_000_000, 0).Add(f.clockSkew),
	}
	if f.dropNextCreate {
		f.dropNextCreate = false
		return errorWithKind("conditional put", object.Key, ErrorTransient, errors.New("response dropped"))
	}
	return nil
}

func (f *fakeClient) Get(_ context.Context, key string) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[key]
	if !ok {
		return nil, errorWithKind("get", key, ErrorNotFound, errors.New("absent"))
	}
	return io.NopCloser(bytes.NewReader(slices.Clone(object.data))), nil
}

func (f *fakeClient) GetRange(_ context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	object, ok := f.objects[key]
	if !ok {
		return nil, errorWithKind("range", key, ErrorNotFound, errors.New("absent"))
	}
	if offset < 0 || length <= 0 || offset+length > int64(len(object.data)) {
		return nil, errorWithKind("range", key, ErrorInvalidResponse, errors.New("invalid range"))
	}
	f.rangeReads++
	return io.NopCloser(bytes.NewReader(slices.Clone(object.data[offset : offset+length]))), nil
}

func (f *fakeClient) List(_ context.Context, prefix, continuation string) (ListPage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	keys := make([]string, 0, len(f.objects))
	for key := range f.objects {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	slices.Sort(keys)
	start := 0
	if continuation != "" {
		value, err := strconv.Atoi(continuation)
		if err != nil || value < 0 || value > len(keys) {
			return ListPage{}, errorWithKind("list", prefix, ErrorInvalidResponse, errors.New("bad token"))
		}
		start = value
	}
	end := min(start+2, len(keys))
	page := ListPage{Objects: make([]ListedObject, 0, end-start)}
	for _, key := range keys[start:end] {
		object := f.objects[key]
		page.Objects = append(page.Objects, ListedObject{Key: key, Size: int64(len(object.data)), LastModified: object.lastModified})
	}
	if end < len(keys) {
		page.NextToken = strconv.Itoa(end)
	}
	return page, nil
}

func (f *fakeClient) StartMultipart(_ context.Context, key string, hash [32]byte) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.multipartUnsupported {
		return "", errorWithKind("start multipart", key, ErrorMultipartUnsupported, errors.New("not implemented"))
	}
	f.nextUpload++
	uploadID := fmt.Sprintf("upload-%d", f.nextUpload)
	f.uploads[uploadID] = &fakeMultipart{key: key, hash: hash, parts: make(map[int32][]byte)}
	return uploadID, nil
}

func (f *fakeClient) UploadPart(_ context.Context, key, uploadID string, number int32, body io.Reader, size int64) (UploadedPart, error) {
	data, err := io.ReadAll(io.LimitReader(body, size+1))
	if err != nil {
		return UploadedPart{}, err
	}
	if int64(len(data)) != size {
		return UploadedPart{}, fmt.Errorf("fake: multipart size mismatch")
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	upload, ok := f.uploads[uploadID]
	if !ok || upload.key != key {
		return UploadedPart{}, errorWithKind("upload part", key, ErrorNotFound, errors.New("upload absent"))
	}
	if f.failPart == number {
		return UploadedPart{}, errorWithKind("upload part", key, ErrorTransient, errors.New("injected part failure"))
	}
	upload.parts[number] = data
	return UploadedPart{Number: number, ETag: fmt.Sprintf("wrong-part-etag-%d", number)}, nil
}

func (f *fakeClient) CompleteMultipart(_ context.Context, key, uploadID string, parts []UploadedPart) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	upload, ok := f.uploads[uploadID]
	if !ok || upload.key != key {
		return errorWithKind("complete multipart", key, ErrorNotFound, errors.New("upload absent"))
	}
	if f.conditionalUnsupported {
		return errorWithKind("complete multipart", key, ErrorConditionalUnsupported, errors.New("not implemented"))
	}
	if _, exists := f.objects[key]; exists {
		return errorWithKind("complete multipart", key, ErrorPrecondition, errors.New("exists"))
	}
	var data []byte
	for _, part := range parts {
		stored, ok := upload.parts[part.Number]
		if !ok {
			return errorWithKind("complete multipart", key, ErrorInvalidResponse, errors.New("missing part"))
		}
		data = append(data, stored...)
	}
	f.objects[key] = fakeStoredObject{
		data: data,
		metadata: map[string]string{
			ApplicationSHA256Metadata: fmt.Sprintf("%x", upload.hash),
		},
		etag:         "multipart-etag-is-not-a-sha256",
		lastModified: time.Unix(1_700_000_000, 0).Add(f.clockSkew),
	}
	delete(f.uploads, uploadID)
	if f.dropNextComplete {
		f.dropNextComplete = false
		return errorWithKind("complete multipart", key, ErrorTransient, errors.New("completion response dropped"))
	}
	return nil
}

func (f *fakeClient) AbortMultipart(_ context.Context, key, uploadID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	upload, ok := f.uploads[uploadID]
	if !ok {
		return errorWithKind("abort multipart", key, ErrorNotFound, errors.New("upload absent"))
	}
	if upload.key != key {
		return errorWithKind("abort multipart", key, ErrorInvalidResponse, errors.New("key mismatch"))
	}
	delete(f.uploads, uploadID)
	f.aborts++
	return nil
}

func (f *fakeClient) ReconcileMultipart(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for uploadID, upload := range f.uploads {
		if upload.key == key {
			delete(f.uploads, uploadID)
		}
	}
	f.reconciliations++
	return nil
}

func (f *fakeClient) SetClockSkew(ctx context.Context, skew time.Duration) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if skew == 0 {
		f.clockResetAttempts++
		_, f.clockResetHadDeadline = ctx.Deadline()
		if f.failClockReset {
			return errors.New("injected clock reset failure")
		}
	}
	f.clockSkew = skew
	return nil
}

func (f *fakeClient) DropNextCreateResponse(_ context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dropNextCreate = true
	return nil
}

func (f *fakeClient) store(key string, data []byte, hash [32]byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[key] = fakeStoredObject{
		data: slices.Clone(data),
		metadata: map[string]string{
			ApplicationSHA256Metadata: fmt.Sprintf("%x", hash),
		},
		etag:         "wrong-etag",
		lastModified: time.Unix(1_700_000_000, 0),
	}
}

type fakeCatalog struct {
	mu      sync.Mutex
	records map[string]catalog.RawSegmentPublication
	orphans map[string]catalog.ObjectOrphan
	events  []string
}

func newFakeCatalog() *fakeCatalog {
	return &fakeCatalog{
		records: make(map[string]catalog.RawSegmentPublication),
		orphans: make(map[string]catalog.ObjectOrphan),
	}
}

func (f *fakeCatalog) FindRawSegment(_ context.Context, key string) (catalog.RawSegmentPublication, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.records[key]
	record.ManifestBytes = slices.Clone(record.ManifestBytes)
	return record, ok, nil
}

func (f *fakeCatalog) RecordVerified(_ context.Context, record catalog.RawSegmentPublication) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.records[record.ObjectKey]; ok {
		if !sameFakePublication(existing, record) {
			return catalog.ErrPublicationConflict
		}
		if existing.State == catalog.RawSegmentQuarantined || existing.State == catalog.RawSegmentSuperseded {
			return catalog.ErrPublicationState
		}
		record.State = existing.State
	} else {
		record.State = catalog.RawSegmentVerified
	}
	record.ManifestBytes = slices.Clone(record.ManifestBytes)
	f.records[record.ObjectKey] = record
	f.events = append(f.events, "verified:"+record.ObjectKey)
	return nil
}

func (f *fakeCatalog) CommitRawSegment(_ context.Context, key string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.records[key]
	if !ok || (record.State != catalog.RawSegmentVerified && record.State != catalog.RawSegmentCommitted) {
		return catalog.ErrPublicationState
	}
	record.State = catalog.RawSegmentCommitted
	f.records[key] = record
	f.events = append(f.events, "committed:"+key)
	return nil
}

func (f *fakeCatalog) QuarantineRawSegment(_ context.Context, key, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	record, ok := f.records[key]
	if !ok || record.State == catalog.RawSegmentCommitted {
		return catalog.ErrPublicationState
	}
	record.State = catalog.RawSegmentQuarantined
	f.records[key] = record
	f.events = append(f.events, "quarantined:"+key)
	return nil
}

func (f *fakeCatalog) RecordObjectOrphan(_ context.Context, orphan catalog.ObjectOrphan) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.orphans[orphan.ObjectKey]; ok &&
		(existing.ByteLength != orphan.ByteLength || existing.ApplicationSHA256 != orphan.ApplicationSHA256 || existing.HasApplicationSHA256 != orphan.HasApplicationSHA256) {
		return catalog.ErrPublicationConflict
	}
	f.orphans[orphan.ObjectKey] = orphan
	f.events = append(f.events, "orphan:"+orphan.ObjectKey)
	return nil
}

func sameFakePublication(left, right catalog.RawSegmentPublication) bool {
	return left.SegmentID == right.SegmentID &&
		left.SourceID == right.SourceID &&
		left.ChannelID == right.ChannelID &&
		left.EpochID == right.EpochID &&
		left.ReceivedStartNS == right.ReceivedStartNS &&
		left.ReceivedEndNS == right.ReceivedEndNS &&
		left.OrdinalStart == right.OrdinalStart &&
		left.OrdinalEnd == right.OrdinalEnd &&
		left.ObjectKey == right.ObjectKey &&
		left.ContentSHA256 == right.ContentSHA256 &&
		left.ByteLength == right.ByteLength &&
		left.ManifestVersion == right.ManifestVersion &&
		left.ManifestSHA256 == right.ManifestSHA256 &&
		bytes.Equal(left.ManifestBytes, right.ManifestBytes)
}

func readyFixture(t interface {
	Helper()
	TempDir() string
	Fatalf(string, ...any)
}, payload []byte) PublishRequest {
	t.Helper()
	directory := t.TempDir()
	contentHash := sha256.Sum256(payload)
	received := time.Date(2025, time.January, 2, 3, 4, 5, 0, time.UTC).UnixNano()
	manifest := segment.ReadyManifest{
		ManifestVersion: 1,
		SourceID:        "00000000-0000-0000-0000-000000000701",
		ChannelID:       "trades",
		EpochKind:       segment.EpochConnection,
		EpochID:         "00000000000000000000000000000702",
		WriterVersion:   "test",
		RotationReason:  segment.RotationShutdown,
		SegmentFile:     "segment.ready",
		Segment: segment.Manifest{
			FormatVersion:      segment.FormatVersion,
			FrameBytes:         uint64(len(payload)),
			RecordCount:        1,
			UncompressedBytes:  uint64(len(payload)),
			CompressedBytes:    uint64(len(payload)),
			FirstOrdinal:       7,
			LastOrdinal:        7,
			FirstReceivedAtNS:  received,
			LastReceivedAtNS:   received,
			CompressedSHA256:   contentHash,
			UncompressedSHA256: contentHash,
		},
	}
	key, err := publicationObjectKey(manifest)
	if err != nil {
		t.Fatalf("publicationObjectKey() error = %v", err)
	}
	manifest.ObjectKey = key
	segmentPath := directory + "/segment.ready"
	if err := os.WriteFile(segmentPath, payload, 0o600); err != nil {
		t.Fatalf("write segment fixture: %v", err)
	}
	manifestData, err := json.Marshal(manifest)
	if err != nil {
		t.Fatalf("marshal manifest fixture: %v", err)
	}
	manifestData = append(manifestData, '\n')
	manifestPath := directory + "/manifest.ready.json"
	if err := os.WriteFile(manifestPath, manifestData, 0o600); err != nil {
		t.Fatalf("write manifest fixture: %v", err)
	}
	return PublishRequest{
		SegmentID: "00000000-0000-0000-0000-000000000703",
		Ready: segment.ReadySegment{
			SegmentPath:    segmentPath,
			ManifestPath:   manifestPath,
			ManifestBytes:  uint64(len(manifestData)),
			ManifestSHA256: sha256.Sum256(manifestData),
			Manifest:       manifest,
		},
	}
}
