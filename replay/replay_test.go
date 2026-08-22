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
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/klauspost/compress/zstd"
)

const (
	testSourceA = "00000000-0000-0000-0000-000000000001"
	testSourceB = "00000000-0000-0000-0000-000000000002"
	testChannel = "trades"
)

type memoryObjects struct {
	mu      sync.RWMutex
	objects map[string][]byte
}

func (m *memoryObjects) Get(_ context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	data, ok := m.objects[key]
	m.mu.RUnlock()
	if !ok {
		return nil, objectstore.ErrNotFound
	}
	return io.NopCloser(bytes.NewReader(bytes.Clone(data))), nil
}

func (m *memoryObjects) put(key string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = bytes.Clone(data)
}

func TestNativeDeterminism(t *testing.T) {
	epoch := "aaaaaaaa-0000-0000-0000-000000000001"
	objects := &memoryObjects{objects: make(map[string][]byte)}
	records := []segment.Envelope{
		testRecord(t, testSourceA, epoch, 1, 0, 100, []byte("same")),
		testRecord(t, testSourceA, epoch, 2, 1, 101, []byte("same")),
		testRecord(t, testSourceA, epoch, 3, 0, 102, []byte("other")),
	}
	inputs := make([]InputDescriptor, 0, len(records))
	for i := range records {
		inputs = append(inputs, testInput(t, objects, i+1, testSourceA, epoch, []segment.Envelope{records[i]}))
	}
	orders := [][]InputDescriptor{
		slices.Clone(inputs),
		{inputs[2], inputs[0], inputs[1]},
		{inputs[1], inputs[2], inputs[0]},
	}
	unordered := map[string]InputDescriptor{
		inputs[0].SegmentID(): inputs[0],
		inputs[1].SegmentID(): inputs[1],
		inputs[2].SegmentID(): inputs[2],
	}
	var mapOrder []InputDescriptor
	for _, input := range unordered {
		mapOrder = append(mapOrder, input)
	}
	orders = append(orders, mapOrder)

	var baseline []Event
	var baselineHash [32]byte
	concurrency := []int{1, 2, MaximumWorkers}
	for i, workers := range concurrency {
		config := DefaultConfig()
		config.Concurrency = workers
		config.Prefetch = 3
		var events []Event
		result, err := ReplaySource(t.Context(), objects, orders[i], config, func(event Event) error {
			events = append(events, event)
			return nil
		})
		if err != nil {
			t.Fatalf("ReplaySource(concurrency=%d) error = %v", workers, err)
		}
		if result.Order != SourceNativeOrder || result.LogicalHashVersion != LogicalHashVersionV1 || result.EventCount != 3 {
			t.Fatalf("ReplaySource(concurrency=%d) result = %+v", workers, result)
		}
		if i == 0 {
			baseline = events
			baselineHash = result.LogicalHash
		} else {
			assertSameEvents(t, events, baseline)
			if result.LogicalHash != baselineHash {
				t.Fatalf("concurrency=%d hash %x differs from concurrency=1 hash %x", workers, result.LogicalHash, baselineHash)
			}
		}
	}
	var mapEvents []Event
	mapResult, err := ReplaySource(t.Context(), objects, orders[3], DefaultConfig(), func(event Event) error {
		mapEvents = append(mapEvents, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaySource(map order) error = %v", err)
	}
	assertSameEvents(t, mapEvents, baseline)
	if mapResult.LogicalHash != baselineHash {
		t.Fatalf("map-order hash %x differs from baseline %x", mapResult.LogicalHash, baselineHash)
	}
	if !bytes.Equal(baseline[0].Record.RawPayload, baseline[1].Record.RawPayload) || baseline[0].Coordinate.ArrivalOrdinal == baseline[1].Coordinate.ArrivalOrdinal || baseline[0].Coordinate.MessageOrdinal == baseline[1].Coordinate.MessageOrdinal {
		t.Fatal("duplicate raw payloads did not retain distinct (arrival_ordinal,message_ordinal) coordinates")
	}
	const amd64Arm64Golden = "f2a1edeaf11b181ff026aaf3a6169b0ec31ee61706d878d54e64ccbaa3e31cae"
	if got := hex.EncodeToString(baselineHash[:]); got != amd64Arm64Golden {
		t.Fatalf("logical hash = %s, want fixed amd64/arm64 golden %s", got, amd64Arm64Golden)
	}
}

func TestEpochOrder(t *testing.T) {
	objects := &memoryObjects{objects: make(map[string][]byte)}
	epochEarlyRandomHigh := "ffffffff-ffff-4fff-8fff-ffffffffffff"
	epochEqualLow := "11111111-1111-4111-8111-111111111111"
	epochEqualHigh := "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"
	inputs := []InputDescriptor{
		testInput(t, objects, 11, testSourceA, epochEqualHigh, []segment.Envelope{testRecord(t, testSourceA, epochEqualHigh, 1, 0, 200, []byte("equal-high"))}),
		testInput(t, objects, 12, testSourceA, epochEarlyRandomHigh, []segment.Envelope{testRecord(t, testSourceA, epochEarlyRandomHigh, 1, 0, 100, []byte("early"))}),
		testInput(t, objects, 13, testSourceA, epochEqualLow, []segment.Envelope{testRecord(t, testSourceA, epochEqualLow, 1, 0, 200, []byte("equal-low"))}),
	}
	var epochs []string
	_, err := ReplaySource(t.Context(), objects, inputs, DefaultConfig(), func(event Event) error {
		if event.Kind == EventRecord {
			epochs = append(epochs, event.Coordinate.StreamEpochID)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaySource() error = %v", err)
	}
	want := []string{epochEarlyRandomHigh, epochEqualLow, epochEqualHigh}
	if !slices.Equal(epochs, want) {
		t.Fatalf("epoch record order = %v, want %v", epochs, want)
	}
}

func TestMerge(t *testing.T) {
	objects := &memoryObjects{objects: make(map[string][]byte)}
	epochA := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	epochB := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	inputs := []InputDescriptor{
		testInput(t, objects, 21, testSourceA, epochA, []segment.Envelope{
			testRecord(t, testSourceA, epochA, 1, 0, 100, []byte("a-1")),
			testRecord(t, testSourceA, epochA, 2, 0, 10, []byte("a-regression")),
			testRecord(t, testSourceA, epochA, 3, 0, 110, []byte("a-3")),
		}),
		testInput(t, objects, 22, testSourceB, epochB, []segment.Envelope{
			testRecord(t, testSourceB, epochB, 1, 0, 50, []byte("b-1")),
			testRecord(t, testSourceB, epochB, 2, 0, 60, []byte("b-2")),
		}),
	}
	var got []string
	result, err := ReplayCollectorObserved(t.Context(), objects, inputs, DefaultConfig(), func(event Event) error {
		if event.Order != CollectorObservedOrder {
			t.Fatalf("merge event label = %q, want %q", event.Order, CollectorObservedOrder)
		}
		if event.Kind == EventRecord {
			got = append(got, fmt.Sprintf("%s:%d:%d", event.Coordinate.SourceID, event.Coordinate.ArrivalOrdinal, event.Coordinate.ReceivedWallTimeNS))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayCollectorObserved() error = %v", err)
	}
	want := []string{
		testSourceB + ":1:50",
		testSourceB + ":2:60",
		testSourceA + ":1:100",
		testSourceA + ":2:10",
		testSourceA + ":3:110",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("collector-observed order = %v, want %v", got, want)
	}
	if result.Order != CollectorObservedOrder || result.EventCount != uint64(len(want)) {
		t.Fatalf("merge result = %+v", result)
	}
}

func TestClockRegression(t *testing.T) {
	objects := &memoryObjects{objects: make(map[string][]byte)}
	epoch := "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	input := testInput(t, objects, 31, testSourceA, epoch, []segment.Envelope{
		testRecord(t, testSourceA, epoch, 1, 0, 1000, []byte("before")),
		testRecord(t, testSourceA, epoch, 2, 0, 5, []byte("regressed")),
		testRecord(t, testSourceA, epoch, 3, 0, 1001, []byte("after")),
	})
	var ordinals []uint64
	var walls []int64
	_, err := ReplaySource(t.Context(), objects, []InputDescriptor{input}, DefaultConfig(), func(event Event) error {
		if event.Kind == EventRecord {
			ordinals = append(ordinals, event.Coordinate.ArrivalOrdinal)
			walls = append(walls, event.Coordinate.ReceivedWallTimeNS)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaySource() error = %v", err)
	}
	if !slices.Equal(ordinals, []uint64{1, 2, 3}) || !slices.Equal(walls, []int64{1000, 5, 1001}) {
		t.Fatalf("source replay ordinals/walls = %v/%v", ordinals, walls)
	}
}

func TestDiscontinuity(t *testing.T) {
	objects := &memoryObjects{objects: make(map[string][]byte)}
	epoch1 := "dddddddd-dddd-4ddd-8ddd-dddddddddddd"
	epoch2 := "eeeeeeee-0000-4000-8000-000000000001"
	disconnect := testDisconnectRecord(t, testSourceA, epoch1, 1, 100)
	first := testInput(t, objects, 41, testSourceA, epoch1, []segment.Envelope{disconnect})
	gap := testInput(t, objects, 42, testSourceA, epoch1, []segment.Envelope{testRecord(t, testSourceA, epoch1, 3, 0, 103, []byte("after-gap"))})
	corrupt := testInput(t, objects, 43, testSourceA, epoch1, []segment.Envelope{testRecord(t, testSourceA, epoch1, 4, 0, 104, []byte("corrupt"))})
	objects.mu.Lock()
	objects.objects[corrupt.ObjectKey()][0] ^= 0xff
	objects.mu.Unlock()
	missing := testInput(t, objects, 44, testSourceA, epoch1, []segment.Envelope{testRecord(t, testSourceA, epoch1, 5, 0, 105, []byte("missing"))})
	objects.mu.Lock()
	delete(objects.objects, missing.ObjectKey())
	objects.mu.Unlock()
	nextEpoch := testInput(t, objects, 45, testSourceA, epoch2, []segment.Envelope{testRecord(t, testSourceA, epoch2, 1, 0, 200, []byte("next-epoch"))})

	var kinds []DiscontinuityKind
	_, err := ReplaySource(t.Context(), objects, []InputDescriptor{nextEpoch, corrupt, first, missing, gap}, Config{Concurrency: MaximumWorkers, Prefetch: 5}, func(event Event) error {
		if event.Kind == EventDiscontinuity {
			kinds = append(kinds, event.Discontinuity.Kind)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaySource() error = %v", err)
	}
	want := []DiscontinuityKind{
		DiscontinuityDisconnect,
		DiscontinuityOrdinalGap,
		DiscontinuityQuarantinedFrame,
		DiscontinuityMissingSegment,
		DiscontinuityEpochBoundary,
	}
	if !slices.Equal(kinds, want) {
		t.Fatalf("discontinuity order = %v, want %v", kinds, want)
	}

	t.Run("frame boundaries validate before emission", func(t *testing.T) {
		publication, data, manifest := testPublicationBytes(t, 47, testSourceA, epoch2, []segment.Envelope{testRecord(t, testSourceA, epoch2, 1, 0, 202, []byte("frame"))})
		data = bytes.Clone(data)
		data[0] ^= 0xff
		publication.ContentSHA256 = sha256.Sum256(data)
		manifest.Segment.CompressedSHA256 = publication.ContentSHA256
		bindManifest(t, &publication, manifest)
		assertQuarantinedBeforeEmission(t, publication, data)
	})
	t.Run("per-record CRC validates before emission", func(t *testing.T) {
		publication, data, manifest := testPublicationBytes(t, 50, testSourceA, epoch2, []segment.Envelope{testRecord(t, testSourceA, epoch2, 1, 0, 205, []byte("crc"))})
		data, manifest = corruptRecordCRC(t, data, manifest)
		publication.ContentSHA256 = manifest.Segment.CompressedSHA256
		publication.ByteLength = int64(len(data))
		bindManifest(t, &publication, manifest)
		assertQuarantinedBeforeEmission(t, publication, data)
	})
	t.Run("record count validates before emission", func(t *testing.T) {
		publication, data, manifest := testPublicationBytes(t, 48, testSourceA, epoch2, []segment.Envelope{testRecord(t, testSourceA, epoch2, 1, 0, 203, []byte("count"))})
		manifest.Segment.RecordCount++
		bindManifest(t, &publication, manifest)
		assertQuarantinedBeforeEmission(t, publication, data)
	})
	t.Run("source and epoch identity validate before emission", func(t *testing.T) {
		publication, data, manifest := testPublicationBytes(t, 49, testSourceA, epoch2, []segment.Envelope{testRecord(t, testSourceA, epoch2, 1, 0, 204, []byte("identity"))})
		publication.SourceID = testSourceB
		manifest.SourceID = testSourceB
		bindManifest(t, &publication, manifest)
		assertQuarantinedBeforeEmission(t, publication, data)
	})

	publication := testPublication(t, 46, testSourceA, epoch2, []segment.Envelope{testRecord(t, testSourceA, epoch2, 2, 0, 201, []byte("quarantined"))})
	publication.State = catalog.RawSegmentQuarantined
	if _, err := NewInputDescriptor(publication); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("NewInputDescriptor(quarantined) error = %v, want ErrInvalidInput", err)
	}
}

func TestDiscontinuityLeadingEpochRange(t *testing.T) {
	objects := &memoryObjects{objects: make(map[string][]byte)}
	epoch := "abababab-abab-4bab-8bab-abababababab"
	input := testInput(t, objects, 51, testSourceA, epoch, []segment.Envelope{
		testRecord(t, testSourceA, epoch, 5, 0, 500, []byte("five")),
	})
	var events []Event
	first, err := ReplaySource(t.Context(), objects, []InputDescriptor{input}, DefaultConfig(), func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaySource() error = %v", err)
	}
	want := []string{
		fmt.Sprintf("d:%d:1-4", DiscontinuityOrdinalGap),
		"r:5:0",
	}
	if got := eventSignatures(events); !slices.Equal(got, want) {
		t.Fatalf("ordered events = %v, want %v", got, want)
	}
	var repeated []Event
	config := DefaultConfig()
	config.Concurrency = MaximumWorkers
	second, err := ReplaySource(t.Context(), objects, []InputDescriptor{input}, config, func(event Event) error {
		repeated = append(repeated, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaySource(max concurrency) error = %v", err)
	}
	if first.LogicalHash != second.LogicalHash || !slices.Equal(eventSignatures(repeated), want) {
		t.Fatalf("leading-range replay changed across concurrency: hashes %x/%x events %v", first.LogicalHash, second.LogicalHash, eventSignatures(repeated))
	}
}

func TestDiscontinuityPartialOverlap(t *testing.T) {
	objects := &memoryObjects{objects: make(map[string][]byte)}
	epoch := "acacacac-acac-4cac-8cac-acacacacacac"
	firstRecords := make([]segment.Envelope, 0, 5)
	for ordinal := uint64(1); ordinal <= 5; ordinal++ {
		firstRecords = append(firstRecords, testRecord(t, testSourceA, epoch, ordinal, 0, 600+int64(ordinal), []byte(fmt.Sprintf("first-%d", ordinal))))
	}
	secondRecords := make([]segment.Envelope, 0, 6)
	for ordinal := uint64(5); ordinal <= 10; ordinal++ {
		secondRecords = append(secondRecords, testRecord(t, testSourceA, epoch, ordinal, 0, 700+int64(ordinal), []byte(fmt.Sprintf("second-%d", ordinal))))
	}
	firstInput := testInput(t, objects, 52, testSourceA, epoch, firstRecords)
	secondInput := testInput(t, objects, 53, testSourceA, epoch, secondRecords)
	var events []Event
	baseline, err := ReplaySource(t.Context(), objects, []InputDescriptor{firstInput, secondInput}, DefaultConfig(), func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaySource() error = %v", err)
	}
	want := []string{
		"r:1:0", "r:2:0", "r:3:0", "r:4:0", "r:5:0",
		fmt.Sprintf("d:%d:5-5", DiscontinuityOrdinalOverlap),
		"r:6:0", "r:7:0", "r:8:0", "r:9:0", "r:10:0",
	}
	if got := eventSignatures(events); !slices.Equal(got, want) {
		t.Fatalf("ordered events = %v, want %v", got, want)
	}
	config := DefaultConfig()
	config.Concurrency = MaximumWorkers
	config.Prefetch = 2
	var reordered []Event
	repeated, err := ReplaySource(t.Context(), objects, []InputDescriptor{secondInput, firstInput}, config, func(event Event) error {
		reordered = append(reordered, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaySource(reordered,max concurrency) error = %v", err)
	}
	if baseline.LogicalHash != repeated.LogicalHash || !slices.Equal(eventSignatures(reordered), want) {
		t.Fatalf("partial-overlap replay changed: hashes %x/%x events %v", baseline.LogicalHash, repeated.LogicalHash, eventSignatures(reordered))
	}

	descriptor := secondInput
	descriptor.ordinalStart = 5
	descriptor.ordinalEnd = 7
	_, epochBytes, err := parseCanonicalUUID(epoch)
	if err != nil {
		t.Fatal(err)
	}
	retained := []segment.Envelope{
		testRecord(t, testSourceA, epoch, 5, 9, 705, []byte("covered")),
		testRecord(t, testSourceA, epoch, 6, 0, 706, []byte("six-zero")),
		testRecord(t, testSourceA, epoch, 6, 1, 706, []byte("six-one")),
		testRecord(t, testSourceA, epoch, 7, 0, 707, []byte("seven")),
	}
	descriptor.epochBytes = epochBytes
	decoded := decodedSegment{descriptor: descriptor, records: retained, disconnects: make([]bool, len(retained))}
	epochGroup := epochPlan{sourceID: testSourceA, epochID: epoch, firstNS: 600}
	next := uint64(6)
	var retainedEvents []Event
	if err := emitDecodedSegment(epochGroup, decoded, &next, func(event Event) error {
		retainedEvents = append(retainedEvents, event)
		return nil
	}); err != nil {
		t.Fatalf("emitDecodedSegment() error = %v", err)
	}
	wantRetained := []string{
		fmt.Sprintf("d:%d:5-5", DiscontinuityOrdinalOverlap),
		"r:6:0", "r:6:1", "r:7:0",
	}
	if got := eventSignatures(retainedEvents); !slices.Equal(got, wantRetained) || next != 8 {
		t.Fatalf("retained overlap events/next = %v/%d, want %v/8", got, next, wantRetained)
	}
}

func eventSignatures(events []Event) []string {
	result := make([]string, 0, len(events))
	for _, event := range events {
		switch event.Kind {
		case EventRecord:
			result = append(result, fmt.Sprintf("r:%d:%d", event.Coordinate.ArrivalOrdinal, event.Coordinate.MessageOrdinal))
		case EventDiscontinuity:
			result = append(result, fmt.Sprintf("d:%d:%d-%d", event.Discontinuity.Kind, event.Discontinuity.FirstOrdinal, event.Discontinuity.LastOrdinal))
		}
	}
	return result
}

func corruptRecordCRC(t *testing.T, compressed []byte, manifest segment.ReadyManifest) ([]byte, segment.ReadyManifest) {
	t.Helper()
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := decoder.DecodeAll(compressed, nil)
	decoder.Close()
	if err != nil {
		t.Fatal(err)
	}
	plain[segment.FrameHeaderSize+12] ^= 0xff
	recordsHash := sha256.Sum256(plain[segment.FrameHeaderSize:])
	copy(plain[56:88], recordsHash[:])
	encoder, err := zstd.NewWriter(nil,
		zstd.WithEncoderCRC(true),
		zstd.WithEncoderConcurrency(1),
		zstd.WithWindowSize(1<<20),
		zstd.WithSingleSegment(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	corrupted := encoder.EncodeAll(plain, nil)
	encoder.Close()
	compressedHash := sha256.Sum256(corrupted)
	uncompressedHash := sha256.Sum256(plain)
	manifest.Segment.CompressedBytes = uint64(len(corrupted))
	manifest.Segment.UncompressedBytes = uint64(len(plain))
	manifest.Segment.CompressedSHA256 = compressedHash
	manifest.Segment.UncompressedSHA256 = uncompressedHash
	manifest.Segment.Frames[0].CompressedBytes = uint64(len(corrupted))
	manifest.Segment.Frames[0].UncompressedBytes = uint64(len(plain))
	manifest.Segment.Frames[0].CompressedSHA256 = compressedHash
	manifest.Segment.Frames[0].UncompressedSHA256 = uncompressedHash
	return corrupted, manifest
}
func assertQuarantinedBeforeEmission(t *testing.T, publication catalog.RawSegmentPublication, data []byte) {
	t.Helper()
	descriptor, err := NewInputDescriptor(publication)
	if err != nil {
		t.Fatalf("NewInputDescriptor() error = %v", err)
	}
	objects := &memoryObjects{objects: map[string][]byte{publication.ObjectKey: bytes.Clone(data)}}
	var events []Event
	_, err = ReplaySource(t.Context(), objects, []InputDescriptor{descriptor}, DefaultConfig(), func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplaySource() error = %v", err)
	}
	if len(events) != 1 || events[0].Kind != EventDiscontinuity || events[0].Discontinuity.Kind != DiscontinuityQuarantinedFrame {
		t.Fatalf("events = %+v, want one quarantined-frame discontinuity and no record emission", events)
	}
}

func testRecord(t *testing.T, sourceID, epochID string, arrival uint64, message uint32, received int64, payload []byte) segment.Envelope {
	t.Helper()
	_, epochBytes, err := parseCanonicalUUID(epochID)
	if err != nil {
		t.Fatal(err)
	}
	return segment.Envelope{
		Kind:               segment.RecordKindWebSocket,
		SourceID:           sourceID,
		ChannelOrEndpoint:  testChannel,
		ConnectionEpoch:    segment.OptionalEpoch{Value: epochBytes, Valid: true},
		ArrivalOrdinal:     arrival,
		MessageOrdinal:     message,
		ReceivedWallTimeNS: received,
		ClockEpochID:       "clock-1",
		PayloadEncoding:    segment.PayloadEncodingBinary,
		RawPayload:         bytes.Clone(payload),
		TerminalOutcome:    segment.OutcomeObserved,
		RecorderVersion:    "test-recorder-v1",
	}
}

func testDisconnectRecord(t *testing.T, sourceID, epochID string, arrival uint64, received int64) segment.Envelope {
	t.Helper()
	base := testRecord(t, sourceID, epochID, arrival, 0, received, nil)
	envelope, err := capture.EnvelopeV1FromSegment(base)
	if err != nil {
		t.Fatalf("EnvelopeV1FromSegment() error = %v", err)
	}
	control, err := capture.NewControlRecord(capture.ControlDisconnect, envelope)
	if err != nil {
		t.Fatalf("NewControlRecord() error = %v", err)
	}
	record, err := control.ToSegment()
	if err != nil {
		t.Fatalf("ControlRecord.ToSegment() error = %v", err)
	}
	return record
}

func testInput(t *testing.T, objects *memoryObjects, id int, sourceID, epochID string, records []segment.Envelope) InputDescriptor {
	t.Helper()
	publication, data, _ := testPublicationBytes(t, id, sourceID, epochID, records)
	descriptor, err := NewInputDescriptor(publication)
	if err != nil {
		t.Fatalf("NewInputDescriptor() error = %v", err)
	}
	objects.put(publication.ObjectKey, data)
	return descriptor
}

func testPublication(t *testing.T, id int, sourceID, epochID string, records []segment.Envelope) catalog.RawSegmentPublication {
	t.Helper()
	publication, _, _ := testPublicationBytes(t, id, sourceID, epochID, records)
	return publication
}

func testPublicationBytes(t *testing.T, id int, sourceID, epochID string, records []segment.Envelope) (catalog.RawSegmentPublication, []byte, segment.ReadyManifest) {
	t.Helper()
	encoded, err := segment.Encode(records, segment.EncodeOptions{FrameBytes: 1 << 20, Concurrency: 1})
	if err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	segmentID := fmt.Sprintf("10000000-0000-4000-8000-%012d", id)
	objectKey := fmt.Sprintf("raw/test/%s.emseg.zst", segmentID)
	manifest := segment.ReadyManifest{
		ManifestVersion: segment.SpoolManifestVersion,
		SourceID:        sourceID,
		ChannelID:       testChannel,
		EpochKind:       segment.EpochConnection,
		EpochID:         strings.ReplaceAll(epochID, "-", ""),
		WriterVersion:   "test-writer-v1",
		SegmentFile:     segmentID + ".emseg.zst",
		ObjectKey:       objectKey,
		Segment:         encoded.Manifest,
	}
	publication := catalog.RawSegmentPublication{
		SegmentID:       segmentID,
		SourceID:        sourceID,
		ChannelID:       testChannel,
		EpochID:         epochID,
		ReceivedStartNS: encoded.Manifest.FirstReceivedAtNS,
		ReceivedEndNS:   encoded.Manifest.LastReceivedAtNS,
		OrdinalStart:    encoded.Manifest.FirstOrdinal,
		OrdinalEnd:      encoded.Manifest.LastOrdinal,
		ObjectKey:       objectKey,
		ContentSHA256:   encoded.Manifest.CompressedSHA256,
		ByteLength:      int64(len(encoded.Bytes)),
		ManifestVersion: segment.SpoolManifestVersion,
		State:           catalog.RawSegmentCommitted,
	}
	bindManifest(t, &publication, manifest)
	return publication, encoded.Bytes, manifest
}

func bindManifest(t *testing.T, publication *catalog.RawSegmentPublication, manifest segment.ReadyManifest) {
	t.Helper()
	manifestBytes, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	publication.ManifestSHA256 = sha256.Sum256(manifestBytes)
	publication.ManifestBytes = manifestBytes
}

func assertSameEvents(t *testing.T, got, want []Event) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("event count = %d, want %d", len(got), len(want))
	}
	for i := range got {
		if got[i].Kind != want[i].Kind || got[i].Order != want[i].Order || got[i].Coordinate != want[i].Coordinate || got[i].Discontinuity != want[i].Discontinuity || !bytes.Equal(got[i].Record.RawPayload, want[i].Record.RawPayload) {
			t.Fatalf("event %d differs: got %+v want %+v", i, got[i], want[i])
		}
	}
}
