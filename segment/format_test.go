package segment

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"
)

func TestFormatGolden(t *testing.T) {
	records := goldenRecords()
	encodedRecord, err := encodeRecord(records[0])
	if err != nil {
		t.Fatal(err)
	}
	wantRecord, err := os.ReadFile("testdata/envelope_v1.bin")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encodedRecord, wantRecord) {
		t.Fatalf("envelope v1 differs from committed golden: got %x want %x", encodedRecord, wantRecord)
	}

	encoded, err := Encode(records, EncodeOptions{FrameBytes: 1 << 20, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	wantSegment, err := os.ReadFile("testdata/segment_v1.zst")
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded.Bytes, wantSegment) {
		t.Fatalf("segment v1 differs from committed golden: got %x want %x", encoded.Bytes, wantSegment)
	}
	manifestData, err := os.ReadFile("testdata/segment_v1_manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var wantManifest Manifest
	if err := json.Unmarshal(manifestData, &wantManifest); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(encoded.Manifest, wantManifest) {
		t.Fatalf("golden manifest differs: got %+v want %+v", encoded.Manifest, wantManifest)
	}
	decoded, err := Decode(wantSegment, &encoded.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(decoded.Records, records) {
		t.Fatalf("golden round trip differs: got %#v want %#v", decoded.Records, records)
	}
	if len(decoded.Frames) != 1 || decoded.Frames[0] != encoded.Manifest.Frames[0] {
		t.Fatalf("golden index differs: got %+v want %+v", decoded.Frames, encoded.Manifest.Frames)
	}
}

func TestFormatEvolution(t *testing.T) {
	record := goldenRecords()[0]
	record.Extensions = []byte{0x80, 0x01, 0xff, 0x00}
	encoded, err := encodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, consumed, err := decodeRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if consumed != len(encoded) || !bytes.Equal(decoded.Extensions, record.Extensions) {
		t.Fatalf("opaque v1 extensions not retained: consumed=%d got=%x", consumed, decoded.Extensions)
	}

	future := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint16(future[4:6], EnvelopeVersion+1)
	if _, _, err := decodeRecord(future); !errors.Is(err, ErrVersion) {
		t.Fatalf("future envelope version error = %v, want ErrVersion", err)
	}
	changedHeader := append([]byte(nil), encoded...)
	binary.LittleEndian.PutUint16(changedHeader[6:8], RecordHeaderSize+1)
	if _, _, err := decodeRecord(changedHeader); !errors.Is(err, ErrVersion) {
		t.Fatalf("changed header size error = %v, want ErrVersion", err)
	}
	jobs, err := buildFrameJobs(goldenRecords(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	futureFrame := append([]byte(nil), jobs[0].plain...)
	binary.LittleEndian.PutUint16(futureFrame[8:10], FormatVersion+1)
	if _, _, err := decodeFrame(futureFrame, 0, 0, 0); !errors.Is(err, ErrVersion) {
		t.Fatalf("future frame version error = %v, want ErrVersion", err)
	}
}

func TestFormatBoundsAndExactPayload(t *testing.T) {
	record := goldenRecords()[0]
	record.RawPayload = []byte{0x00, 0xff, 0x00, 0xfe, '\n', '\r'}
	encoded, err := encodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	decoded, _, err := decodeRecord(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decoded.RawPayload, record.RawPayload) {
		t.Fatalf("payload changed: got %x want %x", decoded.RawPayload, record.RawPayload)
	}
	record.RawPayload = make([]byte, MaxPayloadBytes+1)
	if _, err := encodeRecord(record); !errors.Is(err, ErrBounds) {
		t.Fatalf("oversized payload error = %v, want ErrBounds", err)
	}
}

func TestSegmentDeterministicAcrossConcurrency(t *testing.T) {
	records, err := SyntheticRecords(CorpusPerpetualBook, 99, []int{600 << 10, 600 << 10, 600 << 10})
	if err != nil {
		t.Fatal(err)
	}
	one, err := Encode(records, EncodeOptions{FrameBytes: 1 << 20, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	two, err := Encode(records, EncodeOptions{FrameBytes: 1 << 20, Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(one.Bytes, two.Bytes) || !reflect.DeepEqual(one.Manifest, two.Manifest) {
		t.Fatal("concurrency changed encoded bytes, hashes, index, or bounds")
	}
}

func TestX1DecisionRecord(t *testing.T) {
	data, err := os.ReadFile("testdata/x1_decision.json")
	if err != nil {
		t.Fatal(err)
	}
	var committed DecisionRecord
	if err := json.Unmarshal(data, &committed); err != nil {
		t.Fatal(err)
	}
	if got := FrozenDecision(); !reflect.DeepEqual(got, committed) {
		t.Fatalf("decision record drift: got %+v want %+v", got, committed)
	}
	if (AlternateEvidence{ExactPayloads: true, EvolutionCompatible: true, RecoveryUnambiguous: true, ThroughputImprovement: 0.149999, PeakRSSIncrease: 0}).Qualifies() {
		t.Fatal("alternate below 15% qualified")
	}
	if !(AlternateEvidence{ExactPayloads: true, EvolutionCompatible: true, RecoveryUnambiguous: true, CompressedByteReduction: 0.15, PeakRSSIncrease: 0}).Qualifies() {
		t.Fatal("eligible alternate at 15% did not qualify")
	}
	if (AlternateEvidence{ExactPayloads: true, EvolutionCompatible: true, RecoveryUnambiguous: true, ThroughputImprovement: 0.20, PeakRSSIncrease: 1}).Qualifies() {
		t.Fatal("alternate with increased peak RSS qualified")
	}
}

func TestRotationPolicy(t *testing.T) {
	start := time.Unix(100, 0)
	policy := DefaultRotationPolicy()
	cases := []struct {
		name     string
		bytes    int
		now      time.Time
		epochEnd bool
		shutdown bool
		want     RotationReason
	}{
		{name: "below bounds", bytes: DefaultSegmentBytes - 1, now: start.Add(DefaultSegmentAge - 1), want: RotationNone},
		{name: "size", bytes: DefaultSegmentBytes, now: start, want: RotationSize},
		{name: "age", now: start.Add(DefaultSegmentAge), want: RotationAge},
		{name: "epoch", now: start, epochEnd: true, want: RotationEpochEnd},
		{name: "shutdown", now: start, shutdown: true, want: RotationShutdown},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			if got := policy.Reason(test.bytes, start, test.now, test.epochEnd, test.shutdown); got != test.want {
				t.Fatalf("Reason() = %d, want %d", got, test.want)
			}
		})
	}
}

func TestSyntheticCorpusDeterministic(t *testing.T) {
	for _, family := range RepresentativeFamilies {
		first, err := NewSyntheticCorpus(family, 1234)
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewSyntheticCorpus(family, 1234)
		if err != nil {
			t.Fatal(err)
		}
		payloadBytes := 4097
		if family == CorpusReconnectControl {
			payloadBytes = 0
		}
		for range 8 {
			left, err := first.Next(payloadBytes)
			if err != nil {
				t.Fatal(err)
			}
			right, err := second.Next(payloadBytes)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(left, right) || len(left.RawPayload) != payloadBytes {
				t.Fatalf("family %d corpus is not deterministic or exact-sized", family)
			}
		}
	}
}

func TestSegmentRejectsMixedBoundary(t *testing.T) {
	records := goldenRecords()
	records[1].ChannelOrEndpoint = "other-v1"
	if _, err := Encode(records, EncodeOptions{}); !errors.Is(err, ErrBounds) {
		t.Fatalf("mixed channel error = %v, want ErrBounds", err)
	}
}

func TestMaximumPayloadRoundTrip(t *testing.T) {
	record := goldenRecords()[0]
	record.RawPayload = syntheticPayload(CorpusPerpetualBook, 77, 1, MaxPayloadBytes)
	encoded, err := Encode([]Envelope{record}, EncodeOptions{FrameBytes: MaxSupportedFrameBytes, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Decode(encoded.Bytes, &encoded.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	if len(decoded.Records) != 1 || !bytes.Equal(decoded.Records[0].RawPayload, record.RawPayload) {
		t.Fatal("maximum accepted payload did not round trip exactly")
	}
}

func TestX1LogicalCorpusIndependentOfFrameBoundaries(t *testing.T) {
	records, err := x1LogicalRecords(0x454c4d440003, 2<<20)
	if err != nil {
		t.Fatal(err)
	}
	wantHash := logicalRecordHash(t, records)
	for _, frameBytes := range X1FrameSizes {
		encoded, err := Encode(records, EncodeOptions{FrameBytes: frameBytes, Concurrency: 1})
		if err != nil {
			t.Fatal(err)
		}
		decoded, err := Decode(encoded.Bytes, &encoded.Manifest)
		if err != nil {
			t.Fatal(err)
		}
		if got := logicalRecordHash(t, decoded.Records); got != wantHash {
			t.Fatalf("%d-byte candidate changed logical corpus hash: got %x want %x", frameBytes, got, wantHash)
		}
	}
}

func TestSyntheticCorpusMarketShape(t *testing.T) {
	records, err := SyntheticRecords(CorpusPerpetualBook, 19, []int{512 << 10})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(records[0].RawPayload, []byte(`"bids"`)) || !bytes.Contains(records[0].RawPayload, []byte(`"asks"`)) {
		t.Fatal("book corpus lacks repeated market book structure")
	}
	encoded, err := Encode(records, EncodeOptions{FrameBytes: 1 << 20, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if encoded.Manifest.CompressedBytes >= uint64(len(records[0].RawPayload))/4 {
		t.Fatalf("market-shaped corpus did not compress as expected: %d compressed for %d payload bytes", encoded.Manifest.CompressedBytes, len(records[0].RawPayload))
	}
}

func logicalRecordHash(t *testing.T, records []Envelope) [32]byte {
	t.Helper()
	hasher := sha256.New()
	for _, record := range records {
		encoded, err := MarshalEnvelope(record)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = hasher.Write(encoded)
	}
	var sum [32]byte
	copy(sum[:], hasher.Sum(nil))
	return sum
}

func goldenRecords() []Envelope {
	connectionEpoch := OptionalEpoch{Valid: true}
	copy(connectionEpoch.Value[:], []byte("epoch-0000000001"))
	schema := OptionalHash{Valid: true}
	for i := range schema.Value {
		schema.Value[i] = byte(i)
	}
	return []Envelope{
		{
			Kind:                       RecordKindWebSocket,
			SourceID:                   "golden-source",
			ChannelOrEndpoint:          "trades-v1",
			NativeSymbol:               OptionalString{Value: "BTC/USD", Valid: true},
			InstrumentUID:              OptionalString{Value: "instrument-0001", Valid: true},
			ConnectionEpoch:            connectionEpoch,
			ArrivalOrdinal:             41,
			MessageOrdinal:             2,
			ExchangeTimeNS:             OptionalInt64{Value: 1700000000123456789, Valid: true},
			ExchangeTimeResolution:     TimeResolutionNanosecond,
			ReceivedWallTimeNS:         1700000000123556789,
			ClockEpochID:               "clock-0001",
			MonotonicNSSinceClockEpoch: 123456789,
			ClockOffsetNS:              OptionalInt64{Value: -125000, Valid: true},
			ClockUncertaintyNS:         OptionalInt64{Value: 50000, Valid: true},
			SubscriptionOrRequestID:    OptionalString{Value: "subscription-7", Valid: true},
			HTTPStatusOrWSState:        OptionalString{Value: "open", Valid: true},
			PayloadEncoding:            PayloadEncodingBinary,
			RawPayload:                 []byte{0x00, 0x01, 0x7f, 0x80, 0xfe, 0xff},
			SchemaFingerprint:          schema,
			TerminalOutcome:            OutcomeObserved,
			RecorderVersion:            "golden-recorder-v1",
			Extensions:                 []byte{0x01, 0x02, 0x03},
		},
		{
			Kind:                       RecordKindWebSocket,
			SourceID:                   "golden-source",
			ChannelOrEndpoint:          "trades-v1",
			ConnectionEpoch:            connectionEpoch,
			ArrivalOrdinal:             42,
			ReceivedWallTimeNS:         1700000000123556790,
			ClockEpochID:               "clock-0001",
			MonotonicNSSinceClockEpoch: 123456790,
			PayloadEncoding:            PayloadEncodingNone,
			TerminalOutcome:            OutcomeDisconnected,
			RecorderVersion:            "golden-recorder-v1",
		},
	}
}
