package segment

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"testing"
)

func TestAllBoundaryTruncationAndCorruption(t *testing.T) {
	encoded, err := Encode(goldenRecords(), EncodeOptions{FrameBytes: 1 << 20, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	for cut := range len(encoded.Bytes) {
		result, err := Decode(encoded.Bytes[:cut], &encoded.Manifest)
		if err == nil || !IsDamage(err) {
			t.Fatalf("cut %d/%d error = %v, want detected damage", cut, len(encoded.Bytes), err)
		}
		if result.CompleteBytes != 0 || len(result.Records) != 0 {
			t.Fatalf("cut %d retained incomplete frame: %+v", cut, result)
		}
	}
	for position := range len(encoded.Bytes) {
		damaged := append([]byte(nil), encoded.Bytes...)
		damaged[position] ^= 0x5a
		if _, err := Decode(damaged, &encoded.Manifest); err == nil || !IsDamage(err) {
			t.Fatalf("corruption at byte %d error = %v, want detected damage", position, err)
		}
	}
}

func TestSeededInteriorCorruptions(t *testing.T) {
	encoded, err := Encode(goldenRecords(), EncodeOptions{FrameBytes: 1 << 20, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded.Bytes) < 102 {
		t.Fatalf("golden frame too short for 100 unique interior corruptions: %d", len(encoded.Bytes))
	}
	positions := seededUniquePositions(0x454c4d440003, len(encoded.Bytes), 100)
	for trial, position := range positions {
		damaged := append([]byte(nil), encoded.Bytes...)
		damaged[position] ^= byte(1 + trial%255)
		if _, err := Decode(damaged, &encoded.Manifest); err == nil || !IsDamage(err) {
			t.Fatalf("seeded trial %d at byte %d error = %v, want detected damage", trial, position, err)
		}
	}
}

func TestDecodeRetainsEarlierCompleteFrames(t *testing.T) {
	records, err := SyntheticRecords(CorpusPerpetualBook, 42, []int{600 << 10, 600 << 10, 600 << 10})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Encode(records, EncodeOptions{FrameBytes: 1 << 20, Concurrency: 2})
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded.Manifest.Frames) != 3 {
		t.Fatalf("got %d frames, want 3", len(encoded.Manifest.Frames))
	}

	second := encoded.Manifest.Frames[1]
	cut := int(second.CompressedOffset)
	result, err := Decode(encoded.Bytes[:cut], &encoded.Manifest)
	if !errors.Is(err, ErrTruncated) {
		t.Fatalf("exact frame-boundary truncation error = %v, want ErrTruncated", err)
	}
	if len(result.Frames) != 1 || len(result.Records) != 1 || result.CompleteBytes != uint64(cut) {
		t.Fatalf("boundary truncation retained %+v, want exactly first frame", result)
	}

	damaged := append([]byte(nil), encoded.Bytes...)
	position := int(second.CompressedOffset + second.CompressedBytes/2)
	damaged[position] ^= 0x80
	result, err = Decode(damaged, &encoded.Manifest)
	if err == nil || !IsDamage(err) {
		t.Fatalf("second-frame corruption error = %v, want detected damage", err)
	}
	if len(result.Frames) != 1 || len(result.Records) != 1 || result.CompleteBytes != second.CompressedOffset {
		t.Fatalf("corruption retained %+v, want exactly first frame", result)
	}
}

func TestRecordCRC32CAndPayloadSHA256(t *testing.T) {
	record := goldenRecords()[0]
	encoded, err := encodeRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	crcDamage := append([]byte(nil), encoded...)
	crcDamage[len(crcDamage)-1] ^= 1
	if _, _, err := decodeRecord(crcDamage); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("record CRC damage error = %v, want ErrCorrupt", err)
	}

	hashDamage := append([]byte(nil), encoded...)
	payloadOffset := bytes.Index(hashDamage[RecordHeaderSize:], record.RawPayload)
	if payloadOffset < 0 {
		t.Fatal("payload bytes absent from exact binary framing")
	}
	hashDamage[RecordHeaderSize+payloadOffset] ^= 1
	body := hashDamage[RecordHeaderSize:]
	binary.LittleEndian.PutUint32(hashDamage[12:16], crc32.Checksum(body, crc32cTable))
	if _, _, err := decodeRecord(hashDamage); !errors.Is(err, ErrPayloadHash) {
		t.Fatalf("payload hash damage error = %v, want ErrPayloadHash", err)
	}
}

func TestManifestSummaryIntegrity(t *testing.T) {
	records, err := SyntheticRecords(CorpusPerpetualBook, 73, []int{600 << 10, 600 << 10, 600 << 10})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Encode(records, EncodeOptions{FrameBytes: 1 << 20, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	mutations := []struct {
		name   string
		mutate func(*Manifest)
	}{
		{name: "frame bound", mutate: func(m *Manifest) { m.FrameBytes = 4 << 20 }},
		{name: "record count", mutate: func(m *Manifest) { m.RecordCount++ }},
		{name: "uncompressed bytes", mutate: func(m *Manifest) { m.UncompressedBytes++ }},
		{name: "compressed bytes", mutate: func(m *Manifest) { m.CompressedBytes++ }},
		{name: "first ordinal", mutate: func(m *Manifest) { m.FirstOrdinal++ }},
		{name: "last ordinal", mutate: func(m *Manifest) { m.LastOrdinal++ }},
		{name: "first receive time", mutate: func(m *Manifest) { m.FirstReceivedAtNS++ }},
		{name: "last receive time", mutate: func(m *Manifest) { m.LastReceivedAtNS++ }},
		{name: "uncompressed hash", mutate: func(m *Manifest) { m.UncompressedSHA256[0] ^= 1 }},
		{name: "compressed hash", mutate: func(m *Manifest) { m.CompressedSHA256[0] ^= 1 }},
	}
	for _, test := range mutations {
		t.Run(test.name, func(t *testing.T) {
			manifest := encoded.Manifest
			test.mutate(&manifest)
			if _, err := Decode(encoded.Bytes, &manifest); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("mutated manifest error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestCandidateProbeUsesMultiFramePrefixRecovery(t *testing.T) {
	records, err := SyntheticRecords(CorpusPerpetualBook, 91, []int{600 << 10, 600 << 10, 600 << 10})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := Encode(records, EncodeOptions{FrameBytes: 1 << 20, Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := probeEncodedCandidate(encoded, 91)
	if err != nil {
		t.Fatal(err)
	}
	if probe.truncationsInjected == 0 || probe.truncationsDetected != probe.truncationsInjected ||
		probe.corruptionsInjected != 100 || probe.corruptionsDetected != probe.corruptionsInjected ||
		probe.prefixRecoveriesDetected != probe.prefixRecoveriesInjected {
		t.Fatalf("candidate probe did not detect damage with exact prefix recovery: %+v", probe)
	}
}

func seededUniquePositions(seed uint64, length, count int) []int {
	positions := make([]int, 0, count)
	seen := make(map[int]struct{}, count)
	state := seed
	for len(positions) < count {
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		position := 1 + int((state*0x2545f4914f6cdd1d)%uint64(length-2))
		if _, exists := seen[position]; exists {
			continue
		}
		seen[position] = struct{}{}
		positions = append(positions, position)
	}
	return positions
}
