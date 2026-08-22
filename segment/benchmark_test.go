package segment

import (
	"fmt"
	"testing"
)

func BenchmarkFrame(b *testing.B) {
	for _, frameBytes := range X1FrameSizes {
		for _, concurrency := range X1Concurrencies {
			name := fmt.Sprintf("frame_%dMiB/concurrency_%d", frameBytes>>20, concurrency)
			b.Run(name, func(b *testing.B) {
				records := benchmarkRecords(b, frameBytes-(128<<10))
				b.ReportAllocs()
				b.SetBytes(benchmarkPayloadBytes(records))
				for b.Loop() {
					encoded, err := Encode(records, EncodeOptions{FrameBytes: frameBytes, Concurrency: concurrency})
					if err != nil {
						b.Fatal(err)
					}
					if len(encoded.Manifest.Frames) != 1 {
						b.Fatalf("got %d frames, want 1", len(encoded.Manifest.Frames))
					}
				}
			})
		}
	}
}

func BenchmarkSegment(b *testing.B) {
	for _, frameBytes := range X1FrameSizes {
		for _, concurrency := range X1Concurrencies {
			name := fmt.Sprintf("frame_%dMiB/concurrency_%d", frameBytes>>20, concurrency)
			b.Run(name, func(b *testing.B) {
				payloadBytes := frameBytes * 4
				records := benchmarkRecords(b, payloadBytes)
				b.ReportAllocs()
				b.SetBytes(benchmarkPayloadBytes(records))
				for b.Loop() {
					encoded, err := Encode(records, EncodeOptions{FrameBytes: frameBytes, Concurrency: concurrency})
					if err != nil {
						b.Fatal(err)
					}
					if encoded.Manifest.RecordCount != uint64(len(records)) {
						b.Fatalf("manifest record count = %d, want %d", encoded.Manifest.RecordCount, len(records))
					}
				}
			})
		}
	}
}

func benchmarkRecords(b *testing.B, payloadBytes int) []Envelope {
	b.Helper()
	const recordPayload = 64 << 10
	sizes := make([]int, 0, payloadBytes/recordPayload+1)
	for payloadBytes > 0 {
		size := min(recordPayload, payloadBytes)
		sizes = append(sizes, size)
		payloadBytes -= size
	}
	records, err := SyntheticRecords(CorpusPerpetualBook, 0x5841, sizes)
	if err != nil {
		b.Fatal(err)
	}
	return records
}

func benchmarkPayloadBytes(records []Envelope) int64 {
	var total int64
	for i := range records {
		total += int64(len(records[i].RawPayload))
	}
	return total
}
