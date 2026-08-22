package segment

import (
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
)

const (
	FrameHeaderSize            = 88
	MaxSupportedFrameBytes     = 16 << 20
	maxEncodedFrameWindowBytes = 32 << 20
	maxDecodedFrameBytes       = maxEncodedFrameWindowBytes + MaxRecordBytes
)

var frameMagic = [8]byte{'E', 'M', 'S', 'G', 'F', 'R', 'M', '1'}

type EncodeOptions struct {
	FrameBytes  int
	Concurrency int
}

type FrameIndex struct {
	Ordinal            uint32   `json:"ordinal"`
	CompressedOffset   uint64   `json:"compressed_offset"`
	CompressedBytes    uint64   `json:"compressed_bytes"`
	UncompressedBytes  uint64   `json:"uncompressed_bytes"`
	RecordCount        uint32   `json:"record_count"`
	FirstOrdinal       uint64   `json:"first_ordinal"`
	LastOrdinal        uint64   `json:"last_ordinal"`
	FirstReceivedAtNS  int64    `json:"first_received_at_ns"`
	LastReceivedAtNS   int64    `json:"last_received_at_ns"`
	CompressedSHA256   [32]byte `json:"compressed_sha256"`
	UncompressedSHA256 [32]byte `json:"uncompressed_sha256"`
}

type Manifest struct {
	FormatVersion      uint16       `json:"format_version"`
	FrameBytes         uint64       `json:"frame_bytes"`
	RecordCount        uint64       `json:"record_count"`
	UncompressedBytes  uint64       `json:"uncompressed_bytes"`
	CompressedBytes    uint64       `json:"compressed_bytes"`
	FirstOrdinal       uint64       `json:"first_ordinal"`
	LastOrdinal        uint64       `json:"last_ordinal"`
	FirstReceivedAtNS  int64        `json:"first_received_at_ns"`
	LastReceivedAtNS   int64        `json:"last_received_at_ns"`
	CompressedSHA256   [32]byte     `json:"compressed_sha256"`
	UncompressedSHA256 [32]byte     `json:"uncompressed_sha256"`
	Frames             []FrameIndex `json:"frames"`
}

type EncodedSegment struct {
	Bytes    []byte
	Manifest Manifest
}

type DecodeResult struct {
	Records       []Envelope
	Frames        []FrameIndex
	CompleteBytes uint64
}

type DecodeError struct {
	Frame  uint32
	Offset uint64
	Err    error
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("segment: frame %d at compressed offset %d: %v", e.Frame, e.Offset, e.Err)
}

func (e *DecodeError) Unwrap() error { return e.Err }

// Encode encodes complete records into independent, byte-concatenated RFC 8878
// Zstandard frames. Compression may finish out of order; concatenation never does.
func Encode(records []Envelope, options EncodeOptions) (EncodedSegment, error) {
	options, err := normalizeEncodeOptions(options)
	if err != nil {
		return EncodedSegment{}, err
	}
	if len(records) == 0 {
		empty := sha256.Sum256(nil)
		return EncodedSegment{Manifest: Manifest{
			FormatVersion:      FormatVersion,
			FrameBytes:         uint64(options.FrameBytes),
			CompressedSHA256:   empty,
			UncompressedSHA256: empty,
		}}, nil
	}
	if err := validateSegmentRecords(records); err != nil {
		return EncodedSegment{}, err
	}

	jobs, err := buildFrameJobs(records, options.FrameBytes)
	if err != nil {
		return EncodedSegment{}, err
	}
	outputs, err := compressFrames(jobs, options.Concurrency)
	if err != nil {
		return EncodedSegment{}, err
	}

	var result EncodedSegment
	result.Manifest.FormatVersion = FormatVersion
	result.Manifest.FrameBytes = uint64(options.FrameBytes)
	result.Manifest.RecordCount = uint64(len(records))
	result.Manifest.FirstOrdinal = records[0].ArrivalOrdinal
	result.Manifest.LastOrdinal = records[len(records)-1].ArrivalOrdinal
	result.Manifest.FirstReceivedAtNS = records[0].ReceivedWallTimeNS
	result.Manifest.LastReceivedAtNS = records[len(records)-1].ReceivedWallTimeNS
	result.Manifest.Frames = make([]FrameIndex, 0, len(jobs))
	compressedHasher := sha256.New()
	uncompressedHasher := sha256.New()

	for i := range jobs {
		job := &jobs[i]
		compressed := outputs[i]
		compressedHash := sha256.Sum256(compressed)
		uncompressedHash := sha256.Sum256(job.plain)
		index := FrameIndex{
			Ordinal:            uint32(i),
			CompressedOffset:   uint64(len(result.Bytes)),
			CompressedBytes:    uint64(len(compressed)),
			UncompressedBytes:  uint64(len(job.plain)),
			RecordCount:        uint32(job.recordCount),
			FirstOrdinal:       job.firstOrdinal,
			LastOrdinal:        job.lastOrdinal,
			FirstReceivedAtNS:  job.firstReceivedAtNS,
			LastReceivedAtNS:   job.lastReceivedAtNS,
			CompressedSHA256:   compressedHash,
			UncompressedSHA256: uncompressedHash,
		}
		result.Manifest.Frames = append(result.Manifest.Frames, index)
		result.Bytes = append(result.Bytes, compressed...)
		_, _ = compressedHasher.Write(compressed)
		_, _ = uncompressedHasher.Write(job.plain)
		result.Manifest.UncompressedBytes += uint64(len(job.plain))
	}
	result.Manifest.CompressedBytes = uint64(len(result.Bytes))
	copy(result.Manifest.CompressedSHA256[:], compressedHasher.Sum(nil))
	copy(result.Manifest.UncompressedSHA256[:], uncompressedHasher.Sum(nil))
	return result, nil
}

// Decode validates each complete frame and its records before adding them to the
// result. On damage, records from all earlier complete frames remain available.
// Passing the Encode manifest also detects truncation exactly between frames.
func Decode(data []byte, expected *Manifest) (DecodeResult, error) {
	if expected != nil && expected.FormatVersion != FormatVersion {
		return DecodeResult{}, fmt.Errorf("%w: manifest format version %d", ErrVersion, expected.FormatVersion)
	}
	if expected != nil {
		if _, err := normalizeEncodeOptions(EncodeOptions{FrameBytes: int(expected.FrameBytes), Concurrency: 1}); err != nil {
			return DecodeResult{}, fmt.Errorf("%w: invalid manifest frame bound: %v", ErrCorrupt, err)
		}
	}
	decoder, err := zstd.NewReader(nil,
		zstd.WithDecoderConcurrency(1),
		zstd.WithDecoderMaxMemory(maxDecodedFrameBytes),
		zstd.WithDecoderMaxWindow(maxEncodedFrameWindowBytes),
	)
	if err != nil {
		return DecodeResult{}, fmt.Errorf("segment: create zstd decoder: %w", err)
	}
	defer decoder.Close()

	var result DecodeResult
	var offset int
	var uncompressedHasher hash.Hash
	if expected != nil {
		uncompressedHasher = sha256.New()
	}
	for offset < len(data) {
		frameOrdinal := uint32(len(result.Frames))
		frameLen, frameErr := zstdFrameLength(data[offset:])
		if frameErr != nil {
			return result, &DecodeError{Frame: frameOrdinal, Offset: uint64(offset), Err: frameErr}
		}
		compressed := data[offset : offset+frameLen]
		if expected != nil {
			if int(frameOrdinal) >= len(expected.Frames) {
				return result, &DecodeError{Frame: frameOrdinal, Offset: uint64(offset), Err: fmt.Errorf("%w: unexpected extra frame", ErrCorrupt)}
			}
			if err := validateCompressedAgainstIndex(compressed, uint64(offset), expected.Frames[frameOrdinal]); err != nil {
				return result, &DecodeError{Frame: frameOrdinal, Offset: uint64(offset), Err: err}
			}
		}
		plain, decodeErr := decoder.DecodeAll(compressed, nil)
		if decodeErr != nil {
			return result, &DecodeError{Frame: frameOrdinal, Offset: uint64(offset), Err: fmt.Errorf("%w: zstd decode: %v", ErrCorrupt, decodeErr)}
		}
		frameRecords, index, parseErr := decodeFrame(plain, frameOrdinal, uint64(offset), uint64(frameLen))
		if parseErr != nil {
			return result, &DecodeError{Frame: frameOrdinal, Offset: uint64(offset), Err: parseErr}
		}
		index.CompressedSHA256 = sha256.Sum256(compressed)
		if expected != nil {
			if err := compareFrameIndex(index, expected.Frames[frameOrdinal]); err != nil {
				return result, &DecodeError{Frame: frameOrdinal, Offset: uint64(offset), Err: err}
			}
		}
		if uncompressedHasher != nil {
			_, _ = uncompressedHasher.Write(plain)
		}
		result.Records = append(result.Records, frameRecords...)
		result.Frames = append(result.Frames, index)
		offset += frameLen
		result.CompleteBytes = uint64(offset)
	}

	if expected != nil {
		if len(result.Frames) != len(expected.Frames) {
			return result, &DecodeError{Frame: uint32(len(result.Frames)), Offset: uint64(offset), Err: fmt.Errorf("%w: got %d complete frames, want %d", ErrTruncated, len(result.Frames), len(expected.Frames))}
		}
		var uncompressedHash [32]byte
		copy(uncompressedHash[:], uncompressedHasher.Sum(nil))
		if err := validateManifestSummary(data, result, uncompressedHash, *expected); err != nil {
			return result, err
		}
	}
	return result, nil
}

type frameJob struct {
	plain             []byte
	recordCount       int
	firstOrdinal      uint64
	lastOrdinal       uint64
	firstReceivedAtNS int64
	lastReceivedAtNS  int64
}

type frameOutput struct {
	bytes []byte
	err   error
}

func normalizeEncodeOptions(options EncodeOptions) (EncodeOptions, error) {
	if options.FrameBytes == 0 {
		options.FrameBytes = DefaultFrameBytes
	}
	if options.Concurrency == 0 {
		options.Concurrency = 1
	}
	if options.FrameBytes < 1<<20 || options.FrameBytes > MaxSupportedFrameBytes || options.FrameBytes&(options.FrameBytes-1) != 0 {
		return EncodeOptions{}, fmt.Errorf("%w: frame bytes must be a power of two from 1 MiB through 16 MiB", ErrBounds)
	}
	if options.Concurrency < 1 || options.Concurrency > 64 {
		return EncodeOptions{}, fmt.Errorf("%w: concurrency must be from 1 through 64", ErrBounds)
	}
	return options, nil
}

func validateSegmentRecords(records []Envelope) error {
	first := records[0]
	for i := range records {
		record := records[i]
		if record.SourceID != first.SourceID || record.ChannelOrEndpoint != first.ChannelOrEndpoint ||
			record.ConnectionEpoch != first.ConnectionEpoch || record.PollCycleID != first.PollCycleID {
			return fmt.Errorf("%w: record %d crosses the source/channel/epoch segment boundary", ErrBounds, i)
		}
		if i > 0 && record.ArrivalOrdinal <= records[i-1].ArrivalOrdinal {
			return fmt.Errorf("%w: record %d ordinal %d is not greater than %d", ErrBounds, i, record.ArrivalOrdinal, records[i-1].ArrivalOrdinal)
		}
	}
	return nil
}

func buildFrameJobs(records []Envelope, target int) ([]frameJob, error) {
	jobs := make([]frameJob, 0, len(records)/64+1)
	encodedRecords := make([][]byte, 0, 64)
	recordBytes := 0
	start := 0
	for i, record := range records {
		encoded, err := encodeRecord(record)
		if err != nil {
			return nil, fmt.Errorf("segment: encode record %d: %w", i, err)
		}
		if len(encodedRecords) > 0 && FrameHeaderSize+recordBytes+len(encoded) > target {
			jobs = append(jobs, makeFrameJob(uint32(len(jobs)), encodedRecords, records[start:i]))
			encodedRecords = nil
			recordBytes = 0
			start = i
		}
		encodedRecords = append(encodedRecords, encoded)
		recordBytes += len(encoded)
	}
	if len(encodedRecords) > 0 {
		jobs = append(jobs, makeFrameJob(uint32(len(jobs)), encodedRecords, records[start:]))
	}
	return jobs, nil
}

func makeFrameJob(ordinal uint32, encodedRecords [][]byte, records []Envelope) frameJob {
	return makeFrameJobMeasured(ordinal, encodedRecords, records, nil)
}

func makeFrameJobMeasured(ordinal uint32, encodedRecords [][]byte, records []Envelope, hashDuration *time.Duration) frameJob {
	recordBytes := 0
	for _, encoded := range encodedRecords {
		recordBytes += len(encoded)
	}
	plain := make([]byte, FrameHeaderSize, FrameHeaderSize+recordBytes)
	copy(plain[:8], frameMagic[:])
	binary.LittleEndian.PutUint16(plain[8:10], FormatVersion)
	binary.LittleEndian.PutUint16(plain[10:12], FrameHeaderSize)
	binary.LittleEndian.PutUint32(plain[12:16], ordinal)
	binary.LittleEndian.PutUint32(plain[16:20], uint32(len(records)))
	binary.LittleEndian.PutUint32(plain[20:24], uint32(recordBytes))
	binary.LittleEndian.PutUint64(plain[24:32], records[0].ArrivalOrdinal)
	binary.LittleEndian.PutUint64(plain[32:40], records[len(records)-1].ArrivalOrdinal)
	binary.LittleEndian.PutUint64(plain[40:48], uint64(records[0].ReceivedWallTimeNS))
	binary.LittleEndian.PutUint64(plain[48:56], uint64(records[len(records)-1].ReceivedWallTimeNS))
	for _, encoded := range encodedRecords {
		plain = append(plain, encoded...)
	}
	hashStarted := time.Now()
	recordsHash := sha256.Sum256(plain[FrameHeaderSize:])
	if hashDuration != nil {
		*hashDuration += time.Since(hashStarted)
	}
	copy(plain[56:88], recordsHash[:])
	return frameJob{
		plain:             plain,
		recordCount:       len(records),
		firstOrdinal:      records[0].ArrivalOrdinal,
		lastOrdinal:       records[len(records)-1].ArrivalOrdinal,
		firstReceivedAtNS: records[0].ReceivedWallTimeNS,
		lastReceivedAtNS:  records[len(records)-1].ReceivedWallTimeNS,
	}
}

func compressFrames(jobs []frameJob, concurrency int) ([][]byte, error) {
	outputs := make([]frameOutput, len(jobs))
	workers := min(concurrency, len(jobs))
	var next atomic.Uint64
	var wait sync.WaitGroup
	for range workers {
		wait.Go(func() {
			for {
				i := int(next.Add(1) - 1)
				if i >= len(jobs) {
					return
				}
				window := nextPowerOfTwo(max(len(jobs[i].plain), zstd.MinWindowSize))
				encoder, err := zstd.NewWriter(nil,
					zstd.WithEncoderCRC(true),
					zstd.WithEncoderConcurrency(1),
					zstd.WithWindowSize(window),
					zstd.WithSingleSegment(true),
				)
				if err != nil {
					outputs[i].err = fmt.Errorf("segment: create zstd encoder for frame %d: %w", i, err)
					continue
				}
				outputs[i].bytes = encoder.EncodeAll(jobs[i].plain, nil)
				encoder.Close()
			}
		})
	}
	wait.Wait()

	encoded := make([][]byte, len(outputs))
	for i := range outputs {
		if outputs[i].err != nil {
			return nil, outputs[i].err
		}
		encoded[i] = outputs[i].bytes
	}
	return encoded, nil
}

func nextPowerOfTwo(value int) int {
	power := 1
	for power < value {
		power <<= 1
	}
	return power
}

func decodeFrame(plain []byte, ordinal uint32, compressedOffset, compressedBytes uint64) ([]Envelope, FrameIndex, error) {
	var index FrameIndex
	if len(plain) < FrameHeaderSize {
		return nil, index, fmt.Errorf("%w: frame header needs %d bytes, got %d", ErrTruncated, FrameHeaderSize, len(plain))
	}
	if string(plain[:8]) != string(frameMagic[:]) {
		return nil, index, fmt.Errorf("%w: invalid frame magic", ErrCorrupt)
	}
	if version := binary.LittleEndian.Uint16(plain[8:10]); version != FormatVersion {
		return nil, index, fmt.Errorf("%w: frame version %d", ErrVersion, version)
	}
	if size := binary.LittleEndian.Uint16(plain[10:12]); size != FrameHeaderSize {
		return nil, index, fmt.Errorf("%w: frame header size %d", ErrVersion, size)
	}
	if binary.LittleEndian.Uint32(plain[12:16]) != ordinal {
		return nil, index, fmt.Errorf("%w: frame ordinal got %d want %d", ErrCorrupt, binary.LittleEndian.Uint32(plain[12:16]), ordinal)
	}
	recordCount := binary.LittleEndian.Uint32(plain[16:20])
	recordBytes := binary.LittleEndian.Uint32(plain[20:24])
	if recordCount == 0 {
		return nil, index, fmt.Errorf("%w: empty frame", ErrCorrupt)
	}
	if uint64(FrameHeaderSize)+uint64(recordBytes) != uint64(len(plain)) {
		return nil, index, fmt.Errorf("%w: frame records length got %d want %d", ErrCorrupt, len(plain)-FrameHeaderSize, recordBytes)
	}
	storedRecordsHash := plain[56:88]
	actualRecordsHash := sha256.Sum256(plain[FrameHeaderSize:])
	if string(storedRecordsHash) != string(actualRecordsHash[:]) {
		return nil, index, fmt.Errorf("%w: frame records SHA-256 mismatch", ErrCorrupt)
	}

	records := make([]Envelope, 0, recordCount)
	for cursor := FrameHeaderSize; cursor < len(plain); {
		record, size, err := decodeRecord(plain[cursor:])
		if err != nil {
			return nil, index, fmt.Errorf("record %d: %w", len(records), err)
		}
		records = append(records, record)
		cursor += size
	}
	if len(records) != int(recordCount) {
		return nil, index, fmt.Errorf("%w: frame record count got %d want %d", ErrCorrupt, len(records), recordCount)
	}
	firstOrdinal := binary.LittleEndian.Uint64(plain[24:32])
	lastOrdinal := binary.LittleEndian.Uint64(plain[32:40])
	firstReceived := int64(binary.LittleEndian.Uint64(plain[40:48]))
	lastReceived := int64(binary.LittleEndian.Uint64(plain[48:56]))
	if records[0].ArrivalOrdinal != firstOrdinal || records[len(records)-1].ArrivalOrdinal != lastOrdinal ||
		records[0].ReceivedWallTimeNS != firstReceived || records[len(records)-1].ReceivedWallTimeNS != lastReceived {
		return nil, index, fmt.Errorf("%w: frame bounds disagree with records", ErrCorrupt)
	}
	index = FrameIndex{
		Ordinal:            ordinal,
		CompressedOffset:   compressedOffset,
		CompressedBytes:    compressedBytes,
		UncompressedBytes:  uint64(len(plain)),
		RecordCount:        recordCount,
		FirstOrdinal:       firstOrdinal,
		LastOrdinal:        lastOrdinal,
		FirstReceivedAtNS:  firstReceived,
		LastReceivedAtNS:   lastReceived,
		UncompressedSHA256: sha256.Sum256(plain),
	}
	return records, index, nil
}

func validateCompressedAgainstIndex(compressed []byte, offset uint64, expected FrameIndex) error {
	if offset != expected.CompressedOffset {
		return fmt.Errorf("%w: compressed offset got %d want %d", ErrCorrupt, offset, expected.CompressedOffset)
	}
	if uint64(len(compressed)) != expected.CompressedBytes {
		return fmt.Errorf("%w: compressed frame length got %d want %d", ErrCorrupt, len(compressed), expected.CompressedBytes)
	}
	if hash := sha256.Sum256(compressed); hash != expected.CompressedSHA256 {
		return fmt.Errorf("%w: compressed frame SHA-256 mismatch", ErrCorrupt)
	}
	return nil
}

func compareFrameIndex(got, want FrameIndex) error {
	got.CompressedSHA256 = want.CompressedSHA256
	if got != want {
		return fmt.Errorf("%w: frame index mismatch: got %+v want %+v", ErrCorrupt, got, want)
	}
	return nil
}

func validateManifestSummary(data []byte, result DecodeResult, uncompressedHash [32]byte, expected Manifest) error {
	if len(result.Records) > 0 {
		if err := validateSegmentRecords(result.Records); err != nil {
			return fmt.Errorf("%w: decoded segment boundary/order: %v", ErrCorrupt, err)
		}
	}
	var recordCount, uncompressedBytes uint64
	for _, frame := range result.Frames {
		recordCount += uint64(frame.RecordCount)
		uncompressedBytes += frame.UncompressedBytes
	}
	if expected.RecordCount != recordCount {
		return fmt.Errorf("%w: manifest record count got %d want %d", ErrCorrupt, expected.RecordCount, recordCount)
	}
	if expected.UncompressedBytes != uncompressedBytes {
		return fmt.Errorf("%w: manifest uncompressed bytes got %d want %d", ErrCorrupt, expected.UncompressedBytes, uncompressedBytes)
	}
	if expected.CompressedBytes != uint64(len(data)) {
		return fmt.Errorf("%w: manifest compressed bytes got %d want %d", ErrCorrupt, expected.CompressedBytes, len(data))
	}
	if expected.UncompressedSHA256 != uncompressedHash {
		return fmt.Errorf("%w: manifest uncompressed SHA-256 mismatch", ErrCorrupt)
	}
	if compressedHash := sha256.Sum256(data); expected.CompressedSHA256 != compressedHash {
		return fmt.Errorf("%w: manifest compressed SHA-256 mismatch", ErrCorrupt)
	}
	var firstOrdinal, lastOrdinal uint64
	var firstReceived, lastReceived int64
	if len(result.Records) > 0 {
		firstOrdinal = result.Records[0].ArrivalOrdinal
		lastOrdinal = result.Records[len(result.Records)-1].ArrivalOrdinal
		firstReceived = result.Records[0].ReceivedWallTimeNS
		lastReceived = result.Records[len(result.Records)-1].ReceivedWallTimeNS
	}
	if expected.FirstOrdinal != firstOrdinal || expected.LastOrdinal != lastOrdinal {
		return fmt.Errorf("%w: manifest ordinal bounds got %d-%d want %d-%d", ErrCorrupt, expected.FirstOrdinal, expected.LastOrdinal, firstOrdinal, lastOrdinal)
	}
	if expected.FirstReceivedAtNS != firstReceived || expected.LastReceivedAtNS != lastReceived {
		return fmt.Errorf("%w: manifest receive-time bounds got %d-%d want %d-%d", ErrCorrupt, expected.FirstReceivedAtNS, expected.LastReceivedAtNS, firstReceived, lastReceived)
	}
	if err := validateManifestFrameTarget(result.Records, result.Frames, int(expected.FrameBytes)); err != nil {
		return err
	}
	return nil
}

func validateManifestFrameTarget(records []Envelope, frames []FrameIndex, target int) error {
	recordSizes := make([]int, len(records))
	for i, record := range records {
		encoded, err := encodeRecord(record)
		if err != nil {
			return fmt.Errorf("%w: re-encode manifest record %d: %v", ErrCorrupt, i, err)
		}
		recordSizes[i] = len(encoded)
	}
	recordOffset := 0
	for i, frame := range frames {
		frameEnd := recordOffset + int(frame.RecordCount)
		if frameEnd > len(records) {
			return fmt.Errorf("%w: frame %d record bound exceeds segment", ErrCorrupt, i)
		}
		calculatedBytes := FrameHeaderSize
		for _, size := range recordSizes[recordOffset:frameEnd] {
			calculatedBytes += size
		}
		if uint64(calculatedBytes) != frame.UncompressedBytes {
			return fmt.Errorf("%w: frame %d uncompressed bound got %d want %d", ErrCorrupt, i, frame.UncompressedBytes, calculatedBytes)
		}
		if calculatedBytes > target && frame.RecordCount != 1 {
			return fmt.Errorf("%w: frame %d exceeds manifest frame bound with multiple records", ErrCorrupt, i)
		}
		if frameEnd < len(records) && calculatedBytes+recordSizes[frameEnd] <= target {
			return fmt.Errorf("%w: frame %d ended before the manifest frame bound required", ErrCorrupt, i)
		}
		recordOffset = frameEnd
	}
	if recordOffset != len(records) {
		return fmt.Errorf("%w: manifest frames cover %d of %d records", ErrCorrupt, recordOffset, len(records))
	}
	return nil
}

// zstdFrameLength returns the exact byte length of one standard Zstandard
// frame. It deliberately rejects skippable and reserved frame forms so the v1
// segment grammar has one interoperable representation.
func zstdFrameLength(src []byte) (int, error) {
	if len(src) < 5 {
		return 0, fmt.Errorf("%w: zstd frame header", ErrTruncated)
	}
	if binary.LittleEndian.Uint32(src[:4]) != 0xfd2fb528 {
		return 0, fmt.Errorf("%w: invalid zstd magic", ErrCorrupt)
	}
	descriptor := src[4]
	if descriptor&(1<<4|1<<3) != 0 {
		return 0, fmt.Errorf("%w: reserved or unused zstd frame descriptor bit", ErrCorrupt)
	}
	checksum := descriptor&(1<<2) != 0
	singleSegment := descriptor&(1<<5) != 0
	if !checksum || !singleSegment || descriptor&3 != 0 {
		return 0, fmt.Errorf("%w: zstd frame is outside the fixed v1 checksum/single-segment/no-dictionary profile", ErrVersion)
	}
	cursor := 5
	fcsFlag := descriptor >> 6
	fcsSize := 0
	switch fcsFlag {
	case 0:
		if singleSegment {
			fcsSize = 1
		}
	case 1:
		fcsSize = 2
	case 2:
		fcsSize = 4
	case 3:
		fcsSize = 8
	}
	cursor += fcsSize
	if cursor > len(src) {
		return 0, fmt.Errorf("%w: zstd frame fields", ErrTruncated)
	}
	for {
		if len(src)-cursor < 3 {
			return 0, fmt.Errorf("%w: zstd block header", ErrTruncated)
		}
		header := uint32(src[cursor]) | uint32(src[cursor+1])<<8 | uint32(src[cursor+2])<<16
		cursor += 3
		last := header&1 != 0
		blockType := (header >> 1) & 3
		blockSize := int(header >> 3)
		switch blockType {
		case 0, 2:
		case 1:
			blockSize = 1
		case 3:
			return 0, fmt.Errorf("%w: reserved zstd block type", ErrCorrupt)
		}
		if blockSize > len(src)-cursor {
			return 0, fmt.Errorf("%w: zstd block needs %d bytes, got %d", ErrTruncated, blockSize, len(src)-cursor)
		}
		cursor += blockSize
		if last {
			break
		}
	}
	if checksum {
		if len(src)-cursor < 4 {
			return 0, fmt.Errorf("%w: zstd checksum", ErrTruncated)
		}
		cursor += 4
	}
	return cursor, nil
}

func IsDamage(err error) bool {
	return errors.Is(err, ErrCorrupt) || errors.Is(err, ErrTruncated) || errors.Is(err, ErrPayloadHash)
}
