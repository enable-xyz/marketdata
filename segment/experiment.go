package segment

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"runtime"
	"time"

	"github.com/elastic/go-sysinfo"
	"github.com/elastic/go-sysinfo/types"
)

type X1Config struct {
	Consent       string
	CorpusBytes   int64
	FrameSizes    []int
	Concurrencies []int
	Seed          uint64
}

type X1Report struct {
	ExperimentID         string         `json:"experiment_id"`
	ObservedMeasurements bool           `json:"observed_measurements"`
	CorpusBytes          int64          `json:"corpus_bytes"`
	Seed                 uint64         `json:"seed"`
	StartedAt            time.Time      `json:"started_at"`
	CompletedAt          time.Time      `json:"completed_at"`
	GOOS                 string         `json:"goos"`
	GOARCH               string         `json:"goarch"`
	LogicalCPUs          int            `json:"logical_cpus"`
	FrozenDefaults       DecisionRecord `json:"frozen_defaults"`
	Runs                 []X1RunReport  `json:"runs"`
}

type X1RunReport struct {
	FrameBytes                  int        `json:"frame_bytes"`
	Concurrency                 int        `json:"concurrency"`
	PayloadBytes                int64      `json:"payload_bytes"`
	RecordCount                 uint64     `json:"record_count"`
	FrameCount                  uint64     `json:"frame_count"`
	UncompressedBytes           int64      `json:"uncompressed_bytes"`
	CompressedBytes             int64      `json:"compressed_bytes"`
	CompressionRatio            float64    `json:"compression_ratio"`
	RecordsPerSecond            float64    `json:"records_per_second"`
	PayloadBytesPerSecond       float64    `json:"payload_bytes_per_second"`
	TotalWallTimeNS             int64      `json:"total_wall_time_ns"`
	CorpusWallTimeNS            int64      `json:"corpus_wall_time_ns"`
	FramingWallTimeNS           int64      `json:"framing_wall_time_ns"`
	CompressionWallTimeNS       int64      `json:"compression_wall_time_ns"`
	CorruptionProbeWallTimeNS   int64      `json:"corruption_probe_wall_time_ns"`
	FramingCPUNS                int64      `json:"framing_cpu_ns"`
	CompressionCPUNS            int64      `json:"compression_cpu_ns"`
	FramingCPUShare             float64    `json:"framing_cpu_share"`
	CompressionCPUShare         float64    `json:"compression_cpu_share"`
	FramingCPUIncludesIntegrity bool       `json:"framing_cpu_includes_integrity"`
	CRC32CWallTimeNS            int64      `json:"crc32c_wall_time_ns"`
	PayloadSHA256WallTimeNS     int64      `json:"payload_sha256_wall_time_ns"`
	FrameSHA256WallTimeNS       int64      `json:"frame_sha256_wall_time_ns"`
	SegmentSHA256WallTimeNS     int64      `json:"segment_sha256_wall_time_ns"`
	SegmentCount                uint64     `json:"segment_count"`
	SegmentSHA256               [][32]byte `json:"segment_sha256"`
	LogicalCorpusSHA256         [32]byte   `json:"logical_corpus_sha256"`
	CompressedCorpusSHA256      [32]byte   `json:"compressed_corpus_sha256"`
	BaselineRSSBytes            uint64     `json:"baseline_rss_bytes"`
	PeakRSSBytes                uint64     `json:"peak_rss_bytes"`
	PeakRSSDeltaBytes           uint64     `json:"peak_rss_delta_bytes"`
	TruncationsInjected         int        `json:"truncations_injected"`
	TruncationsDetected         int        `json:"truncations_detected"`
	CorruptionsInjected         int        `json:"corruptions_injected"`
	CorruptionsDetected         int        `json:"corruptions_detected"`
	PrefixRecoveriesInjected    int        `json:"prefix_recoveries_injected"`
	PrefixRecoveriesDetected    int        `json:"prefix_recoveries_detected"`
}

// RunFullX1 performs real work and writes one report only after every fixed run
// completes. Its exact consent, 10 GiB corpus, candidates, and destination
// writer are all mandatory, so ordinary tests and callers cannot start it.
func RunFullX1(ctx context.Context, config X1Config, reportWriter io.Writer) (X1Report, error) {
	if err := validateX1Config(config, reportWriter); err != nil {
		return X1Report{}, err
	}
	process, err := sysinfo.Self()
	if err != nil {
		return X1Report{}, fmt.Errorf("segment: inspect current process for X1: %w", err)
	}
	report := X1Report{
		ExperimentID:         "ELMD-003-X1-full-v1",
		ObservedMeasurements: true,
		CorpusBytes:          config.CorpusBytes,
		Seed:                 config.Seed,
		StartedAt:            time.Now().UTC(),
		GOOS:                 runtime.GOOS,
		GOARCH:               runtime.GOARCH,
		LogicalCPUs:          runtime.NumCPU(),
		FrozenDefaults:       FrozenDecision(),
		Runs:                 make([]X1RunReport, 0, len(config.FrameSizes)*len(config.Concurrencies)),
	}
	for _, frameBytes := range config.FrameSizes {
		for _, concurrency := range config.Concurrencies {
			run, runErr := runX1Candidate(ctx, process, config, frameBytes, concurrency)
			if runErr != nil {
				return X1Report{}, runErr
			}
			report.Runs = append(report.Runs, run)
		}
	}
	if err := validateX1Determinism(report.Runs); err != nil {
		return X1Report{}, err
	}
	report.CompletedAt = time.Now().UTC()
	encoder := json.NewEncoder(reportWriter)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(report); err != nil {
		return X1Report{}, fmt.Errorf("segment: write X1 report: %w", err)
	}
	return report, nil
}

func validateX1Determinism(runs []X1RunReport) error {
	if len(runs) == 0 {
		return errors.New("segment: X1 produced no candidate runs")
	}
	logicalHash := runs[0].LogicalCorpusSHA256
	for i := range runs {
		if runs[i].LogicalCorpusSHA256 != logicalHash {
			return fmt.Errorf("segment: X1 candidates did not consume one fixed logical corpus at run %d", i)
		}
	}
	for i := 0; i < len(runs); i += len(X1Concurrencies) {
		one := runs[i]
		two := runs[i+1]
		if one.FrameBytes != two.FrameBytes || one.PayloadBytes != two.PayloadBytes ||
			one.RecordCount != two.RecordCount || one.FrameCount != two.FrameCount ||
			one.UncompressedBytes != two.UncompressedBytes || one.CompressedBytes != two.CompressedBytes ||
			one.CompressedCorpusSHA256 != two.CompressedCorpusSHA256 ||
			!equalSegmentHashes(one.SegmentSHA256, two.SegmentSHA256) {
			return fmt.Errorf("segment: X1 determinism mismatch for %d-byte frames at concurrency 1/2", one.FrameBytes)
		}
	}
	return nil
}

func equalSegmentHashes(left, right [][32]byte) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

func validateX1Config(config X1Config, writer io.Writer) error {
	if config.Consent != FullX1Consent {
		return fmt.Errorf("segment: full X1 requires explicit consent %q", FullX1Consent)
	}
	if config.CorpusBytes != X1CorpusBytes {
		return fmt.Errorf("segment: full X1 corpus must be exactly %d bytes", X1CorpusBytes)
	}
	if writer == nil {
		return errors.New("segment: full X1 requires an explicit report writer")
	}
	if len(config.FrameSizes) != len(X1FrameSizes) {
		return errors.New("segment: full X1 requires frame sizes 1/4/16 MiB")
	}
	for i, value := range X1FrameSizes {
		if config.FrameSizes[i] != value {
			return errors.New("segment: full X1 requires frame sizes 1/4/16 MiB in order")
		}
	}
	if len(config.Concurrencies) != len(X1Concurrencies) {
		return errors.New("segment: full X1 requires concurrency 1/2")
	}
	for i, value := range X1Concurrencies {
		if config.Concurrencies[i] != value {
			return errors.New("segment: full X1 requires concurrency 1/2 in order")
		}
	}
	return nil
}

func runX1Candidate(ctx context.Context, process types.Process, config X1Config, frameBytes, concurrency int) (X1RunReport, error) {
	baseline, err := process.Memory()
	if err != nil {
		return X1RunReport{}, fmt.Errorf("segment: read baseline RSS: %w", err)
	}
	run := X1RunReport{
		FrameBytes:                  frameBytes,
		Concurrency:                 concurrency,
		PayloadBytes:                config.CorpusBytes,
		BaselineRSSBytes:            baseline.Resident,
		PeakRSSBytes:                baseline.Resident,
		FramingCPUIncludesIntegrity: true,
	}
	started := time.Now()
	framer := newX1CorpusFramer(config.Seed, config.CorpusBytes, frameBytes)
	compressedCorpusHasher := sha256.New()
	segmentHasher := sha256.New()
	var segmentUncompressedBytes int64
	var segmentFrameOrdinal uint32
	finishSegment := func() {
		var sum [32]byte
		copy(sum[:], segmentHasher.Sum(nil))
		run.SegmentSHA256 = append(run.SegmentSHA256, sum)
		run.SegmentCount++
		segmentHasher.Reset()
		segmentUncompressedBytes = 0
	}

	for !framer.Done() {
		if err := ctx.Err(); err != nil {
			return X1RunReport{}, fmt.Errorf("segment: full X1 canceled: %w", err)
		}
		jobs := make([]frameJob, 0, concurrency)
		startsSegment := make([]bool, 0, concurrency)
		projectedSegmentBytes := segmentUncompressedBytes
		projectedFrameOrdinal := segmentFrameOrdinal
		for len(jobs) < concurrency && !framer.Done() {
			job, framingWall, framingCPU, measured, err := framer.NextFrame(process)
			if err != nil {
				return X1RunReport{}, err
			}
			startNewSegment := projectedSegmentBytes > 0 && projectedSegmentBytes+int64(len(job.plain)) > DefaultSegmentBytes
			if startNewSegment {
				projectedSegmentBytes = 0
				projectedFrameOrdinal = 0
			}
			binary.LittleEndian.PutUint32(job.plain[12:16], projectedFrameOrdinal)
			projectedFrameOrdinal++
			projectedSegmentBytes += int64(len(job.plain))
			startsSegment = append(startsSegment, startNewSegment)
			jobs = append(jobs, job)
			run.FramingWallTimeNS += framingWall.Nanoseconds()
			run.FramingCPUNS += framingCPU.Nanoseconds()
			run.PayloadSHA256WallTimeNS += measured.payloadSHA256.Nanoseconds()
			run.CRC32CWallTimeNS += measured.crc32c.Nanoseconds()
			run.FrameSHA256WallTimeNS += measured.frameSHA256.Nanoseconds()
			run.RecordCount += uint64(job.recordCount)
			run.UncompressedBytes += int64(len(job.plain))
			run.FrameCount++
		}
		segmentFrameOrdinal = projectedFrameOrdinal
		compressionStarted := time.Now()
		compressionCPUStarted, err := processCPU(process)
		if err != nil {
			return X1RunReport{}, err
		}
		compressed, err := compressFrames(jobs, concurrency)
		if err != nil {
			return X1RunReport{}, err
		}
		compressionCPUEnded, err := processCPU(process)
		if err != nil {
			return X1RunReport{}, err
		}
		run.CompressionWallTimeNS += time.Since(compressionStarted).Nanoseconds()
		run.CompressionCPUNS += (compressionCPUEnded - compressionCPUStarted).Nanoseconds()
		for i, frame := range compressed {
			frameHashStarted := time.Now()
			_ = sha256.Sum256(frame)
			run.FrameSHA256WallTimeNS += time.Since(frameHashStarted).Nanoseconds()
			if startsSegment[i] {
				finishSegment()
			}
			segmentHashStarted := time.Now()
			_, _ = segmentHasher.Write(frame)
			run.SegmentSHA256WallTimeNS += time.Since(segmentHashStarted).Nanoseconds()
			_, _ = compressedCorpusHasher.Write(frame)
			segmentUncompressedBytes += int64(len(jobs[i].plain))
			run.CompressedBytes += int64(len(frame))
		}
		memory, err := process.Memory()
		if err != nil {
			return X1RunReport{}, fmt.Errorf("segment: sample X1 RSS: %w", err)
		}
		run.PeakRSSBytes = max(run.PeakRSSBytes, memory.Resident)
	}

	if segmentUncompressedBytes > 0 {
		finishSegment()
	}
	copy(run.LogicalCorpusSHA256[:], framer.LogicalHash())
	copy(run.CompressedCorpusSHA256[:], compressedCorpusHasher.Sum(nil))
	run.CorpusWallTimeNS = time.Since(started).Nanoseconds()
	if run.UncompressedBytes != 0 {
		run.CompressionRatio = float64(run.CompressedBytes) / float64(run.UncompressedBytes)
	}
	seconds := float64(run.CorpusWallTimeNS) / float64(time.Second)
	if seconds > 0 {
		run.RecordsPerSecond = float64(run.RecordCount) / seconds
		run.PayloadBytesPerSecond = float64(run.PayloadBytes) / seconds
	}
	totalCPU := run.FramingCPUNS + run.CompressionCPUNS
	if totalCPU > 0 {
		run.FramingCPUShare = float64(run.FramingCPUNS) / float64(totalCPU)
		run.CompressionCPUShare = float64(run.CompressionCPUNS) / float64(totalCPU)
	}
	if run.PeakRSSBytes > run.BaselineRSSBytes {
		run.PeakRSSDeltaBytes = run.PeakRSSBytes - run.BaselineRSSBytes
	}
	damageProbeStarted := time.Now()
	probe, err := experimentDamageProbe(config.Seed, frameBytes, concurrency)
	if err != nil {
		return X1RunReport{}, err
	}
	run.TruncationsInjected = probe.truncationsInjected
	run.TruncationsDetected = probe.truncationsDetected
	run.CorruptionsInjected = probe.corruptionsInjected
	run.CorruptionsDetected = probe.corruptionsDetected
	run.PrefixRecoveriesInjected = probe.prefixRecoveriesInjected
	run.PrefixRecoveriesDetected = probe.prefixRecoveriesDetected
	run.CorruptionProbeWallTimeNS = time.Since(damageProbeStarted).Nanoseconds()
	run.TotalWallTimeNS = time.Since(started).Nanoseconds()
	return run, nil
}

var x1PayloadPattern = [...]int64{64, 4 << 10, 64 << 10, 1 << 20, MaxPayloadBytes}

type x1LogicalCorpus struct {
	seed        uint64
	remaining   int64
	recordIndex uint64
	dataIndex   uint64
}

func (c *x1LogicalCorpus) Next() (Envelope, bool, error) {
	if c.remaining == 0 {
		return Envelope{}, false, nil
	}
	family := RepresentativeFamilies[int(c.recordIndex)%len(RepresentativeFamilies)]
	payloadBytes := int64(0)
	if family != CorpusReconnectControl {
		payloadBytes = min(x1PayloadPattern[int(c.dataIndex)%len(x1PayloadPattern)], c.remaining)
		c.dataIndex++
	}
	corpus := &SyntheticCorpus{Family: family, Seed: c.seed, next: c.recordIndex}
	record, err := corpus.Next(int(payloadBytes))
	if err != nil {
		return Envelope{}, false, err
	}
	normalizeX1Record(&record, c.seed, family)
	c.recordIndex++
	c.remaining -= payloadBytes
	return record, true, nil
}

type x1PreparedRecord struct {
	record  Envelope
	encoded []byte
}

type x1CorpusFramer struct {
	corpus        x1LogicalCorpus
	frameBytes    int
	pending       *x1PreparedRecord
	logicalHasher hash.Hash
}

func newX1CorpusFramer(seed uint64, corpusBytes int64, frameBytes int) *x1CorpusFramer {
	return &x1CorpusFramer{
		corpus:        x1LogicalCorpus{seed: seed, remaining: corpusBytes},
		frameBytes:    frameBytes,
		logicalHasher: sha256.New(),
	}
}

func (f *x1CorpusFramer) Done() bool {
	return f.corpus.remaining == 0 && f.pending == nil
}

func (f *x1CorpusFramer) LogicalHash() []byte {
	return f.logicalHasher.Sum(nil)
}

func (f *x1CorpusFramer) NextFrame(process types.Process) (frameJob, time.Duration, time.Duration, recordMeasurements, error) {
	cpuStarted, err := processCPU(process)
	if err != nil {
		return frameJob{}, 0, 0, recordMeasurements{}, err
	}
	wallStarted := time.Now()
	var measured recordMeasurements
	var records []Envelope
	var encodedRecords [][]byte
	recordBytes := 0
	for {
		if f.pending == nil {
			record, ok, err := f.corpus.Next()
			if err != nil {
				return frameJob{}, 0, 0, recordMeasurements{}, err
			}
			if !ok {
				break
			}
			encoded, err := encodeRecordMeasured(record, &measured)
			if err != nil {
				return frameJob{}, 0, 0, recordMeasurements{}, err
			}
			hashStarted := time.Now()
			_, _ = f.logicalHasher.Write(encoded)
			measured.frameSHA256 += time.Since(hashStarted)
			f.pending = &x1PreparedRecord{record: record, encoded: encoded}
		}
		if len(encodedRecords) > 0 && FrameHeaderSize+recordBytes+len(f.pending.encoded) > f.frameBytes {
			break
		}
		records = append(records, f.pending.record)
		encodedRecords = append(encodedRecords, f.pending.encoded)
		recordBytes += len(f.pending.encoded)
		f.pending = nil
	}
	if len(records) == 0 {
		return frameJob{}, 0, 0, recordMeasurements{}, errors.New("segment: X1 framer produced an empty frame")
	}
	var frameHashDuration time.Duration
	job := makeFrameJobMeasured(0, encodedRecords, records, &frameHashDuration)
	measured.frameSHA256 += frameHashDuration
	wall := time.Since(wallStarted)
	cpuEnded, err := processCPU(process)
	if err != nil {
		return frameJob{}, 0, 0, recordMeasurements{}, err
	}
	return job, wall, cpuEnded - cpuStarted, measured, nil
}

func normalizeX1Record(record *Envelope, seed uint64, family CorpusFamily) {
	record.SourceID = "synthetic-x1"
	record.ChannelOrEndpoint = "representative-v1"
	record.ConnectionEpoch = OptionalEpoch{Value: syntheticEpoch(seed, 0, 0x58), Valid: true}
	record.PollCycleID = OptionalEpoch{}
	record.Extensions = []byte{byte(FormatVersion), byte(family)}
}

func processCPU(process types.Process) (time.Duration, error) {
	cpu, err := process.CPUTime()
	if err != nil {
		return 0, fmt.Errorf("segment: sample process CPU: %w", err)
	}
	return cpu.User + cpu.System, nil
}

type damageProbeResult struct {
	truncationsInjected      int
	truncationsDetected      int
	corruptionsInjected      int
	corruptionsDetected      int
	prefixRecoveriesInjected int
	prefixRecoveriesDetected int
}

func experimentDamageProbe(seed uint64, frameBytes, concurrency int) (damageProbeResult, error) {
	probePayloadBytes := int64(MaxPayloadBytes) + int64(frameBytes)*3 + 1
	records, err := x1LogicalRecords(seed, probePayloadBytes)
	if err != nil {
		return damageProbeResult{}, err
	}
	encoded, err := Encode(records, EncodeOptions{FrameBytes: frameBytes, Concurrency: concurrency})
	if err != nil {
		return damageProbeResult{}, err
	}
	return probeEncodedCandidate(encoded, seed)
}

func probeEncodedCandidate(encoded EncodedSegment, seed uint64) (damageProbeResult, error) {
	if len(encoded.Manifest.Frames) < 3 {
		return damageProbeResult{}, fmt.Errorf("segment: candidate probe produced %d frames, want at least 3", len(encoded.Manifest.Frames))
	}
	var probe damageProbeResult
	checkTruncation := func(cut int, wantFrames int, wantComplete uint64) {
		probe.truncationsInjected++
		probe.prefixRecoveriesInjected++
		result, decodeErr := Decode(encoded.Bytes[:cut], &encoded.Manifest)
		if decodeErr != nil {
			probe.truncationsDetected++
		}
		if decodeErr != nil && len(result.Frames) == wantFrames && result.CompleteBytes == wantComplete {
			probe.prefixRecoveriesDetected++
		}
	}
	checkTruncation(0, 0, 0)
	for i, frame := range encoded.Manifest.Frames {
		if i > 0 {
			checkTruncation(int(frame.CompressedOffset), i, frame.CompressedOffset)
		}
		checkTruncation(int(frame.CompressedOffset+frame.CompressedBytes-1), i, frame.CompressedOffset)
	}

	state := seed | 1
	for trial := range 100 {
		frameIndex := trial % len(encoded.Manifest.Frames)
		frame := encoded.Manifest.Frames[frameIndex]
		state ^= state >> 12
		state ^= state << 25
		state ^= state >> 27
		interiorBytes := max(uint64(1), frame.CompressedBytes-2)
		position := frame.CompressedOffset + 1 + (state*0x2545f4914f6cdd1d)%interiorBytes
		damaged := append([]byte(nil), encoded.Bytes...)
		damaged[position] ^= byte(1 + trial%255)
		probe.corruptionsInjected++
		probe.prefixRecoveriesInjected++
		result, decodeErr := Decode(damaged, &encoded.Manifest)
		if decodeErr != nil {
			probe.corruptionsDetected++
		}
		if decodeErr != nil && len(result.Frames) == frameIndex && result.CompleteBytes == frame.CompressedOffset {
			probe.prefixRecoveriesDetected++
		}
	}
	return probe, nil
}

func x1LogicalRecords(seed uint64, payloadBytes int64) ([]Envelope, error) {
	corpus := x1LogicalCorpus{seed: seed, remaining: payloadBytes}
	records := make([]Envelope, 0, int(payloadBytes/(1<<20))+8)
	for corpus.remaining > 0 {
		record, ok, err := corpus.Next()
		if err != nil {
			return nil, err
		}
		if !ok {
			break
		}
		records = append(records, record)
	}
	return records, nil
}
