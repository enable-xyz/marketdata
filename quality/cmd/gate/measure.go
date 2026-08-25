package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"runtime/debug"
	"slices"
	"time"

	"github.com/elastic/go-sysinfo"
	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/dataset"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/quality"
	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/segment"
)

var errUnsupportedWorkloadStep = errors.New("unsupported workload step")

type monotonicClock interface {
	NowNS() int64
}

type realMonotonicClock struct {
	origin time.Time
}

func newRealMonotonicClock() realMonotonicClock {
	return realMonotonicClock{origin: time.Now()}
}

func (c realMonotonicClock) NowNS() int64 {
	return time.Since(c.origin).Nanoseconds()
}

type rssSampler interface {
	RSSBytes() (uint64, error)
}

type rssSamplerFunc func() (uint64, error)

func (f rssSamplerFunc) RSSBytes() (uint64, error) { return f() }

func newProcessRSSSampler() (rssSampler, error) {
	process, err := sysinfo.Self()
	if err != nil {
		return nil, fmt.Errorf("inspect current process: %w", err)
	}
	return rssSamplerFunc(func() (uint64, error) {
		memory, err := process.Memory()
		if err != nil {
			return 0, err
		}
		return memory.Resident, nil
	}), nil
}

type workloadMode uint8

const (
	modeNative workloadMode = iota + 1
	modeNormalized
	modeTelemetryBlackhole
)

type workSample struct {
	Expected  uint64
	Committed uint64
	Bytes     uint64
	Digest    [sha256.Size]byte
}

type workloadProcessor interface {
	ObjectIndexes(workloadMode) ([]int, error)
	Process(context.Context, int, workloadMode, func() error) (workSample, error)
	Corruption(context.Context) (corruptionEvidence, error)
}

type runEvidence struct {
	DurationNS       int64    `json:"duration_ns"`
	ExpectedRecords  uint64   `json:"expected_records"`
	CommittedRecords uint64   `json:"committed_records"`
	RawBytes         uint64   `json:"raw_bytes"`
	RawSHA256        string   `json:"raw_sha256"`
	Iterations       uint64   `json:"iterations"`
	RSSSamples       []uint64 `json:"rss_samples"`
	PeakRSSBytes     uint64   `json:"peak_rss_bytes"`
}

type memoryEvidence struct {
	QueueBoundBytes     uint64   `json:"queue_bound_bytes"`
	FrameBoundBytes     uint64   `json:"frame_bound_bytes"`
	RowGroupBoundBytes  uint64   `json:"row_group_bound_bytes"`
	PeakRSSBytes        uint64   `json:"peak_rss_bytes"`
	Plateaued           bool     `json:"plateaued"`
	UnboundedGrowth     bool     `json:"unbounded_growth"`
	SustainedRSSSamples []uint64 `json:"sustained_rss_samples"`
}

type replayEvidence struct {
	Native     runEvidence `json:"native"`
	Normalized runEvidence `json:"normalized"`
}

type corruptionCase struct {
	ObjectID string `json:"object_id"`
	Kind     string `json:"kind"`
	Detected bool   `json:"detected"`
}

type corruptionEvidence struct {
	Cases []corruptionCase `json:"cases"`
}

type observationEvidence struct {
	Sustained  runEvidence        `json:"sustained"`
	Burst      runEvidence        `json:"burst"`
	Memory     memoryEvidence     `json:"memory"`
	Replay     replayEvidence     `json:"replay"`
	Telemetry  runEvidence        `json:"telemetry_blackhole"`
	Corruption corruptionEvidence `json:"corruption"`
}

type observationArtifact struct {
	Format                 string                          `json:"format"`
	ManifestSHA256         string                          `json:"manifest_sha256"`
	HardwareManifestSHA256 string                          `json:"hardware_manifest_sha256"`
	WorkloadManifestSHA256 string                          `json:"workload_manifest_sha256"`
	CorpusManifestSHA256   string                          `json:"corpus_manifest_sha256"`
	Performance            quality.PerformanceObservations `json:"performance"`
	Evidence               observationEvidence             `json:"evidence"`
	BodySHA256             string                          `json:"body_sha256"`
}

type measureConfig struct {
	SustainedDurationNS int64
	BurstDurationNS     int64
	EnforceGateMinimums bool
	MaximumIterations   uint64
}

type measurementEngine struct {
	clock     monotonicClock
	rss       rssSampler
	processor workloadProcessor
	rssPolicy rssPolicy
}

func runMeasurement(ctx context.Context, manifest loadedManifest, config measureConfig, clock monotonicClock, rss rssSampler) (observationArtifact, error) {
	if config.SustainedDurationNS <= 0 || config.SustainedDurationNS > quality.MaxReleaseGateDurationNS || config.BurstDurationNS <= 0 || config.BurstDurationNS > quality.MaxReleaseGateDurationNS {
		return observationArtifact{}, errors.New("measurement durations are invalid")
	}
	if config.EnforceGateMinimums && config.SustainedDurationNS < quality.MinimumSustainedDurationNS {
		return observationArtifact{}, fmt.Errorf("sustained duration %s is shorter than the release-gate minimum %s", time.Duration(config.SustainedDurationNS), time.Duration(quality.MinimumSustainedDurationNS))
	}
	if config.EnforceGateMinimums && config.BurstDurationNS < quality.MinimumBurstDurationNS {
		return observationArtifact{}, fmt.Errorf("burst duration %s is shorter than the release-gate minimum %s", time.Duration(config.BurstDurationNS), time.Duration(quality.MinimumBurstDurationNS))
	}
	if err := preflightRepositoryNormalizedCorpus(ctx, manifest); err != nil {
		return observationArtifact{}, fmt.Errorf("fixed corpus preflight: %w", err)
	}
	// The preflight processor is now unreachable. Force its heap back to the
	// operating system before constructing any state whose process RSS is timed.
	debug.FreeOSMemory()
	processor, err := newRepositoryProcessor(manifest)
	if err != nil {
		return observationArtifact{}, err
	}
	engine := measurementEngine{clock: clock, rss: rss, processor: processor, rssPolicy: manifest.value.RSS}
	sustained, err := engine.runFor(ctx, config.SustainedDurationNS, modeNative, config.MaximumIterations)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("sustained measurement: %w", err)
	}
	burst, err := engine.runFor(ctx, config.BurstDurationNS, modeNative, config.MaximumIterations)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("burst measurement: %w", err)
	}
	native, err := engine.runFor(ctx, manifest.value.Durations.ReplayNS, modeNative, config.MaximumIterations)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("native replay measurement: %w", err)
	}
	normalized, err := engine.runFor(ctx, manifest.value.Durations.NormalizedNS, modeNormalized, config.MaximumIterations)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("normalized pipeline measurement: %w", err)
	}
	telemetry, err := engine.runFor(ctx, manifest.value.Durations.TelemetryNS, modeTelemetryBlackhole, config.MaximumIterations)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("telemetry blackhole measurement: %w", err)
	}
	corruption, err := processor.Corruption(ctx)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("corruption measurement: %w", err)
	}
	return buildObservation(manifest, sustained, burst, native, normalized, telemetry, corruption)
}

func preflightRepositoryNormalizedCorpus(ctx context.Context, manifest loadedManifest) error {
	processor, err := newRepositoryProcessor(manifest)
	if err != nil {
		return err
	}
	return preflightNormalizedCorpus(ctx, processor)
}

func preflightNormalizedCorpus(ctx context.Context, processor workloadProcessor) error {
	indexes, err := processor.ObjectIndexes(modeNormalized)
	if err != nil {
		return err
	}
	for _, index := range indexes {
		sample, err := processor.Process(ctx, index, modeNormalized, nil)
		if err != nil {
			return fmt.Errorf("normalized corpus object %d: %w", index, err)
		}
		if sample.Expected == 0 || sample.Committed != sample.Expected {
			return fmt.Errorf("normalized corpus object %d committed %d of %d records", index, sample.Committed, sample.Expected)
		}
	}
	return nil
}

func (e measurementEngine) runFor(ctx context.Context, durationNS int64, mode workloadMode, maximumIterations uint64) (runEvidence, error) {
	indexes, err := e.processor.ObjectIndexes(mode)
	if err != nil {
		return runEvidence{}, err
	}
	if len(indexes) == 0 {
		return runEvidence{}, fmt.Errorf("%w: mode %d has no fixed-corpus objects", errUnsupportedWorkloadStep, mode)
	}
	start := e.clock.NowNS()
	if start < 0 {
		return runEvidence{}, errors.New("monotonic clock returned a negative value")
	}
	baseline, err := e.rss.RSSBytes()
	if err != nil {
		return runEvidence{}, fmt.Errorf("sample baseline RSS: %w", err)
	}
	evidence := runEvidence{RSSSamples: []uint64{baseline}, PeakRSSBytes: baseline}
	hasher := sha256.New()
	previous := start
	nextSample := start + e.rssPolicy.SampleIntervalNS
	appendRSS := func(at int64) error {
		if len(evidence.RSSSamples) >= e.rssPolicy.MaximumSamples {
			return errors.New("RSS sample bound exhausted before measurement completed")
		}
		resident, err := e.rss.RSSBytes()
		if err != nil {
			return fmt.Errorf("sample RSS: %w", err)
		}
		evidence.RSSSamples = append(evidence.RSSSamples, resident)
		evidence.PeakRSSBytes = max(evidence.PeakRSSBytes, resident)
		nextSample = at + e.rssPolicy.SampleIntervalNS
		return nil
	}
	observeRSS := func() error {
		now := e.clock.NowNS()
		if now < nextSample {
			return nil
		}
		return appendRSS(now)
	}
	for {
		if err := ctx.Err(); err != nil {
			return runEvidence{}, err
		}
		if maximumIterations > 0 && evidence.Iterations >= maximumIterations {
			return runEvidence{}, fmt.Errorf("iteration budget exhausted after %d ns, before requested %d ns", previous-start, durationNS)
		}
		objectIndex := indexes[evidence.Iterations%uint64(len(indexes))]
		sample, err := e.processor.Process(ctx, objectIndex, mode, observeRSS)
		if err != nil {
			return runEvidence{}, err
		}
		if sample.Expected == 0 || sample.Committed > sample.Expected {
			return runEvidence{}, errors.New("workload returned invalid raw accounting")
		}
		if err := addRunSample(&evidence, sample); err != nil {
			return runEvidence{}, err
		}
		writeWorkDigest(hasher, uint64(objectIndex), sample)
		evidence.Iterations++
		now := e.clock.NowNS()
		if now <= previous {
			return runEvidence{}, errors.New("monotonic clock did not advance after a workload step")
		}
		previous = now
		if now >= nextSample || now-start >= durationNS {
			if err := appendRSS(now); err != nil {
				return runEvidence{}, err
			}
		}
		if now-start >= durationNS {
			evidence.DurationNS = now - start
			break
		}
	}
	evidence.RawSHA256 = hex.EncodeToString(hasher.Sum(nil))
	return evidence, nil
}

func addRunSample(evidence *runEvidence, sample workSample) error {
	var err error
	evidence.ExpectedRecords, err = checkedSum(evidence.ExpectedRecords, sample.Expected)
	if err != nil || evidence.ExpectedRecords > quality.MaxReleaseGateCount {
		return errors.New("expected record accounting exceeded its bound")
	}
	evidence.CommittedRecords, err = checkedSum(evidence.CommittedRecords, sample.Committed)
	if err != nil || evidence.CommittedRecords > quality.MaxReleaseGateCount {
		return errors.New("committed record accounting exceeded its bound")
	}
	evidence.RawBytes, err = checkedSum(evidence.RawBytes, sample.Bytes)
	if err != nil || evidence.RawBytes > quality.MaxReleaseGateBytes {
		return errors.New("raw byte accounting exceeded its bound")
	}
	return nil
}

func writeWorkDigest(writer hash.Hash, objectIndex uint64, sample workSample) {
	var numbers [24]byte
	binary.BigEndian.PutUint64(numbers[0:8], objectIndex)
	binary.BigEndian.PutUint64(numbers[8:16], sample.Expected)
	binary.BigEndian.PutUint64(numbers[16:24], sample.Committed)
	_, _ = writer.Write(numbers[:])
	_, _ = writer.Write(sample.Digest[:])
}

func buildObservation(manifest loadedManifest, sustained, burst, native, normalized, telemetry runEvidence, corruption corruptionEvidence) (observationArtifact, error) {
	memoryLimit, err := checkedSum(manifest.value.Memory.QueueBoundBytes, manifest.value.Memory.FrameBoundBytes, manifest.value.Memory.RowGroupBoundBytes)
	if err != nil {
		return observationArtifact{}, err
	}
	peak := max(sustained.PeakRSSBytes, burst.PeakRSSBytes, native.PeakRSSBytes, normalized.PeakRSSBytes, telemetry.PeakRSSBytes)
	plateaued, unbounded := classifyRSS(sustained.RSSSamples, manifest.value.RSS.PlateauWindowSamples, manifest.value.RSS.PlateauToleranceBytes)
	memory := memoryEvidence{
		QueueBoundBytes: manifest.value.Memory.QueueBoundBytes, FrameBoundBytes: manifest.value.Memory.FrameBoundBytes,
		RowGroupBoundBytes: manifest.value.Memory.RowGroupBoundBytes, PeakRSSBytes: peak,
		Plateaued: plateaued, UnboundedGrowth: unbounded, SustainedRSSSamples: slices.Clone(sustained.RSSSamples),
	}
	evidence := observationEvidence{Sustained: sustained, Burst: burst, Memory: memory, Replay: replayEvidence{Native: native, Normalized: normalized}, Telemetry: telemetry, Corruption: corruption}
	sustainedRate, err := observedRates(sustained)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("sustained rates: %w", err)
	}
	burstRate, err := observedRates(burst)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("burst rates: %w", err)
	}
	nativeRate, err := observedRates(native)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("native replay rates: %w", err)
	}
	normalizedRate, err := observedRates(normalized)
	if err != nil {
		return observationArtifact{}, fmt.Errorf("normalized rates: %w", err)
	}
	sustainedDigest, err := canonicalDigest(sustained)
	if err != nil {
		return observationArtifact{}, err
	}
	burstDigest, err := canonicalDigest(burst)
	if err != nil {
		return observationArtifact{}, err
	}
	memoryDigest, err := canonicalDigest(memory)
	if err != nil {
		return observationArtifact{}, err
	}
	replayDigest, err := canonicalDigest(evidence.Replay)
	if err != nil {
		return observationArtifact{}, err
	}
	telemetryDigest, err := canonicalDigest(telemetry)
	if err != nil {
		return observationArtifact{}, err
	}
	corruptionDigest, err := canonicalDigest(corruption)
	if err != nil {
		return observationArtifact{}, err
	}
	performance := quality.PerformanceObservations{
		Sustained: quality.SustainedLoadObservation{DurationNS: sustained.DurationNS, MessagesPerSecond: sustainedRate.records, BytesPerSecond: sustainedRate.bytes, Raw: rawAccounting(sustained), EvidenceSHA256: sustainedDigest},
		Burst: quality.BurstObservation{
			DurationNS: burst.DurationNS, MessagesPerSecond: burstRate.records, BytesPerSecond: burstRate.bytes,
			DocumentedBackpressurePath: false, BackpressureBeforeBounds: false,
			MemoryLimitExceeded: burst.PeakRSSBytes > memoryLimit, SpoolLimitExceeded: false, QueueLimitExceeded: false,
			Raw: rawAccounting(burst), EvidenceSHA256: burstDigest,
		},
		Memory: quality.MemoryObservation{
			QueueBoundBytes: memory.QueueBoundBytes, FrameBoundBytes: memory.FrameBoundBytes, RowGroupBoundBytes: memory.RowGroupBoundBytes,
			PeakRSSBytes: memory.PeakRSSBytes, Plateaued: memory.Plateaued, UnboundedGrowth: memory.UnboundedGrowth, EvidenceSHA256: memoryDigest,
		},
		Replay: quality.ReplayPerformanceObservation{
			NativeRecordsPerSecond: nativeRate.records, NativeBytesPerSecond: nativeRate.bytes, NativeWorkers: uint32(manifest.value.Replay.Concurrency),
			NormalizedRecordsPerSecond: normalizedRate.records, NormalizedBytesPerSecond: normalizedRate.bytes, NormalizedWorkers: uint32(manifest.value.Replay.Concurrency),
			ParallelismMeasured: false, ParallelismOrderPreserved: false, EvidenceSHA256: replayDigest,
		},
		Telemetry: quality.TelemetryBlackholeObservation{
			Raw: rawAccounting(telemetry), MemoryBoundBytes: manifest.value.Memory.TelemetryBoundBytes,
			PeakMemoryBytes: telemetry.PeakRSSBytes, CaptureStalled: telemetry.CommittedRecords == 0 || telemetry.CommittedRecords != telemetry.ExpectedRecords,
			EvidenceSHA256: telemetryDigest,
		},
		Corruption: quality.CorruptionObservation{
			InjectedCases: uint64(len(corruption.Cases)), DetectedCases: countDetected(corruption.Cases),
			UndetectedCases: uint64(len(corruption.Cases)) - countDetected(corruption.Cases), EvidenceSHA256: corruptionDigest,
		},
	}
	artifact := observationArtifact{
		Format: observationFormat, ManifestSHA256: manifest.sha256,
		HardwareManifestSHA256: manifest.value.Hardware.Value.ManifestSHA256,
		WorkloadManifestSHA256: manifest.value.Workload.Value.ManifestSHA256,
		CorpusManifestSHA256:   manifest.value.FixedCorpus.Value.ManifestSHA256,
		Performance:            performance, Evidence: evidence,
	}
	if err := sealObservation(&artifact); err != nil {
		return observationArtifact{}, err
	}
	return artifact, nil
}

type rates struct{ records, bytes uint64 }

func observedRates(evidence runEvidence) (rates, error) {
	recordRate, err := perSecond(evidence.CommittedRecords, evidence.DurationNS)
	if err != nil {
		return rates{}, err
	}
	byteRate, err := perSecond(evidence.RawBytes, evidence.DurationNS)
	if err != nil {
		return rates{}, err
	}
	return rates{records: recordRate, bytes: byteRate}, nil
}

func perSecond(count uint64, elapsedNS int64) (uint64, error) {
	if elapsedNS <= 0 {
		return 0, errors.New("elapsed time is not positive")
	}
	hi, lo := bits.Mul64(count, uint64(time.Second))
	if hi >= uint64(elapsedNS) {
		return 0, errors.New("rate exceeds uint64")
	}
	quotient, _ := bits.Div64(hi, lo, uint64(elapsedNS))
	if quotient == 0 || quotient > quality.MaxReleaseGateRate {
		return 0, errors.New("observed rate is zero or exceeds the release-gate bound")
	}
	return quotient, nil
}

func rawAccounting(evidence runEvidence) quality.RawAccounting {
	unexplained := uint64(0)
	if evidence.ExpectedRecords >= evidence.CommittedRecords {
		unexplained = evidence.ExpectedRecords - evidence.CommittedRecords
	}
	return quality.RawAccounting{ExpectedRecords: evidence.ExpectedRecords, CommittedRecords: evidence.CommittedRecords, UnexplainedLossRecords: unexplained}
}

func classifyRSS(samples []uint64, window int, tolerance uint64) (bool, bool) {
	if window < 1 || len(samples) < window*2 {
		return false, false
	}
	first := samples[len(samples)-2*window : len(samples)-window]
	second := samples[len(samples)-window:]
	firstMax := slices.Max(first)
	secondMin, secondMax := slices.Min(second), slices.Max(second)
	plateaued := secondMax-secondMin <= tolerance && secondMax <= firstMax+tolerance
	unbounded := secondMin > firstMax+tolerance
	return plateaued, unbounded
}

func countDetected(cases []corruptionCase) uint64 {
	var count uint64
	for _, item := range cases {
		if item.Detected {
			count++
		}
	}
	return count
}

func canonicalDigest(value any) (string, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return "", fmt.Errorf("marshal evidence: %w", err)
	}
	digest := sha256.Sum256(data)
	return hex.EncodeToString(digest[:]), nil
}

func sealObservation(artifact *observationArtifact) error {
	artifact.BodySHA256 = ""
	digest, err := canonicalDigest(*artifact)
	if err != nil {
		return err
	}
	artifact.BodySHA256 = digest
	return nil
}

type repositoryProcessor struct {
	manifest loadedManifest
	runtimes []*normalizationRuntime
}

type normalizationRuntime struct {
	orchestrator *normalize.Orchestrator
	dataset      datasetSpec
}

func newRepositoryProcessor(manifest loadedManifest) (*repositoryProcessor, error) {
	processor := &repositoryProcessor{manifest: manifest, runtimes: make([]*normalizationRuntime, len(manifest.objects))}
	for i := range manifest.objects {
		object := &manifest.objects[i]
		if object.spec.Normalizer == nil {
			continue
		}
		runtime, err := buildNormalizationRuntime(*object)
		if err != nil {
			return nil, fmt.Errorf("corpus object %q: %w", object.spec.ID, err)
		}
		processor.runtimes[i] = runtime
	}
	return processor, nil
}

func (p *repositoryProcessor) ObjectIndexes(mode workloadMode) ([]int, error) {
	indexes := make([]int, 0, len(p.manifest.objects))
	switch mode {
	case modeNative, modeTelemetryBlackhole:
		for index := range p.manifest.objects {
			indexes = append(indexes, index)
		}
	case modeNormalized:
		for index, runtime := range p.runtimes {
			if runtime != nil {
				indexes = append(indexes, index)
			}
		}
		if len(indexes) == 0 {
			return nil, fmt.Errorf("%w: no corpus object declares a real normalizer", errUnsupportedWorkloadStep)
		}
	default:
		return nil, fmt.Errorf("%w: unknown measurement mode %d", errUnsupportedWorkloadStep, mode)
	}
	return indexes, nil
}

func (p *repositoryProcessor) Process(ctx context.Context, index int, mode workloadMode, observeRSS func() error) (workSample, error) {
	if index < 0 || index >= len(p.manifest.objects) {
		return workSample{}, errors.New("corpus object index is out of range")
	}
	object := &p.manifest.objects[index]
	reader := oneObjectReader{key: object.spec.Publication.ObjectKey, data: object.segment}
	var rows []normalize.Row
	hasher := sha256.New()
	var committed, rawBytes, rawOrdinal uint64
	telemetry := blackholeSink{}
	_, err := replay.ReplaySource(ctx, reader, []replay.InputDescriptor{object.descriptor}, p.manifest.value.Replay.config(), func(event replay.Event) error {
		if event.Kind != replay.EventRecord {
			return fmt.Errorf("native replay emitted a discontinuity for fixed corpus object %q", object.spec.ID)
		}
		envelope, err := capture.EnvelopeV1FromSegment(event.Record)
		if err != nil {
			return fmt.Errorf("capture envelope validation: %w", err)
		}
		if mode == modeNormalized {
			runtime := p.runtimes[index]
			if runtime == nil {
				return fmt.Errorf("%w: normalization for corpus object %q", errUnsupportedWorkloadStep, object.spec.ID)
			}
			record, err := normalize.BindRawRecord(envelope, normalize.Hash(object.contentSHA256), rawOrdinal, nil)
			if err != nil {
				return err
			}
			batch, err := runtime.orchestrator.Normalize([]normalize.RawRecord{record})
			if err != nil {
				return err
			}
			if len(batch.Quarantines) != 0 || len(batch.Rows) == 0 {
				return fmt.Errorf("normalized pipeline did not accept corpus object %q record %d", object.spec.ID, rawOrdinal)
			}
			for _, row := range batch.Rows {
				if err := row.Validate(); err != nil {
					return err
				}
				rows = append(rows, row)
			}
		}
		if mode == modeTelemetryBlackhole {
			telemetry.Observe(envelope.RawPayloadSHA256)
		}
		framed, err := capture.MarshalEnvelopeV1(envelope)
		if err != nil {
			return fmt.Errorf("marshal canonical capture envelope: %w", err)
		}
		var frameLength [8]byte
		binary.BigEndian.PutUint64(frameLength[:], uint64(len(framed)))
		_, _ = hasher.Write(frameLength[:])
		_, _ = hasher.Write(framed)
		committed++
		rawOrdinal++
		if uint64(len(envelope.RawPayload)) > quality.MaxReleaseGateBytes-rawBytes {
			return errors.New("raw payload byte count exceeded bound")
		}
		rawBytes += uint64(len(envelope.RawPayload))
		if observeRSS != nil {
			if err := observeRSS(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return workSample{}, err
	}
	if mode == modeTelemetryBlackhole && telemetry.observed != committed {
		return workSample{}, errors.New("telemetry blackhole did not observe every committed record")
	}
	if mode == modeNormalized {
		runtime := p.runtimes[index]
		if runtime == nil || object.spec.Dataset == nil {
			return workSample{}, fmt.Errorf("%w: dataset build for corpus object %q", errUnsupportedWorkloadStep, object.spec.ID)
		}
		if err := buildDatasetBatch(ctx, rows, runtime.dataset); err != nil {
			return workSample{}, err
		}
		if observeRSS != nil {
			if err := observeRSS(); err != nil {
				return workSample{}, err
			}
		}
	}
	var digest [sha256.Size]byte
	copy(digest[:], hasher.Sum(nil))
	return workSample{Expected: object.descriptor.RecordCount(), Committed: committed, Bytes: rawBytes, Digest: digest}, nil
}

func (p *repositoryProcessor) Corruption(ctx context.Context) (corruptionEvidence, error) {
	evidence := corruptionEvidence{Cases: make([]corruptionCase, 0, len(p.manifest.objects)*2)}
	for _, object := range p.manifest.objects {
		if err := ctx.Err(); err != nil {
			return corruptionEvidence{}, err
		}
		if len(object.segment) < 2 {
			return corruptionEvidence{}, fmt.Errorf("corpus object %q is too short for corruption injection", object.spec.ID)
		}
		truncated := slices.Clone(object.segment[:len(object.segment)-1])
		_, err := segment.Decode(truncated, &object.ready.Segment)
		evidence.Cases = append(evidence.Cases, corruptionCase{ObjectID: object.spec.ID, Kind: "truncated", Detected: segment.IsDamage(err)})
		corrupted := slices.Clone(object.segment)
		corrupted[len(corrupted)/2] ^= 0x01
		_, err = segment.Decode(corrupted, &object.ready.Segment)
		evidence.Cases = append(evidence.Cases, corruptionCase{ObjectID: object.spec.ID, Kind: "bit_flip", Detected: segment.IsDamage(err)})
	}
	return evidence, nil
}

type oneObjectReader struct {
	key  string
	data []byte
}

func (r oneObjectReader) Get(_ context.Context, key string) (io.ReadCloser, error) {
	if key != r.key {
		return nil, errors.New("object key is outside the fixed corpus")
	}
	return io.NopCloser(bytes.NewReader(r.data)), nil
}

func buildNormalizationRuntime(object loadedObject) (*normalizationRuntime, error) {
	spec := object.spec.Normalizer
	if spec == nil {
		return nil, fmt.Errorf("%w: missing normalizer", errUnsupportedWorkloadStep)
	}
	if spec.Kind != "binance_spot" {
		return nil, fmt.Errorf("%w: normalizer %q", errUnsupportedWorkloadStep, spec.Kind)
	}
	if object.spec.Dataset == nil {
		return nil, fmt.Errorf("%w: dataset build is not declared", errUnsupportedWorkloadStep)
	}
	var document struct {
		Version     uint16            `json:"version"`
		Instruments []json.RawMessage `json:"instruments"`
	}
	if err := json.Unmarshal(object.snapshot, &document); err != nil {
		return nil, fmt.Errorf("decode catalog snapshot: %w", err)
	}
	snapshotDigest := sha256.Sum256(object.snapshot)
	snapshot := catalog.Snapshot{Version: document.Version, SHA256: snapshotDigest, Bytes: slices.Clone(object.snapshot), InstrumentCount: len(document.Instruments)}
	until := normalize.OptionalInt64{}
	if spec.EffectiveUntilNS != nil {
		until = normalize.OptionalInt64{Valid: true, Value: *spec.EffectiveUntilNS}
	}
	binding, err := binance.NewSpotMapperBinding(normalize.Hash(snapshotDigest), spec.MapperVersion, spec.EffectiveFromNS, until, normalize.TimeResolution(spec.SourceTimeResolution), nil)
	if err != nil {
		return nil, fmt.Errorf("create Binance Spot mapper binding: %w", err)
	}
	orchestrator, err := normalize.NewOrchestrator(snapshot, []normalize.BoundMapper{binding})
	if err != nil {
		return nil, fmt.Errorf("create normalization orchestrator: %w", err)
	}
	if err := validateDatasetSpec(*object.spec.Dataset); err != nil {
		return nil, err
	}
	return &normalizationRuntime{orchestrator: orchestrator, dataset: *object.spec.Dataset}, nil
}

func validateDatasetSpec(spec datasetSpec) error {
	if spec.Root == "" || !filepath.IsAbs(spec.Root) || filepath.Clean(spec.Root) != spec.Root {
		return errors.New("dataset root must be an explicit clean absolute path")
	}
	info, err := os.Lstat(spec.Root)
	if err != nil {
		return fmt.Errorf("inspect dataset root: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("dataset root must be an existing non-symlink directory")
	}
	for name, value := range map[string]string{
		"dataset policy ID": spec.DatasetPolicyID, "replay config ID": spec.ReplayConfigID, "input manifest set ID": spec.InputManifestSetID,
	} {
		if !validSHA256(value) {
			return fmt.Errorf("%s is not a lowercase SHA-256", name)
		}
	}
	return nil
}

func buildDatasetBatch(ctx context.Context, rows []normalize.Row, spec datasetSpec) (err error) {
	if len(rows) == 0 {
		return errors.New("dataset workload has no normalized rows")
	}
	root, err := os.MkdirTemp(spec.Root, "release-gate-")
	if err != nil {
		return fmt.Errorf("create bounded dataset workspace: %w", err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(root); cleanupErr != nil && err == nil {
			err = fmt.Errorf("remove bounded dataset workspace: %w", cleanupErr)
		}
	}()
	policy, _ := decodeNormalizeHash(spec.DatasetPolicyID)
	replayID, _ := decodeNormalizeHash(spec.ReplayConfigID)
	inputs, _ := decodeNormalizeHash(spec.InputManifestSetID)
	options := dataset.DefaultWriterOptions(policy, replayID, inputs)
	options.RowGroupTargetBytes = spec.RowGroupTargetBytes
	options.PageBufferBytes = spec.PageBufferBytes
	options.Dictionary = spec.Dictionary
	options.BloomFilter = spec.BloomFilter
	options.MaxInputRows = spec.MaxInputRows
	options.MaxParquetRows = spec.MaxParquetRows
	source := &dataset.SliceNormalizedSource{Rows: slices.Clone(rows)}
	if _, err := dataset.BuildNormalizedPartition(ctx, root, source, options); err != nil {
		return fmt.Errorf("build normalized dataset partition: %w", err)
	}
	return nil
}

type blackholeSink struct {
	observed uint64
}

func (s *blackholeSink) Observe([sha256.Size]byte) {
	s.observed++
}
