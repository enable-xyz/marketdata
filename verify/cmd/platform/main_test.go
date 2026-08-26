package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/quality"
	"github.com/enable-xyz/marketdata/verify"
)

func TestPublishBuildsValidBundleWithoutFilePaths(t *testing.T) {
	fixture := newPublisherFixture(t)
	fixture.writeInput(t)

	if err := run([]string{"--input", fixture.inputPath, "--output", fixture.outputPath}, ioDiscard{}); err != nil {
		t.Fatalf("run() error = %v", err)
	}
	bundle, err := os.ReadFile(fixture.outputPath)
	if err != nil {
		t.Fatal(err)
	}
	inventory, _, err := loadInventory(fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verify.ValidatePlatformBundle(bundle, fixture.input.PlatformInput.Declarations, inventory); err != nil {
		t.Fatalf("ValidatePlatformBundle() error = %v", err)
	}
	paths := []string{fixture.inputPath, fixture.outputPath}
	for _, artifact := range fixture.input.Artifacts {
		paths = append(paths, artifact.Path)
	}
	for _, path := range paths {
		if bytes.Contains(bundle, []byte(path)) {
			t.Fatalf("bundle contains filesystem path %q", path)
		}
	}
	info, err := os.Stat(fixture.outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("output permissions = %o, want 600", info.Mode().Perm())
	}
}

func TestPublishRejectsTamperedArtifact(t *testing.T) {
	fixture := newPublisherFixture(t)
	fixture.writeInput(t)
	if err := os.WriteFile(fixture.paths["raw"], []byte("tampered"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := run([]string{"--input", fixture.inputPath, "--output", fixture.outputPath}, ioDiscard{})
	if err == nil || !strings.Contains(err.Error(), "SHA-256 mismatch") {
		t.Fatalf("run() error = %v, want SHA-256 mismatch", err)
	}
	assertNotExist(t, fixture.outputPath)
}

func TestPublishRejectsOperationalGateFailures(t *testing.T) {
	t.Run("tuple mismatch", func(t *testing.T) {
		fixture := newPublisherFixture(t)
		fixture.input.PlatformInput.Rows[0].Tuple.Channel = "different-channel"
		fixture.input.PlatformInput.Declarations[0].Tuple.Channel = "different-channel"
		fixture.writeInput(t)

		err := run([]string{"--input", fixture.inputPath, "--output", fixture.outputPath}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "no exact tuple result") {
			t.Fatalf("run() error = %v, want tuple mismatch", err)
		}
		assertNotExist(t, fixture.outputPath)
	})

	t.Run("failing report", func(t *testing.T) {
		fixture := newPublisherFixture(t)
		gate := passingGateBundle(fixture.contract)
		gate.Performance.Sustained.MessagesPerSecond = 1
		report, err := quality.EvaluateReleaseGate(gate)
		if err != nil {
			t.Fatal(err)
		}
		if report.Passed {
			t.Fatal("test gate unexpectedly passed")
		}
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		fixture.replaceGate(t, encoded)
		fixture.writeInput(t)

		err = run([]string{"--input", fixture.inputPath, "--output", fixture.outputPath}, ioDiscard{})
		if err == nil || !strings.Contains(err.Error(), "release gate failed") {
			t.Fatalf("run() error = %v, want failed release gate", err)
		}
		assertNotExist(t, fixture.outputPath)
	})
}

func TestPublishRejectsUnknownJSON(t *testing.T) {
	fixture := newPublisherFixture(t)
	encoded, err := json.Marshal(fixture.input)
	if err != nil {
		t.Fatal(err)
	}
	encoded = bytes.Replace(encoded, []byte("{"), []byte(`{"unknown":true,`), 1)
	if err := os.WriteFile(fixture.inputPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}

	err = run([]string{"--input", fixture.inputPath, "--output", fixture.outputPath}, ioDiscard{})
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("run() error = %v, want unknown field", err)
	}
	assertNotExist(t, fixture.outputPath)
}

func TestPublishRejectsUnusedArtifact(t *testing.T) {
	fixture := newPublisherFixture(t)
	fixture.addArtifact(t, "unused", []byte("unused immutable bytes"), false)
	fixture.writeInput(t)

	err := run([]string{"--input", fixture.inputPath, "--output", fixture.outputPath}, ioDiscard{})
	if err == nil || !strings.Contains(err.Error(), "is unused") {
		t.Fatalf("run() error = %v, want unused artifact", err)
	}
	assertNotExist(t, fixture.outputPath)
}

func TestPublishDoesNotOverwriteOutput(t *testing.T) {
	fixture := newPublisherFixture(t)
	fixture.writeInput(t)
	const sentinel = "existing output"
	if err := os.WriteFile(fixture.outputPath, []byte(sentinel), 0o600); err != nil {
		t.Fatal(err)
	}

	err := run([]string{"--input", fixture.inputPath, "--output", fixture.outputPath}, ioDiscard{})
	if !errors.Is(err, os.ErrExist) {
		t.Fatalf("run() error = %v, want os.ErrExist", err)
	}
	got, err := os.ReadFile(fixture.outputPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != sentinel {
		t.Fatalf("existing output = %q, want %q", got, sentinel)
	}
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) { return len(p), nil }

type publisherFixture struct {
	input      publicationInput
	inputPath  string
	outputPath string
	paths      map[string]string
	contract   quality.ContractIdentity
}

func newPublisherFixture(t *testing.T) *publisherFixture {
	t.Helper()
	root := t.TempDir()
	closedEnd := int64(2_000_000_000)
	tuple := verify.TupleIdentity{
		SourceID:          "source-a",
		APIVersion:        "v1",
		Entitlement:       "public",
		Channel:           "trades",
		DataFamily:        "trade",
		NativeGranularity: "event",
		CoverageStartNS:   1_000_000_000,
		CoverageEndNS:     &closedEnd,
		AdapterVersion:    "adapter-a/v1",
	}
	contract := quality.ContractIdentity{
		ContractID:        "contract-a",
		SourceID:          tuple.SourceID,
		APIVersion:        tuple.APIVersion,
		Entitlement:       tuple.Entitlement,
		ChannelOrEndpoint: tuple.Channel,
		DataFamily:        tuple.DataFamily,
		NativeGranularity: tuple.NativeGranularity,
		VenueFamily:       "venue-a",
		CoverageStartNS:   tuple.CoverageStartNS,
		CoverageEndNS:     closedEnd,
		AdapterVersion:    tuple.AdapterVersion,
		ContractSHA256:    testDigest("contract"),
		AdapterSHA256:     testDigest("adapter"),
		CanaryRequirement: quality.CanaryRequired,
	}
	fixture := &publisherFixture{
		inputPath:  filepath.Join(root, "input.json"),
		outputPath: filepath.Join(root, "bundle.json"),
		paths:      make(map[string]string),
		contract:   contract,
	}

	gateReport, err := quality.EvaluateReleaseGate(passingGateBundle(contract))
	if err != nil {
		t.Fatal(err)
	}
	if !gateReport.Passed {
		t.Fatal("test release gate did not pass")
	}
	gateBytes, err := json.Marshal(gateReport)
	if err != nil {
		t.Fatal(err)
	}
	gateDigest, _ := fixture.addArtifact(t, "gate", gateBytes, true)

	addReference := func(label string, kind verify.EvidenceKind) verify.ArtifactReference {
		digest, locator := fixture.addArtifact(t, label, []byte("immutable "+label+" artifact"), false)
		return verify.ArtifactReference{Kind: kind, SHA256: digest, Locator: locator}
	}
	row := verify.ParityRow{
		Tuple:                       tuple,
		Lifecycle:                   verify.LifecycleSupported,
		Status:                      verify.ParityProved,
		CanaryRequired:              true,
		Limitation:                  "evidence begins at the declared recorder boundary",
		OperationalGateReportSHA256: gateDigest,
		SourceEvidence:              []verify.ArtifactReference{addReference("source", verify.EvidenceSourceContract)},
		FixtureEvidence:             []verify.ArtifactReference{addReference("fixture", verify.EvidenceFixture)},
		RawEvidence:                 []verify.ArtifactReference{addReference("raw", verify.EvidenceRaw)},
		DatasetEvidence:             []verify.ArtifactReference{addReference("dataset", verify.EvidenceDataset)},
		ReplayEvidence:              []verify.ArtifactReference{addReference("replay", verify.EvidenceReplay)},
		CatalogEvidence:             []verify.ArtifactReference{addReference("catalog", verify.EvidenceCatalog)},
	}
	hardwareDigest, _ := fixture.addArtifact(t, "hardware", []byte("immutable hardware identity"), false)
	workloadDigest, _ := fixture.addArtifact(t, "workload", []byte("immutable workload identity"), false)
	x5Digest, _ := fixture.addArtifact(t, "x5", []byte("immutable x5 evidence"), false)
	fixture.input.PlatformInput = verify.PlatformBundleInput{
		BuildIdentity: verify.BuildInfo{
			Version: "publisher-test.v1",
			Commit:  "0123456789abcdef0123456789abcdef01234567",
			Date:    "2026-08-23T02:00:00Z",
		},
		HardwareIdentitySHA256: hardwareDigest,
		WorkloadIdentitySHA256: workloadDigest,
		X5EvidenceSHA256:       x5Digest,
		Declarations: []verify.TupleDeclaration{{
			Tuple: tuple, Lifecycle: verify.LifecycleSupported, CanaryRequired: true,
		}},
		Rows: []verify.ParityRow{row},
	}
	return fixture
}

func (f *publisherFixture) addArtifact(t *testing.T, label string, data []byte, gate bool) (string, string) {
	t.Helper()
	path := filepath.Join(filepath.Dir(f.inputPath), label+".artifact")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	hexDigest := hex.EncodeToString(digest[:])
	locator := "evidence://publisher-test/" + label
	f.input.Artifacts = append(f.input.Artifacts, artifactInput{
		SHA256: hexDigest, Locator: locator, Path: path, OperationalGate: gate,
	})
	f.paths[label] = path
	return hexDigest, locator
}

func (f *publisherFixture) replaceGate(t *testing.T, data []byte) {
	t.Helper()
	if err := os.WriteFile(f.paths["gate"], data, 0o600); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	hexDigest := hex.EncodeToString(digest[:])
	for i := range f.input.Artifacts {
		if f.input.Artifacts[i].OperationalGate {
			f.input.Artifacts[i].SHA256 = hexDigest
		}
	}
	f.input.PlatformInput.Rows[0].OperationalGateReportSHA256 = hexDigest
}

func (f *publisherFixture) writeInput(t *testing.T) {
	t.Helper()
	encoded, err := json.Marshal(f.input)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.inputPath, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
}

func assertNotExist(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("output exists after rejection: %v", err)
	}
}

func passingGateBundle(contract quality.ContractIdentity) quality.ReleaseGateBundle {
	categories := make([]quality.FaultCategoryObservation, 0, len(quality.RequiredFaultCategoryIDs()))
	for _, categoryID := range quality.RequiredFaultCategoryIDs() {
		categories = append(categories, quality.FaultCategoryObservation{
			CategoryID: categoryID, Passed: true, EvidenceSHA256: testDigest("fault/" + string(categoryID)),
		})
	}
	gates := make([]quality.DeterminismObservation, 0, len(quality.RequiredDeterminismGateIDs()))
	for _, gateID := range quality.RequiredDeterminismGateIDs() {
		gates = append(gates, quality.DeterminismObservation{
			GateID: gateID, Passed: true, EvidenceSHA256: testDigest("determinism/" + string(gateID)),
		})
	}
	canaryStart := int64(1_700_000_000_000_000_000)
	return quality.ReleaseGateBundle{
		Hardware: quality.HardwareIdentity{
			ID: "hardware-a", OS: "linux", Architecture: "amd64", CPUModel: "production-equivalent",
			LogicalCPUs: 16, MemoryBytes: 64 << 30, ProductionEquivalent: true, ManifestSHA256: testDigest("hardware"),
		},
		Workload: quality.WorkloadIdentity{
			ID: "workload-a", ManifestSHA256: testDigest("workload"),
			MaxObservedMessagesPerSecond: 100, MaxObservedBytesPerSecond: 1_000,
			AcquisitionRecordsPerSecond: 100, AcquisitionBytesPerSecond: 1_000,
			Contracts: []quality.ContractIdentity{contract},
		},
		Corpus: quality.FixedCorpusIdentity{
			ID: "corpus-a", ManifestSHA256: testDigest("corpus"), VenueFamilies: []string{"venue-a"},
			PayloadClasses:         []quality.PayloadClass{quality.PayloadTiny, quality.PayloadMedian, quality.PayloadMax},
			HighCardinalitySymbols: true, LongBooks: true, SparseTickerUpdates: true,
			Reconnects: true, LongHistories: true,
		},
		Performance: quality.PerformanceObservations{
			Sustained: quality.SustainedLoadObservation{
				DurationNS: quality.MinimumSustainedDurationNS, MessagesPerSecond: 200, BytesPerSecond: 2_000,
				Raw: quality.RawAccounting{ExpectedRecords: 10_000, CommittedRecords: 10_000}, EvidenceSHA256: testDigest("sustained"),
			},
			Burst: quality.BurstObservation{
				DurationNS: quality.MinimumBurstDurationNS, MessagesPerSecond: 500, BytesPerSecond: 5_000,
				Raw: quality.RawAccounting{ExpectedRecords: 5_000, CommittedRecords: 5_000}, EvidenceSHA256: testDigest("burst"),
			},
			Memory: quality.MemoryObservation{
				QueueBoundBytes: 100, FrameBoundBytes: 100, RowGroupBoundBytes: 100, PeakRSSBytes: 330,
				Plateaued: true, EvidenceSHA256: testDigest("memory"),
			},
			Replay: quality.ReplayPerformanceObservation{
				NativeRecordsPerSecond: 100, NativeBytesPerSecond: 1_000, NativeWorkers: 1,
				NormalizedRecordsPerSecond: 200, NormalizedBytesPerSecond: 2_000, NormalizedWorkers: 1,
				EvidenceSHA256: testDigest("replay"),
			},
			Telemetry: quality.TelemetryBlackholeObservation{
				Raw:              quality.RawAccounting{ExpectedRecords: 1_000, CommittedRecords: 1_000},
				MemoryBoundBytes: 1_000, PeakMemoryBytes: 500, EvidenceSHA256: testDigest("telemetry"),
			},
			Queries: quality.X5QueryBudgetObservation{
				DatasetSHA256: testDigest("x5-dataset"), EvidenceSHA256: testDigest("x5-evidence"),
				RequiredIDs: []string{"query-a"},
				Thresholds: []quality.QueryThreshold{{
					ID: "query-a", MaxLatencyNS: 1_000, ObservedLatencyNS: 1_000,
					MaxResponseBytes: 10_000, ObservedResponseBytes: 10_000,
				}},
			},
			Corruption: quality.CorruptionObservation{
				InjectedCases: 10, DetectedCases: 10, EvidenceSHA256: testDigest("corruption"),
			},
		},
		FaultCoverage:       []quality.ContractFaultCoverage{{ContractID: contract.ContractID, Categories: categories}},
		DeterminismCoverage: []quality.ContractDeterminismCoverage{{ContractID: contract.ContractID, Gates: gates}},
		Canaries: []quality.CanaryObservation{{
			ContractID: contract.ContractID, Status: quality.CanaryPassedStatus,
			StartNS: canaryStart, EndNS: canaryStart + quality.MinimumCanaryDurationNS,
			ObservedNS:     quality.MinimumCanaryDurationNS,
			Raw:            quality.RawAccounting{ExpectedRecords: 20_000, CommittedRecords: 20_000},
			EvidenceSHA256: testDigest("canary"),
		}},
	}
}

func testDigest(label string) string {
	digest := sha256.Sum256([]byte("platform-publisher-test/" + label))
	return hex.EncodeToString(digest[:])
}
