package quality

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestReleaseGatePassingBundle(t *testing.T) {
	t.Parallel()
	bundle := passingReleaseGateBundle()
	report, err := EvaluateReleaseGate(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed {
		t.Fatalf("report did not pass: %#v", firstFailedReleaseGateCheck(report))
	}
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
	body, err := report.CanonicalBytes()
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(body)
	if got, want := report.BodySHA256, hex.EncodeToString(digest[:]); got != want {
		t.Fatalf("body SHA-256 = %q, want %q", got, want)
	}
	if got, err := report.ContentSHA256(); err != nil || got != report.BodySHA256 {
		t.Fatalf("ContentSHA256() = %q, %v", got, err)
	}

	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseReleaseGateReport(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.BodySHA256 != report.BodySHA256 {
		t.Fatalf("parsed body SHA-256 = %q, want %q", parsed.BodySHA256, report.BodySHA256)
	}
	contract := report.Bundle.Workload.Contracts[0]
	if err := report.ValidateContract(contract); err != nil {
		t.Fatal(err)
	}
	result, ok := report.ContractResult(contract.ContractID)
	if !ok || !result.Passed || result.Tuple != contract {
		t.Fatalf("ContractResult() = %#v, %v", result, ok)
	}
	wrong := contract
	wrong.DataFamily = "wrong-family"
	if err := report.ValidateContract(wrong); !errors.Is(err, ErrInvalidReleaseGate) {
		t.Fatalf("ValidateContract(wrong) error = %v", err)
	}
}

func TestReleaseGateCanonicalizesObservationOrder(t *testing.T) {
	t.Parallel()
	canonical, err := EvaluateReleaseGate(passingReleaseGateBundle())
	if err != nil {
		t.Fatal(err)
	}
	shuffled := passingReleaseGateBundle()
	slices.Reverse(shuffled.Workload.Contracts)
	slices.Reverse(shuffled.Corpus.VenueFamilies)
	slices.Reverse(shuffled.Corpus.PayloadClasses)
	slices.Reverse(shuffled.FaultCoverage)
	for index := range shuffled.FaultCoverage {
		slices.Reverse(shuffled.FaultCoverage[index].Categories)
	}
	slices.Reverse(shuffled.DeterminismCoverage)
	for index := range shuffled.DeterminismCoverage {
		slices.Reverse(shuffled.DeterminismCoverage[index].Gates)
	}
	slices.Reverse(shuffled.Canaries)
	slices.Reverse(shuffled.Performance.Queries.RequiredIDs)
	slices.Reverse(shuffled.Performance.Queries.Thresholds)

	reordered, err := EvaluateReleaseGate(shuffled)
	if err != nil {
		t.Fatal(err)
	}
	if reordered.BodySHA256 != canonical.BodySHA256 {
		t.Fatalf("reordered body SHA-256 = %q, want %q", reordered.BodySHA256, canonical.BodySHA256)
	}
	left, err := json.Marshal(canonical)
	if err != nil {
		t.Fatal(err)
	}
	right, err := json.Marshal(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if string(left) != string(right) {
		t.Fatal("canonical reports differ after input reordering")
	}
}

func TestReleaseGateAcceptsDocumentedAlternatives(t *testing.T) {
	t.Parallel()
	t.Run("backpressure before bounds", func(t *testing.T) {
		t.Parallel()
		bundle := passingReleaseGateBundle()
		bundle.Performance.Burst.DurationNS = 1_000_000_000
		bundle.Performance.Burst.MessagesPerSecond = 50
		bundle.Performance.Burst.BytesPerSecond = 500
		bundle.Performance.Burst.DocumentedBackpressurePath = true
		bundle.Performance.Burst.BackpressureBeforeBounds = true
		assertReleaseGatePasses(t, bundle)
	})
	t.Run("measured order-preserving normalized parallelism", func(t *testing.T) {
		t.Parallel()
		bundle := passingReleaseGateBundle()
		bundle.Performance.Replay.NormalizedRecordsPerSecond = 99
		bundle.Performance.Replay.NormalizedBytesPerSecond = 999
		bundle.Performance.Replay.NormalizedWorkers = 4
		bundle.Performance.Replay.ParallelismMeasured = true
		bundle.Performance.Replay.ParallelismOrderPreserved = true
		assertReleaseGatePasses(t, bundle)
	})
}

func TestReleaseGateRejectsMandatoryThresholds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*ReleaseGateBundle)
	}{
		{"blank hardware identity", func(bundle *ReleaseGateBundle) { bundle.Hardware.ID = " " }},
		{"blank hardware digest", func(bundle *ReleaseGateBundle) { bundle.Hardware.ManifestSHA256 = "" }},
		{"unbounded hardware CPUs", func(bundle *ReleaseGateBundle) { bundle.Hardware.LogicalCPUs = MaxReleaseGateWorkers + 1 }},
		{"unbounded hardware memory", func(bundle *ReleaseGateBundle) { bundle.Hardware.MemoryBytes = MaxReleaseGateBytes + 1 }},
		{"non-equivalent hardware", func(bundle *ReleaseGateBundle) { bundle.Hardware.ProductionEquivalent = false }},
		{"blank workload digest", func(bundle *ReleaseGateBundle) { bundle.Workload.ManifestSHA256 = "" }},
		{"unbounded workload rate", func(bundle *ReleaseGateBundle) { bundle.Workload.MaxObservedMessagesPerSecond = MaxReleaseGateRate + 1 }},
		{"missing contracts", func(bundle *ReleaseGateBundle) { bundle.Workload.Contracts = nil }},
		{"duplicate contract ID", func(bundle *ReleaseGateBundle) {
			bundle.Workload.Contracts = append(bundle.Workload.Contracts, bundle.Workload.Contracts[0])
		}},
		{"duplicate tuple", func(bundle *ReleaseGateBundle) {
			duplicate := bundle.Workload.Contracts[0]
			duplicate.ContractID = "contract-duplicate"
			bundle.Workload.Contracts = append(bundle.Workload.Contracts, duplicate)
		}},
		{"blank entitlement", func(bundle *ReleaseGateBundle) { bundle.Workload.Contracts[0].Entitlement = "" }},
		{"unknown entitlement", func(bundle *ReleaseGateBundle) { bundle.Workload.Contracts[0].Entitlement = "unknown" }},
		{"ambiguous coverage", func(bundle *ReleaseGateBundle) {
			bundle.Workload.Contracts[0].CoverageEndNS = bundle.Workload.Contracts[0].CoverageStartNS
		}},
		{"blank contract digest", func(bundle *ReleaseGateBundle) { bundle.Workload.Contracts[0].ContractSHA256 = "" }},
		{"blank adapter digest", func(bundle *ReleaseGateBundle) { bundle.Workload.Contracts[0].AdapterSHA256 = "" }},
		{"unknown canary requirement", func(bundle *ReleaseGateBundle) { bundle.Workload.Contracts[0].CanaryRequirement = "maybe" }},
		{"blank corpus digest", func(bundle *ReleaseGateBundle) { bundle.Corpus.ManifestSHA256 = "" }},
		{"missing venue family", func(bundle *ReleaseGateBundle) { bundle.Corpus.VenueFamilies = bundle.Corpus.VenueFamilies[:1] }},
		{"duplicate venue family", func(bundle *ReleaseGateBundle) {
			bundle.Corpus.VenueFamilies = append(bundle.Corpus.VenueFamilies, bundle.Corpus.VenueFamilies[0])
		}},
		{"missing tiny payload", func(bundle *ReleaseGateBundle) {
			bundle.Corpus.PayloadClasses = []PayloadClass{PayloadMedian, PayloadMax}
		}},
		{"missing high-cardinality corpus", func(bundle *ReleaseGateBundle) { bundle.Corpus.HighCardinalitySymbols = false }},
		{"missing long books", func(bundle *ReleaseGateBundle) { bundle.Corpus.LongBooks = false }},
		{"missing sparse ticker updates", func(bundle *ReleaseGateBundle) { bundle.Corpus.SparseTickerUpdates = false }},
		{"missing reconnects", func(bundle *ReleaseGateBundle) { bundle.Corpus.Reconnects = false }},
		{"missing long histories", func(bundle *ReleaseGateBundle) { bundle.Corpus.LongHistories = false }},
		{"short sustained run", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Sustained.DurationNS = MinimumSustainedDurationNS - 1
		}},
		{"low sustained message rate", func(bundle *ReleaseGateBundle) { bundle.Performance.Sustained.MessagesPerSecond = 199 }},
		{"low sustained byte rate", func(bundle *ReleaseGateBundle) { bundle.Performance.Sustained.BytesPerSecond = 1_999 }},
		{"unbounded sustained rate", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Sustained.MessagesPerSecond = MaxReleaseGateRate + 1
		}},
		{"sustained unexplained loss", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Sustained.Raw.CommittedRecords--
			bundle.Performance.Sustained.Raw.UnexplainedLossRecords = 1
		}},
		{"sustained inexact accounting", func(bundle *ReleaseGateBundle) { bundle.Performance.Sustained.Raw.CommittedRecords-- }},
		{"blank sustained digest", func(bundle *ReleaseGateBundle) { bundle.Performance.Sustained.EvidenceSHA256 = "" }},
		{"short low burst", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Burst.DurationNS = MinimumBurstDurationNS - 1
			bundle.Performance.Burst.MessagesPerSecond = 499
		}},
		{"late backpressure", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Burst.DurationNS = 1_000_000_000
			bundle.Performance.Burst.MessagesPerSecond = 100
			bundle.Performance.Burst.BytesPerSecond = 1_000
			bundle.Performance.Burst.DocumentedBackpressurePath = true
			bundle.Performance.Burst.BackpressureBeforeBounds = false
		}},
		{"burst memory limit", func(bundle *ReleaseGateBundle) { bundle.Performance.Burst.MemoryLimitExceeded = true }},
		{"burst spool limit", func(bundle *ReleaseGateBundle) { bundle.Performance.Burst.SpoolLimitExceeded = true }},
		{"burst queue limit", func(bundle *ReleaseGateBundle) { bundle.Performance.Burst.QueueLimitExceeded = true }},
		{"burst unexplained loss", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Burst.Raw.CommittedRecords--
			bundle.Performance.Burst.Raw.UnexplainedLossRecords = 1
		}},
		{"blank burst digest", func(bundle *ReleaseGateBundle) { bundle.Performance.Burst.EvidenceSHA256 = "" }},
		{"zero queue bound", func(bundle *ReleaseGateBundle) { bundle.Performance.Memory.QueueBoundBytes = 0 }},
		{"RSS above variance", func(bundle *ReleaseGateBundle) { bundle.Performance.Memory.PeakRSSBytes = 331 }},
		{"RSS not plateaued", func(bundle *ReleaseGateBundle) { bundle.Performance.Memory.Plateaued = false }},
		{"unbounded RSS", func(bundle *ReleaseGateBundle) { bundle.Performance.Memory.UnboundedGrowth = true }},
		{"blank memory digest", func(bundle *ReleaseGateBundle) { bundle.Performance.Memory.EvidenceSHA256 = "" }},
		{"low native record rate", func(bundle *ReleaseGateBundle) { bundle.Performance.Replay.NativeRecordsPerSecond = 99 }},
		{"low native byte rate", func(bundle *ReleaseGateBundle) { bundle.Performance.Replay.NativeBytesPerSecond = 999 }},
		{"multiple native workers", func(bundle *ReleaseGateBundle) { bundle.Performance.Replay.NativeWorkers = 2 }},
		{"low normalized record rate", func(bundle *ReleaseGateBundle) { bundle.Performance.Replay.NormalizedRecordsPerSecond = 199 }},
		{"low normalized byte rate", func(bundle *ReleaseGateBundle) { bundle.Performance.Replay.NormalizedBytesPerSecond = 1_999 }},
		{"unordered parallel replay", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Replay.NormalizedRecordsPerSecond = 100
			bundle.Performance.Replay.NormalizedBytesPerSecond = 1_000
			bundle.Performance.Replay.NormalizedWorkers = 4
			bundle.Performance.Replay.ParallelismMeasured = true
			bundle.Performance.Replay.ParallelismOrderPreserved = false
		}},
		{"blank replay digest", func(bundle *ReleaseGateBundle) { bundle.Performance.Replay.EvidenceSHA256 = "" }},
		{"telemetry loss", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Telemetry.Raw.CommittedRecords--
			bundle.Performance.Telemetry.Raw.ExplainedLossRecords = 1
		}},
		{"telemetry unexplained loss", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Telemetry.Raw.CommittedRecords--
			bundle.Performance.Telemetry.Raw.UnexplainedLossRecords = 1
		}},
		{"telemetry memory exceeded", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Telemetry.PeakMemoryBytes = bundle.Performance.Telemetry.MemoryBoundBytes + 1
		}},
		{"telemetry stalled capture", func(bundle *ReleaseGateBundle) { bundle.Performance.Telemetry.CaptureStalled = true }},
		{"blank telemetry digest", func(bundle *ReleaseGateBundle) { bundle.Performance.Telemetry.EvidenceSHA256 = "" }},
		{"missing X5 thresholds", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Queries.RequiredIDs = nil
			bundle.Performance.Queries.Thresholds = nil
		}},
		{"missing required X5 threshold", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Queries.Thresholds = bundle.Performance.Queries.Thresholds[:1]
		}},
		{"duplicate X5 threshold", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Queries.Thresholds[1] = bundle.Performance.Queries.Thresholds[0]
		}},
		{"blank X5 dataset digest", func(bundle *ReleaseGateBundle) { bundle.Performance.Queries.DatasetSHA256 = "" }},
		{"blank X5 evidence digest", func(bundle *ReleaseGateBundle) { bundle.Performance.Queries.EvidenceSHA256 = "" }},
		{"query latency exceeded", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Queries.Thresholds[0].ObservedLatencyNS = bundle.Performance.Queries.Thresholds[0].MaxLatencyNS + 1
		}},
		{"query response exceeded", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Queries.Thresholds[0].ObservedResponseBytes = bundle.Performance.Queries.Thresholds[0].MaxResponseBytes + 1
		}},
		{"query threshold unbounded", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Queries.Thresholds[0].MaxResponseBytes = MaxReleaseGateBytes + 1
		}},
		{"undetected corruption", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Corruption.DetectedCases--
			bundle.Performance.Corruption.UndetectedCases = 1
		}},
		{"inexact corruption accounting", func(bundle *ReleaseGateBundle) { bundle.Performance.Corruption.DetectedCases-- }},
		{"zero corruption cases", func(bundle *ReleaseGateBundle) {
			bundle.Performance.Corruption.InjectedCases = 0
			bundle.Performance.Corruption.DetectedCases = 0
		}},
		{"blank corruption digest", func(bundle *ReleaseGateBundle) { bundle.Performance.Corruption.EvidenceSHA256 = "" }},
		{"missing fault contract", func(bundle *ReleaseGateBundle) { bundle.FaultCoverage = bundle.FaultCoverage[:1] }},
		{"duplicate fault contract", func(bundle *ReleaseGateBundle) {
			bundle.FaultCoverage = append(bundle.FaultCoverage, bundle.FaultCoverage[0])
		}},
		{"unknown fault category", func(bundle *ReleaseGateBundle) { bundle.FaultCoverage[0].Categories[0].CategoryID = "unknown" }},
		{"duplicate fault category", func(bundle *ReleaseGateBundle) {
			bundle.FaultCoverage[0].Categories[1] = bundle.FaultCoverage[0].Categories[0]
		}},
		{"failed fault category", func(bundle *ReleaseGateBundle) {
			bundle.FaultCoverage[0].Categories[0].Passed = false
			bundle.FaultCoverage[0].Categories[0].FailureCount = 1
		}},
		{"blank fault evidence", func(bundle *ReleaseGateBundle) { bundle.FaultCoverage[0].Categories[0].EvidenceSHA256 = "" }},
		{"missing determinism contract", func(bundle *ReleaseGateBundle) { bundle.DeterminismCoverage = bundle.DeterminismCoverage[:1] }},
		{"duplicate determinism contract", func(bundle *ReleaseGateBundle) {
			bundle.DeterminismCoverage = append(bundle.DeterminismCoverage, bundle.DeterminismCoverage[0])
		}},
		{"unknown determinism gate", func(bundle *ReleaseGateBundle) { bundle.DeterminismCoverage[0].Gates[0].GateID = "unknown" }},
		{"duplicate determinism gate", func(bundle *ReleaseGateBundle) {
			bundle.DeterminismCoverage[0].Gates[1] = bundle.DeterminismCoverage[0].Gates[0]
		}},
		{"determinism mismatch", func(bundle *ReleaseGateBundle) {
			bundle.DeterminismCoverage[0].Gates[0].Passed = false
			bundle.DeterminismCoverage[0].Gates[0].MismatchCount = 1
		}},
		{"blank determinism evidence", func(bundle *ReleaseGateBundle) { bundle.DeterminismCoverage[0].Gates[0].EvidenceSHA256 = "" }},
		{"missing canary contract", func(bundle *ReleaseGateBundle) { bundle.Canaries = bundle.Canaries[:1] }},
		{"duplicate canary contract", func(bundle *ReleaseGateBundle) { bundle.Canaries = append(bundle.Canaries, bundle.Canaries[0]) }},
		{"short required canary", func(bundle *ReleaseGateBundle) {
			bundle.Canaries[0].EndNS--
			bundle.Canaries[0].ObservedNS--
		}},
		{"unexplained canary gap", func(bundle *ReleaseGateBundle) {
			bundle.Canaries[0].ObservedNS--
			bundle.Canaries[0].UnexplainedGapNS = 1
		}},
		{"inexact canary interval", func(bundle *ReleaseGateBundle) { bundle.Canaries[0].ObservedNS-- }},
		{"canary raw loss", func(bundle *ReleaseGateBundle) {
			bundle.Canaries[0].Raw.CommittedRecords--
			bundle.Canaries[0].Raw.UnexplainedLossRecords = 1
		}},
		{"unknown required canary status", func(bundle *ReleaseGateBundle) { bundle.Canaries[0].Status = "unknown" }},
		{"ambiguous optional canary", func(bundle *ReleaseGateBundle) {
			bundle.Canaries[1].Status = CanaryPassedStatus
			bundle.Canaries[1].StartNS = 1
			bundle.Canaries[1].EndNS = 2
		}},
		{"blank canary evidence", func(bundle *ReleaseGateBundle) { bundle.Canaries[0].EvidenceSHA256 = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			bundle := passingReleaseGateBundle()
			test.mutate(&bundle)
			assertReleaseGateRejected(t, bundle)
		})
	}
}

func TestReleaseGateRejectsEveryMissingFaultCategory(t *testing.T) {
	t.Parallel()
	for index, categoryID := range RequiredFaultCategoryIDs() {
		t.Run(string(categoryID), func(t *testing.T) {
			t.Parallel()
			bundle := passingReleaseGateBundle()
			categories := bundle.FaultCoverage[0].Categories
			bundle.FaultCoverage[0].Categories = slices.Concat(categories[:index], categories[index+1:])
			assertReleaseGateRejected(t, bundle)
		})
	}
}

func TestReleaseGateRejectsEveryFailedFaultCategory(t *testing.T) {
	t.Parallel()
	for index, categoryID := range RequiredFaultCategoryIDs() {
		t.Run(string(categoryID), func(t *testing.T) {
			t.Parallel()
			bundle := passingReleaseGateBundle()
			bundle.FaultCoverage[0].Categories[index].Passed = false
			bundle.FaultCoverage[0].Categories[index].FailureCount = 1
			assertReleaseGateRejected(t, bundle)
		})
	}
}

func TestReleaseGateRejectsEveryMissingDeterminismGate(t *testing.T) {
	t.Parallel()
	for index, gateID := range RequiredDeterminismGateIDs() {
		t.Run(string(gateID), func(t *testing.T) {
			t.Parallel()
			bundle := passingReleaseGateBundle()
			gates := bundle.DeterminismCoverage[0].Gates
			bundle.DeterminismCoverage[0].Gates = slices.Concat(gates[:index], gates[index+1:])
			assertReleaseGateRejected(t, bundle)
		})
	}
}

func TestReleaseGateRejectsEveryDeterminismMismatch(t *testing.T) {
	t.Parallel()
	for index, gateID := range RequiredDeterminismGateIDs() {
		t.Run(string(gateID), func(t *testing.T) {
			t.Parallel()
			bundle := passingReleaseGateBundle()
			bundle.DeterminismCoverage[0].Gates[index].Passed = false
			bundle.DeterminismCoverage[0].Gates[index].MismatchCount = 1
			assertReleaseGateRejected(t, bundle)
		})
	}
}

func TestReleaseGateRejectsEveryMissingX5Threshold(t *testing.T) {
	t.Parallel()
	for index, threshold := range passingReleaseGateBundle().Performance.Queries.Thresholds {
		t.Run(threshold.ID, func(t *testing.T) {
			t.Parallel()
			bundle := passingReleaseGateBundle()
			bundle.Performance.Queries.Thresholds = slices.Concat(bundle.Performance.Queries.Thresholds[:index], bundle.Performance.Queries.Thresholds[index+1:])
			assertReleaseGateRejected(t, bundle)
		})
	}
}

func TestReleaseGateValidationRejectsTamperingAndUnknownJSON(t *testing.T) {
	t.Parallel()
	report, err := EvaluateReleaseGate(passingReleaseGateBundle())
	if err != nil {
		t.Fatal(err)
	}
	t.Run("hash mismatch", func(t *testing.T) {
		t.Parallel()
		tampered := report
		tampered.BodySHA256 = strings.Repeat("0", sha256.Size*2)
		if err := tampered.Validate(); !errors.Is(err, ErrInvalidReleaseGate) {
			t.Fatalf("Validate() error = %v", err)
		}
	})
	t.Run("decision mismatch", func(t *testing.T) {
		t.Parallel()
		tampered := report
		tampered.Passed = false
		if err := tampered.Validate(); !errors.Is(err, ErrInvalidReleaseGate) {
			t.Fatalf("Validate() error = %v", err)
		}
	})
	t.Run("check mismatch", func(t *testing.T) {
		t.Parallel()
		tampered := report
		tampered.Checks = slices.Clone(report.Checks)
		tampered.Checks[0].Reason = "invented"
		if err := tampered.Validate(); !errors.Is(err, ErrInvalidReleaseGate) {
			t.Fatalf("Validate() error = %v", err)
		}
	})
	t.Run("unknown JSON field", func(t *testing.T) {
		t.Parallel()
		encoded, err := json.Marshal(report)
		if err != nil {
			t.Fatal(err)
		}
		encoded = append(encoded[:len(encoded)-1], []byte(`,"unknown":true}`)...)
		if _, err := ParseReleaseGateReport(encoded); !errors.Is(err, ErrInvalidReleaseGate) {
			t.Fatalf("ParseReleaseGateReport() error = %v", err)
		}
	})
}

func passingReleaseGateBundle() ReleaseGateBundle {
	contracts := []ContractIdentity{
		{
			ContractID: "contract-a", SourceID: "source-a", APIVersion: "v1", Entitlement: "public",
			ChannelOrEndpoint: "trades", DataFamily: "trade", NativeGranularity: "event", VenueFamily: "venue-a",
			CoverageStartNS: 1_000_000_000, CoverageEndNS: 2_000_000_000, AdapterVersion: "adapter-a/v1",
			ContractSHA256: releaseGateDigest("contract-a"), AdapterSHA256: releaseGateDigest("adapter-a"), CanaryRequirement: CanaryRequired,
		},
		{
			ContractID: "contract-b", SourceID: "source-b", APIVersion: "v2", Entitlement: "public",
			ChannelOrEndpoint: "ticker", DataFamily: "ticker", NativeGranularity: "100ms", VenueFamily: "venue-b",
			CoverageStartNS: 3_000_000_000, CoverageEndNS: 4_000_000_000, AdapterVersion: "adapter-b/v1",
			ContractSHA256: releaseGateDigest("contract-b"), AdapterSHA256: releaseGateDigest("adapter-b"), CanaryRequirement: CanaryNotRequired,
		},
	}
	faultCoverage := make([]ContractFaultCoverage, 0, len(contracts))
	determinismCoverage := make([]ContractDeterminismCoverage, 0, len(contracts))
	for _, contract := range contracts {
		categories := make([]FaultCategoryObservation, 0, len(RequiredFaultCategoryIDs()))
		for _, categoryID := range RequiredFaultCategoryIDs() {
			categories = append(categories, FaultCategoryObservation{
				CategoryID: categoryID, Passed: true, EvidenceSHA256: releaseGateDigest(contract.ContractID + "/fault/" + string(categoryID)),
			})
		}
		faultCoverage = append(faultCoverage, ContractFaultCoverage{ContractID: contract.ContractID, Categories: categories})
		gates := make([]DeterminismObservation, 0, len(RequiredDeterminismGateIDs()))
		for _, gateID := range RequiredDeterminismGateIDs() {
			gates = append(gates, DeterminismObservation{
				GateID: gateID, Passed: true, EvidenceSHA256: releaseGateDigest(contract.ContractID + "/determinism/" + string(gateID)),
			})
		}
		determinismCoverage = append(determinismCoverage, ContractDeterminismCoverage{ContractID: contract.ContractID, Gates: gates})
	}
	canaryStart := int64(1_700_000_000_000_000_000)
	return ReleaseGateBundle{
		Hardware: HardwareIdentity{
			ID: "hardware-a", OS: "linux", Architecture: "amd64", CPUModel: "synthetic-production-equivalent",
			LogicalCPUs: 16, MemoryBytes: 64 << 30, ProductionEquivalent: true, ManifestSHA256: releaseGateDigest("hardware"),
		},
		Workload: WorkloadIdentity{
			ID: "workload-a", ManifestSHA256: releaseGateDigest("workload"),
			MaxObservedMessagesPerSecond: 100, MaxObservedBytesPerSecond: 1_000,
			AcquisitionRecordsPerSecond: 100, AcquisitionBytesPerSecond: 1_000, Contracts: contracts,
		},
		Corpus: FixedCorpusIdentity{
			ID: "corpus-a", ManifestSHA256: releaseGateDigest("corpus"), VenueFamilies: []string{"venue-a", "venue-b"},
			PayloadClasses: []PayloadClass{PayloadTiny, PayloadMedian, PayloadMax}, HighCardinalitySymbols: true,
			LongBooks: true, SparseTickerUpdates: true, Reconnects: true, LongHistories: true,
		},
		Performance: PerformanceObservations{
			Sustained: SustainedLoadObservation{
				DurationNS: MinimumSustainedDurationNS, MessagesPerSecond: 200, BytesPerSecond: 2_000,
				Raw: RawAccounting{ExpectedRecords: 10_000, CommittedRecords: 10_000}, EvidenceSHA256: releaseGateDigest("sustained"),
			},
			Burst: BurstObservation{
				DurationNS: MinimumBurstDurationNS, MessagesPerSecond: 500, BytesPerSecond: 5_000,
				Raw: RawAccounting{ExpectedRecords: 5_000, CommittedRecords: 5_000}, EvidenceSHA256: releaseGateDigest("burst"),
			},
			Memory: MemoryObservation{
				QueueBoundBytes: 100, FrameBoundBytes: 100, RowGroupBoundBytes: 100, PeakRSSBytes: 330,
				Plateaued: true, EvidenceSHA256: releaseGateDigest("memory"),
			},
			Replay: ReplayPerformanceObservation{
				NativeRecordsPerSecond: 100, NativeBytesPerSecond: 1_000, NativeWorkers: 1,
				NormalizedRecordsPerSecond: 200, NormalizedBytesPerSecond: 2_000, NormalizedWorkers: 1,
				EvidenceSHA256: releaseGateDigest("replay"),
			},
			Telemetry: TelemetryBlackholeObservation{
				Raw: RawAccounting{ExpectedRecords: 1_000, CommittedRecords: 1_000}, MemoryBoundBytes: 1_000,
				PeakMemoryBytes: 500, EvidenceSHA256: releaseGateDigest("telemetry"),
			},
			Queries: X5QueryBudgetObservation{
				DatasetSHA256: releaseGateDigest("x5-dataset"), EvidenceSHA256: releaseGateDigest("x5-evidence"),
				RequiredIDs: []string{"query-b", "query-a"},
				Thresholds: []QueryThreshold{
					{ID: "query-b", MaxLatencyNS: 2_000, ObservedLatencyNS: 1_999, MaxResponseBytes: 20_000, ObservedResponseBytes: 19_999},
					{ID: "query-a", MaxLatencyNS: 1_000, ObservedLatencyNS: 1_000, MaxResponseBytes: 10_000, ObservedResponseBytes: 10_000},
				},
			},
			Corruption: CorruptionObservation{
				InjectedCases: 10, DetectedCases: 10, EvidenceSHA256: releaseGateDigest("corruption"),
			},
		},
		FaultCoverage:       faultCoverage,
		DeterminismCoverage: determinismCoverage,
		Canaries: []CanaryObservation{
			{
				ContractID: "contract-a", Status: CanaryPassedStatus, StartNS: canaryStart, EndNS: canaryStart + MinimumCanaryDurationNS,
				ObservedNS: MinimumCanaryDurationNS, Raw: RawAccounting{ExpectedRecords: 20_000, CommittedRecords: 20_000},
				EvidenceSHA256: releaseGateDigest("canary-a"),
			},
			{ContractID: "contract-b", Status: CanaryNotRequiredStatus, EvidenceSHA256: releaseGateDigest("canary-b-requirement")},
		},
	}
}

func assertReleaseGatePasses(t *testing.T, bundle ReleaseGateBundle) {
	t.Helper()
	report, err := EvaluateReleaseGate(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Passed {
		t.Fatalf("gate failed: %#v", firstFailedReleaseGateCheck(report))
	}
	if err := report.Validate(); err != nil {
		t.Fatal(err)
	}
}

func assertReleaseGateRejected(t *testing.T, bundle ReleaseGateBundle) {
	t.Helper()
	report, err := EvaluateReleaseGate(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if report.Passed {
		t.Fatal("gate unexpectedly passed")
	}
	if _, err := report.ContentSHA256(); err != nil {
		t.Fatalf("failed report is not stable evidence: %v", err)
	}
	if err := report.Validate(); !errors.Is(err, ErrReleaseGateFailed) {
		t.Fatalf("Validate() error = %v, want ErrReleaseGateFailed; first failed check: %#v", err, firstFailedReleaseGateCheck(report))
	}
}

func firstFailedReleaseGateCheck(report ReleaseGateReport) ReleaseGateCheck {
	for _, check := range report.Checks {
		if !check.Passed {
			return check
		}
	}
	return ReleaseGateCheck{}
}

func releaseGateDigest(label string) string {
	digest := sha256.Sum256([]byte(fmt.Sprintf("release-gate-test/%s", label)))
	return hex.EncodeToString(digest[:])
}
