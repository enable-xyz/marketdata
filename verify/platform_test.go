package verify

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
)

func TestPlatformBundleIsDeterministicAndSelfValidating(t *testing.T) {
	firstInput, firstInventory := syntheticPlatformBundleFixture()
	first, err := BuildPlatformBundle(firstInput, firstInventory)
	if err != nil {
		t.Fatalf("BuildPlatformBundle(first): %v", err)
	}

	secondInput, secondInventory := syntheticPlatformBundleFixture()
	slices.Reverse(secondInput.Declarations)
	slices.Reverse(secondInput.Rows)
	for i := range secondInput.Rows {
		slices.Reverse(secondInput.Rows[i].SourceEvidence)
		slices.Reverse(secondInput.Rows[i].FixtureEvidence)
		slices.Reverse(secondInput.Rows[i].RawEvidence)
		slices.Reverse(secondInput.Rows[i].DatasetEvidence)
		slices.Reverse(secondInput.Rows[i].ReplayEvidence)
		slices.Reverse(secondInput.Rows[i].CatalogEvidence)
	}
	second, err := BuildPlatformBundle(secondInput, secondInventory)
	if err != nil {
		t.Fatalf("BuildPlatformBundle(reordered): %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("platform bundle bytes changed when declarations, rows, and references were reordered")
	}
	if !bytes.HasSuffix(first, []byte("\n")) {
		t.Fatal("platform bundle is not newline terminated")
	}

	bundle, err := ValidatePlatformBundle(first, firstInput.Declarations, firstInventory)
	if err != nil {
		t.Fatalf("ValidatePlatformBundle: %v", err)
	}
	if bundle.PlanSHA256 != PlatformPlanSHA256 || bundle.BodySHA256 == "" {
		t.Fatalf("bundle identities = plan %q body %q", bundle.PlanSHA256, bundle.BodySHA256)
	}
	if len(bundle.Rows) != 4 {
		t.Fatalf("row count = %d, want 4", len(bundle.Rows))
	}

	statuses := make(map[ParityStatus]int)
	requiredCanaries := 0
	optionalCanaries := 0
	openIntervals := 0
	for i, row := range bundle.Rows {
		statuses[row.Status]++
		if row.Status == ParityUnsupported || row.Status == ParityAmbiguous {
			if row.OperationalGateReportSHA256 != "" {
				t.Fatalf("status %q fabricated an operational gate report", row.Status)
			}
			nonSourceEvidence := len(row.FixtureEvidence) + len(row.RawEvidence) +
				len(row.DatasetEvidence) + len(row.ReplayEvidence) + len(row.CatalogEvidence)
			if nonSourceEvidence != 0 {
				t.Fatalf("status %q fabricated %d non-source evidence references", row.Status, nonSourceEvidence)
			}
		}
		if row.CanaryRequired {
			requiredCanaries++
		} else {
			optionalCanaries++
		}
		if row.Tuple.CoverageEndNS == nil {
			openIntervals++
		}
		if i > 0 && compareTupleIdentity(bundle.Rows[i-1].Tuple, row.Tuple) >= 0 {
			t.Fatal("builder did not canonically sort tuple rows")
		}
	}
	for _, status := range []ParityStatus{ParityProved, ParityPartiallyProved, ParityUnsupported, ParityAmbiguous} {
		if statuses[status] != 1 {
			t.Fatalf("status %q count = %d, want 1", status, statuses[status])
		}
	}
	if requiredCanaries == 0 || optionalCanaries == 0 {
		t.Fatalf("canary declarations = %d required, %d optional", requiredCanaries, optionalCanaries)
	}
	if openIntervals == 0 {
		t.Fatal("synthetic bundle did not preserve an optional coverage end")
	}
}

func TestPlatformVocabulariesAreExact(t *testing.T) {
	lifecycle := []LifecycleStatus{
		LifecycleCandidate,
		LifecycleCaptured,
		LifecycleReplayable,
		LifecycleNormalized,
		LifecycleReconstructed,
		LifecycleSupported,
		LifecycleUnsupported,
		LifecycleAmbiguous,
	}
	wantLifecycle := []LifecycleStatus{
		"candidate",
		"captured",
		"replayable",
		"normalized",
		"reconstructed",
		"supported",
		"unsupported",
		"ambiguous",
	}
	if !slices.Equal(lifecycle, wantLifecycle) {
		t.Fatalf("lifecycle vocabulary = %q, want %q", lifecycle, wantLifecycle)
	}

	parity := []ParityStatus{
		ParityProved,
		ParityPartiallyProved,
		ParityUnsupported,
		ParityAmbiguous,
	}
	wantParity := []ParityStatus{"proved", "partially_proved", "unsupported", "ambiguous"}
	if !slices.Equal(parity, wantParity) {
		t.Fatalf("parity vocabulary = %q, want %q", parity, wantParity)
	}
}

func TestBuildPlatformBundleFailsClosed(t *testing.T) {
	missingDigest := strings.Repeat("a", 64)
	tests := []struct {
		name   string
		want   string
		mutate func(*PlatformBundleInput, ArtifactInventory)
	}{
		{
			name: "duplicate declared tuple",
			want: "duplicate declared tuple",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Declarations = append(input.Declarations, input.Declarations[0])
			},
		},
		{
			name: "duplicate parity tuple",
			want: "duplicate",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows = append(input.Rows, input.Rows[0])
			},
		},
		{
			name: "blank limitation",
			want: "limitation is blank",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].Limitation = " "
			},
		},
		{
			name: "unknown parity status",
			want: "invalid parity status",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].Status = "complete"
			},
		},
		{
			name: "unknown lifecycle status",
			want: "invalid lifecycle",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].Lifecycle = "complete"
			},
		},
		{
			name: "candidate parity transition",
			want: "partially_proved parity is invalid",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Declarations[1].Lifecycle = LifecycleCandidate
				input.Rows[1].Lifecycle = LifecycleCandidate
			},
		},
		{
			name: "unsupported row claimed proved",
			want: "proved parity requires",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[2].Status = ParityProved
			},
		},
		{
			name: "ambiguous row missing ambiguity text",
			want: "ambiguity is blank",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[3].Ambiguity = ""
			},
		},
		{
			name: "non-ambiguous row has ambiguity text",
			want: "only valid for an ambiguous",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].Ambiguity = "not applicable"
			},
		},
		{
			name: "missing declared tuple",
			want: "missing one or more declared tuples",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows = input.Rows[:len(input.Rows)-1]
			},
		},
		{
			name: "extra undeclared tuple",
			want: "extra undeclared tuple",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				extra := input.Rows[2]
				extra.Tuple = cloneTupleIdentity(extra.Tuple)
				extra.Tuple.SourceID = "venue-extra"
				input.Rows = append(input.Rows, extra)
			},
		},
		{
			name: "lifecycle differs from declaration",
			want: "lifecycle does not match",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Declarations[0].Lifecycle = LifecycleNormalized
			},
		},
		{
			name: "canary requirement differs from declaration",
			want: "canary requirement does not match",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].CanaryRequired = false
			},
		},
		{
			name: "invalid coverage interval",
			want: "coverage_end_ns must be greater",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				end := input.Rows[0].Tuple.CoverageStartNS
				input.Rows[0].Tuple.CoverageEndNS = &end
			},
		},
		{
			name: "unresolved evidence checksum",
			want: "does not resolve exactly",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].RawEvidence[0].SHA256 = missingDigest
			},
		},
		{
			name: "mismatched evidence locator",
			want: "does not resolve exactly",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].RawEvidence[0].Locator = "evidence://synthetic/different"
			},
		},
		{
			name: "filesystem evidence locator",
			want: "canonical URI",
			mutate: func(input *PlatformBundleInput, inventory ArtifactInventory) {
				ref := &input.Rows[0].RawEvidence[0]
				entry := inventory[ref.SHA256]
				entry.Locator = "/tmp/raw-payload"
				inventory[ref.SHA256] = entry
				ref.Locator = entry.Locator
			},
		},
		{
			name: "unknown evidence kind",
			want: "invalid evidence kind",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].RawEvidence[0].Kind = EvidenceDataset
			},
		},
		{
			name: "duplicate evidence checksum",
			want: "unsorted or duplicated",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].RawEvidence = append(input.Rows[0].RawEvidence, input.Rows[0].RawEvidence[0])
			},
		},
		{
			name: "proved tuple missing source contract",
			want: "lacks source_contract",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].SourceEvidence = input.Rows[2].SourceEvidence
			},
		},
		{
			name: "proved tuple missing dataset evidence",
			want: "lacks dataset or catalog",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[0].DatasetEvidence = nil
			},
		},
		{
			name: "unsupported tuple missing official contract",
			want: "lacks official_contract",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[2].SourceEvidence = input.Rows[0].SourceEvidence
			},
		},
		{
			name: "unresolved partial operational gate report",
			want: "partial operational gate report checksum is unresolved",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[1].OperationalGateReportSHA256 = missingDigest
			},
		},
		{
			name: "unsupported tuple references gate report",
			want: "must not reference an operational gate report",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[2].OperationalGateReportSHA256 = missingDigest
			},
		},
		{
			name: "ambiguous tuple references gate report",
			want: "must not reference an operational gate report",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.Rows[3].OperationalGateReportSHA256 = missingDigest
			},
		},
		{
			name: "supported tuple missing gate validator",
			want: "lacks an operational gate validator",
			mutate: func(input *PlatformBundleInput, inventory ArtifactInventory) {
				digest := input.Rows[0].OperationalGateReportSHA256
				entry := inventory[digest]
				entry.OperationalGateValidator = nil
				inventory[digest] = entry
			},
		},
		{
			name: "operational gate rejects tuple",
			want: "does not prove every required gate",
			mutate: func(input *PlatformBundleInput, inventory ArtifactInventory) {
				digest := input.Rows[0].OperationalGateReportSHA256
				entry := inventory[digest]
				entry.OperationalGateValidator = func(string, TupleIdentity, bool) error {
					return errors.New("required determinism gate failed")
				}
				inventory[digest] = entry
			},
		},
		{
			name: "unresolved X5 evidence",
			want: "X5 evidence checksum is unresolved",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.X5EvidenceSHA256 = missingDigest
			},
		},
		{
			name: "uppercase inventory checksum",
			want: "invalid lowercase SHA-256 key",
			mutate: func(input *PlatformBundleInput, inventory ArtifactInventory) {
				digest := input.X5EvidenceSHA256
				entry := inventory[digest]
				delete(inventory, digest)
				upper := strings.ToUpper(digest)
				inventory[upper] = entry
				input.X5EvidenceSHA256 = upper
			},
		},
		{
			name: "ambiguous inventory locator",
			want: "one locator to multiple checksums",
			mutate: func(input *PlatformBundleInput, inventory ArtifactInventory) {
				hardware := inventory[input.HardwareIdentitySHA256]
				workload := inventory[input.WorkloadIdentitySHA256]
				workload.Locator = hardware.Locator
				inventory[input.WorkloadIdentitySHA256] = workload
			},
		},
		{
			name: "build timestamp is not UTC",
			want: "build date must be explicit UTC",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.BuildIdentity.Date = "2026-08-23T10:00:00+08:00"
			},
		},
		{
			name: "blank build identity",
			want: "build commit is blank",
			mutate: func(input *PlatformBundleInput, _ ArtifactInventory) {
				input.BuildIdentity.Commit = ""
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input, inventory := syntheticPlatformBundleFixture()
			test.mutate(&input, inventory)
			_, err := BuildPlatformBundle(input, inventory)
			if err == nil {
				t.Fatal("BuildPlatformBundle accepted invalid certification evidence")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %q, want substring %q", err, test.want)
			}
		})
	}
}

func TestValidatePlatformBundleRejectsInvalidEncodingAndHashes(t *testing.T) {
	input, inventory := syntheticPlatformBundleFixture()
	encoded, err := BuildPlatformBundle(input, inventory)
	if err != nil {
		t.Fatalf("BuildPlatformBundle: %v", err)
	}

	tests := []struct {
		name string
		want string
		make func(t *testing.T) []byte
	}{
		{
			name: "body hash",
			want: "body hash mismatch",
			make: func(t *testing.T) []byte {
				bundle := decodePlatformBundleForTest(t, encoded)
				bundle.BodySHA256 = strings.Repeat("0", 64)
				return encodePlatformBundleForTest(t, bundle)
			},
		},
		{
			name: "immutable plan hash",
			want: "plan hash mismatch",
			make: func(t *testing.T) []byte {
				bundle := decodePlatformBundleForTest(t, encoded)
				bundle.PlanSHA256 = strings.Repeat("0", 64)
				return remarshalPlatformBundleForTest(t, bundle)
			},
		},
		{
			name: "schema version",
			want: "schema version",
			make: func(t *testing.T) []byte {
				bundle := decodePlatformBundleForTest(t, encoded)
				bundle.SchemaVersion++
				return remarshalPlatformBundleForTest(t, bundle)
			},
		},
		{
			name: "unsorted rows",
			want: "rows are unsorted",
			make: func(t *testing.T) []byte {
				bundle := decodePlatformBundleForTest(t, encoded)
				bundle.Rows[0], bundle.Rows[1] = bundle.Rows[1], bundle.Rows[0]
				return remarshalPlatformBundleForTest(t, bundle)
			},
		},
		{
			name: "unsorted evidence references",
			want: "raw evidence is unsorted",
			make: func(t *testing.T) []byte {
				bundle := decodePlatformBundleForTest(t, encoded)
				bundle.Rows[0].RawEvidence[0], bundle.Rows[0].RawEvidence[1] = bundle.Rows[0].RawEvidence[1], bundle.Rows[0].RawEvidence[0]
				return remarshalPlatformBundleForTest(t, bundle)
			},
		},
		{
			name: "unknown JSON field",
			want: "unknown field",
			make: func(t *testing.T) []byte {
				return bytes.Replace(encoded, []byte("{"), []byte(`{"unknown":true,`), 1)
			},
		},
		{
			name: "trailing JSON",
			want: "trailing JSON",
			make: func(t *testing.T) []byte {
				return append(slices.Clone(encoded), []byte("{}")...)
			},
		},
		{
			name: "noncanonical encoding",
			want: "encoding is not canonical",
			make: func(t *testing.T) []byte {
				var indented bytes.Buffer
				if err := json.Indent(&indented, bytes.TrimSpace(encoded), "", "  "); err != nil {
					t.Fatal(err)
				}
				return indented.Bytes()
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ValidatePlatformBundle(test.make(t), input.Declarations, inventory)
			if err == nil {
				t.Fatal("ValidatePlatformBundle accepted invalid bundle")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %q, want substring %q", err, test.want)
			}
		})
	}
}

func TestPlatformBundleAcceptsStageApplicablePartialEvidence(t *testing.T) {
	input, inventory := syntheticPlatformBundleFixture()
	row := input.Rows[0]
	row.Lifecycle = LifecycleCaptured
	row.Status = ParityPartiallyProved
	row.CanaryRequired = false
	row.OperationalGateReportSHA256 = ""
	row.DatasetEvidence = nil
	row.ReplayEvidence = nil
	row.CatalogEvidence = nil
	input.Rows = []ParityRow{row}
	input.Declarations = []TupleDeclaration{{
		Tuple:          cloneTupleIdentity(row.Tuple),
		Lifecycle:      LifecycleCaptured,
		CanaryRequired: false,
	}}
	if _, err := BuildPlatformBundle(input, inventory); err != nil {
		t.Fatalf("captured partial evidence was rejected: %v", err)
	}
}

func syntheticPlatformBundleFixture() (PlatformBundleInput, ArtifactInventory) {
	inventory := make(ArtifactInventory)
	addDigest := func(label string, validator OperationalGateValidator) string {
		digest := hashHex([]byte("platform synthetic " + label))
		inventory[digest] = ArtifactInventoryEntry{
			Locator:                  "evidence://synthetic/" + label,
			OperationalGateValidator: validator,
		}
		return digest
	}
	addReference := func(label string, kind EvidenceKind) ArtifactReference {
		digest := addDigest(label, nil)
		return ArtifactReference{
			Kind:    kind,
			SHA256:  digest,
			Locator: inventory[digest].Locator,
		}
	}

	closedEnd := int64(2_000_000_000)
	provedTuple := TupleIdentity{
		SourceID:          "venue-a",
		APIVersion:        "ws-v1",
		Entitlement:       "public",
		Channel:           "trades",
		DataFamily:        "trade",
		NativeGranularity: "event",
		CoverageStartNS:   1_000_000_000,
		CoverageEndNS:     &closedEnd,
		AdapterVersion:    "adapter-a.v1",
	}
	partialTuple := TupleIdentity{
		SourceID:          "venue-b",
		APIVersion:        "ws-v5",
		Entitlement:       "public",
		Channel:           "ticker",
		DataFamily:        "quote",
		NativeGranularity: "100ms",
		CoverageStartNS:   1_100_000_000,
		AdapterVersion:    "adapter-b.v2",
	}
	unsupportedEnd := int64(2_200_000_000)
	unsupportedTuple := TupleIdentity{
		SourceID:          "venue-c",
		APIVersion:        "rest-v2",
		Entitlement:       "public",
		Channel:           "liquidations",
		DataFamily:        "liquidation",
		NativeGranularity: "not_exposed",
		CoverageStartNS:   1_200_000_000,
		CoverageEndNS:     &unsupportedEnd,
		AdapterVersion:    "adapter-c.v1",
	}
	ambiguousEnd := int64(2_300_000_000)
	ambiguousTuple := TupleIdentity{
		SourceID:          "venue-d",
		APIVersion:        "ws-v3",
		Entitlement:       "public",
		Channel:           "summary",
		DataFamily:        "open_interest",
		NativeGranularity: "upstream_batch",
		CoverageStartNS:   1_300_000_000,
		CoverageEndNS:     &ambiguousEnd,
		AdapterVersion:    "adapter-d.v1",
	}

	provedGate := addDigest("gate-proved", nil)
	provedGateEntry := inventory[provedGate]
	provedGateEntry.OperationalGateValidator = func(reportSHA256 string, tuple TupleIdentity, canaryRequired bool) error {
		if reportSHA256 != provedGate {
			return fmt.Errorf("gate report hash does not match the inventory entry")
		}
		if platformKey(tuple) != platformKey(provedTuple) {
			return fmt.Errorf("gate report covers a different tuple")
		}
		if !canaryRequired {
			return fmt.Errorf("gate report lacks the required canary")
		}
		return nil
	}
	inventory[provedGate] = provedGateEntry

	proved := ParityRow{
		Tuple:                       provedTuple,
		Lifecycle:                   LifecycleSupported,
		Status:                      ParityProved,
		CanaryRequired:              true,
		Limitation:                  "coverage begins at the v1 recorder start; no archive parity",
		OperationalGateReportSHA256: provedGate,
		SourceEvidence:              []ArtifactReference{addReference("proved-source", EvidenceSourceContract)},
		FixtureEvidence:             []ArtifactReference{addReference("proved-fixture", EvidenceFixture)},
		RawEvidence: []ArtifactReference{
			addReference("proved-raw-b", EvidenceRaw),
			addReference("proved-raw-a", EvidenceRaw),
		},
		DatasetEvidence: []ArtifactReference{addReference("proved-dataset", EvidenceDataset)},
		ReplayEvidence:  []ArtifactReference{addReference("proved-replay", EvidenceReplay)},
		CatalogEvidence: []ArtifactReference{addReference("proved-catalog", EvidenceCatalog)},
	}
	partial := ParityRow{
		Tuple:                       partialTuple,
		Lifecycle:                   LifecycleNormalized,
		Status:                      ParityPartiallyProved,
		CanaryRequired:              false,
		Limitation:                  "upstream ticker is aggregated to a documented 100 ms cadence",
		OperationalGateReportSHA256: addDigest("gate-partial", nil),
		SourceEvidence:              []ArtifactReference{addReference("partial-source", EvidenceSourceContract)},
		FixtureEvidence:             []ArtifactReference{addReference("partial-fixture", EvidenceFixture)},
		RawEvidence:                 []ArtifactReference{addReference("partial-raw", EvidenceRaw)},
		DatasetEvidence:             []ArtifactReference{addReference("partial-dataset", EvidenceDataset)},
		ReplayEvidence:              []ArtifactReference{addReference("partial-replay", EvidenceReplay)},
		CatalogEvidence:             []ArtifactReference{addReference("partial-catalog", EvidenceCatalog)},
	}
	unsupported := ParityRow{
		Tuple:          unsupportedTuple,
		Lifecycle:      LifecycleUnsupported,
		Status:         ParityUnsupported,
		CanaryRequired: false,
		Limitation:     "the public API does not expose a liquidation feed",
		SourceEvidence: []ArtifactReference{addReference("unsupported-official", EvidenceOfficialContract)},
	}
	ambiguous := ParityRow{
		Tuple:          ambiguousTuple,
		Lifecycle:      LifecycleAmbiguous,
		Status:         ParityAmbiguous,
		CanaryRequired: false,
		Limitation:     "the upstream summary does not define interval completeness",
		Ambiguity:      "primary documentation does not distinguish sampled from complete updates",
		SourceEvidence: []ArtifactReference{addReference("ambiguous-official", EvidenceOfficialContract)},
	}

	declarations := []TupleDeclaration{
		{Tuple: cloneTupleIdentity(provedTuple), Lifecycle: LifecycleSupported, CanaryRequired: true},
		{Tuple: cloneTupleIdentity(partialTuple), Lifecycle: LifecycleNormalized, CanaryRequired: false},
		{Tuple: cloneTupleIdentity(unsupportedTuple), Lifecycle: LifecycleUnsupported, CanaryRequired: false},
		{Tuple: cloneTupleIdentity(ambiguousTuple), Lifecycle: LifecycleAmbiguous, CanaryRequired: false},
	}
	return PlatformBundleInput{
		BuildIdentity: BuildInfo{
			Version: "platform-synthetic.v1",
			Commit:  "0123456789abcdef0123456789abcdef01234567",
			Date:    "2026-08-23T02:00:00Z",
		},
		HardwareIdentitySHA256: addDigest("hardware-identity", nil),
		WorkloadIdentitySHA256: addDigest("workload-identity", nil),
		X5EvidenceSHA256:       addDigest("x5-evidence", nil),
		Declarations:           declarations,
		Rows:                   []ParityRow{proved, partial, unsupported, ambiguous},
	}, inventory
}

func decodePlatformBundleForTest(t *testing.T, encoded []byte) PlatformBundle {
	t.Helper()
	var bundle PlatformBundle
	if err := json.Unmarshal(encoded, &bundle); err != nil {
		t.Fatal(err)
	}
	return bundle
}

func encodePlatformBundleForTest(t *testing.T, bundle PlatformBundle) []byte {
	t.Helper()
	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	return append(encoded, '\n')
}

func remarshalPlatformBundleForTest(t *testing.T, bundle PlatformBundle) []byte {
	t.Helper()
	encoded, err := marshalPlatformBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
