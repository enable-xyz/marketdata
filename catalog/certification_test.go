package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
)

func TestCheckDeclarationsValidCompleteLifecycleVocabulary(t *testing.T) {
	declarations := syntheticDeclarations()
	report, err := CheckDeclarations(declarations)
	if err != nil {
		t.Fatalf("CheckDeclarations() error = %v", err)
	}
	if report.Version != DeclarationCheckVersion || report.DeclarationCount != 2 || report.TupleCount != len(tupleLifecycles) {
		t.Fatalf("report identity and counts = %+v", report)
	}
	if len(report.LifecycleCounts) != len(tupleLifecycles) {
		t.Fatalf("lifecycle count keys = %d, want %d", len(report.LifecycleCounts), len(tupleLifecycles))
	}
	for _, state := range tupleLifecycles {
		if report.LifecycleCounts[state] != 1 {
			t.Fatalf("lifecycle count %q = %d, want 1", state, report.LifecycleCounts[state])
		}
	}
	if len(report.TupleIDs) != len(tupleLifecycles) || !slices.IsSorted(report.TupleIDs) {
		t.Fatalf("tuple IDs are not complete and sorted: %v", report.TupleIDs)
	}
	if !validSHA256(report.SHA256) {
		t.Fatalf("report SHA-256 = %q", report.SHA256)
	}
	for _, id := range report.TupleIDs {
		if !validSHA256(id) {
			t.Fatalf("tuple ID = %q", id)
		}
	}
}

func TestCheckDeclarationsRejectsDuplicateExactTuple(t *testing.T) {
	declarations := syntheticDeclarations()
	declarations[0].Tuples = append(declarations[0].Tuples, declarations[0].Tuples[0])
	assertInvalidDeclaration(t, declarations)
}

func TestCheckDeclarationsRejectsInvalidCoverage(t *testing.T) {
	t.Run("inverted", func(t *testing.T) {
		declarations := cloneDeclarations(syntheticDeclarations())
		end := declarations[0].Tuples[0].Coverage.StartNS
		declarations[0].Tuples[0].Coverage.EndNS = &end
		assertInvalidDeclaration(t, declarations)
	})

	t.Run("finite overlap", func(t *testing.T) {
		sourceID := "10000000-0000-0000-0000-000000000001"
		first := syntheticTuple(sourceID, "trades", LifecycleCandidate)
		first.Coverage = finiteCoverage(1_000, 2_000)
		first.TransitionHistory = lifecycleHistory(first.ChannelOrEndpoint, first.Coverage.StartNS, first.State)
		second := syntheticTuple(sourceID, "trades", LifecycleCandidate)
		second.Coverage = finiteCoverage(1_500, 2_500)
		second.TransitionHistory = lifecycleHistory(second.ChannelOrEndpoint, second.Coverage.StartNS, second.State)
		assertInvalidDeclaration(t, []DeclaredSource{{SourceID: sourceID, Tuples: []DeclaredTuple{first, second}}})
	})

	t.Run("open overlap", func(t *testing.T) {
		sourceID := "10000000-0000-0000-0000-000000000001"
		first := syntheticTuple(sourceID, "book", LifecycleCandidate)
		first.Coverage = CoverageInterval{StartNS: 1_000}
		first.TransitionHistory = lifecycleHistory(first.ChannelOrEndpoint, first.Coverage.StartNS, first.State)
		second := syntheticTuple(sourceID, "book", LifecycleCandidate)
		second.Coverage = finiteCoverage(2_000, 3_000)
		second.TransitionHistory = lifecycleHistory(second.ChannelOrEndpoint, second.Coverage.StartNS, second.State)
		assertInvalidDeclaration(t, []DeclaredSource{{SourceID: sourceID, Tuples: []DeclaredTuple{first, second}}})
	})
}

func TestCheckDeclarationsRejectsIllegalLifecycleHistory(t *testing.T) {
	sourceID := "10000000-0000-0000-0000-000000000001"
	tests := []struct {
		name    string
		current TupleLifecycle
		states  []TupleLifecycle
	}{
		{name: "regression", current: LifecycleCaptured, states: []TupleLifecycle{LifecycleCandidate, LifecycleCaptured, LifecycleReplayable, LifecycleCaptured}},
		{name: "missing promotion evidence", current: LifecycleNormalized, states: []TupleLifecycle{LifecycleCandidate, LifecycleNormalized}},
		{name: "unsupported terminal", current: LifecycleAmbiguous, states: []TupleLifecycle{LifecycleCandidate, LifecycleUnsupported, LifecycleAmbiguous}},
		{name: "ambiguous terminal", current: LifecycleCaptured, states: []TupleLifecycle{LifecycleCandidate, LifecycleAmbiguous, LifecycleCaptured}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tuple := syntheticTuple(sourceID, "trades", test.current)
			tuple.TransitionHistory = explicitHistory(tuple.ChannelOrEndpoint, tuple.Coverage.StartNS, test.states)
			assertInvalidDeclaration(t, []DeclaredSource{{SourceID: sourceID, Tuples: []DeclaredTuple{tuple}}})
		})
	}

	for _, evidenceHash := range []string{"", hex.EncodeToString(make([]byte, sha256.Size))} {
		t.Run("blank evidence", func(t *testing.T) {
			tuple := syntheticTuple(sourceID, "trades", LifecycleCandidate)
			tuple.TransitionHistory[0].EvidenceSHA256 = evidenceHash
			assertInvalidDeclaration(t, []DeclaredSource{{SourceID: sourceID, Tuples: []DeclaredTuple{tuple}}})
		})
	}
}

func TestCheckDeclarationsRejectsTerminalReasonOmissions(t *testing.T) {
	for _, state := range []TupleLifecycle{LifecycleUnsupported, LifecycleAmbiguous} {
		t.Run(string(state), func(t *testing.T) {
			declarations := []DeclaredSource{{
				SourceID: "10000000-0000-0000-0000-000000000001",
				Tuples:   []DeclaredTuple{syntheticTuple("10000000-0000-0000-0000-000000000001", "trades", state)},
			}}
			declarations[0].Tuples[0].Limitation = " "
			assertInvalidDeclaration(t, declarations)
		})
	}
}

func TestCheckDeclarationsRejectsBlankDimensions(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*DeclaredTuple)
	}{
		{name: "source ID", mutate: func(tuple *DeclaredTuple) { tuple.SourceID = "" }},
		{name: "API version", mutate: func(tuple *DeclaredTuple) { tuple.APIVersion = " " }},
		{name: "entitlement", mutate: func(tuple *DeclaredTuple) { tuple.Entitlement = "" }},
		{name: "channel or endpoint", mutate: func(tuple *DeclaredTuple) { tuple.ChannelOrEndpoint = "\t" }},
		{name: "data family", mutate: func(tuple *DeclaredTuple) { tuple.DataFamily = "" }},
		{name: "native granularity", mutate: func(tuple *DeclaredTuple) { tuple.NativeGranularity = " " }},
		{name: "adapter version", mutate: func(tuple *DeclaredTuple) { tuple.AdapterVersion = "" }},
		{name: "limitation", mutate: func(tuple *DeclaredTuple) { tuple.Limitation = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			declarations := cloneDeclarations(syntheticDeclarations())
			test.mutate(&declarations[0].Tuples[0])
			assertInvalidDeclaration(t, declarations)
		})
	}
}

func TestCheckDeclarationsRejectsUnsafeSourceIDs(t *testing.T) {
	tests := map[string]string{
		"blank":      "",
		"whitespace": "bybit v5 spot",
		"control":    "bybit\nv5",
		"oversized":  strings.Repeat("s", capture.MaxSourceIDBytes+1),
	}
	for name, sourceID := range tests {
		t.Run(name, func(t *testing.T) {
			declarations := cloneDeclarations(syntheticDeclarations())
			declarations[0].SourceID = sourceID
			for i := range declarations[0].Tuples {
				declarations[0].Tuples[i].SourceID = sourceID
			}
			assertInvalidDeclaration(t, declarations)
		})
	}
}

func TestCheckDeclarationsHashDeterministicUnderInputReorder(t *testing.T) {
	firstInput := syntheticDeclarations()
	secondInput := cloneDeclarations(firstInput)
	slices.Reverse(secondInput)
	for i := range secondInput {
		slices.Reverse(secondInput[i].Tuples)
	}
	first, err := CheckDeclarations(firstInput)
	if err != nil {
		t.Fatalf("first CheckDeclarations() error = %v", err)
	}
	second, err := CheckDeclarations(secondInput)
	if err != nil {
		t.Fatalf("reordered CheckDeclarations() error = %v", err)
	}
	if first.SHA256 != second.SHA256 {
		t.Fatalf("report hash changed under reorder: %s != %s", first.SHA256, second.SHA256)
	}
	if !slices.Equal(first.TupleIDs, second.TupleIDs) {
		t.Fatalf("tuple IDs changed under reorder: %v != %v", first.TupleIDs, second.TupleIDs)
	}
	for _, state := range tupleLifecycles {
		if first.LifecycleCounts[state] != second.LifecycleCounts[state] {
			t.Fatalf("lifecycle count %q changed under reorder", state)
		}
	}
}

func syntheticDeclarations() []DeclaredSource {
	states := tupleLifecycles[:]
	sourceIDs := []string{
		"bybit-v5-spot-public",
		"okx-v5-public-rest",
	}
	declarations := make([]DeclaredSource, len(sourceIDs))
	for i, sourceID := range sourceIDs {
		declarations[i].SourceID = sourceID
		for j := range 4 {
			stateIndex := i*4 + j
			channel := "synthetic.channel." + string(rune('a'+stateIndex))
			declarations[i].Tuples = append(declarations[i].Tuples, syntheticTuple(sourceID, channel, states[stateIndex]))
		}
	}
	declarations[1].Tuples[3].Coverage.EndNS = nil
	return declarations
}

func syntheticTuple(sourceID, channel string, state TupleLifecycle) DeclaredTuple {
	const startNS int64 = 1_000
	tuple := DeclaredTuple{
		SourceID:          sourceID,
		APIVersion:        "synthetic-v1",
		Entitlement:       "public",
		ChannelOrEndpoint: channel,
		DataFamily:        "synthetic-events",
		NativeGranularity: "event",
		Coverage:          finiteCoverage(startNS, 10_000),
		AdapterVersion:    "adapter-v1",
		State:             state,
		Limitation:        "synthetic declaration proves only the bounded fixture contract",
	}
	tuple.TransitionHistory = lifecycleHistory(channel, startNS, state)
	return tuple
}

func lifecycleHistory(channel string, startNS int64, current TupleLifecycle) []LifecycleEvidence {
	if current == LifecycleUnsupported || current == LifecycleAmbiguous {
		return explicitHistory(channel, startNS, []TupleLifecycle{LifecycleCandidate, current})
	}
	progression := tupleLifecycles[:6]
	index := slices.Index(progression, current)
	return explicitHistory(channel, startNS, progression[:index+1])
}

func explicitHistory(channel string, startNS int64, states []TupleLifecycle) []LifecycleEvidence {
	history := make([]LifecycleEvidence, len(states))
	for i, state := range states {
		history[i] = LifecycleEvidence{
			State:          state,
			EffectiveAtNS:  startNS + int64(i),
			EvidenceSHA256: syntheticEvidenceHash(channel + "/" + string(state)),
		}
	}
	return history
}

func syntheticEvidenceHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func finiteCoverage(startNS, endNS int64) CoverageInterval {
	return CoverageInterval{StartNS: startNS, EndNS: &endNS}
}

func cloneDeclarations(input []DeclaredSource) []DeclaredSource {
	cloned := slices.Clone(input)
	for i := range cloned {
		cloned[i].Tuples = slices.Clone(input[i].Tuples)
		for j := range cloned[i].Tuples {
			cloned[i].Tuples[j].TransitionHistory = slices.Clone(input[i].Tuples[j].TransitionHistory)
			if input[i].Tuples[j].Coverage.EndNS != nil {
				end := *input[i].Tuples[j].Coverage.EndNS
				cloned[i].Tuples[j].Coverage.EndNS = &end
			}
		}
	}
	return cloned
}

func assertInvalidDeclaration(t *testing.T, declarations []DeclaredSource) {
	t.Helper()
	if _, err := CheckDeclarations(declarations); !errors.Is(err, ErrInvalidDeclaration) {
		t.Fatalf("CheckDeclarations() error = %v, want ErrInvalidDeclaration", err)
	}
}
