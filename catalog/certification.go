package catalog

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/capture"
)

const DeclarationCheckVersion uint16 = 1

var ErrInvalidDeclaration = errors.New("catalog: invalid declared source")

// TupleLifecycle is the evidence lifecycle of one exact declared source tuple.
// It is deliberately separate from ChannelContract.SupportState.
type TupleLifecycle string

const (
	LifecycleCandidate     TupleLifecycle = "candidate"
	LifecycleCaptured      TupleLifecycle = "captured"
	LifecycleReplayable    TupleLifecycle = "replayable"
	LifecycleNormalized    TupleLifecycle = "normalized"
	LifecycleReconstructed TupleLifecycle = "reconstructed"
	LifecycleSupported     TupleLifecycle = "supported"
	LifecycleUnsupported   TupleLifecycle = "unsupported"
	LifecycleAmbiguous     TupleLifecycle = "ambiguous"
)

var tupleLifecycles = [...]TupleLifecycle{
	LifecycleCandidate,
	LifecycleCaptured,
	LifecycleReplayable,
	LifecycleNormalized,
	LifecycleReconstructed,
	LifecycleSupported,
	LifecycleUnsupported,
	LifecycleAmbiguous,
}

// CoverageInterval is a UTC Unix nanosecond interval. EndNS is exclusive when
// present; nil means the interval is explicitly open-ended.
type CoverageInterval struct {
	StartNS int64  `json:"start_ns"`
	EndNS   *int64 `json:"end_ns"`
}

// LifecycleEvidence records the immutable evidence for one promotion. Entries
// are ordered by EffectiveAtNS and every promoted state must have an entry.
type LifecycleEvidence struct {
	State          TupleLifecycle `json:"state"`
	EffectiveAtNS  int64          `json:"effective_at_ns"`
	EvidenceSHA256 string         `json:"evidence_sha256"`
}

// DeclaredTuple carries the exact eight-dimensional tuple identity plus its
// current evidence state. State and transition evidence are not identity.
type DeclaredTuple struct {
	SourceID          string              `json:"source_id"`
	APIVersion        string              `json:"api_version"`
	Entitlement       string              `json:"entitlement"`
	ChannelOrEndpoint string              `json:"channel_or_endpoint"`
	DataFamily        string              `json:"data_family"`
	NativeGranularity string              `json:"native_granularity"`
	Coverage          CoverageInterval    `json:"coverage"`
	AdapterVersion    string              `json:"adapter_version"`
	State             TupleLifecycle      `json:"state"`
	Limitation        string              `json:"limitation"`
	TransitionHistory []LifecycleEvidence `json:"transition_history"`
}

// DeclaredSource groups caller-supplied tuples under one catalog source ID.
// Each tuple repeats SourceID so its identity remains self-contained.
type DeclaredSource struct {
	SourceID string          `json:"source_id"`
	Tuples   []DeclaredTuple `json:"tuples"`
}

// DeclarationCheckReport is a deterministic projection of every checked
// declaration. LifecycleCounts always contains all eight lifecycle keys.
type DeclarationCheckReport struct {
	Version          uint16                    `json:"version"`
	DeclarationCount int                       `json:"declaration_count"`
	TupleCount       int                       `json:"tuple_count"`
	LifecycleCounts  map[TupleLifecycle]uint64 `json:"lifecycle_counts"`
	TupleIDs         []string                  `json:"tuple_ids"`
	SHA256           string                    `json:"sha256"`
}

type tupleIdentity struct {
	SourceID          string           `json:"source_id"`
	APIVersion        string           `json:"api_version"`
	Entitlement       string           `json:"entitlement"`
	ChannelOrEndpoint string           `json:"channel_or_endpoint"`
	DataFamily        string           `json:"data_family"`
	NativeGranularity string           `json:"native_granularity"`
	Coverage          CoverageInterval `json:"coverage"`
	AdapterVersion    string           `json:"adapter_version"`
}

type tupleSeriesIdentity struct {
	SourceID          string `json:"source_id"`
	APIVersion        string `json:"api_version"`
	Entitlement       string `json:"entitlement"`
	ChannelOrEndpoint string `json:"channel_or_endpoint"`
	DataFamily        string `json:"data_family"`
	NativeGranularity string `json:"native_granularity"`
	AdapterVersion    string `json:"adapter_version"`
}

type declarationRecord struct {
	Identity          tupleIdentity       `json:"identity"`
	State             TupleLifecycle      `json:"state"`
	Limitation        string              `json:"limitation"`
	TransitionHistory []LifecycleEvidence `json:"transition_history"`
}

type lifecycleCount struct {
	State TupleLifecycle `json:"state"`
	Count uint64         `json:"count"`
}

type declarationHashBody struct {
	Version          uint16              `json:"version"`
	DeclarationCount int                 `json:"declaration_count"`
	TupleCount       int                 `json:"tuple_count"`
	LifecycleCounts  []lifecycleCount    `json:"lifecycle_counts"`
	TupleIDs         []string            `json:"tuple_ids"`
	Tuples           []declarationRecord `json:"tuples"`
}

type checkedDeclaration struct {
	record      declarationRecord
	identityKey string
	tupleID     string
}

// Validate rejects a tuple with an incomplete identity, invalid interval,
// unknown state, missing evidence, or illegal promotion history.
func (t DeclaredTuple) Validate() error {
	if !validDeclaredSourceID(t.SourceID) {
		return fmt.Errorf("%w: source_id must be bounded visible ASCII text", ErrInvalidDeclaration)
	}
	for name, value := range map[string]string{
		"api_version":         t.APIVersion,
		"entitlement":         t.Entitlement,
		"channel_or_endpoint": t.ChannelOrEndpoint,
		"data_family":         t.DataFamily,
		"native_granularity":  t.NativeGranularity,
		"adapter_version":     t.AdapterVersion,
		"limitation":          t.Limitation,
	} {
		if err := validateDeclarationText(name, value); err != nil {
			return err
		}
	}
	if t.Coverage.StartNS < 0 {
		return fmt.Errorf("%w: coverage start must be a UTC Unix nanosecond", ErrInvalidDeclaration)
	}
	if t.Coverage.EndNS != nil && *t.Coverage.EndNS <= t.Coverage.StartNS {
		return fmt.Errorf("%w: coverage end must be greater than start", ErrInvalidDeclaration)
	}
	if !validTupleLifecycle(t.State) {
		return fmt.Errorf("%w: unknown tuple state %q", ErrInvalidDeclaration, t.State)
	}
	if err := validateTransitionHistory(t); err != nil {
		return err
	}
	return nil
}

// Validate checks every tuple and temporal identity in one source declaration.
func (s DeclaredSource) Validate() error {
	_, err := CheckDeclarations([]DeclaredSource{s})
	return err
}

// CheckDeclarations validates all caller-supplied declarations and returns a
// stable evidence projection. The report hash covers the canonical report body
// and full declarations while excluding the SHA256 field itself.
func CheckDeclarations(declarations []DeclaredSource) (DeclarationCheckReport, error) {
	if len(declarations) == 0 {
		return DeclarationCheckReport{}, fmt.Errorf("%w: no source declarations", ErrInvalidDeclaration)
	}

	counts := make(map[TupleLifecycle]uint64, len(tupleLifecycles))
	for _, lifecycle := range tupleLifecycles {
		counts[lifecycle] = 0
	}

	checked := make([]checkedDeclaration, 0)
	seenSources := make(map[string]struct{}, len(declarations))
	seenTuples := make(map[string]struct{})
	series := make(map[string][]tupleIdentity)
	for sourceIndex, source := range declarations {
		if !validDeclaredSourceID(source.SourceID) {
			return DeclarationCheckReport{}, fmt.Errorf("%w: declaration %d source_id must be bounded visible ASCII text", ErrInvalidDeclaration, sourceIndex)
		}
		if _, exists := seenSources[source.SourceID]; exists {
			return DeclarationCheckReport{}, fmt.Errorf("%w: duplicate source declaration %q", ErrInvalidDeclaration, source.SourceID)
		}
		seenSources[source.SourceID] = struct{}{}
		if len(source.Tuples) == 0 {
			return DeclarationCheckReport{}, fmt.Errorf("%w: source %q has no tuples", ErrInvalidDeclaration, source.SourceID)
		}
		for tupleIndex, tuple := range source.Tuples {
			if tuple.SourceID != source.SourceID {
				return DeclarationCheckReport{}, fmt.Errorf("%w: source %q tuple %d carries source_id %q", ErrInvalidDeclaration,
					source.SourceID, tupleIndex, tuple.SourceID)
			}
			if err := tuple.Validate(); err != nil {
				return DeclarationCheckReport{}, fmt.Errorf("source %q tuple %d: %w", source.SourceID, tupleIndex, err)
			}
			identity := identityOf(tuple)
			identityBytes, err := json.Marshal(identity)
			if err != nil {
				return DeclarationCheckReport{}, fmt.Errorf("%w: encode tuple identity: %v", ErrInvalidDeclaration, err)
			}
			identityKey := string(identityBytes)
			id := hashHex(identityBytes)
			if _, exists := seenTuples[identityKey]; exists {
				return DeclarationCheckReport{}, fmt.Errorf("%w: duplicate exact tuple %q", ErrInvalidDeclaration, id)
			}
			seenTuples[identityKey] = struct{}{}
			seriesBytes, err := json.Marshal(seriesIdentityOf(tuple))
			if err != nil {
				return DeclarationCheckReport{}, fmt.Errorf("%w: encode tuple series identity: %v", ErrInvalidDeclaration, err)
			}
			series[string(seriesBytes)] = append(series[string(seriesBytes)], identity)
			counts[tuple.State]++
			checked = append(checked, checkedDeclaration{
				record: declarationRecord{
					Identity: identity, State: tuple.State, Limitation: tuple.Limitation,
					TransitionHistory: slices.Clone(tuple.TransitionHistory),
				},
				identityKey: identityKey,
				tupleID:     id,
			})
		}
	}

	for _, intervals := range series {
		slices.SortFunc(intervals, compareTupleIntervals)
		for i := 1; i < len(intervals); i++ {
			previous := intervals[i-1].Coverage
			current := intervals[i].Coverage
			if previous.EndNS == nil || current.StartNS < *previous.EndNS {
				return DeclarationCheckReport{}, fmt.Errorf("%w: overlapping coverage intervals for one tuple series", ErrInvalidDeclaration)
			}
		}
	}

	slices.SortFunc(checked, func(a, b checkedDeclaration) int {
		return strings.Compare(a.identityKey, b.identityKey)
	})
	records := make([]declarationRecord, len(checked))
	tupleIDs := make([]string, len(checked))
	for i, declaration := range checked {
		records[i] = declaration.record
		tupleIDs[i] = declaration.tupleID
	}
	slices.Sort(tupleIDs)
	orderedCounts := make([]lifecycleCount, len(tupleLifecycles))
	for i, lifecycle := range tupleLifecycles {
		orderedCounts[i] = lifecycleCount{State: lifecycle, Count: counts[lifecycle]}
	}
	body, err := json.Marshal(declarationHashBody{
		Version: DeclarationCheckVersion, DeclarationCount: len(declarations), TupleCount: len(records),
		LifecycleCounts: orderedCounts, TupleIDs: tupleIDs, Tuples: records,
	})
	if err != nil {
		return DeclarationCheckReport{}, fmt.Errorf("%w: encode declaration evidence: %v", ErrInvalidDeclaration, err)
	}
	return DeclarationCheckReport{
		Version:          DeclarationCheckVersion,
		DeclarationCount: len(declarations),
		TupleCount:       len(records),
		LifecycleCounts:  counts,
		TupleIDs:         tupleIDs,
		SHA256:           hashHex(body),
	}, nil
}

func validateTransitionHistory(tuple DeclaredTuple) error {
	if len(tuple.TransitionHistory) == 0 {
		return fmt.Errorf("%w: transition history is empty", ErrInvalidDeclaration)
	}
	if tuple.TransitionHistory[0].State != LifecycleCandidate || tuple.TransitionHistory[0].EffectiveAtNS != tuple.Coverage.StartNS {
		return fmt.Errorf("%w: transition history must begin with candidate at coverage start", ErrInvalidDeclaration)
	}
	for i, evidence := range tuple.TransitionHistory {
		if !validTupleLifecycle(evidence.State) {
			return fmt.Errorf("%w: transition %d has unknown state %q", ErrInvalidDeclaration, i, evidence.State)
		}
		if !validSHA256(evidence.EvidenceSHA256) {
			return fmt.Errorf("%w: transition %d has invalid evidence SHA-256", ErrInvalidDeclaration, i)
		}
		if evidence.EffectiveAtNS < tuple.Coverage.StartNS ||
			(tuple.Coverage.EndNS != nil && evidence.EffectiveAtNS >= *tuple.Coverage.EndNS) {
			return fmt.Errorf("%w: transition %d falls outside tuple coverage", ErrInvalidDeclaration, i)
		}
		if i == 0 {
			continue
		}
		previous := tuple.TransitionHistory[i-1]
		if evidence.EffectiveAtNS < previous.EffectiveAtNS {
			return fmt.Errorf("%w: transition effective times must be monotonic", ErrInvalidDeclaration)
		}
		if !legalLifecycleTransition(previous.State, evidence.State) {
			return fmt.Errorf("%w: illegal tuple lifecycle transition %q to %q", ErrInvalidDeclaration,
				previous.State, evidence.State)
		}
	}
	if tuple.TransitionHistory[len(tuple.TransitionHistory)-1].State != tuple.State {
		return fmt.Errorf("%w: current state %q does not match transition history", ErrInvalidDeclaration, tuple.State)
	}
	return nil
}

func legalLifecycleTransition(from, to TupleLifecycle) bool {
	if from == LifecycleUnsupported || from == LifecycleAmbiguous {
		return false
	}
	if to == LifecycleUnsupported || to == LifecycleAmbiguous {
		return true
	}
	for i := 0; i < len(tupleLifecycles)-3; i++ {
		if from == tupleLifecycles[i] {
			return to == tupleLifecycles[i+1]
		}
	}
	return false
}

func validTupleLifecycle(value TupleLifecycle) bool {
	return slices.Contains(tupleLifecycles[:], value)
}

func identityOf(tuple DeclaredTuple) tupleIdentity {
	return tupleIdentity{
		SourceID: tuple.SourceID, APIVersion: tuple.APIVersion, Entitlement: tuple.Entitlement,
		ChannelOrEndpoint: tuple.ChannelOrEndpoint, DataFamily: tuple.DataFamily,
		NativeGranularity: tuple.NativeGranularity, Coverage: tuple.Coverage, AdapterVersion: tuple.AdapterVersion,
	}
}

func seriesIdentityOf(tuple DeclaredTuple) tupleSeriesIdentity {
	return tupleSeriesIdentity{
		SourceID: tuple.SourceID, APIVersion: tuple.APIVersion, Entitlement: tuple.Entitlement,
		ChannelOrEndpoint: tuple.ChannelOrEndpoint, DataFamily: tuple.DataFamily,
		NativeGranularity: tuple.NativeGranularity, AdapterVersion: tuple.AdapterVersion,
	}
}

func compareTupleIntervals(a, b tupleIdentity) int {
	if a.Coverage.StartNS < b.Coverage.StartNS {
		return -1
	}
	if a.Coverage.StartNS > b.Coverage.StartNS {
		return 1
	}
	if a.Coverage.EndNS == nil {
		if b.Coverage.EndNS == nil {
			return 0
		}
		return 1
	}
	if b.Coverage.EndNS == nil {
		return -1
	}
	if *a.Coverage.EndNS < *b.Coverage.EndNS {
		return -1
	}
	if *a.Coverage.EndNS > *b.Coverage.EndNS {
		return 1
	}
	return 0
}

func validateDeclarationText(name, value string) error {
	if err := validateCatalogText(name, value); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidDeclaration, err)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: %s must be nonblank without surrounding whitespace", ErrInvalidDeclaration, name)
	}
	return nil
}

func validDeclaredSourceID(value string) bool {
	if value == "" || len(value) > capture.MaxSourceIDBytes {
		return false
	}
	for _, character := range value {
		if character < 0x21 || character > 0x7e {
			return false
		}
	}
	return true
}

func validSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || value != strings.ToLower(value) {
		return false
	}
	for _, b := range decoded {
		if b != 0 {
			return true
		}
	}
	return false
}

func hashHex(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
