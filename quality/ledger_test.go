package quality

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"
)

func TestOpportunityRules(t *testing.T) {
	defensible := []OpportunityExpectation{
		OpportunityScheduledRESTPoll, OpportunityAcknowledgementDeadline,
		OpportunityHeartbeatDeadline, OpportunityPeriodicPublication,
		OpportunitySubscriptionInventory, OpportunitySequenceInterval,
		OpportunityMetadataDiscovery, OpportunityNativeFilePublication,
	}
	for index, expectation := range defensible {
		row := testOpportunity(index+1, expectation, OpportunityOutcomeObserved)
		if err := row.Validate(); err != nil {
			t.Fatalf("%s Validate() error = %v", expectation, err)
		}
		first, err := row.LogicalHash()
		if err != nil {
			t.Fatalf("%s LogicalHash() error = %v", expectation, err)
		}
		second, err := row.LogicalHash()
		if err != nil || first != second {
			t.Fatalf("%s logical hash is not deterministic", expectation)
		}
	}
	for _, expectation := range []OpportunityExpectation{
		OpportunityStochasticTradeSilence,
		OpportunityChangeTriggeredBBOSilence,
		OpportunityUndocumentedTickerSilence,
	} {
		row := testOpportunity(20, expectation, OpportunityOutcomeUnknown)
		if err := row.Validate(); !errors.Is(err, ErrNoDefensibleDenominator) {
			t.Fatalf("%s error = %v, want ErrNoDefensibleDenominator", expectation, err)
		}
	}
	for outcome := OpportunityOutcomeObserved; outcome <= OpportunityOutcomeUnknown; outcome++ {
		row := testOpportunity(int(outcome)+40, OpportunityScheduledRESTPoll, outcome)
		if err := row.Validate(); err != nil {
			t.Fatalf("closed outcome %s error = %v", outcome, err)
		}
	}
	base := testOpportunity(70, OpportunityScheduledRESTPoll, OpportunityOutcomeObserved)
	changed := base
	changed.TerminalOutcome = OpportunityOutcomeUnknown
	baseHash, _ := base.LogicalHash()
	changedHash, _ := changed.LogicalHash()
	if baseHash == changedHash {
		t.Fatal("terminal outcome did not participate in immutable logical identity")
	}
}

func TestOpportunitySpill(t *testing.T) {
	request := testSpillRequest("generation-1")
	t.Run("archive failure before manifest commit keeps PostgreSQL authoritative", func(t *testing.T) {
		store := newFakeSpillStore(t)
		writer := &fakeArchiveWriter{failBefore: true}
		_, err := ExecuteOpportunitySpill(t.Context(), store, writer, request)
		if !errors.Is(err, ErrUnknownSpillOutcome) {
			t.Fatalf("spill error = %v, want ErrUnknownSpillOutcome", err)
		}
		if len(store.rows) != 2 || store.generation.State != SpillPending {
			t.Fatalf("pre-commit failure changed authority: rows=%d state=%s", len(store.rows), store.generation.State)
		}
	})

	t.Run("lost manifest response reconciles by generation", func(t *testing.T) {
		store := newFakeSpillStore(t)
		writer := &fakeArchiveWriter{failAfter: true}
		generation, err := ExecuteOpportunitySpill(t.Context(), store, writer, request)
		if err != nil {
			t.Fatalf("spill error = %v", err)
		}
		if generation.State != SpillCommitted || len(store.rows) != 0 || writer.writes != 1 {
			t.Fatalf("lost manifest response result: state=%s rows=%d writes=%d", generation.State, len(store.rows), writer.writes)
		}
	})

	t.Run("lost PostgreSQL commit response reconciles without second archive", func(t *testing.T) {
		store := newFakeSpillStore(t)
		store.failCommitAfter = true
		writer := &fakeArchiveWriter{}
		generation, err := ExecuteOpportunitySpill(t.Context(), store, writer, request)
		if err != nil {
			t.Fatalf("spill error = %v", err)
		}
		if generation.State != SpillCommitted || len(store.rows) != 0 || writer.writes != 1 || store.commits != 1 {
			t.Fatalf("lost commit response result: state=%s rows=%d writes=%d commits=%d", generation.State, len(store.rows), writer.writes, store.commits)
		}
	})

	t.Run("crash after commit retains overlap until idempotent delete", func(t *testing.T) {
		store := newFakeSpillStore(t)
		store.failDeleteOnce = true
		writer := &fakeArchiveWriter{}
		if _, err := ExecuteOpportunitySpill(t.Context(), store, writer, request); err == nil {
			t.Fatal("spill unexpectedly survived injected post-commit delete failure")
		}
		if store.generation.State != SpillCommitted || len(store.rows) != 2 {
			t.Fatalf("post-commit crash state = %s rows=%d", store.generation.State, len(store.rows))
		}
		if _, err := ExecuteOpportunitySpill(t.Context(), store, writer, request); err != nil {
			t.Fatalf("retry error = %v", err)
		}
		if len(store.rows) != 0 || writer.writes != 1 || store.deletes != 1 {
			t.Fatalf("retry exact-once result rows=%d writes=%d deletes=%d", len(store.rows), writer.writes, store.deletes)
		}
	})

	t.Run("generation retry preserves exact row membership", func(t *testing.T) {
		store := newFakeSpillStore(t)
		first, err := store.BeginOpportunitySpill(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		second, err := store.BeginOpportunitySpill(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if first.Fingerprint != second.Fingerprint || len(first.RowIdentities) != len(second.RowIdentities) {
			t.Fatalf("generation retry changed membership: first=%+v second=%+v", first, second)
		}
	})
}

func TestCoverageUnion(t *testing.T) {
	first := testOpportunity(1, OpportunityScheduledRESTPoll, OpportunityOutcomeObserved)
	second := testOpportunity(2, OpportunityHeartbeatDeadline, OpportunityOutcomeSourceStale)
	preSpill, err := RecomputeCoverage([]Opportunity{first, second}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	generation, err := NewSpillGeneration(testSpillRequest("coverage-generation"), []Opportunity{first})
	if err != nil {
		t.Fatal(err)
	}
	commit := testArchiveCommit(generation)
	generation.State, generation.Archive = SpillCommitted, &commit
	union, err := RecomputeCoverage([]Opportunity{first, second}, []OpportunityArchive{{Generation: generation, Rows: []Opportunity{first}}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertCoverageEqual(t, preSpill, union)

	conflict := first
	conflict.TerminalOutcome = OpportunityOutcomeUnknown
	if _, err := RecomputeCoverage([]Opportunity{first}, []OpportunityArchive{{Generation: generation, Rows: []Opportunity{conflict}}}, nil); !errors.Is(err, ErrInvalidSpill) {
		t.Fatalf("tampered archive error = %v, want ErrInvalidSpill", err)
	}
	pendingGeneration, err := NewSpillGeneration(testSpillRequest("pending-generation"), []Opportunity{second})
	if err != nil {
		t.Fatal(err)
	}
	pending := OpportunityArchive{Generation: pendingGeneration, Rows: []Opportunity{second}}
	if _, err := RecomputeCoverage([]Opportunity{first}, []OpportunityArchive{pending}, nil); !errors.Is(err, ErrInvalidSpill) {
		t.Fatalf("pending archive error = %v, want ErrInvalidSpill", err)
	}
	missing := OpportunityArchive{Generation: generation}
	if _, err := RecomputeCoverage(nil, []OpportunityArchive{missing}, nil); !errors.Is(err, ErrInvalidSpill) {
		t.Fatalf("missing archive membership error = %v, want ErrInvalidSpill", err)
	}
	quarantinedGeneration := pendingGeneration
	quarantinedGeneration.State = SpillQuarantined
	quarantinedGeneration.QuarantineReason = "synthetic verification failure"
	if _, err := RecomputeCoverage(nil, []OpportunityArchive{{
		Generation: quarantinedGeneration, Rows: []Opportunity{second},
	}}, nil); !errors.Is(err, ErrInvalidSpill) {
		t.Fatalf("quarantined archive error = %v, want ErrInvalidSpill", err)
	}
	badAggregateGeneration := generation
	badAggregateCommit := *generation.Archive
	badAggregateCommit.LogicalHash = sha256.Sum256([]byte("wrong aggregate"))
	badAggregateGeneration.Archive = &badAggregateCommit
	if _, err := RecomputeCoverage(nil, []OpportunityArchive{{
		Generation: badAggregateGeneration, Rows: []Opportunity{first},
	}}, nil); !errors.Is(err, ErrInvalidSpill) {
		t.Fatalf("aggregate mismatch error = %v, want ErrInvalidSpill", err)
	}
}

func TestGapLifecycle(t *testing.T) {
	gap, err := NewGap(Gap{GapID: "gap-1", SourceID: "source-1", ChannelID: "book", RangeStartNS: 10, RangeEndNS: 20,
		DetectionBasis: "sequence_interval", FirstGoodCoordinate: json.RawMessage(`{"ordinal":1}`),
		LastGoodCoordinate: json.RawMessage(`{"ordinal":2}`), AffectedFamilies: []string{"book"},
		Confidence: 1, Evidence: json.RawMessage(`{"sequence_start":10}`), DetectedTimeNS: 21})
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := TransitionGap(gap, GapRecoveredCurrentState, 30)
	if err != nil || recovered.State != GapRecoveredCurrentState || recovered.RangeStartNS != gap.RangeStartNS || recovered.RangeEndNS != gap.RangeEndNS {
		t.Fatalf("recovery = %+v, error=%v", recovered, err)
	}
	if _, err := TransitionGap(gap, GapBackfilledExplicitly, 30); !errors.Is(err, ErrReservedGapState) {
		t.Fatalf("reserved transition error = %v", err)
	}
	if _, err := TransitionGap(recovered, GapPermanent, 40); !errors.Is(err, ErrInvalidGapTransition) {
		t.Fatalf("terminal transition error = %v", err)
	}
	permanent, err := TransitionGap(gap, GapPermanent, 30)
	if err != nil || permanent.State != GapPermanent {
		t.Fatalf("permanent transition = %+v, error=%v", permanent, err)
	}
}

func TestIncidentImmutability(t *testing.T) {
	row := testOpportunity(1, OpportunityScheduledRESTPoll, OpportunityOutcomeObserved)
	before, err := RecomputeCoverage([]Opportunity{row}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	incident, err := NewIncident(IncidentInput{IncidentID: "incident-1", Annotation: "synthetic outage report",
		ReportSource: "synthetic-fixture", AffectedTuples: json.RawMessage(`[{"channel":"poll"}]`),
		HasRange: true, RangeStartNS: 10, RangeEndNS: 20, ReportedTimeNS: 30, CreatedTimeNS: 31})
	if err != nil {
		t.Fatal(err)
	}
	after, err := RecomputeCoverage([]Opportunity{row}, nil, []Incident{incident})
	if err != nil {
		t.Fatal(err)
	}
	if before.Opportunities != after.Opportunities || before.Counts[0].Outcomes[OpportunityOutcomeObserved] != after.Counts[0].Outcomes[OpportunityOutcomeObserved] {
		t.Fatalf("incident changed opportunity outcomes: before=%+v after=%+v", before, after)
	}
	copy := incident.AffectedTuples()
	copy[0] = '{'
	if string(incident.AffectedTuples()) != `[{"channel":"poll"}]` {
		t.Fatal("incident exposed mutable affected tuple storage")
	}
}

func TestSchemaDrift(t *testing.T) {
	left, leftSample, err := StructuralFingerprint([]byte(`{"b":[1,null],"a":"1"}`))
	if err != nil {
		t.Fatal(err)
	}
	right, rightSample, err := StructuralFingerprint([]byte(`{"a":"different","b":[2,null]}`))
	if err != nil {
		t.Fatal(err)
	}
	if left != right || string(leftSample) != string(rightSample) {
		t.Fatalf("key/value-only change altered structural fingerprint: %x/%s != %x/%s", left, leftSample, right, rightSample)
	}
	numeric, _, _ := StructuralFingerprint([]byte(`{"a":1}`))
	textual, _, _ := StructuralFingerprint([]byte(`{"a":"1"}`))
	if numeric == textual {
		t.Fatal("numeric/string representation collapsed")
	}
	semantic, err := SchemaDriftAction(SchemaSemanticChange, true, true)
	if err != nil || !semantic.RawAccepted || semantic.Normalization != NormalizationFailClosed {
		t.Fatalf("semantic action = %+v, error=%v", semantic, err)
	}
	unknown, err := SchemaDriftAction(SchemaUnknownMessageRole, true, true)
	if err != nil || !unknown.RawAccepted || unknown.Normalization != NormalizationQuarantine {
		t.Fatalf("unknown role action = %+v, error=%v", unknown, err)
	}
	optional, err := SchemaDriftAction(SchemaKnownOptionalFieldAbsent, false, false)
	if err != nil || optional.Normalization != NormalizationQuarantine {
		t.Fatalf("unresolved optional absence action = %+v, error=%v", optional, err)
	}
	fingerprint, sample, err := StructuralFingerprint([]byte(`{"field":"value"}`))
	if err != nil {
		t.Fatal(err)
	}
	observation := SchemaObservation{
		SourceID: "source-1", ChannelID: "channel-1", Fingerprint: fingerprint,
		FirstSeenTimeNS: 1, LastSeenTimeNS: 1, ObservationCount: 1,
		Classification: SchemaSemanticChange, ParserCanPreserve: true, OptionalAbsenceResolved: true,
		FirstRawCoordinate: json.RawMessage(`{"ordinal":1}`), LastRawCoordinate: json.RawMessage(`{"ordinal":1}`),
		RedactedSample: sample, MapperDisposition: NormalizationContinue,
	}
	if err := observation.Validate(); !errors.Is(err, ErrInvalidSchemaObservation) {
		t.Fatalf("semantic continue validation error = %v, want ErrInvalidSchemaObservation", err)
	}
	observation.MapperDisposition = NormalizationFailClosed
	if err := observation.Validate(); err != nil {
		t.Fatalf("semantic fail-closed validation error = %v", err)
	}
	observation.Classification = SchemaKnownOptionalFieldAbsent
	observation.OptionalAbsenceResolved = false
	observation.MapperDisposition = NormalizationTransition
	if err := observation.Validate(); !errors.Is(err, ErrInvalidSchemaObservation) {
		t.Fatalf("unresolved optional transition error = %v, want ErrInvalidSchemaObservation", err)
	}
}

func testOpportunity(index int, expectation OpportunityExpectation, outcome OpportunityOutcome) Opportunity {
	return Opportunity{OpportunityID: "opportunity-" + string(rune('a'+index)), LedgerPartition: "quality-v1",
		SourceID: "source-1", ChannelID: "channel-1", Expectation: expectation,
		ExpectedTimeNS: int64(100 + index), WindowStartNS: 100, WindowEndNS: 1000,
		Scope: json.RawMessage(`{"contract":"synthetic-v1"}`), Terminal: true,
		TerminalTimeNS: int64(200 + index), TerminalOutcome: outcome,
		TerminalEvidence: json.RawMessage(`{"raw":"synthetic-coordinate"}`), CreatedTimeNS: int64(90 + index)}
}

func testSpillRequest(generationID string) SpillRequest {
	return SpillRequest{
		GenerationID: generationID, Partition: "quality-v1", ThroughTimeNS: 1000, MaximumRows: 10,
		CatalogSnapshotHash: sha256.Sum256([]byte("synthetic-catalog-snapshot")),
		MapperSetHash:       sha256.Sum256([]byte("synthetic-mapper-set")),
	}
}

func testArchiveCommit(generation SpillGeneration) ArchiveCommit {
	digest := sha256.Sum256([]byte(generation.GenerationID))
	logicalHash, _ := OpportunityArchiveLogicalHash(generation.Rows)
	return ArchiveCommit{GenerationID: generation.GenerationID, GenerationFingerprint: generation.Fingerprint,
		DatasetPartitionID: "dataset-" + generation.GenerationID, ManifestHash: digest,
		ManifestPath: "quality/manifest.json", ParquetPath: "quality/part.parquet",
		DatasetVersion: "parquet-dataset-v1", PartitionKey: "quality/v1", RangeStartNS: generation.From.TerminalTimeNS,
		RangeEndNS: generation.Through.TerminalTimeNS, InputSetHash: digest, CatalogSnapshotHash: generation.CatalogSnapshotHash,
		MapperSetHash: generation.MapperSetHash, LogicalHash: logicalHash, PhysicalHash: digest}
}

func assertCoverageEqual(t *testing.T, left, right CoverageReport) {
	t.Helper()
	if left.Opportunities != right.Opportunities || len(left.Counts) != len(right.Counts) {
		t.Fatalf("coverage union changed totals: left=%+v right=%+v", left, right)
	}
	for index := range left.Counts {
		want, got := left.Counts[index], right.Counts[index]
		if want.Total != got.Total || want.Open != got.Open || len(want.Outcomes) != len(got.Outcomes) {
			t.Fatalf("coverage union changed count %d: left=%+v right=%+v", index, want, got)
		}
		for outcome, count := range want.Outcomes {
			if got.Outcomes[outcome] != count {
				t.Fatalf("coverage outcome %s = %d, want %d", outcome, got.Outcomes[outcome], count)
			}
		}
	}
}

type fakeSpillStore struct {
	rows            map[string]Opportunity
	generation      SpillGeneration
	failCommitAfter bool
	failDeleteOnce  bool
	commits         int
	deletes         int
}

func newFakeSpillStore(t *testing.T) *fakeSpillStore {
	t.Helper()
	rows := []Opportunity{testOpportunity(1, OpportunityScheduledRESTPoll, OpportunityOutcomeObserved),
		testOpportunity(2, OpportunityHeartbeatDeadline, OpportunityOutcomeSourceStale)}
	return &fakeSpillStore{rows: map[string]Opportunity{rows[0].OpportunityID: rows[0], rows[1].OpportunityID: rows[1]}}
}

func (s *fakeSpillStore) BeginOpportunitySpill(_ context.Context, request SpillRequest) (SpillGeneration, error) {
	if s.generation.GenerationID != "" {
		return s.generation, nil
	}
	rows := make([]Opportunity, 0, len(s.rows))
	for _, row := range s.rows {
		rows = append(rows, row)
	}
	generation, err := NewSpillGeneration(request, rows)
	if err != nil {
		return SpillGeneration{}, err
	}
	s.generation = generation
	return generation, nil
}
func (s *fakeSpillStore) OpportunitySpill(context.Context, string) (SpillGeneration, error) {
	return s.generation, nil
}
func (s *fakeSpillStore) CommitOpportunitySpill(_ context.Context, generation SpillGeneration, archive ArchiveCommit) error {
	s.commits++
	s.generation = generation
	s.generation.State, s.generation.Archive = SpillCommitted, &archive
	if s.failCommitAfter {
		s.failCommitAfter = false
		return errors.New("synthetic lost commit response")
	}
	return nil
}
func (s *fakeSpillStore) DeleteCommittedOpportunityRows(context.Context, string) error {
	if s.failDeleteOnce {
		s.failDeleteOnce = false
		return errors.New("synthetic crash before delete")
	}
	for _, identity := range s.generation.RowIdentities {
		delete(s.rows, identity.OpportunityID)
	}
	s.deletes++
	return nil
}
func (s *fakeSpillStore) QuarantineOpportunitySpill(_ context.Context, generationID, reason string) error {
	if generationID != s.generation.GenerationID {
		return ErrInvalidSpill
	}
	if err := ValidateSpillQuarantineReason(reason); err != nil {
		return err
	}
	s.generation.State = SpillQuarantined
	s.generation.QuarantineReason = reason
	return nil
}

type fakeArchiveWriter struct {
	commit     *ArchiveCommit
	failBefore bool
	failAfter  bool
	writes     int
}

func (w *fakeArchiveWriter) LookupOpportunityArchive(_ context.Context, _ SpillGeneration) (ArchiveCommit, bool, error) {
	if w.commit == nil {
		return ArchiveCommit{}, false, nil
	}
	return *w.commit, true, nil
}
func (w *fakeArchiveWriter) WriteOpportunityArchive(_ context.Context, generation SpillGeneration) (ArchiveCommit, error) {
	w.writes++
	if w.failBefore {
		return ArchiveCommit{}, errors.New("synthetic archive failure")
	}
	commit := testArchiveCommit(generation)
	w.commit = &commit
	if w.failAfter {
		w.failAfter = false
		return ArchiveCommit{}, errors.New("synthetic lost manifest response")
	}
	return commit, nil
}
