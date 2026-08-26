package catalog

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"testing"

	"github.com/enable-xyz/marketdata/quality"
	"github.com/jackc/pgx/v5"
)

const (
	qualityTestSourceID = "00000000-0000-0000-0000-000000009001"
	qualityTestMapperID = "00000000-0000-0000-0000-000000009002"
	qualityTestRawID    = "00000000-0000-0000-0000-000000009003"
	qualityTestDataID   = "00000000-0000-0000-0000-000000009004"
)

func TestMigrationV6QualityLedgerRestartSafe(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := initializeVersionTable(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	migrations, err := loadMigrations()
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations[:5] {
		if err := applyMigration(t.Context(), conn, migration); err != nil {
			t.Fatalf("apply migration %d: %v", migration.version, err)
		}
	}
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("Migrate(v5) error = %v", err)
	}
	first := schemaFingerprint(t, conn)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("restart Migrate(v6) error = %v", err)
	}
	second := schemaFingerprint(t, conn)
	if len(first) != len(second) {
		t.Fatalf("restart changed schema fingerprint size: %d != %d", len(first), len(second))
	}
	for index := range first {
		if first[index] != second[index] {
			t.Fatalf("restart changed schema fingerprint at %d: %q != %q", index, first[index], second[index])
		}
	}
	var applied int
	if err := conn.QueryRow(t.Context(), `
SELECT count(*) FROM goose_db_version WHERE version_id = 6 AND is_applied
`).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if applied != 1 {
		t.Fatalf("applied v6 rows = %d, want 1", applied)
	}
	version, err := SchemaVersion(t.Context(), conn)
	if err != nil || version != 6 {
		t.Fatalf("SchemaVersion() = %d, %v; want 6", version, err)
	}
}

func TestOpportunitySpillPostgreSQL(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	insertQualityTestSource(t, conn)
	store, err := NewQualityStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	rows := []quality.Opportunity{
		qualityStoreOpportunity("00000000-0000-0000-0000-000000009101", 1, quality.OpportunityOutcomeObserved),
		qualityStoreOpportunity("00000000-0000-0000-0000-000000009102", 2, quality.OpportunityOutcomeSourceStale),
	}
	for _, row := range rows {
		if err := store.RecordOpportunity(t.Context(), row); err != nil {
			t.Fatalf("RecordOpportunity(%s) error = %v", row.OpportunityID, err)
		}
	}
	preSpill, err := quality.RecomputeCoverage(rows, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	request := qualityStoreSpillRequest("00000000-0000-0000-0000-000000009201")
	generation, err := store.BeginOpportunitySpill(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := store.BeginOpportunitySpill(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if generation.Fingerprint != retry.Fingerprint || len(generation.RowIdentities) != len(retry.RowIdentities) {
		t.Fatalf("generation retry changed membership: first=%+v retry=%+v", generation, retry)
	}
	changed := request
	changed.MaximumRows++
	if _, err := store.BeginOpportunitySpill(t.Context(), changed); !errors.Is(err, quality.ErrInvalidSpill) {
		t.Fatalf("changed generation request error = %v, want ErrInvalidSpill", err)
	}
	changedLineage := request
	changedLineage.CatalogSnapshotHash = sha256.Sum256([]byte("different-catalog-snapshot"))
	if _, err := store.BeginOpportunitySpill(t.Context(), changedLineage); !errors.Is(err, quality.ErrInvalidSpill) {
		t.Fatalf("changed generation lineage error = %v, want ErrInvalidSpill", err)
	}
	current, err := store.CurrentOpportunities(t.Context(), "quality-v1")
	if err != nil || len(current) != len(rows) {
		t.Fatalf("pre-commit PostgreSQL authority rows=%d error=%v", len(current), err)
	}
	archive := qualityStoreArchiveCommit(generation)
	if err := store.CommitOpportunitySpill(t.Context(), generation, archive); err != nil {
		t.Fatalf("CommitOpportunitySpill() error = %v", err)
	}
	if err := store.CommitOpportunitySpill(t.Context(), generation, archive); err != nil {
		t.Fatalf("unknown-ack CommitOpportunitySpill() retry error = %v", err)
	}
	if err := store.QuarantineOpportunitySpill(t.Context(), generation.GenerationID, "late quarantine"); !errors.Is(err, quality.ErrInvalidSpill) {
		t.Fatalf("committed quarantine error = %v, want ErrInvalidSpill", err)
	}
	assertQualityCount(t, conn, "SELECT count(*) FROM opportunity", 2)
	current, err = store.CurrentOpportunities(t.Context(), "quality-v1")
	if err != nil || len(current) != 0 {
		t.Fatalf("committed-boundary PostgreSQL visibility rows=%d error=%v", len(current), err)
	}
	committed, err := store.OpportunitySpill(t.Context(), generation.GenerationID)
	if err != nil {
		t.Fatal(err)
	}
	union, err := quality.RecomputeCoverage(current, []quality.OpportunityArchive{{Generation: committed, Rows: rows}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	assertQualityCoverageEqual(t, preSpill, union)
	if err := store.DeleteCommittedOpportunityRows(t.Context(), generation.GenerationID); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteCommittedOpportunityRows(t.Context(), generation.GenerationID); err != nil {
		t.Fatalf("idempotent delete retry error = %v", err)
	}
	assertQualityCount(t, conn, "SELECT count(*) FROM opportunity", 0)
	committedAfterDelete, err := store.OpportunitySpill(t.Context(), generation.GenerationID)
	if err != nil || committedAfterDelete.Fingerprint != generation.Fingerprint || len(committedAfterDelete.RowIdentities) != len(rows) {
		t.Fatalf("post-delete generation evidence=%+v error=%v", committedAfterDelete, err)
	}
}

func TestOpportunityOpenTerminalTransitionPostgreSQL(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	insertQualityTestSource(t, conn)
	store, _ := NewQualityStore(conn)
	terminal := qualityStoreOpportunity("00000000-0000-0000-0000-000000009111", 11, quality.OpportunityOutcomeObserved)
	open := terminal
	open.Terminal = false
	open.TerminalTimeNS = 0
	open.TerminalOutcome = 0
	open.TerminalEvidence = nil
	if err := store.RecordOpportunity(t.Context(), open); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordOpportunity(t.Context(), open); err != nil {
		t.Fatalf("identical open retry error = %v", err)
	}
	conflictingSchedule := open
	conflictingSchedule.ExpectedTimeNS++
	if err := store.RecordOpportunity(t.Context(), conflictingSchedule); !errors.Is(err, quality.ErrOpportunityConflict) {
		t.Fatalf("conflicting schedule error = %v, want ErrOpportunityConflict", err)
	}
	if err := store.RecordOpportunity(t.Context(), terminal); err != nil {
		t.Fatalf("open-to-terminal transition error = %v", err)
	}
	if err := store.RecordOpportunity(t.Context(), terminal); err != nil {
		t.Fatalf("identical terminal retry error = %v", err)
	}
	changedTerminal := terminal
	changedTerminal.TerminalOutcome = quality.OpportunityOutcomeUnknown
	if err := store.RecordOpportunity(t.Context(), changedTerminal); !errors.Is(err, quality.ErrOpportunityConflict) {
		t.Fatalf("terminal mutation error = %v, want ErrOpportunityConflict", err)
	}
	if err := store.RecordOpportunity(t.Context(), open); !errors.Is(err, quality.ErrOpportunityConflict) {
		t.Fatalf("open retry after terminal error = %v, want ErrOpportunityConflict", err)
	}
	var outcome string
	if err := conn.QueryRow(t.Context(), `
SELECT terminal_outcome_v1::text FROM opportunity WHERE opportunity_id = $1
`, terminal.OpportunityID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), `
UPDATE opportunity SET terminal_outcome_v1 = 'unknown' WHERE opportunity_id = $1
`, terminal.OpportunityID); err == nil {
		t.Fatal("PostgreSQL allowed terminal opportunity mutation")
	}
	if outcome != terminal.TerminalOutcome.String() {
		t.Fatalf("terminal outcome = %s, want %s", outcome, terminal.TerminalOutcome)
	}
}

func TestOpportunitySpillQuarantinePostgreSQL(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	insertQualityTestSource(t, conn)
	store, _ := NewQualityStore(conn)
	row := qualityStoreOpportunity("00000000-0000-0000-0000-000000009121", 21, quality.OpportunityOutcomeMalformed)
	if err := store.RecordOpportunity(t.Context(), row); err != nil {
		t.Fatal(err)
	}
	first, err := store.BeginOpportunitySpill(t.Context(), qualityStoreSpillRequest("00000000-0000-0000-0000-000000009211"))
	if err != nil {
		t.Fatal(err)
	}
	const reason = "synthetic archive verification failure"
	if err := store.QuarantineOpportunitySpill(t.Context(), first.GenerationID, reason); err != nil {
		t.Fatal(err)
	}
	if err := store.QuarantineOpportunitySpill(t.Context(), first.GenerationID, reason); err != nil {
		t.Fatalf("identical quarantine retry error = %v", err)
	}
	if err := store.QuarantineOpportunitySpill(t.Context(), first.GenerationID, "different evidence"); !errors.Is(err, quality.ErrInvalidSpill) {
		t.Fatalf("conflicting quarantine retry error = %v, want ErrInvalidSpill", err)
	}
	quarantined, err := store.OpportunitySpill(t.Context(), first.GenerationID)
	if err != nil || quarantined.State != quality.SpillQuarantined || quarantined.QuarantineReason != reason {
		t.Fatalf("quarantined generation = %+v, error=%v", quarantined, err)
	}
	replacement, err := store.BeginOpportunitySpill(t.Context(), qualityStoreSpillRequest("00000000-0000-0000-0000-000000009212"))
	if err != nil || len(replacement.RowIdentities) != 1 || replacement.RowIdentities[0].OpportunityID != row.OpportunityID {
		t.Fatalf("replacement generation = %+v, error=%v", replacement, err)
	}
	assertQualityCount(t, conn, `
SELECT count(*) FROM opportunity_spill_row WHERE opportunity_id = '00000000-0000-0000-0000-000000009121'
`, 2)
	assertQualityCount(t, conn, `
SELECT count(*) FROM opportunity_spill_row
WHERE opportunity_id = '00000000-0000-0000-0000-000000009121' AND active_claim
`, 1)
	if _, err := conn.Exec(t.Context(), `
UPDATE opportunity_spill SET state = 'pending', quarantine_reason = NULL
WHERE opportunity_spill_id = $1
`, first.GenerationID); err == nil {
		t.Fatal("PostgreSQL allowed terminal quarantined spill mutation")
	}
}

func TestGapLifecyclePostgreSQL(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	insertQualityTestSource(t, conn)
	store, _ := NewQualityStore(conn)
	gap, err := quality.NewGap(quality.Gap{GapID: "00000000-0000-0000-0000-000000009301", SourceID: qualityTestSourceID,
		ChannelID: "synthetic.book", RangeStartNS: 100, RangeEndNS: 200, DetectionBasis: "sequence_interval",
		FirstGoodCoordinate: json.RawMessage(`{"ordinal":10}`), LastGoodCoordinate: json.RawMessage(`{"ordinal":20}`),
		AffectedFamilies: []string{"book"}, Confidence: 1, Evidence: json.RawMessage(`{"basis":"detectable"}`), DetectedTimeNS: 201})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordGap(t.Context(), gap); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordGap(t.Context(), gap); err != nil {
		t.Fatalf("identical gap retry error = %v", err)
	}
	conflictingGap := gap
	conflictingGap.Confidence = 0.5
	if err := store.RecordGap(t.Context(), conflictingGap); !errors.Is(err, quality.ErrInvalidGap) {
		t.Fatalf("conflicting gap retry error = %v, want ErrInvalidGap", err)
	}
	if err := store.ResolveGap(t.Context(), gap.GapID, quality.GapRecoveredCurrentState, -1); !errors.Is(err, quality.ErrInvalidGapTransition) {
		t.Fatalf("negative gap resolution error = %v, want ErrInvalidGapTransition", err)
	}
	if err := store.ResolveGap(t.Context(), gap.GapID, quality.GapRecoveredCurrentState, gap.DetectedTimeNS-1); !errors.Is(err, quality.ErrInvalidGapTransition) {
		t.Fatalf("pre-detection gap resolution error = %v, want ErrInvalidGapTransition", err)
	}
	if _, err := conn.Exec(t.Context(), `
UPDATE gap
SET state = 'recovered_current_state',
    resolved_at = detected_at - interval '1 second',
    resolved_time_ns = detected_time_ns - 1
WHERE gap_id = $1
`, gap.GapID); err == nil {
		t.Fatal("PostgreSQL allowed gap resolution before detection")
	}
	if err := store.ResolveGap(t.Context(), gap.GapID, quality.GapBackfilledExplicitly, 300); !errors.Is(err, quality.ErrReservedGapState) {
		t.Fatalf("reserved ResolveGap error = %v", err)
	}
	if err := store.ResolveGap(t.Context(), gap.GapID, quality.GapRecoveredCurrentState, 300); err != nil {
		t.Fatal(err)
	}
	if err := store.ResolveGap(t.Context(), gap.GapID, quality.GapPermanent, 400); !errors.Is(err, quality.ErrInvalidGapTransition) {
		t.Fatalf("terminal gap mutation error = %v", err)
	}
	var state string
	if err := conn.QueryRow(t.Context(), "SELECT state::text FROM gap WHERE gap_id = $1", gap.GapID).Scan(&state); err != nil {
		t.Fatal(err)
	}
	if state != string(quality.GapRecoveredCurrentState) {
		t.Fatalf("gap state = %s", state)
	}
	if _, err := conn.Exec(t.Context(), "UPDATE gap SET state = 'backfilled_explicitly', resolved_at = now() WHERE gap_id = $1", gap.GapID); err == nil {
		t.Fatal("PostgreSQL emitted reserved backfilled_explicitly state")
	}
}

func TestIncidentImmutabilityPostgreSQL(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	insertQualityTestSource(t, conn)
	store, _ := NewQualityStore(conn)
	opportunity := qualityStoreOpportunity("00000000-0000-0000-0000-000000009401", 1, quality.OpportunityOutcomeObserved)
	if err := store.RecordOpportunity(t.Context(), opportunity); err != nil {
		t.Fatal(err)
	}
	incident, err := quality.NewIncident(quality.IncidentInput{IncidentID: "00000000-0000-0000-0000-000000009402",
		Annotation: "synthetic outage", ReportSource: "synthetic-fixture", AffectedTuples: json.RawMessage(`[{"channel":"synthetic.poll"}]`),
		HasRange: true, RangeStartNS: 100, RangeEndNS: 200, ReportedTimeNS: 300, CreatedTimeNS: 301})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordIncident(t.Context(), incident); err != nil {
		t.Fatal(err)
	}
	changed, _ := quality.NewIncident(quality.IncidentInput{IncidentID: incident.ID(), Annotation: "changed annotation",
		ReportSource: incident.ReportSource(), AffectedTuples: incident.AffectedTuples(), HasRange: true,
		RangeStartNS: incident.RangeStartNS(), RangeEndNS: incident.RangeEndNS(), ReportedTimeNS: incident.ReportedTimeNS(), CreatedTimeNS: incident.CreatedTimeNS()})
	if err := store.RecordIncident(t.Context(), changed); !errors.Is(err, quality.ErrInvalidIncident) {
		t.Fatalf("conflicting incident replay error = %v", err)
	}
	if _, err := conn.Exec(t.Context(), "UPDATE incident SET annotation = 'mutated' WHERE incident_id = $1", incident.ID()); err == nil {
		t.Fatal("PostgreSQL allowed incident mutation")
	}
	var outcome string
	if err := conn.QueryRow(t.Context(), "SELECT terminal_outcome_v1::text FROM opportunity WHERE opportunity_id = $1", opportunity.OpportunityID).Scan(&outcome); err != nil {
		t.Fatal(err)
	}
	if outcome != quality.OpportunityOutcomeObserved.String() {
		t.Fatalf("incident changed outcome to %s", outcome)
	}
}

func TestSchemaDriftPostgreSQL(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	insertQualityTestSource(t, conn)
	insertQualitySchemaEvidence(t, conn)
	store, _ := NewQualityStore(conn)
	fingerprint, sample, err := quality.StructuralFingerprint([]byte(`{"price":"1.0","extra":true}`))
	if err != nil {
		t.Fatal(err)
	}
	observation := quality.SchemaObservation{SourceID: qualityTestSourceID, ChannelID: "synthetic.poll", Fingerprint: fingerprint,
		FirstSeenTimeNS: 1_000, LastSeenTimeNS: 1_000, ObservationCount: 1,
		Classification: quality.SchemaAdditiveUnknownField, ParserCanPreserve: true, OptionalAbsenceResolved: false,
		FirstRawCoordinate: json.RawMessage(`{"ordinal":1}`),
		LastRawCoordinate:  json.RawMessage(`{"ordinal":1}`), RedactedSample: sample,
		MapperDisposition: quality.NormalizationContinue, MapperReleaseID: qualityTestMapperID}
	if err := store.RecordSchemaObservation(t.Context(), observation, qualityTestRawID, 1); err != nil {
		t.Fatal(err)
	}
	var classification, normalization, mapperDisposition string
	var parserCanPreserve, optionalAbsenceResolved bool
	if err := conn.QueryRow(t.Context(), `
SELECT classification::text, normalization_disposition::text, mapper_disposition::text,
       parser_can_preserve, optional_absence_resolved
FROM schema_observation WHERE source_id = $1 AND channel_id = $2 AND fingerprint = $3
`, qualityTestSourceID, observation.ChannelID, fingerprint[:]).Scan(
		&classification, &normalization, &mapperDisposition, &parserCanPreserve, &optionalAbsenceResolved,
	); err != nil {
		t.Fatal(err)
	}
	if classification != string(quality.SchemaAdditiveUnknownField) ||
		normalization != string(quality.NormalizationContinue) || mapperDisposition != "accepted" ||
		!parserCanPreserve || optionalAbsenceResolved {
		t.Fatalf("schema disposition/evidence = %s/%s/%s/%v/%v",
			classification, normalization, mapperDisposition, parserCanPreserve, optionalAbsenceResolved)
	}
	correction, err := quality.NewCorrection(quality.CorrectionInput{CorrectionID: "00000000-0000-0000-0000-000000009403",
		OriginalRawSegmentID: qualityTestRawID, ReplacementDatasetID: qualityTestDataID, MapperReleaseID: qualityTestMapperID,
		Reason: "synthetic mapper correction", Lineage: json.RawMessage(`{"original":"retained"}`), CreatedTimeNS: 2_000})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCorrection(t.Context(), correction); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCorrection(t.Context(), correction); err != nil {
		t.Fatalf("identical correction retry error = %v", err)
	}
	conflictingCorrectionInput := correction.Input()
	conflictingCorrectionInput.Reason = "conflicting synthetic reason"
	conflictingCorrection, err := quality.NewCorrection(conflictingCorrectionInput)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.RecordCorrection(t.Context(), conflictingCorrection); !errors.Is(err, quality.ErrInvalidCorrection) {
		t.Fatalf("conflicting correction retry error = %v, want ErrInvalidCorrection", err)
	}
	if _, err := conn.Exec(t.Context(), "UPDATE correction SET reason = 'erase evidence' WHERE correction_id = $1", correction.Input().CorrectionID); err == nil {
		t.Fatal("PostgreSQL allowed correction lineage mutation")
	}
	if _, err := conn.Exec(t.Context(), "UPDATE correction SET state = 'rejected', committed_at = NULL WHERE correction_id = $1", correction.Input().CorrectionID); err == nil {
		t.Fatal("PostgreSQL allowed committed correction state mutation")
	}
	health := quality.HealthTransition{TransitionID: "00000000-0000-0000-0000-000000009404",
		SourceID: qualityTestSourceID, ChannelID: "synthetic.poll", Dimension: "heartbeat",
		FromState: quality.HealthHealthy, ToState: quality.HealthStale, ObservedTimeNS: 3_000,
		Evidence: json.RawMessage(`{"basis":"synthetic-timeout"}`)}
	if err := store.RecordHealthTransition(t.Context(), health); err != nil {
		t.Fatal(err)
	}
	if err := store.RecordHealthTransition(t.Context(), health); err != nil {
		t.Fatalf("identical health retry error = %v", err)
	}
	conflictingHealth := health
	conflictingHealth.Evidence = json.RawMessage(`{"basis":"different"}`)
	if err := store.RecordHealthTransition(t.Context(), conflictingHealth); !errors.Is(err, quality.ErrInvalidHealth) {
		t.Fatalf("conflicting health retry error = %v, want ErrInvalidHealth", err)
	}
	if _, err := conn.Exec(t.Context(), "UPDATE source_health_transition SET to_state = 'healthy' WHERE transition_id = $1", health.TransitionID); err == nil {
		t.Fatal("PostgreSQL allowed source-health evidence mutation")
	}
	assertQualityCount(t, conn, "SELECT count(*) FROM raw_segment WHERE raw_segment_id = '"+qualityTestRawID+"'", 1)
}

func qualityStoreOpportunity(id string, offset int64, outcome quality.OpportunityOutcome) quality.Opportunity {
	return quality.Opportunity{OpportunityID: id, LedgerPartition: "quality-v1", SourceID: qualityTestSourceID,
		ChannelID: "synthetic.poll", Expectation: quality.OpportunityScheduledRESTPoll,
		ExpectedTimeNS: 1_000 + offset, WindowStartNS: 1_000, WindowEndNS: 2_000,
		Scope: json.RawMessage(`{"request":"synthetic"}`), Terminal: true, TerminalTimeNS: 1_500 + offset,
		TerminalOutcome: outcome, TerminalEvidence: json.RawMessage(`{"coordinate":"synthetic"}`), CreatedTimeNS: 900 + offset}
}

func qualityStoreSpillRequest(generationID string) quality.SpillRequest {
	return quality.SpillRequest{
		GenerationID: generationID, Partition: "quality-v1", ThroughTimeNS: 2_000, MaximumRows: 10,
		CatalogSnapshotHash: sha256.Sum256([]byte("synthetic-catalog-snapshot")),
		MapperSetHash:       sha256.Sum256([]byte("synthetic-mapper-set")),
	}
}

func qualityStoreArchiveCommit(generation quality.SpillGeneration) quality.ArchiveCommit {
	digest := sha256.Sum256([]byte(generation.GenerationID))
	logicalHash, _ := quality.OpportunityArchiveLogicalHash(generation.Rows)
	return quality.ArchiveCommit{GenerationID: generation.GenerationID, GenerationFingerprint: generation.Fingerprint,
		DatasetPartitionID: "00000000-0000-0000-0000-000000009202", ManifestHash: digest,
		ManifestPath:   "quality/v1/kind=opportunity/date=1970-01-01/generation=synthetic.manifest.json",
		ParquetPath:    "quality/v1/kind=opportunity/date=1970-01-01/part=synthetic.parquet",
		DatasetVersion: "parquet-dataset-v1", PartitionKey: "quality/v1/kind=opportunity/date=1970-01-01",
		RangeStartNS: generation.From.TerminalTimeNS, RangeEndNS: generation.Through.TerminalTimeNS,
		InputSetHash: digest, CatalogSnapshotHash: generation.CatalogSnapshotHash, MapperSetHash: generation.MapperSetHash,
		LogicalHash: logicalHash, PhysicalHash: digest}
}

func insertQualityTestSource(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(t.Context(), `
INSERT INTO source (source_id, venue, product_family, api_family, environment, lifecycle_state)
VALUES ($1, 'synthetic', 'quality', 'v1', 'test', 'active')
`, qualityTestSourceID); err != nil {
		t.Fatal(err)
	}
}

func insertQualitySchemaEvidence(t *testing.T, conn *pgx.Conn) {
	t.Helper()
	if _, err := conn.Exec(t.Context(), `
INSERT INTO mapper_release (
    mapper_release_id, mapper_version, build_identity, normalized_schema_version,
    fixture_bundle_hash, source_code_identity
) VALUES ($1, 'quality-mapper-v1', 'synthetic-build', 'v1', decode(repeat('91', 32), 'hex'), 'synthetic-source')
`, qualityTestMapperID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), `
INSERT INTO raw_segment (
    raw_segment_id, source_id, channel_id, epoch_id, receive_time_start_ns,
    receive_time_end_ns, ordinal_start, ordinal_end, object_key, content_hash,
    byte_length, state, committed_at
) VALUES (
    $1, $2, 'synthetic.poll', '00000000-0000-0000-0000-000000009005',
    1, 2, 1, 2, 'raw/synthetic-quality', decode(repeat('92', 32), 'hex'), 10, 'committed', now()
)
`, qualityTestRawID, qualityTestSourceID); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), `
INSERT INTO dataset_partition (
    dataset_partition_id, dataset_family, dataset_version, partition_key,
    range_start_ns, range_end_ns, input_segment_set_hash, catalog_snapshot_hash,
    mapper_set_hash, logical_hash, physical_hash, object_key, canonical, state, committed_at
) VALUES (
    $1, 'trade', 'v1', 'synthetic-quality', 1, 2,
    decode(repeat('93', 32), 'hex'), decode(repeat('94', 32), 'hex'),
    decode(repeat('95', 32), 'hex'), decode(repeat('96', 32), 'hex'),
    decode(repeat('97', 32), 'hex'), 'dataset/synthetic-quality', true, 'committed', now()
)
`, qualityTestDataID); err != nil {
		t.Fatal(err)
	}
}

func assertQualityCount(t *testing.T, conn *pgx.Conn, query string, want int) {
	t.Helper()
	var got int
	if err := conn.QueryRow(t.Context(), query).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("count = %d, want %d: %s", got, want, query)
	}
}

func assertQualityCoverageEqual(t *testing.T, left, right quality.CoverageReport) {
	t.Helper()
	if left.Opportunities != right.Opportunities || len(left.Counts) != len(right.Counts) {
		t.Fatalf("coverage changed: left=%+v right=%+v", left, right)
	}
	for index := range left.Counts {
		if left.Counts[index].Total != right.Counts[index].Total || len(left.Counts[index].Outcomes) != len(right.Counts[index].Outcomes) {
			t.Fatalf("coverage count changed: left=%+v right=%+v", left.Counts[index], right.Counts[index])
		}
		for outcome, count := range left.Counts[index].Outcomes {
			if right.Counts[index].Outcomes[outcome] != count {
				t.Fatalf("outcome %s = %d, want %d", outcome, right.Counts[index].Outcomes[outcome], count)
			}
		}
	}
}
