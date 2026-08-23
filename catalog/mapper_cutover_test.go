package catalog

import (
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestMapperCutoverDeterministicReleaseAndEvidence(t *testing.T) {
	fixtureHash := sha256.Sum256([]byte("fixture-bundle"))
	first, err := NewMapperRelease("mapper-v1", "build-v1", NormalizedSchemaVersionV1, fixtureHash, "source-commit-v1")
	if err != nil {
		t.Fatal(err)
	}
	second, err := NewMapperRelease("mapper-v1", "build-v1", NormalizedSchemaVersionV1, fixtureHash, "source-commit-v1")
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.ReleaseID == "" {
		t.Fatalf("release identity is not deterministic: %#v != %#v", first, second)
	}

	oldRelease := mapperCutoverRelease(t, "old")
	evidence := mapperCutoverEvidence(oldRelease.ReleaseID, first.ReleaseID)
	reordered := evidence
	reordered.Old.AcceptedFields = []string{"Trade:2", "Trade:1"}
	reordered.New.AcceptedFields = []string{"Trade:2", "Trade:1"}
	firstHash, err := evidence.SHA256()
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := reordered.SHA256()
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("dual-run evidence hash changes with accepted-field order: %x != %x", firstHash, secondHash)
	}
}

func TestMapperCutoverEvidenceFailsClosed(t *testing.T) {
	oldRelease := mapperCutoverRelease(t, "old")
	newRelease := mapperCutoverRelease(t, "new")
	fingerprints := mapperCutoverFingerprints()
	from := mapperCutoverTime(100)

	incomplete := MapperBinding{
		SourceID: mapperCutoverSourceID, ChannelID: "trades", EffectiveFrom: from,
		MapperReleaseID: newRelease.ReleaseID, SchemaFingerprints: fingerprints,
		State: MapperBindingActive,
	}
	if err := incomplete.Validate(); !errors.Is(err, ErrInvalidMapperPublication) {
		t.Fatalf("incomplete active binding error = %v, want ErrInvalidMapperPublication", err)
	}

	mismatch := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	mismatch.New.AcceptedFields = append(mismatch.New.AcceptedFields, "Trade:3")
	mismatch.Mismatch = true
	mismatch.MismatchCodes = []string{"accepted_fields"}
	mismatch.Decision = MapperEvidenceRejected
	mismatchingBinding := incomplete
	mismatchingBinding.DualRunEvidence = mismatch
	if err := mismatchingBinding.Validate(); !errors.Is(err, ErrMapperEvidenceMismatch) {
		t.Fatalf("mismatching active binding error = %v, want ErrMapperEvidenceMismatch", err)
	}
	mismatchingBinding.State = MapperBindingRejected
	if err := mismatchingBinding.Validate(); err != nil {
		t.Fatalf("rejected mismatch evidence should remain durable: %v", err)
	}

	falseAcceptance := mismatch
	falseAcceptance.Mismatch = false
	falseAcceptance.MismatchCodes = nil
	falseAcceptance.Decision = MapperEvidenceAccepted
	if err := falseAcceptance.Validate(); !errors.Is(err, ErrMapperEvidenceMismatch) {
		t.Fatalf("false accepted evidence error = %v, want ErrMapperEvidenceMismatch", err)
	}

	logicalProvenanceDifference := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	logicalProvenanceDifference.New.LogicalSHA256 = sha256.Sum256([]byte("new-release-provenance"))
	if err := logicalProvenanceDifference.Validate(); err != nil {
		t.Fatalf("release-specific logical hash difference rejected: %v", err)
	}

	semanticMismatch := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	semanticMismatch.New.SemanticSHA256 = sha256.Sum256([]byte("different-semantic-rows"))
	semanticMismatch.Mismatch = true
	semanticMismatch.MismatchCodes = []string{"semantic_rows"}
	semanticMismatch.Decision = MapperEvidenceRejected
	mismatchingBinding.DualRunEvidence = semanticMismatch
	mismatchingBinding.State = MapperBindingActive
	if err := mismatchingBinding.Validate(); !errors.Is(err, ErrMapperEvidenceMismatch) {
		t.Fatalf("semantic mismatch active binding error = %v, want ErrMapperEvidenceMismatch", err)
	}

	rejectionMismatch := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	rejectionMismatch.New.Rejections = []string{"00000001:rejected:field"}
	rejectionMismatch.New.RejectionSHA256 = sha256.Sum256([]byte("different-rejections"))
	rejectionMismatch.Mismatch = true
	rejectionMismatch.MismatchCodes = []string{"rejections"}
	rejectionMismatch.Decision = MapperEvidenceRejected
	if err := rejectionMismatch.Validate(); err != nil {
		t.Fatalf("durable rejection mismatch evidence rejected: %v", err)
	}

	oversizedString := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	oversizedString.Old.AcceptedFields = []string{strings.Repeat("x", MaxCatalogStringBytes+1)}
	if err := oversizedString.Validate(); !errors.Is(err, ErrInvalidMapperPublication) {
		t.Fatalf("oversized accepted field error = %v, want ErrInvalidMapperPublication", err)
	}

	oversizedRejectionKey := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	oversizedRejectionKey.Old.RejectionCounts = map[string]uint64{
		strings.Repeat("x", MaxCatalogStringBytes+1): 1,
	}
	if err := oversizedRejectionKey.Validate(); !errors.Is(err, ErrInvalidMapperPublication) {
		t.Fatalf("oversized rejection key error = %v, want ErrInvalidMapperPublication", err)
	}

	oversizedMismatchCode := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	oversizedMismatchCode.MismatchCodes = []string{strings.Repeat("x", MaxCatalogStringBytes+1)}
	if err := oversizedMismatchCode.Validate(); !errors.Is(err, ErrInvalidMapperPublication) {
		t.Fatalf("oversized mismatch code error = %v, want ErrInvalidMapperPublication", err)
	}

	oversizedBookResult := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	oversizedBookResult.Old.DownstreamBookResult = strings.Repeat("x", MaxCatalogStringBytes+1)
	if err := oversizedBookResult.Validate(); !errors.Is(err, ErrInvalidMapperPublication) {
		t.Fatalf("oversized downstream result error = %v, want ErrInvalidMapperPublication", err)
	}

	longRejectionDetail := strings.Repeat("x", MaxCatalogStringBytes+1)
	longRejections := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	longRejections.Old.Rejections = []string{longRejectionDetail}
	longRejections.New.Rejections = []string{longRejectionDetail}
	if err := longRejections.Validate(); err != nil {
		t.Fatalf("bounded 32 KiB rejection detail rejected: %v", err)
	}

	oversizedRejectionDetail := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	oversizedRejectionDetail.Old.Rejections = []string{
		strings.Repeat("x", MaxMapperRejectionDetailBytes+1),
	}
	if err := oversizedRejectionDetail.Validate(); !errors.Is(err, ErrInvalidMapperPublication) {
		t.Fatalf("oversized rejection detail error = %v, want ErrInvalidMapperPublication", err)
	}

	oversizedEvidence := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	largeField := strings.Repeat("x", MaxCatalogStringBytes)
	oversizedEvidence.Old.AcceptedFields = slices.Repeat([]string{largeField}, 6_000)
	if err := oversizedEvidence.Validate(); !errors.Is(err, ErrInvalidMapperPublication) {
		t.Fatalf("oversized evidence error = %v, want ErrInvalidMapperPublication", err)
	}
}

func TestMapperCutoverHalfOpenReceiveTimeSelection(t *testing.T) {
	baseline := mapperCutoverRelease(t, "baseline")
	oldRelease := mapperCutoverRelease(t, "old")
	newRelease := mapperCutoverRelease(t, "new")
	boundary := int64(1_750_000_000_000_000_123)
	to := mapperCutoverTime(boundary)
	oldBinding := MapperBinding{
		SourceID: mapperCutoverSourceID, ChannelID: "trades",
		EffectiveFrom: mapperCutoverTime(boundary - 100), EffectiveTo: &to,
		MapperReleaseID: oldRelease.ReleaseID, SchemaFingerprints: mapperCutoverFingerprints(),
		DualRunEvidence: mapperCutoverEvidence(baseline.ReleaseID, oldRelease.ReleaseID),
		State:           MapperBindingRetired,
	}
	newBinding := MapperBinding{
		SourceID: mapperCutoverSourceID, ChannelID: "trades", EffectiveFrom: to,
		MapperReleaseID: newRelease.ReleaseID, SchemaFingerprints: mapperCutoverFingerprints(),
		DualRunEvidence: mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID),
		State:           MapperBindingActive,
	}
	cutover, err := NewMapperCutover([]MapperRelease{baseline, oldRelease, newRelease}, []MapperBinding{newBinding, oldBinding})
	if err != nil {
		t.Fatal(err)
	}

	before, err := cutover.publishedBindingAt(mapperCutoverSourceID, "trades", boundary-1)
	if err != nil || before.MapperReleaseID != oldRelease.ReleaseID {
		t.Fatalf("publication immediately before cutover = %#v, %v", before, err)
	}
	at, err := cutover.publishedBindingAt(mapperCutoverSourceID, "trades", boundary)
	if err != nil || at.MapperReleaseID != newRelease.ReleaseID {
		t.Fatalf("publication at exact cutover nanosecond = %#v, %v", at, err)
	}

	// Exchange time may be on the other side of the boundary; it has no input
	// position in catalog coverage validation or normalize runtime selection.
	exchangeTimeAfterCutover := boundary + 1_000_000
	if exchangeTimeAfterCutover <= boundary || before.MapperReleaseID != oldRelease.ReleaseID {
		t.Fatal("exchange-time value affected receive-time mapper publication")
	}

	nonUTC := newBinding
	nonUTC.EffectiveFrom = to.In(time.FixedZone("exchange-clock", 0))
	if err := nonUTC.Validate(); !errors.Is(err, ErrInvalidMapperPublication) {
		t.Fatalf("non-UTC mapper boundary error = %v, want ErrInvalidMapperPublication", err)
	}

	gapStart := mapperCutoverTime(boundary - 10)
	gappedOld := oldBinding
	gappedOld.EffectiveTo = &gapStart
	gapped, err := NewMapperCutover([]MapperRelease{baseline, oldRelease, newRelease}, []MapperBinding{gappedOld, newBinding})
	if err != nil {
		t.Fatal(err)
	}
	if err := gapped.ValidateSelection(mapperCutoverSourceID, "trades", boundary-5); !errors.Is(err, ErrMapperSelectionUnavailable) {
		t.Fatalf("gap selection validation error = %v, want ErrMapperSelectionUnavailable", err)
	}

	overlapEnd := mapperCutoverTime(boundary + 1)
	overlappingOld := oldBinding
	overlappingOld.EffectiveTo = &overlapEnd
	if _, err := NewMapperCutover([]MapperRelease{baseline, oldRelease, newRelease}, []MapperBinding{overlappingOld, newBinding}); !errors.Is(err, ErrMapperIntervalConflict) {
		t.Fatalf("overlap error = %v, want ErrMapperIntervalConflict", err)
	}
}

func TestMapperCutoverPostgreSQLPublicationAndExactBoundary(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Exec(t.Context(), `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state
) VALUES ($1, 'synthetic', 'mapper-cutover', 'v1', 'test', 'active')
`, mapperCutoverSourceID); err != nil {
		t.Fatal(err)
	}
	store, err := NewPublicationStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	baseline := mapperCutoverRelease(t, "postgres-baseline")
	oldRelease := mapperCutoverRelease(t, "postgres-old")
	newRelease := mapperCutoverRelease(t, "postgres-new")
	for _, release := range []MapperRelease{baseline, oldRelease, newRelease} {
		if err := store.PublishMapperRelease(t.Context(), release); err != nil {
			t.Fatalf("PublishMapperRelease(%s): %v", release.MapperVersion, err)
		}
	}

	fromNS := int64(1_750_000_000_000_000_001)
	boundaryNS := int64(1_750_000_000_000_000_123)
	oldBinding := MapperBinding{
		SourceID: mapperCutoverSourceID, ChannelID: "trades", EffectiveFrom: mapperCutoverTime(fromNS),
		MapperReleaseID: oldRelease.ReleaseID, SchemaFingerprints: mapperCutoverFingerprints(),
		DualRunEvidence: mapperCutoverEvidence(baseline.ReleaseID, oldRelease.ReleaseID),
		State:           MapperBindingActive,
	}
	if err := store.PublishMapperBinding(t.Context(), oldBinding); err != nil {
		t.Fatalf("PublishMapperBinding(old): %v", err)
	}
	next := MapperBinding{
		SourceID: mapperCutoverSourceID, ChannelID: "trades", EffectiveFrom: mapperCutoverTime(boundaryNS),
		MapperReleaseID: newRelease.ReleaseID, SchemaFingerprints: mapperCutoverFingerprints(),
		DualRunEvidence: mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID),
		State:           MapperBindingActive,
	}
	if err := store.PublishMapperCutover(t.Context(), next); err != nil {
		t.Fatalf("PublishMapperCutover(): %v", err)
	}
	if err := store.PublishMapperCutover(t.Context(), next); err != nil {
		t.Fatalf("idempotent PublishMapperCutover(): %v", err)
	}

	candidate := MapperBinding{
		SourceID: mapperCutoverSourceID, ChannelID: "candidate",
		EffectiveFrom: mapperCutoverTime(boundaryNS), MapperReleaseID: newRelease.ReleaseID,
		SchemaFingerprints: mapperCutoverFingerprints(), State: MapperBindingCandidate,
	}
	if err := store.PublishMapperBinding(t.Context(), candidate); err != nil {
		t.Fatalf("PublishMapperBinding(candidate): %v", err)
	}
	if err := store.ActivateMapperBinding(
		t.Context(), mapperCutoverSourceID, "candidate", candidate.EffectiveFrom, DualRunEvidence{},
	); !errors.Is(err, ErrInvalidMapperPublication) {
		t.Fatalf("incomplete ActivateMapperBinding() error = %v, want ErrInvalidMapperPublication", err)
	}
	activationEvidence := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	activationEvidence.ChannelID = "candidate"
	if err := store.ActivateMapperBinding(
		t.Context(), mapperCutoverSourceID, "candidate", candidate.EffectiveFrom, activationEvidence,
	); err != nil {
		t.Fatalf("ActivateMapperBinding(): %v", err)
	}
	if err := store.ValidateMapperSelection(t.Context(), mapperCutoverSourceID, "candidate", boundaryNS); err != nil {
		t.Fatalf("activated candidate coverage: %v", err)
	}

	if err := store.ValidateMapperSelection(t.Context(), mapperCutoverSourceID, "trades", boundaryNS-1); err != nil {
		t.Fatalf("persisted selection validation before boundary: %v", err)
	}
	if err := store.ValidateMapperSelection(t.Context(), mapperCutoverSourceID, "trades", boundaryNS); err != nil {
		t.Fatalf("persisted selection validation at boundary: %v", err)
	}
	var beforeReleaseID, beforeState string
	if err := conn.QueryRow(t.Context(), `
SELECT mapper_release_id::text, state::text
FROM mapper_binding
WHERE source_id = $1 AND channel_id = 'trades'
  AND effective_from_ns <= $2 AND (effective_to_ns IS NULL OR $2 < effective_to_ns)
`, mapperCutoverSourceID, boundaryNS-1).Scan(&beforeReleaseID, &beforeState); err != nil {
		t.Fatal(err)
	}
	if beforeReleaseID != oldRelease.ReleaseID || beforeState != string(MapperBindingRetired) {
		t.Fatalf("persisted publication before boundary = %s/%s", beforeReleaseID, beforeState)
	}
	var atReleaseID string
	if err := conn.QueryRow(t.Context(), `
SELECT mapper_release_id::text
FROM mapper_binding
WHERE source_id = $1 AND channel_id = 'trades'
  AND effective_from_ns <= $2 AND (effective_to_ns IS NULL OR $2 < effective_to_ns)
`, mapperCutoverSourceID, boundaryNS).Scan(&atReleaseID); err != nil {
		t.Fatal(err)
	}
	if atReleaseID != newRelease.ReleaseID {
		t.Fatalf("persisted publication at exact boundary = %s, want %s", atReleaseID, newRelease.ReleaseID)
	}
	if err := store.ValidateMapperSelection(t.Context(), mapperCutoverSourceID, "trades", fromNS-1); !errors.Is(err, ErrMapperSelectionUnavailable) {
		t.Fatalf("persisted gap validation error = %v, want ErrMapperSelectionUnavailable", err)
	}

	sqlEvidence := mapperCutoverEvidence(oldRelease.ReleaseID, newRelease.ReleaseID)
	sqlEvidenceBytes, err := encodeDualRunEvidence(sqlEvidence)
	if err != nil {
		t.Fatal(err)
	}
	var publishable bool
	sqlLogicalDifference := sqlEvidence
	sqlLogicalDifference.New.LogicalSHA256 = sha256.Sum256([]byte("sql-release-provenance"))
	sqlLogicalBytes, err := encodeDualRunEvidence(sqlLogicalDifference)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(t.Context(), `
SELECT catalog_mapper_evidence_publishable($1::jsonb, $2::uuid, $3, $4::uuid)
`, string(sqlLogicalBytes), mapperCutoverSourceID, "trades", newRelease.ReleaseID).Scan(&publishable); err != nil {
		t.Fatal(err)
	}
	if !publishable {
		t.Fatal("PostgreSQL rejected release-specific logical hash difference")
	}

	sqlSemanticMismatch := sqlEvidence
	sqlSemanticMismatch.New.SemanticSHA256 = sha256.Sum256([]byte("sql-semantic-mismatch"))
	sqlSemanticBytes, err := encodeDualRunEvidence(sqlSemanticMismatch)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(t.Context(), `
SELECT catalog_mapper_evidence_publishable($1::jsonb, $2::uuid, $3, $4::uuid)
`, string(sqlSemanticBytes), mapperCutoverSourceID, "trades", newRelease.ReleaseID).Scan(&publishable); err != nil {
		t.Fatal(err)
	}
	if publishable {
		t.Fatal("PostgreSQL accepted mismatching semantic row evidence")
	}
	var complete bool
	if err := conn.QueryRow(t.Context(), `
SELECT catalog_mapper_evidence_complete($1::jsonb)
`, string(sqlSemanticBytes)).Scan(&complete); err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("PostgreSQL accepted false mismatch=false semantic evidence")
	}

	sqlFalseCodes := sqlSemanticMismatch
	sqlFalseCodes.Mismatch = true
	sqlFalseCodes.Decision = MapperEvidenceRejected
	sqlFalseCodesBytes, err := encodeDualRunEvidence(sqlFalseCodes)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(t.Context(), `
SELECT catalog_mapper_evidence_complete($1::jsonb)
`, string(sqlFalseCodesBytes)).Scan(&complete); err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("PostgreSQL accepted mismatch without mismatch codes")
	}

	sqlFalseDecision := sqlFalseCodes
	sqlFalseDecision.MismatchCodes = []string{"semantic_rows"}
	sqlFalseDecision.Decision = MapperEvidenceAccepted
	sqlFalseDecisionBytes, err := encodeDualRunEvidence(sqlFalseDecision)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(t.Context(), `
SELECT catalog_mapper_evidence_complete($1::jsonb)
`, string(sqlFalseDecisionBytes)).Scan(&complete); err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("PostgreSQL accepted mismatch with accepted decision")
	}

	sqlFalseRejection := sqlEvidence
	sqlFalseRejection.Decision = MapperEvidenceRejected
	sqlFalseRejectionBytes, err := encodeDualRunEvidence(sqlFalseRejection)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(t.Context(), `
SELECT catalog_mapper_evidence_complete($1::jsonb)
`, string(sqlFalseRejectionBytes)).Scan(&complete); err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("PostgreSQL accepted matching evidence with rejected decision")
	}

	sqlUnsortedAccepted := sqlEvidence
	sqlUnsortedAccepted.New.AcceptedFields = []string{"Trade:2", "Trade:1"}
	sqlUnsortedAccepted.Mismatch = true
	sqlUnsortedAccepted.MismatchCodes = []string{"accepted_fields_order"}
	sqlUnsortedAccepted.Decision = MapperEvidenceRejected
	sqlUnsortedBytes, err := encodeDualRunEvidence(sqlUnsortedAccepted)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(t.Context(), `
SELECT catalog_mapper_evidence_complete($1::jsonb)
`, string(sqlUnsortedBytes)).Scan(&complete); err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("PostgreSQL accepted differently ordered accepted-field set")
	}

	sqlRejectionMismatch := sqlEvidence
	sqlRejectionMismatch.New.Rejections = []string{"00000001:rejected:field"}
	sqlRejectionMismatch.New.RejectionSHA256 = sha256.Sum256([]byte("sql-rejection-mismatch"))
	sqlRejectionBytes, err := encodeDualRunEvidence(sqlRejectionMismatch)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.QueryRow(t.Context(), `
SELECT catalog_mapper_evidence_publishable($1::jsonb, $2::uuid, $3, $4::uuid)
`, string(sqlRejectionBytes), mapperCutoverSourceID, "trades", newRelease.ReleaseID).Scan(&publishable); err != nil {
		t.Fatal(err)
	}
	if publishable {
		t.Fatal("PostgreSQL accepted mismatching ordered rejection evidence")
	}

	if err := conn.QueryRow(t.Context(), `
SELECT catalog_mapper_evidence_complete(
    jsonb_set($1::jsonb, '{old,downstream_book_result}', to_jsonb(repeat('x', $2)))
)
`, string(sqlEvidenceBytes), MaxCatalogStringBytes+1).Scan(&complete); err != nil {
		t.Fatal(err)
	}
	if complete {
		t.Fatal("PostgreSQL accepted oversized mapper evidence string")
	}

	if _, err := conn.Exec(t.Context(), `
INSERT INTO mapper_binding (
    source_id, channel_id, effective_from, effective_from_ns, mapper_release_id,
    schema_fingerprints, dual_run_evidence, state
) VALUES ($1, 'incomplete', $2, $3, $4, $5::jsonb, '{}'::jsonb, 'active')
`, mapperCutoverSourceID, mapperCutoverTime(boundaryNS), boundaryNS, newRelease.ReleaseID,
		`[{"name":"Trade","version":1,"logical_encoding_version":1,"sha256":"`+HashHex(mapperCutoverFingerprints()[0].SHA256)+`"}]`); err == nil {
		t.Fatal("PostgreSQL accepted active mapper binding without complete dual-run evidence")
	}
}

const mapperCutoverSourceID = "11111111-1111-5111-8111-111111111111"

func mapperCutoverRelease(t *testing.T, name string) MapperRelease {
	t.Helper()
	release, err := NewMapperRelease(
		"mapper-"+name,
		"build-"+name,
		NormalizedSchemaVersionV1,
		sha256.Sum256([]byte("fixtures-"+name)),
		"source-"+name,
	)
	if err != nil {
		t.Fatal(err)
	}
	return release
}

func mapperCutoverFingerprints() []MapperSchemaFingerprint {
	return []MapperSchemaFingerprint{{
		Name: "Trade", Version: 1, LogicalEncodingVersion: 1,
		SHA256: sha256.Sum256([]byte("normalize.Trade.v1")),
	}}
}

func mapperCutoverEvidence(oldReleaseID, newReleaseID string) DualRunEvidence {
	run := MapperRunEvidence{
		AcceptedFields:       []string{"Trade:1", "Trade:2"},
		RejectionCounts:      map[string]uint64{},
		LogicalSHA256:        sha256.Sum256([]byte("logical-rows")),
		SemanticSHA256:       sha256.Sum256([]byte("semantic-rows")),
		Rejections:           []string{},
		RejectionSHA256:      sha256.Sum256([]byte("semantic-rejections")),
		DownstreamBookResult: "applied",
		DownstreamBookSHA256: sha256.Sum256([]byte("book-state")),
	}
	return DualRunEvidence{
		Version:            MapperEvidenceVersion,
		SourceID:           mapperCutoverSourceID,
		ChannelID:          "trades",
		SelectionTimeBasis: MapperSelectionReceivedWall,
		ReceivedStartNS:    10,
		ReceivedEndNS:      20,
		CorpusCount:        2,
		CorpusSHA256:       sha256.Sum256([]byte("corpus")),
		OldMapperReleaseID: oldReleaseID,
		NewMapperReleaseID: newReleaseID,
		Old:                run,
		New:                run,
		MismatchCodes:      []string{},
		Mismatch:           false,
		Decision:           MapperEvidenceAccepted,
	}
}

func mapperCutoverTime(ns int64) time.Time {
	return time.Unix(0, ns).UTC()
}
