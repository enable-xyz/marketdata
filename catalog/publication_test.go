package catalog

import (
	"crypto/sha256"
	"errors"
	"testing"
)

func TestPublicationStoreVerifiedThenCommitted(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	const sourceID = "00000000-0000-0000-0000-000000000601"
	execOK(t, conn, `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state
) VALUES ($1, 'venue', 'product', 'api', 'test', 'active')
`, sourceID)
	store, err := NewPublicationStore(conn)
	if err != nil {
		t.Fatalf("NewPublicationStore() error = %v", err)
	}
	manifest := []byte("{\"manifest_version\":1}\n")
	record := RawSegmentPublication{
		SegmentID:       "00000000-0000-0000-0000-000000000602",
		SourceID:        sourceID,
		ChannelID:       "trades",
		EpochID:         "00000000-0000-0000-0000-000000000603",
		ReceivedStartNS: 100,
		ReceivedEndNS:   200,
		OrdinalStart:    1,
		OrdinalEnd:      10,
		ObjectKey:       "raw/v1/publication-store",
		ContentSHA256:   sha256.Sum256([]byte("segment")),
		ByteLength:      int64(len("segment")),
		ManifestVersion: 1,
		ManifestSHA256:  sha256.Sum256(manifest),
		ManifestBytes:   manifest,
		State:           RawSegmentVerified,
	}
	if err := store.RecordVerified(t.Context(), record); err != nil {
		t.Fatalf("RecordVerified() error = %v", err)
	}
	got, found, err := store.FindRawSegment(t.Context(), record.ObjectKey)
	if err != nil || !found {
		t.Fatalf("FindRawSegment() = found %v, error %v", found, err)
	}
	if got.State != RawSegmentVerified {
		t.Fatalf("state after RecordVerified() = %q, want %q", got.State, RawSegmentVerified)
	}
	if err := store.CommitRawSegment(t.Context(), record.ObjectKey); err != nil {
		t.Fatalf("CommitRawSegment() error = %v", err)
	}
	got, found, err = store.FindRawSegment(t.Context(), record.ObjectKey)
	if err != nil || !found {
		t.Fatalf("FindRawSegment() after commit = found %v, error %v", found, err)
	}
	if got.State != RawSegmentCommitted {
		t.Fatalf("state after CommitRawSegment() = %q, want %q", got.State, RawSegmentCommitted)
	}

	conflict := record
	conflict.ManifestBytes = []byte("different")
	conflict.ManifestSHA256 = sha256.Sum256(conflict.ManifestBytes)
	if err := store.RecordVerified(t.Context(), conflict); !errors.Is(err, ErrPublicationConflict) {
		t.Fatalf("conflicting RecordVerified() error = %v, want ErrPublicationConflict", err)
	}

	quarantine := record
	quarantine.SegmentID = "00000000-0000-0000-0000-000000000604"
	quarantine.EpochID = "00000000-0000-0000-0000-000000000605"
	quarantine.ObjectKey = "raw/v1/quarantine"
	quarantine.ContentSHA256 = sha256.Sum256([]byte("quarantine segment"))
	quarantine.ByteLength = int64(len("quarantine segment"))
	quarantine.ManifestBytes = []byte("{\"manifest_version\":1,\"quarantine\":true}\n")
	quarantine.ManifestSHA256 = sha256.Sum256(quarantine.ManifestBytes)
	if err := store.RecordVerified(t.Context(), quarantine); err != nil {
		t.Fatalf("quarantine RecordVerified() error = %v", err)
	}
	if err := store.QuarantineRawSegment(t.Context(), quarantine.ObjectKey, "bounded verification failed"); err != nil {
		t.Fatalf("QuarantineRawSegment() error = %v", err)
	}
	var reason string
	if err := conn.QueryRow(t.Context(), `
SELECT q.reason
FROM raw_segment_quarantine q
JOIN raw_segment s USING (raw_segment_id)
WHERE s.object_key = $1
`, quarantine.ObjectKey).Scan(&reason); err != nil {
		t.Fatalf("read raw segment quarantine reason: %v", err)
	}
	if reason != "bounded verification failed" {
		t.Fatalf("quarantine reason = %q", reason)
	}
}

func TestPublicationStoreQuarantinesOrphanIdentity(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	store, err := NewPublicationStore(conn)
	if err != nil {
		t.Fatalf("NewPublicationStore() error = %v", err)
	}
	hash := sha256.Sum256([]byte("orphan"))
	orphan := ObjectOrphan{
		ObjectKey:            "raw/v1/orphan",
		ByteLength:           6,
		ApplicationSHA256:    hash,
		HasApplicationSHA256: true,
		Reason:               "no manifest identity",
	}
	if err := store.RecordObjectOrphan(t.Context(), orphan); err != nil {
		t.Fatalf("RecordObjectOrphan() error = %v", err)
	}
	orphan.ByteLength++
	if err := store.RecordObjectOrphan(t.Context(), orphan); !errors.Is(err, ErrPublicationConflict) {
		t.Fatalf("changed orphan identity error = %v, want ErrPublicationConflict", err)
	}
}

func TestPublicationStoreConcurrentQuarantineDoesNotRevive(t *testing.T) {
	fixture := newPostgresFixture(t)
	control := fixture.connect(t)
	verifier := fixture.connect(t)
	if err := Migrate(t.Context(), control); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	const sourceID = "00000000-0000-0000-0000-000000000611"
	const segmentID = "00000000-0000-0000-0000-000000000612"
	const objectKey = "raw/v1/concurrent-quarantine"
	execOK(t, control, `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state
) VALUES ($1, 'venue', 'product', 'api', 'test', 'active')
`, sourceID)
	contentHash := sha256.Sum256([]byte("concurrent quarantine"))
	execOK(t, control, `
INSERT INTO raw_segment (
    raw_segment_id, source_id, channel_id, epoch_id,
    receive_time_start_ns, receive_time_end_ns, ordinal_start, ordinal_end,
    object_key, content_hash, byte_length, state
) VALUES ($1, $2, 'trades', $3, 100, 200, 1, 2, $4, $5, 21, 'pending')
`, segmentID, sourceID, "00000000-0000-0000-0000-000000000613", objectKey, contentHash[:])

	quarantineTx, err := control.Begin(t.Context())
	if err != nil {
		t.Fatalf("begin quarantine transaction: %v", err)
	}
	if _, err := quarantineTx.Exec(t.Context(), `
UPDATE raw_segment
SET state = 'quarantined'
WHERE object_key = $1 AND state = 'pending'
`, objectKey); err != nil {
		quarantineTx.Rollback(t.Context())
		t.Fatalf("stage quarantine: %v", err)
	}
	if _, err := quarantineTx.Exec(t.Context(), `
INSERT INTO raw_segment_quarantine (raw_segment_id, reason)
VALUES ($1, 'concurrent verification regression')
`, segmentID); err != nil {
		quarantineTx.Rollback(t.Context())
		t.Fatalf("stage quarantine reason: %v", err)
	}

	manifest := []byte("{\"manifest_version\":1,\"concurrent\":true}\n")
	record := RawSegmentPublication{
		SegmentID:       segmentID,
		SourceID:        sourceID,
		ChannelID:       "trades",
		EpochID:         "00000000-0000-0000-0000-000000000613",
		ReceivedStartNS: 100,
		ReceivedEndNS:   200,
		OrdinalStart:    1,
		OrdinalEnd:      2,
		ObjectKey:       objectKey,
		ContentSHA256:   contentHash,
		ByteLength:      21,
		ManifestVersion: 1,
		ManifestSHA256:  sha256.Sum256(manifest),
		ManifestBytes:   manifest,
		State:           RawSegmentVerified,
	}
	store, err := NewPublicationStore(verifier)
	if err != nil {
		t.Fatalf("NewPublicationStore() error = %v", err)
	}
	started := make(chan struct{})
	verifyResult := make(chan error, 1)
	go func() {
		close(started)
		verifyResult <- store.RecordVerified(t.Context(), record)
	}()
	<-started
	if err := quarantineTx.Commit(t.Context()); err != nil {
		t.Fatalf("commit quarantine transaction: %v", err)
	}
	if err := <-verifyResult; !errors.Is(err, ErrPublicationState) {
		t.Fatalf("concurrent RecordVerified() error = %v, want ErrPublicationState", err)
	}

	var state string
	var manifests int64
	if err := control.QueryRow(t.Context(), `
SELECT state::text, (
    SELECT count(*) FROM raw_segment_manifest m
    WHERE m.raw_segment_id = s.raw_segment_id
)
FROM raw_segment s
WHERE object_key = $1
`, objectKey).Scan(&state, &manifests); err != nil {
		t.Fatalf("read quarantined segment: %v", err)
	}
	if state != string(RawSegmentQuarantined) || manifests != 0 {
		t.Fatalf("concurrent state=%q manifests=%d, want quarantined with no manifest", state, manifests)
	}
}
