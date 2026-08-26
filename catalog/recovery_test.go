package catalog

import (
	"crypto/sha256"
	"errors"
	"testing"
)

func TestPublicationStoreRecoveryInvalidatesCommittedSegment(t *testing.T) {
	fixture := newPostgresFixture(t)
	conn := fixture.connect(t)
	if err := Migrate(t.Context(), conn); err != nil {
		t.Fatalf("Migrate() error = %v", err)
	}
	const sourceID = "00000000-0000-0000-0000-000000000621"
	execOK(t, conn, `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state
) VALUES ($1, 'venue', 'product', 'api', 'test', 'active')
`, sourceID)
	store, err := NewPublicationStore(conn)
	if err != nil {
		t.Fatalf("NewPublicationStore() error = %v", err)
	}
	manifest := []byte("{\"manifest_version\":1,\"recovery\":true}\n")
	record := RawSegmentPublication{
		SegmentID:       "00000000-0000-0000-0000-000000000622",
		SourceID:        sourceID,
		ChannelID:       "trades",
		EpochID:         "00000000-0000-0000-0000-000000000623",
		ReceivedStartNS: 100,
		ReceivedEndNS:   200,
		OrdinalStart:    1,
		OrdinalEnd:      10,
		ObjectKey:       "raw/v1/recovery-invalidated",
		ContentSHA256:   sha256.Sum256([]byte("restored committed segment")),
		ByteLength:      int64(len("restored committed segment")),
		ManifestVersion: 1,
		ManifestSHA256:  sha256.Sum256(manifest),
		ManifestBytes:   manifest,
		State:           RawSegmentVerified,
	}
	if err := store.RecordVerified(t.Context(), record); err != nil {
		t.Fatalf("RecordVerified() error = %v", err)
	}
	if err := store.CommitRawSegment(t.Context(), record.ObjectKey); err != nil {
		t.Fatalf("CommitRawSegment() error = %v", err)
	}
	record.State = RawSegmentCommitted

	var visible []string
	if err := store.StreamCommittedRawSegments(t.Context(), func(publication RawSegmentPublication) error {
		visible = append(visible, publication.ObjectKey)
		return nil
	}); err != nil {
		t.Fatalf("StreamCommittedRawSegments() before invalidation error = %v", err)
	}
	if len(visible) != 1 || visible[0] != record.ObjectKey {
		t.Fatalf("committed stream before invalidation = %q", visible)
	}

	mismatch := record
	mismatch.ContentSHA256[0] ^= 0xff
	if err := store.InvalidateCommittedRawSegmentForRecovery(t.Context(), mismatch, "wrong identity must not mutate"); !errors.Is(err, ErrPublicationConflict) {
		t.Fatalf("mismatched invalidation error = %v, want ErrPublicationConflict", err)
	}
	got, found, err := store.FindRawSegment(t.Context(), record.ObjectKey)
	if err != nil || !found || got.State != RawSegmentCommitted {
		t.Fatalf("valid committed row changed by mismatched invalidation: found=%v record=%#v error=%v", found, got, err)
	}

	const reason = "recovery primary absent: exact immutable object not found"
	if err := store.InvalidateCommittedRawSegmentForRecovery(t.Context(), record, reason); err != nil {
		t.Fatalf("InvalidateCommittedRawSegmentForRecovery() error = %v", err)
	}
	if err := store.InvalidateCommittedRawSegmentForRecovery(t.Context(), record, reason); err != nil {
		t.Fatalf("idempotent InvalidateCommittedRawSegmentForRecovery() error = %v", err)
	}
	got, found, err = store.FindRawSegment(t.Context(), record.ObjectKey)
	if err != nil || !found || got.State != RawSegmentQuarantined {
		t.Fatalf("invalidated row = found=%v record=%#v error=%v", found, got, err)
	}

	visible = nil
	if err := store.StreamCommittedRawSegments(t.Context(), func(publication RawSegmentPublication) error {
		visible = append(visible, publication.ObjectKey)
		return nil
	}); err != nil {
		t.Fatalf("StreamCommittedRawSegments() after invalidation error = %v", err)
	}
	if len(visible) != 0 {
		t.Fatalf("invalidated committed row remains replay-visible: %q", visible)
	}
	var storedReason string
	if err := conn.QueryRow(t.Context(), `
SELECT q.reason
FROM raw_segment_quarantine q
JOIN raw_segment s USING (raw_segment_id)
WHERE s.object_key = $1
`, record.ObjectKey).Scan(&storedReason); err != nil {
		t.Fatalf("read recovery invalidation evidence: %v", err)
	}
	if storedReason != reason {
		t.Fatalf("recovery invalidation reason = %q, want %q", storedReason, reason)
	}
}
