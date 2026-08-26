package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

const RecoverySnapshotVersion uint16 = 1

// RecoverySnapshot is an application-level manifest of committed catalog rows
// captured from a database backup. PostgreSQL backup encryption, retention and
// point-in-time recovery remain caller-owned; this value supplies the exact
// immutable identities a restore drill must verify before making rows visible.
type RecoverySnapshot struct {
	SnapshotVersion uint16                  `json:"snapshot_version"`
	CreatedTimeNS   int64                   `json:"created_time_ns"`
	Segments        []RawSegmentPublication `json:"segments"`
	SnapshotSHA256  [sha256.Size]byte       `json:"snapshot_sha256"`
}

type recoverySnapshotPayload struct {
	SnapshotVersion uint16                  `json:"snapshot_version"`
	CreatedTimeNS   int64                   `json:"created_time_ns"`
	Segments        []RawSegmentPublication `json:"segments"`
}

// NewRecoverySnapshot freezes a deterministic, content-hashed manifest. Only
// committed rows are backup authority; pending work is recovered from spool and
// object reconciliation instead of being promoted by a database snapshot.
func NewRecoverySnapshot(created time.Time, records []RawSegmentPublication) (RecoverySnapshot, error) {
	if created.IsZero() || created.UnixNano() < 0 {
		return RecoverySnapshot{}, fmt.Errorf("%w: recovery snapshot time is required", ErrInvalidPublication)
	}
	segments := clonePublications(records)
	slices.SortFunc(segments, compareRecoveryPublication)
	segmentIDs := make(map[string]struct{}, len(segments))
	for i := range segments {
		if err := validateRecoveryPublication(segments[i]); err != nil {
			return RecoverySnapshot{}, err
		}
		if i > 0 && segments[i-1].ObjectKey == segments[i].ObjectKey {
			return RecoverySnapshot{}, fmt.Errorf("%w: duplicate recovery snapshot object key", ErrPublicationConflict)
		}
		if _, duplicate := segmentIDs[segments[i].SegmentID]; duplicate {
			return RecoverySnapshot{}, fmt.Errorf("%w: duplicate recovery snapshot segment ID", ErrPublicationConflict)
		}
		segmentIDs[segments[i].SegmentID] = struct{}{}
	}
	payload := recoverySnapshotPayload{SnapshotVersion: RecoverySnapshotVersion, CreatedTimeNS: created.UTC().UnixNano(), Segments: segments}
	hash, err := hashRecoverySnapshot(payload)
	if err != nil {
		return RecoverySnapshot{}, err
	}
	return RecoverySnapshot{SnapshotVersion: payload.SnapshotVersion, CreatedTimeNS: payload.CreatedTimeNS, Segments: segments, SnapshotSHA256: hash}, nil
}

// ValidateRecoverySnapshot detects mutation, reordering, duplicate identities,
// malformed manifests and any attempt to restore a non-committed catalog row.
func ValidateRecoverySnapshot(snapshot RecoverySnapshot) error {
	if snapshot.SnapshotVersion != RecoverySnapshotVersion || snapshot.CreatedTimeNS < 0 || snapshot.SnapshotSHA256 == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: incomplete recovery snapshot", ErrInvalidPublication)
	}
	segmentIDs := make(map[string]struct{}, len(snapshot.Segments))
	for i := range snapshot.Segments {
		if err := validateRecoveryPublication(snapshot.Segments[i]); err != nil {
			return err
		}
		if i > 0 && compareRecoveryPublication(snapshot.Segments[i-1], snapshot.Segments[i]) >= 0 {
			return fmt.Errorf("%w: recovery snapshot is not uniquely ordered", ErrPublicationConflict)
		}
		if _, duplicate := segmentIDs[snapshot.Segments[i].SegmentID]; duplicate {
			return fmt.Errorf("%w: duplicate recovery snapshot segment ID", ErrPublicationConflict)
		}
		segmentIDs[snapshot.Segments[i].SegmentID] = struct{}{}
	}
	payload := recoverySnapshotPayload{SnapshotVersion: snapshot.SnapshotVersion, CreatedTimeNS: snapshot.CreatedTimeNS, Segments: snapshot.Segments}
	hash, err := hashRecoverySnapshot(payload)
	if err != nil {
		return err
	}
	if hash != snapshot.SnapshotSHA256 {
		return fmt.Errorf("%w: recovery snapshot SHA-256 mismatch", ErrPublicationConflict)
	}
	return nil
}

// RecoverySegments returns defensive copies so a verified snapshot cannot be
// mutated through an alias while a restore is in progress.
func RecoverySegments(snapshot RecoverySnapshot) ([]RawSegmentPublication, error) {
	if err := ValidateRecoverySnapshot(snapshot); err != nil {
		return nil, err
	}
	return clonePublications(snapshot.Segments), nil
}

// BackupCommittedRawSegments derives recovery identities from the stable
// committed-manifest stream. It is not a substitute for the caller's encrypted
// PostgreSQL backup or PITR log.
func (s *PublicationStore) BackupCommittedRawSegments(ctx context.Context, created time.Time) (RecoverySnapshot, error) {
	var records []RawSegmentPublication
	if err := s.StreamCommittedRawSegments(ctx, func(record RawSegmentPublication) error {
		records = append(records, record)
		return nil
	}); err != nil {
		return RecoverySnapshot{}, err
	}
	return NewRecoverySnapshot(created, records)
}

// InvalidateCommittedRawSegmentForRecovery atomically withholds an exact
// restored committed identity after its immutable object has been confirmed
// absent or corrupt. The snapshot identity is mandatory so recovery cannot
// quarantine a different valid committed row. An exact retry is idempotent.
func (s *PublicationStore) InvalidateCommittedRawSegmentForRecovery(
	ctx context.Context,
	expected RawSegmentPublication,
	reason string,
) (err error) {
	if err := validateRecoveryPublication(expected); err != nil {
		return err
	}
	if strings.TrimSpace(reason) == "" {
		return fmt.Errorf("%w: recovery invalidation reason is required", ErrInvalidPublication)
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("catalog: begin recovery invalidation: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("catalog: rollback recovery invalidation: %w", rollbackErr))
		}
	}()

	existing, found, err := findRawSegmentForUpdate(ctx, tx, expected.ObjectKey)
	if err != nil {
		return err
	}
	if !found {
		if err := tx.Commit(ctx); err != nil {
			return fmt.Errorf("catalog: finish absent recovery invalidation: %w", err)
		}
		return nil
	}
	if !samePublicationCore(existing, expected) || !sameManifest(existing, expected) {
		return fmt.Errorf("%w: recovery object key %q is bound to different segment identity", ErrPublicationConflict, expected.ObjectKey)
	}

	switch existing.State {
	case RawSegmentCommitted:
		tag, err := tx.Exec(ctx, `
UPDATE raw_segment
SET state = 'quarantined', committed_at = NULL
WHERE object_key = $1 AND state = 'committed'
`, expected.ObjectKey)
		if err != nil {
			return fmt.Errorf("catalog: withhold invalid restored segment: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: committed segment %q changed state during recovery invalidation", ErrPublicationState, expected.ObjectKey)
		}
	case RawSegmentQuarantined:
	case RawSegmentPending, RawSegmentVerified, RawSegmentSuperseded:
		return fmt.Errorf("%w: cannot recovery-invalidate segment in state %q", ErrPublicationState, existing.State)
	default:
		return fmt.Errorf("%w: unknown raw segment state %q", ErrPublicationState, existing.State)
	}

	tag, err := tx.Exec(ctx, `
INSERT INTO raw_segment_quarantine (raw_segment_id, reason)
VALUES ($1, $2)
ON CONFLICT (raw_segment_id) DO NOTHING
`, existing.SegmentID, reason)
	if err != nil {
		return fmt.Errorf("catalog: record recovery invalidation evidence: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var storedReason string
		if err := tx.QueryRow(ctx, `
SELECT reason
FROM raw_segment_quarantine
WHERE raw_segment_id = $1
FOR UPDATE
`, existing.SegmentID).Scan(&storedReason); err != nil {
			return fmt.Errorf("catalog: read recovery invalidation evidence: %w", err)
		}
		if storedReason != reason {
			return fmt.Errorf("%w: recovery invalidation evidence for %q differs", ErrPublicationConflict, expected.ObjectKey)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: finish recovery invalidation: %w", err)
	}
	return nil
}

func validateRecoveryPublication(record RawSegmentPublication) error {
	if record.State != RawSegmentCommitted {
		return fmt.Errorf("%w: recovery snapshot contains state %q", ErrPublicationState, record.State)
	}
	if err := validatePublication(record); err != nil {
		return err
	}
	if strings.TrimSpace(record.ObjectKey) != record.ObjectKey || strings.IndexByte(record.ObjectKey, 0) >= 0 {
		return fmt.Errorf("%w: invalid recovery object key", ErrInvalidPublication)
	}
	return nil
}

func compareRecoveryPublication(left, right RawSegmentPublication) int {
	if order := strings.Compare(left.ObjectKey, right.ObjectKey); order != 0 {
		return order
	}
	return strings.Compare(left.SegmentID, right.SegmentID)
}

func clonePublications(records []RawSegmentPublication) []RawSegmentPublication {
	result := make([]RawSegmentPublication, len(records))
	for i := range records {
		result[i] = records[i]
		result[i].ManifestBytes = bytes.Clone(records[i].ManifestBytes)
	}
	return result
}

func hashRecoverySnapshot(payload recoverySnapshotPayload) ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return [sha256.Size]byte{}, fmt.Errorf("catalog: encode recovery snapshot: %w", err)
	}
	return sha256.Sum256(encoded), nil
}
