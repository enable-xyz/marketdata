package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

var (
	ErrPublicationConflict = errors.New("catalog: publication identity conflict")
	ErrPublicationState    = errors.New("catalog: invalid publication state transition")
	ErrInvalidPublication  = errors.New("catalog: invalid publication")
)

type RawSegmentState string

const (
	RawSegmentPending     RawSegmentState = "pending"
	RawSegmentVerified    RawSegmentState = "verified"
	RawSegmentCommitted   RawSegmentState = "committed"
	RawSegmentQuarantined RawSegmentState = "quarantined"
	RawSegmentSuperseded  RawSegmentState = "superseded"
)

type RawSegmentPublication struct {
	SegmentID       string
	SourceID        string
	ChannelID       string
	EpochID         string
	ReceivedStartNS int64
	ReceivedEndNS   int64
	OrdinalStart    uint64
	OrdinalEnd      uint64
	ObjectKey       string
	ContentSHA256   [32]byte
	ByteLength      int64
	ManifestVersion uint16
	ManifestSHA256  [32]byte
	ManifestBytes   []byte
	State           RawSegmentState
}

type ObjectOrphan struct {
	ObjectKey            string
	ByteLength           int64
	ApplicationSHA256    [32]byte
	HasApplicationSHA256 bool
	Reason               string
}

// PublicationDatabase is implemented by both pgx.Conn and pgxpool.Pool. A pool
// is recommended when publishers share a catalog concurrently.
type PublicationDatabase interface {
	Begin(context.Context) (pgx.Tx, error)
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

type PublicationStore struct {
	database PublicationDatabase
}

func NewPublicationStore(database PublicationDatabase) (*PublicationStore, error) {
	if database == nil {
		return nil, fmt.Errorf("catalog: publication database is required")
	}
	return &PublicationStore{database: database}, nil
}

func (s *PublicationStore) FindRawSegment(ctx context.Context, objectKey string) (RawSegmentPublication, bool, error) {
	if objectKey == "" {
		return RawSegmentPublication{}, false, fmt.Errorf("%w: empty object key", ErrInvalidPublication)
	}
	record, found, err := findRawSegment(ctx, s.database, objectKey)
	if err != nil {
		return RawSegmentPublication{}, false, err
	}
	return record, found, nil
}

// RecordVerified inserts a publication directly into verified, or advances an
// existing exact pending identity. Existing exact verified/committed rows are
// idempotent. No path can skip from pending to committed.
func (s *PublicationStore) RecordVerified(ctx context.Context, record RawSegmentPublication) (err error) {
	if err := validatePublication(record); err != nil {
		return err
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("catalog: begin verified publication: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("catalog: rollback verified publication: %w", rollbackErr))
		}
	}()

	tag, err := tx.Exec(ctx, `
INSERT INTO raw_segment (
    raw_segment_id, source_id, channel_id, epoch_id,
    receive_time_start_ns, receive_time_end_ns, ordinal_start, ordinal_end,
    object_key, content_hash, byte_length, state
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'verified')
ON CONFLICT DO NOTHING
`,
		record.SegmentID,
		record.SourceID,
		record.ChannelID,
		record.EpochID,
		record.ReceivedStartNS,
		record.ReceivedEndNS,
		record.OrdinalStart,
		record.OrdinalEnd,
		record.ObjectKey,
		record.ContentSHA256[:],
		record.ByteLength,
	)
	if err != nil {
		return fmt.Errorf("catalog: insert verified raw segment: %w", err)
	}
	inserted := tag.RowsAffected() == 1
	if inserted {
		if _, err := tx.Exec(ctx, `
INSERT INTO raw_segment_manifest (
    raw_segment_id, manifest_version, manifest_hash, manifest_bytes
) VALUES ($1, $2, $3, $4)
`, record.SegmentID, record.ManifestVersion, record.ManifestSHA256[:], record.ManifestBytes); err != nil {
			return fmt.Errorf("catalog: insert raw segment manifest: %w", err)
		}
	}

	existing, found, err := findRawSegmentForUpdate(ctx, tx, record.ObjectKey)
	if err != nil {
		return err
	}
	if !found || !samePublicationCore(existing, record) {
		return fmt.Errorf("%w: object key %q is bound to different segment identity", ErrPublicationConflict, record.ObjectKey)
	}
	switch existing.State {
	case RawSegmentPending, RawSegmentVerified, RawSegmentCommitted:
	case RawSegmentQuarantined, RawSegmentSuperseded:
		return fmt.Errorf("%w: cannot verify segment in state %q", ErrPublicationState, existing.State)
	default:
		return fmt.Errorf("%w: unknown raw segment state %q", ErrPublicationState, existing.State)
	}

	if existing.ManifestVersion == 0 {
		if _, err := tx.Exec(ctx, `
INSERT INTO raw_segment_manifest (
    raw_segment_id, manifest_version, manifest_hash, manifest_bytes
) VALUES ($1, $2, $3, $4)
ON CONFLICT DO NOTHING
`, record.SegmentID, record.ManifestVersion, record.ManifestSHA256[:], record.ManifestBytes); err != nil {
			return fmt.Errorf("catalog: attach raw segment manifest: %w", err)
		}
		existing, found, err = findRawSegmentForUpdate(ctx, tx, record.ObjectKey)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: raw segment disappeared while attaching manifest", ErrPublicationConflict)
		}
	}
	if !sameManifest(existing, record) {
		return fmt.Errorf("%w: object key %q has different manifest bytes", ErrPublicationConflict, record.ObjectKey)
	}

	if existing.State == RawSegmentPending {
		tag, err := tx.Exec(ctx, `
UPDATE raw_segment
SET state = 'verified'
WHERE object_key = $1 AND state = 'pending'
`, record.ObjectKey)
		if err != nil {
			return fmt.Errorf("catalog: advance pending segment to verified: %w", err)
		}
		if tag.RowsAffected() != 1 {
			return fmt.Errorf("%w: pending segment %q changed state before verification", ErrPublicationState, record.ObjectKey)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: commit verified publication: %w", err)
	}
	return nil
}

func (s *PublicationStore) CommitRawSegment(ctx context.Context, objectKey string) (err error) {
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("catalog: begin segment commit: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("catalog: rollback segment commit: %w", rollbackErr))
		}
	}()

	record, found, err := findRawSegmentForUpdate(ctx, tx, objectKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: no verified segment for key %q", ErrPublicationState, objectKey)
	}
	switch record.State {
	case RawSegmentVerified:
		if record.ManifestVersion == 0 {
			return fmt.Errorf("%w: segment %q has no immutable manifest", ErrPublicationState, objectKey)
		}
		if _, err := tx.Exec(ctx, `
UPDATE raw_segment
SET state = 'committed', committed_at = now()
WHERE object_key = $1 AND state = 'verified'
`, objectKey); err != nil {
			return fmt.Errorf("catalog: commit raw segment: %w", err)
		}
	case RawSegmentCommitted:
	case RawSegmentPending, RawSegmentQuarantined, RawSegmentSuperseded:
		return fmt.Errorf("%w: cannot commit segment in state %q", ErrPublicationState, record.State)
	default:
		return fmt.Errorf("%w: unknown raw segment state %q", ErrPublicationState, record.State)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: finish segment commit: %w", err)
	}
	return nil
}

func (s *PublicationStore) QuarantineRawSegment(ctx context.Context, objectKey, reason string) (err error) {
	if objectKey == "" || strings.TrimSpace(reason) == "" {
		return fmt.Errorf("%w: object key and quarantine reason are required", ErrInvalidPublication)
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("catalog: begin segment quarantine: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("catalog: rollback segment quarantine: %w", rollbackErr))
		}
	}()

	record, found, err := findRawSegmentForUpdate(ctx, tx, objectKey)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: no segment for key %q", ErrPublicationState, objectKey)
	}
	switch record.State {
	case RawSegmentPending, RawSegmentVerified:
		if _, err := tx.Exec(ctx, "UPDATE raw_segment SET state = 'quarantined' WHERE object_key = $1", objectKey); err != nil {
			return fmt.Errorf("catalog: quarantine raw segment: %w", err)
		}
	case RawSegmentQuarantined:
	case RawSegmentCommitted, RawSegmentSuperseded:
		return fmt.Errorf("%w: cannot quarantine segment in state %q", ErrPublicationState, record.State)
	default:
		return fmt.Errorf("%w: unknown raw segment state %q", ErrPublicationState, record.State)
	}
	if _, err := tx.Exec(ctx, `
INSERT INTO raw_segment_quarantine (raw_segment_id, reason)
VALUES ($1, $2)
ON CONFLICT (raw_segment_id) DO NOTHING
`, record.SegmentID, reason); err != nil {
		return fmt.Errorf("catalog: record segment quarantine reason: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: finish segment quarantine: %w", err)
	}
	return nil
}

func (s *PublicationStore) RecordObjectOrphan(ctx context.Context, orphan ObjectOrphan) (err error) {
	if orphan.ObjectKey == "" || orphan.ByteLength < 0 || strings.TrimSpace(orphan.Reason) == "" {
		return fmt.Errorf("%w: invalid object orphan", ErrInvalidPublication)
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("catalog: begin object orphan: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("catalog: rollback object orphan: %w", rollbackErr))
		}
	}()

	var applicationHash any
	if orphan.HasApplicationSHA256 {
		applicationHash = orphan.ApplicationSHA256[:]
	}
	tag, err := tx.Exec(ctx, `
INSERT INTO object_orphan (
    object_key, byte_length, application_sha256, reason
) VALUES ($1, $2, $3, $4)
ON CONFLICT DO NOTHING
`, orphan.ObjectKey, orphan.ByteLength, applicationHash, orphan.Reason)
	if err != nil {
		return fmt.Errorf("catalog: insert object orphan: %w", err)
	}
	if tag.RowsAffected() == 0 {
		var byteLength int64
		var storedHash []byte
		if err := tx.QueryRow(ctx, `
SELECT byte_length, application_sha256
FROM object_orphan
WHERE object_key = $1
FOR UPDATE
`, orphan.ObjectKey).Scan(&byteLength, &storedHash); err != nil {
			return fmt.Errorf("catalog: read existing object orphan: %w", err)
		}
		hashMatches := (!orphan.HasApplicationSHA256 && storedHash == nil) ||
			(orphan.HasApplicationSHA256 && bytes.Equal(storedHash, orphan.ApplicationSHA256[:]))
		if byteLength != orphan.ByteLength || !hashMatches {
			return fmt.Errorf("%w: orphan key %q changed immutable identity", ErrPublicationConflict, orphan.ObjectKey)
		}
		if _, err := tx.Exec(ctx, `
UPDATE object_orphan
SET last_seen_at = now(), reason = $2
WHERE object_key = $1
`, orphan.ObjectKey, orphan.Reason); err != nil {
			return fmt.Errorf("catalog: refresh object orphan: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: commit object orphan: %w", err)
	}
	return nil
}

func validatePublication(record RawSegmentPublication) error {
	if record.SegmentID == "" || record.SourceID == "" || record.ChannelID == "" || record.EpochID == "" || record.ObjectKey == "" || record.ByteLength <= 0 || record.ManifestVersion == 0 || len(record.ManifestBytes) == 0 {
		return fmt.Errorf("%w: required raw segment identity is missing", ErrInvalidPublication)
	}
	if record.ReceivedStartNS < 0 ||
		record.ReceivedEndNS < record.ReceivedStartNS ||
		record.OrdinalEnd < record.OrdinalStart ||
		record.OrdinalStart > uint64(1<<63-1) ||
		record.OrdinalEnd > uint64(1<<63-1) ||
		record.ManifestVersion > uint16(1<<15-1) {
		return fmt.Errorf("%w: invalid raw segment bounds", ErrInvalidPublication)
	}
	if sha256.Sum256(record.ManifestBytes) != record.ManifestSHA256 {
		return fmt.Errorf("%w: manifest SHA-256 does not match exact bytes", ErrInvalidPublication)
	}
	return nil
}

type publicationQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func findRawSegment(ctx context.Context, queryer publicationQueryer, objectKey string) (RawSegmentPublication, bool, error) {
	return scanRawSegment(queryer.QueryRow(ctx, rawSegmentQuery, objectKey), objectKey)
}

func findRawSegmentForUpdate(ctx context.Context, tx pgx.Tx, objectKey string) (RawSegmentPublication, bool, error) {
	return scanRawSegment(tx.QueryRow(ctx, rawSegmentQuery+" FOR UPDATE OF rs", objectKey), objectKey)
}

const rawSegmentQuery = `
SELECT
    rs.raw_segment_id::text, rs.source_id::text, rs.channel_id, rs.epoch_id::text,
    rs.receive_time_start_ns, rs.receive_time_end_ns,
    rs.ordinal_start, rs.ordinal_end, rs.object_key, rs.content_hash,
    rs.byte_length, rs.state::text,
    rsm.manifest_version, rsm.manifest_hash, rsm.manifest_bytes
FROM raw_segment rs
LEFT JOIN raw_segment_manifest rsm USING (raw_segment_id)
WHERE rs.object_key = $1`

func scanRawSegment(row pgx.Row, objectKey string) (RawSegmentPublication, bool, error) {
	var record RawSegmentPublication
	var contentHash, manifestHash []byte
	var ordinalStart, ordinalEnd int64
	var state string
	var manifestVersion pgtype.Int2
	err := row.Scan(
		&record.SegmentID,
		&record.SourceID,
		&record.ChannelID,
		&record.EpochID,
		&record.ReceivedStartNS,
		&record.ReceivedEndNS,
		&ordinalStart,
		&ordinalEnd,
		&record.ObjectKey,
		&contentHash,
		&record.ByteLength,
		&state,
		&manifestVersion,
		&manifestHash,
		&record.ManifestBytes,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return RawSegmentPublication{}, false, nil
	}
	if err != nil {
		return RawSegmentPublication{}, false, fmt.Errorf("catalog: find raw segment %q: %w", objectKey, err)
	}
	if len(contentHash) != len(record.ContentSHA256) {
		return RawSegmentPublication{}, false, fmt.Errorf("%w: raw segment has malformed content hash", ErrPublicationConflict)
	}
	copy(record.ContentSHA256[:], contentHash)
	if ordinalStart < 0 || ordinalEnd < ordinalStart {
		return RawSegmentPublication{}, false, fmt.Errorf("%w: raw segment has malformed ordinal bounds", ErrPublicationConflict)
	}
	record.OrdinalStart = uint64(ordinalStart)
	record.OrdinalEnd = uint64(ordinalEnd)
	record.State = RawSegmentState(state)
	if manifestVersion.Valid {
		if manifestVersion.Int16 <= 0 || len(manifestHash) != len(record.ManifestSHA256) {
			return RawSegmentPublication{}, false, fmt.Errorf("%w: raw segment has malformed manifest", ErrPublicationConflict)
		}
		record.ManifestVersion = uint16(manifestVersion.Int16)
		copy(record.ManifestSHA256[:], manifestHash)
	}
	return record, true, nil
}

func samePublicationCore(left, right RawSegmentPublication) bool {
	return left.SegmentID == right.SegmentID &&
		left.SourceID == right.SourceID &&
		left.ChannelID == right.ChannelID &&
		left.EpochID == right.EpochID &&
		left.ReceivedStartNS == right.ReceivedStartNS &&
		left.ReceivedEndNS == right.ReceivedEndNS &&
		left.OrdinalStart == right.OrdinalStart &&
		left.OrdinalEnd == right.OrdinalEnd &&
		left.ObjectKey == right.ObjectKey &&
		left.ContentSHA256 == right.ContentSHA256 &&
		left.ByteLength == right.ByteLength
}

func sameManifest(left, right RawSegmentPublication) bool {
	return left.ManifestVersion == right.ManifestVersion &&
		left.ManifestSHA256 == right.ManifestSHA256 &&
		bytes.Equal(left.ManifestBytes, right.ManifestBytes)
}
