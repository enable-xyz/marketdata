package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/segment"
)

type RecoveryObjectState string

const (
	RecoveryObjectVerified  RecoveryObjectState = "verified"
	RecoveryObjectAbsent    RecoveryObjectState = "absent"
	RecoveryObjectCorrupted RecoveryObjectState = "corrupted"
)

// VerifiedReplicaRestorer is an explicitly caller-supplied provider operation
// that restores one immutable identity from a replica or backup. The publisher
// never trusts success: it fully hashes the primary object again before any
// catalog row is restored.
type VerifiedReplicaRestorer interface {
	RestoreVerifiedObject(context.Context, string, int64, [sha256.Size]byte) error
}

type CatalogRestoreEvidence struct {
	ObjectKey           string
	SegmentID           string
	ObservedState       RecoveryObjectState
	State               RecoveryObjectState
	Resolution          string
	ByteLength          int64
	ApplicationSHA256   [sha256.Size]byte
	ReceivedStartNS     int64
	ReceivedEndNS       int64
	OrdinalStart        uint64
	OrdinalEnd          uint64
	RestoredFromReplica bool
	PermanentGap        bool
	Reason              string
}

type CatalogRestoreReport struct {
	SnapshotSHA256 [sha256.Size]byte
	Evidence       []CatalogRestoreEvidence
}

// VerifyRecoveryObject performs a complete application SHA-256 read regardless
// of object size. ETag is intentionally ignored. Disaster restore has no local
// source to sample against, so bounded sampling is not sufficient authority.
func VerifyRecoveryObject(ctx context.Context, client Client, key string, size int64, expected [sha256.Size]byte) (ObjectInfo, error) {
	if client == nil || key == "" || size <= 0 || expected == ([sha256.Size]byte{}) {
		return ObjectInfo{}, fmt.Errorf("objectstore: invalid recovery verification request")
	}
	info, err := client.Head(ctx, key)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("objectstore: recovery head %q: %w", key, err)
	}
	if info.Size != size {
		return ObjectInfo{}, fmt.Errorf("%w: recovery key %q has %d bytes, expected %d", ErrSizeMismatch, key, info.Size, size)
	}
	metadataHash, ok := applicationHash(info.Metadata)
	if !ok || metadataHash != expected {
		return ObjectInfo{}, fmt.Errorf("%w: recovery key %q application metadata differs", ErrHashMismatch, key)
	}
	body, err := client.Get(ctx, key)
	if err != nil {
		return ObjectInfo{}, fmt.Errorf("objectstore: recovery get %q: %w", key, err)
	}
	hasher := sha256.New()
	read, readErr := io.CopyBuffer(hasher, io.LimitReader(body, size+1), make([]byte, 64<<10))
	closeErr := body.Close()
	if readErr != nil || closeErr != nil {
		return ObjectInfo{}, fmt.Errorf("objectstore: recovery read %q: %w", key, errors.Join(readErr, closeErr))
	}
	if read != size {
		return ObjectInfo{}, fmt.Errorf("%w: recovery key %q read %d bytes, expected %d", ErrSizeMismatch, key, read, size)
	}
	var actual [sha256.Size]byte
	copy(actual[:], hasher.Sum(nil))
	if actual != expected {
		return ObjectInfo{}, fmt.Errorf("%w: recovery key %q full SHA-256 differs", ErrHashMismatch, key)
	}
	return info, nil
}

func (p *Publisher) VerifyRecoveryPublication(ctx context.Context, record catalog.RawSegmentPublication) (ObjectInfo, error) {
	return VerifyRecoveryObject(ctx, p.client, record.ObjectKey, record.ByteLength, record.ContentSHA256)
}

func (p *Publisher) CatalogPublication(ctx context.Context, objectKey string) (catalog.RawSegmentPublication, bool, error) {
	return p.catalog.FindRawSegment(ctx, objectKey)
}

// RestoreCatalog verifies every backup identity against the primary object
// before making it replay-visible. Missing or corrupt objects are withheld and
// returned as permanent-gap evidence; an exact pre-existing committed row is
// atomically quarantined. Provider/transient failures stop the pass rather than
// being mislabeled as loss; an idempotent retry preserves terminal evidence.
func (p *Publisher) RestoreCatalog(ctx context.Context, snapshot catalog.RecoverySnapshot, replica VerifiedReplicaRestorer) (CatalogRestoreReport, error) {
	records, err := catalog.RecoverySegments(snapshot)
	if err != nil {
		return CatalogRestoreReport{}, err
	}
	recoveryCatalog, ok := p.catalog.(RecoveryPublicationCatalog)
	if !ok {
		return CatalogRestoreReport{}, fmt.Errorf("objectstore: catalog does not support recovery invalidation")
	}
	for _, record := range records {
		if err := validateRecoveryManifest(record); err != nil {
			return CatalogRestoreReport{}, err
		}
	}
	report := CatalogRestoreReport{SnapshotSHA256: snapshot.SnapshotSHA256, Evidence: make([]CatalogRestoreEvidence, 0, len(records))}
	for _, record := range records {
		evidence := CatalogRestoreEvidence{
			ObjectKey: record.ObjectKey, SegmentID: record.SegmentID, ByteLength: record.ByteLength,
			ApplicationSHA256: record.ContentSHA256, ReceivedStartNS: record.ReceivedStartNS,
			ReceivedEndNS: record.ReceivedEndNS, OrdinalStart: record.OrdinalStart, OrdinalEnd: record.OrdinalEnd,
		}
		_, primaryErr := p.VerifyRecoveryPublication(ctx, record)
		switch {
		case primaryErr == nil:
			evidence.ObservedState = RecoveryObjectVerified
		case errors.Is(primaryErr, ErrNotFound):
			evidence.ObservedState = RecoveryObjectAbsent
		case isRecoveryCorruption(primaryErr):
			evidence.ObservedState = RecoveryObjectCorrupted
		default:
			return report, fmt.Errorf("objectstore: verify catalog recovery object %q: %w", record.ObjectKey, primaryErr)
		}

		verifyErr := primaryErr
		var replicaErr error
		var postRestoreErr error
		if isRecoveryKnownLoss(primaryErr) && replica != nil {
			replicaErr = replica.RestoreVerifiedObject(ctx, record.ObjectKey, record.ByteLength, record.ContentSHA256)
			if replicaErr != nil {
				if !isRecoveryKnownLoss(replicaErr) {
					return report, fmt.Errorf("objectstore: restore verified replica %q: %w", record.ObjectKey, replicaErr)
				}
			} else {
				evidence.RestoredFromReplica = true
				_, postRestoreErr = p.VerifyRecoveryPublication(ctx, record)
				verifyErr = postRestoreErr
				if postRestoreErr != nil && !isRecoveryKnownLoss(postRestoreErr) {
					return report, fmt.Errorf("objectstore: verify restored replica object %q: %w", record.ObjectKey, postRestoreErr)
				}
			}
		}

		switch {
		case verifyErr == nil:
			evidence.State = RecoveryObjectVerified
			evidence.Resolution = "catalog_restored_committed"
			if err := p.catalog.RecordVerified(ctx, record); err != nil {
				return report, fmt.Errorf("objectstore: restore verified catalog row %q: %w", record.ObjectKey, err)
			}
			if err := p.catalog.CommitRawSegment(ctx, record.ObjectKey); err != nil {
				return report, fmt.Errorf("objectstore: commit restored catalog row %q: %w", record.ObjectKey, err)
			}
		case errors.Is(verifyErr, ErrNotFound):
			evidence.State = RecoveryObjectAbsent
			evidence.Resolution = "catalog_row_withheld_permanent_gap"
			evidence.PermanentGap = true
			evidence.Reason = recoveryLossReason(evidence.ObservedState, primaryErr, replicaErr, postRestoreErr)
			if err := recoveryCatalog.InvalidateCommittedRawSegmentForRecovery(ctx, record, evidence.Reason); err != nil {
				return report, fmt.Errorf("objectstore: withhold invalid restored catalog row %q: %w", record.ObjectKey, err)
			}
		case isRecoveryCorruption(verifyErr):
			evidence.State = RecoveryObjectCorrupted
			evidence.Resolution = "catalog_row_withheld_quarantined_range"
			evidence.PermanentGap = true
			evidence.Reason = recoveryLossReason(evidence.ObservedState, primaryErr, replicaErr, postRestoreErr)
			if err := recoveryCatalog.InvalidateCommittedRawSegmentForRecovery(ctx, record, evidence.Reason); err != nil {
				return report, fmt.Errorf("objectstore: quarantine invalid restored catalog row %q: %w", record.ObjectKey, err)
			}
		default:
			return report, fmt.Errorf("objectstore: verify catalog recovery object %q: %w", record.ObjectKey, verifyErr)
		}
		report.Evidence = append(report.Evidence, evidence)
	}
	slices.SortFunc(report.Evidence, func(a, b CatalogRestoreEvidence) int { return compareRecoveryStrings(a.ObjectKey, b.ObjectKey) })
	return report, nil
}

func validateRecoveryManifest(record catalog.RawSegmentPublication) error {
	if len(record.ManifestBytes) == 0 || int64(len(record.ManifestBytes)) > maximumManifestBytes {
		return fmt.Errorf("%w: recovery manifest byte length", catalog.ErrInvalidPublication)
	}
	var manifest segment.ReadyManifest
	decoder := json.NewDecoder(bytes.NewReader(record.ManifestBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&manifest); err != nil {
		return fmt.Errorf("%w: decode recovery ready manifest: %v", catalog.ErrInvalidPublication, err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("%w: trailing recovery manifest content", catalog.ErrInvalidPublication)
	}
	canonical, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("objectstore: encode recovery ready manifest: %w", err)
	}
	canonical = append(canonical, '\n')
	if !bytes.Equal(canonical, record.ManifestBytes) {
		return fmt.Errorf("%w: recovery manifest bytes are not canonical", catalog.ErrPublicationConflict)
	}
	if manifest.ManifestVersion != segment.SpoolManifestVersion || manifest.Segment.FormatVersion != segment.FormatVersion {
		return fmt.Errorf("%w: unsupported recovery manifest version", catalog.ErrInvalidPublication)
	}
	epochID, valid := canonicalCatalogUUID(manifest.EpochID)
	if !valid {
		return fmt.Errorf("%w: recovery manifest epoch identity", catalog.ErrInvalidPublication)
	}
	expectedKey, err := publicationObjectKey(manifest)
	if err != nil {
		return err
	}
	if manifest.ManifestVersion != record.ManifestVersion || manifest.SourceID != record.SourceID ||
		manifest.ChannelID != record.ChannelID || epochID != record.EpochID || manifest.ObjectKey != record.ObjectKey ||
		expectedKey != record.ObjectKey || manifest.Segment.FirstReceivedAtNS != record.ReceivedStartNS ||
		manifest.Segment.LastReceivedAtNS != record.ReceivedEndNS || manifest.Segment.FirstOrdinal != record.OrdinalStart ||
		manifest.Segment.LastOrdinal != record.OrdinalEnd || manifest.Segment.CompressedSHA256 != record.ContentSHA256 ||
		int64(manifest.Segment.CompressedBytes) != record.ByteLength {
		return fmt.Errorf("%w: recovery catalog row disagrees with immutable manifest identity", catalog.ErrPublicationConflict)
	}
	return nil
}

func isRecoveryCorruption(err error) bool {
	return errors.Is(err, ErrHashMismatch) || errors.Is(err, ErrSizeMismatch) || errors.Is(err, ErrInvalidResponse)
}

func isRecoveryKnownLoss(err error) bool {
	return errors.Is(err, ErrNotFound) || isRecoveryCorruption(err)
}

func recoveryLossReason(
	observed RecoveryObjectState,
	primaryErr error,
	replicaErr error,
	postRestoreErr error,
) string {
	reason := fmt.Sprintf("primary %s: %v", observed, primaryErr)
	if replicaErr != nil {
		reason += fmt.Sprintf("; replica unavailable or corrupt: %v", replicaErr)
	}
	if postRestoreErr != nil {
		reason += fmt.Sprintf("; primary re-verification after replica restore failed: %v", postRestoreErr)
	}
	return reason
}

func compareRecoveryStrings(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}
