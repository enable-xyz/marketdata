package recovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/enable-xyz/marketdata/warehouse"
)

type CrashState string

const (
	CrashComplete            CrashState = "complete"
	CrashPartial             CrashState = "partial"
	CrashUploadedUncommitted CrashState = "uploaded_uncommitted"
	CrashCommitted           CrashState = "committed"
	CrashQuarantined         CrashState = "quarantined"
	CrashSuperseded          CrashState = "superseded"
	CrashCorrupted           CrashState = "corrupted"
	CrashAbsent              CrashState = "absent"
)

type ArtifactKind string

const (
	ArtifactSpool   ArtifactKind = "spool"
	ArtifactObject  ArtifactKind = "object"
	ArtifactCatalog ArtifactKind = "catalog"
)

type RecoveryEvidence struct {
	Kind              ArtifactKind
	ArtifactID        string
	State             CrashState
	Resolution        string
	SegmentID         string
	ByteLength        int64
	CompleteFrames    uint32
	ApplicationSHA256 [sha256.Size]byte
	ReceivedStartNS   int64
	ReceivedEndNS     int64
	OrdinalStart      uint64
	OrdinalEnd        uint64
	PermanentGap      bool
	Reason            string
}

type PublicationIdentity struct {
	ObjectKey string
	SegmentID string
}

type DrillInput struct {
	SpoolRecovery              segment.RecoveryOptions
	PublicationIdentities      []PublicationIdentity
	CatalogBackup              catalog.RecoverySnapshot
	Replica                    objectstore.VerifiedReplicaRestorer
	RawPrefix                  string
	ParquetManifests           []warehouse.CommittedManifest
	StorageMeasurements        []StorageMeasurement
	BackupDuration             time.Duration
	Provider                   ProviderCapabilities
	CallerRecoveryRequirements []string
}

type DrillReport struct {
	Evidence       []RecoveryEvidence
	CatalogRestore objectstore.CatalogRestoreReport
	Orphans        objectstore.ReconcileResult
	Warehouse      warehouse.FullRebuildReport
	X8             X8Report
}

type Drill struct {
	spool     *segment.Spool
	publisher *objectstore.Publisher
	warehouse *warehouse.Loader
	now       func() time.Time
}

func NewDrill(spool *segment.Spool, publisher *objectstore.Publisher, loader *warehouse.Loader, now func() time.Time) (*Drill, error) {
	if spool == nil || publisher == nil || loader == nil {
		return nil, fmt.Errorf("recovery: explicit spool, publisher and warehouse loader are required")
	}
	if now == nil {
		now = time.Now
	}
	return &Drill{spool: spool, publisher: publisher, warehouse: loader, now: now}, nil
}

// Run executes the recovery boundary in authority order: validate backup
// identity, restore only fully verified catalog rows, reconcile local spool,
// discover object orphans, rebuild ClickHouse from committed Parquet, and emit
// the measured X8 report. Every mutating step is exact and idempotent.
func (d *Drill) Run(ctx context.Context, input DrillInput) (DrillReport, error) {
	backupRecords, err := catalog.RecoverySegments(input.CatalogBackup)
	if err != nil {
		return DrillReport{}, err
	}
	if input.RawPrefix == "" || len(input.ParquetManifests) == 0 {
		return DrillReport{}, fmt.Errorf("recovery: explicit raw prefix and committed Parquet manifests are required")
	}
	if _, err := MeasureX8(X8Measurement{Storage: input.StorageMeasurements, BackupDuration: input.BackupDuration,
		RestoreDuration: time.Nanosecond, RebuildDuration: time.Nanosecond, Provider: input.Provider,
		CallerRecoveryRequirements: input.CallerRecoveryRequirements}); err != nil {
		return DrillReport{}, err
	}
	identities, err := recoveryIdentities(backupRecords, input.PublicationIdentities)
	if err != nil {
		return DrillReport{}, err
	}

	restoreStarted := d.now()
	restore, err := d.publisher.RestoreCatalog(ctx, input.CatalogBackup, input.Replica)
	if err != nil {
		return DrillReport{CatalogRestore: restore}, err
	}
	restoreFinished := d.now()
	restoreDuration, err := observedDuration(restoreStarted, restoreFinished, "catalog restore")
	if err != nil {
		return DrillReport{CatalogRestore: restore}, err
	}
	report := DrillReport{CatalogRestore: restore}
	catalogEvidenceIndex := make(map[string]int, len(restore.Evidence))
	for _, restored := range restore.Evidence {
		state := crashStateForObject(restored.ObservedState)
		catalogEvidenceIndex[restored.ObjectKey] = len(report.Evidence)
		report.Evidence = append(report.Evidence, RecoveryEvidence{
			Kind: ArtifactCatalog, ArtifactID: restored.ObjectKey, State: state, Resolution: restored.Resolution,
			SegmentID: restored.SegmentID, ByteLength: restored.ByteLength, ApplicationSHA256: restored.ApplicationSHA256,
			ReceivedStartNS: restored.ReceivedStartNS, ReceivedEndNS: restored.ReceivedEndNS,
			OrdinalStart: restored.OrdinalStart, OrdinalEnd: restored.OrdinalEnd,
			PermanentGap: restored.PermanentGap, Reason: restored.Reason,
		})
	}

	spoolReport, err := d.spool.Recover(input.SpoolRecovery)
	if err != nil {
		return report, err
	}
	for _, item := range spoolReport.Items {
		if item.Ready == nil {
			report.Evidence = append(report.Evidence, evidenceForRecoveredSpool(item))
			continue
		}
		ready := *item.Ready
		key := ready.Manifest.ObjectKey
		segmentID := identities[key]
		if segmentID == "" {
			moved, quarantineErr := d.spool.QuarantineReady(filepath.Base(ready.ManifestPath), input.SpoolRecovery.Fault)
			if quarantineErr != nil {
				return report, fmt.Errorf("recovery: quarantine ready spool without catalog identity: %w", quarantineErr)
			}
			report.Evidence = append(report.Evidence, RecoveryEvidence{Kind: ArtifactSpool, ArtifactID: key,
				State: CrashComplete, Resolution: "quarantined_missing_catalog_identity",
				ByteLength: int64(ready.Manifest.Segment.CompressedBytes), CompleteFrames: uint32(len(ready.Manifest.Segment.Frames)),
				ApplicationSHA256: ready.Manifest.Segment.CompressedSHA256, ReceivedStartNS: ready.Manifest.Segment.FirstReceivedAtNS,
				ReceivedEndNS: ready.Manifest.Segment.LastReceivedAtNS, OrdinalStart: ready.Manifest.Segment.FirstOrdinal,
				OrdinalEnd: ready.Manifest.Segment.LastOrdinal, PermanentGap: true,
				Reason: fmt.Sprintf("exact catalog identity unavailable; quarantined %d files", len(moved))})
			continue
		}
		before, found, err := d.publisher.CatalogPublication(ctx, key)
		if err != nil {
			return report, err
		}
		published, publishErr := d.publisher.Publish(ctx, objectstore.PublishRequest{SegmentID: segmentID, Ready: ready})
		if publishErr != nil {
			if !isPublicationCorruption(publishErr) {
				return report, publishErr
			}
			if _, err := d.spool.QuarantineReady(filepath.Base(ready.ManifestPath), input.SpoolRecovery.Fault); err != nil {
				return report, errors.Join(publishErr, err)
			}
			report.Evidence = append(report.Evidence, recoveryEvidenceFromReady(ready, segmentID, CrashCorrupted,
				"quarantined_immutable_object_conflict", true, publishErr.Error()))
			continue
		}
		state := CrashComplete
		resolution := "exact_publication_completed"
		if published.Recovered {
			state = CrashUploadedUncommitted
			resolution = "existing_verified_object_committed"
		}
		if found && before.State == catalog.RawSegmentCommitted {
			state = CrashCommitted
			resolution = "committed_publication_verified_retained"
		}
		report.Evidence = append(report.Evidence, recoveryEvidenceFromReady(ready, segmentID, state, resolution, false, ""))
		if index, ok := catalogEvidenceIndex[key]; ok && report.Evidence[index].PermanentGap {
			report.Evidence[index].PermanentGap = false
			report.Evidence[index].Resolution = "catalog_restored_from_verified_spool"
			report.Evidence[index].Reason = ""
			report.CatalogRestore.Evidence[index].PermanentGap = false
			report.CatalogRestore.Evidence[index].Resolution = "catalog_restored_from_verified_spool"
			report.CatalogRestore.Evidence[index].Reason = ""
		}
	}

	orphans, err := d.publisher.Reconcile(ctx, nil, input.RawPrefix)
	if err != nil {
		return report, err
	}
	if !orphans.Complete || orphans.ContinuationCursor != "" {
		return report, fmt.Errorf("recovery: exhaustive object reconciliation did not complete")
	}
	report.Orphans = orphans
	for _, evidence := range orphans.Evidence {
		translated, err := recoveryEvidenceForReconcile(evidence)
		if err != nil {
			return report, err
		}
		report.Evidence = append(report.Evidence, translated)
	}

	rebuildStarted := d.now()
	rebuild, err := d.warehouse.FullRebuild(ctx, input.ParquetManifests)
	report.Warehouse = rebuild
	if err != nil {
		return report, err
	}
	rebuildFinished := d.now()
	rebuildDuration, err := observedDuration(rebuildStarted, rebuildFinished, "warehouse rebuild")
	if err != nil {
		return report, err
	}
	report.X8, err = MeasureX8(X8Measurement{Storage: input.StorageMeasurements, BackupDuration: input.BackupDuration,
		RestoreDuration: restoreDuration, RebuildDuration: rebuildDuration, Provider: input.Provider,
		CallerRecoveryRequirements: input.CallerRecoveryRequirements})
	if err != nil {
		return report, err
	}
	slices.SortFunc(report.Evidence, compareRecoveryEvidence)
	return report, nil
}

func recoveryIdentities(records []catalog.RawSegmentPublication, supplied []PublicationIdentity) (map[string]string, error) {
	identities := make(map[string]string, len(records)+len(supplied))
	for _, record := range records {
		identities[record.ObjectKey] = record.SegmentID
	}
	for _, identity := range supplied {
		if identity.ObjectKey == "" || identity.SegmentID == "" {
			return nil, fmt.Errorf("recovery: incomplete publication identity")
		}
		if existing := identities[identity.ObjectKey]; existing != "" && existing != identity.SegmentID {
			return nil, fmt.Errorf("recovery: conflicting publication identity for %q", identity.ObjectKey)
		}
		identities[identity.ObjectKey] = identity.SegmentID
	}
	return identities, nil
}

func evidenceForRecoveredSpool(item segment.RecoveryItem) RecoveryEvidence {
	state := CrashPartial
	if item.State == segment.RecoveryCorrupt || item.State == segment.RecoveryConflicting || item.State == segment.RecoveryUnidentified {
		state = CrashCorrupted
	}
	artifactID := string(item.State)
	if len(item.Paths) != 0 {
		artifactID = filepath.Base(item.Paths[0])
	}
	resolution := "quarantined_partial_spool"
	if state == CrashCorrupted {
		resolution = "quarantined_corrupt_spool"
	}
	reason := ""
	if item.Err != nil {
		reason = item.Err.Error()
	}
	return RecoveryEvidence{Kind: ArtifactSpool, ArtifactID: artifactID, State: state, Resolution: resolution,
		ByteLength: int64(item.CompleteBytes), CompleteFrames: item.CompleteFrames, PermanentGap: true, Reason: reason}
}

func recoveryEvidenceFromReady(ready segment.ReadySegment, segmentID string, state CrashState, resolution string, permanentGap bool, reason string) RecoveryEvidence {
	manifest := ready.Manifest.Segment
	return RecoveryEvidence{Kind: ArtifactSpool, ArtifactID: ready.Manifest.ObjectKey, State: state, Resolution: resolution,
		SegmentID: segmentID, ByteLength: int64(manifest.CompressedBytes), CompleteFrames: uint32(len(manifest.Frames)),
		ApplicationSHA256: manifest.CompressedSHA256, ReceivedStartNS: manifest.FirstReceivedAtNS,
		ReceivedEndNS: manifest.LastReceivedAtNS, OrdinalStart: manifest.FirstOrdinal, OrdinalEnd: manifest.LastOrdinal,
		PermanentGap: permanentGap, Reason: reason}
}

func crashStateForObject(state objectstore.RecoveryObjectState) CrashState {
	switch state {
	case objectstore.RecoveryObjectVerified:
		return CrashCommitted
	case objectstore.RecoveryObjectCorrupted:
		return CrashCorrupted
	case objectstore.RecoveryObjectAbsent:
		return CrashAbsent
	default:
		return CrashCorrupted
	}
}

func recoveryEvidenceForReconcile(evidence objectstore.ReconcileEvidence) (RecoveryEvidence, error) {
	outcome := evidence.Outcome
	switch evidence.PriorCatalogState {
	case catalog.RawSegmentQuarantined:
		if outcome != objectstore.ReconcileOutcomeQuarantined {
			return RecoveryEvidence{}, fmt.Errorf("recovery: quarantined catalog evidence has outcome %q", outcome)
		}
	case catalog.RawSegmentSuperseded:
		if outcome != objectstore.ReconcileOutcomeSuperseded {
			return RecoveryEvidence{}, fmt.Errorf("recovery: superseded catalog evidence has outcome %q", outcome)
		}
	}

	state := CrashCorrupted
	permanentGap := false
	switch outcome {
	case objectstore.ReconcileOutcomeCommitted:
		state = CrashCommitted
	case objectstore.ReconcileOutcomeQuarantined:
		state = CrashQuarantined
		permanentGap = true
	case objectstore.ReconcileOutcomeSuperseded:
		state = CrashSuperseded
	default:
		return RecoveryEvidence{}, fmt.Errorf("recovery: unknown reconciliation outcome %q", outcome)
	}
	return RecoveryEvidence{
		Kind: ArtifactObject, ArtifactID: evidence.ObjectKey, State: state, Resolution: evidence.Resolution,
		ByteLength: evidence.ByteLength, ApplicationSHA256: evidence.ApplicationSHA256,
		PermanentGap: permanentGap, Reason: evidence.Reason,
	}, nil
}

func isPublicationCorruption(err error) bool {
	return errors.Is(err, objectstore.ErrHashMismatch) || errors.Is(err, objectstore.ErrSizeMismatch) ||
		errors.Is(err, objectstore.ErrInvalidResponse)
}

func observedDuration(start, finish time.Time, operation string) (time.Duration, error) {
	if finish.Before(start) || finish.Equal(start) {
		return 0, fmt.Errorf("recovery: %s measurement is not a positive observed duration", operation)
	}
	return finish.Sub(start), nil
}

func compareRecoveryEvidence(left, right RecoveryEvidence) int {
	if order := strings.Compare(string(left.Kind), string(right.Kind)); order != 0 {
		return order
	}
	if order := strings.Compare(left.ArtifactID, right.ArtifactID); order != 0 {
		return order
	}
	if order := strings.Compare(string(left.State), string(right.State)); order != 0 {
		return order
	}
	return strings.Compare(left.Resolution, right.Resolution)
}
