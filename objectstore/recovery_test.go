package objectstore

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/catalog"
)

func TestRestoreCatalogInvalidatesPreexistingCommittedCorruption(t *testing.T) {
	payload := []byte("recovery-primary")
	request := readyFixture(t, payload)
	record := recoveryRecord(t, request, "00000000-0000-0000-0000-000000000711")
	snapshot, err := catalog.NewRecoverySnapshot(time.Unix(1_710_000_000, 0), []catalog.RawSegmentPublication{record})
	if err != nil {
		t.Fatalf("NewRecoverySnapshot() error = %v", err)
	}
	client := newFakeClient()
	corrupt := []byte("recovery-corrupt")
	client.store(record.ObjectKey, corrupt, sha256.Sum256(corrupt))
	publicationCatalog := newFakeCatalog()
	publicationCatalog.records[record.ObjectKey] = record
	publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	report, err := publisher.RestoreCatalog(t.Context(), snapshot, nil)
	if err != nil {
		t.Fatalf("RestoreCatalog() error = %v", err)
	}
	if len(report.Evidence) != 1 || report.Evidence[0].ObservedState != RecoveryObjectCorrupted ||
		report.Evidence[0].State != RecoveryObjectCorrupted || !report.Evidence[0].PermanentGap {
		t.Fatalf("corrupt restore evidence = %#v", report.Evidence)
	}
	publicationCatalog.mu.Lock()
	state := publicationCatalog.records[record.ObjectKey].State
	reason := publicationCatalog.recoveryReasons[record.ObjectKey]
	publicationCatalog.mu.Unlock()
	if state != catalog.RawSegmentQuarantined || reason != report.Evidence[0].Reason || reason == "" {
		t.Fatalf("invalid restored row state=%q reason=%q evidence=%#v", state, reason, report.Evidence[0])
	}
}

func TestRestoreCatalogContinuesAfterKnownReplicaLoss(t *testing.T) {
	firstPayload := []byte("known-replica-loss")
	secondPayload := []byte("later-valid-record")
	firstRequest := readyFixture(t, firstPayload)
	secondRequest := readyFixture(t, secondPayload)
	first := recoveryRecord(t, firstRequest, "00000000-0000-0000-0000-000000000712")
	second := recoveryRecord(t, secondRequest, "00000000-0000-0000-0000-000000000713")
	payloads := map[string][]byte{first.ObjectKey: firstPayload, second.ObjectKey: secondPayload}
	loss, valid := first, second
	if loss.ObjectKey > valid.ObjectKey {
		loss, valid = valid, loss
	}
	snapshot, err := catalog.NewRecoverySnapshot(time.Unix(1_710_000_001, 0), []catalog.RawSegmentPublication{loss, valid})
	if err != nil {
		t.Fatalf("NewRecoverySnapshot() error = %v", err)
	}

	tests := []struct {
		name       string
		replicaErr error
	}{
		{name: "absent", replicaErr: ErrNotFound},
		{name: "hash mismatch", replicaErr: ErrHashMismatch},
		{name: "size mismatch", replicaErr: ErrSizeMismatch},
		{name: "invalid response", replicaErr: ErrInvalidResponse},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newFakeClient()
			validPayload := payloads[valid.ObjectKey]
			client.store(valid.ObjectKey, validPayload, sha256.Sum256(validPayload))
			publicationCatalog := newFakeCatalog()
			publicationCatalog.records[loss.ObjectKey] = loss
			publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{})
			if err != nil {
				t.Fatalf("NewPublisher() error = %v", err)
			}
			replica := replicaRestorerFunc(func(_ context.Context, key string, _ int64, _ [sha256.Size]byte) error {
				if key != loss.ObjectKey {
					t.Fatalf("replica called for verified primary %q", key)
				}
				return test.replicaErr
			})

			report, err := publisher.RestoreCatalog(t.Context(), snapshot, replica)
			if err != nil {
				t.Fatalf("RestoreCatalog() error = %v", err)
			}
			if len(report.Evidence) != 2 {
				t.Fatalf("RestoreCatalog() evidence count = %d, want 2", len(report.Evidence))
			}
			var lossEvidence, validEvidence CatalogRestoreEvidence
			for _, evidence := range report.Evidence {
				switch evidence.ObjectKey {
				case loss.ObjectKey:
					lossEvidence = evidence
				case valid.ObjectKey:
					validEvidence = evidence
				}
			}
			if lossEvidence.ObservedState != RecoveryObjectAbsent || lossEvidence.State != RecoveryObjectAbsent ||
				!lossEvidence.PermanentGap || lossEvidence.RestoredFromReplica ||
				!strings.Contains(lossEvidence.Reason, "primary absent") ||
				!strings.Contains(lossEvidence.Reason, test.replicaErr.Error()) {
				t.Fatalf("known replica loss evidence = %#v", lossEvidence)
			}
			if validEvidence.State != RecoveryObjectVerified || validEvidence.PermanentGap {
				t.Fatalf("later valid record was not restored: %#v", validEvidence)
			}
			publicationCatalog.mu.Lock()
			lossState := publicationCatalog.records[loss.ObjectKey].State
			validState := publicationCatalog.records[valid.ObjectKey].State
			publicationCatalog.mu.Unlock()
			if lossState != catalog.RawSegmentQuarantined || validState != catalog.RawSegmentCommitted {
				t.Fatalf("catalog states after known replica loss = loss %q valid %q", lossState, validState)
			}
		})
	}
}

func TestRestoreCatalogAbortsOnUnknownReplicaFailure(t *testing.T) {
	payload := []byte("unknown-replica-failure")
	request := readyFixture(t, payload)
	record := recoveryRecord(t, request, "00000000-0000-0000-0000-000000000714")
	snapshot, err := catalog.NewRecoverySnapshot(time.Unix(1_710_000_002, 0), []catalog.RawSegmentPublication{record})
	if err != nil {
		t.Fatalf("NewRecoverySnapshot() error = %v", err)
	}
	publisher, err := NewPublisher(newFakeClient(), newFakeCatalog(), PublisherConfig{})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	transient := errors.New("replica transport unavailable")
	_, err = publisher.RestoreCatalog(t.Context(), snapshot, replicaRestorerFunc(func(context.Context, string, int64, [sha256.Size]byte) error {
		return transient
	}))
	if !errors.Is(err, transient) {
		t.Fatalf("RestoreCatalog() error = %v, want transient replica failure", err)
	}
}

func recoveryRecord(t *testing.T, request PublishRequest, segmentID string) catalog.RawSegmentPublication {
	t.Helper()
	request.SegmentID = segmentID
	prepared, err := preparePublication(request)
	if err != nil {
		t.Fatalf("preparePublication() error = %v", err)
	}
	if err := prepared.file.Close(); err != nil {
		t.Fatalf("close prepared recovery file: %v", err)
	}
	record := prepared.record
	record.State = catalog.RawSegmentCommitted
	return record
}

type replicaRestorerFunc func(context.Context, string, int64, [sha256.Size]byte) error

func (f replicaRestorerFunc) RestoreVerifiedObject(ctx context.Context, key string, size int64, hash [sha256.Size]byte) error {
	return f(ctx, key, size, hash)
}
