package objectstore

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/catalog"
)

func TestReconcileExistingExactObjectAndQuarantineOrphan(t *testing.T) {
	request := readyFixture(t, bytes.Repeat([]byte("existing"), 128))
	client := newFakeClient()
	expectedBytes := bytes.Repeat([]byte("existing"), 128)
	client.store(request.Ready.Manifest.ObjectKey, expectedBytes, sha256.Sum256(expectedBytes))
	orphanKey := "raw/v1/source=00000000-0000-0000-0000-000000000799/date=2025-01-02/hour=03/epoch=00000000000000000000000000000799/segment=1-1-deadbeef.emseg.zst"
	orphanBytes := []byte("unattributed immutable evidence")
	client.store(orphanKey, orphanBytes, sha256.Sum256(orphanBytes))
	publicationCatalog := newFakeCatalog()
	publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	result, err := publisher.Reconcile(t.Context(), []PublishRequest{request}, "raw/v1/")
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !slices.Contains(result.Committed, request.Ready.Manifest.ObjectKey) {
		t.Fatalf("Reconcile() committed = %q, missing expected key", result.Committed)
	}
	if !slices.Contains(result.Orphans, orphanKey) || !slices.Contains(result.Quarantined, orphanKey) {
		t.Fatalf("Reconcile() orphan result = %#v", result)
	}
	publicationCatalog.mu.Lock()
	defer publicationCatalog.mu.Unlock()
	record, ok := publicationCatalog.records[request.Ready.Manifest.ObjectKey]
	if !ok || record.State != catalog.RawSegmentCommitted {
		t.Fatalf("expected record = %#v, found %v", record, ok)
	}
	if _, manufactured := publicationCatalog.records[orphanKey]; manufactured {
		t.Fatal("Reconcile() manufactured a raw segment identity for an orphan")
	}
	orphan, ok := publicationCatalog.orphans[orphanKey]
	if !ok || orphan.ByteLength != int64(len(orphanBytes)) || !orphan.HasApplicationSHA256 {
		t.Fatalf("recorded orphan = %#v, found %v", orphan, ok)
	}
}

func TestReconcileVerifiedCatalogObjectCommitsOnlyAfterFullVerification(t *testing.T) {
	request := readyFixture(t, []byte("small verified object"))
	client := newFakeClient()
	payload := []byte("small verified object")
	client.store(request.Ready.Manifest.ObjectKey, payload, sha256.Sum256(payload))
	publicationCatalog := newFakeCatalog()
	prepared, err := preparePublication(request)
	if err != nil {
		t.Fatalf("preparePublication() error = %v", err)
	}
	prepared.file.Close()
	prepared.record.State = catalog.RawSegmentVerified
	publicationCatalog.records[prepared.record.ObjectKey] = prepared.record
	publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	result, err := publisher.Reconcile(t.Context(), nil, "raw/v1/")
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !slices.Contains(result.Committed, prepared.record.ObjectKey) {
		t.Fatalf("Reconcile() committed = %q, want %q", result.Committed, prepared.record.ObjectKey)
	}
	publicationCatalog.mu.Lock()
	state := publicationCatalog.records[prepared.record.ObjectKey].State
	publicationCatalog.mu.Unlock()
	if state != catalog.RawSegmentCommitted {
		t.Fatalf("reconciled state = %q, want committed", state)
	}
}

func TestReconcileLargeCatalogObjectWithoutLocalIdentityQuarantines(t *testing.T) {
	payload := bytes.Repeat([]byte{0x22}, 257)
	request := readyFixture(t, payload)
	client := newFakeClient()
	client.store(request.Ready.Manifest.ObjectKey, payload, sha256.Sum256(payload))
	publicationCatalog := newFakeCatalog()
	prepared, err := preparePublication(request)
	if err != nil {
		t.Fatalf("preparePublication() error = %v", err)
	}
	prepared.file.Close()
	prepared.record.State = catalog.RawSegmentVerified
	publicationCatalog.records[prepared.record.ObjectKey] = prepared.record
	publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{
		Verify: VerifyPolicy{FullReadLimit: 64, SampleBytes: 16, SampleCount: 3},
	})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	result, err := publisher.Reconcile(t.Context(), nil, "raw/v1/")
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !slices.Contains(result.Quarantined, prepared.record.ObjectKey) {
		t.Fatalf("Reconcile() quarantined = %q, want %q", result.Quarantined, prepared.record.ObjectKey)
	}
	publicationCatalog.mu.Lock()
	state := publicationCatalog.records[prepared.record.ObjectKey].State
	publicationCatalog.mu.Unlock()
	if state != catalog.RawSegmentQuarantined {
		t.Fatalf("reconciled state = %q, want quarantined", state)
	}
}

func TestReconcilePaginationLimitsAndCursor(t *testing.T) {
	client := newFakeClient()
	publicationCatalog := newFakeCatalog()
	for index := range 3 {
		key := fmt.Sprintf("raw/v1/source=00000000-0000-0000-0000-000000000799/orphan-%d", index)
		payload := []byte{byte(index + 1)}
		client.store(key, payload, sha256.Sum256(payload))
	}
	publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{
		Reconcile: ReconcilePolicy{MaxPages: 1, MaxObjects: 1, MaxResults: 2},
	})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	cursor := ""
	seen := make(map[string]struct{})
	for pass := range 3 {
		result, err := publisher.ReconcilePage(t.Context(), nil, "raw/v1/", cursor)
		if err != nil {
			t.Fatalf("ReconcilePage() pass %d error = %v", pass, err)
		}
		if reconcileResultCount(result) > 2 {
			t.Fatalf("pass %d retained %d results, limit 2", pass, reconcileResultCount(result))
		}
		if len(result.Orphans) != 1 {
			t.Fatalf("pass %d orphans = %q, want one", pass, result.Orphans)
		}
		if _, duplicate := seen[result.Orphans[0]]; duplicate {
			t.Fatalf("pass %d repeated orphan %q", pass, result.Orphans[0])
		}
		seen[result.Orphans[0]] = struct{}{}
		if pass < 2 {
			if result.Complete || result.ContinuationCursor == "" {
				t.Fatalf("pass %d completion=%v cursor=%q, want continuation", pass, result.Complete, result.ContinuationCursor)
			}
			cursor = result.ContinuationCursor
		} else if !result.Complete || result.ContinuationCursor != "" {
			t.Fatalf("final completion=%v cursor=%q", result.Complete, result.ContinuationCursor)
		}
	}
	if len(seen) != 3 {
		t.Fatalf("reconciled %d objects, want 3", len(seen))
	}

	if _, err := NewPublisher(client, publicationCatalog, PublisherConfig{
		Reconcile: ReconcilePolicy{MaxPages: 1},
	}); err == nil {
		t.Fatal("NewPublisher() accepted partial zero reconciliation bounds")
	}
}
