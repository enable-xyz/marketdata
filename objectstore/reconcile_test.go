package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"testing"

	"github.com/enable-xyz/marketdata/catalog"
)

func TestOrphanReconcile(t *testing.T) {
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
	if len(result.Evidence) == 0 || !slices.ContainsFunc(result.Evidence, func(e ReconcileEvidence) bool {
		return e.ObjectKey == orphanKey && e.Resolution == "orphan_recorded_quarantined" &&
			e.ByteLength == int64(len(orphanBytes)) && e.ApplicationSHA256 == sha256.Sum256(orphanBytes)
	}) {
		t.Fatalf("Reconcile() omitted exact orphan evidence: %#v", result.Evidence)
	}
	publicationCatalog.mu.Lock()
	record, ok := publicationCatalog.records[request.Ready.Manifest.ObjectKey]
	if !ok || record.State != catalog.RawSegmentCommitted {
		publicationCatalog.mu.Unlock()
		t.Fatalf("expected record = %#v, found %v", record, ok)
	}
	if _, manufactured := publicationCatalog.records[orphanKey]; manufactured {
		publicationCatalog.mu.Unlock()
		t.Fatal("Reconcile() manufactured a raw segment identity for an orphan")
	}
	orphan, ok := publicationCatalog.orphans[orphanKey]
	publicationCatalog.mu.Unlock()
	if !ok || orphan.ByteLength != int64(len(orphanBytes)) || !orphan.HasApplicationSHA256 {
		t.Fatalf("recorded orphan = %#v, found %v", orphan, ok)
	}
	retried, err := publisher.Reconcile(t.Context(), nil, "raw/v1/")
	if err != nil {
		t.Fatalf("idempotent Reconcile() error = %v", err)
	}
	if !slices.Contains(retried.Orphans, orphanKey) || !slices.Contains(retried.Quarantined, orphanKey) {
		t.Fatalf("idempotent orphan evidence = %#v", retried)
	}
	publicationCatalog.mu.Lock()
	_, manufactured := publicationCatalog.records[orphanKey]
	refreshed := publicationCatalog.orphans[orphanKey]
	publicationCatalog.mu.Unlock()
	if manufactured || refreshed.ByteLength != orphan.ByteLength || refreshed.ApplicationSHA256 != orphan.ApplicationSHA256 {
		t.Fatalf("idempotent retry changed orphan identity: before=%#v after=%#v manufactured=%v", orphan, refreshed, manufactured)
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

func TestReconcileExhaustsEveryBoundedPage(t *testing.T) {
	client := newFakeClient()
	publicationCatalog := newFakeCatalog()
	for index := range 5 {
		key := fmt.Sprintf("raw/v1/source=00000000-0000-0000-0000-000000000799/exhaustive-orphan-%d", index)
		payload := []byte{byte(index + 1)}
		client.store(key, payload, sha256.Sum256(payload))
	}
	publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{
		Reconcile: ReconcilePolicy{MaxPages: 1, MaxObjects: 1, MaxResults: 2},
	})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	result, err := publisher.Reconcile(t.Context(), nil, "raw/v1/")
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	if !result.Complete || result.ContinuationCursor != "" || len(result.Orphans) != 5 ||
		len(result.Quarantined) != 5 || len(result.Evidence) != 5 {
		t.Fatalf("exhaustive Reconcile() result = %#v", result)
	}
	if !slices.IsSorted(result.Orphans) {
		t.Fatalf("exhaustive orphan evidence is not deterministic: %q", result.Orphans)
	}
	for index := 1; index < len(result.Orphans); index++ {
		if result.Orphans[index-1] == result.Orphans[index] {
			t.Fatalf("exhaustive reconciliation duplicated %q", result.Orphans[index])
		}
	}
	publicationCatalog.mu.Lock()
	orphanCount := len(publicationCatalog.orphans)
	publicationCatalog.mu.Unlock()
	if orphanCount != 5 {
		t.Fatalf("catalog recorded %d orphans, want 5", orphanCount)
	}
}

func TestReconcileRejectsCyclicAndNonprogressingCursors(t *testing.T) {
	tests := []struct {
		name   string
		client Client
	}{
		{name: "cycle", client: &cyclingListClient{fakeClient: newFakeClient()}},
		{name: "same token", client: &nonprogressListClient{fakeClient: newFakeClient()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			publisher, err := NewPublisher(test.client, newFakeCatalog(), PublisherConfig{
				Reconcile: ReconcilePolicy{MaxPages: 1, MaxObjects: 1, MaxResults: 2},
			})
			if err != nil {
				t.Fatalf("NewPublisher() error = %v", err)
			}
			result, err := publisher.Reconcile(t.Context(), nil, "raw/v1/")
			if !errors.Is(err, ErrInvalidResponse) {
				t.Fatalf("Reconcile() result=%#v error=%v, want ErrInvalidResponse", result, err)
			}
			if result.Complete {
				t.Fatalf("failed exhaustive reconciliation reported complete: %#v", result)
			}
		})
	}
}
func TestReconcileRetainsTypedTerminalCatalogEvidence(t *testing.T) {
	client := newFakeClient()
	publicationCatalog := newFakeCatalog()
	quarantinedKey := "raw/v1/source=00000000-0000-0000-0000-000000000799/quarantined"
	supersededKey := "raw/v1/source=00000000-0000-0000-0000-000000000799/superseded"
	for key, state := range map[string]catalog.RawSegmentState{
		quarantinedKey: catalog.RawSegmentQuarantined,
		supersededKey:  catalog.RawSegmentSuperseded,
	} {
		payload := []byte(key)
		hash := sha256.Sum256(payload)
		client.store(key, payload, hash)
		publicationCatalog.records[key] = catalog.RawSegmentPublication{
			ObjectKey: key, ContentSHA256: hash, ByteLength: int64(len(payload)), State: state,
		}
	}
	publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	first, err := publisher.Reconcile(t.Context(), nil, "raw/v1/")
	if err != nil {
		t.Fatalf("Reconcile() error = %v", err)
	}
	retried, err := publisher.Reconcile(t.Context(), nil, "raw/v1/")
	if err != nil {
		t.Fatalf("idempotent Reconcile() error = %v", err)
	}
	if !first.Complete || !retried.Complete || !slices.Equal(first.Evidence, retried.Evidence) {
		t.Fatalf("terminal evidence changed on retry: first=%#v retried=%#v", first, retried)
	}
	if len(first.Evidence) != 2 {
		t.Fatalf("terminal evidence count = %d, want 2", len(first.Evidence))
	}
	for _, evidence := range first.Evidence {
		switch evidence.ObjectKey {
		case quarantinedKey:
			if evidence.PriorCatalogState != catalog.RawSegmentQuarantined ||
				evidence.Outcome != ReconcileOutcomeQuarantined ||
				evidence.Resolution != "quarantined_catalog_state_retained" {
				t.Fatalf("quarantined evidence = %#v", evidence)
			}
		case supersededKey:
			if evidence.PriorCatalogState != catalog.RawSegmentSuperseded ||
				evidence.Outcome != ReconcileOutcomeSuperseded ||
				evidence.Resolution != "superseded_catalog_state_retained" {
				t.Fatalf("superseded evidence = %#v", evidence)
			}
		default:
			t.Fatalf("unexpected terminal evidence = %#v", evidence)
		}
	}
}

type cyclingListClient struct {
	*fakeClient
}

func (c *cyclingListClient) List(_ context.Context, _ string, continuation string) (ListPage, error) {
	switch continuation {
	case "":
		return ListPage{NextToken: "cursor-a"}, nil
	case "cursor-a":
		return ListPage{NextToken: "cursor-b"}, nil
	case "cursor-b":
		return ListPage{NextToken: "cursor-a"}, nil
	default:
		return ListPage{}, ErrInvalidResponse
	}
}

type nonprogressListClient struct {
	*fakeClient
}

func (c *nonprogressListClient) List(_ context.Context, _ string, continuation string) (ListPage, error) {
	if continuation == "" || continuation == "stuck" {
		return ListPage{NextToken: "stuck"}, nil
	}
	return ListPage{}, ErrInvalidResponse
}
