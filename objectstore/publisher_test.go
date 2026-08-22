package objectstore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/enable-xyz/marketdata/catalog"
)

func TestPublishConditionalRaceRangeAndETagIgnored(t *testing.T) {
	request := readyFixture(t, bytes.Repeat([]byte("range-data"), 1024))
	client := newFakeClient()
	publicationCatalog := newFakeCatalog()
	publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{
		Verify: VerifyPolicy{FullReadLimit: 64, SampleBytes: 17, SampleCount: 3},
	})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}

	start := make(chan struct{})
	results := make(chan PublishResult, 2)
	errorsFound := make(chan error, 2)
	var wait sync.WaitGroup
	for range 2 {
		wait.Go(func() {
			<-start
			result, publishErr := publisher.Publish(t.Context(), request)
			results <- result
			errorsFound <- publishErr
		})
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsFound)
	for publishErr := range errorsFound {
		if publishErr != nil {
			t.Fatalf("Publish() race error = %v", publishErr)
		}
	}
	recovered := 0
	for result := range results {
		if result.State != catalog.RawSegmentCommitted {
			t.Fatalf("Publish() state = %q, want committed", result.State)
		}
		if result.Object.ETag != "deliberately-wrong-etag" {
			t.Fatalf("Publish() unexpectedly changed fake ETag %q", result.Object.ETag)
		}
		if result.Recovered {
			recovered++
		}
	}
	if recovered != 1 {
		t.Fatalf("recovered race publications = %d, want 1", recovered)
	}
	client.mu.Lock()
	objectCount := len(client.objects)
	rangeReads := client.rangeReads
	client.mu.Unlock()
	if objectCount != 1 {
		t.Fatalf("stored objects = %d, want 1", objectCount)
	}
	if rangeReads == 0 {
		t.Fatal("Publish() made no bounded range reads")
	}
	publicationCatalog.mu.Lock()
	defer publicationCatalog.mu.Unlock()
	seenVerified := false
	for _, event := range publicationCatalog.events {
		if strings.HasPrefix(event, "verified:") {
			seenVerified = true
		}
		if strings.HasPrefix(event, "committed:") && !seenVerified {
			t.Fatalf("catalog event order = %q; committed preceded verified", publicationCatalog.events)
		}
	}
}

func TestPublishDroppedResponseReconcilesExistingExactBytes(t *testing.T) {
	request := readyFixture(t, bytes.Repeat([]byte{0x5a}, 4096))
	client := newFakeClient()
	client.dropNextCreate = true
	publicationCatalog := newFakeCatalog()
	publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	result, err := publisher.Publish(t.Context(), request)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if !result.Recovered {
		t.Fatal("Publish() did not report dropped-response recovery")
	}
	publicationCatalog.mu.Lock()
	state := publicationCatalog.records[request.Ready.Manifest.ObjectKey].State
	publicationCatalog.mu.Unlock()
	if state != catalog.RawSegmentCommitted {
		t.Fatalf("catalog state = %q, want committed", state)
	}
}

func TestPublishRejectsSameKeyMismatchAndWrongApplicationHash(t *testing.T) {
	t.Run("different bytes", func(t *testing.T) {
		request := readyFixture(t, []byte("expected closed segment"))
		client := newFakeClient()
		wrong := []byte("different object bytes")
		client.store(request.Ready.Manifest.ObjectKey, wrong, sha256.Sum256(wrong))
		publisher, err := NewPublisher(client, newFakeCatalog(), PublisherConfig{})
		if err != nil {
			t.Fatalf("NewPublisher() error = %v", err)
		}
		_, err = publisher.Publish(t.Context(), request)
		if !errors.Is(err, ErrHashMismatch) && !errors.Is(err, ErrSizeMismatch) {
			t.Fatalf("Publish() mismatch error = %v, want hash or size mismatch", err)
		}
	})

	t.Run("wrong metadata despite matching ETag", func(t *testing.T) {
		request := readyFixture(t, []byte("exact object bytes"))
		client := newFakeClient()
		hash := request.Ready.Manifest.Segment.CompressedSHA256
		client.store(request.Ready.Manifest.ObjectKey, []byte("exact object bytes"), hash)
		client.mu.Lock()
		object := client.objects[request.Ready.Manifest.ObjectKey]
		wrongHash := hash
		wrongHash[0] ^= 0xff
		object.metadata[ApplicationSHA256Metadata] = strings.Repeat("0", 64)
		object.etag = string(hash[:])
		client.objects[request.Ready.Manifest.ObjectKey] = object
		client.mu.Unlock()
		publisher, err := NewPublisher(client, newFakeCatalog(), PublisherConfig{})
		if err != nil {
			t.Fatalf("NewPublisher() error = %v", err)
		}
		_, err = publisher.Publish(t.Context(), request)
		if !errors.Is(err, ErrHashMismatch) {
			t.Fatalf("Publish() metadata error = %v, want ErrHashMismatch (wrong hash %x)", err, wrongHash)
		}
	})
}

func TestPublishMultipartAbortAndDroppedCompletionReconcile(t *testing.T) {
	payload := bytes.Repeat([]byte{0x6d}, int(minimumS3PartBytes+257))
	request := readyFixture(t, payload)

	t.Run("part failure aborts", func(t *testing.T) {
		client := newFakeClient()
		client.failPart = 2
		publicationCatalog := newFakeCatalog()
		publisher, err := NewPublisher(client, publicationCatalog, PublisherConfig{
			MultipartThreshold: 1,
			MultipartPartBytes: minimumS3PartBytes,
		})
		if err != nil {
			t.Fatalf("NewPublisher() error = %v", err)
		}
		_, err = publisher.Publish(t.Context(), request)
		if !errors.Is(err, ErrTransient) {
			t.Fatalf("Publish() part failure = %v, want ErrTransient", err)
		}
		client.mu.Lock()
		objects := len(client.objects)
		uploads := len(client.uploads)
		aborts := client.aborts
		reconciliations := client.reconciliations
		client.mu.Unlock()
		if objects != 0 || uploads != 0 || aborts != 1 || reconciliations == 0 {
			t.Fatalf("multipart cleanup objects=%d uploads=%d aborts=%d reconciliations=%d", objects, uploads, aborts, reconciliations)
		}
		publicationCatalog.mu.Lock()
		events := len(publicationCatalog.events)
		publicationCatalog.mu.Unlock()
		if events != 0 {
			t.Fatalf("catalog changed after failed multipart: %q", publicationCatalog.events)
		}
	})

	t.Run("completion response lost", func(t *testing.T) {
		client := newFakeClient()
		client.dropNextComplete = true
		publisher, err := NewPublisher(client, newFakeCatalog(), PublisherConfig{
			MultipartThreshold: 1,
			MultipartPartBytes: minimumS3PartBytes,
		})
		if err != nil {
			t.Fatalf("NewPublisher() error = %v", err)
		}
		result, err := publisher.Publish(t.Context(), request)
		if err != nil {
			t.Fatalf("Publish() dropped completion error = %v", err)
		}
		if !result.Recovered {
			t.Fatal("Publish() did not report dropped multipart completion recovery")
		}
		client.mu.Lock()
		uploads := len(client.uploads)
		reconciliations := client.reconciliations
		client.mu.Unlock()
		if uploads != 0 || reconciliations == 0 {
			t.Fatalf("multipart recovery uploads=%d reconciliations=%d", uploads, reconciliations)
		}
	})
}

func TestPublishProviderDisqualifiedWithoutConditionalCapability(t *testing.T) {
	request := readyFixture(t, []byte("provider capability"))
	client := newFakeClient()
	client.conditionalUnsupported = true
	publisher, err := NewPublisher(client, newFakeCatalog(), PublisherConfig{})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	_, err = publisher.Publish(context.Background(), request)
	if !errors.Is(err, ErrProviderDisqualified) {
		t.Fatalf("Publish() error = %v, want ErrProviderDisqualified", err)
	}
}

func TestPublishUsesExplicitImmutableManifestReconciler(t *testing.T) {
	request := readyFixture(t, []byte("provider-specific immutable manifest contract"))
	client := newFakeClient()
	client.conditionalUnsupported = true
	publisher, err := NewPublisher(client, newFakeCatalog(), PublisherConfig{Reconciler: client})
	if err != nil {
		t.Fatalf("NewPublisher() error = %v", err)
	}
	result, err := publisher.Publish(t.Context(), request)
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if result.State != catalog.RawSegmentCommitted {
		t.Fatalf("Publish() state = %q, want committed", result.State)
	}
}
