package deployment

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/enable-xyz/marketdata/capture"
)

func TestWriterLeaseHandoff(t *testing.T) {
	manager, err := NewLeaseManager(NewMemoryLeaseStore())
	if err != nil {
		t.Fatal(err)
	}
	old, err := manager.Acquire(t.Context(), "binance-spot/trade", "writer-old")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Acquire(t.Context(), old.Key, "overlapping-writer"); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("overlapping Acquire() error = %v, want lease conflict", err)
	}

	var order []string
	newToken, err := manager.Handoff(t.Context(), old, "writer-new", func(ctx context.Context, token LeaseToken) error {
		order = append(order, "drain")
		if err := ctx.Err(); err != nil {
			return err
		}
		return manager.CommitFenced(ctx, token, func(context.Context) error { return nil })
	})
	if err != nil {
		t.Fatalf("Handoff() error = %v", err)
	}
	order = append(order, "new-acquired")
	if got := strings.Join(order, ","); got != "drain,new-acquired" {
		t.Fatalf("handoff order = %q", got)
	}
	if newToken.Fence != old.Fence+1 {
		t.Fatalf("new fence = %d, want %d", newToken.Fence, old.Fence+1)
	}
	if err := manager.Release(t.Context(), old); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("stale Release() error = %v, want lease conflict", err)
	}
	if err := manager.CommitFenced(t.Context(), newToken, func(context.Context) error { return nil }); err != nil {
		t.Fatalf("stale release cleared newer lease: %v", err)
	}
}

func TestWriterLeaseHandoffFailedDrainCannotAcquire(t *testing.T) {
	manager, err := NewLeaseManager(NewMemoryLeaseStore())
	if err != nil {
		t.Fatal(err)
	}
	old, err := manager.Acquire(t.Context(), "bybit-v5/orderbook", "writer-old")
	if err != nil {
		t.Fatal(err)
	}
	drainErr := errors.New("durable queue did not drain")
	if _, err := manager.Handoff(t.Context(), old, "writer-new", func(context.Context, LeaseToken) error {
		return drainErr
	}); !errors.Is(err, ErrDrainFailed) {
		t.Fatalf("Handoff() error = %v, want drain failure", err)
	}
	if _, err := manager.Acquire(t.Context(), old.Key, "writer-new"); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("Acquire() after failed drain error = %v, want lease conflict", err)
	}
}

type interleavingLeaseStore struct {
	*MemoryLeaseStore
	handoffLoad chan struct{}
}

func (s *interleavingLeaseStore) Load(ctx context.Context, key string) (LeaseRecord, bool, error) {
	if s.handoffLoad != nil {
		close(s.handoffLoad)
		s.handoffLoad = nil
	}
	return s.MemoryLeaseStore.Load(ctx, key)
}

type blockingDurableBoundary struct {
	started     chan struct{}
	startedOnce sync.Once
	release     chan struct{}
	messages    []capture.RawMessage
	commits     int
}

func (b *blockingDurableBoundary) WriteRaw(_ context.Context, message capture.RawMessage) error {
	b.messages = append(b.messages, message)
	return nil
}

func (b *blockingDurableBoundary) FlushCommit(context.Context) (capture.DurableCommit, error) {
	if b.started != nil {
		b.startedOnce.Do(func() { close(b.started) })
	}
	if b.release != nil {
		<-b.release
	}
	b.commits++
	return capture.DurableCommit{SegmentID: "segment", LastCoordinate: b.messages[len(b.messages)-1].Coordinate}, nil
}

func TestFencedCommitExcludesAtomicHandoffInterleaving(t *testing.T) {
	store := &interleavingLeaseStore{MemoryLeaseStore: NewMemoryLeaseStore()}
	manager, err := NewLeaseManager(store)
	if err != nil {
		t.Fatal(err)
	}
	old, err := manager.Acquire(t.Context(), "binance-spot/trade", "writer-old")
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	durable := &blockingDurableBoundary{started: started, release: make(chan struct{})}
	oldBoundary, err := NewFencedDurableBoundary(manager, old, durable)
	if err != nil {
		t.Fatal(err)
	}
	if err := oldBoundary.WriteRaw(t.Context(), capture.RawMessage{Coordinate: "epoch/1"}); err != nil {
		t.Fatal(err)
	}

	commitDone := make(chan error, 1)
	go func() {
		_, err := oldBoundary.FlushCommit(t.Context())
		commitDone <- err
	}()
	<-started

	handoffEntered := make(chan struct{})
	store.handoffLoad = handoffEntered
	type handoffResult struct {
		token LeaseToken
		err   error
	}
	handoffDone := make(chan handoffResult, 1)
	go func() {
		token, err := manager.Handoff(t.Context(), old, "writer-new", func(context.Context, LeaseToken) error { return nil })
		handoffDone <- handoffResult{token: token, err: err}
	}()
	<-handoffEntered
	close(durable.release)
	if err := <-commitDone; err != nil {
		t.Fatalf("old FlushCommit() before handoff error = %v", err)
	}
	handoff := <-handoffDone
	if handoff.err != nil {
		t.Fatalf("Handoff() error = %v", handoff.err)
	}
	if durable.commits != 1 {
		t.Fatalf("durable commits before stale retry = %d, want 1", durable.commits)
	}
	if _, err := oldBoundary.FlushCommit(t.Context()); !errors.Is(err, ErrLeaseConflict) {
		t.Fatalf("stale FlushCommit() error = %v, want lease conflict", err)
	}
	if durable.commits != 1 {
		t.Fatalf("stale writer reached durable commit: commits = %d", durable.commits)
	}
	newBoundary, err := NewFencedDurableBoundary(manager, handoff.token, durable)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newBoundary.FlushCommit(t.Context()); err != nil {
		t.Fatalf("new FlushCommit() error = %v", err)
	}
	if durable.commits != 2 {
		t.Fatalf("new writer durable commits = %d, want 2 total", durable.commits)
	}
}
