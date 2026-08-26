package deployment

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/enable-xyz/marketdata/capture"
)

var (
	ErrLeaseConflict = errors.New("canonical writer lease conflict")
	ErrDrainFailed   = errors.New("canonical writer drain failed")
)

type LeaseState string

const (
	LeaseActive   LeaseState = "active"
	LeaseDraining LeaseState = "draining"
	LeaseReleased LeaseState = "released"
)

// LeaseRecord is the durable compare-and-swap value. Fence never decreases or
// resets when a holder releases the lease.
type LeaseRecord struct {
	Key     string     `json:"key"`
	Holder  string     `json:"holder,omitempty"`
	Fence   uint64     `json:"fence"`
	State   LeaseState `json:"state"`
	Version uint64     `json:"version"`
}

type LeaseToken struct {
	Key    string `json:"key"`
	Holder string `json:"holder"`
	Fence  uint64 `json:"fence"`
}

// LeaseStore must serialize fenced commits with lease transitions. CommitFenced
// validates the exact token and executes commit in the same transaction, CAS,
// or critical section that excludes Acquire, BeginDrain, and Release.
// expectedVersion is zero only when the key must not exist.
type LeaseStore interface {
	Load(context.Context, string) (LeaseRecord, bool, error)
	CompareAndSwap(context.Context, string, uint64, LeaseRecord) error
	CommitFenced(context.Context, LeaseToken, func(context.Context) error) error
}

type LeaseManager struct{ store LeaseStore }

func NewLeaseManager(store LeaseStore) (*LeaseManager, error) {
	if store == nil {
		return nil, errors.New("canonical writer lease store is required")
	}
	return &LeaseManager{store: store}, nil
}

func (m *LeaseManager) Acquire(ctx context.Context, key, holder string) (LeaseToken, error) {
	if key == "" || holder == "" {
		return LeaseToken{}, errors.New("canonical writer lease key and holder are required")
	}
	current, exists, err := m.store.Load(ctx, key)
	if err != nil {
		return LeaseToken{}, fmt.Errorf("loading canonical writer lease: %w", err)
	}
	if exists && current.State != LeaseReleased {
		return LeaseToken{}, fmt.Errorf("%w: %s is %s under fence %d", ErrLeaseConflict, key, current.State, current.Fence)
	}
	if exists && (current.Key != key || current.Fence == ^uint64(0) || current.Version == ^uint64(0)) {
		return LeaseToken{}, fmt.Errorf("%w: invalid persisted lease state", ErrLeaseConflict)
	}
	fence := uint64(1)
	expectedVersion := uint64(0)
	if exists {
		fence = current.Fence + 1
		expectedVersion = current.Version
	}
	next := LeaseRecord{Key: key, Holder: holder, Fence: fence, State: LeaseActive, Version: expectedVersion + 1}
	if err := m.store.CompareAndSwap(ctx, key, expectedVersion, next); err != nil {
		return LeaseToken{}, fmt.Errorf("%w: acquiring %s: %v", ErrLeaseConflict, key, err)
	}
	return LeaseToken{Key: key, Holder: holder, Fence: fence}, nil
}

func (m *LeaseManager) BeginDrain(ctx context.Context, token LeaseToken) error {
	current, err := m.loadOwned(ctx, token, LeaseActive)
	if err != nil {
		return err
	}
	next := current
	next.State = LeaseDraining
	next.Version++
	if err := m.store.CompareAndSwap(ctx, token.Key, current.Version, next); err != nil {
		return fmt.Errorf("%w: beginning drain: %v", ErrLeaseConflict, err)
	}
	return nil
}

func (m *LeaseManager) Release(ctx context.Context, token LeaseToken) error {
	current, err := m.loadOwned(ctx, token, LeaseDraining)
	if err != nil {
		return err
	}
	next := current
	next.Holder = ""
	next.State = LeaseReleased
	next.Version++
	if err := m.store.CompareAndSwap(ctx, token.Key, current.Version, next); err != nil {
		return fmt.Errorf("%w: releasing: %v", ErrLeaseConflict, err)
	}
	return nil
}

// CommitFenced executes a canonical durable commit only while token names the
// current active or draining writer. Validation and commit are one storage
// critical section, so a handoff cannot interleave between them.
func (m *LeaseManager) CommitFenced(ctx context.Context, token LeaseToken, commit func(context.Context) error) error {
	if commit == nil {
		return errors.New("canonical durable commit function is required")
	}
	if token.Key == "" || token.Holder == "" || token.Fence == 0 {
		return fmt.Errorf("%w: incomplete fencing identity", ErrLeaseConflict)
	}
	if err := m.store.CommitFenced(ctx, token, commit); err != nil {
		return fmt.Errorf("committing canonical durable boundary: %w", err)
	}
	return nil
}

// FencedDurableBoundary binds a collector's canonical FlushCommit to one
// source/channel lease token. Raw messages may be staged, but they cannot
// become canonical unless the token is still current at the durable boundary.
type FencedDurableBoundary struct {
	manager *LeaseManager
	token   LeaseToken
	next    capture.DurableBoundary
}

func NewFencedDurableBoundary(manager *LeaseManager, token LeaseToken, next capture.DurableBoundary) (*FencedDurableBoundary, error) {
	if manager == nil || next == nil {
		return nil, errors.New("lease manager and durable boundary are required")
	}
	if token.Key == "" || token.Holder == "" || token.Fence == 0 {
		return nil, fmt.Errorf("%w: incomplete fencing identity", ErrLeaseConflict)
	}
	return &FencedDurableBoundary{manager: manager, token: token, next: next}, nil
}

func (b *FencedDurableBoundary) WriteRaw(ctx context.Context, message capture.RawMessage) error {
	return b.next.WriteRaw(ctx, message)
}

func (b *FencedDurableBoundary) FlushCommit(ctx context.Context) (capture.DurableCommit, error) {
	var committed capture.DurableCommit
	err := b.manager.CommitFenced(ctx, b.token, func(ctx context.Context) error {
		var err error
		committed, err = b.next.FlushCommit(ctx)
		return err
	})
	if err != nil {
		return capture.DurableCommit{}, err
	}
	return committed, nil
}

type DrainFunc func(context.Context, LeaseToken) error

// Handoff enforces acquire(old) -> drain(old) -> release(old) -> acquire(new).
// A failed drain deliberately leaves the old lease in draining state so a new
// writer cannot acquire and overlap it.
func (m *LeaseManager) Handoff(ctx context.Context, old LeaseToken, newHolder string, drain DrainFunc) (LeaseToken, error) {
	if drain == nil {
		return LeaseToken{}, errors.New("canonical writer drain function is required")
	}
	if err := m.BeginDrain(ctx, old); err != nil {
		return LeaseToken{}, err
	}
	if err := drain(ctx, old); err != nil {
		return LeaseToken{}, fmt.Errorf("%w: %v", ErrDrainFailed, err)
	}
	if err := m.Release(ctx, old); err != nil {
		return LeaseToken{}, err
	}
	return m.Acquire(ctx, old.Key, newHolder)
}

func (m *LeaseManager) loadOwned(ctx context.Context, token LeaseToken, required LeaseState) (LeaseRecord, error) {
	if token.Key == "" || token.Holder == "" || token.Fence == 0 {
		return LeaseRecord{}, fmt.Errorf("%w: incomplete fencing identity", ErrLeaseConflict)
	}
	current, exists, err := m.store.Load(ctx, token.Key)
	if err != nil {
		return LeaseRecord{}, fmt.Errorf("loading canonical writer lease: %w", err)
	}
	if !exists || current.Key != token.Key || current.Holder != token.Holder || current.Fence != token.Fence || current.State != required {
		return LeaseRecord{}, fmt.Errorf("%w: stale or out-of-order lease transition", ErrLeaseConflict)
	}
	return current, nil
}

// MemoryLeaseStore is a linearizable in-process implementation for dry-runs
// and deterministic tests. Deployments supply their own durable CAS store.
type MemoryLeaseStore struct {
	mu      sync.Mutex
	records map[string]LeaseRecord
}

func NewMemoryLeaseStore() *MemoryLeaseStore {
	return &MemoryLeaseStore{records: make(map[string]LeaseRecord)}
}

func (s *MemoryLeaseStore) Load(ctx context.Context, key string) (LeaseRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return LeaseRecord{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, exists := s.records[key]
	return record, exists, nil
}

func (s *MemoryLeaseStore) CompareAndSwap(ctx context.Context, key string, expectedVersion uint64, next LeaseRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.records[key]
	if (!exists && expectedVersion != 0) || (exists && current.Version != expectedVersion) {
		return ErrLeaseConflict
	}
	if next.Key != key || next.Version != expectedVersion+1 || next.Fence == 0 {
		return errors.New("invalid canonical writer compare-and-swap value")
	}
	s.records[key] = next
	return nil
}

func (s *MemoryLeaseStore) CommitFenced(ctx context.Context, token LeaseToken, commit func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if commit == nil {
		return errors.New("canonical durable commit function is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current, exists := s.records[token.Key]
	if !exists || current.Key != token.Key || current.Holder != token.Holder || current.Fence != token.Fence ||
		(current.State != LeaseActive && current.State != LeaseDraining) {
		return fmt.Errorf("%w: stale writer fence", ErrLeaseConflict)
	}
	return commit(ctx)
}

var _ LeaseStore = (*MemoryLeaseStore)(nil)
