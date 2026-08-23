package warehouse

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"
)

// StoreIdentity names one concrete ClickHouse table set. Layout records schema
// compatibility, but the fence key is server/database/prefix so differently
// configured Loaders can never bypass the fence for the same named tables.
type StoreIdentity struct {
	ServerDigest string
	Database     string
	TablePrefix  string
	Layout       PartitionLayout
}

func (i StoreIdentity) validate() error {
	if i.ServerDigest == "" || len(i.ServerDigest) > MaxIdentityBytes || strings.IndexByte(i.ServerDigest, 0) >= 0 ||
		i.Database == "" || !identifierPattern.MatchString(i.Database) ||
		i.TablePrefix == "" || !identifierPattern.MatchString(i.TablePrefix) || !i.Layout.valid() {
		return fmt.Errorf("%w: concrete warehouse store identity", ErrInvalidWarehouseInput)
	}
	return nil
}

type storeFenceKey struct {
	serverDigest string
	database     string
	tablePrefix  string
}

func (i StoreIdentity) fenceKey() storeFenceKey {
	return storeFenceKey{serverDigest: i.ServerDigest, database: i.Database, tablePrefix: i.TablePrefix}
}

type Store interface {
	StoreIdentity() StoreIdentity
	EnsureSchema(context.Context) error
	BeginGeneration(context.Context, Generation) error
	Generation(context.Context, GenerationID) (Generation, bool, error)
	ExpectedEventIDs(context.Context, GenerationID) ([]EventID, error)
	ActualEventIDs(context.Context, GenerationID) ([]EventID, error)
	GenerationRowCount(context.Context, GenerationID) (uint64, error)
	InsertRows(context.Context, []Row) error
	SetGenerationState(context.Context, GenerationID, GenerationState, string, time.Time) error
	DeleteGeneration(context.Context, GenerationID) error
	DropPartition(context.Context, Partition) error
	Truncate(context.Context) error
}

// Loader is the canonical warehouse generation loader within one process.
// Its keyed fence is shared by every Loader and NativeStore in that process,
// but it is deliberately not a distributed lock. Root composition must run
// exactly one canonical loader process for a ClickHouse table set; multiple
// processes require a future external lease rather than implied safety here.
type Loader struct {
	store        Store
	storeID      StoreIdentity
	reader       ManifestReader
	serverDigest string
	config       Config
	now          func() time.Time
}

type generationLockEntry struct {
	mu   sync.Mutex
	refs int
}

type generationLockRegistry struct {
	mu      sync.Mutex
	entries map[GenerationID]*generationLockEntry
}

type storeFenceEntry struct {
	mu   sync.RWMutex
	refs int
}

type storeFenceRegistry struct {
	mu      sync.Mutex
	entries map[storeFenceKey]*storeFenceEntry
}

var (
	canonicalGenerationLocks generationLockRegistry
	canonicalStoreFences     storeFenceRegistry
)

func (r *generationLockRegistry) acquire(id GenerationID) func() {
	r.mu.Lock()
	if r.entries == nil {
		r.entries = make(map[GenerationID]*generationLockEntry)
	}
	entry := r.entries[id]
	if entry == nil {
		entry = &generationLockEntry{}
		r.entries[id] = entry
	}
	entry.refs++
	r.mu.Unlock()
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		r.mu.Lock()
		entry.refs--
		if entry.refs == 0 && r.entries[id] == entry {
			delete(r.entries, id)
		}
		r.mu.Unlock()
	}
}

func (r *storeFenceRegistry) acquire(identity StoreIdentity, exclusive bool) func() {
	key := identity.fenceKey()
	r.mu.Lock()
	if r.entries == nil {
		r.entries = make(map[storeFenceKey]*storeFenceEntry)
	}
	entry := r.entries[key]
	if entry == nil {
		entry = &storeFenceEntry{}
		r.entries[key] = entry
	}
	entry.refs++
	r.mu.Unlock()
	if exclusive {
		entry.mu.Lock()
	} else {
		entry.mu.RLock()
	}
	return func() {
		if exclusive {
			entry.mu.Unlock()
		} else {
			entry.mu.RUnlock()
		}
		r.mu.Lock()
		entry.refs--
		if entry.refs == 0 && r.entries[key] == entry {
			delete(r.entries, key)
		}
		r.mu.Unlock()
	}
}

func acquireStoreLoadFence(identity StoreIdentity) func() {
	return canonicalStoreFences.acquire(identity, false)
}

func acquireStoreRebuildFence(identity StoreIdentity) func() {
	return canonicalStoreFences.acquire(identity, true)
}

func acquireGenerationLocks(plans []Generation) func() {
	ids := make([]GenerationID, len(plans))
	for i := range plans {
		ids[i] = plans[i].ID
	}
	slices.SortFunc(ids, compareHash)
	ids = slices.Compact(ids)
	releases := make([]func(), 0, len(ids))
	for _, id := range ids {
		releases = append(releases, canonicalGenerationLocks.acquire(id))
	}
	return func() {
		for i := len(releases) - 1; i >= 0; i-- {
			releases[i]()
		}
	}
}

func NewLoader(store Store, reader ManifestReader, serverDigest string, config Config) (*Loader, error) {
	if store == nil || reader == nil || serverDigest == "" || len(serverDigest) > MaxIdentityBytes {
		return nil, fmt.Errorf("%w: store, manifest reader, and server digest are required", ErrInvalidWarehouseInput)
	}
	storeID := store.StoreIdentity()
	if err := storeID.validate(); err != nil {
		return nil, err
	}
	if storeID.ServerDigest != serverDigest {
		return nil, fmt.Errorf("%w: loader and store server identity differ", ErrInvalidWarehouseInput)
	}
	normalized, err := config.normalized()
	if err != nil {
		return nil, err
	}
	if storeID.Layout != normalized.Layout {
		return nil, fmt.Errorf("%w: loader and store partition layout differ", ErrInvalidWarehouseInput)
	}
	return &Loader{store: store, storeID: storeID, reader: reader, serverDigest: serverDigest, config: normalized, now: time.Now}, nil
}

func (l *Loader) Load(ctx context.Context, input CommittedManifest) (LoadReceipt, error) {
	releaseStore := acquireStoreLoadFence(l.storeID)
	defer releaseStore()
	if err := l.store.EnsureSchema(ctx); err != nil {
		return LoadReceipt{}, fmt.Errorf("warehouse: ensure ClickHouse schema: %w", err)
	}
	generation, err := l.reader.Plan(ctx, input, l.serverDigest, l.config.Layout)
	if err != nil {
		return LoadReceipt{}, err
	}
	release := canonicalGenerationLocks.acquire(generation.ID)
	defer release()
	existing, found, err := l.store.Generation(ctx, generation.ID)
	if err != nil {
		return LoadReceipt{}, fmt.Errorf("warehouse: read load generation: %w", err)
	}
	if !found {
		return l.loadFresh(ctx, input, generation, true, false)
	}
	if !sameGeneration(existing, generation) {
		return LoadReceipt{}, ErrGenerationConflict
	}
	exact, err := l.reconcileExact(ctx, generation)
	if err != nil {
		return LoadReceipt{}, err
	}
	if exact {
		if existing.State != GenerationCommitted {
			if err := l.store.SetGenerationState(ctx, generation.ID, GenerationCommitted, "", l.now().UTC()); err != nil {
				return LoadReceipt{}, fmt.Errorf("warehouse: commit reconciled generation: %w", err)
			}
		}
		return receiptFor(generation, false, existing.State == GenerationUnknown), nil
	}
	if err := l.store.DeleteGeneration(ctx, generation.ID); err != nil {
		return LoadReceipt{}, fmt.Errorf("warehouse: delete mismatched generation: %w", err)
	}
	return l.loadFresh(ctx, input, generation, false, true)
}

// Reconcile never repeats a batch. It commits only an exact persisted expected
// event set plus exact actual event set and physical row count. Any mismatch is
// deleted and rebuilt once from the same verified committed Parquet manifest.
func (l *Loader) Reconcile(ctx context.Context, input CommittedManifest) (LoadReceipt, error) {
	releaseStore := acquireStoreLoadFence(l.storeID)
	defer releaseStore()
	generation, err := l.reader.Plan(ctx, input, l.serverDigest, l.config.Layout)
	if err != nil {
		return LoadReceipt{}, err
	}
	release := canonicalGenerationLocks.acquire(generation.ID)
	defer release()
	existing, found, err := l.store.Generation(ctx, generation.ID)
	if err != nil {
		return LoadReceipt{}, err
	}
	if found && !sameGeneration(existing, generation) {
		return LoadReceipt{}, ErrGenerationConflict
	}
	if found {
		exact, exactErr := l.reconcileExact(ctx, generation)
		if exactErr != nil {
			return LoadReceipt{}, exactErr
		}
		if exact {
			if err := l.store.SetGenerationState(ctx, generation.ID, GenerationCommitted, "", l.now().UTC()); err != nil {
				return LoadReceipt{}, err
			}
			return receiptFor(generation, false, true), nil
		}
	}
	if found {
		if err := l.store.DeleteGeneration(ctx, generation.ID); err != nil {
			return LoadReceipt{}, err
		}
	}
	return l.loadFresh(ctx, input, generation, false, true)
}

func (l *Loader) Rebuild(ctx context.Context, input CommittedManifest) (LoadReceipt, error) {
	releaseStore := acquireStoreLoadFence(l.storeID)
	defer releaseStore()
	generation, err := l.reader.Plan(ctx, input, l.serverDigest, l.config.Layout)
	if err != nil {
		return LoadReceipt{}, err
	}
	release := canonicalGenerationLocks.acquire(generation.ID)
	defer release()
	if err := l.store.DeleteGeneration(ctx, generation.ID); err != nil {
		return LoadReceipt{}, fmt.Errorf("warehouse: delete generation for rebuild: %w", err)
	}
	return l.loadFresh(ctx, input, generation, false, true)
}

func (l *Loader) RebuildPartition(ctx context.Context, partition Partition, inputs []CommittedManifest) ([]LoadReceipt, error) {
	releaseStore := acquireStoreLoadFence(l.storeID)
	defer releaseStore()
	if err := partition.validate(); err != nil {
		return nil, err
	}
	if partition.Layout != l.config.Layout || len(inputs) == 0 {
		return nil, fmt.Errorf("%w: partition layout or empty rebuild input", ErrInvalidWarehouseInput)
	}
	plans := make([]Generation, len(inputs))
	seen := make(map[GenerationID]struct{}, len(inputs))
	for i, input := range inputs {
		plan, err := l.reader.Plan(ctx, input, l.serverDigest, l.config.Layout)
		if err != nil {
			return nil, err
		}
		if plan.PartitionValue != partition.Value {
			return nil, fmt.Errorf("%w: manifest crosses rebuild partition", ErrInvalidWarehouseInput)
		}
		if _, exists := seen[plan.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate rebuild manifest", ErrInvalidWarehouseInput)
		}
		seen[plan.ID] = struct{}{}
		plans[i] = plan
	}
	release := acquireGenerationLocks(plans)
	defer release()
	if err := l.store.EnsureSchema(ctx); err != nil {
		return nil, err
	}
	if err := l.store.DropPartition(ctx, partition); err != nil {
		return nil, fmt.Errorf("warehouse: drop rebuildable partition: %w", err)
	}
	receipts := make([]LoadReceipt, 0, len(inputs))
	for i, input := range inputs {
		receipt, err := l.loadFresh(ctx, input, plans[i], false, true)
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, receipt)
	}
	return receipts, nil
}

func (l *Loader) RebuildAll(ctx context.Context, inputs []CommittedManifest) ([]LoadReceipt, error) {
	releaseStore := acquireStoreLoadFence(l.storeID)
	defer releaseStore()
	plans, err := l.planFullRebuild(ctx, inputs)
	if err != nil {
		return nil, err
	}
	release := acquireGenerationLocks(plans)
	defer release()
	return l.rebuildAllPlanned(ctx, inputs, plans)
}

func (l *Loader) planFullRebuild(ctx context.Context, inputs []CommittedManifest) ([]Generation, error) {
	if len(inputs) == 0 {
		return nil, fmt.Errorf("%w: empty full rebuild input", ErrInvalidWarehouseInput)
	}
	plans := make([]Generation, len(inputs))
	seen := make(map[GenerationID]struct{}, len(inputs))
	for i, input := range inputs {
		plan, err := l.reader.Plan(ctx, input, l.serverDigest, l.config.Layout)
		if err != nil {
			return nil, err
		}
		if _, exists := seen[plan.ID]; exists {
			return nil, fmt.Errorf("%w: duplicate full rebuild manifest", ErrInvalidWarehouseInput)
		}
		seen[plan.ID] = struct{}{}
		plans[i] = plan
	}
	return plans, nil
}

// rebuildAllPlanned requires the caller to hold the store fence followed by
// every generation lock in plans. Disaster recovery retains both through its
// independent persisted-evidence comparison.
func (l *Loader) rebuildAllPlanned(ctx context.Context, inputs []CommittedManifest, plans []Generation) ([]LoadReceipt, error) {
	if len(inputs) != len(plans) || len(inputs) == 0 {
		return nil, fmt.Errorf("%w: full rebuild plan count", ErrInvalidWarehouseInput)
	}
	if err := l.store.EnsureSchema(ctx); err != nil {
		return nil, err
	}
	if err := l.store.Truncate(ctx); err != nil {
		return nil, fmt.Errorf("warehouse: empty rebuildable ClickHouse store: %w", err)
	}
	receipts := make([]LoadReceipt, 0, len(inputs))
	for i, input := range inputs {
		receipt, err := l.loadFresh(ctx, input, plans[i], false, true)
		if err != nil {
			return nil, err
		}
		receipts = append(receipts, receipt)
	}
	return receipts, nil
}

func (l *Loader) loadFresh(ctx context.Context, input CommittedManifest, generation Generation, allowRebuild, rebuilt bool) (LoadReceipt, error) {
	now := l.now().UTC()
	generation.State = GenerationPending
	generation.LastError = ""
	generation.CreatedAt = now
	generation.UpdatedAt = now
	if err := generation.validate(); err != nil {
		return LoadReceipt{}, err
	}
	if err := l.store.BeginGeneration(ctx, generation); err != nil {
		if errors.Is(err, ErrWriteOutcomeUnknown) {
			return l.resolveUnknown(ctx, input, generation, allowRebuild, rebuilt, err)
		}
		return LoadReceipt{}, fmt.Errorf("warehouse: begin generation: %w", err)
	}
	batch := make([]Row, 0, l.config.BatchRows)
	var scanned uint64
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := l.store.InsertRows(ctx, batch); err != nil {
			return err
		}
		batch = batch[:0]
		return nil
	}
	scanErr := l.reader.Scan(ctx, input, generation.ID, func(row Row) error {
		if row.GenerationID != generation.ID || row.ManifestHash != generation.ManifestHash {
			return ErrGenerationConflict
		}
		if _, found := slices.BinarySearchFunc(generation.ExpectedEventIDs, row.EventID, compareHash); !found {
			return fmt.Errorf("%w: Parquet event absent from planned exact set", ErrGenerationConflict)
		}
		if err := row.validate(); err != nil {
			return err
		}
		batch = append(batch, row)
		scanned++
		if len(batch) == cap(batch) {
			return flush()
		}
		return nil
	})
	if scanErr == nil {
		scanErr = flush()
	}
	if scanErr != nil {
		if errors.Is(scanErr, ErrWriteOutcomeUnknown) {
			_ = l.store.SetGenerationState(ctx, generation.ID, GenerationUnknown, boundedError(scanErr), l.now().UTC())
			return l.resolveUnknown(ctx, input, generation, allowRebuild, rebuilt, scanErr)
		}
		_ = l.store.SetGenerationState(ctx, generation.ID, GenerationFailed, boundedError(scanErr), l.now().UTC())
		return LoadReceipt{}, fmt.Errorf("warehouse: load generation rows: %w", scanErr)
	}
	if scanned != generation.ExpectedRowCount {
		err := fmt.Errorf("%w: scanned row count %d, expected %d", ErrReconciliationFailed, scanned, generation.ExpectedRowCount)
		_ = l.store.SetGenerationState(ctx, generation.ID, GenerationFailed, boundedError(err), l.now().UTC())
		return LoadReceipt{}, err
	}
	exact, err := l.reconcileExact(ctx, generation)
	if err != nil {
		return LoadReceipt{}, err
	}
	if !exact {
		if !allowRebuild {
			_ = l.store.SetGenerationState(ctx, generation.ID, GenerationFailed, ErrReconciliationFailed.Error(), l.now().UTC())
			return LoadReceipt{}, ErrReconciliationFailed
		}
		if err := l.store.DeleteGeneration(ctx, generation.ID); err != nil {
			return LoadReceipt{}, err
		}
		return l.loadFresh(ctx, input, generation, false, true)
	}
	if err := l.store.SetGenerationState(ctx, generation.ID, GenerationCommitted, "", l.now().UTC()); err != nil {
		return LoadReceipt{}, fmt.Errorf("warehouse: commit exact generation: %w", err)
	}
	return receiptFor(generation, rebuilt, false), nil
}

func (l *Loader) resolveUnknown(ctx context.Context, input CommittedManifest, generation Generation, allowRebuild, rebuilt bool, cause error) (LoadReceipt, error) {
	exact, err := l.reconcileExact(ctx, generation)
	if err != nil {
		return LoadReceipt{}, err
	}
	if exact {
		if err := l.store.SetGenerationState(ctx, generation.ID, GenerationCommitted, "", l.now().UTC()); err != nil {
			return LoadReceipt{}, err
		}
		receipt := receiptFor(generation, rebuilt, true)
		return receipt, nil
	}
	if err := l.store.DeleteGeneration(ctx, generation.ID); err != nil {
		return LoadReceipt{}, fmt.Errorf("warehouse: delete unknown partial generation: %w", err)
	}
	if !allowRebuild {
		return LoadReceipt{}, errors.Join(ErrReconciliationFailed, cause)
	}
	return l.loadFresh(ctx, input, generation, false, true)
}

func (l *Loader) reconcileExact(ctx context.Context, expected Generation) (bool, error) {
	actualGeneration, found, err := l.store.Generation(ctx, expected.ID)
	if err != nil {
		return false, fmt.Errorf("warehouse: reconcile generation metadata: %w", err)
	}
	if !found || !sameGeneration(actualGeneration, expected) {
		return false, nil
	}
	expectedIDs, err := l.store.ExpectedEventIDs(ctx, expected.ID)
	if err != nil {
		return false, fmt.Errorf("warehouse: reconcile persisted expected event IDs: %w", err)
	}
	actualIDs, err := l.store.ActualEventIDs(ctx, expected.ID)
	if err != nil {
		return false, fmt.Errorf("warehouse: reconcile actual event IDs: %w", err)
	}
	rowCount, err := l.store.GenerationRowCount(ctx, expected.ID)
	if err != nil {
		return false, fmt.Errorf("warehouse: reconcile physical row count: %w", err)
	}
	return slices.Equal(expectedIDs, expected.ExpectedEventIDs) && slices.Equal(actualIDs, expected.ExpectedEventIDs) &&
		rowCount == expected.ExpectedRowCount, nil
}

func sameGeneration(left, right Generation) bool {
	return left.ID == right.ID && left.ServerDigest == right.ServerDigest && left.ManifestHash == right.ManifestHash &&
		left.InputHash == right.InputHash && left.DatasetIdentity == right.DatasetIdentity && left.CatalogIdentity == right.CatalogIdentity &&
		left.SchemaIdentity == right.SchemaIdentity && left.Family == right.Family && left.SourceID == right.SourceID &&
		left.UTCDate == right.UTCDate && left.PartitionValue == right.PartitionValue && left.Layout == right.Layout &&
		left.ExpectedEventSetHash == right.ExpectedEventSetHash && left.ExpectedEventCount == right.ExpectedEventCount &&
		left.ExpectedRowCount == right.ExpectedRowCount
}

func boundedError(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > MaxGenerationError {
		return value[:MaxGenerationError]
	}
	return value
}

func receiptFor(generation Generation, rebuilt, reconciled bool) LoadReceipt {
	return LoadReceipt{GenerationID: generation.ID, ManifestHash: generation.ManifestHash, InputHash: generation.InputHash,
		ExpectedEventCount: generation.ExpectedEventCount, ExpectedRowCount: generation.ExpectedRowCount,
		Rebuilt: rebuilt, ReconciledUnknown: reconciled}
}
