package warehouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoadReconcileModel(t *testing.T) {
	reader := fakeManifestReader{generation: fakeGeneration(100), rows: fakeRows(100)}
	for disconnectPoint := range X5DisconnectPoints {
		t.Run(fmt.Sprint(disconnectPoint), func(t *testing.T) {
			store := &fakeStore{disconnectPoint: disconnectPoint, disconnectArmed: true}
			loader, err := NewLoader(store, reader, "sha256:test-clickhouse-server", Config{BatchRows: 1,
				Compression: CompressionLZ4, Layout: PartitionMonth})
			if err != nil {
				t.Fatal(err)
			}
			receipt, err := loader.Load(t.Context(), CommittedManifest{Root: "synthetic", ManifestPath: "manifest.json",
				ManifestSHA256: reader.generation.ManifestHash, State: ManifestCommitted})
			if err != nil {
				t.Fatalf("disconnect point %d: %v", disconnectPoint, err)
			}
			actual, err := store.ActualEventIDs(t.Context(), reader.generation.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(actual, reader.generation.ExpectedEventIDs) || uint64(len(store.rows)) != reader.generation.ExpectedRowCount {
				t.Fatalf("disconnect point %d lost exact set: ids=%d rows=%d", disconnectPoint, len(actual), len(store.rows))
			}
			if disconnectPoint == X5DisconnectPoints-1 {
				if !receipt.ReconciledUnknown || receipt.Rebuilt {
					t.Fatalf("last acknowledgement should reconcile exact set without rebuild: %#v", receipt)
				}
			} else if !receipt.Rebuilt {
				t.Fatalf("partial generation at disconnect point %d was not rebuilt", disconnectPoint)
			}
		})
	}
	canonicalGenerationLocks.mu.Lock()
	remainingLocks := len(canonicalGenerationLocks.entries)
	canonicalGenerationLocks.mu.Unlock()
	if remainingLocks != 0 {
		t.Fatalf("generation lock registry retained %d unused entries", remainingLocks)
	}
	canonicalStoreFences.mu.Lock()
	remainingFences := len(canonicalStoreFences.entries)
	canonicalStoreFences.mu.Unlock()
	if remainingFences != 0 {
		t.Fatalf("store fence registry retained %d unused entries", remainingFences)
	}
}

func TestFullRebuild(t *testing.T) {
	reader := fakeManifestReader{generation: fakeGeneration(4), rows: fakeRows(4)}
	input := CommittedManifest{Root: "synthetic", ManifestPath: "manifest.json",
		ManifestSHA256: reader.generation.ManifestHash, State: ManifestCommitted}
	store := &fakeStore{}
	loader, err := NewLoader(store, reader, "sha256:test-clickhouse-server",
		Config{BatchRows: 2, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	report, err := loader.FullRebuild(t.Context(), []CommittedManifest{input})
	if err != nil {
		t.Fatalf("FullRebuild() error = %v", err)
	}
	if !slices.Equal(report.Source.EventIDs, reader.generation.ExpectedEventIDs) ||
		!slices.Equal(report.Source.EventIDs, report.Target.EventIDs) ||
		report.Source.EventSetSHA256 != report.Target.EventSetSHA256 ||
		report.Source.LogicalSHA256 != report.Target.LogicalSHA256 ||
		report.Source.AggregateSHA256 != report.Target.AggregateSHA256 ||
		report.Source.RowCount != 4 || report.Target.RowCount != 4 {
		t.Fatalf("full rebuild evidence mismatch: %#v", report)
	}

	corruptStore := &fakeStore{corruptRebuildEvidence: true}
	corruptLoader, err := NewLoader(corruptStore, reader, "sha256:test-clickhouse-server",
		Config{BatchRows: 2, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	corruptReport, err := corruptLoader.FullRebuild(t.Context(), []CommittedManifest{input})
	if !errors.Is(err, ErrFullRebuildMismatch) {
		t.Fatalf("serving-value corrupt FullRebuild() error = %v, want ErrFullRebuildMismatch", err)
	}
	if !slices.Equal(corruptReport.Source.EventIDs, corruptReport.Target.EventIDs) ||
		corruptReport.Source.LogicalSHA256 == corruptReport.Target.LogicalSHA256 ||
		corruptReport.Source.AggregateSHA256 == corruptReport.Target.AggregateSHA256 ||
		reader.rows[0].LogicalHash != corruptStore.rows[0].LogicalHash {
		t.Fatalf("price corruption retaining logical_hash was not detected independently: %#v", corruptReport)
	}
}

func TestCanonicalRebuildRowHashUsesActualServingValues(t *testing.T) {
	row := fakeRows(1)[0]
	baseline, err := canonicalRebuildRowHash(row)
	if err != nil {
		t.Fatal(err)
	}
	changedClaim := row
	changedClaim.LogicalHash = testWarehouseHash(250)
	claimHash, err := canonicalRebuildRowHash(changedClaim)
	if err != nil {
		t.Fatal(err)
	}
	if claimHash != baseline {
		t.Fatal("persisted logical_hash claim changed independent actual-row evidence")
	}
	changedAmount := row
	amount := *row.Amount
	amount.Coefficient = "7500000000000000000"
	changedAmount.Amount = &amount
	amountHash, err := canonicalRebuildRowHash(changedAmount)
	if err != nil {
		t.Fatal(err)
	}
	if amountHash == baseline {
		t.Fatal("query-bearing amount was absent from actual-row evidence")
	}
}
func TestFullRebuildRejectsUnexpectedGeneration(t *testing.T) {
	reader := fakeManifestReader{generation: fakeGeneration(2), rows: fakeRows(2)}
	input := CommittedManifest{
		Root: "synthetic", ManifestPath: "manifest.json",
		ManifestSHA256: reader.generation.ManifestHash, State: ManifestCommitted,
	}
	store := &fakeStore{unexpectedGeneration: true}
	loader, err := NewLoader(store, reader, "sha256:test-clickhouse-server",
		Config{BatchRows: 2, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	report, err := loader.FullRebuild(t.Context(), []CommittedManifest{input})
	if !errors.Is(err, ErrFullRebuildMismatch) {
		t.Fatalf("FullRebuild() error = %v, want ErrFullRebuildMismatch", err)
	}
	if len(report.ExpectedGenerationIDs) != 1 || len(report.PersistedGenerationIDs) != 2 ||
		slices.Equal(report.ExpectedGenerationIDs, report.PersistedGenerationIDs) {
		t.Fatalf("unexpected persisted generation was not retained in evidence: %#v", report)
	}
}

func TestFullRebuildFencesLoadFromSecondLoader(t *testing.T) {
	reader := fakeManifestReader{generation: fakeGeneration(2), rows: fakeRows(2)}
	input := CommittedManifest{
		Root: "synthetic", ManifestPath: "manifest.json",
		ManifestSHA256: reader.generation.ManifestHash, State: ManifestCommitted,
	}
	store := &fakeStore{
		verificationStarted: make(chan struct{}),
		releaseVerification: make(chan struct{}),
	}
	rebuildLoader, err := NewLoader(store, reader, "sha256:test-clickhouse-server",
		Config{BatchRows: 2, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	secondReader := differentFakeManifestReader()
	secondLoader, err := NewLoader(store, secondReader, "sha256:test-clickhouse-server",
		Config{BatchRows: 1, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}

	fullDone := make(chan error, 1)
	go func() {
		_, rebuildErr := rebuildLoader.FullRebuild(t.Context(), []CommittedManifest{input})
		fullDone <- rebuildErr
	}()
	<-store.verificationStarted

	loadStarted := make(chan struct{})
	loadDone := make(chan error, 1)
	go func() {
		close(loadStarted)
		_, loadErr := secondLoader.Load(t.Context(), CommittedManifest{
			Root: "synthetic-second", ManifestPath: "manifest.json",
			ManifestSHA256: secondReader.generation.ManifestHash, State: ManifestCommitted,
		})
		loadDone <- loadErr
	}()
	<-loadStarted
	waitForStoreFenceRefs(t, rebuildLoader.storeID, 2)
	store.mu.Lock()
	interleavedBeforeRelease := store.interleaved
	store.mu.Unlock()
	close(store.releaseVerification)
	if err := <-fullDone; err != nil {
		t.Fatalf("FullRebuild() error = %v", err)
	}
	if err := <-loadDone; err != nil {
		t.Fatalf("second Loader.Load() error = %v", err)
	}
	store.mu.Lock()
	interleavedAfterRelease := store.interleaved
	store.mu.Unlock()
	if interleavedBeforeRelease || interleavedAfterRelease {
		t.Fatal("second loader interleaved with truncate, reload, or verification")
	}
	rebuildRows, err := store.GenerationRowCount(t.Context(), reader.generation.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondRows, err := store.GenerationRowCount(t.Context(), secondReader.generation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rebuildRows != 2 || secondRows != 1 {
		t.Fatalf("serialized loaders lost rows: rebuild=%d second=%d", rebuildRows, secondRows)
	}
	canonicalStoreFences.mu.Lock()
	_, retainedFence := canonicalStoreFences.entries[rebuildLoader.storeID.fenceKey()]
	canonicalStoreFences.mu.Unlock()
	if retainedFence {
		t.Fatal("full rebuild store fence remained registered after both loaders completed")
	}
}

func waitForStoreFenceRefs(t *testing.T, identity StoreIdentity, minimum int) {
	t.Helper()
	for range 100_000 {
		canonicalStoreFences.mu.Lock()
		entry := canonicalStoreFences.entries[identity.fenceKey()]
		refs := 0
		if entry != nil {
			refs = entry.refs
		}
		canonicalStoreFences.mu.Unlock()
		if refs >= minimum {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("store fence never observed %d concurrent users", minimum)
}

func TestSchemaDefaults(t *testing.T) {
	statements, err := (Schema{Database: "marketdata_test", Prefix: "warehouse", Layout: PartitionMonth}).Statements()
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(statements, "\n")
	for _, required := range []string{
		"ENGINE = MergeTree", "toYYYYMM(fromUnixTimestamp64Nano(received_time_ns), 'UTC')",
		"ORDER BY (source_id, instrument_uid, received_time_ns, connection_epoch, arrival_ordinal, message_ordinal)",
		"price Nullable(Decimal(38, 18))", "price_change_percent Nullable(Decimal(38, 8))",
	} {
		if !strings.Contains(joined, required) {
			t.Fatalf("schema is missing %q", required)
		}
	}
	if strings.Contains(joined, "ReplacingMergeTree") {
		t.Fatal("v1 schema must not rely on asynchronous replacement semantics")
	}
}

func TestX5ExperimentContract(t *testing.T) {
	variants := X5Variants()
	if len(variants) != 12 {
		t.Fatalf("variant count = %d, want 12", len(variants))
	}
	seen := make(map[X5Variant]struct{}, len(variants))
	for _, variant := range variants {
		seen[variant] = struct{}{}
	}
	if len(seen) != 12 || X5Rows != 10_000_000 || X5DisconnectPoints != 100 {
		t.Fatalf("X5 matrix narrowed: variants=%d rows=%d disconnects=%d", len(seen), X5Rows, X5DisconnectPoints)
	}
	schedule := X5DisconnectSchedule(X5DisconnectPoints)
	kinds := make(map[BatchKind]int)
	manifests := make(map[int]struct{}, len(schedule))
	for point, selection := range schedule {
		if selection.Point != point || selection.ManifestOrdinal != point || selection.BatchOrdinal != 0 {
			t.Fatalf("disconnect selection %d is not a distinct one-batch manifest point: %#v", point, selection)
		}
		manifests[selection.ManifestOrdinal] = struct{}{}
		kinds[selection.BatchKind]++
	}
	for _, kind := range []BatchKind{BatchGeneration, BatchExpectedIDs, BatchEvents} {
		if kinds[kind] == 0 {
			t.Fatalf("disconnect schedule does not select %q batches", kind)
		}
	}
	if len(schedule) != X5DisconnectPoints {
		t.Fatalf("disconnect schedule length = %d, want %d", len(schedule), X5DisconnectPoints)
	}
	if len(manifests) != X5DisconnectPoints || X5DisconnectSchedule(X5DisconnectPoints-1) != nil {
		t.Fatalf("disconnect schedule must bind exactly 100 distinct manifests")
	}
	if _, err := FreezeX5(X5MeasuredResult{}); !errors.Is(err, ErrMeasuredResultRequired) {
		t.Fatalf("unmeasured result freeze error = %v", err)
	}
}

func TestPinnedX5ProductionSelection(t *testing.T) {
	selection := PinnedX5ProductionSelection()
	if selection.ServerDigest != PinnedX5ServerDigest || selection.Config != DefaultConfig() {
		t.Fatalf("pinned production selection diverges from defaults: %#v", selection)
	}
	if selection.Config != (Config{BatchRows: 100_000, Compression: CompressionLZ4, Layout: PartitionMonth}) ||
		selection.SelectionRule != PinnedX5SelectionRule {
		t.Fatalf("unexpected pinned production decision: %#v", selection)
	}
	fixture, err := ReadPinnedX5Fixture()
	if err != nil {
		t.Fatal(err)
	}
	fastest, err := fastestInvariantX5Case(fixture)
	if err != nil {
		t.Fatal(err)
	}
	pinnedVariant := X5Variant{BatchRows: selection.Config.BatchRows, Compression: selection.Config.Compression,
		Layout: selection.Config.Layout}
	faultConfig := disconnectNativeConfig(NativeConfig{}, "x5", pinnedVariant, nil)
	if faultConfig.BatchRows != pinnedVariant.BatchRows || faultConfig.Compression != pinnedVariant.Compression ||
		faultConfig.Layout != pinnedVariant.Layout {
		t.Fatalf("disconnect matrix lowered or changed the selected variant: %#v", faultConfig)
	}
	if fixture.ServerDigest != selection.ServerDigest || fixture.DisconnectVariant != (X5Variant{}) ||
		fastest.MaxIngestDurationNS != selection.MaxIngestDurationNS ||
		fastest.ExpectedEventSet != selection.ExpectedEventSetSHA256 || fastest.Variant != pinnedVariant {
		t.Fatalf("pinned production decision is not the fastest invariant case: %#v", fastest)
	}
	legacyLast := fixture.DisconnectMatrix[len(fixture.DisconnectMatrix)-1]
	if legacyLast.ManifestOrdinal != 0 || legacyLast.BatchKind != BatchEvents || legacyLast.BatchOrdinal != 99 {
		t.Fatalf("immutable legacy disconnect provenance was relabeled: %#v", legacyLast)
	}
}

func BenchmarkX5DataGenerator10M(b *testing.B) {
	for b.Loop() {
		source := &syntheticTradeSource{start: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC), rows: X5Rows}
		for range X5Rows {
			row, err := source.Next(b.Context())
			if err != nil || row.Kind == "" {
				b.Fatalf("synthetic row: %v", err)
			}
		}
		if _, err := source.Next(b.Context()); !errors.Is(err, io.EOF) {
			b.Fatalf("synthetic source terminal error = %v", err)
		}
	}
}

type fakeManifestReader struct {
	generation Generation
	rows       []Row
}

func (f fakeManifestReader) Plan(context.Context, CommittedManifest, string, PartitionLayout) (Generation, error) {
	generation := f.generation
	generation.ExpectedEventIDs = slices.Clone(f.generation.ExpectedEventIDs)
	return generation, nil
}

func (f fakeManifestReader) Scan(_ context.Context, _ CommittedManifest, generationID GenerationID, consume func(Row) error) error {
	for _, source := range f.rows {
		row := source
		row.GenerationID = generationID
		if err := consume(row); err != nil {
			return err
		}
	}
	return nil
}

type fakeStore struct {
	mu                     sync.Mutex
	identity               StoreIdentity
	generation             Generation
	found                  bool
	expected               []EventID
	rows                   []Row
	insertOrdinal          int
	disconnectPoint        int
	disconnectArmed        bool
	corruptRebuildEvidence bool
	unexpectedGeneration   bool
	verificationStarted    chan struct{}
	releaseVerification    chan struct{}
	verifying              bool
	interleaved            bool
}

func (f *fakeStore) StoreIdentity() StoreIdentity {
	if f.identity == (StoreIdentity{}) {
		return StoreIdentity{
			ServerDigest: "sha256:test-clickhouse-server",
			Database:     "warehouse_test", TablePrefix: "marketdata", Layout: PartitionMonth,
		}
	}
	return f.identity
}

func (f *fakeStore) EnsureSchema(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	return nil
}

func (f *fakeStore) BeginGeneration(_ context.Context, generation Generation) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	f.generation = generation.record()
	f.found = true
	f.expected = slices.Clone(generation.ExpectedEventIDs)
	return nil
}

func (f *fakeStore) Generation(_ context.Context, id GenerationID) (Generation, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	if !f.found || f.generation.ID != id {
		return Generation{}, false, nil
	}
	return f.generation, true, nil
}

func (f *fakeStore) ExpectedEventIDs(_ context.Context, id GenerationID) ([]EventID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	if !f.found || f.generation.ID != id {
		return nil, nil
	}
	return slices.Clone(f.expected), nil
}

func (f *fakeStore) ActualEventIDs(_ context.Context, id GenerationID) ([]EventID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	var ids []EventID
	for _, row := range f.rows {
		if row.GenerationID == id {
			ids = append(ids, row.EventID)
		}
	}
	slices.SortFunc(ids, compareHash)
	return slices.Compact(ids), nil
}

func (f *fakeStore) GenerationRowCount(_ context.Context, id GenerationID) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	var count uint64
	for _, row := range f.rows {
		if row.GenerationID == id {
			count++
		}
	}
	return count, nil
}

func (f *fakeStore) InsertRows(_ context.Context, rows []Row) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	f.rows = append(f.rows, slices.Clone(rows)...)
	ordinal := f.insertOrdinal
	f.insertOrdinal++
	if f.disconnectArmed && ordinal == f.disconnectPoint {
		f.disconnectArmed = false
		return ErrWriteOutcomeUnknown
	}
	return nil
}

func (f *fakeStore) SetGenerationState(_ context.Context, id GenerationID, state GenerationState, lastError string, updatedAt time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	if f.found && f.generation.ID == id {
		f.generation.State = state
		f.generation.LastError = lastError
		f.generation.UpdatedAt = updatedAt
	}
	return nil
}

func (f *fakeStore) DeleteGeneration(_ context.Context, id GenerationID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	if f.found && f.generation.ID == id {
		f.generation = Generation{}
		f.found = false
		f.expected = nil
		f.insertOrdinal = 0
	}
	f.rows = slices.DeleteFunc(f.rows, func(row Row) bool { return row.GenerationID == id })
	return nil
}

func (f *fakeStore) DropPartition(context.Context, Partition) error {
	return f.Truncate(context.Background())
}

func (f *fakeStore) Truncate(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	f.generation = Generation{}
	f.found = false
	f.expected = nil
	f.rows = nil
	f.insertOrdinal = 0
	return nil
}

func (f *fakeStore) PersistedGenerationIDs(context.Context) ([]GenerationID, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.observeOperation()
	seen := make(map[GenerationID]struct{})
	if f.found {
		seen[f.generation.ID] = struct{}{}
	}
	for _, row := range f.rows {
		seen[row.GenerationID] = struct{}{}
	}
	if f.unexpectedGeneration {
		seen[testWarehouseHash(240)] = struct{}{}
	}
	result := make([]GenerationID, 0, len(seen))
	for id := range seen {
		result = append(result, id)
	}
	return result, nil
}

func (f *fakeStore) StreamRebuildRows(_ context.Context, visit func(Row) error) error {
	f.mu.Lock()
	f.observeOperation()
	f.verifying = true
	rows := slices.Clone(f.rows)
	if f.corruptRebuildEvidence && len(rows) != 0 && rows[0].Price != nil {
		price := *rows[0].Price
		price.Coefficient = "999000000000000000000"
		rows[0].Price = &price
	}
	started, release := f.verificationStarted, f.releaseVerification
	if started != nil {
		close(started)
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.verifying = false
		f.mu.Unlock()
	}()
	if release != nil {
		<-release
	}
	for _, row := range rows {
		if err := visit(row); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeStore) observeOperation() {
	if f.verifying {
		f.interleaved = true
	}
}

func fakeGeneration(count int) Generation {
	ids := make([]EventID, count)
	for i := range count {
		ids[i] = testWarehouseHash(byte(i + 1))
	}
	slices.SortFunc(ids, compareHash)
	return Generation{ID: testWarehouseHash(201), ServerDigest: "sha256:test-clickhouse-server", ManifestHash: testWarehouseHash(202),
		InputHash: testWarehouseHash(203), DatasetIdentity: testWarehouseHash(204), CatalogIdentity: testWarehouseHash(205),
		SchemaIdentity: testWarehouseHash(206), Family: "trade", SourceID: "synthetic", UTCDate: "2024-01-01",
		PartitionValue: 202401, Layout: PartitionMonth, ExpectedEventIDs: ids, ExpectedEventSetHash: eventSetHash(ids),
		ExpectedEventCount: uint64(count), ExpectedRowCount: uint64(count), State: GenerationPending}
}

func fakeRows(count int) []Row {
	generation := fakeGeneration(count)
	rows := make([]Row, count)
	for i, eventID := range generation.ExpectedEventIDs {
		price := Decimal{Coefficient: "123450000000000000000", Scale: 18}
		amount := Decimal{Coefficient: "2500000000000000000", Scale: 18}
		rows[i] = Row{GenerationID: generation.ID, ManifestHash: generation.ManifestHash, RowID: rowIdentity(eventID, 0),
			EventID: eventID, LogicalHash: testWarehouseHash(byte(i + 101)), Family: "trade", SourceID: "synthetic",
			ChannelID: "trades", InstrumentUID: "instrument", EpochKind: "connection", ConnectionEpoch: [16]byte{1},
			ReceivedTimeNS: 1_704_067_200_000_000_000 + int64(i), ArrivalOrdinal: uint64(i + 1),
			RawSegmentSHA256: testWarehouseHash(210), RawRecordOrdinal: uint64(i), RawPayloadSHA256: testWarehouseHash(211),
			CatalogSnapshotID: generation.CatalogIdentity, SchemaName: "enable.trade.v1", SchemaVersion: 1,
			DatasetPolicyID: testWarehouseHash(212), ReplayConfigID: testWarehouseHash(213),
			InputManifestSetID: generation.InputHash, Price: &price, Amount: &amount}
	}
	return rows
}

func differentFakeManifestReader() fakeManifestReader {
	generation := fakeGeneration(1)
	generation.ID = testWarehouseHash(220)
	generation.ManifestHash = testWarehouseHash(221)
	generation.InputHash = testWarehouseHash(222)
	generation.DatasetIdentity = testWarehouseHash(223)
	generation.CatalogIdentity = testWarehouseHash(224)
	generation.SchemaIdentity = testWarehouseHash(225)
	generation.ExpectedEventIDs = []EventID{testWarehouseHash(226)}
	generation.ExpectedEventSetHash = eventSetHash(generation.ExpectedEventIDs)
	rows := fakeRows(1)
	rows[0].GenerationID = generation.ID
	rows[0].ManifestHash = generation.ManifestHash
	rows[0].EventID = generation.ExpectedEventIDs[0]
	rows[0].RowID = rowIdentity(rows[0].EventID, 0)
	rows[0].LogicalHash = testWarehouseHash(227)
	rows[0].CatalogSnapshotID = generation.CatalogIdentity
	rows[0].InputManifestSetID = generation.InputHash
	return fakeManifestReader{generation: generation, rows: rows}
}

func testWarehouseHash(seed byte) Hash {
	var hash Hash
	for i := range hash {
		hash[i] = seed + byte(i)
	}
	return hash
}
