package pipeline

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/dataset"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/enable-xyz/marketdata/verify"
	"github.com/enable-xyz/marketdata/warehouse"
)

func TestExportRunOnceDeterministicAndIdempotent(t *testing.T) {
	fixture := newPipelineFixture(t)
	first, err := fixture.exporter.RunOnce(t.Context(), fixture.request)
	if err != nil {
		t.Fatalf("first export: %v", err)
	}
	retryRequest := fixture.request
	retryRequest.BuildRoot = t.TempDir()
	second, err := fixture.exporter.RunOnce(t.Context(), retryRequest)
	if err != nil {
		t.Fatalf("second export: %v", err)
	}
	if !first.Complete || !second.Complete || len(first.Datasets) != 1 || len(second.Datasets) != 1 {
		t.Fatalf("export receipts incomplete: first=%+v second=%+v", first, second)
	}
	if first.InputManifestSetID != second.InputManifestSetID || first.Replay.LogicalHash != second.Replay.LogicalHash ||
		first.Datasets[0].DatasetID != second.Datasets[0].DatasetID || first.Datasets[0].ManifestHash != second.Datasets[0].ManifestHash ||
		first.Datasets[0].PhysicalHash != second.Datasets[0].PhysicalHash {
		t.Fatalf("identical input/config changed deterministic identity: first=%+v second=%+v", first, second)
	}
	if first.Datasets[0].Family != string(dataset.FamilyTrade) || first.Datasets[0].ReusedCommitted || !second.Datasets[0].ReusedCommitted {
		t.Fatalf("unexpected dataset reconciliation: first=%+v second=%+v", first.Datasets[0], second.Datasets[0])
	}
	if fixture.catalog.verifiedWrites != 1 || fixture.catalog.datasetCommits != 1 || len(fixture.catalog.datasets) != 1 {
		t.Fatalf("catalog duplicated deterministic dataset: verified=%d commits=%d datasets=%d", fixture.catalog.verifiedWrites, fixture.catalog.datasetCommits, len(fixture.catalog.datasets))
	}
	page, err := fixture.objects.List(t.Context(), fixture.request.ObjectPrefix, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Objects) != 2 || page.NextToken != "" {
		t.Fatalf("dataset object count = %d, want exact parquet+manifest", len(page.Objects))
	}
}

func TestExportRejectsNonCommittedRawPublication(t *testing.T) {
	fixture := newPipelineFixture(t)
	fixture.raw.publications[0].State = catalog.RawSegmentVerified

	receipt, err := fixture.exporter.RunOnce(t.Context(), fixture.request)
	if !errors.Is(err, ErrRawSelection) || receipt.Complete {
		t.Fatalf("non-committed raw publication error=%v receipt=%+v", err, receipt)
	}
	if len(fixture.catalog.datasets) != 0 || fixture.catalog.verifiedWrites != 0 || fixture.catalog.datasetCommits != 0 {
		t.Fatalf("non-committed raw publication became catalog-visible: %+v", fixture.catalog)
	}
}

func TestExportReportsEmptyReceiveTimeRangeWithoutPublishing(t *testing.T) {
	fixture := newPipelineFixture(t)
	fixture.request.StartReceivedTimeNS = fixture.raw.publications[0].ReceivedEndNS + 1
	fixture.request.EndReceivedTimeNS = fixture.request.StartReceivedTimeNS + 1

	receipt, err := fixture.exporter.RunOnce(t.Context(), fixture.request)
	if !errors.Is(err, ErrEmptyExport) || receipt.Complete {
		t.Fatalf("empty range error=%v receipt=%+v", err, receipt)
	}
	if len(fixture.catalog.datasets) != 0 || fixture.catalog.verifiedWrites != 0 || fixture.catalog.datasetCommits != 0 {
		t.Fatalf("empty range became catalog-visible: %+v", fixture.catalog)
	}
}

func TestExportDoesNotCommitIncompleteDataset(t *testing.T) {
	fixture := newPipelineFixture(t)
	failing := &failManifestCreateClient{Client: fixture.objects}
	exporter, err := NewExporter(fixture.raw, fixture.catalog, failing, fixture.normalizer, fixture.config)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := exporter.RunOnce(t.Context(), fixture.request)
	if err == nil || partial.Complete {
		t.Fatalf("incomplete export error=%v receipt=%+v", err, partial)
	}
	if len(fixture.catalog.datasets) != 0 || fixture.catalog.verifiedWrites != 0 || fixture.catalog.datasetCommits != 0 {
		t.Fatalf("incomplete objects became catalog-visible: %+v", fixture.catalog)
	}
	complete, err := fixture.exporter.RunOnce(t.Context(), fixture.request)
	if err != nil {
		t.Fatalf("reconcile incomplete export: %v", err)
	}
	if !complete.Complete || len(complete.Datasets) != 1 || !complete.Datasets[0].ParquetObject.Recovered || complete.Datasets[0].ManifestObject.Recovered {
		t.Fatalf("unexpected incomplete-output reconciliation: %+v", complete)
	}
}

func TestExportReconcilesAmbiguousImmutableCreate(t *testing.T) {
	fixture := newPipelineFixture(t)
	ambiguous := &ambiguousParquetCreateClient{Client: fixture.objects}
	exporter, err := NewExporter(fixture.raw, fixture.catalog, ambiguous, fixture.normalizer, fixture.config)
	if err != nil {
		t.Fatal(err)
	}

	receipt, err := exporter.RunOnce(t.Context(), fixture.request)
	if err != nil {
		t.Fatalf("reconcile ambiguous create: %v", err)
	}
	if !receipt.Complete || len(receipt.Datasets) != 1 || !receipt.Datasets[0].ParquetObject.Recovered {
		t.Fatalf("ambiguous exact object was not reconciled: %+v", receipt)
	}
	if fixture.catalog.verifiedWrites != 1 || fixture.catalog.datasetCommits != 1 {
		t.Fatalf("reconciled exact object did not commit once: %+v", fixture.catalog)
	}
}

func TestLoadRunOnceUsesWarehouseGenerationIdempotency(t *testing.T) {
	fixture := newPipelineFixture(t)
	exported, err := fixture.exporter.RunOnce(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	store := newFakeWarehouseStore()
	warehouseLoader, err := warehouse.NewLoader(store, warehouse.ParquetManifestReader{}, store.identity.ServerDigest, warehouse.DefaultConfig())
	if err != nil {
		t.Fatal(err)
	}
	loader, err := NewLoader(fixture.catalog, fixture.objects, warehouseLoader, LoaderConfig{MaxParquetBytes: 1 << 30, MaxCoverage: 64})
	if err != nil {
		t.Fatal(err)
	}
	request := LoadRequest{DatasetID: exported.Datasets[0].DatasetID, WorkRoot: t.TempDir()}
	first, err := loader.RunOnce(t.Context(), request)
	if err != nil {
		t.Fatalf("first load: %v", err)
	}
	rowsAfterFirst := store.rowCount()
	second, err := loader.RunOnce(t.Context(), request)
	if err != nil {
		t.Fatalf("second load: %v", err)
	}
	if !first.Complete || !second.Complete || rowsAfterFirst == 0 || store.rowCount() != rowsAfterFirst || store.insertCalls != 1 {
		t.Fatalf("warehouse rerun duplicated rows: first=%+v second=%+v rows=%d/%d inserts=%d", first, second, rowsAfterFirst, store.rowCount(), store.insertCalls)
	}
	if first.Warehouse.GenerationID != second.Warehouse.GenerationID || fixture.catalog.generationWrites != 1 || len(fixture.catalog.generations) != 1 {
		t.Fatalf("generation identity was not idempotent: first=%x second=%x catalog=%+v", first.Warehouse.GenerationID, second.Warehouse.GenerationID, fixture.catalog.generations)
	}
	if !second.Materialization.ManifestObject.Recovered || !second.Materialization.ParquetObject.Recovered {
		t.Fatalf("second load did not reconcile exact local materialization: %+v", second.Materialization)
	}
}

type pipelineFixture struct {
	raw        *fakeRawCatalog
	catalog    *fakeDatasetCatalog
	objects    *verify.FileObjectClient
	normalizer *normalize.Orchestrator
	config     ExporterConfig
	exporter   *Exporter
	request    ExportRequest
}

func newPipelineFixture(t *testing.T) pipelineFixture {
	t.Helper()
	objectRoot := t.TempDir()
	objects, err := verify.OpenFileObjectClient(objectRoot)
	if err != nil {
		t.Fatal(err)
	}
	publication := committedSpotSegment(t, objects)
	normalizer, snapshotID := spotFixtureNormalizer(t)
	config := ExporterConfig{
		Replay: replay.DefaultConfig(), Verify: objectstore.DefaultVerifyPolicy(),
		DatasetPolicyID: testNormalizeHash("pipeline-dataset-policy"), ReplayConfigID: testNormalizeHash("pipeline-replay-config"),
		CatalogSnapshotID: snapshotID, MapperSetID: testNormalizeHash("pipeline-mapper-set"),
		MaxSegments: 8, MaxRecords: 1024, MaxOutputRows: 1024, NormalizeBatch: 64, MaxPartitions: 16,
	}
	raw := &fakeRawCatalog{publications: []catalog.RawSegmentPublication{publication}}
	datasetCatalog := newFakeDatasetCatalog()
	exporter, err := NewExporter(raw, datasetCatalog, objects, normalizer, config)
	if err != nil {
		t.Fatal(err)
	}
	return pipelineFixture{raw: raw, catalog: datasetCatalog, objects: objects, normalizer: normalizer, config: config, exporter: exporter,
		request: ExportRequest{SourceID: binance.SpotSourceID, SegmentIDs: []string{publication.SegmentID},
			StartReceivedTimeNS: publication.ReceivedStartNS, EndReceivedTimeNS: publication.ReceivedEndNS + 1,
			BuildRoot: t.TempDir(), ObjectPrefix: "fixture-datasets"}}
}

func committedSpotSegment(t *testing.T, objects objectstore.Client) catalog.RawSegmentPublication {
	t.Helper()
	payload, err := os.ReadFile(filepath.Join("..", "testdata", "binance", "spot", "synthetic", "trade.json"))
	if err != nil {
		t.Fatal(err)
	}
	payload = bytes.Replace(payload, []byte(`"s":"BNBBTC"`), []byte(`"s":"BTCUSDT"`), 1)
	epoch := [16]byte{0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x47, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x10}
	envelope := capture.EnvelopeV1{
		EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: binance.SpotSourceID, ChannelOrEndpoint: binance.SpotRawChannel,
		NativeSymbol: capture.OptionalString{Value: "BTCUSDT", Valid: true}, ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true},
		ArrivalOrdinal: 1, ExchangeTimeNS: capture.OptionalInt64{Value: 1_672_515_782_136_000_000, Valid: true},
		ExchangeTimeResolution: capture.ExchangeTimeMillisecond, ReceivedWallTimeNS: 1_700_000_000_100_000_000,
		ClockEpochID: "pipeline-fixture-clock", MonotonicNSSinceClockEpoch: 1,
		PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved, RecorderVersion: "pipeline-fixture-v1",
	}
	envelope.SetRawPayload(payload)
	record, err := envelope.ToSegment()
	if err != nil {
		t.Fatal(err)
	}
	spoolRoot, err := filepath.Abs(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	spool, err := segment.OpenSpool(segment.SpoolConfig{Root: spoolRoot, SourceID: binance.SpotSourceID, ChannelID: binance.SpotRawChannel, EpochKind: segment.EpochConnection, EpochID: epoch})
	if err != nil {
		t.Fatal(err)
	}
	writer, err := spool.NewWriter(segment.WriterOptions{WriterVersion: "pipeline-fixture-v1"})
	if err != nil {
		t.Fatal(err)
	}
	if ready, err := writer.Write(record); err != nil || ready != nil {
		t.Fatalf("write fixture segment: ready=%+v error=%v", ready, err)
	}
	ready, err := writer.EndEpoch()
	if err != nil || ready == nil {
		t.Fatalf("seal fixture segment: ready=%+v error=%v", ready, err)
	}
	file, err := os.Open(ready.SegmentPath)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	manifest := ready.Manifest
	if err := objects.PutIfAbsent(t.Context(), objectstore.PutObject{Key: manifest.ObjectKey, Body: file,
		Size: int64(manifest.Segment.CompressedBytes), SHA256: manifest.Segment.CompressedSHA256}); err != nil {
		t.Fatal(err)
	}
	manifestBytes, err := os.ReadFile(ready.ManifestPath)
	if err != nil {
		t.Fatal(err)
	}
	return catalog.RawSegmentPublication{
		SegmentID: "4ac4f35f-37ac-5a9f-b2c3-2ee2aee5a202", SourceID: manifest.SourceID, ChannelID: manifest.ChannelID, EpochID: testUUID(manifest.EpochID),
		ReceivedStartNS: manifest.Segment.FirstReceivedAtNS, ReceivedEndNS: manifest.Segment.LastReceivedAtNS,
		OrdinalStart: manifest.Segment.FirstOrdinal, OrdinalEnd: manifest.Segment.LastOrdinal, ObjectKey: manifest.ObjectKey,
		ContentSHA256: manifest.Segment.CompressedSHA256, ByteLength: int64(manifest.Segment.CompressedBytes),
		ManifestVersion: manifest.ManifestVersion, ManifestSHA256: ready.ManifestSHA256, ManifestBytes: manifestBytes, State: catalog.RawSegmentCommitted,
	}
}

func spotFixtureNormalizer(t *testing.T) (*normalize.Orchestrator, normalize.Hash) {
	t.Helper()
	bundle, err := binance.LoadFixtureBundle(filepath.Join("..", "testdata", "binance", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	page, err := bundle.CapturedPage("active")
	if err != nil {
		t.Fatal(err)
	}
	composed, err := binance.ComposeExchangeInfo([]binance.CapturedPage{page}, binance.ComposeOptions{}, binance.DefaultParserLimits())
	if err != nil {
		t.Fatal(err)
	}
	source, version, channels := binance.SpotCatalogContract()
	snapshot, err := catalog.BuildFreshSnapshot(source, version, channels, composed.Candidates)
	if err != nil {
		t.Fatal(err)
	}
	snapshotID := normalize.Hash(snapshot.SHA256)
	binding, err := binance.NewSpotMapperBinding(snapshotID, binance.SpotMapperVersion, 0, normalize.OptionalInt64{}, normalize.ResolutionMillisecond, nil)
	if err != nil {
		t.Fatal(err)
	}
	orchestrator, err := normalize.NewOrchestrator(snapshot, []normalize.BoundMapper{binding})
	if err != nil {
		t.Fatal(err)
	}
	return orchestrator, snapshotID
}

func testUUID(raw string) string {
	return raw[:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:]
}

func testNormalizeHash(seed string) normalize.Hash {
	return normalize.Hash(sha256.Sum256([]byte(seed)))
}

type fakeRawCatalog struct {
	publications []catalog.RawSegmentPublication
}

func (f *fakeRawCatalog) StreamCommittedRawSegments(ctx context.Context, visit func(catalog.RawSegmentPublication) error) error {
	for _, publication := range f.publications {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := visit(publication); err != nil {
			return err
		}
	}
	return nil
}

type fakeDatasetCatalog struct {
	mu               sync.Mutex
	datasets         map[string]catalog.DatasetPublication
	generations      map[string]catalog.DatasetGenerationCommit
	verifiedWrites   int
	datasetCommits   int
	generationWrites int
}

func newFakeDatasetCatalog() *fakeDatasetCatalog {
	return &fakeDatasetCatalog{datasets: make(map[string]catalog.DatasetPublication), generations: make(map[string]catalog.DatasetGenerationCommit)}
}

func (f *fakeDatasetCatalog) FindDataset(_ context.Context, id string) (catalog.DatasetPublication, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	publication, found := f.datasets[id]
	return cloneDatasetPublication(publication), found, nil
}

func (f *fakeDatasetCatalog) RecordVerifiedDataset(_ context.Context, publication catalog.DatasetPublication) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, found := f.datasets[publication.DatasetID]; found {
		if !sameDatasetPublication(existing, publication) {
			return catalog.ErrQueryConflict
		}
		return nil
	}
	f.datasets[publication.DatasetID] = cloneDatasetPublication(publication)
	f.verifiedWrites++
	return nil
}

func (f *fakeDatasetCatalog) CommitDataset(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	publication, found := f.datasets[id]
	if !found {
		return catalog.ErrQueryNotFound
	}
	if publication.State == catalog.DatasetCommitted {
		return nil
	}
	if publication.State != catalog.DatasetVerified {
		return catalog.ErrQueryConflict
	}
	publication.State = catalog.DatasetCommitted
	f.datasets[id] = publication
	f.datasetCommits++
	return nil
}

func (f *fakeDatasetCatalog) CommitDatasetGeneration(_ context.Context, commit catalog.DatasetGenerationCommit) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	publication, found := f.datasets[commit.DatasetID]
	if !found || publication.State != catalog.DatasetCommitted {
		return catalog.ErrQueryConflict
	}
	if existing, found := f.generations[commit.DatasetID]; found {
		if existing != commit {
			return catalog.ErrQueryConflict
		}
		return nil
	}
	f.generations[commit.DatasetID] = commit
	f.generationWrites++
	return nil
}

func cloneDatasetPublication(value catalog.DatasetPublication) catalog.DatasetPublication {
	value.ManifestBytes = slices.Clone(value.ManifestBytes)
	value.InputSegmentIDs = slices.Clone(value.InputSegmentIDs)
	value.Coverage = slices.Clone(value.Coverage)
	return value
}

type failManifestCreateClient struct {
	objectstore.Client
	failed bool
}

func (c *failManifestCreateClient) PutIfAbsent(ctx context.Context, object objectstore.PutObject) error {
	if !c.failed && strings.Contains(filepath.Base(object.Key), "manifest-") {
		c.failed = true
		return errors.New("synthetic manifest create failure")
	}
	return c.Client.PutIfAbsent(ctx, object)
}

type ambiguousParquetCreateClient struct {
	objectstore.Client
	failed bool
}

func (c *ambiguousParquetCreateClient) PutIfAbsent(ctx context.Context, object objectstore.PutObject) error {
	err := c.Client.PutIfAbsent(ctx, object)
	if err == nil && !c.failed && strings.HasSuffix(object.Key, ".parquet") {
		c.failed = true
		return errors.New("synthetic ambiguous immutable create")
	}
	return err
}

type fakeWarehouseStore struct {
	mu          sync.Mutex
	identity    warehouse.StoreIdentity
	generations map[warehouse.GenerationID]warehouse.Generation
	expected    map[warehouse.GenerationID][]warehouse.EventID
	rows        map[warehouse.GenerationID]map[warehouse.Hash]warehouse.Row
	insertCalls int
}

func newFakeWarehouseStore() *fakeWarehouseStore {
	return &fakeWarehouseStore{
		identity:    warehouse.StoreIdentity{ServerDigest: "pipeline-fixture-server", Database: "pipeline_fixture", TablePrefix: "marketdata", Layout: warehouse.PartitionMonth},
		generations: make(map[warehouse.GenerationID]warehouse.Generation), expected: make(map[warehouse.GenerationID][]warehouse.EventID),
		rows: make(map[warehouse.GenerationID]map[warehouse.Hash]warehouse.Row),
	}
}

func (s *fakeWarehouseStore) StoreIdentity() warehouse.StoreIdentity { return s.identity }
func (s *fakeWarehouseStore) EnsureSchema(context.Context) error     { return nil }

func (s *fakeWarehouseStore) BeginGeneration(_ context.Context, generation warehouse.Generation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, found := s.generations[generation.ID]; found {
		return warehouse.ErrGenerationConflict
	}
	s.expected[generation.ID] = slices.Clone(generation.ExpectedEventIDs)
	generation.ExpectedEventIDs = nil
	s.generations[generation.ID] = generation
	s.rows[generation.ID] = make(map[warehouse.Hash]warehouse.Row)
	return nil
}

func (s *fakeWarehouseStore) Generation(_ context.Context, id warehouse.GenerationID) (warehouse.Generation, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	generation, found := s.generations[id]
	return generation, found, nil
}

func (s *fakeWarehouseStore) ExpectedEventIDs(_ context.Context, id warehouse.GenerationID) ([]warehouse.EventID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.expected[id]), nil
}

func (s *fakeWarehouseStore) ActualEventIDs(_ context.Context, id warehouse.GenerationID) ([]warehouse.EventID, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := make(map[warehouse.EventID]struct{})
	for _, row := range s.rows[id] {
		seen[row.EventID] = struct{}{}
	}
	result := make([]warehouse.EventID, 0, len(seen))
	for eventID := range seen {
		result = append(result, eventID)
	}
	slices.SortFunc(result, func(left, right warehouse.EventID) int { return strings.Compare(string(left[:]), string(right[:])) })
	return result, nil
}

func (s *fakeWarehouseStore) GenerationRowCount(_ context.Context, id warehouse.GenerationID) (uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return uint64(len(s.rows[id])), nil
}

func (s *fakeWarehouseStore) InsertRows(_ context.Context, rows []warehouse.Row) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.insertCalls++
	for _, row := range rows {
		s.rows[row.GenerationID][row.RowID] = row
	}
	return nil
}

func (s *fakeWarehouseStore) SetGenerationState(_ context.Context, id warehouse.GenerationID, state warehouse.GenerationState, lastError string, updated time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	generation, found := s.generations[id]
	if !found {
		return warehouse.ErrGenerationConflict
	}
	generation.State, generation.LastError, generation.UpdatedAt = state, lastError, updated
	s.generations[id] = generation
	return nil
}

func (s *fakeWarehouseStore) DeleteGeneration(_ context.Context, id warehouse.GenerationID) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.generations, id)
	delete(s.expected, id)
	delete(s.rows, id)
	return nil
}

func (s *fakeWarehouseStore) DropPartition(context.Context, warehouse.Partition) error { return nil }
func (s *fakeWarehouseStore) Truncate(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.generations = make(map[warehouse.GenerationID]warehouse.Generation)
	s.expected = make(map[warehouse.GenerationID][]warehouse.EventID)
	s.rows = make(map[warehouse.GenerationID]map[warehouse.Hash]warehouse.Row)
	return nil
}

func (s *fakeWarehouseStore) rowCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	var count int
	for _, rows := range s.rows {
		count += len(rows)
	}
	return count
}

var _ objectstore.Client = (*failManifestCreateClient)(nil)
var _ warehouse.Store = (*fakeWarehouseStore)(nil)
