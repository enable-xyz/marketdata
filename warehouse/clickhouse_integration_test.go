package warehouse

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"slices"
	"sync"
	"testing"
	"time"
)

const (
	clickHouseTestDSNEnv    = "MARKETDATA_TEST_CLICKHOUSE_DSN"
	clickHouseTestDigestEnv = "MARKETDATA_TEST_CLICKHOUSE_SERVER_DIGEST"
)

func TestLoadReconcile(t *testing.T) {
	manifest := integrationManifest(t, 100, 100)
	fault := &oneShotEventFault{}
	store := integrationStore(t, 50, fault)
	loader, err := NewLoader(store, ParquetManifestReader{}, integrationServerDigest(t),
		Config{BatchRows: 50, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := (ParquetManifestReader{}).Plan(t.Context(), manifest, integrationServerDigest(t), PartitionMonth)
	if err != nil {
		t.Fatal(err)
	}
	receipt, err := loader.Load(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	if !receipt.Rebuilt || receipt.GenerationID != plan.ID {
		t.Fatalf("unexpected reconciliation receipt: %#v", receipt)
	}
	if store.ReconnectCount() != 1 {
		t.Fatalf("unknown acknowledgement reconnect count = %d, want 1", store.ReconnectCount())
	}
	actual, err := store.ActualEventIDs(t.Context(), plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(actual, plan.ExpectedEventIDs) {
		t.Fatalf("event-ID set mismatch after unknown acknowledgement: got %d want %d", len(actual), len(plan.ExpectedEventIDs))
	}
	rows, err := store.GenerationRowCount(t.Context(), plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if rows != plan.ExpectedRowCount {
		t.Fatalf("row count = %d, want %d", rows, plan.ExpectedRowCount)
	}
}

func TestLoadReconcileReconnectsEveryBatchKind(t *testing.T) {
	for _, kind := range []BatchKind{BatchGeneration, BatchExpectedIDs, BatchEvents} {
		t.Run(string(kind), func(t *testing.T) {
			manifest := integrationManifest(t, 100, 100)
			fault := &disconnectFault{}
			fault.arm(DisconnectSelection{BatchKind: kind})
			store := integrationStore(t, 25, fault)
			digest := integrationServerDigest(t)
			loader, err := NewLoader(store, ParquetManifestReader{}, digest,
				Config{BatchRows: 25, Compression: CompressionLZ4, Layout: PartitionMonth})
			if err != nil {
				t.Fatal(err)
			}
			plan, err := (ParquetManifestReader{}).Plan(t.Context(), manifest, digest, PartitionMonth)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := loader.Load(t.Context(), manifest); err != nil {
				t.Fatal(err)
			}
			if store.ReconnectCount() != 1 {
				t.Fatalf("%s fault reconnect count = %d, want 1", kind, store.ReconnectCount())
			}
			actual, err := store.ActualEventIDs(t.Context(), plan.ID)
			if err != nil {
				t.Fatal(err)
			}
			rows, err := store.GenerationRowCount(t.Context(), plan.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(actual, plan.ExpectedEventIDs) || rows != plan.ExpectedRowCount {
				t.Fatalf("%s fault mismatch: event IDs=%d/%d rows=%d/%d",
					kind, len(actual), len(plan.ExpectedEventIDs), rows, plan.ExpectedRowCount)
			}
		})
	}
}

func TestLoadReconcileConcurrentSameManifest(t *testing.T) {
	manifest := integrationManifest(t, 100, 100)
	dsn, dsnBound := os.LookupEnv(clickHouseTestDSNEnv)
	digest, digestBound := os.LookupEnv(clickHouseTestDigestEnv)
	if !dsnBound || !digestBound {
		t.Skip("explicit ClickHouse integration bindings are absent")
	}
	nameDigest := sha256.Sum256([]byte(t.Name()))
	config := NativeConfig{DSN: dsn, ServerDigest: digest,
		TablePrefix: "warehouse_test_" + hex.EncodeToString(nameDigest[:6]), BatchRows: 25,
		Compression: CompressionLZ4, Layout: PartitionMonth}
	firstStore, err := OpenNative(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	secondStore, err := OpenNative(t.Context(), config)
	if err != nil {
		_ = firstStore.Close()
		t.Fatal(err)
	}
	for _, store := range []*NativeStore{firstStore, secondStore} {
		if err := store.EnsureSchema(t.Context()); err != nil {
			_ = firstStore.Close()
			_ = secondStore.Close()
			t.Fatal(err)
		}
	}
	if err := firstStore.Truncate(t.Context()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = firstStore.Truncate(context.Background())
		_ = firstStore.Close()
		_ = secondStore.Close()
	})
	loaders := make([]*Loader, 2)
	for i, store := range []*NativeStore{firstStore, secondStore} {
		loaders[i], err = NewLoader(store, ParquetManifestReader{}, digest,
			Config{BatchRows: 25, Compression: CompressionLZ4, Layout: PartitionMonth})
		if err != nil {
			t.Fatal(err)
		}
	}
	start := make(chan struct{})
	var wait sync.WaitGroup
	var receipts [2]LoadReceipt
	var loadErrors [2]error
	for i := range loaders {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			receipts[index], loadErrors[index] = loaders[index].Load(t.Context(), manifest)
		}(i)
	}
	close(start)
	wait.Wait()
	for i, err := range loadErrors {
		if err != nil {
			t.Fatalf("concurrent loader %d: %v", i, err)
		}
	}
	if receipts[0].GenerationID != receipts[1].GenerationID ||
		receipts[0].ManifestHash != receipts[1].ManifestHash ||
		receipts[0].InputHash != receipts[1].InputHash ||
		receipts[0].ExpectedEventCount != receipts[1].ExpectedEventCount ||
		receipts[0].ExpectedRowCount != receipts[1].ExpectedRowCount {
		t.Fatalf("concurrent receipts did not converge: %#v %#v", receipts[0], receipts[1])
	}
	plan, err := (ParquetManifestReader{}).Plan(t.Context(), manifest, digest, PartitionMonth)
	if err != nil {
		t.Fatal(err)
	}
	firstStore.operationMu.Lock()
	var generationRows uint64
	err = firstStore.conn.QueryRow(t.Context(), "SELECT count() FROM "+firstStore.schema.GenerationsTable()+
		" WHERE load_generation_id = ?", string(plan.ID[:])).Scan(&generationRows)
	firstStore.operationMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if generationRows != 1 {
		t.Fatalf("logical generation rows = %d, want 1", generationRows)
	}
	actual, err := firstStore.ActualEventIDs(t.Context(), plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := firstStore.GenerationRowCount(t.Context(), plan.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(actual, plan.ExpectedEventIDs) || rows != plan.ExpectedRowCount {
		t.Fatalf("concurrent load mismatch: event IDs=%d/%d rows=%d/%d",
			len(actual), len(plan.ExpectedEventIDs), rows, plan.ExpectedRowCount)
	}
}

func TestRebuild(t *testing.T) {
	manifests := integrationManifests(t, 100, 50)
	store := integrationStore(t, 50, nil)
	loader, err := NewLoader(store, ParquetManifestReader{}, integrationServerDigest(t),
		Config{BatchRows: 50, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	receipts, err := loader.RebuildAll(t.Context(), manifests)
	if err != nil {
		t.Fatal(err)
	}
	if len(receipts) != len(manifests) {
		t.Fatalf("receipt count = %d, want %d", len(receipts), len(manifests))
	}
	expected := make([]EventID, 0, 100)
	var expectedRows uint64
	for i, manifest := range manifests {
		plan, err := (ParquetManifestReader{}).Plan(t.Context(), manifest, integrationServerDigest(t), PartitionMonth)
		if err != nil {
			t.Fatal(err)
		}
		if receipts[i].ManifestHash != plan.ManifestHash || receipts[i].InputHash != plan.InputHash {
			t.Fatalf("rebuild receipt %d lost manifest/input identity", i)
		}
		expected = append(expected, plan.ExpectedEventIDs...)
		expectedRows += plan.ExpectedRowCount
	}
	slices.SortFunc(expected, compareHash)
	expected = slices.Compact(expected)
	assertAllRows(t, store, expected, expectedRows)
	if err := store.Truncate(t.Context()); err != nil {
		t.Fatal(err)
	}
	actual, rows, err := store.allEventIDsAndRows(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if len(actual) != 0 || rows != 0 {
		t.Fatalf("warehouse did not empty: event IDs=%d rows=%d", len(actual), rows)
	}
	partition := Partition{Layout: PartitionMonth, Value: 202401}
	if _, err := loader.RebuildPartition(t.Context(), partition, manifests); err != nil {
		t.Fatal(err)
	}
	assertAllRows(t, store, expected, expectedRows)
}

func TestDecimalRoundTrip(t *testing.T) {
	manifest := integrationManifest(t, 1, 1)
	store := integrationStore(t, 1, nil)
	loader, err := NewLoader(store, ParquetManifestReader{}, integrationServerDigest(t),
		Config{BatchRows: 1, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := (ParquetManifestReader{}).Plan(t.Context(), manifest, integrationServerDigest(t), PartitionMonth)
	if err != nil {
		t.Fatal(err)
	}
	var source Row
	if err := (ParquetManifestReader{}).Scan(t.Context(), manifest, plan.ID, func(row Row) error {
		source = row
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := loader.Load(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	fields, err := store.DecimalFields(t.Context(), plan.ID, source.EventID)
	if err != nil {
		t.Fatal(err)
	}
	if source.Price == nil || source.Amount == nil || fields["price"] != *source.Price || fields["amount"] != *source.Amount {
		t.Fatalf("decimal round trip mismatch: source price=%#v amount=%#v fields=%#v", source.Price, source.Amount, fields)
	}
}

func TestFullRebuildClickHouse(t *testing.T) {
	manifest := integrationManifest(t, 100, 100)
	store := integrationStore(t, 50, nil)
	loader, err := NewLoader(store, ParquetManifestReader{}, integrationServerDigest(t),
		Config{BatchRows: 50, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	report, err := loader.FullRebuild(t.Context(), []CommittedManifest{manifest})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(report.ExpectedGenerationIDs, report.PersistedGenerationIDs) ||
		!slices.Equal(report.Source.EventIDs, report.Target.EventIDs) ||
		report.Source.EventSetSHA256 != report.Target.EventSetSHA256 ||
		report.Source.LogicalSHA256 != report.Target.LogicalSHA256 ||
		report.Source.AggregateSHA256 != report.Target.AggregateSHA256 ||
		report.Source.RowCount != report.Target.RowCount {
		t.Fatalf("full rebuild evidence mismatch: %#v", report)
	}
}

func integrationManifest(t *testing.T, totalRows, rowsPerManifest uint64) CommittedManifest {
	t.Helper()
	manifests := integrationManifests(t, totalRows, rowsPerManifest)
	if len(manifests) != 1 {
		t.Fatalf("fixture produced %d manifests, want 1", len(manifests))
	}
	return manifests[0]
}

func integrationManifests(t *testing.T, totalRows, rowsPerManifest uint64) []CommittedManifest {
	t.Helper()
	if _, ok := os.LookupEnv(clickHouseTestDSNEnv); !ok {
		t.Skip(clickHouseTestDSNEnv + " is not bound")
	}
	if _, ok := os.LookupEnv(clickHouseTestDigestEnv); !ok {
		t.Skip(clickHouseTestDigestEnv + " is not bound")
	}
	manifests, err := GenerateSyntheticDataset(t.Context(), t.TempDir(), SyntheticDatasetConfig{TotalRows: totalRows,
		RowsPerManifest: rowsPerManifest, StartTime: time.Date(2024, time.January, 1, 0, 0, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	return manifests
}

func integrationStore(t *testing.T, batchRows int, fault AcknowledgementFault) *NativeStore {
	t.Helper()
	dsn, dsnBound := os.LookupEnv(clickHouseTestDSNEnv)
	digest, digestBound := os.LookupEnv(clickHouseTestDigestEnv)
	if !dsnBound || !digestBound {
		t.Skip("explicit ClickHouse integration bindings are absent")
	}
	nameDigest := sha256.Sum256([]byte(t.Name()))
	store, err := OpenNative(t.Context(), NativeConfig{DSN: dsn, ServerDigest: digest,
		TablePrefix: "warehouse_test_" + hex.EncodeToString(nameDigest[:6]), BatchRows: batchRows,
		Compression: CompressionLZ4, Layout: PartitionMonth, AcknowledgementFault: fault})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.EnsureSchema(t.Context()); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	if err := store.Truncate(t.Context()); err != nil {
		_ = store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = store.Truncate(context.Background())
		_ = store.Close()
	})
	return store
}

func integrationServerDigest(t *testing.T) string {
	t.Helper()
	digest, ok := os.LookupEnv(clickHouseTestDigestEnv)
	if !ok {
		t.Skip(clickHouseTestDigestEnv + " is not bound")
	}
	return digest
}

func assertAllRows(t *testing.T, store *NativeStore, expected []EventID, expectedRows uint64) {
	t.Helper()
	actual, rows, err := store.allEventIDsAndRows(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(actual, expected) || rows != expectedRows {
		t.Fatalf("rebuilt warehouse mismatch: event IDs=%d/%d rows=%d/%d", len(actual), len(expected), rows, expectedRows)
	}
}

type oneShotEventFault struct {
	dropped bool
}

func (f *oneShotEventFault) AfterSynchronousBatch(_ context.Context, observation BatchObservation) error {
	if observation.Kind == BatchEvents && !f.dropped {
		f.dropped = true
		return errSyntheticDisconnect
	}
	return nil
}

var errSyntheticDisconnect = errors.New("synthetic ClickHouse acknowledgement disconnect")
