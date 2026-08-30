package readmodel

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/warehouse"
)

type fakeCatalog struct {
	dataset catalog.DatasetIdentity
	filter  catalog.ReferenceFilter
}

func (f *fakeCatalog) Ready() bool { return true }
func (f *fakeCatalog) Sources(context.Context) ([]catalog.SourceProjection, error) {
	return []catalog.SourceProjection{{SourceID: "source", Venue: "binance", ProductFamily: "spot", APIFamily: "spot-v3", Environment: "public", Lifecycle: "active"}}, nil
}
func (f *fakeCatalog) Instruments(context.Context) ([]catalog.InstrumentProjection, error) {
	return []catalog.InstrumentProjection{{InstrumentUID: "instrument", SourceID: "source", NativeID: "BTCUSDT", ListingGeneration: 1, Lifecycle: "active", BaseAsset: "BTC", QuoteAsset: "USDT", Kind: "spot", Multiplier: "1"}}, nil
}
func (f *fakeCatalog) Coverage(context.Context) ([]catalog.CoverageProjection, error) {
	return []catalog.CoverageProjection{{ID: "coverage", Tuple: catalog.TupleProjection{SourceID: "source", ChannelID: "trade", InstrumentUID: "instrument"}, StartReceivedTimeNS: 1, EndReceivedTimeNS: 2, State: "complete", DatasetID: f.dataset.IDString()}}, nil
}
func (f *fakeCatalog) Incidents(context.Context) ([]catalog.IncidentProjection, error) {
	return []catalog.IncidentProjection{{ID: "incident", Tuple: catalog.TupleProjection{SourceID: "source", ChannelID: "trade", InstrumentUID: "instrument"}, StartReceivedTimeNS: 1, EndReceivedTimeNS: 2, Kind: "gap", GapRefID: "gap"}}, nil
}
func (f *fakeCatalog) Datasets(context.Context) ([]catalog.DatasetProjection, error) {
	return []catalog.DatasetProjection{{DatasetID: f.dataset.IDString(), Family: f.dataset.Family, SchemaName: f.dataset.SchemaName, SchemaVersion: f.dataset.SchemaVersion, Committed: true}}, nil
}
func (f *fakeCatalog) Dataset(context.Context, string) (catalog.DatasetIdentity, error) {
	return f.dataset, nil
}
func (f *fakeCatalog) LatestDataset(context.Context, string) (catalog.DatasetIdentity, error) {
	return f.dataset, nil
}
func (f *fakeCatalog) References(_ context.Context, filter catalog.ReferenceFilter) ([]catalog.CoverageReferenceProjection, []catalog.GapReferenceProjection, error) {
	f.filter = filter
	tuple := catalog.TupleProjection{SourceID: "source", ChannelID: "trade", InstrumentUID: "instrument"}
	return []catalog.CoverageReferenceProjection{{ID: "coverage", Tuple: tuple, StartReceivedTimeNS: 1, EndReceivedTimeNS: 2, State: "complete"}}, []catalog.GapReferenceProjection{{ID: "gap", Tuple: tuple, StartReceivedTimeNS: 1, EndReceivedTimeNS: 2, Kind: "disconnect"}}, nil
}

func TestStoreAdaptsCatalogWithoutChangingIdentity(t *testing.T) {
	identity := catalog.DatasetIdentity{ID: sha256.Sum256([]byte("dataset")), Family: "trade", CatalogSnapshotID: sha256.Sum256([]byte("catalog")), SchemaName: "enable.trade.parquet.v1", SchemaVersion: 1}
	catalogStore := &fakeCatalog{dataset: identity}
	store, err := New(catalogStore)
	if err != nil {
		t.Fatal(err)
	}
	if !store.Ready() {
		t.Fatal("Store.Ready() = false")
	}
	dataset, err := store.Dataset(t.Context(), identity.IDString())
	if err != nil {
		t.Fatal(err)
	}
	if dataset.IDString() != identity.IDString() || dataset.CatalogSnapshotIDString() != identity.CatalogSnapshotIDString() {
		t.Fatalf("dataset identity changed: %+v", dataset)
	}
	if sources, err := store.Sources(t.Context()); err != nil || len(sources) != 1 || sources[0].Venue != "binance" {
		t.Fatalf("Sources() = %+v, %v", sources, err)
	}
	if instruments, err := store.Instruments(t.Context()); err != nil || len(instruments) != 1 || instruments[0].NativeID != "BTCUSDT" {
		t.Fatalf("Instruments() = %+v, %v", instruments, err)
	}
	if coverage, err := store.Coverage(t.Context()); err != nil || len(coverage) != 1 || coverage[0].Tuple.ChannelID != "trade" {
		t.Fatalf("Coverage() = %+v, %v", coverage, err)
	}
	if incidents, err := store.Incidents(t.Context()); err != nil || len(incidents) != 1 || incidents[0].GapRefID != "gap" {
		t.Fatalf("Incidents() = %+v, %v", incidents, err)
	}
	if datasets, err := store.Datasets(t.Context()); err != nil || len(datasets) != 1 || !datasets[0].Committed {
		t.Fatalf("Datasets() = %+v, %v", datasets, err)
	}

	spec := warehouse.QuerySpec{Dataset: dataset, SourceIDs: []string{"source"}, ChannelIDs: []string{"trade"}, InstrumentUIDs: []string{"instrument"}, StartReceivedTimeNS: 1, EndReceivedTimeNS: 2, Limit: 10}
	coverage, gaps, err := store.References(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(coverage) != 1 || len(gaps) != 1 || coverage[0].Tuple != gaps[0].Tuple || catalogStore.filter.DatasetID != identity.IDString() {
		t.Fatalf("References() = %+v, %+v; filter %+v", coverage, gaps, catalogStore.filter)
	}
}
