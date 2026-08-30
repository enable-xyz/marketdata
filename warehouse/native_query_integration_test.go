package warehouse

import "testing"

func TestNativeQueryReaderClickHouseIntegration(t *testing.T) {
	manifest := integrationManifest(t, 3, 3)
	store := integrationStore(t, 3, nil)
	digest := integrationServerDigest(t)
	loader, err := NewLoader(store, ParquetManifestReader{}, digest,
		Config{BatchRows: 3, Compression: CompressionLZ4, Layout: PartitionMonth})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := (ParquetManifestReader{}).Plan(t.Context(), manifest, digest, PartitionMonth)
	if err != nil {
		t.Fatal(err)
	}
	var selected Row
	if err := (ParquetManifestReader{}).Scan(t.Context(), manifest, plan.ID, func(row Row) error {
		if selected == (Row{}) {
			selected = row
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := loader.Load(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	reader, err := NewNativeQueryReader(store, staticNativeReferences{})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewQueryAdapter(reader)
	if err != nil {
		t.Fatal(err)
	}
	spec := QuerySpec{
		Dataset: Dataset{ID: plan.ID, Family: selected.Family, CatalogSnapshotID: selected.CatalogSnapshotID,
			SchemaName: selected.SchemaName, SchemaVersion: selected.SchemaVersion},
		SourceIDs: []string{selected.SourceID}, ChannelIDs: []string{selected.ChannelID},
		InstrumentUIDs: []string{selected.InstrumentUID}, StartReceivedTimeNS: selected.ReceivedTimeNS,
		EndReceivedTimeNS: selected.ReceivedTimeNS + 1, Limit: 10,
	}
	page, err := adapter.Page(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Rows) == 0 {
		t.Fatal("native query did not return the loaded row")
	}
	for _, row := range page.Rows {
		if row.DatasetID != plan.ID.String() || row.SourceID != selected.SourceID || row.ChannelID != selected.ChannelID ||
			row.InstrumentUID != selected.InstrumentUID || row.ReceivedTimeNS != selected.ReceivedTimeNS {
			t.Fatalf("native query escaped its declarative bounds: %#v", row)
		}
	}
}
