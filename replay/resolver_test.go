package replay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"testing"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/enable-xyz/marketdata/warehouse"
)

type resolverPublicationLookup struct {
	publications []catalog.RawSegmentPublication
	err          error
	filters      []catalog.RawSegmentFilter
}

func (l *resolverPublicationLookup) CommittedRawSegments(ctx context.Context, filter catalog.RawSegmentFilter) ([]catalog.RawSegmentPublication, error) {
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	filter.SourceIDs = slices.Clone(filter.SourceIDs)
	filter.ChannelIDs = slices.Clone(filter.ChannelIDs)
	filter.InstrumentUIDs = slices.Clone(filter.InstrumentUIDs)
	l.filters = append(l.filters, filter)
	return slices.Clone(l.publications), l.err
}

type resolverDatasetLookup struct {
	manifest catalog.DatasetManifest
	err      error
	ids      []string
}

func (l *resolverDatasetLookup) DatasetManifest(ctx context.Context, id string) (catalog.DatasetManifest, error) {
	if err := context.Cause(ctx); err != nil {
		return catalog.DatasetManifest{}, err
	}
	l.ids = append(l.ids, id)
	return l.manifest, l.err
}

type resolverNormalizedReader struct {
	pages []warehouse.Page
	specs []warehouse.QuerySpec
	page  func(context.Context, warehouse.QuerySpec) (warehouse.Page, error)
}

func (r *resolverNormalizedReader) Page(ctx context.Context, spec warehouse.QuerySpec) (warehouse.Page, error) {
	spec.SourceIDs = slices.Clone(spec.SourceIDs)
	spec.ChannelIDs = slices.Clone(spec.ChannelIDs)
	spec.InstrumentUIDs = slices.Clone(spec.InstrumentUIDs)
	if spec.After != nil {
		after := *spec.After
		spec.After = &after
	}
	r.specs = append(r.specs, spec)
	if r.page != nil {
		return r.page(ctx, spec)
	}
	if len(r.pages) == 0 {
		return warehouse.Page{}, fmt.Errorf("unexpected normalized page read")
	}
	page := r.pages[0]
	r.pages = r.pages[1:]
	return page, nil
}

func TestNativeResolverPreservesCommittedInputAndReplayOrder(t *testing.T) {
	objects := &memoryObjects{objects: make(map[string][]byte)}
	epochLate := "aaaaaaaa-0000-4000-8000-000000000001"
	epochEarly := "bbbbbbbb-0000-4000-8000-000000000001"
	epochOther := "cccccccc-0000-4000-8000-000000000001"
	late, lateBytes, _ := testPublicationBytes(t, 801, testSourceA, epochLate, []segment.Envelope{
		testRecord(t, testSourceA, epochLate, 1, 0, 20, []byte("a-late")),
	})
	other, otherBytes, _ := testPublicationBytes(t, 802, testSourceB, epochOther, []segment.Envelope{
		testRecord(t, testSourceB, epochOther, 1, 0, 15, []byte("b")),
	})
	early, earlyBytes, _ := testPublicationBytes(t, 803, testSourceA, epochEarly, []segment.Envelope{
		testRecord(t, testSourceA, epochEarly, 1, 0, 10, []byte("a-early")),
	})
	objects.put(late.ObjectKey, lateBytes)
	objects.put(other.ObjectKey, otherBytes)
	objects.put(early.ObjectKey, earlyBytes)
	lookup := &resolverPublicationLookup{publications: []catalog.RawSegmentPublication{late, other, early}}
	resolver := newTestNativeResolver(t, lookup, objects, 10, 1<<20, 1<<20)
	if _, err := NewFramedNativeService(resolver, 1); err != nil {
		t.Fatalf("NewFramedNativeService() error = %v", err)
	}
	request := resolverRequest([]string{testSourceA, testSourceB})

	plan, err := resolver.ResolveNative(t.Context(), request)
	if err != nil {
		t.Fatalf("ResolveNative() error = %v", err)
	}
	gotInputOrder := []string{plan.Inputs[0].SegmentID(), plan.Inputs[1].SegmentID(), plan.Inputs[2].SegmentID()}
	wantInputOrder := []string{late.SegmentID, other.SegmentID, early.SegmentID}
	if !slices.Equal(gotInputOrder, wantInputOrder) {
		t.Fatalf("input order = %v, want source-authoritative lookup order %v", gotInputOrder, wantInputOrder)
	}
	if len(lookup.filters) != 1 || lookup.filters[0].DatasetID != request.DatasetID ||
		!slices.Equal(lookup.filters[0].SourceIDs, request.SourceIDs) || lookup.filters[0].StartReceivedTimeNS != request.StartReceivedTimeNS ||
		lookup.filters[0].EndReceivedTimeNS != request.EndReceivedTimeNS || lookup.filters[0].Limit != 10 ||
		lookup.filters[0].MaxManifestBytes != 1<<20 {
		t.Fatalf("raw filter = %+v, want exact request projection and manifest bound", lookup.filters)
	}

	var payloads []string
	result, err := ReplayCollectorObserved(t.Context(), plan.Reader, plan.Inputs, plan.Config, func(event Event) error {
		if event.Kind == EventRecord {
			payloads = append(payloads, string(event.Record.RawPayload))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayCollectorObserved() error = %v", err)
	}
	if result.Order != CollectorObservedOrder || !slices.Equal(payloads, []string{"a-early", "b", "a-late"}) {
		t.Fatalf("replay order = %q (%s), want first-receive epoch order and head-only merge", payloads, result.Order)
	}
}

func TestNativeServiceFiltersRecordsInsideSelectedSegments(t *testing.T) {
	epoch := "abababab-0000-4000-8000-000000000001"
	publication, object, _ := testPublicationBytes(t, 804, testSourceA, epoch, []segment.Envelope{
		testRecord(t, testSourceA, epoch, 1, 0, 10, []byte("before")),
		testRecord(t, testSourceA, epoch, 2, 0, 20, []byte("selected")),
	})
	objects := &memoryObjects{objects: make(map[string][]byte)}
	objects.put(publication.ObjectKey, object)
	resolver := newTestNativeResolver(t, &resolverPublicationLookup{publications: []catalog.RawSegmentPublication{publication}},
		objects, 10, 1<<20, 1<<20)
	service, err := NewFramedNativeService(resolver, 1)
	if err != nil {
		t.Fatal(err)
	}
	request := resolverRequest([]string{testSourceA})
	request.StartReceivedTimeNS, request.EndReceivedTimeNS = 20, 21
	stream, err := service.OpenNative(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	frame, err := stream.Next(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	record, err := segment.UnmarshalEnvelope(frame)
	if err != nil {
		t.Fatal(err)
	}
	if string(record.RawPayload) != "selected" || record.ReceivedWallTimeNS != 20 {
		t.Fatalf("selected native record = %+v", record)
	}
	if _, err := stream.Next(t.Context()); !errors.Is(err, io.EOF) {
		t.Fatalf("native stream terminal error = %v, want EOF", err)
	}
}

func TestNativeResolverRejectsEscapesStateAndBounds(t *testing.T) {
	epoch := "dddddddd-0000-4000-8000-000000000001"
	committed := testPublication(t, 810, testSourceA, epoch, []segment.Envelope{
		testRecord(t, testSourceA, epoch, 1, 0, 10, []byte("one")),
	})
	objects := &memoryObjects{objects: make(map[string][]byte)}
	request := resolverRequest([]string{testSourceA})

	tests := []struct {
		name         string
		publications []catalog.RawSegmentPublication
		maxCount     int
		manifestMax  int64
		objectMax    int64
		want         error
	}{
		{name: "unknown", maxCount: 1, manifestMax: 1 << 20, objectMax: 1 << 20, want: ErrInvalidServiceRequest},
		{name: "too many descriptors", publications: []catalog.RawSegmentPublication{committed, committed}, maxCount: 1, manifestMax: 1 << 20, objectMax: 1 << 20, want: ErrInputBound},
		{name: "manifest bytes", publications: []catalog.RawSegmentPublication{committed}, maxCount: 1, manifestMax: int64(len(committed.ManifestBytes) - 1), objectMax: 1 << 20, want: ErrInputBound},
		{name: "object bytes", publications: []catalog.RawSegmentPublication{committed}, maxCount: 1, manifestMax: 1 << 20, objectMax: committed.ByteLength - 1, want: ErrInputBound},
	}
	uncommitted := committed
	uncommitted.State = catalog.RawSegmentVerified
	tests = append(tests, struct {
		name         string
		publications []catalog.RawSegmentPublication
		maxCount     int
		manifestMax  int64
		objectMax    int64
		want         error
	}{name: "uncommitted", publications: []catalog.RawSegmentPublication{uncommitted}, maxCount: 1, manifestMax: 1 << 20, objectMax: 1 << 20, want: ErrInvalidInput})
	escaped := committed
	escaped.ChannelID = "book"
	tests = append(tests, struct {
		name         string
		publications []catalog.RawSegmentPublication
		maxCount     int
		manifestMax  int64
		objectMax    int64
		want         error
	}{name: "filter escape", publications: []catalog.RawSegmentPublication{escaped}, maxCount: 1, manifestMax: 1 << 20, objectMax: 1 << 20, want: ErrInvalidInput})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lookup := &resolverPublicationLookup{publications: test.publications}
			resolver := newTestNativeResolver(t, lookup, objects, test.maxCount, test.manifestMax, test.objectMax)
			if _, err := resolver.ResolveNative(t.Context(), request); !errors.Is(err, test.want) {
				t.Fatalf("ResolveNative() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestNativeResolverMissingObjectIsDeterministicDiscontinuity(t *testing.T) {
	epoch := "eeeeeeee-0000-4000-8000-000000000001"
	publication := testPublication(t, 820, testSourceA, epoch, []segment.Envelope{
		testRecord(t, testSourceA, epoch, 1, 0, 10, []byte("missing")),
	})
	objects := &memoryObjects{objects: make(map[string][]byte)}
	resolver := newTestNativeResolver(t, &resolverPublicationLookup{publications: []catalog.RawSegmentPublication{publication}}, objects, 1, 1<<20, 1<<20)
	plan, err := resolver.ResolveNative(t.Context(), resolverRequest([]string{testSourceA}))
	if err != nil {
		t.Fatal(err)
	}
	var events []Event
	_, err = ReplayCollectorObserved(t.Context(), plan.Reader, plan.Inputs, plan.Config, func(event Event) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatalf("ReplayCollectorObserved() error = %v", err)
	}
	if len(events) != 1 || events[0].Kind != EventDiscontinuity || events[0].Discontinuity.Kind != DiscontinuityMissingSegment ||
		events[0].Discontinuity.SegmentID != publication.SegmentID {
		t.Fatalf("events = %+v, want exact missing-segment discontinuity", events)
	}
}

func TestResolversHonorCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	objects := &memoryObjects{objects: make(map[string][]byte)}
	native := newTestNativeResolver(t, &resolverPublicationLookup{}, objects, 1, 1<<20, 1<<20)
	if _, err := native.ResolveNative(ctx, resolverRequest([]string{testSourceA})); !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveNative() error = %v, want context canceled", err)
	}

	dataset := resolverDataset()
	lookup := &resolverDatasetLookup{manifest: resolverDatasetManifest(dataset, catalog.DatasetCommitted)}
	reader := &resolverNormalizedReader{page: func(ctx context.Context, _ warehouse.QuerySpec) (warehouse.Page, error) {
		<-ctx.Done()
		return warehouse.Page{}, context.Cause(ctx)
	}}
	resolver, err := NewNormalizedResolver(lookup, reader, NormalizedResolverOptions{PageSize: 2, MaxItems: 10})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNormalizedReplayService(resolver)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := service.OpenNormalized(t.Context(), resolverNormalizedRequest(dataset))
	if err != nil {
		t.Fatal(err)
	}
	nextCtx, nextCancel := context.WithCancel(t.Context())
	nextCancel()
	if _, err := stream.Next(nextCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("normalized Next() error = %v, want context canceled", err)
	}
	_ = stream.Close()
}

func TestNormalizedResolverRejectsUnknownAndUncommittedDatasets(t *testing.T) {
	dataset := resolverDataset()
	request := resolverNormalizedRequest(dataset)
	reader := &resolverNormalizedReader{}

	unknown := &resolverDatasetLookup{err: catalog.ErrQueryNotFound}
	resolver, err := NewNormalizedResolver(unknown, reader, NormalizedResolverOptions{PageSize: 2, MaxItems: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveNormalized(t.Context(), request); !errors.Is(err, catalog.ErrQueryNotFound) {
		t.Fatalf("unknown dataset error = %v, want ErrQueryNotFound", err)
	}

	pending := &resolverDatasetLookup{manifest: resolverDatasetManifest(dataset, catalog.DatasetPending)}
	resolver, err = NewNormalizedResolver(pending, reader, NormalizedResolverOptions{PageSize: 2, MaxItems: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.ResolveNormalized(t.Context(), request); !errors.Is(err, ErrInvalidServiceRequest) {
		t.Fatalf("pending dataset error = %v, want ErrInvalidServiceRequest", err)
	}
}

func TestNormalizedReplayOrdersGapsAndRecordsAndPagesDeterministically(t *testing.T) {
	dataset := resolverDataset()
	request := resolverNormalizedRequest(dataset)
	rowA := resolverQueryRow(dataset, "source-a", "BTC", 20, 1)
	rowB := resolverQueryRow(dataset, "source-b", "ETH", 15, 2)
	rowA.SchemaName, rowA.SchemaVersion = "TradeV1", 1
	rowB.SchemaName, rowB.SchemaVersion = "TradeV1", 1
	gapB := warehouse.GapReference{ID: "gap-a", Tuple: warehouse.Tuple{SourceID: "source-b", ChannelID: testChannel, InstrumentUID: "ETH"}, StartReceivedTimeNS: 12, EndReceivedTimeNS: 14, Kind: "source-gap"}
	gapA := warehouse.GapReference{ID: "gap-b", Tuple: warehouse.Tuple{SourceID: "source-a", ChannelID: testChannel, InstrumentUID: "BTC"}, StartReceivedTimeNS: 10, EndReceivedTimeNS: 11, Kind: "source-gap"}
	gaps := []warehouse.GapReference{gapB, gapA}
	keyA, _ := rowA.SortKey()
	keyB, _ := rowB.SortKey()
	reader := &resolverNormalizedReader{pages: []warehouse.Page{
		{Dataset: dataset, Rows: []warehouse.QueryRow{rowA}, Gaps: gaps, HasMore: true, LastKey: &keyA},
		{Dataset: dataset, Rows: []warehouse.QueryRow{rowB}, Gaps: gaps, LastKey: &keyB},
	}}
	resolver, err := NewNormalizedResolver(&resolverDatasetLookup{manifest: resolverDatasetManifest(dataset, catalog.DatasetCommitted)}, reader,
		NormalizedResolverOptions{PageSize: 1, MaxItems: 4})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewNormalizedReplayService(resolver)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := service.OpenNormalized(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	var order []string
	for {
		item, err := stream.Next(t.Context())
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("Next() error = %v", err)
		}
		if item.Gap != nil {
			order = append(order, item.Gap.ID)
		} else {
			order = append(order, item.Record.SourceID)
		}
	}
	if !slices.Equal(order, []string{"gap-b", "source-a", "gap-a", "source-b"}) {
		t.Fatalf("normalized order = %v, want deterministic source/time merge", order)
	}
	if len(reader.specs) != 2 || reader.specs[0].After != nil || reader.specs[1].After == nil ||
		!slices.Equal(reader.specs[0].SourceIDs, request.SourceIDs) || !slices.Equal(reader.specs[0].ChannelIDs, request.ChannelIDs) ||
		!slices.Equal(reader.specs[0].InstrumentUIDs, request.InstrumentUIDs) || reader.specs[0].StartReceivedTimeNS != request.StartReceivedTimeNS ||
		reader.specs[0].EndReceivedTimeNS != request.EndReceivedTimeNS || reader.specs[0].Limit != 1 {
		t.Fatalf("normalized specs = %+v, want exact bounded filter and stable cursor", reader.specs)
	}
}

func TestNormalizedReplayEnforcesItemBound(t *testing.T) {
	dataset := resolverDataset()
	request := resolverNormalizedRequest(dataset)
	rows := []warehouse.QueryRow{
		resolverQueryRow(dataset, "source-a", "BTC", 10, 1),
		resolverQueryRow(dataset, "source-a", "BTC", 20, 2),
	}
	last, _ := rows[1].SortKey()
	reader := &resolverNormalizedReader{pages: []warehouse.Page{{Dataset: dataset, Rows: rows, LastKey: &last}}}
	resolver, err := NewNormalizedResolver(&resolverDatasetLookup{manifest: resolverDatasetManifest(dataset, catalog.DatasetCommitted)}, reader,
		NormalizedResolverOptions{PageSize: 2, MaxItems: 1})
	if err != nil {
		t.Fatal(err)
	}
	service, _ := NewNormalizedReplayService(resolver)
	stream, err := service.OpenNormalized(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	if _, err := stream.Next(t.Context()); err != nil {
		t.Fatalf("first Next() error = %v", err)
	}
	if _, err := stream.Next(t.Context()); !errors.Is(err, ErrInputBound) {
		t.Fatalf("second Next() error = %v, want ErrInputBound", err)
	}
}

func TestNativeAndNormalizedResolverFilterParity(t *testing.T) {
	dataset := resolverDataset()
	request := resolverNormalizedRequest(dataset)
	epoch := "ffffffff-0000-4000-8000-000000000001"
	publication := testPublication(t, 830, "source-a", epoch, []segment.Envelope{
		testRecord(t, "source-a", epoch, 1, 0, 20, []byte("parity")),
	})
	nativeLookup := &resolverPublicationLookup{publications: []catalog.RawSegmentPublication{publication}}
	native := newTestNativeResolver(t, nativeLookup, &memoryObjects{objects: make(map[string][]byte)}, 2, 1<<20, 1<<20)
	if _, err := native.ResolveNative(t.Context(), request); err != nil {
		t.Fatal(err)
	}

	normalizedReader := &resolverNormalizedReader{pages: []warehouse.Page{{Dataset: dataset}}}
	normalized, err := NewNormalizedResolver(&resolverDatasetLookup{manifest: resolverDatasetManifest(dataset, catalog.DatasetCommitted)}, normalizedReader,
		NormalizedResolverOptions{PageSize: 2, MaxItems: 2})
	if err != nil {
		t.Fatal(err)
	}
	service, _ := NewNormalizedReplayService(normalized)
	stream, err := service.OpenNormalized(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	_, err = stream.Next(t.Context())
	if !errors.Is(err, io.EOF) {
		t.Fatalf("normalized empty Next() error = %v, want EOF", err)
	}
	_ = stream.Close()

	raw := nativeLookup.filters[0]
	query := normalizedReader.specs[0]
	if raw.DatasetID != query.Dataset.IDString() || !slices.Equal(raw.SourceIDs, query.SourceIDs) ||
		!slices.Equal(raw.ChannelIDs, query.ChannelIDs) || !slices.Equal(raw.InstrumentUIDs, query.InstrumentUIDs) ||
		raw.StartReceivedTimeNS != query.StartReceivedTimeNS || raw.EndReceivedTimeNS != query.EndReceivedTimeNS {
		t.Fatalf("native filter %+v differs from normalized query %+v", raw, query)
	}
}

func newTestNativeResolver(t *testing.T, lookup NativePublicationLookup, objects ObjectReader, maxCount int, manifestBytes, objectBytes int64) *NativeResolver {
	t.Helper()
	resolver, err := NewNativeResolver(lookup, objects, NativeResolverOptions{Replay: DefaultConfig(), MaxDescriptors: maxCount,
		MaxManifestBytes: manifestBytes, MaxObjectBytes: objectBytes})
	if err != nil {
		t.Fatalf("NewNativeResolver() error = %v", err)
	}
	return resolver
}

func resolverRequest(sources []string) ServiceRequest {
	return ServiceRequest{DatasetID: strings.Repeat("1", 64), Family: "trade", CatalogSnapshotID: strings.Repeat("2", 64),
		SchemaName: "trade_v1", SchemaVersion: 1, SourceIDs: slices.Clone(sources), ChannelIDs: []string{testChannel},
		StartReceivedTimeNS: 1, EndReceivedTimeNS: 100}
}

func resolverDataset() warehouse.Dataset {
	return warehouse.Dataset{ID: resolverHash(1), Family: "trade", CatalogSnapshotID: resolverHash(2), SchemaName: "trade_v1", SchemaVersion: 1}
}

func resolverNormalizedRequest(dataset warehouse.Dataset) ServiceRequest {
	return ServiceRequest{DatasetID: dataset.IDString(), Family: dataset.Family, CatalogSnapshotID: dataset.CatalogSnapshotIDString(),
		SchemaName: dataset.SchemaName, SchemaVersion: dataset.SchemaVersion, SourceIDs: []string{"source-a", "source-b"},
		ChannelIDs: []string{testChannel}, InstrumentUIDs: []string{"BTC", "ETH"}, StartReceivedTimeNS: 1, EndReceivedTimeNS: 100}
}

func resolverDatasetManifest(dataset warehouse.Dataset, state catalog.DatasetState) catalog.DatasetManifest {
	return catalog.DatasetManifest{Dataset: catalog.DatasetIdentity{ID: [32]byte(dataset.ID), Family: dataset.Family,
		CatalogSnapshotID: [32]byte(dataset.CatalogSnapshotID), SchemaName: dataset.SchemaName, SchemaVersion: dataset.SchemaVersion},
		ManifestPath: "dataset/manifest.json", ManifestSHA256: [32]byte(resolverHash(3)), State: state}
}

func resolverQueryRow(dataset warehouse.Dataset, sourceID, instrument string, received int64, identity byte) warehouse.QueryRow {
	epoch := strings.Repeat(fmt.Sprintf("%02x", identity), 16)
	return warehouse.QueryRow{DatasetID: dataset.IDString(), CatalogSnapshotID: dataset.CatalogSnapshotIDString(), SchemaName: dataset.SchemaName,
		SchemaVersion: dataset.SchemaVersion, EventID: resolverHash(identity + 10).String(), RowID: resolverHash(identity + 20).String(), Family: dataset.Family,
		SourceID: sourceID, ChannelID: testChannel, InstrumentUID: instrument, ConnectionEpoch: epoch, ReceivedTimeNS: received,
		ArrivalOrdinal: uint64(identity), MessageOrdinal: 0, RawSegmentSHA256: resolverHash(identity + 30).String(),
		RawRecordOrdinal: uint64(identity), RawPayloadSHA256: resolverHash(identity + 40).String()}
}

func resolverHash(value byte) warehouse.Hash {
	var result warehouse.Hash
	for index := range result {
		result[index] = value
	}
	return result
}
