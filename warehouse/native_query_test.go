package warehouse

import (
	"context"
	"crypto/sha256"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/shopspring/decimal"
)

func TestNativeQueryReaderParameterizedBoundedSelect(t *testing.T) {
	source := "source') OR 1 = 1 --"
	row := nativeQueryTestRow(source, "channel-a", "instrument-a", 100, 1)
	spec := nativeQueryTestSpec(row, 1)
	spec.Dataset.SchemaName = "enable.trade.parquet.v1"
	spec.Dataset.SchemaVersion = 7
	var capturedQuery string
	var capturedArgs []any
	transport := func(_ context.Context, query string, args ...any) (nativeQueryRows, error) {
		capturedQuery = query
		capturedArgs = slices.Clone(args)
		return newFakeNativeRows(t, []Row{row}), nil
	}
	reader, err := newNativeQueryReaderForTransport("`market`.`events_v1`", transport, staticNativeReferences{})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewQueryAdapter(reader)
	if err != nil {
		t.Fatal(err)
	}
	page, err := adapter.Page(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Rows) != 1 || page.Rows[0].SourceID != source ||
		page.Rows[0].SchemaName != row.SchemaName || page.Rows[0].SchemaVersion != row.SchemaVersion {
		t.Fatalf("unexpected page: %#v", page)
	}
	if strings.Contains(capturedQuery, source) {
		t.Fatalf("query contains caller value: %s", capturedQuery)
	}
	for _, required := range []string{
		"load_generation_id = ?", "catalog_snapshot_id = ?", "family = ?",
		"source_id IN (?)", "received_time_ns >= ?", "received_time_ns < ?",
		"ORDER BY source_id, instrument_uid, received_time_ns, connection_epoch, arrival_ordinal, message_ordinal, row_id LIMIT ?",
	} {
		if !strings.Contains(capturedQuery, required) {
			t.Fatalf("bounded query is missing %q: %s", required, capturedQuery)
		}
	}
	if strings.Contains(capturedQuery, "schema_name = ?") || strings.Contains(capturedQuery, "schema_version = ?") {
		t.Fatalf("query confused physical dataset schema with normalized row schema: %s", capturedQuery)
	}
	if got := capturedArgs[len(capturedArgs)-1]; got != spec.Limit+1 {
		t.Fatalf("query limit argument = %v, want %d", got, spec.Limit+1)
	}
	if !slices.ContainsFunc(capturedArgs, func(value any) bool { return value == source }) {
		t.Fatalf("source was not passed as a bound argument: %#v", capturedArgs)
	}
}

func TestNativeQueryReaderDeterministicPagination(t *testing.T) {
	rows := []Row{
		nativeQueryTestRow("source-a", "channel-a", "instrument-a", 100, 1),
		nativeQueryTestRow("source-a", "channel-a", "instrument-a", 100, 2),
		nativeQueryTestRow("source-a", "channel-a", "instrument-a", 101, 3),
	}
	spec := nativeQueryTestSpec(rows[0], 2)
	var queries []string
	var arguments [][]any
	transport := func(_ context.Context, query string, args ...any) (nativeQueryRows, error) {
		queries = append(queries, query)
		arguments = append(arguments, slices.Clone(args))
		if len(queries) == 1 {
			return newFakeNativeRows(t, rows), nil
		}
		return newFakeNativeRows(t, rows[2:]), nil
	}
	coverage := CoverageReference{ID: "coverage-a", Tuple: Tuple{SourceID: rows[0].SourceID, ChannelID: rows[0].ChannelID, InstrumentUID: rows[0].InstrumentUID},
		StartReceivedTimeNS: 90, EndReceivedTimeNS: 110, State: "complete"}
	gap := GapReference{ID: "gap-a", Tuple: coverage.Tuple, StartReceivedTimeNS: 101, EndReceivedTimeNS: 102, Kind: "disconnect"}
	reader, err := newNativeQueryReaderForTransport("`market`.`events_v1`", transport,
		staticNativeReferences{coverage: []CoverageReference{coverage}, gaps: []GapReference{gap}})
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := NewQueryAdapter(reader)
	if err != nil {
		t.Fatal(err)
	}
	first, err := adapter.Page(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if !first.HasMore || len(first.Rows) != 2 || first.LastKey == nil {
		t.Fatalf("unexpected first page: %#v", first)
	}
	if !slices.Equal(first.Rows[0].CoverageRefIDs, []string{"coverage-a"}) ||
		!slices.Equal(first.Rows[1].CoverageRefIDs, []string{"coverage-a"}) || len(first.Rows[0].GapRefIDs) != 0 {
		t.Fatalf("first-page reference attachment mismatch: %#v", first.Rows)
	}
	spec.After = first.LastKey
	second, err := adapter.Page(t.Context(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if second.HasMore || len(second.Rows) != 1 || second.Rows[0].RowID != rows[2].RowID.String() {
		t.Fatalf("unexpected second page: %#v", second)
	}
	if !slices.Equal(second.Rows[0].CoverageRefIDs, []string{"coverage-a"}) || !slices.Equal(second.Rows[0].GapRefIDs, []string{"gap-a"}) {
		t.Fatalf("second-page reference attachment mismatch: %#v", second.Rows[0])
	}
	combined := []string{first.Rows[0].RowID, first.Rows[1].RowID, second.Rows[0].RowID}
	want := []string{rows[0].RowID.String(), rows[1].RowID.String(), rows[2].RowID.String()}
	if !slices.Equal(combined, want) {
		t.Fatalf("pagination duplicated or omitted rows: got %v want %v", combined, want)
	}
	if len(queries) != 2 || !strings.Contains(queries[1], "(source_id, instrument_uid, received_time_ns, connection_epoch, arrival_ordinal, message_ordinal, row_id) > (?, ?, ?, ?, ?, ?, ?)") {
		t.Fatalf("continuation predicate missing: %v", queries)
	}
	continuation := arguments[1][len(arguments[1])-8 : len(arguments[1])-1]
	wantContinuation := []any{first.LastKey.SourceID, first.LastKey.InstrumentUID, first.LastKey.ReceivedTimeNS,
		string(first.LastKey.ConnectionEpoch[:]), first.LastKey.ArrivalOrdinal, first.LastKey.MessageOrdinal, string(first.LastKey.RowID[:])}
	for i := range continuation {
		if continuation[i] != wantContinuation[i] {
			t.Fatalf("continuation argument %d = %#v, want %#v", i, continuation[i], wantContinuation[i])
		}
	}
}

func TestNativeQueryCursorClosesOnCancellationAndScanError(t *testing.T) {
	row := nativeQueryTestRow("source-a", "channel-a", "instrument-a", 100, 1)
	spec := nativeQueryTestSpec(row, 1)
	t.Run("cancellation", func(t *testing.T) {
		rows := newFakeNativeRows(t, []Row{row})
		reader, err := newNativeQueryReaderForTransport("`market`.`events_v1`", func(context.Context, string, ...any) (nativeQueryRows, error) {
			return rows, nil
		}, staticNativeReferences{})
		if err != nil {
			t.Fatal(err)
		}
		var releases int
		reader.release = func() { releases++ }
		cursor, err := reader.OpenQuery(t.Context(), spec)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := cursor.Next(ctx); !errors.Is(err, context.Canceled) {
			t.Fatalf("Next error = %v, want context cancellation", err)
		}
		if rows.closeCalls != 1 || releases != 1 {
			t.Fatalf("cancellation cleanup: closes=%d releases=%d", rows.closeCalls, releases)
		}
		if err := cursor.Close(); err != nil || rows.closeCalls != 1 || releases != 1 {
			t.Fatalf("idempotent Close error=%v closes=%d releases=%d", err, rows.closeCalls, releases)
		}
	})
	t.Run("scan error", func(t *testing.T) {
		rows := newFakeNativeRows(t, []Row{row})
		rows.scanErr = errors.New("synthetic scan failure")
		reader, err := newNativeQueryReaderForTransport("`market`.`events_v1`", func(context.Context, string, ...any) (nativeQueryRows, error) {
			return rows, nil
		}, staticNativeReferences{})
		if err != nil {
			t.Fatal(err)
		}
		var releases int
		reader.release = func() { releases++ }
		cursor, err := reader.OpenQuery(t.Context(), spec)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cursor.Next(t.Context()); err == nil || !strings.Contains(err.Error(), "synthetic scan failure") {
			t.Fatalf("Next error = %v", err)
		}
		if rows.closeCalls != 1 || releases != 1 {
			t.Fatalf("scan-error cleanup: closes=%d releases=%d", rows.closeCalls, releases)
		}
	})
}

func TestNativeQueryReaderReleasesConnectionOnOpenError(t *testing.T) {
	row := nativeQueryTestRow("source-a", "channel-a", "instrument-a", 100, 1)
	spec := nativeQueryTestSpec(row, 1)
	reader, err := newNativeQueryReaderForTransport("`market`.`events_v1`", func(context.Context, string, ...any) (nativeQueryRows, error) {
		return nil, errors.New("synthetic query failure")
	}, staticNativeReferences{})
	if err != nil {
		t.Fatal(err)
	}
	var releases int
	reader.release = func() { releases++ }
	if _, err := reader.OpenQuery(t.Context(), spec); err == nil || !strings.Contains(err.Error(), "synthetic query failure") {
		t.Fatalf("OpenQuery error = %v", err)
	}
	if releases != 1 {
		t.Fatalf("connection releases = %d, want 1", releases)
	}
}

func TestNativeQueryReaderRejectsUnboundedSpecBeforeTransport(t *testing.T) {
	row := nativeQueryTestRow("source-a", "channel-a", "instrument-a", 100, 1)
	spec := nativeQueryTestSpec(row, 1)
	spec.Limit = 0
	var called bool
	reader, err := newNativeQueryReaderForTransport("`market`.`events_v1`", func(context.Context, string, ...any) (nativeQueryRows, error) {
		called = true
		return nil, nil
	}, staticNativeReferences{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reader.OpenQuery(t.Context(), spec); !errors.Is(err, ErrInvalidQuery) {
		t.Fatalf("OpenQuery error = %v, want invalid query", err)
	}
	if called {
		t.Fatal("transport was called for an unbounded query")
	}
}

type staticNativeReferences struct {
	coverage []CoverageReference
	gaps     []GapReference
	err      error
}

func (r staticNativeReferences) References(context.Context, QuerySpec) ([]CoverageReference, []GapReference, error) {
	return slices.Clone(r.coverage), slices.Clone(r.gaps), r.err
}

type fakeNativeRows struct {
	t          *testing.T
	values     [][]any
	index      int
	scanErr    error
	err        error
	closeErr   error
	closeCalls int
}

func newFakeNativeRows(t *testing.T, rows []Row) *fakeNativeRows {
	t.Helper()
	values := make([][]any, len(rows))
	for i, row := range rows {
		encoded, err := rowValues(row)
		if err != nil {
			t.Fatal(err)
		}
		values[i] = encoded
	}
	return &fakeNativeRows{t: t, values: values, index: -1}
}

func (r *fakeNativeRows) Next() bool {
	if r.index+1 >= len(r.values) {
		return false
	}
	r.index++
	return true
}

func (r *fakeNativeRows) Scan(destinations ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	if r.index < 0 || r.index >= len(r.values) {
		return errors.New("Scan called without a current row")
	}
	values := r.values[r.index]
	if len(destinations) != len(values) {
		return errors.New("destination count mismatch")
	}
	for i, destination := range destinations {
		if err := assignFakeNativeValue(destination, values[i]); err != nil {
			return err
		}
	}
	return nil
}

func (r *fakeNativeRows) Err() error { return r.err }

func (r *fakeNativeRows) Close() error {
	r.closeCalls++
	return r.closeErr
}

func assignFakeNativeValue(destination, value any) error {
	switch output := destination.(type) {
	case *[]byte:
		input, ok := value.([]byte)
		if !ok {
			return errors.New("fake FixedString type mismatch")
		}
		*output = slices.Clone(input)
	case *string:
		*output = value.(string)
	case *int64:
		*output = value.(int64)
	case *uint64:
		*output = value.(uint64)
	case *uint32:
		*output = value.(uint32)
	case *uint16:
		*output = value.(uint16)
	case **decimal.Decimal:
		if value == nil {
			*output = nil
			break
		}
		input := value.(decimal.Decimal)
		*output = &input
	default:
		return errors.New("unsupported fake destination")
	}
	return nil
}

func nativeQueryTestSpec(row Row, limit int) QuerySpec {
	return QuerySpec{
		Dataset: Dataset{ID: row.GenerationID, Family: row.Family, CatalogSnapshotID: row.CatalogSnapshotID,
			SchemaName: row.SchemaName, SchemaVersion: row.SchemaVersion},
		SourceIDs: []string{row.SourceID}, StartReceivedTimeNS: 90, EndReceivedTimeNS: 110, Limit: limit,
	}
}

func nativeQueryTestRow(sourceID, channelID, instrumentUID string, receivedTimeNS int64, ordinal uint64) Row {
	hash := func(label string) Hash { return sha256.Sum256([]byte(label)) }
	epochHash := hash("epoch")
	var epoch [16]byte
	copy(epoch[:], epochHash[:16])
	return Row{
		GenerationID: hash("generation"), ManifestHash: hash("manifest"), RowID: hash("row-" + string(rune(ordinal))),
		EventID: hash("event-" + string(rune(ordinal))), LogicalHash: hash("logical-" + string(rune(ordinal))),
		Family: "trades", SourceID: sourceID, ChannelID: channelID, InstrumentUID: instrumentUID, EpochKind: "connection",
		ConnectionEpoch: epoch, ReceivedTimeNS: receivedTimeNS, ArrivalOrdinal: ordinal, MessageOrdinal: uint32(ordinal),
		RawSegmentSHA256: hash("segment"), RawRecordOrdinal: ordinal, RawPayloadSHA256: hash("payload-" + string(rune(ordinal))),
		CatalogSnapshotID: hash("catalog"), SchemaName: "normalized_v1", SchemaVersion: 1,
		DatasetPolicyID: hash("policy"), ReplayConfigID: hash("replay"), InputManifestSetID: hash("input"), PhysicalOrdinal: ordinal,
	}
}
