package binance

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/jackc/pgx/v5"
)

const spotPostgresDSNEnvironment = "MARKETDATA_TEST_POSTGRES_DSN"

var spotSchemaSequence atomic.Uint64

func TestSpotCatalogFixtureParser(t *testing.T) {
	bundle, err := LoadFixtureBundle("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatalf("LoadFixtureBundle() error = %v", err)
	}
	official, ok := bundle.Metadata("official-schema-excerpt")
	if !ok {
		t.Fatal("official fixture metadata is missing")
	}
	if official.ByteLength != OfficialExchangeInfoExcerptBytes ||
		official.SHA256 != OfficialExchangeInfoExcerptSHA256 || official.ParseableResponse {
		t.Fatalf("official fixture provenance = %+v", official)
	}
	page, err := bundle.CapturedPage("active")
	if err != nil {
		t.Fatalf("CapturedPage(active) error = %v", err)
	}
	composed, err := ComposeExchangeInfo([]CapturedPage{page}, ComposeOptions{}, DefaultParserLimits())
	if err != nil {
		t.Fatalf("ComposeExchangeInfo(active) error = %v", err)
	}
	if len(composed.Candidates) != 2 || composed.Candidates[0].NativeID != "BTCUSDT" || composed.Candidates[1].NativeID != "ETHUSDT" {
		t.Fatalf("candidate identities = %+v", composed.Candidates)
	}
	btc := composed.Symbols[0]
	var tick string
	var unknown Filter
	for _, filter := range btc.Filters {
		switch filter.Type {
		case "PRICE_FILTER":
			for _, field := range filter.ExactFields {
				if field.Name == "tickSize" {
					tick = field.Value
				}
			}
		case "FUTURE_EXACT_FILTER":
			unknown = filter
		}
	}
	if tick != "0.01000000" {
		t.Fatalf("exact tick = %q", tick)
	}
	if unknown.Known || !bytes.Contains(unknown.Raw, []byte("9007199254740993.000000000000000001")) {
		t.Fatalf("unknown filter was not preserved exactly: %+v", unknown)
	}
	fixtureBytes, _ := bundle.Bytes("active")
	if !bytes.Equal(page.Raw, fixtureBytes) {
		t.Fatal("captured page did not retain exact fixture bytes")
	}
	page.Raw[0] = '['
	if _, err := ParseExchangeInfoPage(page, DefaultParserLimits()); !errors.Is(err, ErrInvalidExchangeInfo) {
		t.Fatalf("tampered raw error = %v, want ErrInvalidExchangeInfo", err)
	}
}

func TestOfficialFixtureDigestCannotSelfAuthorize(t *testing.T) {
	manifestBytes, err := os.ReadFile("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	var manifest FixtureBundle
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	for i := range manifest.Fixtures {
		fixture := &manifest.Fixtures[i]
		contents, err := os.ReadFile(filepath.Join("../testdata/binance", fixture.File))
		if err != nil {
			t.Fatal(err)
		}
		if fixture.Name == "official-schema-excerpt" {
			contents = slices.Clone(contents)
			contents[0] ^= 1
			digest := sha256.Sum256(contents)
			fixture.SHA256 = hex.EncodeToString(digest[:])
			fixture.ByteLength = len(contents)
		}
		if err := os.WriteFile(filepath.Join(root, fixture.File), contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	tamperedManifest, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "manifest.json")
	if err := os.WriteFile(manifestPath, tamperedManifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFixtureBundle(manifestPath); err == nil || !strings.Contains(err.Error(), "immutable pinned commit evidence") {
		t.Fatalf("self-authorized official fixture error = %v", err)
	}
}

func TestCapturedPageEnvelopeBinding(t *testing.T) {
	bundle, err := LoadFixtureBundle("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	fixturePage, err := bundle.CapturedPage("active")
	if err != nil {
		t.Fatal(err)
	}
	segment, requestEnvelope, responseEnvelope := capturedSpotEnvelopePair(t, fixturePage)
	if _, err := CapturedPageFromEnvelopes(0, 1, segment, requestEnvelope, responseEnvelope); err != nil {
		t.Fatalf("valid envelope pair error = %v", err)
	}
	tests := []struct {
		name   string
		mutate func(*capture.EnvelopeV1, *capture.EnvelopeV1)
	}{
		{name: "source", mutate: func(_, response *capture.EnvelopeV1) { response.SourceID = "another-source" }},
		{name: "endpoint", mutate: func(request, _ *capture.EnvelopeV1) { request.ChannelOrEndpoint = "rest.other" }},
		{name: "poll cycle", mutate: func(_, response *capture.EnvelopeV1) { response.PollCycleID.Value[0] ^= 1 }},
		{name: "arrival order", mutate: func(request, response *capture.EnvelopeV1) { response.ArrivalOrdinal = request.ArrivalOrdinal }},
		{name: "request identity", mutate: func(_, response *capture.EnvelopeV1) { response.SubscriptionOrRequestID.Value = "different" }},
		{name: "timing", mutate: func(_, response *capture.EnvelopeV1) { response.RequestCompletedAtNS.Value++ }},
		{name: "status", mutate: func(_, response *capture.EnvelopeV1) { response.HTTPStatusOrWSState.Value = "201" }},
		{name: "terminal outcome", mutate: func(_, response *capture.EnvelopeV1) { response.TerminalOutcome = capture.TerminalFailed }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := requestEnvelope
			response := responseEnvelope
			test.mutate(&request, &response)
			if _, err := CapturedPageFromEnvelopes(0, 1, segment, request, response); !errors.Is(err, ErrInvalidExchangeInfo) {
				t.Fatalf("CapturedPageFromEnvelopes() error = %v, want ErrInvalidExchangeInfo", err)
			}
		})
	}
}

func capturedSpotEnvelopePair(t *testing.T, page CapturedPage) (CommittedRawSegment, capture.EnvelopeV1, capture.EnvelopeV1) {
	t.Helper()
	requestEvidence, err := capture.MarshalRESTRequestEvidence(page.Request)
	if err != nil {
		t.Fatal(err)
	}
	responseEvidence, err := capture.MarshalRESTResponseEvidence(page.Response)
	if err != nil {
		t.Fatal(err)
	}
	var pollCycle [16]byte
	for i := range pollCycle {
		pollCycle[i] = byte(i + 1)
	}
	request := capture.EnvelopeV1{
		EnvelopeVersion:            capture.EnvelopeVersion,
		RecordKind:                 capture.RecordKindControl,
		SourceID:                   SpotSourceID,
		ChannelOrEndpoint:          SpotExchangeInfoChannel,
		PollCycleID:                capture.OptionalEpoch{Value: pollCycle, Valid: true},
		ArrivalOrdinal:             1,
		MessageOrdinal:             0,
		ScheduledAtNS:              capture.OptionalInt64{Value: page.Request.ScheduledAtNS, Valid: true},
		RequestStartedAtNS:         capture.OptionalInt64{Value: page.Request.StartedAtNS, Valid: true},
		ReceivedWallTimeNS:         page.Request.StartedAtNS,
		ClockEpochID:               "fixture-clock",
		MonotonicNSSinceClockEpoch: 1,
		SubscriptionOrRequestID:    capture.OptionalString{Value: page.Request.RequestID, Valid: true},
		PayloadEncoding:            capture.PayloadEncodingNone,
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            "fixture-recorder",
		ControlKind:                capture.OptionalControlKind{Value: capture.ControlRequestStarted, Valid: true},
		Extensions:                 requestEvidence,
	}
	request.SetRawPayload(nil)
	response := capture.EnvelopeV1{
		EnvelopeVersion:            capture.EnvelopeVersion,
		RecordKind:                 capture.RecordKindREST,
		SourceID:                   SpotSourceID,
		ChannelOrEndpoint:          SpotExchangeInfoChannel,
		PollCycleID:                capture.OptionalEpoch{Value: pollCycle, Valid: true},
		ArrivalOrdinal:             2,
		MessageOrdinal:             0,
		ScheduledAtNS:              capture.OptionalInt64{Value: page.Request.ScheduledAtNS, Valid: true},
		RequestStartedAtNS:         capture.OptionalInt64{Value: page.Request.StartedAtNS, Valid: true},
		RequestCompletedAtNS:       capture.OptionalInt64{Value: page.Response.CompletedAtNS, Valid: true},
		ReceivedWallTimeNS:         page.Response.CompletedAtNS,
		ClockEpochID:               "fixture-clock",
		MonotonicNSSinceClockEpoch: 2,
		SubscriptionOrRequestID:    capture.OptionalString{Value: page.Request.RequestID, Valid: true},
		HTTPStatusOrWSState:        capture.OptionalString{Value: "200", Valid: true},
		PayloadEncoding:            capture.PayloadEncodingJSON,
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            "fixture-recorder",
		Extensions:                 responseEvidence,
	}
	response.SetRawPayload(page.Raw)
	segmentHash := sha256.Sum256(page.Raw)
	return CommittedRawSegment{
		SegmentID:     "00000000-0000-0000-0000-00000000b001",
		ObjectKey:     "fixtures/binance/envelope-binding.zst",
		ContentSHA256: segmentHash,
		ByteLength:    int64(len(page.Raw)),
	}, request, response
}

func TestSpotCatalogPaginationAndBounds(t *testing.T) {
	bundle, err := LoadFixtureBundle("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatalf("LoadFixtureBundle() error = %v", err)
	}
	page0, err := bundle.CapturedPage("page-0")
	if err != nil {
		t.Fatal(err)
	}
	page1, err := bundle.CapturedPage("page-1")
	if err != nil {
		t.Fatal(err)
	}
	composed, err := ComposeExchangeInfo([]CapturedPage{page1, page0}, ComposeOptions{}, DefaultParserLimits())
	if err != nil {
		t.Fatalf("ComposeExchangeInfo(pages) error = %v", err)
	}
	if got := []string{composed.Candidates[0].NativeID, composed.Candidates[1].NativeID}; !slices.Equal(got, []string{"BTCUSDT", "ETHUSDT"}) {
		t.Fatalf("composed identities = %q", got)
	}
	if _, err := ComposeExchangeInfo([]CapturedPage{page0, page0}, ComposeOptions{}, DefaultParserLimits()); !errors.Is(err, ErrInvalidExchangeInfo) {
		t.Fatalf("duplicate page error = %v", err)
	}
	incomplete := page0
	incomplete.PageCount = 2
	if _, err := ComposeExchangeInfo([]CapturedPage{incomplete}, ComposeOptions{}, DefaultParserLimits()); !errors.Is(err, ErrInvalidExchangeInfo) {
		t.Fatalf("incomplete page error = %v", err)
	}
	conflict := page1
	conflict.Request.RequestID = "fixture-page-1-conflict"
	conflict.Response.RequestID = conflict.Request.RequestID
	conflict.Raw = bytes.Replace(conflict.Raw, []byte("1787385600000"), []byte("1787385600001"), 1)
	conflict.RawSHA256 = catalogPayloadHash(conflict.Raw)
	if _, err := ComposeExchangeInfo([]CapturedPage{page0, conflict}, ComposeOptions{}, DefaultParserLimits()); !errors.Is(err, ErrInvalidExchangeInfo) {
		t.Fatalf("conflicting response identity error = %v", err)
	}
	pageLimits := DefaultParserLimits()
	pageLimits.MaxPages = 1
	if _, err := ComposeExchangeInfo([]CapturedPage{page0, page1}, ComposeOptions{}, pageLimits); !errors.Is(err, ErrInvalidExchangeInfo) {
		t.Fatalf("page bound error = %v", err)
	}
	limits := DefaultParserLimits()
	limits.MaxFilters = 1
	active, _ := bundle.CapturedPage("active")
	if _, err := ParseExchangeInfoPage(active, limits); !errors.Is(err, ErrInvalidExchangeInfo) {
		t.Fatalf("filter bound error = %v", err)
	}
	duplicateKey := active
	duplicateKey.Raw = bytes.Replace(duplicateKey.Raw, []byte(`"timezone":"UTC",`), []byte(`"timezone":"UTC","timezone":"UTC",`), 1)
	duplicateKey.RawSHA256 = catalogPayloadHash(duplicateKey.Raw)
	if _, err := ParseExchangeInfoPage(duplicateKey, DefaultParserLimits()); !errors.Is(err, ErrInvalidExchangeInfo) {
		t.Fatalf("duplicate JSON key error = %v", err)
	}
	filtered := active
	filtered.Request.Parameters = []capture.SanitizedParameter{{Name: "symbol", Value: "BTCUSDT"}}
	if _, err := ParseExchangeInfoPage(filtered, DefaultParserLimits()); !errors.Is(err, ErrInvalidExchangeInfo) {
		t.Fatalf("filtered complete snapshot error = %v", err)
	}
}

func TestSpotSyncObservedAtUsesCompleteResponseBoundary(t *testing.T) {
	bundle, err := LoadFixtureBundle("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	page, err := bundle.CapturedPage("active")
	if err != nil {
		t.Fatal(err)
	}
	composed, err := ComposeExchangeInfo([]CapturedPage{page}, ComposeOptions{}, DefaultParserLimits())
	if err != nil {
		t.Fatal(err)
	}
	if composed.CompletedAtNS != page.Response.CompletedAtNS {
		t.Fatalf("completion boundary = %d, want %d", composed.CompletedAtNS, page.Response.CompletedAtNS)
	}
	composed.CompletedAtNS += 1000
	if _, err := SpotSyncInput(composed); !errors.Is(err, catalog.ErrInvalidCatalog) {
		t.Fatalf("arbitrary observed time error = %v, want catalog.ErrInvalidCatalog", err)
	}
}

func TestSpotCatalogTemporalPostgreSQL(t *testing.T) {
	conn := newSpotPostgres(t)
	store, err := catalog.NewStore(conn)
	if err != nil {
		t.Fatalf("catalog.NewStore() error = %v", err)
	}
	bundle, err := LoadFixtureBundle("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	first := syncSpotFixture(t, conn, store, bundle, "active", nil)
	identical := syncSpotFixture(t, conn, store, bundle, "active", nil)
	if first.SHA256 != identical.SHA256 {
		t.Fatalf("identical sync hashes differ: %x != %x", first.SHA256, identical.SHA256)
	}
	assertCount(t, conn, "SELECT count(*) FROM instrument_version", 2)
	changed := syncSpotFixture(t, conn, store, bundle, "changed", nil)
	if changed.SHA256 == first.SHA256 {
		t.Fatal("metadata change did not change snapshot hash")
	}
	assertCount(t, conn, `
SELECT count(*) FROM instrument_version iv
JOIN instrument i ON i.instrument_uid = iv.instrument_uid
WHERE i.native_id = 'BTCUSDT'
`, 2)
	absence := syncSpotFixture(t, conn, store, bundle, "absence", nil)
	if absence.SHA256 != changed.SHA256 {
		t.Fatalf("temporary absence changed complete snapshot: %x != %x", absence.SHA256, changed.SHA256)
	}
	assertCount(t, conn, `
SELECT count(*) FROM instrument_alias WHERE alias = 'BTCUSDT' AND valid_to IS NULL
`, 1)
	closed := syncSpotFixture(t, conn, store, bundle, "closed", []string{"BTCUSDT"})
	if closed.InstrumentCount != 1 {
		t.Fatalf("closed snapshot instrument count = %d", closed.InstrumentCount)
	}
	assertCount(t, conn, `SELECT count(*) FROM source WHERE source_id = $1`, 1, SpotSourceID)
	assertCount(t, conn, `SELECT count(*) FROM source_version WHERE source_id = $1`, 1, SpotSourceID)
	assertCount(t, conn, `SELECT count(*) FROM channel_contract WHERE source_id = $1 AND channel_id = $2`, 1, SpotSourceID, SpotExchangeInfoChannel)
	assertCount(t, conn, `
SELECT count(*) FROM instrument_alias WHERE alias = 'BTCUSDT' AND valid_to IS NULL
`, 0)
	assertCount(t, conn, `
SELECT count(*) FROM catalog_sync_run
`, 4)
	assertCount(t, conn, `
SELECT count(*) FROM opportunity
WHERE source_id = $1 AND channel_id = $2 AND opportunity_kind = 'metadata_sync'
`, 4, SpotSourceID, SpotExchangeInfoChannel)
}

func TestSpotCatalogRejectsRawEvidenceMismatchPostgreSQL(t *testing.T) {
	conn := newSpotPostgres(t)
	store, err := catalog.NewStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := LoadFixtureBundle("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	page, err := bundle.CapturedPage("active")
	if err != nil {
		t.Fatal(err)
	}
	commitSyntheticRawEvidence(t, conn, &page)
	composed, err := ComposeExchangeInfo([]CapturedPage{page}, ComposeOptions{}, DefaultParserLimits())
	if err != nil {
		t.Fatal(err)
	}
	valid, err := SpotSyncInput(composed)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*catalog.SyncInput)
	}{
		{name: "source", mutate: func(input *catalog.SyncInput) {
			input.Source.SourceID = "00000000-0000-0000-0000-00000000eeee"
		}},
		{name: "channel", mutate: func(input *catalog.SyncInput) {
			channel := input.Channels[0]
			channel.ChannelID = "rest.other"
			input.Channels = append(slices.Clone(input.Channels), channel)
			input.Pages[0].ChannelID = channel.ChannelID
		}},
		{name: "object key", mutate: func(input *catalog.SyncInput) {
			input.Pages[0].RawRecord.ObjectKey += ".other"
		}},
		{name: "segment digest", mutate: func(input *catalog.SyncInput) {
			input.Pages[0].RawRecord.RawSegmentSHA256 = strings.Repeat("0", sha256.Size*2)
		}},
		{name: "in-memory projection", mutate: func(input *catalog.SyncInput) {
			input.Pages[0].RawRecord.EvidenceScope = catalog.RawEvidenceInMemoryProjection
			input.Pages[0].RawRecord.ObjectKey = "in-memory://fixture-projection/not-committed"
		}},
		{name: "segment length", mutate: func(input *catalog.SyncInput) {
			input.Pages[0].RawRecord.RawSegmentByteLength++
		}},
		{name: "poll cycle", mutate: func(input *catalog.SyncInput) {
			input.Pages[0].RawRecord.PollCycleID = "00000000-0000-0000-0000-00000000ffff"
		}},
		{name: "payload digest", mutate: func(input *catalog.SyncInput) {
			input.Pages[0].RawSHA256 = strings.Repeat("0", sha256.Size*2)
		}},
		{name: "payload length", mutate: func(input *catalog.SyncInput) {
			input.Pages[0].RawByteLength--
		}},
		{name: "record coordinate", mutate: func(input *catalog.SyncInput) {
			input.Pages[0].RawRecord.MessageOrdinal++
		}},
		{name: "request identity", mutate: func(input *catalog.SyncInput) {
			input.Pages[0].RequestEvidence = bytes.Replace(
				input.Pages[0].RequestEvidence, []byte("fixture-active"), []byte("fixture-activf"), 1,
			)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := valid
			input.Pages = slices.Clone(valid.Pages)
			test.mutate(&input)
			if _, err := store.Sync(t.Context(), input); !errors.Is(err, catalog.ErrInvalidCatalog) {
				t.Fatalf("Store.Sync() error = %v, want catalog.ErrInvalidCatalog", err)
			}
		})
	}
	assertCount(t, conn, "SELECT count(*) FROM source", 1)
	assertCount(t, conn, "SELECT count(*) FROM source_version", 0)
	assertCount(t, conn, "SELECT count(*) FROM channel_contract", 1)
	assertCount(t, conn, "SELECT count(*) FROM instrument", 0)
	assertCount(t, conn, "SELECT count(*) FROM opportunity", 0)
	assertCount(t, conn, "SELECT count(*) FROM catalog_snapshot", 0)
	assertCount(t, conn, "SELECT count(*) FROM catalog_sync_run", 0)
	if _, err := conn.Exec(t.Context(), `
DELETE FROM raw_segment_manifest WHERE raw_segment_id = $1
`, page.RawRecord.RawSegmentID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sync(t.Context(), valid); !errors.Is(err, catalog.ErrInvalidCatalog) {
		t.Fatalf("manifest-less Store.Sync() error = %v, want catalog.ErrInvalidCatalog", err)
	}
	commitSyntheticRawEvidence(t, conn, &page)
	if _, err := conn.Exec(t.Context(), `
UPDATE raw_segment SET state = 'verified', committed_at = NULL
WHERE raw_segment_id = $1
`, page.RawRecord.RawSegmentID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sync(t.Context(), valid); !errors.Is(err, catalog.ErrInvalidCatalog) {
		t.Fatalf("uncommitted Store.Sync() error = %v, want catalog.ErrInvalidCatalog", err)
	}
	if _, err := conn.Exec(t.Context(), `
UPDATE raw_segment SET state = 'committed', committed_at = $2
WHERE raw_segment_id = $1
`, page.RawRecord.RawSegmentID, time.Unix(0, page.Response.CompletedAtNS).UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sync(t.Context(), valid); err != nil {
		t.Fatalf("valid committed Store.Sync() error = %v", err)
	}
}

func TestSpotCatalogOpportunityConflictDoesNotDoubleCountPostgreSQL(t *testing.T) {
	conn := newSpotPostgres(t)
	store, err := catalog.NewStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := LoadFixtureBundle("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	syncSpotFixture(t, conn, store, bundle, "active", nil)
	page, err := bundle.CapturedPage("active")
	if err != nil {
		t.Fatal(err)
	}
	commitSyntheticRawEvidence(t, conn, &page)
	page.Response.Headers = append(page.Response.Headers, capture.RESTHeader{
		Kind: capture.RESTHeaderUsedWeight, Value: "20",
	})
	composed, err := ComposeExchangeInfo([]CapturedPage{page}, ComposeOptions{}, DefaultParserLimits())
	if err != nil {
		t.Fatal(err)
	}
	input, err := SpotSyncInput(composed)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Sync(t.Context(), input); !errors.Is(err, catalog.ErrInvalidCatalog) {
		t.Fatalf("conflicting opportunity replay error = %v, want catalog.ErrInvalidCatalog", err)
	}
	assertCount(t, conn, `
SELECT count(*) FROM opportunity
WHERE source_id = $1 AND channel_id = $2 AND opportunity_kind = 'metadata_sync'
`, 1, SpotSourceID, SpotExchangeInfoChannel)
}

func TestSymbolReusePostgreSQL(t *testing.T) {
	conn := newSpotPostgres(t)
	store, err := catalog.NewStore(conn)
	if err != nil {
		t.Fatal(err)
	}
	bundle, err := LoadFixtureBundle("../testdata/binance/manifest.json")
	if err != nil {
		t.Fatal(err)
	}
	syncSpotFixture(t, conn, store, bundle, "active", nil)
	syncSpotFixture(t, conn, store, bundle, "closed", []string{"BTCUSDT"})
	snapshot := syncSpotFixture(t, conn, store, bundle, "reuse", nil)
	if snapshot.InstrumentCount != 2 {
		t.Fatalf("reuse snapshot instrument count = %d", snapshot.InstrumentCount)
	}
	rows, err := conn.Query(t.Context(), `
SELECT instrument_uid::text, listing_epoch, first_observed_at
FROM instrument
WHERE source_id = $1 AND native_id = 'BTCUSDT'
ORDER BY listing_epoch
`, SpotSourceID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var uids []string
	var generations []int64
	var observations []time.Time
	for rows.Next() {
		var uid string
		var generation int64
		var observed time.Time
		if err := rows.Scan(&uid, &generation, &observed); err != nil {
			t.Fatal(err)
		}
		uids = append(uids, uid)
		generations = append(generations, generation)
		observations = append(observations, observed)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(uids) != 2 || uids[0] == uids[1] || !slices.Equal(generations, []int64{0, 1}) || !observations[1].After(observations[0]) {
		t.Fatalf("reuse generations uids=%q generations=%v observations=%v", uids, generations, observations)
	}
	assertCount(t, conn, `
SELECT count(*) FROM instrument_alias
WHERE source_id = $1 AND alias = 'BTCUSDT'
`, 2, SpotSourceID)
	assertCount(t, conn, `
SELECT count(*) FROM instrument_alias
WHERE source_id = $1 AND alias = 'BTCUSDT' AND valid_to IS NULL
`, 1, SpotSourceID)
}

func syncSpotFixture(t *testing.T, conn *pgx.Conn, store *catalog.Store, bundle FixtureBundle, name string, closures []string) catalog.Snapshot {
	t.Helper()
	page, err := bundle.CapturedPage(name)
	if err != nil {
		t.Fatal(err)
	}
	commitSyntheticRawEvidence(t, conn, &page)
	composed, err := ComposeExchangeInfo([]CapturedPage{page}, ComposeOptions{ExplicitLifecycleClosures: closures}, DefaultParserLimits())
	if err != nil {
		t.Fatal(err)
	}
	input, err := SpotSyncInput(composed)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := store.Sync(t.Context(), input)
	if err != nil {
		t.Fatalf("Sync(%s) error = %v", name, err)
	}
	return snapshot
}

func commitSyntheticRawEvidence(t *testing.T, conn *pgx.Conn, page *CapturedPage) {
	t.Helper()
	source, _, channels := SpotCatalogContract()
	channel := channels[0]
	_, err := conn.Exec(t.Context(), `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state, created_at
) VALUES ($1, $2, $3, $4, $5, $6, '2000-01-01T00:00:00Z')
ON CONFLICT (source_id) DO NOTHING
`, source.SourceID, source.Venue, source.ProductFamily, source.APIFamily, source.Environment, source.Lifecycle)
	if err != nil {
		t.Fatalf("seed synthetic raw source: %v", err)
	}
	_, err = conn.Exec(t.Context(), `
INSERT INTO channel_contract (
    source_id, channel_id, valid_from, native_selector, channel_role, data_family,
    cadence_source, aggregation, depth, sequence_rules, checksum_rules, payload_schema,
    support_state, limitation
) VALUES (
    $1, $2, '2000-01-01T00:00:00Z', $3::jsonb, $4, $5,
    $6, $7::jsonb, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb,
    $12, $13
)
ON CONFLICT (source_id, channel_id, valid_from) DO NOTHING
`, source.SourceID, channel.ChannelID, channel.NativeSelector, channel.Role, channel.DataFamily,
		channel.CadenceSource, channel.Aggregation, channel.Depth, channel.SequenceRules,
		channel.ChecksumRules, channel.PayloadSchema, channel.SupportState, channel.Limitation)
	if err != nil {
		t.Fatalf("seed synthetic raw channel: %v", err)
	}
	page.RawRecord.EvidenceScope = catalog.RawEvidenceCommitted
	page.RawRecord.ObjectKey = "synthetic-fixtures/binance/" + page.RawRecord.RawSegmentID + ".zst"
	contentHash, err := hex.DecodeString(page.RawRecord.RawSegmentSHA256)
	if err != nil {
		t.Fatal(err)
	}
	_, err = conn.Exec(t.Context(), `
INSERT INTO raw_segment (
    raw_segment_id, source_id, channel_id, epoch_id,
    receive_time_start_ns, receive_time_end_ns, ordinal_start, ordinal_end,
    object_key, content_hash, byte_length, state, committed_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $7, $8, $9, $10, 'committed', $11)
ON CONFLICT (raw_segment_id) DO NOTHING
`, page.RawRecord.RawSegmentID, source.SourceID, channel.ChannelID, page.RawRecord.PollCycleID,
		page.Request.StartedAtNS, page.Response.CompletedAtNS, int64(page.RawRecord.ArrivalOrdinal),
		page.RawRecord.ObjectKey, contentHash, page.RawRecord.RawSegmentByteLength,
		time.Unix(0, page.Response.CompletedAtNS).UTC())
	if err != nil {
		t.Fatalf("seed committed synthetic raw segment: %v", err)
	}
	manifestBytes, err := json.Marshal(map[string]any{
		"format_version": 1,
		"object_key":     page.RawRecord.ObjectKey,
		"record_count":   1,
	})
	if err != nil {
		t.Fatal(err)
	}
	manifestHash := sha256.Sum256(manifestBytes)
	_, err = conn.Exec(t.Context(), `
INSERT INTO raw_segment_manifest (
    raw_segment_id, manifest_version, manifest_hash, manifest_bytes
) VALUES ($1, 1, $2, $3)
ON CONFLICT (raw_segment_id) DO NOTHING
`, page.RawRecord.RawSegmentID, manifestHash[:], manifestBytes)
	if err != nil {
		t.Fatalf("seed synthetic raw segment manifest: %v", err)
	}
	_, err = conn.Exec(t.Context(), `
INSERT INTO raw_record_evidence (
    raw_segment_id, arrival_ordinal, message_ordinal, envelope_version,
    request_id, payload_sha256, payload_byte_length
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (raw_segment_id, arrival_ordinal, message_ordinal) DO NOTHING
`, page.RawRecord.RawSegmentID, int64(page.RawRecord.ArrivalOrdinal), int32(page.RawRecord.MessageOrdinal),
		int16(page.RawRecord.EnvelopeVersion), page.Request.RequestID, page.RawSHA256[:], len(page.Raw))
	if err != nil {
		t.Fatalf("seed synthetic raw record evidence: %v", err)
	}
}

func newSpotPostgres(t *testing.T) *pgx.Conn {
	t.Helper()
	dsn := os.Getenv(spotPostgresDSNEnvironment)
	if dsn == "" {
		t.Skipf("%s is not set; integration test requires explicit PostgreSQL", spotPostgresDSNEnvironment)
	}
	admin, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect PostgreSQL: %v", err)
	}
	schema := fmt.Sprintf("binance_spot_catalog_%d_%d", os.Getpid(), spotSchemaSequence.Add(1))
	quotedSchema := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(t.Context(), "CREATE SCHEMA "+quotedSchema); err != nil {
		admin.Close(context.Background())
		t.Fatalf("create test schema: %v", err)
	}
	config, err := pgx.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if config.RuntimeParams == nil {
		config.RuntimeParams = make(map[string]string)
	}
	config.RuntimeParams["search_path"] = schema + ",public"
	conn, err := pgx.ConnectConfig(t.Context(), config)
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Migrate(t.Context(), conn); err != nil {
		conn.Close(context.Background())
		admin.Exec(context.Background(), "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close(context.Background())
		t.Fatalf("catalog.Migrate() error = %v", err)
	}
	t.Cleanup(func() {
		conn.Close(context.Background())
		admin.Exec(context.Background(), "DROP SCHEMA "+quotedSchema+" CASCADE")
		admin.Close(context.Background())
	})
	return conn
}

func assertCount(t *testing.T, conn *pgx.Conn, query string, want int, arguments ...any) {
	t.Helper()
	var got int
	if err := conn.QueryRow(t.Context(), query, arguments...).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("count for %q = %d, want %d", strings.TrimSpace(query), got, want)
	}
}

func catalogPayloadHash(raw []byte) [32]byte {
	return sha256.Sum256(raw)
}
