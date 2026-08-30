package collector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/segment"
)

func TestDurableRawSinkRestartRepublishesIdempotently(t *testing.T) {
	root := t.TempDir()
	clock, err := capture.NewManualClock(1_000_000, "collector-test-clock")
	if err != nil {
		t.Fatal(err)
	}
	publisher := newFakeSegmentPublisher()
	config := testSinkConfig(root)
	sink, err := NewDurableRawSink(t.Context(), config, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	epoch := testEpoch(capture.EpochConnection, 1)
	raw := []byte(`{"stream":"btcusdt@trade","data":{"p":"1"}}`)
	envelope := testRawEnvelope(t, epoch, binance.SpotRawChannel, 1, raw)
	if err := sink.WriteRaw(t.Context(), envelope); err != nil {
		t.Fatal(err)
	}
	close := testEpochClose(t, epoch, binance.SpotRawChannel, 1, capture.ClosePlanned, nil)
	if err := sink.Commit(t.Context(), capture.EpochCommit{SourceID: binance.SpotSourceID, Epoch: epoch, LastArrivalOrdinal: 1}); err != nil {
		t.Fatal(err)
	}
	if err := sink.CloseEpoch(t.Context(), close); err != nil {
		t.Fatal(err)
	}
	first := publisher.snapshot()
	if len(first) != 1 {
		t.Fatalf("first publication count = %d, want 1", len(first))
	}

	restarted, err := NewDurableRawSink(t.Context(), config, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	stats := restarted.Stats()
	if stats.RecoveredReady != 1 || stats.DuplicatePublications != 1 || stats.ActiveEpochs != 0 {
		t.Fatalf("restart stats = %#v", stats)
	}
	published := publisher.snapshot()
	if len(published) != 2 || published[0].request.SegmentID != published[1].request.SegmentID {
		t.Fatalf("publication identities = %#v", published)
	}

	spool, err := segment.OpenSpool(segment.SpoolConfig{
		Root: root, SourceID: binance.SpotSourceID, ChannelID: binance.SpotRawChannel,
		EpochKind: segment.EpochConnection, EpochID: epoch.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := spool.Ready()
	if err != nil || len(ready) != 1 {
		t.Fatalf("Ready() = %d, %v", len(ready), err)
	}
	var records []capture.EnvelopeV1
	_, err = spool.ReadReady(filepath.Base(ready[0].ManifestPath), func(record segment.Envelope) error {
		envelope, err := capture.EnvelopeV1FromSegment(record)
		if err == nil {
			records = append(records, envelope)
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || !bytes.Equal(records[0].RawPayload, raw) || records[1].ControlKind.Value != capture.ControlDisconnect {
		t.Fatalf("recovered records = %#v", records)
	}
}
func TestDurableRawSinkRestartCleansRecoveredPublication(t *testing.T) {
	root := t.TempDir()
	clock, _ := capture.NewManualClock(1_000_000, "collector-restart-cleanup-clock")
	publisher := newFakeSegmentPublisher()
	config := testSinkConfig(root)
	sink, err := NewDurableRawSink(t.Context(), config, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	epoch := testEpoch(capture.EpochConnection, 11)
	if err := sink.WriteRaw(t.Context(), testRawEnvelope(t, epoch, binance.SpotRawChannel, 1, []byte(`{"e":"trade"}`))); err != nil {
		t.Fatal(err)
	}
	if err := sink.CloseEpoch(t.Context(), testEpochClose(t, epoch, binance.SpotRawChannel, 1, capture.ClosePlanned, nil)); err != nil {
		t.Fatal(err)
	}
	config.CleanupPublished = true
	restarted, err := NewDurableRawSink(t.Context(), config, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	stats := restarted.Stats()
	if stats.RecoveredReady != 1 || stats.DuplicatePublications != 1 {
		t.Fatalf("restart cleanup stats = %#v", stats)
	}
	if _, err := os.Stat(filepath.Join(root, "elmd-segment-v1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered committed namespace remains or stat failed: %v", err)
	}
}

func TestDurableRawSinkReplaysFsyncedJournalAfterInterruptedWriter(t *testing.T) {
	root := t.TempDir()
	clock, _ := capture.NewManualClock(1_000_000, "collector-journal-clock")
	publisher := newFakeSegmentPublisher()
	config := testSinkConfig(root)
	sink, err := NewDurableRawSink(t.Context(), config, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	epoch := testEpoch(capture.EpochConnection, 9)
	raw := []byte(`{"stream":"btcusdt@bookTicker","data":{"u":9}}`)
	if err := sink.WriteRaw(t.Context(), testRawEnvelope(t, epoch, binance.SpotRawChannel, 1, raw)); err != nil {
		t.Fatal(err)
	}
	state := sink.activeEpochs[epochIdentity{kind: epoch.Kind, id: epoch.ID}]
	state.mu.Lock()
	if err := state.journal.file.Close(); err != nil {
		state.mu.Unlock()
		t.Fatal(err)
	}
	state.journal.file = nil
	state.mu.Unlock()

	restarted, err := NewDurableRawSink(t.Context(), config, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	stats := restarted.Stats()
	if stats.JournalReplays != 1 || stats.SegmentsPublished != 1 {
		t.Fatalf("journal recovery stats = %#v", stats)
	}
	publications := publisher.snapshot()
	if len(publications) != 1 {
		t.Fatalf("journal recovery publications = %d, want 1", len(publications))
	}
	spool, err := segment.OpenSpool(segment.SpoolConfig{
		Root: root, SourceID: binance.SpotSourceID, ChannelID: binance.SpotRawChannel,
		EpochKind: segment.EpochConnection, EpochID: epoch.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, err := spool.Ready()
	if err != nil || len(ready) != 1 {
		t.Fatalf("Ready() = %d, %v", len(ready), err)
	}
	var recovered []byte
	_, err = spool.ReadReady(filepath.Base(ready[0].ManifestPath), func(record segment.Envelope) error {
		envelope, err := capture.EnvelopeV1FromSegment(record)
		if err == nil {
			recovered = append([]byte(nil), envelope.RawPayload...)
		}
		return err
	})
	if err != nil || !bytes.Equal(recovered, raw) {
		t.Fatalf("recovered raw = %q, error = %v", recovered, err)
	}
}

func TestDurableRawSinkRecordsBlindDisconnectAndPublishesTerminal(t *testing.T) {
	root := t.TempDir()
	clock, _ := capture.NewManualClock(2_000_000, "collector-gap-clock")
	publisher := newFakeSegmentPublisher()
	sink, err := NewDurableRawSink(t.Context(), testSinkConfig(root), publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	gaps := newFakeGapRecorder()
	if err := sink.attachGapRecorder(t.Context(), gaps, nil); err != nil {
		t.Fatal(err)
	}
	epoch := testEpoch(capture.EpochConnection, 2)
	if err := sink.WriteRaw(t.Context(), testRawEnvelope(t, epoch, binance.SpotRawChannel, 1, []byte(`{"e":"trade"}`))); err != nil {
		t.Fatal(err)
	}
	blind := &capture.BlindInterval{StartedWallTimeNS: 1_000_000, DetectedWallTimeNS: 2_000_000, Reason: capture.CloseTransportFailure}
	if err := sink.Commit(t.Context(), capture.EpochCommit{SourceID: binance.SpotSourceID, Epoch: epoch, LastArrivalOrdinal: 1}); err != nil {
		t.Fatal(err)
	}
	if err := sink.CloseEpoch(t.Context(), testEpochClose(t, epoch, binance.SpotRawChannel, 1, capture.CloseTransportFailure, blind)); err != nil {
		t.Fatal(err)
	}
	stats := sink.Stats()
	if stats.Disconnects != 1 || stats.BlindIntervals != 1 || stats.EpochsClosed != 1 || stats.SegmentsPublished != 1 {
		t.Fatalf("sink stats = %#v", stats)
	}
}

func TestDurableRawSinkPersistsAndResolvesGapAfterDurableObservation(t *testing.T) {
	root := t.TempDir()
	clock, _ := capture.NewManualClock(2_000_000, "collector-gap-persistence-clock")
	gaps := newFakeGapRecorder()
	sink, err := NewDurableRawSink(t.Context(), testSinkConfig(root), newFakeSegmentPublisher(), clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.attachGapRecorder(t.Context(), gaps, nil); err != nil {
		t.Fatal(err)
	}
	epoch := testEpoch(capture.EpochConnection, 3)
	if err := sink.WriteRaw(t.Context(), testRawEnvelope(t, epoch, binance.SpotRawChannel, 1, []byte(`{"e":"trade"}`))); err != nil {
		t.Fatal(err)
	}
	blind := &capture.BlindInterval{StartedWallTimeNS: 1_000_000, DetectedWallTimeNS: 2_000_000, Reason: capture.CloseTransportFailure}
	if err := sink.CloseEpoch(t.Context(), testEpochClose(t, epoch, binance.SpotRawChannel, 1, capture.CloseTransportFailure, blind)); err != nil {
		t.Fatal(err)
	}
	observations, resolutions := gaps.snapshot()
	if len(observations) != 1 || len(resolutions) != 0 || observations[0].Interval != *blind {
		t.Fatalf("persisted gaps = %+v, resolutions = %+v", observations, resolutions)
	}
	next := testEpoch(capture.EpochConnection, 4)
	current := testRawEnvelope(t, next, binance.SpotRawChannel, 1, []byte(`{"e":"bookTicker"}`))
	current.ReceivedWallTimeNS = 3_000_000
	if err := sink.WriteRaw(t.Context(), current); err != nil {
		t.Fatal(err)
	}
	_, resolutions = gaps.snapshot()
	if len(resolutions) != 1 || resolutions["gap-1"] != current.ReceivedWallTimeNS {
		t.Fatalf("gap resolutions = %+v", resolutions)
	}
}

func TestDurableRawSinkResolvesRecoveredJournalObservation(t *testing.T) {
	root := t.TempDir()
	clock, _ := capture.NewManualClock(3_000_000, "collector-gap-restart-clock")
	publisher := newFakeSegmentPublisher()
	gaps := newFakeGapRecorder()
	gaps.open[newGapKey(binance.SpotRawChannel, "")] = "existing-gap"
	gaps.resolveErr = errors.New("synthetic resolution outage")
	config := testSinkConfig(root)
	sink, err := NewDurableRawSink(t.Context(), config, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.attachGapRecorder(t.Context(), gaps, nil); err != nil {
		t.Fatal(err)
	}
	epoch := testEpoch(capture.EpochConnection, 5)
	current := testRawEnvelope(t, epoch, binance.SpotRawChannel, 1, []byte(`{"e":"bookTicker"}`))
	if err := sink.WriteRaw(t.Context(), current); !errors.Is(err, ErrRawSink) {
		t.Fatalf("WriteRaw error = %v, want ErrRawSink", err)
	}
	state := sink.activeEpochs[epochIdentity{kind: epoch.Kind, id: epoch.ID}]
	state.mu.Lock()
	if err := state.journal.file.Close(); err != nil {
		state.mu.Unlock()
		t.Fatal(err)
	}
	state.journal.file = nil
	state.mu.Unlock()
	gaps.mu.Lock()
	gaps.resolveErr = nil
	gaps.mu.Unlock()

	restarted, err := NewDurableRawSink(t.Context(), config, publisher, clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.attachGapRecorder(t.Context(), gaps, nil); err != nil {
		t.Fatal(err)
	}
	_, resolutions := gaps.snapshot()
	if resolutions["existing-gap"] != current.ReceivedWallTimeNS {
		t.Fatalf("recovered gap resolutions = %+v", resolutions)
	}
}

func TestDurableRawSinkAccountsStartupBytesAgainstReservations(t *testing.T) {
	root := t.TempDir()
	const startupBytes = 4096
	if err := os.WriteFile(filepath.Join(root, "caller-owned-baseline"), make([]byte, startupBytes), 0o600); err != nil {
		t.Fatal(err)
	}
	config := testSinkConfig(root)
	reservation, ok := epochReservation(config)
	if !ok {
		t.Fatal("test configuration has no bounded epoch reservation")
	}
	config.MaxBytes = int64(reservation + startupBytes)
	clock, _ := capture.NewManualClock(4_000_000, "collector-quota-clock")
	sink, err := NewDurableRawSink(t.Context(), config, newFakeSegmentPublisher(), clock)
	if err != nil {
		t.Fatal(err)
	}
	first := testEpoch(capture.EpochConnection, 6)
	if err := sink.WriteRaw(t.Context(), testRawEnvelope(t, first, binance.SpotRawChannel, 1, []byte(`{"e":"trade"}`))); err != nil {
		t.Fatal(err)
	}
	second := testEpoch(capture.EpochConnection, 7)
	if err := sink.WriteRaw(t.Context(), testRawEnvelope(t, second, binance.SpotRawChannel, 1, []byte(`{"e":"trade"}`))); !errors.Is(err, ErrRawSink) {
		t.Fatalf("second active epoch error = %v, want quota ErrRawSink", err)
	}
}

func TestDurableRawSinkCleanupPrunesOnlyCommittedNamespace(t *testing.T) {
	root := t.TempDir()
	sentinel := filepath.Join(root, "caller-owned-sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	config := testSinkConfig(root)
	config.CleanupPublished = true
	clock, _ := capture.NewManualClock(5_000_000, "collector-cleanup-clock")
	sink, err := NewDurableRawSink(t.Context(), config, newFakeSegmentPublisher(), clock)
	if err != nil {
		t.Fatal(err)
	}
	epoch := testEpoch(capture.EpochConnection, 8)
	if err := sink.WriteRaw(t.Context(), testRawEnvelope(t, epoch, binance.SpotRawChannel, 1, []byte(`{"e":"trade"}`))); err != nil {
		t.Fatal(err)
	}
	if err := sink.CloseEpoch(t.Context(), testEpochClose(t, epoch, binance.SpotRawChannel, 1, capture.ClosePlanned, nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(sentinel); err != nil {
		t.Fatalf("caller-owned file was pruned: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "elmd-segment-v1")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("committed spool namespace remains or stat failed: %v", err)
	}
}

func TestRunWebSocketForcesCanceledEpochCloseAfterConnectBoundary(t *testing.T) {
	root := t.TempDir()
	clock := newBlockingTestClock(t)
	sink, err := NewDurableRawSink(t.Context(), testSinkConfig(root), newFakeSegmentPublisher(), clock)
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.attachGapRecorder(t.Context(), newFakeGapRecorder(), nil); err != nil {
		t.Fatal(err)
	}
	rate, err := binance.NewSpotVenueRateBudget(0)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(t.Context())
	connector := &blockingSpotConnector{readStarted: make(chan struct{}), cancelOnConnect: cancel}
	runtime := &Runtime{
		config: Config{
			Source:          SourceConfig{WebSocketEndpoint: binance.SpotWSEndpoint},
			Symbols:         []SymbolConfig{{NativeID: "BTCUSDT"}},
			RecorderVersion: "collector-test-v1",
			ReconnectDelay:  time.Second,
		},
		dependencies: Dependencies{
			WSConnector: connector,
			Clock:       clock,
			RateBudget:  rate,
			Epochs:      &counterEpochSource{},
		},
		sink: sink,
	}
	if err := runtime.runWebSocket(ctx); err != nil {
		t.Fatal(err)
	}
	connector.mu.Lock()
	reasons := slices.Clone(connector.closeReasons)
	connector.mu.Unlock()
	stats := sink.Stats()
	if !slices.Equal(reasons, []capture.CloseReason{capture.CloseCanceled}) ||
		stats.ActiveEpochs != 0 || stats.EpochsClosed != 1 || stats.Disconnects != 1 || stats.BlindIntervals != 1 {
		t.Fatalf("forced close reasons = %v, sink stats = %#v", reasons, stats)
	}
}
func TestRemovePublishedReadyRejectsPathsOutsideSpoolRoot(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	segmentPath := filepath.Join(outside, "segment.emseg.zst")
	manifestPath := filepath.Join(outside, "manifest.ready.json")
	if err := os.WriteFile(segmentPath, []byte("segment"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, []byte("manifest"), 0o600); err != nil {
		t.Fatal(err)
	}
	ready := segment.ReadySegment{SegmentPath: segmentPath, ManifestPath: manifestPath}
	ready.Manifest.SegmentFile = filepath.Base(segmentPath)
	if err := removePublishedReady(root, ready); err == nil {
		t.Fatal("out-of-root committed paths were accepted")
	}
	if _, err := os.Stat(segmentPath); err != nil {
		t.Fatalf("out-of-root segment was removed: %v", err)
	}
	if _, err := os.Stat(manifestPath); err != nil {
		t.Fatalf("out-of-root manifest was removed: %v", err)
	}
}

func TestFinishPublishedCleanupCompletesInterruptedPairDeletion(t *testing.T) {
	readyDir := filepath.Join(t.TempDir(), "ready")
	if err := os.MkdirAll(readyDir, 0o700); err != nil {
		t.Fatal(err)
	}
	manifestName := "manifest=synthetic.ready.json"
	segmentName := "segment=synthetic.emseg.zst"
	manifestPath := filepath.Join(readyDir, manifestName)
	segmentPath := filepath.Join(readyDir, segmentName)
	markerPath := filepath.Join(readyDir, publishedCleanupPrefix+"synthetic.json")
	if err := os.WriteFile(manifestPath, []byte("manifest"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(segmentPath, []byte("segment"), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := []byte(`{"manifest":"manifest=synthetic.ready.json","segment":"segment=synthetic.emseg.zst"}`)
	if err := os.WriteFile(markerPath, marker, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := finishPublishedCleanup(readyDir); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{manifestPath, segmentPath, markerPath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("interrupted cleanup path remains or stat failed for %s: %v", filepath.Base(path), err)
		}
	}
}

func TestRuntimeCancellationStopsEveryGoroutineAfterCommittedMetadata(t *testing.T) {
	root := t.TempDir()
	clock := newBlockingTestClock(t)
	publisher := newFakeSegmentPublisher()
	connector := &blockingSpotConnector{readStarted: make(chan struct{})}
	catalogStore := &fakeCatalogSyncer{}
	evidence := &fakeEvidenceRecorder{}
	rate, err := binance.NewSpotVenueRateBudget(0)
	if err != nil {
		t.Fatal(err)
	}
	metadataBody, err := os.ReadFile("../testdata/binance/exchange-info-page-0.json")
	if err != nil {
		t.Fatal(err)
	}
	httpClient := &http.Client{
		Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
			if request.URL.Query().Get("symbols") != `["BTCUSDT"]` {
				return nil, errors.New("metadata request omitted its exact configured symbol scope")
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Body:       io.NopCloser(bytes.NewReader(metadataBody)),
			}, nil
		}),
		Timeout: time.Second,
	}
	runtime, err := New(t.Context(), Config{
		Source: SourceConfig{
			WebSocketEndpoint: binance.SpotWSEndpoint, PublicRESTEndpoint: binance.SpotPublicRESTEndpoint,
			ExchangeInfoEndpoint: SpotExchangeInfoEndpoint, ExchangeInfoMaxBytes: 1 << 20,
		},
		Symbols:         []SymbolConfig{{NativeID: "BTCUSDT", DepthCadence: time.Hour}},
		DepthLimit:      100,
		RecorderVersion: "collector-test-v1",
		ReconnectDelay:  time.Second,
		Spool:           testSinkConfig(root),
	}, Dependencies{
		WSConnector: connector,
		RESTClient: spotRESTFunc(func(context.Context, binance.SpotDepthRequest, uint32) (binance.SpotRESTResponse, error) {
			return binance.SpotRESTResponse{Status: http.StatusOK, Body: []byte(`{"lastUpdateId":1,"bids":[],"asks":[]}`)}, nil
		}),
		MetadataHTTPClient: httpClient,
		Clock:              clock,
		RateBudget:         rate,
		Publisher:          publisher,
		Publications:       publisher,
		Evidence:           evidence,
		Catalog:            catalogStore,
		Epochs:             &counterEpochSource{},
		Gaps:               newFakeGapRecorder(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.SyncMetadata(t.Context()); err != nil {
		t.Fatal(err)
	}
	if runtime.Stats().MetadataSyncs != 1 || catalogStore.calls() != 1 || evidence.calls() != 1 {
		t.Fatalf("one-shot metadata sync stats = %#v, catalog calls = %d, evidence calls = %d", runtime.Stats(), catalogStore.calls(), evidence.calls())
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx) }()
	select {
	case <-connector.readStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("WebSocket read did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run() did not terminate after cancellation")
	}
	stats := runtime.Stats()
	if stats.ActiveGoroutines != 0 || stats.MetadataSyncs != 2 || catalogStore.calls() != 2 || evidence.calls() != 2 ||
		stats.Sink.ActiveEpochs != 0 || stats.Sink.EpochsClosed < 4 ||
		stats.Sink.Disconnects == 0 || stats.Sink.BlindIntervals == 0 {
		t.Fatalf("runtime stats = %#v, catalog calls = %d, evidence calls = %d", stats, catalogStore.calls(), evidence.calls())
	}
	if catalogStore.last.Pages[0].RawRecord.EvidenceScope != catalog.RawEvidenceCommitted {
		t.Fatalf("metadata evidence scope = %q", catalogStore.last.Pages[0].RawRecord.EvidenceScope)
	}
}

func TestRuntimeSyncMetadataGuards(t *testing.T) {
	var missing *Runtime
	if err := missing.SyncMetadata(t.Context()); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("nil SyncMetadata error = %v", err)
	}
	runtime := &Runtime{}
	runtime.running.Store(true)
	if err := runtime.SyncMetadata(t.Context()); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("concurrent SyncMetadata error = %v", err)
	}
	runtime.running.Store(false)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := runtime.SyncMetadata(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled SyncMetadata error = %v", err)
	}
}

func testSinkConfig(root string) RawSinkConfig {
	return RawSinkConfig{
		Root: root, FrameBytes: segment.DefaultFrameBytes, SegmentBytes: segment.DefaultSegmentBytes,
		SegmentMaxAge: segment.DefaultSegmentAge, MaxBytes: 2 << 30, WriterVersion: "collector-test-v1",
	}
}

func testEpoch(kind capture.EpochKind, marker byte) capture.StreamEpoch {
	var id [16]byte
	id[0] = marker
	return capture.StreamEpoch{Kind: kind, ID: id}
}

func testRawEnvelope(t *testing.T, epoch capture.StreamEpoch, channel string, ordinal uint64, raw []byte) capture.EnvelopeV1 {
	t.Helper()
	envelope := capture.EnvelopeV1{
		EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket,
		SourceID: binance.SpotSourceID, ChannelOrEndpoint: channel,
		ConnectionEpoch: capture.OptionalEpoch{Value: epoch.ID, Valid: true}, ArrivalOrdinal: ordinal,
		ReceivedWallTimeNS: int64(ordinal) * 1_000_000, ClockEpochID: "collector-test-clock",
		MonotonicNSSinceClockEpoch: ordinal, PayloadEncoding: capture.PayloadEncodingJSON,
		TerminalOutcome: capture.TerminalObserved, RecorderVersion: "collector-test-v1",
	}
	envelope.SetRawPayload(raw)
	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	return envelope
}

func testEpochClose(t *testing.T, epoch capture.StreamEpoch, channel string, last uint64, reason capture.CloseReason, blind *capture.BlindInterval) capture.EpochClose {
	t.Helper()
	outcome := capture.TerminalObserved
	if reason != capture.ClosePlanned {
		outcome = capture.TerminalDisconnected
	}
	terminal, err := capture.NewControlRecord(capture.ControlDisconnect, capture.EnvelopeV1{
		SourceID: binance.SpotSourceID, ChannelOrEndpoint: channel,
		ConnectionEpoch: capture.OptionalEpoch{Value: epoch.ID, Valid: true}, ArrivalOrdinal: last + 1,
		ReceivedWallTimeNS: int64(last+1) * 1_000_000, ClockEpochID: "collector-test-clock",
		MonotonicNSSinceClockEpoch: last + 1, PayloadEncoding: capture.PayloadEncodingNone,
		TerminalOutcome: outcome, RecorderVersion: "collector-test-v1",
	})
	if err != nil {
		t.Fatal(err)
	}
	return capture.EpochClose{
		Commit:   capture.EpochCommit{SourceID: binance.SpotSourceID, Epoch: epoch, LastArrivalOrdinal: last + 1},
		Terminal: terminal, Reason: reason, BlindInterval: blind,
	}
}

type fakePublication struct {
	request objectstore.PublishRequest
	result  objectstore.PublishResult
}

type fakeSegmentPublisher struct {
	mu           sync.Mutex
	identities   map[string]struct{}
	records      map[string]catalog.RawSegmentPublication
	publications []fakePublication
}

func newFakeSegmentPublisher() *fakeSegmentPublisher {
	return &fakeSegmentPublisher{
		identities: make(map[string]struct{}),
		records:    make(map[string]catalog.RawSegmentPublication),
	}
}

func (p *fakeSegmentPublisher) Publish(_ context.Context, request objectstore.PublishRequest) (objectstore.PublishResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, duplicate := p.identities[request.SegmentID]
	p.identities[request.SegmentID] = struct{}{}
	result := objectstore.PublishResult{
		Object:    objectstore.ObjectInfo{Key: request.Ready.Manifest.ObjectKey, Size: int64(request.Ready.Manifest.Segment.CompressedBytes)},
		Recovered: duplicate, State: catalog.RawSegmentCommitted,
	}
	p.publications = append(p.publications, fakePublication{request: request, result: result})
	ready := request.Ready
	p.records[ready.Manifest.ObjectKey] = catalog.RawSegmentPublication{
		SegmentID:       request.SegmentID,
		SourceID:        ready.Manifest.SourceID,
		ChannelID:       ready.Manifest.ChannelID,
		EpochID:         ready.Manifest.EpochID,
		ReceivedStartNS: ready.Manifest.Segment.FirstReceivedAtNS,
		ReceivedEndNS:   ready.Manifest.Segment.LastReceivedAtNS,
		OrdinalStart:    ready.Manifest.Segment.FirstOrdinal,
		OrdinalEnd:      ready.Manifest.Segment.LastOrdinal,
		ObjectKey:       ready.Manifest.ObjectKey,
		ContentSHA256:   ready.Manifest.Segment.CompressedSHA256,
		ByteLength:      int64(ready.Manifest.Segment.CompressedBytes),
		ManifestVersion: ready.Manifest.ManifestVersion,
		ManifestSHA256:  ready.ManifestSHA256,
		State:           catalog.RawSegmentCommitted,
	}
	return result, nil
}

func (p *fakeSegmentPublisher) snapshot() []fakePublication {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.publications)
}

func (p *fakeSegmentPublisher) FindRawSegment(_ context.Context, objectKey string) (catalog.RawSegmentPublication, bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	record, found := p.records[objectKey]
	return record, found, nil
}

type fakeGapRecorder struct {
	mu           sync.Mutex
	open         map[gapKey]string
	observations []GapObservation
	resolutions  map[string]int64
	resolveErr   error
}

func newFakeGapRecorder() *fakeGapRecorder {
	return &fakeGapRecorder{open: make(map[gapKey]string), resolutions: make(map[string]int64)}
}

func (r *fakeGapRecorder) OpenGap(_ context.Context, _ string, channel, native string) (string, bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id, found := r.open[newGapKey(channel, native)]
	return id, found, nil
}

func (r *fakeGapRecorder) RecordGap(_ context.Context, observation GapObservation) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	id := "gap-" + strconv.Itoa(len(r.observations)+1)
	r.observations = append(r.observations, observation)
	r.open[newGapKey(observation.ChannelID, observation.NativeSymbol)] = id
	return id, nil
}

func (r *fakeGapRecorder) ResolveGap(_ context.Context, id string, resolvedNS int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.resolveErr != nil {
		return r.resolveErr
	}
	r.resolutions[id] = resolvedNS
	for key, openID := range r.open {
		if openID == id {
			delete(r.open, key)
		}
	}
	return nil
}

func (r *fakeGapRecorder) snapshot() ([]GapObservation, map[string]int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	observations := slices.Clone(r.observations)
	resolutions := make(map[string]int64, len(r.resolutions))
	for id, resolvedNS := range r.resolutions {
		resolutions[id] = resolvedNS
	}
	return observations, resolutions
}

type blockingTestClock struct{ *capture.ManualClock }

func newBlockingTestClock(t *testing.T) *blockingTestClock {
	t.Helper()
	clock, err := capture.NewManualClock(1_000_000, "collector-runtime-clock")
	if err != nil {
		t.Fatal(err)
	}
	return &blockingTestClock{ManualClock: clock}
}

func (c *blockingTestClock) WaitUntil(ctx context.Context, _ uint64) error {
	<-ctx.Done()
	return ctx.Err()
}

type counterEpochSource struct {
	mu   sync.Mutex
	next byte
}

func (s *counterEpochSource) NewEpoch(kind capture.EpochKind) (capture.StreamEpoch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	return testEpoch(kind, s.next), nil
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

type spotRESTFunc func(context.Context, binance.SpotDepthRequest, uint32) (binance.SpotRESTResponse, error)

func (f spotRESTFunc) Do(ctx context.Context, request binance.SpotDepthRequest, maximum uint32) (binance.SpotRESTResponse, error) {
	return f(ctx, request, maximum)
}

type blockingSpotConnector struct {
	readStarted     chan struct{}
	once            sync.Once
	cancelOnConnect context.CancelFunc
	mu              sync.Mutex
	closeReasons    []capture.CloseReason
}

func (c *blockingSpotConnector) Connect(context.Context, binance.SpotWSConnectRequest) (binance.SpotWSConnection, error) {
	if c.cancelOnConnect != nil {
		c.cancelOnConnect()
	}
	return &blockingSpotConnection{owner: c}, nil
}

type blockingSpotConnection struct{ owner *blockingSpotConnector }

func (c *blockingSpotConnection) Read(ctx context.Context, _ uint32) (binance.SpotWSFrame, error) {
	c.owner.once.Do(func() { close(c.owner.readStarted) })
	<-ctx.Done()
	return binance.SpotWSFrame{}, ctx.Err()
}

func (*blockingSpotConnection) ReadBuffered(context.Context, uint32) (binance.SpotWSFrame, bool, error) {
	return binance.SpotWSFrame{}, false, nil
}

func (*blockingSpotConnection) Write(context.Context, binance.SpotWSWriteKind, []byte) error {
	return nil
}
func (c *blockingSpotConnection) Close(_ context.Context, reason capture.CloseReason) error {
	c.owner.mu.Lock()
	c.owner.closeReasons = append(c.owner.closeReasons, reason)
	c.owner.mu.Unlock()
	return nil
}

type fakeEvidenceRecorder struct {
	mu    sync.Mutex
	count int
}

func (r *fakeEvidenceRecorder) RecordCommittedRESTEvidence(_ context.Context, publication catalog.RawSegmentPublication, request, response capture.EnvelopeV1) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if publication.State != catalog.RawSegmentCommitted ||
		request.ArrivalOrdinal < publication.OrdinalStart ||
		request.ArrivalOrdinal > publication.OrdinalEnd ||
		response.ArrivalOrdinal < publication.OrdinalStart ||
		response.ArrivalOrdinal > publication.OrdinalEnd ||
		!request.SubscriptionOrRequestID.Valid ||
		request.SubscriptionOrRequestID != response.SubscriptionOrRequestID ||
		response.RawPayloadSHA256 != capture.PayloadHash(response.RawPayload) {
		return errors.New("invalid committed REST evidence")
	}
	r.count++
	return nil
}

func (r *fakeEvidenceRecorder) calls() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.count
}

type fakeCatalogSyncer struct {
	mu    sync.Mutex
	count int
	last  catalog.SyncInput
}

func (s *fakeCatalogSyncer) Sync(_ context.Context, input catalog.SyncInput) (catalog.Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if input.Pages[0].RawRecord.EvidenceScope != catalog.RawEvidenceCommitted {
		return catalog.Snapshot{}, errors.New("catalog sync did not receive committed evidence")
	}
	s.count++
	s.last = input
	return catalog.Snapshot{}, nil
}

func (s *fakeCatalogSyncer) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.count
}

var _ Clock = (*blockingTestClock)(nil)

func TestDeterministicSegmentIDUsesManifestIdentity(t *testing.T) {
	ready := segment.ReadySegment{ManifestSHA256: sha256.Sum256([]byte("manifest"))}
	ready.Manifest.Segment.CompressedSHA256 = sha256.Sum256([]byte("segment"))
	first := deterministicSegmentID(ready)
	second := deterministicSegmentID(ready)
	if first != second || len(first) != 36 {
		t.Fatalf("deterministic IDs = %q, %q", first, second)
	}
}
