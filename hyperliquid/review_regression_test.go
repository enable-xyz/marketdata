package hyperliquid

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/enable-xyz/marketdata/normalize"
)

func TestFixtureVerifierRejectsUndeclaredTreeAndProvenance(t *testing.T) {
	t.Run("undeclared regular file", func(t *testing.T) {
		root := copyFixtureRoot(t)
		if err := os.WriteFile(filepath.Join(root, "undeclared.json"), []byte(`{}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
			t.Fatalf("VerifyFixtures(undeclared file) error = %v", err)
		}
	})
	t.Run("undeclared directory", func(t *testing.T) {
		root := copyFixtureRoot(t)
		if err := os.Mkdir(filepath.Join(root, "undeclared"), 0o700); err != nil {
			t.Fatal(err)
		}
		if _, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
			t.Fatalf("VerifyFixtures(undeclared directory) error = %v", err)
		}
	})
	t.Run("undeclared symlink", func(t *testing.T) {
		root := copyFixtureRoot(t)
		if err := os.Symlink("perp_dexs.json", filepath.Join(root, "undeclared-link.json")); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
			t.Fatalf("VerifyFixtures(undeclared symlink) error = %v", err)
		}
	})
	for _, field := range []string{"source_url", "source_section", "derived_from"} {
		t.Run(field, func(t *testing.T) {
			root := copyFixtureRoot(t)
			var manifest map[string]any
			manifestBytes, err := os.ReadFile(filepath.Join(root, "manifest.json"))
			if err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
				t.Fatal(err)
			}
			entry := manifest["fixtures"].([]any)[0].(map[string]any)
			entry[field] = "https://example.invalid/arbitrary"
			mutated, err := json.Marshal(manifest)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, "manifest.json"), mutated, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := VerifyFixtures(root); !errors.Is(err, ErrFixtureBoundary) {
				t.Fatalf("VerifyFixtures(%s) error = %v", field, err)
			}
		})
	}
}

func TestParsedEconomicValuesAreEvidenceBound(t *testing.T) {
	dexs := testPerpDEXs(t)
	payload := testFixture(t, "hip3_meta_contexts.json")
	generation := testGenerationEvidence(payload, "binding", 1)
	metadata, err := ParsePerpMetadataAndContexts(Mainnet, dexs[1], generation, payload)
	if err != nil {
		t.Fatal(err)
	}
	mismatchedPair := generation
	mismatchedPair.ContextPayloadSHA256 = sha256.Sum256([]byte(`[]`))
	if _, err := ParsePerpMetadataAndContexts(Mainnet, dexs[1], mismatchedPair, payload); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("cross-payload context evidence accepted: %v", err)
	}
	mismatchedEnvelope := generation
	mismatchedEnvelope.EnvelopePayloadSHA256 = sha256.Sum256([]byte(`[{},[]]`))
	if _, err := ParsePerpMetadataAndContexts(Mainnet, dexs[1], mismatchedEnvelope, payload); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("unordered envelope evidence accepted: %v", err)
	}
	provenance := normalize.FieldProvenance{SourceTimeNS: normalize.OptionalInt64{Value: DocumentationAccessTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond, AgeNS: normalize.OptionalUint64{Valid: true}}

	for name, mutate := range map[string]func(*PerpDEX){
		"name":     func(d *PerpDEX) { d.Name = "mutated" },
		"deployer": func(d *PerpDEX) { d.Deployer = "0x9999999999999999999999999999999999999999" },
		"ordinal":  func(d *PerpDEX) { d.Index++ },
	} {
		t.Run("dex "+name, func(t *testing.T) {
			mutated := dexs[1]
			mutate(&mutated)
			if _, err := ParsePerpMetadataAndContexts(Mainnet, mutated, generation, payload); !errors.Is(err, ErrInvalidPayload) {
				t.Fatalf("mutated DEX accepted: %v", err)
			}
		})
	}

	trades, err := ParseTrades(testFixture(t, "trades_duplicate.json"))
	if err != nil {
		t.Fatal(err)
	}
	mutatedTrade := trades[0]
	mutatedTrade.Size = "999"
	if _, err := mutatedTrade.AmountValue(metadata.Universe[0].Identity, "BTC", provenance); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("mutated trade normalized: %v", err)
	}

	snapshot, err := ParseBookSnapshot(testBookEnvelope(t, testFixture(t, "book_slow_initial.json"), BookDepthContract{}))
	if err != nil {
		t.Fatal(err)
	}
	mutatedBook := snapshot
	mutatedBook.Bids = append([]BookLevel(nil), snapshot.Bids...)
	mutatedBook.Bids[0].Size = "999"
	if _, err := mutatedBook.AmountValues(metadata.Universe[0].Identity, "BTC", provenance); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("mutated book normalized: %v", err)
	}
	book, err := NewBook(metadata.Universe[0].Identity, BookDepthContract{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := book.Apply(mutatedBook); !errors.Is(err, ErrBookStreamMismatch) {
		t.Fatalf("mutated book applied: %v", err)
	}

	mutatedContext := metadata.Contexts[0]
	mutatedContext.OpenInterest.Text = "999"
	if _, err := mutatedContext.OpenInterestValue("BTC", provenance); !errors.Is(err, ErrInvalidPayload) {
		t.Fatalf("mutated context normalized: %v", err)
	}
}

func TestBookCaptureIdentityDoesNotInferDepth(t *testing.T) {
	payload := testFixture(t, "book_fast.json")
	slowEnvelope := testBookEnvelope(t, payload, BookDepthContract{})
	snapshot, err := ParseBookSnapshot(slowEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	slow, _ := slowEnvelope.Subscription()
	if snapshot.Depth.Fast || snapshot.CaptureIdentity().StreamIdentity() != slow.StreamIdentity() {
		t.Fatalf("book parser inferred depth from five levels: %+v", snapshot)
	}
	fastEnvelope := testBookEnvelope(t, payload, BookDepthContract{Fast: true})
	fastSnapshot, err := ParseBookSnapshot(fastEnvelope)
	fast, _ := fastEnvelope.Subscription()
	if err != nil || !fastSnapshot.Depth.Fast || fastSnapshot.CaptureIdentity().StreamIdentity() != fast.StreamIdentity() {
		t.Fatalf("fast receive identity not retained: %+v, %v", fastSnapshot, err)
	}
	if _, err := ParseBookSnapshot(ReceiveEnvelope{}); !errors.Is(err, ErrBookStreamMismatch) {
		t.Fatalf("zero/forged receive envelope accepted: %v", err)
	}
}

func TestPublicSocketBudgetsAndExactState(t *testing.T) {
	clock := &fakeMonotonicClock{now: 1}
	sharedMessages, err := NewWeightedLimiter(2000, time.Minute, clock)
	if err != nil {
		t.Fatal(err)
	}
	first := testSocketWithLimiter(t, MainPerpetual, "", clock, sharedMessages, &fakeSocketTransport{})
	second := testSocketWithLimiter(t, MainPerpetual, "", clock, sharedMessages, &fakeSocketTransport{})
	for range 1000 {
		if err := first.Ping(context.Background()); err != nil {
			t.Fatal(err)
		}
		if err := second.Ping(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if err := first.Ping(context.Background()); !errors.Is(err, ErrRateBudget) {
		t.Fatalf("shared 2001st outbound message accepted: %v", err)
	}

	connectionLimiter, err := NewWeightedLimiter(30, time.Minute, clock)
	if err != nil {
		t.Fatal(err)
	}
	dialClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("synthetic dial failure")
	}), Timeout: time.Second}
	if _, err := DialPublicSocket(context.Background(), Mainnet, MainPerpetual, "", dialClient, MaxRawPayloadBytes, sharedMessages, connectionLimiter); err == nil {
		t.Fatal("synthetic dial unexpectedly succeeded")
	}
	if remaining, err := connectionLimiter.Remaining(); err != nil || remaining != 29 {
		t.Fatalf("connection attempt was not reserved before dial: %d, %v", remaining, err)
	}

	pendingClock := &fakeMonotonicClock{now: 1}
	for range 29 {
		if _, err := DialPublicSocket(context.Background(), Mainnet, MainPerpetual, "", dialClient, MaxRawPayloadBytes, sharedMessages, connectionLimiter); err == nil {
			t.Fatal("synthetic dial unexpectedly succeeded")
		}
	}
	if _, err := DialPublicSocket(context.Background(), Mainnet, MainPerpetual, "", dialClient, MaxRawPayloadBytes, sharedMessages, connectionLimiter); !errors.Is(err, ErrRateBudget) {
		t.Fatalf("31st shared connection attempt accepted: %v", err)
	}
	pendingLimiter, err := NewWeightedLimiter(2000, time.Minute, pendingClock)
	if err != nil {
		t.Fatal(err)
	}
	pendingTransport := &fakeSocketTransport{}
	pendingSocket := testSocketWithLimiter(t, MainPerpetual, "", pendingClock, pendingLimiter, pendingTransport)
	subscriptions := make([]Subscription, MaxPendingACK)
	for index := range subscriptions {
		subscriptions[index] = Subscription{Type: SubscriptionTrades, Coin: fmt.Sprintf("C%d", index)}
	}
	if err := pendingSocket.ReconcileSubscriptions(context.Background(), subscriptions); err != nil {
		t.Fatal(err)
	}
	if err := pendingSocket.Subscribe(context.Background(), []Subscription{{Type: SubscriptionTrades, Coin: "OVER"}}); !errors.Is(err, ErrRateBudget) {
		t.Fatalf("101st pending ACK accepted: %v", err)
	}
	pendingClock.now += SubscriptionACKTimeoutNS
	if err := pendingSocket.Subscribe(context.Background(), []Subscription{{Type: SubscriptionTrades, Coin: "OVER"}}); err != nil {
		t.Fatalf("expired pending budget was not freed: %v", err)
	}
	timeouts := pendingSocket.DrainSubscriptionTimeouts()
	if len(timeouts) != MaxPendingACK || timeouts[0].DeadlineNS-timeouts[0].SentAtNS != SubscriptionACKTimeoutNS {
		t.Fatalf("timeout evidence = %+v", timeouts)
	}

	failingTransport := &fakeSocketTransport{writeErr: errors.New("synthetic write failure")}
	failingSocket := testSocket(t, MainPerpetual, "", &fakeMonotonicClock{now: 1}, 2000, failingTransport)
	if err := failingSocket.Subscribe(context.Background(), []Subscription{{Type: SubscriptionTrades, Coin: "BTC"}}); err == nil {
		t.Fatal("write failure unexpectedly succeeded")
	}
	if _, pending, _ := failingSocket.SubscriptionCounts(); pending != 0 {
		t.Fatalf("failed write retained %d pending operations", pending)
	}

	readClock := &fakeMonotonicClock{now: 1}
	readTransport := &fakeSocketTransport{}
	readSocket := testSocket(t, HIP3, "xyz", readClock, 2000, readTransport)
	slow := Subscription{Type: SubscriptionL2Book, Coin: "xyz:BTC"}
	fast := Subscription{Type: SubscriptionL2Book, Coin: "xyz:BTC", Book: BookDepthContract{Fast: true}}
	if err := readSocket.Subscribe(context.Background(), []Subscription{slow}); err != nil {
		t.Fatal(err)
	}
	if err := readSocket.HandleSubscriptionACK(testSubscriptionACK(t, HIP3, "xyz", "subscribe", slow)); err != nil {
		t.Fatal(err)
	}
	readTransport.readPayload = testFixture(t, "book_slow_initial.json")
	envelope, err := readSocket.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := readSocket.Unsubscribe(context.Background(), []Subscription{slow}); err != nil {
		t.Fatal(err)
	}
	if err := readSocket.HandleSubscriptionACK(testSubscriptionACK(t, HIP3, "xyz", "unsubscribe", slow)); err != nil {
		t.Fatal(err)
	}
	if err := readSocket.Subscribe(context.Background(), []Subscription{fast}); !errors.Is(err, ErrBookStreamMismatch) {
		t.Fatalf("same-socket slow-to-fast resubscribe accepted: %v", err)
	}
	readSocket.ResetForReconnect()
	if err := readSocket.ReconcileSubscriptions(context.Background(), []Subscription{fast}); !errors.Is(err, ErrBookStreamMismatch) {
		t.Fatalf("reconnect cleared lifetime book binding: %v", err)
	}
	snapshot, err := ParseBookSnapshot(envelope)
	if err != nil || snapshot.Depth.Fast {
		t.Fatalf("queued slow envelope lost its capture identity: %+v, %v", snapshot, err)
	}

	blockedTransport := &fakeSocketTransport{
		readPayload: testFixture(t, "book_slow_initial.json"),
		readStarted: make(chan struct{}),
		readRelease: make(chan struct{}),
	}
	blockedSocket := testSocket(t, HIP3, "xyz", readClock, 2000, blockedTransport)
	if err := blockedSocket.Subscribe(context.Background(), []Subscription{slow}); err != nil {
		t.Fatal(err)
	}
	if err := blockedSocket.HandleSubscriptionACK(testSubscriptionACK(t, HIP3, "xyz", "subscribe", slow)); err != nil {
		t.Fatal(err)
	}
	type readResult struct {
		envelope ReceiveEnvelope
		err      error
	}
	readResults := make(chan readResult, 1)
	go func() {
		blockedEnvelope, blockedErr := blockedSocket.Read(context.Background())
		readResults <- readResult{envelope: blockedEnvelope, err: blockedErr}
	}()
	released := false
	defer func() {
		if !released {
			close(blockedTransport.readRelease)
		}
	}()
	select {
	case <-blockedTransport.readStarted:
	case <-time.After(time.Second):
		t.Fatal("socket read did not reach blocking transport")
	}
	trades := Subscription{Type: SubscriptionTrades, Coin: "xyz:BTC"}
	subscribeResults := make(chan error, 1)
	go func() {
		subscribeResults <- blockedSocket.Subscribe(context.Background(), []Subscription{trades})
	}()
	select {
	case err := <-subscribeResults:
		if err != nil {
			t.Fatalf("subscribe failed during blocked read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked read held socket state lock during subscribe")
	}
	pingResults := make(chan error, 1)
	go func() { pingResults <- blockedSocket.Ping(context.Background()) }()
	select {
	case err := <-pingResults:
		if err != nil {
			t.Fatalf("ping failed during blocked read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked read held socket state lock during ping")
	}
	ackResults := make(chan error, 1)
	go func() {
		ackResults <- blockedSocket.HandleSubscriptionACK(testSubscriptionACK(t, HIP3, "xyz", "subscribe", trades))
	}()
	select {
	case err := <-ackResults:
		if err != nil {
			t.Fatalf("ACK failed during blocked read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked read held socket state lock during ACK")
	}
	closeResults := make(chan error, 1)
	go func() { closeResults <- blockedSocket.Close() }()
	select {
	case err := <-closeResults:
		if err != nil {
			t.Fatalf("close failed during blocked read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked read held socket state lock during close")
	}
	close(blockedTransport.readRelease)
	released = true
	result := <-readResults
	if !errors.Is(result.err, ErrInvalidSubscription) {
		t.Fatalf("read accepted payload from replaced/closed connection: %+v, %v", result.envelope, result.err)
	}
}

func TestInfoClientWeightedLimiter(t *testing.T) {
	fundingRows := bytes.Repeat([]byte(`{"coin":"BTC","fundingRate":"0.1","premium":"0.1","time":1},`), 21)
	fundingPayload := append([]byte(`[`), fundingRows...)
	fundingPayload[len(fundingPayload)-1] = ']'
	clock := &fakeMonotonicClock{now: 1}
	limiter, err := NewWeightedLimiter(20, time.Minute, clock)
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(bytes.NewReader(fundingPayload))}, nil
	}), Timeout: time.Second}
	info, err := NewInfoClient(Mainnet, client, MaxRawPayloadBytes, limiter)
	if err != nil {
		t.Fatal(err)
	}
	request, err := FundingHistoryRequest("BTC", 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := info.Do(context.Background(), request)
	if err != nil || evidence == nil {
		t.Fatalf("Info Do() = %v, %v", evidence, err)
	}
	if remaining, err := limiter.Remaining(); err != nil || remaining != -2 {
		t.Fatalf("response weight reconciliation remaining = %d, %v", remaining, err)
	}
	if _, err := info.Do(context.Background(), request); !errors.Is(err, ErrRateBudget) {
		t.Fatalf("debt did not block next request: %v", err)
	}
	clock.now += uint64(2 * time.Minute)
	if _, err := info.Do(context.Background(), request); err != nil {
		t.Fatalf("Info budget did not refill: %v", err)
	}

	failureClock := &fakeMonotonicClock{now: 1}
	failureLimiter, err := NewWeightedLimiter(40, time.Minute, failureClock)
	if err != nil {
		t.Fatal(err)
	}
	failureClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusTooManyRequests, Body: io.NopCloser(bytes.NewReader([]byte(`[]`)))}, nil
	}), Timeout: time.Second}
	failureInfo, err := NewInfoClient(Mainnet, failureClient, MaxRawPayloadBytes, failureLimiter)
	if err != nil {
		t.Fatal(err)
	}
	evidence, err = failureInfo.Do(context.Background(), request)
	if evidence == nil || err == nil {
		t.Fatalf("failed response was not bounded and retained: %v, %v", evidence, err)
	}
	if remaining, err := failureLimiter.Remaining(); err != nil || remaining != 20 {
		t.Fatalf("failed response base weight remaining = %d, %v", remaining, err)
	}
	transportFailureClock := &fakeMonotonicClock{now: 1}
	transportFailureLimiter, err := NewWeightedLimiter(40, time.Minute, transportFailureClock)
	if err != nil {
		t.Fatal(err)
	}
	transportFailureClient := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("synthetic transport failure")
	}), Timeout: time.Second}
	transportFailureInfo, err := NewInfoClient(Mainnet, transportFailureClient, MaxRawPayloadBytes, transportFailureLimiter)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := transportFailureInfo.Do(context.Background(), request); err == nil {
		t.Fatal("transport failure unexpectedly succeeded")
	}
	if remaining, err := transportFailureLimiter.Remaining(); err != nil || remaining != 20 {
		t.Fatalf("transport failure base weight remaining = %d, %v", remaining, err)
	}
}

type fakeMonotonicClock struct{ now uint64 }

func (c *fakeMonotonicClock) NowMonotonicNS() uint64 { return c.now }

type fakeSocketTransport struct {
	writes      [][]byte
	readPayload []byte
	readStarted chan struct{}
	readRelease chan struct{}
	writeErr    error
}

func (t *fakeSocketTransport) Write(_ context.Context, _ websocket.MessageType, payload []byte) error {
	if t.writeErr != nil {
		return t.writeErr
	}
	t.writes = append(t.writes, append([]byte(nil), payload...))
	return nil
}
func (t *fakeSocketTransport) Read(ctx context.Context) (websocket.MessageType, []byte, error) {
	if len(t.readPayload) == 0 {
		return 0, nil, io.EOF
	}
	if t.readStarted != nil {
		select {
		case t.readStarted <- struct{}{}:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	if t.readRelease != nil {
		select {
		case <-t.readRelease:
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
	payload := append([]byte(nil), t.readPayload...)
	t.readPayload = nil
	return websocket.MessageText, payload, nil
}
func (*fakeSocketTransport) Close(websocket.StatusCode, string) error { return nil }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }

func testSocket(t *testing.T, family Family, dexName string, clock MonotonicClock, capacity uint32, transport *fakeSocketTransport) *PublicSocket {
	t.Helper()
	limiter, err := NewWeightedLimiter(capacity, time.Minute, clock)
	if err != nil {
		t.Fatal(err)
	}
	return testSocketWithLimiter(t, family, dexName, clock, limiter, transport)
}

func testSocketWithLimiter(t *testing.T, family Family, dexName string, clock MonotonicClock, limiter *WeightedLimiter, transport *fakeSocketTransport) *PublicSocket {
	t.Helper()
	socket, err := newPublicSocketWithTransport(Mainnet, family, dexName, "test", MaxRawPayloadBytes, transport, limiter, clock)
	if err != nil {
		t.Fatal(err)
	}
	return socket
}

func testSubscriptionACK(t *testing.T, family Family, dexName, method string, subscription Subscription) SubscriptionACK {
	t.Helper()
	encoded, err := encodeSubscriptionOperation(method, subscription)
	if err != nil {
		t.Fatal(err)
	}
	var data any
	if err := json.Unmarshal(encoded, &data); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{"channel": "subscriptionResponse", "data": data})
	if err != nil {
		t.Fatal(err)
	}
	ack, err := ParseSubscriptionACK(family, dexName, payload)
	if err != nil {
		t.Fatal(err)
	}
	return ack
}

func copyFixtureRoot(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	manifestBytes := testFixture(t, "manifest.json")
	var manifest FixtureManifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "manifest.json"), manifestBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, entry := range manifest.Fixtures {
		payload := testFixture(t, entry.File)
		path := filepath.Join(root, entry.File)
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, payload, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}
