package serve

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/enable-xyz/marketdata/warehouse"
)

var (
	x6TestOnce   sync.Once
	x6TestResult X6Fixture
	x6TestError  error
)

func measuredX6(t *testing.T) X6Fixture {
	t.Helper()
	x6TestOnce.Do(func() {
		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
		defer cancel()
		x6TestResult, x6TestError = RunX6(ctx)
	})
	if x6TestError != nil {
		t.Fatalf("RunX6: %v", x6TestError)
	}
	return x6TestResult
}

func frozenX6(t *testing.T) X6Fixture {
	t.Helper()
	file, err := os.Open("testdata/x6.json")
	if err != nil {
		t.Fatal(err)
	}
	fixture, decodeErr := DecodeX6Fixture(file)
	closeErr := file.Close()
	if decodeErr != nil {
		t.Fatal(decodeErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	return fixture
}

func TestAuthScopes(t *testing.T) {
	fixture := measuredX6(t)
	want := []Scope{ScopeCatalogRead, ScopeCoverageRead, ScopeQueryRead, ScopeReplayNative, ScopeReplayNormalized, ScopeMetricsRead}
	if len(fixture.Scopes) != len(want) {
		t.Fatalf("scope count = %d, want %d", len(fixture.Scopes), len(want))
	}
	for index, scope := range want {
		result := fixture.Scopes[index]
		if result.Scope != scope || result.AllowedStatus != http.StatusOK || result.DeniedStatus != http.StatusForbidden {
			t.Fatalf("scope result %#v", result)
		}
	}
}

func TestTLSRequired(t *testing.T) {
	config := DefaultConfig()
	config.TLSCertRef, config.TLSKeyRef, config.PagingKeyRef = "missing-cert", "missing-key", "missing-paging"
	config.Principals = []Principal{{ID: "fixture", TokenRef: "missing-token", Scopes: []Scope{ScopeCatalogRead}}}
	if _, err := New(t.Context(), config, x6SecretResolver{}, newX6State().dependencies()); !errors.Is(err, ErrConfiguration) {
		t.Fatalf("New missing TLS error = %v", err)
	}

	material, _, tokens, err := newX6Material()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(t.Context(), x6Config(), material, newX6State().dependencies())
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://fixture.invalid/health/live", nil)
	response := httptest.NewRecorder()
	server.http.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusUpgradeRequired {
		t.Fatalf("plaintext status = %d", response.Code)
	}
	var observable strings.Builder
	observable.WriteString(server.config.TLSCertRef)
	observable.WriteString(server.config.TLSKeyRef)
	observable.WriteString(server.config.PagingKeyRef)
	for _, principal := range server.config.Principals {
		observable.WriteString(principal.ID)
		observable.WriteString(principal.TokenRef)
	}
	for _, token := range tokens {
		if strings.Contains(observable.String(), token) || bytes.Contains(response.Body.Bytes(), []byte(token)) {
			t.Fatal("bearer material became observable")
		}
	}
}

func TestQueryBounds(t *testing.T) {
	config := DefaultConfig()
	base := QueryRequest{Family: "trade", SourceIDs: []string{"source-a"}, StartReceivedTimeNS: 1, EndReceivedTimeNS: 2}
	query, err := normalizeQueryRequest(base, config)
	if err != nil || query.Limit != DefaultPageRows {
		t.Fatalf("default limit = %d, error = %v", query.Limit, err)
	}
	base.Limit = MaximumPageRows
	if _, err := normalizeQueryRequest(base, config); err != nil {
		t.Fatalf("10k limit: %v", err)
	}
	base.Limit = MaximumPageRows + 1
	if _, err := normalizeQueryRequest(base, config); !errors.Is(err, ErrQueryRequest) {
		t.Fatalf("over-limit error = %v", err)
	}
	base.Limit = 1
	base.SourceIDs = make([]string, warehouse.MaximumQuerySources+1)
	for index := range base.SourceIDs {
		base.SourceIDs[index] = "source-" + string(rune(0x100+index))
	}
	if _, err := normalizeQueryRequest(base, config); !errors.Is(err, ErrQueryRequest) {
		t.Fatalf("source bound error = %v", err)
	}
	base.SourceIDs = []string{"source-a"}
	base.StartReceivedTimeNS = 1
	base.EndReceivedTimeNS = 1 + MaximumQueryInterval.Nanoseconds() + 1
	if _, err := normalizeQueryRequest(base, config); !errors.Is(err, ErrQueryRequest) {
		t.Fatalf("interval error = %v", err)
	}

	material, _, tokens, err := newX6Material()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(t.Context(), x6Config(), material, newX6State().dependencies())
	if err != nil {
		t.Fatal(err)
	}
	closedBody := []byte(`{"family":"trade","source_ids":["source-a"],"start_received_time_ns":1,"end_received_time_ns":2,"sql":"SELECT 1"}`)
	request := httptest.NewRequest(http.MethodPost, "https://fixture.invalid/v1/query", bytes.NewReader(closedBody))
	request.TLS = &tls.ConnectionState{}
	request.Header.Set("Authorization", "Bearer "+tokens[ScopeQueryRead])
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	server.http.Handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("arbitrary grammar status = %d", response.Code)
	}
	writeRequest := httptest.NewRequest(http.MethodPost, "https://fixture.invalid/v1/catalog/sources", nil)
	writeRequest.TLS = &tls.ConnectionState{}
	writeRequest.Header.Set("Authorization", "Bearer "+tokens[ScopeCatalogRead])
	writeResponse := httptest.NewRecorder()
	server.http.Handler.ServeHTTP(writeResponse, writeRequest)
	if writeResponse.Code != http.StatusMethodNotAllowed {
		t.Fatalf("write route status = %d", writeResponse.Code)
	}

	dataset := newX6State().dataset
	row := x6QueryRow(dataset, "source-a")
	digest := sha256.Sum256([]byte("bounded-query"))
	row.Price = strings.Repeat("1", int(MaximumResponseBytes)-(2<<10))
	body, err := server.encodeQueryPage(t.Context(), warehouse.Page{Dataset: dataset, Rows: []warehouse.QueryRow{row}}, digest, time.Unix(1, 0))
	if err != nil || int64(len(body)) > MaximumResponseBytes {
		t.Fatalf("near-cap response bytes = %d, error = %v", len(body), err)
	}
	row.Price = strings.Repeat("1", int(MaximumResponseBytes))
	if _, err := server.encodeQueryPage(t.Context(), warehouse.Page{Dataset: dataset, Rows: []warehouse.QueryRow{row}}, digest, time.Unix(1, 0)); !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("oversized row error = %v", err)
	}
}

func TestPageToken(t *testing.T) {
	key := make([]byte, sha256.Size)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("query-a"))
	mismatch := sha256.Sum256([]byte("query-b"))
	row := x6QueryRow(newX6State().dataset, "source-a")
	last, err := row.SortKey()
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	token, err := mintPageToken(key, digest, row.DatasetID, last, now.Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parsePageToken(token, key, digest, now)
	if err != nil || parsed.DatasetID != row.DatasetID || warehouse.CompareSortKey(parsed.LastKey, last) != 0 {
		t.Fatalf("page token round trip = %#v, %v", parsed, err)
	}
	tampered := []byte(token)
	tampered[len(tampered)/2] ^= 1
	if _, err := parsePageToken(string(tampered), key, digest, now); !errors.Is(err, ErrPageToken) {
		t.Fatalf("tamper error = %v", err)
	}
	if _, err := parsePageToken(token, key, mismatch, now); !errors.Is(err, ErrPageToken) {
		t.Fatalf("mismatch error = %v", err)
	}
	if _, err := parsePageToken(token, key, digest, now.Add(time.Minute)); !errors.Is(err, ErrPageToken) {
		t.Fatalf("expiry error = %v", err)
	}
}

func TestBackpressure(t *testing.T) {
	pressure := measuredX6(t).Pressure
	if pressure.SlowClientFastPeerStatus != http.StatusOK || pressure.SeparateQueryStatus != http.StatusOK ||
		pressure.QueueFullStatus != http.StatusServiceUnavailable || !pressure.CancellationObserved || !pressure.DisconnectObserved {
		t.Fatalf("pressure result = %#v", pressure)
	}
}

func TestReplayTransport(t *testing.T) {
	measured := measuredX6(t)
	if err := CompareX6Fixture(frozenX6(t), measured); err != nil {
		t.Fatal(err)
	}
	record := segment.Envelope{Kind: segment.RecordKindREST, SourceID: "source-a", ChannelOrEndpoint: "channel-a",
		ArrivalOrdinal: 1, MessageOrdinal: 1, ReceivedWallTimeNS: 1, ClockEpochID: "clock-a",
		PayloadEncoding: segment.PayloadEncodingJSON, RawPayload: []byte(`{"price":"1.0"}`),
		TerminalOutcome: segment.OutcomeObserved, RecorderVersion: "fixture-v1"}
	frame, err := replay.MarshalServiceEvent(replay.Event{Kind: replay.EventRecord, Record: record})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := segment.UnmarshalEnvelope(frame); err != nil {
		t.Fatalf("native frame: %v", err)
	}
	dataset := newX6State().dataset
	request := replay.ServiceRequest{DatasetID: dataset.IDString(), Family: dataset.Family, CatalogSnapshotID: dataset.CatalogSnapshotIDString(),
		SchemaName: dataset.SchemaName, SchemaVersion: dataset.SchemaVersion, SourceIDs: []string{"source-a"},
		StartReceivedTimeNS: 1, EndReceivedTimeNS: 3}
	gap := warehouse.GapReference{ID: "gap-a", Tuple: warehouse.Tuple{SourceID: "source-a", ChannelID: "channel-a", InstrumentUID: "instrument-a"},
		StartReceivedTimeNS: 1, EndReceivedTimeNS: 2, Kind: "disconnect"}
	item := replay.NormalizedItem{Version: replay.ServiceVersionV1, Type: replay.NormalizedGapKind, DatasetID: dataset.IDString(),
		CatalogSnapshotID: dataset.CatalogSnapshotIDString(), SchemaName: dataset.SchemaName, SchemaVersion: dataset.SchemaVersion, Gap: &gap}
	line, err := item.AppendNDJSON(nil, request)
	if err != nil || len(line) == 0 || line[len(line)-1] != '\n' || !bytes.Contains(line, []byte(`"type":"gap"`)) {
		t.Fatalf("normalized gap line = %q, %v", line, err)
	}
	recordItem := replay.NormalizedItem{Version: replay.ServiceVersionV1, Type: replay.NormalizedRecordKind,
		DatasetID: dataset.IDString(), CatalogSnapshotID: dataset.CatalogSnapshotIDString(),
		SchemaName: dataset.SchemaName, SchemaVersion: dataset.SchemaVersion,
		Record: ptr(x6QueryRow(dataset, "source-a"))}
	badFamily := recordItem
	badFamily.Record = ptr(*recordItem.Record)
	badFamily.Record.Family = "other"
	if _, err := badFamily.AppendNDJSON(nil, request); !errors.Is(err, replay.ErrInvalidServiceRequest) {
		t.Fatalf("normalized family mismatch error = %v", err)
	}

	material, _, tokens, err := newX6Material()
	if err != nil {
		t.Fatal(err)
	}
	server, err := New(t.Context(), x6Config(), material, newX6State().dependencies())
	if err != nil {
		t.Fatal(err)
	}
	smallFrame, err := x6Frame("source-a", 1)
	if err != nil {
		t.Fatal(err)
	}
	flushedFrame, err := x6Frame("source-a", 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	for name, frames := range map[string][][]byte{"sub-buffer": {smallFrame}, "flushed": {flushedFrame}} {
		t.Run("native-error-"+name, func(t *testing.T) {
			server.deps.Native = terminalNativeOpener{frames: frames}
			result := directReplayRequest(t, server, "/v1/replay/native", tokens[ScopeReplayNative])
			if result.status != http.StatusOK || len(result.body) != len(frames[0]) ||
				result.terminal != "source_error" || result.truncated != "" || !result.declared {
				t.Fatalf("native terminal result = %#v", result)
			}
			if name == "flushed" && len(result.body) <= server.config.NativeReplay.BufferBytes {
				t.Fatalf("native body did not flush: %d bytes", len(result.body))
			}
		})
	}
	normalizedCases := map[string][]replay.NormalizedItem{"sub-buffer": {recordItem}}
	recordLine, err := recordItem.AppendNDJSON(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	flushedItems := make([]replay.NormalizedItem, server.config.NormalizedReplay.BufferBytes/len(recordLine)+2)
	for index := range flushedItems {
		flushedItems[index] = recordItem
	}
	normalizedCases["flushed"] = flushedItems
	for name, items := range normalizedCases {
		t.Run("normalized-error-"+name, func(t *testing.T) {
			server.deps.Normalized = terminalNormalizedOpener{items: items}
			result := directReplayRequest(t, server, "/v1/replay/normalized", tokens[ScopeReplayNormalized])
			if result.status != http.StatusOK || len(result.body) == 0 ||
				result.terminal != "source_error" || result.truncated != "" || !result.declared {
				t.Fatalf("normalized terminal result = %#v", result)
			}
			if name == "flushed" && len(result.body) <= server.config.NormalizedReplay.BufferBytes {
				t.Fatalf("normalized body did not flush: %d bytes", len(result.body))
			}
		})
	}
}

func TestShutdown(t *testing.T) {
	pressure := measuredX6(t).Pressure
	if !pressure.BoundedShutdownObserved || pressure.RestartStatus != http.StatusOK {
		t.Fatalf("shutdown result = %#v", pressure)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	gate := newRequestGate(RouteLimits{QueueDepth: 1, Concurrency: 1})
	gate.workers <- struct{}{}
	gate.slots <- struct{}{}
	if _, err := gate.enter(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled queue error = %v", err)
	}

	t.Run("forced shutdown defers secret wipe", func(t *testing.T) {
		material, _, tokens, err := newX6Material()
		if err != nil {
			t.Fatal(err)
		}
		state := newX6State()
		pager := &blockingPager{dataset: state.dataset, entered: make(chan struct{}), release: make(chan struct{})}
		dependencies := state.dependencies()
		dependencies.Query = pager
		server, err := New(t.Context(), x6Config(), material, dependencies)
		if err != nil {
			t.Fatal(err)
		}
		privateKey, ok := server.config.certificate.PrivateKey.(ed25519.PrivateKey)
		if !ok {
			t.Fatalf("private key type = %T", server.config.certificate.PrivateKey)
		}
		certificateDER := server.config.certificate.Certificate[0]
		request := httptest.NewRequest(http.MethodPost, "https://x6.invalid/v1/query", bytes.NewReader(x6QueryBody("source-a", 1)))
		request.Header.Set("Authorization", "Bearer "+tokens[ScopeQueryRead])
		request.Header.Set("Content-Type", "application/json")
		recorder := httptest.NewRecorder()
		requestDone := make(chan struct{})
		go func() {
			server.http.Handler.ServeHTTP(recorder, request)
			close(requestDone)
		}()
		select {
		case <-pager.entered:
		case <-t.Context().Done():
			t.Fatal(context.Cause(t.Context()))
		}

		shutdownContext, cancelShutdown := context.WithCancel(context.Background())
		cancelShutdown()
		if err := server.Shutdown(shutdownContext); !errors.Is(err, context.Canceled) {
			t.Fatalf("forced shutdown error = %v", err)
		}
		if len(server.config.pagingKey) == 0 || server.config.certificate.PrivateKey == nil || bytesAllZero(privateKey) {
			t.Fatal("secrets wiped while handler was still running")
		}

		close(pager.release)
		select {
		case <-requestDone:
		case <-t.Context().Done():
			t.Fatal(context.Cause(t.Context()))
		}
		select {
		case <-server.wipeDone:
		case <-t.Context().Done():
			t.Fatal(context.Cause(t.Context()))
		}
		if recorder.Code != http.StatusOK {
			t.Fatalf("query status = %d", recorder.Code)
		}
		if server.config.pagingKey != nil || server.config.auth.principals != nil ||
			server.config.certificate.PrivateKey != nil || server.config.certificate.Certificate != nil ||
			len(server.http.TLSConfig.Certificates) != 0 || !bytesAllZero(privateKey) || !bytesAllZero(certificateDER) {
			t.Fatal("resolved TLS, authentication, or paging material retained after drain")
		}
	})
}

type terminalNativeOpener struct{ frames [][]byte }

func (o terminalNativeOpener) OpenNative(context.Context, replay.ServiceRequest) (replay.NativeStream, error) {
	return &terminalNativeStream{frames: o.frames}, nil
}

type terminalNativeStream struct {
	frames [][]byte
	next   int
}

func (s *terminalNativeStream) Next(ctx context.Context) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if s.next == len(s.frames) {
		return nil, errX6Terminal
	}
	frame := s.frames[s.next]
	s.next++
	return frame, nil
}

func (*terminalNativeStream) Close() error { return nil }

type terminalNormalizedOpener struct{ items []replay.NormalizedItem }

func (o terminalNormalizedOpener) OpenNormalized(context.Context, replay.ServiceRequest) (replay.NormalizedStream, error) {
	return &terminalNormalizedStream{items: o.items}, nil
}

type terminalNormalizedStream struct {
	items []replay.NormalizedItem
	next  int
}

func (s *terminalNormalizedStream) Next(ctx context.Context) (replay.NormalizedItem, error) {
	if err := ctx.Err(); err != nil {
		return replay.NormalizedItem{}, err
	}
	if s.next == len(s.items) {
		return replay.NormalizedItem{}, errX6Terminal
	}
	item := s.items[s.next]
	s.next++
	return item, nil
}

func (*terminalNormalizedStream) Close() error { return nil }

type directReplayResult struct {
	status    int
	body      []byte
	terminal  string
	truncated string
	declared  bool
}

func directReplayRequest(t *testing.T, server *Server, path, token string) directReplayResult {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "https://x6.invalid"+path, bytes.NewReader(x6ReplayBody("source-a")))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	recorder := httptest.NewRecorder()
	server.http.Handler.ServeHTTP(recorder, request)
	declared := strings.Contains(strings.Join(recorder.Header().Values("Trailer"), ","), ReplayTrailerTerminalError)
	response := recorder.Result()
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return directReplayResult{status: response.StatusCode, body: body, terminal: response.Trailer.Get(ReplayTrailerTerminalError),
		truncated: response.Trailer.Get(ReplayTrailerTruncated), declared: declared}
}

type blockingPager struct {
	dataset warehouse.Dataset
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (p *blockingPager) Page(context.Context, warehouse.QuerySpec) (warehouse.Page, error) {
	p.once.Do(func() { close(p.entered) })
	<-p.release
	return warehouse.Page{Dataset: p.dataset}, nil
}

func bytesAllZero(value []byte) bool {
	for _, current := range value {
		if current != 0 {
			return false
		}
	}
	return true
}
