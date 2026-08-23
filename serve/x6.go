package serve

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/enable-xyz/marketdata/warehouse"
)

const (
	X6FixtureVersion      = uint16(2)
	x6LongReplayFrames    = 64
	x6LongReplayWireBytes = 67_118_016
)

type X6Limits struct {
	QueryQueue            int   `json:"query_queue"`
	QueryConcurrency      int   `json:"query_concurrency"`
	QueryBufferBytes      int   `json:"query_buffer_bytes"`
	NativeQueue           int   `json:"native_queue"`
	NativeConcurrency     int   `json:"native_concurrency"`
	NativeBufferBytes     int   `json:"native_buffer_bytes"`
	NormalizedQueue       int   `json:"normalized_queue"`
	NormalizedConcurrency int   `json:"normalized_concurrency"`
	NormalizedBufferBytes int   `json:"normalized_buffer_bytes"`
	MaximumPageRows       int   `json:"maximum_page_rows"`
	MaximumResponseBytes  int64 `json:"maximum_response_bytes"`
	MaximumReplayBytes    int64 `json:"maximum_replay_bytes"`
	MaximumReplaySeconds  int64 `json:"maximum_replay_seconds"`
}

type X6ScopeResult struct {
	Scope         Scope `json:"scope"`
	AllowedStatus int   `json:"allowed_status"`
	DeniedStatus  int   `json:"denied_status"`
}

type X6WireResult struct {
	Status        int    `json:"status"`
	BodyBytes     int    `json:"body_bytes"`
	SHA256        string `json:"sha256"`
	Truncated     string `json:"truncated"`
	TerminalError string `json:"terminal_error"`
}

type X6ProtocolResult struct {
	Requested       string       `json:"requested"`
	Observed        string       `json:"observed"`
	Query           X6WireResult `json:"query"`
	Native          X6WireResult `json:"native"`
	Normalized      X6WireResult `json:"normalized"`
	NativeError     X6WireResult `json:"native_error"`
	NormalizedError X6WireResult `json:"normalized_error"`
}

type X6PayloadResult struct {
	PayloadBytes int `json:"payload_bytes"`
	FrameBytes   int `json:"frame_bytes"`
	Status       int `json:"status"`
}

type X6PressureResult struct {
	SlowClientFastPeerStatus int  `json:"slow_client_fast_peer_status"`
	SeparateQueryStatus      int  `json:"separate_query_status"`
	QueueFullStatus          int  `json:"queue_full_status"`
	LongReplayStatus         int  `json:"long_replay_status"`
	LongReplayBytes          int  `json:"long_replay_bytes"`
	CancellationObserved     bool `json:"cancellation_observed"`
	DisconnectObserved       bool `json:"disconnect_observed"`
	BoundedShutdownObserved  bool `json:"bounded_shutdown_observed"`
	RestartStatus            int  `json:"restart_status"`
}

type X6Fixture struct {
	Version      uint16             `json:"version"`
	Decision     string             `json:"decision"`
	HealthStatus int                `json:"health_status"`
	Limits       X6Limits           `json:"limits"`
	Scopes       []X6ScopeResult    `json:"scopes"`
	Protocols    []X6ProtocolResult `json:"protocols"`
	Payloads     []X6PayloadResult  `json:"payloads"`
	Pressure     X6PressureResult   `json:"pressure"`
}

func (f X6Fixture) ValidateReleaseGate() error {
	if f.Version != X6FixtureVersion || f.Decision != "tls_http" || f.HealthStatus != http.StatusOK {
		return fmt.Errorf("%w: X6 decision or health result", ErrConfiguration)
	}
	expectedLimits := selectedX6Limits()
	if f.Limits != expectedLimits {
		return fmt.Errorf("%w: X6 selected limits changed", ErrConfiguration)
	}
	expectedScopes := []Scope{ScopeCatalogRead, ScopeCoverageRead, ScopeQueryRead, ScopeReplayNative, ScopeReplayNormalized, ScopeMetricsRead}
	if len(f.Scopes) != len(expectedScopes) {
		return fmt.Errorf("%w: X6 scope matrix", ErrConfiguration)
	}
	for index, scope := range expectedScopes {
		if f.Scopes[index].Scope != scope || f.Scopes[index].AllowedStatus != http.StatusOK || f.Scopes[index].DeniedStatus != http.StatusForbidden {
			return fmt.Errorf("%w: X6 scope result", ErrConfiguration)
		}
	}
	if len(f.Protocols) != 2 || f.Protocols[0].Requested != "http/1.1" || f.Protocols[0].Observed != "HTTP/1.1" ||
		f.Protocols[1].Requested != "h2" || f.Protocols[1].Observed != "HTTP/2.0" {
		return fmt.Errorf("%w: X6 protocol negotiation result", ErrConfiguration)
	}
	for _, result := range f.Protocols {
		if !validX6WireResult(result.Query, "") || !validX6WireResult(result.Native, "") ||
			!validX6WireResult(result.Normalized, "") || !validX6WireResult(result.NativeError, "source_error") ||
			!validX6WireResult(result.NormalizedError, "source_error") {
			return fmt.Errorf("%w: X6 protocol wire result", ErrConfiguration)
		}
	}
	if f.Protocols[0].Query != f.Protocols[1].Query || f.Protocols[0].Native != f.Protocols[1].Native ||
		f.Protocols[0].Normalized != f.Protocols[1].Normalized || f.Protocols[0].NativeError != f.Protocols[1].NativeError ||
		f.Protocols[0].NormalizedError != f.Protocols[1].NormalizedError {
		return fmt.Errorf("%w: X6 protocol wire mismatch", ErrConfiguration)
	}
	expectedPayloads := []int{1, 1 << 10, 1 << 20, segment.MaxPayloadBytes}
	if len(f.Payloads) != len(expectedPayloads) {
		return fmt.Errorf("%w: X6 payload matrix", ErrConfiguration)
	}
	for index, size := range expectedPayloads {
		result := f.Payloads[index]
		if result.PayloadBytes != size || result.FrameBytes <= size || result.FrameBytes > segment.MaxRecordBytes || result.Status != http.StatusOK {
			return fmt.Errorf("%w: X6 payload result", ErrConfiguration)
		}
	}
	pressure := f.Pressure
	if pressure.SlowClientFastPeerStatus != http.StatusOK || pressure.SeparateQueryStatus != http.StatusOK ||
		pressure.QueueFullStatus != http.StatusServiceUnavailable || pressure.LongReplayStatus != http.StatusOK ||
		pressure.LongReplayBytes != x6LongReplayWireBytes || !pressure.CancellationObserved || !pressure.DisconnectObserved ||
		!pressure.BoundedShutdownObserved || pressure.RestartStatus != http.StatusOK {
		return fmt.Errorf("%w: X6 pressure or lifecycle result", ErrConfiguration)
	}
	return nil
}

func validX6WireResult(result X6WireResult, terminalError string) bool {
	if result.Status != http.StatusOK || result.BodyBytes <= 0 || result.Truncated != "" || result.TerminalError != terminalError {
		return false
	}
	digest, err := hex.DecodeString(result.SHA256)
	return err == nil && len(digest) == sha256.Size
}

func EncodeX6Fixture(writer io.Writer, fixture X6Fixture) error {
	if err := fixture.ValidateReleaseGate(); err != nil {
		return err
	}
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")
	return encoder.Encode(fixture)
}

func DecodeX6Fixture(reader io.Reader) (X6Fixture, error) {
	decoder := json.NewDecoder(io.LimitReader(reader, 1<<20))
	decoder.DisallowUnknownFields()
	var fixture X6Fixture
	if err := decoder.Decode(&fixture); err != nil {
		return X6Fixture{}, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return X6Fixture{}, fmt.Errorf("%w: trailing X6 fixture data", ErrConfiguration)
	}
	if err := fixture.ValidateReleaseGate(); err != nil {
		return X6Fixture{}, err
	}
	return fixture, nil
}

func CompareX6Fixture(expected, measured X6Fixture) error {
	if err := expected.ValidateReleaseGate(); err != nil {
		return err
	}
	if err := measured.ValidateReleaseGate(); err != nil {
		return err
	}
	expectedJSON, _ := json.Marshal(expected)
	measuredJSON, _ := json.Marshal(measured)
	if !bytes.Equal(expectedJSON, measuredJSON) {
		return fmt.Errorf("%w: measured X6 fixture differs", ErrConfiguration)
	}
	return nil
}

// RunX6 executes the synthetic in-process TLS matrix. It opens loopback
// listeners only, emits no address or certificate identity, and returns only
// deterministic contract measurements suitable for a checked-in JSON fixture.
func RunX6(ctx context.Context) (X6Fixture, error) {
	if ctx == nil {
		return X6Fixture{}, fmt.Errorf("%w: X6 caller deadline is required", ErrConfiguration)
	}
	if _, ok := ctx.Deadline(); !ok {
		return X6Fixture{}, fmt.Errorf("%w: X6 caller deadline is required", ErrConfiguration)
	}
	material, roots, tokens, err := newX6Material()
	if err != nil {
		return X6Fixture{}, err
	}
	fixtureState := newX6State()
	config := x6Config()
	server, err := New(ctx, config, x6SecretResolver(material), fixtureState.dependencies())
	if err != nil {
		return X6Fixture{}, err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return X6Fixture{}, err
	}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	client1 := x6Client(listener.Addr().String(), roots, false)
	client2 := x6Client(listener.Addr().String(), roots, true)
	defer client1.CloseIdleConnections()
	defer client2.CloseIdleConnections()

	fixture := X6Fixture{Version: X6FixtureVersion, Decision: "tls_http", Limits: selectedX6Limits()}
	health, err := x6Request(ctx, client1, http.MethodGet, "/health/live", "", nil)
	if err != nil {
		return X6Fixture{}, err
	}
	fixture.HealthStatus = health.status

	scopeCases := []struct {
		scope  Scope
		method string
		path   string
		body   []byte
	}{
		{ScopeCatalogRead, http.MethodGet, "/v1/catalog/sources", nil},
		{ScopeCoverageRead, http.MethodGet, "/v1/coverage", nil},
		{ScopeQueryRead, http.MethodPost, "/v1/query", x6QueryBody("source-a", 1)},
		{ScopeReplayNative, http.MethodPost, "/v1/replay/native", x6ReplayBody("source-a")},
		{ScopeReplayNormalized, http.MethodPost, "/v1/replay/normalized", x6ReplayBody("source-a")},
		{ScopeMetricsRead, http.MethodGet, "/metrics", nil},
	}
	for index, test := range scopeCases {
		allowed, requestErr := x6Request(ctx, client1, test.method, test.path, tokens[test.scope], test.body)
		if requestErr != nil {
			return X6Fixture{}, requestErr
		}
		wrongScope := scopeCases[(index+1)%len(scopeCases)].scope
		denied, requestErr := x6Request(ctx, client1, test.method, test.path, tokens[wrongScope], test.body)
		if requestErr != nil {
			return X6Fixture{}, requestErr
		}
		fixture.Scopes = append(fixture.Scopes, X6ScopeResult{Scope: test.scope, AllowedStatus: allowed.status, DeniedStatus: denied.status})
	}

	for _, protocol := range []struct {
		name   string
		client *http.Client
	}{{"http/1.1", client1}, {"h2", client2}} {
		queryResult, requestErr := x6Request(ctx, protocol.client, http.MethodPost, "/v1/query", tokens[ScopeQueryRead], x6QueryBody("source-a", 1))
		if requestErr != nil {
			return X6Fixture{}, requestErr
		}
		nativeResult, requestErr := x6Request(ctx, protocol.client, http.MethodPost, "/v1/replay/native", tokens[ScopeReplayNative], x6ReplayBody("source-a"))
		if requestErr != nil {
			return X6Fixture{}, requestErr
		}
		normalizedResult, requestErr := x6Request(ctx, protocol.client, http.MethodPost, "/v1/replay/normalized", tokens[ScopeReplayNormalized], x6ReplayBody("source-a"))
		if requestErr != nil {
			return X6Fixture{}, requestErr
		}
		nativeErrorResult, requestErr := x6Request(ctx, protocol.client, http.MethodPost, "/v1/replay/native", tokens[ScopeReplayNative], x6ReplayBody("terminal-error"))
		if requestErr != nil {
			return X6Fixture{}, requestErr
		}
		normalizedErrorResult, requestErr := x6Request(ctx, protocol.client, http.MethodPost, "/v1/replay/normalized", tokens[ScopeReplayNormalized], x6ReplayBody("terminal-error"))
		if requestErr != nil {
			return X6Fixture{}, requestErr
		}
		for _, observed := range []string{nativeResult.protocol, normalizedResult.protocol, nativeErrorResult.protocol, normalizedErrorResult.protocol} {
			if observed != queryResult.protocol {
				return X6Fixture{}, fmt.Errorf("%w: X6 endpoint protocol mismatch", ErrConfiguration)
			}
		}
		fixture.Protocols = append(fixture.Protocols, X6ProtocolResult{
			Requested: protocol.name, Observed: queryResult.protocol,
			Query: x6WireResult(queryResult), Native: x6WireResult(nativeResult), Normalized: x6WireResult(normalizedResult),
			NativeError: x6WireResult(nativeErrorResult), NormalizedError: x6WireResult(normalizedErrorResult),
		})
	}
	for _, size := range []int{1, 1 << 10, 1 << 20, segment.MaxPayloadBytes} {
		sourceID := fmt.Sprintf("payload-%d", size)
		result, requestErr := x6Request(ctx, client1, http.MethodPost, "/v1/replay/native", tokens[ScopeReplayNative], x6ReplayBody(sourceID))
		if requestErr != nil {
			return X6Fixture{}, requestErr
		}
		fixture.Payloads = append(fixture.Payloads, X6PayloadResult{PayloadBytes: size, FrameBytes: len(result.body), Status: result.status})
	}
	longReplay, requestErr := x6Request(ctx, client1, http.MethodPost, "/v1/replay/native", tokens[ScopeReplayNative], x6ReplayBody("long"))
	if requestErr != nil {
		return X6Fixture{}, requestErr
	}
	fixture.Pressure.LongReplayStatus = longReplay.status
	fixture.Pressure.LongReplayBytes = len(longReplay.body)

	slowRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://x6.invalid/v1/replay/native", bytes.NewReader(x6ReplayBody("slow-client")))
	if err != nil {
		return X6Fixture{}, err
	}
	slowRequest.Header.Set("Authorization", "Bearer "+tokens[ScopeReplayNative])
	slowRequest.Header.Set("Content-Type", "application/json")
	slowResponse, err := client1.Do(slowRequest)
	if err != nil {
		return X6Fixture{}, err
	}
	one := make([]byte, 1)
	_, _ = slowResponse.Body.Read(one)
	fastPeer, err := x6Request(ctx, client1, http.MethodPost, "/v1/replay/native", tokens[ScopeReplayNative], x6ReplayBody("source-a"))
	if err != nil {
		return X6Fixture{}, err
	}
	fastQuery, err := x6Request(ctx, client1, http.MethodPost, "/v1/query", tokens[ScopeQueryRead], x6QueryBody("source-a", 1))
	if err != nil {
		return X6Fixture{}, err
	}
	_ = slowResponse.Body.Close()
	fixture.Pressure.SlowClientFastPeerStatus = fastPeer.status
	fixture.Pressure.SeparateQueryStatus = fastQuery.status

	blockers := make([]chan error, 0, config.NativeReplay.Concurrency+config.NativeReplay.QueueDepth)
	for range config.NativeReplay.Concurrency {
		requestDone := make(chan error, 1)
		blockers = append(blockers, requestDone)
		go func() {
			_, requestErr := x6Request(ctx, client1, http.MethodPost, "/v1/replay/native", tokens[ScopeReplayNative], x6ReplayBody("blocked"))
			requestDone <- requestErr
		}()
	}
	for range config.NativeReplay.Concurrency {
		select {
		case <-fixtureState.blockStarted:
		case <-ctx.Done():
			return X6Fixture{}, context.Cause(ctx)
		}
	}
	for range config.NativeReplay.QueueDepth {
		server.native.slots <- struct{}{}
	}
	queueFull, err := x6Request(ctx, client1, http.MethodPost, "/v1/replay/native", tokens[ScopeReplayNative], x6ReplayBody("source-a"))
	if err != nil {
		return X6Fixture{}, err
	}
	fixture.Pressure.QueueFullStatus = queueFull.status
	for range config.NativeReplay.QueueDepth {
		<-server.native.slots
	}
	close(fixtureState.blockRelease)
	for _, blocker := range blockers {
		select {
		case requestErr := <-blocker:
			if requestErr != nil {
				return X6Fixture{}, requestErr
			}
		case <-ctx.Done():
			return X6Fixture{}, context.Cause(ctx)
		}
	}

	cancelContext, cancel := context.WithCancel(ctx)
	cancelDone := make(chan error, 1)
	go func() {
		_, requestErr := x6Request(cancelContext, client1, http.MethodPost, "/v1/replay/native", tokens[ScopeReplayNative], x6ReplayBody("cancel"))
		cancelDone <- requestErr
	}()
	select {
	case <-fixtureState.cancelStarted:
		cancel()
	case <-ctx.Done():
		cancel()
		return X6Fixture{}, context.Cause(ctx)
	}
	<-cancelDone
	select {
	case <-fixtureState.cancelObserved:
		fixture.Pressure.CancellationObserved = true
	case <-ctx.Done():
		return X6Fixture{}, context.Cause(ctx)
	}

	disconnectRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://x6.invalid/v1/replay/native", bytes.NewReader(x6ReplayBody("disconnect")))
	if err != nil {
		return X6Fixture{}, err
	}
	disconnectRequest.Header.Set("Authorization", "Bearer "+tokens[ScopeReplayNative])
	disconnectRequest.Header.Set("Content-Type", "application/json")
	disconnectResponse, err := client1.Do(disconnectRequest)
	if err != nil {
		return X6Fixture{}, err
	}
	_, _ = disconnectResponse.Body.Read(one)
	_ = disconnectResponse.Body.Close()
	select {
	case <-fixtureState.disconnectObserved:
		fixture.Pressure.DisconnectObserved = true
	case <-ctx.Done():
		return X6Fixture{}, context.Cause(ctx)
	}

	shutdownDone := make(chan error, 1)
	go func() {
		_, requestErr := x6Request(ctx, client1, http.MethodPost, "/v1/replay/native", tokens[ScopeReplayNative], x6ReplayBody("shutdown-block"))
		shutdownDone <- requestErr
	}()
	select {
	case <-fixtureState.shutdownStarted:
	case <-ctx.Done():
		return X6Fixture{}, context.Cause(ctx)
	}
	shutdownContext, cancelShutdown := context.WithCancel(context.Background())
	cancelShutdown()
	shutdownErr := server.Shutdown(shutdownContext)
	fixture.Pressure.BoundedShutdownObserved = errors.Is(shutdownErr, context.Canceled)
	<-shutdownDone
	select {
	case serveErr := <-serveDone:
		if serveErr != nil {
			return X6Fixture{}, serveErr
		}
	case <-ctx.Done():
		return X6Fixture{}, context.Cause(ctx)
	}

	restartState := newX6State()
	restarted, err := New(ctx, config, x6SecretResolver(material), restartState.dependencies())
	if err != nil {
		return X6Fixture{}, err
	}
	restartListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return X6Fixture{}, err
	}
	restartDone := make(chan error, 1)
	go func() { restartDone <- restarted.Serve(restartListener) }()
	restartClient := x6Client(restartListener.Addr().String(), roots, false)
	restartHealth, err := x6Request(ctx, restartClient, http.MethodGet, "/health/live", "", nil)
	if err != nil {
		return X6Fixture{}, err
	}
	fixture.Pressure.RestartStatus = restartHealth.status
	restartClient.CloseIdleConnections()
	shutdownBound, cancelRestart := context.WithTimeout(context.Background(), time.Second)
	defer cancelRestart()
	if err := restarted.Shutdown(shutdownBound); err != nil {
		return X6Fixture{}, err
	}
	if err := <-restartDone; err != nil {
		return X6Fixture{}, err
	}
	if err := fixture.ValidateReleaseGate(); err != nil {
		return X6Fixture{}, err
	}
	return fixture, nil
}

func selectedX6Limits() X6Limits {
	defaults := DefaultConfig()
	return X6Limits{QueryQueue: defaults.Query.QueueDepth, QueryConcurrency: defaults.Query.Concurrency,
		QueryBufferBytes: defaults.Query.BufferBytes, NativeQueue: defaults.NativeReplay.QueueDepth,
		NativeConcurrency: defaults.NativeReplay.Concurrency, NativeBufferBytes: defaults.NativeReplay.BufferBytes,
		NormalizedQueue: defaults.NormalizedReplay.QueueDepth, NormalizedConcurrency: defaults.NormalizedReplay.Concurrency,
		NormalizedBufferBytes: defaults.NormalizedReplay.BufferBytes, MaximumPageRows: defaults.MaxPageRows,
		MaximumResponseBytes: defaults.MaxResponseBytes, MaximumReplayBytes: defaults.NativeReplay.MaxBytes,
		MaximumReplaySeconds: int64(defaults.NativeReplay.MaxDuration / time.Second)}
}

type x6SecretResolver map[string][]byte

func (r x6SecretResolver) Resolve(_ context.Context, reference string) ([]byte, error) {
	value, ok := r[reference]
	if !ok {
		return nil, ErrConfiguration
	}
	return slices.Clone(value), nil
}

func newX6Material() (x6SecretResolver, *x509.CertPool, map[Scope]string, error) {
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	notBefore := time.Unix(1_600_000_000, 0).UTC()
	template := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "x6.invalid"}, DNSNames: []string{"x6.invalid"},
		NotBefore: notBefore, NotAfter: time.Unix(4_000_000_000, 0).UTC(), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, template, publicKey, privateKey)
	if err != nil {
		return nil, nil, nil, err
	}
	certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(privateKey)
	if err != nil {
		return nil, nil, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	roots := x509.NewCertPool()
	certificate, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, nil, err
	}
	roots.AddCert(certificate)
	material := x6SecretResolver{"cert": certificatePEM, "key": keyPEM}
	paging := make([]byte, sha256.Size)
	if _, err := rand.Read(paging); err != nil {
		return nil, nil, nil, err
	}
	material["paging"] = paging
	tokens := make(map[Scope]string, 6)
	for index, scope := range []Scope{ScopeCatalogRead, ScopeCoverageRead, ScopeQueryRead, ScopeReplayNative, ScopeReplayNormalized, ScopeMetricsRead} {
		tokenBytes := make([]byte, 32)
		if _, err := rand.Read(tokenBytes); err != nil {
			return nil, nil, nil, err
		}
		token := base64.RawURLEncoding.EncodeToString(tokenBytes)
		tokens[scope] = token
		material[fmt.Sprintf("token-%d", index)] = []byte(token)
	}
	return material, roots, tokens, nil
}

func x6Config() Config {
	config := DefaultConfig()
	config.TLSCertRef, config.TLSKeyRef, config.PagingKeyRef = "cert", "key", "paging"
	config.Now = func() time.Time { return time.Unix(1_700_000_000, 0).UTC() }
	for index, scope := range []Scope{ScopeCatalogRead, ScopeCoverageRead, ScopeQueryRead, ScopeReplayNative, ScopeReplayNormalized, ScopeMetricsRead} {
		config.Principals = append(config.Principals, Principal{ID: fmt.Sprintf("principal-%d", index), TokenRef: fmt.Sprintf("token-%d", index), Scopes: []Scope{scope}})
	}
	return config
}

type x6State struct {
	dataset            warehouse.Dataset
	blockStarted       chan struct{}
	blockRelease       chan struct{}
	cancelStarted      chan struct{}
	cancelObserved     chan struct{}
	slowObserved       chan struct{}
	disconnectObserved chan struct{}
	shutdownStarted    chan struct{}
	shutdownObserved   chan struct{}
}

func newX6State() *x6State {
	return &x6State{dataset: warehouse.Dataset{ID: x6Hash(1), Family: "trade", CatalogSnapshotID: x6Hash(2),
		SchemaName: "trade_v1", SchemaVersion: 1}, blockStarted: make(chan struct{}, 64), blockRelease: make(chan struct{}),
		cancelStarted: make(chan struct{}), cancelObserved: make(chan struct{}), slowObserved: make(chan struct{}),
		disconnectObserved: make(chan struct{}), shutdownStarted: make(chan struct{}), shutdownObserved: make(chan struct{})}
}

func (s *x6State) dependencies() Dependencies {
	return Dependencies{Metadata: x6Metadata{s.dataset}, Datasets: x6Datasets{s.dataset}, Query: x6Pager{s.dataset},
		Native: x6Native{s}, Normalized: x6Normalized{s.dataset}, Metrics: x6Metrics{}}
}

type x6Metadata struct{ dataset warehouse.Dataset }

func (x x6Metadata) Sources(context.Context) ([]CatalogSource, error) {
	return []CatalogSource{{SourceID: "source-a", Venue: "synthetic", ProductFamily: "spot", APIFamily: "fixture", Environment: "test", Lifecycle: "active"}}, nil
}
func (x x6Metadata) Instruments(context.Context) ([]CatalogInstrument, error) {
	return []CatalogInstrument{{InstrumentUID: "instrument-a", SourceID: "source-a", NativeID: "SYNTH", ListingGeneration: 1, Lifecycle: "active", BaseAsset: "AAA", QuoteAsset: "BBB", Kind: "spot", Multiplier: "1"}}, nil
}
func (x x6Metadata) Coverage(context.Context) ([]Coverage, error) {
	return []Coverage{{ID: "coverage-a", Tuple: warehouse.Tuple{SourceID: "source-a", ChannelID: "channel-a", InstrumentUID: "instrument-a"}, StartReceivedTimeNS: 1, EndReceivedTimeNS: 2, State: "complete", CatalogSnapshotID: x.dataset.CatalogSnapshotIDString(), DatasetID: x.dataset.IDString()}}, nil
}
func (x x6Metadata) Incidents(context.Context) ([]Incident, error) { return []Incident{}, nil }
func (x x6Metadata) Datasets(context.Context) ([]CatalogDataset, error) {
	return []CatalogDataset{{DatasetID: x.dataset.IDString(), Family: x.dataset.Family, CatalogSnapshotID: x.dataset.CatalogSnapshotIDString(), SchemaName: x.dataset.SchemaName, SchemaVersion: x.dataset.SchemaVersion, Committed: true}}, nil
}
func (x6Metadata) Ready() bool { return true }

type x6Datasets struct{ dataset warehouse.Dataset }

func (x x6Datasets) Dataset(_ context.Context, id string) (warehouse.Dataset, error) {
	if id != x.dataset.IDString() {
		return warehouse.Dataset{}, ErrQueryRequest
	}
	return x.dataset, nil
}
func (x x6Datasets) LatestDataset(_ context.Context, family string) (warehouse.Dataset, error) {
	if family != x.dataset.Family {
		return warehouse.Dataset{}, ErrQueryRequest
	}
	return x.dataset, nil
}

type x6Pager struct{ dataset warehouse.Dataset }

func (x x6Pager) Page(_ context.Context, spec warehouse.QuerySpec) (warehouse.Page, error) {
	row := x6QueryRow(x.dataset, spec.SourceIDs[0])
	return warehouse.Page{Dataset: x.dataset, Rows: []warehouse.QueryRow{row}}, nil
}

func x6QueryRow(dataset warehouse.Dataset, sourceID string) warehouse.QueryRow {
	decimal := "1.000000000000000000"
	return warehouse.QueryRow{DatasetID: dataset.IDString(), CatalogSnapshotID: dataset.CatalogSnapshotIDString(), SchemaName: dataset.SchemaName,
		SchemaVersion: dataset.SchemaVersion, EventID: x6Hash(3).String(), RowID: x6Hash(4).String(), Family: dataset.Family,
		SourceID: sourceID, ChannelID: "channel-a", InstrumentUID: "instrument-a", ConnectionEpoch: "01000000000000000000000000000000",
		ReceivedTimeNS: 1, ArrivalOrdinal: 1, MessageOrdinal: 1, RawSegmentSHA256: x6Hash(5).String(),
		RawRecordOrdinal: 1, RawPayloadSHA256: x6Hash(6).String(), CoverageRefIDs: []string{}, GapRefIDs: []string{}, Price: decimal}
}

type x6Native struct{ state *x6State }

func (x x6Native) OpenNative(_ context.Context, request replay.ServiceRequest) (replay.NativeStream, error) {
	sourceID := request.SourceIDs[0]
	switch sourceID {
	case "slow-client":
		frame, err := x6Frame(sourceID, 1<<20)
		if err != nil {
			return nil, err
		}
		return &x6SlowStream{frame: frame, remaining: 64, observed: x.state.slowObserved}, nil
	case "long":
		frame, err := x6Frame(sourceID, 1<<20)
		if err != nil {
			return nil, err
		}
		frames := make([][]byte, x6LongReplayFrames)
		for index := range frames {
			frames[index] = frame
		}
		return &replay.SliceNativeStream{Frames: frames}, nil
	case "blocked":
		return &x6BlockingStream{started: x.state.blockStarted, release: x.state.blockRelease}, nil
	case "cancel":
		return &x6CancelStream{started: x.state.cancelStarted, observed: x.state.cancelObserved}, nil
	case "disconnect":
		frame, err := x6Frame(sourceID, 1<<20)
		if err != nil {
			return nil, err
		}
		return &x6DisconnectStream{frame: frame, observed: x.state.disconnectObserved}, nil
	case "shutdown-block":
		return &x6CancelStream{started: x.state.shutdownStarted, observed: x.state.shutdownObserved}, nil
	case "terminal-error":
		frame, err := x6Frame(sourceID, 1)
		if err != nil {
			return nil, err
		}
		return &x6TerminalNativeStream{frame: frame}, nil
	}
	size := 1
	_, _ = fmt.Sscanf(sourceID, "payload-%d", &size)
	frame, err := x6Frame(sourceID, size)
	if err != nil {
		return nil, err
	}
	return &replay.SliceNativeStream{Frames: [][]byte{frame}}, nil
}

type x6Normalized struct{ dataset warehouse.Dataset }

func (x x6Normalized) OpenNormalized(_ context.Context, request replay.ServiceRequest) (replay.NormalizedStream, error) {
	item := x6NormalizedItem(x.dataset, request.SourceIDs[0])
	if request.SourceIDs[0] == "terminal-error" {
		return &x6TerminalNormalizedStream{item: item}, nil
	}
	return &replay.SliceNormalizedStream{Items: []replay.NormalizedItem{item}}, nil
}

func x6NormalizedItem(dataset warehouse.Dataset, sourceID string) replay.NormalizedItem {
	return replay.NormalizedItem{Version: replay.ServiceVersionV1, Type: replay.NormalizedRecordKind, DatasetID: dataset.IDString(),
		CatalogSnapshotID: dataset.CatalogSnapshotIDString(), SchemaName: dataset.SchemaName, SchemaVersion: dataset.SchemaVersion,
		Record: ptr(x6QueryRow(dataset, sourceID))}
}

var errX6Terminal = errors.New("synthetic terminal error")

type x6TerminalNativeStream struct {
	frame []byte
	sent  bool
}

func (s *x6TerminalNativeStream) Next(context.Context) ([]byte, error) {
	if s.sent {
		return nil, errX6Terminal
	}
	s.sent = true
	return s.frame, nil
}

func (*x6TerminalNativeStream) Close() error { return nil }

type x6TerminalNormalizedStream struct {
	item replay.NormalizedItem
	sent bool
}

func (s *x6TerminalNormalizedStream) Next(context.Context) (replay.NormalizedItem, error) {
	if s.sent {
		return replay.NormalizedItem{}, errX6Terminal
	}
	s.sent = true
	return s.item, nil
}

func (*x6TerminalNormalizedStream) Close() error { return nil }

type x6Metrics struct{}

func (x6Metrics) Metrics(context.Context) ([]Metric, error) {
	return []Metric{{Name: "marketdata_fixture_ready", Help: "Synthetic X6 readiness.", Type: MetricGauge, Value: 1}}, nil
}

type x6SlowStream struct {
	frame     []byte
	remaining int
	observed  chan struct{}
	once      sync.Once
}

func (s *x6SlowStream) Next(ctx context.Context) ([]byte, error) {
	if s.remaining == 0 {
		return nil, io.EOF
	}
	if err := ctx.Err(); err != nil {
		s.once.Do(func() { close(s.observed) })
		return nil, err
	}
	s.remaining--
	return s.frame, nil
}
func (*x6SlowStream) Close() error { return nil }

type x6BlockingStream struct {
	started chan struct{}
	release chan struct{}
	sent    bool
}

func (s *x6BlockingStream) Next(ctx context.Context) ([]byte, error) {
	if s.sent {
		return nil, io.EOF
	}
	s.sent = true
	s.started <- struct{}{}
	select {
	case <-s.release:
		return nil, io.EOF
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
func (*x6BlockingStream) Close() error { return nil }

type x6CancelStream struct {
	started     chan struct{}
	observed    chan struct{}
	startOnce   sync.Once
	observeOnce sync.Once
}

func (s *x6CancelStream) Next(ctx context.Context) ([]byte, error) {
	s.startOnce.Do(func() { close(s.started) })
	<-ctx.Done()
	s.observeOnce.Do(func() { close(s.observed) })
	return nil, context.Cause(ctx)
}
func (*x6CancelStream) Close() error { return nil }

type x6DisconnectStream struct {
	frame    []byte
	first    bool
	observed chan struct{}
	once     sync.Once
}

func (s *x6DisconnectStream) Next(ctx context.Context) ([]byte, error) {
	if !s.first {
		s.first = true
		return s.frame, nil
	}
	<-ctx.Done()
	s.once.Do(func() { close(s.observed) })
	return nil, context.Cause(ctx)
}
func (s *x6DisconnectStream) Close() error { s.once.Do(func() { close(s.observed) }); return nil }

func x6Frame(sourceID string, payloadBytes int) ([]byte, error) {
	return segment.MarshalEnvelope(segment.Envelope{Kind: segment.RecordKindWebSocket, SourceID: sourceID, ChannelOrEndpoint: "channel-a",
		InstrumentUID: segment.OptionalString{Value: "instrument-a", Valid: true}, ArrivalOrdinal: 1, MessageOrdinal: 1,
		ReceivedWallTimeNS: 1, ClockEpochID: "clock-a", PayloadEncoding: segment.PayloadEncodingBinary,
		RawPayload: bytes.Repeat([]byte{'x'}, payloadBytes), TerminalOutcome: segment.OutcomeObserved, RecorderVersion: "x6-v1"})
}

func x6Hash(value byte) warehouse.Hash { var hash warehouse.Hash; hash[0] = value; return hash }
func ptr[T any](value T) *T            { return &value }

func x6QueryBody(sourceID string, limit int) []byte {
	body, _ := json.Marshal(QueryRequest{Family: "trade", SourceIDs: []string{sourceID}, StartReceivedTimeNS: 1, EndReceivedTimeNS: 2, Limit: limit})
	return body
}
func x6ReplayBody(sourceID string) []byte {
	body, _ := json.Marshal(ReplayRequest{Family: "trade", SourceIDs: []string{sourceID}, StartReceivedTimeNS: 1, EndReceivedTimeNS: 2})
	return body
}

type x6HTTPResult struct {
	status        int
	protocol      string
	body          []byte
	truncated     string
	terminalError string
}

func x6WireResult(result x6HTTPResult) X6WireResult {
	digest := sha256.Sum256(result.body)
	return X6WireResult{Status: result.status, BodyBytes: len(result.body), SHA256: hex.EncodeToString(digest[:]),
		Truncated: result.truncated, TerminalError: result.terminalError}
}

func x6Request(ctx context.Context, client *http.Client, method, path, token string, body []byte) (x6HTTPResult, error) {
	request, err := http.NewRequestWithContext(ctx, method, "https://x6.invalid"+path, bytes.NewReader(body))
	if err != nil {
		return x6HTTPResult{}, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return x6HTTPResult{}, err
	}
	defer response.Body.Close()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 300<<20))
	if err != nil {
		return x6HTTPResult{}, err
	}
	return x6HTTPResult{status: response.StatusCode, protocol: response.Proto, body: responseBody,
		truncated: response.Trailer.Get(ReplayTrailerTruncated), terminalError: response.Trailer.Get(ReplayTrailerTerminalError)}, nil
}

func x6Client(address string, roots *x509.CertPool, http2 bool) *http.Client {
	transport := &http.Transport{TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "x6.invalid"},
		ForceAttemptHTTP2: http2, DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, address)
		}}
	if !http2 {
		transport.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	}
	return &http.Client{Transport: transport}
}
