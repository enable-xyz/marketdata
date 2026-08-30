package collector

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
)

const SpotExchangeInfoEndpoint = binance.SpotPublicRESTEndpoint + binance.SpotRESTPath

var (
	ErrAlreadyRunning = errors.New("collector: runtime already started")
	ErrMetadataSync   = errors.New("collector: Binance Spot metadata sync failed")
)

// EpochSource returns a new opaque connection or poll identity for each call.
type EpochSource interface {
	NewEpoch(capture.EpochKind) (capture.StreamEpoch, error)
}

// CatalogSyncer is the committed-evidence catalog boundary consumed by the
// collector. *catalog.Store implements it.
type CatalogSyncer interface {
	Sync(context.Context, catalog.SyncInput) (catalog.Snapshot, error)
}

// PublicationCatalog resolves the exact committed row written by Publisher.
// *catalog.PublicationStore implements it.
type PublicationCatalog interface {
	FindRawSegment(context.Context, string) (catalog.RawSegmentPublication, bool, error)
}

// EvidenceRecorder records the response coordinate only after its immutable
// segment is committed. *catalog.QueryStore implements it.
type EvidenceRecorder interface {
	RecordCommittedRESTEvidence(context.Context, catalog.RawSegmentPublication, capture.EnvelopeV1, capture.EnvelopeV1) error
}

// RandomEpochSource generates UUID-shaped 128-bit epoch identities from an
// injected cryptographic reader. It is safe for concurrent poll loops.
type RandomEpochSource struct {
	reader io.Reader
	mu     sync.Mutex
}

func NewRandomEpochSource(reader io.Reader) (*RandomEpochSource, error) {
	if reader == nil {
		return nil, fmt.Errorf("%w: epoch entropy reader is required", ErrConfiguration)
	}
	return &RandomEpochSource{reader: reader}, nil
}

func NewCryptoEpochSource() *RandomEpochSource {
	source, _ := NewRandomEpochSource(rand.Reader)
	return source
}

func (s *RandomEpochSource) NewEpoch(kind capture.EpochKind) (capture.StreamEpoch, error) {
	if kind != capture.EpochConnection && kind != capture.EpochPollCycle {
		return capture.StreamEpoch{}, fmt.Errorf("%w: invalid epoch kind", ErrConfiguration)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var id [16]byte
	if _, err := io.ReadFull(s.reader, id[:]); err != nil {
		return capture.StreamEpoch{}, fmt.Errorf("collector: generate epoch identity: %w", err)
	}
	id[6] = id[6]&0x0f | 0x40
	id[8] = id[8]&0x3f | 0x80
	epoch := capture.StreamEpoch{Kind: kind, ID: id}
	if err := epoch.Validate(); err != nil {
		return capture.StreamEpoch{}, fmt.Errorf("collector: generated epoch identity: %w", err)
	}
	return epoch, nil
}

// SourceConfig is the explicit allowlisted production-public Binance Spot
// surface. Empty or alternate endpoints fail closed.
type SourceConfig struct {
	WebSocketEndpoint    string
	PublicRESTEndpoint   string
	ExchangeInfoEndpoint string
	ExchangeInfoMaxBytes int
}

// SymbolConfig declares one live symbol and its independent depth snapshot
// cadence.
type SymbolConfig struct {
	NativeID     string
	DepthCadence time.Duration
}

// Config contains no ambient paths, endpoints, schedules, or retention.
type Config struct {
	Source                    SourceConfig
	Symbols                   []SymbolConfig
	DepthLimit                int
	MicrosecondTime           bool
	RecorderVersion           string
	ReconnectDelay            time.Duration
	Spool                     RawSinkConfig
	ExplicitLifecycleClosures []string
}

// Dependencies are all effectful live boundaries. Production composition uses
// binance.CoderSpotWSConnector, binance.PublicSpotRESTClient,
// objectstore.Publisher backed by catalog.PublicationStore, catalog.QueryStore
// for evidence, and catalog.Store for the temporal sync.
type Dependencies struct {
	WSConnector        binance.SpotWSConnector
	RESTClient         binance.SpotRESTClient
	MetadataHTTPClient *http.Client
	Clock              Clock
	RateBudget         capture.RateBudget
	Publisher          SegmentPublisher
	Publications       PublicationCatalog
	Evidence           EvidenceRecorder
	Catalog            CatalogSyncer
	Gaps               GapRecorder
	Epochs             EpochSource
}

// Stats is a bounded acceptance snapshot.
type Stats struct {
	Sink             SinkStats
	MetadataSyncs    uint64
	WebSocketEpochs  uint64
	DepthPolls       uint64
	Disconnects      uint64
	GapIntervals     uint64
	CaptureFailures  uint64
	ActiveGoroutines uint64
}

type runtimeCounters struct {
	metadataSyncs    atomic.Uint64
	webSocketEpochs  atomic.Uint64
	depthPolls       atomic.Uint64
	disconnects      atomic.Uint64
	gapIntervals     atomic.Uint64
	captureFailures  atomic.Uint64
	activeGoroutines atomic.Uint64
}

// Runtime owns the single always-on Binance Spot live composition.
type Runtime struct {
	config       Config
	dependencies Dependencies
	sink         *DurableRawSink
	metadata     *ExchangeInfoClient
	counters     runtimeCounters
	running      atomic.Bool
}

// New recovers and republishes raw storage before it returns. No network source
// is contacted until Run is invoked.
func New(ctx context.Context, config Config, dependencies Dependencies) (*Runtime, error) {
	if err := validateConfig(config, dependencies); err != nil {
		return nil, err
	}
	metadata, err := NewScopedExchangeInfoClient(config.Source.ExchangeInfoEndpoint, symbolNativeIDs(config.Symbols), dependencies.MetadataHTTPClient)
	if err != nil {
		return nil, err
	}
	sink, err := NewDurableRawSink(ctx, config.Spool, dependencies.Publisher, dependencies.Clock)
	if err != nil {
		return nil, err
	}
	if err := sink.attachGapRecorder(ctx, dependencies.Gaps, config.Symbols); err != nil {
		return nil, err
	}
	config.Symbols = slices.Clone(config.Symbols)
	config.ExplicitLifecycleClosures = slices.Clone(config.ExplicitLifecycleClosures)
	return &Runtime{config: config, dependencies: dependencies, sink: sink, metadata: metadata}, nil
}

func validateConfig(config Config, dependencies Dependencies) error {
	if config.Source.WebSocketEndpoint != binance.SpotWSEndpoint ||
		config.Source.PublicRESTEndpoint != binance.SpotPublicRESTEndpoint ||
		config.Source.ExchangeInfoEndpoint != SpotExchangeInfoEndpoint {
		return fmt.Errorf("%w: exact Binance Spot public endpoints are required", ErrConfiguration)
	}
	if config.Source.ExchangeInfoMaxBytes <= 0 ||
		config.Source.ExchangeInfoMaxBytes > binance.MaxExchangeInfoBytes ||
		config.Spool.FrameBytes < 2<<20 ||
		config.Source.ExchangeInfoMaxBytes > config.Spool.FrameBytes-(128<<10) {
		return fmt.Errorf("%w: explicit source payload bounds exceed the durable frame", ErrConfiguration)
	}
	if err := validateRawSinkConfig(config.Spool); err != nil {
		return err
	}
	if config.RecorderVersion == "" || config.Spool.WriterVersion != config.RecorderVersion || config.ReconnectDelay <= 0 {
		return fmt.Errorf("%w: recorder versions must match and reconnect delay must be positive", ErrConfiguration)
	}
	if _, err := binance.SpotDepthRequestWeight(config.DepthLimit); err != nil {
		return fmt.Errorf("%w: %v", ErrConfiguration, err)
	}
	if len(config.Symbols) == 0 {
		return fmt.Errorf("%w: at least one symbol is required", ErrConfiguration)
	}
	symbols := make([]string, 0, len(config.Symbols))
	seen := make(map[string]struct{}, len(config.Symbols))
	for _, symbol := range config.Symbols {
		if symbol.NativeID == "" || symbol.NativeID != strings.ToUpper(symbol.NativeID) || symbol.DepthCadence <= 0 {
			return fmt.Errorf("%w: canonical uppercase symbol and depth cadence are required", ErrConfiguration)
		}
		if _, exists := seen[symbol.NativeID]; exists {
			return fmt.Errorf("%w: duplicate symbol %q", ErrConfiguration, symbol.NativeID)
		}
		seen[symbol.NativeID] = struct{}{}
		symbols = append(symbols, symbol.NativeID)
	}
	if _, err := binance.NewSpotSubscriptionPlan(symbols); err != nil {
		return fmt.Errorf("%w: subscription plan: %v", ErrConfiguration, err)
	}
	reservation, _ := epochReservation(config.Spool)
	if workers := uint64(len(config.Symbols)) + 1; workers > uint64(config.Spool.MaxBytes)/reservation {
		return fmt.Errorf("%w: spool byte bound cannot reserve all live epochs", ErrConfiguration)
	}
	if dependencies.WSConnector == nil || dependencies.RESTClient == nil || dependencies.MetadataHTTPClient == nil ||
		dependencies.Clock == nil || dependencies.RateBudget == nil || dependencies.Publisher == nil ||
		dependencies.Publications == nil || dependencies.Evidence == nil ||
		dependencies.Catalog == nil || dependencies.Gaps == nil || dependencies.Epochs == nil {
		return fmt.Errorf("%w: all live dependencies are required", ErrConfiguration)
	}
	if dependencies.MetadataHTTPClient.Timeout <= 0 ||
		dependencies.MetadataHTTPClient.Timeout >= config.Spool.SegmentMaxAge {
		return fmt.Errorf("%w: metadata HTTP timeout must be positive and shorter than segment age", ErrConfiguration)
	}
	return nil
}

func (r *Runtime) Stats() Stats {
	return Stats{
		Sink:             r.sink.Stats(),
		MetadataSyncs:    r.counters.metadataSyncs.Load(),
		WebSocketEpochs:  r.counters.webSocketEpochs.Load(),
		DepthPolls:       r.counters.depthPolls.Load(),
		Disconnects:      r.counters.disconnects.Load(),
		GapIntervals:     r.counters.gapIntervals.Load(),
		CaptureFailures:  r.counters.captureFailures.Load(),
		ActiveGoroutines: r.counters.activeGoroutines.Load(),
	}
}

// SyncMetadata performs one committed-raw exchange-info synchronization
// without starting the live WebSocket and depth workers.
func (r *Runtime) SyncMetadata(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("%w: runtime is required", ErrConfiguration)
	}
	if !r.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer r.running.Store(false)
	if err := ctx.Err(); err != nil {
		return err
	}
	return r.syncMetadata(ctx)
}

// Run performs the committed-raw metadata sync first, then runs WebSocket and
// per-symbol depth capture until cancellation or a fatal storage failure.
func (r *Runtime) Run(ctx context.Context) error {
	if r == nil {
		return fmt.Errorf("%w: runtime is required", ErrConfiguration)
	}
	if !r.running.CompareAndSwap(false, true) {
		return ErrAlreadyRunning
	}
	defer r.running.Store(false)
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := r.syncMetadata(ctx); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := len(r.config.Symbols) + 1
	errorsCh := make(chan error, workers)
	var wait sync.WaitGroup
	start := func(run func(context.Context) error) {
		wait.Add(1)
		r.counters.activeGoroutines.Add(1)
		go func() {
			defer wait.Done()
			defer r.counters.activeGoroutines.Add(^uint64(0))
			errorsCh <- run(runCtx)
		}()
	}
	start(r.runWebSocket)
	for _, symbol := range r.config.Symbols {
		symbol := symbol
		start(func(ctx context.Context) error { return r.runDepth(ctx, symbol) })
	}

	var runErr error
	remaining := workers
	canceled := ctx.Done()
	for remaining > 0 {
		select {
		case <-canceled:
			cancel()
			canceled = nil
		case err := <-errorsCh:
			remaining--
			if err != nil && !errors.Is(err, context.Canceled) && runErr == nil {
				runErr = err
				cancel()
			}
		}
	}
	wait.Wait()
	return runErr
}

func (r *Runtime) runWebSocket(ctx context.Context) error {
	symbols := make([]string, len(r.config.Symbols))
	for i, symbol := range r.config.Symbols {
		symbols[i] = symbol.NativeID
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		epoch, err := r.dependencies.Epochs.NewEpoch(capture.EpochConnection)
		if err != nil {
			return err
		}
		collector, err := binance.NewSpotCapture(binance.SpotWSConfig{
			Symbols:         symbols,
			MicrosecondTime: r.config.MicrosecondTime,
			RecorderVersion: r.config.RecorderVersion,
			Endpoint:        r.config.Source.WebSocketEndpoint,
			Epochs:          []capture.StreamEpoch{epoch},
		}, r.dependencies.WSConnector, r.dependencies.Clock, r.dependencies.RateBudget, r.sink)
		if err != nil {
			return err
		}
		r.counters.webSocketEpochs.Add(1)
		result, stepErr := collector.Start(ctx)
		r.observeResult(result)
		if hasSinkFailure(result) {
			return sinkFailureError(stepErr)
		}
		for result.State != capture.RunnerClosed && ctx.Err() == nil {
			result, stepErr = collector.Step(ctx)
			r.observeResult(result)
			if hasSinkFailure(result) {
				return sinkFailureError(stepErr)
			}
		}
		if result.State != capture.RunnerClosed && ctx.Err() != nil {
			result, stepErr = collector.Step(ctx)
			r.observeResult(result)
			if hasSinkFailure(result) {
				return sinkFailureError(stepErr)
			}
		}
		r.sink.ForgetEpoch(epoch)
		if ctx.Err() != nil {
			return nil
		}
		if stepErr != nil {
			r.counters.captureFailures.Add(1)
		}
		if err := waitAfter(ctx, r.dependencies.Clock, r.config.ReconnectDelay); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}
}

func (r *Runtime) runDepth(ctx context.Context, symbol SymbolConfig) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		epoch, err := r.dependencies.Epochs.NewEpoch(capture.EpochPollCycle)
		if err != nil {
			return err
		}
		requestID := "depth." + strings.ToLower(symbol.NativeID) + "." + hex.EncodeToString(epoch.ID[:])
		request, err := binance.NewSpotDepthRequest(requestID, symbol.NativeID, r.config.DepthLimit, r.config.MicrosecondTime)
		if err != nil {
			return err
		}
		scheduled := r.dependencies.Clock.Read().WallTimeNS
		collector, err := binance.NewSpotDepthCapture(binance.SpotDepthConfig{
			Request:         request,
			RecorderVersion: r.config.RecorderVersion,
			Epoch:           epoch,
			ScheduledAtNS:   scheduled,
		}, r.dependencies.RESTClient, r.dependencies.Clock, r.dependencies.RateBudget, r.sink)
		if err != nil {
			return err
		}
		r.counters.depthPolls.Add(1)
		result, stepErr := collector.Start(ctx)
		r.observeResult(result)
		if hasSinkFailure(result) {
			return sinkFailureError(stepErr)
		}
		for result.State != capture.RunnerClosed && ctx.Err() == nil {
			result, stepErr = collector.Step(ctx)
			r.observeResult(result)
			if hasSinkFailure(result) {
				return sinkFailureError(stepErr)
			}
		}
		if result.State != capture.RunnerClosed && ctx.Err() != nil {
			result, stepErr = collector.Step(ctx)
			r.observeResult(result)
			if hasSinkFailure(result) {
				return sinkFailureError(stepErr)
			}
		}
		r.sink.ForgetEpoch(epoch)
		if ctx.Err() != nil {
			return nil
		}
		if stepErr != nil {
			r.counters.captureFailures.Add(1)
		}
		if err := waitAfter(ctx, r.dependencies.Clock, symbol.DepthCadence); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}
}

func (r *Runtime) observeResult(result capture.StepResult) {
	for _, control := range result.Controls {
		if control.Envelope.ControlKind.Value == capture.ControlDisconnect {
			r.counters.disconnects.Add(1)
		}
	}
	for _, fault := range result.Faults {
		if fault.Kind == capture.FaultDisconnectAbrupt || fault.Kind == capture.FaultQueuePressure || fault.Kind == capture.FaultUsefulDataMissed {
			r.counters.gapIntervals.Add(1)
		}
	}
}

func hasSinkFailure(result capture.StepResult) bool {
	for _, fault := range result.Faults {
		if fault.Kind == capture.FaultSinkFailure {
			return true
		}
	}
	return false
}

func sinkFailureError(err error) error {
	if err != nil {
		return err
	}
	return ErrRawSink
}

// ExchangeInfoClient is the collector-owned bounded public GET adapter. It
// accepts only the exact SourceConfig-authorized production endpoint.
type ExchangeInfoClient struct {
	endpoint         *url.URL
	client           *http.Client
	requestedSymbols []string
	symbolsParameter string
}

// NewExchangeInfoClient constructs the complete-venue metadata client retained
// for explicit verification workflows.
func NewExchangeInfoClient(endpoint string, client *http.Client) (*ExchangeInfoClient, error) {
	return newExchangeInfoClient(endpoint, nil, client)
}

// NewScopedExchangeInfoClient restricts metadata acquisition to the exact
// caller-declared symbols captured by the live collector.
func NewScopedExchangeInfoClient(endpoint string, symbols []string, client *http.Client) (*ExchangeInfoClient, error) {
	if len(symbols) == 0 {
		return nil, fmt.Errorf("%w: scoped exchangeInfo symbols are required", ErrConfiguration)
	}
	return newExchangeInfoClient(endpoint, symbols, client)
}

func newExchangeInfoClient(endpoint string, symbols []string, client *http.Client) (*ExchangeInfoClient, error) {
	if client == nil || client.Transport == nil || client.Timeout <= 0 {
		return nil, fmt.Errorf("%w: metadata HTTP transport and timeout are required", ErrConfiguration)
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || endpoint != SpotExchangeInfoEndpoint || parsed.Scheme != "https" || parsed.Host != "data-api.binance.vision" ||
		parsed.Path != binance.SpotRESTPath || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.User != nil {
		return nil, fmt.Errorf("%w: exchangeInfo endpoint is not the exact public contract", ErrConfiguration)
	}
	requested, parameter, err := canonicalMetadataScope(symbols)
	if err != nil {
		return nil, err
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("collector: exchangeInfo redirects are disabled")
	}
	return &ExchangeInfoClient{
		endpoint: parsed, client: &copyClient, requestedSymbols: requested, symbolsParameter: parameter,
	}, nil
}

func canonicalMetadataScope(symbols []string) ([]string, string, error) {
	if symbols == nil {
		return nil, "", nil
	}
	if _, err := binance.NewSpotSubscriptionPlan(symbols); err != nil {
		return nil, "", fmt.Errorf("%w: exchangeInfo symbol scope: %v", ErrConfiguration, err)
	}
	requested := slices.Clone(symbols)
	for index, symbol := range requested {
		if symbol != strings.ToUpper(symbol) {
			return nil, "", fmt.Errorf("%w: exchangeInfo symbol %d is not a canonical uppercase identity", ErrConfiguration, index)
		}
	}
	slices.Sort(requested)
	encoded, err := json.Marshal(requested)
	if err != nil || len(encoded) > capture.MaxRESTParameterValueBytes {
		return nil, "", fmt.Errorf("%w: exchangeInfo symbol scope exceeds bounded request evidence", ErrConfiguration)
	}
	return requested, string(encoded), nil
}

func symbolNativeIDs(symbols []SymbolConfig) []string {
	result := make([]string, len(symbols))
	for index := range symbols {
		result[index] = symbols[index].NativeID
	}
	return result
}

func (c *ExchangeInfoClient) parameters() []capture.SanitizedParameter {
	if c == nil || c.symbolsParameter == "" {
		return nil
	}
	return []capture.SanitizedParameter{{Name: "symbols", Value: c.symbolsParameter}}
}

func (c *ExchangeInfoClient) symbols() []string {
	if c == nil {
		return nil
	}
	return slices.Clone(c.requestedSymbols)
}

type exchangeInfoResponse struct {
	status  int
	headers []capture.RESTHeader
	body    []byte
}

func (c *ExchangeInfoClient) Get(ctx context.Context, maximumBytes int) (exchangeInfoResponse, error) {
	if maximumBytes <= 0 || maximumBytes > binance.MaxExchangeInfoBytes {
		return exchangeInfoResponse{}, fmt.Errorf("%w: exchangeInfo response bound is invalid", ErrConfiguration)
	}
	endpoint := *c.endpoint
	if c.symbolsParameter != "" {
		query := url.Values{}
		query.Set("symbols", c.symbolsParameter)
		endpoint.RawQuery = query.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return exchangeInfoResponse{}, fmt.Errorf("collector: construct exchangeInfo request: %w", err)
	}
	if request.Header.Get("Authorization") != "" || request.Header.Get("X-MBX-APIKEY") != "" {
		return exchangeInfoResponse{}, fmt.Errorf("%w: authentication headers are prohibited", ErrConfiguration)
	}
	response, err := c.client.Do(request)
	if err != nil {
		return exchangeInfoResponse{}, fmt.Errorf("collector: execute exchangeInfo request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(maximumBytes)+1))
	if err != nil {
		return exchangeInfoResponse{}, fmt.Errorf("collector: read exchangeInfo response: %w", err)
	}
	if len(body) > maximumBytes {
		return exchangeInfoResponse{}, fmt.Errorf("%w: exchangeInfo response exceeds %d bytes", ErrMetadataSync, maximumBytes)
	}
	return exchangeInfoResponse{status: response.StatusCode, headers: allowlistedMetadataHeaders(response.Header), body: body}, nil
}

func allowlistedMetadataHeaders(headers http.Header) []capture.RESTHeader {
	candidates := []struct {
		name string
		kind capture.RESTHeaderKind
	}{
		{"Content-Length", capture.RESTHeaderContentLength},
		{"Content-Type", capture.RESTHeaderContentType},
		{"Retry-After", capture.RESTHeaderRetryAfter},
		{"X-Mbx-Used-Weight", capture.RESTHeaderUsedWeight},
		{"X-Mbx-Used-Weight-1m", capture.RESTHeaderUsedWeight},
	}
	seen := make(map[capture.RESTHeaderKind]struct{})
	result := make([]capture.RESTHeader, 0, len(candidates))
	for _, candidate := range candidates {
		value := headers.Get(candidate.name)
		if value == "" || len(value) > capture.MaxRESTHeaderValueBytes || !utf8.ValidString(value) ||
			strings.ContainsAny(value, "\r\n\x00") {
			continue
		}
		if _, exists := seen[candidate.kind]; exists {
			continue
		}
		seen[candidate.kind] = struct{}{}
		result = append(result, capture.RESTHeader{Kind: candidate.kind, Value: value})
	}
	slices.SortFunc(result, func(a, b capture.RESTHeader) int { return strings.Compare(string(a.Kind), string(b.Kind)) })
	return result
}

func (r *Runtime) syncMetadata(ctx context.Context) error {
	epoch, err := r.dependencies.Epochs.NewEpoch(capture.EpochPollCycle)
	if err != nil {
		return err
	}
	defer r.sink.ForgetEpoch(epoch)
	scheduledReading := r.dependencies.Clock.Read()
	scheduled := postgresTimeNS(scheduledReading.WallTimeNS)
	startedReading := r.dependencies.Clock.Read()
	started := postgresTimeNS(startedReading.WallTimeNS)
	requestID := "exchange-info." + hex.EncodeToString(epoch.ID[:])
	requestEvidence := capture.RESTRequestEvidenceV1{
		Version:       capture.RESTEvidenceVersion,
		Kind:          "request",
		RequestID:     requestID,
		Method:        capture.RESTMethodGET,
		Parameters:    r.metadata.parameters(),
		ScheduledAtNS: scheduled,
		StartedAtNS:   started,
	}
	requestExtension, err := capture.MarshalRESTRequestEvidence(requestEvidence)
	if err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	requestEnvelope := capture.EnvelopeV1{
		EnvelopeVersion:            capture.EnvelopeVersion,
		RecordKind:                 capture.RecordKindControl,
		SourceID:                   binance.SpotSourceID,
		ChannelOrEndpoint:          binance.SpotExchangeInfoChannel,
		PollCycleID:                capture.OptionalEpoch{Value: epoch.ID, Valid: true},
		ArrivalOrdinal:             1,
		ScheduledAtNS:              capture.OptionalInt64{Value: scheduled, Valid: true},
		RequestStartedAtNS:         capture.OptionalInt64{Value: started, Valid: true},
		ReceivedWallTimeNS:         started,
		ClockEpochID:               startedReading.ClockEpochID,
		MonotonicNSSinceClockEpoch: startedReading.MonotonicNS,
		SubscriptionOrRequestID:    capture.OptionalString{Value: requestID, Valid: true},
		PayloadEncoding:            capture.PayloadEncodingNone,
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            r.config.RecorderVersion,
		ControlKind:                capture.OptionalControlKind{Value: capture.ControlRequestStarted, Valid: true},
		Extensions:                 requestExtension,
	}
	requestEnvelope.SetRawPayload(nil)
	if err := r.sink.WriteRaw(ctx, requestEnvelope); err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	response, requestErr := r.metadata.Get(ctx, r.config.Source.ExchangeInfoMaxBytes)
	if requestErr != nil {
		closeErr := r.closeMetadataEpoch(ctx, epoch, 1, requestErr, started)
		return errors.Join(ErrMetadataSync, requestErr, closeErr)
	}
	completedReading := r.dependencies.Clock.Read()
	completed := postgresTimeNS(completedReading.WallTimeNS)
	responseEvidence := capture.RESTResponseEvidenceV1{
		Version:       capture.RESTEvidenceVersion,
		Kind:          "response",
		RequestID:     requestID,
		CompletedAtNS: completed,
		Status:        response.status,
		Headers:       response.headers,
	}
	responseExtension, err := capture.MarshalRESTResponseEvidence(responseEvidence)
	if err != nil {
		closeErr := r.closeMetadataEpoch(ctx, epoch, 1, err, started)
		return errors.Join(ErrMetadataSync, err, closeErr)
	}
	responseEnvelope := capture.EnvelopeV1{
		EnvelopeVersion:            capture.EnvelopeVersion,
		RecordKind:                 capture.RecordKindREST,
		SourceID:                   binance.SpotSourceID,
		ChannelOrEndpoint:          binance.SpotExchangeInfoChannel,
		PollCycleID:                capture.OptionalEpoch{Value: epoch.ID, Valid: true},
		ArrivalOrdinal:             2,
		ScheduledAtNS:              capture.OptionalInt64{Value: scheduled, Valid: true},
		RequestStartedAtNS:         capture.OptionalInt64{Value: started, Valid: true},
		RequestCompletedAtNS:       capture.OptionalInt64{Value: completed, Valid: true},
		ReceivedWallTimeNS:         completed,
		ClockEpochID:               completedReading.ClockEpochID,
		MonotonicNSSinceClockEpoch: completedReading.MonotonicNS,
		SubscriptionOrRequestID:    capture.OptionalString{Value: requestID, Valid: true},
		HTTPStatusOrWSState:        capture.OptionalString{Value: strconv.Itoa(response.status), Valid: true},
		PayloadEncoding:            capture.PayloadEncodingJSON,
		TerminalOutcome:            capture.TerminalObserved,
		RecorderVersion:            r.config.RecorderVersion,
		Extensions:                 responseExtension,
	}
	responseEnvelope.SetRawPayload(response.body)
	if err := r.sink.WriteRaw(ctx, responseEnvelope); err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	if err := r.closeMetadataEpoch(ctx, epoch, 2, nil, started); err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	publication, ok := r.sink.Publication(epoch, responseEnvelope.ArrivalOrdinal)
	if !ok {
		return fmt.Errorf("%w: committed response segment coordinate is unavailable", ErrMetadataSync)
	}
	record, found, err := r.dependencies.Publications.FindRawSegment(ctx, publication.ObjectKey)
	if err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	if !found || record.State != catalog.RawSegmentCommitted ||
		record.SegmentID != publication.SegmentID ||
		record.ObjectKey != publication.ObjectKey ||
		record.ContentSHA256 != publication.ContentSHA256 ||
		record.ByteLength != publication.ByteLength {
		return fmt.Errorf("%w: publication catalog does not contain the exact committed response segment", ErrMetadataSync)
	}
	if err := r.dependencies.Evidence.RecordCommittedRESTEvidence(ctx, record, requestEnvelope, responseEnvelope); err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	if response.status != http.StatusOK {
		return fmt.Errorf("%w: exchangeInfo returned HTTP %d", ErrMetadataSync, response.status)
	}
	page, err := binance.CapturedPageFromEnvelopes(0, 1, binance.CommittedRawSegment{
		SegmentID:     record.SegmentID,
		ObjectKey:     record.ObjectKey,
		ContentSHA256: record.ContentSHA256,
		ByteLength:    record.ByteLength,
	}, requestEnvelope, responseEnvelope)
	if err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	composed, err := binance.ComposeScopedExchangeInfo([]binance.CapturedPage{page}, r.metadata.symbols(), binance.ComposeOptions{
		ExplicitLifecycleClosures: slices.Clone(r.config.ExplicitLifecycleClosures),
	}, binance.DefaultParserLimits())
	if err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	input, err := binance.SpotSyncInput(composed)
	if err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	if _, err := r.dependencies.Catalog.Sync(ctx, input); err != nil {
		return errors.Join(ErrMetadataSync, err)
	}
	r.counters.metadataSyncs.Add(1)
	return nil
}

func (r *Runtime) closeMetadataEpoch(ctx context.Context, epoch capture.StreamEpoch, last uint64, cause error, startedNS int64) error {
	reading := r.dependencies.Clock.Read()
	wall := postgresTimeNS(reading.WallTimeNS)
	reason := capture.ClosePlanned
	outcome := capture.TerminalObserved
	if cause != nil {
		reason = capture.CloseTransportFailure
		outcome = capture.TerminalFailed
		if errors.Is(cause, context.Canceled) {
			reason = capture.CloseCanceled
		}
	}
	base := capture.EnvelopeV1{
		SourceID:                   binance.SpotSourceID,
		ChannelOrEndpoint:          binance.SpotExchangeInfoChannel,
		PollCycleID:                capture.OptionalEpoch{Value: epoch.ID, Valid: true},
		ArrivalOrdinal:             last + 1,
		ReceivedWallTimeNS:         wall,
		ClockEpochID:               reading.ClockEpochID,
		MonotonicNSSinceClockEpoch: reading.MonotonicNS,
		PayloadEncoding:            capture.PayloadEncodingNone,
		TerminalOutcome:            outcome,
		RecorderVersion:            r.config.RecorderVersion,
	}
	terminal, err := capture.NewControlRecord(capture.ControlShutdown, base)
	if err != nil {
		return err
	}
	closeContext := ctx
	if ctx.Err() != nil {
		closeContext = context.WithoutCancel(ctx)
	}
	if err := r.sink.Commit(closeContext, capture.EpochCommit{SourceID: binance.SpotSourceID, Epoch: epoch, LastArrivalOrdinal: last}); err != nil {
		return err
	}
	close := capture.EpochClose{
		Commit:   capture.EpochCommit{SourceID: binance.SpotSourceID, Epoch: epoch, LastArrivalOrdinal: last + 1},
		Terminal: terminal,
		Reason:   reason,
	}
	if cause != nil {
		close.BlindInterval = &capture.BlindInterval{StartedWallTimeNS: startedNS, DetectedWallTimeNS: wall, Reason: reason}
	}
	return r.sink.CloseEpoch(closeContext, close)
}

func postgresTimeNS(value int64) int64 {
	if value <= 0 {
		return value
	}
	return value - value%int64(time.Microsecond)
}

var (
	_ EpochSource        = (*RandomEpochSource)(nil)
	_ CatalogSyncer      = (*catalog.Store)(nil)
	_ PublicationCatalog = (*catalog.PublicationStore)(nil)
	_ EvidenceRecorder   = (*catalog.QueryStore)(nil)
)
