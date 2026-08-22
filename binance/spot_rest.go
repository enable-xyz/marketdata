package binance

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/enable-xyz/marketdata/capture"
)

const SpotMaxResponseHeaders = 32

var ErrSpotRESTState = errors.New("binance: invalid Spot REST transport state")

type SpotVenueRateBudget struct {
	connections capture.RateBudget
	requests    capture.RateBudget
}

func NewSpotVenueRateBudget(initialMonotonicNS uint64) (*SpotVenueRateBudget, error) {
	wsPolicy := SpotWSSourceContract().Rate
	depthContract, err := SpotDepthSourceContract(SpotDepthLimitDefault)
	if err != nil {
		return nil, err
	}
	connections, err := capture.NewTokenRateBudget(wsPolicy, initialMonotonicNS)
	if err != nil {
		return nil, err
	}
	requests, err := capture.NewTokenRateBudget(depthContract.Rate, initialMonotonicNS)
	if err != nil {
		return nil, err
	}
	return &SpotVenueRateBudget{connections: connections, requests: requests}, nil
}

func (b *SpotVenueRateBudget) Acquire(nowMonotonicNS uint64, cost uint32) (capture.BudgetDecision, error) {
	if cost == 1 {
		return b.connections.Acquire(nowMonotonicNS, cost)
	}
	return b.requests.Acquire(nowMonotonicNS, cost)
}

func (b *SpotVenueRateBudget) ObserveResponse(nowMonotonicNS uint64, status int, retryAfterNS uint64) (capture.ResponseDecision, error) {
	return b.requests.ObserveResponse(nowMonotonicNS, status, retryAfterNS)
}

type SpotDepthRequest struct {
	RequestID  string
	Method     capture.RESTMethod
	Endpoint   string
	Symbol     string
	Limit      int
	Parameters []capture.SanitizedParameter
	Headers    []capture.RESTHeader
}

func NewSpotDepthRequest(requestID, symbol string, limit int, microsecondTime bool) (SpotDepthRequest, error) {
	if requestID == "" || len(requestID) > capture.MaxIdentityBytes || !utf8.ValidString(requestID) || strings.ContainsAny(requestID, "\x00\r\n") {
		return SpotDepthRequest{}, fmt.Errorf("%w: request ID is invalid", ErrSpotConfiguration)
	}
	if symbol == "" || len(symbol) > capture.MaxSymbolBytes || !utf8.ValidString(symbol) || strings.TrimSpace(symbol) != symbol || strings.ContainsAny(symbol, "/?&#\x00\r\n\t") {
		return SpotDepthRequest{}, fmt.Errorf("%w: native symbol is invalid", ErrSpotConfiguration)
	}
	if _, err := SpotDepthRequestWeight(limit); err != nil {
		return SpotDepthRequest{}, err
	}
	request := SpotDepthRequest{
		RequestID: requestID,
		Method:    capture.RESTMethodGET,
		Endpoint:  SpotDepthEndpoint,
		Symbol:    symbol,
		Limit:     limit,
		Parameters: []capture.SanitizedParameter{
			{Name: "limit", Value: strconv.Itoa(limit)},
			{Name: "symbol", Value: symbol},
		},
	}
	if microsecondTime {
		request.Headers = []capture.RESTHeader{{Kind: capture.RESTHeaderTimeUnit, Value: "MICROSECOND"}}
	}
	return request, nil
}

func SpotDepthRequestWeight(limit int) (uint32, error) {
	switch {
	case limit >= 1 && limit <= 100:
		return 5, nil
	case limit >= 101 && limit <= 500:
		return 25, nil
	case limit >= 501 && limit <= 1000:
		return 50, nil
	case limit >= 1001 && limit <= SpotDepthLimitMaximum:
		return 250, nil
	default:
		return 0, fmt.Errorf("%w: depth limit must be within 1..%d", ErrSpotBounds, SpotDepthLimitMaximum)
	}
}

type SpotHTTPHeader struct {
	Name  string
	Value string
}

type SpotRESTResponse struct {
	Status  int
	Headers []SpotHTTPHeader
	Body    []byte
}

// SpotRESTClient executes only the caller-provided public request. It must
// enforce maxResponseBytes while reading the body and must not follow a
// redirect to a different method, scheme, host, or endpoint.
type SpotRESTClient interface {
	Do(context.Context, SpotDepthRequest, uint32) (SpotRESTResponse, error)
}

type SpotDepthConfig struct {
	Request         SpotDepthRequest
	RecorderVersion string
	Epoch           capture.StreamEpoch
	ScheduledAtNS   int64
}

type SpotDepthCapture struct {
	request   SpotDepthRequest
	runner    *capture.Runner
	transport *spotRESTTransport
	started   bool
	closed    bool
}

func NewSpotDepthCapture(config SpotDepthConfig, client SpotRESTClient, clock capture.Clock, budget capture.RateBudget, sink capture.RawSink) (*SpotDepthCapture, error) {
	if client == nil || clock == nil || budget == nil || sink == nil {
		return nil, fmt.Errorf("%w: REST client, clock, rate budget, and raw sink are required", ErrSpotConfiguration)
	}
	if config.RecorderVersion == "" || len(config.RecorderVersion) > capture.MaxRecorderVersionBytes || config.ScheduledAtNS <= 0 {
		return nil, fmt.Errorf("%w: recorder version and scheduled time are required", ErrSpotConfiguration)
	}
	if err := config.Epoch.Validate(); err != nil || config.Epoch.Kind != capture.EpochPollCycle {
		return nil, fmt.Errorf("%w: REST epoch must be a poll cycle", ErrSpotConfiguration)
	}
	request, err := cloneAndValidateDepthRequest(config.Request)
	if err != nil {
		return nil, err
	}
	contract, err := SpotDepthSourceContract(request.Limit)
	if err != nil {
		return nil, err
	}
	transport := &spotRESTTransport{client: client, request: request, state: spotRESTEmitRequest}
	runner, err := capture.NewRunner(contract, capture.RunnerConfig{
		Epoch:             config.Epoch,
		ChannelOrEndpoint: SpotDepthChannel,
		DataFamily:        SpotDepthDataFamily,
		RecorderVersion:   config.RecorderVersion,
		NativeSymbol:      capture.OptionalString{Value: request.Symbol, Valid: true},
		ScheduledAtNS:     capture.OptionalInt64{Value: config.ScheduledAtNS, Valid: true},
	}, transport, clock, budget, sink, newSpotDepthObserver())
	if err != nil {
		return nil, err
	}
	return &SpotDepthCapture{request: request, runner: runner, transport: transport}, nil
}

func (c *SpotDepthCapture) Request() SpotDepthRequest {
	request, _ := cloneAndValidateDepthRequest(c.request)
	return request
}

func (c *SpotDepthCapture) Start(ctx context.Context) (capture.StepResult, error) {
	if c.started {
		return capture.StepResult{}, fmt.Errorf("%w: depth capture already started", ErrSpotConfiguration)
	}
	c.started = true
	return c.runner.Start(ctx)
}

func (c *SpotDepthCapture) Step(ctx context.Context) (capture.StepResult, error) {
	if !c.started {
		return capture.StepResult{}, capture.ErrRunnerNotStarted
	}
	if c.closed {
		return capture.StepResult{}, capture.ErrRunnerClosed
	}
	result, err := c.runner.Step(ctx)
	for _, control := range result.Controls {
		switch control.Envelope.ControlKind.Value {
		case capture.ControlRequestStarted:
			c.transport.allowResponse()
		case capture.ControlRateLimited:
			c.transport.retryRequest()
		}
	}
	if result.State == capture.RunnerClosed {
		c.closed = true
	}
	return result, err
}

type spotRESTState uint8

const (
	spotRESTEmitRequest spotRESTState = iota + 1
	spotRESTAwaitDecision
	spotRESTReadResponse
	spotRESTClosed
)

type spotRESTTransport struct {
	client  SpotRESTClient
	request SpotDepthRequest
	state   spotRESTState
}

func (t *spotRESTTransport) Next(ctx context.Context) (capture.TransportEvent, error) {
	if err := ctx.Err(); err != nil {
		return capture.TransportEvent{}, err
	}
	switch t.state {
	case spotRESTEmitRequest:
		t.state = spotRESTAwaitDecision
		return capture.TransportEvent{
			Kind:                capture.TransportEventRequest,
			RequestID:           t.request.RequestID,
			Method:              t.request.Method,
			SanitizedParameters: slices.Clone(t.request.Parameters),
			RequestHeaders:      slices.Clone(t.request.Headers),
		}, nil
	case spotRESTAwaitDecision:
		return capture.TransportEvent{}, ErrSpotRESTState
	case spotRESTReadResponse:
		response, err := t.client.Do(ctx, t.request, SpotMaxRawPayloadBytes)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return capture.TransportEvent{}, err
			}
			t.state = spotRESTEmitRequest
			return capture.TransportEvent{Kind: capture.TransportEventFailure, Failure: capture.TransportFailureRequest}, nil
		}
		event, err := spotResponseEvent(t.request.RequestID, response)
		t.state = spotRESTEmitRequest
		if err != nil {
			if len(response.Body) > SpotMaxRawPayloadBytes {
				return capture.TransportEvent{}, err
			}
			status, headers, retryAfterNS := spotPartialResponseMetadata(response)
			return capture.TransportEvent{
				Kind:            capture.TransportEventHTTPResponse,
				Raw:             append([]byte(nil), response.Body...),
				Encoding:        capture.PayloadEncodingJSON,
				HTTPStatus:      status,
				RetryAfterNS:    retryAfterNS,
				RequestID:       t.request.RequestID,
				ResponseHeaders: headers,
				AfterRawFailure: capture.TransportFailureResponseEvidence,
			}, nil
		}
		return event, nil
	case spotRESTClosed:
		return capture.TransportEvent{}, capture.ErrTransportClosed
	default:
		return capture.TransportEvent{}, ErrSpotRESTState
	}
}

func (t *spotRESTTransport) Close(ctx context.Context, _ capture.CloseReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	t.state = spotRESTClosed
	return nil
}

func (t *spotRESTTransport) allowResponse() {
	if t.state == spotRESTAwaitDecision {
		t.state = spotRESTReadResponse
	}
}

func spotResponseEvent(requestID string, response SpotRESTResponse) (capture.TransportEvent, error) {
	if response.Status < 100 || response.Status > 599 {
		return capture.TransportEvent{}, fmt.Errorf("%w: HTTP status %d", ErrSpotConfiguration, response.Status)
	}
	if len(response.Body) > SpotMaxRawPayloadBytes {
		return capture.TransportEvent{}, fmt.Errorf("%w: REST body has %d bytes, maximum is %d", ErrSpotBounds, len(response.Body), SpotMaxRawPayloadBytes)
	}
	if len(response.Headers) > SpotMaxResponseHeaders {
		return capture.TransportEvent{}, fmt.Errorf("%w: REST response has %d headers, maximum is %d", ErrSpotBounds, len(response.Headers), SpotMaxResponseHeaders)
	}
	headers, retryAfterNS, err := sanitizeSpotResponseHeaders(response.Headers)
	if err != nil {
		return capture.TransportEvent{}, err
	}
	return capture.TransportEvent{
		Kind:            capture.TransportEventHTTPResponse,
		Raw:             append([]byte(nil), response.Body...),
		Encoding:        capture.PayloadEncodingJSON,
		HTTPStatus:      response.Status,
		RetryAfterNS:    retryAfterNS,
		RequestID:       requestID,
		ResponseHeaders: headers,
	}, nil
}

func spotPartialResponseMetadata(response SpotRESTResponse) (int, []capture.RESTHeader, uint64) {
	if response.Status < 100 || response.Status > 599 {
		return 0, nil, 0
	}
	limit := min(len(response.Headers), SpotMaxResponseHeaders)
	output := make([]capture.RESTHeader, 0, limit)
	seen := make(map[capture.RESTHeaderKind]struct{}, limit)
	usedWeight := make([]string, 0, 4)
	var retryAfterNS uint64
	for _, input := range response.Headers[:limit] {
		headers, retry, err := sanitizeSpotResponseHeaders([]SpotHTTPHeader{input})
		if err != nil {
			continue
		}
		for _, header := range headers {
			if header.Kind == capture.RESTHeaderUsedWeight {
				usedWeight = append(usedWeight, header.Value)
				continue
			}
			if _, duplicate := seen[header.Kind]; duplicate {
				continue
			}
			seen[header.Kind] = struct{}{}
			output = append(output, header)
			if header.Kind == capture.RESTHeaderRetryAfter {
				retryAfterNS = retry
			}
		}
	}
	if len(usedWeight) != 0 {
		slices.Sort(usedWeight)
		value := strings.Join(usedWeight, ";")
		if len(value) <= capture.MaxRESTHeaderValueBytes {
			output = append(output, capture.RESTHeader{Kind: capture.RESTHeaderUsedWeight, Value: value})
		}
	}
	slices.SortFunc(output, func(a, b capture.RESTHeader) int { return strings.Compare(string(a.Kind), string(b.Kind)) })
	return response.Status, output, retryAfterNS
}

func (t *spotRESTTransport) retryRequest() {
	if t.state == spotRESTAwaitDecision {
		t.state = spotRESTEmitRequest
	}
}

func sanitizeSpotResponseHeaders(input []SpotHTTPHeader) ([]capture.RESTHeader, uint64, error) {
	var output []capture.RESTHeader
	usedWeight := make([]string, 0, 4)
	seen := make(map[capture.RESTHeaderKind]struct{})
	var retryAfterNS uint64
	for _, header := range input {
		if header.Name == "" || len(header.Name) > capture.MaxRESTParameterNameBytes || len(header.Value) > capture.MaxRESTHeaderValueBytes || !utf8.ValidString(header.Name) || !utf8.ValidString(header.Value) || strings.ContainsAny(header.Name+header.Value, "\x00\r\n") {
			return nil, 0, fmt.Errorf("%w: invalid REST response header", ErrSpotConfiguration)
		}
		name := strings.ToLower(header.Name)
		var kind capture.RESTHeaderKind
		switch {
		case name == "retry-after":
			kind = capture.RESTHeaderRetryAfter
			seconds, err := strconv.ParseUint(header.Value, 10, 64)
			if err != nil {
				return nil, 0, fmt.Errorf("%w: Retry-After is not an integer number of seconds", ErrSpotConfiguration)
			}
			if seconds > math.MaxUint64/1_000_000_000 {
				retryAfterNS = math.MaxUint64
			} else {
				retryAfterNS = seconds * 1_000_000_000
			}
		case name == "content-type":
			kind = capture.RESTHeaderContentType
		case name == "content-length":
			kind = capture.RESTHeaderContentLength
		case strings.HasPrefix(name, "x-mbx-used-weight-"):
			usedWeight = append(usedWeight, name+"="+header.Value)
			continue
		default:
			continue
		}
		if _, duplicate := seen[kind]; duplicate {
			return nil, 0, fmt.Errorf("%w: duplicate allowlisted response header %q", ErrSpotConfiguration, name)
		}
		seen[kind] = struct{}{}
		output = append(output, capture.RESTHeader{Kind: kind, Value: header.Value})
	}
	if len(usedWeight) != 0 {
		slices.Sort(usedWeight)
		value := strings.Join(usedWeight, ";")
		if len(value) > capture.MaxRESTHeaderValueBytes {
			return nil, 0, fmt.Errorf("%w: used-weight evidence exceeds its bound", ErrSpotBounds)
		}
		output = append(output, capture.RESTHeader{Kind: capture.RESTHeaderUsedWeight, Value: value})
	}
	slices.SortFunc(output, func(a, b capture.RESTHeader) int { return strings.Compare(string(a.Kind), string(b.Kind)) })
	return output, retryAfterNS, nil
}

func cloneAndValidateDepthRequest(request SpotDepthRequest) (SpotDepthRequest, error) {
	canonical, err := NewSpotDepthRequest(request.RequestID, request.Symbol, request.Limit, len(request.Headers) != 0)
	if err != nil {
		return SpotDepthRequest{}, err
	}
	if request.Method != canonical.Method || request.Endpoint != canonical.Endpoint || !slices.Equal(request.Parameters, canonical.Parameters) || !slices.Equal(request.Headers, canonical.Headers) {
		return SpotDepthRequest{}, fmt.Errorf("%w: depth request is not canonical", ErrSpotConfiguration)
	}
	return canonical, nil
}
