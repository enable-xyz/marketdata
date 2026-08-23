package binance

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
)

const (
	USDMSourceID                   = "c768e25e-3c89-5a64-a9a2-e729c349173d"
	USDMAccessedAtNS               = int64(1787443200000000000)
	USDMPublicEndpoint             = "wss://fstream.binance.com/public"
	USDMMarketEndpoint             = "wss://fstream.binance.com/market"
	USDMRESTBase                   = "https://fapi.binance.com"
	USDMExchangeInfoPath           = "/fapi/v1/exchangeInfo"
	USDMDepthPath                  = "/fapi/v1/depth"
	USDMOpenInterestPath           = "/fapi/v1/openInterest"
	USDMConnectionDocumentationURI = "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/Connect.md"
	USDMBookDocumentationURI       = "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly.md"
	USDMRESTDocumentationURI       = "https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/rest-api/market-data"
	USDMStreamDocumentationURI     = "https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-usd-s-m-futures/api/ws-streams/public"
	USDMChangeLogURI               = "https://developers.binance.com/en/docs/products/derivatives-trading-usds-futures/change-log"
	USDMAggregateTradeCeilingNS    = uint64(100 * time.Millisecond)
	USDMLiquidationWindowNS        = uint64(time.Second)
	USDMOfficialStreamLimit        = 1024
	USDMMaxRawPayloadBytes         = 1 << 20
	USDMFAPIDefaultCapacity        = 2400
	USDMFAPIRefillIntervalNS       = uint64(time.Minute)
	USDMConnectionCapacity         = 300
	USDMConnectionRefillIntervalNS = uint64(5 * time.Minute)
)

var (
	ErrUSDMUnknownStream   = errors.New("binance: unknown USD-M public market-data stream")
	ErrUSDMWrongRoute      = errors.New("binance: USD-M stream is assigned to a different routed endpoint")
	ErrUSDMCandidateStream = errors.New("binance: USD-M candidate stream is not enabled")
	ErrUSDMPrivateEndpoint = errors.New("binance: private and trading endpoints are unsupported")
	ErrUSDMInvalidPoll     = errors.New("binance: invalid USD-M REST poll observation")
)

type USDMRoute string

const (
	USDMRoutePublic USDMRoute = "public"
	USDMRouteMarket USDMRoute = "market"
)

type USDMStreamRole string

const (
	USDMRoleDiffDepth                USDMStreamRole = "diff_depth_regular_book_excludes_rpi"
	USDMRoleNativeBBO                USDMStreamRole = "bookTicker_native_bbo_regular_book_excludes_rpi"
	USDMRoleAggregateTrade           USDMStreamRole = "aggregate_trade_100ms_same_price_taker_side"
	USDMRoleGenericTicker            USDMStreamRole = "24hrTicker_statistics_not_bbo"
	USDMRoleMarkIndexFunding         USDMStreamRole = "markPrice_state_mark_index_funding"
	USDMRoleIndexPrice               USDMStreamRole = "indexPrice_state"
	USDMRoleLargestLiquidationWindow USDMStreamRole = "forceOrder_largest_per_symbol_per_1000ms"
	USDMRoleRPIDepthCandidate        USDMStreamRole = "rpiDepth_candidate_separate_book"
)

type USDMStreamContract struct {
	Suffix               string
	Route                USDMRoute
	Role                 USDMStreamRole
	Support              capture.SupportLevel
	CadenceCeilingNS     uint64
	RPIInclusion         string
	Completeness         string
	CandidateDeclaration string
}

var usdmStreamAllowlist = []USDMStreamContract{
	{Suffix: "@depth", Route: USDMRoutePublic, Role: USDMRoleDiffDepth, Support: capture.SupportAvailable, RPIInclusion: "excluded"},
	{Suffix: "@depth@100ms", Route: USDMRoutePublic, Role: USDMRoleDiffDepth, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(100 * time.Millisecond), RPIInclusion: "excluded"},
	{Suffix: "@depth@250ms", Route: USDMRoutePublic, Role: USDMRoleDiffDepth, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(250 * time.Millisecond), RPIInclusion: "excluded"},
	{Suffix: "@depth@500ms", Route: USDMRoutePublic, Role: USDMRoleDiffDepth, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(500 * time.Millisecond), RPIInclusion: "excluded"},
	{Suffix: "@bookTicker", Route: USDMRoutePublic, Role: USDMRoleNativeBBO, Support: capture.SupportAvailable, RPIInclusion: "excluded"},
	{Suffix: "@aggTrade", Route: USDMRouteMarket, Role: USDMRoleAggregateTrade, Support: capture.SupportAvailable, CadenceCeilingNS: USDMAggregateTradeCeilingNS, RPIInclusion: "q_includes_source_aggregate_nq_excludes_rpi"},
	{Suffix: "@ticker", Route: USDMRouteMarket, Role: USDMRoleGenericTicker, Support: capture.SupportAvailable},
	{Suffix: "@markPrice", Route: USDMRouteMarket, Role: USDMRoleMarkIndexFunding, Support: capture.SupportAvailable},
	{Suffix: "@markPrice@1s", Route: USDMRouteMarket, Role: USDMRoleMarkIndexFunding, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(time.Second)},
	{Suffix: "@indexPrice", Route: USDMRouteMarket, Role: USDMRoleIndexPrice, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(time.Second)},
	{Suffix: "@forceOrder", Route: USDMRouteMarket, Role: USDMRoleLargestLiquidationWindow, Support: capture.SupportAvailable, CadenceCeilingNS: USDMLiquidationWindowNS, Completeness: "largest_in_window"},
	{Suffix: "@rpiDepth@500ms", Route: USDMRoutePublic, Role: USDMRoleRPIDepthCandidate, Support: capture.SupportAmbiguous, CadenceCeilingNS: uint64(500 * time.Millisecond), RPIInclusion: "rpi_only", CandidateDeclaration: "candidate route inferred from the dedicated order-book family; no live subscription until an access-dated routed payload and quantity-direction fixture proves it"},
}

func USDMStreamAllowlist() []USDMStreamContract {
	return slices.Clone(usdmStreamAllowlist)
}

func USDMStreamContractFor(stream string) (USDMStreamContract, error) {
	if stream == "" || strings.HasPrefix(stream, "!") {
		return USDMStreamContract{}, ErrUSDMUnknownStream
	}
	separator := strings.IndexByte(stream, '@')
	if separator <= 0 || separator == len(stream)-1 || stream[:separator] != strings.ToLower(stream[:separator]) {
		return USDMStreamContract{}, ErrUSDMUnknownStream
	}
	suffix := stream[separator:]
	for _, contract := range usdmStreamAllowlist {
		if suffix == contract.Suffix {
			return contract, nil
		}
	}
	return USDMStreamContract{}, ErrUSDMUnknownStream
}

func USDMValidateRoutedStream(route USDMRoute, stream string) (USDMStreamContract, error) {
	contract, err := USDMStreamContractFor(stream)
	if err != nil {
		return USDMStreamContract{}, err
	}
	if route != contract.Route {
		return contract, fmt.Errorf("%w: %s belongs on /%s", ErrUSDMWrongRoute, stream, contract.Route)
	}
	if contract.Support != capture.SupportAvailable {
		return contract, fmt.Errorf("%w: %s", ErrUSDMCandidateStream, stream)
	}
	return contract, nil
}

func USDMWebSocketURL(route USDMRoute, streams []string) (string, error) {
	if route != USDMRoutePublic && route != USDMRouteMarket {
		return "", ErrUSDMPrivateEndpoint
	}
	if len(streams) == 0 || len(streams) > USDMOfficialStreamLimit {
		return "", fmt.Errorf("%w: stream count must be within 1..%d", ErrUSDMUnknownStream, USDMOfficialStreamLimit)
	}
	seen := make(map[string]struct{}, len(streams))
	for _, stream := range streams {
		if _, ok := seen[stream]; ok {
			return "", fmt.Errorf("%w: duplicate stream %s", ErrUSDMUnknownStream, stream)
		}
		seen[stream] = struct{}{}
		if _, err := USDMValidateRoutedStream(route, stream); err != nil {
			return "", err
		}
	}
	base := USDMPublicEndpoint
	if route == USDMRouteMarket {
		base = USDMMarketEndpoint
	}
	return base + "/stream?streams=" + strings.Join(streams, "/"), nil
}

type USDMRESTOperation string

const (
	USDMRESTExchangeInfo USDMRESTOperation = "exchange_info"
	USDMRESTDepth        USDMRESTOperation = "depth_snapshot"
	USDMRESTOpenInterest USDMRESTOperation = "current_open_interest_poll"
)

func USDMRESTRequestWeight(operation USDMRESTOperation, depthLimit int) (uint32, error) {
	switch operation {
	case USDMRESTExchangeInfo, USDMRESTOpenInterest:
		return 1, nil
	case USDMRESTDepth:
		switch {
		case depthLimit >= 1 && depthLimit <= 50:
			return 2, nil
		case depthLimit <= 100:
			return 5, nil
		case depthLimit <= 500:
			return 10, nil
		case depthLimit <= 1000:
			return 20, nil
		default:
			return 0, fmt.Errorf("binance: unsupported USD-M depth limit %d", depthLimit)
		}
	default:
		return 0, fmt.Errorf("binance: unsupported USD-M REST operation %q", operation)
	}
}

type USDMRatePool struct {
	budget *capture.TokenRateBudget
}

func NewUSDMRatePool(capacity uint32, refillIntervalNS, initialMonotonicNS uint64) (*USDMRatePool, error) {
	if capacity == 0 || refillIntervalNS == 0 {
		return nil, errors.New("binance: USD-M FAPI rate capacity and interval must be positive")
	}
	policy := usdmFAPIRatePolicy(capacity, refillIntervalNS, 0)
	budget, err := capture.NewTokenRateBudget(policy, initialMonotonicNS)
	if err != nil {
		return nil, err
	}
	return &USDMRatePool{budget: budget}, nil
}

func NewUSDMDefaultRatePool(initialMonotonicNS uint64) (*USDMRatePool, error) {
	return NewUSDMRatePool(USDMFAPIDefaultCapacity, USDMFAPIRefillIntervalNS, initialMonotonicNS)
}

func (p *USDMRatePool) Acquire(nowMonotonicNS uint64, operation USDMRESTOperation, depthLimit int) (capture.BudgetDecision, error) {
	if p == nil || p.budget == nil {
		return capture.BudgetDecision{}, errors.New("binance: nil USD-M FAPI rate pool")
	}
	cost, err := USDMRESTRequestWeight(operation, depthLimit)
	if err != nil {
		return capture.BudgetDecision{}, err
	}
	return p.budget.Acquire(nowMonotonicNS, cost)
}

func (p *USDMRatePool) ObserveResponse(nowMonotonicNS uint64, status int, retryAfterNS uint64) (capture.ResponseDecision, error) {
	if p == nil || p.budget == nil {
		return capture.ResponseDecision{}, errors.New("binance: nil USD-M FAPI rate pool")
	}
	return p.budget.ObserveResponse(nowMonotonicNS, status, retryAfterNS)
}

func usdmFAPIRatePolicy(capacity uint32, refillIntervalNS uint64, requestCost uint32) capture.RatePolicy {
	return capture.RatePolicy{
		Capacity: capacity, RefillTokens: capacity, RefillIntervalNS: refillIntervalNS, RequestCost: requestCost,
		MaxAttempts: 3, DefaultRetryAfterNS: uint64(time.Second), MaxRetryAfterNS: uint64(3 * 24 * time.Hour), CircuitOpenNS: uint64(3 * 24 * time.Hour),
		RetryableStatusCodes: []int{429}, Retryable5XX: true, TerminalStatusCodes: []int{403}, CircuitStatusCodes: []int{418},
	}
}

func USDMPublicSourceContract() capture.SourceContract {
	return usdmWebSocketSourceContract(USDMRoutePublic)
}

func USDMMarketSourceContract() capture.SourceContract {
	return usdmWebSocketSourceContract(USDMRouteMarket)
}

func usdmWebSocketSourceContract(route USDMRoute) capture.SourceContract {
	capabilities := make([]capture.Capability, 0, len(usdmStreamAllowlist)+2)
	for _, stream := range usdmStreamAllowlist {
		if stream.Route != route {
			continue
		}
		capability := capture.Capability{ChannelOrEndpoint: stream.Suffix, DataFamily: string(stream.Role), Entitlement: "public", Support: stream.Support}
		if stream.Support != capture.SupportAvailable {
			capability.Declaration = stream.CandidateDeclaration
		}
		capabilities = append(capabilities, capability)
	}
	capabilities = append(capabilities,
		capture.Capability{ChannelOrEndpoint: "private", DataFamily: "user-data", Entitlement: "credentials", Support: capture.SupportUnsupported, Declaration: "private routed endpoints are outside the public market-data adapter"},
		capture.Capability{ChannelOrEndpoint: "trading", DataFamily: "orders", Entitlement: "credentials", Support: capture.SupportUnsupported, Declaration: "trading methods do not exist in the USD-M public adapter"},
	)
	return capture.SourceContract{
		Version: capture.SourceContractVersion, SourceID: USDMSourceID, ContractID: "binance.usdm.ws." + string(route) + ".v1", APIVersion: "Binance USD-M Futures WebSocket Market Streams, accessed 2026-08-23",
		Documentation: []capture.DocumentationRef{
			{URL: USDMConnectionDocumentationURI, AccessedAtNS: USDMAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: USDMStreamDocumentationURI, AccessedAtNS: USDMAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: USDMChangeLogURI, AccessedAtNS: USDMAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: capabilities,
		Topology:     capture.ConnectionTopology{Transport: capture.TransportWebSocket, MaxConnections: 1, MaxSubscriptions: USDMOfficialStreamLimit},
		Subscription: capture.SubscriptionPolicy{ACKMode: capture.ACKNone},
		Heartbeat:    capture.HeartbeatPolicy{Mode: capture.HeartbeatPingPong, IntervalNS: uint64(3 * time.Minute), TimeoutNS: uint64(10 * time.Minute)},
		Rate:         capture.RatePolicy{Capacity: USDMConnectionCapacity, RefillTokens: USDMConnectionCapacity, RefillIntervalNS: USDMConnectionRefillIntervalNS, ConnectionCost: 1, MaxAttempts: 1, DefaultRetryAfterNS: uint64(time.Second), MaxRetryAfterNS: USDMConnectionRefillIntervalNS, CircuitOpenNS: USDMConnectionRefillIntervalNS},
		Payload:      capture.PayloadPolicy{MaxRawBytes: USDMMaxRawPayloadBytes, MaxSchemaDepth: 32, MaxSchemaFields: 512, MaxArrayElements: 20_000},
	}
}

func USDMRESTSourceContract(operation USDMRESTOperation, depthLimit int) (capture.SourceContract, error) {
	weight, err := USDMRESTRequestWeight(operation, depthLimit)
	if err != nil {
		return capture.SourceContract{}, err
	}
	endpoint := USDMExchangeInfoPath
	family := "instrument-metadata"
	switch operation {
	case USDMRESTDepth:
		endpoint, family = USDMDepthPath, "book-snapshot"
	case USDMRESTOpenInterest:
		endpoint, family = USDMOpenInterestPath, "current-open-interest-rest-observation"
	}
	return capture.SourceContract{
		Version: capture.SourceContractVersion, SourceID: USDMSourceID, ContractID: "binance.usdm.rest." + string(operation) + ".v1", APIVersion: "Binance USD-M Futures FAPI v1, accessed 2026-08-23",
		Documentation: []capture.DocumentationRef{{URL: USDMRESTDocumentationURI, AccessedAtNS: USDMAccessedAtNS, Authority: capture.RuleOfficialDocumentation}},
		Capabilities: []capture.Capability{
			{ChannelOrEndpoint: endpoint, DataFamily: family, Entitlement: "public", Support: capture.SupportAvailable},
			{ChannelOrEndpoint: "private-rest", DataFamily: "private", Entitlement: "credentials", Support: capture.SupportUnsupported, Declaration: "authenticated REST methods are outside the public USD-M adapter"},
		},
		Topology: capture.ConnectionTopology{Transport: capture.TransportREST, MaxConnections: 1, Throttleable: true},
		Rate:     usdmFAPIRatePolicy(USDMFAPIDefaultCapacity, USDMFAPIRefillIntervalNS, weight),
		Payload:  capture.PayloadPolicy{MaxRawBytes: USDMMaxRawPayloadBytes, MaxSchemaDepth: 32, MaxSchemaFields: 4096, MaxArrayElements: 20_000},
	}, nil
}

func USDMCatalogContract() (catalog.Source, catalog.SourceVersion, []catalog.ChannelContract) {
	source := catalog.Source{SourceID: USDMSourceID, Venue: "binance", ProductFamily: "usdm", APIFamily: "fapi-v1", Environment: "production-public", Lifecycle: "active"}
	version := catalog.SourceVersion{
		OfficialAPIVersion: "Binance USD-M Futures FAPI v1, accessed 2026-08-23", DocumentationURI: USDMRESTDocumentationURI,
		Endpoints:   mustJSON(map[string]any{"rest_base": USDMRESTBase, "exchange_info": USDMExchangeInfoPath, "depth": USDMDepthPath, "open_interest": USDMOpenInterestPath}),
		Topology:    mustJSON(map[string]any{"websocket_public": USDMPublicEndpoint, "websocket_market": USDMMarketEndpoint, "wrong_route_may_be_silently_useless": true}),
		Entitlement: mustJSON(map[string]any{"security_type": "NONE", "credentials_required": false, "private_endpoints": "unsupported"}), Region: "global-public",
		RateContract:          mustJSON(map[string]any{"pool": "shared_fapi_dapi_request_weight", "response_headers_owned_by_pool": true, "binance_terminal_statuses": []int{429, 418}}),
		HeartbeatPolicy:       mustJSON(map[string]any{"server_ping_interval_seconds": 180, "pong_deadline_seconds": 600}),
		AcknowledgementPolicy: mustJSON(map[string]any{"url_subscription": true}),
		ReconnectPolicy:       mustJSON(map[string]any{"socket_lifetime_hours": 24, "book_pu_mismatch_closes_epoch": true}),
	}
	channels := []catalog.ChannelContract{
		{ChannelID: "rest.exchangeInfo.fapi.v1", NativeSelector: mustJSON(map[string]any{"method": "GET", "path": USDMExchangeInfoPath, "security_type": "NONE"}), Role: "instrument_metadata", DataFamily: "catalog", CadenceSource: "caller-scheduled REST opportunity", Aggregation: mustJSON(map[string]any{"kind": "complete_response", "temporary_absence_closes_lifecycle": false}), Depth: mustJSON(map[string]any{"applicable": false}), SequenceRules: mustJSON(map[string]any{"applicable": false}), ChecksumRules: mustJSON(map[string]any{"raw_payload": "sha256"}), PayloadSchema: mustJSON(map[string]any{"unknown_fields": "retained_in_raw_payload", "decimal_representation": "exact_JSON_string"}), SupportState: "supported", Limitation: "lifecycle closes only from an observed transition, never temporary absence"},
		{ChannelID: "rest.openInterest.fapi.v1", NativeSelector: mustJSON(map[string]any{"method": "GET", "path": USDMOpenInterestPath, "security_type": "NONE", "symbol_required": true}), Role: "current_open_interest_rest_observation", DataFamily: "derivative-ticker", CadenceSource: "caller-scheduled poll cycle", Aggregation: mustJSON(map[string]any{"kind": "single_current_observation", "historical_series": false}), Depth: mustJSON(map[string]any{"applicable": false}), SequenceRules: mustJSON(map[string]any{"poll_cycle_epoch": true}), ChecksumRules: mustJSON(map[string]any{"raw_payload": "sha256"}), PayloadSchema: mustJSON(map[string]any{"native_value": "openInterest", "sidedness": "unspecified", "conversion": "none_without_temporal_multiplier_formula"}), SupportState: "supported", Limitation: "poll-only current OI; each response remains a REST observation and is not relabelled as a stream event"},
	}
	return source, version, channels
}
