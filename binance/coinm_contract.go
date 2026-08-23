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
	CoinMSourceID                   = "073c210e-04b7-527b-b9b8-1d144341a534"
	CoinMAccessedAtNS               = int64(1787443200000000000)
	CoinMWebSocketEndpoint          = "wss://dstream.binance.com"
	CoinMRESTBase                   = "https://dapi.binance.com"
	CoinMExchangeInfoPath           = "/dapi/v1/exchangeInfo"
	CoinMDepthPath                  = "/dapi/v1/depth"
	CoinMOpenInterestPath           = "/dapi/v1/openInterest"
	CoinMConnectionDocumentationURI = "https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/Connect.md"
	CoinMBookDocumentationURI       = "https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/websocket-market-streams/How-to-manage-a-local-order-book-correctly.md"
	CoinMIntegrationNoticeURI       = "https://developers.binance.com/en/docs/products/derivatives-trading-coin-futures/Important-CM-UM-Integration-Notice.md"
	CoinMRESTDocumentationURI       = "https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/rest-api/market-data"
	CoinMStreamDocumentationURI     = "https://developers.binance.com/en/docs/catalog/core-trading-derivatives-trading-coin-m-futures/api/ws-streams/~"
	CoinMOfficialStreamLimit        = 1024
	CoinMMaxRawPayloadBytes         = 1 << 20
	CoinMMaxMergedRecords           = 20_000
	CoinMSharedWeightCapacity       = 2400
	CoinMSharedWeightIntervalNS     = uint64(time.Minute)
)

var (
	ErrCoinMUnknownStream        = errors.New("binance: unknown COIN-M public market-data stream")
	ErrCoinMPrivateEndpoint      = errors.New("binance: private and trading COIN-M endpoints are unsupported")
	ErrCoinMInvalidRoute         = errors.New("binance: invalid merged futures route")
	ErrCoinMInvalidMarketPayload = errors.New("binance: invalid COIN-M market payload")
	ErrCoinMInvalidPoll          = errors.New("binance: invalid COIN-M REST poll observation")
)

type CoinMStreamRole string

const (
	CoinMRoleDiffDepth        CoinMStreamRole = "diff_depth_native_contract_amount"
	CoinMRoleNativeBBO        CoinMStreamRole = "bookTicker_native_contract_amount"
	CoinMRoleAggregateTrade   CoinMStreamRole = "aggregate_trade_native_contract_amount"
	CoinMRoleGenericTicker    CoinMStreamRole = "24hrTicker_contract_and_base_volumes"
	CoinMRoleMarkIndexFunding CoinMStreamRole = "markPrice_mark_index_delivery_funding_state"
	CoinMRoleMergedAllMarket  CoinMStreamRole = "merged_um_cm_all_market_route_by_native_st"
)

type CoinMStreamContract struct {
	Selector         string
	Role             CoinMStreamRole
	Support          capture.SupportLevel
	CadenceCeilingNS uint64
	MergedUniverse   bool
	NativeAmountUnit string
}

var coinMStreamAllowlist = []CoinMStreamContract{
	{Selector: "@depth", Role: CoinMRoleDiffDepth, Support: capture.SupportAvailable, NativeAmountUnit: "contracts"},
	{Selector: "@depth@100ms", Role: CoinMRoleDiffDepth, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(100 * time.Millisecond), NativeAmountUnit: "contracts"},
	{Selector: "@depth@250ms", Role: CoinMRoleDiffDepth, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(250 * time.Millisecond), NativeAmountUnit: "contracts"},
	{Selector: "@depth@500ms", Role: CoinMRoleDiffDepth, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(500 * time.Millisecond), NativeAmountUnit: "contracts"},
	{Selector: "@bookTicker", Role: CoinMRoleNativeBBO, Support: capture.SupportAvailable, NativeAmountUnit: "contracts"},
	{Selector: "@aggTrade", Role: CoinMRoleAggregateTrade, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(100 * time.Millisecond), NativeAmountUnit: "contracts"},
	{Selector: "@ticker", Role: CoinMRoleGenericTicker, Support: capture.SupportAvailable, NativeAmountUnit: "v=contracts,q=base_asset"},
	{Selector: "@markPrice", Role: CoinMRoleMarkIndexFunding, Support: capture.SupportAvailable},
	{Selector: "@markPrice@1s", Role: CoinMRoleMarkIndexFunding, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(time.Second)},
	{Selector: "!ticker@arr", Role: CoinMRoleMergedAllMarket, Support: capture.SupportAvailable, MergedUniverse: true, NativeAmountUnit: "st=2 selects COIN-M"},
	{Selector: "!miniTicker@arr", Role: CoinMRoleMergedAllMarket, Support: capture.SupportAvailable, MergedUniverse: true, NativeAmountUnit: "st=2 selects COIN-M"},
	{Selector: "!bookTicker", Role: CoinMRoleMergedAllMarket, Support: capture.SupportAvailable, MergedUniverse: true, NativeAmountUnit: "st=2 selects COIN-M"},
	{Selector: "!forceOrder@arr", Role: CoinMRoleMergedAllMarket, Support: capture.SupportAvailable, MergedUniverse: true, NativeAmountUnit: "st=2 selects COIN-M"},
	{Selector: "!contractInfo", Role: CoinMRoleMergedAllMarket, Support: capture.SupportAvailable, MergedUniverse: true, NativeAmountUnit: "st=2 selects COIN-M"},
	{Selector: "!markPrice@arr", Role: CoinMRoleMergedAllMarket, Support: capture.SupportAvailable, MergedUniverse: true, NativeAmountUnit: "st=2 selects COIN-M"},
	{Selector: "!markPrice@arr@1s", Role: CoinMRoleMergedAllMarket, Support: capture.SupportAvailable, CadenceCeilingNS: uint64(time.Second), MergedUniverse: true, NativeAmountUnit: "st=2 selects COIN-M"},
}

func CoinMStreamAllowlist() []CoinMStreamContract {
	return slices.Clone(coinMStreamAllowlist)
}

func CoinMStreamContractFor(stream string) (CoinMStreamContract, error) {
	if stream == "" {
		return CoinMStreamContract{}, ErrCoinMUnknownStream
	}
	if strings.HasPrefix(stream, "!") {
		for _, contract := range coinMStreamAllowlist {
			if contract.MergedUniverse && stream == contract.Selector {
				return contract, nil
			}
		}
		return CoinMStreamContract{}, ErrCoinMUnknownStream
	}
	separator := strings.IndexByte(stream, '@')
	if separator <= 0 || separator == len(stream)-1 || stream[:separator] != strings.ToLower(stream[:separator]) {
		return CoinMStreamContract{}, ErrCoinMUnknownStream
	}
	for _, contract := range coinMStreamAllowlist {
		if !contract.MergedUniverse && stream[separator:] == contract.Selector {
			return contract, nil
		}
	}
	return CoinMStreamContract{}, ErrCoinMUnknownStream
}

func CoinMWebSocketURL(streams []string) (string, error) {
	if len(streams) == 0 || len(streams) > CoinMOfficialStreamLimit {
		return "", fmt.Errorf("%w: stream count must be within 1..%d", ErrCoinMUnknownStream, CoinMOfficialStreamLimit)
	}
	seen := make(map[string]struct{}, len(streams))
	for _, stream := range streams {
		if _, ok := seen[stream]; ok {
			return "", fmt.Errorf("%w: duplicate stream %s", ErrCoinMUnknownStream, stream)
		}
		seen[stream] = struct{}{}
		if _, err := CoinMStreamContractFor(stream); err != nil {
			return "", err
		}
	}
	return CoinMWebSocketEndpoint + "/stream?streams=" + strings.Join(streams, "/"), nil
}

type CoinMRESTOperation string

const (
	CoinMRESTExchangeInfo CoinMRESTOperation = "exchange_info"
	CoinMRESTDepth        CoinMRESTOperation = "depth_snapshot"
	CoinMRESTOpenInterest CoinMRESTOperation = "current_open_interest_poll"
)

func CoinMRESTRequestWeight(operation CoinMRESTOperation, depthLimit int) (uint32, error) {
	switch operation {
	case CoinMRESTExchangeInfo, CoinMRESTOpenInterest:
		return 1, nil
	case CoinMRESTDepth:
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
			return 0, fmt.Errorf("binance: unsupported COIN-M depth limit %d", depthLimit)
		}
	default:
		return 0, fmt.Errorf("binance: unsupported COIN-M REST operation %q", operation)
	}
}

func CoinMSourceContract() capture.SourceContract {
	capabilities := make([]capture.Capability, 0, len(coinMStreamAllowlist)+2)
	for _, stream := range coinMStreamAllowlist {
		capabilities = append(capabilities, capture.Capability{
			ChannelOrEndpoint: stream.Selector,
			DataFamily:        string(stream.Role),
			Entitlement:       "public",
			Support:           stream.Support,
		})
	}
	capabilities = append(capabilities,
		capture.Capability{ChannelOrEndpoint: "private", DataFamily: "user-data", Entitlement: "credentials", Support: capture.SupportUnsupported, Declaration: "private streams are outside the public COIN-M adapter"},
		capture.Capability{ChannelOrEndpoint: "trading", DataFamily: "orders", Entitlement: "credentials", Support: capture.SupportUnsupported, Declaration: "trading methods do not exist in the COIN-M public adapter"},
	)
	return capture.SourceContract{
		Version:    capture.SourceContractVersion,
		SourceID:   CoinMSourceID,
		ContractID: "binance.coinm.ws.v1",
		APIVersion: "Binance COIN-M Futures DAPI v1 and market streams, accessed 2026-08-23",
		Documentation: []capture.DocumentationRef{
			{URL: CoinMConnectionDocumentationURI, AccessedAtNS: CoinMAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: CoinMIntegrationNoticeURI, AccessedAtNS: CoinMAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: CoinMStreamDocumentationURI, AccessedAtNS: CoinMAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: capabilities,
		Topology:     capture.ConnectionTopology{Transport: capture.TransportWebSocket, MaxConnections: 1, MaxSubscriptions: CoinMOfficialStreamLimit},
		Subscription: capture.SubscriptionPolicy{ACKMode: capture.ACKNone},
		Heartbeat:    capture.HeartbeatPolicy{Mode: capture.HeartbeatPingPong, IntervalNS: uint64(3 * time.Minute), TimeoutNS: uint64(10 * time.Minute)},
		Rate:         capture.RatePolicy{Capacity: 10, RefillTokens: 10, RefillIntervalNS: uint64(time.Second), ConnectionCost: 1, MaxAttempts: 1, DefaultRetryAfterNS: uint64(time.Second), MaxRetryAfterNS: uint64(5 * time.Minute), CircuitOpenNS: uint64(5 * time.Minute)},
		Payload:      capture.PayloadPolicy{MaxRawBytes: CoinMMaxRawPayloadBytes, MaxSchemaDepth: 32, MaxSchemaFields: 512, MaxArrayElements: CoinMMaxMergedRecords},
	}
}

func CoinMRESTSourceContract(operation CoinMRESTOperation, depthLimit int) (capture.SourceContract, error) {
	weight, err := CoinMRESTRequestWeight(operation, depthLimit)
	if err != nil {
		return capture.SourceContract{}, err
	}
	endpoint, family := CoinMExchangeInfoPath, "instrument-metadata"
	switch operation {
	case CoinMRESTDepth:
		endpoint, family = CoinMDepthPath, "book-snapshot"
	case CoinMRESTOpenInterest:
		endpoint, family = CoinMOpenInterestPath, "current-open-interest-rest-observation"
	}
	return capture.SourceContract{
		Version:    capture.SourceContractVersion,
		SourceID:   CoinMSourceID,
		ContractID: "binance.coinm.rest." + string(operation) + ".v1",
		APIVersion: "Binance COIN-M Futures DAPI v1, accessed 2026-08-23",
		Documentation: []capture.DocumentationRef{
			{URL: CoinMRESTDocumentationURI, AccessedAtNS: CoinMAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: CoinMIntegrationNoticeURI, AccessedAtNS: CoinMAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: []capture.Capability{
			{ChannelOrEndpoint: endpoint, DataFamily: family, Entitlement: "public", Support: capture.SupportAvailable},
			{ChannelOrEndpoint: "private-rest", DataFamily: "private", Entitlement: "credentials", Support: capture.SupportUnsupported, Declaration: "authenticated REST methods are outside the public COIN-M adapter"},
		},
		Topology: capture.ConnectionTopology{Transport: capture.TransportREST, MaxConnections: 1, Throttleable: true},
		Rate:     capture.RatePolicy{Capacity: CoinMSharedWeightCapacity, RefillTokens: CoinMSharedWeightCapacity, RefillIntervalNS: CoinMSharedWeightIntervalNS, RequestCost: weight, MaxAttempts: 3, DefaultRetryAfterNS: uint64(time.Second), MaxRetryAfterNS: uint64(3 * 24 * time.Hour), CircuitOpenNS: uint64(3 * 24 * time.Hour), RetryableStatusCodes: []int{429}, Retryable5XX: true, TerminalStatusCodes: []int{403}, CircuitStatusCodes: []int{418}},
		Payload:  capture.PayloadPolicy{MaxRawBytes: CoinMMaxRawPayloadBytes, MaxSchemaDepth: 32, MaxSchemaFields: 4096, MaxArrayElements: CoinMMaxMergedRecords},
	}, nil
}

func CoinMCatalogContract() (catalog.Source, catalog.SourceVersion, []catalog.ChannelContract) {
	source := catalog.Source{SourceID: CoinMSourceID, Venue: "binance", ProductFamily: "coinm", APIFamily: "dapi-v1", Environment: "production-public", Lifecycle: "active"}
	version := catalog.SourceVersion{
		OfficialAPIVersion:    "Binance COIN-M Futures DAPI v1, accessed 2026-08-23",
		DocumentationURI:      CoinMRESTDocumentationURI,
		Endpoints:             mustJSON(map[string]any{"rest_base": CoinMRESTBase, "websocket": CoinMWebSocketEndpoint, "exchange_info": CoinMExchangeInfoPath, "depth": CoinMDepthPath, "open_interest": CoinMOpenInterestPath}),
		Topology:              mustJSON(map[string]any{"merged_all_market_universe": true, "route_native_symbol_type": 2, "host_not_source_authority": true}),
		Entitlement:           mustJSON(map[string]any{"security_type": "NONE", "credentials_required": false, "private_endpoints": "unsupported"}),
		Region:                "global-public",
		RateContract:          mustJSON(map[string]any{"pool": "shared_fapi_dapi_request_weight", "capacity_per_minute": CoinMSharedWeightCapacity, "response_headers_owned_by_pool": true}),
		HeartbeatPolicy:       mustJSON(map[string]any{"server_ping_interval_seconds": 180, "pong_deadline_seconds": 600}),
		AcknowledgementPolicy: mustJSON(map[string]any{"url_subscription": true}),
		ReconnectPolicy:       mustJSON(map[string]any{"socket_lifetime_hours": 24, "book_pu_mismatch_closes_epoch": true}),
	}
	channels := []catalog.ChannelContract{
		{ChannelID: "rest.exchangeInfo.dapi.v1", NativeSelector: mustJSON(map[string]any{"method": "GET", "path": CoinMExchangeInfoPath, "security_type": "NONE"}), Role: "instrument_metadata_contract_size", DataFamily: "catalog", CadenceSource: "caller-scheduled REST opportunity", Aggregation: mustJSON(map[string]any{"kind": "complete_response", "temporary_absence_closes_lifecycle": false}), Depth: mustJSON(map[string]any{"applicable": false}), SequenceRules: mustJSON(map[string]any{"temporal_contract_size": true, "payoff_metadata_required_for_conversion": true}), ChecksumRules: mustJSON(map[string]any{"raw_payload": "sha256"}), PayloadSchema: mustJSON(map[string]any{"contractSize": "exact_decimal", "native_quantity": "contracts", "conversion": "temporal_catalog_only"}), SupportState: "supported", Limitation: "conversion is unavailable without an active temporal contractSize and payoff version"},
		{ChannelID: "rest.openInterest.dapi.v1", NativeSelector: mustJSON(map[string]any{"method": "GET", "path": CoinMOpenInterestPath, "security_type": "NONE", "symbol_required": true}), Role: "current_open_interest_rest_observation", DataFamily: "derivative-ticker", CadenceSource: "caller-scheduled poll cycle", Aggregation: mustJSON(map[string]any{"kind": "single_current_observation", "historical_series": false}), Depth: mustJSON(map[string]any{"applicable": false}), SequenceRules: mustJSON(map[string]any{"poll_cycle_epoch": true}), ChecksumRules: mustJSON(map[string]any{"raw_payload": "sha256"}), PayloadSchema: mustJSON(map[string]any{"native_value": "contracts", "derived_usd": "contracts*temporal_contractSize", "derived_base": "requires_price_and_inverse_payoff"}), SupportState: "supported", Limitation: "base conversion is absent until an explicit price observation is supplied"},
	}
	return source, version, channels
}
