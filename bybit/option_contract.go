package bybit

import (
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/capture"
)

const (
	OptionSourceID       = "bybit-v5-option-public"
	OptionPublicEndpoint = "wss://stream.bybit.com/v5/public/option"

	OptionTradeDocumentationURI      = "https://bybit-exchange.github.io/docs/v5/websocket/public/trade"
	OptionOrderbookDocumentationURI  = "https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook"
	OptionTickerDocumentationURI     = "https://bybit-exchange.github.io/docs/v5/websocket/public/ticker"
	OptionInstrumentDocumentationURI = "https://bybit-exchange.github.io/docs/v5/market/instrument"

	OptionMinimumBookDepth            = 25
	OptionMaximumBookDepth            = 100
	OptionMaxSchemaDepth       uint16 = 32
	OptionMaxSchemaFields      uint32 = 1024
	OptionMaxArrayElements     uint32 = 1 << 16
	OptionRESTMaxSchemaDepth   uint16 = 32
	OptionRESTMaxSchemaFields  uint32 = 4096
	OptionRESTMaxArrayElements uint32 = 1000
)

const RoleOptionTicker SourceRole = "option_ticker"

// OptionSupportMatrix is separate from Spot/Linear/Inverse because an Option
// connection has different topic identity, depth, ticker, and unsupported-role
// semantics.
func OptionSupportMatrix() []RoleSupport {
	roles := []RoleSupport{
		{Role: RoleTrade, Support: capture.SupportAvailable, Limitation: "subscription identity is the base coin, not an option instrument"},
		{Role: RoleBoundedOrderbook, Support: capture.SupportAvailable, Limitation: "only documented option depths 25 and 100; snapshots replace state"},
		{Role: RoleOptionTicker, Support: capture.SupportAvailable, Limitation: "base-coin topic; every message is a complete snapshot with option Greeks"},
		{Role: RoleInstrumentMetadata, Support: capture.SupportAvailable},
		{Role: RoleBBO, Support: capture.SupportUnsupported, Limitation: "Bybit V5 Option exposes no public level-1/BBO topic"},
		{Role: RoleFullOrderbook, Support: capture.SupportUnsupported, Limitation: "Bybit V5 Option exposes bounded depth 25/100, not the public full-book topic"},
		{Role: RoleRPIOrderbook, Support: capture.SupportUnsupported, Limitation: "Bybit V5 Option exposes no public RPI orderbook"},
		{Role: RoleAllLiquidation, Support: capture.SupportUnsupported, Limitation: "allLiquidation is derivatives-only and excludes Option"},
	}
	slices.SortFunc(roles, func(a, b RoleSupport) int { return strings.Compare(string(a.Role), string(b.Role)) })
	return roles
}

func OptionSupports(role SourceRole) (RoleSupport, bool) {
	for _, support := range OptionSupportMatrix() {
		if support.Role == role {
			return support, true
		}
	}
	return RoleSupport{}, false
}

func OptionPublicSourceContract() capture.SourceContract {
	capabilities := make([]capture.Capability, 0, len(OptionSupportMatrix()))
	for _, role := range OptionSupportMatrix() {
		if role.Role == RoleInstrumentMetadata {
			continue
		}
		channel, family := optionCapabilityIdentity(role.Role)
		declaration := ""
		if role.Support != capture.SupportAvailable {
			declaration = role.Limitation
		}
		capabilities = append(capabilities, capture.Capability{
			ChannelOrEndpoint: channel,
			DataFamily:        family,
			Entitlement:       "public",
			Support:           role.Support,
			Declaration:       declaration,
		})
	}
	return capture.SourceContract{
		Version:    capture.SourceContractVersion,
		SourceID:   OptionSourceID,
		ContractID: "bybit.v5.option.public.ws.v1",
		APIVersion: "Bybit V5 Option public WebSocket",
		Documentation: []capture.DocumentationRef{
			{URL: ConnectDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: OptionTradeDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: OptionOrderbookDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: OptionTickerDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: capabilities,
		Topology: capture.ConnectionTopology{
			Transport: capture.TransportWebSocket, MaxConnections: 1,
			MaxSubscriptions: MaxSubscriptions, MaxSubscriptionsPerACK: MaxSubscriptionsPerACK,
			Throttleable: true,
		},
		Subscription: capture.SubscriptionPolicy{ACKMode: capture.ACKExact, ACKTimeoutNS: SubscriptionACKTimeoutNS, MaxPendingACK: MaxPendingACK},
		Heartbeat:    capture.HeartbeatPolicy{Mode: capture.HeartbeatTestResponse, IntervalNS: HeartbeatIntervalNS, TimeoutNS: HeartbeatTimeoutNS},
		UsefulData:   capture.UsefulDataPolicy{},
		Rate: capture.RatePolicy{
			Capacity: 500, RefillTokens: 500, RefillIntervalNS: uint64(5 * time.Minute), ConnectionCost: 1,
			MaxAttempts: 1, DefaultRetryAfterNS: uint64(5 * time.Minute), MaxRetryAfterNS: uint64(10 * time.Minute),
			CircuitOpenNS: uint64(10 * time.Minute), CircuitStatusCodes: []int{403},
		},
		Payload: optionWSPayloadPolicy(),
	}
}

func OptionInstrumentSourceContract() capture.SourceContract {
	return capture.SourceContract{
		Version:       capture.SourceContractVersion,
		SourceID:      OptionSourceID,
		ContractID:    "bybit.v5.option.instrument.rest.v1",
		APIVersion:    "Bybit V5 REST Option instruments",
		Documentation: []capture.DocumentationRef{{URL: OptionInstrumentDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation}},
		Capabilities:  []capture.Capability{{ChannelOrEndpoint: InstrumentInfoPath, DataFamily: "option_instrument_metadata", Entitlement: "public", Support: capture.SupportAvailable}},
		Topology:      capture.ConnectionTopology{Transport: capture.TransportREST, MaxConnections: 1, Throttleable: true},
		Rate: capture.RatePolicy{
			Capacity: 600, RefillTokens: 600, RefillIntervalNS: uint64(time.Minute), RequestCost: 1,
			MaxAttempts: 1, DefaultRetryAfterNS: uint64(time.Minute), MaxRetryAfterNS: uint64(10 * time.Minute),
			CircuitOpenNS: uint64(10 * time.Minute), CircuitStatusCodes: []int{403},
		},
		Payload: optionRESTPayloadPolicy(),
	}
}

func optionCapabilityIdentity(role SourceRole) (string, string) {
	switch role {
	case RoleTrade:
		return "publicTrade.{base_coin}", "option_trade"
	case RoleBoundedOrderbook:
		return "orderbook.{25|100}.{instrument}", "option_book_l2_bounded"
	case RoleOptionTicker:
		return "tickers.{base_coin}", "option_summary"
	case RoleBBO:
		return "orderbook.1.{instrument}", "option_quote"
	case RoleFullOrderbook:
		return "orderbook.full.{instrument}", "option_book_l2_full"
	case RoleRPIOrderbook:
		return "orderbook.rpi.{instrument}", "option_book_l2_rpi"
	case RoleAllLiquidation:
		return "allLiquidation.{instrument}", "option_liquidation"
	case RoleInstrumentMetadata:
		return InstrumentInfoPath, "option_instrument_metadata"
	default:
		return string(role), "unknown"
	}
}
