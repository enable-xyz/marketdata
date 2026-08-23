package bybit

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/capture"
)

const (
	DocumentationAccessDate   = "2026-08-23"
	DocumentationAccessTimeNS = int64(1787443200000000000)

	SpotPublicEndpoint    = "wss://stream.bybit.com/v5/public/spot"
	LinearPublicEndpoint  = "wss://stream.bybit.com/v5/public/linear"
	InversePublicEndpoint = "wss://stream.bybit.com/v5/public/inverse"
	InstrumentInfoPath    = "/v5/market/instruments-info"

	ConnectDocumentationURI       = "https://bybit-exchange.github.io/docs/v5/ws/connect"
	TradeDocumentationURI         = "https://bybit-exchange.github.io/docs/v5/websocket/public/trade"
	OrderbookDocumentationURI     = "https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook"
	FullOrderbookDocumentationURI = "https://bybit-exchange.github.io/docs/v5/websocket/public/full-ob"
	RPIOrderbookDocumentationURI  = "https://bybit-exchange.github.io/docs/v5/websocket/public/orderbook-rpi"
	TickerDocumentationURI        = "https://bybit-exchange.github.io/docs/v5/websocket/public/ticker"
	LiquidationDocumentationURI   = "https://bybit-exchange.github.io/docs/v5/websocket/public/all-liquidation"
	InstrumentDocumentationURI    = "https://bybit-exchange.github.io/docs/v5/market/instrument"
	OpenInterestChangelogURI      = "https://bybit-exchange.github.io/docs/changelog/v5#2026-06-11"

	MaxRawPayloadBytes       = 1 << 20
	MaxSubscriptions         = 256
	MaxSubscriptionsPerACK   = 64
	MaxPendingACK            = 4
	SubscriptionACKTimeoutNS = uint64(10 * time.Second)
	HeartbeatIntervalNS      = uint64(20 * time.Second)
	HeartbeatTimeoutNS       = uint64(10 * time.Second)
)

var (
	ErrInvalidCategory   = errors.New("bybit: invalid V5 category")
	ErrInvalidTopic      = errors.New("bybit: invalid public topic")
	ErrUnsupportedRole   = errors.New("bybit: role is unsupported for category")
	ErrInvalidPayload    = errors.New("bybit: invalid public payload")
	ErrUnseededTicker    = errors.New("bybit: derivative ticker delta requires snapshot seed")
	ErrBookNeedsSnapshot = errors.New("bybit: orderbook requires a snapshot")
	ErrFullBookGap       = errors.New("bybit: full orderbook update ID gap")
	ErrFullSequence      = errors.New("bybit: full orderbook cross sequence regressed")
	ErrFullSnapshotStale = errors.New("bybit: full orderbook snapshot does not exactly match a buffered delta")
	ErrFixtureBoundary   = errors.New("bybit: fixture boundary violation")
)

type Category string

const (
	Spot    Category = "spot"
	Linear  Category = "linear"
	Inverse Category = "inverse"
)

func (c Category) Validate() error {
	switch c {
	case Spot, Linear, Inverse:
		return nil
	default:
		return ErrInvalidCategory
	}
}

func (c Category) SourceID() string {
	switch c {
	case Spot:
		return "bybit-v5-spot-public"
	case Linear:
		return "bybit-v5-linear-public"
	case Inverse:
		return "bybit-v5-inverse-public"
	default:
		return ""
	}
}

func (c Category) PublicEndpoint() string {
	switch c {
	case Spot:
		return SpotPublicEndpoint
	case Linear:
		return LinearPublicEndpoint
	case Inverse:
		return InversePublicEndpoint
	default:
		return ""
	}
}

type SourceRole string

const (
	RoleTrade              SourceRole = "public_trade"
	RoleBoundedOrderbook   SourceRole = "bounded_regular_orderbook"
	RoleFullOrderbook      SourceRole = "full_regular_orderbook"
	RoleRPIOrderbook       SourceRole = "rpi_orderbook"
	RoleBBO                SourceRole = "best_bid_offer"
	RoleGenericTicker      SourceRole = "generic_ticker"
	RoleDerivativeTicker   SourceRole = "derivative_ticker"
	RoleAllLiquidation     SourceRole = "all_liquidation"
	RoleInstrumentMetadata SourceRole = "instrument_metadata"
)

type RoleSupport struct {
	Role       SourceRole
	Support    capture.SupportLevel
	Limitation string
}

func SupportMatrix(category Category) ([]RoleSupport, error) {
	if err := category.Validate(); err != nil {
		return nil, err
	}
	roles := []RoleSupport{
		{Role: RoleTrade, Support: capture.SupportAvailable},
		{Role: RoleBoundedOrderbook, Support: capture.SupportAvailable, Limitation: "declared depth only; snapshots replace state and full-book sequence rules do not apply"},
		{Role: RoleFullOrderbook, Support: capture.SupportAvailable, Limitation: "delta-only; requires an exact REST seq/u bridge capped at 10000 levels per side"},
		{Role: RoleRPIOrderbook, Support: capture.SupportAvailable, Limitation: "separate 50-level source role; never merged into regular-book state"},
		{Role: RoleBBO, Support: capture.SupportAvailable},
		{Role: RoleInstrumentMetadata, Support: capture.SupportAvailable},
	}
	if category == Spot {
		roles = append(roles,
			RoleSupport{Role: RoleGenericTicker, Support: capture.SupportAvailable},
			RoleSupport{Role: RoleDerivativeTicker, Support: capture.SupportUnsupported, Limitation: "Spot ticker is snapshot-only and has no derivative state contract"},
			RoleSupport{Role: RoleAllLiquidation, Support: capture.SupportUnsupported, Limitation: "the allLiquidation topic covers USDT, USDC, and inverse contracts only"},
		)
	} else {
		roles = append(roles,
			RoleSupport{Role: RoleGenericTicker, Support: capture.SupportAvailable, Limitation: "generic fields are retained inside the derivative ticker source without changing its role"},
			RoleSupport{Role: RoleDerivativeTicker, Support: capture.SupportAvailable, Limitation: "sparse deltas require a snapshot seed and reconnect resets all cached fields"},
			RoleSupport{Role: RoleAllLiquidation, Support: capture.SupportAvailable, Limitation: "complete all-observed events; documentation says object while its example supplies an array"},
		)
	}
	slices.SortFunc(roles, func(a, b RoleSupport) int { return strings.Compare(string(a.Role), string(b.Role)) })
	return roles, nil
}

func Supports(category Category, role SourceRole) (RoleSupport, bool) {
	roles, err := SupportMatrix(category)
	if err != nil {
		return RoleSupport{}, false
	}
	for _, support := range roles {
		if support.Role == role {
			return support, true
		}
	}
	return RoleSupport{}, false
}

func PublicSourceContract(category Category) (capture.SourceContract, error) {
	roles, err := SupportMatrix(category)
	if err != nil {
		return capture.SourceContract{}, err
	}
	capabilities := make([]capture.Capability, 0, len(roles))
	for _, role := range roles {
		channel, family := capabilityIdentity(role.Role)
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
	contract := capture.SourceContract{
		Version:    capture.SourceContractVersion,
		SourceID:   category.SourceID(),
		ContractID: fmt.Sprintf("bybit.v5.%s.public.ws.v1", category),
		APIVersion: "Bybit V5 public WebSocket",
		Documentation: []capture.DocumentationRef{
			{URL: ConnectDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: TradeDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: OrderbookDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: FullOrderbookDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: RPIOrderbookDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: TickerDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: LiquidationDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: OpenInterestChangelogURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: capabilities,
		Topology: capture.ConnectionTopology{
			Transport:              capture.TransportWebSocket,
			MaxConnections:         1,
			MaxSubscriptions:       MaxSubscriptions,
			MaxSubscriptionsPerACK: MaxSubscriptionsPerACK,
			Throttleable:           true,
		},
		Subscription: capture.SubscriptionPolicy{
			ACKMode:       capture.ACKExact,
			ACKTimeoutNS:  SubscriptionACKTimeoutNS,
			MaxPendingACK: MaxPendingACK,
		},
		Heartbeat: capture.HeartbeatPolicy{
			Mode:       capture.HeartbeatTestResponse,
			IntervalNS: HeartbeatIntervalNS,
			TimeoutNS:  HeartbeatTimeoutNS,
		},
		UsefulData: capture.UsefulDataPolicy{},
		Rate: capture.RatePolicy{
			Capacity:            500,
			RefillTokens:        500,
			RefillIntervalNS:    uint64(5 * time.Minute),
			ConnectionCost:      1,
			MaxAttempts:         1,
			DefaultRetryAfterNS: uint64(5 * time.Minute),
			MaxRetryAfterNS:     uint64(10 * time.Minute),
			CircuitOpenNS:       uint64(10 * time.Minute),
			CircuitStatusCodes:  []int{403},
		},
		Payload: capture.PayloadPolicy{
			MaxRawBytes:      MaxRawPayloadBytes,
			MaxSchemaDepth:   32,
			MaxSchemaFields:  1024,
			MaxArrayElements: 1 << 16,
		},
	}
	return contract, nil
}

func InstrumentSourceContract(category Category) (capture.SourceContract, error) {
	if err := category.Validate(); err != nil {
		return capture.SourceContract{}, err
	}
	return capture.SourceContract{
		Version:    capture.SourceContractVersion,
		SourceID:   category.SourceID(),
		ContractID: fmt.Sprintf("bybit.v5.%s.instrument.rest.v1", category),
		APIVersion: "Bybit V5 REST",
		Documentation: []capture.DocumentationRef{
			{URL: InstrumentDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: []capture.Capability{{
			ChannelOrEndpoint: InstrumentInfoPath,
			DataFamily:        "instrument_metadata",
			Entitlement:       "public",
			Support:           capture.SupportAvailable,
		}},
		Topology: capture.ConnectionTopology{Transport: capture.TransportREST, MaxConnections: 1, Throttleable: true},
		Rate: capture.RatePolicy{
			Capacity:            600,
			RefillTokens:        600,
			RefillIntervalNS:    uint64(time.Minute),
			RequestCost:         1,
			MaxAttempts:         1,
			DefaultRetryAfterNS: uint64(time.Minute),
			MaxRetryAfterNS:     uint64(10 * time.Minute),
			CircuitOpenNS:       uint64(10 * time.Minute),
			CircuitStatusCodes:  []int{403},
		},
		Payload: capture.PayloadPolicy{MaxRawBytes: MaxRawPayloadBytes, MaxSchemaDepth: 32, MaxSchemaFields: 4096, MaxArrayElements: 1000},
	}, nil
}

func capabilityIdentity(role SourceRole) (string, string) {
	switch role {
	case RoleTrade:
		return "publicTrade.{symbol}", "trade"
	case RoleBoundedOrderbook:
		return "orderbook.{depth}.{symbol}", "book_l2_regular_bounded"
	case RoleFullOrderbook:
		return "orderbook.full.{symbol}", "book_l2_regular_full"
	case RoleRPIOrderbook:
		return "orderbook.rpi.{symbol}", "book_l2_rpi"
	case RoleBBO:
		return "orderbook.1.{symbol}", "quote"
	case RoleGenericTicker:
		return "tickers.{symbol}", "ticker"
	case RoleDerivativeTicker:
		return "tickers.{symbol}", "derivative_ticker"
	case RoleAllLiquidation:
		return "allLiquidation.{symbol}", "liquidation"
	case RoleInstrumentMetadata:
		return InstrumentInfoPath, "instrument_metadata"
	default:
		return string(role), "unknown"
	}
}
