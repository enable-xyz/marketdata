package okx

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

	PublicWebSocketEndpoint   = "wss://ws.okx.com:8443/ws/v5/public"
	BusinessWebSocketEndpoint = "wss://ws.okx.com:8443/ws/v5/business"
	PublicRESTEndpoint        = "https://www.okx.com"

	InstrumentPath     = "/api/v5/public/instruments"
	RESTBookPath       = "/api/v5/market/books"
	HistoryTradesPath  = "/api/v5/market/history-trades"
	FundingHistoryPath = "/api/v5/public/funding-rate-history"
	OptionSummaryPath  = "/api/v5/public/opt-summary"
	IndexTickersPath   = "/api/v5/market/index-tickers"
	MarkPricePath      = "/api/v5/public/mark-price"

	GuideDocumentationURI       = "https://www.okx.com/docs-v5/en/"
	BookDocumentationURI        = "https://www.okx.com/docs-v5/en/#order-book-trading-market-data-ws-order-book-channel"
	ChecksumDocumentationURI    = "https://www.okx.com/docs-v5/log_en/#2026-06-23"
	HistoricalDataURI           = "https://www.okx.com/docs-v5/en/#public-data-rest-api-get-historical-market-data"
	LiquidationDocumentationURI = "https://www.okx.com/docs-v5/en/#public-data-websocket-liquidation-orders-channel"

	MaxRawPayloadBytes       = 1 << 20
	MaxSubscriptions         = 480
	MaxPendingACK            = 64
	SubscriptionACKTimeoutNS = uint64(10 * time.Second)
	HeartbeatIntervalNS      = uint64(25 * time.Second)
	HeartbeatTimeoutNS       = uint64(5 * time.Second)

	ChecksumCutoverTimeNS = int64(1782172800000000000)
)

var (
	ErrInvalidConfiguration = errors.New("okx: invalid caller configuration")
	ErrInvalidSubscription  = errors.New("okx: invalid subscription")
	ErrUnexpectedACK        = errors.New("okx: unexpected subscription acknowledgement")
	ErrVIPEntitlement       = errors.New("okx: VIP4 login evidence is required")
	ErrInvalidPayload       = errors.New("okx: invalid public market-data payload")
	ErrAmbiguousProjection  = errors.New("okx: source evidence is insufficient for normalization")
	ErrFixtureBoundary      = errors.New("okx: fixture boundary violation")
	ErrRateLimited          = errors.New("okx: venue rate budget denied operation")
	ErrNativeManifest       = errors.New("okx: invalid native-file manifest")
)

type InstrumentType string

const (
	Spot    InstrumentType = "SPOT"
	Margin  InstrumentType = "MARGIN"
	Swap    InstrumentType = "SWAP"
	Futures InstrumentType = "FUTURES"
	Option  InstrumentType = "OPTION"
	Any     InstrumentType = "ANY"
)

func (t InstrumentType) Validate() error {
	switch t {
	case Spot, Margin, Swap, Futures, Option, Any:
		return nil
	default:
		return fmt.Errorf("%w: invalid instrument type %q", ErrInvalidConfiguration, t)
	}
}

type SocketKind string

const (
	PublicSocket   SocketKind = "public"
	BusinessSocket SocketKind = "business"
)

func (k SocketKind) Endpoint() string {
	switch k {
	case PublicSocket:
		return PublicWebSocketEndpoint
	case BusinessSocket:
		return BusinessWebSocketEndpoint
	default:
		return ""
	}
}

type SourceRole string

const (
	RoleTradesAll          SourceRole = "business_trades_all_single_match"
	RoleAggregatedTrades   SourceRole = "public_trades_aggregate"
	RoleBaselineBook       SourceRole = "public_books_400_100ms"
	RoleBookBBO            SourceRole = "public_bbo_tbt"
	RoleBook5              SourceRole = "public_books5_replacement"
	RoleVIPBook50          SourceRole = "public_books50_l2_tbt_vip4"
	RoleVIPBook400         SourceRole = "public_books_l2_tbt_vip4"
	RoleRPIBook            SourceRole = "public_books_rpi_separate"
	RoleTicker             SourceRole = "public_tickers"
	RoleOpenInterest       SourceRole = "public_open_interest"
	RoleFundingRate        SourceRole = "public_funding_rate"
	RoleMarkPrice          SourceRole = "public_mark_price"
	RoleIndexTicker        SourceRole = "public_index_tickers"
	RoleOptionSummary      SourceRole = "public_option_summary"
	RoleLiquidation        SourceRole = "public_liquidation_orders_incomplete"
	RoleInstrumentMetadata SourceRole = "rest_instrument_metadata"
	RoleRESTComparisonBook SourceRole = "rest_comparison_book"
	RoleRESTTradeHistory   SourceRole = "rest_bounded_trade_history"
	RoleRESTFundingHistory SourceRole = "rest_funding_history"
	RoleRESTOptionSummary  SourceRole = "rest_option_summary"
	RoleRESTIndexTicker    SourceRole = "rest_index_ticker"
	RoleRESTMarkPrice      SourceRole = "rest_mark_price"
	RoleNativeFileManifest SourceRole = "native_file_manifest_only"
)

type RoleSupport struct {
	Role        SourceRole
	Socket      SocketKind
	Entitlement string
	Support     capture.SupportLevel
	Limitation  string
}

func SupportMatrix() []RoleSupport {
	roles := []RoleSupport{
		{Role: RoleTradesAll, Socket: BusinessSocket, Entitlement: "public", Support: capture.SupportAvailable, Limitation: "one maker match per accepted record; an aggregate count is rejected"},
		{Role: RoleAggregatedTrades, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable, Limitation: "separate aggregate source; never relabelled as per-match"},
		{Role: RoleBaselineBook, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable, Limitation: "400 levels, 100 ms, regular liquidity only"},
		{Role: RoleBookBBO, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleBook5, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable, Limitation: "five-level replacement view; not baseline reconstruction"},
		{Role: RoleVIPBook50, Socket: PublicSocket, Entitlement: "login_plus_vip4", Support: capture.SupportAvailable, Limitation: "10 ms entitlement row; no silent downgrade"},
		{Role: RoleVIPBook400, Socket: PublicSocket, Entitlement: "login_plus_vip4", Support: capture.SupportAvailable, Limitation: "10 ms entitlement row; no silent downgrade"},
		{Role: RoleRPIBook, Socket: PublicSocket, Entitlement: "declared_rpi_access", Support: capture.SupportAmbiguous, Limitation: "distinct reconstructable snapshot-plus-incremental RPI stream; never merged with regular liquidity and not promoted without caller access evidence"},
		{Role: RoleTicker, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleOpenInterest, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleFundingRate, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleMarkPrice, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleIndexTicker, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleOptionSummary, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleLiquidation, Socket: PublicSocket, Entitlement: "public", Support: capture.SupportAvailable, Limitation: "partial_nonchronological source completeness; source order retained"},
		{Role: RoleInstrumentMetadata, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleRESTComparisonBook, Entitlement: "public", Support: capture.SupportAvailable, Limitation: "comparison snapshot only; never a WebSocket splice"},
		{Role: RoleRESTTradeHistory, Entitlement: "public", Support: capture.SupportAvailable, Limitation: "bounded REST observation; never native-history import"},
		{Role: RoleRESTFundingHistory, Entitlement: "public", Support: capture.SupportAvailable, Limitation: "bounded funding history observation; never a complete archive claim"},
		{Role: RoleRESTOptionSummary, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleRESTIndexTicker, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleRESTMarkPrice, Entitlement: "public", Support: capture.SupportAvailable},
		{Role: RoleNativeFileManifest, Entitlement: "caller_owned_filesystem", Support: capture.SupportAvailable, Limitation: "manifest and publication opportunities only; file contents are never imported"},
	}
	slices.SortFunc(roles, func(a, b RoleSupport) int { return strings.Compare(string(a.Role), string(b.Role)) })
	return roles
}

type NativeFileSourceContract struct {
	Version                 uint16
	SourceID                string
	DocumentationURI        string
	DocumentationAccessNS   int64
	PublicationLagDays      [2]uint8
	ManifestOnly            bool
	NativeHistoryImport     bool
	MissingOrEmptyMeansZero bool
	Limitation              string
}

func NativeFileContract() NativeFileSourceContract {
	return NativeFileSourceContract{Version: 1, SourceID: "okx-v5-native-file-publication", DocumentationURI: HistoricalDataURI, DocumentationAccessNS: DocumentationAccessTimeNS, PublicationLagDays: [2]uint8{2, 3}, ManifestOnly: true, NativeHistoryImport: false, MissingOrEmptyMeansZero: false, Limitation: "availability varies by module, instrument, and date; only observed publication opportunities and manifests are verified"}
}

// HandshakeRatePolicy is the shared-IP WebSocket connection budget.
func HandshakeRatePolicy() capture.RatePolicy {
	return capture.RatePolicy{
		Capacity: 3, RefillTokens: 3, RefillIntervalNS: uint64(time.Second), ConnectionCost: 1,
		MaxAttempts: 1, DefaultRetryAfterNS: uint64(time.Second), MaxRetryAfterNS: uint64(time.Minute), CircuitOpenNS: uint64(time.Second),
	}
}

// OperationRatePolicy is the per-connection subscribe, unsubscribe, and login
// operation budget. This public adapter emits subscribe operations only.
func OperationRatePolicy() capture.RatePolicy {
	return capture.RatePolicy{
		Capacity: 480, RefillTokens: 480, RefillIntervalNS: uint64(time.Hour), RequestCost: 1,
		MaxAttempts: 1, DefaultRetryAfterNS: uint64(time.Second), MaxRetryAfterNS: uint64(time.Hour), CircuitOpenNS: uint64(time.Minute),
	}
}

func PublicSourceContract(kind SocketKind) (capture.SourceContract, error) {
	if kind.Endpoint() == "" {
		return capture.SourceContract{}, fmt.Errorf("%w: invalid socket kind", ErrInvalidConfiguration)
	}
	capabilities := make([]capture.Capability, 0, len(SupportMatrix()))
	for _, role := range SupportMatrix() {
		if role.Socket != kind {
			continue
		}
		channel, family := capabilityIdentity(role.Role)
		declaration := ""
		if role.Support != capture.SupportAvailable {
			declaration = role.Limitation
		}
		capabilities = append(capabilities, capture.Capability{
			ChannelOrEndpoint: channel,
			DataFamily:        family,
			Entitlement:       role.Entitlement,
			Support:           role.Support,
			Declaration:       declaration,
		})
	}
	contract := capture.SourceContract{
		Version:    capture.SourceContractVersion,
		SourceID:   "okx-v5-" + string(kind),
		ContractID: "okx.v5." + string(kind) + ".ws.v1",
		APIVersion: "OKX API V5",
		Documentation: []capture.DocumentationRef{
			{URL: GuideDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: BookDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: ChecksumDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: LiquidationDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: capabilities,
		Topology: capture.ConnectionTopology{
			Transport: capture.TransportWebSocket, MaxConnections: 1,
			MaxSubscriptions: MaxSubscriptions, MaxSubscriptionsPerACK: 1, Throttleable: true,
		},
		Subscription: capture.SubscriptionPolicy{ACKMode: capture.ACKExact, ACKTimeoutNS: SubscriptionACKTimeoutNS, MaxPendingACK: MaxPendingACK},
		Heartbeat:    capture.HeartbeatPolicy{Mode: capture.HeartbeatPingPong, IntervalNS: HeartbeatIntervalNS, TimeoutNS: HeartbeatTimeoutNS},
		UsefulData:   capture.UsefulDataPolicy{},
		Rate:         HandshakeRatePolicy(),
		Payload:      capture.PayloadPolicy{MaxRawBytes: MaxRawPayloadBytes, MaxSchemaDepth: 32, MaxSchemaFields: 4096, MaxArrayElements: 1 << 16},
	}
	if err := contract.Validate(); err != nil {
		return capture.SourceContract{}, err
	}
	return contract, nil
}

func RESTSourceContract() (capture.SourceContract, error) {
	capabilities := make([]capture.Capability, 0, 8)
	for _, role := range SupportMatrix() {
		if role.Socket != "" || role.Role == RoleNativeFileManifest {
			continue
		}
		endpoint, family := capabilityIdentity(role.Role)
		declaration := ""
		if role.Support != capture.SupportAvailable {
			declaration = role.Limitation
		}
		capabilities = append(capabilities, capture.Capability{ChannelOrEndpoint: endpoint, DataFamily: family, Entitlement: role.Entitlement, Support: role.Support, Declaration: declaration})
	}
	contract := capture.SourceContract{
		Version: capture.SourceContractVersion, SourceID: "okx-v5-public-rest", ContractID: "okx.v5.public.rest.v1", APIVersion: "OKX API V5",
		Documentation: []capture.DocumentationRef{
			{URL: GuideDocumentationURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
			{URL: HistoricalDataURI, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: capabilities,
		Topology:     capture.ConnectionTopology{Transport: capture.TransportREST, MaxConnections: 1, Throttleable: true},
		Rate: capture.RatePolicy{
			Capacity: 20, RefillTokens: 20, RefillIntervalNS: uint64(2 * time.Second), RequestCost: 1,
			MaxAttempts: 1, DefaultRetryAfterNS: uint64(time.Second), MaxRetryAfterNS: uint64(time.Minute), CircuitOpenNS: uint64(time.Minute),
		},
		Payload: capture.PayloadPolicy{MaxRawBytes: MaxRawPayloadBytes, MaxSchemaDepth: 32, MaxSchemaFields: 4096, MaxArrayElements: 1 << 16},
	}
	if err := contract.Validate(); err != nil {
		return capture.SourceContract{}, err
	}
	return contract, nil
}

func capabilityIdentity(role SourceRole) (string, string) {
	switch role {
	case RoleTradesAll:
		return "trades-all", "trade"
	case RoleAggregatedTrades:
		return "trades", "trade_aggregate"
	case RoleBaselineBook:
		return "books", "book_l2_regular"
	case RoleBookBBO:
		return "bbo-tbt", "quote"
	case RoleBook5:
		return "books5", "book_l2_replacement"
	case RoleVIPBook50:
		return "books50-l2-tbt", "book_l2_regular"
	case RoleVIPBook400:
		return "books-l2-tbt", "book_l2_regular"
	case RoleRPIBook:
		return "books-rpi-tbt", "book_l2_rpi"
	case RoleTicker:
		return "tickers", "ticker"
	case RoleOpenInterest:
		return "open-interest", "derivative_ticker"
	case RoleFundingRate:
		return "funding-rate", "derivative_ticker"
	case RoleMarkPrice:
		return "mark-price", "derivative_ticker"
	case RoleIndexTicker:
		return "index-tickers", "derivative_ticker"
	case RoleOptionSummary:
		return "opt-summary", "option_summary"
	case RoleLiquidation:
		return "liquidation-orders", "liquidation"
	case RoleInstrumentMetadata:
		return InstrumentPath, "instrument_metadata"
	case RoleRESTComparisonBook:
		return RESTBookPath, "book_comparison_snapshot"
	case RoleRESTTradeHistory:
		return HistoryTradesPath, "bounded_trade_history"
	case RoleRESTFundingHistory:
		return FundingHistoryPath, "funding_history"
	case RoleRESTOptionSummary:
		return OptionSummaryPath, "option_summary"
	case RoleRESTIndexTicker:
		return IndexTickersPath, "derivative_ticker"
	case RoleRESTMarkPrice:
		return MarkPricePath, "derivative_ticker"
	case RoleNativeFileManifest:
		return "caller-selected-manifest", "native_file_publication_evidence"
	default:
		return string(role), "unknown"
	}
}
