package hyperliquid

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
	DocumentationAccessDate   = "2026-08-22"
	DocumentationAccessTimeNS = int64(1787356800000000000)

	InfoDocumentationURI         = "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint"
	PerpetualDocumentationURI    = "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/perpetuals"
	SpotDocumentationURI         = "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/info-endpoint/spot"
	SubscriptionDocumentationURI = "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions"
	HeartbeatDocumentationURI    = "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/timeouts-and-heartbeats"
	RateDocumentationURI         = "https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/rate-limits-and-user-limits"
	HIP3DocumentationURI         = "https://hyperliquid.gitbook.io/hyperliquid-docs/hyperliquid-improvement-proposals-hips/hip-3-builder-deployed-perpetuals"
	HistoricalDocumentationURI   = "https://hyperliquid.gitbook.io/hyperliquid-docs/historical-data"

	MaxRawPayloadBytes             = 4 << 20
	MaxSubscriptions               = 1000
	MaxPendingACK                  = 100
	SubscriptionACKTimeoutNS       = uint64(10 * time.Second)
	HeartbeatIntervalNS            = uint64(30 * time.Second)
	HeartbeatTimeoutNS             = uint64(10 * time.Second)
	MaximumLevelsSlow              = 20
	MaximumLevelsFast              = 5
	MaxOutboundMessagesPerMinute   = 2000
	MaxConnectionAttemptsPerMinute = 30
)

var (
	ErrInvalidNetwork      = errors.New("hyperliquid: invalid network")
	ErrInvalidFamily       = errors.New("hyperliquid: invalid market family")
	ErrInvalidPayload      = errors.New("hyperliquid: invalid public payload")
	ErrInvalidSubscription = errors.New("hyperliquid: invalid subscription")
	ErrUnsupportedRole     = errors.New("hyperliquid: unsupported source role")
	ErrPositionalMismatch  = errors.New("hyperliquid: positional metadata/context mismatch")
	ErrBookDepthContract   = errors.New("hyperliquid: invalid book depth contract")
	ErrBookStreamMismatch  = errors.New("hyperliquid: book stream identity mismatch")
	ErrFixtureBoundary     = errors.New("hyperliquid: fixture boundary violation")
	ErrRateBudget          = errors.New("hyperliquid: rate budget exhausted")
	ErrRateClockRegression = errors.New("hyperliquid: rate clock regressed")
)

type Network string

const (
	Mainnet Network = catalog.HyperliquidNetworkMainnet
	Testnet Network = catalog.HyperliquidNetworkTestnet
)

func (n Network) Validate() error {
	switch n {
	case Mainnet, Testnet:
		return nil
	default:
		return ErrInvalidNetwork
	}
}

func (n Network) InfoEndpoint() string {
	switch n {
	case Mainnet:
		return "https://api.hyperliquid.xyz/info"
	case Testnet:
		return "https://api.hyperliquid-testnet.xyz/info"
	default:
		return ""
	}
}

func (n Network) WebSocketEndpoint() string {
	switch n {
	case Mainnet:
		return "wss://api.hyperliquid.xyz/ws"
	case Testnet:
		return "wss://api.hyperliquid-testnet.xyz/ws"
	default:
		return ""
	}
}

type Family = catalog.HyperliquidFamily

const (
	MainPerpetual Family = catalog.HyperliquidMainPerpetual
	Spot          Family = catalog.HyperliquidSpot
	HIP3          Family = catalog.HyperliquidHIP3
)

func validateFamily(family Family) error {
	switch family {
	case MainPerpetual, Spot, HIP3:
		return nil
	default:
		return ErrInvalidFamily
	}
}

type SourceRole string

const (
	RoleTrades              SourceRole = "trades"
	RoleSlowBook            SourceRole = "l2_book_slow_20"
	RoleFastBook            SourceRole = "l2_book_fast_5"
	RoleBBO                 SourceRole = "bbo"
	RoleAssetContext        SourceRole = "asset_context"
	RoleFundingHistory      SourceRole = "funding_history"
	RoleMetadata            SourceRole = "metadata"
	RoleIncrementalBook     SourceRole = "incremental_book"
	RoleMarketLiquidation   SourceRole = "market_wide_liquidation"
	RoleNativeHistoryImport SourceRole = "native_history_import"
	RoleStrictEconomicUnits SourceRole = "strict_economic_units"
)

type RoleSupport struct {
	Role       SourceRole
	Support    capture.SupportLevel
	Limitation string
}

func SupportMatrix(family Family) ([]RoleSupport, error) {
	if err := validateFamily(family); err != nil {
		return nil, err
	}
	roles := []RoleSupport{
		{Role: RoleTrades, Support: capture.SupportAvailable},
		{Role: RoleSlowBook, Support: capture.SupportAvailable},
		{Role: RoleFastBook, Support: capture.SupportAvailable},
		{Role: RoleBBO, Support: capture.SupportAvailable},
		{Role: RoleAssetContext, Support: capture.SupportAvailable},
		{Role: RoleMetadata, Support: capture.SupportAvailable},
		{Role: RoleIncrementalBook, Support: capture.SupportUnsupported, Limitation: "l2Book is a full depth-limited snapshot with no sequence or delta contract"},
		{Role: RoleMarketLiquidation, Support: capture.SupportUnsupported, Limitation: "no public market-wide liquidation channel is declared"},
		{Role: RoleNativeHistoryImport, Support: capture.SupportUnsupported, Limitation: "Enable Labs v1 starts at recorder capture and has no venue-native import path"},
	}
	if family == Spot {
		roles = append(roles,
			RoleSupport{Role: RoleFundingHistory, Support: capture.SupportUnsupported, Limitation: "funding is a perpetual-only family"},
			RoleSupport{Role: RoleStrictEconomicUnits, Support: capture.SupportAvailable},
		)
	} else {
		roles = append(roles, RoleSupport{Role: RoleFundingHistory, Support: capture.SupportAvailable})
		if family == HIP3 {
			roles = append(roles, RoleSupport{Role: RoleStrictEconomicUnits, Support: capture.SupportAmbiguous, Limitation: "contract-generation economic units are provisional and excluded from strict normalized totals"})
		} else {
			roles = append(roles, RoleSupport{Role: RoleStrictEconomicUnits, Support: capture.SupportAvailable})
		}
	}
	slices.SortFunc(roles, func(a, b RoleSupport) int { return strings.Compare(string(a.Role), string(b.Role)) })
	return roles, nil
}

func Supports(family Family, role SourceRole) (RoleSupport, bool) {
	roles, err := SupportMatrix(family)
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

func PublicSourceContract(network Network, family Family, dexName string) (capture.SourceContract, error) {
	if err := network.Validate(); err != nil {
		return capture.SourceContract{}, err
	}
	if err := validateFamily(family); err != nil {
		return capture.SourceContract{}, err
	}
	if err := validateDEXName(family, dexName); err != nil {
		return capture.SourceContract{}, err
	}
	roles, _ := SupportMatrix(family)
	websocketRoles := []SourceRole{RoleTrades, RoleSlowBook, RoleFastBook, RoleBBO, RoleAssetContext}
	capabilities := make([]capture.Capability, 0, len(websocketRoles))
	for _, role := range roles {
		if !slices.Contains(websocketRoles, role.Role) {
			continue
		}
		channel, dataFamily := websocketCapabilityIdentity(role.Role)
		declaration := ""
		if role.Support != capture.SupportAvailable {
			declaration = role.Limitation
		}
		capabilities = append(capabilities, capture.Capability{
			ChannelOrEndpoint: channel, DataFamily: dataFamily, Entitlement: "public",
			Support: role.Support, Declaration: declaration,
		})
	}
	contract := capture.SourceContract{
		Version:       capture.SourceContractVersion,
		SourceID:      sourceID(network, family, dexName),
		ContractID:    fmt.Sprintf("hyperliquid.%s.%s.%s.public.ws.v1", network, family, sourceDEXIdentity(family, dexName)),
		APIVersion:    "Hyperliquid REST Info and public WebSocket, access-dated 2026-08-22",
		Documentation: documentationRefs(),
		Capabilities:  capabilities,
		Topology: capture.ConnectionTopology{
			Transport:              capture.TransportWebSocket,
			MaxConnections:         10,
			MaxSubscriptions:       MaxSubscriptions,
			MaxSubscriptionsPerACK: 1,
			Throttleable:           true,
		},
		Subscription: capture.SubscriptionPolicy{ACKMode: capture.ACKExact, ACKTimeoutNS: SubscriptionACKTimeoutNS, MaxPendingACK: MaxPendingACK},
		Heartbeat:    capture.HeartbeatPolicy{Mode: capture.HeartbeatTestResponse, IntervalNS: HeartbeatIntervalNS, TimeoutNS: HeartbeatTimeoutNS},
		UsefulData:   capture.UsefulDataPolicy{},
		Rate: capture.RatePolicy{
			Capacity: MaxOutboundMessagesPerMinute, RefillTokens: MaxOutboundMessagesPerMinute, RefillIntervalNS: uint64(time.Minute), RequestCost: 1,
			MaxAttempts: 1, DefaultRetryAfterNS: uint64(time.Minute), MaxRetryAfterNS: uint64(10 * time.Minute), CircuitOpenNS: uint64(time.Minute),
		},
		Payload: capture.PayloadPolicy{MaxRawBytes: MaxRawPayloadBytes, MaxSchemaDepth: 32, MaxSchemaFields: 4096, MaxArrayElements: 1 << 17},
	}
	if err := contract.Validate(); err != nil {
		return capture.SourceContract{}, err
	}
	return contract, nil
}

func InfoSourceContract(network Network, family Family, dexName string) (capture.SourceContract, error) {
	contract, err := PublicSourceContract(network, family, dexName)
	if err != nil {
		return capture.SourceContract{}, err
	}
	contract.ContractID = fmt.Sprintf("hyperliquid.%s.%s.%s.info.rest.v1", network, family, sourceDEXIdentity(family, dexName))
	contract.Topology = capture.ConnectionTopology{Transport: capture.TransportREST, MaxConnections: 1, Throttleable: true}
	contract.Subscription = capture.SubscriptionPolicy{}
	contract.Heartbeat = capture.HeartbeatPolicy{}
	infoRoles := []SourceRole{RoleMetadata, RoleFundingHistory, RoleAssetContext, RoleNativeHistoryImport, RoleStrictEconomicUnits}
	roles, _ := SupportMatrix(family)
	contract.Capabilities = make([]capture.Capability, 0, len(infoRoles))
	for _, role := range roles {
		if !slices.Contains(infoRoles, role.Role) {
			continue
		}
		endpoint, dataFamily := infoCapabilityIdentity(role.Role, family)
		declaration := ""
		if role.Support != capture.SupportAvailable {
			declaration = role.Limitation
		}
		contract.Capabilities = append(contract.Capabilities, capture.Capability{
			ChannelOrEndpoint: endpoint, DataFamily: dataFamily, Entitlement: "public",
			Support: role.Support, Declaration: declaration,
		})
	}
	contract.Rate = capture.RatePolicy{
		Capacity: 1200, RefillTokens: 1200, RefillIntervalNS: uint64(time.Minute), RequestCost: 20,
		MaxAttempts: 1, DefaultRetryAfterNS: uint64(time.Minute), MaxRetryAfterNS: uint64(10 * time.Minute), CircuitOpenNS: uint64(time.Minute), Retryable5XX: true,
	}
	if err := contract.Validate(); err != nil {
		return capture.SourceContract{}, err
	}
	return contract, nil
}

func InfoRequestWeight(requestType string) (uint32, error) {
	switch requestType {
	case "l2Book", "allMids":
		return 2, nil
	case "perpDexs", "meta", "metaAndAssetCtxs", "spotMeta", "spotMetaAndAssetCtxs", "fundingHistory":
		return 20, nil
	default:
		return 0, fmt.Errorf("%w: undocumented info request %q", ErrUnsupportedRole, requestType)
	}
}

// InfoFinalWeight reconciles response-dependent Info weight after capture.
// fundingHistory adds one unit for each started group of 20 returned rows.
func InfoFinalWeight(requestType string, returnedItems int) (uint32, error) {
	if returnedItems < 0 {
		return 0, fmt.Errorf("%w: negative returned item count", ErrInvalidPayload)
	}
	base, err := InfoRequestWeight(requestType)
	if err != nil {
		return 0, err
	}
	if requestType != "fundingHistory" || returnedItems == 0 {
		return base, nil
	}
	additional := (uint64(returnedItems) + 19) / 20
	if uint64(base)+additional > uint64(^uint32(0)) {
		return 0, fmt.Errorf("%w: Info weight overflow", ErrInvalidPayload)
	}
	return base + uint32(additional), nil
}

func documentationRefs() []capture.DocumentationRef {
	urls := []string{InfoDocumentationURI, PerpetualDocumentationURI, SpotDocumentationURI, SubscriptionDocumentationURI, HeartbeatDocumentationURI, RateDocumentationURI, HIP3DocumentationURI, HistoricalDocumentationURI}
	refs := make([]capture.DocumentationRef, len(urls))
	for index, uri := range urls {
		refs[index] = capture.DocumentationRef{URL: uri, AccessedAtNS: DocumentationAccessTimeNS, Authority: capture.RuleOfficialDocumentation}
	}
	return refs
}

func sourceID(network Network, family Family, dexName string) string {
	return fmt.Sprintf("hyperliquid-%s-%s-%s", network, family, sourceDEXIdentity(family, dexName))
}

func sourceDEXIdentity(family Family, dexName string) string {
	switch family {
	case MainPerpetual:
		return catalog.HyperliquidMainDEX
	case Spot:
		return catalog.HyperliquidSpotDEX
	default:
		return dexName
	}
}

func validateDEXName(family Family, dexName string) error {
	switch family {
	case MainPerpetual, Spot:
		if dexName != "" {
			return fmt.Errorf("%w: main and spot use the empty wire DEX selector", ErrInvalidFamily)
		}
	case HIP3:
		if dexName == "" || dexName == catalog.HyperliquidMainDEX || dexName == catalog.HyperliquidSpotDEX || len(dexName) > 64 || strings.IndexByte(dexName, 0) >= 0 {
			return fmt.Errorf("%w: invalid HIP-3 DEX name", ErrInvalidFamily)
		}
	default:
		return ErrInvalidFamily
	}
	return nil
}

func websocketCapabilityIdentity(role SourceRole) (string, string) {
	switch role {
	case RoleTrades:
		return "ws:trades", "trade"
	case RoleSlowBook:
		return "ws:l2Book?fast=false", "book_l2_full_snapshot_20"
	case RoleFastBook:
		return "ws:l2Book?fast=true", "book_l2_full_snapshot_5"
	case RoleBBO:
		return "ws:bbo", "quote"
	case RoleAssetContext:
		return "ws:activeAssetCtx", "asset_context"
	default:
		return "", "unknown"
	}
}

func infoCapabilityIdentity(role SourceRole, family Family) (string, string) {
	switch role {
	case RoleAssetContext:
		if family == Spot {
			return "info:spotMetaAndAssetCtxs", "asset_context"
		}
		return "info:metaAndAssetCtxs", "asset_context"
	case RoleFundingHistory:
		return "info:fundingHistory", "funding"
	case RoleMetadata:
		if family == Spot {
			return "info:spotMeta", "instrument_metadata"
		}
		return "info:perpDexs|meta", "instrument_metadata"
	case RoleNativeHistoryImport:
		return "unsupported:native_history_import", "native_history_import"
	case RoleStrictEconomicUnits:
		return "normalize:economic_units", "strict_normalized_total"
	default:
		return "", "unknown"
	}
}
