package deribit

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/capture"
)

const (
	SourceID                  = "deribit-json-rpc-v2"
	ProductionEndpoint        = "wss://www.deribit.com/ws/api/v2"
	TestnetEndpoint           = "wss://test.deribit.com/ws/api/v2"
	DocumentationAccessedAtNS = int64(1787356800000000000)

	BookDocumentationURI       = "https://docs.deribit.com/subscriptions/orderbook/bookinstrument_nameinterval"
	TradeDocumentationURI      = "https://docs.deribit.com/subscriptions/trades/tradesinstrument_nameinterval"
	InstrumentDocumentationURI = "https://docs.deribit.com/api-reference/market-data/public-get_instruments"
	CollectionDocumentationURI = "https://docs.deribit.com/articles/market-data-collection-best-practices"
	RateDocumentationURI       = "https://docs.deribit.com/articles/rate-limits"
	HeartbeatDocumentationURI  = "https://docs.deribit.com/api-reference/session-management/public-set_heartbeat"
	SubscribeDocumentationURI  = "https://docs.deribit.com/api-reference/subscription-management/public-subscribe"
	AuthDocumentationURI       = "https://docs.deribit.com/api-reference/authentication/public-auth"

	MaxSubscriptions      = 256
	MaxRawPayloadBytes    = 4 << 20
	MaxSchemaDepth        = 32
	MaxSchemaFields       = 4096
	MaxArrayElements      = 1 << 16
	SubscriptionTimeoutNS = uint64(15 * time.Second)
	HeartbeatIntervalNS   = uint64(30 * time.Second)
	HeartbeatTimeoutNS    = uint64(10 * time.Second)
	MinimumHeartbeatNS    = uint64(10 * time.Second)
)

var (
	ErrInvalidContract       = errors.New("deribit: invalid source contract")
	ErrAuthorizationRequired = errors.New("deribit: raw cadence requires explicit authorization")
	ErrInvalidChannel        = errors.New("deribit: invalid channel")
)

type Cadence string

const (
	CadenceRaw   Cadence = "raw"
	Cadence100MS Cadence = "100ms"
	CadenceAgg2  Cadence = "agg2"
)

func (c Cadence) Validate() error {
	switch c {
	case CadenceRaw, Cadence100MS, CadenceAgg2:
		return nil
	default:
		return fmt.Errorf("%w: unknown cadence", ErrInvalidContract)
	}
}

type CadencePolicy struct {
	Requested  Cadence
	Fallback   Cadence
	Authorized bool
}

func (p CadencePolicy) Validate() error {
	if err := p.Requested.Validate(); err != nil {
		return err
	}
	if p.Requested == CadenceRaw {
		if !p.Authorized {
			return ErrAuthorizationRequired
		}
		if p.Fallback != Cadence100MS && p.Fallback != CadenceAgg2 {
			return fmt.Errorf("%w: raw cadence requires declared 100ms or agg2 fallback", ErrInvalidContract)
		}
		return nil
	}
	if p.Fallback != "" || p.Authorized {
		return fmt.Errorf("%w: fallback and authorization apply only to requested raw cadence", ErrInvalidContract)
	}
	return nil
}

// FallbackPolicy returns a distinct caller-visible source policy. The adapter
// never silently mutates a raw support row after an authorization failure.
func (p CadencePolicy) FallbackPolicy() (CadencePolicy, error) {
	if err := p.Validate(); err != nil {
		return CadencePolicy{}, err
	}
	if p.Requested != CadenceRaw {
		return CadencePolicy{}, fmt.Errorf("%w: no fallback was declared", ErrInvalidContract)
	}
	return CadencePolicy{Requested: p.Fallback}, nil
}

type SourceRole string

const (
	RoleBook               SourceRole = "book"
	RoleGroupedBookView    SourceRole = "grouped_book_view"
	RoleTrade              SourceRole = "trade"
	RoleTicker             SourceRole = "ticker"
	RoleQuote              SourceRole = "quote"
	RoleFunding            SourceRole = "funding"
	RoleIndex              SourceRole = "index"
	RoleInstrumentCreation SourceRole = "instrument_creation"
	RoleInstrumentState    SourceRole = "instrument_state"
	RoleLiquidation        SourceRole = "liquidation"
)

type RoleSupport struct {
	Role       SourceRole
	Support    capture.SupportLevel
	Limitation string
}

func SupportMatrix() []RoleSupport {
	roles := []RoleSupport{
		{Role: RoleBook, Support: capture.SupportAvailable},
		{Role: RoleGroupedBookView, Support: capture.SupportAvailable, Limitation: "native grouped snapshot/view evidence only; never reconstructable"},
		{Role: RoleTrade, Support: capture.SupportAvailable},
		{Role: RoleTicker, Support: capture.SupportAvailable},
		{Role: RoleQuote, Support: capture.SupportAvailable},
		{Role: RoleFunding, Support: capture.SupportAvailable},
		{Role: RoleIndex, Support: capture.SupportAvailable},
		{Role: RoleInstrumentCreation, Support: capture.SupportAvailable},
		{Role: RoleInstrumentState, Support: capture.SupportAvailable},
		{Role: RoleLiquidation, Support: capture.SupportUnsupported, Limitation: "no public liquidation channel; only M/T/MT flags on public trades"},
	}
	slices.SortFunc(roles, func(a, b RoleSupport) int { return strings.Compare(string(a.Role), string(b.Role)) })
	return roles
}

func SourceContract(policy CadencePolicy) (capture.SourceContract, error) {
	if err := policy.Validate(); err != nil {
		return capture.SourceContract{}, err
	}
	entitlement := "public"
	if policy.Requested == CadenceRaw {
		entitlement = "authorized_raw_1ms_aggregation"
	}
	capabilities := make([]capture.Capability, 0, len(SupportMatrix()))
	for _, support := range SupportMatrix() {
		channel, family := capabilityIdentity(support.Role, policy.Requested)
		declaration := ""
		if support.Support != capture.SupportAvailable {
			declaration = support.Limitation
		}
		capabilities = append(capabilities, capture.Capability{
			ChannelOrEndpoint: channel,
			DataFamily:        family,
			Entitlement:       entitlement,
			Support:           support.Support,
			Declaration:       declaration,
		})
	}
	contract := capture.SourceContract{
		Version:    capture.SourceContractVersion,
		SourceID:   SourceID,
		ContractID: "deribit.json-rpc-v2." + string(policy.Requested) + ".ws.v1",
		APIVersion: "Deribit JSON-RPC v2; documentation accessed 2026-08-22",
		Documentation: []capture.DocumentationRef{
			{URL: BookDocumentationURI, AccessedAtNS: DocumentationAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: TradeDocumentationURI, AccessedAtNS: DocumentationAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: InstrumentDocumentationURI, AccessedAtNS: DocumentationAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: CollectionDocumentationURI, AccessedAtNS: DocumentationAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: RateDocumentationURI, AccessedAtNS: DocumentationAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
			{URL: HeartbeatDocumentationURI, AccessedAtNS: DocumentationAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: capabilities,
		Topology: capture.ConnectionTopology{
			Transport: capture.TransportWebSocket, MaxConnections: 1,
			MaxSubscriptions: MaxSubscriptions, MaxSubscriptionsPerACK: MaxSubscriptions,
			Throttleable: true,
		},
		Subscription: capture.SubscriptionPolicy{
			ACKMode: capture.ACKExact, ACKTimeoutNS: SubscriptionTimeoutNS, MaxPendingACK: 1,
		},
		Heartbeat: capture.HeartbeatPolicy{
			Mode: capture.HeartbeatTestResponse, IntervalNS: HeartbeatIntervalNS,
			TimeoutNS: HeartbeatTimeoutNS, MinimumIntervalNS: MinimumHeartbeatNS,
		},
		Rate: capture.RatePolicy{
			Capacity: 10, RefillTokens: 1, RefillIntervalNS: uint64(300 * time.Millisecond),
			ConnectionCost: 1, RequestCost: 1, MaxAttempts: 1,
			DefaultRetryAfterNS: uint64(time.Second), MaxRetryAfterNS: uint64(time.Minute), CircuitOpenNS: uint64(time.Minute),
		},
		Payload: capture.PayloadPolicy{
			MaxRawBytes: MaxRawPayloadBytes, MaxSchemaDepth: MaxSchemaDepth,
			MaxSchemaFields: MaxSchemaFields, MaxArrayElements: MaxArrayElements,
		},
	}
	if err := contract.Validate(); err != nil {
		return capture.SourceContract{}, err
	}
	return contract, nil
}

func capabilityIdentity(role SourceRole, cadence Cadence) (string, string) {
	suffix := string(cadence)
	switch role {
	case RoleBook:
		return "book.{instrument}." + suffix, "full_book_l2"
	case RoleGroupedBookView:
		return "book.{instrument}.{group}.{depth}." + suffix, "grouped_book_view"
	case RoleTrade:
		return "trades.{instrument}." + suffix, "trade"
	case RoleTicker:
		return "ticker.{instrument}." + suffix, "derivative_option_ticker"
	case RoleQuote:
		return "quote.{instrument}", "quote_bbo"
	case RoleFunding:
		return "perpetual.{instrument}." + suffix, "funding"
	case RoleIndex:
		return "deribit_price_index.{index}", "index"
	case RoleInstrumentCreation:
		return "instrument.creation.{kind}.{currency}", "instrument_lifecycle"
	case RoleInstrumentState:
		return "instrument.state.{kind}.{currency}", "instrument_lifecycle"
	case RoleLiquidation:
		return "public_liquidation_channel", "liquidation"
	default:
		return string(role), "unknown"
	}
}
