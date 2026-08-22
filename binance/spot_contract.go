package binance

import (
	"encoding/hex"
	"time"

	"github.com/enable-xyz/marketdata/capture"
)

const (
	SpotWSContractID               = "binance.spot.ws.raw.v1"
	SpotDepthContractID            = "binance.spot.rest.depth.v1"
	SpotRawChannel                 = "ws.spot.raw.v1"
	SpotDepthChannel               = "rest.depth.v3"
	SpotRawDataFamily              = "native-market-data"
	SpotDepthDataFamily            = "book-snapshot"
	SpotWSEndpoint                 = "wss://data-stream.binance.vision/ws"
	SpotDepthEndpoint              = "/api/v3/depth"
	SpotWSDocumentationURI         = "https://github.com/binance/binance-spot-api-docs/blob/" + SpotRESTCommit + "/web-socket-streams.md"
	SpotDepthDocumentationURI      = "https://github.com/binance/binance-spot-api-docs/blob/" + SpotRESTCommit + "/rest-api.md#order-book"
	SpotTermsURI                   = "https://www.binance.com/en/terms"
	SpotSubscriptionRequestID      = "spot-subscribe-v1"
	SpotOfficialStreamLimit        = 1024
	SpotAdapterStreamLimit         = 256
	SpotSubscriptionBatchLimit     = 64
	SpotSubscriptionBatchCount     = 4
	SpotControlMessagesPerSecond   = 5
	SpotMaxControlMessageBytes     = 16 << 10
	SpotMaxPingPayloadBytes        = 125
	SpotMaxRawPayloadBytes         = 1 << 20
	SpotDepthLimitDefault          = 100
	SpotDepthLimitMaximum          = 5000
	SpotRESTBudgetCapacity         = 6000
	SpotRESTBudgetIntervalNS       = uint64(time.Minute)
	SpotConnectionBudgetCapacity   = 300
	SpotConnectionBudgetIntervalNS = uint64(5 * time.Minute)
	SpotSocketLifetimeNS           = uint64(24 * time.Hour)
	SpotPingIntervalNS             = uint64(20 * time.Second)
	SpotPongDeadlineNS             = uint64(time.Minute)
	SpotACKDeadlineNS              = uint64(10 * time.Second)
	SpotFixtureAccessedAtNS        = int64(1787443200000000000)
)

const (
	SpotOfficialTradeFixtureSHA256         = "25bc1caa02c3905a15eccb04078a73d114ecd32d3834c5ff4fae18f37cc756a8"
	SpotOfficialTradeFixtureLength         = 385
	SpotOfficialDepthFixtureSHA256         = "11b794dac753c42fca68a65957be259723263fcc0214cf7fd686d4309f4d78a4"
	SpotOfficialDepthFixtureLength         = 589
	SpotOfficialBookTickerFixtureSHA256    = "3846e8bcbcd3c93da802ff23554d11c7fdac724a863a54f8e45ff2405f4adb2c"
	SpotOfficialBookTickerFixtureLength    = 273
	SpotOfficialTickerFixtureSHA256        = "ae735d6e1e16dc47504ddce87343083f97535596009738e579d46f734dd4e2de"
	SpotOfficialTickerFixtureLength        = 1152
	SpotOfficialACKFixtureSHA256           = "5139da7abc80668135104d2665b92797a714725f4c840b281350a1c26cd680c8"
	SpotOfficialACKFixtureLength           = 42
	SpotOfficialDepthSnapshotFixtureSHA256 = "93190c63bd51815bd5b1662882e24796583ca2f2e3da06a08a61fb50e066fee3"
	SpotOfficialDepthSnapshotFixtureLength = 196
)

// SpotWSSourceContract declares the pinned official limits and the stricter
// adapter-owned memory bounds. Its raw channel covers exactly the four public
// stream roles returned by SpotSubscriptionPlan.
func SpotWSSourceContract() capture.SourceContract {
	return capture.SourceContract{
		Version:    capture.SourceContractVersion,
		SourceID:   SpotSourceID,
		ContractID: SpotWSContractID,
		APIVersion: "Binance Spot WebSocket Streams",
		Documentation: []capture.DocumentationRef{
			{URL: SpotWSDocumentationURI, AccessedAtNS: SpotFixtureAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: []capture.Capability{
			{ChannelOrEndpoint: SpotRawChannel, DataFamily: SpotRawDataFamily, Entitlement: "public", Support: capture.SupportAvailable},
			{ChannelOrEndpoint: "@trade", DataFamily: "trade", Entitlement: "public", Support: capture.SupportAvailable},
			{ChannelOrEndpoint: "@depth@100ms", DataFamily: "depth-update", Entitlement: "public", Support: capture.SupportAvailable},
			{ChannelOrEndpoint: "@bookTicker", DataFamily: "best-bid-offer", Entitlement: "public", Support: capture.SupportAvailable},
			{ChannelOrEndpoint: "@ticker", DataFamily: "24-hour-ticker", Entitlement: "public", Support: capture.SupportAvailable},
			{ChannelOrEndpoint: "user-data", DataFamily: "private", Entitlement: "credentials", Support: capture.SupportUnsupported, Declaration: "private and user-data endpoints are outside the public raw adapter"},
			{ChannelOrEndpoint: "trading", DataFamily: "orders", Entitlement: "credentials", Support: capture.SupportUnsupported, Declaration: "trading and authenticated methods are outside the public raw adapter"},
		},
		Topology: capture.ConnectionTopology{
			Transport:              capture.TransportWebSocket,
			MaxConnections:         1,
			MaxSubscriptions:       SpotOfficialStreamLimit,
			MaxSubscriptionsPerACK: SpotSubscriptionBatchLimit,
			Throttleable:           false,
		},
		Subscription: capture.SubscriptionPolicy{
			ACKMode:       capture.ACKExact,
			ACKTimeoutNS:  SpotACKDeadlineNS,
			MaxPendingACK: SpotSubscriptionBatchCount,
		},
		Heartbeat: capture.HeartbeatPolicy{
			Mode:       capture.HeartbeatPingPong,
			IntervalNS: SpotPingIntervalNS,
			TimeoutNS:  SpotPongDeadlineNS,
		},
		UsefulData: capture.UsefulDataPolicy{},
		Rate: capture.RatePolicy{
			Capacity:            SpotConnectionBudgetCapacity,
			RefillTokens:        SpotConnectionBudgetCapacity,
			RefillIntervalNS:    SpotConnectionBudgetIntervalNS,
			ConnectionCost:      1,
			MaxAttempts:         1,
			DefaultRetryAfterNS: uint64(time.Second),
			MaxRetryAfterNS:     SpotConnectionBudgetIntervalNS,
			CircuitOpenNS:       SpotConnectionBudgetIntervalNS,
		},
		Payload: spotPayloadPolicy(),
		FixtureIdentities: []capture.FixtureIdentity{
			spotOfficialFixture("official.ws.trade", SpotOfficialTradeFixtureSHA256, SpotOfficialTradeFixtureLength, SpotWSDocumentationURI+"#trade-streams"),
			spotOfficialFixture("official.ws.depth", SpotOfficialDepthFixtureSHA256, SpotOfficialDepthFixtureLength, SpotWSDocumentationURI+"#diff-depth-stream"),
			spotOfficialFixture("official.ws.book-ticker", SpotOfficialBookTickerFixtureSHA256, SpotOfficialBookTickerFixtureLength, SpotWSDocumentationURI+"#individual-symbol-book-ticker-streams"),
			spotOfficialFixture("official.ws.ticker", SpotOfficialTickerFixtureSHA256, SpotOfficialTickerFixtureLength, SpotWSDocumentationURI+"#individual-symbol-ticker-streams"),
			spotOfficialFixture("official.ws.subscribe-ack", SpotOfficialACKFixtureSHA256, SpotOfficialACKFixtureLength, SpotWSDocumentationURI+"#subscribe-to-a-stream"),
		},
	}
}

// SpotDepthSourceContract returns a REST contract whose request weight matches
// the caller-selected documented depth limit tier.
func SpotDepthSourceContract(limit int) (capture.SourceContract, error) {
	weight, err := SpotDepthRequestWeight(limit)
	if err != nil {
		return capture.SourceContract{}, err
	}
	return capture.SourceContract{
		Version:    capture.SourceContractVersion,
		SourceID:   SpotSourceID,
		ContractID: SpotDepthContractID,
		APIVersion: "Binance Spot REST API v3",
		Documentation: []capture.DocumentationRef{
			{URL: SpotDepthDocumentationURI, AccessedAtNS: SpotFixtureAccessedAtNS, Authority: capture.RuleOfficialDocumentation},
		},
		Capabilities: []capture.Capability{
			{ChannelOrEndpoint: SpotDepthChannel, DataFamily: SpotDepthDataFamily, Entitlement: "public", Support: capture.SupportAvailable},
			{ChannelOrEndpoint: "private-rest", DataFamily: "private", Entitlement: "credentials", Support: capture.SupportUnsupported, Declaration: "authenticated REST methods are outside the public snapshot adapter"},
		},
		Topology: capture.ConnectionTopology{Transport: capture.TransportREST, MaxConnections: 1, Throttleable: true},
		Rate: capture.RatePolicy{
			Capacity:             SpotRESTBudgetCapacity,
			RefillTokens:         SpotRESTBudgetCapacity,
			RefillIntervalNS:     SpotRESTBudgetIntervalNS,
			RequestCost:          weight,
			MaxAttempts:          3,
			DefaultRetryAfterNS:  uint64(time.Second),
			MaxRetryAfterNS:      uint64(3 * 24 * time.Hour),
			CircuitOpenNS:        uint64(3 * 24 * time.Hour),
			RetryableStatusCodes: []int{429},
			Retryable5XX:         true,
			TerminalStatusCodes:  []int{403},
			CircuitStatusCodes:   []int{418},
		},
		Payload: spotPayloadPolicy(),
		FixtureIdentities: []capture.FixtureIdentity{
			spotOfficialFixture("official.rest.depth-snapshot", SpotOfficialDepthSnapshotFixtureSHA256, SpotOfficialDepthSnapshotFixtureLength, SpotDepthDocumentationURI),
		},
	}, nil
}

func spotPayloadPolicy() capture.PayloadPolicy {
	return capture.PayloadPolicy{
		MaxRawBytes:      SpotMaxRawPayloadBytes,
		MaxSchemaDepth:   32,
		MaxSchemaFields:  512,
		MaxArrayElements: 20_000,
	}
}

func spotOfficialFixture(id, digest string, length uint32, source string) capture.FixtureIdentity {
	decoded, _ := hex.DecodeString(digest)
	var sum [32]byte
	copy(sum[:], decoded)
	return capture.FixtureIdentity{
		ID:               id,
		SHA256:           sum,
		ByteLength:       length,
		Provenance:       capture.FixturePrimarySource,
		SourceReference:  source,
		LicenseReference: SpotTermsURI,
		AccessedAtNS:     SpotFixtureAccessedAtNS,
	}
}
