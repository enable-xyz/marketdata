package verify

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/bybit"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/deribit"
	"github.com/enable-xyz/marketdata/hyperliquid"
	"github.com/enable-xyz/marketdata/okx"
)

const (
	CanarySelectorBinanceSpot       = "binance-spot"
	CanarySelectorBinanceUSDM       = "binance-usdm"
	CanarySelectorBinanceUSDMPublic = "binance-usdm-public"
	CanarySelectorBinanceUSDMMarket = "binance-usdm-market"
	CanarySelectorBinanceCoinM      = "binance-coinm"

	CanarySelectorBybitSpot    = "bybit-spot"
	CanarySelectorBybitLinear  = "bybit-linear"
	CanarySelectorBybitInverse = "bybit-inverse"
	CanarySelectorBybitOption  = "bybit-option"

	CanarySelectorOKXSpot    = "okx-v5-spot"
	CanarySelectorOKXSwap    = "okx-v5-swap"
	CanarySelectorOKXFutures = "okx-v5-futures"
	CanarySelectorOKXOption  = "okx-v5-option"

	CanarySelectorDeribitPublic100MS = "deribit-v2-public-100ms"

	CanarySelectorHyperliquidMain = "hyperliquid-main"
	CanarySelectorHyperliquidSpot = "hyperliquid-spot"
	CanarySelectorHyperliquidHIP3 = "hyperliquid-hip3"

	CanaryReceiptVersion = uint16(1)

	DeribitRaw1MSLimitation = "raw 1ms cadence requires caller credentials and is unsupported by the public canary"

	canaryInterruptedHeartbeatGapReason = "socket transport lost with heartbeat acknowledgement outstanding"
)

var (
	ErrCanaryConfiguration     = errors.New("verify: invalid canary configuration")
	ErrCanaryUnsupported       = errors.New("verify: canary selector unsupported")
	ErrCanaryDeadline          = errors.New("verify: canary read deadline reached")
	errCanaryACKMismatch       = errors.New("verify: venue subscription ACK mismatch")
	errCanaryHeartbeatMismatch = errors.New("verify: venue heartbeat mismatch")
	errCanaryUnknownStream     = errors.New("verify: venue stream identity mismatch")
	errCanaryInvalidControl    = errors.New("verify: venue control message invalid")
)

type CanaryTerminalReason string

const (
	CanaryTerminalPlannedDuration    CanaryTerminalReason = "planned_duration"
	CanaryTerminalCallerCanceled     CanaryTerminalReason = "caller_canceled"
	CanaryTerminalCallerDeadline     CanaryTerminalReason = "caller_deadline"
	CanaryTerminalACKTimeout         CanaryTerminalReason = "subscription_ack_timeout"
	CanaryTerminalACKMismatch        CanaryTerminalReason = "subscription_ack_mismatch"
	CanaryTerminalHeartbeatTimeout   CanaryTerminalReason = "heartbeat_timeout"
	CanaryTerminalUnknownStream      CanaryTerminalReason = "unknown_stream_identity"
	CanaryTerminalOversize           CanaryTerminalReason = "message_byte_cap_exceeded"
	CanaryTerminalReconnectExhausted CanaryTerminalReason = "reconnects_exhausted"
	CanaryTerminalTransportFailure   CanaryTerminalReason = "transport_failure"
	CanaryTerminalInvalidEvent       CanaryTerminalReason = "invalid_event"
)

type CanaryInterval struct {
	StartedAtUTCNS int64  `json:"started_at_utc_ns"`
	EndedAtUTCNS   int64  `json:"ended_at_utc_ns"`
	Reason         string `json:"reason"`
}

type CanaryReceipt struct {
	Version                uint16                `json:"version"`
	Selector               string                `json:"selector"`
	StartedAtUTCNS         int64                 `json:"started_at_utc_ns"`
	EndedAtUTCNS           int64                 `json:"ended_at_utc_ns"`
	DurationNS             uint64                `json:"duration_ns"`
	Messages               uint64                `json:"messages"`
	Bytes                  uint64                `json:"bytes"`
	RollingSHA256          string                `json:"rolling_sha256"`
	SubscriptionsRequested uint64                `json:"subscriptions_requested"`
	SubscriptionsACKed     uint64                `json:"subscriptions_acked"`
	HeartbeatsSent         uint64                `json:"heartbeats_sent"`
	HeartbeatsACKed        uint64                `json:"heartbeats_acked"`
	HeartbeatsInterrupted  uint64                `json:"heartbeats_interrupted"`
	Reconnects             uint32                `json:"reconnects"`
	PlannedRotations       uint32                `json:"planned_rotations"`
	ExplainedIntervals     []CanaryInterval      `json:"explained_intervals"`
	UnexplainedIntervals   []CanaryInterval      `json:"unexplained_intervals"`
	Routes                 []CanaryRouteEvidence `json:"routes,omitempty"`
	Limitations            []string              `json:"limitations,omitempty"`
	TerminalReason         CanaryTerminalReason  `json:"terminal_reason"`
	ReceiptSHA256          string                `json:"receipt_sha256"`
}

type CanaryRouteEvidence struct {
	Selector      string `json:"selector"`
	ReceiptSHA256 string `json:"receipt_sha256"`
}

type CanaryClock interface {
	capture.Clock
	WaitUntil(context.Context, uint64) error
}

func (c *SystemClock) WaitUntil(ctx context.Context, targetMonotonicNS uint64) error {
	if c == nil {
		return ErrCanaryConfiguration
	}
	now := c.Read().MonotonicNS
	if now >= targetMonotonicNS {
		return ctx.Err()
	}
	delta := targetMonotonicNS - now
	if delta > uint64(math.MaxInt64) {
		delta = uint64(math.MaxInt64)
	}
	timer := time.NewTimer(time.Duration(delta))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (c *SystemClock) NowMonotonicNS() uint64 {
	if c == nil {
		return 0
	}
	return c.Read().MonotonicNS
}

type CanaryRawSink interface {
	WriteCanaryPayload(context.Context, string, int64, []byte) error
}

type CanaryHeartbeatSchedule struct {
	IntervalNS   uint64
	TimeoutNS    uint64
	ACKTimeoutNS uint64
}

type CanaryReconnectPolicy struct {
	MaxAttempts uint32
	BackoffNS   uint64
}

type CanaryRateBudgets struct {
	BinanceDerivatives     capture.RateBudget
	OKXHandshake           capture.RateBudget
	HyperliquidMessages    *hyperliquid.WeightedLimiter
	HyperliquidConnections *hyperliquid.WeightedLimiter
}

type CanaryConfig struct {
	Selector        string
	Instrument      string
	DEX             string
	DurationNS      uint64
	Reconnect       CanaryReconnectPolicy
	MaxMessageBytes uint32
	Heartbeat       CanaryHeartbeatSchedule
	Clock           CanaryClock
	HTTPClient      *http.Client
	RateBudgets     CanaryRateBudgets
	RawSink         CanaryRawSink
	Dial            CanaryDialFunc
	interval        *canaryIntervalBinding
}

type CanaryHeartbeatMode uint8

const (
	CanaryHeartbeatActive CanaryHeartbeatMode = iota + 1
	CanaryHeartbeatChallenge
)

type CanaryEventKind uint8

const (
	CanaryEventMessage CanaryEventKind = iota + 1
	CanaryEventSubscriptionACK
	CanaryEventHeartbeatACK
	CanaryEventHeartbeatChallenge
	CanaryEventDisconnect
	CanaryEventControl
)

type CanaryEvent struct {
	Kind           CanaryEventKind
	Payload        []byte
	StreamIdentity string
	ACKIdentities  []string
	Planned        bool
}

type CanaryConnection interface {
	Subscriptions() []string
	Subscribe(context.Context) error
	Read(context.Context, uint64) (CanaryEvent, error)
	Heartbeat(context.Context, uint64) error
	HeartbeatMode() CanaryHeartbeatMode
	RotationIntervalNS() uint64
	Close() error
}

type CanaryDialFunc func(context.Context, CanaryConfig) (CanaryConnection, error)
type CanaryDriverFunc func(context.Context, CanaryConfig) (CanaryReceipt, error)

type CanaryError struct {
	Reason CanaryTerminalReason
	Cause  error
}

func (e *CanaryError) Error() string {
	if e.Cause == nil {
		return "verify: canary terminated: " + string(e.Reason)
	}
	return fmt.Sprintf("verify: canary terminated: %s: %v", e.Reason, e.Cause)
}

func (e *CanaryError) Unwrap() error { return e.Cause }

func CanaryDriver(selector string) (CanaryDriverFunc, error) {
	switch selector {
	case CanarySelectorBinanceSpot:
		return RunBinanceSpotCanary, nil
	case CanarySelectorBinanceUSDM:
		return RunBinanceUSDMAggregateCanary, nil
	case CanarySelectorBinanceUSDMPublic, CanarySelectorBinanceUSDMMarket, CanarySelectorBinanceCoinM:
		return RunBinanceDerivativeCanary, nil
	case CanarySelectorBybitSpot, CanarySelectorBybitLinear, CanarySelectorBybitInverse, CanarySelectorBybitOption:
		return RunBybitCanary, nil
	case CanarySelectorOKXSpot, CanarySelectorOKXSwap, CanarySelectorOKXFutures, CanarySelectorOKXOption:
		return RunOKXCanary, nil
	case CanarySelectorDeribitPublic100MS:
		return RunDeribitCanary, nil
	case CanarySelectorHyperliquidMain, CanarySelectorHyperliquidSpot, CanarySelectorHyperliquidHIP3:
		return RunHyperliquidCanary, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrCanaryUnsupported, selector)
	}
}

func RunCanary(ctx context.Context, config CanaryConfig) (CanaryReceipt, error) {
	driver, err := CanaryDriver(config.Selector)
	if err != nil {
		return CanaryReceipt{}, err
	}
	return driver(ctx, config)
}

func RunBinanceSpotCanary(ctx context.Context, config CanaryConfig) (CanaryReceipt, error) {
	if config.Selector != CanarySelectorBinanceSpot {
		return CanaryReceipt{}, fmt.Errorf("%w: Binance Spot driver received %q", ErrCanaryConfiguration, config.Selector)
	}
	return runVenueCanary(ctx, config, dialBinanceSpotCanary, nil)
}

func RunBinanceDerivativeCanary(ctx context.Context, config CanaryConfig) (CanaryReceipt, error) {
	switch config.Selector {
	case CanarySelectorBinanceUSDMPublic, CanarySelectorBinanceUSDMMarket, CanarySelectorBinanceCoinM:
		return runVenueCanary(ctx, config, dialBinanceDerivativeCanary, nil)
	default:
		return CanaryReceipt{}, fmt.Errorf("%w: Binance derivative driver received %q", ErrCanaryConfiguration, config.Selector)
	}
}

type canaryIntervalBinding struct {
	start   capture.ClockReading
	endMono uint64
	endWall int64
}

type lockedCanaryRawSink struct {
	mu   sync.Mutex
	sink CanaryRawSink
}

func (s *lockedCanaryRawSink) WriteCanaryPayload(ctx context.Context, selector string, receivedAtUTCNS int64, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sink.WriteCanaryPayload(ctx, selector, receivedAtUTCNS, payload)
}

func RunBinanceUSDMAggregateCanary(ctx context.Context, config CanaryConfig) (CanaryReceipt, error) {
	if config.Selector != CanarySelectorBinanceUSDM {
		return CanaryReceipt{}, fmt.Errorf("%w: Binance USD-M aggregate driver received %q", ErrCanaryConfiguration, config.Selector)
	}
	if err := validateCanaryConfig(config); err != nil {
		return CanaryReceipt{}, err
	}
	start := config.Clock.Read()
	if config.DurationNS > math.MaxUint64-start.MonotonicNS || config.DurationNS > math.MaxInt64 || start.WallTimeNS > math.MaxInt64-int64(config.DurationNS) {
		return CanaryReceipt{}, fmt.Errorf("%w: aggregate interval overflow", ErrCanaryConfiguration)
	}
	binding := &canaryIntervalBinding{
		start: start, endMono: start.MonotonicNS + config.DurationNS,
		endWall: start.WallTimeNS + int64(config.DurationNS),
	}
	if config.RawSink != nil {
		config.RawSink = &lockedCanaryRawSink{sink: config.RawSink}
	}
	type routeResult struct {
		receipt CanaryReceipt
		err     error
	}
	results := make(chan routeResult, 2)
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	for _, selector := range []string{CanarySelectorBinanceUSDMPublic, CanarySelectorBinanceUSDMMarket} {
		routeConfig := config
		routeConfig.Selector = selector
		routeConfig.interval = binding
		go func() {
			receipt, err := RunBinanceDerivativeCanary(runCtx, routeConfig)
			results <- routeResult{receipt: receipt, err: err}
		}()
	}
	routes := make([]CanaryReceipt, 0, 2)
	for range 2 {
		result := <-results
		if result.err != nil {
			cancel()
			for len(routes) < 1 {
				other := <-results
				routes = append(routes, other.receipt)
			}
			return CanaryReceipt{}, result.err
		}
		if err := ValidateCanaryReceipt(result.receipt); err != nil {
			cancel()
			return CanaryReceipt{}, err
		}
		routes = append(routes, result.receipt)
	}
	slices.SortFunc(routes, func(a, b CanaryReceipt) int { return strings.Compare(a.Selector, b.Selector) })
	if len(routes) != 2 || routes[0].StartedAtUTCNS != routes[1].StartedAtUTCNS || routes[0].EndedAtUTCNS != routes[1].EndedAtUTCNS ||
		routes[0].DurationNS != routes[1].DurationNS || routes[0].TerminalReason != CanaryTerminalPlannedDuration || routes[1].TerminalReason != CanaryTerminalPlannedDuration {
		return CanaryReceipt{}, fmt.Errorf("%w: USD-M routes did not cover one exact successful interval", ErrCanaryConfiguration)
	}
	receipt := CanaryReceipt{
		Version: CanaryReceiptVersion, Selector: CanarySelectorBinanceUSDM,
		StartedAtUTCNS: routes[0].StartedAtUTCNS, EndedAtUTCNS: routes[0].EndedAtUTCNS, DurationNS: routes[0].DurationNS,
		TerminalReason: CanaryTerminalPlannedDuration,
	}
	rolling := sha256.New()
	for _, route := range routes {
		receipt.Messages += route.Messages
		receipt.Bytes += route.Bytes
		receipt.SubscriptionsRequested += route.SubscriptionsRequested
		receipt.SubscriptionsACKed += route.SubscriptionsACKed
		receipt.HeartbeatsSent += route.HeartbeatsSent
		receipt.HeartbeatsACKed += route.HeartbeatsACKed
		receipt.HeartbeatsInterrupted += route.HeartbeatsInterrupted
		receipt.Reconnects += route.Reconnects
		receipt.PlannedRotations += route.PlannedRotations
		receipt.ExplainedIntervals = append(receipt.ExplainedIntervals, route.ExplainedIntervals...)
		receipt.Routes = append(receipt.Routes, CanaryRouteEvidence{Selector: route.Selector, ReceiptSHA256: route.ReceiptSHA256})
		_, _ = rolling.Write([]byte(route.Selector))
		_, _ = rolling.Write([]byte{0})
		_, _ = rolling.Write([]byte(route.ReceiptSHA256))
	}
	receipt.RollingSHA256 = hex.EncodeToString(rolling.Sum(nil))
	receipt.ReceiptSHA256 = CanaryReceiptSHA256(receipt)
	if err := ValidateCanaryReceipt(receipt); err != nil {
		return CanaryReceipt{}, err
	}
	return receipt, nil
}

func RunBybitCanary(ctx context.Context, config CanaryConfig) (CanaryReceipt, error) {
	switch config.Selector {
	case CanarySelectorBybitSpot, CanarySelectorBybitLinear, CanarySelectorBybitInverse, CanarySelectorBybitOption:
		return runVenueCanary(ctx, config, dialBybitCanary, nil)
	default:
		return CanaryReceipt{}, fmt.Errorf("%w: Bybit driver received %q", ErrCanaryConfiguration, config.Selector)
	}
}

func RunOKXCanary(ctx context.Context, config CanaryConfig) (CanaryReceipt, error) {
	switch config.Selector {
	case CanarySelectorOKXSpot, CanarySelectorOKXSwap, CanarySelectorOKXFutures, CanarySelectorOKXOption:
		return runVenueCanary(ctx, config, dialOKXCanary, nil)
	default:
		return CanaryReceipt{}, fmt.Errorf("%w: OKX driver received %q", ErrCanaryConfiguration, config.Selector)
	}
}

func RunDeribitCanary(ctx context.Context, config CanaryConfig) (CanaryReceipt, error) {
	if config.Selector != CanarySelectorDeribitPublic100MS {
		return CanaryReceipt{}, fmt.Errorf("%w: Deribit driver supports public 100ms only; %s", ErrCanaryUnsupported, DeribitRaw1MSLimitation)
	}
	return runVenueCanary(ctx, config, dialDeribitCanary, []string{DeribitRaw1MSLimitation})
}

func RunHyperliquidCanary(ctx context.Context, config CanaryConfig) (CanaryReceipt, error) {
	switch config.Selector {
	case CanarySelectorHyperliquidMain, CanarySelectorHyperliquidSpot, CanarySelectorHyperliquidHIP3:
		return runVenueCanary(ctx, config, dialHyperliquidCanary, nil)
	default:
		return CanaryReceipt{}, fmt.Errorf("%w: Hyperliquid driver received %q", ErrCanaryConfiguration, config.Selector)
	}
}

func runVenueCanary(ctx context.Context, config CanaryConfig, realDial CanaryDialFunc, limitations []string) (CanaryReceipt, error) {
	if err := validateCanaryConfig(config); err != nil {
		return CanaryReceipt{}, err
	}
	expected, err := canarySubscriptions(config)
	if err != nil {
		return CanaryReceipt{}, err
	}
	dial := config.Dial
	if dial == nil {
		if config.HTTPClient == nil || config.HTTPClient.Transport == nil || config.HTTPClient.Timeout <= 0 {
			return CanaryReceipt{}, fmt.Errorf("%w: explicit bounded HTTP client is required", ErrCanaryConfiguration)
		}
		dial = realDial
	}
	r := newCanaryRun(config, expected, limitations, dial)
	return r.run(ctx)
}

func validateCanaryConfig(config CanaryConfig) error {
	if config.Clock == nil || config.DurationNS == 0 || config.MaxMessageBytes == 0 || config.Heartbeat.IntervalNS == 0 ||
		config.Heartbeat.TimeoutNS == 0 || config.Heartbeat.ACKTimeoutNS == 0 || config.Reconnect.MaxAttempts == 0 {
		return fmt.Errorf("%w: duration, byte cap, reconnect bound, heartbeat schedule, ACK timeout, and clock are required", ErrCanaryConfiguration)
	}
	reading := config.Clock.Read()
	if reading.ClockEpochID == "" || config.DurationNS > math.MaxUint64-reading.MonotonicNS {
		return fmt.Errorf("%w: invalid clock or duration overflow", ErrCanaryConfiguration)
	}
	if maximum := canaryVenueMaximum(config.Selector); maximum == 0 || config.MaxMessageBytes > maximum {
		return fmt.Errorf("%w: message byte cap exceeds the selected public adapter maximum", ErrCanaryConfiguration)
	}
	if config.Selector == CanarySelectorDeribitPublic100MS && config.Heartbeat.IntervalNS < deribit.MinimumHeartbeatNS {
		return fmt.Errorf("%w: Deribit heartbeat interval is below the public minimum", ErrCanaryConfiguration)
	}
	if config.Selector == CanarySelectorHyperliquidHIP3 {
		if config.DEX == "" {
			return fmt.Errorf("%w: HIP-3 requires explicit DEX identity", ErrCanaryConfiguration)
		}
	} else if config.DEX != "" {
		return fmt.Errorf("%w: DEX identity is valid only for HIP-3", ErrCanaryConfiguration)
	}
	return nil
}

func canaryVenueMaximum(selector string) uint32 {
	switch selector {
	case CanarySelectorBinanceSpot:
		return binance.SpotMaxRawPayloadBytes
	case CanarySelectorBinanceUSDM, CanarySelectorBinanceUSDMPublic, CanarySelectorBinanceUSDMMarket:
		return binance.USDMMaxRawPayloadBytes
	case CanarySelectorBinanceCoinM:
		return binance.CoinMMaxRawPayloadBytes
	case CanarySelectorBybitSpot, CanarySelectorBybitLinear, CanarySelectorBybitInverse, CanarySelectorBybitOption:
		return bybit.MaxRawPayloadBytes
	case CanarySelectorOKXSpot, CanarySelectorOKXSwap, CanarySelectorOKXFutures, CanarySelectorOKXOption:
		return okx.MaxRawPayloadBytes
	case CanarySelectorDeribitPublic100MS:
		return deribit.MaxRawPayloadBytes
	case CanarySelectorHyperliquidMain, CanarySelectorHyperliquidSpot, CanarySelectorHyperliquidHIP3:
		return hyperliquid.MaxRawPayloadBytes
	default:
		return 0
	}
}

func canarySubscriptions(config CanaryConfig) ([]string, error) {
	if config.Instrument == "" {
		return nil, fmt.Errorf("%w: explicit instrument is required", ErrCanaryConfiguration)
	}
	switch config.Selector {
	case CanarySelectorBinanceSpot:
		plan, err := binance.NewSpotSubscriptionPlan([]string{config.Instrument})
		if err != nil {
			return nil, err
		}
		return slices.Clone(plan.Inventory), nil
	case CanarySelectorBinanceUSDMPublic, CanarySelectorBinanceUSDMMarket:
		endpoint := binance.USDMPublicEndpoint
		if config.Selector == CanarySelectorBinanceUSDMMarket {
			endpoint = binance.USDMMarketEndpoint
		}
		plan, err := binance.NewUSDMDerivativeSubscriptionPlan(endpoint, []string{config.Instrument})
		if err != nil {
			return nil, err
		}
		return slices.Clone(plan.Inventory), nil
	case CanarySelectorBinanceCoinM:
		plan, err := binance.NewCoinMDerivativeSubscriptionPlan(binance.CoinMWebSocketEndpoint, []string{config.Instrument})
		if err != nil {
			return nil, err
		}
		return slices.Clone(plan.Inventory), nil
	case CanarySelectorBybitSpot, CanarySelectorBybitLinear, CanarySelectorBybitInverse:
		category := bybitCategory(config.Selector)
		topic, err := (bybit.TopicRequest{Role: bybit.RoleTrade, Symbol: config.Instrument}).Topic(category)
		if err != nil {
			return nil, err
		}
		return []string{topic}, nil
	case CanarySelectorBybitOption:
		topic, err := (bybit.OptionTopicRequest{Role: bybit.RoleBoundedOrderbook, Symbol: config.Instrument, Depth: bybit.OptionMinimumBookDepth}).Topic()
		if err != nil {
			return nil, err
		}
		return []string{topic}, nil
	case CanarySelectorOKXSpot, CanarySelectorOKXSwap, CanarySelectorOKXFutures, CanarySelectorOKXOption:
		arg := okx.SubscriptionArg{Channel: "trades", InstrumentID: config.Instrument}
		if err := arg.Validate(okx.PublicSocket, okx.Entitlement{}); err != nil {
			return nil, err
		}
		return []string{okxIdentity(arg)}, nil
	case CanarySelectorDeribitPublic100MS:
		channels, err := deribit.Channels(deribit.CadencePolicy{Requested: deribit.Cadence100MS}, []deribit.ChannelRequest{{Role: deribit.RoleTrade, Instrument: config.Instrument}})
		if err != nil {
			return nil, err
		}
		return channels, nil
	case CanarySelectorHyperliquidMain, CanarySelectorHyperliquidSpot, CanarySelectorHyperliquidHIP3:
		family := hyperliquidFamily(config.Selector)
		subscription := hyperliquid.Subscription{Type: hyperliquid.SubscriptionTrades, Coin: config.Instrument, DEX: config.DEX}
		if err := subscription.Validate(family, config.DEX); err != nil {
			return nil, err
		}
		return []string{subscription.StreamIdentity()}, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrCanaryUnsupported, config.Selector)
	}
}

type canaryRun struct {
	config            CanaryConfig
	dial              CanaryDialFunc
	expected          []string
	expectedSet       map[string]struct{}
	pending           map[string]struct{}
	receipt           CanaryReceipt
	rolling           hash.Hash
	startedMono       uint64
	endMono           uint64
	connectionAt      uint64
	ackDeadline       uint64
	nextHeartbeat     uint64
	heartbeatDue      uint64
	heartbeatID       uint64
	reconnectAttempts uint32
	gapStartUTCNS     int64
	gapReason         string
	connection        CanaryConnection
}

func newCanaryRun(config CanaryConfig, expected, limitations []string, dial CanaryDialFunc) *canaryRun {
	reading := config.Clock.Read()
	endMono := reading.MonotonicNS + config.DurationNS
	if config.interval != nil {
		reading = config.interval.start
		endMono = config.interval.endMono
	}
	set := make(map[string]struct{}, len(expected))
	for _, identity := range expected {
		set[identity] = struct{}{}
	}
	return &canaryRun{
		config:      config,
		dial:        dial,
		expected:    slices.Clone(expected),
		expectedSet: set,
		receipt: CanaryReceipt{
			Version:        CanaryReceiptVersion,
			Selector:       config.Selector,
			StartedAtUTCNS: reading.WallTimeNS,
			Limitations:    slices.Clone(limitations),
		},
		rolling:     sha256.New(),
		startedMono: reading.MonotonicNS,
		endMono:     endMono,
	}
}

func (r *canaryRun) run(ctx context.Context) (CanaryReceipt, error) {
	if err := ctx.Err(); err != nil {
		return r.fail(ctx, contextReason(err), err, r.config.Clock.Read().WallTimeNS, "caller context ended before connect")
	}
	if err := r.connect(ctx); err != nil {
		return r.fail(ctx, CanaryTerminalTransportFailure, err, r.config.Clock.Read().WallTimeNS, "initial public connection failed")
	}
	for {
		reading := r.config.Clock.Read()
		if err := ctx.Err(); err != nil {
			return r.fail(ctx, contextReason(err), err, reading.WallTimeNS, "caller context ended")
		}
		if reading.MonotonicNS >= r.endMono {
			if len(r.pending) > 0 {
				return r.fail(ctx, CanaryTerminalACKTimeout, ErrCanaryDeadline, reading.WallTimeNS, "planned duration ended before exact subscription ACK")
			}
			if r.heartbeatDue != 0 {
				return r.fail(ctx, CanaryTerminalHeartbeatTimeout, ErrCanaryDeadline, reading.WallTimeNS, "planned duration ended with heartbeat outstanding")
			}
			return r.succeed(ctx)
		}
		if len(r.pending) > 0 && reading.MonotonicNS >= r.ackDeadline {
			return r.fail(ctx, CanaryTerminalACKTimeout, ErrCanaryDeadline, reading.WallTimeNS, "subscription ACK deadline expired")
		}
		if r.heartbeatDue != 0 && reading.MonotonicNS >= r.heartbeatDue {
			return r.fail(ctx, CanaryTerminalHeartbeatTimeout, ErrCanaryDeadline, reading.WallTimeNS, "heartbeat acknowledgement deadline expired")
		}
		if len(r.pending) == 0 && r.connection.HeartbeatMode() == CanaryHeartbeatActive && r.heartbeatDue == 0 && reading.MonotonicNS >= r.nextHeartbeat {
			r.heartbeatID++
			if err := r.connection.Heartbeat(ctx, r.heartbeatID); err != nil {
				return r.fail(ctx, CanaryTerminalTransportFailure, err, reading.WallTimeNS, "heartbeat write failed")
			}
			r.receipt.HeartbeatsSent++
			r.heartbeatDue = boundedAdd(reading.MonotonicNS, r.config.Heartbeat.TimeoutNS)
		}
		if len(r.pending) == 0 && r.connection.HeartbeatMode() == CanaryHeartbeatChallenge && reading.MonotonicNS >= r.nextHeartbeat {
			return r.fail(ctx, CanaryTerminalHeartbeatTimeout, ErrCanaryDeadline, reading.WallTimeNS, "heartbeat challenge deadline expired")
		}
		rotationAt := uint64(math.MaxUint64)
		if interval := r.connection.RotationIntervalNS(); interval != 0 {
			rotationAt = boundedAdd(r.connectionAt, interval)
			if reading.MonotonicNS >= rotationAt {
				if err := r.reconnect(ctx, true, reading.WallTimeNS, "planned venue socket rotation"); err != nil {
					return r.fail(ctx, CanaryTerminalReconnectExhausted, err, r.config.Clock.Read().WallTimeNS, "planned rotation reconnect failed")
				}
				continue
			}
		}
		deadline := r.endMono
		switch {
		case len(r.pending) > 0:
			deadline = min(deadline, r.ackDeadline)
		case r.heartbeatDue != 0:
			deadline = min(deadline, r.heartbeatDue)
		default:
			deadline = min(deadline, r.nextHeartbeat)
		}
		deadline = min(deadline, rotationAt)
		event, err := r.connection.Read(ctx, deadline)
		if len(event.Payload) > 0 {
			if recordErr := r.recordPayload(ctx, event.Payload); recordErr != nil {
				return r.fail(ctx, CanaryTerminalTransportFailure, recordErr, r.config.Clock.Read().WallTimeNS, "raw sink rejected payload")
			}
		}
		if uint64(len(event.Payload)) > uint64(r.config.MaxMessageBytes) {
			return r.fail(ctx, CanaryTerminalOversize, fmt.Errorf("payload has %d bytes, cap is %d", len(event.Payload), r.config.MaxMessageBytes), r.config.Clock.Read().WallTimeNS, "received payload exceeded byte cap")
		}
		if err != nil {
			if errors.Is(err, ErrCanaryDeadline) {
				continue
			}
			if errors.Is(err, errCanaryACKMismatch) {
				return r.fail(ctx, CanaryTerminalACKMismatch, err, r.config.Clock.Read().WallTimeNS, "venue rejected or mismatched exact subscription ACK")
			}
			if errors.Is(err, errCanaryUnknownStream) {
				return r.fail(ctx, CanaryTerminalUnknownStream, err, r.config.Clock.Read().WallTimeNS, "venue emitted an unknown stream identity")
			}
			if errors.Is(err, errCanaryHeartbeatMismatch) {
				return r.fail(ctx, CanaryTerminalHeartbeatTimeout, err, r.config.Clock.Read().WallTimeNS, "venue heartbeat acknowledgement mismatched")
			}
			if errors.Is(err, errCanaryInvalidControl) {
				return r.fail(ctx, CanaryTerminalInvalidEvent, err, r.config.Clock.Read().WallTimeNS, "venue control message was invalid")
			}
			if ctx.Err() != nil {
				return r.fail(ctx, contextReason(ctx.Err()), ctx.Err(), r.config.Clock.Read().WallTimeNS, "caller context ended during read")
			}
			if r.heartbeatDue != 0 {
				r.receipt.HeartbeatsInterrupted++
				r.heartbeatDue = 0
				if reconnectErr := r.reconnect(ctx, false, r.config.Clock.Read().WallTimeNS, canaryInterruptedHeartbeatGapReason); reconnectErr != nil {
					return r.fail(ctx, CanaryTerminalReconnectExhausted, errors.Join(err, reconnectErr), r.config.Clock.Read().WallTimeNS, "read reconnect budget exhausted")
				}
				continue
			}
			if reconnectErr := r.reconnect(ctx, false, r.config.Clock.Read().WallTimeNS, "socket read failed"); reconnectErr != nil {
				return r.fail(ctx, CanaryTerminalReconnectExhausted, errors.Join(err, reconnectErr), r.config.Clock.Read().WallTimeNS, "read reconnect budget exhausted")
			}
			continue
		}
		if receipt, err := r.handleEvent(ctx, event); receipt != nil || err != nil {
			return *receipt, err
		}
	}
}

func (r *canaryRun) handleEvent(ctx context.Context, event CanaryEvent) (*CanaryReceipt, error) {
	reading := r.config.Clock.Read()
	switch event.Kind {
	case CanaryEventMessage:
		if _, ok := r.expectedSet[event.StreamIdentity]; !ok {
			receipt, err := r.fail(ctx, CanaryTerminalUnknownStream, fmt.Errorf("unknown stream %q", event.StreamIdentity), reading.WallTimeNS, "unknown stream identity")
			return &receipt, err
		}
	case CanaryEventSubscriptionACK:
		if len(event.ACKIdentities) == 0 {
			receipt, err := r.fail(ctx, CanaryTerminalACKMismatch, errors.New("empty subscription ACK"), reading.WallTimeNS, "empty subscription ACK")
			return &receipt, err
		}
		for _, identity := range event.ACKIdentities {
			if _, ok := r.pending[identity]; !ok {
				receipt, err := r.fail(ctx, CanaryTerminalACKMismatch, fmt.Errorf("unexpected or duplicate ACK %q", identity), reading.WallTimeNS, "subscription ACK mismatch")
				return &receipt, err
			}
			delete(r.pending, identity)
			r.receipt.SubscriptionsACKed++
		}
		if len(r.pending) == 0 {
			r.closeGap(reading.WallTimeNS)
			r.scheduleNextHeartbeat(reading.MonotonicNS)
		}
	case CanaryEventHeartbeatACK:
		if r.heartbeatDue == 0 {
			receipt, err := r.fail(ctx, CanaryTerminalInvalidEvent, errors.New("unexpected heartbeat ACK"), reading.WallTimeNS, "unexpected heartbeat acknowledgement")
			return &receipt, err
		}
		r.receipt.HeartbeatsACKed++
		r.heartbeatDue = 0
		r.scheduleNextHeartbeat(reading.MonotonicNS)
	case CanaryEventHeartbeatChallenge:
		if r.connection.HeartbeatMode() != CanaryHeartbeatChallenge || len(r.pending) != 0 {
			receipt, err := r.fail(ctx, CanaryTerminalInvalidEvent, errors.New("unexpected heartbeat challenge"), reading.WallTimeNS, "unexpected heartbeat challenge")
			return &receipt, err
		}
		r.heartbeatID++
		if err := r.connection.Heartbeat(ctx, r.heartbeatID); err != nil {
			receipt, runErr := r.fail(ctx, CanaryTerminalTransportFailure, err, reading.WallTimeNS, "heartbeat response write failed")
			return &receipt, runErr
		}
		r.receipt.HeartbeatsSent++
		r.receipt.HeartbeatsACKed++
		r.scheduleNextHeartbeat(reading.MonotonicNS)
	case CanaryEventDisconnect:
		reason := "socket disconnected"
		if r.heartbeatDue != 0 {
			r.receipt.HeartbeatsInterrupted++
			r.heartbeatDue = 0
			reason = canaryInterruptedHeartbeatGapReason
		}
		if err := r.reconnect(ctx, event.Planned, reading.WallTimeNS, reason); err != nil {
			receipt, runErr := r.fail(ctx, CanaryTerminalReconnectExhausted, err, r.config.Clock.Read().WallTimeNS, "disconnect reconnect budget exhausted")
			return &receipt, runErr
		}
	case CanaryEventControl:
		// A venue-owned heartbeat observation carries no application stream
		// identity. It proves challenge-mode liveness without fabricating a
		// client send/ACK pair.
		if len(r.pending) == 0 && r.connection.HeartbeatMode() == CanaryHeartbeatChallenge {
			r.scheduleNextHeartbeat(reading.MonotonicNS)
		}
	default:
		receipt, err := r.fail(ctx, CanaryTerminalInvalidEvent, fmt.Errorf("unknown event kind %d", event.Kind), reading.WallTimeNS, "unknown canary event")
		return &receipt, err
	}
	return nil, nil
}

func (r *canaryRun) scheduleNextHeartbeat(now uint64) {
	delay := r.config.Heartbeat.IntervalNS
	if r.connection.HeartbeatMode() == CanaryHeartbeatChallenge {
		delay = boundedAdd(delay, r.config.Heartbeat.TimeoutNS)
	}
	r.nextHeartbeat = boundedAdd(now, delay)
}

func (r *canaryRun) connect(ctx context.Context) error {
	connection, err := r.dial(ctx, r.config)
	if err != nil {
		return err
	}
	if connection == nil || !slices.Equal(connection.Subscriptions(), r.expected) ||
		(connection.HeartbeatMode() != CanaryHeartbeatActive && connection.HeartbeatMode() != CanaryHeartbeatChallenge) {
		if connection != nil {
			_ = connection.Close()
		}
		return fmt.Errorf("%w: dialer did not bind exact subscription inventory", ErrCanaryConfiguration)
	}
	if err := connection.Subscribe(ctx); err != nil {
		_ = connection.Close()
		return err
	}
	r.connection = connection
	reading := r.config.Clock.Read()
	r.connectionAt = reading.MonotonicNS
	r.pending = make(map[string]struct{}, len(r.expected))
	for _, identity := range r.expected {
		r.pending[identity] = struct{}{}
	}
	r.receipt.SubscriptionsRequested += uint64(len(r.expected))
	r.ackDeadline = boundedAdd(reading.MonotonicNS, r.config.Heartbeat.ACKTimeoutNS)
	r.heartbeatDue = 0
	r.scheduleNextHeartbeat(reading.MonotonicNS)
	return nil
}
func (r *canaryRun) reconnect(ctx context.Context, planned bool, startedUTCNS int64, reason string) error {
	if r.connection != nil {
		_ = r.connection.Close()
		r.connection = nil
	}
	if r.gapStartUTCNS == 0 {
		r.gapStartUTCNS = startedUTCNS
		r.gapReason = reason
	}
	if planned {
		r.receipt.PlannedRotations++
	} else {
		r.receipt.Reconnects++
	}
	var lastErr error
	for r.reconnectAttempts < r.config.Reconnect.MaxAttempts {
		r.reconnectAttempts++
		if r.config.Reconnect.BackoffNS != 0 {
			target := boundedAdd(r.config.Clock.Read().MonotonicNS, r.config.Reconnect.BackoffNS)
			if err := r.config.Clock.WaitUntil(ctx, target); err != nil {
				return err
			}
		}
		if err := r.connect(ctx); err == nil {
			return nil
		} else {
			lastErr = err
		}
	}
	if lastErr == nil {
		lastErr = ErrCanaryConfiguration
	}
	return lastErr
}

func (r *canaryRun) recordPayload(ctx context.Context, payload []byte) error {
	reading := r.config.Clock.Read()
	if r.config.RawSink != nil {
		if err := r.config.RawSink.WriteCanaryPayload(ctx, r.config.Selector, reading.WallTimeNS, slices.Clone(payload)); err != nil {
			return err
		}
	}
	digest := sha256.Sum256(payload)
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(payload)))
	_, _ = r.rolling.Write(length[:])
	_, _ = r.rolling.Write(digest[:])
	binary.BigEndian.PutUint64(length[:], uint64(reading.WallTimeNS))
	_, _ = r.rolling.Write(length[:])
	r.receipt.Messages++
	r.receipt.Bytes += uint64(len(payload))
	return nil
}

func (r *canaryRun) closeGap(endedUTCNS int64) {
	if r.gapStartUTCNS == 0 {
		return
	}
	r.receipt.ExplainedIntervals = append(r.receipt.ExplainedIntervals, CanaryInterval{StartedAtUTCNS: r.gapStartUTCNS, EndedAtUTCNS: endedUTCNS, Reason: r.gapReason})
	r.gapStartUTCNS = 0
	r.gapReason = ""
}

func (r *canaryRun) succeed(ctx context.Context) (CanaryReceipt, error) {
	if r.connection != nil {
		if err := r.connection.Close(); err != nil {
			return r.fail(ctx, CanaryTerminalTransportFailure, err, r.config.Clock.Read().WallTimeNS, "normal close failed")
		}
		r.connection = nil
	}
	return r.finish(CanaryTerminalPlannedDuration), nil
}

func (r *canaryRun) fail(_ context.Context, reason CanaryTerminalReason, cause error, startedUTCNS int64, intervalReason string) (CanaryReceipt, error) {
	if r.connection != nil {
		_ = r.connection.Close()
		r.connection = nil
	}
	ended := r.config.Clock.Read().WallTimeNS
	if r.gapStartUTCNS != 0 {
		startedUTCNS = r.gapStartUTCNS
		intervalReason = r.gapReason + "; " + intervalReason
		r.gapStartUTCNS = 0
	}
	if startedUTCNS > ended {
		startedUTCNS = ended
	}
	r.receipt.UnexplainedIntervals = append(r.receipt.UnexplainedIntervals, CanaryInterval{StartedAtUTCNS: startedUTCNS, EndedAtUTCNS: ended, Reason: intervalReason})
	receipt := r.finish(reason)
	return receipt, &CanaryError{Reason: reason, Cause: cause}
}

func (r *canaryRun) finish(reason CanaryTerminalReason) CanaryReceipt {
	reading := r.config.Clock.Read()
	r.receipt.EndedAtUTCNS = reading.WallTimeNS
	if reading.MonotonicNS >= r.startedMono {
		r.receipt.DurationNS = reading.MonotonicNS - r.startedMono
	}
	if reason == CanaryTerminalPlannedDuration && r.config.interval != nil {
		r.receipt.EndedAtUTCNS = r.config.interval.endWall
		r.receipt.DurationNS = r.config.DurationNS
	}
	r.receipt.RollingSHA256 = hex.EncodeToString(r.rolling.Sum(nil))
	r.receipt.TerminalReason = reason
	r.receipt.ReceiptSHA256 = CanaryReceiptSHA256(r.receipt)
	return r.receipt
}

func contextReason(err error) CanaryTerminalReason {
	if errors.Is(err, context.DeadlineExceeded) {
		return CanaryTerminalCallerDeadline
	}
	return CanaryTerminalCallerCanceled
}

func boundedAdd(a, b uint64) uint64 {
	if b > math.MaxUint64-a {
		return math.MaxUint64
	}
	return a + b
}

func CanaryReceiptSHA256(receipt CanaryReceipt) string {
	material := struct {
		Version                uint16
		Selector               string
		StartedAtUTCNS         int64
		EndedAtUTCNS           int64
		DurationNS             uint64
		Messages               uint64
		Bytes                  uint64
		RollingSHA256          string
		SubscriptionsRequested uint64
		SubscriptionsACKed     uint64
		HeartbeatsSent         uint64
		HeartbeatsACKed        uint64
		HeartbeatsInterrupted  uint64
		Reconnects             uint32
		PlannedRotations       uint32
		ExplainedIntervals     []CanaryInterval
		UnexplainedIntervals   []CanaryInterval
		Limitations            []string
		TerminalReason         CanaryTerminalReason
	}{
		Version: receipt.Version, Selector: receipt.Selector, StartedAtUTCNS: receipt.StartedAtUTCNS,
		EndedAtUTCNS: receipt.EndedAtUTCNS, DurationNS: receipt.DurationNS, Messages: receipt.Messages,
		Bytes: receipt.Bytes, RollingSHA256: receipt.RollingSHA256, SubscriptionsRequested: receipt.SubscriptionsRequested,
		SubscriptionsACKed: receipt.SubscriptionsACKed, HeartbeatsSent: receipt.HeartbeatsSent,
		HeartbeatsACKed: receipt.HeartbeatsACKed, HeartbeatsInterrupted: receipt.HeartbeatsInterrupted,
		Reconnects: receipt.Reconnects, PlannedRotations: receipt.PlannedRotations,
		ExplainedIntervals: receipt.ExplainedIntervals, UnexplainedIntervals: receipt.UnexplainedIntervals,
		Limitations: receipt.Limitations, TerminalReason: receipt.TerminalReason,
	}
	encoded, err := json.Marshal(material)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func ValidateCanaryReceipt(receipt CanaryReceipt) error {
	switch {
	case receipt.Version != CanaryReceiptVersion:
		return fmt.Errorf("%w: unsupported receipt version %d", ErrCanaryConfiguration, receipt.Version)
	case canaryVenueMaximum(receipt.Selector) == 0:
		return fmt.Errorf("%w: unsupported selector %q", ErrCanaryConfiguration, receipt.Selector)
	case receipt.EndedAtUTCNS < receipt.StartedAtUTCNS:
		return fmt.Errorf("%w: receipt ends before it starts", ErrCanaryConfiguration)
	case !validCanaryTerminalReason(receipt.TerminalReason):
		return fmt.Errorf("%w: invalid terminal reason %q", ErrCanaryConfiguration, receipt.TerminalReason)
	case receipt.SubscriptionsACKed > receipt.SubscriptionsRequested:
		return fmt.Errorf("%w: acknowledged subscriptions exceed requests", ErrCanaryConfiguration)
	case receipt.HeartbeatsACKed > receipt.HeartbeatsSent:
		return fmt.Errorf("%w: acknowledged heartbeats exceed sends", ErrCanaryConfiguration)
	case receipt.HeartbeatsInterrupted > receipt.HeartbeatsSent-receipt.HeartbeatsACKed:
		return fmt.Errorf("%w: accounted heartbeats exceed sends", ErrCanaryConfiguration)
	case receipt.HeartbeatsInterrupted > uint64(receipt.Reconnects)+uint64(receipt.PlannedRotations):
		return fmt.Errorf("%w: interrupted heartbeats exceed reconnects", ErrCanaryConfiguration)
	case !validSHA256Hex(receipt.RollingSHA256):
		return fmt.Errorf("%w: invalid rolling payload digest", ErrCanaryConfiguration)
	case !validSHA256Hex(receipt.ReceiptSHA256):
		return fmt.Errorf("%w: invalid receipt digest", ErrCanaryConfiguration)
	}
	if receipt.Selector == CanarySelectorDeribitPublic100MS && !slices.Contains(receipt.Limitations, DeribitRaw1MSLimitation) {
		return fmt.Errorf("%w: Deribit limitation is missing", ErrCanaryConfiguration)
	}
	if receipt.TerminalReason == CanaryTerminalPlannedDuration {
		if len(receipt.UnexplainedIntervals) != 0 {
			return fmt.Errorf("%w: successful receipt has unexplained intervals", ErrCanaryConfiguration)
		}
		if receipt.SubscriptionsACKed != receipt.SubscriptionsRequested {
			return fmt.Errorf("%w: successful receipt has incomplete subscription acknowledgements", ErrCanaryConfiguration)
		}
		if receipt.HeartbeatsACKed != receipt.HeartbeatsSent-receipt.HeartbeatsInterrupted {
			return fmt.Errorf("%w: successful receipt has incomplete heartbeat accounting", ErrCanaryConfiguration)
		}
		interruptedIntervals := 0
		for _, interval := range receipt.ExplainedIntervals {
			if interval.Reason == canaryInterruptedHeartbeatGapReason {
				interruptedIntervals++
			}
		}
		if uint64(interruptedIntervals) != receipt.HeartbeatsInterrupted {
			return fmt.Errorf("%w: successful receipt heartbeat interruptions do not match explained intervals", ErrCanaryConfiguration)
		}
	} else if len(receipt.UnexplainedIntervals) == 0 {
		return fmt.Errorf("%w: unsuccessful receipt has no unexplained interval", ErrCanaryConfiguration)
	}
	for _, intervals := range [][]CanaryInterval{receipt.ExplainedIntervals, receipt.UnexplainedIntervals} {
		for _, interval := range intervals {
			if interval.Reason == "" {
				return fmt.Errorf("%w: interval reason is empty", ErrCanaryConfiguration)
			}
			if interval.EndedAtUTCNS < interval.StartedAtUTCNS {
				return fmt.Errorf("%w: interval ends before it starts", ErrCanaryConfiguration)
			}
			if interval.StartedAtUTCNS < receipt.StartedAtUTCNS || interval.EndedAtUTCNS > receipt.EndedAtUTCNS {
				return fmt.Errorf("%w: interval lies outside receipt bounds", ErrCanaryConfiguration)
			}
		}
	}
	if CanaryReceiptSHA256(receipt) != receipt.ReceiptSHA256 {
		return fmt.Errorf("%w: receipt digest does not match content", ErrCanaryConfiguration)
	}
	return nil
}

func validCanaryTerminalReason(reason CanaryTerminalReason) bool {
	switch reason {
	case CanaryTerminalPlannedDuration, CanaryTerminalCallerCanceled, CanaryTerminalCallerDeadline,
		CanaryTerminalACKTimeout, CanaryTerminalACKMismatch, CanaryTerminalHeartbeatTimeout,
		CanaryTerminalUnknownStream, CanaryTerminalOversize, CanaryTerminalReconnectExhausted,
		CanaryTerminalTransportFailure, CanaryTerminalInvalidEvent:
		return true
	default:
		return false
	}
}

func validSHA256Hex(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

type canaryAsyncRead[T any] struct {
	value T
	err   error
}

type canaryAsyncReader[T any] struct {
	cancel  context.CancelFunc
	results <-chan canaryAsyncRead[T]
	once    sync.Once
}

func newCanaryAsyncReader[T any](read func(context.Context) (T, error)) *canaryAsyncReader[T] {
	readContext, cancel := context.WithCancel(context.Background())
	results := make(chan canaryAsyncRead[T])
	go func() {
		defer close(results)
		for {
			value, err := read(readContext)
			select {
			case <-readContext.Done():
				return
			case results <- canaryAsyncRead[T]{value: value, err: err}:
			}
			if err != nil {
				return
			}
		}
	}()
	return &canaryAsyncReader[T]{cancel: cancel, results: results}
}

func (r *canaryAsyncReader[T]) Read(ctx context.Context, clock CanaryClock, deadlineNS uint64) (T, error) {
	var zero T
	if r == nil {
		return zero, errors.New("verify: canary asynchronous reader is not configured")
	}
	now := clock.Read().MonotonicNS
	if now >= deadlineNS {
		return zero, ErrCanaryDeadline
	}
	delta := deadlineNS - now
	if delta > uint64(math.MaxInt64) {
		delta = uint64(math.MaxInt64)
	}
	timer := time.NewTimer(time.Duration(delta))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-timer.C:
		return zero, ErrCanaryDeadline
	case result, ok := <-r.results:
		if !ok {
			return zero, io.EOF
		}
		return result.value, result.err
	}
}

func (r *canaryAsyncReader[T]) Close() {
	if r != nil {
		r.once.Do(r.cancel)
	}
}

type binanceDerivativeCanarySink struct{}

func (*binanceDerivativeCanarySink) WriteRaw(context.Context, capture.EnvelopeV1) error {
	return nil
}

func (*binanceDerivativeCanarySink) Commit(context.Context, capture.EpochCommit) error {
	return nil
}

func (*binanceDerivativeCanarySink) CloseEpoch(context.Context, capture.EpochClose) error {
	return nil
}

func binanceDerivativeStepPayload(result capture.StepResult) []byte {
	for index := len(result.Envelopes) - 1; index >= 0; index-- {
		envelope := result.Envelopes[index]
		if envelope.RecordKind == capture.RecordKindWebSocket && len(envelope.RawPayload) != 0 {
			return slices.Clone(envelope.RawPayload)
		}
	}
	for index := len(result.Envelopes) - 1; index >= 0; index-- {
		if len(result.Envelopes[index].RawPayload) != 0 {
			return slices.Clone(result.Envelopes[index].RawPayload)
		}
	}
	return nil
}

type binanceDerivativeCanaryStep struct {
	result capture.StepResult
}

type binanceDerivativeCanaryConnection struct {
	clock        CanaryClock
	capture      *binance.DerivativeCapture
	reader       *canaryAsyncReader[binanceDerivativeCanaryStep]
	expected     []string
	requests     map[int64][]string
	acknowledged map[int64]struct{}
	lastPayload  []byte
	closeTimeout time.Duration
	acked        bool
	closed       bool
}

func dialBinanceDerivativeCanary(ctx context.Context, config CanaryConfig) (CanaryConnection, error) {
	if config.RateBudgets.BinanceDerivatives == nil {
		return nil, fmt.Errorf("%w: caller-owned Binance derivative connection budget is required", ErrCanaryConfiguration)
	}
	endpoint := binance.CoinMWebSocketEndpoint
	if config.Selector == CanarySelectorBinanceUSDMPublic {
		endpoint = binance.USDMPublicEndpoint
	} else if config.Selector == CanarySelectorBinanceUSDMMarket {
		endpoint = binance.USDMMarketEndpoint
	}
	digest := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d", config.Selector, config.Clock.Read().ClockEpochID, config.Clock.Read().MonotonicNS)))
	var epochID [16]byte
	copy(epochID[:], digest[:16])
	sink := &binanceDerivativeCanarySink{}
	derivativeConfig := binance.DerivativeWSConfig{
		Symbols: []string{config.Instrument}, RecorderVersion: "verify-canary-v1", Endpoint: endpoint,
		Epochs: []capture.StreamEpoch{{Kind: capture.EpochConnection, ID: epochID}},
	}
	var adapter *binance.DerivativeCapture
	var err error
	if config.Selector == CanarySelectorBinanceCoinM {
		adapter, err = binance.NewCoinMDerivativeCapture(derivativeConfig, config.HTTPClient, config.Clock, config.RateBudgets.BinanceDerivatives, sink)
	} else {
		adapter, err = binance.NewUSDMDerivativeCapture(derivativeConfig, config.HTTPClient, config.Clock, config.RateBudgets.BinanceDerivatives, sink)
	}
	if err != nil {
		return nil, err
	}
	plan := adapter.SubscriptionPlan()
	requests := make(map[int64][]string, len(plan.Requests))
	for _, request := range plan.Requests {
		requests[request.ID] = slices.Clone(request.Streams)
	}
	return &binanceDerivativeCanaryConnection{
		clock: config.Clock, capture: adapter, expected: plan.Inventory,
		requests: requests, acknowledged: make(map[int64]struct{}, len(requests)),
		closeTimeout: config.HTTPClient.Timeout,
	}, nil
}

func (c *binanceDerivativeCanaryConnection) Subscriptions() []string {
	return slices.Clone(c.expected)
}

func (c *binanceDerivativeCanaryConnection) Subscribe(ctx context.Context) error {
	result, err := c.capture.Start(ctx)
	for steps := 0; steps < 8; steps++ {
		if err != nil {
			return err
		}
		for _, control := range result.Controls {
			if control.Envelope.ControlKind.Valid && control.Envelope.ControlKind.Value == capture.ControlSubscribeRequest {
				c.reader = newCanaryAsyncReader(func(readContext context.Context) (binanceDerivativeCanaryStep, error) {
					stepResult, stepErr := c.capture.Step(readContext)
					return binanceDerivativeCanaryStep{result: stepResult}, stepErr
				})
				return nil
			}
		}
		result, err = c.capture.Step(ctx)
	}
	return errors.New("verify: derivative capture did not issue bounded subscription request")
}

func (c *binanceDerivativeCanaryConnection) Read(ctx context.Context, deadline uint64) (CanaryEvent, error) {
	for steps := 0; steps < 16; steps++ {
		step, stepErr := c.reader.Read(ctx, c.clock, deadline)
		result := step.result
		payload := binanceDerivativeStepPayload(result)
		if len(payload) != 0 {
			c.lastPayload = payload
		} else if len(result.Faults) != 0 {
			payload = slices.Clone(c.lastPayload)
		}
		if stepErr != nil {
			return CanaryEvent{Payload: payload}, stepErr
		}
		for _, fault := range result.Faults {
			switch fault.Kind {
			case capture.FaultACKPartial, capture.FaultACKWrong, capture.FaultACKDuplicate, capture.FaultACKRejected, capture.FaultACKTimeout, capture.FaultACKOverflow:
				return CanaryEvent{Payload: payload}, fmt.Errorf("%w: derivative capture fault %d", errCanaryACKMismatch, fault.Kind)
			case capture.FaultHeartbeatMissed, capture.FaultHeartbeatEarly:
				return CanaryEvent{Payload: payload}, fmt.Errorf("%w: derivative capture fault %d", errCanaryHeartbeatMismatch, fault.Kind)
			case capture.FaultSchemaUnknownRole, capture.FaultSchemaMalformed, capture.FaultSchemaTypeChanged:
				var wrapper struct {
					Stream string `json:"stream"`
				}
				_ = json.Unmarshal(payload, &wrapper)
				return CanaryEvent{Payload: payload}, fmt.Errorf("%w: derivative capture fault %d on stream %q", errCanaryUnknownStream, fault.Kind, wrapper.Stream)
			case capture.FaultSchemaOversized:
				return CanaryEvent{Payload: payload}, fmt.Errorf("%w: derivative payload exceeded adapter bound", errCanaryUnknownStream)
			case capture.FaultDisconnectAbrupt:
				return CanaryEvent{Kind: CanaryEventDisconnect, Payload: payload}, nil
			}
		}
		for _, control := range result.Controls {
			if !control.Envelope.ControlKind.Valid || control.Envelope.ControlKind.Value != capture.ControlAcknowledgement {
				continue
			}
			var acknowledgement struct {
				Result json.RawMessage `json:"result"`
				ID     int64           `json:"id"`
			}
			raw := control.Envelope.RawPayload
			if json.Unmarshal(raw, &acknowledgement) != nil || acknowledgement.ID == 0 || string(acknowledgement.Result) != "null" {
				return CanaryEvent{Payload: payload}, fmt.Errorf("%w: malformed derivative acknowledgement", errCanaryACKMismatch)
			}
			identities, ok := c.requests[acknowledgement.ID]
			if !ok {
				return CanaryEvent{Payload: payload}, fmt.Errorf("%w: unknown derivative request %d", errCanaryACKMismatch, acknowledgement.ID)
			}
			if _, duplicate := c.acknowledged[acknowledgement.ID]; duplicate {
				return CanaryEvent{Payload: payload}, fmt.Errorf("%w: duplicate derivative request %d", errCanaryACKMismatch, acknowledgement.ID)
			}
			c.acknowledged[acknowledgement.ID] = struct{}{}
			c.acked = len(c.acknowledged) == len(c.requests)
			if len(payload) == 0 {
				payload = slices.Clone(raw)
			}
			return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: payload, ACKIdentities: slices.Clone(identities)}, nil
		}
		for _, opportunity := range result.Opportunities {
			if opportunity.Expectation == capture.OpportunitySubscriptionInventory && opportunity.TerminalOutcome == capture.OpportunityOutcomeObserved {
				c.acked = true
				return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: payload, ACKIdentities: slices.Clone(c.expected)}, nil
			}
			if opportunity.Expectation == capture.OpportunityHeartbeatDeadline &&
				(opportunity.TerminalOutcome == capture.OpportunityOutcomeObserved || opportunity.TerminalOutcome == capture.OpportunityOutcomeObservedUnchanged) {
				return CanaryEvent{Kind: CanaryEventHeartbeatChallenge, Payload: payload}, nil
			}
		}
		if len(payload) != 0 {
			if c.acked {
				return CanaryEvent{Kind: CanaryEventMessage, Payload: payload, StreamIdentity: c.expected[0]}, nil
			}
			return CanaryEvent{Kind: CanaryEventControl, Payload: payload}, nil
		}
		if result.State == capture.RunnerClosed {
			return CanaryEvent{Kind: CanaryEventDisconnect, Planned: true}, nil
		}
	}
	return CanaryEvent{}, errors.New("verify: derivative capture exceeded bounded control steps")
}

func (*binanceDerivativeCanaryConnection) Heartbeat(context.Context, uint64) error { return nil }
func (*binanceDerivativeCanaryConnection) HeartbeatMode() CanaryHeartbeatMode {
	return CanaryHeartbeatChallenge
}
func (*binanceDerivativeCanaryConnection) RotationIntervalNS() uint64 { return 0 }
func (c *binanceDerivativeCanaryConnection) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), c.closeTimeout)
	defer cancel()
	_, err := c.capture.Close(ctx)
	if errors.Is(err, capture.ErrRunnerClosed) {
		return nil
	}
	return err
}

// Binance Spot

type binanceSpotCanaryConnection struct {
	clock        CanaryClock
	connection   binance.SpotWSConnection
	plan         binance.SpotSubscriptionPlan
	reader       *canaryAsyncReader[binance.SpotWSFrame]
	maximum      uint32
	closeTimeout time.Duration
	pendingPing  []byte
}

func dialBinanceSpotCanary(ctx context.Context, config CanaryConfig) (CanaryConnection, error) {
	connector, err := binance.NewCoderSpotWSConnector(config.HTTPClient)
	if err != nil {
		return nil, err
	}
	connection, err := connector.Connect(ctx, binance.SpotWSConnectRequest{Endpoint: binance.SpotWSEndpoint, MaxApplicationBytes: config.MaxMessageBytes})
	if err != nil {
		return nil, err
	}
	plan, err := binance.NewSpotSubscriptionPlan([]string{config.Instrument})
	if err != nil {
		_ = connection.Close(ctx, capture.CloseACKRejected)
		return nil, err
	}
	canary := &binanceSpotCanaryConnection{
		clock: config.Clock, connection: connection, plan: plan,
		maximum: config.MaxMessageBytes, closeTimeout: config.HTTPClient.Timeout,
	}
	canary.reader = newCanaryAsyncReader(func(readContext context.Context) (binance.SpotWSFrame, error) {
		return connection.Read(readContext, canary.maximum)
	})
	return canary, nil
}

func (c *binanceSpotCanaryConnection) Subscriptions() []string { return slices.Clone(c.plan.Inventory) }
func (c *binanceSpotCanaryConnection) Subscribe(ctx context.Context) error {
	for _, request := range c.plan.Requests {
		if err := c.connection.Write(ctx, binance.SpotWSWriteText, request.Raw); err != nil {
			return err
		}
	}
	return nil
}
func (c *binanceSpotCanaryConnection) Read(ctx context.Context, deadline uint64) (CanaryEvent, error) {
	frame, err := c.reader.Read(ctx, c.clock, deadline)
	if err != nil {
		return CanaryEvent{}, err
	}
	switch frame.Kind {
	case binance.SpotWSFramePing:
		c.pendingPing = slices.Clone(frame.Payload)
		return CanaryEvent{Kind: CanaryEventHeartbeatChallenge, Payload: frame.Payload}, nil
	case binance.SpotWSFrameClose:
		return CanaryEvent{Kind: CanaryEventDisconnect, Payload: frame.Payload}, nil
	case binance.SpotWSFramePong:
		return CanaryEvent{Kind: CanaryEventControl, Payload: frame.Payload}, nil
	case binance.SpotWSFrameText:
		var ack struct {
			Result json.RawMessage `json:"result"`
			ID     int64           `json:"id"`
		}
		if json.Unmarshal(frame.Payload, &ack) == nil && ack.ID != 0 && string(ack.Result) == "null" {
			for _, request := range c.plan.Requests {
				if request.ID == ack.ID {
					return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: frame.Payload, ACKIdentities: slices.Clone(request.Streams)}, nil
				}
			}
			return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: frame.Payload, ACKIdentities: []string{fmt.Sprintf("unknown-request-%d", ack.ID)}}, nil
		}
		streamIdentity, classifyErr := classifyBinanceSpotCanaryStream(frame.Payload)
		if classifyErr != nil {
			return CanaryEvent{Payload: frame.Payload}, classifyErr
		}
		return CanaryEvent{Kind: CanaryEventMessage, Payload: frame.Payload, StreamIdentity: streamIdentity}, nil
	default:
		return CanaryEvent{}, errors.New("binance: unsupported canary frame")
	}
}
func classifyBinanceSpotCanaryStream(payload []byte) (string, error) {
	var object map[string]json.RawMessage
	if json.Unmarshal(payload, &object) != nil {
		return "", fmt.Errorf("%w: Binance Spot payload is not an object", errCanaryUnknownStream)
	}
	var event, symbol string
	if raw, ok := object["e"]; ok && json.Unmarshal(raw, &event) != nil {
		return "", fmt.Errorf("%w: Binance Spot event identity changed type", errCanaryUnknownStream)
	}
	if raw, ok := object["s"]; !ok || json.Unmarshal(raw, &symbol) != nil || symbol == "" {
		keys := make([]string, 0, len(object))
		for key := range object {
			keys = append(keys, key)
		}
		slices.Sort(keys)
		return "", fmt.Errorf("%w: Binance Spot payload has no symbol identity; top-level keys %q", errCanaryUnknownStream, keys)
	}
	suffix := ""
	switch event {
	case "trade":
		suffix = "@trade"
	case "depthUpdate":
		suffix = "@depth@100ms"
	case "24hrTicker":
		suffix = "@ticker"
	case "":
		var updateID uint64
		var bestBid, bestAsk string
		if json.Unmarshal(object["u"], &updateID) == nil && updateID != 0 &&
			json.Unmarshal(object["b"], &bestBid) == nil && bestBid != "" &&
			json.Unmarshal(object["a"], &bestAsk) == nil && bestAsk != "" {
			suffix = "@bookTicker"
		}
	}
	if suffix == "" {
		return "", fmt.Errorf("%w: Binance Spot event %q is not subscribed", errCanaryUnknownStream, event)
	}
	return strings.ToLower(symbol) + suffix, nil
}

func (c *binanceSpotCanaryConnection) Heartbeat(ctx context.Context, _ uint64) error {
	if c.pendingPing == nil {
		return errors.New("binance: no pending server ping")
	}
	err := c.connection.Write(ctx, binance.SpotWSWritePong, c.pendingPing)
	c.pendingPing = nil
	return err
}
func (*binanceSpotCanaryConnection) HeartbeatMode() CanaryHeartbeatMode {
	return CanaryHeartbeatChallenge
}
func (*binanceSpotCanaryConnection) RotationIntervalNS() uint64 { return binance.SpotSocketLifetimeNS }
func (c *binanceSpotCanaryConnection) Close() error {
	c.reader.Close()
	ctx, cancel := context.WithTimeout(context.Background(), c.closeTimeout)
	defer cancel()
	return c.connection.Close(ctx, capture.ClosePlanned)
}

// Bybit

type bybitCanaryConnection struct {
	clock         CanaryClock
	subscriptions []string
	subscribe     func(context.Context) ([]string, error)
	ping          func(context.Context, string) error
	read          func(context.Context) ([]byte, error)
	reader        *canaryAsyncReader[[]byte]
	close         func() error
	requestIDs    map[string][]string
	pendingPingID string
	option        bool
}

func dialBybitCanary(ctx context.Context, config CanaryConfig) (CanaryConnection, error) {
	connection := &bybitCanaryConnection{clock: config.Clock, requestIDs: make(map[string][]string)}
	if config.Selector == CanarySelectorBybitOption {
		request := bybit.OptionTopicRequest{Role: bybit.RoleBoundedOrderbook, Symbol: config.Instrument, Depth: bybit.OptionMinimumBookDepth}
		topic, err := request.Topic()
		if err != nil {
			return nil, err
		}
		socket, err := bybit.DialOptionPublicSocket(ctx, config.HTTPClient, config.MaxMessageBytes)
		if err != nil {
			return nil, err
		}
		connection.subscriptions = []string{topic}
		connection.option = true
		connection.subscribe = func(ctx context.Context) ([]string, error) {
			return socket.Subscribe(ctx, []bybit.OptionTopicRequest{request})
		}
		connection.ping, connection.read, connection.close = socket.Ping, socket.Read, socket.Close
		connection.reader = newCanaryAsyncReader(connection.read)
		return connection, nil
	}
	category := bybitCategory(config.Selector)
	request := bybit.TopicRequest{Role: bybit.RoleTrade, Symbol: config.Instrument}
	topic, err := request.Topic(category)
	if err != nil {
		return nil, err
	}
	socket, err := bybit.DialPublicSocket(ctx, category, config.HTTPClient, config.MaxMessageBytes)
	if err != nil {
		return nil, err
	}
	connection.subscriptions = []string{topic}
	connection.subscribe = func(ctx context.Context) ([]string, error) {
		return socket.Subscribe(ctx, []bybit.TopicRequest{request})
	}
	connection.ping, connection.read, connection.close = socket.Ping, socket.Read, socket.Close
	connection.reader = newCanaryAsyncReader(connection.read)
	return connection, nil
}

func bybitCategory(selector string) bybit.Category {
	switch selector {
	case CanarySelectorBybitSpot:
		return bybit.Spot
	case CanarySelectorBybitLinear:
		return bybit.Linear
	default:
		return bybit.Inverse
	}
}

func (c *bybitCanaryConnection) Subscriptions() []string { return slices.Clone(c.subscriptions) }
func (c *bybitCanaryConnection) Subscribe(ctx context.Context) error {
	ids, err := c.subscribe(ctx)
	if err != nil {
		return err
	}
	if len(ids) != 1 {
		return errors.New("bybit: canary expected one subscription request")
	}
	c.requestIDs[ids[0]] = slices.Clone(c.subscriptions)
	return nil
}
func (c *bybitCanaryConnection) Read(ctx context.Context, deadline uint64) (CanaryEvent, error) {
	payload, err := c.reader.Read(ctx, c.clock, deadline)
	if err != nil {
		return CanaryEvent{}, err
	}
	if c.option {
		ack, ackErr := bybit.ParseOptionSubscriptionACK(payload)
		if ackErr == nil {
			return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: payload, ACKIdentities: slices.Clone(ack.Topics)}, nil
		}
		var envelope struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(payload, &envelope) == nil && envelope.Type == "COMMAND_RESP" {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: %v", errCanaryACKMismatch, ackErr)
		}
	}
	if ack, ackErr := bybit.ParseSubscriptionACK(payload); ackErr == nil {
		identities, ok := c.requestIDs[ack.RequestID]
		if !ok {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: Bybit request %q", errCanaryACKMismatch, ack.RequestID)
		}
		return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: payload, ACKIdentities: slices.Clone(identities)}, nil
	}
	var control struct {
		Success   bool   `json:"success"`
		ReturnMsg string `json:"ret_msg"`
		Operation string `json:"op"`
		RequestID string `json:"req_id"`
	}
	if json.Unmarshal(payload, &control) == nil && control.Operation == "subscribe" {
		return CanaryEvent{Payload: payload}, fmt.Errorf("%w: Bybit rejected subscription request %q", errCanaryACKMismatch, control.RequestID)
	}
	if control.Operation == "ping" && control.ReturnMsg == "pong" {
		if !control.Success || control.RequestID != c.pendingPingID {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: Bybit pong request %q", errCanaryHeartbeatMismatch, control.RequestID)
		}
		c.pendingPingID = ""
		return CanaryEvent{Kind: CanaryEventHeartbeatACK, Payload: payload}, nil
	}
	if c.option && control.Operation == "pong" {
		var pong struct {
			Arguments []string `json:"args"`
		}
		if json.Unmarshal(payload, &pong) != nil || len(pong.Arguments) != 1 || c.pendingPingID == "" {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: malformed Bybit option pong", errCanaryHeartbeatMismatch)
		}
		serverTimeMS, parseErr := strconv.ParseInt(pong.Arguments[0], 10, 64)
		if parseErr != nil || serverTimeMS <= 0 {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: malformed Bybit option pong timestamp", errCanaryHeartbeatMismatch)
		}
		c.pendingPingID = ""
		return CanaryEvent{Kind: CanaryEventHeartbeatACK, Payload: payload}, nil
	}
	var message struct {
		Topic string `json:"topic"`
	}
	if json.Unmarshal(payload, &message) != nil || message.Topic == "" {
		return CanaryEvent{}, errors.New("bybit: unrecognized canary payload")
	}
	return CanaryEvent{Kind: CanaryEventMessage, Payload: payload, StreamIdentity: message.Topic}, nil
}
func (c *bybitCanaryConnection) Heartbeat(ctx context.Context, id uint64) error {
	c.pendingPingID = fmt.Sprintf("canary-ping-%d", id)
	return c.ping(ctx, c.pendingPingID)
}
func (*bybitCanaryConnection) HeartbeatMode() CanaryHeartbeatMode { return CanaryHeartbeatActive }
func (*bybitCanaryConnection) RotationIntervalNS() uint64         { return 0 }
func (c *bybitCanaryConnection) Close() error {
	c.reader.Close()
	return c.close()
}

// OKX

type okxCanaryConnection struct {
	clock        CanaryClock
	socket       *okx.Socket
	reader       *canaryAsyncReader[[]byte]
	session      *okx.SubscriptionSession
	subscription okx.SubscriptionArg
}

func dialOKXCanary(ctx context.Context, config CanaryConfig) (CanaryConnection, error) {
	if config.RateBudgets.OKXHandshake == nil {
		return nil, fmt.Errorf("%w: caller-owned OKX handshake budget is required", ErrCanaryConfiguration)
	}
	arg := okx.SubscriptionArg{Channel: "trades", InstrumentID: config.Instrument}
	session, err := okx.NewSubscriptionSession(okx.PublicSocket, okx.Entitlement{}, []okx.SubscriptionArg{arg})
	if err != nil {
		return nil, err
	}
	socket, err := okx.DialSocket(ctx, okx.SocketConfig{Kind: okx.PublicSocket, Maximum: config.MaxMessageBytes}, config.HTTPClient, config.Clock, config.RateBudgets.OKXHandshake)
	if err != nil {
		return nil, err
	}
	connection := &okxCanaryConnection{clock: config.Clock, socket: socket, session: session, subscription: arg}
	connection.reader = newCanaryAsyncReader(socket.Read)
	return connection, nil
}

func (c *okxCanaryConnection) Subscriptions() []string { return []string{okxIdentity(c.subscription)} }
func (c *okxCanaryConnection) Subscribe(ctx context.Context) error {
	messages, err := c.session.Messages()
	if err != nil {
		return err
	}
	if len(messages) != 1 {
		return errors.New("okx: canary expected one subscription request")
	}
	return c.socket.WriteSubscription(ctx, c.session, messages[0])
}
func (c *okxCanaryConnection) Read(ctx context.Context, deadline uint64) (CanaryEvent, error) {
	payload, err := c.reader.Read(ctx, c.clock, deadline)
	if err != nil {
		return CanaryEvent{}, err
	}
	if string(payload) == "pong" {
		return CanaryEvent{Kind: CanaryEventHeartbeatACK, Payload: payload}, nil
	}
	var eventEnvelope struct {
		Event string `json:"event"`
	}
	_ = json.Unmarshal(payload, &eventEnvelope)
	if eventEnvelope.Event == "notice" {
		if _, noticeErr := okx.ParseServiceNotice(payload); noticeErr != nil {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: %v", errCanaryInvalidControl, noticeErr)
		}
		return CanaryEvent{Kind: CanaryEventDisconnect, Payload: payload, Planned: true}, nil
	}
	if eventEnvelope.Event != "" {
		ack, acknowledgeErr := c.session.Acknowledge(payload)
		if acknowledgeErr != nil {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: %v", errCanaryACKMismatch, acknowledgeErr)
		}
		return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: payload, ACKIdentities: []string{okxIdentity(ack.Argument)}}, nil
	}
	var message struct {
		Argument okx.SubscriptionArg `json:"arg"`
	}
	if json.Unmarshal(payload, &message) != nil || message.Argument.Channel == "" {
		return CanaryEvent{Payload: payload}, fmt.Errorf("%w: OKX payload has no bound argument", errCanaryUnknownStream)
	}
	return CanaryEvent{Kind: CanaryEventMessage, Payload: payload, StreamIdentity: okxIdentity(message.Argument)}, nil
}
func (c *okxCanaryConnection) Heartbeat(ctx context.Context, _ uint64) error {
	return c.socket.Ping(ctx)
}
func (*okxCanaryConnection) HeartbeatMode() CanaryHeartbeatMode { return CanaryHeartbeatActive }
func (*okxCanaryConnection) RotationIntervalNS() uint64         { return 0 }
func (c *okxCanaryConnection) Close() error {
	c.reader.Close()
	return c.socket.Close()
}

func okxIdentity(arg okx.SubscriptionArg) string {
	return arg.Channel + "\x00" + arg.InstrumentType + "\x00" + arg.InstrumentFamily + "\x00" + arg.InstrumentID
}

// Deribit public 100ms

const deribitCanaryTestResponseID uint64 = 1000

type deribitCanaryConnection struct {
	clock               CanaryClock
	socket              *deribit.Socket
	reader              *canaryAsyncReader[[]byte]
	session             *deribit.Session
	channels            []string
	subscribeID         uint64
	heartbeatSetupID    uint64
	heartbeatIntervalNS uint64
	pendingTest         []byte
}

func dialDeribitCanary(ctx context.Context, config CanaryConfig) (CanaryConnection, error) {
	policy := deribit.CadencePolicy{Requested: deribit.Cadence100MS}
	session, err := deribit.NewSession(policy, []deribit.ChannelRequest{{Role: deribit.RoleTrade, Instrument: config.Instrument}})
	if err != nil {
		return nil, err
	}
	socket, err := deribit.Dial(ctx, deribit.Production, config.HTTPClient, config.MaxMessageBytes)
	if err != nil {
		return nil, err
	}
	connection := &deribitCanaryConnection{
		clock: config.Clock, socket: socket, session: session, channels: session.Channels(),
		subscribeID: 2, heartbeatSetupID: 1, heartbeatIntervalNS: config.Heartbeat.IntervalNS,
	}
	connection.reader = newCanaryAsyncReader(socket.Read)
	return connection, nil
}

func (c *deribitCanaryConnection) Subscriptions() []string { return slices.Clone(c.channels) }
func (c *deribitCanaryConnection) Subscribe(ctx context.Context) error {
	seconds := (c.heartbeatIntervalNS + uint64(time.Second) - 1) / uint64(time.Second)
	heartbeat, err := c.session.SetHeartbeatRequest(c.heartbeatSetupID, seconds)
	if err != nil {
		return err
	}
	if err := c.socket.Write(ctx, heartbeat); err != nil {
		return err
	}
	subscribe, err := c.session.SubscribeRequest(c.subscribeID)
	if err != nil {
		return err
	}
	return c.socket.Write(ctx, subscribe)
}
func (c *deribitCanaryConnection) Read(ctx context.Context, deadline uint64) (CanaryEvent, error) {
	payload, err := c.reader.Read(ctx, c.clock, deadline)
	if err != nil {
		return CanaryEvent{}, err
	}
	decision, inspectErr := c.session.Inspect(payload, deribitCanaryTestResponseID)
	if inspectErr != nil {
		if errors.Is(inspectErr, deribit.ErrSubscribeMismatch) {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: %v", errCanaryACKMismatch, inspectErr)
		}
		return CanaryEvent{}, inspectErr
	}
	if decision.Reconciliation != nil {
		return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: payload, ACKIdentities: slices.Clone(decision.Reconciliation.Returned)}, nil
	}
	if decision.Action == deribit.SessionRespondTest {
		c.pendingTest = slices.Clone(decision.Response)
		return CanaryEvent{Kind: CanaryEventHeartbeatChallenge, Payload: payload}, nil
	}
	return classifyDeribitCanaryEnvelope(payload, c.heartbeatSetupID)
}

func classifyDeribitCanaryEnvelope(payload []byte, heartbeatSetupID uint64) (CanaryEvent, error) {
	var envelope struct {
		ID     uint64 `json:"id"`
		Method string `json:"method"`
		Params struct {
			Channel string `json:"channel"`
		} `json:"params"`
	}
	if json.Unmarshal(payload, &envelope) != nil {
		return CanaryEvent{}, errors.New("deribit: unrecognized canary payload")
	}
	if envelope.ID == heartbeatSetupID || envelope.ID == deribitCanaryTestResponseID {
		return CanaryEvent{Kind: CanaryEventControl, Payload: payload}, nil
	}
	if envelope.Method == "subscription" && envelope.Params.Channel != "" {
		return CanaryEvent{Kind: CanaryEventMessage, Payload: payload, StreamIdentity: envelope.Params.Channel}, nil
	}
	if envelope.Method == "heartbeat" {
		return CanaryEvent{Kind: CanaryEventControl, Payload: payload}, nil
	}
	return CanaryEvent{Payload: payload}, fmt.Errorf("%w: Deribit payload has no bound channel", errCanaryUnknownStream)
}
func (c *deribitCanaryConnection) Heartbeat(ctx context.Context, _ uint64) error {
	if c.pendingTest == nil {
		return errors.New("deribit: no pending test request")
	}
	err := c.socket.Write(ctx, c.pendingTest)
	c.pendingTest = nil
	return err
}
func (*deribitCanaryConnection) HeartbeatMode() CanaryHeartbeatMode { return CanaryHeartbeatChallenge }
func (*deribitCanaryConnection) RotationIntervalNS() uint64         { return 0 }
func (c *deribitCanaryConnection) Close() error {
	c.reader.Close()
	return c.socket.Close()
}

// Hyperliquid

type hyperliquidCanaryConnection struct {
	clock        CanaryClock
	socket       *hyperliquid.PublicSocket
	reader       *canaryAsyncReader[hyperliquid.ReceiveEnvelope]
	family       hyperliquid.Family
	dex          string
	subscription hyperliquid.Subscription
}

func dialHyperliquidCanary(ctx context.Context, config CanaryConfig) (CanaryConnection, error) {
	if config.RateBudgets.HyperliquidMessages == nil || config.RateBudgets.HyperliquidConnections == nil {
		return nil, fmt.Errorf("%w: caller-owned Hyperliquid message and connection budgets are required", ErrCanaryConfiguration)
	}
	family := hyperliquidFamily(config.Selector)
	subscription := hyperliquid.Subscription{Type: hyperliquid.SubscriptionTrades, Coin: config.Instrument, DEX: config.DEX}
	socket, err := hyperliquid.DialPublicSocket(ctx, hyperliquid.Mainnet, family, config.DEX, config.HTTPClient, config.MaxMessageBytes, config.RateBudgets.HyperliquidMessages, config.RateBudgets.HyperliquidConnections)
	if err != nil {
		return nil, err
	}
	connection := &hyperliquidCanaryConnection{clock: config.Clock, socket: socket, family: family, dex: config.DEX, subscription: subscription}
	connection.reader = newCanaryAsyncReader(socket.Read)
	return connection, nil
}

func hyperliquidFamily(selector string) hyperliquid.Family {
	switch selector {
	case CanarySelectorHyperliquidMain:
		return hyperliquid.MainPerpetual
	case CanarySelectorHyperliquidSpot:
		return hyperliquid.Spot
	default:
		return hyperliquid.HIP3
	}
}

func (c *hyperliquidCanaryConnection) Subscriptions() []string {
	return []string{c.subscription.StreamIdentity()}
}
func (c *hyperliquidCanaryConnection) Subscribe(ctx context.Context) error {
	return c.socket.Subscribe(ctx, []hyperliquid.Subscription{c.subscription})
}
func (c *hyperliquidCanaryConnection) Read(ctx context.Context, deadline uint64) (CanaryEvent, error) {
	envelope, err := c.reader.Read(ctx, c.clock, deadline)
	if err != nil {
		if errors.Is(err, hyperliquid.ErrBookStreamMismatch) || errors.Is(err, hyperliquid.ErrInvalidPayload) {
			return CanaryEvent{}, fmt.Errorf("%w: %v", errCanaryUnknownStream, err)
		}
		return CanaryEvent{}, err
	}
	payload := envelope.Bytes()
	switch envelope.Channel() {
	case "subscriptionResponse":
		ack, err := hyperliquid.ParseSubscriptionACK(c.family, c.dex, payload)
		if err != nil {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: %v", errCanaryACKMismatch, err)
		}
		if err := c.socket.HandleSubscriptionACK(ack); err != nil {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: %v", errCanaryACKMismatch, err)
		}
		return CanaryEvent{Kind: CanaryEventSubscriptionACK, Payload: payload, ACKIdentities: []string{ack.Subscription.StreamIdentity()}}, nil
	case "pong":
		if _, err := hyperliquid.ParsePong(payload); err != nil {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: %v", errCanaryHeartbeatMismatch, err)
		}
		return CanaryEvent{Kind: CanaryEventHeartbeatACK, Payload: payload}, nil
	default:
		subscription, ok := envelope.Subscription()
		if !ok {
			return CanaryEvent{Payload: payload}, fmt.Errorf("%w: Hyperliquid payload is not bound to an active subscription", errCanaryUnknownStream)
		}
		return CanaryEvent{Kind: CanaryEventMessage, Payload: payload, StreamIdentity: subscription.StreamIdentity()}, nil
	}
}
func (c *hyperliquidCanaryConnection) Heartbeat(ctx context.Context, _ uint64) error {
	return c.socket.Ping(ctx)
}
func (*hyperliquidCanaryConnection) HeartbeatMode() CanaryHeartbeatMode { return CanaryHeartbeatActive }
func (*hyperliquidCanaryConnection) RotationIntervalNS() uint64         { return 0 }
func (c *hyperliquidCanaryConnection) Close() error {
	c.reader.Close()
	return c.socket.Close()
}
