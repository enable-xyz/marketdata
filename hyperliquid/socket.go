package hyperliquid

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
)

type publicSocketTransport interface {
	Write(context.Context, websocket.MessageType, []byte) error
	Read(context.Context) (websocket.MessageType, []byte, error)
	Close(websocket.StatusCode, string) error
}

type pendingSubscription struct {
	method       string
	subscription Subscription
	sentAtNS     uint64
	deadlineNS   uint64
}

type wsBookBindingKey struct {
	family  Family
	dexName string
	coin    string
}

type SubscriptionTimeoutEvidence struct {
	Method       string
	Subscription Subscription
	SentAtNS     uint64
	DeadlineNS   uint64
	ExpiredAtNS  uint64
}

// PublicSocket owns one immutable network/family/DEX connection and uses
// caller-owned aggregate venue limiters shared by every connection.
type PublicSocket struct {
	mu           sync.Mutex
	readMu       sync.Mutex
	network      Network
	family       Family
	dexName      string
	endpoint     string
	maximum      uint32
	conn         publicSocketTransport
	clock        MonotonicClock
	limiter      *WeightedLimiter
	desired      map[string]Subscription
	pending      map[string]pendingSubscription
	active       map[string]Subscription
	timedOut     map[string]SubscriptionTimeoutEvidence
	bookBindings map[wsBookBindingKey]Subscription
}

func DialPublicSocket(ctx context.Context, network Network, family Family, dexName string, client *http.Client, maximum uint32, messageLimiter, connectionLimiter *WeightedLimiter) (*PublicSocket, error) {
	if err := network.Validate(); err != nil {
		return nil, err
	}
	if err := validateFamily(family); err != nil {
		return nil, err
	}
	if err := validateDEXName(family, dexName); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client == nil || client.Transport == nil || client.Timeout <= 0 || maximum == 0 || maximum > MaxRawPayloadBytes {
		return nil, fmt.Errorf("%w: explicit bounded client and payload limit are required", ErrInvalidSubscription)
	}
	if !messageLimiter.matches(MaxOutboundMessagesPerMinute, time.Minute) || !connectionLimiter.matches(MaxConnectionAttemptsPerMinute, time.Minute) {
		return nil, fmt.Errorf("%w: shared 2000/min message and 30/min connection limiters are required", ErrRateBudget)
	}
	endpoint := network.WebSocketEndpoint()
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "wss" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/ws" || !allowlistedAPIHost(network, parsed.Host) {
		return nil, fmt.Errorf("%w: endpoint is not allowlisted", ErrInvalidSubscription)
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("hyperliquid: WebSocket redirects are disabled")
	}
	if err := connectionLimiter.Reserve(1); err != nil {
		return nil, err
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: &copyClient})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: dial public socket: %w", err)
	}
	conn.SetReadLimit(int64(maximum))
	return newPublicSocketWithTransport(network, family, dexName, endpoint, maximum, conn, messageLimiter, messageLimiter.clock)
}

func newPublicSocketWithTransport(network Network, family Family, dexName, endpoint string, maximum uint32, conn publicSocketTransport, limiter *WeightedLimiter, clock MonotonicClock) (*PublicSocket, error) {
	if network.Validate() != nil || validateFamily(family) != nil || validateDEXName(family, dexName) != nil || conn == nil || limiter == nil || clock == nil || maximum == 0 || maximum > MaxRawPayloadBytes {
		return nil, ErrInvalidSubscription
	}
	return &PublicSocket{
		network: network, family: family, dexName: dexName, endpoint: endpoint, maximum: maximum, conn: conn, clock: clock, limiter: limiter,
		desired: make(map[string]Subscription), pending: make(map[string]pendingSubscription), active: make(map[string]Subscription),
		timedOut: make(map[string]SubscriptionTimeoutEvidence), bookBindings: make(map[wsBookBindingKey]Subscription),
	}, nil
}

func (s *PublicSocket) Network() Network { return s.network }
func (s *PublicSocket) Family() Family   { return s.family }
func (s *PublicSocket) DEXName() string  { return s.dexName }
func (s *PublicSocket) Endpoint() string { return s.endpoint }

func (s *PublicSocket) Subscribe(ctx context.Context, subscriptions []Subscription) error {
	if s == nil || len(subscriptions) == 0 {
		return ErrInvalidSubscription
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirePendingLocked(s.clock.NowMonotonicNS())
	target := cloneSubscriptions(s.desired)
	for _, subscription := range subscriptions {
		if subscription.Validate(s.family, s.dexName) != nil {
			return ErrInvalidSubscription
		}
		key := subscription.StreamIdentity()
		if _, duplicate := target[key]; duplicate {
			return fmt.Errorf("%w: duplicate desired stream", ErrInvalidSubscription)
		}
		target[key] = subscription
	}
	return s.reconcileLocked(ctx, target)
}

func (s *PublicSocket) Unsubscribe(ctx context.Context, subscriptions []Subscription) error {
	if s == nil || len(subscriptions) == 0 {
		return ErrInvalidSubscription
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirePendingLocked(s.clock.NowMonotonicNS())
	target := cloneSubscriptions(s.desired)
	for _, subscription := range subscriptions {
		key := subscription.StreamIdentity()
		if current, ok := target[key]; !ok || current != subscription {
			return fmt.Errorf("%w: stream is not desired", ErrInvalidSubscription)
		}
		delete(target, key)
	}
	return s.reconcileLocked(ctx, target)
}

func (s *PublicSocket) ReconcileSubscriptions(ctx context.Context, subscriptions []Subscription) error {
	if s == nil {
		return ErrInvalidSubscription
	}
	target, err := subscriptionMap(s.family, s.dexName, subscriptions)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirePendingLocked(s.clock.NowMonotonicNS())
	return s.reconcileLocked(ctx, target)
}

func (s *PublicSocket) reconcileLocked(ctx context.Context, target map[string]Subscription) error {
	if s.conn == nil || s.limiter == nil || s.clock == nil {
		return ErrInvalidSubscription
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(target) > MaxSubscriptions || ambiguousBookSubscriptions(target) {
		return ErrInvalidSubscription
	}
	for _, subscription := range target {
		if subscription.Type != SubscriptionL2Book {
			continue
		}
		if bound, ok := s.bookBindings[s.bookBindingKey(subscription.Coin)]; ok && bound != subscription {
			return fmt.Errorf("%w: book depth is immutable for this socket", ErrBookStreamMismatch)
		}
	}
	operations := make([]pendingSubscription, 0)
	for key, pending := range s.pending {
		_, wanted := target[key]
		if (pending.method == "subscribe") != wanted {
			return fmt.Errorf("%w: desired state conflicts with pending ACK", ErrInvalidSubscription)
		}
	}
	for key, active := range s.active {
		if _, wanted := target[key]; !wanted {
			if _, waiting := s.pending[key]; !waiting {
				if _, awaitingObservation := s.timedOut[key]; !awaitingObservation {
					operations = append(operations, pendingSubscription{method: "unsubscribe", subscription: active})
				}
			}
		}
	}
	for key, wanted := range target {
		if _, isActive := s.active[key]; !isActive {
			if _, waiting := s.pending[key]; !waiting {
				if _, awaitingObservation := s.timedOut[key]; !awaitingObservation {
					operations = append(operations, pendingSubscription{method: "subscribe", subscription: wanted})
				}
			}
		}
	}
	slices.SortFunc(operations, func(a, b pendingSubscription) int {
		return strings.Compare(a.method+"\x00"+a.subscription.StreamIdentity(), b.method+"\x00"+b.subscription.StreamIdentity())
	})
	if len(s.pending)+len(operations) > MaxPendingACK {
		return ErrRateBudget
	}
	if len(operations) > 0 {
		if err := s.limiter.Reserve(uint32(len(operations))); err != nil {
			return err
		}
	}
	s.desired = cloneSubscriptions(target)
	for _, operation := range operations {
		message, err := encodeSubscriptionOperation(operation.method, operation.subscription)
		if err != nil {
			return err
		}
		if err := s.conn.Write(ctx, websocket.MessageText, message); err != nil {
			return fmt.Errorf("hyperliquid: write %s: %w", operation.method, err)
		}
		now := s.clock.NowMonotonicNS()
		deadline := now + SubscriptionACKTimeoutNS
		if deadline < now {
			deadline = ^uint64(0)
		}
		operation.sentAtNS = now
		operation.deadlineNS = deadline
		s.pending[operation.subscription.StreamIdentity()] = operation
	}
	return nil
}

func (s *PublicSocket) expirePendingLocked(now uint64) {
	for key, pending := range s.pending {
		if now < pending.deadlineNS {
			continue
		}
		delete(s.pending, key)
		s.timedOut[key] = SubscriptionTimeoutEvidence{Method: pending.method, Subscription: pending.subscription, SentAtNS: pending.sentAtNS, DeadlineNS: pending.deadlineNS, ExpiredAtNS: now}
	}
}

func (s *PublicSocket) DrainSubscriptionTimeouts() []SubscriptionTimeoutEvidence {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirePendingLocked(s.clock.NowMonotonicNS())
	evidence := make([]SubscriptionTimeoutEvidence, 0, len(s.timedOut))
	for _, observation := range s.timedOut {
		evidence = append(evidence, observation)
	}
	slices.SortFunc(evidence, func(a, b SubscriptionTimeoutEvidence) int {
		return strings.Compare(a.Subscription.StreamIdentity(), b.Subscription.StreamIdentity())
	})
	s.timedOut = make(map[string]SubscriptionTimeoutEvidence)
	return evidence
}

func (s *PublicSocket) HandleSubscriptionACK(ack SubscriptionACK) error {
	if s == nil || ack.ValidateEvidence(s.family, s.dexName) != nil {
		return ErrInvalidPayload
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirePendingLocked(s.clock.NowMonotonicNS())
	key := ack.Subscription.StreamIdentity()
	pending, ok := s.pending[key]
	if !ok || pending.method != ack.Method || pending.subscription != ack.Subscription {
		return ErrInvalidPayload
	}
	if ack.Method == "subscribe" && len(s.active) >= MaxSubscriptions {
		return ErrInvalidSubscription
	}
	if ack.Method == "subscribe" && ack.Subscription.Type == SubscriptionL2Book {
		if bound, ok := s.bookBindings[s.bookBindingKey(ack.Subscription.Coin)]; ok && bound != ack.Subscription {
			return ErrBookStreamMismatch
		}
	}
	delete(s.pending, key)
	if ack.Method == "subscribe" {
		s.active[key] = ack.Subscription
		if ack.Subscription.Type == SubscriptionL2Book {
			s.bookBindings[s.bookBindingKey(ack.Subscription.Coin)] = ack.Subscription
		}
	} else {
		delete(s.active, key)
	}
	return nil
}

func (s *PublicSocket) ResetForReconnect() []Subscription {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = make(map[string]pendingSubscription)
	s.active = make(map[string]Subscription)
	return sortedSubscriptions(s.desired)
}

func (s *PublicSocket) SubscriptionCounts() (desired, pending, active int) {
	if s == nil {
		return 0, 0, 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirePendingLocked(s.clock.NowMonotonicNS())
	return len(s.desired), len(s.pending), len(s.active)
}

func (s *PublicSocket) Ping(ctx context.Context) error {
	if s == nil {
		return ErrInvalidSubscription
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirePendingLocked(s.clock.NowMonotonicNS())
	if s.conn == nil || s.limiter == nil {
		return ErrInvalidSubscription
	}
	if err := s.limiter.Reserve(1); err != nil {
		return err
	}
	return s.conn.Write(ctx, websocket.MessageText, []byte(`{"method":"ping"}`))
}

func (s *PublicSocket) Read(ctx context.Context) (ReceiveEnvelope, error) {
	if s == nil {
		return ReceiveEnvelope{}, ErrInvalidSubscription
	}
	s.readMu.Lock()
	defer s.readMu.Unlock()

	s.mu.Lock()
	s.expirePendingLocked(s.clock.NowMonotonicNS())
	conn := s.conn
	maximum := s.maximum
	s.mu.Unlock()
	if conn == nil {
		return ReceiveEnvelope{}, ErrInvalidSubscription
	}
	messageType, payload, err := conn.Read(ctx)
	if err != nil {
		return ReceiveEnvelope{}, err
	}
	receivedAtNS := s.clock.NowMonotonicNS()
	if messageType != websocket.MessageText || len(payload) == 0 || len(payload) > int(maximum) {
		return ReceiveEnvelope{}, ErrInvalidPayload
	}
	_, coin, subscriptionType, required, err := receivePayloadIdentity(payload)
	if err != nil {
		return ReceiveEnvelope{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.expirePendingLocked(s.clock.NowMonotonicNS())
	if s.conn != conn {
		return ReceiveEnvelope{}, ErrInvalidSubscription
	}
	var subscription Subscription
	if required {
		found := false
		for _, active := range s.active {
			if active.Type == subscriptionType && active.Coin == coin {
				if found {
					return ReceiveEnvelope{}, ErrBookStreamMismatch
				}
				subscription = active
				found = true
			}
		}
		if !found {
			return ReceiveEnvelope{}, ErrBookStreamMismatch
		}
		if subscriptionType == SubscriptionL2Book {
			bound, ok := s.bookBindings[s.bookBindingKey(coin)]
			if !ok || bound != subscription {
				return ReceiveEnvelope{}, ErrBookStreamMismatch
			}
		}
		return newReceiveEnvelope(payload, receivedAtNS, s.family, s.dexName, subscription, true)
	}
	return newReceiveEnvelope(payload, receivedAtNS, s.family, s.dexName, Subscription{}, false)
}

func (s *PublicSocket) bookBindingKey(coin string) wsBookBindingKey {
	return wsBookBindingKey{family: s.family, dexName: s.dexName, coin: coin}
}

func (s *PublicSocket) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		return nil
	}
	err := s.conn.Close(websocket.StatusNormalClosure, "public market-data capture closed")
	s.conn = nil
	return err
}

func subscriptionMap(family Family, dexName string, subscriptions []Subscription) (map[string]Subscription, error) {
	if len(subscriptions) > MaxSubscriptions {
		return nil, ErrInvalidSubscription
	}
	result := make(map[string]Subscription, len(subscriptions))
	for _, subscription := range subscriptions {
		if subscription.Validate(family, dexName) != nil {
			return nil, ErrInvalidSubscription
		}
		key := subscription.StreamIdentity()
		if _, duplicate := result[key]; duplicate {
			return nil, ErrInvalidSubscription
		}
		result[key] = subscription
	}
	if ambiguousBookSubscriptions(result) {
		return nil, ErrInvalidSubscription
	}
	return result, nil
}

func ambiguousBookSubscriptions(subscriptions map[string]Subscription) bool {
	coins := make(map[string]struct{})
	for _, subscription := range subscriptions {
		if subscription.Type != SubscriptionL2Book {
			continue
		}
		if _, duplicate := coins[subscription.Coin]; duplicate {
			return true
		}
		coins[subscription.Coin] = struct{}{}
	}
	return false
}

func cloneSubscriptions(source map[string]Subscription) map[string]Subscription {
	clone := make(map[string]Subscription, len(source))
	for key, subscription := range source {
		clone[key] = subscription
	}
	return clone
}

func sortedSubscriptions(source map[string]Subscription) []Subscription {
	ordered := make([]Subscription, 0, len(source))
	for _, subscription := range source {
		ordered = append(ordered, subscription)
	}
	slices.SortFunc(ordered, func(a, b Subscription) int { return strings.Compare(a.StreamIdentity(), b.StreamIdentity()) })
	return ordered
}

func allowlistedAPIHost(network Network, host string) bool {
	switch network {
	case Mainnet:
		return host == "api.hyperliquid.xyz"
	case Testnet:
		return host == "api.hyperliquid-testnet.xyz"
	default:
		return false
	}
}
