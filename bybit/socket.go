package bybit

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/coder/websocket"
)

type TopicRequest struct {
	Role   SourceRole
	Symbol string
	Depth  int
}

func (r TopicRequest) Topic(category Category) (string, error) {
	if err := category.Validate(); err != nil {
		return "", err
	}
	if !validSymbol(r.Symbol) {
		return "", fmt.Errorf("%w: invalid symbol", ErrInvalidTopic)
	}
	support, ok := Supports(category, r.Role)
	if !ok || support.Support != 1 {
		return "", fmt.Errorf("%w: %s/%s", ErrUnsupportedRole, category, r.Role)
	}
	switch r.Role {
	case RoleTrade:
		if r.Depth != 0 {
			return "", ErrInvalidTopic
		}
		return "publicTrade." + r.Symbol, nil
	case RoleBoundedOrderbook:
		if !validBoundedDepth(r.Depth) {
			return "", ErrInvalidTopic
		}
		return fmt.Sprintf("orderbook.%d.%s", r.Depth, r.Symbol), nil
	case RoleBBO:
		if r.Depth != 0 && r.Depth != 1 {
			return "", ErrInvalidTopic
		}
		return "orderbook.1." + r.Symbol, nil
	case RoleFullOrderbook:
		if r.Depth != 0 {
			return "", ErrInvalidTopic
		}
		return "orderbook.full." + r.Symbol, nil
	case RoleRPIOrderbook:
		if r.Depth != 0 && r.Depth != 50 {
			return "", ErrInvalidTopic
		}
		return "orderbook.rpi." + r.Symbol, nil
	case RoleGenericTicker, RoleDerivativeTicker:
		if r.Depth != 0 {
			return "", ErrInvalidTopic
		}
		return "tickers." + r.Symbol, nil
	case RoleAllLiquidation:
		if r.Depth != 0 || category == Spot {
			return "", fmt.Errorf("%w: allLiquidation is derivatives-only", ErrUnsupportedRole)
		}
		return "allLiquidation." + r.Symbol, nil
	default:
		return "", fmt.Errorf("%w: %s", ErrUnsupportedRole, r.Role)
	}
}

func validBoundedDepth(depth int) bool {
	switch depth {
	case 1, 50, 200, 1000:
		return true
	default:
		return false
	}
}

func validSymbol(symbol string) bool {
	if symbol == "" || len(symbol) > 64 {
		return false
	}
	for _, r := range symbol {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' {
			return false
		}
	}
	return true
}

type SubscriptionRequest struct {
	RequestID string   `json:"req_id"`
	Operation string   `json:"op"`
	Arguments []string `json:"args"`
}

func SubscriptionMessages(category Category, requests []TopicRequest) ([][]byte, error) {
	if err := category.Validate(); err != nil {
		return nil, err
	}
	if len(requests) == 0 || len(requests) > MaxSubscriptions {
		return nil, fmt.Errorf("%w: subscription count outside 1..%d", ErrInvalidTopic, MaxSubscriptions)
	}
	topics := make([]string, 0, len(requests))
	seen := make(map[string]struct{}, len(requests))
	for _, request := range requests {
		topic, err := request.Topic(category)
		if err != nil {
			return nil, err
		}
		if _, ok := seen[topic]; ok {
			return nil, fmt.Errorf("%w: duplicate topic %s", ErrInvalidTopic, topic)
		}
		seen[topic] = struct{}{}
		topics = append(topics, topic)
	}
	slices.Sort(topics)
	messages := make([][]byte, 0, (len(topics)+MaxSubscriptionsPerACK-1)/MaxSubscriptionsPerACK)
	for first, batch := 0, 1; first < len(topics); first, batch = first+MaxSubscriptionsPerACK, batch+1 {
		last := min(first+MaxSubscriptionsPerACK, len(topics))
		payload, err := json.Marshal(SubscriptionRequest{
			RequestID: fmt.Sprintf("bybit-%s-subscribe-%03d", category, batch),
			Operation: "subscribe",
			Arguments: slices.Clone(topics[first:last]),
		})
		if err != nil {
			return nil, fmt.Errorf("%w: encode subscription: %v", ErrInvalidTopic, err)
		}
		messages = append(messages, payload)
	}
	return messages, nil
}

type SubscriptionACK struct {
	Success      bool   `json:"success"`
	ReturnMsg    string `json:"ret_msg"`
	RequestID    string `json:"req_id"`
	Operation    string `json:"op"`
	ConnectionID string `json:"conn_id"`
}

func ParseSubscriptionACK(payload []byte) (SubscriptionACK, error) {
	var ack SubscriptionACK
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || json.Unmarshal(payload, &ack) != nil {
		return SubscriptionACK{}, ErrInvalidPayload
	}
	if ack.Operation != "subscribe" || ack.RequestID == "" {
		return SubscriptionACK{}, ErrInvalidPayload
	}
	if !ack.Success {
		return ack, fmt.Errorf("%w: subscription rejected: %s", ErrInvalidPayload, ack.ReturnMsg)
	}
	return ack, nil
}

// PublicSocket owns one category-specific public V5 connection. A socket can
// never change category after dialing, so Spot, Linear, and Inverse source
// epochs cannot accidentally share a transport or sparse state cache.
type PublicSocket struct {
	category Category
	endpoint string
	maximum  uint32
	conn     *websocket.Conn
}

func DialPublicSocket(ctx context.Context, category Category, client *http.Client, maximum uint32) (*PublicSocket, error) {
	if err := category.Validate(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client == nil || client.Transport == nil || client.Timeout <= 0 {
		return nil, fmt.Errorf("%w: explicit bounded HTTP client is required", ErrInvalidTopic)
	}
	if maximum == 0 || maximum > MaxRawPayloadBytes {
		return nil, fmt.Errorf("%w: invalid raw payload bound", ErrInvalidTopic)
	}
	endpoint := category.PublicEndpoint()
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "wss" || parsed.Host != "stream.bybit.com" || parsed.User != nil || parsed.RawQuery != "" || parsed.Path != "/v5/public/"+string(category) {
		return nil, fmt.Errorf("%w: endpoint is not allowlisted", ErrInvalidTopic)
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("bybit: WebSocket redirects are disabled")
	}
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: &copyClient})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("bybit: dial %s public socket: %w", category, err)
	}
	conn.SetReadLimit(int64(maximum))
	return &PublicSocket{category: category, endpoint: endpoint, maximum: maximum, conn: conn}, nil
}

func (s *PublicSocket) Category() Category { return s.category }
func (s *PublicSocket) Endpoint() string   { return s.endpoint }

func (s *PublicSocket) Subscribe(ctx context.Context, requests []TopicRequest) ([]string, error) {
	if s == nil || s.conn == nil {
		return nil, ErrInvalidTopic
	}
	messages, err := SubscriptionMessages(s.category, requests)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		var request SubscriptionRequest
		if err := json.Unmarshal(message, &request); err != nil {
			return nil, fmt.Errorf("bybit: decode internally generated subscription: %w", err)
		}
		if err := s.conn.Write(ctx, websocket.MessageText, message); err != nil {
			return nil, fmt.Errorf("bybit: write subscription: %w", err)
		}
		ids = append(ids, request.RequestID)
	}
	return ids, nil
}

func (s *PublicSocket) Ping(ctx context.Context, requestID string) error {
	if s == nil || s.conn == nil || requestID == "" || len(requestID) > 64 || strings.IndexByte(requestID, 0) >= 0 {
		return ErrInvalidTopic
	}
	payload, err := json.Marshal(struct {
		RequestID string `json:"req_id"`
		Operation string `json:"op"`
	}{RequestID: requestID, Operation: "ping"})
	if err != nil {
		return fmt.Errorf("bybit: encode ping: %w", err)
	}
	return s.conn.Write(ctx, websocket.MessageText, payload)
}

func (s *PublicSocket) Read(ctx context.Context) ([]byte, error) {
	if s == nil || s.conn == nil {
		return nil, ErrInvalidTopic
	}
	messageType, payload, err := s.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText || len(payload) == 0 || len(payload) > int(s.maximum) {
		return nil, ErrInvalidPayload
	}
	return slices.Clone(payload), nil
}

func (s *PublicSocket) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	err := s.conn.Close(websocket.StatusNormalClosure, "public market-data capture closed")
	s.conn = nil
	return err
}
