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

type OptionTopicRequest struct {
	Role     SourceRole
	BaseCoin string
	Symbol   string
	Depth    int
}

func (r OptionTopicRequest) Topic() (string, error) {
	support, ok := OptionSupports(r.Role)
	if !ok || support.Support != 1 {
		return "", fmt.Errorf("%w: option/%s", ErrUnsupportedRole, r.Role)
	}
	switch r.Role {
	case RoleTrade:
		if !validBaseCoin(r.BaseCoin) || r.Symbol != "" || r.Depth != 0 {
			return "", fmt.Errorf("%w: option trades require exactly one base-coin identity", ErrInvalidTopic)
		}
		return "publicTrade." + r.BaseCoin, nil
	case RoleOptionTicker:
		if !validBaseCoin(r.BaseCoin) || r.Symbol != "" || r.Depth != 0 {
			return "", fmt.Errorf("%w: option tickers require exactly one base-coin identity", ErrInvalidTopic)
		}
		return "tickers." + r.BaseCoin, nil
	case RoleBoundedOrderbook:
		if r.BaseCoin != "" || !validOptionSymbol(r.Symbol) || !validOptionBookDepth(r.Depth) {
			return "", fmt.Errorf("%w: option books require an instrument and depth 25 or 100", ErrInvalidTopic)
		}
		return fmt.Sprintf("orderbook.%d.%s", r.Depth, r.Symbol), nil
	default:
		return "", fmt.Errorf("%w: option/%s", ErrUnsupportedRole, r.Role)
	}
}

func OptionSubscriptionMessages(requests []OptionTopicRequest) ([][]byte, error) {
	if len(requests) == 0 || len(requests) > MaxSubscriptions {
		return nil, fmt.Errorf("%w: subscription count outside 1..%d", ErrInvalidTopic, MaxSubscriptions)
	}
	topics := make([]string, 0, len(requests))
	seen := make(map[string]struct{}, len(requests))
	for _, request := range requests {
		topic, err := request.Topic()
		if err != nil {
			return nil, err
		}
		if _, exists := seen[topic]; exists {
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
			RequestID: fmt.Sprintf("bybit-option-subscribe-%03d", batch),
			Operation: "subscribe",
			Arguments: slices.Clone(topics[first:last]),
		})
		if err != nil {
			return nil, fmt.Errorf("%w: encode option subscription: %v", ErrInvalidTopic, err)
		}
		messages = append(messages, payload)
	}
	return messages, nil
}

type OptionSubscriptionACK struct {
	ConnectionID string
	Topics       []string
}

func ParseOptionSubscriptionACK(payload []byte) (OptionSubscriptionACK, error) {
	var wire struct {
		Success      *bool  `json:"success"`
		ConnectionID string `json:"conn_id"`
		Type         string `json:"type"`
		Data         struct {
			Failed    []string `json:"failTopics"`
			Succeeded []string `json:"successTopics"`
		} `json:"data"`
	}
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || json.Unmarshal(payload, &wire) != nil ||
		wire.Success == nil || !*wire.Success || wire.Type != "COMMAND_RESP" || wire.ConnectionID == "" ||
		len(wire.Data.Succeeded) == 0 || len(wire.Data.Succeeded) > MaxSubscriptions {
		return OptionSubscriptionACK{}, ErrInvalidPayload
	}
	seen := make(map[string]struct{}, len(wire.Data.Succeeded))
	for _, topic := range wire.Data.Succeeded {
		if topic == "" || len(topic) > 256 || strings.IndexByte(topic, 0) >= 0 {
			return OptionSubscriptionACK{}, ErrInvalidPayload
		}
		if _, duplicate := seen[topic]; duplicate {
			return OptionSubscriptionACK{}, ErrInvalidPayload
		}
		seen[topic] = struct{}{}
	}
	ack := OptionSubscriptionACK{ConnectionID: wire.ConnectionID, Topics: slices.Clone(wire.Data.Succeeded)}
	if len(wire.Data.Failed) != 0 {
		return ack, fmt.Errorf("%w: option subscription rejected topics %q", ErrInvalidPayload, wire.Data.Failed)
	}
	return ack, nil
}

func validBaseCoin(baseCoin string) bool {
	if baseCoin == "" || len(baseCoin) > 16 {
		return false
	}
	for _, r := range baseCoin {
		if (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}
	return true
}

func validOptionBookDepth(depth int) bool {
	return depth == OptionMinimumBookDepth || depth == OptionMaximumBookDepth
}

// OptionPublicSocket owns the distinct /v5/public/option connection. It cannot
// be reclassified as a Spot, Linear, or Inverse socket after dialing.
type OptionPublicSocket struct {
	endpoint string
	maximum  uint32
	conn     *websocket.Conn
}

func DialOptionPublicSocket(ctx context.Context, client *http.Client, maximum uint32) (*OptionPublicSocket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client == nil || client.Transport == nil || client.Timeout <= 0 {
		return nil, fmt.Errorf("%w: explicit bounded HTTP client is required", ErrInvalidTopic)
	}
	if maximum == 0 || maximum > MaxRawPayloadBytes {
		return nil, fmt.Errorf("%w: invalid raw payload bound", ErrInvalidTopic)
	}
	parsed, err := url.Parse(OptionPublicEndpoint)
	if err != nil || parsed.Scheme != "wss" || parsed.Host != "stream.bybit.com" || parsed.User != nil || parsed.RawQuery != "" || parsed.Path != "/v5/public/option" {
		return nil, fmt.Errorf("%w: endpoint is not allowlisted", ErrInvalidTopic)
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("bybit: WebSocket redirects are disabled")
	}
	conn, response, err := websocket.Dial(ctx, OptionPublicEndpoint, &websocket.DialOptions{HTTPClient: &copyClient})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("bybit: dial option public socket: %w", err)
	}
	conn.SetReadLimit(int64(maximum))
	return &OptionPublicSocket{endpoint: OptionPublicEndpoint, maximum: maximum, conn: conn}, nil
}

func (s *OptionPublicSocket) Endpoint() string { return s.endpoint }
func (s *OptionPublicSocket) SourceID() string { return OptionSourceID }

func (s *OptionPublicSocket) Subscribe(ctx context.Context, requests []OptionTopicRequest) ([]string, error) {
	if s == nil || s.conn == nil {
		return nil, ErrInvalidTopic
	}
	messages, err := OptionSubscriptionMessages(requests)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(messages))
	for _, message := range messages {
		var request SubscriptionRequest
		if err := json.Unmarshal(message, &request); err != nil {
			return nil, fmt.Errorf("bybit: decode internally generated option subscription: %w", err)
		}
		if err := s.conn.Write(ctx, websocket.MessageText, message); err != nil {
			return nil, fmt.Errorf("bybit: write option subscription: %w", err)
		}
		ids = append(ids, request.RequestID)
	}
	return ids, nil
}

func (s *OptionPublicSocket) Ping(ctx context.Context, requestID string) error {
	if s == nil || s.conn == nil || requestID == "" || len(requestID) > 64 || strings.IndexByte(requestID, 0) >= 0 {
		return ErrInvalidTopic
	}
	payload, err := json.Marshal(struct {
		RequestID string `json:"req_id"`
		Operation string `json:"op"`
	}{RequestID: requestID, Operation: "ping"})
	if err != nil {
		return fmt.Errorf("bybit: encode option ping: %w", err)
	}
	return s.conn.Write(ctx, websocket.MessageText, payload)
}

func (s *OptionPublicSocket) Read(ctx context.Context) ([]byte, error) {
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

func (s *OptionPublicSocket) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	err := s.conn.Close(websocket.StatusNormalClosure, "option public market-data capture closed")
	s.conn = nil
	return err
}
