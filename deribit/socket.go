package deribit

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"slices"

	"github.com/coder/websocket"
)

type Environment string

const (
	Production Environment = "production"
	Testnet    Environment = "testnet"
)

func (e Environment) Endpoint() (string, error) {
	switch e {
	case Production:
		return ProductionEndpoint, nil
	case Testnet:
		return TestnetEndpoint, nil
	default:
		return "", fmt.Errorf("%w: unknown environment", ErrInvalidContract)
	}
}

// Socket owns one access-dated JSON-RPC v2 WebSocket. Destinations are selected
// only from Environment; payloads, credentials, and redirects cannot alter it.
type Socket struct {
	endpoint string
	maximum  uint32
	conn     *websocket.Conn
}

func Dial(ctx context.Context, environment Environment, client *http.Client, maximum uint32) (*Socket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if client == nil || client.Transport == nil || client.Timeout <= 0 {
		return nil, fmt.Errorf("%w: explicit bounded HTTP client is required", ErrInvalidContract)
	}
	if maximum == 0 || maximum > MaxRawPayloadBytes {
		return nil, fmt.Errorf("%w: invalid raw payload bound", ErrInvalidContract)
	}
	endpoint, err := environment.Endpoint()
	if err != nil {
		return nil, err
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "wss" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/ws/api/v2" ||
		(parsed.Host != "www.deribit.com" && parsed.Host != "test.deribit.com") {
		return nil, fmt.Errorf("%w: endpoint is not allowlisted", ErrInvalidContract)
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("deribit: WebSocket redirects are disabled")
	}
	connection, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: &copyClient})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("deribit: dial JSON-RPC v2 socket: %w", err)
	}
	connection.SetReadLimit(int64(maximum))
	return &Socket{endpoint: endpoint, maximum: maximum, conn: connection}, nil
}

func (s *Socket) Endpoint() string {
	if s == nil {
		return ""
	}
	return s.endpoint
}

func (s *Socket) SourceID() string { return SourceID }

func (s *Socket) Write(ctx context.Context, payload []byte) error {
	if s == nil || s.conn == nil || len(payload) == 0 || len(payload) > int(s.maximum) {
		return ErrInvalidRPC
	}
	if err := s.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return fmt.Errorf("deribit: write JSON-RPC message: %w", err)
	}
	return nil
}

func (s *Socket) Read(ctx context.Context) ([]byte, error) {
	if s == nil || s.conn == nil {
		return nil, ErrInvalidRPC
	}
	messageType, payload, err := s.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText || len(payload) == 0 || len(payload) > int(s.maximum) {
		return nil, ErrInvalidRPC
	}
	return slices.Clone(payload), nil
}

func (s *Socket) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	err := s.conn.Close(websocket.StatusNormalClosure, "Deribit market-data capture closed")
	s.conn = nil
	return err
}
