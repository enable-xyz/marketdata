package binance

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"slices"
	"sync"

	"github.com/coder/websocket"
	"github.com/enable-xyz/marketdata/capture"
)

const maximumBufferedDerivativeControlFrames = 32

// CoderDerivativeWSConnector is the concrete public-data connector shared by
// USD-M and COIN-M. The caller supplies an explicit bounded HTTP client and
// transport; redirects and ambient http.DefaultClient behavior are rejected.
type CoderDerivativeWSConnector struct {
	client *http.Client
}

func NewCoderDerivativeWSConnector(client *http.Client) (*CoderDerivativeWSConnector, error) {
	if client == nil || client.Transport == nil || client.Timeout <= 0 {
		return nil, fmt.Errorf("%w: explicit HTTP transport and timeout are required", ErrDerivativeConfiguration)
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("binance: derivative WebSocket redirects are disabled")
	}
	return &CoderDerivativeWSConnector{client: &copyClient}, nil
}

func (c *CoderDerivativeWSConnector) Connect(ctx context.Context, request DerivativeWSConnectRequest) (DerivativeWSConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	maximum := derivativeMaxRawPayload(request.Product)
	if maximum == 0 || request.MaxApplicationBytes == 0 || request.MaxApplicationBytes > maximum {
		return nil, fmt.Errorf("%w: product or application-message bound is invalid", ErrDerivativeConfiguration)
	}
	endpoint, err := derivativeDialEndpoint(request.Product, request.Endpoint)
	if err != nil {
		return nil, err
	}
	connection := &coderDerivativeWSConnection{maxApplicationBytes: request.MaxApplicationBytes}
	websocketConnection, response, err := websocket.Dial(ctx, endpoint.String(), &websocket.DialOptions{
		HTTPClient: c.client,
		OnPingReceived: func(_ context.Context, payload []byte) bool {
			return connection.onPing(payload)
		},
	})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, &DerivativeConnectFailure{Kind: classifyDerivativeConnectFailure(err), Cause: err}
	}
	websocketConnection.SetReadLimit(int64(request.MaxApplicationBytes))
	connection.connection = websocketConnection
	return connection, nil
}

func derivativeDialEndpoint(product DerivativeProduct, endpoint string) (*url.URL, error) {
	allowed := false
	switch product {
	case DerivativeProductUSDM:
		allowed = endpoint == USDMPublicEndpoint || endpoint == USDMMarketEndpoint
	case DerivativeProductCoinM:
		allowed = endpoint == CoinMWebSocketEndpoint
	}
	if !allowed {
		return nil, fmt.Errorf("%w: product endpoint is not allowlisted", ErrDerivativeConfiguration)
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "wss" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.RawPath != "" || parsed.Opaque != "" {
		return nil, fmt.Errorf("%w: malformed derivative WebSocket endpoint", ErrDerivativeConfiguration)
	}
	switch product {
	case DerivativeProductUSDM:
		if parsed.Host != "fstream.binance.com" || (parsed.Path != "/public" && parsed.Path != "/market") {
			return nil, fmt.Errorf("%w: USD-M endpoint is not the declared public host and route", ErrDerivativeConfiguration)
		}
	case DerivativeProductCoinM:
		if parsed.Host != "dstream.binance.com" || parsed.Path != "" {
			return nil, fmt.Errorf("%w: COIN-M endpoint is not the declared public host", ErrDerivativeConfiguration)
		}
	default:
		return nil, fmt.Errorf("%w: unknown derivative product", ErrDerivativeConfiguration)
	}
	parsed.Path += "/stream"
	return parsed, nil
}

func classifyDerivativeConnectFailure(err error) capture.TransportFailureKind {
	var dns *net.DNSError
	var unknownAuthority x509.UnknownAuthorityError
	var certificateInvalid x509.CertificateInvalidError
	var recordHeader tls.RecordHeaderError
	if errors.As(err, &dns) {
		return capture.TransportFailureDNS
	}
	if errors.As(err, &unknownAuthority) || errors.As(err, &certificateInvalid) || errors.As(err, &recordHeader) {
		return capture.TransportFailureTLS
	}
	return capture.TransportFailureConnect
}

type coderDerivativeWSConnection struct {
	connection          *websocket.Conn
	maxApplicationBytes uint32
	readMu              sync.Mutex
	mu                  sync.Mutex
	control             []DerivativeWSFrame
	buffered            *DerivativeWSFrame
	autoPongs           [][]byte
	controlOverflow     bool
	closed              bool
}

func (c *coderDerivativeWSConnection) onPing(payload []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(payload) > DerivativeMaxPingPayloadBytes || len(c.control) == maximumBufferedDerivativeControlFrames {
		c.controlOverflow = true
		return false
	}
	copyPayload := slices.Clone(payload)
	c.control = append(c.control, DerivativeWSFrame{Kind: DerivativeWSFramePing, Payload: copyPayload})
	c.autoPongs = append(c.autoPongs, slices.Clone(copyPayload))
	return true
}

func (c *coderDerivativeWSConnection) Read(ctx context.Context, maxBytes uint32) (DerivativeWSFrame, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if err := ctx.Err(); err != nil {
		return DerivativeWSFrame{}, err
	}
	if maxBytes == 0 || maxBytes > c.maxApplicationBytes {
		return DerivativeWSFrame{}, ErrDerivativeBounds
	}
	if frame, ok, err := c.popBuffered(maxBytes); ok || err != nil {
		return frame, err
	}
	messageType, payload, err := c.connection.Read(ctx)
	if err != nil {
		if websocket.CloseStatus(err) != -1 {
			return DerivativeWSFrame{Kind: DerivativeWSFrameClose}, nil
		}
		return DerivativeWSFrame{}, err
	}
	if len(payload) > int(maxBytes) {
		return DerivativeWSFrame{}, ErrDerivativeBounds
	}
	kind := DerivativeWSFrameText
	if messageType == websocket.MessageBinary {
		kind = DerivativeWSFrameBinary
	}
	application := DerivativeWSFrame{Kind: kind, Payload: slices.Clone(payload)}
	c.mu.Lock()
	if c.controlOverflow {
		c.mu.Unlock()
		return DerivativeWSFrame{}, ErrDerivativeBounds
	}
	if len(c.control) != 0 {
		frame := c.control[0]
		c.control = c.control[1:]
		c.buffered = &application
		c.mu.Unlock()
		return frame, nil
	}
	c.mu.Unlock()
	return application, nil
}

func (c *coderDerivativeWSConnection) ReadBuffered(ctx context.Context, maxBytes uint32) (DerivativeWSFrame, bool, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if err := ctx.Err(); err != nil {
		return DerivativeWSFrame{}, false, err
	}
	return c.popBuffered(maxBytes)
}

func (c *coderDerivativeWSConnection) popBuffered(maxBytes uint32) (DerivativeWSFrame, bool, error) {
	if maxBytes == 0 || maxBytes > c.maxApplicationBytes {
		return DerivativeWSFrame{}, false, ErrDerivativeBounds
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.controlOverflow {
		return DerivativeWSFrame{}, false, ErrDerivativeBounds
	}
	if len(c.control) != 0 {
		frame := c.control[0]
		c.control = c.control[1:]
		return frame, true, nil
	}
	if c.buffered != nil {
		frame := *c.buffered
		c.buffered = nil
		if len(frame.Payload) > int(maxBytes) {
			return DerivativeWSFrame{}, false, ErrDerivativeBounds
		}
		return frame, true, nil
	}
	return DerivativeWSFrame{}, false, nil
}

func (c *coderDerivativeWSConnection) Write(ctx context.Context, kind DerivativeWSWriteKind, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch kind {
	case DerivativeWSWriteText:
		if len(payload) == 0 || len(payload) > DerivativeMaxControlMessageBytes {
			return ErrDerivativeBounds
		}
		return c.connection.Write(ctx, websocket.MessageText, slices.Clone(payload))
	case DerivativeWSWritePong:
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.autoPongs) == 0 || !slices.Equal(c.autoPongs[0], payload) {
			return ErrDerivativeConnection
		}
		// coder/websocket sent this exact pong synchronously from OnPingReceived.
		c.autoPongs = c.autoPongs[1:]
		return nil
	default:
		return ErrDerivativeConfiguration
	}
}

func (c *coderDerivativeWSConnection) Close(ctx context.Context, reason capture.CloseReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.mu.Unlock()
	status := websocket.StatusGoingAway
	if reason == capture.ClosePlanned {
		status = websocket.StatusNormalClosure
	}
	return c.connection.Close(status, "public derivative market-data capture closed")
}
