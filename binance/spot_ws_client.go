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

const maximumBufferedControlFrames = 32

// CoderSpotWSConnector is the concrete public-data WebSocket connector. The
// caller must supply a bounded HTTP client and transport; ambient proxy,
// timeout, redirect, and TLS choices are never inherited from http.DefaultClient.
type CoderSpotWSConnector struct {
	client *http.Client
}

func NewCoderSpotWSConnector(client *http.Client) (*CoderSpotWSConnector, error) {
	if client == nil || client.Transport == nil || client.Timeout <= 0 {
		return nil, fmt.Errorf("%w: explicit HTTP transport and timeout are required", ErrSpotConfiguration)
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("binance: WebSocket redirects are disabled")
	}
	return &CoderSpotWSConnector{client: &copyClient}, nil
}

func (c *CoderSpotWSConnector) Connect(ctx context.Context, request SpotWSConnectRequest) (SpotWSConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if request.Endpoint != SpotWSEndpoint || request.MaxApplicationBytes == 0 || request.MaxApplicationBytes > SpotMaxRawPayloadBytes {
		return nil, fmt.Errorf("%w: WebSocket endpoint or message bound is not the public Spot contract", ErrSpotConfiguration)
	}
	endpoint, err := url.Parse(request.Endpoint)
	if err != nil || endpoint.Scheme != "wss" || endpoint.Host != "data-stream.binance.vision" || endpoint.Path != "/ws" || endpoint.RawQuery != "" || endpoint.User != nil {
		return nil, fmt.Errorf("%w: WebSocket endpoint is not allowlisted", ErrSpotConfiguration)
	}
	if request.TimeUnit != "" {
		if request.TimeUnit != "MICROSECOND" {
			return nil, fmt.Errorf("%w: unsupported WebSocket time unit", ErrSpotConfiguration)
		}
		query := endpoint.Query()
		query.Set("timeUnit", request.TimeUnit)
		endpoint.RawQuery = query.Encode()
	}

	connection := &coderSpotWSConnection{maxApplicationBytes: request.MaxApplicationBytes}
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
		return nil, &SpotConnectFailure{Kind: classifySpotConnectFailure(err), Cause: err}
	}
	websocketConnection.SetReadLimit(int64(request.MaxApplicationBytes))
	connection.connection = websocketConnection
	return connection, nil
}

func classifySpotConnectFailure(err error) capture.TransportFailureKind {
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

type coderSpotWSConnection struct {
	connection          *websocket.Conn
	maxApplicationBytes uint32
	readMu              sync.Mutex
	mu                  sync.Mutex
	control             []SpotWSFrame
	buffered            *SpotWSFrame
	autoPongs           [][]byte
	controlOverflow     bool
	closed              bool
}

func (c *coderSpotWSConnection) onPing(payload []byte) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || len(payload) > SpotMaxPingPayloadBytes || len(c.control) == maximumBufferedControlFrames {
		c.controlOverflow = true
		return false
	}
	copyPayload := slices.Clone(payload)
	c.control = append(c.control, SpotWSFrame{Kind: SpotWSFramePing, Payload: copyPayload})
	c.autoPongs = append(c.autoPongs, slices.Clone(copyPayload))
	return true
}

func (c *coderSpotWSConnection) Read(ctx context.Context, maxBytes uint32) (SpotWSFrame, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if err := ctx.Err(); err != nil {
		return SpotWSFrame{}, err
	}
	if maxBytes == 0 || maxBytes > c.maxApplicationBytes {
		return SpotWSFrame{}, ErrSpotBounds
	}
	if frame, ok, err := c.popBuffered(maxBytes); ok || err != nil {
		return frame, err
	}
	messageType, payload, err := c.connection.Read(ctx)
	if err != nil {
		if websocket.CloseStatus(err) != -1 {
			return SpotWSFrame{Kind: SpotWSFrameClose}, nil
		}
		return SpotWSFrame{}, err
	}
	if len(payload) > int(maxBytes) {
		return SpotWSFrame{}, ErrSpotBounds
	}
	kind := SpotWSFrameText
	if messageType == websocket.MessageBinary {
		kind = SpotWSFrameBinary
	}
	application := SpotWSFrame{Kind: kind, Payload: slices.Clone(payload)}
	c.mu.Lock()
	if c.controlOverflow {
		c.mu.Unlock()
		return SpotWSFrame{}, ErrSpotBounds
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

func (c *coderSpotWSConnection) ReadBuffered(ctx context.Context, maxBytes uint32) (SpotWSFrame, bool, error) {
	c.readMu.Lock()
	defer c.readMu.Unlock()
	if err := ctx.Err(); err != nil {
		return SpotWSFrame{}, false, err
	}
	frame, ok, err := c.popBuffered(maxBytes)
	return frame, ok, err
}

func (c *coderSpotWSConnection) popBuffered(maxBytes uint32) (SpotWSFrame, bool, error) {
	if maxBytes == 0 || maxBytes > c.maxApplicationBytes {
		return SpotWSFrame{}, false, ErrSpotBounds
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.controlOverflow {
		return SpotWSFrame{}, false, ErrSpotBounds
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
			return SpotWSFrame{}, false, ErrSpotBounds
		}
		return frame, true, nil
	}
	return SpotWSFrame{}, false, nil
}

func (c *coderSpotWSConnection) Write(ctx context.Context, kind SpotWSWriteKind, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	switch kind {
	case SpotWSWriteText:
		if len(payload) == 0 || len(payload) > SpotMaxControlMessageBytes {
			return ErrSpotBounds
		}
		return c.connection.Write(ctx, websocket.MessageText, slices.Clone(payload))
	case SpotWSWritePong:
		c.mu.Lock()
		defer c.mu.Unlock()
		if len(c.autoPongs) == 0 || !slices.Equal(c.autoPongs[0], payload) {
			return ErrSpotConnection
		}
		// coder/websocket sent the exact pong synchronously from OnPingReceived.
		// This call acknowledges that transport-owned write without duplicating it.
		c.autoPongs = c.autoPongs[1:]
		return nil
	default:
		return ErrSpotConfiguration
	}
}

func (c *coderSpotWSConnection) Close(ctx context.Context, reason capture.CloseReason) error {
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
	return c.connection.Close(status, "public market-data capture closed")
}
