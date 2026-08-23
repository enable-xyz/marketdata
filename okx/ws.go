package okx

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"

	"github.com/coder/websocket"

	"github.com/enable-xyz/marketdata/capture"
)

type Entitlement struct {
	LoggedIn       bool
	VIPLevel       uint8
	LoginEvidence  string
	SourceIdentity string
}

func (e Entitlement) requestsVIP4() bool {
	return e.VIPLevel >= 4 && e.SourceIdentity != "" && len(e.SourceIdentity) <= 256 && strings.IndexByte(e.SourceIdentity, 0) < 0
}

func (e Entitlement) permitsVIP4() bool {
	return e.requestsVIP4() && e.LoggedIn && e.LoginEvidence != "" && len(e.LoginEvidence) <= 4096 && strings.IndexByte(e.LoginEvidence, 0) < 0
}

type SubscriptionArg struct {
	Channel          string `json:"channel"`
	InstrumentType   string `json:"instType,omitempty"`
	InstrumentFamily string `json:"instFamily,omitempty"`
	InstrumentID     string `json:"instId,omitempty"`
}

func (a SubscriptionArg) Validate(kind SocketKind, entitlement Entitlement) error {
	if kind.Endpoint() == "" || !validIdentifier(a.Channel, 64) || !validOptionalIdentifier(a.InstrumentType, 32) ||
		!validOptionalIdentifier(a.InstrumentFamily, 128) || !validOptionalIdentifier(a.InstrumentID, 128) {
		return ErrInvalidSubscription
	}
	if a.InstrumentType != "" {
		if err := InstrumentType(a.InstrumentType).Validate(); err != nil {
			return ErrInvalidSubscription
		}
	}
	switch a.Channel {
	case "trades-all":
		if kind != BusinessSocket || a.InstrumentID == "" || a.InstrumentType != "" || a.InstrumentFamily != "" {
			return fmt.Errorf("%w: trades-all requires one business-socket instId", ErrInvalidSubscription)
		}
	case "trades", "books", "books5", "bbo-tbt", "tickers", "funding-rate", "mark-price", "index-tickers":
		if kind != PublicSocket || a.InstrumentID == "" || a.InstrumentType != "" || a.InstrumentFamily != "" {
			return ErrInvalidSubscription
		}
	case "open-interest":
		if kind != PublicSocket || a.InstrumentID == "" || a.InstrumentFamily != "" ||
			(a.InstrumentType != string(Swap) && a.InstrumentType != string(Futures) && a.InstrumentType != string(Option)) {
			return fmt.Errorf("%w: open-interest requires SWAP, FUTURES, or OPTION instType and instId", ErrInvalidSubscription)
		}
	case "books50-l2-tbt", "books-l2-tbt":
		if kind != PublicSocket || a.InstrumentID == "" || a.InstrumentType != "" || a.InstrumentFamily != "" {
			return ErrInvalidSubscription
		}
		if !entitlement.permitsVIP4() {
			return ErrVIPEntitlement
		}
	case "books-rpi-tbt":
		if kind != PublicSocket || a.InstrumentID == "" || a.InstrumentType != "" || a.InstrumentFamily != "" || entitlement.SourceIdentity == "" {
			return fmt.Errorf("%w: RPI access must be caller-declared", ErrInvalidSubscription)
		}
	case "opt-summary":
		if kind != PublicSocket || (a.InstrumentFamily == "") == (a.InstrumentID == "") || a.InstrumentType != "" {
			return ErrInvalidSubscription
		}
	case "liquidation-orders":
		if kind != PublicSocket || (a.InstrumentType != string(Swap) && a.InstrumentType != string(Futures) && a.InstrumentType != string(Option)) || a.InstrumentID != "" {
			return ErrInvalidSubscription
		}
	default:
		return fmt.Errorf("%w: unsupported channel %q", ErrInvalidSubscription, a.Channel)
	}
	return nil
}

func validIdentifier(value string, maximum int) bool {
	if value == "" || len(value) > maximum || strings.IndexByte(value, 0) >= 0 {
		return false
	}
	for _, r := range value {
		if (r < 'A' || r > 'Z') && (r < 'a' || r > 'z') && (r < '0' || r > '9') && r != '-' && r != '_' {
			return false
		}
	}
	return true
}

func validOptionalIdentifier(value string, maximum int) bool {
	return value == "" || validIdentifier(value, maximum)
}

func (a SubscriptionArg) identity() string {
	return a.Channel + "\x00" + a.InstrumentType + "\x00" + a.InstrumentFamily + "\x00" + a.InstrumentID
}

type SubscribeRequest struct {
	ID        string            `json:"id"`
	Operation string            `json:"op"`
	Arguments []SubscriptionArg `json:"args"`
}

func SubscribeMessage(id string, kind SocketKind, entitlement Entitlement, args []SubscriptionArg) ([]byte, error) {
	if !validIdentifier(id, 32) || len(args) == 0 || len(args) > MaxPendingACK {
		return nil, ErrInvalidSubscription
	}
	canonical := slices.Clone(args)
	slices.SortFunc(canonical, func(a, b SubscriptionArg) int { return strings.Compare(a.identity(), b.identity()) })
	for i, arg := range canonical {
		if err := arg.Validate(kind, entitlement); err != nil {
			return nil, err
		}
		if i > 0 && arg.identity() == canonical[i-1].identity() {
			return nil, fmt.Errorf("%w: duplicate argument", ErrInvalidSubscription)
		}
	}
	payload, err := json.Marshal(SubscribeRequest{ID: id, Operation: "subscribe", Arguments: canonical})
	if err != nil {
		return nil, fmt.Errorf("okx: encode subscription: %w", err)
	}
	return payload, nil
}

type SubscriptionACK struct {
	ID           string          `json:"id"`
	Event        string          `json:"event"`
	Argument     SubscriptionArg `json:"arg"`
	ConnectionID string          `json:"connId"`
	Code         string          `json:"code"`
	Message      string          `json:"msg"`
}

type SubscriptionRejection struct {
	Code     string
	Message  string
	Terminal bool
}

func (e *SubscriptionRejection) Error() string {
	return fmt.Sprintf("okx: subscription rejected with code %s: %s", e.Code, e.Message)
}

type TerminalDenialEvidence struct {
	Argument     SubscriptionArg
	Code         string
	Message      string
	ConnectionID string
	Raw          []byte
}

func ParseSubscriptionACK(payload []byte) (SubscriptionACK, error) {
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes {
		return SubscriptionACK{}, ErrInvalidPayload
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var ack SubscriptionACK
	if err := decoder.Decode(&ack); err != nil || decoder.More() {
		return SubscriptionACK{}, ErrInvalidPayload
	}
	if !validOptionalIdentifier(ack.ID, 32) {
		return SubscriptionACK{}, ErrInvalidPayload
	}
	switch ack.Event {
	case "subscribe":
		if ack.Argument.Channel == "" || ack.Code != "" || ack.Message != "" {
			return SubscriptionACK{}, ErrInvalidPayload
		}
		return ack, nil
	case "error":
		if ack.Code == "" || ack.Message == "" {
			return SubscriptionACK{}, ErrInvalidPayload
		}
		return ack, &SubscriptionRejection{Code: ack.Code, Message: ack.Message, Terminal: ack.Code == "64003"}
	default:
		return SubscriptionACK{}, ErrInvalidPayload
	}
}

// SubscriptionSession owns exact desired, pending-request, and acknowledged
// inventories for one socket epoch. Reconnect never assumes server state: it
// clears connection-local state and returns canonical requests with new IDs.
type SubscriptionSession struct {
	kind         SocketKind
	entitlement  Entitlement
	generation   uint64
	connectionID string
	desired      map[string]SubscriptionArg
	acked        map[string]struct{}
	pendingIDs   map[string]string
	denied       map[string]TerminalDenialEvidence
}

func NewSubscriptionSession(kind SocketKind, entitlement Entitlement, args []SubscriptionArg) (*SubscriptionSession, error) {
	if kind.Endpoint() == "" || len(args) == 0 || len(args) > MaxPendingACK {
		return nil, ErrInvalidSubscription
	}
	session := &SubscriptionSession{kind: kind, entitlement: entitlement, generation: 1, desired: make(map[string]SubscriptionArg, len(args)), acked: make(map[string]struct{}, len(args)), pendingIDs: make(map[string]string, len(args)), denied: make(map[string]TerminalDenialEvidence)}
	for _, arg := range args {
		if err := arg.Validate(kind, entitlement); err != nil {
			return nil, err
		}
		identity := arg.identity()
		if _, exists := session.desired[identity]; exists {
			return nil, fmt.Errorf("%w: duplicate desired argument", ErrInvalidSubscription)
		}
		session.desired[identity] = arg
	}
	return session, nil
}

func (s *SubscriptionSession) Generation() uint64 {
	if s == nil {
		return 0
	}
	return s.generation
}

func (s *SubscriptionSession) Pending() int {
	if s == nil {
		return 0
	}
	return len(s.desired) - len(s.acked)
}

func (s *SubscriptionSession) Messages() ([][]byte, error) {
	if s == nil {
		return nil, ErrInvalidSubscription
	}
	if len(s.desired) == 0 {
		return [][]byte{}, nil
	}
	args := make([]SubscriptionArg, 0, len(s.desired))
	for _, arg := range s.desired {
		if _, ok := s.acked[arg.identity()]; !ok {
			args = append(args, arg)
		}
	}
	slices.SortFunc(args, func(a, b SubscriptionArg) int { return strings.Compare(a.identity(), b.identity()) })
	messages := make([][]byte, 0, len(args))
	for index, arg := range args {
		identity := arg.identity()
		id, exists := s.pendingIDs[identity]
		if !exists {
			id = fmt.Sprintf("okx%04d%04d", s.generation, index+1)
		}
		message, err := SubscribeMessage(id, s.kind, s.entitlement, []SubscriptionArg{arg})
		if err != nil {
			return nil, err
		}
		s.pendingIDs[identity] = id
		messages = append(messages, message)
	}
	return messages, nil
}

func (s *SubscriptionSession) validateEchoedRequestID(identity, echoed string) error {
	if echoed == "" {
		return nil
	}
	pending, exists := s.pendingIDs[identity]
	if !exists {
		return fmt.Errorf("%w: acknowledgement echoed an unknown request ID", ErrUnexpectedACK)
	}
	if echoed != pending {
		return fmt.Errorf("%w: acknowledgement request ID did not match its argument", ErrUnexpectedACK)
	}
	return nil
}

// ValidateMessage binds an outgoing operation to this session's validated
// socket kind, caller-supplied entitlement evidence, and pending inventory.
func (s *SubscriptionSession) ValidateMessage(payload []byte) error {
	if s == nil || len(payload) == 0 || len(payload) > MaxRawPayloadBytes {
		return ErrInvalidSubscription
	}
	var request SubscribeRequest
	if err := json.Unmarshal(payload, &request); err != nil || request.Operation != "subscribe" || len(request.Arguments) != 1 {
		return ErrInvalidSubscription
	}
	canonical, err := SubscribeMessage(request.ID, s.kind, s.entitlement, request.Arguments)
	if err != nil || !bytes.Equal(payload, canonical) {
		return ErrInvalidSubscription
	}
	identity := request.Arguments[0].identity()
	if _, desired := s.desired[identity]; !desired {
		return fmt.Errorf("%w: outgoing argument is not desired", ErrInvalidSubscription)
	}
	if _, acknowledged := s.acked[identity]; acknowledged {
		return fmt.Errorf("%w: outgoing argument is already acknowledged", ErrInvalidSubscription)
	}
	return nil
}

func (s *SubscriptionSession) Acknowledge(payload []byte) (SubscriptionACK, error) {
	if s == nil {
		return SubscriptionACK{}, ErrUnexpectedACK
	}
	ack, err := ParseSubscriptionACK(payload)
	if err != nil {
		var rejection *SubscriptionRejection
		if !errors.As(err, &rejection) || !rejection.Terminal {
			return ack, err
		}
		if ack.Code != "64003" || ack.ConnectionID == "" || (ack.Argument.Channel != "books50-l2-tbt" && ack.Argument.Channel != "books-l2-tbt") {
			return ack, err
		}
		if validateErr := ack.Argument.Validate(s.kind, s.entitlement); validateErr != nil {
			return SubscriptionACK{}, fmt.Errorf("%w: invalid terminal-denial argument", ErrUnexpectedACK)
		}
		identity := ack.Argument.identity()
		if _, desired := s.desired[identity]; !desired {
			return SubscriptionACK{}, fmt.Errorf("%w: terminal denial did not match desired inventory", ErrUnexpectedACK)
		}
		if idErr := s.validateEchoedRequestID(identity, ack.ID); idErr != nil {
			return SubscriptionACK{}, idErr
		}
		if s.connectionID == "" {
			s.connectionID = ack.ConnectionID
		} else if s.connectionID != ack.ConnectionID {
			return SubscriptionACK{}, fmt.Errorf("%w: terminal denial connection changed", ErrUnexpectedACK)
		}
		s.denied[identity] = TerminalDenialEvidence{Argument: ack.Argument, Code: ack.Code, Message: ack.Message, ConnectionID: ack.ConnectionID, Raw: slices.Clone(payload)}
		delete(s.desired, identity)
		delete(s.acked, identity)
		delete(s.pendingIDs, identity)
		return ack, err
	}
	if validateErr := ack.Argument.Validate(s.kind, s.entitlement); validateErr != nil {
		return SubscriptionACK{}, fmt.Errorf("%w: invalid acknowledged argument", ErrUnexpectedACK)
	}
	identity := ack.Argument.identity()
	if _, desired := s.desired[identity]; !desired {
		return SubscriptionACK{}, fmt.Errorf("%w: unrequested argument", ErrUnexpectedACK)
	}
	if _, duplicate := s.acked[identity]; duplicate {
		return SubscriptionACK{}, fmt.Errorf("%w: duplicate argument", ErrUnexpectedACK)
	}
	if idErr := s.validateEchoedRequestID(identity, ack.ID); idErr != nil {
		return SubscriptionACK{}, idErr
	}
	if ack.ConnectionID != "" {
		if s.connectionID == "" {
			s.connectionID = ack.ConnectionID
		} else if s.connectionID != ack.ConnectionID {
			return SubscriptionACK{}, fmt.Errorf("%w: acknowledgement connection changed", ErrUnexpectedACK)
		}
	}
	s.acked[identity] = struct{}{}
	delete(s.pendingIDs, identity)
	return ack, nil
}

func (s *SubscriptionSession) TerminalDenials() []TerminalDenialEvidence {
	if s == nil || len(s.denied) == 0 {
		return nil
	}
	identities := make([]string, 0, len(s.denied))
	for identity := range s.denied {
		identities = append(identities, identity)
	}
	slices.Sort(identities)
	result := make([]TerminalDenialEvidence, len(identities))
	for index, identity := range identities {
		result[index] = s.denied[identity]
		result[index].Raw = slices.Clone(result[index].Raw)
	}
	return result
}

// RenewDeniedSubscription is the only in-session transition that clears a
// terminal denial. It requires explicit new intent and different, valid login
// evidence, and resets connection-local acknowledgements.
func (s *SubscriptionSession) RenewDeniedSubscription(entitlement Entitlement, arg SubscriptionArg) error {
	if s == nil || entitlement == s.entitlement || !entitlement.permitsVIP4() || s.generation == ^uint64(0) {
		return ErrVIPEntitlement
	}
	login, err := parseLoginACK([]byte(entitlement.LoginEvidence))
	if err != nil {
		return ErrVIPEntitlement
	}
	if err := arg.Validate(s.kind, entitlement); err != nil {
		return err
	}
	identity := arg.identity()
	denial, denied := s.denied[identity]
	if !denied {
		return fmt.Errorf("%w: subscription has no terminal denial", ErrInvalidSubscription)
	}
	if login.ConnectionID == denial.ConnectionID {
		return ErrVIPEntitlement
	}
	s.entitlement = entitlement
	s.desired[identity] = arg
	delete(s.denied, identity)
	s.generation++
	s.acked = make(map[string]struct{}, len(s.desired))
	s.pendingIDs = make(map[string]string, len(s.desired))
	s.connectionID = ""
	return nil
}

func (s *SubscriptionSession) ReconnectMessages() ([][]byte, error) {
	if s == nil || s.generation == ^uint64(0) {
		return nil, ErrInvalidSubscription
	}
	s.generation++
	s.acked = make(map[string]struct{}, len(s.desired))
	s.pendingIDs = make(map[string]string, len(s.desired))
	s.connectionID = ""
	return s.Messages()
}

// socketConnection is the narrow public transport surface used by Socket.
type socketConnection interface {
	Write(context.Context, websocket.MessageType, []byte) error
	Read(context.Context) (websocket.MessageType, []byte, error)
	SetReadLimit(int64)
	Close(websocket.StatusCode, string) error
}

// LoginConnection is a login-only view of one live OKX connection. The
// caller-owned Authenticator may write only an op=login JSON request and read
// text acknowledgements; shared code never owns credentials or signing.
type LoginConnection interface {
	WriteLogin(context.Context, []byte) error
	ReadLoginACK(context.Context) ([]byte, error)
}

type Authenticator interface {
	Authenticate(context.Context, LoginConnection) ([]byte, error)
}

type AuthenticatorFunc func(context.Context, LoginConnection) ([]byte, error)

func (f AuthenticatorFunc) Authenticate(ctx context.Context, connection LoginConnection) ([]byte, error) {
	return f(ctx, connection)
}

type SocketConfig struct {
	Kind          SocketKind
	Maximum       uint32
	Entitlement   Entitlement
	Authenticator Authenticator
}

type loginOnlyConnection struct {
	conn     socketConnection
	maximum  uint32
	wrote    bool
	lastRead []byte
}

func (c *loginOnlyConnection) WriteLogin(ctx context.Context, payload []byte) error {
	if c == nil || c.conn == nil || c.wrote || len(payload) == 0 || len(payload) > int(c.maximum) {
		return ErrInvalidPayload
	}
	var request struct {
		Operation string            `json:"op"`
		Arguments []json.RawMessage `json:"args"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil || request.Operation != "login" || len(request.Arguments) != 1 || len(request.Arguments[0]) == 0 {
		return ErrInvalidConfiguration
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return ErrInvalidConfiguration
	}
	if err := c.conn.Write(ctx, websocket.MessageText, payload); err != nil {
		return err
	}
	c.wrote = true
	return nil
}

func (c *loginOnlyConnection) ReadLoginACK(ctx context.Context) ([]byte, error) {
	if c == nil || c.conn == nil || !c.wrote {
		return nil, ErrInvalidConfiguration
	}
	messageType, payload, err := c.conn.Read(ctx)
	if err != nil {
		return nil, err
	}
	if messageType != websocket.MessageText || len(payload) == 0 || len(payload) > int(c.maximum) {
		return nil, ErrInvalidPayload
	}
	c.lastRead = slices.Clone(payload)
	return slices.Clone(payload), nil
}

type loginACK struct {
	Event        string `json:"event"`
	Code         string `json:"code"`
	Message      string `json:"msg"`
	ConnectionID string `json:"connId"`
}

func parseLoginACK(payload []byte) (loginACK, error) {
	var ack loginACK
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ack); err != nil {
		return loginACK{}, ErrVIPEntitlement
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF || ack.Event != "login" || ack.Code != "0" || ack.Message != "" || !validIdentifier(ack.ConnectionID, 256) {
		return loginACK{}, ErrVIPEntitlement
	}
	return ack, nil
}

func authenticateVIPSocket(ctx context.Context, connection socketConnection, maximum uint32, clock capture.Clock, operations capture.RateBudget, requested Entitlement, authenticator Authenticator) (Entitlement, []byte, string, error) {
	if !requested.requestsVIP4() {
		if authenticator != nil || requested.LoggedIn || requested.LoginEvidence != "" || requested.VIPLevel != 0 {
			return Entitlement{}, nil, "", ErrInvalidConfiguration
		}
		return requested, nil, "", nil
	}
	if requested.LoggedIn || requested.LoginEvidence != "" || authenticator == nil || connection == nil || clock == nil || operations == nil {
		return Entitlement{}, nil, "", ErrVIPEntitlement
	}
	decision, err := operations.Acquire(clock.Read().MonotonicNS, OperationRatePolicy().RequestCost)
	if err != nil {
		return Entitlement{}, nil, "", err
	}
	if !decision.Allowed {
		return Entitlement{}, nil, "", fmt.Errorf("%w: login retry at monotonic %d", ErrRateLimited, decision.RetryAtMonotonic)
	}
	loginConnection := &loginOnlyConnection{conn: connection, maximum: maximum}
	evidence, err := authenticator.Authenticate(ctx, loginConnection)
	if err != nil || !loginConnection.wrote || !bytes.Equal(evidence, loginConnection.lastRead) {
		return Entitlement{}, nil, "", ErrVIPEntitlement
	}
	ack, err := parseLoginACK(evidence)
	if err != nil {
		return Entitlement{}, nil, "", err
	}
	authenticated := requested
	authenticated.LoggedIn = true
	authenticated.LoginEvidence = string(evidence)
	return authenticated, slices.Clone(evidence), ack.ConnectionID, nil
}

// Socket transports public market-data text frames. Subscription writes must
// be bound to a validated session and consume the per-connection operation
// budget. Reconnect callers use a fresh Socket and the session's exact replay.
type Socket struct {
	kind              SocketKind
	endpoint          string
	maximum           uint32
	clock             capture.Clock
	operations        capture.RateBudget
	conn              socketConnection
	entitlement       Entitlement
	authenticated     bool
	loginACK          []byte
	loginConnectionID string
}

func NewHandshakeBudget(initialMonotonicNS uint64) (*capture.TokenRateBudget, error) {
	return capture.NewTokenRateBudget(HandshakeRatePolicy(), initialMonotonicNS)
}

func NewOperationBudget(initialMonotonicNS uint64) (*capture.TokenRateBudget, error) {
	return capture.NewTokenRateBudget(OperationRatePolicy(), initialMonotonicNS)
}

func DialSocket(ctx context.Context, config SocketConfig, client *http.Client, clock capture.Clock, sharedHandshakeBudget capture.RateBudget) (*Socket, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	endpoint := config.Kind.Endpoint()
	if endpoint == "" || client == nil || client.Transport == nil || client.Timeout <= 0 || config.Maximum == 0 || config.Maximum > MaxRawPayloadBytes || clock == nil || sharedHandshakeBudget == nil {
		return nil, ErrInvalidConfiguration
	}
	if config.Entitlement.requestsVIP4() {
		if config.Kind != PublicSocket || config.Authenticator == nil || config.Entitlement.LoggedIn || config.Entitlement.LoginEvidence != "" {
			return nil, ErrVIPEntitlement
		}
	} else if config.Authenticator != nil || config.Entitlement.LoggedIn || config.Entitlement.LoginEvidence != "" || config.Entitlement.VIPLevel != 0 {
		return nil, ErrInvalidConfiguration
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "wss" || parsed.Host != "ws.okx.com:8443" || parsed.User != nil || parsed.RawQuery != "" ||
		(parsed.Path != "/ws/v5/public" && parsed.Path != "/ws/v5/business") {
		return nil, ErrInvalidConfiguration
	}
	now := clock.Read().MonotonicNS
	decision, err := sharedHandshakeBudget.Acquire(now, HandshakeRatePolicy().ConnectionCost)
	if err != nil {
		return nil, err
	}
	if !decision.Allowed {
		return nil, fmt.Errorf("%w: handshake retry at monotonic %d", ErrRateLimited, decision.RetryAtMonotonic)
	}
	operations, err := NewOperationBudget(now)
	if err != nil {
		return nil, err
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return errors.New("okx: WebSocket redirects are disabled") }
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: &copyClient})
	if response != nil && response.Body != nil {
		_ = response.Body.Close()
	}
	if err != nil {
		return nil, fmt.Errorf("okx: dial %s socket: %w", config.Kind, err)
	}
	conn.SetReadLimit(int64(config.Maximum))
	entitlement, loginEvidence, loginConnectionID, err := authenticateVIPSocket(ctx, conn, config.Maximum, clock, operations, config.Entitlement, config.Authenticator)
	if err != nil {
		_ = conn.Close(websocket.StatusPolicyViolation, "public data authentication failed")
		return nil, err
	}
	return &Socket{kind: config.Kind, endpoint: endpoint, maximum: config.Maximum, clock: clock, operations: operations, conn: conn, entitlement: entitlement, authenticated: entitlement.permitsVIP4(), loginACK: loginEvidence, loginConnectionID: loginConnectionID}, nil
}

func (s *Socket) Kind() SocketKind {
	if s == nil {
		return ""
	}
	return s.kind
}

func (s *Socket) Endpoint() string {
	if s == nil {
		return ""
	}
	return s.endpoint
}

func (s *Socket) Entitlement() Entitlement {
	if s == nil {
		return Entitlement{}
	}
	return s.entitlement
}

func (s *Socket) LoginACKEvidence() []byte {
	if s == nil {
		return nil
	}
	return slices.Clone(s.loginACK)
}

func (s *Socket) WriteSubscription(ctx context.Context, session *SubscriptionSession, payload []byte) error {
	if s == nil || s.conn == nil || s.clock == nil || s.operations == nil || session == nil || session.kind != s.kind ||
		len(payload) == 0 || len(payload) > int(s.maximum) {
		return ErrInvalidPayload
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := session.ValidateMessage(payload); err != nil {
		return err
	}
	if session.entitlement.requestsVIP4() {
		if !s.authenticated || !session.entitlement.permitsVIP4() || session.entitlement != s.entitlement || len(s.loginACK) == 0 || s.loginConnectionID == "" {
			return ErrVIPEntitlement
		}
		if session.connectionID == "" {
			session.connectionID = s.loginConnectionID
		} else if session.connectionID != s.loginConnectionID {
			return fmt.Errorf("%w: VIP session is bound to another authenticated connection", ErrUnexpectedACK)
		}
	}
	decision, err := s.operations.Acquire(s.clock.Read().MonotonicNS, OperationRatePolicy().RequestCost)
	if err != nil {
		return err
	}
	if !decision.Allowed {
		return fmt.Errorf("%w: subscription retry at monotonic %d", ErrRateLimited, decision.RetryAtMonotonic)
	}
	return s.conn.Write(ctx, websocket.MessageText, payload)
}

func (s *Socket) Ping(ctx context.Context) error {
	if s == nil || s.conn == nil {
		return ErrInvalidConfiguration
	}
	return s.conn.Write(ctx, websocket.MessageText, []byte("ping"))
}

func (s *Socket) Read(ctx context.Context) ([]byte, error) {
	if s == nil || s.conn == nil {
		return nil, ErrInvalidConfiguration
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

func (s *Socket) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	err := s.conn.Close(websocket.StatusNormalClosure, "public market-data capture closed")
	s.conn = nil
	s.authenticated = false
	s.loginACK = nil
	s.loginConnectionID = ""
	s.entitlement.LoggedIn = false
	s.entitlement.LoginEvidence = ""
	return err
}
