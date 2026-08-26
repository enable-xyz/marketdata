package deribit

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
)

const (
	JSONRPCVersion      = "2.0"
	CreditExhaustedCode = 10028
)

var (
	ErrInvalidRPC          = errors.New("deribit: invalid JSON-RPC message")
	ErrSubscribeMismatch   = errors.New("deribit: returned subscription list differs from request")
	ErrReconnectRequired   = errors.New("deribit: reconnect required after credit exhaustion")
	ErrSubscriptionPending = errors.New("deribit: subscription acknowledgement is pending")
)

type ClientCredentials struct {
	ClientID     string
	ClientSecret []byte
}

func NewClientCredentials(clientID string, clientSecret []byte) (ClientCredentials, error) {
	credentials := ClientCredentials{ClientID: clientID, ClientSecret: slices.Clone(clientSecret)}
	if err := credentials.validate(); err != nil {
		return ClientCredentials{}, err
	}
	return credentials, nil
}

func (c ClientCredentials) validate() error {
	if c.ClientID == "" || len(c.ClientID) > 256 || strings.IndexByte(c.ClientID, 0) >= 0 ||
		len(c.ClientSecret) == 0 || len(c.ClientSecret) > 1024 || bytes.IndexByte(c.ClientSecret, 0) >= 0 {
		return ErrAuthorizationRequired
	}
	return nil
}

type SubscriptionReconciliation struct {
	Requested  []string
	Returned   []string
	Missing    []string
	Unexpected []string
	Exact      bool
}

func ReconcileSubscriptions(requested, returned []string) (SubscriptionReconciliation, error) {
	requestSet, requestedSorted, err := canonicalChannelSet(requested, false)
	if err != nil {
		return SubscriptionReconciliation{}, err
	}
	returnSet, returnedSorted, err := canonicalChannelSet(returned, true)
	if err != nil {
		return SubscriptionReconciliation{}, err
	}
	result := SubscriptionReconciliation{Requested: requestedSorted, Returned: returnedSorted}
	for _, channel := range requestedSorted {
		if _, ok := returnSet[channel]; !ok {
			result.Missing = append(result.Missing, channel)
		}
	}
	for _, channel := range returnedSorted {
		if _, ok := requestSet[channel]; !ok {
			result.Unexpected = append(result.Unexpected, channel)
		}
	}
	result.Exact = len(result.Missing) == 0 && len(result.Unexpected) == 0
	return result, nil
}

func canonicalChannelSet(channels []string, allowEmpty bool) (map[string]struct{}, []string, error) {
	if len(channels) > MaxSubscriptions || (!allowEmpty && len(channels) == 0) {
		return nil, nil, ErrInvalidChannel
	}
	set := make(map[string]struct{}, len(channels))
	for _, channel := range channels {
		if channel == "" || len(channel) > 512 || strings.IndexByte(channel, 0) >= 0 {
			return nil, nil, ErrInvalidChannel
		}
		if _, exists := set[channel]; exists {
			return nil, nil, fmt.Errorf("%w: duplicate channel", ErrInvalidChannel)
		}
		set[channel] = struct{}{}
	}
	sorted := slices.Clone(channels)
	slices.Sort(sorted)
	return set, sorted, nil
}

type SessionAction string

const (
	SessionContinue             SessionAction = "continue"
	SessionRespondTest          SessionAction = "respond_test"
	SessionReconnectAfterCredit SessionAction = "reconnect_after_credit_refill"
)

type SessionDecision struct {
	Action          SessionAction
	Response        []byte
	Reconciliation  *SubscriptionReconciliation
	ReuseConnection bool
}

type Session struct {
	policy      CadencePolicy
	channels    []string
	authorized  bool
	subscribeID uint64
}

func NewSession(policy CadencePolicy, requests []ChannelRequest) (*Session, error) {
	channels, err := Channels(policy, requests)
	if err != nil {
		return nil, err
	}
	return &Session{policy: policy, channels: channels}, nil
}

func (s *Session) Channels() []string {
	if s == nil {
		return nil
	}
	return slices.Clone(s.channels)
}

func (s *Session) AuthorizationRequest(id uint64, credentials ClientCredentials) ([]byte, error) {
	if s == nil || s.policy.Requested != CadenceRaw || id == 0 {
		return nil, ErrAuthorizationRequired
	}
	if err := credentials.validate(); err != nil {
		return nil, err
	}
	return encodeRPCRequest(id, "public/auth", struct {
		GrantType    string `json:"grant_type"`
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
	}{GrantType: "client_credentials", ClientID: credentials.ClientID, ClientSecret: string(credentials.ClientSecret)})
}

func (s *Session) AcceptAuthorizationResponse(payload []byte, requestID uint64) error {
	if s == nil || s.policy.Requested != CadenceRaw || requestID == 0 {
		return ErrAuthorizationRequired
	}
	var response struct {
		JSONRPC string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Result  struct {
			AccessToken string `json:"access_token"`
		} `json:"result"`
		Error *rpcError `json:"error"`
	}
	if err := decodeRPC(payload, &response); err != nil || response.JSONRPC != JSONRPCVersion {
		return ErrAuthorizationRequired
	}
	if response.Error != nil && response.Error.Code == CreditExhaustedCode {
		s.resetConnectionState()
		return ErrReconnectRequired
	}
	if response.ID != requestID || response.Error != nil || response.Result.AccessToken == "" {
		return ErrAuthorizationRequired
	}
	s.authorized = true
	return nil
}

func (s *Session) SubscribeRequest(id uint64) ([]byte, error) {
	if s == nil || id == 0 {
		return nil, ErrInvalidRPC
	}
	if s.policy.Requested == CadenceRaw && !s.authorized {
		return nil, ErrAuthorizationRequired
	}
	if s.subscribeID != 0 {
		return nil, ErrSubscriptionPending
	}
	method := "public/subscribe"
	payload, err := encodeRPCRequest(id, method, struct {
		Channels []string `json:"channels"`
	}{Channels: slices.Clone(s.channels)})
	if err == nil {
		s.subscribeID = id
	}
	return payload, err
}

func (s *Session) SetHeartbeatRequest(id uint64, intervalSeconds uint64) ([]byte, error) {
	if s == nil || id == 0 || intervalSeconds < 10 || intervalSeconds > 3600 {
		return nil, ErrInvalidRPC
	}
	return encodeRPCRequest(id, "public/set_heartbeat", struct {
		Interval uint64 `json:"interval"`
	}{Interval: intervalSeconds})
}

func (s *Session) Inspect(payload []byte, responseID uint64) (SessionDecision, error) {
	if s == nil {
		return SessionDecision{}, ErrInvalidRPC
	}
	var envelope struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      uint64          `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
		Result  json.RawMessage `json:"result"`
		Error   *rpcError       `json:"error"`
	}
	if err := decodeRPC(payload, &envelope); err != nil {
		return SessionDecision{}, ErrInvalidRPC
	}
	matchedSubscribe := s.subscribeID != 0 && envelope.ID == s.subscribeID
	if matchedSubscribe {
		s.subscribeID = 0
	}
	if envelope.JSONRPC != JSONRPCVersion {
		return SessionDecision{}, ErrInvalidRPC
	}
	if envelope.Error != nil {
		if envelope.Error.Code == CreditExhaustedCode {
			s.resetConnectionState()
			return SessionDecision{Action: SessionReconnectAfterCredit, ReuseConnection: false}, ErrReconnectRequired
		}
		return SessionDecision{}, fmt.Errorf("%w: remote code %d", ErrInvalidRPC, envelope.Error.Code)
	}
	if envelope.Method == "heartbeat" {
		var params struct {
			Type string `json:"type"`
		}
		if err := decodeRaw(envelope.Params, &params); err != nil {
			return SessionDecision{}, ErrInvalidRPC
		}
		if params.Type == "heartbeat" {
			return SessionDecision{Action: SessionContinue, ReuseConnection: true}, nil
		}
		if params.Type != "test_request" || responseID == 0 {
			return SessionDecision{}, ErrInvalidRPC
		}
		response, err := encodeRPCRequest(responseID, "public/test", struct{}{})
		if err != nil {
			return SessionDecision{}, err
		}
		return SessionDecision{Action: SessionRespondTest, Response: response, ReuseConnection: true}, nil
	}
	if matchedSubscribe {
		var returned []string
		if err := decodeRaw(envelope.Result, &returned); err != nil {
			return SessionDecision{}, ErrInvalidRPC
		}
		reconciliation, err := ReconcileSubscriptions(s.channels, returned)
		if err != nil {
			return SessionDecision{}, err
		}
		decision := SessionDecision{Action: SessionContinue, Reconciliation: &reconciliation, ReuseConnection: true}
		if !reconciliation.Exact {
			return decision, ErrSubscribeMismatch
		}
		return decision, nil
	}
	return SessionDecision{Action: SessionContinue, ReuseConnection: true}, nil
}

func (s *Session) resetConnectionState() {
	s.authorized = false
	s.subscribeID = 0
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func encodeRPCRequest(id uint64, method string, params any) ([]byte, error) {
	if id == 0 || method == "" {
		return nil, ErrInvalidRPC
	}
	payload, err := json.Marshal(struct {
		JSONRPC string `json:"jsonrpc"`
		ID      uint64 `json:"id"`
		Method  string `json:"method"`
		Params  any    `json:"params"`
	}{JSONRPC: JSONRPCVersion, ID: id, Method: method, Params: params})
	if err != nil {
		return nil, fmt.Errorf("%w: encode request", ErrInvalidRPC)
	}
	return payload, nil
}

func decodeRPC(payload []byte, destination any) error {
	if len(payload) == 0 || len(payload) > MaxRawPayloadBytes || !json.Valid(payload) {
		return ErrInvalidRPC
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return ErrInvalidRPC
	}
	if err := decoder.Decode(&struct{}{}); err == nil {
		return ErrInvalidRPC
	}
	return nil
}

func decodeRaw(raw json.RawMessage, destination any) error {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return ErrInvalidRPC
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(destination); err != nil {
		return ErrInvalidRPC
	}
	return nil
}
