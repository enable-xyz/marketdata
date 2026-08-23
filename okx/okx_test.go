package okx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/orderbook"
)

func TestOKXTradesAllSingleMatchAndAggregateRejected(t *testing.T) {
	payload := []byte(`{"arg":{"channel":"trades-all","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"42","px":"65000.1","sz":"0.25","side":"buy","ts":"1787443200000","count":"1"}]}`)
	trades, err := ParseTradesAll(payload)
	if err != nil || len(trades) != 1 || trades[0].Source != "trades-all" || trades[0].Count.Text != "1" || string(trades[0].Raw) != string(payload) {
		t.Fatalf("ParseTradesAll() = %#v, %v", trades, err)
	}
	aggregate := []byte(`{"arg":{"channel":"trades-all","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"42","px":"65000.1","sz":"0.25","side":"buy","ts":"1787443200000","count":"2"}]}`)
	if _, err := ParseTradesAll(aggregate); !errors.Is(err, ErrAmbiguousProjection) {
		t.Fatalf("aggregate error = %v, want ErrAmbiguousProjection", err)
	}
	metadata := okxMetadata(t, normalize.TradeSchemaName, normalize.TradeSchemaVersion, "okx-btc-usdt", payload)
	observed := okxValue("1", metadata.SourceEventTimeNS.Value)
	event, err := normalize.MapOKXTrade(metadata, normalize.OKXTradeInput{TradeID: "42", Side: "buy", Price: okxValue("65000.1", metadata.SourceEventTimeNS.Value), Amount: okxValue("0.25", metadata.SourceEventTimeNS.Value), PriceUnit: normalize.SpotPriceUnit("BTC", "USDT"), AmountUnit: normalize.BaseAssetUnit("BTC"), MakerMatchCount: observed})
	if err != nil || event.AggregationKind != normalize.AggregationSingleMatch || event.NativeTradeID != 42 {
		t.Fatalf("MapOKXTrade() = %#v, %v", event, err)
	}
}

func TestOKXExactSubscribeAcknowledgementReconnect(t *testing.T) {
	args := []SubscriptionArg{{Channel: "books", InstrumentID: "BTC-USDT"}, {Channel: "tickers", InstrumentID: "ETH-USDT"}}
	session, err := NewSubscriptionSession(PublicSocket, Entitlement{}, args)
	if err != nil {
		t.Fatal(err)
	}
	messages, err := session.Messages()
	if err != nil || len(messages) != 2 {
		t.Fatalf("Messages() count = %d, %v", len(messages), err)
	}
	ack := []byte(`{"event":"subscribe","arg":{"channel":"books","instId":"BTC-USDT"},"connId":"connection-1","code":"","msg":""}`)
	if _, err := session.Acknowledge(ack); err != nil || session.Pending() != 1 {
		t.Fatalf("Acknowledge() pending = %d, err = %v", session.Pending(), err)
	}
	unexpected := []byte(`{"event":"subscribe","arg":{"channel":"books","instId":"ETH-USDT"},"connId":"connection-1","code":"","msg":""}`)
	if _, err := session.Acknowledge(unexpected); !errors.Is(err, ErrUnexpectedACK) {
		t.Fatalf("unexpected ack error = %v", err)
	}
	wrongConnection := []byte(`{"event":"subscribe","arg":{"channel":"tickers","instId":"ETH-USDT"},"connId":"connection-2","code":"","msg":""}`)
	if _, err := session.Acknowledge(wrongConnection); !errors.Is(err, ErrUnexpectedACK) || session.Pending() != 1 {
		t.Fatalf("cross-connection ack pending = %d, error = %v", session.Pending(), err)
	}
	reconnect, err := session.ReconnectMessages()
	if err != nil || len(reconnect) != len(args) || session.Pending() != len(args) || session.Generation() != 2 {
		t.Fatalf("ReconnectMessages() count=%d pending=%d generation=%d err=%v", len(reconnect), session.Pending(), session.Generation(), err)
	}
	var got []SubscriptionArg
	for _, message := range reconnect {
		var request SubscribeRequest
		if err := json.Unmarshal(message, &request); err != nil || len(request.Arguments) != 1 {
			t.Fatalf("reconnect message = %s, %v", message, err)
		}
		got = append(got, request.Arguments[0])
	}
	slices.SortFunc(got, func(a, b SubscriptionArg) int { return compareString(a.identity(), b.identity()) })
	slices.SortFunc(args, func(a, b SubscriptionArg) int { return compareString(a.identity(), b.identity()) })
	if !slices.Equal(got, args) {
		t.Fatalf("reconnect inventory = %#v, want %#v", got, args)
	}
}

func TestOKXPublicSubscribeAcknowledgementAcceptsCurrentOptionalFields(t *testing.T) {
	arg := SubscriptionArg{Channel: "trades", InstrumentID: "BTC-USDT"}

	t.Run("omitted connection and request IDs", func(t *testing.T) {
		session, err := NewSubscriptionSession(PublicSocket, Entitlement{}, []SubscriptionArg{arg})
		if err != nil {
			t.Fatal(err)
		}
		payload := []byte(`{"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"}}`)
		parsed, err := session.Acknowledge(payload)
		if err != nil || session.Pending() != 0 || parsed.ID != "" || parsed.ConnectionID != "" || parsed.Argument.identity() != arg.identity() {
			t.Fatalf("Acknowledge() = %#v, pending=%d, err=%v", parsed, session.Pending(), err)
		}
	})

	t.Run("echoed matching request and connection IDs", func(t *testing.T) {
		session, err := NewSubscriptionSession(PublicSocket, Entitlement{}, []SubscriptionArg{arg})
		if err != nil {
			t.Fatal(err)
		}
		messages, err := session.Messages()
		if err != nil || len(messages) != 1 {
			t.Fatalf("Messages() = %d, %v", len(messages), err)
		}
		var request SubscribeRequest
		if err := json.Unmarshal(messages[0], &request); err != nil {
			t.Fatalf("decode request: %v", err)
		}
		payload := []byte(fmt.Sprintf(`{"id":%q,"event":"subscribe","arg":{"channel":"trades","instId":"BTC-USDT"},"connId":"f35b84e5"}`, request.ID))
		parsed, err := session.Acknowledge(payload)
		if err != nil || session.Pending() != 0 || parsed.ID != request.ID || parsed.ConnectionID != "f35b84e5" || parsed.Argument.identity() != arg.identity() {
			t.Fatalf("Acknowledge() = %#v, pending=%d, err=%v", parsed, session.Pending(), err)
		}
	})
}

func TestOKXEchoedSubscribeRequestIDMustMatchPendingArgument(t *testing.T) {
	args := []SubscriptionArg{
		{Channel: "books", InstrumentID: "BTC-USDT"},
		{Channel: "tickers", InstrumentID: "ETH-USDT"},
	}
	session, err := NewSubscriptionSession(PublicSocket, Entitlement{}, args)
	if err != nil {
		t.Fatal(err)
	}
	requestIDs := func(messages [][]byte) map[string]string {
		t.Helper()
		ids := make(map[string]string, len(messages))
		for _, message := range messages {
			var request SubscribeRequest
			if err := json.Unmarshal(message, &request); err != nil || len(request.Arguments) != 1 {
				t.Fatalf("decode request %s: %v", message, err)
			}
			ids[request.Arguments[0].identity()] = request.ID
		}
		return ids
	}
	ackPayload := func(id string, arg SubscriptionArg) []byte {
		t.Helper()
		payload, err := json.Marshal(SubscriptionACK{ID: id, Event: "subscribe", Argument: arg})
		if err != nil {
			t.Fatal(err)
		}
		return payload
	}
	assertRejectedWithoutMutation := func(name, id string, arg SubscriptionArg) {
		t.Helper()
		before := session.Pending()
		if _, err := session.Acknowledge(ackPayload(id, arg)); !errors.Is(err, ErrUnexpectedACK) || session.Pending() != before {
			t.Fatalf("%s ACK pending=%d, want %d; error=%v", name, session.Pending(), before, err)
		}
	}

	messages, err := session.Messages()
	if err != nil || len(messages) != len(args) {
		t.Fatalf("Messages() = %d, %v", len(messages), err)
	}
	initialIDs := requestIDs(messages)
	repeated, err := session.Messages()
	if err != nil || len(repeated) != len(args) {
		t.Fatalf("repeated Messages() = %d, %v", len(repeated), err)
	}
	repeatedIDs := requestIDs(repeated)
	for identity, id := range initialIDs {
		if repeatedIDs[identity] != id {
			t.Fatalf("request ID for %q changed from %q to %q within one generation", identity, id, repeatedIDs[identity])
		}
	}

	assertRejectedWithoutMutation("mismatched", initialIDs[args[0].identity()], args[1])
	assertRejectedWithoutMutation("unknown", "okx-unknown-request", args[0])

	reconnected, err := session.ReconnectMessages()
	if err != nil || len(reconnected) != len(args) || session.Pending() != len(args) {
		t.Fatalf("ReconnectMessages() = %d, pending=%d, err=%v", len(reconnected), session.Pending(), err)
	}
	currentIDs := requestIDs(reconnected)
	for _, arg := range args {
		if currentIDs[arg.identity()] == initialIDs[arg.identity()] {
			t.Fatalf("reconnect retained stale request ID %q for %q", currentIDs[arg.identity()], arg.identity())
		}
	}
	assertRejectedWithoutMutation("stale", initialIDs[args[0].identity()], args[0])

	matching := ackPayload(currentIDs[args[0].identity()], args[0])
	if _, err := session.Acknowledge(matching); err != nil || session.Pending() != 1 {
		t.Fatalf("matching ACK pending=%d, error=%v", session.Pending(), err)
	}
	if _, err := session.Acknowledge(matching); !errors.Is(err, ErrUnexpectedACK) || session.Pending() != 1 {
		t.Fatalf("duplicate ACK pending=%d, error=%v", session.Pending(), err)
	}
}

func TestOKXVIPDenialIsTerminalAndNeverDowngrades(t *testing.T) {
	vip := SubscriptionArg{Channel: "books-l2-tbt", InstrumentID: "BTC-USDT"}
	if _, err := NewSubscriptionSession(PublicSocket, Entitlement{}, []SubscriptionArg{vip}); !errors.Is(err, ErrVIPEntitlement) {
		t.Fatalf("missing entitlement error = %v", err)
	}
	initialEntitlement := Entitlement{LoggedIn: true, VIPLevel: 4, LoginEvidence: `{"event":"login","code":"0","msg":"","connId":"connection-1"}`, SourceIdentity: "caller-vip-source"}
	session, err := NewSubscriptionSession(PublicSocket, initialEntitlement, []SubscriptionArg{vip})
	if err != nil {
		t.Fatal(err)
	}
	nonmatching := []byte(`{"event":"error","arg":{"channel":"books-l2-tbt","instId":"ETH-USDT"},"connId":"connection-1","code":"64003","msg":"VIP level is lower than VIP4"}`)
	if _, err := session.Acknowledge(nonmatching); !errors.Is(err, ErrUnexpectedACK) || session.Pending() != 1 || len(session.TerminalDenials()) != 0 {
		t.Fatalf("nonmatching terminal denial mutated inventory: pending=%d denials=%#v err=%v", session.Pending(), session.TerminalDenials(), err)
	}
	denialPayload := []byte(`{"event":"error","arg":{"channel":"books-l2-tbt","instId":"BTC-USDT"},"connId":"connection-1","code":"64003","msg":"VIP level is lower than VIP4"}`)
	ack, err := session.Acknowledge(denialPayload)
	var rejection *SubscriptionRejection
	denials := session.TerminalDenials()
	if !errors.As(err, &rejection) || !rejection.Terminal || rejection.Code != "64003" || ack.Code != "64003" || session.Pending() != 0 ||
		len(denials) != 1 || string(denials[0].Raw) != string(denialPayload) {
		t.Fatalf("VIP rejection = %#v, denials=%#v, pending=%d, err=%v", rejection, denials, session.Pending(), err)
	}
	reconnect, err := session.ReconnectMessages()
	if err != nil || len(reconnect) != 0 || len(session.TerminalDenials()) != 1 {
		t.Fatalf("terminal denial reconnect = %d, %v", len(reconnect), err)
	}
	if err := session.RenewDeniedSubscription(initialEntitlement, vip); !errors.Is(err, ErrVIPEntitlement) {
		t.Fatalf("unchanged entitlement renewed denial = %v", err)
	}
	sameConnection := initialEntitlement
	sameConnection.LoginEvidence = `{ "event":"login", "code":"0", "msg":"", "connId":"connection-1" }`
	if err := session.RenewDeniedSubscription(sameConnection, vip); !errors.Is(err, ErrVIPEntitlement) {
		t.Fatalf("same denied connection renewed denial = %v", err)
	}
	renewed := initialEntitlement
	renewed.LoginEvidence = `{"event":"login","code":"0","msg":"","connId":"connection-2"}`
	if err := session.RenewDeniedSubscription(renewed, vip); err != nil || session.Pending() != 1 || len(session.TerminalDenials()) != 0 {
		t.Fatalf("explicit renewed entitlement = pending %d denials=%#v err=%v", session.Pending(), session.TerminalDenials(), err)
	}
	if messages, err := session.Messages(); err != nil || len(messages) != 1 {
		t.Fatalf("renewed subscription messages = %d, %v", len(messages), err)
	}
}

func TestOKXVIPSessionWritesAndDualBudgets(t *testing.T) {
	clock, err := capture.NewManualClock(1787443200000000000, "okx-budget-clock")
	if err != nil {
		t.Fatal(err)
	}
	handshakes, err := NewHandshakeBudget(clock.Read().MonotonicNS)
	if err != nil {
		t.Fatal(err)
	}
	for index := range 3 {
		decision, err := handshakes.Acquire(clock.Read().MonotonicNS, HandshakeRatePolicy().ConnectionCost)
		if err != nil || !decision.Allowed {
			t.Fatalf("handshake %d = %#v, %v", index, decision, err)
		}
	}
	if decision, err := handshakes.Acquire(clock.Read().MonotonicNS, HandshakeRatePolicy().ConnectionCost); err != nil || decision.Allowed {
		t.Fatalf("fourth handshake = %#v, %v", decision, err)
	}
	transport := &okxTestRoundTripper{}
	client := &http.Client{Transport: transport, Timeout: time.Second}
	if socket, err := DialSocket(context.Background(), SocketConfig{Kind: PublicSocket, Maximum: MaxRawPayloadBytes, Entitlement: Entitlement{VIPLevel: 4, SourceIdentity: "caller-vip-source"}}, client, clock, handshakes); !errors.Is(err, ErrVIPEntitlement) || socket != nil || transport.calls != 0 {
		t.Fatalf("VIP dial without runtime authenticator = socket=%#v err=%v calls=%d", socket, err, transport.calls)
	}
	if socket, err := DialSocket(context.Background(), SocketConfig{Kind: PublicSocket, Maximum: MaxRawPayloadBytes}, client, clock, handshakes); !errors.Is(err, ErrRateLimited) || socket != nil || transport.calls != 0 {
		t.Fatalf("denied handshake performed I/O: socket=%#v err=%v calls=%d", socket, err, transport.calls)
	}
	if err := clock.Advance(uint64(time.Second)); err != nil {
		t.Fatal(err)
	}
	if decision, err := handshakes.Acquire(clock.Read().MonotonicNS, HandshakeRatePolicy().ConnectionCost); err != nil || !decision.Allowed {
		t.Fatalf("refilled handshake = %#v, %v", decision, err)
	}

	operations, err := NewOperationBudget(clock.Read().MonotonicNS)
	if err != nil {
		t.Fatal(err)
	}
	requested := Entitlement{VIPLevel: 4, SourceIdentity: "caller-vip-source"}
	loginEvidence := []byte(`{"event":"login","code":"0","msg":"","connId":"synthetic-login-connection"}`)
	connection := &okxTestConnection{reads: [][]byte{loginEvidence}}
	authenticator := AuthenticatorFunc(func(ctx context.Context, live LoginConnection) ([]byte, error) {
		if err := live.WriteLogin(ctx, []byte(`{"op":"login","args":[{"proof":"synthetic-caller-owned"}]}`)); err != nil {
			return nil, err
		}
		return live.ReadLoginACK(ctx)
	})
	forgedOperations, err := NewOperationBudget(clock.Read().MonotonicNS)
	if err != nil {
		t.Fatal(err)
	}
	forgedConnection := &okxTestConnection{}
	forgedAuthenticator := AuthenticatorFunc(func(context.Context, LoginConnection) ([]byte, error) {
		return slices.Clone(loginEvidence), nil
	})
	if _, _, _, err := authenticateVIPSocket(context.Background(), forgedConnection, MaxRawPayloadBytes, clock, forgedOperations, requested, forgedAuthenticator); !errors.Is(err, ErrVIPEntitlement) || forgedConnection.writes != 0 {
		t.Fatalf("forged login ACK without live proof = %v, writes=%d", err, forgedConnection.writes)
	}
	badOperations, err := NewOperationBudget(clock.Read().MonotonicNS)
	if err != nil {
		t.Fatal(err)
	}
	badConnection := &okxTestConnection{reads: [][]byte{[]byte(`{"event":"login","code":"1","msg":"denied","connId":"synthetic-login-connection"}`)}}
	if _, _, _, err := authenticateVIPSocket(context.Background(), badConnection, MaxRawPayloadBytes, clock, badOperations, requested, authenticator); !errors.Is(err, ErrVIPEntitlement) {
		t.Fatalf("invalid login ACK authentication = %v", err)
	}
	authenticated, rawLoginACK, loginConnectionID, err := authenticateVIPSocket(context.Background(), connection, MaxRawPayloadBytes, clock, operations, requested, authenticator)
	if err != nil || !authenticated.permitsVIP4() || loginConnectionID != "synthetic-login-connection" || string(rawLoginACK) != string(loginEvidence) {
		t.Fatalf("authenticateVIPSocket() = %#v, %s, %q, %v", authenticated, rawLoginACK, loginConnectionID, err)
	}
	socket := &Socket{kind: PublicSocket, endpoint: PublicWebSocketEndpoint, maximum: MaxRawPayloadBytes, clock: clock, operations: operations, conn: connection, entitlement: authenticated, authenticated: true, loginACK: rawLoginACK, loginConnectionID: loginConnectionID}
	vip := SubscriptionArg{Channel: "books-l2-tbt", InstrumentID: "BTC-USDT"}
	session, err := NewSubscriptionSession(PublicSocket, socket.Entitlement(), []SubscriptionArg{vip})
	if err != nil {
		t.Fatal(err)
	}
	messages, err := session.Messages()
	if err != nil || len(messages) != 1 {
		t.Fatalf("VIP Messages() = %d, %v", len(messages), err)
	}
	for index := range 479 {
		if err := socket.WriteSubscription(context.Background(), session, messages[0]); err != nil {
			t.Fatalf("subscription write %d = %v", index, err)
		}
	}
	if session.connectionID != loginConnectionID {
		t.Fatalf("VIP session connection binding = %q, want %q", session.connectionID, loginConnectionID)
	}
	if err := socket.WriteSubscription(context.Background(), session, messages[0]); !errors.Is(err, ErrRateLimited) || connection.writes != 480 {
		t.Fatalf("exhausted subscription write = %v, physical writes=%d", err, connection.writes)
	}
	if err := clock.Advance(uint64(time.Hour)); err != nil {
		t.Fatal(err)
	}
	reconnect, err := session.ReconnectMessages()
	if err != nil || len(reconnect) != 1 {
		t.Fatalf("ReconnectMessages() = %d, %v", len(reconnect), err)
	}
	if err := socket.WriteSubscription(context.Background(), session, reconnect[0]); err != nil || connection.writes != 481 {
		t.Fatalf("reconnect subscription write = %v, writes=%d", err, connection.writes)
	}
}

func TestOKXPublicBusinessAndRESTContracts(t *testing.T) {
	for _, kind := range []SocketKind{PublicSocket, BusinessSocket} {
		contract, err := PublicSourceContract(kind)
		if err != nil || contract.Subscription.ACKMode != capture.ACKExact || contract.Topology.MaxSubscriptionsPerACK != 1 {
			t.Fatalf("PublicSourceContract(%s) = %#v, %v", kind, contract, err)
		}
	}
	rest, err := RESTSourceContract()
	if err != nil || rest.Topology.Transport != capture.TransportREST {
		t.Fatalf("RESTSourceContract() = %#v, %v", rest, err)
	}
	native := NativeFileContract()
	if !native.ManifestOnly || native.NativeHistoryImport || native.MissingOrEmptyMeansZero || native.PublicationLagDays != [2]uint8{2, 3} {
		t.Fatalf("NativeFileContract() = %#v", native)
	}
	if err := (RESTRequest{Endpoint: InstrumentPath, Query: url.Values{"instType": {"SWAP"}}}).validate(); err != nil {
		t.Fatal(err)
	}
	if err := (RESTRequest{Endpoint: "https://attacker.invalid", Query: url.Values{"instType": {"SWAP"}}}).validate(); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("payload-selected endpoint error = %v", err)
	}
	if err := (SubscriptionArg{Channel: "open-interest", InstrumentType: string(Swap), InstrumentID: "BTC-USDT-SWAP"}).Validate(PublicSocket, Entitlement{}); err != nil {
		t.Fatalf("documented open-interest identity = %v", err)
	}
	if err := (SubscriptionArg{Channel: "open-interest", InstrumentID: "BTC-USDT-SWAP"}).Validate(PublicSocket, Entitlement{}); !errors.Is(err, ErrInvalidSubscription) {
		t.Fatalf("open-interest without derivative instType = %v", err)
	}
	if HandshakeRatePolicy().Capacity != 3 || HandshakeRatePolicy().RefillIntervalNS != uint64(time.Second) || OperationRatePolicy().Capacity != 480 || OperationRatePolicy().RefillIntervalNS != uint64(time.Hour) {
		t.Fatalf("OKX dual rate policies are not exact")
	}
}

func TestOKXBaselineBookParserAndNormalizedChecksumCutover(t *testing.T) {
	payload := []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["65001","1","0","1"]],"bids":[["65000","2","0","2"]],"ts":"1787443200000","checksum":0,"prevSeqId":-1,"seqId":100}]}`)
	message, err := ParseBook(payload)
	if err != nil || message.Channel != "books" || message.PreviousSeq != -1 || message.Sequence != 100 || len(message.Bids) != 1 {
		t.Fatalf("ParseBook() = %#v, %v", message, err)
	}
	update := message.ReconstructionUpdate(1787443201000000000)
	if update.Bids[0].OrderCount != "2" || update.Checksum != 0 || update.SourceTimeNS != message.TimestampMS*1_000_000 {
		t.Fatalf("ReconstructionUpdate() = %#v", update)
	}
	metadata := okxMetadata(t, normalize.BookUpdateSchemaName, normalize.BookUpdateSchemaVersion, "okx-btc-usdt", payload)
	normalized, err := message.Normalized(metadata, normalize.SpotPriceUnit("BTC", "USDT"), normalize.BaseAssetUnit("BTC"))
	if err != nil || !normalized.Reconstructable || normalized.ChecksumState != normalize.SourceMissing || normalized.RawChecksum != 0 || normalized.CadenceContract != "100ms" {
		t.Fatalf("BookMessage.Normalized() = %#v, %v", normalized, err)
	}
}

func TestOKXBookSourceTimestampEntitledAndReplacementContracts(t *testing.T) {
	tests := []struct {
		name                string
		payload             []byte
		checksumState       normalize.SourceState
		reconstructable     bool
		snapshotReplacement bool
		rpiIncluded         bool
		liquidityKind       normalize.OKXBookLiquidityKind
	}{
		{name: "delayed_pre_cutover", payload: []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[],"bids":[],"ts":"1782172799999","checksum":123,"prevSeqId":-1,"seqId":1}]}`), checksumState: normalize.SourceValue, reconstructable: true, liquidityKind: normalize.OKXBookRegularLiquidity},
		{name: "entitled_incremental", payload: []byte(`{"arg":{"channel":"books-l2-tbt","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[],"bids":[],"ts":"1787443200000","checksum":0,"prevSeqId":-1,"seqId":1}]}`), checksumState: normalize.SourceMissing, reconstructable: true, liquidityKind: normalize.OKXBookRegularLiquidity},
		{name: "books5_replacement", payload: []byte(`{"arg":{"channel":"books5","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[],"bids":[],"ts":"1787443200000","checksum":0,"prevSeqId":-1,"seqId":1}]}`), checksumState: normalize.SourceMissing, snapshotReplacement: true, liquidityKind: normalize.OKXBookRegularLiquidity},
		{name: "rpi_separate_reconstructable", payload: []byte(`{"arg":{"channel":"books-rpi-tbt","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[],"bids":[],"ts":"1787443200000","checksum":0,"prevSeqId":-1,"seqId":1}]}`), checksumState: normalize.SourceMissing, reconstructable: true, rpiIncluded: true, liquidityKind: normalize.OKXBookRPILiquidity},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err := ParseBook(test.payload)
			if err != nil {
				t.Fatal(err)
			}
			metadata := okxMetadata(t, normalize.BookUpdateSchemaName, normalize.BookUpdateSchemaVersion, "okx-btc-usdt", test.payload)
			projection, err := message.Normalized(metadata, normalize.SpotPriceUnit("BTC", "USDT"), normalize.BaseAssetUnit("BTC"))
			if err != nil || projection.SourceTimeNS != message.TimestampMS*1_000_000 || projection.ChecksumState != test.checksumState ||
				projection.Reconstructable != test.reconstructable || projection.SnapshotReplacement != test.snapshotReplacement || projection.RPIIncluded != test.rpiIncluded || projection.LiquidityKind != test.liquidityKind {
				t.Fatalf("projection = %#v, %v", projection, err)
			}
		})
	}
}

func TestOKXSpotTickerPreservesMissingEmptyAndZero(t *testing.T) {
	const sourceTimeNS = int64(1787443200000000000)
	metadata := okxMetadata(t, normalize.TickerSchemaName, normalize.TickerSchemaVersion, "okx-btc-usdt", []byte(`{"channel":"tickers"}`))
	empty := normalize.OKXField{State: normalize.SourceEmpty, SourceTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond}
	ticker, err := normalize.MapOKXSpotTicker(metadata, normalize.OKXSpotTickerInput{LastPrice: okxValue("65000", sourceTimeNS), BidPrice: empty, BidAmount: normalize.OKXField{State: normalize.SourceMissing}, AskPrice: okxValue("65001", sourceTimeNS), AskAmount: okxValue("0", sourceTimeNS), Open24H: okxValue("64000", sourceTimeNS), High24H: okxValue("66000", sourceTimeNS), Low24H: okxValue("63000", sourceTimeNS), BaseVolume24H: okxValue("0", sourceTimeNS), QuoteVolume24H: okxValue("1000", sourceTimeNS), PriceUnit: normalize.SpotPriceUnit("BTC", "USDT"), BaseUnit: normalize.BaseAssetUnit("BTC"), QuoteUnit: normalize.QuoteAssetUnit("USDT")})
	if err != nil || ticker.BidPrice.State != normalize.SourceEmpty || ticker.BidAmount.State != normalize.SourceMissing || ticker.AskAmount.State != normalize.SourceValue || !ticker.AskAmount.Value.Decimal.IsZero() || !ticker.BaseVolume24H.Value.Decimal.IsZero() {
		t.Fatalf("MapOKXSpotTicker() = %#v, %v", ticker, err)
	}
}

func TestOKXDerivativeOptionAndLifecycleMappings(t *testing.T) {
	const sourceTimeNS = int64(1787443200000000000)
	missing := normalize.OKXField{State: normalize.SourceMissing}
	derivativeMetadata := okxMetadata(t, normalize.DerivativeTickerSchemaName, normalize.DerivativeTickerSchemaVersion, "okx-btc-usdt-swap", []byte(`{"channel":"tickers"}`))
	derivative, err := normalize.MapOKXDerivativeTicker(derivativeMetadata, normalize.OKXDerivativeInput{NativeSourceRole: "okx_v5_tickers_state", LastPrice: okxValue("65000", sourceTimeNS), MarkPrice: okxValue("64999", sourceTimeNS), IndexPrice: okxValue("65001", sourceTimeNS), FundingRate: okxValue("0.0001", sourceTimeNS), NextFundingTime: okxValue("1787446800000", sourceTimeNS), OpenInterest: okxValue("1200", sourceTimeNS), SettlementPrice: missing, Basis: missing, Premium: missing, PriceUnit: normalize.SpotPriceUnit("BTC", "USDT"), OpenInterestUnit: normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: "okx-btc-usdt-swap"}, OISidedness: normalize.OpenInterestUnspecified})
	if err != nil || len(derivative.OpenInterest) != 1 || derivative.OpenInterest[0].Native.Unit.Kind != normalize.NativeUnitContracts {
		t.Fatalf("MapOKXDerivativeTicker() = %#v, %v", derivative, err)
	}
	optionMetadata := okxMetadata(t, normalize.OptionSummarySchemaName, normalize.OptionSummarySchemaVersion, "BTC-USD-260925-65000-C", []byte(`{"channel":"opt-summary"}`))
	venueGreek := normalize.NativeUnit{Kind: normalize.NativeUnitVenueUnspecified, VenueLabel: "okx_native_greek"}
	contracts := normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: "BTC-USD-260925-65000-C"}
	option, err := normalize.MapOKXOptionSummary(optionMetadata, normalize.OKXOptionInput{NativeSourceRole: "okx_v5_opt_summary", Instrument: okxValue("BTC-USD-260925-65000-C", sourceTimeNS), Underlying: okxValue("BTC-USD", sourceTimeNS), Index: okxValue("BTC-USD", sourceTimeNS), Expiry: okxValue("1790294400000", sourceTimeNS), Strike: okxValue("65000", sourceTimeNS), CallPut: okxValue("C", sourceTimeNS), BidPrice: okxValue("0.05", sourceTimeNS), AskPrice: okxValue("0.06", sourceTimeNS), LastPrice: missing, MarkPrice: okxValue("0.055", sourceTimeNS), BidIV: okxValue("0.42", sourceTimeNS), AskIV: okxValue("0.43", sourceTimeNS), MarkIV: okxValue("0.425", sourceTimeNS), Delta: okxValue("0.5", sourceTimeNS), Gamma: missing, Vega: missing, Theta: missing, Rho: missing, OpenInterest: okxValue("100", sourceTimeNS), Volume: okxValue("12", sourceTimeNS), ForwardPrice: missing, UnderlyingPrice: okxValue("65000", sourceTimeNS), IndexPrice: okxValue("65001", sourceTimeNS), PriceUnit: normalize.SpotPriceUnit("BTC", "USD"), GreekUnit: venueGreek, OpenInterestUnit: contracts, VolumeUnit: contracts})
	if err != nil || option.CallPut.Value != normalize.OptionCall || option.Gamma.State != normalize.SourceMissing {
		t.Fatalf("MapOKXOptionSummary() = %#v, %v", option, err)
	}
	if state, err := normalize.OKXInstrumentState("live"); err != nil || state != normalize.InstrumentStateContinuousTrading {
		t.Fatalf("OKXInstrumentState(live) = %q, %v", state, err)
	}
	if _, err := normalize.OKXInstrumentState("mystery"); !errors.Is(err, normalize.ErrInvalidOKXProjection) {
		t.Fatalf("unknown state error = %v", err)
	}
	lifecycleMetadata := okxMetadata(t, normalize.InstrumentEventSchemaName, normalize.InstrumentEventSchemaVersion, "okx-btc-usdt-swap", []byte(`{"state":"live"}`))
	provenance := normalize.FieldProvenance{SourceTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond, AgeNS: normalize.OptionalUint64{Value: uint64(lifecycleMetadata.ReceivedTimeNS - sourceTimeNS), Valid: true}}
	missingProvenance := normalize.FieldProvenance{SourceTimeResolution: normalize.ResolutionAbsent}
	lifecycle, err := normalize.MapOKXLifecycle(lifecycleMetadata, normalize.OKXLifecycleInput{MetadataGeneration: normalize.Uint64Field{State: normalize.SourceValue, Value: 7, Provenance: provenance}, NativeStateBefore: normalize.InstrumentStateField{State: normalize.SourceMissing, Provenance: missingProvenance}, NativeStateAfter: normalize.InstrumentStateField{State: normalize.SourceValue, Value: normalize.InstrumentStateContinuousTrading, Provenance: provenance}, ListingTime: normalize.TimeField{State: normalize.SourceValue, ValueNS: sourceTimeNS, Resolution: normalize.ResolutionMillisecond, Provenance: provenance}, ContinuousTime: normalize.TimeField{State: normalize.SourceMissing, Resolution: normalize.ResolutionAbsent, Provenance: missingProvenance}, ExpiryTime: normalize.TimeField{State: normalize.SourceMissing, Resolution: normalize.ResolutionAbsent, Provenance: missingProvenance}, DeliveryTime: normalize.TimeField{State: normalize.SourceMissing, Resolution: normalize.ResolutionAbsent, Provenance: missingProvenance}, DelistingTime: normalize.TimeField{State: normalize.SourceMissing, Resolution: normalize.ResolutionAbsent, Provenance: missingProvenance}, TickSize: normalize.NumericChange{Old: normalize.NumericField{State: normalize.SourceMissing, Provenance: missingProvenance}, New: normalize.NumericField{State: normalize.SourceMissing, Provenance: missingProvenance}}, LotSize: normalize.NativeNumericChange{Old: normalize.NativeNumericField{State: normalize.SourceMissing, Provenance: missingProvenance}, New: normalize.NativeNumericField{State: normalize.SourceMissing, Provenance: missingProvenance}}, ContractMultiplier: normalize.NativeNumericChange{Old: normalize.NativeNumericField{State: normalize.SourceMissing, Provenance: missingProvenance}, New: normalize.NativeNumericField{State: normalize.SourceMissing, Provenance: missingProvenance}}, Payoff: normalize.TextChange{Old: normalize.TextField{State: normalize.SourceMissing, Provenance: missingProvenance}, New: normalize.TextField{State: normalize.SourceMissing, Provenance: missingProvenance}}, OldRawHash: normalize.HashField{State: normalize.SourceMissing, Provenance: missingProvenance}, NewRawHash: normalize.HashField{State: normalize.SourceValue, Value: normalize.Hash(sha256.Sum256([]byte("new-okx-metadata"))), Provenance: provenance}, ResolutionStatus: normalize.InstrumentResolutionField{State: normalize.SourceValue, Value: normalize.InstrumentResolved, Provenance: provenance}})
	if err != nil || lifecycle.NativeStateAfter.Value != normalize.InstrumentStateContinuousTrading {
		t.Fatalf("MapOKXLifecycle() = %#v, %v", lifecycle, err)
	}
}

func TestOKXLiquidationIncompleteEvidenceAndSourceOrder(t *testing.T) {
	payload := []byte(`{"arg":{"channel":"liquidation-orders"},"data":[{"instType":"SWAP","instFamily":"BTC-USDT","instId":"BTC-USDT-SWAP","uly":"BTC-USDT","details":[{"side":"sell","posSide":"long","bkPx":"64000","sz":"2","bkLoss":"10","ccy":"USDT","ts":"1787443200000"},{"side":"buy","posSide":"short","bkPx":"66000","sz":"1","bkLoss":"5","ccy":"USDT","ts":"1787443200001"}]}]}`)
	batches, err := ParseLiquidations(payload)
	if err != nil || len(batches) != 1 || len(batches[0].Details) != 2 || batches[0].Details[0].Side.Text != "sell" || batches[0].Details[1].Side.Text != "buy" {
		t.Fatalf("ParseLiquidations() = %#v, %v", batches, err)
	}
	metadata := okxMetadata(t, normalize.LiquidationSchemaName, normalize.LiquidationSchemaVersion, "okx-btc-usdt-swap", payload)
	mapped, err := normalize.MapOKXLiquidation(metadata, normalize.OKXLiquidationInput{Side: okxValue("sell", metadata.SourceEventTimeNS.Value), BankruptcyPrice: okxValue("64000", metadata.SourceEventTimeNS.Value), Amount: okxValue("2", metadata.SourceEventTimeNS.Value), PriceUnit: normalize.SpotPriceUnit("BTC", "USDT"), AmountUnit: normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: "okx-btc-usdt-swap"}, BatchID: "arrival-1-detail-0"})
	if err != nil || mapped.Completeness != normalize.LiquidationPartialNonchronological || mapped.Window.Selection != normalize.LiquidationWindowSelectionUnknown {
		t.Fatalf("MapOKXLiquidation() = %#v, %v", mapped, err)
	}
	incomplete := normalize.OKXLiquidationInput{Side: okxValue("sell", metadata.SourceEventTimeNS.Value), BankruptcyPrice: okxValue("64000", metadata.SourceEventTimeNS.Value), Amount: normalize.OKXField{State: normalize.SourceMissing}, PriceUnit: normalize.SpotPriceUnit("BTC", "USDT"), AmountUnit: normalize.NativeUnit{Kind: normalize.NativeUnitContracts, InstrumentUID: "okx-btc-usdt-swap"}, BatchID: "arrival-1-detail-1"}
	if _, err := normalize.MapOKXLiquidation(metadata, incomplete); !errors.Is(err, normalize.ErrInvalidOKXProjection) {
		t.Fatalf("incomplete liquidation error = %v", err)
	}
}

func TestOKXNativeManifestT2T3PublicationGapsAndEmptyFidelity(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "native"), 0o700); err != nil {
		t.Fatal(err)
	}
	published := []byte("synthetic-native-file\n")
	if err := os.WriteFile(filepath.Join(root, "native", "published.csv"), published, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "native", "empty.csv"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	publishedDigest := sha256.Sum256(published)
	start, end := int64(1787184000000000000), int64(1787270399999999999)
	manifest := NativeFileManifest{Version: NativeManifestVersion, Venue: "okx-v5", AccessDate: DocumentationAccessDate, Entries: []NativeFileEntry{
		{Module: "trades", Instrument: "BTC-USDT", MarketDate: "2026-08-20", PublicationLagDays: 2, ExpectedPublicationDate: "2026-08-22", ObservedAtNS: 1787443200000000000, State: NativeFilePublished, ExpectedFile: "native/published.csv", ByteLength: uint64(len(published)), SHA256: hex.EncodeToString(publishedDigest[:]), ObservedCoverageStartNS: &start, ObservedCoverageEndNS: &end},
		{Module: "books", Instrument: "BTC-USDT", MarketDate: "2026-08-20", PublicationLagDays: 3, ExpectedPublicationDate: "2026-08-23", ObservedAtNS: 1787443200000000000, State: NativeFileMissing, ExpectedFile: "absent/books/missing.csv"},
		{Module: "funding", Instrument: "BTC-USDT-SWAP", MarketDate: "2026-08-19", PublicationLagDays: 3, ExpectedPublicationDate: "2026-08-22", ObservedAtNS: 1787443200000000000, State: NativeFilePublishedEmpty, ExpectedFile: "native/empty.csv", SHA256: emptySHA256()},
		{Module: "options", Instrument: "BTC-USD", MarketDate: "2026-08-21", PublicationLagDays: 2, ExpectedPublicationDate: "2026-08-23", ObservedAtNS: 1787443200000000000, State: NativeFileMissing, ExpectedFile: "absent/options/missing-t2.csv"},
		{Module: "index", Instrument: "BTC-USD", MarketDate: "2026-08-22", PublicationLagDays: 3, ExpectedPublicationDate: "2026-08-25", ObservedAtNS: 1787443200000000000, State: NativeFileNotDue, ExpectedFile: "absent/not-due/index.csv"},
	}}
	writeJSON(t, filepath.Join(root, "native-manifest.json"), manifest)
	evidence, err := VerifyNativeManifest(NativeManifestConfig{Root: root, ManifestRelativePath: "native-manifest.json", AsOfDate: "2026-08-23"})
	if err != nil || len(evidence.Files) != 5 || evidence.Files[0].State != NativeFilePublished || evidence.Files[1].State != NativeFileMissing || evidence.Files[1].PublicationLagDays != 3 || evidence.Files[2].State != NativeFilePublishedEmpty || evidence.Files[2].ByteLength != 0 || evidence.Files[3].State != NativeFileMissing || evidence.Files[3].PublicationLagDays != 2 || evidence.Files[4].State != NativeFileNotDue {
		t.Fatalf("VerifyNativeManifest() = %#v, %v", evidence, err)
	}
	fixtureManifest := writeOKXAcceptanceFixtures(t, root, "fixture-manifest.json")
	if len(fixtureManifest.Fixtures) != len(acceptanceFixtureRoles) {
		t.Fatalf("acceptance fixture inventory = %d", len(fixtureManifest.Fixtures))
	}
	venueEvidence, err := VerifyVenue(VenueVerificationConfig{Root: root, FixtureManifestRelative: "fixture-manifest.json", NativeManifestRelative: "native-manifest.json", AsOfDate: "2026-08-23"})
	if err != nil || venueEvidence.EvidenceSHA256 == "" || venueEvidence.NativeFiles.EvidenceSHA256 != evidence.EvidenceSHA256 {
		t.Fatalf("VerifyVenue() = %#v, %v", venueEvidence, err)
	}
}

func TestOKXVerifierSourceFidelityAndExactRootContainment(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	manifest := writeOKXAcceptanceFixtures(t, root, "fixtures.json")
	tradeIndex := fixtureRoleIndex(t, manifest, "trades_all")
	fixture, err := os.ReadFile(filepath.Join(root, manifest.Fixtures[tradeIndex].File))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(fixture)
	summary, err := VerifyFixtures(root, "fixtures.json")
	if err != nil || len(summary.Fixtures) != len(acceptanceFixtureRoles) || summary.Fixtures[tradeIndex].SHA256 != hex.EncodeToString(digest[:]) {
		t.Fatalf("VerifyFixtures() = %#v, %v", summary, err)
	}
	incomplete := manifest
	incomplete.Fixtures = slices.Clone(manifest.Fixtures[:len(manifest.Fixtures)-1])
	writeJSON(t, filepath.Join(root, "incomplete-fixtures.json"), incomplete)
	if _, err := VerifyFixtures(root, "incomplete-fixtures.json"); !errors.Is(err, ErrFixtureBoundary) {
		t.Fatalf("incomplete fixture inventory error = %v", err)
	}
	if _, err := VerifyFixtures(root, "../fixtures.json"); !errors.Is(err, ErrFixtureBoundary) {
		t.Fatalf("traversal error = %v", err)
	}
	sourceIndex := fixtureRoleIndex(t, manifest, "source_contract")
	arbitrary := []byte(`{"synthetic":"not-a-contract"}`)
	if err := os.WriteFile(filepath.Join(root, manifest.Fixtures[sourceIndex].File), arbitrary, 0o600); err != nil {
		t.Fatal(err)
	}
	arbitraryDigest := sha256.Sum256(arbitrary)
	manifest.Fixtures[sourceIndex].ByteLength = uint32(len(arbitrary))
	manifest.Fixtures[sourceIndex].SHA256 = hex.EncodeToString(arbitraryDigest[:])
	writeJSON(t, filepath.Join(root, "fixtures.json"), manifest)
	if _, err := VerifyFixtures(root, "fixtures.json"); !errors.Is(err, ErrFixtureBoundary) {
		t.Fatalf("arbitrary source contract error = %v", err)
	}
	sourcePayload, err := json.Marshal(ExpectedSourceContractFixture())
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, manifest.Fixtures[sourceIndex].File), sourcePayload, 0o600); err != nil {
		t.Fatal(err)
	}
	sourceDigest := sha256.Sum256(sourcePayload)
	manifest.Fixtures[sourceIndex].ByteLength = uint32(len(sourcePayload))
	manifest.Fixtures[sourceIndex].SHA256 = hex.EncodeToString(sourceDigest[:])
	manifest.Fixtures[tradeIndex].Classification = "primary_source_value_projection"
	writeJSON(t, filepath.Join(root, "fixtures.json"), manifest)
	if _, err := VerifyFixtures(root, "fixtures.json"); !errors.Is(err, ErrFixtureBoundary) {
		t.Fatalf("non-synthetic fixture error = %v", err)
	}
	manifest.Fixtures[tradeIndex].Classification = "synthetic_parseable_projection"
	if err := os.WriteFile(filepath.Join(outside, "escape.json"), fixture, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "escape.json"), filepath.Join(root, "escape.json")); err != nil {
		t.Fatal(err)
	}
	manifest.Fixtures[tradeIndex].File = "escape.json"
	writeJSON(t, filepath.Join(root, "fixtures.json"), manifest)
	if _, err := VerifyFixtures(root, "fixtures.json"); !errors.Is(err, ErrFixtureBoundary) {
		t.Fatalf("symlink escape error = %v", err)
	}
}

func writeOKXAcceptanceFixtures(t *testing.T, root, manifestRelative string) FixtureManifest {
	t.Helper()
	preBids := []orderbook.OKXLevel{{Price: "65000", Size: "1", DeprecatedOrders: "0", OrderCount: "1"}}
	preAsks := []orderbook.OKXLevel{{Price: "65001", Size: "1", DeprecatedOrders: "0", OrderCount: "1"}}
	preChecksum := orderbook.OKXChecksum(preBids, preAsks)
	sourceContract, err := json.Marshal(ExpectedSourceContractFixture())
	if err != nil {
		t.Fatal(err)
	}
	payloads := map[string][]byte{
		"source_contract":     sourceContract,
		"trades_all":          []byte(`{"arg":{"channel":"trades-all","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","tradeId":"7","px":"65000","sz":"1","side":"sell","ts":"1787443200000","count":"1"}]}`),
		"book_snapshot":       []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["65001","1","0","1"]],"bids":[["65000","1","0","1"]],"ts":"1787443200000","checksum":0,"prevSeqId":-1,"seqId":100}]}`),
		"book_no_change":      []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[],"bids":[],"ts":"1787443200000","checksum":0,"prevSeqId":100,"seqId":100}]}`),
		"book_reset":          []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"update","data":[{"asks":[],"bids":[["65000","2","0","2"]],"ts":"1787443200000","checksum":0,"prevSeqId":100,"seqId":7}]}`),
		"book_checksum_pre":   []byte(fmt.Sprintf(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["65001","1","0","1"]],"bids":[["65000","1","0","1"]],"ts":"1782172799999","checksum":%d,"prevSeqId":-1,"seqId":1}]}`, preChecksum)),
		"book_checksum_post":  []byte(`{"arg":{"channel":"books","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["65001","1","0","1"]],"bids":[["65000","1","0","1"]],"ts":"1782172800000","checksum":0,"prevSeqId":-1,"seqId":1}]}`),
		"books5":              []byte(`{"arg":{"channel":"books5","instId":"BTC-USDT"},"action":"snapshot","data":[{"asks":[["65001","1","0","1"]],"bids":[["65000","1","0","1"]],"ts":"1787443200000","checksum":0,"prevSeqId":-1,"seqId":1}]}`),
		"vip_denial":          []byte(`{"event":"error","arg":{"channel":"books-l2-tbt","instId":"BTC-USDT"},"connId":"synthetic-connection","code":"64003","msg":"VIP level is lower than VIP4"}`),
		"market_mapping":      []byte(`{"arg":{"channel":"tickers","instId":"BTC-USDT"},"data":[{"instId":"BTC-USDT","last":"65000","bidPx":"","askPx":"65001","askSz":"0","open24h":"64000","high24h":"66000","low24h":"63000","vol24h":"0","volCcy24h":"1000","ts":"1787443200000"}]}`),
		"option_mapping":      []byte(`{"arg":{"channel":"opt-summary","instFamily":"BTC-USD"},"data":[{"instId":"BTC-USD-260925-65000-C","bidPx":"0.05","askPx":"0.06","markPx":"0.055","bidVol":"0.42","askVol":"0.43","markVol":"0.425","delta":"0.5","oi":"10","vol24h":"2","fwdPx":"65010","ulyPx":"65000","idxPx":"65000","ts":"1787443200000"}]}`),
		"lifecycle_mapping":   []byte(`{"code":"0","msg":"","data":[{"instId":"BTC-USDT-SWAP","instType":"SWAP","state":"live","metadataGeneration":"7","listTime":"1787443200000"}]}`),
		"liquidation_mapping": []byte(`{"arg":{"channel":"liquidation-orders"},"data":[{"instType":"SWAP","instFamily":"BTC-USDT","instId":"BTC-USDT-SWAP","uly":"BTC-USDT","details":[{"side":"sell","posSide":"long","bkPx":"64000","sz":"2","bkLoss":"10","ccy":"USDT","ts":"1787443200000"},{"side":"buy","posSide":"short","bkPx":"66000","sz":"1","bkLoss":"5","ccy":"USDT","ts":"1787443200001"}]}]}`),
	}
	manifest := FixtureManifest{
		Version: FixtureManifestVersion, Venue: "okx-v5", AccessDate: DocumentationAccessDate,
		FixtureClaim: "Synthetic acceptance fixtures derived from the access-dated OKX V5 public schemas.",
		Fixtures:     make([]FixtureEntry, 0, len(acceptanceFixtureRoles)),
	}
	for _, role := range acceptanceFixtureRoles {
		payload := payloads[role]
		if len(payload) == 0 {
			t.Fatalf("missing acceptance payload for role %s", role)
		}
		file := role + ".json"
		if err := os.WriteFile(filepath.Join(root, file), payload, 0o600); err != nil {
			t.Fatal(err)
		}
		digest := sha256.Sum256(payload)
		provenance := acceptanceFixtureProvenance[role]
		manifest.Fixtures = append(manifest.Fixtures, FixtureEntry{
			ID: "okx-" + role, Role: role, File: file, Classification: "synthetic_parseable_projection",
			SourceURL: provenance.SourceURL, SourceSection: provenance.SourceSection,
			DerivedFrom: GuideDocumentationURI + " " + role, ByteLength: uint32(len(payload)), SHA256: hex.EncodeToString(digest[:]),
		})
	}
	writeJSON(t, filepath.Join(root, manifestRelative), manifest)
	return manifest
}

func fixtureRoleIndex(t *testing.T, manifest FixtureManifest, role string) int {
	t.Helper()
	for index, entry := range manifest.Fixtures {
		if entry.Role == role {
			return index
		}
	}
	t.Fatalf("fixture role %s not found", role)
	return -1
}

func okxMetadata(t *testing.T, schema string, version uint16, instrument string, payload []byte) normalize.Metadata {
	t.Helper()
	const sourceTimeNS = int64(1787443200000000000)
	const receivedTimeNS = int64(1787443201000000000)
	epoch := [16]byte{1}
	envelope := capture.EnvelopeV1{EnvelopeVersion: capture.EnvelopeVersion, RecordKind: capture.RecordKindWebSocket, SourceID: "okx-v5-public", ChannelOrEndpoint: "synthetic-okx", ConnectionEpoch: capture.OptionalEpoch{Value: epoch, Valid: true}, ArrivalOrdinal: 1, ExchangeTimeNS: capture.OptionalInt64{Value: sourceTimeNS, Valid: true}, ExchangeTimeResolution: capture.ExchangeTimeMillisecond, ReceivedWallTimeNS: receivedTimeNS, ClockEpochID: "okx-synthetic-clock", MonotonicNSSinceClockEpoch: 1, PayloadEncoding: capture.PayloadEncodingJSON, TerminalOutcome: capture.TerminalObserved, RecorderVersion: "okx-fixture-verifier-v1"}
	envelope.SetRawPayload(payload)
	record, err := normalize.BindRawRecord(envelope, normalize.Hash(sha256.Sum256([]byte("okx-synthetic-segment"))), 1, nil)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := normalize.NewMetadata(normalize.MetadataInput{Record: record, SchemaName: schema, SchemaVersion: version, InstrumentUID: instrument, ExchangeTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, ExchangeTimeResolution: normalize.ResolutionMillisecond, SourceEventTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond, SourceSchemaFingerprint: normalize.Hash(sha256.Sum256([]byte("okx-synthetic-schema"))), MapperVersion: "okx-v5-mapper-v1", MapperBindingID: normalize.Hash(sha256.Sum256([]byte("okx-synthetic-binding"))), CatalogSnapshotID: normalize.Hash(sha256.Sum256([]byte("okx-synthetic-catalog")))})
	if err != nil {
		t.Fatal(err)
	}
	return metadata
}

func okxValue(text string, sourceTimeNS int64) normalize.OKXField {
	return normalize.OKXField{State: normalize.SourceValue, Text: text, SourceTimeNS: normalize.OptionalInt64{Value: sourceTimeNS, Valid: true}, SourceTimeResolution: normalize.ResolutionMillisecond}
}

func writeJSON(t *testing.T, path string, value any) {
	t.Helper()
	payload, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, payload, 0o600); err != nil {
		t.Fatal(err)
	}
}

func compareString(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

type okxTestConnection struct {
	writes int
	reads  [][]byte
}

func (c *okxTestConnection) Write(context.Context, websocket.MessageType, []byte) error {
	c.writes++
	return nil
}

func (c *okxTestConnection) Read(context.Context) (websocket.MessageType, []byte, error) {
	if len(c.reads) == 0 {
		return websocket.MessageText, nil, errors.New("no synthetic read available")
	}
	payload := slices.Clone(c.reads[0])
	c.reads = c.reads[1:]
	return websocket.MessageText, payload, nil
}

func (*okxTestConnection) SetReadLimit(int64) {}

func (*okxTestConnection) Close(websocket.StatusCode, string) error { return nil }

type okxTestRoundTripper struct {
	calls int
}

func (r *okxTestRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	r.calls++
	return nil, errors.New("unexpected HTTP I/O")
}
