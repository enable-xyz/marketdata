package capture

import (
	"crypto/sha256"
	"errors"
	"math"
	"testing"
)

func TestSourceContractValidation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		mutate func(*SourceContract)
		valid  bool
	}{
		{name: "complete versioned contract", valid: true},
		{name: "wrong version", mutate: func(c *SourceContract) { c.Version++ }},
		{name: "documentation credentials", mutate: func(c *SourceContract) { c.Documentation[0].URL = "https://user:secret@example.test/rules" }},
		{name: "partial ACK policy", mutate: func(c *SourceContract) { c.Subscription.ACKTimeoutNS = 0 }},
		{name: "overlapping response status", mutate: func(c *SourceContract) { c.Rate.TerminalStatusCodes = []int{403} }},
		{name: "unsorted response status", mutate: func(c *SourceContract) { c.Rate.RetryableStatusCodes = []int{500, 429} }},
		{name: "undeclared ambiguity", mutate: func(c *SourceContract) { c.Capabilities[1].Declaration = "" }},
		{name: "synthetic provenance claim", mutate: func(c *SourceContract) { c.FixtureIdentities[0].SourceReference = "https://example.test/claimed" }},
		{name: "payload exceeds framing", mutate: func(c *SourceContract) { c.Payload.MaxRawBytes = MaxPayloadBytes + 1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			contract := validSourceContractForTest()
			if test.mutate != nil {
				test.mutate(&contract)
			}
			err := contract.Validate()
			if test.valid && err != nil {
				t.Fatalf("Validate() error = %v", err)
			}
			if !test.valid && !errors.Is(err, ErrInvalidSourceContract) {
				t.Fatalf("Validate() error = %v, want ErrInvalidSourceContract", err)
			}
		})
	}
}

func TestSourceContractManualClockAndTimers(t *testing.T) {
	t.Parallel()
	clock, err := NewManualClock(1_000, "clock-test")
	if err != nil {
		t.Fatalf("NewManualClock() error = %v", err)
	}
	timer, err := clock.NewTimer(10)
	if err != nil {
		t.Fatalf("NewTimer() error = %v", err)
	}
	if timer.Fired() {
		t.Fatal("timer fired without an explicit advance")
	}
	if err := clock.Advance(9); err != nil {
		t.Fatalf("Advance(9) error = %v", err)
	}
	clock.SetWall(500)
	reading := clock.Read()
	if reading.WallTimeNS != 500 || reading.MonotonicNS != 9 || timer.Fired() {
		t.Fatalf("regressing wall read/timer = %#v, fired %v", reading, timer.Fired())
	}
	clock.SetWall(500)
	if err := clock.Advance(1); err != nil {
		t.Fatalf("Advance(1) error = %v", err)
	}
	if !timer.Fired() {
		t.Fatal("timer did not fire at its exact manual deadline")
	}
	if timer.Stop() {
		t.Fatal("observed timer reported active on Stop")
	}
	if err := clock.Advance(math.MaxUint64); !errors.Is(err, ErrClockOverflow) {
		t.Fatalf("Advance(MaxUint64) error = %v, want ErrClockOverflow", err)
	}
}

func TestSourceContractVenueRateBudget(t *testing.T) {
	t.Parallel()
	policy := validSourceContractForTest().Rate
	tests := []struct {
		name       string
		status     int
		retryAfter uint64
		want       ResponseDisposition
		clamped    bool
	}{
		{name: "429 retry after", status: 429, retryAfter: 20, want: ResponseRetryable},
		{name: "5xx default retry", status: 500, want: ResponseRetryable},
		{name: "retry after clamp", status: 503, retryAfter: 1_000, want: ResponseRetryable, clamped: true},
		{name: "403 circuit", status: 403, want: ResponseCircuitOpened},
		{name: "418 circuit", status: 418, want: ResponseCircuitOpened},
		{name: "terminal", status: 401, want: ResponseTerminal},
		{name: "accepted", status: 200, want: ResponseAccepted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			budget, err := NewTokenRateBudget(policy, 100)
			if err != nil {
				t.Fatalf("NewTokenRateBudget() error = %v", err)
			}
			decision, err := budget.ObserveResponse(100, test.status, test.retryAfter)
			if err != nil {
				t.Fatalf("ObserveResponse() error = %v", err)
			}
			if decision.Disposition != test.want || decision.RetryAfterClamped != test.clamped {
				t.Fatalf("ObserveResponse() = %#v, want disposition %d clamped %v", decision, test.want, test.clamped)
			}
			acquired, err := budget.Acquire(100, 1)
			if err != nil {
				t.Fatalf("Acquire() error = %v", err)
			}
			if (test.want == ResponseRetryable || test.want == ResponseCircuitOpened) && acquired.Allowed {
				t.Fatalf("Acquire() allowed under response disposition %d", test.want)
			}
		})
	}

	budget, err := NewTokenRateBudget(policy, 0)
	if err != nil {
		t.Fatalf("NewTokenRateBudget() error = %v", err)
	}
	for range policy.Capacity {
		decision, acquireErr := budget.Acquire(0, 1)
		if acquireErr != nil || !decision.Allowed {
			t.Fatalf("Acquire() before exhaustion = %#v, %v", decision, acquireErr)
		}
	}
	blocked, err := budget.Acquire(0, 1)
	if err != nil || blocked.Allowed || blocked.Reason != BudgetExhausted {
		t.Fatalf("exhausted Acquire() = %#v, %v", blocked, err)
	}
	refilled, err := budget.Acquire(policy.RefillIntervalNS, 1)
	if err != nil || !refilled.Allowed {
		t.Fatalf("refilled Acquire() = %#v, %v", refilled, err)
	}
	if _, err := budget.Acquire(0, 1); !errors.Is(err, ErrRateClockRegression) {
		t.Fatalf("regressing Acquire() error = %v, want ErrRateClockRegression", err)
	}

	deficitPolicy := policy
	deficitPolicy.Capacity = 5
	deficitPolicy.RefillTokens = 1
	deficitPolicy.RequestCost = 5
	deficitBudget, err := NewTokenRateBudget(deficitPolicy, 0)
	if err != nil {
		t.Fatalf("NewTokenRateBudget(deficit) error = %v", err)
	}
	if decision, err := deficitBudget.Acquire(0, 5); err != nil || !decision.Allowed {
		t.Fatalf("initial deficit Acquire() = %#v, %v", decision, err)
	}
	deficit, err := deficitBudget.Acquire(0, 5)
	if err != nil || deficit.RetryAtMonotonic != 5*deficitPolicy.RefillIntervalNS {
		t.Fatalf("deficit retry = %#v, %v", deficit, err)
	}
}

func TestSourceContractRESTEvidence(t *testing.T) {
	t.Parallel()
	request := RESTRequestEvidenceV1{
		Version:       RESTEvidenceVersion,
		Kind:          "request",
		RequestID:     "poll-1",
		Method:        RESTMethodGET,
		Parameters:    []SanitizedParameter{{Name: "symbol", Value: "SYNTH"}},
		ScheduledAtNS: 10,
		StartedAtNS:   20,
	}
	encoded, err := MarshalRESTRequestEvidence(request)
	if err != nil {
		t.Fatalf("MarshalRESTRequestEvidence() error = %v", err)
	}
	decoded, err := UnmarshalRESTRequestEvidence(encoded)
	if err != nil || decoded.RequestID != request.RequestID || decoded.Method != request.Method {
		t.Fatalf("UnmarshalRESTRequestEvidence() = %#v, %v", decoded, err)
	}
	request.Parameters[0].Name = "api_key"
	if _, err := MarshalRESTRequestEvidence(request); !errors.Is(err, ErrInvalidRESTEvidence) {
		t.Fatalf("secret request evidence error = %v", err)
	}
	response := RESTResponseEvidenceV1{
		Version:       RESTEvidenceVersion,
		Kind:          "response",
		RequestID:     "poll-1",
		CompletedAtNS: 30,
		Status:        429,
		RetryAfterNS:  50,
		Headers: []RESTHeader{
			{Kind: RESTHeaderContentType, Value: "application/json"},
			{Kind: RESTHeaderRetryAfter, Value: "synthetic"},
		},
	}
	encoded, err = MarshalRESTResponseEvidence(response)
	if err != nil {
		t.Fatalf("MarshalRESTResponseEvidence() error = %v", err)
	}
	decodedResponse, err := UnmarshalRESTResponseEvidence(encoded)
	if err != nil || decodedResponse.Status != 429 || decodedResponse.RetryAfterNS != 50 {
		t.Fatalf("UnmarshalRESTResponseEvidence() = %#v, %v", decodedResponse, err)
	}
	response.Headers = []RESTHeader{{Kind: RESTHeaderKind("authorization"), Value: "secret"}}
	if _, err := MarshalRESTResponseEvidence(response); !errors.Is(err, ErrInvalidRESTEvidence) {
		t.Fatalf("secret response header error = %v", err)
	}
}

func TestSourceContractScriptedTransport(t *testing.T) {
	t.Parallel()
	raw := []byte{0, 1, 2, 3}
	transport, err := NewScriptedTransport([]TransportEvent{{Kind: TransportEventApplication, Raw: raw, Encoding: PayloadEncodingBinary}})
	if err != nil {
		t.Fatalf("NewScriptedTransport() error = %v", err)
	}
	raw[0] = 9
	event, err := transport.Next(t.Context())
	if err != nil {
		t.Fatalf("Next() error = %v", err)
	}
	if event.Raw[0] != 0 || transport.Remaining() != 0 {
		t.Fatalf("scripted event aliases input or did not step: %#v", event)
	}
	if _, err := transport.Next(t.Context()); !errors.Is(err, ErrTransportScriptExhausted) {
		t.Fatalf("exhausted Next() error = %v", err)
	}
	if err := transport.Close(t.Context(), ClosePlanned); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	closed, reason := transport.Closed()
	if !closed || reason != ClosePlanned {
		t.Fatalf("Closed() = %v, %d", closed, reason)
	}
}

func TestRunnerConfigACKBatchFeasibility(t *testing.T) {
	t.Parallel()
	contract := validSourceContractForTest()
	contract.Topology.MaxSubscriptions = 9
	contract.Topology.MaxSubscriptionsPerACK = 2
	contract.Subscription.MaxPendingACK = 4
	if err := contract.Validate(); err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	config := RunnerConfig{
		Epoch:                 StreamEpoch{Kind: EpochConnection, ID: [16]byte{1}},
		ChannelOrEndpoint:     "synthetic.trades.v1",
		DataFamily:            "trades",
		RecorderVersion:       "runner-config-test",
		SubscriptionRequestID: "sub-boundary",
		ExpectedSubscriptions: []string{"one", "two", "three", "four", "five", "six", "seven", "eight"},
	}
	if err := validateRunnerConfig(contract, config); err != nil {
		t.Fatalf("exact four-batch boundary rejected: %v", err)
	}
	config.ExpectedSubscriptions = append(config.ExpectedSubscriptions, "nine")
	if err := validateRunnerConfig(contract, config); !errors.Is(err, ErrInvalidRunnerConfig) {
		t.Fatalf("five batches under four-batch bound error = %v, want ErrInvalidRunnerConfig", err)
	}
}

func validSourceContractForTest() SourceContract {
	fixture := []byte(`{"synthetic":true}`)
	return SourceContract{
		Version:    SourceContractVersion,
		SourceID:   "synthetic-source",
		ContractID: "synthetic.ws.v1",
		APIVersion: "v1",
		Documentation: []DocumentationRef{
			{URL: "https://example.test/official", AccessedAtNS: 1, Authority: RuleOfficialDocumentation},
			{URL: "https://example.test/inference", AccessedAtNS: 1, Authority: RuleAdapterPolicyInference},
		},
		Capabilities: []Capability{
			{ChannelOrEndpoint: "synthetic.trades.v1", DataFamily: "trades", Entitlement: "public", Support: SupportAvailable},
			{ChannelOrEndpoint: "synthetic.book.v1", DataFamily: "book", Entitlement: "public", Support: SupportAmbiguous, Declaration: "synthetic ambiguity used only to exercise validation"},
			{ChannelOrEndpoint: "synthetic.private.v1", DataFamily: "private", Entitlement: "none", Support: SupportUnsupported, Declaration: "private data is outside the public market-data boundary"},
		},
		Topology:     ConnectionTopology{Transport: TransportWebSocket, MaxConnections: 2, MaxSubscriptions: 8, MaxSubscriptionsPerACK: 8, Throttleable: false},
		Subscription: SubscriptionPolicy{ACKMode: ACKExact, ACKTimeoutNS: 100, MaxPendingACK: 1},
		Heartbeat:    HeartbeatPolicy{Mode: HeartbeatPingPong, IntervalNS: 50, TimeoutNS: 100},
		Rate: RatePolicy{
			Capacity:             2,
			RefillTokens:         1,
			RefillIntervalNS:     100,
			ConnectionCost:       1,
			RequestCost:          1,
			MaxAttempts:          2,
			DefaultRetryAfterNS:  10,
			MaxRetryAfterNS:      100,
			CircuitOpenNS:        1_000,
			RetryableStatusCodes: []int{429, 500, 502, 503},
			TerminalStatusCodes:  []int{401},
			CircuitStatusCodes:   []int{403, 418},
		},
		Payload:           PayloadPolicy{MaxRawBytes: 1024, MaxSchemaDepth: 8, MaxSchemaFields: 64, MaxArrayElements: 256},
		FixtureIdentities: []FixtureIdentity{{ID: "synthetic.contract.valid.v1", SHA256: sha256.Sum256(fixture), ByteLength: uint32(len(fixture)), Provenance: FixtureSynthetic}},
	}
}
