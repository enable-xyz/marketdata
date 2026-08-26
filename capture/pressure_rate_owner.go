package capture

import (
	"errors"
	"fmt"
	"slices"
	"sync"
)

var (
	ErrRateOwnerFull         = errors.New("capture: rate owner operation bound reached")
	ErrRateOperationUnknown  = errors.New("capture: rate operation is unknown")
	ErrRateOperationTerminal = errors.New("capture: rate operation is terminal")
	ErrRateReconcileRequired = errors.New("capture: unknown rate operation requires reconciliation")
)

type RateScopeKind string

const (
	RateScopeIP         RateScopeKind = "ip"
	RateScopeAccount    RateScopeKind = "account"
	RateScopeEndpoint   RateScopeKind = "endpoint"
	RateScopeSharedPool RateScopeKind = "shared_pool"
)

type RateOperationKind string

const (
	RateOperationConnection RateOperationKind = "connection"
	RateOperationRequest    RateOperationKind = "request"
	RateOperationHandshake  RateOperationKind = "handshake"
	RateOperationSubscribe  RateOperationKind = "subscribe"
	RateOperationSession    RateOperationKind = "session"
)

type RateOwnerIdentity struct {
	Venue     string
	API       string
	ScopeKind RateScopeKind
	ScopeID   string
}

type RateOwnerConfig struct {
	Identity      RateOwnerIdentity
	Policy        RatePolicy
	MaxOperations int
}

type RateOperation struct {
	ID                  string
	Kind                RateOperationKind
	Cost                uint32
	MaximumAttempts     uint32
	DeadlineMonotonicNS uint64
}

type RateAttempt struct {
	Allowed          bool
	Attempt          uint32
	RetryAtMonotonic uint64
	Reason           BudgetBlockReason
}

type RateOutcomeCertainty uint8

const (
	RateOutcomeKnown RateOutcomeCertainty = iota + 1
	RateOutcomeUnknown
)

// RateResponse is typed response-header evidence. Arbitrary headers and their
// secret-bearing values never enter the owner or telemetry surface.
type RateResponse struct {
	Status            int
	RetryAfterNS      uint64
	UsedWeight        uint64
	UsedWeightPresent bool
	Remaining         uint64
	RemainingPresent  bool
	ResetAfterNS      uint64
	Certainty         RateOutcomeCertainty
}

type RateCompletion struct {
	Disposition      ResponseDisposition
	RetryAtMonotonic uint64
	Reconcile        bool
}

type ReconcileResult uint8

const (
	ReconcileApplied ReconcileResult = iota + 1
	ReconcileNotApplied
	ReconcileTerminal
)

type rateOperationState struct {
	operation    RateOperation
	lastResponse RateResponse
	attempts     uint32
	retryAt      uint64
	inFlight     bool
	unknown      bool
	terminal     bool
}

type RateOperationSnapshot struct {
	Operation    RateOperation
	LastResponse RateResponse
	Attempts     uint32
	RetryAt      uint64
	InFlight     bool
	Unknown      bool
	Terminal     bool
}

// RateOwner is the sole mutable owner of one venue/API/scope budget. It owns
// token admission, typed response evidence, cooldowns, connection operations,
// attempt ceilings, deadlines, and unknown-outcome reconciliation.
type RateOwner struct {
	mu              sync.Mutex
	identity        RateOwnerIdentity
	clock           Clock
	budget          *TokenRateBudget
	maximum         int
	maximumAttempts uint32
	operations      map[string]*rateOperationState
}

func NewRateOwner(config RateOwnerConfig, clock Clock) (*RateOwner, error) {
	if clock == nil || !validRateOwnerIdentity(config.Identity) || config.MaxOperations < 1 || config.MaxOperations > 1_000_000 {
		return nil, errors.New("capture: invalid rate owner configuration")
	}
	budget, err := NewTokenRateBudget(config.Policy, clock.Read().MonotonicNS)
	if err != nil {
		return nil, err
	}
	return &RateOwner{
		identity: config.Identity, clock: clock, budget: budget, maximum: config.MaxOperations,
		maximumAttempts: uint32(config.Policy.MaxAttempts),
		operations:      make(map[string]*rateOperationState, min(config.MaxOperations, 1024)),
	}, nil
}

func (o *RateOwner) Identity() RateOwnerIdentity {
	if o == nil {
		return RateOwnerIdentity{}
	}
	return o.identity
}

func (o *RateOwner) Attempt(operation RateOperation) (RateAttempt, error) {
	if o == nil {
		return RateAttempt{}, errors.New("capture: nil rate owner")
	}
	if !validRateOperation(operation) || operation.MaximumAttempts > o.maximumAttempts {
		return RateAttempt{}, errors.New("capture: invalid rate operation")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	now := o.clock.Read().MonotonicNS
	state, exists := o.operations[operation.ID]
	if !exists {
		if len(o.operations) >= o.maximum {
			return RateAttempt{}, ErrRateOwnerFull
		}
		state = &rateOperationState{operation: operation}
		o.operations[operation.ID] = state
	} else if state.operation != operation {
		return RateAttempt{}, errors.New("capture: stable rate operation identity was reused with different policy")
	}
	if state.terminal {
		return RateAttempt{}, ErrRateOperationTerminal
	}
	if state.unknown {
		return RateAttempt{}, ErrRateReconcileRequired
	}
	if state.inFlight {
		return RateAttempt{}, errors.New("capture: rate operation already in flight")
	}
	if now >= operation.DeadlineMonotonicNS {
		state.terminal = true
		return RateAttempt{}, ErrRateOperationTerminal
	}
	if state.attempts >= operation.MaximumAttempts {
		state.terminal = true
		return RateAttempt{}, ErrRateOperationTerminal
	}
	if now < state.retryAt {
		return RateAttempt{Attempt: state.attempts + 1, RetryAtMonotonic: state.retryAt, Reason: BudgetRetryAfter}, nil
	}
	decision, err := o.budget.Acquire(now, operation.Cost)
	if err != nil {
		return RateAttempt{}, err
	}
	if !decision.Allowed {
		return RateAttempt{Attempt: state.attempts + 1, RetryAtMonotonic: decision.RetryAtMonotonic, Reason: decision.Reason}, nil
	}
	state.attempts++
	state.inFlight = true
	return RateAttempt{Allowed: true, Attempt: state.attempts, Reason: BudgetAllowed}, nil
}

func (o *RateOwner) Complete(operationID string, response RateResponse) (RateCompletion, error) {
	if o == nil || !validRateOwnerText(operationID) || response.Status < 0 || response.Certainty < RateOutcomeKnown || response.Certainty > RateOutcomeUnknown {
		return RateCompletion{}, errors.New("capture: invalid rate completion")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	state, exists := o.operations[operationID]
	if !exists {
		return RateCompletion{}, ErrRateOperationUnknown
	}
	if state.terminal {
		return RateCompletion{}, ErrRateOperationTerminal
	}
	if !state.inFlight {
		return RateCompletion{}, errors.New("capture: rate operation is not in flight")
	}
	state.lastResponse = response
	state.inFlight = false
	if response.Certainty == RateOutcomeUnknown {
		state.unknown = true
		return RateCompletion{Reconcile: true}, nil
	}
	now := o.clock.Read().MonotonicNS
	if err := o.budget.ReconcileHeaders(now, response.UsedWeight, response.UsedWeightPresent,
		response.Remaining, response.RemainingPresent, response.ResetAfterNS); err != nil {
		return RateCompletion{}, err
	}
	decision, err := o.budget.ObserveResponse(now, response.Status, max(response.RetryAfterNS, response.ResetAfterNS))
	if err != nil {
		return RateCompletion{}, err
	}
	completion := RateCompletion{Disposition: decision.Disposition, RetryAtMonotonic: decision.RetryAtMonotonic}
	switch decision.Disposition {
	case ResponseAccepted, ResponseTerminal, ResponseCircuitOpened:
		state.terminal = true
	case ResponseRetryable:
		state.retryAt = decision.RetryAtMonotonic
		if state.attempts >= state.operation.MaximumAttempts || state.retryAt >= state.operation.DeadlineMonotonicNS {
			state.terminal = true
		}
	}
	return completion, nil
}

func (o *RateOwner) Snapshot(operationID string) (RateOperationSnapshot, error) {
	if o == nil {
		return RateOperationSnapshot{}, errors.New("capture: nil rate owner")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	state, exists := o.operations[operationID]
	if !exists {
		return RateOperationSnapshot{}, ErrRateOperationUnknown
	}
	return RateOperationSnapshot{
		Operation: state.operation, LastResponse: state.lastResponse, Attempts: state.attempts, RetryAt: state.retryAt,
		InFlight: state.inFlight, Unknown: state.unknown, Terminal: state.terminal,
	}, nil
}

// Reconcile is the only transition out of an unknown outcome. A retry becomes
// possible only after explicit evidence that the prior operation was not applied.
func (o *RateOwner) Reconcile(operationID string, result ReconcileResult) error {
	if o == nil || !validRateOwnerText(operationID) || result < ReconcileApplied || result > ReconcileTerminal {
		return errors.New("capture: invalid rate reconciliation")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	state, exists := o.operations[operationID]
	if !exists {
		return ErrRateOperationUnknown
	}
	if !state.unknown {
		return errors.New("capture: rate operation does not require reconciliation")
	}
	state.unknown = false
	switch result {
	case ReconcileApplied, ReconcileTerminal:
		state.terminal = true
	case ReconcileNotApplied:
		if state.attempts >= state.operation.MaximumAttempts || o.clock.Read().MonotonicNS >= state.operation.DeadlineMonotonicNS {
			state.terminal = true
			return ErrRateOperationTerminal
		}
	}
	return nil
}

// Forget removes only terminal state, keeping live and unknown identities owned.
func (o *RateOwner) Forget(operationID string) error {
	if o == nil {
		return errors.New("capture: nil rate owner")
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	state, exists := o.operations[operationID]
	if !exists {
		return ErrRateOperationUnknown
	}
	if !state.terminal {
		return errors.New("capture: live rate operation cannot be forgotten")
	}
	delete(o.operations, operationID)
	return nil
}

// RateOwners rejects duplicate venue/API/scope identities so exactly one owner
// exists for each configured budget.
type RateOwners struct {
	owners map[RateOwnerIdentity]*RateOwner
}

func NewRateOwners(configs []RateOwnerConfig, clock Clock) (*RateOwners, error) {
	if len(configs) == 0 || len(configs) > 1024 {
		return nil, errors.New("capture: rate owner set must be nonempty and bounded")
	}
	owners := &RateOwners{owners: make(map[RateOwnerIdentity]*RateOwner, len(configs))}
	for _, config := range configs {
		if _, exists := owners.owners[config.Identity]; exists {
			return nil, fmt.Errorf("capture: duplicate rate owner for %s/%s/%s", config.Identity.Venue, config.Identity.API, config.Identity.ScopeKind)
		}
		owner, err := NewRateOwner(config, clock)
		if err != nil {
			return nil, err
		}
		owners.owners[config.Identity] = owner
	}
	return owners, nil
}

func (o *RateOwners) Owner(identity RateOwnerIdentity) (*RateOwner, bool) {
	if o == nil {
		return nil, false
	}
	owner, exists := o.owners[identity]
	return owner, exists
}

func validRateOwnerIdentity(identity RateOwnerIdentity) bool {
	return validRateOwnerText(identity.Venue) && validRateOwnerText(identity.API) && validRateOwnerText(identity.ScopeID) &&
		slices.Contains([]RateScopeKind{RateScopeIP, RateScopeAccount, RateScopeEndpoint, RateScopeSharedPool}, identity.ScopeKind)
}

func validRateOperation(operation RateOperation) bool {
	return validRateOwnerText(operation.ID) && operation.Cost > 0 && operation.MaximumAttempts > 0 && operation.DeadlineMonotonicNS > 0 &&
		slices.Contains([]RateOperationKind{RateOperationConnection, RateOperationRequest, RateOperationHandshake, RateOperationSubscribe, RateOperationSession}, operation.Kind)
}

func validRateOwnerText(value string) bool {
	if value == "" || len(value) > 256 {
		return false
	}
	for _, character := range value {
		if !((character >= 'a' && character <= 'z') || (character >= 'A' && character <= 'Z') ||
			(character >= '0' && character <= '9') || character == '-' || character == '_' || character == '.' || character == ':' || character == '/') {
			return false
		}
	}
	return true
}
