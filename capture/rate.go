package capture

import (
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
)

var ErrRateClockRegression = errors.New("capture: rate budget monotonic clock regressed")

type BudgetBlockReason uint8

const (
	BudgetAllowed BudgetBlockReason = iota
	BudgetExhausted
	BudgetRetryAfter
	BudgetCircuitOpen
)

type BudgetDecision struct {
	Allowed          bool
	Reason           BudgetBlockReason
	RemainingTokens  uint32
	RetryAtMonotonic uint64
}

type ResponseDisposition uint8

const (
	ResponseAccepted ResponseDisposition = iota + 1
	ResponseRetryable
	ResponseTerminal
	ResponseCircuitOpened
)

type ResponseDecision struct {
	Disposition       ResponseDisposition
	RetryAtMonotonic  uint64
	RetryAfterClamped bool
}

type RateBudget interface {
	Acquire(nowMonotonicNS uint64, cost uint32) (BudgetDecision, error)
	ObserveResponse(nowMonotonicNS uint64, status int, retryAfterNS uint64) (ResponseDecision, error)
}

// TokenRateBudget is a venue-owned token bucket plus explicit retry-after and
// circuit state. It never sleeps or retries an operation itself.
type TokenRateBudget struct {
	mu             sync.Mutex
	policy         RatePolicy
	tokens         uint32
	lastRefillNS   uint64
	retryUntilNS   uint64
	circuitUntilNS uint64
}

func NewTokenRateBudget(policy RatePolicy, initialMonotonicNS uint64) (*TokenRateBudget, error) {
	if err := validateRatePolicy(policy); err != nil {
		return nil, err
	}
	policy.RetryableStatusCodes = slices.Clone(policy.RetryableStatusCodes)
	policy.TerminalStatusCodes = slices.Clone(policy.TerminalStatusCodes)
	policy.CircuitStatusCodes = slices.Clone(policy.CircuitStatusCodes)
	return &TokenRateBudget{
		policy:       policy,
		tokens:       policy.Capacity,
		lastRefillNS: initialMonotonicNS,
	}, nil
}

func (b *TokenRateBudget) Acquire(nowMonotonicNS uint64, cost uint32) (BudgetDecision, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if nowMonotonicNS < b.lastRefillNS {
		return BudgetDecision{}, ErrRateClockRegression
	}
	b.refill(nowMonotonicNS)
	if nowMonotonicNS < b.circuitUntilNS {
		return BudgetDecision{Reason: BudgetCircuitOpen, RemainingTokens: b.tokens, RetryAtMonotonic: b.circuitUntilNS}, nil
	}
	if nowMonotonicNS < b.retryUntilNS {
		return BudgetDecision{Reason: BudgetRetryAfter, RemainingTokens: b.tokens, RetryAtMonotonic: b.retryUntilNS}, nil
	}
	if cost > b.policy.Capacity {
		return BudgetDecision{}, fmt.Errorf("capture: rate cost %d exceeds capacity %d", cost, b.policy.Capacity)
	}
	if cost > b.tokens {
		deficit := cost - b.tokens
		intervals := uint64(deficit / b.policy.RefillTokens)
		if deficit%b.policy.RefillTokens != 0 {
			intervals++
		}
		delay := multiplySaturating(intervals, b.policy.RefillIntervalNS)
		retryAt := addSaturating(b.lastRefillNS, delay)
		return BudgetDecision{Reason: BudgetExhausted, RemainingTokens: b.tokens, RetryAtMonotonic: retryAt}, nil
	}
	b.tokens -= cost
	return BudgetDecision{Allowed: true, Reason: BudgetAllowed, RemainingTokens: b.tokens}, nil
}

func (b *TokenRateBudget) ObserveResponse(nowMonotonicNS uint64, status int, retryAfterNS uint64) (ResponseDecision, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if nowMonotonicNS < b.lastRefillNS {
		return ResponseDecision{}, ErrRateClockRegression
	}
	b.refill(nowMonotonicNS)
	if slices.Contains(b.policy.CircuitStatusCodes, status) {
		until := addSaturating(nowMonotonicNS, b.policy.CircuitOpenNS)
		b.circuitUntilNS = max(b.circuitUntilNS, until)
		return ResponseDecision{Disposition: ResponseCircuitOpened, RetryAtMonotonic: b.circuitUntilNS}, nil
	}
	if slices.Contains(b.policy.TerminalStatusCodes, status) {
		return ResponseDecision{Disposition: ResponseTerminal}, nil
	}
	if slices.Contains(b.policy.RetryableStatusCodes, status) {
		clamped := false
		if retryAfterNS == 0 {
			retryAfterNS = b.policy.DefaultRetryAfterNS
		}
		if retryAfterNS > b.policy.MaxRetryAfterNS {
			retryAfterNS = b.policy.MaxRetryAfterNS
			clamped = true
		}
		until := addSaturating(nowMonotonicNS, retryAfterNS)
		b.retryUntilNS = max(b.retryUntilNS, until)
		return ResponseDecision{Disposition: ResponseRetryable, RetryAtMonotonic: b.retryUntilNS, RetryAfterClamped: clamped}, nil
	}
	return ResponseDecision{Disposition: ResponseAccepted}, nil
}

func (b *TokenRateBudget) refill(nowMonotonicNS uint64) {
	elapsed := nowMonotonicNS - b.lastRefillNS
	intervals := elapsed / b.policy.RefillIntervalNS
	if intervals == 0 {
		return
	}
	missing := b.policy.Capacity - b.tokens
	var added uint64
	if intervals > math.MaxUint64/uint64(b.policy.RefillTokens) {
		added = math.MaxUint64
	} else {
		added = intervals * uint64(b.policy.RefillTokens)
	}
	if added >= uint64(missing) {
		b.tokens = b.policy.Capacity
	} else {
		b.tokens += uint32(added)
	}
	advance := intervals * b.policy.RefillIntervalNS
	b.lastRefillNS += advance
}

func multiplySaturating(left, right uint64) uint64 {
	if left != 0 && right > math.MaxUint64/left {
		return math.MaxUint64
	}
	return left * right
}

func addSaturating(value, delta uint64) uint64 {
	if delta > math.MaxUint64-value {
		return math.MaxUint64
	}
	return value + delta
}
