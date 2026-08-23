package hyperliquid

import (
	"fmt"
	"sync"
	"time"
)

type MonotonicClock interface {
	NowMonotonicNS() uint64
}

type systemMonotonicClock struct {
	started time.Time
}

func newSystemMonotonicClock() *systemMonotonicClock {
	return &systemMonotonicClock{started: time.Now()}
}

func (c *systemMonotonicClock) NowMonotonicNS() uint64 {
	return uint64(time.Since(c.started))
}

// WeightedLimiter is a caller-owned, non-sleeping token budget. Reserve is
// performed before I/O. Reconcile records response-dependent cost, including
// debt, after the bounded response has been captured.
type WeightedLimiter struct {
	mu             sync.Mutex
	clock          MonotonicClock
	capacity       uint32
	refillInterval uint64
	tokens         int64
	lastRefillNS   uint64
}

func NewWeightedLimiter(capacity uint32, refillInterval time.Duration, clock MonotonicClock) (*WeightedLimiter, error) {
	if capacity == 0 || refillInterval <= 0 || clock == nil {
		return nil, fmt.Errorf("%w: invalid weighted limiter", ErrRateBudget)
	}
	now := clock.NowMonotonicNS()
	return &WeightedLimiter{clock: clock, capacity: capacity, refillInterval: uint64(refillInterval), tokens: int64(capacity), lastRefillNS: now}, nil
}

func (l *WeightedLimiter) matches(capacity uint32, refillInterval time.Duration) bool {
	return l != nil && l.clock != nil && l.capacity == capacity && l.refillInterval == uint64(refillInterval)
}

func (l *WeightedLimiter) Reserve(weight uint32) error {
	if l == nil || weight == 0 || weight > l.capacity {
		return ErrRateBudget
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.refill(); err != nil {
		return err
	}
	if int64(weight) > l.tokens {
		return ErrRateBudget
	}
	l.tokens -= int64(weight)
	return nil
}

func (l *WeightedLimiter) Reconcile(additionalWeight uint32) error {
	if l == nil {
		return ErrRateBudget
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.refill(); err != nil {
		return err
	}
	l.tokens -= int64(additionalWeight)
	return nil
}

func (l *WeightedLimiter) Remaining() (int64, error) {
	if l == nil {
		return 0, ErrRateBudget
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.refill(); err != nil {
		return 0, err
	}
	return l.tokens, nil
}

func (l *WeightedLimiter) refill() error {
	now := l.clock.NowMonotonicNS()
	if now < l.lastRefillNS {
		return ErrRateClockRegression
	}
	intervals := (now - l.lastRefillNS) / l.refillInterval
	if intervals == 0 {
		return nil
	}
	if intervals >= 2 || intervals*uint64(l.capacity) >= uint64(max(int64(l.capacity)-l.tokens, 0)) {
		l.tokens = int64(l.capacity)
	} else {
		l.tokens += int64(intervals * uint64(l.capacity))
		if l.tokens > int64(l.capacity) {
			l.tokens = int64(l.capacity)
		}
	}
	l.lastRefillNS += intervals * l.refillInterval
	return nil
}
