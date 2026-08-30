package collector

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/enable-xyz/marketdata/capture"
)

// Clock is the live collector's sole wall, monotonic, and scheduling clock.
// WaitUntil must return promptly when ctx is canceled.
type Clock interface {
	capture.Clock
	WaitUntil(context.Context, uint64) error
}

// SystemClock is a process-local monotonic clock backed by time.Time's
// monotonic component. It starts no goroutines and its waits are cancelable.
type SystemClock struct {
	started time.Time
	epochID string
}

// NewSystemClock constructs a production clock with an explicit evidence
// epoch identity.
func NewSystemClock(epochID string) (*SystemClock, error) {
	if _, err := capture.NewManualClock(0, epochID); err != nil {
		return nil, err
	}
	return &SystemClock{started: time.Now(), epochID: epochID}, nil
}

func (c *SystemClock) Read() capture.ClockReading {
	if c == nil {
		return capture.ClockReading{}
	}
	now := time.Now()
	elapsed := now.Sub(c.started)
	if elapsed < 0 {
		elapsed = 0
	}
	return capture.ClockReading{
		WallTimeNS:   now.UnixNano(),
		MonotonicNS:  uint64(elapsed),
		ClockEpochID: c.epochID,
	}
}

func (c *SystemClock) NewTimer(afterNS uint64) (capture.Timer, error) {
	if c == nil {
		return nil, capture.ErrInvalidClock
	}
	reading := c.Read()
	if afterNS > math.MaxUint64-reading.MonotonicNS {
		return nil, capture.ErrClockOverflow
	}
	return &systemTimer{clock: c, deadline: reading.MonotonicNS + afterNS}, nil
}

func (c *SystemClock) WaitUntil(ctx context.Context, targetMonotonicNS uint64) error {
	if c == nil {
		return capture.ErrInvalidClock
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	now := c.Read().MonotonicNS
	if now >= targetMonotonicNS {
		return nil
	}
	delta := targetMonotonicNS - now
	if delta > uint64(math.MaxInt64) {
		delta = uint64(math.MaxInt64)
	}
	timer := time.NewTimer(time.Duration(delta))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type systemTimer struct {
	clock    *SystemClock
	deadline uint64

	mu       sync.Mutex
	stopped  bool
	observed bool
}

func (t *systemTimer) DeadlineMonotonicNS() uint64 { return t.deadline }

func (t *systemTimer) Fired() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped || t.clock.Read().MonotonicNS < t.deadline {
		return false
	}
	t.observed = true
	return true
}

func (t *systemTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	active := !t.stopped && !t.observed
	t.stopped = true
	return active
}

// waitAfter waits for a clock-relative duration without consulting ambient
// time outside the injected clock.
func waitAfter(ctx context.Context, clock Clock, after time.Duration) error {
	if after <= 0 {
		return errors.New("collector: wait duration must be positive")
	}
	timer, err := clock.NewTimer(uint64(after))
	if err != nil {
		return err
	}
	defer timer.Stop()
	return clock.WaitUntil(ctx, timer.DeadlineMonotonicNS())
}

var _ Clock = (*SystemClock)(nil)
