package verify

import (
	"math"
	"sync"
	"time"

	"github.com/enable-xyz/marketdata/capture"
)

type SystemClock struct {
	started time.Time
	epochID string
}

func NewSystemClock(epochID string) (*SystemClock, error) {
	if _, err := capture.NewManualClock(0, epochID); err != nil {
		return nil, err
	}
	return &SystemClock{started: time.Now(), epochID: epochID}, nil
}

func (c *SystemClock) Read() capture.ClockReading {
	now := time.Now()
	elapsed := now.Sub(c.started)
	if elapsed < 0 {
		elapsed = 0
	}
	return capture.ClockReading{WallTimeNS: now.UnixNano(), MonotonicNS: uint64(elapsed), ClockEpochID: c.epochID}
}

func (c *SystemClock) NewTimer(afterNS uint64) (capture.Timer, error) {
	reading := c.Read()
	if afterNS > math.MaxUint64-reading.MonotonicNS {
		return nil, capture.ErrClockOverflow
	}
	return &systemTimer{clock: c, deadline: reading.MonotonicNS + afterNS}, nil
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

var _ capture.Clock = (*SystemClock)(nil)
