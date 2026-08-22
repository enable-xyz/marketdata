package capture

import (
	"errors"
	"fmt"
	"math"
	"sync"
)

var (
	ErrInvalidClock  = errors.New("capture: invalid clock")
	ErrClockOverflow = errors.New("capture: clock overflow")
)

type ClockReading struct {
	WallTimeNS   int64
	MonotonicNS  uint64
	ClockEpochID string
}

type Timer interface {
	DeadlineMonotonicNS() uint64
	Fired() bool
	Stop() bool
}

type Clock interface {
	Read() ClockReading
	NewTimer(afterNS uint64) (Timer, error)
}

// ManualClock advances only through explicit calls. Wall time may be equal or
// regress while monotonic time remains nondecreasing.
type ManualClock struct {
	mu           sync.Mutex
	wallTimeNS   int64
	monotonicNS  uint64
	clockEpochID string
}

func NewManualClock(wallTimeNS int64, clockEpochID string) (*ManualClock, error) {
	if err := validateContractText("clock_epoch_id", clockEpochID, MaxClockEpochIDBytes); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrInvalidClock, err)
	}
	return &ManualClock{wallTimeNS: wallTimeNS, clockEpochID: clockEpochID}, nil
}

func (c *ManualClock) Read() ClockReading {
	c.mu.Lock()
	defer c.mu.Unlock()
	return ClockReading{
		WallTimeNS:   c.wallTimeNS,
		MonotonicNS:  c.monotonicNS,
		ClockEpochID: c.clockEpochID,
	}
}

func (c *ManualClock) NewTimer(afterNS uint64) (Timer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if afterNS > math.MaxUint64-c.monotonicNS {
		return nil, ErrClockOverflow
	}
	return &manualTimer{clock: c, deadlineNS: c.monotonicNS + afterNS}, nil
}

// Advance moves both clocks by delta without consulting ambient time.
func (c *ManualClock) Advance(deltaNS uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if deltaNS > math.MaxUint64-c.monotonicNS || deltaNS > math.MaxInt64 {
		return ErrClockOverflow
	}
	delta := int64(deltaNS)
	if delta > 0 && c.wallTimeNS > math.MaxInt64-delta {
		return ErrClockOverflow
	}
	c.monotonicNS += deltaNS
	c.wallTimeNS += delta
	return nil
}

// SetWall changes only evidence wall time. It deliberately permits equal and
// regressing values so capture ordering can be tested independently.
func (c *ManualClock) SetWall(wallTimeNS int64) {
	c.mu.Lock()
	c.wallTimeNS = wallTimeNS
	c.mu.Unlock()
}

type manualTimer struct {
	clock      *ManualClock
	deadlineNS uint64
	mu         sync.Mutex
	stopped    bool
	observed   bool
}

func (t *manualTimer) DeadlineMonotonicNS() uint64 { return t.deadlineNS }

func (t *manualTimer) Fired() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return false
	}
	reading := t.clock.Read()
	if reading.MonotonicNS < t.deadlineNS {
		return false
	}
	t.observed = true
	return true
}

func (t *manualTimer) Stop() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	wasActive := !t.stopped && !t.observed
	t.stopped = true
	return wasActive
}
