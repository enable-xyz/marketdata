package replay

import (
	"container/heap"
	"context"
	"fmt"
	"sync"
)

type streamItem struct {
	event Event
	err   error
}

type sourceStream struct {
	items <-chan streamItem
}

type mergeHead struct {
	source int
	event  Event
}

type headHeap []mergeHead

func (h headHeap) Len() int { return len(h) }

func (h headHeap) Less(i, j int) bool {
	return compareMergeCoordinate(h[i].event.Coordinate, h[j].event.Coordinate) < 0
}

func (h headHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *headHeap) Push(value any) {
	*h = append(*h, value.(mergeHead))
}

func (h *headHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	old[last] = mergeHead{}
	*h = old[:last]
	return value
}

// ReplayCollectorObserved performs a head-only k-way merge over already
// ordered source streams. It never reads the next event from a source until its
// current head has been selected by the exact six-field collector key.
func ReplayCollectorObserved(ctx context.Context, reader ObjectReader, inputs []InputDescriptor, config Config, emit EmitFunc) (Result, error) {
	if ctx == nil || reader == nil || emit == nil {
		return Result{}, fmt.Errorf("%w: context, object reader, and emitter are required", ErrInvalidInput)
	}
	normalized, err := config.normalized()
	if err != nil {
		return Result{}, err
	}
	plans, err := buildSourcePlans(inputs, normalized)
	if err != nil {
		return Result{}, err
	}
	hasher, err := newLogicalHasher(LogicalHashVersionV1)
	if err != nil {
		return Result{}, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	streams := make([]sourceStream, len(plans))
	var producers sync.WaitGroup
	for i := range plans {
		items := make(chan streamItem, 1)
		streams[i] = sourceStream{items: items}
		plan := plans[i]
		producers.Add(1)
		go func() {
			defer producers.Done()
			defer close(items)
			err := replaySourcePlan(runCtx, reader, plan, normalized, func(event Event) error {
				select {
				case items <- streamItem{event: event}:
					return nil
				case <-runCtx.Done():
					return runCtx.Err()
				}
			})
			if err != nil {
				select {
				case items <- streamItem{err: err}:
				case <-runCtx.Done():
				}
			}
		}()
	}
	defer func() {
		cancel()
		producers.Wait()
	}()

	heads := make(headHeap, 0, len(streams))
	for i := range streams {
		item, ok := <-streams[i].items
		if !ok {
			continue
		}
		if item.err != nil {
			return Result{}, item.err
		}
		heads = append(heads, mergeHead{source: i, event: item.event})
	}
	heap.Init(&heads)

	var count uint64
	for heads.Len() > 0 {
		head := heap.Pop(&heads).(mergeHead)
		event := head.event
		event.Order = CollectorObservedOrder
		if err := hasher.writeEvent(event); err != nil {
			return Result{}, err
		}
		if err := emit(event); err != nil {
			return Result{}, fmt.Errorf("%w: %v", ErrEmitter, err)
		}
		count++

		item, ok := <-streams[head.source].items
		if !ok {
			continue
		}
		if item.err != nil {
			return Result{}, item.err
		}
		heap.Push(&heads, mergeHead{source: head.source, event: item.event})
	}
	return Result{
		Order:              CollectorObservedOrder,
		LogicalHashVersion: LogicalHashVersionV1,
		LogicalHash:        hasher.sum(),
		EventCount:         count,
	}, nil
}

func compareMergeCoordinate(left, right Coordinate) int {
	if left.ReceivedWallTimeNS < right.ReceivedWallTimeNS {
		return -1
	}
	if left.ReceivedWallTimeNS > right.ReceivedWallTimeNS {
		return 1
	}
	if left.SourceID < right.SourceID {
		return -1
	}
	if left.SourceID > right.SourceID {
		return 1
	}
	if left.EpochFirstReceivedWallTimeNS < right.EpochFirstReceivedWallTimeNS {
		return -1
	}
	if left.EpochFirstReceivedWallTimeNS > right.EpochFirstReceivedWallTimeNS {
		return 1
	}
	if left.StreamEpochID < right.StreamEpochID {
		return -1
	}
	if left.StreamEpochID > right.StreamEpochID {
		return 1
	}
	if left.ArrivalOrdinal < right.ArrivalOrdinal {
		return -1
	}
	if left.ArrivalOrdinal > right.ArrivalOrdinal {
		return 1
	}
	if left.MessageOrdinal < right.MessageOrdinal {
		return -1
	}
	if left.MessageOrdinal > right.MessageOrdinal {
		return 1
	}
	return 0
}
