package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"

	"github.com/enable-xyz/marketdata/segment"
)

const MaximumServiceFrameBuffer = 64

type NativePlan struct {
	Reader ObjectReader
	Inputs []InputDescriptor
	Config Config
}

type NativePlanResolver interface {
	ResolveNative(context.Context, ServiceRequest) (NativePlan, error)
}

// FramedNativeService adapts the canonical native replay engine to a bounded
// pull stream. Its sole goroutine can only fill the configured per-client
// buffer and always stops when the request or stream is closed.
type FramedNativeService struct {
	resolver NativePlanResolver
	buffer   int
}

func NewFramedNativeService(resolver NativePlanResolver, buffer int) (*FramedNativeService, error) {
	if resolver == nil || buffer < 1 || buffer > MaximumServiceFrameBuffer {
		return nil, fmt.Errorf("%w: native resolver and buffer 1..%d are required", ErrInvalidServiceRequest, MaximumServiceFrameBuffer)
	}
	return &FramedNativeService{resolver: resolver, buffer: buffer}, nil
}

func (s *FramedNativeService) OpenNative(ctx context.Context, request ServiceRequest) (NativeStream, error) {
	if s == nil || s.resolver == nil {
		return nil, fmt.Errorf("%w: native service is not initialized", ErrInvalidServiceRequest)
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	plan, err := s.resolver.ResolveNative(ctx, request)
	if err != nil {
		return nil, err
	}
	if plan.Reader == nil || len(plan.Inputs) == 0 {
		return nil, fmt.Errorf("%w: native plan is empty", ErrInvalidServiceRequest)
	}
	if _, err := plan.Config.normalized(); err != nil {
		return nil, err
	}
	streamContext, cancel := context.WithCancel(ctx)
	cursor := &framedNativeCursor{
		cancel: cancel,
		frames: make(chan []byte, s.buffer),
		done:   make(chan struct{}),
	}
	go cursor.run(streamContext, plan, request)
	return cursor, nil
}

type framedNativeCursor struct {
	cancel context.CancelFunc
	frames chan []byte
	done   chan struct{}
	mu     sync.Mutex
	err    error
	closed bool
}

func (c *framedNativeCursor) run(ctx context.Context, plan NativePlan, request ServiceRequest) {
	_, err := ReplayCollectorObserved(ctx, plan.Reader, plan.Inputs, plan.Config, func(event Event) error {
		if !nativeServiceEventSelected(request, event) {
			return nil
		}
		frame, marshalErr := MarshalServiceEvent(event)
		if marshalErr != nil {
			return marshalErr
		}
		select {
		case c.frames <- frame:
			return nil
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	})
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	close(c.frames)
	close(c.done)
}

func nativeServiceEventSelected(request ServiceRequest, event Event) bool {
	_, sourceSelected := slices.BinarySearch(request.SourceIDs, event.Coordinate.SourceID)
	if !sourceSelected || event.Coordinate.ReceivedWallTimeNS < request.StartReceivedTimeNS ||
		event.Coordinate.ReceivedWallTimeNS >= request.EndReceivedTimeNS {
		return false
	}
	if event.Kind == EventDiscontinuity {
		return true
	}
	if event.Kind != EventRecord {
		return false
	}
	instrumentUID := ""
	if event.Record.InstrumentUID.Valid {
		instrumentUID = event.Record.InstrumentUID.Value
	}
	return serviceTupleSelected(request, event.Record.SourceID, event.Record.ChannelOrEndpoint, instrumentUID)
}

func (c *framedNativeCursor) Next(ctx context.Context) ([]byte, error) {
	select {
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	case frame, ok := <-c.frames:
		if ok {
			return slices.Clone(frame), nil
		}
		<-c.done
		c.mu.Lock()
		err := c.err
		c.mu.Unlock()
		if err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
}

func (c *framedNativeCursor) Close() error {
	c.mu.Lock()
	if c.closed {
		err := c.err
		c.mu.Unlock()
		if errors.Is(err, context.Canceled) {
			return nil
		}
		return err
	}
	c.closed = true
	c.mu.Unlock()
	c.cancel()
	<-c.done
	c.mu.Lock()
	err := c.err
	c.mu.Unlock()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

type nativeDiscontinuityPayload struct {
	Version               uint16            `json:"version"`
	Type                  string            `json:"type"`
	Kind                  DiscontinuityKind `json:"kind"`
	Reason                IntegrityReason   `json:"reason"`
	SegmentID             string            `json:"segment_id"`
	PreviousStreamEpochID string            `json:"previous_stream_epoch_id"`
	FirstOrdinal          uint64            `json:"first_ordinal"`
	LastOrdinal           uint64            `json:"last_ordinal"`
	FrameOrdinal          uint32            `json:"frame_ordinal"`
	CompressedOffset      uint64            `json:"compressed_offset"`
}

// MarshalServiceEvent preserves native records byte-for-byte in the canonical
// EnvelopeV1 framing. Replay discontinuities become canonical control
// envelopes with a closed versioned JSON payload, so the stream remains a
// sequence of project frames rather than a second wire schema.
func MarshalServiceEvent(event Event) ([]byte, error) {
	switch event.Kind {
	case EventRecord:
		return segment.MarshalEnvelope(event.Record)
	case EventDiscontinuity:
		payload, err := json.Marshal(nativeDiscontinuityPayload{
			Version: ServiceVersionV1, Type: "replay_discontinuity", Kind: event.Discontinuity.Kind,
			Reason: event.Discontinuity.Reason, SegmentID: event.Discontinuity.SegmentID,
			PreviousStreamEpochID: event.Discontinuity.PreviousStreamEpochID,
			FirstOrdinal:          event.Discontinuity.FirstOrdinal, LastOrdinal: event.Discontinuity.LastOrdinal,
			FrameOrdinal: event.Discontinuity.FrameOrdinal, CompressedOffset: event.Discontinuity.CompressedOffset,
		})
		if err != nil {
			return nil, err
		}
		record := segment.Envelope{
			Kind: segment.RecordKindControl, SourceID: event.Coordinate.SourceID,
			ChannelOrEndpoint: "replay.discontinuity", ArrivalOrdinal: event.Coordinate.ArrivalOrdinal,
			MessageOrdinal: event.Coordinate.MessageOrdinal, ReceivedWallTimeNS: event.Coordinate.ReceivedWallTimeNS,
			ClockEpochID: "replay-service-v1", PayloadEncoding: segment.PayloadEncodingJSON, RawPayload: payload,
			TerminalOutcome: segment.OutcomeObserved, RecorderVersion: "replay-service-v1",
		}
		return segment.MarshalEnvelope(record)
	default:
		return nil, fmt.Errorf("%w: native replay event kind", ErrInvalidServiceRequest)
	}
}
