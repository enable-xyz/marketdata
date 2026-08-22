package replay

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"maps"
	"slices"
	"sync"

	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/segment"
)

type epochPlan struct {
	sourceID string
	epochID  string
	firstNS  int64
	segments []InputDescriptor
}

type sourcePlan struct {
	sourceID string
	epochs   []epochPlan
}

type decodedSegment struct {
	index       int
	descriptor  InputDescriptor
	records     []segment.Envelope
	disconnects []bool
	failure     *Discontinuity
	err         error
}

// ReplaySource emits one source in deterministic epoch/ordinal order. Segment
// validation may finish concurrently, but this call owns the only source
// emitter and consumes validation results by descriptor index.
func ReplaySource(ctx context.Context, reader ObjectReader, inputs []InputDescriptor, config Config, emit EmitFunc) (Result, error) {
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
	if len(plans) != 1 {
		return Result{}, fmt.Errorf("%w: source replay requires exactly one source, got %d", ErrInvalidInput, len(plans))
	}
	hasher, err := newLogicalHasher(LogicalHashVersionV1)
	if err != nil {
		return Result{}, err
	}
	var count uint64
	orderedEmit := func(event Event) error {
		event.Order = SourceNativeOrder
		if err := hasher.writeEvent(event); err != nil {
			return err
		}
		if err := emit(event); err != nil {
			return fmt.Errorf("%w: %v", ErrEmitter, err)
		}
		count++
		return nil
	}
	if err := replaySourcePlan(ctx, reader, plans[0], normalized, orderedEmit); err != nil {
		return Result{}, err
	}
	return Result{
		Order:              SourceNativeOrder,
		LogicalHashVersion: LogicalHashVersionV1,
		LogicalHash:        hasher.sum(),
		EventCount:         count,
	}, nil
}

func buildSourcePlans(inputs []InputDescriptor, config Config) ([]sourcePlan, error) {
	if len(inputs) > config.MaxSegments {
		return nil, fmt.Errorf("%w: got %d segments, limit is %d", ErrInputBound, len(inputs), config.MaxSegments)
	}
	bySource := make(map[string]map[string][]InputDescriptor)
	segmentIDs := make(map[string]struct{}, len(inputs))
	objectKeys := make(map[string]struct{}, len(inputs))
	for _, input := range inputs {
		if input.sourceID == "" || input.epochID == "" || input.segmentID == "" {
			return nil, fmt.Errorf("%w: zero or unconstructed input descriptor", ErrInvalidInput)
		}
		if input.byteLength > config.MaxSegmentBytes || input.manifest.Segment.RecordCount > config.MaxRecordsPerSegment || len(input.manifest.Segment.Frames) > config.MaxFramesPerSegment {
			return nil, fmt.Errorf("%w: segment %q exceeds configured byte, record, or frame limit", ErrInputBound, input.segmentID)
		}
		if _, exists := segmentIDs[input.segmentID]; exists {
			return nil, fmt.Errorf("%w: duplicate committed segment ID %q", ErrInvalidInput, input.segmentID)
		}
		if _, exists := objectKeys[input.objectKey]; exists {
			return nil, fmt.Errorf("%w: duplicate committed object key", ErrInvalidInput)
		}
		segmentIDs[input.segmentID] = struct{}{}
		objectKeys[input.objectKey] = struct{}{}
		epochs := bySource[input.sourceID]
		if epochs == nil {
			epochs = make(map[string][]InputDescriptor)
			bySource[input.sourceID] = epochs
		}
		epochs[input.epochID] = append(epochs[input.epochID], input)
	}
	if len(bySource) > config.MaxSources {
		return nil, fmt.Errorf("%w: got %d sources, limit is %d", ErrInputBound, len(bySource), config.MaxSources)
	}

	sourceIDs := slices.Sorted(maps.Keys(bySource))
	plans := make([]sourcePlan, 0, len(sourceIDs))
	for _, sourceID := range sourceIDs {
		epochMap := bySource[sourceID]
		epochIDs := slices.Sorted(maps.Keys(epochMap))
		epochs := make([]epochPlan, 0, len(epochIDs))
		for _, epochID := range epochIDs {
			descriptors := epochMap[epochID]
			slices.SortFunc(descriptors, compareDescriptorRange)
			epochs = append(epochs, epochPlan{
				sourceID: sourceID,
				epochID:  epochID,
				firstNS:  descriptors[0].receivedStartNS,
				segments: descriptors,
			})
		}
		slices.SortFunc(epochs, func(left, right epochPlan) int {
			if left.firstNS < right.firstNS {
				return -1
			}
			if left.firstNS > right.firstNS {
				return 1
			}
			if left.epochID < right.epochID {
				return -1
			}
			if left.epochID > right.epochID {
				return 1
			}
			return 0
		})
		plans = append(plans, sourcePlan{sourceID: sourceID, epochs: epochs})
	}
	return plans, nil
}

func compareDescriptorRange(left, right InputDescriptor) int {
	if left.ordinalStart < right.ordinalStart {
		return -1
	}
	if left.ordinalStart > right.ordinalStart {
		return 1
	}
	if left.ordinalEnd < right.ordinalEnd {
		return -1
	}
	if left.ordinalEnd > right.ordinalEnd {
		return 1
	}
	if left.segmentID < right.segmentID {
		return -1
	}
	if left.segmentID > right.segmentID {
		return 1
	}
	return 0
}

func replaySourcePlan(ctx context.Context, reader ObjectReader, plan sourcePlan, config Config, emit EmitFunc) error {
	for i := range plan.epochs {
		epoch := plan.epochs[i]
		if i > 0 {
			first := epoch.segments[0]
			boundary := Event{
				Kind: EventDiscontinuity,
				Coordinate: Coordinate{
					ReceivedWallTimeNS:           epoch.firstNS,
					SourceID:                     plan.sourceID,
					EpochFirstReceivedWallTimeNS: epoch.firstNS,
					StreamEpochID:                epoch.epochID,
					ArrivalOrdinal:               first.ordinalStart,
				},
				Discontinuity: Discontinuity{
					Kind:                  DiscontinuityEpochBoundary,
					PreviousStreamEpochID: plan.epochs[i-1].epochID,
					FirstOrdinal:          first.ordinalStart,
					LastOrdinal:           first.ordinalStart,
				},
			}
			if err := emit(boundary); err != nil {
				return err
			}
		}
		if err := replayEpoch(ctx, reader, epoch, config, emit); err != nil {
			return err
		}
	}
	return nil
}

func replayEpoch(ctx context.Context, reader ObjectReader, epoch epochPlan, config Config, emit EmitFunc) error {
	workerCount := min(config.Concurrency, config.Prefetch, len(epoch.segments))
	workerCtx, cancel := context.WithCancel(ctx)
	jobs := make(chan struct {
		index      int
		descriptor InputDescriptor
	}, config.Prefetch)
	results := make(chan decodedSegment, config.Prefetch)
	var workers sync.WaitGroup
	for range workerCount {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for job := range jobs {
				result := validateAndDecodeSegment(workerCtx, reader, job.index, job.descriptor)
				select {
				case results <- result:
				case <-workerCtx.Done():
					return
				}
			}
		}()
	}
	defer func() {
		cancel()
		workers.Wait()
	}()

	nextDispatch := 0
	outstanding := 0
	dispatch := func() {
		for nextDispatch < len(epoch.segments) && outstanding < config.Prefetch {
			jobs <- struct {
				index      int
				descriptor InputDescriptor
			}{index: nextDispatch, descriptor: epoch.segments[nextDispatch]}
			nextDispatch++
			outstanding++
		}
	}
	dispatch()
	pending := make(map[int]decodedSegment, config.Prefetch)
	nextOrdinal := uint64(1)
	for next := range len(epoch.segments) {
		result, ready := pending[next]
		for !ready {
			select {
			case <-ctx.Done():
				close(jobs)
				return ctx.Err()
			case result = <-results:
				if result.index != next {
					pending[result.index] = result
					result, ready = pending[next]
				} else {
					ready = true
				}
			}
		}
		delete(pending, next)
		outstanding--
		if result.err != nil {
			close(jobs)
			return result.err
		}
		if err := emitDecodedSegment(epoch, result, &nextOrdinal, emit); err != nil {
			close(jobs)
			return err
		}
		dispatch()
	}
	close(jobs)
	return nil
}

func validateAndDecodeSegment(ctx context.Context, reader ObjectReader, index int, descriptor InputDescriptor) decodedSegment {
	result := decodedSegment{index: index, descriptor: descriptor}
	body, failure, err := readExactObject(ctx, reader, descriptor)
	if err != nil {
		result.err = err
		return result
	}
	if failure != nil {
		result.failure = failure
		return result
	}
	if sha256.Sum256(body) != descriptor.contentSHA256 {
		result.failure = segmentFailure(descriptor, IntegrityReasonObjectSHA256, 0, 0)
		return result
	}
	decoded, err := segment.Decode(body, &descriptor.manifest.Segment)
	if err != nil {
		var decodeError *segment.DecodeError
		if errors.As(err, &decodeError) {
			result.failure = segmentFailure(descriptor, IntegrityReasonFrame, decodeError.Frame, decodeError.Offset)
		} else {
			result.failure = segmentFailure(descriptor, IntegrityReasonFrame, 0, 0)
		}
		return result
	}
	if uint64(len(decoded.Records)) != descriptor.manifest.Segment.RecordCount {
		result.failure = segmentFailure(descriptor, IntegrityReasonRecordCount, 0, 0)
		return result
	}
	disconnects := make([]bool, len(decoded.Records))
	for i := range decoded.Records {
		record := decoded.Records[i]
		if err := validateRecordIdentity(descriptor, record); err != nil {
			result.failure = segmentFailure(descriptor, IntegrityReasonIdentity, 0, 0)
			return result
		}
		if i > 0 && compareRecordCoordinate(decoded.Records[i-1], record) >= 0 {
			result.failure = segmentFailure(descriptor, IntegrityReasonOrdinalOrder, 0, 0)
			return result
		}
		if record.Kind == segment.RecordKindControl {
			control, err := capture.ControlRecordFromSegment(record)
			if err != nil {
				result.failure = segmentFailure(descriptor, IntegrityReasonRecord, 0, 0)
				return result
			}
			disconnects[i] = control.Envelope.ControlKind.Value == capture.ControlDisconnect
		}
	}
	if len(decoded.Records) == 0 || decoded.Records[0].ArrivalOrdinal != descriptor.ordinalStart || decoded.Records[len(decoded.Records)-1].ArrivalOrdinal != descriptor.ordinalEnd {
		result.failure = segmentFailure(descriptor, IntegrityReasonOrdinalOrder, 0, 0)
		return result
	}
	result.records = decoded.Records
	result.disconnects = disconnects
	return result
}

func validateRecordIdentity(descriptor InputDescriptor, record segment.Envelope) error {
	if record.SourceID != descriptor.sourceID || record.ChannelOrEndpoint != descriptor.channelID {
		return ErrIntegrity
	}
	switch descriptor.manifest.EpochKind {
	case segment.EpochConnection:
		if !record.ConnectionEpoch.Valid || record.PollCycleID.Valid || record.ConnectionEpoch.Value != descriptor.epochBytes {
			return ErrIntegrity
		}
	case segment.EpochPoll:
		if !record.PollCycleID.Valid || record.ConnectionEpoch.Valid || record.PollCycleID.Value != descriptor.epochBytes {
			return ErrIntegrity
		}
	default:
		return ErrIntegrity
	}
	return nil
}

func compareRecordCoordinate(left, right segment.Envelope) int {
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

func readExactObject(ctx context.Context, reader ObjectReader, descriptor InputDescriptor) ([]byte, *Discontinuity, error) {
	body, err := reader.Get(ctx, descriptor.objectKey)
	if errors.Is(err, objectstore.ErrNotFound) {
		failure := missingFailure(descriptor)
		return nil, &failure, nil
	}
	if err != nil {
		return nil, nil, fmt.Errorf("replay: open committed object for segment %q: %w", descriptor.segmentID, err)
	}
	if body == nil {
		return nil, nil, fmt.Errorf("replay: object reader returned a nil body for segment %q", descriptor.segmentID)
	}
	data := make([]byte, int(descriptor.byteLength))
	_, readErr := io.ReadFull(body, data)
	if errors.Is(readErr, io.EOF) || errors.Is(readErr, io.ErrUnexpectedEOF) {
		_ = body.Close()
		failure := segmentFailure(descriptor, IntegrityReasonObjectLength, 0, 0)
		return nil, failure, nil
	}
	if readErr != nil {
		_ = body.Close()
		return nil, nil, fmt.Errorf("replay: read committed object for segment %q: %w", descriptor.segmentID, readErr)
	}
	var extra [1]byte
	extraBytes, extraErr := io.ReadFull(body, extra[:])
	closeErr := body.Close()
	if extraBytes != 0 || extraErr == nil {
		failure := segmentFailure(descriptor, IntegrityReasonObjectLength, 0, 0)
		return nil, failure, nil
	}
	if !errors.Is(extraErr, io.EOF) {
		return nil, nil, fmt.Errorf("replay: finish committed object for segment %q: %w", descriptor.segmentID, extraErr)
	}
	if closeErr != nil {
		return nil, nil, fmt.Errorf("replay: close committed object for segment %q: %w", descriptor.segmentID, closeErr)
	}
	return data, nil, nil
}

func segmentFailure(descriptor InputDescriptor, reason IntegrityReason, frame uint32, offset uint64) *Discontinuity {
	return &Discontinuity{
		Kind:             DiscontinuityQuarantinedFrame,
		Reason:           reason,
		SegmentID:        descriptor.segmentID,
		FirstOrdinal:     descriptor.ordinalStart,
		LastOrdinal:      descriptor.ordinalEnd,
		FrameOrdinal:     frame,
		CompressedOffset: offset,
	}
}

func missingFailure(descriptor InputDescriptor) Discontinuity {
	return Discontinuity{
		Kind:         DiscontinuityMissingSegment,
		SegmentID:    descriptor.segmentID,
		FirstOrdinal: descriptor.ordinalStart,
		LastOrdinal:  descriptor.ordinalEnd,
	}
}

func emitDecodedSegment(epoch epochPlan, decoded decodedSegment, nextOrdinal *uint64, emit EmitFunc) error {
	descriptor := decoded.descriptor
	cursor := *nextOrdinal
	if descriptor.ordinalStart < cursor {
		overlap := discontinuityEvent(epoch, descriptor, Discontinuity{
			Kind:         DiscontinuityOrdinalOverlap,
			Reason:       IntegrityReasonOrdinalOrder,
			SegmentID:    descriptor.segmentID,
			FirstOrdinal: descriptor.ordinalStart,
			LastOrdinal:  min(descriptor.ordinalEnd, cursor-1),
		}, descriptor.receivedStartNS, descriptor.ordinalStart, 0)
		if err := emit(overlap); err != nil {
			return err
		}
	}
	if descriptor.ordinalStart > cursor {
		gap := discontinuityEvent(epoch, descriptor, Discontinuity{
			Kind:         DiscontinuityOrdinalGap,
			FirstOrdinal: cursor,
			LastOrdinal:  descriptor.ordinalStart - 1,
		}, descriptor.receivedStartNS, cursor, 0)
		if err := emit(gap); err != nil {
			return err
		}
		cursor = descriptor.ordinalStart
	}
	if decoded.failure != nil {
		if err := emit(discontinuityEvent(epoch, descriptor, *decoded.failure, descriptor.receivedStartNS, descriptor.ordinalStart, 0)); err != nil {
			return err
		}
		if descriptor.ordinalEnd >= *nextOrdinal {
			*nextOrdinal = descriptor.ordinalEnd + 1
		}
		return nil
	}
	if descriptor.ordinalEnd < cursor {
		return nil
	}

	var lastArrival uint64
	haveLast := false
	for i := range decoded.records {
		record := decoded.records[i]
		if record.ArrivalOrdinal < cursor {
			continue
		}
		var gapStart uint64
		if !haveLast {
			gapStart = cursor
		} else {
			gapStart = lastArrival + 1
		}
		if record.ArrivalOrdinal > gapStart {
			gap := discontinuityEvent(epoch, descriptor, Discontinuity{
				Kind:         DiscontinuityOrdinalGap,
				FirstOrdinal: gapStart,
				LastOrdinal:  record.ArrivalOrdinal - 1,
			}, record.ReceivedWallTimeNS, gapStart, 0)
			if err := emit(gap); err != nil {
				return err
			}
		}
		event := Event{
			Kind: EventRecord,
			Coordinate: Coordinate{
				ReceivedWallTimeNS:           record.ReceivedWallTimeNS,
				SourceID:                     epoch.sourceID,
				EpochFirstReceivedWallTimeNS: epoch.firstNS,
				StreamEpochID:                epoch.epochID,
				ArrivalOrdinal:               record.ArrivalOrdinal,
				MessageOrdinal:               record.MessageOrdinal,
			},
			Record: record,
		}
		if err := emit(event); err != nil {
			return err
		}
		if decoded.disconnects[i] {
			disconnect := discontinuityEvent(epoch, descriptor, Discontinuity{
				Kind:         DiscontinuityDisconnect,
				SegmentID:    descriptor.segmentID,
				FirstOrdinal: record.ArrivalOrdinal,
				LastOrdinal:  record.ArrivalOrdinal,
			}, record.ReceivedWallTimeNS, record.ArrivalOrdinal, record.MessageOrdinal)
			if err := emit(disconnect); err != nil {
				return err
			}
		}
		lastArrival = record.ArrivalOrdinal
		haveLast = true
	}
	*nextOrdinal = descriptor.ordinalEnd + 1
	return nil
}

func discontinuityEvent(epoch epochPlan, descriptor InputDescriptor, discontinuity Discontinuity, receivedNS int64, arrival uint64, message uint32) Event {
	return Event{
		Kind: EventDiscontinuity,
		Coordinate: Coordinate{
			ReceivedWallTimeNS:           receivedNS,
			SourceID:                     descriptor.sourceID,
			EpochFirstReceivedWallTimeNS: epoch.firstNS,
			StreamEpochID:                descriptor.epochID,
			ArrivalOrdinal:               arrival,
			MessageOrdinal:               message,
		},
		Discontinuity: discontinuity,
	}
}
