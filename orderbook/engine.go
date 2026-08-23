package orderbook

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"

	"github.com/enable-xyz/marketdata/normalize"
)

type bufferedUpdate struct {
	event normalize.BookUpdateV1
	bytes uint64
}

type pendingSnapshot struct {
	observation SnapshotObservation
	attempt     int
}

type reconstructionEpoch struct {
	evidence      EpochEvidence
	buffer        []bufferedUpdate
	bufferBytes   uint64
	bufferMinNS   int64
	bufferMaxNS   int64
	pending       *pendingSnapshot
	book          bookState
	localSequence uint64
	inputDigest   *digestEncoder
	outputDigest  *digestEncoder
}

type Engine struct {
	config       Config
	policy       SequencePolicy
	fetcher      SnapshotFetcher
	policyID     normalize.Hash
	state        State
	epoch        *reconstructionEpoch
	parentID     normalize.Hash
	terminal     bool
	allowRestart bool
}

func New(config Config, policy SequencePolicy, fetcher SnapshotFetcher) (*Engine, error) {
	if err := validateConfig(config, policy, fetcher); err != nil {
		return nil, err
	}
	return &Engine{
		config:   config,
		policy:   policy,
		fetcher:  fetcher,
		policyID: policyIdentity(config, policy),
		state:    StateUnseeded,
	}, nil
}

func (e *Engine) State() State {
	if e == nil {
		return StateClosed
	}
	return e.state
}

func (e *Engine) PolicyID() normalize.Hash {
	if e == nil {
		return normalize.Hash{}
	}
	return e.policyID
}

func (e *Engine) Buffered() (messages uint32, bytes uint64) {
	if e == nil || e.epoch == nil {
		return 0, 0
	}
	return uint32(len(e.epoch.buffer)), e.epoch.bufferBytes
}

func (e *Engine) CurrentEvidence() (EpochEvidence, bool) {
	if e == nil || e.epoch == nil {
		return EpochEvidence{}, false
	}
	return cloneEvidence(e.epoch.evidence), true
}

func (e *Engine) Accept(ctx context.Context, event normalize.BookUpdateV1, encodedBytes uint64) (Result, error) {
	if e == nil {
		return Result{State: StateClosed}, ErrClosed
	}
	if err := checkContext(ctx); err != nil {
		return e.cancel(err)
	}
	if e.terminal {
		return Result{State: StateClosed}, ErrClosed
	}
	if err := e.validateUpdate(event); err != nil {
		return e.failCurrent(closeReasonForLevelError(err), err)
	}
	coordinate := metadataCoordinate(event.Metadata)
	result := Result{}
	if e.epoch == nil {
		if e.state == StateClosed && !e.allowRestart {
			return Result{State: StateClosed}, ErrClosed
		}
		e.startEpoch(coordinate, CloseReason("initial"))
		e.allowRestart = false
	} else if e.state == StateClosed {
		if !e.allowRestart && sameRawEpoch(e.epoch.evidence.LastObservedRaw, coordinate) {
			return Result{State: StateClosed}, ErrClosed
		}
		reason := CloseReconnect
		if sameRawEpoch(e.epoch.evidence.LastObservedRaw, coordinate) {
			reason = CloseRawDiscontinuity
		}
		e.startEpoch(coordinate, reason)
		e.allowRestart = false
	} else if e.epoch.evidence.HasObservedRaw && !sameRawEpoch(e.epoch.evidence.LastObservedRaw, coordinate) {
		closed := e.closeCurrent(CloseReconnect, coordinate, true)
		result.ClosedEpochs = append(result.ClosedEpochs, closed)
		e.startEpoch(coordinate, CloseReconnect)
	} else if e.epoch.evidence.HasObservedRaw && compareCoordinate(e.epoch.evidence.LastObservedRaw, coordinate) >= 0 {
		closed := e.closeCurrent(CloseRawDiscontinuity, coordinate, true)
		result.ClosedEpochs = append(result.ClosedEpochs, closed)
		e.startEpoch(coordinate, CloseRawDiscontinuity)
		appendResult, err := e.appendBuffered(event, encodedBytes)
		result.ClosedEpochs = append(result.ClosedEpochs, appendResult.ClosedEpochs...)
		result.State = e.state
		return result.clone(), errors.Join(ErrRawDiscontinuity, err)
	}

	if e.state == StateBuffering {
		appendResult, err := e.appendBuffered(event, encodedBytes)
		result.ClosedEpochs = append(result.ClosedEpochs, appendResult.ClosedEpochs...)
		result.State = e.state
		return result.clone(), err
	}
	if e.state != StateLive {
		return e.failCurrent(CloseMalformedInput, ErrInvalidTransition)
	}
	liveResult, err := e.acceptLive(event, encodedBytes)
	result.Output = liveResult.Output
	result.IgnoredStale = liveResult.IgnoredStale
	result.ClosedEpochs = append(result.ClosedEpochs, liveResult.ClosedEpochs...)
	result.State = e.state
	return result.clone(), err
}

func (e *Engine) Seed(ctx context.Context) (Result, error) {
	if e == nil || e.terminal {
		return Result{State: StateClosed}, ErrClosed
	}
	if err := checkContext(ctx); err != nil {
		return e.cancel(err)
	}
	if e.state != StateBuffering || e.epoch == nil {
		return Result{State: e.state}, ErrInvalidTransition
	}
	if len(e.epoch.buffer) == 0 {
		return Result{State: e.state, SnapshotFetches: e.epoch.evidence.SnapshotFetches}, nil
	}

	for {
		first := e.epoch.buffer[0].event.FirstSequence
		snapshot, err := e.snapshot(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return e.cancel(err)
			}
			if errors.Is(err, ErrSnapshotLimit) || errors.Is(err, ErrSnapshotBytes) || errors.Is(err, ErrInvalidSnapshot) ||
				errors.Is(err, ErrLevelLimit) || errors.Is(err, ErrCrossedBook) || errors.Is(err, ErrNegativeLevel) {
				reason := CloseMalformedInput
				switch {
				case errors.Is(err, ErrSnapshotLimit):
					reason = CloseSnapshotLimit
				case errors.Is(err, ErrSnapshotBytes):
					reason = CloseSnapshotBytes
				case errors.Is(err, ErrLevelLimit):
					reason = CloseLevelLimit
				case errors.Is(err, ErrCrossedBook):
					reason = CloseCrossedBook
				}
				return e.failCurrent(reason, err)
			}
			return Result{State: e.state, SnapshotFetches: e.epoch.evidence.SnapshotFetches}, err
		}
		if e.policy.SnapshotBehind(snapshot.LastSequence, first) {
			e.epoch.evidence.SnapshotAttempts[e.epoch.pending.attempt].Behind = true
			e.epoch.pending = nil
			if e.epoch.evidence.SnapshotFetches >= e.config.Bounds.MaxSnapshotFetches {
				return e.failCurrent(CloseSnapshotLimit, fmt.Errorf("%w: %v", ErrSnapshotLimit, ErrSnapshotBehind))
			}
			continue
		}

		candidate, err := prepareSnapshot(e.config, snapshot)
		if err != nil {
			reason := CloseMalformedInput
			if errors.Is(err, ErrLevelLimit) {
				reason = CloseLevelLimit
			} else if errors.Is(err, ErrCrossedBook) {
				reason = CloseCrossedBook
			}
			return e.failCurrent(reason, err)
		}

		e.discardSnapshotStale(snapshot.LastSequence)
		if len(e.epoch.buffer) == 0 {
			return Result{State: e.state, SnapshotFetches: e.epoch.evidence.SnapshotFetches}, nil
		}
		if e.policy.First(snapshot.LastSequence, e.epoch.buffer[0].event) != SequenceApply {
			return e.splitBuffered(0, CloseSeedBoundary, ErrBoundary)
		}

		e.epoch.book = candidate
		e.epoch.localSequence = snapshot.LastSequence
		e.epoch.evidence.InitialSnapshotID = snapshot.Identity
		e.epoch.evidence.InitialSnapshotRaw = snapshot.RawCoordinate
		e.epoch.evidence.HasInitialSnapshot = true
		e.transition(StateSeeded, snapshot.RawCoordinate, true)

		for index, buffered := range e.epoch.buffer {
			action := e.policy.Next(e.epoch.localSequence, buffered.event)
			if index == 0 {
				action = e.policy.First(snapshot.LastSequence, buffered.event)
			}
			switch action {
			case SequenceStale:
				e.acceptInput(buffered.event, false)
			case SequenceGap:
				return e.splitBuffered(index, CloseForwardGap, ErrSequenceGap)
			case SequenceApply:
				if err := applyEventAtomically(e.config, &e.epoch.book, buffered.event); err != nil {
					reason := closeReasonForLevelError(err)
					return e.failCurrent(reason, err)
				}
				e.epoch.localSequence = buffered.event.LastSequence
				e.acceptInput(buffered.event, true)
			default:
				return e.failCurrent(CloseMalformedInput, ErrInvalidTransition)
			}
		}
		e.clearBuffer()
		e.epoch.pending = nil
		e.transition(StateLive, e.epoch.evidence.LastAcceptedRaw, true)
		output, err := e.emit()
		if err != nil {
			return e.failCurrent(CloseOutputLimit, err)
		}
		return Result{
			State:           e.state,
			Output:          &output,
			SnapshotFetches: e.epoch.evidence.SnapshotFetches,
		}.clone(), nil
	}
}

func (e *Engine) Discontinuity(ctx context.Context) (Result, error) {
	if e == nil || e.terminal {
		return Result{State: StateClosed}, ErrClosed
	}
	if err := checkContext(ctx); err != nil {
		return e.cancel(err)
	}
	result := Result{State: StateClosed}
	if e.epoch != nil && e.state != StateClosed {
		coordinate := e.epoch.evidence.LastObservedRaw
		result.ClosedEpochs = append(result.ClosedEpochs, e.closeCurrent(CloseRawDiscontinuity, coordinate, e.epoch.evidence.HasObservedRaw))
	}
	e.allowRestart = true
	e.state = StateClosed
	return result.clone(), nil
}

func (e *Engine) Close(ctx context.Context) (Result, error) {
	if e == nil {
		return Result{State: StateClosed}, ErrClosed
	}
	if err := checkContext(ctx); err != nil {
		return e.cancel(err)
	}
	if e.terminal {
		return Result{State: StateClosed}, nil
	}
	result := Result{State: StateClosed}
	if e.epoch != nil && e.state != StateClosed {
		coordinate := e.epoch.evidence.LastObservedRaw
		result.ClosedEpochs = append(result.ClosedEpochs, e.closeCurrent(CloseExplicit, coordinate, e.epoch.evidence.HasObservedRaw))
	}
	e.terminal = true
	e.state = StateClosed
	return result.clone(), nil
}

func (e *Engine) validateUpdate(event normalize.BookUpdateV1) error {
	if err := event.Validate(); err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidUpdate, err)
	}
	metadata := event.Metadata
	if metadata.SourceID != e.config.SourceID || metadata.ChannelID != e.config.UpdateChannelID ||
		metadata.InstrumentUID != e.config.Instrument.InstrumentUID || metadata.CatalogSnapshotID != e.config.CatalogSnapshotID ||
		metadata.MapperVersion != e.config.MapperVersion || metadata.MapperBindingID != e.config.MapperBindingID {
		return ErrIdentityDiscontinuity
	}
	coordinate := metadataCoordinate(metadata)
	if !validRawCoordinate(coordinate) || coordinate.EpochKind != normalize.ConnectionEpoch {
		return ErrInvalidUpdate
	}
	if _, _, err := prepareChanges(e.config, event.Bids, normalize.SideBuy); err != nil {
		return err
	}
	if _, _, err := prepareChanges(e.config, event.Asks, normalize.SideSell); err != nil {
		return err
	}
	return nil
}

func (e *Engine) appendBuffered(event normalize.BookUpdateV1, encodedBytes uint64) (Result, error) {
	coordinate := metadataCoordinate(event.Metadata)
	if encodedBytes == 0 || encodedBytes > e.config.Bounds.MaxBufferedBytes ||
		e.epoch.bufferBytes > math.MaxUint64-encodedBytes || e.epoch.bufferBytes+encodedBytes > e.config.Bounds.MaxBufferedBytes {
		e.observe(coordinate)
		return e.failCurrent(CloseBufferOverflow, ErrBufferBytes)
	}
	if len(e.epoch.buffer) >= int(e.config.Bounds.MaxBufferedMessages) {
		e.observe(coordinate)
		return e.failCurrent(CloseBufferOverflow, ErrBufferMessages)
	}
	received := event.Metadata.ReceivedTimeNS
	minimum, maximum := received, received
	if len(e.epoch.buffer) != 0 {
		minimum = min(e.epoch.bufferMinNS, received)
		maximum = max(e.epoch.bufferMaxNS, received)
	}
	if maximum-minimum > e.config.Bounds.MaxBufferSpanNS {
		e.observe(coordinate)
		return e.failCurrent(CloseBufferOverflow, ErrBufferTime)
	}
	cloned := cloneUpdate(event)
	e.epoch.buffer = append(e.epoch.buffer, bufferedUpdate{event: cloned, bytes: encodedBytes})
	e.epoch.bufferBytes += encodedBytes
	e.epoch.bufferMinNS = minimum
	e.epoch.bufferMaxNS = maximum
	e.observe(coordinate)
	return Result{State: e.state}, nil
}

func (e *Engine) acceptLive(event normalize.BookUpdateV1, encodedBytes uint64) (Result, error) {
	if encodedBytes == 0 || encodedBytes > e.config.Bounds.MaxBufferedBytes {
		return e.failCurrent(CloseMalformedInput, ErrBufferBytes)
	}
	action := e.policy.Next(e.epoch.localSequence, event)
	switch action {
	case SequenceStale:
		e.observe(metadataCoordinate(event.Metadata))
		e.acceptInput(event, false)
		return Result{State: e.state, IgnoredStale: true}, nil
	case SequenceGap:
		closed := e.closeCurrent(CloseForwardGap, metadataCoordinate(event.Metadata), true)
		result := Result{ClosedEpochs: []EpochEvidence{closed}}
		e.startEpoch(metadataCoordinate(event.Metadata), CloseForwardGap)
		appendResult, appendErr := e.appendBuffered(event, encodedBytes)
		result.ClosedEpochs = append(result.ClosedEpochs, appendResult.ClosedEpochs...)
		result.State = e.state
		return result, errors.Join(ErrSequenceGap, appendErr)
	case SequenceApply:
		coordinate := metadataCoordinate(event.Metadata)
		e.observe(coordinate)
		if e.epoch.evidence.OutputCount >= e.config.Bounds.MaxOutputs {
			return e.failCurrent(CloseOutputLimit, ErrOutputLimit)
		}
		if err := applyEventAtomically(e.config, &e.epoch.book, event); err != nil {
			return e.failCurrent(closeReasonForLevelError(err), err)
		}
		e.epoch.localSequence = event.LastSequence
		e.acceptInput(event, true)
		output, err := e.emit()
		if err != nil {
			return e.failCurrent(CloseOutputLimit, err)
		}
		return Result{State: e.state, Output: &output}, nil
	default:
		return e.failCurrent(CloseMalformedInput, ErrInvalidTransition)
	}
}

func (e *Engine) snapshot(ctx context.Context) (SnapshotObservation, error) {
	if e.epoch.pending != nil {
		return cloneSnapshot(e.epoch.pending.observation), nil
	}
	if e.epoch.evidence.SnapshotFetches >= e.config.Bounds.MaxSnapshotFetches {
		return SnapshotObservation{}, ErrSnapshotLimit
	}
	e.epoch.evidence.SnapshotFetches++
	observation, err := e.fetcher.Fetch(ctx)
	if contextErr := ctx.Err(); contextErr != nil {
		return SnapshotObservation{}, contextErr
	}
	if err != nil {
		return SnapshotObservation{}, err
	}
	if err := observation.Validate(); err != nil {
		return SnapshotObservation{}, err
	}
	if observation.SourceID != e.config.SourceID || observation.ChannelID != e.config.SnapshotChannelID ||
		observation.InstrumentUID != e.config.Instrument.InstrumentUID || observation.RawCoordinate.EpochKind != normalize.PollCycleEpoch {
		return SnapshotObservation{}, ErrInvalidSnapshot
	}
	if observation.PayloadBytes > e.config.Bounds.MaxSnapshotBytes {
		return SnapshotObservation{}, ErrSnapshotBytes
	}
	if uint32(len(observation.Bids)) > e.config.Bounds.MaxLevelsPerSide ||
		uint32(len(observation.Asks)) > e.config.Bounds.MaxLevelsPerSide {
		return SnapshotObservation{}, ErrLevelLimit
	}
	observation = cloneSnapshot(observation)
	e.epoch.evidence.SnapshotAttempts = append(e.epoch.evidence.SnapshotAttempts, SnapshotAttemptEvidence{
		Identity:      observation.Identity,
		RawCoordinate: observation.RawCoordinate,
		LastSequence:  observation.LastSequence,
	})
	e.epoch.pending = &pendingSnapshot{observation: observation, attempt: len(e.epoch.evidence.SnapshotAttempts) - 1}
	return cloneSnapshot(observation), nil
}

func (e *Engine) discardSnapshotStale(snapshotLast uint64) {
	count := 0
	for count < len(e.epoch.buffer) && e.epoch.buffer[count].event.LastSequence <= snapshotLast {
		e.acceptInput(e.epoch.buffer[count].event, false)
		count++
	}
	if count == 0 {
		return
	}
	e.epoch.buffer = slices.Delete(e.epoch.buffer, 0, count)
	e.recalculateBuffer()
}

func (e *Engine) splitBuffered(index int, reason CloseReason, cause error) (Result, error) {
	remaining := make([]bufferedUpdate, len(e.epoch.buffer)-index)
	for position, buffered := range e.epoch.buffer[index:] {
		remaining[position] = bufferedUpdate{event: cloneUpdate(buffered.event), bytes: buffered.bytes}
	}
	coordinate := metadataCoordinate(remaining[0].event.Metadata)
	if e.epoch.evidence.HasAcceptedRaw {
		e.epoch.evidence.LastObservedRaw = e.epoch.evidence.LastAcceptedRaw
	} else {
		e.epoch.evidence.HasObservedRaw = false
		e.epoch.evidence.FirstObservedRaw = normalize.RawCoordinate{}
		e.epoch.evidence.LastObservedRaw = normalize.RawCoordinate{}
	}
	closed := e.closeCurrent(reason, coordinate, true)
	result := Result{ClosedEpochs: []EpochEvidence{closed}}
	e.startEpoch(coordinate, reason)
	for _, buffered := range remaining {
		appendResult, err := e.appendBuffered(buffered.event, buffered.bytes)
		result.ClosedEpochs = append(result.ClosedEpochs, appendResult.ClosedEpochs...)
		if err != nil {
			result.State = e.state
			return result.clone(), errors.Join(cause, err)
		}
	}
	result.State = e.state
	return result.clone(), cause
}

func (e *Engine) startEpoch(first normalize.RawCoordinate, reason CloseReason) {
	identity := reconstructionEpochIdentity(e.policyID, e.parentID, reason, first)
	inputDigest := newDigestEncoder("enable-labs/orderbook/input-range", 1)
	inputDigest.fixed(e.policyID[:])
	inputDigest.fixed(identity[:])
	outputDigest := newDigestEncoder("enable-labs/orderbook/output-range", 1)
	outputDigest.fixed(identity[:])
	e.epoch = &reconstructionEpoch{
		evidence: EpochEvidence{
			ReconstructionEpochID: identity,
			ParentEpochID:         e.parentID,
			SourceID:              e.config.SourceID,
			UpdateChannelID:       e.config.UpdateChannelID,
			SnapshotChannelID:     e.config.SnapshotChannelID,
			InstrumentUID:         e.config.Instrument.InstrumentUID,
			CatalogSnapshotID:     e.config.CatalogSnapshotID,
			MapperVersion:         e.config.MapperVersion,
			MapperBindingID:       e.config.MapperBindingID,
			PolicyName:            e.policy.Name(),
			PolicyVersion:         e.policy.Version(),
			PolicyID:              e.policyID,
			State:                 StateUnseeded,
			Transitions:           []Transition{{State: StateUnseeded}},
		},
		inputDigest:  inputDigest,
		outputDigest: outputDigest,
	}
	e.state = StateUnseeded
	e.transition(StateBuffering, first, true)
}

func (e *Engine) transition(state State, coordinate normalize.RawCoordinate, hasCoordinate bool) {
	e.state = state
	e.epoch.evidence.State = state
	e.epoch.evidence.Transitions = append(e.epoch.evidence.Transitions, Transition{
		State:         state,
		HasCoordinate: hasCoordinate,
		Coordinate:    coordinate,
	})
}

func (e *Engine) observe(coordinate normalize.RawCoordinate) {
	if !e.epoch.evidence.HasObservedRaw {
		e.epoch.evidence.FirstObservedRaw = coordinate
		e.epoch.evidence.HasObservedRaw = true
	}
	e.epoch.evidence.LastObservedRaw = coordinate
}

func (e *Engine) acceptInput(event normalize.BookUpdateV1, applied bool) {
	coordinate := metadataCoordinate(event.Metadata)
	if !e.epoch.evidence.HasAcceptedRaw {
		e.epoch.evidence.FirstAcceptedRaw = coordinate
		e.epoch.evidence.HasAcceptedRaw = true
	}
	e.epoch.evidence.LastAcceptedRaw = coordinate
	e.epoch.evidence.AcceptedMessages++
	if applied {
		e.epoch.evidence.AppliedMessages++
	} else {
		e.epoch.evidence.StaleMessages++
	}
	appendInputIdentity(e.epoch.inputDigest, event)
}

func (e *Engine) emit() (BookSnapshotV1, error) {
	if e.epoch.evidence.OutputCount >= e.config.Bounds.MaxOutputs {
		return BookSnapshotV1{}, ErrOutputLimit
	}
	if !e.epoch.evidence.HasAcceptedRaw || !e.epoch.evidence.HasInitialSnapshot || e.state != StateLive {
		return BookSnapshotV1{}, ErrInvalidTransition
	}
	output := BookSnapshotV1{
		SchemaName:                BookSnapshotSchemaName,
		SchemaVersion:             BookSnapshotSchemaVersion,
		ProjectionEncodingVersion: ProjectionEncodingVersion,
		SourceID:                  e.config.SourceID,
		UpdateChannelID:           e.config.UpdateChannelID,
		SnapshotChannelID:         e.config.SnapshotChannelID,
		InstrumentUID:             e.config.Instrument.InstrumentUID,
		BaseAssetID:               e.config.Instrument.BaseAssetID,
		QuoteAssetID:              e.config.Instrument.QuoteAssetID,
		CatalogSnapshotID:         e.config.CatalogSnapshotID,
		MapperVersion:             e.config.MapperVersion,
		MapperBindingID:           e.config.MapperBindingID,
		PolicyName:                e.policy.Name(),
		PolicyVersion:             e.policy.Version(),
		PolicyID:                  e.policyID,
		ReconstructionEpochID:     e.epoch.evidence.ReconstructionEpochID,
		InitialSnapshotID:         e.epoch.evidence.InitialSnapshotID,
		InitialSnapshotCoordinate: e.epoch.evidence.InitialSnapshotRaw,
		LastSequence:              e.epoch.localSequence,
		OutputOrdinal:             e.epoch.evidence.OutputCount + 1,
		InputRange: InputRange{
			First: e.epoch.evidence.FirstAcceptedRaw,
			Last:  e.epoch.evidence.LastAcceptedRaw,
			Count: e.epoch.evidence.AcceptedMessages,
			Hash:  e.epoch.inputDigest.sum(),
		},
		Bids: projectedLevels(e.epoch.book.bids, true),
		Asks: projectedLevels(e.epoch.book.asks, false),
	}
	output.ProjectionHash = projectionIdentity(output)
	if err := output.Validate(); err != nil {
		return BookSnapshotV1{}, err
	}
	e.epoch.evidence.OutputCount++
	e.epoch.outputDigest.fixed(output.ProjectionHash[:])
	e.epoch.evidence.OutputHash = e.epoch.outputDigest.sum()
	e.epoch.evidence.LastOutputHash = output.ProjectionHash
	e.epoch.evidence.LastSequence = e.epoch.localSequence
	return output, nil
}

func (e *Engine) closeCurrent(reason CloseReason, coordinate normalize.RawCoordinate, hasCoordinate bool) EpochEvidence {
	if reason == CloseForwardGap || reason == CloseSeedBoundary {
		e.transition(StateGap, coordinate, hasCoordinate)
	}
	e.transition(StateClosed, coordinate, hasCoordinate)
	e.epoch.evidence.CloseReason = reason
	e.parentID = e.epoch.evidence.ReconstructionEpochID
	e.state = StateClosed
	return cloneEvidence(e.epoch.evidence)
}

func (e *Engine) failCurrent(reason CloseReason, cause error) (Result, error) {
	result := Result{State: e.state}
	if e.epoch != nil && e.state != StateClosed {
		coordinate := e.epoch.evidence.LastObservedRaw
		result.ClosedEpochs = append(result.ClosedEpochs, e.closeCurrent(reason, coordinate, e.epoch.evidence.HasObservedRaw))
	} else if e.epoch == nil {
		e.state = StateClosed
	}
	result.State = e.state
	return result.clone(), cause
}

func (e *Engine) cancel(cause error) (Result, error) {
	if e == nil {
		return Result{State: StateClosed}, cause
	}
	result, _ := e.failCurrent(CloseCancelled, cause)
	e.terminal = true
	e.state = StateClosed
	result.State = StateClosed
	return result.clone(), cause
}

func (e *Engine) clearBuffer() {
	e.epoch.buffer = nil
	e.epoch.bufferBytes = 0
	e.epoch.bufferMinNS = 0
	e.epoch.bufferMaxNS = 0
}

func (e *Engine) recalculateBuffer() {
	e.epoch.bufferBytes = 0
	e.epoch.bufferMinNS = 0
	e.epoch.bufferMaxNS = 0
	for index, buffered := range e.epoch.buffer {
		e.epoch.bufferBytes += buffered.bytes
		received := buffered.event.Metadata.ReceivedTimeNS
		if index == 0 {
			e.epoch.bufferMinNS = received
			e.epoch.bufferMaxNS = received
			continue
		}
		e.epoch.bufferMinNS = min(e.epoch.bufferMinNS, received)
		e.epoch.bufferMaxNS = max(e.epoch.bufferMaxNS, received)
	}
}

func cloneUpdate(event normalize.BookUpdateV1) normalize.BookUpdateV1 {
	event.Metadata.QualityFlags = slices.Clone(event.Metadata.QualityFlags)
	event.Bids = slices.Clone(event.Bids)
	event.Asks = slices.Clone(event.Asks)
	return event
}

func sameRawEpoch(left, right normalize.RawCoordinate) bool {
	return left.SourceID == right.SourceID && left.ChannelID == right.ChannelID && left.EpochKind == right.EpochKind && left.EpochID == right.EpochID
}

func compareCoordinate(left, right normalize.RawCoordinate) int {
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

func closeReasonForLevelError(err error) CloseReason {
	switch {
	case errors.Is(err, ErrLevelLimit):
		return CloseLevelLimit
	case errors.Is(err, ErrCrossedBook):
		return CloseCrossedBook
	default:
		return CloseMalformedInput
	}
}
