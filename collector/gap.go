package collector

import (
	"context"
	"fmt"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
)

// GapObservation is the exact durable close boundary used to create one
// quality gap. NativeSymbol is populated for per-instrument REST poll streams
// and intentionally empty for the source-level merged WebSocket stream.
type GapObservation struct {
	SourceID           string
	ChannelID          string
	NativeSymbol       string
	Epoch              capture.StreamEpoch
	LastArrivalOrdinal uint64
	Interval           capture.BlindInterval
}

// GapRecorder owns persistent gap identity and lifecycle. Production binds it
// to PostgreSQL; the collector retains only unresolved IDs needed to mark the
// first subsequently durable current-state observation.
type GapRecorder interface {
	OpenGap(context.Context, string, string, string) (string, bool, error)
	RecordGap(context.Context, GapObservation) (string, error)
	ResolveGap(context.Context, string, int64) error
}

type gapKey struct {
	channel string
	native  string
}

func newGapKey(channel, native string) gapKey {
	if channel != binance.SpotDepthChannel {
		native = ""
	}
	return gapKey{channel: channel, native: native}
}

func (s *DurableRawSink) attachGapRecorder(ctx context.Context, recorder GapRecorder, symbols []SymbolConfig) error {
	if recorder == nil {
		return fmt.Errorf("%w: persistent gap recorder is required", ErrConfiguration)
	}
	tuples := []gapKey{
		newGapKey(binance.SpotRawChannel, ""),
		newGapKey(binance.SpotExchangeInfoChannel, ""),
	}
	for _, symbol := range symbols {
		tuples = append(tuples, newGapKey(binance.SpotDepthChannel, symbol.NativeID))
	}
	pending := make(map[gapKey]string, len(tuples))
	for _, tuple := range tuples {
		if _, seen := pending[tuple]; seen {
			continue
		}
		id, found, err := recorder.OpenGap(ctx, binance.SpotSourceID, tuple.channel, tuple.native)
		if err != nil {
			return fmt.Errorf("collector: recover open gap for %s/%s: %w", tuple.channel, tuple.native, err)
		}
		if found {
			if id == "" {
				return fmt.Errorf("%w: recovered gap identity is empty", ErrConfiguration)
			}
			pending[tuple] = id
		}
	}
	s.mu.Lock()
	s.gaps = recorder
	s.pendingGaps = pending
	recovered := s.recoveredObservations
	s.recoveredObservations = make(map[gapKey]int64)
	s.mu.Unlock()
	for key, resolvedNS := range recovered {
		id := pending[key]
		if id == "" {
			continue
		}
		if err := recorder.ResolveGap(ctx, id, resolvedNS); err != nil {
			return fmt.Errorf("collector: resolve recovered gap for %s/%s: %w", key.channel, key.native, err)
		}
		s.mu.Lock()
		if s.pendingGaps[key] == id {
			delete(s.pendingGaps, key)
		}
		s.mu.Unlock()
	}
	return nil
}

func (s *DurableRawSink) recordGap(ctx context.Context, state *epochState, close capture.EpochClose) error {
	if close.BlindInterval == nil {
		return nil
	}
	terminal := close.Terminal.Envelope
	native := ""
	if terminal.NativeSymbol.Valid {
		native = terminal.NativeSymbol.Value
	}
	key := newGapKey(state.key.channel, native)
	s.mu.Lock()
	recorder := s.gaps
	_, pending := s.pendingGaps[key]
	s.mu.Unlock()
	if recorder == nil {
		return fmt.Errorf("%w: persistent gap recorder is unavailable", ErrRawSink)
	}
	if pending {
		return nil
	}
	id, err := recorder.RecordGap(ctx, GapObservation{
		SourceID: binance.SpotSourceID, ChannelID: state.key.channel, NativeSymbol: key.native,
		Epoch: close.Commit.Epoch, LastArrivalOrdinal: close.Commit.LastArrivalOrdinal,
		Interval: *close.BlindInterval,
	})
	if err != nil {
		return fmt.Errorf("%w: record persistent gap: %v", ErrRawSink, err)
	}
	if id == "" {
		return fmt.Errorf("%w: persistent gap recorder returned an empty identity", ErrRawSink)
	}
	s.mu.Lock()
	if existing := s.pendingGaps[key]; existing == "" {
		s.pendingGaps[key] = id
	}
	s.mu.Unlock()
	return nil
}

func (s *DurableRawSink) resolveGapAfterWrite(ctx context.Context, envelope capture.EnvelopeV1) error {
	if !currentStateObservation(envelope) {
		return nil
	}
	native := ""
	if envelope.NativeSymbol.Valid {
		native = envelope.NativeSymbol.Value
	}
	key := newGapKey(envelope.ChannelOrEndpoint, native)
	s.mu.Lock()
	recorder := s.gaps
	id := s.pendingGaps[key]
	s.mu.Unlock()
	if id == "" {
		return nil
	}
	if recorder == nil {
		return fmt.Errorf("%w: persistent gap recorder is unavailable", ErrRawSink)
	}
	if err := recorder.ResolveGap(ctx, id, envelope.ReceivedWallTimeNS); err != nil {
		return fmt.Errorf("%w: resolve persistent gap: %v", ErrRawSink, err)
	}
	s.mu.Lock()
	if s.pendingGaps[key] == id {
		delete(s.pendingGaps, key)
	}
	s.mu.Unlock()
	return nil
}

func (s *DurableRawSink) retainRecoveredObservation(envelope capture.EnvelopeV1) {
	if !currentStateObservation(envelope) {
		return
	}
	native := ""
	if envelope.NativeSymbol.Valid {
		native = envelope.NativeSymbol.Value
	}
	key := newGapKey(envelope.ChannelOrEndpoint, native)
	s.mu.Lock()
	if prior := s.recoveredObservations[key]; prior == 0 || envelope.ReceivedWallTimeNS < prior {
		s.recoveredObservations[key] = envelope.ReceivedWallTimeNS
	}
	s.mu.Unlock()
}

func currentStateObservation(envelope capture.EnvelopeV1) bool {
	switch envelope.RecordKind {
	case capture.RecordKindWebSocket:
		return envelope.ChannelOrEndpoint == binance.SpotRawChannel
	case capture.RecordKindREST:
		return (envelope.ChannelOrEndpoint == binance.SpotDepthChannel || envelope.ChannelOrEndpoint == binance.SpotExchangeInfoChannel) &&
			envelope.TerminalOutcome == capture.TerminalObserved && envelope.HTTPStatusOrWSState.Valid && envelope.HTTPStatusOrWSState.Value == "200"
	default:
		return false
	}
}
