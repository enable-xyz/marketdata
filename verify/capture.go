package verify

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/objectstore"
	nativereplay "github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/segment"
)

type captureRuntime struct {
	objects    objectstore.Client
	catalog    PublicationCatalog
	ws         binance.SpotWSConnector
	rest       binance.SpotRESTClient
	clock      capture.Clock
	advance    func() error
	now        func() time.Time
	wsEpoch    [16]byte
	restEpochs [][16]byte
}

type captureResult struct {
	opportunities []capture.OpportunityRecord
}

type aggregateCaptureBudget struct {
	mu          sync.Mutex
	maxMessages int
	maxBytes    int64
	messages    int
	bytes       int64
}

func (b *aggregateCaptureBudget) consume(payloadBytes int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.messages >= b.maxMessages || payloadBytes > b.maxBytes-b.bytes {
		return false
	}
	b.messages++
	b.bytes += payloadBytes
	return true
}

func (b *aggregateCaptureBudget) release(payloadBytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.messages--
	b.bytes -= payloadBytes
}

func (b *aggregateCaptureBudget) closeRequired() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.messages >= b.maxMessages-1 || b.bytes >= b.maxBytes
}

const (
	spotTradeObserved uint8 = 1 << iota
	spotDepthObserved
	spotQuoteObserved
	spotTickerObserved
	spotAllDataObserved = spotTradeObserved | spotDepthObserved | spotQuoteObserved | spotTickerObserved
)

type spotEvidenceInventory struct {
	mu                    sync.Mutex
	ackOnce               sync.Once
	ackReady              chan struct{}
	symbols               map[string]uint8
	ack                   bool
	heartbeat             bool
	requireSnapshotBridge bool
	snapshotReady         bool
	depthAfterSnapshot    map[string]bool
}

func newSpotEvidenceInventory(symbols []string, requireSnapshotBridge bool) *spotEvidenceInventory {
	inventory := &spotEvidenceInventory{
		ackReady:              make(chan struct{}),
		symbols:               make(map[string]uint8, len(symbols)),
		requireSnapshotBridge: requireSnapshotBridge,
		depthAfterSnapshot:    make(map[string]bool, len(symbols)),
	}
	for _, symbol := range symbols {
		inventory.symbols[strings.ToUpper(symbol)] = 0
	}
	return inventory
}

func (i *spotEvidenceInventory) markSnapshotReady() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.snapshotReady = true
}

func (i *spotEvidenceInventory) waitForAcknowledgement(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-i.ackReady:
		return nil
	}
}

func (i *spotEvidenceInventory) observe(envelope capture.EnvelopeV1) {
	i.mu.Lock()
	defer i.mu.Unlock()
	if envelope.RecordKind == capture.RecordKindControl && envelope.ControlKind.Valid {
		switch envelope.ControlKind.Value {
		case capture.ControlHeartbeat:
			i.heartbeat = true
			return
		case capture.ControlAcknowledgement:
			i.ack = true
			i.ackOnce.Do(func() { close(i.ackReady) })
			return
		}
	}
	if envelope.PayloadEncoding != capture.PayloadEncodingJSON {
		return
	}
	var object map[string]json.RawMessage
	if json.Unmarshal(envelope.RawPayload, &object) != nil {
		return
	}
	if _, ok := object["result"]; ok {
		i.ack = true
		i.ackOnce.Do(func() { close(i.ackReady) })
		return
	}
	var symbol string
	if json.Unmarshal(object["s"], &symbol) != nil {
		return
	}
	symbol = strings.ToUpper(symbol)
	if _, ok := i.symbols[symbol]; !ok {
		return
	}
	var event string
	_ = json.Unmarshal(object["e"], &event)
	switch event {
	case "trade":
		i.symbols[symbol] |= spotTradeObserved
	case "depthUpdate":
		i.symbols[symbol] |= spotDepthObserved
		if i.snapshotReady {
			i.depthAfterSnapshot[symbol] = true
		}
	case "24hrTicker":
		i.symbols[symbol] |= spotTickerObserved
	default:
		if _, updateID := object["u"]; updateID {
			i.symbols[symbol] |= spotQuoteObserved
		}
	}
}

func (i *spotEvidenceInventory) complete() bool {
	if i == nil {
		return false
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.ack || !i.heartbeat || (i.requireSnapshotBridge && !i.snapshotReady) {
		return false
	}
	for symbol, observed := range i.symbols {
		if observed != spotAllDataObserved ||
			(i.requireSnapshotBridge && !i.depthAfterSnapshot[symbol]) {
			return false
		}
	}
	return true
}

type boundedSpoolSink struct {
	mu          sync.Mutex
	writer      *segment.Writer
	ready       []segment.ReadySegment
	budget      *aggregateCaptureBudget
	inventory   *spotEvidenceInventory
	lastOrdinal uint64
	closed      bool
}

func (s *boundedSpoolSink) WriteRaw(ctx context.Context, envelope capture.EnvelopeV1) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(envelope)
}

func (s *boundedSpoolSink) writeLocked(envelope capture.EnvelopeV1) error {
	if s.closed {
		return errors.New("verify: raw sink is closed")
	}
	payloadBytes := int64(len(envelope.RawPayload))
	if s.budget == nil || !s.budget.consume(payloadBytes) {
		return capture.ErrSinkFull
	}
	record, err := envelope.ToSegment()
	if err != nil {
		s.budget.release(payloadBytes)
		return err
	}
	ready, err := s.writer.Write(record)
	if err != nil {
		s.budget.release(payloadBytes)
		return err
	}
	if ready != nil {
		s.ready = append(s.ready, *ready)
	}
	if s.inventory != nil {
		s.inventory.observe(envelope)
	}
	s.lastOrdinal = envelope.ArrivalOrdinal
	return nil
}

func (s *boundedSpoolSink) Commit(ctx context.Context, commit capture.EpochCommit) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if commit.LastArrivalOrdinal != s.lastOrdinal {
		return errors.New("verify: capture commit does not match durable spool order")
	}
	return nil
}

func (s *boundedSpoolSink) CloseEpoch(ctx context.Context, closeRecord capture.EpochClose) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if closeRecord.Commit.LastArrivalOrdinal != closeRecord.Terminal.Envelope.ArrivalOrdinal ||
		closeRecord.Terminal.Envelope.ArrivalOrdinal != s.lastOrdinal+1 {
		return errors.New("verify: terminal control coordinate is not consecutive")
	}
	if err := s.writeLocked(closeRecord.Terminal.Envelope); err != nil {
		return err
	}
	ready, err := s.writer.EndEpoch()
	if err != nil {
		return err
	}
	if ready != nil {
		s.ready = append(s.ready, *ready)
	}
	s.closed = true
	return nil
}

func (s *boundedSpoolSink) closeRequired() bool {
	return s.budget == nil || s.budget.closeRequired()
}

func (s *boundedSpoolSink) evidenceComplete() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inventory.complete()
}

func (s *boundedSpoolSink) readySegments() []segment.ReadySegment {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ready)
}

func committedCaptureUsage(
	ctx context.Context,
	objects objectstore.Client,
	publications []catalog.RawSegmentPublication,
) (int, int64, error) {
	if len(publications) == 0 {
		return 0, 0, nil
	}
	descriptors := make([]nativereplay.InputDescriptor, len(publications))
	for index, publication := range publications {
		descriptor, err := nativereplay.NewInputDescriptor(publication)
		if err != nil {
			return 0, 0, err
		}
		descriptors[index] = descriptor
	}
	var messages int
	var payloadBytes int64
	_, err := nativereplay.ReplaySource(ctx, objects, descriptors, nativereplay.DefaultConfig(), func(event nativereplay.Event) error {
		if event.Kind == nativereplay.EventDiscontinuity {
			if event.Discontinuity.Reason == nativereplay.IntegrityReasonNone &&
				(event.Discontinuity.Kind == nativereplay.DiscontinuityEpochBoundary ||
					event.Discontinuity.Kind == nativereplay.DiscontinuityDisconnect) {
				return nil
			}
			return errors.New("verify: committed capture budget replay encountered an integrity discontinuity")
		}
		if event.Kind != nativereplay.EventRecord {
			return errors.New("verify: unknown replay event while rebuilding capture budget")
		}
		envelope, err := capture.EnvelopeV1FromSegment(event.Record)
		if err != nil {
			return err
		}
		messages++
		payloadBytes += int64(len(envelope.RawPayload))
		if payloadBytes < 0 {
			return errors.New("verify: committed capture payload budget overflow")
		}
		return nil
	})
	if err != nil {
		return 0, 0, err
	}
	return messages, payloadBytes, nil
}

func ensureRawCapture(ctx context.Context, cfg config.Config, runtime captureRuntime) (captureResult, error) {
	publisher, err := objectstore.NewPublisher(runtime.objects, runtime.catalog, objectstore.PublisherConfig{
		Verify: objectstore.DefaultVerifyPolicy(),
	})
	if err != nil {
		return captureResult{}, err
	}
	rateBudget, err := binance.NewSpotVenueRateBudget(runtime.clock.Read().MonotonicNS)
	if err != nil {
		return captureResult{}, err
	}
	aggregate := &aggregateCaptureBudget{maxMessages: cfg.Verify.MaxMessages, maxBytes: cfg.Verify.MaxBytes}
	if len(runtime.restEpochs) != len(cfg.Sources[0].Symbols) {
		return captureResult{}, errors.New("verify: REST epoch inventory does not cover every configured symbol")
	}
	wsCommitted, err := channelCommitted(ctx, runtime.catalog, binance.SpotRawChannel, runtime.wsEpoch)
	if err != nil {
		return captureResult{}, err
	}
	allCommitted := wsCommitted
	for index := range runtime.restEpochs {
		committed, err := channelCommitted(ctx, runtime.catalog, binance.SpotDepthChannel, runtime.restEpochs[index])
		if err != nil {
			return captureResult{}, err
		}
		allCommitted = allCommitted && committed
	}
	publications, err := committedPublications(ctx, runtime.catalog)
	if err != nil {
		return captureResult{}, err
	}
	currentPublications := selectRunPublications(publications, runtime.wsEpoch, runtime.restEpochs)
	committedMessages, committedBytes, err := committedCaptureUsage(ctx, runtime.objects, currentPublications)
	if err != nil {
		return captureResult{}, fmt.Errorf("verify: rebuild committed capture budget: %w", err)
	}
	if committedMessages > cfg.Verify.MaxMessages || committedBytes > cfg.Verify.MaxBytes {
		return captureResult{}, errors.New("verify: committed partial capture exceeds configured aggregate budget")
	}
	aggregate.messages = committedMessages
	aggregate.bytes = committedBytes
	if allCommitted {
		return captureResult{}, nil
	}
	captureREST := func(runCtx context.Context, result *captureResult) error {
		for symbolIndex := range cfg.Sources[0].Symbols {
			published, err := ensureChannelPublished(runCtx, cfg, runtime, publisher, rateBudget, aggregate, nil, false, symbolIndex, result)
			if err != nil {
				return err
			}
			if !published {
				return fmt.Errorf("verify: REST segment for symbol %d was not committed", symbolIndex)
			}
		}
		return nil
	}
	if cfg.Verify.Mode == config.VerifyModeFixture {
		result := captureResult{}
		inventory := newSpotEvidenceInventory(cfg.Sources[0].Symbols, false)
		published, err := ensureChannelPublished(ctx, cfg, runtime, publisher, rateBudget, aggregate, inventory, true, 0, &result)
		if err != nil {
			return captureResult{}, err
		}
		if !published {
			return captureResult{}, errors.New("verify: WebSocket segment was not committed")
		}
		if err := captureREST(ctx, &result); err != nil {
			return captureResult{}, err
		}
		return result, nil
	}

	liveCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	type websocketResult struct {
		published bool
		capture   captureResult
		err       error
	}
	liveInventory := newSpotEvidenceInventory(cfg.Sources[0].Symbols, true)
	websocketDone := make(chan websocketResult, 1)
	go func() {
		var result captureResult
		published, err := ensureChannelPublished(liveCtx, cfg, runtime, publisher, rateBudget, aggregate, liveInventory, true, 0, &result)
		if err != nil {
			cancel()
		}
		websocketDone <- websocketResult{published: published, capture: result, err: err}
	}()
	var restResult captureResult
	ackErr := liveInventory.waitForAcknowledgement(liveCtx)
	var restErr error
	if ackErr == nil {
		restErr = captureREST(liveCtx, &restResult)
		if restErr == nil {
			liveInventory.markSnapshotReady()
		}
	} else {
		restErr = fmt.Errorf("verify: waiting for WebSocket acknowledgement before REST snapshot: %w", ackErr)
	}
	if restErr != nil {
		cancel()
	}
	websocket := <-websocketDone
	if restErr != nil || websocket.err != nil {
		return captureResult{}, errors.Join(websocket.err, restErr)
	}
	if !websocket.published {
		return captureResult{}, errors.New("verify: WebSocket segment was not committed")
	}
	websocket.capture.opportunities = append(websocket.capture.opportunities, restResult.opportunities...)
	return websocket.capture, nil
}

func ensureChannelPublished(
	ctx context.Context,
	cfg config.Config,
	runtime captureRuntime,
	publisher *objectstore.Publisher,
	budget capture.RateBudget,
	aggregate *aggregateCaptureBudget,
	inventory *spotEvidenceInventory,
	websocket bool,
	symbolIndex int,
	result *captureResult,
) (bool, error) {
	channel := binance.SpotDepthChannel
	epochKind := segment.EpochPoll
	epoch := runtime.restEpochs[symbolIndex]
	if websocket {
		channel = binance.SpotRawChannel
		epochKind = segment.EpochConnection
		epoch = runtime.wsEpoch
	}
	spool, err := segment.OpenSpool(segment.SpoolConfig{
		Root: cfg.Verify.SpoolRoot, SourceID: binance.SpotSourceID, ChannelID: channel, EpochKind: epochKind, EpochID: epoch,
	})
	if err != nil {
		return false, err
	}
	recovery, err := spool.Recover(segment.RecoveryOptions{FrameBytes: segment.DefaultFrameBytes, WriterVersion: VerifierVersion})
	if err != nil {
		return false, err
	}
	for _, item := range recovery.Items {
		if item.Err != nil || item.State == segment.RecoveryCorrupt || item.State == segment.RecoveryConflicting {
			return false, fmt.Errorf("verify: unresolved spool recovery for %s", channel)
		}
	}
	ready, err := spool.Ready()
	if err != nil {
		return false, err
	}
	for _, candidate := range ready {
		if _, err := publishReady(ctx, publisher, candidate); err != nil {
			return false, err
		}
	}
	committed, err := channelCommitted(ctx, runtime.catalog, channel, epoch)
	if err != nil || committed {
		return committed, err
	}

	writer, err := spool.NewWriter(segment.WriterOptions{
		FrameBytes: segment.DefaultFrameBytes, SegmentBytes: segment.DefaultSegmentBytes,
		MaxAge: segment.DefaultSegmentAge, WriterVersion: VerifierVersion, Now: runtime.now,
	})
	if err != nil {
		return false, err
	}
	sink := &boundedSpoolSink{writer: writer, budget: aggregate}
	if websocket {
		sink.inventory = inventory
	}
	if websocket {
		if err := captureWebSocket(ctx, cfg, runtime, budget, sink, result); err != nil {
			return false, err
		}
	} else if err := captureDepth(ctx, cfg, runtime, budget, sink, result, symbolIndex, epoch); err != nil {
		return false, err
	}
	for _, candidate := range sink.readySegments() {
		if _, err := publishReady(ctx, publisher, candidate); err != nil {
			return false, err
		}
	}
	return channelCommitted(ctx, runtime.catalog, channel, epoch)
}

func captureWebSocket(ctx context.Context, cfg config.Config, runtime captureRuntime, budget capture.RateBudget, sink *boundedSpoolSink, output *captureResult) error {
	source := cfg.Sources[0]
	captureAdapter, err := binance.NewSpotCapture(binance.SpotWSConfig{
		Symbols: source.Symbols, Endpoint: binance.SpotWSEndpoint, RecorderVersion: VerifierVersion,
		Epochs: []capture.StreamEpoch{{Kind: capture.EpochConnection, ID: runtime.wsEpoch}},
	}, runtime.ws, runtime.clock, budget, sink)
	if err != nil {
		return err
	}
	result, err := captureAdapter.Start(ctx)
	if err != nil {
		return err
	}
	output.opportunities = append(output.opportunities, result.Opportunities...)
	for result.State != capture.RunnerClosed {
		if runtime.advance != nil {
			if err := runtime.advance(); err != nil {
				return err
			}
		}
		if sink.evidenceComplete() || sink.closeRequired() {
			result, err = captureAdapter.Close(ctx)
		} else {
			result, err = captureAdapter.Step(ctx)
		}
		output.opportunities = append(output.opportunities, result.Opportunities...)
		if err != nil {
			return err
		}
	}
	if !sink.evidenceComplete() {
		return errors.New("verify: WebSocket capture closed before required ACK, heartbeat, and per-symbol data evidence")
	}
	return nil
}

func captureDepth(ctx context.Context, cfg config.Config, runtime captureRuntime, budget capture.RateBudget, sink *boundedSpoolSink, output *captureResult, symbolIndex int, epoch [16]byte) error {
	request, err := binance.NewSpotDepthRequest(fmt.Sprintf("elmd-014-depth-%d", symbolIndex+1), cfg.Sources[0].Symbols[symbolIndex], cfg.Verify.DepthLimit, false)
	if err != nil {
		return err
	}
	reading := runtime.clock.Read()
	captureAdapter, err := binance.NewSpotDepthCapture(binance.SpotDepthConfig{
		Request: request, RecorderVersion: VerifierVersion,
		Epoch: capture.StreamEpoch{Kind: capture.EpochPollCycle, ID: epoch}, ScheduledAtNS: reading.WallTimeNS,
	}, runtime.rest, runtime.clock, budget, sink)
	if err != nil {
		return err
	}
	result, err := captureAdapter.Start(ctx)
	if err != nil {
		return err
	}
	output.opportunities = append(output.opportunities, result.Opportunities...)
	for result.State != capture.RunnerClosed {
		if runtime.advance != nil {
			if err := runtime.advance(); err != nil {
				return err
			}
		}
		result, err = captureAdapter.Step(ctx)
		output.opportunities = append(output.opportunities, result.Opportunities...)
		if err != nil {
			return err
		}
	}
	return nil
}

func publishReady(ctx context.Context, publisher *objectstore.Publisher, ready segment.ReadySegment) (objectstore.PublishResult, error) {
	return publisher.Publish(ctx, objectstore.PublishRequest{SegmentID: stableUUID("raw-segment", ready.Manifest.ObjectKey), Ready: ready})
}

func stableUUID(namespace, value string) string {
	digest := sha256.Sum256([]byte(namespace + "\x00" + value))
	digest[6] = digest[6]&0x0f | 0x50
	digest[8] = digest[8]&0x3f | 0x80
	raw := hex.EncodeToString(digest[:16])
	return raw[:8] + "-" + raw[8:12] + "-" + raw[12:16] + "-" + raw[16:20] + "-" + raw[20:32]
}

func channelCommitted(ctx context.Context, source PublicationCatalog, channel string, epoch [16]byte) (bool, error) {
	epochHex := hex.EncodeToString(epoch[:])
	epochID := epochHex[:8] + "-" + epochHex[8:12] + "-" + epochHex[12:16] + "-" + epochHex[16:20] + "-" + epochHex[20:]
	found := false
	err := source.StreamCommittedRawSegments(ctx, func(publication catalog.RawSegmentPublication) error {
		if publication.SourceID == binance.SpotSourceID && publication.ChannelID == channel && publication.EpochID == epochID {
			found = true
		}
		return nil
	})
	return found, err
}

func committedPublications(ctx context.Context, source PublicationCatalog) ([]catalog.RawSegmentPublication, error) {
	var publications []catalog.RawSegmentPublication
	err := source.StreamCommittedRawSegments(ctx, func(publication catalog.RawSegmentPublication) error {
		publication.ManifestBytes = slices.Clone(publication.ManifestBytes)
		publications = append(publications, publication)
		return nil
	})
	if err != nil {
		return nil, err
	}
	slices.SortFunc(publications, func(left, right catalog.RawSegmentPublication) int {
		if comparison := strings.Compare(left.SourceID, right.SourceID); comparison != 0 {
			return comparison
		}
		if left.ReceivedStartNS < right.ReceivedStartNS {
			return -1
		}
		if left.ReceivedStartNS > right.ReceivedStartNS {
			return 1
		}
		return strings.Compare(left.SegmentID, right.SegmentID)
	})
	return publications, nil
}
