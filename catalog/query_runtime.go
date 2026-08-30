package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"github.com/enable-xyz/marketdata/capture"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// BootstrapSource records only the stable declarative source identity needed
// to break the raw-publication/catalog-observation bootstrap cycle. It does not
// create a source version, channel, instrument, snapshot, or observation.
func (s *QueryStore) BootstrapSource(ctx context.Context, source Source) (err error) {
	if err := source.Validate(); err != nil {
		return err
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return s.unavailable("begin source bootstrap", err)
	}
	defer rollbackQueryTx(ctx, tx, &err)
	_, err = tx.Exec(ctx, `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (source_id) DO NOTHING
`, source.SourceID, source.Venue, source.ProductFamily, source.APIFamily, source.Environment, source.Lifecycle)
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23505" {
			return queryConflict("declarative source identity")
		}
		return fmt.Errorf("catalog: insert declarative source identity: %w", err)
	}
	var existing Source
	if err := tx.QueryRow(ctx, `
SELECT source_id::text, venue, product_family, api_family, environment, lifecycle_state::text
FROM source WHERE source_id = $1 FOR UPDATE
`, source.SourceID).Scan(&existing.SourceID, &existing.Venue, &existing.ProductFamily,
		&existing.APIFamily, &existing.Environment, &existing.Lifecycle); err != nil {
		return fmt.Errorf("catalog: verify declarative source identity: %w", err)
	}
	if existing != source {
		return queryConflict("declarative source identity")
	}
	if err := tx.Commit(ctx); err != nil {
		return s.unavailable("commit source bootstrap", err)
	}
	return nil
}

type committedRESTEvidence struct {
	requestID       string
	arrivalOrdinal  int64
	messageOrdinal  int32
	envelopeVersion int16
	payloadSHA256   [sha256.Size]byte
	payloadBytes    int
}

// RecordCommittedRESTEvidence derives one raw-record evidence row from the
// exact captured request/response envelopes after the containing segment is
// committed. Callers cannot provide a coordinate, request identity, payload
// hash, or byte count independently of those already-validated envelopes.
func (s *QueryStore) RecordCommittedRESTEvidence(
	ctx context.Context,
	publication RawSegmentPublication,
	request capture.EnvelopeV1,
	response capture.EnvelopeV1,
) (err error) {
	if err := validatePublication(publication); err != nil {
		return err
	}
	if publication.State != RawSegmentCommitted {
		return queryConflict("raw evidence publication is not committed")
	}
	evidence, err := deriveCommittedRESTEvidence(publication, request, response)
	if err != nil {
		return err
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return s.unavailable("begin committed REST evidence", err)
	}
	defer rollbackQueryTx(ctx, tx, &err)
	stored, found, err := findRawSegmentForUpdate(ctx, tx, publication.ObjectKey)
	if err != nil {
		return err
	}
	if !found {
		return queryNotFound("committed raw segment publication")
	}
	if stored.State != RawSegmentCommitted || !samePublicationCore(stored, publication) || !sameManifest(stored, publication) {
		return queryConflict("committed raw segment publication identity")
	}
	_, err = tx.Exec(ctx, `
INSERT INTO raw_record_evidence (
    raw_segment_id, arrival_ordinal, message_ordinal, envelope_version,
    request_id, payload_sha256, payload_byte_length
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (raw_segment_id, arrival_ordinal, message_ordinal) DO NOTHING
`, stored.SegmentID, evidence.arrivalOrdinal, evidence.messageOrdinal,
		evidence.envelopeVersion, evidence.requestID, evidence.payloadSHA256[:], evidence.payloadBytes)
	if err != nil {
		return fmt.Errorf("catalog: insert committed REST evidence: %w", err)
	}
	var envelopeVersion int16
	var requestID string
	var payloadHash []byte
	var payloadBytes int
	if err := tx.QueryRow(ctx, `
SELECT envelope_version, request_id, payload_sha256, payload_byte_length
FROM raw_record_evidence
WHERE raw_segment_id = $1 AND arrival_ordinal = $2 AND message_ordinal = $3
`, stored.SegmentID, evidence.arrivalOrdinal, evidence.messageOrdinal).Scan(
		&envelopeVersion, &requestID, &payloadHash, &payloadBytes,
	); err != nil {
		return fmt.Errorf("catalog: verify committed REST evidence: %w", err)
	}
	if envelopeVersion != evidence.envelopeVersion || requestID != evidence.requestID ||
		!bytes.Equal(payloadHash, evidence.payloadSHA256[:]) || payloadBytes != evidence.payloadBytes {
		return queryConflict("committed REST evidence identity")
	}
	if err := tx.Commit(ctx); err != nil {
		return s.unavailable("commit REST evidence", err)
	}
	return nil
}

func deriveCommittedRESTEvidence(
	publication RawSegmentPublication,
	request capture.EnvelopeV1,
	response capture.EnvelopeV1,
) (committedRESTEvidence, error) {
	if err := request.Validate(); err != nil {
		return committedRESTEvidence{}, fmt.Errorf("%w: request envelope: %v", ErrInvalidQueryProjection, err)
	}
	if err := response.Validate(); err != nil {
		return committedRESTEvidence{}, fmt.Errorf("%w: response envelope: %v", ErrInvalidQueryProjection, err)
	}
	if request.RecordKind != capture.RecordKindControl || !request.ControlKind.Valid ||
		request.ControlKind.Value != capture.ControlRequestStarted ||
		response.RecordKind != capture.RecordKindREST || response.TerminalOutcome != capture.TerminalObserved ||
		!request.PollCycleID.Valid || !response.PollCycleID.Valid ||
		request.PollCycleID.Value != response.PollCycleID.Value ||
		!request.SubscriptionOrRequestID.Valid || !response.SubscriptionOrRequestID.Valid ||
		request.SubscriptionOrRequestID.Value != response.SubscriptionOrRequestID.Value ||
		!request.RequestStartedAtNS.Valid || !response.RequestStartedAtNS.Valid ||
		request.RequestStartedAtNS.Value != response.RequestStartedAtNS.Value ||
		request.RequestCompletedAtNS.Valid || !response.RequestCompletedAtNS.Valid ||
		response.RequestCompletedAtNS.Value < response.RequestStartedAtNS.Value ||
		response.ArrivalOrdinal <= request.ArrivalOrdinal ||
		response.ReceivedWallTimeNS < request.ReceivedWallTimeNS ||
		request.SourceID != response.SourceID || request.ChannelOrEndpoint != response.ChannelOrEndpoint ||
		response.SourceID != publication.SourceID || response.ChannelOrEndpoint != publication.ChannelID ||
		len(response.RawPayload) == 0 {
		return committedRESTEvidence{}, fmt.Errorf("%w: request/response boundary", ErrInvalidQueryProjection)
	}
	epochID := uuid.UUID(response.PollCycleID.Value).String()
	if epochID != publication.EpochID ||
		response.ArrivalOrdinal < publication.OrdinalStart || response.ArrivalOrdinal > publication.OrdinalEnd ||
		response.ReceivedWallTimeNS < publication.ReceivedStartNS ||
		response.ReceivedWallTimeNS > publication.ReceivedEndNS ||
		response.ArrivalOrdinal > uint64(math.MaxInt64) || response.MessageOrdinal > uint32(math.MaxInt32) ||
		response.EnvelopeVersion > uint16(math.MaxInt16) {
		return committedRESTEvidence{}, fmt.Errorf("%w: response outside committed segment coordinates", ErrInvalidQueryProjection)
	}
	return committedRESTEvidence{
		requestID:       response.SubscriptionOrRequestID.Value,
		arrivalOrdinal:  int64(response.ArrivalOrdinal),
		messageOrdinal:  int32(response.MessageOrdinal),
		envelopeVersion: int16(response.EnvelopeVersion),
		payloadSHA256:   sha256.Sum256(response.RawPayload),
		payloadBytes:    len(response.RawPayload),
	}, nil
}

func normalizeCheckpoint(value RuntimeCheckpoint) (RuntimeCheckpoint, error) {
	for name, text := range map[string]string{
		"checkpoint_key": value.Key, "source_id": value.SourceID, "channel_id": value.ChannelID,
		"stream_epoch_id": value.StreamEpochID,
	} {
		if !validQueryText(text) {
			return RuntimeCheckpoint{}, fmt.Errorf("%w: %s", ErrInvalidQueryProjection, name)
		}
	}
	if _, err := uuid.Parse(value.SourceID); err != nil {
		return RuntimeCheckpoint{}, fmt.Errorf("%w: checkpoint source UUID", ErrInvalidQueryProjection)
	}
	if _, err := uuid.Parse(value.StreamEpochID); err != nil {
		return RuntimeCheckpoint{}, fmt.Errorf("%w: checkpoint epoch UUID", ErrInvalidQueryProjection)
	}
	if value.InstrumentUID != "" {
		if _, err := uuid.Parse(value.InstrumentUID); err != nil {
			return RuntimeCheckpoint{}, fmt.Errorf("%w: checkpoint instrument UUID", ErrInvalidQueryProjection)
		}
	}
	if value.ReceivedTimeNS < 0 || len(value.StateBytes) == 0 || len(value.StateBytes) > MaximumCheckpointBytes ||
		!nonzeroHash(value.StateSHA256) || sha256.Sum256(value.StateBytes) != value.StateSHA256 || value.UpdatedAt.IsZero() {
		return RuntimeCheckpoint{}, fmt.Errorf("%w: checkpoint coordinate or state", ErrInvalidQueryProjection)
	}
	value.UpdatedAt = value.UpdatedAt.UTC().Truncate(time.Microsecond)
	value.StateBytes = bytes.Clone(value.StateBytes)
	return value, nil
}

// PutCheckpoint serializes writers per checkpoint key. An exact retry succeeds;
// a greater stable coordinate advances atomically; identity changes, regression,
// and mutation at the same coordinate fail closed.
func (s *QueryStore) PutCheckpoint(ctx context.Context, input RuntimeCheckpoint) (err error) {
	value, err := normalizeCheckpoint(input)
	if err != nil {
		return err
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return s.unavailable("begin checkpoint write", err)
	}
	defer rollbackQueryTx(ctx, tx, &err)
	existing, found, err := readCheckpoint(ctx, tx, value.Key, true)
	if err != nil {
		return err
	}
	if !found {
		var instrument any
		if value.InstrumentUID != "" {
			instrument = value.InstrumentUID
		}
		_, err := tx.Exec(ctx, `
INSERT INTO runtime_checkpoint (
    checkpoint_key, source_id, channel_id, instrument_uid, received_time_ns,
    stream_epoch_id, arrival_ordinal, message_ordinal, state_hash, state_bytes, updated_at
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`, value.Key, value.SourceID, value.ChannelID, instrument, value.ReceivedTimeNS,
			value.StreamEpochID, value.ArrivalOrdinal, value.MessageOrdinal,
			value.StateSHA256[:], value.StateBytes, value.UpdatedAt)
		if err != nil {
			return fmt.Errorf("catalog: insert runtime checkpoint: %w", err)
		}
		if err := tx.Commit(ctx); err != nil {
			return s.unavailable("commit checkpoint insert", err)
		}
		return nil
	}
	if existing.SourceID != value.SourceID || existing.ChannelID != value.ChannelID || existing.InstrumentUID != value.InstrumentUID {
		return queryConflict("checkpoint identity")
	}
	coordinate := compareCheckpointCoordinate(existing, value)
	if coordinate > 0 {
		return queryConflict("checkpoint regression")
	}
	if coordinate == 0 {
		if existing.StateSHA256 != value.StateSHA256 || !bytes.Equal(existing.StateBytes, value.StateBytes) || !existing.UpdatedAt.Equal(value.UpdatedAt) {
			return queryConflict("checkpoint mutation at recorded coordinate")
		}
		return tx.Commit(ctx)
	}
	command, err := tx.Exec(ctx, `
UPDATE runtime_checkpoint
SET received_time_ns = $2, stream_epoch_id = $3, arrival_ordinal = $4,
    message_ordinal = $5, state_hash = $6, state_bytes = $7, updated_at = $8
WHERE checkpoint_key = $1
`, value.Key, value.ReceivedTimeNS, value.StreamEpochID, value.ArrivalOrdinal,
		value.MessageOrdinal, value.StateSHA256[:], value.StateBytes, value.UpdatedAt)
	if err != nil {
		return fmt.Errorf("catalog: advance runtime checkpoint: %w", err)
	}
	if !commandChangedOne(command) {
		return queryConflict("checkpoint advance")
	}
	if err := tx.Commit(ctx); err != nil {
		return s.unavailable("commit checkpoint advance", err)
	}
	return nil
}

func (s *QueryStore) Checkpoint(ctx context.Context, key string) (RuntimeCheckpoint, error) {
	if !validQueryText(key) {
		return RuntimeCheckpoint{}, fmt.Errorf("%w: checkpoint key", ErrInvalidQueryProjection)
	}
	value, found, err := readCheckpoint(ctx, s.database, key, false)
	if err != nil {
		return RuntimeCheckpoint{}, s.unavailable("read runtime checkpoint", err)
	}
	if !found {
		return RuntimeCheckpoint{}, queryNotFound("runtime checkpoint")
	}
	return value, nil
}

type checkpointRow interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readCheckpoint(ctx context.Context, source checkpointRow, key string, lock bool) (RuntimeCheckpoint, bool, error) {
	query := `
SELECT checkpoint_key, source_id::text, channel_id, COALESCE(instrument_uid::text, ''),
       received_time_ns, stream_epoch_id::text, arrival_ordinal, message_ordinal,
       state_hash, state_bytes, updated_at
FROM runtime_checkpoint WHERE checkpoint_key = $1`
	if lock {
		query += " FOR UPDATE"
	}
	var value RuntimeCheckpoint
	var arrival int64
	var message int32
	var stateHash []byte
	err := source.QueryRow(ctx, query, key).Scan(&value.Key, &value.SourceID, &value.ChannelID,
		&value.InstrumentUID, &value.ReceivedTimeNS, &value.StreamEpochID, &arrival, &message,
		&stateHash, &value.StateBytes, &value.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return RuntimeCheckpoint{}, false, nil
	}
	if err != nil {
		return RuntimeCheckpoint{}, false, err
	}
	if arrival < 0 || message < 0 || len(stateHash) != sha256.Size {
		return RuntimeCheckpoint{}, false, fmt.Errorf("%w: malformed runtime checkpoint", ErrInvalidQueryProjection)
	}
	value.ArrivalOrdinal, value.MessageOrdinal = uint64(arrival), uint32(message)
	copy(value.StateSHA256[:], stateHash)
	return value.clone(), true, nil
}

func compareCheckpointCoordinate(left, right RuntimeCheckpoint) int {
	if left.ReceivedTimeNS < right.ReceivedTimeNS {
		return -1
	}
	if left.ReceivedTimeNS > right.ReceivedTimeNS {
		return 1
	}
	if value := strings.Compare(left.StreamEpochID, right.StreamEpochID); value != 0 {
		return value
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

func validateGapFilter(filter GapFilter) error {
	if err := validateSortedQueryIDs("source_ids", filter.SourceIDs, true, 64); err != nil {
		return err
	}
	if err := validateSortedQueryIDs("channel_ids", filter.ChannelIDs, false, 64); err != nil {
		return err
	}
	if err := validateSortedQueryIDs("instrument_uids", filter.InstrumentUIDs, false, 256); err != nil {
		return err
	}
	if filter.StartReceivedTimeNS < 0 || filter.StartReceivedTimeNS >= filter.EndReceivedTimeNS ||
		filter.Limit < 1 || filter.Limit > MaximumDatasetCoverage {
		return fmt.Errorf("%w: gap time or result bound", ErrInvalidQueryProjection)
	}
	return nil
}

func (s *QueryStore) Gaps(ctx context.Context, filter GapFilter) ([]GapProjection, error) {
	if err := validateGapFilter(filter); err != nil {
		return nil, err
	}
	rows, err := s.database.Query(ctx, `
SELECT gap_id::text, source_id::text, channel_id, COALESCE(instrument_uid::text, ''),
       range_start_ns, range_end_ns, detection_rule, state::text,
       detected_time_ns, COALESCE(resolved_time_ns, 0)
FROM gap
WHERE source_id::text = ANY($1::text[])
  AND (cardinality($2::text[]) = 0 OR channel_id = ANY($2::text[]))
  AND (cardinality($3::text[]) = 0 OR instrument_uid::text = ANY($3::text[]))
  AND range_start_ns < $5 AND range_end_ns > $4
ORDER BY source_id, channel_id COLLATE "C", COALESCE(instrument_uid::text, ''),
         range_start_ns, range_end_ns, gap_id
LIMIT $6
`, filter.SourceIDs, filter.ChannelIDs, filter.InstrumentUIDs,
		filter.StartReceivedTimeNS, filter.EndReceivedTimeNS, filter.Limit+1)
	if err != nil {
		return nil, s.unavailable("query gaps", err)
	}
	defer rows.Close()
	result := make([]GapProjection, 0, 64)
	for rows.Next() {
		var value GapProjection
		if err := rows.Scan(&value.ID, &value.Tuple.SourceID, &value.Tuple.ChannelID,
			&value.Tuple.InstrumentUID, &value.StartReceivedTimeNS, &value.EndReceivedTimeNS,
			&value.Kind, &value.State, &value.DetectedTimeNS, &value.ResolvedTimeNS); err != nil {
			return nil, s.unavailable("scan gap projection", err)
		}
		if len(result) == filter.Limit {
			return nil, fmt.Errorf("%w: gaps exceed %d", ErrQueryBound, filter.Limit)
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, s.unavailable("iterate gaps", err)
	}
	return result, nil
}

func (s *QueryStore) Gap(ctx context.Context, id string) (GapProjection, error) {
	if _, err := uuid.Parse(id); err != nil {
		return GapProjection{}, fmt.Errorf("%w: gap UUID", ErrInvalidQueryProjection)
	}
	var value GapProjection
	err := s.database.QueryRow(ctx, `
SELECT gap_id::text, source_id::text, channel_id, COALESCE(instrument_uid::text, ''),
       range_start_ns, range_end_ns, detection_rule, state::text,
       detected_time_ns, COALESCE(resolved_time_ns, 0)
FROM gap WHERE gap_id = $1
`, id).Scan(&value.ID, &value.Tuple.SourceID, &value.Tuple.ChannelID,
		&value.Tuple.InstrumentUID, &value.StartReceivedTimeNS, &value.EndReceivedTimeNS,
		&value.Kind, &value.State, &value.DetectedTimeNS, &value.ResolvedTimeNS)
	if errors.Is(err, pgx.ErrNoRows) {
		return GapProjection{}, queryNotFound("gap")
	}
	if err != nil {
		return GapProjection{}, s.unavailable("read gap", err)
	}
	return value, nil
}

// OpenGap returns the sole unresolved gap for one source/channel tuple. More
// than one unresolved interval is a catalog conflict rather than an arbitrary
// choice during collector recovery.
func (s *QueryStore) OpenGap(ctx context.Context, sourceID, channelID, instrumentUID string) (GapProjection, error) {
	if _, err := uuid.Parse(sourceID); err != nil || !validQueryText(channelID) {
		return GapProjection{}, fmt.Errorf("%w: open-gap source or channel identity", ErrInvalidQueryProjection)
	}
	if instrumentUID != "" {
		if _, err := uuid.Parse(instrumentUID); err != nil {
			return GapProjection{}, fmt.Errorf("%w: open-gap instrument identity", ErrInvalidQueryProjection)
		}
	}
	rows, err := s.database.Query(ctx, `
SELECT gap_id::text, source_id::text, channel_id, COALESCE(instrument_uid::text, ''),
       range_start_ns, range_end_ns, detection_rule, state::text,
       detected_time_ns, COALESCE(resolved_time_ns, 0)
FROM gap
WHERE source_id = $1 AND channel_id = $2
  AND (($3 = '' AND instrument_uid IS NULL) OR instrument_uid::text = $3)
  AND state = 'open'
ORDER BY detected_time_ns, gap_id
LIMIT 2
`, sourceID, channelID, instrumentUID)
	if err != nil {
		return GapProjection{}, s.unavailable("query open gap", err)
	}
	defer rows.Close()
	var value GapProjection
	count := 0
	for rows.Next() {
		count++
		if count > 1 {
			return GapProjection{}, queryConflict("multiple open gaps for source/channel tuple")
		}
		if err := rows.Scan(&value.ID, &value.Tuple.SourceID, &value.Tuple.ChannelID,
			&value.Tuple.InstrumentUID, &value.StartReceivedTimeNS, &value.EndReceivedTimeNS,
			&value.Kind, &value.State, &value.DetectedTimeNS, &value.ResolvedTimeNS); err != nil {
			return GapProjection{}, s.unavailable("scan open gap", err)
		}
	}
	if err := rows.Err(); err != nil {
		return GapProjection{}, s.unavailable("iterate open gap", err)
	}
	if count == 0 {
		return GapProjection{}, queryNotFound("open gap")
	}
	return value, nil
}
