package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

type CatalogDatabase interface {
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
}

type Store struct {
	database CatalogDatabase
}

func NewStore(database CatalogDatabase) (*Store, error) {
	if database == nil {
		return nil, errors.New("catalog: database is required")
	}
	return &Store{database: database}, nil
}

func (s *Store) Sync(ctx context.Context, input SyncInput) (snapshot Snapshot, err error) {
	if err := input.Validate(); err != nil {
		return Snapshot{}, err
	}
	input.Channels = slices.Clone(input.Channels)
	slices.SortFunc(input.Channels, func(a, b ChannelContract) int { return strings.Compare(a.ChannelID, b.ChannelID) })
	input.Instruments = slices.Clone(input.Instruments)
	slices.SortFunc(input.Instruments, func(a, b InstrumentCandidate) int { return strings.Compare(a.NativeID, b.NativeID) })
	input.Pages = slices.Clone(input.Pages)
	slices.SortFunc(input.Pages, func(a, b SyncPageEvidence) int { return a.PageIndex - b.PageIndex })

	tx, err := s.database.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.Serializable})
	if err != nil {
		return Snapshot{}, fmt.Errorf("catalog: begin catalog sync: %w", err)
	}
	defer func() {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
			err = errors.Join(err, fmt.Errorf("catalog: rollback catalog sync: %w", rollbackErr))
		}
	}()
	if _, err := tx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", input.Source.SourceID); err != nil {
		return Snapshot{}, fmt.Errorf("catalog: lock source sync: %w", err)
	}
	if err := verifyCommittedRawEvidence(ctx, tx, input); err != nil {
		return Snapshot{}, err
	}
	if err := upsertSource(ctx, tx, input.Source, input.ObservedAt); err != nil {
		return Snapshot{}, err
	}
	if err := upsertSourceVersion(ctx, tx, input.Source.SourceID, input.SourceVersion, input.ObservedAt); err != nil {
		return Snapshot{}, err
	}
	for _, channel := range input.Channels {
		if err := upsertChannel(ctx, tx, input.Source.SourceID, channel, input.ObservedAt); err != nil {
			return Snapshot{}, err
		}
	}
	if err := recordSyncOpportunities(ctx, tx, input); err != nil {
		return Snapshot{}, err
	}
	for _, instrument := range input.Instruments {
		if err := upsertInstrument(ctx, tx, input.Source.SourceID, instrument, input.ObservedAt); err != nil {
			return Snapshot{}, err
		}
	}
	snapshot, err = loadSnapshot(ctx, tx, input.Source.SourceID, input.ObservedAt)
	if err != nil {
		return Snapshot{}, err
	}
	if err := insertSnapshot(ctx, tx, input.Source.SourceID, input.ObservedAt, snapshot); err != nil {
		return Snapshot{}, err
	}
	if err := insertSyncRun(ctx, tx, input, snapshot); err != nil {
		return Snapshot{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return Snapshot{}, fmt.Errorf("catalog: commit catalog sync: %w", err)
	}
	return snapshot, nil
}

func upsertSource(ctx context.Context, tx pgx.Tx, source Source, observedAt time.Time) error {
	_, err := tx.Exec(ctx, `
INSERT INTO source (
    source_id, venue, product_family, api_family, environment, lifecycle_state, created_at
) VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (source_id) DO UPDATE SET lifecycle_state = EXCLUDED.lifecycle_state
WHERE source.venue = EXCLUDED.venue
  AND source.product_family = EXCLUDED.product_family
  AND source.api_family = EXCLUDED.api_family
  AND source.environment = EXCLUDED.environment
`, source.SourceID, source.Venue, source.ProductFamily, source.APIFamily, source.Environment, source.Lifecycle, observedAt)
	if err != nil {
		return fmt.Errorf("catalog: upsert source %s: %w", source.SourceID, err)
	}
	var found Source
	if err := tx.QueryRow(ctx, `
SELECT source_id::text, venue, product_family, api_family, environment, lifecycle_state::text
FROM source WHERE source_id = $1
`, source.SourceID).Scan(&found.SourceID, &found.Venue, &found.ProductFamily, &found.APIFamily, &found.Environment, &found.Lifecycle); err != nil {
		return fmt.Errorf("catalog: read source %s after upsert: %w", source.SourceID, err)
	}
	if found.SourceID != source.SourceID || found.Venue != source.Venue || found.ProductFamily != source.ProductFamily || found.APIFamily != source.APIFamily || found.Environment != source.Environment {
		return fmt.Errorf("%w: source identity conflict", ErrInvalidCatalog)
	}
	return nil
}

func upsertSourceVersion(ctx context.Context, tx pgx.Tx, sourceID string, value SourceVersion, observedAt time.Time) error {
	var current SourceVersion
	var validFrom time.Time
	err := tx.QueryRow(ctx, `
SELECT valid_from, official_api_version, documentation_uri, endpoints, topology, entitlement,
       region, rate_contract, heartbeat_policy, acknowledgement_policy, reconnect_policy
FROM source_version
WHERE source_id = $1 AND valid_to IS NULL
FOR UPDATE
`, sourceID).Scan(
		&validFrom, &current.OfficialAPIVersion, &current.DocumentationURI, &current.Endpoints,
		&current.Topology, &current.Entitlement, &current.Region, &current.RateContract,
		&current.HeartbeatPolicy, &current.AcknowledgementPolicy, &current.ReconnectPolicy,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return insertSourceVersion(ctx, tx, sourceID, value, observedAt)
	}
	if err != nil {
		return fmt.Errorf("catalog: read source version: %w", err)
	}
	equal, err := sourceVersionsEqual(current, value)
	if err != nil {
		return err
	}
	if equal {
		return nil
	}
	if !observedAt.After(validFrom) {
		return fmt.Errorf("%w: changed source version does not advance observed time", ErrInvalidCatalog)
	}
	if _, err := tx.Exec(ctx, `UPDATE source_version SET valid_to = $2 WHERE source_id = $1 AND valid_to IS NULL`, sourceID, observedAt); err != nil {
		return fmt.Errorf("catalog: close source version: %w", err)
	}
	return insertSourceVersion(ctx, tx, sourceID, value, observedAt)
}

func insertSourceVersion(ctx context.Context, tx pgx.Tx, sourceID string, value SourceVersion, observedAt time.Time) error {
	_, err := tx.Exec(ctx, `
INSERT INTO source_version (
    source_id, valid_from, official_api_version, documentation_uri, endpoints, topology,
    entitlement, region, rate_contract, heartbeat_policy, acknowledgement_policy, reconnect_policy
) VALUES ($1, $2, $3, $4, $5::jsonb, $6::jsonb, $7::jsonb, $8, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb)
`, sourceID, observedAt, value.OfficialAPIVersion, value.DocumentationURI, value.Endpoints, value.Topology,
		value.Entitlement, value.Region, value.RateContract, value.HeartbeatPolicy,
		value.AcknowledgementPolicy, value.ReconnectPolicy)
	if err != nil {
		return fmt.Errorf("catalog: insert source version: %w", err)
	}
	return nil
}

func upsertChannel(ctx context.Context, tx pgx.Tx, sourceID string, value ChannelContract, observedAt time.Time) error {
	var current ChannelContract
	var validFrom time.Time
	err := tx.QueryRow(ctx, `
SELECT valid_from, channel_id, native_selector, channel_role, data_family, cadence_source,
       aggregation, depth, sequence_rules, checksum_rules, payload_schema, support_state::text,
       COALESCE(limitation, '')
FROM channel_contract
WHERE source_id = $1 AND channel_id = $2 AND valid_to IS NULL
FOR UPDATE
`, sourceID, value.ChannelID).Scan(
		&validFrom, &current.ChannelID, &current.NativeSelector, &current.Role, &current.DataFamily,
		&current.CadenceSource, &current.Aggregation, &current.Depth, &current.SequenceRules,
		&current.ChecksumRules, &current.PayloadSchema, &current.SupportState, &current.Limitation,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return insertChannel(ctx, tx, sourceID, value, observedAt)
	}
	if err != nil {
		return fmt.Errorf("catalog: read channel %s: %w", value.ChannelID, err)
	}
	equal, err := channelsEqual(current, value)
	if err != nil {
		return err
	}
	if equal {
		return nil
	}
	if !observedAt.After(validFrom) {
		return fmt.Errorf("%w: changed channel %q does not advance observed time", ErrInvalidCatalog, value.ChannelID)
	}
	if _, err := tx.Exec(ctx, `
UPDATE channel_contract SET valid_to = $3
WHERE source_id = $1 AND channel_id = $2 AND valid_to IS NULL
`, sourceID, value.ChannelID, observedAt); err != nil {
		return fmt.Errorf("catalog: close channel %s: %w", value.ChannelID, err)
	}
	return insertChannel(ctx, tx, sourceID, value, observedAt)
}

func insertChannel(ctx context.Context, tx pgx.Tx, sourceID string, value ChannelContract, observedAt time.Time) error {
	var limitation any
	if value.Limitation != "" {
		limitation = value.Limitation
	}
	_, err := tx.Exec(ctx, `
INSERT INTO channel_contract (
    source_id, channel_id, valid_from, native_selector, channel_role, data_family,
    cadence_source, aggregation, depth, sequence_rules, checksum_rules, payload_schema,
    support_state, limitation
) VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, $8::jsonb, $9::jsonb, $10::jsonb, $11::jsonb, $12::jsonb, $13, $14)
`, sourceID, value.ChannelID, observedAt, value.NativeSelector, value.Role, value.DataFamily,
		value.CadenceSource, value.Aggregation, value.Depth, value.SequenceRules,
		value.ChecksumRules, value.PayloadSchema, value.SupportState, limitation)
	if err != nil {
		return fmt.Errorf("catalog: insert channel %s: %w", value.ChannelID, err)
	}
	return nil
}

type currentInstrument struct {
	UID           string
	NativeID      string
	Generation    int64
	ValidFrom     time.Time
	RawHash       []byte
	SchemaVersion string
}

func upsertInstrument(ctx context.Context, tx pgx.Tx, sourceID string, value InstrumentCandidate, observedAt time.Time) error {
	var current currentInstrument
	err := tx.QueryRow(ctx, `
SELECT i.instrument_uid::text, i.native_id, i.listing_epoch, iv.valid_from,
       iv.raw_metadata_hash, iv.normalized_schema_version
FROM instrument_alias ia
JOIN instrument i ON i.source_id = ia.source_id AND i.instrument_uid = ia.instrument_uid
JOIN instrument_version iv ON iv.instrument_uid = i.instrument_uid AND iv.valid_to IS NULL
WHERE ia.source_id = $1 AND ia.alias = $2 AND ia.valid_to IS NULL
FOR UPDATE OF ia, i, iv
`, sourceID, value.NativeID).Scan(
		&current.UID, &current.NativeID, &current.Generation, &current.ValidFrom,
		&current.RawHash, &current.SchemaVersion,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		if value.LifecycleClosure {
			return fmt.Errorf("%w: explicit lifecycle closure for unknown open instrument %q", ErrInvalidCatalog, value.NativeID)
		}
		return insertInstrumentGeneration(ctx, tx, sourceID, value, observedAt)
	}
	if err != nil {
		return fmt.Errorf("catalog: read open instrument %s: %w", value.NativeID, err)
	}
	if current.NativeID != value.NativeID {
		return fmt.Errorf("%w: native alias %q resolves to %q", ErrInvalidCatalog, value.NativeID, current.NativeID)
	}
	if value.LifecycleClosure {
		if !observedAt.After(current.ValidFrom) {
			return fmt.Errorf("%w: lifecycle closure for %q does not advance observed time", ErrInvalidCatalog, value.NativeID)
		}
		if _, err := tx.Exec(ctx, `UPDATE instrument_version SET valid_to = $2 WHERE instrument_uid = $1 AND valid_to IS NULL`, current.UID, observedAt); err != nil {
			return fmt.Errorf("catalog: close instrument %s: %w", value.NativeID, err)
		}
		if _, err := tx.Exec(ctx, `UPDATE instrument_alias SET valid_to = $2 WHERE instrument_uid = $1 AND valid_to IS NULL`, current.UID, observedAt); err != nil {
			return fmt.Errorf("catalog: close aliases for %s: %w", value.NativeID, err)
		}
		return nil
	}
	if len(current.RawHash) == sha256.Size && bytes.Equal(current.RawHash, value.RawMetadataSHA256[:]) && current.SchemaVersion == value.NormalizedSchemaVersion {
		return nil
	}
	if !observedAt.After(current.ValidFrom) {
		return fmt.Errorf("%w: metadata change for %q does not advance observed time", ErrInvalidCatalog, value.NativeID)
	}
	if _, err := tx.Exec(ctx, `UPDATE instrument_version SET valid_to = $2 WHERE instrument_uid = $1 AND valid_to IS NULL`, current.UID, observedAt); err != nil {
		return fmt.Errorf("catalog: close instrument version %s: %w", value.NativeID, err)
	}
	return insertInstrumentVersion(ctx, tx, current.UID, value, observedAt)
}

func insertInstrumentGeneration(ctx context.Context, tx pgx.Tx, sourceID string, value InstrumentCandidate, observedAt time.Time) error {
	for _, alias := range value.Aliases {
		var owner string
		err := tx.QueryRow(ctx, `
SELECT instrument_uid::text FROM instrument_alias
WHERE source_id = $1 AND alias = $2 AND valid_to IS NULL
FOR UPDATE
`, sourceID, alias).Scan(&owner)
		if err == nil {
			return fmt.Errorf("%w: alias %q already has open owner %s", ErrInvalidCatalog, alias, owner)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("catalog: check alias %q: %w", alias, err)
		}
	}
	var generation int64
	if err := tx.QueryRow(ctx, `
SELECT COALESCE(MAX(listing_epoch) + 1, 0) FROM instrument WHERE source_id = $1 AND native_id = $2
`, sourceID, value.NativeID).Scan(&generation); err != nil {
		return fmt.Errorf("catalog: allocate generation for %s: %w", value.NativeID, err)
	}
	uid := deterministicInstrumentUID(sourceID, value.NativeID, generation)
	if _, err := tx.Exec(ctx, `
INSERT INTO instrument (instrument_uid, source_id, native_id, listing_epoch, first_observed_at)
VALUES ($1, $2, $3, $4, $5)
`, uid, sourceID, value.NativeID, generation, observedAt); err != nil {
		return fmt.Errorf("catalog: insert instrument %s generation %d: %w", value.NativeID, generation, err)
	}
	if err := insertInstrumentVersion(ctx, tx, uid, value, observedAt); err != nil {
		return err
	}
	for _, alias := range value.Aliases {
		if _, err := tx.Exec(ctx, `
INSERT INTO instrument_alias (source_id, alias, valid_from, instrument_uid)
VALUES ($1, $2, $3, $4)
`, sourceID, alias, observedAt, uid); err != nil {
			return fmt.Errorf("catalog: insert alias %q: %w", alias, err)
		}
	}
	return nil
}

func insertInstrumentVersion(ctx context.Context, tx pgx.Tx, uid string, value InstrumentCandidate, observedAt time.Time) error {
	aliases, err := json.Marshal(value.Aliases)
	if err != nil {
		return fmt.Errorf("catalog: encode instrument aliases: %w", err)
	}
	rawMetadata, err := CanonicalJSON(value.RawMetadata)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
INSERT INTO instrument_version (
    instrument_uid, valid_from, aliases, lifecycle_state, base_asset, quote_asset,
    margin_asset, settlement_asset, instrument_kind, payoff, multiplier,
    tick_rules, lot_rules, raw_metadata, raw_metadata_hash, normalized_schema_version
) VALUES (
    $1, $2, $3::jsonb, $4, $5, $6, NULLIF($7, ''), $8, $9, $10::jsonb, $11::numeric,
    $12::jsonb, $13::jsonb, $14::jsonb, $15, $16
)
`, uid, observedAt, aliases, value.Lifecycle, value.BaseAsset, value.QuoteAsset,
		value.MarginAsset, value.SettlementAsset, value.Kind, value.Payoff, value.Multiplier,
		value.TickRules, value.LotRules, rawMetadata, value.RawMetadataSHA256[:], value.NormalizedSchemaVersion)
	if err != nil {
		return fmt.Errorf("catalog: insert instrument version %s: %w", value.NativeID, err)
	}
	return nil
}

func loadSnapshot(ctx context.Context, tx pgx.Tx, sourceID string, observedAt time.Time) (Snapshot, error) {
	var input SnapshotInput
	if err := tx.QueryRow(ctx, `
SELECT source_id::text, venue, product_family, api_family, environment, lifecycle_state::text
FROM source WHERE source_id = $1
`, sourceID).Scan(&input.Source.SourceID, &input.Source.Venue, &input.Source.ProductFamily,
		&input.Source.APIFamily, &input.Source.Environment, &input.Source.Lifecycle); err != nil {
		return Snapshot{}, fmt.Errorf("catalog: load snapshot source: %w", err)
	}
	if err := tx.QueryRow(ctx, `
SELECT official_api_version, documentation_uri, endpoints, topology, entitlement, region,
       rate_contract, heartbeat_policy, acknowledgement_policy, reconnect_policy
FROM source_version
WHERE source_id = $1 AND valid_from <= $2 AND (valid_to IS NULL OR valid_to > $2)
`, sourceID, observedAt).Scan(
		&input.SourceVersion.OfficialAPIVersion, &input.SourceVersion.DocumentationURI,
		&input.SourceVersion.Endpoints, &input.SourceVersion.Topology, &input.SourceVersion.Entitlement,
		&input.SourceVersion.Region, &input.SourceVersion.RateContract, &input.SourceVersion.HeartbeatPolicy,
		&input.SourceVersion.AcknowledgementPolicy, &input.SourceVersion.ReconnectPolicy,
	); err != nil {
		return Snapshot{}, fmt.Errorf("catalog: load snapshot source version: %w", err)
	}
	channelRows, err := tx.Query(ctx, `
SELECT channel_id, native_selector, channel_role, data_family, cadence_source, aggregation,
       depth, sequence_rules, checksum_rules, payload_schema, support_state::text, COALESCE(limitation, '')
FROM channel_contract
WHERE source_id = $1 AND valid_from <= $2 AND (valid_to IS NULL OR valid_to > $2)
ORDER BY channel_id
`, sourceID, observedAt)
	if err != nil {
		return Snapshot{}, fmt.Errorf("catalog: query snapshot channels: %w", err)
	}
	for channelRows.Next() {
		var channel ChannelContract
		if err := channelRows.Scan(&channel.ChannelID, &channel.NativeSelector, &channel.Role,
			&channel.DataFamily, &channel.CadenceSource, &channel.Aggregation, &channel.Depth,
			&channel.SequenceRules, &channel.ChecksumRules, &channel.PayloadSchema,
			&channel.SupportState, &channel.Limitation); err != nil {
			channelRows.Close()
			return Snapshot{}, fmt.Errorf("catalog: scan snapshot channel: %w", err)
		}
		input.Channels = append(input.Channels, channel)
	}
	if err := channelRows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("catalog: iterate snapshot channels: %w", err)
	}
	channelRows.Close()

	instrumentRows, err := tx.Query(ctx, `
SELECT i.instrument_uid::text, i.native_id, i.listing_epoch, iv.aliases,
       iv.lifecycle_state::text, COALESCE(iv.base_asset, ''), COALESCE(iv.quote_asset, ''),
       COALESCE(iv.margin_asset, ''), COALESCE(iv.settlement_asset, ''), iv.instrument_kind,
       iv.payoff, iv.multiplier::text, iv.tick_rules, iv.lot_rules,
       encode(iv.raw_metadata_hash, 'hex'), iv.normalized_schema_version
FROM instrument i
JOIN instrument_version iv ON iv.instrument_uid = i.instrument_uid
WHERE i.source_id = $1
  AND iv.valid_from <= $2 AND (iv.valid_to IS NULL OR iv.valid_to > $2)
ORDER BY i.native_id, i.listing_epoch, i.instrument_uid
`, sourceID, observedAt)
	if err != nil {
		return Snapshot{}, fmt.Errorf("catalog: query snapshot instruments: %w", err)
	}
	for instrumentRows.Next() {
		var instrument SnapshotInstrument
		var aliases []byte
		if err := instrumentRows.Scan(
			&instrument.InstrumentUID, &instrument.NativeID, &instrument.ListingGeneration, &aliases,
			&instrument.Lifecycle, &instrument.BaseAsset, &instrument.QuoteAsset, &instrument.MarginAsset,
			&instrument.SettlementAsset, &instrument.Kind, &instrument.Payoff, &instrument.Multiplier,
			&instrument.TickRules, &instrument.LotRules, &instrument.RawMetadataSHA256,
			&instrument.NormalizedSchemaVersion,
		); err != nil {
			instrumentRows.Close()
			return Snapshot{}, fmt.Errorf("catalog: scan snapshot instrument: %w", err)
		}
		if err := json.Unmarshal(aliases, &instrument.Aliases); err != nil || len(instrument.Aliases) == 0 || len(instrument.Aliases) > MaxCatalogAliases {
			instrumentRows.Close()
			return Snapshot{}, fmt.Errorf("catalog: decode snapshot aliases for %s", instrument.NativeID)
		}
		input.Instruments = append(input.Instruments, instrument)
	}
	if err := instrumentRows.Err(); err != nil {
		return Snapshot{}, fmt.Errorf("catalog: iterate snapshot instruments: %w", err)
	}
	instrumentRows.Close()
	return BuildSnapshot(input)
}

func insertSnapshot(ctx context.Context, tx pgx.Tx, sourceID string, observedAt time.Time, snapshot Snapshot) error {
	_, err := tx.Exec(ctx, `
INSERT INTO catalog_snapshot (
    snapshot_sha256, source_id, snapshot_version, snapshot_bytes, instrument_count, first_observed_at
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (snapshot_sha256) DO NOTHING
`, snapshot.SHA256[:], sourceID, int(snapshot.Version), snapshot.Bytes, snapshot.InstrumentCount, observedAt)
	if err != nil {
		return fmt.Errorf("catalog: insert immutable snapshot: %w", err)
	}
	var existingSource string
	var version int
	var existingBytes []byte
	var count int
	if err := tx.QueryRow(ctx, `
SELECT source_id::text, snapshot_version, snapshot_bytes, instrument_count
FROM catalog_snapshot WHERE snapshot_sha256 = $1
`, snapshot.SHA256[:]).Scan(&existingSource, &version, &existingBytes, &count); err != nil {
		return fmt.Errorf("catalog: verify immutable snapshot: %w", err)
	}
	if existingSource != sourceID || version != int(snapshot.Version) || count != snapshot.InstrumentCount || !bytes.Equal(existingBytes, snapshot.Bytes) {
		return errors.New("catalog: immutable snapshot hash identity conflict")
	}
	return nil
}

func verifyCommittedRawEvidence(ctx context.Context, tx pgx.Tx, input SyncInput) error {
	for _, page := range input.Pages {
		if page.RawRecord.EvidenceScope != RawEvidenceCommitted {
			return fmt.Errorf("%w: catalog sync requires committed raw segment evidence for page %d", ErrInvalidCatalog, page.PageIndex)
		}
		var segmentSourceID, segmentChannelID, segmentEpochID, objectKey, state string
		var ordinalStart, ordinalEnd, byteLength int64
		var contentHash, manifestHash, manifestBytes []byte
		var manifestVersion int16
		var committedAt pgtype.Timestamptz
		err := tx.QueryRow(ctx, `
SELECT rs.source_id::text, rs.channel_id, rs.epoch_id::text, rs.ordinal_start, rs.ordinal_end,
       rs.object_key, rs.content_hash, rs.byte_length, rs.state::text, rs.committed_at,
       rsm.manifest_version, rsm.manifest_hash, rsm.manifest_bytes
FROM raw_segment rs
JOIN raw_segment_manifest rsm USING (raw_segment_id)
WHERE rs.raw_segment_id = $1
FOR UPDATE OF rs, rsm
`, page.RawRecord.RawSegmentID).Scan(
			&segmentSourceID, &segmentChannelID, &segmentEpochID, &ordinalStart, &ordinalEnd,
			&objectKey, &contentHash, &byteLength, &state, &committedAt,
			&manifestVersion, &manifestHash, &manifestBytes,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: raw segment %s does not exist", ErrInvalidCatalog, page.RawRecord.RawSegmentID)
		}
		if err != nil {
			return fmt.Errorf("catalog: lock raw segment evidence: %w", err)
		}
		expectedSegmentHash, _ := hex.DecodeString(page.RawRecord.RawSegmentSHA256)
		arrivalOrdinal := int64(page.RawRecord.ArrivalOrdinal)
		computedManifestHash := sha256.Sum256(manifestBytes)
		if segmentSourceID != input.Source.SourceID || segmentChannelID != page.ChannelID ||
			segmentEpochID != page.RawRecord.PollCycleID ||
			arrivalOrdinal < ordinalStart || arrivalOrdinal > ordinalEnd ||
			objectKey != page.RawRecord.ObjectKey ||
			!bytes.Equal(contentHash, expectedSegmentHash) ||
			byteLength != page.RawRecord.RawSegmentByteLength ||
			state != string(RawSegmentCommitted) || !committedAt.Valid ||
			manifestVersion < 1 || len(manifestHash) != sha256.Size || len(manifestBytes) == 0 ||
			!bytes.Equal(manifestHash, computedManifestHash[:]) {
			return fmt.Errorf("%w: raw segment evidence does not match page %d coordinate", ErrInvalidCatalog, page.PageIndex)
		}

		var request struct {
			RequestID string `json:"request_id"`
		}
		if err := json.Unmarshal(page.RequestEvidence, &request); err != nil || request.RequestID == "" {
			return fmt.Errorf("%w: page %d request identity is invalid", ErrInvalidCatalog, page.PageIndex)
		}
		var envelopeVersion int16
		var recordRequestID string
		var payloadHash []byte
		var payloadByteLength int
		err = tx.QueryRow(ctx, `
SELECT envelope_version, request_id, payload_sha256, payload_byte_length
FROM raw_record_evidence
WHERE raw_segment_id = $1 AND arrival_ordinal = $2 AND message_ordinal = $3
FOR UPDATE
`, page.RawRecord.RawSegmentID, arrivalOrdinal, int32(page.RawRecord.MessageOrdinal)).Scan(
			&envelopeVersion, &recordRequestID, &payloadHash, &payloadByteLength,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: raw record evidence is absent for page %d", ErrInvalidCatalog, page.PageIndex)
		}
		if err != nil {
			return fmt.Errorf("catalog: lock raw record evidence: %w", err)
		}
		expectedPayloadHash, _ := hex.DecodeString(page.RawSHA256)
		if envelopeVersion != int16(page.RawRecord.EnvelopeVersion) ||
			recordRequestID != request.RequestID ||
			!bytes.Equal(payloadHash, expectedPayloadHash) ||
			payloadByteLength != page.RawByteLength {
			return fmt.Errorf("%w: raw record evidence does not match page %d payload", ErrInvalidCatalog, page.PageIndex)
		}
	}
	return nil
}

func recordSyncOpportunities(ctx context.Context, tx pgx.Tx, input SyncInput) error {
	for _, page := range input.Pages {
		var request struct {
			Version       uint16          `json:"version"`
			Kind          string          `json:"kind"`
			RequestID     string          `json:"request_id"`
			Method        string          `json:"method"`
			Parameters    json.RawMessage `json:"sanitized_parameters"`
			ScheduledAtNS int64           `json:"scheduled_at_ns"`
			StartedAtNS   int64           `json:"started_at_ns"`
		}
		if err := json.Unmarshal(page.RequestEvidence, &request); err != nil {
			return fmt.Errorf("catalog: decode request opportunity evidence: %w", err)
		}
		var response struct {
			Version       uint16 `json:"version"`
			Kind          string `json:"kind"`
			RequestID     string `json:"request_id"`
			CompletedAtNS int64  `json:"completed_at_ns"`
			Status        int    `json:"status"`
		}
		if err := json.Unmarshal(page.ResponseEvidence, &response); err != nil {
			return fmt.Errorf("catalog: decode response opportunity evidence: %w", err)
		}
		if request.Version != 1 || request.Kind != "request" || request.RequestID == "" || request.Method != "GET" ||
			response.Version != 1 || response.Kind != "response" || response.RequestID != request.RequestID ||
			request.ScheduledAtNS < 0 || request.StartedAtNS < request.ScheduledAtNS ||
			response.CompletedAtNS < request.StartedAtNS || response.Status != 200 ||
			len(request.Parameters) == 0 || !json.Valid(request.Parameters) {
			return fmt.Errorf("%w: inconsistent scheduled opportunity page evidence", ErrInvalidCatalog)
		}
		requestIdentityBytes, err := json.Marshal(struct {
			SourceID      string          `json:"source_id"`
			ChannelID     string          `json:"channel_id"`
			PageIndex     int             `json:"page_index"`
			PageCount     int             `json:"page_count"`
			RequestID     string          `json:"request_id"`
			Method        string          `json:"method"`
			Parameters    json.RawMessage `json:"sanitized_parameters"`
			ScheduledAtNS int64           `json:"scheduled_at_ns"`
		}{
			SourceID: input.Source.SourceID, ChannelID: page.ChannelID,
			PageIndex: page.PageIndex, PageCount: page.PageCount,
			RequestID: request.RequestID, Method: request.Method,
			Parameters: request.Parameters, ScheduledAtNS: request.ScheduledAtNS,
		})
		if err != nil {
			return fmt.Errorf("catalog: encode request opportunity identity: %w", err)
		}
		requestIdentity, err := CanonicalJSON(requestIdentityBytes)
		if err != nil {
			return err
		}
		terminalOutcomeBytes, err := json.Marshal(struct {
			StartedAtNS   int64               `json:"started_at_ns"`
			Response      json.RawMessage     `json:"response"`
			RawRecord     RawRecordCoordinate `json:"raw_record"`
			RawSHA256     string              `json:"raw_sha256"`
			RawByteLength int                 `json:"raw_byte_length"`
		}{
			StartedAtNS: request.StartedAtNS, Response: page.ResponseEvidence,
			RawRecord: page.RawRecord, RawSHA256: page.RawSHA256, RawByteLength: page.RawByteLength,
		})
		if err != nil {
			return fmt.Errorf("catalog: encode response opportunity outcome: %w", err)
		}
		terminalOutcome, err := CanonicalJSON(terminalOutcomeBytes)
		if err != nil {
			return err
		}
		opportunityID := deterministicCatalogUUID("catalog-sync-opportunity-v1", string(requestIdentity))

		var existingSourceID, existingChannelID, kind, state string
		var expectedTimeNS, windowStartNS, windowEndNS, terminalTimeNS int64
		var existingRequestIdentity, existingTerminalOutcome []byte
		err = tx.QueryRow(ctx, `
SELECT source_id::text, channel_id, opportunity_kind, expected_time_ns,
       window_start_ns, window_end_ns, request_identity, state::text,
       terminal_time_ns, terminal_outcome
FROM opportunity
WHERE opportunity_id = $1
FOR UPDATE
`, opportunityID).Scan(
			&existingSourceID, &existingChannelID, &kind, &expectedTimeNS,
			&windowStartNS, &windowEndNS, &existingRequestIdentity, &state,
			&terminalTimeNS, &existingTerminalOutcome,
		)
		if errors.Is(err, pgx.ErrNoRows) {
			_, err = tx.Exec(ctx, `
INSERT INTO opportunity (
    opportunity_id, ledger_partition, source_id, channel_id, instrument_uid,
    opportunity_kind, expected_time_ns, window_start_ns, window_end_ns,
    request_identity, state, terminal_time_ns, terminal_outcome, created_at
) VALUES (
    $1, 'catalog-sync', $2, $3, NULL,
    'metadata_sync', $4, $4, $5,
    $6::jsonb, 'observed', $5, $7::jsonb, $8
)
`, opportunityID, input.Source.SourceID, page.ChannelID,
				request.ScheduledAtNS, response.CompletedAtNS, requestIdentity, terminalOutcome, input.ObservedAt)
			if err != nil {
				return fmt.Errorf("catalog: record scheduled sync opportunity: %w", err)
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("catalog: lock scheduled sync opportunity: %w", err)
		}
		canonicalExistingRequest, requestErr := CanonicalJSON(existingRequestIdentity)
		canonicalExistingOutcome, outcomeErr := CanonicalJSON(existingTerminalOutcome)
		if requestErr != nil || outcomeErr != nil ||
			existingSourceID != input.Source.SourceID || existingChannelID != page.ChannelID ||
			kind != "metadata_sync" || state != "observed" ||
			expectedTimeNS != request.ScheduledAtNS || windowStartNS != request.ScheduledAtNS ||
			windowEndNS != response.CompletedAtNS || terminalTimeNS != response.CompletedAtNS ||
			!bytes.Equal(canonicalExistingRequest, requestIdentity) ||
			!bytes.Equal(canonicalExistingOutcome, terminalOutcome) {
			return fmt.Errorf("%w: scheduled sync opportunity replay conflicts with its terminal outcome", ErrInvalidCatalog)
		}
	}
	return nil
}

func insertSyncRun(ctx context.Context, tx pgx.Tx, input SyncInput, snapshot Snapshot) error {
	pages, err := json.Marshal(input.Pages)
	if err != nil {
		return fmt.Errorf("catalog: encode sync page evidence: %w", err)
	}
	canonicalPages, err := CanonicalJSON(pages)
	if err != nil {
		return err
	}
	runDocument := struct {
		SourceID   string          `json:"source_id"`
		ObservedAt string          `json:"observed_at"`
		Pages      json.RawMessage `json:"pages"`
	}{SourceID: input.Source.SourceID, ObservedAt: input.ObservedAt.Format(time.RFC3339Nano), Pages: canonicalPages}
	runBytes, err := json.Marshal(runDocument)
	if err != nil {
		return fmt.Errorf("catalog: encode sync identity: %w", err)
	}
	runHash := sha256.Sum256(runBytes)
	_, err = tx.Exec(ctx, `
INSERT INTO catalog_sync_run (
    sync_run_sha256, source_id, observed_at, page_count, page_evidence, snapshot_sha256
) VALUES ($1, $2, $3, $4, $5::jsonb, $6)
ON CONFLICT (sync_run_sha256) DO NOTHING
`, runHash[:], input.Source.SourceID, input.ObservedAt, len(input.Pages), canonicalPages, snapshot.SHA256[:])
	if err != nil {
		return fmt.Errorf("catalog: insert sync evidence: %w", err)
	}
	var existingSnapshot []byte
	var existingPages []byte
	if err := tx.QueryRow(ctx, `
SELECT snapshot_sha256, page_evidence
FROM catalog_sync_run WHERE sync_run_sha256 = $1
`, runHash[:]).Scan(&existingSnapshot, &existingPages); err != nil {
		return fmt.Errorf("catalog: verify sync evidence: %w", err)
	}
	canonicalExisting, err := CanonicalJSON(existingPages)
	if err != nil {
		return err
	}
	if !bytes.Equal(existingSnapshot, snapshot.SHA256[:]) || !bytes.Equal(canonicalExisting, canonicalPages) {
		return errors.New("catalog: immutable sync identity conflict")
	}
	return nil
}

func sourceVersionsEqual(a, b SourceVersion) (bool, error) {
	left, err := canonicalSourceVersion(a)
	if err != nil {
		return false, err
	}
	right, err := canonicalSourceVersion(b)
	return bytes.Equal(left, right), err
}

func canonicalSourceVersion(value SourceVersion) ([]byte, error) {
	canonical, err := canonicalSnapshotSourceVersion(value)
	if err != nil {
		return nil, err
	}
	return json.Marshal(canonical)
}

func channelsEqual(a, b ChannelContract) (bool, error) {
	left, err := canonicalSnapshotChannel(a)
	if err != nil {
		return false, err
	}
	right, err := canonicalSnapshotChannel(b)
	if err != nil {
		return false, err
	}
	leftBytes, err := json.Marshal(left)
	if err != nil {
		return false, err
	}
	rightBytes, err := json.Marshal(right)
	return bytes.Equal(leftBytes, rightBytes), err
}

func deterministicInstrumentUID(sourceID, nativeID string, generation int64) string {
	return deterministicCatalogUUID(
		"catalog-instrument-v1",
		fmt.Sprintf("%s\x00%s\x00%d", sourceID, nativeID, generation),
	)
}

func deterministicCatalogUUID(namespace, identity string) string {
	digest := sha256.Sum256([]byte(namespace + "\x00" + identity))
	bytes := digest[:16]
	bytes[6] = (bytes[6] & 0x0f) | 0x50
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	hexValue := hex.EncodeToString(bytes)
	return hexValue[0:8] + "-" + hexValue[8:12] + "-" + hexValue[12:16] + "-" + hexValue[16:20] + "-" + hexValue[20:32]
}
