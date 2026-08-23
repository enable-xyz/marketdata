package catalog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const (
	MapperEvidenceVersion         uint16 = 1
	NormalizedSchemaVersionV1            = "v1"
	MapperSelectionReceivedWall          = "received_wall_time_ns"
	MaxMapperSchemaFingerprints          = 256
	MaxMapperEvidenceFields              = 400_000
	MaxMapperRejectionCodes              = 256
	MaxMapperEvidenceJSONBytes           = 128 << 20
	MaxMapperRejectionDetailBytes        = 32 << 10
)

var (
	ErrInvalidMapperPublication   = errors.New("catalog: invalid mapper publication")
	ErrMapperPublicationConflict  = errors.New("catalog: mapper publication conflict")
	ErrMapperIntervalConflict     = errors.New("catalog: mapper effective interval conflict")
	ErrMapperEvidenceMismatch     = errors.New("catalog: mapper dual-run evidence mismatch")
	ErrMapperSelectionUnavailable = errors.New("catalog: mapper selection unavailable")
	ErrMapperSelectionAmbiguous   = errors.New("catalog: mapper selection ambiguous")
)

type MapperBindingState string

const (
	MapperBindingCandidate MapperBindingState = "candidate"
	MapperBindingDualRun   MapperBindingState = "dual_run"
	MapperBindingActive    MapperBindingState = "active"
	MapperBindingRetired   MapperBindingState = "retired"
	MapperBindingRejected  MapperBindingState = "rejected"
)

type MapperEvidenceDecision string

const (
	MapperEvidenceAccepted MapperEvidenceDecision = "accepted"
	MapperEvidenceRejected MapperEvidenceDecision = "rejected"
)

// MapperRelease is the immutable identity of one mapper build. ReleaseID is a
// deterministic UUID derived from every other field by NewMapperRelease.
type MapperRelease struct {
	ReleaseID               string
	MapperVersion           string
	BuildIdentity           string
	NormalizedSchemaVersion string
	FixtureBundleSHA256     [sha256.Size]byte
	SourceCodeIdentity      string
}

// MapperSchemaFingerprint binds a normalized row name and both of its frozen
// encoding versions to the exact schema bytes accepted by a mapper.
type MapperSchemaFingerprint struct {
	Name                   string
	Version                uint16
	LogicalEncodingVersion uint16
	SHA256                 [sha256.Size]byte
}

// MapperRunEvidence is one side of a deterministic replay comparison.
// LogicalSHA256 retains full release provenance and may differ. SemanticSHA256
// and the ordered Rejections/RejectionSHA256 projection are release-neutral
// equivalence evidence and must match before publication.
type MapperRunEvidence struct {
	AcceptedFields       []string
	RejectionCounts      map[string]uint64
	LogicalSHA256        [sha256.Size]byte
	SemanticSHA256       [sha256.Size]byte
	Rejections           []string
	RejectionSHA256      [sha256.Size]byte
	DownstreamBookResult string
	DownstreamBookSHA256 [sha256.Size]byte
}

// DualRunEvidence is the single durable comparison object required for a
// mapper cutover. Its time basis is always capture receive-wall time; exchange
// timestamps are deliberately absent from this contract.
type DualRunEvidence struct {
	Version            uint16
	SourceID           string
	ChannelID          string
	SelectionTimeBasis string
	ReceivedStartNS    int64
	ReceivedEndNS      int64
	CorpusCount        uint64
	CorpusSHA256       [sha256.Size]byte
	OldMapperReleaseID string
	NewMapperReleaseID string
	Old                MapperRunEvidence
	New                MapperRunEvidence
	MismatchCodes      []string
	Mismatch           bool
	Decision           MapperEvidenceDecision
}

// MapperBinding is a PostgreSQL mapper_binding publication. EffectiveTo nil is
// open-ended. Boundaries must be exact UTC time.Time values and are persisted
// alongside canonical Unix nanoseconds so PostgreSQL microsecond timestamps do
// not alter cutover selection.
type MapperBinding struct {
	SourceID           string
	ChannelID          string
	EffectiveFrom      time.Time
	EffectiveTo        *time.Time
	MapperReleaseID    string
	SchemaFingerprints []MapperSchemaFingerprint
	DualRunEvidence    DualRunEvidence
	State              MapperBindingState
}

type mapperStreamKey struct {
	sourceID  string
	channelID string
}

// MapperCutover is an immutable, in-memory projection of catalog mapper
// publications. It validates the same interval and evidence contracts as the
// PostgreSQL publication methods, but runtime mapping remains normalize-owned.
type MapperCutover struct {
	releases map[string]MapperRelease
	streams  map[mapperStreamKey][]MapperBinding
}

func NewMapperRelease(mapperVersion, buildIdentity, normalizedSchemaVersion string, fixtureBundleSHA256 [sha256.Size]byte, sourceCodeIdentity string) (MapperRelease, error) {
	release := MapperRelease{
		MapperVersion: mapperVersion, BuildIdentity: buildIdentity,
		NormalizedSchemaVersion: normalizedSchemaVersion,
		FixtureBundleSHA256:     fixtureBundleSHA256, SourceCodeIdentity: sourceCodeIdentity,
	}
	release.ReleaseID = deterministicMapperReleaseID(release)
	if err := release.Validate(); err != nil {
		return MapperRelease{}, err
	}
	return release, nil
}

func (r MapperRelease) Validate() error {
	for name, value := range map[string]string{
		"mapper_version": r.MapperVersion, "build_identity": r.BuildIdentity,
		"normalized_schema_version": r.NormalizedSchemaVersion,
		"source_code_identity":      r.SourceCodeIdentity,
	} {
		if err := validateMapperText(name, value); err != nil {
			return err
		}
	}
	if r.NormalizedSchemaVersion != NormalizedSchemaVersionV1 {
		return fmt.Errorf("%w: normalized schema version %q is not frozen v1", ErrInvalidMapperPublication, r.NormalizedSchemaVersion)
	}
	if r.FixtureBundleSHA256 == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: fixture bundle SHA-256 is empty", ErrInvalidMapperPublication)
	}
	if !validUUID(r.ReleaseID) || r.ReleaseID != deterministicMapperReleaseID(r) {
		return fmt.Errorf("%w: mapper release ID is not its deterministic immutable identity", ErrInvalidMapperPublication)
	}
	return nil
}

func deterministicMapperReleaseID(r MapperRelease) string {
	identity := strings.Join([]string{
		r.MapperVersion,
		r.BuildIdentity,
		r.NormalizedSchemaVersion,
		hex.EncodeToString(r.FixtureBundleSHA256[:]),
		r.SourceCodeIdentity,
	}, "\x00")
	return deterministicCatalogUUID("catalog-mapper-release-v1", identity)
}

func (e DualRunEvidence) IsZero() bool {
	return e.Version == 0 && e.SourceID == "" && e.ChannelID == "" && e.SelectionTimeBasis == "" &&
		e.ReceivedStartNS == 0 && e.ReceivedEndNS == 0 && e.CorpusCount == 0 &&
		e.CorpusSHA256 == ([sha256.Size]byte{}) && e.OldMapperReleaseID == "" &&
		e.NewMapperReleaseID == "" && mapperRunEvidenceZero(e.Old) && mapperRunEvidenceZero(e.New) &&
		len(e.MismatchCodes) == 0 && !e.Mismatch && e.Decision == ""
}

func (e DualRunEvidence) Validate() error {
	_, err := canonicalDualRunEvidence(e)
	return err
}

func (e DualRunEvidence) SHA256() ([sha256.Size]byte, error) {
	canonical, err := canonicalDualRunEvidence(e)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	encoded, err := encodeDualRunEvidence(canonical)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (e DualRunEvidence) Publishable() bool {
	canonical, err := canonicalDualRunEvidence(e)
	return err == nil && !canonical.Mismatch && canonical.Decision == MapperEvidenceAccepted
}

func (b MapperBinding) Validate() error {
	_, err := canonicalMapperBinding(b)
	return err
}

func NewMapperCutover(releases []MapperRelease, bindings []MapperBinding) (*MapperCutover, error) {
	if len(releases) == 0 || len(bindings) == 0 {
		return nil, fmt.Errorf("%w: releases and bindings are required", ErrInvalidMapperPublication)
	}
	cutover := &MapperCutover{
		releases: make(map[string]MapperRelease, len(releases)),
		streams:  make(map[mapperStreamKey][]MapperBinding),
	}
	for _, release := range releases {
		if err := release.Validate(); err != nil {
			return nil, err
		}
		if _, exists := cutover.releases[release.ReleaseID]; exists {
			return nil, fmt.Errorf("%w: duplicate release %s", ErrMapperPublicationConflict, release.ReleaseID)
		}
		cutover.releases[release.ReleaseID] = release
	}
	for _, supplied := range bindings {
		binding, err := canonicalMapperBinding(supplied)
		if err != nil {
			return nil, err
		}
		if err := validateBindingReleaseReferences(binding, cutover.releases); err != nil {
			return nil, err
		}
		key := mapperStreamKey{sourceID: binding.SourceID, channelID: binding.ChannelID}
		cutover.streams[key] = append(cutover.streams[key], binding)
	}
	for key, stream := range cutover.streams {
		slices.SortFunc(stream, compareMapperBindings)
		for i := 1; i < len(stream); i++ {
			previousEnd, open := mapperEffectiveToNS(stream[i-1])
			currentStart, _ := mapperBoundaryNS("effective_from", stream[i].EffectiveFrom)
			if open || currentStart < previousEnd {
				return nil, fmt.Errorf("%w: overlapping %s/%s mapper bindings", ErrMapperIntervalConflict, key.sourceID, key.channelID)
			}
		}
		cutover.streams[key] = stream
	}
	return cutover, nil
}

// ValidateSelection proves that the published set has exactly one eligible
// binding at a captured receive-wall time. It returns no mapper: normalize's
// Orchestrator remains the sole runtime selector and mapper executor.
func (c *MapperCutover) ValidateSelection(sourceID, channelID string, receivedWallTimeNS int64) error {
	_, err := c.publishedBindingAt(sourceID, channelID, receivedWallTimeNS)
	return err
}

func (c *MapperCutover) publishedBindingAt(sourceID, channelID string, receivedWallTimeNS int64) (MapperBinding, error) {
	if c == nil || !validUUID(sourceID) || validateMapperText("channel_id", channelID) != nil || receivedWallTimeNS < 0 {
		return MapperBinding{}, fmt.Errorf("%w: invalid receive-time selection identity", ErrInvalidMapperPublication)
	}
	stream := c.streams[mapperStreamKey{sourceID: sourceID, channelID: channelID}]
	index := sort.Search(len(stream), func(i int) bool {
		from, _ := mapperBoundaryNS("effective_from", stream[i].EffectiveFrom)
		return from > receivedWallTimeNS
	}) - 1
	if index < 0 {
		return MapperBinding{}, fmt.Errorf("%w: no mapper publication for %s/%s at receive time %d", ErrMapperSelectionUnavailable, sourceID, channelID, receivedWallTimeNS)
	}
	binding := stream[index]
	from, _ := mapperBoundaryNS("effective_from", binding.EffectiveFrom)
	to, open := mapperEffectiveToNS(binding)
	if receivedWallTimeNS < from || (!open && receivedWallTimeNS >= to) || !mapperStateSelectable(binding.State) {
		return MapperBinding{}, fmt.Errorf("%w: no mapper publication for %s/%s at receive time %d", ErrMapperSelectionUnavailable, sourceID, channelID, receivedWallTimeNS)
	}
	if _, ok := c.releases[binding.MapperReleaseID]; !ok {
		return MapperBinding{}, fmt.Errorf("%w: published mapper release is absent", ErrMapperSelectionUnavailable)
	}
	return cloneMapperBinding(binding), nil
}

func (s *PublicationStore) PublishMapperRelease(ctx context.Context, supplied MapperRelease) (err error) {
	if err := supplied.Validate(); err != nil {
		return err
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("catalog: begin mapper release publication: %w", err)
	}
	defer rollbackMapperTx(ctx, tx, &err, "mapper release publication")
	if _, err := tx.Exec(ctx, `
INSERT INTO mapper_release (
    mapper_release_id, mapper_version, build_identity, normalized_schema_version,
    fixture_bundle_hash, source_code_identity
) VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT DO NOTHING
`, supplied.ReleaseID, supplied.MapperVersion, supplied.BuildIdentity, supplied.NormalizedSchemaVersion,
		supplied.FixtureBundleSHA256[:], supplied.SourceCodeIdentity); err != nil {
		return fmt.Errorf("catalog: insert mapper release: %w", err)
	}
	existing, found, err := findMapperRelease(ctx, tx, supplied.ReleaseID)
	if err != nil {
		return err
	}
	if !found || existing != supplied {
		return fmt.Errorf("%w: release %s has different immutable content", ErrMapperPublicationConflict, supplied.ReleaseID)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: commit mapper release publication: %w", err)
	}
	return nil
}

func (s *PublicationStore) PublishMapperBinding(ctx context.Context, supplied MapperBinding) (err error) {
	binding, err := canonicalMapperBinding(supplied)
	if err != nil {
		return err
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("catalog: begin mapper binding publication: %w", err)
	}
	defer rollbackMapperTx(ctx, tx, &err, "mapper binding publication")
	if err := validateStoredBindingReleaseReferences(ctx, tx, binding); err != nil {
		return err
	}
	if err := insertMapperBinding(ctx, tx, binding); err != nil {
		return err
	}
	existing, found, err := findMapperBinding(ctx, tx, binding.SourceID, binding.ChannelID, mustMapperBoundaryNS(binding.EffectiveFrom), true)
	if err != nil {
		return err
	}
	if !found || !mapperBindingsEqual(existing, binding) {
		return fmt.Errorf("%w: mapper binding identity has different immutable content", ErrMapperPublicationConflict)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: commit mapper binding publication: %w", err)
	}
	return nil
}

// ActivateMapperBinding atomically attaches the final dual-run object and moves
// a candidate or dual-run row to active. Incomplete or mismatching evidence is
// rejected before any catalog mutation.
func (s *PublicationStore) ActivateMapperBinding(ctx context.Context, sourceID, channelID string, effectiveFrom time.Time, evidence DualRunEvidence) (err error) {
	fromNS, err := mapperBoundaryNS("effective_from", effectiveFrom)
	if err != nil || !validUUID(sourceID) || validateMapperText("channel_id", channelID) != nil {
		return fmt.Errorf("%w: invalid mapper binding identity", ErrInvalidMapperPublication)
	}
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("catalog: begin mapper activation: %w", err)
	}
	defer rollbackMapperTx(ctx, tx, &err, "mapper activation")
	binding, found, err := findMapperBinding(ctx, tx, sourceID, channelID, fromNS, true)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("%w: mapper binding does not exist", ErrMapperPublicationConflict)
	}
	binding.DualRunEvidence = evidence
	binding.State = MapperBindingActive
	binding, err = canonicalMapperBinding(binding)
	if err != nil {
		return err
	}
	if err := validateStoredBindingReleaseReferences(ctx, tx, binding); err != nil {
		return err
	}
	evidenceBytes, err := encodeDualRunEvidence(binding.DualRunEvidence)
	if err != nil {
		return err
	}
	current, _, err := findMapperBinding(ctx, tx, sourceID, channelID, fromNS, false)
	if err != nil {
		return err
	}
	switch current.State {
	case MapperBindingCandidate, MapperBindingDualRun:
		if _, err := tx.Exec(ctx, `
UPDATE mapper_binding
SET dual_run_evidence = $4::jsonb, state = 'active'
WHERE source_id = $1 AND channel_id = $2 AND effective_from_ns = $3
`, sourceID, channelID, fromNS, evidenceBytes); err != nil {
			return fmt.Errorf("catalog: activate mapper binding: %w", err)
		}
	case MapperBindingActive:
		if !mapperBindingsEqual(current, binding) {
			return fmt.Errorf("%w: active mapper binding has different evidence", ErrMapperPublicationConflict)
		}
	case MapperBindingRetired, MapperBindingRejected:
		return fmt.Errorf("%w: cannot activate mapper binding in state %q", ErrMapperPublicationConflict, current.State)
	default:
		return fmt.Errorf("%w: unknown mapper binding state %q", ErrInvalidMapperPublication, current.State)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: commit mapper activation: %w", err)
	}
	return nil
}

// PublishMapperCutover closes the current open active binding at the next
// binding's exact receive-time boundary and publishes the next active binding
// in one transaction. The evidence must name the current release as old.
func (s *PublicationStore) PublishMapperCutover(ctx context.Context, supplied MapperBinding) (err error) {
	next, err := canonicalMapperBinding(supplied)
	if err != nil {
		return err
	}
	if next.State != MapperBindingActive {
		return fmt.Errorf("%w: cutover target must be active", ErrInvalidMapperPublication)
	}
	boundaryNS := mustMapperBoundaryNS(next.EffectiveFrom)
	tx, err := s.database.Begin(ctx)
	if err != nil {
		return fmt.Errorf("catalog: begin mapper cutover: %w", err)
	}
	defer rollbackMapperTx(ctx, tx, &err, "mapper cutover")
	if err := validateStoredBindingReleaseReferences(ctx, tx, next); err != nil {
		return err
	}

	current, found, err := findOpenActiveMapperBinding(ctx, tx, next.SourceID, next.ChannelID, boundaryNS)
	if err != nil {
		return err
	}
	if !found {
		existing, exists, findErr := findMapperBinding(ctx, tx, next.SourceID, next.ChannelID, boundaryNS, true)
		if findErr != nil {
			return findErr
		}
		if exists && mapperBindingsEqual(existing, next) {
			if err := tx.Commit(ctx); err != nil {
				return fmt.Errorf("catalog: commit idempotent mapper cutover: %w", err)
			}
			return nil
		}
		return fmt.Errorf("%w: no open active mapper binding precedes cutover", ErrMapperSelectionUnavailable)
	}
	if current.MapperReleaseID != next.DualRunEvidence.OldMapperReleaseID {
		return fmt.Errorf("%w: dual-run old release does not match current active binding", ErrMapperPublicationConflict)
	}
	currentFrom := mustMapperBoundaryNS(current.EffectiveFrom)
	if currentFrom >= boundaryNS {
		return fmt.Errorf("%w: cutover boundary does not follow current binding", ErrMapperIntervalConflict)
	}
	if _, err := tx.Exec(ctx, `
UPDATE mapper_binding
SET effective_to = $4, effective_to_ns = $5, state = 'retired'
WHERE source_id = $1 AND channel_id = $2 AND effective_from_ns = $3 AND state = 'active'
`, current.SourceID, current.ChannelID, currentFrom, next.EffectiveFrom, boundaryNS); err != nil {
		return fmt.Errorf("catalog: retire current mapper binding: %w", err)
	}
	if err := insertMapperBinding(ctx, tx, next); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("catalog: commit mapper cutover: %w", err)
	}
	return nil
}

// ValidateMapperSelection fails closed unless the persisted publication set has
// exactly one eligible interval at a capture receive-wall nanosecond. It
// validates catalog coverage only and never returns or executes a mapper.
func (s *PublicationStore) ValidateMapperSelection(ctx context.Context, sourceID, channelID string, receivedWallTimeNS int64) error {
	if !validUUID(sourceID) || validateMapperText("channel_id", channelID) != nil || receivedWallTimeNS < 0 {
		return fmt.Errorf("%w: invalid receive-time selection identity", ErrInvalidMapperPublication)
	}
	var count int
	if err := s.database.QueryRow(ctx, `
SELECT count(*)
FROM (
    SELECT 1
    FROM mapper_binding
    WHERE source_id = $1 AND channel_id = $2
      AND effective_from_ns <= $3
      AND (effective_to_ns IS NULL OR $3 < effective_to_ns)
      AND state IN ('active', 'retired')
    LIMIT 2
) AS eligible
`, sourceID, channelID, receivedWallTimeNS).Scan(&count); err != nil {
		return fmt.Errorf("catalog: validate mapper selection: %w", err)
	}
	switch count {
	case 0:
		return fmt.Errorf("%w: no mapper publication for %s/%s at receive time %d", ErrMapperSelectionUnavailable, sourceID, channelID, receivedWallTimeNS)
	case 1:
		return nil
	default:
		return fmt.Errorf("%w: multiple mapper publications for %s/%s at receive time %d", ErrMapperSelectionAmbiguous, sourceID, channelID, receivedWallTimeNS)
	}
}

func canonicalMapperBinding(binding MapperBinding) (MapperBinding, error) {
	if !validUUID(binding.SourceID) || !validUUID(binding.MapperReleaseID) {
		return MapperBinding{}, fmt.Errorf("%w: binding source and release IDs must be canonical UUIDs", ErrInvalidMapperPublication)
	}
	if err := validateMapperText("channel_id", binding.ChannelID); err != nil {
		return MapperBinding{}, err
	}
	fromNS, err := mapperBoundaryNS("effective_from", binding.EffectiveFrom)
	if err != nil {
		return MapperBinding{}, err
	}
	if binding.EffectiveTo != nil {
		toNS, boundaryErr := mapperBoundaryNS("effective_to", *binding.EffectiveTo)
		if boundaryErr != nil {
			return MapperBinding{}, boundaryErr
		}
		if toNS <= fromNS {
			return MapperBinding{}, fmt.Errorf("%w: mapper interval is not ordered", ErrMapperIntervalConflict)
		}
		to := time.Unix(0, toNS).UTC()
		binding.EffectiveTo = &to
	}
	binding.EffectiveFrom = time.Unix(0, fromNS).UTC()
	if !validMapperBindingState(binding.State) {
		return MapperBinding{}, fmt.Errorf("%w: unknown mapper binding state %q", ErrInvalidMapperPublication, binding.State)
	}
	if len(binding.SchemaFingerprints) == 0 || len(binding.SchemaFingerprints) > MaxMapperSchemaFingerprints {
		return MapperBinding{}, fmt.Errorf("%w: schema fingerprint count", ErrInvalidMapperPublication)
	}
	binding.SchemaFingerprints = slices.Clone(binding.SchemaFingerprints)
	for i := range binding.SchemaFingerprints {
		fingerprint := binding.SchemaFingerprints[i]
		if err := validateMapperText("schema_fingerprint.name", fingerprint.Name); err != nil {
			return MapperBinding{}, err
		}
		if fingerprint.Version != 1 || fingerprint.LogicalEncodingVersion != 1 || fingerprint.SHA256 == ([sha256.Size]byte{}) {
			return MapperBinding{}, fmt.Errorf("%w: schema fingerprint is not a complete v1 identity", ErrInvalidMapperPublication)
		}
	}
	slices.SortFunc(binding.SchemaFingerprints, compareMapperSchemaFingerprints)
	for i := 1; i < len(binding.SchemaFingerprints); i++ {
		previous, current := binding.SchemaFingerprints[i-1], binding.SchemaFingerprints[i]
		if previous.Name == current.Name && previous.Version == current.Version {
			return MapperBinding{}, fmt.Errorf("%w: duplicate schema fingerprint %s/v%d", ErrInvalidMapperPublication, current.Name, current.Version)
		}
	}

	requiresEvidence := binding.State == MapperBindingActive || binding.State == MapperBindingRetired
	if binding.DualRunEvidence.IsZero() {
		if requiresEvidence {
			return MapperBinding{}, fmt.Errorf("%w: active or retired binding lacks dual-run evidence", ErrInvalidMapperPublication)
		}
		return binding, nil
	}
	evidence, err := canonicalDualRunEvidence(binding.DualRunEvidence)
	if err != nil {
		return MapperBinding{}, err
	}
	binding.DualRunEvidence = evidence
	if evidence.SourceID != binding.SourceID || evidence.ChannelID != binding.ChannelID || evidence.NewMapperReleaseID != binding.MapperReleaseID {
		return MapperBinding{}, fmt.Errorf("%w: dual-run evidence does not identify its binding", ErrInvalidMapperPublication)
	}
	if requiresEvidence && (evidence.Mismatch || evidence.Decision != MapperEvidenceAccepted) {
		return MapperBinding{}, fmt.Errorf("%w: binding state %q requires matching accepted evidence", ErrMapperEvidenceMismatch, binding.State)
	}
	return binding, nil
}

func canonicalDualRunEvidence(evidence DualRunEvidence) (DualRunEvidence, error) {
	if evidence.Version != MapperEvidenceVersion || evidence.SelectionTimeBasis != MapperSelectionReceivedWall ||
		!validUUID(evidence.SourceID) || !validUUID(evidence.OldMapperReleaseID) || !validUUID(evidence.NewMapperReleaseID) ||
		evidence.OldMapperReleaseID == evidence.NewMapperReleaseID || evidence.ReceivedStartNS < 0 ||
		evidence.ReceivedEndNS < evidence.ReceivedStartNS || evidence.CorpusCount == 0 ||
		evidence.CorpusSHA256 == ([sha256.Size]byte{}) {
		return DualRunEvidence{}, fmt.Errorf("%w: incomplete dual-run identity or receive-time corpus", ErrInvalidMapperPublication)
	}
	if err := validateMapperText("dual_run.channel_id", evidence.ChannelID); err != nil {
		return DualRunEvidence{}, err
	}
	oldSize, err := preflightMapperRunEvidence(evidence.Old)
	if err != nil {
		return DualRunEvidence{}, fmt.Errorf("%w: old run: %v", ErrInvalidMapperPublication, err)
	}
	newSize, err := preflightMapperRunEvidence(evidence.New)
	if err != nil {
		return DualRunEvidence{}, fmt.Errorf("%w: new run: %v", ErrInvalidMapperPublication, err)
	}
	if len(evidence.MismatchCodes) > MaxMapperRejectionCodes {
		return DualRunEvidence{}, fmt.Errorf("%w: mismatch code count", ErrInvalidMapperPublication)
	}
	encodedUpperBound := 16 << 10
	if !addMapperEvidenceSize(&encodedUpperBound, oldSize) ||
		!addMapperEvidenceSize(&encodedUpperBound, newSize) ||
		!addMapperEvidenceStringSize(&encodedUpperBound, evidence.ChannelID) {
		return DualRunEvidence{}, fmt.Errorf("%w: mapper dual-run evidence exceeds %d bytes", ErrInvalidMapperPublication, MaxMapperEvidenceJSONBytes)
	}
	for _, code := range evidence.MismatchCodes {
		if err := validateMapperText("mismatch_code", code); err != nil {
			return DualRunEvidence{}, err
		}
		if !addMapperEvidenceStringSize(&encodedUpperBound, code) {
			return DualRunEvidence{}, fmt.Errorf("%w: mapper dual-run evidence exceeds %d bytes", ErrInvalidMapperPublication, MaxMapperEvidenceJSONBytes)
		}
	}

	evidence.Old = cloneCanonicalMapperRunEvidence(evidence.Old)
	evidence.New = cloneCanonicalMapperRunEvidence(evidence.New)
	if err := validateSortedAcceptedFields("old", evidence.Old.AcceptedFields); err != nil {
		return DualRunEvidence{}, err
	}
	if err := validateSortedAcceptedFields("new", evidence.New.AcceptedFields); err != nil {
		return DualRunEvidence{}, err
	}
	evidence.MismatchCodes = slices.Clone(evidence.MismatchCodes)
	slices.Sort(evidence.MismatchCodes)
	for i, code := range evidence.MismatchCodes {
		if i > 0 && code == evidence.MismatchCodes[i-1] {
			return DualRunEvidence{}, fmt.Errorf("%w: duplicate mismatch code %q", ErrInvalidMapperPublication, code)
		}
	}
	computedMismatch := !mapperRunEvidenceEqual(evidence.Old, evidence.New)
	if evidence.Mismatch != computedMismatch || evidence.Mismatch != (len(evidence.MismatchCodes) != 0) {
		return DualRunEvidence{}, fmt.Errorf("%w: mismatch decision disagrees with durable run summaries", ErrMapperEvidenceMismatch)
	}
	switch evidence.Decision {
	case MapperEvidenceAccepted:
		if evidence.Mismatch {
			return DualRunEvidence{}, fmt.Errorf("%w: mismatching evidence cannot be accepted", ErrMapperEvidenceMismatch)
		}
	case MapperEvidenceRejected:
		if !evidence.Mismatch {
			return DualRunEvidence{}, fmt.Errorf("%w: matching evidence cannot carry a mismatch rejection", ErrMapperEvidenceMismatch)
		}
	default:
		return DualRunEvidence{}, fmt.Errorf("%w: dual-run mismatch decision is absent", ErrInvalidMapperPublication)
	}
	if _, err := encodeDualRunEvidence(evidence); err != nil {
		return DualRunEvidence{}, err
	}
	return evidence, nil
}

func preflightMapperRunEvidence(run MapperRunEvidence) (int, error) {
	if run.AcceptedFields == nil || len(run.AcceptedFields) > MaxMapperEvidenceFields ||
		run.RejectionCounts == nil || len(run.RejectionCounts) > MaxMapperRejectionCodes ||
		run.Rejections == nil || len(run.Rejections) > MaxMapperEvidenceFields ||
		run.LogicalSHA256 == ([sha256.Size]byte{}) || run.SemanticSHA256 == ([sha256.Size]byte{}) ||
		run.RejectionSHA256 == ([sha256.Size]byte{}) || run.DownstreamBookSHA256 == ([sha256.Size]byte{}) {
		return 0, fmt.Errorf("incomplete accepted fields, rejection evidence, logical/semantic hashes, or book hash")
	}
	if err := validateMapperText("downstream_book_result", run.DownstreamBookResult); err != nil {
		return 0, err
	}
	encodedUpperBound := 4 << 10
	if !addMapperEvidenceSize(&encodedUpperBound, len(run.AcceptedFields)*3) ||
		!addMapperEvidenceSize(&encodedUpperBound, len(run.Rejections)*3) ||
		!addMapperEvidenceSize(&encodedUpperBound, len(run.RejectionCounts)*32) ||
		!addMapperEvidenceStringSize(&encodedUpperBound, run.DownstreamBookResult) {
		return 0, fmt.Errorf("mapper run evidence exceeds %d bytes", MaxMapperEvidenceJSONBytes)
	}
	for _, field := range run.AcceptedFields {
		if err := validateMapperText("accepted_field", field); err != nil {
			return 0, err
		}
		if !addMapperEvidenceStringSize(&encodedUpperBound, field) {
			return 0, fmt.Errorf("mapper run evidence exceeds %d bytes", MaxMapperEvidenceJSONBytes)
		}
	}
	for code, count := range run.RejectionCounts {
		if err := validateMapperText("rejection_code", code); err != nil {
			return 0, err
		}
		if count == 0 {
			return 0, fmt.Errorf("zero rejection count for %q", code)
		}
		if !addMapperEvidenceStringSize(&encodedUpperBound, code) {
			return 0, fmt.Errorf("mapper run evidence exceeds %d bytes", MaxMapperEvidenceJSONBytes)
		}
	}
	for _, rejection := range run.Rejections {
		if err := validateMapperRejectionDetail(rejection); err != nil {
			return 0, err
		}
		if !addMapperEvidenceStringSize(&encodedUpperBound, rejection) {
			return 0, fmt.Errorf("mapper run evidence exceeds %d bytes", MaxMapperEvidenceJSONBytes)
		}
	}
	return encodedUpperBound, nil
}

func addMapperEvidenceStringSize(total *int, value string) bool {
	if len(value) > (MaxMapperEvidenceJSONBytes-*total-8)/6 {
		return false
	}
	return addMapperEvidenceSize(total, 8+len(value)*6)
}

func addMapperEvidenceSize(total *int, size int) bool {
	if size < 0 || *total > MaxMapperEvidenceJSONBytes-size {
		return false
	}
	*total += size
	return true
}

func cloneCanonicalMapperRunEvidence(run MapperRunEvidence) MapperRunEvidence {
	run.AcceptedFields = slices.Clone(run.AcceptedFields)
	slices.Sort(run.AcceptedFields)
	run.RejectionCounts = maps.Clone(run.RejectionCounts)
	run.Rejections = slices.Clone(run.Rejections)
	return run
}

func validateSortedAcceptedFields(run string, fields []string) error {
	for i := 1; i < len(fields); i++ {
		if fields[i] == fields[i-1] {
			return fmt.Errorf("%w: %s run duplicate accepted field %q", ErrInvalidMapperPublication, run, fields[i])
		}
	}
	return nil
}

func mapperRunEvidenceEqual(left, right MapperRunEvidence) bool {
	return slices.Equal(left.AcceptedFields, right.AcceptedFields) &&
		maps.Equal(left.RejectionCounts, right.RejectionCounts) &&
		left.SemanticSHA256 == right.SemanticSHA256 &&
		slices.Equal(left.Rejections, right.Rejections) &&
		left.RejectionSHA256 == right.RejectionSHA256 &&
		left.DownstreamBookResult == right.DownstreamBookResult &&
		left.DownstreamBookSHA256 == right.DownstreamBookSHA256
}

func mapperRunEvidenceZero(run MapperRunEvidence) bool {
	return len(run.AcceptedFields) == 0 && len(run.RejectionCounts) == 0 &&
		run.LogicalSHA256 == ([sha256.Size]byte{}) && run.SemanticSHA256 == ([sha256.Size]byte{}) &&
		len(run.Rejections) == 0 && run.RejectionSHA256 == ([sha256.Size]byte{}) &&
		run.DownstreamBookResult == "" && run.DownstreamBookSHA256 == ([sha256.Size]byte{})
}

func validateBindingReleaseReferences(binding MapperBinding, releases map[string]MapperRelease) error {
	newRelease, ok := releases[binding.MapperReleaseID]
	if !ok {
		return fmt.Errorf("%w: binding mapper release %s is absent", ErrMapperPublicationConflict, binding.MapperReleaseID)
	}
	if newRelease.NormalizedSchemaVersion != NormalizedSchemaVersionV1 {
		return fmt.Errorf("%w: binding release is not schema v1", ErrInvalidMapperPublication)
	}
	if !binding.DualRunEvidence.IsZero() {
		if _, ok := releases[binding.DualRunEvidence.OldMapperReleaseID]; !ok {
			return fmt.Errorf("%w: dual-run old release %s is absent", ErrMapperPublicationConflict, binding.DualRunEvidence.OldMapperReleaseID)
		}
		if _, ok := releases[binding.DualRunEvidence.NewMapperReleaseID]; !ok {
			return fmt.Errorf("%w: dual-run new release %s is absent", ErrMapperPublicationConflict, binding.DualRunEvidence.NewMapperReleaseID)
		}
	}
	return nil
}

func mapperBoundaryNS(name string, value time.Time) (int64, error) {
	if value.IsZero() || value.Location() != time.UTC {
		return 0, fmt.Errorf("%w: %s must be a nonzero exact UTC time", ErrInvalidMapperPublication, name)
	}
	ns := value.UnixNano()
	if ns < 0 || !time.Unix(0, ns).UTC().Equal(value) {
		return 0, fmt.Errorf("%w: %s is outside nonnegative Unix nanoseconds", ErrInvalidMapperPublication, name)
	}
	return ns, nil
}

func mustMapperBoundaryNS(value time.Time) int64 {
	ns, _ := mapperBoundaryNS("mapper_boundary", value)
	return ns
}

func mapperEffectiveToNS(binding MapperBinding) (int64, bool) {
	if binding.EffectiveTo == nil {
		return 0, true
	}
	return mustMapperBoundaryNS(*binding.EffectiveTo), false
}

func validMapperBindingState(state MapperBindingState) bool {
	switch state {
	case MapperBindingCandidate, MapperBindingDualRun, MapperBindingActive, MapperBindingRetired, MapperBindingRejected:
		return true
	default:
		return false
	}
}

func mapperStateSelectable(state MapperBindingState) bool {
	return state == MapperBindingActive || state == MapperBindingRetired
}

func validateMapperText(name, value string) error {
	if err := validateCatalogText(name, value); err != nil || strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: %s is empty or invalid", ErrInvalidMapperPublication, name)
	}
	return nil
}

func validateMapperRejectionDetail(value string) error {
	if value == "" || len(value) > MaxMapperRejectionDetailBytes || !utf8.ValidString(value) ||
		strings.IndexByte(value, 0) >= 0 || strings.TrimSpace(value) == "" {
		return fmt.Errorf("%w: rejection is empty, oversized, or invalid UTF-8", ErrInvalidMapperPublication)
	}
	return nil
}

func compareMapperSchemaFingerprints(left, right MapperSchemaFingerprint) int {
	if value := strings.Compare(left.Name, right.Name); value != 0 {
		return value
	}
	if left.Version != right.Version {
		if left.Version < right.Version {
			return -1
		}
		return 1
	}
	if left.LogicalEncodingVersion != right.LogicalEncodingVersion {
		if left.LogicalEncodingVersion < right.LogicalEncodingVersion {
			return -1
		}
		return 1
	}
	return bytes.Compare(left.SHA256[:], right.SHA256[:])
}

func compareMapperBindings(left, right MapperBinding) int {
	if value := strings.Compare(left.SourceID, right.SourceID); value != 0 {
		return value
	}
	if value := strings.Compare(left.ChannelID, right.ChannelID); value != 0 {
		return value
	}
	leftNS, rightNS := mustMapperBoundaryNS(left.EffectiveFrom), mustMapperBoundaryNS(right.EffectiveFrom)
	if leftNS < rightNS {
		return -1
	}
	if leftNS > rightNS {
		return 1
	}
	return strings.Compare(left.MapperReleaseID, right.MapperReleaseID)
}

func cloneMapperBinding(binding MapperBinding) MapperBinding {
	binding.SchemaFingerprints = slices.Clone(binding.SchemaFingerprints)
	if binding.EffectiveTo != nil {
		to := *binding.EffectiveTo
		binding.EffectiveTo = &to
	}
	binding.DualRunEvidence.Old.AcceptedFields = slices.Clone(binding.DualRunEvidence.Old.AcceptedFields)
	binding.DualRunEvidence.Old.RejectionCounts = maps.Clone(binding.DualRunEvidence.Old.RejectionCounts)
	binding.DualRunEvidence.Old.Rejections = slices.Clone(binding.DualRunEvidence.Old.Rejections)
	binding.DualRunEvidence.New.AcceptedFields = slices.Clone(binding.DualRunEvidence.New.AcceptedFields)
	binding.DualRunEvidence.New.RejectionCounts = maps.Clone(binding.DualRunEvidence.New.RejectionCounts)
	binding.DualRunEvidence.New.Rejections = slices.Clone(binding.DualRunEvidence.New.Rejections)
	binding.DualRunEvidence.MismatchCodes = slices.Clone(binding.DualRunEvidence.MismatchCodes)
	return binding
}

type mapperFingerprintDocument struct {
	Name                   string `json:"name"`
	Version                uint16 `json:"version"`
	LogicalEncodingVersion uint16 `json:"logical_encoding_version"`
	SHA256                 string `json:"sha256"`
}

type mapperRunEvidenceDocument struct {
	AcceptedFields       []string          `json:"accepted_fields"`
	RejectionCounts      map[string]uint64 `json:"rejection_counts"`
	LogicalSHA256        string            `json:"logical_sha256"`
	SemanticSHA256       string            `json:"semantic_sha256"`
	Rejections           []string          `json:"rejections"`
	RejectionSHA256      string            `json:"rejection_sha256"`
	DownstreamBookResult string            `json:"downstream_book_result"`
	DownstreamBookSHA256 string            `json:"downstream_book_sha256"`
}

type dualRunEvidenceDocument struct {
	Version            uint16                    `json:"version"`
	SourceID           string                    `json:"source_id"`
	ChannelID          string                    `json:"channel_id"`
	SelectionTimeBasis string                    `json:"selection_time_basis"`
	ReceivedStartNS    int64                     `json:"received_start_ns"`
	ReceivedEndNS      int64                     `json:"received_end_ns"`
	CorpusCount        uint64                    `json:"corpus_count"`
	CorpusSHA256       string                    `json:"corpus_sha256"`
	OldMapperReleaseID string                    `json:"old_mapper_release_id"`
	NewMapperReleaseID string                    `json:"new_mapper_release_id"`
	Old                mapperRunEvidenceDocument `json:"old"`
	New                mapperRunEvidenceDocument `json:"new"`
	MismatchCodes      []string                  `json:"mismatch_codes"`
	Mismatch           bool                      `json:"mismatch"`
	Decision           MapperEvidenceDecision    `json:"decision"`
}

func encodeMapperSchemaFingerprints(fingerprints []MapperSchemaFingerprint) ([]byte, error) {
	document := make([]mapperFingerprintDocument, len(fingerprints))
	for i, fingerprint := range fingerprints {
		document[i] = mapperFingerprintDocument{
			Name: fingerprint.Name, Version: fingerprint.Version,
			LogicalEncodingVersion: fingerprint.LogicalEncodingVersion,
			SHA256:                 hex.EncodeToString(fingerprint.SHA256[:]),
		}
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("catalog: encode mapper schema fingerprints: %w", err)
	}
	return CanonicalJSON(encoded)
}

func decodeMapperSchemaFingerprints(encoded []byte) ([]MapperSchemaFingerprint, error) {
	var document []mapperFingerprintDocument
	if err := json.Unmarshal(encoded, &document); err != nil {
		return nil, fmt.Errorf("%w: decode mapper schema fingerprints: %v", ErrMapperPublicationConflict, err)
	}
	fingerprints := make([]MapperSchemaFingerprint, len(document))
	for i, item := range document {
		digest, err := decodeMapperHash(item.SHA256)
		if err != nil {
			return nil, err
		}
		fingerprints[i] = MapperSchemaFingerprint{Name: item.Name, Version: item.Version, LogicalEncodingVersion: item.LogicalEncodingVersion, SHA256: digest}
	}
	return fingerprints, nil
}

func encodeDualRunEvidence(evidence DualRunEvidence) ([]byte, error) {
	if evidence.IsZero() {
		return []byte("{}"), nil
	}
	document := dualRunEvidenceDocument{
		Version: evidence.Version, SourceID: evidence.SourceID, ChannelID: evidence.ChannelID,
		SelectionTimeBasis: evidence.SelectionTimeBasis,
		ReceivedStartNS:    evidence.ReceivedStartNS, ReceivedEndNS: evidence.ReceivedEndNS,
		CorpusCount: evidence.CorpusCount, CorpusSHA256: hex.EncodeToString(evidence.CorpusSHA256[:]),
		OldMapperReleaseID: evidence.OldMapperReleaseID, NewMapperReleaseID: evidence.NewMapperReleaseID,
		Old: mapperRunEvidenceToDocument(evidence.Old), New: mapperRunEvidenceToDocument(evidence.New),
		MismatchCodes: evidence.MismatchCodes, Mismatch: evidence.Mismatch, Decision: evidence.Decision,
	}
	encoded, err := json.Marshal(document)
	if err != nil {
		return nil, fmt.Errorf("catalog: encode mapper dual-run evidence: %w", err)
	}
	if len(encoded) > MaxMapperEvidenceJSONBytes {
		return nil, fmt.Errorf("%w: mapper dual-run evidence exceeds %d bytes", ErrInvalidMapperPublication, MaxMapperEvidenceJSONBytes)
	}
	return encoded, nil
}

func decodeDualRunEvidence(encoded []byte) (DualRunEvidence, error) {
	if bytes.Equal(bytes.TrimSpace(encoded), []byte("{}")) {
		return DualRunEvidence{}, nil
	}
	var document dualRunEvidenceDocument
	if err := json.Unmarshal(encoded, &document); err != nil {
		return DualRunEvidence{}, fmt.Errorf("%w: decode dual-run evidence: %v", ErrMapperPublicationConflict, err)
	}
	corpusHash, err := decodeMapperHash(document.CorpusSHA256)
	if err != nil {
		return DualRunEvidence{}, err
	}
	oldRun, err := mapperRunEvidenceFromDocument(document.Old)
	if err != nil {
		return DualRunEvidence{}, err
	}
	newRun, err := mapperRunEvidenceFromDocument(document.New)
	if err != nil {
		return DualRunEvidence{}, err
	}
	return DualRunEvidence{
		Version: document.Version, SourceID: document.SourceID, ChannelID: document.ChannelID,
		SelectionTimeBasis: document.SelectionTimeBasis,
		ReceivedStartNS:    document.ReceivedStartNS, ReceivedEndNS: document.ReceivedEndNS,
		CorpusCount: document.CorpusCount, CorpusSHA256: corpusHash,
		OldMapperReleaseID: document.OldMapperReleaseID, NewMapperReleaseID: document.NewMapperReleaseID,
		Old: oldRun, New: newRun, MismatchCodes: document.MismatchCodes,
		Mismatch: document.Mismatch, Decision: document.Decision,
	}, nil
}

func mapperRunEvidenceToDocument(run MapperRunEvidence) mapperRunEvidenceDocument {
	return mapperRunEvidenceDocument{
		AcceptedFields: run.AcceptedFields, RejectionCounts: run.RejectionCounts,
		LogicalSHA256:        hex.EncodeToString(run.LogicalSHA256[:]),
		SemanticSHA256:       hex.EncodeToString(run.SemanticSHA256[:]),
		Rejections:           run.Rejections,
		RejectionSHA256:      hex.EncodeToString(run.RejectionSHA256[:]),
		DownstreamBookResult: run.DownstreamBookResult,
		DownstreamBookSHA256: hex.EncodeToString(run.DownstreamBookSHA256[:]),
	}
}

func mapperRunEvidenceFromDocument(document mapperRunEvidenceDocument) (MapperRunEvidence, error) {
	logicalHash, err := decodeMapperHash(document.LogicalSHA256)
	if err != nil {
		return MapperRunEvidence{}, err
	}
	semanticHash, err := decodeMapperHash(document.SemanticSHA256)
	if err != nil {
		return MapperRunEvidence{}, err
	}
	rejectionHash, err := decodeMapperHash(document.RejectionSHA256)
	if err != nil {
		return MapperRunEvidence{}, err
	}
	bookHash, err := decodeMapperHash(document.DownstreamBookSHA256)
	if err != nil {
		return MapperRunEvidence{}, err
	}
	return MapperRunEvidence{
		AcceptedFields: document.AcceptedFields, RejectionCounts: document.RejectionCounts,
		LogicalSHA256: logicalHash, SemanticSHA256: semanticHash,
		Rejections: document.Rejections, RejectionSHA256: rejectionHash,
		DownstreamBookResult: document.DownstreamBookResult, DownstreamBookSHA256: bookHash,
	}, nil
}

func decodeMapperHash(value string) ([sha256.Size]byte, error) {
	var result [sha256.Size]byte
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != sha256.Size || value != strings.ToLower(value) {
		return result, fmt.Errorf("%w: malformed mapper SHA-256", ErrMapperPublicationConflict)
	}
	copy(result[:], decoded)
	return result, nil
}

func validateStoredBindingReleaseReferences(ctx context.Context, tx pgx.Tx, binding MapperBinding) error {
	releases := make(map[string]MapperRelease, 2)
	for _, releaseID := range []string{binding.MapperReleaseID, binding.DualRunEvidence.OldMapperReleaseID} {
		if releaseID == "" {
			continue
		}
		release, found, err := findMapperRelease(ctx, tx, releaseID)
		if err != nil {
			return err
		}
		if !found {
			return fmt.Errorf("%w: mapper release %s is absent", ErrMapperPublicationConflict, releaseID)
		}
		releases[releaseID] = release
	}
	return validateBindingReleaseReferences(binding, releases)
}

func findMapperRelease(ctx context.Context, row interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, releaseID string) (MapperRelease, bool, error) {
	var release MapperRelease
	var fixtureHash []byte
	err := row.QueryRow(ctx, `
SELECT mapper_release_id::text, mapper_version, build_identity,
       normalized_schema_version, fixture_bundle_hash, source_code_identity
FROM mapper_release WHERE mapper_release_id = $1
`, releaseID).Scan(&release.ReleaseID, &release.MapperVersion, &release.BuildIdentity,
		&release.NormalizedSchemaVersion, &fixtureHash, &release.SourceCodeIdentity)
	if errors.Is(err, pgx.ErrNoRows) {
		return MapperRelease{}, false, nil
	}
	if err != nil {
		return MapperRelease{}, false, fmt.Errorf("catalog: find mapper release: %w", err)
	}
	if len(fixtureHash) != sha256.Size {
		return MapperRelease{}, false, fmt.Errorf("%w: stored mapper fixture hash is malformed", ErrMapperPublicationConflict)
	}
	copy(release.FixtureBundleSHA256[:], fixtureHash)
	if err := release.Validate(); err != nil {
		return MapperRelease{}, false, fmt.Errorf("%w: stored mapper release is invalid: %v", ErrMapperPublicationConflict, err)
	}
	return release, true, nil
}

func insertMapperBinding(ctx context.Context, tx pgx.Tx, binding MapperBinding) error {
	fingerprints, err := encodeMapperSchemaFingerprints(binding.SchemaFingerprints)
	if err != nil {
		return err
	}
	evidence, err := encodeDualRunEvidence(binding.DualRunEvidence)
	if err != nil {
		return err
	}
	fromNS := mustMapperBoundaryNS(binding.EffectiveFrom)
	var effectiveTo any
	var effectiveToNS any
	if binding.EffectiveTo != nil {
		effectiveTo = *binding.EffectiveTo
		effectiveToNS = mustMapperBoundaryNS(*binding.EffectiveTo)
	}
	_, err = tx.Exec(ctx, `
INSERT INTO mapper_binding (
    source_id, channel_id, effective_from, effective_to,
    effective_from_ns, effective_to_ns, mapper_release_id,
    schema_fingerprints, dual_run_evidence, state
) VALUES ($1, $2, $3, $4, $5, $6, $7, $8::jsonb, $9::jsonb, $10)
ON CONFLICT (source_id, channel_id, effective_from_ns) DO NOTHING
`, binding.SourceID, binding.ChannelID, binding.EffectiveFrom, effectiveTo,
		fromNS, effectiveToNS, binding.MapperReleaseID, fingerprints, evidence, binding.State)
	if err != nil {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) && postgresError.Code == "23P01" {
			return fmt.Errorf("%w: %s/%s overlaps an existing mapper binding", ErrMapperIntervalConflict, binding.SourceID, binding.ChannelID)
		}
		return fmt.Errorf("catalog: insert mapper binding: %w", err)
	}
	return nil
}

const mapperBindingColumns = `
source_id::text, channel_id, effective_from_ns, effective_to_ns,
mapper_release_id::text, schema_fingerprints, dual_run_evidence, state::text`

func findMapperBinding(ctx context.Context, tx pgx.Tx, sourceID, channelID string, effectiveFromNS int64, forUpdate bool) (MapperBinding, bool, error) {
	query := "SELECT " + mapperBindingColumns + `
FROM mapper_binding
WHERE source_id = $1 AND channel_id = $2 AND effective_from_ns = $3`
	if forUpdate {
		query += " FOR UPDATE"
	}
	binding, found, err := scanMapperBinding(tx.QueryRow(ctx, query, sourceID, channelID, effectiveFromNS))
	if err != nil {
		return MapperBinding{}, false, err
	}
	return binding, found, nil
}

func findOpenActiveMapperBinding(ctx context.Context, tx pgx.Tx, sourceID, channelID string, beforeNS int64) (MapperBinding, bool, error) {
	return scanMapperBinding(tx.QueryRow(ctx, "SELECT "+mapperBindingColumns+`
FROM mapper_binding
WHERE source_id = $1 AND channel_id = $2 AND state = 'active'
  AND effective_from_ns < $3 AND effective_to_ns IS NULL
ORDER BY effective_from_ns DESC
LIMIT 1
FOR UPDATE
`, sourceID, channelID, beforeNS))
}

type mapperScanner interface {
	Scan(...any) error
}

func scanMapperBinding(scanner mapperScanner) (MapperBinding, bool, error) {
	var binding MapperBinding
	var effectiveFromNS int64
	var effectiveToNS *int64
	var fingerprints, evidence []byte
	var state string
	err := scanner.Scan(&binding.SourceID, &binding.ChannelID, &effectiveFromNS, &effectiveToNS,
		&binding.MapperReleaseID, &fingerprints, &evidence, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return MapperBinding{}, false, nil
	}
	if err != nil {
		return MapperBinding{}, false, fmt.Errorf("catalog: scan mapper binding: %w", err)
	}
	binding.EffectiveFrom = time.Unix(0, effectiveFromNS).UTC()
	if effectiveToNS != nil {
		to := time.Unix(0, *effectiveToNS).UTC()
		binding.EffectiveTo = &to
	}
	binding.State = MapperBindingState(state)
	binding.SchemaFingerprints, err = decodeMapperSchemaFingerprints(fingerprints)
	if err != nil {
		return MapperBinding{}, false, err
	}
	binding.DualRunEvidence, err = decodeDualRunEvidence(evidence)
	if err != nil {
		return MapperBinding{}, false, err
	}
	binding, err = canonicalMapperBinding(binding)
	if err != nil {
		return MapperBinding{}, false, fmt.Errorf("%w: stored mapper binding is invalid: %v", ErrMapperPublicationConflict, err)
	}
	return binding, true, nil
}

func mapperBindingsEqual(left, right MapperBinding) bool {
	leftCanonical, leftErr := canonicalMapperBinding(left)
	rightCanonical, rightErr := canonicalMapperBinding(right)
	if leftErr != nil || rightErr != nil {
		return false
	}
	leftFingerprints, leftErr := encodeMapperSchemaFingerprints(leftCanonical.SchemaFingerprints)
	rightFingerprints, rightErr := encodeMapperSchemaFingerprints(rightCanonical.SchemaFingerprints)
	leftEvidence, leftEvidenceErr := encodeDualRunEvidence(leftCanonical.DualRunEvidence)
	rightEvidence, rightEvidenceErr := encodeDualRunEvidence(rightCanonical.DualRunEvidence)
	return leftErr == nil && rightErr == nil && leftEvidenceErr == nil && rightEvidenceErr == nil &&
		leftCanonical.SourceID == rightCanonical.SourceID && leftCanonical.ChannelID == rightCanonical.ChannelID &&
		mustMapperBoundaryNS(leftCanonical.EffectiveFrom) == mustMapperBoundaryNS(rightCanonical.EffectiveFrom) &&
		mapperOptionalBoundaryEqual(leftCanonical.EffectiveTo, rightCanonical.EffectiveTo) &&
		leftCanonical.MapperReleaseID == rightCanonical.MapperReleaseID && leftCanonical.State == rightCanonical.State &&
		bytes.Equal(leftFingerprints, rightFingerprints) && bytes.Equal(leftEvidence, rightEvidence)
}

func mapperOptionalBoundaryEqual(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return mustMapperBoundaryNS(*left) == mustMapperBoundaryNS(*right)
}

func rollbackMapperTx(ctx context.Context, tx pgx.Tx, result *error, operation string) {
	if rollbackErr := tx.Rollback(ctx); rollbackErr != nil && !errors.Is(rollbackErr, pgx.ErrTxClosed) {
		*result = errors.Join(*result, fmt.Errorf("catalog: rollback %s: %w", operation, rollbackErr))
	}
}
