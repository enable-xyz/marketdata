package quality

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"time"
)

var (
	ErrInvalidSpill        = errors.New("quality: invalid opportunity spill")
	ErrNoSpillRows         = errors.New("quality: no terminal opportunities eligible for spill")
	ErrUnknownSpillOutcome = errors.New("quality: opportunity archive outcome is unknown; reconcile the same generation")
)

type SpillState string

const (
	SpillPending     SpillState = "pending"
	SpillCommitted   SpillState = "committed"
	SpillQuarantined SpillState = "quarantined"
)

type OpportunityBoundary struct {
	TerminalTimeNS int64
	OpportunityID  string
}

func compareBoundary(left, right OpportunityBoundary) int {
	if value := cmp.Compare(left.TerminalTimeNS, right.TerminalTimeNS); value != 0 {
		return value
	}
	return cmp.Compare(left.OpportunityID, right.OpportunityID)
}

type SpillRowIdentity struct {
	OpportunityID  string
	TerminalTimeNS int64
	LogicalHash    [sha256.Size]byte
}

type SpillGeneration struct {
	GenerationID        string
	Partition           string
	CatalogSnapshotHash [sha256.Size]byte
	MapperSetHash       [sha256.Size]byte
	From                OpportunityBoundary
	Through             OpportunityBoundary
	Rows                []Opportunity
	RowIdentities       []SpillRowIdentity
	Fingerprint         [sha256.Size]byte
	State               SpillState
	Archive             *ArchiveCommit
	QuarantineReason    string
}

func NewSpillGeneration(request SpillRequest, rows []Opportunity) (SpillGeneration, error) {
	if err := request.Validate(); err != nil {
		return SpillGeneration{}, err
	}
	generation := SpillGeneration{
		GenerationID: request.GenerationID, Partition: request.Partition,
		CatalogSnapshotHash: request.CatalogSnapshotHash, MapperSetHash: request.MapperSetHash,
		Rows: slices.Clone(rows), State: SpillPending,
	}
	slices.SortFunc(generation.Rows, func(left, right Opportunity) int {
		return compareBoundary(OpportunityBoundary{TerminalTimeNS: left.TerminalTimeNS, OpportunityID: left.OpportunityID},
			OpportunityBoundary{TerminalTimeNS: right.TerminalTimeNS, OpportunityID: right.OpportunityID})
	})
	generation.RowIdentities = make([]SpillRowIdentity, len(generation.Rows))
	for index, row := range generation.Rows {
		logicalHash, err := row.LogicalHash()
		if err != nil {
			return SpillGeneration{}, err
		}
		generation.RowIdentities[index] = SpillRowIdentity{
			OpportunityID: row.OpportunityID, TerminalTimeNS: row.TerminalTimeNS, LogicalHash: logicalHash,
		}
	}
	if len(generation.RowIdentities) > 0 {
		generation.From = OpportunityBoundary{TerminalTimeNS: generation.RowIdentities[0].TerminalTimeNS, OpportunityID: generation.RowIdentities[0].OpportunityID}
		last := generation.RowIdentities[len(generation.RowIdentities)-1]
		generation.Through = OpportunityBoundary{TerminalTimeNS: last.TerminalTimeNS, OpportunityID: last.OpportunityID}
	}
	generation.Fingerprint = spillFingerprint(
		generation.GenerationID, generation.Partition, generation.CatalogSnapshotHash, generation.MapperSetHash, generation.RowIdentities,
	)
	if err := generation.Validate(); err != nil {
		return SpillGeneration{}, err
	}
	return generation, nil
}

func (g SpillGeneration) Validate() error {
	if err := validateQualityString("generation_id", g.GenerationID, true); err != nil {
		return errors.Join(ErrInvalidSpill, err)
	}
	if err := validateQualityString("ledger_partition", g.Partition, true); err != nil {
		return errors.Join(ErrInvalidSpill, err)
	}
	if len(g.RowIdentities) == 0 || g.From.TerminalTimeNS < 0 || g.Through.TerminalTimeNS < 0 || compareBoundary(g.From, g.Through) > 0 ||
		g.CatalogSnapshotHash == ([sha256.Size]byte{}) || g.MapperSetHash == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: invalid boundary or generation lineage", ErrInvalidSpill)
	}
	for index, identity := range g.RowIdentities {
		if err := validateQualityString("opportunity_id", identity.OpportunityID, true); err != nil ||
			identity.TerminalTimeNS < 0 || identity.LogicalHash == ([sha256.Size]byte{}) {
			return fmt.Errorf("%w: invalid row identity", ErrInvalidSpill)
		}
		boundary := OpportunityBoundary{TerminalTimeNS: identity.TerminalTimeNS, OpportunityID: identity.OpportunityID}
		if index > 0 {
			previous := g.RowIdentities[index-1]
			if compareBoundary(OpportunityBoundary{TerminalTimeNS: previous.TerminalTimeNS, OpportunityID: previous.OpportunityID}, boundary) >= 0 {
				return fmt.Errorf("%w: row identities are not strictly ordered", ErrInvalidSpill)
			}
		}
	}
	if first := g.RowIdentities[0]; g.From != (OpportunityBoundary{TerminalTimeNS: first.TerminalTimeNS, OpportunityID: first.OpportunityID}) {
		return fmt.Errorf("%w: first boundary does not match rows", ErrInvalidSpill)
	}
	last := g.RowIdentities[len(g.RowIdentities)-1]
	if g.Through != (OpportunityBoundary{TerminalTimeNS: last.TerminalTimeNS, OpportunityID: last.OpportunityID}) {
		return fmt.Errorf("%w: through boundary does not match rows", ErrInvalidSpill)
	}
	if spillFingerprint(g.GenerationID, g.Partition, g.CatalogSnapshotHash, g.MapperSetHash, g.RowIdentities) != g.Fingerprint {
		return fmt.Errorf("%w: generation fingerprint mismatch", ErrInvalidSpill)
	}
	if len(g.Rows) != 0 {
		if len(g.Rows) != len(g.RowIdentities) {
			return fmt.Errorf("%w: row membership mismatch", ErrInvalidSpill)
		}
		date := ""
		for index, row := range g.Rows {
			logicalHash, logicalErr := row.LogicalHash()
			if logicalErr != nil || !row.Terminal || row.LedgerPartition != g.Partition ||
				row.OpportunityID != g.RowIdentities[index].OpportunityID || row.TerminalTimeNS != g.RowIdentities[index].TerminalTimeNS ||
				logicalHash != g.RowIdentities[index].LogicalHash {
				return fmt.Errorf("%w: invalid terminal row membership", ErrInvalidSpill)
			}
			rowDate := time.Unix(0, row.TerminalTimeNS).UTC().Format("2006-01-02")
			if date == "" {
				date = rowDate
			} else if rowDate != date {
				return fmt.Errorf("%w: one spill generation must stay in one UTC date partition", ErrInvalidSpill)
			}
		}
	}
	switch g.State {
	case SpillPending:
		if g.Archive != nil || g.QuarantineReason != "" {
			return fmt.Errorf("%w: pending generation has terminal spill fields", ErrInvalidSpill)
		}
	case SpillQuarantined:
		if g.Archive != nil || validateQualityString("quarantine_reason", g.QuarantineReason, true) != nil {
			return fmt.Errorf("%w: quarantined generation lacks bounded failure evidence", ErrInvalidSpill)
		}
	case SpillCommitted:
		if g.Archive == nil || g.QuarantineReason != "" {
			return fmt.Errorf("%w: committed generation lacks archive identity", ErrInvalidSpill)
		}
		if err := g.Archive.Validate(g); err != nil {
			return err
		}
	default:
		return fmt.Errorf("%w: state %q", ErrInvalidSpill, g.State)
	}
	return nil
}

func spillFingerprint(
	generationID, partition string,
	catalogSnapshotHash, mapperSetHash [sha256.Size]byte,
	rows []SpillRowIdentity,
) [sha256.Size]byte {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("quality-opportunity-spill-generation-v2\x00"))
	writeHashString(hasher, generationID)
	writeHashString(hasher, partition)
	_, _ = hasher.Write(catalogSnapshotHash[:])
	_, _ = hasher.Write(mapperSetHash[:])
	for _, row := range rows {
		writeHashInt64(hasher, row.TerminalTimeNS)
		writeHashString(hasher, row.OpportunityID)
		_, _ = hasher.Write(row.LogicalHash[:])
	}
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result
}

func OpportunityArchiveLogicalHash(rows []Opportunity) ([sha256.Size]byte, error) {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("dataset-opportunity-archive-logical-v1\x00"))
	for _, row := range rows {
		logicalHash, err := row.LogicalHash()
		if err != nil {
			return [sha256.Size]byte{}, err
		}
		_, _ = hasher.Write(logicalHash[:])
	}
	var result [sha256.Size]byte
	copy(result[:], hasher.Sum(nil))
	return result, nil
}

type ArchiveCommit struct {
	GenerationID          string
	GenerationFingerprint [sha256.Size]byte
	DatasetPartitionID    string
	ManifestHash          [sha256.Size]byte
	ManifestPath          string
	ParquetPath           string
	DatasetVersion        string
	PartitionKey          string
	RangeStartNS          int64
	RangeEndNS            int64
	InputSetHash          [sha256.Size]byte
	CatalogSnapshotHash   [sha256.Size]byte
	MapperSetHash         [sha256.Size]byte
	LogicalHash           [sha256.Size]byte
	PhysicalHash          [sha256.Size]byte
}

func ValidateSpillQuarantineReason(reason string) error {
	if err := validateQualityString("quarantine_reason", reason, true); err != nil {
		return errors.Join(ErrInvalidSpill, err)
	}
	return nil
}

func (a ArchiveCommit) Validate(generation SpillGeneration) error {
	for _, field := range []struct{ name, value string }{
		{"generation_id", a.GenerationID}, {"dataset_partition_id", a.DatasetPartitionID}, {"manifest_path", a.ManifestPath},
		{"parquet_path", a.ParquetPath}, {"dataset_version", a.DatasetVersion}, {"partition_key", a.PartitionKey},
	} {
		if err := validateQualityString(field.name, field.value, true); err != nil {
			return errors.Join(ErrInvalidSpill, err)
		}
	}
	if a.GenerationID != generation.GenerationID || a.GenerationFingerprint != generation.Fingerprint ||
		a.CatalogSnapshotHash != generation.CatalogSnapshotHash || a.MapperSetHash != generation.MapperSetHash ||
		a.RangeStartNS != generation.From.TerminalTimeNS || a.RangeEndNS != generation.Through.TerminalTimeNS ||
		a.RangeStartNS < 0 || a.RangeEndNS < a.RangeStartNS {
		return fmt.Errorf("%w: archive boundary, lineage, or generation mismatch", ErrInvalidSpill)
	}
	for _, digest := range [][sha256.Size]byte{a.ManifestHash, a.InputSetHash, a.CatalogSnapshotHash, a.MapperSetHash, a.LogicalHash, a.PhysicalHash} {
		if digest == ([sha256.Size]byte{}) {
			return fmt.Errorf("%w: archive digest is required", ErrInvalidSpill)
		}
	}
	return nil
}

type SpillRequest struct {
	GenerationID        string
	Partition           string
	ThroughTimeNS       int64
	MaximumRows         int
	CatalogSnapshotHash [sha256.Size]byte
	MapperSetHash       [sha256.Size]byte
}

func (r SpillRequest) Validate() error {
	if err := validateQualityString("generation_id", r.GenerationID, true); err != nil {
		return errors.Join(ErrInvalidSpill, err)
	}
	if err := validateQualityString("ledger_partition", r.Partition, true); err != nil {
		return errors.Join(ErrInvalidSpill, err)
	}
	if r.ThroughTimeNS < 0 || r.MaximumRows < 1 || r.MaximumRows > 1_000_000 ||
		r.CatalogSnapshotHash == ([sha256.Size]byte{}) || r.MapperSetHash == ([sha256.Size]byte{}) {
		return fmt.Errorf("%w: invalid spill cutoff, row bound, or lineage", ErrInvalidSpill)
	}
	return nil
}

type OpportunitySpillStore interface {
	BeginOpportunitySpill(context.Context, SpillRequest) (SpillGeneration, error)
	OpportunitySpill(context.Context, string) (SpillGeneration, error)
	CommitOpportunitySpill(context.Context, SpillGeneration, ArchiveCommit) error
	DeleteCommittedOpportunityRows(context.Context, string) error
	QuarantineOpportunitySpill(context.Context, string, string) error
}

type OpportunityArchiveWriter interface {
	LookupOpportunityArchive(context.Context, SpillGeneration) (ArchiveCommit, bool, error)
	WriteOpportunityArchive(context.Context, SpillGeneration) (ArchiveCommit, error)
}

// ExecuteOpportunitySpill is deliberately generation-keyed. It always looks
// up a published manifest before writing and reconciles an unknown database
// acknowledgement before deciding whether to retry. Deletion is later and
// idempotent; the committed boundary already suppresses PostgreSQL overlap.
func ExecuteOpportunitySpill(ctx context.Context, store OpportunitySpillStore, writer OpportunityArchiveWriter, request SpillRequest) (SpillGeneration, error) {
	if store == nil || writer == nil {
		return SpillGeneration{}, fmt.Errorf("%w: store and archive writer are required", ErrInvalidSpill)
	}
	if err := request.Validate(); err != nil {
		return SpillGeneration{}, err
	}
	generation, err := store.BeginOpportunitySpill(ctx, request)
	if err != nil {
		return SpillGeneration{}, err
	}
	if err := generation.Validate(); err != nil {
		return SpillGeneration{}, err
	}
	if generation.State == SpillCommitted {
		if err := store.DeleteCommittedOpportunityRows(ctx, generation.GenerationID); err != nil {
			return generation, err
		}
		return generation, nil
	}
	if generation.State != SpillPending {
		return generation, fmt.Errorf("%w: generation %s is %s", ErrInvalidSpill, generation.GenerationID, generation.State)
	}

	archive, found, err := writer.LookupOpportunityArchive(ctx, generation)
	if err != nil {
		return generation, err
	}
	if !found {
		archive, err = writer.WriteOpportunityArchive(ctx, generation)
		if err != nil {
			reconciled, exists, lookupErr := writer.LookupOpportunityArchive(ctx, generation)
			if lookupErr != nil {
				return generation, errors.Join(err, lookupErr, ErrUnknownSpillOutcome)
			}
			if !exists {
				return generation, errors.Join(err, ErrUnknownSpillOutcome)
			}
			archive = reconciled
		}
	}
	if err := archive.Validate(generation); err != nil {
		return generation, err
	}
	if err := store.CommitOpportunitySpill(ctx, generation, archive); err != nil {
		reconciled, statusErr := store.OpportunitySpill(ctx, generation.GenerationID)
		if statusErr != nil {
			return generation, errors.Join(err, statusErr, ErrUnknownSpillOutcome)
		}
		if reconciled.State != SpillCommitted || reconciled.Archive == nil || *reconciled.Archive != archive {
			return generation, errors.Join(err, ErrUnknownSpillOutcome)
		}
		generation = reconciled
	} else {
		generation.State = SpillCommitted
		generation.Archive = &archive
	}
	if err := store.DeleteCommittedOpportunityRows(ctx, generation.GenerationID); err != nil {
		return generation, err
	}
	return generation, nil
}
