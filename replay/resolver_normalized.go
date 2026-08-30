package replay

import (
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/warehouse"
)

// NormalizedDatasetLookup resolves the immutable catalog publication selected by
// a pinned replay request. catalog.QueryStore satisfies this interface.
type NormalizedDatasetLookup interface {
	DatasetManifest(context.Context, string) (catalog.DatasetManifest, error)
}

// NormalizedReader is the bounded declarative read surface used by normalized
// replay. *warehouse.QueryAdapter satisfies this interface.
type NormalizedReader interface {
	Page(context.Context, warehouse.QuerySpec) (warehouse.Page, error)
}

// NormalizedPlan is a closed, committed read plan. PageSize bounds each reader
// allocation and MaxItems bounds the complete record-and-gap stream.
type NormalizedPlan struct {
	Reader   NormalizedReader
	Dataset  warehouse.Dataset
	PageSize int
	MaxItems int
}

type NormalizedPlanResolver interface {
	ResolveNormalized(context.Context, ServiceRequest) (NormalizedPlan, error)
}

type NormalizedResolverOptions struct {
	PageSize int
	MaxItems int
}

// NormalizedResolver binds replay requests to committed catalog datasets and an
// injected declarative reader. It never selects a mutable/latest dataset.
type NormalizedResolver struct {
	datasets NormalizedDatasetLookup
	reader   NormalizedReader
	options  NormalizedResolverOptions
}

func NewNormalizedResolver(datasets NormalizedDatasetLookup, reader NormalizedReader, options NormalizedResolverOptions) (*NormalizedResolver, error) {
	if datasets == nil || reader == nil {
		return nil, fmt.Errorf("%w: normalized dataset lookup and reader are required", ErrInvalidServiceRequest)
	}
	if options.PageSize < 1 || options.PageSize > warehouse.MaximumQueryRows || options.MaxItems < 1 {
		return nil, fmt.Errorf("%w: normalized page and item bounds are required", ErrInputBound)
	}
	return &NormalizedResolver{datasets: datasets, reader: reader, options: options}, nil
}

func (r *NormalizedResolver) ResolveNormalized(ctx context.Context, request ServiceRequest) (NormalizedPlan, error) {
	if r == nil || r.datasets == nil || r.reader == nil || ctx == nil {
		return NormalizedPlan{}, fmt.Errorf("%w: normalized resolver is not initialized", ErrInvalidServiceRequest)
	}
	if err := request.Validate(); err != nil {
		return NormalizedPlan{}, err
	}
	if err := context.Cause(ctx); err != nil {
		return NormalizedPlan{}, err
	}
	manifest, err := r.datasets.DatasetManifest(ctx, request.DatasetID)
	if err != nil {
		return NormalizedPlan{}, fmt.Errorf("replay: resolve committed dataset %q: %w", request.DatasetID, err)
	}
	if manifest.State != catalog.DatasetCommitted {
		return NormalizedPlan{}, fmt.Errorf("%w: dataset %q is not committed", ErrInvalidServiceRequest, request.DatasetID)
	}
	dataset := warehouse.Dataset{
		ID:                warehouse.GenerationID(manifest.Dataset.ID),
		Family:            manifest.Dataset.Family,
		CatalogSnapshotID: warehouse.Hash(manifest.Dataset.CatalogSnapshotID),
		SchemaName:        manifest.Dataset.SchemaName,
		SchemaVersion:     manifest.Dataset.SchemaVersion,
	}
	if err := dataset.Validate(); err != nil {
		return NormalizedPlan{}, fmt.Errorf("%w: invalid committed dataset: %v", ErrInvalidServiceRequest, err)
	}
	if manifest.ManifestPath == "" || manifest.ManifestSHA256 == ([32]byte{}) {
		return NormalizedPlan{}, fmt.Errorf("%w: committed dataset manifest identity is incomplete", ErrInvalidServiceRequest)
	}
	if dataset.IDString() != request.DatasetID || dataset.Family != request.Family ||
		dataset.CatalogSnapshotIDString() != request.CatalogSnapshotID || dataset.SchemaName != request.SchemaName ||
		dataset.SchemaVersion != request.SchemaVersion {
		return NormalizedPlan{}, fmt.Errorf("%w: committed dataset differs from pinned request", ErrInvalidServiceRequest)
	}
	return NormalizedPlan{Reader: r.reader, Dataset: dataset, PageSize: r.options.PageSize, MaxItems: r.options.MaxItems}, nil
}

// NormalizedReplayService adapts committed normalized plans to the replay
// package's pull-stream contract.
type NormalizedReplayService struct {
	resolver NormalizedPlanResolver
}

func NewNormalizedReplayService(resolver NormalizedPlanResolver) (*NormalizedReplayService, error) {
	if resolver == nil {
		return nil, fmt.Errorf("%w: normalized resolver is required", ErrInvalidServiceRequest)
	}
	return &NormalizedReplayService{resolver: resolver}, nil
}

func (s *NormalizedReplayService) OpenNormalized(ctx context.Context, request ServiceRequest) (NormalizedStream, error) {
	if s == nil || s.resolver == nil || ctx == nil {
		return nil, fmt.Errorf("%w: normalized service is not initialized", ErrInvalidServiceRequest)
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	plan, err := s.resolver.ResolveNormalized(ctx, request)
	if err != nil {
		return nil, err
	}
	if plan.Reader == nil || plan.PageSize < 1 || plan.PageSize > warehouse.MaximumQueryRows || plan.MaxItems < 1 {
		return nil, fmt.Errorf("%w: normalized plan is incomplete", ErrInvalidServiceRequest)
	}
	if plan.Dataset.IDString() != request.DatasetID || plan.Dataset.Family != request.Family ||
		plan.Dataset.CatalogSnapshotIDString() != request.CatalogSnapshotID || plan.Dataset.SchemaName != request.SchemaName ||
		plan.Dataset.SchemaVersion != request.SchemaVersion {
		return nil, fmt.Errorf("%w: normalized plan differs from pinned request", ErrInvalidServiceRequest)
	}
	streamCtx, cancel := context.WithCancel(ctx)
	return &normalizedPlanStream{ctx: streamCtx, cancel: cancel, request: request, plan: plan}, nil
}

type normalizedPlanStream struct {
	ctx     context.Context
	cancel  context.CancelFunc
	request ServiceRequest
	plan    NormalizedPlan

	mu            sync.Mutex
	closed        atomic.Bool
	loaded        bool
	hasMore       bool
	after         *warehouse.SortKey
	records       []warehouse.QueryRow
	recordIndex   int
	gaps          []warehouse.GapReference
	gapIndex      int
	referenceGaps []warehouse.GapReference
	emitted       int
	terminalErr   error
}

func (s *normalizedPlanStream) Next(ctx context.Context) (NormalizedItem, error) {
	if ctx == nil {
		return NormalizedItem{}, fmt.Errorf("%w: normalized stream context is required", ErrInvalidServiceRequest)
	}
	if s.closed.Load() {
		return NormalizedItem{}, io.EOF
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminalErr != nil {
		return NormalizedItem{}, s.terminalErr
	}
	if s.closed.Load() {
		return NormalizedItem{}, io.EOF
	}
	if err := s.ensureHead(ctx); err != nil {
		s.terminalErr = err
		return NormalizedItem{}, err
	}
	if s.emitted == s.plan.MaxItems {
		if s.hasHead() {
			s.terminalErr = fmt.Errorf("%w: normalized stream exceeds %d items", ErrInputBound, s.plan.MaxItems)
			return NormalizedItem{}, s.terminalErr
		}
		return NormalizedItem{}, io.EOF
	}
	if !s.hasHead() {
		return NormalizedItem{}, io.EOF
	}

	item := s.nextHead()
	if err := item.ValidateFor(s.request); err != nil {
		s.terminalErr = err
		return NormalizedItem{}, err
	}
	s.emitted++
	return item, nil
}

func (s *normalizedPlanStream) Close() error {
	if s == nil {
		return nil
	}
	if s.closed.CompareAndSwap(false, true) {
		s.cancel()
	}
	return nil
}

func (s *normalizedPlanStream) ensureHead(ctx context.Context) error {
	for !s.loaded || (s.recordIndex == len(s.records) && s.hasMore) {
		if err := s.loadPage(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *normalizedPlanStream) hasHead() bool {
	return s.recordIndex < len(s.records) || s.gapIndex < len(s.gaps)
}

func (s *normalizedPlanStream) nextHead() NormalizedItem {
	identity := NormalizedItem{
		Version:           ServiceVersionV1,
		DatasetID:         s.request.DatasetID,
		CatalogSnapshotID: s.request.CatalogSnapshotID,
		SchemaName:        s.request.SchemaName,
		SchemaVersion:     s.request.SchemaVersion,
	}
	if s.gapIndex < len(s.gaps) && (s.recordIndex == len(s.records) || compareGapRow(s.gaps[s.gapIndex], s.records[s.recordIndex]) <= 0) {
		gap := s.gaps[s.gapIndex]
		s.gapIndex++
		identity.Type = NormalizedGapKind
		identity.Gap = &gap
		return identity
	}
	row := s.records[s.recordIndex]
	s.recordIndex++
	identity.Type = NormalizedRecordKind
	identity.Record = &row
	return identity
}

func (s *normalizedPlanStream) loadPage(ctx context.Context) error {
	pageCtx, cancel := context.WithCancel(ctx)
	stop := context.AfterFunc(s.ctx, cancel)
	defer func() {
		stop()
		cancel()
	}()

	spec := warehouse.QuerySpec{
		Dataset:             s.plan.Dataset,
		SourceIDs:           slices.Clone(s.request.SourceIDs),
		ChannelIDs:          slices.Clone(s.request.ChannelIDs),
		InstrumentUIDs:      slices.Clone(s.request.InstrumentUIDs),
		StartReceivedTimeNS: s.request.StartReceivedTimeNS,
		EndReceivedTimeNS:   s.request.EndReceivedTimeNS,
		Limit:               s.plan.PageSize,
	}
	if s.after != nil {
		after := *s.after
		spec.After = &after
	}
	page, err := s.plan.Reader.Page(pageCtx, spec)
	if err != nil {
		return err
	}
	if page.Dataset != s.plan.Dataset || len(page.Rows) > s.plan.PageSize || len(page.Coverage)+len(page.Gaps) > warehouse.MaximumQueryReferences {
		return fmt.Errorf("%w: normalized reader escaped plan bounds", ErrInvalidServiceRequest)
	}
	if page.HasMore && len(page.Rows) == 0 {
		return fmt.Errorf("%w: normalized reader did not advance", ErrInvalidServiceRequest)
	}
	if err := validateNormalizedPageRows(s.request, page.Rows, spec.After, page.LastKey); err != nil {
		return err
	}
	if !s.loaded {
		if err := validateNormalizedGaps(s.request, page.Gaps); err != nil {
			return err
		}
		s.referenceGaps = slices.Clone(page.Gaps)
		s.gaps = slices.Clone(page.Gaps)
		slices.SortFunc(s.gaps, compareNormalizedGaps)
	} else if !slices.Equal(page.Gaps, s.referenceGaps) {
		return fmt.Errorf("%w: normalized gap references changed between pages", ErrInvalidServiceRequest)
	}

	s.records = slices.Clone(page.Rows)
	s.recordIndex = 0
	s.hasMore = page.HasMore
	s.loaded = true
	if page.LastKey == nil {
		s.after = nil
	} else {
		last := *page.LastKey
		s.after = &last
	}
	return nil
}

func validateNormalizedPageRows(request ServiceRequest, rows []warehouse.QueryRow, after, last *warehouse.SortKey) error {
	var previous *warehouse.SortKey
	for index := range rows {
		item := NormalizedItem{Version: ServiceVersionV1, Type: NormalizedRecordKind, DatasetID: request.DatasetID,
			CatalogSnapshotID: request.CatalogSnapshotID, SchemaName: request.SchemaName, SchemaVersion: request.SchemaVersion, Record: &rows[index]}
		if err := item.ValidateFor(request); err != nil {
			return err
		}
		key, err := rows[index].SortKey()
		if err != nil {
			return err
		}
		if (after != nil && warehouse.CompareSortKey(key, *after) <= 0) || (previous != nil && warehouse.CompareSortKey(key, *previous) <= 0) {
			return warehouse.ErrUnstableQueryResult
		}
		keyCopy := key
		previous = &keyCopy
	}
	if len(rows) == 0 {
		if last != nil {
			return fmt.Errorf("%w: empty normalized page has a last key", ErrInvalidServiceRequest)
		}
		return nil
	}
	if last == nil || previous == nil || warehouse.CompareSortKey(*last, *previous) != 0 {
		return fmt.Errorf("%w: normalized page last key differs from final row", ErrInvalidServiceRequest)
	}
	return nil
}

func validateNormalizedGaps(request ServiceRequest, gaps []warehouse.GapReference) error {
	for index := range gaps {
		item := NormalizedItem{Version: ServiceVersionV1, Type: NormalizedGapKind, DatasetID: request.DatasetID,
			CatalogSnapshotID: request.CatalogSnapshotID, SchemaName: request.SchemaName, SchemaVersion: request.SchemaVersion, Gap: &gaps[index]}
		if err := item.ValidateFor(request); err != nil {
			return err
		}
		if index > 0 && gaps[index].ID <= gaps[index-1].ID {
			return fmt.Errorf("%w: normalized gap identity order", ErrInvalidServiceRequest)
		}
	}
	return nil
}

func compareNormalizedGaps(left, right warehouse.GapReference) int {
	if n := strings.Compare(left.Tuple.SourceID, right.Tuple.SourceID); n != 0 {
		return n
	}
	if n := strings.Compare(left.Tuple.InstrumentUID, right.Tuple.InstrumentUID); n != 0 {
		return n
	}
	if left.StartReceivedTimeNS < right.StartReceivedTimeNS {
		return -1
	}
	if left.StartReceivedTimeNS > right.StartReceivedTimeNS {
		return 1
	}
	if n := strings.Compare(left.Tuple.ChannelID, right.Tuple.ChannelID); n != 0 {
		return n
	}
	return strings.Compare(left.ID, right.ID)
}

func compareGapRow(gap warehouse.GapReference, row warehouse.QueryRow) int {
	if n := strings.Compare(gap.Tuple.SourceID, row.SourceID); n != 0 {
		return n
	}
	if n := strings.Compare(gap.Tuple.InstrumentUID, row.InstrumentUID); n != 0 {
		return n
	}
	if gap.StartReceivedTimeNS < row.ReceivedTimeNS {
		return -1
	}
	if gap.StartReceivedTimeNS > row.ReceivedTimeNS {
		return 1
	}
	return -1
}

var _ NormalizedOpener = (*NormalizedReplayService)(nil)
var _ NormalizedPlanResolver = (*NormalizedResolver)(nil)
var _ NormalizedReader = (*warehouse.QueryAdapter)(nil)
var _ NormalizedDatasetLookup = (*catalog.QueryStore)(nil)
