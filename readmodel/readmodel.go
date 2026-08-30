// Package readmodel adapts the PostgreSQL catalog projections to serving and
// warehouse query contracts without coupling those owner packages to each other.
package readmodel

import (
	"context"
	"errors"
	"fmt"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/serve"
	"github.com/enable-xyz/marketdata/warehouse"
)

// Catalog is the exact read-only PostgreSQL projection used by the query
// server and ClickHouse query adapter.
type Catalog interface {
	Sources(context.Context) ([]catalog.SourceProjection, error)
	Instruments(context.Context) ([]catalog.InstrumentProjection, error)
	Coverage(context.Context) ([]catalog.CoverageProjection, error)
	Incidents(context.Context) ([]catalog.IncidentProjection, error)
	Datasets(context.Context) ([]catalog.DatasetProjection, error)
	Dataset(context.Context, string) (catalog.DatasetIdentity, error)
	LatestDataset(context.Context, string) (catalog.DatasetIdentity, error)
	References(context.Context, catalog.ReferenceFilter) ([]catalog.CoverageReferenceProjection, []catalog.GapReferenceProjection, error)
	Ready() bool
}

// Store is a stateless projection adapter. The caller owns the underlying
// catalog connection and must keep it open for the Store's lifetime.
type Store struct {
	catalog Catalog
}

func New(store Catalog) (*Store, error) {
	if store == nil {
		return nil, errors.New("readmodel: catalog is required")
	}
	return &Store{catalog: store}, nil
}

func (s *Store) Ready() bool {
	return s != nil && s.catalog != nil && s.catalog.Ready()
}

func (s *Store) Sources(ctx context.Context) ([]serve.CatalogSource, error) {
	values, err := s.catalog.Sources(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]serve.CatalogSource, len(values))
	for i, value := range values {
		result[i] = serve.CatalogSource{
			SourceID: value.SourceID, Venue: value.Venue, ProductFamily: value.ProductFamily,
			APIFamily: value.APIFamily, Environment: value.Environment, Lifecycle: value.Lifecycle,
		}
	}
	return result, nil
}

func (s *Store) Instruments(ctx context.Context) ([]serve.CatalogInstrument, error) {
	values, err := s.catalog.Instruments(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]serve.CatalogInstrument, len(values))
	for i, value := range values {
		result[i] = serve.CatalogInstrument{
			InstrumentUID: value.InstrumentUID, SourceID: value.SourceID, NativeID: value.NativeID,
			ListingGeneration: value.ListingGeneration, Lifecycle: value.Lifecycle, BaseAsset: value.BaseAsset,
			QuoteAsset: value.QuoteAsset, MarginAsset: value.MarginAsset, SettlementAsset: value.SettlementAsset,
			Kind: value.Kind, Multiplier: value.Multiplier,
		}
	}
	return result, nil
}

func (s *Store) Coverage(ctx context.Context) ([]serve.Coverage, error) {
	values, err := s.catalog.Coverage(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]serve.Coverage, len(values))
	for i, value := range values {
		result[i] = serve.Coverage{
			ID: value.ID, Tuple: tuple(value.Tuple), StartReceivedTimeNS: value.StartReceivedTimeNS,
			EndReceivedTimeNS: value.EndReceivedTimeNS, State: value.State,
			CatalogSnapshotID: value.CatalogSnapshotID, DatasetID: value.DatasetID,
		}
	}
	return result, nil
}

func (s *Store) Incidents(ctx context.Context) ([]serve.Incident, error) {
	values, err := s.catalog.Incidents(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]serve.Incident, len(values))
	for i, value := range values {
		result[i] = serve.Incident{
			ID: value.ID, Tuple: tuple(value.Tuple), StartReceivedTimeNS: value.StartReceivedTimeNS,
			EndReceivedTimeNS: value.EndReceivedTimeNS, Kind: value.Kind, GapRefID: value.GapRefID,
			CatalogSnapshotID: value.CatalogSnapshotID,
		}
	}
	return result, nil
}

func (s *Store) Datasets(ctx context.Context) ([]serve.CatalogDataset, error) {
	values, err := s.catalog.Datasets(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]serve.CatalogDataset, len(values))
	for i, value := range values {
		result[i] = serve.CatalogDataset{
			DatasetID: value.DatasetID, Family: value.Family, CatalogSnapshotID: value.CatalogSnapshotID,
			SchemaName: value.SchemaName, SchemaVersion: value.SchemaVersion, Committed: value.Committed,
		}
	}
	return result, nil
}

func (s *Store) Dataset(ctx context.Context, id string) (warehouse.Dataset, error) {
	value, err := s.catalog.Dataset(ctx, id)
	if err != nil {
		return warehouse.Dataset{}, err
	}
	return warehouseDataset(value)
}

func (s *Store) LatestDataset(ctx context.Context, family string) (warehouse.Dataset, error) {
	value, err := s.catalog.LatestDataset(ctx, family)
	if err != nil {
		return warehouse.Dataset{}, err
	}
	return warehouseDataset(value)
}

func warehouseDataset(value catalog.DatasetIdentity) (warehouse.Dataset, error) {
	result := warehouse.Dataset{
		ID: warehouse.GenerationID(value.ID), Family: value.Family,
		CatalogSnapshotID: warehouse.Hash(value.CatalogSnapshotID), SchemaName: value.SchemaName,
		SchemaVersion: value.SchemaVersion,
	}
	if err := result.Validate(); err != nil {
		return warehouse.Dataset{}, fmt.Errorf("readmodel: invalid catalog dataset projection: %w", err)
	}
	return result, nil
}

func (s *Store) References(ctx context.Context, spec warehouse.QuerySpec) ([]warehouse.CoverageReference, []warehouse.GapReference, error) {
	coverage, gaps, err := s.catalog.References(ctx, catalog.ReferenceFilter{
		DatasetID: spec.Dataset.IDString(), SourceIDs: spec.SourceIDs, ChannelIDs: spec.ChannelIDs,
		InstrumentUIDs: spec.InstrumentUIDs, StartReceivedTimeNS: spec.StartReceivedTimeNS,
		EndReceivedTimeNS: spec.EndReceivedTimeNS, Limit: warehouse.MaximumQueryReferences,
	})
	if err != nil {
		return nil, nil, err
	}
	coverageResult := make([]warehouse.CoverageReference, len(coverage))
	for i, value := range coverage {
		coverageResult[i] = warehouse.CoverageReference{
			ID: value.ID, Tuple: tuple(value.Tuple), StartReceivedTimeNS: value.StartReceivedTimeNS,
			EndReceivedTimeNS: value.EndReceivedTimeNS, State: value.State,
		}
	}
	gapResult := make([]warehouse.GapReference, len(gaps))
	for i, value := range gaps {
		gapResult[i] = warehouse.GapReference{
			ID: value.ID, Tuple: tuple(value.Tuple), StartReceivedTimeNS: value.StartReceivedTimeNS,
			EndReceivedTimeNS: value.EndReceivedTimeNS, Kind: value.Kind,
		}
	}
	return coverageResult, gapResult, nil
}

func tuple(value catalog.TupleProjection) warehouse.Tuple {
	return warehouse.Tuple{SourceID: value.SourceID, ChannelID: value.ChannelID, InstrumentUID: value.InstrumentUID}
}

var _ serve.MetadataReader = (*Store)(nil)
var _ serve.DatasetResolver = (*Store)(nil)
var _ warehouse.NativeQueryReferenceReader = (*Store)(nil)
