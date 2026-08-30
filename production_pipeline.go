package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/dataset"
	"github.com/enable-xyz/marketdata/normalize"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/pipeline"
	"github.com/enable-xyz/marketdata/quality"
	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/warehouse"
	"github.com/google/uuid"
)

const (
	productionPartitionWindow              = time.Hour
	builderCheckpointChannel               = "dataset-builder.v1"
	productionPipelineBoundFailureClass    = "pipeline_bound"
	productionPipelineBoundReportSource    = "dataset-builder.v1"
	productionPipelineBoundAnnotation      = "derived dataset hour omitted after reaching a pipeline safety bound"
	productionPipelineBoundIncidentIDScope = "https://enable.xyz/marketdata/incidents/dataset-builder/pipeline-bound/v1"
)

var builderCheckpointEpoch = uuid.NewSHA1(uuid.NameSpaceURL, []byte("https://enable.xyz/marketdata/dataset-builder/v1")).String()

type productionIncidentRecorder interface {
	RecordIncident(context.Context, quality.Incident) error
}

type productionPipelineBoundResult struct {
	Complete      bool   `json:"complete"`
	Degraded      bool   `json:"degraded"`
	Role          string `json:"role"`
	FailureClass  string `json:"failure_class"`
	IncidentID    string `json:"incident_id"`
	SourceID      string `json:"source_id"`
	ChannelID     string `json:"channel_id"`
	WindowStartNS int64  `json:"window_start_ns"`
	WindowEndNS   int64  `json:"window_end_ns"`
}

func runProductionDatasetBuilder(ctx context.Context, cfg config.Config, output io.Writer) error {
	if _, err := productionSpotSource(cfg); err != nil {
		return err
	}
	if cfg.Dataset.PartitionWindow != productionPartitionWindow {
		return errors.New("production dataset builder requires exact one-hour UTC partitions")
	}
	resolver := environmentSecretResolver{}
	pool, err := openCatalogPool(ctx, cfg, resolver)
	if err != nil {
		return err
	}
	defer pool.Close()
	queryStore, err := catalog.NewQueryStore(ctx, pool, min(cfg.Runtime.ShutdownTimeout, 5*time.Second))
	if err != nil {
		return err
	}
	publications, err := catalog.NewPublicationStore(pool)
	if err != nil {
		return err
	}
	incidents, err := catalog.NewQualityStore(pool)
	if err != nil {
		return err
	}
	objects, err := openObjectStore(ctx, cfg, resolver)
	if err != nil {
		return err
	}

	for {
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		built, err := buildAvailableWindows(ctx, cfg, pool, queryStore, publications, incidents, objects, output)
		if err != nil {
			return err
		}
		if err := waitProductionCadence(ctx, cfg.Dataset.BuildCadence, built); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}
}

func buildAvailableWindows(
	ctx context.Context,
	cfg config.Config,
	database catalog.PublicationDatabase,
	queryStore *catalog.QueryStore,
	publications *catalog.PublicationStore,
	incidents productionIncidentRecorder,
	objects objectstore.Client,
	output io.Writer,
) (bool, error) {
	highWater, err := queryStore.Checkpoint(ctx, "collector/"+binance.SpotSourceID+"/"+binance.SpotRawChannel)
	if errors.Is(err, catalog.ErrQueryNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	start, found, err := builderNextStart(ctx, queryStore, publications)
	if err != nil || !found {
		return false, err
	}
	built := false
	for {
		end := start + int64(productionPartitionWindow)
		if end <= start || highWater.ReceivedTimeNS < end {
			return built, nil
		}
		segmentIDs, exportErr := rawSegmentsForWindow(ctx, publications, start, end)
		var receipt pipeline.ExportReceipt
		if exportErr == nil && len(segmentIDs) > 0 {
			receipt, exportErr = exportProductionWindow(ctx, cfg, database, publications, queryStore, objects, start, end, segmentIDs)
		}
		switch {
		case exportErr == nil:
			if len(segmentIDs) > 0 {
				if err := writeProductionResult(output, map[string]any{
					"complete": true, "role": "dataset-builder", "window_start_ns": start,
					"window_end_ns": end, "receipt": receipt,
				}); err != nil {
					return built, err
				}
			}
		case errors.Is(exportErr, pipeline.ErrPipelineBound):
			advance, err := handleProductionExportError(ctx, incidents, output, start, end, exportErr)
			if err != nil {
				return built, err
			}
			if !advance {
				return built, exportErr
			}
		case errors.Is(exportErr, pipeline.ErrEmptyExport):
		default:
			return built, exportErr
		}
		if err := putBuilderCheckpoint(ctx, queryStore, end); err != nil {
			return built, err
		}
		built = true
		start = end
	}
}

func handleProductionExportError(
	ctx context.Context,
	incidents productionIncidentRecorder,
	output io.Writer,
	start, end int64,
	exportErr error,
) (bool, error) {
	if !errors.Is(exportErr, pipeline.ErrPipelineBound) {
		return false, exportErr
	}
	incident, err := productionPipelineBoundIncident(start, end)
	if err != nil {
		return false, err
	}
	if err := incidents.RecordIncident(ctx, incident); err != nil {
		return false, fmt.Errorf("recording dataset-builder degraded hour incident: %w", err)
	}
	if err := writeProductionResult(output, productionPipelineBoundResult{
		Complete:      false,
		Degraded:      true,
		Role:          "dataset-builder",
		FailureClass:  productionPipelineBoundFailureClass,
		IncidentID:    incident.ID(),
		SourceID:      binance.SpotSourceID,
		ChannelID:     binance.SpotRawChannel,
		WindowStartNS: start,
		WindowEndNS:   end,
	}); err != nil {
		return false, err
	}
	return true, nil
}

func productionPipelineBoundIncident(start, end int64) (quality.Incident, error) {
	if start < 0 || start%int64(productionPartitionWindow) != 0 ||
		end-start != int64(productionPartitionWindow) {
		return quality.Incident{}, errors.New("dataset-builder degraded window is not an exact one-hour UTC partition")
	}
	affectedTuples, err := json.Marshal([]struct {
		ChannelID string `json:"channel_id"`
		SourceID  string `json:"source_id"`
	}{{
		ChannelID: binance.SpotRawChannel,
		SourceID:  binance.SpotSourceID,
	}})
	if err != nil {
		return quality.Incident{}, fmt.Errorf("encoding dataset-builder incident tuple: %w", err)
	}
	identity := productionHash(
		"dataset-builder-pipeline-bound-incident-v1",
		binance.SpotSourceID,
		binance.SpotRawChannel,
		fmt.Sprint(start),
		fmt.Sprint(end),
		productionPipelineBoundFailureClass,
	)
	namespace := uuid.NewSHA1(uuid.NameSpaceURL, []byte(productionPipelineBoundIncidentIDScope))
	return quality.NewIncident(quality.IncidentInput{
		IncidentID:     uuid.NewSHA1(namespace, identity[:]).String(),
		Annotation:     productionPipelineBoundAnnotation,
		ReportSource:   productionPipelineBoundReportSource,
		AffectedTuples: affectedTuples,
		HasRange:       true,
		RangeStartNS:   start,
		RangeEndNS:     end,
		ReportedTimeNS: end,
		CreatedTimeNS:  end,
	})
}

func builderNextStart(ctx context.Context, queryStore *catalog.QueryStore, publications *catalog.PublicationStore) (int64, bool, error) {
	checkpoint, err := queryStore.Checkpoint(ctx, builderCheckpointKey())
	if err == nil {
		var state struct {
			NextStartReceivedTimeNS int64 `json:"next_start_received_time_ns"`
			Version                 int   `json:"version"`
		}
		if decodeErr := decodeStrictJSON(checkpoint.StateBytes, &state); decodeErr != nil || state.Version != 1 || state.NextStartReceivedTimeNS < 0 || state.NextStartReceivedTimeNS%int64(productionPartitionWindow) != 0 {
			return 0, false, errors.New("dataset-builder checkpoint state is invalid")
		}
		return state.NextStartReceivedTimeNS, true, nil
	}
	if !errors.Is(err, catalog.ErrQueryNotFound) {
		return 0, false, err
	}
	var earliest int64
	found := false
	err = publications.StreamCommittedRawSegments(ctx, func(publication catalog.RawSegmentPublication) error {
		if publication.SourceID != binance.SpotSourceID || publication.ChannelID != binance.SpotRawChannel {
			return nil
		}
		if !found || publication.ReceivedStartNS < earliest {
			earliest, found = publication.ReceivedStartNS, true
		}
		return nil
	})
	if err != nil || !found {
		return 0, false, err
	}
	return time.Unix(0, earliest).UTC().Truncate(productionPartitionWindow).UnixNano(), true, nil
}

func rawSegmentsForWindow(ctx context.Context, publications *catalog.PublicationStore, start, end int64) ([]string, error) {
	segmentIDs := make([]string, 0, 64)
	err := publications.StreamCommittedRawSegments(ctx, func(publication catalog.RawSegmentPublication) error {
		if publication.SourceID != binance.SpotSourceID || publication.ChannelID != binance.SpotRawChannel ||
			publication.ReceivedStartNS >= end || publication.ReceivedEndNS < start {
			return nil
		}
		if len(segmentIDs) == pipeline.MaximumExportSegments {
			return fmt.Errorf("%w: hourly raw selection exceeds %d segments", pipeline.ErrPipelineBound, pipeline.MaximumExportSegments)
		}
		segmentIDs = append(segmentIDs, publication.SegmentID)
		return nil
	})
	return segmentIDs, err
}

func exportProductionWindow(
	ctx context.Context,
	cfg config.Config,
	database catalog.PublicationDatabase,
	publications *catalog.PublicationStore,
	queryStore *catalog.QueryStore,
	objects objectstore.Client,
	start, end int64,
	segmentIDs []string,
) (pipeline.ExportReceipt, error) {
	snapshot, err := catalog.LoadLatestSnapshot(ctx, database, binance.SpotSourceID)
	if err != nil {
		return pipeline.ExportReceipt{}, err
	}
	catalogID := normalize.Hash(snapshot.SHA256)
	binding, err := binance.NewSpotMapperBinding(catalogID, binance.SpotMapperVersion, 0, normalize.OptionalInt64{}, normalize.ResolutionMicrosecond, nil)
	if err != nil {
		return pipeline.ExportReceipt{}, err
	}
	normalizer, err := normalize.NewOrchestrator(snapshot, []normalize.BoundMapper{binding})
	if err != nil {
		return pipeline.ExportReceipt{}, err
	}
	replayConfig := replay.DefaultConfig()
	datasetPolicyID := productionHash("dataset-policy-v1", cfg.Dataset.PartitionWindow.String(), fmt.Sprint(cfg.Dataset.RowGroupBytes), cfg.Dataset.Compression)
	replayConfigID := productionHash("replay-config-v1", fmt.Sprintf("%+v", replayConfig))
	mapperSetID := productionHash("mapper-set-v1", binance.SpotMapperVersion, fmt.Sprintf("%x", snapshot.SHA256), string(normalize.ResolutionMicrosecond))
	writer := dataset.DefaultWriterOptions(datasetPolicyID, replayConfigID, normalize.Hash{1})
	writer.RowGroupTargetBytes = cfg.Dataset.RowGroupBytes
	writer.Compression = cfg.Dataset.Compression
	exporter, err := pipeline.NewExporter(publications, queryStore, objects, normalizer, pipeline.ExporterConfig{
		Replay: replayConfig, Writer: writer,
		DatasetPolicyID: datasetPolicyID, ReplayConfigID: replayConfigID,
		CatalogSnapshotID: catalogID, MapperSetID: mapperSetID,
		MaxSegments: pipeline.MaximumExportSegments, MaxRecords: pipeline.MaximumExportRecords,
		MaxOutputRows: pipeline.MaximumExportRows, NormalizeBatch: 4096,
		MaxPartitions: pipeline.MaximumExportPartitions,
	})
	if err != nil {
		return pipeline.ExportReceipt{}, err
	}
	return exporter.RunOnce(ctx, pipeline.ExportRequest{
		SourceID: binance.SpotSourceID, SegmentIDs: segmentIDs,
		StartReceivedTimeNS: start, EndReceivedTimeNS: end,
		BuildRoot: cfg.Dataset.WorkingRoot, ObjectPrefix: "datasets/" + binance.SpotSourceID,
	})
}

func putBuilderCheckpoint(ctx context.Context, store *catalog.QueryStore, nextStart int64) error {
	state, err := json.Marshal(map[string]any{"next_start_received_time_ns": nextStart, "version": 1})
	if err != nil {
		return err
	}
	coordinate := nextStart - 1
	return store.PutCheckpoint(ctx, catalog.RuntimeCheckpoint{
		Key: builderCheckpointKey(), SourceID: binance.SpotSourceID, ChannelID: builderCheckpointChannel,
		ReceivedTimeNS: coordinate, StreamEpochID: builderCheckpointEpoch,
		ArrivalOrdinal: uint64(nextStart / int64(productionPartitionWindow)),
		StateSHA256:    sha256.Sum256(state), StateBytes: state,
		UpdatedAt: time.Unix(0, coordinate).UTC(),
	})
}

func builderCheckpointKey() string { return "dataset-builder/" + binance.SpotSourceID }

func productionHash(domain string, values ...string) normalize.Hash {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte(domain))
	for _, value := range values {
		_ = binary.Write(hasher, binary.BigEndian, uint32(len(value)))
		_, _ = hasher.Write([]byte(value))
	}
	var result normalize.Hash
	copy(result[:], hasher.Sum(nil))
	return result
}

func runProductionWarehouseLoader(ctx context.Context, cfg config.Config, output io.Writer) error {
	resolver := environmentSecretResolver{}
	pool, err := openCatalogPool(ctx, cfg, resolver)
	if err != nil {
		return err
	}
	defer pool.Close()
	queryStore, err := catalog.NewQueryStore(ctx, pool, min(cfg.Runtime.ShutdownTimeout, 5*time.Second))
	if err != nil {
		return err
	}
	objects, err := openObjectStore(ctx, cfg, resolver)
	if err != nil {
		return err
	}
	warehouseStore, err := openWarehouse(ctx, cfg, resolver)
	if err != nil {
		return err
	}
	defer warehouseStore.Close()
	selection := warehouse.PinnedX5ProductionSelection()
	generationLoader, err := warehouse.NewLoader(warehouseStore, warehouse.ParquetManifestReader{}, selection.ServerDigest, selection.Config)
	if err != nil {
		return err
	}
	loader, err := pipeline.NewLoader(queryStore, objects, generationLoader, pipeline.LoaderConfig{
		MaxParquetBytes: dataset.MaxPartitionFileBytes, MaxCoverage: pipeline.MaximumLoadCoverage,
	})
	if err != nil {
		return err
	}
	loaded := make(map[string]struct{})
	for {
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
		worked := false
		err := queryStore.StreamCommittedDatasets(ctx, func(publication catalog.DatasetPublication) error {
			if _, ok := loaded[publication.DatasetID]; ok {
				return nil
			}
			receipt, err := loader.RunOnce(ctx, pipeline.LoadRequest{DatasetID: publication.DatasetID, WorkRoot: cfg.Dataset.WorkingRoot})
			if err != nil {
				return err
			}
			loaded[publication.DatasetID] = struct{}{}
			worked = true
			return writeProductionResult(output, map[string]any{"complete": true, "role": "warehouse-loader", "receipt": receipt})
		})
		if err != nil {
			return err
		}
		if err := waitProductionCadence(ctx, cfg.Dataset.BuildCadence, worked); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			return err
		}
	}
}

func waitProductionCadence(ctx context.Context, cadence time.Duration, worked bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if worked {
		return nil
	}
	timer := time.NewTimer(cadence)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
