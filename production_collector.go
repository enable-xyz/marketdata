package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/collector"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/google/uuid"
)

const productionRecorderVersion = "marketdata-binance-spot-v1"

func runProductionMigration(ctx context.Context, cfg config.Config, output io.Writer) error {
	connection, err := openCatalogConnection(ctx, cfg, environmentSecretResolver{})
	if err != nil {
		return err
	}
	defer connection.Close(context.WithoutCancel(ctx))
	if err := catalog.Migrate(ctx, connection); err != nil {
		return err
	}
	return writeProductionResult(output, map[string]any{
		"complete": true,
		"role":     "migration-job",
		"schema":   catalog.MaximumSupportedSchemaVersion,
	})
}

func runProductionCollector(ctx context.Context, cfg config.Config, output io.Writer, metadataOnly bool) (runErr error) {
	source, err := productionSpotSource(cfg)
	if err != nil {
		return err
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
	declarative, _, _ := binance.SpotCatalogContract()
	if err := queryStore.BootstrapSource(ctx, declarative); err != nil {
		return err
	}
	lease, err := acquireProductionWriterLease(ctx, cfg, resolver)
	if err != nil {
		return err
	}
	defer func() {
		releaseContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Runtime.ShutdownTimeout)
		defer cancel()
		runErr = errors.Join(runErr, lease.Release(releaseContext))
	}()
	publicationStore, err := catalog.NewPublicationStore(pool)
	if err != nil {
		return err
	}
	catalogStore, err := catalog.NewStore(pool)
	if err != nil {
		return err
	}
	qualityStore, err := catalog.NewQualityStore(pool)
	if err != nil {
		return err
	}
	objects, err := openObjectStore(ctx, cfg, resolver)
	if err != nil {
		return err
	}
	publisher, err := objectstore.NewPublisher(objects, publicationStore, objectstore.PublisherConfig{
		Reconciler: objects,
	})
	if err != nil {
		return err
	}
	checkpointed := &checkpointingPublisher{next: publisher, checkpoints: queryStore}

	httpTimeout := min(30*time.Second, cfg.Capture.SegmentMaxAge/2)
	httpClient, err := productionHTTPClient(cfg, httpTimeout)
	if err != nil {
		return err
	}
	defer closeProductionHTTPClient(httpClient)
	connector, err := binance.NewCoderSpotWSConnector(httpClient)
	if err != nil {
		return err
	}
	restClient, err := binance.NewPublicSpotRESTClient(binance.SpotPublicRESTEndpoint, httpClient)
	if err != nil {
		return err
	}
	clock, err := collector.NewSystemClock(uuid.NewString())
	if err != nil {
		return err
	}
	budget, err := binance.NewSpotVenueRateBudget(0)
	if err != nil {
		return err
	}

	symbols := make([]collector.SymbolConfig, len(source.Symbols))
	for index, symbol := range source.Symbols {
		symbols[index] = collector.SymbolConfig{NativeID: symbol, DepthCadence: cfg.Capture.DepthSnapshotCadence}
	}
	production, err := collector.New(ctx, collector.Config{
		Source: collector.SourceConfig{
			WebSocketEndpoint:    binance.SpotWSEndpoint,
			PublicRESTEndpoint:   binance.SpotPublicRESTEndpoint,
			ExchangeInfoEndpoint: collector.SpotExchangeInfoEndpoint,
			ExchangeInfoMaxBytes: cfg.Capture.FrameBytes - (128 << 10),
		},
		Symbols:         symbols,
		DepthLimit:      cfg.Capture.DepthSnapshotLimit,
		MicrosecondTime: true,
		RecorderVersion: productionRecorderVersion,
		ReconnectDelay:  cfg.Capture.ReconnectDelay,
		Spool: collector.RawSinkConfig{
			Root: cfg.Capture.SpoolRoot, FrameBytes: cfg.Capture.FrameBytes,
			SegmentBytes: uint64(cfg.Capture.SegmentMaxBytes), SegmentMaxAge: cfg.Capture.SegmentMaxAge,
			MaxBytes: cfg.Runtime.SpoolMaxBytes, WriterVersion: productionRecorderVersion, CleanupPublished: true,
		},
	}, collector.Dependencies{
		WSConnector: connector, RESTClient: restClient, MetadataHTTPClient: httpClient,
		Clock: clock, RateBudget: budget, Publisher: checkpointed,
		Publications: publicationStore, Evidence: queryStore, Catalog: catalogStore,
		Gaps: &catalogGapRecorder{query: queryStore, quality: qualityStore}, Epochs: collector.NewCryptoEpochSource(),
	})
	if err != nil {
		return err
	}

	if metadataOnly {
		if err := production.SyncMetadata(ctx); err != nil {
			return err
		}
	} else if err := production.Run(ctx); err != nil {
		return err
	}
	return writeProductionResult(output, map[string]any{
		"complete": true,
		"role":     map[bool]string{true: "catalog-sync", false: "collector"}[metadataOnly],
		"stats":    production.Stats(),
	})
}

func productionSpotSource(cfg config.Config) (config.SourceConfig, error) {
	if len(cfg.Sources) != 1 {
		return config.SourceConfig{}, errors.New("production Binance Spot runtime requires exactly one source")
	}
	source := cfg.Sources[0]
	endpoints := slices.Clone(source.Endpoints)
	slices.Sort(endpoints)
	expectedEndpoints := []string{binance.SpotPublicRESTEndpoint, binance.SpotWSEndpoint}
	slices.Sort(expectedEndpoints)
	methods := slices.Clone(source.Methods)
	slices.Sort(methods)
	expectedMethods := []string{config.MethodMarketDataHTTPGet, config.MethodMarketDataWebSocket}
	slices.Sort(expectedMethods)
	if source.ID != binance.SpotSourceID || source.API != "binance-spot" ||
		!slices.Equal(endpoints, expectedEndpoints) || !slices.Equal(methods, expectedMethods) ||
		source.EntitlementRef != "" || source.EntitlementScope != "" || len(source.Symbols) == 0 {
		return config.SourceConfig{}, errors.New("production source must be the exact public Binance Spot contract")
	}
	for _, symbol := range source.Symbols {
		if symbol != strings.ToUpper(symbol) {
			return config.SourceConfig{}, errors.New("production Binance Spot symbols must use canonical uppercase native identities")
		}
	}
	return source, nil
}

func closeProductionHTTPClient(client interface{ CloseIdleConnections() }) {
	if client != nil {
		client.CloseIdleConnections()
	}
}

func writeProductionResult(output io.Writer, value any) error {
	if output == nil {
		output = io.Discard
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return fmt.Errorf("writing production role result: %w", err)
	}
	return nil
}
