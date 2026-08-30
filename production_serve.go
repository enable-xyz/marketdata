package main

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/dashboard"
	"github.com/enable-xyz/marketdata/readmodel"
	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/serve"
	"github.com/enable-xyz/marketdata/warehouse"
)

const (
	productionNativeManifestBytes = int64(64 << 20)
	productionReplayBytes         = int64(256 << 20)
	productionNormalizedItems     = 4_000_000
)

func runProductionQueryServer(ctx context.Context, cfg config.Config, composition *runtimeComposition) error {
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
	metadata, err := readmodel.New(queryStore)
	if err != nil {
		return err
	}
	warehouseStore, err := openWarehouse(ctx, cfg, resolver)
	if err != nil {
		return err
	}
	defer warehouseStore.Close()
	nativeReader, err := warehouse.NewNativeQueryReader(warehouseStore, metadata)
	if err != nil {
		return err
	}
	query, err := warehouse.NewQueryAdapter(nativeReader)
	if err != nil {
		return err
	}
	objects, err := openObjectStore(ctx, cfg, resolver)
	if err != nil {
		return err
	}
	replayConfig := replay.DefaultConfig()
	nativeResolver, err := replay.NewNativeResolver(queryStore, objects, replay.NativeResolverOptions{
		Replay: replayConfig, MaxDescriptors: min(replayConfig.MaxSegments, catalog.MaximumRawSegmentResults),
		MaxManifestBytes: productionNativeManifestBytes, MaxObjectBytes: productionReplayBytes,
	})
	if err != nil {
		return err
	}
	native, err := replay.NewFramedNativeService(nativeResolver, replay.MaximumServiceFrameBuffer)
	if err != nil {
		return err
	}
	normalizedResolver, err := replay.NewNormalizedResolver(queryStore, query, replay.NormalizedResolverOptions{
		PageSize: cfg.Serve.DefaultPageRows, MaxItems: productionNormalizedItems,
	})
	if err != nil {
		return err
	}
	normalized, err := replay.NewNormalizedReplayService(normalizedResolver)
	if err != nil {
		return err
	}
	metrics, err := readmodel.NewMetrics(composition.Metrics())
	if err != nil {
		return err
	}
	dashboardHandler, err := dashboard.New(dashboard.Config{BasePath: "/dashboard"})
	if err != nil {
		return err
	}
	serveConfig, err := productionServeConfig(cfg)
	if err != nil {
		return err
	}
	server, err := composition.NewServe(ctx, serveConfig, resolver, serve.Dependencies{
		Metadata: metadata, Datasets: metadata, Query: query,
		Native: native, Normalized: normalized, Metrics: metrics, Dashboard: dashboardHandler,
	})
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.Serve.Listener)
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Runtime.ShutdownTimeout)
		defer cancel()
		return errors.Join(err, server.Shutdown(shutdownCtx))
	}
	serveError := make(chan error, 1)
	go func() { serveError <- server.Serve(listener) }()

	select {
	case err := <-serveError:
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Runtime.ShutdownTimeout)
		defer cancel()
		return errors.Join(err, server.Shutdown(shutdownCtx))
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Runtime.ShutdownTimeout)
		defer cancel()
		shutdownErr := server.Shutdown(shutdownCtx)
		serveErr := <-serveError
		if errors.Is(ctx.Err(), context.Canceled) {
			return errors.Join(shutdownErr, serveErr)
		}
		return errors.Join(ctx.Err(), shutdownErr, serveErr)
	}
}

func productionServeConfig(cfg config.Config) (serve.Config, error) {
	configuration := serve.DefaultConfig()
	configuration.TLSCertRef = cfg.Serve.TLSCertRef
	configuration.TLSKeyRef = cfg.Serve.TLSKeyRef
	configuration.PagingKeyRef = cfg.Serve.PagingKeyRef
	configuration.MaxQueryInterval = cfg.Serve.MaxQueryInterval
	configuration.DefaultPageRows = cfg.Serve.DefaultPageRows
	configuration.MaxPageRows = cfg.Serve.MaxPageRows
	configuration.MaxResponseBytes = cfg.Serve.MaxResponseBytes
	configuration.PageTokenTTL = cfg.Serve.PageTokenTTL
	configuration.ReadHeaderTimeout = cfg.Serve.ReadHeaderTimeout
	configuration.ReadTimeout = cfg.Serve.ReadTimeout
	configuration.WriteTimeout = cfg.Serve.WriteTimeout
	configuration.IdleTimeout = cfg.Serve.IdleTimeout
	configuration.Principals = make([]serve.Principal, len(cfg.Serve.Principals))
	for index, principal := range cfg.Serve.Principals {
		scopes := make([]serve.Scope, len(principal.Scopes))
		for scopeIndex, configured := range principal.Scopes {
			scope, err := productionScope(configured)
			if err != nil {
				return serve.Config{}, err
			}
			scopes[scopeIndex] = scope
		}
		configuration.Principals[index] = serve.Principal{ID: principal.ID, TokenRef: principal.TokenRef, Scopes: scopes}
	}
	return configuration, nil
}

func productionScope(value string) (serve.Scope, error) {
	scope := serve.Scope(value)
	switch scope {
	case serve.ScopeCatalogRead, serve.ScopeCoverageRead, serve.ScopeQueryRead,
		serve.ScopeReplayNative, serve.ScopeReplayNormalized, serve.ScopeMetricsRead:
		return scope, nil
	default:
		return "", errors.New("configured query-server principal has an unsupported scope")
	}
}
