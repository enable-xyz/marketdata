package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/collector"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/quality"
	"github.com/enable-xyz/marketdata/segment"
	"github.com/enable-xyz/marketdata/warehouse"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const maximumResolvedSecretBytes = 64 << 10

type environmentSecretResolver struct{}

func (environmentSecretResolver) Resolve(ctx context.Context, reference string) ([]byte, error) {
	if ctx == nil {
		return nil, errors.New("secret resolution requires a context")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	value, found := os.LookupEnv(reference)
	if !found || value == "" {
		return nil, errors.New("configured secret binding is absent")
	}
	if !strings.HasPrefix(value, "@") {
		if len(value) > maximumResolvedSecretBytes {
			return nil, errors.New("configured literal secret exceeds the byte bound")
		}
		return []byte(value), nil
	}
	path := strings.TrimPrefix(value, "@")
	if path == "" || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return nil, errors.New("configured secret file must be one clean absolute path")
	}
	checked, err := os.Lstat(path)
	if err != nil {
		return nil, errors.New("configured secret file is unavailable")
	}
	if !validSecretFileInfo(checked) {
		return nil, errors.New("configured secret file must be bounded, regular, and non-symlink")
	}
	return readValidatedSecretFile(path, checked)
}

func readValidatedSecretFile(path string, checked os.FileInfo) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, errors.New("opening configured secret file failed")
	}
	defer file.Close()

	opened, err := file.Stat()
	if err != nil || !os.SameFile(checked, opened) || !validSecretFileInfo(opened) {
		return nil, errors.New("configured secret file changed during validation")
	}
	return readExactSecret(file, opened.Size())
}

func readExactSecret(reader io.Reader, size int64) ([]byte, error) {
	secret := make([]byte, int(size))
	if _, err := io.ReadFull(reader, secret); err != nil {
		clear(secret)
		return nil, errors.New("reading configured secret file failed")
	}

	var extra [1]byte
	extraBytes, err := reader.Read(extra[:])
	clear(extra[:])
	if extraBytes != 0 {
		clear(secret)
		return nil, errors.New("configured secret file has invalid length")
	}
	if !errors.Is(err, io.EOF) {
		clear(secret)
		return nil, errors.New("reading configured secret file failed")
	}
	return secret, nil
}

func validSecretFileInfo(info os.FileInfo) bool {
	return info != nil && info.Mode().IsRegular() && info.Mode()&os.ModeSymlink == 0 &&
		info.Size() >= 1 && info.Size() <= maximumResolvedSecretBytes
}

func productionHTTPClient(cfg config.Config, timeout time.Duration) (*http.Client, error) {
	if timeout <= 0 {
		return nil, errors.New("production HTTP timeout must be positive")
	}
	dialTimeout := min(timeout, 10*time.Second)
	transport := &http.Transport{
		Proxy:                 nil,
		DialContext:           (&net.Dialer{Timeout: dialTimeout, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          max(16, cfg.Runtime.MaxConcurrency*4),
		MaxIdleConnsPerHost:   max(4, cfg.Runtime.MaxConcurrency),
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   dialTimeout,
		ResponseHeaderTimeout: timeout,
		ExpectContinueTimeout: time.Second,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return &http.Client{
		Transport: transport,
		Timeout:   timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return errors.New("redirects are disabled")
		},
	}, nil
}

func openCatalogPool(ctx context.Context, cfg config.Config, resolver environmentSecretResolver) (*pgxpool.Pool, error) {
	secret, err := resolver.Resolve(ctx, cfg.Catalog.DSNRef)
	if err != nil {
		return nil, err
	}
	defer clear(secret)
	poolConfig, err := pgxpool.ParseConfig(string(secret))
	if err != nil {
		return nil, errors.New("parsing configured PostgreSQL DSN failed")
	}
	poolConfig.MinConns = int32(cfg.Catalog.MinConns)
	poolConfig.MaxConns = int32(cfg.Catalog.MaxConns)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, errors.New("opening configured PostgreSQL pool failed")
	}
	if err := verifyPostgresMajor(ctx, pool, cfg.Catalog.ServerMajors); err != nil {
		pool.Close()
		return nil, err
	}
	return pool, nil
}

func openCatalogConnection(ctx context.Context, cfg config.Config, resolver environmentSecretResolver) (*pgx.Conn, error) {
	secret, err := resolver.Resolve(ctx, cfg.Catalog.DSNRef)
	if err != nil {
		return nil, err
	}
	defer clear(secret)
	connection, err := pgx.Connect(ctx, string(secret))
	if err != nil {
		return nil, errors.New("opening configured PostgreSQL connection failed")
	}
	if err := verifyPostgresMajor(ctx, connection, cfg.Catalog.ServerMajors); err != nil {
		_ = connection.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	return connection, nil
}

type postgresVersionReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func verifyPostgresMajor(ctx context.Context, reader postgresVersionReader, allowed []int) error {
	var encoded string
	if err := reader.QueryRow(ctx, "SHOW server_version_num").Scan(&encoded); err != nil {
		return errors.New("reading configured PostgreSQL version failed")
	}
	version, err := strconv.Atoi(encoded)
	if err != nil || version < 10_00_00 {
		return errors.New("configured PostgreSQL returned an invalid version")
	}
	major := version / 10_000
	if !slices.Contains(allowed, major) {
		return fmt.Errorf("configured PostgreSQL major %d is outside the declared allowlist", major)
	}
	return nil
}

type productionWriterLease struct {
	connection *pgx.Conn
	key        string
}

func acquireProductionWriterLease(
	ctx context.Context,
	cfg config.Config,
	resolver environmentSecretResolver,
) (*productionWriterLease, error) {
	if cfg.Deployment.WriterLeaseKey == "" || cfg.Deployment.WriterID == "" {
		return nil, errors.New("production writer requires an explicit lease key and writer identity")
	}
	connection, err := openCatalogConnection(ctx, cfg, resolver)
	if err != nil {
		return nil, err
	}
	closeOnError := func(err error) (*productionWriterLease, error) {
		_ = connection.Close(context.WithoutCancel(ctx))
		return nil, err
	}
	if _, err := connection.Exec(ctx, "SELECT set_config('application_name', $1, false)", "enable-market collector "+cfg.Deployment.WriterID); err != nil {
		return closeOnError(errors.New("binding configured writer identity to PostgreSQL failed"))
	}
	var acquired bool
	if err := connection.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtextextended($1, 0))", cfg.Deployment.WriterLeaseKey).Scan(&acquired); err != nil {
		return closeOnError(errors.New("acquiring configured PostgreSQL writer lease failed"))
	}
	if !acquired {
		return closeOnError(errors.New("configured canonical writer lease is already held"))
	}
	return &productionWriterLease{connection: connection, key: cfg.Deployment.WriterLeaseKey}, nil
}

func (l *productionWriterLease) Release(ctx context.Context) error {
	if l == nil || l.connection == nil {
		return nil
	}
	var released bool
	err := l.connection.QueryRow(ctx, "SELECT pg_advisory_unlock(hashtextextended($1, 0))", l.key).Scan(&released)
	closeErr := l.connection.Close(context.WithoutCancel(ctx))
	l.connection = nil
	if err != nil {
		return errors.Join(errors.New("releasing configured PostgreSQL writer lease failed"), closeErr)
	}
	if !released {
		return errors.Join(errors.New("configured PostgreSQL writer lease was not held"), closeErr)
	}
	return closeErr
}

type objectCredentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
}

func openObjectStore(ctx context.Context, cfg config.Config, resolver environmentSecretResolver) (*objectstore.PrefixClient, error) {
	secret, err := resolver.Resolve(ctx, cfg.ObjectStore.CredentialRef)
	if err != nil {
		return nil, err
	}
	defer clear(secret)
	var credential objectCredentials
	if err := decodeStrictJSON(secret, &credential); err != nil || credential.AccessKeyID == "" || credential.SecretAccessKey == "" {
		return nil, errors.New("configured object-store credential must be strict access-key JSON")
	}
	httpClient, err := productionHTTPClient(cfg, 5*time.Minute)
	if err != nil {
		return nil, err
	}
	provider := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID: credential.AccessKeyID, SecretAccessKey: credential.SecretAccessKey,
			SessionToken: credential.SessionToken, Source: "explicit-config",
		}, nil
	})
	client, err := objectstore.NewAWSClient(aws.Config{
		Region: cfg.ObjectStore.Region, Credentials: provider, HTTPClient: httpClient,
	}, cfg.ObjectStore.Bucket, objectstore.AWSOptions{
		UsePathStyle: cfg.ObjectStore.PathStyle, Endpoint: cfg.ObjectStore.Endpoint,
	})
	if err != nil {
		return nil, err
	}
	prefixed, err := objectstore.NewPrefixClient(client, cfg.ObjectStore.Prefix)
	if err != nil {
		return nil, err
	}
	if _, err := prefixed.List(ctx, "raw", ""); err != nil {
		var objectErr *objectstore.Error
		if !errors.As(err, &objectErr) {
			return nil, errors.New("probing configured object store failed")
		}
		return nil, errors.New("configured object store is unavailable")
	}
	return prefixed, nil
}

func openWarehouse(ctx context.Context, cfg config.Config, resolver environmentSecretResolver) (*warehouse.NativeStore, error) {
	selection := warehouse.PinnedX5ProductionSelection()
	if cfg.Warehouse.ServerDigest != selection.ServerDigest || cfg.Warehouse.BatchRows != selection.Config.BatchRows {
		return nil, errors.New("warehouse server digest and batch size must match the pinned production selection")
	}
	secret, err := resolver.Resolve(ctx, cfg.Warehouse.DSNRef)
	if err != nil {
		return nil, err
	}
	defer clear(secret)
	store, err := warehouse.OpenNative(ctx, warehouse.NativeConfig{
		DSN: string(secret), Database: cfg.Warehouse.Database, ServerDigest: cfg.Warehouse.ServerDigest,
		BatchRows: selection.Config.BatchRows, Compression: selection.Config.Compression, Layout: selection.Config.Layout,
	})
	if err != nil {
		return nil, errors.New("opening configured ClickHouse warehouse failed")
	}
	return store, nil
}

type checkpointingPublisher struct {
	next        collector.SegmentPublisher
	checkpoints *catalog.QueryStore
}

func (p *checkpointingPublisher) Publish(ctx context.Context, request objectstore.PublishRequest) (objectstore.PublishResult, error) {
	result, err := p.next.Publish(ctx, request)
	if err != nil {
		return result, err
	}
	checkpoint, err := checkpointForReady(request.Ready)
	if err != nil {
		return result, err
	}
	current, readErr := p.checkpoints.Checkpoint(ctx, checkpoint.Key)
	switch {
	case readErr == nil:
		comparison := compareRuntimeCheckpoint(current, checkpoint)
		if comparison > 0 {
			return result, nil
		}
		if comparison == 0 {
			if current.StateSHA256 != checkpoint.StateSHA256 || !bytes.Equal(current.StateBytes, checkpoint.StateBytes) {
				return result, errors.New("collector checkpoint conflicts at a committed coordinate")
			}
			return result, nil
		}
	case errors.Is(readErr, catalog.ErrQueryNotFound):
	case readErr != nil:
		return result, readErr
	}
	if err := p.checkpoints.PutCheckpoint(ctx, checkpoint); err != nil {
		return result, err
	}
	return result, nil
}

func checkpointForReady(ready segment.ReadySegment) (catalog.RuntimeCheckpoint, error) {
	epoch, err := uuid.Parse(ready.Manifest.EpochID)
	if err != nil || ready.Manifest.SourceID == "" || ready.Manifest.ChannelID == "" || ready.Manifest.Segment.LastReceivedAtNS < 0 {
		return catalog.RuntimeCheckpoint{}, errors.New("committed segment has an invalid checkpoint coordinate")
	}
	state, err := json.Marshal(map[string]any{
		"content_sha256":  hex.EncodeToString(ready.Manifest.Segment.CompressedSHA256[:]),
		"manifest_sha256": hex.EncodeToString(ready.ManifestSHA256[:]),
		"object_key":      ready.Manifest.ObjectKey,
		"ordinal_end":     ready.Manifest.Segment.LastOrdinal,
		"version":         1,
	})
	if err != nil {
		return catalog.RuntimeCheckpoint{}, err
	}
	return catalog.RuntimeCheckpoint{
		Key:      "collector/" + ready.Manifest.SourceID + "/" + ready.Manifest.ChannelID,
		SourceID: ready.Manifest.SourceID, ChannelID: ready.Manifest.ChannelID,
		ReceivedTimeNS: ready.Manifest.Segment.LastReceivedAtNS, StreamEpochID: epoch.String(),
		ArrivalOrdinal: ready.Manifest.Segment.LastOrdinal, MessageOrdinal: 0,
		StateSHA256: sha256.Sum256(state), StateBytes: state,
		UpdatedAt: time.Unix(0, ready.Manifest.Segment.LastReceivedAtNS).UTC(),
	}, nil
}

func compareRuntimeCheckpoint(left, right catalog.RuntimeCheckpoint) int {
	if left.ReceivedTimeNS != right.ReceivedTimeNS {
		if left.ReceivedTimeNS < right.ReceivedTimeNS {
			return -1
		}
		return 1
	}
	if value := strings.Compare(left.StreamEpochID, right.StreamEpochID); value != 0 {
		return value
	}
	if left.ArrivalOrdinal != right.ArrivalOrdinal {
		if left.ArrivalOrdinal < right.ArrivalOrdinal {
			return -1
		}
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

type catalogGapRecorder struct {
	query   *catalog.QueryStore
	quality *catalog.QualityStore
}

func (r *catalogGapRecorder) OpenGap(ctx context.Context, sourceID, channelID, nativeSymbol string) (string, bool, error) {
	instrumentUID, found, err := r.instrumentUID(ctx, sourceID, nativeSymbol)
	if err != nil || nativeSymbol != "" && !found {
		return "", false, err
	}
	gap, err := r.query.OpenGap(ctx, sourceID, channelID, instrumentUID)
	if errors.Is(err, catalog.ErrQueryNotFound) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return gap.ID, true, nil
}

func (r *catalogGapRecorder) RecordGap(ctx context.Context, observation collector.GapObservation) (string, error) {
	instrumentUID, found, err := r.instrumentUID(ctx, observation.SourceID, observation.NativeSymbol)
	if err != nil {
		return "", err
	}
	if observation.NativeSymbol != "" && !found {
		return "", errors.New("gap instrument is absent from the committed catalog")
	}
	start := observation.Interval.StartedWallTimeNS
	end := observation.Interval.DetectedWallTimeNS
	if start < 0 || end < start {
		return "", errors.New("collector gap interval is invalid")
	}
	if end == start {
		if end == int64(^uint64(0)>>1) {
			return "", errors.New("collector gap interval overflows")
		}
		end++
	}
	epochID := uuid.UUID(observation.Epoch.ID).String()
	coordinate, _ := json.Marshal(map[string]any{
		"arrival_ordinal": observation.LastArrivalOrdinal,
		"stream_epoch_id": epochID,
	})
	evidence, _ := json.Marshal(map[string]any{
		"detected_wall_time_ns": observation.Interval.DetectedWallTimeNS,
		"epoch_kind":            uint8(observation.Epoch.Kind),
		"native_symbol":         observation.NativeSymbol,
		"queue_pressure":        observation.Interval.QueuePressure,
		"reason":                uint8(observation.Interval.Reason),
		"started_wall_time_ns":  observation.Interval.StartedWallTimeNS,
	})
	families := gapFamilies(observation.ChannelID)
	id := deterministicGapID(observation, instrumentUID)
	gap, err := quality.NewGap(quality.Gap{
		GapID: id, SourceID: observation.SourceID, ChannelID: observation.ChannelID,
		InstrumentUID: instrumentUID, RangeStartNS: start, RangeEndNS: end,
		DetectionBasis: "collector_blind_interval", FirstGoodCoordinate: coordinate,
		LastGoodCoordinate: coordinate, AffectedFamilies: families, Confidence: 1,
		Evidence: evidence, State: quality.GapOpen, DetectedTimeNS: observation.Interval.DetectedWallTimeNS,
	})
	if err != nil {
		return "", err
	}
	if err := r.quality.RecordGap(ctx, gap); err != nil {
		return "", err
	}
	return id, nil
}

func (r *catalogGapRecorder) ResolveGap(ctx context.Context, id string, resolvedNS int64) error {
	current, err := r.query.Gap(ctx, id)
	if err != nil {
		return err
	}
	if current.State == string(quality.GapRecoveredCurrentState) {
		return nil
	}
	if current.State != string(quality.GapOpen) {
		return errors.New("collector gap is already terminal")
	}
	if resolvedNS < current.DetectedTimeNS {
		resolvedNS = current.DetectedTimeNS
	}
	return r.quality.ResolveGap(ctx, id, quality.GapRecoveredCurrentState, resolvedNS)
}

func (r *catalogGapRecorder) instrumentUID(ctx context.Context, sourceID, nativeSymbol string) (string, bool, error) {
	if nativeSymbol == "" {
		return "", true, nil
	}
	instruments, err := r.query.Instruments(ctx)
	if err != nil {
		return "", false, err
	}
	for _, instrument := range instruments {
		if instrument.SourceID == sourceID && instrument.NativeID == nativeSymbol {
			return instrument.InstrumentUID, true, nil
		}
	}
	return "", false, nil
}

func deterministicGapID(observation collector.GapObservation, instrumentUID string) string {
	hasher := sha256.New()
	_, _ = hasher.Write([]byte("collector-gap-v1\x00"))
	writeGapString := func(value string) {
		_ = binary.Write(hasher, binary.BigEndian, uint32(len(value)))
		_, _ = hasher.Write([]byte(value))
	}
	writeGapString(observation.SourceID)
	writeGapString(observation.ChannelID)
	writeGapString(instrumentUID)
	_, _ = hasher.Write(observation.Epoch.ID[:])
	_ = binary.Write(hasher, binary.BigEndian, observation.LastArrivalOrdinal)
	_ = binary.Write(hasher, binary.BigEndian, observation.Interval.StartedWallTimeNS)
	_ = binary.Write(hasher, binary.BigEndian, observation.Interval.DetectedWallTimeNS)
	_ = binary.Write(hasher, binary.BigEndian, uint8(observation.Interval.Reason))
	sum := hasher.Sum(nil)
	id := make([]byte, 16)
	copy(id, sum)
	id[6] = id[6]&0x0f | 0x50
	id[8] = id[8]&0x3f | 0x80
	return uuid.UUID(id).String()
}

func gapFamilies(channelID string) []string {
	switch channelID {
	case binance.SpotRawChannel:
		return []string{"book_update", "quote", "ticker", "trade"}
	case binance.SpotDepthChannel:
		return []string{"book_snapshot"}
	case binance.SpotExchangeInfoChannel:
		return []string{"catalog"}
	default:
		return []string{"raw"}
	}
}
