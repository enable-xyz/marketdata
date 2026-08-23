package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/bybit"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/cmd"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/objectstore"
	"github.com/enable-xyz/marketdata/verify"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func newCommand() *cmd.Command {
	return cmd.New(cmd.Dependencies{
		Build: cmd.BuildInfo{
			Version: version,
			Commit:  commit,
			Date:    buildDate,
		},
		LoadConfig:     config.Load,
		ValidateSecret: validateEnvironmentSecret,
		Run:            runRole,
		VerifyVenue:    runVerifyVenue,
	})
}

func runRole(_ context.Context, role string, _ config.Config, _ io.Writer) error {
	return fmt.Errorf("%s is not available in this build", role)
}

func validateEnvironmentSecret(ctx context.Context, reference string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	value, present := os.LookupEnv(reference)
	if !present || value == "" {
		return errors.New("configured environment binding is absent")
	}
	return nil
}

func runVerifyVenue(ctx context.Context, venue string, cfg config.Config, output io.Writer) error {
	if output == nil {
		return errors.New("verify venue output is required")
	}
	switch venue {
	case "binance-usdm":
		manifestPath, err := derivativeFixtureManifest(cfg)
		if err != nil {
			return err
		}
		evidence, err := binance.VerifyUSDMFixtures(manifestPath)
		if err != nil {
			return err
		}
		return writeVenueEvidence(output, evidence)
	case "bybit-v5":
		manifestPath, err := derivativeFixtureManifest(cfg)
		if err != nil {
			return err
		}
		evidence, err := bybit.VerifyFixtures(manifestPath)
		if err != nil {
			return err
		}
		return writeVenueEvidence(output, evidence)
	case "binance-spot":
	default:
		return fmt.Errorf("unsupported verify venue %q", venue)
	}
	if err := verify.ValidateVenueInputs(cfg); err != nil {
		return err
	}
	if err := verify.ValidateRuntimeRoots(cfg); err != nil {
		return err
	}
	dependencies := verify.Dependencies{}
	var closeDatabase func()
	if cfg.Verify.Mode == config.VerifyModeLive {
		live, close, err := liveVerifyDependencies(ctx, cfg)
		if err != nil {
			return err
		}
		dependencies = live
		closeDatabase = close
		defer closeDatabase()
	}
	encoded, err := verify.RunVenue(ctx, venue, cfg, verify.BuildInfo{Version: version, Commit: commit, Date: buildDate}, dependencies)
	if err != nil {
		return err
	}
	_, err = output.Write(encoded)
	return err
}

func writeVenueEvidence(output io.Writer, evidence any) error {
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	encoded = append(encoded, '\n')
	_, err = output.Write(encoded)
	return err
}

func derivativeFixtureManifest(cfg config.Config) (string, error) {
	rootInfo, err := os.Lstat(cfg.Verify.FixtureRoot)
	if err != nil || !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("verify fixture root must be one explicit directory")
	}
	manifestInfo, err := os.Lstat(cfg.Verify.FixtureManifest)
	if err != nil || !manifestInfo.Mode().IsRegular() || manifestInfo.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("verify fixture manifest must be one explicit regular file")
	}
	root, err := filepath.EvalSymlinks(cfg.Verify.FixtureRoot)
	if err != nil {
		return "", errors.New("resolving verify fixture root failed")
	}
	manifest, err := filepath.EvalSymlinks(cfg.Verify.FixtureManifest)
	if err != nil {
		return "", errors.New("resolving verify fixture manifest failed")
	}
	if filepath.Dir(manifest) != root {
		return "", errors.New("verify fixture manifest is outside its configured root")
	}
	return manifest, nil
}

type configuredAWSCredentials struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	SessionToken    string `json:"session_token,omitempty"`
}

func liveVerifyDependencies(ctx context.Context, cfg config.Config) (verify.Dependencies, func(), error) {
	dsn, ok := os.LookupEnv(cfg.Catalog.DSNRef)
	if !ok || dsn == "" {
		return verify.Dependencies{}, nil, errors.New("catalog DSN environment binding is absent")
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return verify.Dependencies{}, nil, errors.New("catalog DSN environment binding is invalid")
	}
	credentials, err := loadAWSCredentials(cfg.ObjectStore.CredentialRef)
	if err != nil {
		return verify.Dependencies{}, nil, err
	}
	clockID, err := newRuntimeClockID()
	if err != nil {
		return verify.Dependencies{}, nil, err
	}
	poolConfig.MinConns = int32(cfg.Catalog.MinConns)
	poolConfig.MaxConns = int32(cfg.Catalog.MaxConns)
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return verify.Dependencies{}, nil, errors.New("catalog connection construction failed")
	}
	closePool := func() { pool.Close() }
	migrationConnection, err := pool.Acquire(ctx)
	if err != nil {
		closePool()
		return verify.Dependencies{}, nil, errors.New("catalog migration connection failed")
	}
	if err := catalog.Migrate(ctx, migrationConnection.Conn()); err != nil {
		migrationConnection.Release()
		closePool()
		return verify.Dependencies{}, nil, err
	}
	migrationConnection.Release()
	publicationStore, err := catalog.NewPublicationStore(pool)
	if err != nil {
		closePool()
		return verify.Dependencies{}, nil, err
	}
	snapshot, err := catalog.LoadLatestSnapshot(ctx, pool, binance.SpotSourceID)
	if err != nil {
		closePool()
		return verify.Dependencies{}, nil, err
	}

	provider := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{
			AccessKeyID: credentials.AccessKeyID, SecretAccessKey: credentials.SecretAccessKey,
			SessionToken: credentials.SessionToken, Source: "explicit-verify-environment-binding",
		}, nil
	})
	s3HTTP := explicitHTTPClient(cfg.Verify.MaxDuration, 8)
	awsConfig := aws.Config{Region: cfg.ObjectStore.Region, Credentials: provider, HTTPClient: s3HTTP, RetryMaxAttempts: 3, RetryMode: aws.RetryModeStandard}
	s3Client, err := objectstore.NewAWSClient(awsConfig, cfg.ObjectStore.Bucket, objectstore.AWSOptions{
		UsePathStyle: cfg.ObjectStore.PathStyle, Endpoint: cfg.ObjectStore.Endpoint,
	})
	if err != nil {
		closePool()
		return verify.Dependencies{}, nil, err
	}
	objects, err := verify.NewPrefixedObjectClient(s3Client, cfg.ObjectStore.Prefix)
	if err != nil {
		closePool()
		return verify.Dependencies{}, nil, err
	}
	binanceHTTP := explicitHTTPClient(cfg.Verify.MaxDuration, config.VerifyMaximumSymbols)
	websocket, err := binance.NewCoderSpotWSConnector(binanceHTTP)
	if err != nil {
		closePool()
		return verify.Dependencies{}, nil, err
	}
	rest, err := binance.NewPublicSpotRESTClient(binance.SpotPublicRESTEndpoint, binanceHTTP)
	if err != nil {
		closePool()
		return verify.Dependencies{}, nil, err
	}
	clock, err := verify.NewSystemClock(clockID)
	if err != nil {
		closePool()
		return verify.Dependencies{}, nil, err
	}
	return verify.Dependencies{
		Objects: objects, Catalog: publicationStore, WebSocket: websocket, REST: rest, Clock: clock,
		Now: func() time.Time { return time.Now().UTC() }, CatalogSnapshot: snapshot,
	}, closePool, nil
}

func loadAWSCredentials(reference string) (configuredAWSCredentials, error) {
	encoded, ok := os.LookupEnv(reference)
	if !ok || encoded == "" {
		return configuredAWSCredentials{}, errors.New("object store credential environment binding is absent")
	}
	decoder := json.NewDecoder(bytes.NewBufferString(encoded))
	decoder.DisallowUnknownFields()
	var credentials configuredAWSCredentials
	if err := decoder.Decode(&credentials); err != nil {
		return configuredAWSCredentials{}, errors.New("object store credential environment binding is invalid")
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return configuredAWSCredentials{}, errors.New("object store credential environment binding contains trailing data")
	}
	if credentials.AccessKeyID == "" || credentials.SecretAccessKey == "" {
		return configuredAWSCredentials{}, errors.New("object store credential environment binding is incomplete")
	}
	return credentials, nil
}

func explicitHTTPClient(timeout time.Duration, maximumConnections int) *http.Client {
	dialer := &net.Dialer{Timeout: min(timeout, 10*time.Second), KeepAlive: 30 * time.Second}
	transport := &http.Transport{
		Proxy: nil, DialContext: dialer.DialContext, ForceAttemptHTTP2: true,
		MaxIdleConns: maximumConnections, MaxIdleConnsPerHost: maximumConnections, MaxConnsPerHost: maximumConnections,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: min(timeout, 10*time.Second),
		ResponseHeaderTimeout: timeout, ExpectContinueTimeout: time.Second,
		TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12},
	}
	return &http.Client{Transport: transport, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error {
		return errors.New("redirects are disabled")
	}}
}

func newRuntimeClockID() (string, error) {
	var random [16]byte
	if _, err := rand.Read(random[:]); err != nil {
		return "", errors.New("constructing a live clock epoch failed")
	}
	return "elmd-014-live-" + hex.EncodeToString(random[:]), nil
}
