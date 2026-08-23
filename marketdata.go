package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/bybit"
	"github.com/enable-xyz/marketdata/cmd"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/deployment"
	releaseproof "github.com/enable-xyz/marketdata/release"
	"github.com/enable-xyz/marketdata/verify"
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
		Compose:        composeCommandRuntime,
		Run:            runRole,
		VerifyVenue:    runVerifyVenue,
		VerifyRelease: func(ctx context.Context, options cmd.ReleaseVerifyOptions, output io.Writer) error {
			evidence, err := releaseproof.Verify(ctx, releaseproof.VerifyOptions{
				AMD64Binary: options.AMD64Binary, ARM64Binary: options.ARM64Binary,
				LicensePolicy: options.LicensePolicy, EvidenceOutput: options.EvidenceOutput,
			})
			if err != nil {
				return err
			}
			encoder := json.NewEncoder(output)
			encoder.SetEscapeHTML(false)
			return encoder.Encode(evidence)
		},
		WriteReleaseMetadata: releaseproof.WriteIdentity,
		SmokeRole:            runSmokeRole,
	})
}

type verifierVenueRuntime struct{}

func (*verifierVenueRuntime) DeploymentRole() deployment.Role { return deployment.RoleVerifier }

func (*verifierVenueRuntime) Shutdown(ctx context.Context) error {
	return ctx.Err()
}

func composeCommandRuntime(ctx context.Context, operation string, cfg config.Config, build cmd.BuildInfo, output io.Writer) (cmd.Runtime, error) {
	role, err := deployment.RuntimeRole(operation)
	if err != nil {
		return nil, err
	}
	if operation == "verify venue" {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if role != deployment.RoleVerifier {
			return nil, fmt.Errorf("verify venue resolved unexpected deployment role %q", role)
		}
		return &verifierVenueRuntime{}, nil
	}
	return composeRuntime(ctx, operation, cfg, build, output)
}

func runRole(ctx context.Context, operation string, cfg config.Config, _ cmd.Runtime, output io.Writer) error {
	role, err := deployment.RuntimeRole(operation)
	if err != nil {
		return err
	}
	if cfg.Deployment.Role != string(role) {
		return fmt.Errorf("deployment role %q cannot dispatch %q", cfg.Deployment.Role, operation)
	}
	switch role {
	case deployment.RoleCollector, deployment.RoleCatalogSync, deployment.RoleMigrationJob,
		deployment.RoleDatasetBuilder, deployment.RoleWarehouseLoader, deployment.RoleQueryReplayServer,
		deployment.RoleVerifier, deployment.RoleBackupRecovery:
	default:
		return fmt.Errorf("unsupported deployment role %q", role)
	}
	if !cfg.Deployment.DryRun {
		return fmt.Errorf("%s runtime requires a configured production implementation", role)
	}
	evidence, err := deployment.Smoke(ctx, role)
	if err != nil {
		return fmt.Errorf("executing %s dry-run lifecycle: %w", role, err)
	}
	return deployment.WriteSmokeEvidence(output, evidence)
}

func runSmokeRole(ctx context.Context, value string, output io.Writer) error {
	role, err := deployment.ParseRole(value)
	if err != nil {
		return err
	}
	cfg := syntheticRoleConfig(role)
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("validating synthetic %s configuration: %w", role, err)
	}
	if err := cfg.ValidateRole(ctx, string(role), func(ctx context.Context, reference string) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !strings.HasPrefix(reference, "SMOKE_") {
			return errors.New("non-synthetic secret reference")
		}
		return nil
	}); err != nil {
		return fmt.Errorf("validating synthetic %s role preconditions: %w", role, err)
	}
	runtime, err := composeRuntime(ctx, string(role), cfg, cmd.BuildInfo{Version: version, Commit: commit, Date: buildDate}, output)
	if err != nil {
		return fmt.Errorf("composing synthetic %s runtime: %w", role, err)
	}
	runErr := runRole(ctx, string(role), cfg, runtime, output)
	shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.Runtime.ShutdownTimeout)
	defer cancel()
	return errors.Join(runErr, runtime.Shutdown(shutdownCtx))
}

func syntheticRoleConfig(role deployment.Role) config.Config {
	return config.Config{
		Runtime: config.RuntimeConfig{
			ShutdownTimeout: time.Second, MaxConcurrency: 1,
			ClockProbeInterval: time.Second, SpoolMaxBytes: 1 << 20,
		},
		Capture: config.CaptureConfig{
			DecodeQueueCapacity: 16, DurableQueueCapacity: 16,
			DecodeHighWater: 12, DurableHighWater: 12,
			DecodeLowWater: 4, DurableLowWater: 4,
			MaxRawMessageBytes: 1 << 20, PendingRESTCapacity: 8,
		},
		Security: config.SecurityConfig{MinimumTLSVersion: "1.2", RedirectPolicy: "deny"},
		Deployment: config.DeploymentConfig{
			Role: string(role), DryRun: true,
			WriterLeaseKey: "synthetic/source/channel", WriterID: "synthetic-writer",
		},
		ObjectStore: config.ObjectStoreConfig{
			Endpoint: "https://objects.invalid", Region: "synthetic", Bucket: "synthetic",
			Prefix: "smoke", CredentialRef: "SMOKE_OBJECT_CREDENTIAL",
		},
		Catalog: config.CatalogConfig{DSNRef: "SMOKE_CATALOG_DSN", MinConns: 1, MaxConns: 1, ServerMajors: []int{17}},
		Warehouse: config.WarehouseConfig{
			DSNRef: "SMOKE_WAREHOUSE_DSN", Database: "synthetic",
			ServerDigest: "sha256:synthetic", BatchRows: 1,
		},
		Sources: []config.SourceConfig{{
			ID: "synthetic-binance-spot", API: "binance-spot",
			Endpoints: []string{"https://data-api.binance.vision", "wss://data-stream.binance.vision/ws"},
			Methods:   []string{config.MethodMarketDataHTTPGet, config.MethodMarketDataWebSocket},
			Channels:  []string{"trade"}, Families: []string{"trade"}, Symbols: []string{"BTCUSDT"},
		}},
		Quality: config.QualityConfig{
			AckTimeout: time.Second, HeartbeatTimeout: time.Second, SilenceTimeout: time.Second,
			SequencePolicy: "strict", SchemaPolicy: "quarantine",
		},
		Dataset: config.DatasetConfig{
			PartitionWindow: time.Hour, RowGroupBytes: 1, Compression: "zstd",
			OpportunityArchiveMaxRows: 1,
		},
		Serve: config.ServeConfig{
			ReadTimeout: time.Second, WriteTimeout: time.Second, IdleTimeout: time.Second,
			DefaultPageRows: 1, MaxPageRows: 1, MaxResponseBytes: 1,
		},
		Telemetry: config.TelemetryConfig{LogLevel: "error", MaxSeries: 10_000},
	}
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

func runVerifyVenue(ctx context.Context, venue string, cfg config.Config, runtime cmd.Runtime, output io.Writer) error {
	if output == nil {
		return errors.New("verify venue output is required")
	}
	if runtime == nil {
		return errors.New("verify venue runtime composition is required")
	}
	if cfg.Verify.Mode != config.VerifyModeFixture {
		return errors.New("live venue acquisition requires the collector role; verifier accepts immutable fixture evidence only")
	}
	roleRuntime, ok := runtime.(interface{ DeploymentRole() deployment.Role })
	if !ok || roleRuntime.DeploymentRole() != deployment.RoleVerifier {
		return errors.New("verify venue requires verifier-scoped runtime composition")
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
	case "binance-coinm":
		manifestPath, err := derivativeFixtureManifest(cfg)
		if err != nil {
			return err
		}
		evidence, err := binance.VerifyCoinMFixtures(manifestPath)
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
