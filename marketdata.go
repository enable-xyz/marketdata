package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/bybit"
	"github.com/enable-xyz/marketdata/catalog"
	"github.com/enable-xyz/marketdata/cmd"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/deployment"
	"github.com/enable-xyz/marketdata/deribit"
	"github.com/enable-xyz/marketdata/hyperliquid"
	"github.com/enable-xyz/marketdata/okx"
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
		LoadConfig:              config.Load,
		ValidateSecret:          validateEnvironmentSecret,
		Compose:                 composeCommandRuntime,
		Run:                     runRole,
		CheckPlatformCatalog:    runCheckPlatformCatalog,
		PlatformCatalogTemplate: writePlatformCatalogTemplate,
		VerifyVenue:             runVerifyVenue,
		VerifyReplay:            runVerifyReplay,
		VerifyCoverage:          runVerifyCoverage,
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

type platformCatalogTemplateDocument struct {
	SchemaVersion uint16                         `json:"schema_version"`
	Evidence      PlatformDeclarationEvidence    `json:"evidence"`
	Report        catalog.DeclarationCheckReport `json:"report"`
}

type platformCatalogDocument struct {
	SchemaVersion  uint16                         `json:"schema_version"`
	Declarations   []catalog.DeclaredSource       `json:"declarations"`
	Report         catalog.DeclarationCheckReport `json:"report"`
	EvidenceSHA256 string                         `json:"evidence_sha256"`
}

func writePlatformCatalogTemplate(ctx context.Context, adapterVersion string, startNS int64, endNS *int64, output io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output == nil {
		return errors.New("platform catalog template output is required")
	}
	evidence, err := NewPlatformDeclarationTemplate(adapterVersion, startNS, endNS)
	if err != nil {
		return err
	}
	_, report, err := BuildPlatformDeclarations(evidence)
	if err != nil {
		return err
	}
	return writeVenueEvidence(output, platformCatalogTemplateDocument{
		SchemaVersion: 1,
		Evidence:      evidence,
		Report:        report,
	})
}

func runCheckPlatformCatalog(ctx context.Context, evidencePath, expectedReportSHA256 string, output io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output == nil || evidencePath == "" || !validSHA256Hex(expectedReportSHA256) {
		return errors.New("platform catalog check requires evidence, expected report SHA-256, and output")
	}
	info, err := os.Lstat(evidencePath)
	if err != nil {
		return fmt.Errorf("inspect platform declaration evidence: %w", err)
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > 64<<20 {
		return errors.New("platform declaration evidence must be one bounded regular non-symlink file")
	}
	encoded, err := os.ReadFile(evidencePath)
	if err != nil {
		return fmt.Errorf("read platform declaration evidence: %w", err)
	}
	var template platformCatalogTemplateDocument
	if err := decodeStrictJSON(encoded, &template); err != nil {
		return fmt.Errorf("decode platform declaration evidence: %w", err)
	}
	if template.SchemaVersion != 1 {
		return fmt.Errorf("unsupported platform declaration evidence schema version %d", template.SchemaVersion)
	}
	declarations, report, err := BuildPlatformDeclarations(template.Evidence)
	if err != nil {
		return err
	}
	declaredReport, err := json.Marshal(template.Report)
	if err != nil {
		return fmt.Errorf("encode declared platform catalog report: %w", err)
	}
	recomputedReport, err := json.Marshal(report)
	if err != nil {
		return fmt.Errorf("encode recomputed platform catalog report: %w", err)
	}
	if !bytes.Equal(declaredReport, recomputedReport) {
		return errors.New("platform catalog template report does not match its declaration evidence")
	}
	if report.SHA256 != expectedReportSHA256 {
		return fmt.Errorf("platform catalog report SHA-256 = %s, expected %s", report.SHA256, expectedReportSHA256)
	}
	document := platformCatalogDocument{
		SchemaVersion: 1,
		Declarations:  declarations,
		Report:        report,
	}
	body, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("encode platform catalog evidence: %w", err)
	}
	digest := sha256.Sum256(body)
	document.EvidenceSHA256 = hex.EncodeToString(digest[:])
	return writeVenueEvidence(output, document)
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
	switch operation {
	case "verify venue", "verify replay", "verify coverage":
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if role != deployment.RoleVerifier {
			return nil, fmt.Errorf("%s resolved unexpected deployment role %q", operation, role)
		}
		return &verifierVenueRuntime{}, nil
	}
	return composeRuntime(ctx, operation, cfg, build, output)
}

func runRole(ctx context.Context, operation string, cfg config.Config, runtime cmd.Runtime, output io.Writer) error {
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
		return runProductionRole(ctx, operation, cfg, runtime, output)
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
			ClockProbeInterval: time.Second, SpoolMaxBytes: 16 << 20,
		},
		Capture: config.CaptureConfig{
			SpoolRoot: "/synthetic/spool", FrameBytes: 2 << 20,
			SegmentMaxBytes: 2 << 20, SegmentMaxAge: 2 * time.Second,
			DepthSnapshotLimit: 100, DepthSnapshotCadence: time.Second, ReconnectDelay: 100 * time.Millisecond,
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
			WorkingRoot: "/synthetic/dataset", BuildCadence: time.Second,
			PartitionWindow: time.Hour, RowGroupBytes: 64 << 20, Compression: "zstd",
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
	secret, err := (environmentSecretResolver{}).Resolve(ctx, reference)
	clear(secret)
	return err
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
		return writeScopedFixtureEvidence(output, venue, "binance", "usdm", evidence)
	case "binance-coinm":
		manifestPath, err := derivativeFixtureManifest(cfg)
		if err != nil {
			return err
		}
		evidence, err := binance.VerifyCoinMFixtures(manifestPath)
		if err != nil {
			return err
		}
		return writeScopedFixtureEvidence(output, venue, "binance", "coinm", evidence)
	case "bybit-spot", "bybit-linear", "bybit-inverse":
		manifestPath, err := derivativeFixtureManifest(cfg)
		if err != nil {
			return err
		}
		evidence, err := bybit.VerifyFixtures(manifestPath)
		if err != nil {
			return err
		}
		return writeScopedFixtureEvidence(output, venue, "bybit-v5", strings.TrimPrefix(venue, "bybit-"), evidence)
	case "bybit-option":
		manifestPath, err := derivativeFixtureManifest(cfg)
		if err != nil {
			return err
		}
		evidence, err := bybit.VerifyOptionFixtures(manifestPath)
		if err != nil {
			return err
		}
		return writeScopedFixtureEvidence(output, venue, "bybit-v5", "option", evidence)
	case "okx-v5-spot", "okx-v5-swap", "okx-v5-futures", "okx-v5-option":
		manifestPath, err := derivativeFixtureManifest(cfg)
		if err != nil {
			return err
		}
		evidence, err := okx.VerifyFixtures(filepath.Dir(manifestPath), filepath.Base(manifestPath))
		if err != nil {
			return err
		}
		return writeScopedFixtureEvidence(output, venue, "okx-v5", strings.TrimPrefix(venue, "okx-v5-"), evidence)
	case "deribit-v2":
		manifestPath, err := derivativeFixtureManifest(cfg)
		if err != nil {
			return err
		}
		evidence, err := deribit.VerifyFixtures(manifestPath)
		if err != nil {
			return err
		}
		return writeScopedFixtureEvidence(output, venue, "deribit", "derivatives", evidence)
	case "hyperliquid-main", "hyperliquid-hip3":
		manifestPath, err := derivativeFixtureManifest(cfg)
		if err != nil {
			return err
		}
		evidence, err := hyperliquid.VerifyFixtures(manifestPath)
		if err != nil {
			return err
		}
		product := "main-perpetual"
		if venue == "hyperliquid-hip3" {
			product = "hip3-perpetual"
		}
		return writeScopedFixtureEvidence(output, venue, "hyperliquid", product, evidence)
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
	return writeScopedFixtureEvidence(output, venue, "binance", "spot", json.RawMessage(bytes.TrimSpace(encoded)))
}

type exactVenueVerifier func(context.Context, string, config.Config, cmd.Runtime, io.Writer) error

type replayVerificationEvidence struct {
	SchemaVersion   uint16 `json:"schema_version"`
	Selector        string `json:"selector"`
	ProofScope      string `json:"proof_scope"`
	FirstRunSHA256  string `json:"first_run_sha256"`
	SecondRunSHA256 string `json:"second_run_sha256"`
	Deterministic   bool   `json:"deterministic"`
	EvidenceSHA256  string `json:"evidence_sha256"`
}

func runVerifyReplay(ctx context.Context, venue string, cfg config.Config, runtime cmd.Runtime, output io.Writer) error {
	return runVerifyReplayWith(ctx, venue, cfg, runtime, output, runVerifyVenue)
}

func runVerifyReplayWith(ctx context.Context, venue string, cfg config.Config, runtime cmd.Runtime, output io.Writer, verifyVenue exactVenueVerifier) error {
	if output == nil {
		return errors.New("verify replay output is required")
	}
	if verifyVenue == nil {
		return errors.New("verify replay venue callback is required")
	}
	var first, second bytes.Buffer
	if err := verifyVenue(ctx, venue, cfg, runtime, &first); err != nil {
		return fmt.Errorf("first exact venue verification: %w", err)
	}
	if err := verifyVenue(ctx, venue, cfg, runtime, &second); err != nil {
		return fmt.Errorf("second exact venue verification: %w", err)
	}
	firstBytes := first.Bytes()
	secondBytes := second.Bytes()
	firstDigest := sha256.Sum256(firstBytes)
	secondDigest := sha256.Sum256(secondBytes)
	if !bytes.Equal(firstBytes, secondBytes) {
		return fmt.Errorf(
			"exact venue verification is not deterministic: first sha256 %s, second sha256 %s",
			hex.EncodeToString(firstDigest[:]), hex.EncodeToString(secondDigest[:]),
		)
	}
	evidence := replayVerificationEvidence{
		SchemaVersion:   1,
		Selector:        venue,
		ProofScope:      "exact_venue_evidence_replay",
		FirstRunSHA256:  hex.EncodeToString(firstDigest[:]),
		SecondRunSHA256: hex.EncodeToString(secondDigest[:]),
		Deterministic:   true,
	}
	return writeReplayVerificationEvidence(output, evidence)
}

func writeReplayVerificationEvidence(output io.Writer, evidence replayVerificationEvidence) error {
	material, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(material)
	evidence.EvidenceSHA256 = hex.EncodeToString(digest[:])
	return writeVenueEvidence(output, evidence)
}

type coverageVerificationEvidence struct {
	SchemaVersion       uint16 `json:"schema_version"`
	Selector            string `json:"selector"`
	ProofScope          string `json:"proof_scope"`
	ManifestCoverage    bool   `json:"manifest_coverage"`
	RoleCoverage        bool   `json:"role_coverage"`
	ExactEvidenceSHA256 string `json:"exact_evidence_sha256"`
	EnvelopeSHA256      string `json:"envelope_sha256"`
	EvidenceSHA256      string `json:"evidence_sha256"`
}

type scopedFixtureEnvelope struct {
	SchemaVersion  uint16          `json:"schema_version"`
	Selector       string          `json:"selector"`
	Venue          string          `json:"venue"`
	Product        string          `json:"product"`
	ProofScope     string          `json:"proof_scope"`
	Evidence       json.RawMessage `json:"evidence"`
	EvidenceSHA256 string          `json:"evidence_sha256"`
}

func runVerifyCoverage(ctx context.Context, venue string, cfg config.Config, runtime cmd.Runtime, output io.Writer) error {
	return runVerifyCoverageWith(ctx, venue, cfg, runtime, output, runVerifyVenue)
}

func runVerifyCoverageWith(ctx context.Context, venue string, cfg config.Config, runtime cmd.Runtime, output io.Writer, verifyVenue exactVenueVerifier) error {
	if output == nil {
		return errors.New("verify coverage output is required")
	}
	if verifyVenue == nil {
		return errors.New("verify coverage venue callback is required")
	}
	var source bytes.Buffer
	if err := verifyVenue(ctx, venue, cfg, runtime, &source); err != nil {
		return fmt.Errorf("exact venue verification: %w", err)
	}
	envelopeDigest, err := validateCoverageSource(venue, source.Bytes())
	if err != nil {
		return err
	}
	exactDigest := sha256.Sum256(source.Bytes())
	evidence := coverageVerificationEvidence{
		SchemaVersion:       1,
		Selector:            venue,
		ProofScope:          "offline_fixture_manifest_role_coverage",
		ManifestCoverage:    true,
		RoleCoverage:        true,
		ExactEvidenceSHA256: hex.EncodeToString(exactDigest[:]),
		EnvelopeSHA256:      envelopeDigest,
	}
	material, err := json.Marshal(evidence)
	if err != nil {
		return err
	}
	selfDigest := sha256.Sum256(material)
	evidence.EvidenceSHA256 = hex.EncodeToString(selfDigest[:])
	return writeVenueEvidence(output, evidence)
}

func validateCoverageSource(selector string, encoded []byte) (string, error) {
	var envelope scopedFixtureEnvelope
	envelopeErr := decodeStrictJSON(encoded, &envelope)
	if envelopeErr == nil {
		if envelope.SchemaVersion != 1 || envelope.Selector != selector || envelope.Venue == "" || envelope.Product == "" ||
			envelope.ProofScope != "offline_repository_fixture" || len(envelope.Evidence) == 0 {
			return "", errors.New("verify coverage scoped evidence identity is invalid")
		}
		claimed := envelope.EvidenceSHA256
		if !validSHA256Hex(claimed) {
			return "", errors.New("verify coverage scoped evidence hash is invalid")
		}
		envelope.EvidenceSHA256 = ""
		material, err := json.Marshal(envelope)
		if err != nil {
			return "", err
		}
		digest := sha256.Sum256(material)
		if hex.EncodeToString(digest[:]) != claimed {
			return "", errors.New("verify coverage scoped evidence hash mismatch")
		}
		if err := validateManifestRoleEvidence(selector, envelope.Evidence); err != nil {
			return "", err
		}
		return claimed, nil
	}
	if selector != "bybit-v5" {
		return "", fmt.Errorf("verify coverage requires a strict scoped evidence envelope: %w", envelopeErr)
	}
	var legacy bybit.EvidenceSummary
	if err := decodeStrictJSON(encoded, &legacy); err != nil {
		return "", fmt.Errorf("verify coverage requires scoped evidence or the exact bybit-v5 legacy packet: %w", err)
	}
	if err := validateManifestRoleEvidence(selector, encoded); err != nil {
		return "", err
	}
	claimed := legacy.EvidenceSHA256
	legacy.EvidenceSHA256 = ""
	material, err := json.Marshal(legacy)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(material)
	if hex.EncodeToString(digest[:]) != claimed {
		return "", errors.New("verify coverage bybit-v5 legacy evidence hash mismatch")
	}
	return claimed, nil
}

func decodeStrictJSON(encoded []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

type coverageRoleCheck struct {
	name       string
	fixtureIDs []string
}

var (
	binanceSpotCoverageComponents = []string{
		"capture", "catalog", "native-replay", "normalization",
		"object-publication", "order-book", "parquet", "verifier",
	}
	binanceUSDMCoverageFixtureIDs = []string{
		"official.agg_trade", "official.book_ticker", "official.depth_snapshot", "official.depth_update",
		"official.exchange_info", "official.index_price", "official.liquidation", "official.mark_price",
		"official.open_interest", "official.routing", "official.ticker", "synthetic.liquidation_completeness",
		"synthetic.open_interest_wrong_symbol", "synthetic.pu_gap", "synthetic.q_nq_direction",
		"synthetic.rpi_candidate", "synthetic.wrong_route",
	}
	binanceCoinMCoverageFixtureIDs = []string{
		"official.agg_trade", "official.book_ticker", "official.delivery_funding_empty",
		"official.depth_snapshot", "official.depth_update", "official.exchange_info",
		"official.merged_inconsistency", "official.merged_routing", "official.open_interest",
		"official.ticker", "synthetic.contract_size_change", "synthetic.delivery_funding_zero",
		"synthetic.payoff_mismatch",
	}
	bybitCoverageChecks = map[string][]string{
		"distinct_spot_linear_inverse_sources_and_sockets":      {},
		"public_trade_contract":                                 {"trade"},
		"bounded_and_full_book_non_interchangeability":          {"bounded-gap", "bounded-snapshot", "full-delta", "full-gap", "full-next"},
		"regular_and_rpi_roles_separate":                        {"rpi-delta", "rpi-snapshot"},
		"official_sparse_ticker_seed_empty_and_reconnect_reset": {"ticker-official", "ticker-oi", "ticker-sparse"},
		"both_and_single_side_open_interest_distinct":           {"ticker-oi"},
		"derivatives_only_liquidation_object_array_drift":       {"liquidation-array", "liquidation-object"},
		"instrument_metadata":                                   {"instrument-linear"},
	}
	bybitOptionCoverageChecks = map[string][]string{
		"option_trade_strict_context":                         {"option-trade"},
		"option_bounded_book_minimum_depth":                   {"option-book-25"},
		"option_summary_snapshot_greeks_metadata_and_age":     {"option-metadata", "option-ticker"},
		"option_base_coin_subscription_identity":              {"base-coin-topics"},
		"option_documented_minimum_depth":                     {"minimum-depth"},
		"option_ticker_snapshot_only":                         {"ticker-delta"},
		"option_greek_type_drift_rejected":                    {"greek-type-drift"},
		"option_explicit_unsupported_l1_full_rpi_liquidation": {"unsupported-roles"},
	}
	okxCoverageRoles = []string{
		"book_checksum_post", "book_checksum_pre", "book_no_change", "book_reset", "book_snapshot",
		"books5", "lifecycle_mapping", "liquidation_mapping", "market_mapping", "option_mapping",
		"source_contract", "trades_all", "vip_denial",
	}
	deribitCoverageRoles = []string{
		"book_gap", "book_recovery", "book_snapshot", "credit_exhausted", "funding",
		"heartbeat_test_request", "index", "instrument_inverse", "instrument_linear",
		"instrument_option", "lifecycle_state", "quote", "subscribe_partial", "ticker_linear",
		"ticker_option", "trade_liquidation",
	}
	hyperliquidCoverageChecks = map[string][]string{
		"bbo_context_funding_source_fidelity":        {"active-asset-context", "bbo", "funding-history"},
		"book_depth_full_replacement_no_sequence":    {"fast-book", "slow-book-initial", "slow-book-replacement"},
		"duplicate_trade_key_preserved":              {"duplicate-trades"},
		"metadata_namespace_generation_and_position": {"hip3-meta-contexts", "hip3-positional-mismatch", "main-meta-contexts", "perp-dexs", "spot-meta-contexts"},
		"no_import_and_provisional_exclusion":        {"hip3-meta-contexts", "main-meta-contexts"},
	}
)

func validateManifestRoleEvidence(selector string, encoded []byte) error {
	switch selector {
	case "binance-spot":
		var evidence verify.Evidence
		if err := decodeStrictJSON(encoded, &evidence); err != nil {
			return malformedCoverageEvidence(selector, err)
		}
		components := make([]string, len(evidence.Components))
		for index, component := range evidence.Components {
			components[index] = component.Name
		}
		if evidence.SchemaVersion != verify.EvidenceSchemaVersion || evidence.Venue != selector ||
			!validSHA256Hex(evidence.Hashes.FixtureManifestSHA256) ||
			!exactRoleInventory(components, binanceSpotCoverageComponents) {
			return incompleteCoverageEvidence(selector)
		}
	case "binance-usdm":
		var evidence binance.USDMFixtureEvidence
		if err := decodeStrictJSON(encoded, &evidence); err != nil {
			return malformedCoverageEvidence(selector, err)
		}
		fixtures := make([]string, len(evidence.Fixtures))
		for index, fixture := range evidence.Fixtures {
			fixtures[index] = fixture.ID
		}
		if evidence.Version != binance.USDMFixtureManifestVersion ||
			evidence.ManifestSHA256 == ([sha256.Size]byte{}) ||
			!exactRoleInventory(fixtures, binanceUSDMCoverageFixtureIDs) {
			return incompleteCoverageEvidence(selector)
		}
	case "binance-coinm":
		var evidence binance.CoinMFixtureEvidence
		if err := decodeStrictJSON(encoded, &evidence); err != nil {
			return malformedCoverageEvidence(selector, err)
		}
		fixtures := make([]string, len(evidence.Fixtures))
		for index, fixture := range evidence.Fixtures {
			fixtures[index] = fixture.ID
		}
		if evidence.Version != binance.CoinMFixtureManifestVersion ||
			evidence.ManifestSHA256 == ([sha256.Size]byte{}) ||
			!exactRoleInventory(fixtures, binanceCoinMCoverageFixtureIDs) {
			return incompleteCoverageEvidence(selector)
		}
	case "bybit-spot", "bybit-linear", "bybit-inverse", "bybit-v5":
		var evidence bybit.EvidenceSummary
		if err := decodeStrictJSON(encoded, &evidence); err != nil {
			return malformedCoverageEvidence(selector, err)
		}
		checks := make([]coverageRoleCheck, len(evidence.Checks))
		for index, check := range evidence.Checks {
			checks[index] = coverageRoleCheck{name: check.Name, fixtureIDs: check.FixtureIDs}
		}
		if evidence.Version != bybit.EvidenceVersion || evidence.Venue != "bybit-v5" ||
			evidence.FixtureCount != 14 || !validSHA256Hex(evidence.ManifestSHA256) ||
			!exactCheckInventory(checks, bybitCoverageChecks) {
			return incompleteCoverageEvidence(selector)
		}
	case "bybit-option":
		var evidence bybit.OptionEvidenceSummary
		if err := decodeStrictJSON(encoded, &evidence); err != nil {
			return malformedCoverageEvidence(selector, err)
		}
		checks := make([]coverageRoleCheck, len(evidence.Checks))
		for index, check := range evidence.Checks {
			checks[index] = coverageRoleCheck{name: check.Name, fixtureIDs: check.FixtureIDs}
		}
		if evidence.Version != bybit.OptionEvidenceVersion || evidence.Venue != "bybit-v5-option" ||
			evidence.FixtureCount != 9 || !validSHA256Hex(evidence.ManifestSHA256) ||
			!exactCheckInventory(checks, bybitOptionCoverageChecks) {
			return incompleteCoverageEvidence(selector)
		}
	case "okx-v5-spot", "okx-v5-swap", "okx-v5-futures", "okx-v5-option":
		var evidence okx.FixtureSummary
		if err := decodeStrictJSON(encoded, &evidence); err != nil {
			return malformedCoverageEvidence(selector, err)
		}
		roles := make([]string, len(evidence.Fixtures))
		for index, fixture := range evidence.Fixtures {
			if fixture.ID != "okx-"+fixture.Role {
				return incompleteCoverageEvidence(selector)
			}
			roles[index] = fixture.Role
		}
		if evidence.Version != okx.VenueEvidenceVersion || evidence.Venue != "okx-v5" ||
			evidence.EvidenceScope != "offline_repository_fixture" ||
			!validSHA256Hex(evidence.ManifestSHA256) ||
			!exactRoleInventory(roles, okxCoverageRoles) {
			return incompleteCoverageEvidence(selector)
		}
	case "deribit-v2":
		var evidence deribit.EvidenceSummary
		if err := decodeStrictJSON(encoded, &evidence); err != nil {
			return malformedCoverageEvidence(selector, err)
		}
		if evidence.Version != deribit.FixtureManifestVersion || evidence.Venue != "deribit" ||
			evidence.FixtureCount != uint32(len(deribitCoverageRoles)) ||
			!validSHA256Hex(evidence.ManifestSHA256) ||
			!exactRoleInventory(evidence.Roles, deribitCoverageRoles) {
			return incompleteCoverageEvidence(selector)
		}
	case "hyperliquid-main", "hyperliquid-hip3":
		var evidence hyperliquid.EvidenceSummary
		if err := decodeStrictJSON(encoded, &evidence); err != nil {
			return malformedCoverageEvidence(selector, err)
		}
		checks := make([]coverageRoleCheck, len(evidence.Checks))
		for index, check := range evidence.Checks {
			checks[index] = coverageRoleCheck{name: check.Name, fixtureIDs: check.FixtureIDs}
		}
		if evidence.Version != hyperliquid.EvidenceVersion || evidence.Venue != "hyperliquid" ||
			evidence.FixtureCount != 12 || !validSHA256Hex(evidence.ManifestSHA256) ||
			!exactCheckInventory(checks, hyperliquidCoverageChecks) {
			return incompleteCoverageEvidence(selector)
		}
	default:
		return fmt.Errorf("verify coverage selector %q has no required role inventory", selector)
	}
	return nil
}

func exactCheckInventory(observed []coverageRoleCheck, required map[string][]string) bool {
	if len(observed) != len(required) {
		return false
	}
	seen := make(map[string]struct{}, len(observed))
	for _, check := range observed {
		requiredFixtures, ok := required[check.name]
		if !ok {
			return false
		}
		if _, duplicate := seen[check.name]; duplicate {
			return false
		}
		seen[check.name] = struct{}{}
		if !exactRoleInventory(check.fixtureIDs, requiredFixtures) {
			return false
		}
	}
	return true
}

func exactRoleInventory(observed, required []string) bool {
	if len(observed) != len(required) {
		return false
	}
	observed = slices.Clone(observed)
	required = slices.Clone(required)
	slices.Sort(observed)
	slices.Sort(required)
	for index, role := range observed {
		if role == "" || index > 0 && role == observed[index-1] || role != required[index] {
			return false
		}
	}
	return true
}

func malformedCoverageEvidence(selector string, err error) error {
	return fmt.Errorf("verify coverage %s verifier evidence is malformed: %w", selector, err)
}

func incompleteCoverageEvidence(selector string) error {
	return fmt.Errorf("verify coverage %s verifier manifest or required role inventory is incomplete", selector)
}

func validSHA256Hex(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

type scopedFixtureEvidence struct {
	SchemaVersion  uint16 `json:"schema_version"`
	Selector       string `json:"selector"`
	Venue          string `json:"venue"`
	Product        string `json:"product"`
	ProofScope     string `json:"proof_scope"`
	Evidence       any    `json:"evidence"`
	EvidenceSHA256 string `json:"evidence_sha256"`
}

func writeScopedFixtureEvidence(output io.Writer, selector, venue, product string, evidence any) error {
	if selector == "" || venue == "" || product == "" || evidence == nil {
		return errors.New("scoped fixture evidence identity is incomplete")
	}
	packet := scopedFixtureEvidence{
		SchemaVersion: 1,
		Selector:      selector,
		Venue:         venue,
		Product:       product,
		ProofScope:    "offline_repository_fixture",
		Evidence:      evidence,
	}
	material, err := json.Marshal(packet)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(material)
	packet.EvidenceSHA256 = hex.EncodeToString(digest[:])
	return writeVenueEvidence(output, packet)
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
