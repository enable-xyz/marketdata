// Package config loads and validates explicit, typed runtime configuration.
package config

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/enable-xyz/marketdata/deployment"
	"github.com/go-viper/mapstructure/v2"
	"github.com/spf13/viper"
)

const envPrefix = "ENABLE_MARKET_"

var ErrPathRequired = errors.New("an explicit YAML configuration path is required")

// Overrides contains explicitly set command-line values keyed by configuration path.
type Overrides map[string]any

// Config is one immutable runtime configuration value. Callers pass it by value
// and do not retain the decoder used to construct it.
type Config struct {
	Runtime     RuntimeConfig     `mapstructure:"runtime"`
	Capture     CaptureConfig     `mapstructure:"capture"`
	ObjectStore ObjectStoreConfig `mapstructure:"object_store"`
	Catalog     CatalogConfig     `mapstructure:"catalog"`
	Warehouse   WarehouseConfig   `mapstructure:"warehouse"`
	Sources     []SourceConfig    `mapstructure:"sources"`
	Quality     QualityConfig     `mapstructure:"quality"`
	Dataset     DatasetConfig     `mapstructure:"dataset"`
	Serve       ServeConfig       `mapstructure:"serve"`
	Telemetry   TelemetryConfig   `mapstructure:"telemetry"`
	Security    SecurityConfig    `mapstructure:"security"`
	Deployment  DeploymentConfig  `mapstructure:"deployment"`

	Verify VerifyConfig `mapstructure:"verify"`
}

type RuntimeConfig struct {
	ShutdownTimeout    time.Duration `mapstructure:"shutdown_timeout"`
	MaxConcurrency     int           `mapstructure:"max_concurrency"`
	ClockProbeInterval time.Duration `mapstructure:"clock_probe_interval"`
	SpoolMaxBytes      int64         `mapstructure:"spool_max_bytes"`
}

type SecurityConfig struct {
	MinimumTLSVersion string `mapstructure:"minimum_tls_version"`
	RedirectPolicy    string `mapstructure:"redirect_policy"`
}

type DeploymentConfig struct {
	Role           string `mapstructure:"role"`
	DryRun         bool   `mapstructure:"dry_run"`
	WriterLeaseKey string `mapstructure:"writer_lease_key"`
	WriterID       string `mapstructure:"writer_id"`
}

type CaptureConfig struct {
	DecodeQueueCapacity  int `mapstructure:"decode_queue_capacity"`
	DurableQueueCapacity int `mapstructure:"durable_queue_capacity"`
	DecodeHighWater      int `mapstructure:"decode_high_water"`
	DurableHighWater     int `mapstructure:"durable_high_water"`
	DecodeLowWater       int `mapstructure:"decode_low_water"`
	DurableLowWater      int `mapstructure:"durable_low_water"`
	MaxRawMessageBytes   int `mapstructure:"max_raw_message_bytes"`
	PendingRESTCapacity  int `mapstructure:"pending_rest_capacity"`
}

const (
	VerifyModeFixture = "fixture"
	VerifyModeLive    = "live"

	VerifyMaximumSymbols  = 4
	VerifyMinimumMessages = 16
	VerifyMinimumBytes    = int64(64 << 10)
	VerifyMinimumDuration = time.Second
	VerifyMaximumMessages = 10_000
	VerifyMaximumBytes    = int64(64 << 20)
	VerifyMaximumDuration = 5 * time.Minute
	VerifyMaximumDepth    = 100
)

type VerifyConfig struct {
	Mode            string        `mapstructure:"mode"`
	FixtureRoot     string        `mapstructure:"fixture_root"`
	FixtureManifest string        `mapstructure:"fixture_manifest"`
	SpoolRoot       string        `mapstructure:"spool_root"`
	ArtifactRoot    string        `mapstructure:"artifact_root"`
	MaxMessages     int           `mapstructure:"max_messages"`
	MaxBytes        int64         `mapstructure:"max_bytes"`
	MaxDuration     time.Duration `mapstructure:"max_duration"`
	DepthLimit      int           `mapstructure:"depth_limit"`
}

type ObjectStoreConfig struct {
	Endpoint      string `mapstructure:"endpoint"`
	Region        string `mapstructure:"region"`
	Bucket        string `mapstructure:"bucket"`
	Prefix        string `mapstructure:"prefix"`
	PathStyle     bool   `mapstructure:"path_style"`
	CredentialRef string `mapstructure:"credential_ref"`
}

type CatalogConfig struct {
	DSNRef       string             `mapstructure:"dsn_ref"`
	MinConns     int                `mapstructure:"min_conns"`
	MaxConns     int                `mapstructure:"max_conns"`
	ServerMajors []int              `mapstructure:"server_majors"`
	Check        CatalogCheckConfig `mapstructure:"check"`
}

type CatalogCheckConfig struct {
	FixtureManifest              string   `mapstructure:"fixture_manifest"`
	FixtureNames                 []string `mapstructure:"fixture_names"`
	ExpectedSnapshotSHA256       string   `mapstructure:"expected_snapshot_sha256"`
	PlatformEvidence             string   `mapstructure:"platform_evidence"`
	ExpectedPlatformReportSHA256 string   `mapstructure:"expected_platform_report_sha256"`
}

type WarehouseConfig struct {
	DSNRef       string `mapstructure:"dsn_ref"`
	Database     string `mapstructure:"database"`
	ServerDigest string `mapstructure:"server_digest"`
	BatchRows    int    `mapstructure:"batch_rows"`
}

type SourceConfig struct {
	ID               string        `mapstructure:"id"`
	API              string        `mapstructure:"api"`
	Endpoints        []string      `mapstructure:"endpoints"`
	Methods          []string      `mapstructure:"methods"`
	EntitlementRef   string        `mapstructure:"entitlement_ref"`
	EntitlementScope string        `mapstructure:"entitlement_scope"`
	Channels         []string      `mapstructure:"channels"`
	Families         []string      `mapstructure:"families"`
	Symbols          []string      `mapstructure:"symbols"`
	Cadence          time.Duration `mapstructure:"cadence"`
}

type QualityConfig struct {
	AckTimeout          time.Duration       `mapstructure:"ack_timeout"`
	HeartbeatTimeout    time.Duration       `mapstructure:"heartbeat_timeout"`
	SilenceTimeout      time.Duration       `mapstructure:"silence_timeout"`
	SequencePolicy      string              `mapstructure:"sequence_policy"`
	SchemaPolicy        string              `mapstructure:"schema_policy"`
	OpportunityPolicies []OpportunityPolicy `mapstructure:"opportunity_policies"`
}

type OpportunityPolicy struct {
	SourceID       string        `mapstructure:"source_id"`
	ChannelID      string        `mapstructure:"channel_id"`
	HotRetention   time.Duration `mapstructure:"hot_retention"`
	SpillBatchRows int           `mapstructure:"spill_batch_rows"`
}

type DatasetConfig struct {
	PartitionWindow           time.Duration `mapstructure:"partition_window"`
	RowGroupBytes             int64         `mapstructure:"row_group_bytes"`
	Compression               string        `mapstructure:"compression"`
	DerivedRetention          time.Duration `mapstructure:"derived_retention"`
	OpportunityArchiveMaxRows int           `mapstructure:"opportunity_archive_max_rows"`
}

type ServeConfig struct {
	Listener         string            `mapstructure:"listener"`
	TLSCertRef       string            `mapstructure:"tls_cert_ref"`
	TLSKeyRef        string            `mapstructure:"tls_key_ref"`
	BearerTokenRefs  map[string]string `mapstructure:"bearer_token_refs"`
	ReadTimeout      time.Duration     `mapstructure:"read_timeout"`
	WriteTimeout     time.Duration     `mapstructure:"write_timeout"`
	IdleTimeout      time.Duration     `mapstructure:"idle_timeout"`
	DefaultPageRows  int               `mapstructure:"default_page_rows"`
	MaxPageRows      int               `mapstructure:"max_page_rows"`
	MaxResponseBytes int64             `mapstructure:"max_response_bytes"`
}

type TelemetryConfig struct {
	LogLevel           string        `mapstructure:"log_level"`
	TraceExporterRef   string        `mapstructure:"trace_exporter_ref"`
	TraceQueueCapacity int           `mapstructure:"trace_queue_capacity"`
	TraceBatchSpans    int           `mapstructure:"trace_batch_spans"`
	TraceExportTimeout time.Duration `mapstructure:"trace_export_timeout"`
	MaxSeries          int           `mapstructure:"max_series"`
}

var defaults = map[string]any{
	"runtime.shutdown_timeout":             "15s",
	"runtime.max_concurrency":              8,
	"runtime.clock_probe_interval":         "30s",
	"runtime.spool_max_bytes":              int64(1 << 30),
	"security.minimum_tls_version":         "1.2",
	"security.redirect_policy":             "deny",
	"verify.max_messages":                  64,
	"verify.max_bytes":                     int64(4 << 20),
	"verify.max_duration":                  "10s",
	"verify.depth_limit":                   100,
	"catalog.min_conns":                    1,
	"catalog.max_conns":                    8,
	"warehouse.batch_rows":                 10_000,
	"quality.ack_timeout":                  "5s",
	"quality.heartbeat_timeout":            "30s",
	"quality.silence_timeout":              "60s",
	"quality.sequence_policy":              "strict",
	"quality.schema_policy":                "quarantine",
	"dataset.partition_window":             "1h",
	"dataset.row_group_bytes":              int64(256 << 20),
	"dataset.compression":                  "zstd",
	"dataset.derived_retention":            time.Duration(0),
	"dataset.opportunity_archive_max_rows": 1_000_000,
	"serve.read_timeout":                   "10s",
	"serve.write_timeout":                  "30s",
	"serve.idle_timeout":                   "120s",
	"serve.default_page_rows":              1_000,
	"serve.max_page_rows":                  10_000,
	"serve.max_response_bytes":             int64(16 << 20),
	"telemetry.log_level":                  "info",
	"telemetry.max_series":                 100_000,
}

var registeredKeys = []string{
	"runtime.shutdown_timeout", "runtime.max_concurrency", "runtime.clock_probe_interval", "runtime.spool_max_bytes",
	"capture.decode_queue_capacity", "capture.durable_queue_capacity", "capture.decode_high_water", "capture.durable_high_water",
	"capture.decode_low_water", "capture.durable_low_water", "capture.max_raw_message_bytes", "capture.pending_rest_capacity",
	"security.minimum_tls_version", "security.redirect_policy",
	"deployment.role", "deployment.dry_run", "deployment.writer_lease_key", "deployment.writer_id",
	"object_store.endpoint", "object_store.region", "object_store.bucket", "object_store.prefix", "object_store.path_style", "object_store.credential_ref",
	"verify.mode", "verify.fixture_root", "verify.fixture_manifest", "verify.spool_root", "verify.artifact_root",
	"verify.max_messages", "verify.max_bytes", "verify.max_duration", "verify.depth_limit",
	"catalog.dsn_ref", "catalog.min_conns", "catalog.max_conns", "catalog.server_majors",
	"catalog.check.fixture_manifest", "catalog.check.fixture_names", "catalog.check.expected_snapshot_sha256",
	"warehouse.dsn_ref", "warehouse.database", "warehouse.server_digest", "warehouse.batch_rows",
	"sources",
	"quality.ack_timeout", "quality.heartbeat_timeout", "quality.silence_timeout", "quality.sequence_policy", "quality.schema_policy", "quality.opportunity_policies",
	"dataset.partition_window", "dataset.row_group_bytes", "dataset.compression", "dataset.derived_retention", "dataset.opportunity_archive_max_rows",
	"serve.listener", "serve.tls_cert_ref", "serve.tls_key_ref", "serve.bearer_token_refs", "serve.read_timeout", "serve.write_timeout", "serve.idle_timeout", "serve.default_page_rows", "serve.max_page_rows", "serve.max_response_bytes",
	"telemetry.log_level", "telemetry.trace_exporter_ref", "telemetry.trace_queue_capacity", "telemetry.trace_batch_spans", "telemetry.trace_export_timeout", "telemetry.max_series",
}

// Load constructs a fresh Viper instance, reads only the named YAML file, applies
// explicitly bound environment variables and flag overrides, and strictly decodes.
func Load(path string, overrides Overrides) (Config, error) {
	if path == "" {
		return Config{}, ErrPathRequired
	}
	ext := strings.ToLower(filepath.Ext(path))
	if ext != ".yaml" && ext != ".yml" {
		return Config{}, fmt.Errorf("configuration file must be YAML: %q", ext)
	}

	v := viper.New()
	v.SetConfigFile(path)
	v.SetConfigType("yaml")
	v.AllowEmptyEnv(false)

	allowed := make(map[string]struct{}, len(registeredKeys))
	for _, key := range registeredKeys {
		allowed[key] = struct{}{}
		if err := v.BindEnv(key, envName(key)); err != nil {
			return Config{}, fmt.Errorf("binding environment for %s: %w", key, err)
		}
	}
	for key, value := range defaults {
		v.SetDefault(key, value)
	}
	for key, value := range overrides {
		if _, ok := allowed[key]; !ok {
			return Config{}, fmt.Errorf("unknown flag configuration key %q", key)
		}
		v.Set(key, value)
	}
	if err := v.ReadInConfig(); err != nil {
		return Config{}, fmt.Errorf("reading configuration: %w", err)
	}
	if v.GetString("verify.mode") == VerifyModeLive {
		for _, key := range []string{"verify.max_messages", "verify.max_bytes", "verify.max_duration", "verify.depth_limit"} {
			_, overridden := overrides[key]
			environmentValue, environmentBound := os.LookupEnv(envName(key))
			if !v.InConfig(key) && !overridden && (!environmentBound || environmentValue == "") {
				return Config{}, fmt.Errorf("%s must be explicitly declared for live verification", key)
			}
		}
	}

	var cfg Config
	if err := v.UnmarshalExact(&cfg, viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
		mapstructure.StringToTimeDurationHookFunc(),
	))); err != nil {
		return Config{}, fmt.Errorf("decoding configuration strictly: %w", err)
	}
	configDir, err := filepath.Abs(filepath.Dir(path))
	if err != nil {
		return Config{}, fmt.Errorf("resolving configuration directory: %w", err)
	}
	cfg = cfg.ResolvePaths(configDir)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// ResolvePaths binds configuration-relative verification paths to the exact
// named YAML directory. It performs no I/O and does not search ambient paths.
func (c Config) ResolvePaths(configDir string) Config {
	resolve := func(value string) string {
		if value == "" {
			return ""
		}
		if filepath.IsAbs(value) {
			return filepath.Clean(value)
		}
		return filepath.Clean(filepath.Join(configDir, value))
	}
	c.Verify.FixtureRoot = resolve(c.Verify.FixtureRoot)
	c.Verify.FixtureManifest = resolve(c.Verify.FixtureManifest)
	c.Verify.SpoolRoot = resolve(c.Verify.SpoolRoot)
	c.Verify.ArtifactRoot = resolve(c.Verify.ArtifactRoot)
	return c
}

// PublicDigest hashes the exact typed configuration after replacing every
// secret reference with a configured/not-configured marker. It never returns
// serialized configuration or caller-owned reference names.
func (c Config) PublicDigest() ([sha256.Size]byte, error) {
	redacted := c
	marker := func(reference string) string {
		if reference == "" {
			return ""
		}
		return "configured"
	}
	redacted.ObjectStore.CredentialRef = marker(c.ObjectStore.CredentialRef)
	redacted.Catalog.DSNRef = marker(c.Catalog.DSNRef)
	redacted.Warehouse.DSNRef = marker(c.Warehouse.DSNRef)
	redacted.Sources = slices.Clone(c.Sources)
	for index := range redacted.Sources {
		redacted.Sources[index].EntitlementRef = marker(c.Sources[index].EntitlementRef)
	}
	redacted.Serve.TLSCertRef = marker(c.Serve.TLSCertRef)
	redacted.Serve.TLSKeyRef = marker(c.Serve.TLSKeyRef)
	redacted.Serve.BearerTokenRefs = make(map[string]string, len(c.Serve.BearerTokenRefs))
	for scope, reference := range c.Serve.BearerTokenRefs {
		redacted.Serve.BearerTokenRefs[scope] = marker(reference)
	}
	redacted.Telemetry.TraceExporterRef = marker(c.Telemetry.TraceExporterRef)
	encoded, err := json.Marshal(redacted)
	if err != nil {
		return [sha256.Size]byte{}, errors.New("encoding public configuration digest failed")
	}
	return sha256.Sum256(encoded), nil
}

func envName(key string) string {
	r := strings.NewReplacer(".", "_", "-", "_")
	return envPrefix + strings.ToUpper(r.Replace(key))
}

// Validate checks structural invariants without resolving secrets or performing I/O.
func (c Config) Validate() error {
	if c.Runtime.ShutdownTimeout <= 0 {
		return errors.New("runtime.shutdown_timeout must be positive")
	}
	if c.Runtime.MaxConcurrency < 1 || c.Runtime.MaxConcurrency > 1024 {
		return errors.New("runtime.max_concurrency must be between 1 and 1024")
	}
	if c.Runtime.ClockProbeInterval <= 0 {
		return errors.New("runtime.clock_probe_interval must be positive")
	}
	if c.Runtime.SpoolMaxBytes < 1 {
		return errors.New("runtime.spool_max_bytes must be positive")
	}
	if c.Security.MinimumTLSVersion != "1.2" {
		return errors.New("security.minimum_tls_version must be 1.2")
	}
	if c.Security.RedirectPolicy != "deny" {
		return errors.New("security.redirect_policy must deny redirects")
	}
	if c.Deployment.Role != "" {
		if _, err := deployment.ParseRole(c.Deployment.Role); err != nil {
			return err
		}
	}
	if c.Deployment.DryRun && c.Deployment.Role == "" {
		return errors.New("deployment.dry_run requires an explicit deployment.role")
	}
	if (c.Deployment.WriterLeaseKey == "") != (c.Deployment.WriterID == "") {
		return errors.New("deployment.writer_lease_key and writer_id must be declared together")
	}
	if err := validateSecretReferenceSyntax(c.secretReferences()); err != nil {
		return err
	}
	if err := validateOptionalCapture(c.Capture); err != nil {
		return err
	}
	if err := validateOptionalVerify(c.Verify); err != nil {
		return err
	}
	if err := validateOptionalObjectStore(c.ObjectStore); err != nil {
		return err
	}
	if c.Catalog.MinConns < 0 || c.Catalog.MaxConns < c.Catalog.MinConns {
		return errors.New("catalog connection bounds are invalid")
	}
	if err := validateOptionalCatalogCheck(c.Catalog.Check); err != nil {
		return err
	}
	if c.Warehouse.BatchRows < 1 {
		return errors.New("warehouse.batch_rows must be positive")
	}
	if err := validateSources(c.Sources); err != nil {
		return err
	}
	if c.Quality.AckTimeout <= 0 || c.Quality.HeartbeatTimeout <= 0 || c.Quality.SilenceTimeout <= 0 {
		return errors.New("quality timeouts must be positive")
	}
	if !slices.Contains([]string{"strict", "source-specific"}, c.Quality.SequencePolicy) {
		return fmt.Errorf("unknown quality.sequence_policy %q", c.Quality.SequencePolicy)
	}
	if !slices.Contains([]string{"quarantine", "fail"}, c.Quality.SchemaPolicy) {
		return fmt.Errorf("unknown quality.schema_policy %q", c.Quality.SchemaPolicy)
	}
	if err := validateOpportunityPolicies(c.Quality.OpportunityPolicies); err != nil {
		return err
	}
	if c.Dataset.PartitionWindow <= 0 || c.Dataset.RowGroupBytes < 1 || c.Dataset.DerivedRetention < 0 || c.Dataset.OpportunityArchiveMaxRows < 1 {
		return errors.New("dataset bounds are invalid")
	}
	if c.Dataset.Compression != "zstd" {
		return fmt.Errorf("unknown dataset.compression %q", c.Dataset.Compression)
	}
	if err := validateOptionalServe(c.Serve); err != nil {
		return err
	}
	if !slices.Contains([]string{"debug", "info", "warn", "error"}, c.Telemetry.LogLevel) {
		return fmt.Errorf("unknown telemetry.log_level %q", c.Telemetry.LogLevel)
	}
	if c.Telemetry.MaxSeries < 1 {
		return errors.New("telemetry.max_series must be positive")
	}
	traceActive := c.Telemetry.TraceExporterRef != "" || c.Telemetry.TraceQueueCapacity != 0 ||
		c.Telemetry.TraceBatchSpans != 0 || c.Telemetry.TraceExportTimeout != 0
	if traceActive && (c.Telemetry.TraceExporterRef == "" || c.Telemetry.TraceQueueCapacity < 1 ||
		c.Telemetry.TraceQueueCapacity > 4_096 || c.Telemetry.TraceBatchSpans < 1 ||
		c.Telemetry.TraceBatchSpans > 512 || c.Telemetry.TraceExportTimeout <= 0) {
		return errors.New("telemetry trace exporter reference, queue up to 4096, batch up to 512, and timeout must be declared together")
	}
	return nil
}

// SecretValidator checks whether one opaque caller-owned reference can be
// resolved. Implementations must not include the reference or secret value in
// returned errors.
type SecretValidator func(context.Context, string) error

// ValidateRole checks the destinations and only the secret bindings required
// by the selected operation before composition or runner effects.
func (c Config) ValidateRole(ctx context.Context, operation string, validateSecret SecretValidator) error {
	role, err := deployment.RuntimeRole(operation)
	if err != nil {
		return err
	}
	configuredRole, err := deployment.ParseRole(c.Deployment.Role)
	if err != nil {
		return errors.New("an explicit valid deployment.role is required")
	}
	if configuredRole != role {
		return fmt.Errorf("deployment.role %q cannot run %q", configuredRole, operation)
	}

	switch role {
	case deployment.RoleCollector:
		if err := c.requireSources(); err != nil {
			return err
		}
		if err := c.requireCapture(); err != nil {
			return err
		}
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireCatalog(); err != nil {
			return err
		}
		if c.Deployment.WriterLeaseKey == "" || c.Deployment.WriterID == "" {
			return errors.New("collector requires deployment writer lease key and writer identity")
		}
	case deployment.RoleCatalogSync:
		if err := c.requireSources(); err != nil {
			return err
		}
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireCatalog(); err != nil {
			return err
		}
	case deployment.RoleMigrationJob:
		if err := c.requireCatalog(); err != nil {
			return err
		}
	case deployment.RoleDatasetBuilder:
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireCatalog(); err != nil {
			return err
		}
	case deployment.RoleWarehouseLoader:
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireWarehouse(); err != nil {
			return err
		}
	case deployment.RoleQueryReplayServer:
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireCatalog(); err != nil {
			return err
		}
		if err := c.requireWarehouse(); err != nil {
			return err
		}
		if operation == "serve" && c.Serve.Listener == "" {
			return errors.New("serve destination and authentication are required for serve")
		}
	case deployment.RoleVerifier:
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireCatalog(); err != nil {
			return err
		}
	case deployment.RoleBackupRecovery:
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireCatalog(); err != nil {
			return err
		}
		if err := c.requireWarehouse(); err != nil {
			return err
		}
	}

	for _, ref := range c.roleSecretReferences(role, operation) {
		if validateSecret == nil || validateSecret(ctx, ref.value) != nil {
			return fmt.Errorf("%s is not bound", ref.field)
		}
	}
	return nil
}

type secretReference struct {
	field string
	value string
}

func (c Config) secretReferences() []secretReference {
	refs := make([]secretReference, 0, 5+len(c.Sources)+len(c.Serve.BearerTokenRefs))
	add := func(field, value string) {
		if value != "" {
			refs = append(refs, secretReference{field: field, value: value})
		}
	}
	add("object_store.credential_ref", c.ObjectStore.CredentialRef)
	add("catalog.dsn_ref", c.Catalog.DSNRef)
	add("warehouse.dsn_ref", c.Warehouse.DSNRef)
	for i, source := range c.Sources {
		add(fmt.Sprintf("sources[%d].entitlement_ref", i), source.EntitlementRef)
	}
	add("serve.tls_cert_ref", c.Serve.TLSCertRef)
	add("serve.tls_key_ref", c.Serve.TLSKeyRef)
	scopes := make([]string, 0, len(c.Serve.BearerTokenRefs))
	for scope := range c.Serve.BearerTokenRefs {
		scopes = append(scopes, scope)
	}
	slices.Sort(scopes)
	for _, scope := range scopes {
		add(fmt.Sprintf("serve.bearer_token_refs[%q]", scope), c.Serve.BearerTokenRefs[scope])
	}
	add("telemetry.trace_exporter_ref", c.Telemetry.TraceExporterRef)
	return refs
}

func (c Config) roleSecretReferences(role deployment.Role, operation string) []secretReference {
	refs := make([]secretReference, 0, 8+len(c.Sources)+len(c.Serve.BearerTokenRefs))
	add := func(field, value string) {
		if value != "" {
			refs = append(refs, secretReference{field: field, value: value})
		}
	}
	switch role {
	case deployment.RoleCollector:
		add("object_store.credential_ref", c.ObjectStore.CredentialRef)
		add("catalog.dsn_ref", c.Catalog.DSNRef)
		for i, source := range c.Sources {
			add(fmt.Sprintf("sources[%d].entitlement_ref", i), source.EntitlementRef)
		}
	case deployment.RoleCatalogSync, deployment.RoleDatasetBuilder:
		add("object_store.credential_ref", c.ObjectStore.CredentialRef)
		add("catalog.dsn_ref", c.Catalog.DSNRef)
	case deployment.RoleMigrationJob:
		add("catalog.dsn_ref", c.Catalog.DSNRef)
	case deployment.RoleWarehouseLoader:
		add("object_store.credential_ref", c.ObjectStore.CredentialRef)
		add("warehouse.dsn_ref", c.Warehouse.DSNRef)
	case deployment.RoleQueryReplayServer:
		add("object_store.credential_ref", c.ObjectStore.CredentialRef)
		add("catalog.dsn_ref", c.Catalog.DSNRef)
		add("warehouse.dsn_ref", c.Warehouse.DSNRef)
		if operation == "serve" {
			add("serve.tls_cert_ref", c.Serve.TLSCertRef)
			add("serve.tls_key_ref", c.Serve.TLSKeyRef)
			scopes := make([]string, 0, len(c.Serve.BearerTokenRefs))
			for scope := range c.Serve.BearerTokenRefs {
				scopes = append(scopes, scope)
			}
			slices.Sort(scopes)
			for _, scope := range scopes {
				add(fmt.Sprintf("serve.bearer_token_refs[%q]", scope), c.Serve.BearerTokenRefs[scope])
			}
		}
	case deployment.RoleVerifier:
		add("object_store.credential_ref", c.ObjectStore.CredentialRef)
		add("catalog.dsn_ref", c.Catalog.DSNRef)
	case deployment.RoleBackupRecovery:
		add("object_store.credential_ref", c.ObjectStore.CredentialRef)
		add("catalog.dsn_ref", c.Catalog.DSNRef)
		add("warehouse.dsn_ref", c.Warehouse.DSNRef)
	}
	add("telemetry.trace_exporter_ref", c.Telemetry.TraceExporterRef)
	return refs
}

func validateSecretReferenceSyntax(refs []secretReference) error {
	for _, ref := range refs {
		if !validEnvironmentName(ref.value) {
			return fmt.Errorf("%s must name one opaque environment reference", ref.field)
		}
	}
	return nil
}

func (c Config) requireSources() error {
	if len(c.Sources) == 0 {
		return errors.New("at least one source is required for this role")
	}
	return nil
}

func (c Config) requireCapture() error {
	if !captureActive(c.Capture) {
		return errors.New("collector requires explicit bounded capture pressure configuration")
	}
	if err := validateOptionalCapture(c.Capture); err != nil {
		return fmt.Errorf("collector capture pressure configuration: %w", err)
	}
	return nil
}

func (c Config) requireObjectStore() error {
	if c.ObjectStore.Endpoint == "" {
		return errors.New("object_store destination is required for this role")
	}
	return nil
}

func (c Config) requireCatalog() error {
	if c.Catalog.DSNRef == "" || len(c.Catalog.ServerMajors) == 0 {
		return errors.New("catalog destination and declared server majors are required for this role")
	}
	return nil
}

func (c Config) requireWarehouse() error {
	if c.Warehouse.DSNRef == "" || c.Warehouse.Database == "" || c.Warehouse.ServerDigest == "" {
		return errors.New("warehouse destination, database, and server digest are required for this role")
	}
	return nil
}

type verifyVenueContract struct {
	endpoints   []string
	methods     []string
	channels    []string
	fixtureOnly bool
}

var verifyVenueContracts = map[string]verifyVenueContract{
	"binance-spot": {
		endpoints: []string{"https://data-api.binance.vision", "wss://data-stream.binance.vision/ws"},
		methods:   []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:  []string{"bookTicker", "depth@100ms", "ticker", "trade"},
	},
	"binance-usdm": {
		endpoints:   []string{"https://fapi.binance.com", "wss://fstream.binance.com/market", "wss://fstream.binance.com/public"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"aggTrade", "bookTicker", "depth@100ms", "forceOrder", "indexPrice", "markPrice", "openInterest", "ticker"},
		fixtureOnly: true,
	},
	"binance-coinm": {
		endpoints:   []string{"https://dapi.binance.com", "wss://dstream.binance.com"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"!ticker@arr", "aggTrade", "bookTicker", "depth@100ms", "markPrice", "openInterest", "ticker"},
		fixtureOnly: true,
	},
	// bybit/contract.go and testdata/bybit manifests, accessed 2026-08-23.
	"bybit-spot": {
		endpoints:   []string{"https://api.bybit.com", "wss://stream.bybit.com/v5/public/spot"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"/v5/market/instruments-info", "orderbook.1.{symbol}", "orderbook.full.{symbol}", "orderbook.rpi.{symbol}", "orderbook.{depth}.{symbol}", "publicTrade.{symbol}", "tickers.{symbol}"},
		fixtureOnly: true,
	},
	"bybit-linear": {
		endpoints:   []string{"https://api.bybit.com", "wss://stream.bybit.com/v5/public/linear"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"/v5/market/instruments-info", "allLiquidation.{symbol}", "orderbook.1.{symbol}", "orderbook.full.{symbol}", "orderbook.rpi.{symbol}", "orderbook.{depth}.{symbol}", "publicTrade.{symbol}", "tickers.{symbol}"},
		fixtureOnly: true,
	},
	"bybit-inverse": {
		endpoints:   []string{"https://api.bybit.com", "wss://stream.bybit.com/v5/public/inverse"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"/v5/market/instruments-info", "allLiquidation.{symbol}", "orderbook.1.{symbol}", "orderbook.full.{symbol}", "orderbook.rpi.{symbol}", "orderbook.{depth}.{symbol}", "publicTrade.{symbol}", "tickers.{symbol}"},
		fixtureOnly: true,
	},
	"bybit-option": {
		endpoints:   []string{"https://api.bybit.com", "wss://stream.bybit.com/v5/public/option"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"/v5/market/instruments-info", "orderbook.{25|100}.{instrument}", "publicTrade.{base_coin}", "tickers.{base_coin}"},
		fixtureOnly: true,
	},
	// okx/contract.go and okx/testdata/manifest.json, accessed 2026-08-23.
	"okx-v5-spot": {
		endpoints:   []string{"https://www.okx.com", "wss://ws.okx.com:8443/ws/v5/business", "wss://ws.okx.com:8443/ws/v5/public"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"/api/v5/public/instruments", "bbo-tbt", "books", "books5", "tickers", "trades", "trades-all"},
		fixtureOnly: true,
	},
	"okx-v5-swap": {
		endpoints:   []string{"https://www.okx.com", "wss://ws.okx.com:8443/ws/v5/business", "wss://ws.okx.com:8443/ws/v5/public"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"/api/v5/public/instruments", "bbo-tbt", "books", "books5", "funding-rate", "index-tickers", "liquidation-orders", "mark-price", "open-interest", "tickers", "trades", "trades-all"},
		fixtureOnly: true,
	},
	"okx-v5-futures": {
		endpoints:   []string{"https://www.okx.com", "wss://ws.okx.com:8443/ws/v5/business", "wss://ws.okx.com:8443/ws/v5/public"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"/api/v5/public/instruments", "bbo-tbt", "books", "books5", "index-tickers", "liquidation-orders", "mark-price", "open-interest", "tickers", "trades", "trades-all"},
		fixtureOnly: true,
	},
	"okx-v5-option": {
		endpoints:   []string{"https://www.okx.com", "wss://ws.okx.com:8443/ws/v5/business", "wss://ws.okx.com:8443/ws/v5/public"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"/api/v5/public/instruments", "bbo-tbt", "books", "books5", "index-tickers", "liquidation-orders", "mark-price", "open-interest", "opt-summary", "tickers", "trades", "trades-all"},
		fixtureOnly: true,
	},
	// deribit/contract.go and deribit/testdata/manifest.json, accessed 2026-08-22.
	"deribit-v2": {
		endpoints:   []string{"wss://www.deribit.com/ws/api/v2"},
		methods:     []string{MethodMarketDataWebSocket},
		channels:    []string{"book.{instrument}.100ms", "book.{instrument}.{group}.{depth}.100ms", "deribit_price_index.{index}", "instrument.creation.{kind}.{currency}", "instrument.state.{kind}.{currency}", "perpetual.{instrument}.100ms", "quote.{instrument}", "ticker.{instrument}.100ms", "trades.{instrument}.100ms"},
		fixtureOnly: true,
	},
	// hyperliquid/contract.go and hyperliquid/testdata/manifest.json, accessed 2026-08-22.
	"hyperliquid-main": {
		endpoints:   []string{"https://api.hyperliquid.xyz/info", "wss://api.hyperliquid.xyz/ws"},
		methods:     []string{MethodMarketDataHTTPPost, MethodMarketDataWebSocket},
		channels:    []string{"info:fundingHistory", "info:metaAndAssetCtxs", "info:perpDexs|meta", "ws:activeAssetCtx", "ws:bbo", "ws:l2Book?fast=false", "ws:l2Book?fast=true", "ws:trades"},
		fixtureOnly: true,
	},
	"hyperliquid-hip3": {
		endpoints:   []string{"https://api.hyperliquid.xyz/info", "wss://api.hyperliquid.xyz/ws"},
		methods:     []string{MethodMarketDataHTTPPost, MethodMarketDataWebSocket},
		channels:    []string{"info:fundingHistory", "info:metaAndAssetCtxs", "info:perpDexs|meta", "ws:activeAssetCtx", "ws:bbo", "ws:l2Book?fast=false", "ws:l2Book?fast=true", "ws:trades"},
		fixtureOnly: true,
	},

	// Legacy fixture aggregate retained for the public verify-command compatibility surface.
	"bybit-v5": {
		endpoints:   []string{"https://api.bybit.com", "wss://stream.bybit.com/v5/public/inverse", "wss://stream.bybit.com/v5/public/linear", "wss://stream.bybit.com/v5/public/spot"},
		methods:     []string{MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
		channels:    []string{"allLiquidation.{symbol}", "orderbook.1.{symbol}", "orderbook.full.{symbol}", "orderbook.rpi.{symbol}", "orderbook.{depth}.{symbol}", "publicTrade.{symbol}", "tickers.{symbol}"},
		fixtureOnly: true,
	},
}

// ValidateVerifyVenue fails before any network or write effect. Every selector
// is bound to one access-dated public source contract. Only binance-spot may
// proceed to the separately validated live credential boundary.
func (c Config) ValidateVerifyVenue(ctx context.Context, venue string, validateSecret SecretValidator) error {
	contract, ok := verifyVenueContracts[venue]
	if !ok {
		return fmt.Errorf("verify venue does not support %q", venue)
	}
	if err := validateOptionalVerify(c.Verify); err != nil {
		return err
	}
	if c.Verify.Mode == VerifyModeFixture {
		if err := validateContainedFixtureManifest(c.Verify.FixtureRoot, c.Verify.FixtureManifest); err != nil {
			return err
		}
	}
	if contract.fixtureOnly && c.Verify.Mode != VerifyModeFixture {
		return fmt.Errorf("verify venue %s supports only fixture mode", venue)
	}
	if len(c.Sources) != 1 || c.Sources[0].API != venue {
		return fmt.Errorf("verify venue requires exactly one %s source", venue)
	}
	source := c.Sources[0]
	if len(source.Symbols) < 1 || len(source.Symbols) > VerifyMaximumSymbols {
		return fmt.Errorf("verify venue symbols must be within 1..%d", VerifyMaximumSymbols)
	}
	if source.EntitlementRef != "" || source.EntitlementScope != "" {
		return fmt.Errorf("verify venue %s fixture source cannot declare credentials or entitlement scope", venue)
	}
	if !equalSortedStrings(source.Endpoints, contract.endpoints) {
		return fmt.Errorf("verify venue %s source endpoints do not match the access-dated public allowlist", venue)
	}
	if !equalSortedStrings(source.Methods, contract.methods) {
		return fmt.Errorf("verify venue %s source methods do not match the public market-data contract", venue)
	}
	if !equalSortedStrings(source.Channels, contract.channels) {
		return fmt.Errorf("verify venue %s channels do not match the access-dated fixture contract", venue)
	}
	if venue == "binance-spot" && c.Verify.MaxMessages < 8+4*len(source.Symbols) {
		return errors.New("verify.max_messages cannot cover control plus four data observations per configured symbol")
	}
	if c.Verify.Mode == VerifyModeFixture {
		return nil
	}
	return c.validateBinanceSpotLive(ctx, validateSecret)
}

func equalSortedStrings(actual, required []string) bool {
	sorted := slices.Clone(actual)
	slices.Sort(sorted)
	return slices.Equal(sorted, required)
}

func (c Config) validateBinanceSpotLive(ctx context.Context, validateSecret SecretValidator) error {
	if c.Verify.Mode != VerifyModeLive {
		return errors.New("binance-spot verification has an invalid mode")
	}
	if err := c.requireObjectStore(); err != nil {
		return err
	}
	if err := c.requireCatalog(); err != nil {
		return err
	}
	if c.ObjectStore.Prefix == "" || c.ObjectStore.Prefix != strings.Trim(c.ObjectStore.Prefix, "/") || strings.Contains(c.ObjectStore.Prefix, "\\") {
		return errors.New("object_store.prefix must declare one contained S3 key prefix")
	}
	for _, part := range strings.Split(c.ObjectStore.Prefix, "/") {
		if part == "" || part == "." || part == ".." {
			return errors.New("object_store.prefix contains an unsafe path segment")
		}
	}
	if !validEnvironmentName(c.Catalog.DSNRef) {
		return errors.New("catalog.dsn_ref must name one explicit environment variable")
	}
	if !validEnvironmentName(c.ObjectStore.CredentialRef) {
		return errors.New("object_store.credential_ref must name one explicit environment variable")
	}
	for _, binding := range []struct {
		field string
		value string
	}{
		{field: "catalog.dsn_ref", value: c.Catalog.DSNRef},
		{field: "object_store.credential_ref", value: c.ObjectStore.CredentialRef},
	} {
		if validateSecret == nil || validateSecret(ctx, binding.value) != nil {
			return fmt.Errorf("%s is not bound", binding.field)
		}
	}
	return nil
}

func validateOptionalVerify(c VerifyConfig) error {
	if c.Mode == "" && c.FixtureRoot == "" && c.FixtureManifest == "" && c.SpoolRoot == "" && c.ArtifactRoot == "" {
		return nil
	}
	if c.Mode != VerifyModeFixture && c.Mode != VerifyModeLive {
		return fmt.Errorf("unknown verify.mode %q", c.Mode)
	}
	if c.SpoolRoot == "" || c.ArtifactRoot == "" {
		return errors.New("verify.spool_root and verify.artifact_root are required")
	}
	if !filepath.IsAbs(c.SpoolRoot) || !filepath.IsAbs(c.ArtifactRoot) {
		return errors.New("resolved verify roots must be absolute")
	}
	if c.MaxMessages < VerifyMinimumMessages || c.MaxMessages > VerifyMaximumMessages ||
		c.MaxBytes < VerifyMinimumBytes || c.MaxBytes > VerifyMaximumBytes ||
		c.MaxDuration < VerifyMinimumDuration || c.MaxDuration > VerifyMaximumDuration ||
		c.DepthLimit < 1 || c.DepthLimit > VerifyMaximumDepth {
		return errors.New("verify message, byte, duration, or depth bounds exceed conservative limits")
	}
	if c.Mode == VerifyModeFixture {
		if c.FixtureRoot == "" || c.FixtureManifest == "" ||
			!filepath.IsAbs(c.FixtureRoot) || !filepath.IsAbs(c.FixtureManifest) {
			return errors.New("fixture verification requires explicit resolved fixture_root and fixture_manifest")
		}
		relative, err := filepath.Rel(c.FixtureRoot, c.FixtureManifest)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return errors.New("fixture_manifest must be contained by fixture_root")
		}
		return nil
	}
	if c.FixtureRoot != "" || c.FixtureManifest != "" {
		return errors.New("live verification cannot declare fixture paths")
	}
	return nil
}

func validateContainedFixtureManifest(root, manifest string) error {
	relative, err := filepath.Rel(root, manifest)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("fixture_manifest must be contained by fixture_root")
	}
	resolvedRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolving fixture_root: %w", err)
	}
	resolvedManifest, err := filepath.EvalSymlinks(manifest)
	if err != nil {
		return fmt.Errorf("resolving fixture_manifest: %w", err)
	}
	relative, err = filepath.Rel(resolvedRoot, resolvedManifest)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return errors.New("fixture_manifest must remain contained by fixture_root after resolving symlinks")
	}
	rootInfo, err := os.Stat(resolvedRoot)
	if err != nil || !rootInfo.IsDir() {
		return errors.New("fixture_root must resolve to a directory")
	}
	manifestInfo, err := os.Stat(resolvedManifest)
	if err != nil || !manifestInfo.Mode().IsRegular() {
		return errors.New("fixture_manifest must resolve to a regular file")
	}
	return nil
}

func validEnvironmentName(value string) bool {
	if value == "" {
		return false
	}
	for i, r := range value {
		if i == 0 && !(r == '_' || r >= 'A' && r <= 'Z') {
			return false
		}
		if !(r == '_' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}

func validateOpportunityPolicies(policies []OpportunityPolicy) error {
	seen := make(map[string]struct{}, len(policies))
	for i, policy := range policies {
		if policy.SourceID == "" || policy.ChannelID == "" {
			return fmt.Errorf("quality.opportunity_policies[%d] source_id and channel_id are required", i)
		}
		key := policy.SourceID + "\x00" + policy.ChannelID
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate opportunity policy for source %q channel %q", policy.SourceID, policy.ChannelID)
		}
		seen[key] = struct{}{}
		if policy.HotRetention <= 0 || policy.SpillBatchRows < 1 {
			return fmt.Errorf("quality.opportunity_policies[%d] retention and spill bounds are invalid", i)
		}
	}
	return nil
}

func captureActive(c CaptureConfig) bool {
	return c.DecodeQueueCapacity != 0 || c.DurableQueueCapacity != 0 || c.DecodeHighWater != 0 ||
		c.DurableHighWater != 0 || c.DecodeLowWater != 0 || c.DurableLowWater != 0 ||
		c.MaxRawMessageBytes != 0 || c.PendingRESTCapacity != 0
}

func validateOptionalCapture(c CaptureConfig) error {
	if !captureActive(c) {
		return nil
	}
	if c.DecodeQueueCapacity < 2 || c.DurableQueueCapacity < 2 ||
		c.DecodeQueueCapacity > 1_000_000 || c.DurableQueueCapacity > 1_000_000 ||
		c.MaxRawMessageBytes < 1 || c.MaxRawMessageBytes > 64<<20 ||
		c.PendingRESTCapacity < 1 || c.PendingRESTCapacity > 1_000_000 {
		return errors.New("capture queue capacities and raw message bound are invalid")
	}
	if c.DecodeHighWater < 1 || c.DecodeHighWater >= c.DecodeQueueCapacity ||
		c.DurableHighWater < 1 || c.DurableHighWater >= c.DurableQueueCapacity ||
		c.DecodeLowWater < 0 || c.DecodeLowWater >= c.DecodeHighWater ||
		c.DurableLowWater < 0 || c.DurableLowWater >= c.DurableHighWater {
		return errors.New("capture queue water marks are invalid")
	}
	return nil
}

func validateOptionalCatalogCheck(c CatalogCheckConfig) error {
	fixtureActive := c.FixtureManifest != "" || len(c.FixtureNames) != 0 || c.ExpectedSnapshotSHA256 != ""
	if fixtureActive {
		if c.FixtureManifest == "" || len(c.FixtureNames) == 0 || c.ExpectedSnapshotSHA256 == "" {
			return errors.New("catalog.check fixture_manifest, fixture_names, and expected_snapshot_sha256 must be declared together")
		}
		if filepath.Ext(c.FixtureManifest) != ".json" {
			return errors.New("catalog.check.fixture_manifest must name a JSON manifest")
		}
		if len(c.FixtureNames) > 64 {
			return errors.New("catalog.check.fixture_names exceeds 64-page bound")
		}
		seen := make(map[string]struct{}, len(c.FixtureNames))
		for _, name := range c.FixtureNames {
			if name == "" || len(name) > 256 {
				return errors.New("catalog.check.fixture_names contains an empty or oversized name")
			}
			if _, exists := seen[name]; exists {
				return fmt.Errorf("catalog.check.fixture_names contains duplicate %q", name)
			}
			seen[name] = struct{}{}
		}
		if !validConfigSHA256(c.ExpectedSnapshotSHA256) {
			return errors.New("catalog.check.expected_snapshot_sha256 must be lowercase 64-character hexadecimal")
		}
	}
	platformActive := c.PlatformEvidence != "" || c.ExpectedPlatformReportSHA256 != ""
	if platformActive {
		if c.PlatformEvidence == "" || c.ExpectedPlatformReportSHA256 == "" {
			return errors.New("catalog.check platform_evidence and expected_platform_report_sha256 must be declared together")
		}
		if filepath.Ext(c.PlatformEvidence) != ".json" {
			return errors.New("catalog.check.platform_evidence must name a JSON document")
		}
		if !validConfigSHA256(c.ExpectedPlatformReportSHA256) {
			return errors.New("catalog.check.expected_platform_report_sha256 must be lowercase 64-character hexadecimal")
		}
	}
	return nil
}

func validConfigSHA256(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func validateOptionalObjectStore(c ObjectStoreConfig) error {
	if c.Endpoint == "" && c.Region == "" && c.Bucket == "" && c.Prefix == "" && c.CredentialRef == "" {
		return nil
	}
	if c.Endpoint == "" || c.Region == "" || c.Bucket == "" || c.CredentialRef == "" {
		return errors.New("object_store endpoint, region, bucket, and credential_ref must be declared together")
	}
	return validateURL("object_store.endpoint", c.Endpoint, "http", "https")
}

const (
	MethodMarketDataHTTPGet   = "market-data:http-get"
	MethodMarketDataHTTPPost  = "market-data:http-post"
	MethodMarketDataWebSocket = "market-data:websocket"
	EntitlementScopeReadOnly  = "market-data:read"
)

var sourceOrigins = map[string][]string{
	"binance-spot":     {"https://data-api.binance.vision", "wss://data-stream.binance.vision"},
	"binance-usdm":     {"https://fapi.binance.com", "wss://fstream.binance.com"},
	"binance-coinm":    {"https://dapi.binance.com", "wss://dstream.binance.com"},
	"bybit-spot":       {"https://api.bybit.com", "wss://stream.bybit.com"},
	"bybit-linear":     {"https://api.bybit.com", "wss://stream.bybit.com"},
	"bybit-inverse":    {"https://api.bybit.com", "wss://stream.bybit.com"},
	"bybit-option":     {"https://api.bybit.com", "wss://stream.bybit.com"},
	"okx-v5-spot":      {"https://www.okx.com", "wss://ws.okx.com:8443"},
	"okx-v5-swap":      {"https://www.okx.com", "wss://ws.okx.com:8443"},
	"okx-v5-futures":   {"https://www.okx.com", "wss://ws.okx.com:8443"},
	"okx-v5-option":    {"https://www.okx.com", "wss://ws.okx.com:8443"},
	"deribit-v2":       {"https://www.deribit.com", "wss://www.deribit.com"},
	"hyperliquid-main": {"https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz"},
	"hyperliquid-hip3": {"https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz"},
	"bybit-v5":         {"https://api.bybit.com", "wss://stream.bybit.com"},
	"okx-v5":           {"https://www.okx.com", "wss://ws.okx.com:8443"},
	"hyperliquid":      {"https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz"},
}

var sourceMethods = map[string][]string{
	"binance-spot":     verifyVenueContracts["binance-spot"].methods,
	"binance-usdm":     verifyVenueContracts["binance-usdm"].methods,
	"binance-coinm":    verifyVenueContracts["binance-coinm"].methods,
	"bybit-spot":       verifyVenueContracts["bybit-spot"].methods,
	"bybit-linear":     verifyVenueContracts["bybit-linear"].methods,
	"bybit-inverse":    verifyVenueContracts["bybit-inverse"].methods,
	"bybit-option":     verifyVenueContracts["bybit-option"].methods,
	"okx-v5-spot":      verifyVenueContracts["okx-v5-spot"].methods,
	"okx-v5-swap":      verifyVenueContracts["okx-v5-swap"].methods,
	"okx-v5-futures":   verifyVenueContracts["okx-v5-futures"].methods,
	"okx-v5-option":    verifyVenueContracts["okx-v5-option"].methods,
	"deribit-v2":       verifyVenueContracts["deribit-v2"].methods,
	"hyperliquid-main": verifyVenueContracts["hyperliquid-main"].methods,
	"hyperliquid-hip3": verifyVenueContracts["hyperliquid-hip3"].methods,
	"bybit-v5":         verifyVenueContracts["bybit-v5"].methods,
	"okx-v5":           {MethodMarketDataHTTPGet, MethodMarketDataWebSocket},
	"hyperliquid":      {MethodMarketDataHTTPPost, MethodMarketDataWebSocket},
}

func isOKXV5Source(api string) bool {
	if api == "okx-v5" {
		return true
	}
	_, known := sourceMethods[api]
	return known && strings.HasPrefix(api, "okx-v5-")
}

func validateSources(sources []SourceConfig) error {
	ids := make(map[string]struct{}, len(sources))
	for i, source := range sources {
		if source.ID == "" {
			return fmt.Errorf("sources[%d].id is required", i)
		}
		if _, exists := ids[source.ID]; exists {
			return fmt.Errorf("duplicate source id %q", source.ID)
		}
		ids[source.ID] = struct{}{}
		allowedMethods, known := sourceMethods[source.API]
		if !known {
			return fmt.Errorf("sources[%d] has unknown api %q", i, source.API)
		}
		if len(source.Endpoints) == 0 {
			return fmt.Errorf("sources[%d].endpoints is required", i)
		}
		seenEndpoints := make(map[string]struct{}, len(source.Endpoints))
		for _, endpoint := range source.Endpoints {
			if err := validateSourceDestination(source.API, endpoint); err != nil {
				return fmt.Errorf("sources[%d].endpoints: %w", i, err)
			}
			if _, exists := seenEndpoints[endpoint]; exists {
				return fmt.Errorf("sources[%d].endpoints contains duplicate %q", i, endpoint)
			}
			seenEndpoints[endpoint] = struct{}{}
		}
		if len(source.Methods) == 0 {
			return fmt.Errorf("sources[%d].methods must declare public market-data capabilities", i)
		}
		seenMethods := make(map[string]struct{}, len(source.Methods))
		for _, method := range source.Methods {
			if !slices.Contains(allowedMethods, method) {
				return fmt.Errorf("sources[%d].methods contains private, trading, or unknown capability %q", i, method)
			}
			if _, exists := seenMethods[method]; exists {
				return fmt.Errorf("sources[%d].methods contains duplicate %q", i, method)
			}
			seenMethods[method] = struct{}{}
		}
		for _, endpoint := range source.Endpoints {
			u, _ := url.Parse(endpoint)
			required := MethodMarketDataWebSocket
			if u.Scheme == "https" {
				required = MethodMarketDataHTTPPost
				if slices.Contains(allowedMethods, MethodMarketDataHTTPGet) {
					required = MethodMarketDataHTTPGet
				}
			}
			if _, exists := seenMethods[required]; !exists {
				return fmt.Errorf("sources[%d].methods omits %s required by an endpoint", i, required)
			}
		}
		if source.EntitlementRef == "" {
			if source.EntitlementScope != "" {
				return fmt.Errorf("sources[%d].entitlement_scope requires an opaque entitlement_ref", i)
			}
		} else {
			if source.API != "deribit-v2" && !isOKXV5Source(source.API) {
				return fmt.Errorf("sources[%d] declares entitlement credentials for a non-entitlement public source", i)
			}
			if source.EntitlementScope != EntitlementScopeReadOnly {
				return fmt.Errorf("sources[%d].entitlement_scope must be %q", i, EntitlementScopeReadOnly)
			}
		}
		for _, field := range []struct {
			name   string
			values []string
		}{
			{name: "channels", values: source.Channels},
			{name: "families", values: source.Families},
		} {
			if len(field.values) > 256 {
				return fmt.Errorf("sources[%d].%s exceeds 256 entries", i, field.name)
			}
			seenValues := make(map[string]struct{}, len(field.values))
			for _, value := range field.values {
				if strings.TrimSpace(value) == "" || len(value) > 256 {
					return fmt.Errorf("sources[%d].%s contains an invalid identity", i, field.name)
				}
				if _, exists := seenValues[value]; exists {
					return fmt.Errorf("sources[%d].%s contains duplicate %q", i, field.name, value)
				}
				seenValues[value] = struct{}{}
			}
		}
		if source.Cadence < 0 {
			return fmt.Errorf("sources[%d].cadence cannot be negative", i)
		}
	}
	return nil
}

func validateSourceDestination(api, raw string) error {
	if err := validateURL("source endpoint", raw, "https", "wss"); err != nil {
		return err
	}
	u, _ := url.Parse(raw)
	origin := strings.ToLower(u.Scheme + "://" + u.Host)
	if !slices.Contains(sourceOrigins[api], origin) {
		return fmt.Errorf("%q is not an allowlisted public market-data destination for %s", origin, api)
	}
	if !publicSourcePath(api, strings.ToLower(u.Scheme), u.EscapedPath()) {
		return fmt.Errorf("%q is not an allowlisted public market-data endpoint path for %s", u.EscapedPath(), api)
	}
	return nil
}

func publicSourcePath(api, scheme, path string) bool {
	if scheme == "https" {
		switch api {
		case "deribit-v2":
			return slices.Contains([]string{"", "/", "/api/v2/public"}, path)
		case "hyperliquid", "hyperliquid-main", "hyperliquid-hip3":
			return slices.Contains([]string{"", "/", "/info"}, path)
		default:
			return path == "" || path == "/"
		}
	}
	if isOKXV5Source(api) {
		return slices.Contains([]string{"/ws/v5/public", "/ws/v5/business"}, path)
	}
	switch api {
	case "binance-spot":
		return slices.Contains([]string{"", "/", "/ws", "/stream"}, path)
	case "binance-usdm":
		return slices.Contains([]string{"", "/", "/ws", "/public", "/market"}, path)
	case "binance-coinm":
		return slices.Contains([]string{"", "/", "/ws"}, path)
	case "bybit-spot":
		return path == "/v5/public/spot"
	case "bybit-linear":
		return path == "/v5/public/linear"
	case "bybit-inverse":
		return path == "/v5/public/inverse"
	case "bybit-option":
		return path == "/v5/public/option"
	case "bybit-v5":
		return slices.Contains([]string{"/v5/public/spot", "/v5/public/linear", "/v5/public/inverse", "/v5/public/option"}, path)
	case "deribit-v2":
		return slices.Contains([]string{"", "/", "/ws/api/v2"}, path)
	case "hyperliquid", "hyperliquid-main", "hyperliquid-hip3":
		return slices.Contains([]string{"", "/", "/ws"}, path)
	default:
		return false
	}
}

// AuthorizeRequest binds an effect to the exact configured destination and a
// public market-data capability. Appended paths, redirect targets, and generic
// HTTP method names do not authorize a request.
func (s SourceConfig) AuthorizeRequest(destination, method string) error {
	if !slices.Contains(s.Endpoints, destination) {
		return errors.New("source request destination is not explicitly configured")
	}
	if !slices.Contains(s.Methods, method) {
		return errors.New("source request method capability is not explicitly configured")
	}
	if err := validateSourceDestination(s.API, destination); err != nil {
		return err
	}
	if !slices.Contains(sourceMethods[s.API], method) {
		return errors.New("source request capability is not public market data")
	}
	return nil
}

func (c Config) AuthorizeRedirect(_, _ string) error {
	return errors.New("source redirects are disabled")
}

func validateOptionalServe(c ServeConfig) error {
	active := c.Listener != "" || c.TLSCertRef != "" || c.TLSKeyRef != "" || len(c.BearerTokenRefs) != 0
	if active {
		if c.Listener == "" || c.TLSCertRef == "" || c.TLSKeyRef == "" || len(c.BearerTokenRefs) == 0 {
			return errors.New("serve listener, TLS references, and bearer token references must be declared together")
		}
		if _, _, err := net.SplitHostPort(c.Listener); err != nil {
			return fmt.Errorf("serve.listener must be host:port: %w", err)
		}
		for scope, ref := range c.BearerTokenRefs {
			if strings.TrimSpace(scope) == "" || strings.TrimSpace(ref) == "" {
				return errors.New("serve bearer token scopes and references must be non-empty")
			}
		}
	}
	if c.ReadTimeout <= 0 || c.WriteTimeout <= 0 || c.IdleTimeout <= 0 {
		return errors.New("serve timeouts must be positive")
	}
	if c.DefaultPageRows < 1 || c.MaxPageRows < c.DefaultPageRows || c.MaxPageRows > 10_000 {
		return errors.New("serve page bounds are invalid")
	}
	if c.MaxResponseBytes < 1 || c.MaxResponseBytes > 16<<20 {
		return errors.New("serve.max_response_bytes must be between 1 and 16777216")
	}
	return nil
}

func validateURL(field, raw string, schemes ...string) error {
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Host == "" || u.Hostname() == "" || !slices.Contains(schemes, strings.ToLower(u.Scheme)) ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" {
		return fmt.Errorf("%s is not a valid %s URL", field, strings.Join(schemes, "/"))
	}
	return nil
}
