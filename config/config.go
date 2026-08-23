// Package config loads and validates explicit, typed runtime configuration.
package config

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

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
	ObjectStore ObjectStoreConfig `mapstructure:"object_store"`
	Catalog     CatalogConfig     `mapstructure:"catalog"`
	Warehouse   WarehouseConfig   `mapstructure:"warehouse"`
	Sources     []SourceConfig    `mapstructure:"sources"`
	Quality     QualityConfig     `mapstructure:"quality"`
	Dataset     DatasetConfig     `mapstructure:"dataset"`
	Serve       ServeConfig       `mapstructure:"serve"`
	Telemetry   TelemetryConfig   `mapstructure:"telemetry"`

	Verify VerifyConfig `mapstructure:"verify"`
}

type RuntimeConfig struct {
	ShutdownTimeout    time.Duration `mapstructure:"shutdown_timeout"`
	MaxConcurrency     int           `mapstructure:"max_concurrency"`
	ClockProbeInterval time.Duration `mapstructure:"clock_probe_interval"`
	SpoolMaxBytes      int64         `mapstructure:"spool_max_bytes"`
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
	FixtureManifest        string   `mapstructure:"fixture_manifest"`
	FixtureNames           []string `mapstructure:"fixture_names"`
	ExpectedSnapshotSHA256 string   `mapstructure:"expected_snapshot_sha256"`
}

type WarehouseConfig struct {
	DSNRef       string `mapstructure:"dsn_ref"`
	Database     string `mapstructure:"database"`
	ServerDigest string `mapstructure:"server_digest"`
	BatchRows    int    `mapstructure:"batch_rows"`
}

type SourceConfig struct {
	ID             string        `mapstructure:"id"`
	API            string        `mapstructure:"api"`
	Endpoints      []string      `mapstructure:"endpoints"`
	EntitlementRef string        `mapstructure:"entitlement_ref"`
	Channels       []string      `mapstructure:"channels"`
	Symbols        []string      `mapstructure:"symbols"`
	Cadence        time.Duration `mapstructure:"cadence"`
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
	LogLevel         string `mapstructure:"log_level"`
	TraceExporterRef string `mapstructure:"trace_exporter_ref"`
	MetricsListener  string `mapstructure:"metrics_listener"`
	MaxSeries        int    `mapstructure:"max_series"`
}

var defaults = map[string]any{
	"runtime.shutdown_timeout":             "15s",
	"runtime.max_concurrency":              8,
	"runtime.clock_probe_interval":         "30s",
	"runtime.spool_max_bytes":              int64(1 << 30),
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
	"telemetry.max_series":                 10_000,
}

var registeredKeys = []string{
	"runtime.shutdown_timeout", "runtime.max_concurrency", "runtime.clock_probe_interval", "runtime.spool_max_bytes",
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
	"telemetry.log_level", "telemetry.trace_exporter_ref", "telemetry.metrics_listener", "telemetry.max_series",
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
	return nil
}

// SecretValidator checks whether one opaque caller-owned reference can be
// resolved. Implementations must not include the reference or secret value in
// returned errors.
type SecretValidator func(context.Context, string) error

// ValidateRole checks the destinations and secret bindings required before a
// role may cross its network or write-effect boundary.
func (c Config) ValidateRole(ctx context.Context, role string, validateSecret SecretValidator) error {
	switch role {
	case "collect", "catalog sync":
		if err := c.requireSources(); err != nil {
			return err
		}
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireCatalog(); err != nil {
			return err
		}
	case "verify venue":
		return c.ValidateVerifyVenue(ctx, "binance-spot", validateSecret)
	case "catalog inspect", "catalog check":
		if err := c.requireCatalog(); err != nil {
			return err
		}
	case "replay native", "replay normalized", "export parquet", "verify segment", "verify replay", "verify coverage":
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireCatalog(); err != nil {
			return err
		}
	case "serve":
		if err := c.requireObjectStore(); err != nil {
			return err
		}
		if err := c.requireCatalog(); err != nil {
			return err
		}
		if err := c.requireWarehouse(); err != nil {
			return err
		}
		if c.Serve.Listener == "" {
			return errors.New("serve destination and authentication are required for serve")
		}
	default:
		return fmt.Errorf("unknown runtime role %q", role)
	}

	for _, ref := range c.secretReferences() {
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

func (c Config) requireSources() error {
	if len(c.Sources) == 0 {
		return errors.New("at least one source is required for this role")
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

// ValidateVerifyVenue fails before any network or write effect. Fixture mode
// uses only explicit local roots; live mode additionally resolves exactly the
// PostgreSQL and S3 credential environment names declared by the caller.
func (c Config) ValidateVerifyVenue(ctx context.Context, venue string, validateSecret SecretValidator) error {
	if venue != "binance-spot" {
		return fmt.Errorf("verify venue supports only binance-spot")
	}
	if err := validateOptionalVerify(c.Verify); err != nil {
		return err
	}
	if len(c.Sources) != 1 || c.Sources[0].API != venue {
		return errors.New("verify venue requires exactly one binance-spot source")
	}
	source := c.Sources[0]
	if len(source.Symbols) < 1 || len(source.Symbols) > VerifyMaximumSymbols {
		return fmt.Errorf("verify venue symbols must be within 1..%d", VerifyMaximumSymbols)
	}
	if c.Verify.MaxMessages < 8+4*len(source.Symbols) {
		return errors.New("verify.max_messages cannot cover control plus four data observations per configured symbol")
	}
	requiredChannels := []string{"bookTicker", "depth@100ms", "ticker", "trade"}
	channels := slices.Clone(source.Channels)
	slices.Sort(channels)
	if !slices.Equal(channels, requiredChannels) {
		return errors.New("verify venue requires trade, depth@100ms, bookTicker, and ticker channels exactly once")
	}
	requiredEndpoints := []string{
		"https://data-api.binance.vision",
		"wss://data-stream.binance.vision/ws",
	}
	endpoints := slices.Clone(source.Endpoints)
	slices.Sort(endpoints)
	if !slices.Equal(endpoints, requiredEndpoints) {
		return errors.New("verify venue source endpoints must be the allowlisted Binance public market-data endpoints")
	}
	if c.Verify.Mode == VerifyModeFixture {
		return nil
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
		return nil
	}
	if c.FixtureRoot != "" || c.FixtureManifest != "" {
		return errors.New("live verification cannot declare fixture paths")
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

func validateOptionalCatalogCheck(c CatalogCheckConfig) error {
	active := c.FixtureManifest != "" || len(c.FixtureNames) != 0 || c.ExpectedSnapshotSHA256 != ""
	if !active {
		return nil
	}
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
	decoded, err := hex.DecodeString(c.ExpectedSnapshotSHA256)
	if err != nil || len(decoded) != sha256.Size || c.ExpectedSnapshotSHA256 != strings.ToLower(c.ExpectedSnapshotSHA256) {
		return errors.New("catalog.check.expected_snapshot_sha256 must be lowercase 64-character hexadecimal")
	}
	return nil
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

func validateSources(sources []SourceConfig) error {
	ids := make(map[string]struct{}, len(sources))
	allowedAPIs := []string{"binance-spot", "binance-usdm", "binance-coinm", "bybit-v5", "okx-v5", "deribit-v2", "hyperliquid"}
	for i, source := range sources {
		if source.ID == "" {
			return fmt.Errorf("sources[%d].id is required", i)
		}
		if _, exists := ids[source.ID]; exists {
			return fmt.Errorf("duplicate source id %q", source.ID)
		}
		ids[source.ID] = struct{}{}
		if !slices.Contains(allowedAPIs, source.API) {
			return fmt.Errorf("sources[%d] has unknown api %q", i, source.API)
		}
		if len(source.Endpoints) == 0 {
			return fmt.Errorf("sources[%d].endpoints is required", i)
		}
		for _, endpoint := range source.Endpoints {
			if err := validateURL(fmt.Sprintf("sources[%d].endpoints", i), endpoint, "https", "wss"); err != nil {
				return err
			}
		}
		if source.Cadence < 0 {
			return fmt.Errorf("sources[%d].cadence cannot be negative", i)
		}
	}
	return nil
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
	if err != nil || u.Host == "" || !slices.Contains(schemes, strings.ToLower(u.Scheme)) || u.User != nil {
		return fmt.Errorf("%s is not a valid %s URL", field, strings.Join(schemes, "/"))
	}
	return nil
}
