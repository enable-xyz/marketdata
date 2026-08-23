// Package cmd owns the Cobra routing surface and configuration bindings.
package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"time"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/deployment"
	"github.com/spf13/cobra"
)

type Command = cobra.Command

type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

type ReleaseVerifyOptions struct {
	AMD64Binary    string
	ARM64Binary    string
	LicensePolicy  string
	EvidenceOutput string
}

type Runtime interface {
	Shutdown(context.Context) error
}

type Dependencies struct {
	Build                BuildInfo
	LoadConfig           func(string, config.Overrides) (config.Config, error)
	ValidateSecret       config.SecretValidator
	Compose              func(context.Context, string, config.Config, BuildInfo, io.Writer) (Runtime, error)
	Run                  func(context.Context, string, config.Config, Runtime, io.Writer) error
	CheckCatalog         func(context.Context, string, []string, string, io.Writer) error
	VerifyVenue          func(context.Context, string, config.Config, Runtime, io.Writer) error
	VerifyRelease        func(context.Context, ReleaseVerifyOptions, io.Writer) error
	WriteReleaseMetadata func(io.Writer) error
	SmokeRole            func(context.Context, string, io.Writer) error
}

// New returns one isolated command tree. It retains no package-level Cobra or
// configuration state, so callers may safely construct and execute many trees.
func New(deps Dependencies) *cobra.Command {
	if deps.LoadConfig == nil {
		deps.LoadConfig = config.Load
	}
	if deps.CheckCatalog == nil {
		deps.CheckCatalog = binance.CheckFixtureCatalog
	}
	root := &cobra.Command{
		Use:           "enable-market",
		Short:         "Record, replay, and serve primary-source market data",
		Version:       formatVersion(deps.Build),
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.SetVersionTemplate("{{.Version}}\n")
	root.PersistentFlags().String("config", "", "explicit YAML configuration file")
	root.PersistentFlags().Duration("shutdown-timeout", 0, "override runtime shutdown timeout")
	root.PersistentFlags().Int("max-concurrency", 0, "override runtime concurrency bound")
	root.PersistentFlags().String("log-level", "", "override telemetry log level")

	root.AddCommand(
		effectCommand("collect", "Run configured source adapters", "collect", deps),
		catalogCommand(deps),
		replayCommand(deps),
		exportCommand(deps),
		verifyCommand(deps),
		effectCommand("serve", "Serve authenticated read-only query and replay endpoints", "serve", deps),
		effectCommand("migrate", "Run PostgreSQL DDL under the migration role", "migration job", deps),
		effectCommand("load", "Load committed Parquet into the warehouse", "warehouse load", deps),
		effectCommand("backup", "Run caller-governed backup or recovery access", "backup recovery", deps),
		releaseCommand(deps),
		smokeCommand(deps),
		completionCommand(root),
	)
	return root
}

func formatVersion(info BuildInfo) string {
	version := info.Version
	if version == "" {
		version = "dev"
	}
	commit := info.Commit
	if commit == "" {
		commit = "none"
	}
	date := info.Date
	if date == "" {
		date = "unknown"
	}
	return fmt.Sprintf("%s (commit: %s, built: %s)", version, commit, date)
}

func catalogCommand(deps Dependencies) *cobra.Command {
	parent := &cobra.Command{Use: "catalog", Short: "Maintain and inspect temporal catalogs"}
	parent.AddCommand(
		effectCommand("sync", "Synchronize official source metadata once", "catalog sync", deps),
		effectCommand("inspect", "Inspect catalog state at an exact UTC instant", "catalog inspect", deps),
		catalogCheckCommand(deps),
	)
	return parent
}

func replayCommand(deps Dependencies) *cobra.Command {
	parent := &cobra.Command{Use: "replay", Short: "Replay committed raw or normalized records"}
	parent.AddCommand(
		effectCommand("native", "Replay fully verified native records", "replay native", deps),
		effectCommand("normalized", "Replay deterministically normalized records", "replay normalized", deps),
	)
	return parent
}

func exportCommand(deps Dependencies) *cobra.Command {
	parent := &cobra.Command{Use: "export", Short: "Build immutable derived datasets"}
	parent.AddCommand(effectCommand("parquet", "Build committed Parquet partitions", "export parquet", deps))
	return parent
}

func verifyCommand(deps Dependencies) *cobra.Command {
	parent := &cobra.Command{Use: "verify", Short: "Verify integrity, replay, coverage, and venue evidence"}
	parent.AddCommand(
		effectCommand("segment", "Verify segment objects, frames, records, and boundaries", "verify segment", deps),
		effectCommand("replay", "Verify deterministic logical and physical replay hashes", "verify replay", deps),
		effectCommand("coverage", "Recompute coverage from the logical opportunity ledger", "verify coverage", deps),
		verifyVenueCommand(deps),
	)
	return parent
}

func verifyVenueCommand(deps Dependencies) *cobra.Command {
	command := &cobra.Command{
		Use:   "venue",
		Short: "Execute one exact venue evidence packet",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			venue, err := cmd.Flags().GetString("venue")
			if err != nil {
				return fmt.Errorf("reading venue flag: %w", err)
			}
			if venue == "" {
				return errors.New("an explicit --venue is required")
			}
			path, err := cmd.Flags().GetString("config")
			if err != nil {
				return fmt.Errorf("reading config flag: %w", err)
			}
			cfg, err := deps.LoadConfig(path, explicitOverrides(cmd))
			if err != nil {
				return fmt.Errorf("loading configuration: %w", err)
			}
			if err := validateVerifyVenueRole(cmd.Context(), cfg, deps.ValidateSecret); err != nil {
				return fmt.Errorf("validating verify venue role: %w", err)
			}
			if err := cfg.ValidateVerifyVenue(cmd.Context(), venue, deps.ValidateSecret); err != nil {
				return fmt.Errorf("validating verify venue preconditions: %w", err)
			}
			if deps.VerifyVenue == nil {
				return errorsForMissingRole("verify venue")
			}
			if deps.Compose == nil {
				return errors.New("verify venue has no runtime composition")
			}
			runtime, err := deps.Compose(cmd.Context(), "verify venue", cfg, deps.Build, io.Discard)
			if err != nil {
				return fmt.Errorf("composing verify venue runtime: %w", err)
			}
			if runtime == nil {
				return errors.New("composing verify venue runtime returned nil")
			}
			verifyErr := deps.VerifyVenue(cmd.Context(), venue, cfg, runtime, cmd.OutOrStdout())
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(cmd.Context()), cfg.Runtime.ShutdownTimeout)
			defer cancel()
			shutdownErr := runtime.Shutdown(shutdownCtx)
			return errors.Join(verifyErr, shutdownErr)
		},
	}
	command.Flags().String("venue", "", "exact venue contract to verify")
	command.ValidArgsFunction = cobra.NoFileCompletions
	return command
}

// Fixture verification is intrinsically offline and the command itself fixes
// the identity to verifier. Live acquisition is a collector effect and is
// rejected before runtime composition.
func validateVerifyVenueRole(_ context.Context, cfg config.Config, _ config.SecretValidator) error {
	required, err := deployment.RuntimeRole("verify venue")
	if err != nil {
		return err
	}
	if cfg.Verify.Mode == config.VerifyModeLive {
		return errors.New("live venue acquisition requires the collector role; verifier accepts immutable fixture evidence only")
	}
	if cfg.Deployment.Role == "" && cfg.Verify.Mode == config.VerifyModeFixture {
		return nil
	}
	configured, err := deployment.ParseRole(cfg.Deployment.Role)
	if err != nil {
		return errors.New("an explicit valid deployment.role is required")
	}
	if configured != required {
		return fmt.Errorf("deployment.role %q cannot run %q", configured, "verify venue")
	}
	return nil
}

func catalogCheckCommand(deps Dependencies) *cobra.Command {
	return &cobra.Command{
		Use:   "check",
		Short: "Validate catalog integrity without mutation",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := cmd.Flags().GetString("config")
			if err != nil {
				return fmt.Errorf("reading config flag: %w", err)
			}
			cfg, err := deps.LoadConfig(path, explicitOverrides(cmd))
			if err != nil {
				return fmt.Errorf("loading configuration: %w", err)
			}
			manifestPath := cfg.Catalog.Check.FixtureManifest
			if !filepath.IsAbs(manifestPath) {
				manifestPath = filepath.Join(filepath.Dir(path), manifestPath)
			}
			return deps.CheckCatalog(
				cmd.Context(),
				filepath.Clean(manifestPath),
				cfg.Catalog.Check.FixtureNames,
				cfg.Catalog.Check.ExpectedSnapshotSHA256,
				cmd.OutOrStdout(),
			)
		},
	}
}

func effectCommand(use, short, role string, deps Dependencies) *cobra.Command {
	return &cobra.Command{
		Use:   use,
		Short: short,
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			path, err := cmd.Flags().GetString("config")
			if err != nil {
				return fmt.Errorf("reading config flag: %w", err)
			}
			cfg, err := deps.LoadConfig(path, explicitOverrides(cmd))
			if err != nil {
				return fmt.Errorf("loading configuration: %w", err)
			}
			if err := cfg.ValidateRole(cmd.Context(), role, deps.ValidateSecret); err != nil {
				return fmt.Errorf("validating %s preconditions: %w", role, err)
			}
			if deps.Run == nil {
				return errorsForMissingRole(role)
			}
			if deps.Compose == nil {
				return fmt.Errorf("%s has no runtime composition", role)
			}
			runtime, err := deps.Compose(cmd.Context(), role, cfg, deps.Build, cmd.OutOrStdout())
			if err != nil {
				return fmt.Errorf("composing %s runtime: %w", role, err)
			}
			if runtime == nil {
				return fmt.Errorf("composing %s runtime returned nil", role)
			}
			runErr := deps.Run(cmd.Context(), role, cfg, runtime, cmd.OutOrStdout())
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(cmd.Context()), cfg.Runtime.ShutdownTimeout)
			defer cancel()
			shutdownErr := runtime.Shutdown(shutdownCtx)
			return errors.Join(runErr, shutdownErr)
		},
	}
}

func releaseCommand(deps Dependencies) *cobra.Command {
	parent := &cobra.Command{Use: "release", Short: "Inspect deterministic release evidence"}
	verify := &cobra.Command{
		Use:   "verify",
		Short: "Inspect both Linux binaries and emit typed SBOM and license evidence",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			options := ReleaseVerifyOptions{}
			var err error
			if options.AMD64Binary, err = cmd.Flags().GetString("amd64"); err != nil {
				return err
			}
			if options.ARM64Binary, err = cmd.Flags().GetString("arm64"); err != nil {
				return err
			}
			if options.LicensePolicy, err = cmd.Flags().GetString("licenses"); err != nil {
				return err
			}
			if options.EvidenceOutput, err = cmd.Flags().GetString("evidence"); err != nil {
				return err
			}
			if options.AMD64Binary == "" || options.ARM64Binary == "" || options.LicensePolicy == "" || options.EvidenceOutput == "" {
				return errors.New("--amd64, --arm64, --licenses, and --evidence are required")
			}
			if deps.VerifyRelease == nil {
				return errors.New("release verifier is not configured")
			}
			return deps.VerifyRelease(cmd.Context(), options, cmd.OutOrStdout())
		},
	}
	verify.Flags().String("amd64", "", "caller-built linux/amd64 binary")
	verify.Flags().String("arm64", "", "caller-built linux/arm64 binary")
	verify.Flags().String("licenses", "", "exact typed license policy JSON")
	verify.Flags().String("evidence", "", "new immutable release evidence JSON")
	metadata := &cobra.Command{
		Use:   "metadata",
		Short: "Print the architecture-independent embedded release identity",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if deps.WriteReleaseMetadata == nil {
				return errors.New("release metadata is not configured")
			}
			return deps.WriteReleaseMetadata(cmd.OutOrStdout())
		},
	}
	parent.AddCommand(verify, metadata)
	return parent
}

func smokeCommand(deps Dependencies) *cobra.Command {
	command := &cobra.Command{
		Use:   "smoke",
		Short: "Run role-bound synthetic dry-run authorization paths",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if deps.SmokeRole == nil {
				return errors.New("role smoke runtime is not configured")
			}
			value, err := cmd.Flags().GetString("role")
			if err != nil {
				return err
			}
			if value == "all" {
				for _, role := range deployment.Roles() {
					if err := deps.SmokeRole(cmd.Context(), string(role), cmd.OutOrStdout()); err != nil {
						return fmt.Errorf("smoking %s: %w", role, err)
					}
				}
				return nil
			}
			role, err := deployment.ParseRole(value)
			if err != nil {
				return err
			}
			return deps.SmokeRole(cmd.Context(), string(role), cmd.OutOrStdout())
		},
	}
	command.Flags().String("role", "all", "deployment role or all")
	return command
}

func explicitOverrides(cmd *cobra.Command) config.Overrides {
	overrides := make(config.Overrides)
	if flag := cmd.Flags().Lookup("shutdown-timeout"); flag != nil && flag.Changed {
		value, err := time.ParseDuration(flag.Value.String())
		if err == nil {
			overrides["runtime.shutdown_timeout"] = value
		}
	}
	if flag := cmd.Flags().Lookup("max-concurrency"); flag != nil && flag.Changed {
		if value, err := strconv.Atoi(flag.Value.String()); err == nil {
			overrides["runtime.max_concurrency"] = value
		}
	}
	if flag := cmd.Flags().Lookup("log-level"); flag != nil && flag.Changed {
		overrides["telemetry.log_level"] = flag.Value.String()
	}
	return overrides
}

func errorsForMissingRole(role string) error {
	return fmt.Errorf("%s has no configured runtime", role)
}

func completionCommand(root *cobra.Command) *cobra.Command {
	return &cobra.Command{
		Use:       "completion [bash|fish|powershell|zsh]",
		Short:     "Generate shell completion",
		Args:      cobra.ExactArgs(1),
		ValidArgs: []string{"bash", "fish", "powershell", "zsh"},
		RunE: func(cmd *cobra.Command, args []string) error {
			out := cmd.OutOrStdout()
			switch args[0] {
			case "bash":
				return root.GenBashCompletionV2(out, true)
			case "fish":
				return root.GenFishCompletion(out, true)
			case "powershell":
				return root.GenPowerShellCompletionWithDesc(out)
			case "zsh":
				return root.GenZshCompletion(out)
			default:
				return fmt.Errorf("unsupported shell %q", args[0])
			}
		},
	}
}
