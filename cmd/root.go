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
	"github.com/spf13/cobra"
)

type Command = cobra.Command

type BuildInfo struct {
	Version string
	Commit  string
	Date    string
}

type Dependencies struct {
	Build          BuildInfo
	LoadConfig     func(string, config.Overrides) (config.Config, error)
	ValidateSecret config.SecretValidator
	Run            func(context.Context, string, config.Config, io.Writer) error
	CheckCatalog   func(context.Context, string, []string, string, io.Writer) error
	VerifyVenue    func(context.Context, string, config.Config, io.Writer) error
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
			if err := cfg.ValidateVerifyVenue(cmd.Context(), venue, deps.ValidateSecret); err != nil {
				return fmt.Errorf("validating verify venue preconditions: %w", err)
			}
			if deps.VerifyVenue == nil {
				return errorsForMissingRole("verify venue")
			}
			return deps.VerifyVenue(cmd.Context(), venue, cfg, cmd.OutOrStdout())
		},
	}
	command.Flags().String("venue", "", "exact venue contract to verify")
	command.ValidArgsFunction = cobra.NoFileCompletions
	return command
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
			return deps.Run(cmd.Context(), role, cfg, cmd.OutOrStdout())
		},
	}
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
