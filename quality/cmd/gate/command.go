package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"runtime"
	"time"

	"github.com/enable-xyz/marketdata/quality"
	"github.com/spf13/cobra"
)

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "release-gate",
		Short:         "Measure and verify the ELMD-029 release gate",
		Args:          cobra.NoArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newPrepareCommand(), newMeasureCommand(), newVerifyCommand())
	return root
}

func newMeasureCommand() *cobra.Command {
	var manifestPath, trustedSignerPublicKey, outputPath string
	var duration, burstDuration time.Duration
	command := &cobra.Command{
		Use:   "measure",
		Short: "Run the fixed corpus and write an immutable observation artifact",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			manifest, err := loadInputManifest(manifestPath, trustedSignerPublicKey)
			if err != nil {
				return err
			}
			if manifest.value.Hardware.Value.OS != runtime.GOOS || manifest.value.Hardware.Value.Architecture != runtime.GOARCH || manifest.value.Hardware.Value.LogicalCPUs != uint32(runtime.NumCPU()) {
				return errors.New("signed hardware OS, architecture, or logical CPU identity does not match the measurement process")
			}
			rss, err := newProcessRSSSampler()
			if err != nil {
				return err
			}
			artifact, err := runMeasurement(command.Context(), manifest, measureConfig{
				SustainedDurationNS: duration.Nanoseconds(), BurstDurationNS: burstDuration.Nanoseconds(), EnforceGateMinimums: true,
			}, newRealMonotonicClock(), rss)
			if err != nil {
				return err
			}
			data, err := json.Marshal(artifact)
			if err != nil {
				return fmt.Errorf("marshal observation artifact: %w", err)
			}
			if err := writeExclusive(outputPath, data); err != nil {
				return fmt.Errorf("write observation artifact: %w", err)
			}
			digest := sha256.Sum256(data)
			_, err = fmt.Fprintln(command.OutOrStdout(), hex.EncodeToString(digest[:]))
			return err
		},
	}
	flags := command.Flags()
	flags.StringVar(&manifestPath, "input-manifest", "", "path to the explicit signed fixed-corpus manifest")
	flags.StringVar(&trustedSignerPublicKey, "trusted-signer-public-key", "", "caller-pinned lowercase hex Ed25519 public key")
	flags.StringVar(&outputPath, "output", "", "create-exclusive observation artifact path")
	flags.DurationVar(&duration, "duration", 0, "sustained measurement duration (at least 60m)")
	flags.DurationVar(&burstDuration, "burst-duration", 0, "burst measurement duration (at least 30s)")
	_ = command.MarkFlagRequired("input-manifest")
	_ = command.MarkFlagRequired("trusted-signer-public-key")
	_ = command.MarkFlagRequired("output")
	_ = command.MarkFlagRequired("duration")
	_ = command.MarkFlagRequired("burst-duration")
	return command
}

func newVerifyCommand() *cobra.Command {
	var manifestPath, trustedSignerPublicKey, outputPath string
	var observation, fault, determinism, canary, x5 evidencePath
	command := &cobra.Command{
		Use:   "verify",
		Short: "Evaluate immutable observations and prior evidence receipts",
		Args:  cobra.NoArgs,
		RunE: func(command *cobra.Command, _ []string) error {
			manifest, err := loadInputManifest(manifestPath, trustedSignerPublicKey)
			if err != nil {
				return err
			}
			report, gateErr := verifyReleaseGate(manifest, verifyInputs{
				Observation: observation, Fault: fault, Determinism: determinism, Canary: canary, X5: x5,
			})
			if gateErr != nil && !errors.Is(gateErr, quality.ErrReleaseGateFailed) {
				return gateErr
			}
			data, err := marshalFullReport(report)
			if err != nil {
				return fmt.Errorf("marshal release gate report: %w", err)
			}
			if err := writeExclusive(outputPath, data); err != nil {
				return fmt.Errorf("write release gate report: %w", err)
			}
			digest := sha256.Sum256(data)
			if _, err := fmt.Fprintln(command.OutOrStdout(), hex.EncodeToString(digest[:])); err != nil {
				return err
			}
			return gateErr
		},
	}
	flags := command.Flags()
	flags.StringVar(&manifestPath, "input-manifest", "", "path to the explicit signed fixed-corpus manifest")
	flags.StringVar(&trustedSignerPublicKey, "trusted-signer-public-key", "", "caller-pinned lowercase hex Ed25519 public key")
	flags.StringVar(&outputPath, "output", "", "create-exclusive release gate report path")
	bindEvidenceFlags(flags.StringVar, &observation, "observation", "observation artifact")
	bindEvidenceFlags(flags.StringVar, &fault, "fault-receipt", "fault receipt")
	bindEvidenceFlags(flags.StringVar, &determinism, "determinism-receipt", "determinism receipt")
	bindEvidenceFlags(flags.StringVar, &canary, "canary-receipt", "canary receipt")
	bindEvidenceFlags(flags.StringVar, &x5, "x5-receipt", "X5 query receipt")
	for _, name := range []string{
		"input-manifest", "trusted-signer-public-key", "output", "observation", "observation-sha256", "fault-receipt", "fault-receipt-sha256",
		"determinism-receipt", "determinism-receipt-sha256", "canary-receipt", "canary-receipt-sha256", "x5-receipt", "x5-receipt-sha256",
	} {
		_ = command.MarkFlagRequired(name)
	}
	return command
}

type stringFlagBinder func(*string, string, string, string)

func bindEvidenceFlags(bind stringFlagBinder, target *evidencePath, name, description string) {
	bind(&target.Path, name, "", "path to the "+description)
	bind(&target.SHA256, name+"-sha256", "", "caller-pinned lowercase SHA-256 of the "+description)
}

func execute(ctx context.Context, stdout, stderr io.Writer, args []string) error {
	command := newRootCommand()
	command.SetOut(stdout)
	command.SetErr(stderr)
	command.SetArgs(args)
	return command.ExecuteContext(ctx)
}
