package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/enable-xyz/marketdata/warehouse"
)

const (
	modeVerify  = "verify"
	modeMeasure = "measure"
)

type options struct {
	mode         string
	dsn          string
	serverDigest string
	database     string
	datasetRoot  string
	tablePrefix  string
	selection    warehouse.X5ProductionSelection
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stderr, os.Stdout); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stderr, stdout io.Writer) error {
	config, err := parseOptions(args, stderr)
	if err != nil {
		return err
	}
	manifests, err := warehouse.GenerateSyntheticDataset(ctx, config.datasetRoot, warehouse.DefaultX5SyntheticDatasetConfig())
	if err != nil {
		return err
	}
	disconnectVariant := warehouse.X5Variant{BatchRows: config.selection.Config.BatchRows,
		Compression: config.selection.Config.Compression, Layout: config.selection.Config.Layout}
	runConfig := warehouse.X5RunConfig{Native: warehouse.NativeConfig{
		DSN: config.dsn, Database: config.database, ServerDigest: config.serverDigest, TablePrefix: config.tablePrefix,
		BatchRows: config.selection.Config.BatchRows, Compression: config.selection.Config.Compression,
		Layout: config.selection.Config.Layout,
	}, Manifests: manifests, DisconnectVariant: disconnectVariant}
	if config.mode == modeMeasure {
		result, err := warehouse.RunX5(ctx, runConfig)
		if err != nil {
			return err
		}
		return warehouse.WriteX5Fixture(stdout, result)
	}
	if err := warehouse.VerifyPinnedX5Budgets(ctx, runConfig); err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(struct {
		Verified       bool             `json:"verified"`
		ServerDigest   string           `json:"server_digest"`
		SelectedConfig warehouse.Config `json:"selected_config"`
		SelectionRule  string           `json:"selection_rule"`
	}{Verified: true, ServerDigest: config.selection.ServerDigest, SelectedConfig: config.selection.Config,
		SelectionRule: config.selection.SelectionRule})
}

func parseOptions(args []string, stderr io.Writer) (options, error) {
	selection := warehouse.PinnedX5ProductionSelection()
	result := options{mode: modeVerify, serverDigest: selection.ServerDigest, tablePrefix: "x5", selection: selection}
	flags := flag.NewFlagSet("x5", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&result.mode, "mode", modeVerify, "release gate mode: verify (pinned fixture) or explicit measure")
	flags.StringVar(&result.dsn, "dsn", "", "explicit native ClickHouse DSN")
	flags.StringVar(&result.serverDigest, "server-digest", selection.ServerDigest, "explicit ClickHouse server digest")
	flags.StringVar(&result.database, "database", "", "explicit ClickHouse database (or include it in the DSN)")
	flags.StringVar(&result.datasetRoot, "dataset-root", "", "caller-owned root for the deterministic synthetic Parquet corpus")
	flags.StringVar(&result.tablePrefix, "table-prefix", "x5", "identifier prefix for replaceable X5 tables")
	if err := flags.Parse(args); err != nil {
		return options{}, err
	}
	if flags.NArg() != 0 || result.dsn == "" || result.serverDigest == "" || result.datasetRoot == "" {
		return options{}, fmt.Errorf("x5: -dsn, -server-digest, and -dataset-root are required; positional arguments are not accepted")
	}
	switch result.mode {
	case modeVerify:
		if result.serverDigest != selection.ServerDigest {
			return options{}, fmt.Errorf("x5: verify mode requires pinned server digest %q", selection.ServerDigest)
		}
	case modeMeasure:
	default:
		return options{}, fmt.Errorf("x5: -mode must be %q or %q", modeVerify, modeMeasure)
	}
	return result, nil
}
