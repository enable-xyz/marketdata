package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/enable-xyz/marketdata/dataset"
	"github.com/spf13/cobra"
)

func newCommand() *cobra.Command {
	var engine, parquetPath string
	command := &cobra.Command{Use: "polars-vector", Short: "Verify the committed Parquet golden with Polars", Args: cobra.NoArgs,
		SilenceUsage: true, SilenceErrors: true, RunE: func(command *cobra.Command, _ []string) error {
			if err := dataset.RunPolarsVector(command.Context(), engine, parquetPath); err != nil {
				return err
			}
			_, err := fmt.Fprintln(command.OutOrStdout(), "Polars interoperability vector matched")
			return err
		}}
	command.Flags().StringVar(&engine, "engine", "", "explicit Python executable with Polars installed (required)")
	command.Flags().StringVar(&parquetPath, "parquet", "testdata/dataset/interop-v1.parquet", "Parquet golden path")
	_ = command.MarkFlagRequired("engine")
	return command
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	command := newCommand()
	if err := command.ExecuteContext(ctx); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
