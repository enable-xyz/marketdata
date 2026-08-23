package main

import (
	"fmt"
	"os"

	"github.com/enable-xyz/marketdata/dataset"
	"github.com/spf13/cobra"
)

func newCommand() *cobra.Command {
	var output string
	command := &cobra.Command{Use: "dataset-golden", Short: "Generate the deterministic tiny Parquet interoperability golden", Args: cobra.NoArgs,
		SilenceUsage: true, SilenceErrors: true, RunE: func(command *cobra.Command, _ []string) error {
			digest, err := dataset.WriteInteropGolden(output)
			if err != nil {
				return err
			}
			_, err = fmt.Fprintf(command.OutOrStdout(), "%x  %s\n", digest, output)
			return err
		}}
	command.Flags().StringVar(&output, "output", "testdata/dataset/interop-v1.parquet", "golden output path")
	return command
}

func main() {
	command := newCommand()
	if err := command.Execute(); err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
