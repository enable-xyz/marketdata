package main

import (
	"context"
	"fmt"
	"io"

	"github.com/enable-xyz/marketdata/cmd"
	"github.com/enable-xyz/marketdata/config"
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
		LoadConfig: config.Load,
		Run:        runRole,
	})
}

func runRole(_ context.Context, role string, _ config.Config, _ io.Writer) error {
	return fmt.Errorf("%s is not available in this build", role)
}
