package main

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/enable-xyz/marketdata/cmd"
	"github.com/enable-xyz/marketdata/config"
	"github.com/enable-xyz/marketdata/deployment"
)

func runProductionRole(ctx context.Context, operation string, cfg config.Config, runtime cmd.Runtime, output io.Writer) error {
	composition, ok := runtime.(*runtimeComposition)
	if !ok || composition == nil {
		return errors.New("production role requires the configured runtime composition")
	}
	role, err := deployment.RuntimeRole(operation)
	if err != nil {
		return err
	}
	switch role {
	case deployment.RoleMigrationJob:
		return runProductionMigration(ctx, cfg, output)
	case deployment.RoleCollector:
		return runProductionCollector(ctx, cfg, output, false)
	case deployment.RoleCatalogSync:
		return runProductionCollector(ctx, cfg, output, true)
	case deployment.RoleDatasetBuilder:
		return runProductionDatasetBuilder(ctx, cfg, output)
	case deployment.RoleWarehouseLoader:
		return runProductionWarehouseLoader(ctx, cfg, output)
	case deployment.RoleQueryReplayServer:
		if operation != "serve" && operation != string(deployment.RoleQueryReplayServer) {
			return fmt.Errorf("%s is served through the authenticated query-replay server; run the serve command", operation)
		}
		return runProductionQueryServer(ctx, cfg, composition)
	case deployment.RoleVerifier:
		return errors.New("the selected verifier requires its dedicated bounded verify command")
	case deployment.RoleBackupRecovery:
		return errors.New("backup and recovery effects require a caller-selected destination and are performed by the documented datastore-native procedure")
	default:
		return fmt.Errorf("unsupported production deployment role %q", role)
	}
}
