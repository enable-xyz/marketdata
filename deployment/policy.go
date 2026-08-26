// Package deployment defines the deny-by-default runtime role boundary and
// canonical-writer lease protocol.
package deployment

import (
	"errors"
	"fmt"
	"maps"
	"slices"
)

type Role string

const (
	RoleCollector         Role = "collector"
	RoleCatalogSync       Role = "catalog-sync"
	RoleMigrationJob      Role = "migration-job"
	RoleDatasetBuilder    Role = "dataset-builder"
	RoleWarehouseLoader   Role = "warehouse-loader"
	RoleQueryReplayServer Role = "query-replay-server"
	RoleVerifier          Role = "verifier"
	RoleBackupRecovery    Role = "backup-recovery"
)

var allRoles = []Role{
	RoleCollector,
	RoleCatalogSync,
	RoleMigrationJob,
	RoleDatasetBuilder,
	RoleWarehouseLoader,
	RoleQueryReplayServer,
	RoleVerifier,
	RoleBackupRecovery,
}

func Roles() []Role { return slices.Clone(allRoles) }

func ParseRole(value string) (Role, error) {
	role := Role(value)
	if !slices.Contains(allRoles, role) {
		return "", fmt.Errorf("unknown deployment role %q", value)
	}
	return role, nil
}

// RuntimeRole maps the existing command operation names to the one deployment
// identity whose permissions apply to that operation.
func RuntimeRole(operation string) (Role, error) {
	switch operation {
	case "collect", string(RoleCollector):
		return RoleCollector, nil
	case "catalog sync", string(RoleCatalogSync):
		return RoleCatalogSync, nil
	case "migration job", string(RoleMigrationJob):
		return RoleMigrationJob, nil
	case "export parquet", string(RoleDatasetBuilder):
		return RoleDatasetBuilder, nil
	case "warehouse load", string(RoleWarehouseLoader):
		return RoleWarehouseLoader, nil
	case "catalog inspect", "replay native", "replay normalized", "serve", string(RoleQueryReplayServer):
		return RoleQueryReplayServer, nil
	case "verify segment", "verify replay", "verify coverage", "verify venue", string(RoleVerifier):
		return RoleVerifier, nil
	case "backup recovery", string(RoleBackupRecovery):
		return RoleBackupRecovery, nil
	default:
		return "", fmt.Errorf("unknown runtime operation %q", operation)
	}
}

type Operation string

const (
	OperationConnect  Operation = "connect"
	OperationRead     Operation = "read"
	OperationCreate   Operation = "create"
	OperationAppend   Operation = "append"
	OperationWrite    Operation = "write"
	OperationDDL      Operation = "ddl"
	OperationLock     Operation = "lock"
	OperationCorrect  Operation = "correct"
	OperationDelete   Operation = "delete"
	OperationSnapshot Operation = "snapshot"
	OperationVersion  Operation = "version"
)

type Resource string

const (
	ResourcePublicMarketDataNetwork Resource = "network/public-market-data"
	ResourceOfficialMetadataNetwork Resource = "network/official-metadata"
	ResourceRawObjects              Resource = "object/raw"
	ResourceRawMetadata             Resource = "object/raw-metadata"
	ResourceCommittedObjects        Resource = "object/committed"
	ResourceDerivedObjects          Resource = "object/derived"
	ResourceCommittedParquet        Resource = "object/parquet-committed"
	ResourceOpportunityLedger       Resource = "catalog/opportunity-subset"
	ResourceSegmentLedger           Resource = "catalog/segment-subset"
	ResourceTemporalCatalog         Resource = "catalog/temporal"
	ResourcePostgreSQLSchema        Resource = "postgresql/schema"
	ResourcePostgreSQLAdvisoryLock  Resource = "postgresql/advisory-lock"
	ResourceDatasetManifest         Resource = "dataset/manifest"
	ResourceClickHouse              Resource = "clickhouse/warehouse"
	ResourceLoadGeneration          Resource = "warehouse/load-generation"
	ResourceQuarantineReport        Resource = "quality/quarantine-report"
	ResourceCallerSnapshot          Resource = "backup/caller-snapshot"
	ResourceObjectVersion           Resource = "backup/object-version"
)

type Access struct {
	Operation Operation `json:"operation"`
	Resource  Resource  `json:"resource"`
	// Existing is meaningful for create operations. Create-only permissions
	// never authorize replacing an already present object.
	Existing bool `json:"existing,omitempty"`
}

var ErrPermissionDenied = errors.New("deployment permission denied")

// Authorizer is immutable after construction. Its zero value denies every
// request.
type Authorizer struct {
	permissions map[Role]map[Access]struct{}
}

func NewAuthorizer() Authorizer {
	permissions := make(map[Role]map[Access]struct{}, len(rolePermissions))
	for role, allowed := range rolePermissions {
		permissions[role] = maps.Clone(allowed)
	}
	return Authorizer{permissions: permissions}
}

func (a Authorizer) Authorize(role Role, access Access) error {
	if _, err := ParseRole(string(role)); err != nil {
		return fmt.Errorf("%w: %v", ErrPermissionDenied, err)
	}
	if access.Existing && access.Operation == OperationCreate {
		return fmt.Errorf("%w: %s cannot overwrite %s", ErrPermissionDenied, role, access.Resource)
	}
	allowed, ok := a.permissions[role]
	if !ok {
		return fmt.Errorf("%w: %s has no permissions", ErrPermissionDenied, role)
	}
	access.Existing = false
	if _, ok := allowed[access]; !ok {
		return fmt.Errorf("%w: %s cannot %s %s", ErrPermissionDenied, role, access.Operation, access.Resource)
	}
	return nil
}

func (a Authorizer) Allowed(role Role) []Access {
	allowed := a.permissions[role]
	result := make([]Access, 0, len(allowed))
	for access := range allowed {
		result = append(result, access)
	}
	slices.SortFunc(result, func(left, right Access) int {
		if left.Resource != right.Resource {
			return stringCompare(string(left.Resource), string(right.Resource))
		}
		return stringCompare(string(left.Operation), string(right.Operation))
	})
	return result
}

func stringCompare(left, right string) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func access(operation Operation, resource Resource) Access {
	return Access{Operation: operation, Resource: resource}
}

func permissionSet(values ...Access) map[Access]struct{} {
	result := make(map[Access]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}

var rolePermissions = map[Role]map[Access]struct{}{
	RoleCollector: permissionSet(
		access(OperationConnect, ResourcePublicMarketDataNetwork),
		access(OperationCreate, ResourceRawObjects),
		access(OperationAppend, ResourceOpportunityLedger),
		access(OperationAppend, ResourceSegmentLedger),
		access(OperationAppend, ResourceTemporalCatalog),
	),
	RoleCatalogSync: permissionSet(
		access(OperationConnect, ResourceOfficialMetadataNetwork),
		access(OperationCreate, ResourceRawMetadata),
		access(OperationAppend, ResourceTemporalCatalog),
	),
	RoleMigrationJob: permissionSet(
		access(OperationDDL, ResourcePostgreSQLSchema),
		access(OperationLock, ResourcePostgreSQLAdvisoryLock),
	),
	RoleDatasetBuilder: permissionSet(
		access(OperationRead, ResourceCommittedObjects),
		access(OperationCreate, ResourceDerivedObjects),
		access(OperationAppend, ResourceDatasetManifest),
	),
	RoleWarehouseLoader: permissionSet(
		access(OperationRead, ResourceCommittedParquet),
		access(OperationWrite, ResourceClickHouse),
		access(OperationWrite, ResourceLoadGeneration),
	),
	RoleQueryReplayServer: permissionSet(
		access(OperationRead, ResourceTemporalCatalog),
		access(OperationRead, ResourceClickHouse),
		access(OperationRead, ResourceCommittedObjects),
		access(OperationRead, ResourceDerivedObjects),
		access(OperationRead, ResourceCommittedParquet),
	),
	RoleVerifier: permissionSet(
		access(OperationRead, ResourceTemporalCatalog),
		access(OperationRead, ResourceClickHouse),
		access(OperationRead, ResourceCommittedObjects),
		access(OperationRead, ResourceDerivedObjects),
		access(OperationRead, ResourceCommittedParquet),
		access(OperationCreate, ResourceQuarantineReport),
	),
	RoleBackupRecovery: permissionSet(
		access(OperationSnapshot, ResourceCallerSnapshot),
		access(OperationVersion, ResourceObjectVersion),
	),
}
