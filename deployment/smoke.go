package deployment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/enable-xyz/marketdata/capture"
)

const SmokeFormat = "enable-market-role-smoke/v1"

type SmokeEvidence struct {
	Format             string `json:"format"`
	Role               Role   `json:"role"`
	AllowedChecks      int    `json:"allowed_checks"`
	DeniedChecks       int    `json:"denied_checks"`
	OverwriteDenials   int    `json:"overwrite_denials"`
	WriterHandoffFence uint64 `json:"writer_handoff_fence,omitempty"`
}

type smokeFixture struct {
	effects int
	objects map[Resource]map[string]struct{}
}

func newSmokeFixture() *smokeFixture {
	fixture := &smokeFixture{objects: make(map[Resource]map[string]struct{})}
	for _, resource := range []Resource{
		ResourceRawObjects,
		ResourceRawMetadata,
		ResourceCommittedObjects,
		ResourceDerivedObjects,
		ResourceCommittedParquet,
		ResourceQuarantineReport,
	} {
		fixture.objects[resource] = make(map[string]struct{})
	}
	for _, resource := range []Resource{ResourceCommittedObjects, ResourceDerivedObjects, ResourceCommittedParquet} {
		fixture.objects[resource]["verified"] = struct{}{}
	}
	return fixture
}

type smokeBoundary struct {
	role       Role
	authorizer Authorizer
	fixture    *smokeFixture
}

func (b smokeBoundary) effect(ctx context.Context, request Access, action func() error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := b.authorizer.Authorize(b.role, request); err != nil {
		return err
	}
	if action != nil {
		if err := action(); err != nil {
			return err
		}
	}
	b.fixture.effects++
	return nil
}

type smokeNetworkClient struct{ smokeBoundary }

func (c smokeNetworkClient) Connect(ctx context.Context, resource Resource) error {
	return c.effect(ctx, Access{Operation: OperationConnect, Resource: resource}, nil)
}

type smokeObjectClient struct{ smokeBoundary }

func (c smokeObjectClient) Create(ctx context.Context, resource Resource, key string) error {
	_, exists := c.fixture.objects[resource][key]
	return c.effect(ctx, Access{Operation: OperationCreate, Resource: resource, Existing: exists}, func() error {
		c.fixture.objects[resource][key] = struct{}{}
		return nil
	})
}

func (c smokeObjectClient) Read(ctx context.Context, resource Resource, key string) error {
	return c.effect(ctx, Access{Operation: OperationRead, Resource: resource}, func() error {
		if _, exists := c.fixture.objects[resource][key]; !exists {
			return fmt.Errorf("fixture object %s/%s is absent", resource, key)
		}
		return nil
	})
}

func (c smokeObjectClient) Delete(ctx context.Context, resource Resource, key string) error {
	return c.effect(ctx, Access{Operation: OperationDelete, Resource: resource}, func() error {
		delete(c.fixture.objects[resource], key)
		return nil
	})
}

type smokeCatalogClient struct{ smokeBoundary }

func (c smokeCatalogClient) Append(ctx context.Context, resource Resource) error {
	return c.effect(ctx, Access{Operation: OperationAppend, Resource: resource}, nil)
}

func (c smokeCatalogClient) Read(ctx context.Context, resource Resource) error {
	return c.effect(ctx, Access{Operation: OperationRead, Resource: resource}, nil)
}

func (c smokeCatalogClient) Correct(ctx context.Context, resource Resource) error {
	return c.effect(ctx, Access{Operation: OperationCorrect, Resource: resource}, nil)
}

type smokeWarehouseClient struct{ smokeBoundary }

func (c smokeWarehouseClient) Read(ctx context.Context) error {
	return c.effect(ctx, Access{Operation: OperationRead, Resource: ResourceClickHouse}, nil)
}

func (c smokeWarehouseClient) Write(ctx context.Context, resource Resource) error {
	return c.effect(ctx, Access{Operation: OperationWrite, Resource: resource}, nil)
}

type smokeMigrationClient struct{ smokeBoundary }

func (c smokeMigrationClient) DDL(ctx context.Context) error {
	return c.effect(ctx, Access{Operation: OperationDDL, Resource: ResourcePostgreSQLSchema}, nil)
}

func (c smokeMigrationClient) Lock(ctx context.Context) error {
	return c.effect(ctx, Access{Operation: OperationLock, Resource: ResourcePostgreSQLAdvisoryLock}, nil)
}

type smokeBackupClient struct{ smokeBoundary }

func (c smokeBackupClient) Snapshot(ctx context.Context) error {
	return c.effect(ctx, Access{Operation: OperationSnapshot, Resource: ResourceCallerSnapshot}, nil)
}

func (c smokeBackupClient) Version(ctx context.Context) error {
	return c.effect(ctx, Access{Operation: OperationVersion, Resource: ResourceObjectVersion}, nil)
}

type smokeRuntime struct {
	fixture   *smokeFixture
	network   smokeNetworkClient
	objects   smokeObjectClient
	catalog   smokeCatalogClient
	warehouse smokeWarehouseClient
	migration smokeMigrationClient
	backup    smokeBackupClient
}

func newSmokeRuntime(role Role) *smokeRuntime {
	fixture := newSmokeFixture()
	boundary := smokeBoundary{role: role, authorizer: NewAuthorizer(), fixture: fixture}
	return &smokeRuntime{
		fixture:   fixture,
		network:   smokeNetworkClient{smokeBoundary: boundary},
		objects:   smokeObjectClient{smokeBoundary: boundary},
		catalog:   smokeCatalogClient{smokeBoundary: boundary},
		warehouse: smokeWarehouseClient{smokeBoundary: boundary},
		migration: smokeMigrationClient{smokeBoundary: boundary},
		backup:    smokeBackupClient{smokeBoundary: boundary},
	}
}

type smokeExercise struct {
	runtime  *smokeRuntime
	evidence SmokeEvidence
}

func (s *smokeExercise) allow(name string, call func() error) error {
	before := s.runtime.fixture.effects
	if err := call(); err != nil {
		return fmt.Errorf("allowed %s lifecycle effect failed: %w", name, err)
	}
	if s.runtime.fixture.effects != before+1 {
		return fmt.Errorf("allowed %s lifecycle effect did not reach its fixture", name)
	}
	s.evidence.AllowedChecks++
	return nil
}

func (s *smokeExercise) deny(name string, overwrite bool, call func() error) error {
	before := s.runtime.fixture.effects
	if err := call(); !errors.Is(err, ErrPermissionDenied) {
		return fmt.Errorf("forbidden %s lifecycle effect returned %v", name, err)
	}
	if s.runtime.fixture.effects != before {
		return fmt.Errorf("forbidden %s lifecycle effect reached its fixture", name)
	}
	s.evidence.DeniedChecks++
	if overwrite {
		s.evidence.OverwriteDenials++
	}
	return nil
}

// Smoke executes each role's concrete lifecycle through scoped object,
// catalog, warehouse, migration, query, and verifier clients. A forbidden
// client call that reaches its fixture fails the smoke.
func Smoke(ctx context.Context, role Role) (SmokeEvidence, error) {
	if ctx == nil {
		return SmokeEvidence{}, errors.New("role smoke context is required")
	}
	if err := ctx.Err(); err != nil {
		return SmokeEvidence{}, err
	}
	if _, err := ParseRole(string(role)); err != nil {
		return SmokeEvidence{}, err
	}
	exercise := &smokeExercise{
		runtime:  newSmokeRuntime(role),
		evidence: SmokeEvidence{Format: SmokeFormat, Role: role},
	}
	if err := exercise.run(ctx, role); err != nil {
		return SmokeEvidence{}, err
	}
	if exercise.evidence.AllowedChecks == 0 || exercise.evidence.DeniedChecks == 0 {
		return SmokeEvidence{}, errors.New("role smoke omitted an allowed or denied lifecycle")
	}
	if role == RoleCollector {
		fence, err := smokeCollectorFence(ctx)
		if err != nil {
			return SmokeEvidence{}, err
		}
		exercise.evidence.WriterHandoffFence = fence
	}
	return exercise.evidence, nil
}

func (s *smokeExercise) run(ctx context.Context, role Role) error {
	switch role {
	case RoleCollector:
		if err := s.allow("collector source connect", func() error {
			return s.runtime.network.Connect(ctx, ResourcePublicMarketDataNetwork)
		}); err != nil {
			return err
		}
		if err := s.allow("collector raw create", func() error {
			return s.runtime.objects.Create(ctx, ResourceRawObjects, "segment")
		}); err != nil {
			return err
		}
		for name, resource := range map[string]Resource{
			"collector opportunity append": ResourceOpportunityLedger,
			"collector segment append":     ResourceSegmentLedger,
			"collector temporal append":    ResourceTemporalCatalog,
		} {
			if err := s.allow(name, func() error { return s.runtime.catalog.Append(ctx, resource) }); err != nil {
				return err
			}
		}
		if err := s.deny("collector raw overwrite", true, func() error {
			return s.runtime.objects.Create(ctx, ResourceRawObjects, "segment")
		}); err != nil {
			return err
		}
		return s.deny("collector raw delete", false, func() error {
			return s.runtime.objects.Delete(ctx, ResourceRawObjects, "segment")
		})
	case RoleCatalogSync:
		if err := s.allow("catalog-sync metadata connect", func() error {
			return s.runtime.network.Connect(ctx, ResourceOfficialMetadataNetwork)
		}); err != nil {
			return err
		}
		if err := s.allow("catalog-sync raw metadata create", func() error {
			return s.runtime.objects.Create(ctx, ResourceRawMetadata, "metadata")
		}); err != nil {
			return err
		}
		if err := s.allow("catalog-sync temporal append", func() error {
			return s.runtime.catalog.Append(ctx, ResourceTemporalCatalog)
		}); err != nil {
			return err
		}
		if err := s.deny("catalog-sync raw metadata overwrite", true, func() error {
			return s.runtime.objects.Create(ctx, ResourceRawMetadata, "metadata")
		}); err != nil {
			return err
		}
		return s.deny("catalog-sync collection read", false, func() error {
			return s.runtime.objects.Read(ctx, ResourceCommittedObjects, "verified")
		})
	case RoleMigrationJob:
		if err := s.allow("migration DDL", func() error { return s.runtime.migration.DDL(ctx) }); err != nil {
			return err
		}
		if err := s.allow("migration advisory lock", func() error { return s.runtime.migration.Lock(ctx) }); err != nil {
			return err
		}
		if err := s.deny("migration source access", false, func() error {
			return s.runtime.network.Connect(ctx, ResourcePublicMarketDataNetwork)
		}); err != nil {
			return err
		}
		return s.deny("migration object access", false, func() error {
			return s.runtime.objects.Read(ctx, ResourceCommittedObjects, "verified")
		})
	case RoleDatasetBuilder:
		if err := s.allow("dataset committed read", func() error {
			return s.runtime.objects.Read(ctx, ResourceCommittedObjects, "verified")
		}); err != nil {
			return err
		}
		if err := s.allow("dataset derived create", func() error {
			return s.runtime.objects.Create(ctx, ResourceDerivedObjects, "dataset")
		}); err != nil {
			return err
		}
		if err := s.allow("dataset manifest append", func() error {
			return s.runtime.catalog.Append(ctx, ResourceDatasetManifest)
		}); err != nil {
			return err
		}
		if err := s.deny("dataset derived overwrite", true, func() error {
			return s.runtime.objects.Create(ctx, ResourceDerivedObjects, "dataset")
		}); err != nil {
			return err
		}
		return s.deny("dataset derived delete", false, func() error {
			return s.runtime.objects.Delete(ctx, ResourceDerivedObjects, "dataset")
		})
	case RoleWarehouseLoader:
		if err := s.allow("warehouse parquet read", func() error {
			return s.runtime.objects.Read(ctx, ResourceCommittedParquet, "verified")
		}); err != nil {
			return err
		}
		if err := s.allow("warehouse write", func() error {
			return s.runtime.warehouse.Write(ctx, ResourceClickHouse)
		}); err != nil {
			return err
		}
		if err := s.allow("warehouse generation write", func() error {
			return s.runtime.warehouse.Write(ctx, ResourceLoadGeneration)
		}); err != nil {
			return err
		}
		return s.deny("warehouse catalog correction", false, func() error {
			return s.runtime.catalog.Correct(ctx, ResourceTemporalCatalog)
		})
	case RoleQueryReplayServer:
		if err := s.allow("query catalog read", func() error {
			return s.runtime.catalog.Read(ctx, ResourceTemporalCatalog)
		}); err != nil {
			return err
		}
		if err := s.allow("query warehouse read", func() error { return s.runtime.warehouse.Read(ctx) }); err != nil {
			return err
		}
		for name, resource := range map[string]Resource{
			"query committed object read": ResourceCommittedObjects,
			"query derived object read":   ResourceDerivedObjects,
			"query parquet read":          ResourceCommittedParquet,
		} {
			if err := s.allow(name, func() error { return s.runtime.objects.Read(ctx, resource, "verified") }); err != nil {
				return err
			}
		}
		if err := s.deny("query warehouse write", false, func() error {
			return s.runtime.warehouse.Write(ctx, ResourceClickHouse)
		}); err != nil {
			return err
		}
		return s.deny("query catalog append", false, func() error {
			return s.runtime.catalog.Append(ctx, ResourceTemporalCatalog)
		})
	case RoleVerifier:
		if err := s.allow("verifier catalog read", func() error {
			return s.runtime.catalog.Read(ctx, ResourceTemporalCatalog)
		}); err != nil {
			return err
		}
		if err := s.allow("verifier warehouse read", func() error { return s.runtime.warehouse.Read(ctx) }); err != nil {
			return err
		}
		for name, resource := range map[string]Resource{
			"verifier committed object read": ResourceCommittedObjects,
			"verifier derived object read":   ResourceDerivedObjects,
			"verifier parquet read":          ResourceCommittedParquet,
		} {
			if err := s.allow(name, func() error { return s.runtime.objects.Read(ctx, resource, "verified") }); err != nil {
				return err
			}
		}
		if err := s.allow("verifier quarantine report create", func() error {
			return s.runtime.objects.Create(ctx, ResourceQuarantineReport, "report")
		}); err != nil {
			return err
		}
		if err := s.deny("verifier catalog correction", false, func() error {
			return s.runtime.catalog.Correct(ctx, ResourceTemporalCatalog)
		}); err != nil {
			return err
		}
		if err := s.deny("verifier quarantine delete", false, func() error {
			return s.runtime.objects.Delete(ctx, ResourceQuarantineReport, "report")
		}); err != nil {
			return err
		}
		return s.deny("verifier migration DDL", false, func() error {
			return s.runtime.migration.DDL(ctx)
		})
	case RoleBackupRecovery:
		if err := s.allow("backup snapshot", func() error { return s.runtime.backup.Snapshot(ctx) }); err != nil {
			return err
		}
		if err := s.allow("backup object version", func() error { return s.runtime.backup.Version(ctx) }); err != nil {
			return err
		}
		if err := s.deny("backup object delete", false, func() error {
			return s.runtime.objects.Delete(ctx, ResourceCommittedObjects, "verified")
		}); err != nil {
			return err
		}
		return s.deny("backup catalog correction", false, func() error {
			return s.runtime.catalog.Correct(ctx, ResourceTemporalCatalog)
		})
	default:
		return fmt.Errorf("unsupported role lifecycle %q", role)
	}
}

type smokeDurableBoundary struct {
	messages []capture.RawMessage
	commits  int
}

func (b *smokeDurableBoundary) WriteRaw(ctx context.Context, message capture.RawMessage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	b.messages = append(b.messages, message)
	return nil
}

func (b *smokeDurableBoundary) FlushCommit(ctx context.Context) (capture.DurableCommit, error) {
	if err := ctx.Err(); err != nil {
		return capture.DurableCommit{}, err
	}
	if len(b.messages) == 0 {
		return capture.DurableCommit{}, errors.New("durable fixture has no staged raw message")
	}
	b.commits++
	return capture.DurableCommit{
		SegmentID:      fmt.Sprintf("segment-%d", b.commits),
		LastCoordinate: b.messages[len(b.messages)-1].Coordinate,
	}, nil
}

func smokeCollectorFence(ctx context.Context) (uint64, error) {
	manager, err := NewLeaseManager(NewMemoryLeaseStore())
	if err != nil {
		return 0, err
	}
	old, err := manager.Acquire(ctx, "synthetic/source/channel", "old-writer")
	if err != nil {
		return 0, err
	}
	durable := &smokeDurableBoundary{}
	oldBoundary, err := NewFencedDurableBoundary(manager, old, durable)
	if err != nil {
		return 0, err
	}
	if err := oldBoundary.WriteRaw(ctx, capture.RawMessage{Coordinate: "epoch/1"}); err != nil {
		return 0, err
	}
	if _, err := oldBoundary.FlushCommit(ctx); err != nil {
		return 0, err
	}
	newToken, err := manager.Handoff(ctx, old, "new-writer", func(ctx context.Context, _ LeaseToken) error {
		_, err := oldBoundary.FlushCommit(ctx)
		return err
	})
	if err != nil {
		return 0, err
	}
	commitsAfterHandoff := durable.commits
	if _, err := oldBoundary.FlushCommit(ctx); !errors.Is(err, ErrLeaseConflict) {
		return 0, errors.New("stale collector reached canonical durable boundary")
	}
	if durable.commits != commitsAfterHandoff {
		return 0, errors.New("stale collector mutated canonical durable fixture")
	}
	newBoundary, err := NewFencedDurableBoundary(manager, newToken, durable)
	if err != nil {
		return 0, err
	}
	if _, err := newBoundary.FlushCommit(ctx); err != nil {
		return 0, err
	}
	return newToken.Fence, nil
}

func WriteSmokeEvidence(output io.Writer, evidence SmokeEvidence) error {
	if output == nil {
		return errors.New("smoke evidence output is required")
	}
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(evidence); err != nil {
		return fmt.Errorf("encoding role smoke evidence: %w", err)
	}
	return nil
}
