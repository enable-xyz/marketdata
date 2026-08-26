package deployment

import (
	"errors"
	"testing"
)

func TestPermissionDenial(t *testing.T) {
	authorizer := NewAuthorizer()
	for _, role := range Roles() {
		evidence, err := Smoke(t.Context(), role)
		if err != nil {
			t.Fatalf("Smoke(%s) error = %v", role, err)
		}
		if evidence.AllowedChecks == 0 || evidence.DeniedChecks == 0 {
			t.Fatalf("Smoke(%s) did not exercise both allow and deny paths: %+v", role, evidence)
		}
	}

	for _, denied := range []struct {
		role   Role
		access Access
	}{
		{RoleCollector, Access{Operation: OperationCreate, Resource: ResourceRawObjects, Existing: true}},
		{RoleCollector, Access{Operation: OperationDelete, Resource: ResourceRawObjects}},
		{RoleQueryReplayServer, Access{Operation: OperationWrite, Resource: ResourceClickHouse}},
		{RoleQueryReplayServer, Access{Operation: OperationAppend, Resource: ResourceTemporalCatalog}},
		{RoleVerifier, Access{Operation: OperationCorrect, Resource: ResourceTemporalCatalog}},
		{RoleVerifier, Access{Operation: OperationDelete, Resource: ResourceQuarantineReport}},
		{RoleMigrationJob, Access{Operation: OperationConnect, Resource: ResourcePublicMarketDataNetwork}},
		{RoleMigrationJob, Access{Operation: OperationRead, Resource: ResourceRawObjects}},
	} {
		if err := authorizer.Authorize(denied.role, denied.access); !errors.Is(err, ErrPermissionDenied) {
			t.Errorf("Authorize(%s, %+v) error = %v, want permission denial", denied.role, denied.access, err)
		}
	}
}

func TestSmokeFailsWhenScopedClientPermitsForbiddenEffect(t *testing.T) {
	forbidden := Access{Operation: OperationCorrect, Resource: ResourceTemporalCatalog}
	rolePermissions[RoleVerifier][forbidden] = struct{}{}
	defer delete(rolePermissions[RoleVerifier], forbidden)

	if _, err := Smoke(t.Context(), RoleVerifier); err == nil {
		t.Fatal("verifier smoke passed after its scoped catalog client permitted correction")
	}
}

func TestVerifierDenialsArePreEffect(t *testing.T) {
	runtime := newSmokeRuntime(RoleVerifier)
	before := runtime.fixture.effects
	if err := runtime.catalog.Correct(t.Context(), ResourceTemporalCatalog); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("verifier correction error = %v, want permission denial", err)
	}
	if err := runtime.objects.Delete(t.Context(), ResourceQuarantineReport, "report"); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("verifier delete error = %v, want permission denial", err)
	}
	if err := runtime.migration.DDL(t.Context()); !errors.Is(err, ErrPermissionDenied) {
		t.Fatalf("verifier DDL error = %v, want permission denial", err)
	}
	if runtime.fixture.effects != before {
		t.Fatalf("forbidden verifier calls reached fixtures: effects = %d, want %d", runtime.fixture.effects, before)
	}
}
