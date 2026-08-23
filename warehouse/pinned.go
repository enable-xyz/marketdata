package warehouse

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
)

const (
	PinnedX5ServerDigest                 = "clickhouse/clickhouse-server@sha256:7c39abeb161d627fa3ca6a1e5f6241ecdc24501e8463486e61b80be3ab4471b0"
	PinnedX5SelectionRule                = "minimum max_ingest_duration_ns among cases preserving exact event/query invariants, plus a live selected-variant 100-point reconciliation gate"
	PinnedX5MaxIngestDurationNS    int64 = 339_838_668_229
	PinnedX5ExpectedEventSetSHA256       = "0c5eb1f842ce28cb1a6c5baf1d16243ccd2b94a6972a6425da23b0f78f55251f"
)

//go:embed testdata/x5-fixture.json
var pinnedX5FixtureJSON []byte

type X5ProductionSelection struct {
	ServerDigest           string
	Config                 Config
	SelectionRule          string
	MaxIngestDurationNS    int64
	ExpectedEventSetSHA256 string
}

func PinnedX5ProductionSelection() X5ProductionSelection {
	return X5ProductionSelection{
		ServerDigest:           PinnedX5ServerDigest,
		Config:                 Config{BatchRows: 100_000, Compression: CompressionLZ4, Layout: PartitionMonth},
		SelectionRule:          PinnedX5SelectionRule,
		MaxIngestDurationNS:    PinnedX5MaxIngestDurationNS,
		ExpectedEventSetSHA256: PinnedX5ExpectedEventSetSHA256,
	}
}

func ReadPinnedX5Fixture() (X5Fixture, error) {
	fixture, err := ReadX5Fixture(bytes.NewReader(pinnedX5FixtureJSON))
	if err != nil {
		return X5Fixture{}, fmt.Errorf("warehouse: read pinned X5 fixture: %w", err)
	}
	selection := PinnedX5ProductionSelection()
	if fixture.ServerDigest != selection.ServerDigest {
		return X5Fixture{}, fmt.Errorf("%w: pinned X5 server digest", ErrGenerationConflict)
	}
	pinnedVariant := X5Variant{BatchRows: selection.Config.BatchRows, Compression: selection.Config.Compression,
		Layout: selection.Config.Layout}
	// The immutable v1 fixture honestly retains its legacy 1,000-row,
	// single-manifest disconnect evidence. A future fixture with an explicit
	// fault variant must agree with the pinned selection; the legacy zero value
	// is never relabeled and is superseded by fresh live gate evidence.
	if fixture.DisconnectVariant != (X5Variant{}) && fixture.DisconnectVariant != pinnedVariant {
		return X5Fixture{}, fmt.Errorf("%w: pinned X5 disconnect variant", ErrGenerationConflict)
	}
	fastest, err := fastestInvariantX5Case(fixture)
	if err != nil {
		return X5Fixture{}, err
	}
	if fastest.Variant != pinnedVariant || fastest.MaxIngestDurationNS != selection.MaxIngestDurationNS ||
		fastest.ExpectedEventSet != selection.ExpectedEventSetSHA256 {
		return X5Fixture{}, fmt.Errorf("%w: pinned X5 production selection no longer matches measured evidence", ErrGenerationConflict)
	}
	return fixture, nil
}

func VerifyPinnedX5Budgets(ctx context.Context, config X5RunConfig) error {
	selection := PinnedX5ProductionSelection()
	normalized, err := (Config{BatchRows: config.Native.BatchRows, Compression: config.Native.Compression,
		Layout: config.Native.Layout}).normalized()
	if err != nil {
		return err
	}
	pinnedVariant := X5Variant{BatchRows: selection.Config.BatchRows, Compression: selection.Config.Compression,
		Layout: selection.Config.Layout}
	if config.Native.ServerDigest != selection.ServerDigest || normalized != selection.Config ||
		config.DisconnectVariant != pinnedVariant {
		return fmt.Errorf("%w: pinned X5 server digest, production configuration, or disconnect variant", ErrGenerationConflict)
	}
	fixture, err := ReadPinnedX5Fixture()
	if err != nil {
		return err
	}
	return VerifyX5Budgets(ctx, config, fixture)
}

func fastestInvariantX5Case(fixture X5Fixture) (X5CaseBudget, error) {
	if err := validateFixture(fixture); err != nil {
		return X5CaseBudget{}, err
	}
	baseline := fixture.Cases[0]
	var fastest X5CaseBudget
	for _, candidate := range fixture.Cases {
		if candidate.ExpectedEventCount != baseline.ExpectedEventCount || candidate.ExpectedRowCount != baseline.ExpectedRowCount ||
			candidate.ExpectedEventSet != baseline.ExpectedEventSet || !sameX5QueryResults(candidate.Queries, baseline.Queries) {
			continue
		}
		if fastest.MaxIngestDurationNS == 0 || candidate.MaxIngestDurationNS < fastest.MaxIngestDurationNS {
			fastest = candidate
		}
	}
	if fastest.MaxIngestDurationNS == 0 {
		return X5CaseBudget{}, fmt.Errorf("%w: no X5 case preserves exact invariants", ErrMeasuredResultRequired)
	}
	return fastest, nil
}

func sameX5QueryResults(left, right []QueryBudget) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Name != right[i].Name || left[i].ExpectedSHA256 != right[i].ExpectedSHA256 ||
			left[i].MaxResponseRows != right[i].MaxResponseRows || left[i].MaxResponseBytes != right[i].MaxResponseBytes {
			return false
		}
	}
	return true
}
