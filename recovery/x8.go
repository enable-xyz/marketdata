package recovery

import (
	"errors"
	"fmt"
	"math/bits"
	"slices"
	"strings"
	"time"
)

const ProjectionDays uint16 = 30

var ErrInvalidMeasurement = errors.New("recovery: invalid X8 measurement")

type CapabilityState string

const (
	CapabilitySupported   CapabilityState = "supported"
	CapabilityUnsupported CapabilityState = "unsupported"
	CapabilityUnknown     CapabilityState = "unknown"
)

type ProviderCapabilities struct {
	ProviderID   string
	Versioning   CapabilityState
	Immutability CapabilityState
	Replication  CapabilityState
	Evidence     string
}

type StorageMeasurement struct {
	SourceID       string
	Family         string
	ObservedBytes  uint64
	ObservedWindow time.Duration
}

type CapacityProjection struct {
	SourceID       string
	Family         string
	ObservedBytes  uint64
	ObservedWindow time.Duration
	ProjectedBytes uint64
}

type X8Measurement struct {
	Storage                    []StorageMeasurement
	BackupDuration             time.Duration
	RestoreDuration            time.Duration
	RebuildDuration            time.Duration
	Provider                   ProviderCapabilities
	CallerRecoveryRequirements []string
}

type UnresolvedDecision struct {
	Status string
	Reason string
}

type X8Report struct {
	ProjectionDays             uint16
	Capacity                   []CapacityProjection
	ProjectedBytesTotal        uint64
	BackupDuration             time.Duration
	RestoreDuration            time.Duration
	RebuildDuration            time.Duration
	Provider                   ProviderCapabilities
	CallerRecoveryRequirements []string
	RPO                        UnresolvedDecision
	RTO                        UnresolvedDecision
	RawRetention               UnresolvedDecision
	DeletionPolicy             UnresolvedDecision
	RawDeletionAuthorized      bool
}

// MeasureX8 projects exactly 30 days from observed byte windows and records
// caller/provider inputs. It deliberately cannot freeze RPO, RTO, retention or
// deletion authority; those decisions require the caller's deployment policy
// after this evidence exists.
func MeasureX8(input X8Measurement) (X8Report, error) {
	if len(input.Storage) == 0 || input.BackupDuration <= 0 || input.RestoreDuration <= 0 || input.RebuildDuration <= 0 {
		return X8Report{}, fmt.Errorf("%w: storage and positive observed durations are required", ErrInvalidMeasurement)
	}
	if err := validateProvider(input.Provider); err != nil {
		return X8Report{}, err
	}
	capacity := make([]CapacityProjection, len(input.Storage))
	var total uint64
	seenStorage := make(map[[2]string]struct{}, len(input.Storage))
	for i, sample := range input.Storage {
		if sample.SourceID == "" || sample.Family == "" || sample.ObservedBytes == 0 || sample.ObservedWindow <= 0 ||
			len(sample.SourceID) > 256 || len(sample.Family) > 256 || strings.IndexByte(sample.SourceID, 0) >= 0 || strings.IndexByte(sample.Family, 0) >= 0 {
			return X8Report{}, fmt.Errorf("%w: incomplete storage sample", ErrInvalidMeasurement)
		}
		key := [2]string{sample.SourceID, sample.Family}
		if _, duplicate := seenStorage[key]; duplicate {
			return X8Report{}, fmt.Errorf("%w: duplicate source/family storage sample", ErrInvalidMeasurement)
		}
		seenStorage[key] = struct{}{}
		projected, err := projectBytes(sample.ObservedBytes, sample.ObservedWindow, 30*24*time.Hour)
		if err != nil {
			return X8Report{}, err
		}
		if ^uint64(0)-total < projected {
			return X8Report{}, fmt.Errorf("%w: projected capacity overflow", ErrInvalidMeasurement)
		}
		total += projected
		capacity[i] = CapacityProjection{SourceID: sample.SourceID, Family: sample.Family,
			ObservedBytes: sample.ObservedBytes, ObservedWindow: sample.ObservedWindow, ProjectedBytes: projected}
	}
	slices.SortFunc(capacity, func(left, right CapacityProjection) int {
		if order := strings.Compare(left.SourceID, right.SourceID); order != 0 {
			return order
		}
		return strings.Compare(left.Family, right.Family)
	})
	requirements := slices.Clone(input.CallerRecoveryRequirements)
	for _, requirement := range requirements {
		if strings.TrimSpace(requirement) == "" || len(requirement) > 4096 || strings.IndexByte(requirement, 0) >= 0 {
			return X8Report{}, fmt.Errorf("%w: invalid caller recovery requirement", ErrInvalidMeasurement)
		}
	}
	slices.Sort(requirements)
	decisionReason := "unresolved caller decision pending measured report and deployment policy"
	return X8Report{
		ProjectionDays: ProjectionDays, Capacity: capacity, ProjectedBytesTotal: total,
		BackupDuration: input.BackupDuration, RestoreDuration: input.RestoreDuration, RebuildDuration: input.RebuildDuration,
		Provider: input.Provider, CallerRecoveryRequirements: requirements,
		RPO: UnresolvedDecision{Status: "unresolved", Reason: decisionReason},
		RTO: UnresolvedDecision{Status: "unresolved", Reason: decisionReason},
		RawRetention: UnresolvedDecision{Status: "default_indefinite",
			Reason: "canonical raw and manifests remain retained indefinitely unless an explicit caller policy is later authorized"},
		DeletionPolicy: UnresolvedDecision{Status: "unresolved",
			Reason: "raw deletion requires explicit caller authority and replay-impact proof"},
		RawDeletionAuthorized: false,
	}, nil
}

func validateProvider(provider ProviderCapabilities) error {
	if strings.TrimSpace(provider.ProviderID) == "" || len(provider.ProviderID) > 256 ||
		strings.TrimSpace(provider.Evidence) == "" || len(provider.Evidence) > 4096 || strings.IndexByte(provider.Evidence, 0) >= 0 {
		return fmt.Errorf("%w: provider identity and capability evidence are required", ErrInvalidMeasurement)
	}
	for _, state := range []CapabilityState{provider.Versioning, provider.Immutability, provider.Replication} {
		if state != CapabilitySupported && state != CapabilityUnsupported && state != CapabilityUnknown {
			return fmt.Errorf("%w: unknown provider capability state %q", ErrInvalidMeasurement, state)
		}
	}
	return nil
}

func projectBytes(observed uint64, observedWindow, projectionWindow time.Duration) (uint64, error) {
	window := uint64(observedWindow)
	projection := uint64(projectionWindow)
	whole := projection / window
	remainder := projection % window
	if whole != 0 && observed > ^uint64(0)/whole {
		return 0, fmt.Errorf("%w: projected capacity overflow", ErrInvalidMeasurement)
	}
	projected := observed * whole
	high, low := bits.Mul64(observed, remainder)
	partial, partialRemainder := bits.Div64(high, low, window)
	if partialRemainder != 0 {
		if partial == ^uint64(0) {
			return 0, fmt.Errorf("%w: projected capacity overflow", ErrInvalidMeasurement)
		}
		partial++
	}
	if ^uint64(0)-projected < partial {
		return 0, fmt.Errorf("%w: projected capacity overflow", ErrInvalidMeasurement)
	}
	return projected + partial, nil
}
