package normalize

import (
	"fmt"
	"strings"
)

const (
	SourceHealthSchemaName           = "SourceHealthV1"
	SourceHealthSchemaVersion uint16 = 1
	MaxHealthIdentityBytes           = 256
	MaxHealthDetailBytes             = 4096
)

type HealthDimension string

const (
	HealthDNS                 HealthDimension = "dns"
	HealthTLS                 HealthDimension = "tls"
	HealthConnect             HealthDimension = "connect"
	HealthAuth                HealthDimension = "auth"
	HealthSubscriptionAck     HealthDimension = "subscription_ack"
	HealthPing                HealthDimension = "ping"
	HealthPong                HealthDimension = "pong"
	HealthUsefulDataSilence   HealthDimension = "useful_data_silence"
	HealthChannelMessageRate  HealthDimension = "channel_message_rate"
	HealthChannelByteRate     HealthDimension = "channel_byte_rate"
	HealthChannelInterArrival HealthDimension = "channel_inter_arrival"
	HealthExchangeLag         HealthDimension = "exchange_lag"
	HealthClock               HealthDimension = "clock"
	HealthSequence            HealthDimension = "sequence"
	HealthSnapshot            HealthDimension = "snapshot"
	HealthChecksum            HealthDimension = "checksum"
	HealthReconstruction      HealthDimension = "reconstruction"
	HealthDecode              HealthDimension = "decode"
	HealthSchema              HealthDimension = "schema"
	HealthRESTLatency         HealthDimension = "rest_latency"
	HealthRESTStatus          HealthDimension = "rest_status"
	HealthRESTTimeout         HealthDimension = "rest_timeout"
	HealthRESTWeight          HealthDimension = "rest_weight"
	HealthRESTBan             HealthDimension = "rest_ban"
	HealthWriterQueue         HealthDimension = "writer_queue"
	HealthSpool               HealthDimension = "spool"
	HealthCommit              HealthDimension = "commit"
	HealthCatalogAge          HealthDimension = "catalog_age"
	HealthCatalogAssociation  HealthDimension = "catalog_association"
	HealthParquetLag          HealthDimension = "parquet_lag"
	HealthClickHouseLag       HealthDimension = "clickhouse_lag"
	HealthReplayPressure      HealthDimension = "replay_pressure"
	HealthQueryPressure       HealthDimension = "query_pressure"
)

func validHealthDimension(dimension HealthDimension) bool {
	switch dimension {
	case HealthDNS, HealthTLS, HealthConnect, HealthAuth, HealthSubscriptionAck, HealthPing, HealthPong,
		HealthUsefulDataSilence, HealthChannelMessageRate, HealthChannelByteRate, HealthChannelInterArrival,
		HealthExchangeLag, HealthClock, HealthSequence, HealthSnapshot, HealthChecksum, HealthReconstruction,
		HealthDecode, HealthSchema, HealthRESTLatency, HealthRESTStatus, HealthRESTTimeout, HealthRESTWeight,
		HealthRESTBan, HealthWriterQueue, HealthSpool, HealthCommit, HealthCatalogAge, HealthCatalogAssociation,
		HealthParquetLag, HealthClickHouseLag, HealthReplayPressure, HealthQueryPressure:
		return true
	default:
		return false
	}
}

type HealthScope string

const (
	HealthScopeSource     HealthScope = "source"
	HealthScopeChannel    HealthScope = "channel"
	HealthScopeInstrument HealthScope = "instrument"
	HealthScopeWriter     HealthScope = "writer"
	HealthScopeDataset    HealthScope = "dataset"
	HealthScopeService    HealthScope = "service"
)

type HealthStatus string

const (
	HealthStatusUnknown      HealthStatus = "unknown"
	HealthStatusUp           HealthStatus = "up"
	HealthStatusDown         HealthStatus = "down"
	HealthStatusPending      HealthStatus = "pending"
	HealthStatusAcknowledged HealthStatus = "acknowledged"
	HealthStatusTimedOut     HealthStatus = "timed_out"
	HealthStatusWithinLimit  HealthStatus = "within_limit"
	HealthStatusRateLimited  HealthStatus = "rate_limited"
	HealthStatusBanned       HealthStatus = "banned"
	HealthStatusCompatible   HealthStatus = "compatible"
	HealthStatusIncompatible HealthStatus = "incompatible"
	HealthStatusContiguous   HealthStatus = "contiguous"
	HealthStatusGap          HealthStatus = "gap"
	HealthStatusValid        HealthStatus = "valid"
	HealthStatusInvalid      HealthStatus = "invalid"
	HealthStatusAvailable    HealthStatus = "available"
	HealthStatusUnavailable  HealthStatus = "unavailable"
	HealthStatusCurrent      HealthStatus = "current"
	HealthStatusStale        HealthStatus = "stale"
	HealthStatusAssociated   HealthStatus = "associated"
	HealthStatusUnassociated HealthStatus = "unassociated"
	HealthStatusIdle         HealthStatus = "idle"
	HealthStatusPressured    HealthStatus = "pressured"
	HealthStatusQueued       HealthStatus = "queued"
	HealthStatusCommitted    HealthStatus = "committed"
)

func validHealthStatus(status HealthStatus) bool {
	switch status {
	case HealthStatusUnknown, HealthStatusUp, HealthStatusDown, HealthStatusPending, HealthStatusAcknowledged,
		HealthStatusTimedOut, HealthStatusWithinLimit, HealthStatusRateLimited, HealthStatusBanned,
		HealthStatusCompatible, HealthStatusIncompatible, HealthStatusContiguous, HealthStatusGap,
		HealthStatusValid, HealthStatusInvalid, HealthStatusAvailable, HealthStatusUnavailable,
		HealthStatusCurrent, HealthStatusStale, HealthStatusAssociated, HealthStatusUnassociated,
		HealthStatusIdle, HealthStatusPressured, HealthStatusQueued, HealthStatusCommitted:
		return true
	default:
		return false
	}
}

type HealthStatusField struct {
	State      SourceState
	Value      HealthStatus
	Provenance FieldProvenance
}

func (f HealthStatusField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid health-status source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue {
		if f.Value != "" {
			return fmt.Errorf("%w: unavailable health status has a value", ErrInvalidNormalized)
		}
		return nil
	}
	if !validHealthStatus(f.Value) {
		return fmt.Errorf("%w: unknown health status", ErrInvalidNormalized)
	}
	return nil
}

type HealthUnit string

const (
	HealthUnitCount             HealthUnit = "count"
	HealthUnitBytes             HealthUnit = "bytes"
	HealthUnitNanoseconds       HealthUnit = "nanoseconds"
	HealthUnitRatio             HealthUnit = "ratio"
	HealthUnitStatusCode        HealthUnit = "status_code"
	HealthUnitWeight            HealthUnit = "weight"
	HealthUnitMessagesPerSecond HealthUnit = "messages_per_second"
	HealthUnitBytesPerSecond    HealthUnit = "bytes_per_second"
	HealthUnitUnixNanoseconds   HealthUnit = "unix_nanoseconds"
)

type HealthMeasurement struct {
	Decimal Decimal
	Unit    HealthUnit
}

func (m HealthMeasurement) Validate() error {
	if err := m.Decimal.Validate(); err != nil {
		return err
	}
	switch m.Unit {
	case HealthUnitCount, HealthUnitBytes, HealthUnitNanoseconds, HealthUnitRatio, HealthUnitStatusCode,
		HealthUnitWeight, HealthUnitMessagesPerSecond, HealthUnitBytesPerSecond, HealthUnitUnixNanoseconds:
		return nil
	default:
		return fmt.Errorf("%w: unknown health measurement unit", ErrInvalidNormalized)
	}
}

type HealthMeasurementField struct {
	State      SourceState
	Value      HealthMeasurement
	Provenance FieldProvenance
}

func (f HealthMeasurementField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid health-measurement source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue {
		if f.Value != (HealthMeasurement{}) {
			return fmt.Errorf("%w: unavailable health measurement has a value", ErrInvalidNormalized)
		}
		return nil
	}
	return f.Value.Validate()
}

type HealthTextField struct {
	State      SourceState
	Value      string
	Provenance FieldProvenance
}

func (f HealthTextField) Validate() error {
	if !validFieldState(f.State) {
		return fmt.Errorf("%w: invalid health-text source state", ErrInvalidNormalized)
	}
	if err := f.Provenance.Validate(f.State != SourceMissing); err != nil {
		return err
	}
	if f.State != SourceValue {
		if f.Value != "" {
			return fmt.Errorf("%w: unavailable health text has a value", ErrInvalidNormalized)
		}
		return nil
	}
	if f.Value == "" || len(f.Value) > MaxHealthDetailBytes || strings.IndexByte(f.Value, 0) >= 0 {
		return fmt.Errorf("%w: invalid health text value", ErrInvalidNormalized)
	}
	return nil
}

// SourceHealthV1 records one operational state transition. InstrumentUID in
// common metadata is optional only for this source-level family.
type SourceHealthV1 struct {
	Metadata            Metadata
	Dimension           HealthDimension
	Scope               HealthScope
	Component           string
	NativeRole          string
	PreviousStatus      HealthStatusField
	CurrentStatus       HealthStatusField
	NativePreviousState HealthTextField
	NativeCurrentState  HealthTextField
	PreviousMeasurement HealthMeasurementField
	CurrentMeasurement  HealthMeasurementField
	WindowStart         TimeField
	WindowEnd           TimeField
	NativeCode          HealthTextField
	Detail              HealthTextField
}

func (e SourceHealthV1) Validate() error {
	if err := validateSchema(e.Metadata, SourceHealthSchemaName, SourceHealthSchemaVersion); err != nil {
		return err
	}
	if !validHealthDimension(e.Dimension) {
		return fmt.Errorf("%w: unknown source-health dimension", ErrInvalidNormalized)
	}
	switch e.Scope {
	case HealthScopeSource, HealthScopeChannel, HealthScopeInstrument, HealthScopeWriter, HealthScopeDataset, HealthScopeService:
	default:
		return fmt.Errorf("%w: unknown source-health scope", ErrInvalidNormalized)
	}
	for _, identity := range []string{e.Component, e.NativeRole} {
		if identity == "" || len(identity) > MaxHealthIdentityBytes || strings.IndexByte(identity, 0) >= 0 {
			return fmt.Errorf("%w: invalid source-health identity", ErrInvalidNormalized)
		}
	}
	if e.Scope == HealthScopeInstrument && e.Metadata.InstrumentUID == "" {
		return fmt.Errorf("%w: instrument-scoped health lacks instrument identity", ErrInvalidNormalized)
	}
	if err := e.PreviousStatus.Validate(); err != nil {
		return err
	}
	if err := e.CurrentStatus.Validate(); err != nil || e.CurrentStatus.State != SourceValue {
		return fmt.Errorf("%w: source-health current status is unavailable", ErrInvalidNormalized)
	}
	for _, field := range []HealthTextField{e.NativePreviousState, e.NativeCurrentState, e.NativeCode, e.Detail} {
		if err := field.Validate(); err != nil {
			return err
		}
	}
	if err := e.PreviousMeasurement.Validate(); err != nil {
		return err
	}
	if err := e.CurrentMeasurement.Validate(); err != nil {
		return err
	}
	if err := e.WindowStart.Validate(); err != nil {
		return err
	}
	if err := e.WindowEnd.Validate(); err != nil {
		return err
	}
	if (e.WindowStart.State == SourceValue) != (e.WindowEnd.State == SourceValue) {
		return fmt.Errorf("%w: health observation window bounds mismatch", ErrInvalidNormalized)
	}
	if e.WindowStart.State == SourceValue && e.WindowEnd.ValueNS < e.WindowStart.ValueNS {
		return fmt.Errorf("%w: health observation window is reversed", ErrInvalidNormalized)
	}
	statusChanged := e.PreviousStatus.State != SourceValue || e.PreviousStatus.Value != e.CurrentStatus.Value
	nativeChanged := e.NativePreviousState.State != e.NativeCurrentState.State || e.NativePreviousState.Value != e.NativeCurrentState.Value
	measurementChanged := e.PreviousMeasurement.State != e.CurrentMeasurement.State || e.PreviousMeasurement.Value != e.CurrentMeasurement.Value
	if !statusChanged && !nativeChanged && !measurementChanged {
		return fmt.Errorf("%w: source-health event contains no state change", ErrInvalidNormalized)
	}
	return nil
}
