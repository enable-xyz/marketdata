package capture

import (
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	SourceContractVersion  uint16 = 1
	MaxDocumentationRefs          = 16
	MaxCapabilities               = 128
	MaxStatusCodes                = 32
	MaxFixtureIdentities          = 64
	MaxContractTextBytes          = 512
	MaxSchemaDepth                = 64
	MaxSchemaFields               = 4096
	MaxSchemaArrayElements        = 1 << 20
)

var ErrInvalidSourceContract = errors.New("capture: invalid source contract")

type SourceContractError struct {
	Field   string
	Problem string
}

func (e *SourceContractError) Error() string {
	return fmt.Sprintf("capture: invalid source contract %s: %s", e.Field, e.Problem)
}

func (e *SourceContractError) Unwrap() error { return ErrInvalidSourceContract }

type RuleAuthority uint8

const (
	RuleOfficialDocumentation RuleAuthority = iota + 1
	RuleAdapterPolicyInference
)

type DocumentationRef struct {
	URL          string
	AccessedAtNS int64
	Authority    RuleAuthority
}

type TransportKind uint8

const (
	TransportWebSocket TransportKind = iota + 1
	TransportREST
)

type ConnectionTopology struct {
	Transport              TransportKind
	MaxConnections         uint16
	MaxSubscriptions       uint16
	MaxSubscriptionsPerACK uint16
	Throttleable           bool
}

type ACKMode uint8

const (
	ACKNone ACKMode = iota + 1
	ACKExact
)

type SubscriptionPolicy struct {
	ACKMode       ACKMode
	ACKTimeoutNS  uint64
	MaxPendingACK uint16
}

type HeartbeatMode uint8

const (
	HeartbeatNone HeartbeatMode = iota + 1
	HeartbeatPingPong
	HeartbeatServerMessage
	HeartbeatTestResponse
)

type HeartbeatPolicy struct {
	Mode              HeartbeatMode
	IntervalNS        uint64
	TimeoutNS         uint64
	MinimumIntervalNS uint64
}

// UsefulDataPolicy is independent of transport heartbeat health. A zero
// MaxSilenceNS declares stochastic arrivals and disables useful-data deadlines.
// A nonzero value is the documented maximum tolerated gap between useful data
// messages.
type UsefulDataPolicy struct {
	MaxSilenceNS uint64
}

type RatePolicy struct {
	Capacity             uint32
	RefillTokens         uint32
	RefillIntervalNS     uint64
	ConnectionCost       uint32
	RequestCost          uint32
	MaxAttempts          uint16
	DefaultRetryAfterNS  uint64
	MaxRetryAfterNS      uint64
	CircuitOpenNS        uint64
	RetryableStatusCodes []int
	Retryable5XX         bool
	TerminalStatusCodes  []int
	CircuitStatusCodes   []int
}

type PayloadPolicy struct {
	MaxRawBytes      uint32
	MaxSchemaDepth   uint16
	MaxSchemaFields  uint32
	MaxArrayElements uint32
}

type SupportLevel uint8

const (
	SupportAvailable SupportLevel = iota + 1
	SupportUnsupported
	SupportAmbiguous
)

type Capability struct {
	ChannelOrEndpoint string
	DataFamily        string
	Entitlement       string
	Support           SupportLevel
	Declaration       string
}

type FixtureProvenance uint8

const (
	FixtureSynthetic FixtureProvenance = iota + 1
	FixturePrimarySource
)

type FixtureIdentity struct {
	ID               string
	SHA256           [32]byte
	ByteLength       uint32
	Provenance       FixtureProvenance
	SourceReference  string
	LicenseReference string
	AccessedAtNS     int64
}

// SourceContract is the versioned, bounded source behavior consumed by Runner.
// It contains declarations only; credentials and transport destinations do not
// belong at this boundary.
type SourceContract struct {
	Version           uint16
	SourceID          string
	ContractID        string
	APIVersion        string
	Documentation     []DocumentationRef
	Capabilities      []Capability
	Topology          ConnectionTopology
	Subscription      SubscriptionPolicy
	Heartbeat         HeartbeatPolicy
	UsefulData        UsefulDataPolicy
	Rate              RatePolicy
	Payload           PayloadPolicy
	FixtureIdentities []FixtureIdentity
}

func (c SourceContract) Validate() error {
	if c.Version != SourceContractVersion {
		return sourceContractError("version", fmt.Sprintf("got %d, want %d", c.Version, SourceContractVersion))
	}
	if err := validateContractText("source_id", c.SourceID, MaxSourceIDBytes); err != nil {
		return err
	}
	if err := validateContractText("contract_id", c.ContractID, MaxContractIDBytes); err != nil {
		return err
	}
	if err := validateContractText("api_version", c.APIVersion, MaxIdentityBytes); err != nil {
		return err
	}
	if len(c.Documentation) == 0 || len(c.Documentation) > MaxDocumentationRefs {
		return sourceContractError("documentation", fmt.Sprintf("must contain 1..%d references", MaxDocumentationRefs))
	}
	for i, ref := range c.Documentation {
		if err := validateDocumentationRef(ref); err != nil {
			return sourceContractError(fmt.Sprintf("documentation[%d]", i), err.Error())
		}
	}
	if len(c.Capabilities) == 0 || len(c.Capabilities) > MaxCapabilities {
		return sourceContractError("capabilities", fmt.Sprintf("must contain 1..%d declarations", MaxCapabilities))
	}
	seenCapabilities := make(map[string]struct{}, len(c.Capabilities))
	for i, capability := range c.Capabilities {
		if err := validateCapability(capability); err != nil {
			return sourceContractError(fmt.Sprintf("capabilities[%d]", i), err.Error())
		}
		key := capability.ChannelOrEndpoint + "\x00" + capability.DataFamily
		if _, ok := seenCapabilities[key]; ok {
			return sourceContractError(fmt.Sprintf("capabilities[%d]", i), "duplicates channel/data family declaration")
		}
		seenCapabilities[key] = struct{}{}
	}
	if err := validateTopology(c.Topology, c.Subscription, c.Heartbeat, c.UsefulData); err != nil {
		return err
	}
	if err := validateRatePolicy(c.Rate); err != nil {
		return err
	}
	if err := validatePayloadPolicy(c.Payload); err != nil {
		return err
	}
	if len(c.FixtureIdentities) > MaxFixtureIdentities {
		return sourceContractError("fixture_identities", fmt.Sprintf("has %d entries, maximum is %d", len(c.FixtureIdentities), MaxFixtureIdentities))
	}
	seenFixtures := make(map[string]struct{}, len(c.FixtureIdentities))
	for i, fixture := range c.FixtureIdentities {
		if err := validateFixtureIdentity(fixture); err != nil {
			return sourceContractError(fmt.Sprintf("fixture_identities[%d]", i), err.Error())
		}
		if _, ok := seenFixtures[fixture.ID]; ok {
			return sourceContractError(fmt.Sprintf("fixture_identities[%d]", i), "duplicates fixture identity")
		}
		seenFixtures[fixture.ID] = struct{}{}
	}
	return nil
}

func (c SourceContract) Capability(channelOrEndpoint, dataFamily string) (Capability, bool) {
	for _, capability := range c.Capabilities {
		if capability.ChannelOrEndpoint == channelOrEndpoint && capability.DataFamily == dataFamily {
			return capability, true
		}
	}
	return Capability{}, false
}

func validateDocumentationRef(ref DocumentationRef) error {
	if ref.Authority != RuleOfficialDocumentation && ref.Authority != RuleAdapterPolicyInference {
		return errors.New("has invalid rule authority")
	}
	if ref.AccessedAtNS <= 0 {
		return errors.New("requires a positive access time")
	}
	if len(ref.URL) > MaxContractTextBytes || !utf8.ValidString(ref.URL) {
		return errors.New("URL is invalid or too long")
	}
	parsed, err := url.Parse(ref.URL)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return errors.New("URL must be an absolute HTTPS reference")
	}
	if parsed.User != nil {
		return errors.New("URL must not contain credentials")
	}
	return nil
}

func validateCapability(capability Capability) error {
	if err := validateContractText("channel_or_endpoint", capability.ChannelOrEndpoint, MaxContractIDBytes); err != nil {
		return err
	}
	if err := validateContractText("data_family", capability.DataFamily, MaxIdentityBytes); err != nil {
		return err
	}
	if err := validateContractText("entitlement", capability.Entitlement, MaxIdentityBytes); err != nil {
		return err
	}
	if capability.Support < SupportAvailable || capability.Support > SupportAmbiguous {
		return errors.New("support has an invalid value")
	}
	if capability.Support == SupportAvailable && capability.Declaration != "" {
		return errors.New("available capability must not carry an unsupported/ambiguity declaration")
	}
	if capability.Support != SupportAvailable {
		if err := validateContractText("declaration", capability.Declaration, MaxContractTextBytes); err != nil {
			return err
		}
	}
	return nil
}

func validateTopology(topology ConnectionTopology, subscription SubscriptionPolicy, heartbeat HeartbeatPolicy, usefulData UsefulDataPolicy) error {
	if topology.Transport != TransportWebSocket && topology.Transport != TransportREST {
		return sourceContractError("topology.transport", "has an invalid value")
	}
	if topology.MaxConnections == 0 {
		return sourceContractError("topology.max_connections", "must be positive")
	}
	if topology.Transport == TransportREST {
		if topology.MaxSubscriptions != 0 || topology.MaxSubscriptionsPerACK != 0 || subscription != (SubscriptionPolicy{}) ||
			heartbeat != (HeartbeatPolicy{}) || usefulData != (UsefulDataPolicy{}) {
			return sourceContractError("topology", "REST contracts cannot declare subscription, ACK, heartbeat, or useful-data behavior")
		}
		return nil
	}
	if topology.MaxSubscriptions == 0 {
		return sourceContractError("topology.max_subscriptions", "must be positive for WebSocket contracts")
	}
	if subscription.ACKMode != ACKNone && subscription.ACKMode != ACKExact {
		return sourceContractError("subscription.ack_mode", "has an invalid value")
	}
	if subscription.ACKMode == ACKExact {
		if topology.MaxSubscriptionsPerACK == 0 || topology.MaxSubscriptionsPerACK > topology.MaxSubscriptions {
			return sourceContractError("topology.max_subscriptions_per_ack", "must be within the subscription limit")
		}
		if subscription.ACKTimeoutNS == 0 || subscription.MaxPendingACK == 0 {
			return sourceContractError("subscription", "exact ACK requires bounded pending count and timeout")
		}
		// MaxPendingACK bounds the batches retained by a concrete runner. The
		// runner validates feasibility against its selected inventory.
	} else if subscription.ACKTimeoutNS != 0 || subscription.MaxPendingACK != 0 || topology.MaxSubscriptionsPerACK != 0 {
		return sourceContractError("subscription", "ACK-none must not declare ACK bounds")
	}
	if heartbeat.Mode < HeartbeatNone || heartbeat.Mode > HeartbeatTestResponse {
		return sourceContractError("heartbeat.mode", "has an invalid value")
	}
	if heartbeat.Mode == HeartbeatNone {
		if heartbeat.IntervalNS != 0 || heartbeat.TimeoutNS != 0 || heartbeat.MinimumIntervalNS != 0 {
			return sourceContractError("heartbeat", "heartbeat-none must not declare timing behavior")
		}
	} else if heartbeat.IntervalNS == 0 || heartbeat.TimeoutNS == 0 {
		return sourceContractError("heartbeat", "enabled heartbeat requires interval and timeout")
	}
	if heartbeat.MinimumIntervalNS > heartbeat.IntervalNS {
		return sourceContractError("heartbeat.minimum_interval_ns", "cannot exceed the declared interval")
	}
	return nil
}

func validateRatePolicy(policy RatePolicy) error {
	if policy.Capacity == 0 || policy.RefillTokens == 0 || policy.RefillIntervalNS == 0 {
		return sourceContractError("rate", "capacity and refill budget must be positive")
	}
	if policy.ConnectionCost > policy.Capacity || policy.RequestCost > policy.Capacity {
		return sourceContractError("rate", "operation cost exceeds capacity")
	}
	if policy.MaxAttempts == 0 {
		return sourceContractError("rate.max_attempts", "must be positive")
	}
	if policy.DefaultRetryAfterNS == 0 || policy.MaxRetryAfterNS == 0 || policy.DefaultRetryAfterNS > policy.MaxRetryAfterNS {
		return sourceContractError("rate.retry_after", "requires a positive bounded default")
	}
	if policy.CircuitOpenNS == 0 {
		return sourceContractError("rate.circuit_open_ns", "must be positive")
	}
	sets := []struct {
		name   string
		values []int
	}{
		{"retryable_status_codes", policy.RetryableStatusCodes},
		{"terminal_status_codes", policy.TerminalStatusCodes},
		{"circuit_status_codes", policy.CircuitStatusCodes},
	}
	seen := make(map[int]string)
	for _, set := range sets {
		if len(set.values) > MaxStatusCodes {
			return sourceContractError("rate."+set.name, fmt.Sprintf("has %d entries, maximum is %d", len(set.values), MaxStatusCodes))
		}
		for _, status := range set.values {
			if status < 100 || status > 599 {
				return sourceContractError("rate."+set.name, fmt.Sprintf("contains invalid HTTP status %d", status))
			}
			if prior, ok := seen[status]; ok {
				return sourceContractError("rate."+set.name, fmt.Sprintf("status %d also appears in %s", status, prior))
			}
			seen[status] = set.name
		}
		if !slices.IsSorted(set.values) {
			return sourceContractError("rate."+set.name, "must be sorted for deterministic lookup")
		}
	}
	return nil
}

func validatePayloadPolicy(policy PayloadPolicy) error {
	if policy.MaxRawBytes == 0 || policy.MaxRawBytes > MaxPayloadBytes {
		return sourceContractError("payload.max_raw_bytes", fmt.Sprintf("must be within 1..%d", MaxPayloadBytes))
	}
	if policy.MaxSchemaDepth == 0 || policy.MaxSchemaDepth > MaxSchemaDepth {
		return sourceContractError("payload.max_schema_depth", fmt.Sprintf("must be within 1..%d", MaxSchemaDepth))
	}
	if policy.MaxSchemaFields == 0 || policy.MaxSchemaFields > MaxSchemaFields {
		return sourceContractError("payload.max_schema_fields", fmt.Sprintf("must be within 1..%d", MaxSchemaFields))
	}
	if policy.MaxArrayElements == 0 || policy.MaxArrayElements > MaxSchemaArrayElements {
		return sourceContractError("payload.max_array_elements", fmt.Sprintf("must be within 1..%d", MaxSchemaArrayElements))
	}
	return nil
}

func validateFixtureIdentity(fixture FixtureIdentity) error {
	if err := validateContractText("id", fixture.ID, MaxIdentityBytes); err != nil {
		return err
	}
	if fixture.ByteLength == 0 {
		return errors.New("byte length must be positive")
	}
	if fixture.SHA256 == ([32]byte{}) {
		return errors.New("SHA-256 must be present")
	}
	switch fixture.Provenance {
	case FixtureSynthetic:
		if !strings.HasPrefix(fixture.ID, "synthetic.") {
			return errors.New("synthetic identity must use the synthetic. prefix")
		}
		if fixture.SourceReference != "" || fixture.LicenseReference != "" || fixture.AccessedAtNS != 0 {
			return errors.New("synthetic fixture must not claim source, license, or access provenance")
		}
	case FixturePrimarySource:
		if fixture.SourceReference == "" || fixture.LicenseReference == "" || fixture.AccessedAtNS <= 0 {
			return errors.New("primary-source fixture requires source, license, and access provenance")
		}
		if err := validateDocumentationRef(DocumentationRef{URL: fixture.SourceReference, AccessedAtNS: fixture.AccessedAtNS, Authority: RuleOfficialDocumentation}); err != nil {
			return fmt.Errorf("source reference: %w", err)
		}
	default:
		return errors.New("has invalid provenance kind")
	}
	return nil
}

func validateContractText(field, value string, maximum int) error {
	if value == "" {
		return sourceContractError(field, "is required")
	}
	if !utf8.ValidString(value) {
		return sourceContractError(field, "is not valid UTF-8")
	}
	if len(value) > maximum {
		return sourceContractError(field, fmt.Sprintf("has %d bytes, maximum is %d", len(value), maximum))
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return sourceContractError(field, "contains a control delimiter")
	}
	return nil
}

func sourceContractError(field, problem string) error {
	return &SourceContractError{Field: field, Problem: problem}
}

func durationNS(duration time.Duration) uint64 {
	if duration <= 0 {
		return 0
	}
	return uint64(duration)
}
