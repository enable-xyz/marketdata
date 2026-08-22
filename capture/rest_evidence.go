package capture

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"
)

const (
	RESTEvidenceVersion        uint16 = 1
	MaxRESTParameters                 = 32
	MaxRESTHeaders                    = 16
	MaxRESTParameterNameBytes         = 64
	MaxRESTParameterValueBytes        = 256
	MaxRESTHeaderValueBytes           = 256
)

var ErrInvalidRESTEvidence = errors.New("capture: invalid REST evidence")

type RESTMethod string

const (
	RESTMethodGET  RESTMethod = "GET"
	RESTMethodHEAD RESTMethod = "HEAD"
)

type SanitizedParameter struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type RESTHeaderKind string

const (
	RESTHeaderRetryAfter     RESTHeaderKind = "retry-after"
	RESTHeaderRateLimitLimit RESTHeaderKind = "rate-limit-limit"
	RESTHeaderRateRemaining  RESTHeaderKind = "rate-limit-remaining"
	RESTHeaderRateReset      RESTHeaderKind = "rate-limit-reset"
	RESTHeaderUsedWeight     RESTHeaderKind = "used-weight"
	RESTHeaderContentType    RESTHeaderKind = "content-type"
	RESTHeaderContentLength  RESTHeaderKind = "content-length"
)

type RESTHeader struct {
	Kind  RESTHeaderKind `json:"kind"`
	Value string         `json:"value"`
}

type RESTRequestEvidenceV1 struct {
	Version       uint16               `json:"version"`
	Kind          string               `json:"kind"`
	RequestID     string               `json:"request_id"`
	Method        RESTMethod           `json:"method"`
	Parameters    []SanitizedParameter `json:"sanitized_parameters"`
	ScheduledAtNS int64                `json:"scheduled_at_ns"`
	StartedAtNS   int64                `json:"started_at_ns"`
}

type RESTResponseEvidenceV1 struct {
	Version       uint16       `json:"version"`
	Kind          string       `json:"kind"`
	RequestID     string       `json:"request_id"`
	CompletedAtNS int64        `json:"completed_at_ns"`
	Status        int          `json:"status"`
	RetryAfterNS  uint64       `json:"retry_after_ns"`
	Headers       []RESTHeader `json:"allowlisted_headers"`
}

func MarshalRESTRequestEvidence(evidence RESTRequestEvidenceV1) ([]byte, error) {
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return nil, fmt.Errorf("capture: encode REST request evidence: %w", err)
	}
	maximum := MaxExtensionBytes - controlExtensionHeaderSize
	if len(encoded) > maximum {
		return nil, fmt.Errorf("%w: encoded request has %d bytes, maximum is %d", ErrInvalidRESTEvidence, len(encoded), maximum)
	}
	return encoded, nil
}

func UnmarshalRESTRequestEvidence(encoded []byte) (RESTRequestEvidenceV1, error) {
	if len(encoded) == 0 || len(encoded) > MaxExtensionBytes {
		return RESTRequestEvidenceV1{}, ErrInvalidRESTEvidence
	}
	var evidence RESTRequestEvidenceV1
	if err := json.Unmarshal(encoded, &evidence); err != nil {
		return RESTRequestEvidenceV1{}, fmt.Errorf("%w: decode request: %v", ErrInvalidRESTEvidence, err)
	}
	if err := evidence.Validate(); err != nil {
		return RESTRequestEvidenceV1{}, err
	}
	return evidence, nil
}

func (e RESTRequestEvidenceV1) Validate() error {
	if e.Version != RESTEvidenceVersion || e.Kind != "request" {
		return fmt.Errorf("%w: request version or kind", ErrInvalidRESTEvidence)
	}
	if err := validateContractText("rest.request_id", e.RequestID, MaxIdentityBytes); err != nil {
		return errors.Join(ErrInvalidRESTEvidence, err)
	}
	if e.Method != RESTMethodGET && e.Method != RESTMethodHEAD {
		return fmt.Errorf("%w: method %q is not allowlisted", ErrInvalidRESTEvidence, e.Method)
	}
	if len(e.Parameters) > MaxRESTParameters {
		return fmt.Errorf("%w: has %d parameters, maximum is %d", ErrInvalidRESTEvidence, len(e.Parameters), MaxRESTParameters)
	}
	if !slices.IsSortedFunc(e.Parameters, func(a, b SanitizedParameter) int { return strings.Compare(a.Name, b.Name) }) {
		return fmt.Errorf("%w: parameters must be sorted", ErrInvalidRESTEvidence)
	}
	for i, parameter := range e.Parameters {
		if err := validateRESTText(parameter.Name, MaxRESTParameterNameBytes); err != nil {
			return fmt.Errorf("%w: parameter %d name: %v", ErrInvalidRESTEvidence, i, err)
		}
		if i > 0 && parameter.Name == e.Parameters[i-1].Name {
			return fmt.Errorf("%w: duplicate parameter %q", ErrInvalidRESTEvidence, parameter.Name)
		}
		if sensitiveRESTName(parameter.Name) {
			return fmt.Errorf("%w: sensitive parameter name %q", ErrInvalidRESTEvidence, parameter.Name)
		}
		if err := validateRESTText(parameter.Value, MaxRESTParameterValueBytes); err != nil {
			return fmt.Errorf("%w: parameter %d value: %v", ErrInvalidRESTEvidence, i, err)
		}
	}
	return nil
}

func MarshalRESTResponseEvidence(evidence RESTResponseEvidenceV1) ([]byte, error) {
	if err := evidence.Validate(); err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return nil, fmt.Errorf("capture: encode REST response evidence: %w", err)
	}
	if len(encoded) > MaxExtensionBytes {
		return nil, fmt.Errorf("%w: encoded response has %d bytes, maximum is %d", ErrInvalidRESTEvidence, len(encoded), MaxExtensionBytes)
	}
	return encoded, nil
}

func UnmarshalRESTResponseEvidence(encoded []byte) (RESTResponseEvidenceV1, error) {
	if len(encoded) == 0 || len(encoded) > MaxExtensionBytes {
		return RESTResponseEvidenceV1{}, ErrInvalidRESTEvidence
	}
	var evidence RESTResponseEvidenceV1
	if err := json.Unmarshal(encoded, &evidence); err != nil {
		return RESTResponseEvidenceV1{}, fmt.Errorf("%w: decode response: %v", ErrInvalidRESTEvidence, err)
	}
	if err := evidence.Validate(); err != nil {
		return RESTResponseEvidenceV1{}, err
	}
	return evidence, nil
}

func (e RESTResponseEvidenceV1) Validate() error {
	if e.Version != RESTEvidenceVersion || e.Kind != "response" {
		return fmt.Errorf("%w: response version or kind", ErrInvalidRESTEvidence)
	}
	if err := validateContractText("rest.request_id", e.RequestID, MaxIdentityBytes); err != nil {
		return errors.Join(ErrInvalidRESTEvidence, err)
	}
	if e.Status < 100 || e.Status > 599 {
		return fmt.Errorf("%w: invalid status %d", ErrInvalidRESTEvidence, e.Status)
	}
	if len(e.Headers) > MaxRESTHeaders {
		return fmt.Errorf("%w: has %d headers, maximum is %d", ErrInvalidRESTEvidence, len(e.Headers), MaxRESTHeaders)
	}
	if !slices.IsSortedFunc(e.Headers, func(a, b RESTHeader) int { return strings.Compare(string(a.Kind), string(b.Kind)) }) {
		return fmt.Errorf("%w: headers must be sorted", ErrInvalidRESTEvidence)
	}
	for i, header := range e.Headers {
		if !validRESTHeaderKind(header.Kind) {
			return fmt.Errorf("%w: header %q is not allowlisted", ErrInvalidRESTEvidence, header.Kind)
		}
		if i > 0 && header.Kind == e.Headers[i-1].Kind {
			return fmt.Errorf("%w: duplicate header %q", ErrInvalidRESTEvidence, header.Kind)
		}
		if err := validateRESTText(header.Value, MaxRESTHeaderValueBytes); err != nil {
			return fmt.Errorf("%w: header %q: %v", ErrInvalidRESTEvidence, header.Kind, err)
		}
	}
	return nil
}

func validRESTHeaderKind(kind RESTHeaderKind) bool {
	switch kind {
	case RESTHeaderRetryAfter,
		RESTHeaderRateLimitLimit,
		RESTHeaderRateRemaining,
		RESTHeaderRateReset,
		RESTHeaderUsedWeight,
		RESTHeaderContentType,
		RESTHeaderContentLength:
		return true
	default:
		return false
	}
}

func validateRESTText(value string, maximum int) error {
	if value == "" {
		return errors.New("is required")
	}
	if len(value) > maximum || !utf8.ValidString(value) {
		return fmt.Errorf("is invalid or exceeds %d bytes", maximum)
	}
	if strings.ContainsAny(value, "\r\n\x00") {
		return errors.New("contains a control delimiter")
	}
	return nil
}

func sensitiveRESTName(name string) bool {
	normalized := strings.NewReplacer("-", "", "_", "", ".", "").Replace(strings.ToLower(name))
	for _, sensitive := range []string{"apikey", "secret", "signature", "token", "authorization", "password", "credential", "cookie"} {
		if strings.Contains(normalized, sensitive) {
			return true
		}
	}
	return false
}
