package binance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/spf13/pathologize"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	FixtureManifestVersion            = 1
	MaxFixtureManifestBytes           = 64 << 10
	MaxFixtureEntries                 = 32
	OfficialExchangeInfoExcerptBytes  = 2090
	OfficialExchangeInfoExcerptSHA256 = "487b37f889c319f3e632ad0bd3d16557f2c8ef16ff30c19ca34a0414c8f36e2b"
)

type FixtureBundle struct {
	Version     int               `json:"version"`
	Repository  string            `json:"repository"`
	Path        string            `json:"path"`
	Commit      string            `json:"commit"`
	Section     string            `json:"section"`
	SourceURL   string            `json:"source_url"`
	AccessDate  string            `json:"access_date"`
	Fixtures    []FixtureMetadata `json:"fixtures"`
	root        string
	bytesByName map[string][]byte
}

type FixtureMetadata struct {
	Name              string `json:"name"`
	File              string `json:"file"`
	Classification    string `json:"classification"`
	ParseableResponse bool   `json:"parseable_response"`
	DerivedFrom       string `json:"derived_from,omitempty"`
	ByteLength        int    `json:"byte_length"`
	SHA256            string `json:"sha256"`
	PageIndex         int    `json:"page_index,omitempty"`
	PageCount         int    `json:"page_count,omitempty"`
	RequestID         string `json:"request_id,omitempty"`
	ScheduledAtNS     int64  `json:"scheduled_at_ns,omitempty"`
	StartedAtNS       int64  `json:"started_at_ns,omitempty"`
	CompletedAtNS     int64  `json:"completed_at_ns,omitempty"`
}

func LoadFixtureBundle(manifestPath string) (FixtureBundle, error) {
	if manifestPath == "" {
		return FixtureBundle{}, errors.New("binance: fixture manifest path is required")
	}
	manifestBytes, err := readBoundedFile(manifestPath, MaxFixtureManifestBytes)
	if err != nil {
		return FixtureBundle{}, fmt.Errorf("binance: read fixture manifest: %w", err)
	}
	limits := DefaultParserLimits()
	limits.MaxResponseBytes = MaxFixtureManifestBytes
	if err := validateJSONStructure(manifestBytes, limits); err != nil {
		return FixtureBundle{}, fmt.Errorf("binance: fixture manifest: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(manifestBytes))
	decoder.DisallowUnknownFields()
	var bundle FixtureBundle
	if err := decoder.Decode(&bundle); err != nil {
		return FixtureBundle{}, fmt.Errorf("binance: decode fixture manifest strictly: %w", err)
	}
	if bundle.Version != FixtureManifestVersion || bundle.Repository != "binance/binance-spot-api-docs" || bundle.Path != "rest-api.md" || bundle.Commit != SpotRESTCommit || bundle.SourceURL != SpotRESTDocumentationURI {
		return FixtureBundle{}, errors.New("binance: fixture manifest immutable source identity mismatch")
	}
	if bundle.Section != "Exchange information / Response example" {
		return FixtureBundle{}, errors.New("binance: fixture manifest section mismatch")
	}
	accessDate, err := time.Parse("2006-01-02", bundle.AccessDate)
	if err != nil || accessDate.Format("2006-01-02") != bundle.AccessDate {
		return FixtureBundle{}, errors.New("binance: fixture manifest access_date must be an exact date")
	}
	if len(bundle.Fixtures) == 0 || len(bundle.Fixtures) > MaxFixtureEntries {
		return FixtureBundle{}, fmt.Errorf("binance: fixture manifest count %d is outside bounds", len(bundle.Fixtures))
	}
	bundle.root = filepath.Dir(manifestPath)
	bundle.bytesByName = make(map[string][]byte, len(bundle.Fixtures))
	names := make(map[string]struct{}, len(bundle.Fixtures))
	files := make(map[string]struct{}, len(bundle.Fixtures))
	hasOfficial := false
	for i, fixture := range bundle.Fixtures {
		if err := validateFixtureMetadata(fixture); err != nil {
			return FixtureBundle{}, fmt.Errorf("binance: fixture %d: %w", i, err)
		}
		if _, exists := names[fixture.Name]; exists {
			return FixtureBundle{}, fmt.Errorf("binance: duplicate fixture name %q", fixture.Name)
		}
		names[fixture.Name] = struct{}{}
		if _, exists := files[fixture.File]; exists {
			return FixtureBundle{}, fmt.Errorf("binance: duplicate fixture file %q", fixture.File)
		}
		files[fixture.File] = struct{}{}
		if !pathologize.IsClean(fixture.File) || filepath.Base(fixture.File) != fixture.File {
			return FixtureBundle{}, fmt.Errorf("binance: unsafe fixture filename %q", fixture.File)
		}
		fixturePath := pathologize.Join(bundle.root, fixture.File)
		contents, err := readBoundedFile(fixturePath, MaxExchangeInfoBytes)
		if err != nil {
			return FixtureBundle{}, fmt.Errorf("binance: read fixture %q: %w", fixture.Name, err)
		}
		if len(contents) != fixture.ByteLength {
			return FixtureBundle{}, fmt.Errorf("binance: fixture %q byte length = %d, manifest = %d", fixture.Name, len(contents), fixture.ByteLength)
		}
		digest := sha256.Sum256(contents)
		if hex.EncodeToString(digest[:]) != fixture.SHA256 {
			return FixtureBundle{}, fmt.Errorf("binance: fixture %q SHA-256 mismatch", fixture.Name)
		}
		if fixture.ParseableResponse {
			if err := validateJSONStructure(contents, DefaultParserLimits()); err != nil {
				return FixtureBundle{}, fmt.Errorf("binance: fixture %q response: %w", fixture.Name, err)
			}
		}
		if fixture.Classification == "exact_official_schema_excerpt" {
			hasOfficial = true
			if fixture.ParseableResponse || fixture.Name != "official-schema-excerpt" ||
				fixture.ByteLength != OfficialExchangeInfoExcerptBytes ||
				fixture.SHA256 != OfficialExchangeInfoExcerptSHA256 {
				return FixtureBundle{}, errors.New("binance: official schema excerpt differs from immutable pinned commit evidence")
			}
		}
		bundle.bytesByName[fixture.Name] = contents
	}
	if !hasOfficial {
		return FixtureBundle{}, errors.New("binance: fixture bundle lacks exact official schema excerpt")
	}
	for _, fixture := range bundle.Fixtures {
		if fixture.DerivedFrom != "" {
			if _, exists := names[fixture.DerivedFrom]; !exists {
				return FixtureBundle{}, fmt.Errorf("binance: fixture %q has unknown derivation %q", fixture.Name, fixture.DerivedFrom)
			}
		}
	}
	return bundle, nil
}

func (b FixtureBundle) Metadata(name string) (FixtureMetadata, bool) {
	for _, fixture := range b.Fixtures {
		if fixture.Name == name {
			return fixture, true
		}
	}
	return FixtureMetadata{}, false
}

func (b FixtureBundle) Bytes(name string) ([]byte, bool) {
	contents, ok := b.bytesByName[name]
	return slices.Clone(contents), ok
}

func (b FixtureBundle) CapturedPage(name string) (CapturedPage, error) {
	metadata, ok := b.Metadata(name)
	if !ok {
		return CapturedPage{}, fmt.Errorf("binance: fixture %q is not declared", name)
	}
	if !metadata.ParseableResponse {
		return CapturedPage{}, fmt.Errorf("binance: fixture %q is not a parseable response", name)
	}
	contents, ok := b.Bytes(name)
	if !ok {
		return CapturedPage{}, fmt.Errorf("binance: fixture %q bytes are unavailable", name)
	}
	request := capture.RESTRequestEvidenceV1{
		Version:       capture.RESTEvidenceVersion,
		Kind:          "request",
		RequestID:     metadata.RequestID,
		Method:        capture.RESTMethodGET,
		Parameters:    []capture.SanitizedParameter{},
		ScheduledAtNS: metadata.ScheduledAtNS,
		StartedAtNS:   metadata.StartedAtNS,
	}
	response := capture.RESTResponseEvidenceV1{
		Version:       capture.RESTEvidenceVersion,
		Kind:          "response",
		RequestID:     metadata.RequestID,
		CompletedAtNS: metadata.CompletedAtNS,
		Status:        200,
		Headers:       []capture.RESTHeader{{Kind: capture.RESTHeaderContentType, Value: "application/json"}},
	}
	return NewInMemoryCapturedPage(metadata.PageIndex, metadata.PageCount, request, response, contents), nil
}

func validateFixtureMetadata(fixture FixtureMetadata) error {
	for name, value := range map[string]string{
		"name":           fixture.Name,
		"file":           fixture.File,
		"classification": fixture.Classification,
		"sha256":         fixture.SHA256,
	} {
		if value == "" || len(value) > MaxExchangeInfoStringBytes || !utf8.ValidString(value) {
			return fmt.Errorf("%s is empty, oversized, or invalid UTF-8", name)
		}
	}
	decoded, err := hex.DecodeString(fixture.SHA256)
	if err != nil || len(decoded) != sha256.Size || fixture.SHA256 != strings.ToLower(fixture.SHA256) {
		return errors.New("sha256 must be lowercase 64-character hexadecimal")
	}
	if fixture.ByteLength < 1 || fixture.ByteLength > MaxExchangeInfoBytes {
		return errors.New("byte_length is outside bounds")
	}
	allowed := []string{"exact_official_schema_excerpt", "synthetic_contract_response", "synthetic_page_composition_response"}
	if !slices.Contains(allowed, fixture.Classification) {
		return fmt.Errorf("unknown classification %q", fixture.Classification)
	}
	if fixture.Classification != "exact_official_schema_excerpt" {
		if !fixture.ParseableResponse || fixture.DerivedFrom == "" || fixture.PageCount < 1 || fixture.PageCount > MaxExchangeInfoPages || fixture.PageIndex < 0 || fixture.PageIndex >= fixture.PageCount || fixture.RequestID == "" {
			return errors.New("synthetic response metadata is incomplete")
		}
		if fixture.ScheduledAtNS < 0 || fixture.StartedAtNS < fixture.ScheduledAtNS || fixture.CompletedAtNS < fixture.StartedAtNS {
			return errors.New("synthetic request timing evidence regresses")
		}
	}
	return nil
}
func readBoundedFile(path string, maximum int) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	contents, err := io.ReadAll(io.LimitReader(file, int64(maximum)+1))
	if err != nil {
		return nil, err
	}
	if len(contents) > maximum {
		return nil, fmt.Errorf("file exceeds %d-byte bound", maximum)
	}
	return contents, nil
}
