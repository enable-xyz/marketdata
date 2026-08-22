package binance

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/catalog"
)

type CheckEvidence struct {
	Status                    string   `json:"status"`
	VerificationScope         string   `json:"verification_scope"`
	SourceID                  string   `json:"source_id"`
	ChannelCount              int      `json:"channel_count"`
	InstrumentCount           int      `json:"instrument_count"`
	PageCount                 int      `json:"page_count"`
	FixtureNames              []string `json:"fixture_names"`
	RawSHA256                 []string `json:"raw_sha256"`
	SnapshotSHA256            string   `json:"snapshot_sha256"`
	DocumentationCommit       string   `json:"documentation_commit"`
	OfficialFixtureSHA256     string   `json:"official_fixture_sha256"`
	OfficialFixtureByteLength int      `json:"official_fixture_byte_length"`
	UnknownFilterTypes        int      `json:"unknown_filter_types"`
	UnknownAdditiveFields     int      `json:"unknown_additive_fields"`
}

func CheckFixtureCatalog(ctx context.Context, manifestPath string, fixtureNames []string, expectedSnapshotSHA256 string, output io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if output == nil {
		return errors.New("binance: catalog check output is required")
	}
	if len(fixtureNames) == 0 || len(fixtureNames) > MaxExchangeInfoPages {
		return fmt.Errorf("binance: catalog check fixture count %d is outside bounds", len(fixtureNames))
	}
	if len(expectedSnapshotSHA256) != sha256.Size*2 || expectedSnapshotSHA256 != strings.ToLower(expectedSnapshotSHA256) {
		return errors.New("binance: expected snapshot SHA-256 must be lowercase 64-character hexadecimal")
	}
	if decoded, err := hex.DecodeString(expectedSnapshotSHA256); err != nil || len(decoded) != sha256.Size {
		return errors.New("binance: expected snapshot SHA-256 is invalid")
	}
	bundle, err := LoadFixtureBundle(manifestPath)
	if err != nil {
		return err
	}
	seenFixtures := make(map[string]struct{}, len(fixtureNames))
	pages := make([]CapturedPage, 0, len(fixtureNames))
	for _, name := range fixtureNames {
		if _, exists := seenFixtures[name]; exists {
			return fmt.Errorf("binance: duplicate catalog check fixture %q", name)
		}
		seenFixtures[name] = struct{}{}
		page, err := bundle.CapturedPage(name)
		if err != nil {
			return err
		}
		pages = append(pages, page)
	}
	composed, err := ComposeExchangeInfo(pages, ComposeOptions{}, DefaultParserLimits())
	if err != nil {
		return err
	}
	source, version, channels := SpotCatalogContract()
	if err := source.Validate(); err != nil {
		return err
	}
	if err := version.Validate(); err != nil {
		return err
	}
	for _, channel := range channels {
		if err := channel.Validate(); err != nil {
			return err
		}
	}
	for _, candidate := range composed.Candidates {
		if err := candidate.Validate(); err != nil {
			return err
		}
	}
	snapshot, err := catalog.BuildFreshSnapshot(source, version, channels, composed.Candidates)
	if err != nil {
		return err
	}
	snapshotHash := catalog.HashHex(snapshot.SHA256)
	if snapshotHash != expectedSnapshotSHA256 {
		return fmt.Errorf("binance: catalog snapshot SHA-256 = %s, expected %s", snapshotHash, expectedSnapshotSHA256)
	}
	if _, ok := bundle.Metadata("official-schema-excerpt"); !ok {
		return errors.New("binance: official schema fixture evidence is missing")
	}
	evidence := CheckEvidence{
		Status:                    "verified",
		VerificationScope:         catalog.RawEvidenceInMemoryProjection,
		SourceID:                  source.SourceID,
		ChannelCount:              len(channels),
		InstrumentCount:           snapshot.InstrumentCount,
		PageCount:                 len(pages),
		FixtureNames:              slices.Clone(fixtureNames),
		SnapshotSHA256:            snapshotHash,
		DocumentationCommit:       bundle.Commit,
		OfficialFixtureSHA256:     OfficialExchangeInfoExcerptSHA256,
		OfficialFixtureByteLength: OfficialExchangeInfoExcerptBytes,
	}
	for _, page := range pages {
		evidence.RawSHA256 = append(evidence.RawSHA256, hex.EncodeToString(page.RawSHA256[:]))
	}
	for _, symbol := range composed.Symbols {
		for _, filter := range symbol.Filters {
			if !filter.Known {
				evidence.UnknownFilterTypes++
			}
			evidence.UnknownAdditiveFields += len(filter.UnknownFields)
		}
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		return fmt.Errorf("binance: encode catalog check evidence: %w", err)
	}
	if _, err := output.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("binance: write catalog check evidence: %w", err)
	}
	return nil
}
