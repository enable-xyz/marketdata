package verify

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/enable-xyz/marketdata/binance"
	"github.com/enable-xyz/marketdata/capture"
	"github.com/enable-xyz/marketdata/catalog"
)

const fixtureManifestVersion = 1

type fixtureManifest struct {
	Version             uint16              `json:"version"`
	SourceID            string              `json:"source_id"`
	Symbol              string              `json:"symbol"`
	ConnectionEpoch     string              `json:"connection_epoch"`
	PollEpoch           string              `json:"poll_epoch"`
	ClockEpochID        string              `json:"clock_epoch_id"`
	StartWallTimeNS     int64               `json:"start_wall_time_ns"`
	StepNanoseconds     uint64              `json:"step_nanoseconds"`
	HeartbeatPayloadHex string              `json:"heartbeat_payload_hex"`
	Catalog             fixtureCatalogInput `json:"catalog"`
	Inputs              []fixtureInput      `json:"inputs"`
}

type fixtureCatalogInput struct {
	Manifest string   `json:"manifest"`
	Names    []string `json:"names"`
}

type fixtureInput struct {
	Role       string `json:"role"`
	File       string `json:"file"`
	ByteLength int    `json:"byte_length"`
	SHA256     string `json:"sha256"`
}

type loadedFixture struct {
	manifest       fixtureManifest
	manifestSHA256 [sha256.Size]byte
	inputSHA256    [sha256.Size]byte
	connectionID   [16]byte
	pollID         [16]byte
	payloads       map[string][]byte
	catalog        catalog.Snapshot
}

func loadFixture(ctx context.Context, root, manifestPath string) (loadedFixture, error) {
	if err := ctx.Err(); err != nil {
		return loadedFixture{}, err
	}
	if err := requireRealDirectory(root); err != nil {
		return loadedFixture{}, err
	}
	manifestPath, err := containedFixturePath(root, manifestPath)
	if err != nil {
		return loadedFixture{}, err
	}
	encoded, err := os.ReadFile(manifestPath)
	if err != nil {
		return loadedFixture{}, fmt.Errorf("verify: read fixture manifest: %w", err)
	}
	manifestHash := sha256.Sum256(encoded)
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var manifest fixtureManifest
	if err := decoder.Decode(&manifest); err != nil {
		return loadedFixture{}, fmt.Errorf("verify: decode fixture manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return loadedFixture{}, errors.New("verify: fixture manifest has trailing JSON")
	}
	if manifest.Version != fixtureManifestVersion || manifest.SourceID != binance.SpotSourceID || manifest.Symbol == "" ||
		manifest.StartWallTimeNS <= 0 || manifest.StepNanoseconds == 0 || manifest.ClockEpochID == "" || len(manifest.Inputs) != 6 {
		return loadedFixture{}, errors.New("verify: incomplete fixture identity or bounds")
	}
	connectionID, err := parseUUID(manifest.ConnectionEpoch)
	if err != nil {
		return loadedFixture{}, fmt.Errorf("verify: connection epoch: %w", err)
	}
	pollID, err := parseUUID(manifest.PollEpoch)
	if err != nil {
		return loadedFixture{}, fmt.Errorf("verify: poll epoch: %w", err)
	}
	heartbeat, err := hex.DecodeString(manifest.HeartbeatPayloadHex)
	if err != nil || len(heartbeat) == 0 || len(heartbeat) > binance.SpotMaxPingPayloadBytes {
		return loadedFixture{}, errors.New("verify: invalid heartbeat fixture")
	}

	roles := []string{"ack", "book_ticker", "depth", "snapshot", "ticker", "trade"}
	payloads := make(map[string][]byte, len(roles)+1)
	inputHasher := sha256.New()
	seen := make(map[string]struct{}, len(roles))
	for _, input := range manifest.Inputs {
		if !slices.Contains(roles, input.Role) {
			return loadedFixture{}, fmt.Errorf("verify: unknown fixture role %q", input.Role)
		}
		if _, duplicate := seen[input.Role]; duplicate {
			return loadedFixture{}, fmt.Errorf("verify: duplicate fixture role %q", input.Role)
		}
		seen[input.Role] = struct{}{}
		path, err := containedFixturePath(root, filepath.Join(root, filepath.FromSlash(input.File)))
		if err != nil {
			return loadedFixture{}, err
		}
		payload, err := os.ReadFile(path)
		if err != nil {
			return loadedFixture{}, fmt.Errorf("verify: read %s fixture: %w", input.Role, err)
		}
		digest := sha256.Sum256(payload)
		if input.ByteLength != len(payload) || input.SHA256 != hex.EncodeToString(digest[:]) {
			return loadedFixture{}, fmt.Errorf("verify: %s fixture identity mismatch", input.Role)
		}
		payloads[input.Role] = payload
		_, _ = inputHasher.Write([]byte(input.Role))
		_, _ = inputHasher.Write(digest[:])
	}
	if len(seen) != len(roles) {
		return loadedFixture{}, errors.New("verify: fixture role inventory is incomplete")
	}
	payloads["heartbeat"] = heartbeat
	_, _ = inputHasher.Write([]byte("heartbeat"))
	heartbeatHash := sha256.Sum256(heartbeat)
	_, _ = inputHasher.Write(heartbeatHash[:])

	catalogPath, err := containedFixturePath(root, filepath.Join(root, filepath.FromSlash(manifest.Catalog.Manifest)))
	if err != nil {
		return loadedFixture{}, err
	}
	bundle, err := binance.LoadFixtureBundle(catalogPath)
	if err != nil {
		return loadedFixture{}, err
	}
	pages := make([]binance.CapturedPage, 0, len(manifest.Catalog.Names))
	for _, name := range manifest.Catalog.Names {
		page, err := bundle.CapturedPage(name)
		if err != nil {
			return loadedFixture{}, err
		}
		pages = append(pages, page)
		_, _ = inputHasher.Write([]byte("catalog:" + name))
		_, _ = inputHasher.Write(page.RawSHA256[:])
	}
	composed, err := binance.ComposeExchangeInfo(pages, binance.ComposeOptions{}, binance.DefaultParserLimits())
	if err != nil {
		return loadedFixture{}, err
	}
	source, sourceVersion, channels := binance.SpotCatalogContract()
	snapshot, err := catalog.BuildFreshSnapshot(source, sourceVersion, channels, composed.Candidates)
	if err != nil {
		return loadedFixture{}, err
	}
	var inputHash [sha256.Size]byte
	copy(inputHash[:], inputHasher.Sum(nil))
	return loadedFixture{
		manifest: manifest, manifestSHA256: manifestHash, inputSHA256: inputHash,
		connectionID: connectionID, pollID: pollID, payloads: payloads, catalog: snapshot,
	}, nil
}

func containedFixturePath(root, path string) (string, error) {
	if !filepath.IsAbs(root) || !filepath.IsAbs(path) {
		return "", errors.New("verify: fixture path must resolve absolutely")
	}
	cleanRoot := filepath.Clean(root)
	cleanPath := filepath.Clean(path)
	relative, err := filepath.Rel(cleanRoot, cleanPath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", errors.New("verify: fixture path escaped fixture_root")
	}
	realRoot, err := filepath.EvalSymlinks(cleanRoot)
	if err != nil {
		return "", errors.New("verify: fixture_root must be an existing directory")
	}
	rootInfo, err := os.Stat(realRoot)
	if err != nil || !rootInfo.IsDir() {
		return "", errors.New("verify: fixture_root must be an existing directory")
	}
	realPath, err := filepath.EvalSymlinks(cleanPath)
	if err != nil {
		return "", errors.New("verify: fixture path must be an existing regular file")
	}
	realRelative, err := filepath.Rel(realRoot, realPath)
	if err != nil || realRelative == ".." || strings.HasPrefix(realRelative, ".."+string(filepath.Separator)) || filepath.IsAbs(realRelative) {
		return "", errors.New("verify: fixture path escaped fixture_root through a symbolic link")
	}
	info, err := os.Lstat(realPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return "", errors.New("verify: fixture path must be an existing regular file")
	}
	return realPath, nil
}

func parseUUID(value string) ([16]byte, error) {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return [16]byte{}, errors.New("non-canonical UUID")
	}
	raw := value[:8] + value[9:13] + value[14:18] + value[19:23] + value[24:]
	decoded, err := hex.DecodeString(raw)
	if err != nil || len(decoded) != 16 {
		return [16]byte{}, errors.New("non-canonical UUID")
	}
	var result [16]byte
	copy(result[:], decoded)
	if result == ([16]byte{}) {
		return [16]byte{}, errors.New("zero UUID")
	}
	return result, nil
}

type fixtureWSConnector struct {
	frames []binance.SpotWSFrame
	used   bool
}

func (c *fixtureWSConnector) Connect(ctx context.Context, request binance.SpotWSConnectRequest) (binance.SpotWSConnection, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.used || request.Endpoint != binance.SpotWSEndpoint || request.MaxApplicationBytes != binance.SpotMaxRawPayloadBytes {
		return nil, errors.New("verify: invalid fixture WebSocket connection")
	}
	c.used = true
	frames := make([]binance.SpotWSFrame, len(c.frames))
	for i, frame := range c.frames {
		frames[i] = binance.SpotWSFrame{Kind: frame.Kind, Payload: slices.Clone(frame.Payload)}
	}
	return &fixtureWSConnection{frames: frames}, nil
}

type fixtureWSConnection struct {
	frames []binance.SpotWSFrame
	next   int
	closed bool
	pings  [][]byte
}

func (c *fixtureWSConnection) Read(ctx context.Context, maxBytes uint32) (binance.SpotWSFrame, error) {
	if err := ctx.Err(); err != nil {
		return binance.SpotWSFrame{}, err
	}
	if c.closed || c.next == len(c.frames) {
		return binance.SpotWSFrame{}, capture.ErrTransportClosed
	}
	frame := c.frames[c.next]
	c.next++
	if len(frame.Payload) > int(maxBytes) {
		return binance.SpotWSFrame{}, binance.ErrSpotBounds
	}
	if frame.Kind == binance.SpotWSFramePing {
		c.pings = append(c.pings, slices.Clone(frame.Payload))
	}
	return binance.SpotWSFrame{Kind: frame.Kind, Payload: slices.Clone(frame.Payload)}, nil
}

func (c *fixtureWSConnection) ReadBuffered(ctx context.Context, _ uint32) (binance.SpotWSFrame, bool, error) {
	if err := ctx.Err(); err != nil {
		return binance.SpotWSFrame{}, false, err
	}
	return binance.SpotWSFrame{}, false, nil
}

func (c *fixtureWSConnection) Write(ctx context.Context, kind binance.SpotWSWriteKind, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if kind == binance.SpotWSWriteText {
		if len(payload) == 0 || len(payload) > binance.SpotMaxControlMessageBytes {
			return binance.ErrSpotBounds
		}
		return nil
	}
	if kind != binance.SpotWSWritePong || len(c.pings) == 0 || !slices.Equal(c.pings[0], payload) {
		return binance.ErrSpotConnection
	}
	c.pings = c.pings[1:]
	return nil
}

func (c *fixtureWSConnection) Close(ctx context.Context, _ capture.CloseReason) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.closed = true
	return nil
}

type fixtureRESTClient struct {
	body []byte
	used bool
}

func (c *fixtureRESTClient) Do(ctx context.Context, request binance.SpotDepthRequest, maxBytes uint32) (binance.SpotRESTResponse, error) {
	if err := ctx.Err(); err != nil {
		return binance.SpotRESTResponse{}, err
	}
	if c.used || request.Method != capture.RESTMethodGET || request.Endpoint != binance.SpotDepthEndpoint || len(c.body) > int(maxBytes) {
		return binance.SpotRESTResponse{}, errors.New("verify: invalid fixture REST request")
	}
	c.used = true
	return binance.SpotRESTResponse{
		Status:  200,
		Headers: []binance.SpotHTTPHeader{{Name: "Content-Length", Value: fmt.Sprintf("%d", len(c.body))}, {Name: "Content-Type", Value: "application/json"}, {Name: "X-MBX-USED-WEIGHT-1M", Value: "5"}},
		Body:    slices.Clone(c.body),
	}, nil
}

func newFixtureTransports(fixture loadedFixture) (binance.SpotWSConnector, binance.SpotRESTClient) {
	return &fixtureWSConnector{frames: []binance.SpotWSFrame{
		{Kind: binance.SpotWSFrameText, Payload: fixture.payloads["ack"]},
		{Kind: binance.SpotWSFrameText, Payload: fixture.payloads["trade"]},
		{Kind: binance.SpotWSFrameText, Payload: fixture.payloads["depth"]},
		{Kind: binance.SpotWSFrameText, Payload: fixture.payloads["book_ticker"]},
		{Kind: binance.SpotWSFrameText, Payload: fixture.payloads["ticker"]},
		{Kind: binance.SpotWSFramePing, Payload: fixture.payloads["heartbeat"]},
		{Kind: binance.SpotWSFrameClose},
	}}, &fixtureRESTClient{body: fixture.payloads["snapshot"]}
}

var _ binance.SpotWSConnector = (*fixtureWSConnector)(nil)
var _ binance.SpotWSConnection = (*fixtureWSConnection)(nil)
var _ binance.SpotRESTClient = (*fixtureRESTClient)(nil)
