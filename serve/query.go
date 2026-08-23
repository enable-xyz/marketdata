package serve

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/enable-xyz/marketdata/replay"
	"github.com/enable-xyz/marketdata/warehouse"
)

type normalizedQuery struct {
	DatasetID           string   `json:"dataset_id"`
	Family              string   `json:"family"`
	SourceIDs           []string `json:"source_ids"`
	ChannelIDs          []string `json:"channel_ids"`
	InstrumentUIDs      []string `json:"instrument_uids"`
	StartReceivedTimeNS int64    `json:"start_received_time_ns"`
	EndReceivedTimeNS   int64    `json:"end_received_time_ns"`
	Limit               int      `json:"limit"`
	PageToken           string   `json:"-"`
}

type normalizedReplay struct {
	DatasetID           string
	Family              string
	SourceIDs           []string
	ChannelIDs          []string
	InstrumentUIDs      []string
	StartReceivedTimeNS int64
	EndReceivedTimeNS   int64
}

func normalizeQueryRequest(request QueryRequest, config Config) (normalizedQuery, error) {
	if (request.DatasetID == "") == (request.Family == "") {
		return normalizedQuery{}, fmt.Errorf("%w: exactly one of dataset_id or family is required", ErrQueryRequest)
	}
	datasetID := request.DatasetID
	if datasetID != "" {
		parsed, err := warehouse.ParseHash(datasetID)
		if err != nil {
			return normalizedQuery{}, fmt.Errorf("%w: dataset_id", ErrQueryRequest)
		}
		datasetID = parsed.String()
	}
	if request.Family != "" && !validRequestText(request.Family) {
		return normalizedQuery{}, fmt.Errorf("%w: family", ErrQueryRequest)
	}
	sources, err := normalizeIDs(request.SourceIDs, 1, warehouse.MaximumQuerySources)
	if err != nil {
		return normalizedQuery{}, err
	}
	channels, err := normalizeIDs(request.ChannelIDs, 0, warehouse.MaximumQueryChannels)
	if err != nil {
		return normalizedQuery{}, err
	}
	instruments, err := normalizeIDs(request.InstrumentUIDs, 0, warehouse.MaximumQueryInstruments)
	if err != nil {
		return normalizedQuery{}, err
	}
	if err := validateInterval(request.StartReceivedTimeNS, request.EndReceivedTimeNS, config.MaxQueryInterval); err != nil {
		return normalizedQuery{}, err
	}
	limit := request.Limit
	if limit == 0 {
		limit = config.DefaultPageRows
	}
	if limit < 1 || limit > config.MaxPageRows || limit > MaximumPageRows {
		return normalizedQuery{}, fmt.Errorf("%w: limit", ErrQueryRequest)
	}
	if len(request.PageToken) > 32<<10 {
		return normalizedQuery{}, ErrPageToken
	}
	return normalizedQuery{DatasetID: datasetID, Family: request.Family, SourceIDs: sources, ChannelIDs: channels,
		InstrumentUIDs: instruments, StartReceivedTimeNS: request.StartReceivedTimeNS, EndReceivedTimeNS: request.EndReceivedTimeNS,
		Limit: limit, PageToken: request.PageToken}, nil
}

func normalizeReplayRequest(request ReplayRequest, config Config) (normalizedReplay, error) {
	query, err := normalizeQueryRequest(QueryRequest{DatasetID: request.DatasetID, Family: request.Family,
		SourceIDs: request.SourceIDs, ChannelIDs: request.ChannelIDs, InstrumentUIDs: request.InstrumentUIDs,
		StartReceivedTimeNS: request.StartReceivedTimeNS, EndReceivedTimeNS: request.EndReceivedTimeNS, Limit: 1}, config)
	if err != nil {
		return normalizedReplay{}, err
	}
	return normalizedReplay{DatasetID: query.DatasetID, Family: query.Family, SourceIDs: query.SourceIDs,
		ChannelIDs: query.ChannelIDs, InstrumentUIDs: query.InstrumentUIDs,
		StartReceivedTimeNS: query.StartReceivedTimeNS, EndReceivedTimeNS: query.EndReceivedTimeNS}, nil
}

func (q normalizedQuery) digest() ([sha256.Size]byte, error) {
	encoded, err := json.Marshal(q)
	if err != nil {
		return [sha256.Size]byte{}, err
	}
	return sha256.Sum256(encoded), nil
}

func (q normalizedQuery) spec(dataset warehouse.Dataset, after *warehouse.SortKey) warehouse.QuerySpec {
	return warehouse.QuerySpec{Dataset: dataset, SourceIDs: q.SourceIDs, ChannelIDs: q.ChannelIDs,
		InstrumentUIDs: q.InstrumentUIDs, StartReceivedTimeNS: q.StartReceivedTimeNS,
		EndReceivedTimeNS: q.EndReceivedTimeNS, Limit: q.Limit, After: after}
}

func (r normalizedReplay) serviceRequest(dataset warehouse.Dataset) replay.ServiceRequest {
	return replay.ServiceRequest{DatasetID: dataset.IDString(), Family: dataset.Family,
		CatalogSnapshotID: dataset.CatalogSnapshotIDString(), SchemaName: dataset.SchemaName, SchemaVersion: dataset.SchemaVersion,
		SourceIDs: r.SourceIDs, ChannelIDs: r.ChannelIDs, InstrumentUIDs: r.InstrumentUIDs,
		StartReceivedTimeNS: r.StartReceivedTimeNS, EndReceivedTimeNS: r.EndReceivedTimeNS}
}

func pinQueryDataset(ctx context.Context, resolver DatasetResolver, query normalizedQuery, token *pageTokenPayload) (warehouse.Dataset, error) {
	if resolver == nil {
		return warehouse.Dataset{}, ErrConfiguration
	}
	if token != nil {
		dataset, err := resolver.Dataset(ctx, token.DatasetID)
		if err != nil {
			return warehouse.Dataset{}, err
		}
		if dataset.IDString() != token.DatasetID || (query.DatasetID != "" && dataset.IDString() != query.DatasetID) ||
			(query.Family != "" && dataset.Family != query.Family) {
			return warehouse.Dataset{}, ErrPageToken
		}
		return dataset, dataset.Validate()
	}
	if query.DatasetID != "" {
		dataset, err := resolver.Dataset(ctx, query.DatasetID)
		if err != nil {
			return warehouse.Dataset{}, err
		}
		if dataset.IDString() != query.DatasetID {
			return warehouse.Dataset{}, ErrQueryRequest
		}
		return dataset, dataset.Validate()
	}
	dataset, err := resolver.LatestDataset(ctx, query.Family)
	if err != nil {
		return warehouse.Dataset{}, err
	}
	if dataset.Family != query.Family {
		return warehouse.Dataset{}, ErrQueryRequest
	}
	return dataset, dataset.Validate()
}

func pinReplayDataset(ctx context.Context, resolver DatasetResolver, request normalizedReplay) (warehouse.Dataset, error) {
	query := normalizedQuery{DatasetID: request.DatasetID, Family: request.Family}
	return pinQueryDataset(ctx, resolver, query, nil)
}

type pageTokenPayload struct {
	Version     uint16            `json:"v"`
	QueryDigest string            `json:"q"`
	DatasetID   string            `json:"d"`
	LastKey     warehouse.SortKey `json:"k"`
	ExpiresAt   int64             `json:"e"`
}

func mintPageToken(key []byte, digest [sha256.Size]byte, datasetID string, last warehouse.SortKey, expiresAt time.Time) (string, error) {
	if len(key) < sha256.Size || expiresAt.IsZero() {
		return "", ErrConfiguration
	}
	if _, err := warehouse.ParseHash(datasetID); err != nil {
		return "", ErrQueryRequest
	}
	if err := last.Validate(); err != nil {
		return "", err
	}
	payload, err := json.Marshal(pageTokenPayload{Version: 1, QueryDigest: hex.EncodeToString(digest[:]), DatasetID: datasetID,
		LastKey: last, ExpiresAt: expiresAt.UTC().Unix()})
	if err != nil {
		return "", err
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	signature := mac.Sum(nil)
	return base64.RawURLEncoding.EncodeToString(payload) + "." + base64.RawURLEncoding.EncodeToString(signature), nil
}

func parsePageToken(token string, key []byte, expected [sha256.Size]byte, now time.Time) (pageTokenPayload, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 2 || len(key) < sha256.Size {
		return pageTokenPayload{}, ErrPageToken
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || len(payload) == 0 || len(payload) > 16<<10 {
		return pageTokenPayload{}, ErrPageToken
	}
	signature, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil || len(signature) != sha256.Size {
		return pageTokenPayload{}, ErrPageToken
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(payload)
	if subtle.ConstantTimeCompare(signature, mac.Sum(nil)) != 1 {
		return pageTokenPayload{}, ErrPageToken
	}
	var parsed pageTokenPayload
	decoder := json.NewDecoder(strings.NewReader(string(payload)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&parsed); err != nil {
		return pageTokenPayload{}, ErrPageToken
	}
	if parsed.Version != 1 || parsed.ExpiresAt <= now.UTC().Unix() || parsed.DatasetID == "" || parsed.QueryDigest != hex.EncodeToString(expected[:]) {
		return pageTokenPayload{}, ErrPageToken
	}
	if _, err := warehouse.ParseHash(parsed.DatasetID); err != nil {
		return pageTokenPayload{}, ErrPageToken
	}
	if err := parsed.LastKey.Validate(); err != nil {
		return pageTokenPayload{}, ErrPageToken
	}
	return parsed, nil
}

func normalizeIDs(values []string, minimum, maximum int) ([]string, error) {
	if len(values) < minimum || len(values) > maximum {
		return nil, fmt.Errorf("%w: identifier list bound", ErrQueryRequest)
	}
	result := slices.Clone(values)
	slices.Sort(result)
	for index, value := range result {
		if !validRequestText(value) || (index > 0 && value == result[index-1]) {
			return nil, fmt.Errorf("%w: invalid or duplicate identifier", ErrQueryRequest)
		}
	}
	return result, nil
}

func validateInterval(start, end int64, maximum time.Duration) error {
	if start >= end || uint64(end)-uint64(start) > uint64(maximum.Nanoseconds()) {
		return fmt.Errorf("%w: received-time interval", ErrQueryRequest)
	}
	return nil
}

func validRequestText(value string) bool {
	return value != "" && len(value) <= warehouse.MaxIdentityBytes && utf8.ValidString(value) &&
		strings.IndexFunc(value, unicode.IsControl) < 0
}
