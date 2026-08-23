package hyperliquid

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
)

type InfoRequest struct {
	requestType string
	body        []byte
	weight      uint32
}

func (r InfoRequest) Type() string   { return r.requestType }
func (r InfoRequest) Weight() uint32 { return r.weight }
func (r InfoRequest) Bytes() []byte  { return slices.Clone(r.body) }

func PerpDexsRequest() (InfoRequest, error) {
	return newInfoRequest("perpDexs", struct {
		Type string `json:"type"`
	}{Type: "perpDexs"})
}

func PerpMetadataRequest(dexName string, withContexts bool) (InfoRequest, error) {
	if dexName != "" && (len(dexName) > 64 || !validCoin(dexName)) {
		return InfoRequest{}, ErrInvalidPayload
	}
	requestType := "meta"
	if withContexts {
		requestType = "metaAndAssetCtxs"
	}
	return newInfoRequest(requestType, struct {
		Type string `json:"type"`
		DEX  string `json:"dex,omitempty"`
	}{Type: requestType, DEX: dexName})
}

func SpotMetadataRequest(withContexts bool) (InfoRequest, error) {
	requestType := "spotMeta"
	if withContexts {
		requestType = "spotMetaAndAssetCtxs"
	}
	return newInfoRequest(requestType, struct {
		Type string `json:"type"`
	}{Type: requestType})
}

func FundingHistoryRequest(coin string, startTimeMS int64, endTimeMS *int64) (InfoRequest, error) {
	if !validCoin(coin) || startTimeMS < 0 || (endTimeMS != nil && *endTimeMS < startTimeMS) {
		return InfoRequest{}, ErrInvalidPayload
	}
	return newInfoRequest("fundingHistory", struct {
		Type      string `json:"type"`
		Coin      string `json:"coin"`
		StartTime int64  `json:"startTime"`
		EndTime   *int64 `json:"endTime,omitempty"`
	}{Type: "fundingHistory", Coin: coin, StartTime: startTimeMS, EndTime: endTimeMS})
}

func L2BookRequest(coin string, nSigFigs, mantissa uint8) (InfoRequest, error) {
	contract := BookDepthContract{NSigFigs: nSigFigs, Mantissa: mantissa}
	if !validCoin(coin) || contract.Validate() != nil {
		return InfoRequest{}, ErrInvalidPayload
	}
	var sig, man *uint8
	if nSigFigs != 0 {
		value := nSigFigs
		sig = &value
	}
	if mantissa != 0 {
		value := mantissa
		man = &value
	}
	return newInfoRequest("l2Book", struct {
		Type     string `json:"type"`
		Coin     string `json:"coin"`
		NSigFigs *uint8 `json:"nSigFigs,omitempty"`
		Mantissa *uint8 `json:"mantissa,omitempty"`
	}{Type: "l2Book", Coin: coin, NSigFigs: sig, Mantissa: man})
}

func newInfoRequest(requestType string, body any) (InfoRequest, error) {
	weight, err := InfoRequestWeight(requestType)
	if err != nil {
		return InfoRequest{}, err
	}
	encoded, err := json.Marshal(body)
	if err != nil || len(encoded) == 0 || len(encoded) > MaxRawPayloadBytes {
		return InfoRequest{}, ErrInvalidPayload
	}
	return InfoRequest{requestType: requestType, body: encoded, weight: weight}, nil
}

type InfoClient struct {
	network  Network
	endpoint string
	client   http.Client
	maximum  int64
	limiter  *WeightedLimiter
}

func NewInfoClient(network Network, client *http.Client, maximum int64, limiter *WeightedLimiter) (*InfoClient, error) {
	if err := network.Validate(); err != nil {
		return nil, err
	}
	if client == nil || client.Transport == nil || client.Timeout <= 0 || maximum <= 0 || maximum > MaxRawPayloadBytes || limiter == nil {
		return nil, fmt.Errorf("%w: explicit bounded HTTP client, payload limit, and weighted limiter are required", ErrInvalidPayload)
	}
	endpoint := network.InfoEndpoint()
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || parsed.Path != "/info" || !allowlistedAPIHost(network, parsed.Host) {
		return nil, fmt.Errorf("%w: endpoint is not allowlisted", ErrInvalidPayload)
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("hyperliquid: Info redirects are disabled")
	}
	return &InfoClient{network: network, endpoint: endpoint, client: copyClient, maximum: maximum, limiter: limiter}, nil
}

func (c *InfoClient) Do(ctx context.Context, request InfoRequest) (*RawEvidence, error) {
	if c == nil || c.limiter == nil || request.requestType == "" || request.weight == 0 || len(request.body) == 0 || !json.Valid(request.body) {
		return nil, ErrInvalidPayload
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := c.limiter.Reserve(request.weight); err != nil {
		return nil, err
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(request.body))
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: build Info request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := c.client.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("hyperliquid: Info request: %w", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, c.maximum+1))
	if err != nil || int64(len(payload)) > c.maximum {
		return nil, ErrInvalidPayload
	}
	evidence, evidenceErr := newRawEvidence(payload)
	if evidenceErr != nil {
		return nil, evidenceErr
	}
	returnedItems, err := responseDependentItemCount(request.requestType, payload)
	if err != nil {
		return evidence, err
	}
	finalWeight, err := InfoFinalWeight(request.requestType, returnedItems)
	if err != nil {
		return evidence, err
	}
	if err := c.limiter.Reconcile(finalWeight - request.weight); err != nil {
		return evidence, err
	}
	if response.StatusCode != http.StatusOK {
		return evidence, fmt.Errorf("hyperliquid: Info status %d", response.StatusCode)
	}
	return evidence, nil
}

func responseDependentItemCount(requestType string, payload []byte) (int, error) {
	if requestType != "fundingHistory" {
		return 0, nil
	}
	var entries []json.RawMessage
	if json.Unmarshal(payload, &entries) != nil {
		return 0, ErrInvalidPayload
	}
	return len(entries), nil
}
