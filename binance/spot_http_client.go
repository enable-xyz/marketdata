package binance

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const SpotPublicRESTEndpoint = "https://data-api.binance.vision"

// PublicSpotRESTClient executes only unauthenticated GET /api/v3/depth against
// the market-data-only Binance host. The caller owns the explicit transport and
// timeout; redirects and ambient http defaults are disabled.
type PublicSpotRESTClient struct {
	base   *url.URL
	client *http.Client
}

func NewPublicSpotRESTClient(endpoint string, client *http.Client) (*PublicSpotRESTClient, error) {
	if client == nil || client.Transport == nil || client.Timeout <= 0 {
		return nil, fmt.Errorf("%w: explicit HTTP transport and timeout are required", ErrSpotConfiguration)
	}
	base, err := url.Parse(endpoint)
	if err != nil || endpoint != SpotPublicRESTEndpoint || base.Scheme != "https" || base.Host != "data-api.binance.vision" || base.Path != "" || base.RawQuery != "" || base.User != nil {
		return nil, fmt.Errorf("%w: REST endpoint is not the allowlisted market-data-only host", ErrSpotConfiguration)
	}
	copyClient := *client
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return errors.New("binance: REST redirects are disabled")
	}
	return &PublicSpotRESTClient{base: base, client: &copyClient}, nil
}

func (c *PublicSpotRESTClient) Do(ctx context.Context, request SpotDepthRequest, maxResponseBytes uint32) (SpotRESTResponse, error) {
	if err := ctx.Err(); err != nil {
		return SpotRESTResponse{}, err
	}
	canonical, err := cloneAndValidateDepthRequest(request)
	if err != nil {
		return SpotRESTResponse{}, err
	}
	if canonical.Method != "GET" || canonical.Endpoint != SpotDepthEndpoint || maxResponseBytes == 0 || maxResponseBytes > SpotMaxRawPayloadBytes {
		return SpotRESTResponse{}, fmt.Errorf("%w: REST request exceeds the public contract", ErrSpotConfiguration)
	}
	endpoint := *c.base
	endpoint.Path = SpotDepthEndpoint
	query := url.Values{}
	query.Set("limit", strconv.Itoa(canonical.Limit))
	query.Set("symbol", canonical.Symbol)
	endpoint.RawQuery = query.Encode()

	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return SpotRESTResponse{}, fmt.Errorf("binance: construct public depth request: %w", err)
	}
	for _, header := range canonical.Headers {
		if header.Kind != "time-unit" || header.Value != "MICROSECOND" {
			return SpotRESTResponse{}, fmt.Errorf("%w: non-public request header", ErrSpotConfiguration)
		}
		httpRequest.Header.Set("X-MBX-TIME-UNIT", header.Value)
	}
	if httpRequest.Header.Get("Authorization") != "" || httpRequest.Header.Get("X-MBX-APIKEY") != "" {
		return SpotRESTResponse{}, fmt.Errorf("%w: authentication headers are prohibited", ErrSpotConfiguration)
	}

	response, err := c.client.Do(httpRequest)
	if err != nil {
		return SpotRESTResponse{}, fmt.Errorf("binance: execute public depth request: %w", err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, int64(maxResponseBytes)+1))
	if err != nil {
		return SpotRESTResponse{}, fmt.Errorf("binance: read bounded depth response: %w", err)
	}
	if len(body) > int(maxResponseBytes) {
		return SpotRESTResponse{}, ErrSpotBounds
	}
	return SpotRESTResponse{
		Status:  response.StatusCode,
		Headers: publicSpotResponseHeaders(response.Header),
		Body:    body,
	}, nil
}

func publicSpotResponseHeaders(headers http.Header) []SpotHTTPHeader {
	result := make([]SpotHTTPHeader, 0, 8)
	for _, name := range []string{"Content-Length", "Content-Type", "Retry-After"} {
		values := slices.Clone(headers.Values(name))
		slices.Sort(values)
		for _, value := range values {
			result = append(result, SpotHTTPHeader{Name: name, Value: value})
		}
	}
	usedWeightNames := make([]string, 0, 4)
	for name := range headers {
		if strings.HasPrefix(strings.ToLower(name), "x-mbx-used-weight-") {
			usedWeightNames = append(usedWeightNames, name)
		}
	}
	slices.Sort(usedWeightNames)
	for _, name := range usedWeightNames {
		values := slices.Clone(headers.Values(name))
		slices.Sort(values)
		for _, value := range values {
			result = append(result, SpotHTTPHeader{Name: name, Value: value})
		}
	}
	if len(result) > SpotMaxResponseHeaders {
		return result[:SpotMaxResponseHeaders]
	}
	return result
}
