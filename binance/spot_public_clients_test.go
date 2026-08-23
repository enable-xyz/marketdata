package binance

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type spotRoundTripFunc func(*http.Request) (*http.Response, error)

func (f spotRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestPublicSpotRESTClientUsesOnlyPinnedMarketDataRequest(t *testing.T) {
	const snapshot = `{"lastUpdateId":156,"bids":[],"asks":[]}`
	transport := spotRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet || request.URL.String() != "https://data-api.binance.vision/api/v3/depth?limit=100&symbol=BNBBTC" {
			t.Fatalf("unexpected public request: %s %s", request.Method, request.URL)
		}
		if request.Header.Get("Authorization") != "" || request.Header.Get("X-MBX-APIKEY") != "" {
			t.Fatal("public request carried authentication")
		}
		return &http.Response{
			StatusCode: 200,
			Header:     http.Header{"Content-Type": []string{"application/json"}, "X-Mbx-Used-Weight-1m": []string{"5"}},
			Body:       io.NopCloser(strings.NewReader(snapshot)),
			Request:    request,
		}, nil
	})
	client, err := NewPublicSpotRESTClient(SpotPublicRESTEndpoint, &http.Client{Transport: transport, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewSpotDepthRequest("test-depth", "BNBBTC", 100, false)
	if err != nil {
		t.Fatal(err)
	}
	response, err := client.Do(t.Context(), request, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if response.Status != 200 || string(response.Body) != snapshot {
		t.Fatalf("unexpected response: %+v", response)
	}
}

func TestPublicSpotClientsFailClosedOnAmbientOrNonPublicConfiguration(t *testing.T) {
	if _, err := NewPublicSpotRESTClient(SpotPublicRESTEndpoint, &http.Client{Timeout: time.Second}); err == nil {
		t.Fatal("REST client accepted an ambient transport")
	}
	if _, err := NewPublicSpotRESTClient("https://api.binance.com", &http.Client{Transport: spotRoundTripFunc(nil), Timeout: time.Second}); err == nil {
		t.Fatal("REST client accepted the trading host")
	}
	connector, err := NewCoderSpotWSConnector(&http.Client{Transport: spotRoundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Fatal("rejected WebSocket endpoint reached the network transport")
		return nil, nil
	}), Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := connector.Connect(context.Background(), SpotWSConnectRequest{Endpoint: "wss://stream.binance.com/ws", MaxApplicationBytes: 1024}); err == nil {
		t.Fatal("WebSocket connector accepted a non-public endpoint")
	}
}
