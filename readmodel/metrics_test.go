package readmodel

import (
	"testing"

	"github.com/enable-xyz/marketdata/quality"
)

func TestMetricsAggregatesBoundedDimensions(t *testing.T) {
	metrics, err := quality.NewMetrics(quality.MetricConfig{Sources: []string{"source"}, Channels: []string{"trade"}, Families: []string{"trade"}, MaxSeries: 100_000})
	if err != nil {
		t.Fatal(err)
	}
	attributes := quality.MetricAttributes{"source": "source", "channel": "trade", "family": "trade", "outcome": "observed"}
	if err := metrics.Record(quality.SignalChannelMessages, 2, attributes); err != nil {
		t.Fatal(err)
	}
	if err := metrics.Record(quality.SignalExchangeLag, 0.25, attributes); err != nil {
		t.Fatal(err)
	}
	reader, err := NewMetrics(metrics)
	if err != nil {
		t.Fatal(err)
	}
	values, err := reader.Metrics(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]float64{}
	for _, value := range values {
		found[value.Name] = value.Value
	}
	if found["enable_market_channel_messages_total"] != 2 {
		t.Fatalf("message total = %v", found["enable_market_channel_messages_total"])
	}
	if found["enable_market_exchange_lag_seconds_count"] != 1 || found["enable_market_exchange_lag_seconds_sum"] != 0.25 {
		t.Fatalf("exchange lag aggregates = count %v sum %v", found["enable_market_exchange_lag_seconds_count"], found["enable_market_exchange_lag_seconds_sum"])
	}
}
