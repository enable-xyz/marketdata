package readmodel

import (
	"context"
	"errors"
	"fmt"

	"github.com/enable-xyz/marketdata/quality"
	"github.com/enable-xyz/marketdata/serve"
	dto "github.com/prometheus/client_model/go"
)

// Metrics projects the bounded Prometheus registry into serve's deliberately
// label-free metrics contract. Values are aggregated across the registry's
// predeclared, cardinality-bounded label dimensions.
type Metrics struct {
	metrics *quality.Metrics
}

func NewMetrics(metrics *quality.Metrics) (*Metrics, error) {
	if metrics == nil || metrics.Registry() == nil {
		return nil, errors.New("readmodel: metrics registry is required")
	}
	return &Metrics{metrics: metrics}, nil
}

func (m *Metrics) Metrics(ctx context.Context) ([]serve.Metric, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	families, err := m.metrics.Registry().Gather()
	if err != nil {
		return nil, fmt.Errorf("readmodel: gather bounded metrics: %w", err)
	}
	result := make([]serve.Metric, 0, len(families)*2)
	for _, family := range families {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		name, help := family.GetName(), family.GetHelp()
		switch family.GetType() {
		case dto.MetricType_COUNTER:
			var total float64
			for _, metric := range family.Metric {
				total += metric.GetCounter().GetValue()
			}
			result = append(result, serve.Metric{Name: name, Help: help, Type: serve.MetricCounter, Value: total})
		case dto.MetricType_GAUGE:
			var total float64
			for _, metric := range family.Metric {
				total += metric.GetGauge().GetValue()
			}
			result = append(result, serve.Metric{Name: name, Help: help, Type: serve.MetricGauge, Value: total})
		case dto.MetricType_HISTOGRAM:
			var count uint64
			var sum float64
			for _, metric := range family.Metric {
				count += metric.GetHistogram().GetSampleCount()
				sum += metric.GetHistogram().GetSampleSum()
			}
			result = append(result,
				serve.Metric{Name: name + "_count", Help: help + " Aggregated observation count.", Type: serve.MetricCounter, Value: float64(count)},
				serve.Metric{Name: name + "_sum", Help: help + " Aggregated observation sum.", Type: serve.MetricCounter, Value: sum},
			)
		default:
			return nil, fmt.Errorf("readmodel: unsupported metric family %q", name)
		}
	}
	return result, nil
}

var _ serve.MetricsReader = (*Metrics)(nil)
