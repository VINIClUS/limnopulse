package workermonitor

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
)

// MetricPublisher is the part of *cloudwatch.Client the monitor uses; tests use a fake.
type MetricPublisher interface {
	PutMetricData(ctx context.Context, params *cloudwatch.PutMetricDataInput, optFns ...func(*cloudwatch.Options)) (*cloudwatch.PutMetricDataOutput, error)
}

// Monitor runs one unit-health pass.
type Monitor struct {
	Systemctl Systemctl
	Publisher MetricPublisher
	Now       func() time.Time
}

// Summary is what one pass judged and how many data points it sent.
type Summary struct {
	Published int      `json:"published"`
	Roles     []Report `json:"roles"`
}

// Run queries every monitored role's units, then sends one UnitHealthy datum per enabled
// role in a single PutMetricData call. A failed query publishes nothing; a failed query or
// publish is a runtime error (exit 1), and the alarms then see missing data, which
// breaches. With no enabled role it makes no call at all.
func (monitor Monitor) Run(ctx context.Context, table Table) (Summary, error) {
	states := make(map[string]UnitState)
	for _, role := range table.Monitored() {
		for _, unit := range []string{role.Units.Service, role.Units.Timer} {
			if unit == "" {
				continue
			}
			state, err := monitor.Systemctl.Show(ctx, unit)
			if err != nil {
				return Summary{}, err
			}
			states[unit] = state
		}
	}
	now := monitor.Now()
	summary := Summary{Roles: Evaluate(table, states, now)}
	data := metricData(table.UnitHealth, summary.Roles, now)
	if len(data) == 0 {
		return summary, nil
	}
	_, err := monitor.Publisher.PutMetricData(ctx, &cloudwatch.PutMetricDataInput{
		Namespace:  aws.String(table.UnitHealth.Namespace),
		MetricData: data,
	})
	if err != nil {
		return summary, fmt.Errorf("put %s %s: %w", table.UnitHealth.Namespace, table.UnitHealth.Metric, err)
	}
	summary.Published = len(data)
	return summary, nil
}

func metricData(health UnitHealth, reports []Report, now time.Time) []types.MetricDatum {
	var data []types.MetricDatum
	for _, report := range reports {
		if !report.Enabled {
			continue
		}
		value := 0.0
		if report.Healthy {
			value = 1
		}
		data = append(data, types.MetricDatum{
			MetricName: aws.String(health.Metric),
			Dimensions: []types.Dimension{{Name: aws.String(health.Dimension), Value: aws.String(report.Role)}},
			Timestamp:  aws.Time(now),
			Unit:       types.StandardUnitNone,
			Value:      aws.Float64(value),
		})
	}
	return data
}
