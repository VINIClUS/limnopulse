package influx

import (
	"context"
	"fmt"

	api "github.com/influxdata/influxdb-client-go/v2/api"
)

type ClientQuerier struct {
	API api.QueryAPI
}

func (querier ClientQuerier) Query(ctx context.Context, flux string) ([]Row, error) {
	result, err := querier.API.Query(ctx, flux)
	if err != nil {
		return nil, err
	}
	var rows []Row
	for result.Next() {
		record := result.Record()
		value, ok := metricValue(record.Values())
		if !ok {
			continue
		}
		sequence, _ := integer(record.ValueByKey("seq"))
		rows = append(rows, Row{
			Time: record.Time(), DeviceID: fmt.Sprint(record.ValueByKey("device_id")),
			Value: value, Sequence: sequence,
		})
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}

func metricValue(values map[string]interface{}) (float64, bool) {
	for metric := range allowedMetrics {
		if value, ok := numeric(values[metric]); ok {
			return value, true
		}
	}
	return 0, false
}

func numeric(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case int64:
		return float64(typed), true
	case uint64:
		return float64(typed), true
	default:
		return 0, false
	}
}

func integer(value any) (int64, bool) {
	switch typed := value.(type) {
	case int64:
		return typed, true
	case uint64:
		return int64(typed), true
	case float64:
		return int64(typed), true
	default:
		return 0, false
	}
}
