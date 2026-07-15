package influx

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
)

var allowedMetrics = map[string]struct{}{
	"temp_c": {}, "ph": {}, "do_mg_l": {}, "turbidity_ntu": {},
	"salinity_ppt": {}, "battery_v": {}, "rssi": {},
}

type Row struct {
	Time     time.Time
	DeviceID string
	Value    float64
	Sequence int64
}

type Querier interface {
	Query(context.Context, string) ([]Row, error)
}

type WindowReader struct {
	Bucket  string
	Querier Querier
}

func (reader WindowReader) Evaluate(ctx context.Context, rule alertevaluator.EvaluationRule, spec alertevaluator.WindowSpec) (alertevaluator.WindowResult, error) {
	query, err := BuildQuery(reader.Bucket, rule, spec)
	if err != nil {
		return alertevaluator.WindowResult{}, err
	}
	rows, err := reader.Querier.Query(ctx, query)
	if err != nil {
		return alertevaluator.WindowResult{}, fmt.Errorf("query InfluxDB window: %w", err)
	}
	samples := make([]alertevaluator.Sample, 0, len(rows))
	for _, row := range rows {
		if row.Time.IsZero() || math.IsNaN(row.Value) || math.IsInf(row.Value, 0) {
			continue
		}
		samples = append(samples, alertevaluator.Sample{
			Time: row.Time.UTC(), DeviceID: row.DeviceID, Value: row.Value, Seq: row.Sequence,
		})
	}
	return alertevaluator.EvaluateSamples(spec, samples), nil
}

func BuildQuery(bucket string, rule alertevaluator.EvaluationRule, spec alertevaluator.WindowSpec) (string, error) {
	if _, ok := allowedMetrics[rule.Metric]; !ok {
		return "", fmt.Errorf("unsupported alert metric %q", rule.Metric)
	}
	if !spec.End.After(spec.Start) {
		return "", fmt.Errorf("window end must be after start")
	}
	filters := fmt.Sprintf(
		`  |> filter(fn: (r) => r["_measurement"] == "water_quality")
  |> filter(fn: (r) => r["tenant_id"] == %s)
  |> filter(fn: (r) => r["pond_id"] == %s)`,
		fluxString(rule.TenantID), fluxString(rule.PondID),
	)
	if rule.DeviceID != "" {
		filters += fmt.Sprintf("\n  |> filter(fn: (r) => r[\"device_id\"] == %s)", fluxString(rule.DeviceID))
	}
	return fmt.Sprintf(
		`from(bucket: %s)
  |> range(start: time(v: %s), stop: time(v: %s))
%s
  |> filter(fn: (r) => r["_field"] == %s or r["_field"] == "seq")
  |> pivot(rowKey: ["_time", "device_id"], columnKey: ["_field"], valueColumn: "_value")
  |> keep(columns: ["_time", "device_id", %s, "seq"])
  |> sort(columns: ["_time", "device_id"])`,
		fluxString(bucket), fluxString(spec.Start.UTC().Format(time.RFC3339Nano)),
		fluxString(spec.End.UTC().Format(time.RFC3339Nano)), filters,
		fluxString(rule.Metric), fluxString(rule.Metric),
	), nil
}

func fluxString(value string) string {
	encoded, _ := json.Marshal(value)
	return string(encoded)
}
