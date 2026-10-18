package influx

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
)

// absentSequence is the observation_numeric sentinel for a missing sequence.
const absentSequence = -1

type V2Row struct {
	Time        time.Time
	DeviceID    string
	ComponentID string
	Value       float64
	Sequence    int64
}

type V2Querier interface {
	Query(context.Context, string) ([]V2Row, error)
}

// V2WindowReader evaluates /v1 rules over observation_numeric. It keeps only
// legacy default Components and quality_state "valid" points.
type V2WindowReader struct {
	Bucket  string
	Querier V2Querier
}

func (reader V2WindowReader) Evaluate(ctx context.Context, rule alertevaluator.EvaluationRule, spec alertevaluator.WindowSpec) (alertevaluator.WindowResult, error) {
	query, err := BuildV2Query(reader.Bucket, rule, spec)
	if err != nil {
		return alertevaluator.WindowResult{}, err
	}
	rows, err := reader.Querier.Query(ctx, query)
	if err != nil {
		return alertevaluator.WindowResult{}, fmt.Errorf("query InfluxDB v2 window: %w", err)
	}
	samples := make([]alertevaluator.Sample, 0, len(rows))
	for _, row := range rows {
		if row.Time.IsZero() || math.IsNaN(row.Value) || math.IsInf(row.Value, 0) {
			continue
		}
		if row.ComponentID != DefaultComponentIDForLegacyDevice(rule.TenantID, row.DeviceID) {
			continue
		}
		sequence := row.Sequence
		if sequence == absentSequence {
			sequence = 0
		}
		samples = append(samples, alertevaluator.Sample{
			Time: row.Time.UTC(), DeviceID: row.DeviceID, Value: row.Value, Seq: sequence,
		})
	}
	return alertevaluator.EvaluateSamples(spec, samples), nil
}

// BuildV2Query builds the observation_numeric window query for a /v1 rule.
// Rule IDs are escaped rather than validated: /v1 IDs are plain strings that
// the legacy reader accepts, and escaping alone defeats Flux injection.
func BuildV2Query(bucket string, rule alertevaluator.EvaluationRule, spec alertevaluator.WindowSpec) (string, error) {
	metricKey, ok := MetricKeyForLegacyMetric(rule.Metric)
	if !ok {
		return "", fmt.Errorf("unsupported alert metric %q", rule.Metric)
	}
	if !spec.End.After(spec.Start) {
		return "", fmt.Errorf("window end must be after start")
	}
	filters := fmt.Sprintf(
		`  |> filter(fn: (r) => r["_measurement"] == "observation_numeric")
  |> filter(fn: (r) => r["tenant_id"] == %s)
  |> filter(fn: (r) => r["asset_id"] == %s)`,
		fluxV2String(rule.TenantID), fluxV2String(AssetIDForLegacyPond(rule.TenantID, rule.PondID)),
	)
	if rule.DeviceID != "" {
		filters += fmt.Sprintf("\n  |> filter(fn: (r) => r[\"device_id\"] == %s)", fluxV2String(rule.DeviceID))
	}
	filters += fmt.Sprintf("\n  |> filter(fn: (r) => r[\"metric_key\"] == %s)", fluxV2String(metricKey))
	return fmt.Sprintf(
		`from(bucket: %s)
  |> range(start: time(v: %s), stop: time(v: %s))
%s
  |> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "sequence" or r["_field"] == "quality_state")
  |> pivot(rowKey: ["_time", "device_id", "component_id"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => r["quality_state"] == "valid")
  |> keep(columns: ["_time", "device_id", "component_id", "value", "sequence"])
  |> group()
  |> sort(columns: ["_time", "device_id", "component_id"])`,
		fluxV2String(bucket), fluxV2String(spec.Start.UTC().Format(time.RFC3339Nano)),
		fluxV2String(spec.End.UTC().Format(time.RFC3339Nano)), filters,
	), nil
}

// fluxV2String is fluxString plus the `${` escape that defeats Flux string
// interpolation; a lone `$` stays as is. The legacy query keeps fluxString.
func fluxV2String(value string) string {
	return strings.ReplaceAll(fluxString(value), "${", `\${`)
}
