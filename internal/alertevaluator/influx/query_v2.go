package influx

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"

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
	if err == nil {
		var rows []V2Row
		if rows, err = reader.Querier.Query(ctx, query); err == nil {
			return alertevaluator.EvaluateSamples(spec, defaultComponentSamples(rule.TenantID, rows)), nil
		}
		err = fmt.Errorf("query InfluxDB v2 window: %w", err)
	}
	return alertevaluator.WindowResult{}, err
}

// defaultComponentSamples keeps the rows of each device's legacy default
// Component with a finite value and maps the -1 sequence sentinel to 0.
func defaultComponentSamples(tenantID string, rows []V2Row) []alertevaluator.Sample {
	samples := make([]alertevaluator.Sample, 0, len(rows))
	for _, row := range rows {
		keep := row.ComponentID == DefaultComponentIDForLegacyDevice(tenantID, row.DeviceID) &&
			!row.Time.IsZero() && !math.IsNaN(row.Value) && !math.IsInf(row.Value, 0)
		if !keep {
			continue
		}
		sample := alertevaluator.Sample{Time: row.Time.UTC(), DeviceID: row.DeviceID, Value: row.Value, Seq: row.Sequence}
		if sample.Seq == absentSequence {
			sample.Seq = 0
		}
		samples = append(samples, sample)
	}
	return samples
}

// BuildV2Query builds the observation_numeric window query for a /v1 rule.
// Rule IDs are escaped rather than validated: /v1 IDs are plain strings, the
// same model as the legacy reader. Every literal goes through fluxV2String
// (P2 plan D19), so in Flux it decodes to exactly its value and can neither
// interpolate nor end early; a value that cannot round-trip is an error and
// nothing is sent.
func BuildV2Query(bucket string, rule alertevaluator.EvaluationRule, spec alertevaluator.WindowSpec) (string, error) {
	metricKey, ok := MetricKeyForLegacyMetric(rule.Metric)
	if !ok {
		return "", fmt.Errorf("unsupported alert metric %q", rule.Metric)
	}
	if !spec.End.After(spec.Start) {
		return "", fmt.Errorf("window end must be after start")
	}
	var err error
	literal := func(name, value string) string {
		encoded, encodeErr := fluxV2String(value)
		if encodeErr != nil && err == nil {
			err = fmt.Errorf("%s: %w", name, encodeErr)
		}
		return encoded
	}
	filters := fmt.Sprintf(
		`  |> filter(fn: (r) => r["_measurement"] == "observation_numeric")
  |> filter(fn: (r) => r["tenant_id"] == %s)
  |> filter(fn: (r) => r["asset_id"] == %s)`,
		literal("tenant ID", rule.TenantID), literal("asset ID", AssetIDForLegacyPond(rule.TenantID, rule.PondID)),
	)
	if rule.DeviceID != "" {
		filters += fmt.Sprintf("\n  |> filter(fn: (r) => r[\"device_id\"] == %s)", literal("device ID", rule.DeviceID))
	}
	filters += fmt.Sprintf("\n  |> filter(fn: (r) => r[\"metric_key\"] == %s)", literal("metric key", metricKey))
	query := fmt.Sprintf(
		`from(bucket: %s)
  |> range(start: time(v: %s), stop: time(v: %s))
%s
  |> filter(fn: (r) => r["_field"] == "value" or r["_field"] == "sequence" or r["_field"] == "quality_state")
  |> pivot(rowKey: ["_time", "device_id", "component_id"], columnKey: ["_field"], valueColumn: "_value")
  |> filter(fn: (r) => r["quality_state"] == "valid")
  |> keep(columns: ["_time", "device_id", "component_id", "value", "sequence"])
  |> group()
  |> sort(columns: ["_time", "device_id", "component_id"])`,
		literal("bucket", bucket), literal("window start", spec.Start.UTC().Format(time.RFC3339Nano)),
		literal("window end", spec.End.UTC().Format(time.RFC3339Nano)), filters,
	)
	if err != nil {
		return "", err
	}
	return query, nil
}

// fluxV2Finish turns json.Encoder output into a Flux literal. encoding/json
// escapes U+2028 and U+2029 even without HTML escaping, and Flux has no \u
// escape, so both go back to raw characters; matching `\\` first keeps an
// escaped backslash followed by "u2028" intact. Every `$` becomes `\x24`.
var fluxV2Finish = strings.NewReplacer(`\\`, `\\`, `\u2028`, "\u2028", `\u2029`, "\u2029", "$", `\x24`)

// fluxV2String builds a lossless Flux string literal (P2 plan D19, #84): JSON
// without non-ASCII or HTML escapes, with every `$` written as `\x24`. Invalid
// UTF-8 and C0 controls other than \n, \r and \t cannot round-trip, so they are
// errors. The legacy query keeps fluxString unchanged.
func fluxV2String(value string) (string, error) {
	if !utf8.ValidString(value) {
		return "", fmt.Errorf("flux string literal is not valid UTF-8")
	}
	if index := strings.IndexFunc(value, rejectedFluxControl); index >= 0 {
		return "", fmt.Errorf("flux string literal holds control character %U", rune(value[index]))
	}
	var encoded strings.Builder
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return "", fmt.Errorf("encode flux string literal: %w", err)
	}
	return fluxV2Finish.Replace(strings.TrimSuffix(encoded.String(), "\n")), nil
}

func rejectedFluxControl(char rune) bool {
	return char < 0x20 && char != '\n' && char != '\r' && char != '\t'
}
