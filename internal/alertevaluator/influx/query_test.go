package influx

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
)

type fakeQuerier struct {
	query string
	rows  []Row
	err   error
}

func (query *fakeQuerier) Query(_ context.Context, flux string) ([]Row, error) {
	query.query = flux
	return query.rows, query.err
}

func TestBuildQueryEscapesAllRuleControlledLiterals(t *testing.T) {
	start := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)
	query, err := BuildQuery("raw\"bucket", alertevaluator.EvaluationRule{
		Rule: alertevaluator.Rule{TenantID: "tenant\"x"}, PondID: "pond\\x", DeviceID: "dev\nx", Metric: "ph",
	}, alertevaluator.WindowSpec{Start: start, End: start.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{`from(bucket: "raw\"bucket")`, `r["tenant_id"] == "tenant\"x"`, `r["pond_id"] == "pond\\x"`, `r["device_id"] == "dev\nx"`, `r["_field"] == "ph"`} {
		if !strings.Contains(query, want) {
			t.Fatalf("query missing %q:\n%s", want, query)
		}
	}
	if !strings.Contains(query, `range(start: time(v: "2026-07-15T12:00:00Z"), stop: time(v: "2026-07-15T12:01:00Z"))`) {
		t.Fatalf("query has unbounded or unstable range:\n%s", query)
	}
}

func TestBuildQueryRejectsUnknownMetricAndInvalidWindow(t *testing.T) {
	start := time.Now().UTC()
	for _, test := range []struct {
		metric string
		start  time.Time
		end    time.Time
	}{
		{metric: `ph\") |> yield()`, start: start, end: start.Add(time.Minute)},
		{metric: "ph", start: start, end: start},
	} {
		if _, err := BuildQuery("raw", alertevaluator.EvaluationRule{Metric: test.metric}, alertevaluator.WindowSpec{Start: test.start, End: test.end}); err == nil {
			t.Fatalf("BuildQuery(%q) unexpectedly succeeded", test.metric)
		}
	}
}

func TestWindowReaderDelegatesRowsToCanonicalQualityEvaluation(t *testing.T) {
	end := time.Date(2026, 7, 15, 12, 1, 0, 0, time.UTC)
	query := &fakeQuerier{}
	for offset := time.Duration(0); offset < time.Minute; offset += 10 * time.Second {
		query.rows = append(query.rows, Row{Time: end.Add(-time.Minute).Add(offset), DeviceID: "dev_1", Value: 4.2, Sequence: int64(offset / time.Second)})
	}
	reader := WindowReader{Bucket: "raw", Querier: query}
	spec := alertevaluator.WindowSpec{
		Start: end.Add(-time.Minute), End: end, ExpectedSampleInterval: 10 * time.Second,
		EvaluationCadence: time.Minute, MinimumCoverageRatio: .8, MinimumPoints: 3,
		MaximumSampleAge: 30 * time.Second, Aggregation: alertevaluator.AggregationMean,
	}
	result, err := reader.Evaluate(context.Background(), alertevaluator.EvaluationRule{
		Rule: alertevaluator.Rule{TenantID: "tnt_1"}, PondID: "pond_1", Metric: "ph",
	}, spec)
	if err != nil {
		t.Fatal(err)
	}
	if result.Quality != alertevaluator.QualitySufficient || result.Value != 4.2 || result.ValidPointCount != 6 {
		t.Fatalf("result = %#v", result)
	}
}

func TestWindowReaderPropagatesQueryFailure(t *testing.T) {
	query := &fakeQuerier{err: errors.New("influx unavailable")}
	reader := WindowReader{Bucket: "raw", Querier: query}
	start := time.Now().UTC()
	_, err := reader.Evaluate(context.Background(), alertevaluator.EvaluationRule{Metric: "ph"}, alertevaluator.WindowSpec{Start: start, End: start.Add(time.Minute)})
	if err == nil || !strings.Contains(err.Error(), "influx unavailable") {
		t.Fatalf("err = %v", err)
	}
}
