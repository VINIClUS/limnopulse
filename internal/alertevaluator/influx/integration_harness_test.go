package influx

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/domain"
)

const (
	harnessSmokeMeasurement = "harness_smoke"
	harnessSmokeLine        = harnessSmokeMeasurement + ",probe=p1 value=21.5 1767268800000000000"
)

var harnessSmokeTime = time.Date(2026, time.January, 1, 12, 0, 0, 0, time.UTC)

type harnessSmokeRow struct {
	measurement string
	field       string
	probe       string
	value       any
	time        time.Time
}

func harnessSmokeRows(t *testing.T, client influxdb2.Client, bucket *domain.Bucket) []harnessSmokeRow {
	t.Helper()
	flux := fmt.Sprintf(
		`from(bucket: %q) |> range(start: 2026-01-01T00:00:00Z, stop: 2026-01-02T00:00:00Z) |> filter(fn: (r) => r._measurement == %q)`,
		bucket.Name, harnessSmokeMeasurement,
	)
	result, err := client.QueryAPI(*bucket.OrgID).Query(context.Background(), flux)
	if err != nil {
		t.Fatalf("query bucket %q: %v", bucket.Name, err)
	}
	var rows []harnessSmokeRow
	for result.Next() {
		record := result.Record()
		rows = append(rows, harnessSmokeRow{
			measurement: record.Measurement(),
			field:       record.Field(),
			probe:       fmt.Sprint(record.ValueByKey("probe")),
			value:       record.Value(),
			time:        record.Time(),
		})
	}
	if err := result.Err(); err != nil {
		t.Fatalf("read bucket %q: %v", bucket.Name, err)
	}
	return rows
}

func assertNoRetentionRule(t *testing.T, bucket *domain.Bucket) {
	t.Helper()
	for _, rule := range bucket.RetentionRules {
		if rule.EverySeconds != 0 {
			t.Fatalf("bucket %q has retention rule %+v, want none", bucket.Name, rule)
		}
	}
}

func TestInfluxIntegrationHarnessSmoke(t *testing.T) {
	ctx := context.Background()
	outerClient, outerBucket := newIntegrationBucket(t)
	assertNoRetentionRule(t, outerBucket)

	var inner *domain.Bucket
	t.Run("writes and reads one point in an isolated bucket", func(t *testing.T) {
		client, bucket := newIntegrationBucket(t)
		inner = bucket
		if bucket.Name == outerBucket.Name {
			t.Fatalf("buckets are not unique: both named %q", bucket.Name)
		}
		assertNoRetentionRule(t, bucket)
		found, err := client.BucketsAPI().FindBucketByName(ctx, bucket.Name)
		if err != nil || found.Id == nil || bucket.Id == nil || *found.Id != *bucket.Id {
			t.Fatalf("bucket %q is not on the server: %v", bucket.Name, err)
		}

		writeLines(t, client, bucket, harnessSmokeLine)

		rows := harnessSmokeRows(t, client, bucket)
		if len(rows) == 0 {
			t.Fatal("Flux read returned no rows for the written point")
		}
		want := harnessSmokeRow{
			measurement: harnessSmokeMeasurement,
			field:       "value",
			probe:       "p1",
			value:       21.5,
			time:        harnessSmokeTime,
		}
		if len(rows) != 1 || rows[0].measurement != want.measurement || rows[0].field != want.field ||
			rows[0].probe != want.probe || rows[0].value != want.value || !rows[0].time.Equal(want.time) {
			t.Fatalf("rows = %+v, want exactly [%+v]", rows, want)
		}
	})
	if inner == nil {
		t.Fatal("the subtest did not create a bucket")
	}

	if rows := harnessSmokeRows(t, outerClient, outerBucket); len(rows) != 0 {
		t.Fatalf("outer bucket %q sees the inner bucket's data: %+v", outerBucket.Name, rows)
	}
	_, err := outerClient.BucketsAPI().FindBucketByName(ctx, inner.Name)
	if err == nil {
		t.Fatalf("bucket %q survived its test's cleanup", inner.Name)
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("look up deleted bucket %q: %v", inner.Name, err)
	}
}

func TestIntegrationHarnessSkipsWithoutOptIn(t *testing.T) {
	for _, value := range []string{"", "0", "true", "yes"} {
		t.Setenv("RUN_INFLUX_INTEGRATION", value)
		skipped := false
		t.Run("RUN_INFLUX_INTEGRATION="+value, func(t *testing.T) {
			defer func() { skipped = t.Skipped() }()
			newIntegrationBucket(t)
		})
		if !skipped {
			t.Errorf("newIntegrationBucket did not skip with RUN_INFLUX_INTEGRATION=%q", value)
		}
	}
}
