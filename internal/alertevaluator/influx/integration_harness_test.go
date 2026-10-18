package influx

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/domain"
)

const (
	harnessSmokeMeasurement = "harness_smoke"
	harnessSmokeLine        = harnessSmokeMeasurement + ",probe=p1 value=21.5 1767268800000000000"
	harnessSmokeSecondLine  = harnessSmokeMeasurement + ",probe=p2 value=7 1767268800000000000"
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
		assertNoRetentionRule(t, found)

		writeLines(t, client, bucket, harnessSmokeLine, harnessSmokeSecondLine)

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
		if len(rows) != 2 || rows[0].measurement != want.measurement || rows[0].field != want.field ||
			rows[0].probe != want.probe || rows[0].value != want.value || !rows[0].time.Equal(want.time) {
			t.Fatalf("rows = %+v, want [%+v, p2 = 7]", rows, want)
		}
		if rows[1].probe != "p2" || rows[1].value != 7.0 || !rows[1].time.Equal(want.time) {
			t.Fatalf("second row = %+v, want probe p2 = 7 at %s", rows[1], want.time)
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

func TestInfluxIntegrationHarnessDoesNotSkipWhenOptedIn(t *testing.T) {
	if os.Getenv("RUN_INFLUX_INTEGRATION") != "1" {
		t.Skip("set RUN_INFLUX_INTEGRATION=1 to run against a real InfluxDB")
	}
	skipped := true
	t.Run("opted in", func(t *testing.T) {
		defer func() { skipped = t.Skipped() }()
		newIntegrationBucket(t)
	})
	if skipped {
		t.Fatal("newIntegrationBucket skipped although RUN_INFLUX_INTEGRATION=1")
	}
}

func TestIntegrationSettingsUseContractDefaultsAndOverrides(t *testing.T) {
	for _, name := range []string{"INFLUX_INTEGRATION_URL", "INFLUX_INTEGRATION_TOKEN", "INFLUX_INTEGRATION_ORG"} {
		t.Setenv(name, "")
	}
	url, token, org := integrationSettings()
	if url != "http://127.0.0.1:8086" || token != "local-dev-token" || org != "limnopulse" {
		t.Fatalf("defaults = (%q, %q, %q)", url, token, org)
	}

	t.Setenv("INFLUX_INTEGRATION_URL", "http://127.0.0.1:18086")
	t.Setenv("INFLUX_INTEGRATION_TOKEN", "override-token")
	t.Setenv("INFLUX_INTEGRATION_ORG", "override-org")
	url, token, org = integrationSettings()
	if url != "http://127.0.0.1:18086" || token != "override-token" || org != "override-org" {
		t.Fatalf("overrides = (%q, %q, %q)", url, token, org)
	}
}

func integrationEnv(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

// integrationSettings resolves the harness connection settings; an empty
// variable counts as unset.
func integrationSettings() (url, token, org string) {
	return integrationEnv("INFLUX_INTEGRATION_URL", "http://127.0.0.1:8086"),
		integrationEnv("INFLUX_INTEGRATION_TOKEN", "local-dev-token"),
		integrationEnv("INFLUX_INTEGRATION_ORG", "limnopulse")
}

// newIntegrationBucket skips unless RUN_INFLUX_INTEGRATION=1. Otherwise it
// creates a uniquely named bucket with no retention rule through BucketsAPI()
// and deletes it when the test ends.
func newIntegrationBucket(t *testing.T) (influxdb2.Client, *domain.Bucket) {
	t.Helper()
	if os.Getenv("RUN_INFLUX_INTEGRATION") != "1" {
		t.Skip("set RUN_INFLUX_INTEGRATION=1 to run against a real InfluxDB")
	}
	url, token, orgName := integrationSettings()
	client := influxdb2.NewClient(url, token)
	t.Cleanup(client.Close)
	ctx := context.Background()
	org, err := client.OrganizationsAPI().FindOrganizationByName(ctx, orgName)
	if err != nil {
		t.Fatalf("find org %q at %s: %v", orgName, url, err)
	}
	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		t.Fatalf("bucket name suffix: %v", err)
	}
	bucket, err := client.BucketsAPI().CreateBucketWithName(ctx, org, "it_"+hex.EncodeToString(suffix))
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	// Registered after client.Close, so it runs first.
	t.Cleanup(func() {
		if err := client.BucketsAPI().DeleteBucket(context.Background(), bucket); err != nil {
			t.Errorf("delete bucket %q: %v", bucket.Name, err)
		}
	})
	return client, bucket
}

// writeLines writes line-protocol records synchronously.
func writeLines(t *testing.T, client influxdb2.Client, bucket *domain.Bucket, lines ...string) {
	t.Helper()
	if err := client.WriteAPIBlocking(*bucket.OrgID, bucket.Name).WriteRecord(context.Background(), lines...); err != nil {
		t.Fatalf("write to bucket %q: %v", bucket.Name, err)
	}
}
