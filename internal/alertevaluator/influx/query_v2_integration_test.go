package influx

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
)

// fluxHexReference writes value as `\x` escapes of its UTF-8 bytes: an encoding
// independent of the D19 rule, for the server to compare against.
func fluxHexReference(value string) string {
	var reference strings.Builder
	reference.WriteByte('"')
	for _, b := range []byte(value) {
		fmt.Fprintf(&reference, `\x%02x`, b)
	}
	reference.WriteByte('"')
	return reference.String()
}

// TestInfluxIntegrationV2LiteralsRoundTrip compares each literal on the server, because
// the annotated CSV the client reads turns "\n" into "\r\n" and drops "\r".
func TestInfluxIntegrationV2LiteralsRoundTrip(t *testing.T) {
	client, bucket := newIntegrationBucket(t)
	queryAPI := client.QueryAPI(*bucket.OrgID)
	for _, value := range fluxRoundTripValues {
		t.Run(fmt.Sprintf("%+q", value), func(t *testing.T) {
			literal, err := v2TenantLiteral(value)
			if err != nil {
				t.Fatal(err)
			}
			flux := "import \"array\"\nimport \"strings\"\n" +
				"array.from(rows: [{v: " + literal + "}])\n" +
				"  |> map(fn: (r) => ({equal: r.v == " + fluxHexReference(value) + ", length: strings.strlen(v: r.v)}))"
			result, err := queryAPI.Query(context.Background(), flux)
			if err != nil {
				t.Fatalf("query with literal %+q: %v", literal, err)
			}
			var rows []string
			for result.Next() {
				record := result.Record()
				rows = append(rows, fmt.Sprintf("%v/%v", record.ValueByKey("equal"), record.ValueByKey("length")))
			}
			if err := result.Err(); err != nil {
				t.Fatalf("read literal %+q: %v", literal, err)
			}
			if want := fmt.Sprintf("true/%d", utf8.RuneCountInString(value)); len(rows) != 1 || rows[0] != want {
				t.Fatalf("literal %+q gives rows %v, want [%s]", literal, rows, want)
			}
		})
	}
}

// TestInfluxIntegrationV2ReaderReadsFreeTextIDsAndBucket drives the evaluator call site:
// /v1 rule IDs are escaped, not validated (#71), so free text reaches these literals.
func TestInfluxIntegrationV2ReaderReadsFreeTextIDsAndBucket(t *testing.T) {
	client, scratch := newIntegrationBucket(t)
	ctx := context.Background()
	org, err := client.OrganizationsAPI().FindOrganizationByID(ctx, *scratch.OrgID)
	if err != nil {
		t.Fatalf("find org: %v", err)
	}
	bucket, err := client.BucketsAPI().CreateBucketWithName(ctx, org, scratch.Name+"_$${a}é<&>\u2028\\${z}")
	if err != nil {
		t.Fatalf("create bucket: %v", err)
	}
	// Registered after newIntegrationBucket's client.Close, so it runs first.
	t.Cleanup(func() {
		if err := client.BucketsAPI().DeleteBucket(context.Background(), bucket); err != nil {
			t.Errorf("delete bucket %q: %v", bucket.Name, err)
		}
	})

	// Line protocol keeps these tag values as written: no space, comma, equals sign, CR/LF
	// or trailing backslash. Each decoy differs by one character the old rule mishandled.
	tenant, decoyTenant := "t$${a}é<&>\u2028", `t$\${a}é<&>`+"\u2028"
	device, decoyDevice := `d${a}"b\c$`, `d\${a}"b\c$`
	pond := "pond_1"
	line := func(tenantID, deviceID string, at time.Time, value float64) string {
		return fmt.Sprintf(
			"observation_numeric,asset_id=%s,component_id=%s,device_id=%s,metric_key=water.ph,tenant_id=%s "+
				"quality_state=\"valid\",sequence=1i,value=%v %d",
			AssetIDForLegacyPond(tenantID, pond), DefaultComponentIDForLegacyDevice(tenantID, deviceID),
			deviceID, tenantID, value, at.UnixNano(),
		)
	}
	end := time.Date(2026, 7, 9, 12, 1, 0, 0, time.UTC)
	spec := readerSpec(end, alertevaluator.AggregationMax)
	var lines []string
	for offset := time.Duration(0); offset < time.Minute; offset += 10 * time.Second {
		at := spec.Start.Add(offset)
		lines = append(lines,
			line(tenant, device, at, 7),
			line(decoyTenant, device, at.Add(time.Second), 100),
			line(tenant, decoyDevice, at.Add(2*time.Second), 100),
		)
	}
	writeLines(t, client, bucket, lines...)

	queryAPI := client.QueryAPI(*bucket.OrgID)
	rule := alertevaluator.EvaluationRule{
		Rule: alertevaluator.Rule{TenantID: tenant}, PondID: pond, DeviceID: device, Metric: "ph",
	}
	result, err := V2WindowReader{Bucket: bucket.Name, Querier: ClientV2Querier{API: queryAPI}}.Evaluate(ctx, rule, spec)
	if err != nil {
		t.Fatalf("v2 reader: %v", err)
	}
	if result.Quality != alertevaluator.QualitySufficient || result.Value != 7 || result.ValidPointCount != 6 ||
		result.ContributingDeviceCount != 1 {
		t.Fatalf("v2 result = %#v, want the six points of the exact tenant and device", result)
	}

	// The documented D19 divergence: the unchanged legacy literals cannot express `<&>`.
	_, legacyErr := WindowReader{Bucket: bucket.Name, Querier: ClientQuerier{API: queryAPI}}.Evaluate(ctx, rule, spec)
	if legacyErr == nil {
		t.Fatal("legacy reader read the free-text IDs; the D19 divergence note is stale")
	}
}
