package influx

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	"github.com/influxdata/influxdb-client-go/v2/api"
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

// serverDecodes asks the server whether literal holds exactly want. It compares on
// the server, because the annotated CSV the client reads turns "\n" into "\r\n"
// and drops "\r".
func serverDecodes(queryAPI api.QueryAPI, literal, want string) error {
	flux := "import \"array\"\nimport \"strings\"\n" +
		"array.from(rows: [{v: " + literal + "}])\n" +
		"  |> map(fn: (r) => ({equal: r.v == " + fluxHexReference(want) + ", length: strings.strlen(v: r.v)}))"
	result, err := queryAPI.Query(context.Background(), flux)
	if err != nil {
		return fmt.Errorf("query with literal %+q: %w", literal, err)
	}
	var rows []string
	for result.Next() {
		record := result.Record()
		rows = append(rows, fmt.Sprintf("%v/%v", record.ValueByKey("equal"), record.ValueByKey("length")))
	}
	if err := result.Err(); err != nil {
		return fmt.Errorf("read literal %+q: %w", literal, err)
	}
	if expected := fmt.Sprintf("true/%d", utf8.RuneCountInString(want)); len(rows) != 1 || rows[0] != expected {
		return fmt.Errorf("literal %+q gives rows %v, want [%s]", literal, rows, expected)
	}
	return nil
}

func TestInfluxIntegrationV2LiteralsRoundTrip(t *testing.T) {
	client, bucket := newIntegrationBucket(t)
	queryAPI := client.QueryAPI(*bucket.OrgID)
	for _, value := range loadFluxLiteralFixture(t).RoundTrip {
		t.Run(fmt.Sprintf("%+q", value), func(t *testing.T) {
			literal, err := v2Literal(value)
			if err != nil {
				t.Fatal(err)
			}
			if err := serverDecodes(queryAPI, literal, value); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// TestInfluxIntegrationFluxLiteralDecoderAgreesWithTheServer ties the test
// decoder to InfluxDB 2.7.11: what it decodes, the server decodes the same way,
// and what the server refuses, it refuses too.
func TestInfluxIntegrationFluxLiteralDecoderAgreesWithTheServer(t *testing.T) {
	client, bucket := newIntegrationBucket(t)
	queryAPI := client.QueryAPI(*bucket.OrgID)
	for _, literal := range []string{
		`"plain"`, `""`, `"\x24{a}"`, `"\x24\x24{a}"`, `"a\"b\\c"`, `"\n\r\t"`, `"é<&>"`, `"\\u2028"`,
		"\"\u2028\u2029\"", `"\\\x24{z}"`, "\"a\x7fb\u0085\"",
	} {
		decoded, err := decodeFluxLiteral(literal)
		if err != nil {
			t.Fatalf("decoder rejects %+q: %v", literal, err)
		}
		if err := serverDecodes(queryAPI, literal, decoded); err != nil {
			t.Fatal(err)
		}
	}
	for _, literal := range []string{`"\b"`, `"\f"`, `"\u2028"`, `"\/"`, `"$\${a}"`, `"$\""`, `"\x2"`, `"\xZZ"`} {
		if decoded, err := decodeFluxLiteral(literal); err == nil {
			t.Errorf("decoder accepts %+q as %+q, but the server refuses it", literal, decoded)
		}
		if result, err := queryAPI.Query(context.Background(), "import \"array\"\narray.from(rows: [{v: "+literal+"}])"); err == nil {
			for result.Next() {
			}
			if result.Err() == nil {
				t.Errorf("server accepts %+q; drop it from the refused list", literal)
			}
		}
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

	// Line protocol keeps these tag values as written: no space, comma, equals sign,
	// CR/LF or trailing backslash.
	tenant, device, pond := "t$${a}é<&>\u2028", `d${a}"b\c$`, "pond_1"
	asset := AssetIDForLegacyPond(tenant, pond)
	// Each decoy differs from the rule in one tag only and would pass every other
	// check (its component is the default one the reader keeps), so only the
	// tenant or device literal keeps it out. Its value 100 would win the Max.
	decoyTenant, decoyDevice := `t$\${a}é<&>`+"\u2028", `d\${a}"b\c$`
	line := func(tenantID, deviceID, componentID string, at time.Time, value float64) string {
		return fmt.Sprintf(
			"observation_numeric,asset_id=%s,component_id=%s,device_id=%s,metric_key=water.ph,tenant_id=%s "+
				"quality_state=\"valid\",sequence=1i,value=%v %d",
			asset, componentID, deviceID, tenantID, value, at.UnixNano(),
		)
	}
	end := time.Date(2026, 7, 9, 12, 1, 0, 0, time.UTC)
	spec := readerSpec(end, alertevaluator.AggregationMax)
	var lines []string
	for offset := time.Duration(0); offset < time.Minute; offset += 10 * time.Second {
		at := spec.Start.Add(offset)
		lines = append(lines,
			line(tenant, device, DefaultComponentIDForLegacyDevice(tenant, device), at, 7),
			line(decoyTenant, device, DefaultComponentIDForLegacyDevice(tenant, device), at.Add(time.Second), 100),
			line(tenant, decoyDevice, DefaultComponentIDForLegacyDevice(tenant, decoyDevice), at.Add(2*time.Second), 100),
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

	// The pond-wide rule sees both devices of the tenant, so the device decoys are real rows.
	pondWide := rule
	pondWide.DeviceID = ""
	result, err = V2WindowReader{Bucket: bucket.Name, Querier: ClientV2Querier{API: queryAPI}}.Evaluate(ctx, pondWide, spec)
	if err != nil || result.Value != 100 || result.ContributingDeviceCount != 2 {
		t.Fatalf("pond-wide v2 result = (%#v, %v), want both devices of the tenant", result, err)
	}

	// The documented D19 divergence: on a plain bucket, the unchanged legacy literals
	// still cannot express these IDs (json.Marshal escapes `<&>` as \u003c, and `${a}` interpolates).
	legacy := WindowReader{Bucket: scratch.Name, Querier: ClientQuerier{API: client.QueryAPI(*scratch.OrgID)}}
	if _, err := legacy.Evaluate(ctx, rule, spec); err == nil {
		t.Fatal("legacy reader read the free-text IDs; the D19 divergence note is stale")
	}
}
