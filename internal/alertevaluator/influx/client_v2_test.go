package influx

import (
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	api "github.com/influxdata/influxdb-client-go/v2/api"
)

type fakeQueryAPI struct {
	api.QueryAPI
	csv   string
	err   error
	query string
}

func (fake *fakeQueryAPI) Query(_ context.Context, flux string) (*api.QueryTableResult, error) {
	fake.query = flux
	if fake.err != nil {
		return nil, fake.err
	}
	return api.NewQueryTableResult(io.NopCloser(strings.NewReader(fake.csv))), nil
}

var _ V2Querier = ClientV2Querier{}

// pivotedCSV is the annotated CSV shape of the v2 window query after
// pivot/keep/group: one table of float values and integer sequences, then a
// table whose value is an integer and whose sequence is a float.
const pivotedCSV = `#datatype,string,long,dateTime:RFC3339Nano,string,string,double,long
#group,false,false,false,false,false,false,false
#default,_result,,,,,,
,result,table,_time,device_id,component_id,value,sequence
,,0,2026-07-09T12:00:00Z,local-device-001,cmp_32c73f5ac8dc596e9446956875639204,25.1,1
,,0,2026-07-09T12:00:00.75Z,local-device-001,cmp_32c73f5ac8dc596e9446956875639204,6.4,-1
,,0,2026-07-09T12:00:01Z,dev_22222222222222222222222222222222,cmp_a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1,6.6,9223372036854775807

#datatype,string,long,dateTime:RFC3339Nano,string,string,long,double
#group,false,false,false,false,false,false,false
#default,_result,,,,,,
,result,table,_time,device_id,component_id,value,sequence
,,1,2026-07-09T12:00:02Z,series-device-a,cmp_bef1b51d956b56c08b45b73f554869f7,-67,3
`

func TestClientV2QuerierParsesPivotedRows(t *testing.T) {
	fake := &fakeQueryAPI{csv: pivotedCSV}
	rows, err := ClientV2Querier{API: fake}.Query(context.Background(), "the flux")
	if err != nil {
		t.Fatal(err)
	}
	if fake.query != "the flux" {
		t.Fatalf("query sent = %q", fake.query)
	}
	at := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	want := []V2Row{
		{Time: at, DeviceID: "local-device-001", ComponentID: "cmp_32c73f5ac8dc596e9446956875639204", Value: 25.1, Sequence: 1},
		{Time: at.Add(750 * time.Millisecond), DeviceID: "local-device-001", ComponentID: "cmp_32c73f5ac8dc596e9446956875639204", Value: 6.4},
		{Time: at.Add(time.Second), DeviceID: "dev_22222222222222222222222222222222", ComponentID: "cmp_a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1a1", Value: 6.6, Sequence: 9223372036854775807},
		{Time: at.Add(2 * time.Second), DeviceID: "series-device-a", ComponentID: "cmp_bef1b51d956b56c08b45b73f554869f7", Value: -67, Sequence: 3},
	}
	if len(rows) != len(want) {
		t.Fatalf("rows = %#v", rows)
	}
	for index := range want {
		got := rows[index]
		if !got.Time.Equal(want[index].Time) || got.DeviceID != want[index].DeviceID ||
			got.ComponentID != want[index].ComponentID || got.Value != want[index].Value {
			t.Errorf("row %d = %#v, want %#v", index, got, want[index])
		}
		// The reader maps the -1 sentinel to Seq 0 (see
		// TestV2WindowReaderMapsAbsentSequenceSentinelToZero); the client may
		// pass it through or map it early. Other sequences pass exactly.
		if index == 1 {
			if got.Sequence != -1 && got.Sequence != 0 {
				t.Errorf("row %d sequence = %d, want the -1 sentinel or 0", index, got.Sequence)
			}
		} else if got.Sequence != want[index].Sequence {
			t.Errorf("row %d sequence = %d, want %d", index, got.Sequence, want[index].Sequence)
		}
	}
}

func TestClientV2QuerierReturnsEmptyResult(t *testing.T) {
	rows, err := ClientV2Querier{API: &fakeQueryAPI{csv: ""}}.Query(context.Background(), "q")
	if err != nil || len(rows) != 0 {
		t.Fatalf("rows, err = %#v, %v", rows, err)
	}
}

func TestClientV2QuerierPropagatesQueryAndStreamErrors(t *testing.T) {
	sentinel := errors.New("bucket not found")
	if rows, err := (ClientV2Querier{API: &fakeQueryAPI{err: sentinel}}).Query(context.Background(), "q"); !errors.Is(err, sentinel) || rows != nil {
		t.Fatalf("rows, err = %#v, %v", rows, err)
	}

	streamFailure := pivotedCSV + `
#datatype,string,string
#group,true,true
#default,,
,error,reference
,"panic: runtime error",897
`
	rows, err := ClientV2Querier{API: &fakeQueryAPI{csv: streamFailure}}.Query(context.Background(), "q")
	if err == nil || !strings.Contains(err.Error(), "panic: runtime error") {
		t.Fatalf("err = %v, want the in-stream query error", err)
	}
	if rows != nil {
		t.Fatalf("rows = %#v, want nil on a stream error", rows)
	}
}

func TestV2WindowReaderOverClientMapsSentinelLikeLegacyMissingSeq(t *testing.T) {
	// Same instant and device: a legacy row without seq (Seq 0) ties with a
	// row whose seq is 0, so the first row is kept. The v2 -1 sentinel must
	// behave the same way end to end through ClientV2Querier.
	const csv = `#datatype,string,long,dateTime:RFC3339Nano,string,string,double,long
#group,false,false,false,false,false,false,false
#default,_result,,,,,,
,result,table,_time,device_id,component_id,value,sequence
,,0,2026-07-09T12:00:02Z,local-device-001,cmp_32c73f5ac8dc596e9446956875639204,1,-1
,,0,2026-07-09T12:00:02Z,local-device-001,cmp_32c73f5ac8dc596e9446956875639204,2,0
`
	start := time.Date(2026, 7, 9, 12, 0, 0, 0, time.UTC)
	reader := V2WindowReader{Bucket: "raw", Querier: ClientV2Querier{API: &fakeQueryAPI{csv: csv}}}
	result, err := reader.Evaluate(context.Background(), vectorRule("", "ph"), singleBucketSpec(start))
	if err != nil {
		t.Fatal(err)
	}
	if result.Value != 1 || result.ValidPointCount != 1 {
		t.Fatalf("result = %#v, want the first row kept (value 1)", result)
	}
}

func TestV2RowShape(t *testing.T) {
	// V2Row is a locked contract: Time, DeviceID, ComponentID, Value, Sequence.
	fields := reflect.VisibleFields(reflect.TypeOf(V2Row{}))
	var got []string
	for _, field := range fields {
		got = append(got, field.Name+" "+field.Type.String())
	}
	want := []string{"Time time.Time", "DeviceID string", "ComponentID string", "Value float64", "Sequence int64"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("V2Row fields = %v, want %v", got, want)
	}
}
