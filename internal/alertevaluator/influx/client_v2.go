package influx

import (
	"context"
	"fmt"

	api "github.com/influxdata/influxdb-client-go/v2/api"
)

type ClientV2Querier struct {
	API api.QueryAPI
}

func (querier ClientV2Querier) Query(ctx context.Context, flux string) ([]V2Row, error) {
	result, err := querier.API.Query(ctx, flux)
	if err != nil {
		return nil, err
	}
	var rows []V2Row
	for result.Next() {
		record := result.Record()
		value, ok := numeric(record.ValueByKey("value"))
		if !ok {
			continue
		}
		sequence, _ := integer(record.ValueByKey("sequence"))
		rows = append(rows, V2Row{
			Time: record.Time(), DeviceID: fmt.Sprint(record.ValueByKey("device_id")),
			ComponentID: fmt.Sprint(record.ValueByKey("component_id")),
			Value:       value, Sequence: sequence,
		})
	}
	if err := result.Err(); err != nil {
		return nil, err
	}
	return rows, nil
}
