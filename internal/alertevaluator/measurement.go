package alertevaluator

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
)

// Persistable magnitudes for a present measurement. DynamoDB numbers stop near
// 1e126 and 1e-130, and the Python adapters' Decimal normalization raises from
// 1e28; the bounds keep a margin to both. No water-quality measurement comes
// near them, so a value outside is an invalid aggregation.
const (
	minMeasurementMagnitude = 1e-100
	maxMeasurementMagnitude = 1e25
)

// Measurement is an evaluation value that may be absent. The zero value is
// absent, so an unset field can never read as a measured zero. A present value
// is always finite and persistable.
type Measurement struct {
	value   float64
	present bool
}

// Measured returns a present measurement, or an absent one for NaN, ±Inf and
// non-zero magnitudes outside [minMeasurementMagnitude, maxMeasurementMagnitude].
// Negative zero becomes positive zero so the value is stable across JSON
// encoders that read "-0" as an integer.
func Measured(value float64) Measurement {
	if value == 0 {
		return Measurement{value: 0, present: true}
	}
	magnitude := math.Abs(value)
	if math.IsNaN(value) || magnitude < minMeasurementMagnitude || magnitude > maxMeasurementMagnitude {
		return Measurement{}
	}
	return Measurement{value: value, present: true}
}

// Get returns the value and whether it is present.
func (measurement Measurement) Get() (float64, bool) {
	return measurement.value, measurement.present
}

func (measurement Measurement) IsPresent() bool {
	return measurement.present
}

func (measurement Measurement) String() string {
	if !measurement.present {
		return "absent"
	}
	return strconv.FormatFloat(measurement.value, 'g', -1, 64)
}

// MarshalJSON writes a number, or null when absent. It has a value receiver so
// it also applies when the enclosing struct is marshalled by value.
func (measurement Measurement) MarshalJSON() ([]byte, error) {
	if !measurement.present {
		return []byte("null"), nil
	}
	return json.Marshal(measurement.value)
}

// UnmarshalJSON reads a number as present and null as absent.
func (measurement *Measurement) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*measurement = Measurement{}
		return nil
	}
	var value float64
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("measurement must be a number or null: %w", err)
	}
	*measurement = Measured(value)
	return nil
}
