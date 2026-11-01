package alertevaluator

import (
	"encoding/json"
	"math"
	"testing"
	"time"
)

func TestMeasurementZeroValueIsAbsent(t *testing.T) {
	var measurement Measurement
	if value, ok := measurement.Get(); ok || value != 0 || measurement.IsPresent() {
		t.Fatalf("zero Measurement = (%v, %v), want absent", value, ok)
	}
}

func TestMeasuredKeepsZeroNegativeAndPositiveValuesPresent(t *testing.T) {
	for _, want := range []float64{0, -3.5, 4.2, 1e21, 1e-7} {
		value, ok := Measured(want).Get()
		if !ok || value != want || !Measured(want).IsPresent() {
			t.Fatalf("Measured(%v) = (%v, %v), want present %v", want, value, ok, want)
		}
	}
}

func TestMeasuredRejectsNonFiniteValues(t *testing.T) {
	for _, invalid := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if Measured(invalid).IsPresent() {
			t.Fatalf("Measured(%v) is present, want absent", invalid)
		}
	}
}

func TestMeasuredKeepsOnlyThePersistableRange(t *testing.T) {
	// DynamoDB numbers stop near 1e126 and 1e-130, and the Python adapters'
	// Decimal normalization raises from 1e28; the range keeps a margin to both.
	for _, inside := range []float64{1e25, -1e25, 1e-100, -1e-100, 123456.789} {
		if value, ok := Measured(inside).Get(); !ok || value != inside {
			t.Fatalf("Measured(%v) = (%v, %v), want present", inside, value, ok)
		}
	}
	for _, outside := range []float64{1e26, -1e26, 1e200, math.MaxFloat64, 1e-101, -1e-101, 5e-324} {
		if Measured(outside).IsPresent() {
			t.Fatalf("Measured(%v) is present, want absent outside the persistable range", outside)
		}
	}
}

func TestMeasuredNormalizesNegativeZero(t *testing.T) {
	value, ok := Measured(math.Copysign(0, -1)).Get()
	if !ok || value != 0 || math.Signbit(value) {
		t.Fatalf("Measured(-0) = (%v, %v, signbit %v), want present +0", value, ok, math.Signbit(value))
	}
}

func TestMeasurementJSONEncodesNumberOrNullThroughValueReceiver(t *testing.T) {
	// The struct is passed by value, as json.Marshal(decision.Next) does, so a
	// pointer-receiver MarshalJSON would be skipped and encode as {}.
	tests := []struct {
		measurement Measurement
		want        string
	}{
		{Measured(0), `{"V":0}`},
		{Measured(-3.5), `{"V":-3.5}`},
		{Measured(4.2), `{"V":4.2}`},
		{Measured(1e-7), `{"V":1e-7}`},
		{Measurement{}, `{"V":null}`},
	}
	for _, test := range tests {
		encoded, err := json.Marshal(struct{ V Measurement }{test.measurement})
		if err != nil {
			t.Fatal(err)
		}
		if string(encoded) != test.want {
			t.Fatalf("encoded %v as %s, want %s", test.measurement, encoded, test.want)
		}
	}
}

func TestMeasurementJSONDecodesNumberNullAndMissing(t *testing.T) {
	tests := []struct {
		document    string
		wantPresent bool
		wantValue   float64
	}{
		{`{"V":0}`, true, 0},
		{`{"V":-3.5}`, true, -3.5},
		{`{"V":4.2}`, true, 4.2},
		{`{"V":1e-07}`, true, 1e-7},
		{`{"V":-0}`, true, 0},
		{`{"V":null}`, false, 0},
		{`{"V":1e+200}`, false, 0},
	}
	for _, test := range tests {
		// Seed a present value so a null that leaves the field untouched fails.
		decoded := struct{ V Measurement }{V: Measured(99)}
		if err := json.Unmarshal([]byte(test.document), &decoded); err != nil {
			t.Fatalf("decode %s: %v", test.document, err)
		}
		value, ok := decoded.V.Get()
		if ok != test.wantPresent || (ok && (value != test.wantValue || (value == 0 && math.Signbit(value)))) {
			t.Fatalf("decode %s = (%v, %v), want present=%v value=%v", test.document, value, ok, test.wantPresent, test.wantValue)
		}
	}
	var missing struct{ V Measurement }
	if err := json.Unmarshal([]byte(`{}`), &missing); err != nil || missing.V.IsPresent() {
		t.Fatalf("missing key decoded as %v (err %v), want absent", missing.V, err)
	}
}

func TestMeasurementJSONRejectsNonNumbers(t *testing.T) {
	for _, document := range []string{`{"V":"4.2"}`, `{"V":{}}`, `{"V":true}`, `{"V":[0]}`} {
		var decoded struct{ V Measurement }
		if err := json.Unmarshal([]byte(document), &decoded); err == nil {
			t.Fatalf("decode %s succeeded with %v, want error", document, decoded.V)
		}
	}
}

func TestEvaluationIsDeterminateOnlyWhenSufficientWithValue(t *testing.T) {
	tests := []struct {
		quality Quality
		value   Measurement
		want    bool
	}{
		{QualitySufficient, Measured(0), true},
		{QualitySufficient, Measured(-3.5), true},
		{QualitySufficient, Measurement{}, false},
		{QualityInsufficientData, Measured(0), false},
		{QualityStaleData, Measured(9.9), false},
		{QualityQueryError, Measurement{}, false},
		{"", Measured(1), false},
	}
	for _, test := range tests {
		evaluation := Evaluation{Quality: test.quality, Value: test.value}
		if got := evaluation.IsDeterminate(); got != test.want {
			t.Fatalf("IsDeterminate(%s, %v) = %v, want %v", test.quality, test.value, got, test.want)
		}
	}
}

func TestEvaluationNormalizedEnforcesTheDeterminacyInvariant(t *testing.T) {
	slot := time.Date(2026, 7, 15, 12, 0, 45, 0, time.UTC)
	tests := []struct {
		name string
		in   Evaluation
		want Evaluation
	}{
		{"determinate is unchanged",
			Evaluation{Slot: slot, Quality: QualitySufficient, Value: Measured(0), Breached: true, MissedSlots: 2},
			Evaluation{Slot: slot, Quality: QualitySufficient, Value: Measured(0), Breached: true, MissedSlots: 2}},
		{"sufficient without value becomes insufficient",
			Evaluation{Slot: slot, Quality: QualitySufficient, Breached: true, MissedSlots: 1},
			Evaluation{Slot: slot, Quality: QualityInsufficientData, MissedSlots: 1}},
		{"stale drops its value and breach",
			Evaluation{Slot: slot, Quality: QualityStaleData, Value: Measured(9.9), Breached: true, MissedSlots: 3},
			Evaluation{Slot: slot, Quality: QualityStaleData, MissedSlots: 3}},
		{"query error drops a placeholder zero",
			Evaluation{Slot: slot, Quality: QualityQueryError, Value: Measured(0), Breached: true},
			Evaluation{Slot: slot, Quality: QualityQueryError}},
		{"empty quality drops its value",
			Evaluation{Slot: slot, Value: Measured(1)},
			Evaluation{Slot: slot}},
	}
	for _, test := range tests {
		got := test.in.Normalized()
		if got != test.want {
			t.Fatalf("%s: Normalized() = %#v, want %#v", test.name, got, test.want)
		}
		if got.IsDeterminate() != got.Value.IsPresent() || (got.Quality == QualitySufficient) != got.IsDeterminate() {
			t.Fatalf("%s: normalized evaluation breaks the invariant: %#v", test.name, got)
		}
	}
}
