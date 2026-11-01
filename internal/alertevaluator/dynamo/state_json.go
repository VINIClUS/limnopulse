package dynamo

import (
	"encoding/json"
	"fmt"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
)

// stateSchemaVersion marks state_json documents whose LastValue is
// presence-aware: a number is present, null or a missing key is absent.
// Documents without the marker were written by evaluators that stored
// LastValue as a float64; see legacyLastValue.
const stateSchemaVersion = 2

// stateDocument is the state_json shape. Embedding keeps the field names the
// Python adapters already read and rewrite.
type stateDocument struct {
	SchemaVersion int
	alertevaluator.State
}

func encodeStateJSON(state alertevaluator.State) (string, error) {
	encoded, err := json.Marshal(stateDocument{SchemaVersion: stateSchemaVersion, State: state})
	if err != nil {
		return "", err
	}
	return string(encoded), nil
}

func decodeStateJSON(document string) (alertevaluator.State, error) {
	var decoded stateDocument
	if err := json.Unmarshal([]byte(document), &decoded); err != nil {
		return alertevaluator.State{}, err
	}
	switch decoded.SchemaVersion {
	case stateSchemaVersion:
	case 0, 1:
		decoded.State.LastValue = legacyLastValue(decoded.State)
	default:
		return alertevaluator.State{}, fmt.Errorf("%w: version %d", alertevaluator.ErrUnsupportedStateSchema, decoded.SchemaVersion)
	}
	return decoded.State, nil
}

// legacyLastValue reads a LastValue stored as a float64. Legacy evaluators
// assign it only on sufficient evaluations, but its zero value is 0, so a 0
// is a measurement only when the last evaluation was sufficient. A non-zero
// value is a retained measurement.
func legacyLastValue(state alertevaluator.State) alertevaluator.Measurement {
	value, ok := state.LastValue.Get()
	if !ok || (value == 0 && state.LastQuality != alertevaluator.QualitySufficient) {
		return alertevaluator.Measurement{}
	}
	return state.LastValue
}
