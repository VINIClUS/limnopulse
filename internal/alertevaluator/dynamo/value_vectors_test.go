package dynamo

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type vectorValue struct {
	Present bool    `json:"present"`
	Value   float64 `json:"value"`
}

type valueVectors struct {
	GoEncodeBaseState json.RawMessage `json:"go_encode_base_state"`
	StateVectors      []struct {
		Name                      string       `json:"name"`
		Writer                    string       `json:"writer"`
		StoredStateJSON           string       `json:"stored_state_json"`
		ExpectedLastValue         vectorValue  `json:"expected_last_value"`
		PythonResolvedStateJSON   string       `json:"python_resolved_state_json"`
		ExpectedAfterOldGoRewrite *vectorValue `json:"expected_after_old_go_rewrite"`
		GoEncode                  *struct {
			LastQuality string      `json:"last_quality"`
			LastValue   vectorValue `json:"last_value"`
		} `json:"go_encode"`
	} `json:"state_vectors"`
	StateErrorVectors []struct {
		Name            string `json:"name"`
		StoredStateJSON string `json:"stored_state_json"`
	} `json:"state_error_vectors"`
	DynamoDBValueVectors []struct {
		Name      string            `json:"name"`
		Value     vectorValue       `json:"value"`
		Attribute map[string]string `json:"attribute"`
	} `json:"dynamodb_value_vectors"`
}

func loadValueVectors(t *testing.T) valueVectors {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "alert_evaluation_value_vectors.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vectors valueVectors
	if err := json.Unmarshal(data, &vectors); err != nil {
		t.Fatal(err)
	}
	if len(vectors.StateVectors) == 0 || len(vectors.StateErrorVectors) == 0 || len(vectors.DynamoDBValueVectors) == 0 {
		t.Fatal("value vectors are empty")
	}
	return vectors
}

func (value vectorValue) measurement() alertevaluator.Measurement {
	if !value.Present {
		return alertevaluator.Measurement{}
	}
	return alertevaluator.Measured(value.Value)
}

func assertMeasurement(t *testing.T, label string, got alertevaluator.Measurement, want vectorValue) {
	t.Helper()
	value, ok := got.Get()
	if ok != want.Present || (ok && value != want.Value) {
		t.Fatalf("%s: LastValue = %v, want present=%v value=%v", label, got, want.Present, want.Value)
	}
}

func TestStateVectorsDecodeToTheirExpectedLastValue(t *testing.T) {
	for _, vector := range loadValueVectors(t).StateVectors {
		state, err := decodeStateJSON(vector.StoredStateJSON)
		if err != nil {
			t.Fatalf("%s: decode stored document: %v", vector.Name, err)
		}
		assertMeasurement(t, vector.Name+" stored", state.LastValue, vector.ExpectedLastValue)
	}
}

func TestStateVectorsDecodeThePythonRewrittenDocument(t *testing.T) {
	for _, vector := range loadValueVectors(t).StateVectors {
		state, err := decodeStateJSON(vector.PythonResolvedStateJSON)
		if err != nil {
			t.Fatalf("%s: decode Python rewrite: %v", vector.Name, err)
		}
		assertMeasurement(t, vector.Name+" python rewrite", state.LastValue, vector.ExpectedLastValue)
		if state.Mode != alertevaluator.ModeHealthy || state.ActiveEventID != "" {
			t.Fatalf("%s: Python rewrite lifecycle = %#v", vector.Name, state)
		}
	}
}

func TestStateVectorsPinTheGoWriterBytes(t *testing.T) {
	vectors := loadValueVectors(t)
	encoded := 0
	for _, vector := range vectors.StateVectors {
		if vector.GoEncode == nil {
			continue
		}
		var state alertevaluator.State
		if err := json.Unmarshal(vectors.GoEncodeBaseState, &state); err != nil {
			t.Fatal(err)
		}
		state.LastQuality = alertevaluator.Quality(vector.GoEncode.LastQuality)
		state.LastValue = vector.GoEncode.LastValue.measurement()

		document, err := encodeStateJSON(state)
		if err != nil {
			t.Fatalf("%s: encode: %v", vector.Name, err)
		}
		if document != vector.StoredStateJSON {
			t.Fatalf("%s: Go wrote\n%s\nwant\n%s", vector.Name, document, vector.StoredStateJSON)
		}
		encoded++
	}
	if encoded == 0 {
		t.Fatal("no go_encode vectors")
	}
}

// mainBranchState is the evaluator state struct as it was before the value
// became optional. An old evaluator decodes into it and re-encodes it, which
// drops SchemaVersion and turns null into 0.
type mainBranchState struct {
	Mode                     alertevaluator.Mode
	ConfirmedSlots           int
	PendingSince             time.Time
	LastBreachSlot           time.Time
	LastEvaluatedSlot        time.Time
	LastQuality              alertevaluator.Quality
	LastValue                float64
	ActiveEventID            string
	ActiveStatus             alertevaluator.EventStatus
	ActiveOpenedAt           time.Time
	OpeningOutboxes          map[alertevaluator.Channel]string
	CooldownUntil            time.Time
	LastNotifiedEventID      string
	SuppressionSourceEventID string
}

func TestOldEvaluatorRewriteOfVersionTwoStateFallsBackToLegacyDecode(t *testing.T) {
	checked := 0
	for _, vector := range loadValueVectors(t).StateVectors {
		if vector.ExpectedAfterOldGoRewrite == nil {
			continue
		}
		var old mainBranchState
		if err := json.Unmarshal([]byte(vector.StoredStateJSON), &old); err != nil {
			t.Fatalf("%s: an old evaluator cannot read the version 2 document: %v", vector.Name, err)
		}
		rewritten, err := json.Marshal(old)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(rewritten), "SchemaVersion") {
			t.Fatalf("%s: old rewrite kept the marker: %s", vector.Name, rewritten)
		}
		state, err := decodeStateJSON(string(rewritten))
		if err != nil {
			t.Fatalf("%s: decode old rewrite: %v", vector.Name, err)
		}
		assertMeasurement(t, vector.Name+" after old rewrite", state.LastValue, *vector.ExpectedAfterOldGoRewrite)
		if state.Mode != old.Mode || state.ActiveEventID != old.ActiveEventID || state.LastQuality != old.LastQuality ||
			!state.CooldownUntil.Equal(old.CooldownUntil) || state.OpeningOutboxes[alertevaluator.ChannelEmail] != old.OpeningOutboxes[alertevaluator.ChannelEmail] {
			t.Fatalf("%s: old rewrite lost lifecycle fields: %#v", vector.Name, state)
		}
		checked++
	}
	if checked == 0 {
		t.Fatal("no old-rewrite vectors")
	}
}

func TestStateErrorVectorsFailClosed(t *testing.T) {
	for _, vector := range loadValueVectors(t).StateErrorVectors {
		if state, err := decodeStateJSON(vector.StoredStateJSON); err == nil {
			t.Fatalf("%s: decoded %#v, want error", vector.Name, state)
		}
	}
}

func stateItemOutput(t *testing.T, document string, revision int64) *dynamodb.GetItemOutput {
	t.Helper()
	item, err := attributevalue.MarshalMap(map[string]any{
		"PK": "TENANT#tnt_1", "SK": "ALERT_STATE#rule_1", "entity_type": "alert_evaluation_state",
		"state_revision": revision, "state_json": document,
	})
	if err != nil {
		t.Fatal(err)
	}
	return &dynamodb.GetItemOutput{Item: item}
}

func TestLoadStateDecodesVectorsAndKeepsRevision(t *testing.T) {
	work, err := workFromItem(ruleItemWithLease())
	if err != nil {
		t.Fatal(err)
	}
	vectors := loadValueVectors(t)
	for _, vector := range vectors.StateVectors {
		store := Store{Table: "domain", Client: &fakeClient{getOutput: stateItemOutput(t, vector.StoredStateJSON, 7)}}
		loaded, err := store.LoadState(context.Background(), work)
		if err != nil {
			t.Fatalf("%s: %v", vector.Name, err)
		}
		if loaded.Revision != 7 {
			t.Fatalf("%s: revision = %d", vector.Name, loaded.Revision)
		}
		assertMeasurement(t, vector.Name+" LoadState", loaded.State.LastValue, vector.ExpectedLastValue)
	}
	for _, vector := range vectors.StateErrorVectors {
		store := Store{Table: "domain", Client: &fakeClient{getOutput: stateItemOutput(t, vector.StoredStateJSON, 7)}}
		if _, err := store.LoadState(context.Background(), work); err == nil {
			t.Fatalf("%s: LoadState succeeded, want error", vector.Name)
		}
	}
}

func TestCommitWritesStateJSONWithMarkerAndRetainedValue(t *testing.T) {
	work, err := workFromItem(ruleItemWithLease())
	if err != nil {
		t.Fatal(err)
	}
	slot := time.Date(2026, 7, 15, 12, 1, 45, 0, time.UTC)
	tests := []struct {
		name       string
		previous   alertevaluator.Measurement
		evaluation alertevaluator.Evaluation
		want       string
		wantValue  vectorValue
	}{
		{"determinate zero", alertevaluator.Measured(4.2), alertevaluator.Evaluation{Slot: slot, Quality: alertevaluator.QualitySufficient, Value: alertevaluator.Measured(0)}, `"LastValue":0`, vectorValue{Present: true, Value: 0}},
		{"stale keeps retained zero", alertevaluator.Measured(0), alertevaluator.Evaluation{Slot: slot, Quality: alertevaluator.QualityStaleData}, `"LastValue":0`, vectorValue{Present: true, Value: 0}},
		{"query error keeps retained negative", alertevaluator.Measured(-3.5), alertevaluator.Evaluation{Slot: slot, Quality: alertevaluator.QualityQueryError}, `"LastValue":-3.5`, vectorValue{Present: true, Value: -3.5}},
		{"never determinate stays null", alertevaluator.Measurement{}, alertevaluator.Evaluation{Slot: slot, Quality: alertevaluator.QualityInsufficientData}, `"LastValue":null`, vectorValue{}},
	}
	for _, test := range tests {
		current := alertevaluator.State{Mode: alertevaluator.ModeHealthy, LastValue: test.previous}
		decision := alertevaluator.Decide(work.Rule.Rule, current, test.evaluation, time.Minute)
		items, err := (Store{Table: "domain"}).commitItems(alertevaluator.CommitRequest{
			Work: work, PreviousState: alertevaluator.VersionedState{State: current, Revision: 2},
			Evaluation: test.evaluation, Decision: decision, Slot: slot, NextDue: slot.Add(time.Minute),
		})
		if err != nil {
			t.Fatalf("%s: %v", test.name, err)
		}
		state := unmarshalItem(t, items[1].Put.Item)
		document, _ := state["state_json"].(string)
		if !strings.HasPrefix(document, `{"SchemaVersion":2,`) || !strings.Contains(document, test.want) {
			t.Fatalf("%s: state_json = %s, want marker and %s", test.name, document, test.want)
		}
		decoded, err := decodeStateJSON(document)
		if err != nil {
			t.Fatal(err)
		}
		assertMeasurement(t, test.name+" round trip", decoded.LastValue, test.wantValue)
	}
}

func openingCommit(t *testing.T, value alertevaluator.Measurement) (alertevaluator.CommitRequest, []types.TransactWriteItem) {
	t.Helper()
	work, err := workFromItem(ruleItemWithLease())
	if err != nil {
		t.Fatal(err)
	}
	work.Rule.Duration = time.Minute
	slot := time.Date(2026, 7, 15, 12, 0, 45, 0, time.UTC)
	evaluation := alertevaluator.Evaluation{Slot: slot, Quality: alertevaluator.QualitySufficient, Breached: true, Value: value}
	decision := alertevaluator.Decide(work.Rule.Rule, alertevaluator.State{}, evaluation, time.Minute)
	if decision.Transition != alertevaluator.TransitionOpened {
		t.Fatalf("setup decision = %#v", decision)
	}
	request := alertevaluator.CommitRequest{
		Work: work, Evaluation: evaluation, Decision: decision, Slot: slot, NextDue: slot.Add(time.Minute),
	}
	items, err := (Store{Table: "domain"}).commitItems(request)
	if err != nil {
		t.Fatal(err)
	}
	return request, items
}

func activeMetadataCommit(t *testing.T, evaluation alertevaluator.Evaluation) []types.TransactWriteItem {
	t.Helper()
	work, err := workFromItem(ruleItemWithLease())
	if err != nil {
		t.Fatal(err)
	}
	current := alertevaluator.State{
		Mode: alertevaluator.ModeActive, ActiveEventID: "alert_1", ActiveStatus: alertevaluator.StatusOpen,
		LastValue: alertevaluator.Measured(4.2),
	}
	decision := alertevaluator.Decide(work.Rule.Rule, current, evaluation, time.Minute)
	if decision.Transition != alertevaluator.TransitionNone || decision.Next.Mode != alertevaluator.ModeActive {
		t.Fatalf("setup decision = %#v", decision)
	}
	items, err := (Store{Table: "domain"}).commitItems(alertevaluator.CommitRequest{
		Work: work, PreviousState: alertevaluator.VersionedState{State: current, Revision: 1},
		Evaluation: evaluation, Decision: decision, Slot: evaluation.Slot, NextDue: evaluation.Slot.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 3 || items[0].Update == nil || items[2].Update == nil {
		t.Fatalf("transaction shape = %#v", items)
	}
	return items
}

func numberAttribute(t *testing.T, label string, value types.AttributeValue, want string) {
	t.Helper()
	number, ok := value.(*types.AttributeValueMemberN)
	if !ok || number.Value != want {
		t.Fatalf("%s = %#v, want N %q", label, value, want)
	}
}

func TestDynamoDBValueVectorsForEveryValueAttribute(t *testing.T) {
	slot := time.Date(2026, 7, 15, 12, 1, 45, 0, time.UTC)
	for _, vector := range loadValueVectors(t).DynamoDBValueVectors {
		if vector.Value.Present {
			want := vector.Attribute["N"]
			if len(vector.Attribute) != 1 || want == "" {
				t.Fatalf("%s: present vector needs one N attribute: %#v", vector.Name, vector.Attribute)
			}
			_, items := openingCommit(t, vector.Value.measurement())
			numberAttribute(t, vector.Name+" rule :value", items[0].Update.ExpressionAttributeValues[":value"], want)
			numberAttribute(t, vector.Name+" event last_evaluation_value", items[2].Put.Item["last_evaluation_value"], want)
			for _, outbox := range items[4:] {
				numberAttribute(t, vector.Name+" outbox evaluation_value", outbox.Put.Item["evaluation_value"], want)
			}

			metadata := activeMetadataCommit(t, alertevaluator.Evaluation{
				Slot: slot, Quality: alertevaluator.QualitySufficient, Breached: true, Value: vector.Value.measurement(),
			})
			for name, update := range map[string]*types.Update{"rule": metadata[0].Update, "event": metadata[2].Update} {
				if !strings.Contains(*update.UpdateExpression, "#last_value = :value") || strings.Contains(*update.UpdateExpression, "REMOVE #last_value") {
					t.Fatalf("%s: %s update = %s", vector.Name, name, *update.UpdateExpression)
				}
				numberAttribute(t, vector.Name+" "+name+" metadata :value", update.ExpressionAttributeValues[":value"], want)
			}
			continue
		}

		if vector.Attribute != nil {
			t.Fatalf("%s: absent vector must have no attribute: %#v", vector.Name, vector.Attribute)
		}
		for _, evaluation := range []alertevaluator.Evaluation{
			{Slot: slot, Quality: alertevaluator.QualityInsufficientData},
			{Slot: slot, Quality: alertevaluator.QualityStaleData, Value: alertevaluator.Measured(9.9)},
			{Slot: slot, Quality: alertevaluator.QualityQueryError, Value: alertevaluator.Measured(0)},
			{Slot: slot, Quality: alertevaluator.QualitySufficient, Breached: true},
		} {
			metadata := activeMetadataCommit(t, evaluation)
			for name, update := range map[string]*types.Update{"rule": metadata[0].Update, "event": metadata[2].Update} {
				_, hasValue := update.ExpressionAttributeValues[":value"]
				if hasValue || !strings.Contains(*update.UpdateExpression, "REMOVE #last_value") || strings.Contains(*update.UpdateExpression, "#last_value = :value") {
					t.Fatalf("%s/%s: %s update = %s values = %#v", vector.Name, evaluation.Quality, name, *update.UpdateExpression, update.ExpressionAttributeValues)
				}
				for token, value := range update.ExpressionAttributeValues {
					if _, isNull := value.(*types.AttributeValueMemberNULL); isNull {
						t.Fatalf("%s/%s: %s update writes NULL for %s", vector.Name, evaluation.Quality, name, token)
					}
				}
			}
		}
	}
}

func TestCommitRefusesToOpenOrResolveWithoutADeterminateValue(t *testing.T) {
	request, _ := openingCommit(t, alertevaluator.Measured(4.2))
	request.Evaluation.Value = alertevaluator.Measurement{}
	if _, err := (Store{Table: "domain"}).commitItems(request); err == nil {
		t.Fatal("opening commit without a value succeeded")
	}

	work, err := workFromItem(ruleItemWithLease())
	if err != nil {
		t.Fatal(err)
	}
	slot := time.Date(2026, 7, 15, 12, 5, 45, 0, time.UTC)
	current := alertevaluator.State{
		Mode: alertevaluator.ModeActive, ActiveEventID: "alert_existing", ActiveStatus: alertevaluator.StatusOpen,
		OpeningOutboxes: map[alertevaluator.Channel]string{alertevaluator.ChannelEmail: "outbox_open_email"},
	}
	clean := alertevaluator.Evaluation{Slot: slot, Quality: alertevaluator.QualitySufficient, Value: alertevaluator.Measured(8.1)}
	decision := alertevaluator.Decide(work.Rule.Rule, current, clean, time.Minute)
	if decision.Transition != alertevaluator.TransitionRecovered {
		t.Fatalf("setup decision = %#v", decision)
	}
	resolution := alertevaluator.CommitRequest{
		Work: work, PreviousState: alertevaluator.VersionedState{State: current, Revision: 1},
		Evaluation: clean, Decision: decision, Slot: slot, NextDue: slot.Add(time.Minute),
	}
	items, err := (Store{Table: "domain"}).commitItems(resolution)
	if err != nil {
		t.Fatal(err)
	}
	numberAttribute(t, "resolution :value", items[2].Update.ExpressionAttributeValues[":value"], "8.1")

	resolution.Evaluation.Value = alertevaluator.Measurement{}
	if _, err := (Store{Table: "domain"}).commitItems(resolution); err == nil {
		t.Fatal("resolution commit without a value succeeded")
	}
	resolution.Evaluation = alertevaluator.Evaluation{Slot: slot, Quality: alertevaluator.QualityStaleData, Value: alertevaluator.Measured(8.1)}
	if _, err := (Store{Table: "domain"}).commitItems(resolution); err == nil {
		t.Fatal("resolution commit with an indeterminate evaluation succeeded")
	}
}

func TestUnsupportedStateSchemaIsDistinguishableFromMalformedState(t *testing.T) {
	work, err := workFromItem(ruleItemWithLease())
	if err != nil {
		t.Fatal(err)
	}
	for _, vector := range loadValueVectors(t).StateErrorVectors {
		wantSchema := vector.Name == "unknown_future_version" || vector.Name == "negative_version"
		_, decodeErr := decodeStateJSON(vector.StoredStateJSON)
		store := Store{Table: "domain", Client: &fakeClient{getOutput: stateItemOutput(t, vector.StoredStateJSON, 7)}}
		_, loadErr := store.LoadState(context.Background(), work)
		for label, err := range map[string]error{"decode": decodeErr, "LoadState": loadErr} {
			if err == nil || errors.Is(err, alertevaluator.ErrUnsupportedStateSchema) != wantSchema {
				t.Fatalf("%s %s: err = %v, want unsupported schema = %v", vector.Name, label, err, wantSchema)
			}
		}
	}
}

func TestCommitRecordsSufficientWithoutValueAsInsufficient(t *testing.T) {
	work, err := workFromItem(ruleItemWithLease())
	if err != nil {
		t.Fatal(err)
	}
	slot := time.Date(2026, 7, 15, 12, 1, 45, 0, time.UTC)
	current := alertevaluator.State{
		Mode: alertevaluator.ModeActive, ActiveEventID: "alert_1", ActiveStatus: alertevaluator.StatusOpen,
		LastValue: alertevaluator.Measured(4.2),
	}
	tokens := map[string]string{}
	for name, evaluation := range map[string]alertevaluator.Evaluation{
		"sufficient without value": {Slot: slot, Quality: alertevaluator.QualitySufficient, Breached: true},
		"insufficient":             {Slot: slot, Quality: alertevaluator.QualityInsufficientData},
	} {
		client := &fakeClient{}
		request := alertevaluator.CommitRequest{
			Work: work, PreviousState: alertevaluator.VersionedState{State: current, Revision: 1}, Evaluation: evaluation,
			Decision: alertevaluator.Decide(work.Rule.Rule, current, evaluation, time.Minute), Slot: slot, NextDue: slot.Add(time.Minute),
		}
		if err := (Store{Table: "domain", Client: client}).Commit(context.Background(), request); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		items := client.transactInput.TransactItems
		for label, update := range map[string]*types.Update{"rule": items[0].Update, "event": items[2].Update} {
			quality, ok := update.ExpressionAttributeValues[":quality"].(*types.AttributeValueMemberS)
			if !ok || quality.Value != string(alertevaluator.QualityInsufficientData) {
				t.Fatalf("%s: %s recorded quality %#v, want insufficient_data", name, label, update.ExpressionAttributeValues[":quality"])
			}
			if _, hasValue := update.ExpressionAttributeValues[":value"]; hasValue {
				t.Fatalf("%s: %s update writes a value", name, label)
			}
		}
		state, err := decodeStateJSON(unmarshalItem(t, items[1].Put.Item)["state_json"].(string))
		if err != nil {
			t.Fatal(err)
		}
		if state.LastQuality != alertevaluator.QualityInsufficientData {
			t.Fatalf("%s: state LastQuality = %s", name, state.LastQuality)
		}
		tokens[name] = *client.transactInput.ClientRequestToken
	}
	if tokens["sufficient without value"] != tokens["insufficient"] {
		t.Fatalf("commit tokens differ for equivalent evaluations: %#v", tokens)
	}
}
