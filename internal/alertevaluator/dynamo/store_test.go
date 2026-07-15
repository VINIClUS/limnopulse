package dynamo

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type fakeClient struct {
	queryInput    *dynamodb.QueryInput
	queryOutput   *dynamodb.QueryOutput
	updateInput   *dynamodb.UpdateItemInput
	updateOutput  *dynamodb.UpdateItemOutput
	getOutput     *dynamodb.GetItemOutput
	transactInput *dynamodb.TransactWriteItemsInput
	updateInputs  []*dynamodb.UpdateItemInput
}

func (client *fakeClient) Query(_ context.Context, input *dynamodb.QueryInput, _ ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error) {
	client.queryInput = input
	return client.queryOutput, nil
}

func (client *fakeClient) UpdateItem(_ context.Context, input *dynamodb.UpdateItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error) {
	client.updateInput = input
	client.updateInputs = append(client.updateInputs, input)
	return client.updateOutput, nil
}

func TestBackfillQueriesExplicitTenantAndOnlyUpdatesEnabledActiveRules(t *testing.T) {
	active, _ := attributevalue.MarshalMap(ruleItem())
	disabledValue := ruleItem()
	disabledValue["rule_id"] = "rule_disabled"
	disabledValue["SK"] = "ALERT_RULE#rule_disabled"
	disabledValue["enabled"] = false
	disabled, _ := attributevalue.MarshalMap(disabledValue)
	client := &fakeClient{
		queryOutput:  &dynamodb.QueryOutput{Items: []map[string]types.AttributeValue{active, disabled}},
		updateOutput: &dynamodb.UpdateItemOutput{},
	}
	store := Store{Table: "domain", Client: client}
	now := time.Date(2026, 7, 15, 12, 0, 0, 0, time.UTC)

	summary, err := store.BackfillSchedule(context.Background(), BackfillOptions{
		Tenants: []string{"tnt_1"}, EvaluationTime: now, Apply: true, PageSize: 25,
	})
	if err != nil {
		t.Fatal(err)
	}
	if summary.Queried != 2 || summary.Eligible != 1 || summary.Updated != 1 || len(client.updateInputs) != 1 {
		t.Fatalf("summary = %#v, updates = %d", summary, len(client.updateInputs))
	}
	if client.queryInput.IndexName != nil || !strings.Contains(*client.queryInput.KeyConditionExpression, "begins_with(SK") {
		t.Fatalf("backfill did not use tenant Query: %#v", client.queryInput)
	}
	update := client.updateInputs[0]
	if !strings.Contains(
		*update.UpdateExpression,
		"#evaluation_revision = if_not_exists(#evaluation_revision, :initial_revision)",
	) {
		t.Fatalf("backfill did not initialize evaluation revision: %s", *update.UpdateExpression)
	}
	if update.ExpressionAttributeNames["#evaluation_revision"] != "evaluation_revision" {
		t.Fatalf("evaluation revision attribute name = %#v", update.ExpressionAttributeNames)
	}
	initialRevision, ok := update.ExpressionAttributeValues[":initial_revision"].(*types.AttributeValueMemberN)
	if !ok || initialRevision.Value != "1" {
		t.Fatalf("initial evaluation revision = %#v", update.ExpressionAttributeValues[":initial_revision"])
	}
}

func (client *fakeClient) GetItem(_ context.Context, _ *dynamodb.GetItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error) {
	return client.getOutput, nil
}

func (client *fakeClient) TransactWriteItems(_ context.Context, input *dynamodb.TransactWriteItemsInput, _ ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error) {
	client.transactInput = input
	return &dynamodb.TransactWriteItemsOutput{}, nil
}

func TestQueryDueUsesEvaluationGSIAndPaginatesWithoutScan(t *testing.T) {
	lastKey := map[string]types.AttributeValue{
		"PK":     &types.AttributeValueMemberS{Value: "TENANT#tnt_1"},
		"SK":     &types.AttributeValueMemberS{Value: "ALERT_RULE#rule_1"},
		"GSI1PK": &types.AttributeValueMemberS{Value: "ALERT_EVALUATION#V1#BUCKET#06"},
		"GSI1SK": &types.AttributeValueMemberS{Value: "2026-07-15T12:00:45.000000000Z#TENANT#tnt_1#RULE#rule_1"},
	}
	client := &fakeClient{queryOutput: &dynamodb.QueryOutput{
		Items: []map[string]types.AttributeValue{lastKey}, LastEvaluatedKey: lastKey,
	}}
	store := Store{Table: "domain", Client: client}
	due := time.Date(2026, 7, 15, 12, 0, 45, 0, time.UTC)

	page, err := store.QueryDue(context.Background(), alertevaluator.DueRequest{Bucket: 6, DueThrough: due, PageSize: 25})
	if err != nil {
		t.Fatal(err)
	}
	if *client.queryInput.IndexName != EvaluationIndex || *client.queryInput.KeyConditionExpression != "GSI1PK = :pk AND GSI1SK <= :due" || *client.queryInput.Limit != 25 {
		t.Fatalf("query = %#v", client.queryInput)
	}
	if page.Candidates[0].RuleID != "rule_1" || page.Candidates[0].Bucket != 6 || page.NextToken == "" {
		t.Fatalf("page = %#v", page)
	}
	decoded, err := decodeToken(page.NextToken)
	if err != nil || len(decoded) != 4 {
		t.Fatalf("token = %q, err = %v", page.NextToken, err)
	}
}

func TestClaimUsesDueBoundLeaseExpiryAndFencingEpoch(t *testing.T) {
	rule := ruleItem()
	rule["lease_owner"] = "run_1"
	rule["lease_epoch"] = int64(4)
	attributes, err := attributevalue.MarshalMap(rule)
	if err != nil {
		t.Fatal(err)
	}
	client := &fakeClient{updateOutput: &dynamodb.UpdateItemOutput{Attributes: attributes}}
	store := Store{Table: "domain", Client: client}
	now := time.Date(2026, 7, 15, 12, 1, 1, 0, time.UTC)

	work, err := store.Claim(context.Background(), alertevaluator.Candidate{PK: "TENANT#tnt_1", SK: "ALERT_RULE#rule_1"}, alertevaluator.LeaseRequest{
		Owner: "run_1", Now: now, ExpiresAt: now.Add(15 * time.Second), DueThrough: now.Add(-16 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	condition := *client.updateInput.ConditionExpression
	for _, fragment := range []string{"#enabled = :true", "#status = :active", "#next_due <= :due_through", "#lease_expires <= :now", "#lease_owner = :owner"} {
		if !strings.Contains(condition, fragment) {
			t.Fatalf("condition missing %q: %s", fragment, condition)
		}
	}
	if work.LeaseEpoch != 4 || work.Rule.Duration != 2*time.Minute || work.Rule.EvaluationRevision != 3 {
		t.Fatalf("work = %#v", work)
	}
}

func TestWorkDecodeRejectsUnknownOperatorInsteadOfTreatingItAsClean(t *testing.T) {
	item := ruleItem()
	item["operator"] = "unknown"
	encoded, err := attributevalue.MarshalMap(item)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := workFromItem(encoded); err == nil || !strings.Contains(err.Error(), "operator") {
		t.Fatalf("workFromItem() error = %v", err)
	}
}

func TestCommitOpeningAtomicallyWritesEventTransitionAndOutboxes(t *testing.T) {
	client := &fakeClient{}
	store := Store{Table: "domain", Client: client}
	slot := time.Date(2026, 7, 15, 12, 0, 45, 0, time.UTC)
	work, err := workFromItem(ruleItemWithLease())
	if err != nil {
		t.Fatal(err)
	}
	work.Rule.Duration = time.Minute
	evaluation := alertevaluator.Evaluation{Slot: slot, Quality: alertevaluator.QualitySufficient, Breached: true, Value: 4.2}
	decision := alertevaluator.Decide(work.Rule.Rule, alertevaluator.State{}, evaluation, time.Minute)

	err = store.Commit(context.Background(), alertevaluator.CommitRequest{
		Work: work, PreviousState: alertevaluator.VersionedState{}, Evaluation: evaluation,
		Decision: decision, Slot: slot, NextDue: slot.Add(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	items := client.transactInput.TransactItems
	if len(items) != 6 {
		t.Fatalf("transaction has %d items, want rule + state + event + transition + 2 outboxes", len(items))
	}
	if items[0].Update == nil || items[1].Put == nil || items[2].Put == nil || items[3].Put == nil || items[4].Put == nil || items[5].Put == nil {
		t.Fatalf("transaction shape = %#v", items)
	}
	if client.transactInput.ClientRequestToken == nil || len(*client.transactInput.ClientRequestToken) > 36 {
		t.Fatalf("client token = %#v", client.transactInput.ClientRequestToken)
	}
	event := unmarshalItem(t, items[2].Put.Item)
	if event["event_id"] != decision.EventID || fmt.Sprint(event["rule_version"]) != "2" || event["window_end"] != alertevaluator.FixedUTCTimestamp(slot) {
		t.Fatalf("event = %#v", event)
	}
}

func ruleItem() map[string]any {
	return map[string]any{
		"PK": "TENANT#tnt_1", "SK": "ALERT_RULE#rule_1", "tenant_id": "tnt_1", "rule_id": "rule_1",
		"name": "Low oxygen", "pond_id": "pond_1", "metric": "do_mg_l", "operator": "<", "threshold": 5.0,
		"aggregation": "min", "window": "1m", "duration": "2m", "severity": "critical",
		"channels": []string{"email", "telegram"}, "cooldown_seconds": int64(1800), "enabled": true,
		"status": "active", "version": int64(2), "evaluation_revision": int64(3),
		"next_evaluation_at": "2026-07-15T12:00:45.000000000Z",
	}
}

func ruleItemWithLease() map[string]types.AttributeValue {
	item := ruleItem()
	item["lease_owner"] = "run_1"
	item["lease_epoch"] = int64(4)
	encoded, _ := attributevalue.MarshalMap(item)
	return encoded
}

func unmarshalItem(t *testing.T, item map[string]types.AttributeValue) map[string]any {
	t.Helper()
	var value map[string]any
	if err := attributevalue.UnmarshalMap(item, &value); err != nil {
		t.Fatal(err)
	}
	return value
}
