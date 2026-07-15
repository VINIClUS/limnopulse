package dynamo

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

const EvaluationIndex = "AlertEvaluationByDue"

type Client interface {
	Query(context.Context, *dynamodb.QueryInput, ...func(*dynamodb.Options)) (*dynamodb.QueryOutput, error)
	UpdateItem(context.Context, *dynamodb.UpdateItemInput, ...func(*dynamodb.Options)) (*dynamodb.UpdateItemOutput, error)
	GetItem(context.Context, *dynamodb.GetItemInput, ...func(*dynamodb.Options)) (*dynamodb.GetItemOutput, error)
	TransactWriteItems(context.Context, *dynamodb.TransactWriteItemsInput, ...func(*dynamodb.Options)) (*dynamodb.TransactWriteItemsOutput, error)
}

type Store struct {
	Table  string
	Client Client
}

func (store Store) QueryDue(ctx context.Context, request alertevaluator.DueRequest) (alertevaluator.DuePage, error) {
	values, err := attributevalue.MarshalMap(map[string]any{
		":pk":  fmt.Sprintf("ALERT_EVALUATION#V1#BUCKET#%02d", request.Bucket),
		":due": alertevaluator.FixedUTCTimestamp(request.DueThrough) + "#\uffff",
	})
	if err != nil {
		return alertevaluator.DuePage{}, err
	}
	input := &dynamodb.QueryInput{
		TableName: aws.String(store.Table), IndexName: aws.String(EvaluationIndex),
		KeyConditionExpression:    aws.String("GSI1PK = :pk AND GSI1SK <= :due"),
		ExpressionAttributeValues: values, Limit: aws.Int32(int32(request.PageSize)),
		ConsistentRead: aws.Bool(false),
	}
	if request.NextToken != "" {
		input.ExclusiveStartKey, err = decodeToken(request.NextToken)
		if err != nil {
			return alertevaluator.DuePage{}, fmt.Errorf("decode pagination token: %w", err)
		}
	}
	output, err := store.Client.Query(ctx, input)
	if err != nil {
		return alertevaluator.DuePage{}, fmt.Errorf("query due rules: %w", err)
	}
	page := alertevaluator.DuePage{Candidates: make([]alertevaluator.Candidate, 0, len(output.Items))}
	for _, item := range output.Items {
		var key struct {
			PK string `dynamodbav:"PK"`
			SK string `dynamodbav:"SK"`
		}
		if err := attributevalue.UnmarshalMap(item, &key); err != nil {
			return alertevaluator.DuePage{}, fmt.Errorf("decode due rule key: %w", err)
		}
		ruleID := strings.TrimPrefix(key.SK, "ALERT_RULE#")
		if key.PK == "" || ruleID == key.SK || ruleID == "" {
			return alertevaluator.DuePage{}, fmt.Errorf("invalid alert rule key in evaluation index")
		}
		page.Candidates = append(page.Candidates, alertevaluator.Candidate{
			PK: key.PK, SK: key.SK, RuleID: ruleID, Bucket: request.Bucket,
		})
	}
	if len(output.LastEvaluatedKey) > 0 {
		page.NextToken, err = encodeToken(output.LastEvaluatedKey)
		if err != nil {
			return alertevaluator.DuePage{}, fmt.Errorf("encode pagination token: %w", err)
		}
	}
	return page, nil
}

func (store Store) Claim(ctx context.Context, candidate alertevaluator.Candidate, lease alertevaluator.LeaseRequest) (alertevaluator.Work, error) {
	key, _ := attributevalue.MarshalMap(map[string]string{"PK": candidate.PK, "SK": candidate.SK})
	values, _ := attributevalue.MarshalMap(map[string]any{
		":owner": lease.Owner, ":now": alertevaluator.FixedUTCTimestamp(lease.Now),
		":expires":     alertevaluator.FixedUTCTimestamp(lease.ExpiresAt),
		":due_through": alertevaluator.FixedUTCTimestamp(lease.DueThrough),
		":zero":        int64(0), ":one": int64(1), ":true": true, ":active": "active",
	})
	output, err := store.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(store.Table), Key: key,
		UpdateExpression:    aws.String("SET #lease_owner = :owner, #lease_expires = :expires, #lease_epoch = if_not_exists(#lease_epoch, :zero) + :one"),
		ConditionExpression: aws.String("attribute_exists(PK) AND #enabled = :true AND #status = :active AND #next_due <= :due_through AND (attribute_not_exists(#lease_owner) OR attribute_not_exists(#lease_expires) OR #lease_expires <= :now OR #lease_owner = :owner)"),
		ExpressionAttributeNames: map[string]string{
			"#enabled": "enabled", "#status": "status", "#next_due": "next_evaluation_at",
			"#lease_owner": "lease_owner", "#lease_expires": "lease_expires_at", "#lease_epoch": "lease_epoch",
		},
		ExpressionAttributeValues: values, ReturnValues: types.ReturnValueAllNew,
	})
	if err != nil {
		if isConditional(err) {
			return alertevaluator.Work{}, alertevaluator.ErrLeaseConflict
		}
		return alertevaluator.Work{}, fmt.Errorf("claim alert rule: %w", err)
	}
	work, err := workFromItem(output.Attributes)
	if err != nil {
		return alertevaluator.Work{}, fmt.Errorf("decode claimed alert rule: %w", err)
	}
	return work, nil
}

func (store Store) LoadState(ctx context.Context, work alertevaluator.Work) (alertevaluator.VersionedState, error) {
	key, _ := attributevalue.MarshalMap(map[string]string{
		"PK": work.PK, "SK": stateSortKey(work.Rule.RuleID),
	})
	output, err := store.Client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(store.Table), Key: key, ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return alertevaluator.VersionedState{}, fmt.Errorf("load alert evaluation state: %w", err)
	}
	if len(output.Item) == 0 {
		return alertevaluator.VersionedState{}, nil
	}
	var item struct {
		StateJSON string `dynamodbav:"state_json"`
		Revision  int64  `dynamodbav:"state_revision"`
	}
	if err := attributevalue.UnmarshalMap(output.Item, &item); err != nil {
		return alertevaluator.VersionedState{}, fmt.Errorf("decode alert evaluation state: %w", err)
	}
	var state alertevaluator.State
	if err := json.Unmarshal([]byte(item.StateJSON), &state); err != nil {
		return alertevaluator.VersionedState{}, fmt.Errorf("decode alert evaluation state JSON: %w", err)
	}
	return alertevaluator.VersionedState{State: state, Revision: item.Revision}, nil
}

func (store Store) Commit(ctx context.Context, request alertevaluator.CommitRequest) error {
	items, err := store.commitItems(request)
	if err != nil {
		return err
	}
	token := commitToken(request)
	_, err = store.Client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{
		TransactItems: items, ClientRequestToken: aws.String(token),
	})
	if err != nil {
		if isConditional(err) {
			return fmt.Errorf("commit alert evaluation lost fencing condition: %w", alertevaluator.ErrLeaseConflict)
		}
		return fmt.Errorf("commit alert evaluation: %w", err)
	}
	return nil
}

func (store Store) commitItems(request alertevaluator.CommitRequest) ([]types.TransactWriteItem, error) {
	ruleUpdate, err := store.ruleScheduleUpdate(request)
	if err != nil {
		return nil, err
	}
	statePut, err := store.statePut(request)
	if err != nil {
		return nil, err
	}
	items := []types.TransactWriteItem{{Update: ruleUpdate}, {Put: statePut}}

	switch request.Decision.Transition {
	case alertevaluator.TransitionOpened, alertevaluator.TransitionSuppressed:
		event, err := store.eventPut(request)
		if err != nil {
			return nil, err
		}
		items = append(items, types.TransactWriteItem{Put: event})
		transition, err := store.transitionPut(request, request.Decision.EventID)
		if err != nil {
			return nil, err
		}
		items = append(items, types.TransactWriteItem{Put: transition})
	case alertevaluator.TransitionRecovered:
		resolved, err := store.eventResolutionUpdate(request)
		if err != nil {
			return nil, err
		}
		items = append(items, types.TransactWriteItem{Update: resolved})
		transition, err := store.transitionPut(request, request.Decision.ResolvedEventID)
		if err != nil {
			return nil, err
		}
		items = append(items, types.TransactWriteItem{Put: transition})
	case alertevaluator.TransitionNone:
		if request.Decision.Next.Mode == alertevaluator.ModeActive {
			metadata, err := store.eventMetadataUpdate(request)
			if err != nil {
				return nil, err
			}
			items = append(items, types.TransactWriteItem{Update: metadata})
		}
	}
	for _, outbox := range request.Decision.Outboxes {
		put, err := store.outboxPut(request, outbox)
		if err != nil {
			return nil, err
		}
		items = append(items, types.TransactWriteItem{Put: put})
	}
	return items, nil
}

func (store Store) ruleScheduleUpdate(request alertevaluator.CommitRequest) (*types.Update, error) {
	key, _ := attributevalue.MarshalMap(map[string]string{"PK": request.Work.PK, "SK": request.Work.SK})
	valueMap := map[string]any{
		":next_due": alertevaluator.FixedUTCTimestamp(request.NextDue),
		":gsi_sk":   alertevaluator.FixedUTCTimestamp(request.NextDue) + "#TENANT#" + request.Work.Rule.TenantID + "#RULE#" + request.Work.Rule.RuleID,
		":slot":     alertevaluator.FixedUTCTimestamp(request.Slot), ":quality": string(request.Evaluation.Quality),
		":version":             request.Work.Rule.Version,
		":evaluation_revision": request.Work.Rule.EvaluationRevision, ":owner": request.Work.LeaseOwner,
		":epoch": request.Work.LeaseEpoch, ":true": true, ":active": "active",
	}
	updateExpression := "SET #next_due = :next_due, #gsi_sk = :gsi_sk, #last_slot = :slot, #last_quality = :quality"
	if request.Evaluation.Quality == alertevaluator.QualitySufficient {
		valueMap[":value"] = request.Evaluation.Value
		updateExpression += ", #last_value = :value REMOVE #lease_owner, #lease_expires"
	} else {
		updateExpression += " REMOVE #last_value, #lease_owner, #lease_expires"
	}
	values, err := attributevalue.MarshalMap(valueMap)
	if err != nil {
		return nil, err
	}
	return &types.Update{
		TableName: aws.String(store.Table), Key: key,
		UpdateExpression:    aws.String(updateExpression),
		ConditionExpression: aws.String("#version = :version AND #evaluation_revision = :evaluation_revision AND #lease_owner = :owner AND #lease_epoch = :epoch AND #enabled = :true AND #status = :active"),
		ExpressionAttributeNames: map[string]string{
			"#next_due": "next_evaluation_at", "#gsi_sk": "GSI1SK", "#last_slot": "last_evaluated_slot",
			"#last_quality": "last_evaluation_quality", "#last_value": "last_evaluation_value",
			"#version": "version", "#evaluation_revision": "evaluation_revision", "#lease_owner": "lease_owner",
			"#lease_expires": "lease_expires_at", "#lease_epoch": "lease_epoch", "#enabled": "enabled", "#status": "status",
		}, ExpressionAttributeValues: values,
	}, nil
}

func (store Store) statePut(request alertevaluator.CommitRequest) (*types.Put, error) {
	stateJSON, err := json.Marshal(request.Decision.Next)
	if err != nil {
		return nil, fmt.Errorf("encode alert state: %w", err)
	}
	item, err := attributevalue.MarshalMap(map[string]any{
		"PK": request.Work.PK, "SK": stateSortKey(request.Work.Rule.RuleID),
		"entity_type": "alert_evaluation_state", "tenant_id": request.Work.Rule.TenantID,
		"rule_id": request.Work.Rule.RuleID, "state_revision": request.PreviousState.Revision + 1,
		"state_json": string(stateJSON), "updated_at": alertevaluator.FixedUTCTimestamp(request.Slot),
	})
	if err != nil {
		return nil, err
	}
	put := &types.Put{TableName: aws.String(store.Table), Item: item}
	if request.PreviousState.Revision == 0 {
		put.ConditionExpression = aws.String("attribute_not_exists(PK) AND attribute_not_exists(SK)")
	} else {
		values, _ := attributevalue.MarshalMap(map[string]any{":revision": request.PreviousState.Revision})
		put.ConditionExpression = aws.String("#revision = :revision")
		put.ExpressionAttributeNames = map[string]string{"#revision": "state_revision"}
		put.ExpressionAttributeValues = values
	}
	return put, nil
}

func (store Store) eventPut(request alertevaluator.CommitRequest) (*types.Put, error) {
	status := request.Decision.Next.ActiveStatus
	item, err := attributevalue.MarshalMap(map[string]any{
		"PK": request.Work.PK, "SK": eventSortKey(request.Decision.EventID), "entity_type": "alert_event",
		"event_id": request.Decision.EventID, "tenant_id": request.Work.Rule.TenantID,
		"rule_id": request.Work.Rule.RuleID, "rule_version": request.Work.Rule.Version,
		"evaluation_revision": request.Work.Rule.EvaluationRevision, "rule_name": request.Work.Rule.Name,
		"pond_id": request.Work.Rule.PondID, "device_id": request.Work.Rule.DeviceID,
		"metric": request.Work.Rule.Metric, "operator": request.Work.Rule.Operator,
		"threshold": request.Work.Rule.Threshold, "aggregation": string(request.Work.Rule.Aggregation),
		"severity": request.Work.Rule.Severity, "status": string(status), "opened_at": alertevaluator.FixedUTCTimestamp(request.Slot),
		"created_at": alertevaluator.FixedUTCTimestamp(request.Slot), "updated_at": alertevaluator.FixedUTCTimestamp(request.Slot),
		"version": int64(1), "schema_version": int64(1),
		"confirmed_open_window_end": alertevaluator.FixedUTCTimestamp(request.Slot),
		"window_start":              alertevaluator.FixedUTCTimestamp(request.Slot.Add(-request.Work.Rule.Window)),
		"window_end":                alertevaluator.FixedUTCTimestamp(request.Slot), "last_evaluated_at": alertevaluator.FixedUTCTimestamp(request.Slot),
		"last_evaluation_quality": string(request.Evaluation.Quality), "last_evaluation_value": request.Evaluation.Value,
		"suppression_source_event_id": request.Decision.Next.SuppressionSourceEventID,
		"GSI2PK":                      request.Work.PK + "#ALERT_EVENTS",
		"GSI2SK":                      alertevaluator.FixedUTCTimestamp(request.Slot) + "#EVENT#" + request.Decision.EventID,
	})
	if err != nil {
		return nil, err
	}
	return &types.Put{TableName: aws.String(store.Table), Item: item, ConditionExpression: aws.String("attribute_not_exists(PK) AND attribute_not_exists(SK)")}, nil
}

func (store Store) transitionPut(request alertevaluator.CommitRequest, eventID string) (*types.Put, error) {
	item, err := attributevalue.MarshalMap(map[string]any{
		"PK":          request.Work.PK,
		"SK":          eventSortKey(eventID) + "#TRANSITION#" + alertevaluator.FixedUTCTimestamp(request.Slot) + "#" + string(request.Decision.Transition),
		"entity_type": "alert_event_transition", "event_id": eventID,
		"tenant_id": request.Work.Rule.TenantID, "rule_id": request.Work.Rule.RuleID,
		"transition": string(request.Decision.Transition), "actor_type": "evaluator",
		"created_at": alertevaluator.FixedUTCTimestamp(request.Slot),
	})
	if err != nil {
		return nil, err
	}
	return &types.Put{TableName: aws.String(store.Table), Item: item, ConditionExpression: aws.String("attribute_not_exists(PK) AND attribute_not_exists(SK)")}, nil
}

func (store Store) outboxPut(request alertevaluator.CommitRequest, outbox alertevaluator.OutboxDecision) (*types.Put, error) {
	eventID := request.Decision.EventID
	if eventID == "" {
		eventID = request.Decision.ResolvedEventID
	}
	item, err := attributevalue.MarshalMap(map[string]any{
		"PK": request.Work.PK, "SK": "NOTIFICATION_OUTBOX#" + outbox.OutboxID,
		"entity_type": "notification_outbox", "outbox_id": outbox.OutboxID,
		"event_id": eventID, "tenant_id": request.Work.Rule.TenantID, "rule_id": request.Work.Rule.RuleID,
		"channel": string(outbox.Channel), "kind": string(outbox.Kind), "status": string(outbox.Status),
		"depends_on_outbox_id": outbox.DependsOnOutboxID, "created_at": alertevaluator.FixedUTCTimestamp(request.Slot),
	})
	if err != nil {
		return nil, err
	}
	return &types.Put{TableName: aws.String(store.Table), Item: item, ConditionExpression: aws.String("attribute_not_exists(PK) AND attribute_not_exists(SK)")}, nil
}

func (store Store) eventMetadataUpdate(request alertevaluator.CommitRequest) (*types.Update, error) {
	if request.Evaluation.Quality == alertevaluator.QualitySufficient {
		return store.activeEventUpdate(request, activeEventMetadataWithValue)
	}
	return store.activeEventUpdate(request, activeEventMetadataWithoutValue)
}

func (store Store) eventResolutionUpdate(request alertevaluator.CommitRequest) (*types.Update, error) {
	return store.activeEventUpdate(request, activeEventResolution)
}

type activeEventUpdateKind uint8

const (
	activeEventMetadataWithValue activeEventUpdateKind = iota
	activeEventMetadataWithoutValue
	activeEventResolution
)

func (store Store) activeEventUpdate(request alertevaluator.CommitRequest, kind activeEventUpdateKind) (*types.Update, error) {
	eventID := request.Decision.Next.ActiveEventID
	valueMap := map[string]any{
		":slot": alertevaluator.FixedUTCTimestamp(request.Slot), ":quality": string(request.Evaluation.Quality),
		":open": "open", ":acknowledged": "acknowledged",
		":suppressed": "suppressed",
	}
	names := map[string]string{
		"#status": "status", "#last_at": "last_evaluated_at",
		"#last_quality": "last_evaluation_quality", "#last_value": "last_evaluation_value",
	}
	var expression string
	switch kind {
	case activeEventMetadataWithValue:
		expression = "SET #last_at = :slot, #last_quality = :quality, #last_value = :value"
		valueMap[":value"] = request.Evaluation.Value
	case activeEventMetadataWithoutValue:
		expression = "SET #last_at = :slot, #last_quality = :quality REMOVE #last_value"
	case activeEventResolution:
		eventID = request.Decision.ResolvedEventID
		expression = "SET #status = :resolved, #resolved_at = :slot, #updated_at = :slot, #version = #version + :one, #last_at = :slot, #last_quality = :quality, #last_value = :value"
		valueMap[":value"] = request.Evaluation.Value
		valueMap[":resolved"] = "resolved"
		valueMap[":one"] = int64(1)
		names["#resolved_at"] = "resolved_at"
		names["#updated_at"] = "updated_at"
		names["#version"] = "version"
	default:
		return nil, fmt.Errorf("unsupported active event update kind %d", kind)
	}
	key, _ := attributevalue.MarshalMap(map[string]string{"PK": request.Work.PK, "SK": eventSortKey(eventID)})
	values, err := attributevalue.MarshalMap(valueMap)
	if err != nil {
		return nil, err
	}
	return &types.Update{
		TableName: aws.String(store.Table), Key: key, UpdateExpression: aws.String(expression),
		ConditionExpression:      aws.String("#status IN (:open, :acknowledged, :suppressed)"),
		ExpressionAttributeNames: names, ExpressionAttributeValues: values,
	}, nil
}

type rawRule struct {
	PK                 string   `dynamodbav:"PK"`
	SK                 string   `dynamodbav:"SK"`
	TenantID           string   `dynamodbav:"tenant_id"`
	RuleID             string   `dynamodbav:"rule_id"`
	Name               string   `dynamodbav:"name"`
	PondID             string   `dynamodbav:"pond_id"`
	DeviceID           string   `dynamodbav:"device_id"`
	Metric             string   `dynamodbav:"metric"`
	Operator           string   `dynamodbav:"operator"`
	Threshold          float64  `dynamodbav:"threshold"`
	Aggregation        string   `dynamodbav:"aggregation"`
	Window             string   `dynamodbav:"window"`
	Duration           string   `dynamodbav:"duration"`
	Severity           string   `dynamodbav:"severity"`
	Channels           []string `dynamodbav:"channels"`
	CooldownSeconds    int64    `dynamodbav:"cooldown_seconds"`
	Version            int64    `dynamodbav:"version"`
	EvaluationRevision int64    `dynamodbav:"evaluation_revision"`
	NextEvaluationAt   string   `dynamodbav:"next_evaluation_at"`
	LeaseOwner         string   `dynamodbav:"lease_owner"`
	LeaseEpoch         int64    `dynamodbav:"lease_epoch"`
}

func workFromItem(item map[string]types.AttributeValue) (alertevaluator.Work, error) {
	var raw rawRule
	if err := attributevalue.UnmarshalMap(item, &raw); err != nil {
		return alertevaluator.Work{}, err
	}
	if err := validateRawRule(raw); err != nil {
		return alertevaluator.Work{}, err
	}
	window, err := time.ParseDuration(raw.Window)
	if err != nil {
		return alertevaluator.Work{}, fmt.Errorf("window: %w", err)
	}
	duration, err := time.ParseDuration(raw.Duration)
	if err != nil {
		return alertevaluator.Work{}, fmt.Errorf("duration: %w", err)
	}
	nextDue, err := time.Parse(time.RFC3339Nano, raw.NextEvaluationAt)
	if err != nil {
		return alertevaluator.Work{}, fmt.Errorf("next_evaluation_at: %w", err)
	}
	channels := make([]alertevaluator.Channel, 0, len(raw.Channels))
	for _, channel := range raw.Channels {
		channels = append(channels, alertevaluator.Channel(channel))
	}
	return alertevaluator.Work{
		PK: raw.PK, SK: raw.SK, NextEvaluationAt: nextDue.UTC(), LeaseOwner: raw.LeaseOwner, LeaseEpoch: raw.LeaseEpoch,
		Rule: alertevaluator.EvaluationRule{
			Rule: alertevaluator.Rule{
				TenantID: raw.TenantID, RuleID: raw.RuleID, Version: raw.Version,
				EvaluationRevision: raw.EvaluationRevision, Duration: duration,
				Cooldown: time.Duration(raw.CooldownSeconds) * time.Second, Channels: channels,
			},
			Name: raw.Name, PondID: raw.PondID, DeviceID: raw.DeviceID, Metric: raw.Metric,
			Operator: raw.Operator, Threshold: raw.Threshold, Aggregation: alertevaluator.Aggregation(raw.Aggregation),
			Window: window, Severity: raw.Severity,
		},
	}, nil
}

func validateRawRule(raw rawRule) error {
	if raw.PK == "" || raw.SK == "" || raw.TenantID == "" || raw.RuleID == "" || raw.PondID == "" {
		return fmt.Errorf("alert rule identity is incomplete")
	}
	if raw.Version < 1 || raw.EvaluationRevision < 1 {
		return fmt.Errorf("alert rule versions must be positive")
	}
	switch raw.Operator {
	case "<", "<=", ">", ">=":
	default:
		return fmt.Errorf("unsupported alert operator %q", raw.Operator)
	}
	switch alertevaluator.Aggregation(raw.Aggregation) {
	case alertevaluator.AggregationMin, alertevaluator.AggregationMax,
		alertevaluator.AggregationMean, alertevaluator.AggregationLast:
	default:
		return fmt.Errorf("unsupported alert aggregation %q", raw.Aggregation)
	}
	for _, channel := range raw.Channels {
		if channel != string(alertevaluator.ChannelEmail) && channel != string(alertevaluator.ChannelTelegram) {
			return fmt.Errorf("unsupported alert channel %q", channel)
		}
	}
	return nil
}

type tokenKey struct {
	PK     string `json:"pk"`
	SK     string `json:"sk"`
	GSI1PK string `json:"gsi1_pk,omitempty"`
	GSI1SK string `json:"gsi1_sk,omitempty"`
}

func encodeToken(key map[string]types.AttributeValue) (string, error) {
	var value struct {
		PK     string `dynamodbav:"PK"`
		SK     string `dynamodbav:"SK"`
		GSI1PK string `dynamodbav:"GSI1PK"`
		GSI1SK string `dynamodbav:"GSI1SK"`
	}
	if err := attributevalue.UnmarshalMap(key, &value); err != nil {
		return "", err
	}
	encoded, err := json.Marshal(tokenKey{
		PK: value.PK, SK: value.SK, GSI1PK: value.GSI1PK, GSI1SK: value.GSI1SK,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeToken(token string) (map[string]types.AttributeValue, error) {
	encoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	var value tokenKey
	if err := json.Unmarshal(encoded, &value); err != nil {
		return nil, err
	}
	if value.PK == "" || value.SK == "" {
		return nil, fmt.Errorf("pagination token is missing its key")
	}
	key := map[string]string{"PK": value.PK, "SK": value.SK}
	if value.GSI1PK != "" || value.GSI1SK != "" {
		if value.GSI1PK == "" || value.GSI1SK == "" {
			return nil, fmt.Errorf("pagination token has an incomplete index key")
		}
		key["GSI1PK"] = value.GSI1PK
		key["GSI1SK"] = value.GSI1SK
	}
	return attributevalue.MarshalMap(key)
}

func stateSortKey(ruleID string) string  { return "ALERT_STATE#" + ruleID }
func eventSortKey(eventID string) string { return "ALERT_EVENT#" + eventID }

func commitToken(request alertevaluator.CommitRequest) string {
	canonical := fmt.Sprintf(
		"%s\x00%s\x00%d\x00%d\x00%s\x00%s\x00%s\x00%s",
		request.Work.Rule.TenantID,
		request.Work.Rule.RuleID,
		request.Work.LeaseEpoch,
		request.PreviousState.Revision,
		alertevaluator.FixedUTCTimestamp(request.Slot),
		request.Evaluation.Quality,
		request.Decision.Transition,
		request.Decision.EventID+request.Decision.ResolvedEventID,
	)
	digest := sha256.Sum256([]byte(canonical))
	return "eval-" + hex.EncodeToString(digest[:])[:31]
}

func isConditional(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "ConditionalCheckFailedException" || apiErr.ErrorCode() == "TransactionCanceledException"
}
