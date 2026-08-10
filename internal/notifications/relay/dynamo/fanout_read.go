package dynamo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type eventSnapshot struct {
	PK                  string   `dynamodbav:"PK"`
	SK                  string   `dynamodbav:"SK"`
	EntityType          string   `dynamodbav:"entity_type"`
	TenantID            string   `dynamodbav:"tenant_id"`
	EventID             string   `dynamodbav:"event_id"`
	RuleID              string   `dynamodbav:"rule_id"`
	RuleName            string   `dynamodbav:"rule_name"`
	Severity            string   `dynamodbav:"severity"`
	PondID              string   `dynamodbav:"pond_id"`
	DeviceID            string   `dynamodbav:"device_id"`
	Metric              string   `dynamodbav:"metric"`
	Operator            string   `dynamodbav:"operator"`
	Threshold           float64  `dynamodbav:"threshold"`
	WindowStart         string   `dynamodbav:"window_start"`
	WindowEnd           string   `dynamodbav:"window_end"`
	LastEvaluatedAt     string   `dynamodbav:"last_evaluated_at"`
	LastEvaluationValue *float64 `dynamodbav:"last_evaluation_value"`
}

type memberSnapshot struct {
	PK          string `dynamodbav:"PK"`
	SK          string `dynamodbav:"SK"`
	EntityType  string `dynamodbav:"entity_type"`
	TenantID    string `dynamodbav:"tenant_id"`
	RecipientID string `dynamodbav:"cognito_sub"`
	Role        string `dynamodbav:"role"`
	Status      string `dynamodbav:"status"`
	Version     int64  `dynamodbav:"version"`
	CreatedAt   string `dynamodbav:"created_at"`
}

type preferenceSnapshot struct {
	PK              string `dynamodbav:"PK"`
	SK              string `dynamodbav:"SK"`
	EntityType      string `dynamodbav:"entity_type"`
	TenantID        string `dynamodbav:"tenant_id"`
	RecipientID     string `dynamodbav:"cognito_sub"`
	EmailEnabled    bool   `dynamodbav:"email_enabled"`
	EmailAddress    string `dynamodbav:"email_address"`
	EmailVerified   bool   `dynamodbav:"email_verified"`
	MinimumSeverity string `dynamodbav:"minimum_severity"`
}

type membershipPage struct {
	Members    []memberSnapshot
	NextCursor string
}

func (store Store) loadEvent(ctx context.Context, work relay.Work) (eventSnapshot, error) {
	item, err := store.getConsistent(ctx, "TENANT#"+work.TenantID, "ALERT_EVENT#"+work.EventID)
	if err != nil {
		return eventSnapshot{}, err
	}
	if len(item) == 0 {
		return eventSnapshot{}, fmt.Errorf("alert event is missing")
	}
	var event eventSnapshot
	if err := attributevalue.UnmarshalMap(item, &event); err != nil {
		return eventSnapshot{}, fmt.Errorf("decode alert event: %w", err)
	}
	if event.EntityType != "alert_event" || event.TenantID != work.TenantID ||
		event.EventID != work.EventID || event.RuleID != work.RuleID {
		return eventSnapshot{}, fmt.Errorf("alert event identity is invalid")
	}
	if _, err := severityRank(event.Severity); err != nil {
		return eventSnapshot{}, err
	}
	if _, err := metricUnit(event.Metric); err != nil {
		return eventSnapshot{}, err
	}
	return event, nil
}

func (store Store) queryMemberships(
	ctx context.Context,
	work relay.Work,
	pageSize int,
) (membershipPage, error) {
	values, err := attributevalue.MarshalMap(map[string]string{
		":pk": "TENANT#" + work.TenantID, ":member_prefix": "MEMBER#",
	})
	if err != nil {
		return membershipPage{}, err
	}
	input := &awssdk.QueryInput{
		TableName:                 aws.String(store.Table),
		KeyConditionExpression:    aws.String("#pk = :pk AND begins_with(#sk, :member_prefix)"),
		ExpressionAttributeNames:  map[string]string{"#pk": "PK", "#sk": "SK"},
		ExpressionAttributeValues: values, Limit: aws.Int32(int32(pageSize)),
		ConsistentRead: aws.Bool(true),
	}
	if work.Cursor != "" {
		input.ExclusiveStartKey, err = decodeBaseCursor(work.Cursor)
		if err != nil {
			return membershipPage{}, fmt.Errorf("decode expansion cursor: %w", err)
		}
	}
	output, err := store.Client.Query(ctx, input)
	if err != nil {
		return membershipPage{}, fmt.Errorf("query tenant memberships: %w", err)
	}
	page := membershipPage{Members: make([]memberSnapshot, 0, len(output.Items))}
	for _, item := range output.Items {
		var member memberSnapshot
		if err := attributevalue.UnmarshalMap(item, &member); err != nil {
			return membershipPage{}, fmt.Errorf("decode tenant membership: %w", err)
		}
		if member.EntityType != "tenant_member" || member.TenantID != work.TenantID ||
			member.PK != "TENANT#"+work.TenantID || member.SK != "MEMBER#"+member.RecipientID ||
			member.RecipientID == "" || member.Role == "" || member.Status == "" || member.Version < 1 {
			return membershipPage{}, fmt.Errorf("tenant membership is malformed")
		}
		if _, err := time.Parse(time.RFC3339Nano, member.CreatedAt); err != nil {
			return membershipPage{}, fmt.Errorf("tenant membership timestamp is malformed")
		}
		page.Members = append(page.Members, member)
	}
	if len(output.LastEvaluatedKey) != 0 {
		page.NextCursor, err = encodeBaseCursor(output.LastEvaluatedKey)
		if err != nil {
			return membershipPage{}, fmt.Errorf("encode expansion cursor: %w", err)
		}
	}
	return page, nil
}

func (store Store) loadPreference(
	ctx context.Context,
	tenantID string,
	recipientID string,
) (*preferenceSnapshot, error) {
	item, err := store.getConsistent(
		ctx, "TENANT#"+tenantID, "NOTIFICATION_PREFERENCE#USER#"+recipientID,
	)
	if err != nil || len(item) == 0 {
		return nil, err
	}
	var preference preferenceSnapshot
	if err := attributevalue.UnmarshalMap(item, &preference); err != nil {
		return nil, fmt.Errorf("decode notification preference: %w", err)
	}
	if preference.EntityType != "notification_preference" || preference.TenantID != tenantID ||
		preference.RecipientID != recipientID || preference.EmailAddress == "" ||
		!isASCII(preference.EmailAddress) {
		return nil, fmt.Errorf("notification preference is malformed")
	}
	if _, err := severityRank(preference.MinimumSeverity); err != nil {
		return nil, fmt.Errorf("notification preference severity is invalid")
	}
	return &preference, nil
}

func (store Store) addressSuppressed(ctx context.Context, address string) (bool, error) {
	key, err := notifications.DeliverabilityStorageKey(address)
	if err != nil {
		return false, err
	}
	item, err := store.getConsistent(ctx, key.PartitionKey, key.SortKey)
	if err != nil {
		return false, err
	}
	if len(item) == 0 {
		legacyKey, legacyErr := notifications.LegacyDeliverabilityStorageKey(address)
		if legacyErr != nil {
			return false, legacyErr
		}
		if legacyKey != key {
			item, err = store.getConsistent(ctx, legacyKey.PartitionKey, legacyKey.SortKey)
			if err != nil {
				return false, err
			}
		}
	}
	if len(item) == 0 {
		return false, nil
	}
	var record struct {
		Deliverability notifications.EmailDeliverability `dynamodbav:"deliverability"`
	}
	if err := attributevalue.UnmarshalMap(item, &record); err != nil {
		return false, fmt.Errorf("decode email deliverability: %w", err)
	}
	if err := record.Deliverability.Validate(); err != nil {
		return false, fmt.Errorf("email deliverability is invalid")
	}
	switch record.Deliverability {
	case notifications.EmailDeliverabilityUnknown, notifications.EmailDeliverabilityDeliverable:
		return false, nil
	case notifications.EmailDeliverabilitySuppressed:
		return true, nil
	}
	return false, fmt.Errorf("email deliverability is invalid")
}

func (store Store) getConsistent(
	ctx context.Context,
	pk string,
	sk string,
) (map[string]types.AttributeValue, error) {
	key, err := attributevalue.MarshalMap(map[string]string{"PK": pk, "SK": sk})
	if err != nil {
		return nil, err
	}
	output, err := store.Client.GetItem(ctx, &awssdk.GetItemInput{
		TableName: aws.String(store.Table), Key: key, ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("read relay dependency: %w", err)
	}
	return output.Item, nil
}

func (event eventSnapshot) templateData() (notifications.EmailTemplateData, error) {
	return event.templateDataFor(notifications.NotificationKindOpening, relay.EvaluationSnapshot{})
}

func (event eventSnapshot) templateDataFor(
	kind notifications.NotificationKind,
	snapshot relay.EvaluationSnapshot,
) (notifications.EmailTemplateData, error) {
	unit, err := metricUnit(event.Metric)
	if err != nil {
		return notifications.EmailTemplateData{}, err
	}
	windowStart, err := time.Parse(time.RFC3339Nano, event.WindowStart)
	if err != nil {
		return notifications.EmailTemplateData{}, fmt.Errorf("event window start is invalid")
	}
	windowEnd, err := time.Parse(time.RFC3339Nano, event.WindowEnd)
	if err != nil {
		return notifications.EmailTemplateData{}, fmt.Errorf("event window end is invalid")
	}
	var observedValue *float64
	evaluatedAt := windowEnd
	if snapshot.Present() {
		if err := snapshot.Validate(); err != nil {
			return notifications.EmailTemplateData{}, err
		}
		windowStart = snapshot.WindowStart
		windowEnd = snapshot.WindowEnd
		evaluatedAt = snapshot.EvaluatedAt
		observedValue = snapshot.Value
	} else {
		lastEvaluatedAt, parseErr := time.Parse(time.RFC3339Nano, event.LastEvaluatedAt)
		if parseErr != nil {
			return notifications.EmailTemplateData{}, fmt.Errorf("event evaluation time is invalid")
		}
		switch kind {
		case notifications.NotificationKindOpening:
			if lastEvaluatedAt.Equal(windowEnd) {
				observedValue = event.LastEvaluationValue
			}
		case notifications.NotificationKindRecovery:
			duration := windowEnd.Sub(windowStart)
			if duration <= 0 {
				return notifications.EmailTemplateData{}, fmt.Errorf("event evaluation window is invalid")
			}
			windowEnd = lastEvaluatedAt
			windowStart = lastEvaluatedAt.Add(-duration)
			evaluatedAt = lastEvaluatedAt
			observedValue = event.LastEvaluationValue
		default:
			return notifications.EmailTemplateData{}, kind.Validate()
		}
	}
	return notifications.EmailTemplateData{
		RuleName: normalizeLegacyRuleName(event.RuleName), Severity: event.Severity, TenantID: event.TenantID,
		PondID: event.PondID, DeviceID: event.DeviceID, Metric: event.Metric, Unit: unit,
		Operator: event.Operator, Threshold: event.Threshold, ObservedValue: observedValue,
		EvaluationWindow: windowEnd.Sub(windowStart), WindowStart: windowStart,
		WindowEnd: windowEnd, EvaluatedAt: evaluatedAt, EventID: event.EventID,
	}, nil
}

func normalizeLegacyRuleName(value string) string {
	value = strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, value)
	value = strings.Join(strings.Fields(value), " ")
	if value == "" {
		return "Regra de alerta"
	}
	return value
}

func metricUnit(metric string) (string, error) {
	units := map[string]string{
		"temp_c": "°C", "ph": "pH", "do_mg_l": "mg/L", "turbidity_ntu": "NTU",
		"salinity_ppt": "ppt", "battery_v": "V", "rssi": "dBm",
	}
	unit, ok := units[metric]
	if !ok {
		return "", fmt.Errorf("alert event metric is unsupported")
	}
	return unit, nil
}

func severityRank(severity string) (int, error) {
	switch severity {
	case "warning":
		return 1, nil
	case "critical":
		return 2, nil
	default:
		return 0, fmt.Errorf("alert severity is unsupported")
	}
}

func isASCII(value string) bool {
	for _, char := range value {
		if char > 127 {
			return false
		}
	}
	return true
}

type baseCursor struct {
	PK string `json:"pk"`
	SK string `json:"sk"`
}

func encodeBaseCursor(key map[string]types.AttributeValue) (string, error) {
	var decoded baseCursor
	if err := attributevalue.UnmarshalMap(key, &decoded); err != nil {
		return "", err
	}
	if decoded.PK == "" || decoded.SK == "" {
		return "", fmt.Errorf("base cursor is incomplete")
	}
	encoded, err := json.Marshal(decoded)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodeBaseCursor(cursor string) (map[string]types.AttributeValue, error) {
	encoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, err
	}
	var decoded baseCursor
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	if strings.TrimSpace(decoded.PK) == "" || strings.TrimSpace(decoded.SK) == "" {
		return nil, fmt.Errorf("base cursor is incomplete")
	}
	return attributevalue.MarshalMap(decoded)
}
