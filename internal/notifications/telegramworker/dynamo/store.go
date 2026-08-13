package dynamo

import (
	"context"
	"fmt"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	workerdynamo "github.com/VINIClUS/limnopulse/internal/notifications/worker/dynamo"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Store struct {
	workerdynamo.Store
}

func New(table string, client workerdynamo.Client) Store {
	return Store{Store: workerdynamo.Store{Table: table, Client: client}}
}

func (store Store) CheckGates(
	ctx context.Context,
	record worker.DeliveryRecord,
) (worker.GateResult, error) {
	snapshot := record.Delivery
	if snapshot.Channel != notifications.ChannelTelegram || snapshot.DestinationID == "" ||
		snapshot.TelegramChatID <= 0 {
		return worker.GateResult{}, fmt.Errorf("Telegram delivery snapshot is invalid")
	}
	membershipItem, err := store.get(ctx, "TENANT#"+snapshot.TenantID, "MEMBER#"+snapshot.RecipientID)
	if err != nil {
		return worker.GateResult{}, err
	}
	var membership struct {
		EntityType  string `dynamodbav:"entity_type"`
		TenantID    string `dynamodbav:"tenant_id"`
		RecipientID string `dynamodbav:"cognito_sub"`
		Status      string `dynamodbav:"status"`
		Version     int64  `dynamodbav:"version"`
	}
	if len(membershipItem) == 0 {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonRecipientMembershipInactive}, nil
	}
	if err := attributevalue.UnmarshalMap(membershipItem, &membership); err != nil ||
		membership.EntityType != "tenant_member" || membership.TenantID != snapshot.TenantID ||
		membership.RecipientID != snapshot.RecipientID || membership.Version < 1 {
		return worker.GateResult{}, fmt.Errorf("current membership is malformed")
	}
	if membership.Status != "active" {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonRecipientMembershipInactive}, nil
	}
	preferenceItem, err := store.get(ctx, "TENANT#"+snapshot.TenantID, "NOTIFICATION_PREFERENCE#USER#"+snapshot.RecipientID)
	if err != nil {
		return worker.GateResult{}, err
	}
	if len(preferenceItem) == 0 {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonCancelled}, nil
	}
	var preference struct {
		EntityType      string `dynamodbav:"entity_type"`
		TenantID        string `dynamodbav:"tenant_id"`
		RecipientID     string `dynamodbav:"cognito_sub"`
		TelegramEnabled bool   `dynamodbav:"telegram_enabled"`
		MinimumSeverity string `dynamodbav:"minimum_severity"`
		Version         int64  `dynamodbav:"version"`
	}
	if err := attributevalue.UnmarshalMap(preferenceItem, &preference); err != nil ||
		preference.EntityType != "notification_preference" || preference.TenantID != snapshot.TenantID ||
		preference.RecipientID != snapshot.RecipientID || preference.Version < 1 {
		return worker.GateResult{}, fmt.Errorf("current notification preference is malformed")
	}
	if !preference.TelegramEnabled {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonCancelled}, nil
	}
	bindingItem, err := store.get(ctx, "TENANT#"+snapshot.TenantID, "TELEGRAM_BINDING#USER#"+snapshot.RecipientID)
	if err != nil {
		return worker.GateResult{}, err
	}
	if len(bindingItem) == 0 {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonCancelled}, nil
	}
	var binding struct {
		EntityType    string `dynamodbav:"entity_type"`
		TenantID      string `dynamodbav:"tenant_id"`
		RecipientID   string `dynamodbav:"recipient_id"`
		DestinationID string `dynamodbav:"destination_id"`
		Status        string `dynamodbav:"status"`
		Version       int64  `dynamodbav:"version"`
	}
	if err := attributevalue.UnmarshalMap(bindingItem, &binding); err != nil ||
		binding.EntityType != "telegram_binding" || binding.TenantID != snapshot.TenantID ||
		binding.RecipientID != snapshot.RecipientID || binding.Version < 1 {
		return worker.GateResult{}, fmt.Errorf("current Telegram binding is malformed")
	}
	if binding.Status != "verified" || binding.DestinationID != snapshot.DestinationID {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonCancelled}, nil
	}
	destinationItem, err := store.get(ctx, "TELEGRAM_DESTINATION#"+snapshot.DestinationID, "META")
	if err != nil {
		return worker.GateResult{}, err
	}
	if len(destinationItem) == 0 {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonTelegramDestinationSuppressed}, nil
	}
	var destination struct {
		EntityType    string `dynamodbav:"entity_type"`
		DestinationID string `dynamodbav:"destination_id"`
		RecipientID   string `dynamodbav:"recipient_id"`
		ChatID        int64  `dynamodbav:"chat_id"`
		Status        string `dynamodbav:"status"`
		Version       int64  `dynamodbav:"version"`
	}
	if err := attributevalue.UnmarshalMap(destinationItem, &destination); err != nil ||
		destination.EntityType != "telegram_destination" || destination.DestinationID != snapshot.DestinationID ||
		destination.RecipientID != snapshot.RecipientID || destination.ChatID != snapshot.TelegramChatID ||
		destination.Version < 1 {
		return worker.GateResult{}, fmt.Errorf("current Telegram destination is malformed")
	}
	if destination.Status != "active" {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonTelegramDestinationSuppressed}, nil
	}
	eventItem, err := store.get(ctx, "TENANT#"+snapshot.TenantID, "ALERT_EVENT#"+snapshot.EventID)
	if err != nil {
		return worker.GateResult{}, err
	}
	var event struct {
		PK         string `dynamodbav:"PK"`
		SK         string `dynamodbav:"SK"`
		EntityType string `dynamodbav:"entity_type"`
		TenantID   string `dynamodbav:"tenant_id"`
		EventID    string `dynamodbav:"event_id"`
		RuleID     string `dynamodbav:"rule_id"`
		Severity   string `dynamodbav:"severity"`
		Status     string `dynamodbav:"status"`
	}
	if err := attributevalue.UnmarshalMap(eventItem, &event); err != nil ||
		event.PK != "TENANT#"+snapshot.TenantID || event.SK != "ALERT_EVENT#"+snapshot.EventID ||
		event.EntityType != "alert_event" || event.TenantID != snapshot.TenantID ||
		event.EventID != snapshot.EventID || event.RuleID != snapshot.RuleID {
		return worker.GateResult{}, fmt.Errorf("current alert event is malformed")
	}
	minimumRank, ok := severityRank(preference.MinimumSeverity)
	if !ok {
		return worker.GateResult{}, fmt.Errorf("current minimum severity is malformed")
	}
	eventRank, ok := severityRank(event.Severity)
	if !ok {
		return worker.GateResult{}, fmt.Errorf("current event severity is malformed")
	}
	if eventRank < minimumRank {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonCancelled}, nil
	}
	validEventState := (snapshot.Kind == notifications.NotificationKindOpening &&
		(event.Status == "open" || event.Status == "acknowledged")) ||
		(snapshot.Kind == notifications.NotificationKindRecovery && event.Status == "resolved")
	if !validEventState {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonCancelled}, nil
	}
	return worker.GateResult{Allowed: true, Fence: worker.GateFence{
		Channel:           notifications.ChannelTelegram,
		MembershipVersion: membership.Version, PreferenceVersion: preference.Version,
		PreferenceMinimumSeverity: preference.MinimumSeverity, EventSeverity: event.Severity,
		EventStatus:           event.Status,
		TelegramDestinationID: destination.DestinationID, TelegramChatID: destination.ChatID,
		TelegramBindingVersion: binding.Version, TelegramDestinationVersion: destination.Version,
	}}, nil
}

func (store Store) get(ctx context.Context, pk, sk string) (map[string]types.AttributeValue, error) {
	key, err := attributevalue.MarshalMap(map[string]string{"PK": pk, "SK": sk})
	if err != nil {
		return nil, err
	}
	output, err := store.Client.GetItem(ctx, &awssdk.GetItemInput{
		TableName: &store.Table, Key: key, ConsistentRead: boolPtr(true),
	})
	if err != nil {
		return nil, err
	}
	return output.Item, nil
}

func severityRank(value string) (int, bool) {
	switch value {
	case "warning":
		return 1, true
	case "critical":
		return 2, true
	}
	return 0, false
}

func boolPtr(value bool) *bool { return &value }
