package dynamo

import (
	"context"
	"fmt"
	"strings"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (store Store) CheckGates(ctx context.Context, record worker.DeliveryRecord) (worker.GateResult, error) {
	snapshot := record.Delivery
	membershipItem, err := store.getConsistent(ctx, "TENANT#"+snapshot.TenantID, "MEMBER#"+snapshot.RecipientID)
	if err != nil {
		return worker.GateResult{}, err
	}
	if len(membershipItem) == 0 {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonRecipientMembershipInactive}, nil
	}
	var membership struct {
		PK          string `dynamodbav:"PK"`
		SK          string `dynamodbav:"SK"`
		EntityType  string `dynamodbav:"entity_type"`
		TenantID    string `dynamodbav:"tenant_id"`
		RecipientID string `dynamodbav:"cognito_sub"`
		Role        string `dynamodbav:"role"`
		Status      string `dynamodbav:"status"`
		Version     int64  `dynamodbav:"version"`
	}
	if err := attributevalue.UnmarshalMap(membershipItem, &membership); err != nil {
		return worker.GateResult{}, fmt.Errorf("decode current membership: %w", err)
	}
	if membership.PK != "TENANT#"+snapshot.TenantID || membership.SK != "MEMBER#"+snapshot.RecipientID ||
		membership.EntityType != "tenant_member" || membership.TenantID != snapshot.TenantID ||
		membership.RecipientID != snapshot.RecipientID || membership.Role == "" || membership.Version < 1 || membership.Status == "" {
		return worker.GateResult{}, fmt.Errorf("current membership is malformed")
	}
	if membership.Status != "active" {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonRecipientMembershipInactive}, nil
	}
	preferenceItem, err := store.getConsistent(
		ctx, "TENANT#"+snapshot.TenantID, "NOTIFICATION_PREFERENCE#USER#"+snapshot.RecipientID,
	)
	if err != nil {
		return worker.GateResult{}, err
	}
	if len(preferenceItem) == 0 {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonCancelled}, nil
	}
	var preference struct {
		PK              string `dynamodbav:"PK"`
		SK              string `dynamodbav:"SK"`
		EntityType      string `dynamodbav:"entity_type"`
		TenantID        string `dynamodbav:"tenant_id"`
		RecipientID     string `dynamodbav:"cognito_sub"`
		Version         int64  `dynamodbav:"version"`
		EmailEnabled    bool   `dynamodbav:"email_enabled"`
		EmailAddress    string `dynamodbav:"email_address"`
		EmailVerified   bool   `dynamodbav:"email_verified"`
		MinimumSeverity string `dynamodbav:"minimum_severity"`
	}
	if err := attributevalue.UnmarshalMap(preferenceItem, &preference); err != nil {
		return worker.GateResult{}, fmt.Errorf("decode current notification preference: %w", err)
	}
	if preference.PK != "TENANT#"+snapshot.TenantID ||
		preference.SK != "NOTIFICATION_PREFERENCE#USER#"+snapshot.RecipientID ||
		preference.EntityType != "notification_preference" || preference.TenantID != snapshot.TenantID ||
		preference.RecipientID != snapshot.RecipientID || preference.Version < 1 ||
		preference.EmailAddress == "" || !isASCII(preference.EmailAddress) {
		return worker.GateResult{}, fmt.Errorf("current notification preference is malformed")
	}
	minimumSeverity, err := severityRank(preference.MinimumSeverity)
	if err != nil {
		return worker.GateResult{}, fmt.Errorf("current notification preference severity is invalid: %w", err)
	}
	if !preference.EmailEnabled || !preference.EmailVerified ||
		!strings.EqualFold(preference.EmailAddress, snapshot.NormalizedEmail) {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonCancelled}, nil
	}
	eventItem, err := store.getConsistent(ctx, "TENANT#"+snapshot.TenantID, "ALERT_EVENT#"+snapshot.EventID)
	if err != nil {
		return worker.GateResult{}, err
	}
	if len(eventItem) == 0 {
		return worker.GateResult{}, fmt.Errorf("current alert event is missing")
	}
	var event struct {
		PK         string `dynamodbav:"PK"`
		SK         string `dynamodbav:"SK"`
		EntityType string `dynamodbav:"entity_type"`
		TenantID   string `dynamodbav:"tenant_id"`
		EventID    string `dynamodbav:"event_id"`
		RuleID     string `dynamodbav:"rule_id"`
		Severity   string `dynamodbav:"severity"`
	}
	if err := attributevalue.UnmarshalMap(eventItem, &event); err != nil {
		return worker.GateResult{}, fmt.Errorf("decode current alert event: %w", err)
	}
	if event.PK != "TENANT#"+snapshot.TenantID || event.SK != "ALERT_EVENT#"+snapshot.EventID ||
		event.EntityType != "alert_event" || event.TenantID != snapshot.TenantID ||
		event.EventID != snapshot.EventID || event.RuleID != snapshot.RuleID {
		return worker.GateResult{}, fmt.Errorf("current alert event is malformed")
	}
	eventSeverity, err := severityRank(event.Severity)
	if err != nil {
		return worker.GateResult{}, fmt.Errorf("current alert event severity is invalid: %w", err)
	}
	if eventSeverity < minimumSeverity {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonCancelled}, nil
	}
	deliverabilityReads, err := store.currentDeliverability(ctx, snapshot.NormalizedEmail)
	if err != nil {
		return worker.GateResult{}, err
	}
	fence := worker.GateFence{
		MembershipVersion:          membership.Version,
		PreferenceVersion:          preference.Version,
		PreferenceEmailAddress:     preference.EmailAddress,
		PreferenceMinimumSeverity:  preference.MinimumSeverity,
		EventSeverity:              event.Severity,
		DeliverabilityDependencies: make([]worker.DeliverabilityDependency, 0, len(deliverabilityReads)),
	}
	for _, read := range deliverabilityReads {
		dependency := worker.DeliverabilityDependency{Key: read.Key}
		if len(read.Item) == 0 {
			fence.DeliverabilityDependencies = append(fence.DeliverabilityDependencies, dependency)
			continue
		}
		var deliverability struct {
			State notifications.EmailDeliverability `dynamodbav:"deliverability"`
		}
		if err := attributevalue.UnmarshalMap(read.Item, &deliverability); err != nil {
			return worker.GateResult{}, fmt.Errorf("decode current deliverability: %w", err)
		}
		if err := deliverability.State.Validate(); err != nil {
			return worker.GateResult{}, fmt.Errorf("current deliverability is malformed")
		}
		switch deliverability.State {
		case notifications.EmailDeliverabilityUnknown, notifications.EmailDeliverabilityDeliverable:
			dependency.Exists = true
			dependency.State = deliverability.State
			fence.DeliverabilityDependencies = append(fence.DeliverabilityDependencies, dependency)
		case notifications.EmailDeliverabilitySuppressed:
			return worker.GateResult{CancellationReason: notifications.CancellationReasonEmailSuppressed}, nil
		default:
			return worker.GateResult{}, fmt.Errorf("current deliverability is malformed")
		}
	}
	if !fence.IsComplete() {
		return worker.GateResult{}, fmt.Errorf("current gate fence is malformed")
	}
	return worker.GateResult{Allowed: true, Fence: fence}, nil
}

type deliverabilityRead struct {
	Key  notifications.StorageKey
	Item map[string]types.AttributeValue
}

func (store Store) currentDeliverability(
	ctx context.Context,
	email string,
) ([]deliverabilityRead, error) {
	deliverabilityKey, err := notifications.DeliverabilityStorageKey(email)
	if err != nil {
		return nil, err
	}
	deliverabilityItem, err := store.getConsistent(ctx, deliverabilityKey.PartitionKey, deliverabilityKey.SortKey)
	if err != nil {
		return nil, err
	}
	if len(deliverabilityItem) != 0 {
		return []deliverabilityRead{{Key: deliverabilityKey, Item: deliverabilityItem}}, nil
	}
	legacyKey, err := notifications.LegacyDeliverabilityStorageKey(email)
	if err != nil {
		return nil, err
	}
	if legacyKey == deliverabilityKey {
		return []deliverabilityRead{{Key: deliverabilityKey}}, nil
	}
	legacyItem, err := store.getConsistent(ctx, legacyKey.PartitionKey, legacyKey.SortKey)
	if err != nil {
		return nil, err
	}
	return []deliverabilityRead{
		{Key: deliverabilityKey},
		{Key: legacyKey, Item: legacyItem},
	}, nil
}

func severityRank(value string) (int, error) {
	switch value {
	case "warning":
		return 1, nil
	case "critical":
		return 2, nil
	}
	return 0, fmt.Errorf("alert severity is unsupported")
}

func isASCII(value string) bool {
	return !strings.ContainsFunc(value, func(char rune) bool { return char > 127 })
}
