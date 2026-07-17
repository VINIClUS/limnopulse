package dynamo

import (
	"context"
	"fmt"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
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
		membership.EntityType != "membership" || membership.TenantID != snapshot.TenantID ||
		membership.RecipientID != snapshot.RecipientID || membership.Role == "" || membership.Version < 1 || membership.Status == "" {
		return worker.GateResult{}, fmt.Errorf("current membership is malformed")
	}
	if membership.Status != "active" {
		return worker.GateResult{CancellationReason: notifications.CancellationReasonRecipientMembershipInactive}, nil
	}
	deliverabilityKey, err := notifications.DeliverabilityStorageKey(snapshot.NormalizedEmail)
	if err != nil {
		return worker.GateResult{}, err
	}
	deliverabilityItem, err := store.getConsistent(ctx, deliverabilityKey.PartitionKey, deliverabilityKey.SortKey)
	if err != nil {
		return worker.GateResult{}, err
	}
	if len(deliverabilityItem) == 0 {
		return worker.GateResult{Allowed: true}, nil
	}
	var deliverability struct {
		State notifications.EmailDeliverability `dynamodbav:"deliverability"`
	}
	if err := attributevalue.UnmarshalMap(deliverabilityItem, &deliverability); err != nil {
		return worker.GateResult{}, fmt.Errorf("decode current deliverability: %w", err)
	}
	if err := deliverability.State.Validate(); err != nil {
		return worker.GateResult{}, fmt.Errorf("current deliverability is malformed")
	}
	switch deliverability.State {
	case notifications.EmailDeliverabilityUnknown, notifications.EmailDeliverabilityDeliverable:
		return worker.GateResult{Allowed: true}, nil
	case notifications.EmailDeliverabilitySuppressed:
		return worker.GateResult{CancellationReason: notifications.CancellationReasonEmailSuppressed}, nil
	}
	return worker.GateResult{}, fmt.Errorf("current deliverability is malformed")
}
