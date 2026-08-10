package dynamo

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type relayItem struct {
	PK                    string                         `dynamodbav:"PK"`
	SK                    string                         `dynamodbav:"SK"`
	EntityType            string                         `dynamodbav:"entity_type"`
	TenantID              string                         `dynamodbav:"tenant_id"`
	OutboxID              string                         `dynamodbav:"outbox_id"`
	DeliveryID            string                         `dynamodbav:"delivery_id"`
	EventID               string                         `dynamodbav:"event_id"`
	RuleID                string                         `dynamodbav:"rule_id"`
	DependsOnOutboxID     string                         `dynamodbav:"depends_on_outbox_id"`
	Kind                  notifications.NotificationKind `dynamodbav:"kind"`
	Channel               notifications.Channel          `dynamodbav:"channel"`
	ExpansionStatus       string                         `dynamodbav:"expansion_status"`
	ExpansionRevision     int64                          `dynamodbav:"expansion_revision"`
	ExpansionCursor       string                         `dynamodbav:"expansion_cursor"`
	ExpansionStartedAt    string                         `dynamodbav:"expansion_started_at"`
	RecipientsExamined    int                            `dynamodbav:"recipients_examined"`
	DeliveriesCreated     int                            `dynamodbav:"deliveries_created"`
	DeliveriesCancelled   int                            `dynamodbav:"deliveries_cancelled"`
	RecipientsFiltered    int                            `dynamodbav:"recipients_filtered"`
	DeliveryState         string                         `dynamodbav:"state"`
	DeliveryRevision      int64                          `dynamodbav:"delivery_revision"`
	AvailableAt           string                         `dynamodbav:"available_at"`
	RelaySchemaVersion    int64                          `dynamodbav:"relay_schema_version"`
	RelayWorkKind         notifications.WorkKind         `dynamodbav:"relay_work_kind"`
	RelayPK               string                         `dynamodbav:"relay_gsi_pk"`
	RelaySK               string                         `dynamodbav:"relay_gsi_sk"`
	RelayLeaseOwner       string                         `dynamodbav:"relay_lease_owner"`
	RelayLeaseEpoch       int64                          `dynamodbav:"relay_lease_epoch"`
	Traceparent           string                         `dynamodbav:"traceparent"`
	EvaluationWindowStart string                         `dynamodbav:"evaluation_window_start"`
	EvaluationWindowEnd   string                         `dynamodbav:"evaluation_window_end"`
	EvaluatedAt           string                         `dynamodbav:"evaluated_at"`
	EvaluationValue       *float64                       `dynamodbav:"evaluation_value"`
}

func (store Store) Reload(
	ctx context.Context,
	candidate relay.Candidate,
	relayTime time.Time,
) (relay.Work, bool, error) {
	if store.Client == nil || strings.TrimSpace(store.Table) == "" {
		return relay.Work{}, false, fmt.Errorf("relay DynamoDB store is not configured")
	}
	key, err := attributevalue.MarshalMap(map[string]string{"PK": candidate.PK, "SK": candidate.SK})
	if err != nil {
		return relay.Work{}, false, fmt.Errorf("marshal relay base key: %w", err)
	}
	output, err := store.Client.GetItem(ctx, &awssdk.GetItemInput{
		TableName: aws.String(store.Table), Key: key, ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return relay.Work{}, false, fmt.Errorf("reload relay base item: %w", err)
	}
	if len(output.Item) == 0 {
		return relay.Work{}, false, nil
	}
	var item relayItem
	if err := attributevalue.UnmarshalMap(output.Item, &item); err != nil {
		return relay.Work{}, false, fmt.Errorf("decode relay base item: %w", err)
	}
	if item.PK != candidate.PK || item.SK != candidate.SK ||
		item.RelayPK != candidate.RelayPK || item.RelaySK != candidate.RelaySK {
		return relay.Work{}, false, nil
	}
	if item.RelaySchemaVersion != notifications.RelaySchemaVersion {
		return relay.Work{}, false, fmt.Errorf("unsupported relay schema version")
	}
	_, markerExists := output.Item["relay_work_kind"]
	if markerExists && item.RelayWorkKind != candidate.Kind {
		return relay.Work{}, false, fmt.Errorf(
			"relay base work kind %q does not match candidate %q",
			item.RelayWorkKind, candidate.Kind,
		)
	}
	availableAt, err := time.Parse(fixedUTCLayout, item.AvailableAt)
	if err != nil {
		return relay.Work{}, false, fmt.Errorf("decode relay available time: %w", err)
	}
	if availableAt.After(relayTime) || !availableAt.Equal(candidate.AvailableAt) {
		return relay.Work{}, false, nil
	}
	itemID := item.OutboxID
	state := item.ExpansionStatus
	revision := item.ExpansionRevision
	switch candidate.Kind {
	case notifications.WorkKindIntent:
		if item.EntityType != "notification_outbox" || item.Kind != notifications.NotificationKindOpening {
			return relay.Work{}, false, fmt.Errorf("intent base item has invalid type")
		}
		if item.PK != "TENANT#"+item.TenantID || item.SK != "NOTIFICATION_OUTBOX#"+item.OutboxID {
			return relay.Work{}, false, fmt.Errorf("intent base item has invalid storage key")
		}
		if state != "pending" {
			return relay.Work{}, false, nil
		}
	case notifications.WorkKindDependency:
		if item.EntityType != "notification_outbox" || item.Kind != notifications.NotificationKindRecovery {
			return relay.Work{}, false, fmt.Errorf("dependency base item has invalid type")
		}
		if item.PK != "TENANT#"+item.TenantID || item.SK != "NOTIFICATION_OUTBOX#"+item.OutboxID {
			return relay.Work{}, false, fmt.Errorf("dependency base item has invalid storage key")
		}
		if state != "pending" {
			return relay.Work{}, false, nil
		}
	case notifications.WorkKindDelivery:
		if item.EntityType != "notification_delivery" {
			return relay.Work{}, false, fmt.Errorf("delivery base item has invalid type")
		}
		if item.PK != "NOTIFICATION_OUTBOX#"+item.OutboxID || item.SK != "DELIVERY#"+item.DeliveryID {
			return relay.Work{}, false, fmt.Errorf("delivery base item has invalid storage key")
		}
		itemID = item.DeliveryID
		state = item.DeliveryState
		revision = item.DeliveryRevision
		if state != string(notifications.DeliveryStatePending) {
			return relay.Work{}, false, nil
		}
	default:
		return relay.Work{}, false, candidate.Kind.Validate()
	}
	if itemID == "" || item.TenantID == "" || item.OutboxID == "" || item.EventID == "" || item.RuleID == "" ||
		state == "" || item.Channel != notifications.ChannelEmail || item.Kind.Validate() != nil {
		return relay.Work{}, false, fmt.Errorf("relay base item is incomplete")
	}
	wantIndex, err := notifications.BuildRelayIndexKey(candidate.Kind, item.TenantID, itemID, availableAt)
	if err != nil {
		return relay.Work{}, false, fmt.Errorf("validate relay index: %w", err)
	}
	if wantIndex.PartitionKey != item.RelayPK || wantIndex.SortKey != item.RelaySK {
		return relay.Work{}, false, nil
	}
	var expansionStartedAt time.Time
	if item.ExpansionStartedAt != "" {
		expansionStartedAt, err = time.Parse(fixedUTCLayout, item.ExpansionStartedAt)
		if err != nil {
			return relay.Work{}, false, fmt.Errorf("decode expansion start time: %w", err)
		}
	}
	evaluation, err := evaluationSnapshotFromItem(output.Item, item)
	if err != nil {
		return relay.Work{}, false, err
	}
	return relay.Work{
		Candidate: candidate, TenantID: item.TenantID, ItemID: itemID,
		OutboxID: item.OutboxID, DeliveryID: item.DeliveryID, EventID: item.EventID, RuleID: item.RuleID,
		DependsOnOutboxID: item.DependsOnOutboxID, NotificationKind: item.Kind,
		Channel: item.Channel, State: state, Revision: revision, Cursor: item.ExpansionCursor,
		ExpansionStartedAt: expansionStartedAt, RecipientsExamined: item.RecipientsExamined,
		DeliveriesCreated: item.DeliveriesCreated, DeliveriesCancelled: item.DeliveriesCancelled,
		RecipientsFiltered: item.RecipientsFiltered, LeaseOwner: item.RelayLeaseOwner,
		LeaseEpoch: item.RelayLeaseEpoch, Traceparent: item.Traceparent,
		Evaluation: evaluation,
	}, true, nil
}

func evaluationSnapshotFromItem(
	attributes map[string]types.AttributeValue,
	item relayItem,
) (relay.EvaluationSnapshot, error) {
	_, hasStart := attributes["evaluation_window_start"]
	_, hasEnd := attributes["evaluation_window_end"]
	_, hasEvaluatedAt := attributes["evaluated_at"]
	_, hasValue := attributes["evaluation_value"]
	if !hasStart && !hasEnd && !hasEvaluatedAt && !hasValue {
		return relay.EvaluationSnapshot{}, nil
	}
	if !hasStart || !hasEnd || !hasEvaluatedAt {
		return relay.EvaluationSnapshot{}, fmt.Errorf("relay evaluation snapshot is incomplete")
	}
	windowStart, err := time.Parse(fixedUTCLayout, item.EvaluationWindowStart)
	if err != nil {
		return relay.EvaluationSnapshot{}, fmt.Errorf("decode relay evaluation window start: %w", err)
	}
	windowEnd, err := time.Parse(fixedUTCLayout, item.EvaluationWindowEnd)
	if err != nil {
		return relay.EvaluationSnapshot{}, fmt.Errorf("decode relay evaluation window end: %w", err)
	}
	evaluatedAt, err := time.Parse(fixedUTCLayout, item.EvaluatedAt)
	if err != nil {
		return relay.EvaluationSnapshot{}, fmt.Errorf("decode relay evaluation time: %w", err)
	}
	snapshot := relay.EvaluationSnapshot{
		WindowStart: windowStart, WindowEnd: windowEnd, EvaluatedAt: evaluatedAt,
		Value: item.EvaluationValue,
	}
	if err := snapshot.Validate(); err != nil {
		return relay.EvaluationSnapshot{}, fmt.Errorf("validate relay evaluation snapshot: %w", err)
	}
	return snapshot, nil
}
