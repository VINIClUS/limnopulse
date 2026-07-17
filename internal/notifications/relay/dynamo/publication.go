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
)

func (store Store) MarkQueued(
	ctx context.Context,
	work relay.Work,
	result relay.QueuedResult,
) error {
	if work.Kind != notifications.WorkKindDelivery || work.State != "pending" ||
		work.LeaseOwner == "" || work.LeaseEpoch < 1 || work.Revision < 1 ||
		result.QueuedAt.IsZero() || strings.TrimSpace(result.MessageID) == "" {
		return fmt.Errorf("invalid queued delivery mutation")
	}
	key, err := attributevalue.MarshalMap(map[string]string{"PK": work.PK, "SK": work.SK})
	if err != nil {
		return err
	}
	values, err := attributevalue.MarshalMap(map[string]any{
		":pending": work.State, ":queued": "queued", ":revision": work.Revision,
		":next_revision": work.Revision + 1, ":lease_owner": work.LeaseOwner,
		":lease_epoch": work.LeaseEpoch, ":relay_pk": work.RelayPK, ":relay_sk": work.RelaySK,
		":queued_at": result.QueuedAt.UTC().Format(fixedUTCLayout), ":message_id": result.MessageID,
	})
	if err != nil {
		return err
	}
	names := map[string]string{
		"#state": "state", "#revision": "delivery_revision",
		"#lease_owner": "relay_lease_owner", "#lease_epoch": "relay_lease_epoch",
		"#lease_expires": "relay_lease_expires_at", "#relay_pk": "relay_gsi_pk",
		"#relay_sk": "relay_gsi_sk", "#available_at": "available_at",
		"#queued_at": "queued_at", "#message_id": "sqs_message_id",
	}
	_, err = store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName: aws.String(store.Table), Key: key,
		UpdateExpression: aws.String(
			"SET #state = :queued, #queued_at = :queued_at, #message_id = :message_id, " +
				"#revision = :next_revision REMOVE #relay_pk, #relay_sk, #available_at, " +
				"#lease_owner, #lease_epoch, #lease_expires",
		),
		ConditionExpression: aws.String(
			"#state = :pending AND #revision = :revision AND #lease_owner = :lease_owner " +
				"AND #lease_epoch = :lease_epoch AND #relay_pk = :relay_pk AND #relay_sk = :relay_sk",
		),
		ExpressionAttributeNames: names, ExpressionAttributeValues: values,
	})
	if err != nil {
		return fmt.Errorf("mark notification delivery queued: %w", err)
	}
	return nil
}

func (store Store) Reschedule(ctx context.Context, work relay.Work, next time.Time) error {
	if next.IsZero() || work.LeaseOwner == "" || work.LeaseEpoch < 1 || work.ItemID == "" {
		return fmt.Errorf("invalid relay reschedule mutation")
	}
	stateName, revisionName, cursorName, err := mutationFieldNames(work.Kind)
	if err != nil {
		return err
	}
	index, err := notifications.BuildRelayIndexKey(work.Kind, work.TenantID, work.ItemID, next)
	if err != nil {
		return err
	}
	key, err := attributevalue.MarshalMap(map[string]string{"PK": work.PK, "SK": work.SK})
	if err != nil {
		return err
	}
	valueMap := map[string]any{
		":state": work.State, ":revision": work.Revision, ":next_revision": work.Revision + 1,
		":lease_owner": work.LeaseOwner, ":lease_epoch": work.LeaseEpoch,
		":relay_pk": work.RelayPK, ":relay_sk": work.RelaySK,
		":next_available_at": next.UTC().Format(fixedUTCLayout),
		":next_relay_pk":     index.PartitionKey, ":next_relay_sk": index.SortKey,
	}
	names := map[string]string{
		"#state": stateName, "#revision": revisionName,
		"#lease_owner": "relay_lease_owner", "#lease_epoch": "relay_lease_epoch",
		"#lease_expires": "relay_lease_expires_at", "#relay_pk": "relay_gsi_pk",
		"#relay_sk": "relay_gsi_sk", "#available_at": "available_at",
	}
	condition := "#state = :state "
	if work.Revision == 0 {
		condition += "AND (attribute_not_exists(#revision) OR #revision = :revision) "
	} else {
		condition += "AND #revision = :revision "
	}
	condition += "AND #lease_owner = :lease_owner AND #lease_epoch = :lease_epoch " +
		"AND #relay_pk = :relay_pk AND #relay_sk = :relay_sk"
	if cursorName != "" {
		names["#cursor"] = cursorName
		valueMap[":cursor"] = work.Cursor
		if work.Cursor == "" {
			condition += " AND (attribute_not_exists(#cursor) OR #cursor = :cursor)"
		} else {
			condition += " AND #cursor = :cursor"
		}
	}
	values, err := attributevalue.MarshalMap(valueMap)
	if err != nil {
		return err
	}
	_, err = store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName: aws.String(store.Table), Key: key,
		UpdateExpression: aws.String(
			"SET #available_at = :next_available_at, #relay_pk = :next_relay_pk, " +
				"#relay_sk = :next_relay_sk, #revision = :next_revision " +
				"REMOVE #lease_owner, #lease_epoch, #lease_expires",
		),
		ConditionExpression: aws.String(condition), ExpressionAttributeNames: names,
		ExpressionAttributeValues: values,
	})
	if err != nil {
		return fmt.Errorf("reschedule notification relay work: %w", err)
	}
	return nil
}
