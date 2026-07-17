package dynamo

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/worker"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (store Store) Acquire(
	ctx context.Context,
	job notifications.JobEnvelope,
	request worker.ClaimRequest,
) (worker.AcquireResult, error) {
	if store.Client == nil || store.Table == "" || job.Validate() != nil || request.Owner == "" ||
		request.Now.IsZero() || !request.ExpiresAt.After(request.Now) {
		return worker.AcquireResult{}, fmt.Errorf("invalid worker claim request")
	}
	key, err := notifications.DeliveryStorageKey(job.OutboxID, job.DeliveryID)
	if err != nil {
		return worker.AcquireResult{}, err
	}
	item, err := store.getConsistent(ctx, key.PartitionKey, key.SortKey)
	if err != nil {
		return worker.AcquireResult{}, err
	}
	if len(item) == 0 {
		return worker.AcquireResult{Disposition: worker.AcquireAwaitDLQ}, nil
	}
	stored, record, err := decodeDelivery(item)
	if err != nil {
		return worker.AcquireResult{Disposition: worker.AcquireAwaitDLQ}, nil
	}
	if record.Delivery.TenantID != job.TenantID || record.Delivery.OutboxID != job.OutboxID ||
		record.Delivery.DeliveryID != job.DeliveryID || record.Delivery.EventID != job.EventID ||
		record.Delivery.Kind != job.Kind || record.Delivery.Channel != job.Channel {
		return worker.AcquireResult{Disposition: worker.AcquireAwaitDLQ}, nil
	}
	if record.AwaitingDLQ {
		return worker.AcquireResult{Disposition: worker.AcquireAwaitDLQ}, nil
	}
	switch record.Delivery.State {
	case notifications.DeliveryStateSucceeded, notifications.DeliveryStatePermanentFailed,
		notifications.DeliveryStateCancelled, notifications.DeliveryStateUnknown:
		return worker.AcquireResult{Disposition: worker.AcquireTerminal}, nil
	case notifications.DeliveryStatePending:
		if request.SQSMessageID == "" {
			return worker.AcquireResult{Disposition: worker.AcquireAwaitDLQ}, nil
		}
		if stored.RelayLeaseExpiresAt != "" {
			expiresAt, parseErr := parseRequiredTime("relay lease expiry", stored.RelayLeaseExpiresAt)
			if parseErr != nil {
				return worker.AcquireResult{Disposition: worker.AcquireAwaitDLQ}, nil
			}
			if expiresAt.After(request.Now) {
				return worker.AcquireResult{Disposition: worker.AcquireDeferred, RetryAfter: expiresAt.Sub(request.Now)}, nil
			}
		}
		stored, record, err = store.repairPublication(ctx, stored, record, request.Now, request.SQSMessageID)
		if err != nil {
			if isConditional(err) {
				return worker.AcquireResult{Disposition: worker.AcquireDeferred, RetryAfter: time.Second}, nil
			}
			return worker.AcquireResult{}, err
		}
	case notifications.DeliveryStateRetryableFailed:
		if !record.NextAttemptAt.IsZero() && record.NextAttemptAt.After(request.Now) {
			return worker.AcquireResult{Disposition: worker.AcquireDeferred, RetryAfter: record.NextAttemptAt.Sub(request.Now)}, nil
		}
		if record.AwaitingIntervention {
			return worker.AcquireResult{Disposition: worker.AcquireDeferred, RetryAfter: 15 * time.Minute}, nil
		}
	case notifications.DeliveryStateProcessing:
		if record.LeaseExpiresAt.IsZero() {
			return worker.AcquireResult{Disposition: worker.AcquireAwaitDLQ}, nil
		}
		if record.LeaseExpiresAt.After(request.Now) {
			return worker.AcquireResult{Disposition: worker.AcquireDeferred, RetryAfter: record.LeaseExpiresAt.Sub(request.Now)}, nil
		}
		if record.LastAttemptID != "" {
			outcome, loadErr := store.loadAttemptOutcome(ctx, record.Delivery.DeliveryID, record.LastAttemptID)
			if loadErr != nil {
				return worker.AcquireResult{}, loadErr
			}
			switch outcome {
			case notifications.AttemptOutcomeStarted:
				record.StartedAttemptID = record.LastAttemptID
			case notifications.AttemptOutcomeRetryable, notifications.AttemptOutcomeAmbiguous:
				// The previous attempt was completed before this claim. A crash between
				// Claim and BeginAttempt may therefore reclaim without another grace.
			default:
				return worker.AcquireResult{Disposition: worker.AcquireAwaitDLQ}, nil
			}
		}
	case notifications.DeliveryStateQueued:
	default:
		return worker.AcquireResult{Disposition: worker.AcquireAwaitDLQ}, nil
	}
	claimed, err := store.claim(ctx, stored, record, request)
	if err != nil {
		if isConditional(err) {
			return worker.AcquireResult{Disposition: worker.AcquireDeferred, RetryAfter: time.Second}, nil
		}
		return worker.AcquireResult{}, err
	}
	claimed.StartedAttemptID = record.StartedAttemptID
	return worker.AcquireResult{Disposition: worker.AcquireClaimed, Record: claimed}, nil
}

func (store Store) repairPublication(
	ctx context.Context,
	stored deliveryItem,
	record worker.DeliveryRecord,
	now time.Time,
	sqsMessageID string,
) (deliveryItem, worker.DeliveryRecord, error) {
	if err := validateTransition(record.Delivery, notifications.DeliveryStateQueued); err != nil {
		return deliveryItem{}, worker.DeliveryRecord{}, err
	}
	key, _ := attributevalue.MarshalMap(map[string]string{"PK": stored.PK, "SK": stored.SK})
	values, _ := attributevalue.MarshalMap(map[string]any{
		":pending": string(notifications.DeliveryStatePending), ":queued": string(notifications.DeliveryStateQueued),
		":revision": record.Revision, ":next_revision": record.Revision + 1, ":now": fixedTime(now),
		":tenant_id": record.Delivery.TenantID, ":outbox_id": record.Delivery.OutboxID,
		":delivery_id": record.Delivery.DeliveryID, ":event_id": record.Delivery.EventID,
		":sqs_message_id": sqsMessageID,
	})
	output, err := store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName: aws.String(store.Table), Key: key, ReturnValues: types.ReturnValueAllNew,
		UpdateExpression:    aws.String("SET #state = :queued, #queued_at = if_not_exists(#queued_at, :now), #message_id = if_not_exists(#message_id, :sqs_message_id), #publication_repaired_at = :now, #updated_at = :now, #revision = :next_revision REMOVE #relay_pk, #relay_sk, #available_at, #relay_lease_owner, #relay_lease_epoch, #relay_lease_expires"),
		ConditionExpression: aws.String("#state = :pending AND #revision = :revision AND #tenant_id = :tenant_id AND #outbox_id = :outbox_id AND #delivery_id = :delivery_id AND #event_id = :event_id AND (attribute_not_exists(#relay_lease_expires) OR #relay_lease_expires <= :now)"),
		ExpressionAttributeNames: map[string]string{
			"#state": "state", "#revision": "delivery_revision", "#queued_at": "queued_at",
			"#message_id":              "sqs_message_id",
			"#publication_repaired_at": "publication_repaired_at", "#updated_at": "updated_at",
			"#relay_pk": "relay_gsi_pk", "#relay_sk": "relay_gsi_sk", "#available_at": "available_at",
			"#relay_lease_owner": "relay_lease_owner", "#relay_lease_epoch": "relay_lease_epoch",
			"#relay_lease_expires": "relay_lease_expires_at", "#tenant_id": "tenant_id",
			"#outbox_id": "outbox_id", "#delivery_id": "delivery_id", "#event_id": "event_id",
		}, ExpressionAttributeValues: values,
	})
	if err != nil {
		return deliveryItem{}, worker.DeliveryRecord{}, fmt.Errorf("repair notification publication: %w", err)
	}
	return decodeDelivery(output.Attributes)
}

func (store Store) claim(
	ctx context.Context,
	stored deliveryItem,
	record worker.DeliveryRecord,
	request worker.ClaimRequest,
) (worker.DeliveryRecord, error) {
	if err := validateTransition(record.Delivery, notifications.DeliveryStateProcessing); err != nil {
		return worker.DeliveryRecord{}, err
	}
	key, _ := attributevalue.MarshalMap(map[string]string{"PK": stored.PK, "SK": stored.SK})
	valuesMap := map[string]any{
		":state": string(record.Delivery.State), ":processing": string(notifications.DeliveryStateProcessing),
		":revision": record.Revision, ":next_revision": record.Revision + 1,
		":owner": request.Owner, ":epoch": record.LeaseEpoch + 1,
		":expires": fixedTime(request.ExpiresAt), ":now": fixedTime(request.Now),
	}
	condition := "#state = :state AND #revision = :revision"
	if record.Delivery.State == notifications.DeliveryStateProcessing {
		condition += " AND #lease_expires <= :now"
	}
	values, _ := attributevalue.MarshalMap(valuesMap)
	output, err := store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName: aws.String(store.Table), Key: key, ReturnValues: types.ReturnValueAllNew,
		UpdateExpression:    aws.String("SET #state = :processing, #revision = :next_revision, #lease_owner = :owner, #lease_epoch = :epoch, #lease_expires = :expires, #updated_at = :now REMOVE #next_attempt_at"),
		ConditionExpression: aws.String(condition), ExpressionAttributeNames: map[string]string{
			"#state": "state", "#revision": "delivery_revision", "#lease_owner": "delivery_lease_owner",
			"#lease_epoch": "delivery_lease_epoch", "#lease_expires": "delivery_lease_expires_at",
			"#updated_at": "updated_at", "#next_attempt_at": "next_attempt_at",
		}, ExpressionAttributeValues: values,
	})
	if err != nil {
		return worker.DeliveryRecord{}, fmt.Errorf("claim notification delivery: %w", err)
	}
	_, claimed, err := decodeDelivery(output.Attributes)
	return claimed, err
}

func (store Store) loadAttemptOutcome(ctx context.Context, deliveryID, attemptID string) (notifications.AttemptOutcome, error) {
	key, err := notifications.AttemptStorageKey(deliveryID, attemptID)
	if err != nil {
		return "", err
	}
	item, err := store.getConsistent(ctx, key.PartitionKey, key.SortKey)
	if err != nil {
		return "", err
	}
	if len(item) == 0 {
		return "", fmt.Errorf("last attempt record is missing")
	}
	var attempt attemptItem
	if err := attributevalue.UnmarshalMap(item, &attempt); err != nil {
		return "", fmt.Errorf("decode last attempt: %w", err)
	}
	if attempt.EntityType != "notification_attempt" || attempt.PK != key.PartitionKey || attempt.SK != key.SortKey ||
		attempt.DeliveryID != deliveryID || attempt.AttemptID != attemptID {
		return "", fmt.Errorf("last attempt identity is invalid")
	}
	if err := attempt.Outcome.Validate(); err != nil {
		return "", fmt.Errorf("last attempt outcome is invalid")
	}
	return attempt.Outcome, nil
}

func (store Store) getConsistent(ctx context.Context, pk, sk string) (map[string]types.AttributeValue, error) {
	key, err := attributevalue.MarshalMap(map[string]string{"PK": pk, "SK": sk})
	if err != nil {
		return nil, err
	}
	output, err := store.Client.GetItem(ctx, &awssdk.GetItemInput{
		TableName: aws.String(store.Table), Key: key, ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("read notification worker dependency: %w", err)
	}
	return output.Item, nil
}

func isConditional(err error) bool {
	var conditional *types.ConditionalCheckFailedException
	return errors.As(err, &conditional)
}
