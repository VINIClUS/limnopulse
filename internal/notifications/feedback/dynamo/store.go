package dynamo

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/feedback"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	DefaultTransportRetention = 7 * 24 * time.Hour
	maxReconcileAttempts      = 3
)

type Client interface {
	GetItem(context.Context, *awssdk.GetItemInput, ...func(*awssdk.Options)) (*awssdk.GetItemOutput, error)
	TransactWriteItems(context.Context, *awssdk.TransactWriteItemsInput, ...func(*awssdk.Options)) (*awssdk.TransactWriteItemsOutput, error)
}

type Store struct {
	Table              string
	Client             Client
	TransportRetention time.Duration
}

func (store Store) Reconcile(
	ctx context.Context,
	event feedback.Event,
	now time.Time,
) (feedback.ReconcileResult, error) {
	if store.Client == nil || strings.TrimSpace(store.Table) == "" || now.IsZero() {
		return feedback.ReconcileResult{}, fmt.Errorf("invalid SES feedback store configuration")
	}
	if err := event.Validate(); err != nil {
		return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, nil
	}
	now = now.UTC()
	for attempt := 0; attempt < maxReconcileAttempts; attempt++ {
		result, retry, err := store.reconcileOnce(ctx, event, now)
		if err != nil || !retry {
			return result, err
		}
	}
	return feedback.ReconcileResult{}, fmt.Errorf("reconcile SES feedback after concurrent changes")
}

func (store Store) reconcileOnce(
	ctx context.Context,
	event feedback.Event,
	now time.Time,
) (feedback.ReconcileResult, bool, error) {
	transportKey, _ := feedback.TransportDedupeKey(event.EventBridgeID)
	semanticKey, _ := feedback.SemanticDedupeKey(event.ProviderMessageID, event.SemanticType)

	transport, err := store.getConsistent(ctx, transportKey)
	if err != nil {
		return feedback.ReconcileResult{}, false, err
	}
	if len(transport) != 0 {
		if !validDedupeItem(transport, transportKey, "ses_feedback_transport_dedupe") {
			return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, false, nil
		}
		return feedback.ReconcileResult{Disposition: feedback.ReconcileDuplicate}, false, nil
	}

	semantic, err := store.getConsistent(ctx, semanticKey)
	if err != nil {
		return feedback.ReconcileResult{}, false, err
	}
	if len(semantic) != 0 {
		if !validDedupeItem(semantic, semanticKey, "ses_provider_event") {
			return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, false, nil
		}
		if err := store.persistTransportDuplicate(ctx, transportKey, event, now); err != nil {
			return feedback.ReconcileResult{}, false, err
		}
		return feedback.ReconcileResult{Disposition: feedback.ReconcileDuplicate}, false, nil
	}

	attemptKey, _ := notifications.AttemptStorageKey(event.DeliveryID, event.AttemptID)
	attemptItem, err := store.getConsistent(ctx, attemptKey)
	if err != nil {
		return feedback.ReconcileResult{}, false, err
	}
	if len(attemptItem) == 0 {
		return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, false, nil
	}
	attempt, err := decodeAttempt(attemptItem, attemptKey, event)
	if err != nil {
		return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, false, nil
	}

	deliveryKey, err := notifications.DeliveryStorageKey(attempt.OutboxID, event.DeliveryID)
	if err != nil {
		return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, false, nil
	}
	deliveryItem, err := store.getConsistent(ctx, deliveryKey)
	if err != nil {
		return feedback.ReconcileResult{}, false, err
	}
	if len(deliveryItem) == 0 {
		return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, false, nil
	}
	delivery, err := decodeDelivery(deliveryItem, deliveryKey, event, attempt.OutboxID)
	if err != nil {
		return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, false, nil
	}

	var suppression suppressionRecord
	writeSuppression := false
	if event.SuppressionReason != "" {
		suppressionKey, keyErr := notifications.DeliverabilityStorageKey(delivery.NormalizedEmail)
		if keyErr != nil {
			return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, false, nil
		}
		item, getErr := store.getConsistent(ctx, suppressionKey)
		if getErr != nil {
			return feedback.ReconcileResult{}, false, getErr
		}
		suppression, err = decodeSuppression(item, suppressionKey)
		if err != nil {
			return feedback.ReconcileResult{Disposition: feedback.ReconcileAwaitDLQ}, false, nil
		}
		writeSuppression = event.SuppressionReason.Rank() > suppression.Rank
	}

	input, err := store.reconcileTransaction(
		event, now, transportKey, semanticKey, attemptKey, deliveryKey,
		attempt, delivery, suppression, writeSuppression,
	)
	if err != nil {
		return feedback.ReconcileResult{}, false, err
	}
	_, err = store.Client.TransactWriteItems(ctx, input)
	if err == nil {
		return feedback.ReconcileResult{
			Disposition: feedback.ReconcileApplied,
			Suppressed:  event.SuppressionReason != "",
		}, false, nil
	}
	if !isTransactionCanceled(err) {
		return feedback.ReconcileResult{}, false, fmt.Errorf("persist SES feedback reconciliation: %w", err)
	}
	duplicate, lookupErr := store.dedupeNowExists(ctx, transportKey, semanticKey)
	if lookupErr != nil {
		return feedback.ReconcileResult{}, false, lookupErr
	}
	if duplicate {
		return feedback.ReconcileResult{Disposition: feedback.ReconcileDuplicate}, false, nil
	}
	return feedback.ReconcileResult{}, true, nil
}

func (store Store) getConsistent(
	ctx context.Context,
	key notifications.StorageKey,
) (map[string]types.AttributeValue, error) {
	encodedKey, err := attributevalue.MarshalMap(map[string]string{
		"PK": key.PartitionKey,
		"SK": key.SortKey,
	})
	if err != nil {
		return nil, err
	}
	output, err := store.Client.GetItem(ctx, &awssdk.GetItemInput{
		TableName: aws.String(store.Table), Key: encodedKey, ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, fmt.Errorf("read SES feedback durable dependency: %w", err)
	}
	if output == nil {
		return nil, fmt.Errorf("read SES feedback durable dependency returned no output")
	}
	return output.Item, nil
}

func (store Store) persistTransportDuplicate(
	ctx context.Context,
	key notifications.StorageKey,
	event feedback.Event,
	now time.Time,
) error {
	put, err := store.transportPut(key, event, now)
	if err != nil {
		return err
	}
	_, err = store.Client.TransactWriteItems(ctx, &awssdk.TransactWriteItemsInput{
		ClientRequestToken: aws.String(clientRequestToken(event.EventBridgeID)),
		TransactItems:      []types.TransactWriteItem{{Put: put}},
	})
	if err == nil || isTransactionCanceled(err) {
		return nil
	}
	return fmt.Errorf("persist SES feedback transport dedupe: %w", err)
}

func (store Store) dedupeNowExists(
	ctx context.Context,
	transportKey notifications.StorageKey,
	semanticKey notifications.StorageKey,
) (bool, error) {
	transport, err := store.getConsistent(ctx, transportKey)
	if err != nil {
		return false, err
	}
	if len(transport) != 0 {
		return true, nil
	}
	semantic, err := store.getConsistent(ctx, semanticKey)
	if err != nil {
		return false, err
	}
	return len(semantic) != 0, nil
}

func validDedupeItem(
	item map[string]types.AttributeValue,
	key notifications.StorageKey,
	entityType string,
) bool {
	var stored struct {
		PK         string `dynamodbav:"PK"`
		SK         string `dynamodbav:"SK"`
		EntityType string `dynamodbav:"entity_type"`
	}
	return attributevalue.UnmarshalMap(item, &stored) == nil && stored.PK == key.PartitionKey &&
		stored.SK == key.SortKey && stored.EntityType == entityType
}

func isTransactionCanceled(err error) bool {
	var cancelled *types.TransactionCanceledException
	return errors.As(err, &cancelled)
}

func clientRequestToken(identity string) string {
	digest := sha256.Sum256([]byte(identity))
	return "sesfb_" + hex.EncodeToString(digest[:15])
}

func fixedTime(value time.Time) string {
	return value.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

var _ feedback.Store = Store{}
