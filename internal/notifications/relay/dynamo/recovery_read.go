package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type openingDelivery struct {
	PK              string `dynamodbav:"PK"`
	SK              string `dynamodbav:"SK"`
	EntityType      string `dynamodbav:"entity_type"`
	TenantID        string `dynamodbav:"tenant_id"`
	OutboxID        string `dynamodbav:"outbox_id"`
	DeliveryID      string `dynamodbav:"delivery_id"`
	EventID         string `dynamodbav:"event_id"`
	RuleID          string `dynamodbav:"rule_id"`
	Kind            string `dynamodbav:"kind"`
	Channel         string `dynamodbav:"channel"`
	RecipientID     string `dynamodbav:"recipient_id"`
	NormalizedEmail string `dynamodbav:"normalized_email"`
	State           string `dynamodbav:"state"`
	Revision        int64  `dynamodbav:"delivery_revision"`
	AvailableAt     string `dynamodbav:"available_at"`
	Membership      struct {
		Role    string `dynamodbav:"role"`
		Status  string `dynamodbav:"status"`
		Version int64  `dynamodbav:"version"`
	} `dynamodbav:"membership_snapshot"`
	Content struct {
		Locale notifications.Locale `dynamodbav:"locale"`
	} `dynamodbav:"content"`
}

type openingDeliveryPage struct {
	Deliveries []openingDelivery
	NextCursor string
}

func (store Store) requireExpandedOpening(
	ctx context.Context,
	work relay.Work,
	relayTime time.Time,
) error {
	item, err := store.getConsistent(
		ctx, "TENANT#"+work.TenantID, "NOTIFICATION_OUTBOX#"+work.DependsOnOutboxID,
	)
	if err != nil {
		return err
	}
	if len(item) == 0 {
		return fmt.Errorf("opening notification outbox is missing")
	}
	var opening struct {
		PK              string `dynamodbav:"PK"`
		SK              string `dynamodbav:"SK"`
		EntityType      string `dynamodbav:"entity_type"`
		TenantID        string `dynamodbav:"tenant_id"`
		OutboxID        string `dynamodbav:"outbox_id"`
		EventID         string `dynamodbav:"event_id"`
		Kind            string `dynamodbav:"kind"`
		ExpansionStatus string `dynamodbav:"expansion_status"`
		AvailableAt     string `dynamodbav:"available_at"`
	}
	if err := attributevalue.UnmarshalMap(item, &opening); err != nil {
		return fmt.Errorf("decode opening notification outbox: %w", err)
	}
	if opening.EntityType != "notification_outbox" || opening.TenantID != work.TenantID ||
		opening.OutboxID != work.DependsOnOutboxID || opening.EventID != work.EventID ||
		opening.Kind != "opening" {
		return fmt.Errorf("opening notification outbox identity is invalid")
	}
	if opening.ExpansionStatus == "expanded" {
		return nil
	}
	next := relayTime.Add(time.Minute)
	if opening.AvailableAt != "" {
		availableAt, parseErr := time.Parse(fixedUTCLayout, opening.AvailableAt)
		if parseErr != nil {
			return fmt.Errorf("opening notification availability is invalid")
		}
		if availableAt.After(relayTime) {
			next = availableAt
		}
	}
	return &relay.RetryAtError{At: next}
}

func (store Store) queryOpeningDeliveries(
	ctx context.Context,
	work relay.Work,
	pageSize int,
) (openingDeliveryPage, error) {
	// An unknown opening consumes both a recovery Delivery mutation and an
	// opening-revision fence in the same TransactWriteItems. Capping dependency
	// pages at 49 leaves room for every fence and the outbox checkpoint.
	if pageSize > 49 {
		pageSize = 49
	}
	values, err := attributevalue.MarshalMap(map[string]string{
		":pk": "NOTIFICATION_OUTBOX#" + work.DependsOnOutboxID, ":delivery_prefix": "DELIVERY#",
	})
	if err != nil {
		return openingDeliveryPage{}, err
	}
	input := &awssdk.QueryInput{
		TableName:                 aws.String(store.Table),
		KeyConditionExpression:    aws.String("#pk = :pk AND begins_with(#sk, :delivery_prefix)"),
		ExpressionAttributeNames:  map[string]string{"#pk": "PK", "#sk": "SK"},
		ExpressionAttributeValues: values, Limit: aws.Int32(int32(pageSize)),
		ConsistentRead: aws.Bool(true),
	}
	if work.Cursor != "" {
		input.ExclusiveStartKey, err = decodeBaseCursor(work.Cursor)
		if err != nil {
			return openingDeliveryPage{}, fmt.Errorf("decode dependency cursor: %w", err)
		}
	}
	output, err := store.Client.Query(ctx, input)
	if err != nil {
		return openingDeliveryPage{}, fmt.Errorf("query opening deliveries: %w", err)
	}
	page := openingDeliveryPage{Deliveries: make([]openingDelivery, 0, len(output.Items))}
	for _, item := range output.Items {
		var delivery openingDelivery
		if err := attributevalue.UnmarshalMap(item, &delivery); err != nil {
			return openingDeliveryPage{}, fmt.Errorf("decode opening delivery: %w", err)
		}
		state := notifications.DeliveryState(delivery.State)
		if delivery.EntityType != "notification_delivery" || delivery.TenantID != work.TenantID ||
			delivery.OutboxID != work.DependsOnOutboxID || delivery.EventID != work.EventID ||
			delivery.RuleID != work.RuleID || delivery.Kind != "opening" || delivery.Channel != "email" ||
			delivery.PK != "NOTIFICATION_OUTBOX#"+work.DependsOnOutboxID ||
			delivery.SK != "DELIVERY#"+delivery.DeliveryID || delivery.RecipientID == "" ||
			delivery.NormalizedEmail == "" || state.Validate() != nil || delivery.Revision < 1 {
			return openingDeliveryPage{}, fmt.Errorf("opening delivery is malformed")
		}
		page.Deliveries = append(page.Deliveries, delivery)
	}
	if len(output.LastEvaluatedKey) != 0 {
		page.NextCursor, err = encodeBaseCursor(output.LastEvaluatedKey)
		if err != nil {
			return openingDeliveryPage{}, fmt.Errorf("encode dependency cursor: %w", err)
		}
	}
	return page, nil
}

func (store Store) currentMembershipActive(
	ctx context.Context,
	tenantID string,
	recipientID string,
) (bool, error) {
	item, err := store.getConsistent(ctx, "TENANT#"+tenantID, "MEMBER#"+recipientID)
	if err != nil || len(item) == 0 {
		return false, err
	}
	var member memberSnapshot
	if err := attributevalue.UnmarshalMap(item, &member); err != nil {
		return false, fmt.Errorf("decode current membership: %w", err)
	}
	if member.EntityType != "tenant_member" || member.TenantID != tenantID ||
		member.RecipientID != recipientID || member.Role == "" || member.Version < 1 {
		return false, fmt.Errorf("current membership is malformed")
	}
	return member.Status == "active", nil
}
