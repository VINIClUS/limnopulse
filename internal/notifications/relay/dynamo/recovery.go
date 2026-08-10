package dynamo

import (
	"context"
	"fmt"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

func (store Store) ExpandDependency(
	ctx context.Context,
	work relay.Work,
	request relay.ExpandRequest,
) (relay.WorkResult, error) {
	if work.Kind != notifications.WorkKindDependency ||
		work.NotificationKind != notifications.NotificationKindRecovery ||
		work.DependsOnOutboxID == "" || work.State != "pending" ||
		work.LeaseOwner == "" || work.LeaseEpoch < 1 {
		return relay.WorkResult{}, fmt.Errorf("invalid recovery expansion work")
	}
	if request.RelayTime.IsZero() || request.PageSize < 1 || request.PageSize > 99 {
		return relay.WorkResult{}, fmt.Errorf("invalid recovery expansion request")
	}
	if err := store.requireExpandedOpening(ctx, work, request.RelayTime); err != nil {
		return relay.WorkResult{}, err
	}
	page, err := store.queryOpeningDeliveries(ctx, work, request.PageSize)
	if err != nil {
		return relay.WorkResult{}, err
	}
	event, err := store.loadEvent(ctx, work)
	if err != nil {
		return relay.WorkResult{}, err
	}
	templateData, err := event.templateDataFor(work.NotificationKind, work.Evaluation)
	if err != nil {
		return relay.WorkResult{}, err
	}
	renderer, err := store.renderer()
	if err != nil {
		return relay.WorkResult{}, err
	}
	result := relay.WorkResult{}
	deliveries := make([]notifications.Delivery, 0, len(page.Deliveries))
	dependencyChecks := make([]types.TransactWriteItem, 0)
	for _, opening := range page.Deliveries {
		result.RecipientsExamined++
		deliveryID, idErr := notifications.NewDeliveryID(
			work.EventID, notifications.NotificationKindRecovery, notifications.ChannelEmail, opening.RecipientID,
		)
		if idErr != nil {
			return relay.WorkResult{}, idErr
		}
		params := notifications.DeliveryParams{
			TenantID: work.TenantID, OutboxID: work.OutboxID, DeliveryID: deliveryID,
			EventID: work.EventID, RuleID: work.RuleID, Kind: notifications.NotificationKindRecovery,
			Channel: notifications.ChannelEmail, DependsOnOutboxID: work.DependsOnOutboxID,
			DependsOnDeliveryID: opening.DeliveryID, RecipientID: opening.RecipientID,
			NormalizedEmail: opening.NormalizedEmail,
			MembershipSnapshot: notifications.MembershipSnapshot{
				Role: opening.Membership.Role, Status: opening.Membership.Status,
				Version: opening.Membership.Version,
			},
			CreatedAt: request.RelayTime, UpdatedAt: request.RelayTime,
		}
		state := notifications.DeliveryState(opening.State)
		var delivery notifications.Delivery
		if state == notifications.DeliveryStateUnknown || isNonterminalDelivery(state) {
			if localeErr := opening.Content.Locale.Validate(); localeErr != nil {
				return relay.WorkResult{}, fmt.Errorf("opening delivery locale is invalid")
			}
			params.Content, err = renderer.Render(
				notifications.TemplateAlertRecoveryV1, opening.Content.Locale, templateData,
			)
			if err == nil {
				delivery, err = notifications.NewWaitingDependencyDelivery(params)
			}
			if err == nil {
				check, checkErr := store.openingDependencyCondition(opening)
				if checkErr != nil {
					return relay.WorkResult{}, checkErr
				}
				dependencyChecks = append(dependencyChecks, types.TransactWriteItem{ConditionCheck: check})
			}
			result.DeliveriesCreated++
		} else if state != notifications.DeliveryStateSucceeded {
			params.CancellationReason = notifications.CancellationReasonOpeningNotSucceeded
			delivery, err = notifications.NewCancelledDelivery(params)
			result.DeliveriesCancelled++
		} else {
			active, membershipErr := store.currentMembershipActive(ctx, work.TenantID, opening.RecipientID)
			if membershipErr != nil {
				return relay.WorkResult{}, membershipErr
			}
			if !active {
				params.CancellationReason = notifications.CancellationReasonRecipientMembershipInactive
				delivery, err = notifications.NewCancelledDelivery(params)
				result.DeliveriesCancelled++
			} else {
				suppressed, deliverabilityErr := store.addressSuppressed(ctx, opening.NormalizedEmail)
				if deliverabilityErr != nil {
					return relay.WorkResult{}, deliverabilityErr
				}
				if suppressed {
					params.CancellationReason = notifications.CancellationReasonEmailSuppressed
					delivery, err = notifications.NewCancelledDelivery(params)
					result.DeliveriesCancelled++
				} else {
					if localeErr := opening.Content.Locale.Validate(); localeErr != nil {
						return relay.WorkResult{}, fmt.Errorf("opening delivery locale is invalid")
					}
					params.Content, err = renderer.Render(
						notifications.TemplateAlertRecoveryV1, opening.Content.Locale, templateData,
					)
					if err == nil {
						delivery, err = notifications.NewPendingDelivery(params)
					}
					result.DeliveriesCreated++
				}
			}
		}
		if err == nil && state == notifications.DeliveryStateSucceeded {
			check, checkErr := store.openingDependencyCondition(opening)
			if checkErr != nil {
				return relay.WorkResult{}, checkErr
			}
			dependencyChecks = append(dependencyChecks, types.TransactWriteItem{ConditionCheck: check})
		}
		if err != nil {
			return relay.WorkResult{}, fmt.Errorf("build recovery delivery: %w", err)
		}
		deliveries = append(deliveries, delivery)
	}
	if err := store.commitFanoutPage(ctx, work, request, deliveries, dependencyChecks, result, page.NextCursor); err != nil {
		return relay.WorkResult{}, err
	}
	return result, nil
}

func (store Store) openingDependencyCondition(opening openingDelivery) (*types.ConditionCheck, error) {
	key, err := attributevalue.MarshalMap(map[string]string{"PK": opening.PK, "SK": opening.SK})
	if err != nil {
		return nil, err
	}
	values, err := attributevalue.MarshalMap(map[string]any{
		":entity": "notification_delivery", ":outbox_id": opening.OutboxID,
		":delivery_id": opening.DeliveryID, ":event_id": opening.EventID,
		":state": opening.State, ":revision": opening.Revision,
	})
	if err != nil {
		return nil, err
	}
	return &types.ConditionCheck{
		TableName: aws.String(store.Table), Key: key,
		ConditionExpression: aws.String("#entity = :entity AND #outbox_id = :outbox_id AND #delivery_id = :delivery_id AND #event_id = :event_id AND #state = :state AND #revision = :revision"),
		ExpressionAttributeNames: map[string]string{
			"#entity": "entity_type", "#outbox_id": "outbox_id", "#delivery_id": "delivery_id",
			"#event_id": "event_id", "#state": "state", "#revision": "delivery_revision",
		},
		ExpressionAttributeValues: values,
	}, nil
}

func isNonterminalDelivery(state notifications.DeliveryState) bool {
	switch state {
	case notifications.DeliveryStatePending,
		notifications.DeliveryStateQueued,
		notifications.DeliveryStateProcessing,
		notifications.DeliveryStateRetryableFailed:
		return true
	default:
		return false
	}
}
