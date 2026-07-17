package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
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
	for _, opening := range page.Deliveries {
		if isNonterminalDelivery(notifications.DeliveryState(opening.State)) {
			next := request.RelayTime.Add(time.Minute)
			if opening.AvailableAt != "" {
				availableAt, parseErr := time.Parse(fixedUTCLayout, opening.AvailableAt)
				if parseErr != nil {
					return relay.WorkResult{}, fmt.Errorf("opening delivery availability is invalid")
				}
				if availableAt.After(request.RelayTime) {
					next = availableAt
				}
			}
			return relay.WorkResult{}, &relay.RetryAtError{At: next}
		}
	}
	event, err := store.loadEvent(ctx, work)
	if err != nil {
		return relay.WorkResult{}, err
	}
	templateData, err := event.templateData()
	if err != nil {
		return relay.WorkResult{}, err
	}
	renderer, err := store.renderer()
	if err != nil {
		return relay.WorkResult{}, err
	}
	result := relay.WorkResult{}
	deliveries := make([]notifications.Delivery, 0, len(page.Deliveries))
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
		if state != notifications.DeliveryStateSucceeded {
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
		if err != nil {
			return relay.WorkResult{}, fmt.Errorf("build recovery delivery: %w", err)
		}
		deliveries = append(deliveries, delivery)
	}
	if err := store.commitFanoutPage(ctx, work, request, deliveries, result, page.NextCursor); err != nil {
		return relay.WorkResult{}, err
	}
	return result, nil
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
