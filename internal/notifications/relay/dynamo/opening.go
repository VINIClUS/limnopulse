package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
)

func (store Store) ExpandIntent(
	ctx context.Context,
	work relay.Work,
	request relay.ExpandRequest,
) (relay.WorkResult, error) {
	if work.Kind != notifications.WorkKindIntent || work.NotificationKind != notifications.NotificationKindOpening ||
		work.State != "pending" || work.LeaseOwner == "" || work.LeaseEpoch < 1 {
		return relay.WorkResult{}, fmt.Errorf("invalid opening expansion work")
	}
	if request.RelayTime.IsZero() || request.PageSize < 1 || request.PageSize > 99 {
		return relay.WorkResult{}, fmt.Errorf("invalid opening expansion request")
	}
	event, err := store.loadEvent(ctx, work)
	if err != nil {
		return relay.WorkResult{}, err
	}
	templateData, err := event.templateData()
	if err != nil {
		return relay.WorkResult{}, err
	}
	page, err := store.queryMemberships(ctx, work, request.PageSize)
	if err != nil {
		return relay.WorkResult{}, err
	}
	expansionStartedAt := work.ExpansionStartedAt
	if expansionStartedAt.IsZero() {
		expansionStartedAt = request.RelayTime
	}
	eventSeverity, err := severityRank(event.Severity)
	if err != nil {
		return relay.WorkResult{}, err
	}
	result := relay.WorkResult{}
	deliveries := make([]notifications.Delivery, 0, len(page.Members))
	for _, member := range page.Members {
		result.RecipientsExamined++
		createdAt, parseErr := time.Parse(time.RFC3339Nano, member.CreatedAt)
		if parseErr != nil {
			return relay.WorkResult{}, fmt.Errorf("tenant membership timestamp is malformed")
		}
		if member.Status != "active" || createdAt.After(expansionStartedAt) {
			result.RecipientsFiltered++
			continue
		}
		preference, preferenceErr := store.loadPreference(ctx, work.TenantID, member.RecipientID)
		if preferenceErr != nil {
			return relay.WorkResult{}, preferenceErr
		}
		if preference == nil || !preference.EmailEnabled || !preference.EmailVerified {
			result.RecipientsFiltered++
			continue
		}
		minimumSeverity, rankErr := severityRank(preference.MinimumSeverity)
		if rankErr != nil {
			return relay.WorkResult{}, rankErr
		}
		if eventSeverity < minimumSeverity {
			result.RecipientsFiltered++
			result.FilteredBySeverity++
			continue
		}
		suppressed, deliverabilityErr := store.addressSuppressed(ctx, preference.EmailAddress)
		if deliverabilityErr != nil {
			return relay.WorkResult{}, deliverabilityErr
		}
		deliveryID, idErr := notifications.NewDeliveryID(
			work.EventID, notifications.NotificationKindOpening, notifications.ChannelEmail, member.RecipientID,
		)
		if idErr != nil {
			return relay.WorkResult{}, idErr
		}
		params := notifications.DeliveryParams{
			TenantID: work.TenantID, OutboxID: work.OutboxID, DeliveryID: deliveryID,
			EventID: work.EventID, RuleID: work.RuleID, Kind: notifications.NotificationKindOpening,
			Channel: notifications.ChannelEmail, RecipientID: member.RecipientID,
			NormalizedEmail: preference.EmailAddress,
			MembershipSnapshot: notifications.MembershipSnapshot{
				Role: member.Role, Status: member.Status, Version: member.Version,
			},
			CreatedAt: request.RelayTime, UpdatedAt: request.RelayTime,
		}
		var delivery notifications.Delivery
		if suppressed {
			params.CancellationReason = notifications.CancellationReasonEmailSuppressed
			delivery, err = notifications.NewCancelledDelivery(params)
			result.DeliveriesCancelled++
		} else {
			renderer, rendererErr := store.renderer()
			if rendererErr != nil {
				return relay.WorkResult{}, rendererErr
			}
			params.Content, err = renderer.Render(
				notifications.TemplateAlertOpeningV1, notifications.LocalePTBR, templateData,
			)
			if err == nil {
				delivery, err = notifications.NewPendingDelivery(params)
			}
			result.DeliveriesCreated++
		}
		if err != nil {
			return relay.WorkResult{}, fmt.Errorf("build opening delivery: %w", err)
		}
		deliveries = append(deliveries, delivery)
	}
	if err := store.commitFanoutPage(ctx, work, request, deliveries, result, page.NextCursor); err != nil {
		return relay.WorkResult{}, err
	}
	return result, nil
}

func (store Store) renderer() (*notifications.TemplateRenderer, error) {
	if store.Renderer != nil {
		return store.Renderer, nil
	}
	renderer, err := notifications.NewTemplateRenderer()
	if err != nil {
		return nil, fmt.Errorf("initialize notification renderer: %w", err)
	}
	return renderer, nil
}
