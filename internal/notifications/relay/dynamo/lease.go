package dynamo

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/smithy-go"
)

func (store Store) Claim(
	ctx context.Context,
	work relay.Work,
	lease relay.LeaseRequest,
) (relay.Work, bool, error) {
	if strings.TrimSpace(lease.Owner) == "" || lease.Now.IsZero() || lease.ExpiresAt.IsZero() ||
		lease.DueThrough.IsZero() || !lease.ExpiresAt.After(lease.Now) {
		return relay.Work{}, false, fmt.Errorf("invalid relay lease request")
	}
	stateName, revisionName, cursorName, err := mutationFieldNames(work.Kind)
	if err != nil {
		return relay.Work{}, false, err
	}
	key, err := attributevalue.MarshalMap(map[string]string{"PK": work.PK, "SK": work.SK})
	if err != nil {
		return relay.Work{}, false, fmt.Errorf("marshal relay lease key: %w", err)
	}
	values := map[string]any{
		":schema": int64(1), ":relay_work_kind": string(work.Kind),
		":relay_pk": work.RelayPK, ":relay_sk": work.RelaySK,
		":due": lease.DueThrough.UTC().Format(fixedUTCLayout), ":state": work.State,
		":revision": work.Revision, ":owner": lease.Owner,
		":now":     lease.Now.UTC().Format(fixedUTCLayout),
		":expires": lease.ExpiresAt.UTC().Format(fixedUTCLayout),
		":zero":    int64(0), ":one": int64(1),
	}
	names := map[string]string{
		"#relay_schema": "relay_schema_version", "#relay_work_kind": "relay_work_kind",
		"#relay_pk": "relay_gsi_pk",
		"#relay_sk": "relay_gsi_sk", "#available_at": "available_at",
		"#state": stateName, "#revision": revisionName,
		"#lease_owner": "relay_lease_owner", "#lease_epoch": "relay_lease_epoch",
		"#lease_expires": "relay_lease_expires_at",
	}
	condition := "#relay_schema = :schema AND #relay_work_kind = :relay_work_kind " +
		"AND #relay_pk = :relay_pk AND #relay_sk = :relay_sk " +
		"AND #available_at <= :due AND #state = :state "
	if work.Revision == 0 {
		condition += "AND (attribute_not_exists(#revision) OR #revision = :revision) "
	} else {
		condition += "AND #revision = :revision "
	}
	condition += "AND (attribute_not_exists(#lease_expires) OR #lease_expires <= :now OR #lease_owner = :owner)"
	if cursorName != "" {
		names["#cursor"] = cursorName
		values[":cursor"] = work.Cursor
		if work.Cursor == "" {
			condition += " AND (attribute_not_exists(#cursor) OR #cursor = :cursor)"
		} else {
			condition += " AND #cursor = :cursor"
		}
	}
	encodedValues, err := attributevalue.MarshalMap(values)
	if err != nil {
		return relay.Work{}, false, fmt.Errorf("marshal relay lease values: %w", err)
	}
	output, err := store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName: aws.String(store.Table), Key: key,
		UpdateExpression: aws.String(
			"SET #lease_owner = :owner, #lease_expires = :expires, " +
				"#lease_epoch = if_not_exists(#lease_epoch, :zero) + :one",
		),
		ConditionExpression:       aws.String(condition),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: encodedValues,
		ReturnValues:              types.ReturnValueAllNew,
	})
	if err != nil {
		if isConditional(err) {
			return relay.Work{}, false, nil
		}
		return relay.Work{}, false, fmt.Errorf("claim relay lease: %w", err)
	}
	if len(output.Attributes) == 0 {
		return relay.Work{}, false, fmt.Errorf("claim relay lease returned no item")
	}
	var claimed relayItem
	if err := attributevalue.UnmarshalMap(output.Attributes, &claimed); err != nil {
		return relay.Work{}, false, fmt.Errorf("decode claimed relay item: %w", err)
	}
	if claimed.RelayLeaseEpoch <= work.LeaseEpoch {
		return relay.Work{}, false, fmt.Errorf("relay lease epoch did not advance")
	}
	work.LeaseEpoch = claimed.RelayLeaseEpoch
	work.LeaseOwner = lease.Owner
	return work, true, nil
}

func mutationFieldNames(kind notifications.WorkKind) (state, revision, cursor string, err error) {
	switch kind {
	case notifications.WorkKindIntent, notifications.WorkKindDependency:
		return "expansion_status", "expansion_revision", "expansion_cursor", nil
	case notifications.WorkKindDelivery:
		return "state", "delivery_revision", "", nil
	default:
		return "", "", "", kind.Validate()
	}
}

func isConditional(err error) bool {
	var apiErr smithy.APIError
	if !errors.As(err, &apiErr) {
		return false
	}
	return apiErr.ErrorCode() == "ConditionalCheckFailedException"
}
