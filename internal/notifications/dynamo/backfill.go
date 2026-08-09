package dynamo

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type Client interface {
	Query(context.Context, *awssdk.QueryInput, ...func(*awssdk.Options)) (*awssdk.QueryOutput, error)
	UpdateItem(context.Context, *awssdk.UpdateItemInput, ...func(*awssdk.Options)) (*awssdk.UpdateItemOutput, error)
}

type Store struct {
	Table  string
	Client Client
}

type BackfillOptions struct {
	Tenants  []string
	Apply    bool
	PageSize int
	MaxRows  int
}

type BackfillSummary struct {
	DryRun            bool `json:"dry_run"`
	Tenants           int  `json:"tenants"`
	RowsQueried       int  `json:"rows_queried"`
	RowsNeedingUpdate int  `json:"rows_needing_update"`
	WouldUpdate       int  `json:"would_update"`
	Updated           int  `json:"updated"`
	Noop              int  `json:"noop"`
	SchemaConflicts   int  `json:"schema_conflicts"`
	ConcurrentChanges int  `json:"concurrent_changes"`
	DecodeFailures    int  `json:"decode_failures"`
	UpdateFailures    int  `json:"update_failures"`
	RowFailures       int  `json:"row_failures"`
	LimitReached      bool `json:"limit_reached"`
	DeadlineReached   bool `json:"deadline_reached"`
}

const (
	DefaultBackfillMaxRows = 10_000
	fixedUTCLayout         = "2006-01-02T15:04:05.000000000Z"
)

type relayOutbox struct {
	PK        string                         `dynamodbav:"PK"`
	SK        string                         `dynamodbav:"SK"`
	TenantID  string                         `dynamodbav:"tenant_id"`
	OutboxID  string                         `dynamodbav:"outbox_id"`
	Channel   string                         `dynamodbav:"channel"`
	Kind      notifications.NotificationKind `dynamodbav:"kind"`
	Status    string                         `dynamodbav:"status"`
	CreatedAt string                         `dynamodbav:"created_at"`
}

type relayDelivery struct {
	PK                 string                      `dynamodbav:"PK"`
	SK                 string                      `dynamodbav:"SK"`
	EntityType         string                      `dynamodbav:"entity_type"`
	TenantID           string                      `dynamodbav:"tenant_id"`
	OutboxID           string                      `dynamodbav:"outbox_id"`
	DeliveryID         string                      `dynamodbav:"delivery_id"`
	State              notifications.DeliveryState `dynamodbav:"state"`
	Revision           int64                       `dynamodbav:"delivery_revision"`
	RelaySchemaVersion int64                       `dynamodbav:"relay_schema_version"`
	AvailableAt        string                      `dynamodbav:"available_at"`
	WorkKind           notifications.WorkKind      `dynamodbav:"relay_work_kind"`
	RelayPK            string                      `dynamodbav:"relay_gsi_pk"`
	RelaySK            string                      `dynamodbav:"relay_gsi_sk"`
}

type migrationKind uint8

const (
	migrationNoop migrationKind = iota
	migrationUpdateEmail
	migrationUpdateWorkKind
	migrationUpdateRelayIndex
	migrationUpdateTelegram
	migrationSchemaConflict
)

type relayMigration struct {
	Outbox        relayOutbox
	Expansion     string
	AvailableAt   string
	WorkKind      notifications.WorkKind
	RelayIndexKey notifications.RelayIndexKey
}

type relayDeliveryMigration struct {
	Delivery      relayDelivery
	RelayIndexKey notifications.RelayIndexKey
}

func (store Store) BackfillRelay(ctx context.Context, options BackfillOptions) (BackfillSummary, error) {
	summary := BackfillSummary{DryRun: !options.Apply, Tenants: len(options.Tenants)}
	if len(options.Tenants) == 0 {
		return summary, fmt.Errorf("at least one explicit tenant is required")
	}
	if options.PageSize < 1 {
		return summary, fmt.Errorf("page size must be positive")
	}
	maxRows := options.MaxRows
	if maxRows == 0 {
		maxRows = DefaultBackfillMaxRows
	}
	if maxRows < 0 {
		return summary, fmt.Errorf("max rows must be positive")
	}
	for _, tenantID := range options.Tenants {
		if strings.TrimSpace(tenantID) == "" || strings.ContainsRune(tenantID, '\x00') {
			return summary, fmt.Errorf("tenant ID is invalid")
		}
	}
	for _, tenantID := range options.Tenants {
		var lastKey map[string]types.AttributeValue
		for {
			if stopped, err := backfillContextStopped(ctx, &summary); stopped {
				return summary, err
			}
			if summary.RowsQueried >= maxRows {
				summary.LimitReached = true
				return summary, nil
			}
			queryLimit := options.PageSize
			if remaining := maxRows - summary.RowsQueried; remaining < queryLimit {
				queryLimit = remaining
			}
			values, err := attributevalue.MarshalMap(map[string]string{
				":pk": "TENANT#" + tenantID, ":prefix": "NOTIFICATION_OUTBOX#",
			})
			if err != nil {
				return summary, fmt.Errorf("encode relay backfill query: %w", err)
			}
			output, err := store.Client.Query(ctx, &awssdk.QueryInput{
				TableName:                 aws.String(store.Table),
				KeyConditionExpression:    aws.String("PK = :pk AND begins_with(SK, :prefix)"),
				ExpressionAttributeValues: values,
				ExclusiveStartKey:         lastKey,
				Limit:                     aws.Int32(int32(queryLimit)),
				ConsistentRead:            aws.Bool(true),
			})
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
					summary.DeadlineReached = true
					return summary, nil
				}
				if ctx.Err() != nil {
					return summary, ctx.Err()
				}
				return summary, fmt.Errorf("query notification outboxes: %w", err)
			}
			for _, item := range output.Items {
				if stopped, stopErr := backfillContextStopped(ctx, &summary); stopped {
					return summary, stopErr
				}
				if summary.RowsQueried >= maxRows {
					summary.LimitReached = true
					return summary, nil
				}
				summary.RowsQueried++
				kind, migration, err := classifyRelayOutbox(item, tenantID)
				if err != nil {
					summary.DecodeFailures++
					summary.RowFailures++
					continue
				}
				switch kind {
				case migrationNoop:
					summary.Noop++
				case migrationSchemaConflict:
					summary.SchemaConflicts++
					summary.RowFailures++
				default:
					summary.RowsNeedingUpdate++
					if !options.Apply {
						summary.WouldUpdate++
					} else if err := store.updateRelayOutbox(ctx, kind, migration); err != nil {
						if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
							summary.DeadlineReached = true
							return summary, nil
						}
						if ctx.Err() != nil {
							return summary, ctx.Err()
						}
						if isConditionalCheckFailed(err) {
							summary.ConcurrentChanges++
						} else {
							summary.UpdateFailures++
						}
						summary.RowFailures++
					} else {
						summary.Updated++
					}
				}
				if stopped, deliveryErr := store.backfillRelayDeliveries(
					ctx, options.PageSize, maxRows, &summary, migration.Outbox,
				); stopped {
					return summary, deliveryErr
				}
			}
			lastKey = output.LastEvaluatedKey
			if len(lastKey) == 0 {
				break
			}
		}
	}
	return summary, nil
}

func (store Store) backfillRelayDeliveries(
	ctx context.Context,
	pageSize int,
	maxRows int,
	summary *BackfillSummary,
	outbox relayOutbox,
) (bool, error) {
	if outbox.Channel != "email" {
		return false, nil
	}
	var lastKey map[string]types.AttributeValue
	for {
		if stopped, err := backfillContextStopped(ctx, summary); stopped {
			return true, err
		}
		if summary.RowsQueried >= maxRows {
			summary.LimitReached = true
			return true, nil
		}
		queryLimit := pageSize
		if remaining := maxRows - summary.RowsQueried; remaining < queryLimit {
			queryLimit = remaining
		}
		values, err := attributevalue.MarshalMap(map[string]string{
			":pk": "NOTIFICATION_OUTBOX#" + outbox.OutboxID, ":prefix": "DELIVERY#",
		})
		if err != nil {
			return true, fmt.Errorf("encode relay delivery backfill query: %w", err)
		}
		output, err := store.Client.Query(ctx, &awssdk.QueryInput{
			TableName:                 aws.String(store.Table),
			KeyConditionExpression:    aws.String("PK = :pk AND begins_with(SK, :prefix)"),
			ExpressionAttributeValues: values,
			ExclusiveStartKey:         lastKey,
			Limit:                     aws.Int32(int32(queryLimit)),
			ConsistentRead:            aws.Bool(true),
		})
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
				summary.DeadlineReached = true
				return true, nil
			}
			if ctx.Err() != nil {
				return true, ctx.Err()
			}
			return true, fmt.Errorf("query notification deliveries: %w", err)
		}
		for _, item := range output.Items {
			if stopped, err := backfillContextStopped(ctx, summary); stopped {
				return true, err
			}
			if summary.RowsQueried >= maxRows {
				summary.LimitReached = true
				return true, nil
			}
			summary.RowsQueried++
			kind, migration, classifyErr := classifyRelayDelivery(item, outbox)
			if classifyErr != nil {
				summary.DecodeFailures++
				summary.RowFailures++
				continue
			}
			switch kind {
			case migrationNoop:
				summary.Noop++
				continue
			case migrationSchemaConflict:
				summary.SchemaConflicts++
				summary.RowFailures++
				continue
			}
			summary.RowsNeedingUpdate++
			if summary.DryRun {
				summary.WouldUpdate++
				continue
			}
			if updateErr := store.updateRelayDelivery(ctx, kind, migration); updateErr != nil {
				if errors.Is(updateErr, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
					summary.DeadlineReached = true
					return true, nil
				}
				if ctx.Err() != nil {
					return true, ctx.Err()
				}
				if isConditionalCheckFailed(updateErr) {
					summary.ConcurrentChanges++
				} else {
					summary.UpdateFailures++
				}
				summary.RowFailures++
				continue
			}
			summary.Updated++
		}
		lastKey = output.LastEvaluatedKey
		if len(lastKey) == 0 {
			return false, nil
		}
	}
}

func backfillContextStopped(
	ctx context.Context,
	summary *BackfillSummary,
) (bool, error) {
	err := ctx.Err()
	if err == nil {
		return false, nil
	}
	if errors.Is(err, context.DeadlineExceeded) {
		summary.DeadlineReached = true
		return true, nil
	}
	return true, err
}

func isConditionalCheckFailed(err error) bool {
	var conditional *types.ConditionalCheckFailedException
	return errors.As(err, &conditional)
}

func classifyRelayOutbox(
	item map[string]types.AttributeValue,
	tenantID string,
) (migrationKind, relayMigration, error) {
	var outbox relayOutbox
	if err := attributevalue.UnmarshalMap(item, &outbox); err != nil {
		return migrationNoop, relayMigration{}, fmt.Errorf("decode notification outbox: %w", err)
	}
	if outbox.PK != "TENANT#"+tenantID || outbox.TenantID != tenantID ||
		outbox.SK != "NOTIFICATION_OUTBOX#"+outbox.OutboxID || outbox.OutboxID == "" ||
		strings.ContainsRune(outbox.OutboxID, '\x00') || strings.ContainsRune(tenantID, '\x00') {
		return migrationNoop, relayMigration{}, fmt.Errorf("notification outbox identity is invalid")
	}

	migration := relayMigration{Outbox: outbox}
	hasRelay := hasAnyRelayField(item)
	if _, exists := item["relay_schema_version"]; exists &&
		!numberAttributeEquals(item, "relay_schema_version", notifications.RelaySchemaVersion) {
		return migrationSchemaConflict, migration, nil
	}
	if outbox.Channel == "telegram" {
		migration.Expansion = "deferred_unsupported_channel"
		if !hasRelay {
			return migrationUpdateTelegram, migration, nil
		}
		if isCanonicalTelegram(item) {
			return migrationNoop, migration, nil
		}
		return migrationSchemaConflict, migration, nil
	}
	if outbox.Channel != "email" {
		if hasRelay {
			return migrationSchemaConflict, migration, nil
		}
		return migrationNoop, relayMigration{}, fmt.Errorf("notification outbox channel is unsupported")
	}

	workKind, err := notifications.ClassifyOutboxRelayWork(
		outbox.Kind,
		notifications.OutboxStatus(outbox.Status),
	)
	if err != nil {
		if hasRelay {
			return migrationSchemaConflict, migration, nil
		}
		return migrationNoop, relayMigration{}, fmt.Errorf("notification outbox kind or status is unsupported")
	}
	if isExpandedEmail(item, workKind) {
		return migrationNoop, migration, nil
	}
	createdAt, err := time.Parse(fixedUTCLayout, outbox.CreatedAt)
	if err != nil || createdAt.UTC().Format(fixedUTCLayout) != outbox.CreatedAt {
		if hasRelay {
			return migrationSchemaConflict, migration, nil
		}
		return migrationNoop, relayMigration{}, fmt.Errorf("notification outbox created_at is not canonical")
	}
	migration.Expansion = "pending"
	migration.AvailableAt = outbox.CreatedAt
	migration.WorkKind = workKind
	migration.RelayIndexKey, err = notifications.BuildRelayIndexKey(
		workKind,
		outbox.TenantID,
		outbox.OutboxID,
		createdAt,
	)
	if err != nil {
		return migrationNoop, relayMigration{}, fmt.Errorf("build relay index key: %w", err)
	}
	if !hasRelay {
		return migrationUpdateEmail, migration, nil
	}
	if isCanonicalEmail(item, migration) {
		return migrationNoop, migration, nil
	}
	if isCanonicalEmailWithoutWorkKind(item, migration) {
		return migrationUpdateWorkKind, migration, nil
	}
	if isPreviousEmailLayout(item, migration) {
		return migrationUpdateRelayIndex, migration, nil
	}
	return migrationSchemaConflict, migration, nil
}

func hasAnyRelayField(item map[string]types.AttributeValue) bool {
	for _, name := range []string{
		"relay_schema_version", "expansion_status", "available_at", "relay_work_kind",
		"relay_gsi_pk", "relay_gsi_sk",
	} {
		if _, exists := item[name]; exists {
			return true
		}
	}
	return false
}

func isCanonicalEmail(item map[string]types.AttributeValue, migration relayMigration) bool {
	return numberAttributeEquals(item, "relay_schema_version", notifications.RelaySchemaVersion) &&
		stringAttributeEquals(item, "expansion_status", migration.Expansion) &&
		stringAttributeEquals(item, "available_at", migration.AvailableAt) &&
		stringAttributeEquals(item, "relay_work_kind", string(migration.WorkKind)) &&
		stringAttributeEquals(item, "relay_gsi_pk", migration.RelayIndexKey.PartitionKey) &&
		stringAttributeEquals(item, "relay_gsi_sk", migration.RelayIndexKey.SortKey)
}

func isExpandedEmail(item map[string]types.AttributeValue, workKind notifications.WorkKind) bool {
	if !numberAttributeEquals(item, "relay_schema_version", notifications.RelaySchemaVersion) ||
		!stringAttributeEquals(item, "expansion_status", "expanded") ||
		!stringAttributeEquals(item, "relay_work_kind", string(workKind)) {
		return false
	}
	for _, name := range []string{"available_at", "relay_gsi_pk", "relay_gsi_sk"} {
		if _, exists := item[name]; exists {
			return false
		}
	}
	return true
}

func isCanonicalEmailWithoutWorkKind(
	item map[string]types.AttributeValue,
	migration relayMigration,
) bool {
	if _, exists := item["relay_work_kind"]; exists {
		return false
	}
	return numberAttributeEquals(item, "relay_schema_version", notifications.RelaySchemaVersion) &&
		stringAttributeEquals(item, "expansion_status", migration.Expansion) &&
		stringAttributeEquals(item, "available_at", migration.AvailableAt) &&
		stringAttributeEquals(item, "relay_gsi_pk", migration.RelayIndexKey.PartitionKey) &&
		stringAttributeEquals(item, "relay_gsi_sk", migration.RelayIndexKey.SortKey)
}

func isPreviousEmailLayout(item map[string]types.AttributeValue, migration relayMigration) bool {
	return numberAttributeEquals(item, "relay_schema_version", notifications.RelaySchemaVersion) &&
		stringAttributeEquals(item, "expansion_status", migration.Expansion) &&
		stringAttributeEquals(item, "available_at", migration.AvailableAt) &&
		stringAttributeEquals(item, "relay_work_kind", string(migration.WorkKind)) &&
		stringAttributeEquals(item, "relay_gsi_pk", migration.RelayIndexKey.PartitionKey) &&
		stringAttributeEquals(item, "relay_gsi_sk", previousRelaySortKey(
			migration.AvailableAt, migration.WorkKind, migration.Outbox.TenantID, migration.Outbox.OutboxID,
		))
}

func classifyRelayDelivery(
	item map[string]types.AttributeValue,
	outbox relayOutbox,
) (migrationKind, relayDeliveryMigration, error) {
	var delivery relayDelivery
	if err := attributevalue.UnmarshalMap(item, &delivery); err != nil {
		return migrationNoop, relayDeliveryMigration{}, fmt.Errorf("decode notification delivery: %w", err)
	}
	if delivery.PK != "NOTIFICATION_OUTBOX#"+outbox.OutboxID ||
		delivery.SK != "DELIVERY#"+delivery.DeliveryID || delivery.DeliveryID == "" ||
		delivery.EntityType != "notification_delivery" || delivery.TenantID != outbox.TenantID ||
		delivery.OutboxID != outbox.OutboxID || strings.ContainsRune(delivery.DeliveryID, '\x00') {
		return migrationNoop, relayDeliveryMigration{}, fmt.Errorf("notification delivery identity is invalid")
	}
	if delivery.State != notifications.DeliveryStatePending {
		return migrationNoop, relayDeliveryMigration{}, nil
	}
	availableAt, err := time.Parse(fixedUTCLayout, delivery.AvailableAt)
	if err != nil || availableAt.UTC().Format(fixedUTCLayout) != delivery.AvailableAt {
		return migrationNoop, relayDeliveryMigration{}, fmt.Errorf("notification delivery available_at is not canonical")
	}
	index, err := notifications.BuildRelayIndexKey(
		notifications.WorkKindDelivery, delivery.TenantID, delivery.DeliveryID, availableAt,
	)
	if err != nil {
		return migrationNoop, relayDeliveryMigration{}, fmt.Errorf("build delivery relay index key: %w", err)
	}
	migration := relayDeliveryMigration{Delivery: delivery, RelayIndexKey: index}
	if delivery.RelaySchemaVersion != notifications.RelaySchemaVersion ||
		delivery.WorkKind != notifications.WorkKindDelivery || delivery.RelayPK != index.PartitionKey ||
		delivery.Revision < 1 {
		return migrationSchemaConflict, migration, nil
	}
	if delivery.RelaySK == index.SortKey {
		return migrationNoop, migration, nil
	}
	if delivery.RelaySK == previousRelaySortKey(
		delivery.AvailableAt, notifications.WorkKindDelivery, delivery.TenantID, delivery.DeliveryID,
	) {
		return migrationUpdateRelayIndex, migration, nil
	}
	return migrationSchemaConflict, migration, nil
}

func previousRelaySortKey(availableAt string, workKind notifications.WorkKind, tenantID, itemID string) string {
	return fmt.Sprintf(
		"%s#%s#%s#%s",
		availableAt,
		workKind,
		base64.RawURLEncoding.EncodeToString([]byte(tenantID)),
		base64.RawURLEncoding.EncodeToString([]byte(itemID)),
	)
}

func isCanonicalTelegram(item map[string]types.AttributeValue) bool {
	if !stringAttributeEquals(item, "expansion_status", "deferred_unsupported_channel") {
		return false
	}
	for _, name := range []string{"relay_schema_version", "available_at", "relay_work_kind", "relay_gsi_pk", "relay_gsi_sk"} {
		if _, exists := item[name]; exists {
			return false
		}
	}
	return true
}

func numberAttributeEquals(item map[string]types.AttributeValue, name string, want int64) bool {
	value, ok := item[name].(*types.AttributeValueMemberN)
	return ok && value.Value == strconv.FormatInt(want, 10)
}

func stringAttributeEquals(item map[string]types.AttributeValue, name, want string) bool {
	value, ok := item[name].(*types.AttributeValueMemberS)
	return ok && value.Value == want
}

func (store Store) updateRelayOutbox(
	ctx context.Context,
	kind migrationKind,
	migration relayMigration,
) error {
	key, err := attributevalue.MarshalMap(map[string]string{
		"PK": migration.Outbox.PK, "SK": migration.Outbox.SK,
	})
	if err != nil {
		return fmt.Errorf("encode notification outbox key: %w", err)
	}
	valueMap := map[string]any{
		":expected_tenant_id":  migration.Outbox.TenantID,
		":expected_outbox_id":  migration.Outbox.OutboxID,
		":expected_channel":    migration.Outbox.Channel,
		":expected_status":     migration.Outbox.Status,
		":expected_created_at": migration.Outbox.CreatedAt,
		":expansion_status":    migration.Expansion,
	}
	updateExpression := "SET #expansion_status = :expansion_status"
	conditionExpression := "#tenant_id = :expected_tenant_id AND #outbox_id = :expected_outbox_id AND " +
		"#channel = :expected_channel AND #status = :expected_status AND #created_at = :expected_created_at AND " +
		"attribute_not_exists(#relay_schema_version) AND attribute_not_exists(#expansion_status) AND " +
		"attribute_not_exists(#available_at) AND attribute_not_exists(#relay_work_kind) AND " +
		"attribute_not_exists(#relay_gsi_pk) AND attribute_not_exists(#relay_gsi_sk)"
	if kind == migrationUpdateEmail {
		valueMap[":relay_schema_version"] = notifications.RelaySchemaVersion
		valueMap[":available_at"] = migration.AvailableAt
		valueMap[":relay_work_kind"] = string(migration.WorkKind)
		valueMap[":relay_gsi_pk"] = migration.RelayIndexKey.PartitionKey
		valueMap[":relay_gsi_sk"] = migration.RelayIndexKey.SortKey
		updateExpression = "SET #relay_schema_version = :relay_schema_version, #expansion_status = :expansion_status, " +
			"#available_at = :available_at, #relay_work_kind = :relay_work_kind, " +
			"#relay_gsi_pk = :relay_gsi_pk, #relay_gsi_sk = :relay_gsi_sk"
	} else if kind == migrationUpdateWorkKind {
		delete(valueMap, ":expansion_status")
		valueMap[":relay_work_kind"] = string(migration.WorkKind)
		valueMap[":expected_relay_schema_version"] = notifications.RelaySchemaVersion
		valueMap[":expected_expansion_status"] = migration.Expansion
		valueMap[":expected_available_at"] = migration.AvailableAt
		valueMap[":expected_relay_gsi_pk"] = migration.RelayIndexKey.PartitionKey
		valueMap[":expected_relay_gsi_sk"] = migration.RelayIndexKey.SortKey
		updateExpression = "SET #relay_work_kind = :relay_work_kind"
		conditionExpression = "#tenant_id = :expected_tenant_id AND #outbox_id = :expected_outbox_id AND " +
			"#channel = :expected_channel AND #status = :expected_status AND #created_at = :expected_created_at AND " +
			"#relay_schema_version = :expected_relay_schema_version AND " +
			"#expansion_status = :expected_expansion_status AND #available_at = :expected_available_at AND " +
			"#relay_gsi_pk = :expected_relay_gsi_pk AND #relay_gsi_sk = :expected_relay_gsi_sk AND " +
			"attribute_not_exists(#relay_work_kind)"
	} else if kind == migrationUpdateRelayIndex {
		delete(valueMap, ":expansion_status")
		valueMap[":expected_relay_schema_version"] = notifications.RelaySchemaVersion
		valueMap[":expected_expansion_status"] = migration.Expansion
		valueMap[":expected_available_at"] = migration.AvailableAt
		valueMap[":expected_relay_work_kind"] = string(migration.WorkKind)
		valueMap[":expected_relay_gsi_pk"] = migration.RelayIndexKey.PartitionKey
		valueMap[":expected_relay_gsi_sk"] = previousRelaySortKey(
			migration.AvailableAt, migration.WorkKind, migration.Outbox.TenantID, migration.Outbox.OutboxID,
		)
		valueMap[":relay_gsi_sk"] = migration.RelayIndexKey.SortKey
		updateExpression = "SET #relay_gsi_sk = :relay_gsi_sk"
		conditionExpression = "#tenant_id = :expected_tenant_id AND #outbox_id = :expected_outbox_id AND " +
			"#channel = :expected_channel AND #status = :expected_status AND #created_at = :expected_created_at AND " +
			"#relay_schema_version = :expected_relay_schema_version AND " +
			"#expansion_status = :expected_expansion_status AND #available_at = :expected_available_at AND " +
			"#relay_work_kind = :expected_relay_work_kind AND #relay_gsi_pk = :expected_relay_gsi_pk AND " +
			"#relay_gsi_sk = :expected_relay_gsi_sk"
	}
	values, err := attributevalue.MarshalMap(valueMap)
	if err != nil {
		return fmt.Errorf("encode relay backfill update: %w", err)
	}
	_, err = store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName:           aws.String(store.Table),
		Key:                 key,
		UpdateExpression:    aws.String(updateExpression),
		ConditionExpression: aws.String(conditionExpression),
		ExpressionAttributeNames: map[string]string{
			"#tenant_id": "tenant_id", "#outbox_id": "outbox_id", "#channel": "channel",
			"#status": "status", "#created_at": "created_at", "#relay_schema_version": "relay_schema_version",
			"#expansion_status": "expansion_status", "#available_at": "available_at",
			"#relay_work_kind": "relay_work_kind",
			"#relay_gsi_pk":    "relay_gsi_pk", "#relay_gsi_sk": "relay_gsi_sk",
		},
		ExpressionAttributeValues: values,
	})
	if err != nil {
		return fmt.Errorf("update notification relay fields: %w", err)
	}
	return nil
}

func (store Store) updateRelayDelivery(
	ctx context.Context,
	kind migrationKind,
	migration relayDeliveryMigration,
) error {
	if kind != migrationUpdateRelayIndex {
		return fmt.Errorf("unsupported notification delivery migration")
	}
	key, err := attributevalue.MarshalMap(map[string]string{
		"PK": migration.Delivery.PK, "SK": migration.Delivery.SK,
	})
	if err != nil {
		return fmt.Errorf("encode notification delivery key: %w", err)
	}
	values, err := attributevalue.MarshalMap(map[string]any{
		":expected_entity_type":          "notification_delivery",
		":expected_tenant_id":            migration.Delivery.TenantID,
		":expected_outbox_id":            migration.Delivery.OutboxID,
		":expected_delivery_id":          migration.Delivery.DeliveryID,
		":expected_state":                string(notifications.DeliveryStatePending),
		":expected_revision":             migration.Delivery.Revision,
		":expected_relay_schema_version": notifications.RelaySchemaVersion,
		":expected_available_at":         migration.Delivery.AvailableAt,
		":expected_relay_work_kind":      string(notifications.WorkKindDelivery),
		":expected_relay_gsi_pk":         migration.RelayIndexKey.PartitionKey,
		":expected_relay_gsi_sk": previousRelaySortKey(
			migration.Delivery.AvailableAt,
			notifications.WorkKindDelivery,
			migration.Delivery.TenantID,
			migration.Delivery.DeliveryID,
		),
		":relay_gsi_sk": migration.RelayIndexKey.SortKey,
	})
	if err != nil {
		return fmt.Errorf("encode notification delivery relay migration: %w", err)
	}
	_, err = store.Client.UpdateItem(ctx, &awssdk.UpdateItemInput{
		TableName:        aws.String(store.Table),
		Key:              key,
		UpdateExpression: aws.String("SET #relay_gsi_sk = :relay_gsi_sk"),
		ConditionExpression: aws.String(
			"#entity_type = :expected_entity_type AND #tenant_id = :expected_tenant_id AND " +
				"#outbox_id = :expected_outbox_id AND #delivery_id = :expected_delivery_id AND " +
				"#state = :expected_state AND #revision = :expected_revision AND " +
				"#relay_schema_version = :expected_relay_schema_version AND " +
				"#available_at = :expected_available_at AND #relay_work_kind = :expected_relay_work_kind AND " +
				"#relay_gsi_pk = :expected_relay_gsi_pk AND #relay_gsi_sk = :expected_relay_gsi_sk",
		),
		ExpressionAttributeNames: map[string]string{
			"#entity_type": "entity_type", "#tenant_id": "tenant_id", "#outbox_id": "outbox_id",
			"#delivery_id": "delivery_id", "#state": "state", "#revision": "delivery_revision",
			"#relay_schema_version": "relay_schema_version", "#available_at": "available_at",
			"#relay_work_kind": "relay_work_kind", "#relay_gsi_pk": "relay_gsi_pk",
			"#relay_gsi_sk": "relay_gsi_sk",
		},
		ExpressionAttributeValues: values,
	})
	if err != nil {
		return fmt.Errorf("update notification delivery relay fields: %w", err)
	}
	return nil
}
