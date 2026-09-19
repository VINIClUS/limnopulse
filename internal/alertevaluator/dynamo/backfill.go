package dynamo

import (
	"context"
	"fmt"
	"time"

	"github.com/VINIClUS/limnopulse/internal/alertevaluator"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type BackfillOptions struct {
	Tenants        []string
	EvaluationTime time.Time
	Apply          bool
	PageSize       int
	Limit          int
}

type BackfillSummary struct {
	EvaluationTime      time.Time `json:"evaluation_time"`
	DryRun              bool      `json:"dry_run"`
	Tenants             int       `json:"tenants"`
	Queried             int       `json:"rules_queried"`
	Eligible            int       `json:"rules_eligible"`
	Updated             int       `json:"rules_updated"`
	AlertEventsQueried  int       `json:"alert_events_queried,omitempty"`
	AlertEventsEligible int       `json:"alert_events_eligible,omitempty"`
	AlertEventsUpdated  int       `json:"alert_events_updated,omitempty"`
	AlertEventsSkipped  int       `json:"alert_events_skipped,omitempty"`
}

func (store Store) BackfillActiveAlertIndex(ctx context.Context, options BackfillOptions) (BackfillSummary, error) {
	if len(options.Tenants) == 0 {
		return BackfillSummary{}, fmt.Errorf("at least one explicit tenant is required")
	}
	if options.PageSize < 1 {
		return BackfillSummary{}, fmt.Errorf("page size must be positive")
	}
	if options.Limit < 0 {
		return BackfillSummary{}, fmt.Errorf("limit cannot be negative")
	}
	summary := BackfillSummary{EvaluationTime: time.Now().UTC(), DryRun: !options.Apply, Tenants: len(options.Tenants)}
	processed := 0
	for _, tenantID := range options.Tenants {
		if tenantID == "" {
			return summary, fmt.Errorf("tenant id cannot be empty")
		}
		var lastKey map[string]types.AttributeValue
		for {
			if options.Limit > 0 && processed >= options.Limit {
				return summary, nil
			}
			values, _ := attributevalue.MarshalMap(map[string]string{":pk": "TENANT#" + tenantID, ":prefix": "ALERT_EVENT#"})
			output, err := store.Client.Query(ctx, &dynamodb.QueryInput{
				TableName:                 aws.String(store.Table),
				KeyConditionExpression:    aws.String("PK = :pk AND begins_with(SK, :prefix)"),
				ExpressionAttributeValues: values, Limit: aws.Int32(int32(options.PageSize)),
				ExclusiveStartKey: lastKey, ConsistentRead: aws.Bool(true),
			})
			if err != nil {
				return summary, fmt.Errorf("query alert events for tenant %s: %w", tenantID, err)
			}
			for _, item := range output.Items {
				if options.Limit > 0 && processed >= options.Limit {
					return summary, nil
				}
				processed++
				summary.AlertEventsQueried++
				var event struct {
					EventID  string `dynamodbav:"event_id"`
					TenantID string `dynamodbav:"tenant_id"`
					Status   string `dynamodbav:"status"`
					OpenedAt string `dynamodbav:"opened_at"`
				}
				if err := attributevalue.UnmarshalMap(item, &event); err != nil {
					return summary, fmt.Errorf("decode alert event during backfill: %w", err)
				}
				if event.Status != string(alertevaluator.StatusOpen) && event.Status != string(alertevaluator.StatusAcknowledged) {
					continue
				}
				summary.AlertEventsEligible++
				if !options.Apply {
					continue
				}
				updated, err := store.updateActiveAlertIndex(ctx, event.TenantID, event.EventID, event.OpenedAt)
				if err != nil {
					return summary, err
				}
				if updated {
					summary.AlertEventsUpdated++
				} else {
					summary.AlertEventsSkipped++
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

func (store Store) updateActiveAlertIndex(ctx context.Context, tenantID, eventID, openedAt string) (bool, error) {
	key, err := attributevalue.MarshalMap(map[string]string{"PK": "TENANT#" + tenantID, "SK": "ALERT_EVENT#" + eventID})
	if err != nil {
		return false, err
	}
	values, err := attributevalue.MarshalMap(map[string]string{
		":gsi_pk": "TENANT#" + tenantID + "#ACTIVE_ALERT_EVENTS", ":gsi_sk": openedAt + "#EVENT#" + eventID,
		":open": string(alertevaluator.StatusOpen), ":acknowledged": string(alertevaluator.StatusAcknowledged),
	})
	if err != nil {
		return false, err
	}
	_, err = store.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(store.Table), Key: key,
		UpdateExpression:          aws.String("SET #gsi_pk = :gsi_pk, #gsi_sk = :gsi_sk"),
		ConditionExpression:       aws.String("#status IN (:open, :acknowledged) AND attribute_not_exists(#gsi_pk)"),
		ExpressionAttributeNames:  map[string]string{"#gsi_pk": "GSI3PK", "#gsi_sk": "GSI3SK", "#status": "status"},
		ExpressionAttributeValues: values,
	})
	if err != nil {
		if isConditional(err) {
			return false, nil
		}
		return false, fmt.Errorf("backfill active alert index for %s/%s: %w", tenantID, eventID, err)
	}
	return true, nil
}

func (store Store) BackfillSchedule(ctx context.Context, options BackfillOptions) (BackfillSummary, error) {
	if len(options.Tenants) == 0 {
		return BackfillSummary{}, fmt.Errorf("at least one explicit tenant is required")
	}
	if options.PageSize < 1 {
		return BackfillSummary{}, fmt.Errorf("page size must be positive")
	}
	if options.EvaluationTime.IsZero() {
		options.EvaluationTime = time.Now().UTC()
	}
	summary := BackfillSummary{
		EvaluationTime: options.EvaluationTime.UTC(), DryRun: !options.Apply, Tenants: len(options.Tenants),
	}
	for _, tenantID := range options.Tenants {
		if tenantID == "" {
			return summary, fmt.Errorf("tenant id cannot be empty")
		}
		var lastKey map[string]types.AttributeValue
		for {
			values, _ := attributevalue.MarshalMap(map[string]string{
				":pk": "TENANT#" + tenantID, ":prefix": "ALERT_RULE#",
			})
			input := &dynamodb.QueryInput{
				TableName:                 aws.String(store.Table),
				KeyConditionExpression:    aws.String("PK = :pk AND begins_with(SK, :prefix)"),
				ExpressionAttributeValues: values, Limit: aws.Int32(int32(options.PageSize)),
				ExclusiveStartKey: lastKey, ConsistentRead: aws.Bool(true),
			}
			output, err := store.Client.Query(ctx, input)
			if err != nil {
				return summary, fmt.Errorf("query alert rules for tenant %s: %w", tenantID, err)
			}
			for _, item := range output.Items {
				summary.Queried++
				var rule struct {
					PK      string `dynamodbav:"PK"`
					SK      string `dynamodbav:"SK"`
					RuleID  string `dynamodbav:"rule_id"`
					Enabled bool   `dynamodbav:"enabled"`
					Status  string `dynamodbav:"status"`
				}
				if err := attributevalue.UnmarshalMap(item, &rule); err != nil {
					return summary, fmt.Errorf("decode alert rule during backfill: %w", err)
				}
				if !rule.Enabled || rule.Status != "active" {
					continue
				}
				summary.Eligible++
				if !options.Apply {
					continue
				}
				if err := store.updateSchedule(ctx, rule.PK, rule.SK, tenantID, rule.RuleID, options.EvaluationTime); err != nil {
					return summary, err
				}
				summary.Updated++
			}
			lastKey = output.LastEvaluatedKey
			if len(lastKey) == 0 {
				break
			}
		}
	}
	return summary, nil
}

func (store Store) updateSchedule(ctx context.Context, pk, sk, tenantID, ruleID string, evaluationTime time.Time) error {
	bucket := alertevaluator.EvaluationBucket(tenantID, ruleID)
	due := alertevaluator.NextCompleteSlot(evaluationTime, alertevaluator.EvaluationCadence, 15*time.Second)
	dueText := alertevaluator.FixedUTCTimestamp(due)
	key, _ := attributevalue.MarshalMap(map[string]string{"PK": pk, "SK": sk})
	values, _ := attributevalue.MarshalMap(map[string]any{
		":bucket": bucket, ":next_due": dueText,
		":gsi_pk":           fmt.Sprintf("ALERT_EVALUATION#V1#BUCKET#%02d", bucket),
		":gsi_sk":           dueText + "#TENANT#" + tenantID + "#RULE#" + ruleID,
		":initial_revision": 1, ":true": true, ":active": "active",
	})
	_, err := store.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(store.Table), Key: key,
		UpdateExpression: aws.String(
			"SET #bucket = :bucket, #next_due = :next_due, #gsi_pk = :gsi_pk, #gsi_sk = :gsi_sk, " +
				"#evaluation_revision = if_not_exists(#evaluation_revision, :initial_revision)",
		),
		ConditionExpression: aws.String("#enabled = :true AND #status = :active"),
		ExpressionAttributeNames: map[string]string{
			"#bucket": "evaluation_bucket", "#next_due": "next_evaluation_at",
			"#gsi_pk": "GSI1PK", "#gsi_sk": "GSI1SK", "#enabled": "enabled", "#status": "status",
			"#evaluation_revision": "evaluation_revision",
		}, ExpressionAttributeValues: values,
	})
	if err != nil {
		return fmt.Errorf("backfill schedule for %s/%s: %w", tenantID, ruleID, err)
	}
	return nil
}
