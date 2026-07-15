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
}

type BackfillSummary struct {
	EvaluationTime time.Time `json:"evaluation_time"`
	DryRun         bool      `json:"dry_run"`
	Tenants        int       `json:"tenants"`
	Queried        int       `json:"rules_queried"`
	Eligible       int       `json:"rules_eligible"`
	Updated        int       `json:"rules_updated"`
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
		":gsi_pk": fmt.Sprintf("ALERT_EVALUATION#V1#BUCKET#%02d", bucket),
		":gsi_sk": dueText + "#TENANT#" + tenantID + "#RULE#" + ruleID,
		":true":   true, ":active": "active",
	})
	_, err := store.Client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(store.Table), Key: key,
		UpdateExpression:    aws.String("SET #bucket = :bucket, #next_due = :next_due, #gsi_pk = :gsi_pk, #gsi_sk = :gsi_sk"),
		ConditionExpression: aws.String("#enabled = :true AND #status = :active"),
		ExpressionAttributeNames: map[string]string{
			"#bucket": "evaluation_bucket", "#next_due": "next_evaluation_at",
			"#gsi_pk": "GSI1PK", "#gsi_sk": "GSI1SK", "#enabled": "enabled", "#status": "status",
		}, ExpressionAttributeValues: values,
	})
	if err != nil {
		return fmt.Errorf("backfill schedule for %s/%s: %w", tenantID, ruleID, err)
	}
	return nil
}
