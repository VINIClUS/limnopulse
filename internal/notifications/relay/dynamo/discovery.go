package dynamo

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	"github.com/VINIClUS/limnopulse/internal/notifications/relay"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const fixedUTCLayout = "2006-01-02T15:04:05.000000000Z"

func (store Store) QueryDue(ctx context.Context, request relay.DueRequest) (relay.DuePage, error) {
	if store.Client == nil || strings.TrimSpace(store.Table) == "" {
		return relay.DuePage{}, fmt.Errorf("relay DynamoDB store is not configured")
	}
	if request.Bucket < 0 || request.Bucket >= notifications.RelayBucketCount ||
		request.DueThrough.IsZero() || request.PageSize < 1 {
		return relay.DuePage{}, fmt.Errorf("invalid relay due query")
	}
	values, err := attributevalue.MarshalMap(map[string]string{
		":pk":  fmt.Sprintf("NOTIFICATION_RELAY#V1#BUCKET#%02d", request.Bucket),
		":due": request.DueThrough.UTC().Format(fixedUTCLayout) + "#~",
	})
	if err != nil {
		return relay.DuePage{}, fmt.Errorf("marshal relay query: %w", err)
	}
	input := &awssdk.QueryInput{
		TableName:              aws.String(store.Table),
		IndexName:              aws.String(RelayIndex),
		KeyConditionExpression: aws.String("#relay_pk = :pk AND #relay_sk <= :due"),
		ExpressionAttributeNames: map[string]string{
			"#relay_pk": "relay_gsi_pk",
			"#relay_sk": "relay_gsi_sk",
		},
		ExpressionAttributeValues: values,
		Limit:                     aws.Int32(int32(request.PageSize)),
		ConsistentRead:            aws.Bool(false),
	}
	if request.NextToken != "" {
		input.ExclusiveStartKey, err = decodePageToken(request.NextToken)
		if err != nil {
			return relay.DuePage{}, fmt.Errorf("decode relay page token: %w", err)
		}
	}
	output, err := store.Client.Query(ctx, input)
	if err != nil {
		return relay.DuePage{}, fmt.Errorf("query relay index: %w", err)
	}
	page := relay.DuePage{Candidates: make([]relay.Candidate, 0, len(output.Items))}
	for _, item := range output.Items {
		candidate, decodeErr := candidateFromItem(item)
		if decodeErr != nil {
			return relay.DuePage{}, fmt.Errorf("decode relay candidate: %w", decodeErr)
		}
		page.Candidates = append(page.Candidates, candidate)
	}
	if len(output.LastEvaluatedKey) != 0 {
		page.NextToken, err = encodePageToken(output.LastEvaluatedKey)
		if err != nil {
			return relay.DuePage{}, fmt.Errorf("encode relay page token: %w", err)
		}
	}
	return page, nil
}

func candidateFromItem(item map[string]types.AttributeValue) (relay.Candidate, error) {
	var raw struct {
		PK      string `dynamodbav:"PK"`
		SK      string `dynamodbav:"SK"`
		RelayPK string `dynamodbav:"relay_gsi_pk"`
		RelaySK string `dynamodbav:"relay_gsi_sk"`
	}
	if err := attributevalue.UnmarshalMap(item, &raw); err != nil {
		return relay.Candidate{}, err
	}
	parts := strings.SplitN(raw.RelaySK, "#", 4)
	if raw.PK == "" || raw.SK == "" || raw.RelayPK == "" || len(parts) != 4 {
		return relay.Candidate{}, fmt.Errorf("candidate is missing relay identity")
	}
	availableAt, err := time.Parse(fixedUTCLayout, parts[0])
	if err != nil {
		return relay.Candidate{}, fmt.Errorf("available time: %w", err)
	}
	kind := notifications.WorkKind(parts[1])
	if err := kind.Validate(); err != nil {
		return relay.Candidate{}, err
	}
	return relay.Candidate{
		PK: raw.PK, SK: raw.SK, RelayPK: raw.RelayPK, RelaySK: raw.RelaySK,
		Kind: kind, AvailableAt: availableAt,
	}, nil
}

type pageToken struct {
	PK      string `json:"pk"`
	SK      string `json:"sk"`
	RelayPK string `json:"relay_pk"`
	RelaySK string `json:"relay_sk"`
}

func encodePageToken(key map[string]types.AttributeValue) (string, error) {
	var decoded struct {
		PK      string `dynamodbav:"PK"`
		SK      string `dynamodbav:"SK"`
		RelayPK string `dynamodbav:"relay_gsi_pk"`
		RelaySK string `dynamodbav:"relay_gsi_sk"`
	}
	if err := attributevalue.UnmarshalMap(key, &decoded); err != nil {
		return "", err
	}
	if decoded.PK == "" || decoded.SK == "" || decoded.RelayPK == "" || decoded.RelaySK == "" {
		return "", fmt.Errorf("relay page key is incomplete")
	}
	encoded, err := json.Marshal(pageToken{
		PK: decoded.PK, SK: decoded.SK, RelayPK: decoded.RelayPK, RelaySK: decoded.RelaySK,
	})
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

func decodePageToken(token string) (map[string]types.AttributeValue, error) {
	encoded, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, err
	}
	var decoded pageToken
	if err := json.Unmarshal(encoded, &decoded); err != nil {
		return nil, err
	}
	if decoded.PK == "" || decoded.SK == "" || decoded.RelayPK == "" || decoded.RelaySK == "" {
		return nil, fmt.Errorf("relay page token is incomplete")
	}
	return attributevalue.MarshalMap(map[string]string{
		"PK": decoded.PK, "SK": decoded.SK,
		"relay_gsi_pk": decoded.RelayPK, "relay_gsi_sk": decoded.RelaySK,
	})
}
