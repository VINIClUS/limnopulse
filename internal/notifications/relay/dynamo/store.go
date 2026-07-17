package dynamo

import (
	"context"

	"github.com/VINIClUS/limnopulse/internal/notifications"
	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

const RelayIndex = "NotificationRelayByAvailableAt"

type Client interface {
	Query(context.Context, *awssdk.QueryInput, ...func(*awssdk.Options)) (*awssdk.QueryOutput, error)
	GetItem(context.Context, *awssdk.GetItemInput, ...func(*awssdk.Options)) (*awssdk.GetItemOutput, error)
	UpdateItem(context.Context, *awssdk.UpdateItemInput, ...func(*awssdk.Options)) (*awssdk.UpdateItemOutput, error)
	TransactWriteItems(context.Context, *awssdk.TransactWriteItemsInput, ...func(*awssdk.Options)) (*awssdk.TransactWriteItemsOutput, error)
}

type Store struct {
	Table    string
	Client   Client
	Renderer *notifications.TemplateRenderer
}
