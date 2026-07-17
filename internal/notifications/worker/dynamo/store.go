package dynamo

import (
	"context"

	awssdk "github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Client interface {
	GetItem(context.Context, *awssdk.GetItemInput, ...func(*awssdk.Options)) (*awssdk.GetItemOutput, error)
	UpdateItem(context.Context, *awssdk.UpdateItemInput, ...func(*awssdk.Options)) (*awssdk.UpdateItemOutput, error)
	TransactWriteItems(context.Context, *awssdk.TransactWriteItemsInput, ...func(*awssdk.Options)) (*awssdk.TransactWriteItemsOutput, error)
}

type Store struct {
	Table  string
	Client Client
}
