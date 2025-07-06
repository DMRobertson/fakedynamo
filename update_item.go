package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) UpdateItem(input *dynamodb.UpdateItemInput) (*dynamodb.UpdateItemOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) UpdateItemWithContext(context aws.Context, input *dynamodb.UpdateItemInput, option ...request.Option) (*dynamodb.UpdateItemOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) UpdateItemRequest(input *dynamodb.UpdateItemInput) (*request.Request, *dynamodb.UpdateItemOutput) {
	// TODO implement me
	panic("implement me")
}
