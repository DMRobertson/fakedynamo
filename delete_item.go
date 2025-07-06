package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) DeleteItemWithContext(_ aws.Context, input *dynamodb.DeleteItemInput, _ ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	return d.DeleteItem(input)
}

func (d *DB) DeleteItemRequest(_ *dynamodb.DeleteItemInput) (*request.Request, *dynamodb.DeleteItemOutput) {
	panic("not implemented: DeleteItemRequest")
}
