package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) TransactWriteItems(input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) TransactWriteItemsWithContext(_ aws.Context, input *dynamodb.TransactWriteItemsInput, _ ...request.Option) (*dynamodb.TransactWriteItemsOutput, error) {
	return d.TransactWriteItems(input)
}

func (d *DB) TransactWriteItemsRequest(_ *dynamodb.TransactWriteItemsInput) (*request.Request, *dynamodb.TransactWriteItemsOutput) {
	panic("not implemented: TransactWriteItemsRequest")
}
