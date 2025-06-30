package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) BatchGetItem(input *dynamodb.BatchGetItemInput) (*dynamodb.BatchGetItemOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) BatchGetItemWithContext(_ aws.Context, input *dynamodb.BatchGetItemInput, _ ...request.Option) (*dynamodb.BatchGetItemOutput, error) {
	return d.BatchGetItem(input)
}

func (d *DB) BatchGetItemRequest(input *dynamodb.BatchGetItemInput) (*request.Request, *dynamodb.BatchGetItemOutput) {
	panic("not implemented: BatchGetItemRequest")
}

func (d *DB) BatchGetItemPages(input *dynamodb.BatchGetItemInput, f func(*dynamodb.BatchGetItemOutput, bool) bool) error {
	// TODO implement me
	panic("implement me")
}

func (d *DB) BatchGetItemPagesWithContext(_ aws.Context, input *dynamodb.BatchGetItemInput, f func(*dynamodb.BatchGetItemOutput, bool) bool, _ ...request.Option) error {
	return d.BatchGetItemPages(input, f)
}

func (d *DB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) GetItemWithContext(_ aws.Context, input *dynamodb.GetItemInput, _ ...request.Option) (*dynamodb.GetItemOutput, error) {
	return d.GetItem(input)
}

func (d *DB) GetItemRequest(_ *dynamodb.GetItemInput) (*request.Request, *dynamodb.GetItemOutput) {
	panic("not implemented: GetItemRequest")
}
