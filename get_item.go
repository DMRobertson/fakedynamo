package fakedynamo

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) GetItem(input *dynamodb.GetItemInput) (*dynamodb.GetItemOutput, error) {
	var errs []error
	if input.Key == nil {
		errs = append(errs, newValidationError("Key is a required field"))
	}
	if input.TableName == nil {
		errs = append(errs, newValidationError("TableName is a required field"))
	}
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	t, exists := d.tables.Get(tableKey(*input.TableName))
	if !exists {
		return nil, &dynamodb.ResourceNotFoundException{}
	}

	if t.schema.sort == "" && len(input.Key) != 1 {
		return nil, newValidationError("must provide partition key only")
	}
	if t.schema.sort != "" && len(input.Key) != 2 {
		return nil, newValidationError("must provide partition and sort keys only")
	}

	err := d.validateItemMatchesSchema(input.Key, t)
	if err != nil {
		return nil, err
	}

	var output dynamodb.GetItemOutput
	record, exists := t.records.Get(input.Key)
	if exists {
		// TODO: projection expressions here
		output.Item = record
	}

	return &output, nil
}

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

func (d *DB) GetItemWithContext(_ aws.Context, input *dynamodb.GetItemInput, _ ...request.Option) (*dynamodb.GetItemOutput, error) {
	return d.GetItem(input)
}

func (d *DB) GetItemRequest(_ *dynamodb.GetItemInput) (*request.Request, *dynamodb.GetItemOutput) {
	panic("not implemented: GetItemRequest")
}
