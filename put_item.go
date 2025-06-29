package fakedynamo

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	var errs []error
	if err := validatePutItemInputItem(input.Item); err != nil {
		errs = append(errs, err)
	}

	var _ table
	var exists bool
	if input.TableName == nil {
		errs = append(errs, newValidationError("TableName is a required field"))
	} else if _, exists = d.tables.Get(tableKey(*input.TableName)); !exists {
		errs = append(errs, &dynamodb.ResourceNotFoundException{})
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	// partitionValue := val(input.Item[t.schema.partition])
	// TODO: ConditionalCheckFailedException

	return nil, nil
}

func validatePutItemInputItem(item avmap) error {
	if item == nil {
		return newValidationError("Item is a required field")
	}

	var errs []error
	for key, value := range item {
		if len(key) > 65535 {
			errs = append(errs, newValidationErrorf("Item.%s key too large, max 65535 characters", key[:100]))
		}
		if value == nil {
			errs = append(errs, newValidationErrorf("Item.%s is nil", key))
		} else {
			typesSet := toInt(value.B != nil) +
				toInt(value.BOOL != nil) +
				toInt(value.BS != nil) +
				toInt(value.L != nil) +
				toInt(value.M != nil) +
				toInt(value.N != nil) +
				toInt(value.NS != nil) +
				toInt(value.NULL != nil) +
				toInt(value.S != nil) +
				toInt(value.SS != nil)
			if typesSet != 1 {
				errs = append(errs, newValidationErrorf("Item.%s must have exactly 1 data type specified", key))
			}
		}
	}

	return errors.Join(errs...)
}

func (d *DB) PutItemWithContext(_ aws.Context, input *dynamodb.PutItemInput, _ ...request.Option) (*dynamodb.PutItemOutput, error) {
	return d.PutItem(input)
}

func (d *DB) PutItemRequest(input *dynamodb.PutItemInput) (*request.Request, *dynamodb.PutItemOutput) {
	panic("not implemented: PutItemRequest")
}
