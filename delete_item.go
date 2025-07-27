package fakedynamo

import (
	"errors"

	"github.com/DMRobertson/fakedynamo/conditionexpression"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) DeleteItem(input *dynamodb.DeleteItemInput) (*dynamodb.DeleteItemOutput, error) {
	var errs []error
	if input.Key == nil {
		errs = append(errs, newValidationError("Key is a required field"))
	}
	if input.TableName == nil {
		errs = append(errs, newValidationError("TableName is a required field"))
	}
	if input.ConditionalOperator != nil {
		errs = append(errs, errors.New("not implemented: ConditionalOperator (deprecated by DynamoDB)"))
	}
	if input.Expected != nil {
		errs = append(errs, errors.New("not implemented: Expected (deprecated by DynamoDB)"))
	}

	returnValues := valOr(input.ReturnValues, dynamodb.ReturnValueNone)
	switch returnValues {
	case dynamodb.ReturnValueNone:
	case dynamodb.ReturnValueAllOld:
	default:
		errs = append(errs, newValidationError("ReturnValues must be NONE or ALL_OLD for DeleteItem"))
	}
	returnValuesOnConditionCheckFailure := valOr(input.ReturnValuesOnConditionCheckFailure, dynamodb.ReturnValueNone)
	switch returnValuesOnConditionCheckFailure {
	case dynamodb.ReturnValueNone:
	case dynamodb.ReturnValueAllOld:
	default:
		errs = append(errs, newValidationError("ReturnValuesOnConditionCheckFailure must be NONE or ALL_OLD for DeleteItem"))
	}

	var condition *conditionexpression.Expression
	if input.ConditionExpression != nil {
		expr, err := conditionexpression.Parse(*input.ConditionExpression)
		if err != nil {
			errs = append(errs, newValidationErrorf("failed to parse ConditionExpression: %s", err))
		} else {
			condition = &expr
		}
	}

	println(condition)

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	t, exists := d.tables.Get(tableKey(*input.TableName))
	if !exists {
		return nil, &dynamodb.ResourceNotFoundException{}
	}

	err := validateAvmapMatchesSchema(input.Key, t, "Key")
	if err != nil {
		return nil, err
	}

	previous, exists := t.records.Get(input.Key)
	// Test condition expr if needed

	_, _ = t.records.Delete(input.Key)
	// Unless you specify conditions, the DeleteItem is an idempotent operation;
	// running it multiple times on the same item or attribute does not result
	// in an error response.

	return &dynamodb.DeleteItemOutput{
		Attributes: previous,
	}, nil
}

func (d *DB) DeleteItemWithContext(_ aws.Context, input *dynamodb.DeleteItemInput, _ ...request.Option) (*dynamodb.DeleteItemOutput, error) {
	return d.DeleteItem(input)
}

func (d *DB) DeleteItemRequest(_ *dynamodb.DeleteItemInput) (*request.Request, *dynamodb.DeleteItemOutput) {
	panic("not implemented: DeleteItemRequest")
}
