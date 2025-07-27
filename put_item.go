package fakedynamo

import (
	"errors"
	"fmt"

	"github.com/DMRobertson/fakedynamo/conditionexpression"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	if input.Expected != nil {
		return nil, errors.New("not implemented: PutItemInput.Expected (deprecated by DynamoDB)")
	} else if input.ConditionalOperator != nil {
		return nil, errors.New("not implemented: PutItemInput.ConditionalOperator (deprecated by DynamoDB)")
	}

	var errs []error
	if err := validatePutItemInputMap(input.Item, ""); err != nil {
		errs = append(errs, err)
	}

	returnValues := valOr(input.ReturnValues, dynamodb.ReturnValueNone)
	switch returnValues {
	case dynamodb.ReturnValueNone:
	case dynamodb.ReturnValueAllOld:
	default:
		errs = append(errs, newValidationError("ReturnValues must be NONE or ALL_OLD for PutItem"))
	}
	returnValuesOnConditionCheckFailure := valOr(input.ReturnValuesOnConditionCheckFailure, dynamodb.ReturnValueNone)
	switch returnValuesOnConditionCheckFailure {
	case dynamodb.ReturnValueNone:
	case dynamodb.ReturnValueAllOld:
	default:
		errs = append(errs, newValidationError("ReturnValuesOnConditionCheckFailure must be NONE or ALL_OLD for PutItem"))
	}

	var conditionexpr *conditionexpression.Expression
	if input.ConditionExpression != nil {
		expr, err := conditionexpression.Parse(*input.ConditionExpression)
		if err != nil {
			return nil, fmt.Errorf("failed to parse condition expression: %w", err)
		}
		conditionexpr = &expr
	}

	var t table
	var exists bool
	if input.TableName == nil {
		errs = append(errs, newValidationError("TableName is a required field"))
		return nil, errors.Join(errs...)
	}

	d.mu.RLock()
	defer d.mu.RUnlock()
	if t, exists = d.tables.Get(tableKey(*input.TableName)); !exists {
		errs = append(errs, &dynamodb.ResourceNotFoundException{})
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	pVal, err := validateAvmapMatchesSchema(input.Item, t, "Item")
	if err != nil {
		return nil, err
	}
	var existing avmap
	partition := t.getPartition(pVal)
	existing, _ = partition.Get(input.Item)

	if conditionexpr != nil {
		result, err := conditionexpr.Evaluate(existing, input.ExpressionAttributeNames, input.ExpressionAttributeValues)
		if err != nil {
			return nil, fmt.Errorf("error evaluating condition expression: %w", err)
		}
		if !result {
			checkErr := &dynamodb.ConditionalCheckFailedException{}
			if returnValuesOnConditionCheckFailure == dynamodb.ReturnValueAllOld {
				checkErr.Item = existing
			}
			return nil, checkErr
		}
	}

	output := &dynamodb.PutItemOutput{}
	previous, replaced := partition.ReplaceOrInsert(input.Item)
	if replaced && returnValues == dynamodb.ReturnValueAllOld {
		output.Attributes = previous
	}

	return output, nil
}

func validateKeyAttributeCount(key avmap, t table) error {
	if t.schema.sort == "" && len(key) != 1 {
		return newValidationError("must provide partition key only")
	}
	if t.schema.sort != "" && len(key) != 2 {
		return newValidationError("must provide partition and sort keys only")
	}

	return nil
}

func validateAvmapMatchesSchema(item avmap, t table, itemDesc string) (
	*dynamodb.AttributeValue, error,
) {
	var errs []error

	partitionName := t.schema.partition
	partitionVal, exists := item[partitionName]
	if !exists {
		errs = append(errs, newValidationErrorf("%s does not define required key %s", itemDesc, partitionName))
	}
	if sortName := t.schema.sort; sortName != "" {
		_, exists = item[sortName]
		if !exists {
			errs = append(errs, newValidationErrorf("%s does not define required key %s", itemDesc, sortName))
		}
	}

	for attrName, definedType := range t.schema.types {
		if attrVal := item[attrName]; attrVal != nil {
			err := checkAttributeType(definedType, attrVal)
			if err != nil {
				errs = append(errs, newValidationErrorf("%s.%s: %s", itemDesc, attrName, err.Error()))
			}
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return partitionVal, nil
}

func checkAttributeType(definedType string, attrVal *dynamodb.AttributeValue) error {
	switch definedType {
	case dynamodb.ScalarAttributeTypeS:
		if attrVal.S == nil {
			return errors.New("type mismatch, defined to have type S")
		} else if len(*attrVal.S) == 0 {
			return errors.New("cannot be empty string")
		}
	case dynamodb.ScalarAttributeTypeB:
		if attrVal.B == nil {
			return errors.New("type mismatch, defined to have type B")
		} else if len(attrVal.B) == 0 {
			return errors.New("cannot be empty binary string")
		}
	case dynamodb.ScalarAttributeTypeN:
		if attrVal.N == nil {
			return errors.New("type mismatch, defined to have type N")
		} else if len(*attrVal.N) == 0 {
			return errors.New("must be interpretable as a number")
		}
	}
	return nil
}

func validatePutItemInputMap(item avmap, fieldPath string) error {
	if item == nil {
		return newValidationErrorf("Item%s is a required field", fieldPath)
	}

	var errs []error
	for key, element := range item {
		if len(key) > 65535 {
			errs = append(errs, newValidationErrorf(
				"Item%s.%s(...) attribute name too large, must be less than 65536 characters", fieldPath, key[:100]))
		}
		err := validatePutItemInputAttributeValue(element, fmt.Sprintf("%s.%s", fieldPath, key))
		if err != nil {
			errs = append(errs, err)
		}
	}

	// TODO: check item size here too, see
	// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/CapacityUnitCalculations.html
	return errors.Join(errs...)
}

func validatePutItemInputAttributeValue(value *dynamodb.AttributeValue, fieldPath string) error {
	if value == nil {
		return newValidationErrorf("Item%s is nil", fieldPath)
	}

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
		return newValidationErrorf("Item%s must have exactly 1 data type specified", fieldPath)
	}

	// TODO: validate number format
	// TODO: validate uniqueness of items in sets, and nonemptiness

	if value.L != nil {
		var errs []error
		for i, element := range value.L {
			err := validatePutItemInputAttributeValue(element, fmt.Sprintf("%s[%d]", fieldPath, i))
			if err != nil {
				errs = append(errs, err)
			}
		}
		return errors.Join(errs...)
	}
	if value.M != nil {
		return validatePutItemInputMap(value.M, fieldPath)
	}
	return nil
}

func (d *DB) PutItemWithContext(_ aws.Context, input *dynamodb.PutItemInput, _ ...request.Option) (*dynamodb.PutItemOutput, error) {
	return d.PutItem(input)
}

func (d *DB) PutItemRequest(_ *dynamodb.PutItemInput) (*request.Request, *dynamodb.PutItemOutput) {
	panic("not implemented: PutItemRequest")
}
