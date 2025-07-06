package fakedynamo

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
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
	returnValuesOnConditionCheckFailure := valOr(input.ReturnValues, dynamodb.ReturnValueNone)
	switch returnValuesOnConditionCheckFailure {
	case dynamodb.ReturnValueNone:
	case dynamodb.ReturnValueAllOld:
	default:
		errs = append(errs, newValidationError("ReturnValuesOnConditionCheckFailure must be NONE or ALL_OLD for PutItem"))
	}

	// TODO: parse condition expression
	// TODO: check condition expression before write

	var t table
	var exists bool
	if input.TableName == nil {
		errs = append(errs, newValidationError("TableName is a required field"))
		return nil, errors.Join(errs...)
	} else if t, exists = d.tables.Get(tableKey(*input.TableName)); !exists {
		errs = append(errs, &dynamodb.ResourceNotFoundException{})
	}

	errs = append(errs, d.validateItemMatchesSchema(input.Item, t))
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	var output dynamodb.PutItemOutput
	previous, replaced := t.records.ReplaceOrInsert(input.Item)
	if replaced && returnValues == dynamodb.ReturnValueAllOld {
		output.Attributes = previous
	}

	return &output, nil
}

func (d *DB) validateItemMatchesSchema(item avmap, t table) error {
	var errs []error

	partitionName := t.schema.partition
	_, exists := item[partitionName]
	if !exists {
		errs = append(errs, newValidationErrorf("Item does not define required key %s", partitionName))
	}
	if sortName := t.schema.sort; sortName != "" {
		_, exists = item[sortName]
		if !exists {
			errs = append(errs, newValidationErrorf("Item does not define required key %s", sortName))
		}
	}

	for attrName, definedType := range t.schema.types {
		if attrVal := item[attrName]; attrVal != nil {
			switch definedType {
			case dynamodb.ScalarAttributeTypeS:
				if attrVal.S == nil {
					errs = append(errs, newValidationErrorf("Type mismatch for Item.%s: defined to have type S", attrName))
				} else if len(*attrVal.S) == 0 {
					errs = append(errs, newValidationErrorf("Item.%s.S cannot be empty", attrName))
				}
			case dynamodb.ScalarAttributeTypeB:
				if attrVal.B == nil {
					errs = append(errs, newValidationErrorf("Type mismatch for Item.%s: defined to have type B", attrName))
				} else if len(attrVal.B) == 0 {
					errs = append(errs, newValidationErrorf("Item.%s.B cannot be empty", attrName))
				}
			case dynamodb.ScalarAttributeTypeN:
				if attrVal.N == nil {
					errs = append(errs, newValidationErrorf("Type mismatch for Item.%s: defined to have type N", attrName))
				} else if len(*attrVal.N) == 0 {
					errs = append(errs, newValidationErrorf("Item.%s.N cannot be empty", attrName))
				}
			}
		}
	}
	return errors.Join(errs...)
}

func validatePutItemInputMap(item avmap, fieldPath string) error {
	if item == nil {
		return newValidationErrorf("Item%s is a required field", fieldPath)
	}

	var errs []error
	for key, element := range item {
		if len(key) > 65535 {
			errs = append(errs, newValidationErrorf(
				"Item%s.%s(...) key too large, max 65535 characters", fieldPath, key[:100]))
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
	// TODO: validate uniqueness of items in sets, and nonemptyness

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
