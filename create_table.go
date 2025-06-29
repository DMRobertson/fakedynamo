package fakedynamo

import (
	"errors"
	"slices"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	errs := []error{
		validateCreateTableInputAttributeDefinitions(input.AttributeDefinitions),
		validateCreateTableInputKeySchema(input.KeySchema),
	}

	var schema *tableSchema
	if noErrors(errs) {
		var err error
		schema, err = parseTableSchema(input)
		errs = append(errs, err)
	}

	errs = append(errs, validateCreateTableInputTableName(input.TableName))
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	if d.tables.Has(tableKey(*input.TableName)) {
		return nil, &dynamodb.ResourceInUseException{}
	}
	_, _ = d.tables.ReplaceOrInsert(table{
		spec:      input,
		createdAt: time.Now().UTC(),
		schema:    *schema,
	})
	return &dynamodb.CreateTableOutput{
		TableDescription: d.describeTable(*input.TableName),
	}, nil
}

var validAttributeTypes = []string{
	dynamodb.ScalarAttributeTypeS,
	dynamodb.ScalarAttributeTypeN,
	dynamodb.ScalarAttributeTypeB,
}

func validateCreateTableInputAttributeDefinitions(input []*dynamodb.AttributeDefinition) error {
	if input == nil {
		return newValidationError("AttributeDefinitions is a required field")
	}

	var errs []error
	for i, attr := range input {
		if attr == nil {
			errs = append(errs, newValidationErrorf("AttributeDefinitions[%d] is nil", i))
			continue
		}

		if attr.AttributeName == nil {
			errs = append(errs, newValidationErrorf("AttributeDefinitions[%d].AttributeName is a required field", i))
		} else if len(*attr.AttributeName) < 1 || len(*attr.AttributeName) > 255 {
			errs = append(errs, newValidationErrorf("AttributeDefinitions[%d].AttributeName must be between 1 and 255 characters", i))
		}

		if attr.AttributeType == nil {
			errs = append(errs, newValidationErrorf("AttributeDefinitions[%d].AttributeType is a required field", i))
		} else if !slices.Contains(validAttributeTypes, *attr.AttributeType) {
			errs = append(errs, newValidationErrorf(`AttributeDefinitions[%d].AttributeType must be one of %v`, i, validAttributeTypes))
		}
	}
	return errors.Join(errs...)
}

func validateCreateTableInputKeySchema(input []*dynamodb.KeySchemaElement) error {
	if input == nil {
		return newValidationError("KeySchema is a required field")
	} else if len(input) == 0 || len(input) > 2 {
		return newValidationError("KeySchema must contain 1 or 2 items")
	}

	var errs []error

	if input[0] == nil {
		errs = append(errs, newValidationErrorf("KeySchema[0] is nil"))
	} else {
		if val(input[0].KeyType) != dynamodb.KeyTypeHash {
			errs = append(errs, newValidationError("KeySchema[0] must have type HASH"))
		}
		if len(val(input[0].AttributeName)) == 0 {
			errs = append(errs, newValidationError("KeySchema[0] has no AttributeName"))
		}
	}

	if len(input) > 1 {
		if input[1] == nil {
			errs = append(errs, newValidationErrorf("KeySchema[1] is nil"))
		} else {
			if val(input[1].KeyType) != dynamodb.KeyTypeRange {
				errs = append(errs, newValidationError("KeySchema[1] must have type RANGE"))
			}
			if len(val(input[1].AttributeName)) == 0 {
				errs = append(errs, newValidationError("KeySchema[1] has no AttributeName"))
			}
		}
	}

	return errors.Join(errs...)
}

func validateCreateTableInputTableName(input *string) error {
	if input == nil {
		return newValidationError("TableName is a required field")
	} else if len(*input) < 1 || len(*input) > 1024 {
		return newValidationError("TableName must be between 1 and 1024 characters")
	}
	return nil
}

func parseTableSchema(input *dynamodb.CreateTableInput) (*tableSchema, error) {
	attrTypes := make(map[string]string)

	for _, attr := range input.AttributeDefinitions {
		attrTypes[*attr.AttributeName] = *attr.AttributeType
	}

	var errs []error

	partitionAttrName := *input.KeySchema[0].AttributeName
	_, exists := attrTypes[partitionAttrName]
	if !exists {
		errs = append(errs, newValidationErrorf("%s is missing from AttributeDefinitions", partitionAttrName))
	}

	var sortAttrName string
	if len(input.KeySchema) > 1 {
		sortAttrName = *input.KeySchema[1].AttributeName
		_, exists := attrTypes[sortAttrName]
		if !exists {
			errs = append(errs, newValidationErrorf("%s is missing from AttributeDefinitions", sortAttrName))
		}
	}

	if err := errors.Join(errs...); err != nil {
		return nil, err
	}

	schema := tableSchema{
		partition: partitionAttrName,
		sort:      sortAttrName,
		types:     attrTypes,
	}

	return &schema, nil
}

func (d *DB) CreateTableWithContext(_ aws.Context, input *dynamodb.CreateTableInput, _ ...request.Option) (*dynamodb.CreateTableOutput, error) {
	return d.CreateTable(input)
}

func (d *DB) CreateTableRequest(_ *dynamodb.CreateTableInput) (*request.Request, *dynamodb.CreateTableOutput) {
	panic("not implemented: CreateTableRequest")
}
