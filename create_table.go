package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) CreateTable(input *dynamodb.CreateTableInput) (*dynamodb.CreateTableOutput, error) {
	if input.AttributeDefinitions == nil {
		return nil, newValidationException("AttributeDefinitions is a required field")
	}

	if input.TableName == nil {
		return nil, newValidationException("TableName is a required field")
	} else if len(*input.TableName) < 3 || len(*input.TableName) > 255 {
		return nil, newValidationException("TableName must be between 3 and 255 characters")
	}

	if _, exists := d.tables[*input.TableName]; exists {
		return nil, &dynamodb.ResourceInUseException{}
	}
	d.tables[*input.TableName] = table{}

	return &dynamodb.CreateTableOutput{}, nil
}

func (d *DB) CreateTableWithContext(_ aws.Context, input *dynamodb.CreateTableInput, _ ...request.Option) (*dynamodb.CreateTableOutput, error) {
	return d.CreateTable(input)
}

func (d *DB) CreateTableRequest(_ *dynamodb.CreateTableInput) (*request.Request, *dynamodb.CreateTableOutput) {
	panic("not implemented: CreateTableRequest")
}
