package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) DeleteTable(input *dynamodb.DeleteTableInput) (*dynamodb.DeleteTableOutput, error) {
	if input.TableName == nil {
		return nil, newValidationError("TableName is a required field")
	}
	d.mu.Lock()
	defer d.mu.Unlock()

	desc := d.describeTable(*input.TableName)
	if desc == nil {
		return nil, &dynamodb.ResourceNotFoundException{}
	}

	desc.TableStatus = ptr(dynamodb.TableStatusDeleting)

	_, _ = d.tables.Delete(tableKey(*input.TableName))
	return &dynamodb.DeleteTableOutput{
		TableDescription: desc,
	}, nil
}

func (d *DB) DeleteTableWithContext(_ aws.Context, input *dynamodb.DeleteTableInput, _ ...request.Option) (*dynamodb.DeleteTableOutput, error) {
	return d.DeleteTable(input)
}

func (d *DB) DeleteTableRequest(input *dynamodb.DeleteTableInput) (*request.Request, *dynamodb.DeleteTableOutput) {
	panic("not implemented: DeleteTableRequest")
}
