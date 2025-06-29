package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) ListTablesWithContext(_ aws.Context, input *dynamodb.ListTablesInput, _ ...request.Option) (*dynamodb.ListTablesOutput, error) {
	return d.ListTables(input)
}

func (d *DB) ListTablesRequest(input *dynamodb.ListTablesInput) (*request.Request, *dynamodb.ListTablesOutput) {
	panic("not implemented: ListTablesRequest")
}

func (d *DB) ListTablesPages(input *dynamodb.ListTablesInput, processPage func(*dynamodb.ListTablesOutput, bool) bool) error {
	inputCopy := input
	for {
		output, err := d.ListTables(inputCopy)
		if err != nil {
			return err
		}
		lastPage := output.LastEvaluatedTableName == nil
		shouldContinue := processPage(output, lastPage)
		if lastPage || !shouldContinue {
			break
		}
		inputCopy.ExclusiveStartTableName = output.LastEvaluatedTableName
	}
	return nil
}

func (d *DB) ListTablesPagesWithContext(_ aws.Context, input *dynamodb.ListTablesInput, processPage func(*dynamodb.ListTablesOutput, bool) bool, _ ...request.Option) error {
	return d.ListTablesPages(input, processPage)
}
