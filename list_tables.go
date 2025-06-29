package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) ListTables(input *dynamodb.ListTablesInput) (*dynamodb.ListTablesOutput, error) {
	if input.Limit != nil && (*input.Limit < 1 || *input.Limit > 100) {
		return nil, newValidationError("Limit must be between 1 and 100")
	}

	inputCopy := *input
	if input.Limit == nil {
		inputCopy.Limit = ptr[int64](100)
	}

	start := tableKey(val(input.ExclusiveStartTableName))
	output := dynamodb.ListTablesOutput{
		TableNames: []*string{},
	}
	d.tables.AscendGreaterOrEqual(start, func(t table) bool {
		if *t.spec.TableName == *start.spec.TableName {
			// Ignore the previous ExclusiveStartTableName
			return true
		}
		output.TableNames = append(output.TableNames, t.spec.TableName)
		return len(output.TableNames) < int(*inputCopy.Limit)
	})

	if len(output.TableNames) == 0 {
		return &output, nil
	}

	lastTableName := output.TableNames[len(output.TableNames)-1]
	biggest, exists := d.tables.Max()
	if exists && tableLess(tableKey(*lastTableName), biggest) {
		output.LastEvaluatedTableName = lastTableName
	}

	return &output, nil
}

func (d *DB) ListTablesWithContext(_ aws.Context, input *dynamodb.ListTablesInput, _ ...request.Option) (*dynamodb.ListTablesOutput, error) {
	return d.ListTables(input)
}

func (d *DB) ListTablesRequest(_ *dynamodb.ListTablesInput) (*request.Request, *dynamodb.ListTablesOutput) {
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
