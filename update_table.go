package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) UpdateTable(input *dynamodb.UpdateTableInput) (*dynamodb.UpdateTableOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) UpdateTableWithContext(context aws.Context, input *dynamodb.UpdateTableInput, option ...request.Option) (*dynamodb.UpdateTableOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) UpdateTableRequest(input *dynamodb.UpdateTableInput) (*request.Request, *dynamodb.UpdateTableOutput) {
	// TODO implement me
	panic("implement me")
}
