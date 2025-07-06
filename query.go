package fakedynamo

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) Query(input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) QueryWithContext(_ aws.Context, input *dynamodb.QueryInput, _ ...request.Option) (*dynamodb.QueryOutput, error) {
	return d.Query(input)
}

func (d *DB) QueryRequest(input *dynamodb.QueryInput) (*request.Request, *dynamodb.QueryOutput) {
	panic("not implemented: QueryRequest")
}

func (d *DB) QueryPages(input *dynamodb.QueryInput, f func(*dynamodb.QueryOutput, bool) bool) error {
	// TODO implement me
	panic("implement me")
}

func (d *DB) QueryPagesWithContext(_ aws.Context, input *dynamodb.QueryInput, f func(*dynamodb.QueryOutput, bool) bool, _ ...request.Option) error {
	return d.QueryPages(input, f)
}
