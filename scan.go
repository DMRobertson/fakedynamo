package fakedynamo

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return nil, errors.New("not implemented lol")
}

func (d *DB) ScanWithContext(_ aws.Context, input *dynamodb.ScanInput, _ ...request.Option) (*dynamodb.ScanOutput, error) {
	return d.Scan(input)
}

func (d *DB) ScanRequest(_ *dynamodb.ScanInput) (*request.Request, *dynamodb.ScanOutput) {
	panic("not implemented: ScanRequest")
}

func (d *DB) ScanPages(input *dynamodb.ScanInput, processPage func(*dynamodb.ScanOutput, bool) bool) error {
	input = shallowCopy(input)
	for {
		output, err := d.Scan(input)
		if err != nil {
			return err
		}
		lastPage := output.LastEvaluatedKey == nil
		shouldContinue := processPage(output, lastPage)
		if lastPage || !shouldContinue {
			break
		}
		input.ExclusiveStartKey = output.LastEvaluatedKey
	}
	return nil
}

func (d *DB) ScanPagesWithContext(_ aws.Context, input *dynamodb.ScanInput, f func(*dynamodb.ScanOutput, bool) bool, _ ...request.Option) error {
	return d.ScanPages(input, f)
}
