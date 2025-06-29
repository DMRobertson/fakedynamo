package fakedynamo

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func (d *DB) DescribeTable(input *dynamodb.DescribeTableInput) (*dynamodb.DescribeTableOutput, error) {
	// TODO implement me
	panic("implement me")
}

func (d *DB) describeTable(tableName string) *dynamodb.TableDescription {
	table, exists := d.tables[tableName]
	if !exists {
		return nil
	}
	spec := table.originalInput

	return &dynamodb.TableDescription{
		ArchivalSummary:           nil,
		AttributeDefinitions:      spec.AttributeDefinitions,
		BillingModeSummary:        nil,
		CreationDateTime:          &table.createdAt,
		DeletionProtectionEnabled: spec.DeletionProtectionEnabled,
		GlobalSecondaryIndexes:    nil,
		GlobalTableVersion:        nil,
		ItemCount:                 ptr[int64](0),
		KeySchema:                 spec.KeySchema,
		LatestStreamArn:           nil,
		LatestStreamLabel:         nil,
		LocalSecondaryIndexes:     nil,
		OnDemandThroughput:        spec.OnDemandThroughput,
		ProvisionedThroughput: &dynamodb.ProvisionedThroughputDescription{
			LastDecreaseDateTime:   &time.Time{},
			LastIncreaseDateTime:   &time.Time{},
			NumberOfDecreasesToday: nil,
			ReadCapacityUnits:      nil,
			WriteCapacityUnits:     nil,
		},
		Replicas:            nil,
		RestoreSummary:      nil,
		SSEDescription:      nil,
		StreamSpecification: spec.StreamSpecification,
		TableArn:            nil,
		TableClassSummary: &dynamodb.TableClassSummary{
			LastUpdateDateTime: &time.Time{},
			TableClass:         nil,
		},
		TableId:        nil,
		TableName:      spec.TableName,
		TableSizeBytes: ptr[int64](0),
		TableStatus:    ptr(dynamodb.TableStatusActive),
	}
}

func (d *DB) DescribeTableWithContext(_ aws.Context, input *dynamodb.DescribeTableInput, _ ...request.Option) (*dynamodb.DescribeTableOutput, error) {
	return d.DescribeTable(input)
}

func (d *DB) DescribeTableRequest(input *dynamodb.DescribeTableInput) (*request.Request, *dynamodb.DescribeTableOutput) {
	panic("not implemented: DescribeTableRequest")
}
