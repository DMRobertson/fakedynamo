// Package fakedynamo implements an in-memory, approximate implementation of
// DynamoDB. We make a best-effort attempt to
//
// Why? We mock shedloads of DynamoDB API calls at work, and it would be
// useful to have a fake for running tests without needing to coordinate
// spinning up another process. But to be honest, it's an interesting
// side-project which is an excuse to better understand the DynamoDB API.
//
// Prior art:
//   - [Amazon's official local implementation]
//   - [LocalStack's implementation]
//   - https://github.com/ebh/mockdynamodb/, a mock rather than a fake
//   - https://github.com/fsprojects/TestDynamo, in F#
//   - https://github.com/architect/dynalite, in node.js
//
// [Amazon's official local implementation]: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html
// [LocalStack's implementation]: https://docs.localstack.cloud/user-guide/aws/dynamodb/
package fakedynamo

import (
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type DB struct {
	tables map[string]table
}

func NewDB() *DB {
	return &DB{
		tables: make(map[string]table),
	}
}

type table struct {
	originalInput *dynamodb.CreateTableInput
	schema        tableSchema
	createdAt     time.Time
}

type tableSchema struct {
	partition string
	sort      string

	// others is a map from [dynamodb.AttributeDefinition.AttributeName]
	// to [dynamodb.AttributeDefinition.AttributeType].
	others map[string]string
}
