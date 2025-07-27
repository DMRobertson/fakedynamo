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
	"bytes"
	"cmp"
	"encoding/base64"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/google/btree"
)

type DB struct {
	// mu guards access to tables.
	mu sync.RWMutex
	// tables tracks the tables stored in the database. It's a BTree rather than
	// a plain map, because ListTables needs to be able to paginate through in a
	// consistent order.
	tables *btree.BTreeG[table]
}

func NewDB() *DB {
	return &DB{
		mu:     sync.RWMutex{},
		tables: btree.NewG(2, tableLess),
	}
}

// table models a single DynamoDB table.
type table struct {
	spec *dynamodb.CreateTableInput
	// schema holds parsed information from the spec for easy access.
	schema    tableSchema
	createdAt time.Time

	// DynamoDB's records are conceptually stored in two ways.
	//
	//  - A simple table (no sort key) is a key-value map from
	//    partition keys to records.
	//  - A composite table is a map from partition key to a list of records
	//    sorted by their sort key values.
	//
	// For simplicity, we implement the simple table as a composite table where
	// all the sort keys are identical.
	//
	// We store each partition separately, which more closely mimics Dynamo's
	// implementation. We previously considered storing partitions in the same
	// BTree, using a lexicographic sort on the pair (partition key, sort key).
	// But this makes it harder to implement a parallel scan.
	partitions map[string]*btree.BTreeG[avmap]
}

type tableSchema struct {
	partition string
	sort      string

	// types is a map from [dynamodb.AttributeDefinition.AttributeName]
	// to [dynamodb.AttributeDefinition.AttributeType].
	types map[string]string
}

func tableKey(name string) table {
	return table{spec: &dynamodb.CreateTableInput{ //nolint:exhaustruct
		TableName: &name,
	}}
}

func tableLess(a, b table) bool {
	return cmp.Less(*a.spec.TableName, *b.spec.TableName)
}

type avmap = map[string]*dynamodb.AttributeValue

func (t *table) getPartition(pval *dynamodb.AttributeValue) *btree.BTreeG[avmap] {
	var pvalString string
	switch {
	case pval.S != nil:
		pvalString = *pval.S
	case pval.N != nil:
		pvalString = *pval.N
	case pval.B != nil:
		pvalString = base64.StdEncoding.EncodeToString(pval.B)
	}

	partition := t.partitions[pvalString]
	if partition == nil {
		partition = btree.NewG[avmap](4, makePartitionLess(t.schema))
		t.partitions[pvalString] = partition
	}
	return partition
}

func makePartitionLess(schema tableSchema) btree.LessFunc[avmap] {
	if schema.sort == "" {
		return func(a, b avmap) bool {
			return false
		}
	}

	switch schema.types[schema.sort] {
	case dynamodb.ScalarAttributeTypeS:
		return func(a, b avmap) bool {
			return cmp.Less(*a[schema.sort].S, *b[schema.sort].S)
		}
	case dynamodb.ScalarAttributeTypeN:
		return func(a, b avmap) bool {
			return cmp.Less(*a[schema.sort].N, *b[schema.sort].N)
		}
	case dynamodb.ScalarAttributeTypeB:
		return func(a, b avmap) bool {
			return bytes.Compare(a[schema.sort].B, b[schema.sort].B) < 0
		}
	}
	panic("unreachable")
}
