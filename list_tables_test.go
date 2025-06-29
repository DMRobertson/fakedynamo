package fakedynamo_test

import (
	"cmp"
	"fmt"
	"slices"
	"testing"

	"github.com/DMRobertson/fakedynamo"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_ListTables_ValidationErrors(t *testing.T) {
	db := fakedynamo.NewDB()
	result, err := db.ListTables(&dynamodb.ListTablesInput{Limit: ptr[int64](0)})
	assert.ErrorContains(t, err, "Limit must be between 1 and 100")
	assert.Nil(t, result)

	result, err = db.ListTables(&dynamodb.ListTablesInput{Limit: ptr[int64](101)})
	assert.ErrorContains(t, err, "Limit must be between 1 and 100")
	assert.Nil(t, result)
}

func TestDB_ListTables_DefaultLimitSize(t *testing.T) {
	db := fakedynamo.NewDB()
	for i := range 200 {
		_, err := db.CreateTable(&dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{{
				AttributeName: ptr("id"),
				AttributeType: ptr(dynamodb.ScalarAttributeTypeS),
			}},
			KeySchema: []*dynamodb.KeySchemaElement{{
				AttributeName: ptr("id"),
				KeyType:       ptr(dynamodb.KeyTypeHash),
			}},
			TableName: ptr(fmt.Sprintf("table-%d", i)),
		})
		require.NoError(t, err)
	}

	result, err := db.ListTables(&dynamodb.ListTablesInput{})
	assert.NoError(t, err)
	assert.Len(t, result.TableNames, 100)
}

func TestDB_ListTables_Pagination(t *testing.T) {
	db := fakedynamo.NewDB()
	expectedNames := make([]*string, 250)
	for i := range expectedNames {
		tableName := fmt.Sprintf("table-%d", i)
		expectedNames[i] = &tableName
		_, err := db.CreateTable(&dynamodb.CreateTableInput{
			AttributeDefinitions: []*dynamodb.AttributeDefinition{{
				AttributeName: ptr("id"),
				AttributeType: ptr(dynamodb.ScalarAttributeTypeS),
			}},
			KeySchema: []*dynamodb.KeySchemaElement{{
				AttributeName: ptr("id"),
				KeyType:       ptr(dynamodb.KeyTypeHash),
			}},
			TableName: &tableName,
		})
		require.NoError(t, err)
	}

	result1, err := db.ListTables(&dynamodb.ListTablesInput{})
	assert.NoError(t, err)
	require.NotNil(t, result1)
	assert.Len(t, result1.TableNames, 100)
	require.NotNil(t, result1.LastEvaluatedTableName)

	result2, err := db.ListTables(&dynamodb.ListTablesInput{
		ExclusiveStartTableName: result1.LastEvaluatedTableName,
	})
	assert.NoError(t, err)
	require.NotNil(t, result2)
	assert.Len(t, result2.TableNames, 100)
	require.NotNil(t, result2.LastEvaluatedTableName)

	result3, err := db.ListTables(&dynamodb.ListTablesInput{
		ExclusiveStartTableName: result2.LastEvaluatedTableName,
	})
	assert.NoError(t, err)
	require.NotNil(t, result3)
	assert.Len(t, result3.TableNames, 50)
	assert.Nil(t, result3.LastEvaluatedTableName)

	allNames := append(result1.TableNames, result2.TableNames...)
	allNames = append(allNames, result3.TableNames...)
	// Dynamo doesn't guarantee the iteration order, so we don't impose that
	// either.
	compareStringPtr := func(a, b *string) int {
		return cmp.Compare(*a, *b)
	}
	slices.SortFunc(allNames, compareStringPtr)
	slices.SortFunc(expectedNames, compareStringPtr)
	assert.Equal(t, expectedNames, allNames)
}
