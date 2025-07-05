package fakedynamo_test

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_ListTables_ValidationErrors(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	_, err := db.ListTables(&dynamodb.ListTablesInput{Limit: ptr[int64](0)})
	assert.ErrorContains(t, err, "Limit must be between 1 and 100")

	_, err = db.ListTables(&dynamodb.ListTablesInput{Limit: ptr[int64](101)})
	assert.ErrorContains(t, err, "Limit must be between 1 and 100")
}

func TestDB_ListTables_DefaultLimitSize(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	for range 200 {
		input := exampleCreateTableInputCompositePrimaryKey()
		_, err := db.CreateTable(input)
		require.NoError(t, err)
	}

	result, err := db.ListTables(&dynamodb.ListTablesInput{})
	assert.NoError(t, err)
	assert.Len(t, result.TableNames, 100)
}

func TestDB_ListTables_Pagination(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	expectedNames := make([]*string, 250)
	for i := range expectedNames {
		input := exampleCreateTableInputCompositePrimaryKey()
		expectedNames[i] = input.TableName
		_, err := db.CreateTable(input)
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
	slices.SortFunc(allNames, comparePtr[string])
	slices.SortFunc(expectedNames, comparePtr[string])
	assert.Equal(t, expectedNames, allNames)
}

func TestDB_ListTablesPages(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	expectedNames := make([]*string, 250)
	for i := range expectedNames {
		input := exampleCreateTableInputCompositePrimaryKey()
		expectedNames[i] = input.TableName
		_, err := db.CreateTable(input)
		require.NoError(t, err)
	}

	var allNames []*string
	processPage := func(page *dynamodb.ListTablesOutput, lastPage bool) bool {
		allNames = append(allNames, page.TableNames...)
		return true
	}
	err := db.ListTablesPages(&dynamodb.ListTablesInput{}, processPage)
	assert.NoError(t, err)

	// Dynamo doesn't guarantee the iteration order, so we don't impose that
	// either.
	slices.SortFunc(allNames, comparePtr[string])
	slices.SortFunc(expectedNames, comparePtr[string])
	assert.Equal(t, expectedNames, allNames)
}
