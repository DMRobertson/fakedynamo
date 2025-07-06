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
	db := makeTestDB(t)
	_, err := db.ListTables(&dynamodb.ListTablesInput{Limit: ptr[int64](0)})
	assertErrorContains(t, err, "Limit", "1")

	_, err = db.ListTables(&dynamodb.ListTablesInput{Limit: ptr[int64](101)})
	assertErrorContains(t, err, "Limit", "100")
}

func TestDB_ListTables_Pagination(t *testing.T) {
	// Note: not Parallel so that we know exactly what tables to expect.
	db := makeTestDB(t)
	expectedNames := make([]*string, 210)
	for i := range expectedNames {
		input := exampleCreateTableInputCompositePrimaryKey()
		expectedNames[i] = input.TableName
		_, err := db.CreateTable(input)
		require.NoError(t, err)
		if err != nil {
			t.FailNow()
		}
	}

	result1, err := db.ListTables(&dynamodb.ListTablesInput{})
	assert.NoError(t, err)
	require.NotNil(t, result1)
	// NB: this tests that the default value for Limit is 100.
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
	assert.Len(t, result3.TableNames, 10)
	assert.Nil(t, result3.LastEvaluatedTableName)

	allNames := append(result1.TableNames, result2.TableNames...)
	allNames = append(allNames, result3.TableNames...)
	// Dynamo doesn't guarantee the iteration order, so we don't impose that
	// either.
	slices.SortFunc(allNames, comparePtr[string])
	slices.SortFunc(expectedNames, comparePtr[string])
	assert.Equal(t, expectedNames, allNames)

	// Now repeat the test using ListTablesPages.
	// (This avoids having to recreate new tables when running against ddblocal)

	var allNamesPages []*string
	processPage := func(page *dynamodb.ListTablesOutput, lastPage bool) bool {
		allNamesPages = append(allNamesPages, page.TableNames...)
		return true
	}
	err = db.ListTablesPages(&dynamodb.ListTablesInput{}, processPage)
	assert.NoError(t, err)

	// Dynamo doesn't guarantee the iteration order, so we don't impose that
	// either.
	slices.SortFunc(allNamesPages, comparePtr[string])
	slices.SortFunc(expectedNames, comparePtr[string])
	assert.Equal(t, expectedNames, allNamesPages)
}
