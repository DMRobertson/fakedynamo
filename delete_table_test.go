package fakedynamo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_DeleteTable_ErrorsIfNoTableNameGiven(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	_, err := db.DeleteTable(&dynamodb.DeleteTableInput{})
	assertErrorContains(t, err, "TableName", "required field")
}

func TestDB_DeleteTable_ErrorsIfTableMissing(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	_, err := db.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: ptr("my-table"),
	})
	var expectedErr *dynamodb.ResourceNotFoundException
	assert.ErrorAs(t, err, &expectedErr)
}

func TestDB_DeleteTable_HappyPath(t *testing.T) {
	t.Parallel()

	db := makeTestDB()
	input := exampleCreateTableInputCompositePrimaryKey()
	_, err := db.CreateTable(input)
	require.NoError(t, err)

	deleteResult, err := db.DeleteTable(&dynamodb.DeleteTableInput{TableName: input.TableName})
	assert.NoError(t, err)
	require.NotNil(t, deleteResult)
	require.NotNil(t, deleteResult.TableDescription)
	assert.Equal(t, *input.TableName, val(deleteResult.TableDescription.TableName))

	_, err = db.DescribeTable(&dynamodb.DescribeTableInput{TableName: input.TableName})
	var expectedErr *dynamodb.ResourceNotFoundException
	assert.ErrorAs(t, err, &expectedErr)
}
