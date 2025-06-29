package fakedynamo_test

import (
	"testing"

	"github.com/DMRobertson/fakedynamo"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_DeleteTable_ErrorsIfNoTableNameGiven(t *testing.T) {
	t.Parallel()
	db := fakedynamo.NewDB()
	result, err := db.DeleteTable(&dynamodb.DeleteTableInput{})
	var expectedErr *dynamodb.ResourceNotFoundException
	assert.ErrorAs(t, err, &expectedErr)
	assert.Nil(t, result)

}

func TestDB_DeleteTable_HappyPath(t *testing.T) {
	t.Parallel()

	db := fakedynamo.NewDB()
	input := exampleCreateTableInput()
	_, err := db.CreateTable(input)
	require.NoError(t, err)

	deleteResult, err := db.DeleteTable(&dynamodb.DeleteTableInput{TableName: input.TableName})
	assert.NoError(t, err)
	require.NotNil(t, deleteResult)
	require.NotNil(t, deleteResult.TableDescription)
	assert.Equal(t, *input.TableName, val(deleteResult.TableDescription.TableName))

	describeResult, err := db.DescribeTable(&dynamodb.DescribeTableInput{TableName: input.TableName})
	var expectedErr *dynamodb.ResourceNotFoundException
	assert.ErrorAs(t, err, &expectedErr)
	assert.Nil(t, describeResult)
}
