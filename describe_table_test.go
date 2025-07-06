package fakedynamo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_DescribeTable_ReturnsErrorForMissingTable(t *testing.T) {
	t.Parallel()
	db := makeTestDB(t)
	_, err := db.DescribeTable(&dynamodb.DescribeTableInput{TableName: ptr("my-table")})
	var expectedErr *dynamodb.ResourceNotFoundException
	assert.ErrorAs(t, err, &expectedErr)
}

func TestDB_DescribeTable_HappyPath(t *testing.T) {
	t.Parallel()
	db := makeTestDB(t)
	createInput := exampleCreateTableInputCompositePrimaryKey()
	_, err := db.CreateTable(createInput)
	require.NoError(t, err)

	result, err := db.DescribeTable(&dynamodb.DescribeTableInput{TableName: createInput.TableName})
	assert.NoError(t, err)
	require.NotNil(t, result)

	assert.Equal(t, createInput.TableName, result.Table.TableName)
	assert.Equal(t, createInput.KeySchema, result.Table.KeySchema)
	assert.Equal(t, createInput.AttributeDefinitions, result.Table.AttributeDefinitions)
}
