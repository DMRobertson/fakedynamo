package fakedynamo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_ValidationErrors_ReturnsValidationException_ForMissingRequiredFields(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	_, err := db.GetItem(&dynamodb.GetItemInput{})
	assert.ErrorContains(t, err, "Key is a required field")
	assert.ErrorContains(t, err, "TableName is a required field")
}

func TestDB_ValidationErrors_ReturnsValidationException_NoSuchTable(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	_, err := db.GetItem(&dynamodb.GetItemInput{
		Key:       map[string]*dynamodb.AttributeValue{},
		TableName: ptr("blah"),
	})
	var expectedErr *dynamodb.ResourceNotFoundException
	assert.ErrorAs(t, err, &expectedErr)
}

func TestDB_ValidationErrors_ReturnsValidation_ForNonPrimaryKeyFields(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	simpleTable, err := db.CreateTable(exampleCreateTableInputSimplePrimaryKey())
	require.NoError(t, err)

	_, err = db.GetItem(&dynamodb.GetItemInput{
		TableName: simpleTable.TableDescription.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"Foo":      {S: ptr("foo")},
			"blahblah": {NULL: ptr(true)},
		},
	})
	assert.ErrorContains(t, err, "must provide partition key only")

	compositeTable, err := db.CreateTable(exampleCreateTableInputCompositePrimaryKey())
	require.NoError(t, err)

	_, err = db.GetItem(&dynamodb.GetItemInput{
		TableName: compositeTable.TableDescription.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"Foo":      {S: ptr("foo")},
			"Bar":      {S: ptr("bar")},
			"blahblah": {NULL: ptr(true)},
		},
	})
	assert.ErrorContains(t, err, "must provide partition and sort keys only")
}

func TestDB_GetItem_SimplePartitionKey_Success(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	tableOutput, err := db.CreateTable(exampleCreateTableInputSimplePrimaryKey())
	require.NoError(t, err)

	record := map[string]*dynamodb.AttributeValue{
		"Foo":   {S: ptr("foo")},
		"other": {S: ptr("OTHER")},
	}
	_, err = db.PutItem(&dynamodb.PutItemInput{
		TableName: tableOutput.TableDescription.TableName,
		Item:      record,
	})
	require.NoError(t, err)

	output, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: tableOutput.TableDescription.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"Foo": {S: ptr("foo")},
		},
	})
	assert.NoError(t, err)
	require.NotNil(t, output)
	assert.Equal(t, record, output.Item)
}

func TestDB_GetItem_CompositePartitionKey(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	tableOutput, err := db.CreateTable(exampleCreateTableInputCompositePrimaryKey())
	require.NoError(t, err)

	record := map[string]*dynamodb.AttributeValue{
		"Foo":   {S: ptr("foo")},
		"Bar":   {S: ptr("bar")},
		"other": {S: ptr("OTHER")},
	}
	_, err = db.PutItem(&dynamodb.PutItemInput{
		TableName: tableOutput.TableDescription.TableName,
		Item:      record,
	})
	require.NoError(t, err)

	output, err := db.GetItem(&dynamodb.GetItemInput{
		TableName: tableOutput.TableDescription.TableName,
		Key: map[string]*dynamodb.AttributeValue{
			"Foo": {S: ptr("foo")},
			"Bar": {S: ptr("bar")},
		},
	})
	assert.NoError(t, err)
	require.NotNil(t, output)
	assert.Equal(t, record, output.Item)
}
