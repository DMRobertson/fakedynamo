package fakedynamo_test

import (
	"maps"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_DeleteItem(t *testing.T) {
	t.Parallel()

	type testCase struct {
		Name  string
		Input dynamodb.DeleteItemInput

		ExpectErrorMessages []string
		ExpectErrorAs       any
	}

	db := makeTestDB(t)
	table, err := db.CreateTable(exampleCreateTableInputSimplePrimaryKey())
	require.NoError(t, err)
	testCases := []testCase{
		{
			Name: "Returns ValidationException when Key is missing",
			Input: dynamodb.DeleteItemInput{
				TableName: table.TableDescription.TableName,
			},
			ExpectErrorMessages: []string{"Key", "required field"},
		},
		{
			Name: "Returns ValidationException when Key is missing",
			Input: dynamodb.DeleteItemInput{
				Key: map[string]*dynamodb.AttributeValue{},
			},
			ExpectErrorMessages: []string{"TableName", "required field"},
		},
		{
			Name: "Returns ValidationException on bad ConditionExpression",
			Input: dynamodb.DeleteItemInput{
				TableName:           table.TableDescription.TableName,
				Key:                 map[string]*dynamodb.AttributeValue{},
				ConditionExpression: ptr("NOT A CONDITION EXPRESSION"),
			},
			ExpectErrorMessages: []string{"ValidationException", "Condition"},
		},
		{
			Name: "Returns ValidationException on bad ReturnValues value",
			Input: dynamodb.DeleteItemInput{
				TableName:    table.TableDescription.TableName,
				Key:          map[string]*dynamodb.AttributeValue{},
				ReturnValues: ptr("bums"),
			},
			ExpectErrorMessages: []string{"ValidationException", "Return", "Values"},
		},
		{
			Name: "Returns ValidationException on bad ReturnValuesOnConditionCheckFailure value",
			Input: dynamodb.DeleteItemInput{
				TableName:                           table.TableDescription.TableName,
				Key:                                 map[string]*dynamodb.AttributeValue{},
				ReturnValuesOnConditionCheckFailure: ptr("bums"),
			},
			ExpectErrorMessages: []string{"ValidationException", "ReturnValuesOnConditionCheckFailure"},
		},
		{
			Name: "Returns ResourceNotFoundException when table does not exist",
			Input: dynamodb.DeleteItemInput{
				TableName: ptr("no-such-table"),
				Key:       map[string]*dynamodb.AttributeValue{},
			},
			ExpectErrorAs: new(*dynamodb.ResourceNotFoundException),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()

			_, err := db.DeleteItem(&tc.Input)
			assertErrorContains(t, err, tc.ExpectErrorMessages...)
			if tc.ExpectErrorAs != nil {
				assert.ErrorAs(t, err, &tc.ExpectErrorAs)
			}
		})
	}
}

func TestDB_DeleteItem_RejectsInvalidKeys(t *testing.T) {
	t.Parallel()
	db := makeTestDB(t)

	key := map[string]*dynamodb.AttributeValue{
		"Foo": {S: ptr("hello")},
	}

	simpleTable, err := db.CreateTable(exampleCreateTableInputSimplePrimaryKey())
	require.NoError(t, err)

	_, err = db.PutItem(&dynamodb.PutItemInput{
		TableName: simpleTable.TableDescription.TableName,
		Item:      key,
	})
	require.NoError(t, err)

	_, err = db.DeleteItem(&dynamodb.DeleteItemInput{
		Key:       map[string]*dynamodb.AttributeValue{},
		TableName: simpleTable.TableDescription.TableName,
	})
	assert.ErrorContains(t, err, "ValidationException")
	assert.ErrorContains(t, err, "key")
}

func TestDB_DeleteItem_DoesNotErrorOnMissingItem(t *testing.T) {
	t.Parallel()
	db := makeTestDB(t)

	simpleTable, err := db.CreateTable(exampleCreateTableInputSimplePrimaryKey())
	require.NoError(t, err)

	_, err = db.DeleteItem(&dynamodb.DeleteItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Foo": {S: ptr("hello")},
		},
		TableName: simpleTable.TableDescription.TableName,
	})
	assert.NoError(t, err)
}

func TestDB_DeleteItem_ReturnsOldValues(t *testing.T) {
	t.Parallel()
	db := makeTestDB(t)

	key := map[string]*dynamodb.AttributeValue{
		"Foo": {S: ptr("hello")},
	}
	item := map[string]*dynamodb.AttributeValue{
		"Bar": {S: ptr("world")},
	}
	maps.Copy(item, key)

	simpleTable, err := db.CreateTable(exampleCreateTableInputSimplePrimaryKey())
	require.NoError(t, err)

	_, err = db.PutItem(&dynamodb.PutItemInput{
		TableName: simpleTable.TableDescription.TableName,
		Item:      item,
	})
	require.NoError(t, err)

	result, err := db.DeleteItem(&dynamodb.DeleteItemInput{
		Key:          key,
		TableName:    simpleTable.TableDescription.TableName,
		ReturnValues: ptr(dynamodb.ReturnValueAllOld),
	})
	assert.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, item, result.Attributes)

	// GetItem now returns nothing
	getResult, err := db.GetItem(&dynamodb.GetItemInput{
		Key:       key,
		TableName: simpleTable.TableDescription.TableName,
	})
	assert.NoError(t, err)
	assert.Empty(t, getResult.Item)

	// Re-delete should not return old values
	result, err = db.DeleteItem(&dynamodb.DeleteItemInput{
		Key:          key,
		TableName:    simpleTable.TableDescription.TableName,
		ReturnValues: ptr(dynamodb.ReturnValueAllOld),
	})
	assert.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Attributes)
}

func TestDB_DeleteItem_EnforcesConditionExpression(t *testing.T) {
	t.Parallel()
	db := makeTestDB(t)

	key := map[string]*dynamodb.AttributeValue{
		"Foo": {S: ptr("hello")},
	}
	item := map[string]*dynamodb.AttributeValue{
		"Bar": {S: ptr("world")},
	}
	maps.Copy(item, key)

	simpleTable, err := db.CreateTable(exampleCreateTableInputSimplePrimaryKey())
	require.NoError(t, err)

	_, err = db.PutItem(&dynamodb.PutItemInput{
		TableName: simpleTable.TableDescription.TableName,
		Item:      item,
	})
	require.NoError(t, err)

	_, err = db.DeleteItem(&dynamodb.DeleteItemInput{
		Key:                                 key,
		TableName:                           simpleTable.TableDescription.TableName,
		ConditionExpression:                 ptr("attribute_not_exists(Bar)"),
		ReturnValuesOnConditionCheckFailure: ptr(dynamodb.ReturnValueAllOld),
	})
	var checkErr *dynamodb.ConditionalCheckFailedException
	require.ErrorAs(t, err, &checkErr)
	assert.Equal(t, item, checkErr.Item)

	// Record has not been deleted
	getResult, err := db.GetItem(&dynamodb.GetItemInput{
		Key:       key,
		TableName: simpleTable.TableDescription.TableName,
	})
	assert.NoError(t, err)
	assert.Equal(t, item, getResult.Item)
}
