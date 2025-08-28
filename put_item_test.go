package fakedynamo_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PutItem_ValidationErrors(t *testing.T) {
	t.Parallel()

	type testCase struct {
		Name      string
		Input     dynamodb.PutItemInput
		SkipLocal string

		ExpectErrorMessages []string
		ExpectErrorAs       any
	}

	hugeKey := string(make([]byte, 65536))

	exampleSimpleTable := exampleCreateTableInputSimplePrimaryKey()
	exampleCompositeTable := exampleCreateTableInputCompositePrimaryKey()
	exampleSimpleBinaryTable := exampleCreateTableInputSimplePrimaryKey()
	exampleSimpleBinaryTable.AttributeDefinitions[0].AttributeType = ptr(dynamodb.ScalarAttributeTypeB)
	exampleSimpleNumberTable := exampleCreateTableInputSimplePrimaryKey()
	exampleSimpleNumberTable.AttributeDefinitions[0].AttributeType = ptr(dynamodb.ScalarAttributeTypeN)

	db := makeTestDB(t)
	for _, spec := range []*dynamodb.CreateTableInput{
		exampleSimpleTable, exampleCompositeTable, exampleSimpleBinaryTable, exampleSimpleNumberTable,
	} {
		_, err := db.CreateTable(spec)
		require.NoError(t, err)
	}

	testCases := []testCase{
		{
			Name: "Returns ValidationException when Item is missing",
			Input: dynamodb.PutItemInput{
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"Item", "required field"},
		},
		{
			Name: "Returns ValidationException when Item keys are oversized",
			// SkipLocal: "Oversized item keys never get a response from DynamoDB Local",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo":   {NULL: ptr(true)},
					hugeKey: {S: ptr("a")},
				},
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"ValidationException", "attribute", "name", "6553"},
		},
		{
			Name:      "Returns ValidationException when Item value is nil",
			SkipLocal: "Nil attributes never get a response from DynamoDB Local",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo": nil,
				},
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"Item.Foo is nil"},
		},
		{
			Name: "Returns ValidationException when Item value has no types",
			Input: dynamodb.PutItemInput{
				Item:      map[string]*dynamodb.AttributeValue{"Foo": {}},
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"exactly", "data", "type"},
		},
		{
			Name: "Returns ValidationException when Item value has multiple types",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{"Foo": {
					S: ptr("123"),
					N: ptr("123"),
				}},
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"exactly", "data", "type"},
		},
		{
			Name: "Returns ValidationException when list elements are invalid",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo": {L: []*dynamodb.AttributeValue{
						{S: ptr("B"), N: ptr("3")},
					}},
				},
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"data", "type", "exactly"},
		},
		{
			Name: "Returns ValidationException when map elements are invalid",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"A": {M: map[string]*dynamodb.AttributeValue{
						hugeKey: {S: ptr("B")},
					}},
				},
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"65536"},
		},
		{
			Name: "Returns ValidationException when TableName is missing",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{},
			},
			ExpectErrorMessages: []string{"Table", "Name", "required field"},
		},
		{
			Name: "Returns ResourceNotFoundException when table does not exist",
			Input: dynamodb.PutItemInput{
				TableName: ptr("does-not-exist"),
			},
			// Note: the syntax new(TypeImplementingError) is important.
			// Just writing &TypeImplementingError{} seems to make errors.As
			// return true unconditionally, and I'm not 100% sure why; there's
			// something about the way interfaces and pointers work that I'm
			// not grokking. See also
			// https://stackoverflow.com/questions/76110748/using-errors-as-while-iterating-over-test-struct-returning-second-argument-to/76111240#76111240
			ExpectErrorAs: new(*dynamodb.ResourceNotFoundException),
		},
		{
			Name: "Returns ValidationException for invalid ReturnValues",
			Input: dynamodb.PutItemInput{
				Item:         map[string]*dynamodb.AttributeValue{},
				TableName:    exampleSimpleTable.TableName,
				ReturnValues: ptr("POTATO"),
			},
			ExpectErrorMessages: []string{"ValidationException", "Return", "Values"},
		},
		{
			Name: "Returns ValidationException for invalid ReturnValuesOnConditionCheckFailure",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo": {S: ptr("hello")},
				},
				TableName:                           exampleSimpleTable.TableName,
				ReturnValuesOnConditionCheckFailure: ptr("POTATO"),
			},
			ExpectErrorMessages: []string{"ValidationException", "ReturnValuesOnConditionCheckFailure"},
		},
		{
			Name: "Returns ValidationException when partition key is missing",
			Input: dynamodb.PutItemInput{
				Item:      map[string]*dynamodb.AttributeValue{},
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"ValidationException", "required key"},
		},
		{
			Name: "Returns ValidationException when sort key is missing",
			Input: dynamodb.PutItemInput{
				Item:      map[string]*dynamodb.AttributeValue{},
				TableName: exampleCompositeTable.TableName,
			},
			ExpectErrorMessages: []string{"ValidationException", "required key"},
		},
		{
			Name: "Returns ValidationException on String key type mismatch",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo": {N: ptr("123")},
				},
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"ValidationException", "Type mismatch"},
		},
		{
			Name: "Returns ValidationException on String key with empty value",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo": {S: ptr("")},
				},
				TableName: exampleSimpleTable.TableName,
			},
			ExpectErrorMessages: []string{"Foo", "empty"},
		},
		{
			Name: "Returns ValidationException on Binary key type mismatch",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo": {S: ptr("123")},
				},
				TableName: exampleSimpleBinaryTable.TableName,
			},
			ExpectErrorMessages: []string{"ValidationException", "Type mismatch"},
		},
		{
			Name: "Returns ValidationException on Binary key with empty value",

			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo": {B: []byte{}},
				},
				TableName: exampleSimpleBinaryTable.TableName,
			},
			ExpectErrorMessages: []string{"ValidationException", "Foo", "empty"},
		},
		{
			Name: "Returns ValidationException on Number key type mismatch",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo": {S: ptr("bar")},
				},
				TableName: exampleSimpleNumberTable.TableName,
			},
			ExpectErrorMessages: []string{"ValidationException", "Type mismatch"},
		},
		{
			Name: "Returns ValidationException on Number key with empty value",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{
					"Foo": {N: ptr("")},
				},
				TableName: exampleSimpleNumberTable.TableName,
			},
			ExpectErrorMessages: []string{"ValidationException", "number"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
			if dynamodbSession != nil && tc.SkipLocal != "" {
				t.Skip(tc.SkipLocal)
			}

			_, err := db.PutItem(&tc.Input)
			assertErrorContains(t, err, tc.ExpectErrorMessages...)
			if tc.ExpectErrorAs != nil {
				assert.ErrorAs(t, err, &tc.ExpectErrorAs)
			}
		})
	}
}

func TestDB_PutItem_ReturnValues(t *testing.T) {
	t.Parallel()

	db := makeTestDB(t)
	tableOutput, err := db.CreateTable(exampleCreateTableInputSimplePrimaryKey())
	require.NoError(t, err)

	makeRecord := func(other string) map[string]*dynamodb.AttributeValue {
		return map[string]*dynamodb.AttributeValue{
			"Foo":   {S: ptr("foo")},
			"Other": {S: &other},
		}
	}

	// First write overwrites nothing.
	result, err := db.PutItem(&dynamodb.PutItemInput{
		Item:      makeRecord("A"),
		TableName: tableOutput.TableDescription.TableName,
	})
	assert.NoError(t, err)
	require.NotNil(t, result)
	assert.Nil(t, result.Attributes)

	// Overwriting with no return values specified returns no attributes.
	result, err = db.PutItem(&dynamodb.PutItemInput{
		Item:      makeRecord("B"),
		TableName: tableOutput.TableDescription.TableName,
	})
	assert.NoError(t, err)
	require.NotNil(t, result)
	assert.Nil(t, result.Attributes)

	// Overwriting with an explicit NONE return values returns no attributes.
	recordC := makeRecord("C")
	result, err = db.PutItem(&dynamodb.PutItemInput{
		Item:         recordC,
		TableName:    tableOutput.TableDescription.TableName,
		ReturnValues: ptr(dynamodb.ReturnValueNone),
	})
	assert.NoError(t, err)
	require.NotNil(t, result)
	assert.Nil(t, result.Attributes)

	// Overwriting returns the old record
	recordD := makeRecord("D")
	result, err = db.PutItem(&dynamodb.PutItemInput{
		Item:         recordD,
		TableName:    tableOutput.TableDescription.TableName,
		ReturnValues: ptr(dynamodb.ReturnValueAllOld),
	})
	assert.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, recordC, result.Attributes)

	// We should still be able to retrieve the latest write.
	getResult, err := db.GetItem(&dynamodb.GetItemInput{
		Key: map[string]*dynamodb.AttributeValue{
			"Foo": {S: ptr("foo")},
		},
		TableName: tableOutput.TableDescription.TableName,
	})
	assert.NoError(t, err)
	require.NotNil(t, getResult)
	assert.Equal(t, recordD, getResult.Item)
}

func TestDB_PutItem_ConditionExpressionHandling(t *testing.T) {
	t.Parallel()
	db := makeTestDB(t)
	createdTable, err := db.CreateTable(exampleCreateTableInputSimplePrimaryKey())
	require.NoError(t, err)

	// Use PutItem to replace a record; error because it does not already exist.
	item := map[string]*dynamodb.AttributeValue{
		"Foo": {S: ptr("foo")},
	}
	_, err = db.PutItem(&dynamodb.PutItemInput{
		Item:                                item,
		TableName:                           createdTable.TableDescription.TableName,
		ConditionExpression:                 aws.String("attribute_exists(Foo)"),
		ReturnValuesOnConditionCheckFailure: ptr(dynamodb.ReturnValueAllOld),
	})
	var checkFailError *dynamodb.ConditionalCheckFailedException
	require.ErrorAs(t, err, &checkFailError)
	assert.Empty(t, checkFailError.Item)

	// Now write the item, conditional on it not existing. Should succeed.
	_, err = db.PutItem(&dynamodb.PutItemInput{
		Item:                                item,
		TableName:                           createdTable.TableDescription.TableName,
		ConditionExpression:                 aws.String("attribute_not_exists(Foo)"),
		ReturnValuesOnConditionCheckFailure: ptr(dynamodb.ReturnValueAllOld),
	})
	require.NoError(t, err)

	// Repeat the last write. Should fail.
	_, err = db.PutItem(&dynamodb.PutItemInput{
		Item:                                item,
		TableName:                           createdTable.TableDescription.TableName,
		ConditionExpression:                 aws.String("attribute_not_exists(Foo)"),
		ReturnValuesOnConditionCheckFailure: ptr(dynamodb.ReturnValueAllOld),
	})
	require.ErrorAs(t, err, &checkFailError)
	assert.Equal(t, item, checkFailError.Item)

	// Now try a condition update which succeeds.
	result, err := db.PutItem(&dynamodb.PutItemInput{
		Item:                      item,
		TableName:                 createdTable.TableDescription.TableName,
		ConditionExpression:       aws.String("Foo = :foo AND attribute_not_exists(#bar)"),
		ExpressionAttributeNames:  map[string]*string{"#bar": ptr("Bar")},
		ExpressionAttributeValues: map[string]*dynamodb.AttributeValue{":foo": {S: ptr("foo")}},
		ReturnValues:              ptr(dynamodb.ReturnValueAllOld),
	})
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, item, result.Attributes)
}
