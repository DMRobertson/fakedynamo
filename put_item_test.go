package fakedynamo_test

import (
	"testing"

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
		ExpectErrorAs       error
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
						{S: ptr("B"), N: ptr("3")}}},
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
			ExpectErrorAs: &dynamodb.ResourceNotFoundException{},
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
