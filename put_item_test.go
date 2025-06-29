package fakedynamo_test

import (
	"testing"

	"github.com/DMRobertson/fakedynamo"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_PutItem_ValidationErrors(t *testing.T) {
	t.Parallel()

	type testCase struct {
		Name  string
		Input dynamodb.PutItemInput
		Setup func(*testing.T, *fakedynamo.DB, *testCase)

		ExpectErrorMessage string
		ExpectErrorAs      error
	}

	hugeKey := string(make([]byte, 65536))
	exampleSimpleTableSpec := exampleCreateTableInputSimplePrimaryKey()
	exampleCompositeTableSpec := exampleCreateTableInputCompositePrimaryKey()

	testCases := []testCase{
		{
			Name:               "Returns ValidationException when Item is missing",
			Input:              dynamodb.PutItemInput{},
			ExpectErrorMessage: "Item is a required field",
		},
		{
			Name: "Returns ValidationException when Item keys are oversized",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{hugeKey: nil},
			},
			ExpectErrorMessage: "key too large, max 65535 characters",
		},
		{
			Name: "Returns ValidationException when Item value is nil",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{"123": nil},
			},
			ExpectErrorMessage: "Item.123 is nil",
		},
		{
			Name: "Returns ValidationException when Item value has no types",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{"123": {}},
			},
			ExpectErrorMessage: "Item.123 must have exactly 1 data type specified",
		},
		{
			Name: "Returns ValidationException when Item value has multiple types",
			Input: dynamodb.PutItemInput{
				Item: map[string]*dynamodb.AttributeValue{"123": {
					S: ptr("123"),
					N: ptr("123"),
				}},
			},
			ExpectErrorMessage: "Item.123 must have exactly 1 data type specified",
		},
		{
			Name:               "Returns ValidationException when TableName is missing",
			Input:              dynamodb.PutItemInput{},
			ExpectErrorMessage: "TableName is a required field",
		},
		{
			Name: "Returns ResourceNotFoundException when table does not exist",
			Input: dynamodb.PutItemInput{
				TableName: ptr("does-not-exist"),
			},
			ExpectErrorAs: &dynamodb.ResourceNotFoundException{},
		},
		{
			Name: "Returns ValidationException when partition key is missing",
			Setup: func(t *testing.T, db *fakedynamo.DB, tc *testCase) {
				_, err := db.CreateTable(exampleSimpleTableSpec)
				require.NoError(t, err)
			},
			Input: dynamodb.PutItemInput{
				Item:      map[string]*dynamodb.AttributeValue{},
				TableName: exampleSimpleTableSpec.TableName,
			},
			ExpectErrorMessage: "Item does not define partition key Foo",
		},
		{
			Name: "Returns ValidationException when sort key is missing",
			Setup: func(t *testing.T, db *fakedynamo.DB, tc *testCase) {
				_, err := db.CreateTable(exampleCompositeTableSpec)
				require.NoError(t, err)
			},
			Input: dynamodb.PutItemInput{
				Item:      map[string]*dynamodb.AttributeValue{},
				TableName: exampleCompositeTableSpec.TableName,
			},
			ExpectErrorMessage: "Item does not define sort key Bar",
		},

		// TODO: Returns ValidationException when sort key is missing
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			db := fakedynamo.NewDB()
			if tc.Setup != nil {
				tc.Setup(t, db, &tc)
			}

			result, err := db.PutItem(&tc.Input)
			assert.ErrorContains(t, err, tc.ExpectErrorMessage)
			if tc.ExpectErrorAs != nil {
				assert.ErrorAs(t, err, &tc.ExpectErrorAs)
			}
			assert.Nil(t, result)
		})
	}

}
