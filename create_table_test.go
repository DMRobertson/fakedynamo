package fakedynamo_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_CreateTable_ValidationErrors(t *testing.T) {
	t.Parallel()

	twoKbString := string(make([]byte, 2048))

	type testCase struct {
		Name  string
		Input dynamodb.CreateTableInput

		ExpectErrorMessage string
	}

	testCases := []testCase{
		{
			Name:               "Returns ValidationException for missing AttributeDefinitions",
			ExpectErrorMessage: "AttributeDefinitions is a required field",
		},
		{
			Name: "Returns ValidationException for nil AttributeDefinition",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{nil},
			},
			ExpectErrorMessage: "AttributeDefinitions[0] is nil",
		},
		{
			Name: "Returns ValidationException for AttributeDefinition without AttributeName",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
			},
			ExpectErrorMessage: "AttributeDefinitions[0].AttributeName is a required field",
		},
		{
			Name: "Returns ValidationException for undersized AttributeName",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: ptr(""),
				}},
			},
			ExpectErrorMessage: "AttributeDefinitions[0].AttributeName must be between 1 and 255 characters",
		},
		{
			Name: "Returns ValidationException for oversized AttributeName",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: &twoKbString,
				}},
			},
			ExpectErrorMessage: "AttributeDefinitions[0].AttributeName must be between 1 and 255 characters",
		},
		{
			Name: "Returns ValidationException for missing AttributeType",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
			},
			ExpectErrorMessage: "AttributeDefinitions[0].AttributeType is a required field",
		},
		{
			Name: "Returns ValidationException for invalid AttributeType",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeType: ptr("D"),
				}},
			},
			ExpectErrorMessage: "AttributeDefinitions[0].AttributeType must be one of [S N B]",
		},
		{
			Name:               "Returns ValidationException for missing KeySchema",
			Input:              dynamodb.CreateTableInput{},
			ExpectErrorMessage: "KeySchema is a required field",
		},
		{
			Name: "Returns ValidationException for undersized KeySchema",
			Input: dynamodb.CreateTableInput{
				KeySchema: []*dynamodb.KeySchemaElement{},
			},
			ExpectErrorMessage: "KeySchema must contain 1 or 2 items",
		},
		{
			Name: "Returns ValidationException for oversized KeySchema",
			Input: dynamodb.CreateTableInput{
				KeySchema: []*dynamodb.KeySchemaElement{{}, {}, {}},
			},
			ExpectErrorMessage: "KeySchema must contain 1 or 2 items",
		},
		{
			Name: "Returns ValidationException for nil KeySchema entry",
			Input: dynamodb.CreateTableInput{
				KeySchema: []*dynamodb.KeySchemaElement{nil, {}},
			},
			ExpectErrorMessage: "KeySchema[0] is nil",
		},
		{
			Name:               "Returns ValidationException for missing table name",
			Input:              dynamodb.CreateTableInput{},
			ExpectErrorMessage: "TableName is a required field",
		},
		{
			Name: "Returns ValidationException for undersized table name",
			Input: dynamodb.CreateTableInput{
				TableName: aws.String(""),
			},
			ExpectErrorMessage: "TableName must be between 1 and 1024 characters",
		},
		{
			Name: "Returns ValidationException for oversized table name",
			Input: dynamodb.CreateTableInput{
				TableName: &twoKbString,
			},
			ExpectErrorMessage: "TableName must be between 1 and 1024 characters",
		},
		{
			Name: "Returns ValidationException when partition key's attribute is not defined",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: ptr("Foo"),
					AttributeType: ptr("S"),
				}},
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: ptr("Bar"),
					KeyType:       ptr(dynamodb.KeyTypeHash),
				}},
			},
			ExpectErrorMessage: "Bar is missing from AttributeDefinitions",
		},
		{
			Name: "Returns ValidationException when sort key's attribute is not defined",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: ptr("Foo"),
					AttributeType: ptr("S"),
				}},
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: ptr("Foo"),
					KeyType:       ptr(dynamodb.KeyTypeHash),
				}, {
					AttributeName: ptr("Bar"),
					KeyType:       ptr(dynamodb.KeyTypeRange),
				}},
			},
			ExpectErrorMessage: "Bar is missing from AttributeDefinitions",
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			db := makeTestDB()
			result, err := db.CreateTable(&tc.Input)

			assert.ErrorContains(t, err, tc.ExpectErrorMessage)
			assert.Nil(t, result)
		})
	}
}

func TestDB_CreateTable_ErrorsWhenTableExists(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	input := dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{{
			AttributeName: ptr("Foo"),
			AttributeType: ptr("S"),
		}},
		KeySchema: []*dynamodb.KeySchemaElement{{
			AttributeName: ptr("Foo"),
			KeyType:       ptr(dynamodb.KeyTypeHash),
		}},
		TableName: aws.String("my-table"),
	}
	result, err := db.CreateTable(&input)
	require.NoError(t, err)
	require.NotNil(t, result)

	result, err = db.CreateTable(&input)
	var expectedErr *dynamodb.ResourceInUseException
	assert.ErrorAs(t, err, &expectedErr)
	assert.Nil(t, result)
}

func exampleCreateTableInputSimplePrimaryKey() *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: ptr("Foo"),
				AttributeType: ptr("S"),
			},
			{
				AttributeName: ptr("String"),
				AttributeType: ptr("S"),
			},
			{
				AttributeName: ptr("Binary"),
				AttributeType: ptr("B"),
			},
			{
				AttributeName: ptr("Number"),
				AttributeType: ptr("N"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: ptr("Foo"),
				KeyType:       ptr(dynamodb.KeyTypeHash),
			},
		},

		TableName: aws.String("my-table"),
	}
}

func exampleCreateTableInputCompositePrimaryKey() *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: ptr("Foo"),
				AttributeType: ptr("S"),
			},
			{
				AttributeName: ptr("Bar"),
				AttributeType: ptr("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: ptr("Foo"),
				KeyType:       ptr(dynamodb.KeyTypeHash),
			}, {
				AttributeName: ptr("Bar"),
				KeyType:       ptr(dynamodb.KeyTypeRange),
			},
		},

		TableName: aws.String("composite-table"),
	}
}

func TestDB_CreateTable_HappyPath(t *testing.T) {
	t.Parallel()
	db := makeTestDB()
	input := exampleCreateTableInputCompositePrimaryKey()
	result, err := db.CreateTable(input)
	assert.NoError(t, err)
	require.NotNil(t, result)
	require.NotNil(t, result.TableDescription)
	td := result.TableDescription

	assert.Equal(t, input.AttributeDefinitions, td.AttributeDefinitions)
	assert.WithinDuration(t, time.Now(), val(td.CreationDateTime), time.Second)
	assert.Equal(t, ptr[int64](0), td.ItemCount)
	assert.Equal(t, input.KeySchema, td.KeySchema)
	assert.Equal(t, input.OnDemandThroughput, td.OnDemandThroughput)
	assert.Equal(t, input.StreamSpecification, td.StreamSpecification)
	assert.Equal(t, input.TableName, td.TableName)
	assert.Equal(t, ptr(dynamodb.TableStatusActive), td.TableStatus)
}
