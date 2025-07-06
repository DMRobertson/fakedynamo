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

		ExpectErrorMessages []string
	}

	testCases := []testCase{
		{
			Name:                "Returns ValidationException for missing AttributeDefinitions",
			ExpectErrorMessages: []string{"AttributeDefinitions", "required field"},
		},
		{
			Name: "Returns ValidationException for nil AttributeDefinition",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{nil},
			},
			ExpectErrorMessages: []string{"AttributeDefinitions[0] is nil"},
		},
		{
			Name: "Returns ValidationException for AttributeDefinition without AttributeName",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
			},
			ExpectErrorMessages: []string{"AttributeDefinitions[0].AttributeName", "required field"},
		},
		{
			Name: "Returns ValidationException for undersized AttributeName",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: ptr(""),
				}},
			},
			ExpectErrorMessages: []string{"AttributeDefinitions[0].AttributeName must be between 1 and 255 characters"},
		},
		{
			Name: "Returns ValidationException for oversized AttributeName",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: &twoKbString,
				}},
			},
			ExpectErrorMessages: []string{"AttributeDefinitions[0].AttributeName must be between 1 and 255 characters"},
		},
		{
			Name: "Returns ValidationException for missing AttributeType",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
			},
			ExpectErrorMessages: []string{"AttributeDefinitions[0].AttributeType is a required field"},
		},
		{
			Name: "Returns ValidationException for invalid AttributeType",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeType: ptr("D"),
				}},
			},
			ExpectErrorMessages: []string{"AttributeDefinitions[0].AttributeType must be one of [S N B]"},
		},
		{
			Name:                "Returns ValidationException for missing KeySchema",
			Input:               dynamodb.CreateTableInput{},
			ExpectErrorMessages: []string{"KeySchema is a required field"},
		},
		{
			Name: "Returns ValidationException for undersized KeySchema",
			Input: dynamodb.CreateTableInput{
				KeySchema: []*dynamodb.KeySchemaElement{},
			},
			ExpectErrorMessages: []string{"KeySchema", "1"},
		},
		{
			Name: "Returns ValidationException for oversized KeySchema",
			Input: dynamodb.CreateTableInput{
				KeySchema: []*dynamodb.KeySchemaElement{{}, {}, {}},
			},
			ExpectErrorMessages: []string{"KeySchema", "2"},
		},
		{
			Name: "Returns ValidationException for nil KeySchema entry",
			Input: dynamodb.CreateTableInput{
				KeySchema: []*dynamodb.KeySchemaElement{nil, {}},
			},
			ExpectErrorMessages: []string{"KeySchema[0] is nil"},
		},
		{
			Name:                "Returns ValidationException for missing table name",
			Input:               dynamodb.CreateTableInput{},
			ExpectErrorMessages: []string{"TableName", "required field"},
		},
		{
			Name: "Returns ValidationException for undersized table name",
			Input: dynamodb.CreateTableInput{
				TableName: aws.String(""),
			},
			ExpectErrorMessages: []string{"TableName", "1"},
		},
		{
			Name: "Returns ValidationException for oversized table name",
			Input: dynamodb.CreateTableInput{
				TableName: &twoKbString,
			},
			ExpectErrorMessages: []string{"TableName", "1024"},
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
			ExpectErrorMessages: []string{"Bar is missing from AttributeDefinitions"},
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
			ExpectErrorMessages: []string{"Bar is missing from AttributeDefinitions"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			db := makeTestDB(t)
			_, err := db.CreateTable(&tc.Input)
			assertErrorContains(t, err, tc.ExpectErrorMessages...)
		})
	}
}

func TestDB_CreateTable_ErrorsWhenTableExists(t *testing.T) {
	t.Parallel()
	db := makeTestDB(t)
	input := exampleCreateTableInputSimplePrimaryKey()
	result, err := db.CreateTable(input)
	require.NoError(t, err)
	require.NotNil(t, result)

	result, err = db.CreateTable(input)
	var expectedErr *dynamodb.ResourceInUseException
	assert.ErrorAs(t, err, &expectedErr)
}

func exampleCreateTableInputSimplePrimaryKey() *dynamodb.CreateTableInput {
	return &dynamodb.CreateTableInput{
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: ptr("Foo"),
				AttributeType: ptr("S"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: ptr("Foo"),
				KeyType:       ptr(dynamodb.KeyTypeHash),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  ptr[int64](1),
			WriteCapacityUnits: ptr[int64](1),
		},
		TableName: aws.String("simple-table-" + nonce()),
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
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  ptr[int64](1),
			WriteCapacityUnits: ptr[int64](1),
		},
		TableName: aws.String("composite-table-" + nonce()),
	}
}

func TestDB_CreateTable_HappyPath(t *testing.T) {
	t.Parallel()
	db := makeTestDB(t)
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
