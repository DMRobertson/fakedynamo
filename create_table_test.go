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
			Name: "Returns ValidationException for missing AttributeDefinitions",
			Input: dynamodb.CreateTableInput{
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: ptr("foo"),
					KeyType:       ptr(dynamodb.KeyTypeHash),
				}},
				TableName: ptr("example-table"),
			},
			ExpectErrorMessages: []string{"AttributeDefinitions", "required field"},
		},
		{
			Name: "Returns ValidationException for missing AttributeDefinition fields",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: ptr("foo"),
					KeyType:       ptr(dynamodb.KeyTypeHash),
				}},
				TableName: ptr("example-table"),
			},
			ExpectErrorMessages: []string{
				"AttributeDefinitions[0].AttributeName",
				"AttributeDefinitions[0].AttributeType",
				"required field",
			},
		},
		{
			Name: "Returns ValidationException for undersized AttributeName",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: ptr(""),
					AttributeType: ptr(dynamodb.ScalarAttributeTypeS),
				}},
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: ptr("foo"),
					KeyType:       ptr(dynamodb.KeyTypeHash),
				}},
				TableName: ptr("example-table"),
			},
			ExpectErrorMessages: []string{"AttributeDefinitions[0].AttributeName", "1"},
		},
		{
			Name: "Returns ValidationException for oversized AttributeName",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: &twoKbString,
					AttributeType: ptr(dynamodb.ScalarAttributeTypeS),
				}},
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: ptr("foo"),
					KeyType:       ptr(dynamodb.KeyTypeHash),
				}},
				TableName: ptr("example-table"),
			},
			ExpectErrorMessages: []string{"255", "characters"},
		},
		{
			Name: "Returns ValidationException for invalid AttributeType",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: ptr("foo"),
					AttributeType: ptr("no-such-type"),
				}},
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: ptr("foo"),
					KeyType:       ptr(dynamodb.KeyTypeHash),
				}},
				TableName: ptr("example-table"),
			},
			ExpectErrorMessages: []string{"[B, N, S]"},
		},
		{
			Name: "Returns ValidationException for missing KeySchema",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{},
				TableName:            ptr("example-table"),
			},
			ExpectErrorMessages: []string{"KeySchema", "required field"},
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
			Name:                "Returns ValidationException for missing table name",
			Input:               dynamodb.CreateTableInput{},
			ExpectErrorMessages: []string{"TableName", "required field"},
		},
		{
			Name: "Returns ValidationException for undersized table name",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: ptr("foo"),
					AttributeType: ptr(dynamodb.ScalarAttributeTypeS),
				}},
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: ptr("foo"),
					KeyType:       ptr(dynamodb.KeyTypeHash),
				}},
				TableName: ptr("aa"),
			},
			ExpectErrorMessages: []string{"Table", "name", "3", "characters"},
		},
		{
			Name: "Returns ValidationException for oversized table name",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{
					AttributeName: ptr("foo"),
					AttributeType: ptr(dynamodb.ScalarAttributeTypeS),
				}},
				KeySchema: []*dynamodb.KeySchemaElement{{
					AttributeName: ptr("foo"),
					KeyType:       ptr(dynamodb.KeyTypeHash),
				}},
				TableName: &twoKbString,
			},
			ExpectErrorMessages: []string{"Table", "name", "255", "characters"},
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
				TableName: ptr("example-table"),
			},
			ExpectErrorMessages: []string{"Attribute", "Definitions"},
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
				TableName: ptr("example-table"),
			},
			ExpectErrorMessages: []string{"Attribute", "Definitions"},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			t.Parallel()
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
	_, err := db.CreateTable(input)
	require.NoError(t, err)

	_, err = db.CreateTable(input)
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
