package fakedynamo_test

import (
	"testing"

	"github.com/DMRobertson/fakedynamo"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDB_CreateTable(t *testing.T) {
	t.Parallel()

	threeHundredCharString := string(make([]byte, 300))

	type testCase struct {
		Name   string
		Setup  func(t *testing.T, db *fakedynamo.DB, tc *testCase)
		Input  dynamodb.CreateTableInput
		Assert func(t *testing.T, result *dynamodb.CreateTableOutput, err error)
	}
	expectValidationError := expectValidationError[*dynamodb.CreateTableOutput]

	testCases := []testCase{
		{
			Name:   "Returns ValidationException for missing AttributeDefinitions",
			Assert: expectValidationError,
		},
		{
			Name: "Returns ValidationException for missing KeySchema",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
			},
			Assert: expectValidationError,
		},
		{
			Name: "Returns ValidationException for undersized KeySchema",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
				KeySchema:            []*dynamodb.KeySchemaElement{},
			},
			Assert: expectValidationError,
		},
		{
			Name: "Returns ValidationException for oversized KeySchema",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
				KeySchema:            []*dynamodb.KeySchemaElement{{}, {}, {}},
			},
			Assert: expectValidationError,
		},
		{
			Name: "Returns ValidationException for missing table name",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
				KeySchema:            []*dynamodb.KeySchemaElement{{}},
			},
			Assert: expectValidationError,
		},
		{
			Name: "Returns ValidationException for undersized table name",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
				KeySchema:            []*dynamodb.KeySchemaElement{{}},
				TableName:            aws.String("ab"),
			},
			Assert: expectValidationError,
		},
		{
			Name: "Returns ValidationException for oversized table name",
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
				KeySchema:            []*dynamodb.KeySchemaElement{{}},
				TableName:            aws.String(threeHundredCharString),
			},
			Assert: expectValidationError,
		},
		{
			Name: "Returns ResourceInUseException when table already exists",
			Setup: func(t *testing.T, db *fakedynamo.DB, tc *testCase) {
				result, err := db.CreateTable(&tc.Input)
				require.NoError(t, err)
				require.NotNil(t, result)
			},
			Input: dynamodb.CreateTableInput{
				AttributeDefinitions: []*dynamodb.AttributeDefinition{{}},
				KeySchema:            []*dynamodb.KeySchemaElement{{}},
				TableName:            aws.String("my-table"),
			},
			Assert: func(t *testing.T, result *dynamodb.CreateTableOutput, err error) {
				var expectedErr *dynamodb.ResourceInUseException
				assert.ErrorAs(t, err, &expectedErr)
				assert.Nil(t, result)
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
			db := fakedynamo.NewDB()
			if tc.Setup != nil {
				tc.Setup(t, db, &tc)
			}
			result, err := db.CreateTable(&tc.Input)
			tc.Assert(t, result, err)
		})
	}
}

func expectValidationError[R any](t *testing.T, result R, err error) {
	assert.ErrorContains(t, err, "ValidationException")
	assert.Nil(t, result)
}
