package conditionexpression_test

import (
	"testing"

	"github.com/DMRobertson/fakedynamo/conditionexpression"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParser_Parse(t *testing.T) {
	t.Parallel()

	examples := []string{
		// Key condition expressions
		// From https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
		"partitionKeyName = :partitionkeyval",
		"ForumName = :name",
		"partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval",
		"ForumName = :name and Subject = :sub",
		"sortKeyName = :sortkeyval",
		"sortKeyName < :sortkeyval",
		"sortKeyName <= :sortkeyval",
		"sortKeyName > :sortkeyval",
		"sortKeyName >= :sortkeyval",
		"#S = :myval",
		"sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2",
		"begins_with ( sortKeyName, :sortkeyval )",

		"Id = :id and begins_with(ReplyDateTime, :dt)",
		// Generic condition expressions
		// From https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
		"attribute_exists (#Pictures.#SideView)",
		"attribute_not_exists (Manufacturer)",
		"attribute_type (ProductReviews.FiveStar, :v_sub)",
		"begins_with (Pictures.FrontView, :v_sub)",
		"contains (Color, :v_sub)",
		"size (Brand) <= :v_sub",
		"size(VideoClip) > :v_sub",
		"size (Color) < :v_sub",
		"size(ProductReviews.OneStar) > :v_sub",
		"contains(Color, :c) and Price <= :p",
	}

	for _, expr := range examples {
		t.Run(expr, func(t *testing.T) {
			_, err := conditionexpression.Parse(expr)
			assert.NoError(t, err)
		})
	}
}

func TestEvaluate(t *testing.T) {
	type TestCase struct {
		Condition string
		Item      map[string]*dynamodb.AttributeValue
		Names     map[string]*string
		Values    map[string]*dynamodb.AttributeValue

		ExpectedResult bool
	}

	testCases := []TestCase{
		{
			Condition: "partitionKeyName = :partitionkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"partitionKeyName": {S: ptr("foo")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":partitionkeyval": {S: ptr("foo")},
			},
			ExpectedResult: true,
		},
		{
			Condition: "partitionKeyName = :partitionkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"partitionKeyName": {S: ptr("bar")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":partitionkeyval": {S: ptr("foo")},
			},
			ExpectedResult: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Condition, func(t *testing.T) {
			expr, err := conditionexpression.Parse(tc.Condition)
			require.NoError(t, err)
			result, err := expr.Evaluate(tc.Item, tc.Names, tc.Values)
			require.NoError(t, err)
			assert.Equal(t, tc.ExpectedResult, result)
		})
	}
}

func ptr[T any](v T) *T {
	return &v
}
