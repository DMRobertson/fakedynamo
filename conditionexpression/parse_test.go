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
		"partitionKeyName = :partitionkeyval OR sortKeyName = :sortkeyval",
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
		"#Color IN (:red, :green, :blue)",
		// Generic condition expressions
		// From https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
		"attribute_exists (#Pictures[0].#SideView)",
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
		Name      string
		Condition string
		Item      map[string]*dynamodb.AttributeValue
		Names     map[string]*string
		Values    map[string]*dynamodb.AttributeValue

		ExpectedResult bool
	}

	testCases := []TestCase{
		{
			Name:      "string equality, result true",
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
			Name:      "string equality, result false",
			Condition: "partitionKeyName = :partitionkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"partitionKeyName": {S: ptr("bar")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":partitionkeyval": {S: ptr("foo")},
			},
			ExpectedResult: false,
		},
		{
			Name:      "conjunction, result true",
			Condition: "partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"partitionKeyName": {N: ptr("1")},
				"sortKeyName":      {N: ptr("2")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":partitionkeyval": {N: ptr("1")},
				":sortkeyval":      {N: ptr("2")},
			},
			ExpectedResult: true,
		},
		{
			Name:      "conjunction, result false",
			Condition: "partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"partitionKeyName": {N: ptr("1")},
				"sortKeyName":      {N: ptr("2")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":partitionkeyval": {N: ptr("1")},
				":sortkeyval":      {N: ptr("22")},
			},
			ExpectedResult: false,
		},
		{
			Name:      "byte less than, result false",
			Condition: "sortKeyName < :sortkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"sortKeyName": {B: []byte("zzz")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":sortkeyval": {B: []byte("aaa")},
			},
			ExpectedResult: false,
		},
		{
			Name:      "numerical less than equal, result false",
			Condition: "sortKeyName <= :sortkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"sortKeyName": {N: ptr("10")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":sortkeyval": {N: ptr("9")},
			},
			ExpectedResult: false,
		},
		{
			Name:      "string not equal, result true",
			Condition: "sortKeyName <> :sortkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"sortKeyName": {S: ptr("a")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":sortkeyval": {S: ptr("A")},
			},
			ExpectedResult: true,
		},
		{
			Name:      "string greater or equal, result true",
			Condition: "sortKeyName >= :sortkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"sortKeyName": {S: ptr("a")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":sortkeyval": {S: ptr("a")},
			},
			ExpectedResult: true,
		},
		{
			Name:      "string greater, result true",
			Condition: "sortKeyName >= :sortkeyval",
			Item: map[string]*dynamodb.AttributeValue{
				"sortKeyName": {S: ptr("aardvark")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":sortkeyval": {S: ptr("a")},
			},
			ExpectedResult: true,
		},
		{
			Name: "string equality using attr name, result " +
				"false",
			Condition: "#S = :myval",
			Item: map[string]*dynamodb.AttributeValue{
				"foo": {S: ptr("something else")},
			},
			Names: map[string]*string{
				"#S": ptr("foo"),
			},
			Values: map[string]*dynamodb.AttributeValue{
				":myval": {S: ptr("foobar")},
			},
			ExpectedResult: false,
		},
		{
			Name:      "string equality using attr name, result true",
			Condition: "#S = :myval",
			Item: map[string]*dynamodb.AttributeValue{
				"foo": {S: ptr("foobar")},
			},
			Names: map[string]*string{
				"#S": ptr("foo"),
			},
			Values: map[string]*dynamodb.AttributeValue{
				":myval": {S: ptr("foobar")},
			},
			ExpectedResult: true,
		},
		{
			Name:      "between, result false",
			Condition: "sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2",
			Item: map[string]*dynamodb.AttributeValue{
				"sortKeyName": {S: ptr("m")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":sortkeyval1": {S: ptr("a")},
				":sortkeyval2": {S: ptr("b")},
			},
			ExpectedResult: false,
		},
		{
			Name:      "between, result true",
			Condition: "sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2",
			Item: map[string]*dynamodb.AttributeValue{
				"sortKeyName": {N: ptr("456")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":sortkeyval1": {N: ptr("123")},
				":sortkeyval2": {N: ptr("789")},
			},
			ExpectedResult: true,
		},
		{
			Name:      "begins_with, result true",
			Condition: "begins_with ( sortKeyName, :sortkeyval )",
			Item: map[string]*dynamodb.AttributeValue{
				"sortKeyName": {S: ptr("abcdef")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":sortkeyval": {S: ptr("abc")},
			},
			ExpectedResult: true,
		},
		{
			Name:      "begins_with, result false",
			Condition: "begins_with ( sortKeyName, :sortkeyval )",
			Item: map[string]*dynamodb.AttributeValue{
				"sortKeyName": {S: ptr("abcdef")},
			},
			Values: map[string]*dynamodb.AttributeValue{
				":sortkeyval": {S: ptr("xyz")},
			},
			ExpectedResult: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.Name, func(t *testing.T) {
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
