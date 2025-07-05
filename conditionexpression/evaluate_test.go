package conditionexpression_test

import (
	"testing"

	"github.com/DMRobertson/fakedynamo/conditionexpression"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExpression_Evaluate(t *testing.T) {
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
		{
			Name:      "membership, result true",
			Condition: "#Col IN (:red, :green, :blue)",
			Item: map[string]*dynamodb.AttributeValue{
				"Color": {S: ptr("red")},
			},
			Names: map[string]*string{
				"#Col": ptr("Color"),
			},
			Values: map[string]*dynamodb.AttributeValue{
				":red":   {S: ptr("red")},
				":green": {S: ptr("green")},
				":blue":  {S: ptr("blue")},
			},
			ExpectedResult: true,
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
