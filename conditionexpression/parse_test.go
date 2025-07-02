package conditionexpression_test

import (
	"testing"

	"github.com/DMRobertson/fakedynamo/conditionexpression"
	"github.com/stretchr/testify/assert"
)

func TestParser_Parse(t *testing.T) {
	t.Parallel()

	examples := []string{
		// Key condition expressions
		// From https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
		"partitionKeyName = :partitionkeyval",
		"partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval",
		"sortKeyName = :sortkeyval",
		"sortKeyName < :sortkeyval",
		"sortKeyName <= :sortkeyval",
		"sortKeyName > :sortkeyval",
		"sortKeyName >= :sortkeyval",
		"Size = :myval",
		"#S = :myval",
		"sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2",
		"begins_with ( sortKeyName, :sortkeyval )",
		// Generic condition expressions
		// From https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
		"attribute_exists (#Pictures.#SideView)",
		"attribute_not_exists (Manufacturer)",
		// "attribute_type (ProductReviews.FiveStar, :v_sub)",
		// "begins_with (Pictures.FrontView, :v_sub)",
		// "contains (Color, :v_sub)",
		// "size (Brand) <= :v_sub",
		// "size(VideoClip) > :v_sub",
		// "size (Color) < :v_sub",
		// "size(ProductReviews.OneStar) > :v_sub",
		// "a > 3 and a < 5",
		// "a OR b",
		// "a AND b",
		// "NOT a",
		// "a OR b AND c",
		// "(a OR b) AND c",
	}

	for _, expr := range examples {
		t.Run(expr, func(t *testing.T) {
			p, err := conditionexpression.Parse(expr)
			if !assert.NoError(t, err) {
				p.PrintSyntaxTree()
			}
		})
	}
}
