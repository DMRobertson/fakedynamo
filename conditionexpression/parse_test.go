package conditionexpression_test

import (
	"strings"
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
		"ForumName = :name",
		"partitionKeyName = :partitionkeyval AND sortKeyName = :sortkeyval",
		"partitionKeyName = :partitionkeyval OR sortKeyName = :sortkeyval",
		"ForumName = :name and Subject = :sub",
		"sortKeyName = :sortkeyval",
		"sortKeyName < :sortkeyval",
		"sortKeyName <= :sortkeyval",
		"sortKeyName > :sortkeyval",
		"sortKeyName >= :sortkeyval",
		"#S = :myValue",
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
		"NOT (Aaa = :a OR #B = :b)",
	}

	for _, expr := range examples {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()
			_, err := conditionexpression.Parse(expr)
			assert.NoError(t, err)
		})
	}
}

func TestParser_Parse_RejectsOversizedMembershipTest(t *testing.T) {
	t.Parallel()

	inListOfSize := func(count int) string {
		var builder strings.Builder
		builder.WriteString("Name IN (:val")
		for range count - 1 {
			builder.WriteString(", :val")
		}
		builder.WriteString(")")
		return builder.String()
	}

	_, err := conditionexpression.Parse(inListOfSize(100))
	assert.NoError(t, err)

	_, err = conditionexpression.Parse(inListOfSize(101))
	assert.ErrorContains(t, err, "too many arguments to IN expression")
}

func TestParser_Parse_RejectsReservedWords(t *testing.T) {
	t.Parallel()

	_, err := conditionexpression.Parse("Array = :s")
	assert.ErrorContains(t, err, "reserved")
}

func ptr[T any](v T) *T {
	return &v
}
