// Package conditionexpression parses DynamoDB conditions expressions, as
// documented at https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html.
//
// This includes the stricter subset of "key conditions" which are used when
// querying, inspecting partition and sort keys only.
//
// TODO: see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html#limits-expression-parameters for max limits
package conditionexpression

//go:generate peg grammar.peg

func Parse(s string) (string, error) {
	p := &parser{
		Buffer: s,
		Pretty: true,
	}
	_ = p.Init()

	if err := p.Parse(); err != nil {
		return "", err
	}

	p.PrintSyntaxTree()
	return p.SprintSyntaxTree(), nil
}
