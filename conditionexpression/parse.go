// Package conditionexpression parses DynamoDB conditions expressions, as
// documented at https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html.
//
// This includes the stricter subset of "key conditions" which are used when
// querying, inspecting partition and sort keys only.
//
// TODO: see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html#limits-expression-parameters for max limits
package conditionexpression

import (
	"io"
)

//go:generate peg grammar.peg

type Expression struct {
	buffer string
	ast    *node32
}

func Parse(s string) (Expression, error) {
	p := &parser{
		Buffer: s,
		Pretty: true,
	}
	err := p.Init()
	if err != nil {
		return Expression{}, err
	}

	if err = p.Parse(); err != nil {
		return Expression{}, err
	}

	root := p.AST()
	dropBoringTokens(&root)
	expr := Expression{
		buffer: p.Buffer,
		ast:    root,
	}

	return expr, nil
}

func dropBoringTokens(referer **node32) {
	for n := *referer; n != nil; n = n.next {
		switch n.pegRule {
		case ruleSP, ruleMAYBE_SP, ruleAND:
			// Drop this node and any children, replacing them with the next sibling.
			(*referer) = n.next
		default:
			dropBoringTokens(&n.up)
			referer = &n.next
		}
	}
}

func (e Expression) PrettyPrint(w io.Writer) {
	e.ast.PrettyPrint(w, e.buffer)
}

func ptr[T any](v T) *T { return &v }
