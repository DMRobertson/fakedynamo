// Package conditionexpression parses DynamoDB conditions expressions, as
// documented at https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html.
//
// This includes the stricter subset of "key conditions" which are used when
// querying, inspecting partition and sort keys only.
//
// TODO: see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html#limits-expression-parameters for max limits
package conditionexpression

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
)

//go:generate peg grammar.peg

type Expression struct {
	buffer string
	ast    *node32
}

const (
	plainPrint  = false
	prettyPrint = true
)

func Parse(s string) (Expression, error) {
	return parse(s, plainPrint)
}

func ParsePretty(s string) (Expression, error) {
	return parse(s, prettyPrint)
}

func parse(s string, pretty bool) (Expression, error) {
	// TODO: enforce length limits defined here:
	//       https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html#limits-expression-parameters
	p := &parser{ //nolint:exhaustruct
		Buffer: s,
		Pretty: pretty,
	}
	err := p.Init()
	if err != nil {
		return Expression{}, err
	}

	if err = p.Parse(); err != nil {
		return Expression{}, err
	}

	root := p.AST()
	// Okay, I guess this is why people have a separate lexing and parsing
	// stage.
	dropBoringTokens(&root)

	errs := []error{
		checkInOperationLength(root),
		checkForReservedWords(root, p.Buffer),
	}
	if err := errors.Join(errs...); err != nil {
		return Expression{}, err
	}

	expr := Expression{
		buffer: p.Buffer,
		ast:    root,
	}

	return expr, nil
}

func dropBoringTokens(referer **node32) {
	for n := *referer; n != nil; n = n.next {
		switch n.pegRule {
		case ruleSP, ruleMAYBE_SP:
			// Drop this node and any children, replacing them with the next sibling.
			*referer = n.next
		default:
			dropBoringTokens(&n.up)
			referer = &n.next
		}
	}
}

func checkInOperationLength(node *node32) error {
	for n := node; n != nil; n = n.next {
		if node.pegRule == ruleMembership {
			children := readAllChildren(node)
			// The probe is one child, and then we can have up to 100
			if len(children) > 101 {
				return errors.New("too many arguments to IN expression (max 100)")
			}
		}
		if err := checkInOperationLength(node.up); err != nil {
			return err
		}
	}
	return nil
}

func checkForReservedWords(node *node32, buf string) error {
	for n := node; n != nil; n = n.next {
		if err := checkForReservedWords(node.up, buf); err != nil {
			return err
		}
		if node.pegRule == ruleName {
			name := buf[node.begin:node.end]
			if slices.Contains(reservedWords, strings.ToUpper(name)) {
				return fmt.Errorf("contains reserved word '%s'", name)
			}
		}
	}
	return nil
}

func (e Expression) PrettyPrint(w io.Writer) {
	e.ast.PrettyPrint(w, e.buffer)
}

func ptr[T any](v T) *T { return &v }
