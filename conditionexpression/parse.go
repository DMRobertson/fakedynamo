// Package conditionexpression parses DynamoDB conditions expressions, as
// documented at https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html.
//
// This includes the stricter subset of "key conditions" which are used when
// querying, inspecting partition and sort keys only.
//
// TODO: see https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Constraints.html#limits-expression-parameters for max limits
package conditionexpression

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
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
	dropSpaces(&root)
	expr := Expression{
		buffer: p.Buffer,
		ast:    root,
	}

	return expr, nil
}

func dropSpaces(referrer **node32) {
	n := *referrer
	switch n.pegRule {
	case ruleSP, ruleMAYBE_SP:
		// Drop this node and any children, replacing them with the next sibling.
		*referrer = n.next
	default:
		if n.up != nil {
			dropSpaces(&n.up)
		}
	}
	if n.next != nil {
		dropSpaces(&n.next)
	}
}

func (e Expression) PrettyPrint(w io.Writer) {
	e.ast.PrettyPrint(w, e.buffer)
}

// func (e *Expression) Evaluate(item map[string]*dynamodb.AttributeValue) bool {
// 	stack := []*node32{e.ast}
// 	for len(stack) > 0 {
// 		node := stack[len(stack)-1]
// 		for range len(stack) {
// 			fmt.Print(" ")
// 		}
// 		fmt.Println(node, e.buffer[node.begin:node.end])
// 		if node.up != nil {
// 			stack = append(stack, node.up)
// 		} else if node.next != nil {
// 			stack[len(stack)-1] = node.next
// 		} else {
// 			// pop children until head has next or stack empty
// 			for len(stack) > 0 && stack[len(stack)-1].next == nil {
// 				stack = stack[:len(stack)-1]
// 			}
// 			if len(stack) > 0 {
// 				stack[len(stack)-1] = stack[len(stack)-1].next
// 			}
// 		}
// 	}
// 	return true
// }

func (e Expression) Evaluate(
	item map[string]*dynamodb.AttributeValue,
	names map[string]*string,
	values map[string]*dynamodb.AttributeValue,
) (bool, error) {
	val, err := e.evaluate(e.ast, item, names, values)
	if err != nil {
		return false, err
	}
	return *val.BOOL, nil
}

func (e Expression) evaluate(
	node *node32,
	item map[string]*dynamodb.AttributeValue,
	names map[string]*string,
	values map[string]*dynamodb.AttributeValue,
) (*dynamodb.AttributeValue, error) {
	if node == nil {
		panic("should not happen!")
	}
	println(node.String())
	switch node.pegRule {
	case ruleConditionExpression,
		ruleBooleanAtom,
		ruleDisjunct,
		ruleConjunct,
		ruleCondition,
		ruleOperand:
		return e.evaluate(node.up, item, names, values)
	case ruleNegation:
		result, err := e.evaluate(node.up, item, names, values)
		if err != nil {
			return nil, err
		}
		result.BOOL = ptr(!*result.BOOL)
		return result, nil
	case ruleComparison:
		children := readChildren(node, 3)
		op1, comparator, op2 := children[0], children[1], children[2]
		val1, err1 := e.evaluate(op1, item, names, values)
		val2, err2 := e.evaluate(op2, item, names, values)
		if err := errors.Join(err1, err2); err != nil {
			return nil, err
		}

		result, err := e.compare(*val1, e.text(comparator), *val2)
		if err != nil {
			return nil, err
		}
		return &dynamodb.AttributeValue{BOOL: &result}, nil
	case ruleDocumentPath:
		return e.walkDocumentPath(node.up, item, names)
	case ruleExpressionAttributeValue:
		key := e.text(node)
		val, exists := values[key]
		if !exists {
			return nil, fmt.Errorf("no such value '%s'", key)
		}
		return val, nil
	case ruleRange,
		ruleMembership,
		ruleFunctionReturningBool,
		ruleAttributeExists,
		ruleAttributeNotExists,
		ruleAttributeType,
		ruleBeginsWith,
		ruleContains:
		panic("todo")
	case ruleExpressionAttributeName,
		ruleRawAttribute,
		ruleName,
		ruleListDereference,
		ruleMapDereference,
		ruleSize,
		ruleComparator,
		ruleAND,
		ruleOR:
		panic("don't think these should be evaluated")
	case ruleUnknown, ruleMAYBE_SP, ruleSP, ruleEND:
		// Pruned
	default:
	}
	panic("should not happen!")
}

func (e Expression) compare(val1 dynamodb.AttributeValue, operator string, val2 dynamodb.AttributeValue) (bool, error) {
	t1 := attrType(val1)
	t2 := attrType(val2)
	if t1 == "" {
		return false, errors.New("no value specified in LHS of comparison")
	} else if t2 == "" {
		return false, errors.New("no value specified in RHS of comparison")
	} else if t1 != t2 {
		return false, fmt.Errorf("type mismatch: %s %s %s", t1, operator, t2)
	}

	switch t1 {
	case expression.Boolean:
		lhsBool, rhsBool := *val1.BOOL, *val2.BOOL
		switch operator {
		case "=":
			return lhsBool == rhsBool, nil
		case "<>":
			return lhsBool != rhsBool, nil
		default:
			return false, fmt.Errorf("invalid comparison of booleans: %s", operator)
		}
	case expression.String:
		comparison := strings.Compare(*val1.S, *val2.S)
		return compare(operator, comparison), nil
	case expression.Number:
		comparison := strings.Compare(*val1.N, *val2.N)
		return compare(operator, comparison), nil
	case expression.Binary:
		comparison := bytes.Compare(val1.B, val2.B)
		return compare(operator, comparison), nil
	case
		expression.List,
		expression.Map,
		expression.StringSet,
		expression.NumberSet,
		expression.BinarySet:
		return false, fmt.Errorf("cannot compare values of type %s", t1)
	case expression.Null:
		return false, errors.New("not implemented: comparing NULLs (what does Dynamo do?)")
	}

	panic("unreachable")
}

func compare(operator string, result int) bool {
	switch operator {
	case "<":
		return result < 0
	case "<=":
		return result <= 0
	case "=":
		return result == 0
	case "<>":
		return result != 0
	case ">":
		return result > 0
	case ">=":
		return result >= 0
	}
	panic("unreachable")
}

func readChildren(parent *node32, count int) []*node32 {
	node := parent.up
	out := make([]*node32, count)
	for i := range count {
		out[i] = node
		node = node.next
	}
	return out
}

func (e Expression) text(node *node32) string {
	return e.buffer[node.begin:node.end]
}

func ptr[T any](v T) *T { return &v }

func attrType(val dynamodb.AttributeValue) expression.DynamoDBAttributeType {
	switch {
	case val.S != nil:
		return expression.String
	case val.SS != nil:
		return expression.StringSet
	case val.N != nil:
		return expression.Number
	case val.NS != nil:
		return expression.NumberSet
	case val.B != nil:
		return expression.Binary
	case val.BS != nil:
		return expression.BinarySet
	case val.BOOL != nil:
		return expression.Boolean
	case val.NULL != nil:
		return expression.Null
	case val.L != nil:
		return expression.List
	case val.M != nil:
		return expression.Map
	}
	return ""
}

func (e Expression) walkDocumentPath(node *node32,
	item map[string]*dynamodb.AttributeValue,
	names map[string]*string,
) (*dynamodb.AttributeValue, error) {
	path := ""
	cursor := &dynamodb.AttributeValue{M: item}
	var exists bool

	for node != nil {
		println(node.String())
		switch node.pegRule {
		case ruleName:
			switch node.up.pegRule {
			case ruleRawAttribute:
				if cursor.M == nil {
					if path == "" {
						path = "root item"
					}
					return nil, fmt.Errorf("%s is not a map", path)
				}
				key := e.text(node.up)
				cursor, exists = cursor.M[key]
				if !exists {
					return nil, fmt.Errorf("no such key '%s'", key)
				}
				node = node.next
				path += key
			case ruleExpressionAttributeName:
				if cursor.M == nil {
					return nil, fmt.Errorf("%s is not a map", path)
				}
				substitution := e.text(node.up)
				key, exists := names[substitution]
				if !exists || key == nil {
					return nil, fmt.Errorf("no such name '%s'", substitution)
				}

				cursor, exists = cursor.M[*key]
				if !exists {
					return nil, fmt.Errorf("no such key '%s'", *key)
				}
				node = node.next
				path += *key
			default:
				panic("unreachable")
			}
		case ruleListDereference:
			panic("TODO")
		case ruleMapDereference:
			panic("TODO")
		default:
			panic("unreachable")
		}
	}

	return cursor, nil
}
