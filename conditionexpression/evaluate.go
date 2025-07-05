package conditionexpression

import (
	"bytes"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/shopspring/decimal"
)

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
	switch node.pegRule {
	case ruleConditionExpression,
		ruleCondition,
		ruleBooleanAtom,
		ruleConjunct,
		ruleOperand,
		ruleFunctionReturningBool:
		return e.evaluate(node.up, item, names, values)
	case ruleDisjunct:
		children := readAllChildren(node)
		result := true
		for _, child := range children {
			val, err := e.evaluate(child, item, names, values)
			if err != nil {
				return nil, err
			}
			result = result && *val.BOOL
		}
		return &dynamodb.AttributeValue{BOOL: &result}, nil
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
	case ruleRange:
		children := readChildren(node, 3)
		probe, lower, upper := children[0], children[1], children[2]
		probeVal, err1 := e.evaluate(probe, item, names, values)
		lowerVal, err2 := e.evaluate(lower, item, names, values)
		upperVal, err3 := e.evaluate(upper, item, names, values)
		if err := errors.Join(err1, err2, err3); err != nil {
			return nil, err
		}

		probeType := attrType(*probeVal)
		lowerType := attrType(*lowerVal)
		upperType := attrType(*upperVal)
		if probeType != lowerType || probeType != upperType {
			return nil, fmt.Errorf("incompatible types in BETWEEN operation: %s, %s and %s", probeType, lowerType, upperType)
		}
		if !slices.Contains(dynamodb.ScalarAttributeType_Values(), string(probeType)) {
			return nil, fmt.Errorf("cannot compare values of type %s", probeType)
		}

		aboveLower, err1 := e.compare(*lowerVal, "<=", *probeVal)
		belowUpper, err2 := e.compare(*probeVal, "<=", *upperVal)
		if err := errors.Join(err1, err2); err != nil {
			return nil, err
		}
		return &dynamodb.AttributeValue{BOOL: ptr(aboveLower && belowUpper)}, nil
	case ruleBeginsWith:
		children := readChildren(node, 2)
		probe, err1 := e.evaluate(children[0], item, names, values)
		prefix, err2 := e.evaluate(children[1], item, names, values)
		if err := errors.Join(err1, err2); err != nil {
			return nil, err
		}
		probeType := attrType(*probe)
		prefixType := attrType(*prefix)
		if probeType != expression.String || prefixType != expression.String {
			return nil, fmt.Errorf("begins_with arguments must be strings, got %s, %s)", probeType, prefixType)
		}

		return &dynamodb.AttributeValue{
			BOOL: ptr(strings.HasPrefix(*probe.S, *prefix.S)),
		}, nil
	case ruleMembership,
		ruleAttributeExists,
		ruleAttributeNotExists,
		ruleAttributeType,
		ruleContains:
		panic("todo")
	case ruleExpressionAttributeName,
		ruleRawAttribute,
		ruleName,
		ruleListDereference,
		ruleMapDereference,
		ruleSize,
		ruleComparator,
		ruleOR:
		panic("don't think these should be evaluated")
	case ruleUnknown, ruleMAYBE_SP, ruleSP, ruleEND, ruleAND:
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
		lhs, err1 := decimal.NewFromString(*val1.N)
		rhs, err2 := decimal.NewFromString(*val2.N)
		if err := errors.Join(err1, err2); err != nil {
			return false, fmt.Errorf("failed to parse number(s): %w", err)
		}
		comparison := lhs.Compare(rhs)
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

func readAllChildren(parent *node32) []*node32 {
	var children []*node32
	node := parent.up
	for node != nil {
		children = append(children, node)
		node = node.next
	}
	return children
}

func (e Expression) text(node *node32) string {
	return e.buffer[node.begin:node.end]
}

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
