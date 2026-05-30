package executor

import (
	"fmt"
	"strings"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
	"github.com/rodrigo0345/omag/internal/storage"
	"github.com/rodrigo0345/omag/internal/storage/schema"
)

// evalExpr recursively evaluates a GoSQLX expression against a decoded row
// (map[string]any). Returns a bool for predicate expressions and a native Go
// value (int64/float64/string/bool) for value expressions.
func evalExpr(expr ast.Expression, row map[string]any) (any, error) {
	if expr == nil {
		return true, nil
	}

	switch node := expr.(type) {
	case *ast.LiteralValue:
		return ToNative(fmt.Sprintf("%v", node.Value), node.Type), nil

	case *ast.Identifier:
		v, ok := row[node.Name]
		if !ok {
			// unknown column → nil
			return nil, nil
		}
		return v, nil

	case *ast.BinaryExpression:
		return evalBinary(node, row)

	case *ast.UnaryExpression:
		inner, err := evalExpr(node.Expr, row)
		if err != nil {
			return nil, err
		}
		b, ok := inner.(bool)
		if !ok {
			return nil, fmt.Errorf("NOT requires a boolean operand")
		}
		return !b, nil

	case *ast.InExpression:
		return evalIn(node, row)

	case *ast.BetweenExpression:
		return evalBetween(node, row)

	default:
		return nil, fmt.Errorf("unsupported expression type %T", expr)
	}
}

func evalBinary(node *ast.BinaryExpression, row map[string]any) (any, error) {
	op := strings.ToUpper(node.Operator)

	// Logical operators recurse first.
	if op == "AND" || op == "OR" {
		left, err := evalExpr(node.Left, row)
		if err != nil {
			return nil, err
		}
		right, err := evalExpr(node.Right, row)
		if err != nil {
			return nil, err
		}
		lb, lok := left.(bool)
		rb, rok := right.(bool)
		if !lok || !rok {
			return nil, fmt.Errorf("AND/OR require boolean operands")
		}
		if op == "AND" {
			return lb && rb, nil
		}
		return lb || rb, nil
	}

	// Arithmetic / comparison.
	leftVal, err := evalExpr(node.Left, row)
	if err != nil {
		return nil, err
	}
	rightVal, err := evalExpr(node.Right, row)
	if err != nil {
		return nil, err
	}

	switch op {
	case "=":
		return compareEq(leftVal, rightVal), nil
	case "!=", "<>":
		return !compareEq(leftVal, rightVal), nil
	case "<":
		cmp, cerr := compareOrd(leftVal, rightVal)
		return cerr == nil && cmp < 0, cerr
	case "<=":
		cmp, cerr := compareOrd(leftVal, rightVal)
		return cerr == nil && cmp <= 0, cerr
	case ">":
		cmp, cerr := compareOrd(leftVal, rightVal)
		return cerr == nil && cmp > 0, cerr
	case ">=":
		cmp, cerr := compareOrd(leftVal, rightVal)
		return cerr == nil && cmp >= 0, cerr
	case "LIKE":
		return evalLike(leftVal, rightVal)
	default:
		return nil, fmt.Errorf("unsupported binary operator: %s", op)
	}
}

func evalIn(node *ast.InExpression, row map[string]any) (any, error) {
	target, err := evalExpr(node.Expr, row)
	if err != nil {
		return nil, err
	}

	for _, item := range node.List {
		v, err := evalExpr(item, row)
		if err != nil {
			continue
		}
		if compareEq(target, v) {
			if node.Not {
				return false, nil
			}
			return true, nil
		}
	}
	if node.Not {
		return true, nil
	}
	return false, nil
}

func evalBetween(node *ast.BetweenExpression, row map[string]any) (any, error) {
	target, err := evalExpr(node.Expr, row)
	if err != nil {
		return nil, err
	}
	lo, err := evalExpr(node.Lower, row)
	if err != nil {
		return nil, err
	}
	hi, err := evalExpr(node.Upper, row)
	if err != nil {
		return nil, err
	}

	cmpLo, err := compareOrd(target, lo)
	if err != nil {
		return false, nil
	}
	cmpHi, err := compareOrd(target, hi)
	if err != nil {
		return false, nil
	}

	inRange := cmpLo >= 0 && cmpHi <= 0
	if node.Not {
		return !inRange, nil
	}
	return inRange, nil
}

func evalLike(left, right any) (bool, error) {
	s, ok1 := left.(string)
	pattern, ok2 := right.(string)
	if !ok1 || !ok2 {
		return false, nil
	}
	// Simple LIKE: % matches any sequence, _ matches one char.
	return matchLike(s, pattern), nil
}

func matchLike(s, pattern string) bool {
	if pattern == "%" {
		return true
	}
	if len(pattern) == 0 {
		return len(s) == 0
	}
	if pattern[0] == '%' {
		for i := 0; i <= len(s); i++ {
			if matchLike(s[i:], pattern[1:]) {
				return true
			}
		}
		return false
	}
	if len(s) == 0 {
		return false
	}
	if pattern[0] == '_' || pattern[0] == s[0] {
		return matchLike(s[1:], pattern[1:])
	}
	return false
}

// compareEq returns true if a and b are equal, handling cross-type numeric
// comparisons by converting to float64.
func compareEq(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Try numeric comparison first.
	af, aerr := toFloat64(a)
	bf, berr := toFloat64(b)
	if aerr == nil && berr == nil {
		return af == bf
	}
	// Fall back to string comparison.
	return fmt.Sprintf("%v", a) == fmt.Sprintf("%v", b)
}

// compareOrd returns -1, 0, or +1 like strings.Compare.
func compareOrd(a, b any) (int, error) {
	af, aerr := toFloat64(a)
	bf, berr := toFloat64(b)
	if aerr == nil && berr == nil {
		switch {
		case af < bf:
			return -1, nil
		case af > bf:
			return 1, nil
		default:
			return 0, nil
		}
	}
	// String fallback.
	as := fmt.Sprintf("%v", a)
	bs := fmt.Sprintf("%v", b)
	return strings.Compare(as, bs), nil
}

// BuildFilter returns a storage.RowFilterFunction that decodes each raw scan
// entry and applies the WHERE expression.
func BuildFilter(ts schema.ITableSchema, expr ast.Expression) storage.RowFilterFunction {
	if expr == nil {
		return nil
	}
	return func(entry storage.ScanEntry) bool {
		row, err := DecodeRow(ts, entry.Value)
		if err != nil {
			return false
		}
		result, err := evalExpr(expr, row)
		if err != nil {
			return false
		}
		b, ok := result.(bool)
		return ok && b
	}
}
