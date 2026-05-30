package executor

import (
	"testing"

	"github.com/ajitpratap0/GoSQLX/pkg/sql/ast"
)

// helpers to build AST nodes quickly in tests.

func litInt(v int64) *ast.LiteralValue  { return &ast.LiteralValue{Value: v, Type: "int"} }
func litStr(v string) *ast.LiteralValue { return &ast.LiteralValue{Value: v, Type: "string"} }
func litBool(v bool) *ast.LiteralValue  { return &ast.LiteralValue{Value: v, Type: "bool"} }
func ident(name string) *ast.Identifier { return &ast.Identifier{Name: name} }
func binExpr(op string, l, r ast.Expression) *ast.BinaryExpression {
	return &ast.BinaryExpression{Left: l, Operator: op, Right: r}
}
func inExpr(col ast.Expression, items []ast.Expression, not bool) *ast.InExpression {
	return &ast.InExpression{Expr: col, List: items, Not: not}
}
func betweenExpr(col, lo, hi ast.Expression, not bool) *ast.BetweenExpression {
	return &ast.BetweenExpression{Expr: col, Lower: lo, Upper: hi, Not: not}
}

// --- Literal evaluation ---

func TestEvalExpr_IntLiteral(t *testing.T) {
	v, err := evalExpr(litInt(42), nil)
	if err != nil || v.(int64) != 42 {
		t.Fatalf("expected 42, got %v %v", v, err)
	}
}

func TestEvalExpr_StringLiteral(t *testing.T) {
	v, err := evalExpr(litStr("hello"), nil)
	if err != nil || v.(string) != "hello" {
		t.Fatalf("expected hello, got %v %v", v, err)
	}
}

func TestEvalExpr_BoolLiteral(t *testing.T) {
	v, err := evalExpr(litBool(true), nil)
	if err != nil || !v.(bool) {
		t.Fatalf("expected true, got %v %v", v, err)
	}
}

func TestEvalExpr_NilExpr(t *testing.T) {
	v, err := evalExpr(nil, nil)
	if err != nil || v != true {
		t.Fatalf("nil expr should return true, got %v %v", v, err)
	}
}

// --- Column lookup ---

func TestEvalExpr_ColumnLookup(t *testing.T) {
	row := map[string]any{"age": int64(30)}
	v, err := evalExpr(ident("age"), row)
	if err != nil || v.(int64) != 30 {
		t.Fatalf("expected 30, got %v %v", v, err)
	}
}

func TestEvalExpr_MissingColumn(t *testing.T) {
	v, err := evalExpr(ident("missing"), map[string]any{})
	if err != nil || v != nil {
		t.Fatalf("missing column should return nil, got %v %v", v, err)
	}
}

// --- Comparison operators ---

func TestEvalBinary_Eq_True(t *testing.T) {
	row := map[string]any{"x": int64(5)}
	v, err := evalExpr(binExpr("=", ident("x"), litInt(5)), row)
	if err != nil || !v.(bool) {
		t.Fatalf("5 = 5 should be true, got %v %v", v, err)
	}
}

func TestEvalBinary_Eq_False(t *testing.T) {
	row := map[string]any{"x": int64(5)}
	v, err := evalExpr(binExpr("=", ident("x"), litInt(6)), row)
	if err != nil || v.(bool) {
		t.Fatalf("5 = 6 should be false, got %v %v", v, err)
	}
}

func TestEvalBinary_Ne(t *testing.T) {
	row := map[string]any{"x": int64(5)}
	v, _ := evalExpr(binExpr("<>", ident("x"), litInt(6)), row)
	if !v.(bool) {
		t.Fatalf("5 <> 6 should be true")
	}
}

func TestEvalBinary_Lt(t *testing.T) {
	row := map[string]any{"x": int64(3)}
	v, _ := evalExpr(binExpr("<", ident("x"), litInt(5)), row)
	if !v.(bool) {
		t.Fatal("3 < 5 should be true")
	}
}

func TestEvalBinary_Gt(t *testing.T) {
	row := map[string]any{"x": int64(10)}
	v, _ := evalExpr(binExpr(">", ident("x"), litInt(5)), row)
	if !v.(bool) {
		t.Fatal("10 > 5 should be true")
	}
}

func TestEvalBinary_Le(t *testing.T) {
	row := map[string]any{"x": int64(5)}
	v, _ := evalExpr(binExpr("<=", ident("x"), litInt(5)), row)
	if !v.(bool) {
		t.Fatal("5 <= 5 should be true")
	}
}

func TestEvalBinary_Ge(t *testing.T) {
	row := map[string]any{"x": int64(6)}
	v, _ := evalExpr(binExpr(">=", ident("x"), litInt(5)), row)
	if !v.(bool) {
		t.Fatal("6 >= 5 should be true")
	}
}

func TestEvalBinary_StringEq(t *testing.T) {
	row := map[string]any{"name": "alice"}
	v, _ := evalExpr(binExpr("=", ident("name"), litStr("alice")), row)
	if !v.(bool) {
		t.Fatal("string eq should be true")
	}
}

// --- Logical operators ---

func TestEvalBinary_And_TrueTrue(t *testing.T) {
	row := map[string]any{"a": int64(1), "b": int64(2)}
	expr := binExpr("AND",
		binExpr(">", ident("a"), litInt(0)),
		binExpr(">", ident("b"), litInt(0)),
	)
	v, _ := evalExpr(expr, row)
	if !v.(bool) {
		t.Fatal("true AND true should be true")
	}
}

func TestEvalBinary_And_TrueFalse(t *testing.T) {
	row := map[string]any{"a": int64(1), "b": int64(-1)}
	expr := binExpr("AND",
		binExpr(">", ident("a"), litInt(0)),
		binExpr(">", ident("b"), litInt(0)),
	)
	v, _ := evalExpr(expr, row)
	if v.(bool) {
		t.Fatal("true AND false should be false")
	}
}

func TestEvalBinary_Or_FalseTrue(t *testing.T) {
	row := map[string]any{"a": int64(-1), "b": int64(1)}
	expr := binExpr("OR",
		binExpr(">", ident("a"), litInt(0)),
		binExpr(">", ident("b"), litInt(0)),
	)
	v, _ := evalExpr(expr, row)
	if !v.(bool) {
		t.Fatal("false OR true should be true")
	}
}

// --- IN operator ---

func TestEvalIn_Match(t *testing.T) {
	row := map[string]any{"color": "red"}
	expr := inExpr(ident("color"), []ast.Expression{litStr("red"), litStr("blue")}, false)
	v, _ := evalExpr(expr, row)
	if !v.(bool) {
		t.Fatal("'red' IN ('red','blue') should be true")
	}
}

func TestEvalIn_NoMatch(t *testing.T) {
	row := map[string]any{"color": "green"}
	expr := inExpr(ident("color"), []ast.Expression{litStr("red"), litStr("blue")}, false)
	v, _ := evalExpr(expr, row)
	if v.(bool) {
		t.Fatal("'green' IN ('red','blue') should be false")
	}
}

func TestEvalIn_Not(t *testing.T) {
	row := map[string]any{"color": "green"}
	expr := inExpr(ident("color"), []ast.Expression{litStr("red"), litStr("blue")}, true)
	v, _ := evalExpr(expr, row)
	if !v.(bool) {
		t.Fatal("'green' NOT IN ('red','blue') should be true")
	}
}

// --- BETWEEN operator ---

func TestEvalBetween_InRange(t *testing.T) {
	row := map[string]any{"score": int64(50)}
	expr := betweenExpr(ident("score"), litInt(10), litInt(100), false)
	v, _ := evalExpr(expr, row)
	if !v.(bool) {
		t.Fatal("50 BETWEEN 10 AND 100 should be true")
	}
}

func TestEvalBetween_OutOfRange(t *testing.T) {
	row := map[string]any{"score": int64(5)}
	expr := betweenExpr(ident("score"), litInt(10), litInt(100), false)
	v, _ := evalExpr(expr, row)
	if v.(bool) {
		t.Fatal("5 BETWEEN 10 AND 100 should be false")
	}
}

func TestEvalBetween_Not(t *testing.T) {
	row := map[string]any{"score": int64(5)}
	expr := betweenExpr(ident("score"), litInt(10), litInt(100), true)
	v, _ := evalExpr(expr, row)
	if !v.(bool) {
		t.Fatal("5 NOT BETWEEN 10 AND 100 should be true")
	}
}

// --- LIKE ---

func TestMatchLike_ExactMatch(t *testing.T) {
	if !matchLike("hello", "hello") {
		t.Fatal("exact match failed")
	}
}

func TestMatchLike_PrefixWildcard(t *testing.T) {
	if !matchLike("hello world", "hello%") {
		t.Fatal("prefix wildcard failed")
	}
}

func TestMatchLike_SuffixWildcard(t *testing.T) {
	if !matchLike("hello world", "%world") {
		t.Fatal("suffix wildcard failed")
	}
}

func TestMatchLike_ContainsWildcard(t *testing.T) {
	if !matchLike("hello world", "%lo wo%") {
		t.Fatal("contains wildcard failed")
	}
}

func TestMatchLike_SingleChar(t *testing.T) {
	if !matchLike("cat", "c_t") {
		t.Fatal("single-char wildcard failed")
	}
}

func TestMatchLike_AllWildcard(t *testing.T) {
	if !matchLike("anything", "%") {
		t.Fatal("% should match anything")
	}
}

func TestMatchLike_NoMatch(t *testing.T) {
	if matchLike("dog", "cat") {
		t.Fatal("no match should return false")
	}
}

// --- Cross-type comparison ---

func TestCompareEq_Int32VsInt64(t *testing.T) {
	if !compareEq(int32(5), int64(5)) {
		t.Fatal("int32(5) == int64(5) should be true")
	}
}

func TestCompareEq_StringVsString(t *testing.T) {
	if !compareEq("abc", "abc") {
		t.Fatal("string equality failed")
	}
	if compareEq("abc", "xyz") {
		t.Fatal("string inequality failed")
	}
}

func TestCompareEq_NilNil(t *testing.T) {
	if !compareEq(nil, nil) {
		t.Fatal("nil == nil should be true")
	}
}

func TestCompareEq_NilNonNil(t *testing.T) {
	if compareEq(nil, 5) {
		t.Fatal("nil != 5 should be false")
	}
}

func TestCompareOrd_Numerics(t *testing.T) {
	cmp, _ := compareOrd(int64(3), int64(5))
	if cmp >= 0 {
		t.Fatal("3 < 5 should return negative")
	}
	cmp, _ = compareOrd(int64(5), int64(5))
	if cmp != 0 {
		t.Fatal("5 == 5 should return 0")
	}
	cmp, _ = compareOrd(int64(7), int64(5))
	if cmp <= 0 {
		t.Fatal("7 > 5 should return positive")
	}
}

func TestCompareOrd_Strings(t *testing.T) {
	cmp, _ := compareOrd("apple", "banana")
	if cmp >= 0 {
		t.Fatal("apple < banana should return negative")
	}
}
