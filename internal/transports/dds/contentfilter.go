// Copyright (C) 2026 Podomy.
// SPDX-License-Identifier: AGPL-3.0-or-later

package dds

// Content filtered topics, Weld side per-sample filtering with drop reasons.
//
// DDS ContentFilteredTopic lets a reader subscribe with a filter expression
// and drop non-matching samples. Weld has no filter support today, so this
// file provides the pure building blocks a future take-path hook can call:
// a lexer plus parser for a documented subset of the DDS SQL filter
// expression grammar, an evaluator over XCDR1 payloads described by
// types.MessageMembers, distinct drop-reason constants for filter-miss
// versus filter-eval-error following the datapath reason pattern in
// datapath.go, and a compile-once/evaluate-many subscription helper that is
// safe for concurrent use.
//
// Supported expression grammar (subset of the DDS SQL filter grammar):
//
//	filter    := or_expr
//	or_expr   := and_expr (OR and_expr)*
//	and_expr  := not_expr (AND not_expr)*
//	not_expr  := NOT not_expr | predicate
//	predicate := "(" or_expr ")"
//	           | operand
//	           | operand comp_op operand
//	           | operand [NOT] BETWEEN operand AND operand
//	           | operand [NOT] IN "(" operand ("," operand)* ")"
//	           | operand [NOT] LIKE operand
//	operand   := field_ref | number | string | TRUE | FALSE | NULL
//	           | ("-" | "+") number
//	field_ref := ident ("." ident)*
//	comp_op   := "=" | "<>" | "!=" | "<" | "<=" | ">" | ">="
//
// Notes on the subset: comparisons work on scalar numeric, string, and bool
// fields including nested dotted paths such as header.frame_id. BETWEEN is
// inclusive on both ends. IN holds one or more operands. LIKE matches %
// against any run of runes and _ against exactly one rune, and is case
// sensitive. AND binds tighter than OR, NOT binds tightest, and parens
// group explicitly. AND and OR short-circuit: the right side never evaluates
// when the left decides the outcome, so a skipped error never surfaces. A
// bare boolean field or literal reads as a predicate. Keywords are case
// insensitive, field names are case sensitive. Strings use single quotes
// with '' as an escaped quote. Integer literals without fraction or exponent
// that fit int64 compare as exact int64; anything else compares as float64.
// NaN never matches: any comparison with NaN reports false with no error.
// Numeric, string, and bool operands never mix in one comparison: mixing
// yields an eval error, never a silent false. Array and sequence fields are
// not filterable: decoding skips them to keep later field offsets right,
// and referencing one yields an eval error. Expressions above 1024 tokens or
// nested deeper than 32 NOT plus paren levels refuse loudly at compile time.

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/podomy/weld/internal/types"
)

// Drop reason codes for content filter outcomes.
//
// Filter-miss means the sample decoded fine and evaluated to false.
// Filter-eval-error means the sample could not be evaluated at all, such
// as a truncated payload, an unknown field, or a type mismatch. The two
// share the countDropReason pattern from datapath.go so dashboards can tell
// quiet filtering apart from malformed samples.
const (
	// DropReasonContentFilterMiss counts samples that evaluated but did not match.
	DropReasonContentFilterMiss DropReason = "content-filter-miss"
	// DropReasonContentFilterEvalError counts samples that failed evaluation.
	DropReasonContentFilterEvalError DropReason = "content-filter-eval-error"
)

var (
	// ErrContentFilterMiss reports a sample that evaluated but did not match.
	ErrContentFilterMiss = errors.New("dds: content filter miss")
	// ErrContentFilterEvalError reports a sample that failed evaluation.
	ErrContentFilterEvalError = errors.New("dds: content filter eval error")
	// ErrContentFilterParse reports a filter expression that failed to parse.
	ErrContentFilterParse = errors.New("dds: content filter parse error")
)

// Content filter expression limits bound adversarial inputs.
//
// Token count caps lexer output so a megabyte of operators cannot exhaust
// memory. Nesting depth caps NOT chains plus parenthesized groups so deeply
// nested input cannot exhaust the call stack. Both refuse loudly with the
// parse sentinel instead of truncating silently.
const (
	// maxFilterTokens caps lexer tokens in one expression excluding EOF.
	maxFilterTokens = 1024
	// maxFilterDepth caps NOT plus paren nesting in one expression.
	maxFilterDepth = 32
)

// ParseError is a typed filter parse failure naming the byte position.
type ParseError struct {
	// Pos is the byte offset in the expression where parsing failed.
	Pos int
	// Msg describes the failure without position or package prefix.
	Msg string
}

// Error renders the parse failure with its byte position.
func (e *ParseError) Error() string {
	return fmt.Sprintf("dds: content filter parse at byte %d: %s", e.Pos, e.Msg)
}

// filterTokenKind names one lexer token kind.
type filterTokenKind int

// Lexer token kinds for the filter expression grammar.
const (
	tokEOF filterTokenKind = iota
	tokIdent
	tokNumber
	tokString
	tokEq
	tokNe
	tokLt
	tokLe
	tokGt
	tokGe
	tokLParen
	tokRParen
	tokComma
	tokDot
	tokMinus
	tokPlus
	tokAnd
	tokOr
	tokNot
	tokBetween
	tokIn
	tokLike
	tokTrue
	tokFalse
	tokNull
)

// filterToken is one lexer output with its byte position.
type filterToken struct {
	kind filterTokenKind
	text string
	num  float64
	// isInt reports the literal parsed as an exact int64.
	isInt bool
	// ival holds the exact int64 value when isInt is true.
	ival int64
	pos  int
}

// filterKeyword maps lowercase words to keyword token kinds.
var filterKeyword = map[string]filterTokenKind{
	"and":     tokAnd,
	"or":      tokOr,
	"not":     tokNot,
	"between": tokBetween,
	"in":      tokIn,
	"like":    tokLike,
	"true":    tokTrue,
	"false":   tokFalse,
	"null":    tokNull,
}

// isFilterIdentStart reports whether c can start an identifier.
func isFilterIdentStart(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// isFilterIdentChar reports whether c can continue an identifier.
func isFilterIdentChar(c byte) bool {
	return isFilterIdentStart(c) || (c >= '0' && c <= '9')
}

// isFilterDigit reports whether c is an ASCII digit.
func isFilterDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

// skipFilterWhitespace advances past spaces, tabs, and newlines.
func skipFilterWhitespace(expr string, i int) int {
	for i < len(expr) {
		c := expr[i]
		if c != ' ' && c != '\t' && c != '\n' && c != '\r' {
			break
		}

		i++
	}

	return i
}

// lexFilterIdent scans one identifier or keyword starting at start.
func lexFilterIdent(expr string, start int) (filterToken, int) {
	i := start
	for i < len(expr) && isFilterIdentChar(expr[i]) {
		i++
	}

	word := expr[start:i]
	kind := tokIdent

	if kw, ok := filterKeyword[strings.ToLower(word)]; ok {
		kind = kw
	}

	return filterToken{kind: kind, text: word, pos: start}, i
}

// lexFilterNext scans one non-space token at i.
func lexFilterNext(expr string, i int) (filterToken, int, error) {
	c := expr[i]

	if isFilterIdentStart(c) {
		tok, next := lexFilterIdent(expr, i)
		return tok, next, nil
	}

	if isFilterDigit(c) {
		return lexFilterNumber(expr, i)
	}

	if c == '\'' {
		return lexFilterString(expr, i)
	}

	tok, next, ok := lexFilterSymbol(expr, i)
	if !ok {
		return filterToken{}, 0, &ParseError{Pos: i, Msg: fmt.Sprintf("unexpected character %q", string(c))}
	}

	return tok, next, nil
}

// lexFilter tokenizes one filter expression, naming positions on failure.
//
// Token count above maxFilterTokens refuses loudly with the parse sentinel
// instead of growing without bound.
func lexFilter(expr string) ([]filterToken, error) {
	var toks []filterToken

	i := 0

	for i < len(expr) {
		i = skipFilterWhitespace(expr, i)
		if i >= len(expr) {
			break
		}

		tok, next, err := lexFilterNext(expr, i)
		if err != nil {
			return nil, err
		}

		toks = append(toks, tok)
		if len(toks) > maxFilterTokens {
			return nil, &ParseError{Pos: tok.pos, Msg: fmt.Sprintf("expression exceeds %d tokens", maxFilterTokens)}
		}

		i = next
	}

	toks = append(toks, filterToken{kind: tokEOF, pos: len(expr)})

	return toks, nil
}

// scanFilterDigits advances past ASCII digits starting at i.
func scanFilterDigits(expr string, i int) int {
	for i < len(expr) && isFilterDigit(expr[i]) {
		i++
	}

	return i
}

// scanFilterFraction consumes an optional fraction starting at i.
func scanFilterFraction(expr string, i int) int {
	if i >= len(expr) || expr[i] != '.' {
		return i
	}

	i++

	return scanFilterDigits(expr, i)
}

// scanFilterExponent consumes an exponent starting at e and reports its end.
func scanFilterExponent(expr string, e int) (int, error) {
	j := e + 1

	if j < len(expr) && (expr[j] == '+' || expr[j] == '-') {
		j++
	}

	if j >= len(expr) || !isFilterDigit(expr[j]) {
		return 0, &ParseError{Pos: e, Msg: "malformed number exponent"}
	}

	return scanFilterDigits(expr, j), nil
}

// lexFilterNumber scans one numeric literal starting at a digit.
//
// Integer text without fraction or exponent parses as an exact int64 when
// it fits, so large counters compare exactly instead of through float64.
// Anything else parses as float64. Malformed text refuses loudly.
func lexFilterNumber(expr string, start int) (filterToken, int, error) {
	i := scanFilterDigits(expr, start)
	i = scanFilterFraction(expr, i)

	if i < len(expr) && (expr[i] == 'e' || expr[i] == 'E') {
		next, err := scanFilterExponent(expr, i)
		if err != nil {
			return filterToken{}, 0, err
		}

		i = next
	}

	text := expr[start:i]

	if !strings.ContainsAny(text, ".eE") {
		ival, err := strconv.ParseInt(text, 10, 64)
		if err == nil {
			return filterToken{kind: tokNumber, text: text, num: float64(ival), isInt: true, ival: ival, pos: start}, i, nil
		}
	}

	num, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return filterToken{}, 0, &ParseError{Pos: start, Msg: fmt.Sprintf("malformed number %q", text)}
	}

	return filterToken{kind: tokNumber, text: text, num: num, pos: start}, i, nil
}

// lexFilterString scans one single-quoted literal with ” as escaped quote.
func lexFilterString(expr string, start int) (filterToken, int, error) {
	var sb strings.Builder

	i := start + 1

	for {
		if i >= len(expr) {
			return filterToken{}, 0, &ParseError{Pos: start, Msg: "unterminated string literal"}
		}

		if expr[i] == '\'' {
			if i+1 < len(expr) && expr[i+1] == '\'' {
				sb.WriteByte('\'') //nolint:errcheck // strings.Builder writes never fail
				i += 2

				continue
			}

			i++

			return filterToken{kind: tokString, text: sb.String(), pos: start}, i, nil
		}

		sb.WriteByte(expr[i]) //nolint:errcheck // strings.Builder writes never fail
		i++
	}
}

// lexFilterSingle scans one single-char punctuation or operator token.
func lexFilterSingle(c byte, i int) (filterToken, int, bool) {
	switch c {
	case '(':
		return filterToken{kind: tokLParen, text: "(", pos: i}, i + 1, true
	case ')':
		return filterToken{kind: tokRParen, text: ")", pos: i}, i + 1, true
	case ',':
		return filterToken{kind: tokComma, text: ",", pos: i}, i + 1, true
	case '.':
		return filterToken{kind: tokDot, text: ".", pos: i}, i + 1, true
	case '-':
		return filterToken{kind: tokMinus, text: "-", pos: i}, i + 1, true
	case '+':
		return filterToken{kind: tokPlus, text: "+", pos: i}, i + 1, true
	case '=':
		return filterToken{kind: tokEq, text: "=", pos: i}, i + 1, true
	default:
		return filterToken{}, 0, false
	}
}

// lexFilterLess scans a less-than family token starting with "<".
func lexFilterLess(expr string, i int) (filterToken, int) {
	if i+1 < len(expr) && expr[i+1] == '>' {
		return filterToken{kind: tokNe, text: "<>", pos: i}, i + 2
	}

	if i+1 < len(expr) && expr[i+1] == '=' {
		return filterToken{kind: tokLe, text: "<=", pos: i}, i + 2
	}

	return filterToken{kind: tokLt, text: "<", pos: i}, i + 1
}

// lexFilterGreater scans a greater-than family token starting with ">".
func lexFilterGreater(expr string, i int) (filterToken, int) {
	if i+1 < len(expr) && expr[i+1] == '=' {
		return filterToken{kind: tokGe, text: ">=", pos: i}, i + 2
	}

	return filterToken{kind: tokGt, text: ">", pos: i}, i + 1
}

// lexFilterBang scans a not-equal token starting with "!" or reports miss.
func lexFilterBang(expr string, i int) (filterToken, int, bool) {
	if i+1 < len(expr) && expr[i+1] == '=' {
		return filterToken{kind: tokNe, text: "!=", pos: i}, i + 2, true
	}

	return filterToken{}, 0, false
}

// lexFilterSymbol scans one operator or punctuation token at i.
func lexFilterSymbol(expr string, i int) (filterToken, int, bool) {
	c := expr[i]

	if tok, next, ok := lexFilterSingle(c, i); ok {
		return tok, next, true
	}

	switch c {
	case '<':
		tok, next := lexFilterLess(expr, i)
		return tok, next, true
	case '>':
		tok, next := lexFilterGreater(expr, i)
		return tok, next, true
	case '!':
		return lexFilterBang(expr, i)
	default:
		return filterToken{}, 0, false
	}
}

// filterParser is a recursive descent parser over lexer tokens.
type filterParser struct {
	toks []filterToken
	pos  int
	// depth tracks NOT plus paren nesting against maxFilterDepth.
	depth int
}

// peek returns the current token without consuming it.
func (p *filterParser) peek() filterToken {
	return p.toks[p.pos]
}

// peekAt returns the token n positions ahead, or EOF past the end.
func (p *filterParser) peekAt(n int) filterToken {
	if p.pos+n >= len(p.toks) {
		return filterToken{kind: tokEOF, pos: p.toks[len(p.toks)-1].pos}
	}

	return p.toks[p.pos+n]
}

// next consumes and returns the current token.
func (p *filterParser) next() filterToken {
	tok := p.toks[p.pos]

	if tok.kind != tokEOF {
		p.pos++
	}

	return tok
}

// fail builds a parse error at the current token.
func (p *filterParser) fail(msg string) error {
	return &ParseError{Pos: p.peek().pos, Msg: msg}
}

// filterNode is one parsed expression node, either predicate or operand.
type filterNode interface {
	// isFilterNode marks the node type; the evaluator switches on it.
	isFilterNode()
}

// literalNode holds one parsed constant operand.
type literalNode struct {
	val filterValue
}

// fieldNode holds one dotted field path operand.
type fieldNode struct {
	path []string
	pos  int
}

// cmpNode holds one binary comparison between two operands.
type cmpNode struct {
	op    filterTokenKind
	left  filterNode
	right filterNode
}

// logicNode holds one AND or OR of two predicates.
type logicNode struct {
	op    filterTokenKind
	left  filterNode
	right filterNode
}

// notNode holds one logical negation of a predicate.
type notNode struct {
	inner filterNode
}

// betweenNode holds one [NOT] BETWEEN low AND high predicate.
type betweenNode struct {
	field  filterNode
	low    filterNode
	high   filterNode
	negate bool
}

// inNode holds one [NOT] IN list predicate.
type inNode struct {
	field  filterNode
	items  []filterNode
	negate bool
}

// likeNode holds one [NOT] LIKE pattern predicate.
type likeNode struct {
	field   filterNode
	pattern filterNode
	negate  bool
}

// isFilterNode marks literalNode as a filterNode.
func (literalNode) isFilterNode() {}

// isFilterNode marks fieldNode as a filterNode.
func (fieldNode) isFilterNode() {}

// isFilterNode marks cmpNode as a filterNode.
func (cmpNode) isFilterNode() {}

// isFilterNode marks logicNode as a filterNode.
func (logicNode) isFilterNode() {}

// isFilterNode marks notNode as a filterNode.
func (notNode) isFilterNode() {}

// isFilterNode marks betweenNode as a filterNode.
func (betweenNode) isFilterNode() {}

// isFilterNode marks inNode as a filterNode.
func (inNode) isFilterNode() {}

// isFilterNode marks likeNode as a filterNode.
func (likeNode) isFilterNode() {}

// parseFilter parses one full expression and requires EOF at the end.
func parseFilter(toks []filterToken) (filterNode, error) {
	p := &filterParser{toks: toks}

	root, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokEOF {
		return nil, p.fail("unexpected trailing input")
	}

	return root, nil
}

// parseOr parses AND expressions joined by OR.
func (p *filterParser) parseOr() (filterNode, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.peek().kind == tokOr {
		op := p.next()

		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}

		left = logicNode{op: op.kind, left: left, right: right}
	}

	return left, nil
}

// parseAnd parses NOT expressions joined by AND.
func (p *filterParser) parseAnd() (filterNode, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.peek().kind == tokAnd {
		op := p.next()

		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		left = logicNode{op: op.kind, left: left, right: right}
	}

	return left, nil
}

// parseNot parses one optional NOT prefix.
//
// NOT chains nest against maxFilterDepth so adversarial input cannot
// exhaust the call stack.
func (p *filterParser) parseNot() (filterNode, error) {
	if p.peek().kind == tokNot {
		if p.depth >= maxFilterDepth {
			return nil, p.fail(fmt.Sprintf("filter nesting exceeds %d", maxFilterDepth))
		}
		p.depth++
		defer func() { p.depth-- }()

		p.next()

		inner, err := p.parseNot()
		if err != nil {
			return nil, err
		}

		return notNode{inner: inner}, nil
	}

	return p.parsePredicate()
}

// parsePredicate parses one comparison, BETWEEN, IN, LIKE, group, or bare operand.
//
// Parenthesized groups nest against maxFilterDepth so adversarial input
// cannot exhaust the call stack.
// parseParenPredicate parses a parenthesized subexpression. The caller has
// already seen the opening parenthesis; depth is enforced before descending.
func (p *filterParser) parseParenPredicate() (filterNode, error) {
	if p.depth >= maxFilterDepth {
		return nil, p.fail(fmt.Sprintf("filter nesting exceeds %d", maxFilterDepth))
	}
	p.depth++
	defer func() { p.depth-- }()

	p.next()

	inner, err := p.parseOr()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokRParen {
		return nil, p.fail("missing closing parenthesis")
	}

	p.next()

	return inner, nil
}

// parseNotSuffix parses NOT followed by BETWEEN, IN, or LIKE.
func (p *filterParser) parseNotSuffix(left filterNode) (filterNode, error) {
	switch p.peekAt(1).kind { //nolint:exhaustive // default rejects NOT not followed by BETWEEN, IN, or LIKE
	case tokBetween, tokIn, tokLike:
		p.next()

		return p.parseSuffix(left, true)
	default:
		return nil, p.fail("NOT must precede AND, OR, BETWEEN, IN, or LIKE")
	}
}

func (p *filterParser) parsePredicate() (filterNode, error) {
	if p.peek().kind == tokLParen {
		return p.parseParenPredicate()
	}

	left, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	switch p.peek().kind { //nolint:exhaustive // default returns bare operand for non-operator tokens
	case tokEq, tokNe, tokLt, tokLe, tokGt, tokGe:
		op := p.next()

		right, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		return cmpNode{op: op.kind, left: left, right: right}, nil
	case tokBetween, tokIn, tokLike:
		return p.parseSuffix(left, false)
	case tokNot:
		return p.parseNotSuffix(left)
	default:
		return left, nil
	}
}

// parseBetweenTail parses low AND high after a BETWEEN token.
func (p *filterParser) parseBetweenTail(field filterNode, negate bool) (filterNode, error) {
	p.next()

	low, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokAnd {
		return nil, p.fail("BETWEEN needs AND between bounds")
	}

	p.next()

	high, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	return betweenNode{field: field, low: low, high: high, negate: negate}, nil
}

// parseInTail parses a parenthesized item list after an IN token.
func (p *filterParser) parseInTail(field filterNode, negate bool) (filterNode, error) {
	p.next()

	if p.peek().kind != tokLParen {
		return nil, p.fail("IN needs a parenthesized list")
	}

	p.next()

	if p.peek().kind == tokRParen {
		return nil, p.fail("IN needs at least one item")
	}

	items, err := p.parseInItems()
	if err != nil {
		return nil, err
	}

	if p.peek().kind != tokRParen {
		return nil, p.fail("missing closing parenthesis for IN list")
	}

	p.next()

	return inNode{field: field, items: items, negate: negate}, nil
}

// parseInItems parses comma-separated operands until a non-comma token.
func (p *filterParser) parseInItems() ([]filterNode, error) {
	var items []filterNode

	for {
		item, err := p.parseOperand()
		if err != nil {
			return nil, err
		}

		items = append(items, item)

		if p.peek().kind != tokComma {
			break
		}

		p.next()
	}

	return items, nil
}

// parseLikeTail parses a pattern operand after a LIKE token.
func (p *filterParser) parseLikeTail(field filterNode, negate bool) (filterNode, error) {
	p.next()

	pattern, err := p.parseOperand()
	if err != nil {
		return nil, err
	}

	return likeNode{field: field, pattern: pattern, negate: negate}, nil
}

// parseSuffix parses one BETWEEN, IN, or LIKE tail after its field operand.
func (p *filterParser) parseSuffix(field filterNode, negate bool) (filterNode, error) {
	switch p.peek().kind { //nolint:exhaustive // default rejects non-suffix tails with a typed error
	case tokBetween:
		return p.parseBetweenTail(field, negate)
	case tokIn:
		return p.parseInTail(field, negate)
	case tokLike:
		return p.parseLikeTail(field, negate)
	default:
		return nil, p.fail("expected BETWEEN, IN, or LIKE")
	}
}

// parseOperandField parses a dotted field path starting at tok.
func (p *filterParser) parseOperandField(tok filterToken) (filterNode, error) {
	p.next()

	path := []string{tok.text}

	for p.peek().kind == tokDot {
		p.next()

		next, err := p.nextIdent()
		if err != nil {
			return nil, err
		}

		path = append(path, next.text)
	}

	return fieldNode{path: path, pos: tok.pos}, nil
}

// parseOperandSigned parses a signed numeric literal starting at sign.
//
// Integer text without fraction or exponent stays an exact int64, including
// the minimum int64 spelled as minus plus 9223372036854775808. Anything
// else stays float64.
func (p *filterParser) parseOperandSigned(sign filterToken) (filterNode, error) {
	p.next()

	num, err := p.nextNumber()
	if err != nil {
		return nil, err
	}

	if num.isInt {
		if sign.kind == tokMinus {
			return literalNode{val: numSignedValue(-num.ival)}, nil
		}

		return literalNode{val: numSignedValue(num.ival)}, nil
	}

	if sign.kind == tokMinus && !strings.ContainsAny(num.text, ".eE") {
		negText := "-" + num.text
		ival, perr := strconv.ParseInt(negText, 10, 64)
		if perr == nil {
			return literalNode{val: numSignedValue(ival)}, nil
		}
	}

	if sign.kind == tokMinus {
		num.num = -num.num
	}

	return literalNode{val: numFloatValue(num.num)}, nil
}

// parseOperandAtom parses one unsigned literal token.
//
// Integer text without fraction or exponent becomes an exact int64 value
// when it fits; anything else becomes float64.
func (p *filterParser) parseOperandAtom(tok filterToken) (filterNode, error) {
	switch tok.kind { //nolint:exhaustive // default rejects non-literal tokens with a typed error
	case tokNumber:
		p.next()

		if tok.isInt {
			return literalNode{val: numSignedValue(tok.ival)}, nil
		}

		return literalNode{val: numFloatValue(tok.num)}, nil
	case tokString:
		p.next()

		return literalNode{val: filterValue{kind: fvStr, str: tok.text}}, nil
	case tokTrue:
		p.next()

		return literalNode{val: filterValue{kind: fvBool, boolean: true}}, nil
	case tokFalse:
		p.next()

		return literalNode{val: filterValue{kind: fvBool, boolean: false}}, nil
	case tokNull:
		p.next()

		return literalNode{val: filterValue{kind: fvNull}}, nil
	default:
		return nil, p.fail("expected a field name or a value")
	}
}

// parseOperand parses one field path, literal, or signed number.
func (p *filterParser) parseOperand() (filterNode, error) {
	tok := p.peek()

	switch tok.kind { //nolint:exhaustive // default rejects non-operand tokens with a typed error
	case tokIdent:
		return p.parseOperandField(tok)
	case tokMinus, tokPlus:
		return p.parseOperandSigned(tok)
	case tokNumber, tokString, tokTrue, tokFalse, tokNull:
		return p.parseOperandAtom(tok)
	default:
		return nil, p.fail("expected a field name or a value")
	}
}

// nextIdent consumes one identifier token or reports a parse error.
func (p *filterParser) nextIdent() (filterToken, error) {
	tok := p.peek()

	if tok.kind != tokIdent {
		return filterToken{}, p.fail("expected a field name after dot")
	}

	p.next()

	return tok, nil
}

// nextNumber consumes one numeric token or reports a parse error.
func (p *filterParser) nextNumber() (filterToken, error) {
	tok := p.peek()

	if tok.kind != tokNumber {
		return filterToken{}, p.fail("expected a number after sign")
	}

	p.next()

	return tok, nil
}

// filterValueKind names one evaluated value family.
type filterValueKind int

// Evaluated value families for filter operands.
const (
	fvNull filterValueKind = iota
	fvNum
	fvStr
	fvBool
	fvUnsupported
)

// numFlavor names the exact numeric storage behind one number.
type numFlavor int

// Numeric storage flavors, kept exact until a float forces promotion.
const (
	numSigned numFlavor = iota
	numUnsigned
	numFloat
)

// filterNum holds one number with exact integer storage when possible.
type filterNum struct {
	flavor numFlavor
	f      float64
	i      int64
	u      uint64
}

// filterValue is one evaluated operand: null, number, string, or bool.
type filterValue struct {
	kind    filterValueKind
	num     filterNum
	str     string
	boolean bool
	note    string
}

// numFloatValue wraps one float64 as a float number value.
func numFloatValue(f float64) filterValue {
	return filterValue{kind: fvNum, num: filterNum{flavor: numFloat, f: f}}
}

// numSignedValue wraps one int64 as a signed number value.
func numSignedValue(i int64) filterValue {
	return filterValue{kind: fvNum, num: filterNum{flavor: numSigned, i: i, f: float64(i)}}
}

// numUnsignedValue wraps one uint64 as an unsigned number value.
func numUnsignedValue(u uint64) filterValue {
	return filterValue{kind: fvNum, num: filterNum{flavor: numUnsigned, u: u, f: float64(u)}}
}

// valueKindName names one value kind for eval error text.
func valueKindName(kind filterValueKind) string {
	switch kind {
	case fvNull:
		return "null"
	case fvNum:
		return "number"
	case fvStr:
		return "string"
	case fvBool:
		return "bool"
	case fvUnsupported:
		return "unsupported"
	default:
		return "unsupported"
	}
}

// cmpFloat64 orders two float64 values.
func cmpFloat64(af, bf float64) int {
	switch {
	case af < bf:
		return -1
	case af > bf:
		return 1
	default:
		return 0
	}
}

// cmpInt64 orders two int64 values.
func cmpInt64(ai, bi int64) int {
	switch {
	case ai < bi:
		return -1
	case ai > bi:
		return 1
	default:
		return 0
	}
}

// cmpUint64 orders two uint64 values.
func cmpUint64(au, bu uint64) int {
	switch {
	case au < bu:
		return -1
	case au > bu:
		return 1
	default:
		return 0
	}
}

// compareMixedNonNegative orders a non-negative signed value against unsigned.
func compareMixedNonNegative(aIsSigned bool, au, bu uint64) int {
	c := cmpUint64(au, bu)
	if c == 0 {
		return 0
	}

	if aIsSigned {
		return c
	}

	return -c
}

// compareMixedNums orders one signed and one unsigned number.
func compareMixedNums(a, b filterNum) int {
	aIsSigned := a.flavor == numSigned

	var si int64

	var u uint64

	if aIsSigned {
		si = a.i
		u = b.u
	} else {
		si = b.i
		u = a.u
	}

	if si < 0 {
		if aIsSigned {
			return -1
		}

		return 1
	}

	return compareMixedNonNegative(aIsSigned, uint64(si), u)
}

// compareFilterNums orders two numbers, promoting to float when needed.
func compareFilterNums(a, b filterNum) int {
	if a.flavor == numFloat || b.flavor == numFloat {
		return cmpFloat64(a.f, b.f)
	}

	if a.flavor == numSigned && b.flavor == numSigned {
		return cmpInt64(a.i, b.i)
	}

	if a.flavor == numUnsigned && b.flavor == numUnsigned {
		return cmpUint64(a.u, b.u)
	}

	return compareMixedNums(a, b)
}

// likeMatch reports whether s matches pattern with % and _ wildcards.
//
// Percent matches any run including empty, underscore matches exactly one
// rune, and all other runes match literally. Matching runs on runes, not
// bytes, so multibyte characters count as one. Matching is case sensitive.
func likeMatch(s, pattern string) bool {
	sRunes := []rune(s)
	pRunes := []rune(pattern)
	si, pi := 0, 0
	star, mark := -1, 0

	for si < len(sRunes) {
		if pi < len(pRunes) && (pRunes[pi] == '_' || pRunes[pi] == sRunes[si]) {
			si++
			pi++

			continue
		}

		if pi < len(pRunes) && pRunes[pi] == '%' {
			star = pi
			mark = si
			pi++

			continue
		}

		if star != -1 {
			pi = star + 1
			mark++
			si = mark

			continue
		}

		return false
	}

	for pi < len(pRunes) && pRunes[pi] == '%' {
		pi++
	}

	return pi == len(pRunes)
}

// decodeFilterFields decodes one XCDR1 sample into dotted-path field values.
//
// Nested messages flatten with dots, so header.frame_id addresses the nested
// string. Array and sequence members are skipped to keep later offsets
// right and recorded as unsupported so referencing them fails loudly.
// Truncated input, unknown nested tables, and trailing bytes all yield eval
// errors, never a panic.
func decodeFilterFields(payload []byte, desc *types.MessageMembers) (map[string]filterValue, error) {
	if desc == nil {
		return nil, errors.New("dds: content filter eval nil descriptor") //nolint:wrapcheck // new error, nothing to wrap
	}

	dec, err := types.NewDecoder(payload)
	if err != nil {
		return nil, fmt.Errorf("dds: content filter eval header: %w", err)
	}

	out := map[string]filterValue{}

	err = decodeFilterStruct(dec, desc, "", out)
	if err != nil {
		return nil, err
	}

	err = dec.Finish()
	if err != nil {
		return nil, fmt.Errorf("dds: content filter eval trailing bytes: %w", err)
	}

	return out, nil
}

// decodeFilterArrayMember skips one array member and marks it unsupported.
func decodeFilterArrayMember(dec *types.Decoder, m *types.MessageMember, key string, out map[string]filterValue) error {
	err := skipFilterMember(dec, m)
	if err != nil {
		return fmt.Errorf("dds: content filter eval skip %q: %w", key, err)
	}

	out[key] = filterValue{kind: fvUnsupported, note: key}

	return nil
}

// decodeFilterNestedMember decodes one nested message into dotted keys.
func decodeFilterNestedMember(dec *types.Decoder, m *types.MessageMember, key string, out map[string]filterValue) error {
	nested, err := nestedFilterMembers(m)
	if err != nil {
		return fmt.Errorf("dds: content filter eval nested %q: %w", key, err)
	}

	err = decodeFilterStruct(dec, nested, key+".", out)
	if err != nil {
		return err
	}

	return nil
}

// decodeFilterScalarMember reads one scalar member into dotted keys.
func decodeFilterScalarMember(dec *types.Decoder, m *types.MessageMember, key string, out map[string]filterValue) error {
	val, err := readFilterScalar(dec, m)
	if err != nil {
		return fmt.Errorf("dds: content filter eval field %q: %w", key, err)
	}

	out[key] = val

	return nil
}

// decodeFilterMember decodes one member in field order into flat dotted keys.
func decodeFilterMember(dec *types.Decoder, m *types.MessageMember, prefix string, out map[string]filterValue) error {
	key := prefix + m.Name

	if m.IsArray {
		return decodeFilterArrayMember(dec, m, key, out)
	}

	if m.IsNested() {
		return decodeFilterNestedMember(dec, m, key, out)
	}

	return decodeFilterScalarMember(dec, m, key, out)
}

// decodeFilterStruct decodes one message in field order into flat dotted keys.
func decodeFilterStruct(dec *types.Decoder, desc *types.MessageMembers, prefix string, out map[string]filterValue) error {
	for i := range desc.Members {
		m := &desc.Members[i]

		err := decodeFilterMember(dec, m, prefix, out)
		if err != nil {
			return err
		}
	}

	return nil
}

// nestedFilterMembers returns the nested table, resolving lazy references.
func nestedFilterMembers(m *types.MessageMember) (*types.MessageMembers, error) {
	if m.Members != nil {
		return m.Members, nil
	}

	nested, err := m.NestedMembers()
	if err != nil {
		return nil, fmt.Errorf("dds: content filter eval resolve nested: %w", err)
	}

	return nested, nil
}

// readFilterFloatScalar reads one float member by type ID.
func readFilterFloatScalar(dec *types.Decoder, id types.RosTypeID) (filterValue, error) {
	switch id { //nolint:exhaustive // default rejects non-float type IDs for this family
	case types.RosTypeFloat:
		v, err := dec.ReadFloat32()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numFloatValue(float64(v)), nil
	case types.RosTypeDouble:
		v, err := dec.ReadFloat64()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numFloatValue(v), nil
	default:
		return filterValue{}, fmt.Errorf("dds: content filter eval unsupported type id %d", uint8(id))
	}
}

// readFilterSignedScalar reads one signed integer member by type ID.
func readFilterSignedScalar(dec *types.Decoder, id types.RosTypeID) (filterValue, error) {
	switch id { //nolint:exhaustive // default rejects non-signed type IDs for this family
	case types.RosTypeChar, types.RosTypeInt8:
		v, err := dec.ReadInt8()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numSignedValue(int64(v)), nil
	case types.RosTypeInt16:
		v, err := dec.ReadInt16()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numSignedValue(int64(v)), nil
	case types.RosTypeInt32:
		v, err := dec.ReadInt32()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numSignedValue(int64(v)), nil
	case types.RosTypeInt64:
		v, err := dec.ReadInt64()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numSignedValue(v), nil
	default:
		return filterValue{}, fmt.Errorf("dds: content filter eval unsupported type id %d", uint8(id))
	}
}

// readFilterUnsignedScalar reads one unsigned integer member by type ID.
func readFilterUnsignedScalar(dec *types.Decoder, id types.RosTypeID) (filterValue, error) {
	switch id { //nolint:exhaustive // default rejects non-unsigned type IDs for this family
	case types.RosTypeOctet, types.RosTypeUint8:
		v, err := dec.ReadUint8()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numUnsignedValue(uint64(v)), nil
	case types.RosTypeUint16:
		v, err := dec.ReadUint16()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numUnsignedValue(uint64(v)), nil
	case types.RosTypeWChar:
		v, err := dec.ReadWchar()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numUnsignedValue(uint64(v)), nil
	case types.RosTypeUint32:
		v, err := dec.ReadUint32()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numUnsignedValue(uint64(v)), nil
	case types.RosTypeUint64:
		v, err := dec.ReadUint64()
		if err != nil {
			return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
		}

		return numUnsignedValue(v), nil
	default:
		return filterValue{}, fmt.Errorf("dds: content filter eval unsupported type id %d", uint8(id))
	}
}

// readFilterBoolScalar reads one boolean member.
func readFilterBoolScalar(dec *types.Decoder) (filterValue, error) {
	v, err := dec.ReadBool()
	if err != nil {
		return filterValue{}, fmt.Errorf("dds: content filter eval read: %w", err)
	}

	return filterValue{kind: fvBool, boolean: v}, nil
}

// readFilterTextScalar reads one string or wstring member with bounds.
func readFilterTextScalar(dec *types.Decoder, m *types.MessageMember) (filterValue, error) {
	switch m.TypeID { //nolint:exhaustive // default rejects non-text type IDs for this family
	case types.RosTypeString:
		v, err := readFilterString(dec, m)
		if err != nil {
			return filterValue{}, err
		}

		return filterValue{kind: fvStr, str: v}, nil
	case types.RosTypeWString:
		v, err := readFilterWString(dec, m)
		if err != nil {
			return filterValue{}, err
		}

		return filterValue{kind: fvStr, str: v}, nil
	default:
		return filterValue{}, fmt.Errorf("dds: content filter eval unsupported type id %d", uint8(m.TypeID))
	}
}

// readFilterScalar reads one scalar member with CDR decoder primitives.
func readFilterScalar(dec *types.Decoder, m *types.MessageMember) (filterValue, error) {
	switch m.TypeID {
	case types.RosTypeFloat, types.RosTypeDouble:
		return readFilterFloatScalar(dec, m.TypeID)
	case types.RosTypeChar, types.RosTypeInt8, types.RosTypeInt16, types.RosTypeInt32, types.RosTypeInt64:
		return readFilterSignedScalar(dec, m.TypeID)
	case types.RosTypeOctet, types.RosTypeUint8, types.RosTypeUint16, types.RosTypeWChar, types.RosTypeUint32, types.RosTypeUint64:
		return readFilterUnsignedScalar(dec, m.TypeID)
	case types.RosTypeBoolean:
		return readFilterBoolScalar(dec)
	case types.RosTypeString, types.RosTypeWString:
		return readFilterTextScalar(dec, m)
	case types.RosTypeLongDbl, types.RosTypeMessage:
		return filterValue{}, fmt.Errorf("dds: content filter eval unsupported type id %d", uint8(m.TypeID))
	default:
		return filterValue{}, fmt.Errorf("dds: content filter eval unsupported type id %d", uint8(m.TypeID))
	}
}

// readFilterString reads one string honoring the member upper bound.
func readFilterString(dec *types.Decoder, m *types.MessageMember) (string, error) {
	var (
		v   string
		err error
	)

	if m.StringUpperBound > 0 {
		v, err = dec.ReadBoundedString(int(m.StringUpperBound))
	} else {
		v, err = dec.ReadString()
	}

	if err != nil {
		return "", fmt.Errorf("dds: content filter eval read: %w", err)
	}

	return v, nil
}

// readFilterWString reads one wstring honoring the member upper bound.
func readFilterWString(dec *types.Decoder, m *types.MessageMember) (string, error) {
	var (
		v   string
		err error
	)

	if m.StringUpperBound > 0 {
		v, err = dec.ReadBoundedWString(int(m.StringUpperBound))
	} else {
		v, err = dec.ReadWString()
	}

	if err != nil {
		return "", fmt.Errorf("dds: content filter eval read: %w", err)
	}

	return v, nil
}

// skipFilterMember skips one array or sequence member, discarding elements.
func skipFilterMember(dec *types.Decoder, m *types.MessageMember) error {
	count, err := filterArrayCount(dec, m)
	if err != nil {
		return err
	}

	if m.IsNested() {
		nested, err := nestedFilterMembers(m)
		if err != nil {
			return err
		}

		for range count {
			discard := map[string]filterValue{}

			err = decodeFilterStruct(dec, nested, "", discard)
			if err != nil {
				return err
			}
		}

		return nil
	}

	for range count {
		_, err = readFilterScalar(dec, m)
		if err != nil {
			return err
		}
	}

	return nil
}

// filterArrayCount reads one element count for fixed or sequence members.
func filterArrayCount(dec *types.Decoder, m *types.MessageMember) (int, error) {
	if !m.IsUpperBound {
		return int(m.ArraySize), nil
	}

	var (
		n   int
		err error
	)

	if m.ArraySize > 0 {
		n, err = dec.ReadBoundedSequenceLength(int(m.ArraySize))
	} else {
		n, err = dec.ReadSequenceLength()
	}

	if err != nil {
		return 0, fmt.Errorf("dds: content filter eval read length: %w", err)
	}

	return n, nil
}

// evalFilterValue evaluates one operand node to a value.
func evalFilterValue(node filterNode, fields map[string]filterValue) (filterValue, error) {
	switch n := node.(type) {
	case literalNode:
		return n.val, nil
	case fieldNode:
		key := strings.Join(n.path, ".")

		val, ok := fields[key]
		if !ok {
			return filterValue{}, fmt.Errorf("dds: content filter eval unknown field %q", key)
		}

		if val.kind == fvUnsupported {
			return filterValue{}, fmt.Errorf("dds: content filter eval field %q is an array and not filterable", key)
		}

		return val, nil
	default:
		truth, err := evalFilterPredicate(node, fields)
		if err != nil {
			return filterValue{}, err
		}

		return filterValue{kind: fvBool, boolean: truth}, nil
	}
}

// evalFilterLogic evaluates one AND or OR of two predicates.
//
// Evaluation is lazy with short-circuit semantics: AND skips the right side
// when the left is false and OR skips the right side when the left is true,
// so an error on the skipped side never surfaces. Documented by design.
func evalFilterLogic(n logicNode, fields map[string]filterValue) (bool, error) {
	left, err := evalFilterPredicate(n.left, fields)
	if err != nil {
		return false, err
	}

	if n.op == tokAnd {
		if !left {
			return false, nil
		}

		right, err := evalFilterPredicate(n.right, fields)
		if err != nil {
			return false, err
		}

		return left && right, nil
	}

	if left {
		return true, nil
	}

	right, err := evalFilterPredicate(n.right, fields)
	if err != nil {
		return false, err
	}

	return left || right, nil
}

// evalFilterNot evaluates one logical negation of a predicate.
func evalFilterNot(n notNode, fields map[string]filterValue) (bool, error) {
	inner, err := evalFilterPredicate(n.inner, fields)
	if err != nil {
		return false, err
	}

	return !inner, nil
}

// evalFilterBarePredicate evaluates one bare operand as a boolean predicate.
func evalFilterBarePredicate(node filterNode, fields map[string]filterValue) (bool, error) {
	val, err := evalFilterValue(node, fields)
	if err != nil {
		return false, err
	}

	if val.kind != fvBool {
		return false, fmt.Errorf(
			"dds: content filter eval predicate is %s, want bool",
			valueKindName(val.kind),
		)
	}

	return val.boolean, nil
}

// evalFilterPredicate evaluates one predicate node to a bool.
func evalFilterPredicate(node filterNode, fields map[string]filterValue) (bool, error) {
	switch n := node.(type) {
	case logicNode:
		return evalFilterLogic(n, fields)
	case notNode:
		return evalFilterNot(n, fields)
	case cmpNode:
		return evalFilterCmp(n, fields)
	case betweenNode:
		return evalFilterBetween(n, fields)
	case inNode:
		return evalFilterIn(n, fields)
	case likeNode:
		return evalFilterLike(n, fields)
	default:
		return evalFilterBarePredicate(node, fields)
	}
}

// evalFilterCmp evaluates one binary comparison of two operands.
func evalFilterCmp(n cmpNode, fields map[string]filterValue) (bool, error) {
	left, err := evalFilterValue(n.left, fields)
	if err != nil {
		return false, err
	}

	right, err := evalFilterValue(n.right, fields)
	if err != nil {
		return false, err
	}

	cmp, err := compareFilterValues(n.op, left, right)
	if err != nil {
		return false, err
	}

	return cmp, nil
}

// compareFilterValues applies one comparison operator to two values.
//
// NaN never matches: any comparison with a NaN operand reports false with
// no error, including equality and inequality, so corrupt floats filter out
// instead of matching or erroring.
// compareFilterNumsOrdered applies one operator to two numbers.
//
// NaN never matches: any comparison with a NaN operand reports false with
// no error, including equality and inequality, so corrupt floats filter out
// instead of matching or erroring.
func compareFilterNumsOrdered(op filterTokenKind, left, right filterNum) (bool, error) {
	if isFilterNaN(left) || isFilterNaN(right) {
		return false, nil
	}

	return compareFilterOp(op, compareFilterNums(left, right)), nil
}

// compareFilterBool applies one operator to two booleans.
//
// Only equality operators apply; ordering operators fail loudly.
func compareFilterBool(op filterTokenKind, left, right bool) (bool, error) {
	switch op { //nolint:exhaustive // default rejects non-equality bool operators with a typed error
	case tokEq:
		return left == right, nil
	case tokNe:
		return left != right, nil
	default:
		return false, errors.New("dds: content filter eval bool supports only = and <>") //nolint:wrapcheck // new error, nothing to wrap
	}
}

func compareFilterValues(op filterTokenKind, left, right filterValue) (bool, error) {
	if left.kind == fvNull || right.kind == fvNull {
		return false, errors.New("dds: content filter eval null comparison") //nolint:wrapcheck // new error, nothing to wrap
	}

	if left.kind != right.kind {
		return false, fmt.Errorf(
			"dds: content filter eval type mismatch %s versus %s",
			valueKindName(left.kind),
			valueKindName(right.kind),
		)
	}

	switch left.kind {
	case fvNum:
		return compareFilterNumsOrdered(op, left.num, right.num)
	case fvStr:
		return compareFilterOp(op, strings.Compare(left.str, right.str)), nil
	case fvBool:
		return compareFilterBool(op, left.boolean, right.boolean)
	case fvNull, fvUnsupported:
		return false, fmt.Errorf(
			"dds: content filter eval cannot compare %s values",
			valueKindName(left.kind),
		)
	default:
		return false, fmt.Errorf(
			"dds: content filter eval cannot compare %s values",
			valueKindName(left.kind),
		)
	}
}

// isFilterNaN reports whether n is a float NaN.
//
// Only float flavor can be NaN; integer flavors never are.
func isFilterNaN(n filterNum) bool {
	if n.flavor != numFloat {
		return false
	}

	return math.IsNaN(n.f)
}

// compareFilterOp maps one ordering to one comparison operator result.
func compareFilterOp(op filterTokenKind, cmp int) bool {
	switch op { //nolint:exhaustive // default treats greater-or-equal as the ordering fallback for comparison operators
	case tokEq:
		return cmp == 0
	case tokNe:
		return cmp != 0
	case tokLt:
		return cmp < 0
	case tokLe:
		return cmp <= 0
	case tokGt:
		return cmp > 0
	case tokGe:
		return cmp >= 0
	default:
		return cmp >= 0
	}
}

// evalFilterBetween evaluates one inclusive BETWEEN predicate.
func evalFilterBetween(n betweenNode, fields map[string]filterValue) (bool, error) {
	val, err := evalFilterValue(n.field, fields)
	if err != nil {
		return false, err
	}

	low, err := evalFilterValue(n.low, fields)
	if err != nil {
		return false, err
	}

	high, err := evalFilterValue(n.high, fields)
	if err != nil {
		return false, err
	}

	loOK, err := compareFilterValues(tokGe, val, low)
	if err != nil {
		return false, err
	}

	hiOK, err := compareFilterValues(tokLe, val, high)
	if err != nil {
		return false, err
	}

	matched := loOK && hiOK

	if n.negate {
		return !matched, nil
	}

	return matched, nil
}

// evalFilterIn evaluates one IN list predicate with strict element types.
func evalFilterIn(n inNode, fields map[string]filterValue) (bool, error) {
	val, err := evalFilterValue(n.field, fields)
	if err != nil {
		return false, err
	}

	matched := false

	for _, item := range n.items {
		candidate, err := evalFilterValue(item, fields)
		if err != nil {
			return false, err
		}

		eq, err := compareFilterValues(tokEq, val, candidate)
		if err != nil {
			return false, err
		}

		if eq {
			matched = true
		}
	}

	if n.negate {
		return !matched, nil
	}

	return matched, nil
}

// evalFilterLike evaluates one LIKE predicate with % and _ wildcards.
func evalFilterLike(n likeNode, fields map[string]filterValue) (bool, error) {
	val, err := evalFilterValue(n.field, fields)
	if err != nil {
		return false, err
	}

	pattern, err := evalFilterValue(n.pattern, fields)
	if err != nil {
		return false, err
	}

	if val.kind != fvStr || pattern.kind != fvStr {
		return false, fmt.Errorf(
			"dds: content filter eval LIKE needs strings, have %s LIKE %s",
			valueKindName(val.kind),
			valueKindName(pattern.kind),
		)
	}

	matched := likeMatch(val.str, pattern.str)

	if n.negate {
		return !matched, nil
	}

	return matched, nil
}

// ContentFilter is a compiled filter expression for one subscription.
//
// Compile once with CompileContentFilter, then call Evaluate or Apply per
// sample. The parsed tree is immutable after compile, so concurrent
// Evaluate calls are safe without locking.
type ContentFilter struct {
	expr string
	root filterNode
}

// CompileContentFilter parses one filter expression for later evaluation.
//
// An empty expression or any grammar violation returns an error joining
// ErrContentFilterParse with a *ParseError naming the byte position.
func CompileContentFilter(expr string) (*ContentFilter, error) {
	if strings.TrimSpace(expr) == "" {
		perr := &ParseError{Pos: 0, Msg: "empty expression"}

		return nil, fmt.Errorf("dds: compile content filter: %w", errors.Join(ErrContentFilterParse, perr))
	}

	toks, err := lexFilter(expr)
	if err != nil {
		return nil, fmt.Errorf("dds: compile content filter: %w", errors.Join(ErrContentFilterParse, err))
	}

	root, err := parseFilter(toks)
	if err != nil {
		return nil, fmt.Errorf("dds: compile content filter: %w", errors.Join(ErrContentFilterParse, err))
	}

	return &ContentFilter{expr: expr, root: root}, nil
}

// Expression returns the source text this filter was compiled from.
func (f *ContentFilter) Expression() string {
	if f == nil {
		return ""
	}

	return f.expr
}

// Evaluate matches one XCDR1 payload against the compiled filter.
//
// It returns true plus nil when the sample matches, false plus nil when
// the sample decoded but did not match, and false plus an error joining
// ErrContentFilterEvalError when the sample cannot be evaluated, such as
// a truncated payload, an unknown field, or a type mismatch. A nil filter
// or descriptor is an eval error. It never panics.
func (f *ContentFilter) Evaluate(payload []byte, desc *types.MessageMembers) (bool, error) {
	if f == nil || f.root == nil {
		return false, fmt.Errorf("dds: content filter eval: %w", errors.Join(
			ErrContentFilterEvalError,
			errors.New("dds: content filter eval nil filter"),
		))
	}

	fields, err := decodeFilterFields(payload, desc)
	if err != nil {
		return false, fmt.Errorf("dds: content filter eval: %w", errors.Join(ErrContentFilterEvalError, err))
	}

	matched, err := evalFilterPredicate(f.root, fields)
	if err != nil {
		return false, fmt.Errorf("dds: content filter eval: %w", errors.Join(ErrContentFilterEvalError, err))
	}

	return matched, nil
}

// Apply evaluates one sample and classifies the drop accounting outcome.
//
// A match returns true with an empty reason and nil error. A miss returns
// false with DropReasonContentFilterMiss and an error wrapping
// ErrContentFilterMiss. An eval failure returns false with
// DropReasonContentFilterEvalError and the joined eval error. The take
// path hook can feed the returned reason straight into countDropReason.
func (f *ContentFilter) Apply(payload []byte, desc *types.MessageMembers) (bool, DropReason, error) {
	matched, err := f.Evaluate(payload, desc)
	if err != nil {
		if errors.Is(err, ErrContentFilterEvalError) {
			return false, DropReasonContentFilterEvalError, err
		}

		return false, DropReasonContentFilterEvalError, fmt.Errorf("dds: content filter apply: %w", errors.Join(ErrContentFilterEvalError, err))
	}

	if !matched {
		expr := f.Expression()

		return false, DropReasonContentFilterMiss, fmt.Errorf("dds: content filter miss %q: %w", expr, ErrContentFilterMiss)
	}

	return true, "", nil
}
