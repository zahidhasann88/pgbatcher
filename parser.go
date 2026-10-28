// Package pgbatcher coalesces explicitly opted-in single-row PostgreSQL INSERTs.
package pgbatcher

import (
	"fmt"
	"strings"
	"unicode"
)

type token struct {
	s          string
	kind       byte
	start, end int
}
type insertShape struct {
	prefix, suffix         string
	valuesStart, valuesEnd int
	placeholderCount       int
}

func tokenize(sql string) ([]token, error) {
	out := make([]token, 0, 32)
	for i := 0; i < len(sql); {
		c := sql[i]
		if c == 0 {
			return nil, fmt.Errorf("NUL in SQL")
		}
		if c == ' ' || c == '\t' || c == '\r' || c == '\n' || c == '\f' {
			i++
			continue
		}
		if c == '-' && i+1 < len(sql) && sql[i+1] == '-' {
			i += 2
			for i < len(sql) && sql[i] != '\n' {
				i++
			}
			continue
		}
		if c == '/' && i+1 < len(sql) && sql[i+1] == '*' {
			i += 2
			depth := 1
			for i < len(sql) && depth > 0 {
				if i+1 < len(sql) && sql[i:i+2] == "/*" {
					depth++
					i += 2
				} else if i+1 < len(sql) && sql[i:i+2] == "*/" {
					depth--
					i += 2
				} else {
					i++
				}
			}
			if depth != 0 {
				return nil, fmt.Errorf("unterminated comment")
			}
			continue
		}
		start := i
		if c == '\'' || c == '"' {
			quote := c
			i++
			for i < len(sql) {
				if sql[i] == quote {
					if i+1 < len(sql) && sql[i+1] == quote {
						i += 2
						continue
					}
					i++
					break
				}
				if sql[i] == '\\' && quote == '\'' && i+1 < len(sql) {
					i += 2
				} else {
					i++
				}
			}
			if i > len(sql) || sql[i-1] != quote {
				return nil, fmt.Errorf("unterminated quote")
			}
			out = append(out, token{sql[start:i], quote, start, i})
			continue
		}
		if c == '$' {
			j := i + 1
			for j < len(sql) && (sql[j] == '_' || unicode.IsLetter(rune(sql[j])) || unicode.IsDigit(rune(sql[j]))) {
				j++
			}
			if j < len(sql) && sql[j] == '$' {
				delim := sql[i : j+1]
				k := strings.Index(sql[j+1:], delim)
				if k < 0 {
					return nil, fmt.Errorf("unterminated dollar quote")
				}
				i = j + 1 + k + len(delim)
				out = append(out, token{sql[start:i], 'q', start, i})
				continue
			}
			j = i + 1
			for j < len(sql) && sql[j] >= '0' && sql[j] <= '9' {
				j++
			}
			if j > i+1 {
				out = append(out, token{sql[i:j], 'p', i, j})
				i = j
				continue
			}
		}
		if c == '_' || unicode.IsLetter(rune(c)) || c >= 128 {
			i++
			for i < len(sql) && (sql[i] == '_' || sql[i] == '$' || unicode.IsLetter(rune(sql[i])) || unicode.IsDigit(rune(sql[i])) || sql[i] >= 128) {
				i++
			}
			out = append(out, token{sql[start:i], 'w', start, i})
			continue
		}
		if c >= '0' && c <= '9' {
			i++
			for i < len(sql) && (sql[i] == '.' || sql[i] == '_' || unicode.IsDigit(rune(sql[i]))) {
				i++
			}
			out = append(out, token{sql[start:i], 'n', start, i})
			continue
		}
		i++
		out = append(out, token{sql[start:i], 's', start, i})
	}
	return out, nil
}
func eq(t token, s string) bool { return t.kind == 'w' && strings.EqualFold(t.s, s) }
func simpleIdent(t token) bool  { return t.kind == 'w' || t.kind == '"' }
func parseInsert(sql string) (insertShape, bool) {
	ts, err := tokenize(sql)
	if err != nil || len(ts) < 8 {
		return insertShape{}, false
	}
	if ts[len(ts)-1].s == ";" {
		ts = ts[:len(ts)-1]
	}
	for _, t := range ts {
		if t.s == ";" {
			return insertShape{}, false
		}
	}
	if !eq(ts[0], "INSERT") || !eq(ts[1], "INTO") {
		return insertShape{}, false
	}
	i := 2
	if !simpleIdent(ts[i]) {
		return insertShape{}, false
	}
	i++
	if i+1 < len(ts) && ts[i].s == "." && simpleIdent(ts[i+1]) {
		i += 2
	}
	if i >= len(ts) || ts[i].s != "(" {
		return insertShape{}, false
	}
	i++
	cols := 0
	expect := true
	for i < len(ts) && ts[i].s != ")" {
		if expect {
			if !simpleIdent(ts[i]) {
				return insertShape{}, false
			}
			cols++
			expect = false
		} else if ts[i].s == "," {
			expect = true
		} else {
			return insertShape{}, false
		}
		i++
	}
	if cols == 0 || expect || i >= len(ts) {
		return insertShape{}, false
	}
	i++
	if i >= len(ts) || !eq(ts[i], "VALUES") {
		return insertShape{}, false
	}
	i++
	if i >= len(ts) || ts[i].s != "(" {
		return insertShape{}, false
	}
	i++
	vstart := i
	placeholders := 0
	expect = true
	for i < len(ts) && ts[i].s != ")" {
		if expect {
			if ts[i].kind != 'p' {
				return insertShape{}, false
			}
			placeholders++
			expect = false
		} else if ts[i].s == "," {
			expect = true
		} else {
			return insertShape{}, false
		}
		i++
	}
	if placeholders != cols || expect || i >= len(ts) {
		return insertShape{}, false
	}
	vend := i
	i++
	if i < len(ts) && !(eq(ts[i], "ON") && i+3 < len(ts) && eq(ts[i+1], "CONFLICT")) {
		return insertShape{}, false
	}
	for j := i; j < len(ts); j++ {
		if ts[j].s == ";" || eq(ts[j], "VALUES") {
			return insertShape{}, false
		}
	}
	for _, t := range ts[vend+1:] {
		if t.kind == 'p' {
			return insertShape{}, false
		}
	}
	return insertShape{prefix: sql[:ts[vstart-2].end], suffix: sql[ts[vend].end:], valuesStart: vstart, valuesEnd: vend, placeholderCount: placeholders}, true
}

// RETURNING and ON CONFLICT are excluded because one aggregate result cannot be split per caller.
func batchableShape(sql string, args []any) (insertShape, bool) {
	s, ok := supportedShape(sql, args)
	if !ok {
		return insertShape{}, false
	}
	tail, err := tokenize(s.suffix)
	if err != nil {
		return insertShape{}, false
	}
	for i, t := range tail {
		if eq(t, "RETURNING") || (eq(t, "ON") && i+1 < len(tail) && eq(tail[i+1], "CONFLICT")) {
			return insertShape{}, false
		}
	}
	return s, true
}

func supportedShape(sql string, args []any) (insertShape, bool) {
	s, ok := parseInsert(sql)
	if !ok || s.placeholderCount != len(args) {
		return insertShape{}, false
	}
	ts, err := tokenize(sql)
	if err != nil {
		return insertShape{}, false
	}
	n := 0
	for _, t := range ts {
		if t.kind == 'p' {
			n++
			if t.s != fmt.Sprintf("$%d", n) {
				return insertShape{}, false
			}
		}
	}
	if n != len(args) {
		return insertShape{}, false
	}
	return s, true
}
