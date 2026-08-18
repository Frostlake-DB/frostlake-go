package frostlake

import (
	"context"
	"database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

type stmt struct {
	conn     *conn
	query    string
	numInput int
}

func newStmt(c *conn, query string) *stmt {
	return &stmt{conn: c, query: query, numInput: inputCount(query)}
}

// inputCount is what database/sql checks argument counts against. Named placeholders count
// once each however often they appear. A statement mixing the two styles reports -1 (unknown)
// so the check is skipped and execution can explain the real problem.
func inputCount(query string) int {
	positional := 0
	names := make(map[string]bool)
	for _, p := range scanPlaceholders(query) {
		if p.name == "" {
			positional++
		} else {
			names[p.name] = true
		}
	}
	if positional > 0 && len(names) > 0 {
		return -1
	}
	if len(names) > 0 {
		return len(names)
	}
	return positional
}

func (s *stmt) Close() error { return nil }

func (s *stmt) NumInput() int { return s.numInput }

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.conn.ExecContext(context.Background(), s.query, named(args))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.conn.QueryContext(context.Background(), s.query, named(args))
}

// ExecContext and QueryContext implement driver.StmtExecContext and driver.StmtQueryContext.
// Without them database/sql falls back to the pair above, and a caller's deadline never
// reaches the request.
func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.conn.ExecContext(ctx, s.query, args)
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	return s.conn.QueryContext(ctx, s.query, args)
}

func named(args []driver.Value) []driver.NamedValue {
	out := make([]driver.NamedValue, len(args))
	for i, a := range args {
		out[i] = driver.NamedValue{Ordinal: i + 1, Value: a}
	}
	return out
}

// placeholder is one bind site in a statement: a positional ? (name empty) or a :name.
type placeholder struct {
	start, end int
	name       string
}

// scanPlaceholders finds every bind site in a statement, skipping string literals, quoted
// identifiers, dollar-quoted bodies and comments. Counting and substitution both read this
// one scan, so they cannot disagree about what is a placeholder.
func scanPlaceholders(query string) []placeholder {
	var out []placeholder
	for i := 0; i < len(query); i++ {
		switch query[i] {
		case '\'':
			i = skipString(query, i) - 1
		case '"':
			i = skipQuoted(query, i, '"') - 1
		case '$':
			if opensDollarQuote(query, i) {
				i = skipDollarQuoted(query, i) - 1
			}
		case '-':
			if i+1 < len(query) && query[i+1] == '-' {
				i = skipLine(query, i) - 1
			}
		case '/':
			if i+1 < len(query) && query[i+1] == '/' {
				i = skipLine(query, i) - 1
			} else if i+1 < len(query) && query[i+1] == '*' {
				j := strings.Index(query[i+2:], "*/")
				if j < 0 {
					return out
				}
				i = i + 2 + j + 2 - 1
			}
		case '?':
			out = append(out, placeholder{start: i, end: i + 1})
		case ':':
			// :: is a cast and := an assignment; neither introduces a parameter, and a
			// leading digit means a positional reference rather than a name.
			if i+1 < len(query) && (query[i+1] == ':' || query[i+1] == '=') {
				i++
				continue
			}
			j := i + 1
			for j < len(query) && isWordByte(query[j]) {
				j++
			}
			if j > i+1 && !(query[i+1] >= '0' && query[i+1] <= '9') {
				out = append(out, placeholder{start: i, end: j, name: strings.ToUpper(query[i+1 : j])})
				i = j - 1
			}
		}
	}
	return out
}

// substitute inlines the placeholders with formatted literals — the same client-side binding
// Frostlake's JDBC driver performs, since the protocol has no server-side binding.
func substitute(query string, args []driver.NamedValue, loc *time.Location) (string, error) {
	sites := scanPlaceholders(query)
	positional, named := 0, 0
	for _, p := range sites {
		if p.name == "" {
			positional++
		} else {
			named++
		}
	}
	if positional > 0 && named > 0 {
		return "", errors.New("frostlake: a statement may use ? or :name placeholders, not both")
	}
	if named > 0 {
		return renderNamed(query, sites, args, loc)
	}
	return renderPositional(query, sites, args, loc)
}

func renderPositional(query string, sites []placeholder, args []driver.NamedValue, loc *time.Location) (string, error) {
	if len(sites) != len(args) {
		return "", fmt.Errorf("frostlake: statement has %d placeholder(s), got %d argument(s)",
			len(sites), len(args))
	}
	if len(sites) == 0 {
		return query, nil
	}
	values := make(map[int]driver.Value, len(args))
	for _, a := range args {
		if a.Name != "" {
			return "", fmt.Errorf("frostlake: argument %q is named, but the statement uses positional ? placeholders", a.Name)
		}
		values[a.Ordinal] = a.Value
	}
	var out strings.Builder
	out.Grow(len(query) + 16*len(sites))
	cursor := 0
	for i, p := range sites {
		out.WriteString(query[cursor:p.start])
		v, ok := values[i+1]
		if !ok {
			return "", fmt.Errorf("frostlake: no argument bound for placeholder %d", i+1)
		}
		lit, err := formatLiteral(v, loc)
		if err != nil {
			return "", err
		}
		out.WriteString(lit)
		cursor = p.end
	}
	out.WriteString(query[cursor:])
	return out.String(), nil
}

func renderNamed(query string, sites []placeholder, args []driver.NamedValue, loc *time.Location) (string, error) {
	values := make(map[string]driver.Value, len(args))
	for _, a := range args {
		if a.Name == "" {
			return "", errors.New("frostlake: the statement uses :name placeholders, so every argument needs a name — pass sql.Named values")
		}
		values[strings.ToUpper(a.Name)] = a.Value
	}
	used := make(map[string]bool, len(values))
	var out strings.Builder
	out.Grow(len(query) + 16*len(sites))
	cursor := 0
	for _, p := range sites {
		out.WriteString(query[cursor:p.start])
		v, ok := values[p.name]
		if !ok {
			return "", fmt.Errorf("frostlake: no argument bound for :%s", strings.ToLower(p.name))
		}
		used[p.name] = true
		lit, err := formatLiteral(v, loc)
		if err != nil {
			return "", err
		}
		out.WriteString(lit)
		cursor = p.end
	}
	out.WriteString(query[cursor:])
	for name := range values {
		if !used[name] {
			return "", fmt.Errorf("frostlake: argument :%s does not appear in the statement", strings.ToLower(name))
		}
	}
	return out.String(), nil
}

// skipString returns the index just past a single-quoted literal starting at i.
// Both ” and backslash escapes end up inside the literal (backslash always
// escapes in Frostlake's string dialect).
func skipString(s string, i int) int {
	j := i + 1
	for j < len(s) {
		switch s[j] {
		case '\\':
			j += 2
		case '\'':
			if j+1 < len(s) && s[j+1] == '\'' {
				j += 2
			} else {
				return j + 1
			}
		default:
			j++
		}
	}
	return j
}

func skipQuoted(s string, i int, q byte) int {
	j := i + 1
	for j < len(s) {
		if s[j] == q {
			if j+1 < len(s) && s[j+1] == q {
				j += 2
				continue
			}
			return j + 1
		}
		j++
	}
	return j
}

// opensDollarQuote reports whether the $$ at i starts a dollar-quoted body. A $ is legal
// inside an unquoted identifier, so A$$B is a name rather than the start of a body — a real
// delimiter is never preceded by an identifier character.
func opensDollarQuote(s string, i int) bool {
	if i+1 >= len(s) || s[i+1] != '$' {
		return false
	}
	return i == 0 || !isWordByte(s[i-1])
}

// skipDollarQuoted returns the index just past a $$…$$ body starting at i. Function and
// procedure bodies are written this way, and their contents are not SQL — a ? inside one is
// part of the body, never a placeholder.
func skipDollarQuoted(s string, i int) int {
	if j := strings.Index(s[i+2:], "$$"); j >= 0 {
		return i + 2 + j + 2
	}
	return len(s)
}

func skipLine(s string, i int) int {
	if j := strings.IndexByte(s[i:], '\n'); j >= 0 {
		return i + j + 1
	}
	return len(s)
}

func countPlaceholders(query string) int {
	n := 0
	for _, p := range scanPlaceholders(query) {
		if p.name == "" {
			n++
		}
	}
	return n
}

// namedPlaceholders returns the parameter names a statement carries, upper-cased, in order of
// first appearance.
func namedPlaceholders(query string) []string {
	var out []string
	seen := make(map[string]bool)
	for _, p := range scanPlaceholders(query) {
		if p.name != "" && !seen[p.name] {
			seen[p.name] = true
			out = append(out, p.name)
		}
	}
	return out
}

// encodeString mirrors the engine's canonical literal encoder: backslashes doubled
// (backslash always escapes), quotes doubled.
func encodeString(text string) string {
	text = strings.ReplaceAll(text, `\`, `\\`)
	text = strings.ReplaceAll(text, `'`, `''`)
	return "'" + text + "'"
}

func formatLiteral(v driver.Value, loc *time.Location) (string, error) {
	switch x := v.(type) {
	case nil:
		return "NULL", nil
	case bool:
		return strconv.FormatBool(x), nil
	case int64:
		return strconv.FormatInt(x, 10), nil
	case float64:
		// FormatFloat spells these NaN, +Inf and -Inf, which the parser reads as identifiers.
		switch {
		case math.IsNaN(x):
			return "'NaN'::FLOAT", nil
		case math.IsInf(x, 1):
			return "'Inf'::FLOAT", nil
		case math.IsInf(x, -1):
			return "'-Inf'::FLOAT", nil
		}
		return strconv.FormatFloat(x, 'g', -1, 64), nil
	case string:
		return encodeString(x), nil
	case []byte:
		return "X'" + hex.EncodeToString(x) + "'", nil
	case time.Time:
		// TIMESTAMP_NTZ stores a wall clock, so the zone has to be pinned somewhere. Rendering
		// in loc keeps two Times naming the same instant from storing as different timestamps,
		// and matches the zone their values are read back in.
		if loc == nil {
			loc = time.UTC
		}
		return "'" + x.In(loc).Format("2006-01-02T15:04:05.999999999") + "'::TIMESTAMP_NTZ", nil
	default:
		return "", fmt.Errorf("frostlake: unsupported parameter type %T", v)
	}
}
