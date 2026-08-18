package frostlake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"math"
	"strings"
	"testing"
	"time"
)

// Statements and their parameters: finding placeholders, rendering values into them, the
// literal encoding that goes on the wire, and the prepared-statement path.

func TestCountPlaceholders(t *testing.T) {
	cases := map[string]int{
		"SELECT 1":                       0,
		"SELECT ?":                       1,
		"SELECT ?, ?, ?":                 3,
		"SELECT 'a?b'":                   0,
		"SELECT 'a''?b'":                 0,
		`SELECT "a?b"`:                   0,
		"SELECT 1 -- a ? here\n, ?":      1,
		"SELECT 1 // a ? here\n, ?":      1,
		"SELECT /* a ? here */ ?":        1,
		"SELECT $$a?b$$":                 0,
		"SELECT $$a?b$$, ?":              1,
		"SELECT $$ ? $$ || $$ ? $$ || ?": 1,
		`SELECT 'esc\'? still literal'`:  0,
		"SELECT $$unterminated ?":        0,
	}
	for q, want := range cases {
		if got := countPlaceholders(q); got != want {
			t.Errorf("countPlaceholders(%q) = %d, want %d", q, got, want)
		}
	}
}

func TestScanPlaceholdersNamed(t *testing.T) {
	cases := map[string][]string{
		"SELECT :a, :b":        {"A", "B"},
		"SELECT :a, :a":        {"A"},
		"SELECT '1'::INT":      nil,
		"SELECT x := 1":        nil,
		"SELECT ':a'":          nil,
		`SELECT ":a"`:          nil,
		"SELECT $$:a$$":        nil,
		"SELECT 1 -- :a\n":     nil,
		"SELECT /* :a */ 1":    nil,
		"SELECT :1":            nil,
		"SELECT :name_1$x":     {"NAME_1$X"},
		"SELECT a::NUMBER, :v": {"V"},
	}
	for query, want := range cases {
		got := namedPlaceholders(query)
		if len(got) != len(want) {
			t.Errorf("namedPlaceholders(%q) = %v, want %v", query, got, want)
			continue
		}
		for i := range want {
			if got[i] != want[i] {
				t.Errorf("namedPlaceholders(%q) = %v, want %v", query, got, want)
				break
			}
		}
	}
}

func TestInputCount(t *testing.T) {
	cases := map[string]int{
		"SELECT 1":      0,
		"SELECT ?, ?":   2,
		"SELECT :a, :b": 2,
		"SELECT :a, :a": 1,
		"SELECT :a, ?":  -1,
		"SELECT $$?$$":  0,
	}
	for query, want := range cases {
		if got := inputCount(query); got != want {
			t.Errorf("inputCount(%q) = %d, want %d", query, got, want)
		}
	}
}

// $ is legal inside an unquoted identifier, so A$$B is a name, not a body delimiter.
func TestDollarInIdentifierIsNotAQuote(t *testing.T) {
	q := "SELECT A$$B FROM t WHERE x = ?"
	if n := countPlaceholders(q); n != 1 {
		t.Errorf("countPlaceholders(%q) = %d, want 1", q, n)
	}
	got, err := substitute(q, named([]driver.Value{int64(7)}), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if got != "SELECT A$$B FROM t WHERE x = 7" {
		t.Errorf("substitute = %q", got)
	}
	if !opensDollarQuote("AS $$body$$", 3) {
		t.Error("a delimiter after whitespace should open a body")
	}
	if opensDollarQuote("A$$B", 1) {
		t.Error("$$ inside an identifier should not open a body")
	}
}

func TestSubstitute(t *testing.T) {
	cases := []struct {
		query string
		args  []driver.Value
		want  string
	}{
		{"SELECT ?", []driver.Value{int64(7)}, "SELECT 7"},
		{"SELECT ?, ?", []driver.Value{"a", nil}, "SELECT 'a', NULL"},
		{"SELECT 'a?b' || ?", []driver.Value{"!"}, "SELECT 'a?b' || '!'"},
		{`SELECT "c?l", ?`, []driver.Value{true}, `SELECT "c?l", true`},
		{"SELECT ? -- ?\n", []driver.Value{int64(1)}, "SELECT 1 -- ?\n"},
		{"SELECT /* ? */ ?", []driver.Value{int64(1)}, "SELECT /* ? */ 1"},
		// A $$ body is not SQL: a ? inside it belongs to the function being defined.
		{"SELECT $$a?b$$, ?", []driver.Value{"x"}, "SELECT $$a?b$$, 'x'"},
		{"SELECT $$?$$ || $$?$$ || ?", []driver.Value{"z"}, "SELECT $$?$$ || $$?$$ || 'z'"},
	}
	for _, c := range cases {
		got, err := substitute(c.query, named(c.args), time.UTC)
		if err != nil {
			t.Errorf("substitute(%q): %v", c.query, err)
			continue
		}
		if got != c.want {
			t.Errorf("substitute(%q) = %q, want %q", c.query, got, c.want)
		}
	}
}

// A placeholder without an argument used to become NULL, and a surplus argument used to
// vanish — both without an error.
func TestSubstituteArgumentCountIsChecked(t *testing.T) {
	if _, err := substitute("SELECT ?, ?", named([]driver.Value{int64(1)}), time.UTC); err == nil {
		t.Error("two placeholders with one argument should be an error")
	}
	if _, err := substitute("SELECT ?", named([]driver.Value{int64(1), int64(2)}), time.UTC); err == nil {
		t.Error("one placeholder with two arguments should be an error")
	}
	if _, err := substitute("SELECT ?", nil, time.UTC); err == nil {
		t.Error("a placeholder with no arguments should be an error")
	}
	if _, err := substitute("SELECT 1", named([]driver.Value{int64(1)}), time.UTC); err == nil {
		t.Error("an argument with no placeholder should be an error")
	}
	got, err := substitute("SELECT 1", nil, time.UTC)
	if err != nil || got != "SELECT 1" {
		t.Errorf("no placeholders and no arguments = %q, %v", got, err)
	}
}

// A UDF body is exactly the case that used to be rewritten.
func TestSubstituteLeavesFunctionBodiesAlone(t *testing.T) {
	body := "CREATE FUNCTION f(a STRING) RETURNS STRING LANGUAGE PYTHON AS $$\n" +
		"def f(a):\n    return a + '?' if a else '?'\n$$"
	if n := countPlaceholders(body); n != 0 {
		t.Fatalf("countPlaceholders(function body) = %d, want 0", n)
	}
	got, err := substitute(body, nil, time.UTC)
	if err != nil {
		t.Fatalf("substitute(function body): %v", err)
	}
	if got != body {
		t.Errorf("function body was rewritten:\n%s", got)
	}
}

func TestSubstituteNamed(t *testing.T) {
	args := []driver.NamedValue{
		{Name: "who", Ordinal: 1, Value: "ada"},
		{Name: "n", Ordinal: 2, Value: int64(3)},
	}
	got, err := substitute("SELECT :who, :n, :who", args, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if want := "SELECT 'ada', 3, 'ada'"; got != want {
		t.Errorf("substitute = %q, want %q", got, want)
	}
}

func TestSubstituteNamedErrors(t *testing.T) {
	cases := []struct {
		name  string
		query string
		args  []driver.NamedValue
		want  string
	}{
		{"missing argument", "SELECT :a, :b",
			[]driver.NamedValue{{Name: "a", Ordinal: 1, Value: int64(1)}}, "no argument bound for :b"},
		{"unused argument", "SELECT :a",
			[]driver.NamedValue{{Name: "a", Ordinal: 1, Value: int64(1)}, {Name: "z", Ordinal: 2, Value: int64(2)}},
			"does not appear in the statement"},
		{"unnamed argument", "SELECT :a",
			[]driver.NamedValue{{Ordinal: 1, Value: int64(1)}}, "every argument needs a name"},
		{"mixed styles", "SELECT :a, ?",
			[]driver.NamedValue{{Name: "a", Ordinal: 1, Value: int64(1)}}, "not both"},
		{"named argument for positional statement", "SELECT ?",
			[]driver.NamedValue{{Name: "a", Ordinal: 1, Value: int64(1)}}, "uses positional"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := substitute(c.query, c.args, time.UTC)
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("err = %v, want it to mention %q", err, c.want)
			}
		})
	}
}

func TestEncodeString(t *testing.T) {
	cases := map[string]string{
		"plain":     "'plain'",
		"O'Hara":    "'O''Hara'",
		`back\dir`:  `'back\\dir'`,
		`trail\`:    `'trail\\'`,
		`both'\end`: `'both''\\end'`,
	}
	for in, want := range cases {
		if got := encodeString(in); got != want {
			t.Errorf("encodeString(%q) = %s, want %s", in, got, want)
		}
	}
}

func TestFormatLiteral(t *testing.T) {
	cases := []struct {
		value driver.Value
		want  string
	}{
		{nil, "NULL"},
		{true, "true"},
		{int64(-5), "-5"},
		{1.5, "1.5"},
		{"it's", "'it''s'"},
		{[]byte{0xde, 0xad}, "X'dead'"},
		{math.NaN(), "'NaN'::FLOAT"},
		{math.Inf(1), "'Inf'::FLOAT"},
		{math.Inf(-1), "'-Inf'::FLOAT"},
	}
	for _, c := range cases {
		got, err := formatLiteral(c.value, time.UTC)
		if err != nil {
			t.Errorf("formatLiteral(%v): %v", c.value, err)
			continue
		}
		if got != c.want {
			t.Errorf("formatLiteral(%v) = %s, want %s", c.value, got, c.want)
		}
	}
	if _, err := formatLiteral(struct{}{}, time.UTC); err == nil {
		t.Error("an unsupported parameter type should be an error")
	}
}

// The same instant must bind to the same literal whatever zone the caller's Time carries.
func TestFormatLiteralNormalisesTimeZone(t *testing.T) {
	instant := time.Date(2024, 3, 5, 12, 0, 0, 0, time.UTC)
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("no zoneinfo available: %v", err)
	}
	utc, err := formatLiteral(instant, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	shifted, err := formatLiteral(instant.In(ny), time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if utc != shifted {
		t.Errorf("same instant bound differently: %s vs %s", utc, shifted)
	}
	if want := "'2024-03-05T12:00:00'::TIMESTAMP_NTZ"; utc != want {
		t.Errorf("formatLiteral(instant, time.UTC) = %s, want %s", utc, want)
	}
}

// Binding renders in the DSN's zone, so a value written and read back keeps its wall clock.
func TestFormatLiteralInLocation(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("no zoneinfo: %v", err)
	}
	instant := time.Date(2024, 3, 5, 12, 0, 0, 0, time.UTC)
	got, err := formatLiteral(instant, ny)
	if err != nil {
		t.Fatal(err)
	}
	if want := "'2024-03-05T07:00:00'::TIMESTAMP_NTZ"; got != want {
		t.Errorf("formatLiteral in New York = %s, want %s", got, want)
	}
}

// A placeholder with no argument used to bind NULL and report success.
func TestArgumentCountMismatchIsRejected(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE reg_args_db",
		"USE DATABASE reg_args_db",
		"CREATE TABLE pairs (a INTEGER, b INTEGER)",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO pairs VALUES (?, ?)", 1); err == nil {
		t.Error("an INSERT missing an argument should fail rather than store NULL")
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO pairs VALUES (?, ?)", 1, 2, 3); err == nil {
		t.Error("an INSERT with a surplus argument should fail")
	}
	var rows int64
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM pairs").Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 0 {
		t.Errorf("%d row(s) were stored by statements that should not have run", rows)
	}
}

// A $$ body is not SQL. countPlaceholders used to see the ? inside one, so database/sql
// demanded an argument for a statement that has no placeholders.
func TestDollarQuotedBodySurvivesPreparation(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE reg_body_db",
		"USE DATABASE reg_body_db",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	const ddl = "CREATE OR REPLACE FUNCTION marks() RETURNS STRING AS $$ 'a?b' $$"
	st, err := conn.PrepareContext(ctx, ddl)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if _, err := st.ExecContext(ctx); err != nil {
		t.Fatalf("a statement whose only ? sits in a $$ body should need no arguments: %v", err)
	}
	var got string
	if err := conn.QueryRowContext(ctx, "SELECT marks()").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "a?b" {
		t.Errorf("function body was rewritten: got %q, want %q", got, "a?b")
	}
}

// :name parameters bind by name.
func TestNamedParameters(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE gap_named_db",
		"USE DATABASE gap_named_db",
		"CREATE TABLE people (id INTEGER, name VARCHAR)",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO people VALUES (:id, :name)",
		sql.Named("name", "Ada"), sql.Named("id", 1)); err != nil {
		t.Fatalf("named INSERT (arguments deliberately out of order): %v", err)
	}
	var name string
	if err := conn.QueryRowContext(ctx, "SELECT name FROM people WHERE id = :id",
		sql.Named("id", 1)).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Ada" {
		t.Errorf("name = %q, want Ada", name)
	}
	// A cast is not a parameter.
	var n int64
	if err := conn.QueryRowContext(ctx, "SELECT '7'::INT + :n", sql.Named("n", 1)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 8 {
		t.Errorf("cast beside a named parameter = %d, want 8", n)
	}
}

func TestPlaceholderInsideLiteralIsNotSubstituted(t *testing.T) {
	db := open(t)
	defer db.Close()
	var s string
	if err := db.QueryRow("SELECT 'a?b' || ?", "!").Scan(&s); err != nil {
		t.Fatal(err)
	}
	if s != "a?b!" {
		t.Fatalf("s = %q, want a?b!", s)
	}
}

// FormatFloat spells these NaN, +Inf and -Inf, which the parser read as identifiers.
func TestFloatSpecialsBind(t *testing.T) {
	db := open(t)
	defer db.Close()
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		var out string
		if err := db.QueryRow("SELECT TO_VARCHAR(?)", v).Scan(&out); err != nil {
			t.Errorf("binding %v: %v", v, err)
			continue
		}
		if out == "" {
			t.Errorf("binding %v produced an empty result", v)
		}
	}
}

// The prepared-statement path had no test at all.
func TestPreparedStatement(t *testing.T) {
	db := open(t)
	defer db.Close()
	st, err := db.Prepare("SELECT ? + ?")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	var sum int64
	if err := st.QueryRow(2, 3).Scan(&sum); err != nil {
		t.Fatal(err)
	}
	if sum != 5 {
		t.Errorf("prepared SELECT ? + ? = %d, want 5", sum)
	}
	if _, err := st.Query(1); err == nil {
		t.Error("a prepared statement given too few arguments should fail")
	}
}

// A prepared statement has to honour the caller's deadline too. Without StmtQueryContext
// database/sql falls back to the context-free path, and a deadline never reaches the request.
func TestPreparedStatementHonoursContext(t *testing.T) {
	db := open(t)
	defer db.Close()
	st, err := db.Prepare("SELECT SYSTEM$WAIT(3)")
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	var v any
	start := time.Now()
	err = st.QueryRowContext(ctx).Scan(&v)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a 3s query under a 300ms deadline should not have succeeded")
	}
	if elapsed > 2*time.Second {
		t.Errorf("the deadline took %v to bite, so it did not reach the request", elapsed)
	}
}
