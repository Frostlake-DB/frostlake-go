package frostlake

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

// The DSN and what it configures: parsing and its refusals, the USE statements a session is
// opened with, identifier quoting, and the connector that parses a DSN once.

func TestParseDSN(t *testing.T) {
	cases := []struct {
		dsn      string
		baseURL  string
		database string
		schema   string
	}{
		{"frostlake://localhost:18082", "http://localhost:18082", "", ""},
		{"frostlake://localhost:18082/MY_DB", "http://localhost:18082", "MY_DB", ""},
		{"frostlake://h:1/MY_DB?schema=PUBLIC", "http://h:1", "MY_DB", "PUBLIC"},
		{"http://h:1/MY_DB", "http://h:1", "MY_DB", ""},
		{"frostlake://h:1/", "http://h:1", "", ""},
	}
	for _, c := range cases {
		cfg, err := parseDSN(c.dsn)
		if err != nil {
			t.Fatalf("parseDSN(%q): %v", c.dsn, err)
		}
		if cfg.baseURL != c.baseURL || cfg.database != c.database || cfg.schema != c.schema {
			t.Errorf("parseDSN(%q) = %+v, want %s %s %s", c.dsn, cfg, c.baseURL, c.database, c.schema)
		}
	}
	for _, bad := range []string{"mysql://h:1", "frostlake://", "://"} {
		if _, err := parseDSN(bad); err == nil {
			t.Errorf("parseDSN(%q) should have failed", bad)
		}
	}
}

func TestParseDSNOptions(t *testing.T) {
	cfg, err := parseDSN("frostlake://h:1/DB?schema=S&role=R&warehouse=W&timeout=90s&loc=America/New_York")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.database != "DB" || cfg.schema != "S" || cfg.role != "R" || cfg.warehouse != "W" {
		t.Errorf("scope parsed as %+v", cfg)
	}
	if cfg.timeout != 90*time.Second {
		t.Errorf("timeout = %v, want 90s", cfg.timeout)
	}
	if cfg.loc == nil || cfg.loc.String() != "America/New_York" {
		t.Errorf("loc = %v", cfg.loc)
	}
	if cfg.baseURL != "http://h:1" {
		t.Errorf("baseURL = %s", cfg.baseURL)
	}
}

func TestParseDSNDefaults(t *testing.T) {
	cfg, err := parseDSN("frostlake://h:1")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.timeout != defaultTimeout {
		t.Errorf("timeout = %v, want %v", cfg.timeout, defaultTimeout)
	}
	if cfg.loc != time.UTC {
		t.Errorf("loc = %v, want UTC", cfg.loc)
	}
}

func TestParseDSNTLS(t *testing.T) {
	for dsn, want := range map[string]string{
		"https://h:1/DB":              "https://h:1",
		"frostlake://h:1/DB?tls=true": "https://h:1",
		"frostlake://h:1/DB?tls=0":    "http://h:1",
		"http://h:1/DB":               "http://h:1",
	} {
		cfg, err := parseDSN(dsn)
		if err != nil {
			t.Errorf("parseDSN(%q): %v", dsn, err)
			continue
		}
		if cfg.baseURL != want {
			t.Errorf("parseDSN(%q).baseURL = %s, want %s", dsn, cfg.baseURL, want)
		}
	}
}

// Silently dropping a credential, or a mistyped option, is worse than refusing it.
func TestParseDSNRejections(t *testing.T) {
	cases := map[string]string{
		"frostlake://user:pass@h:1/DB":     "username or password",
		"frostlake://h:1/DB?schemaa=S":     "unknown DSN parameter",
		"frostlake://h:1/DB?timeout=soon":  "not a duration",
		"frostlake://h:1/DB?timeout=-5s":   "negative",
		"frostlake://h:1/DB?loc=Mars/Base": "not a known zone",
		"frostlake://h:1/DB?tls=maybe":     "not a boolean",
	}
	for dsn, want := range cases {
		_, err := parseDSN(dsn)
		if err == nil {
			t.Errorf("parseDSN(%q) should have failed", dsn)
			continue
		}
		if !strings.Contains(err.Error(), want) {
			t.Errorf("parseDSN(%q) = %v, want it to mention %q", dsn, err, want)
		}
	}
}

func TestUseStatements(t *testing.T) {
	cfg := &config{database: "MY_DB", schema: "my schema"}
	got := cfg.useStatements()
	want := []string{"USE DATABASE MY_DB", `USE SCHEMA "my schema"`}
	if len(got) != len(want) {
		t.Fatalf("useStatements() = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("useStatements()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
	if len((&config{}).useStatements()) != 0 {
		t.Error("a DSN with no database or schema should need no USE statements")
	}
}

func TestUseStatementsOrder(t *testing.T) {
	cfg := &config{role: "R", warehouse: "W", database: "D", schema: "S"}
	want := []string{"USE ROLE R", "USE WAREHOUSE W", "USE DATABASE D", "USE SCHEMA S"}
	got := cfg.useStatements()
	if len(got) != len(want) {
		t.Fatalf("useStatements() = %v", got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("useStatements()[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

// An identifier from a DSN must not be able to break out of its quoting.
func TestQuoteIdent(t *testing.T) {
	cases := map[string]string{
		"MY_DB": "MY_DB",
		"A$B":   "A$B",
		"X1":    "X1",
		"my_db": `"my_db"`,
		"My_Db": `"My_Db"`,
		"a b":   `"a b"`,
		"9DB":   `"9DB"`,
		"$DB":   `"$DB"`,
		`x"y`:   `"x""y"`,
		`"; --`: `"""; --"`,
		"":      `""`,
		"naïve": `"naïve"`,
	}
	for in, want := range cases {
		if got := quoteIdent(in); got != want {
			t.Errorf("quoteIdent(%q) = %s, want %s", in, got, want)
		}
	}
}

// A connector parses the DSN once and hands sql.OpenDB connections made from it.
func TestConnector(t *testing.T) {
	requireServer(t)
	c, err := NewConnector(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	db := sql.OpenDB(c)
	defer db.Close()
	var v int64
	if err := db.QueryRow("SELECT 1").Scan(&v); err != nil {
		t.Fatal(err)
	}
	if v != 1 {
		t.Errorf("SELECT 1 = %d", v)
	}
	if _, err := NewConnector("mysql://nope"); err == nil {
		t.Error("NewConnector should reject a foreign DSN")
	}
}

// Role and warehouse from the DSN are applied to the session.
func TestDSNRoleAndWarehouse(t *testing.T) {
	requireServer(t)
	db, err := sql.Open("frostlake", testDSN+"?role=ACCOUNTADMIN&warehouse=COMPUTE_WH")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var role, warehouse string
	if err := db.QueryRow("SELECT CURRENT_ROLE(), CURRENT_WAREHOUSE()").Scan(&role, &warehouse); err != nil {
		t.Fatal(err)
	}
	if role != "ACCOUNTADMIN" {
		t.Errorf("CURRENT_ROLE() = %q, want ACCOUNTADMIN", role)
	}
	if warehouse != "COMPUTE_WH" {
		t.Errorf("CURRENT_WAREHOUSE() = %q, want COMPUTE_WH", warehouse)
	}
}

// The DSN's timeout bounds a request.
func TestDSNTimeout(t *testing.T) {
	requireServer(t)
	db, err := sql.Open("frostlake", testDSN+"?timeout=200ms")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v any
	start := time.Now()
	err = db.QueryRow("SELECT SYSTEM$WAIT(3)").Scan(&v)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("a 3s query under a 200ms timeout should fail")
	}
	if elapsed > 2*time.Second {
		t.Errorf("the timeout took %v to bite", elapsed)
	}
}
