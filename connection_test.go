package frostlake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// The connection itself: the session scope it carries and how that survives pooling,
// transactions, and the errors it reports when a statement or the transport fails.

func TestLeadingWords(t *testing.T) {
	cases := []struct {
		statement string
		want      []string
	}{
		{"SELECT 1", []string{"SELECT", "1"}},
		{"  use database x", []string{"USE", "DATABASE", "X"}},
		{"\n\t-- a comment\nCREATE OR REPLACE DATABASE d",
			[]string{"CREATE", "OR", "REPLACE", "DATABASE", "D"}},
		{"/* c */ alter session set x", []string{"ALTER", "SESSION", "SET", "X"}},
		{"// line\ndrop table t", []string{"DROP", "TABLE", "T"}},
		{"", nil},
		{"(SELECT 1)", nil},
		{"/* unterminated", nil},
	}
	for _, c := range cases {
		got := leadingWords(c.statement, 6)
		if len(got) != len(c.want) {
			t.Errorf("leadingWords(%q) = %v, want %v", c.statement, got, c.want)
			continue
		}
		for i := range c.want {
			if got[i] != c.want[i] {
				t.Errorf("leadingWords(%q) = %v, want %v", c.statement, got, c.want)
				break
			}
		}
	}
}

// Semicolons inside literals, quoted identifiers, dollar-quoted bodies and comments do not
// end a statement.
func TestSplitStatements(t *testing.T) {
	cases := map[string]int{
		"SELECT 1":                     1,
		"SELECT 1; SELECT 2":           2,
		"SELECT 1; SELECT 2; SELECT 3": 3,
		"SELECT ';'":                   1,
		`SELECT "a;b"`:                 1,
		"SELECT $$a;b$$":               1,
		"SELECT 1 -- ;\n":              1,
		"SELECT /* ; */ 1":             1,
		"SELECT 1;":                    2,
	}
	for query, want := range cases {
		if got := len(splitStatements(query)); got != want {
			t.Errorf("splitStatements(%q) = %d statements, want %d", query, got, want)
		}
	}
}

// CREATE DATABASE and CREATE SCHEMA move the session's current scope, exactly as in
// Snowflake, so they count as scope changes alongside USE and the SET family.
func TestChangesSessionScope(t *testing.T) {
	dirty := []string{
		"USE DATABASE x", "use schema y", "SET v = 1", "UNSET v",
		"ALTER SESSION SET TIMEZONE = 'UTC'",
		"CREATE DATABASE d", "CREATE SCHEMA s", "DROP DATABASE d", "DROP SCHEMA s",
		"CREATE OR REPLACE DATABASE d", "CREATE TRANSIENT DATABASE d",
		"DROP DATABASE IF EXISTS d", "CREATE SCHEMA IF NOT EXISTS s",
		"  -- comment\n  USE DATABASE x",
		// A scope change behind a leading statement moves the session just the same.
		"SELECT 1; USE DATABASE x",
		"INSERT INTO t VALUES (1); ALTER SESSION SET TIMEZONE = 'UTC'",
	}
	for _, q := range dirty {
		if !changesSessionScope(q) {
			t.Errorf("changesSessionScope(%q) = false, want true", q)
		}
	}
	clean := []string{
		"SELECT 1", "INSERT INTO t VALUES (1)", "UPDATE t SET a = 1", "DELETE FROM t",
		"MERGE INTO t USING s ON x", "BEGIN", "COMMIT", "ROLLBACK", "",
		// Ordinary DDL leaves the scope where it was; treating it as a scope change
		// retired a connection for every statement.
		"CREATE TABLE t (a INT)", "CREATE OR REPLACE TABLE t (a INT)",
		"CREATE TEMPORARY TABLE t (a INT)", "DROP TABLE t", "CREATE VIEW v AS SELECT 1",
		"CREATE OR REPLACE FUNCTION f() RETURNS INT AS $$ 1 $$",
		"ALTER TABLE t ADD COLUMN b INT", "DROP FUNCTION f",
		"SELECT 1; SELECT 2", "SELECT ';'", "SELECT $$USE DATABASE x$$",
	}
	for _, q := range clean {
		if changesSessionScope(q) {
			t.Errorf("changesSessionScope(%q) = true, want false", q)
		}
	}
}

func TestSessionStatePersistsAcrossStatements(t *testing.T) {
	db := open(t)
	defer db.Close()
	// Single conn so USE state provably rides the session.
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE DATABASE go_sess_db"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "USE DATABASE go_sess_db"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "CREATE TABLE t1 (a INTEGER)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO t1 VALUES (7)"); err != nil {
		t.Fatal(err)
	}
	var a int64
	if err := conn.QueryRowContext(ctx, "SELECT a FROM t1").Scan(&a); err != nil {
		t.Fatal(err)
	}
	if a != 7 {
		t.Fatalf("a = %d, want 7", a)
	}
}

// A pooled connection carries its engine session, so a statement that changed the session's
// scope used to leak into whichever query picked that connection up next: half of a set of
// concurrent queries ran in one database and half in another.
func TestSessionScopeDoesNotLeakAcrossPooledConnections(t *testing.T) {
	setup := open(t)
	defer setup.Close()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE reg_home_db",
		"CREATE OR REPLACE DATABASE reg_other_db",
	} {
		if _, err := setup.Exec(s); err != nil {
			t.Fatal(err)
		}
	}

	for _, dsn := range []struct {
		name string
		dsn  string
	}{
		{"DSN names a database", testDSN + "/REG_HOME_DB"},
		{"DSN names no database", testDSN},
	} {
		t.Run(dsn.name, func(t *testing.T) {
			db, err := sql.Open("frostlake", dsn.dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(4)
			db.SetMaxIdleConns(4)

			// Open several connections so more than one can serve the queries below.
			var conns []*sql.Conn
			for i := 0; i < 4; i++ {
				c, err := db.Conn(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				var one int64
				if err := c.QueryRowContext(context.Background(), "SELECT 1").Scan(&one); err != nil {
					t.Fatal(err)
				}
				conns = append(conns, c)
			}
			for _, c := range conns {
				c.Close()
			}

			if _, err := db.Exec("USE DATABASE reg_other_db"); err != nil {
				t.Fatal(err)
			}

			var mu sync.Mutex
			seen := map[string]int{}
			var wg sync.WaitGroup
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					var name string
					if err := db.QueryRow("SELECT CURRENT_DATABASE()").Scan(&name); err != nil {
						mu.Lock()
						seen["error: "+err.Error()]++
						mu.Unlock()
						return
					}
					mu.Lock()
					seen[name]++
					mu.Unlock()
				}()
			}
			wg.Wait()
			if len(seen) != 1 {
				t.Errorf("concurrent queries disagreed about the current database: %v", seen)
			}
			if _, ok := seen["REG_OTHER_DB"]; ok {
				t.Errorf("a connection-scoped USE leaked to the pool: %v", seen)
			}
		})
	}
}

// A DSN naming a database that does not exist has to keep failing. The USE used to be dropped
// from the queue before it ran, so only the first statement failed and every later one
// silently ran in the server's default database.
func TestFailedDSNUseKeepsFailing(t *testing.T) {
	requireServer(t)
	db, err := sql.Open("frostlake", testDSN+"/REG_NO_SUCH_DB")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for attempt := 1; attempt <= 3; attempt++ {
		var name string
		err := conn.QueryRowContext(ctx, "SELECT CURRENT_DATABASE()").Scan(&name)
		if err == nil {
			t.Fatalf("statement %d ran anyway, in database %q", attempt, name)
		}
		if !strings.Contains(err.Error(), "REG_NO_SUCH_DB") {
			t.Fatalf("statement %d failed for the wrong reason: %v", attempt, err)
		}
	}
}

func TestTransactionRollback(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, stmt := range []string{
		"CREATE OR REPLACE DATABASE go_tx_db",
		"USE DATABASE go_tx_db",
		"CREATE TABLE acc (n INTEGER)",
		"INSERT INTO acc VALUES (1)",
	} {
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO acc VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM acc").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("count after rollback = %d, want 1", n)
	}
}

// The commit path was never exercised: only rollback had a test.
func TestTransactionCommit(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE reg_commit_db",
		"USE DATABASE reg_commit_db",
		"CREATE TABLE acc (n INTEGER)",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO acc VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	var n int64
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM acc").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("count after commit = %d, want 1", n)
	}
}

// An isolation level the engine cannot provide is refused, not silently ignored.
func TestUnsupportedTransactionOptions(t *testing.T) {
	db := open(t)
	defer db.Close()
	ctx := context.Background()
	_, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err == nil {
		t.Error("serializable isolation should be refused")
	} else if !strings.Contains(err.Error(), "isolation") {
		t.Errorf("err = %v, want it to name the isolation level", err)
	}
	_, err = db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err == nil {
		t.Error("a read-only transaction should be refused")
	}
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelReadCommitted})
	if err != nil {
		t.Fatalf("read committed is what the engine offers: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
}

// Commit rides the transaction's context; Rollback deliberately does not, so a
// cancelled transaction can still be ended.
func TestTransactionContext(t *testing.T) {
	requireServer(t)
	c, err := NewConnector(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	beginner, ok := dc.(driver.ConnBeginTx)
	if !ok {
		t.Fatal("the connection should implement ConnBeginTx")
	}

	ctx, cancel := context.WithCancel(context.Background())
	dtx, err := beginner.BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := dtx.Commit(); err == nil {
		t.Error("committing under a cancelled context should fail")
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	dtx2, err := beginner.BeginTx(ctx2, driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cancel2()
	if err := dtx2.Rollback(); err != nil {
		t.Errorf("rollback must still run after the transaction's context is cancelled: %v", err)
	}
}

func TestSnippet(t *testing.T) {
	if got := snippet([]byte("   ")); got != "(empty body)" {
		t.Errorf("snippet(blank) = %q", got)
	}
	long := strings.Repeat("x", maxErrorBody*2)
	got := snippet([]byte(long))
	if len(got) > maxErrorBody+len("…") {
		t.Errorf("snippet did not bound its output: %d bytes", len(got))
	}
	if !strings.HasSuffix(got, "…") {
		t.Error("a truncated snippet should say so")
	}
}

func TestFailureMessageIsNeverEmpty(t *testing.T) {
	cases := []struct {
		out     sqlResponse
		status  int
		payload string
		want    string
	}{
		{sqlResponse{ErrorMessage: "SQL compilation error"}, 200, "{}", "SQL compilation error"},
		{sqlResponse{Error: "SQL is required"}, 400, "{}", "frostlake: SQL is required"},
	}
	for _, c := range cases {
		if got := failureMessage(&c.out, c.status, []byte(c.payload)); got != c.want {
			t.Errorf("failureMessage() = %q, want %q", got, c.want)
		}
	}
	bare := failureMessage(&sqlResponse{}, 200, []byte(`{"hello":"world"}`))
	if bare == "" {
		t.Fatal("failureMessage returned an empty message")
	}
	if !strings.Contains(bare, "200") || !strings.Contains(bare, "hello") {
		t.Errorf("a message with nothing to report should carry the status and body: %q", bare)
	}
}

func TestErrorTypes(t *testing.T) {
	var err error = &Error{Message: "SQL compilation error", Statement: "SELECT x", Status: 200}
	var ferr *Error
	if !errors.As(err, &ferr) {
		t.Fatal("errors.As should find a *frostlake.Error")
	}
	if ferr.Statement != "SELECT x" || ferr.Status != 200 {
		t.Errorf("error carries %+v", ferr)
	}
	if err.Error() != "SQL compilation error" {
		t.Errorf("Error() = %q", err.Error())
	}

	cause := errors.New("dial tcp: refused")
	var terr error = &TransportError{Endpoint: "http://h/api/execute", Err: cause}
	var tp *TransportError
	if !errors.As(terr, &tp) {
		t.Fatal("errors.As should find a *frostlake.TransportError")
	}
	if !errors.Is(terr, cause) {
		t.Error("a TransportError should unwrap to its cause")
	}
	if !strings.Contains(terr.Error(), "refused") {
		t.Errorf("Error() = %q", terr.Error())
	}
}

// A statement failure arrives as *frostlake.Error carrying the statement and status.
func TestStructuredError(t *testing.T) {
	db := open(t)
	defer db.Close()
	_, err := db.Exec("SELECT * FROM no_such_table_at_all")
	if err == nil {
		t.Fatal("expected an error")
	}
	var ferr *Error
	if !errors.As(err, &ferr) {
		t.Fatalf("err = %#v, want a *frostlake.Error", err)
	}
	if !strings.Contains(ferr.Message, "does not exist") {
		t.Errorf("Message = %q", ferr.Message)
	}
	if !strings.Contains(ferr.Statement, "no_such_table_at_all") {
		t.Errorf("Statement = %q", ferr.Statement)
	}
	// The engine reports every statement failure through its exception path, so these arrive
	// as HTTP 500 rather than a 200 carrying success:false. Either way the answer is a proper
	// Frostlake response, which is why the status alone must never decide how it is read.
	if ferr.Status == 0 {
		t.Error("Status should carry the HTTP status the answer arrived with")
	}
	var terr *TransportError
	if errors.As(err, &terr) {
		t.Error("a statement failure is not a transport failure")
	}
}

// An unreachable server is a transport failure, not a statement failure.
func TestStructuredTransportError(t *testing.T) {
	db, err := sql.Open("frostlake", "frostlake://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("SELECT 1")
	if err == nil {
		t.Fatal("expected an error")
	}
	var terr *TransportError
	if !errors.As(err, &terr) {
		t.Fatalf("err = %#v, want a *frostlake.TransportError", err)
	}
	if terr.Endpoint == "" {
		t.Error("a transport error should name the endpoint")
	}
}

// An HTML error page used to surface as a JSON parser complaint about a '<'.
func TestTransportReportsHTTPFailures(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
		want   []string
	}{
		{"html error page", 500, "<html>Internal Server Error</html>", []string{"500", "html"}},
		{"empty body", 502, "", []string{"502", "empty body"}},
		{"not a frostlake server", 200, `{"hello":"world"}`, []string{"200", "hello"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			db, err := sql.Open("frostlake", stubDSN(t, c.status, c.body))
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			var v any
			err = db.QueryRow("SELECT 1").Scan(&v)
			if err == nil {
				t.Fatal("expected an error")
			}
			for _, want := range c.want {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q does not mention %q", err, want)
				}
			}
		})
	}
}

// The engine refuses an empty statement with a differently shaped body, which used to reach
// the caller as an error that printed as nothing at all.
func TestTransportReportsRequestRejection(t *testing.T) {
	db, err := sql.Open("frostlake", stubDSN(t, 400, `{"error":"SQL is required"}`))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("SELECT 1")
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "SQL is required") {
		t.Errorf("error = %q, want it to carry the server's reason", err)
	}
}

func TestTransportPassesStatementErrorThrough(t *testing.T) {
	body := `{"success":false,"sessionId":"s1","errorMessage":"SQL compilation error:\nnope","resultSets":null}`
	db, err := sql.Open("frostlake", stubDSN(t, 200, body))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec("SELECT 1")
	if err == nil || !strings.Contains(err.Error(), "SQL compilation error") {
		t.Errorf("err = %v, want the server's message", err)
	}
}

// A USE riding behind a leading statement moves the session just as surely as one standing
// alone. The scope check used to read only the first keyword, so the connection went back to
// the pool still pointing somewhere else.
func TestMultiStatementScopeChangeIsRestored(t *testing.T) {
	setup := open(t)
	defer setup.Close()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE ms_home_db",
		"CREATE OR REPLACE DATABASE ms_other_db",
	} {
		if _, err := setup.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	db, err := sql.Open("frostlake", testDSN+"/MS_HOME_DB")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	var name string
	if err := db.QueryRow("SELECT CURRENT_DATABASE()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "MS_HOME_DB" {
		t.Fatalf("start = %s, want MS_HOME_DB", name)
	}
	if _, err := db.Exec("SELECT 1; USE DATABASE ms_other_db"); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT CURRENT_DATABASE()").Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "MS_HOME_DB" {
		t.Errorf("after a multi-statement USE the connection is on %s, want MS_HOME_DB restored", name)
	}
}

// Only CREATE and DROP of a DATABASE or SCHEMA move the scope. Counting every CREATE retired
// the connection for each one, and the engine accrued a session per statement.
func TestOrdinaryDDLDoesNotChurnConnections(t *testing.T) {
	db := open(t)
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE churn_db",
		"USE DATABASE churn_db",
	} {
		if _, err := db.Exec(s); err != nil {
			t.Fatal(err)
		}
	}
	var settle int64
	if err := db.QueryRow("SELECT 1").Scan(&settle); err != nil {
		t.Fatal(err)
	}
	before := activeSessions(t)
	for i := 0; i < 10; i++ {
		if _, err := db.Exec("CREATE OR REPLACE TABLE churn_t (a INTEGER)"); err != nil {
			t.Fatal(err)
		}
	}
	grew := activeSessions(t) - before
	if grew > 1 {
		t.Errorf("ten ordinary DDL statements added %d engine session(s); they should reuse one", grew)
	}
}

// A 200 only says something is listening. Ping used to accept any server that answered one.
func TestPingRejectsANonFrostlakeServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("<html>hello</html>"))
	}))
	defer srv.Close()
	db, err := sql.Open("frostlake", "frostlake://"+strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.Ping(); err == nil {
		t.Error("Ping should refuse a server that does not answer as a Frostlake engine")
	}
}

func TestPingAcceptsTheEngine(t *testing.T) {
	db := open(t)
	defer db.Close()
	if err := db.Ping(); err != nil {
		t.Errorf("Ping against a real engine: %v", err)
	}
}

// A commit that never reached the engine used to leave the transaction open there, with the
// driver believing it had ended.
func TestFailedCommitEndsTheTransaction(t *testing.T) {
	requireServer(t)
	c, err := NewConnector(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	exec := dc.(driver.ExecerContext)
	query := dc.(driver.QueryerContext)
	bg := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE failed_commit_db",
		"USE DATABASE failed_commit_db",
		"CREATE TABLE t (a INTEGER)",
	} {
		if _, err := exec.ExecContext(bg, s, nil); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(bg)
	dtx, err := dc.(driver.ConnBeginTx).BeginTx(ctx, driver.TxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := exec.ExecContext(bg, "INSERT INTO t VALUES (1)", nil); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := dtx.Commit(); err == nil {
		t.Fatal("committing under a cancelled context should fail")
	}
	rows, err := query.QueryContext(bg, "SELECT COUNT(*) FROM t", nil)
	if err != nil {
		t.Fatal(err)
	}
	dest := make([]driver.Value, 1)
	if err := rows.Next(dest); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	if n, ok := dest[0].(int64); !ok || n != 0 {
		t.Errorf("after a failed commit the connection still sees %v row(s); the transaction was not ended", dest[0])
	}
	validator, ok := dc.(driver.Validator)
	if !ok {
		t.Fatal("the connection should implement driver.Validator")
	}
	if validator.IsValid() {
		t.Error("a connection whose commit failed should not be handed to anyone else")
	}
}

// The engine reclaims idle sessions and then re-creates one under the same id at its default
// scope, which a client cannot tell apart from its own session surviving. A connection that
// has sat idle therefore has its scope re-established rather than assumed.
func TestIdleConnectionRefreshesItsScope(t *testing.T) {
	cfg, err := parseDSN("frostlake://h:1/DB?schema=S")
	if err != nil {
		t.Fatal(err)
	}
	c := newConn(cfg)
	c.pendingUse = nil

	c.lastActivity = time.Now()
	if c.sessionMayHaveLapsed() {
		t.Error("a connection used a moment ago has not lapsed")
	}
	if err := c.ResetSession(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.pendingUse) != 0 {
		t.Errorf("a fresh connection should need no scope replay, queued %v", c.pendingUse)
	}

	c.lastActivity = time.Now().Add(-2 * scopeRefreshAfter)
	if !c.sessionMayHaveLapsed() {
		t.Error("a connection idle past the refresh window may have lapsed")
	}
	if err := c.ResetSession(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(c.pendingUse) != 2 {
		t.Errorf("an idle connection should replay its DSN scope, queued %v", c.pendingUse)
	}

	// A connection that has never run a statement has nothing to re-establish.
	fresh := newConn(cfg)
	fresh.pendingUse = nil
	if fresh.sessionMayHaveLapsed() {
		t.Error("a connection that never ran a statement has no session to lose")
	}
}

// A connection whose transport just broke must not be handed to the next caller: its socket
// is gone, and with it possibly the engine session behind it.
func TestTransportFailureRetiresTheConnection(t *testing.T) {
	c, err := NewConnector("frostlake://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	validator, ok := dc.(driver.Validator)
	if !ok {
		t.Fatal("the connection should implement driver.Validator")
	}
	if !validator.IsValid() {
		t.Fatal("a connection that has done nothing yet is valid")
	}
	_, err = dc.(driver.QueryerContext).QueryContext(context.Background(), "SELECT 1", nil)
	if err == nil {
		t.Fatal("expected a transport failure")
	}
	if validator.IsValid() {
		t.Error("a connection whose transport failed should be retired, not pooled")
	}
}

// Retiring must not cost the caller the diagnosis. Answering driver.ErrBadConn instead would
// turn this into a bare "driver: bad connection".
func TestTransportFailureKeepsItsDiagnosis(t *testing.T) {
	db, err := sql.Open("frostlake", "frostlake://127.0.0.1:9")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var v any
	err = db.QueryRow("SELECT 1").Scan(&v)
	if err == nil {
		t.Fatal("expected an error")
	}
	if errors.Is(err, driver.ErrBadConn) {
		t.Fatal("the statement's fate is unknown after a transport failure; it must not be retried")
	}
	for _, want := range []string{"127.0.0.1:9", "refused"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q lost the detail %q", err, want)
		}
	}
	var terr *TransportError
	if !errors.As(err, &terr) {
		t.Errorf("err = %#v, want a *frostlake.TransportError", err)
	}
}

// A cancelled context is the caller changing their mind, not a broken connection.
func TestCancelDoesNotRetireTheConnection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
	}))
	defer srv.Close()
	c, err := NewConnector("frostlake://" + strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := dc.(driver.QueryerContext).QueryContext(ctx, "SELECT 1", nil); err == nil {
		t.Fatal("expected the deadline to bite")
	}
	if !dc.(driver.Validator).IsValid() {
		t.Error("a cancelled statement should leave the connection usable")
	}
}

// An answer that is not a Frostlake response means we cannot identify the far side.
func TestUnidentifiableServerRetiresTheConnection(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("<html>not frostlake</html>"))
	}))
	defer srv.Close()
	c, err := NewConnector("frostlake://" + strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer dc.Close()
	if _, err := dc.(driver.QueryerContext).QueryContext(context.Background(), "SELECT 1", nil); err == nil {
		t.Fatal("expected an error")
	}
	if dc.(driver.Validator).IsValid() {
		t.Error("a connection to something that is not a Frostlake engine should be retired")
	}
}
