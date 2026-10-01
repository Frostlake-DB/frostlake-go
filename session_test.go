package frostlake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// How a connection keeps its idea of the engine session in step with the engine's: requireSession
// once the engine is known to offer it, recovery from the engine's refusal of a lost session, and
// releasing the session on close. The scripted tests need no engine: every request a scenario
// makes is one it scripted.

// scripted is one reply of a scripted engine: a status and body, a connection dropped without an
// answer, or a request left unanswered until the client gives up.
type scripted struct {
	status int
	body   string
	drop   bool
	hang   bool
}

// sentRequest is one request a scripted engine received, with its JSON body read back.
type sentRequest struct {
	method string
	path   string
	body   map[string]any
}

func (r sentRequest) sql() string {
	text, _ := r.body["sql"].(string)
	return text
}

func (r sentRequest) sessionID() string {
	id, _ := r.body["sessionId"].(string)
	return id
}

// requireSession is the field's value and whether the request carried it at all.
func (r sentRequest) requireSession() (value, present bool) {
	raw, present := r.body["requireSession"]
	value, _ = raw.(bool)
	return value, present
}

func (r sentRequest) autoCommit() bool {
	value, _ := r.body["autoCommit"].(bool)
	return value
}

// scriptedEngine answers POST /api/execute from a script and records every request. An execute
// the script does not cover fails the test. DELETE /api/sessions/{id} takes its answer from a
// script of its own, and is answered as released when that one is empty.
type scriptedEngine struct {
	t       *testing.T
	srv     *httptest.Server
	mu      sync.Mutex
	script  []scripted
	deletes []scripted
	sent    []sentRequest
}

func newScriptedEngine(t *testing.T) *scriptedEngine {
	t.Helper()
	e := &scriptedEngine{t: t}
	e.srv = httptest.NewServer(http.HandlerFunc(e.serve))
	t.Cleanup(e.srv.Close)
	return e
}

func (e *scriptedEngine) serve(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/api/health" {
		w.Write([]byte(`{"status":"healthy","activeSessions":0}`))
		return
	}
	raw, _ := io.ReadAll(r.Body)
	var body map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &body); err != nil {
			e.t.Errorf("request body is not JSON: %s", raw)
		}
	}
	e.mu.Lock()
	e.sent = append(e.sent, sentRequest{method: r.Method, path: r.URL.Path, body: body})
	var next scripted
	switch {
	case r.Method == http.MethodDelete && len(e.deletes) > 0:
		next, e.deletes = e.deletes[0], e.deletes[1:]
	case r.Method == http.MethodDelete:
		next = released
	case len(e.script) > 0:
		next, e.script = e.script[0], e.script[1:]
	default:
		e.mu.Unlock()
		e.t.Errorf("unscripted request: %s %s %s", r.Method, r.URL.Path, raw)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	e.mu.Unlock()
	switch {
	case next.hang:
		<-r.Context().Done()
	case next.drop:
		if hijacker, ok := w.(http.Hijacker); ok {
			if socket, _, err := hijacker.Hijack(); err == nil {
				socket.Close()
			}
		}
	default:
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(next.status)
		w.Write([]byte(next.body))
	}
}

// reply queues answers for the execute requests to come.
func (e *scriptedEngine) reply(answers ...scripted) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.script = append(e.script, answers...)
}

// replyToDelete queues answers for the DELETE requests to come.
func (e *scriptedEngine) replyToDelete(answers ...scripted) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.deletes = append(e.deletes, answers...)
}

func (e *scriptedEngine) requests() []sentRequest {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]sentRequest(nil), e.sent...)
}

// executes is the SQL of every POST /api/execute so far.
func (e *scriptedEngine) executes() []string {
	var out []string
	for _, r := range e.requests() {
		if r.path == "/api/execute" {
			out = append(out, r.sql())
		}
	}
	return out
}

// deletesSent is every DELETE so far.
func (e *scriptedEngine) deletesSent() []sentRequest {
	var out []sentRequest
	for _, r := range e.requests() {
		if r.method == http.MethodDelete {
			out = append(out, r)
		}
	}
	return out
}

// last is the most recent request.
func (e *scriptedEngine) last() sentRequest {
	all := e.requests()
	if len(all) == 0 {
		e.t.Fatal("no request was sent")
	}
	return all[len(all)-1]
}

// pending fails the test when scripted answers were never asked for.
func (e *scriptedEngine) pending() {
	e.t.Helper()
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.script) > 0 {
		e.t.Errorf("%d scripted answer(s) were never asked for", len(e.script))
	}
}

func (e *scriptedEngine) dsn(rest string) string {
	return "frostlake://" + strings.TrimPrefix(e.srv.URL, "http://") + rest
}

// answer is an engine that reports newSession (0.1.0 and later) answering with no result set.
func answer(sessionID string, fresh bool) scripted {
	return scripted{status: 200, body: fmt.Sprintf(
		`{"errorMessage":null,"executionTimeMs":1,"newSession":%t,"resultSets":[],"sessionId":%q,"success":true}`,
		fresh, sessionID)}
}

// answerNumber is the same answer carrying one NUMBER column N holding value.
func answerNumber(sessionID string, value int) scripted {
	return scripted{status: 200, body: fmt.Sprintf(
		`{"errorMessage":null,"executionTimeMs":1,"newSession":false,"resultSets":[{"columns":[{"dataType":"NUMBER","name":"N","nullable":false,"precision":38,"scale":0}],"rowCount":1,"rows":[[%d]],"updateCount":-1}],"sessionId":%q,"success":true}`,
		value, sessionID)}
}

// legacyAnswer is an engine that predates newSession (0.0.7) answering.
func legacyAnswer(sessionID string) scripted {
	return scripted{status: 200, body: fmt.Sprintf(
		`{"errorMessage":null,"executionTimeMs":1,"resultSets":[],"sessionId":%q,"success":true}`, sessionID)}
}

// sessionGone is the 404 a requireSession request gets when its session is gone.
func sessionGone(sessionID string) scripted {
	return scripted{status: 404, body: fmt.Sprintf(
		`{"errorMessage":"Session '%s' does not exist or has expired.","executionTimeMs":0,"newSession":false,"resultSets":[],"sessionId":null,"success":false}`,
		sessionID)}
}

// released is the engine's answer to DELETE /api/sessions/{id}.
var released = scripted{status: 200,
	body: `{"errorMessage":null,"executionTimeMs":0,"newSession":false,"resultSets":[],"sessionId":null,"success":true}`}

const (
	scopedDSN = "/APP?schema=PUBLIC"
	useApp    = "USE DATABASE APP"
	usePublic = "USE SCHEMA PUBLIC"
)

// pinnedOn opens a pool on a scripted engine and pins one connection of it, which then holds
// session s1 on the DSN's scope after one statement: the engine reports newSession.
func pinnedOn(t *testing.T, e *scriptedEngine) (*sql.DB, *sql.Conn) {
	t.Helper()
	db, err := sql.Open("frostlake", e.dsn(scopedDSN))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	e.reply(answer("s1", true), answer("s1", false), answer("s1", false))
	if _, err := conn.ExecContext(context.Background(), "SELECT 0"); err != nil {
		t.Fatal(err)
	}
	return db, conn
}

func sameStatements(t *testing.T, got, want []string) {
	t.Helper()
	if strings.Join(got, " | ") != strings.Join(want, " | ") {
		t.Errorf("statements sent:\n got  %q\n want %q", got, want)
	}
}

func asSessionLost(t *testing.T, err error, wording string) *SessionLostError {
	t.Helper()
	var lost *SessionLostError
	if !errors.As(err, &lost) {
		t.Fatalf("err = %#v, want a *frostlake.SessionLostError", err)
	}
	if !strings.Contains(lost.Message, wording) {
		t.Errorf("message %q does not mention %q", lost.Message, wording)
	}
	if errors.Is(err, driver.ErrBadConn) {
		t.Error("a lost session must not reach database/sql as ErrBadConn, which would re-run the statement elsewhere")
	}
	return lost
}

// The first request names no session, so there is nothing to require; the first answer says the
// engine offers requireSession, and every request naming the session carries it from then on.
func TestRequireSessionFollowsTheFirstAnswer(t *testing.T) {
	e := newScriptedEngine(t)
	_, conn := pinnedOn(t, e)
	e.reply(answer("s1", false))
	if _, err := conn.ExecContext(context.Background(), "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	sent := e.requests()
	sameStatements(t, e.executes(), []string{useApp, usePublic, "SELECT 0", "SELECT 1"})
	if id := sent[0].sessionID(); id != "" {
		t.Errorf("the first request named session %q before one existed", id)
	}
	if _, present := sent[0].requireSession(); present {
		t.Error("the first request carried requireSession before the engine said it offers it")
	}
	for _, r := range sent[1:] {
		if value, present := r.requireSession(); r.sessionID() != "s1" || !present || !value {
			t.Errorf("%q went with session %q and requireSession %v (present %v); want s1 and true",
				r.sql(), r.sessionID(), value, present)
		}
	}
	e.pending()
}

// An engine that predates newSession has neither requireSession nor DELETE /api/sessions, and
// its parser may refuse a field it does not know: it is never sent either.
func TestAnOlderEngineIsSentNeitherRequireSessionNorADelete(t *testing.T) {
	e := newScriptedEngine(t)
	db, err := sql.Open("frostlake", e.dsn(scopedDSN))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e.reply(legacyAnswer("old1"), legacyAnswer("old1"), legacyAnswer("old1"), legacyAnswer("old1"))
	for _, q := range []string{"SELECT 1", "SELECT 2"} {
		if _, err := conn.ExecContext(context.Background(), q); err != nil {
			t.Fatal(err)
		}
	}
	conn.Close()
	db.Close()
	for _, r := range e.requests() {
		if _, present := r.requireSession(); present {
			t.Errorf("%s %s %q carried requireSession to an engine that predates it", r.method, r.path, r.sql())
		}
	}
	if deletes := e.deletesSent(); len(deletes) != 0 {
		t.Errorf("closing sent %d DELETE(s) to an engine that has no such endpoint", len(deletes))
	}
	e.pending()
}

// The engine refuses a session it no longer holds (it expired, was released, or the server
// restarted) and nothing ran. A session that held nothing but the DSN's scope is replaced: the
// scope goes onto a fresh session, and the statement is sent once more.
func TestALostSessionIsReplacedAndTheStatementSentOnceMore(t *testing.T) {
	e := newScriptedEngine(t)
	db, conn := pinnedOn(t, e)
	e.reply(sessionGone("s1"), answer("s2", true), answer("s2", false), answerNumber("s2", 2))
	var n int64
	if err := conn.QueryRowContext(context.Background(), "SELECT 2 AS N").Scan(&n); err != nil {
		t.Fatalf("the statement should have been sent once more on a fresh session: %v", err)
	}
	if n != 2 {
		t.Errorf("N = %d, want 2", n)
	}
	sameStatements(t, e.executes(), []string{useApp, usePublic, "SELECT 0",
		"SELECT 2 AS N", useApp, usePublic, "SELECT 2 AS N"})
	sent := e.requests()
	if id := sent[4].sessionID(); id != "" {
		t.Errorf("the fresh session's scope went to session %q; it should start a new one", id)
	}
	if value, _ := sent[6].requireSession(); sent[6].sessionID() != "s2" || !value {
		t.Errorf("the statement was re-sent with session %q, requireSession %v; want s2 and true",
			sent[6].sessionID(), value)
	}
	e.pending()
	// The replacement is what is released on close, not the lost session.
	conn.Close()
	db.Close()
	if deletes := e.deletesSent(); len(deletes) != 1 || deletes[0].path != "/api/sessions/s2" {
		t.Errorf("closing sent %v; want one DELETE /api/sessions/s2", deletes)
	}
}

// A statement is sent once more, never twice: the engine refusing the session it just started
// is reported, and the connection starts over on the next statement.
func TestASecondRefusalIsReported(t *testing.T) {
	e := newScriptedEngine(t)
	_, conn := pinnedOn(t, e)
	e.reply(sessionGone("s1"), answer("s2", true), answer("s2", false), sessionGone("s2"))
	_, err := conn.ExecContext(context.Background(), "SELECT 2")
	asSessionLost(t, err, "just started")
	sameStatements(t, e.executes()[3:], []string{"SELECT 2", useApp, usePublic, "SELECT 2"})

	e.reply(answer("s3", true), answer("s3", false), answer("s3", false))
	if _, err := conn.ExecContext(context.Background(), "SELECT 3"); err != nil {
		t.Fatalf("the connection should start over after the refusal: %v", err)
	}
	sent := e.requests()
	if id := sent[len(sent)-3].sessionID(); id != "" {
		t.Errorf("the next statement's scope went to session %q; it should start a new one", id)
	}
	e.pending()
}

// A transaction does not survive its session. The statement is reported, not re-run where it
// would commit on its own, and the Tx refuses everything after it: its Commit cannot succeed.
func TestALostTransactionIsReportedNotReplaced(t *testing.T) {
	for _, end := range []string{"commit", "rollback"} {
		t.Run(end, func(t *testing.T) {
			e := newScriptedEngine(t)
			_, conn := pinnedOn(t, e)
			ctx := context.Background()
			e.reply(answer("s1", false))
			tx, err := conn.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			// Ends the Tx when a check below stops the test early; after Commit or Rollback it
			// is ErrTxDone and sends nothing.
			defer tx.Rollback()
			e.reply(sessionGone("s1"))
			_, err = tx.ExecContext(ctx, "INSERT INTO T VALUES (1)")
			asSessionLost(t, err, "transaction")
			sent := len(e.requests())
			_, err = tx.ExecContext(ctx, "INSERT INTO T VALUES (2)")
			asSessionLost(t, err, "transaction")
			if end == "commit" {
				asSessionLost(t, tx.Commit(), "nothing was committed")
			} else if err := tx.Rollback(); err != nil {
				t.Errorf("rolling back a transaction the engine discarded should succeed: %v", err)
			}
			if now := len(e.requests()); now != sent {
				t.Errorf("%d request(s) went out for a transaction that is already gone", now-sent)
			}
			sameStatements(t, e.executes(), []string{useApp, usePublic, "SELECT 0", "BEGIN",
				"INSERT INTO T VALUES (1)"})

			// The connection stays usable: the next statement starts over on the DSN's scope,
			// outside any transaction.
			e.reply(answer("s2", true), answer("s2", false), answer("s2", false))
			if _, err := conn.ExecContext(ctx, "SELECT 1"); err != nil {
				t.Fatal(err)
			}
			if last := e.last(); last.sessionID() != "s2" || !last.autoCommit() {
				t.Errorf("the next statement went to %q with autoCommit %v; want s2 and true",
					last.sessionID(), last.autoCommit())
			}
			e.pending()
		})
	}
}

// A transaction opened with a BEGIN statement rather than BeginTx is lost the same way.
func TestALostTransactionBegunInSQLIsReported(t *testing.T) {
	e := newScriptedEngine(t)
	_, conn := pinnedOn(t, e)
	ctx := context.Background()
	e.reply(answer("s1", false), sessionGone("s1"))
	if _, err := conn.ExecContext(ctx, "BEGIN TRANSACTION"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.ExecContext(ctx, "INSERT INTO T VALUES (1)")
	asSessionLost(t, err, "transaction")
	e.reply(answer("s2", true), answer("s2", false), answer("s2", false))
	if _, err := conn.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatal(err)
	}
	sameStatements(t, e.executes()[3:], []string{"BEGIN TRANSACTION", "INSERT INTO T VALUES (1)",
		useApp, usePublic, "SELECT 1"})
	e.pending()
}

// BeginTx that finds the session gone under a transaction a BEGIN statement opened reports the
// loss, and with no Tx left to end it the connection carries on rather than refusing statements.
func TestABeginTxFindingATransactionLostLeavesTheConnectionUsable(t *testing.T) {
	e := newScriptedEngine(t)
	_, conn := pinnedOn(t, e)
	ctx := context.Background()
	e.reply(answer("s1", false), sessionGone("s1"))
	if _, err := conn.ExecContext(ctx, "BEGIN"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.BeginTx(ctx, nil)
	asSessionLost(t, err, "transaction")
	e.reply(answer("s2", true), answer("s2", false), answer("s2", false))
	if _, err := conn.ExecContext(ctx, "SELECT 1"); err != nil {
		t.Fatalf("the connection should carry on after a failed BeginTx: %v", err)
	}
	if last := e.last(); last.sessionID() != "s2" || !last.autoCommit() {
		t.Errorf("the next statement went to %q with autoCommit %v; want s2 and true",
			last.sessionID(), last.autoCommit())
	}
	e.pending()
}

// A session that held context a fresh one would not have — a moved scope, a variable, a
// setting, a temporary object — is reported rather than replaced: re-run elsewhere, the statement
// would read something else.
func TestALostContextIsReportedNotReplaced(t *testing.T) {
	for _, statement := range []string{
		"USE SCHEMA OTHER",
		"SET v = 1",
		"UNSET v",
		"ALTER SESSION SET TIMEZONE = 'UTC'",
		"CREATE TEMPORARY TABLE tt (a INT)",
		"CREATE OR REPLACE DATABASE d2",
		"DROP SCHEMA IF EXISTS s2",
		"SELECT 1; USE SCHEMA OTHER",
	} {
		t.Run(statement, func(t *testing.T) {
			e := newScriptedEngine(t)
			_, conn := pinnedOn(t, e)
			ctx := WithMultiStatement(context.Background(), 0)
			e.reply(answer("s1", false), sessionGone("s1"))
			if _, err := conn.ExecContext(ctx, statement); err != nil {
				t.Fatal(err)
			}
			_, err := conn.ExecContext(context.Background(), "SELECT * FROM T")
			asSessionLost(t, err, "context")
			sameStatements(t, e.executes()[3:], []string{statement, "SELECT * FROM T"})
			e.pending()
		})
	}
}

// Ordinary DDL leaves nothing a fresh session would miss, so its session is replaced.
func TestALostSessionAfterOrdinaryDDLIsReplaced(t *testing.T) {
	e := newScriptedEngine(t)
	_, conn := pinnedOn(t, e)
	ctx := context.Background()
	e.reply(answer("s1", false), sessionGone("s1"), answer("s2", true), answer("s2", false), answer("s2", false))
	if _, err := conn.ExecContext(ctx, "CREATE OR REPLACE TABLE T (a INT)"); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.ExecContext(ctx, "INSERT INTO T VALUES (1)"); err != nil {
		t.Fatalf("a session that held nothing but its scope should be replaced: %v", err)
	}
	e.pending()
}

// A pooled connection whose session moved is put back on the DSN's scope before its next
// borrower, and the scope finding the session gone simply goes onto a fresh one.
func TestAPooledConnectionFindingItsSessionGoneStartsAFreshOne(t *testing.T) {
	e := newScriptedEngine(t)
	db, err := sql.Open("frostlake", e.dsn(scopedDSN))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	e.reply(answer("s1", true), answer("s1", false), answer("s1", false))
	if _, err := db.Exec("USE SCHEMA OTHER"); err != nil {
		t.Fatal(err)
	}
	e.reply(sessionGone("s1"), answer("s2", true), answer("s2", false), answerNumber("s2", 7))
	var n int64
	if err := db.QueryRow("SELECT 7 AS N").Scan(&n); err != nil {
		t.Fatalf("the next borrower should get a fresh session on the DSN's scope: %v", err)
	}
	sameStatements(t, e.executes(), []string{useApp, usePublic, "USE SCHEMA OTHER",
		useApp, useApp, usePublic, "SELECT 7 AS N"})
	e.pending()
}

// A refusal leaves the connection clean — no session, the DSN's scope queued — so database/sql
// keeps it, and whoever borrows it next starts a fresh session on the DSN's scope.
func TestARefusedConnectionGoesBackToThePoolClean(t *testing.T) {
	e := newScriptedEngine(t)
	db, conn := pinnedOn(t, e)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	e.reply(answer("s1", false), sessionGone("s1"))
	if _, err := conn.ExecContext(ctx, "USE SCHEMA OTHER"); err != nil {
		t.Fatal(err)
	}
	_, err := conn.ExecContext(ctx, "SELECT 1")
	asSessionLost(t, err, "context")
	var valid bool
	if err := conn.Raw(func(dc any) error {
		valid = dc.(driver.Validator).IsValid()
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if !valid {
		t.Error("a connection that only lost its session should stay in the pool")
	}
	conn.Close()
	e.reply(answer("s2", true), answer("s2", false), answerNumber("s2", 3))
	var n int64
	if err := db.QueryRow("SELECT 3 AS N").Scan(&n); err != nil {
		t.Fatal(err)
	}
	sameStatements(t, e.executes()[4:], []string{"SELECT 1", useApp, usePublic, "SELECT 3 AS N"})
	if id := e.requests()[5].sessionID(); id != "" {
		t.Errorf("the next borrower's scope went to session %q; it should start a new one", id)
	}
	e.pending()
}

// connectOn opens one driver connection on a scripted engine, bypassing the pool, and runs a
// statement on it, answered by replies, so that it holds a session.
func connectOn(t *testing.T, e *scriptedEngine, rest string, replies ...scripted) driver.Conn {
	t.Helper()
	c, err := NewConnector(e.dsn(rest))
	if err != nil {
		t.Fatal(err)
	}
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	e.reply(replies...)
	if _, err := dc.(driver.ExecerContext).ExecContext(context.Background(), "SELECT 1", nil); err != nil {
		t.Fatal(err)
	}
	return dc
}

// Closing releases the session with one DELETE, which also rolls back a transaction it left
// open; closing again sends nothing.
func TestClosingReleasesTheSessionOnce(t *testing.T) {
	e := newScriptedEngine(t)
	dc := connectOn(t, e, "", answer("s1", true))
	if err := dc.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	deletes := e.deletesSent()
	if len(deletes) != 1 || deletes[0].path != "/api/sessions/s1" {
		t.Fatalf("closing sent %v; want one DELETE /api/sessions/s1", deletes)
	}
	sent := len(e.requests())
	if err := dc.Close(); err != nil {
		t.Errorf("a second Close: %v", err)
	}
	if now := len(e.requests()); now != sent {
		t.Errorf("a second Close sent %d request(s)", now-sent)
	}
}

// Releasing is a courtesy: whatever the engine answers, or fails to, Close reports no error, and
// a DELETE left unanswered costs no more than the DSN's timeout.
func TestClosingNeverFails(t *testing.T) {
	for _, c := range []struct {
		name  string
		reply scripted
	}{
		{"released", released},
		{"unknown session", scripted{status: 404, body: `{"success":false,"sessionId":null,"errorMessage":"Session 's1' does not exist or has expired."}`}},
		{"method not allowed", scripted{status: 405, body: `{"error":"Method not allowed"}`}},
		{"not a Frostlake answer", scripted{status: 502, body: "<html>Bad Gateway</html>"}},
		{"closed socket", scripted{drop: true}},
		{"no answer", scripted{hang: true}},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newScriptedEngine(t)
			dc := connectOn(t, e, "?timeout=300ms", answer("s1", true))
			e.replyToDelete(c.reply)
			started := time.Now()
			if err := dc.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
			if took := time.Since(started); took > 3*time.Second {
				t.Errorf("Close took %v; releasing is bounded by the DSN's timeout", took)
			}
			if deletes := e.deletesSent(); len(deletes) != 1 {
				t.Errorf("closing sent %d DELETE(s); want 1", len(deletes))
			}
		})
	}
}

// Nothing is sent on close without a session to release.
func TestClosingWithoutASessionSendsNothing(t *testing.T) {
	e := newScriptedEngine(t)
	c, err := NewConnector(e.dsn(scopedDSN))
	if err != nil {
		t.Fatal(err)
	}
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := dc.Close(); err != nil {
		t.Fatal(err)
	}
	if sent := e.requests(); len(sent) != 0 {
		t.Errorf("closing a connection that never ran a statement sent %d request(s)", len(sent))
	}
}

// The engine that reports newSession refuses a session it no longer holds, so the idle re-scope
// that guards against an older engine's silent replacement is not needed against it.
func TestIdleRescopeIsOnlyForAnOlderEngine(t *testing.T) {
	for _, c := range []struct {
		name    string
		replies []scripted
		want    int
	}{
		{"engine reports newSession", []scripted{answer("s1", true), answer("s1", false), answer("s1", false)}, 0},
		{"engine predates newSession", []scripted{legacyAnswer("old1"), legacyAnswer("old1"), legacyAnswer("old1")}, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			e := newScriptedEngine(t)
			dc := connectOn(t, e, scopedDSN, c.replies...)
			defer dc.Close()
			fc := dc.(*conn)
			fc.lastActivity = time.Now().Add(-2 * scopeRefreshAfter)
			if err := fc.ResetSession(context.Background()); err != nil {
				t.Fatal(err)
			}
			if len(fc.pendingUse) != c.want {
				t.Errorf("an idle connection queued %v; want %d statement(s)", fc.pendingUse, c.want)
			}
		})
	}
}

// A BEGIN statement run through the pool leaves the engine session inside a transaction that no
// caller holds: the pool promises nobody that their next statement lands on the same connection.
// That connection is not pooled again. database/sql discards it as the call returns, and closing
// it releases the session, which rolls the transaction back; the next statement starts a fresh
// session outside it.
func TestATransactionOpenedThroughThePoolIsNotPooled(t *testing.T) {
	e := newScriptedEngine(t)
	db, err := sql.Open("frostlake", e.dsn(scopedDSN))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	e.reply(answer("s1", true), answer("s1", false), answer("s1", false))
	if _, err := db.Exec("BEGIN"); err != nil {
		t.Fatal(err)
	}
	if deletes := e.deletesSent(); len(deletes) != 1 || deletes[0].path != "/api/sessions/s1" {
		t.Fatalf("the connection a BEGIN left inside a transaction went back to the pool: DELETEs sent %v", deletes)
	}
	e.reply(answer("s2", true), answer("s2", false), answer("s2", false))
	if _, err := db.Exec("INSERT INTO T VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	sameStatements(t, e.executes(), []string{useApp, usePublic, "BEGIN",
		useApp, usePublic, "INSERT INTO T VALUES (1)"})
	// The scope went onto a fresh session, and the INSERT ran there, in autocommit.
	if id := e.requests()[4].sessionID(); id != "" {
		t.Errorf("the next statement's scope went to session %q; it should start a new one", id)
	}
	if last := e.last(); last.sessionID() != "s2" || !last.autoCommit() {
		t.Errorf("the INSERT went to %q with autoCommit %v; want s2 and true", last.sessionID(), last.autoCommit())
	}
	e.pending()
}

// A transaction its caller ends — across the calls of a pinned connection, or within one call —
// leaves the connection pooled.
func TestATransactionItsCallerEndsKeepsItsConnectionPooled(t *testing.T) {
	e := newScriptedEngine(t)
	db, conn := pinnedOn(t, e)
	db.SetMaxOpenConns(1)
	ctx := context.Background()
	e.reply(answer("s1", false), answer("s1", false), answer("s1", false))
	for _, statement := range []string{"BEGIN", "INSERT INTO T VALUES (1)", "COMMIT"} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	conn.Close()
	e.reply(answer("s1", false), answer("s1", false))
	if _, err := db.ExecContext(WithMultiStatement(ctx, 0), "BEGIN; INSERT INTO T VALUES (2); COMMIT"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if deletes := e.deletesSent(); len(deletes) != 0 {
		t.Errorf("a connection whose transaction was ended was discarded: DELETEs sent %v", deletes)
	}
	for _, r := range e.requests()[3:] {
		if r.sessionID() != "s1" {
			t.Errorf("%q went to session %q; the pooled connection should have kept s1", r.sql(), r.sessionID())
		}
	}
	e.pending()
}

// A pinned connection returned to the pool with its transaction still open is discarded too:
// closing it releases the session, which rolls the transaction back.
func TestAPinnedConnectionReturnedInsideATransactionIsDiscarded(t *testing.T) {
	e := newScriptedEngine(t)
	_, conn := pinnedOn(t, e)
	ctx := context.Background()
	e.reply(answer("s1", false), answer("s1", false))
	for _, statement := range []string{"BEGIN", "INSERT INTO T VALUES (1)"} {
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	conn.Close()
	if deletes := e.deletesSent(); len(deletes) != 1 || deletes[0].path != "/api/sessions/s1" {
		t.Errorf("a connection returned inside a transaction was pooled: DELETEs sent %v", deletes)
	}
}

// Whether the pool may keep a connection follows the transaction its statements opened and
// closed.
func TestAConnectionInsideATransactionIsNotValidForThePool(t *testing.T) {
	e := newScriptedEngine(t)
	dc := connectOn(t, e, "", answer("s1", true))
	defer dc.Close()
	validator := dc.(driver.Validator)
	for _, step := range []struct {
		statement string
		valid     bool
	}{
		{"BEGIN", false},
		{"INSERT INTO T VALUES (1)", false},
		{"COMMIT", true},
		{"START TRANSACTION", false},
		{"ROLLBACK", true},
		{"BEGIN WORK", false},
		{"COMMIT WORK", true},
		// A scripting block is not a transaction.
		{"BEGIN LET n INTEGER := 1; RETURN :n; END;", true},
	} {
		e.reply(answer("s1", false))
		if _, err := dc.(driver.ExecerContext).ExecContext(context.Background(), step.statement, nil); err != nil {
			t.Fatal(err)
		}
		if got := validator.IsValid(); got != step.valid {
			t.Errorf("after %q IsValid() = %v, want %v", step.statement, got, step.valid)
		}
	}
}

// Engine-backed: a session released out of band (as the engine's idle reaper or a restart would
// lose it) is replaced by a fresh one on the DSN's scope, and the statement runs there.

// lostScopeDB makes the database and schema the engine-backed session tests name in their DSN.
func lostScopeDB(t *testing.T) *sql.DB {
	t.Helper()
	setup := open(t)
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE go_lost_db",
		"CREATE OR REPLACE SCHEMA go_lost_db.lost_schema",
		"CREATE OR REPLACE TABLE go_lost_db.lost_schema.lost_t (a INTEGER)",
	} {
		mustExec(t, setup, s)
	}
	setup.Close()
	db, err := sql.Open("frostlake", testDSN+"/GO_LOST_DB?schema=LOST_SCHEMA")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func sessionOf(t *testing.T, pinned *sql.Conn) string {
	t.Helper()
	var id string
	if err := pinned.Raw(func(dc any) error {
		id = dc.(*conn).sessionID
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if id == "" {
		t.Fatal("the connection holds no session")
	}
	return id
}

// releaseOutOfBand ends a session behind its connection's back.
func releaseOutOfBand(t *testing.T, id string) {
	t.Helper()
	base := strings.Replace(testDSN, "frostlake://", "http://", 1)
	req, err := http.NewRequest(http.MethodDelete, base+"/api/sessions/"+id, nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("releasing session %s answered HTTP %d", id, resp.StatusCode)
	}
}

func currentScope(t *testing.T, q interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}) string {
	t.Helper()
	var scope string
	if err := q.QueryRowContext(context.Background(),
		"SELECT CURRENT_DATABASE() || '.' || CURRENT_SCHEMA()").Scan(&scope); err != nil {
		t.Fatal(err)
	}
	return scope
}

func TestAReleasedSessionComesBackOnTheDSNScope(t *testing.T) {
	db := lostScopeDB(t)
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if scope := currentScope(t, conn); scope != "GO_LOST_DB.LOST_SCHEMA" {
		t.Fatalf("the DSN's scope is %s, want GO_LOST_DB.LOST_SCHEMA", scope)
	}
	id := sessionOf(t, conn)
	releaseOutOfBand(t, id)
	if scope := currentScope(t, conn); scope != "GO_LOST_DB.LOST_SCHEMA" {
		t.Errorf("after its session was released the connection runs in %s, want GO_LOST_DB.LOST_SCHEMA", scope)
	}
	if sessionOf(t, conn) == id {
		t.Error("the connection still names the released session")
	}
}

// The same through the pool, with no connection pinned.
func TestAPooledConnectionOutlivesItsReleasedSession(t *testing.T) {
	db := lostScopeDB(t)
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if scope := currentScope(t, db); scope != "GO_LOST_DB.LOST_SCHEMA" {
		t.Fatalf("the DSN's scope is %s, want GO_LOST_DB.LOST_SCHEMA", scope)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	releaseOutOfBand(t, sessionOf(t, conn))
	conn.Close()
	if scope := currentScope(t, db); scope != "GO_LOST_DB.LOST_SCHEMA" {
		t.Errorf("after its session was released the pool runs in %s, want GO_LOST_DB.LOST_SCHEMA", scope)
	}
}

func TestAReleasedSessionUnderATransactionIsReported(t *testing.T) {
	db := lostScopeDB(t)
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if scope := currentScope(t, conn); scope != "GO_LOST_DB.LOST_SCHEMA" {
		t.Fatalf("the DSN's scope is %s, want GO_LOST_DB.LOST_SCHEMA", scope)
	}
	id := sessionOf(t, conn)
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "INSERT INTO lost_t VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	releaseOutOfBand(t, id)
	_, err = tx.ExecContext(ctx, "INSERT INTO lost_t VALUES (2)")
	asSessionLost(t, err, "transaction")
	var lost *SessionLostError
	if err := tx.Commit(); !errors.As(err, &lost) {
		t.Errorf("committing a transaction that went with its session: %v, want a *frostlake.SessionLostError", err)
	}
	// The connection stays usable, on the DSN's scope, and nothing of the transaction survived.
	if scope := currentScope(t, conn); scope != "GO_LOST_DB.LOST_SCHEMA" {
		t.Errorf("after the loss the connection runs in %s, want GO_LOST_DB.LOST_SCHEMA", scope)
	}
	var n int64
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM lost_t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Errorf("%d row(s) of the lost transaction survived", n)
	}
}

func TestClosingLowersTheEngineSessionCount(t *testing.T) {
	requireServer(t)
	c, err := NewConnector(testDSN)
	if err != nil {
		t.Fatal(err)
	}
	dc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := dc.(driver.ExecerContext).ExecContext(context.Background(), "SELECT 1", nil); err != nil {
		t.Fatal(err)
	}
	before := activeSessions(t)
	if err := dc.Close(); err != nil {
		t.Fatal(err)
	}
	if after := activeSessions(t); after != before-1 {
		t.Errorf("closing a connection left %d active session(s); want %d", after, before-1)
	}
}

// openTransactions counts the transactions the engine holds open, in every session.
func openTransactions(t *testing.T, db *sql.DB) int {
	t.Helper()
	rows, err := db.Query("SHOW TRANSACTIONS")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	n := 0
	for rows.Next() {
		n++
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return n
}

// Engine-backed: through a one-connection pool, a BEGIN statement and then an INSERT. The INSERT
// must not join the transaction the BEGIN left behind, where closing the pool would roll it back
// unseen; it runs on a fresh session and commits on its own.
func TestARawBeginThroughThePoolDoesNotSwallowTheNextStatement(t *testing.T) {
	setup := open(t)
	defer setup.Close()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE go_raw_tx_db",
		"CREATE OR REPLACE TABLE go_raw_tx_db.public.raw_t (a INTEGER)",
	} {
		mustExec(t, setup, s)
	}
	before := openTransactions(t, setup)

	db, err := sql.Open("frostlake", testDSN+"/GO_RAW_TX_DB?schema=PUBLIC")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	mustExec(t, db, "BEGIN")
	// Nobody holds that transaction, and it is gone as soon as the call that opened it returns.
	if now := openTransactions(t, setup); now != before {
		t.Errorf("after a BEGIN through the pool the engine holds %d open transaction(s), want %d", now, before)
	}
	mustExec(t, db, "INSERT INTO raw_t VALUES (1)")
	db.Close()

	fresh := open(t)
	defer fresh.Close()
	var n int64
	if err := fresh.QueryRow("SELECT COUNT(*) FROM go_raw_tx_db.public.raw_t").Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("a fresh pool sees %d row(s); the INSERT after a pooled BEGIN should have committed on its own", n)
	}
}
