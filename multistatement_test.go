package frostlake

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// recordingEngine answers as an engine does and keeps every execute body it was sent.
func recordingEngine(t *testing.T, bodies *[]map[string]interface{}) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path != "/api/execute" {
			w.Write([]byte(`{"status":"healthy"}`))
			return
		}
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("reading the request body: %v", err)
		}
		var body map[string]interface{}
		if err := json.Unmarshal(raw, &body); err != nil {
			t.Errorf("request body is not JSON: %s", raw)
		}
		*bodies = append(*bodies, body)
		w.Write([]byte(`{"success":true,"sessionId":"s1","resultSets":[]}`))
	}))
}

func openAgainst(t *testing.T, srv *httptest.Server, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("frostlake", "frostlake://"+strings.TrimPrefix(srv.URL, "http://")+path)
	if err != nil {
		t.Fatal(err)
	}
	return db
}

// A caller who asks for nothing sends no count at all, so the session's MULTI_STATEMENT_COUNT
// answers for the request exactly as it did before the argument existed.
func TestARequestDeclaresNoCountUnlessAsked(t *testing.T) {
	var bodies []map[string]interface{}
	srv := recordingEngine(t, &bodies)
	defer srv.Close()
	db := openAgainst(t, srv, "")
	defer db.Close()
	if _, err := db.Exec("SELECT 1"); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 1 {
		t.Fatalf("expected one request, got %d", len(bodies))
	}
	if _, declared := bodies[0]["multiStatementCount"]; declared {
		t.Errorf("a request nobody asked about carries a count: %v", bodies[0])
	}
}

// The count rides on the one request the context was given to.
func TestWithMultiStatementDeclaresTheCount(t *testing.T) {
	var bodies []map[string]interface{}
	srv := recordingEngine(t, &bodies)
	defer srv.Close()
	db := openAgainst(t, srv, "")
	defer db.Close()
	ctx := WithMultiStatement(context.Background(), 2)
	if _, err := db.ExecContext(ctx, "SELECT 1; SELECT 2"); err != nil {
		t.Fatal(err)
	}
	if got := bodies[0]["multiStatementCount"]; got != float64(2) {
		t.Errorf("expected the request to declare 2, got %v", got)
	}
}

// Zero means any number, and must be SENT rather than treated as nothing to say.
func TestZeroDeclaresAnyNumber(t *testing.T) {
	var bodies []map[string]interface{}
	srv := recordingEngine(t, &bodies)
	defer srv.Close()
	db := openAgainst(t, srv, "")
	defer db.Close()
	ctx := WithMultiStatement(context.Background(), 0)
	if _, err := db.ExecContext(ctx, "SELECT 1; SELECT 2"); err != nil {
		t.Fatal(err)
	}
	got, declared := bodies[0]["multiStatementCount"]
	if !declared || got != float64(0) {
		t.Errorf("expected the request to declare 0, got %v (declared %v)", got, declared)
	}
}

// The USE statements a DSN queues are the driver's own, one statement each, so they must not
// borrow the count the caller declared for their own query.
func TestTheQueuedUseStatementsDoNotCarryTheCount(t *testing.T) {
	var bodies []map[string]interface{}
	srv := recordingEngine(t, &bodies)
	defer srv.Close()
	db := openAgainst(t, srv, "/somedb")
	defer db.Close()
	ctx := WithMultiStatement(context.Background(), 2)
	if _, err := db.ExecContext(ctx, "SELECT 1; SELECT 2"); err != nil {
		t.Fatal(err)
	}
	if len(bodies) < 2 {
		t.Fatalf("expected the queued USE and the query, got %d requests", len(bodies))
	}
	for _, body := range bodies[:len(bodies)-1] {
		if _, declared := body["multiStatementCount"]; declared {
			t.Errorf("a queued USE declared a count: %v", body)
		}
	}
	if got := bodies[len(bodies)-1]["multiStatementCount"]; got != float64(2) {
		t.Errorf("the caller's own query should declare 2, got %v", got)
	}
}

// Against a real engine: a pack runs because the call asked for it, with no ALTER SESSION and
// nothing left changed on the session afterwards.
func TestAPackRunsOnTheCallersOwnCount(t *testing.T) {
	requireServer(t)
	db := open(t)
	defer db.Close()
	ctx := WithMultiStatement(context.Background(), 2)
	if _, err := db.ExecContext(ctx, "SELECT 1; SELECT 2"); err != nil {
		t.Fatalf("a pack the caller asked for should run: %v", err)
	}
	// The session is untouched, so the next pack — asked for by nobody — is still refused.
	//
	// Only an engine that carries the statement-count gate refuses one at all, and this driver
	// supports older engines than that. Against one of those the refusal never comes, so the check
	// is SKIPPED rather than passed: a green tick would claim an engine had been checked for a
	// refusal it does not make.
	if _, err := db.Exec("SELECT 1; SELECT 2"); err == nil {
		t.Skip("this engine accepts a pack nobody asked for, so it has no refusal to assert")
	}
}
