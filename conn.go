package frostlake

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// maxErrorBody bounds how much of a response is kept for error reporting. The body is not
// buffered beyond this — a result set streams straight into the decoder.
const maxErrorBody = 512

type conn struct {
	cfg        *config
	client     *http.Client
	sessionID  string
	autoCommit bool
	// USE statements from the DSN that must run before the next user statement.
	pendingUse []string
	// Set once a statement has moved the session off the DSN's scope, so a pooled
	// connection is put back on it before database/sql hands it out again.
	sessionDirty bool
	// When the connection last spoke to the engine, so an idle one can have its scope
	// re-established rather than assumed. Zero until its first statement.
	lastActivity time.Time
	// Set when the connection is known to be in a state the driver cannot vouch for, so
	// the pool discards it instead of handing it to someone else.
	bad    bool
	closed bool
}

func newConn(cfg *config) *conn {
	return &conn{
		cfg:        cfg,
		client:     &http.Client{Timeout: cfg.timeout},
		autoCommit: true,
		pendingUse: cfg.useStatements(),
	}
}

// quoteIdent quotes an identifier unless it is already a plain upper-case name, matching
// Frostlake's folding rules (an unquoted identifier folds to upper case). Embedded quotes are
// doubled, so a name arriving from a DSN cannot break out of the quoting.
func quoteIdent(name string) string {
	if isPlainUpperIdent(name) {
		return name
	}
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// isPlainUpperIdent reports whether name survives unquoted: upper-case letters, digits,
// underscore and $, never leading with a digit or $.
func isPlainUpperIdent(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'A' && c <= 'Z', c == '_':
		case c >= '0' && c <= '9', c == '$':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return true
}

// The wire format of POST /api/execute.
type sqlRequest struct {
	SQL        string `json:"sql"`
	SessionID  string `json:"sessionId,omitempty"`
	AutoCommit bool   `json:"autoCommit"`
}

type sqlResponse struct {
	Success      bool   `json:"success"`
	SessionID    string `json:"sessionId"`
	ErrorMessage string `json:"errorMessage"`
	// Set when the endpoint rejects the request itself rather than running it — those
	// answers carry {"error": …} instead of a statement response.
	Error      string          `json:"error"`
	ResultSets []resultSetData `json:"resultSets"`
}

// The wire format of GET /api/health.
type healthResponse struct {
	Status string `json:"status"`
}

type resultSetData struct {
	Columns  []columnData        `json:"columns"`
	Rows     [][]json.RawMessage `json:"rows"`
	RowCount int                 `json:"rowCount"`
}

type columnData struct {
	Name      string `json:"name"`
	DataType  string `json:"dataType"`
	Nullable  bool   `json:"nullable"`
	Precision int    `json:"precision"`
	Scale     int    `json:"scale"`
}

func (c *conn) execute(ctx context.Context, query string) (*sqlResponse, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	// Each USE is dropped from the queue only once it has succeeded. A DSN naming a database
	// that does not exist has to keep failing; the alternative is later statements quietly
	// running in the default scope.
	for len(c.pendingUse) > 0 {
		if _, err := c.roundTrip(ctx, c.pendingUse[0]); err != nil {
			return nil, err
		}
		c.pendingUse = c.pendingUse[1:]
	}
	resp, err := c.roundTrip(ctx, query)
	if err != nil {
		return nil, err
	}
	c.lastActivity = time.Now()
	if changesSessionScope(query) {
		c.sessionDirty = true
	}
	return resp, nil
}

func (c *conn) roundTrip(ctx context.Context, query string) (*sqlResponse, error) {
	body, err := json.Marshal(sqlRequest{SQL: query, SessionID: c.sessionID, AutoCommit: c.autoCommit})
	if err != nil {
		return nil, err
	}
	endpoint := c.cfg.baseURL + "/api/execute"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.client.Do(req)
	if err != nil {
		c.retireUnless(err)
		return nil, &TransportError{Endpoint: endpoint, Err: err}
	}
	defer resp.Body.Close()
	// Keep a bounded head of the answer so a body that is not a Frostlake response can be
	// quoted back, then decode from the head plus the rest — a large result set still streams.
	head := make([]byte, maxErrorBody)
	n, _ := io.ReadFull(resp.Body, head)
	head = head[:n]
	var out sqlResponse
	if err := json.NewDecoder(io.MultiReader(bytes.NewReader(head), resp.Body)).Decode(&out); err != nil {
		// A proxy error page, the wrong port, a crashed server. Report what came back rather
		// than where the JSON parser gave up, and do not keep a connection whose far side we
		// cannot identify.
		c.bad = true
		return nil, &TransportError{Endpoint: endpoint, Status: resp.StatusCode, Body: snippet(head)}
	}
	if out.SessionID != "" {
		c.sessionID = out.SessionID
	}
	if !out.Success {
		return nil, &Error{
			Message:   failureMessage(&out, resp.StatusCode, head),
			Statement: query,
			Status:    resp.StatusCode,
		}
	}
	return &out, nil
}

// retireUnless marks the connection unusable after a transport failure, so the pool discards
// it rather than handing on a socket that just broke — and with it an engine session that may
// no longer exist behind it.
//
// It deliberately stops short of returning driver.ErrBadConn for the statement. That would
// tell database/sql the statement never ran and let it re-run on another connection, which is
// only safe when the request provably never left; a failure after the request was written
// would silently duplicate an INSERT. Two measurements say the trade is not worth taking even
// where it would be safe: net/http already replaces a stale pooled socket by itself, and
// answering ErrBadConn turns a caller's "connection refused: dial tcp …" into a bare
// "driver: bad connection".
//
// A cancelled context is not a broken connection — the caller changed their mind, the socket
// and the session are both fine — so that case keeps the connection.
func (c *conn) retireUnless(err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return
	}
	c.bad = true
}

// failureMessage never returns the empty string. A response can report failure carrying no
// message at all — an empty statement is refused with a differently shaped body — and an error
// that prints as nothing tells the caller less than the status code would.
func failureMessage(out *sqlResponse, status int, head []byte) string {
	if out.ErrorMessage != "" {
		return out.ErrorMessage
	}
	if out.Error != "" {
		return "frostlake: " + out.Error
	}
	return fmt.Sprintf("frostlake: statement failed with HTTP %d and no error message: %s",
		status, snippet(head))
}

func snippet(payload []byte) string {
	text := strings.TrimSpace(string(payload))
	if text == "" {
		return "(empty body)"
	}
	if len(text) > maxErrorBody {
		return strings.ToValidUTF8(text[:maxErrorBody], "") + "…"
	}
	return text
}

// changesSessionScope reports whether a request can move the session off the scope the DSN
// established.
//
// A request may hold more than one statement, and a USE riding behind a leading SELECT moves
// the scope just as surely as one standing alone, so every statement is examined rather than
// only the first.
func changesSessionScope(query string) bool {
	for _, statement := range splitStatements(query) {
		if statementChangesScope(statement) {
			return true
		}
	}
	return false
}

// statementChangesScope decides for a single statement. Only USE, the SET family, ALTER
// SESSION, and CREATE or DROP of a DATABASE or SCHEMA move the session — CREATE TABLE and its
// kind leave the scope exactly where it was, and counting them retired a connection for every
// DDL statement a caller ran.
func statementChangesScope(statement string) bool {
	words := leadingWords(statement, 6)
	if len(words) == 0 {
		return false
	}
	switch words[0] {
	case "USE", "SET", "UNSET":
		return true
	case "ALTER":
		return namesObject(words[1:], "SESSION")
	case "CREATE", "DROP":
		return namesObject(words[1:], "DATABASE", "SCHEMA")
	}
	return false
}

// namesObject walks the words between CREATE/DROP/ALTER and the object being named, stepping
// over the modifiers that may sit between them — CREATE OR REPLACE DATABASE, DROP SCHEMA IF
// EXISTS — and reports whether the object is one of want.
func namesObject(words []string, want ...string) bool {
	for _, word := range words {
		switch word {
		case "OR", "REPLACE", "TRANSIENT", "TEMPORARY", "TEMP", "VOLATILE",
			"LOCAL", "GLOBAL", "SECURE", "IF", "NOT", "EXISTS":
			continue
		}
		for _, candidate := range want {
			if word == candidate {
				return true
			}
		}
		return false
	}
	return false
}

// splitStatements splits a request on its top-level semicolons, leaving alone any that sit
// inside a string literal, a quoted identifier, a dollar-quoted body or a comment. A
// procedural block is split along with everything else, which only makes the scope check
// more willing to flag — the safe direction.
func splitStatements(query string) []string {
	var out []string
	start := 0
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
					i = len(query)
				} else {
					i = i + 2 + j + 2 - 1
				}
			}
		case ';':
			out = append(out, query[start:i])
			start = i + 1
		}
	}
	return append(out, query[start:])
}

// leadingWords returns up to n words from the start of a statement, upper-cased, skipping
// whitespace and comments and stopping at the first thing that is not a word.
func leadingWords(statement string, n int) []string {
	var out []string
	i := 0
	for i < len(statement) && len(out) < n {
		switch {
		case statement[i] == ' ', statement[i] == '\t', statement[i] == '\n', statement[i] == '\r':
			i++
		case strings.HasPrefix(statement[i:], "--"), strings.HasPrefix(statement[i:], "//"):
			i = skipLine(statement, i)
		case strings.HasPrefix(statement[i:], "/*"):
			j := strings.Index(statement[i+2:], "*/")
			if j < 0 {
				return out
			}
			i = i + 2 + j + 2
		case isWordByte(statement[i]):
			start := i
			for i < len(statement) && isWordByte(statement[i]) {
				i++
			}
			out = append(out, strings.ToUpper(statement[start:i]))
		default:
			return out
		}
	}
	return out
}

func isWordByte(c byte) bool {
	return c == '_' || c == '$' ||
		(c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}

// Prepare implements driver.Conn. Statements are prepared client-side.
func (c *conn) Prepare(query string) (driver.Stmt, error) {
	return newStmt(c, query), nil
}

// PrepareContext implements driver.ConnPrepareContext. Preparation is client-side and needs
// no round trip, so the context only has to be honoured if it is already done.
func (c *conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return newStmt(c, query), nil
}

func (c *conn) Close() error {
	c.closed = true
	return nil
}

// ResetSession implements driver.SessionResetter. A pooled connection carries its engine
// session with it, so a statement that changed the session's scope would otherwise leak into
// whichever query picks the connection up next. Put the DSN's scope back before reuse; with no
// DSN scope to restore, retire the connection so the next one starts from the server default.
func (c *conn) ResetSession(_ context.Context) error {
	if c.closed || c.bad {
		return driver.ErrBadConn
	}
	c.autoCommit = true
	if !c.sessionDirty && !c.sessionMayHaveLapsed() {
		return nil
	}
	c.sessionDirty = false
	use := c.cfg.useStatements()
	if len(use) == 0 {
		return driver.ErrBadConn
	}
	// Applied lazily, so a connection that is never used again costs no round trip.
	c.pendingUse = use
	return nil
}

// IsValid implements driver.Validator. A connection the driver cannot vouch for — one whose
// transaction ended in a state it had to guess at — is dropped rather than handed on.
func (c *conn) IsValid() bool {
	return !c.closed && !c.bad
}

// scopeRefreshAfter is how long a connection may sit idle before the driver stops trusting
// that its engine session still exists. The engine reclaims idle sessions and then re-creates
// one under the very same id at the server's default scope, which a client cannot tell apart
// from its own session surviving — so an idle connection has its scope re-established rather
// than assumed. Comfortably under the engine's 30-minute default.
const scopeRefreshAfter = 5 * time.Minute

func (c *conn) sessionMayHaveLapsed() bool {
	return !c.lastActivity.IsZero() && time.Since(c.lastActivity) > scopeRefreshAfter
}

// Begin implements driver.Conn (transactions ride the session's autoCommit flag).
func (c *conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

func (c *conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// Accepting an option the engine cannot honour would be a quiet lie about isolation.
	if opts.ReadOnly {
		return nil, errors.New("frostlake: read-only transactions are not supported")
	}
	switch sql.IsolationLevel(opts.Isolation) {
	case sql.LevelDefault, sql.LevelReadCommitted:
	default:
		return nil, fmt.Errorf("frostlake: isolation level %s is not supported; the engine offers read committed",
			sql.IsolationLevel(opts.Isolation))
	}
	c.autoCommit = false
	if _, err := c.execute(ctx, "BEGIN"); err != nil {
		c.autoCommit = true
		return nil, err
	}
	return &tx{conn: c, ctx: ctx}, nil
}

// ExecContext implements driver.ExecerContext.
func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	rendered, err := substitute(query, args, c.cfg.loc)
	if err != nil {
		return nil, err
	}
	resp, err := c.execute(ctx, rendered)
	if err != nil {
		return nil, err
	}
	return resultOf(resp), nil
}

// QueryContext implements driver.QueryerContext.
func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rendered, err := substitute(query, args, c.cfg.loc)
	if err != nil {
		return nil, err
	}
	resp, err := c.execute(ctx, rendered)
	if err != nil {
		return nil, err
	}
	return rowsOf(resp, c.cfg.loc), nil
}

// Ping implements driver.Pinger via the server's health endpoint.
func (c *conn) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.cfg.baseURL+"/api/health", nil)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return driver.ErrBadConn
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return driver.ErrBadConn
	}
	// A 200 on its own only says something is listening. Anything can serve that; the health
	// payload is what says it is a Frostlake engine, and a wrong address should be reported
	// rather than retried on another connection.
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxErrorBody))
	if err != nil {
		return driver.ErrBadConn
	}
	var health healthResponse
	if err := json.Unmarshal(body, &health); err != nil || health.Status == "" {
		return &TransportError{
			Endpoint: c.cfg.baseURL + "/api/health",
			Status:   resp.StatusCode,
			Body:     snippet(body),
		}
	}
	return nil
}

type tx struct {
	conn *conn
	ctx  context.Context
}

func (t *tx) Commit() error {
	_, err := t.conn.execute(t.context(), "COMMIT")
	t.conn.autoCommit = true
	if err != nil {
		// The COMMIT may never have reached the engine, which would then still be holding
		// the transaction open with no one left to end it. Roll back on a context that
		// cannot already be cancelled, and retire the connection either way rather than
		// returning it to the pool in a state the driver has guessed at.
		t.conn.execute(context.Background(), "ROLLBACK")
		t.conn.bad = true
	}
	return err
}

// Rollback deliberately runs on a fresh context. database/sql rolls back when the
// transaction's own context is cancelled, and reusing that cancelled context would fail
// immediately — leaving the transaction open on the server rather than ending it.
func (t *tx) Rollback() error {
	_, err := t.conn.execute(context.Background(), "ROLLBACK")
	t.conn.autoCommit = true
	return err
}

func (t *tx) context() context.Context {
	if t.ctx == nil {
		return context.Background()
	}
	return t.ctx
}
