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
	"net/url"
	"strings"
	"time"
)

// maxErrorBody bounds how much of a response is kept for error reporting. The body is not
// buffered beyond this — a result set streams straight into the decoder.
const maxErrorBody = 512

// closeBudget bounds the courtesy request that releases the engine session when a connection
// closes; a shorter DSN timeout bounds it further.
const closeBudget = 5 * time.Second

// sessionSupport is what the engine is known to offer for sessions.
type sessionSupport int

const (
	// sessionsUnknown: no answer has named a session yet.
	sessionsUnknown sessionSupport = iota
	// sessionsTracked: the engine reports newSession, and with it honours requireSession and
	// releases a session on DELETE /api/sessions/{id}.
	sessionsTracked
	// sessionsUntracked: an engine that predates newSession, and with it both of the above. Its
	// parser may refuse a field it does not know, so it is sent neither.
	sessionsUntracked
)

// What the SessionLostError the driver raises says went with the session.
const (
	lostTransaction = "frostlake: the engine no longer holds this connection's session (it expired, was released, or the server restarted), so its open transaction is gone; the statement did not run"
	lostContext     = "frostlake: the engine no longer holds this connection's session (it expired, was released, or the server restarted), and the context set up on it (USE, SET, ALTER SESSION or a temporary object) went with it, so the statement was not re-run; the next statement starts a fresh session on the connection's scope"
	lostFresh       = "frostlake: the engine refused a session it had just started; the statement did not run"
	lostTxStatement = "frostlake: this transaction went with the connection's session, which the engine no longer holds; the statement did not run, and the transaction can only be rolled back"
	lostCommit      = "frostlake: this transaction went with the connection's session, which the engine no longer holds, so nothing was committed"
)

// errSessionGone is the engine's 404 for a session it was required to resume and no longer
// holds: nothing ran. It never reaches a caller — recover and applyScope answer it.
var errSessionGone = errors.New("frostlake: the engine no longer holds the connection's session")

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
	// What the engine offers for sessions, settled by the first answer that names one.
	sessions sessionSupport
	// Set once a statement left context on the session that a fresh session on the DSN's scope
	// would not have — USE, SET or UNSET, ALTER SESSION, a temporary object, CREATE or DROP of a
	// DATABASE or SCHEMA — so a lost session is reported rather than replaced. Putting the DSN's
	// scope back clears it.
	holdsContext bool
	// Whether a transaction is open on the session: from BEGIN or START TRANSACTION until
	// COMMIT or ROLLBACK.
	inTransaction bool
	// Set when the session was lost under a transaction begun with BeginTx: the Tx refuses
	// every further statement until Commit or Rollback ends it.
	txLost bool
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
	SQL       string `json:"sql"`
	SessionID string `json:"sessionId,omitempty"`
	// RequireSession asks the engine to refuse (404) rather than start a fresh session under an
	// id it no longer holds. Sent only with a session id, to an engine known to offer it.
	RequireSession bool `json:"requireSession,omitempty"`
	AutoCommit     bool `json:"autoCommit"`
	// MultiStatementCount is how many statements this ONE request carries, or nil when it
	// declares none and the session's MULTI_STATEMENT_COUNT answers for it. Zero means any
	// number. See WithMultiStatement.
	MultiStatementCount *int `json:"multiStatementCount,omitempty"`
}

type sqlResponse struct {
	Success   bool   `json:"success"`
	SessionID string `json:"sessionId"`
	// NewSession says whether the request ran in a session started for it. An engine that
	// predates the field sends none — and has neither requireSession nor DELETE /api/sessions.
	NewSession   *bool  `json:"newSession"`
	ErrorMessage string `json:"errorMessage"`
	// Set when the endpoint rejects the request itself rather than running it — those
	// answers carry {"error": …} instead of a statement response.
	Error      string          `json:"error"`
	ResultSets []resultSetData `json:"resultSets"`
	// replaced is set when the engine ran the request in a fresh session in place of the one
	// that was sent, which only an engine not asked to require its session does.
	replaced bool
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
	// Length is a text column's length in characters or a binary column's in bytes. Only those
	// two families carry it, and a server that predates the field sends none.
	Length *int64 `json:"length"`
}

func (c *conn) execute(ctx context.Context, query string) (*sqlResponse, error) {
	if c.closed {
		return nil, driver.ErrBadConn
	}
	if c.txLost {
		return nil, &SessionLostError{Message: lostTxStatement, Statement: query}
	}
	if err := c.applyScope(ctx, query); err != nil {
		return nil, err
	}
	statements := multiStatementCount(ctx)
	resp, err := c.roundTrip(ctx, query, statements)
	if errors.Is(err, errSessionGone) {
		resp, err = c.recover(ctx, query, statements)
	}
	if err != nil {
		return nil, err
	}
	c.lastActivity = time.Now()
	if changesSessionScope(query) {
		c.sessionDirty = true
	}
	c.track(query)
	return resp, nil
}

// applyScope runs the DSN's queued USE statements ahead of the caller's statement, query. Each is
// dropped from the queue only once it has succeeded. A DSN naming a database that does not exist
// has to keep failing; the alternative is later statements quietly running in the default scope.
//
// A session found gone while its scope goes back on is replaced by a fresh one, which takes the
// whole scope from its first statement: nothing ran, and the scope is exactly what a fresh
// session needs. Only a transaction open on the lost session stands in the way, and is reported.
func (c *conn) applyScope(ctx context.Context, query string) error {
	restarted := false
	for len(c.pendingUse) > 0 {
		// The queued USE statements are the driver's own, one statement each, so they never
		// carry the count the caller declared for their query.
		resp, err := c.roundTrip(ctx, c.pendingUse[0], nil)
		if errors.Is(err, errSessionGone) {
			// Putting the scope back resets the session's context anyway.
			c.holdsContext = false
			if err := c.refuseLost(query); err != nil {
				return err
			}
			if restarted {
				return &SessionLostError{Message: lostFresh, Statement: query}
			}
			restarted = true
			continue
		}
		if err != nil {
			return err
		}
		if resp.replaced {
			// A fresh session took over part-way through, and absorb queued the whole scope.
			continue
		}
		c.pendingUse = c.pendingUse[1:]
	}
	return nil
}

// recover answers the engine's refusal of a session it no longer holds — it expired, was
// released, or the server restarted — and nothing ran. With a transaction or context gone with
// the session, re-running would put the statement somewhere its author did not intend, so that
// is reported; otherwise a fresh session on the DSN's scope takes over and the statement is sent
// once more.
func (c *conn) recover(ctx context.Context, query string, statements *int) (*sqlResponse, error) {
	if err := c.refuseLost(query); err != nil {
		return nil, err
	}
	if err := c.applyScope(ctx, query); err != nil {
		return nil, err
	}
	resp, err := c.roundTrip(ctx, query, statements)
	if errors.Is(err, errSessionGone) {
		c.dropSession()
		return nil, &SessionLostError{Message: lostFresh, Statement: query}
	}
	return resp, err
}

// refuseLost drops a session the engine no longer holds and reports the loss when the session
// held a transaction or context that query may depend on. nil means a fresh session may take
// over.
func (c *conn) refuseLost(query string) error {
	hadTransaction := c.inTransaction
	hadContext := c.holdsContext
	c.dropSession()
	if hadTransaction {
		// A transaction begun with BeginTx stays refused until its Tx ends it: a statement run
		// now would land in a fresh session, outside anything the Tx later commits or rolls back.
		c.txLost = !c.autoCommit
		return &SessionLostError{Message: lostTransaction, Statement: query}
	}
	if hadContext {
		return &SessionLostError{Message: lostContext, Statement: query}
	}
	return nil
}

// dropSession forgets a session the engine no longer holds, with everything tracked about it,
// and queues the DSN's scope for the fresh session the next statement starts.
func (c *conn) dropSession() {
	c.sessionID = ""
	c.pendingUse = c.cfg.useStatements()
	c.sessionDirty = false
	c.holdsContext = false
	c.inTransaction = false
}

// absorb learns from an answer that names a session. Whether it carries newSession settles what
// the engine offers. A fresh session started in place of the one that was sent — which only
// happens while requireSession is not sent — means whatever the old one held is gone, and the
// DSN's scope goes back on before the next statement. It reports whether that happened.
func (c *conn) absorb(newSession *bool, sentID bool) bool {
	if newSession == nil {
		if c.sessions == sessionsUnknown {
			c.sessions = sessionsUntracked
		}
		return false
	}
	c.sessions = sessionsTracked
	if !*newSession || !sentID {
		return false
	}
	c.pendingUse = c.cfg.useStatements()
	c.sessionDirty = false
	c.holdsContext = false
	c.inTransaction = false
	return true
}

// track updates what the session holds from the text of a request that succeeded. Every
// statement in it counts: a USE riding behind a leading SELECT moves the session just the same.
func (c *conn) track(query string) {
	for _, statement := range splitStatements(query) {
		if touchesSession(statement) {
			c.holdsContext = true
		}
		switch transactionEffect(statement) {
		case txBegins:
			c.inTransaction = true
		case txEnds:
			c.inTransaction = false
		}
	}
}

func (c *conn) roundTrip(ctx context.Context, query string, statements *int) (*sqlResponse, error) {
	sentID := c.sessionID
	// Resume this session or refuse: without requireSession the engine starts a fresh session
	// under the same id when the old one has gone, and the statement runs in the wrong context.
	// Only an engine known to offer it is asked, since an older one may refuse a field it does
	// not know.
	require := sentID != "" && c.sessions == sessionsTracked
	body, err := json.Marshal(sqlRequest{SQL: query, SessionID: sentID, RequireSession: require,
		AutoCommit: c.autoCommit, MultiStatementCount: statements})
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
	if resp.StatusCode == http.StatusNotFound && require && !out.Success && out.SessionID == "" {
		return nil, errSessionGone
	}
	if out.SessionID != "" {
		c.sessionID = out.SessionID
		out.replaced = c.absorb(out.NewSession, sentID != "")
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

// sessionModifiers are the words that may sit between CREATE, DROP or ALTER and the kind of
// object being named.
var sessionModifiers = map[string]bool{
	"OR": true, "REPLACE": true, "TRANSIENT": true, "TEMPORARY": true, "TEMP": true,
	"VOLATILE": true, "LOCAL": true, "GLOBAL": true, "SECURE": true, "IF": true, "NOT": true,
	"EXISTS": true, "PUBLIC": true, "PRIVATE": true, "ICEBERG": true, "DYNAMIC": true,
	"HYBRID": true, "EVENT": true, "RECURSIVE": true, "MATERIALIZED": true, "EXTERNAL": true,
}

// touchesSession reports whether a statement leaves context behind that a fresh session on the
// DSN's scope would not have: a moved scope (USE, CREATE or DROP of a DATABASE or SCHEMA), a
// session variable or setting (SET, UNSET, ALTER SESSION), or a temporary object. A lost session
// that held any of it is reported rather than replaced. It is wider than statementChangesScope,
// which decides only whether the pool has to put the scope back: a temporary table does not move
// the scope, but a statement re-run without it would read something else.
func touchesSession(statement string) bool {
	words := leadingWords(statement, 16)
	if len(words) == 0 {
		return false
	}
	verb, rest := words[0], words[1:]
	switch verb {
	case "USE", "SET", "UNSET":
		return true
	case "ALTER", "CREATE", "DROP":
		i := 0
		temporary := false
		for i < len(rest) && sessionModifiers[rest[i]] {
			switch rest[i] {
			case "TEMPORARY", "TEMP", "VOLATILE":
				temporary = true
			}
			i++
		}
		if verb == "ALTER" {
			return i < len(rest) && rest[i] == "SESSION"
		}
		if i < len(rest) && (rest[i] == "DATABASE" || rest[i] == "SCHEMA") {
			return true
		}
		return verb == "CREATE" && temporary
	}
	return false
}

// txEffect is what a statement does to the session's transaction.
type txEffect int

const (
	txNone txEffect = iota
	txBegins
	txEnds
)

// transactionEffect reads a statement's effect on the session's transaction. BEGIN on its own, or
// with TRANSACTION, WORK or NAME, opens one, as does START TRANSACTION; BEGIN followed by a
// statement opens a scripting block instead. COMMIT and ROLLBACK end one.
func transactionEffect(statement string) txEffect {
	words := leadingWords(statement, 2)
	switch {
	case len(words) == 1 && words[0] == "BEGIN":
		return txBegins
	case len(words) == 2 && words[0] == "BEGIN" &&
		(words[1] == "TRANSACTION" || words[1] == "WORK" || words[1] == "NAME"):
		return txBegins
	case len(words) == 2 && words[0] == "START" && words[1] == "TRANSACTION":
		return txBegins
	case len(words) > 0 && (words[0] == "COMMIT" || words[0] == "ROLLBACK"):
		return txEnds
	}
	return txNone
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

// Close releases the engine session with DELETE /api/sessions/{id}, which also rolls back a
// transaction the session left open. Releasing is a courtesy, never an error: it is bounded by
// the shorter of the DSN's timeout and closeBudget, and the engine reclaims an idle session by
// itself when the request fails. An engine that predates newSession has no such endpoint and is
// sent nothing, and neither is anything sent by a second Close.
func (c *conn) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	c.release()
	return nil
}

// release ends the engine session, when there is one and the engine is known to release it.
func (c *conn) release() {
	id := c.sessionID
	c.sessionID = ""
	c.inTransaction = false
	if id == "" || c.sessions != sessionsTracked {
		return
	}
	budget := closeBudget
	if c.cfg.timeout > 0 && c.cfg.timeout < budget {
		budget = c.cfg.timeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete,
		c.cfg.baseURL+"/api/sessions/"+url.PathEscape(id), nil)
	if err != nil {
		return
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return
	}
	// Drained, so the socket can go back to the transport the open connections share.
	io.Copy(io.Discard, io.LimitReader(resp.Body, maxErrorBody))
	resp.Body.Close()
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
	// A transaction lost with the session is over once its Tx has ended, which database/sql
	// sees to before the connection comes back.
	c.txLost = false
	if !c.sessionDirty && !c.sessionMayHaveLapsed() {
		return nil
	}
	c.sessionDirty = false
	use := c.cfg.useStatements()
	if len(use) == 0 {
		return driver.ErrBadConn
	}
	// Applied lazily, so a connection that is never used again costs no round trip. The next
	// borrower starts from the DSN's scope, as a fresh session would, so a session lost after
	// this is replaced rather than reported, whatever an earlier borrower had set up on it.
	c.pendingUse = use
	c.holdsContext = false
	return nil
}

// IsValid implements driver.Validator. A connection the driver cannot vouch for — one whose
// transaction ended in a state it had to guess at — is dropped rather than handed on.
//
// So is one returned while a transaction is open on its session. database/sql ends every Tx before
// the connection comes back, so that transaction was opened by a statement — a BEGIN run through
// the pool, or left open on a sql.Conn — and belongs to no caller the pool could hand the
// connection to: the next borrower's statements would join it, and be rolled back unseen with it.
// database/sql discards the connection as it comes back, and Close releases the session, which
// rolls the transaction back at once rather than leaving it open until the connection is next
// borrowed.
func (c *conn) IsValid() bool {
	return !c.closed && !c.bad && !c.inTransaction
}

// scopeRefreshAfter is how long a connection may sit idle before the driver stops trusting
// that its engine session still exists. An engine that predates newSession reclaims idle
// sessions and then re-creates one under the very same id at the server's default scope, which
// a client cannot tell apart from its own session surviving — so an idle connection to such an
// engine has its scope re-established rather than assumed. Comfortably under the engine's
// 30-minute default.
const scopeRefreshAfter = 5 * time.Minute

// sessionMayHaveLapsed reports whether an idle connection's session may have been reclaimed
// without the driver being told. An engine that reports newSession never leaves it guessing: it
// refuses a session it no longer holds (requireSession), and the driver recovers from that.
func (c *conn) sessionMayHaveLapsed() bool {
	return c.sessions != sessionsTracked && !c.lastActivity.IsZero() &&
		time.Since(c.lastActivity) > scopeRefreshAfter
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
		// A failed BeginTx leaves no Tx behind to end a transaction lost with the session, so
		// nothing stays refused.
		c.autoCommit = true
		c.txLost = false
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
	if t.conn.txLost {
		// The transaction went with the session: there is nothing left to commit, and sending
		// COMMIT to a fresh session would report a success that never happened.
		t.conn.txLost = false
		t.conn.autoCommit = true
		return &SessionLostError{Message: lostCommit, Statement: "COMMIT"}
	}
	_, err := t.conn.execute(t.context(), "COMMIT")
	t.conn.autoCommit = true
	var lost *SessionLostError
	if errors.As(err, &lost) {
		// The COMMIT found the session gone and did not run; the engine discarded the
		// transaction with the session, so nothing is left open to roll back.
		t.conn.txLost = false
		return err
	}
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
	if t.conn.txLost {
		// The engine discarded the transaction with the session; nothing is left to roll back.
		t.conn.txLost = false
		t.conn.autoCommit = true
		return nil
	}
	_, err := t.conn.execute(context.Background(), "ROLLBACK")
	t.conn.autoCommit = true
	var lost *SessionLostError
	if errors.As(err, &lost) {
		// Found gone on the way: the transaction went with the session, which is all a
		// rollback asks for.
		t.conn.txLost = false
		return nil
	}
	return err
}

func (t *tx) context() context.Context {
	if t.ctx == nil {
		return context.Background()
	}
	return t.ctx
}
