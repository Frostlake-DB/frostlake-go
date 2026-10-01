# frostlake-go

A pure-Go `database/sql` driver for [Frostlake](https://frostlake.dev), speaking the
engine's HTTP protocol against a running `DatabaseHttpServer`. No JVM, no cgo — stdlib only.

## Engine version

Requires a Frostlake engine **0.2.0 or newer**. Ask a running server which one it is with
`SELECT CURRENT_VERSION()` — every release answers it, so the check works against any engine.

The driver versions independently of the engine: it speaks the HTTP protocol, not
the jar, so this is a floor rather than a lockstep pin.

## Usage

```go
import (
    "database/sql"
    _ "github.com/Frostlake-DB/frostlake-go"
)

db, err := sql.Open("frostlake", "frostlake://localhost:18082/MY_DB?schema=PUBLIC")
```

Or skip the registry and parse the DSN once:

```go
c, err := frostlake.NewConnector("frostlake://localhost:18082/MY_DB?schema=PUBLIC")
db := sql.OpenDB(c)
```

### DSN

```
frostlake://host:port[/DATABASE][?param=value&…]
```

| Parameter | Meaning | Default |
| --- | --- | --- |
| `schema` | schema to `USE` on every new session | — |
| `role` | role to `USE` on every new session | — |
| `warehouse` | warehouse to `USE` on every new session | — |
| `timeout` | per-request timeout, as a Go duration; `0` disables it | `5m` |
| `loc` | IANA zone the engine's zoneless timestamps belong to | `UTC` |
| `tls` | `true` to speak HTTPS — an `https://` DSN does the same | `false` |

The role, warehouse, database and schema are applied as `USE` statements on the session
before its first statement. An unknown parameter is an error rather than a silent no-op, and
so is a username or password — the engine's HTTP API has no authentication to hand them to.

## Semantics

- **Parameters** are inlined client-side (the protocol has no server-side binding), with the
  same rules as Frostlake's JDBC driver: strings escape backslashes and quotes, `[]byte`
  binds as a hex `BINARY` literal, `time.Time` as a `TIMESTAMP_NTZ` literal. A `?` inside a
  string literal, quoted identifier, `$$…$$` body or comment is never a placeholder. The
  argument count has to match the placeholder count — a placeholder left without an argument
  is an error, never a silently bound `NULL`. A call with no arguments binds nothing, so its
  statement is sent exactly as written: `v:field` paths, a scripting block's `:variable` and a
  cursor's `?` bound by `OPEN … USING` reach the engine untouched, and a `?` nothing binds is
  refused there with `Bind variable ? not set.`
- **Timestamps** bind in the DSN's `loc`, UTC unless you say otherwise. `TIMESTAMP_NTZ` stores
  a wall clock, so the zone a `time.Time` carries has to be pinned somewhere; rendering every
  value in one zone is what keeps two `time.Time`s naming the same instant from storing as two
  different timestamps, and it matches the zone they are read back in. A year past 9999 is
  written in full and one before the year 1 with its sign (`12000-…`, `-0001-…`), and the engine
  reads either as written.
- **Named parameters**: named arguments are `sql.Named` values and bind `:name` placeholders by
  name, so their order does not matter. A `::` cast, a `:=` assignment and a `:1` positional
  reference are never parameters, and neither is a semi-structured path: a colon glued to the
  end of an operand — `v:field`, `PARSE_JSON(?):a`, `arr[0]:b`, `"Col":a`, `$1:a`, `?:a` —
  reads a field, so a `:name` placeholder has to follow whitespace, an operator, a comma or an
  opening bracket (`WHERE id = :id`). Write a path without a space before its colon: named
  arguments take `v :field` for a placeholder, and so does a prepared statement's argument
  count. Positional arguments bind only the `?` placeholders and leave any `:name` to the
  engine, which reads one inside a scripting block as the block's own variable
  (`BEGIN LET n INTEGER := ?; RETURN :n; END;`). Named arguments bind only `:name`
  placeholders, and a statement that also holds a `?` is refused. A prepared statement expects
  one argument per `?`, or per distinct `:name`; a path never counts.
- **Types**: `DATE`, `TIME` and every `TIMESTAMP` variant scan as `time.Time`; `BINARY` as
  `[]byte`; integral `NUMBER` columns as `int64`, fractional and floating as `float64`;
  `BOOLEAN` as `bool`; VARIANT/OBJECT/ARRAY as their JSON text in a `string`. An integer too
  wide for `int64` — `NUMBER(38,0)` holds them — arrives as its exact digits in a `string`
  rather than a rounded `float64`. `TIMESTAMP_LTZ`/`_TZ` keep the offset the engine sends;
  the zoneless types are read in the DSN's `loc`. A year past 9999 arrives in full, as the
  account prints it (`12000-01-01`), and reads into `time.Time` unchanged, as does a signed year
  (`-1-01-15`, or java.time's `-0001-01-01` and `+20201-01-15`). A fraction of a second arrives
  whole: the engine sends three, six or nine digits, as many as the value needs, so a
  `time.Time` bound into a `TIMESTAMP_NTZ` or `TIME` column reads back to the nanosecond.
  Engines before 0.1.0 cut a timestamp to the millisecond and a `TIME` to the second in transit,
  and then that is what arrives.
- **Column metadata**: `sql.ColumnType` reports the database type name, nullability, decimal
  precision and scale for numeric columns, the length of text and binary columns (characters
  and bytes; engines before 0.1.0 do not send it, and then none is reported), and the Go type
  each column scans as.
- **Several statements in one request**, once they are asked for. As on the account, a request
  carries one statement unless it says otherwise, or `A; B` is refused with
  `Actual statement count 2 did not match the desired statement count 1.` Ask per call with
  `WithMultiStatement`, which is what the account's own Go driver calls it:

  ```go
  ctx := frostlake.WithMultiStatement(context.Background(), 2)
  rows, err := db.QueryContext(ctx, "SELECT 1; SELECT 2")
  ```

  `0` allows any number. The count rides on that one request: it does not change the session's
  `MULTI_STATEMENT_COUNT`, so nothing else sharing the connection is affected and there is nothing
  to restore afterwards. `ALTER SESSION SET MULTI_STATEMENT_COUNT = n` still works when a whole
  session should carry packs. Either way `A; B` answers with one result set each,
  `Rows.NextResultSet` walks them, and `RowsAffected` adds up the DML counts.
- **Errors** are typed. A failure the engine reported is a `*frostlake.Error` carrying the
  message, the statement as sent, and the HTTP status. A failure that never became an answer —
  refused connection, expired context, a proxy replying instead — is a `*frostlake.TransportError`,
  which unwraps to its cause. A statement that was not run because the engine no longer holds the
  session it depended on is a `*frostlake.SessionLostError` (see [Session lifetime](#session-lifetime)).

  ```go
  var ferr *frostlake.Error
  if errors.As(err, &ferr) {
      log.Printf("engine refused: %s", ferr.Message)
  }
  ```

  **`Error.Statement` holds the rendered SQL.** Because binding is client-side, that means
  every parameter inlined — a bound password or card number appears in it verbatim. The
  message from `Error()` carries none of it, so log `Message` freely and treat `Statement` as
  sensitive.
- **Transactions**: `Begin`/`Commit`/`Rollback` ride the session's autocommit flag plus
  `BEGIN`/`COMMIT`/`ROLLBACK` statements, matching the JDBC transport. The engine offers read
  committed, so `BeginTx` accepts the default and `sql.LevelReadCommitted` and refuses any
  other level, along with `ReadOnly` — an option it cannot honour is refused rather than
  ignored. `Commit` rides the transaction's context; `Rollback` deliberately does not, so a
  transaction whose context was cancelled can still be ended rather than left open.

  A transaction opened with a `BEGIN` or `START TRANSACTION` statement instead lives on one
  connection only while you hold that connection: run it on a `sql.Conn`, or in one call that
  also ends it (`BEGIN; …; COMMIT` with `WithMultiStatement`). The pool promises nothing about
  which connection your next `db.Exec` gets, so a connection that comes back to it with a
  transaction still open is not pooled again: `database/sql` discards it, and closing it releases
  the session (see [Session lifetime](#session-lifetime)), which rolls the transaction back at
  once. The next statement starts a fresh session outside it, rather than joining a transaction
  nobody will commit. Against an engine before 0.1.0 the discarded session keeps that
  transaction until its idle expiry, where nobody sees it.
- **A broken connection is retired, never retried.** When the transport fails — the host
  refuses, the socket dies, the answer is not a Frostlake response — the connection is marked
  unusable so the pool discards it, and the error reaches you with its diagnosis intact. The
  statement is *not* re-run on another connection: after a transport failure its fate is
  unknown, and re-running it would duplicate an INSERT. A cancelled context is not a broken
  connection and keeps the connection alive.
- **RowsAffected** is derived from the engine's one-cell DML result
  (`number of rows inserted` / `updated` / `deleted`). `LastInsertId` returns an error — the
  engine has no generated row ids, and a zero would read as a real one.
- **Sessions and the pool**: one HTTP session per `driver.Conn`, so `database/sql`'s pool maps
  pooled connections to engine sessions 1:1. A statement that moves the session's scope —
  `USE`, the `SET` family, `ALTER SESSION`, and `CREATE`/`DROP` of a `DATABASE` or `SCHEMA` —
  is therefore scoped to one connection, not to the pool. Every statement in a request is
  examined, so a `USE` riding behind a leading `SELECT` counts too. On its way back to the
  pool that connection is put back on the DSN's scope; if the DSN named none, it is retired
  instead, so the next query cannot inherit a scope it never asked for. Against an engine before
  0.1.0, a connection that has sat idle long enough that its engine session may have been
  reclaimed has its scope re-established rather than assumed.

  Name the scope in the DSN, or hold a single `sql.Conn`, rather than issuing `USE` against a
  `sql.DB`. Two routes still move the scope without the driver seeing it — `EXECUTE IMMEDIATE`
  of a `USE`, and a procedure that switches scope when `CALL`ed — so prefer the DSN when it
  matters.

### Session lifetime

An engine session lives until it is released or has sat idle for 30 minutes, and a restart
loses every one. From 0.1.0 the engine says so: each answer carries `newSession`, and a request
can ask it to refuse a session it no longer holds rather than quietly start a fresh one under the
same id. The driver learns which kind of engine it is talking to from the first answer that names
a session.

- **What is sent.** Once the engine has answered with `newSession`, every request naming the
  session carries `requireSession: true`. An engine before 0.1.0 is sent neither that field nor
  anything else it does not know.
- **After a lost session.** The engine refuses the request (HTTP 404) and nothing ran. The
  driver drops the session, then:
  - if a transaction was open on it — `BeginTx`, or a `BEGIN` / `START TRANSACTION` statement —
    the statement fails with `*frostlake.SessionLostError`: the transaction is gone and the
    statement did not run. A `BeginTx` transaction stays dead: its `Tx` refuses every further
    statement with the same error without sending it, `Commit` reports it, and `Rollback`
    succeeds without a round trip, since the engine discarded the transaction with the session;
  - else, if a statement on the session set up context that a fresh session would not have —
    `USE`, `SET`/`UNSET`, `ALTER SESSION`, a temporary object, `CREATE`/`DROP` of a `DATABASE` or
    `SCHEMA` — the statement fails with `*frostlake.SessionLostError` and is not re-run, since a
    fresh session would run it somewhere else;
  - otherwise the DSN's scope (`USE ROLE`/`WAREHOUSE`/`DATABASE`/`SCHEMA`) goes onto a fresh
    session and the statement is sent once more. A second refusal fails with
    `*frostlake.SessionLostError`.

  Every request is examined, so a `USE` riding behind a leading `SELECT` counts too. Putting a
  pooled connection back on the DSN's scope, on its way to the next borrower, clears what an
  earlier borrower set up, as a fresh session does. After the error the connection stays usable:
  its next statement starts a fresh session on the DSN's scope. The error is deliberately not
  `driver.ErrBadConn` — although nothing ran, that would have `database/sql` re-run the
  statement on another connection, in a fresh session without the transaction or context it
  depended on, which is the re-run the error exists to prevent; and it would close a pinned
  `sql.Conn` that is in fact fine. The connection is left clean — no session, the DSN's scope
  queued — so the pool keeps it, and the next borrower starts from the DSN's scope.

  ```go
  var lost *frostlake.SessionLostError
  if errors.As(err, &lost) {
      // redo the whole unit of work
  }
  ```

  Against an engine before 0.1.0 none of this can happen: a lost session is re-created under
  the same id at the server's default scope, and the idle re-scope above is the only guard.
- **Close.** Closing a connection sends `DELETE /api/sessions/{id}`, which releases the session
  and rolls back a transaction it left open, so `SetConnMaxIdleTime`, `SetMaxIdleConns` and
  `db.Close()` hand sessions back instead of leaving them to the 30-minute sweep. A connection
  returned to the pool with a transaction open is closed this way as it comes back (see
  **Transactions** above). It is a
  courtesy: bounded by the shorter of the DSN's `timeout` and five seconds, and whatever it
  meets — a 404, a 405, a closed socket, no answer — `Close` reports no error. A connection that
  never ran a statement, one closed a second time, and one talking to an engine before 0.1.0
  send nothing.

## Known limitations

- **Against an engine before 0.1.0, server sessions are not released on close.** Those engines
  have no endpoint for ending a session, so a closed connection's session lingers until the
  engine's own 30-minute idle sweep reclaims it, and connection churn accrues server-side
  sessions.
- **Against an engine before 0.1.0, a session idle past that sweep silently resumes at the
  server's default scope**, because such an engine re-creates an expired session under the same
  id — nothing in its answer tells a client its session was reclaimed. The driver covers this by
  re-establishing the DSN's scope on a connection that has been idle for more than five minutes,
  but anything else the session held (session variables, an `ALTER SESSION` setting) is gone.
  Keep `SetConnMaxIdleTime` below the engine's session timeout if connections may sit unused for
  long stretches.
- **A date before the year 1 can arrive as its year of the era.** The engine prints such a
  `DATE` or `TIMESTAMP` column value without a sign, as the account prints a date reached by date
  arithmetic — the year -1 as `0002` — so the driver can only read it as that year AD. `YEAR()`
  still answers the signed year, and a bound `time.Time` before the year 1 is stored as written.

## Tests

The unit tests need nothing installed — no engine, no JVM:

```bash
go test ./...
```

The integration tests additionally boot a real server from an engine classpath, and skip
themselves when `FROSTLAKE_CLASSPATH` is unset:

```bash
JAVA_HOME=~/.jdks/liberica-17.0.18 \
FROSTLAKE_CLASSPATH="<engine classes>:<dependency classpath>" \
go test ./...
```

## Testkit corpus runner

`cmd/testkit` replays the engine's testkit corpus — the language-neutral SQL suites in the
engine repository (`engine/src/test/resources/testkit/suites`, format in the `SCHEMA.md` beside
them) — through this driver, with the reference runner's reset, normalization and capability
rules. Name that testkit directory in `FL_CORPUS` and `go test` replays the corpus too, with the
engine set up as for the integration tests; without `FL_CORPUS` the replay is skipped:

```bash
FL_CORPUS=/path/to/frostlake/engine/src/test/resources/testkit \
FROSTLAKE_CLASSPATH="<engine classes>:<dependency classpath>" \
go test -count=1 ./...
```

Give `FL_CORPUS` as an absolute path: `go test` runs the replay in `cmd/testkit`, and reads a
relative one from there. `-count=1` keeps `go test` from answering with a cached pass, since the
engine is not part of what it caches. The runner also works on its own, from the checkout root:

```bash
FL_CORPUS=/path/to/frostlake/engine/src/test/resources/testkit \
FROSTLAKE_URL=frostlake://127.0.0.1:18082 go run ./cmd/testkit
```

| Variable | Meaning | Default |
| --- | --- | --- |
| `FL_CORPUS` | the testkit directory to replay, its suite files in `suites/*.json` | — (nothing is replayed) |
| `FROSTLAKE_URL` | the engine to attach to | — |
| `FROSTLAKE_CLASSPATH` | without `FROSTLAKE_URL`: boot an engine from this classpath, with `JAVA_HOME`'s `java` | — |
| `FROSTLAKE_TESTKIT_REPORT` | the TSV report, one row per case | `results/testkit-go.tsv` |
| `FROSTLAKE_TESTKIT_FILTER` | only the suites whose name contains one of these comma-separated words | all |

It prints `testkit [go]: <passed> passed, <failed> failed, <skipped> skipped` and exits 1 when a
case failed or errored, which fails `go test`. Cases skipped for `go` or `http` are SKIPs. Checks
the transport cannot make — an expected error's code or SQLSTATE — are listed in
`missing-apis-go.md` beside the report rather than failed. The corpus tracks the engine it lives
with, so replay it against that engine: an older one is expected to fail cases a newer corpus
holds.

## License

Apache-2.0 — see [LICENSE](LICENSE).
