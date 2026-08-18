# frostlake-go

A pure-Go `database/sql` driver for [Frostlake](https://frostlake.dev), speaking the
engine's HTTP protocol against a running `DatabaseHttpServer`. No JVM, no cgo — stdlib only.

## Engine version

Requires a Frostlake engine **0.0.7 or newer**. Ask a running server which one it is with
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
  is an error, never a silently bound `NULL`.
- **Timestamps** bind in the DSN's `loc`, UTC unless you say otherwise. `TIMESTAMP_NTZ` stores
  a wall clock, so the zone a `time.Time` carries has to be pinned somewhere; rendering every
  value in one zone is what keeps two `time.Time`s naming the same instant from storing as two
  different timestamps, and it matches the zone they are read back in.
- **Named parameters**: a statement may use positional `?` or named `:name` placeholders —
  one style per statement, mixing them is an error. Named arguments are `sql.Named` values and
  bind by name, so their order does not matter. A `::` cast, a `:=` assignment and a `:1`
  positional reference are never parameters.
- **Types**: `DATE`, `TIME` and every `TIMESTAMP` variant scan as `time.Time`; `BINARY` as
  `[]byte`; integral `NUMBER` columns as `int64`, fractional and floating as `float64`;
  `BOOLEAN` as `bool`; VARIANT/OBJECT/ARRAY as their JSON text in a `string`. An integer too
  wide for `int64` — `NUMBER(38,0)` holds them — arrives as its exact digits in a `string`
  rather than a rounded `float64`. `TIMESTAMP_LTZ`/`_TZ` keep the offset the engine sends;
  the zoneless types are read in the DSN's `loc`.
- **Column metadata**: `sql.ColumnType` reports the database type name, nullability, decimal
  precision and scale for numeric columns, and the Go type each column scans as.
- **Several statements in one request**: `A; B` answers with one result set each.
  `Rows.NextResultSet` walks them, and `RowsAffected` adds up the DML counts.
- **Errors** are typed. A failure the engine reported is a `*frostlake.Error` carrying the
  message, the statement as sent, and the HTTP status. A failure that never became an answer —
  refused connection, expired context, a proxy replying instead — is a `*frostlake.TransportError`,
  which unwraps to its cause.

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
  instead, so the next query cannot inherit a scope it never asked for. A connection that has
  sat idle long enough that its engine session may have been reclaimed has its scope
  re-established rather than assumed.

  Name the scope in the DSN, or hold a single `sql.Conn`, rather than issuing `USE` against a
  `sql.DB`. Two routes still move the scope without the driver seeing it — `EXECUTE IMMEDIATE`
  of a `USE`, and a procedure that switches scope when `CALL`ed — so prefer the DSN when it
  matters.

## Known limitations

- **Server sessions are not released on close.** The HTTP API has no endpoint for ending a
  session, so a closed connection's session lingers until the engine's own 30-minute idle
  sweep reclaims it. Connection churn therefore accrues server-side sessions.
- **A session idle past that sweep silently resumes at the server's default scope**, because
  the engine re-creates an expired session under the same id — nothing in the answer tells a
  client its session was reclaimed. The driver covers this by re-establishing the DSN's scope
  on a connection that has been idle for more than five minutes, but anything else the session
  held (session variables, an `ALTER SESSION` setting) is gone. Keep `SetConnMaxIdleTime`
  below the engine's session timeout if connections may sit unused for long stretches.
- **Timestamps lose sub-millisecond precision in transit.** The engine holds full nanoseconds,
  but the HTTP layer serialises milliseconds, so a `time.Time` round trip is
  millisecond-precise however fine the value bound.

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

## License

Apache-2.0 — see [LICENSE](LICENSE).
