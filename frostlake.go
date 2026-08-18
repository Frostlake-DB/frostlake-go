// Package frostlake provides a database/sql driver for Frostlake, speaking the
// engine's HTTP protocol (POST /api/execute against a running DatabaseHttpServer).
//
// DSN format:
//
//	frostlake://host:port[/DATABASE][?param=value&…]
//
// Example:
//
//	db, err := sql.Open("frostlake", "frostlake://localhost:18082/MY_DB?schema=PUBLIC")
//
// Parameters:
//
//	schema     schema to USE on every new session
//	role       role to USE on every new session
//	warehouse  warehouse to USE on every new session
//	timeout    per-request timeout as a Go duration (default 5m; 0 disables it)
//	loc        IANA zone the engine's zoneless timestamps belong to (default UTC)
//	tls        true to speak HTTPS; an https:// DSN does the same
//
// Statement parameters are inlined client-side (there is no server-side binding), exactly
// like Frostlake's JDBC driver: strings are quoted with backslash and quote escaping,
// []byte binds as a hex BINARY literal, time.Time as a TIMESTAMP_NTZ literal in loc.
// Both positional (?) and named (:name) placeholders are supported, one style per statement.
package frostlake

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// defaultTimeout bounds a single request. Queries can legitimately run for minutes, so it is
// generous rather than snappy; set timeout=0 in the DSN to remove it entirely.
const defaultTimeout = 5 * time.Minute

func init() {
	sql.Register("frostlake", &Driver{})
}

// Driver implements database/sql/driver.Driver for Frostlake.
type Driver struct{}

// Open opens a new connection to a Frostlake HTTP server.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return newConn(cfg), nil
}

type config struct {
	baseURL   string // http://host:port or https://host:port
	database  string
	schema    string
	role      string
	warehouse string
	timeout   time.Duration
	loc       *time.Location
}

// useStatements renders the DSN's scope as the USE statements a fresh session needs, in
// dependency order. Rebuilt on demand, so a pooled connection can be put back on this scope.
func (c *config) useStatements() []string {
	var out []string
	if c.role != "" {
		out = append(out, "USE ROLE "+quoteIdent(c.role))
	}
	if c.warehouse != "" {
		out = append(out, "USE WAREHOUSE "+quoteIdent(c.warehouse))
	}
	if c.database != "" {
		out = append(out, "USE DATABASE "+quoteIdent(c.database))
	}
	if c.schema != "" {
		out = append(out, "USE SCHEMA "+quoteIdent(c.schema))
	}
	return out
}

func parseDSN(dsn string) (*config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, fmt.Errorf("frostlake: invalid DSN %q: %w", dsn, err)
	}
	scheme := "http"
	switch u.Scheme {
	case "frostlake", "http":
	case "https":
		scheme = "https"
	default:
		return nil, errors.New("frostlake: DSN must start with frostlake://, http:// or https://")
	}
	if u.Host == "" {
		return nil, errors.New("frostlake: DSN is missing host[:port]")
	}
	// The engine's HTTP API has no authentication, so credentials in a DSN would be
	// silently discarded. Saying so beats pretending they were used.
	if u.User != nil {
		return nil, errors.New("frostlake: the DSN carries a username or password, which the engine's HTTP API does not accept")
	}
	cfg := &config{timeout: defaultTimeout, loc: time.UTC}
	if p := strings.Trim(u.Path, "/"); p != "" {
		cfg.database = p
	}
	for key, values := range u.Query() {
		value := ""
		if len(values) > 0 {
			value = values[len(values)-1]
		}
		switch strings.ToLower(key) {
		case "schema":
			cfg.schema = value
		case "role":
			cfg.role = value
		case "warehouse":
			cfg.warehouse = value
		case "timeout":
			d, err := time.ParseDuration(value)
			if err != nil {
				return nil, fmt.Errorf("frostlake: timeout %q is not a duration: %w", value, err)
			}
			if d < 0 {
				return nil, fmt.Errorf("frostlake: timeout %q is negative", value)
			}
			cfg.timeout = d
		case "loc":
			loc, err := time.LoadLocation(value)
			if err != nil {
				return nil, fmt.Errorf("frostlake: loc %q is not a known zone: %w", value, err)
			}
			cfg.loc = loc
		case "tls":
			on, err := strconv.ParseBool(value)
			if err != nil {
				return nil, fmt.Errorf("frostlake: tls %q is not a boolean: %w", value, err)
			}
			if on {
				scheme = "https"
			}
		default:
			return nil, fmt.Errorf("frostlake: unknown DSN parameter %q", key)
		}
	}
	cfg.baseURL = scheme + "://" + u.Host
	return cfg, nil
}
