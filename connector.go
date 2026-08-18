package frostlake

import (
	"context"
	"database/sql/driver"
)

// connector holds a DSN that has already been parsed, so opening a connection costs no
// parsing and a bad DSN is reported once rather than on every connection.
type connector struct {
	cfg *config
}

// NewConnector parses a DSN up front and returns a connector for sql.OpenDB, which skips
// both the driver registry and re-parsing the DSN per connection.
//
//	c, err := frostlake.NewConnector("frostlake://localhost:18082/MY_DB?schema=PUBLIC")
//	db := sql.OpenDB(c)
func NewConnector(dsn string) (driver.Connector, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	return &connector{cfg: cfg}, nil
}

func (c *connector) Connect(_ context.Context) (driver.Conn, error) {
	return newConn(c.cfg), nil
}

func (c *connector) Driver() driver.Driver { return &Driver{} }

// OpenConnector implements driver.DriverContext.
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	return NewConnector(dsn)
}
