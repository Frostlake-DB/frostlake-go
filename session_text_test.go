package frostlake

import "testing"

// The statement classifiers that decide whether a lost session is replaced or reported.

func TestTouchesSession(t *testing.T) {
	touching := []string{
		"USE DATABASE x", "use schema y", "USE ROLE r", "SET v = 1", "UNSET v",
		"ALTER SESSION SET TIMEZONE = 'UTC'", "alter session unset timezone",
		"CREATE DATABASE d", "CREATE OR REPLACE DATABASE d", "CREATE TRANSIENT DATABASE d",
		"DROP DATABASE IF EXISTS d", "CREATE SCHEMA IF NOT EXISTS s", "DROP SCHEMA s",
		// A temporary object lives exactly as long as its session.
		"CREATE TEMPORARY TABLE t (a INT)", "CREATE OR REPLACE TEMP TABLE t (a INT)",
		"CREATE VOLATILE TABLE t (a INT)", "CREATE TEMPORARY STAGE st",
		"CREATE OR REPLACE TEMPORARY VIEW v AS SELECT 1",
		"  -- comment\n  USE DATABASE x", "/* c */ SET v = 1",
	}
	for _, s := range touching {
		if !touchesSession(s) {
			t.Errorf("touchesSession(%q) = false, want true", s)
		}
	}
	untouched := []string{
		"SELECT 1", "INSERT INTO t VALUES (1)", "UPDATE t SET a = 1", "DELETE FROM t",
		"CREATE TABLE t (a INT)", "CREATE OR REPLACE TABLE t (a INT)", "DROP TABLE t",
		"CREATE VIEW v AS SELECT 1", "ALTER TABLE t ADD COLUMN b INT", "DROP TEMPORARY TABLE t",
		"CREATE OR REPLACE FUNCTION f() RETURNS INT AS $$ 1 $$", "BEGIN", "COMMIT", "ROLLBACK",
		"SELECT 'USE DATABASE x'", "", "   ",
	}
	for _, s := range untouched {
		if touchesSession(s) {
			t.Errorf("touchesSession(%q) = true, want false", s)
		}
	}
}

func TestTransactionEffect(t *testing.T) {
	cases := map[string]txEffect{
		"BEGIN":                         txBegins,
		"begin transaction":             txBegins,
		"BEGIN WORK":                    txBegins,
		"BEGIN NAME t1":                 txBegins,
		"START TRANSACTION":             txBegins,
		"  -- open\n BEGIN":             txBegins,
		"COMMIT":                        txEnds,
		"commit work":                   txEnds,
		"ROLLBACK":                      txEnds,
		"ROLLBACK WORK":                 txEnds,
		"SELECT 1":                      txNone,
		"INSERT INTO t VALUES (1)":      txNone,
		"":                              txNone,
		"START":                         txNone,
		"BEGIN SELECT 1":                txNone,
		"BEGIN\n  LET x := 1":           txNone,
		"BEGIN\n  INSERT INTO t VALUES": txNone,
	}
	for statement, want := range cases {
		if got := transactionEffect(statement); got != want {
			t.Errorf("transactionEffect(%q) = %d, want %d", statement, got, want)
		}
	}
}
