package frostlake

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"math"
	"reflect"
	"testing"
	"time"
)

// Reading answers back: how a JSON cell becomes a Go value, what a column reports about
// itself, walking result sets, and the row counts a DML statement returns.

func TestIsIntegral(t *testing.T) {
	cases := []struct {
		col  columnData
		want bool
	}{
		{columnData{DataType: "NUMBER", Precision: 38, Scale: 0}, true},
		{columnData{DataType: "NUMBER", Precision: 12, Scale: 2}, false},
		{columnData{DataType: "NUMBER(12,2)"}, false},
		{columnData{DataType: "NUMBER(38,0)"}, true},
		{columnData{DataType: "DECIMAL", Scale: 4}, false},
		{columnData{DataType: "INTEGER"}, true},
		{columnData{DataType: "BIGINT"}, true},
		{columnData{DataType: "FLOAT"}, false},
		{columnData{DataType: "VARCHAR"}, false},
		{columnData{DataType: "TIMESTAMP_NTZ"}, false},
		{columnData{}, false},
	}
	for _, c := range cases {
		if got := isIntegral(c.col); got != c.want {
			t.Errorf("isIntegral(%+v) = %v, want %v", c.col, got, c.want)
		}
	}
}

func TestConvert(t *testing.T) {
	num := columnData{DataType: "NUMBER", Precision: 38, Scale: 0}
	dec := columnData{DataType: "NUMBER", Precision: 12, Scale: 2}
	str := columnData{DataType: "VARCHAR"}
	cases := []struct {
		raw  string
		col  columnData
		want driver.Value
	}{
		{"null", num, nil},
		{"", num, nil},
		{`"text"`, str, "text"},
		{`"a\"b"`, str, `a"b`},
		{"true", columnData{DataType: "BOOLEAN"}, true},
		{"false", columnData{DataType: "BOOLEAN"}, false},
		{"42", num, int64(42)},
		{"-42", num, int64(-42)},
		{"12.34", dec, 12.34},
		{"12", dec, 12.0},
		{"1.5e3", columnData{DataType: "FLOAT"}, 1500.0},
		{"9223372036854775807", num, int64(math.MaxInt64)},
		// Wider than int64: the digits are kept exactly rather than rounded into a float64.
		{"9223372036854775808", num, "9223372036854775808"},
		{"123456789012345678901234567890", num, "123456789012345678901234567890"},
		{`{"k":1}`, columnData{DataType: "VARIANT"}, `{"k":1}`},
	}
	for _, c := range cases {
		got, err := convert(json.RawMessage(c.raw), c.col, time.UTC)
		if err != nil {
			t.Errorf("convert(%q): %v", c.raw, err)
			continue
		}
		if got != c.want {
			t.Errorf("convert(%q, %s) = %#v (%T), want %#v", c.raw, c.col.DataType, got, got, c.want)
		}
	}
}

func TestConvertTemporal(t *testing.T) {
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("no zoneinfo: %v", err)
	}
	cases := []struct {
		raw  string
		col  columnData
		loc  *time.Location
		want time.Time
	}{
		{`"2024-03-05"`, columnData{DataType: "DATE"}, time.UTC,
			time.Date(2024, 3, 5, 0, 0, 0, 0, time.UTC)},
		{`"2024-03-05 06:07:08.123"`, columnData{DataType: "TIMESTAMP_NTZ"}, time.UTC,
			time.Date(2024, 3, 5, 6, 7, 8, 123000000, time.UTC)},
		{`"2024-03-05 06:07:08"`, columnData{DataType: "DATETIME"}, time.UTC,
			time.Date(2024, 3, 5, 6, 7, 8, 0, time.UTC)},
		// A zoneless value belongs to whatever zone the DSN named.
		{`"2024-03-05 06:07:08"`, columnData{DataType: "TIMESTAMP_NTZ"}, ny,
			time.Date(2024, 3, 5, 6, 7, 8, 0, ny)},
		{`"06:07:08"`, columnData{DataType: "TIME"}, time.UTC,
			time.Date(0, 1, 1, 6, 7, 8, 0, time.UTC)},
	}
	for _, c := range cases {
		v, err := convert(json.RawMessage(c.raw), c.col, c.loc)
		if err != nil {
			t.Errorf("convert(%s): %v", c.raw, err)
			continue
		}
		got, ok := v.(time.Time)
		if !ok {
			t.Errorf("convert(%s) = %T, want time.Time", c.raw, v)
			continue
		}
		if !got.Equal(c.want) {
			t.Errorf("convert(%s) = %s, want %s", c.raw, got, c.want)
		}
	}
}

// TIMESTAMP_LTZ and _TZ carry their own offset, which must win over the DSN's zone.
func TestConvertZonedTemporal(t *testing.T) {
	v, err := convert(json.RawMessage(`"2024-03-05 06:07:08.123 +0200"`),
		columnData{DataType: "TIMESTAMP_TZ"}, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := v.(time.Time)
	if !ok {
		t.Fatalf("convert = %T, want time.Time", v)
	}
	want := time.Date(2024, 3, 5, 4, 7, 8, 123000000, time.UTC)
	if !got.Equal(want) {
		t.Errorf("convert = %s (%s), want the instant %s", got, got.Location(), want)
	}
	if _, offset := got.Zone(); offset != 2*3600 {
		t.Errorf("offset = %d seconds, want 7200", offset)
	}
}

func TestConvertBinary(t *testing.T) {
	v, err := convert(json.RawMessage(`"DEADBEEF"`), columnData{DataType: "BINARY"}, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := v.([]byte)
	if !ok {
		t.Fatalf("convert = %T, want []byte", v)
	}
	want := []byte{0xde, 0xad, 0xbe, 0xef}
	if string(got) != string(want) {
		t.Errorf("convert = %x, want %x", got, want)
	}
	// Anything that is not hex is handed back as text rather than lost.
	v2, err := convert(json.RawMessage(`"not hex"`), columnData{DataType: "BINARY"}, time.UTC)
	if err != nil {
		t.Fatal(err)
	}
	if v2 != "not hex" {
		t.Errorf("non-hex BINARY = %#v, want the text", v2)
	}
}

// A cell too wide for int64 keeps its digits only where the column is integral. A fractional
// or floating column still yields a float64, so one column does not change Go type with the
// value it happens to hold.
func TestWideValueOnlyStringifiesOnIntegralColumns(t *testing.T) {
	const wide = "100000000000000000000"
	cases := []struct {
		col  columnData
		want driver.Value
	}{
		{columnData{DataType: "NUMBER", Precision: 38, Scale: 0}, wide},
		{columnData{DataType: "NUMBER", Precision: 20, Scale: 2}, 1e20},
		{columnData{DataType: "FLOAT"}, 1e20},
		{columnData{DataType: "VARIANT"}, 1e20},
	}
	for _, c := range cases {
		got, err := convert(json.RawMessage(wide), c.col, time.UTC)
		if err != nil {
			t.Errorf("convert on %s: %v", c.col.DataType, err)
			continue
		}
		if got != c.want {
			t.Errorf("convert on %s = %#v (%T), want %#v", c.col.DataType, got, got, c.want)
		}
	}
}

func TestScanTypeOf(t *testing.T) {
	cases := []struct {
		col  columnData
		want reflect.Type
	}{
		{columnData{DataType: "NUMBER", Scale: 0}, typeInt64},
		{columnData{DataType: "NUMBER", Scale: 2}, typeFloat64},
		{columnData{DataType: "FLOAT"}, typeFloat64},
		{columnData{DataType: "BOOLEAN"}, typeBool},
		{columnData{DataType: "VARCHAR"}, typeString},
		{columnData{DataType: "BINARY"}, typeBytes},
		{columnData{DataType: "DATE"}, typeTime},
		{columnData{DataType: "TIMESTAMP_LTZ"}, typeTime},
		{columnData{DataType: "VARIANT"}, typeString},
	}
	for _, c := range cases {
		if got := scanTypeOf(c.col); got != c.want {
			t.Errorf("scanTypeOf(%s) = %v, want %v", c.col.DataType, got, c.want)
		}
	}
}

func TestHasPrecisionScale(t *testing.T) {
	if !hasPrecisionScale(columnData{DataType: "NUMBER", Precision: 38}) {
		t.Error("a NUMBER with a precision should report one")
	}
	if hasPrecisionScale(columnData{DataType: "VARCHAR"}) {
		t.Error("VARCHAR has no decimal size")
	}
	if hasPrecisionScale(columnData{DataType: "NUMBER"}) {
		t.Error("a NUMBER with no precision on the wire should report none")
	}
}

func TestResultOf(t *testing.T) {
	resp := &sqlResponse{ResultSets: []resultSetData{{
		Columns: []columnData{{Name: "number of rows inserted"}},
		Rows:    [][]json.RawMessage{{json.RawMessage("3")}},
	}}}
	res := resultOf(resp)
	n, err := res.RowsAffected()
	if err != nil || n != 3 {
		t.Errorf("RowsAffected() = %d, %v; want 3", n, err)
	}
	// A zero that reads as a real row id is worse than an error.
	if _, err := res.LastInsertId(); err == nil {
		t.Error("LastInsertId should report that it is unsupported")
	}
	if n, _ := resultOf(&sqlResponse{}).RowsAffected(); n != 0 {
		t.Errorf("RowsAffected() with no result sets = %d, want 0", n)
	}
}

func TestResultOfSumsAcrossStatements(t *testing.T) {
	one := resultSetData{
		Columns: []columnData{{Name: "number of rows inserted"}},
		Rows:    [][]json.RawMessage{{json.RawMessage("2")}},
	}
	two := resultSetData{
		Columns: []columnData{{Name: "number of rows updated"}},
		Rows:    [][]json.RawMessage{{json.RawMessage("3")}},
	}
	n, err := resultOf(&sqlResponse{ResultSets: []resultSetData{one, two}}).RowsAffected()
	if err != nil || n != 5 {
		t.Errorf("RowsAffected() = %d, %v; want 5", n, err)
	}
}

func TestRowsWalkResultSets(t *testing.T) {
	mk := func(name, cell string) resultSetData {
		return resultSetData{
			Columns: []columnData{{Name: name, DataType: "NUMBER"}},
			Rows:    [][]json.RawMessage{{json.RawMessage(cell)}},
		}
	}
	r := rowsOf(&sqlResponse{ResultSets: []resultSetData{mk("A", "1"), mk("B", "2")}}, time.UTC)
	if got := r.Columns(); len(got) != 1 || got[0] != "A" {
		t.Fatalf("first set columns = %v", got)
	}
	if !r.HasNextResultSet() {
		t.Fatal("a second result set should be visible")
	}
	if err := r.NextResultSet(); err != nil {
		t.Fatal(err)
	}
	if got := r.Columns(); len(got) != 1 || got[0] != "B" {
		t.Fatalf("second set columns = %v", got)
	}
	if r.HasNextResultSet() {
		t.Error("there is no third result set")
	}
}

func TestDDLDMLAndTypedQuery(t *testing.T) {
	db := open(t)
	defer db.Close()

	mustExec(t, db, "CREATE OR REPLACE DATABASE go_test_db")
	mustExec(t, db, "USE DATABASE go_test_db")
	mustExec(t, db, "CREATE TABLE people (id INTEGER, name VARCHAR, score FLOAT, ok BOOLEAN)")

	res, err := db.Exec("INSERT INTO people VALUES (?, ?, ?, ?), (?, ?, ?, ?)",
		1, "Ada O'Hara \\ Byron", 9.5, true,
		2, "Grace", 8.25, false)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 2 {
		t.Fatalf("RowsAffected = %d, want 2", n)
	}

	var (
		id    int64
		name  string
		score float64
		ok    bool
	)
	row := db.QueryRow("SELECT id, name, score, ok FROM people WHERE id = ?", 1)
	if err := row.Scan(&id, &name, &score, &ok); err != nil {
		t.Fatal(err)
	}
	if id != 1 || name != `Ada O'Hara \ Byron` || score != 9.5 || !ok {
		t.Fatalf("got (%d, %q, %v, %v)", id, name, score, ok)
	}
}

// Temporal columns arrive as time.Time rather than text.
func TestTemporalColumnsScanIntoTime(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE gap_time_db",
		"USE DATABASE gap_time_db",
		"CREATE TABLE stamps (d DATE, tm TIME, ntz TIMESTAMP_NTZ, ltz TIMESTAMP_LTZ, tz TIMESTAMP_TZ)",
		`INSERT INTO stamps SELECT '2024-03-05'::DATE, '06:07:08'::TIME,
		   '2024-03-05 06:07:08.123'::TIMESTAMP_NTZ, '2024-03-05 06:07:08.123'::TIMESTAMP_LTZ,
		   '2024-03-05 06:07:08.123 +0200'::TIMESTAMP_TZ`,
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	var d, tm, ntz, ltz, tz time.Time
	if err := conn.QueryRowContext(ctx, "SELECT d, tm, ntz, ltz, tz FROM stamps").
		Scan(&d, &tm, &ntz, &ltz, &tz); err != nil {
		t.Fatalf("scanning temporals into time.Time: %v", err)
	}
	if d.Year() != 2024 || d.Month() != time.March || d.Day() != 5 {
		t.Errorf("DATE = %s", d)
	}
	if tm.Hour() != 6 || tm.Minute() != 7 || tm.Second() != 8 {
		t.Errorf("TIME = %s", tm)
	}
	if !ntz.Equal(time.Date(2024, 3, 5, 6, 7, 8, 123000000, time.UTC)) {
		t.Errorf("TIMESTAMP_NTZ = %s", ntz)
	}
	if _, offset := tz.Zone(); offset != 2*3600 {
		t.Errorf("TIMESTAMP_TZ offset = %d, want 7200 (%s)", offset, tz)
	}
}

// A time.Time survives a round trip through a TIMESTAMP_NTZ column.
func TestTimeRoundTrip(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE gap_rt_db",
		"USE DATABASE gap_rt_db",
		"CREATE TABLE t (a TIMESTAMP_NTZ)",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	want := time.Date(2024, 3, 5, 12, 34, 56, 789000000, time.UTC)
	if _, err := conn.ExecContext(ctx, "INSERT INTO t VALUES (?)", want); err != nil {
		t.Fatal(err)
	}
	var got time.Time
	if err := conn.QueryRowContext(ctx, "SELECT a FROM t").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Errorf("round trip gave %s, want %s", got, want)
	}
}

// BINARY arrives as []byte, and a []byte parameter comes back unchanged.
func TestBinaryRoundTrip(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE gap_bin_db",
		"USE DATABASE gap_bin_db",
		"CREATE TABLE b (v BINARY)",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	want := []byte{0xde, 0xad, 0xbe, 0xef, 0x00, 0x10}
	if _, err := conn.ExecContext(ctx, "INSERT INTO b VALUES (?)", want); err != nil {
		t.Fatal(err)
	}
	var got []byte
	if err := conn.QueryRowContext(ctx, "SELECT v FROM b").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Errorf("BINARY round trip gave %x, want %x", got, want)
	}
}

// NUMBER(38,0) holds values float64 cannot name; they used to arrive rounded.
func TestWideIntegersKeepTheirDigits(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	const wide = "123456789012345678901234567890"
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE reg_num_db",
		"USE DATABASE reg_num_db",
		"CREATE TABLE wide (v NUMBER(38,0))",
		"INSERT INTO wide VALUES (" + wide + ")",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	var got string
	if err := conn.QueryRowContext(ctx, "SELECT v FROM wide").Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != wide {
		t.Errorf("NUMBER(38,0) came back as %s, want %s", got, wide)
	}

	var fits int64
	if err := conn.QueryRowContext(ctx, "SELECT 9223372036854775807").Scan(&fits); err != nil {
		t.Fatal(err)
	}
	if fits != math.MaxInt64 {
		t.Errorf("int64-range value = %d, want %d", fits, int64(math.MaxInt64))
	}
}

// TIMESTAMP_NTZ stores a wall clock, so two Times naming the same instant have to store the
// same value. They used to store the wall clock of whatever zone the caller happened to hold.
func TestSameInstantStoresIdentically(t *testing.T) {
	db := open(t)
	defer db.Close()
	ny, err := time.LoadLocation("America/New_York")
	if err != nil {
		t.Skipf("no zoneinfo available: %v", err)
	}
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE reg_tz_db",
		"USE DATABASE reg_tz_db",
		"CREATE TABLE stamps (a TIMESTAMP_NTZ)",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	instant := time.Date(2024, 3, 5, 12, 0, 0, 0, time.UTC)
	for _, v := range []time.Time{instant, instant.In(ny)} {
		if _, err := conn.ExecContext(ctx, "INSERT INTO stamps VALUES (?)", v); err != nil {
			t.Fatal(err)
		}
	}
	var distinct int64
	if err := conn.QueryRowContext(ctx, "SELECT COUNT(DISTINCT a) FROM stamps").Scan(&distinct); err != nil {
		t.Fatal(err)
	}
	if distinct != 1 {
		var stored string
		conn.QueryRowContext(ctx, "SELECT LISTAGG(TO_VARCHAR(a), ' | ') FROM stamps").Scan(&stored)
		t.Errorf("the same instant stored %d different values: %s", distinct, stored)
	}
}

// Column metadata reaches sql.ColumnType.
func TestColumnMetadata(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE gap_meta_db",
		"USE DATABASE gap_meta_db",
		"CREATE TABLE m (req NUMBER(12,2) NOT NULL, opt VARCHAR, ts TIMESTAMP_NTZ)",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	rws, err := conn.QueryContext(ctx, "SELECT req, opt, ts FROM m")
	if err != nil {
		t.Fatal(err)
	}
	defer rws.Close()
	cts, err := rws.ColumnTypes()
	if err != nil {
		t.Fatal(err)
	}
	if len(cts) != 3 {
		t.Fatalf("got %d columns", len(cts))
	}
	if nullable, ok := cts[0].Nullable(); !ok || nullable {
		t.Errorf("req nullable = (%v, %v), want (false, true)", nullable, ok)
	}
	if nullable, ok := cts[1].Nullable(); !ok || !nullable {
		t.Errorf("opt nullable = (%v, %v), want (true, true)", nullable, ok)
	}
	if p, s, ok := cts[0].DecimalSize(); !ok || p != 12 || s != 2 {
		t.Errorf("req decimal size = (%d, %d, %v), want (12, 2, true)", p, s, ok)
	}
	if _, _, ok := cts[1].DecimalSize(); ok {
		t.Error("a VARCHAR should report no decimal size")
	}
	if got := cts[0].ScanType(); got != reflect.TypeOf(float64(0)) {
		t.Errorf("req scan type = %v, want float64", got)
	}
	if got := cts[2].ScanType(); got != reflect.TypeOf(time.Time{}) {
		t.Errorf("ts scan type = %v, want time.Time", got)
	}
}

// A request holding several statements exposes every result set, and its row counts add up.
func TestMultiStatementResultSets(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE gap_multi_db",
		"USE DATABASE gap_multi_db",
		"CREATE TABLE m (a INTEGER)",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	res, err := conn.ExecContext(ctx, "INSERT INTO m VALUES (1); INSERT INTO m VALUES (2), (3)")
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := res.RowsAffected(); n != 3 {
		t.Errorf("RowsAffected across two INSERTs = %d, want 3", n)
	}

	rws, err := conn.QueryContext(ctx, "SELECT 1 AS a; SELECT 2 AS b; SELECT 3 AS c")
	if err != nil {
		t.Fatal(err)
	}
	defer rws.Close()
	var seen []int64
	for {
		for rws.Next() {
			var v int64
			if err := rws.Scan(&v); err != nil {
				t.Fatal(err)
			}
			seen = append(seen, v)
		}
		if !rws.NextResultSet() {
			break
		}
	}
	if err := rws.Err(); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 || seen[0] != 1 || seen[1] != 2 || seen[2] != 3 {
		t.Errorf("walked result sets gave %v, want [1 2 3]", seen)
	}
}

// LastInsertId has no meaning here, and a zero reads as a real row id.
func TestLastInsertIdReportsUnsupported(t *testing.T) {
	db := open(t)
	defer db.Close()
	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx := context.Background()
	for _, s := range []string{
		"CREATE OR REPLACE DATABASE reg_lid_db",
		"USE DATABASE reg_lid_db",
		"CREATE TABLE t (a INTEGER)",
	} {
		if _, err := conn.ExecContext(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	res, err := conn.ExecContext(ctx, "INSERT INTO t VALUES (1)")
	if err != nil {
		t.Fatal(err)
	}
	if n, err := res.RowsAffected(); err != nil || n != 1 {
		t.Errorf("RowsAffected() = %d, %v; want 1", n, err)
	}
	if _, err := res.LastInsertId(); err == nil {
		t.Error("LastInsertId should report that it is unsupported")
	}
}
