package frostlake

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type rows struct {
	sets []resultSetData
	set  int
	pos  int
	loc  *time.Location
}

func rowsOf(resp *sqlResponse, loc *time.Location) *rows {
	return &rows{sets: resp.ResultSets, loc: loc}
}

// current is the result set being read. A request that produced none reads as empty rather
// than panicking, which is what a DDL statement run through Query looks like.
func (r *rows) current() resultSetData {
	if r.set >= 0 && r.set < len(r.sets) {
		return r.sets[r.set]
	}
	return resultSetData{}
}

func (r *rows) column(i int) columnData {
	cols := r.current().Columns
	if i >= 0 && i < len(cols) {
		return cols[i]
	}
	return columnData{}
}

func (r *rows) Columns() []string {
	cols := r.current().Columns
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c.Name
	}
	return out
}

func (r *rows) Close() error { return nil }

func (r *rows) Next(dest []driver.Value) error {
	data := r.current().Rows
	if r.pos >= len(data) {
		return io.EOF
	}
	row := data[r.pos]
	r.pos++
	for i := range dest {
		if i >= len(row) {
			dest[i] = nil
			continue
		}
		v, err := convert(row[i], r.column(i), r.loc)
		if err != nil {
			return err
		}
		dest[i] = v
	}
	return nil
}

// HasNextResultSet and NextResultSet implement driver.RowsNextResultSet, so a request holding
// several statements can be read past its first answer.
func (r *rows) HasNextResultSet() bool { return r.set+1 < len(r.sets) }

func (r *rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	r.set++
	r.pos = 0
	return nil
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	return r.column(index).DataType
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	cols := r.current().Columns
	if index < 0 || index >= len(cols) {
		return false, false
	}
	return cols[index].Nullable, true
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	col := r.column(index)
	if !hasPrecisionScale(col) {
		return 0, 0, false
	}
	return int64(col.Precision), int64(declaredScale(col)), true
}

// ColumnTypeLength reports a text or binary column's length — characters for text, bytes for
// binary — as the server declares it. Any other type, and a server too old to send the length,
// reports none.
func (r *rows) ColumnTypeLength(index int) (length int64, ok bool) {
	col := r.column(index)
	if col.Length == nil {
		return 0, false
	}
	return *col.Length, true
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	return scanTypeOf(r.column(index))
}

type result struct{ affected int64 }

// resultOf derives RowsAffected from the DML answer — Frostlake reports it as a one-cell
// result set titled "number of rows inserted"/"updated"/"deleted". A request holding several
// statements answers with one such set each, and the counts add up.
func resultOf(resp *sqlResponse) driver.Result {
	res := &result{}
	for _, rs := range resp.ResultSets {
		if len(rs.Columns) >= 1 && strings.HasPrefix(strings.ToLower(rs.Columns[0].Name), "number of rows") &&
			len(rs.Rows) == 1 && len(rs.Rows[0]) >= 1 {
			if n, err := strconv.ParseInt(string(rs.Rows[0][0]), 10, 64); err == nil {
				res.affected += n
			}
		}
	}
	return res
}

// LastInsertId is not supported: the engine has no notion of a generated row id, and the
// database/sql contract asks for an error rather than a zero that reads as a real value.
func (r *result) LastInsertId() (int64, error) {
	return 0, errors.New("frostlake: LastInsertId is not supported")
}

func (r *result) RowsAffected() (int64, error) { return r.affected, nil }
