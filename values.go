package frostlake

import (
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// The layouts the engine emits. TIME arrives without a fractional part and TIMESTAMP_LTZ/TZ
// carry a numeric offset; the fractional and T-separated variants are accepted anyway, so a
// server that spells them with more precision still parses.
var (
	dateLayouts  = []string{"2006-01-02"}
	timeLayouts  = []string{"15:04:05.999999999", "15:04:05"}
	naiveLayouts = []string{
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
	}
	zonedLayouts = []string{
		"2006-01-02 15:04:05.999999999 -0700",
		"2006-01-02 15:04:05 -0700",
		"2006-01-02 15:04:05.999999999 -07:00",
		"2006-01-02 15:04:05 -07:00",
		"2006-01-02T15:04:05.999999999Z07:00",
	}
)

// temporalKind says how a column's text should be read back into a time.Time.
type temporalKind int

const (
	notTemporal temporalKind = iota
	dateKind
	timeKind
	naiveKind // TIMESTAMP_NTZ: a wall clock with no zone of its own
	zonedKind // TIMESTAMP_LTZ / _TZ: a wall clock plus an offset
)

// baseTypeName strips any (p,s) suffix, so NUMBER(38,0) and NUMBER answer alike.
func baseTypeName(dataType string) string {
	name := strings.ToUpper(strings.TrimSpace(dataType))
	if i := strings.IndexByte(name, '('); i >= 0 {
		return strings.TrimSpace(name[:i])
	}
	return name
}

func kindOf(dataType string) temporalKind {
	switch baseTypeName(dataType) {
	case "DATE":
		return dateKind
	case "TIME":
		return timeKind
	case "TIMESTAMP", "TIMESTAMP_NTZ", "DATETIME":
		return naiveKind
	case "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		return zonedKind
	}
	return notTemporal
}

func isBinary(dataType string) bool {
	switch baseTypeName(dataType) {
	case "BINARY", "VARBINARY":
		return true
	}
	return false
}

// isIntegral reports whether a column holds whole numbers. The wire carries precision and
// scale as their own fields — dataType is the bare word NUMBER — so scale is what decides,
// with an inline NUMBER(p,s) spelling honoured as a fallback.
func isIntegral(col columnData) bool {
	switch baseTypeName(col.DataType) {
	case "INT", "INTEGER", "BIGINT", "SMALLINT", "TINYINT", "BYTEINT":
		return true
	case "NUMBER", "NUMERIC", "DECIMAL":
		return declaredScale(col) == 0
	}
	return false
}

// declaredScale prefers the wire's scale field, falling back to an inline NUMBER(p,s) spelling
// for servers that put precision and scale in the type name.
func declaredScale(col columnData) int {
	if col.Scale != 0 {
		return col.Scale
	}
	name := col.DataType
	open := strings.IndexByte(name, '(')
	comma := strings.IndexByte(name, ',')
	end := strings.IndexByte(name, ')')
	if open >= 0 && comma > open && end > comma {
		if n, err := strconv.Atoi(strings.TrimSpace(name[comma+1 : end])); err == nil {
			return n
		}
	}
	return col.Scale
}

// parseTemporal reads a temporal cell. loc gives the zone for the kinds that carry none of
// their own — a DATE, a TIME and a TIMESTAMP_NTZ are wall clocks, and something has to say
// which zone that clock belongs to.
func parseTemporal(text string, kind temporalKind, loc *time.Location) (time.Time, error) {
	var layouts []string
	switch kind {
	case dateKind:
		layouts = dateLayouts
	case timeKind:
		layouts = timeLayouts
	case naiveKind:
		layouts = naiveLayouts
	case zonedKind:
		layouts = zonedLayouts
	default:
		return time.Time{}, fmt.Errorf("frostlake: %q is not a temporal value", text)
	}
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, text, loc); err == nil {
			return t, nil
		}
	}
	// A zoned column whose value arrived without an offset is still readable as a wall clock.
	if kind == zonedKind {
		for _, layout := range naiveLayouts {
			if t, err := time.ParseInLocation(layout, text, loc); err == nil {
				return t, nil
			}
		}
	}
	return time.Time{}, fmt.Errorf("frostlake: cannot read %q as a %s value", text, typeNameOf(kind))
}

func typeNameOf(kind temporalKind) string {
	switch kind {
	case dateKind:
		return "DATE"
	case timeKind:
		return "TIME"
	case naiveKind:
		return "TIMESTAMP_NTZ"
	case zonedKind:
		return "TIMESTAMP_TZ"
	}
	return "non-temporal"
}

// convert maps a JSON cell to a Go value guided by the column's declared type: temporals
// become time.Time, BINARY []byte, integral NUMBERs int64, fractional and floating types
// float64, BOOLEAN bool, and everything else its text (VARIANT/OBJECT/ARRAY as JSON).
func convert(raw json.RawMessage, col columnData, loc *time.Location) (driver.Value, error) {
	s := string(raw)
	if s == "null" || len(s) == 0 {
		return nil, nil
	}
	if s[0] == '"' {
		var text string
		if err := json.Unmarshal(raw, &text); err != nil {
			return nil, err
		}
		if kind := kindOf(col.DataType); kind != notTemporal {
			return parseTemporal(text, kind, loc)
		}
		if isBinary(col.DataType) {
			decoded, err := hex.DecodeString(text)
			if err != nil {
				// Not hex after all: hand back the text rather than losing the value.
				return text, nil
			}
			return decoded, nil
		}
		return text, nil
	}
	if s == "true" || s == "false" {
		return s == "true", nil
	}
	// A bare JSON number. Whole values on an integral column are int64; anything else float64.
	if !strings.ContainsAny(s, ".eE") {
		n, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			if isIntegral(col) {
				return n, nil
			}
		} else if isIntegral(col) {
			// Wider than int64 — NUMBER(38,0) holds values float64 cannot name. Handing back
			// the digits keeps them exact; a float64 would silently round them.
			return s, nil
		}
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	// Structured cell (object/array): hand back its JSON text.
	return s, nil
}

var (
	typeInt64   = reflect.TypeOf(int64(0))
	typeFloat64 = reflect.TypeOf(float64(0))
	typeBool    = reflect.TypeOf(false)
	typeString  = reflect.TypeOf("")
	typeBytes   = reflect.TypeOf([]byte(nil))
	typeTime    = reflect.TypeOf(time.Time{})
)

// scanTypeOf reports the Go type a column's values arrive as, for sql.ColumnType.ScanType.
func scanTypeOf(col columnData) reflect.Type {
	if kindOf(col.DataType) != notTemporal {
		return typeTime
	}
	if isBinary(col.DataType) {
		return typeBytes
	}
	if isIntegral(col) {
		return typeInt64
	}
	switch baseTypeName(col.DataType) {
	case "NUMBER", "NUMERIC", "DECIMAL", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "REAL":
		return typeFloat64
	case "BOOLEAN":
		return typeBool
	}
	return typeString
}

// hasPrecisionScale reports whether precision and scale mean anything for this column.
func hasPrecisionScale(col columnData) bool {
	switch baseTypeName(col.DataType) {
	case "NUMBER", "NUMERIC", "DECIMAL":
		return col.Precision > 0
	}
	return false
}
