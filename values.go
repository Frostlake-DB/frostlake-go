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

// The layouts the engine emits. A timestamp carries three, six or nine fractional digits, as
// many as its value needs, and a TIME carries the same when it has a fraction and none on a whole
// second. TIMESTAMP_LTZ/TZ add a ±HHMM offset. The layouts read a fraction of up to nine digits,
// or none, so an engine before 0.1.0, which cuts a timestamp to the millisecond and a TIME to the
// second, parses too; the T-separated and colon-offset variants are accepted anyway.
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
	if kind != timeKind {
		if year, rest, ok := splitYear(text); ok {
			// Read in UTC, whose clock has no gaps: loc's daylight-saving rules for the stand-in
			// year would move a wall clock the real year keeps. withYear places it in loc.
			if t, hasOffset, ok := parseLayouts(rest, kind, layouts, time.UTC); ok {
				if t, ok := withYear(t, year, hasOffset, loc); ok {
					return t, nil
				}
			}
			return time.Time{}, cannotRead(text, kind)
		}
	}
	if t, _, ok := parseLayouts(text, kind, layouts, loc); ok {
		return t, nil
	}
	return time.Time{}, cannotRead(text, kind)
}

func cannotRead(text string, kind temporalKind) error {
	return fmt.Errorf("frostlake: cannot read %q as a %s value", text, typeNameOf(kind))
}

// parseLayouts tries a kind's layouts in turn, and reports whether the one that matched read an
// offset from the text.
func parseLayouts(text string, kind temporalKind, layouts []string, loc *time.Location) (t time.Time, hasOffset, ok bool) {
	for _, layout := range layouts {
		if t, err := time.ParseInLocation(layout, text, loc); err == nil {
			return t, kind == zonedKind, true
		}
	}
	// A zoned column whose value arrived without an offset is still readable as a wall clock.
	if kind == zonedKind {
		for _, layout := range naiveLayouts {
			if t, err := time.ParseInLocation(layout, text, loc); err == nil {
				return t, false, true
			}
		}
	}
	return time.Time{}, false, false
}

// maxYearDigits is the widest year a Frostlake engine can hold (java.time stops at
// ±999,999,999). time.Time holds every one of them exactly.
const maxYearDigits = 9

// standInYear takes the place of a year the layouts cannot read. It is a leap year, so the text's
// 29 February parses too; withYear then checks that the real year has one.
const standInYear = "2000"

// splitYear separates the year from a temporal text when Go's four-digit year field cannot read
// it: a year past 9999, which the engine prints in full as the account does ("12000-01-01"), or a
// signed year — the account's spelling of a year before 1 ("-1-01-15"), and java.time's padded
// one ("-0001-01-01", "+20201-01-15"). It returns the year and the text with the stand-in year in
// its place. A plain four-digit year, and a text that does not open with a year, report ok=false.
func splitYear(text string) (year int, rest string, ok bool) {
	i := 0
	signed := i < len(text) && (text[i] == '+' || text[i] == '-')
	if signed {
		i++
	}
	start := i
	for i < len(text) && text[i] >= '0' && text[i] <= '9' {
		i++
	}
	digits := i - start
	if digits == 0 || digits > maxYearDigits || i == len(text) || text[i] != '-' {
		return 0, "", false
	}
	if !signed && digits <= 4 {
		return 0, "", false
	}
	year, err := strconv.Atoi(text[start:i])
	if err != nil {
		return 0, "", false
	}
	if text[0] == '-' {
		year = -year
	}
	return year, standInYear + text[i:], true
}

// withYear moves a value parsed under the stand-in year to its real year, keeping what the text
// said: its wall clock, and its offset when it wrote one. A value without an offset is a wall
// clock in loc. A value with one names an instant, placed by that offset; as
// time.ParseInLocation does, it lands in loc when loc keeps that offset at that instant, and in a
// zone fixed at the offset otherwise. It reports false for a 29 February in a year that has none.
func withYear(parsed time.Time, year int, hasOffset bool, loc *time.Location) (time.Time, bool) {
	wall := time.Date(year, parsed.Month(), parsed.Day(), parsed.Hour(), parsed.Minute(),
		parsed.Second(), parsed.Nanosecond(), time.UTC)
	if wall.Month() != parsed.Month() || wall.Day() != parsed.Day() {
		return time.Time{}, false
	}
	if !hasOffset {
		return time.Date(year, parsed.Month(), parsed.Day(), parsed.Hour(), parsed.Minute(),
			parsed.Second(), parsed.Nanosecond(), loc), true
	}
	_, offset := parsed.Zone()
	instant := wall.Add(-time.Duration(offset) * time.Second)
	if _, inLoc := instant.In(loc).Zone(); inLoc == offset {
		return instant.In(loc), true
	}
	return instant.In(time.FixedZone("", offset)), true
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
