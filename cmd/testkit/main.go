// Command testkit replays the engine's testkit corpus through this driver: the language-neutral
// SQL suites that live with the engine (engine/src/test/resources/testkit/suites, their format in
// the SCHEMA.md beside them), reported case by case the way the engine's Java reference runner
// reports them.
//
//	FL_CORPUS=/path/to/frostlake/engine/src/test/resources/testkit \
//	FROSTLAKE_URL=frostlake://127.0.0.1:18082 go run ./cmd/testkit
//
// Without FL_CORPUS it replays nothing and says so. go test ./... runs it as TestCorpus, which is
// skipped unless FL_CORPUS is set.
//
// Every statement travels database/sql, this driver, HTTP and DatabaseHttpServer, so the replay
// checks the driver as much as the engine: each cell the driver decodes is written back as the
// text the engine sent for it before it is compared, and a cell it cannot decode is an ERROR.
//
// Environment:
//
//	FL_CORPUS                 the testkit directory to replay, its suite files in suites/*.json
//	FROSTLAKE_URL             the engine to attach to, frostlake://host:port
//	FROSTLAKE_CLASSPATH       without FROSTLAKE_URL, boot an engine from this classpath (JAVA_HOME's java)
//	FROSTLAKE_TESTKIT_REPORT  the TSV report; by default results/testkit-go.tsv, missing-apis-go.md beside it
//	FROSTLAKE_TESTKIT_FILTER  only the suites whose file or suite name contains one of these comma-separated words
//
// The semantics are SCHEMA.md's, and the reference runner's where SCHEMA.md leaves them implicit:
//
//   - a case whose skip clause names "go", or "http" — the transport this driver speaks — is a SKIP;
//   - before each case the session is reset (MULTI_STATEMENT_COUNT = 0, then a fresh
//     test_db.test_schema made current), and the case's steps run in order on that same session: the
//     whole replay runs on one pinned connection, as the reference runs on one session;
//   - the first failed check stops the case. A refusal the engine reports is a statement's result;
//     any other failure — the transport, a cell the driver cannot decode — is an ERROR;
//   - capabilities: SESSION, COLUMN_NAMES and UPDATE_COUNT, derived from the count grid. There is no
//     ERROR_CODE, since the engine's refusal carries a message only, so an expected code or sqlState
//     is recorded in missing-apis-go.md rather than failed;
//   - values compare after the reference's normalization: NULL and booleans folded, anything numeric
//     rounded half up to 10 significant digits, everything else trimmed text. A VARIANT, OBJECT or
//     ARRAY cell that is a JSON string is decoded once, as the reference decodes it.
//
// It ends with "testkit [go]: <passed> passed, <failed> failed, <skipped> skipped", an errored case
// counted as failed, and exits 1 when any case failed or errored.
package main

import (
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	frostlake "github.com/Frostlake-DB/frostlake-go"
)

// backend is this runner's name, as a case's skip clause spells it.
const backend = "go"

// skipKeys are the names in a skip clause this runner honours: its own, and the transport it rides.
var skipKeys = []string{backend, "http"}

// resetContext runs before every case: a session that accepts a script of any length, then an empty
// test_db.test_schema, made current.
var resetContext = []string{
	"ALTER SESSION SET MULTI_STATEMENT_COUNT = 0",
	"CREATE OR REPLACE DATABASE test_db",
	"USE DATABASE test_db",
	"CREATE OR REPLACE SCHEMA test_schema",
	"USE SCHEMA test_schema",
}

// requestTimeout bounds one statement, as the reference's HTTP backend bounds it, unless
// FROSTLAKE_URL names a timeout of its own.
const requestTimeout = "60s"

// missingErrorCode is the check the transport cannot make: the engine's refusal carries no code.
const missingErrorCode = "ERROR_CODE: cannot check error code/sqlState (backend reports message only)"

// cellSeparator joins a row's normalized cells, so rows compare as single strings.
const cellSeparator = "\x1f"

// shownFailures bounds how many failed cases are printed as they happen; the report holds them all.
const shownFailures = 50

// noCorpus is what a run without FL_CORPUS prints, and what TestCorpus skips with.
const noCorpus = "set FL_CORPUS to frostlake's engine/src/test/resources/testkit to replay the testkit corpus"

func main() {
	os.Exit(run())
}

func run() int {
	corpus := os.Getenv("FL_CORPUS")
	if corpus == "" {
		fmt.Printf("testkit [%s]: %s\n", backend, noCorpus)
		return 0
	}
	files, err := suiteFiles(corpus)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testkit [%s]: %v\n", backend, err)
		return 2
	}
	report := os.Getenv("FROSTLAKE_TESTKIT_REPORT")
	if report == "" {
		report = filepath.Join(checkoutRoot(), "results", "testkit-"+backend+".tsv")
	}

	dsn, stop, err := engine()
	if err != nil {
		fmt.Fprintf(os.Stderr, "testkit [%s]: %v\n", backend, err)
		return 2
	}
	defer stop()
	connector, err := frostlake.NewConnector(dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testkit [%s]: %v\n", backend, err)
		return 2
	}
	db := sql.OpenDB(connector)
	defer db.Close()
	if err := reachable(db); err != nil {
		fmt.Fprintf(os.Stderr, "testkit [%s]: the engine at %s does not answer: %v\n", backend, dsn, err)
		return 2
	}

	r := &runner{db: db, missing: map[string][]string{}}
	defer r.retire()
	words := filterWords()
	started := time.Now()
	var cases []caseRow
	counts := map[string]int{}
	suitesRun := 0
	for _, file := range files {
		doc, err := readSuite(file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "testkit [%s]: %v\n", backend, err)
			return 2
		}
		stem := strings.TrimSuffix(filepath.Base(file), ".json")
		suite, ok := getStr(doc, "suite")
		if !ok {
			suite = stem
		}
		if !wanted(words, stem, suite) {
			continue
		}
		suitesRun++
		for _, each := range getArr(doc, "tests") {
			test := obj(each)
			name, _ := getStr(test, "name")
			row := caseRow{suite: suite, test: name}
			if key, reason, skipped := skipClause(test); skipped {
				row.status = "SKIP"
				row.detail = "skip[" + key + "]: " + reason
			} else {
				begin := time.Now()
				row.status, row.step, row.detail = r.runCase(suite, name, test)
				row.ms = time.Since(begin).Milliseconds()
			}
			counts[row.status]++
			cases = append(cases, row)
			if (row.status == "FAIL" || row.status == "ERROR") && counts["FAIL"]+counts["ERROR"] <= shownFailures {
				fmt.Printf("  %-5s %s / %s%s: %s\n", row.status, suite, name, stepSuffix(row.step), clip(row.detail, 400))
			}
		}
	}

	if err := writeReport(report, cases); err != nil {
		fmt.Fprintf(os.Stderr, "testkit [%s]: cannot write the report: %v\n", backend, err)
		return 2
	}
	notes := filepath.Join(filepath.Dir(report), "missing-apis-"+backend+".md")
	if err := r.writeMissingAPIs(notes); err != nil {
		fmt.Fprintf(os.Stderr, "testkit [%s]: cannot write %s: %v\n", backend, notes, err)
		return 2
	}
	failed := counts["FAIL"] + counts["ERROR"]
	fmt.Printf("testkit [%s]: %d passed, %d failed, %d skipped\n", backend, counts["PASS"], failed, counts["SKIP"])
	fmt.Printf("  %d of the failed errored; %d check(s) need an API the transport lacks (%s)\n",
		counts["ERROR"], r.missingChecks(), notes)
	fmt.Printf("  %d suites, %d cases in %.1fs; report %s\n", suitesRun, len(cases),
		time.Since(started).Seconds(), report)
	if failed > 0 {
		return 1
	}
	return 0
}

// caseRow is one line of the report.
type caseRow struct {
	suite, test, status string
	step                int // the step that failed, counted from 1; 0 when none did
	detail              string
	ms                  int64
}

// runner replays cases on one pinned connection, and so on one engine session.
type runner struct {
	db   *sql.DB
	conn *sql.Conn
	// missing maps a check the transport cannot make to the steps that asked for it.
	missing map[string][]string
	notes   []string
}

// runCase resets the session and runs a case's steps until the first failed check.
func (r *runner) runCase(suite, name string, test map[string]any) (status string, step int, detail string) {
	for _, statement := range resetContext {
		res, err := r.execute(statement)
		if err != nil {
			return "ERROR", 0, fmt.Sprintf("resetContext failed on '%s': %v", statement, err)
		}
		if res.refused() {
			return "ERROR", 0, fmt.Sprintf("resetContext failed on '%s': %s", statement, *res.refusal)
		}
	}
	for i, each := range getArr(test, "steps") {
		stepDef := obj(each)
		statement, _ := getStr(stepDef, "sql")
		res, err := r.execute(statement)
		if err != nil {
			return "ERROR", i + 1, fmt.Sprintf("%v  [sql: %s]", err, statement)
		}
		failure, missing := check(getObj(stepDef, "expect"), res)
		for _, note := range missing {
			r.recordMissing(note, fmt.Sprintf("%s / %s, step %d", suite, name, i+1))
		}
		if failure != "" {
			return "FAIL", i + 1, failure + "  [sql: " + statement + "]"
		}
	}
	return "PASS", 0, ""
}

// session is the pinned connection, pinned afresh after a transport failure retired the last one.
func (r *runner) session() (*sql.Conn, error) {
	if r.conn == nil {
		conn, err := r.db.Conn(context.Background())
		if err != nil {
			return nil, err
		}
		r.conn = conn
	}
	return r.conn, nil
}

// retire gives the pinned connection back. The driver has marked one whose transport failed as
// unusable, so the pool discards it and the next case starts on a new session.
func (r *runner) retire() {
	if r.conn != nil {
		r.conn.Close()
		r.conn = nil
	}
}

// execute runs one statement on the pinned session. A refusal the engine reports comes back as the
// result; anything else that goes wrong is the error.
func (r *runner) execute(statement string) (*result, error) {
	conn, err := r.session()
	if err != nil {
		return nil, err
	}
	rows, err := conn.QueryContext(context.Background(), statement)
	if err != nil {
		var refusal *frostlake.Error
		if errors.As(err, &refusal) {
			message := refusal.Message
			return &result{updateCount: -1, refusal: &message}, nil
		}
		r.retire()
		return nil, err
	}
	defer rows.Close()
	return readResult(rows)
}

func (r *runner) recordMissing(note, where string) {
	if _, seen := r.missing[note]; !seen {
		r.notes = append(r.notes, note)
	}
	r.missing[note] = append(r.missing[note], where)
}

func (r *runner) missingChecks() int {
	n := 0
	for _, places := range r.missing {
		n += len(places)
	}
	return n
}

// writeMissingAPIs lists the checks the corpus asked for that this transport cannot express. It is
// written on every run, so a report never sits beside an earlier run's list.
func (r *runner) writeMissingAPIs(path string) error {
	var b strings.Builder
	fmt.Fprintf(&b, "# Missing APIs for backend `%s`\n\n", backend)
	b.WriteString("Checks the corpus asks for that this driver's transport cannot express. They are recorded,\n")
	b.WriteString("not failed: the day the transport reports them, these checks light up without a change to\n")
	b.WriteString("any case.\n\n")
	if len(r.notes) == 0 {
		b.WriteString("None: every check the replayed cases asked for was made.\n")
	}
	for _, note := range r.notes {
		places := r.missing[note]
		fmt.Fprintf(&b, "- %s: %d check(s)\n", note, len(places))
		for _, where := range places {
			fmt.Fprintf(&b, "  - %s\n", where)
		}
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// result is one statement's outcome, in the reference's shape: the first result grid as text (a nil
// cell is SQL NULL), the DML count or -1, or the engine's refusal.
type result struct {
	columns     []string
	rows        [][]*string
	updateCount int64
	refusal     *string
}

func (r *result) refused() bool { return r.refusal != nil }

// readResult reads the first result set, which is the one the reference compares. Each cell is
// written back as the text the engine sent for it.
func readResult(rows *sql.Rows) (*result, error) {
	names, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	types, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	out := &result{columns: names, rows: [][]*string{}, updateCount: -1}
	values := make([]any, len(names))
	targets := make([]any, len(names))
	for i := range values {
		targets[i] = &values[i]
	}
	for rows.Next() {
		if err := rows.Scan(targets...); err != nil {
			return nil, err
		}
		cells := make([]*string, len(values))
		for i, value := range values {
			typeName := types[i].DatabaseTypeName()
			cells[i] = wireText(value, typeName)
			if semiStructured(typeName) {
				cells[i] = semiStructuredValue(cells[i])
			}
		}
		out.rows = append(out.rows, cells)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out.deriveUpdateCount()
	return out, nil
}

// deriveUpdateCount reads a DML count the way the reference does when the transport reports none out
// of band: a single row whose columns are all named "number of …" counts its first cell.
func (r *result) deriveUpdateCount() {
	if r.updateCount >= 0 || len(r.columns) == 0 || len(r.rows) != 1 {
		return
	}
	for _, name := range r.columns {
		if !strings.HasPrefix(strings.ToLower(name), "number of") {
			return
		}
	}
	if len(r.rows[0]) == 0 || r.rows[0][0] == nil {
		return
	}
	if n, err := strconv.ParseInt(javaTrim(*r.rows[0][0]), 10, 64); err == nil {
		r.updateCount = n
	}
}

// wireText writes a decoded cell back as the text the engine sends for it (the HTTP API's wire
// format): numbers plain, BINARY as upper-case hex, and temporal values in the engine's layouts, with
// as many fractional digits as the value needs — three, six or nine, never fewer than three for a
// timestamp and none for a whole-second TIME.
func wireText(value any, typeName string) *string {
	var text string
	switch v := value.(type) {
	case nil:
		return nil
	case bool:
		text = strconv.FormatBool(v)
	case int64:
		text = strconv.FormatInt(v, 10)
	case float64:
		switch {
		case math.IsNaN(v):
			text = "NaN"
		case math.IsInf(v, 1):
			text = "Infinity"
		case math.IsInf(v, -1):
			text = "-Infinity"
		default:
			text = strconv.FormatFloat(v, 'f', -1, 64)
		}
	case string:
		text = v
	case []byte:
		text = strings.ToUpper(hex.EncodeToString(v))
	case time.Time:
		text = temporalText(v, typeName)
	default:
		text = fmt.Sprint(v)
	}
	return &text
}

func temporalText(t time.Time, typeName string) string {
	switch baseTypeName(typeName) {
	case "DATE":
		return t.Format("2006-01-02")
	case "TIME":
		return t.Format("15:04:05") + fraction(t.Nanosecond(), 0)
	case "TIMESTAMP_LTZ", "TIMESTAMP_TZ":
		return t.Format("2006-01-02 15:04:05") + fraction(t.Nanosecond(), 3) + t.Format(" -0700")
	}
	return t.Format("2006-01-02 15:04:05") + fraction(t.Nanosecond(), 3)
}

// fraction is a second's fraction as the engine writes it: three, six or nine digits, as many as the
// value needs, and at least minimum.
func fraction(nanos, minimum int) string {
	width := 9
	switch {
	case nanos == 0:
		width = 0
	case nanos%1000000 == 0:
		width = 3
	case nanos%1000 == 0:
		width = 6
	}
	if width < minimum {
		width = minimum
	}
	if width == 0 {
		return ""
	}
	return "." + fmt.Sprintf("%09d", nanos)[:width]
}

// baseTypeName strips a (p,s) suffix, so NUMBER(38,0) and NUMBER answer alike.
func baseTypeName(typeName string) string {
	name := strings.ToUpper(strings.TrimSpace(typeName))
	if i := strings.IndexByte(name, '('); i >= 0 {
		name = strings.TrimSpace(name[:i])
	}
	return name
}

// semiStructured reports whether a column carries VARIANT, OBJECT or ARRAY values, by the type the
// engine declared for it, never by what a cell looks like.
func semiStructured(typeName string) bool {
	switch strings.ToUpper(typeName) {
	case "VARIANT", "OBJECT", "ARRAY":
		return true
	}
	return false
}

// semiStructuredValue is the value a semi-structured cell carries, as the corpus records it. A client
// is handed such a cell as its JSON text, a string's own quotes included; the corpus records the value,
// a rather than "a". So a cell that is a JSON string becomes that string's content, and anything else —
// an object, a number, text that is not JSON at all — is left exactly as it came.
func semiStructuredValue(cell *string) *string {
	if cell == nil {
		return nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(*cell), &decoded); err != nil {
		return cell
	}
	if text, ok := decoded.(string); ok {
		return &text
	}
	return cell
}

// check holds one step's result to its expect block. It answers the first failed check, or "" when
// every check held, and the checks it could not make for want of a capability.
func check(expect map[string]any, res *result) (failure string, missing []string) {
	if refusal := getObj(expect, "error"); refusal != nil {
		return checkRefusal(refusal, res)
	}
	if res.refused() {
		return "unexpected error: " + *res.refusal, nil
	}
	if expect == nil {
		return "", nil
	}
	if want, ok := expect["value"]; ok {
		value := asText(want)
		actual := firstCell(res.rows)
		if norm(value) != norm(actual) {
			return fmt.Sprintf("value [%s] != expected [%s]", show(actual), show(value)), nil
		}
	}
	if rows := getArr(expect, "rows"); rows != nil {
		if diff := gridDiff(expectedGrid(rows), res.rows, getBool(expect, "ordered")); diff != "" {
			return diff, nil
		}
	}
	if want, ok := getInt(expect, "rowCount"); ok {
		if got := int64(len(res.rows)); got != want {
			return fmt.Sprintf("rowCount %d != expected %d", got, want), nil
		}
	}
	if columns := getArr(expect, "columns"); columns != nil {
		if mismatch := columnMismatch(columns, res.columns); mismatch != "" {
			return mismatch, nil
		}
	}
	if want, ok := getInt(expect, "updateCount"); ok {
		if res.updateCount != want {
			return fmt.Sprintf("updateCount %d != expected %d", res.updateCount, want), nil
		}
	}
	return "", nil
}

// checkRefusal holds a step that must fail: it did, with the expected message fragment. A code or
// sqlState cannot be checked, since the refusal carries neither.
func checkRefusal(want map[string]any, res *result) (string, []string) {
	if !res.refused() {
		return "expected an error, statement succeeded", nil
	}
	fragment, ok := getStr(want, "messageContains")
	if ok && !strings.Contains(strings.ToLower(*res.refusal), strings.ToLower(fragment)) {
		return fmt.Sprintf("error message [%s] does not contain [%s]", *res.refusal, fragment), nil
	}
	_, code := getStr(want, "code")
	_, state := getStr(want, "sqlState")
	if !code && !state {
		return "", nil
	}
	return "", []string{missingErrorCode}
}

func expectedGrid(rows []any) [][]*string {
	grid := make([][]*string, 0, len(rows))
	for _, row := range rows {
		cells := arr(row)
		line := make([]*string, len(cells))
		for i, cell := range cells {
			line[i] = asText(cell)
		}
		grid = append(grid, line)
	}
	return grid
}

func firstCell(rows [][]*string) *string {
	if len(rows) == 0 || len(rows[0]) == 0 {
		return nil
	}
	return rows[0][0]
}

func columnMismatch(expected []any, got []string) string {
	if len(got) != len(expected) {
		return fmt.Sprintf("column count %d != expected %d [%s]", len(got), len(expected), strings.Join(got, ", "))
	}
	for i := range expected {
		want := show(asText(expected[i]))
		if !strings.EqualFold(want, got[i]) {
			return fmt.Sprintf("column[%d] [%s] != expected [%s]", i, got[i], want)
		}
	}
	return ""
}

func gridDiff(want, got [][]*string, ordered bool) string {
	expected, actual := canon(want), canon(got)
	if !ordered {
		sort.Strings(expected)
		sort.Strings(actual)
	}
	if sameLines(expected, actual) {
		return ""
	}
	return "rows differ: expected " + showGrid(expected) + " got " + showGrid(actual)
}

func sameLines(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// canon spells each row as its normalized cells, each followed by the separator.
func canon(grid [][]*string) []string {
	out := make([]string, 0, len(grid))
	for _, row := range grid {
		var line strings.Builder
		for _, cell := range row {
			line.WriteString(norm(cell))
			line.WriteString(cellSeparator)
		}
		out = append(out, line.String())
	}
	return out
}

func showGrid(lines []string) string {
	return "[" + strings.ReplaceAll(strings.Join(lines, ", "), cellSeparator, "|") + "]"
}

func show(cell *string) string {
	if cell == nil {
		return "null"
	}
	return *cell
}

// norm is the reference's value normalization, applied to both sides before they compare: NULL and
// the empty string alike, booleans in any case, anything java.math.BigDecimal reads rounded half up to
// 10 significant digits and written plain without trailing zeros, and anything else as trimmed text.
func norm(raw *string) string {
	if raw == nil {
		return "NULL"
	}
	value := javaTrim(*raw)
	switch {
	case value == "", strings.EqualFold(value, "null"):
		return "NULL"
	case strings.EqualFold(value, "true"):
		return "TRUE"
	case strings.EqualFold(value, "false"):
		return "FALSE"
	}
	number, ok := parseDecimal(value)
	if !ok {
		return value
	}
	if number.unscaled.Sign() == 0 {
		return "0"
	}
	return number.round(10).stripTrailingZeros().plain()
}

// javaTrim trims as Java's String.trim does: every character up to and including the space.
func javaTrim(s string) string {
	return strings.TrimFunc(s, func(r rune) bool { return r <= ' ' })
}

// decimal is a number as java.math.BigDecimal holds it: unscaled × 10^-scale.
type decimal struct {
	unscaled *big.Int
	scale    int64
}

// parseDecimal reads text as BigDecimal's string constructor does: an optional sign, digits with at
// most one decimal point, and an optional exponent. It reports false for anything BigDecimal refuses.
func parseDecimal(s string) (decimal, bool) {
	i := 0
	negative := false
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		negative = s[i] == '-'
		i++
	}
	var digits []byte
	var fractionDigits int64
	point := false
	for i < len(s) {
		r, size := utf8.DecodeRuneInString(s[i:])
		if d, ok := digitValue(r); ok {
			digits = append(digits, byte('0'+d))
			if point {
				fractionDigits++
			}
			i += size
			continue
		}
		if r == '.' && !point {
			point = true
			i++
			continue
		}
		break
	}
	if len(digits) == 0 {
		return decimal{}, false
	}
	var exponent int64
	if i < len(s) {
		if s[i] != 'e' && s[i] != 'E' {
			return decimal{}, false
		}
		i++
		negativeExponent := false
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			negativeExponent = s[i] == '-'
			i++
		}
		read := 0
		for i < len(s) {
			r, size := utf8.DecodeRuneInString(s[i:])
			d, ok := digitValue(r)
			if !ok {
				return decimal{}, false
			}
			exponent = exponent*10 + int64(d)
			if exponent > math.MaxInt32 {
				return decimal{}, false
			}
			i += size
			read++
		}
		if read == 0 {
			return decimal{}, false
		}
		if negativeExponent {
			exponent = -exponent
		}
	}
	scale := fractionDigits - exponent
	if scale > math.MaxInt32 || scale < math.MinInt32 {
		return decimal{}, false
	}
	unscaled, _ := new(big.Int).SetString(string(digits), 10)
	if negative {
		unscaled.Neg(unscaled)
	}
	return decimal{unscaled: unscaled, scale: scale}, true
}

// digitValue reads a decimal digit. BigDecimal takes any Unicode decimal digit in the basic plane, not
// only ASCII; those come in runs of ten from zero to nine, so a digit's value is its place in its run.
func digitValue(r rune) (int, bool) {
	if r >= '0' && r <= '9' {
		return int(r - '0'), true
	}
	if r > 0xFFFF || !unicode.IsDigit(r) {
		return 0, false
	}
	zero := r
	for unicode.IsDigit(zero - 1) {
		zero--
	}
	return int(r-zero) % 10, true
}

// round keeps at most precision significant digits, rounding half up as MathContext(precision) does.
func (d decimal) round(precision int) decimal {
	magnitude := new(big.Int).Abs(d.unscaled)
	drop := len(magnitude.String()) - precision
	if drop <= 0 {
		return d
	}
	divisor := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(drop)), nil)
	quotient, remainder := new(big.Int).QuoRem(magnitude, divisor, new(big.Int))
	if remainder.Lsh(remainder, 1).Cmp(divisor) >= 0 {
		quotient.Add(quotient, big.NewInt(1))
	}
	if d.unscaled.Sign() < 0 {
		quotient.Neg(quotient)
	}
	return decimal{unscaled: quotient, scale: d.scale - int64(drop)}
}

func (d decimal) stripTrailingZeros() decimal {
	unscaled, scale := new(big.Int).Set(d.unscaled), d.scale
	if unscaled.Sign() == 0 {
		return decimal{unscaled: unscaled}
	}
	ten, digit := big.NewInt(10), new(big.Int)
	for {
		quotient, remainder := new(big.Int).QuoRem(unscaled, ten, digit)
		if remainder.Sign() != 0 {
			return decimal{unscaled: unscaled, scale: scale}
		}
		unscaled, scale = quotient, scale-1
	}
}

// plain writes the number as BigDecimal.toPlainString does: no exponent, the scale's digits after the
// point.
func (d decimal) plain() string {
	if d.unscaled.Sign() == 0 && d.scale <= 0 {
		return "0"
	}
	digits := new(big.Int).Abs(d.unscaled).String()
	var body string
	switch {
	case d.scale <= 0:
		body = digits + strings.Repeat("0", int(-d.scale))
	case int64(len(digits)) > d.scale:
		point := len(digits) - int(d.scale)
		body = digits[:point] + "." + digits[point:]
	default:
		body = "0." + strings.Repeat("0", int(d.scale)-len(digits)) + digits
	}
	if d.unscaled.Sign() < 0 {
		return "-" + body
	}
	return body
}

// Reading the suite documents. They decode to plain values with json.Number for numbers, and these
// helpers read them as the reference's Json helpers do: a missing or mistyped member reads as absent.

func readSuite(path string) (map[string]any, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.UseNumber()
	var doc any
	if err := decoder.Decode(&doc); err != nil {
		return nil, fmt.Errorf("%s does not parse: %v", path, err)
	}
	suite := obj(doc)
	if suite == nil {
		return nil, fmt.Errorf("%s holds no suite object", path)
	}
	return suite, nil
}

func obj(v any) map[string]any {
	m, _ := v.(map[string]any)
	return m
}

func arr(v any) []any {
	a, _ := v.([]any)
	return a
}

func getObj(m map[string]any, key string) map[string]any { return obj(m[key]) }

func getArr(m map[string]any, key string) []any { return arr(m[key]) }

func getStr(m map[string]any, key string) (string, bool) {
	text := asText(m[key])
	if text == nil {
		return "", false
	}
	return *text, true
}

func getInt(m map[string]any, key string) (int64, bool) {
	number, ok := m[key].(json.Number)
	if !ok {
		return 0, false
	}
	if n, err := number.Int64(); err == nil {
		return n, true
	}
	f, err := number.Float64()
	if err != nil {
		return 0, false
	}
	return int64(f), true
}

func getBool(m map[string]any, key string) bool {
	flag, ok := m[key].(bool)
	return ok && flag
}

// asText is an expected value as text: a number as BigDecimal.toPlainString writes it, a boolean as
// true or false, and null as SQL NULL.
func asText(v any) *string {
	var text string
	switch x := v.(type) {
	case nil:
		return nil
	case string:
		text = x
	case bool:
		text = strconv.FormatBool(x)
	case json.Number:
		text = x.String()
		if number, ok := parseDecimal(text); ok {
			text = number.plain()
		}
	default:
		encoded, _ := json.Marshal(x)
		text = string(encoded)
	}
	return &text
}

// skipClause reports whether a case's skip clause names this runner, and with which name and reason.
func skipClause(test map[string]any) (key, reason string, skipped bool) {
	clause := getObj(test, "skip")
	for _, named := range getArr(clause, "backends") {
		name := show(asText(named))
		for _, honoured := range skipKeys {
			if strings.EqualFold(name, honoured) {
				reason, _ = getStr(clause, "reason")
				return name, reason, true
			}
		}
	}
	return "", "", false
}

// suiteFiles lists the suite files of the testkit directory FL_CORPUS names, in name order. One
// that holds none is an error, not an empty replay: the corpus was asked for, and it is not there.
func suiteFiles(corpus string) ([]string, error) {
	suites, err := filepath.Abs(filepath.Join(corpus, "suites"))
	if err != nil {
		return nil, fmt.Errorf("FL_CORPUS=%s: %v", corpus, err)
	}
	files, err := filepath.Glob(filepath.Join(suites, "*.json"))
	if err == nil && len(files) == 0 {
		err = fmt.Errorf("no suite files (*.json) in %s", suites)
	}
	if err != nil {
		return nil, fmt.Errorf("FL_CORPUS=%s: %v", corpus, err)
	}
	sort.Strings(files)
	return files, nil
}

func filterWords() []string {
	var words []string
	for _, word := range strings.Split(os.Getenv("FROSTLAKE_TESTKIT_FILTER"), ",") {
		if word = strings.TrimSpace(word); word != "" {
			words = append(words, strings.ToLower(word))
		}
	}
	return words
}

func wanted(words []string, names ...string) bool {
	if len(words) == 0 {
		return true
	}
	for _, word := range words {
		for _, name := range names {
			if strings.Contains(strings.ToLower(name), word) {
				return true
			}
		}
	}
	return false
}

// Engine and report plumbing.

// engine names the engine to replay against: the one FROSTLAKE_URL names, or one booted from
// FROSTLAKE_CLASSPATH for this run and stopped with it.
func engine() (dsn string, stop func(), err error) {
	if address := os.Getenv("FROSTLAKE_URL"); address != "" {
		dsn, err := withRequestTimeout(address)
		return dsn, func() {}, err
	}
	if classpath := os.Getenv("FROSTLAKE_CLASSPATH"); classpath != "" {
		return bootEngine(classpath)
	}
	return "", nil, errors.New("no engine: set FROSTLAKE_URL to attach to one, or FROSTLAKE_CLASSPATH to boot one")
}

// withRequestTimeout bounds each statement as the reference does, unless the DSN already says how.
func withRequestTimeout(dsn string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("FROSTLAKE_URL %q: %v", dsn, err)
	}
	query := parsed.Query()
	for key := range query {
		if strings.EqualFold(key, "timeout") {
			return dsn, nil
		}
	}
	query.Set("timeout", requestTimeout)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}

// bootEngine starts a DatabaseHttpServer on a free port, in a user.home and working directory of its
// own, so its stage files and log are this run's alone.
func bootEngine(classpath string) (string, func(), error) {
	java := "java"
	if home := os.Getenv("JAVA_HOME"); home != "" {
		java = filepath.Join(home, "bin", "java")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", nil, err
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()
	home, err := os.MkdirTemp("", "frostlake-testkit-")
	if err != nil {
		return "", nil, err
	}
	server := exec.Command(java, "-Duser.home="+home, "-cp", classpath,
		"dev.frostlake.http.DatabaseHttpServer", strconv.Itoa(port))
	server.Dir = home
	if err := server.Start(); err != nil {
		os.RemoveAll(home)
		return "", nil, fmt.Errorf("cannot start %s: %v", java, err)
	}
	exited := make(chan error, 1)
	go func() { exited <- server.Wait() }()
	stop := func() {
		server.Process.Kill()
		<-exited
		os.RemoveAll(home)
	}
	health := fmt.Sprintf("http://127.0.0.1:%d/api/health", port)
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(health); err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				dsn, err := withRequestTimeout(fmt.Sprintf("frostlake://127.0.0.1:%d", port))
				return dsn, stop, err
			}
		}
		select {
		case err := <-exited:
			os.RemoveAll(home)
			return "", nil, fmt.Errorf("the engine booted from FROSTLAKE_CLASSPATH exited during startup: %v", err)
		case <-time.After(200 * time.Millisecond):
		}
	}
	stop()
	return "", nil, fmt.Errorf("the engine booted from FROSTLAKE_CLASSPATH never became healthy on port %d", port)
}

// reachable pings the engine through the driver, giving one that is still starting a little time.
func reachable(db *sql.DB) error {
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		err = db.PingContext(ctx)
		cancel()
		if err == nil {
			return nil
		}
		time.Sleep(200 * time.Millisecond)
	}
	return err
}

// checkoutRoot is the directory holding go.mod, found upward from the working directory.
func checkoutRoot() string {
	start, err := os.Getwd()
	if err != nil {
		return "."
	}
	for dir := start; ; {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return start
		}
		dir = parent
	}
}

func writeReport(path string, cases []caseRow) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	var b strings.Builder
	b.WriteString("suite\ttest\tstatus\tfailedStep\tdetail\tms\n")
	for _, c := range cases {
		step := ""
		if c.step > 0 {
			step = strconv.Itoa(c.step)
		}
		fmt.Fprintf(&b, "%s\t%s\t%s\t%s\t%s\t%d\n", field(c.suite), field(c.test), c.status, step, field(c.detail), c.ms)
	}
	return os.WriteFile(path, []byte(b.String()), 0o644)
}

// field keeps a report cell on its line and in its column.
func field(s string) string {
	return strings.NewReplacer("\t", " ", "\r", " ", "\n", " ").Replace(s)
}

func stepSuffix(step int) string {
	if step == 0 {
		return ""
	}
	return fmt.Sprintf(", step %d", step)
}

func clip(s string, n int) string {
	s = field(s)
	if len(s) <= n {
		return s
	}
	return strings.ToValidUTF8(s[:n], "") + "…"
}
