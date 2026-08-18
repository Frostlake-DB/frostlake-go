package frostlake

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

// Test harness. TestMain boots a real DatabaseHttpServer from an engine classpath when one
// is configured; without it the unit tests still run and every test needing a server skips
// itself. Helpers shared by the other test files live here.

var (
	testDSN     string
	serverProc  *exec.Cmd
	serverReady bool
)

// RequireServer skips a test that needs a live engine when none was started.
func requireServer(t *testing.T) {
	t.Helper()
	if !serverReady {
		t.Skip("FROSTLAKE_CLASSPATH not set; integration test skipped")
	}
}

func open(t *testing.T) *sql.DB {
	t.Helper()
	requireServer(t)
	db, err := sql.Open("frostlake", testDSN)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Ping(); err != nil {
		t.Fatal(err)
	}
	return db
}

func mustExec(t *testing.T, db *sql.DB, q string) {
	t.Helper()
	if _, err := db.Exec(q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// stubDSN starts a server that answers every statement with one canned reply, for driving a
// real connection at responses a live engine would not produce.
// activeSessions asks the engine how many sessions it is holding, so a test can tell whether
// connections are being churned.
func activeSessions(t *testing.T) int {
	t.Helper()
	base := strings.Replace(testDSN, "frostlake://", "http://", 1)
	resp, err := http.Get(base + "/api/sessions")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		ActiveSessions int `json:"activeSessions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out.ActiveSessions
}

func stubDSN(t *testing.T, status int, body string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/health" {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"status":"healthy","activeSessions":0}`))
			return
		}
		w.WriteHeader(status)
		w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return "frostlake://" + strings.TrimPrefix(srv.URL, "http://")
}

// TestMain starts a Frostlake HTTP server from an already-built engine classpath. Set
// FROSTLAKE_CLASSPATH (engine classes + dependencies) and optionally JAVA_HOME; without a
// classpath the integration tests skip and the unit tests run on their own.
func TestMain(m *testing.M) {
	cp := os.Getenv("FROSTLAKE_CLASSPATH")
	if cp == "" {
		// The unit tests still have to run. Exiting here reported a pass for a suite
		// that had executed nothing at all.
		fmt.Println("FROSTLAKE_CLASSPATH not set; running unit tests only")
		os.Exit(m.Run())
	}
	javaHome := os.Getenv("JAVA_HOME")
	javaBin := "java"
	if javaHome != "" {
		javaBin = javaHome + "/bin/java"
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		panic(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()

	serverProc = exec.Command(javaBin, "-cp", cp, "dev.frostlake.http.DatabaseHttpServer", fmt.Sprintf("%d", port))
	serverProc.Stdout = os.Stderr
	serverProc.Stderr = os.Stderr
	if err := serverProc.Start(); err != nil {
		panic(err)
	}
	base := fmt.Sprintf("http://127.0.0.1:%d", port)
	for i := 0; i < 100; i++ {
		resp, err := http.Get(base + "/api/health")
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				serverReady = true
				break
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	// A server that never answers used to leave every test failing on a refused connection.
	// Say so once instead.
	if !serverReady {
		serverProc.Process.Kill()
		serverProc.Wait()
		fmt.Printf("frostlake server never became healthy on port %d; check FROSTLAKE_CLASSPATH and JAVA_HOME\n", port)
		os.Exit(1)
	}
	testDSN = fmt.Sprintf("frostlake://127.0.0.1:%d", port)

	code := m.Run()
	serverProc.Process.Kill()
	serverProc.Wait()
	os.Exit(code)
}
