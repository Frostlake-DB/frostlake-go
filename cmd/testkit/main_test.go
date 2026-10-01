package main

import (
	"os"
	"testing"
)

// TestCorpus replays the testkit corpus inside go test ./..., against the engine the command itself
// would use. The replay takes minutes, so it runs only when FL_CORPUS names the testkit directory.
func TestCorpus(t *testing.T) {
	corpus := os.Getenv("FL_CORPUS")
	if corpus == "" {
		t.Skip(noCorpus)
	}
	if _, err := suiteFiles(corpus); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("FROSTLAKE_URL") == "" && os.Getenv("FROSTLAKE_CLASSPATH") == "" {
		t.Skip("FROSTLAKE_URL and FROSTLAKE_CLASSPATH not set; no engine to replay the testkit corpus against")
	}
	if code := run(); code != 0 {
		t.Fatalf("the testkit corpus replay exited %d: a case failed or errored, or the replay could not start (see above)", code)
	}
}
