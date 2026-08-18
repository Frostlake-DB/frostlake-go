package frostlake

import "fmt"

// Error is the failure the engine itself reported: the statement compiled badly, referenced
// something that does not exist, or failed while running. Callers that need to tell a bad
// statement from a bad network reach for this with errors.As.
//
//	var ferr *frostlake.Error
//	if errors.As(err, &ferr) {
//	    log.Printf("engine refused %q: %s", ferr.Statement, ferr.Message)
//	}
type Error struct {
	// Message is the engine's own wording, unmodified.
	Message string
	// Statement is the SQL as it was sent, after client-side parameter substitution.
	Statement string
	// Status is the HTTP status the answer arrived with. Statement failures are reported
	// with 200; 500 means the engine threw while handling the request.
	Status int
}

func (e *Error) Error() string { return e.Message }

// TransportError is a failure that never became an answer from the engine: the host refused
// the connection, the context expired, a proxy replied instead, or the body was not a
// Frostlake response at all. Err carries the underlying cause where there is one.
type TransportError struct {
	// Endpoint is the URL that was called.
	Endpoint string
	// Status is the HTTP status, or 0 when the request never completed.
	Status int
	// Body is a bounded excerpt of whatever came back, empty when nothing did.
	Body string
	// Err is the underlying error, if the failure came from the HTTP client.
	Err error
}

func (e *TransportError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("frostlake: %s: %v", e.Endpoint, e.Err)
	}
	return fmt.Sprintf("frostlake: %s answered HTTP %d with a body that is not a Frostlake response: %s",
		e.Endpoint, e.Status, e.Body)
}

func (e *TransportError) Unwrap() error { return e.Err }
