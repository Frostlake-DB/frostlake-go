package frostlake

import "context"

// multiStatementKey carries a per-call statement count on a context.
type multiStatementKey struct{}

// WithMultiStatement returns a context that lets ONE call carry num statements.
//
// A request holds a single statement unless it says otherwise, so a script of several is refused
// unless the caller asks for it. num is how many statements the next query on this context carries;
// 0 allows any number. The count rides on that request alone and does not change the session's
// MULTI_STATEMENT_COUNT, so anything else sharing the connection is unaffected and there is nothing
// to restore afterwards.
//
//	ctx := frostlake.WithMultiStatement(context.Background(), 2)
//	rows, err := db.QueryContext(ctx, "SELECT 1; SELECT 2")
func WithMultiStatement(ctx context.Context, num int) context.Context {
	return context.WithValue(ctx, multiStatementKey{}, num)
}

// multiStatementCount is the count a context declares, or nil when it declares none.
func multiStatementCount(ctx context.Context) *int {
	if ctx == nil {
		return nil
	}
	num, ok := ctx.Value(multiStatementKey{}).(int)
	if !ok {
		return nil
	}
	return &num
}
