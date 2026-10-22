package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/http"
)

// BadRequestError marks a whole request the client has to fix (HTTP 400). Any other error from an
// ingest call is the server's (HTTP 500): sending the same request again may succeed.
type BadRequestError struct{ Msg string }

func (e *BadRequestError) Error() string { return e.Msg }

func badRequest(format string, args ...any) error {
	return &BadRequestError{Msg: fmt.Sprintf(format, args...)}
}

// IsBadRequest reports whether err is a BadRequestError.
func IsBadRequest(err error) bool {
	var b *BadRequestError
	return errors.As(err, &b)
}

// RecordResult is one record's outcome in a threads or commits batch, in request order.
type RecordResult struct {
	Index  int    `json:"index"`
	ID     string `json:"id,omitempty"`
	Status string `json:"status"` // created | updated | unchanged | error
	Error  string `json:"error,omitempty"`
}

// ThreadBatchResult reports what a threads or commits batch changed. One bad record never blocks
// the rest: it gets status "error" in Results and the others are written.
type ThreadBatchResult struct {
	Created   int            `json:"created"`
	Updated   int            `json:"updated"`
	Unchanged int            `json:"unchanged"`
	Errors    int            `json:"errors"`
	Results   []RecordResult `json:"results"`
}

func (r *ThreadBatchResult) add(it RecordResult) {
	switch it.Status {
	case "created":
		r.Created++
	case "updated":
		r.Updated++
	case "unchanged":
		r.Unchanged++
	default:
		r.Errors++
	}
	r.Results = append(r.Results, it)
}

// HTTPStatus is 200 when every record went through and 207 when some were refused.
func (r *ThreadBatchResult) HTTPStatus() int {
	if r.Errors > 0 {
		return http.StatusMultiStatus
	}
	return http.StatusOK
}

// upsertRecord runs one record's upsert (a query returning xmax = 0, or no row when unchanged) and
// its journal entry under a savepoint, so a record the database refuses is rolled back alone.
// A refused record returns an "error" result; a server-side failure returns err (abort the batch).
func (a *Archive) upsertRecord(ctx context.Context, tx *sql.Tx, i int, id, kind, query string, args ...any) (RecordResult, error) {
	res := RecordResult{Index: i, ID: id}
	if _, err := tx.ExecContext(ctx, `SAVEPOINT tmx_record`); err != nil {
		return res, err
	}
	var inserted bool
	errUp := tx.QueryRowContext(ctx, query, args...).Scan(&inserted)
	switch {
	case errors.Is(errUp, sql.ErrNoRows):
		res.Status = "unchanged"
	case errUp != nil:
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT tmx_record`); err != nil {
			return res, err
		}
		if !isDataError(errUp) {
			return res, fmt.Errorf("%s %q: %w", kind, id, errUp)
		}
		res.Status, res.Error = "error", errUp.Error()
		return res, nil
	case inserted:
		res.Status = "created"
	default:
		res.Status = "updated"
	}
	if res.Status != "unchanged" {
		if err := a.journal(ctx, tx, kind, id); err != nil {
			return res, fmt.Errorf("analytics journal: %w", err)
		}
	}
	if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT tmx_record`); err != nil {
		return res, err
	}
	return res, nil
}
