package sessionbindings

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

type deadlineDriver struct{}
type deadlineConn struct{}

func (deadlineDriver) Open(string) (driver.Conn, error)  { return deadlineConn{}, nil }
func (deadlineConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unused") }
func (deadlineConn) Close() error                        { return nil }
func (deadlineConn) Begin() (driver.Tx, error)           { return nil, errors.New("unused") }
func boundedBootContext(ctx context.Context) error {
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Second {
		return errors.New("boot query has no five-second deadline")
	}
	return context.DeadlineExceeded
}
func (deadlineConn) ExecContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Result, error) {
	return nil, boundedBootContext(ctx)
}
func (deadlineConn) QueryContext(ctx context.Context, _ string, _ []driver.NamedValue) (driver.Rows, error) {
	return nil, boundedBootContext(ctx)
}
func init() { sql.Register("tmx-session-deadline", deadlineDriver{}) }

func TestBootOpenAndRestoreBoundDatabaseCalls(t *testing.T) {
	db, err := sql.Open("tmx-session-deadline", "")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	if _, err = Open(context.Background(), db, "test"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Open: %v", err)
	}
	w := &Worker{store: &Store{db: db, table: "bindings"}}
	if err = w.restore(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("restore: %v", err)
	}
}
