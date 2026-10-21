package sessionbindings

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestPostgresSessionBindingsRestartAndDelta(t *testing.T) {
	dsn := os.Getenv("TMX_SESSION_BINDINGS_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_SESSION_BINDINGS_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	schema := fmt.Sprintf("sessions_test_%d", time.Now().UnixNano())
	ctx := context.Background()
	if _, err = db.ExecContext(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`) }()
	s, err := Open(ctx, db, schema)
	if err != nil {
		t.Fatal(err)
	}
	bindings := []auth.SessionBinding{{AuthID: "b", Aliases: []string{"one", "alias"}, ExpiresAt: time.Now().Add(time.Hour)}}
	if err = s.Save(ctx, bindings); err != nil {
		t.Fatal(err)
	}
	var before, after time.Time
	if err = db.QueryRow(`SELECT updated_at FROM ` + s.table).Scan(&before); err != nil {
		t.Fatal(err)
	}
	if err = s.Save(ctx, bindings); err != nil {
		t.Fatal(err)
	}
	if err = db.QueryRow(`SELECT updated_at FROM ` + s.table).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if !after.Equal(before) {
		t.Fatal("unchanged entry was rewritten")
	}
	fresh, err := Open(ctx, db, schema)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := fresh.Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != 1 || loaded[0].AuthID != "b" || len(loaded[0].Aliases) != 2 {
		t.Fatalf("restored %+v", loaded)
	}
	// Worker restore populates previous keys so invalidation is durable too.
	fresh.previous["one"] = "loaded"
	if err = fresh.Save(ctx, nil); err != nil {
		t.Fatal(err)
	}
	loaded, err = fresh.Load(ctx)
	if err != nil || len(loaded) != 0 {
		t.Fatalf("delete: %+v %v", loaded, err)
	}
}
