package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/pool"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestTMXPoolHistoryDisabled(t *testing.T) {
	pool.SetDefaultRecorderForTest(nil)
	r := gin.New()
	r.GET("/h", tmxPoolHistory)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/h", nil))
	if rec.Code != 200 || rec.Body.String() != `{"enabled":false,"items":[],"next_after_id":0}` {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
}

// TestTMXPoolHistoryContract needs TMX_REQUESTLOG_TEST_DSN.
func TestTMXPoolHistoryContract(t *testing.T) {
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_REQUESTLOG_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("tmxpoolapi_%d", time.Now().UnixNano())
	if _, err = db.Exec(`CREATE SCHEMA ` + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.SetDefaultRecorderForTest(nil)
		_, _ = db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		_ = db.Close()
	})
	m := auth.NewManager(nil, &auth.ResetSoonestSelector{}, nil)
	if _, err = m.Register(context.Background(), &auth.Auth{ID: "claude-carol@example.com.json", Provider: "claude", Status: auth.StatusActive}); err != nil {
		t.Fatal(err)
	}
	recorder, err := pool.NewRecorder(context.Background(), db, schema, func(ctx context.Context) pool.Snapshot {
		return tmxPoolBuild(ctx, nil, m, "")
	}, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err = recorder.Tick(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	pool.SetDefaultRecorderForTest(recorder)
	r := gin.New()
	r.GET("/h", tmxPoolHistory)

	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/h?limit=1", nil))
	var body struct {
		Enabled     bool               `json:"enabled"`
		Items       []pool.HistoryRow  `json:"items"`
		NextAfterID int64              `json:"next_after_id"`
		Stats       pool.RecorderStats `json:"stats"`
	}
	if err = json.Unmarshal(rec.Body.Bytes(), &body); err != nil || rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body.String())
	}
	if !body.Enabled || len(body.Items) != 1 || body.NextAfterID != body.Items[0].ID || body.Stats.Snapshots != 2 || body.Stats.IntervalSeconds != 300 {
		t.Fatalf("page = %s", rec.Body.String())
	}
	if body.Items[0].AuthID != "claude-c•••@e•••.com.json" {
		t.Fatalf("auth_id not masked: %s", body.Items[0].AuthID)
	}
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", fmt.Sprintf("/h?after_id=%d&auth_id=%s", body.NextAfterID, "claude-c%E2%80%A2%E2%80%A2%E2%80%A2@e%E2%80%A2%E2%80%A2%E2%80%A2.com.json"), nil))
	if err = json.Unmarshal(rec.Body.Bytes(), &body); err != nil || len(body.Items) != 1 || body.NextAfterID != 0 {
		t.Fatalf("page 2 = %s", rec.Body.String())
	}
	for _, bad := range []string{"/h?after_id=x", "/h?from=y", "/h?from=20&to=10"} {
		rec = httptest.NewRecorder()
		r.ServeHTTP(rec, httptest.NewRequest("GET", bad, nil))
		if rec.Code != 400 {
			t.Fatalf("%s: %d", bad, rec.Code)
		}
	}
}
