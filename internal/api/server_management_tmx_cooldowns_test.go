package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/cooldowns"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// TestTMXCooldownRoutesContract pins GET /tmx/cooldowns/history and /tmx/cooldowns/stats. The
// data part runs against a real Postgres when TMX_COOLDOWNS_TEST_DSN is set.
func TestTMXCooldownRoutesContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cooldowns.SetDefaultForTest(nil)
	t.Cleanup(func() { cooldowns.SetDefaultForTest(nil) })
	r := gin.New()
	registerTMXCooldownRoutes(r.Group("/v8/management"))
	call := func(path string) (int, map[string]any) {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		var out map[string]any
		_ = json.Unmarshal(w.Body.Bytes(), &out)
		return w.Code, out
	}

	for _, bad := range []string{"?from=yesterday", "?to=nope", "?from=2026-10-22T00:00:00Z&to=2026-10-21T00:00:00Z",
		"?from=2020-01-01T00:00:00Z&to=2026-01-01T00:00:00Z", "?limit=0", "?limit=99999"} {
		if code, out := call("/v8/management/tmx/cooldowns/history" + bad); code != 400 || out["error"] == nil {
			t.Errorf("%s: %d %v", bad, code, out)
		}
	}
	if code, out := call("/v8/management/tmx/cooldowns/history"); code != 200 || out["enabled"] != false || out["events"] == nil || out["windows"] == nil {
		t.Fatalf("disabled history: %d %v", code, out)
	}
	if code, out := call("/v8/management/tmx/cooldowns/stats"); code != 200 || out["enabled"] != false {
		t.Fatalf("disabled stats: %d %v", code, out)
	}

	dsn := os.Getenv("TMX_COOLDOWNS_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_COOLDOWNS_TEST_DSN not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("tmx_cdapi_%d", time.Now().UnixNano())
	t.Setenv("TMX_COOLDOWNS_SPOOL", t.TempDir())
	store, err := cooldowns.Open(context.Background(), db, schema, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		store.Close()
		_, _ = db.Exec("DROP SCHEMA " + schema + " CASCADE")
		_ = db.Close()
	})
	cooldowns.SetDefaultForTest(store)

	now := time.Now().UTC().Truncate(time.Second)
	store.ObserveCooldowns(coreauth.CooldownReport{Seq: 1, At: now.Add(-time.Hour), AuthID: "a", Windows: []coreauth.CooldownWindowState{
		{AuthID: "a", AuthIndex: "ia", AuthLabel: "a@x", Provider: "claude", Model: "opus", Reason: "quota", HTTPStatus: 429, RetryAt: now.Add(time.Hour)}}})
	store.ObserveCooldowns(coreauth.CooldownReport{Seq: 2, At: now.Add(-30 * time.Minute), AuthID: "a"})
	store.ObserveFailure(coreauth.FailureReport{At: now.Add(-time.Hour), AuthID: "a", Provider: "claude", Model: "opus", HTTPStatus: 429})
	deadline := time.Now().Add(10 * time.Second)
	for store.Stats()["written"] < 3 && time.Now().Before(deadline) {
		_ = store.FlushContext(context.Background())
		time.Sleep(50 * time.Millisecond)
	}

	q := url.Values{"from": {now.Add(-2 * time.Hour).Format(time.RFC3339)}, "to": {now.Add(time.Minute).Format(time.RFC3339)}, "auth_id": {"a"}}
	code, out := call("/v8/management/tmx/cooldowns/history?" + q.Encode())
	if code != 200 || out["enabled"] != true {
		t.Fatalf("history: %d %v", code, out)
	}
	for _, k := range []string{"from", "to", "events", "windows", "windows_truncated", "next_cursor", "stats"} {
		if _, ok := out[k]; !ok {
			t.Errorf("history missing %q", k)
		}
	}
	events := out["events"].([]any)
	if len(events) != 3 {
		t.Fatalf("events: %v", events)
	}
	ev := events[0].(map[string]any) // newest first: the end
	for _, k := range []string{"id", "event_key", "ts", "recorded_at", "kind", "source", "cooldown_id", "auth_id", "model", "started_at", "ended_at", "end_reason"} {
		if _, ok := ev[k]; !ok {
			t.Errorf("event missing %q: %v", k, ev)
		}
	}
	if ev["kind"] != "cooldown_end" || ev["end_reason"] != "cleared" || ev["source"] != "proxy" {
		t.Fatalf("end event: %v", ev)
	}
	windows := out["windows"].([]any)
	if len(windows) != 1 {
		t.Fatalf("windows: %v", windows)
	}
	w := windows[0].(map[string]any)
	for _, k := range []string{"cooldown_id", "auth_id", "auth_index", "auth_label", "provider", "model", "reason", "http_status",
		"started_at", "retry_at", "ended_at", "end_reason", "duration_ms", "open", "updates", "source"} {
		if _, ok := w[k]; !ok {
			t.Errorf("window missing %q: %v", k, w)
		}
	}
	if w["open"] != false || w["duration_ms"].(float64) != float64(30*time.Minute/time.Millisecond) || w["cooldown_id"] != ev["cooldown_id"] {
		t.Fatalf("window: %v", w)
	}

	q.Set("limit", "1")
	_, p1 := call("/v8/management/tmx/cooldowns/history?" + q.Encode())
	cursor, _ := p1["next_cursor"].(string)
	if len(p1["events"].([]any)) != 1 || cursor == "" {
		t.Fatalf("page 1: %v", p1)
	}
	q.Set("cursor", cursor)
	q.Set("kind", "rate_limited_429,cooldown_start")
	_, p2 := call("/v8/management/tmx/cooldowns/history?" + q.Encode())
	if len(p2["events"].([]any)) != 1 || len(p2["windows"].([]any)) != 0 {
		t.Fatalf("page 2: %v", p2)
	}
	q.Set("cursor", "%%%")
	if code, _ = call("/v8/management/tmx/cooldowns/history?" + q.Encode()); code != 400 {
		t.Fatalf("bad cursor: %d", code)
	}
	if code, out = call("/v8/management/tmx/cooldowns/stats"); code != 200 || out["enabled"] != true ||
		out["stats"].(map[string]any)["written"].(float64) < 3 {
		t.Fatalf("stats: %d %v", code, out)
	}
}
