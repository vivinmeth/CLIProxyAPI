package api

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/durability"
)

func TestTMXDurabilityContract(t *testing.T) {
	gin.SetMode(gin.TestMode)
	at := time.Date(2026, 10, 22, 5, 0, 0, 0, time.UTC)
	durability.Register("test-a", func() durability.Stats {
		return durability.Stats{Queued: 1, SpooledPending: 2, SpooledBytes: 300, Replayed: 4, Deadlettered: 1,
			DeadletterTotal: 3, LastError: "boom", LastErrorAt: &at, SpoolDir: "/x/a"}
	})
	durability.Register("test-b", func() durability.Stats { return durability.Stats{Dropped: 5} })
	t.Cleanup(func() { durability.Register("test-a", nil); durability.Register("test-b", nil) })
	r := gin.New()
	registerTMXDurabilityRoutes(r.Group("/v8/management"))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/v8/management/tmx/durability", nil))
	if w.Code != 200 {
		t.Fatalf("status %d", w.Code)
	}
	var out struct {
		Lossless bool                      `json:"lossless"`
		Totals   map[string]int64          `json:"totals"`
		Writers  map[string]map[string]any `json:"writers"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if out.Lossless || out.Totals["dropped"] != 5 || out.Totals["spooled_pending"] != 2 || out.Totals["deadletter_total"] != 3 {
		t.Fatalf("totals %+v lossless %v", out.Totals, out.Lossless)
	}
	a := out.Writers["test-a"]
	for _, k := range []string{"queued", "spooled", "spooled_pending", "spooled_bytes", "replayed", "dropped",
		"deadlettered", "deadletter_total", "last_error", "last_error_at", "spool_dir"} {
		if _, ok := a[k]; !ok {
			t.Errorf("writer missing %q", k)
		}
	}
	if a["last_error"] != "boom" || a["last_error_at"] != "2026-10-22T05:00:00Z" {
		t.Fatalf("writer a = %v", a)
	}
	if b := out.Writers["test-b"]; b["last_error_at"] != nil || b["last_error"] != "" {
		t.Fatalf("writer b = %v", b)
	}
}
