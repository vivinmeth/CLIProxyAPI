package cooldowns

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// End to end with the real auth manager: every result is observed (a success reports an empty
// cooldown set, which writes nothing), a 429 writes its point event and a window start, and
// clearing the cooldown writes the window's end.
func TestManagerResultsBecomeEvents(t *testing.T) {
	db, schema := testDB(t)
	ctx := context.Background()
	manager := auth.NewManager(nil, nil, nil)
	a := &auth.Auth{ID: uuid.NewString() + "-e2e", Provider: "claude", Label: "me@example", Attributes: map[string]string{"api_key": "k"}}
	reg := registry.GetGlobalRegistry()
	reg.RegisterClient(a.ID, "claude", []*registry.ModelInfo{{ID: "claude-x"}})
	t.Cleanup(func() { reg.UnregisterClient(a.ID) })
	if _, err := manager.Register(ctx, a); err != nil {
		t.Fatal(err)
	}
	s, err := Open(ctx, db, schema, manager)
	if err != nil {
		t.Fatal(err)
	}
	auth.SetCooldownObserver(s) // what Start does after Open
	t.Cleanup(s.Close)

	for i := 0; i < 5; i++ {
		manager.MarkResult(ctx, auth.Result{AuthID: a.ID, Provider: "claude", Model: "claude-x", Success: true})
	}
	waitFor(t, s, "successes observed", func() bool { return s.Stats()["observed"] >= 5 })
	if w := s.Stats()["written"]; w != 0 {
		t.Fatalf("successes wrote %d events", w)
	}

	wait := 90 * time.Second
	manager.MarkResult(ctx, auth.Result{AuthID: a.ID, Provider: "claude", Model: "claude-x",
		RetryAfter: &wait, Error: &auth.Error{HTTPStatus: http.StatusTooManyRequests, Message: "rate limited"}})
	waitFor(t, s, "429 written", func() bool {
		return countKind(t, db, schema, KindRateLimited) == 1 && countKind(t, db, schema, KindStart) >= 1
	})
	var provider, index string
	if err = db.QueryRow(`SELECT provider, auth_index FROM `+schema+`.cooldown_events WHERE kind = $1 AND model = 'claude-x'`,
		KindStart).Scan(&provider, &index); err != nil || provider != "claude" || index == "" {
		t.Fatalf("start row provider=%q auth_index=%q (%v)", provider, index, err)
	}

	if _, _, err = manager.ResetQuota(ctx, a.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, s, "window end", func() bool { return countKind(t, db, schema, KindEnd) >= 1 })
	if st := s.Stats(); st["dropped"] != 0 || st["rejected"] != 0 || st["open_windows"] != 0 {
		t.Fatalf("stats after clear: %v", st)
	}
}
