package auth

import (
	"context"
	"testing"
	"time"

	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestSessionBindingSnapshotRestorePreservesPicksAndAliases(t *testing.T) {
	ctx := context.Background()
	original := NewSessionAffinitySelector(&FillFirstSelector{})
	defer original.Stop()
	original.cache.SetAliases("b", "claude::claude:session-one::opus", "claude::claude:session-alias::opus")
	manager := NewManager(nil, original, nil)
	bindings := manager.SnapshotSessionBindings()
	if len(bindings) != 1 {
		t.Fatalf("snapshot groups=%d", len(bindings))
	}
	fresh := NewSessionAffinitySelector(&FillFirstSelector{})
	defer fresh.Stop()
	restored := NewManager(nil, fresh, nil)
	a, b := &Auth{ID: "a", Provider: "claude"}, &Auth{ID: "b", Provider: "claude"}
	for _, credential := range []*Auth{a, b} {
		if _, err := restored.Register(ctx, credential); err != nil {
			t.Fatal(err)
		}
	}
	restored.RestoreSessionBindings(bindings)
	for _, session := range []string{"session-one", "session-alias"} {
		picked, err := fresh.Pick(ctx, "claude", "opus", executor.Options{Headers: map[string][]string{"X-Claude-Code-Session-Id": {session}}}, []*Auth{a, b})
		if err != nil || picked.ID != "b" {
			t.Fatalf("restored pick=%v err=%v", picked, err)
		}
	}
	if restored.BoundSessionCounts()["b"] != 1 {
		t.Fatalf("aliases counted as separate sessions: %v", restored.BoundSessionCounts())
	}
}

func TestSessionBindingRestoreFiltersExpiredDisabledAndUnknown(t *testing.T) {
	selector := NewSessionAffinitySelector(nil)
	defer selector.Stop()
	m := NewManager(nil, selector, nil)
	_, err := m.Register(context.Background(), &Auth{ID: "disabled", Provider: "claude", Disabled: true})
	if err != nil {
		t.Fatal(err)
	}
	m.RestoreSessionBindings([]SessionBinding{
		{AuthID: "disabled", Aliases: []string{"disabled"}, ExpiresAt: time.Now().Add(time.Hour)},
		{AuthID: "unknown", Aliases: []string{"unknown"}, ExpiresAt: time.Now().Add(time.Hour)},
		{AuthID: "unknown", Aliases: []string{"expired"}, ExpiresAt: time.Now().Add(-time.Hour)},
	})
	if got := m.SnapshotSessionBindings(); len(got) != 0 {
		t.Fatalf("invalid bindings restored: %+v", got)
	}
}
