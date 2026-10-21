package quota

import (
	"context"
	"fmt"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"testing"
	"time"
)

func TestQuotaRoutingSeedAndPoll(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	f := &fakeFetcher{auths: []*coreauth.Auth{{ID: "seed-a", Provider: "claude", Status: coreauth.StatusActive}, {ID: "seed-b", Provider: "claude", Status: coreauth.StatusActive}}, calls: map[string]int{}, status: 200}
	s := openTestService(t, f, &now)
	_, err := s.db.Exec(`DELETE FROM ` + s.table)
	if err != nil {
		t.Fatal(err)
	}
	for i, a := range f.auths {
		reset := now.Add(time.Duration(6-i*5) * time.Hour)
		_, err = s.db.Exec(`INSERT INTO `+s.table+` (ts,auth_id,provider,"window",used_pct,resets_at,source) VALUES ($1,$2,'claude','7d',20,$3,'polled')`, now.Add(-time.Minute), a.ID, reset)
		if err != nil {
			t.Fatal(err)
		}
	}
	m := coreauth.NewManager(nil, &coreauth.ResetSoonestSelector{}, nil)
	for _, a := range f.auths {
		if _, err = m.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	restored, err := newService(context.Background(), s.db, "tmxq", f, m)
	if err != nil {
		t.Fatal(err)
	}
	selector := &coreauth.ResetSoonestSelector{}
	selected, err := selector.Pick(context.Background(), "claude", "", executor.Options{}, m.List())
	if err != nil || selected.ID != "seed-b" {
		t.Fatalf("boot did not seed weekly order: %v %v", selected, err)
	}
	// Successful polling updates the live routing view immediately.
	f.usage = fmt.Sprintf(`{"seven_day":{"utilization":30,"resets_at":%q}}`, now.Add(30*time.Minute).Format(time.RFC3339))
	if _, _, err = restored.PollNow(context.Background(), "seed-a"); err != nil {
		t.Fatal(err)
	}
	selected, err = selector.Pick(context.Background(), "claude", "", executor.Options{}, m.List())
	if err != nil || selected.ID != "seed-a" {
		t.Fatalf("poll did not feed routing: %v %v", selected, err)
	}
	m.ClearQuotaSignals("seed-a")
	restored.seedRouting()
	a, _ := m.GetByID("seed-a")
	if len(a.Quota.Signals) != 0 {
		t.Fatal("tick resurrected snapshot after reset clearing")
	}
}

// Load test B3: the boot seed runs before the watcher bootstrap re-syncs every auth file. The
// re-sync (a plain Update of the same account) must not discard the seeded readings, which the
// seed does not re-apply within the same registration.
func TestQuotaBootSeedSurvivesWatcherResync(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Second)
	f := &fakeFetcher{auths: []*coreauth.Auth{{ID: "resync-a-hot", Provider: "claude", Status: coreauth.StatusActive}, {ID: "resync-b-cool", Provider: "claude", Status: coreauth.StatusActive}}, calls: map[string]int{}, status: 200}
	s := openTestService(t, f, &now)
	if _, err := s.db.Exec(`DELETE FROM ` + s.table); err != nil {
		t.Fatal(err)
	}
	// resync-a-hot has the soonest weekly reset but is at 97% of its 5h window.
	rows := []struct {
		id, window string
		used       float64
		reset      time.Duration
	}{
		{"resync-a-hot", "7d", 20, time.Hour}, {"resync-a-hot", "5h", 97, 2 * time.Hour},
		{"resync-b-cool", "7d", 20, 120 * time.Hour}, {"resync-b-cool", "5h", 10, 2 * time.Hour},
	}
	for _, r := range rows {
		if _, err := s.db.Exec(`INSERT INTO `+s.table+` (ts,auth_id,provider,"window",used_pct,resets_at,source) VALUES ($1,$2,'claude',$3,$4,$5,'polled')`, now.Add(-time.Minute), r.id, r.window, r.used, now.Add(r.reset)); err != nil {
			t.Fatal(err)
		}
	}
	m := coreauth.NewManager(nil, &coreauth.ResetSoonestSelector{}, nil)
	for _, a := range f.auths {
		if _, err := m.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	restored, err := newService(context.Background(), s.db, "tmxq", f, m)
	if err != nil {
		t.Fatal(err)
	}
	pick := func() string {
		t.Helper()
		selected, errPick := (&coreauth.ResetSoonestSelector{}).Pick(m.PoolSelectionContext(context.Background()), "claude", "", executor.Options{}, m.List())
		if errPick != nil {
			t.Fatal(errPick)
		}
		return selected.ID
	}
	if got := pick(); got != "resync-b-cool" {
		t.Fatalf("boot seed: picked %s, want resync-b-cool", got)
	}
	for _, a := range f.auths {
		if _, err = m.Update(context.Background(), &coreauth.Auth{ID: a.ID, Provider: a.Provider, Status: coreauth.StatusActive}); err != nil {
			t.Fatal(err)
		}
	}
	restored.seedRouting()
	if got := pick(); got != "resync-b-cool" {
		t.Fatalf("after the watcher re-sync picked %s: the seeded 97%% reading was lost", got)
	}
}
