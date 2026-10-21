package auth

import (
	"context"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestPoolViewMatchesLiveAdmission(t *testing.T) {
	m, e := admissionFixture(t, 2, 1, false, false)
	defer close(e.release)
	cfg := &config.Config{}
	cfg.Routing.ResetSoonest.FiveHourThreshold = .7
	cfg.Routing.ResetSoonest.SevenDayThreshold = .8
	cfg.Routing.ResetSoonest.MaxInflightPerCredential = 1
	m.SetConfig(cfg)
	auths := m.List()
	m.tmxAdmission.counter(auths[0].ID).Add(1)
	defer m.tmxAdmission.counter(auths[0].ID).Add(-1)
	ctx := m.PoolSelectionContext(context.Background())
	view := admissionView(ctx)
	if view.five != .7 || view.seven != .8 || view.cap != 1 || view.count(auths[0].ID) != 1 {
		t.Fatalf("pool view %+v", view)
	}
	selector := &ResetSoonestSelector{}
	displayed, err := selector.Pick(ctx, "claude", "claude-sonnet-5", executor.Options{}, auths)
	if err != nil {
		t.Fatal(err)
	}
	live, err := m.tmxPickWithAdmission(context.Background(), func(ctx context.Context) (refreshSelection, error) {
		a, err := selector.Pick(ctx, "claude", "claude-sonnet-5", executor.Options{}, auths)
		return refreshSelection{auth: a}, err
	})
	if err != nil || live.auth.ID != displayed.ID || displayed.ID == auths[0].ID {
		t.Fatalf("display/live mismatch: %v %v %v", displayed, live.auth, err)
	}
}
