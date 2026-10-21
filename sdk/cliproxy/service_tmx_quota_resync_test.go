package cliproxy

import (
	"context"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
)

// tmx (load test B3): boot order is store load → quota poller (first poll) → watcher bootstrap,
// which re-syncs every auth file. The bootstrap must not discard the poll, or the account the
// usage API reports at 97% is routed first until passive headers arrive.
func TestServiceWatcherBootstrapKeepsBootPolledQuota(t *testing.T) {
	service := &Service{cfg: &config.Config{}, coreManager: coreauth.NewManager(nil, &coreauth.ResetSoonestSelector{}, nil)}
	fileAuth := func(id string) *coreauth.Auth {
		return &coreauth.Auth{ID: id, Provider: "claude", Status: coreauth.StatusActive, Metadata: map[string]any{"type": "claude", "account_uuid": "account-" + id, "access_token": "synthetic"}}
	}
	ids := []string{"resync-01", "resync-02", "resync-03"}
	for _, id := range ids {
		t.Cleanup(func() { GlobalModelRegistry().UnregisterClient(id) })
		service.applyCoreAuthAddOrUpdate(context.Background(), fileAuth(id))
	}
	now := time.Now().UTC().Truncate(time.Second)
	for i, id := range ids {
		used, weekly := 10.0, 10.0
		if id == "resync-01" {
			used = 97
		}
		if !service.coreManager.ApplyQuotaObservation(id, "claude", []coreauth.QuotaWindowObservation{
			{Window: "5h", UsedPercent: &used, ResetsAt: now.Add(2 * time.Hour), ObservedAt: now},
			{Window: "7d", UsedPercent: &weekly, ResetsAt: now.Add(time.Duration(24*(i+1)) * time.Hour), ObservedAt: now},
		}) {
			t.Fatalf("poll for %s not applied", id)
		}
	}
	for _, id := range ids {
		service.applyCoreAuthAddOrUpdate(context.Background(), fileAuth(id))
	}
	m := service.coreManager
	got, err := (&coreauth.ResetSoonestSelector{}).Pick(m.PoolSelectionContext(context.Background()), "claude", "", cliproxyexecutor.Options{}, m.List())
	if err != nil {
		t.Fatal(err)
	}
	if got.ID == "resync-01" {
		t.Fatal("watcher bootstrap discarded the polled 97% reading: resync-01 picked first")
	}
}
