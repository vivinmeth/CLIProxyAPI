package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/pool"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestTMXPoolContract(t *testing.T) {
	m := auth.NewManager(nil, &auth.ResetSoonestSelector{}, nil)
	now := time.Now()
	for _, a := range []*auth.Auth{
		{ID: "pool-a", Provider: "claude", Label: "claude-john@example.dev.json", Status: auth.StatusActive, Metadata: map[string]any{"access_token": "must-not-appear", "email": "private@example.dev", "plan_type": "max_20x"}, Quota: auth.QuotaState{ObservedAt: now, Signals: map[string]string{"Anthropic-Ratelimit-Unified-7d-Reset": strconv.FormatInt(now.Add(time.Hour).Unix(), 10), "Anthropic-Ratelimit-Unified-5h-Utilization": "0.3"}}},
		{ID: "pool-b", Provider: "claude", Status: auth.StatusError, Unavailable: true, NextRetryAfter: now.Add(time.Hour), Quota: auth.QuotaState{Exceeded: true, Reason: "credential_quota", NextRecoverAt: now.Add(time.Hour)}},
		{ID: "pool-disabled", Provider: "claude", Disabled: true},
	} {
		if _, err := m.Register(context.Background(), a); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(a.ID, "claude", []*registry.ModelInfo{{ID: "pool-model", Created: now.Unix()}})
		defer registry.GetGlobalRegistry().UnregisterClient(a.ID)
	}
	r := gin.New()
	registerTMXManagementRoutes(r.Group("/v8/management"), nil, m)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/v8/management/tmx/pool?model=pool-model", nil))
	if rec.Code != 200 {
		t.Fatalf("status=%d %s", rec.Code, rec.Body.String())
	}
	raw := rec.Body.String()
	for _, forbidden := range []string{"must-not-appear", "private@example.dev", "john@example.dev", "access_token", "metadata"} {
		if strings.Contains(raw, forbidden) {
			t.Fatalf("sensitive field %q exposed", forbidden)
		}
	}
	var result pool.Snapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Credentials) != 3 {
		t.Fatalf("credentials=%d", len(result.Credentials))
	}
	a := result.Credentials[0]
	if a.Label != "claude-j•••@e•••.dev.json" || a.PickRank == nil || *a.PickRank != 1 || a.PlanType != "max_20x" || len(a.Windows) != 2 || a.InFlight != 0 {
		t.Fatalf("first=%+v", a)
	}
	b := result.Credentials[1]
	if b.Reason != "quota" || !b.Unavailable || b.NextRetryAfter == nil || len(b.Cooldowns) == 0 || b.PickRank != nil {
		t.Fatalf("cooling=%+v", b)
	}
	if result.Credentials[2].Enabled || result.Credentials[2].PickRank != nil {
		t.Fatal("disabled credential eligible")
	}
}
