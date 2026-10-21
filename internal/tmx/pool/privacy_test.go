package pool

import (
	"context"
	"encoding/json"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestPoolMasksIDsAndEmptyProviderLocalPart(t *testing.T) {
	s := Build(context.Background(), []*auth.Auth{{ID: "codex-hash-alice@example.com-prolite.json", Provider: "codex", Label: "claude-@x.com"}}, nil, nil, nil, "")
	raw, _ := json.Marshal(s)
	if strings.Contains(string(raw), "alice@example.com") {
		t.Fatal("email leaked in auth_id")
	}
	if s.Credentials[0].Label != "claude-•••@x•••.com" {
		t.Fatalf("label=%s", s.Credentials[0].Label)
	}
}

func TestPoolRankUsesConfiguredAdmissionThreshold(t *testing.T) {
	now := time.Now()
	m := auth.NewManager(nil, &auth.ResetSoonestSelector{}, nil)
	cfg := &config.Config{}
	cfg.Routing.ResetSoonest.SevenDayThreshold = .5
	m.SetConfig(cfg)
	auths := []*auth.Auth{}
	for i, id := range []string{"a", "b"} {
		a := &auth.Auth{ID: id, Provider: "claude", Status: auth.StatusActive}
		used := "0.6"
		if i == 1 {
			used = "0.1"
		}
		a.Quota.ObserveResponseHeadersForProvider("claude", map[string][]string{"Anthropic-Ratelimit-Unified-7d-Utilization": {used}, "Anthropic-Ratelimit-Unified-7d-Reset": {strconv.FormatInt(now.Add(time.Duration(i+1)*time.Hour).Unix(), 10)}}, now)
		auths = append(auths, a)
	}
	s := Build(context.Background(), auths, nil, nil, m, "")
	if s.Credentials[1].PickRank == nil || *s.Credentials[1].PickRank != 1 {
		t.Fatalf("configured threshold ignored: %+v", s.Credentials)
	}
}
