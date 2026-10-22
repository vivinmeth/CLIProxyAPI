// Package pool projects routing and quota state without exposing token metadata.
package pool

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/quota"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	executor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// InFlightCounter is the optional adapter for credential admission counters.
// The endpoint reports zero until the runtime supplies one.
type InFlightCounter interface{ InFlightCount(authID string) int }

type Credential struct {
	Provider       string              `json:"provider"`
	AuthID         string              `json:"auth_id"`
	AuthIndex      string              `json:"auth_index"`
	Label          string              `json:"label"`
	PlanType       string              `json:"plan_type"`
	Enabled        bool                `json:"enabled"`
	Status         auth.Status         `json:"status"`
	Unavailable    bool                `json:"unavailable"`
	NextRetryAfter *time.Time          `json:"next_retry_after"`
	Reason         string              `json:"reason,omitempty"`
	Cooldowns      []auth.CooldownView `json:"cooldowns"`
	Windows        []quota.Current     `json:"windows"`
	InFlight       int                 `json:"in_flight"`
	BoundSessions  int                 `json:"bound_sessions"`
	PickRank       *int                `json:"pick_rank"`
	RankModel      string              `json:"rank_model"`
}

type Snapshot struct {
	Credentials []Credential `json:"credentials"`
	ObservedAt  time.Time    `json:"observed_at"`
}

// Build calls the actual reset-soonest selector on detached snapshots, so its
// ordering remains consistent as the routing track evolves. It never binds sessions.
// model overrides the default (newest advertised model per provider).
func Build(ctx context.Context, auths []*auth.Auth, counts map[string]int, accounts []quota.Account, counter InFlightCounter, model string) Snapshot {
	now := time.Now()
	out := Snapshot{Credentials: []Credential{}, ObservedAt: now.UTC()}
	filtered := make([]*auth.Auth, 0, len(auths))
	for _, credential := range auths {
		if credential != nil {
			filtered = append(filtered, credential)
		}
	}
	if source, ok := counter.(interface {
		PoolSelectionContext(context.Context) context.Context
	}); ok {
		ctx = source.PoolSelectionContext(ctx)
	}
	auths = filtered
	sort.Slice(auths, func(i, j int) bool { return auths[i].ID < auths[j].ID })
	byProvider := map[string][]*auth.Auth{}
	for _, a := range auths {
		byProvider[a.Provider] = append(byProvider[a.Provider], a)
	}
	ranks := map[string]int{}
	models := map[string]string{}
	selector := &auth.ResetSoonestSelector{}
	for provider, candidates := range byProvider {
		rankModel := model
		if rankModel == "" {
			advertised := registry.GetGlobalRegistry().GetAvailableModelsByProvider(provider)
			sort.Slice(advertised, func(i, j int) bool {
				if advertised[i].Created != advertised[j].Created {
					return advertised[i].Created > advertised[j].Created
				}
				return advertised[i].ID < advertised[j].ID
			})
			if len(advertised) > 0 {
				rankModel = advertised[0].ID
			}
		}
		models[provider] = rankModel
		for rank := 1; len(candidates) > 0; rank++ {
			picked, err := selector.Pick(ctx, provider, rankModel, executor.Options{}, candidates)
			if err != nil || picked == nil {
				break
			}
			ranks[picked.ID] = rank
			remaining := make([]*auth.Auth, 0, len(candidates)-1)
			for _, a := range candidates {
				if a.ID != picked.ID {
					remaining = append(remaining, a)
				}
			}
			candidates = remaining
		}
	}
	latest := map[string]quota.Account{}
	for _, account := range accounts {
		latest[account.AuthID] = account
	}
	for _, a := range auths {
		c := Credential{Provider: a.Provider, AuthID: maskLabel(a.ID), AuthIndex: a.EnsureIndex(), Label: maskLabel(a.Label), Enabled: !a.Disabled && a.Status != auth.StatusDisabled, Status: a.Status, Unavailable: a.Unavailable, Cooldowns: auth.CooldownSnapshotForAuth(a, now), Windows: []quota.Current{}, BoundSessions: counts[a.ID], RankModel: models[a.Provider]}
		if c.Label == "" {
			c.Label = maskLabel(a.ID)
		}
		if !a.NextRetryAfter.IsZero() {
			retry := a.NextRetryAfter.UTC()
			c.NextRetryAfter = &retry
		}
		if rank, ok := ranks[a.ID]; ok {
			r := rank
			c.PickRank = &r
		}
		if counter != nil {
			c.InFlight = counter.InFlightCount(a.ID)
		}
		if plan, ok := a.Metadata["plan_type"].(string); ok {
			c.PlanType = plan
		}
		windows := map[string]quota.Current{}
		var passive []quota.Window
		switch a.Provider {
		case "claude":
			passive = quota.ParseClaudeSignals(a.Quota.Signals)
		case "codex":
			passive = quota.ParseCodexSignals(a.Quota.Signals, a.Quota.ObservedAt)
		}
		for _, window := range passive {
			windows[window.Window] = quota.Current{Window: window, ObservedAt: a.Quota.ObservedAt, Source: "passive", Stale: window.ResetsAt != nil && !window.ResetsAt.After(now)}
		}
		if account, ok := latest[a.ID]; ok {
			if account.Plan != "" {
				c.PlanType = account.Plan
			}
			for _, window := range account.Windows {
				if previous, ok := windows[window.Window.Window]; !ok || window.ObservedAt.After(previous.ObservedAt) {
					windows[window.Window.Window] = window
				}
			}
		}
		keys := make([]string, 0, len(windows))
		for key := range windows {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			c.Windows = append(c.Windows, windows[key])
		}
		if a.Quota.Exceeded {
			c.Reason = "quota"
		} else if a.LastError != nil {
			switch a.LastError.HTTPStatus {
			case 401, 402, 403:
				c.Reason = "auth"
			case 429:
				c.Reason = "quota"
			default:
				if a.Unavailable {
					c.Reason = "transient"
				}
			}
		} else if a.Unavailable {
			c.Reason = "transient"
		}
		out.Credentials = append(out.Credentials, c)
	}
	return out
}

var emailPattern = regexp.MustCompile(`([A-Za-z0-9._%+\-]+)@([A-Za-z0-9\-]+(?:\.[A-Za-z0-9\-]+)+)`)

func maskLabel(text string) string {
	return emailPattern.ReplaceAllStringFunc(text, func(email string) string {
		local, domain, _ := strings.Cut(email, "@")
		prefix := ""
		for _, provider := range []string{"antigravity", "claude", "codex", "devin", "gemini", "grok", "iflow", "kimi", "meta", "qwen", "xai"} {
			for _, sep := range []string{"-", "_"} {
				p := provider + sep
				if strings.HasPrefix(strings.ToLower(local), p) {
					prefix = local[:len(p)]
					local = local[len(p):]
					break
				}
			}
			if prefix != "" {
				break
			}
		}
		labels := strings.Split(domain, ".")
		suffix := ""
		for len(labels) > 2 {
			ext := strings.ToLower(labels[len(labels)-1])
			if ext != "json" && ext != "yaml" && ext != "yml" && ext != "txt" && ext != "toml" {
				break
			}
			suffix = "." + labels[len(labels)-1] + suffix
			labels = labels[:len(labels)-1]
		}
		for i := 0; i < len(labels)-1; i++ {
			labels[i] = labels[i][:1] + "•••"
		}
		initial := ""
		if len(local) > 0 {
			initial = local[:1]
		}
		return prefix + initial + "•••@" + strings.Join(labels, ".") + suffix
	})
}
