package quota

import (
	"context"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/opencode"
	log "github.com/sirupsen/logrus"
)

// OpenCode Go readings (internal/tmx/opencode): one quota account per OpenCode account, id
// opencode:<entry>:<account> (opencode:<entry> without accounts; provider = the entry, label
// "OpenCode Go · <account>", plan = the account's plan), windows
// from its settings, source "passive". The anchored 5-hour window carries its reset time; it still
// never reaches routing: record() doesn't apply routing, and the routing seed only walks real
// credentials. Readings are for the dashboard and history only.

// OpenCode returns the OpenCode quota settings store, or nil.
func (s *Service) OpenCode() *opencode.Store {
	if s == nil {
		return nil
	}
	return s.oc
}

func (s *Service) tickOpenCode(ctx context.Context, now time.Time, seen map[string]bool) {
	if s.oc == nil {
		return
	}
	subs, err := s.oc.List(ctx)
	if err != nil {
		log.WithError(err).Warn("tmx quota: reading OpenCode settings failed")
		return
	}
	for _, st := range subs {
		readings, errR := s.oc.ReadAll(ctx, st, now)
		if errR != nil {
			log.WithError(errR).Warnf("tmx quota: OpenCode readings for %s failed", st.Entry)
			continue
		}
		for _, r := range readings {
			id := opencode.AccountID(st.Entry, r.Account)
			label := "OpenCode Go"
			if r.Account != "" {
				label += " · " + r.Account
			}
			seen[id] = true
			s.mu.Lock()
			a := s.accountLocked(id)
			a.present, a.Provider, a.Label, a.Plan = true, st.Entry, label, r.Plan
			s.mu.Unlock()
			windows := make([]Window, 0, len(r.Windows))
			for _, w := range r.Windows {
				windows = append(windows, Window{Window: w.Window, UsedPct: w.UsedPct, ResetsAt: w.ResetsAt, Status: w.Status})
			}
			s.record(ctx, id, windows, "passive", now.UTC())
		}
	}
}
