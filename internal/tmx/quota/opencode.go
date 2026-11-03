package quota

import (
	"context"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/opencode"
	log "github.com/sirupsen/logrus"
)

// OpenCode Go readings (internal/tmx/opencode): one account per (entry, model), id
// opencode:<entry>:<model>, windows from the subscription's settings, source "passive". Readings
// carry no reset time (OpenCode's window anchors are unknown), so they never reach routing
// (applyRouting skips windows without one): they are for the dashboard and history only.

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
		readings, errR := s.oc.Readings(ctx, st, now)
		if errR != nil {
			log.WithError(errR).Warnf("tmx quota: OpenCode readings for %s failed", st.Entry)
			continue
		}
		for _, m := range readings {
			id := opencode.AccountID(st.Entry, m.Model)
			seen[id] = true
			s.mu.Lock()
			a := s.accountLocked(id)
			a.present, a.Provider, a.Label, a.Plan = true, st.Entry, m.Model, st.Plan
			s.mu.Unlock()
			windows := make([]Window, 0, len(m.Windows))
			for _, w := range m.Windows {
				windows = append(windows, Window{Window: w.Window, UsedPct: w.UsedPct, Status: w.Status})
			}
			s.record(ctx, id, windows, "passive", now.UTC())
		}
	}
}
