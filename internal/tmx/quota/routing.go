package quota

import (
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"time"
)

func (s *Service) applyRouting(id, provider string, windows []Window, at time.Time) {
	if s.observer == nil {
		return
	}
	observations := make([]coreauth.QuotaWindowObservation, 0, len(windows))
	for _, w := range windows {
		if w.ResetsAt == nil {
			continue
		}
		observations = append(observations, coreauth.QuotaWindowObservation{Window: w.Window, UsedPercent: w.UsedPct, ResetsAt: *w.ResetsAt, Status: w.Status, ObservedAt: at})
	}
	s.observer.ApplyQuotaObservation(id, provider, observations)
}

// Seed once per registration, including auths loaded after the management server.
// Reset-credit clearing cannot accidentally resurrect the boot snapshot on a later tick.
func (s *Service) seedRouting() {
	if s.observer == nil {
		return
	}
	now := s.now()
	for _, auth := range s.fetch.TmxAuths() {
		if auth == nil {
			continue
		}
		s.mu.Lock()
		if epoch, ok := s.seeded[auth.ID]; ok && epoch == auth.RegistrationEpoch {
			s.mu.Unlock()
			continue
		}
		if s.seeded == nil {
			s.seeded = map[string]uint64{}
		}
		s.seeded[auth.ID] = auth.RegistrationEpoch
		var readings []Current
		if a := s.accounts[auth.ID]; a != nil {
			for _, c := range a.current {
				if now.Sub(c.ObservedAt) <= coreauth.QuotaObservationMaxAge && !c.ObservedAt.After(now) && c.ResetsAt != nil && c.ResetsAt.After(now) {
					readings = append(readings, c)
				}
			}
		}
		s.mu.Unlock()
		for _, c := range readings {
			s.applyRouting(auth.ID, auth.Provider, []Window{c.Window}, c.ObservedAt)
		}
	}
}
