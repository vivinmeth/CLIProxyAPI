package quota

import "context"

// CloseContext cancels network polling, waits for its writer, and records final
// passive observations while the shared request-log pool is still open.
func (s *Service) CloseContext(ctx context.Context) error {
	if s == nil || s.loopCancel == nil {
		return nil
	}
	s.closeOnce.Do(s.loopCancel)
	select {
	case <-s.loopDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	return s.FlushPassiveContext(ctx)
}

// FlushPassiveContext checkpoints current per-window observations without stopping polling.
func (s *Service) FlushPassiveContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	for _, a := range s.fetch.TmxAuths() {
		spec, _, ok := specFor(a)
		if !ok || a.Quota.ObservedAt.IsZero() {
			continue
		}
		s.mu.Lock()
		state := s.accountLocked(a.ID)
		due := a.Quota.ObservedAt.After(state.lastPassive)
		s.mu.Unlock()
		if due {
			for _, r := range passiveReadings(a, spec, s.now()) {
				s.record(ctx, a.ID, []Window{r.Window}, "passive", r.ObservedAt.UTC())
			}
			s.mu.Lock()
			state.lastPassive = a.Quota.ObservedAt
			s.mu.Unlock()
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	return nil
}
