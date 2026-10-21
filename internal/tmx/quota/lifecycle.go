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
			s.record(ctx, a.ID, spec.passive(a.Quota.Signals, a.Quota.ObservedAt), "passive", a.Quota.ObservedAt)
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
