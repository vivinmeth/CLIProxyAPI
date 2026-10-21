package requestlog

import "context"

// CloseContext waits for the existing durable spool flush within the caller's
// shutdown deadline. The database writer continues until the process exits.
func (s *Store) CloseContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.closeWaitOnce.Do(func() { s.closeDone = make(chan struct{}); go func() { s.Close(); close(s.closeDone) }() })
	select {
	case <-s.closeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
