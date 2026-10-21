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

// FlushContext checkpoints accepted rows while leaving the writer and shared DB
// open for requests still draining. Pending failures are also persisted to disk.
func (s *Store) FlushContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	ack := make(chan struct{})
	select {
	case s.flush <- ack:
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ack:
		return nil
	case <-s.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
