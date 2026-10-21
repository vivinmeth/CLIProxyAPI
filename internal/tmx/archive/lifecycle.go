package archive

import "context"

// CloseContext bounds how long shutdown waits for index and R2 writes. Already
// spooled compressed blobs remain available for the next uploader on timeout.
func (a *Archive) CloseContext(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.closeWaitOnce.Do(func() { a.closeDone = make(chan struct{}); go func() { a.Close(); close(a.closeDone) }() })
	select {
	case <-a.closeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// FlushContext waits for accepted exchanges to reach the durable blob spool and
// index. R2 upload can finish later; the uploader resumes from the spool on boot.
func (a *Archive) FlushContext(ctx context.Context) error {
	if a == nil {
		return nil
	}
	var pending []chan struct{}
	a.pending.Range(func(_, done any) bool { pending = append(pending, done.(chan struct{})); return true })
	for _, done := range pending {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
