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
