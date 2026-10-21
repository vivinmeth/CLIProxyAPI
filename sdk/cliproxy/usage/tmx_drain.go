package usage

import "context"

// StopAndWait flushes the asynchronous dispatcher before its durable sinks close.
// Stop retains its nonblocking SDK contract.
func (m *Manager) StopAndWait(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.Start(context.Background())
	m.Stop()
	select {
	case <-m.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
