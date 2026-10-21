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

// FlushContext waits for records already queued without stopping future intake.
func (m *Manager) FlushContext(ctx context.Context) error {
	if m == nil {
		return nil
	}
	m.Start(context.Background())
	ack := make(chan struct{})
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		select {
		case <-m.done:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	m.queue = append(m.queue, queueItem{flushed: ack})
	m.mu.Unlock()
	m.cond.Signal()
	select {
	case <-ack:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
