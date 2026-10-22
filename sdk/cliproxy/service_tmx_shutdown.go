package cliproxy

import (
	"context"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/archive"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/cooldowns"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/quota"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	log "github.com/sirupsen/logrus"
)

// tmxShutdownWindow reserves up to five seconds for worker flushes inside the
// configured total window. Zero retains immediate connection closure, with a
// short separate flush window for records already accepted by workers.
func (s *Service) tmxShutdownWindow(parent context.Context) (context.Context, context.CancelFunc, context.Context, context.CancelFunc) {
	timeout := s.currentConfig().ShutdownDrainTimeout()
	if timeout <= 0 {
		ctx, cancel := context.WithTimeout(parent, 5*time.Second)
		return ctx, cancel, ctx, func() {}
	}
	ctx, cancel := context.WithTimeout(parent, timeout)
	deadline, _ := ctx.Deadline()
	reserve := min(5*time.Second, max(time.Duration(0), time.Until(deadline))/10)
	drain, drainCancel := context.WithDeadline(ctx, deadline.Add(-reserve))
	return ctx, cancel, drain, drainCancel
}
func (s *Service) flushTMXWorkers(ctx context.Context) {
	// Polling shares requestlog's pool; archive can still emit rejected-request rows.
	for _, closeWorker := range []func(context.Context) error{quota.Default().CloseContext, archive.Default().CloseContext,
		cooldowns.Default().CloseContext, requestlog.Default().CloseContext} {
		if err := closeWorker(ctx); err != nil {
			log.WithError(err).Warn("tmx: shutdown flush window elapsed")
		}
	}
}

// The independent checkpoint starts by T+5s (earlier for short shutdown windows).
// It keeps sinks open so turns that finish later in the drain are still recorded.
func tmxCheckpointDelay(drain context.Context) time.Duration {
	delay := 5 * time.Second
	if deadline, ok := drain.Deadline(); ok {
		delay = min(delay, max(0, time.Until(deadline)/2))
	}
	return delay
}

func tmxStartCheckpoint(ctx, drain context.Context, drained <-chan struct{}, workers ...func(context.Context) error) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTimer(tmxCheckpointDelay(drain))
		defer timer.Stop()
		select {
		case <-drained:
			return
		case <-timer.C:
		case <-drain.Done():
		}
		// Start every worker independently: a blocked archive or DB must not prevent
		// another worker from even beginning its checkpoint before the deadline.
		var wg sync.WaitGroup
		for _, flush := range workers {
			wg.Go(func() {
				if err := flush(ctx); err != nil {
					log.WithError(err).Warn("tmx: early shutdown checkpoint incomplete")
				}
			})
		}
		wg.Wait()
	}()
	return done
}

func (s *Service) startTMXShutdownCheckpoint(ctx, drain context.Context, drained <-chan struct{}) <-chan struct{} {
	return tmxStartCheckpoint(ctx, drain, drained,
		s.sessionBindings.FlushContext,
		usage.DefaultManager().FlushContext,
		quota.Default().FlushPassiveContext,
		archive.Default().FlushContext,
		cooldowns.Default().FlushContext,
		requestlog.Default().FlushContext,
	)
}
