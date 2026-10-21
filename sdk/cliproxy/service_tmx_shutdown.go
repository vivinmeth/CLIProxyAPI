package cliproxy

import (
	"context"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/archive"
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
	for _, closeWorker := range []func(context.Context) error{quota.Default().CloseContext, archive.Default().CloseContext, requestlog.Default().CloseContext} {
		if err := closeWorker(ctx); err != nil {
			log.WithError(err).Warn("tmx: shutdown flush window elapsed")
		}
	}
}
