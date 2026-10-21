package cliproxy

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestTMXShutdownReservesWorkerWindow(t *testing.T) {
	s := &Service{cfg: &config.Config{}}
	ctx, cancel, drain, drainCancel := s.tmxShutdownWindow(context.Background())
	defer cancel()
	defer drainCancel()
	end, _ := ctx.Deadline()
	drainEnd, _ := drain.Deadline()
	if end.Sub(drainEnd) != 5*time.Second {
		t.Fatalf("flush window=%v", end.Sub(drainEnd))
	}
}
