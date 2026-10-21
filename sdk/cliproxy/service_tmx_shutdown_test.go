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

func TestShutdownCheckpointsStartWhileServerStillDraining(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	drain, cancelDrain := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelDrain()
	drained := make(chan struct{}) // Simulate Stop that has not returned.
	started := make(chan int, 5)
	release := make(chan struct{})
	workers := make([]func(context.Context) error, 5)
	for i := range workers {
		workers[i] = func(context.Context) error { started <- i; <-release; return nil }
	}
	done := tmxStartCheckpoint(ctx, drain, drained, workers...)
	defer func() { close(drained); close(release); <-done }()
	for range workers {
		select {
		case <-started:
		case <-drain.Done():
			t.Fatal("worker checkpoint skipped until drain deadline")
		}
	}
	select {
	case <-done:
		t.Fatal("checkpoint returned before workers flushed")
	default:
	}
	if delay := tmxCheckpointDelay(context.Background()); delay > 5*time.Second {
		t.Fatalf("unbounded checkpoint delay %v", delay)
	}
}

func TestCompletedDrainDoesNotWaitForCheckpointTimer(t *testing.T) {
	drained := make(chan struct{})
	close(drained)
	done := tmxStartCheckpoint(context.Background(), context.Background(), drained, func(context.Context) error { t.Error("checkpoint after completed drain"); return nil })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fast shutdown waited for checkpoint")
	}
}
