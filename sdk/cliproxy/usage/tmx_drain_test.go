package usage

import (
	"context"
	"testing"
)

type drainTestPlugin struct{ started, release, delivered chan struct{} }

func (p drainTestPlugin) HandleUsage(context.Context, Record) {
	close(p.started)
	<-p.release
	close(p.delivered)
}
func TestStopAndWaitFlushesPendingUsage(t *testing.T) {
	m := NewManager(0)
	p := drainTestPlugin{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	m.Register(p)
	m.Publish(context.Background(), Record{})
	<-p.started
	stopped := make(chan error, 1)
	go func() { stopped <- m.StopAndWait(context.Background()) }()
	select {
	case <-stopped:
		t.Fatal("returned before sink delivery completed")
	default:
	}
	close(p.release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.delivered:
	default:
		t.Fatal("sink did not complete")
	}
}

func TestFlushContextKeepsUsageIntakeOpen(t *testing.T) {
	m := NewManager(10)
	p := &checkpointPlugin{seen: make(chan string, 2)}
	m.Register(p)
	m.Publish(context.Background(), Record{Model: "before"})
	if err := m.FlushContext(context.Background()); err != nil {
		t.Fatal(err)
	}
	if model := <-p.seen; model != "before" {
		t.Fatal(model)
	}
	m.Publish(context.Background(), Record{Model: "after"})
	if err := m.StopAndWait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if model := <-p.seen; model != "after" {
		t.Fatal(model)
	}
}

type checkpointPlugin struct{ seen chan string }

func (p *checkpointPlugin) HandleUsage(_ context.Context, r Record) { p.seen <- r.Model }
