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
