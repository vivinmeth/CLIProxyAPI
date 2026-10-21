package requestlog

import (
	"testing"
)

func TestEnqueueAfterCloseIsCountedWithoutPanic(t *testing.T) {
	s := &Store{queue: make(chan Row, 1), closing: true}
	close(s.queue)
	s.Enqueue(Row{})
	if s.dropped.Load() != 1 {
		t.Fatal("late row not counted")
	}
}
