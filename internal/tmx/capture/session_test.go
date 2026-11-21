package capture

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

func TestSessionHintLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name  string
		ids   []string
		close bool
		want  string
	}{
		{name: "empty"}, {name: "same", ids: []string{"", "a", "a", ""}, want: "a"},
		{name: "conflict", ids: []string{"a", "b", "a"}}, {name: "reverse conflict", ids: []string{"b", "a"}},
		{name: "closed", ids: []string{"a"}, close: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := &SessionHint{}
			for _, id := range tc.ids {
				h.Note(id)
			}
			if tc.close {
				h.Close()
			}
			if got := h.Take(); got != tc.want {
				t.Fatalf("Take=%q want=%q", got, tc.want)
			}
			h.Note("late")
			if h.Take() != "" {
				t.Fatal("late write survived close")
			}
		})
	}
	var absent *SessionHint
	absent.Note("x")
	absent.Close()
	if absent.Take() != "" || SessionHintFrom(nil) != nil || SessionHintFrom(context.Background()) != nil {
		t.Fatal("missing holder")
	}
	h := &SessionHint{}
	if SessionHintFrom(WithSessionHint(context.Background(), h)) != h {
		t.Fatal("context holder changed")
	}
}

func TestSessionHintConcurrentNotes(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(fmt.Sprint(conflict), func(t *testing.T) {
			h := &SessionHint{}
			var wg sync.WaitGroup
			for i := 0; i < 64; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					id := "same"
					if conflict && i%2 == 0 {
						id = "other"
					}
					h.Note(id)
				}(i)
			}
			wg.Wait()
			want := "same"
			if conflict {
				want = ""
			}
			if h.Take() != want {
				t.Fatal("concurrent observations changed outcome")
			}
			for i := 0; i < 64; i++ {
				wg.Add(1)
				go func() { defer wg.Done(); h.Note("late"); h.Close() }()
			}
			wg.Wait()
			if h.Take() != "" {
				t.Fatal("late observations survived")
			}
		})
	}
}
