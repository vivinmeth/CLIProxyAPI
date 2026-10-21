package capture

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"
)

func TestBodySpillsBeforeGlobalOrExchangeBudget(t *testing.T) {
	baseline := LiveBytes()
	Configure(1024, 512, t.TempDir())
	defer Configure(0, 0, "")
	group := &Group{}
	first, second := group.NewBody(), group.NewBody()
	defer first.Release()
	defer second.Release()
	payload := bytes.Repeat([]byte("x"), 300)
	if _, err := first.Write(payload); err != nil {
		t.Fatal(err)
	}
	if first.Spilled() {
		t.Fatal("small capture spilled")
	}
	if _, err := second.Write(payload); err != nil {
		t.Fatal(err)
	}
	if !second.Spilled() {
		t.Fatal("shared exchange threshold not enforced")
	}
	// A different exchange must spill rather than exceed the global reservation.
	third := &Body{}
	defer third.Release()
	Configure(400, 4096, currentLimits().spool)
	if _, err := third.Write(payload); err != nil {
		t.Fatal(err)
	}
	if !third.Spilled() {
		t.Fatal("global budget not enforced")
	}
	if LiveBytes() > 400+baseline {
		t.Fatalf("budget exceeded: %d", LiveBytes())
	}
	for _, body := range []*Body{first, second, third} {
		reader, err := body.Reader()
		if err != nil {
			t.Fatal(err)
		}
		got, err := io.ReadAll(reader)
		if err != nil || !bytes.Equal(got, payload) {
			t.Fatalf("lossy capture: %v", err)
		}
	}
	first.Release()
	second.Release()
	third.Release()
	if LiveBytes() != baseline {
		t.Fatalf("leaked reservations: %d", LiveBytes()-baseline)
	}
}

func TestBodyDiskFailureCountedAndDoesNotLeak(t *testing.T) {
	dir := t.TempDir()
	bad := filepath.Join(dir, "file")
	if err := os.WriteFile(bad, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	Configure(64, 64, bad)
	defer Configure(0, 0, "")
	before, drops := LiveBytes(), Dropped()
	body := &Body{}
	if _, err := body.Write([]byte("small")); err != nil {
		t.Fatal(err)
	}
	if _, err := body.Write(make([]byte, 128)); err == nil {
		t.Fatal("expected disk failure")
	}
	if LiveBytes() != before || Dropped() != drops+1 {
		t.Fatalf("live=%d dropped=%d", LiveBytes(), Dropped())
	}
	body.Release()
}
