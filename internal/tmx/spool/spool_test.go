package spool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// sink records delivered payloads; while down it fails every delivery.
type sink struct {
	mu     sync.Mutex
	down   bool
	got    []string
	refuse map[string]bool // payloads refused permanently
	calls  int
}

func (k *sink) deliver(_ context.Context, items []Item) (int, error) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.calls++
	if k.down {
		return 0, errors.New("sink down")
	}
	for i, it := range items {
		b, err := it.Bytes()
		if err != nil {
			return i, err
		}
		if k.refuse[string(b)] {
			return i, Permanent(fmt.Errorf("refused %q", b))
		}
		k.got = append(k.got, string(b))
	}
	return len(items), nil
}

func (k *sink) set(down bool) {
	k.mu.Lock()
	k.down = down
	k.mu.Unlock()
}

func (k *sink) delivered() []string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return append([]string(nil), k.got...)
}

func opts(k *sink) Options {
	return Options{Name: "test", MinBackoff: 10 * time.Millisecond, MaxBackoff: 40 * time.Millisecond, Deliver: k.deliver}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestReplaysAfterSinkRecovers(t *testing.T) {
	k := &sink{down: true}
	s, err := Open(t.TempDir(), opts(k))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 50; i++ {
		if err = s.Append([]byte(fmt.Sprintf("item-%02d", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitFor(t, "a failed attempt", s.Failing)
	if st := s.Stats(); st.SpooledPending != 50 || st.Spooled != 50 || st.Replayed != 0 || st.LastError == "" {
		t.Fatalf("while down: %+v", st)
	}
	k.set(false)
	s.Recovered()
	waitFor(t, "replay", func() bool { return s.Pending() == 0 })
	got := k.delivered()
	if len(got) != 50 || got[0] != "item-00" || got[49] != "item-49" {
		t.Fatalf("delivered %d: %v", len(got), got)
	}
	if st := s.Stats(); st.Replayed != 50 || st.SpooledBytes != 0 || st.Dropped != 0 || s.Failing() {
		t.Fatalf("after: %+v failing=%v", st, s.Failing())
	}
	// Delivered segments are removed.
	segs, _ := filepath.Glob(filepath.Join(s.Dir(), "seg-*"))
	if len(segs) != 0 {
		t.Fatalf("segments left: %v", segs)
	}
}

// Items survive a restart, acknowledged items are not delivered again, and a torn append (crash
// mid-write) is cut off and kept in the dead letters.
func TestCrashAndReplay(t *testing.T) {
	dir := t.TempDir()
	k := &sink{down: true}
	s, err := Open(dir, Options{Name: "test", Deliver: nil})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 5; i++ {
		_ = s.Append([]byte(fmt.Sprintf("a%d", i)))
	}
	// Partial delivery: the first two are acknowledged.
	s.opts.Deliver = func(ctx context.Context, items []Item) (int, error) {
		k.mu.Lock()
		defer k.mu.Unlock()
		for i, it := range items[:2] {
			b, _ := it.Bytes()
			k.got = append(k.got, string(b))
			_ = i
		}
		return 2, errors.New("connection lost")
	}
	if err = s.drain(); err == nil || err.Error() != "connection lost" {
		t.Fatalf("drain err = %v", err)
	}
	segs, _ := filepath.Glob(filepath.Join(dir, "seg-*.spool"))
	if len(segs) != 1 {
		t.Fatalf("segments = %v", segs)
	}
	// "Crash": no Close; a torn frame is left at the end of the segment.
	f, _ := os.OpenFile(segs[0], os.O_WRONLY|os.O_APPEND, 0)
	_, _ = f.Write([]byte("TMXS\x00\x00\x00\x00\xff\xff\xff\xff\xff\xff\xff\xffhalf-writ"))
	_ = f.Close()
	s.lock.Close() // the dead process' lock goes with it

	k2 := &sink{}
	s2, err := Open(dir, opts(k2))
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	waitFor(t, "replay", func() bool { return s2.Pending() == 0 && len(k2.delivered()) == 3 })
	if got := strings.Join(k2.delivered(), ","); got != "a2,a3,a4" {
		t.Fatalf("replayed %s", got)
	}
	st := s2.Stats()
	if st.Deadlettered != 1 || st.DeadletterTotal != 1 {
		t.Fatalf("torn tail not dead-lettered: %+v", st)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, deadDirName, reasonsName))
	if !strings.Contains(string(raw), "torn or corrupt frame") {
		t.Fatalf("reasons = %s", raw)
	}
	dead, _ := os.ReadFile(filepath.Join(dir, deadDirName, "test.spool"))
	if !strings.Contains(string(dead), "half-writ") {
		t.Fatalf("dead letter lost the torn bytes")
	}
}

func TestPermanentFailuresAreDeadLetteredAndKept(t *testing.T) {
	dir := t.TempDir()
	k := &sink{refuse: map[string]bool{"bad": true}}
	s, err := Open(dir, opts(k))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{"ok1", "bad", "ok2"} {
		_ = s.Append([]byte(p))
	}
	if err = s.Deadletter([]byte("refused-live"), "constraint violation"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "replay", func() bool { return s.Pending() == 0 })
	if got := strings.Join(k.delivered(), ","); got != "ok1,ok2" {
		t.Fatalf("delivered %s", got)
	}
	st := s.Stats()
	if st.Deadlettered != 2 || st.DeadletterTotal != 2 || st.Replayed != 2 || st.Dropped != 0 {
		t.Fatalf("stats %+v", st)
	}
	s.Close()
	// Dead letters persist and are counted again after a restart.
	s2, err := Open(dir, Options{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if st = s2.Stats(); st.DeadletterTotal != 2 || st.Deadlettered != 0 {
		t.Fatalf("after restart %+v", st)
	}
	dead, _ := os.ReadFile(filepath.Join(dir, deadDirName, "test.spool"))
	if !strings.Contains(string(dead), "bad") || !strings.Contains(string(dead), "refused-live") {
		t.Fatalf("dead letters = %q", dead)
	}
	reasons, _ := os.ReadFile(filepath.Join(dir, deadDirName, reasonsName))
	if !strings.Contains(string(reasons), `refused \"bad\"`) || !strings.Contains(string(reasons), "constraint violation") {
		t.Fatalf("reasons = %s", reasons)
	}
}

func TestCapDropsLoudlyInsteadOfFillingTheDisk(t *testing.T) {
	k := &sink{down: true}
	o := opts(k)
	o.MaxBytes = 100
	s, err := Open(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	payload := []byte(strings.Repeat("x", 30)) // 46 bytes a frame
	if err = s.Append(payload); err != nil {
		t.Fatal(err)
	}
	if err = s.Append(payload); err != nil {
		t.Fatal(err)
	}
	if err = s.Append(payload); !errors.Is(err, ErrFull) {
		t.Fatalf("third append = %v", err)
	}
	st := s.Stats()
	if st.Dropped != 1 || st.SpooledPending != 2 || st.SpooledBytes != 92 || !strings.Contains(st.LastError, "size cap") {
		t.Fatalf("stats %+v", st)
	}
	// Once replayed, there is room again.
	k.set(false)
	s.Recovered()
	waitFor(t, "replay", func() bool { return s.Pending() == 0 })
	if err = s.Append(payload); err != nil {
		t.Fatal(err)
	}
}

func TestSegmentsRotateAndStreamingAppend(t *testing.T) {
	k := &sink{down: true}
	o := opts(k)
	o.SegmentBytes = 64
	o.MaxBatch = 3
	s, err := Open(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := 0; i < 10; i++ {
		err = s.AppendFunc(0, func(w io.Writer) error {
			_, errW := fmt.Fprintf(w, "part-%d|", i)
			if errW == nil {
				_, errW = fmt.Fprintf(w, "rest-%d", i)
			}
			return errW
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// A failing stream spools nothing.
	if err = s.AppendFunc(0, func(w io.Writer) error {
		_, _ = w.Write([]byte("partial"))
		return errors.New("source broke")
	}); err == nil {
		t.Fatal("expected the stream error")
	}
	segs, _ := filepath.Glob(filepath.Join(s.Dir(), "seg-*.spool"))
	if len(segs) < 3 {
		t.Fatalf("expected rotation, segments = %d", len(segs))
	}
	k.set(false)
	s.Recovered()
	waitFor(t, "replay", func() bool { return s.Pending() == 0 })
	got := k.delivered()
	if len(got) != 10 || got[0] != "part-0|rest-0" || got[9] != "part-9|rest-9" {
		t.Fatalf("delivered %v", got)
	}
	if st := s.Stats(); st.Dropped != 1 || st.Spooled != 10 {
		t.Fatalf("stats %+v", st)
	}
}

func TestDirectoryIsExclusive(t *testing.T) {
	dir := t.TempDir()
	s, err := Open(dir, Options{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err = Open(dir, Options{Name: "test"}); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open = %v", err)
	}
	s2, err := OpenFirstFree(dir, Options{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if s2.Dir() != dir+"-2" {
		t.Fatalf("dir = %s", s2.Dir())
	}
}

func TestAppendAfterCloseIsCounted(t *testing.T) {
	s, err := Open(t.TempDir(), Options{Name: "test"})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	if err = s.Append([]byte("late")); !errors.Is(err, ErrClosed) || s.Stats().Dropped != 1 {
		t.Fatalf("append after close = %v, dropped %d", err, s.Stats().Dropped)
	}
}

func TestDirDefaults(t *testing.T) {
	t.Setenv("TMX_SPOOL_DIR", "")
	t.Setenv("TMX_ARCHIVE_SPOOL", "")
	t.Setenv("WRITABLE_PATH", "/w")
	if _, err := os.Stat("/CLIProxyAPI/archive-spool"); err == nil {
		t.Skip("running inside the proxy container")
	}
	if got := Dir("requestlog"); got != "/w/tmx-spool/requestlog" {
		t.Fatalf("writable: %s", got)
	}
	vol := t.TempDir()
	t.Setenv("TMX_ARCHIVE_SPOOL", vol)
	if got := Dir("requestlog"); got != filepath.Join(vol, "durable", "requestlog") {
		t.Fatalf("archive volume: %s", got)
	}
	t.Setenv("TMX_SPOOL_DIR", "/x")
	if got := Dir("quota"); got != "/x/quota" {
		t.Fatalf("explicit: %s", got)
	}
	t.Setenv("TMX_SPOOL_MAX_MB", "5")
	t.Setenv("TMX_SPOOL_QUOTA_MAX_MB", "7")
	if MaxBytesFromEnv("quota", 1) != 7<<20 || MaxBytesFromEnv("archive", 1) != 5<<20 {
		t.Fatal("max bytes env")
	}
}

func TestAppendBatch(t *testing.T) {
	k := &sink{down: true}
	o := opts(k)
	o.MaxBytes = 200
	s, err := Open(t.TempDir(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err = s.AppendBatch([][]byte{[]byte("b1"), []byte("b2"), []byte("b3")}); err != nil {
		t.Fatal(err)
	}
	// Over the cap: none of the batch is spooled, all of it is counted.
	big := []byte(strings.Repeat("y", 60))
	if err = s.AppendBatch([][]byte{big, big}); !errors.Is(err, ErrFull) {
		t.Fatalf("batch over cap = %v", err)
	}
	if st := s.Stats(); st.SpooledPending != 3 || st.Dropped != 2 || st.Spooled != 3 {
		t.Fatalf("stats %+v", st)
	}
	k.set(false)
	s.Recovered()
	waitFor(t, "replay", func() bool { return s.Pending() == 0 })
	if got := strings.Join(k.delivered(), ","); got != "b1,b2,b3" {
		t.Fatalf("delivered %s", got)
	}
}
