package capture

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestConfigureSweepsOnlyPreviousProcessSpills(t *testing.T) {
	dir := t.TempDir()
	if err := os.MkdirAll(filepath.Join(dir, "live"), 0700); err != nil {
		t.Fatal(err)
	}
	old := processStart.Add(-time.Hour)
	paths := []string{"live/capture-old", "stream-old.tmp", "live/capture-current", "stream-current.tmp", "retained.zst", "live/unrelated"}
	for i, name := range paths {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte("abc"), 0600); err != nil {
			t.Fatal(err)
		}
		if i < 2 || i >= 4 {
			if err := os.Chtimes(path, old, old); err != nil {
				t.Fatal(err)
			}
		}
	}
	Configure(0, 0, dir)
	defer Configure(0, 0, "")
	for i, name := range paths {
		_, err := os.Stat(filepath.Join(dir, name))
		if i < 2 && !os.IsNotExist(err) {
			t.Errorf("abandoned %s retained: %v", name, err)
		}
		if i >= 2 && err != nil {
			t.Errorf("current/durable %s removed: %v", name, err)
		}
	}
	// A second configure (hot reload) also leaves this process's files intact.
	Configure(0, 0, dir)
	if files, bytes := LiveSpillStats(dir); files != 2 || bytes != 6 {
		t.Fatalf("spill stats files=%d bytes=%d", files, bytes)
	}
}
