package capture

import (
	"os"
	"path/filepath"
	"time"

	log "github.com/sirupsen/logrus"
)

// Package initialization precedes any capture or archive worker in this process.
// A hot reload must never remove files owned by this process.
var processStart = time.Now()

func spillPaths(spool string) []string {
	var paths []string
	for _, pattern := range []string{filepath.Join(spool, "live", "capture-*"), filepath.Join(spool, "stream-*.tmp")} {
		matches, _ := filepath.Glob(pattern)
		paths = append(paths, matches...)
	}
	return paths
}

// SweepSpills reclaims unfinished capture/compression files from previous runs.
// Compressed .zst blobs and the read cache are durable and must remain untouched.
func SweepSpills(spool string) {
	for _, path := range spillPaths(spool) {
		info, err := os.Lstat(path)
		if err != nil || !info.Mode().IsRegular() || !info.ModTime().Before(processStart) {
			continue
		}
		if err = os.Remove(path); err != nil {
			log.WithError(err).Warn("tmx capture: could not remove abandoned spill")
		}
	}
}

// LiveSpillStats includes live body spills and temporary streaming compressors.
func LiveSpillStats(spool string) (files, bytes int64) {
	for _, path := range spillPaths(spool) {
		if info, err := os.Lstat(path); err == nil && info.Mode().IsRegular() {
			files++
			bytes += info.Size()
		}
	}
	return
}
