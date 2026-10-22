// Package durability is the registry behind GET /v8/management/tmx/durability (tokenmaxx fork):
// every background writer that persists data (request log, quota snapshots, archive capture,
// cooldown events, ...) registers a function returning its counters, so a single read shows
// whether anything is queued, spooled on disk, dropped or dead-lettered.
//
// A writer registers once when it starts:
//
//	durability.Register("cooldowns", func() durability.Stats {
//		st := mySpool.Stats()           // spooled_pending, spooled_bytes, replayed, dead letters ...
//		st.Queued = int64(len(queue))   // what is still only in memory
//		st.Dropped += droppedElsewhere.Load()
//		return st
//	})
//
// and unregisters (Register(name, nil)) when it stops. It is a leaf package: no project imports.
package durability

import (
	"sort"
	"sync"
	"time"
)

// Stats are one writer's durability counters. Counters are since process start unless noted.
type Stats struct {
	// Queued: items accepted but only in memory (the writer's channel / batch).
	Queued int64 `json:"queued"`
	// Spooled: items written to the on-disk spool since start (sink down, queue full, ...).
	Spooled int64 `json:"spooled"`
	// SpooledPending / SpooledBytes: items (and frame bytes) on disk waiting for replay, including
	// those left by a previous run.
	SpooledPending int64 `json:"spooled_pending"`
	SpooledBytes   int64 `json:"spooled_bytes"`
	// Replayed: spooled items the sink has since accepted.
	Replayed int64 `json:"replayed"`
	// Dropped: items lost for good (spool over its size cap, disk error, writer closed). Should be 0.
	Dropped int64 `json:"dropped"`
	// Deadlettered: items the sink refused permanently, kept in the dead-letter file.
	Deadlettered int64 `json:"deadlettered"`
	// DeadletterTotal: everything in the dead-letter file, all runs included (never deleted).
	DeadletterTotal int64      `json:"deadletter_total"`
	LastError       string     `json:"last_error"`
	LastErrorAt     *time.Time `json:"last_error_at"`
	// SpoolDir is where the writer spools (empty when it has no spool).
	SpoolDir string `json:"spool_dir,omitempty"`
}

var (
	mu      sync.RWMutex
	writers = map[string]func() Stats{}
)

// Register installs (or replaces) the counters of writer name; a nil fn removes it.
func Register(name string, fn func() Stats) {
	mu.Lock()
	defer mu.Unlock()
	if fn == nil {
		delete(writers, name)
		return
	}
	writers[name] = fn
}

// Names returns the registered writers, sorted.
func Names() []string {
	mu.RLock()
	defer mu.RUnlock()
	out := make([]string, 0, len(writers))
	for name := range writers {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Snapshot reads every registered writer's counters.
func Snapshot() map[string]Stats {
	mu.RLock()
	fns := make(map[string]func() Stats, len(writers))
	for name, fn := range writers {
		fns[name] = fn
	}
	mu.RUnlock()
	out := make(map[string]Stats, len(fns))
	for name, fn := range fns {
		out[name] = fn()
	}
	return out
}
