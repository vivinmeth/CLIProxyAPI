// Package spool is a small durable on-disk queue for the tokenmaxx writers (tokenmaxx fork).
//
// A writer that can't hand an item to its sink (Postgres down, a queue over its limit) appends it
// here instead of dropping it; a background replayer hands spooled items back to the writer's
// Deliver function, with backoff, until the sink takes them. Layout under the spool directory:
//
//	seg-<unixnano>-<seq>.spool   append-only segments of frames, oldest first
//	seg-....spool.ack            offset of the segment's first frame not yet delivered
//	deadletter/<name>.spool      frames the sink refused for good (never deleted)
//	deadletter/reasons.jsonl     one line per dead letter: when, why, where it came from
//
// Frame: "TMXS" | crc32c(payload) uint32 | len uint64 | payload, big endian. Append fsyncs before
// it returns. A crash mid-append leaves a torn frame at a segment's end; Open moves those bytes to
// the dead-letter file (the append never returned, so its writer didn't count it as stored).
// Segments are removed once every frame in them was delivered (the items are in the sink then).
//
// Delivery is at least once: a crash between the sink's commit and the ack re-delivers, so sinks
// must be idempotent (row keys with ON CONFLICT DO NOTHING). The spool is bounded by MaxBytes:
// an append over the cap is refused and counted as a drop, loudly, rather than filling the disk.
package spool

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/durability"
	log "github.com/sirupsen/logrus"
)

const (
	frameMagic  = 0x544d5853 // "TMXS"
	headerSize  = 16
	pendingLen  = ^uint64(0) // length field while the payload is being written
	segPrefix   = "seg-"
	segSuffix   = ".spool"
	ackSuffix   = ".ack"
	deadDirName = "deadletter"
	reasonsName = "reasons.jsonl"

	DefaultMaxBytes      int64 = 4 << 30
	DefaultSegmentBytes  int64 = 64 << 20
	DefaultMaxBatch            = 200
	DefaultMaxBatchBytes int64 = 8 << 20
	DefaultMinBackoff          = time.Second
	DefaultMaxBackoff          = 30 * time.Second
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	// ErrFull: the append would take the spool over MaxBytes; the item is dropped (and counted).
	ErrFull = errors.New("spool over its size cap")
	// ErrClosed: the spool was closed; the item is dropped (and counted).
	ErrClosed = errors.New("spool closed")
	// ErrLocked: another spool (this or another process) has the directory open.
	ErrLocked = errors.New("spool directory in use")
)

// Permanent marks a delivery error as final for one item: the item goes to the dead-letter file
// instead of being retried.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanentError
	return errors.As(err, &p)
}

type permanentError struct{ err error }

func (p permanentError) Error() string { return p.err.Error() }
func (p permanentError) Unwrap() error { return p.err }

// Options configure a spool. Zero values take the defaults above.
type Options struct {
	Name          string // writer name, used in logs and the dead-letter file name
	MaxBytes      int64  // cap on undelivered frame bytes
	SegmentBytes  int64  // a segment is closed and a new one started past this size
	MaxBatch      int    // items per Deliver call
	MaxBatchBytes int64  // payload bytes per Deliver call (a single larger item is still delivered)
	MinBackoff    time.Duration
	MaxBackoff    time.Duration
	// Deliver hands items to the sink, oldest first. It returns how many leading items the sink
	// now holds; with an error, items[done] failed: Permanent(err) dead-letters it and replay
	// continues, any other error stops this pass and retries later with backoff. Nil disables
	// the replayer (items stay on disk).
	Deliver func(ctx context.Context, items []Item) (done int, err error)
}

// Item is one spooled payload, readable until Deliver returns.
type Item struct {
	f    *os.File
	from string // segment@offset, for dead-letter reasons
	off  int64  // payload offset
	size int64
}

// Size is the payload size in bytes.
func (it Item) Size() int64 { return it.size }

// Reader streams the payload.
func (it Item) Reader() io.Reader { return io.NewSectionReader(it.f, it.off, it.size) }

// Bytes reads the whole payload.
func (it Item) Bytes() ([]byte, error) {
	b := make([]byte, it.size)
	if n, err := it.f.ReadAt(b, it.off); n < len(b) {
		return nil, err
	}
	return b, nil
}

type segment struct {
	name  string
	size  int64 // bytes of complete frames
	acked int64 // offset of the first undelivered frame
	items int64 // undelivered frames
}

// Spool is one writer's durable queue. Append is safe for concurrent use.
type Spool struct {
	dir  string
	opts Options
	lock io.Closer

	mu     sync.Mutex
	segs   []*segment // oldest first; the last one is being appended to while active != nil
	active *os.File
	seq    int
	closed bool

	pendingBytes atomic.Int64
	pendingItems atomic.Int64
	spooled      atomic.Int64
	replayed     atomic.Int64
	dropped      atomic.Int64
	dead         atomic.Int64
	deadTotal    atomic.Int64
	failing      atomic.Bool
	notBefore    atomic.Int64 // unix nanos: no replay attempt before this (backoff)

	errMu     sync.Mutex
	lastErr   string
	lastErrAt time.Time

	deadMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
	kick   chan struct{}
	stop   chan struct{}
	done   chan struct{}
}

// Open opens (or creates) the spool in dir, repairs torn appends left by a crash and starts the
// replayer. Segments from earlier runs are replayed first.
func Open(dir string, opts Options) (*Spool, error) {
	if opts.Name == "" {
		opts.Name = filepath.Base(dir)
	}
	if opts.MaxBytes <= 0 {
		opts.MaxBytes = DefaultMaxBytes
	}
	if opts.SegmentBytes <= 0 {
		opts.SegmentBytes = DefaultSegmentBytes
	}
	if opts.MaxBatch <= 0 {
		opts.MaxBatch = DefaultMaxBatch
	}
	if opts.MaxBatchBytes <= 0 {
		opts.MaxBatchBytes = DefaultMaxBatchBytes
	}
	if opts.MinBackoff <= 0 {
		opts.MinBackoff = DefaultMinBackoff
	}
	if opts.MaxBackoff < opts.MinBackoff {
		opts.MaxBackoff = max(DefaultMaxBackoff, opts.MinBackoff)
	}
	if err := os.MkdirAll(filepath.Join(dir, deadDirName), 0o700); err != nil {
		return nil, err
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	s := &Spool{dir: dir, opts: opts, lock: lock, kick: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{})}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	if err = s.load(); err != nil {
		_ = lock.Close()
		return nil, err
	}
	if n := s.pendingItems.Load(); n > 0 {
		log.Infof("tmx spool %s: %d items (%d bytes) from an earlier run queued for replay", opts.Name, n, s.pendingBytes.Load())
	}
	if opts.Deliver != nil {
		go s.run()
	} else {
		close(s.done)
	}
	s.Kick()
	return s, nil
}

// OpenFirstFree is Open on dir, or on dir-2, dir-3, ... when dir is in use by another spool
// (two stores of the same writer in one process, as in tests). Production runs one per writer.
func OpenFirstFree(dir string, opts Options) (*Spool, error) {
	try := dir
	for i := 2; ; i++ {
		s, err := Open(try, opts)
		if !errors.Is(err, ErrLocked) || i > 20 {
			if err == nil && try != dir {
				log.Warnf("tmx spool %s: %s is in use, spooling to %s", opts.Name, dir, try)
			}
			return s, err
		}
		try = dir + "-" + strconv.Itoa(i)
	}
}

// Dir is where writer name spools by default:
//
//	TMX_SPOOL_DIR/<name>                       when set
//	<archive spool>/durable/<name>             TMX_ARCHIVE_SPOOL, else /CLIProxyAPI/archive-spool
//	                                           when it exists (the persistent volume on prod/staging)
//	WRITABLE_PATH/tmx-spool/<name>             otherwise
//	<temp dir>/tmx-spool/<name>                last resort (lost with the container)
func Dir(name string) string {
	if root := strings.TrimSpace(os.Getenv("TMX_SPOOL_DIR")); root != "" {
		return filepath.Join(root, name)
	}
	for _, root := range []string{os.Getenv("TMX_ARCHIVE_SPOOL"), "/CLIProxyAPI/archive-spool"} {
		if root = strings.TrimSpace(root); root == "" {
			continue
		}
		if st, err := os.Stat(root); err == nil && st.IsDir() {
			return filepath.Join(root, "durable", name)
		}
	}
	if root := strings.TrimSpace(os.Getenv("WRITABLE_PATH")); root != "" {
		return filepath.Join(root, "tmx-spool", name)
	}
	return filepath.Join(os.TempDir(), "tmx-spool", name)
}

// MaxBytesFromEnv is the size cap for writer name: TMX_SPOOL_<NAME>_MAX_MB, else TMX_SPOOL_MAX_MB,
// else def.
func MaxBytesFromEnv(name string, def int64) int64 {
	for _, key := range []string{"TMX_SPOOL_" + strings.ToUpper(name) + "_MAX_MB", "TMX_SPOOL_MAX_MB"} {
		if n, err := strconv.ParseInt(strings.TrimSpace(os.Getenv(key)), 10, 64); err == nil && n > 0 {
			return n << 20
		}
	}
	return def
}

// load reads the segments left by earlier runs.
func (s *Spool) load() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return err
	}
	var names []string
	for _, e := range entries {
		if n := e.Name(); e.Type().IsRegular() && strings.HasPrefix(n, segPrefix) && strings.HasSuffix(n, segSuffix) {
			names = append(names, n)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		seg, errSeg := s.loadSegment(name)
		if errSeg != nil {
			return errSeg
		}
		if seg.acked >= seg.size {
			s.removeSegmentFiles(name)
			continue
		}
		s.segs = append(s.segs, seg)
		s.pendingBytes.Add(seg.size - seg.acked)
		s.pendingItems.Add(seg.items)
	}
	if raw, errR := os.ReadFile(filepath.Join(s.dir, deadDirName, reasonsName)); errR == nil {
		s.deadTotal.Store(int64(strings.Count(string(raw), "\n")))
	}
	return nil
}

// loadSegment counts a segment's undelivered frames and moves a torn tail to the dead letters.
func (s *Spool) loadSegment(name string) (*segment, error) {
	path := filepath.Join(s.dir, name)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return nil, err
	}
	seg := &segment{name: name, size: st.Size()}
	if raw, errA := os.ReadFile(path + ackSuffix); errA == nil {
		if n, errN := strconv.ParseInt(strings.TrimSpace(string(raw)), 10, 64); errN == nil && n >= 0 && n <= seg.size {
			seg.acked = n
		}
	}
	off := seg.acked
	for off < seg.size {
		n, errF := readFrame(f, off, seg.size, true)
		if errF != nil {
			// Only the last append can be torn; keep its bytes anyway.
			if errDL := s.deadletterRange(f, off, seg.size-off, name, "torn or corrupt frame at load: "+errF.Error()); errDL != nil {
				return nil, errDL
			}
			if errT := f.Truncate(off); errT != nil {
				return nil, errT
			}
			_ = f.Sync()
			log.Warnf("tmx spool %s: %s: %d bytes from offset %d were not a complete frame (moved to deadletter)",
				s.opts.Name, name, seg.size-off, off)
			seg.size = off
			break
		}
		off += headerSize + n
		seg.items++
	}
	return seg, nil
}

// readFrame validates the frame at off (fully inside limit) and returns its payload length.
func readFrame(f *os.File, off, limit int64, checkCRC bool) (int64, error) {
	var hdr [headerSize]byte
	if limit-off < headerSize {
		return 0, errors.New("short header")
	}
	if _, err := f.ReadAt(hdr[:], off); err != nil {
		return 0, err
	}
	if binary.BigEndian.Uint32(hdr[0:4]) != frameMagic {
		return 0, errors.New("bad magic")
	}
	n := binary.BigEndian.Uint64(hdr[8:16])
	if n == pendingLen || int64(n) < 0 || int64(n) > limit-off-headerSize {
		return 0, errors.New("incomplete payload")
	}
	if checkCRC {
		h := crc32.New(crcTable)
		if _, err := io.Copy(h, io.NewSectionReader(f, off+headerSize, int64(n))); err != nil {
			return 0, err
		}
		if h.Sum32() != binary.BigEndian.Uint32(hdr[4:8]) {
			return 0, errors.New("checksum mismatch")
		}
	}
	return int64(n), nil
}

// Append spools one payload (fsynced before it returns).
func (s *Spool) Append(payload []byte) error {
	return s.AppendFunc(int64(len(payload)), func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	})
}

// AppendFunc spools one payload streamed by write; sizeHint is checked against the cap.
// On any error nothing is spooled and the item is counted as dropped.
func (s *Spool) AppendFunc(sizeHint int64, write func(io.Writer) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return s.drop(ErrClosed)
	}
	if s.pendingBytes.Load()+headerSize+max(sizeHint, 0) > s.opts.MaxBytes {
		return s.drop(fmt.Errorf("%w (%d of %d bytes pending)", ErrFull, s.pendingBytes.Load(), s.opts.MaxBytes))
	}
	f, seg, err := s.activeLocked()
	if err != nil {
		return s.drop(err)
	}
	start := seg.size
	n, err := writeFrame(f, start, write)
	if err != nil {
		_ = f.Truncate(start)
		return s.drop(err)
	}
	seg.size = start + headerSize + n
	seg.items++
	s.pendingBytes.Add(headerSize + n)
	s.pendingItems.Add(1)
	s.spooled.Add(1)
	s.Kick()
	return nil
}

// AppendBatch spools several payloads with one fsync. It is all or nothing: on an error none is
// spooled and every one is counted as dropped.
func (s *Spool) AppendBatch(payloads [][]byte) error {
	if len(payloads) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dropAll := func(err error) error {
		s.dropped.Add(int64(len(payloads) - 1))
		return s.drop(err)
	}
	if s.closed {
		return dropAll(ErrClosed)
	}
	var total int64
	for _, p := range payloads {
		total += headerSize + int64(len(p))
	}
	if s.pendingBytes.Load()+total > s.opts.MaxBytes {
		return dropAll(fmt.Errorf("%w (%d of %d bytes pending, batch of %d)", ErrFull, s.pendingBytes.Load(), s.opts.MaxBytes, len(payloads)))
	}
	f, seg, err := s.activeLocked()
	if err != nil {
		return dropAll(err)
	}
	start, off := seg.size, seg.size
	for _, p := range payloads {
		n, errW := writeFrameNoSync(f, off, func(w io.Writer) error {
			_, errP := w.Write(p)
			return errP
		})
		if errW != nil {
			_ = f.Truncate(start)
			return dropAll(errW)
		}
		off += headerSize + n
	}
	if err = f.Sync(); err != nil {
		_ = f.Truncate(start)
		return dropAll(err)
	}
	seg.size = off
	seg.items += int64(len(payloads))
	s.pendingBytes.Add(off - start)
	s.pendingItems.Add(int64(len(payloads)))
	s.spooled.Add(int64(len(payloads)))
	s.Kick()
	return nil
}

// writeFrame writes a frame at off (length patched in after the payload) and fsyncs it.
func writeFrame(f *os.File, off int64, write func(io.Writer) error) (int64, error) {
	n, err := writeFrameNoSync(f, off, write)
	if err != nil {
		return 0, err
	}
	return n, f.Sync()
}

func writeFrameNoSync(f *os.File, off int64, write func(io.Writer) error) (int64, error) {
	var hdr [headerSize]byte
	binary.BigEndian.PutUint32(hdr[0:4], frameMagic)
	binary.BigEndian.PutUint64(hdr[8:16], pendingLen)
	if _, err := f.WriteAt(hdr[:], off); err != nil {
		return 0, err
	}
	w := &frameWriter{f: f, off: off + headerSize, h: crc32.New(crcTable)}
	if err := write(w); err != nil {
		return 0, err
	}
	binary.BigEndian.PutUint32(hdr[4:8], w.h.Sum32())
	binary.BigEndian.PutUint64(hdr[8:16], uint64(w.n))
	if _, err := f.WriteAt(hdr[:], off); err != nil {
		return 0, err
	}
	return w.n, nil
}

type frameWriter struct {
	f   *os.File
	off int64
	n   int64
	h   hash.Hash32
}

func (w *frameWriter) Write(p []byte) (int, error) {
	n, err := w.f.WriteAt(p, w.off+w.n)
	w.n += int64(n)
	_, _ = w.h.Write(p[:n])
	return n, err
}

// activeLocked returns the segment being appended to, starting a new one when needed.
func (s *Spool) activeLocked() (*os.File, *segment, error) {
	if s.active != nil {
		seg := s.segs[len(s.segs)-1]
		if seg.size < s.opts.SegmentBytes {
			return s.active, seg, nil
		}
		_ = s.active.Close()
		s.active = nil
	}
	s.seq++
	name := fmt.Sprintf("%s%019d-%06d%s", segPrefix, time.Now().UnixNano(), s.seq, segSuffix)
	f, err := os.OpenFile(filepath.Join(s.dir, name), os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, nil, err
	}
	syncDir(s.dir)
	seg := &segment{name: name}
	s.segs = append(s.segs, seg)
	s.active = f
	return f, seg, nil
}

func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// drop counts a lost item and says so loudly.
func (s *Spool) drop(err error) error {
	n := s.dropped.Add(1)
	s.NoteError(err)
	if n <= 10 || n%100 == 1 {
		log.WithError(err).Errorf("tmx spool %s: ITEM DROPPED, could not spool it (%d dropped since start)", s.opts.Name, n)
	}
	return err
}

// NoteError records err as the writer's last error (shown by the durability endpoint).
func (s *Spool) NoteError(err error) {
	if err == nil {
		return
	}
	s.errMu.Lock()
	s.lastErr, s.lastErrAt = err.Error(), time.Now().UTC()
	s.errMu.Unlock()
}

// Kick asks the replayer to try now (it still honours the backoff after a failure).
func (s *Spool) Kick() {
	select {
	case s.kick <- struct{}{}:
	default:
	}
}

// Recovered tells the replayer the sink works again (a live write just succeeded): it retries
// right away instead of waiting out its backoff.
func (s *Spool) Recovered() {
	if s.pendingItems.Load() == 0 && !s.failing.Load() {
		return
	}
	s.notBefore.Store(0)
	s.Kick()
}

// Failing reports that the last replay attempt failed and none has succeeded since; writers
// spool new items directly instead of waiting on a sink that is known to be down.
func (s *Spool) Failing() bool { return s != nil && s.failing.Load() }

// Pending is the number of spooled items not yet delivered.
func (s *Spool) Pending() int64 {
	if s == nil {
		return 0
	}
	return s.pendingItems.Load()
}

// Dir is the spool's directory.
func (s *Spool) Dir() string { return s.dir }

// Stats returns the spool's counters (Queued is the writer's to fill in).
func (s *Spool) Stats() durability.Stats {
	st := durability.Stats{
		Spooled:         s.spooled.Load(),
		SpooledPending:  s.pendingItems.Load(),
		SpooledBytes:    s.pendingBytes.Load(),
		Replayed:        s.replayed.Load(),
		Dropped:         s.dropped.Load(),
		Deadlettered:    s.dead.Load(),
		DeadletterTotal: s.deadTotal.Load(),
		SpoolDir:        s.dir,
	}
	s.errMu.Lock()
	if s.lastErr != "" {
		at := s.lastErrAt
		st.LastError, st.LastErrorAt = s.lastErr, &at
	}
	s.errMu.Unlock()
	return st
}

// Close stops the replayer (cancelling a delivery in progress) and closes the spool. Undelivered
// items stay on disk for the next Open.
func (s *Spool) Close() {
	if s == nil {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	close(s.stop)
	s.cancel()
	<-s.done
	s.mu.Lock()
	if s.active != nil {
		_ = s.active.Close()
		s.active = nil
	}
	s.mu.Unlock()
	_ = s.lock.Close()
}

// run is the replayer: drain whenever kicked, backing off while the sink fails.
func (s *Spool) run() {
	defer close(s.done)
	backoff := s.opts.MinBackoff
	timer := time.NewTimer(s.opts.MaxBackoff)
	defer timer.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-s.kick:
		case <-timer.C:
		}
		if wait := time.Until(time.Unix(0, s.notBefore.Load())); wait > 0 {
			timer.Reset(wait)
			continue
		}
		if s.pendingItems.Load() == 0 {
			s.failing.Store(false)
			timer.Reset(s.opts.MaxBackoff)
			continue
		}
		err := s.drain()
		if s.ctx.Err() != nil {
			return
		}
		if err == nil {
			s.failing.Store(false)
			backoff = s.opts.MinBackoff
			s.notBefore.Store(0)
			timer.Reset(s.opts.MaxBackoff)
			continue
		}
		if !s.failing.Swap(true) {
			log.WithError(err).Warnf("tmx spool %s: replay failed; %d items stay spooled and will retry", s.opts.Name, s.pendingItems.Load())
		}
		s.NoteError(err)
		s.notBefore.Store(time.Now().Add(backoff).UnixNano())
		timer.Reset(backoff)
		backoff = min(2*backoff, s.opts.MaxBackoff)
	}
}

// drain delivers every complete frame, oldest first, until the sink fails or nothing is left.
func (s *Spool) drain() error {
	for {
		s.mu.Lock()
		if len(s.segs) == 0 {
			s.mu.Unlock()
			return nil
		}
		seg := s.segs[0]
		size, acked := seg.size, seg.acked
		s.mu.Unlock()
		if acked >= size {
			s.retire(seg)
			continue
		}
		if err := s.drainSegment(seg, acked, size); err != nil {
			return err
		}
	}
}

func (s *Spool) drainSegment(seg *segment, off, size int64) error {
	f, err := os.Open(filepath.Join(s.dir, seg.name))
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	for off < size {
		var batch []Item
		var ends []int64
		var bytes int64
		next := off
		for next < size && len(batch) < s.opts.MaxBatch && (len(batch) == 0 || bytes < s.opts.MaxBatchBytes) {
			n, errF := readFrame(f, next, size, true)
			if errF != nil {
				if len(batch) > 0 {
					break // deliver what precedes it first
				}
				// Corruption inside a complete frame (disk error): keep the rest of the segment.
				if errDL := s.deadletterRange(f, next, size-next, seg.name, "corrupt frame: "+errF.Error()); errDL != nil {
					return errDL
				}
				s.ack(seg, size, size-next, s.itemsLeft(seg))
				return nil
			}
			batch = append(batch, Item{f: f, from: fmt.Sprintf("%s@%d", seg.name, next), off: next + headerSize, size: n})
			ends = append(ends, next+headerSize+n)
			bytes += n
			next += headerSize + n
		}
		done, errD := s.opts.Deliver(s.ctx, batch)
		done = min(max(done, 0), len(batch))
		if done > 0 {
			s.replayed.Add(int64(done))
			s.ack(seg, ends[done-1], ends[done-1]-off, int64(done))
			off = ends[done-1]
		}
		if errD == nil {
			if done < len(batch) {
				return fmt.Errorf("deliver took %d of %d items without an error", done, len(batch))
			}
			continue
		}
		if !IsPermanent(errD) || done >= len(batch) {
			return errD
		}
		it := batch[done]
		if errDL := s.deadletterRange(f, it.off, it.size, it.from, errD.Error()); errDL != nil {
			return errDL // retried later: a refused item is never acknowledged before it is kept
		}
		s.ack(seg, ends[done], ends[done]-off, 1)
		off = ends[done]
	}
	return nil
}

// ack records that seg is delivered up to off (bytes / items fewer pending).
func (s *Spool) ack(seg *segment, off, bytes, items int64) {
	s.mu.Lock()
	seg.acked = off
	seg.items -= items
	s.mu.Unlock()
	s.pendingBytes.Add(-bytes)
	s.pendingItems.Add(-items)
	path := filepath.Join(s.dir, seg.name+ackSuffix)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(strconv.FormatInt(off, 10)), 0o600); err == nil {
		_ = os.Rename(tmp, path)
	}
}

// retire removes a fully delivered segment (the active one too, when nothing was appended since
// the caller looked; otherwise the caller's next pass delivers the new frames).
func (s *Spool) retire(seg *segment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.segs) == 0 || s.segs[0] != seg || seg.acked < seg.size {
		return
	}
	if len(s.segs) == 1 && s.active != nil {
		_ = s.active.Close()
		s.active = nil
	}
	s.segs = s.segs[1:]
	s.removeSegmentFiles(seg.name)
}

func (s *Spool) removeSegmentFiles(name string) {
	path := filepath.Join(s.dir, name)
	_ = os.Remove(path + ackSuffix)
	_ = os.Remove(path)
}

// Deadletter stores a payload the sink refused for good, with the reason. Dead letters are never
// deleted; DeadletterTotal counts them across runs.
// A failed dead-letter write loses the payload and is counted as a drop.
func (s *Spool) Deadletter(payload []byte, reason string) error {
	err := s.deadletter(int64(len(payload)), func(w io.Writer) error {
		_, err := w.Write(payload)
		return err
	}, "live", reason)
	if err != nil {
		s.drop(fmt.Errorf("dead-letter write failed (%s): %w", reason, err))
	}
	return err
}

// itemsLeft is how many undelivered frames seg still counts.
func (s *Spool) itemsLeft(seg *segment) int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return seg.items
}

func (s *Spool) deadletterRange(f *os.File, off, size int64, from, reason string) error {
	return s.deadletter(size, func(w io.Writer) error {
		_, err := io.Copy(w, io.NewSectionReader(f, off, size))
		return err
	}, from, reason)
}

type deadReason struct {
	TS     time.Time `json:"ts"`
	Writer string    `json:"writer"`
	File   string    `json:"file"`
	Offset int64     `json:"offset"`
	Bytes  int64     `json:"bytes"`
	From   string    `json:"from"`
	Reason string    `json:"reason"`
}

func (s *Spool) deadletter(size int64, write func(io.Writer) error, from, reason string) error {
	s.deadMu.Lock()
	defer s.deadMu.Unlock()
	dir := filepath.Join(s.dir, deadDirName)
	name := s.opts.Name + segSuffix
	err := func() error {
		f, err := os.OpenFile(filepath.Join(dir, name), os.O_RDWR|os.O_CREATE, 0o600)
		if err != nil {
			return err
		}
		defer func() { _ = f.Close() }()
		st, err := f.Stat()
		if err != nil {
			return err
		}
		n, err := writeFrame(f, st.Size(), write)
		if err != nil {
			_ = f.Truncate(st.Size())
			return err
		}
		line, _ := json.Marshal(deadReason{TS: time.Now().UTC(), Writer: s.opts.Name, File: name,
			Offset: st.Size(), Bytes: n, From: from, Reason: reason})
		r, err := os.OpenFile(filepath.Join(dir, reasonsName), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
		if err != nil {
			return err
		}
		defer func() { _ = r.Close() }()
		if _, err = r.Write(append(line, '\n')); err != nil {
			return err
		}
		return r.Sync()
	}()
	if err != nil {
		s.NoteError(fmt.Errorf("dead-letter write failed: %w", err))
		return err
	}
	s.dead.Add(1)
	s.deadTotal.Add(1)
	s.NoteError(errors.New("dead-lettered: " + reason))
	log.Warnf("tmx spool %s: item (%d bytes) dead-lettered to %s: %s", s.opts.Name, size, filepath.Join(dir, name), reason)
	return nil
}
