package capture

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	log "github.com/sirupsen/logrus"
)

const DefaultLiveMaxBytes int64 = 512 << 20
const DefaultExchangeMaxBytes int64 = 8 << 20

type captureLimits struct {
	max, exchange int64
	spool         string
}

var limits atomic.Pointer[captureLimits]
var liveBytes atomic.Int64
var captureDrops atomic.Int64

// Configure is safe during hot reload. Existing bodies keep their owned storage.
func Configure(max, exchange int64, spool string) {
	if max <= 0 {
		max = DefaultLiveMaxBytes
	}
	if exchange <= 0 {
		exchange = DefaultExchangeMaxBytes
	}
	if spool == "" {
		spool = os.Getenv("TMX_ARCHIVE_SPOOL")
	}
	if spool == "" {
		spool = "/CLIProxyAPI/archive-spool"
	}
	limits.Store(&captureLimits{max, exchange, spool})
}
func currentLimits() *captureLimits {
	if p := limits.Load(); p != nil {
		return p
	}
	return &captureLimits{DefaultLiveMaxBytes, DefaultExchangeMaxBytes, "/CLIProxyAPI/archive-spool"}
}
func reserve(n int64) bool {
	for {
		old := liveBytes.Load()
		if n > currentLimits().max-old {
			return false
		}
		if liveBytes.CompareAndSwap(old, old+n) {
			return true
		}
	}
}
func LiveBytes() int64 { return liveBytes.Load() }
func Dropped() int64   { return captureDrops.Load() }

// Body owns a bounded memory capture or a spool file. Ownership moves to Exchange;
// Release must run after archiving or dropping it. Writes never affect client delivery.
// Group shares the per-exchange memory threshold across body and timing sources.
type Group struct{ bytes atomic.Int64 }

func (g *Group) NewBody() *Body { return &Body{group: g} }

type Body struct {
	group    *Group
	parts    [][]byte
	file     *os.File
	size     int64
	reserved int64
	err      error
	once     sync.Once
}

func (b *Body) Write(p []byte) (int, error) {
	if b.err != nil {
		return 0, b.err
	}
	if len(p) == 0 {
		return 0, nil
	}
	// Account for the retained slice header as well as its exact-sized backing array.
	n := int64(len(p)) + 24
	if b.file == nil {
		used := b.reserved
		if b.group != nil {
			used = b.group.bytes.Load()
		}
		if used+n > currentLimits().exchange || !reserve(n) {
			if err := b.spill(); err != nil {
				return 0, b.fail(err)
			}
		} else {
			b.reserved += n
			if b.group != nil {
				b.group.bytes.Add(n)
			}
			b.parts = append(b.parts, bytes.Clone(p))
			b.size += int64(len(p))
			return len(p), nil
		}
	}
	written, err := b.file.Write(p)
	b.size += int64(written)
	if err != nil {
		return written, b.fail(err)
	}
	return written, nil
}
func (b *Body) spill() error {
	dir := filepath.Join(currentLimits().spool, "live")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, "capture-*")
	if err != nil {
		return err
	}
	b.file = f
	for _, p := range b.parts {
		if _, err = f.Write(p); err != nil {
			return err
		}
	}
	b.parts = nil
	liveBytes.Add(-b.reserved)
	if b.group != nil {
		b.group.bytes.Add(-b.reserved)
	}
	b.reserved = 0
	return nil
}
func (b *Body) fail(err error) error {
	if b.err == nil {
		b.err = err
		if captureDrops.Add(1)%20 == 1 {
			log.WithError(err).Warn("tmx capture: disk spill failed; dropping capture")
		}
		b.Release()
	}
	return err
}
func (b *Body) MemorySize() int64 {
	if b == nil {
		return 0
	}
	return b.reserved
}
func (b *Body) Size() int64 {
	if b == nil {
		return 0
	}
	return b.size
}
func (b *Body) Err() error {
	if b == nil {
		return nil
	}
	return b.err
}
func (b *Body) Spilled() bool { return b != nil && b.file != nil }

// Reader never materializes a spilled body. The caller must finish before Release.
func (b *Body) Reader() (io.Reader, error) {
	if b == nil {
		return bytes.NewReader(nil), nil
	}
	if b.err != nil {
		return nil, b.err
	}
	if b.file != nil {
		return io.NewSectionReader(b.file, 0, b.size), nil
	}
	readers := make([]io.Reader, 0, len(b.parts))
	for _, p := range b.parts {
		readers = append(readers, bytes.NewReader(p))
	}
	return io.MultiReader(readers...), nil
}

// MemoryBytes permits existing JSON splitting only when it fits the live budget.
// File captures stay streamed; multi-part memory copies are reserved before allocation.
func (b *Body) MemoryBytes() ([]byte, func(), bool) {
	if b == nil || b.file != nil || b.err != nil {
		return nil, func() {}, false
	}
	if len(b.parts) == 0 {
		return nil, func() {}, true
	}
	if len(b.parts) == 1 {
		return b.parts[0], func() {}, true
	}
	if !reserve(b.size) {
		return nil, func() {}, false
	}
	data := make([]byte, 0, int(b.size))
	for _, p := range b.parts {
		data = append(data, p...)
	}
	return data, func() { liveBytes.Add(-b.size) }, true
}
func (b *Body) Prefix(limit int64) []byte {
	reader, err := b.Reader()
	if err != nil {
		return nil
	}
	data, _ := io.ReadAll(io.LimitReader(reader, limit))
	return data
}
func (b *Body) Release() {
	if b == nil {
		return
	}
	b.once.Do(func() {
		if b.file != nil {
			name := b.file.Name()
			_ = b.file.Close()
			_ = os.Remove(name)
		}
		b.parts = nil
		liveBytes.Add(-b.reserved)
		if b.group != nil {
			b.group.bytes.Add(-b.reserved)
		}
		b.reserved = 0
	})
}

// Capture copies payload and any pre-existing spool source through the bounded writer.
func Capture(payload []byte, source io.WriterTo) *Body { return CaptureInGroup(payload, source, nil) }
func CaptureInGroup(payload []byte, source io.WriterTo, group *Group) *Body {
	b := &Body{group: group}
	if _, err := b.Write(payload); err != nil {
		return b
	}
	if source != nil {
		if _, err := source.WriteTo(b); err != nil {
			b.fail(err)
		}
	}
	return b
}
func (ex *Exchange) Release() {
	if ex == nil {
		return
	}
	for _, body := range ex.Bodies {
		body.Release()
	}
}
func (ex *Exchange) BodySize(name string, fallback []byte) int64 {
	if b := ex.Bodies[name]; b != nil {
		return b.Size()
	}
	return int64(len(fallback))
}
func (ex *Exchange) CaptureError() error {
	for _, b := range ex.Bodies {
		if b.Err() != nil {
			return b.Err()
		}
	}
	return nil
}

var _ io.Writer = (*Body)(nil)
