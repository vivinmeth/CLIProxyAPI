package archive

import (
	"bytes"
	"container/list"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	log "github.com/sirupsen/logrus"
)

// BlobStore is a content-addressed store: every blob is keyed by the sha256 of its raw bytes and
// written once. Bytes are zstd-compressed into a local spool directory first, indexed in
// Postgres, then uploaded to R2 by a background loop and removed from the spool. A blob is
// readable from the spool until it is uploaded and from R2 afterwards, so an R2 outage never
// blocks or loses a write.
type BlobStore struct {
	db     *sql.DB
	schema string
	r2     *minio.Client
	bucket string
	prefix string // "<env>/blobs"
	spool  string

	enc *zstd.Encoder
	dec *zstd.Decoder

	known   *lruSet
	kick    chan struct{}
	stopped chan struct{}
	stop    chan struct{}

	stored   atomic.Int64
	dedup    atomic.Int64
	uploaded atomic.Int64
	failures atomic.Int64
}

func newBlobStore(db *sql.DB, schema string, r2 *minio.Client, bucket, env, spool string) (*BlobStore, error) {
	if err := os.MkdirAll(spool, 0o750); err != nil {
		return nil, fmt.Errorf("spool dir: %w", err)
	}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return nil, err
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	b := &BlobStore{
		db: db, schema: schema, r2: r2, bucket: bucket, prefix: env + "/blobs", spool: spool,
		enc: enc, dec: dec, known: newLRUSet(200000),
		kick: make(chan struct{}, 1), stop: make(chan struct{}), stopped: make(chan struct{}),
	}
	go b.uploadLoop()
	return b, nil
}

// HashOf returns the blob key for data.
func HashOf(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// ValidHash reports whether s looks like a blob key.
func ValidHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

func (b *BlobStore) objectKey(hash string) string {
	return b.prefix + "/" + hash[:2] + "/" + hash[2:4] + "/" + hash + ".zst"
}

func (b *BlobStore) spoolPath(hash string) string {
	return filepath.Join(b.spool, hash+".zst")
}

// Put stores data and returns its hash. Already-stored content costs one index lookup at most.
func (b *BlobStore) Put(ctx context.Context, data []byte, mediaType, kind string) (string, error) {
	hash := HashOf(data)
	if b.known.Has(hash) {
		b.dedup.Add(1)
		return hash, nil
	}
	var exists bool
	if err := b.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM `+b.schema+`.blobs WHERE hash = $1)`, hash).Scan(&exists); err != nil {
		return "", fmt.Errorf("blob lookup: %w", err)
	}
	if exists {
		b.known.Add(hash)
		b.dedup.Add(1)
		return hash, nil
	}
	compressed := b.enc.EncodeAll(data, nil)
	path := b.spoolPath(hash)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, compressed, 0o640); err != nil {
		return "", fmt.Errorf("spool write: %w", err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return "", fmt.Errorf("spool rename: %w", err)
	}
	if _, err := b.db.ExecContext(ctx, `INSERT INTO `+b.schema+`.blobs
		(hash, size, stored_size, media_type, kind) VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (hash) DO NOTHING`, hash, len(data), len(compressed), mediaType, kind); err != nil {
		return "", fmt.Errorf("blob index: %w", err)
	}
	b.known.Add(hash)
	b.stored.Add(1)
	select {
	case b.kick <- struct{}{}:
	default:
	}
	return hash, nil
}

// Missing returns the subset of hashes that are not stored yet.
func (b *BlobStore) Missing(ctx context.Context, hashes []string) ([]string, error) {
	missing := []string{}
	for _, h := range hashes {
		if b.known.Has(h) {
			continue
		}
		var exists bool
		if err := b.db.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM `+b.schema+`.blobs WHERE hash = $1)`, h).Scan(&exists); err != nil {
			return nil, err
		}
		if exists {
			b.known.Add(h)
		} else {
			missing = append(missing, h)
		}
	}
	return missing, nil
}

// Get returns the raw bytes of a blob.
func (b *BlobStore) Get(ctx context.Context, hash string) ([]byte, error) {
	if !ValidHash(hash) {
		return nil, fmt.Errorf("invalid blob hash")
	}
	compressed, err := os.ReadFile(b.spoolPath(hash))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		if b.r2 == nil {
			return nil, fmt.Errorf("blob %s not found", hash)
		}
		obj, errGet := b.r2.GetObject(ctx, b.bucket, b.objectKey(hash), minio.GetObjectOptions{})
		if errGet != nil {
			return nil, errGet
		}
		defer func() { _ = obj.Close() }()
		if compressed, err = io.ReadAll(obj); err != nil {
			return nil, err
		}
	}
	return b.dec.DecodeAll(compressed, nil)
}

// MediaType returns the stored media type of a blob ("" when unknown).
func (b *BlobStore) MediaType(ctx context.Context, hash string) string {
	var mt string
	_ = b.db.QueryRowContext(ctx, `SELECT media_type FROM `+b.schema+`.blobs WHERE hash = $1`, hash).Scan(&mt)
	return mt
}

func (b *BlobStore) uploadLoop() {
	defer close(b.stopped)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		b.uploadPending()
		select {
		case <-b.stop:
			b.uploadPending()
			return
		case <-b.kick:
		case <-ticker.C:
		}
	}
}

const uploadConcurrency = 8

// uploadPending pushes every spooled blob to R2 with a few parallel uploads (bulk imports produce
// thousands of small blobs). Without R2 (dev) blobs simply stay spooled.
func (b *BlobStore) uploadPending() {
	if b.r2 == nil {
		return
	}
	entries, err := os.ReadDir(b.spool)
	if err != nil {
		return
	}
	work := make(chan string)
	var failed atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < uploadConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for hash := range work {
				if failed.Load() {
					continue
				}
				if errUp := b.uploadOne(hash); errUp != nil {
					failed.Store(true)
					if b.failures.Add(1)%50 == 1 {
						log.WithError(errUp).Warn("tmx archive: upload to R2 failed; blobs stay spooled and will retry")
					}
				}
			}
		}()
	}
	for _, entry := range entries {
		name := entry.Name()
		hash := strings.TrimSuffix(name, ".zst")
		if !strings.HasSuffix(name, ".zst") || !ValidHash(hash) {
			continue
		}
		if failed.Load() {
			break
		}
		work <- hash
	}
	close(work)
	wg.Wait()
}

func (b *BlobStore) uploadOne(hash string) error {
	path := b.spoolPath(hash)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil // already uploaded by another pass
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err = b.r2.PutObject(ctx, b.bucket, b.objectKey(hash), bytes.NewReader(data), int64(len(data)),
		minio.PutObjectOptions{ContentType: "application/zstd"}); err != nil {
		return err
	}
	if _, err = b.db.ExecContext(ctx, `UPDATE `+b.schema+`.blobs SET uploaded_at = now() WHERE hash = $1`, hash); err != nil {
		return err
	}
	_ = os.Remove(path)
	b.uploaded.Add(1)
	return nil
}

// SpoolBacklog counts blobs waiting for upload.
func (b *BlobStore) SpoolBacklog() int {
	entries, err := os.ReadDir(b.spool)
	if err != nil {
		return -1
	}
	n := 0
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".zst") {
			n++
		}
	}
	return n
}

func (b *BlobStore) close() {
	close(b.stop)
	<-b.stopped
}

// lruSet is a bounded set of recently seen hashes, saving index lookups for repeated content
// (system prompts, tool definitions and earlier turns are re-sent on every request).
type lruSet struct {
	mu    sync.Mutex
	limit int
	order *list.List
	items map[string]*list.Element
}

func newLRUSet(limit int) *lruSet {
	return &lruSet{limit: limit, order: list.New(), items: make(map[string]*list.Element)}
}

func (s *lruSet) Has(k string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[k]; ok {
		s.order.MoveToFront(el)
		return true
	}
	return false
}

func (s *lruSet) Add(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[k]; ok {
		s.order.MoveToFront(el)
		return
	}
	s.items[k] = s.order.PushFront(k)
	for s.order.Len() > s.limit {
		last := s.order.Back()
		s.order.Remove(last)
		delete(s.items, last.Value.(string))
	}
}
