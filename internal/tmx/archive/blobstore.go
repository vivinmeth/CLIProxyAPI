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
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	log "github.com/sirupsen/logrus"
)

// BlobStore is a content-addressed store: every blob is keyed by the sha256 of its raw bytes and
// written once. Bytes are zstd-compressed into a local spool directory first, indexed in
// Postgres, then uploaded to R2 by a background loop and removed from the spool. A blob is
// readable from the spool until it is uploaded and from R2 afterwards, so an R2 outage never
// blocks or loses a write.
//
// R2 bills per object written, and a request produces dozens of small blobs, so the upload loop
// concatenates everything spooled since the last pass into one pack object and records each
// blob's pack key and byte offset; reads fetch just that byte range. Blobs written before packs
// existed, and blobs too big to be worth packing, are stored as one object per hash.
type BlobStore struct {
	db     *sql.DB
	schema string
	r2     *minio.Client
	bucket string
	prefix string // "<env>/blobs"
	// readPrefixes are other environments' blob folders tried for loose blobs this environment
	// doesn't have (TMX_ARCHIVE_READ_FALLBACK_ENVS, e.g. staging running on a copy of prod's
	// index). Read-only: uploads always go to prefix.
	readPrefixes []string
	spool        string

	enc *zstd.Encoder
	dec *zstd.Decoder

	known     *lruSet
	cache     *blobCache
	lastSweep time.Time // upload loop only
	// hashLocks serialise Put's spool write + index insert with the uploader's removal of an
	// unindexed spool file of the same hash (striped by hash).
	hashLocks [64]sync.Mutex
	kick      chan struct{}
	stopped   chan struct{}
	stop      chan struct{}

	stored   atomic.Int64
	dedup    atomic.Int64
	uploaded atomic.Int64
	packs    atomic.Int64
	failures atomic.Int64
	swept    atomic.Int64
	// mismatched: objects found at a blob's key holding other bytes (left as is; blob packed).
	mismatched atomic.Int64
}

func newBlobStore(db *sql.DB, schema string, r2 *minio.Client, bucket, env, spool string) (*BlobStore, error) {
	capture.SweepSpills(spool)
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
		enc: enc, dec: dec, known: newLRUSet(200000), cache: newBlobCache(spool),
		kick: make(chan struct{}, 1), stop: make(chan struct{}), stopped: make(chan struct{}),
	}
	for _, fb := range strings.Split(os.Getenv("TMX_ARCHIVE_READ_FALLBACK_ENVS"), ",") {
		if fb = strings.TrimSpace(fb); fb != "" && fb != env {
			b.readPrefixes = append(b.readPrefixes, fb+"/blobs")
		}
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
	// The whole check-then-store runs under the hash's lock: the sweep deletes a row and forgets
	// the hash under the same lock, so a lookup can't re-learn a row that is being deleted.
	lock := b.hashLock(hash)
	lock.Lock()
	defer lock.Unlock()
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
		exists, err := b.lookup(ctx, h)
		if err != nil {
			return nil, err
		}
		if !exists {
			missing = append(missing, h)
		}
	}
	return missing, nil
}

// lookup reports whether a blob is indexed, remembering hits; under the hash's lock (see Put).
func (b *BlobStore) lookup(ctx context.Context, h string) (bool, error) {
	lock := b.hashLock(h)
	lock.Lock()
	defer lock.Unlock()
	if b.known.Has(h) {
		return true, nil
	}
	var exists bool
	if err := b.db.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM `+b.schema+`.blobs WHERE hash = $1)`, h).Scan(&exists); err != nil {
		return false, err
	}
	if exists {
		b.known.Add(h)
	}
	return exists, nil
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
		if cached, ok := b.cache.get(hash); ok {
			return b.dec.DecodeAll(cached, nil)
		}
		key, opts := b.objectKey(hash), minio.GetObjectOptions{}
		var packKey sql.NullString
		var packOffset, storedSize sql.NullInt64
		if errIdx := b.db.QueryRowContext(ctx, `SELECT pack_key, pack_offset, stored_size FROM `+b.schema+
			`.blobs WHERE hash = $1`, hash).Scan(&packKey, &packOffset, &storedSize); errIdx != nil &&
			!errors.Is(errIdx, sql.ErrNoRows) {
			return nil, errIdx
		}
		if packKey.Valid && packKey.String != "" {
			key = packKey.String
			if errRange := opts.SetRange(packOffset.Int64, packOffset.Int64+storedSize.Int64-1); errRange != nil {
				return nil, errRange
			}
		}
		if compressed, err = b.readObject(ctx, key, opts); err != nil {
			if packKey.Valid && packKey.String != "" {
				return nil, err
			}
			if compressed, err = b.readFallback(ctx, hash, err); err != nil {
				return nil, err
			}
		}
		b.cache.put(hash, compressed)
	}
	return b.dec.DecodeAll(compressed, nil)
}

func (b *BlobStore) readObject(ctx context.Context, key string, opts minio.GetObjectOptions) ([]byte, error) {
	obj, err := b.r2.GetObject(ctx, b.bucket, key, opts)
	if err != nil {
		return nil, err
	}
	defer func() { _ = obj.Close() }()
	return io.ReadAll(obj)
}

// readFallback tries the fallback environments for a loose blob; first is the original error.
func (b *BlobStore) readFallback(ctx context.Context, hash string, first error) ([]byte, error) {
	for _, p := range b.readPrefixes {
		if data, err := b.readObject(ctx, p+"/"+hash[:2]+"/"+hash[2:4]+"/"+hash+".zst", minio.GetObjectOptions{}); err == nil {
			return data, nil
		}
	}
	return nil, first
}

// MediaType returns the stored media type of a blob ("" when unknown).
func (b *BlobStore) MediaType(ctx context.Context, hash string) string {
	var mt string
	_ = b.db.QueryRowContext(ctx, `SELECT media_type FROM `+b.schema+`.blobs WHERE hash = $1`, hash).Scan(&mt)
	return mt
}

func (b *BlobStore) uploadLoop() {
	defer close(b.stopped)
	ticker := time.NewTicker(packInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.stop:
			b.uploadPending()
			return
		case <-b.kick:
			// Upload early only once enough has piled up (bulk imports); otherwise wait for the
			// ticker so each pass makes one pack instead of one object per request.
			if b.SpoolBacklog() < packMaxBlobs {
				continue
			}
		case <-ticker.C:
		}
		b.uploadPending()
	}
}

const (
	uploadConcurrency = 4
	packInterval      = 30 * time.Second
	packMaxBytes      = 32 << 20 // per pack object
	packMaxBlobs      = 5000
	packLoneBytes     = 8 << 20 // blobs at least this big get their own object
	// strayAge: a spool file without an index row this old is from a Put that failed before
	// indexing; an index row this old without upload or spool file points nowhere.
	strayAge      = time.Hour
	sweepInterval = time.Hour
)

// spooled is one blob waiting in the spool.
type spooled struct {
	hash string
	data []byte
}

// uploadPending pushes every spooled blob to R2, grouped into pack objects. Without R2 (dev) blobs
// simply stay spooled.
func (b *BlobStore) uploadPending() {
	if b.r2 == nil {
		return
	}
	entries, err := os.ReadDir(b.spool)
	if err != nil {
		return
	}
	jobs := make(chan []spooled)
	var failed atomic.Bool
	var wg sync.WaitGroup
	for i := 0; i < uploadConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for group := range jobs {
				if failed.Load() {
					continue
				}
				var errUp error
				if len(group) == 1 && len(group[0].data) >= packLoneBytes {
					errUp = b.uploadOne(group[0])
				} else {
					errUp = b.uploadPack(group)
				}
				if errUp != nil {
					failed.Store(true)
					if b.failures.Add(1)%50 == 1 {
						log.WithError(errUp).Warn("tmx archive: upload to R2 failed; blobs stay spooled and will retry")
					}
				}
			}
		}()
	}
	var group []spooled
	size := 0
	flush := func() {
		if len(group) > 0 && !failed.Load() {
			jobs <- group
		}
		group, size = nil, 0
	}
	for _, entry := range entries {
		if failed.Load() {
			break
		}
		name := entry.Name()
		hash := strings.TrimSuffix(name, ".zst")
		if !strings.HasSuffix(name, ".zst") || !ValidHash(hash) {
			continue
		}
		data, errRead := os.ReadFile(b.spoolPath(hash))
		if errRead != nil {
			continue // uploaded by another pass
		}
		if len(data) >= packLoneBytes {
			jobs <- []spooled{{hash: hash, data: data}}
			continue
		}
		if size+len(data) > packMaxBytes || len(group) >= packMaxBlobs {
			flush()
		}
		group = append(group, spooled{hash: hash, data: data})
		size += len(data)
	}
	flush()
	close(jobs)
	wg.Wait()
	if !failed.Load() && time.Since(b.lastSweep) >= sweepInterval {
		b.lastSweep = time.Now()
		b.sweepUnbacked()
	}
}

// sweepUnbacked deletes index rows that were never uploaded and have no spool file (lost before
// uploads only removed indexed files). Their content is gone; without the row the next Put of
// the same bytes (a re-import) stores them again instead of deduplicating against nothing.
func (b *BlobStore) sweepUnbacked() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	rows, err := b.db.QueryContext(ctx, `SELECT hash FROM `+b.schema+`.blobs
		WHERE uploaded_at IS NULL AND created_at < $1`, time.Now().Add(-strayAge))
	if err != nil {
		return
	}
	var gone []string
	for rows.Next() {
		var h string
		if rows.Scan(&h) != nil {
			continue
		}
		if _, errStat := os.Stat(b.spoolPath(h)); errors.Is(errStat, os.ErrNotExist) {
			gone = append(gone, h)
		}
	}
	if rows.Err() != nil {
		_ = rows.Close()
		return
	}
	_ = rows.Close()
	n := 0
	for _, h := range gone {
		if b.sweepOne(ctx, h) {
			n++
		}
	}
	if n > 0 {
		b.swept.Add(int64(n))
		log.WithField("blobs", n).Warn("tmx archive: removed index rows of blobs that were never uploaded; re-import their sessions")
	}
}

// sweepOne deletes one unbacked row and forgets the hash, under the hash's lock, rechecking that
// no Put has spooled it again meanwhile.
func (b *BlobStore) sweepOne(ctx context.Context, h string) bool {
	lock := b.hashLock(h)
	lock.Lock()
	defer lock.Unlock()
	if _, err := os.Stat(b.spoolPath(h)); !errors.Is(err, os.ErrNotExist) {
		return false
	}
	tag, err := b.db.ExecContext(ctx, `DELETE FROM `+b.schema+`.blobs WHERE hash = $1 AND uploaded_at IS NULL`, h)
	if err != nil {
		log.WithError(err).Warn("tmx archive: sweeping an unbacked blob row failed")
		return false
	}
	b.known.Remove(h)
	n, _ := tag.RowsAffected()
	return n > 0
}

// settleSpooled removes the spool files of uploaded blobs. A file whose index row didn't exist
// yet (Put writes the file before inserting the row) stays and goes into a later pass, unless it
// is old enough to be from a Put that failed.
func (b *BlobStore) settleSpooled(hashes []string, indexed map[string]bool) {
	for _, h := range hashes {
		if indexed[h] {
			_ = os.Remove(b.spoolPath(h))
			continue
		}
		b.removeStray(h)
	}
}

// removeStray removes an unindexed spool file old enough to be from a Put that failed, under the
// hash's lock so a concurrent Put of the same bytes can't lose its fresh file.
func (b *BlobStore) removeStray(h string) {
	lock := b.hashLock(h)
	lock.Lock()
	defer lock.Unlock()
	st, err := os.Stat(b.spoolPath(h))
	if err != nil || time.Since(st.ModTime()) <= strayAge {
		return
	}
	var exists bool
	if b.db.QueryRow(`SELECT EXISTS (SELECT 1 FROM `+b.schema+`.blobs WHERE hash = $1)`, h).Scan(&exists) == nil && !exists {
		_ = os.Remove(b.spoolPath(h))
	}
}

func (b *BlobStore) hashLock(h string) *sync.Mutex {
	var n byte
	if len(h) > 0 {
		n = h[len(h)-1]
	}
	return &b.hashLocks[int(n)%len(b.hashLocks)]
}

// uploadOne stores a single blob as its own object (legacy layout, used for very large blobs).
// Objects are write-once (the bucket may lock the prefix, refusing overwrites), so an object
// already at the key is checked instead of written again: it is there from an earlier attempt
// whose index update failed. One that doesn't hold this blob's bytes is left alone and the blob
// goes into a pack (a new key) instead.
func (b *BlobStore) uploadOne(blob spooled) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	key := b.objectKey(blob.hash)
	held, err := b.objectHolds(ctx, key, blob)
	if err != nil {
		return err
	}
	if held == objectMissing {
		_, errPut := b.r2.PutObject(ctx, b.bucket, key, bytes.NewReader(blob.data),
			int64(len(blob.data)), minio.PutObjectOptions{ContentType: "application/zstd"})
		if errPut != nil {
			// A concurrent or unacknowledged write may have stored it (a locked key refuses the
			// PUT): what's there now decides.
			if held, err = b.objectHolds(ctx, key, blob); err != nil || held == objectMissing {
				return errPut
			}
		} else {
			held = objectMatches
		}
	}
	if held == objectDiffers {
		b.mismatched.Add(1)
		log.WithField("key", key).Error("tmx archive: object at a blob's key holds other bytes; left as is, blob stored in a pack")
		return b.uploadPack([]spooled{blob})
	}
	tag, err := b.db.ExecContext(ctx, `UPDATE `+b.schema+`.blobs SET uploaded_at = now() WHERE hash = $1`, blob.hash)
	if err != nil {
		return err
	}
	n, _ := tag.RowsAffected()
	b.settleSpooled([]string{blob.hash}, map[string]bool{blob.hash: n > 0})
	b.uploaded.Add(n)
	return nil
}

const (
	objectMissing = iota
	objectMatches // holds this blob (same stored bytes, or another encoding of the same content)
	objectDiffers
)

// objectHolds reports whether the object at key is absent, holds blob, or holds something else.
func (b *BlobStore) objectHolds(ctx context.Context, key string, blob spooled) (int, error) {
	st, err := b.r2.StatObject(ctx, b.bucket, key, minio.StatObjectOptions{})
	if err != nil {
		if minio.ToErrorResponse(err).StatusCode == http.StatusNotFound {
			return objectMissing, nil
		}
		return 0, err
	}
	if st.Size == int64(len(blob.data)) {
		return objectMatches, nil
	}
	// Another compression of the same content (an older encoder) is just as good.
	data, err := b.readObject(ctx, key, minio.GetObjectOptions{})
	if err != nil {
		return 0, err
	}
	if raw, errDec := b.dec.DecodeAll(data, nil); errDec == nil && HashOf(raw) == blob.hash {
		return objectMatches, nil
	}
	return objectDiffers, nil
}

// uploadPack concatenates blobs into one object, then records each blob's pack and offset. If the
// index update fails (or a blob's row isn't committed yet) the blobs stay spooled and go into a
// later pack; the orphaned pack bytes are only wasted space.
func (b *BlobStore) uploadPack(group []spooled) error {
	var buf bytes.Buffer
	hashes := make([]string, len(group))
	offsets := make([]int64, len(group))
	for i, blob := range group {
		hashes[i] = blob.hash
		offsets[i] = int64(buf.Len())
		buf.Write(blob.data)
	}
	now := time.Now().UTC()
	key := fmt.Sprintf("%s/packs/%s/%s-%s.pack", strings.TrimSuffix(b.prefix, "/blobs"),
		now.Format("2006/01/02"), now.Format("150405.000000000"), group[0].hash[:12])
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if _, err := b.r2.PutObject(ctx, b.bucket, key, bytes.NewReader(buf.Bytes()), int64(buf.Len()),
		minio.PutObjectOptions{ContentType: "application/octet-stream"}); err != nil {
		return err
	}
	rows, err := b.db.QueryContext(ctx, `UPDATE `+b.schema+`.blobs AS b
		SET uploaded_at = now(), pack_key = $1, pack_offset = u.off
		FROM unnest($2::text[], $3::bigint[]) AS u(hash, off) WHERE b.hash = u.hash RETURNING b.hash`,
		key, pgArray(hashes), pgInt64Array(offsets))
	if err != nil {
		return err
	}
	indexed := make(map[string]bool, len(hashes))
	for rows.Next() {
		var h string
		if err = rows.Scan(&h); err != nil {
			_ = rows.Close()
			return err
		}
		indexed[h] = true
	}
	if err = closeRows(rows); err != nil {
		return err
	}
	b.settleSpooled(hashes, indexed)
	b.uploaded.Add(int64(len(indexed)))
	b.packs.Add(1)
	return nil
}

// pgArray / pgInt64Array render Postgres array literals (lib-agnostic; hashes are hex only).
func pgArray(items []string) string {
	return "{" + strings.Join(items, ",") + "}"
}

func pgInt64Array(items []int64) string {
	parts := make([]string, len(items))
	for i, v := range items {
		parts[i] = strconv.FormatInt(v, 10)
	}
	return "{" + strings.Join(parts, ",") + "}"
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

func (s *lruSet) Remove(k string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if el, ok := s.items[k]; ok {
		s.order.Remove(el)
		delete(s.items, k)
	}
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
