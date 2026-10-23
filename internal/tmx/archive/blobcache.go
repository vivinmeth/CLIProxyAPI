package archive

import (
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// blobCache keeps compressed blobs read back from R2 on local disk, so viewing the same
// conversation again costs no R2 read operations. Bounded by TMX_ARCHIVE_CACHE_MB (default 1024);
// least recently used files are evicted. It is only a cache: losing it loses nothing.
type blobCache struct {
	dir      string
	maxBytes int64

	mu        sync.Mutex
	lastSweep time.Time
	hits      int64
	misses    int64
}

func newBlobCache(spool string) *blobCache {
	mb := int64(1024)
	if n, err := strconv.ParseInt(strings.TrimSpace(os.Getenv("TMX_ARCHIVE_CACHE_MB")), 10, 64); err == nil && n >= 0 {
		mb = n
	}
	if mb == 0 {
		return nil
	}
	dir := filepath.Join(spool, ".read-cache")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil
	}
	return &blobCache{dir: dir, maxBytes: mb << 20}
}

func (c *blobCache) path(hash string) string { return filepath.Join(c.dir, hash+".zst") }

func (c *blobCache) get(hash string) ([]byte, bool) {
	if c == nil {
		return nil, false
	}
	data, err := os.ReadFile(c.path(hash))
	c.mu.Lock()
	defer c.mu.Unlock()
	if err != nil {
		c.misses++
		return nil, false
	}
	c.hits++
	now := time.Now()
	_ = os.Chtimes(c.path(hash), now, now)
	return data, true
}

func (c *blobCache) put(hash string, compressed []byte) {
	if c == nil || int64(len(compressed)) > c.maxBytes/4 {
		return
	}
	tmp := c.path(hash) + ".tmp"
	if os.WriteFile(tmp, compressed, 0o640) != nil {
		return
	}
	if os.Rename(tmp, c.path(hash)) != nil {
		_ = os.Remove(tmp)
		return
	}
	c.mu.Lock()
	due := time.Since(c.lastSweep) > time.Minute
	if due {
		c.lastSweep = time.Now()
	}
	c.mu.Unlock()
	if due {
		go c.sweep()
	}
}

// cacheBlock: files are counted at the disk space they take (whole 4 KiB blocks); most cached
// blobs are a few hundred bytes, so counting their length let the cache take ~3x its limit.
const cacheBlock = 4096

// sweep evicts least recently used files until the cache is under its size limit.
func (c *blobCache) sweep() {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return
	}
	type f struct {
		name string
		size int64
		at   time.Time
	}
	var files []f
	var total int64
	for _, e := range entries {
		info, errInfo := e.Info()
		if errInfo != nil || !strings.HasSuffix(e.Name(), ".zst") {
			continue
		}
		size := (info.Size() + cacheBlock - 1) / cacheBlock * cacheBlock
		files = append(files, f{e.Name(), size, info.ModTime()})
		total += size
	}
	if total <= c.maxBytes {
		return
	}
	sort.Slice(files, func(i, j int) bool { return files[i].at.Before(files[j].at) })
	for _, file := range files {
		if total <= c.maxBytes*9/10 {
			break
		}
		if os.Remove(filepath.Join(c.dir, file.name)) == nil {
			total -= file.size
		}
	}
}

func (c *blobCache) stats() map[string]any {
	if c == nil {
		return map[string]any{"enabled": false}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return map[string]any{"enabled": true, "hits": c.hits, "misses": c.misses, "max_mb": c.maxBytes >> 20}
}
