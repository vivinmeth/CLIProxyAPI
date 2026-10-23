package archive

import (
	"bytes"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestBlobCacheGetPutEvict(t *testing.T) {
	t.Setenv("TMX_ARCHIVE_CACHE_MB", "1")
	spool := t.TempDir()
	c := newBlobCache(spool)
	if c == nil {
		t.Fatal("cache disabled")
	}
	if _, ok := c.get("a"); ok {
		t.Fatal("hit on empty cache")
	}
	data := bytes.Repeat([]byte{1}, 200<<10) // 200 KiB each, cap 1 MiB
	for i, h := range []string{"h1", "h2", "h3", "h4", "h5", "h6"} {
		c.put(h, data)
		old := time.Now().Add(time.Duration(i-10) * time.Minute)
		_ = os.Chtimes(c.path(h), old, old)
	}
	if got, ok := c.get("h6"); !ok || !bytes.Equal(got, data) {
		t.Fatal("miss after put")
	}
	c.sweep()
	var total int64
	entries, _ := os.ReadDir(c.dir)
	for _, e := range entries {
		info, _ := e.Info()
		total += info.Size()
	}
	if total > c.maxBytes {
		t.Fatalf("cache %d bytes over cap %d", total, c.maxBytes)
	}
	if _, ok := c.get("h1"); ok {
		t.Fatal("oldest entry survived eviction")
	}
	if _, ok := c.get("h6"); !ok {
		t.Fatal("most recent entry evicted")
	}
	// The cache lives under the spool but never looks like a spooled blob.
	if filepath.Dir(c.dir) != spool {
		t.Fatalf("cache dir %s", c.dir)
	}
	t.Setenv("TMX_ARCHIVE_CACHE_MB", "0")
	if newBlobCache(t.TempDir()) != nil {
		t.Fatal("TMX_ARCHIVE_CACHE_MB=0 should disable the cache")
	}
}

// Small blobs count at the disk blocks they take: 1000 files of 10 bytes fill a 1 MiB cache
// (~4 MB of disk), not 10 KB of it.
func TestBlobCacheCountsBlocks(t *testing.T) {
	t.Setenv("TMX_ARCHIVE_CACHE_MB", "1")
	c := newBlobCache(t.TempDir())
	for i := 0; i < 1000; i++ {
		h := "s" + strconv.Itoa(i)
		c.put(h, []byte("0123456789"))
		old := time.Now().Add(time.Duration(i-2000) * time.Second)
		_ = os.Chtimes(c.path(h), old, old)
	}
	c.sweep()
	entries, _ := os.ReadDir(c.dir)
	if n, limit := len(entries), int(c.maxBytes/cacheBlock); n > limit {
		t.Fatalf("%d files kept, %d blocks fit the cap", n, limit)
	}
	if _, ok := c.get("s999"); !ok {
		t.Fatal("most recent entry evicted")
	}
}
