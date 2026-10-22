package archive

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// A lone blob whose object already exists (an earlier attempt's PUT landed, its index update
// didn't) is never written again: same bytes or another encoding of the same content count as
// uploaded; an object holding other bytes is left alone and the blob goes into a pack. Locked
// prefixes refuse overwrites, so a second PUT would fail forever. Setup: blobstore_pack_test.go.
func TestLoneBlobUploadIsWriteOnce(t *testing.T) {
	endpoint := os.Getenv("TMX_ARCHIVE_TEST_S3")
	dsn := os.Getenv("TMX_REQUESTLOG_TEST_DSN")
	if endpoint == "" || dsn == "" {
		t.Skip("TMX_ARCHIVE_TEST_S3 / TMX_REQUESTLOG_TEST_DSN not set")
	}
	ctx := context.Background()
	s3, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4("minioadmin", "minioadmin", "")})
	if err != nil {
		t.Fatal(err)
	}
	bucket := "tmx-once-test"
	if exists, _ := s3.BucketExists(ctx, bucket); !exists {
		if err = s3.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	a, err := Open(ctx, dsn, s3, bucket, "test", t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, err = a.db.Exec("DELETE FROM " + schema + ".blobs"); err != nil {
		t.Fatal(err)
	}
	lone := func() ([]byte, string) { // incompressible, so it gets its own object
		data := make([]byte, packLoneBytes+1024)
		_, _ = rand.Read(data)
		h, errPut := a.blobs.Put(ctx, data, "application/octet-stream", "test")
		if errPut != nil {
			t.Fatal(errPut)
		}
		return data, h
	}
	put := func(key string, body []byte) {
		if _, errPut := s3.PutObject(ctx, bucket, key, bytes.NewReader(body), int64(len(body)), minio.PutObjectOptions{}); errPut != nil {
			t.Fatal(errPut)
		}
	}
	spooled := func(h string) []byte {
		b, errRead := os.ReadFile(a.blobs.spoolPath(h))
		if errRead != nil {
			t.Fatal(errRead)
		}
		return b
	}
	etag := func(key string) string {
		st, errStat := s3.StatObject(ctx, bucket, key, minio.StatObjectOptions{})
		if errStat != nil {
			t.Fatal(errStat)
		}
		return st.ETag
	}

	// 1. The same stored bytes are already there.
	d1, h1 := lone()
	put(a.blobs.objectKey(h1), spooled(h1))
	// 2. Another encoding of the same content (different size) is there.
	d2, h2 := lone()
	enc, _ := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false))
	other := enc.EncodeAll(d2, nil)
	if len(other) == len(spooled(h2)) {
		other = append(other, enc.EncodeAll(nil, nil)...) // an empty frame: same content, other size
	}
	put(a.blobs.objectKey(h2), other)
	// 3. Something else sits at the key.
	d3, h3 := lone()
	put(a.blobs.objectKey(h3), []byte("not this blob"))
	before := map[string]string{h1: etag(a.blobs.objectKey(h1)), h2: etag(a.blobs.objectKey(h2)), h3: etag(a.blobs.objectKey(h3))}

	a.blobs.uploadPending()
	if n := a.blobs.SpoolBacklog(); n != 0 || a.blobs.failures.Load() != 0 {
		t.Fatalf("backlog %d, failures %d after upload", n, a.blobs.failures.Load())
	}
	for h, tag := range before {
		if got := etag(a.blobs.objectKey(h)); got != tag {
			t.Fatalf("object of %s was rewritten", h[:12])
		}
	}
	var pack3 string
	var pending int
	if err = a.db.QueryRow(`SELECT coalesce((SELECT pack_key FROM `+schema+`.blobs WHERE hash = $1), ''),
		(SELECT count(*) FROM `+schema+`.blobs WHERE uploaded_at IS NULL)`, h3).Scan(&pack3, &pending); err != nil {
		t.Fatal(err)
	}
	if pack3 == "" || pending != 0 || a.blobs.mismatched.Load() != 1 {
		t.Fatalf("pack of mismatched blob = %q, not uploaded = %d, mismatches = %d", pack3, pending, a.blobs.mismatched.Load())
	}
	a.blobs.known = newLRUSet(10)
	_ = os.RemoveAll(a.blobs.cache.dir)
	for h, want := range map[string][]byte{h1: d1, h2: d2, h3: d3} {
		got, errGet := a.blobs.Get(ctx, h)
		if errGet != nil || !bytes.Equal(got, want) {
			t.Fatalf("blob %s read back wrong (%v)", h[:12], errGet)
		}
	}
}
