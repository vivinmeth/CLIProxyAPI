package archive

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

// Runs when TMX_ARCHIVE_TEST_S3 (host:port of a throwaway S3 with keys minioadmin/minioadmin) and
// TMX_REQUESTLOG_TEST_DSN are set. MinIO images aren't pullable on alien; SeaweedFS works:
//
//	echo '{"identities":[{"name":"t","credentials":[{"accessKey":"minioadmin","secretKey":"minioadmin"}],
//	  "actions":["Admin","Read","Write","List"]}]}' > /tmp/s3.json
//	docker run -d --rm -v /tmp/s3.json:/s3.json:ro -p 127.0.0.1:58333:8333 chrislusf/seaweedfs \
//	  server -s3 -s3.config=/s3.json
func TestPackUploadAndRangedRead(t *testing.T) {
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
	bucket := "tmx-pack-test"
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

	want := map[string][]byte{}
	for i := 0; i < 40; i++ {
		data := []byte(fmt.Sprintf("blob %d %s", i, strings.Repeat("x", i*37)))
		h, errPut := a.blobs.Put(ctx, data, "text/plain", "test")
		if errPut != nil {
			t.Fatal(errPut)
		}
		want[h] = data
	}
	big := bytes.Repeat([]byte{1, 2, 3, 4, 5, 6, 7, 8}, packLoneBytes) // compresses small: packed
	hBig, err := a.blobs.Put(ctx, big, "application/octet-stream", "test")
	if err != nil {
		t.Fatal(err)
	}
	want[hBig] = big

	a.blobs.uploadPending()
	if n := a.blobs.SpoolBacklog(); n != 0 {
		t.Fatalf("spool backlog after upload = %d", n)
	}
	var packs, unpacked int
	if err = a.db.QueryRow("SELECT count(DISTINCT pack_key), count(*) FILTER (WHERE pack_key IS NULL) FROM "+
		schema+".blobs").Scan(&packs, &unpacked); err != nil {
		t.Fatal(err)
	}
	if packs != 1 || unpacked != 0 {
		t.Fatalf("packs = %d, unpacked = %d; want 1 pack holding every blob", packs, unpacked)
	}
	objects := 0
	for range s3.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: "test/", Recursive: true}) {
		objects++
	}
	if objects != 1 {
		t.Fatalf("R2 objects = %d, want 1", objects)
	}
	a.blobs.known = newLRUSet(10) // force reads to go through the index, not memory
	for h, data := range want {
		got, errGet := a.blobs.Get(ctx, h)
		if errGet != nil {
			t.Fatal(errGet)
		}
		if !bytes.Equal(got, data) {
			t.Fatalf("blob %s: ranged read mismatch", h[:12])
		}
	}
	cost, err := a.Cost(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if cost.Writes30d != 1 {
		t.Fatalf("cost writes_30d = %d, want 1", cost.Writes30d)
	}
}
