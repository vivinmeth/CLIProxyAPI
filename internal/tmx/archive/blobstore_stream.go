package archive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"

	"github.com/klauspost/compress/zstd"
)

// PutReader hashes and compresses a capture directly into the archive spool. Its
// memory use is fixed by the compressor window, independent of the body size.
func (b *BlobStore) PutReader(ctx context.Context, reader io.Reader, mediaType, kind string) (string, error) {
	tmp, err := os.CreateTemp(b.spool, "stream-*.tmp")
	if err != nil {
		return "", err
	}
	defer func() { _ = tmp.Close(); _ = os.Remove(tmp.Name()) }()
	enc, err := zstd.NewWriter(tmp, zstd.WithEncoderLevel(zstd.SpeedDefault), zstd.WithEncoderConcurrency(1), zstd.WithWindowSize(1<<20))
	if err != nil {
		return "", err
	}
	hasher := sha256.New()
	size, err := io.Copy(enc, io.TeeReader(reader, hasher))
	errClose := enc.Close()
	if err != nil {
		return "", err
	}
	if errClose != nil {
		return "", errClose
	}
	if err = ctx.Err(); err != nil {
		return "", err
	}
	hash := hex.EncodeToString(hasher.Sum(nil))
	lock := b.hashLock(hash)
	lock.Lock()
	defer lock.Unlock()
	if b.known.Has(hash) {
		b.dedup.Add(1)
		return hash, nil
	}
	var exists bool
	err = b.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM `+b.schema+`.blobs WHERE hash=$1)`, hash).Scan(&exists)
	if err != nil {
		return "", err
	}
	if exists {
		b.known.Add(hash)
		b.dedup.Add(1)
		return hash, nil
	}
	info, err := tmp.Stat()
	if err != nil {
		return "", err
	}
	if err = tmp.Close(); err != nil {
		return "", err
	}
	if err = os.Rename(tmp.Name(), b.spoolPath(hash)); err != nil {
		return "", fmt.Errorf("stream spool rename: %w", err)
	}
	if _, err = b.db.ExecContext(ctx, `INSERT INTO `+b.schema+`.blobs (hash,size,stored_size,media_type,kind) VALUES ($1,$2,$3,$4,$5) ON CONFLICT(hash) DO NOTHING`, hash, size, info.Size(), mediaType, kind); err != nil {
		return "", err
	}
	b.known.Add(hash)
	b.stored.Add(1)
	select {
	case b.kick <- struct{}{}:
	default:
	}
	return hash, nil
}
