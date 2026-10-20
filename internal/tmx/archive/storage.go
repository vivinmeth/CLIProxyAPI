package archive

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
)

const (
	storageCacheTTL    = time.Hour
	storageRefreshGap  = time.Minute
	storageListTimeout = 2 * time.Minute
)

// EnvStorage is what one environment folder ("prod/", "staging/", "dev/") holds in the bucket.
type EnvStorage struct {
	Objects    int64 `json:"objects"`
	Bytes      int64 `json:"bytes"`
	Packs      int64 `json:"packs"`
	PackBytes  int64 `json:"pack_bytes"`
	Loose      int64 `json:"loose"` // one object per blob (pre-pack, or >= 8 MiB)
	LooseBytes int64 `json:"loose_bytes"`
}

// StorageUsage is a listing of the whole archive bucket (all environments share it).
type StorageUsage struct {
	Bucket          string                 `json:"bucket"`
	ListedAt        time.Time              `json:"listed_at"`
	Objects         int64                  `json:"objects"`
	Bytes           int64                  `json:"bytes"`
	ByEnv           map[string]*EnvStorage `json:"by_env"`
	StorageUSDMonth float64                `json:"storage_usd_month"` // list price, free tier ignored
	ThisEnv         string                 `json:"this_env"`
}

type storageCache struct {
	mu   sync.Mutex
	last *StorageUsage
}

// ErrNoR2 is returned when the archive has no R2 client (dev).
var ErrNoR2 = errors.New("R2 is not configured for this environment")

// Storage lists the bucket (cached for an hour; refresh forces a new listing at most once a
// minute). With packs the bucket holds few objects, so a listing is a handful of LIST calls.
func (a *Archive) Storage(ctx context.Context, refresh bool) (*StorageUsage, error) {
	if a.blobs.r2 == nil {
		return nil, ErrNoR2
	}
	a.storage.mu.Lock()
	defer a.storage.mu.Unlock()
	if last := a.storage.last; last != nil {
		age := time.Since(last.ListedAt)
		if age < storageCacheTTL && (!refresh || age < storageRefreshGap) {
			return last, nil
		}
	}
	ctx, cancel := context.WithTimeout(ctx, storageListTimeout)
	defer cancel()
	u := &StorageUsage{Bucket: a.blobs.bucket, ByEnv: map[string]*EnvStorage{}, ThisEnv: a.env}
	for obj := range a.blobs.r2.ListObjects(ctx, a.blobs.bucket, minio.ListObjectsOptions{Recursive: true}) {
		if obj.Err != nil {
			return nil, obj.Err
		}
		env, rest, _ := strings.Cut(obj.Key, "/")
		e := u.ByEnv[env]
		if e == nil {
			e = &EnvStorage{}
			u.ByEnv[env] = e
		}
		e.Objects++
		e.Bytes += obj.Size
		switch {
		case strings.HasPrefix(rest, "packs/"):
			e.Packs++
			e.PackBytes += obj.Size
		case strings.HasPrefix(rest, "blobs/"):
			e.Loose++
			e.LooseBytes += obj.Size
		}
		u.Objects++
		u.Bytes += obj.Size
	}
	u.ListedAt = time.Now().UTC()
	u.StorageUSDMonth = round(float64(u.Bytes) / 1e9 * r2StoragePerGBMonth)
	a.storage.last = u
	return u, nil
}
