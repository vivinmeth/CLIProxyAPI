package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"sort"
	"sync"

	"github.com/minio/minio-go/v7"
)

const (
	prefetchWorkers  = 8
	prefetchMaxGap   = 1 << 20  // merge blobs of one pack into one read across gaps up to this
	prefetchMaxSpan  = 32 << 20 // never read more than this in one range request
	prefetchMaxBatch = 20000
)

// Prefetch pulls blobs that are only in R2 into the local read cache with as few requests as
// possible: blobs from the same pack are read with one ranged GET per contiguous span, and spans
// are fetched in parallel. Afterwards Get serves them from disk. Missing or local blobs are skipped;
// errors are ignored (Get falls back to a direct read).
func (b *BlobStore) Prefetch(ctx context.Context, hashes []string) {
	if b.r2 == nil || b.cache == nil || len(hashes) == 0 {
		return
	}
	need := make([]string, 0, len(hashes))
	seen := map[string]bool{}
	for _, h := range hashes {
		if seen[h] || !ValidHash(h) {
			continue
		}
		seen[h] = true
		if _, err := os.Stat(b.spoolPath(h)); err == nil {
			continue
		}
		if _, err := os.Stat(b.cache.path(h)); err == nil {
			continue
		}
		need = append(need, h)
		if len(need) >= prefetchMaxBatch {
			break
		}
	}
	if len(need) == 0 {
		return
	}
	rows, err := b.db.QueryContext(ctx, `SELECT hash, pack_key, pack_offset, stored_size FROM `+b.schema+
		`.blobs WHERE hash = ANY($1::text[]) AND uploaded_at IS NOT NULL`, pgArray(need))
	if err != nil {
		return
	}
	type ref struct {
		hash   string
		offset int64
		size   int64
	}
	packs := map[string][]ref{}
	var loose []string
	for rows.Next() {
		var h string
		var pack sql.NullString
		var off, size sql.NullInt64
		if rows.Scan(&h, &pack, &off, &size) != nil {
			continue
		}
		if pack.Valid && pack.String != "" {
			packs[pack.String] = append(packs[pack.String], ref{h, off.Int64, size.Int64})
		} else {
			loose = append(loose, h)
		}
	}
	_ = rows.Close()

	type job struct {
		key   string
		start int64
		end   int64 // inclusive; -1 for a whole object
		refs  []ref
	}
	var jobs []job
	for key, refs := range packs {
		sort.Slice(refs, func(i, j int) bool { return refs[i].offset < refs[j].offset })
		cur := job{key: key, start: refs[0].offset, end: refs[0].offset + refs[0].size - 1, refs: refs[:1]}
		for _, r := range refs[1:] {
			if r.offset-cur.end-1 <= prefetchMaxGap && r.offset+r.size-cur.start <= prefetchMaxSpan {
				if e := r.offset + r.size - 1; e > cur.end {
					cur.end = e
				}
				cur.refs = append(cur.refs, r)
				continue
			}
			jobs = append(jobs, cur)
			cur = job{key: key, start: r.offset, end: r.offset + r.size - 1, refs: []ref{r}}
		}
		jobs = append(jobs, cur)
	}
	for _, h := range loose {
		jobs = append(jobs, job{key: b.objectKey(h), end: -1, refs: []ref{{hash: h}}})
	}

	work := make(chan job)
	var wg sync.WaitGroup
	for i := 0; i < prefetchWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range work {
				opts := minio.GetObjectOptions{}
				if j.end >= 0 && opts.SetRange(j.start, j.end) != nil {
					continue
				}
				data, errRead := b.readObject(ctx, j.key, opts)
				if errRead != nil && j.end < 0 {
					data, errRead = b.readFallback(ctx, j.refs[0].hash, errRead)
				}
				if errRead != nil {
					continue
				}
				if j.end < 0 {
					b.cache.put(j.refs[0].hash, data)
					continue
				}
				for _, r := range j.refs {
					lo, hi := r.offset-j.start, r.offset-j.start+r.size
					if lo >= 0 && hi <= int64(len(data)) {
						b.cache.put(r.hash, data[lo:hi])
					}
				}
			}
		}()
	}
	for _, j := range jobs {
		select {
		case work <- j:
		case <-ctx.Done():
		}
	}
	close(work)
	wg.Wait()
}

// prefetchBodies warms the cache for everything needed to rebuild the given manifests (two
// rounds: the manifests, then every element, media and part they point at).
func (a *Archive) prefetchBodies(ctx context.Context, manifests []string) {
	for depth := 0; depth < 3 && len(manifests) > 0; depth++ {
		a.blobs.Prefetch(ctx, manifests)
		var leaves, nested []string
		for _, h := range manifests {
			raw, err := a.blobs.Get(ctx, h)
			if err != nil {
				continue
			}
			var m Manifest
			if json.Unmarshal(raw, &m) != nil {
				continue
			}
			if m.Raw != "" {
				leaves = append(leaves, m.Raw)
			}
			for _, k := range m.Keys {
				if k.Blob != "" {
					leaves = append(leaves, k.Blob)
				}
				leaves = append(leaves, k.Items...)
			}
			for _, md := range m.Media {
				leaves = append(leaves, md.Blob)
			}
			for _, p := range m.Parts {
				if p.Text != "" {
					leaves = append(leaves, p.Text)
				}
				if p.Body != "" {
					nested = append(nested, p.Body)
				}
			}
		}
		a.blobs.Prefetch(ctx, leaves)
		manifests = nested
	}
}
