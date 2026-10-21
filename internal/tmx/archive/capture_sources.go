package archive

import (
	"context"
	"encoding/json"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
)

func exchangeMemory(ex *capture.Exchange) int64 {
	size := int64(len(ex.RequestBody) + len(ex.ResponseBody) + len(ex.APIRequest) + len(ex.APIResponse) + len(ex.WebsocketTimeline) + len(ex.APIWebsocketTimeline))
	for _, body := range ex.Bodies {
		size += body.MemorySize()
	}
	return size
}

func (a *Archive) putCapture(ctx context.Context, ex *capture.Exchange, name string, fallback []byte, mediaType, kind string, split bool) (string, error) {
	body := ex.Bodies[name]
	if body == nil {
		if len(fallback) == 0 {
			return "", nil
		}
		if split {
			return a.splitBody(ctx, fallback, kind)
		}
		return a.blobs.Put(ctx, fallback, mediaType, kind)
	}
	if body.Size() == 0 {
		return "", nil
	}
	if split {
		if data, release, ok := body.MemoryBytes(); ok {
			defer release()
			return a.splitBody(ctx, data, kind)
		}
	}
	reader, err := body.Reader()
	if err != nil {
		return "", err
	}
	hash, err := a.blobs.PutReader(ctx, reader, mediaType, kind)
	if err != nil || !split {
		return hash, err
	}
	// Preserve the existing manifest contract while avoiding full JSON parsing of
	// spilled bodies. Reading rebuilds exactly the captured bytes from this raw blob.
	raw, _ := json.Marshal(Manifest{V: manifestVersion, Format: "raw", Raw: hash})
	return a.blobs.Put(ctx, raw, "application/json", kind+".manifest")
}

func capturePrefix(ex *capture.Exchange, name string, fallback []byte, limit int64) []byte {
	if body := ex.Bodies[name]; body != nil {
		return body.Prefix(limit)
	}
	if int64(len(fallback)) > limit {
		return fallback[:limit]
	}
	return fallback
}
