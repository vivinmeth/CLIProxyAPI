package managementasset

// tmx: our panel comes from vivinmeth/Cli-Proxy-API-Management-Center releases. Upstream's
// updater falls back to its own hosted panel (cpamc.router-for.me) when the release download
// fails and no local copy exists, which after a restart silently replaced our UI with upstream's
// (2026-11-07). These helpers keep the fallback for upstream's repo only and give the asset
// download a longer timeout and a few retries.

import (
	"context"
	"net/http"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	tmxAssetDownloadTimeout  = 2 * time.Minute
	tmxAssetDownloadAttempts = 3
)

// tmxAssetRetryDelay is a variable so tests can shorten it.
var tmxAssetRetryDelay = 5 * time.Second

// tmxUpstreamFallbackAllowed reports whether the panel may fall back to upstream's hosted page:
// only when the configured repository resolves to upstream's own releases.
func tmxUpstreamFallbackAllowed(panelRepository string) bool {
	return resolveReleaseURL(panelRepository) == defaultManagementReleaseURL
}

// tmxDownloadAsset downloads the release asset with a longer timeout than the release API call,
// retrying a few times; the panel is several MB and GitHub's CDN is sometimes slow.
func tmxDownloadAsset(ctx context.Context, client *http.Client, downloadURL string) ([]byte, string, error) {
	slow := *client
	if slow.Timeout < tmxAssetDownloadTimeout {
		slow.Timeout = tmxAssetDownloadTimeout
	}
	var lastErr error
	for attempt := 1; attempt <= tmxAssetDownloadAttempts; attempt++ {
		data, hash, err := downloadAsset(ctx, &slow, downloadURL)
		if err == nil {
			return data, hash, nil
		}
		lastErr = err
		if attempt == tmxAssetDownloadAttempts {
			break
		}
		log.WithError(err).Warnf("management asset download attempt %d/%d failed, retrying", attempt, tmxAssetDownloadAttempts)
		select {
		case <-ctx.Done():
			return nil, "", lastErr
		case <-time.After(tmxAssetRetryDelay):
		}
	}
	return nil, "", lastErr
}
