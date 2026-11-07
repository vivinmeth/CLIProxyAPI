package managementasset

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestTmxUpstreamFallbackAllowed(t *testing.T) {
	cases := map[string]bool{
		"": true,
		"https://github.com/router-for-me/Cli-Proxy-API-Management-Center":                           true,
		"https://api.github.com/repos/router-for-me/Cli-Proxy-API-Management-Center/releases/latest": true,
		"https://github.com/vivinmeth/Cli-Proxy-API-Management-Center":                               false,
		"https://github.com/vivinmeth/Cli-Proxy-API-Management-Center.git":                           false,
		"https://api.github.com/repos/vivinmeth/Cli-Proxy-API-Management-Center":                     false,
	}
	for repo, want := range cases {
		if got := tmxUpstreamFallbackAllowed(repo); got != want {
			t.Errorf("tmxUpstreamFallbackAllowed(%q) = %v, want %v", repo, got, want)
		}
	}
}

func TestTmxDownloadAssetRetriesThenSucceeds(t *testing.T) {
	prev := tmxAssetRetryDelay
	tmxAssetRetryDelay = time.Millisecond
	t.Cleanup(func() { tmxAssetRetryDelay = prev })

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) < tmxAssetDownloadAttempts {
			http.Error(w, "slow cdn", http.StatusBadGateway)
			return
		}
		_, _ = w.Write([]byte("<html>tmx</html>"))
	}))
	defer srv.Close()

	data, hash, err := tmxDownloadAsset(context.Background(), &http.Client{Timeout: time.Second}, srv.URL)
	if err != nil {
		t.Fatalf("tmxDownloadAsset: %v", err)
	}
	if string(data) != "<html>tmx</html>" || hash == "" {
		t.Fatalf("unexpected result data=%q hash=%q", data, hash)
	}
	if got := calls.Load(); got != tmxAssetDownloadAttempts {
		t.Fatalf("calls = %d, want %d", got, tmxAssetDownloadAttempts)
	}
}

func TestTmxDownloadAssetGivesUpAfterAttempts(t *testing.T) {
	prev := tmxAssetRetryDelay
	tmxAssetRetryDelay = time.Millisecond
	t.Cleanup(func() { tmxAssetRetryDelay = prev })

	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, "down", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	if _, _, err := tmxDownloadAsset(context.Background(), &http.Client{Timeout: time.Second}, srv.URL); err == nil {
		t.Fatal("expected an error")
	}
	if got := calls.Load(); got != tmxAssetDownloadAttempts {
		t.Fatalf("calls = %d, want %d", got, tmxAssetDownloadAttempts)
	}
}
