package quota

import (
	"context"
	"net/http"
	"strings"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

// Codex image limits (tokenmaxx fork). ChatGPT's image backend answers with
// X-Codex-Active-Limit: imagegen_premium and the image limit (a daily window on prolite,
// 2026-11) in the X-Codex-Primary-* slot coding responses use for their own windows. The auth
// manager drops those window fields so they never reach routing
// (sdk/cliproxy/auth/tmx_quota_imagegen.go); this file records them instead, as their own
// windows ("image_1d"), from the response headers every usage record carries. wham/usage does
// not list the image limit (checked 2026-11-07: additional_rate_limits null), so these passive
// readings are the only source.

// imagePendingMax bounds readings waiting for the next tick (one per image response).
const imagePendingMax = 256

type imageReading struct {
	authID  string
	plan    string
	windows []Window
	at      time.Time
}

// ImageWindowPrefix starts every image-limit window name.
const ImageWindowPrefix = "image_"

// IsImageWindow reports whether a window is an image limit (never a coding window).
func IsImageWindow(window string) bool { return strings.HasPrefix(window, ImageWindowPrefix) }

// imageWindowName names an image limit's window: "image_<length>" for imagegen_premium (the only
// limit seen), "image_<rest>_<length>" for any other imagegen limit.
func imageWindowName(limit, length string) string {
	rest := strings.Trim(strings.TrimPrefix(strings.ToLower(strings.TrimSpace(limit)), "imagegen"), "_")
	rest = strings.Trim(nonSlug.ReplaceAllString(rest, "_"), "_")
	if rest == "" || rest == "premium" {
		return ImageWindowPrefix + length
	}
	return ImageWindowPrefix + rest + "_" + length
}

// ParseCodexImageLimit reads the image-limit windows from a Codex response's headers; nil
// unless the response carries an imagegen active limit.
func ParseCodexImageLimit(headers http.Header, observedAt time.Time) ([]Window, string) {
	limit := strings.TrimSpace(headers.Get("X-Codex-Active-Limit"))
	if !strings.HasPrefix(strings.ToLower(limit), "imagegen") {
		return nil, ""
	}
	signals := make(map[string]string, len(headers))
	for key, values := range headers {
		if len(values) > 0 && strings.HasPrefix(strings.ToLower(key), "x-codex-") {
			signals[http.CanonicalHeaderKey(key)] = values[len(values)-1]
		}
	}
	windows := ParseCodexSignals(signals, observedAt)
	for i := range windows {
		windows[i].Window = imageWindowName(limit, windows[i].Window)
	}
	return windows, strings.ToLower(strings.TrimSpace(headers.Get("X-Codex-Plan-Type")))
}

// imageLimitPlugin feeds image-limit readings from usage records to the running service.
type imageLimitPlugin struct{}

func init() { coreusage.RegisterPlugin(imageLimitPlugin{}) }

func (imageLimitPlugin) HandleUsage(_ context.Context, record coreusage.Record) {
	if s := Default(); s != nil {
		s.observeImageLimit(record)
	}
}

// observeImageLimit queues the record's image-limit reading for the next tick (no I/O here: the
// usage worker is shared with the request log).
func (s *Service) observeImageLimit(record coreusage.Record) {
	if !strings.EqualFold(strings.TrimSpace(record.Provider), "codex") || record.AuthID == "" || len(record.ResponseHeaders) == 0 {
		return
	}
	at := record.RequestedAt.Add(record.Latency)
	if record.RequestedAt.IsZero() {
		at = s.now()
	}
	windows, plan := ParseCodexImageLimit(record.ResponseHeaders, at)
	if len(windows) == 0 {
		return
	}
	s.imageMu.Lock()
	defer s.imageMu.Unlock()
	if len(s.imagePending) >= imagePendingMax {
		s.imagePending = s.imagePending[1:]
	}
	s.imagePending = append(s.imagePending, imageReading{authID: record.AuthID, plan: plan, windows: windows, at: at.UTC()})
}

// recordImageLimits writes the queued image-limit readings as passive snapshots.
func (s *Service) recordImageLimits(ctx context.Context) {
	s.imageMu.Lock()
	pending := s.imagePending
	s.imagePending = nil
	s.imageMu.Unlock()
	for _, r := range pending {
		s.mu.Lock()
		a := s.accountLocked(r.authID)
		if a.Provider == "" {
			a.Provider = "codex"
		}
		if a.Plan == "" {
			a.Plan = r.plan
		}
		s.mu.Unlock()
		s.record(ctx, r.authID, r.windows, "passive", r.at)
	}
}
