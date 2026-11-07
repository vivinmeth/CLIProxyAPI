package quota

import (
	"context"
	"net/http"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

// imageHeaders are the quota headers of a real image response (prod archive, 2026-11-07 05:38).
func imageHeaders() http.Header {
	h := http.Header{}
	for k, v := range map[string]string{
		"X-Codex-Active-Limit":                         "imagegen_premium",
		"X-Codex-Plan-Type":                            "prolite",
		"X-Codex-Credits-Has-Credits":                  "False",
		"X-Codex-Primary-Over-Secondary-Limit-Percent": "0",
		"X-Codex-Primary-Reset-After-Seconds":          "84669",
		"X-Codex-Primary-Reset-At":                     "1794114579",
		"X-Codex-Primary-Used-Percent":                 "1",
		"X-Codex-Primary-Window-Minutes":               "1440",
		"X-Codex-Secondary-Reset-After-Seconds":        "0",
		"X-Codex-Secondary-Reset-At":                   "",
		"X-Codex-Secondary-Used-Percent":               "0",
		"X-Codex-Secondary-Window-Minutes":             "0",
	} {
		h.Set(k, v)
	}
	return h
}

func TestParseCodexImageLimit(t *testing.T) {
	at := time.Date(2026, 11, 7, 5, 38, 41, 0, time.UTC)
	windows, plan := ParseCodexImageLimit(imageHeaders(), at)
	if plan != "prolite" || len(windows) != 1 {
		t.Fatalf("plan %q windows %+v", plan, windows)
	}
	w := windows[0]
	if w.Window != "image_1d" || w.UsedPct == nil || *w.UsedPct != 1 || w.ResetsAt == nil ||
		!w.ResetsAt.Equal(time.Date(2026, 11, 8, 5, 9, 39, 0, time.UTC)) {
		t.Fatalf("window = %+v", w)
	}

	coding := imageHeaders()
	coding.Del("X-Codex-Active-Limit")
	if ws, _ := ParseCodexImageLimit(coding, at); ws != nil {
		t.Fatalf("a coding response is not an image limit: %+v", ws)
	}
	other := imageHeaders()
	other.Set("X-Codex-Active-Limit", "imagegen_standard")
	if ws, _ := ParseCodexImageLimit(other, at); len(ws) != 1 || ws[0].Window != "image_standard_1d" {
		t.Fatalf("other image limit = %+v", ws)
	}
	full := imageHeaders()
	full.Set("X-Codex-Primary-Used-Percent", "100")
	full.Set("X-Codex-Limit-Reached", "true")
	if ws, _ := ParseCodexImageLimit(full, at); len(ws) != 1 || ws[0].Status != "rejected" {
		t.Fatalf("spent image limit = %+v", ws)
	}
}

type routingSpy struct {
	windows []coreauth.QuotaWindowObservation
}

func (r *routingSpy) ApplyQuotaObservation(_, _ string, ws []coreauth.QuotaWindowObservation) bool {
	r.windows = append(r.windows, ws...)
	return true
}

func TestImageWindowsNeverFeedRouting(t *testing.T) {
	spy := &routingSpy{}
	s := &Service{observer: spy}
	reset := time.Now().Add(time.Hour)
	used := 3.0
	s.applyRouting("codex-a.json", "codex", []Window{{Window: "image_1d", UsedPct: &used, ResetsAt: &reset},
		{Window: "7d", UsedPct: &used, ResetsAt: &reset}}, time.Now())
	if len(spy.windows) != 1 || spy.windows[0].Window != "7d" {
		t.Fatalf("routing got %+v", spy.windows)
	}
}

func TestImagePollNotForcedByImageWindow(t *testing.T) {
	now := time.Date(2026, 11, 9, 0, 0, 0, 0, time.UTC)
	past := now.Add(-time.Hour)
	last := now.Add(-15 * time.Minute)
	s := &Service{pollEvery: 10 * time.Minute, slowEvery: 60 * time.Minute}
	a := &account{Account: Account{AuthID: "codex-a.json", LastPollAt: &last}, current: map[string]Current{
		"image_1d": {Window: Window{Window: "image_1d", ResetsAt: &past}},
	}}
	// Fresh coding headers cover 7d; the expired image window must not force a poll.
	if s.pollDueLocked(a, []Window{{Window: "7d"}}, true, now) {
		t.Fatal("an expired image window forced a poll")
	}
	a.current["5h"] = Current{Window: Window{Window: "5h", ResetsAt: &past}}
	if !s.pollDueLocked(a, []Window{{Window: "7d"}}, true, now) {
		t.Fatal("an expired coding window must still force a poll")
	}
}

func TestQuotaRecordsImageLimitFromUsage(t *testing.T) {
	clock := time.Date(2026, 11, 7, 5, 39, 0, 0, time.UTC)
	codex := &coreauth.Auth{ID: "codex-img.json", Provider: "codex", Label: "img@example.com"}
	f := &fakeFetcher{auths: []*coreauth.Auth{codex}, status: 200, calls: map[string]int{},
		codexUsage: `{"plan_type":"prolite","rate_limit":{"primary_window":{"used_percent":25,"limit_window_seconds":604800,"reset_at":1794499173}}}`}
	s := openTestService(t, f, &clock)
	if _, err := s.db.Exec(`DELETE FROM ` + s.table); err != nil {
		t.Fatal(err)
	}
	s.accounts = map[string]*account{}
	ctx := context.Background()

	rec := coreusage.Record{Provider: "codex", AuthID: codex.ID, RequestedAt: clock.Add(-30 * time.Second),
		Latency: 11 * time.Second, ResponseHeaders: imageHeaders()}
	s.observeImageLimit(rec)
	s.observeImageLimit(rec) // the same reading again: no second row
	coding := coreusage.Record{Provider: "codex", AuthID: codex.ID, RequestedAt: clock, ResponseHeaders: http.Header{
		"X-Codex-Primary-Used-Percent": {"30"}, "X-Codex-Primary-Window-Minutes": {"10080"}}}
	s.observeImageLimit(coding) // not an image response
	s.tick(ctx)

	var window, source, plan, label string
	var used float64
	var resets, ts time.Time
	if err := s.db.QueryRow(`SELECT "window", source, plan, label, used_pct, resets_at, ts FROM `+s.table+
		` WHERE "window" LIKE 'image_%'`).Scan(&window, &source, &plan, &label, &used, &resets, &ts); err != nil {
		t.Fatal(err)
	}
	if window != "image_1d" || source != "passive" || plan != "prolite" || label != "img@example.com" || used != 1 ||
		!resets.Equal(time.Date(2026, 11, 8, 5, 9, 39, 0, time.UTC)) || !ts.Equal(clock.Add(-19*time.Second)) {
		t.Fatalf("row = %s %s %s %s %v %s %s", window, source, plan, label, used, resets, ts)
	}
	var images, weekly int
	if err := s.db.QueryRow(`SELECT count(*) FILTER (WHERE "window" LIKE 'image_%'), count(*) FILTER (WHERE "window" = '7d') FROM `+
		s.table).Scan(&images, &weekly); err != nil {
		t.Fatal(err)
	}
	if images != 1 || weekly != 1 {
		t.Fatalf("image rows %d, weekly rows %d (want 1, 1)", images, weekly)
	}
	acc := s.Accounts()
	if len(acc) != 1 || len(acc[0].Windows) != 2 {
		t.Fatalf("accounts = %+v", acc)
	}
}
