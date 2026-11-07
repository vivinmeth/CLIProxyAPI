package auth

import (
	"net/http"
	"testing"
	"time"
)

// Headers as ChatGPT's Codex backend sent them on 2026-11-07 (prolite): a coding response carries
// the weekly window in the primary slot, an image response its daily image limit in the same slot.
func codingHeaders() http.Header {
	return http.Header{
		"X-Codex-Plan-Type":                   {"prolite"},
		"X-Codex-Primary-Used-Percent":        {"24"},
		"X-Codex-Primary-Window-Minutes":      {"10080"},
		"X-Codex-Primary-Reset-At":            {"1794499173"},
		"X-Codex-Primary-Reset-After-Seconds": {"467629"},
	}
}

func imageHeaders(used string) http.Header {
	return http.Header{
		"X-Codex-Active-Limit":                         {"imagegen_premium"},
		"X-Codex-Credits-Has-Credits":                  {"False"},
		"X-Codex-Credits-Unlimited":                    {"False"},
		"X-Codex-Plan-Type":                            {"prolite"},
		"X-Codex-Primary-Over-Secondary-Limit-Percent": {"0"},
		"X-Codex-Primary-Reset-After-Seconds":          {"82896"},
		"X-Codex-Primary-Reset-At":                     {"1794114579"},
		"X-Codex-Primary-Used-Percent":                 {used},
		"X-Codex-Primary-Window-Minutes":               {"1440"},
		"X-Codex-Secondary-Reset-After-Seconds":        {"0"},
		"X-Codex-Secondary-Used-Percent":               {"0"},
		"X-Codex-Secondary-Window-Minutes":             {"0"},
	}
}

func TestTmxImageResponseKeepsCodingWindows(t *testing.T) {
	now := time.Now()
	var q QuotaState
	if !q.ObserveResponseHeadersForProvider("codex", codingHeaders(), now) {
		t.Fatal("coding headers not observed")
	}
	q.ObserveResponseHeadersForProvider("codex", imageHeaders("100"), now.Add(time.Second))

	for name, want := range map[string]string{
		"X-Codex-Primary-Window-Minutes": "10080",
		"X-Codex-Primary-Used-Percent":   "24",
		"X-Codex-Primary-Reset-At":       "1794499173",
	} {
		if got := q.Signals[name]; got != want {
			t.Errorf("%s = %q after an image response, want the coding value %q", name, got, want)
		}
	}
	for _, name := range []string{"X-Codex-Active-Limit", "X-Codex-Secondary-Window-Minutes", "X-Codex-Secondary-Used-Percent"} {
		if v, ok := q.Signals[name]; ok {
			t.Errorf("image-limit field %s = %q kept at credential level", name, v)
		}
	}
	if q.Signals["X-Codex-Credits-Has-Credits"] != "False" {
		t.Errorf("account-wide credits field dropped: %v", q.Signals)
	}

	a := &Auth{Provider: "codex", Quota: q}
	if !codexWeeklyWindow(a.Quota.Signals, "Primary") {
		t.Fatal("weekly window lost for reset-soonest ranking")
	}
	if overQuotaThreshold(a, now.Add(2*time.Second), "gpt-6.1-sol", tmxAdmissionView{five: 0.9, seven: 0.9}) {
		t.Fatal("a full image limit put the account over its coding threshold")
	}
}

func TestTmxImageOnlyAccountRecordsNoWindow(t *testing.T) {
	var q QuotaState
	q.ObserveResponseHeadersForProvider("codex", imageHeaders("1"), time.Now())
	if _, ok := q.Signals["X-Codex-Primary-Window-Minutes"]; ok {
		t.Fatalf("image limit recorded as the account's primary window: %v", q.Signals)
	}
	if q.Signals["X-Codex-Plan-Type"] != "prolite" {
		t.Fatalf("plan type dropped: %v", q.Signals)
	}
}

func TestTmxOtherActiveLimitsUntouched(t *testing.T) {
	h := codingHeaders()
	h.Set("X-Codex-Active-Limit", "codex")
	var q QuotaState
	q.ObserveResponseHeadersForProvider("codex", h, time.Now())
	if q.Signals["X-Codex-Primary-Window-Minutes"] != "10080" || q.Signals["X-Codex-Active-Limit"] != "codex" {
		t.Fatalf("non-image active limit changed: %v", q.Signals)
	}
}
