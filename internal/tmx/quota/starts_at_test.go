package quota

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParsersStartsAt(t *testing.T) {
	const reset = int64(1793500000)
	at := time.Unix(reset-600, 0).UTC()
	for _, provider := range []string{"codex", "meta"} {
		for _, source := range []string{"usage", "signals"} {
			t.Run(provider+"/"+source, func(t *testing.T) {
				for _, tc := range []struct {
					name, length, reset string
					wantRows            int
					wantLength          time.Duration
				}{
					{"known", "240", strconv.FormatInt(reset, 10), 1, 240 * time.Minute},
					{"unknown length", "", strconv.FormatInt(reset, 10), 0, 0},
					{"zero length", "0", strconv.FormatInt(reset, 10), 0, 0},
					{"negative length", "-1", strconv.FormatInt(reset, 10), 0, 0},
					{"unknown reset", "240", "", 1, 0},
					{"zero reset", "240", "0", 1, 0},
					{"negative reset", "240", "-1", 1, 0},
				} {
					t.Run(tc.name, func(t *testing.T) {
						var ws []Window
						var err error
						lengthField, resetField := "", ""
						if source == "usage" {
							if tc.length != "" {
								mins, _ := strconv.ParseInt(tc.length, 10, 64)
								if provider == "codex" {
									lengthField = fmt.Sprintf(`,"limit_window_seconds":%d`, mins*60)
								} else {
									lengthField = `,"window_duration_mins":` + tc.length
								}
							}
							if tc.reset != "" {
								key := "resets_at"
								if provider == "codex" {
									key = "reset_at"
								}
								resetField = fmt.Sprintf(`,%q:%s`, key, tc.reset)
							}
							if provider == "codex" {
								ws, _, err = ParseCodexUsage([]byte(`{"rate_limit":{"primary_window":{"used_percent":12` + lengthField + resetField + `}}}`))
							} else {
								ws, _, err = ParseMetaKeyUsage([]byte(`{"subs_usage":{"window":{"used_percent":12` + lengthField + resetField + `}}}`))
							}
						} else {
							base := "X-Codex-Primary-"
							if provider == "meta" {
								base = metaHeaderBase + metaSlotWindow + "-"
							}
							signals := map[string]string{base + "Used-Percent": "12"}
							if tc.length != "" {
								signals[base+"Window-Minutes"] = tc.length
							}
							if tc.reset != "" {
								signals[base+"Reset-At"] = tc.reset
							}
							if provider == "codex" {
								ws = ParseCodexSignals(signals, at)
							} else {
								ws = ParseMetaSignals(signals, at)
							}
						}
						if err != nil || len(ws) != tc.wantRows {
							t.Fatalf("windows = %+v, err = %v, want %d rows", ws, err, tc.wantRows)
						}
						if len(ws) == 0 {
							return
						}
						if tc.wantLength == 0 {
							if ws[0].StartsAt != nil {
								t.Fatalf("invented start: %v", ws[0].StartsAt)
							}
						} else if want := time.Unix(reset, 0).UTC().Add(-tc.wantLength); ws[0].StartsAt == nil || !ws[0].StartsAt.Equal(want) {
							t.Fatalf("start = %v, want %v", ws[0].StartsAt, want)
						}
					})
				}
			})
		}
	}
}

func TestMetaWeeklyStartsAtRequiresExplicitLength(t *testing.T) {
	for _, length := range []string{"", "0", "-1", "10080"} {
		t.Run("minutes="+length, func(t *testing.T) {
			signals := map[string]string{
				metaHeaderBase + "Weekly-Used-Percent": "10",
				metaHeaderBase + "Weekly-Reset-At":     "1793500000",
			}
			field := ""
			if length != "" {
				signals[metaHeaderBase+"Weekly-Window-Minutes"] = length
				field = `,"window_duration_mins":` + length
			}
			polled, _, err := ParseMetaKeyUsage([]byte(`{"subs_usage":{"weekly":{"used_percent":10,"resets_at":1793500000` + field + `}}}`))
			if err != nil {
				t.Fatal(err)
			}
			for _, ws := range [][]Window{polled, ParseMetaSignals(signals, time.Time{})} {
				if len(ws) != 1 || ws[0].Window != "7d" {
					t.Fatalf("weekly window = %+v", ws)
				}
				if length == "10080" {
					want := time.Unix(1793500000, 0).UTC().Add(-7 * 24 * time.Hour)
					if ws[0].StartsAt == nil || !ws[0].StartsAt.Equal(want) {
						t.Fatalf("weekly start = %v, want %v", ws[0].StartsAt, want)
					}
				} else if ws[0].StartsAt != nil {
					t.Fatalf("weekly fallback invented a start: %v", ws[0].StartsAt)
				}
			}
		})
	}
}

func TestCodexStartsAtUsesExactSecondsAndRelativeReset(t *testing.T) {
	ws, _, err := ParseCodexUsage([]byte(`{"rate_limit":{"primary_window":{"used_percent":12,"limit_window_seconds":61,"reset_at":1793500000}}}`))
	if err != nil || len(ws) != 1 || ws[0].StartsAt == nil || ws[0].StartsAt.Unix() != 1793500000-61 {
		t.Fatalf("exact seconds: %+v, err = %v", ws, err)
	}
	for _, observed := range []time.Time{time.Unix(1793500000, 123).UTC(), {}} {
		for _, after := range []string{"600", "0"} {
			ws = ParseCodexSignals(map[string]string{
				"X-Codex-Primary-Used-Percent": "12", "X-Codex-Primary-Window-Minutes": "300",
				"X-Codex-Primary-Reset-After-Seconds": after,
			}, observed)
			if len(ws) != 1 {
				t.Fatalf("relative reset: %+v", ws)
			}
			if observed.IsZero() {
				if ws[0].StartsAt != nil {
					t.Fatalf("relative reset without observation invented start: %v", ws[0].StartsAt)
				}
			} else {
				n, _ := strconv.Atoi(after)
				want := observed.Truncate(time.Second).Add(time.Duration(n)*time.Second - 5*time.Hour)
				if ws[0].StartsAt == nil || !ws[0].StartsAt.Equal(want) {
					t.Fatalf("relative start = %v, want %v", ws[0].StartsAt, want)
				}
			}
		}
	}
}

func TestStartsAtJSON(t *testing.T) {
	start := time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC)
	for _, ptr := range []*time.Time{nil, &start} {
		for _, v := range []any{Current{Window: Window{StartsAt: ptr}}, Snapshot{StartsAt: ptr}, snapshotRecord{StartsAt: ptr}} {
			body, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.Contains(string(body), `"starts_at":"2026-11-18T00:00:00Z"`); got != (ptr != nil) || (ptr == nil && strings.Contains(string(body), `"starts_at"`)) {
				t.Fatalf("starts_at JSON = %s", body)
			}
		}
	}
}
