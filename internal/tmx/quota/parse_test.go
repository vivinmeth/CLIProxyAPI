package quota

import (
	"testing"
	"time"
)

func TestParseClaudeSignals(t *testing.T) {
	ws := ParseClaudeSignals(map[string]string{
		"Anthropic-Ratelimit-Unified-5h-Reset":       "1792520400",
		"Anthropic-Ratelimit-Unified-5h-Status":      "allowed",
		"Anthropic-Ratelimit-Unified-5h-Utilization": "0.02",
		"Anthropic-Ratelimit-Unified-7d-Reset":       "1793088000",
		"Anthropic-Ratelimit-Unified-7d-Utilization": "0.04",
		"Anthropic-Ratelimit-Unified-Status":         "allowed",
	})
	if len(ws) != 2 || ws[0].Window != "5h" || ws[1].Window != "7d" {
		t.Fatalf("windows = %+v", ws)
	}
	if *ws[0].UsedPct != 2 || ws[0].Status != "allowed" || ws[0].ResetsAt.Unix() != 1792520400 {
		t.Fatalf("5h = %+v", ws[0])
	}
	if *ws[1].UsedPct != 4 {
		t.Fatalf("7d used = %v", *ws[1].UsedPct)
	}
}

const usageFixture = `{"five_hour":{"utilization":2.0,"resets_at":"2026-10-20T18:19:59.555617+00:00","limit_dollars":null},
"seven_day":{"utilization":4.0,"resets_at":"2026-10-27T07:59:59.555641+00:00","limit_dollars":null},
"seven_day_opus":null,"iguana_necktie":{"utilization":12,"resets_at":"2026-10-27T07:59:59Z","limit_dollars":50},
"limits":[{"kind":"session","percent":2,"resets_at":"2026-10-20T18:19:59Z","scope":null},
{"kind":"weekly_scoped","percent":37.5,"resets_at":"2026-10-27T07:59:59.555814+00:00","scope":{"model":{"display_name":"Fable 5"}}}]}`

func TestParseClaudeUsage(t *testing.T) {
	ws, err := ParseClaudeUsage([]byte(usageFixture))
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]Window{}
	for _, w := range ws {
		got[w.Window] = w
	}
	if len(got) != 3 {
		t.Fatalf("windows = %+v (dollar pools and nulls must be skipped)", ws)
	}
	if *got["5h"].UsedPct != 2 || *got["7d"].UsedPct != 4 || *got["7d:fable-5"].UsedPct != 37.5 {
		t.Fatalf("values = %+v", got)
	}
	want := time.Date(2026, 10, 27, 7, 59, 59, 0, time.UTC)
	if !got["7d:fable-5"].ResetsAt.Equal(want) {
		t.Fatalf("fable reset = %v", got["7d:fable-5"].ResetsAt)
	}
}

func TestParseClaudePlan(t *testing.T) {
	cases := map[string]string{
		`{"account":{"has_claude_max":true,"has_claude_pro":false},"organization":{"organization_type":"claude_max","rate_limit_tier":"default_claude_max_20x","subscription_status":"active"}}`: "max_20x",
		`{"account":{"has_claude_max":true},"organization":{"rate_limit_tier":"default_claude_max_5x"}}`:                                                                                         "max_5x",
		`{"account":{"has_claude_max":false,"has_claude_pro":true}}`:                                                                                                                             "pro",
		`{"account":{"has_claude_max":false,"has_claude_pro":false}}`:                                                                                                                            "free",
		`{"organization":{"organization_type":"claude_team","subscription_status":"active"}}`:                                                                                                    "team",
		`not json`: "",
	}
	for in, want := range cases {
		if got := ParseClaudePlan([]byte(in)); got != want {
			t.Errorf("plan(%s) = %q, want %q", in, got, want)
		}
	}
}
