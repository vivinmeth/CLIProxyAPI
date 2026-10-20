package requestlog

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func TestClassify(t *testing.T) {
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name   string
		ctx    context.Context
		record coreusage.Record
		want   string
	}{
		{"success", context.Background(), coreusage.Record{}, OutcomeSuccess},
		{"upstream 429", context.Background(), coreusage.Record{Failed: true, Fail: coreusage.Failure{StatusCode: 429, Body: "rate limited"}}, OutcomeFailed},
		{"client closed 499", context.Background(), coreusage.Record{Failed: true, Fail: coreusage.Failure{StatusCode: 499}}, OutcomeCanceled},
		{"context canceled body", context.Background(), coreusage.Record{Failed: true, Fail: coreusage.Failure{StatusCode: 500, Body: "read: context canceled"}}, OutcomeCanceled},
		{"canceled request ctx", canceledCtx, coreusage.Record{Failed: true, Fail: coreusage.Failure{StatusCode: 502}}, OutcomeCanceled},
	}
	for _, tc := range cases {
		if got, _ := classify(tc.ctx, tc.record); got != tc.want {
			t.Errorf("%s: got %s, want %s", tc.name, got, tc.want)
		}
	}
	if !errors.Is(canceledCtx.Err(), context.Canceled) {
		t.Fatal("sanity")
	}
}

func TestRowFromRecordNeverStoresTheKey(t *testing.T) {
	key := "sk-live-super-secret-key-123456"
	row := rowFromRecord(context.Background(), coreusage.Record{
		RequestID:   "r1",
		TraceID:     "abcd1234",
		Provider:    "claude",
		Model:       "claude-opus-5-5",
		APIKey:      key,
		AuthID:      "claude-x.json",
		Source:      "me@example.com",
		RequestedAt: time.Date(2026, 10, 20, 9, 0, 0, 0, time.FixedZone("IST", 19800)),
		Latency:     1500 * time.Millisecond,
		TTFT:        300 * time.Millisecond,
		Detail:      coreusage.Detail{InputTokens: 10, OutputTokens: 20, CacheReadTokens: 1000, CacheCreationTokens: 50},
	})
	if row.APIKeyHash == "" || strings.Contains(row.APIKeyHash, "secret") || len(row.APIKeyHash) != 16 {
		t.Fatalf("api key hash = %q", row.APIKeyHash)
	}
	if row.Outcome != OutcomeSuccess || row.Error != "" {
		t.Fatalf("outcome/error = %q/%q", row.Outcome, row.Error)
	}
	if row.TS.Location() != time.UTC || row.TS.Hour() != 3 || row.TS.Minute() != 30 {
		t.Fatalf("ts not normalised to UTC: %v", row.TS)
	}
	if row.LatencyMs != 1500 || row.TTFTMs != 300 || row.CacheRead != 1000 || row.AuthLabel != "me@example.com" {
		t.Fatalf("unexpected row: %+v", row)
	}
}

func TestTruncateKeepsValidUTF8(t *testing.T) {
	got := truncate(strings.Repeat("é", 10), 5)
	if !strings.HasSuffix(got, "…") || strings.ContainsRune(got, '�') {
		t.Fatalf("truncate = %q", got)
	}
}

func TestClientAddressPrefersForwardedFor(t *testing.T) {
	if got := clientAddress("10.64.0.64, 10.0.1.4", "10.0.1.4", "10.0.1.4"); got != "10.64.0.64" {
		t.Fatalf("got %q", got)
	}
	if got := clientAddress("", "", "10.64.0.28"); got != "10.64.0.28" {
		t.Fatalf("got %q", got)
	}
}
