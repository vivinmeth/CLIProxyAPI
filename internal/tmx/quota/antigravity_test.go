package quota

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/json"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func antigravityTestSpec(t *testing.T) providerSpec {
	t.Helper()
	spec, _, ok := specFor(&coreauth.Auth{ID: "synthetic", Provider: "antigravity"})
	if !ok {
		t.Fatal("Antigravity quota provider is not tracked")
	}
	return spec
}

func agFixture(fraction, reset string) string {
	return fmt.Sprintf(`{"groups":[{"displayName":"Gemini Pro","buckets":[{"bucketId":"pro-short","window":"5h","remainingFraction":%s,"resetTime":%q}]}]}`, fraction, reset)
}

func TestAntigravityParseWindows(t *testing.T) {
	spec := antigravityTestSpec(t)
	for _, fraction := range []string{"0", "1", "0.25", `"0.75"`} {
		t.Run(fraction, func(t *testing.T) {
			ws, plan, err := spec.parseUsage([]byte(agFixture(fraction, "2026-11-22T12:34:56.123456+05:30")))
			if err != nil || len(ws) != 1 || plan != "" {
				t.Fatalf("windows=%+v plan=%q error=%v", ws, plan, err)
			}
			var value any
			_ = json.Unmarshal([]byte(fraction), &value)
			var remaining float64
			switch v := value.(type) {
			case float64:
				remaining = v
			case string:
				_, _ = fmt.Sscan(v, &remaining)
			}
			w := ws[0]
			reset, _ := time.Parse(time.RFC3339Nano, "2026-11-22T12:34:56.123456+05:30")
			if w.UsedPct == nil || math.Abs(*w.UsedPct-(1-remaining)*100) > 1e-9 || w.ResetsAt == nil || !w.ResetsAt.Equal(reset) || w.StartsAt == nil || w.ResetsAt.Sub(*w.StartsAt) != 5*time.Hour {
				t.Fatalf("wrong reading: %+v", w)
			}
		})
	}
	// Same bucket ID in two model groups must not merge; snake_case is part of CPAMC's contract.
	body := `{"groups":[{"displayName":"Gemini Pro","buckets":[{"bucketId":"shared","window":"weekly","remainingFraction":0.3,"resetTime":"2026-11-29T00:00:00Z"}]},{"display_name":"Claude","buckets":[{"bucket_id":"shared","window":"provider-defined","remaining_fraction":"0.6","reset_time":"2026-11-23T00:00:00Z"},{"bucket_id":"no-reset","remaining_fraction":1}]}]}`
	ws, _, err := spec.parseUsage([]byte(body))
	if err != nil || len(ws) != 3 {
		t.Fatalf("groups: %+v %v", ws, err)
	}
	seen := map[string]bool{}
	for _, w := range ws {
		if seen[w.Window] {
			t.Fatalf("group identity collision: %+v", ws)
		}
		seen[w.Window] = true
		if strings.HasPrefix(w.Window, "7d:") {
			if w.StartsAt == nil || w.ResetsAt.Sub(*w.StartsAt) != 7*24*time.Hour {
				t.Fatal("explicit weekly duration lost")
			}
		} else if w.StartsAt != nil {
			t.Fatal("unknown duration invented a start")
		}
	}
	for _, window := range []string{"5h", "five-hour", "five_hour", "weekly", "week", "", "daily", "future-window"} {
		body := fmt.Sprintf(`{"groups":[{"buckets":[{"window":%q,"remainingFraction":0.5}]}]}`, window)
		ws, _, err := spec.parseUsage([]byte(body))
		if err != nil || len(ws) != 1 || ws[0].ResetsAt != nil || ws[0].StartsAt != nil {
			t.Fatalf("window without reset: %+v %v", ws, err)
		}
	}
}

func TestAntigravityRejectsUnsupportedOrMalformedData(t *testing.T) {
	spec := antigravityTestSpec(t)
	for _, body := range []string{
		`not-json`, `null`, `[]`, `{}`, `{"models":{}}`, `{"groups":null}`, `{"groups":[]}`, `{"groups":[{}]}`, `{"groups":{}}`,
		`{"groups":[{"buckets":[{"remainingFraction":0.5,"resetTime":123}]}]}`,
		`{"groups":[{"buckets":[{"remainingFraction":null}]}]}`,
		`{"groups":[{"buckets":[{"remainingFraction":true}]}]}`,
		agFixture(`""`, ""), agFixture(`"NaN"`, ""), agFixture(`"Inf"`, ""), agFixture("-0.1", ""), agFixture("1.1", ""),
		agFixture("0.5", "tomorrow"),
		`{"groups":[{"buckets":[{"remainingFraction":0.5},{"remainingFraction":"broken"}]}]}`,
		`{"groups":[{"displayName":"Pro","buckets":[{"bucketId":"same","remainingFraction":0.5},{"bucketId":"same","remainingFraction":0.1}]}]}`,
	} {
		t.Run(body, func(t *testing.T) {
			ws, plan, err := spec.parseUsage([]byte(body))
			if err == nil || len(ws) != 0 || plan != "" {
				t.Fatalf("unsupported data became evidence: %+v %q %v", ws, plan, err)
			}
		})
	}
}

func TestAntigravitySkipsMissingFractions(t *testing.T) {
	spec := antigravityTestSpec(t)
	for _, missing := range []string{
		`{"bucketId":"omitted","resetTime":"2026-11-22T00:00:00Z"}`,
		`{"bucketId":"omitted","remainingFraction":null}`,
		`{"bucketId":"omitted","remaining_fraction":null}`,
		`{"bucketId":"omitted","remainingFraction":null,"remaining_fraction":null}`,
	} {
		t.Run(missing, func(t *testing.T) {
			body := fmt.Sprintf(`{"groups":[{"displayName":"Pro","buckets":[%s,{"bucketId":"valid","remainingFraction":0.25,"resetTime":"2026-11-23T00:00:00Z"}]}]}`, missing)
			ws, _, err := spec.parseUsage([]byte(body))
			if err != nil || len(ws) != 1 || !strings.HasSuffix(ws[0].Window, ":valid") || ws[0].UsedPct == nil || *ws[0].UsedPct != 75 || ws[0].ResetsAt == nil || ws[0].ResetsAt.Format(time.RFC3339) != "2026-11-23T00:00:00Z" {
				t.Fatalf("omitted fraction suppressed valid bucket or became usage: %+v %v", ws, err)
			}
			allMissing := fmt.Sprintf(`{"groups":[{"buckets":[%s,{}]}]}`, missing)
			ws, _, err = spec.parseUsage([]byte(allMissing))
			if err == nil || len(ws) != 0 {
				t.Fatalf("all omitted fractions became evidence: %+v %v", ws, err)
			}
		})
	}
	// A null camelCase value may still carry a real fraction in snake_case.
	ws, _, err := spec.parseUsage([]byte(`{"groups":[{"buckets":[{"remainingFraction":null,"remaining_fraction":0}]}]}`))
	if err != nil || len(ws) != 1 || ws[0].UsedPct == nil || *ws[0].UsedPct != 100 {
		t.Fatalf("explicit zero fraction lost: %+v %v", ws, err)
	}
}

type agRequest struct{ id, project, method, url string }
type agFetcher struct {
	auths    []*coreauth.Auth
	replies  map[string]fakeRoute
	requests []agRequest
}

func (f *agFetcher) TmxAuths() []*coreauth.Auth { return f.auths }
func (f *agFetcher) TmxFetch(context.Context, *coreauth.Auth, string, map[string]string) (int, []byte, error) {
	return 0, nil, errors.New("unexpected GET")
}
func (f *agFetcher) TmxRequest(_ context.Context, a *coreauth.Auth, method, url string, headers map[string]string, body []byte) (int, []byte, error) {
	var payload map[string]string
	if err := json.Unmarshal(body, &payload); err != nil || len(payload) != 1 || payload["project"] == "" {
		return 0, nil, errors.New("invalid project body")
	}
	if headers["Authorization"] != "Bearer $TOKEN$" || headers["Content-Type"] != "application/json" || !strings.HasPrefix(headers["User-Agent"], "antigravity/hub/") {
		return 0, nil, errors.New("invalid authenticated headers")
	}
	f.requests = append(f.requests, agRequest{a.ID, payload["project"], method, url})
	r := f.replies[a.ID]
	return r.status, []byte(r.body), nil
}

// A synthetic SQL sink exercises record/PollNow without a database or credentials.
// It does not claim to verify PostgreSQL persistence (the existing integration tests do).
type agSQLSink struct{ rows [][]driver.NamedValue }

func (d *agSQLSink) Connect(context.Context) (driver.Conn, error) { return &agSQLConn{d}, nil }
func (d *agSQLSink) Driver() driver.Driver                        { return nil }

type agSQLConn struct{ sink *agSQLSink }

func (*agSQLConn) Prepare(string) (driver.Stmt, error) { return nil, errors.New("unexpected prepare") }
func (*agSQLConn) Close() error                        { return nil }
func (*agSQLConn) Begin() (driver.Tx, error)           { return nil, errors.New("unexpected transaction") }
func (c *agSQLConn) ExecContext(_ context.Context, _ string, args []driver.NamedValue) (driver.Result, error) {
	c.sink.rows = append(c.sink.rows, append([]driver.NamedValue(nil), args...))
	return driver.RowsAffected(1), nil
}

func TestAntigravityPollAccountIsolation(t *testing.T) {
	antigravityTestSpec(t)
	now := time.Date(2026, 11, 21, 12, 0, 0, 0, time.UTC)
	f := &agFetcher{auths: []*coreauth.Auth{
		{ID: "ag-a", Provider: "antigravity", Metadata: map[string]any{"project_id": "synthetic-a"}},
		{ID: "ag-b", Provider: "antigravity", Metadata: map[string]any{"project_id": "synthetic-b"}},
	}, replies: map[string]fakeRoute{
		"ag-a": {200, agFixture("0.25", "2026-11-21T12:05:00Z")},
		"ag-b": {200, agFixture("0.75", "2026-11-21T18:00:00Z")},
	}}
	sink := &agSQLSink{}
	db := sql.OpenDB(sink)
	t.Cleanup(func() { _ = db.Close() })
	s := &Service{db: db, table: "synthetic.quota_snapshots", fetch: f, now: func() time.Time { return now }, pollEvery: 10 * time.Minute, slowEvery: time.Hour, accounts: map[string]*account{}}
	s.tick(context.Background())
	if len(f.requests) != 2 || len(sink.rows) != 2 || f.requests[0].project != "synthetic-a" || f.requests[1].project != "synthetic-b" {
		t.Fatalf("account POSTs=%+v rows=%d", f.requests, len(sink.rows))
	}
	for _, req := range f.requests {
		if req.method != "POST" || req.url != "https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary" {
			t.Fatalf("request: %+v", req)
		}
	}
	first := s.Accounts()
	if len(first) != 2 || len(first[0].Windows) != 1 || len(first[1].Windows) != 1 || *first[0].Windows[0].UsedPct != 75 || *first[1].Windows[0].UsedPct != 25 || first[0].Windows[0].ResetsAt.Equal(*first[1].Windows[0].ResetsAt) {
		t.Fatalf("accounts: %+v", first)
	}
	now = now.Add(11 * time.Minute)
	f.replies["ag-a"] = fakeRoute{429, `{"error":"synthetic"}`}
	f.replies["ag-b"] = fakeRoute{200, agFixture("0.5", "2026-11-21T20:00:00Z")}
	s.tick(context.Background())
	failed, healthy := s.Accounts()[0], s.Accounts()[1]
	if failed.PollError != "rate limited (429)" || len(failed.Windows) != 1 || !failed.Windows[0].Stale || !failed.Windows[0].ObservedAt.Equal(first[0].Windows[0].ObservedAt) || *failed.Windows[0].UsedPct != 75 || healthy.PollError != "" || *healthy.Windows[0].UsedPct != 50 || healthy.Windows[0].Stale {
		t.Fatalf("failed=%+v healthy=%+v", failed, healthy)
	}
	before := len(f.requests)
	if _, throttled, err := s.PollNow(context.Background(), "ag-a"); err != nil || !throttled || len(f.requests) != before {
		t.Fatal("failed account cooldown not enforced", err)
	}
	for _, body := range []string{
		`{"groups":[]}`,
		`{"groups":[{"buckets":[{}, {"remainingFraction":null,"remaining_fraction":null}]}]}`,
		`{"groups":[{"buckets":[{"remainingFraction":0.5},{"remainingFraction":"broken"}]}]}`,
	} {
		now = now.Add(time.Minute)
		f.replies["ag-b"] = fakeRoute{200, body}
		bad, throttled, err := s.PollNow(context.Background(), "ag-b")
		if err != nil || throttled || bad.PollError == "" || len(bad.Windows) != 1 || *bad.Windows[0].UsedPct != 50 || !bad.Windows[0].ObservedAt.Equal(healthy.Windows[0].ObservedAt) {
			t.Fatalf("missing/malformed response cleared evidence: %+v %v", bad, err)
		}
	}
	// Removing project metadata is an account error before the authenticated adapter.
	now = now.Add(time.Minute)
	f.auths[1].Metadata = nil
	before = len(f.requests)
	missing, _, err := s.PollNow(context.Background(), "ag-b")
	if err != nil || !strings.Contains(missing.PollError, "missing project ID") || len(f.requests) != before || len(missing.Windows) != 1 || !missing.Windows[0].ObservedAt.Equal(healthy.Windows[0].ObservedAt) {
		t.Fatalf("missing project lost state: %+v %v", missing, err)
	}
	// A's rate-limit pause ends independently and its new reset does not renew B.
	now = now.Add(30 * time.Minute)
	f.replies["ag-a"] = fakeRoute{200, agFixture("1", "2026-11-22T12:00:00Z")}
	recovered, throttled, err := s.PollNow(context.Background(), "ag-a")
	if err != nil || throttled || recovered.PollError != "" || recovered.Windows[0].Stale || *recovered.Windows[0].UsedPct != 0 || !s.Accounts()[1].Windows[0].ObservedAt.Equal(healthy.Windows[0].ObservedAt) {
		t.Fatalf("recovery crossed accounts: %+v %v", recovered, err)
	}
}

func TestAntigravityMissingProjectNeverFetches(t *testing.T) {
	spec := antigravityTestSpec(t)
	f := &agFetcher{}
	s := &Service{fetch: f}
	for _, a := range []*coreauth.Auth{nil, {ID: "missing", Provider: "antigravity"}, {ID: "wrong-type", Metadata: map[string]any{"project_id": 42}}} {
		_, _, err := s.fetchUsage(context.Background(), spec, a)
		if err == nil || !strings.Contains(err.Error(), "project") || len(f.requests) != 0 {
			t.Fatalf("missing project: %v", err)
		}
	}
}

func TestAntigravityBodyMetadataAndEscaping(t *testing.T) {
	project := "synthetic-\"quoted\\project"
	for _, auth := range []*coreauth.Auth{
		{Metadata: map[string]any{"project_id": " " + project + " "}, Attributes: map[string]string{"project_id": "wrong-account"}},
		{Metadata: map[string]any{"projectId": project}},
		{Attributes: map[string]string{"project_id": project}},
		{Attributes: map[string]string{"projectId": project}},
		{Attributes: map[string]string{"gemini_virtual_project": project}},
	} {
		body, err := antigravityBody(auth)
		var parsed map[string]string
		if err != nil || json.Unmarshal(body, &parsed) != nil || len(parsed) != 1 || parsed["project"] != project {
			t.Fatalf("project body did not preserve account metadata: %v", err)
		}
	}
}

func TestQuotaBodyHookKeepsLegacyFetchSemantics(t *testing.T) {
	// Reuse the existing Meta request spy to prove its static body still wins
	// when the optional per-account callback is absent.
	f := &metaFetcher{fakeFetcher: fakeFetcher{calls: map[string]int{}, status: 200}}
	s := &Service{fetch: f}
	meta := &coreauth.Auth{ID: "synthetic-meta", Provider: "meta", Metadata: map[string]any{"base_url": "http://stub/v1", "dca_token": "dca:synthetic"}}
	status, _, err := s.fetchUsage(context.Background(), providers["meta"], meta)
	if err != nil || status != 200 || f.posts != 1 || f.method != "POST" || f.body != "{}" || f.auth != "Bearer dca:synthetic" {
		t.Fatalf("legacy POST changed: status=%d error=%v body=%q", status, err, f.body)
	}
	for _, provider := range []string{"claude", "codex"} {
		auth := &coreauth.Auth{ID: "synthetic-" + provider, Provider: provider}
		_, _, err := s.fetchUsage(context.Background(), providers[provider], auth)
		if err != nil || f.calls[providers[provider].usageURL] != 1 || f.posts != 1 || f.headers["Authorization"] != "Bearer $TOKEN$" {
			t.Fatalf("legacy %s GET changed: %v", provider, err)
		}
	}
}
