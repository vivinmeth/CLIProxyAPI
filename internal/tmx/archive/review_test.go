package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

// openTestReview starts analytics on a fresh review schema with agents_rw managed by the proxy.
func openTestReview(t *testing.T) (*Archive, *analytics) {
	t.Helper()
	dsn := os.Getenv("TMX_ANALYTICS_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_ANALYTICS_TEST_DSN not set")
	}
	raw, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	for _, stmt := range []string{`DROP SCHEMA IF EXISTS review CASCADE`,
		`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'agents') THEN CREATE ROLE agents NOLOGIN; END IF; END $$`} {
		if _, err = raw.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	_ = raw.Close()
	t.Setenv(reviewRWEnv, "rw-test-pw")
	return openTestAnalytics(t)
}

// connectAs opens the analytics test DB as another role.
func connectAs(t *testing.T, role, pw string) *sql.DB {
	t.Helper()
	u, err := url.Parse(os.Getenv("TMX_ANALYTICS_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	u.User = url.UserPassword(role, pw)
	db, err := sql.Open("pgx", u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func pgCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}

// agents_rw can write review.* and read everything, but can't write analytics tables or change
// review rows; the read-only role reads review.*; review rows are append-only even for the owner.
func TestReviewRolePermissions(t *testing.T) {
	_, an := openTestReview(t)
	if !an.review.SchemaReady || !an.review.RoleExists || !an.review.RoleManaged || !an.review.Grants || an.review.SetupSQL != "" {
		t.Fatalf("review setup = %+v", &an.review)
	}
	rw := connectAs(t, reviewRWRole, "rw-test-pw")
	var reviewID, findingID, verdictID int64
	if err := rw.QueryRow(`INSERT INTO review.reviews (model, repo, pr_number) VALUES ('gpt-6', 'o/r', 1) RETURNING id`).Scan(&reviewID); err != nil {
		t.Fatalf("agents_rw insert review: %v", err)
	}
	if err := rw.QueryRow(`INSERT INTO review.findings (review_id, text) VALUES ($1, '  Off-by-one   in loop ') RETURNING id`,
		reviewID).Scan(&findingID); err != nil {
		t.Fatalf("agents_rw insert finding: %v", err)
	}
	if err := rw.QueryRow(`INSERT INTO review.verdicts (finding_id, verdict, unique_to_model) VALUES ($1, 'valid', true) RETURNING id`,
		findingID).Scan(&verdictID); err != nil {
		t.Fatalf("agents_rw insert verdict: %v", err)
	}
	var hash, by string
	if err := rw.QueryRow(`SELECT text_hash, recorded_by FROM review.findings WHERE id = $1`, findingID).Scan(&hash, &by); err != nil {
		t.Fatal(err)
	}
	if want := HashOf([]byte("off-by-one in loop")); hash != want || by != reviewRWRole {
		t.Fatalf("text_hash %s (want %s), recorded_by %s", hash, want, by)
	}
	an.refreshLinks(true) // tasks reads the request_sessions materialized view
	for _, q := range []string{`SELECT count(*) FROM sessions`, `SELECT count(*) FROM items`, `SELECT count(*) FROM tasks`,
		`SELECT count(*) FROM review.model_tally`, `SELECT count(*) FROM prs`} {
		var n int
		if err := rw.QueryRow(q).Scan(&n); err != nil {
			t.Fatalf("agents_rw %s: %v", q, err)
		}
	}
	for _, q := range []string{
		`INSERT INTO sessions (id, kind, version) VALUES ('x', 'imported', 1)`,
		`INSERT INTO commits (id, repo, sha) VALUES ('r@abcdef0', 'r', 'abcdef0')`,
		`DELETE FROM sessions`,
		`UPDATE review.verdicts SET verdict = 'invalid'`,
		`DELETE FROM review.findings`,
		`TRUNCATE review.verdicts`,
		`CREATE TABLE review.mine (x int)`,
		`CREATE TABLE public.mine (x int)`,
		`DROP TABLE review.verdicts`,
	} {
		if _, err := rw.Exec(q); pgCode(err) != "42501" {
			t.Fatalf("agents_rw %q: want permission denied, got %v", q, err)
		}
	}
	// Owner too: append-only.
	for _, q := range []string{`UPDATE review.verdicts SET verdict = 'invalid'`, `DELETE FROM review.reviews`, `TRUNCATE review.verdicts`} {
		if _, err := an.db.Exec(q); err == nil || !strings.Contains(err.Error(), "append-only") {
			t.Fatalf("owner %q: want append-only error, got %v", q, err)
		}
	}
	var sel, ins bool
	if err := an.db.QueryRow(`SELECT has_table_privilege('agents', 'review.verdicts', 'SELECT'),
		has_table_privilege('agents', 'review.verdicts', 'INSERT')`).Scan(&sel, &ins); err != nil {
		t.Fatal(err)
	}
	if !sel || ins {
		t.Fatalf("agents on review.verdicts: select %v insert %v", sel, ins)
	}
	// Views are recreated on every start: agents_rw keeps SELECT on them.
	if err := an.ensureSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := rw.QueryRow(`SELECT has_table_privilege('tasks', 'SELECT') AND has_table_privilege('pr_outcomes', 'SELECT')
		AND NOT has_table_privilege('prs', 'INSERT')`).Scan(&sel); err != nil || !sel {
		t.Fatalf("agents_rw on recreated views: %v %v", sel, err)
	}
	// Restarting doesn't create the role twice and keeps the password in sync.
	if !an.review.RoleExists || an.review.Error != "" {
		t.Fatalf("second setup = %+v", &an.review)
	}
}

// Wiping and rebuilding analytics (the runbook's TRUNCATE ... CASCADE, a backfill, a session
// delete, the views recreated) leaves review.* intact, and findings still join their items.
func TestReviewSurvivesRebuild(t *testing.T) {
	a, an := openTestReview(t)
	ctx := context.Background()
	ts := time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC)
	in := SessionIngest{Harness: "claude-code", ExternalID: "rev-1", StartedAt: &ts, Messages: []MessageIngest{
		{ExternalID: "m1", Seq: 1, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":"review PR 7"}`)},
		{ExternalID: "m2", Seq: 2, Role: "assistant", TS: &ts, Model: "claude-opus-5-5",
			Message: json.RawMessage(`{"role":"assistant","content":"Finding: nil map write in foo.go:12"}`)},
	}}
	if _, err := a.IngestSession(ctx, in); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	seq, idx := 1, 0
	res, err := a.RecordReviews(ctx, []ReviewIngest{{ExternalID: "run-1", Model: "claude-opus-5-5", Harness: "claude-code",
		SessionID: "claude-code:rev-1", Repo: "o/r", PRNumber: 7, CommitSHA: "ABCDEF1",
		Findings: []FindingIngest{{ExternalID: "f1", Text: "nil map write", Path: "foo.go", Line: &[]int{12}[0],
			SessionID: "claude-code:rev-1", Seq: &seq, Idx: &idx}}}})
	if err != nil || res.Created != 1 || len(res.Results[0].Findings) != 1 {
		t.Fatalf("record = %+v %v", res, err)
	}
	if _, err = a.RecordVerdicts(ctx, []VerdictIngest{{FindingID: res.Results[0].Findings[0].ID, Verdict: "valid",
		JudgedByModel: "gpt-6"}}); err != nil {
		t.Fatal(err)
	}
	counts := func() string {
		var r, f, v int
		if err := an.db.QueryRow(`SELECT (SELECT count(*) FROM review.reviews), (SELECT count(*) FROM review.findings),
			(SELECT count(*) FROM review.verdicts)`).Scan(&r, &f, &v); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%d/%d/%d", r, f, v)
	}
	before := counts()
	for _, stmt := range []string{"TRUNCATE sessions CASCADE", "TRUNCATE requests, exchanges, state, failed",
		"TRUNCATE threads CASCADE", "TRUNCATE commits CASCADE", "TRUNCATE prs CASCADE"} {
		if _, err = an.db.Exec(stmt); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = a.AnalyticsBackfill(ctx); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if err = an.ensureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	if got := counts(); got != before || before != "1/1/1" {
		t.Fatalf("review rows %s after rebuild (before %s)", got, before)
	}
	var text string
	if err = an.db.QueryRow(`SELECT i.text FROM review.findings f JOIN items i ON (i.session_id, i.seq, i.idx) = (f.session_id, f.seq, f.idx)`).Scan(&text); err != nil ||
		!strings.Contains(text, "nil map write") {
		t.Fatalf("finding -> item join: %q %v", text, err)
	}
	// Deleting the session (cascades through public) doesn't reach review either.
	if _, err = a.DeleteSession(ctx, "claude-code:rev-1"); err != nil {
		t.Fatal(err)
	}
	drain(t, an)
	if got := counts(); got != before {
		t.Fatalf("review rows %s after session delete", got)
	}
}

// The management API: idempotent by external id, per-item errors, verdicts superseded (never
// updated), tally by latest verdict.
func TestReviewAPI(t *testing.T) {
	a, _ := openTestReview(t)
	ctx := context.Background()
	reviews := []ReviewIngest{
		{ExternalID: "r-a", Model: "model-a", Repo: "o/r", PRNumber: 3, Findings: []FindingIngest{
			{ExternalID: "1", Text: "bug one"}, {ExternalID: "2", Text: "style nit"}}},
		{Model: ""}, // per-item error
		{ExternalID: "r-b", Model: "model-b", Repo: "o/r", PRNumber: 3, Findings: []FindingIngest{{ExternalID: "1", Text: "Bug  one"}}},
		{ExternalID: "r-c", Model: "model-b", CommitSHA: "not-hex"},
	}
	res, err := a.RecordReviews(ctx, reviews)
	if err != nil || res.Created != 2 || res.Errors != 2 || res.Results[1].Status != "error" || res.Results[1].Retryable ||
		res.Results[3].Status != "error" {
		t.Fatalf("record = %+v %v", res, err)
	}
	again, err := a.RecordReviews(ctx, reviews)
	if err != nil || again.Exists != 2 || again.Results[0].ID != res.Results[0].ID ||
		again.Results[0].Findings[1].ID != res.Results[0].Findings[1].ID || again.Results[0].Findings[1].Status != "exists" {
		t.Fatalf("re-record = %+v %v", again, err)
	}
	a1, a2, b1 := res.Results[0].Findings[0].ID, res.Results[0].Findings[1].ID, res.Results[2].Findings[0].ID
	vs := []VerdictIngest{
		{ExternalID: "v1", FindingID: a1, Verdict: "valid", UniqueToModel: &[]bool{false}[0], JudgedByModel: "judge"},
		{ExternalID: "v2", FindingID: a2, Verdict: "valid", UniqueToModel: &[]bool{true}[0]},
		{ExternalID: "v3", ReviewExternalID: "r-b", FindingExternalID: "1", Verdict: "dup", DuplicateOf: a1},
		{FindingID: 999999, Verdict: "valid"}, // unknown finding
		{FindingID: a1, Verdict: "maybe"},     // bad value
	}
	vr, err := a.RecordVerdicts(ctx, vs)
	if err != nil || vr.Created != 3 || vr.Errors != 2 || vr.Results[3].Retryable || vr.Results[4].Retryable {
		t.Fatalf("verdicts = %+v %v", vr, err)
	}
	if vr, err = a.RecordVerdicts(ctx, vs[:3]); err != nil || vr.Exists != 3 {
		t.Fatalf("re-send verdicts = %+v %v", vr, err)
	}
	// A second opinion supersedes: a2 is now invalid.
	if vr, err = a.RecordVerdicts(ctx, []VerdictIngest{{ExternalID: "v4", FindingID: a2, Verdict: "invalid", Rationale: "intended"}}); err != nil || vr.Created != 1 {
		t.Fatalf("supersede = %+v %v", vr, err)
	}
	tally, err := a.ReviewTally(ctx, TallyQuery{Repo: "O/R"})
	if err != nil {
		t.Fatal(err)
	}
	byModel := map[string]TallyRow{}
	for _, r := range tally {
		byModel[r.Model] = r
	}
	ma, mb := byModel["model-a"], byModel["model-b"]
	if ma.Reviews != 1 || ma.Findings != 2 || ma.Valid != 1 || ma.Invalid != 1 || ma.UniqueValid != 0 || ma.Precision == nil || *ma.Precision != 0.5 {
		t.Fatalf("model-a = %+v", ma)
	}
	if mb.Findings != 1 || mb.Dup != 1 || mb.Precision != nil {
		t.Fatalf("model-b = %+v", mb)
	}
	items, err := a.ReviewFindings(ctx, FindingQuery{Model: "model-a", Verdict: "invalid"})
	if err != nil || len(items) != 1 || !strings.Contains(string(items[0]), `"verdicts": 2`) {
		t.Fatalf("findings = %s %v", items, err)
	}
	if items, err = a.ReviewFindings(ctx, FindingQuery{Verdict: "none"}); err != nil || len(items) != 0 {
		t.Fatalf("unjudged = %s %v", items, err)
	}
	// Same normalized text, same hash: duplicates across models are findable.
	var n int
	if err = a.analytics.db.QueryRow(`SELECT count(DISTINCT text_hash) FROM review.findings WHERE id IN ($1, $2)`, a1, b1).Scan(&n); err != nil || n != 1 {
		t.Fatalf("hashes %d %v", n, err)
	}
	if _, err = a.RecordVerdicts(ctx, make([]VerdictIngest, maxReviewItem+1)); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("too large: %v", err)
	}
}

// Without the password (or the privilege) the role isn't created and the setup SQL is offered.
func TestReviewSetupSQLWhenRoleMissing(t *testing.T) {
	sqlText := ReviewSetupSQL("analytics")
	for _, want := range []string{`CREATE ROLE agents_rw LOGIN PASSWORD :'pw'`, `GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA review TO agents_rw`,
		`GRANT SELECT ON ALL TABLES IN SCHEMA public TO agents_rw`, `GRANT CONNECT ON DATABASE "analytics" TO agents_rw`} {
		if !strings.Contains(sqlText, want) {
			t.Fatalf("setup SQL lacks %q:\n%s", want, sqlText)
		}
	}
	if strings.Contains(sqlText, "INSERT ON ALL TABLES IN SCHEMA public") || strings.Contains(sqlText, "UPDATE") ||
		strings.Contains(sqlText, "DELETE") {
		t.Fatalf("setup SQL grants too much:\n%s", sqlText)
	}
}
