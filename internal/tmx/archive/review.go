package archive

// Review verdicts (tokenmaxx fork): a durable, writable store in the analytics DB for judging PR
// reviews — which reviewer run (model, harness, session) raised which finding, and what judges
// (agents or people) decided about it ("valid, and only this model caught it").
//
// Unlike everything else in the analytics DB this is primary data, not derived: it lives in its
// own schema `review`, which no rebuild touches. Rebuild code only drops named views in public,
// truncates / deletes public tables, and review tables have no foreign keys into public (so
// TRUNCATE ... CASCADE and session deletes can't reach them) and review views read review tables
// only (so dropping public views never depends on them). Rows are append-only: a new verdict
// supersedes the old one; UPDATE / DELETE / TRUNCATE raise.
//
// Access: the role agents_rw (created here when TMX_ANALYTICS_AGENTS_RW_PASSWORD is set and the
// analytics user may create roles; otherwise the SQL is logged and shown on /tmx/review/status)
// may SELECT everything and INSERT into review.* only. The read-only agents role gets SELECT on
// review.* too. The management API (/tmx/review/*) writes the same tables for agents without SQL.

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	log "github.com/sirupsen/logrus"
)

const (
	reviewSchema  = "review"
	reviewRWRole  = "agents_rw"
	reviewRORole  = "agents"
	reviewRWEnv   = "TMX_ANALYTICS_AGENTS_RW_PASSWORD"
	maxReviewItem = 1000
)

var reviewSchemaSQL = []string{
	`CREATE SCHEMA IF NOT EXISTS review`,
	// Append-only guard for every review table.
	`CREATE OR REPLACE FUNCTION review.append_only() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
		RAISE EXCEPTION 'review.% is append-only: insert a new row instead (a new verdict supersedes the old one)', TG_TABLE_NAME;
	END $$`,
	// A reviewer run: one model reviewing one PR / commit.
	`CREATE TABLE IF NOT EXISTS review.reviews (
		id               BIGSERIAL PRIMARY KEY,
		external_id      TEXT UNIQUE,                 -- caller's idempotency key (optional)
		model            TEXT NOT NULL DEFAULT '',    -- the reviewer model
		harness          TEXT NOT NULL DEFAULT '',    -- claude-code | codex | opencode | github-app …
		session_id       TEXT NOT NULL DEFAULT '',    -- analytics sessions.id of the reviewer run
		thread_id        TEXT NOT NULL DEFAULT '',    -- analytics threads.id, if a thread ran it
		repo             TEXT NOT NULL DEFAULT '',    -- owner/name (prs.repo)
		pr_number        BIGINT,                      -- prs.number
		commit_sha       TEXT NOT NULL DEFAULT '',    -- the reviewed head (prs.head_sha / commits.sha)
		github_review_id BIGINT,                      -- pr_reviews.review_id when it was posted to GitHub
		started_at       TIMESTAMPTZ,
		finished_at      TIMESTAMPTZ,
		source           TEXT NOT NULL DEFAULT '',    -- who recorded it: github | session | agent:<name> | manual …
		metadata         JSONB NOT NULL DEFAULT '{}',
		recorded_by      TEXT NOT NULL DEFAULT current_user,
		recorded_at      TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS reviews_pr_idx ON review.reviews (repo, pr_number)`,
	`CREATE INDEX IF NOT EXISTS reviews_commit_idx ON review.reviews (commit_sha) WHERE commit_sha <> ''`,
	`CREATE INDEX IF NOT EXISTS reviews_session_idx ON review.reviews (session_id) WHERE session_id <> ''`,
	`CREATE INDEX IF NOT EXISTS reviews_model_idx ON review.reviews (model)`,
	// A finding a review raised, anchored to the conversation item that said it.
	`CREATE TABLE IF NOT EXISTS review.findings (
		id                BIGSERIAL PRIMARY KEY,
		review_id         BIGINT NOT NULL REFERENCES review.reviews(id),
		external_id       TEXT,                       -- caller's key, unique within the review
		text              TEXT NOT NULL,
		text_hash         TEXT NOT NULL DEFAULT '',   -- sha256 of the trimmed, lower-cased, space-collapsed text (set on insert)
		path              TEXT NOT NULL DEFAULT '',
		line              INTEGER,
		end_line          INTEGER,
		severity          TEXT NOT NULL DEFAULT '',
		category          TEXT NOT NULL DEFAULT '',
		session_id        TEXT NOT NULL DEFAULT '',   -- analytics items (session_id, seq, idx)
		seq               INTEGER,
		idx               INTEGER,
		github_comment_id BIGINT,                     -- pr_review_comments.comment_id
		metadata          JSONB NOT NULL DEFAULT '{}',
		recorded_by       TEXT NOT NULL DEFAULT current_user,
		recorded_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
		UNIQUE (review_id, external_id)
	)`,
	`CREATE INDEX IF NOT EXISTS findings_review_idx ON review.findings (review_id)`,
	`CREATE INDEX IF NOT EXISTS findings_hash_idx ON review.findings (text_hash)`,
	`CREATE INDEX IF NOT EXISTS findings_item_idx ON review.findings (session_id, seq, idx) WHERE session_id <> ''`,
	`CREATE INDEX IF NOT EXISTS findings_comment_idx ON review.findings (github_comment_id) WHERE github_comment_id IS NOT NULL`,
	`CREATE OR REPLACE FUNCTION review.finding_hash() RETURNS trigger LANGUAGE plpgsql AS $$
	BEGIN
		NEW.text_hash := encode(sha256(convert_to(lower(regexp_replace(btrim(NEW.text), '\s+', ' ', 'g')), 'UTF8')), 'hex');
		RETURN NEW;
	END $$`,
	`CREATE OR REPLACE TRIGGER findings_hash BEFORE INSERT ON review.findings FOR EACH ROW EXECUTE FUNCTION review.finding_hash()`,
	// A judgment of a finding. Never updated: the latest row per finding (highest id) is the verdict.
	`CREATE TABLE IF NOT EXISTS review.verdicts (
		id              BIGSERIAL PRIMARY KEY,
		external_id     TEXT UNIQUE,                  -- caller's idempotency key (optional)
		finding_id      BIGINT NOT NULL REFERENCES review.findings(id),
		verdict         TEXT NOT NULL CHECK (verdict IN ('valid', 'invalid', 'dup', 'unclear')),
		duplicate_of    BIGINT REFERENCES review.findings(id),
		unique_to_model BOOLEAN,                      -- valid and no other reviewer model raised it
		severity        TEXT NOT NULL DEFAULT '',
		judged_by_model TEXT NOT NULL DEFAULT '',
		judged_by       TEXT NOT NULL DEFAULT '',     -- agent / person
		session_id      TEXT NOT NULL DEFAULT '',     -- the judge's session (analytics sessions.id)
		rationale       TEXT NOT NULL DEFAULT '',
		metadata        JSONB NOT NULL DEFAULT '{}',
		ts              TIMESTAMPTZ NOT NULL DEFAULT now(),
		recorded_by     TEXT NOT NULL DEFAULT current_user,
		recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
	)`,
	`CREATE INDEX IF NOT EXISTS verdicts_finding_idx ON review.verdicts (finding_id, id DESC)`,
	`CREATE OR REPLACE TRIGGER reviews_append_only BEFORE UPDATE OR DELETE ON review.reviews FOR EACH ROW EXECUTE FUNCTION review.append_only()`,
	`CREATE OR REPLACE TRIGGER reviews_no_truncate BEFORE TRUNCATE ON review.reviews FOR EACH STATEMENT EXECUTE FUNCTION review.append_only()`,
	`CREATE OR REPLACE TRIGGER findings_append_only BEFORE UPDATE OR DELETE ON review.findings FOR EACH ROW EXECUTE FUNCTION review.append_only()`,
	`CREATE OR REPLACE TRIGGER findings_no_truncate BEFORE TRUNCATE ON review.findings FOR EACH STATEMENT EXECUTE FUNCTION review.append_only()`,
	`CREATE OR REPLACE TRIGGER verdicts_append_only BEFORE UPDATE OR DELETE ON review.verdicts FOR EACH ROW EXECUTE FUNCTION review.append_only()`,
	`CREATE OR REPLACE TRIGGER verdicts_no_truncate BEFORE TRUNCATE ON review.verdicts FOR EACH STATEMENT EXECUTE FUNCTION review.append_only()`,
	// Views read review tables only (never public ones: public views are dropped and recreated).
	`CREATE OR REPLACE VIEW review.latest_verdicts AS
	SELECT DISTINCT ON (v.finding_id) v.finding_id, v.id AS verdict_id, v.verdict, v.duplicate_of, v.unique_to_model,
		v.severity, v.judged_by_model, v.judged_by, v.rationale, v.ts,
		(SELECT count(*) FROM review.verdicts x WHERE x.finding_id = v.finding_id) AS verdicts
	FROM review.verdicts v ORDER BY v.finding_id, v.id DESC`,
	`CREATE OR REPLACE VIEW review.findings_judged AS
	SELECT f.id AS finding_id, f.review_id, r.model, r.harness, r.repo, r.pr_number, r.commit_sha,
		r.session_id AS review_session_id, f.path, f.line, f.end_line, f.severity, f.category, f.text, f.text_hash,
		f.session_id, f.seq, f.idx, f.github_comment_id, lv.verdict, lv.unique_to_model, lv.judged_by_model,
		lv.judged_by, lv.ts AS judged_at, COALESCE(lv.verdicts, 0) AS verdicts, f.recorded_at
	FROM review.findings f JOIN review.reviews r ON r.id = f.review_id
		LEFT JOIN review.latest_verdicts lv ON lv.finding_id = f.id`,
	`CREATE OR REPLACE VIEW review.model_tally AS
	SELECT r.model, count(DISTINCT r.id) AS reviews, count(f.id) AS findings, count(lv.finding_id) AS judged,
		count(*) FILTER (WHERE lv.verdict = 'valid') AS valid,
		count(*) FILTER (WHERE lv.verdict = 'invalid') AS invalid,
		count(*) FILTER (WHERE lv.verdict = 'dup') AS dup,
		count(*) FILTER (WHERE lv.verdict = 'unclear') AS unclear,
		count(*) FILTER (WHERE lv.verdict = 'valid' AND lv.unique_to_model) AS unique_valid,
		round(count(*) FILTER (WHERE lv.verdict = 'valid')::numeric
			/ NULLIF(count(*) FILTER (WHERE lv.verdict IN ('valid', 'invalid')), 0), 3) AS precision
	FROM review.reviews r LEFT JOIN review.findings f ON f.review_id = r.id
		LEFT JOIN review.latest_verdicts lv ON lv.finding_id = f.id
	GROUP BY r.model`,
}

// reviewGrantSQL gives the read-write role SELECT on everything and INSERT on review.* only, and
// the read-only role SELECT on review.*. Run by the owner of the analytics objects; default
// privileges cover tables and views created later (public views are recreated on every start).
func reviewGrantSQL(database string) (rw, ro []string) {
	rw = []string{
		`GRANT CONNECT ON DATABASE ` + quoteIdent(database) + ` TO ` + reviewRWRole,
		`GRANT USAGE ON SCHEMA public TO ` + reviewRWRole,
		`GRANT SELECT ON ALL TABLES IN SCHEMA public TO ` + reviewRWRole,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO ` + reviewRWRole,
		`GRANT USAGE ON SCHEMA review TO ` + reviewRWRole,
		`GRANT SELECT, INSERT ON ALL TABLES IN SCHEMA review TO ` + reviewRWRole,
		`GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA review TO ` + reviewRWRole,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA review GRANT SELECT, INSERT ON TABLES TO ` + reviewRWRole,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA review GRANT USAGE, SELECT ON SEQUENCES TO ` + reviewRWRole,
	}
	ro = []string{
		`GRANT USAGE ON SCHEMA review TO ` + reviewRORole,
		`GRANT SELECT ON ALL TABLES IN SCHEMA review TO ` + reviewRORole,
		`ALTER DEFAULT PRIVILEGES IN SCHEMA review GRANT SELECT ON TABLES TO ` + reviewRORole,
	}
	return rw, ro
}

// reviewRoleAttrs are applied to agents_rw on creation (like agents: bounded queries and
// connections).
var reviewRoleAttrs = []string{
	`ALTER ROLE ` + reviewRWRole + ` CONNECTION LIMIT 8`,
	`ALTER ROLE ` + reviewRWRole + ` SET statement_timeout = '30s'`,
	`ALTER ROLE ` + reviewRWRole + ` SET idle_in_transaction_session_timeout = '60s'`,
}

// ReviewSetupSQL is what to run (psql, as the analytics owner, `-v pw=...`) when the proxy can't
// create agents_rw itself. The proxy applies the grants on its next start either way.
func ReviewSetupSQL(database string) string {
	rw, _ := reviewGrantSQL(database)
	lines := append([]string{`CREATE ROLE ` + reviewRWRole + ` LOGIN PASSWORD :'pw'`}, reviewRoleAttrs...)
	lines = append(lines, rw...)
	return strings.Join(lines, ";\n") + ";"
}

// reviewState is what ensureReview found; reported on /tmx/review/status.
type reviewState struct {
	mu          sync.Mutex
	SchemaReady bool
	RoleExists  bool
	RoleManaged bool // the proxy created it / set its password from the env
	Grants      bool
	Error       string
	SetupSQL    string // set when the role is missing and the proxy couldn't create it
}

// ensureReview creates the review schema and sets up the roles. Failures are logged and reported
// on the status endpoint; they never take analytics down.
func (an *analytics) ensureReview(ctx context.Context) {
	st := &an.review
	st.mu.Lock()
	defer st.mu.Unlock()
	for _, stmt := range reviewSchemaSQL {
		if _, err := an.db.ExecContext(ctx, stmt); err != nil {
			st.Error = fmt.Sprintf("review schema: %v", err)
			log.WithError(err).Error("tmx analytics: review schema")
			return
		}
	}
	st.SchemaReady = true
	var database string
	var privileged bool
	if err := an.db.QueryRowContext(ctx, `SELECT current_database(), rolsuper OR rolcreaterole FROM pg_roles
		WHERE rolname = current_user`).Scan(&database, &privileged); err != nil {
		st.Error = fmt.Sprintf("review roles: %v", err)
		return
	}
	rw, ro := reviewGrantSQL(database)
	if an.roleExists(ctx, reviewRORole) {
		an.execAll(ctx, st, ro)
	}
	pw := os.Getenv(reviewRWEnv)
	st.RoleExists = an.roleExists(ctx, reviewRWRole)
	if pw != "" && privileged {
		var stmt string
		verb := "ALTER"
		if !st.RoleExists {
			verb = "CREATE"
		}
		// The password is quoted by the server (format %L); never logged here.
		err := an.db.QueryRowContext(ctx, `SELECT format('`+verb+` ROLE `+reviewRWRole+` LOGIN PASSWORD %L', $1::text)`, pw).Scan(&stmt)
		if err == nil {
			_, err = an.db.ExecContext(ctx, stmt)
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "42710" { // created concurrently by another instance
				err = nil
			}
		}
		if err != nil {
			st.Error = fmt.Sprintf("%s role %s: %v", strings.ToLower(verb), reviewRWRole, err)
			log.WithError(err).Warnf("tmx analytics: %s role %s", strings.ToLower(verb), reviewRWRole)
		} else {
			if !st.RoleExists {
				log.Infof("tmx analytics: created role %s", reviewRWRole)
			}
			st.RoleExists, st.RoleManaged = true, true
			an.execAll(ctx, st, reviewRoleAttrs)
		}
	}
	if !st.RoleExists {
		st.SetupSQL = ReviewSetupSQL(database)
		why := reviewRWEnv + " is not set"
		if pw != "" {
			why = "the analytics user can't create roles"
		}
		log.Warnf("tmx analytics: role %s is missing (%s); run as the analytics owner, then restart:\n%s",
			reviewRWRole, why, st.SetupSQL)
		return
	}
	st.SetupSQL = ""
	st.Grants = an.execAll(ctx, st, rw)
}

func (an *analytics) roleExists(ctx context.Context, role string) bool {
	var ok bool
	_ = an.db.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1)`, role).Scan(&ok)
	return ok
}

// execAll runs every statement, recording (not stopping at) failures.
func (an *analytics) execAll(ctx context.Context, st *reviewState, stmts []string) bool {
	ok := true
	for _, stmt := range stmts {
		if _, err := an.db.ExecContext(ctx, stmt); err != nil {
			ok = false
			st.Error = fmt.Sprintf("%s: %v", stmt, err)
			log.WithError(err).Warnf("tmx analytics: %s", stmt)
		}
	}
	return ok
}

// ---- management API ----

// ReviewIngest is one reviewer run, optionally with its findings.
type ReviewIngest struct {
	ExternalID     string          `json:"external_id,omitempty"`
	Model          string          `json:"model"`
	Harness        string          `json:"harness,omitempty"`
	SessionID      string          `json:"session_id,omitempty"`
	ThreadID       string          `json:"thread_id,omitempty"`
	Repo           string          `json:"repo,omitempty"`
	PRNumber       int64           `json:"pr_number,omitempty"`
	CommitSHA      string          `json:"commit_sha,omitempty"`
	GitHubReviewID int64           `json:"github_review_id,omitempty"`
	StartedAt      *time.Time      `json:"started_at,omitempty"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
	Source         string          `json:"source,omitempty"`
	Metadata       json.RawMessage `json:"metadata,omitempty"`
	Findings       []FindingIngest `json:"findings,omitempty"`
}

// FindingIngest is one finding of a review.
type FindingIngest struct {
	ExternalID      string          `json:"external_id,omitempty"`
	Text            string          `json:"text"`
	Path            string          `json:"path,omitempty"`
	Line            *int            `json:"line,omitempty"`
	EndLine         *int            `json:"end_line,omitempty"`
	Severity        string          `json:"severity,omitempty"`
	Category        string          `json:"category,omitempty"`
	SessionID       string          `json:"session_id,omitempty"`
	Seq             *int            `json:"seq,omitempty"`
	Idx             *int            `json:"idx,omitempty"`
	GitHubCommentID int64           `json:"github_comment_id,omitempty"`
	Metadata        json.RawMessage `json:"metadata,omitempty"`
}

// VerdictIngest is one judgment. The finding is named by id, or by the review's and the
// finding's external ids.
type VerdictIngest struct {
	ExternalID        string          `json:"external_id,omitempty"`
	FindingID         int64           `json:"finding_id,omitempty"`
	ReviewExternalID  string          `json:"review_external_id,omitempty"`
	FindingExternalID string          `json:"finding_external_id,omitempty"`
	Verdict           string          `json:"verdict"` // valid | invalid | dup | unclear
	DuplicateOf       int64           `json:"duplicate_of,omitempty"`
	UniqueToModel     *bool           `json:"unique_to_model,omitempty"`
	Severity          string          `json:"severity,omitempty"`
	JudgedByModel     string          `json:"judged_by_model,omitempty"`
	JudgedBy          string          `json:"judged_by,omitempty"`
	SessionID         string          `json:"session_id,omitempty"`
	Rationale         string          `json:"rationale,omitempty"`
	TS                *time.Time      `json:"ts,omitempty"`
	Metadata          json.RawMessage `json:"metadata,omitempty"`
}

// ReviewItemResult is the outcome of one item of a batch: created | exists (already recorded under
// its external_id; nothing written) | error (nothing written; Retryable = a server-side failure,
// send it again).
type ReviewItemResult struct {
	Index      int                `json:"index"`
	Status     string             `json:"status"`
	ID         int64              `json:"id,omitempty"`
	ExternalID string             `json:"external_id,omitempty"`
	Error      string             `json:"error,omitempty"`
	Retryable  bool               `json:"retryable,omitempty"`
	Findings   []ReviewItemResult `json:"findings,omitempty"`
}

// ReviewBatchResult counts a batch's item results.
type ReviewBatchResult struct {
	Created int                `json:"created"`
	Exists  int                `json:"exists"`
	Errors  int                `json:"errors"`
	Results []ReviewItemResult `json:"results"`
}

func (r *ReviewBatchResult) add(it ReviewItemResult) {
	switch it.Status {
	case "created":
		r.Created++
	case "exists":
		r.Exists++
	default:
		r.Errors++
	}
	r.Results = append(r.Results, it)
}

// ErrBatchTooLarge is returned for a batch over the item limit.
var ErrBatchTooLarge = fmt.Errorf("at most %d items per batch", maxReviewItem)

const reviewRecorder = "api"

func (a *Archive) reviewDB() (*sql.DB, error) {
	if a.analytics == nil {
		return nil, ErrAnalyticsOff
	}
	a.analytics.review.mu.Lock()
	ready := a.analytics.review.SchemaReady
	a.analytics.review.mu.Unlock()
	if !ready {
		return nil, fmt.Errorf("review schema is not ready (see /tmx/review/status)")
	}
	return a.analytics.db, nil
}

func validJSONOpt(r json.RawMessage) bool { return len(r) == 0 || json.Valid(r) }

// RecordReviews stores reviewer runs with their findings, one transaction per review. A review
// whose external_id is already recorded is left alone ("exists", with its id); its findings are
// matched by their external ids (new findings with new external ids are added).
func (a *Archive) RecordReviews(ctx context.Context, in []ReviewIngest) (*ReviewBatchResult, error) {
	if len(in) > maxReviewItem {
		return nil, ErrBatchTooLarge
	}
	db, err := a.reviewDB()
	if err != nil {
		return nil, err
	}
	res := &ReviewBatchResult{Results: []ReviewItemResult{}}
	for i, r := range in {
		res.add(recordReview(ctx, db, i, r))
	}
	return res, nil
}

func reviewErr(i int, ext string, err error, retryable bool) ReviewItemResult {
	return ReviewItemResult{Index: i, Status: "error", ExternalID: ext, Error: err.Error(), Retryable: retryable}
}

func recordReview(ctx context.Context, db *sql.DB, i int, r ReviewIngest) ReviewItemResult {
	r.ExternalID, r.Model = strings.TrimSpace(r.ExternalID), strings.TrimSpace(r.Model)
	switch {
	case r.Model == "":
		return reviewErr(i, r.ExternalID, errors.New("model is required"), false)
	case !validJSONOpt(r.Metadata):
		return reviewErr(i, r.ExternalID, errors.New("metadata is not valid JSON"), false)
	case r.CommitSHA != "" && !validSHA(strings.ToLower(r.CommitSHA)):
		return reviewErr(i, r.ExternalID, errors.New("commit_sha must be 7-64 hex characters"), false)
	case r.PRNumber < 0:
		return reviewErr(i, r.ExternalID, errors.New("pr_number can't be negative"), false)
	}
	for j, f := range r.Findings {
		if err := checkFinding(f); err != nil {
			return reviewErr(i, r.ExternalID, fmt.Errorf("finding %d: %w", j, err), false)
		}
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return reviewErr(i, r.ExternalID, err, true)
	}
	defer func() { _ = tx.Rollback() }()
	out := ReviewItemResult{Index: i, ExternalID: r.ExternalID, Status: "created"}
	err = tx.QueryRowContext(ctx, `INSERT INTO review.reviews (external_id, model, harness, session_id, thread_id,
		repo, pr_number, commit_sha, github_review_id, started_at, finished_at, source, metadata, recorded_by)
		VALUES (NULLIF($1, ''), $2, $3, $4, $5, $6, NULLIF($7, 0), $8, NULLIF($9, 0), $10, $11, $12, $13, $14)
		ON CONFLICT (external_id) DO NOTHING RETURNING id`,
		r.ExternalID, cleanText(r.Model), cleanText(r.Harness), cleanText(r.SessionID), cleanText(r.ThreadID),
		cleanText(r.Repo), r.PRNumber, strings.ToLower(r.CommitSHA), r.GitHubReviewID, timePtr(r.StartedAt),
		timePtr(r.FinishedAt), cleanText(r.Source), jsonOrEmpty(r.Metadata), reviewRecorder).Scan(&out.ID)
	if errors.Is(err, sql.ErrNoRows) {
		out.Status = "exists"
		err = tx.QueryRowContext(ctx, `SELECT id FROM review.reviews WHERE external_id = $1`, r.ExternalID).Scan(&out.ID)
	}
	if err != nil {
		return reviewErr(i, r.ExternalID, err, true)
	}
	for j, f := range r.Findings {
		fr := ReviewItemResult{Index: j, ExternalID: strings.TrimSpace(f.ExternalID), Status: "created"}
		err = tx.QueryRowContext(ctx, `INSERT INTO review.findings (review_id, external_id, text, path, line, end_line,
			severity, category, session_id, seq, idx, github_comment_id, metadata, recorded_by)
			VALUES ($1, NULLIF($2, ''), $3, $4, $5, $6, $7, $8, $9, $10, $11, NULLIF($12, 0), $13, $14)
			ON CONFLICT (review_id, external_id) DO NOTHING RETURNING id`,
			out.ID, fr.ExternalID, cleanText(f.Text), cleanText(f.Path), f.Line, f.EndLine, cleanText(f.Severity),
			cleanText(f.Category), cleanText(f.SessionID), f.Seq, f.Idx, f.GitHubCommentID, jsonOrEmpty(f.Metadata),
			reviewRecorder).Scan(&fr.ID)
		if errors.Is(err, sql.ErrNoRows) {
			fr.Status = "exists"
			err = tx.QueryRowContext(ctx, `SELECT id FROM review.findings WHERE review_id = $1 AND external_id = $2`,
				out.ID, fr.ExternalID).Scan(&fr.ID)
		}
		if err != nil {
			return reviewErr(i, r.ExternalID, fmt.Errorf("finding %d: %w", j, err), true)
		}
		out.Findings = append(out.Findings, fr)
	}
	if err = tx.Commit(); err != nil {
		return reviewErr(i, r.ExternalID, err, true)
	}
	return out
}

func checkFinding(f FindingIngest) error {
	switch {
	case strings.TrimSpace(f.Text) == "":
		return errors.New("text is required")
	case !validJSONOpt(f.Metadata):
		return errors.New("metadata is not valid JSON")
	case (f.Seq != nil || f.Idx != nil) && strings.TrimSpace(f.SessionID) == "":
		return errors.New("seq / idx need session_id")
	}
	return nil
}

var verdictValues = map[string]bool{"valid": true, "invalid": true, "dup": true, "unclear": true}

// RecordVerdicts appends verdicts, one transaction each. A verdict whose external_id is already
// recorded is left alone ("exists"); without an external_id every call appends.
func (a *Archive) RecordVerdicts(ctx context.Context, in []VerdictIngest) (*ReviewBatchResult, error) {
	if len(in) > maxReviewItem {
		return nil, ErrBatchTooLarge
	}
	db, err := a.reviewDB()
	if err != nil {
		return nil, err
	}
	res := &ReviewBatchResult{Results: []ReviewItemResult{}}
	for i, v := range in {
		res.add(recordVerdict(ctx, db, i, v))
	}
	return res, nil
}

func recordVerdict(ctx context.Context, db *sql.DB, i int, v VerdictIngest) ReviewItemResult {
	v.ExternalID = strings.TrimSpace(v.ExternalID)
	v.Verdict = strings.ToLower(strings.TrimSpace(v.Verdict))
	switch {
	case !verdictValues[v.Verdict]:
		return reviewErr(i, v.ExternalID, errors.New("verdict must be valid, invalid, dup or unclear"), false)
	case !validJSONOpt(v.Metadata):
		return reviewErr(i, v.ExternalID, errors.New("metadata is not valid JSON"), false)
	case v.FindingID == 0 && (v.ReviewExternalID == "" || v.FindingExternalID == ""):
		return reviewErr(i, v.ExternalID, errors.New("finding_id (or review_external_id + finding_external_id) is required"), false)
	}
	findingID := v.FindingID
	if findingID == 0 {
		err := db.QueryRowContext(ctx, `SELECT f.id FROM review.findings f JOIN review.reviews r ON r.id = f.review_id
			WHERE r.external_id = $1 AND f.external_id = $2`, v.ReviewExternalID, v.FindingExternalID).Scan(&findingID)
		if errors.Is(err, sql.ErrNoRows) {
			return reviewErr(i, v.ExternalID, errors.New("finding not found"), false)
		}
		if err != nil {
			return reviewErr(i, v.ExternalID, err, true)
		}
	}
	out := ReviewItemResult{Index: i, ExternalID: v.ExternalID, Status: "created"}
	err := db.QueryRowContext(ctx, `INSERT INTO review.verdicts (external_id, finding_id, verdict, duplicate_of,
		unique_to_model, severity, judged_by_model, judged_by, session_id, rationale, metadata, ts, recorded_by)
		VALUES (NULLIF($1, ''), $2, $3, NULLIF($4, 0), $5, $6, $7, $8, $9, $10, $11, COALESCE($12, now()), $13)
		ON CONFLICT (external_id) DO NOTHING RETURNING id`,
		v.ExternalID, findingID, v.Verdict, v.DuplicateOf, v.UniqueToModel, cleanText(v.Severity),
		cleanText(v.JudgedByModel), cleanText(v.JudgedBy), cleanText(v.SessionID), cleanText(v.Rationale),
		jsonOrEmpty(v.Metadata), timePtr(v.TS), reviewRecorder).Scan(&out.ID)
	if errors.Is(err, sql.ErrNoRows) {
		out.Status = "exists"
		err = db.QueryRowContext(ctx, `SELECT id FROM review.verdicts WHERE external_id = $1`, v.ExternalID).Scan(&out.ID)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" { // foreign key: unknown finding / duplicate_of
		return reviewErr(i, v.ExternalID, errors.New("finding or duplicate_of not found"), false)
	}
	if err != nil {
		return reviewErr(i, v.ExternalID, err, true)
	}
	return out
}

// TallyQuery filters ReviewTally.
type TallyQuery struct {
	Model, Harness, Repo string
	Since                time.Time // reviews recorded (or started) since
}

// TallyRow is one reviewer model's record, by latest verdict.
type TallyRow struct {
	Model       string   `json:"model"`
	Reviews     int64    `json:"reviews"`
	Findings    int64    `json:"findings"`
	Judged      int64    `json:"judged"`
	Valid       int64    `json:"valid"`
	Invalid     int64    `json:"invalid"`
	Dup         int64    `json:"dup"`
	Unclear     int64    `json:"unclear"`
	UniqueValid int64    `json:"unique_valid"`
	Precision   *float64 `json:"precision"` // valid / (valid + invalid); null when none
}

// ReviewTally is review.model_tally with filters.
func (a *Archive) ReviewTally(ctx context.Context, q TallyQuery) ([]TallyRow, error) {
	db, err := a.reviewDB()
	if err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT r.model, count(DISTINCT r.id), count(f.id), count(lv.finding_id),
			count(*) FILTER (WHERE lv.verdict = 'valid'), count(*) FILTER (WHERE lv.verdict = 'invalid'),
			count(*) FILTER (WHERE lv.verdict = 'dup'), count(*) FILTER (WHERE lv.verdict = 'unclear'),
			count(*) FILTER (WHERE lv.verdict = 'valid' AND lv.unique_to_model),
			round(count(*) FILTER (WHERE lv.verdict = 'valid')::numeric
				/ NULLIF(count(*) FILTER (WHERE lv.verdict IN ('valid', 'invalid')), 0), 3)::float8
		FROM review.reviews r LEFT JOIN review.findings f ON f.review_id = r.id
			LEFT JOIN review.latest_verdicts lv ON lv.finding_id = f.id
		WHERE ($1 = '' OR r.model = $1) AND ($2 = '' OR r.harness = $2) AND ($3 = '' OR lower(r.repo) = lower($3))
			AND COALESCE(r.started_at, r.recorded_at) >= $4
		GROUP BY r.model ORDER BY count(*) FILTER (WHERE lv.verdict = 'valid') DESC, r.model`,
		q.Model, q.Harness, q.Repo, q.Since)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []TallyRow{}
	for rows.Next() {
		var t TallyRow
		if err = rows.Scan(&t.Model, &t.Reviews, &t.Findings, &t.Judged, &t.Valid, &t.Invalid, &t.Dup, &t.Unclear,
			&t.UniqueValid, &t.Precision); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// FindingQuery filters ReviewFindings.
type FindingQuery struct {
	ReviewID    int64
	Model, Repo string
	PRNumber    int64
	Verdict     string // a latest verdict, or "none" for unjudged findings
	Limit       int
	AfterID     int64 // the last finding_id of the previous page
}

// ReviewFindings lists findings with their review and latest verdict (review.findings_judged),
// oldest first, a page at a time: pass the last finding_id back as AfterID.
func (a *Archive) ReviewFindings(ctx context.Context, q FindingQuery) ([]json.RawMessage, error) {
	db, err := a.reviewDB()
	if err != nil {
		return nil, err
	}
	if q.Limit <= 0 || q.Limit > 1000 {
		q.Limit = 100
	}
	rows, err := db.QueryContext(ctx, `SELECT to_jsonb(j) FROM review.findings_judged j
		WHERE ($1 = 0 OR review_id = $1) AND ($2 = '' OR model = $2) AND ($3 = '' OR lower(repo) = lower($3))
			AND ($4 = 0 OR pr_number = $4)
			AND ($5 = '' OR ($5 = 'none' AND verdict IS NULL) OR verdict = $5) AND finding_id > $6
		ORDER BY finding_id LIMIT $7`, q.ReviewID, q.Model, q.Repo, q.PRNumber, q.Verdict, q.AfterID, q.Limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []json.RawMessage{}
	for rows.Next() {
		var doc []byte
		if err = rows.Scan(&doc); err != nil {
			return nil, err
		}
		out = append(out, doc)
	}
	return out, rows.Err()
}

// ReviewStatus reports the review store's setup and row counts.
func (a *Archive) ReviewStatus(ctx context.Context) (map[string]any, error) {
	an := a.analytics
	if an == nil {
		return nil, ErrAnalyticsOff
	}
	st := &an.review
	st.mu.Lock()
	out := map[string]any{"schema": reviewSchema, "role": reviewRWRole, "schema_ready": st.SchemaReady,
		"role_exists": st.RoleExists, "role_managed": st.RoleManaged, "grants_applied": st.Grants}
	if st.Error != "" {
		out["error"] = st.Error
	}
	if st.SetupSQL != "" {
		out["setup_sql"] = st.SetupSQL
	}
	ready := st.SchemaReady
	st.mu.Unlock()
	if ready {
		counts := map[string]int64{}
		for _, t := range []string{"reviews", "findings", "verdicts"} {
			var n int64
			if err := an.db.QueryRowContext(ctx, `SELECT count(*) FROM review.`+t).Scan(&n); err == nil {
				counts[t] = n
			}
		}
		out["counts"] = counts
	}
	return out, nil
}
