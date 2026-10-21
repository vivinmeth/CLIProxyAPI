package archive

// Analytics is the derived, rebuildable query layer (tokenmaxx fork): every archived session —
// imported transcripts and proxied traffic — extracted into a separate Postgres
// (TMX_ANALYTICS_DSN) as sessions / messages / items, plus mirrors of tmx.requests and
// tmx_archive.exchanges. R2 + the archive index stay the source of truth; nothing here is
// needed to rebuild them, and everything here can be rebuilt from them (POST
// /tmx/analytics/backfill).
//
// Changes reach it through tmx_archive.analytics_journal, written in the same transaction as the
// archive change (session ingest / replace / delete, exchange capture). The worker reads the
// journal, looks at the current archive state for each entry and rewrites the session from its
// first changed message, so replays are harmless and live proxied sessions only append.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// trgmIndexSQL: substring search over every item, plus a small one over prose (kind = 'text':
// user/assistant/system text) — tool outputs are most of the bytes and make prose searches with
// common trigrams recheck far more rows.
var trgmIndexSQL = []string{
	`CREATE INDEX IF NOT EXISTS items_text_trgm_idx ON items USING gin (text gin_trgm_ops)`,
	`CREATE INDEX IF NOT EXISTS items_prose_trgm_idx ON items USING gin (text gin_trgm_ops) WHERE kind = 'text'`,
}

const (
	// analyticsVersion is bumped when extraction changes; sessions extracted by an older version
	// are rewritten on the next backfill.
	analyticsVersion  = 2
	journalTable      = schema + ".analytics_journal"
	journalBatch      = 1000
	sessionBatch      = 100 // imported sessions per step (see step)
	analyticsPoll     = 3 * time.Second
	prefetchChunkMsgs = 5000
	maxAttempts       = 3
	insertParamLimit  = 60000
	// writeSession inserts once this much message/item content (or this many messages) is produced.
	writeChunkBytes = 16 << 20
	writeChunkMsgs  = 500
)

type analytics struct {
	a  *Archive
	db *sql.DB

	reqMirror  *mirror // tmx.requests -> requests (nil when the request log is off)
	exchMirror *mirror // tmx_archive.exchanges -> exchanges

	wake chan struct{}
	stop chan struct{}
	done chan struct{}

	processed   atomic.Int64
	linksDirty  atomic.Bool // request_sessions needs a refresh
	linksAt     time.Time   // last refresh (run loop only)
	extracted   atomic.Int64
	failures    atomic.Int64
	backfilling atomic.Bool
	idxMu       sync.Mutex // trigram index state + DDL (backfill vs deferred build)
	ctx         context.Context
	cancel      context.CancelFunc
	lastErr     atomic.Value // string
	lastRunUnix atomic.Int64
	mu          sync.Mutex // serialises step() and backfill bookkeeping
}

// startAnalytics connects to TMX_ANALYTICS_DSN when set. Failures disable analytics, never the
// archive.
func (a *Archive) startAnalytics(ctx context.Context) {
	dsn := strings.TrimSpace(os.Getenv("TMX_ANALYTICS_DSN"))
	if dsn == "" {
		// Nothing reads the journal: stop the usage-row triggers filling it.
		if rl := requestlog.Default(); rl != nil {
			dropRequestTriggers(ctx, rl.DB(), rl.Schema()+".requests")
		}
		return
	}
	an, err := openAnalytics(ctx, a, dsn)
	if err != nil {
		log.WithError(err).Error("tmx analytics: disabled")
		return
	}
	a.analytics = an
	go an.run()
	log.Info("tmx analytics: on")
}

func openAnalytics(ctx context.Context, a *Archive, dsn string) (*analytics, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err = db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	an := &analytics{a: a, db: db, wake: make(chan struct{}, 1), stop: make(chan struct{}),
		done: make(chan struct{})}
	an.ctx, an.cancel = context.WithCancel(context.Background())
	if err = an.ensureSchema(pingCtx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return an, nil
}

func (an *analytics) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE EXTENSION IF NOT EXISTS pg_trgm`,
		`CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS sessions (
			id           TEXT PRIMARY KEY,           -- imported: <harness>:<external_id>; proxied: proxy:<client session id>
			kind         TEXT NOT NULL,              -- imported | proxy
			harness      TEXT NOT NULL DEFAULT '',
			external_id  TEXT NOT NULL DEFAULT '',   -- the client's own session id
			alt_ids      TEXT[] NOT NULL DEFAULT '{}', -- proxy: canonical session ids (tmx.requests.session_id)
			machine      TEXT NOT NULL DEFAULT '',
			project      TEXT NOT NULL DEFAULT '',
			cwd          TEXT NOT NULL DEFAULT '',
			title        TEXT NOT NULL DEFAULT '',
			via_proxy    BOOLEAN,
			shadowed_by  TEXT,                       -- proxy session also imported: the imported session id
			started_at   TIMESTAMPTZ,
			ended_at     TIMESTAMPTZ,
			model        TEXT NOT NULL DEFAULT '',
			messages     INTEGER NOT NULL DEFAULT 0,
			items        INTEGER NOT NULL DEFAULT 0,
			exchanges    INTEGER NOT NULL DEFAULT 0,
			trace_id     TEXT NOT NULL DEFAULT '',   -- proxy: the exchange the messages come from
			metadata     JSONB NOT NULL DEFAULT '{}',
			version      INTEGER NOT NULL,
			extracted_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS sessions_started_idx ON sessions (started_at DESC)`,
		`CREATE INDEX IF NOT EXISTS sessions_harness_idx ON sessions (harness, machine)`,
		`CREATE INDEX IF NOT EXISTS sessions_external_idx ON sessions (external_id)`,
		`CREATE INDEX IF NOT EXISTS sessions_alt_idx ON sessions USING gin (alt_ids)`,
		`CREATE TABLE IF NOT EXISTS messages (
			session_id  TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
			seq         INTEGER NOT NULL,            -- position in the session, 0-based
			source_seq  BIGINT NOT NULL DEFAULT 0,   -- imported: the harness's seq
			external_id TEXT NOT NULL DEFAULT '',
			parent_id   TEXT NOT NULL DEFAULT '',
			ts          TIMESTAMPTZ,
			role        TEXT NOT NULL DEFAULT '',
			model       TEXT NOT NULL DEFAULT '',
			message     JSONB,                       -- as stored, media replaced by "tmx-media:<sha256>"
			usage       JSONB,
			metadata    JSONB NOT NULL DEFAULT '{}',
			blob        TEXT NOT NULL DEFAULT '',    -- archive blob (manifest / element / response) for R2 lookups
			digest      TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (session_id, seq)
		)`,
		`CREATE TABLE IF NOT EXISTS items (
			session_id TEXT NOT NULL,
			seq        INTEGER NOT NULL,
			idx        INTEGER NOT NULL,
			role       TEXT NOT NULL DEFAULT '',     -- system | user | assistant | tool | event
			kind       TEXT NOT NULL DEFAULT '',     -- text | thinking | tool_call | tool_result | image | other
			name       TEXT NOT NULL DEFAULT '',     -- tool name
			call_id    TEXT NOT NULL DEFAULT '',
			is_error   BOOLEAN NOT NULL DEFAULT FALSE,
			text       TEXT NOT NULL DEFAULT '',     -- capped at 64 KiB per item
			data       JSONB,                        -- untruncated structure: tool_call arguments, raw "other" block
			ts         TIMESTAMPTZ,
			model      TEXT NOT NULL DEFAULT '',
			PRIMARY KEY (session_id, seq, idx),
			FOREIGN KEY (session_id, seq) REFERENCES messages (session_id, seq) ON DELETE CASCADE
		)`,
		`ALTER TABLE items ADD COLUMN IF NOT EXISTS data JSONB`,
		`CREATE INDEX IF NOT EXISTS messages_external_idx ON messages (session_id, external_id)`,
		`CREATE INDEX IF NOT EXISTS items_kind_name_idx ON items (kind, name)`,
		`DROP INDEX IF EXISTS items_call_idx`,
		// Joins a tool_call to its tool_result within the session.
		`CREATE INDEX IF NOT EXISTS items_session_call_idx ON items (session_id, call_id) WHERE call_id <> ''`,
		`CREATE INDEX IF NOT EXISTS items_ts_idx ON items (ts)`,
		`CREATE TABLE IF NOT EXISTS failed (
			journal_id BIGINT PRIMARY KEY,
			kind       TEXT NOT NULL,
			ref        TEXT NOT NULL,
			error      TEXT NOT NULL,
			failed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
	}
	for _, stmt := range stmts {
		if _, err := an.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("analytics schema: %w", err)
		}
	}
	// The trigram index is dropped while a backfill runs (maintaining it per insert is most of
	// the cost) and rebuilt once the journal is drained; see buildDeferredIndexes.
	if pending, err := an.getState(ctx, "trgm_index"); err != nil {
		return err
	} else if pending != "pending" {
		for _, stmt := range trgmIndexSQL {
			if _, err = an.db.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("analytics schema: %w", err)
			}
		}
	}
	// Old versions read the journal with a cursor (and could skip late commits); the entries they
	// left behind are simply replayed — extraction is idempotent.
	if _, err := an.db.ExecContext(ctx, `DELETE FROM state WHERE key IN ('journal_id', 'requests_id', 'requests_recon_id')`); err != nil {
		return err
	}
	var err error
	if an.exchMirror, err = ensureMirror(ctx, an.a.db, an.db, schema, "exchanges", "exchanges", "trace_id",
		[]string{"ts", "session_id", "client_session_id"}); err != nil {
		return err
	}
	if rl := requestlog.Default(); rl != nil {
		if an.reqMirror, err = ensureMirror(ctx, rl.DB(), an.db, rl.Schema(), "requests", "requests", "id",
			[]string{"ts", "session_id", "trace_id", "import_ref", "source, ts"}); err != nil {
			return err
		}
		if err = installRequestTriggers(ctx, rl.DB(), rl.Schema()+".requests"); err != nil {
			log.WithError(err).Warn("tmx analytics: tmx.requests is not in the archive database; usage rows are not mirrored")
			an.reqMirror = nil
		}
	}
	return an.ensureViews(ctx)
}

// ensureViews (re)creates the views over requests, in one transaction so readers never see them
// missing.
func (an *analytics) ensureViews(ctx context.Context) error {
	tx, err := an.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Dependents first: thread_usage reads session_usage.
	stmts := append([]string{`DROP VIEW IF EXISTS thread_usage, skill_invocations, session_threads, session_tree`},
		threadSchemaSQL...)
	if an.reqMirror != nil {
		stmts = append(stmts, analyticsViewsSQL...)
	}
	stmts = append(stmts, threadViewsSQL...)
	if an.reqMirror != nil {
		stmts = append(stmts, threadUsageSQL...)
	}
	for _, stmt := range stmts {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("analytics views: %w", err)
		}
	}
	return tx.Commit()
}

// journal records an archive change for the analytics worker. ex is the caller's transaction
// (or the archive DB), so the entry commits together with the change.
func (a *Archive) journal(ctx context.Context, ex interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, kind, ref string) error {
	if a.analytics == nil {
		return nil
	}
	_, err := ex.ExecContext(ctx, `INSERT INTO `+journalTable+` (kind, ref) VALUES ($1, $2)`, kind, ref)
	if err == nil {
		a.analytics.poke()
	}
	return err
}

func (an *analytics) poke() {
	select {
	case an.wake <- struct{}{}:
	default:
	}
}

func (an *analytics) close() {
	an.cancel() // in-flight work is abandoned; the journal keeps it for the next start
	close(an.stop)
	<-an.done
	_ = an.db.Close()
}

func (an *analytics) run() {
	defer close(an.done)
	tick := time.NewTicker(analyticsPoll)
	defer tick.Stop()
	for {
		select {
		case <-an.stop:
			return
		case <-tick.C:
		case <-an.wake:
			// Let a burst of journal entries (one per streamed turn) land before reading them.
			time.Sleep(500 * time.Millisecond)
		}
		for {
			more, err := an.step()
			if err != nil {
				an.setErr(err)
				break
			}
			if !more {
				an.buildDeferredIndexes()
				an.refreshLinks(false)
				break
			}
			select {
			case <-an.stop:
				return
			default:
			}
		}
	}
}

func (an *analytics) setErr(err error) {
	an.lastErr.Store(err.Error())
	if an.failures.Add(1)%20 == 1 {
		log.WithError(err).Warn("tmx analytics: step failed")
	}
}

func (an *analytics) getState(ctx context.Context, key string) (string, error) {
	var v string
	err := an.db.QueryRowContext(ctx, `SELECT value FROM state WHERE key = $1`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (an *analytics) setState(ctx context.Context, key, value string) error {
	_, err := an.db.ExecContext(ctx, `INSERT INTO state (key, value) VALUES ($1, $2)
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`, key, value)
	return err
}

func (an *analytics) stateInt(ctx context.Context, key string) (int64, error) {
	v, err := an.getState(ctx, key)
	if err != nil || v == "" {
		return 0, err
	}
	var n int64
	_, err = fmt.Sscan(v, &n)
	return n, err
}

type journalEntry struct {
	id        int64
	kind, ref string
	attempts  int
}

// step processes one journal batch plus new tmx.requests rows. more = there is probably more.
func (an *analytics) step() (bool, error) {
	an.mu.Lock()
	defer an.mu.Unlock()
	ctx, cancel := context.WithTimeout(an.ctx, 30*time.Minute)
	defer cancel()
	an.lastRunUnix.Store(time.Now().Unix())

	// The journal is a queue: processed entries are deleted. (A cursor over the BIGSERIAL id
	// would skip entries whose transaction commits after a later id's.) Failed entries back off
	// and don't hold up the rest. Cheap live entries (usage rows, exchanges) go first; imported
	// sessions, which can take seconds each, come at most sessionBatch per step, so a bulk
	// import or backfill never holds live usage back for long.
	rows, err := an.a.db.QueryContext(ctx, `(SELECT id, kind, ref, attempts FROM `+journalTable+`
			WHERE kind <> 'session' AND (not_before IS NULL OR not_before <= now()) ORDER BY id LIMIT $1)
		UNION ALL
		(SELECT id, kind, ref, attempts FROM `+journalTable+`
			WHERE kind = 'session' AND (not_before IS NULL OR not_before <= now()) ORDER BY id LIMIT $2)`,
		journalBatch, sessionBatch)
	if err != nil {
		return false, err
	}
	var entries []journalEntry
	for rows.Next() {
		var e journalEntry
		if err = rows.Scan(&e.id, &e.kind, &e.ref, &e.attempts); err != nil {
			_ = rows.Close()
			return false, err
		}
		entries = append(entries, e)
	}
	if err = closeRows(rows); err != nil {
		return false, err
	}
	if len(entries) == 0 {
		return false, nil
	}
	nSessions := 0
	for _, e := range entries {
		if e.kind == "session" {
			nSessions++
		}
	}
	more := len(entries)-nSessions == journalBatch || nSessions == sessionBatch

	// Collapse the batch: work is state-based, so each session is processed once.
	var traces, requestIDs []string
	requestsTruncated := false
	imported := map[string]bool{}
	proxy := map[string]bool{}
	threads := map[string]bool{}
	byRef := map[string][]journalEntry{}
	for _, e := range entries {
		switch e.kind {
		case "exchange":
			traces = append(traces, e.ref)
		case "session":
			imported[e.ref] = true
		case "proxy":
			proxy[e.ref] = true
		case "request":
			requestIDs = append(requestIDs, e.ref)
		case "requests_truncate":
			requestsTruncated = true
		case "thread":
			threads[e.ref] = true
		}
		byRef[e.kind+"\x00"+e.ref] = append(byRef[e.kind+"\x00"+e.ref], e)
	}
	failedIDs := map[int64]error{}
	fail := func(kind, ref string, err error) {
		for _, e := range byRef[kind+"\x00"+ref] {
			failedIDs[e.id] = err
		}
	}
	if requestsTruncated {
		if _, errT := an.reconcileAllRequests(ctx); errT != nil {
			fail("requests_truncate", "*", errT)
		}
	}
	if len(requestIDs) > 0 {
		if errR := an.syncRequestRows(ctx, requestIDs); errR != nil {
			for _, id := range requestIDs {
				fail("request", id, errR)
			}
		}
	}
	if len(traces) > 0 {
		if _, errM := an.exchMirror.copy(ctx, `trace_id = ANY($1::text[])`, pgArray(traces)); errM != nil {
			for _, t := range traces {
				fail("exchange", t, errM)
			}
		} else {
			keys, errK := an.proxyKeysForTraces(ctx, traces)
			if errK != nil {
				return false, errK
			}
			for _, k := range keys {
				proxy[k] = true
			}
		}
	}
	for id := range threads {
		if errT := an.extractThread(ctx, id); errT != nil {
			fail("thread", id, errT)
		}
	}
	importedIDs := make([]string, 0, len(imported))
	for id := range imported {
		importedIDs = append(importedIDs, id)
	}
	sort.Strings(importedIDs)
	for id, errS := range an.extractImportedBatch(ctx, importedIDs) {
		fail("session", id, errS)
	}
	for key := range proxy {
		if errP := an.extractProxy(ctx, key); errP != nil {
			fail("proxy", key, errP)
			// An exchange entry produced this key; retry those too.
			for _, t := range traces {
				fail("exchange", t, errP)
			}
		} else {
			an.extracted.Add(1)
		}
	}

	// Done entries are deleted; failed ones retry after 1, 2, ... minutes and after maxAttempts go
	// to the failed table (a backfill reruns them).
	var done []string
	cleared := map[string][]string{} // kind -> refs that went through: their old failures are moot
	for _, e := range entries {
		errE, bad := failedIDs[e.id]
		if !bad {
			done = append(done, fmt.Sprint(e.id))
			if e.kind == "session" || e.kind == "proxy" || e.kind == "thread" {
				cleared[e.kind] = append(cleared[e.kind], e.ref)
			}
			if e.kind != "thread" {
				an.linksDirty.Store(true)
			}
			continue
		}
		an.setErr(fmt.Errorf("%s %s: %w", e.kind, e.ref, errE))
		if e.attempts+1 >= maxAttempts {
			// Acknowledge only once the failure is recorded; otherwise keep retrying.
			if _, errF := an.db.ExecContext(ctx, `INSERT INTO failed (journal_id, kind, ref, error) VALUES ($1,$2,$3,$4)
				ON CONFLICT (journal_id) DO UPDATE SET error = EXCLUDED.error, failed_at = now()`,
				e.id, e.kind, e.ref, cleanText(errE.Error())); errF == nil {
				done = append(done, fmt.Sprint(e.id))
				log.WithError(errE).Warnf("tmx analytics: giving up on %s %s", e.kind, e.ref)
				continue
			}
		}
		if _, err = an.a.db.ExecContext(ctx, `UPDATE `+journalTable+` SET attempts = attempts + 1,
			not_before = now() + make_interval(mins => attempts + 1) WHERE id = $1`, e.id); err != nil {
			return false, err
		}
	}
	if len(done) > 0 {
		if _, err = an.a.db.ExecContext(ctx, `DELETE FROM `+journalTable+` WHERE id = ANY($1::bigint[])`,
			"{"+strings.Join(done, ",")+"}"); err != nil {
			return false, err
		}
		an.processed.Add(int64(len(done)))
	}
	for kind, refs := range cleared {
		_, _ = an.db.ExecContext(ctx, `DELETE FROM failed WHERE kind = $1 AND ref = ANY($2::text[])`, kind, pgTextArray(refs))
	}
	return more, nil
}

// buildDeferredIndexes rebuilds the trigram index after a backfill has drained the journal.
func (an *analytics) buildDeferredIndexes() {
	an.idxMu.Lock()
	defer an.idxMu.Unlock()
	ctx, cancel := context.WithTimeout(an.ctx, 2*time.Hour)
	defer cancel()
	if v, err := an.getState(ctx, "trgm_index"); err != nil || v != "pending" || an.backfilling.Load() {
		return
	}
	var backlog int64
	if an.a.db.QueryRowContext(ctx, `SELECT count(*) FROM `+journalTable+` WHERE attempts = 0`).Scan(&backlog) != nil || backlog > 0 {
		return
	}
	conn, err := an.db.Conn(ctx)
	if err != nil {
		an.setErr(err)
		return
	}
	defer func() { _ = conn.Close() }()
	start := time.Now()
	_, _ = conn.ExecContext(ctx, `SET maintenance_work_mem = '512MB'`)
	for _, stmt := range trgmIndexSQL {
		if _, err = conn.ExecContext(ctx, stmt); err != nil {
			an.setErr(fmt.Errorf("trigram index: %w", err))
			return
		}
	}
	_ = an.setState(ctx, "trgm_index", "built")
	log.Infof("tmx analytics: trigram index built in %s", time.Since(start).Round(time.Second))
}

// installRequestTriggers journals every inserted, updated or deleted tmx.requests row (statement
// triggers with transition tables), in the writer's own transaction. Needs the journal in the same
// database as tmx.requests (prod: one DB).
func installRequestTriggers(ctx context.Context, db *sql.DB, table string) error {
	var ok bool
	if err := db.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, journalTable).Scan(&ok); err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("%s not found next to %s", journalTable, table)
	}
	stmts := []string{
		`CREATE OR REPLACE FUNCTION ` + schema + `.analytics_journal_requests() RETURNS trigger LANGUAGE plpgsql AS $$
				BEGIN
			-- Bulk updates (the request log's pricing pass) journal one full reconcile, not a row
			-- each: hundreds of thousands of entries would hold live usage back.
						-- One entry per statement (never skipped because one is pending: a reconcile already
			-- running would miss this statement's rows); a step collapses them into one reconcile.
			IF TG_OP = 'TRUNCATE' OR (TG_OP = 'UPDATE' AND current_setting('tmx.analytics_bulk', true) = 'on') THEN
				INSERT INTO ` + journalTable + ` (kind, ref) VALUES ('requests_truncate', '*');
			ELSIF TG_OP = 'DELETE' THEN
				INSERT INTO ` + journalTable + ` (kind, ref) SELECT 'request', id::text FROM old_rows;
			ELSE
				INSERT INTO ` + journalTable + ` (kind, ref) SELECT 'request', id::text FROM new_rows;
			END IF;
			RETURN NULL;
		END $$`,
		`CREATE OR REPLACE TRIGGER analytics_journal_ins AFTER INSERT ON ` + table +
			` REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION ` + schema + `.analytics_journal_requests()`,
		`CREATE OR REPLACE TRIGGER analytics_journal_upd AFTER UPDATE ON ` + table +
			` REFERENCING NEW TABLE AS new_rows FOR EACH STATEMENT EXECUTE FUNCTION ` + schema + `.analytics_journal_requests()`,
		`CREATE OR REPLACE TRIGGER analytics_journal_del AFTER DELETE ON ` + table +
			` REFERENCING OLD TABLE AS old_rows FOR EACH STATEMENT EXECUTE FUNCTION ` + schema + `.analytics_journal_requests()`,
		`CREATE OR REPLACE TRIGGER analytics_journal_trunc AFTER TRUNCATE ON ` + table +
			` FOR EACH STATEMENT EXECUTE FUNCTION ` + schema + `.analytics_journal_requests()`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return nil
}

// dropRequestTriggers stops journaling tmx.requests (analytics turned off).
func dropRequestTriggers(ctx context.Context, db *sql.DB, table string) {
	for _, t := range []string{"analytics_journal_ins", "analytics_journal_upd", "analytics_journal_del", "analytics_journal_trunc"} {
		_, _ = db.ExecContext(ctx, `DROP TRIGGER IF EXISTS `+t+` ON `+table)
	}
}

// syncRequestRows copies the given tmx.requests rows; ids no longer in the source are deleted.
func (an *analytics) syncRequestRows(ctx context.Context, ids []string) error {
	if an.reqMirror == nil {
		return nil
	}
	arr := "{" + strings.Join(ids, ",") + "}"
	if _, err := an.reqMirror.copy(ctx, `id = ANY($1::bigint[])`, arr); err != nil {
		return err
	}
	present := map[string]bool{}
	rows, err := an.reqMirror.src.QueryContext(ctx, `SELECT id::text FROM `+an.reqMirror.srcTable+` WHERE id = ANY($1::bigint[])`, arr)
	if err != nil {
		return err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			_ = rows.Close()
			return err
		}
		present[id] = true
	}
	if err = closeRows(rows); err != nil {
		return err
	}
	var gone []string
	for _, id := range ids {
		if !present[id] {
			gone = append(gone, id)
		}
	}
	if len(gone) > 0 {
		_, err = an.db.ExecContext(ctx, `DELETE FROM requests WHERE id = ANY($1::bigint[])`, "{"+strings.Join(gone, ",")+"}")
	}
	return err
}

// reconcileAllRequests makes the requests copy equal to tmx.requests: upserts everything and
// deletes copies whose source row is gone (backfill).
func (an *analytics) reconcileAllRequests(ctx context.Context) (int, error) {
	if an.reqMirror == nil {
		return 0, nil
	}
	n, err := an.reqMirror.copy(ctx, `TRUE`)
	if err != nil {
		return n, err
	}
	src := map[int64]bool{}
	rows, err := an.reqMirror.src.QueryContext(ctx, `SELECT id FROM `+an.reqMirror.srcTable)
	if err != nil {
		return n, err
	}
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			_ = rows.Close()
			return n, err
		}
		src[id] = true
	}
	if err = closeRows(rows); err != nil {
		return n, err
	}
	rows, err = an.db.QueryContext(ctx, `SELECT id FROM requests`)
	if err != nil {
		return n, err
	}
	var gone []string
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			_ = rows.Close()
			return n, err
		}
		if !src[id] {
			gone = append(gone, fmt.Sprint(id))
		}
	}
	if err = closeRows(rows); err != nil {
		return n, err
	}
	for start := 0; start < len(gone); start += 5000 {
		end := min(start+5000, len(gone))
		if _, err = an.db.ExecContext(ctx, `DELETE FROM requests WHERE id = ANY($1::bigint[])`,
			"{"+strings.Join(gone[start:end], ",")+"}"); err != nil {
			return n, err
		}
	}
	return n, nil
}

// proxyKeysForTraces maps captured exchanges to their proxy session keys.
func (an *analytics) proxyKeysForTraces(ctx context.Context, traces []string) ([]string, error) {
	rows, err := an.a.db.QueryContext(ctx, `SELECT DISTINCT CASE WHEN client_session_id <> '' THEN client_session_id
		ELSE session_id END FROM `+schema+`.exchanges WHERE trace_id = ANY($1::text[]) AND method = 'POST'`, pgArray(traces))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var keys []string
	for rows.Next() {
		var k string
		if err = rows.Scan(&k); err != nil {
			return nil, err
		}
		if k != "" {
			keys = append(keys, k)
		}
	}
	return keys, rows.Err()
}

// ---- extraction ----

// msgRow is one message as it should be stored.
type msgRow struct {
	sourceSeq            int64
	externalID, parentID string
	ts                   *time.Time
	role, model          string
	usage, metadata      []byte
	blob                 string
	digest               string
	// produce returns the stored message JSON and its items; only called for rows being written.
	produce func() (json.RawMessage, []ConvItem, error)
}

type sessionRow struct {
	id, kind, harness, externalID string
	altIDs                        []string
	machine, project, cwd, title  string
	viaProxy                      *bool
	shadowedBy                    *string
	startedAt, endedAt            *time.Time
	model                         string
	exchanges                     int
	traceID                       string
	metadata                      []byte
}

// writeSession stores the session row and rewrites messages from the first one whose digest
// changed (all of them when the session was extracted by an older version). All in one
// transaction; messages are produced and inserted a chunk at a time so a huge session never sits
// in memory whole.
func (an *analytics) writeSession(ctx context.Context, s sessionRow, msgs []msgRow) error {
	existing := map[int]string{}
	var storedVersion sql.NullInt64
	if err := an.db.QueryRowContext(ctx, `SELECT version FROM sessions WHERE id = $1`, s.id).Scan(&storedVersion); err != nil &&
		!errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if !storedVersion.Valid || storedVersion.Int64 == analyticsVersion {
		rows, err := an.db.QueryContext(ctx, `SELECT seq, digest FROM messages WHERE session_id = $1`, s.id)
		if err != nil {
			return err
		}
		for rows.Next() {
			var seq int
			var d string
			if err = rows.Scan(&seq, &d); err != nil {
				_ = rows.Close()
				return err
			}
			existing[seq] = d
		}
		if err = closeRows(rows); err != nil {
			return err
		}
	}
	first := 0
	for first < len(msgs) && existing[first] == msgs[first].digest && existing[first] != "" {
		first++
	}
	sameVersion := !storedVersion.Valid || storedVersion.Int64 == analyticsVersion
	unchanged := sameVersion && first == len(msgs) && len(existing) == len(msgs)

	tx, err := an.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	alt := s.altIDs
	if alt == nil {
		alt = []string{}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO sessions (id, kind, harness, external_id, alt_ids, machine,
		project, cwd, title, via_proxy, shadowed_by, started_at, ended_at, model, messages, exchanges, trace_id,
		metadata, version, extracted_at)
		VALUES ($1,$2,$3,$4,$5::text[],$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19, now())
		ON CONFLICT (id) DO UPDATE SET kind = EXCLUDED.kind, harness = EXCLUDED.harness,
			external_id = EXCLUDED.external_id, alt_ids = EXCLUDED.alt_ids, machine = EXCLUDED.machine,
			project = EXCLUDED.project, cwd = EXCLUDED.cwd, title = EXCLUDED.title, via_proxy = EXCLUDED.via_proxy,
			shadowed_by = EXCLUDED.shadowed_by, started_at = EXCLUDED.started_at, ended_at = EXCLUDED.ended_at,
			model = EXCLUDED.model, messages = EXCLUDED.messages, exchanges = EXCLUDED.exchanges,
			trace_id = EXCLUDED.trace_id, metadata = EXCLUDED.metadata, version = EXCLUDED.version,
			extracted_at = now()`,
		s.id, s.kind, s.harness, s.externalID, pgArray(alt), cleanText(s.machine), cleanText(s.project),
		cleanText(s.cwd), cleanText(s.title), s.viaProxy, s.shadowedBy, s.startedAt, s.endedAt, s.model,
		len(msgs), s.exchanges, s.traceID, jsonOrEmpty(s.metadata), analyticsVersion); err != nil {
		return fmt.Errorf("session row: %w", err)
	}
	if !unchanged {
		if _, err = tx.ExecContext(ctx, `DELETE FROM messages WHERE session_id = $1 AND seq >= $2`, s.id, first); err != nil {
			return err
		}
		var mrows, irows [][]any
		pending := 0
		flush := func() error {
			if err := insertRows(ctx, tx, "messages", []string{"session_id", "seq", "source_seq", "external_id",
				"parent_id", "ts", "role", "model", "message", "usage", "metadata", "blob", "digest"}, mrows, ""); err != nil {
				return fmt.Errorf("messages: %w", err)
			}
			if err := insertRows(ctx, tx, "items", []string{"session_id", "seq", "idx", "role", "kind", "name", "call_id",
				"is_error", "text", "data", "ts", "model"}, irows, ""); err != nil {
				return fmt.Errorf("items: %w", err)
			}
			mrows, irows, pending = nil, nil, 0
			return nil
		}
		for seq := first; seq < len(msgs); seq++ {
			m := msgs[seq]
			msg, items, errP := m.produce()
			if errP != nil {
				return errP
			}
			mrows = append(mrows, []any{s.id, seq, m.sourceSeq, cleanText(m.externalID), cleanText(m.parentID), m.ts,
				m.role, m.model, nullableJSON(msg), nullableJSON(m.usage), jsonOrEmpty(m.metadata), m.blob, m.digest})
			pending += len(msg)
			for j, it := range items {
				ts := it.TS
				if ts == nil {
					ts = m.ts
				}
				irows = append(irows, []any{s.id, seq, j, it.Role, it.Kind, cleanText(it.Name), cleanText(it.CallID),
					it.IsError, cleanText(it.Text), nullableJSON([]byte(it.Data)), ts, it.Model})
				pending += len(it.Text) + len(it.Data)
			}
			if pending >= writeChunkBytes || len(mrows) >= writeChunkMsgs {
				if err = flush(); err != nil {
					return err
				}
			}
		}
		if err = flush(); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE sessions SET items = (SELECT count(*) FROM items WHERE session_id = $1)
		WHERE id = $1`, s.id); err != nil {
		return err
	}
	return tx.Commit()
}

func (an *analytics) deleteSession(ctx context.Context, id string) error {
	_, err := an.db.ExecContext(ctx, `DELETE FROM sessions WHERE id = $1`, id)
	return err
}

// extractImportedBatch extracts imported sessions, warming the read cache per chunk of sessions
// so blobs come out of R2 a pack span at a time. Returns the sessions that failed.
func (an *analytics) extractImportedBatch(ctx context.Context, ids []string) map[string]error {
	failed := map[string]error{}
	for start := 0; start < len(ids); {
		var manifests []string
		end := start
		for end < len(ids) && (end == start || len(manifests) < prefetchChunkMsgs) {
			hs, err := an.messageHashes(ctx, ids[end])
			if err != nil {
				failed[ids[end]] = err
			}
			manifests = append(manifests, hs...)
			end++
		}
		an.a.prefetchBodiesOpt(ctx, manifests, false)
		for _, id := range ids[start:end] {
			if failed[id] != nil {
				continue
			}
			if err := an.extractImported(ctx, id); err != nil {
				failed[id] = err
			} else {
				an.extracted.Add(1)
			}
		}
		start = end
	}
	return failed
}

func (an *analytics) messageHashes(ctx context.Context, id string) ([]string, error) {
	rows, err := an.a.db.QueryContext(ctx, `SELECT message FROM `+schema+`.session_messages WHERE session_id = $1`, id)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var h string
		if err = rows.Scan(&h); err != nil {
			return nil, err
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (an *analytics) extractImported(ctx context.Context, id string) error {
	s := sessionRow{id: id, kind: "imported"}
	var started, ended sql.NullTime
	var via sql.NullBool
	err := an.a.db.QueryRowContext(ctx, `SELECT harness, external_id, machine, project, cwd, title, via_proxy,
		started_at, ended_at, metadata FROM `+schema+`.sessions WHERE id = $1`, id).Scan(&s.harness, &s.externalID,
		&s.machine, &s.project, &s.cwd, &s.title, &via, &started, &ended, &s.metadata)
	if errors.Is(err, sql.ErrNoRows) {
		// Deleted: drop it, un-shadow its proxied twin, drop its usage copies.
		var ext string
		_ = an.db.QueryRowContext(ctx, `SELECT external_id FROM sessions WHERE id = $1`, id).Scan(&ext)
		if err = an.deleteSession(ctx, id); err != nil {
			return err
		}
		if ext != "" {
			_, _ = an.db.ExecContext(ctx, `UPDATE sessions SET shadowed_by = NULL WHERE id = $1 AND shadowed_by = $2`,
				"proxy:"+ext, id)
		}
		return nil
	}
	if err != nil {
		return err
	}
	if via.Valid {
		s.viaProxy = &via.Bool
	}
	s.startedAt, s.endedAt = nullTimePtr(started), nullTimePtr(ended)

	rows, err := an.a.db.QueryContext(ctx, `SELECT external_id, seq, parent_id, ts, role, model, message, usage,
		metadata FROM `+schema+`.session_messages WHERE session_id = $1 ORDER BY seq, ts, external_id`, id)
	if err != nil {
		return err
	}
	var msgs []msgRow
	for rows.Next() {
		var m msgRow
		var ts sql.NullTime
		var usage []byte
		if err = rows.Scan(&m.externalID, &m.sourceSeq, &m.parentID, &ts, &m.role, &m.model, &m.blob, &usage,
			&m.metadata); err != nil {
			_ = rows.Close()
			return err
		}
		m.ts, m.usage = nullTimePtr(ts), usage
		m.digest = digestOf(m.blob, string(usage), string(m.metadata), m.externalID, m.parentID, m.role, m.model,
			fmt.Sprint(m.sourceSeq), timeKey(m.ts))
		blob, role, model, ts2 := m.blob, m.role, m.model, m.ts
		m.produce = func() (json.RawMessage, []ConvItem, error) {
			body, errB := an.a.rebuildBodyNoMedia(ctx, blob)
			if errB != nil {
				return nil, nil, errB
			}
			msg := gjson.GetBytes(body, "message")
			items := normalizeMessage(msg, role)
			for i := range items {
				items[i].TS = ts2
				if model != "" && items[i].Role == "assistant" {
					items[i].Model = model
				}
			}
			return json.RawMessage(msg.Raw), items, nil
		}
		if m.model != "" {
			s.model = m.model
		}
		msgs = append(msgs, m)
	}
	if err = closeRows(rows); err != nil {
		return err
	}
	if err = an.writeSession(ctx, s, msgs); err != nil {
		return err
	}
	// Its proxied twin (same client session id) is shadowed by this transcript.
	_, err = an.db.ExecContext(ctx, `UPDATE sessions SET shadowed_by = $1 WHERE id = $2`, id, "proxy:"+s.externalID)
	return err
}

type proxyExchange struct {
	traceID          string
	ts               time.Time
	finished         sql.NullTime
	status           int
	requestBytes     int64
	reqBody, resBody string
	sessionID        string
}

// extractProxy rebuilds a proxied session (key = the client's session id, or the canonical one)
// from the exchange with the largest successful request plus its response — the same choice
// GET /tmx/archive/conversation makes.
func (an *analytics) extractProxy(ctx context.Context, key string) error {
	id := "proxy:" + key
	rows, err := an.a.db.QueryContext(ctx, `SELECT trace_id, ts, finished_at, status, request_bytes, request_body,
		response_body, session_id, client_session_id FROM `+schema+`.exchanges
		WHERE (client_session_id = $1 OR session_id = $1) AND method = 'POST' ORDER BY ts`, key)
	if err != nil {
		return err
	}
	var exch []proxyExchange
	alt := map[string]bool{}
	for rows.Next() {
		var e proxyExchange
		var client string
		if err = rows.Scan(&e.traceID, &e.ts, &e.finished, &e.status, &e.requestBytes, &e.reqBody, &e.resBody,
			&e.sessionID, &client); err != nil {
			_ = rows.Close()
			return err
		}
		// The session_id match only counts for exchanges without a client id of their own.
		if client != "" && client != key {
			continue
		}
		exch = append(exch, e)
		if e.sessionID != "" && e.sessionID != key {
			alt[e.sessionID] = true
		}
	}
	if err = closeRows(rows); err != nil {
		return err
	}
	if len(exch) == 0 {
		return an.deleteSession(ctx, id)
	}
	s := sessionRow{id: id, kind: "proxy", externalID: key, exchanges: len(exch)}
	for k := range alt {
		s.altIDs = append(s.altIDs, k)
	}
	sort.Strings(s.altIDs)
	first, last := exch[0].ts, exch[len(exch)-1].ts
	if exch[len(exch)-1].finished.Valid {
		last = exch[len(exch)-1].finished.Time
	}
	s.startedAt, s.endedAt = &first, &last
	var shadow string
	if errS := an.a.db.QueryRowContext(ctx, `SELECT id FROM `+schema+`.sessions WHERE external_id = $1
		ORDER BY updated_at DESC LIMIT 1`, key).Scan(&shadow); errS == nil {
		s.shadowedBy = &shadow
	}

	best := -1
	for i, e := range exch {
		if e.status >= 200 && e.status < 300 && (best < 0 || e.requestBytes >= exch[best].requestBytes) {
			best = i
		}
	}
	var msgs []msgRow
	if best >= 0 {
		b := exch[best]
		s.traceID = b.traceID
		if rl := requestlog.Default(); rl != nil {
			_ = rl.DB().QueryRowContext(ctx, `SELECT harness, machine FROM `+rl.Schema()+`.requests
				WHERE trace_id = $1 ORDER BY id LIMIT 1`, b.traceID).Scan(&s.harness, &s.machine)
		}
		an.a.prefetchBodiesOpt(ctx, []string{b.reqBody}, false)
		an.a.blobs.Prefetch(ctx, []string{b.resBody})
		if msgs, s.model, err = an.proxyMessages(ctx, b); err != nil {
			return err
		}
		for _, m := range msgs {
			if m.role == "user" && s.title == "" {
				if _, items, errP := m.produce(); errP == nil {
					for _, it := range items {
						if it.Kind == "text" && strings.TrimSpace(it.Text) != "" {
							s.title = truncateRunes(strings.TrimSpace(it.Text), 120)
							break
						}
					}
				}
			}
		}
	}
	return an.writeSession(ctx, s, msgs)
}

// proxyMessages splits the request into one message per system block / conversation element and
// appends the response as the final assistant message.
func (an *analytics) proxyMessages(ctx context.Context, e proxyExchange) ([]msgRow, string, error) {
	var msgs []msgRow
	ts := e.ts
	model := ""
	if e.reqBody != "" {
		body, err := an.a.rebuildBodyNoMedia(ctx, e.reqBody)
		if err != nil {
			return nil, "", err
		}
		elementBlobs := map[string][]string{}
		if raw, errM := an.a.blobs.Get(ctx, e.reqBody); errM == nil {
			var mf Manifest
			if json.Unmarshal(raw, &mf) == nil {
				for _, k := range mf.Keys {
					if k.Array {
						elementBlobs[k.Key] = k.Items
					}
				}
			}
		}
		root := gjson.ParseBytes(body)
		model = root.Get("model").String()
		add := func(role, blob string, raw gjson.Result, conv func() []ConvItem) {
			rawCopy := raw.Raw
			msgs = append(msgs, msgRow{role: role, model: model, ts: &ts, blob: blob,
				digest:  digestOf(blob, HashOf([]byte(rawCopy)), role),
				produce: func() (json.RawMessage, []ConvItem, error) { return json.RawMessage(rawCopy), conv(), nil }})
		}
		if sys := root.Get("system"); sys.Exists() {
			add("system", "", sys, func() []ConvItem { return contentItems(sys, "system") })
		}
		if ins := root.Get("instructions"); ins.Type == gjson.String && ins.String() != "" {
			text := ins.String()
			add("system", "", ins, func() []ConvItem { return []ConvItem{item("system", "text", text, "")} })
		}
		for _, key := range []string{"messages", "input", "contents"} {
			v := root.Get(key)
			if v.Type == gjson.String {
				text := v.String()
				add("user", "", v, func() []ConvItem { return []ConvItem{item("user", "text", text, "")} })
				continue
			}
			blobs := elementBlobs[key]
			i := 0
			v.ForEach(func(_, m gjson.Result) bool {
				blob := ""
				if i < len(blobs) {
					blob = blobs[i]
				}
				i++
				mm := m
				role := firstNonEmptyStr(m.Get("role").String(), "event")
				if role == "model" {
					role = "assistant"
				}
				add(role, blob, mm, func() []ConvItem { return normalizeMessage(mm, "") })
				return true
			})
		}
	}
	if e.resBody != "" {
		resp, err := an.a.blobs.Get(ctx, e.resBody)
		if err != nil {
			return nil, "", err
		}
		done := e.ts
		if e.finished.Valid {
			done = e.finished.Time
		}
		items := responseItems(resp)
		for i := range items {
			items[i].Model = model
		}
		msgs = append(msgs, msgRow{role: "assistant", model: model, ts: &done, blob: e.resBody,
			digest: digestOf(e.resBody, e.traceID, "response"),
			produce: func() (json.RawMessage, []ConvItem, error) {
				out, _ := json.Marshal(map[string]any{"role": "assistant", "response_of": e.traceID})
				return out, items, nil
			}})
	}
	return msgs, model, nil
}

// ---- backfill & status ----

// BackfillResult reports what a backfill queued.
type BackfillResult struct {
	Sessions    int64 `json:"sessions"`
	Proxy       int64 `json:"proxy_sessions"`
	Threads     int64 `json:"threads"`
	Exchanges   int64 `json:"exchanges"`
	RequestRows int   `json:"request_rows"` // tmx.requests rows copied (copies of deleted rows are removed)
	Replayed    int   `json:"replayed"`     // failed entries + sessions deleted from the archive
}

// importedGhosts lists imported sessions in analytics that the archive no longer has.
func (an *analytics) importedGhosts(ctx context.Context) ([]string, error) {
	have := map[string]bool{}
	rows, err := an.a.db.QueryContext(ctx, `SELECT id FROM `+schema+`.sessions`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			_ = rows.Close()
			return nil, err
		}
		have[id] = true
	}
	if err = closeRows(rows); err != nil {
		return nil, err
	}
	rows, err = an.db.QueryContext(ctx, `SELECT id FROM sessions WHERE kind = 'imported'`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		if !have[id] {
			out = append(out, id)
		}
	}
	return out, rows.Err()
}

// ErrAnalyticsOff is returned when TMX_ANALYTICS_DSN is not configured.
var ErrAnalyticsOff = errors.New("analytics is off (TMX_ANALYTICS_DSN)")

// AnalyticsBackfill queues every archived session (imported and proxied) for extraction, copies
// every captured exchange and restarts the tmx.requests copy from the beginning. Idempotent.
func (a *Archive) AnalyticsBackfill(ctx context.Context) (*BackfillResult, error) {
	an := a.analytics
	if an == nil {
		return nil, ErrAnalyticsOff
	}
	an.backfilling.Store(true)
	defer an.backfilling.Store(false)
	res := &BackfillResult{}
	an.idxMu.Lock()
	err := an.setState(ctx, "trgm_index", "pending")
	if err == nil {
		_, err = an.db.ExecContext(ctx, `DROP INDEX IF EXISTS items_text_trgm_idx, items_prose_trgm_idx`)
	}
	an.idxMu.Unlock()
	if err != nil {
		return nil, err
	}
	// Replay what failed before, and drop imported sessions the archive no longer has (a delete
	// whose journal entry failed): extractImported deletes them.
	var replay [][2]string
	var replayedFailed []string
	rows, err := an.db.QueryContext(ctx, `SELECT journal_id, kind, ref FROM failed`)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var jid int64
		var k, r string
		if err = rows.Scan(&jid, &k, &r); err != nil {
			_ = rows.Close()
			return nil, err
		}
		replay = append(replay, [2]string{k, r})
		replayedFailed = append(replayedFailed, fmt.Sprint(jid))
	}
	if err = closeRows(rows); err != nil {
		return nil, err
	}
	ghosts, err := an.importedGhosts(ctx)
	if err != nil {
		return nil, err
	}
	for _, g := range ghosts {
		replay = append(replay, [2]string{"session", g})
	}
	threadGhosts, err := an.threadGhosts(ctx)
	if err != nil {
		return nil, err
	}
	for _, g := range threadGhosts {
		replay = append(replay, [2]string{"thread", g})
	}
	for _, r := range replay {
		if _, err = a.db.ExecContext(ctx, `INSERT INTO `+journalTable+` (kind, ref) VALUES ($1, $2)`, r[0], r[1]); err != nil {
			return nil, err
		}
	}
	res.Replayed = len(replay)
	n, errX := an.exchMirror.copy(ctx, `TRUE`)
	if errX != nil {
		return nil, fmt.Errorf("exchanges: %w", errX)
	}
	res.Exchanges = int64(n)
	tag, err := a.db.ExecContext(ctx, `INSERT INTO `+journalTable+` (kind, ref)
		SELECT 'session', id FROM `+schema+`.sessions ORDER BY started_at NULLS LAST, id`)
	if err != nil {
		return nil, err
	}
	res.Sessions, _ = tag.RowsAffected()
	tag, err = a.db.ExecContext(ctx, `INSERT INTO `+journalTable+` (kind, ref)
		SELECT DISTINCT 'proxy', CASE WHEN client_session_id <> '' THEN client_session_id ELSE session_id END
		FROM `+schema+`.exchanges WHERE method = 'POST' AND (client_session_id <> '' OR session_id <> '')`)
	if err != nil {
		return nil, err
	}
	res.Proxy, _ = tag.RowsAffected()
	tag, err = a.db.ExecContext(ctx, `INSERT INTO `+journalTable+` (kind, ref) SELECT 'thread', id FROM `+schema+`.threads`)
	if err != nil {
		return nil, err
	}
	res.Threads, _ = tag.RowsAffected()
	// Under the worker lock: a row mirrored by step() during the scan must not be judged absent.
	an.mu.Lock()
	res.RequestRows, err = an.reconcileAllRequests(ctx)
	an.mu.Unlock()
	if err != nil {
		return nil, fmt.Errorf("requests: %w", err)
	}
	if len(replayedFailed) > 0 {
		_, _ = an.db.ExecContext(ctx, `DELETE FROM failed WHERE journal_id = ANY($1::bigint[])`,
			"{"+strings.Join(replayedFailed, ",")+"}")
	}
	an.poke()
	return res, nil
}

// AnalyticsStatus reports progress and counts.
func (a *Archive) AnalyticsStatus(ctx context.Context) (map[string]any, error) {
	an := a.analytics
	if an == nil {
		return nil, ErrAnalyticsOff
	}
	var backlog, retrying int64
	_ = a.db.QueryRowContext(ctx, `SELECT count(*), count(*) FILTER (WHERE attempts > 0) FROM `+journalTable).Scan(&backlog, &retrying)
	counts := map[string]int64{}
	for _, t := range []string{"sessions", "messages", "items", "exchanges", "requests", "failed"} {
		if t == "requests" && an.reqMirror == nil {
			continue
		}
		var n int64
		// Estimates for the big tables keep this cheap; exact counts for the small ones.
		q := `SELECT count(*) FROM ` + t
		if t == "items" || t == "messages" || t == "requests" {
			q = `SELECT GREATEST(reltuples, 0)::bigint FROM pg_class WHERE relname = '` + t + `'`
		}
		if err := an.db.QueryRowContext(ctx, q).Scan(&n); err == nil {
			counts[t] = n
		}
	}
	byKind := map[string]int64{}
	if rows, err := an.db.QueryContext(ctx, `SELECT kind, count(*) FROM sessions GROUP BY kind`); err == nil {
		for rows.Next() {
			var k string
			var n int64
			if rows.Scan(&k, &n) == nil {
				byKind[k] = n
			}
		}
		_ = rows.Close()
	}
	lastErr, _ := an.lastErr.Load().(string)
	trgm, _ := an.getState(ctx, "trgm_index")
	if trgm == "" {
		trgm = "built"
	}
	return map[string]any{
		"enabled":          true,
		"version":          analyticsVersion,
		"journal_backlog":  backlog,
		"journal_retrying": retrying,
		"counts":           counts,
		"sessions_by_kind": byKind,
		"processed":        an.processed.Load(),
		"extracted":        an.extracted.Load(),
		"failures":         an.failures.Load(),
		"last_error":       lastErr,
		"last_run":         time.Unix(an.lastRunUnix.Load(), 0).UTC(),
		"backfilling":      an.backfilling.Load(),
		"trgm_index":       trgm,
	}, nil
}

// ---- mirrors ----

// mirror copies a source table into the analytics DB with the same columns (picked up from the
// catalog, so new columns follow automatically) and upserts by key.
type mirror struct {
	src, dst *sql.DB
	srcTable string // schema-qualified
	dstTable string
	key      string
	cols     []string
}

func ensureMirror(ctx context.Context, src, dst *sql.DB, srcSchema, srcName, dstTable, key string, indexes []string) (*mirror, error) {
	rows, err := src.QueryContext(ctx, `SELECT a.attname, format_type(a.atttypid, a.atttypmod)
		FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2 AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum`,
		srcSchema, srcName)
	if err != nil {
		return nil, err
	}
	type col struct{ name, typ string }
	var cols []col
	for rows.Next() {
		var c col
		if err = rows.Scan(&c.name, &c.typ); err != nil {
			_ = rows.Close()
			return nil, err
		}
		cols = append(cols, c)
	}
	if err = closeRows(rows); err != nil {
		return nil, err
	}
	m := &mirror{src: src, dst: dst, srcTable: srcSchema + "." + srcName, dstTable: dstTable, key: key}
	var keyType string
	for _, c := range cols {
		m.cols = append(m.cols, c.name)
		if c.name == key {
			keyType = c.typ
		}
	}
	if keyType == "" {
		return nil, fmt.Errorf("mirror %s: no column %s", m.srcTable, key)
	}
	if _, err = dst.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+dstTable+` (`+quoteIdent(key)+` `+keyType+` PRIMARY KEY)`); err != nil {
		return nil, err
	}
	for _, c := range cols {
		if c.name == key {
			continue
		}
		if _, err = dst.ExecContext(ctx, `ALTER TABLE `+dstTable+` ADD COLUMN IF NOT EXISTS `+quoteIdent(c.name)+` `+c.typ); err != nil {
			return nil, err
		}
	}
	for _, ix := range indexes {
		name := dstTable + "_" + strings.NewReplacer(", ", "_", " ", "_").Replace(ix) + "_idx"
		if _, err = dst.ExecContext(ctx, `CREATE INDEX IF NOT EXISTS `+name+` ON `+dstTable+` (`+ix+`)`); err != nil {
			return nil, err
		}
	}
	return m, nil
}

// copy upserts the source rows matching where. Returns how many rows were copied.
func (m *mirror) copy(ctx context.Context, where string, args ...any) (int, error) {
	quoted := make([]string, len(m.cols))
	for i, c := range m.cols {
		quoted[i] = quoteIdent(c)
	}
	rows, err := m.src.QueryContext(ctx, `SELECT `+strings.Join(quoted, ", ")+` FROM `+m.srcTable+` WHERE `+where, args...)
	if err != nil {
		return 0, err
	}
	defer func() { _ = rows.Close() }()
	set := make([]string, 0, len(m.cols))
	for _, c := range m.cols {
		if c != m.key {
			set = append(set, quoteIdent(c)+" = EXCLUDED."+quoteIdent(c))
		}
	}
	conflict := ` ON CONFLICT (` + quoteIdent(m.key) + `) DO UPDATE SET ` + strings.Join(set, ", ")
	var batch [][]any
	total := 0
	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		if errI := insertRows(ctx, m.dst, m.dstTable, quoted, batch, conflict); errI != nil {
			return errI
		}
		total += len(batch)
		batch = batch[:0]
		return nil
	}
	for rows.Next() {
		vals := make([]any, len(m.cols))
		ptrs := make([]any, len(m.cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err = rows.Scan(ptrs...); err != nil {
			return total, err
		}
		for i, v := range vals {
			if s, ok := v.(string); ok {
				vals[i] = cleanText(s)
			}
		}
		batch = append(batch, vals)
		if len(batch) >= 500 {
			if err = flush(); err != nil {
				return total, err
			}
		}
	}
	if err = rows.Err(); err != nil {
		return total, err
	}
	return total, flush()
}

// insertRows inserts rows in multi-row statements under Postgres' parameter limit.
func insertRows(ctx context.Context, ex interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}, table string, cols []string, rows [][]any, suffix string) error {
	if len(rows) == 0 {
		return nil
	}
	per := insertParamLimit / len(cols)
	for start := 0; start < len(rows); start += per {
		end := min(start+per, len(rows))
		var b strings.Builder
		b.WriteString("INSERT INTO " + table + " (" + strings.Join(cols, ", ") + ") VALUES ")
		args := make([]any, 0, (end-start)*len(cols))
		for i, r := range rows[start:end] {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteByte('(')
			for j := range cols {
				if j > 0 {
					b.WriteByte(',')
				}
				fmt.Fprintf(&b, "$%d", len(args)+1)
				args = append(args, r[j])
			}
			b.WriteByte(')')
		}
		b.WriteString(suffix)
		if _, err := ex.ExecContext(ctx, b.String(), args...); err != nil {
			return err
		}
	}
	return nil
}

// ---- helpers ----

// closeRows reports an iteration error (a stream cut short) before closing: a partial read must
// never pass for a complete one.
func closeRows(r *sql.Rows) error {
	if err := r.Err(); err != nil {
		_ = r.Close()
		return err
	}
	return r.Close()
}

func quoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }

func digestOf(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

func timeKey(t *time.Time) string {
	if t == nil {
		return ""
	}
	return t.UTC().Format(time.RFC3339Nano)
}

func nullTimePtr(t sql.NullTime) *time.Time {
	if !t.Valid {
		return nil
	}
	v := t.Time.UTC()
	return &v
}

// cleanText makes a string storable in Postgres TEXT: valid UTF-8, no NUL bytes.
func cleanText(s string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "�")
	}
	if strings.IndexByte(s, 0) >= 0 {
		s = strings.ReplaceAll(s, "\x00", "")
	}
	return s
}

// cleanJSON makes raw JSON storable as jsonb (which rejects \u0000 and invalid UTF-8). Invalid
// JSON is stored as a JSON string.
func cleanJSON(raw []byte) []byte {
	b := sanitizeJSONEscapes([]byte(cleanText(string(raw))))
	if !json.Valid(b) {
		out, _ := json.Marshal(string(b))
		return out
	}
	return b
}

// sanitizeJSONEscapes rewrites the \u escapes jsonb rejects — \u0000 and unpaired surrogates —
// to �. It walks the document token by token, so an escaped backslash followed by "u0000"
// (literal text) is left alone, and structure and numbers are untouched.
func sanitizeJSONEscapes(b []byte) []byte {
	if !bytes.Contains(b, []byte(`\u`)) {
		return b
	}
	hex4 := func(i int) (rune, bool) {
		if i+6 > len(b) || b[i] != '\\' || b[i+1] != 'u' {
			return 0, false
		}
		var r rune
		for _, c := range b[i+2 : i+6] {
			switch {
			case c >= '0' && c <= '9':
				r = r<<4 | rune(c-'0')
			case c >= 'a' && c <= 'f':
				r = r<<4 | rune(c-'a'+10)
			case c >= 'A' && c <= 'F':
				r = r<<4 | rune(c-'A'+10)
			default:
				return 0, false
			}
		}
		return r, true
	}
	const repl = `�`
	out := make([]byte, 0, len(b))
	inString := false
	for i := 0; i < len(b); i++ {
		c := b[i]
		if !inString {
			if c == '"' {
				inString = true
			}
			out = append(out, c)
			continue
		}
		switch c {
		case '"':
			inString = false
			out = append(out, c)
		case '\\':
			r, ok := hex4(i)
			if !ok {
				out = append(out, c)
				if i+1 < len(b) {
					out = append(out, b[i+1])
					i++
				}
				continue
			}
			switch {
			case r == 0, r >= 0xDC00 && r <= 0xDFFF:
				out = append(out, repl...)
				i += 5
			case r >= 0xD800 && r <= 0xDBFF:
				if lo, ok2 := hex4(i + 6); ok2 && lo >= 0xDC00 && lo <= 0xDFFF {
					out = append(out, b[i:i+12]...)
					i += 11
				} else {
					out = append(out, repl...)
					i += 5
				}
			default:
				out = append(out, b[i:i+6]...)
				i += 5
			}
		default:
			out = append(out, c)
		}
	}
	return out
}

func nullableJSON(raw []byte) any {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	return string(cleanJSON(raw))
}

func jsonOrEmpty(raw []byte) string {
	if len(raw) == 0 {
		return "{}"
	}
	return string(cleanJSON(raw))
}

func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
