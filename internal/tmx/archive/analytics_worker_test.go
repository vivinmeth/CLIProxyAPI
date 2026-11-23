package archive

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	log "github.com/sirupsen/logrus"
)

func workerSQL(t *testing.T, db *sql.DB, query string, args ...any) {
	t.Helper()
	if _, err := db.Exec(query, args...); err != nil {
		t.Fatal(err)
	}
}

func workerCount(t *testing.T, an *analytics, query string, args ...any) int {
	t.Helper()
	db := an.db
	if strings.Contains(query, journalTable) {
		db = an.a.db
	}
	var n int
	if err := db.QueryRow(query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func queueWork(t *testing.T, an *analytics, kind, ref string, attempts int) journalEntry {
	t.Helper()
	e := journalEntry{kind: kind, ref: ref, attempts: attempts}
	if err := an.a.db.QueryRow(`INSERT INTO `+journalTable+` (kind,ref,attempts) VALUES ($1,$2,$3) RETURNING id`, kind, ref, attempts).Scan(&e.id); err != nil {
		t.Fatal(err)
	}
	return e
}

func queueState(t *testing.T, an *analytics, id int64) (int, bool) {
	t.Helper()
	var attempts int
	var deferred bool
	err := an.a.db.QueryRow(`SELECT attempts, not_before>now() FROM `+journalTable+` WHERE id=$1`, id).Scan(&attempts, &deferred)
	if err != nil {
		t.Fatal(err)
	}
	return attempts, deferred
}

func ownedWorker(t *testing.T) (*Archive, *analytics) {
	t.Helper()
	if os.Getenv("TMX_ANALYTICS_TEST_DSN") == "" || os.Getenv("TMX_REQUESTLOG_TEST_DSN") == "" {
		t.Fatal("owned two-database fixtures required")
	}
	a, an := openTestAnalytics(t)
	l := defaultWorkerLimits
	l.legacy = time.Second
	l.raw = 200 * time.Millisecond
	l.expensive = 200 * time.Millisecond
	l.finalize = 300 * time.Millisecond
	an.worker.limits = &l
	t.Cleanup(an.cancel)
	return a, an
}

func TestRawWorkerFreshFinalizeAndUnstarted(t *testing.T) {
	_, an := ownedWorker(t)
	first := queueWork(t, an, "session", "claude-code:first", 0)
	second := queueWork(t, an, "proxy", "unstarted", 0)
	var snapshot []string
	var processedBefore int64
	an.worker.hook = func(ctx context.Context, code string) error {
		switch code {
		case "work_session":
			an.cancel()
			<-ctx.Done()
			return ctx.Err()
		case "work_proxy":
			t.Fatal("admitted work after lifecycle cancellation")
		case "finalize":
			if ctx.Err() != nil {
				t.Fatal("finalize inherited work cancellation")
			}
			processedBefore = an.processed.Load()
			rows, err := an.a.db.QueryContext(ctx, `SELECT id::text FROM `+journalTable)
			if err != nil {
				return err
			}
			for rows.Next() {
				var id string
				if err = rows.Scan(&id); err != nil {
					_ = rows.Close()
					return err
				}
				snapshot = append(snapshot, id)
			}
			return closeRows(rows)
		}
		return nil
	}
	if _, err := an.step(); err != nil {
		t.Fatal(err)
	}
	if attempts, deferred := queueState(t, an, first.id); attempts != 0 || !deferred {
		t.Fatal("budget consumed an attempt or omitted defer")
	}
	var attempts int
	var before sql.NullTime
	if err := an.a.db.QueryRow(`SELECT attempts,not_before FROM `+journalTable+` WHERE id=$1`, second.id).Scan(&attempts, &before); err != nil || attempts != 0 || before.Valid {
		t.Fatalf("unstarted changed: %d %v %v", attempts, before, err)
	}
	remaining := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=ANY($1::bigint[])`, "{"+strings.Join(snapshot, ",")+"}")
	if an.processed.Load()-processedBefore != int64(len(snapshot)-remaining) || an.worker.deferred.Load() != 1 || an.worker.passes.Load() != 1 {
		t.Fatalf("wrong counters: processed=%d before=%d snapshot=%d remaining=%d deferred=%d passes=%d", an.processed.Load(), processedBefore, len(snapshot), remaining, an.worker.deferred.Load(), an.worker.passes.Load())
	}
}

func TestRawWorkerOutcomesAndRetry(t *testing.T) {
	_, an := ownedWorker(t)
	entries := []journalEntry{
		queueWork(t, an, "proxy", "completed", 0), queueWork(t, an, "proxy", "deferred", 1),
		queueWork(t, an, "proxy", "failed", 0), queueWork(t, an, "proxy", "terminal", 2),
		queueWork(t, an, "proxy", "unknown", 1), queueWork(t, an, "proxy", "unstarted", 0),
	}
	outcomes := []journalOutcome{workCompleted, workDeferred, workFailed, workFailed, workUnknownCommit, workUnstarted}
	var units []*journalUnit
	for i, e := range entries {
		units = append(units, &journalUnit{entries: []journalEntry{e}, outcome: outcomes[i], err: errors.New("synthetic")})
	}
	if err := an.finalizeWork(context.Background(), units); err != nil {
		t.Fatal(err)
	}
	if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id IN ($1,$2)`, entries[0].id, entries[3].id); n != 0 {
		t.Fatal("completed/terminal retained")
	}
	if attempts, deferred := queueState(t, an, entries[1].id); attempts != 1 || !deferred {
		t.Fatal("defer consumed attempt")
	}
	if attempts, deferred := queueState(t, an, entries[2].id); attempts != 1 || !deferred {
		t.Fatal("ordinary retry changed")
	}
	if attempts, deferred := queueState(t, an, entries[4].id); attempts != 1 || !deferred {
		t.Fatal("unknown consumed attempt")
	}
	if n := workerCount(t, an, `SELECT count(*) FROM failed WHERE journal_id=$1`, entries[3].id); n != 1 {
		t.Fatal("no durable terminal record")
	}
	if an.processed.Load() != 2 || an.worker.deferred.Load() != 2 || an.worker.unknown.Load() != 1 {
		t.Fatal("incorrect counters")
	}
	if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1 AND attempts=0 AND not_before IS NULL`, entries[5].id); n != 1 {
		t.Fatal("unstarted acknowledged")
	}
}

// Real PostgreSQL trigger failures prove transaction rollback at each primary operation and
// analytics failure/cleanup ordering. These are not SQL mocks.
func TestRawWorkerFinalizationErrorsRetainObligations(t *testing.T) {
	for _, tc := range []struct {
		name, db, table, event, when string
		outcome                      journalOutcome
		attempts                     int
		keys                         []string
	}{
		{"proxy_insert", "primary", journalTable, "INSERT", "NEW.kind = 'proxy'", workCompleted, 0, []string{"required"}},
		{"ack", "primary", journalTable, "DELETE", "true", workCompleted, 0, nil},
		{"retry", "primary", journalTable, "UPDATE", "true", workFailed, 0, nil},
		{"defer", "primary", journalTable, "UPDATE", "true", workDeferred, 0, nil},
		{"deadletter", "analytics", "failed", "INSERT", "true", workFailed, 2, nil},
		{"cleanup", "analytics", "failed", "DELETE", "true", workCompleted, 0, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, an := ownedWorker(t)
			e := queueWork(t, an, "proxy", "fault", tc.attempts)
			db := an.a.db
			if tc.db == "analytics" {
				db = an.db
			}
			workerSQL(t, an.db, `INSERT INTO failed (journal_id,kind,ref,error) VALUES (-99,'proxy','fault','old')`)
			workerSQL(t, db, `CREATE OR REPLACE FUNCTION flt212_refuse() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic refusal'; END $$`)
			workerSQL(t, db, `CREATE TRIGGER flt212_refuse BEFORE `+tc.event+` ON `+tc.table+` FOR EACH ROW WHEN (`+tc.when+`) EXECUTE FUNCTION flt212_refuse()`)
			t.Cleanup(func() {
				workerSQL(t, db, `DROP TRIGGER flt212_refuse ON `+tc.table)
				workerSQL(t, db, `DROP FUNCTION flt212_refuse()`)
			})
			u := &journalUnit{entries: []journalEntry{e}, outcome: tc.outcome, err: errors.New("synthetic"), proxyKeys: tc.keys}
			err := an.finalizeWork(context.Background(), []*journalUnit{u})
			if err == nil {
				t.Fatal("fault accepted")
			}
			var pg interface{ SQLState() string }
			if !errors.As(err, &pg) || pg.SQLState() != "P0001" || strings.Contains(err.Error(), "synthetic refusal") {
				t.Fatalf("finalizer lost private cause or exposed SQL message: %v", err)
			}
			if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1 AND attempts=$2`, e.id, tc.attempts); n != 1 {
				t.Fatal("lost or mutated source")
			}
			if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE ref='required'`); n != 0 {
				t.Fatal("partial proxy handoff")
			}
			if an.processed.Load() != 0 {
				t.Fatal("credited failed finalization")
			}
		})
	}
}

func syntheticExchange(t *testing.T, a *Archive, trace string) {
	t.Helper()
	now := time.Now().UTC()
	err := a.storeExchange(context.Background(), &capture.Exchange{TraceID: trace, SessionID: "canonical", URL: "/v1/messages", Method: "POST", StartedAt: now, FinishedAt: now.Add(time.Second), Status: 200,
		RequestHeaders: map[string][]string{"X-Claude-Code-Session-Id": {"client"}},
		RequestBody:    []byte(`{"model":"synthetic","messages":[{"role":"user","content":"hello"}]}`),
		ResponseBody:   []byte("data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"text_delta\",\"text\":\"world\"}}\n")})
	if err != nil {
		t.Fatal(err)
	}
}

func TestRawWorkerExchangeHandoffCrashReplay(t *testing.T) {
	for _, boundary := range []string{"mirror_committed", "primary_before_commit", "primary_committed"} {
		t.Run(boundary, func(t *testing.T) {
			a, an := ownedWorker(t)
			syntheticExchange(t, a, "trace")
			var selected int64
			if err := a.db.QueryRow(`SELECT id FROM ` + journalTable + ` WHERE kind='exchange'`).Scan(&selected); err != nil {
				t.Fatal(err)
			}
			var lateID int64
			an.worker.hook = func(ctx context.Context, code string) error {
				if code == boundary {
					if lateID == 0 {
						lateID = queueWork(t, an, "exchange", "trace", 0).id
					}
					return &unknownCommit{errors.New("synthetic crash")}
				}
				return nil
			}
			_, err := an.step()
			if boundary != "mirror_committed" && err == nil {
				t.Fatal("finalize crash omitted")
			}
			if n := workerCount(t, an, `SELECT count(*) FROM exchanges WHERE trace_id='trace'`); n != 1 {
				t.Fatal("raw mirror did not commit")
			}
			if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, lateID); n != 1 {
				t.Fatal("new same-ref ID acknowledged")
			}
			if boundary == "primary_committed" {
				if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, selected); n != 0 {
					t.Fatal("committed ack missing")
				}
				if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE kind='proxy' AND ref='client'`); n != 1 {
					t.Fatal("ack without durable proxy obligation")
				}
			} else if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1 AND attempts=0`, selected); n != 1 {
				t.Fatal("lost unknown exchange")
			}
			an.worker.hook = nil
			workerSQL(t, a.db, `UPDATE `+journalTable+` SET not_before=NULL`)
			drain(t, an)
			if n := workerCount(t, an, `SELECT count(*) FROM sessions WHERE id='proxy:client'`); n != 1 {
				t.Fatal("restart lost handoff")
			}
			if n := workerCount(t, an, `SELECT count(*) FROM messages WHERE session_id='proxy:client'`); n != 2 {
				t.Fatal("replay duplicated logical messages")
			}
			if n := workerCount(t, an, `SELECT count(*) FROM items WHERE session_id='proxy:client'`); n != 2 {
				t.Fatal("replay duplicated items")
			}
			queueWork(t, an, "exchange", "trace", 0)
			drain(t, an)
			if n := workerCount(t, an, `SELECT count(*) FROM messages WHERE session_id='proxy:client'`); n != 2 {
				t.Fatal("repeat replay changed logical messages")
			}
		})
	}
}

func TestRawWorkerFailedRecordCrashReplay(t *testing.T) {
	_, an := ownedWorker(t)
	e := queueWork(t, an, "proxy", "broken", 2)
	u := &journalUnit{entries: []journalEntry{e}, outcome: workFailed, err: errors.New("synthetic")}
	an.worker.hook = func(_ context.Context, code string) error {
		if code == "failed_record_committed" {
			return errors.New("crash")
		}
		return nil
	}
	if err := an.finalizeWork(context.Background(), []*journalUnit{u}); err == nil {
		t.Fatal("missing crash")
	}
	if n := workerCount(t, an, `SELECT count(*) FROM failed WHERE journal_id=$1`, e.id); n != 1 {
		t.Fatal("missing terminal evidence")
	}
	if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id); n != 1 {
		t.Fatal("source lost at crash")
	}
	an.worker.hook = nil
	if err := an.finalizeWork(context.Background(), []*journalUnit{u}); err != nil {
		t.Fatal(err)
	}
	if n := workerCount(t, an, `SELECT count(*) FROM failed WHERE journal_id=$1`, e.id); n != 1 {
		t.Fatal("failed upsert not idempotent")
	}
}

func TestRawWorkerFairClassesAndPoison(t *testing.T) {
	a, an := ownedWorker(t)
	// More old expensive entries than the old single non-session selection window.
	workerSQL(t, a.db, `INSERT INTO `+journalTable+` (kind,ref) SELECT 'proxy','poison' FROM generate_series(1,1100)`)
	queueWork(t, an, "session", "missing-session", 0)
	queueWork(t, an, "requests_truncate", "*", 0)
	workerSQL(t, a.db, `INSERT INTO tmx.requests (ts,outcome,model) VALUES(now(),'success','fresh')`)
	syntheticExchange(t, a, "fresh-trace")
	seen := map[string]int{}
	an.worker.hook = func(ctx context.Context, code string) error {
		if strings.HasPrefix(code, "work_") {
			seen[code]++
		}
		if code == "work_proxy" {
			return context.DeadlineExceeded
		}
		return nil
	}
	if _, err := an.step(); err != nil {
		t.Fatal(err)
	}
	if seen["work_proxy"] != 1 || seen["work_session"] != 1 || seen["work_requests_truncate"] != 1 {
		t.Fatalf("coalescing/fairness: %v", seen)
	}
	if n := workerCount(t, an, `SELECT count(*) FROM exchanges WHERE trace_id='fresh-trace'`); n != 1 {
		t.Fatal("raw exchange hidden by expensive prefix")
	}
	if n := workerCount(t, an, `SELECT count(*) FROM requests WHERE model='fresh'`); n != 1 {
		t.Fatal("raw request hidden by expensive prefix")
	}
	if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE ref='poison' AND attempts=0 AND not_before>now()`); n != 100 {
		t.Fatalf("poison selected IDs = %d", n)
	}
	if an.worker.deferred.Load() != 100 {
		t.Fatal("poison deferrals invisible")
	}
	// Every expensive class appears before the next ref in any one class.
	var entries []journalEntry
	for i, k := range expensiveKinds {
		entries = append(entries, journalEntry{id: int64(i), kind: k, ref: k})
	}
	classes := makeUnits(entries)
	for _, k := range expensiveKinds {
		if len(classes[k]) != 1 {
			t.Fatal("missing class")
		}
	}
}

func TestRawWorkerRealLockBudgets(t *testing.T) {
	for _, target := range []string{"source_read", "destination_write", "session_transaction", "primary_finalize", "analytics_finalize", "links"} {
		t.Run(target, func(t *testing.T) {
			a, an := ownedWorker(t)
			syntheticExchange(t, a, "locked-trace")
			e := queueWork(t, an, "proxy", "locked", 0)
			db, table := an.db, "exchanges"
			switch target {
			case "source_read":
				db, table = a.db, schema+".exchanges"
			case "session_transaction":
				table = "sessions"
			case "primary_finalize":
				db, table = a.db, journalTable
			case "analytics_finalize":
				table = "failed"
			case "links":
				table = "request_sessions"
			}
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err = tx.Exec(`LOCK TABLE ` + table + ` IN ACCESS EXCLUSIVE MODE`); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			start := time.Now()
			switch target {
			case "source_read", "destination_write":
				_, err = an.exchMirror.copy(ctx, `TRUE`)
			case "session_transaction":
				err = an.writeSession(ctx, sessionRow{id: "proxy:locked", kind: "proxy"}, nil)
			case "links":
				originalCtx := an.ctx
				an.ctx = ctx
				an.refreshLinks(true)
				an.ctx = originalCtx
				if !an.links.full {
					t.Fatal("failed refresh discarded dirt")
				}
				err = ctx.Err()
			default:
				err = an.finalizeWork(ctx, []*journalUnit{{entries: []journalEntry{e}, outcome: workCompleted}})
			}
			if err == nil {
				t.Fatal("lock failed to exhaust context")
			}
			if time.Since(start) > 2*time.Second {
				t.Fatal("lock cancellation exceeded allowance")
			}
			if an.processed.Load() != 0 {
				t.Fatal("credited locked work")
			}
			if err = tx.Rollback(); err != nil {
				t.Fatal(err)
			}
			if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id); n != 1 {
				t.Fatal("lock lost obligation")
			}
		})
	}
}

func TestRawWorkerLinksBacklogDirtyRestart(t *testing.T) {
	a, an := ownedWorker(t)
	workerSQL(t, a.db, `INSERT INTO tmx.requests (ts,outcome,model) VALUES(now(),'success','linked')`)
	queueWork(t, an, "unknown-kind", "untouched", 0)
	if _, err := an.step(); err != nil {
		t.Fatal(err)
	}
	an.refreshLinks(false)
	assertLinksMatch(t, an)
	if n := workerCount(t, an, `SELECT count(*) FROM request_sessions`); n != 1 {
		t.Fatal("links did not refresh with backlog")
	}
	an.links.reset()
	for i := 0; i < maxLinkDirty+100; i++ {
		an.markLinks("request", fmt.Sprint(i))
	}
	if !an.links.full {
		t.Fatal("overflow did not select legacy full refresh")
	}
	workerSQL(t, an.db, `TRUNCATE request_sessions`)
	an.links = linkDirty{} // restart loses RAM, the initial full refresh restores it
	an.refreshLinks(false)
	assertLinksMatch(t, an)
	if n := workerCount(t, an, `SELECT count(*) FROM request_sessions`); n != 1 {
		t.Fatal("startup full refresh lost")
	}
}

// TMX_FLT212_CAPTURE=1 reruns the shutdown finalize case as first written: the 300 ms owned
// finalize budget, with the hook's Ping and snapshot on the product context, so a capture run
// can reproduce the 1-in-10 failure. The default is the strict earned-ACK test (see below).
const flt212CaptureEnv = "TMX_FLT212_CAPTURE"

// flt212Cause maps the first finalize failure to a fixed token. It never formats the error,
// so FLT212-CAUSE lines carry no DB text, IDs or refs.
func flt212Cause(err error) string {
	if err == nil {
		return "none"
	}
	var unknown *unknownCommit
	if !errors.As(err, &unknown) {
		return "not_unknown_commit"
	}
	inner := unknown.err
	var pg *pgconn.PgError
	var network net.Error
	switch {
	case inner == context.DeadlineExceeded:
		return "presend_sql_ctx"
	case errors.Is(inner, sql.ErrTxDone):
		return "tx_done"
	case pgconn.SafeToRetry(inner):
		return "presend_pgconn"
	case pgconn.Timeout(inner):
		return "inflight_timeout"
	case errors.Is(inner, driver.ErrBadConn), errors.As(inner, &network) && !network.Timeout(),
		errors.Is(inner, io.EOF), errors.Is(inner, io.ErrUnexpectedEOF):
		return "conn"
	case errors.As(inner, &pg):
		var safe *workerError
		if errors.As(safeWorkerError("cause", pg), &safe) && safe.state != "" {
			return "server_" + safe.state // validated five-character SQLSTATE only
		}
		return "server"
	case errors.Is(inner, pgx.ErrTxCommitRollback):
		return "commit_rollback"
	}
	return "other"
}

// flt212LogHook keeps the first finalize failure that setErr logs. setErr logs through
// log.WithError, so the entry holds the real chain while its message stays a fixed code.
type flt212LogHook struct {
	mu    sync.Mutex
	first error
}

func (h *flt212LogHook) Levels() []log.Level { return log.AllLevels }
func (h *flt212LogHook) Fire(entry *log.Entry) error {
	err, _ := entry.Data[log.ErrorKey].(error)
	var safe *workerError
	if errors.As(err, &safe) && strings.HasPrefix(safe.stage, "finalize") {
		h.mu.Lock()
		if h.first == nil {
			h.first = err
		}
		h.mu.Unlock()
	}
	return nil
}
func (h *flt212LogHook) cause() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return flt212Cause(h.first)
}

func installFLT212LogHook(t *testing.T) *flt212LogHook {
	t.Helper()
	h := &flt212LogHook{}
	logger := log.StandardLogger()
	previousLevel := logger.GetLevel()
	if !logger.IsLevelEnabled(log.WarnLevel) {
		logger.SetLevel(log.WarnLevel) // setErr logs at Warn; hooks fire only for enabled levels
	}
	previous := logger.ReplaceHooks(make(log.LevelHooks))
	hooks := make(log.LevelHooks)
	for level, hs := range previous {
		hooks[level] = append([]log.Hook(nil), hs...)
	}
	hooks.Add(h)
	logger.ReplaceHooks(hooks)
	t.Cleanup(func() {
		logger.ReplaceHooks(previous)
		logger.SetLevel(previousLevel)
	})
	return h
}

// flt212Setting reads one server setting as a fixed enum value, on its own context.
func flt212Setting(db *sql.DB, name string, allowed ...string) string {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var v string
	if err := db.QueryRowContext(ctx, "SHOW "+name).Scan(&v); err != nil {
		return "unread"
	}
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	return "other"
}

// flt212Timeline records how much finalize budget is left at each boundary, and when the
// close handshake started and ended, so a capture run shows where the budget went.
type flt212Timeline struct {
	mu                sync.Mutex
	left              map[string]time.Duration
	entered, released time.Time
	leftAfterHook     time.Duration
	afterHookMarked   bool
}

func (tl *flt212Timeline) mark(ctx context.Context, code string) {
	deadline, ok := ctx.Deadline()
	if !ok {
		return
	}
	tl.mu.Lock()
	defer tl.mu.Unlock()
	if _, seen := tl.left[code]; !seen {
		tl.left[code] = time.Until(deadline)
	}
}

func (tl *flt212Timeline) at(stamp *time.Time) {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	*stamp = time.Now()
}

func (tl *flt212Timeline) afterHook(ctx context.Context) {
	if deadline, ok := ctx.Deadline(); ok {
		tl.mu.Lock()
		tl.leftAfterHook, tl.afterHookMarked = time.Until(deadline), true
		tl.mu.Unlock()
	}
}

// line formats durations in milliseconds; -1 means the boundary was never reached.
func (tl *flt212Timeline) line() string {
	tl.mu.Lock()
	defer tl.mu.Unlock()
	ms := func(code string) int64 {
		if d, ok := tl.left[code]; ok {
			return d.Milliseconds()
		}
		return -1
	}
	handshake, afterHook := int64(-1), int64(-1)
	if !tl.entered.IsZero() && !tl.released.IsZero() {
		handshake = tl.released.Sub(tl.entered).Milliseconds()
	}
	if tl.afterHookMarked {
		afterHook = tl.leftAfterHook.Milliseconds()
	}
	return fmt.Sprintf("left_at_finalize_ms=%d handshake_ms=%d left_after_hook_ms=%d left_before_commit_ms=%d left_at_committed_ms=%d",
		ms("finalize"), handshake, afterHook, ms("primary_before_commit"), ms("primary_committed"))
}

func TestRawWorkerShutdownFinalizesBeforeDBClose(t *testing.T) {
	t.Run("work_proxy", func(t *testing.T) {
		_, an := ownedWorker(t)
		e := queueWork(t, an, "proxy", "stopping", 0)
		entered := make(chan struct{})
		an.worker.hook = func(ctx context.Context, code string) error {
			if code == "work_proxy" {
				close(entered)
				<-ctx.Done()
				return ctx.Err()
			}
			return nil
		}
		before := runtime.NumGoroutine()
		go an.run()
		an.poke()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("worker not admitted")
		}
		closed := make(chan struct{})
		go func() { an.close(); close(closed) }()
		select {
		case <-closed:
		case <-time.After(2 * time.Second):
			t.Fatal("cooperative close unbounded")
		}
		if _, err := an.step(); err != nil {
			t.Fatal(err)
		}
		if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id); n != 1 {
			t.Fatal("shutdown lost incomplete work")
		}
		assertShutdownJoinedAndReplays(t, an, e, before)
	})
	// The strict earned-ACK case runs under the production finalize budget, with the hook's
	// own Ping and snapshot on separate contexts, so instrumentation cannot spend the budget it
	// is checking. The FLT212-CAUSE line stays, so any later expiry explains itself.
	t.Run("finalize", func(t *testing.T) {
		capture := os.Getenv(flt212CaptureEnv) == "1"
		mode := "strict"
		_, an := ownedWorker(t)
		if capture {
			mode = "capture" // ownedWorker's 300 ms finalize budget, as in the failed run
		} else {
			l := *an.worker.limits
			l.finalize = defaultWorkerLimits.finalize
			an.worker.limits = &l
		}
		logHook := installFLT212LogHook(t)
		fsync := flt212Setting(an.a.db, "fsync", "on", "off")
		syncCommit := flt212Setting(an.a.db, "synchronous_commit", "on", "off", "local", "remote_write", "remote_apply")
		e := queueWork(t, an, "proxy", "stopping", 0)
		entered := make(chan struct{})
		release := make(chan struct{})
		timeline := &flt212Timeline{left: map[string]time.Duration{}}
		var rawFinalized atomic.Bool
		var matched, stoppingAtRelease bool
		var snapshot map[int64]bool
		var hookErr error
		an.worker.hook = func(ctx context.Context, code string) error {
			if code == "raw_finalized" {
				rawFinalized.Store(true)
				return nil
			}
			// Act only inside the legacy finalize: never a raw-phase finalize, never twice.
			if !rawFinalized.Load() || an.worker.phase.Load() != "finalize" {
				return nil
			}
			timeline.mark(ctx, code)
			if code != "finalize" || matched {
				return nil
			}
			own, cancelOwn := context.WithTimeout(context.Background(), 2*time.Second)
			var queued int
			hookErr = an.a.db.QueryRowContext(own, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id).Scan(&queued)
			cancelOwn()
			if hookErr != nil || queued != 1 {
				return hookErr
			}
			matched = true
			timeline.at(&timeline.entered)
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				hookErr = ctx.Err()
				return hookErr
			}
			timeline.at(&timeline.released)
			stoppingAtRelease = an.worker.stopping.Load()
			probe := ctx
			if !capture {
				var cancelProbe context.CancelFunc
				probe, cancelProbe = context.WithTimeout(context.Background(), 2*time.Second)
				defer cancelProbe()
			}
			defer timeline.afterHook(ctx)
			if hookErr = an.db.PingContext(probe); hookErr != nil {
				return hookErr
			}
			var rows *sql.Rows
			rows, hookErr = an.a.db.QueryContext(probe, `SELECT id FROM `+journalTable)
			if hookErr != nil {
				return hookErr
			}
			snapshot = map[int64]bool{}
			for rows.Next() {
				var id int64
				if hookErr = rows.Scan(&id); hookErr != nil {
					_ = rows.Close()
					return hookErr
				}
				snapshot[id] = true
			}
			hookErr = closeRows(rows)
			return hookErr
		}
		before := runtime.NumGoroutine()
		go an.run()
		an.poke()
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("worker not admitted")
		}
		closeStart := time.Now()
		closed := make(chan struct{})
		go func() { an.close(); close(closed) }()
		select {
		case <-an.ctx.Done(): // close sets stopping before cancelling admission
		case <-time.After(2 * time.Second):
			t.Fatal("close did not stop admission")
		}
		close(release)
		closeBound := 2 * time.Second
		if !capture {
			closeBound = an.limits().finalize + 2*time.Second
		}
		select {
		case <-closed:
		case <-time.After(closeBound):
			t.Fatal("cooperative close unbounded")
		}
		closeMS := time.Since(closeStart).Milliseconds()
		// A COMMIT that timed out in flight may still finish on the server: wait before
		// asserting, so the capture can tell an unknown outcome from a definite rollback.
		lateCommit := "n/a"
		if workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id) == 1 {
			lateCommit = "false"
			for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
				time.Sleep(50 * time.Millisecond)
				if workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id) == 0 {
					lateCommit = "true"
					break
				}
			}
		}
		code, _ := an.worker.finalizeCode.Load().(string)
		if code == "" {
			code = "none"
		}
		t.Logf("FLT212-CAUSE mode=%s class=%s %s close_ms=%d late_commit=%s code=%s fsync=%s sync_commit=%s",
			mode, logHook.cause(), timeline.line(), closeMS, lateCommit, code, fsync, syncCommit)
		if _, err := an.step(); err != nil {
			t.Fatal(err)
		}
		if hookErr != nil {
			t.Fatalf("finalize hook: %v", safeWorkerError("hook", hookErr))
		}
		if !matched || !stoppingAtRelease {
			t.Fatalf("legacy finalize matched=%t stopping_at_release=%t", matched, stoppingAtRelease)
		}
		if !snapshot[e.id] {
			t.Fatal("intended job absent before acknowledgment")
		}
		remaining := map[int64]bool{}
		rows, err := an.a.db.Query(`SELECT id FROM ` + journalTable)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var id int64
			if err = rows.Scan(&id); err != nil {
				_ = rows.Close()
				t.Fatal(err)
			}
			remaining[id] = true
		}
		if err = closeRows(rows); err != nil {
			t.Fatal(err)
		}
		if remaining[e.id] {
			t.Fatal("earned synthetic job not acknowledged")
		}
		var acked int64
		for id := range snapshot {
			if !remaining[id] {
				acked++
			}
		}
		if processed := an.processed.Load(); processed != acked {
			t.Fatalf("processed=%d acknowledged=%d snapshot=%d remaining=%d", processed, acked, len(snapshot), len(remaining))
		}
		if an.worker.finalizeFailures.Load() != 0 || code != "ok" {
			t.Fatalf("finalization failures=%d code=%s", an.worker.finalizeFailures.Load(), code)
		}
		if an.worker.deferred.Load() != 0 || an.worker.unknown.Load() != 0 {
			t.Fatalf("deferred=%d unknown=%d", an.worker.deferred.Load(), an.worker.unknown.Load())
		}
		t.Logf("processed=%d acknowledged=%d snapshot=%d remaining=%d", an.processed.Load(), acked, len(snapshot), len(remaining))
		assertShutdownJoinedAndReplays(t, an, e, before)
	})
}

// assertShutdownJoinedAndReplays checks that run joined before the DB closed, then reopens the
// analytics connection and worker state, rather than clearing the old stopping flag.
// Incomplete work survives shutdown and replays on a real restart.
func assertShutdownJoinedAndReplays(t *testing.T, an *analytics, e journalEntry, before int) {
	t.Helper()
	select {
	case <-an.done:
	default:
		t.Fatal("run goroutine abandoned")
	}
	if runtime.NumGoroutine() > before+4 {
		t.Fatal("worker goroutine growth")
	}
	restarted, err := openAnalytics(context.Background(), an.a, os.Getenv("TMX_ANALYTICS_TEST_DSN"))
	if err != nil {
		t.Fatal(err)
	}
	defer restarted.cancel()
	defer func() { _ = restarted.db.Close() }()
	workerSQL(t, an.a.db, `UPDATE `+journalTable+` SET not_before=NULL WHERE id=$1`, e.id)
	drain(t, restarted)
	if n := workerCount(t, restarted, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id); n != 0 {
		t.Fatal("restart did not discharge retained obligation")
	}
}

// expiryBatch queues one mixed finalize batch: a completed exchange with a proxy key, a
// completed proxy (doneRef), a deferred unit, a retry and a terminal failure.
func expiryBatch(t *testing.T, an *analytics, doneRef string) ([]journalEntry, []*journalUnit) {
	t.Helper()
	entries := []journalEntry{
		queueWork(t, an, "exchange", "expiry-exchange", 0),
		queueWork(t, an, "proxy", doneRef, 0),
		queueWork(t, an, "proxy", "expiry-deferred", 0),
		queueWork(t, an, "proxy", "expiry-retry", 0),
		queueWork(t, an, "proxy", "expiry-terminal", maxAttempts-1),
	}
	var units []*journalUnit
	for i, outcome := range []journalOutcome{workCompleted, workCompleted, workDeferred, workFailed, workFailed} {
		u := &journalUnit{entries: []journalEntry{entries[i]}, outcome: outcome, err: errors.New("synthetic")}
		if i == 0 {
			u.proxyKeys = []string{"expiry-handoff"}
		}
		units = append(units, u)
	}
	return entries, units
}

// assertExpiryRetained checks a failed primary finalize left every selected row queued and
// unchanged, inserted no proxy row and credited nothing. The analytics terminal record commits
// separately and first, as the design specifies, so it exists; its failed_at is returned.
func assertExpiryRetained(t *testing.T, an *analytics, entries []journalEntry, err error, classes ...string) time.Time {
	t.Helper()
	var safe *workerError
	if !errors.As(err, &safe) || !strings.HasPrefix(safe.stage, "finalize_primary_") {
		t.Fatalf("expiry not a primary finalize failure: %v", err)
	}
	matched := false
	for _, class := range classes {
		matched = matched || safe.class == class
	}
	if !matched {
		t.Fatalf("expiry class=%s want one of %v", safe.class, classes)
	}
	for _, e := range entries {
		if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1 AND attempts=$2 AND not_before IS NULL`, e.id, e.attempts); n != 1 {
			t.Fatal("expiry lost a row or kept a rolled-back retry/defer update")
		}
	}
	if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE kind='proxy' AND ref='expiry-handoff'`); n != 0 {
		t.Fatal("proxy handoff committed without its acknowledgment")
	}
	var failedAt time.Time
	if err = an.db.QueryRow(`SELECT failed_at FROM failed WHERE journal_id=$1`, entries[4].id).Scan(&failedAt); err != nil {
		t.Fatal("terminal record missing before its acknowledgment")
	}
	if an.processed.Load() != 0 || an.worker.deferred.Load() != 0 || an.worker.unknown.Load() != 0 || an.worker.finalizeFailures.Load() != 1 {
		t.Fatalf("expiry counters processed=%d deferred=%d unknown=%d failures=%d",
			an.processed.Load(), an.worker.deferred.Load(), an.worker.unknown.Load(), an.worker.finalizeFailures.Load())
	}
	return failedAt
}

// assertExpiryReplays reruns the same units with no fault and checks they finish exactly once.
func assertExpiryReplays(t *testing.T, an *analytics, entries []journalEntry, units []*journalUnit, firstFailedAt time.Time) {
	t.Helper()
	exchange, done, deferred, retry, terminal := entries[0], entries[1], entries[2], entries[3], entries[4]
	an.worker.hook = nil
	if err := an.finalizePhase(units); err != nil {
		t.Fatal(err)
	}
	for _, e := range []journalEntry{exchange, done, terminal} {
		if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id); n != 0 {
			t.Fatal("replay did not acknowledge")
		}
	}
	if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE kind='proxy' AND ref='expiry-handoff'`); n != 1 {
		t.Fatalf("replay handoff rows=%d", n)
	}
	var failedAt time.Time
	if err := an.db.QueryRow(`SELECT failed_at FROM failed WHERE journal_id=$1`, terminal.id).Scan(&failedAt); err != nil || !failedAt.After(firstFailedAt) {
		t.Fatal("replay did not refresh the terminal record")
	}
	if attempts, def := queueState(t, an, deferred.id); attempts != 0 || !def {
		t.Fatal("replayed defer consumed an attempt or omitted defer")
	}
	if attempts, def := queueState(t, an, retry.id); attempts != 1 || !def {
		t.Fatal("replayed retry not applied once")
	}
	if code, _ := an.worker.finalizeCode.Load().(string); code != "ok" || an.processed.Load() != 3 || an.worker.deferred.Load() != 1 || an.worker.finalizeFailures.Load() != 1 {
		t.Fatalf("replay code=%s processed=%d deferred=%d failures=%d", code, an.processed.Load(), an.worker.deferred.Load(), an.worker.finalizeFailures.Load())
	}
}

// A finalize budget that expires at the primary COMMIT must retain every obligation and
// replay it. It runs through finalizePhase, so the fresh finalize context is covered.
func TestRawWorkerFinalizeBudgetExpiryRetainsAndReplays(t *testing.T) {
	// before_commit (C1): database/sql sees the finalize context already done and never sends
	// COMMIT, so the primary transaction rolls back.
	t.Run("before_commit", func(t *testing.T) {
		_, an := ownedWorker(t)
		entries, units := expiryBatch(t, an, "expiry-done")
		an.worker.hook = func(ctx context.Context, code string) error {
			if code == "primary_before_commit" {
				<-ctx.Done() // the real tx.Commit then runs on the expired finalize context
			}
			return nil
		}
		err := an.finalizePhase(units)
		firstFailedAt := assertExpiryRetained(t, an, entries, err, "unknown_commit_presend", "unknown_commit_tx_done")
		assertExpiryReplays(t, an, entries, units, firstFailedAt)
	})
	// during_commit: a deferred constraint trigger holds COMMIT for 0.6 s against the 300 ms
	// owned finalize budget, so the deadline hits with COMMIT in flight. pgx then cancels it:
	// on the read timeout, pgconn's asyncClose sends a CancelRequest, which aborts the trigger's
	// sleep and rolls the COMMIT back. So this fixture shows a rollback, not a late commit.
	// A real late commit (a WAL-flush stall the cancel cannot stop) is seen only as the capture
	// run's late_commit=true, and is not tested here.
	t.Run("during_commit", func(t *testing.T) {
		_, an := ownedWorker(t)
		entries, units := expiryBatch(t, an, "slow-commit")
		// The function-level setting keeps the server from noticing the closed client socket
		// during the sleep, so only the cancel can end the transaction early.
		workerSQL(t, an.a.db, `CREATE FUNCTION flt212_slow_commit() RETURNS trigger LANGUAGE plpgsql
   SET client_connection_check_interval = 0 AS $$ BEGIN PERFORM pg_sleep(0.6); RETURN NULL; END $$`)
		workerSQL(t, an.a.db, `CREATE CONSTRAINT TRIGGER flt212_slow_commit AFTER DELETE ON `+journalTable+`
   DEFERRABLE INITIALLY DEFERRED FOR EACH ROW WHEN (OLD.ref = 'slow-commit') EXECUTE FUNCTION flt212_slow_commit()`)
		dropTrigger := func() {
			workerSQL(t, an.a.db, `DROP TRIGGER IF EXISTS flt212_slow_commit ON `+journalTable)
			workerSQL(t, an.a.db, `DROP FUNCTION IF EXISTS flt212_slow_commit()`)
		}
		t.Cleanup(dropTrigger)
		started := time.Now()
		err := an.finalizePhase(units)
		// Check only after the trigger's sleep would have ended, so a COMMIT the cancel failed
		// to stop would already show as acknowledged rows.
		time.Sleep(time.Until(started.Add(1500 * time.Millisecond)))
		firstFailedAt := assertExpiryRetained(t, an, entries, err, "unknown_commit_inflight_timeout")
		dropTrigger()
		assertExpiryReplays(t, an, entries, units, firstFailedAt)
	})
}

// A lost reply after a work write must never be taken as an ordinary failure: that would
// consume an attempt and, at the ceiling, dead-letter and acknowledge an obligation whose
// outcome is unknown. A COMMIT the server rejects is the opposite case: it rolled back, so it is
// a definite failure that follows the ordinary attempts and ceiling instead of looping forever.
//   - lost_reply lets the real write land, then reports a broken connection: after a thread,
//     commit or PR extraction COMMIT (through commitUnit), and after the exchange mirror's
//     autocommit copy (a raw write with no unknownCommit wrapper).
//   - commit_refused makes PostgreSQL fail the extraction COMMIT with an ERROR (P0001, and a
//     40001 serialization failure for threads), from a deferred constraint trigger.
func TestRawWorkerLostReplyNeverDeadLetters(t *testing.T) {
	ts := time.Date(2026, 10, 21, 9, 0, 0, 0, time.UTC)
	for _, tc := range []lostReplyCase{
		{kind: "thread", boundary: "unit_committed", table: "threads", lost: io.ErrUnexpectedEOF, refusals: []string{"P0001", "40001"},
			queue: func(t *testing.T, a *Archive) {
				if _, err := a.IngestThreads(context.Background(), ThreadBatch{Source: "t3", Threads: []ThreadIngest{{ExternalID: "lost-reply", Title: "x"}}}); err != nil {
					t.Fatal(err)
				}
			}},
		{kind: "commit", boundary: "unit_committed", table: "commits", lost: driver.ErrBadConn, refusals: []string{"P0001"},
			queue: func(t *testing.T, a *Archive) {
				if _, err := a.IngestCommits(context.Background(), CommitBatch{Machine: "owned", Commits: []CommitIngest{
					{Repo: "github.com/o/r", SHA: "eeeeeee5", CommittedAt: &ts, Additions: 1, Files: 1}}}); err != nil {
					t.Fatal(err)
				}
			}},
		{kind: "pull_request", boundary: "unit_committed", table: "prs", lost: io.EOF, refusals: []string{"P0001"},
			queue: func(t *testing.T, a *Archive) {
				if _, err := a.IngestPullRequests(context.Background(), PullRequestBatch{Machine: "owned", PullRequests: []PullRequestIngest{
					{Pull: ghPull(41, "lost reply", "open", "", "2026-10-20T10:00:00Z", "")}}}); err != nil {
					t.Fatal(err)
				}
			}},
		// The exchange mirror is an autocommit write: it has no COMMIT of its own to refuse.
		{kind: "exchange", boundary: "mirror_committed", table: "exchanges", lost: io.ErrUnexpectedEOF, trace: "lost-reply-trace",
			queue: func(t *testing.T, a *Archive) { syntheticExchange(t, a, "lost-reply-trace") }},
	} {
		modes := []string{"lost_reply"}
		for _, state := range tc.refusals {
			modes = append(modes, "commit_refused_"+state)
		}
		for _, mode := range modes {
			for _, attempts := range []int{0, maxAttempts - 1} {
				t.Run(fmt.Sprintf("%s/%s/attempts_%d", tc.kind, mode, attempts), func(t *testing.T) {
					testLostReply(t, tc, mode, attempts)
				})
			}
		}
	}
}

type lostReplyCase struct {
	kind, boundary, table, trace string
	lost                         error
	refusals                     []string // SQLSTATEs a deferred trigger raises at COMMIT
	queue                        func(t *testing.T, a *Archive)
}

func testLostReply(t *testing.T, tc lostReplyCase, mode string, attempts int) {
	a, an := ownedWorker(t)
	tc.queue(t, a)
	workerSQL(t, a.db, `UPDATE `+journalTable+` SET attempts=$1 WHERE kind=$2`, attempts, tc.kind)
	var e journalEntry
	if err := a.db.QueryRow(`SELECT id,kind,ref,attempts FROM `+journalTable+` WHERE kind=$1`, tc.kind).Scan(&e.id, &e.kind, &e.ref, &e.attempts); err != nil {
		t.Fatalf("one %s obligation expected: %v", tc.kind, err)
	}
	countRows := `SELECT count(*) FROM ` + tc.table + ` WHERE id=$1`
	ref := e.ref
	if tc.trace != "" {
		countRows, ref = `SELECT count(*) FROM `+tc.table+` WHERE trace_id=$1`, tc.trace
	}
	refused := strings.TrimPrefix(mode, "commit_refused_")
	if refused == mode {
		refused = ""
	}
	if refused != "" {
		// Deferred, so the ERROR comes from COMMIT itself and rolls the write back.
		workerSQL(t, an.db, `CREATE FUNCTION flt212_refuse_commit() RETURNS trigger LANGUAGE plpgsql AS
   $$ BEGIN RAISE EXCEPTION 'synthetic commit refusal' USING ERRCODE = '`+refused+`'; END $$`)
		workerSQL(t, an.db, `CREATE CONSTRAINT TRIGGER flt212_refuse_commit AFTER INSERT OR UPDATE ON `+tc.table+`
   DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION flt212_refuse_commit()`)
		dropTrigger := func() {
			workerSQL(t, an.db, `DROP TRIGGER IF EXISTS flt212_refuse_commit ON `+tc.table)
			workerSQL(t, an.db, `DROP FUNCTION IF EXISTS flt212_refuse_commit()`)
		}
		t.Cleanup(dropTrigger)
		testRefusedCommit(t, a, an, e, countRows, ref, refused, dropTrigger)
		return
	}
	class := "work:unknown_commit_conn"
	if tc.kind == "exchange" {
		class = "work:connection" // a raw write's lost reply, with no COMMIT wrapper
	}
	an.worker.hook = func(_ context.Context, code string) error {
		if code == tc.boundary {
			return tc.lost // the write landed; its reply did not
		}
		return nil
	}
	u := &journalUnit{entries: []journalEntry{e}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	an.executeUnit(ctx, u)
	cancel()
	if u.outcome != workUnknownCommit {
		t.Fatalf("outcome=%d, want unknown", u.outcome)
	}
	if got := safeWorkerError("work", u.err).Error(); got != class {
		t.Fatalf("class=%s want %s", got, class)
	}
	if n := workerCount(t, an, countRows, ref); n != 1 {
		t.Fatalf("fixture write did not land: %d", n)
	}
	if err := an.finalizePhase([]*journalUnit{u}); err != nil {
		t.Fatal(err)
	}
	// Retained like a deferral: attempts unchanged and not_before moved, at the ceiling too.
	if got, def := queueState(t, an, e.id); got != attempts || !def {
		t.Fatalf("attempts=%d deferred=%t, want attempts=%d deferred", got, def, attempts)
	}
	if n := workerCount(t, an, `SELECT count(*) FROM failed WHERE journal_id=$1 OR (kind=$2 AND ref=$3)`, e.id, e.kind, e.ref); n != 0 {
		t.Fatal("unknown outcome dead-lettered")
	}
	if an.processed.Load() != 0 || an.worker.unknown.Load() != 1 || an.worker.deferred.Load() != 1 {
		t.Fatalf("counters processed=%d unknown=%d deferred=%d", an.processed.Load(), an.worker.unknown.Load(), an.worker.deferred.Load())
	}
	// Replay with no fault: the obligation completes once and acknowledges.
	an.worker.hook = nil
	workerSQL(t, a.db, `UPDATE `+journalTable+` SET not_before=NULL WHERE id=$1`, e.id)
	drain(t, an)
	if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id); n != 0 {
		t.Fatal("replay did not acknowledge")
	}
	if n := workerCount(t, an, countRows, ref); n != 1 {
		t.Fatalf("replay rows=%d, want exactly 1", n)
	}
	if n := workerCount(t, an, `SELECT count(*) FROM failed`); n != 0 {
		t.Fatal("replay dead-lettered")
	}
}

// testRefusedCommit checks a server-rejected COMMIT is a definite failure: an ordinary retry
// below the ceiling, and at the ceiling a terminal record before the acknowledgment. No
// analytics row exists either way, because the transaction rolled back.
func testRefusedCommit(t *testing.T, a *Archive, an *analytics, e journalEntry, countRows, ref, state string, dropTrigger func()) {
	t.Helper()
	u := &journalUnit{entries: []journalEntry{e}}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	an.executeUnit(ctx, u)
	cancel()
	if u.outcome != workFailed {
		t.Fatalf("outcome=%d, want failed", u.outcome)
	}
	if got := safeWorkerError("work", u.err).Error(); got != "work:commit_rejected:"+state {
		t.Fatalf("class=%s want work:commit_rejected:%s", got, state)
	}
	if n := workerCount(t, an, countRows, ref); n != 0 {
		t.Fatalf("rolled-back COMMIT left %d rows", n)
	}
	if err := an.finalizePhase([]*journalUnit{u}); err != nil {
		t.Fatal(err)
	}
	if an.worker.unknown.Load() != 0 || an.worker.deferred.Load() != 0 {
		t.Fatalf("definite failure counted unknown=%d deferred=%d", an.worker.unknown.Load(), an.worker.deferred.Load())
	}
	if e.attempts+1 >= maxAttempts {
		if n := workerCount(t, an, `SELECT count(*) FROM failed WHERE journal_id=$1 AND error=$2`, e.id, "work:commit_rejected:"+state); n != 1 {
			t.Fatal("ceiling did not write the terminal record")
		}
		if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id); n != 0 || an.processed.Load() != 1 {
			t.Fatalf("ceiling did not acknowledge: rows=%d processed=%d", n, an.processed.Load())
		}
		if n := workerCount(t, an, countRows, ref); n != 0 {
			t.Fatal("dead-lettered work left an analytics row")
		}
		return
	}
	// Below the ceiling: one ordinary attempt with its backoff, then a normal replay.
	if got, def := queueState(t, an, e.id); got != e.attempts+1 || !def {
		t.Fatalf("attempts=%d deferred=%t, want attempts=%d with backoff", got, def, e.attempts+1)
	}
	if n := workerCount(t, an, `SELECT count(*) FROM failed`); n != 0 || an.processed.Load() != 0 {
		t.Fatal("retry dead-lettered or acknowledged")
	}
	dropTrigger()
	workerSQL(t, a.db, `UPDATE `+journalTable+` SET not_before=NULL WHERE id=$1`, e.id)
	drain(t, an)
	if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id); n != 0 {
		t.Fatal("replay did not acknowledge")
	}
	if n := workerCount(t, an, countRows, ref); n != 1 {
		t.Fatalf("replay rows=%d, want exactly 1", n)
	}
}

// The statement triggers count actual PostgreSQL operation groups and inject latency once
// per SQL statement. Increasing the fixture by 100x must not increase that count.
func TestRawWorkerSetBasedFinalization(t *testing.T) {
	for _, n := range []int{1, 100} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			_, an := ownedWorker(t)
			var units []*journalUnit
			var deferredIDs, retryIDs, unstartedIDs []int64
			for i := 0; i < n; i++ {
				for j, outcome := range []journalOutcome{workCompleted, workCompleted, workDeferred, workUnknownCommit, workFailed, workFailed, workUnstarted} {
					kind := "proxy"
					if j == 0 {
						kind = "exchange"
					}
					attempts := i % 2
					if j == 5 {
						attempts = 2
					}
					e := queueWork(t, an, kind, fmt.Sprintf("mixed-%d-%d-'\\", i, j), attempts)
					u := &journalUnit{entries: []journalEntry{e}, outcome: outcome, err: errors.New("private body and DSN")}
					if j == 0 {
						u.proxyKeys = []string{fmt.Sprint(i), "shared", fmt.Sprint(i)}
					}
					if j == 1 {
						workerSQL(t, an.db, `INSERT INTO failed (journal_id,kind,ref,error) VALUES ($1,$2,$3,'old')`, -e.id, kind, e.ref)
					}
					units = append(units, u)
					switch j {
					case 2, 3:
						deferredIDs = append(deferredIDs, e.id)
					case 4:
						retryIDs = append(retryIDs, e.id)
					case 6:
						unstartedIDs = append(unstartedIDs, e.id)
					}
				}
			}
			for _, db := range []*sql.DB{an.a.db, an.db} {
				workerSQL(t, db, `CREATE TABLE flt212_statements (op text)`)
				workerSQL(t, db, `CREATE FUNCTION flt212_statement() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN
    INSERT INTO flt212_statements VALUES (TG_OP); PERFORM pg_sleep(0.015); RETURN NULL; END $$`)
				table := journalTable
				if db == an.db {
					table = "failed"
				}
				workerSQL(t, db, `CREATE TRIGGER flt212_statement AFTER INSERT OR UPDATE OR DELETE ON `+table+` FOR EACH STATEMENT EXECUTE FUNCTION flt212_statement()`)
				t.Cleanup(func() {
					workerSQL(t, db, `DROP TRIGGER flt212_statement ON `+table)
					workerSQL(t, db, `DROP FUNCTION flt212_statement()`)
					workerSQL(t, db, `DROP TABLE flt212_statements`)
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := an.finalizeWork(ctx, units); err != nil {
				t.Fatal(err)
			}
			var primary, analytics int
			if err := an.a.db.QueryRow(`SELECT count(*) FROM flt212_statements`).Scan(&primary); err != nil {
				t.Fatal(err)
			}
			if err := an.db.QueryRow(`SELECT count(*) FROM flt212_statements`).Scan(&analytics); err != nil {
				t.Fatal(err)
			}
			// ON CONFLICT invokes both INSERT and UPDATE statement triggers for one SQL command.
			if primary != 4 || analytics != 3 {
				t.Fatalf("statement groups scale with entries: primary=%d analytics=%d", primary, analytics)
			}
			if an.processed.Load() != int64(3*n) || an.worker.deferred.Load() != int64(2*n) || an.worker.unknown.Load() != int64(n) {
				t.Fatal("mixed counters")
			}
			if got := workerCount(t, an, `SELECT count(*) FROM failed`); got != n {
				t.Fatalf("failed rows=%d", got)
			}
			if got := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE kind='proxy' AND (ref='shared' OR ref ~ '^[0-9]+$')`); got != n+1 {
				t.Fatalf("unique handoff keys=%d", got)
			}
			for i, id := range deferredIDs {
				attempts, def := queueState(t, an, id)
				if attempts != (i/2)%2 || !def {
					t.Fatal("defer changed attempts/backoff")
				}
			}
			for i, id := range retryIDs {
				attempts, def := queueState(t, an, id)
				if attempts != i%2+1 || !def {
					t.Fatal("retry changed per-row attempts/backoff")
				}
			}
			for i, id := range unstartedIDs {
				if got := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1 AND attempts=$2 AND not_before IS NULL`, id, i%2); got != 1 {
					t.Fatal("unstarted changed")
				}
			}
			// Every retry uses its own prior attempts, not the first row's backoff.
			if got := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE ref LIKE 'mixed-%-4-%' AND
    abs(extract(epoch FROM (not_before-now()))-attempts*60)<5`); got != n {
				t.Fatal("per-row backoff mismatch")
			}
			t.Logf("entries=%d primary statement groups=%d analytics statement triggers=%d", len(units), primary, analytics)
		})
	}
}

func TestRawWorkerLateIDsAndLookupFailure(t *testing.T) {
	t.Run("late_ids", func(t *testing.T) {
		a, an := ownedWorker(t)
		syntheticExchange(t, a, "late-trace")
		workerSQL(t, a.db, `DELETE FROM `+journalTable)
		tx, err := a.db.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()
		var low int64
		if err = tx.QueryRow(`INSERT INTO ` + journalTable + ` (kind,ref) VALUES ('exchange','late-trace') RETURNING id`).Scan(&low); err != nil {
			t.Fatal(err)
		}
		selected := queueWork(t, an, "exchange", "late-trace", 0)
		var later int64
		an.worker.hook = func(_ context.Context, code string) error {
			if code == "mirror_committed" {
				if err := tx.Commit(); err != nil {
					return err
				}
				later = queueWork(t, an, "exchange", "late-trace", 0).id
			}
			return nil
		}
		if _, err = an.step(); err != nil {
			t.Fatal(err)
		}
		if low >= selected.id || later <= selected.id {
			t.Fatal("invalid fixture ordering")
		}
		if got := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=ANY($1::bigint[])`, fmt.Sprintf("{%d,%d}", low, later)); got != 2 {
			t.Fatal("ack swallowed later/lower-ID obligations")
		}
		if got := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, selected.id); got != 0 {
			t.Fatal("selected exchange not acknowledged")
		}
		an.worker.hook = nil
		drain(t, an)
		if got := workerCount(t, an, `SELECT count(*) FROM sessions WHERE id='proxy:client'`); got != 1 {
			t.Fatal("replay lost session")
		}
	})
	t.Run("lookup_error", func(t *testing.T) {
		a, an := ownedWorker(t)
		syntheticExchange(t, a, "lookup-trace")
		var id int64
		if err := a.db.QueryRow(`SELECT id FROM ` + journalTable + ` WHERE kind='exchange'`).Scan(&id); err != nil {
			t.Fatal(err)
		}
		an.worker.hook = func(_ context.Context, code string) error {
			if code == "mirror_committed" {
				workerSQL(t, a.db, `ALTER TABLE `+schema+`.exchanges RENAME TO flt212_hidden_exchanges`)
			}
			return nil
		}
		t.Cleanup(func() { workerSQL(t, a.db, `ALTER TABLE `+schema+`.flt212_hidden_exchanges RENAME TO exchanges`) })
		if _, err := an.step(); err != nil {
			t.Fatal(err)
		}
		if got := workerCount(t, an, `SELECT count(*) FROM exchanges WHERE trace_id='lookup-trace'`); got != 1 {
			t.Fatal("mirror fixture not durable")
		}
		attempts, def := queueState(t, an, id)
		if attempts != 1 || !def {
			t.Fatal("lookup error treated as no-key success")
		}
		if got := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE kind='proxy'`); got != 0 {
			t.Fatal("failed lookup produced handoff")
		}
	})
}

func TestRawWorkerEligibleIndexGate(t *testing.T) {
	_, an := ownedWorker(t)
	for _, kind := range []string{"session", "proxy"} {
		ready := queueWork(t, an, kind, "ready", 0)
		if err := an.setState(context.Background(), "trgm_index", "pending"); err != nil {
			t.Fatal(err)
		}
		an.buildDeferredIndexes()
		if state, err := an.getState(context.Background(), "trgm_index"); err != nil || state != "pending" {
			t.Fatal("ready extraction did not block", state, err)
		}
		workerSQL(t, an.a.db, `UPDATE `+journalTable+` SET not_before=now()+interval '1 hour' WHERE id=$1`, ready.id)
	}
	for _, kind := range []string{"request", "exchange", "requests_truncate", "unknown-future"} {
		queueWork(t, an, kind, "durable", 0)
	}
	an.buildDeferredIndexes()
	if state, err := an.getState(context.Background(), "trgm_index"); err != nil || state != "built" {
		t.Fatal("retained deferred/unknown/raw work blocked index", state, err)
	}
	if got := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE kind='unknown-future'`); got != 1 {
		t.Fatal("unknown obligation lost")
	}
}

func TestRawWorkerSafeCauseAndUnknownGauge(t *testing.T) {
	_, an := ownedWorker(t)
	_, cause := an.db.Exec(`DO $$ BEGIN RAISE EXCEPTION 'private raw-ref DSN body message'; END $$`)
	if cause == nil {
		t.Fatal("missing real SQL cause")
	}
	safe := safeWorkerError("finalize_cleanup", cause)
	if !errors.Is(safe, cause) || safe.Error() != "finalize_cleanup:error:P0001" {
		t.Fatalf("safe code or unwrap: %v", safe)
	}
	var logged bytes.Buffer
	previousOutput := log.StandardLogger().Out
	log.SetOutput(&logged)
	defer log.SetOutput(previousOutput)
	an.setErr(safe)
	if !strings.Contains(logged.String(), safe.Error()) || strings.Contains(logged.String(), "private") {
		t.Fatalf("unsafe worker log: %s", logged.String())
	}
	queueWork(t, an, "future-kind", "private raw-ref", 0)
	for i := 0; i < 2; i++ {
		if _, err := an.step(); err != nil {
			t.Fatal(err)
		}
	}
	if an.worker.unknownKinds.Load() != 1 || an.worker.unknown.Load() != 0 || an.failures.Load() != 1 {
		t.Fatal("unknown-kind gauge inflated events/failures")
	}
	if got := an.lastErr.Load(); got != "finalize_cleanup:error:P0001" {
		t.Fatalf("unknown replaced diagnostic: %v", got)
	}
	status, err := an.a.AnalyticsStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if status["last_error"] != "finalize_cleanup:error:P0001" || status["unknown_kinds_selected"] != int64(1) || status["unknown_outcomes"] != int64(0) {
		t.Fatal("unsafe or conflated status")
	}
}

// These real table locks deliberately outlast the rejected 30s reconcile and 10s link
// deadlines. Raw copies AND exact acknowledgments must be visible while maintenance waits.
func TestRawWorkerBeforeSlowLegacyAndEquivalentDrain(t *testing.T) {
	a, an := ownedWorker(t)
	limits := defaultWorkerLimits
	an.worker.limits = &limits
	workerSQL(t, a.db, `INSERT INTO `+journalTable+` (kind,ref) SELECT 'proxy','old-expensive' FROM generate_series(1,1100)`)
	syntheticExchange(t, a, "slow-legacy-trace")
	workerSQL(t, a.db, `INSERT INTO tmx.requests (ts,outcome,model,trace_id,harness,machine,cost_usd)
  VALUES (now(),'success','synthetic','slow-legacy-trace','claude-code','owned',0.125)`)
	queueWork(t, an, "requests_truncate", "*", 0)
	var rawIDs []int64
	rows, err := a.db.Query(`SELECT id FROM ` + journalTable + ` WHERE kind IN ('request','exchange')`)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var id int64
		if err = rows.Scan(&id); err != nil {
			t.Fatal(err)
		}
		rawIDs = append(rawIDs, id)
	}
	if err = closeRows(rows); err != nil {
		t.Fatal(err)
	}
	var lock *sql.Tx
	entered := make(chan struct{})
	an.worker.hook = func(ctx context.Context, code string) error {
		switch code {
		case "raw_finalized":
			var err error
			lock, err = an.db.Begin()
			if err != nil {
				return err
			}
			_, err = lock.Exec(`LOCK TABLE requests IN SHARE MODE`)
			return err
		case "work_requests_truncate":
			deadline, ok := ctx.Deadline()
			if !ok || time.Until(deadline) < 29*time.Minute {
				return errors.New("legacy reconciliation budget changed")
			}
			close(entered)
		}
		return nil
	}
	result := make(chan error, 1)
	go func() { _, err := an.step(); result <- err }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("legacy reconcile not reached")
	}
	// All observations below occur before allowing the old full reconciliation to finish.
	for _, id := range rawIDs {
		if got := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, id); got != 0 {
			t.Fatal("raw ack waited for maintenance")
		}
	}
	if got := workerCount(t, an, `SELECT count(*) FROM requests WHERE trace_id='slow-legacy-trace' AND cost_usd=0.125`); got != 1 {
		t.Fatal("raw request not visible")
	}
	if got := workerCount(t, an, `SELECT count(*) FROM exchanges WHERE trace_id='slow-legacy-trace'`); got != 1 {
		t.Fatal("raw exchange not visible")
	}
	if got := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE kind='proxy' AND ref='client'`); got != 1 {
		t.Fatal("raw ack without atomic proxy obligation")
	}
	// A bulk repricing behind the completed raw turn must still reach the final cost view.
	workerSQL(t, a.db, `UPDATE tmx.requests SET cost_usd=0.75 WHERE trace_id='slow-legacy-trace'`)
	timer := time.NewTimer(31 * time.Second)
	select {
	case err := <-result:
		t.Fatalf("legacy reconcile completed before release: %v", err)
	case <-timer.C:
	}
	if err = lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("legacy reconcile did not finish after lock release")
	}
	an.worker.hook = nil
	drain(t, an)
	var cost float64
	if err = an.db.QueryRow(`SELECT cost_usd FROM request_costs WHERE request_id=(SELECT id FROM requests WHERE trace_id='slow-legacy-trace')`).Scan(&cost); err != nil || cost != 0.75 {
		t.Fatalf("legacy cost=%v err=%v", cost, err)
	}
	// Startup full link refresh retains lifecycle cancellation, with no new per-call ceiling.
	workerSQL(t, an.db, `TRUNCATE request_sessions`)
	an.links = linkDirty{}
	lock, err = an.db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = lock.Exec(`LOCK TABLE request_sessions IN SHARE MODE`); err != nil {
		t.Fatal(err)
	}
	linksDone := make(chan struct{})
	go func() { an.refreshLinks(true); close(linksDone) }()
	timer = time.NewTimer(11 * time.Second)
	select {
	case <-linksDone:
		t.Fatal("full link refresh returned before release")
	case <-timer.C:
	}
	if err = lock.Rollback(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-linksDone:
	case <-time.After(5 * time.Second):
		t.Fatal("full links did not finish")
	}
	if an.links.full {
		t.Fatal("legacy full refresh deferred indefinitely")
	}
	assertLinksMatch(t, an)
	if got := workerCount(t, an, `SELECT count(*) FROM request_sessions WHERE canonical_session_id='proxy:client'`); got != 1 {
		t.Fatal("canonical link differs")
	}
	if got := workerCount(t, an, `SELECT count(*) FROM sessions WHERE id='proxy:client' AND harness='claude-code' AND machine='owned'`); got != 1 {
		t.Fatal("legacy attribution differs")
	}
	if err = an.db.QueryRow(`SELECT cost_usd FROM session_usage WHERE session_id='proxy:client'`).Scan(&cost); err != nil || cost != 0.75 {
		t.Fatalf("session rollup=%v err=%v", cost, err)
	}
	t.Log("raw exact ACK preceded blocked maintenance; legacy reconcile exceeded 30s and full links exceeded 10s, final cost/link/attribution equality passed")
}
