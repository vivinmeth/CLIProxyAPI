package archive

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
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
	a, an := openTestAnalytics(t)
	l := defaultWorkerLimits
	l.pass = time.Second
	l.raw = 200 * time.Millisecond
	l.expensive = 200 * time.Millisecond
	l.finalize = 300 * time.Millisecond
	l.links = 200 * time.Millisecond
	an.worker.limits = &l
	t.Cleanup(an.cancel)
	return a, an
}

func TestBoundedWorkerFreshFinalizeAndUnstarted(t *testing.T) {
	_, an := ownedWorker(t)
	first := queueWork(t, an, "session", "claude-code:first", 0)
	second := queueWork(t, an, "proxy", "unstarted", 0)
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
	if an.processed.Load() != 0 || an.worker.deferred.Load() != 1 || an.worker.passes.Load() != 1 {
		t.Fatal("wrong completion counters")
	}
}

func TestBoundedWorkerOutcomesAndRetry(t *testing.T) {
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
func TestBoundedWorkerFinalizationErrorsRetainObligations(t *testing.T) {
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
			if err := an.finalizeWork(context.Background(), []*journalUnit{u}); err == nil {
				t.Fatal("fault accepted")
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

func TestBoundedWorkerExchangeHandoffCrashReplay(t *testing.T) {
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

func TestBoundedWorkerFailedRecordCrashReplay(t *testing.T) {
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

func TestBoundedWorkerFairClassesAndPoison(t *testing.T) {
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

func TestBoundedWorkerRealLockBudgets(t *testing.T) {
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
				an.refreshLinksContext(ctx, true)
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

func TestBoundedWorkerLinksBacklogDirtyRestart(t *testing.T) {
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
	if !an.links.full || an.links.size() != 0 {
		t.Fatal("dirty map unbounded")
	}
	workerSQL(t, an.db, `TRUNCATE request_sessions`)
	an.links = linkDirty{} // restart loses RAM, the initial full refresh restores it
	an.refreshLinks(false)
	assertLinksMatch(t, an)
	if n := workerCount(t, an, `SELECT count(*) FROM request_sessions`); n != 1 {
		t.Fatal("startup full refresh lost")
	}
}

func TestBoundedWorkerShutdownFinalizesBeforeDBClose(t *testing.T) {
	for _, phase := range []string{"work_proxy", "finalize"} {
		t.Run(phase, func(t *testing.T) {
			_, an := ownedWorker(t)
			e := queueWork(t, an, "proxy", "stopping", 0)
			entered := make(chan struct{})
			release := make(chan struct{})
			var snapshot map[int64]bool
			var hookErr error
			an.worker.hook = func(ctx context.Context, code string) error {
				if code == phase {
					close(entered)
					if phase == "work_proxy" {
						<-ctx.Done()
						return ctx.Err()
					}
					select {
					case <-release:
					case <-ctx.Done():
						hookErr = ctx.Err()
						return hookErr
					}
					hookErr = an.db.PingContext(ctx)
					if hookErr != nil {
						return hookErr
					}
					var rows *sql.Rows
					rows, hookErr = an.a.db.QueryContext(ctx, `SELECT id FROM `+journalTable)
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
			if phase == "finalize" {
				select {
				case <-an.ctx.Done(): // close sets stopping before cancelling admission
				case <-time.After(2 * time.Second):
					t.Fatal("close did not stop admission")
				}
				close(release)
			}
			select {
			case <-closed:
			case <-time.After(2 * time.Second):
				t.Fatal("cooperative close unbounded")
			}
			if _, err := an.step(); err != nil {
				t.Fatal(err)
			}
			if n := workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id); phase == "work_proxy" && n != 1 {
				t.Fatal("shutdown lost incomplete work")
			}
			if phase == "finalize" {
				if hookErr != nil {
					t.Fatalf("finalize hook: %v", hookErr)
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
				if an.worker.finalizeFailures.Load() != 0 || an.worker.finalizeCode.Load() != "ok" {
					t.Fatalf("finalization failures=%d code=%v", an.worker.finalizeFailures.Load(), an.worker.finalizeCode.Load())
				}
				t.Logf("processed=%d acknowledged=%d snapshot=%d remaining=%d", an.processed.Load(), acked, len(snapshot), len(remaining))
			}
			// run has returned and all synchronous work has joined before the DB closes.
			select {
			case <-an.done:
			default:
				t.Fatal("run goroutine abandoned")
			}
			if runtime.NumGoroutine() > before+4 {
				t.Fatal("worker goroutine growth")
			}
		})
	}
}
