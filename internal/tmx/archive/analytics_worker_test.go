package archive

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

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

func TestRawWorkerShutdownFinalizesBeforeDBClose(t *testing.T) {
	for _, phase := range []string{"work_proxy", "finalize"} {
		t.Run(phase, func(t *testing.T) {
			_, an := ownedWorker(t)
			e := queueWork(t, an, "proxy", "stopping", 0)
			entered := make(chan struct{})
			release := make(chan struct{})
			var snapshot map[int64]bool
			var hookErr error
			an.worker.hook = func(ctx context.Context, code string) error {
				if code == phase && (phase != "finalize" || an.worker.phase.Load() == "finalize" && workerCount(t, an, `SELECT count(*) FROM `+journalTable+` WHERE id=$1`, e.id) == 1) {
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
			// Reopen the analytics connection and worker state, rather than clearing the old
			// stopping flag. Incomplete work survives shutdown and replays on a real restart.
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
		})
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
