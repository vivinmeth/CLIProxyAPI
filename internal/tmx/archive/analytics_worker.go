package archive

// Bounded serial journal execution. Only selected IDs with an explicit durable outcome may
// leave the primary queue. Work and finalization deliberately have separate deadlines.
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

type workerLimits struct {
	pass, raw, expensive, finalize, links, deferFor time.Duration
}

var defaultWorkerLimits = workerLimits{120 * time.Second, 15 * time.Second, 30 * time.Second,
	10 * time.Second, 10 * time.Second, 60 * time.Second}

type workerState struct {
	limits                                                   *workerLimits // immutable once run starts; tests use short budgets
	nextClass                                                int           // owned by mu, including direct step callers
	stopping                                                 atomic.Bool
	drainUntil                                               atomic.Int64
	passes, completedAt, deferred, unknown, finalizeFailures atomic.Int64
	phase                                                    atomic.Value // fixed code, no refs or errors
	phaseAt                                                  atomic.Int64
	finalizeCode                                             atomic.Value
	linksPending                                             atomic.Bool
	// Test hook runs synchronously at durable boundaries. Never used to run work in a goroutine.
	hook func(context.Context, string) error
}

func (an *analytics) limits() workerLimits {
	if an.worker.limits != nil {
		return *an.worker.limits
	}
	return defaultWorkerLimits
}

func (an *analytics) phase(code string) {
	an.worker.phaseAt.Store(time.Now().Unix())
	an.worker.phase.Store(code)
}

func (an *analytics) boundary(ctx context.Context, code string) error {
	if an.worker.hook != nil {
		return an.worker.hook(ctx, code)
	}
	return nil
}

type journalOutcome uint8

const (
	workUnstarted journalOutcome = iota
	workCompleted
	workDeferred
	workFailed
	workUnknownCommit
)

// A unit owns exactly these selected entries, even if a newer same-ref row arrives during work.
type journalUnit struct {
	entries   []journalEntry
	outcome   journalOutcome
	err       error
	proxyKeys []string
}

// A failed COMMIT may have committed. It must be replayed without acknowledging or consuming
// an ordinary-error attempt. All state writes are idempotent; no cross-database transaction exists.
type unknownCommit struct{ err error }

func (e *unknownCommit) Error() string { return "analytics commit outcome unknown" }
func (e *unknownCommit) Unwrap() error { return e.err }
func commitWork(tx *sql.Tx) error {
	if err := tx.Commit(); err != nil {
		return &unknownCommit{err}
	}
	return nil
}

var expensiveKinds = []string{"session", "proxy", "requests_truncate", "thread", "commit", "prompt", "attachment", "pull_request"}

// Each class gets a reserved selection window, so expensive work cannot hide raw mirrors.
// Ten non-session windows (including unknown kinds) sum to journalBatch; session retains its
// sessionBatch ceiling. Explicit ordering applies inside each class and over the result.
func (an *analytics) selectWork(ctx context.Context) ([]journalEntry, error) {
	rows, err := an.a.db.QueryContext(ctx, `WITH classes AS (
		SELECT unnest(ARRAY['request','exchange','proxy','requests_truncate','thread','commit',
			'prompt','attachment','pull_request','session','?']) AS kind
	) SELECT j.id, j.kind, j.ref, j.attempts FROM classes c CROSS JOIN LATERAL (
		SELECT id, kind, ref, attempts FROM `+journalTable+`
		WHERE (kind = c.kind OR (c.kind = '?' AND kind NOT IN
			('request','exchange','proxy','requests_truncate','thread','commit','prompt','attachment','pull_request','session')))
		AND (not_before IS NULL OR not_before <= now()) ORDER BY id
		LIMIT CASE WHEN c.kind = 'session' THEN $1::bigint ELSE $2::bigint END
	) j ORDER BY j.id`, sessionBatch, journalBatch/10)
	if err != nil {
		return nil, err
	}
	var entries []journalEntry
	for rows.Next() {
		var e journalEntry
		if err = rows.Scan(&e.id, &e.kind, &e.ref, &e.attempts); err != nil {
			_ = rows.Close()
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, closeRows(rows)
}

// Units and refs keep first-ID order; maps are only used to coalesce, never to schedule.
func makeUnits(entries []journalEntry) map[string][]*journalUnit {
	classes := map[string][]*journalUnit{}
	byRef := map[string]*journalUnit{}
	for _, e := range entries {
		key := e.kind + "\x00" + e.ref
		u := byRef[key]
		if u == nil {
			u = &journalUnit{}
			byRef[key] = u
			classes[e.kind] = append(classes[e.kind], u)
		}
		u.entries = append(u.entries, e)
	}
	return classes
}

func (an *analytics) executeUnit(ctx context.Context, u *journalUnit) {
	e := u.entries[0]
	refs := make([]string, 0, len(u.entries))
	seen := map[string]bool{}
	for _, entry := range u.entries {
		if !seen[entry.ref] {
			refs = append(refs, entry.ref)
			seen[entry.ref] = true
		}
	}
	err := an.boundary(ctx, "work_"+e.kind)
	if err == nil {
		switch e.kind {
		case "request":
			err = an.syncRequestRows(ctx, refs)
		case "exchange":
			_, err = an.exchMirror.copy(ctx, `trace_id = ANY($1::text[])`, pgArray(refs))
			if err == nil {
				err = an.boundary(ctx, "mirror_committed")
			}
			if err == nil {
				u.proxyKeys, err = an.proxyKeysForTraces(ctx, refs)
			}
		case "requests_truncate":
			_, err = an.reconcileAllRequests(ctx)
		case "session":
			err = an.extractImportedBatch(ctx, refs)[e.ref]
		case "proxy":
			err = an.extractProxy(ctx, e.ref)
		case "thread":
			err = an.extractThread(ctx, e.ref)
		case "commit":
			err = an.extractCommit(ctx, e.ref)
		case "prompt":
			err = an.extractPrompts(ctx, refs)[e.ref]
		case "attachment":
			_, err = an.attMirror.copy(ctx, `id = ANY($1::text[])`, pgTextArray(refs))
		case "pull_request":
			err = an.extractPullRequest(ctx, e.ref)
		default:
			return // unknown kinds retain their obligations
		}
	}
	u.err = err
	var unknown *unknownCommit
	var network net.Error
	switch {
	case errors.As(err, &unknown):
		u.outcome = workUnknownCommit
	case err == nil:
		u.outcome = workCompleted
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		u.outcome = workDeferred
	case errors.Is(err, driver.ErrBadConn) || errors.As(err, &network):
		u.outcome = workUnknownCommit
	default:
		u.outcome = workFailed
	}
}

// step never credits selected-but-unstarted work. A pass reserves both raw turns, then rotates
// expensive classes and refs. At most one expensive unit is admitted per turn, synchronously.
func (an *analytics) step() (bool, error) {
	an.mu.Lock()
	defer an.mu.Unlock()
	if an.worker.stopping.Load() || an.ctx.Err() != nil {
		return false, nil
	}
	limits := an.limits()
	ctx, cancel := context.WithTimeout(an.ctx, limits.pass)
	defer cancel()
	an.phase("select")
	defer an.phase("idle")
	an.lastRunUnix.Store(time.Now().Unix())
	entries, err := an.selectWork(ctx)
	if err != nil {
		return false, err
	}
	classes := makeUnits(entries)
	var units []*journalUnit
	for _, kind := range []string{"request", "exchange"} {
		// A raw unit is bounded by 100 selected IDs (not merely distinct refs).
		var batch *journalUnit
		for _, u := range classes[kind] {
			for _, e := range u.entries {
				if batch == nil || len(batch.entries) == 100 {
					batch = &journalUnit{}
					units = append(units, batch)
				}
				batch.entries = append(batch.entries, e)
			}
		}
	}
	// Visit every class in rotation, then repeat with the next ref from each class.
	start := an.worker.nextClass
	an.worker.nextClass = (start + 1) % len(expensiveKinds)
	for remaining := true; remaining; {
		remaining = false
		for i := range expensiveKinds {
			kind := expensiveKinds[(start+i)%len(expensiveKinds)]
			if len(classes[kind]) > 0 {
				units = append(units, classes[kind][0])
				classes[kind] = classes[kind][1:]
				remaining = true
			}
		}
	}
	// Keep unknown entries explicit and unstarted. They are neither acknowledged nor failed.
	for _, e := range entries {
		known := e.kind == "request" || e.kind == "exchange"
		for _, k := range expensiveKinds {
			known = known || e.kind == k
		}
		if !known {
			an.lastErr.Store("unknown_kind")
			an.worker.unknown.Add(1)
		}
	}
	for _, u := range units {
		if ctx.Err() != nil || an.worker.stopping.Load() {
			break
		}
		budget := limits.expensive
		if k := u.entries[0].kind; k == "request" || k == "exchange" {
			budget = limits.raw
		}
		an.phase("work")
		unitCtx, unitCancel := context.WithTimeout(ctx, budget)
		an.executeUnit(unitCtx, u)
		// Analytics work is already durable. Keep link dirt even if the primary COMMIT later
		// succeeds but its reply is lost, leaving no journal row to mark it on the next pass.
		if u.outcome == workCompleted {
			for _, e := range u.entries {
				an.markLinks(e.kind, e.ref)
			}
		}
		if u.outcome == workFailed {
			an.setErr(errors.New("work_failed"))
		}
		unitCancel()
	}
	cancel()
	an.phase("finalize")
	// If stop preceded finalization, cap this fresh deadline by the remaining drain allowance.
	// Stop during finalization leaves its already-running timeout in force.
	finalCtx, finalCancel := context.WithTimeout(context.Background(), limits.finalize)
	defer finalCancel()
	if n := an.worker.drainUntil.Load(); n != 0 {
		var drainCancel context.CancelFunc
		finalCtx, drainCancel = context.WithDeadline(finalCtx, time.Unix(0, n))
		defer drainCancel()
	}
	if err = an.finalizeWork(finalCtx, units); err != nil {
		an.worker.finalizeFailures.Add(1)
		an.phase("idle")
		return false, err
	}
	an.worker.passes.Add(1)
	an.worker.completedAt.Store(time.Now().Unix())
	an.phase("idle")
	// A successful exchange handoff can have just created proxy jobs. Ask for another turn
	// after any admitted batch; the following empty selection cheaply ends a burst.
	return len(units) > 0 && !an.worker.stopping.Load(), nil
}

func (an *analytics) finalizeWork(ctx context.Context, units []*journalUnit) error {
	fail := func(code string) error { an.worker.finalizeCode.Store(code); return errors.New(code) }
	if err := an.boundary(ctx, "finalize"); err != nil {
		return fail("finalize_unavailable")
	}
	// Terminal failures must exist durably in analytics before the source may disappear.
	// Cleanup happens before source ack, after successful analytics work. If it fails, source
	// entries remain retryable. Exchange handoff does not clear session/proxy failures.
	for _, u := range units {
		for _, e := range u.entries {
			if u.outcome == workFailed && e.attempts+1 >= maxAttempts {
				if _, err := an.db.ExecContext(ctx, `INSERT INTO failed (journal_id,kind,ref,error) VALUES ($1,$2,$3,$4)
					ON CONFLICT (journal_id) DO UPDATE SET error=EXCLUDED.error, failed_at=now()`,
					e.id, e.kind, e.ref, cleanText(u.err.Error())); err != nil {
					return fail("finalize_failed_record")
				}
				if err := an.boundary(ctx, "failed_record_committed"); err != nil {
					return fail("finalize_failed_record_unknown")
				}
			}
		}
		if u.outcome == workCompleted {
			e := u.entries[0]
			if e.kind != "request" && e.kind != "exchange" && e.kind != "requests_truncate" {
				if _, err := an.db.ExecContext(ctx, `DELETE FROM failed WHERE kind=$1 AND ref=$2`, e.kind, e.ref); err != nil {
					return fail("finalize_cleanup")
				}
			}
		}
	}
	tx, err := an.a.db.BeginTx(ctx, nil)
	if err != nil {
		return fail("finalize_primary_begin")
	}
	defer func() { _ = tx.Rollback() }()
	var done []string
	var deferred, unknown int64
	keys := map[string]bool{}
	for _, u := range units {
		for _, e := range u.entries {
			switch u.outcome {
			case workCompleted:
				done = append(done, fmt.Sprint(e.id))
				for _, key := range u.proxyKeys {
					keys[key] = true
				}
			case workDeferred, workUnknownCommit:
				if _, err = tx.ExecContext(ctx, `UPDATE `+journalTable+` SET not_before=now()+($2*interval '1 second') WHERE id=$1`, e.id, an.limits().deferFor.Seconds()); err != nil {
					return fail("finalize_defer")
				}
				deferred++
				if u.outcome == workUnknownCommit {
					unknown++
				}
			case workFailed:
				if e.attempts+1 >= maxAttempts {
					done = append(done, fmt.Sprint(e.id))
					continue
				}
				if _, err = tx.ExecContext(ctx, `UPDATE `+journalTable+` SET attempts=attempts+1,
					not_before=now()+make_interval(mins=>attempts+1) WHERE id=$1`, e.id); err != nil {
					return fail("finalize_retry")
				}
			}
		}
	}
	orderedKeys := make([]string, 0, len(keys))
	for key := range keys {
		orderedKeys = append(orderedKeys, key)
	}
	sort.Strings(orderedKeys)
	for _, key := range orderedKeys {
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+journalTable+` (kind,ref) VALUES ('proxy',$1)`, key); err != nil {
			return fail("finalize_proxy_handoff")
		}
	}
	if len(done) > 0 {
		if _, err = tx.ExecContext(ctx, `DELETE FROM `+journalTable+` WHERE id=ANY($1::bigint[])`, "{"+strings.Join(done, ",")+"}"); err != nil {
			return fail("finalize_ack")
		}
	}
	if err = an.boundary(ctx, "primary_before_commit"); err != nil {
		return fail("finalize_primary_unknown")
	}
	if err = tx.Commit(); err != nil {
		return fail("finalize_primary_unknown")
	}
	if err = an.boundary(ctx, "primary_committed"); err != nil {
		return fail("finalize_primary_unknown")
	}
	an.processed.Add(int64(len(done)))
	an.worker.deferred.Add(deferred)
	an.worker.unknown.Add(unknown)
	for _, u := range units {
		if u.outcome == workCompleted {
			for _, e := range u.entries {
				an.markLinks(e.kind, e.ref)
			}
			if u.entries[0].kind == "proxy" {
				an.extracted.Add(1)
			}
		}
	}
	an.worker.finalizeCode.Store("ok")
	return nil
}
