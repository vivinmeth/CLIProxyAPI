package archive

// Serial journal execution with exact selected-ID outcomes. Raw work is finalized before
// legacy maintenance; analytics writes and primary acknowledgments are separate commits.
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

type workerLimits struct{ legacy, raw, expensive, finalize, deferFor time.Duration }

var defaultWorkerLimits = workerLimits{30 * time.Minute, 15 * time.Second, 30 * time.Second, 10 * time.Second, 60 * time.Second}

type workerState struct {
	limits                                                                 *workerLimits // immutable once run starts; shortened only by owned tests
	stopping                                                               atomic.Bool
	passes, completedAt, deferred, unknown, unknownKinds, finalizeFailures atomic.Int64
	phase, finalizeCode                                                    atomic.Value // fixed safe codes only
	phaseAt                                                                atomic.Int64
	// Synchronous test boundary, never an abandoned-work goroutine.
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

// Error() contains only our fixed stage, class and SQLSTATE. Unwrap retains the private
// cause for in-process diagnosis; neither status nor logs format the underlying message.
type workerError struct {
	stage, class, state string
	cause               error
}

func (e *workerError) Error() string {
	code := e.stage + ":" + e.class
	if e.state != "" {
		code += ":" + e.state
	}
	return code
}
func (e *workerError) Unwrap() error { return e.cause }
func safeWorkerError(stage string, err error) error {
	var safe *workerError
	if errors.As(err, &safe) {
		return safe
	}
	class := "error"
	var unknown *unknownCommit
	switch {
	case errors.As(err, &unknown):
		class = "unknown_commit_" + commitCause(unknown.err)
	case errors.Is(err, context.DeadlineExceeded):
		class = "timeout"
	case errors.Is(err, context.Canceled):
		class = "canceled"
	case lostConnection(err):
		class = "connection"
	}
	state := ""
	var pg interface{ SQLState() string }
	if errors.As(err, &pg) {
		candidate := pg.SQLState()
		if len(candidate) == 5 && strings.IndexFunc(candidate, func(r rune) bool { return !(r >= '0' && r <= '9' || r >= 'A' && r <= 'Z') }) == -1 {
			state = candidate
		}
	}
	return &workerError{stage: stage, class: class, state: state, cause: err}
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

// commitUnit commits a work unit's analytics transaction (session, thread, commit and PR
// extraction). Every COMMIT error is an unknown outcome. The owned-test boundary runs after a
// COMMIT the server applied, so a test can report a lost reply while the data has landed.
func (an *analytics) commitUnit(ctx context.Context, tx *sql.Tx) error {
	if err := commitWork(tx); err != nil {
		return err
	}
	if err := an.boundary(ctx, "unit_committed"); err != nil {
		return &unknownCommit{err}
	}
	return nil
}

// lostConnection reports a broken connection. The server may have applied the write whose
// reply was lost (an autocommit statement, or a COMMIT), so the outcome is unknown.
func lostConnection(err error) bool {
	var network net.Error
	return errors.Is(err, driver.ErrBadConn) || errors.As(err, &network) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

// commitCause names a failed COMMIT with a fixed token for diagnosis only. Every case stays an
// unknown outcome and retains its obligations; none is treated as definitely not committed.
// A bare context.Canceled is left to "other": pgx returns it mid-flight as well as before send.
func commitCause(err error) string {
	var network net.Error
	switch {
	case errors.Is(err, sql.ErrTxDone):
		return "tx_done" // database/sql rolled back after its context ended
	case err == context.DeadlineExceeded, pgconn.SafeToRetry(err):
		return "presend" // database/sql's pre-commit check, or pgconn sent nothing
	case pgconn.Timeout(err):
		return "inflight_timeout"
	case errors.Is(err, driver.ErrBadConn), errors.As(err, &network) && !network.Timeout(), errors.Is(err, io.EOF), errors.Is(err, io.ErrUnexpectedEOF):
		return "conn"
	}
	return "other"
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
	// Only a definite error becomes workFailed, the one outcome that consumes an attempt and,
	// at the ceiling, dead-letters and acknowledges. An unknown COMMIT or a lost connection is
	// retained like a deferral: not_before moves by deferFor, attempts stay unchanged, and it
	// never reaches the ceiling.
	var unknown *unknownCommit
	switch {
	case errors.As(err, &unknown):
		u.outcome = workUnknownCommit
	case err == nil:
		u.outcome = workCompleted
	case ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
		u.outcome = workDeferred
	case lostConnection(err):
		u.outcome = workUnknownCommit
	default:
		u.outcome = workFailed
	}
}

// Raw classes have reserved selection windows and commit their bookkeeping first. Only
// then does the serial worker enter legacy maintenance. Full request reconciliation keeps
// its original 30-minute context, and link/attribution refresh keeps its lifecycle context.
// Those whole-table operations can still delay a subsequent raw turn.
func (an *analytics) step() (bool, error) {
	an.mu.Lock()
	defer an.mu.Unlock()
	if an.worker.stopping.Load() || an.ctx.Err() != nil {
		return false, nil
	}
	an.lastRunUnix.Store(time.Now().Unix())
	an.phase("select")
	defer an.phase("idle")
	ctx, cancel := context.WithTimeout(an.ctx, an.limits().legacy)
	defer cancel()
	entries, err := an.selectWork(ctx)
	if err != nil {
		return false, safeWorkerError("select", err)
	}
	classes := makeUnits(entries)
	var unknownKinds int64
	for kind, us := range classes {
		known := kind == "request" || kind == "exchange"
		for _, k := range expensiveKinds {
			known = known || kind == k
		}
		if !known {
			for _, u := range us {
				unknownKinds += int64(len(u.entries))
			}
		}
	}
	an.worker.unknownKinds.Store(unknownKinds) // selected snapshot gauge, never an error/event count
	if len(entries) == 0 {
		return false, nil
	}

	var raw []*journalUnit
	for _, kind := range []string{"request", "exchange"} {
		// Selection bounds this batch by IDs, including duplicate refs.
		if len(classes[kind]) == 0 {
			continue
		}
		u := &journalUnit{}
		for _, group := range classes[kind] {
			u.entries = append(u.entries, group.entries...)
		}
		raw = append(raw, u)
		if ctx.Err() != nil || an.worker.stopping.Load() {
			break
		}
		an.phase("raw")
		unitCtx, unitCancel := context.WithTimeout(ctx, an.limits().raw)
		an.executeUnit(unitCtx, u)
		unitCancel()
		an.keepLinkDirt(u)
	}
	if err = an.finalizePhase(raw); err != nil {
		return false, err
	}
	if err = an.boundary(ctx, "raw_finalized"); err != nil {
		return false, safeWorkerError("raw_boundary", err)
	}

	// Keep the legacy whole reconcile first in maintenance, with the full original budget.
	// Other extraction units retain bounded individual turns and explicit outcomes.
	legacyCtx, legacyCancel := context.WithTimeout(an.ctx, an.limits().legacy)
	defer legacyCancel()
	var legacy []*journalUnit
	for _, kind := range []string{"requests_truncate", "thread", "commit", "prompt", "attachment", "pull_request", "session", "proxy"} {
		for _, u := range classes[kind] {
			legacy = append(legacy, u)
			if legacyCtx.Err() != nil || an.worker.stopping.Load() {
				continue
			}
			an.phase("maintenance")
			unitCtx := legacyCtx
			unitCancel := func() {}
			if kind != "requests_truncate" {
				unitCtx, unitCancel = context.WithTimeout(legacyCtx, an.limits().expensive)
			}
			an.executeUnit(unitCtx, u)
			unitCancel()
			an.keepLinkDirt(u)
		}
	}
	if err = an.finalizePhase(legacy); err != nil {
		return false, err
	}
	an.worker.passes.Add(1)
	an.worker.completedAt.Store(time.Now().Unix())
	// An exchange ack may have inserted proxy obligations; another selection sees those.
	return len(raw)+len(legacy) > 0 && !an.worker.stopping.Load(), nil
}

func (an *analytics) keepLinkDirt(u *journalUnit) {
	// Mark before primary commit, since an acknowledged-but-unknown commit cannot be
	// replayed to recover RAM dirt. A restart independently schedules a full link refresh.
	if u.outcome == workCompleted {
		for _, e := range u.entries {
			an.markLinks(e.kind, e.ref)
		}
	}
	if u.outcome == workFailed {
		an.setErr(safeWorkerError("work", u.err))
	}
}

func (an *analytics) finalizePhase(units []*journalUnit) error {
	admitted := false
	for _, u := range units {
		admitted = admitted || u.outcome != workUnstarted
	}
	if !admitted {
		return nil
	}
	an.phase("finalize")
	// Shutdown cancels work, but does not inherit an expired work context. Close joins the
	// worker before closing either database. Context-blind local calls can delay this point.
	ctx, cancel := context.WithTimeout(context.Background(), an.limits().finalize)
	defer cancel()
	if err := an.finalizeWork(ctx, units); err != nil {
		an.worker.finalizeFailures.Add(1)
		return err
	}
	return nil
}

// finalizeWork executes at most six set-based writes, plus BEGIN/COMMIT: terminal records,
// old-failure cleanup, deferred rows, retries, proxy obligations and exact acknowledgments.
// A terminal analytics record and successful cleanup precede every primary acknowledgment.
func (an *analytics) finalizeWork(ctx context.Context, units []*journalUnit) error {
	fail := func(stage string, err error) error {
		safe := safeWorkerError(stage, err)
		an.worker.finalizeCode.Store(safe.Error())
		return safe
	}
	if err := an.boundary(ctx, "finalize"); err != nil {
		return fail("finalize_unavailable", err)
	}
	var done, deferred, retries, failedIDs, failedKinds, failedRefs, failedErrors, clearKinds, clearRefs []string
	var unknownIDs []string
	keys := map[string]bool{}
	for _, u := range units {
		for _, e := range u.entries {
			id := fmt.Sprint(e.id)
			switch u.outcome {
			case workCompleted:
				done = append(done, id)
				for _, k := range u.proxyKeys {
					keys[k] = true
				}
				switch e.kind {
				case "session", "proxy", "thread", "commit", "prompt", "attachment", "pull_request":
					clearKinds = append(clearKinds, e.kind)
					clearRefs = append(clearRefs, e.ref)
				}
			case workDeferred, workUnknownCommit:
				deferred = append(deferred, id)
				if u.outcome == workUnknownCommit {
					unknownIDs = append(unknownIDs, id)
				}
			case workFailed:
				if e.attempts+1 >= maxAttempts {
					failedIDs = append(failedIDs, id)
					failedKinds = append(failedKinds, e.kind)
					failedRefs = append(failedRefs, e.ref)
					failedErrors = append(failedErrors, safeWorkerError("work", u.err).Error())
					done = append(done, id)
				} else {
					retries = append(retries, id)
				}
			}
		}
	}
	idsArray := func(ids []string) string { return "{" + strings.Join(ids, ",") + "}" }
	if len(failedIDs) > 0 {
		if err := an.boundary(ctx, "sql_failed_record"); err != nil {
			return fail("finalize_failed_record", err)
		}
		if _, err := an.db.ExecContext(ctx, `INSERT INTO failed (journal_id,kind,ref,error)
   SELECT * FROM unnest($1::bigint[],$2::text[],$3::text[],$4::text[])
   ON CONFLICT (journal_id) DO UPDATE SET error=EXCLUDED.error,failed_at=now()`,
			idsArray(failedIDs), pgTextArray(failedKinds), pgTextArray(failedRefs), pgTextArray(failedErrors)); err != nil {
			return fail("finalize_failed_record", err)
		}
		if err := an.boundary(ctx, "failed_record_committed"); err != nil {
			return fail("finalize_failed_record_unknown", err)
		}
	}
	if len(clearKinds) > 0 {
		if err := an.boundary(ctx, "sql_cleanup"); err != nil {
			return fail("finalize_cleanup", err)
		}
		if _, err := an.db.ExecContext(ctx, `DELETE FROM failed f USING unnest($1::text[],$2::text[]) c(kind,ref)
   WHERE f.kind=c.kind AND f.ref=c.ref`, pgTextArray(clearKinds), pgTextArray(clearRefs)); err != nil {
			return fail("finalize_cleanup", err)
		}
	}
	tx, err := an.a.db.BeginTx(ctx, nil)
	if err != nil {
		return fail("finalize_primary_begin", err)
	}
	defer func() { _ = tx.Rollback() }()
	var deferredCount, unknownCount int64
	if len(deferred) > 0 {
		if err = an.boundary(ctx, "sql_defer"); err != nil {
			return fail("finalize_defer", err)
		}
		// RETURNING counts only rows actually retained; attempts remain unchanged.
		rows, errD := tx.QueryContext(ctx, `UPDATE `+journalTable+` SET not_before=now()+($2*interval '1 second')
   WHERE id=ANY($1::bigint[]) RETURNING id`, idsArray(deferred), an.limits().deferFor.Seconds())
		if errD != nil {
			return fail("finalize_defer", errD)
		}
		unknownSet := map[string]bool{}
		for _, id := range unknownIDs {
			unknownSet[id] = true
		}
		for rows.Next() {
			var id int64
			if errD = rows.Scan(&id); errD != nil {
				_ = rows.Close()
				return fail("finalize_defer", errD)
			}
			deferredCount++
			if unknownSet[fmt.Sprint(id)] {
				unknownCount++
			}
		}
		if errD = closeRows(rows); errD != nil {
			return fail("finalize_defer", errD)
		}
	}
	if len(retries) > 0 {
		if err = an.boundary(ctx, "sql_retry"); err != nil {
			return fail("finalize_retry", err)
		}
		if _, err = tx.ExecContext(ctx, `UPDATE `+journalTable+` SET attempts=attempts+1,
   not_before=now()+make_interval(mins=>attempts+1) WHERE id=ANY($1::bigint[])`, idsArray(retries)); err != nil {
			return fail("finalize_retry", err)
		}
	}
	if len(keys) > 0 {
		ordered := make([]string, 0, len(keys))
		for k := range keys {
			ordered = append(ordered, k)
		}
		sort.Strings(ordered)
		if err = an.boundary(ctx, "sql_proxy_handoff"); err != nil {
			return fail("finalize_proxy_handoff", err)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+journalTable+` (kind,ref) SELECT 'proxy',unnest($1::text[])`, pgTextArray(ordered)); err != nil {
			return fail("finalize_proxy_handoff", err)
		}
	}
	var acknowledged int64
	if len(done) > 0 {
		if err = an.boundary(ctx, "sql_ack"); err != nil {
			return fail("finalize_ack", err)
		}
		result, errA := tx.ExecContext(ctx, `DELETE FROM `+journalTable+` WHERE id=ANY($1::bigint[])`, idsArray(done))
		if errA != nil {
			return fail("finalize_ack", errA)
		}
		acknowledged, errA = result.RowsAffected()
		if errA != nil {
			return fail("finalize_ack", errA)
		}
	}
	if err = an.boundary(ctx, "primary_before_commit"); err != nil {
		return fail("finalize_primary_unknown", err)
	}
	if err = commitWork(tx); err != nil {
		return fail("finalize_primary_unknown", err)
	}
	if err = an.boundary(ctx, "primary_committed"); err != nil {
		return fail("finalize_primary_unknown", err)
	}
	an.processed.Add(acknowledged)
	an.worker.deferred.Add(deferredCount)
	an.worker.unknown.Add(unknownCount)
	for _, u := range units {
		if u.outcome == workCompleted && u.entries[0].kind == "proxy" {
			an.extracted.Add(1)
		}
	}
	an.worker.finalizeCode.Store("ok")
	return nil
}
