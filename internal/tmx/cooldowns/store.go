package cooldowns

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

const (
	observeQueueSize  = 8192
	eventQueueSize    = 10000
	batchSize         = 200
	flushInterval     = 2 * time.Second
	expireInterval    = 5 * time.Second
	reconcileInterval = 15 * time.Second
	// Events whose insert failed wait in memory (up to pendingMax) and are retried with backoff;
	// while any are pending they are mirrored to a JSONL spool file that the next start reads back.
	pendingMax    = 100000
	retryMaxDelay = time.Minute
	spoolEvery    = 2 * time.Second
	spoolFileName = "tmx-cooldown-events-pending.jsonl"
)

var schemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Snapshotter is the auth manager: the full live-cooldown snapshot the tracker reconciles with.
type Snapshotter interface {
	CooldownSnapshot() auth.CooldownReport
}

type observation struct {
	report  *auth.CooldownReport
	failure *auth.FailureReport
}

// Store writes tmx.cooldown_events. It implements auth.CooldownObserver.
type Store struct {
	db        *sql.DB
	schema    string
	manager   Snapshotter
	spoolFile string

	obs    chan observation
	events chan Event
	flush  chan chan struct{}

	queueMu sync.RWMutex
	closing bool

	observed     atomic.Int64
	dropped      atomic.Int64 // observations or events lost (queue full, pending limit, closed)
	written      atomic.Int64
	writeErrors  atomic.Int64
	rejected     atomic.Int64
	pendingRows  atomic.Int64
	openWindows  atomic.Int64
	resumeErrors atomic.Int64
	trigger      atomic.Bool
	needSnapshot atomic.Bool // an observation was dropped: reconcile at the next tick

	stopOnce      sync.Once
	closeWaitOnce sync.Once
	closeDone     chan struct{}
	trackDone     chan struct{}
	writeDone     chan struct{}
}

var current atomic.Pointer[Store]

// Default returns the running store, or nil when cooldown history is off.
func Default() *Store { return current.Load() }

// SetDefaultForTest replaces the running store (tests only).
func SetDefaultForTest(s *Store) { current.Store(s) }

// Start opens the history on the request log's database (db / schema) and installs the
// observer on the auth manager. TMX_COOLDOWNS=off disables it; TMX_COOLDOWNS_TRIGGER=off skips
// the cooldown_store trigger. Failures are logged and never block startup.
func Start(ctx context.Context, db *sql.DB, schema string, manager Snapshotter) *Store {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TMX_COOLDOWNS")), "off") {
		log.Info("tmx cooldown history: disabled by TMX_COOLDOWNS=off")
		return nil
	}
	s, err := Open(ctx, db, schema, manager)
	if err != nil {
		log.WithError(err).Error("tmx cooldown history: disabled, could not open store")
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("TMX_COOLDOWNS_TRIGGER")), "off") {
		s.installTrigger(ctx, cooldownStoreTable())
	}
	current.Store(s)
	auth.SetCooldownObserver(s)
	log.Infof("tmx cooldown history: writing to %s.cooldown_events", schema)
	return s
}

// Open ensures the table, resumes open windows and starts the tracker and writer.
func Open(ctx context.Context, db *sql.DB, schema string, manager Snapshotter) (*Store, error) {
	if !schemaNamePattern.MatchString(schema) {
		return nil, fmt.Errorf("invalid schema name %q", schema)
	}
	s := &Store{
		db:        db,
		schema:    schema,
		manager:   manager,
		spoolFile: spoolPath(),
		obs:       make(chan observation, observeQueueSize),
		events:    make(chan Event, eventQueueSize),
		flush:     make(chan chan struct{}),
		trackDone: make(chan struct{}),
		writeDone: make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := s.ensureSchema(ctx); err != nil {
		return nil, err
	}
	pending := readSpool(s.spoolFile)
	t := newTracker()
	open, err := s.loadOpen(ctx)
	if err != nil {
		// Without the open windows a restored cooldown starts a second window; the old one is
		// ended as superseded the next time open windows are loaded.
		s.resumeErrors.Add(1)
		log.WithError(err).Warn("tmx cooldown history: could not load open windows")
	}
	resumed := t.resume(open, time.Now())
	for _, ev := range pending {
		t.applyEvent(ev)
	}
	go s.write(pending)
	go s.track(t, resumed)
	return s, nil
}

func (s *Store) table() string { return s.schema + ".cooldown_events" }

func (s *Store) ensureSchema(ctx context.Context) error {
	t := s.table()
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + s.schema,
		// Append-only and never pruned: the cooldown history is a long-term KPI record.
		`CREATE TABLE IF NOT EXISTS ` + t + ` (
			id            BIGSERIAL PRIMARY KEY,
			event_key     TEXT NOT NULL,
			ts            TIMESTAMPTZ NOT NULL,
			recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
			kind          TEXT NOT NULL,
			source        TEXT NOT NULL DEFAULT 'proxy',
			cooldown_id   TEXT NOT NULL DEFAULT '',
			auth_id       TEXT NOT NULL DEFAULT '',
			auth_index    TEXT NOT NULL DEFAULT '',
			auth_label    TEXT NOT NULL DEFAULT '',
			provider      TEXT NOT NULL DEFAULT '',
			model         TEXT NOT NULL DEFAULT '',
			reason        TEXT NOT NULL DEFAULT '',
			http_status   INTEGER NOT NULL DEFAULT 0,
			error_code    TEXT NOT NULL DEFAULT '',
			message       TEXT NOT NULL DEFAULT '',
			retry_at      TIMESTAMPTZ,
			retry_after_ms BIGINT NOT NULL DEFAULT 0,
			backoff_level INTEGER NOT NULL DEFAULT 0,
			credential_scope BOOLEAN NOT NULL DEFAULT FALSE,
			started_at    TIMESTAMPTZ,
			ended_at      TIMESTAMPTZ,
			end_reason    TEXT NOT NULL DEFAULT '',
			detail        JSONB
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS cooldown_events_key_idx ON ` + t + ` (event_key)`,
		`CREATE INDEX IF NOT EXISTS cooldown_events_ts_idx ON ` + t + ` (ts DESC, id DESC)`,
		`CREATE INDEX IF NOT EXISTS cooldown_events_auth_ts_idx ON ` + t + ` (auth_id, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS cooldown_events_kind_ts_idx ON ` + t + ` (kind, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS cooldown_events_cooldown_idx ON ` + t + ` (cooldown_id) WHERE cooldown_id <> ''`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

// ObserveCooldowns implements auth.CooldownObserver; it never blocks.
func (s *Store) ObserveCooldowns(rep auth.CooldownReport) {
	if !s.offer(observation{report: &rep}) {
		s.needSnapshot.Store(true)
	}
}

// ObserveFailure implements auth.CooldownObserver; it never blocks.
func (s *Store) ObserveFailure(f auth.FailureReport) {
	if FailureKind(f) == "" {
		return
	}
	s.offer(observation{failure: &f})
}

func (s *Store) offer(o observation) bool {
	if s == nil {
		return false
	}
	s.queueMu.RLock()
	defer s.queueMu.RUnlock()
	if s.closing {
		s.dropped.Add(1)
		return false
	}
	select {
	case s.obs <- o:
		s.observed.Add(1)
		return true
	default:
		if s.dropped.Add(1)%100 == 1 {
			log.Warn("tmx cooldown history: observation queue full, dropping")
		}
		return false
	}
}

// Stats reports writer counters ("dropped" is anything lost before reaching the database).
func (s *Store) Stats() map[string]int64 {
	if s == nil {
		return nil
	}
	trigger := int64(0)
	if s.trigger.Load() {
		trigger = 1
	}
	return map[string]int64{
		"observed":          s.observed.Load(),
		"written":           s.written.Load(),
		"dropped":           s.dropped.Load(),
		"write_errors":      s.writeErrors.Load(),
		"rejected":          s.rejected.Load(),
		"pending":           s.pendingRows.Load(),
		"queued":            int64(len(s.obs) + len(s.events)),
		"open_windows":      s.openWindows.Load(),
		"resume_errors":     s.resumeErrors.Load(),
		"trigger_installed": trigger,
	}
}

// track runs the tracker: reports in, events out. The first snapshot settles windows resumed from
// before a restart.
func (s *Store) track(t *tracker, resumed []Event) {
	defer close(s.trackDone)
	defer close(s.events)
	emit := func(evs []Event) {
		for _, ev := range evs {
			select {
			case s.events <- ev:
			default:
				if s.dropped.Add(1)%100 == 1 {
					log.Warn("tmx cooldown history: event queue full, dropping")
				}
			}
		}
		s.openWindows.Store(int64(len(t.open)))
	}
	emit(resumed)
	snapshot := func(first bool) {
		if s.manager == nil {
			return
		}
		s.needSnapshot.Store(false)
		rep := s.manager.CooldownSnapshot()
		if first {
			emit(t.settleRestart(rep))
		}
		emit(t.apply(rep, SourceReconcile))
	}
	snapshot(true)
	expire := time.NewTicker(expireInterval)
	defer expire.Stop()
	reconcile := time.NewTicker(reconcileInterval)
	defer reconcile.Stop()
	for {
		select {
		case o, ok := <-s.obs:
			if !ok {
				return
			}
			if o.report != nil {
				emit(t.apply(*o.report, SourceProxy))
			} else if ev, okEv := failureEvent(*o.failure); okEv {
				emit([]Event{ev})
			}
		case now := <-expire.C:
			emit(t.expire(now))
			if s.needSnapshot.Load() {
				snapshot(false)
			}
		case <-reconcile.C:
			snapshot(false)
		}
	}
}

// write is the database writer: batches, retries with backoff, spools while anything is pending.
func (s *Store) write(spooled []Event) {
	defer close(s.writeDone)
	w := &writer{s: s, pending: spooled, spooled: len(spooled) > 0}
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	batch := make([]Event, 0, batchSize)
	flush := func() {
		w.write(batch)
		batch = batch[:0]
	}
	for {
		select {
		case ev, ok := <-s.events:
			if !ok {
				flush()
				w.persistAt(time.Time{})
				return
			}
			batch = append(batch, ev)
			if len(batch) >= batchSize {
				flush()
			}
		case ack := <-s.flush:
			for n := len(s.events); n > 0; n-- {
				ev, ok := <-s.events
				if !ok {
					break
				}
				batch = append(batch, ev)
			}
			flush()
			w.persistAt(time.Time{})
			close(ack)
		case <-ticker.C:
			flush()
		}
	}
}

// FlushContext checkpoints accepted events (writes them, or spools what can't be written).
func (s *Store) FlushContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	ack := make(chan struct{})
	select {
	case s.flush <- ack:
	case <-s.writeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-ack:
		return nil
	case <-s.writeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close stops intake, writes what is queued and spools whatever the database didn't take. Open
// windows stay open: the next start resumes them. The shared database handle is not closed.
func (s *Store) Close() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		auth.SetCooldownObserver(nil)
		s.queueMu.Lock()
		s.closing = true
		close(s.obs)
		s.queueMu.Unlock()
		<-s.trackDone
		<-s.writeDone
	})
}

// CloseContext is Close bounded by the shutdown deadline.
func (s *Store) CloseContext(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.closeWaitOnce.Do(func() { s.closeDone = make(chan struct{}); go func() { s.Close(); close(s.closeDone) }() })
	select {
	case <-s.closeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

type writer struct {
	s         *Store
	pending   []Event
	nextRetry time.Time
	delay     time.Duration
	spooled   bool
	dirty     bool
	lastSpool time.Time
}

// write inserts batch after anything still pending. Only rows the database refuses for their
// content are dropped (counted as rejected); anything else is kept and retried. Inserts are
// idempotent per event_key, so a retry never duplicates.
func (w *writer) write(batch []Event) {
	s := w.s
	w.pending = append(w.pending, batch...)
	if over := len(w.pending) - pendingMax; over > 0 {
		s.dropped.Add(int64(over))
		log.Warnf("tmx cooldown history: %d unwritten events over the pending limit dropped", over)
		w.pending = append([]Event(nil), w.pending[over:]...)
	}
	before := len(w.pending)
	defer func() {
		s.pendingRows.Store(int64(len(w.pending)))
		if len(batch) > 0 || len(w.pending) != before {
			w.dirty = true
		}
		w.persistAt(time.Now())
	}()
	if len(w.pending) == 0 || time.Now().Before(w.nextRetry) {
		return
	}
	if w.delay > 0 {
		w.persistAt(time.Now())
	}
	backoff := func(err error) {
		s.writeErrors.Add(1)
		w.delay = min(max(2*w.delay, flushInterval), retryMaxDelay)
		w.nextRetry = time.Now().Add(w.delay)
		log.WithError(err).Warnf("tmx cooldown history: write failed; %d events kept for retry", len(w.pending))
	}
	for len(w.pending) > 0 {
		chunk := w.pending[:min(batchSize, len(w.pending))]
		err := s.insert(chunk)
		if err == nil {
			s.written.Add(int64(len(chunk)))
			w.pending = w.pending[len(chunk):]
			continue
		}
		if !isDataError(err) {
			backoff(err)
			return
		}
		for len(chunk) > 0 {
			errOne := s.insert(chunk[:1])
			switch {
			case errOne == nil:
				s.written.Add(1)
			case isDataError(errOne):
				s.rejected.Add(1)
				log.WithError(errOne).Warnf("tmx cooldown history: event rejected (%s %s)", chunk[0].Kind, chunk[0].AuthID)
			default:
				backoff(errOne)
				return
			}
			chunk = chunk[1:]
			w.pending = w.pending[1:]
		}
	}
	w.pending, w.delay, w.nextRetry = nil, 0, time.Time{}
}

func isDataError(err error) bool {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || len(pgErr.Code) < 2 {
		return false
	}
	switch pgErr.Code[:2] {
	case "22", "23":
		return true
	}
	return false
}

// persistAt mirrors pending events to the spool file (atomically) when they changed, at most
// every spoolEvery (now zero = regardless), and removes it once nothing is pending.
func (w *writer) persistAt(now time.Time) {
	path := w.s.spoolFile
	if len(w.pending) == 0 {
		if !w.spooled && !w.dirty {
			return
		}
		if err := os.Remove(path); err == nil || os.IsNotExist(err) {
			w.spooled, w.dirty = false, false
		}
		return
	}
	if !w.dirty || (!now.IsZero() && now.Sub(w.lastSpool) < spoolEvery) {
		return
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	for i := range w.pending {
		_ = enc.Encode(&w.pending[i])
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b.Bytes(), 0o600); err != nil {
		log.WithError(err).Warnf("tmx cooldown history: can't spool %d pending events", len(w.pending))
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.WithError(err).Warnf("tmx cooldown history: can't spool %d pending events", len(w.pending))
		return
	}
	w.spooled, w.dirty, w.lastSpool = true, false, now
}

func readSpool(path string) []Event {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var out []Event
	dec := json.NewDecoder(bytes.NewReader(raw))
	for {
		var ev Event
		if dec.Decode(&ev) != nil {
			break
		}
		out = append(out, ev)
	}
	if len(out) > 0 {
		log.Infof("tmx cooldown history: %d events from the previous run queued for writing", len(out))
	}
	return out
}

// spoolPath: TMX_COOLDOWNS_SPOOL (a directory), else the archive spool volume (persistent on
// prod), else WRITABLE_PATH, else the temp dir.
func spoolPath() string {
	for _, dir := range []string{os.Getenv("TMX_COOLDOWNS_SPOOL"), os.Getenv("TMX_ARCHIVE_SPOOL"),
		"/CLIProxyAPI/archive-spool", os.Getenv("WRITABLE_PATH")} {
		if dir = strings.TrimSpace(dir); dir == "" {
			continue
		}
		if st, err := os.Stat(dir); err == nil && st.IsDir() {
			return filepath.Join(dir, spoolFileName)
		}
	}
	return filepath.Join(os.TempDir(), spoolFileName)
}

var columnNames = []string{"event_key", "ts", "kind", "source", "cooldown_id", "auth_id", "auth_index",
	"auth_label", "provider", "model", "reason", "http_status", "error_code", "message", "retry_at",
	"retry_after_ms", "backoff_level", "credential_scope", "started_at", "ended_at", "end_reason"}

func eventFields(e *Event) []any {
	return []any{e.EventKey, e.TS, e.Kind, e.Source, e.CooldownID, e.AuthID, e.AuthIndex,
		e.AuthLabel, e.Provider, e.Model, e.Reason, e.HTTPStatus, e.ErrorCode, e.Message, e.RetryAt,
		e.RetryAfterMs, e.BackoffLevel, e.CredScope, e.StartedAt, e.EndedAt, e.EndReason}
}

func (s *Store) insert(events []Event) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var b strings.Builder
	args := make([]any, 0, len(events)*len(columnNames))
	for i := range events {
		if events[i].EventKey == "" {
			events[i].EventKey = newKey()
		}
		if events[i].Source == "" {
			events[i].Source = SourceProxy
		}
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("(")
		for c := range columnNames {
			if c > 0 {
				b.WriteString(",")
			}
			fmt.Fprintf(&b, "$%d", i*len(columnNames)+c+1)
		}
		b.WriteString(")")
		args = append(args, eventFields(&events[i])...)
	}
	_, err := s.db.ExecContext(ctx, "INSERT INTO "+s.table()+" ("+strings.Join(columnNames, ", ")+") VALUES "+
		b.String()+" ON CONFLICT (event_key) DO NOTHING", args...)
	return err
}

// loadOpen reads windows with a start and no end (and their latest update) from the database.
func (s *Store) loadOpen(ctx context.Context) ([]*openWindow, error) {
	t := s.table()
	rows, err := s.db.QueryContext(ctx, `SELECT st.cooldown_id, st.started_at, st.ts, st.auth_id, st.auth_index,
		st.auth_label, st.provider, st.model,
		COALESCE(u.reason, st.reason), COALESCE(u.http_status, st.http_status),
		COALESCE(u.error_code, st.error_code), COALESCE(u.message, st.message),
		COALESCE(u.retry_at, st.retry_at), COALESCE(u.backoff_level, st.backoff_level)
		FROM `+t+` st
		LEFT JOIN LATERAL (SELECT reason, http_status, error_code, message, retry_at, backoff_level FROM `+t+` x
			WHERE x.cooldown_id = st.cooldown_id AND x.kind = 'cooldown_update' ORDER BY x.ts DESC, x.id DESC LIMIT 1) u ON TRUE
		WHERE st.kind = 'cooldown_start' AND st.cooldown_id <> ''
		AND NOT EXISTS (SELECT 1 FROM `+t+` e WHERE e.cooldown_id = st.cooldown_id AND e.kind = 'cooldown_end')`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var out []*openWindow
	for rows.Next() {
		var ow openWindow
		var started sql.NullTime
		var ts time.Time
		var retry sql.NullTime
		st := &ow.state
		if err = rows.Scan(&ow.id, &started, &ts, &st.AuthID, &st.AuthIndex, &st.AuthLabel, &st.Provider,
			&st.Model, &st.Reason, &st.HTTPStatus, &st.ErrorCode, &st.Message, &retry, &st.BackoffLevel); err != nil {
			return nil, err
		}
		ow.startedAt = ts
		if started.Valid {
			ow.startedAt = started.Time
		}
		if retry.Valid {
			st.RetryAt = retry.Time
		}
		out = append(out, &ow)
	}
	return out, rows.Err()
}

// cooldownStoreTable is the upstream Postgres store's cooldown table (PGSTORE_SCHEMA, default
// table name). Empty when the Postgres store isn't configured.
func cooldownStoreTable() string {
	if strings.TrimSpace(os.Getenv("PGSTORE_DSN")) == "" && strings.TrimSpace(os.Getenv("pgstore_dsn")) == "" {
		return ""
	}
	schema := strings.TrimSpace(os.Getenv("PGSTORE_SCHEMA"))
	if schema == "" {
		schema = strings.TrimSpace(os.Getenv("pgstore_schema"))
	}
	if schema == "" {
		return "cooldown_store"
	}
	return schema + ".cooldown_store"
}

// installTrigger adds a backstop on the upstream cooldown_store: every row it writes or clears is
// appended here (source 'trigger', kind store_set / store_cleared). The trigger swallows its own
// errors, so it can never fail a cooldown_store write. Skipped when that table isn't in this
// database.
func (s *Store) installTrigger(ctx context.Context, table string) {
	if table == "" {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	var reg sql.NullString
	if err := s.db.QueryRowContext(ctx, `SELECT to_regclass($1)::text`, table).Scan(&reg); err != nil || !reg.Valid {
		log.Infof("tmx cooldown history: %s is not in this database; no trigger", table)
		return
	}
	if err := s.InstallTriggerOn(ctx, reg.String); err != nil {
		log.WithError(err).Warnf("tmx cooldown history: could not add the trigger on %s", reg.String)
		return
	}
	log.Infof("tmx cooldown history: trigger on %s appends to %s", reg.String, s.table())
}

// InstallTriggerOn installs the cooldown_store trigger on table (a regclass name).
func (s *Store) InstallTriggerOn(ctx context.Context, table string) error {
	fn := s.schema + ".cooldown_store_history"
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	stmts := []string{
		`SET LOCAL lock_timeout = '5s'`,
		`CREATE OR REPLACE FUNCTION ` + fn + `() RETURNS trigger LANGUAGE plpgsql AS $fn$
BEGIN
	BEGIN
		INSERT INTO ` + s.table() + ` (event_key, ts, kind, source, auth_id, model, provider, reason,
			http_status, error_code, message, retry_at, backoff_level, detail)
		VALUES ('trigger:' || gen_random_uuid()::text, now(),
			CASE WHEN NEW.deleted THEN 'store_cleared' ELSE 'store_set' END, 'trigger',
			NEW.auth_id, NEW.model,
			COALESCE(NEW.content->>'provider', ''), COALESCE(NEW.content->>'reason', ''),
			COALESCE((NEW.content->'last_error'->>'http_status')::int, 0),
			COALESCE(NEW.content->'last_error'->>'code', ''),
			LEFT(COALESCE(NEW.content->'last_error'->>'message', ''), 500),
			CASE WHEN NEW.deleted THEN NULL ELSE NULLIF((NEW.content->>'next_retry_after')::timestamptz, '0001-01-01T00:00:00Z') END,
			COALESCE((NEW.content->'quota'->>'backoff_level')::int, 0),
			NEW.content);
	EXCEPTION WHEN OTHERS THEN
		RAISE WARNING 'tmx cooldown history trigger: %', SQLERRM;
	END;
	RETURN NULL;
END
$fn$`,
		`CREATE OR REPLACE TRIGGER tmx_cooldown_history_ins AFTER INSERT ON ` + table + `
			FOR EACH ROW EXECUTE FUNCTION ` + fn + `()`,
		`CREATE OR REPLACE TRIGGER tmx_cooldown_history_upd AFTER UPDATE ON ` + table + `
			FOR EACH ROW WHEN (OLD.content IS DISTINCT FROM NEW.content OR OLD.deleted IS DISTINCT FROM NEW.deleted)
			EXECUTE FUNCTION ` + fn + `()`,
	}
	for _, stmt := range stmts {
		if _, err = tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	s.trigger.Store(true)
	return nil
}
