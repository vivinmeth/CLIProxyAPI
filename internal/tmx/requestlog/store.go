package requestlog

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const (
	defaultSchema        = "tmx"
	defaultRetentionDays = 0 // keep forever: the request log is the long-term usage record
	queueSize            = 10000
	batchSize            = 200
	flushInterval        = 2 * time.Second
	pruneInterval        = 6 * time.Hour
	// Rows whose insert failed wait in memory (up to pendingMax) and are retried with backoff; on
	// shutdown they go to a spool file that the next start reads back.
	pendingMax    = 100000
	retryMaxDelay = time.Minute
	spoolFileName = "tmx-requestlog-pending.jsonl"
)

var schemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Store owns the database handle and the background writer.
type Store struct {
	db            *sql.DB
	schema        string
	retentionDays int
	queue         chan Row
	queueMu       sync.RWMutex
	closing       bool
	dropped       atomic.Int64
	written       atomic.Int64
	writeErrors   atomic.Int64
	rejected      atomic.Int64 // rows the database refused one by one (logged, not retried)
	pendingRows   atomic.Int64
	stopOnce      sync.Once
	closeWaitOnce sync.Once
	closeDone     chan struct{}
	done          chan struct{}

	keyMachines atomic.Pointer[map[string]string] // api_key_hash -> machine (key_machines)
	prices      atomic.Pointer[priceSnapshot]
	stopPrices  chan struct{}
	spoolFile   string // pending-rows file, resolved once at Open (see spoolPath)
}

var current atomic.Pointer[Store]

// Default returns the running store, or nil when request logging is disabled.
func Default() *Store { return current.Load() }

// StartFromEnv starts request logging when a DSN is available:
//
//	TMX_REQUESTLOG=off                 disable
//	TMX_REQUESTLOG_DSN                 Postgres DSN (falls back to PGSTORE_DSN)
//	TMX_REQUESTLOG_SCHEMA              schema name (default "tmx")
//	TMX_REQUESTLOG_RETENTION_DAYS      live proxy rows older than this are pruned (default 0 = keep
//	                                   forever); imported rows (extern:*) are never pruned
//
// Failure to connect is logged and leaves logging disabled; it never blocks startup.
func StartFromEnv(ctx context.Context) *Store {
	if strings.EqualFold(strings.TrimSpace(os.Getenv("TMX_REQUESTLOG")), "off") {
		log.Info("tmx request log: disabled by TMX_REQUESTLOG=off")
		return nil
	}
	dsn := strings.TrimSpace(os.Getenv("TMX_REQUESTLOG_DSN"))
	if dsn == "" {
		dsn = strings.TrimSpace(os.Getenv("PGSTORE_DSN"))
	}
	if dsn == "" {
		log.Debug("tmx request log: no DSN (TMX_REQUESTLOG_DSN / PGSTORE_DSN); disabled")
		return nil
	}
	schema := strings.TrimSpace(os.Getenv("TMX_REQUESTLOG_SCHEMA"))
	if schema == "" {
		schema = defaultSchema
	}
	retention := defaultRetentionDays
	if raw := strings.TrimSpace(os.Getenv("TMX_REQUESTLOG_RETENTION_DAYS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			retention = n
		}
	}
	store, err := Open(ctx, dsn, schema, retention)
	if err != nil {
		log.WithError(err).Error("tmx request log: disabled, could not open store")
		return nil
	}
	current.Store(store)
	log.Infof("tmx request log: writing to %s.requests (retention %d days)", schema, retention)
	return store
}

// Open connects, ensures the schema and starts the writer and pruner.
func Open(ctx context.Context, dsn, schema string, retentionDays int) (*Store, error) {
	if !schemaNamePattern.MatchString(schema) {
		return nil, fmt.Errorf("invalid schema name %q", schema)
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	db.SetConnMaxIdleTime(5 * time.Minute)
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if errPing := db.PingContext(pingCtx); errPing != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping: %w", errPing)
	}
	s := &Store{
		db:            db,
		schema:        schema,
		retentionDays: retentionDays,
		queue:         make(chan Row, queueSize),
		done:          make(chan struct{}),
		stopPrices:    make(chan struct{}),
		spoolFile:     spoolPath(),
	}
	if errSchema := s.ensureSchema(pingCtx); errSchema != nil {
		_ = db.Close()
		return nil, errSchema
	}
	go s.run()
	go s.backfillAccounting()
	go s.backfillHarness()
	go s.backfillBilling()
	go s.pricesLoop()
	if err := s.loadKeyMachines(pingCtx); err != nil {
		log.WithError(err).Warn("tmx request log: loading the key -> machine ledger failed")
	}
	return s, nil
}

func (s *Store) table() string { return s.schema + ".requests" }

func (s *Store) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + s.schema,
		`CREATE TABLE IF NOT EXISTS ` + s.table() + ` (
			id               BIGSERIAL PRIMARY KEY,
			ts               TIMESTAMPTZ NOT NULL,
			request_id       TEXT NOT NULL DEFAULT '',
			trace_id         TEXT NOT NULL DEFAULT '',
			session_id       TEXT NOT NULL DEFAULT '',
			provider         TEXT NOT NULL DEFAULT '',
			executor         TEXT NOT NULL DEFAULT '',
			model            TEXT NOT NULL DEFAULT '',
			alias            TEXT NOT NULL DEFAULT '',
			response_model   TEXT NOT NULL DEFAULT '',
			auth_id          TEXT NOT NULL DEFAULT '',
			auth_index       TEXT NOT NULL DEFAULT '',
			auth_label       TEXT NOT NULL DEFAULT '',
			auth_type        TEXT NOT NULL DEFAULT '',
			api_key_hash     TEXT NOT NULL DEFAULT '',
			access_provider  TEXT NOT NULL DEFAULT '',
			client_ip        TEXT NOT NULL DEFAULT '',
			user_agent       TEXT NOT NULL DEFAULT '',
			stream           BOOLEAN NOT NULL DEFAULT FALSE,
			outcome          TEXT NOT NULL,
			status_code      INTEGER NOT NULL DEFAULT 0,
			error            TEXT NOT NULL DEFAULT '',
			latency_ms       BIGINT NOT NULL DEFAULT 0,
			ttft_ms          BIGINT NOT NULL DEFAULT 0,
			input_tokens     BIGINT NOT NULL DEFAULT 0,
			output_tokens    BIGINT NOT NULL DEFAULT 0,
			reasoning_tokens BIGINT NOT NULL DEFAULT 0,
			cached_tokens    BIGINT NOT NULL DEFAULT 0,
			cache_read       BIGINT NOT NULL DEFAULT 0,
			cache_creation   BIGINT NOT NULL DEFAULT 0,
			total_tokens     BIGINT NOT NULL DEFAULT 0,
			reasoning_effort TEXT NOT NULL DEFAULT '',
			service_tier     TEXT NOT NULL DEFAULT ''
		)`,
		`CREATE INDEX IF NOT EXISTS requests_ts_idx ON ` + s.table() + ` (ts DESC)`,
		`CREATE INDEX IF NOT EXISTS requests_auth_ts_idx ON ` + s.table() + ` (auth_id, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS requests_outcome_ts_idx ON ` + s.table() + ` (outcome, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS requests_session_idx ON ` + s.table() + ` (session_id, ts DESC) WHERE session_id <> ''`,
		// v2: normalised accounting + provenance (imported sessions).
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS uncached_input BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS cache_write BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS output_non_reasoning BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS accounting_quality TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'tmx-proxy'`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS machine TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS import_ref TEXT`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS shadowed BOOLEAN NOT NULL DEFAULT FALSE`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS harness TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS client_mode TEXT NOT NULL DEFAULT ''`,
		`CREATE UNIQUE INDEX IF NOT EXISTS requests_import_ref_idx ON ` + s.table() + ` (import_ref) WHERE import_ref IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS requests_source_ts_idx ON ` + s.table() + ` (source, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS requests_key_ts_idx ON ` + s.table() + ` (api_key_hash, ts DESC)`,
		// v3: 1-hour cache writes, speed, list-price cost fixed at write time (prices.go).
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS cache_write_1h BIGINT NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS speed TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS cost_input DOUBLE PRECISION`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS cost_cache_read DOUBLE PRECISION`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS cost_cache_write DOUBLE PRECISION`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS cost_output DOUBLE PRECISION`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS cost_usd DOUBLE PRECISION`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS priced_as TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS price_source TEXT`,
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS priced_at TIMESTAMPTZ`,
		// row_key: set by the writer, makes retried inserts idempotent.
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS row_key TEXT`,
		`CREATE UNIQUE INDEX IF NOT EXISTS requests_row_key_idx ON ` + s.table() + ` (row_key) WHERE row_key IS NOT NULL`,
		// v4: billing mode; client keys' machines from fleet's key ledger (SetKeyMachines).
		`ALTER TABLE ` + s.table() + ` ADD COLUMN IF NOT EXISTS billing TEXT NOT NULL DEFAULT ''`,
		`CREATE TABLE IF NOT EXISTS ` + s.schema + `.key_machines (
			api_key_hash TEXT PRIMARY KEY,
			machine      TEXT NOT NULL,
			updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`CREATE INDEX IF NOT EXISTS requests_price_pending_idx ON ` + s.table() + ` (id)
			WHERE price_source IS NULL OR price_source = 'unpriced'`,
		`CREATE TABLE IF NOT EXISTS ` + s.schema + `.price_catalogs (
			fetched_at TIMESTAMPTZ PRIMARY KEY,
			source     TEXT NOT NULL,
			catalog    JSONB NOT NULL
		)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

// Enqueue hands a row to the writer without blocking; when the queue is full the row is dropped
// and counted rather than slowing the request path. (The writer never blocks on the database:
// failed inserts wait in its pending list, so the queue only fills under extreme load.)
func (s *Store) Enqueue(row Row) {
	if s == nil {
		return
	}
	s.queueMu.RLock()
	defer s.queueMu.RUnlock()
	if s.closing {
		s.dropped.Add(1)
		return
	}
	if row.RowKey == "" {
		row.RowKey = newRowKey()
	}
	if row.Machine == "" && row.APIKeyHash != "" {
		if m := s.keyMachines.Load(); m != nil {
			row.Machine = (*m)[row.APIKeyHash]
		}
	}
	select {
	case s.queue <- row:
	default:
		if s.dropped.Add(1)%100 == 1 {
			log.Warn("tmx request log: queue full, dropping rows")
		}
	}
}

// Stats reports writer counters for the status endpoint.
func (s *Store) Stats() map[string]int64 {
	return map[string]int64{
		"written":      s.written.Load(),
		"dropped":      s.dropped.Load(),
		"write_errors": s.writeErrors.Load(),
		"rejected":     s.rejected.Load(),
		"pending":      s.pendingRows.Load(),
		"queued":       int64(len(s.queue)),
	}
}

// Close flushes queued rows and stops the writer.
func (s *Store) Close() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.stopPrices)
		s.queueMu.Lock()
		s.closing = true
		close(s.queue)
		s.queueMu.Unlock()
		<-s.done
		_ = s.db.Close()
	})
}

func (s *Store) run() {
	defer close(s.done)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	prune := time.NewTicker(pruneInterval)
	defer prune.Stop()
	s.prune()

	w := &writer{s: s, pending: readSpool(s.spoolFile)}
	w.spooled = len(w.pending) > 0
	batch := make([]Row, 0, batchSize)
	flush := func() {
		w.write(batch)
		batch = batch[:0]
	}
	for {
		select {
		case row, ok := <-s.queue:
			if !ok {
				flush()
				w.persistAt(time.Time{}) // whatever is still pending survives the restart
				return
			}
			batch = append(batch, row)
			if len(batch) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		case <-prune.C:
			s.prune()
		}
	}
}

// writer is the run loop's insert state: rows that failed to insert are kept and retried.
type writer struct {
	s         *Store
	pending   []Row
	nextRetry time.Time
	delay     time.Duration
	spooled   bool      // the spool file holds pending rows
	dirty     bool      // pending changed since the spool was last written
	lastSpool time.Time // throttles spool rewrites during an outage
}

// write prices and inserts batch (after anything still pending). Rows are only dropped when the
// database refuses them for what they contain (data / constraint errors, logged one by one);
// anything else (connection loss, deadlock, a restart) keeps them for a retry with backoff, and
// while rows are pending they are also kept in the spool file, so a crash doesn't lose them.
// Inserts are idempotent per row key, so a retry never double counts.
func (w *writer) write(batch []Row) {
	s := w.s
	snap := s.priceSnap()
	for i := range batch {
		if batch[i].RowKey == "" {
			batch[i].RowKey = newRowKey()
		}
		priceRow(&batch[i], snap)
	}
	w.pending = append(w.pending, batch...)
	if over := len(w.pending) - pendingMax; over > 0 {
		s.dropped.Add(int64(over))
		log.Warnf("tmx request log: %d unwritten rows over the pending limit dropped", over)
		w.pending = append([]Row(nil), w.pending[over:]...)
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
	if w.delay > 0 { // retrying: save what is pending before possibly long database attempts
		w.persistAt(time.Now())
	}
	backoff := func(err error) {
		s.writeErrors.Add(1)
		w.delay = min(max(2*w.delay, flushInterval), retryMaxDelay)
		w.nextRetry = time.Now().Add(w.delay)
		log.WithError(err).Warnf("tmx request log: write failed; %d rows kept for retry", len(w.pending))
	}
	for len(w.pending) > 0 {
		chunk := w.pending[:min(batchSize, len(w.pending))]
		err := s.insert(chunk)
		if err == nil {
			s.written.Add(int64(len(chunk)))
			w.pending = w.pending[len(chunk):]
			continue
		}
		if !isDataError(err) || !s.reachable() {
			backoff(err)
			return
		}
		// Something in the chunk is refused: find it row by row.
		for len(chunk) > 0 {
			errOne := s.insert(chunk[:1])
			switch {
			case errOne == nil:
				s.written.Add(1)
			case isDataError(errOne):
				s.rejected.Add(1)
				log.WithError(errOne).Warnf("tmx request log: row rejected (trace %s, model %s)", chunk[0].TraceID, chunk[0].Model)
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

// isDataError: the database refused the row's content (data exception or integrity violation:
// invalid text, out of range, constraint); retrying it would fail again. Everything else —
// including schema / permission errors (class 42), which are fixed by fixing the database — is
// kept and retried.
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

func newRowKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func (s *Store) reachable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.db.PingContext(ctx) == nil
}

// spoolPath is the pending-rows file: TMX_REQUESTLOG_SPOOL (a directory), else the archive
// spool volume (persistent on prod), else WRITABLE_PATH, else the temp dir.
func spoolPath() string {
	for _, dir := range []string{os.Getenv("TMX_REQUESTLOG_SPOOL"), os.Getenv("TMX_ARCHIVE_SPOOL"),
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

// spoolEvery: while rows are pending the spool is rewritten at most this often, before each
// write attempt (a long outage isn't rewritten on every flush). A crash during an outage loses
// the rows received since the last spool write: up to spoolEvery plus the batch being written.
// When the database is healthy rows are never spooled; they live in memory for one flush.
const spoolEvery = 10 * time.Second

// persistAt mirrors the pending rows to the spool file (atomically) when they changed and the
// last write is spoolEvery old (now zero = write regardless), or removes the file once nothing
// is pending; readSpool loads it at the next start. A failed write stays dirty and is retried.
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
		log.WithError(err).Warnf("tmx request log: can't spool %d pending rows", len(w.pending))
		return
	}
	if err := os.Rename(tmp, path); err != nil {
		log.WithError(err).Warnf("tmx request log: can't spool %d pending rows", len(w.pending))
		return
	}
	w.spooled, w.dirty, w.lastSpool = true, false, now
}

func readSpool(path string) []Row {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var rows []Row
	dec := json.NewDecoder(bytes.NewReader(raw))
	for {
		var r Row
		if dec.Decode(&r) != nil {
			break
		}
		rows = append(rows, r)
	}
	if len(rows) > 0 {
		log.Infof("tmx request log: %d rows from the previous run queued for writing", len(rows))
	}
	return rows
}

// columnNames and rowFields must stay in the same order; they drive insert and scan.
var columnNames = []string{"ts", "request_id", "trace_id", "session_id", "provider", "executor",
	"model", "alias", "response_model", "auth_id", "auth_index", "auth_label", "auth_type",
	"api_key_hash", "access_provider", "client_ip", "user_agent", "stream", "outcome",
	"status_code", "error", "latency_ms", "ttft_ms", "input_tokens", "output_tokens",
	"reasoning_tokens", "cached_tokens", "cache_read", "cache_creation", "total_tokens",
	"reasoning_effort", "service_tier", "uncached_input", "cache_write", "output_non_reasoning",
	"accounting_quality", "source", "machine", "import_ref", "shadowed", "harness", "client_mode",
	"cache_write_1h", "speed", "cost_input", "cost_cache_read", "cost_cache_write", "cost_output", "cost_usd",
	"priced_as", "price_source", "priced_at", "row_key", "billing"}

func rowFields(r *Row) []any {
	return []any{&r.TS, &r.RequestID, &r.TraceID, &r.SessionID, &r.Provider, &r.Executor,
		&r.Model, &r.Alias, &r.ResponseModel, &r.AuthID, &r.AuthIndex, &r.AuthLabel, &r.AuthType,
		&r.APIKeyHash, &r.AccessProvider, &r.ClientIP, &r.UserAgent, &r.Stream, &r.Outcome,
		&r.StatusCode, &r.Error, &r.LatencyMs, &r.TTFTMs, &r.InputTokens, &r.OutputTokens,
		&r.ReasoningTokens, &r.CachedTokens, &r.CacheRead, &r.CacheCreation, &r.TotalTokens,
		&r.ReasoningEffort, &r.ServiceTier, &r.UncachedInput, &r.CacheWrite, &r.OutputNonReasoning,
		&r.AccountingQuality, &r.Source, &r.Machine, &r.importRefPtr, &r.Shadowed, &r.Harness, &r.ClientMode,
		&r.CacheWrite1h, &r.Speed, &r.CostInput, &r.CostCacheRead, &r.CostCacheWrite, &r.CostOutput, &r.CostUSD,
		&r.PricedAs, &r.PriceSource, &r.PricedAt, &r.rowKeyPtr, &r.Billing}
}

var insertColumns = strings.Join(columnNames, ", ")

// insertValues renders "($1,...),($n,...)" for n rows and returns the flattened args.
func insertValues(rows []Row) (string, []any) {
	var b strings.Builder
	args := make([]any, 0, len(rows)*len(columnNames))
	for i := range rows {
		r := &rows[i]
		r.syncImportRef()
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
		args = append(args, rowFields(r)...)
	}
	return b.String(), args
}

func (s *Store) insert(rows []Row) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	values, args := insertValues(rows)
	// A retried batch whose first attempt did commit (the acknowledgement was lost) inserts nothing.
	_, err := s.db.ExecContext(ctx, "INSERT INTO "+s.table()+" ("+insertColumns+") VALUES "+values+
		" ON CONFLICT (row_key) WHERE row_key IS NOT NULL DO NOTHING", args...)
	return err
}

// Execer runs statements (a *sql.DB or the caller's *sql.Tx).
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// pricedColumns keep their values on a re-import that doesn't change what the row is priced on
// (pricedOn): a row's cost is fixed when its tokens are written.
var pricedColumns = map[string]bool{"cost_input": true, "cost_cache_read": true, "cost_cache_write": true,
	"cost_output": true, "cost_usd": true, "priced_as": true, "price_source": true, "priced_at": true}

var pricedOn = []string{"provider", "model", "uncached_input", "cache_read", "cache_write", "cache_write_1h",
	"output_non_reasoning", "reasoning_tokens", "speed", "service_tier"}

// UpsertImported writes usage rows derived from an imported session (source "extern:<harness>").
// Rows are keyed by ImportRef, so re-importing a session updates them instead of adding copies.
// A row keeps the cost it was first priced at unless its tokens, model or tier change.
func (s *Store) UpsertImported(ctx context.Context, rows []Row) error {
	if s == nil {
		return nil
	}
	return s.UpsertImportedWith(ctx, s.db, rows)
}

// UpsertImportedWith is UpsertImported on ex (the caller's transaction, when the request log
// lives in the same database).
func (s *Store) UpsertImportedWith(ctx context.Context, ex Execer, rows []Row) error {
	if s == nil || len(rows) == 0 {
		return nil
	}
	snap := s.priceSnap()
	for i := range rows {
		priceRow(&rows[i], snap)
	}
	same := make([]string, len(pricedOn))
	for i, c := range pricedOn {
		same[i] = "t." + c + " IS NOT DISTINCT FROM EXCLUDED." + c
	}
	keep := "(t.price_source IS NOT NULL AND t.price_source <> 'unpriced' AND " + strings.Join(same, " AND ") + ")"
	set := make([]string, 0, len(columnNames))
	for _, c := range columnNames {
		switch {
		case c == "import_ref":
		case pricedColumns[c]:
			set = append(set, c+" = CASE WHEN "+keep+" THEN t."+c+" ELSE EXCLUDED."+c+" END")
		default:
			set = append(set, c+" = EXCLUDED."+c)
		}
	}
	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		values, args := insertValues(rows[start:end])
		if _, err := ex.ExecContext(ctx, "INSERT INTO "+s.table()+" AS t ("+insertColumns+") VALUES "+values+
			" ON CONFLICT (import_ref) WHERE import_ref IS NOT NULL DO UPDATE SET "+strings.Join(set, ", "),
			args...); err != nil {
			return err
		}
	}
	return nil
}

// DeleteImported removes imported usage rows whose import_ref starts with prefix ("<session>#"),
// except those listed in keep. It returns the number of rows removed.
func (s *Store) DeleteImported(ctx context.Context, prefix string, keep []string) (int64, error) {
	if s == nil {
		return 0, nil
	}
	return s.DeleteImportedWith(ctx, s.db, prefix, keep)
}

// DeleteImportedWith is DeleteImported on ex.
func (s *Store) DeleteImportedWith(ctx context.Context, ex Execer, prefix string, keep []string) (int64, error) {
	if s == nil || prefix == "" {
		return 0, nil
	}
	quoted := make([]string, len(keep))
	for i, k := range keep {
		quoted[i] = `"` + strings.NewReplacer(`\`, `\\`, `"`, `\"`).Replace(k) + `"`
	}
	res, err := ex.ExecContext(ctx, `DELETE FROM `+s.table()+` WHERE import_ref IS NOT NULL
		AND starts_with(import_ref, $1) AND NOT (import_ref = ANY($2::text[]))`, prefix,
		"{"+strings.Join(quoted, ",")+"}")
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// backfillHarness labels rows written before the harness columns existed. Rows are grouped by
// (source, user_agent), which has few distinct values, and updated per group.
func (s *Store) backfillHarness() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT source, user_agent FROM `+s.table()+` WHERE harness = ''`)
	if err != nil {
		log.WithError(err).Warn("tmx request log: harness backfill query failed")
		return
	}
	type pair struct{ source, ua string }
	var pairs []pair
	for rows.Next() {
		var p pair
		if rows.Scan(&p.source, &p.ua) == nil {
			pairs = append(pairs, p)
		}
	}
	_ = rows.Close()
	total := int64(0)
	for _, p := range pairs {
		harness, mode := HarnessFor(p.source, p.ua)
		if harness == "" {
			continue
		}
		res, errUp := s.db.ExecContext(ctx, `UPDATE `+s.table()+` SET harness = $1, client_mode = $2
			WHERE harness = '' AND source = $3 AND user_agent = $4`, harness, mode, p.source, p.ua)
		if errUp != nil {
			log.WithError(errUp).Warn("tmx request log: harness backfill update failed")
			return
		}
		n, _ := res.RowsAffected()
		total += n
	}
	if total > 0 {
		log.Infof("tmx request log: labelled %d rows with a harness", total)
	}
}

// backfillAccounting fills the normalised columns for rows written before they existed, using
// the same upstream accounting code as live rows.
func (s *Store) backfillAccounting() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	total := 0
	for {
		rows, err := s.db.QueryContext(ctx, `SELECT id, provider, executor, input_tokens, output_tokens,
			reasoning_tokens, cached_tokens, cache_read, cache_creation, total_tokens FROM `+s.table()+`
			WHERE accounting_quality = '' AND source = 'tmx-proxy' ORDER BY id LIMIT 500`)
		if err != nil {
			log.WithError(err).Warn("tmx request log: accounting backfill query failed")
			return
		}
		type item struct {
			id  int64
			row Row
		}
		var batch []item
		for rows.Next() {
			var it item
			var provider, executor string
			var d coreusage.Detail
			if err = rows.Scan(&it.id, &provider, &executor, &d.InputTokens, &d.OutputTokens,
				&d.ReasoningTokens, &d.CachedTokens, &d.CacheReadTokens, &d.CacheCreationTokens,
				&d.TotalTokens); err != nil {
				break
			}
			d = coreusage.EnsureTokenBreakdownForProvider(d, provider, executor)
			it.row.applyBreakdown(d.TokenBreakdown)
			if it.row.AccountingQuality == "" {
				it.row.AccountingQuality = "unclassified"
			}
			batch = append(batch, it)
		}
		_ = rows.Close()
		if err != nil || len(batch) == 0 {
			break
		}
		for _, it := range batch {
			r := it.row
			if _, err = s.db.ExecContext(ctx, `UPDATE `+s.table()+` SET uncached_input = $2, cache_read = $3,
				cache_write = $4, output_non_reasoning = $5, reasoning_tokens = $6,
				accounting_quality = $7, total_tokens = GREATEST(total_tokens, $8) WHERE id = $1`,
				it.id, r.UncachedInput, r.CacheRead, r.CacheWrite, r.OutputNonReasoning,
				r.ReasoningTokens, r.AccountingQuality, r.TotalTokens); err != nil {
				log.WithError(err).Warn("tmx request log: accounting backfill update failed")
				return
			}
		}
		total += len(batch)
	}
	if total > 0 {
		log.Infof("tmx request log: backfilled normalised accounting for %d rows", total)
	}
}

func (s *Store) prune() {
	if s.retentionDays <= 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	res, err := s.db.ExecContext(ctx,
		// Only live proxy rows age out. Imported rows are the only copy of external history in the
		// request log and must never be pruned.
		"DELETE FROM "+s.table()+" WHERE ts < now() - make_interval(days => $1) AND source = 'tmx-proxy' AND import_ref IS NULL",
		s.retentionDays)
	if err != nil {
		log.WithError(err).Warn("tmx request log: prune failed")
		return
	}
	if n, _ := res.RowsAffected(); n > 0 {
		log.Infof("tmx request log: pruned %d rows older than %d days", n, s.retentionDays)
	}
}

// DB exposes the handle for tests and maintenance tooling.
func (s *Store) DB() *sql.DB { return s.db }

// Schema returns the schema the store writes to.
func (s *Store) Schema() string { return s.schema }

// SetDefaultForTest replaces the running store (tests only).
func SetDefaultForTest(s *Store) { current.Store(s) }
