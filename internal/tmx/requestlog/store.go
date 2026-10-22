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
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/durability"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/pricing"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/spool"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const (
	defaultSchema        = "tmx"
	defaultRetentionDays = 0 // keep forever: the request log is the long-term usage record
	batchSize            = 200
	flushInterval        = 2 * time.Second
	pruneInterval        = 6 * time.Hour
	// Rows the database can't take right now (it is down, or the queue is full) go to a durable
	// on-disk spool (internal/tmx/spool) and are replayed when it recovers; rows it refuses for
	// their content go to the spool's dead-letter file. Nothing is dropped unless the spool is
	// over its cap (TMX_SPOOL_REQUESTLOG_MAX_MB, default 1 GiB), and that is counted.
	spoolMaxBytes = 1 << 30
	// legacySpoolFileName: the pending-rows file of earlier versions, moved into the spool at start.
	legacySpoolFileName = "tmx-requestlog-pending.jsonl"
	// allowPruneValue must be in TMX_REQUESTLOG_ALLOW_PRUNE for a nonzero retention to delete rows.
	allowPruneValue = "yes-delete-history"
)

// queueSize is the in-memory queue in front of the writer (a variable for tests).
var queueSize = 10000

var schemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Store owns the database handle and the background writer.
type Store struct {
	db            *sql.DB
	schema        string
	retentionDays int
	queue         chan Row
	flush         chan chan struct{}
	queueMu       sync.RWMutex
	closing       bool
	dropped       atomic.Int64
	written       atomic.Int64
	writeErrors   atomic.Int64
	rejected      atomic.Int64 // rows the database refused for their content (dead-lettered)
	spooled       atomic.Int64 // rows sent to the spool (database down / queue full)
	stopOnce      sync.Once
	closeWaitOnce sync.Once
	closeDone     chan struct{}
	done          chan struct{}

	keyMachines atomic.Pointer[map[string]string] // api_key_hash -> machine (key_machines)
	prices      atomic.Pointer[priceSnapshot]
	stopPrices  chan struct{}
	spool       *spool.Spool // nil only when the spool directory can't be opened
}

var current atomic.Pointer[Store]

// Default returns the running store, or nil when request logging is disabled.
func Default() *Store { return current.Load() }

// StartFromEnv starts request logging when a DSN is available:
//
//	TMX_REQUESTLOG=off                 disable
//	TMX_REQUESTLOG_DSN                 Postgres DSN (falls back to PGSTORE_DSN)
//	TMX_REQUESTLOG_SCHEMA              schema name (default "tmx")
//	TMX_REQUESTLOG_RETENTION_DAYS      default 0 = keep forever (the request log is the long-term
//	                                   usage record). A nonzero value is IGNORED, with a loud error,
//	                                   unless TMX_REQUESTLOG_ALLOW_PRUNE=yes-delete-history is also
//	                                   set; then live proxy rows older than that many days are
//	                                   deleted. Imported rows (extern:*) are never pruned.
//	TMX_SPOOL_DIR / TMX_SPOOL_REQUESTLOG_MAX_MB   durable spool location and cap (spool.Dir)
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
	retention := retentionFromEnv()
	store, err := Open(ctx, dsn, schema, retention)
	if err != nil {
		log.WithError(err).Error("tmx request log: disabled, could not open store")
		return nil
	}
	current.Store(store)
	durability.Register("requestlog", store.Durability)
	retained := "kept forever"
	if retention > 0 {
		retained = fmt.Sprintf("PRUNED after %d days", retention)
	}
	spoolDir := "none"
	if store.spool != nil {
		spoolDir = store.spool.Dir()
	}
	log.Infof("tmx request log: writing to %s.requests (rows %s, spool %s)", schema, retained, spoolDir)
	return store
}

// retentionFromEnv is the retention in days: 0 (keep forever) unless TMX_REQUESTLOG_RETENTION_DAYS
// is nonzero AND TMX_REQUESTLOG_ALLOW_PRUNE=yes-delete-history. Deleting history is never the
// default and never the result of one stray variable.
func retentionFromEnv() int {
	raw := strings.TrimSpace(os.Getenv("TMX_REQUESTLOG_RETENTION_DAYS"))
	if raw == "" {
		return defaultRetentionDays
	}
	n, err := strconv.Atoi(raw)
	switch {
	case err != nil || n < 0:
		log.Errorf("tmx request log: TMX_REQUESTLOG_RETENTION_DAYS=%q is not a day count; IGNORED, rows are kept forever", raw)
		return defaultRetentionDays
	case n == 0:
		return 0
	case strings.TrimSpace(os.Getenv("TMX_REQUESTLOG_ALLOW_PRUNE")) != allowPruneValue:
		log.Errorf("tmx request log: TMX_REQUESTLOG_RETENTION_DAYS=%d IGNORED, rows are kept forever: pruning "+
			"deletes request history. Also set TMX_REQUESTLOG_ALLOW_PRUNE=%s to really delete rows older than %d days",
			n, allowPruneValue, n)
		return defaultRetentionDays
	default:
		log.Warnf("tmx request log: DELETING live proxy rows older than %d days (TMX_REQUESTLOG_ALLOW_PRUNE is set)", n)
		return n
	}
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
		flush:         make(chan chan struct{}),
		done:          make(chan struct{}),
		stopPrices:    make(chan struct{}),
	}
	if errSchema := s.ensureSchema(pingCtx); errSchema != nil {
		_ = db.Close()
		return nil, errSchema
	}
	s.openSpool()
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

// Enqueue hands a row to the writer without waiting on the database. When the queue is full (the
// writer is stuck on a slow database) or the store is closing, the row goes to the on-disk spool
// instead (one fsynced append; only under overload).
func (s *Store) Enqueue(row Row) {
	if s == nil {
		return
	}
	s.queueMu.RLock()
	defer s.queueMu.RUnlock()
	if row.RowKey == "" {
		row.RowKey = newRowKey()
	}
	if row.Machine == "" && row.APIKeyHash != "" {
		if m := s.keyMachines.Load(); m != nil {
			row.Machine = (*m)[row.APIKeyHash]
		}
	}
	if s.closing {
		s.spoolRows([]Row{row})
		return
	}
	select {
	case s.queue <- row:
	default:
		if n := s.spooled.Load(); n%1000 == 0 {
			log.Warn("tmx request log: queue full, spooling rows to disk")
		}
		s.spoolRows([]Row{row})
	}
}

// Stats reports writer counters for the status endpoint. pending = rows in the on-disk spool.
func (s *Store) Stats() map[string]int64 {
	d := s.Durability()
	return map[string]int64{
		"written":      s.written.Load(),
		"dropped":      d.Dropped,
		"write_errors": s.writeErrors.Load(),
		"rejected":     s.rejected.Load(),
		"pending":      d.SpooledPending,
		"queued":       d.Queued,
		"spooled":      d.Spooled,
		"replayed":     d.Replayed,
		"deadlettered": d.Deadlettered,
	}
}

// Durability is the request log's entry in GET /tmx/durability.
func (s *Store) Durability() durability.Stats {
	var d durability.Stats
	if s.spool != nil {
		d = s.spool.Stats()
	}
	d.Queued = int64(len(s.queue))
	d.Dropped += s.dropped.Load()
	return d
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
		s.spool.Close() // what is still spooled is replayed by the next start
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

	batch := make([]Row, 0, batchSize)
	flush := func() {
		s.write(batch)
		batch = batch[:0]
	}
	for {
		select {
		case row, ok := <-s.queue:
			if !ok {
				flush()
				return
			}
			batch = append(batch, row)
			if len(batch) >= batchSize {
				flush()
			}
		case ack := <-s.flush:
			// Flush the snapshot accepted before the checkpoint, without stopping intake.
			n := len(s.queue)
			stopped := false
			for i := 0; i < n; i++ {
				row, ok := <-s.queue
				if !ok {
					stopped = true
					break
				}
				batch = append(batch, row)
			}
			flush()
			close(ack)
			if stopped {
				return
			}
		case <-ticker.C:
			flush()
		case <-prune.C:
			s.prune()
		}
	}
}

// write prices and inserts batch. Rows the database can't take now (connection loss, a restart,
// a timeout) go to the spool and are replayed when it is back; while the spool's replay is
// failing, rows go straight to the spool instead of waiting on a database known to be down. A
// row refused for what it contains (data / constraint error) is dead-lettered alone; the rest of
// its batch is written. Inserts are idempotent per row key, so a replay never double counts.
func (s *Store) write(batch []Row) {
	if len(batch) == 0 {
		return
	}
	snap := s.priceSnap()
	for i := range batch {
		if batch[i].RowKey == "" {
			batch[i].RowKey = newRowKey()
		}
		priceRow(&batch[i], snap)
	}
	if s.spool != nil && s.spool.Failing() {
		s.spoolRows(batch)
		return
	}
	for len(batch) > 0 {
		done, err := s.insertRows(context.Background(), batch)
		s.written.Add(int64(done))
		batch = batch[done:]
		switch {
		case err == nil:
			if s.spool != nil {
				s.spool.Recovered()
			}
			return
		case spool.IsPermanent(err):
			s.reject(batch[0], err)
			batch = batch[1:]
		default:
			if s.writeErrors.Add(1)%20 == 1 {
				log.WithError(err).Warnf("tmx request log: write failed; %d rows spooled to disk for retry", len(batch))
			}
			if s.spool != nil {
				s.spool.NoteError(err)
			}
			s.spoolRows(batch)
			return
		}
	}
}

// insertRows inserts rows in chunks and returns how many leading rows are now in the table. On an
// error the row at that index (and those after it) are not: spool.Permanent when the database
// refuses that row for its content, a plain error when it can't take rows right now.
func (s *Store) insertRows(ctx context.Context, rows []Row) (int, error) {
	done := 0
	for done < len(rows) {
		chunk := rows[done:min(done+batchSize, len(rows))]
		err := s.insertContext(ctx, chunk)
		if err == nil {
			done += len(chunk)
			continue
		}
		if !isDataError(err) || !s.reachable() {
			return done, err
		}
		// Something in the chunk is refused: find it row by row.
		for i := range chunk {
			errOne := s.insertContext(ctx, chunk[i:i+1])
			switch {
			case errOne == nil:
				done++
			case isDataError(errOne):
				return done, spool.Permanent(errOne)
			default:
				return done, errOne
			}
		}
	}
	return done, nil
}

// reject dead-letters a row the database refused for its content.
func (s *Store) reject(row Row, err error) {
	s.rejected.Add(1)
	log.WithError(err).Warnf("tmx request log: row rejected, dead-lettered (trace %s, model %s)", row.TraceID, row.Model)
	payload, errJ := json.Marshal(&row)
	if s.spool == nil || errJ != nil {
		s.dropped.Add(1)
		return
	}
	_ = s.spool.Deadletter(payload, err.Error())
}

// spoolRows appends rows to the on-disk spool (one fsync for the lot). Without a spool they are
// dropped, counted and logged.
func (s *Store) spoolRows(rows []Row) {
	payloads := make([][]byte, 0, len(rows))
	for i := range rows {
		if rows[i].RowKey == "" {
			rows[i].RowKey = newRowKey()
		}
		payload, err := json.Marshal(&rows[i])
		if err != nil || s.spool == nil {
			if s.dropped.Add(1)%100 == 1 {
				log.Errorf("tmx request log: ROW DROPPED, no spool to keep it (%d dropped since start)", s.dropped.Load())
			}
			continue
		}
		payloads = append(payloads, payload)
	}
	if len(payloads) > 0 && s.spool.AppendBatch(payloads) == nil { // failures are counted by the spool
		s.spooled.Add(int64(len(payloads)))
	}
}

// deliverSpooled is the spool's replay sink: decode and insert in one batch.
func (s *Store) deliverSpooled(ctx context.Context, items []spool.Item) (int, error) {
	rows := make([]Row, 0, len(items))
	var decodeErr error
	for _, it := range items {
		raw, err := it.Bytes()
		if err != nil {
			decodeErr = err
			break
		}
		var r Row
		if err = json.Unmarshal(raw, &r); err != nil {
			decodeErr = spool.Permanent(fmt.Errorf("undecodable spooled row: %w", err))
			break
		}
		rows = append(rows, r)
	}
	done, err := s.insertRows(ctx, rows)
	s.written.Add(int64(done))
	if err == nil {
		err = decodeErr
	}
	if spool.IsPermanent(err) {
		s.rejected.Add(1)
	}
	return done, err
}

// openSpool opens the durable spool and moves the pending-rows file of earlier versions into it.
func (s *Store) openSpool() {
	name := "requestlog"
	if s.schema != defaultSchema {
		name += "-" + s.schema
	}
	sp, err := spool.OpenFirstFree(spool.Dir(name), spool.Options{Name: name, Deliver: s.deliverSpooled,
		MaxBatch: batchSize, MaxBytes: spool.MaxBytesFromEnv("requestlog", spoolMaxBytes)})
	if err != nil {
		log.WithError(err).Error("tmx request log: NO DURABLE SPOOL, rows the database can't take will be dropped")
		return
	}
	s.spool = sp
	legacy := legacySpoolPath()
	if rows := readSpool(legacy); len(rows) > 0 {
		before := sp.Stats().Dropped
		s.spoolRows(rows)
		if sp.Stats().Dropped != before {
			log.Errorf("tmx request log: could not move every row of %s into the spool; file kept", legacy)
			return
		}
	}
	_ = os.Remove(legacy)
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

// legacySpoolPath is where earlier versions kept rows pending a retry: TMX_REQUESTLOG_SPOOL (a
// directory), else the archive spool volume, else WRITABLE_PATH, else the temp dir.
func legacySpoolPath() string {
	for _, dir := range []string{os.Getenv("TMX_REQUESTLOG_SPOOL"), os.Getenv("TMX_ARCHIVE_SPOOL"),
		"/CLIProxyAPI/archive-spool", os.Getenv("WRITABLE_PATH")} {
		if dir = strings.TrimSpace(dir); dir == "" {
			continue
		}
		if st, err := os.Stat(dir); err == nil && st.IsDir() {
			return filepath.Join(dir, legacySpoolFileName)
		}
	}
	return filepath.Join(os.TempDir(), legacySpoolFileName)
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
		log.Infof("tmx request log: %d rows from an earlier version's pending file moved to the spool", len(rows))
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

func (s *Store) insert(rows []Row) error { return s.insertContext(context.Background(), rows) }

func (s *Store) insertContext(ctx context.Context, rows []Row) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
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
	// An "unpriced" row also keeps that against an estimate (prices.go: estimates price new traffic only).
	keep := "((t.price_source IS NOT NULL AND t.price_source <> 'unpriced' OR t.price_source = 'unpriced' AND " +
		"EXCLUDED.price_source LIKE '" + pricing.EstimatePrefix + "%') AND " + strings.Join(same, " AND ") + ")"
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
