package requestlog

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const (
	defaultSchema        = "tmx"
	defaultRetentionDays = 365
	queueSize            = 10000
	batchSize            = 200
	flushInterval        = 2 * time.Second
	pruneInterval        = 6 * time.Hour
)

var schemaNamePattern = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Store owns the database handle and the background writer.
type Store struct {
	db            *sql.DB
	schema        string
	retentionDays int
	queue         chan Row
	dropped       atomic.Int64
	written       atomic.Int64
	writeErrors   atomic.Int64
	stopOnce      sync.Once
	done          chan struct{}
}

var current atomic.Pointer[Store]

// Default returns the running store, or nil when request logging is disabled.
func Default() *Store { return current.Load() }

// StartFromEnv starts request logging when a DSN is available:
//
//	TMX_REQUESTLOG=off                 disable
//	TMX_REQUESTLOG_DSN                 Postgres DSN (falls back to PGSTORE_DSN)
//	TMX_REQUESTLOG_SCHEMA              schema name (default "tmx")
//	TMX_REQUESTLOG_RETENTION_DAYS      rows older than this are pruned (default 365, 0 = keep)
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
	}
	if errSchema := s.ensureSchema(pingCtx); errSchema != nil {
		_ = db.Close()
		return nil, errSchema
	}
	go s.run()
	go s.backfillAccounting()
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
		`CREATE UNIQUE INDEX IF NOT EXISTS requests_import_ref_idx ON ` + s.table() + ` (import_ref) WHERE import_ref IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS requests_source_ts_idx ON ` + s.table() + ` (source, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS requests_key_ts_idx ON ` + s.table() + ` (api_key_hash, ts DESC)`,
	}
	for _, stmt := range stmts {
		if _, err := s.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

// Enqueue hands a row to the writer without blocking; when the queue is full the row is dropped
// and counted rather than slowing the request path.
func (s *Store) Enqueue(row Row) {
	if s == nil {
		return
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
		"queued":       int64(len(s.queue)),
	}
}

// Close flushes queued rows and stops the writer.
func (s *Store) Close() {
	if s == nil {
		return
	}
	s.stopOnce.Do(func() {
		close(s.queue)
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

	batch := make([]Row, 0, batchSize)
	flush := func() {
		if len(batch) == 0 {
			return
		}
		if err := s.insert(batch); err != nil {
			s.writeErrors.Add(1)
			log.WithError(err).Warnf("tmx request log: dropped batch of %d rows", len(batch))
		} else {
			s.written.Add(int64(len(batch)))
		}
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
		case <-ticker.C:
			flush()
		case <-prune.C:
			s.prune()
		}
	}
}

// columnNames and rowFields must stay in the same order; they drive insert and scan.
var columnNames = []string{"ts", "request_id", "trace_id", "session_id", "provider", "executor",
	"model", "alias", "response_model", "auth_id", "auth_index", "auth_label", "auth_type",
	"api_key_hash", "access_provider", "client_ip", "user_agent", "stream", "outcome",
	"status_code", "error", "latency_ms", "ttft_ms", "input_tokens", "output_tokens",
	"reasoning_tokens", "cached_tokens", "cache_read", "cache_creation", "total_tokens",
	"reasoning_effort", "service_tier", "uncached_input", "cache_write", "output_non_reasoning",
	"accounting_quality", "source", "machine", "import_ref", "shadowed"}

func rowFields(r *Row) []any {
	return []any{&r.TS, &r.RequestID, &r.TraceID, &r.SessionID, &r.Provider, &r.Executor,
		&r.Model, &r.Alias, &r.ResponseModel, &r.AuthID, &r.AuthIndex, &r.AuthLabel, &r.AuthType,
		&r.APIKeyHash, &r.AccessProvider, &r.ClientIP, &r.UserAgent, &r.Stream, &r.Outcome,
		&r.StatusCode, &r.Error, &r.LatencyMs, &r.TTFTMs, &r.InputTokens, &r.OutputTokens,
		&r.ReasoningTokens, &r.CachedTokens, &r.CacheRead, &r.CacheCreation, &r.TotalTokens,
		&r.ReasoningEffort, &r.ServiceTier, &r.UncachedInput, &r.CacheWrite, &r.OutputNonReasoning,
		&r.AccountingQuality, &r.Source, &r.Machine, &r.importRefPtr, &r.Shadowed}
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
	_, err := s.db.ExecContext(ctx, "INSERT INTO "+s.table()+" ("+insertColumns+") VALUES "+values, args...)
	return err
}

// UpsertImported writes usage rows derived from an imported session (source "extern:<harness>").
// Rows are keyed by ImportRef, so re-importing a session updates them instead of adding copies.
func (s *Store) UpsertImported(ctx context.Context, rows []Row) error {
	if s == nil || len(rows) == 0 {
		return nil
	}
	set := make([]string, 0, len(columnNames))
	for _, c := range columnNames {
		if c != "import_ref" {
			set = append(set, c+" = EXCLUDED."+c)
		}
	}
	for start := 0; start < len(rows); start += batchSize {
		end := min(start+batchSize, len(rows))
		values, args := insertValues(rows[start:end])
		if _, err := s.db.ExecContext(ctx, "INSERT INTO "+s.table()+" ("+insertColumns+") VALUES "+values+
			" ON CONFLICT (import_ref) WHERE import_ref IS NOT NULL DO UPDATE SET "+strings.Join(set, ", "),
			args...); err != nil {
			return err
		}
	}
	return nil
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
		"DELETE FROM "+s.table()+" WHERE ts < now() - make_interval(days => $1)", s.retentionDays)
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
