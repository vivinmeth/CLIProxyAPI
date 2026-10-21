// Package archive keeps every byte of model traffic (tokenmaxx fork): full proxied exchanges
// captured in the proxy, plus sessions ingested from harnesses that never went through it.
//
// Content lives in a content-addressed blob store (local spool -> Cloudflare R2, zstd), the
// index in Postgres schema tmx_archive. Nothing is redacted except credential headers.
package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // "pgx" database/sql driver
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	coresession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
	log "github.com/sirupsen/logrus"
)

const (
	schema             = "tmx_archive"
	defaultBucket      = "tmx-bodies"
	defaultSpool       = "/CLIProxyAPI/archive-spool"
	queueBytesLimit    = 256 << 20 // in-flight exchange bytes before new captures are dropped
	queueLengthLimit   = 4096
	captureWorkerCount = 2
)

// Archive is the running archive service.
type Archive struct {
	db      *sql.DB
	blobs   *BlobStore
	env     string
	cost    costTracker
	storage storageCache

	analytics *analytics // derived query layer; nil when TMX_ANALYTICS_DSN is unset

	queue         chan *capture.Exchange
	queueBytes    atomic.Int64
	captured      atomic.Int64
	dropped       atomic.Int64
	failed        atomic.Int64
	wg            sync.WaitGroup
	closeOnce     sync.Once
	closeWaitOnce sync.Once
	closeDone     chan struct{}
	closing       atomic.Bool
	queueMu       sync.RWMutex
	pending       sync.Map // accepted exchanges -> completion channels
}

var current atomic.Pointer[Archive]

// Default returns the running archive or nil.
func Default() *Archive { return current.Load() }

// StartFromEnv starts the archive when configured:
//
//	TMX_ARCHIVE=on                   enable (default off)
//	TMX_ARCHIVE_ENV                  folder in the bucket: prod | staging | dev (required)
//	TMX_ARCHIVE_DSN                  Postgres DSN (falls back to TMX_REQUESTLOG_DSN, PGSTORE_DSN)
//	TMX_ARCHIVE_R2_ENDPOINT          e.g. <account>.r2.cloudflarestorage.com (no scheme)
//	TMX_ARCHIVE_R2_ACCESS_KEY_ID / TMX_ARCHIVE_R2_SECRET_ACCESS_KEY
//	TMX_ARCHIVE_BUCKET               default tmx-bodies
//	TMX_ARCHIVE_SPOOL                local spool dir (default /CLIProxyAPI/archive-spool; mount a volume)
//	TMX_ARCHIVE_CAPTURE=off          keep the ingest API but don't capture proxied traffic
//
// Without R2 credentials blobs stay in the spool (dev). Failures never block startup.
func StartFromEnv(ctx context.Context) *Archive {
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("TMX_ARCHIVE")), "on") {
		return nil
	}
	env := strings.TrimSpace(os.Getenv("TMX_ARCHIVE_ENV"))
	if env != "prod" && env != "staging" && env != "dev" {
		log.Error("tmx archive: TMX_ARCHIVE_ENV must be prod, staging or dev; archive disabled")
		return nil
	}
	dsn := firstEnv("TMX_ARCHIVE_DSN", "TMX_REQUESTLOG_DSN", "PGSTORE_DSN")
	if dsn == "" {
		log.Error("tmx archive: no Postgres DSN; archive disabled")
		return nil
	}
	var r2 *minio.Client
	if endpoint := strings.TrimSpace(os.Getenv("TMX_ARCHIVE_R2_ENDPOINT")); endpoint != "" {
		client, err := minio.New(strings.TrimPrefix(endpoint, "https://"), &minio.Options{
			Creds: credentials.NewStaticV4(os.Getenv("TMX_ARCHIVE_R2_ACCESS_KEY_ID"),
				os.Getenv("TMX_ARCHIVE_R2_SECRET_ACCESS_KEY"), ""),
			Secure: true,
			Region: "auto",
		})
		if err != nil {
			log.WithError(err).Error("tmx archive: R2 client; archive disabled")
			return nil
		}
		r2 = client
	} else {
		log.Warn("tmx archive: no R2 endpoint, blobs stay in the local spool")
	}
	bucket := strings.TrimSpace(os.Getenv("TMX_ARCHIVE_BUCKET"))
	if bucket == "" {
		bucket = defaultBucket
	}
	spool := strings.TrimSpace(os.Getenv("TMX_ARCHIVE_SPOOL"))
	if spool == "" {
		spool = defaultSpool
	}
	a, err := Open(ctx, dsn, r2, bucket, env, spool)
	if err != nil {
		log.WithError(err).Error("tmx archive: disabled")
		return nil
	}
	a.startAnalytics(ctx)
	current.Store(a)
	go func() {
		// Imported usage rows written when a whole session was shadowed get their direct turns back.
		if n, errR := a.Reshadow(context.Background()); errR != nil {
			log.WithError(errR).Warn("tmx archive: reshadowing imported usage rows failed")
		} else if n > 0 {
			log.Infof("tmx archive: reshadowed %d imported usage rows", n)
		}
	}()
	if !strings.EqualFold(strings.TrimSpace(os.Getenv("TMX_ARCHIVE_CAPTURE")), "off") {
		capture.SetSink(a.enqueue)
	}
	log.Infof("tmx archive: on (env %s, bucket %s, spool %s, capture %v)", env, bucket, spool, capture.Enabled())
	return a
}

func firstEnv(keys ...string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(os.Getenv(k)); v != "" {
			return v
		}
	}
	return ""
}

// Open connects and starts the workers.
func Open(ctx context.Context, dsn string, r2 *minio.Client, bucket, env, spool string) (*Archive, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(6)
	db.SetMaxIdleConns(3)
	pingCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err = db.PingContext(pingCtx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if err = ensureSchema(pingCtx, db); err != nil {
		_ = db.Close()
		return nil, err
	}
	blobs, err := newBlobStore(db, schema, r2, bucket, env, spool)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	a := &Archive{db: db, blobs: blobs, env: env, queue: make(chan *capture.Exchange, queueLengthLimit)}
	for i := 0; i < captureWorkerCount; i++ {
		a.wg.Add(1)
		go a.worker()
	}
	go a.costWatch()
	return a, nil
}

func ensureSchema(ctx context.Context, db *sql.DB) error {
	stmts := []string{
		`CREATE SCHEMA IF NOT EXISTS ` + schema,
		`CREATE TABLE IF NOT EXISTS ` + schema + `.blobs (
			hash        TEXT PRIMARY KEY,
			size        BIGINT NOT NULL,
			stored_size BIGINT NOT NULL,
			media_type  TEXT NOT NULL DEFAULT '',
			kind        TEXT NOT NULL DEFAULT '',
			created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			uploaded_at TIMESTAMPTZ
		)`,
		`CREATE TABLE IF NOT EXISTS ` + schema + `.exchanges (
			trace_id                TEXT PRIMARY KEY,
			ts                      TIMESTAMPTZ NOT NULL,
			finished_at             TIMESTAMPTZ,
			session_id              TEXT NOT NULL DEFAULT '',
			method                  TEXT NOT NULL DEFAULT '',
			url                     TEXT NOT NULL DEFAULT '',
			status                  INTEGER NOT NULL DEFAULT 0,
			streaming               BOOLEAN NOT NULL DEFAULT FALSE,
			first_chunk_ms          BIGINT NOT NULL DEFAULT 0,
			duration_ms             BIGINT NOT NULL DEFAULT 0,
			request_headers         TEXT NOT NULL DEFAULT '',
			request_body            TEXT NOT NULL DEFAULT '',
			response_headers        TEXT NOT NULL DEFAULT '',
			response_body           TEXT NOT NULL DEFAULT '',
			response_chunks         TEXT NOT NULL DEFAULT '',
			upstream_request        TEXT NOT NULL DEFAULT '',
			upstream_response       TEXT NOT NULL DEFAULT '',
			ws_timeline             TEXT NOT NULL DEFAULT '',
			upstream_ws_timeline    TEXT NOT NULL DEFAULT '',
			errors                  JSONB NOT NULL DEFAULT '[]',
			request_bytes           BIGINT NOT NULL DEFAULT 0,
			response_bytes          BIGINT NOT NULL DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS exchanges_ts_idx ON ` + schema + `.exchanges (ts DESC)`,
		`CREATE INDEX IF NOT EXISTS exchanges_session_idx ON ` + schema + `.exchanges (session_id, ts) WHERE session_id <> ''`,
		// The client's own session id (Claude Code transcript id, Codex session) - what imports use.
		`ALTER TABLE ` + schema + `.exchanges ADD COLUMN IF NOT EXISTS client_session_id TEXT NOT NULL DEFAULT ''`,
		`ALTER TABLE ` + schema + `.blobs ADD COLUMN IF NOT EXISTS pack_key TEXT`,
		`ALTER TABLE ` + schema + `.blobs ADD COLUMN IF NOT EXISTS pack_offset BIGINT`,
		`CREATE INDEX IF NOT EXISTS exchanges_client_session_idx ON ` + schema + `.exchanges (client_session_id) WHERE client_session_id <> ''`,
		`CREATE TABLE IF NOT EXISTS ` + schema + `.sessions (
			id              TEXT PRIMARY KEY,
			harness         TEXT NOT NULL,
			external_id     TEXT NOT NULL,
			harness_version TEXT NOT NULL DEFAULT '',
			machine         TEXT NOT NULL DEFAULT '',
			project         TEXT NOT NULL DEFAULT '',
			cwd             TEXT NOT NULL DEFAULT '',
			title           TEXT NOT NULL DEFAULT '',
			via_proxy       BOOLEAN,
			started_at      TIMESTAMPTZ,
			ended_at        TIMESTAMPTZ,
			metadata        JSONB NOT NULL DEFAULT '{}',
			created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
			updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
			UNIQUE (harness, external_id)
		)`,
		`CREATE TABLE IF NOT EXISTS ` + schema + `.session_messages (
			session_id   TEXT NOT NULL REFERENCES ` + schema + `.sessions(id) ON DELETE CASCADE,
			external_id  TEXT NOT NULL,
			seq          BIGINT NOT NULL,
			parent_id    TEXT NOT NULL DEFAULT '',
			ts           TIMESTAMPTZ,
			role         TEXT NOT NULL DEFAULT '',
			model        TEXT NOT NULL DEFAULT '',
			message      TEXT NOT NULL,
			usage        JSONB,
			metadata     JSONB NOT NULL DEFAULT '{}',
			ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (session_id, external_id)
		)`,
		`CREATE INDEX IF NOT EXISTS session_messages_seq_idx ON ` + schema + `.session_messages (session_id, seq)`,
		`CREATE TABLE IF NOT EXISTS ` + schema + `.session_sources (
			session_id  TEXT NOT NULL REFERENCES ` + schema + `.sessions(id) ON DELETE CASCADE,
			name        TEXT NOT NULL,
			blob        TEXT NOT NULL,
			size        BIGINT NOT NULL DEFAULT 0,
			ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
			PRIMARY KEY (session_id, name, blob)
		)`,
		// Orchestrator threads (threads.go): the record as sent.
		`CREATE TABLE IF NOT EXISTS ` + schema + `.threads (
			id          TEXT PRIMARY KEY,           -- <source>:<external_id>
			source      TEXT NOT NULL,              -- t3
			external_id TEXT NOT NULL,
			machine     TEXT NOT NULL DEFAULT '',
			doc         JSONB NOT NULL,             -- ThreadIngest
			created_at  TIMESTAMPTZ,
			updated_at  TIMESTAMPTZ,
			ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		// Git commits (commits.go): the record as sent.
		`CREATE TABLE IF NOT EXISTS ` + schema + `.commits (
			id           TEXT PRIMARY KEY,           -- <repo>@<sha>
			repo         TEXT NOT NULL,
			sha          TEXT NOT NULL,
			machine      TEXT NOT NULL DEFAULT '',
			doc          JSONB NOT NULL,             -- CommitIngest
			committed_at TIMESTAMPTZ,
			ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		// Changes for the analytics worker (analytics.go), written with the change itself.
		`CREATE TABLE IF NOT EXISTS ` + schema + `.analytics_journal (
			id         BIGSERIAL PRIMARY KEY,
			kind       TEXT NOT NULL, -- session | exchange | proxy
			ref        TEXT NOT NULL,
			created_at TIMESTAMPTZ NOT NULL DEFAULT now()
		)`,
		`ALTER TABLE ` + schema + `.analytics_journal ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE ` + schema + `.analytics_journal ADD COLUMN IF NOT EXISTS not_before TIMESTAMPTZ`,
	}
	for _, stmt := range stmts {
		if _, err := db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

// enqueue is the capture sink. It never blocks: when the queue is over its byte or length limit
// the exchange is dropped and counted.
func (a *Archive) enqueue(ex *capture.Exchange) {
	a.queueMu.RLock()
	defer a.queueMu.RUnlock()
	if a.closing.Load() {
		ex.Release()
		a.drop()
		return
	}
	size := exchangeMemory(ex)
	if a.queueBytes.Add(size) > queueBytesLimit {
		a.queueBytes.Add(-size)
		ex.Release()
		a.drop()
		return
	}
	finished := make(chan struct{})
	a.pending.Store(ex, finished)
	select {
	case a.queue <- ex:
	default:
		a.pending.Delete(ex)
		close(finished)
		a.queueBytes.Add(-size)
		ex.Release()
		a.drop()
	}
}

func (a *Archive) drop() {
	if a.dropped.Add(1)%20 == 1 {
		log.Warn("tmx archive: capture queue full, dropping exchanges")
	}
}

func (a *Archive) worker() {
	defer a.wg.Done()
	for ex := range a.queue {
		size := exchangeMemory(ex)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		if err := a.storeExchange(ctx, ex); err != nil {
			if a.failed.Add(1)%20 == 1 {
				log.WithError(err).Warn("tmx archive: failed to store exchange")
			}
		} else {
			a.captured.Add(1)
		}
		cancel()
		ex.Release()
		a.queueBytes.Add(-size)
		if done, ok := a.pending.LoadAndDelete(ex); ok {
			close(done.(chan struct{}))
		}
	}
}

// credentialHeaders are the only thing removed from captures: live client keys and OAuth tokens.
var credentialHeaders = map[string]bool{
	"authorization": true, "proxy-authorization": true, "x-api-key": true, "x-goog-api-key": true,
	"api-key": true, "cookie": true, "set-cookie": true, "x-management-key": true,
}

func redactHeaders(h map[string][]string) map[string][]string {
	out := make(map[string][]string, len(h))
	for k, v := range h {
		if credentialHeaders[strings.ToLower(k)] {
			out[k] = []string{"[redacted]"}
			continue
		}
		out[k] = append([]string(nil), v...)
	}
	return out
}

func (a *Archive) storeExchange(ctx context.Context, ex *capture.Exchange) error {
	requestPrefix := capturePrefix(ex, "request", ex.RequestBody, 1<<20)
	clientSession := clientSessionID(ex.RequestHeaders, nil)
	if clientSession == "" {
		clientSession = ex.ClientSessionID
	}
	if clientSession == "" {
		clientSession = clientSessionID(ex.RequestHeaders, requestPrefix)
	}
	if ex.SessionID == "" {
		// The middleware runs before session resolution; derive it the same way upstream does so
		// exchanges join tmx.requests.session_id.
		if info, ok := coresession.ExtractSessionInfo(http.Header(ex.RequestHeaders), requestPrefix, nil); ok {
			ex.SessionID = coresession.NormalizeToCanonicalUUID(info.SessionID)
			if ex.SessionID == "" {
				ex.SessionID = info.SessionID
			}
		}
	}
	put := func(data []byte, mediaType, kind string) (string, error) {
		if len(data) == 0 {
			return "", nil
		}
		return a.blobs.Put(ctx, data, mediaType, kind)
	}
	headers := func(h map[string][]string, kind string) (string, error) {
		if len(h) == 0 {
			return "", nil
		}
		data, _ := json.Marshal(redactHeaders(h))
		return put(data, "application/json", kind)
	}
	var (
		row struct {
			reqHeaders, reqBody, respHeaders, respBody, chunks, upReq, upResp, ws, upWS string
		}
		err error
	)
	if row.reqHeaders, err = headers(ex.RequestHeaders, "request.headers"); err != nil {
		return err
	}
	if row.reqBody, err = a.putCapture(ctx, ex, "request", ex.RequestBody, "application/json", "request", true); err != nil {
		return err
	}
	if row.respHeaders, err = headers(ex.ResponseHeaders, "response.headers"); err != nil {
		return err
	}
	if row.respBody, err = a.putCapture(ctx, ex, "response", ex.ResponseBody, contentTypeOf(ex.ResponseHeaders), "response.body", false); err != nil {
		return err
	}
	if ex.Bodies["chunks"] != nil {
		row.chunks, err = a.putCapture(ctx, ex, "chunks", nil, "application/json", "response.chunks", false)
		if err != nil {
			return err
		}
	} else if len(ex.Chunks) > 0 {
		data, _ := json.Marshal(ex.Chunks)
		if row.chunks, err = put(data, "application/json", "response.chunks"); err != nil {
			return err
		}
	}
	if row.upReq, err = a.putCapture(ctx, ex, "upstream.request", ex.APIRequest, "text/plain", "upstream.request", true); err != nil {
		return err
	}
	if row.upResp, err = a.putCapture(ctx, ex, "upstream.response", ex.APIResponse, "text/plain", "upstream.response", false); err != nil {
		return err
	}
	if row.ws, err = a.putCapture(ctx, ex, "ws", ex.WebsocketTimeline, "text/plain", "ws.timeline", false); err != nil {
		return err
	}
	if row.upWS, err = a.putCapture(ctx, ex, "upstream.ws", ex.APIWebsocketTimeline, "text/plain", "upstream.ws.timeline", false); err != nil {
		return err
	}
	errorsJSON, _ := json.Marshal(append([]string{}, ex.Errors...))
	var firstChunk, duration int64
	if !ex.FirstChunkAt.IsZero() {
		firstChunk = ex.FirstChunkAt.Sub(ex.StartedAt).Milliseconds()
	}
	if !ex.FinishedAt.IsZero() {
		duration = ex.FinishedAt.Sub(ex.StartedAt).Milliseconds()
	}
	traceID := ex.TraceID
	if traceID == "" {
		traceID = "anon-" + HashOf([]byte(fmt.Sprintf("%s|%s|%d", ex.URL, ex.StartedAt, len(ex.RequestBody))))[:24]
	}
	insert := `INSERT INTO ` + schema + `.exchanges (trace_id, ts, finished_at,
		session_id, method, url, status, streaming, first_chunk_ms, duration_ms, request_headers,
		request_body, response_headers, response_body, response_chunks, upstream_request,
		upstream_response, ws_timeline, upstream_ws_timeline, errors, request_bytes, response_bytes,
		client_session_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
		ON CONFLICT (trace_id) DO NOTHING`
	if a.analytics != nil {
		// One statement, so the journal entry exists exactly when the exchange row does.
		insert = `WITH ins AS (` + insert + ` RETURNING trace_id) INSERT INTO ` + journalTable +
			` (kind, ref) SELECT 'exchange', trace_id FROM ins`
	}
	_, err = a.db.ExecContext(ctx, insert,
		traceID, ex.StartedAt.UTC(), nullTime(ex.FinishedAt), ex.SessionID, ex.Method, ex.URL, ex.Status,
		ex.Streaming, firstChunk, duration, row.reqHeaders, row.reqBody, row.respHeaders, row.respBody,
		row.chunks, row.upReq, row.upResp, row.ws, row.upWS, string(errorsJSON),
		ex.BodySize("request", ex.RequestBody), ex.BodySize("response", ex.ResponseBody), clientSession)
	if err == nil && a.analytics != nil {
		a.analytics.poke()
	}
	if err == nil {
		// Requests the proxy refused before trying any account never reach the usage plugin;
		// record them here so they show up in the request log as "rejected".
		if rej, ok := rejectedRow(ex); ok {
			rej.TraceID, rej.RequestID = traceID, "rejected:"+traceID
			requestlog.Default().Enqueue(rej)
		}
	}
	return err
}

// clientSessionID is the session id as the client knows it: Claude Code's X-Claude-Code-Session-Id
// (also its transcript file name and metadata.user_id.session_id), Codex's session_id header.
func clientSessionID(headers map[string][]string, body []byte) string {
	for k, v := range headers {
		switch strings.ToLower(k) {
		case "x-claude-code-session-id", "session_id", "session-id", "x-session-id", "conversation_id":
			if len(v) > 0 && strings.TrimSpace(v[0]) != "" {
				return strings.TrimSpace(v[0])
			}
		}
	}
	return coresession.ClaudeMetadataSessionID(body)
}

func contentTypeOf(h map[string][]string) string {
	for k, v := range h {
		if strings.EqualFold(k, "Content-Type") && len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

func nullTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t.UTC()
}

func sniffMediaType(b []byte) string {
	return http.DetectContentType(b)
}

// Stats for the status endpoint.
func (a *Archive) Stats() map[string]any {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cost, errCost := a.Cost(ctx)
	if errCost != nil {
		log.WithError(errCost).Debug("tmx archive: cost estimate failed")
	}
	spillFiles, spillBytes := capture.LiveSpillStats(a.blobs.spool)
	return map[string]any{
		"live_spill_files":     spillFiles,
		"live_spill_bytes":     spillBytes,
		"r2_cost_estimate":     cost,
		"env":                  a.env,
		"capture":              capture.Enabled(),
		"live_capture_bytes":   capture.LiveBytes(),
		"live_capture_dropped": capture.Dropped(),
		"captured":             a.captured.Load(),
		"dropped":              a.dropped.Load(),
		"failed":               a.failed.Load(),
		"queued":               len(a.queue),
		"queued_bytes":         a.queueBytes.Load(),
		"blobs_stored":         a.blobs.stored.Load(),
		"blobs_deduped":        a.blobs.dedup.Load(),
		"blobs_uploaded":       a.blobs.uploaded.Load(),
		"packs_uploaded":       a.blobs.packs.Load(),
		"read_cache":           a.blobs.cache.stats(),
		"upload_failures":      a.blobs.failures.Load(),
		"unbacked_swept":       a.blobs.swept.Load(),
		"spool_backlog":        a.blobs.SpoolBacklog(),
		"r2_configured":        a.blobs.r2 != nil,
		"analytics":            a.analytics != nil,
	}
}

// Close stops capture, drains queued exchanges and pushes spooled blobs.
func (a *Archive) Close() {
	if a == nil {
		return
	}
	a.closeOnce.Do(func() {
		capture.SetSink(nil)
		a.queueMu.Lock()
		a.closing.Store(true)
		close(a.queue)
		a.queueMu.Unlock()
		a.wg.Wait()
		if a.analytics != nil {
			a.analytics.close()
		}
		a.blobs.close()
		_ = a.db.Close()
	})
}
