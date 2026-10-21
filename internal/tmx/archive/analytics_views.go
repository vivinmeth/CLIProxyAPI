package archive

// Views over the analytics copy of tmx.requests (tokenmaxx fork): which session a usage row
// belongs to, its list-price cost (stored on the row by the request log, see
// requestlog/prices.go), and per-session totals. Recreated on start so they follow the code.

import (
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
)

var analyticsViewsSQL = []string{
	`DROP VIEW IF EXISTS session_usage, request_costs CASCADE`, // dependents are recreated after
	// request_sessions was a plain view before it was materialized: drop whichever it is.
	`DO $$ BEGIN
		IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'request_sessions' AND relkind = 'v') THEN
			DROP VIEW request_sessions CASCADE;
		END IF;
		DROP MATERIALIZED VIEW IF EXISTS request_sessions CASCADE;
	END $$`,
	`DROP TABLE IF EXISTS model_prices`, // v2's own price table; costs are now stored per row
	// Which extracted session (and message) a usage row belongs to. Imported rows: import_ref
	// "<harness>:<session>#<message>". Proxied rows: the captured exchange (trace_id), else the
	// client session id. canonical_session_id prefers the imported transcript over its proxied twin.
	// Materialized: computing the link per row is most of the cost of session_usage / thread_usage.
	// The worker refreshes it after usage rows or sessions change (at most every linksRefresh);
	// it starts empty and is filled right after start.
	`CREATE MATERIALIZED VIEW request_sessions AS
	SELECT r.id AS request_id, x.id AS session_id, COALESCE(s.shadowed_by, x.id) AS canonical_session_id,
		m.seq AS message_seq, x.via
	FROM requests r
	LEFT JOIN LATERAL (
		SELECT c.id, c.via FROM (
			SELECT s.id, 'import_ref' AS via, 1 AS p FROM sessions s
				WHERE r.import_ref <> '' AND s.id = split_part(r.import_ref, '#', 1)
			UNION ALL
			SELECT s.id, 'trace_id', 2 FROM exchanges e JOIN sessions s ON s.id = 'proxy:' ||
				CASE WHEN e.client_session_id <> '' THEN e.client_session_id ELSE e.session_id END
				WHERE r.trace_id <> '' AND e.trace_id = r.trace_id
			UNION ALL
			SELECT s.id, 'session_id', 3 FROM sessions s WHERE r.session_id <> '' AND s.id = 'proxy:' || r.session_id
			UNION ALL
			SELECT s.id, 'session_id', 3 FROM sessions s WHERE r.session_id <> '' AND s.alt_ids @> ARRAY[r.session_id]
			UNION ALL
			SELECT s.id, 'external_id', 4 FROM sessions s
				WHERE r.session_id <> '' AND s.kind = 'imported' AND s.external_id = r.session_id
		) c ORDER BY c.p LIMIT 1
	) x ON true
	LEFT JOIN sessions s ON s.id = x.id
		LEFT JOIN messages m ON r.import_ref <> '' AND m.session_id = x.id
		AND m.external_id = split_part(r.import_ref, '#', 2)
	WITH NO DATA`,
	`CREATE UNIQUE INDEX request_sessions_request_idx ON request_sessions (request_id)`,
	`CREATE INDEX request_sessions_canonical_idx ON request_sessions (canonical_session_id)`,
	// List-price cost per usage row, as stored on the row when its tokens were written
	// (tmx.requests cost_* columns; NULL = unpriced or not priced yet).
	`CREATE VIEW request_costs AS
		SELECT r.id AS request_id, r.ts, r.source, r.harness, r.machine, r.provider, r.model, r.shadowed,
		r.auth_id, r.auth_label, r.auth_type, r.billing,
		r.priced_as, r.price_source, r.priced_at, r.speed, r.service_tier,
		r.uncached_input, r.cache_read, r.cache_write, r.cache_write_1h, r.output_tokens,
		r.cost_input, r.cost_cache_read, r.cost_cache_write, r.cost_output, r.cost_usd
	FROM requests r`,
	// Tokens and list-price cost per session (canonical: an imported transcript absorbs its
	// proxied twin). Rows of shadowed (double-counted) proxy usage are left out.
	`CREATE VIEW session_usage AS
	SELECT rs.canonical_session_id AS session_id, count(*) AS requests,
		min(c.ts) AS first_request, max(c.ts) AS last_request,
		sum(COALESCE(c.uncached_input, 0) + COALESCE(c.cache_read, 0) + COALESCE(c.cache_write, 0)) AS prompt_tokens,
		sum(COALESCE(c.cache_read, 0)) AS cache_read_tokens, sum(COALESCE(c.output_tokens, 0)) AS output_tokens,
		sum(c.cost_usd) AS cost_usd, count(*) FILTER (WHERE c.cost_usd IS NULL) AS unpriced_requests
	FROM request_sessions rs JOIN request_costs c ON c.request_id = rs.request_id
	WHERE rs.canonical_session_id IS NOT NULL AND NOT COALESCE(c.shadowed, false)
	GROUP BY rs.canonical_session_id`,
}

// linksRefresh: request_sessions (and so session_usage / thread_usage) lags usage rows and
// sessions by at most this much.
const linksRefresh = 2 * time.Minute

// refreshLinks refreshes request_sessions when something it reads changed and the last refresh
// is linksRefresh old, or regardless with force. The first fill after start is a plain refresh
// (the view starts empty); later ones are concurrent, so readers never wait.
func (an *analytics) refreshLinks(force bool) {
	if an.reqMirror == nil {
		return
	}
	var populated bool
	if err := an.db.QueryRowContext(an.ctx, `SELECT relispopulated FROM pg_class WHERE relname = 'request_sessions'
		AND relkind = 'm'`).Scan(&populated); err != nil {
		return
	}
	if populated && !force && (!an.linksDirty.Load() || time.Since(an.linksAt) < linksRefresh) {
		return
	}
	an.linksDirty.Store(false)
	stmt := `REFRESH MATERIALIZED VIEW CONCURRENTLY request_sessions`
	if !populated {
		stmt = `REFRESH MATERIALIZED VIEW request_sessions`
	}
	start := time.Now()
	if _, err := an.db.ExecContext(an.ctx, stmt); err != nil {
		an.linksDirty.Store(true)
		an.setErr(fmt.Errorf("refresh request_sessions: %w", err))
		return
	}
	an.linksAt = time.Now()
	log.Debugf("tmx analytics: request_sessions refreshed in %s", time.Since(start).Round(time.Millisecond))
}
