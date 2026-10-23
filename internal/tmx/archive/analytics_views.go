package archive

// Views over the analytics copy of tmx.requests (tokenmaxx fork): which session a usage row
// belongs to, its list-price cost (stored on the row by the request log, see
// requestlog/prices.go), and per-session totals. Recreated on start so they follow the code.

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

var analyticsViewsSQL = []string{
	`DROP VIEW IF EXISTS session_usage, request_costs CASCADE`, // dependents are recreated after
	// request_sessions was a plain view, then a materialized view, before it became a table.
	`DO $$ BEGIN
		IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'request_sessions' AND relkind = 'v') THEN
			DROP VIEW request_sessions CASCADE;
		END IF;
		IF EXISTS (SELECT 1 FROM pg_class WHERE relname = 'request_sessions' AND relkind = 'm') THEN
			DROP MATERIALIZED VIEW request_sessions CASCADE;
		END IF;
	END $$`,
	`DROP VIEW IF EXISTS request_links`,
	`DROP TABLE IF EXISTS model_prices`, // v2's own price table; costs are now stored per row
	// Which extracted session (and message) a usage row belongs to. Imported rows: import_ref
	// "<harness>:<session>#<message>". Proxied rows: the captured exchange (trace_id), else the
	// client session id. canonical_session_id prefers the imported transcript over its proxied twin.
	// request_links computes it; request_sessions keeps it (computing the link per row is most of
	// the cost of session_usage / thread_usage). The worker updates request_sessions for the usage
	// rows, sessions and exchanges each step changed (refreshLinks), and in full at start.
	`CREATE TABLE IF NOT EXISTS request_sessions (
		request_id           BIGINT PRIMARY KEY,
		session_id           TEXT,
		canonical_session_id TEXT,
		message_seq          INTEGER,
		via                  TEXT
	)`,
	`CREATE INDEX IF NOT EXISTS request_sessions_canonical_idx ON request_sessions (canonical_session_id)`,
	`CREATE INDEX IF NOT EXISTS request_sessions_session_idx ON request_sessions (session_id)`,
	`CREATE INDEX IF NOT EXISTS requests_import_session_idx ON requests (split_part(import_ref, '#', 1)) WHERE import_ref <> ''`,
	`CREATE VIEW request_links AS
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
		AND m.external_id = split_part(r.import_ref, '#', 2)`,
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

// linkDirty is what changed since request_sessions was last brought up to date (run loop only).
type linkDirty struct {
	full     bool // rebuild every row: at start, after a bulk reconcile, or when too much changed
	requests map[int64]bool
	sessions map[string]bool // analytics session ids (imported "<harness>:<id>", "proxy:<key>")
	traces   map[string]bool
}

// maxLinkDirty: past this many changed refs a full rebuild is cheaper than looking them up.
const maxLinkDirty = 20000

func (d *linkDirty) size() int { return len(d.requests) + len(d.sessions) + len(d.traces) }

func (d *linkDirty) reset() {
	d.full, d.requests, d.sessions, d.traces = false, map[int64]bool{}, map[string]bool{}, map[string]bool{}
}

// markLinks records refs whose request_sessions rows may have changed.
func (an *analytics) markLinks(kind, ref string) {
	d := &an.links
	if d.requests == nil {
		d.reset()
		d.full = true
	}
	switch kind {
	case "request":
		if id, err := strconv.ParseInt(ref, 10, 64); err == nil {
			d.requests[id] = true
		}
	case "session":
		d.sessions[ref] = true
	case "proxy":
		d.sessions["proxy:"+ref] = true
	case "exchange":
		d.traces[ref] = true
	case "requests_truncate":
		d.full = true
	}
	if d.size() > maxLinkDirty {
		d.full = true
	}
}

// linkUpsert writes request_links rows for the requests in ids (a subquery yielding request
// ids), touching only rows whose link changed, and drops rows of requests that are gone.
func linkUpsert(ids string) string {
	return `WITH ids AS (` + ids + `),
	fresh AS (SELECT l.* FROM request_links l WHERE l.request_id IN (SELECT id FROM ids)),
	up AS (INSERT INTO request_sessions SELECT * FROM fresh
		ON CONFLICT (request_id) DO UPDATE SET session_id = EXCLUDED.session_id,
			canonical_session_id = EXCLUDED.canonical_session_id, message_seq = EXCLUDED.message_seq, via = EXCLUDED.via
		WHERE (request_sessions.session_id, request_sessions.canonical_session_id, request_sessions.message_seq,
			request_sessions.via) IS DISTINCT FROM (EXCLUDED.session_id, EXCLUDED.canonical_session_id,
			EXCLUDED.message_seq, EXCLUDED.via)
		RETURNING 1)
	DELETE FROM request_sessions rs WHERE rs.request_id IN (SELECT id FROM ids)
		AND NOT EXISTS (SELECT 1 FROM fresh f WHERE f.request_id = rs.request_id)`
}

// linkAffected: the requests whose link a changed session ($1), usage row ($2) or exchange ($3)
// can move — rows linked to it now (or to its shadowed twin), and rows that may link to it next
// by import_ref, client session id (its own, an alias, or an imported external id) or trace.
const linkAffected = `WITH ds AS (SELECT unnest($1::text[]) AS id),
	keys AS (
		SELECT substr(id, 7) AS k FROM ds WHERE id LIKE 'proxy:%'
		UNION SELECT unnest(s.alt_ids) FROM sessions s JOIN ds ON ds.id = s.id
		UNION SELECT s.external_id FROM sessions s JOIN ds ON ds.id = s.id WHERE s.kind = 'imported'
	),
	traces AS (
		SELECT unnest($3::text[]) AS t
		UNION SELECT e.trace_id FROM exchanges e JOIN keys ON keys.k = e.client_session_id
		UNION SELECT e.trace_id FROM exchanges e JOIN keys ON keys.k = e.session_id
	)
	SELECT rs.request_id AS id FROM request_sessions rs
		WHERE rs.session_id IN (SELECT id FROM ds) OR rs.canonical_session_id IN (SELECT id FROM ds)
	UNION SELECT rs.request_id FROM request_sessions rs JOIN sessions s ON s.id = rs.session_id
		WHERE s.shadowed_by IN (SELECT id FROM ds)
	UNION SELECT r.id FROM requests r WHERE r.import_ref <> '' AND split_part(r.import_ref, '#', 1) IN (SELECT id FROM ds)
	UNION SELECT r.id FROM requests r WHERE r.session_id <> '' AND r.session_id IN (SELECT k FROM keys)
	UNION SELECT r.id FROM requests r WHERE r.trace_id <> '' AND r.trace_id IN (SELECT t FROM traces)
	UNION SELECT unnest($2::bigint[])`

// refreshLinks brings request_sessions up to date: the rows the last steps' changes can move, or
// every row (force, at start, after a bulk reconcile). Only changed rows are written, so readers
// never see it empty and a quiet step costs nothing.
func (an *analytics) refreshLinks(force bool) {
	if an.reqMirror == nil {
		return
	}
	d := &an.links
	if d.requests == nil {
		d.reset()
		d.full = true
	}
	if force {
		d.full = true
	}
	if !d.full && d.size() == 0 {
		return
	}
	start := time.Now()
	var err error
	if d.full {
		_, err = an.db.ExecContext(an.ctx, linkUpsert(`SELECT id FROM requests UNION SELECT request_id FROM request_sessions`))
	} else {
		reqs := make([]string, 0, len(d.requests))
		for id := range d.requests {
			reqs = append(reqs, strconv.FormatInt(id, 10))
		}
		_, err = an.db.ExecContext(an.ctx, linkUpsert(linkAffected), pgArray(mapKeys(d.sessions)),
			"{"+strings.Join(reqs, ",")+"}", pgArray(mapKeys(d.traces)))
	}
	if err != nil {
		an.setErr(fmt.Errorf("refresh request_sessions: %w", err))
		return // kept: the next refresh retries
	}
	full := d.full
	d.reset()
	log.Debugf("tmx analytics: request_sessions updated (full=%v) in %s", full, time.Since(start).Round(time.Millisecond))
}

func mapKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
