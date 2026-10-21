package archive

// Views over the analytics copy of tmx.requests (tokenmaxx fork): which session a usage row
// belongs to, its list-price cost (stored on the row by the request log, see
// requestlog/prices.go), and per-session totals. Recreated on start so they follow the code.

var analyticsViewsSQL = []string{
	`DROP VIEW IF EXISTS session_usage, request_costs, request_sessions CASCADE`, // dependents are recreated after
	`DROP TABLE IF EXISTS model_prices`,                                          // v2's own price table; costs are now stored per row
	// Which extracted session (and message) a usage row belongs to. Imported rows: import_ref
	// "<harness>:<session>#<message>". Proxied rows: the captured exchange (trace_id), else the
	// client session id. canonical_session_id prefers the imported transcript over its proxied twin.
	`CREATE VIEW request_sessions AS
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
