package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
)

// RebuildResult reports what RebuildUsage found and wrote.
type RebuildResult struct {
	Sessions int  `json:"sessions"`
	Missing  int  `json:"missing"` // stored messages with usage that had no request-log row
	Written  int  `json:"written"`
	DryRun   bool `json:"dry_run"`
}

// RebuildUsage restores imported usage rows from the archive's stored messages: every message
// with usage whose request-log row (import_ref "<session>#<message>") is missing gets it back,
// built by the same code as ingest. Existing rows are never touched, so it is safe to re-run.
// harness narrows it to one harness ("" = all).
func (a *Archive) RebuildUsage(ctx context.Context, harness string, dryRun bool) (*RebuildResult, error) {
	store := requestlog.Default()
	if store == nil {
		return nil, fmt.Errorf("request log is not running")
	}
	q := `SELECT s.id, s.harness, s.external_id, s.harness_version, s.machine, s.via_proxy, s.started_at,
			m.external_id, m.ts, m.model, m.usage
		FROM ` + schema + `.session_messages m JOIN ` + schema + `.sessions s ON s.id = m.session_id
		WHERE m.usage IS NOT NULL AND ($1 = '' OR s.harness = $1)
		  AND NOT EXISTS (SELECT 1 FROM ` + store.Schema() + `.requests r WHERE r.import_ref = m.session_id || '#' || m.external_id)
		ORDER BY s.id, m.seq`
	rows, err := a.db.QueryContext(ctx, q, harness)
	if err != nil {
		return nil, err
	}
	type item struct {
		key string
		in  SessionIngest
		via *bool
	}
	var order []string
	bySession := map[string]*item{}
	res := &RebuildResult{DryRun: dryRun}
	for rows.Next() {
		var id, h, ext, ver, machine, msgID, model string
		var via sql.NullBool
		var started, ts sql.NullTime
		var usage []byte
		if err = rows.Scan(&id, &h, &ext, &ver, &machine, &via, &started, &msgID, &ts, &model, &usage); err != nil {
			_ = rows.Close()
			return nil, err
		}
		it := bySession[id]
		if it == nil {
			it = &item{key: id, in: SessionIngest{Harness: h, ExternalID: ext, HarnessVersion: ver, Machine: machine}}
			if via.Valid {
				v := via.Bool
				it.via = &v
			}
			if started.Valid {
				st := started.Time
				it.in.StartedAt = &st
			}
			bySession[id] = it
			order = append(order, id)
		}
		m := MessageIngest{ExternalID: msgID, Model: model, Usage: json.RawMessage(usage)}
		if ts.Valid {
			t := ts.Time
			m.TS = &t
		}
		it.in.Messages = append(it.in.Messages, m)
		res.Missing++
	}
	if err = rows.Close(); err != nil {
		return nil, err
	}
	res.Sessions = len(order)
	if dryRun {
		return res, nil
	}
	for _, id := range order {
		it := bySession[id]
		var since *time.Time
		if it.via != nil && *it.via {
			since = a.proxiedSince(ctx, store, it.in.ExternalID)
		}
		built := usageRows(it.in, it.key, it.via, since)
		if err = store.UpsertImported(ctx, built); err != nil {
			return res, fmt.Errorf("session %s: %w", id, err)
		}
		res.Written += len(built)
	}
	return res, nil
}

// Reshadow recomputes the shadowed flag of every imported usage row with the per-message rule
// (usageRows): only rows from the session's first proxied call on duplicate proxied traffic.
// Rows written when a whole session was shadowed get their direct turns back. Needs the request
// log in the archive database; idempotent.
func (a *Archive) Reshadow(ctx context.Context) (int64, error) {
	store := requestlog.Default()
	if !a.usageInArchiveDB(ctx, store) {
		return 0, nil
	}
	t := store.Schema() + ".requests"
	// Same rule as proxiedSince: exchanges with the client's id, proxied rows under it or under
	// the canonical ids those exchanges carry.
	tag, err := a.db.ExecContext(ctx, `WITH first AS (
			SELECT s.id, COALESCE(s.via_proxy, false) AS via, LEAST(
				(SELECT min(e.ts) FROM `+schema+`.exchanges e
					WHERE e.client_session_id = s.external_id OR e.session_id = s.external_id),
				(SELECT min(p.ts) FROM `+t+` p WHERE p.source = $1 AND (p.session_id = s.external_id
					OR p.session_id IN (SELECT e.session_id FROM `+schema+`.exchanges e
						WHERE e.client_session_id = s.external_id AND e.session_id <> '')))) AS since
			FROM `+schema+`.sessions s)
		UPDATE `+t+` r SET shadowed = (f.via AND f.since IS NOT NULL AND r.ts >= f.since)
		FROM first f
		WHERE r.import_ref IS NOT NULL AND split_part(r.import_ref, '#', 1) = f.id
			AND r.shadowed IS DISTINCT FROM (f.via AND f.since IS NOT NULL AND r.ts >= f.since)`, requestlog.SourceProxy)
	if err != nil {
		return 0, err
	}
	return tag.RowsAffected()
}

// CacheTTLResult reports what BackfillCacheTTL found.
type CacheTTLResult struct {
	Scanned int   `json:"scanned"` // proxied rows with cache writes and a captured response
	Updated int   `json:"updated"` // rows that got their 1-hour share (and were repriced)
	Tokens  int64 `json:"tokens_1h"`
	DryRun  bool  `json:"dry_run"`
}

var ephemeral1hRe = regexp.MustCompile(`"ephemeral_1h_input_tokens"\s*:\s*(\d+)`)

// BackfillCacheTTL fills cache_write_1h for proxied rows written before it was recorded, from the
// usage in their captured responses (Anthropic cache_creation.ephemeral_1h_input_tokens), and
// sends those rows back to pricing. Rows without a captured response are left as 5-minute writes.
// Idempotent: rows that already have a 1-hour share are skipped.
func (a *Archive) BackfillCacheTTL(ctx context.Context, dryRun bool) (*CacheTTLResult, error) {
	store := requestlog.Default()
	if !a.usageInArchiveDB(ctx, store) {
		return nil, fmt.Errorf("the request log is not in the archive database")
	}
	t := store.Schema() + ".requests"
	rows, err := a.db.QueryContext(ctx, `SELECT r.id, r.cache_write, e.response_body FROM `+t+` r
		JOIN `+schema+`.exchanges e ON e.trace_id = r.trace_id
		WHERE r.source = $1 AND r.cache_write > 0 AND r.cache_write_1h = 0 AND r.trace_id <> ''
			AND e.response_body <> '' ORDER BY r.id`, requestlog.SourceProxy)
	if err != nil {
		return nil, err
	}
	type item struct {
		id, write int64
		body      string
	}
	var items []item
	for rows.Next() {
		var it item
		if err = rows.Scan(&it.id, &it.write, &it.body); err != nil {
			_ = rows.Close()
			return nil, err
		}
		items = append(items, it)
	}
	if err = closeRows(rows); err != nil {
		return nil, err
	}
	res := &CacheTTLResult{Scanned: len(items), DryRun: dryRun}
	for start := 0; start < len(items); start += 500 {
		chunk := items[start:min(start+500, len(items))]
		hashes := make([]string, len(chunk))
		for i, it := range chunk {
			hashes[i] = it.body
		}
		a.blobs.Prefetch(ctx, hashes)
		for _, it := range chunk {
			body, errB := a.blobs.Get(ctx, it.body)
			if errB != nil {
				continue
			}
			var oneHour int64
			for _, m := range ephemeral1hRe.FindAllSubmatch(body, -1) {
				if v, errP := strconv.ParseInt(string(m[1]), 10, 64); errP == nil && v > oneHour {
					oneHour = v
				}
			}
			oneHour = min(oneHour, it.write)
			if oneHour <= 0 {
				continue
			}
			res.Updated++
			res.Tokens += oneHour
			if dryRun {
				continue
			}
			if _, err = a.db.ExecContext(ctx, `UPDATE `+t+` SET cache_write_1h = $2, cost_input = NULL,
				cost_cache_read = NULL, cost_cache_write = NULL, cost_output = NULL, cost_usd = NULL,
				priced_as = '', price_source = NULL, priced_at = NULL WHERE id = $1`, it.id, oneHour); err != nil {
				return res, err
			}
		}
	}
	if res.Updated > 0 && !dryRun {
		if _, err = store.PricePending(ctx, false); err != nil {
			return res, fmt.Errorf("repricing: %w", err)
		}
	}
	return res, nil
}
