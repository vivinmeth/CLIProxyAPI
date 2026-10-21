package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	coresession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
)

// ImportUsage is the normalised usage object an ingested message may carry (field "usage").
// Token meanings match tmx.requests: prompt = uncached_input + cache_read + cache_write;
// output is the TOTAL output and reasoning is the part of it spent on thinking.
type ImportUsage struct {
	Provider          string `json:"provider"`
	Model             string `json:"model"`
	Account           string `json:"account"`
	UncachedInput     int64  `json:"uncached_input"`
	CacheRead         int64  `json:"cache_read"`
	CacheWrite        int64  `json:"cache_write"`
	CacheWrite1h      int64  `json:"cache_write_1h"` // part of cache_write with a 1-hour TTL (Anthropic)
	Output            int64  `json:"output"`
	Reasoning         int64  `json:"reasoning"`
	Outcome           string `json:"outcome"`
	StatusCode        int    `json:"status_code"`
	LatencyMs         int64  `json:"latency_ms"`
	TTFTMs            int64  `json:"ttft_ms"`
	ServiceTier       string `json:"service_tier"` // the tier billed (OpenAI priority / flex …)
	Speed             string `json:"speed"`        // Anthropic usage.speed ("fast")
	AccountingQuality string `json:"accounting_quality"`
}

// SourceFor is the tmx.requests source label of an imported harness.
func SourceFor(harness string) string { return "extern:" + harness }

// usageRows turns messages with usage into request-log rows. A row is shadowed when the proxy
// already recorded that traffic: the session went through the proxy (viaProxy) and the message
// is from proxiedSince (the session's first proxied call) on. Direct turns before it count.
func usageRows(in SessionIngest, sessionKey string, viaProxy *bool, proxiedSince *time.Time) []requestlog.Row {
	var rows []requestlog.Row
	for _, m := range in.Messages {
		if len(strings.TrimSpace(string(m.Usage))) == 0 || string(m.Usage) == "null" {
			continue
		}
		var u ImportUsage
		if err := json.Unmarshal(m.Usage, &u); err != nil {
			continue
		}
		ts := time.Now().UTC()
		switch {
		case m.TS != nil && !m.TS.IsZero():
			ts = m.TS.UTC()
		case in.StartedAt != nil && !in.StartedAt.IsZero():
			ts = in.StartedAt.UTC()
		}
		model := u.Model
		if model == "" {
			model = m.Model
		}
		outcome := u.Outcome
		if outcome == "" {
			outcome = requestlog.OutcomeSuccess
		}
		quality := u.AccountingQuality
		if quality == "" {
			quality = "complete"
		}
		nonReasoning := u.Output - u.Reasoning
		if nonReasoning < 0 {
			nonReasoning = 0
		}
		externalID := strings.TrimSpace(m.ExternalID)
		shadowed := viaProxy != nil && *viaProxy && proxiedSince != nil && !ts.Before(*proxiedSince)
		rows = append(rows, requestlog.Row{
			TS:                 ts,
			RequestID:          externalID,
			SessionID:          in.ExternalID,
			Provider:           u.Provider,
			Executor:           in.Harness,
			Model:              model,
			Alias:              model,
			ResponseModel:      model,
			AuthLabel:          u.Account,
			UserAgent:          strings.TrimSpace(in.Harness + "/" + in.HarnessVersion),
			Outcome:            outcome,
			StatusCode:         u.StatusCode,
			LatencyMs:          u.LatencyMs,
			TTFTMs:             u.TTFTMs,
			InputTokens:        u.UncachedInput,
			OutputTokens:       u.Output,
			ReasoningTokens:    u.Reasoning,
			CacheRead:          u.CacheRead,
			CacheCreation:      u.CacheWrite,
			TotalTokens:        u.UncachedInput + u.CacheRead + u.CacheWrite + u.Output,
			ServiceTier:        u.ServiceTier,
			Speed:              u.Speed,
			UncachedInput:      u.UncachedInput,
			CacheWrite:         u.CacheWrite,
			CacheWrite1h:       min(max(u.CacheWrite1h, 0), u.CacheWrite),
			OutputNonReasoning: nonReasoning,
			AccountingQuality:  quality,
			Source:             SourceFor(in.Harness),
			Machine:            in.Machine,
			ImportRef:          sessionKey + "#" + externalID,
			Shadowed:           shadowed,
		})
	}
	return rows
}

// usageRefsWithout lists the usage keys of messages sent without usage: a re-import can move a
// message's usage elsewhere (Claude Code attaches it to the last streamed chunk), and the old row
// must go.
func usageRefsWithout(in SessionIngest, sessionKey string) []string {
	var refs []string
	for _, m := range in.Messages {
		if len(strings.TrimSpace(string(m.Usage))) == 0 || string(m.Usage) == "null" {
			refs = append(refs, sessionKey+"#"+strings.TrimSpace(m.ExternalID))
		}
	}
	return refs
}

// proxiedSince is when the proxy first served this session (nil when it never did): the earliest
// captured exchange carrying the client's session id, or proxied usage row under it or under the
// canonical ids the proxy gave it (exchanges map one to the other).
func (a *Archive) proxiedSince(ctx context.Context, store *requestlog.Store, externalID string) *time.Time {
	ids := []string{externalID}
	// Subagents are imported as "<parent>:<agent>" but reach the proxy under the parent's session
	// id, so the parent's proxied traffic counts for them.
	if parent, _, ok := strings.Cut(externalID, ":"); ok && parent != "" {
		ids = append(ids, parent)
	}
	for _, id := range ids[:len(ids):len(ids)] {
		if c := coresession.NormalizeToCanonicalUUID(id); c != id && c != "" {
			ids = append(ids, c)
		}
	}
	var first *time.Time
	consider := func(t sql.NullTime) {
		if t.Valid && (first == nil || t.Time.Before(*first)) {
			v := t.Time.UTC()
			first = &v
		}
	}
	var canonical []string
	if rows, err := a.db.QueryContext(ctx, `SELECT session_id, min(ts) FROM `+schema+`.exchanges
		WHERE client_session_id = ANY($1::text[]) OR session_id = ANY($1::text[]) GROUP BY session_id`,
		pgTextArray(ids)); err == nil {
		for rows.Next() {
			var sid string
			var ts sql.NullTime
			if rows.Scan(&sid, &ts) == nil {
				consider(ts)
				if sid != "" {
					canonical = append(canonical, sid)
				}
			}
		}
		_ = rows.Close()
	}
	if store != nil {
		var reqFirst sql.NullTime
		if store.DB().QueryRowContext(ctx, `SELECT min(ts) FROM `+store.Schema()+`.requests
			WHERE source = $1 AND session_id = ANY($2::text[])`, requestlog.SourceProxy,
			pgTextArray(append(ids, canonical...))).Scan(&reqFirst) == nil {
			consider(reqFirst)
		}
	}
	return first
}

// writeUsage upserts the session's usage rows, drops rows of messages that no longer carry usage
// and, on replace, rows of messages that are gone. ex is the ingest transaction when the request
// log lives in the archive database, so usage and archive commit together.
func (a *Archive) writeUsage(ctx context.Context, ex requestlog.Execer, in SessionIngest, sessionKey string,
	viaProxy *bool, since *time.Time, res *IngestResult) error {
	store := requestlog.Default()
	if store == nil {
		return nil
	}
	rows := usageRows(in, sessionKey, viaProxy, since)
	res.UsageRows = len(rows)
	if err := store.UpsertImportedWith(ctx, ex, rows); err != nil {
		return fmt.Errorf("usage rows: %w", err)
	}
	if stale := usageRefsWithout(in, sessionKey); len(stale) > 0 {
		tag, err := ex.ExecContext(ctx, `DELETE FROM `+store.Schema()+`.requests WHERE import_ref = ANY($1::text[])`,
			pgTextArray(stale))
		if err != nil {
			return fmt.Errorf("stale usage rows: %w", err)
		}
		n, _ := tag.RowsAffected()
		res.UsageRemoved += n
	}
	if in.Replace {
		keep := make([]string, 0, len(rows))
		for _, row := range rows {
			keep = append(keep, row.ImportRef)
		}
		n, err := store.DeleteImportedWith(ctx, ex, sessionKey+"#", keep)
		if err != nil {
			return fmt.Errorf("replace usage rows: %w", err)
		}
		res.UsageRemoved += n
	}
	return nil
}

// usageInArchiveDB reports whether the request log's table is in the archive database (same
// server, same database), so usage can be written in the ingest transaction.
func (a *Archive) usageInArchiveDB(ctx context.Context, store *requestlog.Store) bool {
	if store == nil {
		return false
	}
	const fp = `SELECT current_database() || '|' || pg_postmaster_start_time()::text || '|' ||
		COALESCE(inet_server_port(), 0)::text || '|' || (to_regclass($1) IS NOT NULL)::text`
	var mine, theirs string
	if a.db.QueryRowContext(ctx, fp, store.Schema()+".requests").Scan(&mine) != nil ||
		store.DB().QueryRowContext(ctx, fp, store.Schema()+".requests").Scan(&theirs) != nil {
		return false
	}
	return mine == theirs && strings.HasSuffix(mine, "|true")
}
