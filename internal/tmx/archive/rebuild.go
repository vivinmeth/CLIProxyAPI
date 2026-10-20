package archive

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

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
		built := usageRows(it.in, it.key, it.via)
		if err = store.UpsertImported(ctx, built); err != nil {
			return res, fmt.Errorf("session %s: %w", id, err)
		}
		res.Written += len(built)
	}
	return res, nil
}
