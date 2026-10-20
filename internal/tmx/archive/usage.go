package archive

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
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
	Output            int64  `json:"output"`
	Reasoning         int64  `json:"reasoning"`
	Outcome           string `json:"outcome"`
	StatusCode        int    `json:"status_code"`
	LatencyMs         int64  `json:"latency_ms"`
	TTFTMs            int64  `json:"ttft_ms"`
	ServiceTier       string `json:"service_tier"`
	AccountingQuality string `json:"accounting_quality"`
}

// SourceFor is the tmx.requests source label of an imported harness.
func SourceFor(harness string) string { return "extern:" + harness }

// usageRows turns messages with usage into request-log rows. Imported rows are shadowed when the
// session also went through the proxy, so the same traffic is never counted twice by default.
func usageRows(in SessionIngest, sessionKey string, viaProxy *bool) []requestlog.Row {
	shadowed := viaProxy != nil && *viaProxy
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
			UncachedInput:      u.UncachedInput,
			CacheWrite:         u.CacheWrite,
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

func (a *Archive) writeUsage(ctx context.Context, in SessionIngest, sessionKey string, viaProxy *bool) (int, error) {
	store := requestlog.Default()
	if store == nil {
		return 0, nil
	}
	rows := usageRows(in, sessionKey, viaProxy)
	return len(rows), store.UpsertImported(ctx, rows)
}
