// Package requestlog persists one row per upstream model call to Postgres (tokenmaxx fork).
//
// Rows come from the usage plugin hook, are buffered in memory and written in batches by a
// single background goroutine, so request handling never waits on the database. Nothing here
// stores prompts or responses; error bodies are truncated and keys are never recorded (only a
// short hash of the client API key).
package requestlog

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/clienterror"
	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	coresession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

// Outcome values stored in the outcome column.
const (
	OutcomeSuccess  = "success"
	OutcomeFailed   = "failed"
	OutcomeCanceled = "canceled"
)

const maxErrorLen = 2000

// Row is one upstream execution attempt. Retries of the same inbound request share TraceID.
type Row struct {
	TS              time.Time `json:"ts"`
	RequestID       string    `json:"request_id"`
	TraceID         string    `json:"trace_id"`
	SessionID       string    `json:"session_id"`
	Provider        string    `json:"provider"`
	Executor        string    `json:"executor"`
	Model           string    `json:"model"`
	Alias           string    `json:"alias"`
	ResponseModel   string    `json:"response_model"`
	AuthID          string    `json:"auth_id"`
	AuthIndex       string    `json:"auth_index"`
	AuthLabel       string    `json:"auth_label"`
	AuthType        string    `json:"auth_type"`
	APIKeyHash      string    `json:"api_key_hash"`
	AccessProvider  string    `json:"access_provider"`
	ClientIP        string    `json:"client_ip"`
	UserAgent       string    `json:"user_agent"`
	Stream          bool      `json:"stream"`
	Outcome         string    `json:"outcome"`
	StatusCode      int       `json:"status_code"`
	Error           string    `json:"error"`
	LatencyMs       int64     `json:"latency_ms"`
	TTFTMs          int64     `json:"ttft_ms"`
	InputTokens     int64     `json:"input_tokens"`
	OutputTokens    int64     `json:"output_tokens"`
	ReasoningTokens int64     `json:"reasoning_tokens"`
	CachedTokens    int64     `json:"cached_tokens"`
	CacheRead       int64     `json:"cache_read"`
	CacheCreation   int64     `json:"cache_creation"`
	TotalTokens     int64     `json:"total_tokens"`
	ReasoningEffort string    `json:"reasoning_effort"`
	ServiceTier     string    `json:"service_tier"`
	// Normalised accounting (identical meaning for every provider and source):
	// input total = uncached_input + cache_read + cache_write;
	// output total = output_non_reasoning + reasoning_tokens (reasoning is part of output).
	UncachedInput      int64  `json:"uncached_input"`
	CacheWrite         int64  `json:"cache_write"`
	OutputNonReasoning int64  `json:"output_non_reasoning"`
	AccountingQuality  string `json:"accounting_quality"`
	// Provenance: "tmx-proxy" for proxied calls, "extern:<harness>" for imported sessions.
	Source  string `json:"source"`
	Machine string `json:"machine"`
	// Harness / ClientMode: see HarnessFor. Filled at write time when empty.
	Harness    string `json:"harness"`
	ClientMode string `json:"client_mode"`
	ImportRef  string `json:"import_ref,omitempty"`
	// Shadowed rows duplicate traffic the proxy already recorded (an imported session that also
	// went through the proxy); they are excluded from totals unless asked for.
	Shadowed bool `json:"shadowed"`

	importRefPtr *string // scan/insert target for the nullable import_ref column
}

// syncImportRef maps ImportRef to the nullable column (empty = NULL, so the unique index only
// constrains imported rows).
func (r *Row) syncImportRef() {
	if r.Harness == "" {
		r.Harness, r.ClientMode = HarnessFor(r.Source, r.UserAgent)
	}
	if r.ImportRef == "" {
		r.importRefPtr = nil
		return
	}
	ref := r.ImportRef
	r.importRefPtr = &ref
}

func (r *Row) loadImportRef() {
	if r.importRefPtr != nil {
		r.ImportRef = *r.importRefPtr
	}
}

// SourceProxy marks rows recorded by the proxy itself.
const SourceProxy = "tmx-proxy"

// applyBreakdown fills the normalised columns from upstream's v2 token accounting.
func (r *Row) applyBreakdown(b coreusage.TokenBreakdown) {
	r.UncachedInput = b.Input.UncachedTokens
	r.CacheRead = b.Input.CacheReadTokens
	r.CacheWrite = b.Input.CacheWriteTokens
	r.OutputNonReasoning = b.Output.NonReasoningTokens
	r.ReasoningTokens = b.Output.ReasoningTokens
	r.AccountingQuality = string(b.Quality)
	if b.TotalTokens > 0 {
		r.TotalTokens = b.TotalTokens
	}
}

// classify maps a usage record to success / failed / canceled. A client that goes away (Esc in
// Claude Code, closed socket) surfaces as 499 or a context.Canceled error; those are not
// upstream failures and are kept apart so they don't read as incidents.
func classify(ctx context.Context, record coreusage.Record) (string, int) {
	status := record.Fail.StatusCode
	if !record.Failed {
		return OutcomeSuccess, status
	}
	if status == clienterror.StatusClientClosedRequest {
		return OutcomeCanceled, status
	}
	body := strings.ToLower(record.Fail.Body)
	if strings.Contains(body, "context canceled") || strings.Contains(body, "client disconnected") {
		return OutcomeCanceled, status
	}
	if ctx != nil && errors.Is(ctx.Err(), context.Canceled) {
		return OutcomeCanceled, status
	}
	return OutcomeFailed, status
}

// clientAddress prefers the first X-Forwarded-For hop: behind Traefik the socket peer is the
// swarm overlay address, which says nothing about which machine sent the request.
func clientAddress(forwardedFor, resolved, peer string) string {
	if first, _, _ := strings.Cut(forwardedFor, ","); strings.TrimSpace(first) != "" {
		return strings.TrimSpace(first)
	}
	if resolved = strings.TrimSpace(resolved); resolved != "" {
		return resolved
	}
	return strings.TrimSpace(peer)
}

// machineEnvTokens are leading segments that name an environment, not a machine
// ("sk-tmx-stg-macbook-…" is the macbook's staging key).
var machineEnvTokens = map[string]bool{"stg": true, "staging": true, "prod": true, "dev": true}

// MachineFromKey reads the machine name from a client key named "sk-tmx-[<env>-]<machine>-<secret>".
// Random keys ("sk-tmx-<hex>") and anything else return "". The name must contain a letter so a
// hex secret is never mistaken for one.
func MachineFromKey(key string) string {
	rest, ok := strings.CutPrefix(strings.TrimSpace(key), "sk-tmx-")
	if !ok {
		return ""
	}
	parts := strings.Split(rest, "-")
	for len(parts) > 2 && machineEnvTokens[strings.ToLower(parts[0])] {
		parts = parts[1:]
	}
	if len(parts) < 2 {
		return ""
	}
	name := strings.ToLower(parts[0])
	hasLetter := false
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z':
			hasLetter = true
		case r >= '0' && r <= '9', r == '_', r == '.':
		default:
			return ""
		}
	}
	if !hasLetter || len(name) > 40 || isHex(name) || machineEnvTokens[name] {
		return ""
	}
	return name
}

func isHex(s string) bool {
	for _, r := range s {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return len(s) >= 8
}

func hashAPIKey(key string) string {
	key = strings.TrimSpace(key)
	if key == "" {
		return ""
	}
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:])[:16]
}

func truncate(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	cut := s[:n]
	for !utf8.ValidString(cut) && len(cut) > 0 {
		cut = cut[:len(cut)-1]
	}
	return cut + "…"
}

// rowFromRecord builds a Row from a usage record and the request context.
func rowFromRecord(ctx context.Context, record coreusage.Record) Row {
	ts := record.RequestedAt
	if ts.IsZero() {
		ts = time.Now()
	}
	meta := internallogging.GetClientRequestMetadata(ctx)
	clientIP := clientAddress(meta.XForwardedFor, meta.ResolvedClientIP, meta.ClientIP)
	traceID := strings.TrimSpace(record.TraceID)
	if traceID == "" {
		traceID = strings.TrimSpace(internallogging.GetRequestID(ctx))
	}
	sessionID := strings.TrimSpace(record.SessionID)
	if sessionID == "" {
		sessionID = strings.TrimSpace(meta.SessionID)
	}
	sessionID = coresession.NormalizeToCanonicalUUID(sessionID)
	accessProvider := strings.TrimSpace(record.AccessProvider)
	if accessProvider == "" {
		accessProvider = strings.TrimSpace(coreusage.AccessProviderFromContext(ctx))
	}
	effort := strings.TrimSpace(record.ReasoningEffort)
	if effort == "" {
		effort = coreusage.ReasoningEffortFromContext(ctx)
	}
	tier := strings.TrimSpace(record.ServiceTier)
	if tier == "" {
		tier = strings.TrimSpace(record.RequestServiceTier)
	}
	stream := record.Stream
	if !stream {
		stream = coreusage.StreamFromContext(ctx)
	}
	detail := coreusage.EnsureTokenBreakdownForProvider(record.Detail, record.Provider, record.ExecutorType)
	outcome, status := classify(ctx, record)
	errText := ""
	if outcome != OutcomeSuccess {
		errText = truncate(record.Fail.Body, maxErrorLen)
	}

	row := Row{
		Source:          SourceProxy,
		TS:              ts.UTC(),
		RequestID:       strings.TrimSpace(record.RequestID),
		TraceID:         traceID,
		SessionID:       sessionID,
		Provider:        strings.TrimSpace(record.Provider),
		Executor:        strings.TrimSpace(record.ExecutorType),
		Model:           strings.TrimSpace(record.Model),
		Alias:           strings.TrimSpace(record.Alias),
		ResponseModel:   strings.TrimSpace(record.ResponseModel),
		AuthID:          strings.TrimSpace(record.AuthID),
		AuthIndex:       strings.TrimSpace(record.AuthIndex),
		AuthLabel:       strings.TrimSpace(record.Source),
		AuthType:        strings.TrimSpace(record.AuthType),
		APIKeyHash:      hashAPIKey(record.APIKey),
		Machine:         MachineFromKey(record.APIKey),
		AccessProvider:  accessProvider,
		ClientIP:        clientIP,
		UserAgent:       truncate(meta.UserAgent, 300),
		Stream:          stream,
		Outcome:         outcome,
		StatusCode:      status,
		Error:           errText,
		LatencyMs:       record.Latency.Milliseconds(),
		TTFTMs:          record.TTFT.Milliseconds(),
		InputTokens:     detail.InputTokens,
		OutputTokens:    detail.OutputTokens,
		ReasoningTokens: detail.ReasoningTokens,
		CachedTokens:    detail.CachedTokens,
		CacheRead:       detail.CacheReadTokens,
		CacheCreation:   detail.CacheCreationTokens,
		TotalTokens:     detail.TotalTokens,
		ReasoningEffort: effort,
		ServiceTier:     tier,
	}
	// Raw provider-shaped columns above are kept as reported; the normalised ones come from
	// upstream's accounting (cache_read and reasoning_tokens take its values).
	row.applyBreakdown(detail.TokenBreakdown)
	return row
}
