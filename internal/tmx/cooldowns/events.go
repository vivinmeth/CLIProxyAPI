// Package cooldowns keeps a durable, never-pruned history of credential cooldowns and of the
// failures behind them (429s, usage limits, auth errors) in Postgres tmx.cooldown_events.
//
// The auth manager reports each auth's live cooldowns after every state change it makes
// (sdk/cliproxy/auth/tmx_cooldown_events.go); a tracker turns those reports into
// cooldown_start / cooldown_update / cooldown_end events that share a cooldown_id, so every
// window can be charted. A periodic reconcile against the manager's full snapshot catches state
// changes made elsewhere (restore, config, refresh) and windows that expire on their own.
package cooldowns

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Event kinds.
const (
	KindStart       = "cooldown_start"
	KindUpdate      = "cooldown_update" // retry_at / reason / status of an open window changed
	KindEnd         = "cooldown_end"
	KindRateLimited = "rate_limited_429" // a 429 that isn't a usage limit
	KindUsageLimit  = "usage_limit"      // plan / usage quota exhausted (429 usage_limit_reached, credential-wide 429, 402)
	KindAuthError   = "auth_error"       // 401 / 403 / invalid_grant
	KindOverloaded  = "overloaded"       // 529
	KindStoreSet    = "store_set"        // trigger on cooldown_store: a cooldown row written
	KindStoreClear  = "store_cleared"    // trigger on cooldown_store: a cooldown row cleared
)

// Event sources.
const (
	SourceProxy     = "proxy"     // reported by the auth manager as it happened
	SourceReconcile = "reconcile" // found by comparing against the manager's snapshot
	SourceTrigger   = "trigger"   // written by the database trigger on cooldown_store
)

// End reasons.
const (
	EndExpired    = "expired"    // retry_at passed
	EndCleared    = "cleared"    // cleared before retry_at (success, reset, reset credit, disable)
	EndRestart    = "restart"    // the window was open before a restart and is gone after it
	EndSuperseded = "superseded" // a duplicate open window for the same auth/model
)

const maxMessageLen = 500

// Event is one row of tmx.cooldown_events. ID and RecordedAt are set by the database.
type Event struct {
	ID           int64      `json:"id,omitempty"`
	EventKey     string     `json:"event_key"`
	TS           time.Time  `json:"ts"`
	RecordedAt   *time.Time `json:"recorded_at,omitempty"`
	Kind         string     `json:"kind"`
	Source       string     `json:"source"`
	CooldownID   string     `json:"cooldown_id,omitempty"`
	AuthID       string     `json:"auth_id"`
	AuthIndex    string     `json:"auth_index,omitempty"`
	AuthLabel    string     `json:"auth_label,omitempty"`
	Provider     string     `json:"provider,omitempty"`
	Model        string     `json:"model"`
	Reason       string     `json:"reason,omitempty"`
	HTTPStatus   int        `json:"http_status,omitempty"`
	ErrorCode    string     `json:"error_code,omitempty"`
	Message      string     `json:"message,omitempty"`
	RetryAt      *time.Time `json:"retry_at,omitempty"`
	RetryAfterMs int64      `json:"retry_after_ms,omitempty"`
	BackoffLevel int        `json:"backoff_level,omitempty"`
	CredScope    bool       `json:"credential_scope,omitempty"`
	StartedAt    *time.Time `json:"started_at,omitempty"`
	EndedAt      *time.Time `json:"ended_at,omitempty"`
	EndReason    string     `json:"end_reason,omitempty"`
}

func newKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	u := t.UTC()
	return &u
}

func truncate(s string) string {
	s = strings.TrimSpace(s)
	if len(s) <= maxMessageLen {
		return s
	}
	s = s[:maxMessageLen]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s
}

// FailureKind classifies a failed result; "" means it isn't recorded.
func FailureKind(f auth.FailureReport) string {
	text := strings.ToLower(f.ErrorCode + " " + f.Message)
	switch {
	case strings.Contains(text, "invalid_grant"):
		return KindAuthError
	case f.HTTPStatus == 401 || f.HTTPStatus == 403:
		return KindAuthError
	case f.HTTPStatus == 402:
		return KindUsageLimit
	case f.HTTPStatus == 429:
		if f.CredentialScope || isUsageLimitText(text) {
			return KindUsageLimit
		}
		return KindRateLimited
	case f.HTTPStatus == 529:
		return KindOverloaded
	}
	return ""
}

func isUsageLimitText(text string) bool {
	for _, marker := range []string{"usage_limit", "usage limit", "quota", "insufficient_quota", "limit reached",
		"out of credits"} {
		if strings.Contains(text, marker) {
			return true
		}
	}
	return false
}

// failureEvent converts a failure report into a point event, or false when it isn't recorded.
func failureEvent(f auth.FailureReport) (Event, bool) {
	kind := FailureKind(f)
	if kind == "" {
		return Event{}, false
	}
	ev := Event{
		EventKey:   newKey(),
		TS:         f.At.UTC(),
		Kind:       kind,
		Source:     SourceProxy,
		AuthID:     f.AuthID,
		AuthIndex:  f.AuthIndex,
		AuthLabel:  f.AuthLabel,
		Provider:   f.Provider,
		Model:      f.Model,
		HTTPStatus: f.HTTPStatus,
		ErrorCode:  truncate(f.ErrorCode),
		Message:    truncate(f.Message),
		CredScope:  f.CredentialScope,
	}
	if f.RetryAfter != nil && *f.RetryAfter > 0 {
		ev.RetryAfterMs = f.RetryAfter.Milliseconds()
		ev.RetryAt = timePtr(f.At.Add(*f.RetryAfter))
	}
	return ev, true
}
