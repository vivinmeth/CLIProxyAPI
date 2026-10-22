package auth

// tokenmaxx fork: durable cooldown history. internal/tmx/cooldowns installs a CooldownObserver;
// the conductor reports an auth's live cooldowns after each state change it makes (MarkResult,
// ResetQuota) and every failed result. The observer turns the reports into start / update / end
// events and writes them to Postgres. Reports are sent while m.mu is held, so observers must
// only enqueue (never block or call back into the Manager).

import (
	"strings"
	"sync/atomic"
	"time"
)

// CooldownWindowState is one live cooldown: credential-wide when Model is empty.
type CooldownWindowState struct {
	AuthID       string
	AuthIndex    string
	AuthLabel    string
	Provider     string
	Model        string
	Reason       string
	HTTPStatus   int
	ErrorCode    string
	Message      string
	RetryAt      time.Time
	BackoffLevel int
}

// CooldownReport is the complete set of live cooldowns of one auth (AuthID set) or of every auth
// (AuthID empty, a snapshot). Seq orders reports: a report never describes state older than one
// with a lower Seq.
type CooldownReport struct {
	Seq     uint64
	At      time.Time
	AuthID  string
	Windows []CooldownWindowState
}

// FailureReport is one failed upstream result as the auth manager saw it.
type FailureReport struct {
	At              time.Time
	AuthID          string
	AuthIndex       string
	AuthLabel       string
	Provider        string
	Model           string
	HTTPStatus      int
	ErrorCode       string
	Message         string
	RetryAfter      *time.Duration
	CredentialScope bool
}

// CooldownObserver receives cooldown and failure reports. Both methods must not block.
type CooldownObserver interface {
	ObserveCooldowns(CooldownReport)
	ObserveFailure(FailureReport)
}

type cooldownObserverBox struct{ o CooldownObserver }

var (
	tmxCooldownObserver atomic.Pointer[cooldownObserverBox]
	tmxCooldownSeq      atomic.Uint64
)

// SetCooldownObserver installs (or, with nil, removes) the process-wide cooldown observer.
func SetCooldownObserver(o CooldownObserver) {
	if o == nil {
		tmxCooldownObserver.Store(nil)
		return
	}
	tmxCooldownObserver.Store(&cooldownObserverBox{o: o})
}

func currentCooldownObserver() CooldownObserver {
	if box := tmxCooldownObserver.Load(); box != nil {
		return box.o
	}
	return nil
}

// tmxReportCooldownsLocked reports auth's live cooldowns. Callers hold m.mu for writing, which
// orders the report's Seq against other reports and snapshots.
func (m *Manager) tmxReportCooldownsLocked(auth *Auth, now time.Time) {
	o := currentCooldownObserver()
	if o == nil || auth == nil || auth.ID == "" {
		return
	}
	o.ObserveCooldowns(CooldownReport{
		Seq:     tmxCooldownSeq.Add(1),
		At:      now,
		AuthID:  auth.ID,
		Windows: m.tmxCooldownWindowsLocked(auth, now),
	})
}

// CooldownSnapshot returns every auth's live cooldowns; the observer reconciles against it.
func (m *Manager) CooldownSnapshot() CooldownReport {
	if m == nil {
		return CooldownReport{At: time.Now()}
	}
	m.mu.RLock()
	defer m.mu.RUnlock()
	now := time.Now()
	rep := CooldownReport{Seq: tmxCooldownSeq.Load(), At: now}
	for _, auth := range m.auths {
		rep.Windows = append(rep.Windows, m.tmxCooldownWindowsLocked(auth, now)...)
	}
	return rep
}

// tmxCooldownWindowsLocked mirrors what the cooldown store persists for auth.
func (m *Manager) tmxCooldownWindowsLocked(auth *Auth, now time.Time) []CooldownWindowState {
	records := m.cooldownStateRecordsForAuthLocked(auth, now)
	if len(records) == 0 {
		return nil
	}
	index := tmxAuthIndex(auth)
	out := make([]CooldownWindowState, 0, len(records))
	for _, r := range records {
		w := CooldownWindowState{
			AuthID:       r.AuthID,
			AuthIndex:    index,
			AuthLabel:    strings.TrimSpace(auth.Label),
			Provider:     r.Provider,
			Model:        r.Model,
			Reason:       r.Reason,
			RetryAt:      r.NextRetryAfter,
			BackoffLevel: r.Quota.BackoffLevel,
		}
		if r.LastError != nil {
			w.HTTPStatus = r.LastError.HTTPStatus
			w.ErrorCode = r.LastError.Code
			w.Message = r.LastError.Message
		}
		out = append(out, w)
	}
	return out
}

// tmxReportFailure reports a failed result; auth may be nil (stale results).
func tmxReportFailure(result Result, auth *Auth) {
	o := currentCooldownObserver()
	if o == nil || result.Success || result.AuthID == "" {
		return
	}
	f := FailureReport{
		At:              time.Now(),
		AuthID:          result.AuthID,
		Provider:        strings.TrimSpace(result.Provider),
		Model:           strings.TrimSpace(result.Model),
		HTTPStatus:      statusCodeFromResult(result.Error),
		RetryAfter:      result.RetryAfter,
		CredentialScope: result.CredentialScope,
	}
	if f.Model == "" {
		f.Model = strings.TrimSpace(result.RouteModel)
	}
	if result.Error != nil {
		f.ErrorCode = result.Error.Code
		f.Message = result.Error.Message
	}
	if auth != nil {
		f.AuthIndex = tmxAuthIndex(auth)
		f.AuthLabel = strings.TrimSpace(auth.Label)
		if f.Provider == "" {
			f.Provider = strings.TrimSpace(auth.Provider)
		}
	}
	o.ObserveFailure(f)
}

// tmxAuthIndex is EnsureIndex without mutating auth (safe under a read lock).
func tmxAuthIndex(auth *Auth) string {
	if idx := strings.TrimSpace(auth.Index); idx != "" {
		return idx
	}
	if seed := auth.indexSeed(); seed != "" {
		return stableAuthIndex(seed)
	}
	return ""
}
