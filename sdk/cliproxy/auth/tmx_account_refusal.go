package auth

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	log "github.com/sirupsen/logrus"
)

const tmxRefusalMetadataKey = "tmx_account_refusal"

// ErrTmxRefusalStale means the response belongs to a superseded registration or grant.
var ErrTmxRefusalStale = errors.New("account refusal belongs to a superseded credential")

var errTmxRefusalUnavailable = errors.New("account refusal could not be persisted")
var errTmxRefusalInvalid = errors.New("invalid account refusal")

// TmxRefuseAccount applies a proven quota refusal without recording an inference.
// The sent snapshot must describe the credentials that produced the response.
func (m *Manager) TmxRefuseAccount(ctx context.Context, sent *Auth, code string) error {
	if m == nil || sent == nil || sent.ID == "" || !refusals.TerminalCode(code) {
		return errTmxRefusalInvalid
	}
	if ctx == nil {
		ctx = context.Background()
	}
	release, err := m.lockAuthMutationContext(ctx, sent.ID)
	if err != nil {
		return errTmxRefusalUnavailable
	}
	defer release()
	result := Result{AuthID: sent.ID, Provider: sent.Provider, RegistrationEpoch: sent.RegistrationEpoch,
		CredentialVersion: sent.CredentialVersion, AccessToken: authAccessToken(sent), RefreshToken: authRefreshToken(sent),
		CredentialScope: true, Error: &Error{Code: code, Message: refusals.StatusMessage(code), HTTPStatus: http.StatusForbidden}}
	m.mu.Lock()
	current := m.auths[sent.ID]
	// Google grant replacement wins even when the new grant retains the old
	// access token, which the shared stale helper otherwise accepts immediately.
	grantChanged := current != nil && refusals.GoogleProvider(current.Provider) && current.CredentialVersion != sent.CredentialVersion && authRefreshToken(current) != result.RefreshToken
	if current == nil || current.RegistrationEpoch != sent.RegistrationEpoch || grantChanged || isStaleExecutionResult(result, current) {
		m.mu.Unlock()
		return ErrTmxRefusalStale
	}
	if !refusals.GoogleProvider(current.Provider) || !refusals.GoogleProvider(sent.Provider) {
		m.mu.Unlock()
		return errTmxRefusalInvalid
	}
	if tmxAuthDisabled(current) {
		m.mu.Unlock()
		return nil
	}
	current.LastError = cloneError(result.Error)
	tmxRestoreAccountRefusal(current)
	tmxFinalizeAccountRefusal(current, false, result)
	current.Generation++
	current.UpdatedAt = time.Now()
	persisted := m.tmxPersistAccountRefusalLocked(ctx, current)
	tmxLogAccountRefusal(current, result, persisted)
	snapshot := current.Clone()
	m.mu.Unlock()
	release()
	if m.scheduler != nil {
		m.scheduler.upsertAuthResult(snapshot, nil, true)
	}
	m.queueRefreshUnschedule(sent.ID)
	if !persisted {
		result.Error.Message += refusals.MemoryOnlySuffix
	}
	tmxReportFailure(result, snapshot, true)
	if !persisted {
		return errTmxRefusalUnavailable
	}
	return nil
}

type accountRefusalClearContextKey struct{}

// WithAccountRefusalClear authorizes one operator-requested re-enable. The
// incoming auth must also be enabled; the intent is never persisted as metadata.
func WithAccountRefusalClear(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, accountRefusalClearContextKey{}, true)
}

func hasAccountRefusalClear(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	clear, _ := ctx.Value(accountRefusalClearContextKey{}).(bool)
	return clear
}

func tmxAuthDisabled(a *Auth) bool {
	return a != nil && (a.Disabled || a.Status == StatusDisabled)
}

// Classify against the registered provider, not an untrusted result provider.
// Run before the upstream stale-result guard so account identity rules apply.
func tmxClassifyAccountRefusal(result Result, a *Auth) Result {
	if result.Success || result.Error == nil || a == nil {
		return result
	}
	code := refusals.Detect(a.Provider, statusCodeFromResult(result.Error), result.Error.Code, result.Error.Message)
	if refusals.GoogleProvider(a.Provider) && result.Error.HTTPStatus == http.StatusForbidden && result.Error.Code == refusals.GoogleValidation {
		code = refusals.GoogleValidation // Already classified by the executor, without its response body.
	}
	if code != "" {
		if code == refusals.GoogleValidation {
			// Never clone the raw validation response or any session-bearing metadata.
			result.Error = &Error{Code: code, Message: refusals.StatusMessage(code), HTTPStatus: http.StatusForbidden}
			result.RetryAfter = nil
		} else {
			result.Error = cloneError(result.Error) // Preserve the legacy ToS appeal contract.
		}
		result.Error.Code = code
		result.CredentialScope = refusals.TerminalCode(code)
	}
	return result
}

func tmxTerminalRefusal(err *Error) bool {
	return err != nil && refusals.TerminalCode(err.Code)
}

func tmxHasAccountRefusal(a *Auth) bool {
	return tmxAccountRefusalCode(a) != ""
}

func tmxAccountRefusalCode(a *Auth) string {
	if a == nil {
		return ""
	}
	code, _ := a.Metadata[tmxRefusalMetadataKey].(string)
	if refusals.TerminalCode(code) {
		return code
	}
	if tmxTerminalRefusal(a.LastError) {
		return a.LastError.Code
	}
	return ""
}

// The registered record owns its refusal. Only the explicit operator intent
// may clear it; missing fields and an enabled incoming copy are not consent.
// The return value requests repair of storage that omitted the refusal marker.
func tmxReconcileAccountRefusal(ctx context.Context, existing, incoming *Auth) bool {
	code := tmxAccountRefusalCode(existing)
	if code == "" {
		tmxRestoreAccountRefusal(incoming)
		return false
	}
	if hasAccountRefusalClear(ctx) && !tmxAuthDisabled(incoming) {
		tmxClearAccountRefusal(incoming)
		return false
	}
	missingMarker := tmxAccountRefusalCodeFromMetadata(incoming) != code
	if incoming.Metadata == nil {
		incoming.Metadata = make(map[string]any)
	}
	incoming.Metadata[tmxRefusalMetadataKey] = code
	for _, key := range []string{"tos_appeal_url", "tos_appeal_url_text"} {
		if value, ok := existing.Metadata[key]; ok {
			incoming.Metadata[key] = value
		}
	}
	incoming.LastError = cloneError(existing.LastError)
	tmxRestoreAccountRefusal(incoming)
	return missingMarker
}

func tmxAccountRefusalCodeFromMetadata(a *Auth) string {
	if a == nil {
		return ""
	}
	code, _ := a.Metadata[tmxRefusalMetadataKey].(string)
	return code
}

func tmxClearAccountRefusal(a *Auth) {
	delete(a.Metadata, tmxRefusalMetadataKey)
	delete(a.Metadata, "tos_appeal_url")
	delete(a.Metadata, "tos_appeal_url_text")
	if a.Metadata != nil {
		a.Metadata["disabled"] = false
	}
	a.LastError = nil
	a.StatusMessage = ""
	a.NextRetryAfter = time.Time{}
	a.NextRefreshAfter = time.Time{}
	now := time.Now()
	for _, state := range a.ModelStates {
		if state != nil && tmxTerminalRefusal(state.LastError) {
			resetModelState(state, now)
		}
	}
	updateAggregatedAvailability(a, now)
}

func tmxRefusalStatusMessage(err *Error) string {
	return refusals.StatusMessage(err.Code)
}

// The upstream path owns disabling/persistence. Finalize only the new transition:
// remove refresh scheduling and prepare one safe operator log and history event.
func tmxFinalizeAccountRefusal(a *Auth, wasDisabled bool, result Result) bool {
	if wasDisabled || !tmxAuthDisabled(a) || !tmxTerminalRefusal(result.Error) {
		return false
	}
	a.Unavailable = true
	a.NextRetryAfter = time.Time{}
	a.NextRefreshAfter = time.Time{}
	if a.Metadata == nil {
		a.Metadata = make(map[string]any)
	}
	a.Metadata[tmxRefusalMetadataKey] = result.Error.Code
	a.Metadata["disabled"] = true
	return true
}

func tmxLogAccountRefusal(a *Auth, result Result, persisted bool) {
	message := "account refusal: credential " + tmxAuthIndex(a) + " disabled: " + tmxRefusalStatusMessage(result.Error)
	if !persisted {
		message += refusals.MemoryOnlySuffix
	}
	appeal := refusals.RedactTOSAppealURL(result.Error.Metadata["tos_appeal_url"])
	if appeal != "" {
		message += "; appeal=" + appeal
	}
	if persisted {
		log.Warn(message)
	} else {
		log.Error(message)
	}
}

type tmxRefusalSaveContextKey struct{}
type tmxRefusalSaveOutcome struct{ written bool }

func tmxNoteRefusalSave(ctx context.Context, path string, err error) {
	if ctx != nil {
		if outcome, ok := ctx.Value(tmxRefusalSaveContextKey{}).(*tmxRefusalSaveOutcome); ok {
			outcome.written = err == nil && path != ""
		}
	}
}

func (m *Manager) tmxPersistAccountRefusalLocked(ctx context.Context, a *Auth) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	outcome := &tmxRefusalSaveOutcome{}
	err := m.persistLocked(context.WithValue(ctx, tmxRefusalSaveContextKey{}, outcome), a)
	return err == nil && outcome.written
}

func tmxAttestationCooling(a *Auth, model string, now time.Time) bool {
	if tmxAuthDisabled(a) {
		return false
	}
	if model != "" {
		s := existingModelState(a, model)
		return s != nil && s.LastError != nil && s.LastError.Code == refusals.AttestationRefused && s.NextRetryAfter.After(now)
	}
	return a.LastError != nil && a.LastError.Code == refusals.AttestationRefused && a.NextRetryAfter.After(now)
}

func tmxFinalizeAttestationRefusal(a *Auth, model string, wasCooling bool, result Result, now time.Time) {
	if result.Error == nil || result.Error.Code != refusals.AttestationRefused || !tmxAttestationCooling(a, model, now) {
		return
	}
	// Keep the safe reason in pool output and persisted cooldown windows.
	if a.LastError != nil && a.LastError.Code == refusals.AttestationRefused {
		a.StatusMessage = refusals.AttestationRefused
	}
	if s := existingModelState(a, model); s != nil && s.LastError != nil && s.LastError.Code == refusals.AttestationRefused {
		s.StatusMessage = refusals.AttestationRefused
	}
	if !wasCooling {
		log.Warnf("attestation refusal: credential %s cooling: %s", tmxAuthIndex(a), refusals.AttestationRefused)
	}
}

// Stores persist token metadata, not runtime status fields. Restore the reason
// on load/file updates, and remove it only after an explicit re-enable.
func tmxRestoreAccountRefusal(a *Auth) {
	if a == nil {
		return
	}
	code := tmxAccountRefusalCode(a)
	if !refusals.TerminalCode(code) {
		return
	}
	if a.Metadata == nil {
		a.Metadata = make(map[string]any)
	}
	a.Metadata[tmxRefusalMetadataKey] = code
	a.Metadata["disabled"] = true
	a.Disabled = true
	a.Status = StatusDisabled
	a.Unavailable = true
	a.StatusMessage = refusals.StatusMessage(code)
	a.NextRetryAfter = time.Time{}
	a.NextRefreshAfter = time.Time{}
	if a.LastError == nil || a.LastError.Code != code || code == refusals.GoogleValidation {
		a.LastError = &Error{Code: code, Message: a.StatusMessage, HTTPStatus: http.StatusForbidden}
	}
}
