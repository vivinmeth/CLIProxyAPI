package auth

import (
	"net/http"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	log "github.com/sirupsen/logrus"
)

const tmxRefusalMetadataKey = "tmx_account_refusal"

func tmxAuthDisabled(a *Auth) bool {
	return a != nil && (a.Disabled || a.Status == StatusDisabled)
}

// Classify against the registered provider, not an untrusted result provider.
// Run before the upstream stale-result guard so account identity rules apply.
func tmxClassifyAccountRefusal(result Result, a *Auth) Result {
	if result.Success || result.Error == nil || a == nil {
		return result
	}
	if code := refusals.Detect(a.Provider, statusCodeFromResult(result.Error), result.Error.Code, result.Error.Message); code != "" {
		result.Error = cloneError(result.Error)
		result.Error.Code = code
		result.CredentialScope = refusals.TerminalCode(code)
	}
	return result
}

func tmxTerminalRefusal(err *Error) bool {
	return err != nil && refusals.TerminalCode(err.Code)
}

func tmxHasAccountRefusal(a *Auth) bool {
	return tmxAuthDisabled(a) && tmxTerminalRefusal(a.LastError)
}

func tmxRefusalStatusMessage(err *Error) string {
	return refusals.StatusMessage(err.Code)
}

// The upstream path owns disabling/persistence. Finalize only the new transition:
// remove refresh scheduling and emit one safe operator log and history event.
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
	appeal := refusals.RedactTOSAppealURL(result.Error.Metadata["tos_appeal_url"])
	if appeal != "" {
		log.Warnf("account refusal: credential %s disabled: %s; appeal=%s", tmxAuthIndex(a), a.StatusMessage, appeal)
	} else {
		log.Warnf("account refusal: credential %s disabled: %s", tmxAuthIndex(a), a.StatusMessage)
	}
	return true
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
	code, _ := a.Metadata[tmxRefusalMetadataKey].(string)
	if code == "" && tmxTerminalRefusal(a.LastError) {
		code = a.LastError.Code // Also recover refusals made before the metadata marker existed.
	}
	if !refusals.TerminalCode(code) {
		return
	}
	if !tmxAuthDisabled(a) {
		delete(a.Metadata, tmxRefusalMetadataKey)
		delete(a.Metadata, "tos_appeal_url")
		delete(a.Metadata, "tos_appeal_url_text")
		if tmxTerminalRefusal(a.LastError) {
			a.LastError = nil
			a.StatusMessage = ""
		}
		now := time.Now()
		for _, state := range a.ModelStates {
			if state != nil && tmxTerminalRefusal(state.LastError) {
				resetModelState(state, now)
			}
		}
		updateAggregatedAvailability(a, now)
		return
	}
	a.Disabled = true
	a.Status = StatusDisabled
	a.Unavailable = true
	a.StatusMessage = refusals.StatusMessage(code)
	a.NextRetryAfter = time.Time{}
	a.NextRefreshAfter = time.Time{}
	a.LastError = &Error{Code: code, Message: a.StatusMessage, HTTPStatus: http.StatusForbidden}
}
