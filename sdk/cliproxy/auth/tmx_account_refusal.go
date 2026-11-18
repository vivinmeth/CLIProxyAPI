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
		result.CredentialScope = true
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
	log.Warnf("account refusal: credential %s disabled: %s", tmxAuthIndex(a), a.StatusMessage)
	return true
}

// Stores persist token metadata, not runtime status fields. Restore the reason
// on load/file updates, and remove it only after an explicit re-enable.
func tmxRestoreAccountRefusal(a *Auth) {
	if a == nil {
		return
	}
	code, _ := a.Metadata[tmxRefusalMetadataKey].(string)
	if !refusals.TerminalCode(code) {
		return
	}
	if !tmxAuthDisabled(a) {
		delete(a.Metadata, tmxRefusalMetadataKey)
		if tmxTerminalRefusal(a.LastError) {
			a.LastError = nil
			a.StatusMessage = ""
		}
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
