// Package refusals classifies evidenced account and request attestation refusals.
// The auth conductor applies its existing upstream TOS disable transition.
package refusals

import (
	"net/http"
	"strings"

	"github.com/MoeForge/moego/ojson"
)

const MetaAttestation = "account_refusal_meta_attestation"

// GoogleValidation requires operator action before this account can infer again.
const GoogleValidation = "account_refusal_google_validation"

// MemoryOnlySuffix is the fixed persistence warning allowed in refusal events.
const MemoryOnlySuffix = "; memory-only: not persisted"

// AttestationRefused marks a tool-bearing request refusal, not an account ban.
const AttestationRefused = "attestation_refused"

const metaAttestationCode = "client_attestation_denied"

type rule struct {
	provider, code, text string
}

// Sources: umbrella wiki/muse-code.md, 2026-11-18 request a980ecb0;
// internal/runtime/executor/meta_executor_muse_tools_test.go (error.code).
// Google ToS appeal handling remains in the upstream Antigravity executor.
var rules = []rule{
	{provider: "meta", code: metaAttestationCode},
	{provider: "meta", text: "muse code subscriptions cover requests from the muse code client only"},
}

// Detect deliberately excludes generic 403s and guessed ban wording.
func Detect(provider string, status int, code, body string) string {
	if status != http.StatusForbidden {
		return ""
	}
	provider = strings.ToLower(strings.TrimSpace(provider))
	if GoogleProvider(provider) {
		return Google(status, body)
	}
	code = strings.ToLower(strings.TrimSpace(code))
	if provider == "meta" && code == AttestationRefused {
		return AttestationRefused
	}
	message := body
	if ojson.Valid(body) {
		if code != metaAttestationCode {
			code = strings.ToLower(strings.TrimSpace(ojson.Get(body, "error.code").String()))
		}
		message = ojson.Get(body, "error.message").String()
	}
	message = strings.ToLower(message)
	for _, r := range rules {
		if provider == r.provider && ((r.code != "" && code == r.code) || (r.text != "" && strings.Contains(message, r.text))) {
			return MetaAttestation
		}
	}
	return ""
}

// GoogleProvider identifies the Google subscription executors.
func GoogleProvider(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "antigravity":
		return true
	}
	return false
}

// Google returns only a fixed internal code. Generic permission errors, message
// text and ErrorInfo metadata never establish an account refusal.
func Google(status int, body string) string {
	if status != http.StatusForbidden || !ojson.Valid(body) {
		return ""
	}
	code := ojson.Get(body, "error.code")
	if code.Type != ojson.Number || code.Float() != http.StatusForbidden || ojson.Get(body, "error.status").String() != "PERMISSION_DENIED" {
		return ""
	}
	details := ojson.Get(body, "error.details")
	if !details.IsArray() {
		return ""
	}
	for _, detail := range details.Array() {
		if detail.Get("@type").String() != "type.googleapis.com/google.rpc.ErrorInfo" {
			continue
		}
		switch detail.Get("reason").String() {
		case "TOS_VIOLATION":
			return "tos_violation"
		case "VALIDATION_REQUIRED":
			return GoogleValidation
		}
	}
	return ""
}

// TerminalCode includes the upstream executor's existing classification.
func TerminalCode(code string) bool {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "tos_violation", MetaAttestation, GoogleValidation:
		return true
	}
	return false
}

// StatusMessage uses bounded operator text, never an upstream response body.
func StatusMessage(code string) string {
	if code == GoogleValidation {
		return "VALIDATION_REQUIRED: Google requires account verification; resolve before manually re-enabling"
	}
	if strings.EqualFold(strings.TrimSpace(code), MetaAttestation) {
		return metaAttestationCode + ": Meta requires the Muse Code client; resolve before manually re-enabling"
	}
	return "tos_violation"
}

// RedactTOSAppealURL removes signed/tracking query parameters and fragments.
func RedactTOSAppealURL(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if idx := strings.IndexAny(trimmed, "?#"); idx >= 0 {
		trimmed = trimmed[:idx]
	}
	return trimmed
}
