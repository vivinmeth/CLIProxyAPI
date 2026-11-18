// Package refusals classifies only evidenced, terminal account refusals.
// The auth conductor applies its existing upstream TOS disable transition.
package refusals

import (
	"net/http"
	"strings"

	"github.com/MoeForge/moego/ojson"
)

const MetaAttestation = "account_refusal_meta_attestation"

const metaAttestationCode = "client_attestation_denied"

type rule struct {
	provider, code, text string
}

// Sources: umbrella wiki/muse-code.md, 2026-11-18 request a980ecb0;
// internal/runtime/executor/meta_executor_muse_tools_test.go (error.code).
// Google TOS parsing remains in the upstream Antigravity executor.
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
	code = strings.ToLower(strings.TrimSpace(code))
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

// TerminalCode includes the upstream executor's existing classification.
func TerminalCode(code string) bool {
	switch strings.ToLower(strings.TrimSpace(code)) {
	case "tos_violation", MetaAttestation:
		return true
	}
	return false
}

// StatusMessage uses bounded operator text, never an upstream response body.
func StatusMessage(code string) string {
	if strings.EqualFold(strings.TrimSpace(code), MetaAttestation) {
		return metaAttestationCode + ": Meta requires the Muse Code client; resolve before manually re-enabling"
	}
	return "tos_violation"
}
