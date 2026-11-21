package refusals

import (
	"strings"
	"testing"
)

func TestGoogleExactRefusal(t *testing.T) {
	const body = `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Verify at https://example.invalid/session-private","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"VALIDATION_REQUIRED","metadata":{"verification_url":"https://example.invalid/session-private"}}]}}`
	for _, provider := range []string{"antigravity"} {
		for _, tc := range []struct {
			name, body, want string
			status           int
		}{
			{"validation", body, "account_refusal_google_validation", 403},
			{"tos", strings.ReplaceAll(body, "VALIDATION_REQUIRED", "TOS_VIOLATION"), "tos_violation", 403},
			{"http401", body, "", 401},
			{"http429", body, "", 429},
			{"http500", body, "", 500},
			{"wrong code", strings.ReplaceAll(body, `"code":403`, `"code":401`), "", 403},
			{"string code", strings.ReplaceAll(body, `"code":403`, `"code":"403"`), "", 403},
			{"fractional code", strings.ReplaceAll(body, `"code":403`, `"code":403.5`), "", 403},
			{"wrong status", strings.ReplaceAll(body, "PERMISSION_DENIED", "RESOURCE_EXHAUSTED"), "", 403},
			{"wrong detail", strings.ReplaceAll(body, "google.rpc.ErrorInfo", "google.rpc.Help"), "", 403},
			{"wrong reason", strings.ReplaceAll(body, "VALIDATION_REQUIRED", "SOME_OTHER_REASON"), "", 403},
			{"plain permission", `{"error":{"code":403,"status":"PERMISSION_DENIED"}}`, "", 403},
			{"malformed", body[:len(body)-1], "", 403},
		} {
			t.Run(provider+"/"+tc.name, func(t *testing.T) {
				if got := Detect(provider, tc.status, "", tc.body); got != tc.want {
					t.Fatalf("Detect = %q, want %q", got, tc.want)
				}
			})
		}
	}
	for _, provider := range []string{"meta", "codex", "openai", "claude", "vertex", "gemini", "gemini-cli"} {
		if got := Detect(provider, 403, "", body); got != "" {
			t.Fatalf("wrong provider %s matched %q", provider, got)
		}
	}
}

func TestDetect(t *testing.T) {
	for _, tc := range []struct {
		name, provider, code, body, want string
		status                           int
	}{
		{name: "meta code body", provider: "meta", status: 403, body: `{"error":{"code":"client_attestation_denied"}}`, want: MetaAttestation},
		{name: "meta code", provider: "meta", status: 403, code: "client_attestation_denied", want: MetaAttestation},
		{name: "meta tools context", provider: "meta", status: 403, code: AttestationRefused, body: `{"error":{"code":"client_attestation_denied"}}`, want: AttestationRefused},
		{name: "meta message", provider: "meta", status: 403, body: `{"error":{"message":"This API key is billed to a Muse Code subscription. Muse Code subscriptions cover requests from the Muse Code client only. Use it in Muse Code, or use a pay-as-you-go API key for other clients."}}`, want: MetaAttestation},
		{name: "meta text", provider: "meta", status: 403, body: "Muse Code subscriptions cover requests from the Muse Code client only", want: MetaAttestation},
		{name: "wrong provider", provider: "codex", status: 403, body: `{"error":{"code":"client_attestation_denied"}}`},
		{name: "wrong status", provider: "meta", status: 429, body: `{"error":{"code":"client_attestation_denied"}}`},
		{name: "ordinary 403", provider: "meta", status: 403, body: `{"error":{"code":"permission_denied"}}`},
		{name: "unproven ban", provider: "claude", status: 403, body: "account suspended"},
		{name: "generic Google permission", provider: "gemini", status: 403, body: `{"error":{"status":"PERMISSION_DENIED"}}`},
		{name: "unrelated JSON text", provider: "meta", status: 403, body: `{"request":{"code":"client_attestation_denied","message":"Muse Code subscriptions cover requests from the Muse Code client only"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := Detect(tc.provider, tc.status, tc.code, tc.body); got != tc.want {
				t.Fatalf("Detect = %q, want %q", got, tc.want)
			}
		})
	}
}
