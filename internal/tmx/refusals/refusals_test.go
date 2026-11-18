package refusals

import "testing"

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
