package pool

import (
	"context"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestPoolExplainsAccountRefusal(t *testing.T) {
	for _, tc := range []struct{ provider, code, body, reason string }{
		{"meta", "", `{"error":{"code":"client_attestation_denied"}}`, refusals.MetaAttestation},
		{"antigravity", "tos_violation", "TOS_VIOLATION", "tos_violation"},
	} {
		m := auth.NewManager(nil, nil, nil)
		_, _ = m.Register(context.Background(), &auth.Auth{ID: "refused", Provider: tc.provider, Status: auth.StatusActive})
		m.MarkResult(context.Background(), auth.Result{AuthID: "refused", Provider: tc.provider, Model: "model", Error: &auth.Error{HTTPStatus: 403, Code: tc.code, Message: tc.body}})
		a, _ := m.GetByID("refused")
		got := Build(context.Background(), []*auth.Auth{a}, nil, nil, nil, "").Credentials[0]
		if got.Enabled || got.Status != auth.StatusDisabled || got.Reason != tc.reason || got.StatusMessage != refusals.StatusMessage(tc.reason) || got.PickRank != nil || got.NextRetryAfter != nil {
			t.Fatalf("pool=%+v", got)
		}
	}
}
