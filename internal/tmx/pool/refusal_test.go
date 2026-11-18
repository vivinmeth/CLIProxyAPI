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

func TestPoolExplainsToolAttestationCooldown(t *testing.T) {
	m := auth.NewManager(nil, nil, nil)
	ctx := context.Background()
	_, _ = m.Register(ctx, &auth.Auth{ID: "tools", Provider: "meta", Status: auth.StatusActive})
	m.MarkResult(ctx, auth.Result{AuthID: "tools", Provider: "meta", Model: "model", Error: &auth.Error{HTTPStatus: 403, Code: refusals.AttestationRefused, Message: `{"error":{"code":"client_attestation_denied"}}`}})
	a, _ := m.GetByID("tools")
	got := Build(ctx, []*auth.Auth{a}, nil, nil, nil, "").Credentials[0]
	if !got.Enabled || got.Status == auth.StatusDisabled || got.Reason != refusals.AttestationRefused || got.StatusMessage != refusals.AttestationRefused || got.NextRetryAfter == nil {
		t.Fatalf("pool=%+v", got)
	}
	if len(got.Cooldowns) != 1 || got.Cooldowns[0].Reason != refusals.AttestationRefused || got.Cooldowns[0].HTTPStatus != 403 {
		t.Fatalf("pool cooldowns=%+v", got.Cooldowns)
	}
}
