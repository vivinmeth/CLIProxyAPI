package cooldowns

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestAccountRefusalHasOwnReasonAndNoRetry(t *testing.T) {
	for _, code := range []string{refusals.MetaAttestation, "tos_violation"} {
		retry := time.Hour
		ev, ok := failureEvent(auth.FailureReport{At: time.Now(), AuthID: "refused", HTTPStatus: 403, ErrorCode: code, RetryAfter: &retry})
		if !ok || ev.Kind != KindAccountRefusal || ev.Reason != code || !ev.CredScope || ev.RetryAt != nil || ev.RetryAfterMs != 0 || ev.Message != refusals.StatusMessage(code) {
			t.Fatalf("event=%+v recorded=%v", ev, ok)
		}
	}
}

func TestToolAttestationHasOwnReasonWithOrdinary403History(t *testing.T) {
	ev, ok := failureEvent(auth.FailureReport{At: time.Now(), AuthID: "tools", HTTPStatus: 403, ErrorCode: refusals.AttestationRefused})
	if !ok || ev.Kind != KindAuthError || ev.Reason != refusals.AttestationRefused || ev.CredScope {
		t.Fatalf("event=%+v recorded=%v", ev, ok)
	}
}
