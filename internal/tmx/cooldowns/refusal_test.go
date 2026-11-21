package cooldowns

import (
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestAccountRefusalHasOwnReasonAndNoRetry(t *testing.T) {
	for _, code := range []string{refusals.MetaAttestation, "tos_violation", refusals.GoogleValidation} {
		retry := time.Hour
		ev, ok := failureEvent(auth.FailureReport{At: time.Now(), AuthID: "refused", HTTPStatus: 403, ErrorCode: code, RetryAfter: &retry, Message: "https://example.invalid/verification/session-private"})
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

func TestAccountRefusalMemoryOnlyNoteIsFixed(t *testing.T) {
	for _, message := range []string{refusals.StatusMessage(refusals.GoogleValidation) + refusals.MemoryOnlySuffix, "https://example.invalid/session-private"} {
		ev, ok := failureEvent(auth.FailureReport{At: time.Now(), AuthID: "refused", HTTPStatus: 403, ErrorCode: refusals.GoogleValidation, Message: message})
		want := refusals.StatusMessage(refusals.GoogleValidation)
		if message == want+refusals.MemoryOnlySuffix {
			want = message
		}
		if !ok || ev.Kind != KindAccountRefusal || ev.Message != want {
			t.Fatalf("unsafe or missing persistence note: %+v", ev)
		}
	}
}
