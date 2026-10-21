package auth

import (
	"context"
	"net/http"
	"testing"
	"time"
)

func markOverloaded(t *testing.T, model string, retryAfter *time.Duration) *Auth {
	t.Helper()
	withQuotaCooldownEnabled(t)
	manager := NewManager(nil, nil, nil)
	auth := &Auth{ID: "overloaded-" + model + "-" + time.Now().Format("150405.000000000"), Provider: "claude"}
	if _, err := manager.Register(WithSkipPersist(context.Background()), auth); err != nil {
		t.Fatalf("register auth: %v", err)
	}
	manager.MarkResult(context.Background(), Result{
		AuthID:     auth.ID,
		Provider:   auth.Provider,
		Model:      model,
		RetryAfter: retryAfter,
		Error:      &Error{HTTPStatus: statusOverloaded, Message: "overloaded"},
	})
	updated, ok := manager.GetByID(auth.ID)
	if !ok || updated == nil {
		t.Fatal("auth missing")
	}
	return updated
}

func TestOverloadedGetsShortTransientCooldown(t *testing.T) {
	start := time.Now()
	state := markOverloaded(t, "claude-opus-5", nil).ModelStates["claude-opus-5"]
	if state == nil {
		t.Fatal("model state missing")
	}
	if !state.Unavailable || state.NextRetryAfter.After(time.Now().Add(overloadedCooldown)) || state.NextRetryAfter.Before(start.Add(overloadedCooldown)) {
		t.Fatalf("529 cooldown until %v; want %v, not the generic %v", state.NextRetryAfter, overloadedCooldown, transientErrorCooldown)
	}
	if state.Quota.Exceeded {
		t.Fatal("an overload is not a quota exhaustion")
	}

	hint := 3 * time.Second
	state = markOverloaded(t, "claude-opus-5", &hint).ModelStates["claude-opus-5"]
	if state.NextRetryAfter.After(time.Now().Add(hint)) {
		t.Fatalf("a Retry-After hint must win: cooldown until %v", state.NextRetryAfter)
	}

	auth := markOverloaded(t, "", nil)
	if auth.NextRetryAfter.After(time.Now().Add(overloadedCooldown)) || auth.StatusMessage != "transient upstream error" {
		t.Fatalf("auth-level 529: until %v, message %q", auth.NextRetryAfter, auth.StatusMessage)
	}
}

func TestOverloadedJoinsRetryRoundsLike503(t *testing.T) {
	if !isCredentialRetryRoundStatus(http.StatusServiceUnavailable) || !isCredentialRetryRoundStatus(statusOverloaded) {
		t.Fatal("529 must get the same extra retry round as 503")
	}
	if got := cooldownErrorReason(&Error{HTTPStatus: statusOverloaded}); got != "transient_error" {
		t.Fatalf("cooldown reason = %q, want transient_error", got)
	}
}
