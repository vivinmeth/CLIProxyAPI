package cliproxy

import (
	"testing"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestResetSoonestRoutingSelector(t *testing.T) {
	for _, input := range []string{"reset-soonest", "resetsoonest", "soonest-reset", "rs", " Reset-Soonest "} {
		state := normalizedRoutingRuntimeState(&internalconfig.Config{
			Routing: internalconfig.RoutingConfig{Strategy: input},
		})
		if state.strategy != "reset-soonest" {
			t.Fatalf("strategy(%q) = %q, want reset-soonest", input, state.strategy)
		}
		if _, ok := newRoutingSelector(state).(*coreauth.ResetSoonestSelector); !ok {
			t.Fatalf("selector type(%q) = %T, want *auth.ResetSoonestSelector", input, newRoutingSelector(state))
		}
	}
}

func TestResetSoonestRoutingSelectorWithSessionAffinity(t *testing.T) {
	state := normalizedRoutingRuntimeState(&internalconfig.Config{
		Routing: internalconfig.RoutingConfig{Strategy: "reset-soonest", SessionAffinity: true},
	})
	selector, ok := newRoutingSelector(state).(*coreauth.SessionAffinitySelector)
	if !ok {
		t.Fatalf("selector type = %T, want *auth.SessionAffinitySelector", newRoutingSelector(state))
	}
	selector.Stop()
}
