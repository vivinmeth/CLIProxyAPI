package config

import "testing"

func TestRoutingStrategyResetSoonestParses(t *testing.T) {
	for _, raw := range []string{
		"routing: {strategy: reset-soonest}\n",
		"config-version: 8\nrouting: {strategy: reset-soonest}\n",
	} {
		cfg, err := ParseConfigBytes([]byte(raw))
		if err != nil || cfg.Routing.Strategy != "reset-soonest" {
			t.Fatalf("ParseConfigBytes(%q) strategy = %v, %v; want reset-soonest", raw, cfg, err)
		}
	}
	if err := ValidateV8Config([]byte("config-version: 8\nrouting: {strategy: reset-soonest}\n")); err != nil {
		t.Fatalf("ValidateV8Config() error = %v", err)
	}
}
