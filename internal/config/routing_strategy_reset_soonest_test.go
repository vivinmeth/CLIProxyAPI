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

func TestResetSoonestPolicyParsesV8(t *testing.T) {
	raw := []byte("config-version: 8\nrouting:\n  strategy: reset-soonest\n  reset-soonest:\n    five-hour-threshold: 0.9\n    seven-day-threshold: 0.97\n    max-inflight-per-credential: 8\n")
	if err := ValidateV8Config(raw); err != nil {
		t.Fatal(err)
	}
	cfg, err := ParseConfigBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if p := cfg.Routing.ResetSoonest; p.FiveHourThreshold != .9 || p.SevenDayThreshold != .97 || p.MaxInflightPerCredential != 8 {
		t.Fatalf("policy = %+v", p)
	}
}
