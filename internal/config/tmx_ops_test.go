package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTMXOpsConfigDefaultsAndCanonicalRoundTrip(t *testing.T) {
	cfg, err := ParseConfigBytes([]byte("config-version: 8\n"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ShutdownDrainTimeout() != 90*time.Second || cfg.Routing.SessionAffinityPersist != nil {
		t.Fatalf("defaults: %+v", cfg.Server)
	}
	raw := []byte("config-version: 8\nserver:\n  host: localhost\n  shutdown-drain-timeout: 0\nrouting:\n  session-affinity-persist: false\narchive:\n  live-capture-max-bytes: 1234\n  live-capture-exchange-max-bytes: 567\n")
	cfg, err = ParseConfigBytes(raw)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ShutdownDrainTimeout() != 0 || cfg.Host != "localhost" || cfg.Routing.SessionAffinityPersist == nil || *cfg.Routing.SessionAffinityPersist || cfg.Archive.LiveCaptureMaxBytes != 1234 {
		t.Fatal("config values lost")
	}
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err = os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = SaveConfigPreserveComments(path, cfg); err != nil {
		t.Fatal(err)
	}
	saved, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	fresh, err := ParseConfigBytes(saved)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.ShutdownDrainTimeout() != 0 || fresh.Archive.LiveCaptureExchangeMaxBytes != 567 || fresh.Routing.SessionAffinityPersist == nil || *fresh.Routing.SessionAffinityPersist {
		t.Fatalf("round trip: %s", saved)
	}
	example, err := os.ReadFile("../../config.example.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = ParseConfigBytes(example); err != nil {
		t.Fatalf("example invalid: %v", err)
	}
}
