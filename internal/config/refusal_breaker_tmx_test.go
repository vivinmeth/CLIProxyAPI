package config

import (
	"fmt"
	"testing"
	"time"
)

func TestTMXMetaRefusalBreakerConfig(t *testing.T) {
	for _, tc := range []struct{ threshold, seconds, wantThreshold, wantSeconds int }{
		{0, 0, 2, 3600}, {1, 60, 1, 60}, {5, 7200, 5, 7200}, {-1, -1, 1, 60}, {101, 86401, 100, 86400},
	} {
		cfg, err := ParseConfigBytes([]byte(fmt.Sprintf("tmx-meta-refusal-breaker:\n  threshold: %d\n  window-seconds: %d\n", tc.threshold, tc.seconds)))
		if err != nil {
			t.Fatal(err)
		}
		n, window := cfg.CloneForRuntime().TMXMetaRefusalBreaker.Effective()
		if n != tc.wantThreshold || window != time.Duration(tc.wantSeconds)*time.Second {
			t.Fatalf("%+v: got %d %s", tc, n, window)
		}
	}
	if _, err := ParseConfigBytes([]byte("tmx-meta-refusal-breaker:\n  threshold: many\n")); err == nil {
		t.Fatal("invalid threshold accepted")
	}
}
