package config

import "time"

// MetaRefusalBreakerConfig bounds process-local automatic Meta account disables.
// Zero values use defaults; out-of-range values clamp to the documented bounds.
type MetaRefusalBreakerConfig struct {
	Threshold     int `yaml:"threshold" json:"threshold"`
	WindowSeconds int `yaml:"window-seconds" json:"window-seconds"`
}

func (c MetaRefusalBreakerConfig) Effective() (int, time.Duration) {
	threshold, seconds := c.Threshold, c.WindowSeconds
	if threshold == 0 {
		threshold = 2
	}
	if seconds == 0 {
		seconds = 3600
	}
	threshold = max(1, min(100, threshold))
	seconds = max(60, min(86400, seconds))
	return threshold, time.Duration(seconds) * time.Second
}
