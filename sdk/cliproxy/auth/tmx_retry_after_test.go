package auth

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"
)

type upstreamRefusalTestError struct {
	status     int
	retryAfter *time.Duration
}

func (e upstreamRefusalTestError) Error() string              { return fmt.Sprintf("upstream %d", e.status) }
func (e upstreamRefusalTestError) StatusCode() int            { return e.status }
func (e upstreamRefusalTestError) RetryAfter() *time.Duration { return e.retryAfter }

func TestSafeResponseHeadersCarriesUpstreamRetryAfter(t *testing.T) {
	d := func(v time.Duration) *time.Duration { return &v }
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"429 with deadline", upstreamRefusalTestError{http.StatusTooManyRequests, d(90 * time.Second)}, "90"},
		{"rounds up partial seconds", upstreamRefusalTestError{http.StatusTooManyRequests, d(1500 * time.Millisecond)}, "2"},
		{"weekly reset is capped at a day", upstreamRefusalTestError{http.StatusTooManyRequests, d(72 * time.Hour)}, "86400"},
		{"503 with deadline", upstreamRefusalTestError{http.StatusServiceUnavailable, d(5 * time.Second)}, "5"},
		{"529 with deadline", upstreamRefusalTestError{statusOverloaded, d(5 * time.Second)}, "5"},
		{"wrapped", fmt.Errorf("execute: %w", upstreamRefusalTestError{http.StatusTooManyRequests, d(30 * time.Second)}), "30"},
		{"unknown deadline", upstreamRefusalTestError{http.StatusTooManyRequests, nil}, ""},
		{"non-retry status", upstreamRefusalTestError{http.StatusBadRequest, d(30 * time.Second)}, ""},
		{"plain error", errors.New("boom"), ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := SafeResponseHeaders(tc.err).Get("Retry-After"); got != tc.want {
				t.Fatalf("Retry-After = %q, want %q", got, tc.want)
			}
		})
	}
}
