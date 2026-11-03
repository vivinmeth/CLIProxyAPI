package opencode

import "testing"

func TestIsAccountLimit(t *testing.T) {
	limit := []byte(`{"type":"error","error":{"type":"GoUsageLimitError","message":"Go usage limit exceeded"},"metadata":{"workspace":"wrk_x","limitName":"5 hour"}}`)
	if !IsAccountLimit(429, limit) {
		t.Fatal("Go usage limit not recognised")
	}
	for _, c := range []struct {
		status int
		body   string
	}{{500, string(limit)}, {429, `{"error":{"type":"rate_limit","message":"slow down"}}`}, {429, `not json`}, {402, `{"error":{"message":"Insufficient account funds"}}`}} {
		if IsAccountLimit(c.status, []byte(c.body)) {
			t.Errorf("%d %s read as the Go account limit", c.status, c.body)
		}
	}
}
