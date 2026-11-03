package helps

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestTMXCodexClaudeUpstream(t *testing.T) {
	ctxWith := func(ua string) context.Context {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		c.Request.Header.Set("User-Agent", ua)
		return context.WithValue(context.Background(), "gin", c)
	}
	codex := ctxWith("codex_exec/0.165.0 (Ubuntu 24.4.0; x86_64) dumb (codex_exec; 0.165.0)")
	claude := ctxWith("claude-cli/2.1.280 (external, cli)")
	for _, tc := range []struct {
		name     string
		ctx      context.Context
		format   string
		upstream string
		refused  bool
	}{
		{"codex, claude upstream behind an alias pool", codex, "openai", "anthropic/claude-sonnet-5", true},
		{"any responses client, claude upstream", claude, "openai-response", "claude-opus-5-5", true},
		{"claude code on messages", claude, "claude", "anthropic/claude-sonnet-5", false},
		{"codex, non-claude upstream", codex, "openai-response", "kimi-k3", false},
		{"no request context", context.Background(), "openai", "claude-opus-5-5", false},
	} {
		err := TMXCodexClaudeUpstream(tc.ctx, tc.format, tc.upstream)
		if (err != nil) != tc.refused {
			t.Errorf("%s: err = %v", tc.name, err)
		}
		if err != nil {
			if sc, ok := err.(interface{ StatusCode() int }); !ok || sc.StatusCode() != http.StatusBadRequest {
				t.Errorf("%s: not a 400: %v", tc.name, err)
			}
		}
	}
}
