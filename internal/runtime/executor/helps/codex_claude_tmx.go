package helps

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
)

// tmxRefusal is the request-scoped 400 for a Claude upstream model asked for by a Codex client.
type tmxRefusal struct{ body string }

func (e tmxRefusal) Error() string   { return e.body }
func (e tmxRefusal) StatusCode() int { return http.StatusBadRequest }

// TMXCodexClaudeUpstream is the owner's per-client rule checked where an executor knows the
// upstream model (tokenmaxx fork; internal/tmx/clients): an alias can pool several upstream names,
// so the handler-level check can't see every one. A Codex client, or any Responses API request,
// sending a Claude upstream model gets a 400. nil otherwise.
func TMXCodexClaudeUpstream(ctx context.Context, sourceFormat, upstreamModel string) error {
	if !clients.IsClaudeModel(upstreamModel, nil) {
		return nil
	}
	var h http.Header
	if ctx != nil {
		if c, ok := ctx.Value("gin").(*gin.Context); ok && c != nil && c.Request != nil {
			h = c.Request.Header
		}
	}
	if sourceFormat != "openai-response" && !clients.IsCodex(h) {
		return nil
	}
	return tmxRefusal{body: clients.CodexClaudeErrorBody(upstreamModel)}
}
