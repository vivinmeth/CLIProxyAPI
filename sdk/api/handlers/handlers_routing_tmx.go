package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/muse"
)

// tmxContributorBlocked refuses Muse Contributor-mode models (and aliases of them) before routing
// (tokenmaxx fork; internal/tmx/muse/contributor.go). They are unregistered, so without this the
// client would only see "unknown provider for model".
func tmxContributorBlocked(models ...string) *interfaces.ErrorMessage {
	for _, m := range models {
		if muse.ContributorRequestBlocked(m) {
			return &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: errors.New(muse.ContributorErrorBody(m))}
		}
	}
	return nil
}

// tmxProvidersForExecution is providersForExecution plus the owner's per-client rule (tokenmaxx
// fork; internal/tmx/clients): a Codex client asking for a Claude model, by id, alias or any route
// that resolves to the Claude provider (the final model, after "auto" resolves), gets a 400 before
// routing. Any request on the Responses API counts as Codex's: no other client sends Claude models
// there. Claude work goes through Claude Code.
func (h *BaseAPIHandler) tmxProvidersForExecution(ctx context.Context, entryProtocol, modelName, originalRequestedModel string, allowImageModel bool, routeDecision modelRouteDecision, execOptions modelExecutionOptions) ([]string, string, *interfaces.ErrorMessage) {
	providers, normalizedModel, errMsg := h.providersForExecution(modelName, originalRequestedModel, allowImageModel, routeDecision, execOptions)
	if errMsg != nil {
		return providers, normalizedModel, errMsg
	}
	if errMsg = tmxCodexClaudeRefusal(ctx, entryProtocol, providers, originalRequestedModel, modelName, normalizedModel, routeDecision.Model); errMsg != nil {
		return nil, "", errMsg
	}
	return providers, normalizedModel, nil
}

// tmxCodexClaudeRefusal is the per-client rule's check, also run before a plugin executor (which
// skips provider resolution): the 400 for a Claude model asked for by a Codex or Responses client.
func tmxCodexClaudeRefusal(ctx context.Context, entryProtocol string, providers []string, models ...string) *interfaces.ErrorMessage {
	if entryProtocol != constant.OpenaiResponse && !clients.IsCodex(tmxRequestHeaders(ctx)) {
		return nil
	}
	for _, m := range models {
		if m != "" && clients.ResolvesToClaude(m, providers) {
			return &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: errors.New(clients.CodexClaudeErrorBody(models[0]))}
		}
	}
	return nil
}

// tmxRequestHeaders returns the inbound request's headers carried in ctx, or nil.
func tmxRequestHeaders(ctx context.Context) http.Header {
	if ctx == nil {
		return nil
	}
	if c, ok := ctx.Value("gin").(*gin.Context); ok && c != nil && c.Request != nil {
		return c.Request.Header
	}
	return nil
}
