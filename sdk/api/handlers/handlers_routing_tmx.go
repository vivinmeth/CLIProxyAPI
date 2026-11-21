package handlers

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/constant"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
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

// tmxProvidersForExecution checks resolved routes before dispatch: Grok-shell uses only xAI,
// and Codex/Responses clients cannot request Claude models (internal/tmx/clients).
func (h *BaseAPIHandler) tmxProvidersForExecution(ctx context.Context, entryProtocol, modelName, originalRequestedModel string, allowImageModel bool, routeDecision modelRouteDecision, execOptions modelExecutionOptions) ([]string, string, *interfaces.ErrorMessage) {
	providers, normalizedModel, errMsg := h.providersForExecution(modelName, originalRequestedModel, allowImageModel, routeDecision, execOptions)
	if errMsg != nil {
		return providers, normalizedModel, errMsg
	}
	if errMsg = tmxClientModelRefusal(ctx, entryProtocol, providers, execOptions.Headers, originalRequestedModel, modelName, normalizedModel, routeDecision.Model); errMsg != nil {
		return nil, "", errMsg
	}
	return providers, normalizedModel, nil
}

// tmxClientModelRefusal applies Grok's boundary alongside the existing Codex/Responses rule.
// Plugin executors pass no providers, so their opaque routes cannot prove xAI identity.
// Explicit helper headers may identify Grok, but cannot erase an inbound Grok identity.
func tmxClientModelRefusal(ctx context.Context, entryProtocol string, providers []string, headers http.Header, models ...string) *interfaces.ErrorMessage {
	if clients.IsGrokShell(tmxRequestHeaders(ctx)) || clients.IsGrokShell(headers) {
		allowed := clients.OnlyXAI(providers)
		for _, model := range models {
			if model == "" || strings.TrimSpace(thinking.ParseSuffix(model).ModelName) == "auto" {
				continue
			}
			if !clients.ResolvesOnlyToXAI(model, nil) {
				allowed = false
			}
		}
		if !allowed {
			return &interfaces.ErrorMessage{StatusCode: http.StatusBadRequest, Error: errors.New(clients.GrokModelErrorBody(models[0]))}
		}
	}
	return tmxCodexClaudeRefusal(ctx, entryProtocol, providers, models...)
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
