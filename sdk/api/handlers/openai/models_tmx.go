package openai

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
)

// tmxModelsForClient drops hidden models (OpenCode Zen) from a model list, and Claude models when
// the list goes to a Codex client (tokenmaxx fork; internal/tmx/clients).
func tmxModelsForClient(c *gin.Context, models []map[string]any) []map[string]any {
	codex := c != nil && c.Request != nil && clients.IsCodex(c.Request.Header)
	return clients.FilterModels(models, codex)
}

// tmxCodexCatalogModels filters the ?client_version= catalog: Claude models never go to Codex.
// "cpa" asks for upstream's CPA capability format, which is not a Codex catalog, so only hidden
// models are dropped there.
func tmxCodexCatalogModels(models []map[string]any, clientVersion string) []map[string]any {
	return clients.FilterModels(models, clientVersion != "cpa")
}
