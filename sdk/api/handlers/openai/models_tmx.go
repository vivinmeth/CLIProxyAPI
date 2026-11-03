package openai

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
)

// tmxModelsForClient drops Claude models from a list served to a Codex client (tokenmaxx fork;
// internal/tmx/clients).
func tmxModelsForClient(c *gin.Context, models []map[string]any) []map[string]any {
	if c == nil || c.Request == nil || !clients.IsCodex(c.Request.Header) {
		return models
	}
	return clients.FilterModelsForCodex(models, registry.GetGlobalRegistry().GetModelProviders)
}
