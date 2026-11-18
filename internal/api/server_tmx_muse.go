package api

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/muse"
)

// registerTMXMuseModels serves Muse Code's model catalog at GET /muse-code/models on upstream's
// /muse-code group (client-key auth), where Muse fetches it (it strips the endpoint's /v1). It
// answers with Meta's own catalog minus contributor models, falling back to the registry's meta
// models, in place of upstream's registry-wide musecode.HandleModels. Upstream's relay keeps
// /muse-code/config, /search and /browser_open.
func (s *Server) registerTMXMuseModels(group *gin.RouterGroup) {
	group.GET("/models", gin.WrapF(muse.Handler(s.handlers.AuthManager)))
}
