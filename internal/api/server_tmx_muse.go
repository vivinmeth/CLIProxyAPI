package api

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/muse"
)

// registerTMXMuseRoutes serves Muse Code's model catalog at the host root, where Muse fetches it
// (it strips the endpoint's /v1). Same client-key auth as /v1.
func (s *Server) registerTMXMuseRoutes() {
	s.engine.GET(muse.CatalogPath, AuthMiddleware(s.accessManager), gin.WrapF(muse.Handler(s.handlers.AuthManager)))
}
