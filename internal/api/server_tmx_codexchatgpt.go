package api

import (
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api/middleware"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/codexchatgpt"
)

// registerTMXCodexChatGPTRoutes answers the ChatGPT-backend calls Codex makes in ChatGPT login
// mode (internal/tmx/codexchatgpt).
//
//   - Analytics carries the client key, so it uses client-key auth like /backend-api/codex; once
//     authenticated it is not archived (no model traffic, one batch per turn), while a bad key
//     still shows as a rejected row.
//   - MCP gets no key from Codex (its connectors client sends none), so it has no auth; it only
//     ever answers an empty server, and is not archived.
//   - Refresh has no auth either (Codex sends no key). It skips capture, and the global request
//     logger excludes this path so neither successful nor failed requests expose refresh tokens.
//   - Accounts requires client-key auth before it reads the account header. Capture is skipped
//     even on auth failure.
func (s *Server) registerTMXCodexChatGPTRoutes() {
	s.engine.POST(codexchatgpt.AnalyticsPath, AuthMiddleware(s.accessManager), middleware.TMXSkipCapture, gin.WrapF(codexchatgpt.Analytics))
	s.engine.Any(codexchatgpt.MCPPath, middleware.TMXSkipCapture, gin.WrapF(codexchatgpt.MCP))
	s.engine.POST(codexchatgpt.RefreshPath, middleware.TMXSkipCapture, gin.WrapF(codexchatgpt.Refresh))
	s.engine.GET(codexchatgpt.AccountsPath, middleware.TMXSkipCapture, AuthMiddleware(s.accessManager), gin.WrapF(codexchatgpt.Accounts))
}
