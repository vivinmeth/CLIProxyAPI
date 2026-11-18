package middleware

import "github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/codexchatgpt"

// Exclude the whole exchange before any body capture, including full and error-only file logs.
func tmxSkipCodexRefreshLogging(path string) bool {
	return path == codexchatgpt.RefreshPath
}
