package api

import (
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
)

func (s *Server) configureTMXCapture(cfg *config.Config) {
	if cfg != nil {
		capture.Configure(cfg.Archive.LiveCaptureMaxBytes, cfg.Archive.LiveCaptureExchangeMaxBytes, "")
	}
}
