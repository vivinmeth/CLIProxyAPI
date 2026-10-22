package handlers

import (
	"errors"
	"net/http"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
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
