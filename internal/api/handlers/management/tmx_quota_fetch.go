package management

// tokenmaxx fork: lets internal/tmx/quota poll provider usage endpoints with the same token
// refresh and per-account proxy settings as /requests/api-call, without going over HTTP.

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// TmxAuths returns a snapshot of every account the proxy knows.
func (h *Handler) TmxAuths() []*coreauth.Auth {
	if h == nil || h.authManager == nil {
		return nil
	}
	return h.authManager.List()
}

// TmxFetch performs an authenticated GET for auth. "$TOKEN$" in header values is replaced with the
// account's (refreshed) access token.
func (h *Handler) TmxFetch(ctx context.Context, auth *coreauth.Auth, url string, headers map[string]string) (int, []byte, error) {
	if h == nil || auth == nil {
		return 0, nil, errors.New("no auth")
	}
	token, err := h.resolveTokenForAuth(ctx, auth, "")
	if token == "" {
		if err == nil {
			err = errors.New("auth token not found")
		}
		return 0, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, strings.ReplaceAll(v, "$TOKEN$", token))
	}
	client := &http.Client{Timeout: defaultAPICallTimeout, Transport: h.apiCallTransport(auth, "")}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, body, err
}
