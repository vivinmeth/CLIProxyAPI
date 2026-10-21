package management

// tokenmaxx fork: what internal/tmx/resets needs from the management handler — authenticated
// provider calls with a body (reset-credit redeem POSTs), plus the scoped cooldown and quota-signal
// release that follows a successful redeem.

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// TmxSend performs an authenticated request for auth. "$TOKEN$" in header values is replaced with
// the account's access token. The caller bounds the call with ctx.
func (h *Handler) TmxSend(ctx context.Context, auth *coreauth.Auth, method, url string, headers map[string]string, body []byte) (int, []byte, error) {
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
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
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
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, respBody, err
}

// TmxClearRecoveredQuota releases what a redeemed reset credit restored on one auth: the quota
// cooldowns and passive quota signals of the recovered windows, nothing else.
func (h *Handler) TmxClearRecoveredQuota(ctx context.Context, authID string, r coreauth.QuotaRecovery) error {
	if h == nil || h.authManager == nil {
		return errors.New("core auth manager unavailable")
	}
	_, err := h.authManager.ClearRecoveredQuota(ctx, authID, r)
	return err
}
