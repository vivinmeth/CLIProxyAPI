package management

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/MoeForge/moego/json"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Commit only tokens on the current record. A disabled account still needs a
// rotated refresh token, but the request guard prevents sending its usage call.
func (h *Handler) tmxCommitAntigravityTokens(ctx context.Context, base *coreauth.Auth, access, refresh string, expires int64) (string, *coreauth.Auth, error) {
	if h == nil || h.authManager == nil {
		return "", nil, errTmxAuthChanged
	}
	now := time.Now()
	committed, ok, err := h.authManager.Modify(ctx, base.ID, base.RegistrationEpoch, func(current *coreauth.Auth) error {
		if current.RegistrationEpoch != base.RegistrationEpoch || current.CredentialVersion != base.CredentialVersion || coreauth.CredentialsChanged(base, current) {
			return errTmxAuthChanged
		}
		if current.Metadata == nil {
			current.Metadata = make(map[string]any)
		}
		current.Metadata["access_token"] = strings.TrimSpace(access)
		if strings.TrimSpace(refresh) != "" {
			current.Metadata["refresh_token"] = strings.TrimSpace(refresh)
		}
		if expires > 0 {
			current.Metadata["expires_in"] = expires
			current.Metadata["timestamp"] = now.UnixMilli()
			current.Metadata["expired"] = now.Add(time.Duration(expires) * time.Second).Format(time.RFC3339)
		}
		if _, exists := current.Metadata["type"]; !exists {
			current.Metadata["type"] = "antigravity"
		}
		current.LastRefreshedAt = now
		return nil
	})
	if err != nil || !ok || committed == nil {
		return "", nil, errTmxAuthChanged
	}
	return strings.TrimSpace(tokenValueFromMetadata(committed.Metadata)), committed, nil
}

func (h *Handler) tmxRefreshAntigravityOAuthAccessToken(ctx context.Context, auth *coreauth.Auth, requestProxyURL string) (string, *coreauth.Auth, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if auth == nil {
		return "", nil, nil
	}

	metadata := auth.Metadata
	if len(metadata) == 0 {
		return "", nil, fmt.Errorf("antigravity oauth metadata missing")
	}

	current := strings.TrimSpace(tokenValueFromMetadata(metadata))
	if current != "" && !antigravityTokenNeedsRefresh(metadata) {
		return current, auth, nil
	}

	refreshToken := stringValue(metadata, "refresh_token")
	if refreshToken == "" {
		return "", nil, fmt.Errorf("antigravity refresh token missing")
	}
	base := auth.Clone() // tmx: fence the token-only commit against concurrent credential changes.

	tokenURL := strings.TrimSpace(antigravityOAuthTokenURL)
	if tokenURL == "" {
		tokenURL = "https://oauth2.googleapis.com/token"
	}
	form := url.Values{}
	form.Set("client_id", antigravityOAuthClientID)
	form.Set("client_secret", antigravityOAuthClientSecret)
	form.Set("grant_type", "refresh_token")
	form.Set("refresh_token", refreshToken)

	req, errReq := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if errReq != nil {
		return "", nil, errReq
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	httpClient := &http.Client{
		Timeout:   defaultAPICallTimeout,
		Transport: h.apiCallTransport(auth, requestProxyURL),
	}
	resp, errDo := httpClient.Do(req)
	if errDo != nil {
		return "", nil, errDo
	}
	defer func() { _ = resp.Body.Close() }()

	bodyBytes, errRead := io.ReadAll(resp.Body)
	if errRead != nil {
		return "", nil, errRead
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return "", nil, fmt.Errorf("antigravity oauth token refresh failed: status %d", resp.StatusCode)
	}

	var tokenResp struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		ExpiresIn    int64  `json:"expires_in"`
		TokenType    string `json:"token_type"`
	}
	if errUnmarshal := json.Unmarshal(bodyBytes, &tokenResp); errUnmarshal != nil {
		return "", nil, fmt.Errorf("antigravity oauth token refresh returned invalid JSON")
	}

	if strings.TrimSpace(tokenResp.AccessToken) == "" {
		return "", nil, fmt.Errorf("antigravity oauth token refresh returned empty access_token")
	}

	// tmx: preserve the current operator state and reject superseded refreshes.
	return h.tmxCommitAntigravityTokens(ctx, base, tokenResp.AccessToken, tokenResp.RefreshToken, tokenResp.ExpiresIn)
}
