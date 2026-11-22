package management

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strings"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

const (
	tmxXAIBillingURL = "https://cli-chat-proxy.grok.com/v1/billing?format=credits"
	// Installed CLI baseline for this billing contract. Do not fetch npm metadata
	// from a billing read, or borrow the executor's older fallback fingerprint.
	tmxXAIBillingVersion = "1.0.89"
)

var errTmxXAIBilling = errors.New("xai billing: current OAuth credentials unavailable or changed")

// tmxCurrentXAIBillingAuth only reads the lifecycle-owned auth manager. It never
// resolves a token, loads a profile, contacts discovery, or refreshes a grant.
func (h *Handler) tmxCurrentXAIBillingAuth(expected *coreauth.Auth) (*coreauth.Auth, error) {
	if h == nil || h.authManager == nil || expected == nil {
		return nil, errTmxXAIBilling
	}
	current, ok := h.authManager.GetByID(expected.ID)
	if !ok || current.RegistrationEpoch != expected.RegistrationEpoch || current.CredentialVersion != expected.CredentialVersion || !strings.EqualFold(strings.TrimSpace(current.Provider), "xai") || current.AuthKind() != coreauth.AuthKindOAuth || current.Disabled || current.Status == coreauth.StatusDisabled {
		return nil, errTmxXAIBilling
	}
	token := stringValue(current.Metadata, "access_token")
	subject := stringValue(current.Metadata, "sub")
	expiry, err := time.Parse(time.RFC3339Nano, stringValue(current.Metadata, "expired"))
	if token == "" || subject == "" || err != nil || !expiry.After(time.Now()) {
		return nil, errTmxXAIBilling
	}
	return current, nil
}

// TmxXAIBilling is a private quota transport contract, not a management API-call
// endpoint. Its one fixed GET uses the current token and subject from the same
// manager snapshot, with account/global proxy policy. There are no refreshes,
// redirects, explicit retries, or credential mutations, even after a 401.
func (h *Handler) TmxXAIBilling(ctx context.Context, expected *coreauth.Auth) (int, []byte, *coreauth.Auth, error) {
	sent, err := h.tmxCurrentXAIBillingAuth(expected)
	if err != nil {
		return 0, nil, nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, tmxXAIBillingURL, nil)
	if err != nil {
		return 0, nil, nil, errTmxXAIBilling
	}
	req.Header.Set("Authorization", "Bearer "+stringValue(sent.Metadata, "access_token"))
	req.Header.Set("X-XAI-Token-Auth", "xai-grok-cli")
	req.Header.Set("x-userid", stringValue(sent.Metadata, "sub"))
	req.Header.Set("x-grok-client-version", tmxXAIBillingVersion)
	req.Header.Set("x-grok-client-mode", "headless")
	req.Header.Set("User-Agent", tmxXAIBillingUserAgent())
	client := &http.Client{Transport: h.apiCallTransport(sent, ""), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	// Recheck immediately before sending, after transport setup. The request uses
	// only sent's token/subject, never a mix with a concurrent replacement.
	if _, err = h.tmxCurrentXAIBillingAuth(sent); err != nil {
		return 0, nil, nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, sent, errTmxXAIBilling
	}
	defer func() { _ = resp.Body.Close() }()
	const maxBillingBytes = 4 << 20
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxBillingBytes+1))
	if err != nil || len(data) > maxBillingBytes {
		return 0, nil, sent, errTmxXAIBilling
	}
	if _, err = h.tmxCurrentXAIBillingAuth(sent); err != nil {
		return 0, nil, sent, err
	}
	return resp.StatusCode, data, sent, nil
}

// grok-build's shared-client UserAgent::render uses the public grok-shell
// product/version with Rust platform spelling (http/lib.rs:114-190).
func tmxXAIBillingUserAgent() string {
	osName, arch := runtime.GOOS, runtime.GOARCH
	if osName == "darwin" {
		osName = "macos"
	}
	switch arch {
	case "amd64":
		arch = "x86_64"
	case "arm64":
		arch = "aarch64"
	case "386":
		arch = "x86"
	}
	return fmt.Sprintf("grok-shell/%s (%s; %s)", tmxXAIBillingVersion, osName, arch)
}
