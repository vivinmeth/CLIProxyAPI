package management

// tokenmaxx fork: usage requests use normal token refresh and per-account proxies.
import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

var (
	errTmxAuthChanged  = errors.New("account changed during request")
	errTmxAuthDisabled = errors.New("account disabled")
	errTmxUsageFailed  = errors.New("account usage request failed")
)

func (h *Handler) tmxGuardGoogleUsage(expected *coreauth.Auth, token string, checkToken bool) (*coreauth.Auth, error) {
	if h.authManager == nil {
		return nil, errTmxAuthChanged
	}
	current, ok := h.authManager.GetByID(expected.ID)
	if !ok || current.RegistrationEpoch != expected.RegistrationEpoch || current.CredentialVersion != expected.CredentialVersion || !refusals.GoogleProvider(current.Provider) {
		return nil, errTmxAuthChanged
	}
	if current.Disabled || current.Status == coreauth.StatusDisabled {
		return nil, errTmxAuthDisabled
	}
	if checkToken && token != strings.TrimSpace(tokenValueFromMetadata(current.Metadata)) {
		return nil, errTmxAuthChanged
	}
	return current, nil
}

func (h *Handler) TmxAuths() []*coreauth.Auth {
	if h == nil || h.authManager == nil {
		return nil
	}
	return h.authManager.List()
}

// TmxRefuseAccount persists a terminal quota refusal without inference accounting.
func (h *Handler) TmxRefuseAccount(ctx context.Context, sent *coreauth.Auth, code string) error {
	if h == nil || h.authManager == nil {
		return errTmxAuthChanged
	}
	return h.authManager.TmxRefuseAccount(ctx, sent, code)
}

func (h *Handler) TmxFetch(ctx context.Context, auth *coreauth.Auth, url string, headers map[string]string) (int, []byte, error) {
	return h.TmxRequest(ctx, auth, http.MethodGet, url, headers, nil)
}

// TmxRequest preserves the existing management/reset fetcher contract.
func (h *Handler) TmxRequest(ctx context.Context, auth *coreauth.Auth, method, url string, headers map[string]string, body []byte) (int, []byte, error) {
	status, data, _, err := h.TmxRequestSent(ctx, auth, method, url, headers, body)
	return status, data, err
}

// TmxRequestSent returns a read-only snapshot captured immediately before Do.
// A Google caller's snapshot is never mutated, even if its shared read outlives it.
func (h *Handler) TmxRequestSent(ctx context.Context, auth *coreauth.Auth, method, url string, headers map[string]string, body []byte) (int, []byte, *coreauth.Auth, error) {
	if h == nil || auth == nil {
		return 0, nil, nil, errors.New("no auth")
	}
	google := refusals.GoogleProvider(auth.Provider)
	expected := auth
	if google {
		if _, err := h.tmxGuardGoogleUsage(auth, "", false); err != nil {
			return 0, nil, nil, err
		}
	}
	token := ""
	for _, value := range headers {
		if strings.Contains(value, "$TOKEN$") {
			var err error
			if google {
				token, expected, err = h.tmxRefreshAntigravityOAuthAccessToken(coreauth.WithTmxTokenPersistence(ctx), auth, "")
			} else {
				token, err = h.resolveTokenForAuth(ctx, auth, "")
			}
			if token == "" || google && err != nil {
				if google && !errors.Is(err, errTmxAuthChanged) {
					err = errTmxUsageFailed
				}
				if err == nil {
					err = errors.New("auth token not found")
				}
				return 0, nil, nil, err
			}
			break
		}
	}
	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, url, reader)
	if err != nil {
		if google {
			err = errTmxUsageFailed
		}
		return 0, nil, nil, err
	}
	for k, v := range headers {
		req.Header.Set(k, strings.ReplaceAll(v, "$TOKEN$", token))
	}
	client := &http.Client{Timeout: defaultAPICallTimeout, Transport: h.apiCallTransport(auth, "")}
	var sent *coreauth.Auth
	if google {
		sent, err = h.tmxGuardGoogleUsage(expected, token, true)
		if err != nil {
			return 0, nil, nil, err
		}
	}
	resp, err := client.Do(req)
	if err != nil {
		if google {
			err = errTmxUsageFailed
		}
		return 0, nil, sent, err
	}
	defer func() { _ = resp.Body.Close() }()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if google {
		current, ok := h.authManager.GetByID(sent.ID)
		if !ok || current.RegistrationEpoch != sent.RegistrationEpoch || current.CredentialVersion != sent.CredentialVersion {
			return 0, nil, sent, errTmxAuthChanged
		}
		if err != nil {
			return 0, nil, sent, errTmxUsageFailed
		}
	}
	return resp.StatusCode, data, sent, err
}
