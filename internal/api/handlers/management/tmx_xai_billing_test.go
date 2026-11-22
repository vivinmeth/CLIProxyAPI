package management

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type xaiBillingReader interface {
	TmxXAIBilling(context.Context, *coreauth.Auth) (int, []byte, *coreauth.Auth, error)
}

func billingReader(t *testing.T, h *Handler) xaiBillingReader {
	t.Helper()
	r, ok := any(h).(xaiBillingReader)
	if !ok {
		t.Fatal("current-token billing helper missing")
	}
	return r
}
func syntheticXAIBillingAuth() *coreauth.Auth {
	return &coreauth.Auth{ID: "billing-synthetic", Provider: "xai", Status: coreauth.StatusActive, Metadata: map[string]any{"auth_kind": "oauth", "access_token": "synthetic-current", "sub": "synthetic-user", "expired": "2099-01-01T00:00:00Z", "refresh_token": "synthetic-unused"}}
}

// Route the fixed production URL to TLS loopback. No DNS or vendor requests.
func xaiBillingLoopback(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	transport := server.Client().Transport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // synthetic loopback certificate
	transport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr != "cli-chat-proxy.grok.com:443" {
			return nil, fmt.Errorf("unexpected non-loopback destination")
		}
		return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
	}
	old := http.DefaultTransport
	http.DefaultTransport = transport
	t.Cleanup(func() { http.DefaultTransport = old; transport.CloseIdleConnections() })
}
func TestTmxXAIBillingCurrentTokenHeadersAndNoRetry(t *testing.T) {
	var calls atomic.Int32
	xaiBillingLoopback(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != "GET" || r.URL.RequestURI() != "/v1/billing?format=credits" || r.Host != "cli-chat-proxy.grok.com" {
			t.Error("wrong billing endpoint", r.URL)
		}
		for key, want := range map[string]string{"Authorization": "Bearer synthetic-current", "X-XAI-Token-Auth": "xai-grok-cli", "x-userid": "synthetic-user", "x-grok-client-version": "1.0.89", "x-grok-client-mode": "headless"} {
			if got := r.Header.Get(key); got != want {
				t.Errorf("%s differs", key)
			}
		}
		if !strings.Contains(r.Header.Get("User-Agent"), "grok-shell/1.0.89 (") {
			t.Error("wrong public shared-client User-Agent")
		}
		for _, key := range []string{"x-authenticateresponse", "x-grok-client-identifier"} {
			if r.Header.Get(key) != "" {
				t.Error("unexpected legacy header", key)
			}
		}
		w.WriteHeader(401)
		fmt.Fprint(w, `{"error":"synthetic"}`)
	})
	m := coreauth.NewManager(nil, nil, nil)
	a, err := m.Register(context.Background(), syntheticXAIBillingAuth())
	if err != nil {
		t.Fatal(err)
	}
	h := &Handler{authManager: m}
	st, _, sent, err := billingReader(t, h).TmxXAIBilling(context.Background(), a)
	if err != nil || st != 401 || sent == nil || sent.ID != a.ID || sent.CredentialVersion != a.CredentialVersion || calls.Load() != 1 {
		t.Fatalf("st=%d sent=%v calls=%d err=%v", st, sent != nil, calls.Load(), err)
	}
	live, _ := m.GetByID(a.ID)
	if live.CredentialVersion != a.CredentialVersion || live.Metadata["access_token"] != a.Metadata["access_token"] || live.Metadata["refresh_token"] != a.Metadata["refresh_token"] {
		t.Fatal("billing changed credentials")
	}
}
func TestTmxXAIBillingFailsClosed(t *testing.T) {
	var calls atomic.Int32
	xaiBillingLoopback(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fmt.Fprint(w, `{"config":{"creditUsagePercent":25}}`)
	})
	for _, mode := range []string{"nil-auth", "nil-manager", "disabled", "status-disabled", "provider", "api-key", "no-token", "no-sub", "no-expiry", "bad-expiry", "expired", "new-grant", "new-registration"} {
		t.Run(mode, func(t *testing.T) {
			m := coreauth.NewManager(nil, nil, nil)
			auth := syntheticXAIBillingAuth()
			switch mode {
			case "disabled":
				auth.Disabled = true
			case "status-disabled":
				auth.Status = coreauth.StatusDisabled
			case "provider":
				auth.Provider = "claude"
			case "api-key":
				auth.Metadata["auth_kind"] = "api_key"
			case "no-token":
				delete(auth.Metadata, "access_token")
			case "no-sub":
				delete(auth.Metadata, "sub")
			case "no-expiry":
				delete(auth.Metadata, "expired")
			case "bad-expiry":
				auth.Metadata["expired"] = "later"
			case "expired":
				auth.Metadata["expired"] = "2000-01-01T00:00:00Z"
			}
			a, err := m.Register(context.Background(), auth)
			if err != nil {
				t.Fatal(err)
			}
			h := &Handler{authManager: m}
			switch mode {
			case "nil-auth":
				a = nil
			case "nil-manager":
				h.authManager = nil
			case "new-grant":
				_, _, err = m.Modify(context.Background(), a.ID, a.RegistrationEpoch, func(c *coreauth.Auth) error { c.Metadata["access_token"] = "synthetic-replacement"; return nil })
			case "new-registration":
				_, err = m.Register(context.Background(), syntheticXAIBillingAuth())
			}
			if err != nil {
				t.Fatal(err)
			}
			st, body, _, err := billingReader(t, h).TmxXAIBilling(context.Background(), a)
			if err == nil || st != 0 || len(body) != 0 || calls.Load() != 0 {
				t.Fatalf("unsafe request mode=%s status=%d calls=%d err=%v", mode, st, calls.Load(), err)
			}
		})
	}
}
func TestTmxXAIBillingDoesNotFollowRedirect(t *testing.T) {
	var calls atomic.Int32
	xaiBillingLoopback(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Redirect(w, r, "https://cli-chat-proxy.grok.com/v1/not-billing", 302)
	})
	m := coreauth.NewManager(nil, nil, nil)
	a, err := m.Register(context.Background(), syntheticXAIBillingAuth())
	if err != nil {
		t.Fatal(err)
	}
	st, _, _, err := billingReader(t, &Handler{authManager: m}).TmxXAIBilling(context.Background(), a)
	if err != nil || st != 302 || calls.Load() != 1 {
		t.Fatalf("redirect followed: status=%d calls=%d err=%v", st, calls.Load(), err)
	}
}
func TestTmxXAIBillingRejectsChangedAccountDuringRead(t *testing.T) {
	for _, mode := range []string{"rotation", "disable", "registration"} {
		t.Run(mode, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			xaiBillingLoopback(t, func(w http.ResponseWriter, r *http.Request) {
				close(entered)
				<-release
				fmt.Fprint(w, `{"config":{"creditUsagePercent":25}}`)
			})
			m := coreauth.NewManager(nil, nil, nil)
			a, err := m.Register(context.Background(), syntheticXAIBillingAuth())
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			reader := billingReader(t, &Handler{authManager: m})
			go func() {
				st, body, _, err := reader.TmxXAIBilling(context.Background(), a)
				if err == nil || st != 0 || len(body) != 0 {
					done <- fmt.Errorf("stale result accepted: status=%d", st)
					return
				}
				done <- nil
			}()
			<-entered
			if mode == "registration" {
				_, err = m.Register(context.Background(), syntheticXAIBillingAuth())
			} else {
				_, _, err = m.Modify(context.Background(), a.ID, a.RegistrationEpoch, func(c *coreauth.Auth) error {
					if mode == "rotation" {
						c.Metadata["access_token"] = "synthetic-rotated"
					} else {
						c.Disabled = true
					}
					return nil
				})
			}
			close(release)
			if err != nil {
				t.Fatal(err)
			}
			if err = <-done; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTmxXAIBillingKeepsPerAccountProxyAndSanitizesErrors(t *testing.T) {
	var calls [2]atomic.Int32
	proxies := make([]*httptest.Server, 2)
	for i := range proxies {
		i := i
		proxies[i] = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls[i].Add(1)
			if r.Method != "CONNECT" || r.Host != "cli-chat-proxy.grok.com:443" {
				t.Error("unexpected proxy destination")
			}
			w.WriteHeader(503)
			fmt.Fprint(w, "synthetic-private-proxy-detail")
		}))
		defer proxies[i].Close()
	}
	m := coreauth.NewManager(nil, nil, nil)
	for i := range proxies {
		auth := syntheticXAIBillingAuth()
		auth.ID = fmt.Sprintf("proxy-%d", i)
		auth.ProxyURL = proxies[i].URL
		a, err := m.Register(context.Background(), auth)
		if err != nil {
			t.Fatal(err)
		}
		st, b, _, err := billingReader(t, &Handler{authManager: m}).TmxXAIBilling(context.Background(), a)
		if err == nil || strings.Contains(err.Error(), "private") || strings.Contains(err.Error(), proxies[i].URL) || st != 0 || len(b) != 0 {
			t.Fatal("transport failure exposed private details")
		}
		if calls[i].Load() != 1 {
			t.Fatal("retry or missing proxy request", calls[i].Load())
		}
	}
	if calls[0].Load() != 1 || calls[1].Load() != 1 {
		t.Fatal("account proxy isolation lost")
	}
}

func TestTmxXAIBillingReadsNormalLifecycleRotation(t *testing.T) {
	var calls atomic.Int32
	xaiBillingLoopback(t, func(w http.ResponseWriter, r *http.Request) {
		n := calls.Add(1)
		token, subject := "synthetic-current", "synthetic-user"
		if n == 2 {
			token, subject = "synthetic-lifecycle-token", "synthetic-lifecycle-sub"
		}
		if r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("x-userid") != subject {
			t.Error("token and subject do not match current lifecycle snapshot")
		}
		fmt.Fprint(w, `{"config":{"creditUsagePercent":25}}`)
	})
	m := coreauth.NewManager(nil, nil, nil)
	a, err := m.Register(context.Background(), syntheticXAIBillingAuth())
	if err != nil {
		t.Fatal(err)
	}
	reader := billingReader(t, &Handler{authManager: m})
	st, _, _, err := reader.TmxXAIBilling(context.Background(), a)
	if err != nil || st != 200 {
		t.Fatal("initial current read failed", err)
	}
	current, _, err := m.Modify(context.Background(), a.ID, a.RegistrationEpoch, func(c *coreauth.Auth) error {
		c.Metadata["access_token"] = "synthetic-lifecycle-token"
		c.Metadata["sub"] = "synthetic-lifecycle-sub"
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err = reader.TmxXAIBilling(context.Background(), a); err == nil || calls.Load() != 1 {
		t.Fatal("stale snapshot sent after lifecycle rotation")
	}
	st, _, sent, err := reader.TmxXAIBilling(context.Background(), current)
	if err != nil || st != 200 || sent == nil || sent.CredentialVersion != current.CredentialVersion || calls.Load() != 2 {
		t.Fatal("current lifecycle token not adopted", err)
	}
}
