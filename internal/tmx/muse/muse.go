// Package muse serves Meta's Muse Code CLI (and `muse serve`, which T3 runs) through the proxy.
//
// Muse points at the proxy with settings.json `endpoint_transport.base_url`. Before every session
// it fetches GET /muse-code/models from that URL's origin and refuses to start without a catalog,
// then sends inference to POST /v1/responses. This package answers the catalog: Meta's own when a
// Meta credential is logged in, otherwise one built from the registry's meta models. Contributor
// models are refused and unlisted (contributor.go).
package muse

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// Harness is the request-log label for Muse traffic.
const Harness = "muse-code"

// CatalogPath is where Muse looks for its model catalog, at the endpoint's origin.
const CatalogPath = "/muse-code/models"

// upstreamTimeout bounds the upstream catalog fetch so a slow Meta answer falls back to the
// registry catalog instead of holding Muse's startup.
const upstreamTimeout = 10 * time.Second

// IsClient reports whether a request came from Muse: its user agent is "muse-build/…" (or
// "muse-code/…"), and its x-client-id names the surface ("tbh:tui", "tbh:exec", "tbh:serve", …).
func IsClient(userAgent, clientID string) bool {
	ua := strings.ToLower(strings.TrimSpace(userAgent))
	if strings.HasPrefix(ua, "muse-build/") || strings.HasPrefix(ua, "muse-code/") {
		return true
	}
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(clientID)), "tbh:")
}

// IsClientHeaders is IsClient over request headers.
func IsClientHeaders(h http.Header) bool {
	if h == nil {
		return false
	}
	return IsClient(h.Get("User-Agent"), h.Get("X-Client-Id"))
}

// ClientMode names how Muse was run: the x-client-id surface ("tbh:exec" → "exec") when sent,
// else the user agent's "(non-interactive; …)" / "(interactive; …)" / "(serve; …)" tag.
func ClientMode(userAgent, clientID string) string {
	if surface, ok := strings.CutPrefix(strings.ToLower(strings.TrimSpace(clientID)), "tbh:"); ok && surface != "" {
		return surface
	}
	ua := strings.ToLower(userAgent)
	switch {
	case strings.Contains(ua, "(non-interactive"):
		return "exec"
	case strings.Contains(ua, "(interactive"):
		return "tui"
	case strings.Contains(ua, "(serve"):
		return "serve"
	}
	return ""
}

// Handler answers GET /muse-code/models. Callers mount it behind client-key auth.
func Handler(mgr *auth.Manager) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		body := upstreamCatalog(r.Context(), mgr)
		if body == nil {
			body = RegistryCatalog(registry.GetMetaModels())
		} else {
			body = filterCatalog(body) // Contributor mode is off on this proxy (contributor.go)
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "private, no-store")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}
}

// upstreamCatalog forwards Meta's own catalog through the first usable Meta credential, or
// returns nil when there is none or the call fails.
func upstreamCatalog(ctx context.Context, mgr *auth.Manager) []byte {
	if mgr == nil {
		return nil
	}
	for _, a := range mgr.List() {
		if a == nil || !strings.EqualFold(a.Provider, "meta") || a.Disabled || a.Status == auth.StatusDisabled {
			continue
		}
		body, err := fetchCatalog(ctx, mgr, a)
		if err != nil {
			log.WithField("auth_id", a.ID).Debugf("muse: upstream catalog: %v", err)
			continue
		}
		return body
	}
	return nil
}

func fetchCatalog(ctx context.Context, mgr *auth.Manager, a *auth.Auth) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	req, err := mgr.NewHttpRequest(ctx, a, http.MethodGet, catalogURL(a), nil, http.Header{"Accept": {"application/json"}})
	if err != nil {
		return nil, err
	}
	resp, err := mgr.HttpRequest(ctx, a, req)
	if err != nil {
		return nil, err
	}
	defer func() {
		if errClose := resp.Body.Close(); errClose != nil {
			log.Debugf("muse: close catalog body: %v", errClose)
		}
	}()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("upstream status %d", resp.StatusCode)
	}
	if !gjson.ValidBytes(body) || !gjson.GetBytes(body, "data").IsArray() {
		return nil, fmt.Errorf("upstream catalog is not a model list")
	}
	return body, nil
}

// catalogURL is the catalog at the origin of the credential's API base URL
// (https://api.meta.ai/v1 → https://api.meta.ai/muse-code/models).
func catalogURL(a *auth.Auth) string {
	base := "https://api.meta.ai"
	if a != nil {
		if b := strings.TrimSpace(a.Attributes["base_url"]); b != "" {
			base = b
		} else if b, ok := a.Metadata["base_url"].(string); ok && strings.TrimSpace(b) != "" {
			base = strings.TrimSpace(b)
		}
	}
	if scheme, rest, ok := strings.Cut(base, "://"); ok {
		host, _, _ := strings.Cut(rest, "/")
		return scheme + "://" + host + CatalogPath
	}
	return "https://api.meta.ai" + CatalogPath
}

// RegistryCatalog builds the catalog in the shape Muse 1.4.11 reads: an OpenAI model list whose
// rows carry metadata["muse-code"] (models.dev-style). Verified against the CLI: a row without
// that object is hidden, `limit.context` / `limit.output` become the context and output limits,
// `name` the label, `variants` keys the selectable efforts, and `release_date` is what lets Muse
// resolve a default model under `muse serve`. Contributor variants are left out; Muse picks them
// itself in contributor mode.
func RegistryCatalog(models []*registry.ModelInfo) []byte {
	type row struct {
		ID      string         `json:"id"`
		Object  string         `json:"object"`
		Created int64          `json:"created"`
		OwnedBy string         `json:"owned_by"`
		Meta    map[string]any `json:"metadata"`
	}
	rows := make([]row, 0, len(models))
	for _, m := range models {
		if m == nil || m.ID == "" || strings.HasSuffix(m.ID, "-contributor") {
			continue
		}
		name := m.DisplayName
		if name == "" {
			name = m.ID
		}
		input := []string{}
		for _, mod := range m.SupportedInputModalities {
			input = append(input, strings.ToLower(mod))
		}
		if len(input) == 0 {
			input = []string{"text"}
		}
		variants := map[string]any{}
		if m.Thinking != nil {
			for _, level := range m.Thinking.Levels {
				variants[level] = map[string]any{}
			}
		}
		created := m.Created
		if created <= 0 {
			created = time.Now().Unix()
		}
		meta := map[string]any{
			"name":         name,
			"family":       "muse",
			"description":  m.Description,
			"release_date": time.Unix(created, 0).UTC().Format("2006-01-02"),
			"is_default":   len(rows) == 0,
			"is_hidden":    false,
			"attachment":   len(input) > 1,
			"reasoning":    len(variants) > 0,
			"tool_call":    true,
			"temperature":  false,
			"modalities":   map[string]any{"input": input, "output": []string{"text"}},
			"limit":        map[string]any{"context": m.ContextLength, "output": m.MaxCompletionTokens},
			"variants":     variants,
		}
		rows = append(rows, row{ID: m.ID, Object: "model", Created: created, OwnedBy: "meta", Meta: map[string]any{"muse-code": meta}})
	}
	out, err := json.Marshal(map[string]any{"object": "list", "data": rows})
	if err != nil {
		return []byte(`{"object":"list","data":[]}`)
	}
	return out
}
