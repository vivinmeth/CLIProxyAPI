package muse

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

func TestIsClientAndMode(t *testing.T) {
	cases := []struct {
		ua, clientID string
		is           bool
		mode         string
	}{
		{"muse-build/1.4.11 (non-interactive; macos-aarch64; build abc)", "tbh:exec", true, "exec"},
		{"muse-build/1.4.11 (interactive; linux-x86_64; build abc)", "", true, "tui"},
		{"muse-build/1.4.11 (serve; linux-x86_64; build abc)", "", true, "serve"},
		{"muse-code/2.0", "", true, ""},
		{"node", "tbh:serve", true, "serve"},
		{"codex_exec/0.163.0", "", false, ""},
		{"", "", false, ""},
	}
	for _, c := range cases {
		if got := IsClient(c.ua, c.clientID); got != c.is {
			t.Errorf("IsClient(%q, %q) = %v, want %v", c.ua, c.clientID, got, c.is)
		}
		if got := ClientMode(c.ua, c.clientID); c.is && got != c.mode {
			t.Errorf("ClientMode(%q, %q) = %q, want %q", c.ua, c.clientID, got, c.mode)
		}
	}
}

func TestRegistryCatalogShape(t *testing.T) {
	body := RegistryCatalog(registry.GetMetaModels())
	if gjson.GetBytes(body, "object").String() != "list" {
		t.Fatalf("object = %s", body)
	}
	rows := gjson.GetBytes(body, "data").Array()
	if len(rows) == 0 {
		t.Fatalf("no rows: %s", body)
	}
	defaults := 0
	for _, r := range rows {
		id := r.Get("id").String()
		if len(id) > len("-contributor") && id[len(id)-len("-contributor"):] == "-contributor" {
			t.Errorf("contributor row %s listed", id)
		}
		m := r.Get(`metadata.muse-code`)
		// Muse hides rows without a name, both limits and a release date (no default otherwise).
		for _, f := range []string{"name", "limit.context", "limit.output", "release_date", "variants"} {
			if !m.Get(f).Exists() {
				t.Errorf("%s: metadata.muse-code.%s missing", id, f)
			}
		}
		if m.Get("limit.context").Int() <= 0 || m.Get("limit.output").Int() <= 0 {
			t.Errorf("%s: limits not positive: %s", id, m.Get("limit").Raw)
		}
		if m.Get("is_default").Bool() {
			defaults++
		}
	}
	if defaults != 1 {
		t.Errorf("defaults = %d, want 1", defaults)
	}
	if !gjson.GetBytes(body, `data.#(id=="muse-wonder-full-1.0")`).Exists() {
		t.Errorf("muse-wonder-full-1.0 missing")
	}
}

func TestCatalogURL(t *testing.T) {
	cases := map[string]string{
		"":                           "https://api.meta.ai/muse-code/models",
		"https://api.meta.ai/v1":     "https://api.meta.ai/muse-code/models",
		"http://127.0.0.1:9000/v1/x": "http://127.0.0.1:9000/muse-code/models",
	}
	for base, want := range cases {
		a := &auth.Auth{Attributes: map[string]string{"base_url": base}}
		if got := catalogURL(a); got != want {
			t.Errorf("catalogURL(%q) = %q, want %q", base, got, want)
		}
	}
}

func TestHandlerFallsBackWithoutMetaAuth(t *testing.T) {
	rec := httptest.NewRecorder()
	Handler(auth.NewManager(nil, nil, nil))(rec, httptest.NewRequest(http.MethodGet, CatalogPath, nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if n := len(gjson.GetBytes(rec.Body.Bytes(), "data").Array()); n == 0 {
		t.Fatalf("empty catalog: %s", rec.Body.String())
	}
}
