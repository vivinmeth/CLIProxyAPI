package management

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	sdkAuth "github.com/router-for-me/CLIProxyAPI/v8/sdk/auth"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestTMXManagementRefusalClearRequiresDisabledFalse(t *testing.T) {
	for _, kind := range []string{"status", "fields", "plugin-source"} {
		t.Run(kind, func(t *testing.T) {
			ctx := context.Background()
			dir := t.TempDir()
			path := filepath.Join(dir, "refusal.json")
			if err := os.WriteFile(path, []byte(`{"type":"antigravity","disabled":true}`), 0600); err != nil {
				t.Fatal(err)
			}
			store := sdkAuth.NewFileTokenStore()
			store.SetBaseDir(dir)
			m := coreauth.NewManager(store, nil, nil)
			a := &coreauth.Auth{ID: "refusal.json", FileName: "refusal.json", Provider: "antigravity", Status: coreauth.StatusActive, Metadata: map[string]any{"type": "antigravity", "disabled": false}, Attributes: map[string]string{"path": path}}
			if kind == "plugin-source" {
				coreauth.MarkPluginVirtualAuth(a, path, 0)
			}
			if _, err := m.Register(ctx, a); err != nil {
				t.Fatal(err)
			}
			m.MarkResult(ctx, coreauth.Result{AuthID: a.ID, Provider: a.Provider, Model: "model", Error: &coreauth.Error{HTTPStatus: 403, Code: refusals.GoogleValidation, Message: refusals.StatusMessage(refusals.GoogleValidation)}})
			if kind != "plugin-source" {
				fresh := coreauth.NewManager(store, nil, nil)
				if err := fresh.Load(ctx); err != nil {
					t.Fatal(err)
				}
				loaded := fresh.List()
				if len(loaded) != 1 || !loaded[0].Disabled || loaded[0].Metadata["tmx_account_refusal"] != refusals.GoogleValidation {
					t.Fatal("file store lost refusal on restart")
				}
			}
			h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: dir}, m)
			patch := func(body string, fields bool) {
				t.Helper()
				rec := httptest.NewRecorder()
				c, _ := gin.CreateTestContext(rec)
				c.Request = httptest.NewRequest(http.MethodPatch, "/v8/management/auth-files", strings.NewReader(body))
				c.Request.Header.Set("Content-Type", "application/json")
				if fields {
					h.PatchAuthFileFields(c)
				} else {
					h.PatchAuthFileStatus(c)
				}
				if rec.Code != http.StatusOK {
					t.Fatalf("PATCH status=%d body=%s", rec.Code, rec.Body.String())
				}
			}
			if kind == "fields" {
				patch(`{"name":"refusal.json","note":"operator note"}`, true)
				got, _ := m.GetByID(a.ID)
				if !got.Disabled || got.Metadata["tmx_account_refusal"] != refusals.GoogleValidation || got.Metadata["note"] != "operator note" {
					t.Fatal("note patch cleared refusal or lost note")
				}
			}
			patch(`{"name":"refusal.json","disabled":false}`, kind == "fields")
			got, _ := m.GetByID(a.ID)
			if got.Disabled || got.Status != coreauth.StatusActive || got.LastError != nil || got.Metadata["tmx_account_refusal"] != nil || got.Metadata["disabled"] != false || !got.NextRefreshAfter.IsZero() {
				t.Fatalf("explicit re-enable failed: %+v", got)
			}
			for _, state := range got.ModelStates {
				if state.LastError != nil || state.Status == coreauth.StatusDisabled {
					t.Fatalf("refused model retained: %+v", state)
				}
			}
			restarted := coreauth.NewManager(store, nil, nil)
			if err := restarted.Load(ctx); err != nil {
				t.Fatal(err)
			}
			if len(restarted.List()) != 1 {
				t.Fatal("file store did not reload the fixture")
			}
			for _, loaded := range restarted.List() {
				if loaded.Disabled || loaded.Metadata["tmx_account_refusal"] != nil {
					t.Fatal("clear was not retained by the file store")
				}
			}
		})
	}
}
