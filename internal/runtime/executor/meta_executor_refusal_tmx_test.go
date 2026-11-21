package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	log "github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

const tmxValidationResponse = `{"error":{"code":403,"status":"PERMISSION_DENIED","message":"Verify at https://example.invalid/verification/session-private?token=synthetic-secret","details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"VALIDATION_REQUIRED","metadata":{"verification_url":"https://example.invalid/verification/session-private?token=synthetic-secret"}}]}}`

func TestTMXGoogleRefusalStatusClassification(t *testing.T) {
	for _, tc := range []struct {
		name, body, want string
		status           int
	}{
		{"validation", tmxValidationResponse, refusals.GoogleValidation, 403},
		{"legacy ToS", antigravityTOSBody, "tos_violation", 403},
		{"malformed validation", tmxValidationResponse[:len(tmxValidationResponse)-1], "", 403},
		{"malformed ToS", antigravityTOSBody[:len(antigravityTOSBody)-1], "", 403},
		{"wrong ToS HTTP status", antigravityTOSBody, "", 500},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := newAntigravityStatusErr(tc.status, []byte(tc.body))
			if err.ErrorCode() != tc.want {
				t.Fatalf("ErrorCode=%q, want %q", err.ErrorCode(), tc.want)
			}
		})
	}
}

func TestTMXGoogleValidationExecuteLogsAreBounded(t *testing.T) {
	originalHooks := log.StandardLogger().ReplaceHooks(make(log.LevelHooks))
	originalLevel := log.GetLevel()
	log.SetLevel(log.DebugLevel)
	t.Cleanup(func() { log.StandardLogger().ReplaceHooks(originalHooks); log.SetLevel(originalLevel) })
	hook := logtest.NewGlobal()
	useFakeAntigravityKVClient(t, newFakeAntigravityKVClient(), false, nil)
	for _, tc := range []struct {
		name, body string
		status     int
		validation bool
	}{
		{"validation", tmxValidationResponse, 403, true},
		{"ordinary403", `{"error":{"message":"ordinary permission marker"}}`, 403, false},
		{"wrong HTTP status", tmxValidationResponse, 500, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook.Reset()
			calls := 0
			transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				return &http.Response{StatusCode: tc.status, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(tc.body)), Request: req}, nil
			})
			ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(transport))
			m := cliproxyauth.NewManager(nil, nil, nil)
			a, err := m.Register(ctx, &cliproxyauth.Auth{ID: "validation", Provider: "antigravity", Status: cliproxyauth.StatusActive, Attributes: map[string]string{"base_url": "http://example.invalid"}, Metadata: map[string]any{"access_token": "synthetic-access", "project_id": "synthetic-project", "expired": time.Now().Add(time.Hour).Format(time.RFC3339)}})
			if err != nil {
				t.Fatal(err)
			}
			exec := NewAntigravityExecutor(&config.Config{})
			_, err = exec.Execute(ctx, a, cliproxyexecutor.Request{Model: "gemini-2.5-flash", Payload: []byte(`{"request":{"contents":[{"role":"user","parts":[{"text":"hello"}]}]}}`)}, cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatAntigravity})
			if calls != 1 || err == nil {
				t.Fatalf("calls=%d error=%v", calls, err)
			}
			resultErr := cliproxyauth.ResultErrorFromError(err)
			m.MarkResult(ctx, cliproxyauth.Result{AuthID: a.ID, Provider: a.Provider, Model: "gemini-2.5-flash", Error: resultErr})
			got, _ := m.GetByID(a.ID)
			if tc.validation {
				if !got.Disabled || got.StatusMessage != refusals.StatusMessage(refusals.GoogleValidation) || resultErr.Code != refusals.GoogleValidation || resultErr.Message != got.StatusMessage || len(resultErr.Metadata) != 0 {
					t.Errorf("unsafe validation result: %+v", resultErr)
				}
				if len(got.Metadata) != len(a.Metadata)+1 || got.Metadata["tmx_account_refusal"] != refusals.GoogleValidation {
					t.Errorf("unsafe terminal metadata: %+v", got.Metadata)
				}
			}
			debugSeen := false
			for _, entry := range hook.AllEntries() {
				message := fmt.Sprintf("%s %+v", entry.Message, entry.Data)
				if tc.validation {
					for _, forbidden := range []string{"session-private", "synthetic-secret", "verification_url", "https://example.invalid/verification"} {
						if strings.Contains(message, forbidden) {
							t.Fatalf("validation URL leaked in log: %s", message)
						}
					}
				}
				if strings.HasPrefix(entry.Message, "antigravity executor: upstream error status:") {
					debugSeen = true
					want := helps.SummarizeErrorBody("application/json", []byte(tc.body))
					if tc.validation {
						want = refusals.StatusMessage(refusals.GoogleValidation)
					}
					if entry.Message != fmt.Sprintf("antigravity executor: upstream error status: %d, body: %s", tc.status, want) {
						t.Fatalf("debug summary changed: %s", entry.Message)
					}
				}
			}
			if !debugSeen {
				t.Fatal("Execute did not exercise debug summary")
			}
		})
	}
}

func TestTMXMetaRefusalUsesFinalRequestTools(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, body := range []string{
			`{"error":{"code":"client_attestation_denied"}}`,
			`{"error":{"message":"Muse Code subscriptions cover requests from the Muse Code client only."}}`,
		} {
			for _, tc := range []struct {
				name          string
				tools, remove bool
			}{
				{name: "no tools"},
				{name: "tools", tools: true},
				{name: "filtered tools", tools: true, remove: true},
			} {
				t.Run(fmt.Sprintf("stream=%v/%s/%s", stream, body, tc.name), func(t *testing.T) {
					cfg := &config.Config{}
					if tc.remove {
						cfg.Payload.Filter = []config.PayloadFilterRule{{Models: []config.PayloadModelRule{{Name: "muse-*"}}, Params: []string{"tools"}}}
					}
					transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
						request, err := io.ReadAll(req.Body)
						if err != nil {
							t.Fatal(err)
						}
						if got := len(ojson.GetBytes(request, "tools").Array()) > 0; got != (tc.tools && !tc.remove) {
							t.Fatalf("unexpected final tools: %s", request)
						}
						return &http.Response{StatusCode: 403, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
					})
					ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", http.RoundTripper(transport))
					m := cliproxyauth.NewManager(nil, nil, nil)
					for _, id := range []string{"refused", "sibling"} {
						if _, err := m.Register(ctx, &cliproxyauth.Auth{ID: id, Provider: "meta", Status: cliproxyauth.StatusActive, Attributes: map[string]string{"api_key": "test-key", "base_url": "http://example.invalid"}}); err != nil {
							t.Fatal(err)
						}
					}
					a, _ := m.GetByID("refused")
					exec := NewMetaExecutor(cfg)
					payload := []byte(`{"input":"hello"}`)
					if tc.tools {
						payload = []byte(`{"input":"hello","tools":[{"type":"function","name":"lookup","parameters":{"type":"object"}}]}`)
					}
					req := cliproxyexecutor.Request{Model: "muse-wonder-full-1.0", Payload: payload}
					opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("codex"), Stream: stream}
					var err error
					if stream {
						_, err = exec.ExecuteStream(ctx, a, req, opts)
					} else {
						_, err = exec.Execute(ctx, a, req, opts)
					}
					if err == nil {
						t.Fatal("expected refusal")
					}
					resultErr := cliproxyauth.ResultErrorFromError(err)
					if resultErr.HTTPStatus != 403 || resultErr.Message != body {
						t.Fatalf("lost original error: %+v", resultErr)
					}
					toolRefusal := tc.tools && !tc.remove
					if toolRefusal && resultErr.Code != refusals.AttestationRefused {
						t.Fatalf("lost tool attestation context: %+v", resultErr)
					}
					m.MarkResult(ctx, cliproxyauth.Result{AuthID: a.ID, Provider: "meta", Model: req.Model, Error: resultErr})
					a, _ = m.GetByID(a.ID)
					if a.Disabled == toolRefusal {
						t.Fatalf("final tools=%v disabled=%v", toolRefusal, a.Disabled)
					}
					sibling, _ := m.GetByID("sibling")
					if sibling.Disabled || sibling.Unavailable || sibling.LastError != nil {
						t.Fatal("refusal affected sibling")
					}
				})
			}
		}
	}
}
