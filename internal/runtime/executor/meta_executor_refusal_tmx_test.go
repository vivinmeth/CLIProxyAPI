package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/refusals"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

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
