package executor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/gorilla/websocket"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

type auditExecutor interface {
	Execute(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (cliproxyexecutor.Response, error)
	ExecuteStream(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error)
}

type auditReadError struct{}

func (auditReadError) Read([]byte) (int, error) { return 0, errors.New("audit stream read failed") }

func auditCapture(t *testing.T) (context.Context, *multiProviderUsageCapture) {
	t.Helper()
	capture := &multiProviderUsageCapture{alias: t.Name(), records: make(chan coreusage.Record, 4)}
	coreusage.RegisterNamedPlugin(t.Name(), capture)
	t.Cleanup(func() { coreusage.RegisterNamedPlugin(t.Name(), multiProviderNoopUsagePlugin{}) })
	return coreusage.WithRequestedModelAlias(context.Background(), t.Name()), capture
}

func TestAuditStreamFailurePreservesObservedUsage(t *testing.T) {
	for _, tc := range []struct {
		name, stream   string
		format         sdktranslator.Format
		scannerFailure bool
	}{
		{"openai error frame", "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\nevent: error\ndata: {\"error\":{\"message\":\"failed\"}}\n\n", sdktranslator.FormatOpenAI, false},
		{"openai scanner failure", "data: {\"usage\":{\"prompt_tokens\":10,\"completion_tokens\":5,\"total_tokens\":15}}\n\n", sdktranslator.FormatOpenAI, true},
		{"openai multiline error usage", "event: error\ndata: {\"usage\":{\"prompt_tokens\":10,\ndata: \"completion_tokens\":5,\"total_tokens\":15},\"error\":{\"message\":\"failed\"}}\n\n", sdktranslator.FormatOpenAI, false},
		{"gemini scanner failure", "data: {\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":5,\"totalTokenCount\":15}}\n\n", sdktranslator.FormatGemini, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, capture := auditCapture(t)
			ctx = context.WithValue(ctx, "cliproxy.roundtripper", openAICompatTTFTRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				var reader io.Reader = strings.NewReader(tc.stream)
				if tc.scannerFailure {
					reader = io.MultiReader(reader, auditReadError{})
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Body: io.NopCloser(reader), Request: req}, nil
			}))
			auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": "http://example.invalid/v1", "api_key": "test"}}
			var executor auditExecutor
			payload := []byte(`{"model":"audit-model","messages":[{"role":"user","content":"hi"}]}`)
			if tc.format == sdktranslator.FormatGemini {
				executor = NewGeminiExecutor(&config.Config{})
				payload = []byte(`{"contents":[{"role":"user","parts":[{"text":"hi"}]}]}`)
			} else {
				executor = NewOpenAICompatExecutor("openai-compatibility", &config.Config{})
			}
			result, errExecute := executor.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "audit-model", Payload: payload}, cliproxyexecutor.Options{SourceFormat: tc.format, Stream: true})
			if errExecute != nil {
				t.Fatal(errExecute)
			}
			failed := false
			for chunk := range result.Chunks {
				failed = failed || chunk.Err != nil
			}
			record := capture.await(t)
			if !failed || !record.Failed || record.Fail.Body == "" || record.Detail.InputTokens != 10 || record.Detail.OutputTokens != 5 || record.Detail.TotalTokens != 15 {
				t.Fatalf("failed=%t record=%+v", failed, record)
			}
		})
	}
}

func TestAuditCodexTerminalFailurePreservesUsage(t *testing.T) {
	for _, mode := range []string{"http sync", "http stream", "http buffered", "ws sync", "ws stream", "ws buffered"} {
		for _, terminal := range []string{"response.incomplete", "response.failed", "response.failed after usage"} {
			t.Run(mode+"/"+terminal, func(t *testing.T) {
				ctx, capture := auditCapture(t)
				events := []string{`{"type":"` + terminal + `","response":{"status":"incomplete","output":[],"error":{"message":"failed"},"usage":{"input_tokens":10,"output_tokens":0,"total_tokens":10,"input_tokens_details":{"cached_tokens":4}}}}`}
				if terminal == "response.failed after usage" {
					events = []string{`{"type":"response.in_progress","response":{"usage":{"input_tokens":10,"output_tokens":0,"total_tokens":10,"input_tokens_details":{"cached_tokens":4}}}}`, `{"type":"response.failed","response":{"error":{"message":"failed"}}}`}
				}
				var server *httptest.Server
				cfg := codexBufferingConfig(strings.Contains(mode, "buffered"))
				var executor auditExecutor = NewCodexExecutor(cfg)
				if strings.HasPrefix(mode, "ws") {
					upgrader := websocket.Upgrader{}
					server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						conn, errUpgrade := upgrader.Upgrade(w, r, nil)
						if errUpgrade != nil {
							t.Error(errUpgrade)
							return
						}
						defer func() { _ = conn.Close() }()
						if _, _, errRead := conn.ReadMessage(); errRead != nil {
							return
						}
						for _, event := range events {
							if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(event)); errWrite != nil {
								return
							}
						}
					}))
					exec := NewCodexWebsocketsExecutor(cfg)
					exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
					executor = exec
				} else {
					server = codexSSEServer(events...)
				}
				t.Cleanup(server.Close)
				req, opts := codexTestRequest()
				failed := false
				if strings.Contains(mode, "sync") {
					_, errExecute := executor.Execute(ctx, codexTestAuth(server.URL), req, opts)
					failed = errExecute != nil
				} else {
					result, errExecute := executor.ExecuteStream(ctx, codexTestAuth(server.URL), req, opts)
					failed = errExecute != nil
					if result != nil {
						for chunk := range result.Chunks {
							failed = failed || chunk.Err != nil
						}
					}
				}
				record := capture.await(t)
				if !failed || !record.Failed || record.Detail.InputTokens != 10 || record.Detail.CacheReadTokens != 4 || record.Detail.TotalTokens != 10 {
					t.Fatalf("failed=%t record=%+v", failed, record)
				}
			})
		}
	}
}

func TestAuditUsageModelMatchesFinalPayload(t *testing.T) {
	for _, provider := range []string{"openai", "kimi"} {
		for _, stream := range []bool{false, true} {
			name := provider + "/sync"
			if stream {
				name = provider + "/stream"
			}
			t.Run(name, func(t *testing.T) {
				ctx, capture := auditCapture(t)
				ctx = context.WithValue(ctx, "cliproxy.roundtripper", openAICompatTTFTRoundTripFunc(func(req *http.Request) (*http.Response, error) {
					body, errRead := io.ReadAll(req.Body)
					if errRead != nil {
						t.Error(errRead)
					}
					if model := gjson.GetBytes(body, "model").String(); model != "actual-upstream" {
						t.Errorf("sent model=%s", model)
					}
					response := `{"model":"actual-upstream","choices":[{"message":{"role":"assistant","content":"ok"}}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`
					if stream {
						response = "data: " + response + "\n\ndata: [DONE]\n\n"
					}
					return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Body: io.NopCloser(strings.NewReader(response)), Request: req}, nil
				}))
				cfg := &config.Config{Payload: config.PayloadConfig{Override: []config.PayloadRule{{Models: []config.PayloadModelRule{{Name: "*"}}, Params: map[string]any{"model": "actual-upstream"}}}}}
				var executor auditExecutor = NewOpenAICompatExecutor("openai-compatibility", cfg)
				if provider == "kimi" {
					executor = NewKimiExecutor(cfg)
				}
				auth := &cliproxyauth.Auth{Attributes: map[string]string{"base_url": "http://example.invalid/v1", "api_key": "test"}, Metadata: map[string]any{"access_token": "test"}}
				req := cliproxyexecutor.Request{Model: "kimi-k2.8", Payload: []byte(`{"model":"kimi-k2.8","messages":[{"role":"user","content":"hi"}]}`)}
				opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAI, Stream: stream}
				if stream {
					result, errExecute := executor.ExecuteStream(ctx, auth, req, opts)
					if errExecute != nil {
						t.Fatal(errExecute)
					}
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							t.Fatal(chunk.Err)
						}
					}
				} else if _, errExecute := executor.Execute(ctx, auth, req, opts); errExecute != nil {
					t.Fatal(errExecute)
				}
				record := capture.await(t)
				if record.Model != "actual-upstream" || record.Alias != t.Name() || record.ResponseModel != "actual-upstream" {
					t.Fatalf("record=%+v", record)
				}
			})
		}
	}
}

func TestAuditProvisionalClaudeUsagePublishedWithCancellation(t *testing.T) {
	ctx, capture := auditCapture(t)
	reporter := helps.NewUsageReporter(ctx, "claude", "claude-opus-5", nil)
	var buffer helps.StreamUsageBuffer
	buffer.ObserveClaudeStream([]byte(`data: {"type":"message_start","message":{"usage":{"input_tokens":10,"cache_read_input_tokens":40,"output_tokens":8}}}`))
	buffer.PublishFailure(ctx, reporter, context.Canceled)
	// The deferred success publisher must not replace the failed attempt.
	buffer.Publish(ctx, reporter)
	record := capture.await(t)
	if !record.Failed || record.Detail.TokenBreakdown.Quality != coreusage.TokenAccountingQualityUnclassified ||
		record.Detail.InputTokens != 10 || record.Detail.CacheReadTokens != 40 || record.Detail.OutputTokens != 8 || record.Detail.TotalTokens != 58 {
		t.Fatalf("record=%+v", record)
	}
}

func TestAuditEmptyUsageRecordsAreUnclassified(t *testing.T) {
	for _, mode := range []string{"publish", "ensure", "failure"} {
		t.Run(mode, func(t *testing.T) {
			ctx, capture := auditCapture(t)
			reporter := helps.NewUsageReporter(ctx, "claude", "claude-opus-5", nil)
			switch mode {
			case "publish":
				reporter.Publish(ctx, coreusage.Detail{})
			case "ensure":
				reporter.EnsurePublished(ctx)
			case "failure":
				reporter.PublishFailure(ctx, context.Canceled)
			}
			record := capture.await(t)
			if !record.Detail.TokenBreakdown.Valid() || record.Detail.TokenBreakdown.Quality != coreusage.TokenAccountingQualityUnclassified {
				t.Fatalf("empty record=%+v", record)
			}
		})
	}
}
