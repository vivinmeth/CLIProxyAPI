package handlers

import (
	"context"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v8/sdk/config"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

func TestGrokRequestsRefuseNonXAIWithoutExecution(t *testing.T) {
	for _, provider := range []string{"claude", "codex", "meta", "antigravity", "openai"} {
		t.Run(provider, func(t *testing.T) {
			model := "grok-looking-" + provider
			var calls atomic.Int32
			executor := &modelExecutionCaptureExecutor{provider: provider,
				execute: func(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
					calls.Add(1)
					return coreexecutor.Response{Payload: []byte(`{}`)}, nil
				},
				stream: func(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
					calls.Add(1)
					chunks := make(chan coreexecutor.StreamChunk, 1)
					chunks <- coreexecutor.StreamChunk{Payload: []byte(`data: {}`)}
					close(chunks)
					return &coreexecutor.StreamResult{Chunks: chunks}, nil
				},
			}
			h := newModelExecutionHandler(t, model, executor, &sdkconfig.SDKConfig{})
			for _, protocol := range []string{"openai", "openai-response"} {
				for _, suffix := range []string{"", "(high)"} {
					for _, helper := range []bool{false, true} {
						for _, stream := range []bool{false, true} {
							ctx := contextWithHeaders(http.Header{"User-Agent": {"grok-shell/0.2.119"}})
							var errMsg *interfaces.ErrorMessage
							if helper {
								req := ModelExecutionRequest{EntryProtocol: protocol, ExitProtocol: protocol, Model: model + suffix, Body: []byte(`{}`), Stream: stream}
								if stream {
									_, errMsg = h.ExecuteModelStream(ctx, req)
								} else {
									_, errMsg = h.ExecuteModel(ctx, req)
								}
							} else if stream {
								_, _, errs := h.ExecuteStreamWithAuthManager(ctx, protocol, model+suffix, []byte(`{}`), "")
								errMsg = <-errs
							} else {
								_, _, errMsg = h.ExecuteWithAuthManager(ctx, protocol, model+suffix, []byte(`{}`), "")
							}
							if errMsg == nil || errMsg.StatusCode != 400 || !strings.Contains(errMsg.Error.Error(), "non_xai_models_not_for_grok") {
								t.Fatalf("protocol=%s suffix=%s helper=%v stream=%v error=%+v", protocol, suffix, helper, stream, errMsg)
							}
						}
					}
				}
			}
			if calls.Load() != 0 {
				t.Fatalf("upstream calls=%d", calls.Load())
			}
		})
	}
}

func TestGrokPluginExecutorRefusedEvenForRegisteredXAI(t *testing.T) {
	r := registry.GetGlobalRegistry()
	r.RegisterClient("grok-plugin-xai", "xai", []*registry.ModelInfo{{ID: "grok-plugin-model"}})
	t.Cleanup(func() { r.UnregisterClient("grok-plugin-xai") })
	host := &handlerDirectExecutorRouteHost{}
	host.hasRouters = true
	host.route = func(context.Context, pluginapi.ModelRouteRequest) (pluginapi.ModelRouteResponse, bool) {
		return pluginapi.ModelRouteResponse{Handled: true, TargetKind: pluginapi.ModelRouteTargetExecutor, Target: "opaque-executor"}, true
	}
	h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	h.SetModelRouterHost(host)
	ctx := contextWithHeaders(http.Header{"User-Agent": {"grok-shell/0.2.119"}})
	for _, protocol := range []string{"openai", "openai-response"} {
		_, _, errMsg := h.ExecuteWithAuthManager(ctx, protocol, "grok-plugin-model", []byte(`{}`), "")
		if errMsg == nil || errMsg.StatusCode != 400 {
			t.Fatalf("error=%+v", errMsg)
		}
		_, _, errs := h.ExecuteStreamWithAuthManager(ctx, protocol, "grok-plugin-model", []byte(`{}`), "")
		if errMsg = <-errs; errMsg == nil || errMsg.StatusCode != 400 {
			t.Fatalf("stream error=%+v", errMsg)
		}
		_, _, errMsg = h.ExecuteCountWithAuthManager(ctx, protocol, "grok-plugin-model", []byte(`{}`), "")
		if errMsg == nil || errMsg.StatusCode != 400 {
			t.Fatalf("count error=%+v", errMsg)
		}
	}
	if host.lastPluginID != "" {
		t.Fatal("plugin executor was called")
	}
}

func TestGrokBoundaryPreservesOtherClientsAndValidXAI(t *testing.T) {
	for _, tc := range []struct {
		ua, protocol, provider string
		denied                 bool
	}{
		{"grok-shell/0.2.119", "openai", "xai", false},
		{"grok-shell/0.2.119", "openai-response", "xai", false},
		{"claude-cli/2.1.280", "claude", "claude", false},
		{"codex_cli_rs/0.130.0", "openai", "claude", true},
		{"curl/8", "openai-response", "claude", true},
		{"codex_cli_rs/0.130.0", "openai-response", "codex", false},
		{"codex_cli_rs/0.130.0", "openai-response", "meta", false},
		{"codex_cli_rs/0.130.0", "openai-response", "xai", false},
		{"antigravity/1.0", "openai", "antigravity", false},
		{"muse-code/1.4.11", "openai-response", "meta", false},
		{"curl/8", "openai", "claude", false},
	} {
		t.Run(tc.ua+"/"+tc.protocol+"/"+tc.provider, func(t *testing.T) {
			var calls atomic.Int32
			ex := &modelExecutionCaptureExecutor{provider: tc.provider,
				execute: func(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
					calls.Add(1)
					return coreexecutor.Response{Payload: []byte(`{}`)}, nil
				},
				stream: func(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (*coreexecutor.StreamResult, error) {
					calls.Add(1)
					chunks := make(chan coreexecutor.StreamChunk, 1)
					chunks <- coreexecutor.StreamChunk{Payload: []byte(`data: {}`)}
					close(chunks)
					return &coreexecutor.StreamResult{Chunks: chunks}, nil
				},
			}
			model := "valid-client-model"
			h := newModelExecutionHandler(t, model, ex, &sdkconfig.SDKConfig{})
			ctx := contextWithHeaders(http.Header{"User-Agent": {tc.ua}})
			_, _, errMsg := h.ExecuteWithAuthManager(ctx, tc.protocol, model, []byte(`{}`), "")
			if tc.denied {
				if errMsg == nil || !strings.Contains(errMsg.Error.Error(), "claude_models_not_for_codex") {
					t.Fatalf("existing refusal changed: %+v", errMsg)
				}
				if calls.Load() != 0 {
					t.Fatalf("upstream calls=%d", calls.Load())
				}
				return
			}
			if errMsg != nil {
				t.Fatalf("allowed request refused: %+v", errMsg)
			}
			data, _, errs := h.ExecuteStreamWithAuthManager(ctx, tc.protocol, model, []byte(`{}`), "")
			if data == nil {
				t.Fatalf("allowed stream refused: %+v", <-errs)
			}
			for range data {
			}
			for errMsg := range errs {
				if errMsg != nil {
					t.Fatalf("allowed stream refused: %+v", errMsg)
				}
			}
			if calls.Load() != 2 {
				t.Fatalf("upstream calls=%d, want 2", calls.Load())
			}
		})
	}
}

func TestGrokResolvedRoutesAndHelperHeaders(t *testing.T) {
	r := registry.GetGlobalRegistry()
	r.RegisterClient("grok-route-xai", "xai", []*registry.ModelInfo{{ID: "grok-alias"}, {ID: "route-pool"}, {ID: "native-target"}, {ID: "suffixed-fallback(high)"}})
	r.RegisterClient("grok-route-codex", "codex", []*registry.ModelInfo{{ID: "route-pool"}, {ID: "grok-looking-target"}, {ID: "suffixed-fallback(high)"}})
	t.Cleanup(func() { r.UnregisterClient("grok-route-xai"); r.UnregisterClient("grok-route-codex") })
	h := NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, nil)
	headers := http.Header{"User-Agent": {"grok-shell/0.2.119"}}
	ctx := contextWithHeaders(headers)
	for _, tc := range []struct {
		model   string
		route   modelRouteDecision
		opts    modelExecutionOptions
		allowed bool
	}{
		{model: "grok-alias", allowed: true},
		{model: "route-pool"},
		{model: "suffixed-fallback(high)"},
		{model: "grok-alias", route: modelRouteDecision{Provider: "codex", Model: "grok-looking-target"}},
		{model: "grok-alias", route: modelRouteDecision{Provider: "xai", Model: "grok-looking-target"}},
		{model: "grok-alias", route: modelRouteDecision{Provider: "xai", Model: "unknown-target"}},
		{model: "grok-alias", route: modelRouteDecision{Provider: "xai", Model: "native-target"}, allowed: true},
		{model: "grok-alias", opts: modelExecutionOptions{ForcedProvider: "meta"}},
	} {
		_, _, errMsg := h.tmxProvidersForExecution(ctx, "openai", tc.model, tc.model, false, tc.route, tc.opts)
		if tc.allowed {
			if errMsg != nil {
				t.Fatalf("allowed %+v: %+v", tc, errMsg)
			}
		} else if errMsg == nil || errMsg.StatusCode != 400 {
			t.Fatalf("denied %+v: %+v", tc, errMsg)
		}
	}
	// A plain helper context must still enforce an explicit Grok identity.
	for _, stream := range []bool{false, true} {
		req := ModelExecutionRequest{EntryProtocol: "openai", ExitProtocol: "openai", Model: "grok-looking-target", Headers: headers, Body: []byte(`{}`), Stream: stream}
		var errMsg *interfaces.ErrorMessage
		if stream {
			_, errMsg = h.ExecuteModelStream(context.Background(), req)
		} else {
			_, errMsg = h.ExecuteModel(context.Background(), req)
		}
		if errMsg == nil || errMsg.StatusCode != 400 || !strings.Contains(errMsg.Error.Error(), "non_xai_models_not_for_grok") {
			t.Fatalf("helper stream=%v: %+v", stream, errMsg)
		}
	}
	_, _, errMsg := h.ExecuteWithAuthManager(ctx, "openai", "unknown-model", []byte(`{}`), "")
	if errMsg == nil || errMsg.StatusCode != 400 || !strings.Contains(errMsg.Error.Error(), "model_not_found") {
		t.Fatalf("unknown model error changed: %+v", errMsg)
	}
}

func TestGrokAutoAndMixedAliasNeverDispatchNonXAI(t *testing.T) {
	for _, provider := range []string{"xai", "codex"} {
		t.Run(provider, func(t *testing.T) {
			var calls atomic.Int32
			ex := &modelExecutionCaptureExecutor{provider: provider, execute: func(context.Context, *coreauth.Auth, coreexecutor.Request, coreexecutor.Options) (coreexecutor.Response, error) {
				calls.Add(1)
				return coreexecutor.Response{Payload: []byte(`{}`)}, nil
			}}
			model := "auto-alias-choice"
			h := newModelExecutionHandler(t, model, ex, &sdkconfig.SDKConfig{})
			r := registry.GetGlobalRegistry()
			// ResolveAutoModel chooses the newest registration. Use a fixed future timestamp,
			// independent of other package tests, without changing the global registry itself.
			r.RegisterClient("model-execution-"+model, provider, []*registry.ModelInfo{{ID: model, Created: 1 << 62, MetadataModelID: "upstream-model"}})
			ctx := contextWithHeaders(http.Header{"User-Agent": {"grok-shell/0.2.119"}})
			_, _, errMsg := h.ExecuteWithAuthManager(ctx, "openai", "auto(high)", []byte(`{}`), "")
			if provider == "xai" {
				if errMsg != nil || calls.Load() != 1 {
					t.Fatalf("xAI auto not allowed: calls=%d error=%+v", calls.Load(), errMsg)
				}
			} else {
				if errMsg == nil || errMsg.StatusCode != 400 || calls.Load() != 0 {
					t.Fatalf("non-xAI auto reached execution: calls=%d error=%+v", calls.Load(), errMsg)
				}
			}
			calls.Store(0)
			r.RegisterClient("mixed-alias-sibling", "meta", []*registry.ModelInfo{{ID: model}})
			t.Cleanup(func() { r.UnregisterClient("mixed-alias-sibling") })
			for _, protocol := range []string{"openai", "openai-response"} {
				_, _, errMsg = h.ExecuteWithAuthManager(ctx, protocol, model, []byte(`{}`), "")
				if errMsg == nil || errMsg.StatusCode != 400 || calls.Load() != 0 {
					t.Fatalf("mixed alias reached execution: calls=%d error=%+v", calls.Load(), errMsg)
				}
			}
		})
	}
}
