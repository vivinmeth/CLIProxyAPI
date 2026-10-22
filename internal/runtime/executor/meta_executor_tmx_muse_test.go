package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/muse"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// Muse on /v1/responses keeps Meta's subscription_usage events (its usage meter reads them);
// every other client and format still has them filtered.
func TestMetaExecutor_MusePassesSubscriptionUsage(t *testing.T) {
	const usageData = `{"subscription":{"tier":"1234567890123456","weekly":{"resets_at":1792368000,"used_percent":80}},"type":"response.subscription_usage"}`
	upstream := "event: response.subscription_usage\ndata: " + usageData + "\n\n" +
		"event: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"in_progress\",\"model\":\"muse-spark-1.3\"}}\n\n" +
		"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"item_id\":\"msg_1\",\"output_index\":0,\"content_index\":0,\"delta\":\"hello\"}\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"muse-spark-1.3\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, upstream)
	}))
	defer server.Close()
	exec := NewMetaExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "meta", Attributes: map[string]string{"api_key": "k", "base_url": server.URL}}

	museHeaders := http.Header{"User-Agent": {"muse-build/1.4.11 (non-interactive; linux-x86_64; build x)"}, "X-Client-Id": {"tbh:exec"}}
	cases := []struct {
		name    string
		format  string
		payload string
		headers http.Header
		want    bool
	}{
		{"muse responses", "openai-response", `{"model":"muse-spark-1.3","input":"hi"}`, museHeaders, true},
		{"other responses client", "openai-response", `{"model":"muse-spark-1.3","input":"hi"}`, http.Header{"User-Agent": {"codex_exec/0.163.0"}}, false},
		{"muse on chat completions", "openai", `{"model":"muse-spark-1.3","messages":[{"role":"user","content":"hi"}]}`, museHeaders, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := exec.ExecuteStream(context.Background(), auth, cliproxyexecutor.Request{Model: "muse-spark-1.3", Payload: []byte(tc.payload)},
				cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString(tc.format), Stream: true, Headers: tc.headers})
			if err != nil {
				t.Fatal(err)
			}
			var got strings.Builder
			for chunk := range res.Chunks {
				if chunk.Err != nil {
					t.Fatal(chunk.Err)
				}
				got.Write(chunk.Payload)
				got.WriteByte('\n')
			}
			out := got.String()
			if has := strings.Contains(out, "event: response.subscription_usage\ndata: "+usageData+"\n\n"); has != tc.want {
				t.Fatalf("subscription_usage passed = %v, want %v:\n%s", has, tc.want, out)
			}
			if !tc.want && strings.Contains(out, "1234567890123456") {
				t.Fatalf("subscription data leaked:\n%s", out)
			}
			if !strings.Contains(out, "hello") {
				t.Fatalf("missing content:\n%s", out)
			}
		})
	}
}

// Every Meta stream and non-stream answer turns subscription_usage into the credential's
// X-Meta-Subscription-* quota signals (internal/tmx/quota reads them), whatever the client.
func TestMetaExecutor_SubscriptionUsageBecomesQuotaSignals(t *testing.T) {
	const usageData = `{"subscription":{"tier":"1234567890123456","weekly":{"resets_at":1792368000,"used_percent":3},"window":{"resets_at":1792272519,"used_percent":12,"window_duration_mins":300}},"type":"response.subscription_usage"}`
	upstream := "event: response.subscription_usage\ndata: " + usageData + "\n\n" +
		"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"object\":\"response\",\"status\":\"completed\",\"model\":\"muse-spark-1.3\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, upstream)
	}))
	defer server.Close()
	exec := NewMetaExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "meta", Attributes: map[string]string{"api_key": "k", "base_url": server.URL}}
	want := map[string]string{
		"X-Meta-Subscription-Window-Used-Percent":   "12",
		"X-Meta-Subscription-Window-Reset-At":       "1792272519",
		"X-Meta-Subscription-Window-Window-Minutes": "300",
		"X-Meta-Subscription-Weekly-Used-Percent":   "3",
		"X-Meta-Subscription-Weekly-Reset-At":       "1792368000",
	}
	check := func(t *testing.T, ctx context.Context) {
		t.Helper()
		got := logging.GetResponseHeaders(ctx)
		for k, v := range want {
			if got.Get(k) != v {
				t.Fatalf("%s = %q, want %q (all: %v)", k, got.Get(k), v, got)
			}
		}
		var q cliproxyauth.QuotaState
		if !q.ObserveResponseHeadersForProvider("meta", got, time.Now()) || len(q.Signals) != len(want) {
			t.Fatalf("meta quota observation kept %v", q.Signals)
		}
	}
	payload := []byte(`{"model":"muse-spark-1.3","input":"hi"}`)
	t.Run("stream", func(t *testing.T) {
		ctx := logging.WithResponseHeadersHolder(context.Background())
		res, err := exec.ExecuteStream(ctx, auth, cliproxyexecutor.Request{Model: "muse-spark-1.3", Payload: payload},
			cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Stream: true})
		if err != nil {
			t.Fatal(err)
		}
		for range res.Chunks {
		}
		check(t, ctx)
	})
	t.Run("non-stream", func(t *testing.T) {
		ctx := logging.WithResponseHeadersHolder(context.Background())
		if _, err := exec.Execute(ctx, auth, cliproxyexecutor.Request{Model: "muse-spark-1.3", Payload: payload},
			cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response")}); err != nil {
			t.Fatal(err)
		}
		check(t, ctx)
	})
}

// Contributor-mode models are refused before anything is sent to Meta (tmx/muse/contributor.go).
func TestMetaExecutor_RefusesContributorModels(t *testing.T) {
	t.Setenv("TMX_META_CONTRIBUTOR", "")
	hits := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits++ }))
	defer server.Close()
	exec := NewMetaExecutor(&config.Config{})
	auth := &cliproxyauth.Auth{Provider: "meta", Attributes: map[string]string{"api_key": "k", "base_url": server.URL}}
	req := cliproxyexecutor.Request{Model: "muse-spark-1.3-contributor", Payload: []byte(`{"model":"muse-spark-1.3-contributor","input":"hi"}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), Stream: true}
	_, errStream := exec.ExecuteStream(context.Background(), auth, req, opts)
	_, errExec := exec.Execute(context.Background(), auth, req, opts)
	_, errCount := exec.CountTokens(context.Background(), auth, req, opts)
	for _, err := range []error{errStream, errExec, errCount} {
		if !muse.IsContributorError(err) || !strings.Contains(err.Error(), muse.ContributorBlockedMessage) {
			t.Fatalf("err = %v", err)
		}
		if sc, ok := err.(interface{ IsRequestScoped() bool }); !ok || !sc.IsRequestScoped() {
			t.Fatalf("refusal must be request-scoped: %T", err)
		}
	}
	if hits != 0 {
		t.Fatalf("upstream called %d times", hits)
	}
}
