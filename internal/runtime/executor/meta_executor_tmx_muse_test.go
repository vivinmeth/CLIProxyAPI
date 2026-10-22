package executor

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
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
