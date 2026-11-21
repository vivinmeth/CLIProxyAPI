package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

// TestAntigravityTwoAccountAdmissionAcrossCredentialPrepAndStream covers real AG
// preparation and stream lifetimes under soft-cap ranking, not full-pool rejection.
// It excludes OAuth cancellation, affinity spill, auto-refresh, credits, Home and
// revocation of already-admitted work. Warm-token project discovery has its own test.
func TestAntigravityTwoAccountAdmissionAcrossCredentialPrepAndStream(t *testing.T) {
	resetAntigravityCreditsRetryState()
	const model = "gemini-3.8-flash-high"
	idA, idB := "ag-adm-"+t.Name()+"-a", "ag-adm-"+t.Name()+"-b"
	refreshA, refreshB := t.Name()+"-refresh-a", t.Name()+"-refresh-b"
	cfg := &config.Config{}
	cfg.Routing.ResetSoonest.MaxInflightPerCredential = 1
	manager := cliproxyauth.NewManager(nil, &cliproxyauth.ResetSoonestSelector{}, nil)
	manager.SetConfig(cfg)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(NewAntigravityExecutor(cfg))

	tokenEntered, releaseToken := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseToken) }) }
	var mu sync.Mutex
	tokenCalls, projectCalls, generationCalls := map[string]int{}, map[string]int{}, map[string]int{}
	generationBodies := map[string][]byte{}
	var unexpected []string
	badRequest := func(message string) (*http.Response, error) {
		mu.Lock()
		unexpected = append(unexpected, message)
		mu.Unlock()
		return nil, fmt.Errorf("fixture: %s", message)
	}
	pipeReader, pipeWriter := io.Pipe()
	var writers sync.WaitGroup
	transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		bearer := strings.TrimPrefix(req.Header.Get("Authorization"), "Bearer ")
		switch {
		case req.URL.String() == "https://oauth2.googleapis.com/token":
			if errParse := req.ParseForm(); errParse != nil {
				return badRequest("invalid OAuth form")
			}
			refresh := req.Form.Get("refresh_token")
			mu.Lock()
			tokenCalls[refresh]++
			calls := tokenCalls[refresh]
			mu.Unlock()
			if refresh != refreshA || calls != 1 {
				return badRequest("unexpected or repeated OAuth refresh")
			}
			close(tokenEntered)
			select {
			case <-releaseToken:
				return issue6199AntigravityJSONResponse(req, `{"access_token":"fresh-a","expires_in":3600}`), nil
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		case req.URL.Path == "/v1internal:loadCodeAssist":
			mu.Lock()
			projectCalls[bearer]++
			mu.Unlock()
			if bearer != "fresh-a" && bearer != "token-b" {
				return badRequest("metadata lookup used an unexpected token")
			}
			return issue6199AntigravityJSONResponse(req, `{"cloudaicompanionProject":"proj-a","paidTier":{"id":"tier","availableCredits":[]}}`), nil
		case req.URL.Path == "/v1internal:streamGenerateContent":
			body, errRead := io.ReadAll(req.Body)
			if errRead != nil {
				return badRequest("could not read generation body")
			}
			mu.Lock()
			generationCalls[bearer]++
			generationBodies[bearer] = body
			calls := generationCalls[bearer]
			mu.Unlock()
			response := &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"text/event-stream"}}, Request: req}
			switch {
			case bearer == "fresh-a" && calls == 1:
				response.Body = io.NopCloser(strings.NewReader("data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"Hello, world!\"}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":10,\"candidatesTokenCount\":5,\"totalTokenCount\":15},\"modelVersion\":\"gemini-3.8-flash\",\"responseId\":\"resp-a\"}}\n\n"))
			case bearer == "token-b" && calls == 1:
				// A custom transport must close its body on cancellation itself.
				writers.Add(1)
				context.AfterFunc(req.Context(), func() {
					defer writers.Done()
					_ = pipeWriter.CloseWithError(context.Canceled)
				})
				writers.Add(1)
				go func() {
					defer writers.Done()
					_, errWrite := io.WriteString(pipeWriter, "data: {\"response\":{\"candidates\":[{\"content\":{\"role\":\"model\",\"parts\":[{\"text\":\"held\"}]}}],\"responseId\":\"resp-b\"}}\n\n")
					if errWrite != nil && req.Context().Err() == nil {
						_, _ = badRequest("could not write B bootstrap event")
					}
				}()
				response.Body = pipeReader
			default:
				return badRequest("generation used a stale/unexpected token or repeated an account")
			}
			return response, nil
		default:
			return badRequest("unexpected request path: " + req.URL.Path)
		}
	})
	baseCtx := context.WithValue(context.Background(), "cliproxy.roundtripper", transport)
	ctxA, cancelA := context.WithTimeout(baseCtx, 10*time.Second)
	ctxB, cancelB := context.WithTimeout(baseCtx, 10*time.Second)
	type result struct {
		stream *cliproxyexecutor.StreamResult
		err    error
	}
	var pending []<-chan result
	var streams []*cliproxyexecutor.StreamResult
	drain := func(stream *cliproxyexecutor.StreamResult, checkErrors bool) error {
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		var firstErr error
		for {
			select {
			case chunk, ok := <-stream.Chunks:
				if !ok {
					return firstErr
				}
				if checkErrors && firstErr == nil {
					firstErr = chunk.Err
				}
			case <-timer.C:
				return fmt.Errorf("stream did not close within 5s")
			}
		}
	}
	t.Cleanup(func() {
		defer func() {
			for _, id := range []string{idA, idB} {
				antigravityCreditsBalanceByAuth.Delete(id)
				antigravityCreditsHintRefreshByID.Delete(id)
				cliproxyauth.SetAntigravityCreditsHint(id, cliproxyauth.AntigravityCreditsHint{})
				registry.GetGlobalRegistry().UnregisterClient(id)
			}
			resetAntigravityCreditsRetryState()
		}()
		release()
		cancelA()
		cancelB()
		_ = pipeWriter.CloseWithError(context.Canceled)
		_ = pipeReader.Close()
		for _, done := range pending {
			select {
			case received := <-done:
				if received.stream != nil {
					streams = append(streams, received.stream)
				}
			case <-time.After(5 * time.Second):
				t.Error("ExecuteStream did not return during cleanup")
			}
		}
		for _, stream := range streams {
			if errDrain := drain(stream, false); errDrain != nil {
				t.Error(errDrain)
			}
		}
		writersDone := make(chan struct{})
		go func() { writers.Wait(); close(writersDone) }()
		select {
		case <-writersDone:
		case <-time.After(5 * time.Second):
			t.Error("pipe writer did not finish during cleanup")
		}
		for _, id := range []string{idA, idB} {
			if value, ok := antigravityCreditsHintRefreshByID.Load(id); ok {
				waitForAntigravityCreditsRefresh(t, value.(*antigravityCreditsHintRefreshState))
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if len(unexpected) != 0 {
			t.Errorf("unexpected requests after background work finished: %v", unexpected)
		}
	})
	for _, auth := range []*cliproxyauth.Auth{
		{ID: idA, Provider: "antigravity", Status: cliproxyauth.StatusActive, Metadata: map[string]any{"access_token": "stale-a", "refresh_token": refreshA, "expired": time.Now().Add(2 * time.Minute).Format(time.RFC3339)}},
		{ID: idB, Provider: "antigravity", Status: cliproxyauth.StatusActive, Metadata: map[string]any{"access_token": "token-b", "refresh_token": refreshB, "project_id": "proj-b", "expired": time.Now().Add(24 * time.Hour).Format(time.RFC3339)}},
	} {
		registry.GetGlobalRegistry().RegisterClient(auth.ID, "antigravity", []*registry.ModelInfo{{ID: model}})
		if _, errRegister := manager.Register(baseCtx, auth); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	req := cliproxyexecutor.Request{Model: model, Payload: []byte(`{"model":"gemini-3.8-flash-high","input":"hello","stream":true}`)}
	opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, ResponseFormat: sdktranslator.FormatOpenAIResponse, Stream: true}
	start := func(ctx context.Context) chan result {
		done := make(chan result, 1)
		pending = append(pending, done)
		go func() {
			stream, errStream := manager.ExecuteStream(ctx, []string{"antigravity"}, req, opts)
			done <- result{stream, errStream}
		}()
		return done
	}
	receive := func(done <-chan result) *cliproxyexecutor.StreamResult {
		t.Helper()
		select {
		case received := <-done:
			for i, ch := range pending {
				if ch == done {
					pending = append(pending[:i], pending[i+1:]...)
					break
				}
			}
			if received.stream != nil {
				streams = append(streams, received.stream)
			}
			if received.err != nil || received.stream == nil {
				t.Fatalf("ExecuteStream: stream=%v error=%v", received.stream, received.err)
			}
			return received.stream
		case <-time.After(5 * time.Second):
			t.Fatal("ExecuteStream did not bootstrap within 5s")
			return nil
		}
	}
	assertCounts := func(a, b int) {
		t.Helper()
		if gotA, gotB := manager.InFlightCount(idA), manager.InFlightCount(idB); gotA != a || gotB != b {
			t.Fatalf("in-flight A/B = %d/%d, want %d/%d", gotA, gotB, a, b)
		}
	}
	assertRequests := func(a, b int) {
		t.Helper()
		mu.Lock()
		defer mu.Unlock()
		if generationCalls["fresh-a"] != a || generationCalls["token-b"] != b || len(generationCalls) != a+b || len(unexpected) != 0 {
			t.Fatalf("generation calls=%v unexpected=%v, want A/B %d/%d", generationCalls, unexpected, a, b)
		}
		if tokenCalls[refreshA] != 1 || tokenCalls[refreshB] != 0 || len(tokenCalls) != 1 {
			t.Fatalf("OAuth calls=%v, want A once and B never", tokenCalls)
		}
		for token, project := range map[string]string{"fresh-a": "proj-a", "token-b": "proj-b"} {
			if generationCalls[token] > 0 && ojson.GetBytes(generationBodies[token], "project").String() != project {
				t.Errorf("generation project for %s = %q, want %q", token, ojson.GetBytes(generationBodies[token], "project").String(), project)
			}
		}
		if a > 0 && projectCalls["fresh-a"] == 0 {
			t.Error("A did not discover its project with its refreshed token")
		}
	}
	aDone := start(ctxA)
	select {
	case <-tokenEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("A did not enter OAuth")
	}
	assertCounts(1, 0)
	assertRequests(0, 0)
	streamB := receive(start(ctxB))
	assertCounts(1, 1)
	assertRequests(0, 1)
	select {
	case received := <-aDone:
		aDone <- received // Keep the result available for failure cleanup.
		t.Fatal("A returned while OAuth was held")
	default:
	}
	release()
	streamA := receive(aDone)
	if errDrain := drain(streamA, true); errDrain != nil {
		t.Fatal(errDrain)
	}
	assertCounts(0, 1)
	assertRequests(1, 1)
	stored, ok := manager.GetByID(idA)
	if !ok || metaStringValue(stored.Metadata, "access_token") != "fresh-a" || metaStringValue(stored.Metadata, "project_id") != "proj-a" {
		t.Fatal("manager did not retain A's refreshed token and discovered project")
	}
	cancelB()
	if errDrain := drain(streamB, false); errDrain != nil {
		t.Fatal(errDrain)
	}
	assertCounts(0, 0)
	assertRequests(1, 1)
}
