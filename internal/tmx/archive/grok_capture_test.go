package archive

import (
	"bytes"
	"context"
	"embed"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MoeForge/moego/json"
	"github.com/MoeForge/moego/ojson"
	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/api/middleware"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/requestlog"
	coresession "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/session"
)

const grokNative = "00000000-0000-0000-0000-000000000001"
const grokGroup = "00000000-0000-0000-0000-000000000002"

//go:embed testdata/grok/*.json
var grokCaptureFixtures embed.FS

type grokCaptureFixture struct {
	Method, Path string
	Headers      http.Header
	Body         json.RawMessage
}

func loadGrokCaptureFixture(t *testing.T, name string) grokCaptureFixture {
	t.Helper()
	b, err := grokCaptureFixtures.ReadFile("testdata/grok/grok-" + name + ".json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture grokCaptureFixture
	if err = json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	return fixture
}

func captureGrokFixture(t *testing.T, fixture grokCaptureFixture, inbound string, logOn bool) *capture.Exchange {
	t.Helper()
	var ex *capture.Exchange
	capture.SetSink(func(v *capture.Exchange) { ex = v })
	t.Cleanup(func() { capture.SetSink(nil) })
	r := gin.New()
	r.Use(middleware.RequestLoggingMiddleware(logging.NewFileRequestLogger(logOn, t.TempDir(), "", 0)))
	r.Handle(fixture.Method, fixture.Path, func(c *gin.Context) {
		if inbound != "" {
			c.Request = c.Request.WithContext(logging.WithClientRequestMetadata(c.Request.Context(), logging.ClientRequestMetadata{SessionID: inbound}))
		}
		_, _ = io.ReadAll(c.Request.Body)
		c.Data(200, "application/json", []byte(`{"choices":[{"message":{"role":"assistant","content":"synthetic reply"}}]}`))
	})
	req := httptest.NewRequest(fixture.Method, fixture.Path, bytes.NewReader(fixture.Body))
	req.Header = fixture.Headers.Clone()
	r.ServeHTTP(httptest.NewRecorder(), req)
	if ex == nil {
		t.Fatal("no captured exchange")
	}
	t.Cleanup(ex.Release)
	return ex
}

func TestGrokCaptureRealRequestShapes(t *testing.T) {
	for _, name := range []string{"main", "helper"} {
		for _, logOn := range []bool{false, true} {
			t.Run(name+map[bool]string{false: "/logs-off", true: "/logs-on"}[logOn], func(t *testing.T) {
				ex := captureGrokFixture(t, loadGrokCaptureFixture(t, name), "", logOn)
				wantNative, wantModel := "", "grok-4.6"
				if name == "main" {
					wantNative, wantModel = grokNative, "grok-4.7"
				}
				if ex.SessionID != wantNative || ex.ClientSessionID != grokGroup || ex.RequestModel != wantModel {
					t.Fatalf("captured native=%q group=%q model=%q; want native=%q group=%q model=%q", ex.SessionID, ex.ClientSessionID, ex.RequestModel, wantNative, grokGroup, wantModel)
				}
			})
		}
	}
}

func TestGrokCapturePrecedenceAndGuards(t *testing.T) {
	for _, mode := range []string{"inbound", "generic-header", "claude-body", "codex-header", "agy-header", "other-model", "other-client", "other-route", "no-group", "no-native", "invalid-group", "invalid-native", "duplicate-group", "next-turn"} {
		t.Run(mode, func(t *testing.T) {
			f := loadGrokCaptureFixture(t, "main")
			wantNative, wantGroup, inbound := grokNative, grokGroup, ""
			switch mode {
			case "inbound":
				inbound, wantNative, wantGroup = "inbound-authority", "inbound-authority", ""
			case "generic-header", "codex-header", "agy-header":
				key := map[string]string{"generic-header": "X-Session-ID", "codex-header": "Session_id", "agy-header": "X-Http-Session-Id"}[mode]
				f.Headers.Set(key, "other-authority")
				wantNative, wantGroup = coresession.NormalizeToCanonicalUUID("other-authority"), ""
			case "claude-body":
				doc := ojson.NewDoc(f.Body)
				_ = doc.Set("metadata.user_id", `{"session_id":"other-authority"}`)
				f.Body = doc.Bytes()
				wantNative, wantGroup = coresession.NormalizeToCanonicalUUID("other-authority"), "other-authority"
			case "other-model":
				doc := ojson.NewDoc(f.Body)
				_ = doc.Set("model", "gpt-offline")
				f.Body = doc.Bytes()
				wantNative, wantGroup = "", ""
			case "other-client":
				f.Headers.Set("X-Grok-Client-Identifier", "other-client")
				wantNative, wantGroup = "", ""
			case "other-route":
				f.Path = "/v1/messages"
				wantNative, wantGroup = "", ""
			case "no-group":
				f.Headers.Del("X-Grok-Conv-Group-Id")
				wantGroup = ""
			case "no-native":
				f.Headers.Set("X-Grok-Session-Id", "")
				wantNative = "" // Conv-Id is not a substitute for Session-Id.
			case "invalid-group":
				f.Headers.Set("X-Grok-Conv-Group-Id", "bad\nvalue")
				wantGroup = ""
			case "invalid-native":
				f.Headers.Set("X-Grok-Session-Id", string(bytes.Repeat([]byte("x"), 257)))
				wantNative = ""
			case "duplicate-group":
				f.Headers["x-grok-conv-group-id"] = []string{"conflicting-group"}
				wantGroup = ""
			case "next-turn":
				f.Headers.Set("X-Grok-Conv-Id", "next-conversation")
				f.Headers.Set("X-Grok-Agent-Id", "next-agent")
				f.Headers.Set("X-Grok-Req-Id", "next-request")
				doc := ojson.NewDoc(f.Body)
				_ = doc.Set("messages.1.content", "another synthetic turn")
				f.Body = doc.Bytes()
			}
			ex := captureGrokFixture(t, f, inbound, false)
			if ex.SessionID != wantNative || ex.ClientSessionID != wantGroup {
				t.Fatalf("native=%q group=%q, want native=%q group=%q", ex.SessionID, ex.ClientSessionID, wantNative, wantGroup)
			}
		})
	}
}

func TestGrokCaptureStoredAnalyticsLinks(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	for _, name := range []string{"helper", "main"} {
		ex := captureGrokFixture(t, loadGrokCaptureFixture(t, name), "", false)
		ex.TraceID = "grok-capture-" + name
		if err := a.storeExchange(ctx, ex); err != nil {
			t.Fatal(err)
		}
		wantNative := ""
		if name == "main" {
			wantNative = grokNative
		}
		var native, group string
		if err := a.db.QueryRow(`SELECT session_id, client_session_id FROM `+schema+`.exchanges WHERE trace_id=$1`, ex.TraceID).Scan(&native, &group); err != nil || native != wantNative || group != grokGroup {
			t.Fatalf("stored %s native=%q group=%q err=%v", name, native, group, err)
		}
		if _, err := requestlog.Default().DB().Exec(`INSERT INTO tmx.requests (ts, outcome, provider, model, trace_id, session_id) VALUES ($1,'success','xai',$2,$3,'')`, ex.StartedAt, ex.RequestModel, ex.TraceID); err != nil {
			t.Fatal(err)
		}
		drain(t, an)
	}
	if n := count(t, an, `SELECT count(*) FROM sessions WHERE id=$1 AND exchanges=2 AND alt_ids @> ARRAY[$2]`, "proxy:"+grokGroup, grokNative); n != 1 {
		t.Fatalf("grouped session with native alternate = %d", n)
	}
	if n := count(t, an, `SELECT count(*) FROM request_sessions WHERE session_id=$1 AND canonical_session_id=$1 AND via='trace_id'`, "proxy:"+grokGroup); n != 2 {
		t.Fatalf("usage links to group = %d, want 2", n)
	}
	for _, key := range []string{grokGroup, grokNative} {
		conv, err := a.Conversation(ctx, key, "", "", "proxy")
		if err != nil || conv.Source != "analytics" || conv.Model != "grok-4.7" || len(conv.Exchanges) != 2 {
			t.Fatalf("conversation %s = %+v, %v", key, conv, err)
		}
	}
	hits, err := a.AnalyticsSearch(ctx, SearchQuery{Q: "grok archive regression marker"})
	if err != nil || len(hits) != 1 || hits[0].SessionID != "proxy:"+grokGroup {
		t.Fatalf("search = %+v, %v", hits, err)
	}
}
