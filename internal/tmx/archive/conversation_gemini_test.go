package archive

import (
	"bytes"
	"context"
	"database/sql"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/MoeForge/moego/json"
	"github.com/MoeForge/moego/ojson"
	"github.com/klauspost/compress/zstd"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
)

const geminiReply = `{"candidates":[{"index":0,"content":{"role":"model","parts":[
	{"text":"Check the weather.","thought":true},
	{"text":"It is sunny."},
	{"functionCall":{"id":"weather-1","name":"get_weather","args":{"city":"Paris"}}},
	{"executableCode":{"language":"PYTHON","code":"print(1)"}}
]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":30},"modelVersion":"gemini-2.5-pro"}`

func TestRequestModel(t *testing.T) {
	const native = "/v1beta/models/gemini-4-argon:streamGenerateContent?alt=sse&key=offline-secret#offline-fragment"
	for _, tc := range []struct {
		name, body, url, want string
	}{
		{"native", `{ "contents": [], "generationConfig": {"thinkingConfig": {"thinkingLevel": "high"}} }`, native, "gemini-4-argon"},
		{"absolute", `{}`, "http://127.0.0.1:8317" + native, "gemini-4-argon"},
		{"userinfo", `{}`, "https://offline-user:offline-secret@example.test" + native, "gemini-4-argon"},
		{"json", `{}`, "/v1beta/models/gemini-4-argon:generateContent", "gemini-4-argon"},
		{"count", `{}`, "/v1/models/gemini-2.5-flash-lite:countTokens?key=offline-secret", "gemini-2.5-flash-lite"},
		{"unversioned", `{}`, "/models/gemini-4-argon:generateContent", "gemini-4-argon"},
		{"body wins", `{"model":"explicit-model"}`, native, "explicit-model"},
		{"empty body model", `{"model":""}`, native, "gemini-4-argon"},
		{"anthropic", `{"model":"claude-opus-5-5"}`, "/v1/messages", "claude-opus-5-5"},
		{"openai", `{"model":"gpt-6.1-sol"}`, "/v1/responses", "gpt-6.1-sol"},
		{"body with malformed url", `{"model":"explicit-model"}`, "http://[", "explicit-model"},
		{"empty url", `{}`, "", ""},
		{"malformed", `{}`, "http://[", ""},
		{"bad escape", `{}`, "/v1beta/models/gemini-%zz:generateContent", ""},
		{"opaque", `{}`, "https:models/gemini-4-argon:generateContent", ""},
		{"wrong scheme", `{}`, "file:///v1beta/models/gemini-4-argon:generateContent", ""},
		{"missing host", `{}`, "https:///v1beta/models/gemini-4-argon:generateContent", ""},
		{"non gemini route", `{}`, "/v1/messages?model=gemini-4-argon", ""},
		{"non gemini model", `{}`, "/v1beta/models/gpt-6.1-sol:generateContent", ""},
		{"missing model", `{}`, "/v1beta/models/:generateContent", ""},
		{"empty gemini model", `{}`, "/v1beta/models/gemini-:generateContent", ""},
		{"missing action", `{}`, "/v1beta/models/gemini-4-argon", ""},
		{"wrong action", `{}`, "/v1beta/models/gemini-4-argon:predict", ""},
		{"extra path", `{}`, "/v1beta/models/gemini-4-argon:generateContent/extra", ""},
		{"embedded key", `{}`, "/v1beta/models/gemini-4-argon;key=offline-secret:generateContent", ""},
		{"escaped query", `{}`, "/v1beta/models/gemini-4-argon%3Fkey=offline-secret:generateContent", ""},
		{"escaped slash", `{}`, "/v1beta/models/gemini-4-argon%2Foffline-secret:generateContent", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := requestModel([]byte(tc.body), tc.url); got != tc.want {
				t.Errorf("requestModel = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestGeminiResponseJSON(t *testing.T) {
	for _, body := range []string{geminiReply, `{"response":` + geminiReply + `,"traceId":"offline"}`} {
		items := responseItems([]byte(body))
		if got := fmtSlice(kinds(items)); got != "[assistant:thinking assistant:text assistant:tool_call assistant:other]" {
			t.Fatalf("Gemini response items = %s", got)
		}
		if items[0].Text != "Check the weather." || items[1].Text != "It is sunny." {
			t.Fatalf("Gemini text = %+v", items[:2])
		}
		call := items[2]
		if call.Name != "get_weather" || call.CallID != "weather-1" || call.Text != `{"city":"Paris"}` || call.Data != call.Text {
			t.Fatalf("Gemini call = %+v", call)
		}
		if items[3].Data != `{"executableCode":{"language":"PYTHON","code":"print(1)"}}` {
			t.Fatalf("unknown part lost: %+v", items[3])
		}
	}
}

func TestGeminiRequestTools(t *testing.T) {
	items := normalizeRequest([]byte(`{"contents":[
		{"role":"user","parts":[{"text":"Weather?"}]},
		{"role":"model","parts":[{"functionCall":{"id":"w1","name":"weather","args":{"city":"Paris"}}}]},
		{"role":"user","parts":[{"functionResponse":{"id":"w1","name":"weather","response":{"temperature":22,"unit":"C"}}}]},
		{"role":"model","parts":[{"functionCall":{"name":"clock","args":{}}}]},
		{"role":"user","parts":[{"functionResponse":{"name":"clock","response":{"error":"offline"}}}]}
	]}`))
	if got := fmtSlice(kinds(items)); got != "[user:text assistant:tool_call tool:tool_result assistant:tool_call tool:tool_result]" {
		t.Fatalf("Gemini request = %s", got)
	}
	if items[1].CallID != "w1" || items[2].CallID != "w1" || items[2].Name != "weather" ||
		items[2].Text != `{"temperature":22,"unit":"C"}` || items[2].Data != items[2].Text {
		t.Fatalf("Gemini result = %+v", items[2])
	}
	if items[3].CallID != "" || items[4].CallID != "" {
		t.Fatalf("invented ids: %+v", items[3:])
	}
}

func TestGeminiResponseStream(t *testing.T) {
	sse := strings.Join([]string{
		`: keepalive`,
		`data: {"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"Look ","thought":true}]}}]}`,
		`data: {"candidates":[{"index":0,"content":{"parts":[{"text":"first.","thought":true}]}}]}`,
		`data: {"response":{"candidates":[{"index":0,"content":{"parts":[{"text":"Hello "}]}}]}}`,
		`data: {"candidates":[{"index":0,"content":{"parts":[{"text":"world"},{"functionCall":{"id":"c1","name":"read","args":{"path":"a.go"}}}]}}]}`,
		`data: {"candidates":[{"index":0,"content":{"parts":[{"functionCall":{"id":"c1","name":"read","args":{"path":"a.go"}}}]}}]}`,
		`data: {"candidates":[{"index":0,"content":{"parts":[{"functionResponse":{"id":"c1","name":"read","response":{"output":"package a"}}}]}}]}`,
		`data: {"candidates":[{"index":0,"content":{"parts":[{"functionResponse":{"id":"c1","name":"read","response":{"output":"package a"}}},{"fileData":{"mimeType":"image/png","fileUri":"gs://offline/image"}}]}}]}`,
		`data: {"candidates":[{"index":0,"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":20}}`,
		`data: [DONE]`,
	}, "\n\n")
	items := responseItems([]byte(sse))
	if got := fmtSlice(kinds(items)); got != "[assistant:thinking assistant:text assistant:tool_call tool:tool_result assistant:other]" {
		t.Fatalf("Gemini stream = %s: %+v", got, items)
	}
	if items[0].Text != "Look first." || items[1].Text != "Hello world" || items[2].CallID != "c1" ||
		items[2].Data != `{"path":"a.go"}` || items[3].Name != "read" || items[3].CallID != "c1" ||
		items[3].Data != `{"output":"package a"}` || ojson.Get(items[4].Data, "fileData.fileUri").String() != "gs://offline/image" {
		t.Fatalf("Gemini stream details = %+v", items)
	}
}

func TestGeminiStreamToolIdentity(t *testing.T) {
	// Missing ids are normal on Gemini. Each complete part remains a separate call
	// or result, including equal parts in the same frame or in later frames.
	for _, part := range []string{
		`{"functionCall":{"name":"clock","args":{}}}`,
		`{"functionResponse":{"name":"clock","response":{"time":"12:00"}}}`,
	} {
		frame := func(parts string) string {
			return `data: {"candidates":[{"content":{"role":"model","parts":[` + parts + `]}}]}` + "\n\n"
		}
		for _, tc := range []struct {
			stream string
			count  int
		}{
			{frame(part) + frame(part), 2},
			{frame(part + "," + part), 2},
			{frame(part+","+part) + frame(part+","+part), 4},
		} {
			items := responseItems([]byte(tc.stream))
			if len(items) != tc.count {
				t.Fatalf("idless streamed tools: got %d, want %d: %+v", len(items), tc.count, items)
			}
			for _, it := range items {
				if it.CallID != "" || it.Name != "clock" {
					t.Fatalf("idless tool identity = %+v", it)
				}
			}
		}
	}
	sse := `data: {"candidates":[{"index":0,"content":{"parts":[{"functionCall":{"id":"a","name":"clock","args":{}}}]}},{"index":1,"content":{"parts":[{"functionCall":{"id":"a","name":"clock","args":{}}}]}}]}`
	if items := responseItems([]byte(sse)); len(items) != 2 {
		t.Fatalf("separate candidates collapsed = %+v", items)
	}
}

func TestGeminiEmptyParts(t *testing.T) {
	for _, empty := range []string{
		`{"text":""}`,
		`{"text":"","thought":true}`,
		`{"text":"","thoughtSignature":"sig-empty"}`,
		`{"thoughtSignature":"sig-only"}`,
		`{"thought":true,"thoughtSignature":"sig-only"}`,
		`{"thought_signature":"sig-only"}`,
	} {
		t.Run(empty, func(t *testing.T) {
			frame := func(parts string) string {
				return `data: {"candidates":[{"index":0,"content":{"role":"model","parts":[` + parts + `]}}],"modelVersion":"gemini-2.5-pro"}` + "\n\n"
			}
			// Empty and signature-only frames must not split adjacent text or leave
			// a trailing item after a completed reply.
			sse := frame(`{"text":"Hello "}`) + frame(empty) + frame(`{"text":"world"}`) + frame(empty)
			items := responseItems([]byte(sse))
			if len(items) != 1 || items[0].Kind != "text" || items[0].Text != "Hello world" {
				t.Fatalf("stream with empty part = %+v", items)
			}
			body := `{"candidates":[{"content":{"role":"model","parts":[{"text":"Hello "},` + empty + `,{"text":"world"},` + empty + `]}}]}`
			items = responseItems([]byte(body))
			if len(items) != 2 || items[0].Kind != "text" || items[0].Text != "Hello " || items[1].Kind != "text" || items[1].Text != "world" {
				t.Fatalf("JSON with empty parts = %+v", items)
			}
		})
	}
	// Imported content blocks retain their prior behavior; only Gemini parts are skipped.
	items := normalizeRequest([]byte(`{"messages":[{"role":"assistant","content":[{"text":""},{"type":"text","text":""}]}]}`))
	if len(items) != 2 || items[0].Kind != "other" || items[0].Data != `{"text":""}` || items[1].Kind != "text" {
		t.Fatalf("imported empty content changed: %+v", items)
	}
}

func TestGeminiFunctionResponseError(t *testing.T) {
	const payload = `{"error":{"code":"NOT_FOUND","message":"missing file"},"output":{"path":"a.go"}}`
	body := `{"contents":[{"role":"user","parts":[{"functionResponse":{"id":"read-1","name":"read","response":` + payload + `}}]}]}`
	items := normalizeRequest([]byte(body))
	if len(items) != 1 || items[0].Kind != "tool_result" || !items[0].IsError || items[0].Name != "read" ||
		items[0].CallID != "read-1" || items[0].Text != payload || items[0].Data != payload {
		t.Fatalf("Gemini error result = %+v", items)
	}
}

func TestGeminiMalformedResponse(t *testing.T) {
	for _, body := range []string{"", "null", `{`, `{"candidates":null}`, `{"candidates":[null,{}, {"content":null}]}`,
		`{"candidates":[{"content":{"parts":null}}]}`, `{"candidates":[{"content":{"parts":"invalid"}}]}`,
		"data: {\"candidates\":[\n\ndata: [DONE]\n"} {
		if items := responseItems([]byte(body)); len(items) != 0 {
			t.Errorf("malformed/missing response %q = %+v", body, items)
		}
	}
	for _, part := range []string{`{"functionCall":null}`, `{"functionCall":"bad"}`, `{"functionResponse":[]}`, `{"thought":true,"text":{}}`,
		`{"text":"","future":{"value":1}}`, `{"thoughtSignature":{}}`, `{"text":null,"thoughtSignature":"sig"}`,
		`{"text":"","thoughtSignature":7}`, `{"text":"","thought":true,"future":{"value":1}}`} {
		body := `{"candidates":[{"content":{"parts":[` + part + `]}}]}`
		items := responseItems([]byte(body))
		if len(items) != 1 || items[0].Kind != "other" || items[0].Data != part {
			t.Errorf("malformed part lost: %+v", items)
		}
	}
}

func TestGeminiProxyMessages(t *testing.T) {
	// Exercise the actual analytics producer with local compressed blobs, no DB,
	// uploader, credentials or network.
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer enc.Close()
	dec, err := zstd.NewReader(nil, zstd.WithDecoderConcurrency(1))
	if err != nil {
		t.Fatal(err)
	}
	defer dec.Close()
	blobs := &BlobStore{spool: t.TempDir(), dec: dec}
	put := func(body string) string {
		hash := HashOf([]byte(body))
		if err := os.WriteFile(blobs.spoolPath(hash), enc.EncodeAll([]byte(body), nil), 0o600); err != nil {
			t.Fatal(err)
		}
		return hash
	}
	an := &analytics{a: &Archive{blobs: blobs}}
	finished := time.Date(2026, 11, 21, 7, 0, 0, 0, time.UTC)
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(geminiReply)); err != nil {
		t.Fatal(err)
	}
	for _, request := range []struct {
		name, url, model, bodyModel string
	}{
		{"native", "/v1beta/models/gemini-4-argon:streamGenerateContent?alt=sse", "gemini-4-argon", ""},
		{"absolute", "http://127.0.0.1:8317/v1beta/models/gemini-4-argon:generateContent?key=offline-secret#offline-fragment", "gemini-4-argon", ""},
		{"helper", "/v1beta/models/gemini-2.5-flash-lite:generateContent", "gemini-2.5-flash-lite", ""},
		{"explicit", "/v1beta/models/gemini-4-argon:streamGenerateContent?alt=sse", "explicit-model", `"model":"explicit-model",`},
		{"non gemini", "/v1/messages", "", ""},
	} {
		t.Run(request.name, func(t *testing.T) {
			req := put(`{` + request.bodyModel + `"contents":[{"role":"user","parts":[{"text":"Weather?"}]}],"generationConfig":{"thinkingConfig":{"thinkingLevel":"high"}}}`)
			manifest, err := json.Marshal(Manifest{Format: "raw", Raw: req})
			if err != nil {
				t.Fatal(err)
			}
			for _, response := range []string{geminiReply, "data: " + compact.String() + "\n\ndata: [DONE]\n"} {
				rows, model, err := an.proxyMessages(context.Background(), proxyExchange{
					traceID: "gemini-offline", reqBody: put(string(manifest)), resBody: put(response),
					url:      request.url,
					finished: sql.NullTime{Time: finished, Valid: true},
				})
				if err != nil || model != request.model || len(rows) != 2 {
					t.Fatalf("analytics messages = %+v, %q, %v", rows, model, err)
				}
				for _, row := range rows {
					if row.model != model {
						t.Fatalf("analytics message model = %q, want %q", row.model, model)
					}
				}
				_, items, err := rows[1].produce()
				if got := fmtSlice(kinds(items)); got != "[assistant:thinking assistant:text assistant:tool_call assistant:other]" {
					t.Fatalf("analytics Gemini items = %s", got)
				}
				want := responseItems([]byte(geminiReply))
				for i := range want {
					want[i].Model = model
				}
				if err != nil || !reflect.DeepEqual(items, want) || rows[1].role != "assistant" || !rows[1].ts.Equal(finished) {
					t.Fatalf("analytics reply = %+v, %v", items, err)
				}
			}
		})
	}
}

func TestGeminiModelAttributionStored(t *testing.T) {
	a, an := openTestAnalytics(t)
	ctx := context.Background()
	start := time.Date(2026, 11, 21, 7, 0, 0, 0, time.UTC)
	ex := &capture.Exchange{
		TraceID: "gemini-native", SessionID: "gemini-native-session", Method: "POST",
		URL: "/v1beta/models/gemini-4-argon:streamGenerateContent?alt=sse", Status: 200,
		StartedAt: start, FinishedAt: start.Add(time.Second),
		RequestBody:  []byte(`{"contents":[{"role":"user","parts":[{"text":"Weather?"}]}]}`),
		ResponseBody: []byte(geminiReply),
	}
	if err := a.storeExchange(ctx, ex); err != nil {
		t.Fatal(err)
	}
	// Exercise the archive reader before analytics extraction, then the SQL
	// SELECT/Scan and persisted session/message/item models through the worker.
	check := func(source string) {
		t.Helper()
		conv, err := a.Conversation(ctx, ex.SessionID, "", "", "proxy")
		if err != nil {
			t.Fatal(err)
		}
		if conv.Source != source || conv.Model != "gemini-4-argon" || len(conv.Items) != 5 {
			t.Fatalf("Gemini conversation = %+v", conv)
		}
		for _, it := range conv.Items {
			if it.Role == "assistant" && it.Model != conv.Model {
				t.Errorf("assistant model = %q, want %q", it.Model, conv.Model)
			}
		}
	}
	check("") // The archive reader leaves Source empty; analytics labels its source.
	drain(t, an)
	if n := count(t, an, `SELECT count(*) FROM sessions WHERE id = 'proxy:gemini-native-session' AND model = 'gemini-4-argon'`); n != 1 {
		t.Fatalf("attributed sessions = %d", n)
	}
	if n := count(t, an, `SELECT count(*) FROM messages WHERE session_id = 'proxy:gemini-native-session' AND model = 'gemini-4-argon'`); n != 2 {
		t.Fatalf("attributed messages = %d", n)
	}
	if n := count(t, an, `SELECT count(*) FROM items WHERE session_id = 'proxy:gemini-native-session' AND role = 'assistant' AND model = 'gemini-4-argon'`); n != 4 {
		t.Fatalf("attributed assistant items = %d", n)
	}
	check("analytics")
}
