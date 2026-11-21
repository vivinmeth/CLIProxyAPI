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
)

const geminiReply = `{"candidates":[{"index":0,"content":{"role":"model","parts":[
	{"text":"Check the weather.","thought":true},
	{"text":"It is sunny."},
	{"functionCall":{"id":"weather-1","name":"get_weather","args":{"city":"Paris"}}},
	{"executableCode":{"language":"PYTHON","code":"print(1)"}}
]},"finishReason":"STOP"}],"usageMetadata":{"candidatesTokenCount":30},"modelVersion":"gemini-2.5-pro"}`

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
	// Missing call ids must stay missing. Preserve two identical calls in one frame,
	// but do not repeat them when the same complete parts arrive again.
	frame := `data: {"candidates":[{"content":{"parts":[{"functionCall":{"name":"clock","args":{}}},{"functionCall":{"name":"clock","args":{}}}]}}]}`
	items := responseItems([]byte(frame + "\n\n" + frame + "\n\n"))
	if len(items) != 2 || items[0].CallID != "" || items[1].CallID != "" {
		t.Fatalf("idless streamed calls = %+v", items)
	}
	sse := `data: {"candidates":[{"index":0,"content":{"parts":[{"functionCall":{"id":"a","name":"clock","args":{}}}]}},{"index":1,"content":{"parts":[{"functionCall":{"id":"a","name":"clock","args":{}}}]}}]}`
	if items = responseItems([]byte(sse)); len(items) != 2 {
		t.Fatalf("separate candidates collapsed = %+v", items)
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
	for _, part := range []string{`{"functionCall":null}`, `{"functionCall":"bad"}`, `{"functionResponse":[]}`, `{"thought":true,"text":{}}`} {
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
	req := put(`{"model":"gemini-2.5-pro","contents":[{"role":"user","parts":[{"text":"Weather?"}]}]}`)
	manifest, err := json.Marshal(Manifest{Format: "raw", Raw: req})
	if err != nil {
		t.Fatal(err)
	}
	an := &analytics{a: &Archive{blobs: blobs}}
	finished := time.Date(2026, 11, 21, 7, 0, 0, 0, time.UTC)
	var compact bytes.Buffer
	if err := json.Compact(&compact, []byte(geminiReply)); err != nil {
		t.Fatal(err)
	}
	for _, response := range []string{geminiReply, "data: " + compact.String() + "\n\ndata: [DONE]\n"} {
		rows, model, err := an.proxyMessages(context.Background(), proxyExchange{
			traceID: "gemini-offline", reqBody: put(string(manifest)), resBody: put(response),
			finished: sql.NullTime{Time: finished, Valid: true},
		})
		if err != nil || model != "gemini-2.5-pro" || len(rows) != 2 {
			t.Fatalf("analytics messages = %+v, %q, %v", rows, model, err)
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
}
