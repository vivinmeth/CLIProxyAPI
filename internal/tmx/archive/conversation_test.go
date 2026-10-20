package archive

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
	"github.com/tidwall/gjson"
)

func kinds(items []ConvItem) []string {
	var out []string
	for _, it := range items {
		out = append(out, it.Role+":"+it.Kind)
	}
	return out
}

func TestNormalizeAnthropicRequestAndStream(t *testing.T) {
	req := `{"model":"claude-opus-5-5","system":[{"type":"text","text":"You are Claude Code."}],
	"messages":[{"role":"user","content":[{"type":"text","text":"list files"},{"type":"image","source":{"type":"base64","data":"x"}}]},
	{"role":"assistant","content":[{"type":"thinking","thinking":"hmm"},{"type":"tool_use","name":"Bash","input":{"cmd":"ls"}}]},
	{"role":"user","content":[{"type":"tool_result","content":[{"type":"text","text":"a.go"}]}]}]}`
	got := kinds(normalizeRequest([]byte(req)))
	want := "[system:text user:text user:image assistant:thinking assistant:tool_call tool:tool_result]"
	if fmtSlice(got) != want {
		t.Fatalf("request items = %v, want %v", got, want)
	}
	sse := "event: message_start\ndata: {\"type\":\"message_start\"}\n\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":0,\"delta\":{\"type\":\"thinking_delta\",\"thinking\":\"ok \"}}\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"Here \"}}\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":1,\"delta\":{\"type\":\"text_delta\",\"text\":\"you go\"}}\n" +
		"data: {\"type\":\"content_block_start\",\"index\":2,\"content_block\":{\"type\":\"tool_use\",\"name\":\"Read\"}}\n" +
		"data: {\"type\":\"content_block_delta\",\"index\":2,\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"p\\\":1}\"}}\n"
	resp := responseItems([]byte(sse))
	if len(resp) != 3 || resp[0].Kind != "thinking" || resp[1].Text != "Here you go" || resp[2].Name != "Read" || resp[2].Text != `{"p":1}` {
		t.Fatalf("stream items = %+v", resp)
	}
}

func TestNormalizeOpenAIResponses(t *testing.T) {
	req := `{"model":"gpt-5.6-sol","instructions":"be brief","input":[
	{"type":"message","role":"user","content":[{"type":"input_text","text":"run date"}]},
	{"type":"function_call","name":"shell","arguments":"{\"cmd\":\"date\"}"},
	{"type":"function_call_output","output":"Tue"}]}`
	if got := fmtSlice(kinds(normalizeRequest([]byte(req)))); got != "[system:text user:text assistant:tool_call tool:tool_result]" {
		t.Fatalf("responses request = %v", got)
	}
	sse := "data: {\"type\":\"response.output_text.delta\",\"delta\":\"done\"}\n"
	if r := responseItems([]byte(sse)); len(r) != 1 || r[0].Text != "done" {
		t.Fatalf("responses stream = %+v", r)
	}
	jsonResp := `{"content":[{"type":"text","text":"hi"}]}`
	if r := responseItems([]byte(jsonResp)); len(r) != 1 || r[0].Role != "assistant" || r[0].Text != "hi" {
		t.Fatalf("json response = %+v", r)
	}
	if r := normalizeMessage(gjson.Parse(`{"role":"user","content":"plain"}`), ""); r[0].Text != "plain" {
		t.Fatalf("string content = %+v", r)
	}
}

func fmtSlice(s []string) string {
	out := "["
	for i, v := range s {
		if i > 0 {
			out += " "
		}
		out += v
	}
	return out + "]"
}

func TestConversationProxyAndImported(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx := context.Background()
	start := time.Now().Add(-time.Minute)
	turn := func(trace, body, resp string, at time.Time) {
		ex := &capture.Exchange{TraceID: trace, SessionID: "s-proxy", URL: "/v1/messages", Method: "POST",
			StartedAt: at, FinishedAt: at.Add(time.Second), Status: 200, Streaming: true,
			RequestBody: []byte(body), ResponseBody: []byte(resp)}
		if err := a.storeExchange(ctx, ex); err != nil {
			t.Fatal(err)
		}
	}
	sse := func(text string) string {
		return `data: {"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"` + text + `"}}` + "\n"
	}
	turn("t1", `{"model":"claude-opus-5-5","messages":[{"role":"user","content":"hi"}]}`, sse("hello"), start)
	turn("t2", `{"model":"claude-opus-5-5","messages":[{"role":"user","content":"hi"},{"role":"assistant","content":"hello"},{"role":"user","content":"more"}]}`, sse("sure"), start.Add(10*time.Second))
	// A tiny side request (title generation) later in the same session must not be picked.
	turn("t3", `{"model":"claude-haiku-5-5","messages":[{"role":"user","content":"title?"}]}`, sse("Greeting"), start.Add(20*time.Second))

	conv, err := a.Conversation(ctx, "s-proxy", "", "")
	if err != nil {
		t.Fatal(err)
	}
	if conv.Kind != "proxy" || conv.TraceID != "t2" || len(conv.Exchanges) != 3 || len(conv.Items) != 4 ||
		conv.Items[3].Text != "sure" || conv.Items[3].Role != "assistant" || conv.Model != "claude-opus-5-5" {
		t.Fatalf("proxy conversation = %+v", conv)
	}
	byTrace, err := a.Conversation(ctx, "s-proxy", "", "t3")
	if err != nil || byTrace.TraceID != "t3" || byTrace.Items[len(byTrace.Items)-1].Text != "Greeting" {
		t.Fatalf("by trace = %+v %v", byTrace, err)
	}

	ts := time.Now().UTC().Add(-time.Hour)
	if _, err = a.IngestSession(ctx, SessionIngest{Harness: "opencode", ExternalID: "oc-1", Title: "Fix bug",
		Messages: []MessageIngest{
			{ExternalID: "m1", Seq: 1, Role: "user", TS: &ts, Message: json.RawMessage(`{"role":"user","content":"fix it"}`)},
			{ExternalID: "m2", Seq: 2, Role: "assistant", TS: &ts, Model: "gpt-5.6-sol",
				Message: json.RawMessage(`{"role":"assistant","content":[{"type":"text","text":"done"}]}`)},
		}}); err != nil {
		t.Fatal(err)
	}
	imp, err := a.Conversation(ctx, "oc-1", "", "")
	if err != nil || imp.Kind != "imported" || imp.SessionID != "opencode:oc-1" || imp.Title != "Fix bug" ||
		len(imp.Items) != 2 || imp.Items[1].Model != "gpt-5.6-sol" || imp.Items[1].Text != "done" {
		t.Fatalf("imported = %+v %v", imp, err)
	}
	if _, err = a.Conversation(ctx, "nope", "", ""); !errors.Is(err, ErrNoConversation) {
		t.Fatalf("unknown session err = %v", err)
	}
}
