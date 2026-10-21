package archive

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// ConvItem is one displayable piece of a conversation, normalised across Anthropic Messages,
// OpenAI Responses / Chat Completions and imported harness transcripts.
type ConvItem struct {
	Role string `json:"role"` // system | user | assistant | tool | event
	Kind string `json:"kind"` // text | thinking | tool_call | tool_result | image | other
	Text string `json:"text,omitempty"`
	Name string `json:"name,omitempty"` // tool name
	// CallID links a tool_call to its tool_result (Anthropic tool_use id, OpenAI call_id).
	CallID  string     `json:"call_id,omitempty"`
	IsError bool       `json:"is_error,omitempty"` // tool_result reported an error
	TS      *time.Time `json:"ts,omitempty"`
	Model   string     `json:"model,omitempty"`
	// Data is the item's structured form as JSON, untruncated: a tool call's arguments, or the
	// raw block of an "other" item. Only analytics stores it.
	Data string `json:"-"`
}

// ConvExchange is one captured proxied request of the session.
type ConvExchange struct {
	TraceID      string    `json:"trace_id"`
	TS           time.Time `json:"ts"`
	URL          string    `json:"url"`
	Status       int       `json:"status"`
	RequestBytes int64     `json:"request_bytes"`
}

// Conversation is what GET /tmx/archive/conversation returns.
type Conversation struct {
	Kind      string         `json:"kind"` // imported | proxy
	SessionID string         `json:"session_id"`
	Harness   string         `json:"harness,omitempty"`
	Title     string         `json:"title,omitempty"`
	Model     string         `json:"model,omitempty"`
	Items     []ConvItem     `json:"items"`
	Truncated bool           `json:"truncated,omitempty"`
	TraceID   string         `json:"trace_id,omitempty"` // proxy: the exchange the items come from
	Exchanges []ConvExchange `json:"exchanges,omitempty"`
	Source    string         `json:"source,omitempty"` // analytics | archive
}

// ErrNoConversation is returned when nothing is archived for the session.
var ErrNoConversation = errors.New("no archived conversation for this session")

const (
	convMaxItems    = 4000
	convMaxItemText = 64 << 10
	convMaxExch     = 500
	convMaxItemData = 4 << 20
)

// Conversation rebuilds a session for reading. Imported sessions come from their stored messages
// (harness narrows which harness:id to use). Proxied sessions come from the captured exchange with
// the largest request (the main thread's latest turn carries the whole conversation) plus its
// response; traceID picks a specific exchange instead.
// kind "proxy" or "imported" forces one source; "" prefers the imported transcript when both exist.
func (a *Archive) Conversation(ctx context.Context, sessionID, harness, traceID, kind string) (*Conversation, error) {
	sessionID = strings.TrimSpace(sessionID)
	if sessionID == "" && traceID == "" {
		return nil, fmt.Errorf("session_id or trace_id is required")
	}
	// Served from the analytics DB when the session is extracted there; a specific exchange
	// (trace_id) and sessions not extracted yet are rebuilt from the archive.
	if a.analytics != nil && traceID == "" {
		conv, ok, err := a.analytics.conversation(ctx, sessionID, harness, kind)
		if err != nil {
			// The archive is the source of truth; an analytics outage must not break reading.
			log.WithError(err).Warn("tmx analytics: conversation read failed, using the archive")
		} else if ok {
			return conv, nil
		}
	}
	if kind == "imported" {
		return a.importedConversation(ctx, sessionID, harness)
	}
	if traceID == "" && kind != "proxy" {
		if conv, err := a.importedConversation(ctx, sessionID, harness); err == nil || !errors.Is(err, ErrNoConversation) {
			return conv, err
		}
	}
	return a.proxyConversation(ctx, sessionID, traceID)
}

func (a *Archive) importedConversation(ctx context.Context, sessionID, harness string) (*Conversation, error) {
	var id, h, title string
	err := a.db.QueryRowContext(ctx, `SELECT id, harness, title FROM `+schema+`.sessions
		WHERE (id = $1 OR external_id = $1) AND ($2 = '' OR harness = $2) ORDER BY updated_at DESC LIMIT 1`,
		sessionID, harness).Scan(&id, &h, &title)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoConversation
	}
	if err != nil {
		return nil, err
	}
	rows, err := a.db.QueryContext(ctx, `SELECT ts, role, model, message FROM `+schema+`.session_messages
		WHERE session_id = $1 ORDER BY seq, ts LIMIT $2`, id, convMaxItems+1)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	conv := &Conversation{Kind: "imported", SessionID: id, Harness: h, Title: title, Items: []ConvItem{}}
	type stored struct {
		ts                sql.NullTime
		role, model, hash string
	}
	var msgs []stored
	for rows.Next() {
		var m stored
		if err = rows.Scan(&m.ts, &m.role, &m.model, &m.hash); err != nil {
			return nil, err
		}
		if len(msgs) >= convMaxItems {
			conv.Truncated = true
			break
		}
		msgs = append(msgs, m)
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	_ = rows.Close()
	hashes := make([]string, len(msgs))
	for i, m := range msgs {
		hashes[i] = m.hash
	}
	a.prefetchBodies(ctx, hashes)
	for _, m := range msgs {
		ts, role, model, hash := m.ts, m.role, m.model, m.hash
		body, errBody := a.rebuildBody(ctx, hash)
		if errBody != nil {
			return nil, fmt.Errorf("message: %w", errBody)
		}
		msg := gjson.GetBytes(body, "message")
		items := normalizeMessage(msg, role)
		for i := range items {
			if ts.Valid {
				t := ts.Time.UTC()
				items[i].TS = &t
			}
			if model != "" && items[i].Role == "assistant" {
				items[i].Model = model
			}
		}
		conv.Items = append(conv.Items, items...)
	}
	return conv, nil
}

func (a *Archive) proxyConversation(ctx context.Context, sessionID, traceID string) (*Conversation, error) {
	conv := &Conversation{Kind: "proxy", SessionID: sessionID, Items: []ConvItem{}}
	if sessionID != "" {
		rows, err := a.db.QueryContext(ctx, `SELECT trace_id, ts, url, status, request_bytes FROM `+schema+`.exchanges
			WHERE (session_id = $1 OR client_session_id = $1) AND method = 'POST' ORDER BY ts LIMIT $2`,
			sessionID, convMaxExch)
		if err != nil {
			return nil, err
		}
		for rows.Next() {
			var e ConvExchange
			if err = rows.Scan(&e.TraceID, &e.TS, &e.URL, &e.Status, &e.RequestBytes); err != nil {
				_ = rows.Close()
				return nil, err
			}
			conv.Exchanges = append(conv.Exchanges, e)
		}
		if err = rows.Close(); err != nil {
			return nil, err
		}
	}
	if traceID == "" {
		best := -1
		for i, e := range conv.Exchanges {
			if e.Status >= 200 && e.Status < 300 && (best < 0 || e.RequestBytes >= conv.Exchanges[best].RequestBytes) {
				best = i
			}
		}
		if best < 0 {
			return nil, ErrNoConversation
		}
		traceID = conv.Exchanges[best].TraceID
	}
	meta, err := scanExchange(a.db.QueryRowContext(ctx, `SELECT `+exchangeColumns+` FROM `+schema+`.exchanges
		WHERE trace_id = $1`, traceID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNoConversation
	}
	if err != nil {
		return nil, err
	}
	conv.TraceID = traceID
	a.prefetchBodies(ctx, []string{meta.RequestBody})
	a.blobs.Prefetch(ctx, []string{meta.ResponseBody})
	if conv.SessionID == "" {
		conv.SessionID = meta.SessionID
	}
	if meta.RequestBody != "" {
		req, errReq := a.rebuildBody(ctx, meta.RequestBody)
		if errReq != nil {
			return nil, errReq
		}
		conv.Model = gjson.GetBytes(req, "model").String()
		conv.Items = append(conv.Items, normalizeRequest(req)...)
	}
	if meta.ResponseBody != "" {
		resp, errResp := a.blobs.Get(ctx, meta.ResponseBody)
		if errResp != nil {
			return nil, errResp
		}
		for _, it := range responseItems(resp) {
			it.Model = conv.Model
			t := meta.TS
			if meta.FinishedAt != nil {
				t = *meta.FinishedAt
			}
			it.TS = &t
			conv.Items = append(conv.Items, it)
		}
	}
	if len(conv.Items) > convMaxItems {
		conv.Items, conv.Truncated = conv.Items[len(conv.Items)-convMaxItems:], true
	}
	return conv, nil
}

// normalizeRequest reads the conversation out of a request body (Anthropic system+messages,
// OpenAI Responses instructions+input, Chat Completions messages).
func normalizeRequest(body []byte) []ConvItem {
	var out []ConvItem
	root := gjson.ParseBytes(body)
	if sys := root.Get("system"); sys.Exists() {
		out = append(out, contentItems(sys, "system")...)
	}
	if ins := root.Get("instructions"); ins.Type == gjson.String && ins.String() != "" {
		out = append(out, item("system", "text", ins.String(), ""))
	}
	for _, key := range []string{"messages", "input", "contents"} {
		v := root.Get(key)
		if v.Type == gjson.String {
			out = append(out, item("user", "text", v.String(), ""))
			continue
		}
		v.ForEach(func(_, m gjson.Result) bool {
			out = append(out, normalizeMessage(m, "")...)
			return true
		})
	}
	return out
}

// normalizeMessage handles one message / input item in any of the supported shapes.
func normalizeMessage(m gjson.Result, fallbackRole string) []ConvItem {
	role := firstNonEmptyStr(m.Get("role").String(), fallbackRole, "event")
	if role == "model" {
		role = "assistant"
	}
	switch m.Get("type").String() {
	case "function_call", "custom_tool_call", "local_shell_call":
		args := firstNonEmptyStr(m.Get("arguments").String(), m.Get("input").String(), m.Get("action").Raw)
		return []ConvItem{withData(withCall(item("assistant", "tool_call", args, m.Get("name").String()), m.Get("call_id").String(), false), args)}
	case "function_call_output", "custom_tool_call_output", "local_shell_call_output":
		return []ConvItem{withCall(item("tool", "tool_result", outputText(m.Get("output")), ""), m.Get("call_id").String(), false)}
	case "reasoning":
		var parts []string
		m.Get("summary").ForEach(func(_, s gjson.Result) bool {
			parts = append(parts, s.Get("text").String())
			return true
		})
		return []ConvItem{item("assistant", "thinking", strings.Join(parts, "\n"), "")}
	}
	if m.Get("bubbleId").Exists() {
		return cursorBubble(m, fallbackRole)
	}
	content := m.Get("content")
	if !content.Exists() {
		content = m.Get("parts")
	}
	if !content.Exists() {
		if !m.IsObject() && !m.IsArray() && m.String() != "" {
			return []ConvItem{item(role, "text", m.String(), "")}
		}
		return []ConvItem{otherItem(role, m)}
	}
	items := contentItems(content, role)
	// Chat Completions tool calls live next to content.
	m.Get("tool_calls").ForEach(func(_, tc gjson.Result) bool {
		args := tc.Get("function.arguments").String()
		items = append(items, withData(withCall(item(role, "tool_call", args, tc.Get("function.name").String()), tc.Get("id").String(), false), args))
		return true
	})
	if role == "tool" && len(items) > 0 {
		for i := range items {
			items[i].Kind = "tool_result"
			if items[i].CallID == "" {
				items[i].CallID = m.Get("tool_call_id").String()
			}
		}
	}
	return items
}

// contentItems turns a content value (string or block array) into items.
func contentItems(content gjson.Result, role string) []ConvItem {
	if content.Type == gjson.String {
		return []ConvItem{item(role, "text", content.String(), "")}
	}
	var out []ConvItem
	content.ForEach(func(_, b gjson.Result) bool {
		if b.Type == gjson.String {
			out = append(out, item(role, "text", b.String(), ""))
			return true
		}
		switch t := b.Get("type").String(); t {
		case "text", "input_text", "output_text", "":
			text := b.Get("text").String()
			if text == "" && t == "" {
				out = append(out, otherItem(role, b))
			} else {
				out = append(out, item(role, "text", text, ""))
			}
		case "thinking", "redacted_thinking":
			out = append(out, item(role, "thinking", firstNonEmptyStr(b.Get("thinking").String(), "[redacted thinking]"), ""))
		case "reasoning": // OpenCode
			out = append(out, item(role, "thinking", b.Get("text").String(), ""))
		case "tool_use", "server_tool_use":
			out = append(out, withData(withCall(item(role, "tool_call", b.Get("input").Raw, b.Get("name").String()), b.Get("id").String(), false), b.Get("input").Raw))
		case "tool": // OpenCode: the call and its result in one part
			out = append(out, openCodeTool(b)...)
		case "step-start": // OpenCode: only a snapshot id
		case "file": // OpenCode attachment
			if strings.HasPrefix(b.Get("mime").String(), "image/") {
				out = append(out, item(role, "image", "["+firstNonEmptyStr(b.Get("filename").String(), "image")+"]", ""))
			} else {
				out = append(out, otherItem(role, b))
			}
		case "tool_result":
			out = append(out, withCall(item("tool", "tool_result", outputText(b.Get("content")), ""), b.Get("tool_use_id").String(), b.Get("is_error").Bool()))
		case "image", "input_image", "document", "input_file":
			out = append(out, item(role, "image", "["+t+"]", ""))
		default:
			out = append(out, otherItem(role, b))
		}
		return true
	})
	return out
}

// openCodeTool splits an OpenCode tool part (tool, callID, state{status, input, output, error})
// into a tool_call and, once it has run, its tool_result.
func openCodeTool(b gjson.Result) []ConvItem {
	st := b.Get("state")
	callID := b.Get("callID").String()
	input := st.Get("input").Raw
	out := []ConvItem{withData(withCall(item("assistant", "tool_call", input, b.Get("tool").String()), callID, false), input)}
	switch st.Get("status").String() {
	case "completed":
		out = append(out, withCall(item("tool", "tool_result", outputText(st.Get("output")), ""), callID, false))
	case "error":
		out = append(out, withCall(item("tool", "tool_result", st.Get("error").String(), ""), callID, true))
	}
	return out
}

// cursorBubble reads a Cursor chat bubble (type 1 = user, 2 = assistant): thinking, text, and a
// tool call with its result (toolFormerData). Everything else in a bubble is editor context.
func cursorBubble(m gjson.Result, fallbackRole string) []ConvItem {
	role := fallbackRole
	switch m.Get("type").Int() {
	case 1:
		role = "user"
	case 2:
		role = "assistant"
	}
	role = firstNonEmptyStr(role, "assistant")
	var out []ConvItem
	if t := m.Get("thinking.text").String(); strings.TrimSpace(t) != "" {
		out = append(out, item(role, "thinking", t, ""))
	}
	if t := m.Get("text").String(); strings.TrimSpace(t) != "" {
		out = append(out, item(role, "text", t, ""))
	}
	if tf := m.Get("toolFormerData"); tf.Get("name").String() != "" {
		args := firstNonEmptyStr(tf.Get("rawArgs").String(), tf.Get("params").String())
		callID := tf.Get("toolCallId").String()
		out = append(out, withData(withCall(item("assistant", "tool_call", args, tf.Get("name").String()), callID, false), args))
		if res := tf.Get("result").String(); res != "" || tf.Get("status").String() == "error" {
			status := tf.Get("status").String()
			out = append(out, withCall(item("tool", "tool_result", firstNonEmptyStr(res, tf.Get("error").Raw, status), ""),
				callID, status == "error"))
		}
	}
	return out
}

func outputText(v gjson.Result) string {
	if v.Type == gjson.String {
		return v.String()
	}
	var parts []string
	v.ForEach(func(_, b gjson.Result) bool {
		if s := b.Get("text").String(); s != "" {
			parts = append(parts, s)
		} else if b.Type == gjson.String {
			parts = append(parts, b.String())
		}
		return true
	})
	if len(parts) == 0 {
		return v.Raw
	}
	return strings.Join(parts, "\n")
}

// responseItems extracts the assistant's reply from a response body: a JSON response or an SSE
// stream (Anthropic content_block deltas, OpenAI Responses output_text deltas, Chat Completions
// choice deltas).
func responseItems(body []byte) []ConvItem {
	trimmed := bytes.TrimSpace(body)
	if len(trimmed) > 0 && trimmed[0] == '{' && gjson.ValidBytes(trimmed) {
		root := gjson.ParseBytes(trimmed)
		if c := root.Get("content"); c.IsArray() {
			return contentItems(c, "assistant")
		}
		var out []ConvItem
		root.Get("output").ForEach(func(_, o gjson.Result) bool {
			out = append(out, normalizeMessage(o, "assistant")...)
			return true
		})
		root.Get("choices").ForEach(func(_, ch gjson.Result) bool {
			out = append(out, normalizeMessage(ch.Get("message"), "assistant")...)
			return true
		})
		return out
	}
	var text, thinking strings.Builder
	tools := map[int]*ConvItem{}
	var toolOrder []int
	sc := bufio.NewScanner(bytes.NewReader(body))
	sc.Buffer(make([]byte, 1<<20), 16<<20)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		data, ok := strings.CutPrefix(line, "data:")
		if !ok {
			continue
		}
		ev := gjson.Parse(strings.TrimSpace(data))
		switch ev.Get("type").String() {
		case "content_block_start":
			if cb := ev.Get("content_block"); cb.Get("type").String() == "tool_use" {
				idx := int(ev.Get("index").Int())
				tools[idx] = &ConvItem{Role: "assistant", Kind: "tool_call", Name: cb.Get("name").String(), CallID: cb.Get("id").String()}
				toolOrder = append(toolOrder, idx)
			}
		case "content_block_delta":
			d := ev.Get("delta")
			switch d.Get("type").String() {
			case "text_delta":
				text.WriteString(d.Get("text").String())
			case "thinking_delta":
				thinking.WriteString(d.Get("thinking").String())
			case "input_json_delta":
				if t := tools[int(ev.Get("index").Int())]; t != nil {
					t.Text += d.Get("partial_json").String()
				}
			}
		case "response.output_text.delta":
			text.WriteString(ev.Get("delta").String())
		case "response.reasoning_summary_text.delta":
			thinking.WriteString(ev.Get("delta").String())
		case "response.output_item.done":
			if it := ev.Get("item"); strings.HasSuffix(it.Get("type").String(), "call") {
				idx := 1000 + len(toolOrder)
				tools[idx] = &ConvItem{Role: "assistant", Kind: "tool_call", Name: it.Get("name").String(),
					Text: firstNonEmptyStr(it.Get("arguments").String(), it.Get("input").String()), CallID: it.Get("call_id").String()}
				toolOrder = append(toolOrder, idx)
			}
		default:
			ev.Get("choices").ForEach(func(_, ch gjson.Result) bool {
				text.WriteString(ch.Get("delta.content").String())
				return true
			})
		}
	}
	var out []ConvItem
	if thinking.Len() > 0 {
		out = append(out, item("assistant", "thinking", thinking.String(), ""))
	}
	if text.Len() > 0 {
		out = append(out, item("assistant", "text", text.String(), ""))
	}
	for _, idx := range toolOrder {
		t := *tools[idx]
		out = append(out, withData(withCall(item(t.Role, t.Kind, t.Text, t.Name), t.CallID, false), t.Text))
	}
	return out
}

func withCall(it ConvItem, callID string, isError bool) ConvItem {
	it.CallID, it.IsError = callID, isError
	return it
}

// withData attaches raw as the item's structured form (not huge): JSON as is, free-form text (a
// Codex apply_patch input) as a JSON string.
func withData(it ConvItem, raw string) ConvItem {
	raw = strings.TrimSpace(raw)
	if raw == "" || len(raw) > convMaxItemData {
		return it
	}
	if gjson.Valid(raw) {
		it.Data = raw
	} else if b, err := json.Marshal(raw); err == nil {
		it.Data = string(b)
	}
	return it
}

// otherItem keeps a block nothing else understands: as text, and as data for JSON queries.
func otherItem(role string, b gjson.Result) ConvItem {
	return withData(item(role, "other", b.Raw, ""), b.Raw)
}

func item(role, kind, text, name string) ConvItem {
	if len(text) > convMaxItemText {
		cut := convMaxItemText
		for cut > 0 && (text[cut]&0xC0) == 0x80 {
			cut--
		}
		text = text[:cut] + "\n… [truncated]"
	}
	return ConvItem{Role: role, Kind: kind, Text: text, Name: name}
}

func firstNonEmptyStr(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
