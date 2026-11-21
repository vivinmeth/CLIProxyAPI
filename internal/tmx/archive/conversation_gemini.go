package archive

import "github.com/MoeForge/moego/ojson"

// Gemini parts omit type. Only claim parts with a valid known shape, leaving malformed and
// future parts to otherItem so analytics keeps the original bytes.
func geminiPart(part ojson.Result, role string) (ConvItem, bool) {
	if part.Get("type").Exists() {
		return ConvItem{}, false
	}
	for _, key := range []string{"functionCall", "functionResponse"} {
		fn := part.Get(key)
		if !fn.IsObject() || fn.Get("name").Type != ojson.String {
			continue
		}
		name, id := fn.Get("name").String(), fn.Get("id").String()
		if key == "functionCall" {
			args := fn.Get("args").Raw
			return withData(withCall(item(role, "tool_call", args, name), id, false), args), true
		}
		result := fn.Get("response")
		return withData(withCall(item("tool", "tool_result", result.String(), name), id, false), result.Raw), true
	}
	if part.Get("thought").Type == ojson.True && part.Get("text").Type == ojson.String {
		return item(role, "thinking", part.Get("text").String(), ""), true
	}
	return ConvItem{}, false
}

func geminiCandidates(root ojson.Result) ojson.Result {
	candidates := root.Get("candidates")
	if !candidates.IsArray() {
		// Antigravity may retain the upstream response envelope.
		candidates = root.Get("response.candidates")
	}
	if !candidates.IsArray() {
		return ojson.Result{}
	}
	return candidates
}

func geminiContent(candidate ojson.Result) ojson.Result {
	content := candidate.Get("content")
	if !content.IsObject() || !content.Get("parts").IsArray() {
		return ojson.Result{}
	}
	return content
}

type geminiToolKey struct {
	candidate, kind, name, callID, data string
}

// Gemini text parts are deltas; tool parts are complete values and can repeat in later frames.
// Count equal tools per frame so two identical idless calls in one frame survive, while a
// repeated frame does not add them again. Without upstream ids, later identical calls cannot
// be distinguished from replays; keep ids empty rather than inventing links.
type geminiStream struct {
	items         []ConvItem
	seen          map[geminiToolKey]int
	lastCandidate string
}

func (s *geminiStream) add(root ojson.Result) {
	geminiCandidates(root).ForEach(func(position, candidate ojson.Result) bool {
		content := geminiContent(candidate)
		if !content.Exists() {
			return true
		}
		candidateID := firstNonEmptyStr(candidate.Get("index").Raw, position.String())
		counts := map[geminiToolKey]int{}
		for _, it := range normalizeMessageValue(content, "assistant") {
			if it.Kind == "tool_call" || it.Kind == "tool_result" {
				key := geminiToolKey{candidateID, it.Kind, it.Name, it.CallID, it.Data}
				counts[key]++
				if s.seen[key] >= counts[key] {
					continue
				}
				if s.seen == nil {
					s.seen = map[geminiToolKey]int{}
				}
				s.seen[key] = counts[key]
			}
			if n := len(s.items); n > 0 && s.lastCandidate == candidateID &&
				(it.Kind == "text" || it.Kind == "thinking") && s.items[n-1].Kind == it.Kind && s.items[n-1].Role == it.Role {
				last := &s.items[n-1]
				*last = item(last.Role, last.Kind, last.Text+it.Text, last.Name)
			} else {
				s.items = append(s.items, it)
			}
			s.lastCandidate = candidateID
		}
		return true
	})
}
