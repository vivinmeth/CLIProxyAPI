package archive

import "github.com/MoeForge/moego/ojson"

// Only skip known empty text/signature carriers. Extra fields or malformed values must
// survive as raw parts, and function/media parts carrying a signature still have content.
func emptyGeminiPart(part ojson.Result) bool {
	if !part.IsObject() {
		return false
	}
	known, carrier := true, false
	part.ForEach(func(key, value ojson.Result) bool {
		switch key.String() {
		case "text":
			known = value.Type == ojson.String && value.String() == ""
			carrier = true
		case "thoughtSignature", "thought_signature":
			known = value.Type == ojson.String
			carrier = true
		case "thought":
			known = value.IsBool()
		default:
			known = false
		}
		return known
	})
	return known && carrier
}

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
		return withData(withCall(item("tool", "tool_result", result.String(), name), id, result.Get("error").Exists()), result.Raw), true
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

// Gemini text parts are deltas. Deduplicate repeated complete tool parts only with an upstream
// call id; equal idless parts are separate calls/results, even across frames. Count tools per
// frame to preserve same-frame multiplicity and keep ids empty when the provider omits them.
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
			if it.CallID != "" && (it.Kind == "tool_call" || it.Kind == "tool_result") {
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
