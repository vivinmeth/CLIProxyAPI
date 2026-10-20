package archive

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// Coding agents resend the whole conversation on every request. Storing bodies verbatim would
// store each conversation hundreds of times, so JSON bodies are split into a manifest that points
// at one blob per conversation element (message, input item, system block, tool). Unchanged
// elements hash the same on every turn and are stored once.
//
// Two edits make that work while staying lossless:
//   - cache_control markers move to the newest messages every turn; they are removed before
//     hashing and recorded in the manifest (path + raw value) so reconstruction puts them back.
//   - inline media (base64 images, PDFs, data: URLs) is decoded and stored as its own blob with
//     its real media type; the element keeps a placeholder and the manifest records where.
//
// Reconstruction is JSON-equivalent; unmodified elements keep their exact bytes (sjson edits in
// place, preserving key order and formatting).

const (
	manifestVersion = 1
	inlineMaxBytes  = 512
	mediaMinBytes   = 1024
)

// splitKeys are top-level keys whose array values are stored per element.
var splitKeys = map[string]bool{
	"messages": true, "input": true, "contents": true, "system": true, "tools": true,
}

// Manifest describes how to rebuild a JSON body from blobs.
type Manifest struct {
	V      int            `json:"v"`
	Format string         `json:"format"` // "json", "raw" or "sections"
	Raw    string         `json:"raw,omitempty"`
	Parts  []SectionPart  `json:"parts,omitempty"` // Format "sections", concatenated in order
	Keys   []ManifestKey  `json:"keys,omitempty"`
	Cache  []PathValue    `json:"cache_control,omitempty"`
	Media  []MediaRef     `json:"media,omitempty"`
	Stats  *ManifestStats `json:"stats,omitempty"`
}

// SectionPart is one piece of a "sections" body: either literal text (stored as a blob) or a
// JSON body (a nested manifest, split per element like any other JSON body).
type SectionPart struct {
	Text string `json:"t,omitempty"`
	Body string `json:"m,omitempty"`
}

// ManifestKey is one top-level key, in original order.
type ManifestKey struct {
	Key    string          `json:"k"`
	Inline json.RawMessage `json:"v,omitempty"`
	Blob   string          `json:"b,omitempty"`
	Items  []string        `json:"i,omitempty"`
	Array  bool            `json:"a,omitempty"` // value was an array (Items may be empty)
}

// PathValue is a value removed from element Item of key Key at sjson path Path.
type PathValue struct {
	Key   string          `json:"k"`
	Item  int             `json:"i"`
	Path  string          `json:"p"`
	Value json.RawMessage `json:"v"`
}

// MediaRef records media extracted from element Item of key Key at Path. Prefix is kept for
// data: URLs ("data:image/png;base64,").
type MediaRef struct {
	Key    string `json:"k"`
	Item   int    `json:"i"`
	Path   string `json:"p"`
	Blob   string `json:"b"`
	Prefix string `json:"x,omitempty"`
}

// ManifestStats are informational.
type ManifestStats struct {
	Bytes    int `json:"bytes"`
	Elements int `json:"elements"`
	Media    int `json:"media"`
}

const placeholderPrefix = "tmx-blob:"

// splitBody stores body as blobs and returns the manifest blob hash.
func (a *Archive) splitBody(ctx context.Context, body []byte, kind string) (string, error) {
	if len(body) == 0 {
		return "", nil
	}
	m := Manifest{V: manifestVersion, Format: "json", Stats: &ManifestStats{Bytes: len(body)}}
	trimmed := bytes.TrimSpace(body)
	if !gjson.ValidBytes(trimmed) || len(trimmed) == 0 || trimmed[0] != '{' {
		if parts := splitSections(body); parts != nil {
			return a.storeSections(ctx, body, parts, kind)
		}
		hash, err := a.blobs.Put(ctx, body, sniffMediaType(body), kind+".raw")
		if err != nil {
			return "", err
		}
		m.Format, m.Raw = "raw", hash
		return a.putJSON(ctx, m, kind+".manifest")
	}

	var failed error
	gjson.ParseBytes(trimmed).ForEach(func(key, value gjson.Result) bool {
		k := key.String()
		mk := ManifestKey{Key: k}
		switch {
		case splitKeys[k] && value.IsArray():
			mk.Array = true
			index := 0
			value.ForEach(func(_, item gjson.Result) bool {
				hash, err := a.storeElement(ctx, &m, k, index, []byte(item.Raw), kind)
				if err != nil {
					failed = err
					return false
				}
				mk.Items = append(mk.Items, hash)
				index++
				m.Stats.Elements++
				return true
			})
		case len(value.Raw) > inlineMaxBytes:
			hash, err := a.storeElement(ctx, &m, k, -1, []byte(value.Raw), kind)
			if err != nil {
				failed = err
				return false
			}
			mk.Blob = hash
		default:
			mk.Inline = json.RawMessage(value.Raw)
		}
		m.Keys = append(m.Keys, mk)
		return failed == nil
	})
	if failed != nil {
		return "", failed
	}
	return a.putJSON(ctx, m, kind+".manifest")
}

// storeElement strips cache_control, extracts media and stores one element.
func (a *Archive) storeElement(ctx context.Context, m *Manifest, key string, item int, raw []byte, kind string) (string, error) {
	edited := raw
	var cachePaths []string
	var media []leaf
	walkJSON(gjson.ParseBytes(raw), "", func(path string, v gjson.Result) bool {
		last := path
		if i := strings.LastIndexByte(path, '.'); i >= 0 {
			last = path[i+1:]
		}
		if last == "cache_control" {
			cachePaths = append(cachePaths, path)
			return false
		}
		if v.Type == gjson.String && len(v.Raw) >= mediaMinBytes {
			media = append(media, leaf{path: path, value: v.String()})
		}
		return true
	})
	for _, p := range cachePaths {
		m.Cache = append(m.Cache, PathValue{Key: key, Item: item, Path: p,
			Value: json.RawMessage(gjson.GetBytes(edited, p).Raw)})
		var err error
		if edited, err = sjson.DeleteBytes(edited, p); err != nil {
			return "", fmt.Errorf("strip cache_control: %w", err)
		}
	}
	for _, l := range media {
		data, mediaType, prefix, ok := decodeMedia(l.value, gjson.GetBytes(raw, siblingPath(l.path, "media_type")).String())
		if !ok {
			continue
		}
		hash, err := a.blobs.Put(ctx, data, mediaType, "media")
		if err != nil {
			return "", err
		}
		if edited, err = sjson.SetBytes(edited, l.path, placeholderPrefix+hash); err != nil {
			return "", fmt.Errorf("media placeholder: %w", err)
		}
		m.Media = append(m.Media, MediaRef{Key: key, Item: item, Path: l.path, Blob: hash, Prefix: prefix})
		m.Stats.Media++
	}
	return a.blobs.Put(ctx, edited, "application/json", kind+".element")
}

func (a *Archive) putJSON(ctx context.Context, v any, kind string) (string, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return "", err
	}
	return a.blobs.Put(ctx, data, "application/json", kind)
}

// rebuildBody reconstructs a body from its manifest blob.
func (a *Archive) rebuildBody(ctx context.Context, manifestHash string) ([]byte, error) {
	if manifestHash == "" {
		return nil, nil
	}
	raw, err := a.blobs.Get(ctx, manifestHash)
	if err != nil {
		return nil, err
	}
	var m Manifest
	if err = json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("manifest: %w", err)
	}
	if m.Format == "raw" {
		return a.blobs.Get(ctx, m.Raw)
	}
	if m.Format == "sections" {
		var out []byte
		for _, part := range m.Parts {
			var piece []byte
			var errPart error
			if part.Body != "" {
				piece, errPart = a.rebuildBody(ctx, part.Body)
			} else if part.Text != "" {
				piece, errPart = a.blobs.Get(ctx, part.Text)
			}
			if errPart != nil {
				return nil, errPart
			}
			out = append(out, piece...)
		}
		return out, nil
	}
	restore := func(key string, item int, hash string) ([]byte, error) {
		el, errGet := a.blobs.Get(ctx, hash)
		if errGet != nil {
			return nil, errGet
		}
		for _, md := range m.Media {
			if md.Key != key || md.Item != item {
				continue
			}
			data, errMedia := a.blobs.Get(ctx, md.Blob)
			if errMedia != nil {
				return nil, errMedia
			}
			if el, errMedia = sjson.SetBytes(el, md.Path, md.Prefix+base64.StdEncoding.EncodeToString(data)); errMedia != nil {
				return nil, errMedia
			}
		}
		for _, cc := range m.Cache {
			if cc.Key != key || cc.Item != item {
				continue
			}
			var errSet error
			if el, errSet = sjson.SetRawBytes(el, cc.Path, cc.Value); errSet != nil {
				return nil, errSet
			}
		}
		return el, nil
	}
	var out bytes.Buffer
	out.WriteByte('{')
	for i, k := range m.Keys {
		if i > 0 {
			out.WriteByte(',')
		}
		name, _ := json.Marshal(k.Key)
		out.Write(name)
		out.WriteByte(':')
		switch {
		case k.Array:
			out.WriteByte('[')
			for j, h := range k.Items {
				if j > 0 {
					out.WriteByte(',')
				}
				el, errEl := restore(k.Key, j, h)
				if errEl != nil {
					return nil, errEl
				}
				out.Write(el)
			}
			out.WriteByte(']')
		case k.Blob != "":
			el, errEl := restore(k.Key, -1, k.Blob)
			if errEl != nil {
				return nil, errEl
			}
			out.Write(el)
		default:
			out.Write(k.Inline)
		}
	}
	out.WriteByte('}')
	return out.Bytes(), nil
}

type leaf struct {
	path  string
	value string
}

// walkJSON visits every value with its sjson path; fn returns false to skip children.
func walkJSON(v gjson.Result, path string, fn func(path string, v gjson.Result) bool) {
	if path != "" && !fn(path, v) {
		return
	}
	switch {
	case v.IsObject():
		v.ForEach(func(key, child gjson.Result) bool {
			walkJSON(child, joinPath(path, escapePathKey(key.String())), fn)
			return true
		})
	case v.IsArray():
		i := 0
		v.ForEach(func(_, child gjson.Result) bool {
			walkJSON(child, joinPath(path, strconv.Itoa(i)), fn)
			i++
			return true
		})
	}
}

func joinPath(base, part string) string {
	if base == "" {
		return part
	}
	return base + "." + part
}

func escapePathKey(k string) string {
	var b strings.Builder
	for _, r := range k {
		switch r {
		case '.', '*', '?', '|', '#', '@', '\\', '!', '=', '<', '>', '%', ':':
			b.WriteByte('\\')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func siblingPath(path, name string) string {
	if i := strings.LastIndexByte(path, '.'); i >= 0 {
		return path[:i+1] + name
	}
	return name
}

// decodeMedia recognises data: URLs and long base64 strings (Anthropic image/document sources,
// Gemini inlineData, OpenAI input_image/input_file).
func decodeMedia(s, siblingMediaType string) (data []byte, mediaType, prefix string, ok bool) {
	if strings.HasPrefix(s, "data:") {
		comma := strings.IndexByte(s, ',')
		if comma < 0 || !strings.Contains(s[:comma], ";base64") {
			return nil, "", "", false
		}
		decoded, err := base64.StdEncoding.DecodeString(s[comma+1:])
		if err != nil {
			return nil, "", "", false
		}
		mt := strings.TrimSuffix(strings.TrimPrefix(s[:comma], "data:"), ";base64")
		return decoded, mt, s[:comma+1], true
	}
	if strings.ContainsAny(s, " \n\t{}\"") {
		return nil, "", "", false
	}
	decoded, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return nil, "", "", false
	}
	// Only treat it as media when re-encoding is exact, so reconstruction stays lossless.
	if base64.StdEncoding.EncodeToString(decoded) != s {
		return nil, "", "", false
	}
	mt := siblingMediaType
	if mt == "" {
		mt = sniffMediaType(decoded)
	}
	return decoded, mt, "", true
}

// sectionBodyMarker starts a JSON body inside a request-log text dump ("=== API REQUEST 1 ===",
// headers, then "Body:\n{...}"). Retried attempts repeat the block.
var sectionBodyMarker = []byte("\nBody:\n")

// textPart / bodyPart are the pieces splitSections finds; bodyPart bytes are a JSON object.
type sectionPiece struct {
	data []byte
	json bool
}

// splitSections cuts a text dump into literal text and embedded JSON object bodies. It returns nil
// when no embedded JSON body is found, so the caller falls back to storing the dump verbatim.
// Concatenating the pieces always reproduces body exactly.
func splitSections(body []byte) []sectionPiece {
	var pieces []sectionPiece
	found := false
	rest := body
	for {
		i := bytes.Index(rest, sectionBodyMarker)
		if i < 0 {
			break
		}
		start := i + len(sectionBodyMarker)
		if start >= len(rest) || rest[start] != '{' {
			pieces = append(pieces, sectionPiece{data: rest[:start]})
			rest = rest[start:]
			continue
		}
		end := jsonObjectEnd(rest[start:])
		if end < 0 {
			break
		}
		pieces = append(pieces, sectionPiece{data: rest[:start]}, sectionPiece{data: rest[start : start+end], json: true})
		rest = rest[start+end:]
		found = true
	}
	if !found {
		return nil
	}
	if len(rest) > 0 {
		pieces = append(pieces, sectionPiece{data: rest})
	}
	return pieces
}

// jsonObjectEnd returns the length of the JSON object at the start of b, or -1 when b does not
// start with a complete, valid object.
func jsonObjectEnd(b []byte) int {
	depth, inString, escaped := 0, false, false
	for i, c := range b {
		switch {
		case inString:
			switch {
			case escaped:
				escaped = false
			case c == '\\':
				escaped = true
			case c == '"':
				inString = false
			}
		case c == '"':
			inString = true
		case c == '{' || c == '[':
			depth++
		case c == '}' || c == ']':
			depth--
			if depth == 0 {
				if !gjson.ValidBytes(b[:i+1]) {
					return -1
				}
				return i + 1
			}
		}
	}
	return -1
}

// storeSections stores a text dump as text blobs plus per-element JSON manifests.
func (a *Archive) storeSections(ctx context.Context, body []byte, pieces []sectionPiece, kind string) (string, error) {
	m := Manifest{V: manifestVersion, Format: "sections", Stats: &ManifestStats{Bytes: len(body)}}
	for _, piece := range pieces {
		if len(piece.data) == 0 {
			continue
		}
		if piece.json {
			hash, err := a.splitBody(ctx, piece.data, kind)
			if err != nil {
				return "", err
			}
			m.Parts = append(m.Parts, SectionPart{Body: hash})
			continue
		}
		hash, err := a.blobs.Put(ctx, piece.data, "text/plain", kind+".text")
		if err != nil {
			return "", err
		}
		m.Parts = append(m.Parts, SectionPart{Text: hash})
	}
	return a.putJSON(ctx, m, kind+".manifest")
}
