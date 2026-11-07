package helps

import (
	"bytes"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	"github.com/tidwall/gjson"
)

// Image request details for the request log (tokenmaxx fork; sdk/cliproxy/usage.ImageDetail).
// Executors read what an image call asked for from the body they send upstream and what came
// back from the response, and hand it to the reporter; every record the reporter builds after
// that carries it, failures included.

// tmxImageState is the reporter's image detail (embedded in UsageReporter).
type tmxImageState struct {
	imageMu sync.Mutex
	image   *usage.ImageDetail
}

// SetImageDetail attaches d (copied) to the records built from now on; nil detaches.
func (r *UsageReporter) SetImageDetail(d *usage.ImageDetail) {
	if r == nil {
		return
	}
	r.imageMu.Lock()
	r.image = d.Clone()
	r.imageMu.Unlock()
}

func (r *UsageReporter) imageDetail() *usage.ImageDetail {
	if r == nil {
		return nil
	}
	r.imageMu.Lock()
	defer r.imageMu.Unlock()
	return r.image.Clone()
}

// ImageOperation is "edit" for an images edit endpoint path, else "generate".
func ImageOperation(path string) string {
	if strings.HasSuffix(strings.TrimSpace(path), "/edits") {
		return "edit"
	}
	return "generate"
}

// ImageRequestDetail reads an Images API body (JSON, as sent upstream: edits arrive here
// already rewritten from multipart) for operation "generate" or "edit".
func ImageRequestDetail(operation string, body []byte) *usage.ImageDetail {
	d := &usage.ImageDetail{Operation: operation}
	if !gjson.ValidBytes(body) {
		return d
	}
	root := gjson.ParseBytes(body)
	d.RequestedSize = imageString(root, "size")
	d.RequestedQuality = imageString(root, "quality")
	d.RequestedBackground = imageString(root, "background")
	d.RequestedFormat = imageString(root, "output_format")
	if n := root.Get("n"); n.Type == gjson.Number {
		d.RequestedN = n.Int()
	}
	if images := root.Get("images"); images.IsArray() {
		d.InputImages = len(images.Array())
	} else if root.Get("image").Exists() {
		d.InputImages = 1
	}
	d.Mask = root.Get("mask").Exists()
	return d
}

// ObserveImagesResponse fills the returned fields from an Images API JSON response.
func ObserveImagesResponse(d *usage.ImageDetail, data []byte) {
	if d == nil || !gjson.ValidBytes(data) {
		return
	}
	root := gjson.ParseBytes(data)
	d.ReturnedCount = len(root.Get("data").Array())
	setReturned(d, root)
}

var (
	imageStreamCompleted = []byte(`.completed"`)
	imageStreamDataTag   = []byte("data:")
)

// imageStreamMaxLine caps one buffered SSE line (a completed event carries the whole image).
const imageStreamMaxLine = 64 << 20

// ImageStreamObserver counts the images of a streamed Images API response from its raw chunks,
// which split lines anywhere: "image_generation.completed" / "image_edit.completed" events.
type ImageStreamObserver struct {
	Detail *usage.ImageDetail
	line   []byte
	skip   bool // the current line is over the cap
}

// Write feeds one raw chunk and reports whether it completed an image.
func (o *ImageStreamObserver) Write(chunk []byte) bool {
	if o == nil || o.Detail == nil {
		return false
	}
	completed := false
	for len(chunk) > 0 {
		i := bytes.IndexByte(chunk, '\n')
		part := chunk
		if i >= 0 {
			part = chunk[:i]
		}
		if !o.skip {
			if len(o.line)+len(part) > imageStreamMaxLine {
				o.line, o.skip = nil, true
			} else {
				o.line = append(o.line, part...)
			}
		}
		if i < 0 {
			break
		}
		if !o.skip && o.observeLine(o.line) {
			completed = true
		}
		o.line, o.skip = o.line[:0], false
		chunk = chunk[i+1:]
	}
	return completed
}

func (o *ImageStreamObserver) observeLine(line []byte) bool {
	line = bytes.TrimSpace(line)
	if !bytes.HasPrefix(line, imageStreamDataTag) || !bytes.Contains(line, imageStreamCompleted) {
		return false
	}
	payload := bytes.TrimSpace(line[len(imageStreamDataTag):])
	if !gjson.ValidBytes(payload) {
		return false
	}
	root := gjson.ParseBytes(payload)
	switch root.Get("type").String() {
	case "image_generation.completed", "image_edit.completed":
	default:
		return false
	}
	o.Detail.ReturnedCount++
	if o.Detail.ReturnedCount == 1 {
		setReturned(o.Detail, root)
	}
	return true
}

// ImageToolDetail reads an image_generation tool call from a Responses request body and its
// response.completed event: the tool's options as requested, its image_generation_call outputs
// as returned. Nil when the body declares no such tool.
func ImageToolDetail(body, completed []byte) *usage.ImageDetail {
	var tool gjson.Result
	for _, t := range gjson.GetBytes(body, "tools").Array() {
		if t.Get("type").String() == "image_generation" {
			tool = t
			break
		}
	}
	if !tool.Exists() {
		return nil
	}
	d := &usage.ImageDetail{Operation: firstImageString(imageString(tool, "action"), "auto")}
	d.RequestedSize = imageString(tool, "size")
	d.RequestedQuality = imageString(tool, "quality")
	d.RequestedBackground = imageString(tool, "background")
	d.RequestedFormat = imageString(tool, "output_format")
	for _, item := range gjson.GetBytes(completed, "response.output").Array() {
		if item.Get("type").String() != "image_generation_call" {
			continue
		}
		d.ReturnedCount++
		if d.ReturnedCount == 1 {
			setReturned(d, item)
		}
	}
	return d
}

func setReturned(d *usage.ImageDetail, node gjson.Result) {
	d.ReturnedSize = imageString(node, "size")
	d.ReturnedQuality = imageString(node, "quality")
	d.ReturnedBackground = imageString(node, "background")
	d.ReturnedFormat = imageString(node, "output_format")
}

func imageString(node gjson.Result, path string) string {
	v := node.Get(path)
	if v.Type != gjson.String {
		return ""
	}
	return strings.TrimSpace(v.String())
}

func firstImageString(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}
