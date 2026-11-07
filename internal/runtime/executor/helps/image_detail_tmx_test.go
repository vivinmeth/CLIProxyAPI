package helps

import (
	"context"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func TestImageRequestAndResponseDetail(t *testing.T) {
	body := []byte(`{"model":"gpt-image-2","prompt":"chest","size":"1024x1024","quality":"low","background":"transparent","output_format":"png","n":2}`)
	d := ImageRequestDetail(ImageOperation("/images/generations"), body)
	ObserveImagesResponse(d, []byte(`{"created":1,"data":[{"b64_json":"AAAA"}],"size":"1536x1024","quality":"medium","background":"transparent","output_format":"png"}`))
	want := usage.ImageDetail{Operation: "generate", RequestedSize: "1024x1024", RequestedQuality: "low",
		RequestedBackground: "transparent", RequestedFormat: "png", RequestedN: 2, ReturnedCount: 1,
		ReturnedSize: "1536x1024", ReturnedQuality: "medium", ReturnedBackground: "transparent", ReturnedFormat: "png"}
	if *d != want {
		t.Fatalf("detail = %+v", *d)
	}

	edit := ImageRequestDetail(ImageOperation("/v1/images/edits"), []byte(`{"prompt":"open","images":[{"image_url":"data:a"},{"image_url":"data:b"}],"mask":{"image_url":"data:m"}}`))
	if edit.Operation != "edit" || edit.InputImages != 2 || !edit.Mask || edit.RequestedSize != "" || edit.RequestedN != 0 {
		t.Fatalf("edit = %+v", *edit)
	}
	if d := ImageRequestDetail("generate", []byte("not json")); d == nil || d.Operation != "generate" {
		t.Fatalf("non-JSON body = %+v", d)
	}
}

func TestImageStreamObserverAcrossChunks(t *testing.T) {
	o := &ImageStreamObserver{Detail: &usage.ImageDetail{Operation: "generate"}}
	big := strings.Repeat("A", 100_000)
	stream := "event: image_generation.partial_image\ndata: {\"type\":\"image_generation.partial_image\",\"b64_json\":\"" + big + "\"}\n\n" +
		"event: image_generation.completed\ndata: {\"type\":\"image_generation.completed\",\"b64_json\":\"" + big +
		"\",\"size\":\"1024x1536\",\"quality\":\"high\",\"background\":\"opaque\",\"output_format\":\"webp\"}\n\n"
	completed := 0
	for i := 0; i < len(stream); i += 32 * 1024 {
		if o.Write([]byte(stream[i:min(i+32*1024, len(stream))])) {
			completed++
		}
	}
	d := o.Detail
	if completed != 1 || d.ReturnedCount != 1 || d.ReturnedSize != "1024x1536" || d.ReturnedQuality != "high" ||
		d.ReturnedBackground != "opaque" || d.ReturnedFormat != "webp" {
		t.Fatalf("completed %d, detail %+v", completed, *d)
	}
}

func TestReporterCarriesImageDetail(t *testing.T) {
	r := &UsageReporter{model: "gpt-5.4-mini"}
	main := r.buildRecord(usage.Detail{InputTokens: 5}, false)
	if main.Image != nil {
		t.Fatalf("no image detail set, got %+v", main.Image)
	}
	d := &usage.ImageDetail{Operation: "generate", ReturnedCount: 1}
	r.SetImageDetail(d)
	d.ReturnedCount = 9 // the reporter keeps its own copy
	rec, ok := r.buildAdditionalModelRecord("gpt-image-2", usage.Detail{InputTokens: 1, OutputTokens: 2})
	if !ok || rec.Image == nil || rec.Image.ReturnedCount != 1 {
		t.Fatalf("image record = %+v", rec.Image)
	}
	failed := r.buildRecord(usage.Detail{}, true, failFromErrors(context.Canceled))
	if failed.Image == nil || failed.Image.Operation != "generate" {
		t.Fatalf("failed record = %+v", failed.Image)
	}
}
