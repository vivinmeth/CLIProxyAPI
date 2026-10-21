package archive

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/capture"
)

func TestSpilledCaptureStreamsIntoArchiveAndRebuilds(t *testing.T) {
	a, _ := openTestArchive(t)
	dir := t.TempDir()
	capture.Configure(1024, 512, dir)
	defer capture.Configure(0, 0, "")
	request := []byte(`{"model":"opus","messages":[{"role":"user","content":"` + string(bytes.Repeat([]byte("hello"), 1000)))
	request = append(request, []byte(`"}]}`)...)
	response := bytes.Repeat([]byte("data: test\n\n"), 10000)
	ex := &capture.Exchange{TraceID: "spilled", URL: "/v1/messages", Method: "POST", Status: 200, StartedAt: time.Now(), RequestHeaders: map[string][]string{"X-Claude-Code-Session-Id": {"client-session"}}, ResponseHeaders: http.Header{"Content-Type": {"text/event-stream"}}, Bodies: map[string]*capture.Body{"request": capture.Capture(request, nil), "response": capture.Capture(response, nil)}}
	defer ex.Release()
	if !ex.Bodies["response"].Spilled() {
		t.Fatal("response must spill")
	}
	if err := a.storeExchange(context.Background(), ex); err != nil {
		t.Fatal(err)
	}
	result, err := a.GetExchange(context.Background(), ex.TraceID)
	if err != nil {
		t.Fatal(err)
	}
	if result["response_body"] != string(response) {
		t.Fatal("response changed")
	}
	meta := result["meta"].(*ExchangeMeta)
	rebuilt, err := a.rebuildBody(context.Background(), meta.RequestBody)
	if err != nil || !bytes.Equal(rebuilt, request) {
		t.Fatalf("request changed: %v", err)
	}
	if meta.RequestBytes != int64(len(request)) || meta.ResponseBytes != int64(len(response)) {
		t.Fatalf("sizes %+v", meta)
	}
	ex.Release()
	entries, err := os.ReadDir(dir + "/live")
	if err != nil || len(entries) != 0 {
		t.Fatalf("spools leaked: %d %v", len(entries), err)
	}
}

func TestArchiveDropReleasesCaptureSources(t *testing.T) {
	capture.Configure(1024, 512, t.TempDir())
	defer capture.Configure(0, 0, "")
	before := capture.LiveBytes()
	a := &Archive{queue: make(chan *capture.Exchange)}
	ex := &capture.Exchange{Bodies: map[string]*capture.Body{"response": capture.Capture([]byte("body"), nil)}}
	a.enqueue(ex)
	if capture.LiveBytes() != before {
		t.Fatal("queue drop leaked memory")
	}
	ex = &capture.Exchange{Bodies: map[string]*capture.Body{"response": capture.Capture([]byte("body"), nil)}}
	a.closing.Store(true)
	a.enqueue(ex)
	if capture.LiveBytes() != before {
		t.Fatal("closing drop leaked memory")
	}
}

func TestArchiveCheckpointWaitsForAcceptedExchangesAndKeepsIntakeOpen(t *testing.T) {
	a, _ := openTestArchive(t)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, id := range []string{"checkpoint-before", "checkpoint-after"} {
		a.enqueue(&capture.Exchange{TraceID: id, URL: "/v1/messages", Method: "POST", StartedAt: time.Now(), Status: 200, RequestBody: []byte(`{"model":"opus"}`)})
		if err := a.FlushContext(ctx); err != nil {
			t.Fatal(err)
		}
		if _, err := a.GetExchange(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestCapturePerBodyThresholdPreservesRequestDedup(t *testing.T) {
	a, _ := openTestArchive(t)
	capture.Configure(1024, 512, t.TempDir())
	defer capture.Configure(0, 0, "")
	group := &capture.Group{}
	payload := []byte(`{"messages":[{"role":"user","content":"` + string(bytes.Repeat([]byte("x"), 260)) + `"}]}`)
	ex := &capture.Exchange{Bodies: map[string]*capture.Body{"request": capture.CaptureInGroup(payload, nil, group), "upstream.request": capture.CaptureInGroup(payload, nil, group)}}
	defer ex.Release()
	var previous string
	for _, name := range []string{"request", "upstream.request"} {
		hash, err := a.putCapture(context.Background(), ex, name, nil, "application/json", "request", true)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := a.blobs.Get(context.Background(), hash)
		if err != nil {
			t.Fatal(err)
		}
		var manifest Manifest
		if err = json.Unmarshal(raw, &manifest); err != nil {
			t.Fatal(err)
		}
		if manifest.Format != "json" || len(manifest.Keys) == 0 || len(manifest.Keys[0].Items) != 1 {
			t.Fatalf("small independent body lost section dedup: %+v", manifest)
		}
		if previous != "" && hash != previous {
			t.Fatal("request copies did not dedup")
		}
		previous = hash
	}
}
