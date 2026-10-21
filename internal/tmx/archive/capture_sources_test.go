package archive

import (
	"bytes"
	"context"
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
