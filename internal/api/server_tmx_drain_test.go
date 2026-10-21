package api

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
)

func TestTMXDrainCompletesStreamAndRejectsNewRequests(t *testing.T) {
	s := &Server{cfg: &config.Config{}}
	r := gin.New()
	r.Use(s.tmxDrainMiddleware())
	started, release := make(chan struct{}), make(chan struct{})
	r.GET("/stream", func(c *gin.Context) {
		c.Writer.WriteHeader(http.StatusOK)
		_, _ = c.Writer.WriteString("first\n")
		c.Writer.Flush()
		close(started)
		<-release
		_, _ = c.Writer.WriteString("last\n")
	})
	ts := httptest.NewServer(r)
	defer ts.Close()
	s.server = ts.Config
	resp, err := http.Get(ts.URL + "/stream")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	<-started
	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop(context.Background()) }()
	// Synchronize on the drain transition without timing assumptions.
	deadline := time.After(3 * time.Second)
	for {
		s.drain.mu.Lock()
		draining := s.drain.draining
		s.drain.mu.Unlock()
		if draining {
			break
		}
		select {
		case <-deadline:
			t.Fatal("drain did not start")
		default:
		}
	}
	for _, path := range []string{"/healthz", "/stream"} {
		res, errGet := http.Get(ts.URL + path)
		if errGet != nil {
			t.Fatal(errGet)
		}
		_ = res.Body.Close()
		if res.StatusCode != 503 {
			t.Fatalf("%s = %d", path, res.StatusCode)
		}
	}
	select {
	case <-stopped:
		t.Fatal("stopped before active stream completed")
	default:
	}
	close(release)
	body, err := io.ReadAll(resp.Body)
	if err != nil || string(body) != "first\nlast\n" {
		t.Fatalf("body %q: %v", body, err)
	}
	if err = <-stopped; err != nil {
		t.Fatal(err)
	}
}

func TestTMXHealthServingUnauthenticated(t *testing.T) {
	s := NewServer(&config.Config{}, nil, nil, "")
	rec := httptest.NewRecorder()
	s.engine.ServeHTTP(rec, httptest.NewRequest("GET", "/healthz", nil))
	if rec.Code != 200 || rec.Body.String() != `{"status":"ok"}` {
		t.Fatalf("health: %d %s", rec.Code, rec.Body.String())
	}
}

func TestTMXDrainWaitsForHijackedWebsocketHandler(t *testing.T) {
	s := &Server{cfg: &config.Config{}}
	r := gin.New()
	r.Use(s.tmxDrainMiddleware())
	started, release := make(chan struct{}), make(chan struct{})
	r.GET("/ws", func(c *gin.Context) {
		conn, rw, err := c.Writer.Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = rw.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = rw.Flush()
		close(started)
		<-release
		_, _ = conn.Write([]byte("complete"))
	})
	ts := httptest.NewServer(r)
	defer ts.Close()
	s.server = ts.Config
	conn, err := net.Dial("tcp", ts.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	if _, err = io.WriteString(conn, "GET /ws HTTP/1.1\r\nHost: test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n"); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(conn)
	response, err := http.ReadResponse(reader, nil)
	if err != nil || response.StatusCode != 101 {
		t.Fatalf("upgrade: %v", err)
	}
	<-started
	stopped := make(chan error, 1)
	go func() { stopped <- s.Stop(context.Background()) }()
	// Wait for admission to close; the upgraded connection must stay alive meanwhile.
	for {
		s.drain.mu.Lock()
		draining := s.drain.draining
		s.drain.mu.Unlock()
		if draining {
			break
		}
		runtime.Gosched()
	}
	select {
	case <-stopped:
		t.Fatal("closed a live WebSocket handler")
	default:
	}
	close(release)
	payload, err := io.ReadAll(reader)
	if err != nil || string(payload) != "complete" {
		t.Fatalf("websocket: %q %v", payload, err)
	}
	if err = <-stopped; err != nil {
		t.Fatal(err)
	}
}

func TestTMXDrainZeroClosesActiveStreamImmediately(t *testing.T) {
	zero := 0
	s := &Server{cfg: &config.Config{Server: config.ServerConfig{ShutdownDrainTimeout: &zero}}}
	r := gin.New()
	r.Use(s.tmxDrainMiddleware())
	started, exited := make(chan struct{}), make(chan struct{})
	r.GET("/stream", func(c *gin.Context) { close(started); <-c.Request.Context().Done(); close(exited) })
	ts := httptest.NewServer(r)
	defer ts.Close()
	s.server = ts.Config
	done := make(chan struct{})
	go func() {
		defer close(done)
		res, _ := http.Get(ts.URL + "/stream")
		if res != nil {
			_ = res.Body.Close()
		}
	}()
	<-started
	if err := s.Stop(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("immediate close did not interrupt stream")
	}
	<-exited
}
