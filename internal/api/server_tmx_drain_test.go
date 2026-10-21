package api

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/drainws"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
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
		var payload struct {
			Error struct{ Type, Code, Message string }
		}
		errDecode := json.NewDecoder(res.Body).Decode(&payload)
		_ = res.Body.Close()
		if res.Header.Get("Retry-After") != "5" || errDecode != nil || payload.Error.Type != "server_error" || payload.Error.Code != "server_draining" {
			t.Fatalf("drain response headers=%v payload=%+v err=%v", res.Header, payload, errDecode)
		}
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

func TestTMXDrainWebsocketIdleAndActiveGrace(t *testing.T) {
	for _, mode := range []string{"idle", "complete", "deadline"} {
		t.Run(mode, func(t *testing.T) {
			s := &Server{cfg: &config.Config{}}
			r := gin.New()
			r.Use(s.tmxDrainMiddleware())
			started, release, ended := make(chan struct{}), make(chan struct{}), make(chan struct{})
			r.GET("/ws", func(c *gin.Context) {
				defer close(ended)
				upgrader := websocket.Upgrader{}
				conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
				if err != nil {
					return
				}
				defer func() { _ = conn.Close() }()
				session := drainws.Register(c.Request.Context(), func() {
					_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseServiceRestart, "restart"), time.Now().Add(time.Second))
					_ = conn.Close()
				})
				if mode != "idle" {
					if !session.Begin() {
						return
					}
				}
				defer session.End()
				close(started)
				if mode == "complete" {
					<-release
					_ = conn.WriteMessage(websocket.TextMessage, []byte("complete"))
					return
				}
				_, _, _ = conn.ReadMessage()
			})
			ts := httptest.NewServer(r)
			defer ts.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(ts.URL, "http")+"/ws", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			<-started
			finished := make(chan struct{})
			grace := time.Second
			if mode == "deadline" {
				grace = 20 * time.Millisecond
			}
			go func() { s.drain.wait(context.Background(), 90*time.Second, grace); close(finished) }()
			if mode == "complete" {
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
				case <-finished:
					t.Fatal("active response closed before completion")
				default:
				}
				close(release)
				_, body, err := conn.ReadMessage()
				if err != nil || string(body) != "complete" {
					t.Fatalf("response=%s err=%v", body, err)
				}
			}
			_ = conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			_, _, err = conn.ReadMessage()
			var closeErr *websocket.CloseError
			if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseServiceRestart {
				t.Fatalf("wanted restart close frame: %v", err)
			}
			select {
			case <-finished:
			case <-time.After(time.Second):
				t.Fatal("WS held full HTTP drain")
			}
			<-ended
		})
	}
}

func TestTMXDrainRejectionContract(t *testing.T) {
	s := &Server{}
	s.drain.wait(context.Background(), 0, 0)
	r := gin.New()
	r.Use(s.tmxDrainMiddleware())
	r.GET("/v1/responses", func(c *gin.Context) { c.Status(200) })
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("GET", "/v1/responses", nil))
	var payload struct {
		Error struct{ Type, Code, Message string }
	}
	err := json.Unmarshal(rec.Body.Bytes(), &payload)
	if rec.Code != 503 || rec.Header().Get("Retry-After") != "5" || err != nil || payload.Error.Type != "server_error" || payload.Error.Code != "server_draining" || payload.Error.Message == "" {
		t.Fatalf("drain response status=%d headers=%v body=%s err=%v", rec.Code, rec.Header(), rec.Body.String(), err)
	}
}
