package api

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
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
	if rec.Code != 200 || rec.Body.String() != `{"status":"serving"}` {
		t.Fatalf("health: %d %s", rec.Code, rec.Body.String())
	}
}
