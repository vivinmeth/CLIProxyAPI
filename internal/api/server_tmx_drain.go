package api

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/httpwire"
)

// tmxDrain tracks handler lifetimes, including handlers serving hijacked WebSockets.
// The same lock gates admission and the transition to draining.
type tmxDrain struct {
	mu       sync.Mutex
	draining bool
	closed   bool
	active   int
	done     chan struct{}
	conns    map[net.Conn]struct{}
}

func (s *Server) tmxDrainMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		s.drain.mu.Lock()
		draining := s.drain.draining
		if draining {
			s.drain.mu.Unlock()
			if c.Request.Method == http.MethodHead {
				c.AbortWithStatus(http.StatusServiceUnavailable)
			} else {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"status": "draining"})
			}
			return
		}

		s.drain.active++
		writer := &tmxDrainWriter{ResponseWriter: c.Writer, drain: &s.drain}
		c.Writer = writer
		s.drain.mu.Unlock()
		defer func() {
			s.drain.mu.Lock()
			defer s.drain.mu.Unlock()
			if writer.conn != nil {
				delete(s.drain.conns, writer.conn)
			}
			s.drain.active--
			if s.drain.draining && s.drain.active == 0 {
				close(s.drain.done)
			}
		}()
		c.Next()
	}
}

func (d *tmxDrain) wait(ctx context.Context, timeout time.Duration) {
	d.mu.Lock()
	if !d.draining {
		d.draining = true
		d.done = make(chan struct{})
		if d.active == 0 {
			close(d.done)
		}
	}
	done := d.done
	d.mu.Unlock()
	if timeout <= 0 {
		return
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-ctx.Done():
	case <-timer.C:
	}
}

type tmxDrainWriter struct {
	gin.ResponseWriter
	drain *tmxDrain
	conn  net.Conn
}

func (w *tmxDrainWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := w.ResponseWriter.Hijack()
	if err == nil {
		w.drain.mu.Lock()
		if w.drain.closed {
			w.drain.mu.Unlock()
			_ = conn.Close()
			return nil, nil, net.ErrClosed
		}
		if w.drain.conns == nil {
			w.drain.conns = make(map[net.Conn]struct{})
		}
		w.drain.conns[conn] = struct{}{}
		w.conn = conn
		w.drain.mu.Unlock()
	}
	return conn, rw, err
}
func (d *tmxDrain) closeHijacked() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for conn := range d.conns {
		_ = conn.Close()
		delete(d.conns, conn)
	}
}

func (w *tmxDrainWriter) FlushError() error           { return httpwire.FlushResponse(w.ResponseWriter) }
func (w *tmxDrainWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
