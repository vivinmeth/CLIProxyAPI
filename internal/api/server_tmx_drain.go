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
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/drainws"
)

// tmxDrain tracks handler lifetimes, including handlers serving hijacked WebSockets.
// The same lock gates admission and the transition to draining.
type tmxDrain struct {
	mu       sync.Mutex
	draining bool
	closed   bool
	active   int
	done     chan struct{}
	conns    map[net.Conn]*drainws.Session
}

func (s *Server) tmxDrainMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		s.drain.mu.Lock()
		draining := s.drain.draining
		if draining {
			s.drain.mu.Unlock()
			c.Header("Retry-After", "5")
			if c.Request.Method == http.MethodHead {
				c.AbortWithStatus(http.StatusServiceUnavailable)
			} else {
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": gin.H{"message": "Server is draining; retry shortly", "type": "server_error", "code": "server_draining"}})
			}
			return
		}

		s.drain.active++
		writer := &tmxDrainWriter{ResponseWriter: c.Writer, drain: &s.drain}
		c.Writer = writer
		c.Request = c.Request.WithContext(drainws.WithRegistrar(c.Request.Context(), writer.registerWebsocket))
		s.drain.mu.Unlock()
		defer func() {
			s.drain.mu.Lock()
			defer s.drain.mu.Unlock()
			if writer.conn != nil {
				delete(s.drain.conns, writer.conn)
			} else {
				s.drain.active--
			}
			s.drain.signalDoneLocked()
		}()
		c.Next()
	}
}

func (d *tmxDrain) wait(ctx context.Context, timeout, websocketGrace time.Duration) {
	d.mu.Lock()
	if !d.draining {
		d.draining = true
		d.done = make(chan struct{})
		d.signalDoneLocked()
	}
	done := d.done
	sessions := make([]*drainws.Session, 0, len(d.conns))
	for _, session := range d.conns {
		if session != nil {
			sessions = append(sessions, session)
		}
	}
	d.mu.Unlock()
	// A socket gets its own deadline even while HTTP streams continue draining.
	timerWS := time.AfterFunc(max(0, min(timeout, websocketGrace)), d.closeHijacked)
	defer timerWS.Stop()
	for _, session := range sessions {
		session.Drain()
	}
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
			w.drain.conns = make(map[net.Conn]*drainws.Session)
		}
		w.drain.conns[conn] = nil
		w.drain.active--
		w.conn = conn
		w.drain.mu.Unlock()
	}
	return conn, rw, err
}
func (d *tmxDrain) signalDoneLocked() {
	if d.draining && d.active == 0 && len(d.conns) == 0 {
		select {
		case <-d.done:
		default:
			close(d.done)
		}
	}
}

func (w *tmxDrainWriter) registerWebsocket(session *drainws.Session) {
	d := w.drain
	d.mu.Lock()
	closed, draining := d.closed, d.draining
	if !closed {
		d.conns[w.conn] = session
	}
	d.mu.Unlock()
	if closed {
		session.Close()
	} else if draining {
		session.Drain()
	}
}

func (d *tmxDrain) closeHijacked() {
	d.mu.Lock()
	d.closed = true
	conns := d.conns
	d.conns = nil
	d.mu.Unlock()
	// Control writes can block briefly; never hold the admission mutex for I/O.
	var wg sync.WaitGroup
	for conn, session := range conns {
		wg.Go(func() {
			if session != nil {
				session.Close()
			} else {
				_ = conn.Close()
			}
		})
	}
	wg.Wait()
	d.mu.Lock()
	d.signalDoneLocked()
	d.mu.Unlock()
}

func (w *tmxDrainWriter) FlushError() error           { return httpwire.FlushResponse(w.ResponseWriter) }
func (w *tmxDrainWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
