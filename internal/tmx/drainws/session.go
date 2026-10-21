// Package drainws connects WebSocket response activity to server shutdown without
// coupling protocol handlers to the API server.
package drainws

import (
	"context"
	"sync"
)

type registrarKey struct{}

func WithRegistrar(ctx context.Context, register func(*Session)) context.Context {
	return context.WithValue(ctx, registrarKey{}, register)
}

type Session struct {
	mu             sync.Mutex
	busy, draining bool
	close          func()
	once           sync.Once
}

func Register(ctx context.Context, closeSocket func()) *Session {
	s := &Session{close: closeSocket}
	if register, ok := ctx.Value(registrarKey{}).(func(*Session)); ok {
		register(s)
	}
	return s
}

// Begin rejects new turns once shutdown begins. End closes a completed turn
// during drain, rather than leaving its now-idle socket open until the deadline.
func (s *Session) Begin() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.draining {
		return false
	}
	s.busy = true
	return true
}
func (s *Session) End() {
	s.mu.Lock()
	s.busy = false
	closing := s.draining
	s.mu.Unlock()
	if closing {
		s.Close()
	}
}
func (s *Session) Drain() {
	s.mu.Lock()
	s.draining = true
	closing := !s.busy
	s.mu.Unlock()
	if closing {
		s.Close()
	}
}
func (s *Session) Close() { s.once.Do(s.close) }
