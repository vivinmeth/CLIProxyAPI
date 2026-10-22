// Package spooltest helps integration tests simulate a database outage: a TCP proxy in front of
// the throwaway Postgres that can be taken down (listener closed, live connections cut) and
// brought back on the same address.
package spooltest

import (
	"io"
	"net"
	"net/url"
	"sync"
	"testing"
)

// Proxy forwards connections to a target address while up.
type Proxy struct {
	t      testing.TB
	target string
	addr   string

	mu    sync.Mutex
	ln    net.Listener
	conns map[net.Conn]bool
}

// NewProxy starts a proxy to target (host:port) on a free local port.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &Proxy{t: t, target: target, addr: ln.Addr().String(), conns: map[net.Conn]bool{}}
	p.serve(ln)
	t.Cleanup(p.Down)
	return p
}

// DSN rewrites a postgres:// DSN to go through a new proxy.
func DSN(t testing.TB, dsn string) (string, *Proxy) {
	t.Helper()
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		t.Fatalf("spooltest: need a postgres:// URL DSN, got an unparsable one")
	}
	p := NewProxy(t, u.Host)
	u.Host = p.addr
	return u.String(), p
}

func (p *Proxy) serve(ln net.Listener) {
	p.mu.Lock()
	p.ln = ln
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go p.forward(c)
		}
	}()
}

func (p *Proxy) forward(c net.Conn) {
	up, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = c.Close()
		return
	}
	p.mu.Lock()
	if p.ln == nil {
		p.mu.Unlock()
		_ = c.Close()
		_ = up.Close()
		return
	}
	p.conns[c], p.conns[up] = true, true
	p.mu.Unlock()
	go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
	_, _ = io.Copy(c, up)
	_ = c.Close()
}

// Down refuses new connections and cuts the open ones.
func (p *Proxy) Down() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
		p.ln = nil
	}
	for c := range p.conns {
		_ = c.Close()
		delete(p.conns, c)
	}
}

// Up listens again on the same address (no-op while up).
func (p *Proxy) Up() {
	p.t.Helper()
	p.mu.Lock()
	up := p.ln != nil
	p.mu.Unlock()
	if up {
		return
	}
	ln, err := net.Listen("tcp", p.addr)
	if err != nil {
		p.t.Fatal(err)
	}
	p.serve(ln)
}
