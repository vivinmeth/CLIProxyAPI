// Package sessionbindings persists session affinity independently of token storage.
package sessionbindings

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

type Store struct {
	db       *sql.DB
	table    string
	previous map[string]string
	mu       sync.Mutex
}

// Open shares the existing tmx database pool and follows its schema migration style.
func Open(ctx context.Context, db *sql.DB, schema string) (*Store, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	table := `"` + strings.ReplaceAll(schema, `"`, `""`) + `".session_bindings`
	_, err := db.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS `+table+` (
 session_key TEXT PRIMARY KEY, auth_id TEXT NOT NULL, aliases JSONB NOT NULL,
 expires_at TIMESTAMPTZ NOT NULL, updated_at TIMESTAMPTZ NOT NULL DEFAULT now())`)
	if err != nil {
		return nil, err
	}
	return &Store{db: db, table: table, previous: make(map[string]string)}, nil
}

func (s *Store) Load(ctx context.Context) ([]auth.SessionBinding, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.db.QueryContext(ctx, `SELECT auth_id,aliases,expires_at FROM `+s.table+` WHERE expires_at > now() ORDER BY session_key`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []auth.SessionBinding{}
	for rows.Next() {
		var binding auth.SessionBinding
		var aliases []byte
		if err = rows.Scan(&binding.AuthID, &aliases, &binding.ExpiresAt); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(aliases, &binding.Aliases); err != nil {
			return nil, err
		}
		out = append(out, binding)
	}
	return out, rows.Err()
}

// Save writes changed groups and deletes groups invalidated since the last successful save.
func (s *Store) Save(ctx context.Context, bindings []auth.SessionBinding) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := make(map[string]string, len(bindings))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, binding := range bindings {
		if len(binding.Aliases) == 0 || !binding.ExpiresAt.After(time.Now()) {
			continue
		}
		key := binding.Aliases[0]
		// PostgreSQL timestamps have microsecond resolution.
		binding.ExpiresAt = binding.ExpiresAt.UTC().Truncate(time.Microsecond)
		raw, errMarshal := json.Marshal(binding)
		if errMarshal != nil {
			return errMarshal
		}
		next[key] = string(raw)
		if s.previous[key] == string(raw) {
			continue
		}
		aliases, _ := json.Marshal(binding.Aliases)
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+s.table+` (session_key,auth_id,aliases,expires_at) VALUES ($1,$2,$3,$4)
   ON CONFLICT(session_key) DO UPDATE SET auth_id=EXCLUDED.auth_id,aliases=EXCLUDED.aliases,expires_at=EXCLUDED.expires_at,updated_at=now()`, key, binding.AuthID, aliases, binding.ExpiresAt); err != nil {
			return err
		}
	}
	for key := range s.previous {
		if _, ok := next[key]; !ok {
			if _, err = tx.ExecContext(ctx, `DELETE FROM `+s.table+` WHERE session_key=$1`, key); err != nil {
				return err
			}
		}
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM `+s.table+` WHERE expires_at <= now()`); err != nil {
		return err
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	s.previous = next
	return nil
}

type Worker struct {
	stop    chan struct{}
	done    chan struct{}
	once    sync.Once
	store   *Store
	manager *auth.Manager
	enabled func() bool
}

func Start(ctx context.Context, db *sql.DB, schema string, manager *auth.Manager, enabled func() bool) (*Worker, error) {
	store, err := Open(ctx, db, schema)
	if err != nil {
		return nil, err
	}
	w := &Worker{make(chan struct{}), make(chan struct{}), sync.Once{}, store, manager, enabled}
	if enabled() {
		if err = w.restore(ctx); err != nil {
			return nil, fmt.Errorf("restore session bindings: %w", err)
		}
	}
	go w.run()
	return w, nil
}
func (w *Worker) restore(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	bindings, err := w.store.Load(ctx)
	if err != nil {
		return err
	}
	// Track loaded keys too, so disabled/removed credentials are deleted on the next save.
	for _, b := range bindings {
		if len(b.Aliases) > 0 {
			w.store.previous[b.Aliases[0]] = "loaded"
		}
	}
	w.manager.RestoreSessionBindings(bindings)
	return nil
}
func (w *Worker) flush() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.store.Save(ctx, w.manager.SnapshotSessionBindings()); err != nil {
		log.WithError(err).Warn("tmx: session binding persistence failed")
	}
}
func (w *Worker) run() {
	defer close(w.done)
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	wasEnabled := w.enabled()
	for {
		select {
		case <-ticker.C:
			enabled := w.enabled()
			if enabled && !wasEnabled {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				err := w.restore(ctx)
				cancel()
				if err != nil {
					log.WithError(err).Warn("tmx: session binding restore failed")
					continue
				}
			}
			if enabled {
				w.flush()
			}
			wasEnabled = enabled
		case <-w.stop:
			if w.enabled() {
				w.flush()
			}
			return
		}
	}
}
func (w *Worker) Close() {
	if w == nil {
		return
	}
	w.once.Do(func() { close(w.stop); <-w.done })
}

// CloseContext bounds the final binding save by the service shutdown window.
func (w *Worker) CloseContext(ctx context.Context) {
	if w == nil {
		return
	}
	done := make(chan struct{})
	go func() { w.Close(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
	}
}

// FlushContext checkpoints bindings while the service still accepts completions.
func (w *Worker) FlushContext(ctx context.Context) error {
	if w == nil || !w.enabled() {
		return nil
	}
	return w.store.Save(ctx, w.manager.SnapshotSessionBindings())
}
