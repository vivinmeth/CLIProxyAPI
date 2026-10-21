package requestlog

// Billing backfill and the client key -> machine ledger (tokenmaxx fork). Rows get their machine
// from the client key's name ("sk-tmx-<machine>-…"); keys without one (the first day's random
// keys) are named by fleet's ledger: SetKeyMachines stores it, fills the machine of existing rows
// from those keys and of every later row (Enqueue).

import (
	"context"
	"fmt"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
)

// KeyMachine maps a client key (by api_key_hash: the first 16 hex of the key's sha256) to a
// machine.
type KeyMachine struct {
	APIKeyHash string `json:"api_key_hash"`
	Machine    string `json:"machine"`
}

// KeyMachinesResult reports what SetKeyMachines changed.
type KeyMachinesResult struct {
	Stored      int   `json:"stored"`
	RowsUpdated int64 `json:"rows_updated"` // existing rows without a machine that got one
}

func (s *Store) loadKeyMachines(ctx context.Context) error {
	rows, err := s.db.QueryContext(ctx, `SELECT api_key_hash, machine FROM `+s.schema+`.key_machines`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	m := map[string]string{}
	for rows.Next() {
		var k, v string
		if err = rows.Scan(&k, &v); err != nil {
			return err
		}
		m[k] = v
	}
	if err = rows.Err(); err != nil {
		return err
	}
	s.keyMachines.Store(&m)
	return nil
}

// SetKeyMachines upserts ledger entries, then gives rows of those keys that have no machine one.
// Rows that already have a machine (from the key's name) are never changed.
func (s *Store) SetKeyMachines(ctx context.Context, entries []KeyMachine) (*KeyMachinesResult, error) {
	res := &KeyMachinesResult{}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	for i, e := range entries {
		e.APIKeyHash, e.Machine = strings.ToLower(strings.TrimSpace(e.APIKeyHash)), strings.TrimSpace(e.Machine)
		if len(e.APIKeyHash) != 16 || !isHex(e.APIKeyHash) || e.Machine == "" || len(e.Machine) > 64 {
			return nil, fmt.Errorf("entry %d: api_key_hash must be 16 hex characters and machine non-empty", i)
		}
		if _, err = tx.ExecContext(ctx, `INSERT INTO `+s.schema+`.key_machines (api_key_hash, machine) VALUES ($1, $2)
			ON CONFLICT (api_key_hash) DO UPDATE SET machine = EXCLUDED.machine, updated_at = now()`,
			e.APIKeyHash, e.Machine); err != nil {
			return nil, err
		}
		tag, errU := tx.ExecContext(ctx, `UPDATE `+s.table()+` SET machine = $2 WHERE api_key_hash = $1 AND machine = ''`,
			e.APIKeyHash, e.Machine)
		if errU != nil {
			return nil, errU
		}
		n, _ := tag.RowsAffected()
		res.RowsUpdated += n
		res.Stored++
	}
	if err = tx.Commit(); err != nil {
		return nil, err
	}
	if err = s.loadKeyMachines(ctx); err != nil {
		return res, err
	}
	return res, nil
}

// KeyMachines lists the ledger.
func (s *Store) KeyMachines(ctx context.Context) ([]KeyMachine, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT api_key_hash, machine FROM `+s.schema+`.key_machines ORDER BY machine, api_key_hash`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := []KeyMachine{}
	for rows.Next() {
		var e KeyMachine
		if err = rows.Scan(&e.APIKeyHash, &e.Machine); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// backfillBilling fills billing on proxied rows written before it existed (from auth_type and
// provider, like live rows), and auth_id on imported rows from the account in auth_label.
func (s *Store) backfillBilling() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	for _, stmt := range []string{
		`UPDATE ` + s.table() + ` SET billing = CASE
			WHEN provider ILIKE '%ollama%' THEN 'local'
			WHEN auth_type = 'oauth' THEN 'subscription'
			WHEN auth_type = 'apikey' THEN 'api_key' END
		WHERE billing = '' AND source = 'tmx-proxy'
			AND (provider ILIKE '%ollama%' OR auth_type IN ('oauth', 'apikey'))`,
		`UPDATE ` + s.table() + ` SET billing = 'local' WHERE billing = '' AND provider ILIKE '%ollama%'`,
		`UPDATE ` + s.table() + ` SET auth_id = auth_label WHERE auth_id = '' AND auth_label <> '' AND source LIKE 'extern:%'`,
	} {
		n, err := s.bulkExec(ctx, stmt)
		if err != nil {
			log.WithError(err).Warn("tmx request log: billing / account backfill failed")
			return
		}
		if n > 0 {
			log.Infof("tmx request log: billing / account backfill updated %d rows", n)
		}
	}
}

// bulkExec runs a many-row update as a bulk update: the analytics trigger journals one reconcile
// instead of an entry per row (see writePrices).
func (s *Store) bulkExec(ctx context.Context, stmt string, args ...any) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err = tx.ExecContext(ctx, `SET LOCAL tmx.analytics_bulk = 'on'`); err != nil {
		return 0, err
	}
	tag, err := tx.ExecContext(ctx, stmt, args...)
	if err != nil {
		return 0, err
	}
	n, _ := tag.RowsAffected()
	return n, tx.Commit()
}
