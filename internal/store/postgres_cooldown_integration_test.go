package store

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// Run with TMX_COOLDOWN_TEST_DSN pointed at a disposable PostgreSQL 18 instance.
func TestPostgresCooldownRealRestartScopesAndRecovery(t *testing.T) {
	dsn := os.Getenv("TMX_COOLDOWN_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_COOLDOWN_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("cooldown_test_%d", time.Now().UnixNano())
	cfg := PostgresStoreConfig{DSN: dsn, Schema: schema, SpoolDir: t.TempDir()}
	s, err := NewPostgresStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = s.db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); _ = s.Close() }()
	if err = s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	manager := func(store *PostgresStore) *auth.Manager {
		m := auth.NewManager(nil, nil, nil)
		m.SetConfig(&config.Config{SaveCooldownStatus: true})
		for _, id := range []string{"credential", "model", "transient", "expired"} {
			if _, err := m.Register(ctx, &auth.Auth{ID: id, Provider: "claude"}); err != nil {
				t.Fatal(err)
			}
		}
		m.SetCooldownStateStore(store.CooldownStateStore())
		return m
	}
	m := manager(s)
	retry := 20 * time.Minute
	m.MarkResult(ctx, auth.Result{AuthID: "credential", Provider: "claude", Model: "opus", CredentialScope: true, RetryAfter: &retry, Error: &auth.Error{HTTPStatus: 429, Message: "quota"}})
	m.MarkResult(ctx, auth.Result{AuthID: "model", Provider: "claude", Model: "friendly-alias", RetryAfter: &retry, Error: &auth.Error{HTTPStatus: 429, Message: "model quota"}})
	m.MarkResult(ctx, auth.Result{AuthID: "transient", Provider: "claude", Model: "upstream-model", Error: &auth.Error{HTTPStatus: 503, Message: "unavailable"}})
	records, err := s.CooldownStateStore().Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) < 3 {
		t.Fatalf("cooldowns were not persisted: %+v", records)
	}
	records = append(records, auth.CooldownStateRecord{AuthID: "expired", Provider: "claude", NextRetryAfter: time.Now().Add(-time.Hour), UpdatedAt: time.Now().Add(-2 * time.Hour)})
	if err = s.CooldownStateStore().Save(ctx, records); err != nil {
		t.Fatal(err)
	}
	// Open a new pool/store and manager, as a process restart would.
	cfg.SpoolDir = t.TempDir()
	fresh, err := NewPostgresStore(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = fresh.Close() }()
	restored := manager(fresh)
	if err = restored.RestoreCooldownStates(ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"credential", "model", "transient"} {
		a, _ := restored.GetByID(id)
		views := auth.CooldownSnapshotForAuth(a, time.Now())
		if len(views) == 0 {
			t.Fatalf("%s not restored: %+v", id, a)
		}
		if id == "credential" && (!a.Quota.Exceeded || a.Quota.Reason != "credential_quota") {
			t.Fatalf("lost credential scope: %+v", a.Quota)
		}
		if id == "model" && a.ModelStates["friendly-alias"] == nil {
			t.Fatal("lost model alias scope")
		}
		if id == "transient" && a.ModelStates["upstream-model"] == nil {
			t.Fatal("lost model scope")
		}
	}
	expired, _ := restored.GetByID("expired")
	if expired.Unavailable {
		t.Fatal("restored expired cooldown")
	}
	// An old process cannot shorten a newer database record.
	current, err := fresh.CooldownStateStore().Load(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var newer auth.CooldownStateRecord
	for _, record := range current {
		if record.AuthID == "model" && record.Model == "friendly-alias" {
			newer = record
			break
		}
	}
	stale := newer
	stale.UpdatedAt = stale.UpdatedAt.Add(-time.Hour)
	stale.NextRetryAfter = time.Now().Add(time.Minute)
	staleRecords := append([]auth.CooldownStateRecord(nil), current...)
	for i := range staleRecords {
		if staleRecords[i].AuthID == stale.AuthID && staleRecords[i].Model == stale.Model {
			staleRecords[i] = stale
		}
	}
	if err = s.CooldownStateStore().Save(ctx, staleRecords); err != nil {
		t.Fatal(err)
	}
	var actual time.Time
	if err = s.db.QueryRow(`SELECT (content->>'next_retry_after')::timestamptz FROM ` + schema + `.cooldown_store WHERE auth_id='model' AND model='friendly-alias'`).Scan(&actual); err != nil {
		t.Fatal(err)
	}
	if !actual.Equal(newer.NextRetryAfter.Round(time.Microsecond)) {
		t.Fatalf("stale update shortened cooldown: %v != %v", actual, newer.NextRetryAfter)
	}
	// The observer that read the current version can clear it on recovery.
	if _, err = fresh.CooldownStateStore().Load(ctx); err != nil {
		t.Fatal(err)
	}
	if err = fresh.CooldownStateStore().Save(ctx, nil); err != nil {
		t.Fatal(err)
	}
	var deleted bool
	if err = s.db.QueryRow(`SELECT deleted FROM ` + schema + `.cooldown_store WHERE auth_id='model' AND model='friendly-alias'`).Scan(&deleted); err != nil || !deleted {
		t.Fatalf("recovery soft delete=%v err=%v", deleted, err)
	}
}

func TestPostgresAuthSaveSkipsUnchangedRowAndPersistsRotation(t *testing.T) {
	dsn := os.Getenv("TMX_COOLDOWN_TEST_DSN")
	if dsn == "" {
		t.Skip("TMX_COOLDOWN_TEST_DSN not set")
	}
	ctx := context.Background()
	schema := fmt.Sprintf("auth_hash_test_%d", time.Now().UnixNano())
	s, err := NewPostgresStore(ctx, PostgresStoreConfig{DSN: dsn, Schema: schema, SpoolDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = s.db.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); _ = s.Close() }()
	if err = s.EnsureSchema(ctx); err != nil {
		t.Fatal(err)
	}
	credential := &auth.Auth{ID: "account.json", Provider: "claude", Metadata: map[string]any{"type": "claude", "access_token": "first"}}
	if _, err = s.Save(ctx, credential); err != nil {
		t.Fatal(err)
	}
	version := func() string {
		var v string
		if err := s.db.QueryRow(`SELECT xmin::text FROM ` + schema + `.auth_store WHERE id='account.json'`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	first := version()
	if _, err = s.Save(ctx, credential); err != nil {
		t.Fatal(err)
	}
	if version() != first {
		t.Fatal("unchanged Save upserted auth row")
	}
	credential.Metadata["access_token"] = "rotated"
	if _, err = s.Save(ctx, credential); err != nil {
		t.Fatal(err)
	}
	if version() == first {
		t.Fatal("OAuth token rotation was not persisted immediately")
	}
}
