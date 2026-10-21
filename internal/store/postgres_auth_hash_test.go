package store

import (
	"context"
	"errors"
	"testing"
)

func TestPostgresAuthSkipsOnlySuccessfullyPersistedUnchangedContent(t *testing.T) {
	writes := 0
	fail := false
	s := &PostgresStore{persistAuthFn: func(context.Context, string, []byte) error {
		writes++
		if fail {
			return errors.New("offline")
		}
		return nil
	}}
	save := func(raw string) error { return s.persistAuth(context.Background(), "account", []byte(raw)) }
	if err := save(`{"access_token":"first","disabled":false}`); err != nil {
		t.Fatal(err)
	}
	if err := save(`{ "disabled":false, "access_token":"first" }`); err != nil {
		t.Fatal(err)
	}
	if writes != 1 {
		t.Fatalf("unchanged writes = %d", writes)
	}
	fail = true
	if err := save(`{"access_token":"rotated"}`); err == nil {
		t.Fatal("expected DB failure")
	}
	fail = false
	if err := save(`{"access_token":"rotated"}`); err != nil {
		t.Fatal(err)
	}
	if writes != 3 {
		t.Fatalf("rotation must retry immediately: %d writes", writes)
	}
	if err := save(`{"access_token":"rotated"}`); err != nil {
		t.Fatal(err)
	}
	if writes != 3 {
		t.Fatalf("unchanged rotation wrote again: %d", writes)
	}
}
