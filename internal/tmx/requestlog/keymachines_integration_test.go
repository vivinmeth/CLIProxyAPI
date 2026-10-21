package requestlog

import (
	"context"
	"testing"
	"time"
)

func TestBillingFor(t *testing.T) {
	for _, c := range []struct{ auth, provider, want string }{
		{"oauth", "claude", BillingSubscription}, {"apikey", "openai", BillingAPIKey},
		{"oauth", "soa-ollama", BillingLocal}, {"", "codex", ""},
	} {
		if got := BillingFor(c.auth, c.provider); got != c.want {
			t.Errorf("BillingFor(%q, %q) = %q, want %q", c.auth, c.provider, got, c.want)
		}
	}
}

// The ledger names the machine of existing rows of a key (never overwriting a machine from the
// key's name) and of later rows; billing and imported accounts are backfilled.
func TestKeyMachinesAndBillingBackfill(t *testing.T) {
	s, _ := openPricedStore(t)
	ctx := context.Background()
	const k = "0123456789abcdef"
	if err := s.insert([]Row{
		{TS: time.Now(), TraceID: "old", APIKeyHash: k, Outcome: OutcomeSuccess, Source: SourceProxy, AuthType: "oauth"},
		{TS: time.Now(), TraceID: "named", APIKeyHash: k, Machine: "alien", Outcome: OutcomeSuccess, Source: SourceProxy},
		{TS: time.Now(), TraceID: "imp", Outcome: OutcomeSuccess, Source: "extern:nanx-app", AuthLabel: "openai:iam", ImportRef: "n#1"},
	}); err != nil {
		t.Fatal(err)
	}
	res, err := s.SetKeyMachines(ctx, []KeyMachine{{APIKeyHash: k, Machine: "sonofanton"}})
	if err != nil || res.Stored != 1 || res.RowsUpdated != 1 {
		t.Fatalf("set = %+v %v", res, err)
	}
	if _, err = s.SetKeyMachines(ctx, []KeyMachine{{APIKeyHash: "nothex", Machine: "x"}}); err == nil {
		t.Fatal("bad hash accepted")
	}
	machineOf := func(trace string) string {
		var m string
		_ = s.db.QueryRow(`SELECT machine FROM `+s.table()+` WHERE trace_id = $1`, trace).Scan(&m)
		return m
	}
	if machineOf("old") != "sonofanton" || machineOf("named") != "alien" {
		t.Fatalf("machines = %q %q", machineOf("old"), machineOf("named"))
	}
	s.Enqueue(Row{TS: time.Now(), TraceID: "new", APIKeyHash: k, Outcome: OutcomeSuccess})
	s.stopOnce.Do(func() { close(s.stopPrices); close(s.queue); <-s.done })
	if machineOf("new") != "sonofanton" {
		t.Fatalf("new row machine = %q", machineOf("new"))
	}
	s.backfillBilling()
	var billing, authID string
	_ = s.db.QueryRow(`SELECT billing FROM ` + s.table() + ` WHERE trace_id = 'old'`).Scan(&billing)
	_ = s.db.QueryRow(`SELECT auth_id FROM ` + s.table() + ` WHERE trace_id = 'imp'`).Scan(&authID)
	if billing != BillingSubscription || authID != "openai:iam" {
		t.Fatalf("billing = %q, imported auth_id = %q", billing, authID)
	}
}
