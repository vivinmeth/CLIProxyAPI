package pricing

import (
	"math"
	"testing"
)

const testCatalog = `{
"anthropic":{"models":{"claude-opus-5-5":{"cost":{"input":4,"output":20,"cache_read":0.2,"cache_write":5}},
	"claude-haiku-4-5":{"cost":{"input":1,"output":5}}}},
"openai":{"models":{"gpt-5.5":{"cost":{"input":5,"output":30,"cache_read":0.5,
	"tiers":[{"input":10,"output":45,"cache_read":1,"tier":{"type":"context","size":272000}}]}},
	"gpt-5-mini":{"cost":{"input":0.25,"output":2}}}},
"azure":{"models":{"gpt-5-mini":{"cost":{"input":0.225,"output":1.8}}}},
"openrouter":{"models":{"qwen/qwen3-coder":{"cost":{"input":0.3,"output":1.2}},"free":{"cost":{"input":0,"output":0}}}},
"groq":{"models":{"kimi-k2":{"cost":{"input":1,"output":3}}}},
"deepinfra":{"models":{"kimi-k2":{"cost":{"input":0.5,"output":2}}}},
"fireworks":{"models":{"glm-5":{"cost":{"input":0.6,"output":2.2}}}},
"together":{"models":{"glm-5":{"cost":{"input":0.6,"output":2.2}}}}}`

func catalog(t *testing.T) Catalog {
	t.Helper()
	c, err := ParseModelsDev([]byte(testCatalog))
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestResolve(t *testing.T) {
	c := catalog(t)
	for _, tc := range []struct{ provider, model, want string }{
		{"claude", "claude-opus-5-5[1m]", "anthropic/claude-opus-5-5"},
		{"anthropic", "claude-haiku-4-5-20251001", "anthropic/claude-haiku-4-5"},
		{"google", "antigravity-claude-opus-5-5-thinking", "anthropic/claude-opus-5-5"}, // maker
		{"opencode", "claude-opus-5-5", "anthropic/claude-opus-5-5"},
		{"litellm-surkyl", "az-gpt-5-mini", "openai/gpt-5-mini"}, // maker wins over resellers
		{"openrouter", "qwen/qwen3-coder", "openrouter/qwen/qwen3-coder"},
		{"opencode-go", "kimi-k2", ""},              // resellers disagree: unpriced, not arbitrary
		{"opencode-go", "glm-5", "fireworks/glm-5"}, // resellers agree
		{"openrouter", "anthropic/claude-haiku-4.5", ""},
		{"cursor", "default", ""},
		{"openrouter", "free", ""},
	} {
		p, id, _, ok := c.Resolve(tc.provider, tc.model)
		got := ""
		if ok {
			got = p + "/" + id
		}
		if got != tc.want {
			t.Errorf("%s %s priced as %q, want %q", tc.provider, tc.model, got, tc.want)
		}
	}
}

func near(a, b float64) bool { return math.Abs(a-b) < 1e-12 }

func TestPrice(t *testing.T) {
	c := catalog(t)
	// 1M uncached, 2M read, 100K written (60K of them 1-hour), 500K output.
	u := Usage{Provider: "claude", Model: "claude-opus-5-5", Uncached: 1_000_000, CacheRead: 2_000_000,
		CacheWrite: 100_000, CacheWrite1h: 60_000, Output: 500_000}
	got, ok := c.Price(u)
	if !ok {
		t.Fatal("unpriced")
	}
	wantWrite := (40_000*5.0 + 60_000*8.0) / 1e6 // 5m at cache_write, 1h at 2x input
	if !near(got.Input, 4) || !near(got.CacheRead, 0.4) || !near(got.CacheWrite, wantWrite) || !near(got.Output, 10) ||
		!near(got.Total, 4+0.4+wantWrite+10) || got.PricedAs != "anthropic/claude-opus-5-5" {
		t.Fatalf("cost = %+v", got)
	}
	u.Speed = "fast"
	if fast, _ := c.Price(u); !near(fast.Total, 2*got.Total) || fast.PricedAs != "anthropic/claude-opus-5-5 fast×2" {
		t.Fatalf("fast = %+v", fast)
	}
	// Long context: a 300K prompt is charged the tier's rates in full.
	long, _ := c.Price(Usage{Provider: "codex", Model: "gpt-5.5", Uncached: 100_000, CacheRead: 200_000, Output: 1000})
	if !near(long.Input, 1.0) || !near(long.CacheRead, 0.2) || !near(long.Output, 0.045) || long.PricedAs != "openai/gpt-5.5 long" {
		t.Fatalf("long = %+v", long)
	}
	short, _ := c.Price(Usage{Provider: "codex", Model: "gpt-5.5", Uncached: 100_000, ServiceTier: "flex"})
	if !near(short.Input, 0.25) || short.PricedAs != "openai/gpt-5.5 flex×0.5" {
		t.Fatalf("flex = %+v", short)
	}
	if local, ok := c.Price(Usage{Provider: "soa-ollama", Model: "qwen", Uncached: 10}); !ok || local.Total != 0 || local.Source != "local" {
		t.Fatalf("ollama = %+v %v", local, ok)
	}
	if _, ok := c.Price(Usage{Provider: "cursor", Model: "default", Uncached: 10}); ok {
		t.Fatal("unknown model priced")
	}
	// Haiku has no cache rates in the catalog: they fall back to input.
	h, _ := c.Price(Usage{Provider: "anthropic", Model: "claude-haiku-4-5", CacheRead: 1_000_000, CacheWrite: 1_000_000})
	if !near(h.CacheRead, 1) || !near(h.CacheWrite, 1) {
		t.Fatalf("haiku = %+v", h)
	}
}

func TestMetaOfficialPrices(t *testing.T) {
	// models.dev disagreeing must not matter: Meta's own list prices win for the meta provider.
	cat, err := ParseModelsDev([]byte(`{"meta":{"models":{"muse-spark-1.3":{"cost":{"input":9,"output":9}}}},
		"opper":{"models":{"muse-wonder-full-1.0":{"cost":{"input":1,"output":1}}}},"anthropic":{"models":{}}}`))
	if err != nil {
		t.Fatal(err)
	}
	c, ok := cat.Price(Usage{Provider: "meta", Model: "muse-spark-1.3", Uncached: 1_000_000, CacheRead: 1_000_000, Output: 1_000_000})
	if !ok || c.Source != "meta.ai" || c.PricedAs != "meta/muse-spark-1.3" || math.Abs(c.Total-(1.25+0.15+4.25)) > 1e-9 {
		t.Fatalf("spark = %+v ok=%v", c, ok)
	}
	if c, ok = cat.Price(Usage{Provider: "meta", Model: "muse-wonder-1.0", Uncached: 1_000_000, CacheRead: 1_000_000, Output: 1_000_000}); !ok || math.Abs(c.Total-24.20) > 1e-9 {
		t.Fatalf("wonder = %+v ok=%v", c, ok)
	}
	if c, ok = cat.Price(Usage{Provider: "meta", Model: "muse-spark-1.3-contributor", Uncached: 1_000_000, CacheRead: 1_000_000, Output: 1_000_000}); !ok || math.Abs(c.Total-0.302) > 1e-9 {
		t.Fatalf("spark contributor = %+v ok=%v", c, ok)
	}
	if c, ok = cat.Price(Usage{Provider: "meta", Model: "muse-wonder-1.0-contributor", Output: 1}); ok {
		t.Fatalf("wonder contributor must stay unpriced: %+v", c)
	}
	// Wonder Full: Fable 5.1's rates as an estimate (the reseller's $1 is ignored), flat at any
	// context length, cache writes at the input rate.
	c, ok = cat.Price(Usage{Provider: "meta", Model: "muse-wonder-full-1.0", Uncached: 1_000_000, CacheRead: 1_000_000,
		CacheWrite: 1_000_000, Output: 1_000_000})
	if !ok || c.Source != "estimate:fable-5.1" || !IsEstimate(c.Source) || c.PricedAs != "meta/muse-wonder-full-1.0" ||
		!near(c.Total, 10+0.25+10+50) {
		t.Fatalf("wonder full = %+v ok=%v", c, ok)
	}
	if long, _ := cat.Price(Usage{Provider: "meta", Model: "muse-wonder-full-1.0", Uncached: 2_000_000}); !near(long.Total, 20) {
		t.Fatalf("wonder full long = %+v", long)
	}
	if IsEstimate("meta.ai") || IsEstimate("models.dev") {
		t.Fatal("list prices reported as estimates")
	}
}

func TestAddPinned(t *testing.T) {
	cat := Catalog{"anthropic": {}}
	if !AddPinned(cat) || AddPinned(cat) {
		t.Fatal("AddPinned must report the first change only")
	}
	delete(cat["meta"], "muse-wonder-full-1.0")
	if !AddPinned(cat) {
		t.Fatal("a snapshot without the estimate must count as changed")
	}
}
