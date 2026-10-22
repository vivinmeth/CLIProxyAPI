// Package pricing turns a usage row's tokens into list-price USD (tokenmaxx fork).
//
// Rates come from the models.dev catalog, resolved the way the fleet engine does
// (fleet-engine/src/usage/pricing.rs): the row's own provider, then the model's maker, then any
// catalog entry only when every entry for the id agrees on the price. Models the catalog doesn't
// know stay unpriced, never $0. On top of the catalog:
//   - long-context tiers: a prompt above the tier size is charged the tier's rates in full;
//   - Anthropic 1-hour cache writes cost 2x the input rate (5-minute writes are the catalog's
//     cache_write, 1.25x);
//   - speed / service tier multipliers the catalog doesn't carry (see tierMultiplier);
//   - Meta's own list prices for the Muse models (metaPrices), over whatever the catalog says, and
//     owner-set estimates for Muse models Meta publishes no price for (metaEstimates).
package pricing

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
)

// Rate is USD per 1M tokens.
type Rate struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheRead  float64 `json:"cache_read"`
	CacheWrite float64 `json:"cache_write"`
}

// Model is one catalog entry.
type Model struct {
	Rate
	// LongThreshold > 0: prompts above it are charged Long for the whole request.
	LongThreshold int64 `json:"long_threshold,omitempty"`
	Long          *Rate `json:"long,omitempty"`
	// Source names where the rate came from when it isn't models.dev.
	Source string `json:"source,omitempty"`
}

// metaPrices are Meta's pay-as-you-go API prices (USD per 1M tokens) from
// dev.meta.ai/docs/pricing-rate-limits, checked 2026-10-22: no long-context premium, no separate
// cache-write price (writes are charged as input). muse-wonder-full-1.0 has no API price (see
// metaEstimates); the Wonder contributor variant has none and stays unpriced.
var metaPrices = map[string]Rate{
	"muse-wonder-1.0":            {Input: 4.00, Output: 20.00, CacheRead: 0.20, CacheWrite: 4.00},
	"muse-spark-1.3":             {Input: 1.25, Output: 4.25, CacheRead: 0.15, CacheWrite: 1.25},
	"muse-spark-1.2":             {Input: 1.25, Output: 4.25, CacheRead: 0.15, CacheWrite: 1.25},
	"muse-spark-1.1":             {Input: 1.25, Output: 4.25, CacheRead: 0.15, CacheWrite: 1.25},
	"muse-spark-1.3-contributor": {Input: 0.10, Output: 0.20, CacheRead: 0.002, CacheWrite: 0.10},
	"muse-spark-1.2-contributor": {Input: 0.10, Output: 0.20, CacheRead: 0.002, CacheWrite: 0.10},
}

const metaPriceSource = "meta.ai"

// EstimatePrefix starts the Source of a price that is not the provider's own (rows keep it in
// price_source; the request rollups count them as `estimated`).
const EstimatePrefix = "estimate:"

// metaEstimates are unofficial prices for Muse models Meta sells no API access to (owner's call,
// 2026-10-22). muse-wonder-full-1.0 is priced at Claude Fable 5.1's list rates, flat at any
// context length: Anthropic and OpenAI both price their top tier at $10 / $50, and Meta prices
// Wonder exactly like Opus 5.5. Cache writes are charged as input, as on Meta's priced models.
var metaEstimates = map[string]Model{
	"muse-wonder-full-1.0": {Rate: Rate{Input: 10.00, Output: 50.00, CacheRead: 0.25, CacheWrite: 10.00},
		Source: EstimatePrefix + "fable-5.1"},
}

// IsEstimate reports whether a price source is an estimate rather than a list price.
func IsEstimate(source string) bool { return strings.HasPrefix(source, EstimatePrefix) }

// AddPinned writes the code-pinned Meta prices and estimates into cat and reports whether that
// changed it (a stored snapshot from before a pin was added).
func AddPinned(cat Catalog) bool {
	if cat["meta"] == nil {
		cat["meta"] = map[string]Model{}
	}
	changed := false
	set := func(id string, m Model) {
		if old, ok := cat["meta"][id]; !ok || old.Rate != m.Rate || old.Source != m.Source ||
			old.LongThreshold != m.LongThreshold || old.Long != nil {
			changed = true
		}
		cat["meta"][id] = m
	}
	for id, r := range metaPrices {
		set(id, Model{Rate: r, Source: metaPriceSource})
	}
	for id, m := range metaEstimates {
		set(id, m)
	}
	return changed
}

// Catalog: provider id -> lower-case model id -> prices. Only priced models are kept.
type Catalog map[string]map[string]Model

type rawCost struct {
	Input      float64  `json:"input"`
	Output     float64  `json:"output"`
	CacheRead  *float64 `json:"cache_read"`
	CacheWrite *float64 `json:"cache_write"`
	Tiers      []struct {
		Input      *float64 `json:"input"`
		Output     *float64 `json:"output"`
		CacheRead  *float64 `json:"cache_read"`
		CacheWrite *float64 `json:"cache_write"`
		Tier       struct {
			Type string `json:"type"`
			Size int64  `json:"size"`
		} `json:"tier"`
	} `json:"tiers"`
}

// ParseModelsDev reads the models.dev api.json document.
func ParseModelsDev(raw []byte) (Catalog, error) {
	var doc map[string]struct {
		Models map[string]struct {
			Cost *rawCost `json:"cost"`
		} `json:"models"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, err
	}
	cat := Catalog{}
	for prov, p := range doc {
		for id, m := range p.Models {
			c := m.Cost
			if c == nil || (c.Input <= 0 && c.Output <= 0) {
				continue
			}
			// A missing cache rate falls back to the input rate (no discount assumed).
			or := func(v *float64, d float64) float64 {
				if v != nil {
					return *v
				}
				return d
			}
			mod := Model{Rate: Rate{Input: c.Input, Output: c.Output,
				CacheRead: or(c.CacheRead, c.Input), CacheWrite: or(c.CacheWrite, c.Input)}}
			for _, t := range c.Tiers {
				if t.Tier.Type != "context" || t.Tier.Size <= 0 {
					continue
				}
				mod.LongThreshold = t.Tier.Size
				mod.Long = &Rate{Input: or(t.Input, mod.Input), Output: or(t.Output, mod.Output),
					CacheRead: or(t.CacheRead, mod.CacheRead), CacheWrite: or(t.CacheWrite, mod.CacheWrite)}
				break
			}
			if cat[prov] == nil {
				cat[prov] = map[string]Model{}
			}
			cat[prov][strings.ToLower(id)] = mod
		}
	}
	if len(cat) == 0 {
		return nil, fmt.Errorf("price catalog is empty")
	}
	AddPinned(cat)
	return cat, nil
}

// CatalogProvider maps the proxy's provider names to models.dev provider ids.
func CatalogProvider(provider string) string {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "claude", "anthropic":
		return "anthropic"
	case "codex", "openai":
		return "openai"
	case "gemini", "gemini-cli", "antigravity", "google":
		return "google"
	case "xai", "grok":
		return "xai"
	}
	return strings.ToLower(strings.TrimSpace(provider))
}

// maker is the catalog provider that makes a model, from its id.
func maker(id string) string {
	switch {
	case strings.HasPrefix(id, "claude-"):
		return "anthropic"
	case strings.HasPrefix(id, "gpt-"), strings.HasPrefix(id, "o1"), strings.HasPrefix(id, "o3"),
		strings.HasPrefix(id, "o4"), strings.HasPrefix(id, "codex-"):
		return "openai"
	case strings.HasPrefix(id, "gemini-"):
		return "google"
	case strings.HasPrefix(id, "grok-"):
		return "xai"
	}
	return ""
}

// candidates lists the ids a model name may be priced as (base first) and the bare id: `[1m]`
// suffixes and dated ids (-20261001) price like the base model; `vendor/` paths,
// `antigravity-` / `az-` prefixes, `-thinking` suffixes and `-0` minor versions wrap a catalog id.
func candidates(model string) ([]string, string) {
	base := strings.ToLower(strings.TrimSpace(model))
	base, _, _ = strings.Cut(base, "[")
	out := []string{base}
	add := func(c string) {
		for _, o := range out {
			if o == c {
				return
			}
		}
		out = append(out, c)
	}
	if i := strings.LastIndexByte(base, '-'); i > 0 && len(base)-i-1 == 8 && strings.Trim(base[i+1:], "0123456789") == "" {
		add(base[:i])
	}
	bare := base[strings.LastIndexByte(base, '/')+1:]
	for _, p := range []string{"antigravity-", "az-"} {
		bare = strings.TrimPrefix(bare, p)
	}
	bare = strings.TrimSuffix(bare, "-thinking")
	add(bare)
	if s, ok := strings.CutSuffix(bare, "-0"); ok {
		add(s)
	}
	return out, bare
}

// Resolve finds the catalog entry for a row's provider and model; ok is false when unpriced.
func (c Catalog) Resolve(provider, model string) (catProvider, catModel string, m Model, ok bool) {
	ids, bare := candidates(model)
	// The row's own provider first, then the model's maker (a router or harness reselling it),
	// each over every candidate id before looking anywhere else.
	for _, p := range []string{CatalogProvider(provider), maker(bare)} {
		if p == "" {
			continue
		}
		for _, id := range ids {
			if mod, found := c[p][id]; found {
				return p, id, mod, true
			}
		}
	}
	// Meta's own rows only take Meta's prices (or our estimates): a reseller listing a model Meta
	// sells no API access to (muse-wonder-full-1.0) must not price subscription traffic.
	if CatalogProvider(provider) == "meta" {
		return "", "", Model{}, false
	}
	// Elsewhere only when every catalog entry for the id agrees on the price: resellers price the
	// same id differently, and picking one of them would be arbitrary.
	provs := make([]string, 0, len(c))
	for p := range c {
		provs = append(provs, p)
	}
	sort.Strings(provs)
	for _, id := range ids {
		var first *Model
		firstProv := ""
		agree := true
		for _, p := range provs {
			mod, found := c[p][id]
			if !found {
				continue
			}
			if first == nil {
				mm := mod
				first, firstProv = &mm, p
			} else if mod.Rate != first.Rate {
				agree = false
			}
		}
		if first != nil {
			if !agree {
				return "", "", Model{}, false
			}
			return firstProv, id, *first, true
		}
	}
	return "", "", Model{}, false
}

// Usage is a row's tokens in the proxy's normalised form: prompt = uncached + cache read + cache
// write (CacheWrite1h is the part of CacheWrite written with a 1-hour TTL); Output includes
// reasoning.
type Usage struct {
	Provider     string
	Model        string
	Uncached     int64
	CacheRead    int64
	CacheWrite   int64
	CacheWrite1h int64
	Output       int64
	Speed        string // Anthropic usage.speed ("fast"); "" / "standard" = normal
	ServiceTier  string // returned tier (OpenAI "priority" / "flex"; Anthropic "standard" …)
}

// Cost is a priced row. Fields are USD.
type Cost struct {
	Input      float64
	CacheRead  float64
	CacheWrite float64
	Output     float64
	Total      float64
	// PricedAs records what was applied: "<provider>/<model>", plus " long", " fast×2" ….
	PricedAs string
	Source   string // "models.dev" | "local" | "meta.ai" | "estimate:<basis>"
}

// Price returns the list-price cost of u, or ok=false when the model is unpriced.
func (c Catalog) Price(u Usage) (Cost, bool) {
	if strings.Contains(strings.ToLower(u.Provider), "ollama") { // local models cost nothing
		return Cost{PricedAs: "local/" + u.Model, Source: "local"}, true
	}
	prov, id, m, ok := c.Resolve(u.Provider, u.Model)
	if !ok {
		return Cost{}, false
	}
	r := m.Rate
	note := prov + "/" + id
	if m.Long != nil && m.LongThreshold > 0 && u.Uncached+u.CacheRead+u.CacheWrite > m.LongThreshold {
		r = *m.Long
		note += " long"
	}
	write1h := r.CacheWrite
	if prov == "anthropic" {
		write1h = 2 * r.Input
	}
	oneHour := min(max(u.CacheWrite1h, 0), u.CacheWrite)
	mult, label := tierMultiplier(prov, u.Speed, u.ServiceTier)
	if label != "" {
		note += " " + label
	}
	cost := Cost{
		Input:      float64(u.Uncached) * r.Input / 1e6 * mult,
		CacheRead:  float64(u.CacheRead) * r.CacheRead / 1e6 * mult,
		CacheWrite: (float64(u.CacheWrite-oneHour)*r.CacheWrite + float64(oneHour)*write1h) / 1e6 * mult,
		Output:     float64(u.Output) * r.Output / 1e6 * mult,
		PricedAs:   note,
		Source:     "models.dev",
	}
	if m.Source != "" {
		cost.Source = m.Source
	}
	cost.Total = cost.Input + cost.CacheRead + cost.CacheWrite + cost.Output
	if math.IsNaN(cost.Total) || math.IsInf(cost.Total, 0) {
		return Cost{}, false
	}
	return cost, true
}

// tierMultiplier: rates the catalog doesn't carry, applied to every token class. Anthropic fast
// mode and OpenAI priority are 2x, OpenAI flex 0.5x (per the 2026-10-21 cost audit; verify
// against the providers' pages when they first appear in the data — rows keep speed and
// service_tier, so they can be repriced).
func tierMultiplier(catProvider, speed, tier string) (float64, string) {
	speed, tier = strings.ToLower(strings.TrimSpace(speed)), strings.ToLower(strings.TrimSpace(tier))
	if catProvider == "anthropic" && speed == "fast" {
		return 2, "fast×2"
	}
	if catProvider == "openai" {
		switch tier {
		case "priority":
			return 2, "priority×2"
		case "flex":
			return 0.5, "flex×0.5"
		}
	}
	return 1, ""
}
