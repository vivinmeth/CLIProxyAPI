package helps

import (
	"strings"

	"github.com/MoeForge/moego/ojson"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

// tmxProviderCost reads OpenCode's cost fields from an OpenAI-style usage object (tokenmaxx fork;
// sdk/cliproxy/usage.ProviderCost). Only a usage object carrying an "opencode" object counts, so
// other upstreams that send a bare usage.cost (OpenRouter) keep being priced from the catalog.
func tmxProviderCost(usageNode ojson.Result) *usage.ProviderCost {
	oc := usageNode.Get("opencode")
	if !oc.IsObject() {
		return nil
	}
	num := func(r ojson.Result) *float64 {
		if r.Type != ojson.Number {
			return nil
		}
		v := r.Float()
		return &v
	}
	return &usage.ProviderCost{
		Charged:       num(usageNode.Get("cost")),
		ListCost:      num(oc.Get("list_cost")),
		Allowance:     num(oc.Get("allowance")),
		BillingSource: strings.ToLower(strings.TrimSpace(oc.Get("billing_source").String())),
		Estimated:     oc.Get("estimated").Bool(),
	}
}
