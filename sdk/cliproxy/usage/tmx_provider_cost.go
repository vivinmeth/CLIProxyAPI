package usage

// ProviderCost is what an upstream itself reported a request cost (tokenmaxx fork). OpenCode
// (Go and Zen/Console) puts it beside the token counts in every response's usage object:
//
//	"usage": {..., "cost": 0.00412, "opencode": {"billing_source": "go", "list_cost": 0.00412,
//	  "allowance": 0.0031, "tokens": {...}}}
//
// Nil when the upstream reported none. Pointers stay nil for fields the response didn't carry.
type ProviderCost struct {
	// Charged is usage.cost: US dollars charged to the pay-as-you-go balance (0 within Go limits).
	Charged *float64
	// ListCost is opencode.list_cost: US dollars at the model's price (peak and long-context aware).
	ListCost *float64
	// Allowance is opencode.allowance (Go only): dollars of the model's monthly limit this request used.
	Allowance *float64
	// BillingSource is opencode.billing_source: free, balance, go, go-plus or byok.
	BillingSource string
	// Estimated is opencode.estimated: cost is an estimate (own provider key only).
	Estimated bool
}
