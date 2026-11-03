package helps

import "testing"

// OpenCode's final chat-completions chunk (stream_options.include_usage) and non-stream body.
const opencodeChatUsage = `{"id":"x","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":1200,"completion_tokens":300,"total_tokens":1500,"cost":0,"opencode":{"billing_source":"go","list_cost":0.00412,"allowance":0.0031,"tokens":{"input":1200,"output":300,"reasoning":120,"cache_read":8000}}}}`

func TestParseOpenAIUsageReadsOpenCodeCost(t *testing.T) {
	d := ParseOpenAIUsage([]byte(opencodeChatUsage))
	pc := d.ProviderCost
	if pc == nil {
		t.Fatal("ProviderCost nil")
	}
	if pc.BillingSource != "go" || pc.Charged == nil || *pc.Charged != 0 || pc.ListCost == nil || *pc.ListCost != 0.00412 ||
		pc.Allowance == nil || *pc.Allowance != 0.0031 || pc.Estimated {
		t.Fatalf("ProviderCost = %+v", pc)
	}
	if d.InputTokens != 1200 || d.OutputTokens != 300 {
		t.Fatalf("tokens changed: %+v", d)
	}
}

func TestStreamUsageKeepsOpenCodeCost(t *testing.T) {
	var b StreamUsageBuffer
	b.ObserveOpenAIStream([]byte(`data: {"choices":[{"delta":{"content":"hi"}}]}`))
	b.ObserveOpenAIStream([]byte("data: " + opencodeChatUsage))
	if b.detail.ProviderCost == nil || *b.detail.ProviderCost.Allowance != 0.0031 {
		t.Fatalf("stream lost ProviderCost: %+v", b.detail.ProviderCost)
	}
}

func TestOtherUpstreamsKeepCatalogPricing(t *testing.T) {
	// OpenRouter sends a bare usage.cost: not OpenCode's report, so no ProviderCost.
	d := ParseOpenAIUsage([]byte(`{"usage":{"prompt_tokens":10,"completion_tokens":2,"total_tokens":12,"cost":0.0001}}`))
	if d.ProviderCost != nil {
		t.Fatalf("ProviderCost = %+v, want nil", d.ProviderCost)
	}
	if ParseClaudeUsage([]byte(`{"usage":{"input_tokens":5,"output_tokens":1}}`)).ProviderCost != nil {
		t.Fatal("Claude usage without opencode got a ProviderCost")
	}
}
