package helps

import "testing"

// tokenmaxx fork: 1-hour cache writes, speed and the billed tier survive parsing and stream merging.
func TestClaudeUsageCacheTTLSpeedTier(t *testing.T) {
	start := `data: {"type":"message_start","message":{"usage":{"input_tokens":5,"cache_creation_input_tokens":1000,` +
		`"cache_read_input_tokens":0,"cache_creation":{"ephemeral_5m_input_tokens":400,"ephemeral_1h_input_tokens":600},` +
		`"output_tokens":1,"service_tier":"standard","speed":"fast"}}}`
	delta := `data: {"type":"message_delta","usage":{"output_tokens":42}}`
	var b StreamUsageBuffer
	b.ObserveClaudeStream([]byte(start))
	b.ObserveClaudeStream([]byte(delta))
	d, ok := b.Detail()
	if !ok || d.CacheCreationTokens != 1000 || d.CacheWrite1hTokens != 600 || d.ResponseSpeed != "fast" ||
		d.ResponseServiceTier != "standard" || d.OutputTokens != 42 {
		t.Fatalf("detail = %+v", d)
	}
}
