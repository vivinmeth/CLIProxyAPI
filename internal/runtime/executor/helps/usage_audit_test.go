package helps

import (
	"context"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func TestClaudeStreamQualityRequiresFinalUsage(t *testing.T) {
	for _, plugin := range []bool{false, true} {
		var buffer StreamUsageBuffer
		observe := func(line string) {
			if plugin {
				ObservePluginExecutorStreamUsage("claude", []byte(line), &buffer)
			} else {
				buffer.ObserveClaudeStream([]byte(line))
			}
		}
		observe(`data: {"type":"message_start","message":{"usage":{"input_tokens":10,"cache_read_input_tokens":40,"output_tokens":8}}}`)
		assertQuality := func(want usage.TokenAccountingQuality, output int64) {
			t.Helper()
			detail, ok := buffer.Detail()
			detail = usage.EnsureTokenBreakdownForProvider(detail, "claude", "")
			if !ok || !detail.TokenBreakdown.Valid() || detail.TokenBreakdown.Quality != want || detail.OutputTokens != output || detail.TokenBreakdown.Input.CacheReadTokens != 40 {
				t.Fatalf("plugin=%t: detail = %+v, want quality=%s output=%d", plugin, detail, want, output)
			}
		}
		assertQuality(usage.TokenAccountingQualityUnclassified, 8)
		// Neither a stop event without usage nor an interim delta proves final usage.
		observe(`data: {"type":"message_delta","delta":{},"usage":{"output_tokens":9}}`)
		observe(`data: {"type":"message_stop"}`)
		assertQuality(usage.TokenAccountingQualityUnclassified, 9)
		observe(`data: {"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":20}}`)
		assertQuality(usage.TokenAccountingQualityComplete, 20)
	}
}

func TestInteractionsPluginStreamPreservesSeparateReasoningSemantics(t *testing.T) {
	for _, protocol := range []string{"interactions", "interactions-response"} {
		var buffer StreamUsageBuffer
		ObservePluginExecutorStreamUsage(protocol, []byte(`data: {"usage":{"input_tokens":80,"cached_tokens":30,"output_tokens":4,"reasoning_tokens":6,"total_tokens":90}}`), &buffer)
		final := []byte(`data: {"usage":{"input_tokens":100,"cached_tokens":40,"cache_write_tokens":10,"output_tokens":20,"reasoning_tokens":10,"total_tokens":130}}`)
		ObservePluginExecutorStreamUsage(protocol, final, &buffer)
		detail, ok := buffer.Detail()
		want, _ := ParseInteractionsStreamUsage(final)
		if !ok || detail != want || !detail.TokenBreakdown.Valid() || detail.TokenBreakdown.Quality != usage.TokenAccountingQualityComplete ||
			detail.TokenBreakdown.Input.UncachedTokens != 50 || detail.TokenBreakdown.Output.NonReasoningTokens != 20 || detail.TokenBreakdown.Output.ReasoningTokens != 10 {
			t.Fatalf("%s: detail = %+v, want %+v", protocol, detail, want)
		}
	}
}

func TestUsageReporterUpstreamModelPreservesAliasAndSideModel(t *testing.T) {
	reporter := NewUsageReporter(context.Background(), "kimi", "kimi-alias", nil)
	reporter.SetUpstreamModel("kimi-for-coding")
	reporter.SetResponseModel("served-model")
	record := reporter.buildRecord(usage.Detail{}, false)
	if record.Model != "kimi-for-coding" || record.Alias != "kimi-alias" || record.ResponseModel != "served-model" {
		t.Fatalf("record = %+v", record)
	}
	side := reporter.buildRecordForModel("image-model", usage.Detail{}, false, usage.Failure{})
	if side.Model != "image-model" || side.ResponseModel != "" {
		t.Fatalf("side model record = %+v", side)
	}
}
