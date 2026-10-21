package usage

import "testing"

func TestEmptyUsageIsUnclassified(t *testing.T) {
	for _, provider := range []string{"", "claude", "codex", "gemini", "openai-compatibility"} {
		t.Run(provider, func(t *testing.T) {
			for _, empty := range []Detail{{}, {ResponseServiceTier: "default"}, {TokenBreakdown: NewSubsetTokenBreakdown(0, 0, 0, 0, 0, 0)}} {
				detail := EnsureTokenBreakdownForProvider(empty, provider, "")
				if !detail.TokenBreakdown.Valid() || detail.TokenBreakdown.Quality != TokenAccountingQualityUnclassified {
					t.Fatalf("empty usage = %+v", detail)
				}
			}
		})
	}
	if got := NewUnclassifiedTokenBreakdown(0); got.Quality != TokenAccountingQualityUnclassified || !got.Valid() {
		t.Fatalf("unknown zero usage = %+v", got)
	}
}

func TestClaudeLegacyCachedTokensRecovery(t *testing.T) {
	for _, tc := range []struct {
		name                          string
		cached, read, write, wantRead int64
	}{
		{"legacy read", 40, 0, 0, 40},
		{"legacy read and write", 40, 0, 10, 40},
		{"write alias is not a read", 10, 0, 10, 0},
		{"explicit read wins", 40, 7, 10, 7},
		{"no cache", 0, 0, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, provider := range []string{"claude", "anthropic"} {
				detail := EnsureTokenBreakdownForProvider(Detail{InputTokens: 20, OutputTokens: 5,
					CachedTokens: tc.cached, CacheReadTokens: tc.read, CacheCreationTokens: tc.write}, provider, "")
				b := detail.TokenBreakdown
				if !b.Valid() || b.Quality != TokenAccountingQualityComplete || b.Input.UncachedTokens != 20 ||
					b.Input.CacheReadTokens != tc.wantRead || b.Input.CacheWriteTokens != tc.write || b.TotalTokens != 25+tc.wantRead+tc.write {
					t.Fatalf("%s breakdown = %+v", provider, b)
				}
				if again := EnsureTokenBreakdownForProvider(detail, provider, ""); again != detail {
					t.Fatalf("normalization is not idempotent: %+v", again)
				}
			}
		})
	}
}
