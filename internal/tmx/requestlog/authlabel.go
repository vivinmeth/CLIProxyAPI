package requestlog

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

// safeAuthLabel is the auth_label stored for a row (tokenmaxx fork). Upstream's usage source is the
// login's email or project, but for an API-key credential it falls back to the raw upstream key,
// and with no credential to the client's own proxy key. Neither may be stored: an API-key
// credential is labelled "key:<12 hex of sha256>" (stable per key, never reversible), the client key
// is dropped (api_key_hash already identifies it), and anything shaped like a known secret is
// fingerprinted too.
func safeAuthLabel(record coreusage.Record) string {
	label := strings.TrimSpace(record.Source)
	switch {
	case label == "":
		return ""
	case label == strings.TrimSpace(record.APIKey):
		return ""
	case strings.EqualFold(strings.TrimSpace(record.AuthType), "apikey"), looksLikeSecret(label):
		return KeyFingerprint(label)
	}
	return label
}

// KeyFingerprint is the stored stand-in for an API key: "key:" + the first 12 hex of its SHA-256.
func KeyFingerprint(key string) string {
	sum := sha256.Sum256([]byte(strings.TrimSpace(key)))
	return "key:" + hex.EncodeToString(sum[:])[:12]
}

// secretPrefixes are key formats seen on this proxy (OpenCode, OpenAI-style and tmx client keys,
// Anthropic, Google, xAI).
var secretPrefixes = []string{"oc_sk_", "sk-", "sk_", "aiza", "xai-", "ghp_", "gho_"}

func looksLikeSecret(s string) bool {
	if strings.Contains(s, "@") || strings.ContainsAny(s, " \t") {
		return false
	}
	lower := strings.ToLower(s)
	for _, p := range secretPrefixes {
		if strings.HasPrefix(lower, p) {
			return true
		}
	}
	return false
}
