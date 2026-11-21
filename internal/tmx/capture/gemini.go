package capture

import (
	"net/url"
	"strings"
)

// NativeGeminiModelAction returns only a validated model identifier and native
// action. URL credentials, query values and fragments never enter model labels.
func NativeGeminiModelAction(rawURL string) (string, string) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Opaque != "" || (u.Scheme != "" && (u.Scheme != "http" && u.Scheme != "https" || u.Host == "")) {
		return "", ""
	}
	for _, prefix := range []string{"/v1beta/models/", "/v1/models/", "/models/"} {
		if native, ok := strings.CutPrefix(u.Path, prefix); ok {
			model, action, found := strings.Cut(native, ":")
			if !found || (action != "generateContent" && action != "streamGenerateContent" && action != "countTokens") || !strings.HasPrefix(model, "gemini-") || len(model) == len("gemini-") {
				return "", ""
			}
			for _, c := range model {
				if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '.') {
					return "", ""
				}
			}
			return model, action
		}
	}
	return "", ""
}
