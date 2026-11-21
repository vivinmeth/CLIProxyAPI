package capture

import "testing"

func TestNativeGeminiModelAction(t *testing.T) {
	for _, prefix := range []string{"/v1beta/models/", "/v1/models/", "/models/"} {
		for _, action := range []string{"generateContent", "streamGenerateContent", "countTokens"} {
			for _, origin := range []string{"", "https://user:secret@offline.invalid"} {
				model, got := NativeGeminiModelAction(origin + prefix + "gemini-test_1.0:" + action + "?key=secret#secret")
				if model != "gemini-test_1.0" || got != action {
					t.Fatalf("model=%q action=%q", model, got)
				}
			}
		}
	}
	for _, url := range []string{"", "http://[", "https:models/gemini-a:generateContent", "https:///models/gemini-a:generateContent", "file:///models/gemini-a:generateContent", "/models/gemini-:generateContent", "/models/gpt-a:generateContent", "/models/gemini-a:predict", "/models/gemini-a:generateContent/extra", "/models/gemini-a%2Fsecret:generateContent", "/models/gemini-a%3Fkey=secret:generateContent", "/models/gemini-%zz:generateContent"} {
		if model, action := NativeGeminiModelAction(url); model != "" || action != "" {
			t.Fatalf("invalid URL accepted: model=%q action=%q", model, action)
		}
	}
}

func TestNativeGeminiGenerationAction(t *testing.T) {
	for _, prefix := range []string{"/v1beta/models/", "/v1/models/", "/models/"} {
		for _, model := range []string{"gemini-2.5-pro(high)", "gemini-2.5-pro%28high%29", "vertex-flash", "native-gemini-flash"} {
			for _, action := range []string{"generateContent", "streamGenerateContent"} {
				for _, origin := range []string{"", "https://user:secret@offline.invalid"} {
					rawURL := origin + prefix + model + ":" + action + "?key=secret#secret"
					if got := NativeGeminiGenerationAction(rawURL); got != action {
						t.Fatalf("action=%q want=%q", got, action)
					}
					if label, labelAction := NativeGeminiModelAction(rawURL); label != "" || labelAction != "" {
						t.Fatalf("alias/suffix broadened model labels: %q %q", label, labelAction)
					}
				}
			}
		}
	}
	for _, rawURL := range []string{
		"", "http://[", "https:models/vertex-flash:generateContent", "https:///models/vertex-flash:generateContent", "file:///models/vertex-flash:generateContent",
		"/v1/messages?model=vertex-flash:generateContent", "/v1/chat/completions", "/other/models/vertex-flash:generateContent",
		"/v1beta/models/:generateContent", "/v1beta/models/%20:generateContent", "/v1beta/models/vertex-flash:", "/v1beta/models/vertex-flash",
		"/v1beta/models/vertex-flash:countTokens", "/v1beta/models/vertex-flash:predict", "/v1beta/models/vertex-flash:generateContent/extra",
		"/v1beta/models/vertex-flash:extra:generateContent", "/v1beta/models/vertex%2Fflash:generateContent", "/v1beta/models/vertex%5Cflash:generateContent",
		"/v1beta/models/vertex%3Fkey=secret:generateContent", "/v1beta/models/vertex%23secret:generateContent", "/v1beta/models/vertex%0Aflash:generateContent",
		"/v1beta/models/vertex%zz:generateContent", "/v1beta/models/%E2%80%83:generateContent",
	} {
		if got := NativeGeminiGenerationAction(rawURL); got != "" {
			t.Fatalf("invalid generation route accepted: action=%q", got)
		}
	}
}
