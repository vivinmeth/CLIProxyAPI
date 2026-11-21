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
