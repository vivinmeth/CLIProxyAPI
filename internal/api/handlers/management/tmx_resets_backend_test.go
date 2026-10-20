package management

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

// TmxSend is what reset-credit redeems go out through: the POST body, the token substitution and
// the headers must reach the provider exactly as built.
func TestTmxSendPostsBodyWithToken(t *testing.T) {
	var gotMethod, gotAuth, gotType, gotAccount, gotBody string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotAuth = r.Method, r.Header.Get("Authorization")
		gotType, gotAccount = r.Header.Get("Content-Type"), r.Header.Get("Chatgpt-Account-Id")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.WriteHeader(200)
		_, _ = w.Write([]byte(`{"code":"reset"}`))
	}))
	defer srv.Close()

	h := &Handler{}
	auth := &coreauth.Auth{ID: "codex-1", Provider: "codex", Metadata: map[string]any{"access_token": "tok-123"}}
	body := []byte(`{"redeem_request_id":"r1","credit_id":"c1"}`)
	code, resp, err := h.TmxSend(context.Background(), auth, http.MethodPost, srv.URL+"/consume",
		map[string]string{"Authorization": "Bearer $TOKEN$", "Content-Type": "application/json", "Chatgpt-Account-Id": "acct"}, body)
	if err != nil || code != 200 || string(resp) != `{"code":"reset"}` {
		t.Fatalf("send: %d %q %v", code, resp, err)
	}
	if gotMethod != http.MethodPost || gotAuth != "Bearer tok-123" || gotType != "application/json" ||
		gotAccount != "acct" || gotBody != string(body) {
		t.Fatalf("upstream saw method=%s auth=%q type=%q account=%q body=%q", gotMethod, gotAuth, gotType, gotAccount, gotBody)
	}

	// GET without a body (the reads) still works, and a missing token fails before any request.
	if code, _, err = h.TmxSend(context.Background(), auth, http.MethodGet, srv.URL, nil, nil); err != nil || code != 200 || gotBody != "" {
		t.Fatalf("get: %d %v body=%q", code, err, gotBody)
	}
	gotMethod = ""
	if _, _, err = h.TmxSend(context.Background(), &coreauth.Auth{ID: "x", Provider: "codex"}, http.MethodPost, srv.URL, nil, body); err == nil || gotMethod != "" {
		t.Fatalf("no token must fail without calling upstream: err=%v method=%q", err, gotMethod)
	}
}
