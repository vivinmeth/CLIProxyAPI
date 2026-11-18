package executor

import (
	"bytes"
	"net/http"
	"strings"

	"github.com/MoeForge/moego/ojson"
	claudeauth "github.com/router-for-me/CLIProxyAPI/v8/internal/auth/claude"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/clienterror"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/util"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type claudeDiagnosticsRequestState struct {
	key      string
	sequence uint64
	promptID string
}

func injectClaudeDiagnostics(body []byte, auth *cliproxyauth.Auth, sessionID string) ([]byte, claudeDiagnosticsRequestState) {
	key, sequence, previousMessageID, _, promptID := helps.BeginClaudeContinuity(claudeDiagnosticsCredentialIdentity(auth), sessionID, false, "")
	return injectClaudeDiagnosticsWithState(body, key, sequence, previousMessageID, promptID)
}

func injectClaudeDiagnosticsDoc(doc *ojson.Doc, auth *cliproxyauth.Auth, sessionID string) claudeDiagnosticsRequestState {
	key, sequence, previousMessageID, _, promptID := helps.BeginClaudeContinuity(claudeDiagnosticsCredentialIdentity(auth), sessionID, false, "")
	return injectClaudeDiagnosticsWithStateDoc(doc, key, sequence, previousMessageID, promptID)
}

func injectClaudeDiagnosticsWithStateDoc(doc *ojson.Doc, key string, sequence uint64, previousMessageID string, promptIDs ...string) claudeDiagnosticsRequestState {
	if doc == nil || key == "" {
		return claudeDiagnosticsRequestState{}
	}
	promptID := ""
	if len(promptIDs) > 0 {
		promptID = promptIDs[0]
	}
	value := `{"previous_message_id":null}`
	if previousMessageID != "" {
		value = `{"previous_message_id":` + marshalJSONStringWithoutHTMLEscape(previousMessageID) + `}`
	}
	if errSet := doc.SetRawBytes("diagnostics", []byte(value)); errSet != nil {
		return claudeDiagnosticsRequestState{}
	}
	return claudeDiagnosticsRequestState{key: key, sequence: sequence, promptID: promptID}
}

func injectClaudeDiagnosticsWithState(body []byte, key string, sequence uint64, previousMessageID string, promptIDs ...string) ([]byte, claudeDiagnosticsRequestState) {
	if key == "" {
		return body, claudeDiagnosticsRequestState{}
	}
	promptID := ""
	if len(promptIDs) > 0 {
		promptID = promptIDs[0]
	}
	value := `{"previous_message_id":null}`
	if previousMessageID != "" {
		value = `{"previous_message_id":` + marshalJSONStringWithoutHTMLEscape(previousMessageID) + `}`
	}

	if diagnostics := ojson.GetBytesNoCopy(body, "diagnostics"); diagnostics.Exists() {
		updated, errSet := ojson.SetRawBytes(body, "diagnostics", []byte(value))
		if errSet == nil {
			return updated, claudeDiagnosticsRequestState{key: key, sequence: sequence, promptID: promptID}
		}
	}
	if contextManagement := ojson.GetBytesNoCopy(body, "context_management"); contextManagement.Exists() {
		start := contextManagement.Index
		insertAt := start + len(contextManagement.Raw)
		if start >= 0 && insertAt >= start && insertAt <= len(body) && bytes.Equal(body[start:insertAt], []byte(contextManagement.Raw)) {
			updated := make([]byte, 0, len(body)+len(value)+len(`,"diagnostics":`))
			updated = append(updated, body[:insertAt]...)
			updated = append(updated, `,"diagnostics":`...)
			updated = append(updated, value...)
			updated = append(updated, body[insertAt:]...)
			return updated, claudeDiagnosticsRequestState{key: key, sequence: sequence, promptID: promptID}
		}
	}
	updated, errSet := ojson.SetRawBytes(body, "diagnostics", []byte(value))
	if errSet != nil {
		return body, claudeDiagnosticsRequestState{}
	}
	return updated, claudeDiagnosticsRequestState{key: key, sequence: sequence, promptID: promptID}
}

func commitClaudeContinuity(state claudeDiagnosticsRequestState, messageID, requestID string) {
	helps.CommitClaudeContinuity(state.key, state.sequence, messageID, requestID, state.promptID)
}

func commitClaudeDiagnostics(state claudeDiagnosticsRequestState, messageID string) {
	commitClaudeContinuity(state, messageID, "")
}

func claudeDiagnosticsCredentialIdentity(auth *cliproxyauth.Auth) string {
	if auth == nil {
		return ""
	}
	if id := strings.TrimSpace(auth.ID); id != "" {
		return "id:" + id
	}
	if index := strings.TrimSpace(auth.Index); index != "" {
		return "index:" + index
	}
	deviceIDs := claudeauth.NormalizeDeviceIDPool(claudeauth.ReadDeviceIDPool(&auth.Metadata))
	if len(deviceIDs) > 0 {
		return "device:" + deviceIDs[0]
	}
	if accountUUID := helps.ClaudeCredentialAccountUUID(auth); accountUUID != "" {
		return "account:" + accountUUID
	}
	return ""
}

func claudeMessageIDFromResponse(data []byte) string {
	return strings.Clone(strings.TrimSpace(ojson.GetBytesNoCopy(data, "id").String()))
}

func observeClaudeStreamJSONPayload(payload []byte, hasDataPrefix bool, messageID *string, completed *bool) {
	root := util.ParseGJSONBytesNoCopy(payload)
	switch root.Get("type").String() {
	case "message_start":
		if id := strings.TrimSpace(root.Get("message.id").String()); id != "" {
			*messageID = strings.Clone(id)
		}
	case "message_stop":
		*completed = true
	case "message":
		if !hasDataPrefix && strings.TrimSpace(root.Get("stop_reason").String()) != "" {
			if id := strings.TrimSpace(root.Get("id").String()); id != "" {
				*messageID = strings.Clone(id)
			}
			*completed = true
		}
	}
}

func observeClaudeSSEEvent(rawEvent []byte, messageID *string, completed *bool, streamErr *error) {
	observeClaudeSSEEventWithHeaders(rawEvent, nil, false, messageID, completed, streamErr)
}

// observeClaudeSSEEventWithHeaders is observeClaudeSSEEvent with the response headers captured at
// stream start, so an in-stream error is classified with the same quota semantics as over HTTP.
func observeClaudeSSEEventWithHeaders(rawEvent []byte, headers http.Header, modelLevelCooling bool, messageID *string, completed *bool, streamErr *error) {
	var dataLines [][]byte
	for _, line := range bytes.Split(rawEvent, []byte("\n")) {
		trimmed := bytes.TrimSpace(line)
		if !bytes.HasPrefix(trimmed, []byte("data:")) {
			continue
		}
		payload := bytes.TrimSpace(trimmed[len("data:"):])
		if len(payload) > 0 {
			dataLines = append(dataLines, payload)
		}
	}
	if len(dataLines) == 0 {
		trimmed := bytes.TrimSpace(rawEvent)
		if bytes.HasPrefix(trimmed, []byte("{")) && ojson.ValidBytes(trimmed) {
			observeClaudeStreamJSONPayload(trimmed, false, messageID, completed)
		}
		return
	}
	joined := bytes.TrimSpace(bytes.Join(dataLines, []byte("\n")))
	if !ojson.ValidBytes(joined) {
		return
	}
	observeClaudeStreamJSONPayload(joined, true, messageID, completed)
	if streamErr != nil && *streamErr == nil {
		if util.ParseGJSONBytesNoCopy(joined).Get("type").String() == "error" {
			*streamErr = classifyClaudeStreamError(joined, headers, modelLevelCooling) // tmx: in-stream errors classified like HTTP refusals
		} else if refusalMsg, blocked := clienterror.ClaudePolicyRefusalMessage(joined); blocked {
			*streamErr = statusErr{code: 400, msg: refusalMsg}
		}
	}
}

// claudeStatusOverloaded is Anthropic's non-standard "overloaded" status.
const claudeStatusOverloaded = 529

// classifyClaudeStreamError maps an SSE error event to the status the same refusal carries over
// HTTP, so the auth manager applies the matching cooldown: a rate_limit_error is a 429 parsed with
// the stream-start headers (window, reset, credential or model scope), overloaded_error a 529,
// authentication/permission errors 401/403. Anything else stays a 502. The event has already been
// forwarded, so this only decides cooldown and the client-visible error status; it never resumes
// the turn on another credential.
func classifyClaudeStreamError(payload []byte, headers http.Header, modelLevelCooling bool) error {
	switch strings.TrimSpace(ojson.GetBytesNoCopy(payload, "error.type").String()) {
	case "rate_limit_error":
		return classifyClaudeUpstreamErrorWithCooling(http.StatusTooManyRequests, headers, payload, modelLevelCooling)
	case "overloaded_error":
		return statusErr{code: claudeStatusOverloaded, msg: string(payload)}
	case "authentication_error":
		return statusErr{code: http.StatusUnauthorized, msg: string(payload)}
	case "permission_error":
		return statusErr{code: http.StatusForbidden, msg: string(payload)}
	}
	return statusErr{code: http.StatusBadGateway, msg: string(payload)}
}

func claudeMessageIDFromSSE(data []byte) string {
	var messageID string
	completed := false
	var eventBuf bytes.Buffer
	for _, line := range bytes.Split(data, []byte("\n")) {
		eventBuf.Write(line)
		eventBuf.WriteByte('\n')
		if len(bytes.TrimSpace(line)) == 0 {
			observeClaudeSSEEvent(eventBuf.Bytes(), &messageID, &completed, nil)
			eventBuf.Reset()
		}
	}
	if eventBuf.Len() > 0 {
		observeClaudeSSEEvent(eventBuf.Bytes(), &messageID, &completed, nil)
	}
	if !completed {
		return ""
	}
	return messageID
}
