package muse

// Contributor mode (tokenmaxx fork). Meta's terms forbid confidential content in Muse Contributor
// mode, and client work goes through this proxy, so its models (ids ending in -contributor, and
// aliases of them) are refused for provider meta and left out of every model list. Owner's call,
// 2026-10-22. TMX_META_CONTRIBUTOR=allow turns them back on (read per call; restart not needed
// for requests, but model lists change on the next credential registration).

import (
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/thinking"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ContributorBlockedMessage is the refusal clients see.
const ContributorBlockedMessage = "contributor-mode models are disabled on this proxy"

// ContributorBlockedCode is the error code of the refusal.
const ContributorBlockedCode = "contributor_mode_disabled"

const contributorSuffix = "-contributor"

// ContributorAllowed reports whether TMX_META_CONTRIBUTOR=allow is set (default: blocked).
func ContributorAllowed() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("TMX_META_CONTRIBUTOR"))) {
	case "allow", "allowed", "on", "true", "1":
		return true
	}
	return false
}

// IsContributorModel reports whether a model id (thinking suffix ignored) is a Contributor-mode id.
func IsContributorModel(model string) bool {
	base := strings.ToLower(strings.TrimSpace(thinking.ParseSuffix(strings.TrimSpace(model)).ModelName))
	return strings.HasSuffix(base, contributorSuffix)
}

// contributorAliases holds the lower-cased aliases (oauth-model-alias, meta channel) that point at
// a Contributor model, noted when meta models are registered: they are never registered, so the
// request path can't resolve them any other way.
var contributorAliases sync.Map

// ContributorRequestBlocked reports whether a client's requested model must be refused before
// routing: a Muse Contributor id, or an alias noted by DropContributorModels.
func ContributorRequestBlocked(model string) bool {
	if ContributorAllowed() {
		return false
	}
	base := strings.ToLower(strings.TrimSpace(thinking.ParseSuffix(strings.TrimSpace(model)).ModelName))
	if base == "" {
		return false
	}
	if strings.HasPrefix(base[strings.LastIndexByte(base, '/')+1:], "muse-") && strings.HasSuffix(base, contributorSuffix) {
		return true
	}
	_, ok := contributorAliases.Load(base)
	return ok
}

// DropContributorModels removes Contributor models from a meta credential's model list (before
// aliases are applied, so aliases of them are not registered either) and notes those aliases.
// With TMX_META_CONTRIBUTOR=allow it returns models unchanged.
func DropContributorModels(models []*registry.ModelInfo, aliases []config.OAuthModelAlias) []*registry.ModelInfo {
	for _, a := range aliases {
		key := strings.ToLower(strings.TrimSpace(a.Alias))
		if key == "" {
			continue
		}
		if IsContributorModel(a.Name) && !ContributorAllowed() {
			contributorAliases.Store(key, true)
		} else {
			contributorAliases.Delete(key)
		}
	}
	if ContributorAllowed() {
		return models
	}
	out := make([]*registry.ModelInfo, 0, len(models))
	for _, m := range models {
		if m != nil && IsContributorModel(m.ID) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// ContributorErrorBody is the OpenAI-style error body of the refusal.
func ContributorErrorBody(model string) string {
	body := `{"error":{"message":"","type":"invalid_request_error","code":"` + ContributorBlockedCode + `","param":"model"}}`
	msg := ContributorBlockedMessage
	if m := strings.TrimSpace(model); m != "" {
		msg += " (" + m + ")"
	}
	if out, err := sjson.Set(body, "error.message", msg); err == nil {
		return out
	}
	return `{"error":{"message":"` + ContributorBlockedMessage + `","type":"invalid_request_error","code":"` + ContributorBlockedCode + `","param":"model"}}`
}

// ContributorError is the refusal as an error for the executor: HTTP 400 and request-scoped, so
// the credential is neither cooled down nor rotated.
type ContributorError struct{ Model string }

func (e *ContributorError) Error() string         { return ContributorErrorBody(e.Model) }
func (e *ContributorError) StatusCode() int       { return http.StatusBadRequest }
func (e *ContributorError) IsRequestScoped() bool { return true }

// IsContributorError reports whether err is the refusal.
func IsContributorError(err error) bool {
	var ce *ContributorError
	return errors.As(err, &ce)
}

// CheckUpstreamModel is the executor's guard: the model sent to Meta (after alias resolution).
func CheckUpstreamModel(model string) error {
	if ContributorAllowed() || !IsContributorModel(model) {
		return nil
	}
	return &ContributorError{Model: model}
}

// filterCatalog drops Contributor rows from Meta's own catalog (data[].id).
func filterCatalog(body []byte) []byte {
	if ContributorAllowed() {
		return body
	}
	rows := gjson.GetBytes(body, "data").Array()
	kept := make([]string, 0, len(rows))
	dropped := false
	for _, r := range rows {
		if IsContributorModel(r.Get("id").String()) {
			dropped = true
			continue
		}
		kept = append(kept, r.Raw)
	}
	if !dropped {
		return body
	}
	out, err := sjson.SetRawBytes(body, "data", []byte("["+strings.Join(kept, ",")+"]"))
	if err != nil {
		return body
	}
	return out
}
