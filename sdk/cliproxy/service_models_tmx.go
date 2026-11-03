package cliproxy

import (
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
)

// tmxNoteAliasTargets records every upstream name each alias of an openai-compatibility entry
// routes to (tokenmaxx fork; internal/tmx/clients): an alias pooled over a Claude and a non-Claude
// upstream must stay out of Codex lists, and the registry keeps only the first one's metadata.
func tmxNoteAliasTargets(compat *config.OpenAICompatibility) {
	if compat == nil {
		return
	}
	targets := map[string][]string{}
	for _, m := range compat.Models {
		name := strings.TrimSpace(m.Name)
		alias := strings.TrimSpace(m.Alias)
		if alias == "" {
			alias = name
		}
		if alias != "" && name != "" {
			targets[alias] = append(targets[alias], name)
		}
	}
	clients.SetAliasTargets(compat.Name, targets)
}
