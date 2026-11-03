package cliproxy

import (
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
)

// tmxInstallCompatSource lets internal/tmx/clients read every alias target of the live config's
// enabled openai-compatibility entries (tokenmaxx fork): an alias pooled over a Claude and a
// non-Claude upstream, bare or prefixed, must stay out of Codex lists and requests.
func (s *Service) tmxInstallCompatSource() {
	clients.SetCompatSource(func() []clients.CompatEntry { return tmxCompatEntries(s.currentConfig()) })
}

func tmxCompatEntries(cfg *config.Config) []clients.CompatEntry {
	if cfg == nil {
		return nil
	}
	out := make([]clients.CompatEntry, 0, len(cfg.OpenAICompatibility))
	for _, c := range cfg.OpenAICompatibility {
		if c.Disabled {
			continue
		}
		e := clients.CompatEntry{Prefix: c.Prefix, Models: make([]clients.CompatModel, 0, len(c.Models))}
		for _, m := range c.Models {
			e.Models = append(e.Models, clients.CompatModel{Name: m.Name, Alias: m.Alias})
		}
		out = append(out, e)
	}
	return out
}
