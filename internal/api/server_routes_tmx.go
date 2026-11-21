package api

import (
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/tmx/clients"
)

// tmxListedModelInfos drops models hidden from every list (OpenCode Zen; internal/tmx/clients).
func tmxListedModelInfos(infos []*registry.ModelInfo) []*registry.ModelInfo {
	out := make([]*registry.ModelInfo, 0, len(infos))
	for _, info := range infos {
		if info != nil && clients.Hidden(info.ID) {
			continue
		}
		out = append(out, info)
	}
	return out
}

func tmxGrokRegistryModelAllowed(id string) bool {
	return !clients.Hidden(id) && clients.ResolvesOnlyToXAI(id, nil)
}

// Home is authoritative for its catalog, including unknown or mixed routes. Do not
// substitute local registry metadata when Home's route evidence is incomplete.
func tmxGrokHomeModelAllowed(entry homeModelEntry, providers []string) bool {
	if clients.Hidden(entry.id) || !clients.OnlyXAI(providers) {
		return false
	}
	for _, route := range entry.routes {
		if !clients.OnlyXAI([]string{route.provider}) {
			return false
		}
	}
	return true
}
