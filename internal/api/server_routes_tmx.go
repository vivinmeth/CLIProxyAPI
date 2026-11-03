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
