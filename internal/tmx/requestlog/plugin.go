package requestlog

import (
	"context"

	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func init() {
	coreusage.RegisterPlugin(usagePlugin{})
}

// usagePlugin forwards every usage record to the running store; a no-op while disabled.
type usagePlugin struct{}

func (usagePlugin) HandleUsage(ctx context.Context, record coreusage.Record) {
	store := Default()
	if store == nil {
		return
	}
	store.Enqueue(rowFromRecord(ctx, record))
}
