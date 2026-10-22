package requestlog

import (
	"context"
	"testing"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	coreusage "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func TestRowFromRecordLabelsMuseByClientID(t *testing.T) {
	ctx := internallogging.WithClientRequestMetadata(context.Background(), internallogging.ClientRequestMetadata{
		UserAgent: "some-wrapper/1.0", ClientID: "tbh:serve",
	})
	row := rowFromRecord(ctx, coreusage.Record{Provider: "meta", Model: "muse-spark-1.3"})
	if row.Harness != "muse-code" || row.ClientMode != "serve" {
		t.Fatalf("harness/mode = %q/%q, want muse-code/serve", row.Harness, row.ClientMode)
	}
}
