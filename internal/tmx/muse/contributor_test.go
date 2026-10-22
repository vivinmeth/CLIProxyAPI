package muse

import (
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/tidwall/gjson"
)

func TestContributorBlockedByDefault(t *testing.T) {
	t.Setenv("TMX_META_CONTRIBUTOR", "")
	models := []*registry.ModelInfo{{ID: "muse-spark-1.3"}, {ID: "muse-spark-1.3-contributor"}, {ID: "muse-wonder-full-1.0-contributor"}}
	aliases := []config.OAuthModelAlias{{Name: "muse-spark-1.3-contributor", Alias: "spark-c"}, {Name: "muse-spark-1.3", Alias: "spark"}}
	got := DropContributorModels(models, aliases)
	if len(got) != 1 || got[0].ID != "muse-spark-1.3" {
		t.Fatalf("kept %v", got)
	}
	for model, want := range map[string]bool{
		"muse-spark-1.3-contributor": true, "MUSE-Wonder-1.0-Contributor(high)": true, "spark-c": true,
		"muse-spark-1.3": false, "spark": false, "gpt-5.5": false, "": false,
	} {
		if ContributorRequestBlocked(model) != want {
			t.Errorf("ContributorRequestBlocked(%q) = %v, want %v", model, !want, want)
		}
	}
	err := CheckUpstreamModel("muse-spark-1.2-contributor")
	if err == nil || !IsContributorError(err) {
		t.Fatalf("upstream guard = %v", err)
	}
	ce := err.(*ContributorError)
	if ce.StatusCode() != 400 || !ce.IsRequestScoped() || gjson.Get(err.Error(), "error.code").String() != ContributorBlockedCode ||
		!strings.HasPrefix(gjson.Get(err.Error(), "error.message").String(), ContributorBlockedMessage) {
		t.Fatalf("refusal = %d %s", ce.StatusCode(), err)
	}
	if CheckUpstreamModel("muse-spark-1.3") != nil {
		t.Fatal("regular model refused")
	}
	cat := []byte(`{"object":"list","data":[{"id":"muse-spark-1.3"},{"id":"muse-spark-1.3-contributor"}]}`)
	if ids := gjson.GetBytes(filterCatalog(cat), "data.#.id").String(); ids != `["muse-spark-1.3"]` {
		t.Fatalf("catalog ids = %s", ids)
	}
}

func TestContributorAllowSwitch(t *testing.T) {
	t.Setenv("TMX_META_CONTRIBUTOR", "allow")
	models := []*registry.ModelInfo{{ID: "muse-spark-1.3"}, {ID: "muse-spark-1.3-contributor"}}
	if got := DropContributorModels(models, []config.OAuthModelAlias{{Name: "muse-spark-1.3-contributor", Alias: "spark-c2"}}); len(got) != 2 {
		t.Fatalf("allow dropped models: %v", got)
	}
	if ContributorRequestBlocked("muse-spark-1.3-contributor") || ContributorRequestBlocked("spark-c2") || CheckUpstreamModel("muse-spark-1.3-contributor") != nil {
		t.Fatal("allow still blocks")
	}
	cat := []byte(`{"data":[{"id":"muse-spark-1.3-contributor"}]}`)
	if string(filterCatalog(cat)) != string(cat) {
		t.Fatal("allow filtered the catalog")
	}
}
