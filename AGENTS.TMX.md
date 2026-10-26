# CLIProxyAPI (tokenmaxx fork)
Kind: code · Shape: single · Stack: go · Git mode: trunk

`vivinmeth/CLIProxyAPI`, our fork of `router-for-me/CLIProxyAPI`: the Go proxy behind the
tokenmaxx home proxy. `AGENTS.md` is upstream's file and its rules still apply (gofmt, build
check, translator and payload rules); this overlay adds the fork's rules and wins where they
conflict. It stays a separate file so upstream merges don't conflict.

## Map

Work happens in the `CLIProxyAPI/` submodule of the umbrella repo
`~/workspace/vivinmeth/repos/tokenmaxx-proxy`. Its `AGENTS.md` and `wiki/` hold the machine,
ports, Dokploy and secrets; read `../AGENTS.md` before anything that touches deploy or prod. The
separate clone at `~/workspace/vivinmeth/repos/CLIProxyAPI` is parked: don't work there
(`../wiki/parked-fork-clones.md`).

**New fork-only code goes under `internal/tmx/`**; keep edits to upstream files to small hooks
(often a `// tmx` line or a `*_tmx.go` file beside the upstream one). Our changes, which must keep
working:

- **`reset-soonest` routing:** `sdk/cliproxy/auth/selector_reset_soonest*.go`, plus wiring in
  `conductor_selection.go`, `service_config.go`, `config_basic.go`. Within the top priority tier:
  unobserved credentials first, then soonest weekly reset, then already-passed resets. It bypasses
  the scheduler fast path on purpose. `../wiki/routing-reset-soonest.md`.
- **Request log** (`internal/tmx/requestlog/`): usage-plugin hook → batched writes to Postgres
  `tmx.requests`. Routes `GET /v8/management/tmx/requests[/summary]` in
  `internal/api/server_management_tmx.go`; hooks in `server_management_v8.go`, `cmd/server/main.go`.
  `../wiki/observability-plan.md`.
- **Archive** (`internal/tmx/archive/`, `internal/tmx/capture/`): full exchange capture + session
  ingest. Hooks: `capture.Enabled()` in the request-log gates in
  `runtime/executor/helps/logging_helpers.go`, `sdk/api/handlers/handlers.go`, `handlers_errors.go`,
  `openai/openai_responses_websocket.go`, `middleware/request_logging.go`; the response writer calls
  `tmxTee`/`tmxSubmit` (`middleware/response_writer_tmx.go`). Keep these when merging upstream.
  `../wiki/archive-ingest.md`.
- **Analytics** (`internal/tmx/archive/analytics*.go`): derived query layer in its own Postgres
  (`TMX_ANALYTICS_DSN`), fed by `tmx_archive.analytics_journal` written in the same transaction as
  ingest/replace/delete and capture. Routes `/tmx/analytics/{status,backfill,search}`;
  `/tmx/archive/conversation` reads from it when the session is extracted. `../wiki/analytics.md`.
- **PR ingest + review verdicts** (`internal/tmx/archive/pullrequests.go`, `analytics_prs.go`,
  `review.go`; routes in `internal/api/server_management_tmx_review.go`): PRs stored raw and
  versioned in `tmx_archive.pull_request*`, extracted to analytics `prs` / `pr_*`.
- **Reset credits** (`internal/tmx/resets/`): `GET /tmx/resets`, `POST /tmx/resets/poll`,
  `GET /tmx/resets/history`, `POST /tmx/resets/redeem` (verdicts in `verdict.go`; routes + contract
  test in `internal/api/server_management_tmx_resets*.go`; helpers in
  `handlers/management/tmx_resets_backend.go`; hook `sdk/cliproxy/auth/tmx_quota_signals.go`,
  `Manager.ClearQuotaSignals`). `../wiki/reset-credits.md`, fleet contract
  `../wiki/reset-credits-api.md`.
- **Muse Code** (`internal/tmx/muse/`): `GET /muse-code/models` (`internal/api/server_tmx_muse.go`,
  hook in `server_routes.go`); Meta `response.subscription_usage` passed to Muse clients
  (`meta_executor_stream.go`, `isMuseClient`); harness `muse-code` (`ClientID` on
  `logging.ClientRequestMetadata`, set in `sdk/api/handlers/handlers.go`). Quota signals
  (`muse/usage.go`, `meta` in `sdk/cliproxy/auth/quota_signals.go`) read by
  `internal/tmx/quota/meta.go`, which also polls `/muse-code/key`; Meta prices in
  `internal/tmx/pricing`. Contributor block (`muse/contributor.go`): hooks in
  `sdk/api/handlers/handlers_routing.go` (`handlers_routing_tmx.go`), `sdk/cliproxy/service_models.go`
  and the top of the Meta executor's Execute / ExecuteStream / CountTokens. `../wiki/muse-code.md`,
  `../wiki/quota-history.md`.
- **Cooldown history** (`internal/tmx/cooldowns/`): append-only `tmx.cooldown_events`, a backstop
  trigger on `cliproxy.cooldown_store`, `GET /tmx/cooldowns/{history,stats}`. Hooks:
  `sdk/cliproxy/auth/tmx_cooldown_events.go`, `// tmx` lines in `conductor_cooldown.go`, start in
  `service_lifecycle.go`. `../wiki/cooldown-history.md`.
- **Durable spool** (`internal/tmx/spool/`, registry `internal/tmx/durability/`): writers that
  can't reach Postgres append to an fsynced spool under `<archive spool volume>/durable/<writer>`
  and replay; refused items go to `deadletter/`. `GET /v8/management/tmx/durability`. New writers
  call `durability.Register(name, fn)`. `../wiki/durability-spool.md`.

## Done

Go is in `~/.local/go/bin` (often not on `PATH`). Before handing off:

- `gofmt -l` on changed files (`internal/pluginhost/host.go` isn't gofmt-clean upstream; not ours
  to fix), `go build -o /tmp/cpa ./cmd/server`, and `go test` on the packages you touched (e.g.
  `./sdk/cliproxy/... ./internal/api/... ./internal/config/...`).
- Integration tests need throwaway Postgres (`postgres:18-alpine`) DSNs: `TMX_REQUESTLOG_TEST_DSN`,
  `TMX_ANALYTICS_TEST_DSN` (a second database), `TMX_COOLDOWNS_TEST_DSN`.
- `../scripts/dev-services.sh rebuild` and try the change on the dev backend (:8317).

## Project rules

- **Main is deployed:** never rebase or force-push it. `git submodule update` leaves a detached
  HEAD: `git checkout main` and `git pull --ff-only` before committing. After pushing, commit the
  new pointer in the umbrella (`git add CLIProxyAPI`) and push.
- **Upstream:** remote `upstream` = `router-for-me/CLIProxyAPI` (add it if missing). Sync by merge,
  not rebase (`../wiki/upstream-sync.md`). Fork-only guidance goes here, never in upstream's
  `AGENTS.md` or `CLAUDE.md`.
- **The analytics `review` schema is primary data** (append-only, written by role `agents_rw` and
  `/tmx/review/*`): never drop it, and never give review tables foreign keys into, or views over,
  public objects (rebuilds truncate and recreate those). Tests: `review_test.go`.
- **Reset-credit claims** are journaled in `tmx.reset_redemptions` before they are sent; keep that
  order.
- **Dev server:** don't start it with `nohup`/`setsid` from your shell (T3 kills the session's
  processes when it ends); it runs as the user service `tokenmaxx-dev-backend`.
- **Git identity** is set locally (`Vivin Meth <vivinmeth@surkyl.com>`); never change global git
  config.

## Tools

| Command | Does |
|---|---|
| `../scripts/dev-services.sh rebuild` | go build here, restart the dev backend |
| `../scripts/dev-services.sh status backend` | service state; logs in `/tmp/tokenmaxx-backend.log` |
| `../scripts/deploy-backend.sh staging\|prod` | deploy pushed `main` (staging first), verify the running commit; runbook `../wiki/deployment-dokploy.md` |

Dev config is `config.yaml` here (git-ignored); dev keys are in `../.dev-keys`. Secret locations:
`../AGENTS.md` § Project rules. Skill: `go`.
