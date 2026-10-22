# AGENTS.TMX.md — tokenmaxx fork guidance

This repo is `vivinmeth/CLIProxyAPI`, a fork of `router-for-me/CLIProxyAPI` used by the
tokenmaxx home proxy. Everything in `AGENTS.md` (upstream's rules) still applies. This file adds
fork-only rules and overrides `AGENTS.md` where they conflict. It's kept separate so upstream
merges don't conflict with it.

## Where things are

Work happens in the **`CLIProxyAPI/` submodule of the umbrella repo**
`~/workspace/vivinmeth/repos/tokenmaxx-proxy`. The umbrella's `AGENTS.md` and `wiki/` hold the
full context (machine, network, ports, Dokploy, secrets); read `../AGENTS.md` before anything that
touches deploy, prod, or secrets.

(A separate clone at `~/workspace/vivinmeth/repos/CLIProxyAPI` and a matching T3 project exist
but are **parked**. Don't work there; see `../wiki/parked-fork-clones.md`.)

## Our changes vs upstream (keep these working)

- `reset-soonest` routing strategy: `sdk/cliproxy/auth/selector_reset_soonest*.go`, plus wiring
  in `conductor_selection.go`, `service_config.go`, `config_basic.go`. Within the top priority
  tier: unobserved credentials first, then soonest weekly reset, then already-passed resets. It
  bypasses the scheduler fast path on purpose. Details: `../wiki/routing-reset-soonest.md`.

- **Request log** (`internal/tmx/requestlog/`): usage-plugin hook → buffered batch writes to
  Postgres `tmx.requests` (outcomes success/failed/canceled, tokens incl. cache, latency, account,
  client). Routes `GET /v8/management/tmx/requests[/summary]` in `internal/api/server_management_tmx.go`.
  Upstream files only carry one-line hooks (`server_management_v8.go`, `cmd/server/main.go`).
  Integration test needs `TMX_REQUESTLOG_TEST_DSN` (throwaway `postgres:18-alpine` container).
  Details: `../wiki/observability-plan.md`.
- **Archive** (`internal/tmx/archive/`, `internal/tmx/capture/`): full exchange capture + session
  ingest. Upstream hooks: `capture.Enabled()` added to the request-log gates in
  `runtime/executor/helps/logging_helpers.go`, `sdk/api/handlers/handlers.go`, `handlers_errors.go`,
  `openai/openai_responses_websocket.go`, `middleware/request_logging.go`; the response writer calls
  `tmxTee`/`tmxSubmit` (logic in `middleware/response_writer_tmx.go`). Keep these when merging
  upstream. Contract: `../wiki/archive-ingest.md`.
- **Analytics** (`internal/tmx/archive/analytics*.go`): derived query layer in its own Postgres
  (`TMX_ANALYTICS_DSN`). `tmx_archive.analytics_journal` is written in the same transaction as
  session ingest/replace/delete and exchange capture; the worker extracts sessions / messages /
  items and mirrors `tmx.requests` + `exchanges`. Routes `/tmx/analytics/{status,backfill,search}`;
  `/tmx/archive/conversation` reads from it when the session is extracted. Integration tests need
  `TMX_ANALYTICS_TEST_DSN` (a second throwaway database). Details: `../wiki/analytics.md`.
- **PR ingest + review verdicts** (`internal/tmx/archive/pullrequests.go`, `analytics_prs.go`,
  `review.go`; routes in `internal/api/server_management_tmx_review.go`): GitHub PRs stored raw
  (blobs) + versioned in `tmx_archive.pull_request*`, extracted to analytics `prs` / `pr_*`. The
  analytics schema **`review`** is primary data (append-only, written by role `agents_rw` and
  `/tmx/review/*`): never drop it, and never give review tables foreign keys into / views over
  public objects (rebuilds truncate and recreate those). Tests: `review_test.go`.
- **Reset credits** (`internal/tmx/resets/`): list/redeem Codex + Claude banked resets,
  `GET /tmx/resets`, `POST /tmx/resets/poll`, `GET /tmx/resets/history`, `POST /tmx/resets/redeem`
  (fleet contract `../wiki/reset-credits-api.md`; verdicts in `verdict.go`; routes + contract test in
  `internal/api/server_management_tmx_resets*.go`,
  one hook line in `server_management_v8.go`; handler helpers in
  `handlers/management/tmx_resets_backend.go`). Every claim is journaled in `tmx.reset_redemptions`
  before it is sent. Upstream-package hook: `sdk/cliproxy/auth/tmx_quota_signals.go`
  (`Manager.ClearQuotaSignals`). Details: `../wiki/reset-credits.md`.
- **Muse Code** (`internal/tmx/muse/`): `GET /muse-code/models` (route in
  `internal/api/server_tmx_muse.go`, one hook line in `server_routes.go`), Meta
  `response.subscription_usage` passed to Muse clients (`meta_executor_stream.go` +
  `isMuseClient` on the Responses SSE framer), harness `muse-code` (`ClientID` on
  `logging.ClientRequestMetadata`, set in `sdk/api/handlers/handlers.go`). Details: `../wiki/muse-code.md`.
- **Cooldown history** (`internal/tmx/cooldowns/`): append-only `tmx.cooldown_events` (windows as
  start/update/end sharing a `cooldown_id`, plus 429 / usage-limit / auth-error / 529 events), a
  backstop trigger on `cliproxy.cooldown_store`, `GET /tmx/cooldowns/{history,stats}`. Upstream
  hooks: `sdk/cliproxy/auth/tmx_cooldown_events.go` + `// tmx` lines in `conductor_cooldown.go`,
  start in `service_lifecycle.go`. Integration tests need `TMX_COOLDOWNS_TEST_DSN`.
- **Durable spool** (`internal/tmx/spool/`, registry `internal/tmx/durability/`): writers that
  can't reach Postgres (request log, quota snapshots, archive capture) append to an fsynced
  on-disk spool under `<archive spool volume>/durable/<writer>` and replay with backoff; refused
  items go to `deadletter/` (never deleted). Counters: `GET /v8/management/tmx/durability`. New
  writers register with `durability.Register(name, fn)`. Details: `../wiki/durability-spool.md`.
- Put new fork-only code under `internal/tmx/` and keep upstream-file edits to small hooks.

## Git

- Identity is set locally (`Vivin Meth <vivinmeth@surkyl.com>`). Never change global git config.
- `git submodule update` leaves this checkout on a detached HEAD: `git checkout main` (and
  `git pull --ff-only`) before committing.
- Committing and pushing straight to `main` is fine. `main` is published and deployed from:
  never force-push or rebase it.
- Remotes: `origin` = fork, `upstream` = `router-for-me/CLIProxyAPI` (add it if missing). Sync by
  merge, not rebase (`../wiki/upstream-sync.md`).
- After pushing, commit the new submodule pointer in the umbrella (`git add CLIProxyAPI`) and push.

## Dev backend (port 8317)

This checkout runs the dev backend as the user systemd service `tokenmaxx-dev-backend`.
`config.yaml` (git-ignored) lives here; dev keys are in `../.dev-keys`.

```sh
../scripts/dev-services.sh rebuild        # go build here, then restart the service
../scripts/dev-services.sh status backend # logs: /tmp/tokenmaxx-backend.log
```

- Don't start the server with `nohup`/`setsid` from your shell. T3 kills every process in an
  agent's session scope when the session ends, so it would die. Use the service.
- Checks before handing off (Go is in `~/.local/go/bin`, which may not be on PATH): `gofmt -l` on
  changed files, `go build -o /tmp/cpa ./cmd/server`, targeted `go test` (e.g.
  `./sdk/cliproxy/... ./internal/api/... ./internal/config/...`). `internal/pluginhost/host.go`
  isn't gofmt-clean upstream; that isn't ours to fix.

## Deploy

Prod (Dokploy app `proxy`, port 8318) builds from `origin/main` of this fork. Push, then trigger
the deploy with `../scripts/dokploy.sh` (runbook: `../wiki/deployment-dokploy.md`). Ask the owner
before anything destructive.

## Secrets

Never print, commit, or paste keys. They live in `../.dev-keys`, `../dockploy-api-key`, and
`~/.local/share/tokenmaxx-deploy/secrets.env`; refer to them by variable name.
