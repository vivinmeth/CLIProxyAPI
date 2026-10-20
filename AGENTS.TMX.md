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
- **Reset credits** (`internal/tmx/resets/`): list/redeem Codex + Claude banked resets,
  `GET /tmx/resets`, `POST /tmx/resets/poll`, `GET /tmx/resets/history`, `POST /tmx/resets/redeem`
  (fleet contract `../wiki/reset-credits-api.md`; verdicts in `verdict.go`; routes + contract test in
  `internal/api/server_management_tmx_resets*.go`,
  one hook line in `server_management_v8.go`; handler helpers in
  `handlers/management/tmx_resets_backend.go`). Every claim is journaled in `tmx.reset_redemptions`
  before it is sent. Upstream-package hook: `sdk/cliproxy/auth/tmx_quota_signals.go`
  (`Manager.ClearQuotaSignals`). Details: `../wiki/reset-credits.md`.
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
