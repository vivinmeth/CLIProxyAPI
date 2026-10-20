# AGENTS.TMX.md — tokenmaxx fork guidance

This checkout is `vivinmeth/CLIProxyAPI`, a fork of `router-for-me/CLIProxyAPI` used by the
tokenmaxx home proxy. Everything in `AGENTS.md` (upstream's rules) still applies. This file adds
fork-only rules and overrides `AGENTS.md` where they conflict. It's kept separate so upstream
merges don't conflict with it.

## Where things are

| What | Path |
|---|---|
| This repo (working clone, T3 project `CLIProxyAPI`) | `~/workspace/vivinmeth/repos/CLIProxyAPI` |
| UI fork (working clone, T3 project `Cli-Proxy-API-Management-Center`) | `~/workspace/vivinmeth/repos/Cli-Proxy-API-Management-Center` |
| Umbrella repo (orchestration, wiki, deploy, pins) | `~/workspace/vivinmeth/repos/tokenmaxx-proxy` |

The umbrella's `AGENTS.md` and `wiki/` hold the full context (machine, network, ports, Dokploy,
secrets). Read `tokenmaxx-proxy/AGENTS.md` before anything that touches deploy, prod, or
secrets. The umbrella's `CLIProxyAPI/` submodule is a **read-only pin**: never edit or commit
there; all work happens in this clone.

## Our changes vs upstream (keep these working)

- `reset-soonest` routing strategy: `sdk/cliproxy/auth/selector_reset_soonest*.go`, plus wiring
  in `conductor_selection.go`, `service_config.go`, `config_basic.go`. Within the top priority
  tier: unobserved credentials first, then soonest weekly reset, then already-passed resets. It
  bypasses the scheduler fast path on purpose. Details: `tokenmaxx-proxy/wiki/routing-reset-soonest.md`.

## Git

- Identity is set locally (`Vivin Meth <vivinmeth@surkyl.com>`). Never change global git config.
- Committing and pushing straight to `main` is fine; branches and worktrees are optional (use
  them for parallel work). `main` is published and deployed from: never force-push or rebase it.
- Remotes: `origin` = fork, `upstream` = `router-for-me/CLIProxyAPI`. Sync by merge, not rebase
  (`tokenmaxx-proxy/wiki/upstream-sync.md`).
- After pushing, record the new commit in the umbrella:
  `~/workspace/vivinmeth/repos/tokenmaxx-proxy/scripts/bump-pins.sh` (then commit + push there).
  Only pushed commits may be pinned.

## Dev backend (port 8317)

The main checkout of this clone **is** the dev backend: `config.yaml` (git-ignored) lives here,
and dev keys are in `tokenmaxx-proxy/.dev-keys`.

```sh
export PATH=$HOME/.local/go/bin:$PATH        # Go >= 1.26
go build -o cli-proxy-api.new ./cmd/server && pkill -x cli-proxy-api; mv cli-proxy-api.new cli-proxy-api
setsid nohup ./cli-proxy-api --config config.yaml --no-browser > /tmp/tokenmaxx-backend.log 2>&1 < /dev/null &
```

- Checks before handing off: `gofmt -l` on changed files, `go build -o /tmp/cpa ./cmd/server`,
  targeted `go test` (e.g. `./sdk/cliproxy/... ./internal/api/... ./internal/config/...`).
  `internal/pluginhost/host.go` isn't gofmt-clean upstream; that isn't ours to fix.
- **In a worktree, don't touch the dev server.** Port 8317 belongs to the main checkout. Build to
  `/tmp`, run tests, and if you must run a server, use another port and your own config.
- Start long-running processes with `setsid nohup … &` (harness tasks get killed). Never
  `pkill -f <pattern>` from a shell whose command line contains the pattern; use `pkill -x` or a PID.

## Deploy

Prod (Dokploy app `proxy`, port 8318) builds from `origin/main` of this fork. Push, then trigger
the deploy from the umbrella: `tokenmaxx-proxy/scripts/dokploy.sh` (runbook:
`tokenmaxx-proxy/wiki/deployment-dokploy.md`). Ask the owner before anything destructive.

## Secrets

Never print, commit, or paste keys. They live in `tokenmaxx-proxy/.dev-keys`,
`tokenmaxx-proxy/dockploy-api-key`, and `~/.local/share/tokenmaxx-deploy/secrets.env`; refer to
them by variable name.
