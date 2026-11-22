# xAI billing quota reader

The fork reads `GET https://cli-chat-proxy.grok.com/v1/billing?format=credits`
on the existing quota scheduler, normally every ten minutes with per-account
jitter. Cached `/v8/management/tmx/quota/current` and quota history use the
existing window fields:

| Field | Meaning |
| --- | --- |
| `window` | `7d` for `USAGE_PERIOD_TYPE_WEEKLY`, `monthly` for `USAGE_PERIOD_TYPE_MONTHLY`, otherwise `billing` |
| `used_pct` | Included allowance used, or JSON `null` when unknown |
| `starts_at` | Exact supplied start in UTC; omitted when unknown |
| `resets_at` | Exact supplied end in UTC; JSON `null` when unknown |
| `observed_at`, `source` | Existing observation timestamp and `polled` source |

Monthly periods have no fixed 30-day duration. A percentage alone does not imply
a start, reset or duration. Missing percentages do not mean zero usage or full
remaining allowance. A successful change of period replaces the previous included
pool in current state; recorded history remains.

The reader prefers `config.creditUsagePercent` (numeric, 0 through 100) and
`config.currentPeriod.{type,start,end}`. With no percentage it can derive usage
from legacy `used.val / monthlyLimit.val`, only with both objects present,
nonnegative usage and a positive limit. The vendor documents `used: {}` as proto3
zero; an omitted or null `used` object remains unknown. Explicit null, fractional
or nonnumeric `val` is malformed. Zero limit cannot produce a percentage.
Legacy `billingPeriodStart` and `billingPeriodEnd` apply only without a newer
`currentPeriod` object. Invalid timestamps or reversed/equal bounds reject the
reading and preserve earlier evidence. Unknown values in an otherwise usable
reading stay unknown. Empty or malformed replies do not refresh observations.

`onDemandCap`, `onDemandUsed`, `prepaidBalance`, per-product usage and billing
history are separate from included allowance. They do not enter subscription
windows. Request/token rate-limit headers also do not define subscription windows.
No subscription tier is inferred and no settings/profile request is made.

The dedicated `TmxXAIBilling` transport uses only the auth manager's current,
enabled xAI OAuth snapshot, with matching registration and credential versions,
a nonempty access token and subject, and a future `expired` timestamp. Unknown or
expired credentials fail closed until the normal OAuth lifecycle supplies usable
credentials. The reader never acquires or refreshes a token, updates auth metadata,
or falls back to the generic management API-call path. It preserves account/global
proxy selection. Requests use CLI version `1.0.89`, public `grok-shell` User-Agent,
`headless` mode, bearer auth, `X-XAI-Token-Auth: xai-grok-cli` and same-snapshot
`x-userid`. No redirects or explicit retries are allowed.

A billing 401 stops polling that registration immediately, including manual polls.
The stop is **memory-only**: token rotation and disabling/re-enabling do not clear
it, but explicit re-registration or process restart does. It does not persist or
change account disabled/retry/first-401 metadata. Other 4xx responses pause for an
hour; 429 pauses for thirty minutes. Failures preserve earlier observations. Sent
identity checks reject results after credential replacement or account disable.

Source: [xai-org/grok-build at 2bdd1d6](https://github.com/xai-org/grok-build/blob/2bdd1d6a6369de0e8c68132ea4539e9abd9e14a8/crates/codegen/xai-grok-shell/src/extensions/billing.rs),
lines 20–109 and 182–208; shared-client headers and User-Agent in
`crates/codegen/xai-grok-http/src/lib.rs`, lines 114–190 and 250–274; public
`Generic` client label in `xai-grok-workspace/src/permission/types.rs`, lines 60–99.
Live account billing behavior awaits captain-authorized validation after the
bundled release; synthetic and loopback checks cannot prove that payload.
