TL;DR: This note generalizes the two-line hedge model for any pair of net-profit payout multipliers. Model 1 makes the favorite outcome break even and leaves the underdog as the upside case. Model 2 makes the underdog outcome break even and therefore minimizes loss on that side as tightly as possible.

# Wager Hedge Models for Two Outcomes

## Notation

Let:

- $u$ = underdog payout multiplier
- $f$ = favorite payout multiplier
- $x$ = stake on the underdog
- $y$ = stake on the favorite
- $T = x + y$ = total amount staked

This document assumes the quoted payouts are **net-profit multipliers**.

That means:

- if the underdog wins, net result is
  $$
  \Pi_U = ux - y
  $$
- if the favorite wins, net result is
  $$
  \Pi_F = fy - x
  $$

If your source uses decimal odds that include return of stake, convert first:

$$
u = d_U - 1, \qquad f = d_F - 1
$$

---

## Model 1: Favorite Break-Even, Underdog Upside

Use this when the goal is:

- favorite outcome should break even
- underdog outcome should be the profit case

### Constraint

Set favorite outcome to zero:

$$
fy - x = 0
$$

So:

$$
x = fy
$$

### Allocation from total bankroll $T$

$$
x = \frac{f}{1+f}T
$$

$$
y = \frac{1}{1+f}T
$$

### Outcomes

- if favorite wins:
  $$
  \Pi_F = 0
  $$
- if underdog wins:
  $$
  \Pi_U = \frac{uf - 1}{1+f}T
  $$

### Ratio form

$$
x:y = f:1
$$

---

## Model 2: Underdog Break-Even, Minimum Underdog-Side Loss

Use this when the goal is:

- underdog outcome should be as close to break even as possible
- ideally the underdog side itself lands exactly at zero

### Constraint

Set underdog outcome to zero:

$$
ux - y = 0
$$

So:

$$
y = ux
$$

### Allocation from total bankroll $T$

$$
x = \frac{1}{1+u}T
$$

$$
y = \frac{u}{1+u}T
$$

### Outcomes

- if underdog wins:
  $$
  \Pi_U = 0
  $$
- if favorite wins:
  $$
  \Pi_F = \frac{uf - 1}{1+u}T
  $$

### Ratio form

$$
x:y = 1:u
$$

---

## Quick Comparison

| Model   | Underdog stake $x$ | Favorite stake $y$ |    If underdog wins |    If favorite wins |
| ------- | -----------------: | -----------------: | ------------------: | ------------------: |
| Model 1 |   $\frac{f}{1+f}T$ |   $\frac{1}{1+f}T$ | $\frac{uf-1}{1+f}T$ |                 $0$ |
| Model 2 |   $\frac{1}{1+u}T$ |   $\frac{u}{1+u}T$ |                 $0$ | $\frac{uf-1}{1+u}T$ |

---

## Worked Examples

For each example below, I use a total stake of $T = \$100$.

## Example A: Payouts $3.25 / 1.25$

Here:

- $u = 3.25$
- $f = 1.25$

### Model 1

$$
x = \frac{1.25}{2.25}(100) = 55.56
$$

$$
y = \frac{1}{2.25}(100) = 44.44
$$

Outcomes:

- underdog wins:
  $$
  3.25(55.56) - 44.44 \approx 136.13
  $$
- favorite wins:
  $$
  1.25(44.44) - 55.56 \approx 0.00
  $$

### Model 2

$$
x = \frac{1}{4.25}(100) = 23.53
$$

$$
y = \frac{3.25}{4.25}(100) = 76.47
$$

Outcomes:

- underdog wins:
  $$
  3.25(23.53) - 76.47 \approx 0.00
  $$
- favorite wins:
  $$
  1.25(76.47) - 23.53 \approx 72.06
  $$

---

## Example B: Payouts $2.05 / 1.60$

Here:

- $u = 2.05$
- $f = 1.60$

### Model 1

$$
x = \frac{1.60}{2.60}(100) = 61.54
$$

$$
y = \frac{1}{2.60}(100) = 38.46
$$

Outcomes:

- underdog wins:
  $$
  2.05(61.54) - 38.46 \approx 87.69
  $$
- favorite wins:
  $$
  1.60(38.46) - 61.54 \approx 0.00
  $$

### Model 2

$$
x = \frac{1}{3.05}(100) = 32.79
$$

$$
y = \frac{2.05}{3.05}(100) = 67.21
$$

Outcomes:

- underdog wins:
  $$
  2.05(32.79) - 67.21 \approx 0.00
  $$
- favorite wins:
  $$
  1.60(67.21) - 32.79 \approx 74.75
  $$

---

## Example C: Payouts $1.67$ and $3.80$

To preserve the notation that the underdog has the larger payout, I normalize this pair as:

- $u = 3.80$
- $f = 1.67$

If you intended the reverse labeling, swap $u$ and $f$ in the formulas.

### Model 1

$$
x = \frac{1.67}{2.67}(100) = 62.55
$$

$$
y = \frac{1}{2.67}(100) = 37.45
$$

Outcomes:

- underdog wins:
  $$
  3.80(62.55) - 37.45 \approx 200.24
  $$
- favorite wins:
  $$
  1.67(37.45) - 62.55 \approx 0.00
  $$

### Model 2

$$
x = \frac{1}{4.80}(100) = 20.83
$$

$$
y = \frac{3.80}{4.80}(100) = 79.17
$$

Outcomes:

- underdog wins:
  $$
  3.80(20.83) - 79.17 \approx 0.00
  $$
- favorite wins:
  $$
  1.67(79.17) - 20.83 \approx 111.39
  $$

---

## Practical Notes

- Model 1 is the right choice when you want the favorite line to behave like insurance and the underdog line to carry the upside.
- Model 2 is the right choice when you want the underdog side to avoid turning into a negative result; it forces the underdog outcome to break even exactly.
- The product $uf$ is the key feasibility check.
  - If $uf > 1$, both models produce a nonnegative opposite-side outcome.
  - If $uf = 1$, both sides can be tuned to break even.
  - If $uf < 1$, one side must remain negative no matter how you hedge.

---

## Plug-In Recipe

Given any pair of net-profit multipliers $(u, f)$ and total stake $T$:

### Model 1

$$
x = \frac{f}{1+f}T, \qquad y = \frac{1}{1+f}T
$$

### Model 2

$$
x = \frac{1}{1+u}T, \qquad y = \frac{u}{1+u}T
$$

This gives a reusable two-outcome staking template for any payout pair.

---

## Model 3: Guaranteed / Equal-Profit Hedge

The first two models intentionally protect one outcome more than the other:

- Model 1 sets the favorite outcome to break even.
- Model 2 sets the underdog outcome to break even.

The combined model sets both outcomes equal and therefore maximizes the worst-case result.

### Constraint

Set the two net outcomes equal:

$$
ux - y = fy - x
$$

Rearrange:

$$
(u+1)x = (f+1)y
$$

### Allocation from total bankroll $T$

$$
x = \frac{f+1}{u+f+2}T
$$

$$
y = \frac{u+1}{u+f+2}T
$$

### Guaranteed result

Under this allocation, both outcomes are identical:

$$
\Pi_U = \Pi_F = \Pi_G
$$

where

$$
\Pi_G = \frac{uf - 1}{u+f+2}T
$$

### Feasibility condition

- If $uf > 1$, Model 3 guarantees positive profit.
- If $uf = 1$, Model 3 guarantees break even.
- If $uf < 1$, Model 3 cannot guarantee a nonnegative result.

### Ratio form

$$
x:y = (f+1):(u+1)
$$

---

## Kalshi-Specific Translation of the Guaranteed Model

Kalshi event markets are binary contracts. A YES contract settles at $\$1.00$ if YES is correct and $\$0.00$ otherwise. A NO contract settles at $\$1.00$ if NO is correct and $\$0.00$ otherwise.

That means the Kalshi version of the guaranteed A+B model is not driven by arbitrary payout multipliers. It is driven by two entry prices into the two complementary sides of the same market.

### Definitions

For a single Kalshi market $M$:

- $p_Y$ = average fill price paid to buy YES
- $p_N$ = average fill price paid to buy NO
- $q$ = matched contracts on each side after reconciliation
- $F$ = total fees and rounding deductions for both legs combined

The total cost to acquire the pair is:

$$
C = q(p_Y + p_N) + F
$$

The total settlement payout is always:

$$
P = q
$$

because exactly one of YES or NO settles to $\$1.00$ per contract and the other settles to $\$0.00$.

So the guaranteed net result is:

$$
\Pi_{Kalshi} = q - q(p_Y + p_N) - F
$$

or equivalently:

$$
\Pi_{Kalshi} = q(1 - p_Y - p_N) - F
$$

### Guaranteed-profit condition on Kalshi

The pair is guaranteed profitable only if:

$$
q(1 - p_Y - p_N) - F > 0
$$

For a per-contract condition, define fee reserve per paired contract as $f_r = F / q$. Then:

$$
p_Y + p_N + f_r < 1
$$

### Important market-structure implication

If you immediately cross the spread on both sides using current asks, the sum usually will not be below $1.00$. In a standard Kalshi orderbook:

- YES ask is implied by $1 -$ best NO bid
- NO ask is implied by $1 -$ best YES bid

So an immediate taker pair usually costs:

$$
(1 - b_N) + (1 - b_Y) = 2 - b_Y - b_N
$$

which is usually greater than $1.00$.

Therefore, the realistic Kalshi guaranteed-payout strategy is a **paired maker-entry strategy**:

1. post passive buy orders on both YES and NO,
2. require the target bid prices to satisfy the guaranteed-profit inequality,
3. allow the pair only when equal contract counts are filled on both sides.

### Orderbook translation

For Kalshi orderbook response `orderbook_fp`:

- best YES bid = last price in `yes_dollars`
- best NO bid = last price in `no_dollars`
- implied YES ask = $1 -$ best NO bid
- implied NO ask = $1 -$ best YES bid

Let:

- $b_Y$ = best YES bid
- $b_N$ = best NO bid

If the system posts paired resting bids at prices $p_Y \le b_Y$ and $p_N \le b_N$, then the scanner must require:

$$
p_Y + p_N + f_r + m \le 1
$$

where:

- $f_r$ = configured conservative fee reserve per paired contract
- $m$ = configured minimum profit floor per paired contract

This is the production screening inequality.

### Production safety addendum - 2026-07-02 - terminal pre-wire coverability

The current Polyventure live bridge is no longer described by the economic inequality alone.

The inequality remains the economic entry condition, but live submit requires an additional terminal
pre-wire coverability pass immediately before final sizing and before any pair plan or order intent is
persisted. The production shape is:

1. upstream scan/ranking proposes a saved set,
2. `_prepare_bridge_submit_survivors(...)` performs final live readback for each upstream survivor,
3. each evaluated candidate emits `runtime_events.event_type = 'submit_bridge_final_coverability_checked'`,
4. candidates that fail final price availability, divergence, static coverability, dynamic sizing, pair-plan validation, or flow coverability are removed from the final submit set,
5. the helper computes the final sizing summary only from the remaining final set,
6. the helper emits `runtime_events.event_type = 'submit_bridge_final_sizing_resolved'`,
7. downstream submit planning proceeds only for `final_submit_tickers`.

This is a fail-closed money-path boundary. If the final set is empty, no pair plans and no orders are created.

Recent live monitored proof on 2026-07-02 exercised the negative/no-order path:

- final coverability evidence was emitted in both fresh live runs,
- final sizing reconciliation was emitted in both fresh live runs,
- `final_submit_tickers = []` and `qualifying_candidate_count = 0` in both fresh live runs,
- no pair plans and no orders were created,
- observed blocks were legitimate final-risk blocks (`coverability_divergence_blocked` and `live_price_unavailable`).

Therefore, the guaranteed-profit model should be read as necessary but not sufficient for live submit.
Terminal coverability is the live admission authority at the last local decision boundary.

---

## Python Application Plan: Kalshi Paired-Hedge Trader

This section defines a zero-ambiguity implementation plan for a Python application that scans Kalshi markets, identifies paired YES+NO opportunities near market close, and executes only paired orders whose combined entry cost satisfies the guaranteed-profit inequality after conservative fee reserves.

This plan is for a Python application that starts in the **demo** environment and does not enable production trading until the demo validation gates all pass.

## Execution governance rule

This plan is the active authority surface for the Kalshi paired-trader build.

Execution must follow these rules:

1. the goals, requirements, acceptance gates, and operator-facing promises in this plan are locked as written,
2. lateral implementation deltas may be used only when they are necessary to achieve the documented goal set without changing it,
3. lateral implementation deltas must not redefine project requirements, success criteria, scope boundaries, or milestone meaning,
4. structural changes, scope additions, scope demotions, and user-facing behavior changes require explicit discussion and approval before they are treated as accepted execution,
5. implementation evidence, execution summaries, and retained artifacts measure progress against this plan and may not rewrite the plan to match implementation drift.

## Superseding addendum - 2026-05-07 - operator-facing execution modes are offline / sandbox / live

TL;DR: The operator-facing execution path is now relocked to three modes: `offline`, `sandbox`, and `live`. `demo` is rejected as user-facing wording because it poorly describes what the non-live path actually does. Existing `demo` references in this authority document should be treated as legacy compatibility terminology unless they are specifically describing internal implementation plumbing that has not yet migrated.

### Locked user-facing mode meanings

- `offline`
  - setup, readiness, credential posture, websocket URL setup, and adjustable-parameter review
  - should guide the user through recommended defaults and corrective next actions
- `sandbox`
  - automated large-scale non-live execution lane
  - should collect both optimal and intentionally nonoptimal picks so analytics and weight adaptation have real comparative evidence
  - should minimize operator-initiated steps after launch
  - should automatically update weights once collection and validation thresholds are met
- `live`
  - final gated implementation lane
  - should be offered only when safety and validation thresholds are satisfied

### Execution-plan consequence

Where this plan previously used `demo` as a human-facing execution concept, read that as:

- `offline` when the text is talking about setup/readiness/guided non-execution work,
- `sandbox` when the text is talking about the non-live execution and evidence-collection lane.

The new top-tier user path is therefore:

1. `offline`
2. `sandbox`
3. `live`

## Product Objective

Build a Python service that:

1. continuously scans Kalshi open markets,
2. filters to markets close to expiration,
3. ranks candidates with large YES/NO price asymmetry and positive paired edge,
4. submits paired passive buy orders on YES and NO only when the minimum guaranteed-profit condition is satisfied,
5. reconciles fills until equal matched quantity exists on both sides,
6. cancels or hedges unmatched exposure within a strict timeout,
7. records realized and projected P&L per pair,
8. runs in demo first and then optionally production with identical code paths and different configuration.

## Execution Modes

Version 1 must implement exactly three strategy modes.

### Mode `ab_guarded` — default run mode

This is the default startup mode.

Purpose:

- scan for paired YES+NO entries,
- require guaranteed positive projected edge after fee reserve,
- place paired orders only when the A+B inequality is satisfied.

Required entry rule:

$$
p_Y + p_N + KALSHI\_FEE\_RESERVE\_DOLLARS \le 1 - KALSHI\_MIN\_PROFIT\_DOLLARS
$$

### Mode `a_targeted`

This mode is disabled unless explicitly requested by the operator.

Purpose:

- take directional underdog-side entries,
- treat favorite outcome as the protected or lower-priority case,
- accept directional outcome risk by design.

### Mode `b_targeted`

This mode is disabled unless explicitly requested by the operator.

Purpose:

- take directional favorite-side entries,
- treat underdog outcome as the protected or lower-priority case,
- accept directional outcome risk by design.

### Mode-selection rule

The application must start in `ab_guarded` unless the operator explicitly passes a different mode on the CLI.

If a targeted mode is selected, the runtime must print a mode warning that the trade is no longer guaranteed across outcomes.

## Explicit Scope for Version 1

Version 1 will trade only:

- binary event markets,
- a single Kalshi account,
- one subaccount at a time,
- paired YES+NO long entries in the same market,
- passive bid orders only,
- markets whose close time falls inside a configured late-entry window.

Version 1 will not implement:

- market orders,
- short-selling,
- RFQ workflows,
- multi-market portfolio optimization,
- cross-event arbitrage,
- multivariate event collections,
- autonomous production deployment before demo acceptance.

## Compliance Constraints

The implementation must be explicitly scoped to comply with the Kalshi Developer Agreement as currently understood.

### Allowed scope

The application is for one Kalshi member only and may be used only to facilitate that member's own trading.

### Explicit design constraints

Version 1 must not:

- facilitate trading for other members,
- support multiple Kalshi member accounts,
- sublicense or expose Kalshi API access to third parties,
- redistribute or warehouse API data beyond what is required to facilitate the operator's own trading,
- benchmark Kalshi services,
- use any manipulative order behavior including spoofing or wash-like patterns.

### Data-retention rule

Stored API-derived data must be limited to what is needed for:

- current strategy execution,
- reconciliation,
- audit evidence,
- debugging of the operator's own trading workflow.

The application must not maintain a general-purpose historical data product sourced from Kalshi API data.

## Environment Contract

The application will require the following environment variables:

| Variable                        | Required | Meaning                                                                                                              |
| ------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------- |
| `KALSHI_ENV`                    | yes      | `demo` or `prod`                                                                                                     |
| `KALSHI_API_KEY_ID`             | yes      | Kalshi API key identifier                                                                                            |
| `KALSHI_PRIVATE_KEY_FILE`       | yes      | Absolute or workspace-relative path to local PEM private-key file                                                    |
| `KALSHI_PRIVATE_KEY_INLINE`     | no       | Demo-only emergency inline key override; not allowed in steady-state or production                                   |
| `KALSHI_API_BASE_URL`           | yes      | `https://demo-api.kalshi.co/trade-api/v2` for demo or `https://api.elections.kalshi.com/trade-api/v2` for production |
| `KALSHI_WEBSOCKET_URL`          | yes      | Demo or production websocket endpoint                                                                                |
| `KALSHI_SUBACCOUNT`             | no       | Integer subaccount, default `0`                                                                                      |
| `KALSHI_SCAN_INTERVAL_MS`       | yes      | REST scan loop period                                                                                                |
| `KALSHI_ENTRY_WINDOW_START_SEC` | yes      | Maximum seconds before close to allow new paired entries                                                             |
| `KALSHI_ENTRY_WINDOW_END_SEC`   | yes      | Minimum seconds before close to allow new paired entries                                                             |
| `KALSHI_MIN_EDGE_DOLLARS`       | yes      | Minimum guaranteed gross edge per paired contract before fees                                                        |
| `KALSHI_FEE_RESERVE_DOLLARS`    | yes      | Conservative fee reserve per paired contract                                                                         |
| `KALSHI_MIN_PROFIT_DOLLARS`     | yes      | Minimum net profit floor per paired contract after fee reserve                                                       |
| `KALSHI_MAX_PAIR_CONTRACTS`     | yes      | Maximum contracts per paired candidate                                                                               |
| `KALSHI_MAX_OPEN_PAIRS`         | yes      | Maximum simultaneously active pair objects                                                                           |
| `KALSHI_MAX_UNHEDGED_SEC`       | yes      | Maximum allowed time for one-leg exposure before protective action                                                   |
| `KALSHI_CANCEL_ON_PAUSE`        | yes      | `true` or `false`; Version 1 default is `true`                                                                       |
| `KALSHI_LOG_LEVEL`              | yes      | Logging verbosity                                                                                                    |
| `KALSHI_STATE_DB_PATH`          | yes      | SQLite file path for persistent state                                                                                |

### Version 1 default values

The initial defaults will be:

| Variable                        |                                    Default |
| ------------------------------- | -----------------------------------------: |
| `KALSHI_ENV`                    |                                     `demo` |
| `KALSHI_API_BASE_URL`           |  `https://demo-api.kalshi.co/trade-api/v2` |
| `KALSHI_WEBSOCKET_URL`          | `wss://demo-api.kalshi.co/trade-api/ws/v2` |
| `KALSHI_SUBACCOUNT`             |                                        `0` |
| `KALSHI_SCAN_INTERVAL_MS`       |                                     `2000` |
| `KALSHI_ENTRY_WINDOW_START_SEC` |                                      `900` |
| `KALSHI_ENTRY_WINDOW_END_SEC`   |                                       `60` |
| `KALSHI_MIN_EDGE_DOLLARS`       |                                     `0.03` |
| `KALSHI_FEE_RESERVE_DOLLARS`    |                                     `0.02` |
| `KALSHI_MIN_PROFIT_DOLLARS`     |                                     `0.01` |
| `KALSHI_MAX_PAIR_CONTRACTS`     |                                    `10.00` |
| `KALSHI_MAX_OPEN_PAIRS`         |                                       `20` |
| `KALSHI_MAX_UNHEDGED_SEC`       |                                        `5` |
| `KALSHI_CANCEL_ON_PAUSE`        |                                     `true` |
| `KALSHI_LOG_LEVEL`              |                                     `INFO` |
| `KALSHI_STATE_DB_PATH`          |                 `var/kalshi_pairs.sqlite3` |

## Secure Credential Schema

Version 1 must treat inline private-key material inside `.env` as a temporary development shortcut, not the steady-state design.

### Required rule

The application must separate:

- non-secret runtime settings in `.env`, and
- secret key material in a local Git-ignored file or OS secret store.

The private key must never be committed, logged, or written to SQLite.

This plan must also follow Polymath-level security expectations:

- names-only documentation,
- fail-closed trust behavior,
- environment-based secret injection,
- protected local secret-store integrity where available,
- security messaging that explains the issue without exposing secret material.

### Approved layout

#### `.env`

`.env` may contain:

- `KALSHI_ENV`
- `KALSHI_API_KEY_ID`
- `KALSHI_API_BASE_URL`
- `KALSHI_WEBSOCKET_URL`
- `KALSHI_SUBACCOUNT`
- all timing, sizing, risk, and logging settings
- `KALSHI_PRIVATE_KEY_FILE`

`.env` must not contain raw PEM key text in the intended secure design.

#### Local secret file

Private key files must live in Git-ignored local paths such as:

- `secrets/kalshi/demo/private_key.pem`
- `secrets/kalshi/prod/private_key.pem`

Demo and production keys must be separate.

#### Optional future secret-store support

Version 2 may support:

- Windows Credential Manager
- Windows DPAPI-encrypted local storage
- external vault / secret-manager retrieval

Version 1 default is file-based secret storage.

### Revised credential variables

| Variable                    | Required | Meaning                                                          |
| --------------------------- | -------- | ---------------------------------------------------------------- |
| `KALSHI_API_KEY_ID`         | yes      | Kalshi API key identifier                                        |
| `KALSHI_PRIVATE_KEY_FILE`   | yes      | Path to local PEM file containing the RSA private key            |
| `KALSHI_PRIVATE_KEY_INLINE` | no       | Emergency demo-only override; disabled by default in normal runs |

### Runtime loading order

1. if `KALSHI_PRIVATE_KEY_FILE` is set, load the PEM from that file,
2. else if `KALSHI_PRIVATE_KEY_INLINE` is set and `KALSHI_ENV=demo`, allow it only behind an explicit development flag,
3. else fail startup.

Production mode must reject inline private-key material.

### Legacy variable compatibility rule

`KALSHI_PRIVATE_KEY_PATH` is deprecated in the secure design.

Version 1 implementation must handle it as follows:

1. if `KALSHI_PRIVATE_KEY_FILE` is set, use it,
2. else if `KALSHI_PRIVATE_KEY_PATH` points to an existing PEM file, accept it as a temporary compatibility alias and emit a warning,
3. else if `KALSHI_PRIVATE_KEY_PATH` contains inline PEM text, reject startup unless demo mode and explicit development override are both enabled,
4. production mode must reject `KALSHI_PRIVATE_KEY_PATH` inline PEM input completely.

### Required guardrails

The implementation must:

1. fail startup if the key file path does not exist,
2. redact all credential-bearing settings in logs,
3. never print API key ID and private key material together,
4. keep `.env`, `secrets/`, and exported credential files out of Git,
5. store only file paths, never private-key contents, in config snapshots,
6. support sandbox and production keys as separate files,
7. remain single-user and single-member in Version 1.

### Immediate migration plan

If raw private-key material is ever pasted into `.env`, the secure schema requires this follow-up:

1. create a fresh sandbox key pair,
2. store the private key in a Git-ignored PEM file,
3. replace inline key text in `.env` with `KALSHI_PRIVATE_KEY_FILE=<path>`,
4. remove the inline key material from `.env`.

### Key-generation artifact handling

Any text artifact emitted by Kalshi key generation must be treated as secret-bearing transitional material.

Required handling rules:

1. it must not be treated as a durable planning artifact,
2. it must not be referenced in documentation by value,
3. it must not be committed, published, or copied into reports,
4. if retained briefly for operator handling, it must live only in a local Git-ignored path,
5. after the PEM has been moved into the approved secret location, the transient key-generation output must be removed from the working flow.

### Screenshot and support-artifact handling

Screenshots and local support materials may be used as style references and operator evidence, but they are local-only by default.

Required rules:

- screenshots must not be treated as publishable artifacts unless explicitly exported,
- screenshots must not contain visible secrets, tokens, or raw key material,
- user-facing reports should reference evidence by names-only path tail or artifact label rather than by secret-bearing absolute path.

## Polymath Security Alignment

Version 1 must align with the following Polymath-level security expectations drawn from local repository guidance.

### Security invariants

1. no secrets in source control,
2. environment is the keyring for runtime secret injection,
3. names-only documentation and evidence,
4. fail closed on missing, invalid, expired, or unverifiable trust material,
5. explicit operator authorization for sensitive state changes,
6. retained evidence must be verifiable,
7. path containment must prevent writes outside declared roots.

### Concrete implementation requirements

The implementation must:

- validate required credential variable names without printing values,
- redact secret-bearing literals from logs and user-facing output,
- reject startup when trust material is missing or ambiguous,
- keep local secret overlays and generated state out of publishable artifacts,
- distinguish local evidence from exportable evidence,
- avoid exposing unnecessary absolute local paths in operator-facing text,
- never let noninteractive or JSON modes perform surprise installs, secret mutation, or trust-store changes.

### Authorization rule

Sensitive changes remain operator-gated. Version 1 must not silently:

- rotate credentials,
- rewrite secret files,
- rebaseline any protected secret store,
- promote demo configuration to production,
- publish evidence bundles containing local state.

## Pre-Execution Readiness Gate

No sandbox or production execution may begin until all of the following are true:

1. credentials are stored using the secure credential schema,
2. `.env` contains only a file path, not inline PEM key material,
3. any previously exposed inline sandbox key has been rotated,
4. the runtime can successfully complete a signed authenticated balance request,
5. the runtime can successfully fetch open markets,
6. the runtime can evaluate A+B candidates in dry-run mode without submitting orders,
7. all credential-bearing log output is redacted,
8. the operator explicitly selects `dry-run` before any order-capable mode is enabled.

If any one of these conditions fails, execution status is `NO-GO`.

### Minimum first executable milestone

Before any order placement code is enabled, the application must support this exact dry-run chain:

1. load config,
2. load private key from file,
3. sign one authenticated request,
4. call balance endpoint,
5. call open-markets endpoint,
6. compute A+B candidate edges,
7. print candidate results without placing or amending orders.

Order submission is blocked until that milestone passes.

## Repository Layout Contract

The Python application will use this layout:

```text
kalshi_paired_trader/
  pyproject.toml
  README.md
  .env
  src/
   kalshi_paired_trader/
    __init__.py
    config.py
    types.py
    auth.py
    http_client.py
    websocket_client.py
    market_data.py
    strategy.py
    execution.py
    risk.py
    persistence.py
    service.py
    cli.py
  tests/
   test_config.py
   test_auth.py
   test_strategy.py
   test_execution.py
   test_risk.py
   test_persistence.py
   test_demo_integration.py
```

## Module Contracts

### `config.py`

Responsibility: load, validate, and expose typed runtime configuration.

Required public contract:

- `class Settings(BaseModel | dataclass)`
- `def load_settings() -> Settings`

`Settings` fields must exactly match the environment contract listed above.

If any required variable is missing, `load_settings()` must raise a startup error that names every missing field.

Configuration-display rule:

When configuration is rendered for operators, it must use names-only output and must never print raw secret values, inline PEM material, or unnecessary full local paths.

### `types.py`

Responsibility: define all immutable domain objects.

Required domain models:

- `MarketSnapshot`
- `OrderbookSnapshot`
- `CandidatePair`
- `PairOrderPlan`
- `SubmittedOrder`
- `PairState`
- `FillEvent`
- `PairPosition`
- `PairPnl`

Minimum fields:

#### `MarketSnapshot`

- `ticker: str`
- `event_ticker: str`
- `title: str | None`
- `yes_sub_title: str`
- `no_sub_title: str`
- `open_time: datetime`
- `close_time: datetime`
- `latest_expiration_time: datetime`
- `status: str`
- `yes_bid_dollars: Decimal`
- `yes_ask_dollars: Decimal`
- `no_bid_dollars: Decimal`
- `no_ask_dollars: Decimal`
- `yes_bid_size_fp: Decimal`
- `yes_ask_size_fp: Decimal`
- `volume_fp: Decimal`
- `volume_24h_fp: Decimal`
- `open_interest_fp: Decimal`
- `can_close_early: bool`
- `rules_primary: str`
- `rules_secondary: str`
- `price_ranges: list[PriceRange]`

#### `OrderbookSnapshot`

- `ticker: str`
- `yes_bids: list[tuple[Decimal, Decimal]]`
- `no_bids: list[tuple[Decimal, Decimal]]`
- `best_yes_bid: Decimal | None`
- `best_no_bid: Decimal | None`
- `best_yes_ask_implied: Decimal | None`
- `best_no_ask_implied: Decimal | None`
- `captured_at: datetime`

#### `CandidatePair`

- `ticker: str`
- `seconds_to_close: int`
- `target_yes_bid: Decimal`
- `target_no_bid: Decimal`
- `edge_gross_per_contract: Decimal`
- `fee_reserve_per_contract: Decimal`
- `edge_net_per_contract: Decimal`
- `asymmetry: Decimal`
- `max_size_contracts: Decimal`
- `ranking_key: tuple`

#### `PairOrderPlan`

- `pair_id: str`
- `ticker: str`
- `yes_price: Decimal`
- `no_price: Decimal`
- `contract_count: Decimal`
- `yes_client_order_id: str`
- `no_client_order_id: str`
- `time_in_force: str`
- `post_only: bool`
- `cancel_order_on_pause: bool`
- `subaccount: int`

#### `PairState`

Allowed states:

- `DISCOVERED`
- `PLANNED`
- `SUBMITTING`
- `RESTING_BOTH`
- `PARTIAL_ONE_SIDE`
- `PARTIAL_BOTH`
- `LOCKED`
- `CANCELING`
- `CANCELED`
- `ERROR`

### `auth.py`

Responsibility: load the RSA private key and sign authenticated requests.

Required public contract:

- `def load_private_key(path: str) -> RSAPrivateKey`
- `def create_signature(private_key: RSAPrivateKey, timestamp_ms: str, method: str, path: str) -> str`
- `def build_auth_headers(private_key: RSAPrivateKey, api_key_id: str, method: str, path: str) -> dict[str, str]`

Signing algorithm is fixed and must exactly follow Kalshi documentation:

1. compute `timestamp_ms` in integer milliseconds,
2. strip query string from path,
3. compute message bytes from `timestamp + method + path_without_query`,
4. sign with RSA-PSS using SHA256 and digest-length salt,
5. base64 encode the signature,
6. emit headers:
  - `KALSHI-ACCESS-KEY`
  - `KALSHI-ACCESS-SIGNATURE`
  - `KALSHI-ACCESS-TIMESTAMP`

### `http_client.py`

Responsibility: send authenticated and unauthenticated REST calls.

Required public contract:

- `def get_markets(status: str, limit: int, cursor: str | None = None) -> GetMarketsResponse`
- `def get_market(ticker: str) -> MarketSnapshot`
- `def get_orderbook(ticker: str, depth: int = 0) -> OrderbookSnapshot`
- `def create_order_v2(...) -> SubmittedOrder`
- `def cancel_order_v2(order_id: str) -> None`
- `def get_order(order_id: str) -> SubmittedOrder`
- `def get_balance() -> Decimal`
- `def get_positions() -> list[PairPosition]`
- `def get_account_api_limits() -> AccountLimits`
- `def create_order_group(contracts_limit_fp: Decimal, subaccount: int = 0) -> str`

HTTP behavior rules:

- retry network timeouts up to 3 times with exponential backoff,
- never retry `400`, `401`, or `409` automatically,
- retry `429` with exponential backoff and jitter,
- log every request method, path, latency, and status code,
- redact auth headers in logs.

Additional rules:

- the startup path must call `GET /account/limits` once and persist the active read/write bucket settings,
- the runtime must derive its internal rate-limit budget from the live account-limits response rather than only static assumptions,
- `GET /portfolio/balance` values must be treated as cents and converted safely to Decimal dollars only at the edge of the domain layer.

Security messaging rule:

HTTP auth and trust failures must return reason-coded, secret-safe errors that explain what check failed and what safe next action the operator should take.

### `websocket_client.py`

Responsibility: stream order, fill, and market updates.

Required subscribed channels in Version 1:

- `orderbook_delta`
- `ticker`
- `fill`
- `user_orders`

Required public contract:

- `def connect() -> None`
- `def subscribe(channels: list[str], market_tickers: list[str]) -> list[int]`
- `def unsubscribe(subscription_ids: list[int]) -> None`
- `def next_event(timeout_sec: float) -> dict`

Required message handling:

- `orderbook_snapshot`
- `orderbook_delta`
- `user_order`
- `fill`
- `error`
- `subscribed`
- `ok`
- `unsubscribed`

WebSocket rules:

- authenticate during the handshake with the Kalshi auth headers,
- sign the websocket handshake path exactly as `timestamp + "GET" + "/trade-api/ws/v2"`,
- track `sid` per subscription,
- track message `seq` where present,
- reconnect automatically after disconnect,
- resubscribe to the prior channel set after reconnect,
- if reconnect occurs during active pair exposure, elevate risk status to `WARN` and force an immediate REST reconciliation.

Orderbook consistency rules:

1. on subscription, require and persist the initial `orderbook_snapshot`,
2. apply `orderbook_delta` messages only after a snapshot exists,
3. check `seq` monotonicity for every orderbook message,
4. if a sequence gap is detected, request a fresh snapshot using `get_snapshot` or rebuild the subscription,
5. use websocket orderbook state as the live source of price movement during active pairs.

Order and fill tracking rules:

- `user_order` messages are the authoritative real-time source for order lifecycle state,
- `fill` messages are the authoritative real-time source for fill-level execution and fees,
- if websocket and REST disagree, persist both observations and mark the pair for reconciliation review.

### `market_data.py`

Responsibility: translate REST and websocket responses into normalized snapshots.

Required public contract:

- `def fetch_open_markets() -> list[MarketSnapshot]`
- `def enrich_with_orderbook(ticker: str) -> tuple[MarketSnapshot, OrderbookSnapshot]`
- `def compute_seconds_to_close(market: MarketSnapshot, now: datetime) -> int`
- `def derive_implied_asks(orderbook: OrderbookSnapshot) -> tuple[Decimal | None, Decimal | None]`
- `def normalize_orderbook_snapshot(ws_or_rest_payload: dict) -> OrderbookSnapshot`

Normalization rule:

Both REST `orderbook_fp` and websocket `orderbook_snapshot` / `orderbook_delta` payloads must be normalized into one common in-memory representation before strategy logic runs.

### `strategy.py`

Responsibility: implement the deterministic scanning and ranking algorithm.

Required public contract:

- `def find_candidates(markets: list[MarketSnapshot], now: datetime, settings: Settings) -> list[CandidatePair]`
- `def build_pair_order_plan(candidate: CandidatePair, settings: Settings) -> PairOrderPlan`

#### Candidate filter algorithm

For each market:

1. reject unless market status is `active`,
2. reject unless current time is between `open_time` and `close_time`,
3. compute `seconds_to_close = close_time - now`,
4. reject unless
  $$
  KALSHI\_ENTRY\_WINDOW\_END\_SEC \le seconds\_to\_close \le KALSHI\_ENTRY\_WINDOW\_START\_SEC
  $$
5. reject if best YES bid or best NO bid is missing,
6. set target prices:
  $$
  p_Y = b_Y
  $$
  $$
  p_N = b_N
  $$
7. compute gross edge per contract:
  $$
  e_g = 1 - p_Y - p_N
  $$
8. set configured fee reserve:
  $$
  e_f = KALSHI\_FEE\_RESERVE\_DOLLARS
  $$
9. compute net edge floor:
  $$
  e_n = e_g - e_f
  $$
10. reject unless:
  $$
  e_g \ge KALSHI\_MIN\_EDGE\_DOLLARS
  $$
  and
  $$
  e_n \ge KALSHI\_MIN\_PROFIT\_DOLLARS
  $$
11. compute asymmetry:
  $$
  a = |p_Y - p_N|
  $$
12. compute size cap:
  $$
  q_{max} = KALSHI\_MAX\_PAIR\_CONTRACTS
  $$
13. accept as candidate.

#### Candidate ranking algorithm

Sort accepted candidates by this exact descending tuple:

1. `edge_net_per_contract`
2. `asymmetry`
3. `volume_24h_fp`
4. `open_interest_fp`
5. negative `seconds_to_close`

This ranking means:

- stronger guaranteed edge wins first,
- larger price asymmetry wins second,
- more actively traded markets win third,
- more open interest wins fourth,
- closer-to-close markets win fifth.

### `execution.py`

Responsibility: submit paired orders, reconcile fills, and enforce pair symmetry.

Required public contract:

- `def submit_pair(plan: PairOrderPlan) -> PairState`
- `def reconcile_pair(pair_id: str) -> PairState`
- `def cancel_pair(pair_id: str) -> PairState`
- `def hedge_unmatched(pair_id: str) -> PairState`
- `def ensure_order_group(pair_id: str, contract_limit_fp: Decimal) -> str`

#### Submission algorithm

1. create `pair_id = uuid4()`.
2. create one client order ID for YES and one for NO.
3. create an order group with a contracts limit over a rolling 15-second window sized to the planned pair quantity.
3. submit YES buy order first using V2 order API with:
  - `ticker = market ticker`
  - `side = bid`
  - `count = contract_count`
  - `price = yes_price`
  - `time_in_force = good_till_canceled`
  - `post_only = true`
  - `self_trade_prevention_type = taker_at_cross`
  - `cancel_order_on_pause = true`
  - `subaccount = configured subaccount`
  - `order_group_id = created order group`
4. submit NO buy order second using the same shape but `price = no_price` and the opposite book representation as required by the chosen API mapping layer.
5. persist both orders in SQLite in the same transaction.
6. move pair state to `RESTING_BOTH` only if both order submissions succeed.
7. if either submission fails, cancel any successfully placed sibling order immediately and mark the pair `ERROR`.

Order-group rule:

The order group is a safety brake, not the primary position manager. Its purpose is to prevent unexpected matched-contract bursts over the rolling 15-second window.

#### Fill reconciliation algorithm

Let:

- $q_Y$ = filled YES contracts
- $q_N$ = filled NO contracts
- $q_L = \min(q_Y, q_N)$ = locked contracts
- $q_U = |q_Y - q_N|$ = unmatched contracts

The pair is economically guaranteed only on $q_L$ contracts.

The system must therefore compute guaranteed P&L only on the locked quantity:

$$
\Pi_{locked} = q_L(1 - \bar{p}_Y - \bar{p}_N) - F_L
$$

where:

- $\bar{p}_Y$ = weighted average YES fill price on locked quantity,
- $\bar{p}_N$ = weighted average NO fill price on locked quantity,
- $F_L$ = realized fees attributable to the locked quantity.

State transitions:

- if $q_Y = 0$ and $q_N = 0$: remain `RESTING_BOTH`
- if $q_Y = q_N > 0$: state `LOCKED`
- if $q_Y \ne q_N$: state `PARTIAL_ONE_SIDE` or `PARTIAL_BOTH`

#### Unmatched exposure algorithm (shelter window)

`KALSHI_MAX_UNHEDGED_SEC` is a **shelter window** measured as seconds before market close.
It is **not** an order-age timeout: the reconcile sweep must never force a one-sided pair
to `ERROR` because a fill has rested past some elapsed duration.

When a `PARTIAL_ONE_SIDE` / `PARTIAL_BOTH` pair reaches the shelter window
(seconds-to-close $\le$ `KALSHI_MAX_UNHEDGED_SEC`):

1. cancel only the remaining resting quantity on the **ahead (over-filled) leg**, capping
   further exposure on the side that is already ahead,
2. **preserve the opposite (deficient) repair order** — leave it resting and open to fill;
   do not cancel it and do not cross the market to catch it up,
3. compute unmatched exposure size $q_U$ and guaranteed P&L only on the matched (locked)
   quantity $q_L$,
4. project `REPAIR_LIVE` while the preserved repair order is still live, or
   `EXPOSURE_CAPPED` once it is no longer live (closed / canceled / filled),
5. leave settlement reconciliation to terminalize the pair when the market finalizes
   (`SETTLED_EXPOSURE` for a one-sided settled fill), carrying realized P&L.

Version 1 caps the ahead side and leaves the repair order working; it does not cross the
market to chase the deficient leg, and it does not freeze the pair to `ERROR` on elapsed
order age. Intelligent recovery of the deficient leg (e.g. repricing the resting order via
the Kalshi amend endpoint to a current fillable price) is a planned enhancement, not part
of this version.

Cancel semantics rule:

Cancellation logic must consume the V2 cancel response `{order_id, client_order_id, reduced_by}` and persist `reduced_by` as the authoritative canceled remaining quantity for that action.

### `risk.py`

Responsibility: reject unsafe actions before any order is sent.

Required public contract:

- `def can_open_new_pair(current_pairs: list[PairState], balance: Decimal, settings: Settings) -> bool`
- `def validate_pair_plan(plan: PairOrderPlan, candidate: CandidatePair, settings: Settings) -> None`
- `def validate_post_fill(pair: PairState, settings: Settings) -> None`

Required pre-trade checks:

1. current open pair count must be less than `KALSHI_MAX_OPEN_PAIRS`,
2. free cash must exceed maximum planned paired spend,
3. market status must still be `active`,
4. `seconds_to_close` must still be inside the allowed entry window,
5. gross edge and net edge must still exceed configured thresholds,
6. Thursday maintenance window 3:00-5:00 AM ET must hard-block new entries,
7. any exchange pause or trading pause must hard-block new entries,
8. if websocket is disconnected and there is open unmatched exposure, hard-block new entries.

Additional checks:

9. live read/write API limits must have been fetched successfully at startup,
10. targeted modes must require explicit CLI confirmation,
11. sandbox and production credentials must not be mixed,
12. operator mode must default to dry-run unless `--allow-orders` is explicitly present.

### `persistence.py`

Responsibility: persist orders, fills, pair state, and P&L.

Required storage engine: SQLite.

Required tables:

- `markets_seen`
- `pair_plans`
- `orders`
- `fills`
- `pair_states`
- `pair_pnl_snapshots`
- `service_heartbeats`
- `account_api_limits`
- `operator_actions`
- `runtime_events`

Minimum persistence rule:

- every state transition must be written before the next side effect occurs.

Retention rule:

Persistence must be limited to execution, reconciliation, and operator-audit needs for the member's own trading workflow.

Evidence rule:

Persisted operator evidence must be names-only where possible and must never store private-key bytes, raw `.env` lines, or secret-bearing support artifacts.

### `service.py`

Responsibility: orchestrate the end-to-end runtime loop.

Required runtime loop:

1. load settings,
2. initialize logging,
3. load private key,
4. fetch account API limits,
5. open SQLite,
6. connect websocket,
7. start reconciliation worker,
8. every `KALSHI_SCAN_INTERVAL_MS`:
  - fetch open markets,
  - filter by entry window,
  - fetch orderbook for candidates,
  - rank candidates,
  - validate risk,
  - if mode is `ab_guarded`, compute guaranteed-pair candidates,
  - if mode is targeted, compute only the explicitly requested directional candidates,
  - in dry-run mode, print and persist decisions without placing orders,
  - in order-enabled mode, submit new pair plans until `KALSHI_MAX_OPEN_PAIRS` is reached,
  - reconcile all active pairs,
  - persist heartbeat.

Startup hard-fail rules:

The service must fail startup if:

- credentials violate the secure schema,
- account API limits cannot be loaded,
- websocket authentication fails,
- SQLite cannot be opened,
- the configured mode is targeted without explicit operator request.

### `cli.py`

Responsibility: provide operator commands.

Required commands:

- `kalshi-pairs run`
- `kalshi-pairs scan-once`
- `kalshi-pairs reconcile`
- `kalshi-pairs cancel-all`
- `kalshi-pairs report`

Required flags:

- `--mode ab_guarded|a_targeted|b_targeted`
- `--dry-run`
- `--allow-orders`
- `--env demo|prod`
- `--subaccount N`

CLI safety rule:

`--dry-run` is the default. `--allow-orders` must be required before any live order submission path is enabled.

## Polymath Presentation Layer

Version 1 must adopt a Polymath-style operator presentation contract for CLI help, terminal output, blocked states, dry-run results, and retained reports.

### Core output contract

Every operator-facing surface must answer:

1. what ran,
2. what happened,
3. why it happened,
4. what should happen next,
5. where the evidence is, when evidence exists.

### Human output structure

Human terminal output should use this default order:

1. title or command family,
2. decision/status line,
3. summary section,
4. details section when useful,
5. evidence/artifacts section when applicable,
6. next action section when the operator can act.

### Decision vocabulary

Decision lines must use explicit vocabulary such as:

- `go`
- `no-go`
- `prompt`
- `pass`
- `fail`
- `planned`
- `scaffold`

Dry-run, simulated, partial, or planned states must never be presented as completed success.

### Help-page contract

CLI help output must include:

- usage,
- short summary,
- command groups or subcommands,
- options,
- copyable examples for common workflows,
- clear explanation of dry-run and order-capable modes,
- JSON-mode mention when supported.

### No-go output contract

Blocked or no-go output must include:

- the command or action that stopped,
- reason code or reason family,
- plain-language explanation,
- blockers or blocker reasons when available,
- whether the operator can fix it,
- exact next action or next command when known.

### Evidence-display contract

Operator-visible evidence summaries must include:

- stable run/session/report identifier,
- generated timestamp,
- status,
- evidence location or path tail,
- next review command when useful.

### Path-display contract

User-facing output must prefer project-relative paths or meaningful path tails.

Full absolute paths should be printed only when the operator truly needs them.

### JSON / human parity rule

If a JSON output mode exists, human output may summarize it, but must not hide operator-relevant facts that appear in the structured packet.

### Local screenshot-derived pattern reference

The presentation layer should mirror the demonstrated local style patterns already visible in repository support artifacts:

- grouped help output with short descriptive rows,
- early explicit decision lines,
- summary-plus-details layout,
- blocked output that explains fallback state and next understanding step,
- calm, low-noise operator wording.

## Exact Paired-Edge Algorithms

### Algorithm A: Candidate discovery

Input:

- open markets REST response,
- orderbook response per market,
- current time,
- settings.

Output:

- sorted list of `CandidatePair` objects.

Algorithm:

1. call `GET /markets?status=open&limit=1000` and paginate until exhausted,
2. for each market, parse `close_time`, `status`, `volume_24h_fp`, `open_interest_fp`, `yes_bid_dollars`, `no_bid_dollars`,
3. retain only markets inside the time window,
4. call `GET /markets/{ticker}/orderbook`,
5. extract `b_Y` and `b_N` from the last price level in `yes_dollars` and `no_dollars`,
6. compute candidate edge values,
7. reject non-qualifying markets,
8. sort by the ranking tuple,
9. return results.

### Algorithm B: Pair plan sizing

Input:

- one `CandidatePair`,
- available balance,
- configured limits.

Output:

- one `PairOrderPlan`.

Algorithm:

1. compute paired spend per contract:
  $$
  s = p_Y + p_N
  $$
2. compute maximum affordable contracts:
  $$
  q_{cash} = \left\lfloor \frac{available\_cash}{s + KALSHI\_FEE\_RESERVE\_DOLLARS} \right\rfloor
  $$
3. compute:
  $$
  q = \min(q_{cash}, KALSHI\_MAX\_PAIR\_CONTRACTS)
  $$
4. reject if $q < 1.00$ contracts,
5. emit a plan using `good_till_canceled`, `post_only=true`, `cancel_order_on_pause=true`.

### Algorithm C: Guaranteed P&L computation

For any locked pair quantity $q_L$:

$$
\Pi_{gross} = q_L(1 - \bar{p}_Y - \bar{p}_N)
$$

$$
\Pi_{net,projected} = q_L(1 - \bar{p}_Y - \bar{p}_N - KALSHI\_FEE\_RESERVE\_DOLLARS)
$$

$$
\Pi_{net,realized} = q_L(1 - \bar{p}_Y - \bar{p}_N) - F_{realized}
$$

The service must display all three values separately.

### Algorithm D: Late-stage entry policy

The system may submit a new pair only if:

$$
KALSHI\_ENTRY\_WINDOW\_END\_SEC \le seconds\_to\_close \le KALSHI\_ENTRY\_WINDOW\_START\_SEC
$$

Version 1 default window is:

- start scanning for entry at 15 minutes to close,
- stop creating new entries at 60 seconds to close.

The system may continue reconciling and canceling after the entry window closes, but it may not open a new pair.

## Testing Plan

### Calamum cross-validated orchestration

Version 1 testing must include cross-validated execution through the externally installed `calamum` orchestrator.

This is specifically valuable because this workspace is an adoption environment outside the Calamum development repository.

The plan must therefore treat Calamum as an external retained-evidence harness, not merely as an implementation detail inside its own source tree.

### Calamum command surface used by this plan

The relevant command families and capabilities are:

- `calamum test list`
- `calamum test show <definition_id>`
- `calamum test run <definition_id>`
- `calamum test runs list`
- `calamum test runs show <run_id>`
- `calamum test reports generate --scope job|project|domain`
- `calamum project register`
- `calamum project set`
- `calamum project current`
- `calamum project validate`

Calamum also exposes retained-run and aggregate-report flows with JSON-first output, Markdown companions, manifests, checksums, and optional signatures.

### Three Calamum test classes

Per Calamum's current model, each named test definition may use one, two, or all three of the following lane classes:

1. `pytest` — automated code-level assertions,
2. `sandbox_test` — controlled scripted or simulated workflow execution,
3. `empirical_test` — real observed or operator-reviewed validation.

For this Kalshi project, every implementation slice must define and execute all three lane classes unless a later explicit design decision narrows a slice for justified reasons.

### Per-slice Calamum testing rule

Each implementation slice must be represented as one Calamum definition with:

- one `pytest` lane,
- one `sandbox_test` lane,
- one `empirical_test` lane,
- retained evidence sufficient to generate run-level and project-level reports.

The intent is one slice = one real thing under test = one retained evidence pack spanning the three test classes.

### Required Calamum evidence contract

Every slice-level Calamum run must retain at minimum:

- `report.json`,
- `report.md`,
- `manifest.json`,
- `checksums.json`,
- per-step stdout capture,
- per-step stderr capture when applicable.

Project-level testing rounds must also generate aggregate reports from retained runs.

### Slice-level Calamum rounds

The Kalshi implementation must use the following Calamum testing rounds.

#### Round 1 — secure bootstrap and auth slice

Scope:

- config loading,
- key-path resolution,
- fail-closed secret posture,
- RSA signing,
- authenticated balance call.

Calamum lanes:

- `pytest`: config and auth unit assertions,
- `sandbox_test`: controlled CLI dry-run against local demo configuration,
- `empirical_test`: operator review of retained evidence proving names-only output and correct no-go/go behavior.

#### Round 2 — market discovery and candidate evaluation slice

Scope:

- open-market scan,
- candidate filtering,
- ranking,
- dry-run candidate output,
- JSON/human parity.

Calamum lanes:

- `pytest`: candidate math and ranking assertions,
- `sandbox_test`: controlled `scan-once` workflow against demo market data,
- `empirical_test`: operator review of candidate ordering and retained report clarity.

#### Round 3 — persistence and pair-state slice

Scope:

- SQLite state model,
- pair-plan persistence,
- fill persistence,
- state-transition integrity,
- retained artifact integrity.

Calamum lanes:

- `pytest`: state-transition and schema assertions,
- `sandbox_test`: controlled retained-run generation against local persistence flows,
- `empirical_test`: operator review of manifests, checksums, and report truthfulness.

#### Round 4 — websocket, reconciliation, and risk slice

Scope:

- websocket snapshot/delta handling,
- fill correlation,
- reconciliation,
- unmatched-exposure controls,
- risk-gate behavior.

Calamum lanes:

- `pytest`: parser, reconciliation, and risk assertions,
- `sandbox_test`: simulated message-flow scenarios and timeout handling,
- `empirical_test`: operator review of blocked states, recovery guidance, and retained evidence coherence.

#### Round 4A — completed-work contract-alignment sub-slice

Scope:

- close unmet expectations inside already-entered slices without changing the plan goals, milestone meaning, or current dry-run/demo boundary,
- bring the existing `service.py`, `risk.py`, `http_client.py`, `execution.py`, and `types.py` surfaces into full contract alignment for the work that has already been pulled into the implementation state,
- harden the current dry-run runtime so it satisfies the full expectations of the intended plan for the already-completed rounds,
- correct any implementation over-narrowing or truthfulness gaps in retained evidence and operator-facing summaries,
- preserve the explicit rule that order-enabled behavior remains blocked until the later documented gates are satisfied.

Governance rule:

- this sub-slice exists to close expectation debt inside already-entered work, not to change project goals or authorize later-stage behavior,
- structural changes, scope additions, scope demotions, and user-facing behavior changes still require explicit discussion and approval before they are treated as accepted execution,
- the allowed work in this sub-slice is contract completion, hardening, truthfulness correction, and test/evidence completion for the current stage.

Required completion targets:

1. `service.py` must satisfy the full dry-run orchestration expectations already implied by the completed slices, including candidate enrichment, risk validation, reconciliation cadence, heartbeat truthfulness, and current-scope persistence behavior.
2. `risk.py` must implement the full current-stage gate set that applies before order enablement, including the documented dry-run safety posture and blocked-entry conditions that do not depend on later live trading enablement.
3. `http_client.py` must satisfy the existing REST-slice expectations for retry policy, 429 handling, request logging, auth redaction, and secret-safe trust failure reporting.
4. `execution.py` must satisfy the current-stage pair lifecycle contract for plan, cancel, reconciliation, unmatched-exposure handling, and order-group semantics without prematurely enabling the later live order path.
5. `types.py` and related render/output surfaces must reflect the full required domain contract for the already-entered slices and keep human/json evidence truthful and consistent.
6. any surface that currently overstates completion must be corrected so the real system state and retained evidence agree.

Calamum lanes:

- `pytest`: focused contract assertions for the closed gaps in service, risk, HTTP, execution, types, and operator-surface parity,
- `sandbox_test`: controlled dry-run execution proving the hardened current-stage runtime behaves as documented without enabling orders,
- `empirical_test`: operator review proving the resulting system state is truthful, plan-aligned, and still inside the approved boundary.

Required retained-evidence outcomes:

- one named Calamum definition dedicated to the contract-alignment sub-slice,
- retained `report.json`, `report.md`, `manifest.json`, `checksums.json`, and per-step stdout/stderr captures,
- explicit evidence that the completed sub-slice produced a real system state that aligns with the full expectations of the intended plan for already-entered work.

#### Round 5 — order-capable sandbox slice

Scope:

- pair planning,
- sandbox order submission,
- cancel flow,
- partial-fill handling,
- sandbox aggregate reporting.

Calamum lanes:

- `pytest`: order-plan and response-shape assertions,
- `sandbox_test`: controlled demo/sandbox execution path,
- `empirical_test`: operator review of retained end-to-end sandbox evidence before any broader enablement.

### Unit tests

Must cover:

- RSA signing correctness,
- query-string stripping before signing,
- candidate filter logic,
- candidate ranking order,
- pair-size computation,
- guaranteed P&L computation,
- unmatched-exposure timeout logic,
- persistence of state transitions.

### Demo integration tests

Must cover:

1. authenticated balance request,
2. market scan against demo markets,
3. paired order submission with unique client order IDs,
4. order cancellation flow,
5. websocket subscribe/reconnect/resubscribe,
6. reconciliation after partial fill events.

### Required dry-run acceptance tests

1. signed websocket handshake succeeds in demo,
2. initial `orderbook_snapshot` is received and normalized,
3. subsequent `orderbook_delta` messages are applied in sequence,
4. `user_order` and `fill` messages are persisted and correlated by `order_id` and `client_order_id`,
5. account API limits are fetched and stored,
6. default mode runs as `ab_guarded`,
7. targeted modes refuse to run without explicit operator selection,
8. order-capable mode refuses to start without `--allow-orders`.

### Required presentation and security acceptance tests

1. help output includes usage, command groups, options, and examples,
2. dry-run output clearly states that no order was submitted,
3. no-go output includes reason, plain-language explanation, and next action,
4. operator-facing output exposes evidence references without leaking secrets,
5. JSON output remains machine-clean and free of human progress noise,
6. configuration validation errors name missing variables without printing values,
7. secret-bearing inputs are redacted in logs and user-facing diagnostics,
8. local-only evidence paths are shortened in human output unless full paths are required.

### Required Calamum acceptance tests

1. the Kalshi project can be registered and resolved through `calamum project` commands,
2. each implementation slice has one named Calamum definition,
3. each slice definition declares all three lane classes: `pytest`, `sandbox_test`, and `empirical_test`,
4. each slice run produces retained run artifacts under Calamum's retained-evidence contract,
5. each slice run can be reviewed through `calamum test runs show <run_id>`,
6. slice batches can be aggregated through `calamum test reports generate --scope project`,
7. aggregate reports truthfully reflect the retained runs and artifact manifests,
8. project-level retained evidence remains local-only unless explicitly exported or signed for a later privileged flow.

### Aggregate interpretation rule

Project-scoped Calamum aggregates are historical evidence ledgers, not a shortcut that erases earlier failed or superseded attempts.

Interpretation requirements:

1. each retained run must remain visible in aggregate history with its true result,
2. a failed attempt that is later corrected must remain in the ledger as historical evidence,
3. current implementation status must be judged against the latest accepted run for each completed slice,
4. acceptance-gate review must use the latest accepted slice set, not the existence of earlier failed attempts by itself,
5. a successful completed-work contract-alignment run closes current-stage expectation debt only and does not authorize later order-enabled behavior.

### Acceptance gates before any production enablement

All of the following must be true:

1. zero auth failures in the latest demo test run,
2. zero unhandled exceptions in the latest 7-day demo soak test,
3. zero orphan orders left unreconciled in demo,
4. projected and realized P&L calculations match persisted fills within $\$0.01$ on every closed demo pair,
5. pause handling verified during a simulated disconnect/pause scenario,
6. operator sign-off on demo evidence,
7. user-facing dry-run, no-go, and help outputs pass Polymath presentation review,
8. secret-safety review confirms names-only evidence and no leaked key material in retained artifacts,
9. every completed implementation slice has passed all three Calamum lane classes,
10. a project-scoped Calamum aggregate has been generated and reviewed for the latest accepted slice set.

## Implementation Sequence

The build order is fixed:

1. migrate credentials to file-based secret storage,
2. rotate any inline sandbox key previously pasted into `.env`,
3. create project skeleton,
4. implement config loading,
5. implement auth/signing,
6. implement REST client,
7. implement a dry-run authenticated balance check,
8. implement domain types,
9. implement market scanner,
10. implement candidate ranking,
11. implement dry-run A+B candidate evaluation,
12. implement paired order planner,
13. implement persistence,
14. implement reconciliation engine,
15. implement websocket client,
16. implement risk gates,
17. implement CLI,
18. register the Kalshi project in Calamum and validate project context resolution,
19. create one Calamum definition for the secure bootstrap/auth slice and run all three lane classes,
20. create one Calamum definition for the market discovery/candidate slice and run all three lane classes,
21. create one Calamum definition for the persistence/pair-state slice and run all three lane classes,
22. create one Calamum definition for the websocket/reconciliation/risk slice and run all three lane classes,
23. run demo integration tests,
23A. execute the completed-work contract-alignment sub-slice to close unmet expectations inside already-entered work before advancing to the order-capable sandbox slice,
24. create one Calamum definition for the order-capable sandbox slice and run all three lane classes,
25. generate a project-scoped Calamum aggregate from retained runs,
26. run demo soak test,
27. only then consider enabling order placement in sandbox,
28. only after sandbox acceptance gates pass may production configuration be considered.

## Observability and Operator Evidence

The application must produce structured logs for:

- startup configuration summary with secrets redacted,
- mode selection,
- account API limits,
- websocket connection lifecycle,
- subscription IDs and channels,
- candidate decisions,
- order submissions,
- cancel actions,
- fill events,
- pair state transitions,
- dry-run versus order-enabled execution path.

Required dashboard / report outputs:

- current mode,
- sandbox vs production environment,
- available balance,
- API tier limits,
- active pairs,
- locked quantity by pair,
- unmatched exposure by pair,
- projected A+B edge,
- realized fees,
- latest reconciliation status.

Required rendered-surface properties:

- calm operator tone,
- explicit decision near the top,
- aligned key-value presentation where facts are operational,
- truthfully labeled dry-run versus live execution,
- evidence references that do not expose secret-bearing paths or values.

## Operator Runbooks

### Runbook: first demo dry-run

1. verify `KALSHI_ENV=demo`,
2. verify key is loaded from `KALSHI_PRIVATE_KEY_FILE`,
3. start with `--mode ab_guarded --dry-run`,
4. confirm signed balance call works,
5. confirm open-markets scan works,
6. confirm candidate output prints without order submission,
7. review logs for redaction and websocket health.

### Runbook: enable sandbox orders

Preconditions:

- all readiness-gate items pass,
- dry-run tests pass,
- sandbox key was rotated after any inline exposure,
- operator explicitly approves order-capable sandbox execution.

Steps:

1. run in `ab_guarded` mode only,
2. enable `--allow-orders`,
3. cap `KALSHI_MAX_PAIR_CONTRACTS` conservatively,
4. monitor first submitted pair end-to-end,
5. verify order, fill, and reconciliation telemetry before further scale-up.

### Runbook: halt conditions

Immediately halt new entries if any of the following occur:

- repeated websocket auth failures,
- orderbook sequence gaps that are not repaired,
- repeated 429 errors beyond bounded retry policy,
- unmatched exposure persists beyond allowed policy,
- projected and realized P&L diverge materially,
- any indication of account restriction or compliance inquiry.

## Final Design Rule

The service must treat a paired YES+NO position as guaranteed only on the quantity that is filled on both sides at verified prices and only after accounting for configured fee reserve in planning and realized fees in reporting.

No unmatched quantity may ever be reported as guaranteed.
