# Polyventure

<p align="center">
	<img src="assets/images/polyventure_logo.png" alt="Polyventure logo" width="360">
</p>

> Dry-run-first Polymath operator shell for paired YES + NO hedge workflows on Kalshi binary event markets.

A [Polymath](https://polymath-global.com) open-source project.

Polyventure is a **dry-run-first operator shell** that implements a paired **YES + NO hedge** workflow on
Kalshi binary event markets. It scans open markets, ranks candidate pairs by a documented edge model,
plans hedged orders, and — when explicitly authorized — places and reconciles them, while keeping
planned, simulated, sandboxed, and live states strictly distinct in every operator-facing surface.

The full strategy, edge model, sizing rules, and acceptance gates are specified in
[`docs/WAGER_HEDGE_MODELS.md`](docs/WAGER_HEDGE_MODELS.md), which is the authoritative model surface.

> **Disclaimer.** This is research and educational software provided as-is under the Apache License 2.0. It is
> not financial advice and is not affiliated with or endorsed by Kalshi. The live lane places real orders
> with real capital and carries real financial risk. You are solely responsible for compliance with all
> applicable terms, laws, and regulations. See [`assets/legal/NOTICE.md`](assets/legal/NOTICE.md).

## What it does

The core idea is a two-sided hedge on a binary market: when the combined cost of a YES position and an
offsetting NO position is low enough relative to the guaranteed $1 settlement, the pair captures a small
structural edge if both legs fill. Polyventure implements the discovery, ranking, coverability checks,
sizing, order placement, and settlement reconciliation around that idea, with a strong **fail-closed**
posture — lane, credential, and endpoint selection must be explicit, and unresolved values rest in an
inert `offline` state rather than silently defaulting.

Current capabilities include: typed config loading, RSA-PSS request signing, authenticated balance
lookup, open-market scanning, deterministic candidate ranking, human and JSON dry-run output,
SQLite-backed runtime persistence, websocket normalization and reconciliation, pair planning with
partial-fill / one-sided-exposure handling, and a bounded dry-run soak harness.

## Architecture

The application lives under [`src/polyventure/`](src/polyventure/):

| Area | Modules |
|---|---|
| Entry / shell | `cli`, `__main__`, `web_app`, `tray`, `popup` |
| Market + strategy | `market_data`, `strategy`, `candidate_identity`, `flow_evidence`, `parameter_optimizers`, `kalshi_units` |
| Execution + risk | `execution`, `risk`, `service`, `soak` |
| Platform I/O | `http_client`, `websocket_client`, `auth`, `config`, `types` |
| Persistence + integrity | `persistence`, `signed_evidence`, `transition_contract_audit`, `sandbox_preflight` |

Additional technical reference for the Kalshi integration is under
[`docs/guides/kalshi-reference-guide/`](docs/guides/kalshi-reference-guide/).

## Install

Requires **Python 3.14** (`>=3.14,<3.15`).

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate   |   POSIX: source .venv/bin/activate
pip install -e .          # runtime
pip install -e '.[dev]'   # + test dependencies
```

## Configure

```bash
cp .env.example .env
```

Then set your Kalshi API key id(s) and provide an RSA private key file under `secrets/kalshi/`
(paths are configured in `.env`). The shell **boots offline** and performs no operative action until a
lane is explicitly selected; sandbox/dry-run is the safe default and the live lane must be opted into
deliberately. No credentials or keys are included in this repository.

## Run

```bash
python -m polyventure.cli --help     # explore commands
polyventure                          # console entry point (after install)
```

Batch/CLI entry points require the operation lane to be stated explicitly rather than inferring it.

## Test

The public test surface runs with standard `pytest`. The broader Polymath validation posture uses
[Calamum](https://github.com/joediggidyyy/calamum) as the retained-evidence test runner; this standalone
review package does not require Calamum to run the included tests.

<p align="center">
	<img src="assets/images/calamum_logo_color.png" alt="Calamum retained-evidence test runner" width="220">
</p>

```bash
pytest
```

## Reproducibility notes

This public package contains the application source, its test suite, and curated documentation. It
**intentionally excludes** live runtime databases, retained validation evidence, operator session state,
and internal development history — none of which are required to build, run (dry-run), or test the
software, and some of which would contain real trading data. The bounded local soak harness does not by
itself satisfy the long-duration demo gate described in the model doc.

## License

Apache-2.0 — see [`LICENSE`](LICENSE).

---

<p align="center">
	<img src="assets/images/polymath_global.png" alt="Polymath Global" width="260">
</p>
