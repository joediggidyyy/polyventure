# Changelog

**Document ID**: `POLYVENTURE_CHANGELOG`  
**Status**: Public change history  
**Maintainer**: Joe Waller  
**Project**: Polyventure / Polymath  
**Last updated**: 2026-07-09

---

<p align="center">
  <img src="assets/images/polyventure_logo.png" alt="Polyventure logo" width="200">
</p>

All notable, publicly relevant changes to this repository are recorded here. Dates are UTC. This project
uses semantic-style version boundaries; the public history begins at the first public release.

## 0.1.0 - 2026-07-09

Initial public release for scholarly review and reproducibility.

### Added

- the Polyventure application source: a dry-run-first Polymath operator shell implementing a paired YES +
  NO hedge workflow on Kalshi binary event markets (typed config, RSA-PSS request signing, market scanning,
  deterministic candidate ranking, SQLite-backed persistence, websocket normalization and reconciliation,
  pair planning with partial-fill / one-sided-exposure handling, and a bounded dry-run soak harness)
- the pytest test suite, which provisions an ephemeral signing key so the signed-evidence surface is
  reproducible without shipping any secret
- reproducibility documentation, including the hedge model reference and a Kalshi integration reference guide
- standard community-health files (contributing, security, code of conduct, support, citation)

### Notes

- No credentials, API keys, or signing keys are included. Real-money use requires operator-provided keys.
- Internal development history, live runtime databases, and retained operational evidence are intentionally
  excluded from this public package; they are not required to build, run (dry-run), or test the software.

---

<p align="center">
  <img src="assets/images/polymath_global.png" alt="Polymath Global" width="220">
</p>
