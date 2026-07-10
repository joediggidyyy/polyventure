# Security Policy

**Document ID**: `POLYVENTURE_SECURITY`  
**Status**: Public vulnerability reporting  
**Maintainer**: Joe Waller  
**Project**: Polyventure / Polymath  
**Last updated**: 2026-07-09

---

<p align="center">
  <img src="assets/images/polyventure_logo.png" alt="Polyventure logo" width="150">
</p>

## Scope

Polyventure is a Polymath dry-run-first operator shell that implements a paired YES + NO hedge workflow on
Kalshi binary event markets. It is research and educational software; the live lane places real orders with
real capital.

Security reports are especially valuable for:

- credential, key, or lane resolution that could route real money against operator intent
- request signing, authentication, or trust-store / signed-evidence verification bypasses
- fail-closed guards that can be made to fail open (lane, coverability, or risk gates)
- persistence, reconciliation, or settlement logic that could misreport exposure or P&L
- local-path, secret, or personal-data leakage in output, logs, or exported evidence

If you are unsure whether something is security-relevant, report it privately first.

## Supported surfaces

| Surface                              | Status                                                                   |
| ------------------------------------ | ------------------------------------------------------------------------ |
| `main`                               | supported for coordinated disclosure                                     |
| latest tagged release                | supported                                                                |
| older tags or ad hoc local builds    | best effort; you may be asked to retest on the latest supported boundary |

## How to report a vulnerability

Please **do not open a public GitHub issue** for a sensitive vulnerability.

Preferred reporting path:

1. use GitHub private vulnerability reporting / a private security advisory for this repository if it is
   available in your view
2. if private reporting is not available, contact the maintainer privately through their GitHub profile
   rather than posting details publicly

Please include:

- affected version, tag, or commit SHA
- host OS and Python version
- concise reproduction steps
- expected behavior versus actual behavior
- impact assessment
- a minimal proof of concept or log excerpt, with sensitive values redacted

Please do **not** include real API keys, private signing keys, account identifiers, or personal data
unless the maintainer explicitly asks for them through a private channel.

## Coordinated disclosure expectations

Polyventure aims to:

- acknowledge a private report within 7 calendar days
- provide an initial triage or status update within 14 calendar days
- coordinate a fix and disclosure timeline based on severity, reproducibility, and money-path risk

When possible, please allow a private remediation window before public disclosure.

## Public issues are fine for

- non-sensitive hardening suggestions
- documentation gaps that do not reveal an exploit path
- already-public warning-tier findings that do not create an active exploitation risk

## Scope notes

- This repository ships **no** credentials, API keys, or signing keys. Any real-money use requires you to
  provision your own, and you are solely responsible for their protection.
- Reproduction should be performed dry-run / offline unless you are deliberately and safely exercising a
  sandbox or live lane with your own account.

---

<p align="center">
  <img src="assets/images/polymath_global.png" alt="Polymath Global" width="220">
</p>
