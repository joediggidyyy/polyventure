# Contributing to Polyventure

**Document ID**: `POLYVENTURE_CONTRIBUTING`  
**Status**: Public contributing guidelines  
**Maintainer**: Joe Waller  
**Project**: Polyventure / Polymath  
**Last updated**: 2026-07-09

---

<p align="center">
  <img src="assets/images/polyventure_logo.png" alt="Polyventure logo" width="200">
</p>

Thanks for your interest. Polyventure is a Polymath open-source project released for scholarly review and
reproducibility. Contributions, questions, and reproduction reports are welcome.

## Working root

Treat this repository root as the canonical working root for build, test, and documentation work. Do not
rely on parent-repository paths, machine-local absolute paths, or environment-specific assumptions in any
public-facing surface.

## Environment

Polyventure targets **Python 3.14** (`>=3.14,<3.15`).

```bash
python -m venv .venv
# Windows: .venv\Scripts\activate   |   POSIX: source .venv/bin/activate
pip install -e '.[dev]'
```

No credentials or keys are included in this repository. The shell boots **offline** and performs no
operative action until a lane is explicitly selected; sandbox / dry-run is the safe default.

## Baseline checks

Run these from the repository root before opening a pull request:

```bash
pytest
```

Calamum is the Polymath retained-evidence runner used by the broader development workflow, but this public
package is intentionally testable with plain `pytest` from a clean clone.

<p align="center">
  <img src="assets/images/calamum_logo_color.png" alt="Calamum retained-evidence test runner" width="220">
</p>

The test suite provisions an ephemeral signing key for the signed-evidence surface, so it runs without any
real key on disk. Tests that exercise operator-specific developer tooling skip gracefully when that tooling
is not present in this package.

## Design principles to preserve

- **Fail closed, no silent fallbacks.** Lane, credential, and endpoint selection must be explicit; an
  unresolved value rests in an inert `offline` state rather than silently defaulting to an operative lane.
- **Dry-run first.** Keep planned, simulated, sandboxed, and live states explicit and distinct in every
  operator-facing surface. Do not broaden live-lane scope without a clear, documented reason.
- **Authoritative data only.** Financial-decision inputs come from authoritative platform data, not implied
  or derived values.

## Documentation discipline

Keep public-facing docs:

- free of local absolute paths and personal data
- free of parent-repository assumptions
- explicit about validated versus planned behavior

## Pull requests

- Keep changes focused and describe what was validated and how.
- Include or update tests for behavior changes.
- Ensure `pytest` passes and no new secrets, keys, or absolute paths are introduced.

## Security reports

If you discover a potentially sensitive vulnerability, **do not open a public issue first**. Use the
private reporting guidance in [`SECURITY.md`](SECURITY.md).

---

<p align="center">
  <img src="assets/images/polymath_global.png" alt="Polymath Global" width="240">
</p>
