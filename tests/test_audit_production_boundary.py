"""Static boundary guards: audit-tier code MUST NOT leak into the shippable
``polyventure`` production package.

These tests are deterministic, fast, and source-only (no imports of audit
modules). They fail-closed if any of the following occurs:

1. A production source file imports any audit-tier module (the
   ``semantics_staging`` monitor, the audit cross-walk, the audit schema
   validator, or any future ``monitor_*`` module placed in that directory).
2. A production source file references an audit-tier artifact path or
   directory (monitor packet directories, cross-walk output directories, the
   audit schema filename).
3. A production source file reads any of the monitor's output dict keys
   (``run_response_projection_rows``, ``latest_run_response_projection``,
   ``runtime_scan_event_rows``) -- those are monitor-side projections and
   must never be consumed by production code.
4. The wheel packaging configuration in ``pyproject.toml`` would include the
   ``semantics_staging`` monitor directory.

The single permitted writer-side contract token in production is the literal
``event_type='run_response_projection'`` used by the persistence helper in
``web_app.py``; the contract is one-way (production writes, monitor reads via
the shared SQLite table). That writer token is allowed by design.

This test does NOT enforce style-level UI-language rules at runtime; it is a
pure source-tree static check intended to catch boundary violations before
they ship.
"""

from __future__ import annotations

import re
from pathlib import Path


SRC_ROOT = Path(__file__).resolve().parent.parent / 'src' / 'polyventure'
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Production source files (no tests, no audit-tier modules).
def _production_source_files() -> list[Path]:
    return sorted(p for p in SRC_ROOT.rglob('*.py') if p.is_file())


# ---------- Rule 1: no imports of audit-tier modules ----------

# Audit-tier module names that must NEVER be imported by production code.
# These live outside src/ and are explicitly development-scoped.
_FORBIDDEN_IMPORT_TOKENS = (
    'semantics_staging',
    'monitor_find_candidates_live_run',
    'audit_ui_backend_crosswalk',
    'audit_schema_validator',
)

_IMPORT_LINE_RE = re.compile(r'^\s*(?:from|import)\s+([\w\.]+)', re.MULTILINE)


def test_production_source_does_not_import_audit_tier_modules():
    offenders: list[str] = []
    for path in _production_source_files():
        text = path.read_text(encoding='utf-8')
        for match in _IMPORT_LINE_RE.finditer(text):
            module = match.group(1)
            for token in _FORBIDDEN_IMPORT_TOKENS:
                if token in module:
                    offenders.append(f'{path.relative_to(PROJECT_ROOT)}: {match.group(0).strip()}')
    assert offenders == [], (
        'Production code imported audit-tier module(s); the monitor / audit '
        'tier must remain development-scoped:\n  ' + '\n  '.join(offenders)
    )


# ---------- Rule 2: no audit-tier artifact path references ----------

# Filesystem path or directory tokens that only the audit tier produces.
# Production code has no legitimate reason to reference these strings.
_FORBIDDEN_ARTIFACT_PATH_TOKENS = (
    'find_candidates_proof_packets',
    'audit_ui_backend_crosswalk',
    'audit_sample_schema_v1',
    'semantics_staging/',
    'semantics_staging\\\\',
    '.calamum',
    'calamum_run_index',
)


# ---------- Rule 5: no calamum vocabulary in production source ----------

# Calamum is a separate development-tier validation harness. Production code
# must not reference its name, its lane vocabulary, or its artifact contracts.
# The bare token 'pytest' is NOT forbidden -- it is generic Python tooling
# vocabulary used in unrelated comments and identifiers.
_FORBIDDEN_CALAMUM_VOCABULARY_TOKENS = (
    'calamum',
    'Calamum',
    'sandbox_test',
    'empirical_test',
)


def test_production_source_does_not_reference_calamum_vocabulary():
    offenders: list[str] = []
    for path in _production_source_files():
        text = path.read_text(encoding='utf-8')
        for token in _FORBIDDEN_CALAMUM_VOCABULARY_TOKENS:
            if token in text:
                offenders.append(f'{path.relative_to(PROJECT_ROOT)}: contains {token!r}')
    assert offenders == [], (
        'Production code referenced calamum-tier vocabulary; the calamum '
        'validation harness is development-scoped and must not leak into the '
        'shippable package:\n  ' + '\n  '.join(offenders)
    )


def test_production_source_does_not_reference_audit_artifact_paths():
    offenders: list[str] = []
    for path in _production_source_files():
        text = path.read_text(encoding='utf-8')
        for token in _FORBIDDEN_ARTIFACT_PATH_TOKENS:
            if token in text:
                offenders.append(f'{path.relative_to(PROJECT_ROOT)}: contains {token!r}')
    assert offenders == [], (
        'Production code referenced audit-tier artifact path token(s); '
        'production must not know where the monitor or cross-walk writes:\n  '
        + '\n  '.join(offenders)
    )


# ---------- Rule 3: no consumption of monitor-side projection keys ----------

# Dict keys produced by the monitor's summarize_database_runtime() output.
# These names belong to the audit tier; production must not read them back.
_FORBIDDEN_MONITOR_OUTPUT_KEYS = (
    'run_response_projection_rows',
    'latest_run_response_projection',
    'runtime_scan_event_rows',
)


def test_production_source_does_not_consume_monitor_output_keys():
    offenders: list[str] = []
    for path in _production_source_files():
        text = path.read_text(encoding='utf-8')
        for key in _FORBIDDEN_MONITOR_OUTPUT_KEYS:
            if key in text:
                offenders.append(f'{path.relative_to(PROJECT_ROOT)}: contains {key!r}')
    assert offenders == [], (
        'Production code referenced monitor-side projection key(s); these '
        'keys are emitted by the audit monitor and must not be consumed by '
        'production:\n  ' + '\n  '.join(offenders)
    )


# ---------- Rule 4: packaging excludes audit-tier directories ----------

def test_pyproject_packaging_excludes_audit_tier_directories():
    pyproject = (PROJECT_ROOT / 'pyproject.toml').read_text(encoding='utf-8')
    # The setuptools find scope must be src/ only.
    assert "where = ['src']" in pyproject or 'where = ["src"]' in pyproject, (
        'pyproject.toml must scope setuptools.packages.find to src/ only so '
        'that semantics_staging/ cannot be packaged into the wheel.'
    )
    # The include allow-list must NOT mention the monitor staging dir.
    assert 'semantics_staging' not in pyproject, (
        'pyproject.toml must not mention semantics_staging/; the audit '
        'monitor is development-scoped and must not ship.'
    )
