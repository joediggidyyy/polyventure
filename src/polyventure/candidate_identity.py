"""Canonical candidate identity — single shared derivation.

Lane 0 of the candidate single-source-of-truth work
(CANDIDATE_PERSISTENCE_UNIFORM_CONTRACT_BMAP_2026-06-19).

Both candidate writers (the candidate-math evidence contract in service.py and the
save_selection / card path in web_app.py) MUST derive candidate identity through this
one function so the same candidate yields the same `candidate_uid` / `candidate_key`
across every surface. The derivation uses display-stable INTRINSIC fields
(ticker + event_ticker + a stable selection/contract label) and explicitly NOT
`qualifier_tier` / `rank` / price strings, which change across re-scans and re-ranks.

This matches the card path's historical `_candidate_review_uid` derivation for real
candidates (every real candidate carries a ticker); it is index-free so both paths
agree without positional context, and falls back to a deterministic digest only for
the degenerate case of a candidate with no identifying fields at all (never a real
candidate) — deterministic, so both paths still agree.
"""
from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

# Candidate-expiry seed buffers (seconds) — data-derived (BMAP §9.1, from the
# 2026-06-19/20 monitor latency series), ratified 2026-06-20. These are the SEED
# and the FLOOR: the self-calibrator (Lane S) may raise the effective buffers above
# these from live latency, but the operating value must never fall below them.
SEED_VIEW_BUFFER_SEC = 75
SEED_SUBMIT_BUFFER_SEC = 10
SEED_POST_SUBMIT_PROCESSING_BUFFER_SEC = 180
FLOOR_POST_SUBMIT_PROCESSING_BUFFER_SEC = 60

# Selection/contract label precedence — display-stable fields that disambiguate
# distinct selections of the same market. Order preserved from the card path.
_SELECTION_LABEL_FIELDS = (
  'contract_label',
  'selection_label',
  'yes_sub_title',
  'no_sub_title',
  'market_title',
  'title',
  'event_title',
)

# Fields hashed for the deterministic last-resort identity (no real candidate hits this).
_DIGEST_FIELDS = ('ticker', 'event_ticker', *_SELECTION_LABEL_FIELDS)


def candidate_identity_component(value: Any) -> str:
  """Whitespace-normalize a single identity component (matches the card path helper)."""
  return re.sub(r'\s+', ' ', str(value or '').strip())


def canonical_candidate_uid(candidate: Mapping[str, Any]) -> str:
  """Return the canonical, display-stable candidate identity.

  Precedence:
    1. an explicit `candidate_uid` already on the record (idempotent reuse);
    2. `ticker :: event_ticker :: selection_label` from intrinsic fields;
    3. a deterministic digest of the identifying fields (degenerate only).
  """
  explicit = candidate_identity_component(candidate.get('candidate_uid'))
  if explicit:
    return explicit
  ticker = candidate_identity_component(candidate.get('ticker'))
  event_ticker = candidate_identity_component(candidate.get('event_ticker'))
  selection_label = ''
  for field in _SELECTION_LABEL_FIELDS:
    selection_label = candidate_identity_component(candidate.get(field))
    if selection_label:
      break
  parts = [part for part in (ticker, event_ticker, selection_label) if part]
  if parts:
    return '::'.join(parts)
  # Degenerate: no ticker and no labels. Deterministic digest so both writer paths
  # still produce the same id (never a real candidate — all carry a ticker).
  digest = hashlib.sha256(
    json.dumps({field: candidate.get(field) for field in _DIGEST_FIELDS}, sort_keys=True, default=str).encode('utf-8')
  ).hexdigest()[:12]
  return f'candidate-{digest}'


def canonical_candidate_key(candidate: Mapping[str, Any]) -> str:
  """The candidate key is the canonical uid (one identity, one key)."""
  return canonical_candidate_uid(candidate)


def _parse_close_time(value: Any) -> datetime | None:
  raw = candidate_identity_component(value)
  if not raw:
    return None
  try:
    parsed = datetime.fromisoformat(raw.replace('Z', '+00:00'))
  except ValueError:
    return None
  if parsed.tzinfo is None:
    parsed = parsed.replace(tzinfo=timezone.utc)
  return parsed.astimezone(timezone.utc)


def _iso_z(moment: datetime) -> str:
  return moment.replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def compute_candidate_deadlines(
  close_time_utc: Any,
  effective_buffers: Mapping[str, Any] | None = None,
) -> dict[str, str] | None:
  """Discovery-time expiry clock (BMAP Lane A) — single shared derivation.

  Returns the three deadline timestamps derived from one close time minus positive
  buffers, or ``None`` (FAIL CLOSED, write nothing) when the close time is absent /
  unparseable, or when the buffers violate the ordering invariant ``view >= submit``.

    market_close_at_utc   = close                       (Kalshi close / backstop, #3)
    submit_expires_at_utc = close - submit_buffer       (#2 submit-eligibility deadline)
    view_expires_at_utc   = close - view_buffer         (#1 selection-eligibility deadline)

  ``view >= submit`` guarantees the selection surface closes before the submit window,
  so a still-selectable candidate is always still submittable. The only way to violate
  it is misconfigured manual seeds; the Lane S parameter-submit floor prevents that at
  entry, and this helper fails closed if it ever sees a violation.
  """
  close = _parse_close_time(close_time_utc)
  if close is None:
    return None
  buffers = effective_buffers or {}
  try:
    view_buffer = int(buffers.get('view', SEED_VIEW_BUFFER_SEC))
    submit_buffer = int(buffers.get('submit', SEED_SUBMIT_BUFFER_SEC))
  except (TypeError, ValueError):
    return None
  if submit_buffer < 0 or view_buffer < submit_buffer:
    return None  # invariant violation -> fail closed (Lane S also warns at entry)
  return {
    'market_close_at_utc': _iso_z(close),
    'submit_expires_at_utc': _iso_z(close - timedelta(seconds=submit_buffer)),
    'view_expires_at_utc': _iso_z(close - timedelta(seconds=view_buffer)),
  }
