"""Shared outcome-labeling evidence logic (single source of truth).

Both the offline flow-balance dataset/replay tool
(`tools/build_flow_balance_replay_dataset.py`) and the in-app selection/timing
optimizers (entry window C2, joint edge/profit C3) import this module so the
outcome labels they reason over are computed by exactly ONE implementation.

This is the VL3 "calculation cross-validation" guarantee from the parameter-
optimization BMAP: one labeler, no drift between the offline evidence and the
live optimizer. Pure logic only -- no I/O, no DB, no network.
"""

from __future__ import annotations

from typing import Iterable

# Outcome labels that carry one-sided / settlement exposure (money-losing risk mode).
OUTCOME_EXPOSURE_LABELS = frozenset(
  {'one_sided_exposure', 'settled_exposure', 'partial_both_unlocked'}
)
# Outcome labels that are clean (both legs, cleanly cancelled, or no fill).
OUTCOME_CLEAN_LABELS = frozenset(
  {'both_filled_or_locked', 'canceled_clean', 'submitted_no_fill'}
)

# Pair states that mean the pair is still in-flight (not a settlement-shaped
# terminal), used to distinguish partial_both_unlocked from one_sided_exposure.
ACTIVE_STATES = frozenset(
  {
    'PLANNED',
    'SUBMITTING',
    'SUBMITTED',
    'RESTING_BOTH',
    'PARTIAL_BOTH',
    'PARTIAL_ONE_SIDE',
    'REPAIR_LIVE',
    'EXPOSURE_CAPPED',
    'LOCKED',
  }
)
ERROR_STATES = frozenset({'ERROR', 'RECONCILE_REQUIRED'})

# Human-readable precedence of the labeler, in rule order.
LABEL_PRECEDENCE_DOC = (
  '1 error/reconcile terminal -> error_or_reconcile_required',
  '2 zero fills: CANCELED terminal -> canceled_clean; any non-planned order -> submitted_no_fill; else unknown',
  '3 equal fills on both sides (> 0) -> both_filled_or_locked',
  '4 unequal fills + (terminal SETTLED_EXPOSURE or unambiguous authoritative CSV settlement) -> settled_exposure',
  '5 unequal fills, both sides > 0, terminal still active -> partial_both_unlocked',
  '6 unequal fills otherwise (includes plain SETTLED: fill truth overrides terminal projection) -> one_sided_exposure',
)


def label_pair_outcome(
  *,
  yes_filled,
  no_filled,
  raw_terminal_state,
  order_statuses: Iterable[str],
  csv_settlement_confirmed: bool,
) -> str:
  """Return the canonical outcome label for a pair from fill truth + terminal state.

  Fill truth (unequal filled contracts on the two sides) overrides terminal
  projection: a pair recorded as plain SETTLED but with only one side filled is
  labeled one_sided_exposure, not clean. See LABEL_PRECEDENCE_DOC.
  """
  terminal = raw_terminal_state or ''
  if terminal in ERROR_STATES:
    return 'error_or_reconcile_required'
  if yes_filled == 0 and no_filled == 0:
    if terminal == 'CANCELED':
      return 'canceled_clean'
    submitted = any(not key.endswith(':planned') for key in order_statuses)
    if submitted:
      return 'submitted_no_fill'
    return 'unknown'
  if yes_filled == no_filled:
    return 'both_filled_or_locked'
  # Unequal fill truth: exposure-bearing regardless of terminal projection.
  if terminal == 'SETTLED_EXPOSURE' or (
    csv_settlement_confirmed and terminal not in ACTIVE_STATES
  ):
    return 'settled_exposure'
  if yes_filled > 0 and no_filled > 0 and terminal in ACTIVE_STATES:
    return 'partial_both_unlocked'
  return 'one_sided_exposure'


def is_exposure_label(label: str) -> bool:
  """True if the label carries one-sided/settlement exposure (loss-risk mode)."""
  return label in OUTCOME_EXPOSURE_LABELS


def is_clean_label(label: str) -> bool:
  """True if the label is a clean outcome (no one-sided exposure)."""
  return label in OUTCOME_CLEAN_LABELS


def is_authoritative_label(label: str) -> bool:
  """True if the label is a real, guard-evaluable outcome (clean or exposure)."""
  return label in OUTCOME_EXPOSURE_LABELS or label in OUTCOME_CLEAN_LABELS
