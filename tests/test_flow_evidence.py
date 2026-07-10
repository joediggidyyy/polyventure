"""Unit coverage for the shared outcome-labeling module (single source of truth)."""

from __future__ import annotations

from polyventure.flow_evidence import (
  OUTCOME_CLEAN_LABELS,
  OUTCOME_EXPOSURE_LABELS,
  is_authoritative_label,
  is_clean_label,
  is_exposure_label,
  label_pair_outcome,
)


def _label(yes, no, terminal='FILLED', statuses=('o:executed',), csv=False):
  return label_pair_outcome(
    yes_filled=yes, no_filled=no, raw_terminal_state=terminal,
    order_statuses=statuses, csv_settlement_confirmed=csv,
  )


def test_error_terminal():
  assert _label(5, 5, terminal='ERROR') == 'error_or_reconcile_required'
  assert _label(3, 0, terminal='RECONCILE_REQUIRED') == 'error_or_reconcile_required'


def test_zero_fill_variants():
  assert _label(0, 0, terminal='CANCELED') == 'canceled_clean'
  assert _label(0, 0, terminal='SUBMITTED', statuses=('o:resting',)) == 'submitted_no_fill'
  assert _label(0, 0, terminal='PLANNED', statuses=('o:planned',)) == 'unknown'


def test_equal_fills_clean():
  assert _label(10, 10) == 'both_filled_or_locked'


def test_unequal_fill_overrides_plain_settled():
  # Fill truth beats terminal projection: one-sided even when recorded SETTLED.
  assert _label(51, 0, terminal='SETTLED') == 'one_sided_exposure'


def test_settled_exposure_terminal():
  assert _label(51, 0, terminal='SETTLED_EXPOSURE') == 'settled_exposure'


def test_csv_settlement_confirms_exposure():
  assert _label(51, 0, terminal='SETTLED', csv=True) == 'settled_exposure'


def test_partial_both_unlocked_when_active():
  assert _label(30, 20, terminal='PARTIAL_BOTH') == 'partial_both_unlocked'


def test_label_set_membership_helpers():
  assert is_exposure_label('one_sided_exposure')
  assert is_clean_label('both_filled_or_locked')
  assert is_authoritative_label('one_sided_exposure')
  assert is_authoritative_label('both_filled_or_locked')
  assert not is_authoritative_label('unknown')
  # sets are disjoint
  assert not (OUTCOME_EXPOSURE_LABELS & OUTCOME_CLEAN_LABELS)
