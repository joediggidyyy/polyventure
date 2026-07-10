"""Unit coverage for the pure entry-window optimizer (C2 core)."""

from __future__ import annotations

from polyventure.parameter_optimizers import optimize_entry_window

_START = 900  # entry_window_start_sec seed
_END = 75     # entry_window_end_sec seed


def _rows(*specs):
  """specs: (seconds_to_close, label, count) tuples -> flat row list."""
  out = []
  for seconds, label, count in specs:
    out.extend({'seconds_to_close': seconds, 'outcome_label': label} for _ in range(count))
  return out


def _run(rows, **kw):
  return optimize_entry_window(rows, current_start_sec=_START, current_end_sec=_END, **kw)


def test_insufficient_evidence_holds_seed():
  res = _run(_rows((150, 'both_filled_or_locked', 10)))
  assert res['status'] == 'insufficient_evidence'
  assert res['suggested_start_sec'] == _START
  assert res['suggested_end_sec'] == _END


def test_best_band_recovers_and_narrows():
  # Clean cluster near 150s, exposure far off near 600s.
  res = _run(_rows((150, 'both_filled_or_locked', 35), (600, 'one_sided_exposure', 10)))
  assert res['status'] == 'optimized'
  # narrowed toward the clean band from both ends, never widened
  assert _END < res['suggested_end_sec'] <= _START
  assert _END <= res['suggested_start_sec'] < _START
  assert res['evidence']['authoritative_rows'] == 45


def test_no_net_positive_band_holds_seed():
  # Exposure dominates everywhere -> no band worth selecting.
  res = _run(_rows((150, 'one_sided_exposure', 20), (600, 'one_sided_exposure', 20)))
  assert res['status'] == 'no_change'
  assert res['suggested_start_sec'] == _START
  assert res['suggested_end_sec'] == _END


def test_never_widens_beyond_seed():
  res = _run(_rows((150, 'both_filled_or_locked', 40)))
  # narrowing invariant: start only decreases, end only increases
  assert res['suggested_start_sec'] <= _START
  assert res['suggested_end_sec'] >= _END


def test_throttle_limits_move():
  # With throttle 0.2, start moves at most 20% of the gap toward the target.
  res = _run(_rows((150, 'both_filled_or_locked', 40)), throttle=0.20)
  # target start ~164; move is a fraction of (164-900), so start stays well above 164
  assert res['suggested_start_sec'] > 300


def test_deterministic():
  rows = _rows((150, 'both_filled_or_locked', 35), (600, 'one_sided_exposure', 10))
  assert _run(rows) == _run(rows)


def test_non_authoritative_rows_ignored():
  # 'unknown' rows don't count toward the meter.
  res = _run(_rows((150, 'unknown', 40)))
  assert res['status'] == 'insufficient_evidence'
  assert res['evidence']['authoritative_rows'] == 0


def test_floor_end_sec_clamps_suggested_end():
  """Operator finding 2026-07-04: the ratified view buffer (75s) is a floor on the
  window end; a below-floor current value must not produce a below-floor suggestion."""
  rows = _rows((150, 'both_filled_or_locked', 35), (600, 'one_sided_exposure', 10))
  # current end deep below the floor; evidence pulls upward but the first throttled
  # step (60 -> ~78 unclamped is fine) — force a small throttle to land below 75.
  res = optimize_entry_window(
    rows, current_start_sec=900, current_end_sec=60, throttle=0.05, floor_end_sec=75,
  )
  assert res['status'] == 'optimized'
  assert res['suggested_end_sec'] >= 75
  assert res['evidence'].get('floor_clamped') is True


def test_floor_end_sec_inactive_when_above_floor():
  rows = _rows((150, 'both_filled_or_locked', 35), (600, 'one_sided_exposure', 10))
  res = optimize_entry_window(
    rows, current_start_sec=900, current_end_sec=120, floor_end_sec=75,
  )
  # already above the floor: clamp must not fire or alter the evidence flag
  assert res['suggested_end_sec'] >= 75
  assert 'floor_clamped' not in res['evidence']


def test_floor_clamp_preserves_end_below_start_invariant():
  # Degenerate cage: floor near the start bound still yields end < start.
  rows = _rows((150, 'both_filled_or_locked', 35))
  res = optimize_entry_window(
    rows, current_start_sec=160, current_end_sec=60, floor_end_sec=200,
  )
  assert res['suggested_end_sec'] < res['suggested_start_sec']
