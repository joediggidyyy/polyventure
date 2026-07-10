"""Regression guard for the no-fallback lane-labeling invariant.

Plan: SCHEMA_WIDE_LANE_DEFAULT_REMEDIATION_BMAP_2026-06-19 (Component 3).

Enforces, as an executable contract:
  1. Every lane-bearing table is born with `operation_lane TEXT NOT NULL` and NO
     schema DEFAULT on a fresh database (canonical shape).
  2. `_normalize_operation_lane` fails closed on empty/None/unknown (no 'sandbox'
     fallback).
  3. The two write paths that were silently defaulting now require an explicit lane
     and write exactly the lane supplied.
  4. Source guard: no lane-bearing CREATE TABLE declares operation_lane with a
     DEFAULT; the only retained `DEFAULT 'sandbox'` literals are the documented
     legacy ADD-COLUMN migration calls.

If any of these fail, the no-fallback principle has been violated -- treat as a
blocker, not a flaky test.
"""
import inspect
import re

import pytest

from polyventure import persistence
from polyventure.persistence import (
  _normalize_operation_lane,
  open_database,
  persist_candidate_review_candidates,
  persist_candidate_review_run,
  persist_candidate_saved_set,
  persist_candidate_saved_set_evaluation,
)

# The 15 tables that historically carried `operation_lane TEXT NOT NULL DEFAULT 'sandbox'`.
LANE_DEFAULT_TABLES = (
  'pair_plans', 'orders', 'fills', 'pair_states', 'pair_pnl_snapshots',
  'service_heartbeats', 'account_api_limits', 'operator_actions',
  'runtime_events', 'analytical_snapshots', 'candidate_review_runs',
  'candidate_review_candidates', 'candidate_saved_sets',
  'candidate_saved_set_members', 'candidate_saved_set_evaluations',
)


def test_fresh_db_lane_columns_have_no_default() -> None:
  connection = open_database(':memory:')
  for table in LANE_DEFAULT_TABLES:
    info = {row['name']: row for row in connection.execute(f'PRAGMA table_info({table})').fetchall()}
    assert 'operation_lane' in info, f'{table} missing operation_lane column'
    col = info['operation_lane']
    assert col['notnull'] == 1, f'{table}.operation_lane must be NOT NULL'
    assert col['dflt_value'] is None, (
      f"{table}.operation_lane must have NO default (canonical no-fallback shape); "
      f"found default {col['dflt_value']!r}"
    )


@pytest.mark.parametrize('bad', ['', None, 'production', 'SANDBOX_', 'liv'])
def test_normalize_operation_lane_fails_closed(bad) -> None:
  with pytest.raises(ValueError):
    _normalize_operation_lane(bad)


@pytest.mark.parametrize('good', ['sandbox', 'live', 'SANDBOX', ' Live '])
def test_normalize_operation_lane_accepts_valid(good) -> None:
  assert _normalize_operation_lane(good) in {'sandbox', 'live'}


def _seed_run(connection, run_id: str, lane: str) -> None:
  persist_candidate_review_run(
    connection,
    run_id=run_id,
    recorded_at_utc='2026-06-19T00:00:00Z',
    operation_lane=lane,
    candidate_signature='sig',
    candidate_count=1,
    source_action='scan',
  )


@pytest.mark.parametrize('lane', ['sandbox', 'live'])
def test_persist_candidate_review_candidates_writes_exact_lane(lane) -> None:
  connection = open_database(':memory:')
  _seed_run(connection, f'{lane}-run', lane)
  persist_candidate_review_candidates(
    connection,
    run_id=f'{lane}-run',
    recorded_at_utc='2026-06-19T00:00:01Z',
    operation_lane=lane,
    candidates=[{'candidate_uid': f'{lane}-c', 'candidate_key': f'{lane}-c'}],
  )
  row = connection.execute(
    'SELECT operation_lane FROM candidate_review_candidates WHERE candidate_uid = ?',
    (f'{lane}-c',),
  ).fetchone()
  assert row['operation_lane'] == lane


def test_persist_candidate_review_candidates_rejects_empty_lane() -> None:
  connection = open_database(':memory:')
  _seed_run(connection, 'x-run', 'sandbox')
  with pytest.raises(ValueError):
    persist_candidate_review_candidates(
      connection,
      run_id='x-run',
      recorded_at_utc='2026-06-19T00:00:01Z',
      operation_lane='',
      candidates=[{'candidate_uid': 'x-c', 'candidate_key': 'x-c'}],
    )


@pytest.mark.parametrize('lane', ['sandbox', 'live'])
def test_persist_candidate_saved_set_evaluation_writes_exact_lane(lane) -> None:
  connection = open_database(':memory:')
  _seed_run(connection, f'{lane}-run2', lane)
  persist_candidate_saved_set(
    connection,
    saved_set_id=f'{lane}-set',
    run_id=f'{lane}-run2',
    recorded_at_utc='2026-06-19T00:00:01Z',
    operation_lane=lane,
    saved_key_count=1,
    state_id='review_hold',
    source_action='save_selection',
    members=[{'candidate_uid': f'{lane}-c', 'candidate_key': f'{lane}-c'}],
  )
  persist_candidate_saved_set_evaluation(
    connection,
    saved_set_id=f'{lane}-set',
    recorded_at_utc='2026-06-19T00:00:02Z',
    operation_lane=lane,
    evaluation_status='saved',
    actionability_status='active_valid',
    visibility_status='default_actionable',
    offline_verifiable=True,
    online_revalidation_required=False,
  )
  row = connection.execute(
    'SELECT operation_lane FROM candidate_saved_set_evaluations WHERE saved_set_id = ?',
    (f'{lane}-set',),
  ).fetchone()
  assert row['operation_lane'] == lane


def test_persist_candidate_saved_set_evaluation_rejects_empty_lane() -> None:
  connection = open_database(':memory:')
  with pytest.raises(ValueError):
    persist_candidate_saved_set_evaluation(
      connection,
      saved_set_id='no-lane-set',
      recorded_at_utc='2026-06-19T00:00:02Z',
      operation_lane='',
      evaluation_status='saved',
      actionability_status='active_valid',
      visibility_status='default_actionable',
      offline_verifiable=True,
      online_revalidation_required=False,
    )


def test_source_has_no_lane_default_in_create_table() -> None:
  """No CREATE TABLE column pairs operation_lane with a DEFAULT. The only retained
  `DEFAULT 'sandbox'` literals are the documented legacy ADD-COLUMN migration calls
  (_ensure_column(...)), which are required by SQLite and never fire on writes."""
  source = inspect.getsource(persistence)
  # Inline column declarations of the canonical shape must not carry a default.
  bad_inline = re.findall(r"operation_lane\s+TEXT\s+NOT\s+NULL\s+DEFAULT", source)
  assert not bad_inline, (
    'A CREATE TABLE column declares operation_lane with a DEFAULT -- no-fallback violation. '
    f'Found {len(bad_inline)} occurrence(s).'
  )
  # Every retained `DEFAULT 'sandbox'` literal must be inside an _ensure_column call
  # (the legacy ADD-COLUMN path). Confirm none appear as a bare column declaration.
  for match in re.finditer(r"DEFAULT 'sandbox'", source):
    window = source[max(0, match.start() - 80):match.start()]
    assert '_ensure_column' in window, (
      "A `DEFAULT 'sandbox'` literal appears outside an _ensure_column legacy-migration "
      'call -- only the legacy ADD-COLUMN path may retain it.'
    )
