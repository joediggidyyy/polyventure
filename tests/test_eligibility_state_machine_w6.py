from __future__ import annotations

from pathlib import Path

import pytest

from polyventure.persistence import (
  fetch_operator_notifications,
  open_database,
  persist_candidate_review_candidates,
  persist_candidate_review_run,
)
from polyventure.service import _apply_eligibility_event


def _seed_candidate(db_path: Path, *, run_id: str = 'run-1', candidate_uid: str = 'cand-1') -> tuple[object, str, str]:
  connection = open_database(db_path)
  persist_candidate_review_run(
    connection,
    run_id=run_id,
    recorded_at_utc='2026-06-04T00:00:00Z',
    operation_lane='sandbox',
    candidate_signature='sig',
    candidate_count=1,
    source_action='scan',
  )
  persist_candidate_review_candidates(
    connection,
    run_id=run_id,
    recorded_at_utc='2026-06-04T00:00:00Z',
    operation_lane='sandbox',
    candidates=[
      {
        'candidate_uid': candidate_uid,
        'candidate_key': candidate_uid,
        'ticker': 'TICKER',
        'qualifier_tier': 'live_qualifying',
      }
    ],
  )
  return connection, run_id, candidate_uid


def test_in_flight_revocation_transitions_to_canceled_terminal(tmp_path: Path) -> None:
  connection, run_id, candidate_uid = _seed_candidate(tmp_path / 'state.db')
  with connection:
    connection.execute(
      "UPDATE candidate_review_candidates SET lifecycle_stage='in_flight' WHERE run_id=? AND candidate_uid=?",
      (run_id, candidate_uid),
    )

  _apply_eligibility_event(
    connection,
    run_id=run_id,
    candidate_uid=candidate_uid,
    event_key='eligibility_revoked_in_flight',
    operation_lane='sandbox',
    profile_token='kalshi-test01',
  )

  row = connection.execute(
    "SELECT lifecycle_stage, eligibility_status, terminal_cause, terminal_subcause FROM candidate_review_candidates WHERE run_id=? AND candidate_uid=?",
    (run_id, candidate_uid),
  ).fetchone()
  assert row is not None
  assert row['lifecycle_stage'] == 'terminal'
  assert row['eligibility_status'] == 'revoked_in_flight'
  assert row['terminal_cause'] == 'canceled'
  assert row['terminal_subcause'] == 'eligibility_revoked'

  notifications = fetch_operator_notifications(
    connection,
    operation_lane='sandbox',
    profile_token='kalshi-test01',
  )
  assert notifications
  assert notifications[0]['level'] == 'warn'


def test_discovered_blocked_sets_missing_blocked(tmp_path: Path) -> None:
  connection, run_id, candidate_uid = _seed_candidate(tmp_path / 'state.db')

  _apply_eligibility_event(
    connection,
    run_id=run_id,
    candidate_uid=candidate_uid,
    event_key='eligibility_blocked',
    operation_lane='sandbox',
    profile_token='kalshi-test01',
  )

  row = connection.execute(
    "SELECT lifecycle_stage, eligibility_status, terminal_cause FROM candidate_review_candidates WHERE run_id=? AND candidate_uid=?",
    (run_id, candidate_uid),
  ).fetchone()
  assert row is not None
  assert row['lifecycle_stage'] == 'discovered'
  assert row['eligibility_status'] == 'missing_blocked'
  assert row['terminal_cause'] is None


def test_unknown_eligibility_event_raises(tmp_path: Path) -> None:
  connection, run_id, candidate_uid = _seed_candidate(tmp_path / 'state.db')

  with pytest.raises(KeyError):
    _apply_eligibility_event(
      connection,
      run_id=run_id,
      candidate_uid=candidate_uid,
      event_key='unknown_event',
      operation_lane='sandbox',
      profile_token='kalshi-test01',
    )
