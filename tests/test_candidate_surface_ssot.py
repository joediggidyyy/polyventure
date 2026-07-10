"""Lanes 1-4 — candidate surface single-source-of-truth.

Plan: CANDIDATE_SURFACE_SINGLE_SOURCE_OF_TRUTH_BMAP_2026-06-19.

Proves that every candidate-bearing surface derives from one canonical DB query:
  1  _query_canonical_candidates returns only qualifying active set for a session,
     deduped by ticker; near-miss and other-session rows excluded.
  2  _build_pair_monitor_payload derives cards from the canonical DB query, not the
     ephemeral scan payload; card count equals canonical count; lifecycle_stage present.
  3  result_candidate_count reflects canonical DB count, not ephemeral scan result.
  4  _fetch_stage_columns (execution-panel driver) excludes near-miss rows — counts
     and stage items from the canonical query agree with Lane 1.
"""
from __future__ import annotations

import json
import os
import tempfile
from typing import Any

import pytest

from datetime import datetime, timezone

from polyventure.persistence import (
  open_database,
  persist_candidate_review_candidates,
  persist_candidate_review_run,
  persist_runtime_event,
)
from polyventure.candidate_identity import (
  SEED_SUBMIT_BUFFER_SEC,
  SEED_VIEW_BUFFER_SEC,
  compute_candidate_deadlines,
)
from polyventure.service import _lane_session_stopped_after, _mark_expired_candidates_terminal
from polyventure.web_app import (
  _build_pair_monitor_payload,
  _fetch_stage_columns,
  _query_canonical_candidates,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tmp_db() -> str:
  fd, path = tempfile.mkstemp(suffix='.db')
  os.close(fd)
  open_database(path)  # initialise schema
  return path


def _seed_run(
  db_path: str,
  run_id: str,
  lane_session_id: str,
  lane: str = 'live',
) -> None:
  conn = open_database(db_path)
  persist_candidate_review_run(
    conn,
    run_id=run_id,
    recorded_at_utc='2026-06-19T20:00:00Z',
    operation_lane=lane,
    lane_session_id=lane_session_id,
    candidate_signature='sig',
    candidate_count=1,
    source_action='scan',
  )


def _seed_candidate(
  db_path: str,
  run_id: str,
  ticker: str,
  qualifier_tier: str,
  lifecycle_stage: str = 'discovered',
  lane: str = 'live',
  detail: dict[str, Any] | None = None,
) -> str:
  uid = f'{ticker}::{qualifier_tier}'
  conn = open_database(db_path)
  persist_candidate_review_candidates(
    conn,
    run_id=run_id,
    recorded_at_utc='2026-06-19T20:00:01Z',
    operation_lane=lane,
    candidates=[{
      'candidate_uid': uid,
      'candidate_key': uid,
      'ticker': ticker,
      'qualifier_tier': qualifier_tier,
      'review_row_origin': 'current',
      **(detail or {'yes_sub_title': f'Test {ticker}', 'event_ticker': ticker}),
    }],
  )
  if lifecycle_stage != 'discovered':
    conn.execute(
      "UPDATE candidate_review_candidates SET lifecycle_stage = ? WHERE candidate_uid = ? AND run_id = ?",
      (lifecycle_stage, uid, run_id),
    )
    conn.commit()
  return uid


def _payload(db_path: str, lane_session_id: str) -> dict:
  return {
    'review_selection': {'persisted_lane_session_id': lane_session_id},
    'scan_runtime': {},
    'settings': {'state_db_path': db_path, 'operation_lane': 'live'},
  }


# ---------------------------------------------------------------------------
# Lane 1 — canonical query
# ---------------------------------------------------------------------------

def test_canonical_query_returns_qualifying_only() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-A')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying')
  _seed_candidate(db, 'run1', 'KXETH', 'near_miss')
  rows = _query_canonical_candidates('sess-A', db)
  tickers = {r['ticker'] for r in rows}
  assert 'KXBTC' in tickers, 'qualifying ticker must be in canonical set'
  assert 'KXETH' not in tickers, 'near_miss ticker must be excluded from canonical set'


def test_canonical_query_deduplicates_by_ticker() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-B')
  _seed_run(db, 'run2', 'sess-B')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying')
  _seed_candidate(db, 'run2', 'KXBTC', 'live_qualifying')
  rows = _query_canonical_candidates('sess-B', db)
  assert len(rows) == 1, 'same ticker across multiple runs must be deduped to one row'
  assert rows[0]['ticker'] == 'KXBTC'


def test_canonical_query_excludes_other_session_rows() -> None:
  db = _tmp_db()
  _seed_run(db, 'run-A', 'sess-active')
  _seed_run(db, 'run-B', 'sess-old')
  _seed_candidate(db, 'run-A', 'KXBTC', 'live_qualifying')
  _seed_candidate(db, 'run-B', 'KXETH', 'live_qualifying')
  rows = _query_canonical_candidates('sess-active', db)
  tickers = {r['ticker'] for r in rows}
  assert 'KXBTC' in tickers
  assert 'KXETH' not in tickers, 'rows from a different lane_session must be excluded'


def test_canonical_query_returns_empty_when_no_session() -> None:
  db = _tmp_db()
  rows = _query_canonical_candidates('', db)
  assert rows == []


def test_canonical_query_includes_lifecycle_stage() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-C')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying', lifecycle_stage='terminal')
  rows = _query_canonical_candidates('sess-C', db)
  assert rows[0]['lifecycle_stage'] == 'terminal'


# ---------------------------------------------------------------------------
# Lane 2 — cards from canonical query
# ---------------------------------------------------------------------------

def test_cards_derive_from_db_not_ephemeral_payload() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-D')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying', detail={
    'yes_sub_title': 'BTC target', 'event_ticker': 'KXBTC15M',
  })
  payload = {
    **_payload(db, 'sess-D'),
    # Ephemeral scan payload with a DIFFERENT ticker — must be ignored.
    'candidates': [{'ticker': 'KXFAKE', 'candidate_uid': 'fake'}],
    'sandbox_candidates_extended': [{'ticker': 'KXFAKE2', 'candidate_uid': 'fake2'}],
    '_stage_columns': [],
    '_in_flight_candidate_count': 0,
    '_total_stage_candidate_count': 0,
    '_active_stage_candidate_count': 0,
  }
  result = _build_pair_monitor_payload(payload)
  card_tickers = {c['ticker'] for c in result['candidate_rows']}
  assert 'KXBTC' in card_tickers, 'canonical DB ticker must appear in cards'
  assert 'KXFAKE' not in card_tickers, 'ephemeral payload ticker must not appear in cards'
  assert 'KXFAKE2' not in card_tickers, 'ephemeral payload ticker must not appear in cards'


def test_cards_fail_closed_when_no_session() -> None:
  db = _tmp_db()
  payload = {
    'review_selection': {},
    'scan_runtime': {},
    'settings': {'state_db_path': db, 'operation_lane': 'live'},
    'candidates': [{'ticker': 'KXBTC', 'candidate_uid': 'x'}],
    '_stage_columns': [],
    '_in_flight_candidate_count': 0,
    '_total_stage_candidate_count': 0,
    '_active_stage_candidate_count': 0,
  }
  result = _build_pair_monitor_payload(payload)
  assert result['candidate_count'] == 0
  assert result['candidate_rows'] == []


def test_offline_lane_empties_candidate_surfaces() -> None:
  db = _tmp_db()
  _seed_run(db, 'run-offline', 'sess-offline')
  _seed_candidate(db, 'run-offline', 'KXOFF', 'live_qualifying', lifecycle_stage='in_flight')
  payload = {
    **_payload(db, 'sess-offline'),
    'settings': {'state_db_path': db, 'operation_lane': 'offline'},
    '_stage_columns': [],
    '_in_flight_candidate_count': 1,
    '_total_stage_candidate_count': 1,
    '_active_stage_candidate_count': 1,
  }

  result = _build_pair_monitor_payload(payload)
  stages = _fetch_stage_columns(payload)

  assert result['candidate_count'] == 0
  assert result['candidate_rows'] == []
  assert stages['in_flight_candidate_count'] == 0
  assert stages['total_stage_candidate_count'] == 0
  assert stages['active_stage_candidate_count'] == 0
  assert all(not column['items'] for column in stages['stage_columns'])


def test_cards_carry_lifecycle_stage() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-E')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying', lifecycle_stage='terminal')
  payload = {
    **_payload(db, 'sess-E'),
    '_stage_columns': [],
    '_in_flight_candidate_count': 0,
    '_total_stage_candidate_count': 0,
    '_active_stage_candidate_count': 0,
  }
  result = _build_pair_monitor_payload(payload)
  assert result['candidate_rows'][0]['lifecycle_stage'] == 'terminal'


def test_cards_exclude_near_miss() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-F')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying')
  _seed_candidate(db, 'run1', 'KXETH', 'near_miss')
  payload = {
    **_payload(db, 'sess-F'),
    '_stage_columns': [],
    '_in_flight_candidate_count': 0,
    '_total_stage_candidate_count': 0,
    '_active_stage_candidate_count': 0,
  }
  result = _build_pair_monitor_payload(payload)
  card_tickers = {c['ticker'] for c in result['candidate_rows']}
  assert 'KXBTC' in card_tickers
  assert 'KXETH' not in card_tickers


# ---------------------------------------------------------------------------
# Lane 3 — canonical count coherence
# ---------------------------------------------------------------------------

def test_canonical_count_excludes_near_miss() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-G')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying')
  _seed_candidate(db, 'run1', 'KXETH', 'near_miss')
  count = len(_query_canonical_candidates('sess-G', db))
  assert count == 1, 'near_miss must not contribute to the canonical count'


def test_canonical_count_agrees_with_card_count() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-H')
  for i in range(3):
    _seed_candidate(db, 'run1', f'KXTICKER{i}', 'live_qualifying')
  _seed_candidate(db, 'run1', 'KXNM', 'near_miss')
  canonical_count = len(_query_canonical_candidates('sess-H', db))
  payload = {
    **_payload(db, 'sess-H'),
    '_stage_columns': [],
    '_in_flight_candidate_count': 0,
    '_total_stage_candidate_count': 0,
    '_active_stage_candidate_count': 0,
  }
  result = _build_pair_monitor_payload(payload)
  assert result['candidate_count'] == canonical_count == 3


# ---------------------------------------------------------------------------
# Lane 4 — stage columns exclude near-miss (execution-panel driver)
# ---------------------------------------------------------------------------

def test_stage_columns_exclude_near_miss() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-I')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying', lifecycle_stage='discovered')
  _seed_candidate(db, 'run1', 'KXETH', 'near_miss', lifecycle_stage='discovered')
  payload = _payload(db, 'sess-I')
  result = _fetch_stage_columns(payload)
  queued_tickers = {item['ticker'] for item in result['stage_columns'][0]['items']}
  assert 'KXBTC' in queued_tickers
  assert 'KXETH' not in queued_tickers
  assert result['in_flight_candidate_count'] == 1


def test_canonical_count_operator_session_spans_multiple_runs() -> None:
  # Lane 3 session-scope fix: the count override uses persisted_lane_session_id
  # (operator session), which accumulates across all scan runs in the session.
  # Verifies that the canonical query returns the deduped union of all runs for
  # the operator session, not just one scan session.
  db = _tmp_db()
  operator_session = 'live-20260619T230212Z-testfix'
  _seed_run(db, 'scan-run-1', operator_session)
  _seed_run(db, 'scan-run-2', operator_session)
  _seed_candidate(db, 'scan-run-1', 'KXBTC', 'live_qualifying')
  _seed_candidate(db, 'scan-run-2', 'KXETH', 'live_qualifying')
  # same ticker in both runs — dedup must keep only one
  _seed_candidate(db, 'scan-run-1', 'KXDUP', 'live_qualifying')
  _seed_candidate(db, 'scan-run-2', 'KXDUP', 'live_qualifying')
  rows = _query_canonical_candidates(operator_session, db)
  tickers = {r['ticker'] for r in rows}
  assert 'KXBTC' in tickers, 'run-1 ticker must appear in operator session count'
  assert 'KXETH' in tickers, 'run-2 ticker must appear in operator session count'
  assert len(rows) == 3, 'deduped union across 2 runs must yield 3 unique tickers'


# ---------------------------------------------------------------------------
# P1 — lifecycle-precedence dedup (terminal masking / blink fix)
# ---------------------------------------------------------------------------

def _seed_staged(db, run_id, sess, ticker, stage, cause=None) -> None:
  # Insert one row for `ticker` under its own run (simulates the per-cycle run_id
  # churn: same ticker, new run_id each call -> a new row with a higher rowid).
  _seed_run(db, run_id, sess)
  _seed_candidate(db, run_id, ticker, 'live_qualifying', lifecycle_stage=stage)
  if cause is not None:
    conn = open_database(db)
    conn.execute(
      "UPDATE candidate_review_candidates SET terminal_cause = ? WHERE run_id = ? AND ticker = ?",
      (cause, run_id, ticker),
    )
    conn.commit()


def test_p1_expired_terminal_wins_over_newer_discovered() -> None:
  # The core fix: an expired_unfilled terminal row must win over a newer churn-inserted
  # discovered row for the same ticker (otherwise the card never shows cancelled).
  db = _tmp_db()
  _seed_staged(db, 'run-old', 'sess-P1', 'KXEXP', 'terminal', cause='expired_unfilled')
  _seed_staged(db, 'run-new', 'sess-P1', 'KXEXP', 'discovered')  # newer rowid
  rows = _query_canonical_candidates('sess-P1', db)
  assert len(rows) == 1, 'one row per ticker'
  assert rows[0]['lifecycle_stage'] == 'terminal', 'expired terminal must win over a newer discovered row'


def test_p1_auto_cancel_beats_newer_rediscovery() -> None:
  # Lane D (2026-06-23): auto_cancel now has precedence over a newer re-discovered row
  # in the same session, consistent with expired_unfilled. Once a candidate has been
  # auto-cancelled in this session (including via the bridge-submit path), a scan
  # re-persist must not surface it as discovered again. This supersedes the original
  # P1 rationale which only excluded operator-halt reversibility for expired_unfilled.
  db = _tmp_db()
  _seed_staged(db, 'run-old', 'sess-P1', 'KXHALT', 'terminal', cause='auto_cancel')
  _seed_staged(db, 'run-new', 'sess-P1', 'KXHALT', 'discovered')  # newer rowid
  rows = _query_canonical_candidates('sess-P1', db)
  assert rows[0]['lifecycle_stage'] == 'terminal', (
    'Lane D: auto_cancel terminal must win over a newer discovered row '
    '(prevents zero-bid re-queue masking after bridge-submit auto_cancel)'
  )


def test_p1_auto_cancel_wins_when_newest() -> None:
  # Halted and NOT resumed: the auto_cancel terminal is the newest row -> cancelled.
  db = _tmp_db()
  _seed_staged(db, 'run-old', 'sess-P1', 'KXSTOP', 'discovered')
  _seed_staged(db, 'run-new', 'sess-P1', 'KXSTOP', 'terminal', cause='auto_cancel')  # newer rowid
  rows = _query_canonical_candidates('sess-P1', db)
  assert rows[0]['lifecycle_stage'] == 'terminal'
  assert rows[0]['terminal_cause'] == 'auto_cancel'


def test_p1_count_coherence_unchanged() -> None:
  # Dedup still yields exactly one row per distinct ticker regardless of stage mix.
  db = _tmp_db()
  _seed_staged(db, 'r1', 'sess-P1', 'KXA', 'terminal', cause='expired_unfilled')
  _seed_staged(db, 'r2', 'sess-P1', 'KXA', 'discovered')
  _seed_staged(db, 'r3', 'sess-P1', 'KXB', 'discovered')
  _seed_staged(db, 'r4', 'sess-P1', 'KXC', 'in_flight')
  rows = _query_canonical_candidates('sess-P1', db)
  assert {r['ticker'] for r in rows} == {'KXA', 'KXB', 'KXC'}
  assert len(rows) == 3, 'one row per distinct ticker; count unchanged by precedence'


def test_p1_execution_panel_shows_expired_as_cancelled() -> None:
  # _fetch_stage_columns consumes the same canonical query, so the expired ticker
  # must land in the cancelled column (not queued) once P1 unmasks the terminal row.
  db = _tmp_db()
  _seed_staged(db, 'run-old', 'sess-P1', 'KXEXP', 'terminal', cause='expired_unfilled')
  _seed_staged(db, 'run-new', 'sess-P1', 'KXEXP', 'discovered')
  result = _fetch_stage_columns(_payload(db, 'sess-P1'))
  cancelled = [c for c in result['stage_columns'] if c['stage_id'] == 'cancelled'][0]['items']
  queued = [c for c in result['stage_columns'] if c['stage_id'] == 'queued'][0]['items']
  assert any(item['ticker'] == 'KXEXP' for item in cancelled), 'expired ticker must show in cancelled'
  assert not any(item['ticker'] == 'KXEXP' for item in queued), 'expired ticker must not remain queued'


def test_stage_columns_agree_with_canonical_count() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-J')
  for i in range(4):
    _seed_candidate(db, 'run1', f'KXTICKER{i}', 'live_qualifying')
  _seed_candidate(db, 'run1', 'KXNM', 'near_miss')
  canonical_count = len(_query_canonical_candidates('sess-J', db))
  payload = _payload(db, 'sess-J')
  result = _fetch_stage_columns(payload)
  assert result['in_flight_candidate_count'] == canonical_count == 4


# ---------------------------------------------------------------------------
# Lane B — three-deadline transition engine (reads Lane A deadline columns)
# ---------------------------------------------------------------------------

_NOW = datetime(2026, 6, 19, 23, 0, 0, tzinfo=timezone.utc)


def _seed_candidate_with_close(db, run_id, ticker, close_time_utc, lifecycle_stage='discovered') -> None:
  _seed_candidate(
    db, run_id, ticker, 'live_qualifying', lifecycle_stage=lifecycle_stage,
    detail={'event_ticker': ticker, 'yes_sub_title': ticker, 'close_time_utc': close_time_utc},
  )


def test_transition_view_lapse_before_close() -> None:
  # close is still 30s in the FUTURE, but the view deadline (close - 75s) has passed:
  # the candidate must leave the selection surface BEFORE the market closes (#1).
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-K')
  _seed_candidate_with_close(db, 'run1', 'KXVIEW', '2026-06-19T23:00:30Z')
  conn = open_database(db)
  _mark_expired_candidates_terminal(conn, operation_lane='live', operator_lane_session_id='sess-K', recorded_at=_NOW)
  row = conn.execute(
    "SELECT lifecycle_stage, terminal_cause, terminal_subcause FROM candidate_review_candidates WHERE ticker = 'KXVIEW'",
  ).fetchone()
  assert row[0] == 'terminal'
  assert row[1] == 'expired_unfilled'
  assert row[2] == 'view_window_lapsed', 'view-window lapse must fire before market close'


def test_transition_market_close_backstop_for_in_flight() -> None:
  # An in_flight candidate is outside the view-lapse scope; the market-close backstop
  # (#3) catches it once close has passed, with the market_closed subcause.
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-K')
  _seed_candidate_with_close(db, 'run1', 'KXFLIGHT', '2026-06-19T22:00:00Z', lifecycle_stage='in_flight')
  conn = open_database(db)
  _mark_expired_candidates_terminal(conn, operation_lane='live', operator_lane_session_id='sess-K', recorded_at=_NOW)
  row = conn.execute(
    "SELECT lifecycle_stage, terminal_cause, terminal_subcause FROM candidate_review_candidates WHERE ticker = 'KXFLIGHT'",
  ).fetchone()
  assert row[0] == 'terminal'
  assert row[1] == 'expired_unfilled'
  assert row[2] == 'market_closed', 'in_flight past close must be caught by the market-close backstop'


def test_transition_ignores_future_deadlines() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-L')
  _seed_candidate_with_close(db, 'run1', 'KXLIVE', '2099-01-01T00:00:00Z')
  conn = open_database(db)
  _mark_expired_candidates_terminal(conn, operation_lane='live', operator_lane_session_id='sess-L', recorded_at=_NOW)
  row = conn.execute("SELECT lifecycle_stage FROM candidate_review_candidates WHERE ticker = 'KXLIVE'").fetchone()
  assert row[0] == 'discovered', 'candidate whose deadlines are all in the future must not transition'


def test_transition_ignores_null_deadlines() -> None:
  # No close_time at discovery -> null deadlines (Lane A fail-closed) -> never transitions.
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-N')
  _seed_candidate(db, 'run1', 'KXNULL', 'live_qualifying', lifecycle_stage='discovered')
  conn = open_database(db)
  _mark_expired_candidates_terminal(conn, operation_lane='live', operator_lane_session_id='sess-N', recorded_at=_NOW)
  row = conn.execute("SELECT lifecycle_stage FROM candidate_review_candidates WHERE ticker = 'KXNULL'").fetchone()
  assert row[0] == 'discovered', 'a candidate with null deadlines must never be transitioned'


def _seed_stop_transition(db, sess, recorded_at_utc, reason='operator_stop', state='stopped') -> None:
  conn = open_database(db)
  persist_runtime_event(
    conn, level='INFO', event_type='automation_policy_transition',
    recorded_at_utc=recorded_at_utc, operation_lane='live', lane_session_id=sess,
    detail={'automation_state_id': state, 'transition_reason': reason},
  )


def test_stop2_fence_no_transition_does_not_skip() -> None:
  db = _tmp_db()
  cycle = datetime(2026, 6, 20, 23, 0, 0, tzinfo=timezone.utc)
  assert _lane_session_stopped_after(open_database(db), lane_session_id='s', operation_lane='live', cycle_recorded_at=cycle) is False


def test_stop2_fence_skips_straggler() -> None:
  # cycle started 23:00:00; operator stop landed AFTER at 23:00:30 -> straggler -> skip.
  db = _tmp_db()
  _seed_stop_transition(db, 's', '2026-06-20T23:00:30Z')
  cycle = datetime(2026, 6, 20, 23, 0, 0, tzinfo=timezone.utc)
  assert _lane_session_stopped_after(open_database(db), lane_session_id='s', operation_lane='live', cycle_recorded_at=cycle) is True


def test_stop2_fence_allows_manual_run_after_stop() -> None:
  # operator stopped at 23:00:30; a MANUAL run cycle starts later at 23:02:00 -> NOT a
  # straggler (its cycle time is after the stop) -> must NOT skip.
  db = _tmp_db()
  _seed_stop_transition(db, 's', '2026-06-20T23:00:30Z')
  manual_cycle = datetime(2026, 6, 20, 23, 2, 0, tzinfo=timezone.utc)
  assert _lane_session_stopped_after(open_database(db), lane_session_id='s', operation_lane='live', cycle_recorded_at=manual_cycle) is False


def test_stop2_fence_resume_clears() -> None:
  # stop then a later resume -> latest transition is a start -> do not skip.
  db = _tmp_db()
  _seed_stop_transition(db, 's', '2026-06-20T23:00:30Z')
  _seed_stop_transition(db, 's', '2026-06-20T23:01:00Z', reason='operator_start', state='active')
  cycle = datetime(2026, 6, 20, 23, 0, 0, tzinfo=timezone.utc)
  assert _lane_session_stopped_after(open_database(db), lane_session_id='s', operation_lane='live', cycle_recorded_at=cycle) is False


def test_p2b2_view_deadline_decision() -> None:
  # P2-B2 excludes a candidate from the snapshot persist when its view-selection
  # deadline is past; the decision is compute_candidate_deadlines(close).view_expires_at_utc
  # <= now, and it fails OPEN (include) when the close time is absent/unparseable.
  now = '2026-06-20T00:00:00Z'
  past = compute_candidate_deadlines('2020-01-01T00:00:00Z')
  future = compute_candidate_deadlines('2099-01-01T00:00:00Z')
  assert past is not None and past['view_expires_at_utc'] <= now      # excluded by B2
  assert future is not None and future['view_expires_at_utc'] > now   # included by B2
  assert compute_candidate_deadlines(None) is None                    # fail open -> included


def test_expiry_engine_idempotent_on_repeat_call() -> None:
  # T1 runs the engine on every heartbeat (~2s); repeated calls must be a safe no-op
  # (only non-terminal past-deadline rows are touched), so it can run standalone.
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-T1')
  _seed_candidate_with_close(db, 'run1', 'KXIDEM', '2026-06-19T23:00:30Z')  # view deadline past at _NOW
  conn = open_database(db)
  for _ in range(3):
    _mark_expired_candidates_terminal(conn, operation_lane='live', operator_lane_session_id='sess-T1', recorded_at=_NOW)
  row = conn.execute(
    "SELECT lifecycle_stage, terminal_cause, terminal_subcause FROM candidate_review_candidates WHERE ticker = 'KXIDEM'",
  ).fetchone()
  assert row[0] == 'terminal' and row[1] == 'expired_unfilled' and row[2] == 'view_window_lapsed'


def test_transition_ignores_other_session() -> None:
  db = _tmp_db()
  _seed_run(db, 'run-other', 'sess-OTHER')
  _seed_candidate_with_close(db, 'run-other', 'KXOTHER', '2020-01-01T00:00:00Z')
  conn = open_database(db)
  _mark_expired_candidates_terminal(conn, operation_lane='live', operator_lane_session_id='sess-ACTIVE', recorded_at=_NOW)
  row = conn.execute("SELECT lifecycle_stage FROM candidate_review_candidates WHERE ticker = 'KXOTHER'").fetchone()
  assert row[0] == 'discovered', 'candidate from a different session must not be transitioned'


# ---------------------------------------------------------------------------
# Lane A — discovery-time expiry clock
# ---------------------------------------------------------------------------

def test_compute_deadlines_seed_buffers() -> None:
  out = compute_candidate_deadlines('2026-06-20T01:00:00Z')
  assert out is not None
  assert out['market_close_at_utc'] == '2026-06-20T01:00:00Z'
  # close - submit(10s) ; close - view(75s)
  assert out['submit_expires_at_utc'] == '2026-06-20T00:59:50Z'
  assert out['view_expires_at_utc'] == '2026-06-20T00:58:45Z'


def test_compute_deadlines_custom_buffers() -> None:
  out = compute_candidate_deadlines('2026-06-20T01:00:00Z', {'view': 120, 'submit': 30})
  assert out is not None
  assert out['submit_expires_at_utc'] == '2026-06-20T00:59:30Z'
  assert out['view_expires_at_utc'] == '2026-06-20T00:58:00Z'


def test_compute_deadlines_fail_closed_on_missing_close() -> None:
  assert compute_candidate_deadlines(None) is None
  assert compute_candidate_deadlines('') is None
  assert compute_candidate_deadlines('not-a-timestamp') is None


def test_compute_deadlines_fail_closed_on_invariant_violation() -> None:
  # view_buffer < submit_buffer would let a candidate be submittable after it left
  # the selection surface — must fail closed (write no deadlines).
  assert compute_candidate_deadlines('2026-06-20T01:00:00Z', {'view': 5, 'submit': 10}) is None


def test_compute_deadlines_seed_constants_satisfy_invariant() -> None:
  assert SEED_VIEW_BUFFER_SEC >= SEED_SUBMIT_BUFFER_SEC >= 0


def test_persist_writes_deadline_columns_from_close_time() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-DL')
  _seed_candidate(db, 'run1', 'KXCLK', 'live_qualifying', detail={
    'yes_sub_title': 'clock', 'event_ticker': 'KXCLK', 'close_time_utc': '2026-06-20T01:00:00Z',
  })
  conn = open_database(db)
  row = conn.execute(
    "SELECT market_close_at_utc, view_expires_at_utc, submit_expires_at_utc"
    " FROM candidate_review_candidates WHERE ticker = 'KXCLK'",
  ).fetchone()
  assert row[0] == '2026-06-20T01:00:00Z'
  assert row[1] == '2026-06-20T00:58:45Z'
  assert row[2] == '2026-06-20T00:59:50Z'


def test_persist_null_deadlines_when_no_close_time() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-DL2')
  _seed_candidate(db, 'run1', 'KXNOCLK', 'live_qualifying', detail={
    'yes_sub_title': 'no clock', 'event_ticker': 'KXNOCLK',
  })
  conn = open_database(db)
  row = conn.execute(
    "SELECT market_close_at_utc, view_expires_at_utc, submit_expires_at_utc"
    " FROM candidate_review_candidates WHERE ticker = 'KXNOCLK'",
  ).fetchone()
  assert row[0] is None and row[1] is None and row[2] is None


def test_persist_preserves_deadlines_on_conflict() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-DL3')
  conn = open_database(db)
  base = {
    'candidate_uid': 'KXSTAMP::live_qualifying', 'candidate_key': 'KXSTAMP::live_qualifying',
    'ticker': 'KXSTAMP', 'qualifier_tier': 'live_qualifying', 'review_row_origin': 'current',
    'event_ticker': 'KXSTAMP', 'close_time_utc': '2026-06-20T01:00:00Z',
  }
  # First discovery stamps deadlines with the seed buffers.
  persist_candidate_review_candidates(
    conn, run_id='run1', recorded_at_utc='2026-06-20T00:00:00Z',
    operation_lane='live', candidates=[dict(base)],
  )
  # Re-persist the SAME candidate in the SAME run with DIFFERENT buffers — deadlines
  # must be preserved (stamped once at first discovery), not recomputed.
  persist_candidate_review_candidates(
    conn, run_id='run1', recorded_at_utc='2026-06-20T00:01:00Z',
    operation_lane='live', candidates=[dict(base)],
    effective_buffers={'view': 600, 'submit': 300},
  )
  row = conn.execute(
    "SELECT view_expires_at_utc, submit_expires_at_utc"
    " FROM candidate_review_candidates WHERE ticker = 'KXSTAMP'",
  ).fetchone()
  assert row[0] == '2026-06-20T00:58:45Z', 'view deadline must be the first-stamped seed value'
  assert row[1] == '2026-06-20T00:59:50Z', 'submit deadline must be the first-stamped seed value'


# ---------------------------------------------------------------------------
# S1 — retry-wedge release (Stage-2 BMAP). The zero-found-retry stalls ("no
# countdown") because the gate counted the canonical set, which keeps terminal
# (expired/cancelled) cards; the gate must read the non-terminal ACTIONABLE count.
# A locked saved selection whose candidates all expired must also release.
# ---------------------------------------------------------------------------

def test_s1_active_count_excludes_terminal_candidates() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-S1')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying', lifecycle_stage='discovered')
  _seed_candidate(db, 'run1', 'KXETH', 'live_qualifying', lifecycle_stage='terminal')
  rows = _query_canonical_candidates('sess-S1', db)
  active = sum(1 for r in rows if r['lifecycle_stage'] != 'terminal')
  assert len(rows) == 2, 'canonical set keeps terminal cards so card/count coherence holds'
  assert active == 1, 'S1: the actionable count must exclude terminal candidates'


def test_s1_all_terminal_yields_zero_active_count_and_cancelled_columns() -> None:
  db = _tmp_db()
  _seed_run(db, 'run1', 'sess-S1b')
  _seed_candidate(db, 'run1', 'KXBTC', 'live_qualifying', lifecycle_stage='terminal')
  _seed_candidate(db, 'run1', 'KXETH', 'live_qualifying', lifecycle_stage='terminal')
  rows = _query_canonical_candidates('sess-S1b', db)
  active = sum(1 for r in rows if r['lifecycle_stage'] != 'terminal')
  assert active == 0, 'S1: all-terminal -> 0 actionable -> the zero-found-retry must resume'
  # And the execution panel renders them CANCELLED, not stale QUEUED.
  cols = _fetch_stage_columns(_payload(db, 'sess-S1b'))
  stage_by_id = {c['stage_id']: c['items'] for c in cols['stage_columns']}
  assert len(stage_by_id['queued']) == 0, 'S1: no terminal candidate may show as queued'
  assert len(stage_by_id['cancelled']) == 2, 'S1: terminal candidates render in the cancelled column'


def test_s1_wiring_present_in_web_app_source() -> None:
  src_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'src', 'polyventure', 'web_app.py')
  with open(src_path, 'r', encoding='utf-8') as handle:
    text = handle.read()
  # S1a: server seeds + overrides the actionable count; the retry gate reads it (full-count fallback).
  assert 'result_active_candidate_count' in text, 'S1a: server must publish the actionable count'
  assert "if str(_r.get('lifecycle_stage') or '') != 'terminal'" in text, 'S1a: override must count non-terminal rows'
  assert 'runtime.result_active_candidate_count ?? runtime.result_candidate_count' in text, 'S1a: retry gate must read the actionable count'
  # S1b: the all-expired release branch transitions to the unlocked expired state.
  assert '_all_saved_expired' in text, 'S1b: all-expired detector must exist'
  assert "'review_hold_saved_set_expired'" in text, 'S1b: release must transition to the expired state'


# ---------------------------------------------------------------------------
# STOP-3 SB: reconcile prior-session orphaned in_flight (abrupt-close backstop).
# Fail-closed: preserve any in_flight that may carry a live order, and the active session.
# ---------------------------------------------------------------------------

def test_sb_reconcile_orphaned_in_flight_fail_closed() -> None:
  from polyventure.service import _reconcile_orphaned_in_flight
  db = _tmp_db()
  _seed_run(db, 'run-old', 'sess-old')
  _seed_run(db, 'run-active', 'sess-active')
  _seed_candidate(db, 'run-old', 'TINERT', 'live_qualifying', lifecycle_stage='in_flight')   # no live pair -> reconcile
  _seed_candidate(db, 'run-old', 'TLIVE', 'live_qualifying', lifecycle_stage='in_flight')     # live pair -> preserve
  _seed_candidate(db, 'run-active', 'TACTIVE', 'live_qualifying', lifecycle_stage='in_flight') # active session -> untouched

  pairs = [{'ticker': 'TLIVE', 'state': 'SUBMITTING'}]  # only TLIVE carries a live/open order
  n = _reconcile_orphaned_in_flight(
    open_database(db),
    current_operating_session_id='sess-active',
    pairs=pairs,
    recorded_at_utc='2026-06-20T10:00:00Z',
    operation_lane='live',
  )
  assert n == 1, 'SB: only the inert prior-session orphan is reconciled'

  def _state(ticker: str) -> tuple[str, str]:
    row = open_database(db).execute(
      'SELECT lifecycle_stage, terminal_cause FROM candidate_review_candidates WHERE ticker = ?',
      (ticker,),
    ).fetchone()
    return str(row[0] or ''), str(row[1] or '')

  assert _state('TINERT') == ('terminal', 'orphaned_teardown_reconciled'), 'SB: inert orphan -> distinct cause'
  assert _state('TLIVE')[0] == 'in_flight', 'SB: live-pair orphan PRESERVED (fail-closed)'
  assert _state('TACTIVE')[0] == 'in_flight', 'SB: active-session in_flight untouched'

  # No active session -> no-op (fail-safe).
  assert _reconcile_orphaned_in_flight(
    open_database(db), current_operating_session_id='', pairs=[], recorded_at_utc='x', operation_lane='live',
  ) == 0


# ---------------------------------------------------------------------------
# D2: auto_cancel lifecycle precedence (Lane D, 2026-06-23)
# ---------------------------------------------------------------------------

def _seed_candidate_with_cause(
  db_path: str,
  run_id: str,
  ticker: str,
  lifecycle_stage: str,
  terminal_cause: str | None,
) -> None:
  uid = f'{ticker}::{run_id}'
  conn = open_database(db_path)
  persist_candidate_review_candidates(
    conn,
    run_id=run_id,
    recorded_at_utc='2026-06-23T13:00:00Z',
    operation_lane='live',
    candidates=[{
      'candidate_uid': uid,
      'candidate_key': uid,
      'ticker': ticker,
      'qualifier_tier': 'live_qualifying',
      'review_row_origin': 'current',
      'yes_sub_title': ticker,
      'event_ticker': ticker,
    }],
  )
  if lifecycle_stage != 'discovered':
    conn.execute(
      'UPDATE candidate_review_candidates SET lifecycle_stage = ?, terminal_cause = ? WHERE candidate_uid = ? AND run_id = ?',
      (lifecycle_stage, terminal_cause, uid, run_id),
    )
    conn.commit()


def test_d2_auto_cancel_wins_over_newer_discovered() -> None:
  db = _tmp_db()
  _seed_run(db, 'run-old', 'sess-D2')
  _seed_run(db, 'run-new', 'sess-D2')
  # Older row: auto_cancel terminal
  _seed_candidate_with_cause(db, 'run-old', 'KXMVE-AC', 'terminal', 'auto_cancel')
  # Newer row: discovered (scan re-persist)
  _seed_candidate_with_cause(db, 'run-new', 'KXMVE-AC', 'discovered', None)

  rows = _query_canonical_candidates('sess-D2', db)
  ac_rows = [r for r in rows if r['ticker'] == 'KXMVE-AC']
  assert len(ac_rows) == 1, 'D2: dedup must produce exactly one row per ticker'
  assert ac_rows[0]['lifecycle_stage'] == 'terminal', 'D2: auto_cancel terminal must win over newer discovered row'
  assert ac_rows[0]['terminal_cause'] == 'auto_cancel', 'D2: terminal_cause must be auto_cancel'


def test_d2_expired_unfilled_precedence_preserved() -> None:
  db = _tmp_db()
  _seed_run(db, 'run-old', 'sess-D2b')
  _seed_run(db, 'run-new', 'sess-D2b')
  _seed_candidate_with_cause(db, 'run-old', 'KXMVE-EU', 'terminal', 'expired_unfilled')
  _seed_candidate_with_cause(db, 'run-new', 'KXMVE-EU', 'discovered', None)

  rows = _query_canonical_candidates('sess-D2b', db)
  eu_rows = [r for r in rows if r['ticker'] == 'KXMVE-EU']
  assert len(eu_rows) == 1
  assert eu_rows[0]['terminal_cause'] == 'expired_unfilled', 'D2: expired_unfilled precedence must be preserved'


def test_d2_discovered_wins_when_no_terminal() -> None:
  db = _tmp_db()
  _seed_run(db, 'run-1', 'sess-D2c')
  _seed_run(db, 'run-2', 'sess-D2c')
  _seed_candidate_with_cause(db, 'run-1', 'KXMVE-OPEN', 'discovered', None)
  _seed_candidate_with_cause(db, 'run-2', 'KXMVE-OPEN', 'discovered', None)

  rows = _query_canonical_candidates('sess-D2c', db)
  open_rows = [r for r in rows if r['ticker'] == 'KXMVE-OPEN']
  assert len(open_rows) == 1, 'D2: still deduped to one row'
  assert open_rows[0]['lifecycle_stage'] == 'discovered', 'D2: newest discovered wins when no terminal exists'
