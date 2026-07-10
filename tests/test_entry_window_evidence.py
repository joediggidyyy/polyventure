"""Fixture coverage for the read-only C2 entry-window evidence query."""

from __future__ import annotations

import sqlite3

from polyventure.persistence import initialize_database, load_entry_window_evidence


def _db(path):
  conn = sqlite3.connect(path)
  conn.row_factory = sqlite3.Row
  initialize_database(conn)
  return conn


def _market(conn, ticker, close):
  conn.execute(
    'INSERT OR REPLACE INTO markets_seen (ticker, status, close_time_utc, last_seen_at_utc) VALUES (?,?,?,?)',
    (ticker, 'active', close, '2026-07-02T20:00:00+00:00'),
  )


def _pair(conn, pair_id, ticker, created, lane='live'):
  conn.execute(
    'INSERT INTO pair_plans (pair_id, ticker, yes_price_dollars, no_price_dollars, contract_count, '
    'yes_client_order_id, no_client_order_id, time_in_force, post_only, cancel_order_on_pause, '
    'subaccount, operation_lane, created_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)',
    (pair_id, ticker, '0.45', '0.55', '10', f'{pair_id}-y', f'{pair_id}-n', 'GTC', 1, 1, 0, lane, created),
  )


def _fill(conn, pair_id, side, count, lane='live'):
  conn.execute(
    'INSERT INTO fills (fill_id, pair_id, order_id, client_order_id, side, price_dollars, '
    'contract_count, fee_dollars, operation_lane, created_at_utc) VALUES (?,?,?,?,?,?,?,?,?,?)',
    (f'{pair_id}-{side}', pair_id, f'{pair_id}:{side}', f'{pair_id}-{side}', side, '0.45',
     str(count), '0.00', lane, '2026-07-02T21:02:00+00:00'),
  )


def _state(conn, pair_id, state, lane='live'):
  conn.execute(
    'INSERT INTO pair_states (pair_id, state, operation_lane, lane_session_id, detail_json, recorded_at_utc) '
    'VALUES (?,?,?,?,?,?)',
    (pair_id, state, lane, 's1', '{}', '2026-07-02T21:10:00+00:00'),
  )


def test_empty_when_no_pairs(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  assert load_entry_window_evidence(conn, 'live') == []


def test_timing_and_labels(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  # both-filled pair, 300s before close
  _market(conn, 'KXA', '2026-07-02T21:05:00+00:00')
  _pair(conn, 'p-clean', 'KXA', '2026-07-02T21:00:00+00:00')
  _fill(conn, 'p-clean', 'yes', 10)
  _fill(conn, 'p-clean', 'no', 10)
  _state(conn, 'p-clean', 'FILLED')
  # one-sided pair (only NO filled), plain SETTLED, 200s before close
  _market(conn, 'KXB', '2026-07-02T21:05:00+00:00')
  _pair(conn, 'p-exp', 'KXB', '2026-07-02T21:01:40+00:00')
  _fill(conn, 'p-exp', 'no', 10)
  _state(conn, 'p-exp', 'SETTLED')
  conn.commit()

  rows = load_entry_window_evidence(conn, 'live')
  by_secs = {r['seconds_to_close']: r['outcome_label'] for r in rows}
  assert by_secs[300] == 'both_filled_or_locked'
  assert by_secs[200] == 'one_sided_exposure'  # fill truth overrides plain SETTLED


def test_pair_without_close_time_excluded(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  # no markets_seen row for this ticker -> no close time -> excluded
  _pair(conn, 'p-x', 'KXNOCLOSE', '2026-07-02T21:00:00+00:00')
  _fill(conn, 'p-x', 'yes', 10)
  _fill(conn, 'p-x', 'no', 10)
  _state(conn, 'p-x', 'FILLED')
  conn.commit()
  assert load_entry_window_evidence(conn, 'live') == []


def test_lane_scoped(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  _market(conn, 'KXA', '2026-07-02T21:05:00+00:00')
  _pair(conn, 'p-sb', 'KXA', '2026-07-02T21:00:00+00:00', lane='sandbox')
  _fill(conn, 'p-sb', 'yes', 10, lane='sandbox')
  _fill(conn, 'p-sb', 'no', 10, lane='sandbox')
  _state(conn, 'p-sb', 'FILLED', lane='sandbox')
  conn.commit()
  assert load_entry_window_evidence(conn, 'live') == []
  assert len(load_entry_window_evidence(conn, 'sandbox')) == 1
