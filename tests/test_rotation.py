from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import json
import sqlite3

import pytest

from polyventure.persistence import (
  archive_discrete_tables,
  consolidate_heartbeats,
  initialize_database,
  open_database,
  persist_service_heartbeat,
  rotate_database,
)
from polyventure.service import _heartbeat_balance_at


UTC = timezone.utc


def _db(tmp_path):
  """Open a fresh isolated database."""
  return open_database(tmp_path / 'test.sqlite3')


def _hb(connection, *, lane, ts, balance=None, status='ok'):
  """Insert a service_heartbeat row."""
  detail: dict = {}
  if balance is not None:
    detail['funds_refresh_status'] = 'fresh'
    detail['available_funds_snapshot'] = str(balance)
  persist_service_heartbeat(
    connection,
    component='ws',
    status=status,
    recorded_at_utc=ts,
    operation_lane=lane,
    detail=detail,
  )


def _ts(now, *, days=0, hours=0, minutes=0):
  """Return ISO string offset from now."""
  return (now - timedelta(days=days, hours=hours, minutes=minutes)).astimezone(UTC).isoformat()


# ---------------------------------------------------------------------------
# R1: correct tier assignment
# ---------------------------------------------------------------------------

def test_daily_tier_assigned_for_rows_older_than_7d(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  old_ts = _ts(now, days=8)
  _hb(conn, lane='sandbox', ts=old_ts, balance='42.00')

  result = consolidate_heartbeats(conn, now_utc=now)

  assert result['daily_buckets_written'] == 1
  assert result['hourly_buckets_written'] == 0
  assert result['raw_rows_removed'] == 1

  raw = conn.execute('SELECT COUNT(*) FROM service_heartbeats').fetchone()[0]
  assert raw == 0

  daily = conn.execute(
    "SELECT * FROM service_heartbeats_consolidated WHERE tier='daily'"
  ).fetchall()
  assert len(daily) == 1
  assert daily[0]['operation_lane'] == 'sandbox'


def test_hourly_tier_assigned_for_rows_24h_to_7d(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  mid_ts = _ts(now, hours=26)
  _hb(conn, lane='sandbox', ts=mid_ts, balance='50.00')

  result = consolidate_heartbeats(conn, now_utc=now)

  assert result['hourly_buckets_written'] == 1
  assert result['daily_buckets_written'] == 0
  assert result['raw_rows_removed'] == 1

  hourly = conn.execute(
    "SELECT * FROM service_heartbeats_consolidated WHERE tier='hourly'"
  ).fetchall()
  assert len(hourly) == 1


# ---------------------------------------------------------------------------
# R2 / R3: correct latest_balance_snapshot
# ---------------------------------------------------------------------------

def test_hourly_bucket_holds_latest_fresh_balance(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  # Two fresh heartbeats in the same hour, 26h and 26h+1m ago
  ts1 = _ts(now, hours=26, minutes=1)
  ts2 = _ts(now, hours=26)
  _hb(conn, lane='sandbox', ts=ts1, balance='10.00')
  _hb(conn, lane='sandbox', ts=ts2, balance='20.00')  # later = higher balance

  consolidate_heartbeats(conn, now_utc=now)

  row = conn.execute(
    "SELECT latest_balance_snapshot FROM service_heartbeats_consolidated WHERE tier='hourly'"
  ).fetchone()
  assert row is not None
  assert Decimal(row['latest_balance_snapshot']) == Decimal('20.00')


def test_daily_bucket_holds_latest_fresh_balance(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  ts1 = _ts(now, days=8, hours=1)
  ts2 = _ts(now, days=8)
  _hb(conn, lane='sandbox', ts=ts1, balance='5.00')
  _hb(conn, lane='sandbox', ts=ts2, balance='15.00')

  consolidate_heartbeats(conn, now_utc=now)

  row = conn.execute(
    "SELECT latest_balance_snapshot FROM service_heartbeats_consolidated WHERE tier='daily'"
  ).fetchone()
  assert row is not None
  assert Decimal(row['latest_balance_snapshot']) == Decimal('15.00')


# ---------------------------------------------------------------------------
# R4: 1-hour safety floor
# ---------------------------------------------------------------------------

def test_safety_floor_prevents_consolidation_of_recent_rows(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  # Row within 1h of now — should never be touched regardless of tier boundary
  recent_ts = _ts(now, minutes=30)
  _hb(conn, lane='sandbox', ts=recent_ts, balance='99.00')

  result = consolidate_heartbeats(conn, now_utc=now)

  assert result['raw_rows_removed'] == 0
  raw = conn.execute('SELECT COUNT(*) FROM service_heartbeats').fetchone()[0]
  assert raw == 1


def test_safety_floor_with_injected_now(tmp_path):
  # Inject a future now_utc so the 'old' row actually qualifies, but also
  # verify the floor is 1h before that now — not before wall-clock now.
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  injected_now = now + timedelta(days=10)  # pretend it's 10 days later
  old_ts = _ts(now, days=0)  # 'now' is 10d old relative to injected_now

  _hb(conn, lane='sandbox', ts=old_ts, balance='1.00')

  result = consolidate_heartbeats(conn, now_utc=injected_now)
  # Row is 10 days old relative to injected_now → daily tier
  assert result['daily_buckets_written'] == 1
  assert result['raw_rows_removed'] == 1


# ---------------------------------------------------------------------------
# R5: financial tables untouched
# ---------------------------------------------------------------------------

def test_financial_tables_untouched_by_rotate(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)

  # Insert a pair_plan row directly
  with conn:
    conn.execute(
      'INSERT INTO pair_plans (pair_id, ticker, yes_price_dollars, no_price_dollars, '
      'contract_count, yes_client_order_id, no_client_order_id, time_in_force, '
      'post_only, cancel_order_on_pause, subaccount, operation_lane, created_at_utc) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      ('p1', 'T', '0.3', '0.4', '5', 'y1', 'n1', 'GTC', 0, 0, 0, 'sandbox', now.isoformat()),
    )
    # Insert a pair_pnl_snapshots row
    conn.execute(
      'INSERT INTO pair_pnl_snapshots (pair_id, locked_contracts, gross_dollars, '
      'net_projected_dollars, net_realized_dollars, operation_lane, recorded_at_utc) '
      'VALUES (?, ?, ?, ?, ?, ?, ?)',
      ('p1', '5', '100.00', '95.00', '0.00', 'sandbox', _ts(now, days=60)),
    )

  rotate_database(conn, now_utc=now, archive_db_path=tmp_path / 'archive.sqlite3')

  assert conn.execute('SELECT COUNT(*) FROM pair_plans').fetchone()[0] == 1
  assert conn.execute('SELECT COUNT(*) FROM pair_pnl_snapshots').fetchone()[0] == 1


# ---------------------------------------------------------------------------
# R6: saved sets untouched
# ---------------------------------------------------------------------------

def test_saved_sets_untouched_by_rotate(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)

  with conn:
    conn.execute(
      'INSERT INTO candidate_saved_sets (saved_set_id, operation_lane, saved_key_count, '
      'state_id, source_action, detail_json, recorded_at_utc, profile_token) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      ('ss1', 'sandbox', 3, 'sid', 'manual', '{}', _ts(now, days=60), 'tok1'),
    )

  rotate_database(conn, now_utc=now, archive_db_path=tmp_path / 'archive.sqlite3')

  assert conn.execute('SELECT COUNT(*) FROM candidate_saved_sets').fetchone()[0] == 1


# ---------------------------------------------------------------------------
# R7: lane_active_datapack untouched
# ---------------------------------------------------------------------------

def test_lane_active_datapack_untouched_by_rotate(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)

  with conn:
    conn.execute(
      'INSERT INTO lane_active_datapack (operation_lane, profile_token, became_active_at_utc, '
      'mint_basis, detail_json, recorded_at_utc) '
      'VALUES (?, ?, ?, ?, ?, ?)',
      ('sandbox', 'tok1', now.isoformat(), 'key_path_derived', '{}', now.isoformat()),
    )

  rotate_database(conn, now_utc=now, archive_db_path=tmp_path / 'archive.sqlite3')

  assert conn.execute('SELECT COUNT(*) FROM lane_active_datapack').fetchone()[0] == 1


# ---------------------------------------------------------------------------
# R8: idempotency
# ---------------------------------------------------------------------------

def test_rotate_database_idempotent(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  old_ts = _ts(now, days=8)
  _hb(conn, lane='sandbox', ts=old_ts, balance='30.00')

  r1 = consolidate_heartbeats(conn, now_utc=now)
  r2 = consolidate_heartbeats(conn, now_utc=now)

  assert r1['raw_rows_removed'] == 1
  assert r2['raw_rows_removed'] == 0
  assert r2['daily_buckets_written'] == 0


def test_archive_idempotent(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  old_ts = _ts(now, days=35)  # runtime_events threshold is 30d; use 35d to qualify

  with conn:
    conn.execute(
      'INSERT INTO runtime_events (level, event_type, operation_lane, detail_json, recorded_at_utc, profile_token) '
      'VALUES (?, ?, ?, ?, ?, ?)',
      ('info', 'scan', 'sandbox', '{}', old_ts, 'tok'),
    )

  archive_path = tmp_path / 'archive.sqlite3'
  r1 = archive_discrete_tables(conn, now_utc=now, archive_db_path=archive_path)
  r2 = archive_discrete_tables(conn, now_utc=now, archive_db_path=archive_path)

  assert r1.get('runtime_events', 0) == 1
  assert r2.get('runtime_events', 0) == 0


# ---------------------------------------------------------------------------
# R10: _heartbeat_balance_at — Step 1 (raw) hit
# ---------------------------------------------------------------------------

def test_heartbeat_balance_at_raw_tier(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  recent_ts = _ts(now, minutes=30)
  _hb(conn, lane='sandbox', ts=recent_ts, balance='55.50')

  result = _heartbeat_balance_at(conn, operation_lane='sandbox', at_utc=now)
  assert result == Decimal('55.50')


# ---------------------------------------------------------------------------
# R11: _heartbeat_balance_at — Step 2 (hourly) fallthrough
# ---------------------------------------------------------------------------

def test_heartbeat_balance_at_hourly_fallthrough(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  # Insert a row that will be hourly-consolidated (26h old)
  mid_ts = _ts(now, hours=26)
  _hb(conn, lane='sandbox', ts=mid_ts, balance='77.77')

  # Consolidate so the raw row moves to the hourly tier
  consolidate_heartbeats(conn, now_utc=now)

  # Raw table should now be empty for this lane
  raw = conn.execute(
    'SELECT COUNT(*) FROM service_heartbeats WHERE operation_lane=?', ('sandbox',)
  ).fetchone()[0]
  assert raw == 0

  # Query for a timestamp after mid_ts — should fall through to hourly tier
  query_at = datetime.fromisoformat(mid_ts) + timedelta(minutes=5)
  result = _heartbeat_balance_at(conn, operation_lane='sandbox', at_utc=query_at)
  assert result == Decimal('77.77')


# ---------------------------------------------------------------------------
# R12: _heartbeat_balance_at — Step 3 (daily) fallthrough
# ---------------------------------------------------------------------------

def test_heartbeat_balance_at_daily_fallthrough(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  old_ts = _ts(now, days=8)
  _hb(conn, lane='sandbox', ts=old_ts, balance='88.88')

  consolidate_heartbeats(conn, now_utc=now)

  raw = conn.execute('SELECT COUNT(*) FROM service_heartbeats WHERE operation_lane=?', ('sandbox',)).fetchone()[0]
  assert raw == 0

  query_at = datetime.fromisoformat(old_ts) + timedelta(minutes=5)
  result = _heartbeat_balance_at(conn, operation_lane='sandbox', at_utc=query_at)
  assert result == Decimal('88.88')


# ---------------------------------------------------------------------------
# R13: _heartbeat_balance_at — no data → Decimal('0')
# ---------------------------------------------------------------------------

def test_heartbeat_balance_at_empty_returns_zero(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  result = _heartbeat_balance_at(conn, operation_lane='sandbox', at_utc=now)
  assert result == Decimal('0')


# ---------------------------------------------------------------------------
# Track 2: archive_discrete_tables moves rows and respects FK guard
# ---------------------------------------------------------------------------

def test_archive_moves_old_candidate_rows(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  old_ts = _ts(now, days=8)

  # Insert a run then a candidate
  with conn:
    conn.execute(
      'INSERT INTO candidate_review_runs '
      '(run_id, operation_lane, candidate_signature, candidate_count, source_action, detail_json, recorded_at_utc, profile_token) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      ('r1', 'sandbox', 'sig', 1, 'scan', '{}', old_ts, 'tok'),
    )
    conn.execute(
      'INSERT INTO candidate_review_candidates '
      '(run_id, candidate_uid, candidate_key, detail_json, recorded_at_utc, profile_token, operation_lane) '
      "VALUES (?, ?, ?, ?, ?, ?, 'sandbox')",
      ('r1', 'cu1', 'ck1', '{}', old_ts, 'tok'),
    )

  archive_path = tmp_path / 'archive.sqlite3'
  result = archive_discrete_tables(conn, now_utc=now, archive_db_path=archive_path)

  assert result.get('candidate_review_candidates', 0) == 1
  hot = conn.execute('SELECT COUNT(*) FROM candidate_review_candidates').fetchone()[0]
  assert hot == 0


def test_archive_run_skipped_when_referenced_by_saved_set(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  old_ts = _ts(now, days=35)

  with conn:
    conn.execute(
      'INSERT INTO candidate_review_runs '
      '(run_id, operation_lane, candidate_signature, candidate_count, source_action, detail_json, recorded_at_utc, profile_token) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      ('r1', 'sandbox', 'sig', 1, 'scan', '{}', old_ts, 'tok'),
    )
    # Saved set references this run — must NOT be archived
    conn.execute(
      'INSERT INTO candidate_saved_sets '
      '(saved_set_id, run_id, operation_lane, saved_key_count, state_id, source_action, detail_json, recorded_at_utc, profile_token) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
      ('ss1', 'r1', 'sandbox', 0, 'sid', 'manual', '{}', now.isoformat(), 'tok'),
    )

  archive_path = tmp_path / 'archive.sqlite3'
  result = archive_discrete_tables(conn, now_utc=now, archive_db_path=archive_path)

  # Run should remain in hot DB because it's referenced by a saved set
  hot_runs = conn.execute('SELECT COUNT(*) FROM candidate_review_runs').fetchone()[0]
  assert hot_runs == 1
  assert result.get('candidate_review_runs', 0) == 0


# ---------------------------------------------------------------------------
# R14 (sandbox): archive DB has WAL mode + integrity_check passes
# ---------------------------------------------------------------------------

def test_r14_archive_db_wal_and_integrity(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  old_ts = _ts(now, days=35)

  with conn:
    conn.execute(
      'INSERT INTO runtime_events (level, event_type, operation_lane, detail_json, recorded_at_utc, profile_token) '
      'VALUES (?, ?, ?, ?, ?, ?)',
      ('info', 'scan', 'sandbox', '{}', old_ts, 'tok'),
    )

  archive_path = tmp_path / 'kalshi_archive.sqlite3'
  archive_discrete_tables(conn, now_utc=now, archive_db_path=archive_path)
  assert archive_path.exists(), 'archive DB file must be created'

  probe = sqlite3.connect(str(archive_path))
  try:
    jm = probe.execute('PRAGMA journal_mode').fetchone()[0]
    assert jm == 'wal', f'archive DB journal_mode: expected wal, got {jm!r}'
    ic = probe.execute('PRAGMA integrity_check').fetchone()[0]
    assert ic == 'ok', f'archive DB integrity_check: {ic!r}'
  finally:
    probe.close()


# ---------------------------------------------------------------------------
# R15 (sandbox): hot DB WAL active + integrity intact + datapack readable
# ---------------------------------------------------------------------------

def test_r15_hot_db_wal_and_integrity_post_rotation(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)
  _hb(conn, lane='sandbox', ts=_ts(now, days=8), balance='42.00')

  with conn:
    conn.execute(
      'INSERT INTO lane_active_datapack (operation_lane, profile_token, became_active_at_utc, '
      'mint_basis, detail_json, recorded_at_utc) VALUES (?, ?, ?, ?, ?, ?)',
      ('sandbox', 'tok1', now.isoformat(), 'key_path_derived', '{}', now.isoformat()),
    )

  rotate_database(conn, now_utc=now, archive_db_path=tmp_path / 'archive.sqlite3')

  jm = conn.execute('PRAGMA journal_mode').fetchone()[0]
  assert jm == 'wal', f'hot DB journal_mode: expected wal, got {jm!r}'

  ic = conn.execute('PRAGMA integrity_check').fetchone()[0]
  assert ic == 'ok', f'hot DB integrity_check: {ic!r}'

  row = conn.execute('SELECT operation_lane FROM lane_active_datapack').fetchone()
  assert row is not None, 'lane_active_datapack must be readable post-rotation'
  assert row['operation_lane'] == 'sandbox'


# ---------------------------------------------------------------------------
# R16 (sandbox): 50 hourly + 20 daily raw rows → ≤70 consolidated; raw empty
# ---------------------------------------------------------------------------

def test_r16_consolidation_bulk_row_count(tmp_path):
  conn = _db(tmp_path)
  now = datetime.now(UTC)

  for i in range(20):
    _hb(conn, lane='sandbox', ts=_ts(now, days=8 + i), balance=str(10 + i))

  for i in range(50):
    _hb(conn, lane='sandbox', ts=_ts(now, hours=26 + i), balance=str(50 + i))

  result = consolidate_heartbeats(conn, now_utc=now)

  assert result['daily_buckets_written'] == 20
  assert result['hourly_buckets_written'] == 50
  assert result['raw_rows_removed'] == 70

  consolidated_count = conn.execute(
    'SELECT COUNT(*) FROM service_heartbeats_consolidated'
  ).fetchone()[0]
  assert consolidated_count <= 70, f'Expected ≤70 consolidated rows, got {consolidated_count}'

  raw_count = conn.execute('SELECT COUNT(*) FROM service_heartbeats').fetchone()[0]
  assert raw_count == 0
