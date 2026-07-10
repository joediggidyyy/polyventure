"""C1 (sizing persistence) — money-code-level validation for the derived-value
persistence helpers.

Covers the parameter-optimization BMAP §7 register V1/V4 plus the money-code
validation contract §7.1: VL5 (no-clobber of operator working defaults) and VL1/VL4
(persist+prune timing budget, asserted so regression fails closed).
"""

from __future__ import annotations

import json
import sqlite3
import statistics
import time
from pathlib import Path

from polyventure.persistence import (
  DYNAMIC_SIZING_SNAPSHOT_TYPE,
  initialize_database,
  load_lane_defaults,
  load_latest_dynamic_sizing_snapshot,
  persist_dynamic_sizing_snapshot,
  persist_lane_defaults,
)

_SIZING_FIELDS = {
  'effective_density': '0.58',
  'dynamic_pair_notional_pct': '0.16',
  'dynamic_pair_notional_cap_dollars': '80.00',
  'dynamic_max_contracts': '9',
  'binding_limiter': 'dynamic_notional_cap',
}


def _db(path: Path) -> sqlite3.Connection:
  conn = sqlite3.connect(path)
  conn.row_factory = sqlite3.Row
  initialize_database(conn)
  return conn


def _count(conn, lane: str) -> int:
  return conn.execute(
    'SELECT COUNT(*) FROM analytical_snapshots WHERE operation_lane = ? AND snapshot_type = ?',
    (lane, DYNAMIC_SIZING_SNAPSHOT_TYPE),
  ).fetchone()[0]


# --- correctness -------------------------------------------------------------


def test_persist_then_load_latest_roundtrip(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  persist_dynamic_sizing_snapshot(conn, 'live', _SIZING_FIELDS, lane_session_id='sess-1')
  got = load_latest_dynamic_sizing_snapshot(conn, 'live')
  assert got is not None
  assert got['values'] == _SIZING_FIELDS
  assert got['source'] == 'computed:sizing'
  assert got['carried_over'] is True
  assert got['recorded_at_utc']


def test_load_latest_returns_newest(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  for i in range(3):
    persist_dynamic_sizing_snapshot(conn, 'live', {'dynamic_max_contracts': str(i)})
  got = load_latest_dynamic_sizing_snapshot(conn, 'live')
  assert got['values']['dynamic_max_contracts'] == '2'


def test_load_none_when_empty(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  assert load_latest_dynamic_sizing_snapshot(conn, 'live') is None


def test_lane_isolation(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  persist_dynamic_sizing_snapshot(conn, 'live', {'dynamic_max_contracts': '9'})
  assert load_latest_dynamic_sizing_snapshot(conn, 'sandbox') is None
  assert load_latest_dynamic_sizing_snapshot(conn, 'live')['values']['dynamic_max_contracts'] == '9'


# --- retention (keep-latest-5) ----------------------------------------------


def test_retention_keeps_latest_five(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  for i in range(8):
    persist_dynamic_sizing_snapshot(conn, 'live', {'dynamic_max_contracts': str(i)})
  assert _count(conn, 'live') == 5
  # newest is retained
  assert load_latest_dynamic_sizing_snapshot(conn, 'live')['values']['dynamic_max_contracts'] == '7'


# --- VL5: no-clobber of operator working defaults ---------------------------


def test_persist_does_not_clobber_operator_lane_defaults(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  # operator sets working defaults
  persist_lane_defaults(conn, 'live', {'min_edge_dollars': '0.05', 'max_open_pairs': '4'})
  before = load_lane_defaults(conn, 'live')
  # sizing persistence must not touch them
  for i in range(6):
    persist_dynamic_sizing_snapshot(conn, 'live', {'dynamic_max_contracts': str(i)})
  after = load_lane_defaults(conn, 'live')
  assert after == before == {'min_edge_dollars': '0.05', 'max_open_pairs': '4'}


# --- VL1/VL4: persist+prune timing budget (fails closed on regression) -------


def test_persist_prune_timing_budget(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  # warm-up (fill retention window)
  for _ in range(6):
    persist_dynamic_sizing_snapshot(conn, 'live', _SIZING_FIELDS)
  durations: list[float] = []
  for _ in range(40):
    start = time.perf_counter()
    persist_dynamic_sizing_snapshot(conn, 'live', _SIZING_FIELDS)
    durations.append(time.perf_counter() - start)
  median = statistics.median(durations)
  # Scan-cycle budget: persist+prune median well under 10 ms (BMAP §7.1 VL1).
  # Median is the stable guard (disk fsync variance makes a hard p99 flaky); a gross
  # regression (O(n) prune, heavy serialization) blows past this immediately.
  assert median < 0.010, f'persist+prune median {median * 1000:.2f} ms exceeds 10 ms budget'


# --- rehydrate read path (C1 §10.1): sizing posture carries over -------------


def test_rehydrate_sizing_from_snapshot_when_runtime_empty(tmp_path):
  from polyventure import service

  conn = _db(tmp_path / 'db.sqlite3')
  # no runtime_events/heartbeats carry sizing -> cold-start would be empty
  persist_dynamic_sizing_snapshot(conn, 'live', _SIZING_FIELDS, lane_session_id='s1')
  posture = service._load_latest_sizing_posture(conn, operation_lane='live')
  assert posture.get('effective_density') == '0.58'
  assert posture.get('dynamic_max_contracts') == '9'
  assert posture.get('binding_limiter') == 'dynamic_notional_cap'
  # carried-over provenance is surfaced so the UI can flag it
  assert posture.get('source_name') == 'computed:sizing_last_ready'


def test_rehydrate_absent_when_no_snapshot(tmp_path):
  from polyventure import service

  conn = _db(tmp_path / 'db.sqlite3')
  posture = service._load_latest_sizing_posture(conn, operation_lane='live')
  # nothing persisted and no runtime sizing -> empty, not fabricated
  assert posture.get('effective_density') is None


def test_live_runtime_sizing_wins_over_carried_snapshot(tmp_path):
  """At-risk adjacent: a live scan's runtime sizing must never be shadowed by a
  carried-over snapshot. The snapshot is appended as a gap-filling source only."""
  from polyventure import service

  conn = _db(tmp_path / 'db.sqlite3')
  # carried-over snapshot from a prior session
  persist_dynamic_sizing_snapshot(conn, 'live', {'effective_density': '0.58', 'dynamic_max_contracts': '9'})
  # a live runtime_event carries fresh sizing
  conn.execute(
    'INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc) '
    'VALUES (?,?,?,?,?,?,?)',
    ('INFO', 'sizing_posture', None, 'live', 's2',
     json.dumps({'effective_density': '9.9', 'dynamic_max_contracts': '42'}),
     '2026-07-03T23:59:59+00:00'),
  )
  conn.commit()
  posture = service._load_latest_sizing_posture(conn, operation_lane='live')
  # live value wins; snapshot did not shadow it
  assert posture.get('effective_density') == '9.9'
  assert posture.get('dynamic_max_contracts') == '42'
