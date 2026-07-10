"""Coverage for per-field provenance on working defaults (C5 OPTIMIZED pill support)."""

from __future__ import annotations

import sqlite3

from polyventure.persistence import (
  initialize_database,
  load_lane_default_sources,
  load_lane_defaults,
  persist_lane_defaults,
)


def _db(path):
  conn = sqlite3.connect(path)
  conn.row_factory = sqlite3.Row
  initialize_database(conn)
  return conn


def test_default_source_is_operator(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  persist_lane_defaults(conn, 'live', {'min_edge_dollars': '0.05'})
  assert load_lane_defaults(conn, 'live') == {'min_edge_dollars': '0.05'}
  assert load_lane_default_sources(conn, 'live') == {'min_edge_dollars': 'operator'}


def test_optimizer_source_recorded(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  persist_lane_defaults(
    conn, 'live',
    {'entry_window_start_sec': '753', 'entry_window_end_sec': '90', 'min_edge_dollars': '0.05'},
    sources={'entry_window_start_sec': 'optimizer:entry', 'entry_window_end_sec': 'optimizer:entry'},
  )
  sources = load_lane_default_sources(conn, 'live')
  assert sources['entry_window_start_sec'] == 'optimizer:entry'
  assert sources['entry_window_end_sec'] == 'optimizer:entry'
  # unlisted field defaults to operator
  assert sources['min_edge_dollars'] == 'operator'


def test_full_replace_semantics_preserved(tmp_path):
  conn = _db(tmp_path / 'db.sqlite3')
  persist_lane_defaults(conn, 'live', {'a': '1', 'b': '2'})
  # a subsequent write replaces the lane's whole state (documented behavior)
  persist_lane_defaults(conn, 'live', {'a': '9'}, sources={'a': 'optimizer:x'})
  assert load_lane_defaults(conn, 'live') == {'a': '9'}
  assert load_lane_default_sources(conn, 'live') == {'a': 'optimizer:x'}


def test_migration_adds_source_column(tmp_path):
  # Simulate a legacy DB lacking the source column, then migrate.
  path = tmp_path / 'legacy.sqlite3'
  raw = sqlite3.connect(path)
  raw.execute(
    'CREATE TABLE operator_lane_defaults (operation_lane TEXT NOT NULL, field_id TEXT NOT NULL, '
    'value TEXT NOT NULL, recorded_at_utc TEXT NOT NULL, PRIMARY KEY (operation_lane, field_id))'
  )
  raw.execute(
    "INSERT INTO operator_lane_defaults VALUES ('live', 'min_edge_dollars', '0.05', '2026-07-01T00:00:00+00:00')"
  )
  raw.commit()
  raw.close()

  conn = _db(path)  # initialize_database runs the migration
  # legacy row survives and reads back as operator-sourced
  assert load_lane_defaults(conn, 'live') == {'min_edge_dollars': '0.05'}
  assert load_lane_default_sources(conn, 'live') == {'min_edge_dollars': 'operator'}
