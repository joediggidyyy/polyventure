"""Focused tests for W1 (profile_token columns + migration), W2 (lane_active_datapack +
resolver), and W3 (candidate lifecycle columns).

These tests exercise the persistence layer directly so the schema/migration/resolver
contracts can be verified independently of the service and web layers."""

from __future__ import annotations

import sqlite3
from pathlib import Path

from polyventure.persistence import (
  CANDIDATE_LIFECYCLE_STAGES,
  LANE_ACTIVE_DATAPACK_CLOSED_CAUSES,
  PROFILE_TOKEN_LANE_EPHEMERAL_PREFIX,
  PROFILE_TOKEN_PARTITIONED_TABLES,
  PROFILE_TOKEN_UNBACKFILLED_SENTINEL,
  close_active_datapack,
  initialize_database,
  open_database,
  resolve_active_profile_token,
)


# ---------- W1: profile_token columns + migration ----------


def test_profile_token_column_present_on_all_partitioned_tables(tmp_path: Path) -> None:
  conn = open_database(tmp_path / 'kalshi.sqlite3')
  try:
    for table in PROFILE_TOKEN_PARTITIONED_TABLES:
      cols = [row['name'] for row in conn.execute(f'PRAGMA table_info({table})').fetchall()]
      assert 'profile_token' in cols, f'profile_token missing on {table}'
  finally:
    conn.close()


def test_initialize_database_is_idempotent(tmp_path: Path) -> None:
  db = tmp_path / 'kalshi.sqlite3'
  conn = open_database(db)
  try:
    initialize_database(conn)
    initialize_database(conn)
  finally:
    conn.close()


def test_backfill_stamps_sentinel_rows(tmp_path: Path) -> None:
  db = tmp_path / 'kalshi.sqlite3'
  # First open to get schema, then drop a sentinel-bearing row into runtime_events.
  conn = open_database(db)
  conn.execute(
    "INSERT INTO runtime_events(level, event_type, detail_json, recorded_at_utc, operation_lane, profile_token) "
    "VALUES('info', 'test', '{}', '2026-01-01T00:00:00Z', 'sandbox', ?)",
    (PROFILE_TOKEN_UNBACKFILLED_SENTINEL,),
  )
  conn.commit()
  conn.close()
  # Re-init triggers backfill.
  conn2 = open_database(db)
  try:
    row = conn2.execute(
      'SELECT profile_token FROM runtime_events WHERE operation_lane=?', ('sandbox',)
    ).fetchone()
    assert row is not None
    assert row['profile_token'] != PROFILE_TOKEN_UNBACKFILLED_SENTINEL
    assert row['profile_token'].startswith(PROFILE_TOKEN_LANE_EPHEMERAL_PREFIX)
    ladp = conn2.execute(
      'SELECT mint_basis, closed_cause FROM lane_active_datapack WHERE operation_lane=?',
      ('sandbox',),
    ).fetchone()
    assert ladp['mint_basis'] == 'lane_ephemeral'
    assert ladp['closed_cause'] == 'superseded_by_load'
  finally:
    conn2.close()


def test_backfill_does_not_rerun_when_lane_active_row_exists(tmp_path: Path) -> None:
  db = tmp_path / 'kalshi.sqlite3'
  conn = open_database(db)
  try:
    # Mint via resolver to seed an open row, then close it.
    resolve_active_profile_token(conn, 'sandbox')
    close_active_datapack(conn, 'sandbox', closed_cause='cli_clear')
    conn.commit()
    # Inject a sentinel-bearing orphan row.
    conn.execute(
      "INSERT INTO runtime_events(level, event_type, detail_json, recorded_at_utc, operation_lane, profile_token) "
      "VALUES('info', 'orphan', '{}', '2026-01-01T00:00:00Z', 'sandbox', ?)",
      (PROFILE_TOKEN_UNBACKFILLED_SENTINEL,),
    )
    conn.commit()
  finally:
    conn.close()
  # Re-init should NOT mint a second ephemeral row because lane_active_datapack already has one.
  conn2 = open_database(db)
  try:
    rows = conn2.execute(
      'SELECT COUNT(*) AS n FROM lane_active_datapack WHERE operation_lane=?',
      ('sandbox',),
    ).fetchone()
    assert rows['n'] == 1
    # The sentinel row remains unrewritten (no automatic recovery for post-init orphans).
    orphan = conn2.execute(
      "SELECT profile_token FROM runtime_events WHERE event_type='orphan'"
    ).fetchone()
    assert orphan['profile_token'] == PROFILE_TOKEN_UNBACKFILLED_SENTINEL
  finally:
    conn2.close()


# ---------- W2: lane_active_datapack + resolver ----------


def test_resolver_first_call_mints_and_inserts(tmp_path: Path) -> None:
  conn = open_database(tmp_path / 'kalshi.sqlite3')
  try:
    token = resolve_active_profile_token(conn, 'sandbox')
    assert token.startswith(PROFILE_TOKEN_LANE_EPHEMERAL_PREFIX)
    row = conn.execute(
      'SELECT profile_token, mint_basis, closed_at_utc FROM lane_active_datapack '
      'WHERE operation_lane=?',
      ('sandbox',),
    ).fetchone()
    assert row['profile_token'] == token
    assert row['mint_basis'] == 'lane_ephemeral'
    assert row['closed_at_utc'] is None
  finally:
    conn.close()


def test_resolver_second_call_reuses_open_token(tmp_path: Path) -> None:
  conn = open_database(tmp_path / 'kalshi.sqlite3')
  try:
    t1 = resolve_active_profile_token(conn, 'sandbox')
    t2 = resolve_active_profile_token(conn, 'sandbox')
    assert t1 == t2
    n = conn.execute(
      'SELECT COUNT(*) AS n FROM lane_active_datapack WHERE operation_lane=?',
      ('sandbox',),
    ).fetchone()['n']
    assert n == 1
  finally:
    conn.close()


def test_resolver_with_key_path_uses_key_path_derived_basis(tmp_path: Path) -> None:
  conn = open_database(tmp_path / 'kalshi.sqlite3')
  key_file = tmp_path / 'creds' / 'kalshi-prod.pem'
  key_file.parent.mkdir(parents=True, exist_ok=True)
  key_file.write_text('dummy')
  try:
    token = resolve_active_profile_token(conn, 'live', key_path=str(key_file))
    assert token.startswith('kalshi-')
    row = conn.execute(
      'SELECT mint_basis FROM lane_active_datapack WHERE operation_lane=?', ('live',),
    ).fetchone()
    assert row['mint_basis'] == 'key_path_derived'
  finally:
    conn.close()


def test_partial_unique_index_blocks_second_open_row(tmp_path: Path) -> None:
  conn = open_database(tmp_path / 'kalshi.sqlite3')
  try:
    resolve_active_profile_token(conn, 'sandbox')
    try:
      conn.execute(
        '''
        INSERT INTO lane_active_datapack
        (operation_lane, profile_token, became_active_at_utc, mint_basis, recorded_at_utc)
        VALUES (?, ?, ?, ?, ?)
        ''',
        ('sandbox', 'kalshi-lane-abcdef', '2026-01-01T00:00:00Z', 'lane_ephemeral',
         '2026-01-01T00:00:00Z'),
      )
      raised = False
    except sqlite3.IntegrityError:
      raised = True
    assert raised, 'partial unique index should reject a second open row for the same lane'
  finally:
    conn.close()


def test_close_active_datapack_closes_open_row_and_validates_cause(tmp_path: Path) -> None:
  conn = open_database(tmp_path / 'kalshi.sqlite3')
  try:
    resolve_active_profile_token(conn, 'sandbox')
    closed = close_active_datapack(conn, 'sandbox', closed_cause='cli_clear')
    assert closed is not None
    row = conn.execute(
      'SELECT closed_at_utc, closed_cause FROM lane_active_datapack WHERE operation_lane=?',
      ('sandbox',),
    ).fetchone()
    assert row['closed_at_utc'] is not None
    assert row['closed_cause'] == 'cli_clear'
    # A subsequent resolver call should mint a NEW token (the prior is closed).
    fresh = resolve_active_profile_token(conn, 'sandbox')
    assert fresh != closed
    # Invalid cause is rejected.
    try:
      close_active_datapack(conn, 'sandbox', closed_cause='not_a_real_cause')
      raised = False
    except ValueError:
      raised = True
    assert raised
  finally:
    conn.close()


# ---------- W3: candidate lifecycle columns ----------


def test_candidate_lifecycle_columns_present_with_defaults(tmp_path: Path) -> None:
  conn = open_database(tmp_path / 'kalshi.sqlite3')
  try:
    cols = {row['name']: row for row in conn.execute(
      'PRAGMA table_info(candidate_review_candidates)').fetchall()}
    for column in (
      'lifecycle_stage', 'terminal_cause', 'terminal_subcause', 'terminal_at_utc',
      'eligibility_status', 'polymath_eligibility_until_utc', 'expires_at_utc',
    ):
      assert column in cols, f'{column} missing from candidate_review_candidates'
    assert cols['lifecycle_stage']['dflt_value'] == "'discovered'"
    assert cols['eligibility_status']['dflt_value'] == "'present'"
  finally:
    conn.close()


def test_candidate_lifecycle_transition_writes_terminal_fields(tmp_path: Path) -> None:
  conn = open_database(tmp_path / 'kalshi.sqlite3')
  try:
    # Seed a parent run row, then a candidate row at the default 'discovered' stage.
    conn.execute(
      "INSERT INTO candidate_review_runs(run_id, candidate_signature, candidate_count, "
      "source_action, detail_json, recorded_at_utc, operation_lane, profile_token) "
      "VALUES('run-1', 'sig-1', 1, 'test', '{}', '2026-01-01T00:01:00Z', 'sandbox', 'kalshi-test')"
    )
    conn.execute(
      "INSERT INTO candidate_review_candidates(run_id, candidate_uid, candidate_key, ticker, "
      "detail_json, recorded_at_utc, operation_lane, profile_token) "
      "VALUES('run-1', 'uid-1', 'key-1', 'TICK', '{}', '2026-01-01T00:01:00Z', 'sandbox', 'kalshi-test')"
    )
    conn.commit()
    row = conn.execute(
      "SELECT lifecycle_stage, eligibility_status, terminal_cause, terminal_at_utc "
      "FROM candidate_review_candidates WHERE candidate_uid='uid-1'"
    ).fetchone()
    assert row['lifecycle_stage'] == 'discovered'
    assert row['eligibility_status'] == 'present'
    assert row['terminal_cause'] is None
    assert row['terminal_at_utc'] is None
    # Transition discovered -> selected -> in_flight -> terminal(canceled)
    conn.execute(
      "UPDATE candidate_review_candidates SET lifecycle_stage='selected' "
      "WHERE candidate_uid='uid-1'"
    )
    conn.execute(
      "UPDATE candidate_review_candidates SET lifecycle_stage='in_flight' "
      "WHERE candidate_uid='uid-1'"
    )
    conn.execute(
      "UPDATE candidate_review_candidates SET lifecycle_stage='terminal', "
      "terminal_cause='canceled', terminal_subcause='operator_request', "
      "terminal_at_utc='2026-01-01T00:05:00Z' WHERE candidate_uid='uid-1'"
    )
    conn.commit()
    row = conn.execute(
      "SELECT lifecycle_stage, terminal_cause, terminal_at_utc FROM candidate_review_candidates "
      "WHERE candidate_uid='uid-1'"
    ).fetchone()
    assert row['lifecycle_stage'] == 'terminal'
    assert row['terminal_cause'] == 'canceled'
    assert row['terminal_at_utc'] == '2026-01-01T00:05:00Z'
  finally:
    conn.close()


def test_lifecycle_stage_enum_values_documented() -> None:
  assert set(CANDIDATE_LIFECYCLE_STAGES) == {'discovered', 'selected', 'in_flight', 'terminal'}


def test_closed_cause_enum_includes_required_terms() -> None:
  required = {'overwrite_orphaned', 'extracted_to_store', 'cli_mutate', 'cli_clear',
              'superseded_by_load'}
  assert required.issubset(set(LANE_ACTIVE_DATAPACK_CLOSED_CAUSES))
