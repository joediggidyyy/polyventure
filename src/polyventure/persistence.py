from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import logging
from pathlib import Path
import sqlite3
from typing import Any
from uuid import uuid4

from . import signed_evidence
from .candidate_identity import compute_candidate_deadlines
from .flow_evidence import label_pair_outcome
from .types import AccountLimits, FillEvent, PairOrderPlan, PairPnlSnapshot

_LOGGER = logging.getLogger(__name__)


REQUIRED_TABLES = (
  'markets_seen',
  'pair_plans',
  'orders',
  'fills',
  'pair_states',
  'pair_pnl_snapshots',
  'service_heartbeats',
  'service_heartbeats_consolidated',
  'account_api_limits',
  'operator_actions',
  'runtime_events',
  'analytical_snapshots',
  'candidate_review_runs',
  'candidate_review_candidates',
  'candidate_saved_sets',
  'candidate_saved_set_members',
  'candidate_saved_set_evaluations',
  'known_non_binary_markets',
)

LANE_BEARING_TABLES = frozenset({
  'pair_plans',
  'orders',
  'fills',
  'pair_states',
  'pair_pnl_snapshots',
  'service_heartbeats',
  'account_api_limits',
  'operator_actions',
  'runtime_events',
  'analytical_snapshots',
  'candidate_review_runs',
  'candidate_saved_sets',
  'known_non_binary_markets',
})

LANE_SESSION_TABLES = frozenset({
  'pair_states',
  'pair_pnl_snapshots',
  'service_heartbeats',
  'account_api_limits',
  'operator_actions',
  'runtime_events',
  'analytical_snapshots',
  'known_non_binary_markets',
})

DATAPACK_SCHEMA_VERSION = '2026-05-21.stage2'
DATAPACK_PROFILE_TOKEN_PREFIX = 'kalshi-'

# Tables that gain `profile_token` partition column per plan W1.
# Note: register §7.18.1 names `service_rate_caps`; the actual table is `account_api_limits`.
PROFILE_TOKEN_PARTITIONED_TABLES = (
  'candidate_review_runs',
  'candidate_review_candidates',
  'candidate_saved_sets',
  'candidate_saved_set_members',
  'candidate_saved_set_evaluations',
  'operator_actions',
  'runtime_events',
  'analytical_snapshots',
  'account_api_limits',
)
PROFILE_TOKEN_UNBACKFILLED_SENTINEL = '__unbackfilled__'
PROFILE_TOKEN_LANE_EPHEMERAL_PREFIX = 'kalshi-lane-'
LANE_ACTIVE_DATAPACK_CLOSED_CAUSES = (
  'overwrite_orphaned',
  'extracted_to_store',
  'cli_mutate',
  'cli_clear',
  'superseded_by_load',
)
LANE_ACTIVE_DATAPACK_MINT_BASES = ('key_path_derived', 'lane_ephemeral')
CANDIDATE_LIFECYCLE_STAGES = ('discovered', 'selected', 'in_flight', 'terminal')
CANDIDATE_TERMINAL_CAUSES = ('reconciled', 'canceled', 'expired_unfilled', 'rejected', 'failed')
CANDIDATE_ELIGIBILITY_STATUSES = (
  'present',
  'missing_blocked',
  'revoked_post_select',
  'revoked_in_flight',
)
NOTIFICATION_LEVELS = ('info', 'warn', 'error')
NOTIFICATION_SOURCES = ('eligibility', 'lane_change', 'connection', 'system')

DATAPACK_FAMILY_SPECS: tuple[dict[str, Any], ...] = (
  {
    'family_id': 'runtime_state',
    'classification': 'export_then_optional_purge',
    'packaging_mode': 'bounded_non_secret',
    'restore_mode': 'lane_partition_replace',
    'purge_eligible': True,
    'revalidation_required': False,
    'tables': (
      'pair_plans',
      'orders',
      'fills',
      'pair_states',
      'pair_pnl_snapshots',
      'service_heartbeats',
      'account_api_limits',
      'operator_actions',
      'runtime_events',
    ),
  },
  {
    'family_id': 'analytical_state',
    'classification': 'restore_with_revalidation',
    'packaging_mode': 'bounded_non_secret',
    'restore_mode': 'restore_with_revalidation',
    'purge_eligible': True,
    'revalidation_required': True,
    'tables': (
      'analytical_snapshots',
    ),
  },
  {
    'family_id': 'candidate_review_history',
    'classification': 'history_only',
    'packaging_mode': 'bounded_non_secret',
    'restore_mode': 'history_only',
    'purge_eligible': False,
    'revalidation_required': False,
    'tables': (
      'candidate_review_runs',
      'candidate_review_candidates',
      'candidate_saved_sets',
      'candidate_saved_set_members',
      'candidate_saved_set_evaluations',
      'known_non_binary_markets',
    ),
  },
  {
    'family_id': 'synthetic_refinement_fixtures',
    'classification': 'synthetic_proof',
    'packaging_mode': 'synthetic_only',
    'restore_mode': 'proof_only',
    'purge_eligible': False,
    'revalidation_required': False,
    'tables': (),
  },
  {
    'family_id': 'evidence_refs',
    'classification': 'retain_in_place',
    'packaging_mode': 'reference_only',
    'restore_mode': 'retain_in_place',
    'purge_eligible': False,
    'revalidation_required': False,
    'tables': (),
  },
  {
    'family_id': 'session_control_state',
    'classification': 'never_package',
    'packaging_mode': 'excluded',
    'restore_mode': 'never_package',
    'purge_eligible': False,
    'revalidation_required': True,
    'tables': (),
  },
)


SCHEMA_STATEMENTS = (
  '''
  CREATE TABLE IF NOT EXISTS markets_seen (
    ticker TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    close_time_utc TEXT,
    last_seen_at_utc TEXT NOT NULL
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS pair_plans (
    pair_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    yes_price_dollars TEXT NOT NULL,
    no_price_dollars TEXT NOT NULL,
    contract_count TEXT NOT NULL,
    yes_client_order_id TEXT NOT NULL,
    no_client_order_id TEXT NOT NULL,
    time_in_force TEXT NOT NULL,
    post_only INTEGER NOT NULL,
    cancel_order_on_pause INTEGER NOT NULL,
    subaccount INTEGER NOT NULL,
    operation_lane TEXT NOT NULL,
    created_at_utc TEXT NOT NULL
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS orders (
    order_id TEXT PRIMARY KEY,
    pair_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    side TEXT NOT NULL,
    price_dollars TEXT NOT NULL,
    contract_count TEXT NOT NULL,
    status TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY (pair_id) REFERENCES pair_plans(pair_id)
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS fills (
    fill_id TEXT PRIMARY KEY,
    pair_id TEXT NOT NULL,
    order_id TEXT NOT NULL,
    client_order_id TEXT NOT NULL,
    side TEXT NOT NULL,
    price_dollars TEXT NOT NULL,
    contract_count TEXT NOT NULL,
    fee_dollars TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    created_at_utc TEXT NOT NULL,
    FOREIGN KEY (pair_id) REFERENCES pair_plans(pair_id)
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS pair_states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id TEXT NOT NULL,
    state TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    FOREIGN KEY (pair_id) REFERENCES pair_plans(pair_id)
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS pair_pnl_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id TEXT NOT NULL,
    locked_contracts TEXT NOT NULL,
    gross_dollars TEXT NOT NULL,
    net_projected_dollars TEXT NOT NULL,
    net_realized_dollars TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    recorded_at_utc TEXT NOT NULL,
    FOREIGN KEY (pair_id) REFERENCES pair_plans(pair_id)
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS service_heartbeats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    component TEXT NOT NULL,
    status TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS account_api_limits (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    usage_tier TEXT NOT NULL,
    read_refill_rate INTEGER NOT NULL,
    read_bucket_capacity INTEGER NOT NULL,
    write_refill_rate INTEGER NOT NULL,
    write_bucket_capacity INTEGER NOT NULL,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    recorded_at_utc TEXT NOT NULL
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS operator_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    action TEXT NOT NULL,
    pair_id TEXT,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS runtime_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    level TEXT NOT NULL,
    event_type TEXT NOT NULL,
    pair_id TEXT,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS analytical_snapshots (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    snapshot_type TEXT NOT NULL,
    evidence_class TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS candidate_review_runs (
    run_id TEXT PRIMARY KEY,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    candidate_signature TEXT NOT NULL,
    candidate_count INTEGER NOT NULL,
    source_action TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS candidate_review_candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    candidate_uid TEXT NOT NULL,
    candidate_key TEXT NOT NULL,
    ticker TEXT,
    qualifier_tier TEXT,
    review_row_origin TEXT,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    UNIQUE(run_id, candidate_uid),
    FOREIGN KEY (run_id) REFERENCES candidate_review_runs(run_id)
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS candidate_saved_sets (
    saved_set_id TEXT PRIMARY KEY,
    run_id TEXT,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    saved_key_count INTEGER NOT NULL,
    state_id TEXT NOT NULL,
    source_action TEXT NOT NULL,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    FOREIGN KEY (run_id) REFERENCES candidate_review_runs(run_id)
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS candidate_saved_set_members (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    saved_set_id TEXT NOT NULL,
    candidate_uid TEXT NOT NULL,
    candidate_key TEXT NOT NULL,
    member_order INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    UNIQUE(saved_set_id, candidate_uid),
    FOREIGN KEY (saved_set_id) REFERENCES candidate_saved_sets(saved_set_id)
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS candidate_saved_set_evaluations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    saved_set_id TEXT NOT NULL,
    evaluation_status TEXT NOT NULL,
    actionability_status TEXT NOT NULL,
    visibility_status TEXT NOT NULL,
    offline_verifiable INTEGER NOT NULL,
    online_revalidation_required INTEGER NOT NULL,
    detail_json TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    FOREIGN KEY (saved_set_id) REFERENCES candidate_saved_sets(saved_set_id)
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS known_non_binary_markets (
    ledger_key TEXT PRIMARY KEY,
    series_ticker TEXT,
    event_ticker TEXT,
    market_ticker TEXT,
    shape_signature TEXT NOT NULL,
    classification_reason TEXT NOT NULL,
    actionability TEXT NOT NULL,
    market_count INTEGER NOT NULL,
    mutually_exclusive TEXT,
    sample_sibling_tickers_json TEXT NOT NULL,
    first_seen_utc TEXT NOT NULL,
    last_seen_utc TEXT NOT NULL,
    seen_count INTEGER NOT NULL,
    source_run_id TEXT,
    source_runtime_event_id TEXT,
    operation_lane TEXT NOT NULL,
    lane_session_id TEXT,
    detail_json TEXT NOT NULL
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS lane_active_datapack (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    operation_lane TEXT NOT NULL,
    profile_token TEXT NOT NULL,
    became_active_at_utc TEXT NOT NULL,
    closed_at_utc TEXT,
    closed_cause TEXT,
    mint_basis TEXT NOT NULL,
    extract_manifest_path TEXT,
    cli_command TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    recorded_at_utc TEXT NOT NULL
  )
  ''',
  '''
  CREATE UNIQUE INDEX IF NOT EXISTS lane_active_datapack_open
    ON lane_active_datapack(operation_lane) WHERE closed_at_utc IS NULL
  ''',
  '''
  CREATE TABLE IF NOT EXISTS operator_notifications (
    notification_id TEXT PRIMARY KEY,
    created_at_utc TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    profile_token TEXT NOT NULL,
    level TEXT NOT NULL,
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    source TEXT NOT NULL,
    related_candidate_id TEXT,
    dismissed_at_utc TEXT,
    dismissed_by TEXT,
    visibility_expires_at_utc TEXT
  )
  ''',
  '''
  CREATE INDEX IF NOT EXISTS operator_notifications_lane_token
    ON operator_notifications(operation_lane, profile_token, created_at_utc DESC)
  ''',
  '''
  CREATE TABLE IF NOT EXISTS operator_lane_defaults (
    operation_lane TEXT NOT NULL,
    field_id TEXT NOT NULL,
    value TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'operator',
    recorded_at_utc TEXT NOT NULL,
    PRIMARY KEY (operation_lane, field_id)
  )
  ''',
  '''
  CREATE TABLE IF NOT EXISTS service_heartbeats_consolidated (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    tier TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    bucket_start_utc TEXT NOT NULL,
    latest_balance_snapshot TEXT NOT NULL,
    latest_fresh_at_utc TEXT NOT NULL,
    row_count INTEGER NOT NULL DEFAULT 1,
    UNIQUE(tier, operation_lane, bucket_start_utc)
  )
  ''',
  '''
  CREATE INDEX IF NOT EXISTS idx_hbc_lane_tier_bucket
    ON service_heartbeats_consolidated (operation_lane, tier, bucket_start_utc DESC)
  ''',
  # D1 hot-path indexes (funds-decoupling + DB-performance BMAP 2026-06-20): the
  # STOP halt-fence guard and the latest-heartbeat / funds-heartbeat lookups filter
  # on these columns over large tables (runtime_events grows to tens of thousands of
  # rows). Without these only the PK autoindexes exist, so each is a full table scan.
  # Candidate-review rebuilds filter runs by lane_session_id before joining candidate
  # rows; keep that seek indexed so replay rebuilds do not scan accumulated runs. All
  # lane_session_id reads of candidate_review_runs are order-insensitive (IN-subqueries)
  # or already carry an explicit ORDER BY, so this index does not reorder any
  # non-deterministic query (the earlier determinism deferral is resolved).
  '''
  CREATE INDEX IF NOT EXISTS idx_crr_lane_session
    ON candidate_review_runs(lane_session_id)
  ''',
  '''
  CREATE INDEX IF NOT EXISTS idx_rte_session_type
    ON runtime_events (lane_session_id, event_type)
  ''',
  '''
  CREATE INDEX IF NOT EXISTS idx_shb_lane_id
    ON service_heartbeats (operation_lane, id DESC)
  ''',
  # Lane A coverability instrumentation (additive, read-only capture): authoritative
  # both-sided resting depth + per-side traded flow recorded at decision time, linked
  # to the pair so a future entry gate (Lane B) can be calibrated against real evidence.
  # Explicit lane, no DEFAULT (honors the schema-wide lane-default remediation).
  '''
  CREATE TABLE IF NOT EXISTS pair_liquidity_observations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pair_id TEXT NOT NULL,
    ticker TEXT NOT NULL,
    phase TEXT NOT NULL,
    operation_lane TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    readback_status TEXT NOT NULL,
    yes_bid_depth_json TEXT NOT NULL,
    no_bid_depth_json TEXT NOT NULL,
    best_yes_bid TEXT,
    best_no_bid TEXT,
    yes_depth_within_band TEXT,
    no_depth_within_band TEXT,
    yes_flow_window_fp TEXT,
    no_flow_window_fp TEXT,
    flow_window_sec TEXT,
    divergence TEXT,
    volume_24h_fp TEXT,
    volume_fp TEXT,
    open_interest_fp TEXT,
    intended_yes_price TEXT,
    intended_no_price TEXT,
    intended_contract_count TEXT,
    lane_session_id TEXT
  )
  ''',
  '''
  CREATE INDEX IF NOT EXISTS idx_plo_pair_phase
    ON pair_liquidity_observations (pair_id, phase)
  ''',
)


def _normalize_operation_lane(operation_lane: str | None) -> str:
  # Fail closed: no fallback, no default. An absent/empty lane is not silently
  # coerced to 'sandbox' -- doing so would write live data under the sandbox label
  # (or vice versa). Callers that legitimately mean "all lanes" pass None and must
  # guard with `if operation_lane is not None else None` BEFORE calling this; they
  # never reach here with None. Every other caller must supply an explicit lane.
  lane = str(operation_lane or '').strip().lower()
  if lane not in {'sandbox', 'live'}:
    raise ValueError('operation_lane must be sandbox or live (got empty/None or unknown value).')
  return lane


def _column_exists(connection: sqlite3.Connection, table: str, column: str) -> bool:
  rows = connection.execute("PRAGMA table_info('{table}')".format(table=table)).fetchall()
  return any(str(row['name']) == column for row in rows)


def _ensure_column(connection: sqlite3.Connection, table: str, column: str, definition: str) -> None:
  if _column_exists(connection, table, column):
    return
  connection.execute(
    'ALTER TABLE {table} ADD COLUMN {column} {definition}'.format(
      table=table,
      column=column,
      definition=definition,
    )
  )


# Paths that have already been rotated in this process. rotate_database runs once per
# unique resolved DB path per process start (DATABASE_ROTATION BMAP 2026-06-17 §5).
_rotated_paths: set[str] = set()


def open_database(path: str | Path) -> sqlite3.Connection:
  db_path = Path(path)
  db_path.parent.mkdir(parents=True, exist_ok=True)
  connection = sqlite3.connect(db_path)
  connection.row_factory = sqlite3.Row
  # Concurrency & availability (DATABASE_CONCURRENCY_AND_AVAILABILITY BMAP 2026-06-17):
  # WAL lets readers and writers proceed concurrently, so a long scan write
  # transaction no longer blocks bootstrap reads (audit F-2) and the funds banner can
  # read fresh balance mid-scan (F-1). busy_timeout waits gracefully on lock
  # contention; synchronous=NORMAL is the crash-safe WAL pairing. Pragmas are set on
  # every connection (idempotent); WAL persists in the DB header.
  connection.execute('PRAGMA journal_mode = WAL')
  connection.execute('PRAGMA busy_timeout = 5000')
  connection.execute('PRAGMA synchronous = NORMAL')
  connection.execute('PRAGMA foreign_keys = ON')
  _verify_wal_active(connection, db_path)
  initialize_database(connection)
  path_key = str(db_path.resolve())
  if path_key not in _rotated_paths:
    rotate_database(connection)
    _rotated_paths.add(path_key)
  return connection


def connect_readonly(path: str | Path, *, timeout: float = 5.0) -> sqlite3.Connection:
  """Open a connection for READ-ONLY projection queries (no schema init, no rotation/archival).

  Rendering a read projection (cards, stage columns, execution-panel counts) must never mutate or
  rotate the operator database as a side effect. Unlike open_database, this performs no
  initialize_database and no rotate_database; it only sets the safe read pragmas. Tables are assumed
  to already exist (created at startup). A missing table/DB surfaces as a query error the caller
  logs and degrades from, rather than a silent schema mutation or archival sweep on a read path.
  """
  connection = sqlite3.connect(Path(path), timeout=float(timeout))
  connection.row_factory = sqlite3.Row
  connection.execute('PRAGMA busy_timeout = {ms}'.format(ms=max(0, int(float(timeout) * 1000))))
  return connection


def _verify_wal_active(connection: sqlite3.Connection, db_path: Path) -> None:
  # Fail-closed visibility: WAL can silently fall back on unusual filesystems. For a
  # file-backed database this should read 'wal'; in-memory databases legitimately
  # report 'memory' and are not flagged.
  mode = str(connection.execute('PRAGMA journal_mode').fetchone()[0] or '').lower()
  is_memory = str(db_path) == ':memory:' or str(db_path).endswith(':memory:')
  if not is_memory and mode != 'wal':
    _LOGGER.warning(
      'database journal_mode is %r (expected wal) for %s; reader/writer concurrency may be degraded',
      mode,
      db_path.name,
    )


# ---------------------------------------------------------------------------
# Database rotation: heartbeat consolidation (Track 1) + discrete archival (Track 2)
# ---------------------------------------------------------------------------

ARCHIVE_DB_FILENAME = 'kalshi_archive.sqlite3'

# Tables moved to the archive DB on age-based thresholds.
# ts_col: the timestamp column used for the age check.
# threshold_days: rows older than this are eligible for archival.
# pk_col: primary key column, used for the id-confirmed DELETE guard.
ARCHIVE_TABLE_SPECS: dict[str, dict[str, Any]] = {
  'candidate_review_candidates': {
    'ts_col': 'recorded_at_utc',
    'threshold_days': 7,
    'pk_col': 'id',
  },
  'candidate_review_runs': {
    'ts_col': 'recorded_at_utc',
    'threshold_days': 30,
    'pk_col': 'run_id',
  },
  'runtime_events': {
    'ts_col': 'recorded_at_utc',
    'threshold_days': 30,
    'pk_col': 'id',
  },
  'analytical_snapshots': {
    'ts_col': 'recorded_at_utc',
    'threshold_days': 30,
    'pk_col': 'id',
  },
  'account_api_limits': {
    'ts_col': 'recorded_at_utc',
    'threshold_days': 7,
    'pk_col': 'id',
  },
  'markets_seen': {
    'ts_col': 'last_seen_at_utc',
    'threshold_days': 30,
    'pk_col': 'ticker',
  },
}


def _floor_to_bucket(ts: str, tier: str) -> str:
  # Floor an ISO8601 timestamp string to the start of its day (tier='daily') or
  # hour (tier='hourly'). Handles both 'T' and space separators.
  normalized = ts.replace(' ', 'T')
  if tier == 'daily':
    return normalized[:10] + 'T00:00:00+00:00'
  if tier == 'hourly':
    return normalized[:13] + ':00:00+00:00'
  raise ValueError(f'Unknown tier: {tier!r}')


def _ensure_archive_table(
  archive_conn: sqlite3.Connection,
  table: str,
  col_names: list[str],
  col_types: list[str],
  pk_col: str,
) -> None:
  existing = archive_conn.execute(
    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
  ).fetchone()
  if existing:
    return
  col_defs = ', '.join(
    f'"{name}" {typ}' for name, typ in zip(col_names, col_types)
  )
  archive_conn.execute(f'CREATE TABLE IF NOT EXISTS "{table}" ({col_defs})')
  archive_conn.execute(
    f'CREATE UNIQUE INDEX IF NOT EXISTS "{table}_pk_idx" ON "{table}" ("{pk_col}")'
  )
  archive_conn.commit()


def consolidate_heartbeats(
  connection: sqlite3.Connection,
  *,
  now_utc: datetime | None = None,
) -> dict[str, int]:
  if now_utc is None:
    now_utc = datetime.now(timezone.utc)

  # 1-hour safety floor: no row newer than this is ever removed from the raw table.
  # threshold_days >> 1h for all thresholds, so effective cutoffs equal the tier cutoffs.
  safety_iso = (now_utc - timedelta(hours=1)).astimezone(timezone.utc).isoformat()
  daily_iso = (now_utc - timedelta(days=7)).astimezone(timezone.utc).isoformat()
  hourly_iso = (now_utc - timedelta(hours=24)).astimezone(timezone.utc).isoformat()

  daily_buckets_written = 0
  hourly_buckets_written = 0
  raw_rows_removed = 0

  # --- Step A: daily consolidation (raw rows older than 7d) ---
  daily_rows = connection.execute(
    'SELECT id, operation_lane, detail_json, recorded_at_utc FROM service_heartbeats '
    'WHERE recorded_at_utc < ? AND recorded_at_utc < ? '
    'ORDER BY operation_lane, recorded_at_utc ASC, id ASC',
    (daily_iso, safety_iso),
  ).fetchall()

  daily_buckets: dict[tuple[str, str], dict[str, Any]] = {}
  for row in daily_rows:
    lane = row['operation_lane']
    ts = row['recorded_at_utc']
    key = (lane, _floor_to_bucket(ts, 'daily'))
    bucket = daily_buckets.setdefault(key, {'balance': '0', 'fresh_at': ts, 'count': 0})
    bucket['count'] += 1
    try:
      detail = json.loads(row['detail_json'] or '{}') or {}
    except Exception:
      detail = {}
    if detail.get('funds_refresh_status') == 'fresh':
      bucket['balance'] = str(detail.get('available_funds_snapshot') or '0')
      bucket['fresh_at'] = ts

  with connection:
    for (lane, bucket_start), data in daily_buckets.items():
      cursor = connection.execute(
        '''
        INSERT OR IGNORE INTO service_heartbeats_consolidated
            (tier, operation_lane, bucket_start_utc, latest_balance_snapshot,
             latest_fresh_at_utc, row_count)
        VALUES ('daily', ?, ?, ?, ?, ?)
        ''',
        (lane, bucket_start, data['balance'], data['fresh_at'], data['count']),
      )
      if cursor.rowcount > 0:
        daily_buckets_written += 1
    deleted = connection.execute(
      'DELETE FROM service_heartbeats WHERE recorded_at_utc < ? AND recorded_at_utc < ?',
      (daily_iso, safety_iso),
    )
    raw_rows_removed += deleted.rowcount

  # --- Step B: hourly consolidation (raw rows 24h–7d old) ---
  hourly_rows = connection.execute(
    'SELECT id, operation_lane, detail_json, recorded_at_utc FROM service_heartbeats '
    'WHERE recorded_at_utc >= ? AND recorded_at_utc < ? AND recorded_at_utc < ? '
    'ORDER BY operation_lane, recorded_at_utc ASC, id ASC',
    (daily_iso, hourly_iso, safety_iso),
  ).fetchall()

  hourly_buckets: dict[tuple[str, str], dict[str, Any]] = {}
  for row in hourly_rows:
    lane = row['operation_lane']
    ts = row['recorded_at_utc']
    key = (lane, _floor_to_bucket(ts, 'hourly'))
    bucket = hourly_buckets.setdefault(key, {'balance': '0', 'fresh_at': ts, 'count': 0})
    bucket['count'] += 1
    try:
      detail = json.loads(row['detail_json'] or '{}') or {}
    except Exception:
      detail = {}
    if detail.get('funds_refresh_status') == 'fresh':
      bucket['balance'] = str(detail.get('available_funds_snapshot') or '0')
      bucket['fresh_at'] = ts

  with connection:
    for (lane, bucket_start), data in hourly_buckets.items():
      cursor = connection.execute(
        '''
        INSERT OR IGNORE INTO service_heartbeats_consolidated
            (tier, operation_lane, bucket_start_utc, latest_balance_snapshot,
             latest_fresh_at_utc, row_count)
        VALUES ('hourly', ?, ?, ?, ?, ?)
        ''',
        (lane, bucket_start, data['balance'], data['fresh_at'], data['count']),
      )
      if cursor.rowcount > 0:
        hourly_buckets_written += 1
    deleted = connection.execute(
      'DELETE FROM service_heartbeats '
      'WHERE recorded_at_utc >= ? AND recorded_at_utc < ? AND recorded_at_utc < ?',
      (daily_iso, hourly_iso, safety_iso),
    )
    raw_rows_removed += deleted.rowcount

  return {
    'daily_buckets_written': daily_buckets_written,
    'hourly_buckets_written': hourly_buckets_written,
    'raw_rows_removed': raw_rows_removed,
  }


def archive_discrete_tables(
  connection: sqlite3.Connection,
  *,
  now_utc: datetime | None = None,
  archive_db_path: Path | None = None,
) -> dict[str, int]:
  if now_utc is None:
    now_utc = datetime.now(timezone.utc)

  if archive_db_path is None:
    for row in connection.execute('PRAGMA database_list').fetchall():
      if row['name'] == 'main' and row['file']:
        archive_db_path = Path(row['file']).parent / ARCHIVE_DB_FILENAME
        break
    if archive_db_path is None:
      _LOGGER.debug('archive_discrete_tables: no file-backed DB path; skipping')
      return {}

  archive_db_path = Path(archive_db_path)
  archive_db_path.parent.mkdir(parents=True, exist_ok=True)

  safety_iso = (now_utc - timedelta(hours=1)).astimezone(timezone.utc).isoformat()

  archive_conn = sqlite3.connect(archive_db_path)
  archive_conn.row_factory = sqlite3.Row
  try:
    archive_conn.execute('PRAGMA journal_mode = WAL')
    archive_conn.execute('PRAGMA busy_timeout = 5000')
    archive_conn.execute('PRAGMA synchronous = NORMAL')

    results: dict[str, int] = {}

    for table, spec in ARCHIVE_TABLE_SPECS.items():
      ts_col = spec['ts_col']
      pk_col = spec['pk_col']
      threshold_iso = (
        now_utc - timedelta(days=spec['threshold_days'])
      ).astimezone(timezone.utc).isoformat()

      # candidate_review_runs: skip runs still referenced by saved sets (FK guard)
      extra_where = ''
      if table == 'candidate_review_runs':
        extra_where = (
          " AND run_id NOT IN"
          " (SELECT COALESCE(run_id, '') FROM candidate_saved_sets)"
        )

      rows_to_archive = connection.execute(
        f'SELECT * FROM "{table}" WHERE {ts_col} < ? AND {ts_col} < ?{extra_where}',
        (threshold_iso, safety_iso),
      ).fetchall()

      if not rows_to_archive:
        results[table] = 0
        continue

      col_info = connection.execute(f'PRAGMA table_info("{table}")').fetchall()
      col_names = [c['name'] for c in col_info]
      col_types = [c['type'] or 'TEXT' for c in col_info]

      _ensure_archive_table(archive_conn, table, col_names, col_types, pk_col)

      placeholders = ', '.join(['?'] * len(col_names))
      col_list = ', '.join(f'"{c}"' for c in col_names)

      inserted_pks: list[Any] = []
      with archive_conn:
        for row in rows_to_archive:
          try:
            archive_conn.execute(
              f'INSERT OR IGNORE INTO "{table}" ({col_list}) VALUES ({placeholders})',
              tuple(row[c] for c in col_names),
            )
            inserted_pks.append(row[pk_col])
          except Exception as exc:
            _LOGGER.warning(
              'archive_discrete_tables: failed to archive %s pk=%s: %s',
              table, row[pk_col], exc,
            )

      if not inserted_pks:
        results[table] = 0
        continue

      confirmed = {
        r[0]
        for r in archive_conn.execute(
          f'SELECT "{pk_col}" FROM "{table}"'
          f' WHERE "{pk_col}" IN ({",".join(["?"] * len(inserted_pks))})',
          inserted_pks,
        ).fetchall()
      }
      to_delete = [pk for pk in inserted_pks if pk in confirmed]

      if to_delete:
        with connection:
          connection.execute(
            f'DELETE FROM "{table}" WHERE "{pk_col}" IN ({",".join(["?"] * len(to_delete))})',
            to_delete,
          )
        results[table] = len(to_delete)
      else:
        results[table] = 0

    return results
  finally:
    archive_conn.close()


def rotate_database(
  connection: sqlite3.Connection,
  *,
  now_utc: datetime | None = None,
  archive_db_path: Path | None = None,
) -> None:
  if now_utc is None:
    now_utc = datetime.now(timezone.utc)
  consolidate_result = consolidate_heartbeats(connection, now_utc=now_utc)
  archive_result = archive_discrete_tables(
    connection, now_utc=now_utc, archive_db_path=archive_db_path
  )
  _LOGGER.info(
    'rotate_database: %s',
    json.dumps({
      'now_utc': now_utc.isoformat(),
      'consolidate_heartbeats': consolidate_result,
      'archive_discrete_tables': archive_result,
    }),
  )


def initialize_database(connection: sqlite3.Connection) -> None:
  with connection:
    for statement in SCHEMA_STATEMENTS:
      connection.execute(statement)
    # Canonical lane shape is `operation_lane TEXT NOT NULL` with NO default -- the
    # CREATE TABLE statements above declare it inline so every NEW database is born
    # canonical. The _ensure_column calls below are the LEGACY ADD-COLUMN path for
    # pre-existing databases that predate the column. SQLite forbids ADD COLUMN with
    # NOT NULL and no default on an existing table, so the DEFAULT here is mandatory
    # for that path only -- it is dead on new databases (column already exists, the
    # call is a no-op) and never fires on any write because every persist function now
    # supplies an explicit lane. Do NOT read this DEFAULT as a fallback violation; it
    # is the unavoidable SQLite migration mechanic. (No-fallback principle preserved.)
    for table in LANE_BEARING_TABLES:
      _ensure_column(connection, table, 'operation_lane', "TEXT NOT NULL DEFAULT 'sandbox'")
    for table in LANE_SESSION_TABLES:
      _ensure_column(connection, table, 'lane_session_id', 'TEXT')
    # C5 provenance: per-field source on working defaults (operator vs optimizer:*).
    # DEFAULT 'operator' is correct for pre-existing operator-set rows and is the
    # SQLite legacy ADD-COLUMN mechanic only; new writes pass an explicit source.
    _ensure_column(connection, 'operator_lane_defaults', 'source', "TEXT NOT NULL DEFAULT 'operator'")
    _migrate_profile_token_columns(connection)
    _migrate_candidate_lifecycle_columns(connection)
    _backfill_lane_active_datapack(connection)


def _migrate_profile_token_columns(connection: sqlite3.Connection) -> None:
  default_literal = "TEXT NOT NULL DEFAULT '{sentinel}'".format(
    sentinel=PROFILE_TOKEN_UNBACKFILLED_SENTINEL,
  )
  # Child tables that inherit lane via parent join historically lacked an explicit
  # operation_lane column; add it so per-lane partitioning of profile_token is uniform.
  # These three now declare operation_lane inline in CREATE TABLE (canonical NOT NULL,
  # no default), so on a new database these calls are no-ops. The retained DEFAULT is
  # the LEGACY ADD-COLUMN path only (SQLite requires a default to add a NOT NULL column
  # to an existing table); it never fires on writes -- every persist function supplies
  # an explicit lane. See initialize_database for the full rationale.
  for child in ('candidate_review_candidates', 'candidate_saved_set_members',
                'candidate_saved_set_evaluations'):
    _ensure_column(connection, child, 'operation_lane', "TEXT NOT NULL DEFAULT 'sandbox'")
  for table in PROFILE_TOKEN_PARTITIONED_TABLES:
    _ensure_column(connection, table, 'profile_token', default_literal)


def _migrate_candidate_lifecycle_columns(connection: sqlite3.Connection) -> None:
  _ensure_column(
    connection,
    'candidate_review_candidates',
    'lifecycle_stage',
    "TEXT NOT NULL DEFAULT 'discovered'",
  )
  _ensure_column(connection, 'candidate_review_candidates', 'terminal_cause', 'TEXT')
  _ensure_column(connection, 'candidate_review_candidates', 'terminal_subcause', 'TEXT')
  _ensure_column(connection, 'candidate_review_candidates', 'terminal_at_utc', 'TEXT')
  _ensure_column(
    connection,
    'candidate_review_candidates',
    'eligibility_status',
    "TEXT NOT NULL DEFAULT 'present'",
  )
  _ensure_column(
    connection,
    'candidate_review_candidates',
    'polymath_eligibility_until_utc',
    'TEXT',
  )
  _ensure_column(connection, 'candidate_review_candidates', 'expires_at_utc', 'TEXT')
  # Lane A (candidate-expiry clock): discovery-time deadlines, stamped once at first
  # discovery and preserved on conflict. Nullable TEXT (ISO-8601 UTC), no DEFAULT.
  _ensure_column(connection, 'candidate_review_candidates', 'market_close_at_utc', 'TEXT')
  _ensure_column(connection, 'candidate_review_candidates', 'view_expires_at_utc', 'TEXT')
  _ensure_column(connection, 'candidate_review_candidates', 'submit_expires_at_utc', 'TEXT')


def _lanes_with_unbackfilled_rows(connection: sqlite3.Connection) -> dict[str, dict[str, Any]]:
  """Return per-lane min/max recorded_at + row_count for rows still bearing the sentinel token.

  The sentinel default is only present after the migration ALTER ran. Rows minted by
  post-migration code carry a real profile_token from `resolve_active_profile_token` and are
  ignored here.
  """
  lane_stats: dict[str, dict[str, Any]] = {}
  for table in PROFILE_TOKEN_PARTITIONED_TABLES:
    columns = _table_columns(connection, table)
    if 'profile_token' not in columns or 'operation_lane' not in columns:
      continue
    ts_column = 'recorded_at_utc' if 'recorded_at_utc' in columns else None
    if ts_column is None:
      # Tables without a timestamp column still count toward row totals.
      rows = connection.execute(
        'SELECT operation_lane AS lane, COUNT(*) AS n FROM {table} '
        'WHERE profile_token = ? GROUP BY operation_lane'.format(table=table),
        (PROFILE_TOKEN_UNBACKFILLED_SENTINEL,),
      ).fetchall()
      for row in rows:
        lane = str(row['lane'] or 'sandbox')
        stats = lane_stats.setdefault(lane, {'count': 0, 'min_ts': None, 'max_ts': None})
        stats['count'] += int(row['n'] or 0)
      continue
    rows = connection.execute(
      'SELECT operation_lane AS lane, COUNT(*) AS n, MIN({ts}) AS mn, MAX({ts}) AS mx '
      'FROM {table} WHERE profile_token = ? GROUP BY operation_lane'.format(
        table=table, ts=ts_column,
      ),
      (PROFILE_TOKEN_UNBACKFILLED_SENTINEL,),
    ).fetchall()
    for row in rows:
      lane = str(row['lane'] or 'sandbox')
      stats = lane_stats.setdefault(lane, {'count': 0, 'min_ts': None, 'max_ts': None})
      stats['count'] += int(row['n'] or 0)
      mn = row['mn']
      mx = row['mx']
      if mn and (stats['min_ts'] is None or str(mn) < stats['min_ts']):
        stats['min_ts'] = str(mn)
      if mx and (stats['max_ts'] is None or str(mx) > stats['max_ts']):
        stats['max_ts'] = str(mx)
  return lane_stats


def _mint_lane_ephemeral_profile_token(operation_lane: str, seed_iso: str) -> str:
  seed = '{lane}|{ts}'.format(lane=operation_lane, ts=seed_iso)
  digest = hashlib.sha256(seed.encode('utf-8')).hexdigest()
  return f'{PROFILE_TOKEN_LANE_EPHEMERAL_PREFIX}{digest[-6:]}'


def _mint_lane_ephemeral_runtime_token(operation_lane: str) -> str:
  # Runtime mint adds nanosecond precision so back-to-back mints don't collide.
  import time as _time
  seed = '{lane}|{ns}'.format(lane=operation_lane, ns=_time.time_ns())
  digest = hashlib.sha256(seed.encode('utf-8')).hexdigest()
  return f'{PROFILE_TOKEN_LANE_EPHEMERAL_PREFIX}{digest[-6:]}'


def _backfill_lane_active_datapack(connection: sqlite3.Connection) -> None:
  """One-shot, idempotent backfill of lane_active_datapack for pre-migration rows.

  For each lane that still has rows bearing the sentinel token AND no existing
  lane_active_datapack row, mint exactly one `lane_ephemeral` token, seed a
  closed `lane_active_datapack` row, then rewrite the sentinel on every
  partitioned table for that lane.
  """
  lane_stats = _lanes_with_unbackfilled_rows(connection)
  if not lane_stats:
    return
  now_iso = _utc_now_iso()
  for lane, stats in lane_stats.items():
    existing = connection.execute(
      'SELECT 1 FROM lane_active_datapack WHERE operation_lane = ? LIMIT 1',
      (lane,),
    ).fetchone()
    if existing is not None:
      continue
    seed_iso = stats['min_ts'] or now_iso
    minted = _mint_lane_ephemeral_profile_token(lane, seed_iso)
    became_active = stats['min_ts'] or now_iso
    closed_at = stats['max_ts'] or now_iso
    detail = _json_detail({'backfilled': True, 'row_count': int(stats['count'])})
    connection.execute(
      '''
      INSERT INTO lane_active_datapack
      (operation_lane, profile_token, became_active_at_utc, closed_at_utc,
       closed_cause, mint_basis, detail_json, recorded_at_utc)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (lane, minted, became_active, closed_at, 'superseded_by_load',
       'lane_ephemeral', detail, now_iso),
    )
    for table in PROFILE_TOKEN_PARTITIONED_TABLES:
      columns = _table_columns(connection, table)
      if 'profile_token' not in columns or 'operation_lane' not in columns:
        continue
      connection.execute(
        'UPDATE {table} SET profile_token = ? '
        'WHERE operation_lane = ? AND profile_token = ?'.format(table=table),
        (minted, lane, PROFILE_TOKEN_UNBACKFILLED_SENTINEL),
      )


def resolve_active_profile_token(
  connection: sqlite3.Connection,
  operation_lane: str,
  *,
  key_path: str | None = None,
) -> str:
  """Return the active profile_token for the lane, minting one if none is open.

  Mint basis is `key_path_derived` when a key path is supplied, otherwise
  `lane_ephemeral`. The new row is inserted atomically; the partial unique
  index guarantees zero-or-one open row per lane.
  """
  lane = _normalize_operation_lane(operation_lane)
  row = connection.execute(
    'SELECT profile_token FROM lane_active_datapack '
    'WHERE operation_lane = ? AND closed_at_utc IS NULL LIMIT 1',
    (lane,),
  ).fetchone()
  if row is not None:
    return str(row['profile_token'])
  now_iso = _utc_now_iso()
  if key_path:
    minted = profile_token_for_key_path(key_path) or _mint_lane_ephemeral_runtime_token(lane)
    mint_basis = 'key_path_derived' if profile_token_for_key_path(key_path) else 'lane_ephemeral'
  else:
    minted = _mint_lane_ephemeral_runtime_token(lane)
    mint_basis = 'lane_ephemeral'
  connection.execute(
    '''
    INSERT INTO lane_active_datapack
    (operation_lane, profile_token, became_active_at_utc, mint_basis, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?)
    ''',
    (lane, minted, now_iso, mint_basis, now_iso),
  )
  return minted


def close_active_datapack(
  connection: sqlite3.Connection,
  operation_lane: str,
  *,
  closed_cause: str,
  extract_manifest_path: str | None = None,
  cli_command: str | None = None,
) -> str | None:
  """Close the open lane_active_datapack row for the lane. Returns the closed token or None."""
  if closed_cause not in LANE_ACTIVE_DATAPACK_CLOSED_CAUSES:
    raise ValueError(f'closed_cause must be one of {LANE_ACTIVE_DATAPACK_CLOSED_CAUSES}')
  lane = _normalize_operation_lane(operation_lane)
  row = connection.execute(
    'SELECT id, profile_token FROM lane_active_datapack '
    'WHERE operation_lane = ? AND closed_at_utc IS NULL LIMIT 1',
    (lane,),
  ).fetchone()
  if row is None:
    return None
  now_iso = _utc_now_iso()
  connection.execute(
    '''
    UPDATE lane_active_datapack
    SET closed_at_utc = ?, closed_cause = ?, extract_manifest_path = ?, cli_command = ?
    WHERE id = ?
    ''',
    (now_iso, closed_cause, extract_manifest_path, cli_command, int(row['id'])),
  )
  return str(row['profile_token'])


def _json_detail(detail: dict[str, Any] | None = None) -> str:
  return json.dumps(detail or {}, sort_keys=True, default=str)


def _json_load(value: str | None) -> dict[str, Any]:
  try:
    loaded = json.loads(value or '{}')
  except (TypeError, json.JSONDecodeError):
    return {}
  return loaded if isinstance(loaded, dict) else {}


def _utc_now_iso() -> str:
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def api_key_hash_for_id(api_key_id: str | None) -> str:
  normalized = str(api_key_id or '').strip().lower()
  if not normalized:
    raise ValueError('A names-only API key id is required to derive api_key_hash.')
  return hashlib.sha256(normalized.encode('utf-8')).hexdigest()[:16]


def profile_token_for_key_path(path_value: str | None) -> str | None:
  normalized = str(path_value or '').strip()
  if not normalized:
    return None
  resolved = str(Path(normalized).expanduser().resolve())
  digest = hashlib.sha256(resolved.encode('utf-8')).hexdigest()
  return f'{DATAPACK_PROFILE_TOKEN_PREFIX}{digest[-6:]}'


def _family_spec(family_id: str) -> dict[str, Any]:
  for spec in DATAPACK_FAMILY_SPECS:
    if str(spec.get('family_id')) == family_id:
      return dict(spec)
  raise KeyError(f'Unknown datapack family: {family_id}')


def _table_columns(connection: sqlite3.Connection, table: str) -> list[str]:
  rows = connection.execute("PRAGMA table_info('{table}')".format(table=table)).fetchall()
  return [str(row['name']) for row in rows]


def _rows_for_table(
  connection: sqlite3.Connection,
  *,
  table: str,
  operation_lane: str,
) -> list[dict[str, Any]]:
  lane = _normalize_operation_lane(operation_lane)
  if table == 'candidate_review_candidates':
    rows = connection.execute(
      '''
      SELECT c.*
      FROM candidate_review_candidates c
      JOIN candidate_review_runs r ON r.run_id = c.run_id
      WHERE r.operation_lane = ?
      ORDER BY c.id ASC
      ''',
      (lane,),
    ).fetchall()
  elif table == 'candidate_saved_set_members':
    rows = connection.execute(
      '''
      SELECT m.*
      FROM candidate_saved_set_members m
      JOIN candidate_saved_sets s ON s.saved_set_id = m.saved_set_id
      WHERE s.operation_lane = ?
      ORDER BY m.id ASC
      ''',
      (lane,),
    ).fetchall()
  elif table == 'candidate_saved_set_evaluations':
    rows = connection.execute(
      '''
      SELECT e.*
      FROM candidate_saved_set_evaluations e
      JOIN candidate_saved_sets s ON s.saved_set_id = e.saved_set_id
      WHERE s.operation_lane = ?
      ORDER BY e.id ASC
      ''',
      (lane,),
    ).fetchall()
  elif table in LANE_BEARING_TABLES:
    rows = connection.execute(
      'SELECT * FROM {table} WHERE operation_lane = ? ORDER BY rowid ASC'.format(table=table),
      (lane,),
    ).fetchall()
  else:
    rows = connection.execute(
      'SELECT * FROM {table} ORDER BY rowid ASC'.format(table=table)
    ).fetchall()
  return [dict(row) for row in rows]


def synthetic_refinement_fixture_family(*, operation_lane: str) -> dict[str, Any]:
  lane = _normalize_operation_lane(operation_lane)
  return {
    'family_id': 'synthetic_refinement_fixtures',
    'provenance': 'synthetic_refinement',
    'operation_lane': lane,
    'fixture_scenarios': [
      {
        'scenario_id': 'sparse_frontier',
        'label': 'Sparse frontier',
        'candidate_count': 4,
        'threshold_rank': 2,
        'scores': [0.88, 0.74, 0.41, 0.18],
        'margins': [0.29, 0.15, -0.08, -0.31],
      },
      {
        'scenario_id': 'crowded_elbow',
        'label': 'Crowded elbow',
        'candidate_count': 9,
        'threshold_rank': 5,
        'scores': [0.93, 0.88, 0.84, 0.81, 0.78, 0.52, 0.49, 0.45, 0.42],
        'margins': [0.33, 0.28, 0.24, 0.21, 0.18, -0.08, -0.11, -0.15, -0.18],
      },
      {
        'scenario_id': 'noisy_threshold',
        'label': 'Noisy threshold edge cases',
        'candidate_count': 7,
        'threshold_rank': 3,
        'scores': [0.77, 0.74, 0.71, 0.69, 0.68, 0.66, 0.61],
        'margins': [0.09, 0.06, 0.03, -0.01, -0.02, -0.04, -0.09],
      },
    ],
    'notes': 'Deterministic synthetic proof fixtures for Stage 2 datapack packaging and later renderer validation.',
  }


def _inventory_entry_by_family(inventory: list[dict[str, Any]], family_id: str) -> dict[str, Any] | None:
  for entry in inventory:
    if str(entry.get('family_id') or '').strip() == family_id:
      return entry
  return None


def _table_payload_for_family(
  payloads: dict[str, Any],
  *,
  family_id: str,
  table_name: str,
) -> dict[str, Any] | None:
  family_payload = payloads.get(family_id)
  if not isinstance(family_payload, dict):
    return None
  tables = family_payload.get('tables')
  if not isinstance(tables, dict):
    return None
  table_payload = tables.get(table_name)
  return table_payload if isinstance(table_payload, dict) else None


def _set_family_table_rows(
  payloads: dict[str, Any],
  *,
  family_id: str,
  table_name: str,
  rows: list[dict[str, Any]],
) -> None:
  table_payload = _table_payload_for_family(payloads, family_id=family_id, table_name=table_name)
  if table_payload is None:
    return
  table_payload['rows'] = rows


def _retimestamp_iso(iso_value: str, *, minute_offset: int) -> str:
  try:
    parsed = datetime.fromisoformat(str(iso_value).replace('Z', '+00:00'))
  except ValueError:
    parsed = datetime.now(timezone.utc)
  shifted = parsed + timedelta(minutes=minute_offset)
  return shifted.astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')


def _build_synthetic_candidate_rows_from_fixtures(
  fixture_scenarios: list[dict[str, Any]],
) -> list[dict[str, Any]]:
  rows: list[dict[str, Any]] = []
  rank = 1
  for scenario_index, scenario in enumerate(fixture_scenarios, start=1):
    scenario_id = str(scenario.get('scenario_id') or f'scenario-{scenario_index}')
    scores = scenario.get('scores') if isinstance(scenario.get('scores'), list) else []
    margins = scenario.get('margins') if isinstance(scenario.get('margins'), list) else []
    for item_index, score_value in enumerate(scores, start=1):
      score = float(score_value)
      margin = float(margins[item_index - 1]) if item_index - 1 < len(margins) else (score - 0.7)
      if margin >= 0:
        qualifier_tier = 'live_qualifying'
        selection_status = 'selected'
      elif margin >= -0.08:
        qualifier_tier = 'near_miss'
        selection_status = 'near_miss'
      else:
        qualifier_tier = 'sandbox_extended'
        selection_status = 'rejected'
      ticker = 'SYN-{scenario:02d}-{item:02d}'.format(scenario=scenario_index, item=item_index)
      density_weight = max(0.05, min(1.0, score))
      liquidity_score = max(0.1, min(2.0, (0.45 + (item_index * 0.08))))
      edge_net = margin + 0.12
      edge_gross = edge_net + 0.04
      rows.append(
        {
          'candidate_uid': '{scenario_id}:{ticker}:{rank}'.format(scenario_id=scenario_id, ticker=ticker, rank=rank),
          'candidate_key': hashlib.sha256('{ticker}:{rank}:{tier}'.format(ticker=ticker, rank=rank, tier=qualifier_tier).encode('utf-8')).hexdigest()[:16],
          'ticker': ticker,
          'rank': rank,
          'qualifier_tier': qualifier_tier,
          'review_row_origin': 'current',
          'feature_vector': {
            'edge_gross_per_contract': str(round(edge_gross, 6)),
            'edge_net_per_contract': str(round(edge_net, 6)),
            'liquidity_score': str(round(liquidity_score, 6)),
            'density_weight': str(round(density_weight, 6)),
            'projected_profit_dollars': str(round(max(0.0, edge_net * 12.0), 6)),
            'fee_drag_dollars': str(round(0.02 * 12.0, 6)),
            'seconds_to_close': int(max(75, 1080 - (rank * 27))),
            'timing_pressure': str(round(min(1.0, rank / 12.0), 6)),
            'sizing_pressure': str(round(min(1.0, rank / 10.0), 6)),
            'per_contract_spend': str(round(0.48 + (rank * 0.003), 6)),
            'qualifier_tier': qualifier_tier,
            'selection_status': selection_status,
          },
          'score_components': {
            'edge_strength': str(round(max(0.0, edge_gross / 0.06), 6)),
            'liquidity_depth': str(round(max(0.0, liquidity_score / 1.0), 6)),
            'density_weight': str(round(density_weight, 6)),
            'timing_pressure': str(round(min(1.0, rank / 12.0), 6)),
            'sizing_capacity': str(round(max(0.0, 1.0 - (rank * 0.03)), 6)),
          },
          'composite_score': {
            'weighted_score': str(round(score, 6)),
            'normalized_score': str(round(score, 6)),
            'threshold_margin': str(round(margin, 6)),
            'rank': rank,
            'score_model_version': 'ov-u5a-candidate-score.v1',
            'weight_vector_reference': 'weights-synthetic-refinement',
          },
          'threshold_outcome': {
            'selection_status': selection_status,
            'gross_edge_margin': str(round(edge_gross - 0.03, 6)),
            'net_profit_margin': str(round(edge_net - 0.05, 6)),
            'threshold_margin': str(round(margin, 6)),
            'passes_current_thresholds': margin >= 0,
            'selected_by_current_policy': margin >= 0,
          },
          'density_components': {
            'edge_reference': '0.06',
            'edge_ratio': str(round(max(0.0, edge_net / 0.06), 6)),
            'edge_weight': str(round(max(0.75, min(1.25, edge_net / 0.06 if edge_net > 0 else 0.75)), 6)),
            'liquidity_reference': '1.0',
            'liquidity_ratio': str(round(max(0.0, liquidity_score / 1.0), 6)),
            'liquidity_weight': str(round(max(0.75, min(1.25, liquidity_score / 1.0)), 6)),
          },
          'scenario_id': scenario_id,
        }
      )
      rank += 1
  return rows


def _synthetic_analytical_outputs(
  *,
  lane: str,
  recorded_at_utc: str,
  lane_session_id: str,
  candidate_rows: list[dict[str, Any]],
) -> dict[str, Any]:
  selected_rows = [row for row in candidate_rows if bool((row.get('threshold_outcome') or {}).get('selected_by_current_policy'))]
  near_miss_rows = [row for row in candidate_rows if str((row.get('feature_vector') or {}).get('selection_status') or '') == 'near_miss']
  top_rows = candidate_rows[:3]
  return {
    'candidate_math_evidence_contract': {
      'view_id': 'candidate_math_evidence_contract',
      'contract_version': 'ov-u5a-candidate-math-contract.v1',
      'model_reference': {
        'score_model_version': 'ov-u5a-candidate-score.v1',
        'weight_vector_reference': 'weights-synthetic-refinement',
        'component_weights': {
          'edge_strength': '0.35',
          'liquidity_depth': '0.25',
          'density_weight': '0.2',
          'timing_pressure': '0.1',
          'sizing_capacity': '0.1',
        },
      },
      'candidate_evidence_rows': candidate_rows,
      'authority_boundary': 'explanation_only_not_workflow_authority',
    },
    'candidate_density_curve': {
      'view_id': 'candidate_density_curve',
      'operation_lane': lane,
      'generated_at_utc': recorded_at_utc,
      'lane_session_id': lane_session_id,
      'series': [
        {
          'x': row.get('rank'),
          'y': (row.get('feature_vector') or {}).get('density_weight'),
          'ticker': row.get('ticker'),
          'qualifier_tier': row.get('qualifier_tier'),
        }
        for row in candidate_rows
      ],
    },
    'threshold_boundary_marker': {
      'view_id': 'threshold_boundary_marker',
      'operation_lane': lane,
      'generated_at_utc': recorded_at_utc,
      'tier_transition': {
        'transition_rank': (selected_rows[-1].get('rank') if selected_rows else None),
        'boundary_ticker': (selected_rows[-1].get('ticker') if selected_rows else None),
      },
    },
    'comparative_ranking_snapshot': {
      'view_id': 'comparative_ranking_snapshot',
      'top_rows': [
        {
          'ticker': row.get('ticker'),
          'rank': row.get('rank'),
          'qualifier_tier': row.get('qualifier_tier'),
          'density_weight': (row.get('feature_vector') or {}).get('density_weight'),
          'liquidity_score': (row.get('feature_vector') or {}).get('liquidity_score'),
          'edge_net_per_contract': (row.get('feature_vector') or {}).get('edge_net_per_contract'),
          'seconds_to_close': (row.get('feature_vector') or {}).get('seconds_to_close'),
        }
        for row in top_rows
      ],
      'near_miss_rows': [
        {
          'ticker': row.get('ticker'),
          'rank': row.get('rank'),
          'qualifier_tier': row.get('qualifier_tier'),
          'density_weight': (row.get('feature_vector') or {}).get('density_weight'),
          'liquidity_score': (row.get('feature_vector') or {}).get('liquidity_score'),
          'edge_net_per_contract': (row.get('feature_vector') or {}).get('edge_net_per_contract'),
          'seconds_to_close': (row.get('feature_vector') or {}).get('seconds_to_close'),
        }
        for row in near_miss_rows[:3]
      ],
    },
    'factor_contribution': {
      'view_id': 'factor_contribution',
      'candidate_rows': [
        {
          'ticker': row.get('ticker'),
          'rank': row.get('rank'),
          'qualifier_tier': row.get('qualifier_tier'),
          'edge_net_per_contract': (row.get('feature_vector') or {}).get('edge_net_per_contract'),
          'liquidity_score': (row.get('feature_vector') or {}).get('liquidity_score'),
          'density_weight': (row.get('feature_vector') or {}).get('density_weight'),
          'density_components': row.get('density_components'),
        }
        for row in candidate_rows[:5]
      ],
    },
    'parameter_sensitivity_delta': {
      'view_id': 'parameter_sensitivity_delta',
      'baseline_derived': {
        'effective_density': '0.58',
        'dynamic_pair_notional_pct': '0.16',
        'dynamic_max_contracts': '18',
        'binding_limiter': 'dynamic_notional_cap',
      },
      'scenarios': [
        {
          'scenario_id': 'increase_target_deployment_pct',
          'parameter': 'target_deployment_pct',
          'baseline_value': 0.3,
          'scenario_value': 0.35,
          'delta_value': 0.05,
          'derived_delta': {
            'dynamic_pair_notional_pct': '0.18',
            'dynamic_pair_notional_pct_delta': '0.02',
            'dynamic_max_contracts': '21',
            'dynamic_max_contracts_delta': '3',
            'binding_limiter': 'dynamic_notional_cap',
          },
        },
        {
          'scenario_id': 'decrease_target_deployment_pct',
          'parameter': 'target_deployment_pct',
          'baseline_value': 0.3,
          'scenario_value': 0.25,
          'delta_value': -0.05,
          'derived_delta': {
            'dynamic_pair_notional_pct': '0.14',
            'dynamic_pair_notional_pct_delta': '-0.02',
            'dynamic_max_contracts': '15',
            'dynamic_max_contracts_delta': '-3',
            'binding_limiter': 'dynamic_notional_cap',
          },
        },
      ],
    },
    'advisory_parameter_adjustment': {
      'view_id': 'advisory_parameter_adjustment',
      'recommendation_status': 'review_increase',
      'parameter': 'target_deployment_pct',
      'current_value': 0.3,
      'recommended_value': 0.35,
      'reason_summary': 'Synthetic replay indicates additional deployable room while preserving bounded controls.',
    },
    'dependency_group_recommendations': {
      'view_id': 'dependency_group_recommendations',
      'advisory_only': True,
      'no_auto_apply': True,
      'groups': [],
      'freshness_reference': {
        'generated_at_utc': recorded_at_utc,
        'lane_session_id': lane_session_id,
        'source_population_scope': 'synthetic_refinement_seed',
      },
    },
  }


def _inject_synthetic_mature_timeline(
  *,
  lane: str,
  created_at: str,
  payloads: dict[str, Any],
  inventory: list[dict[str, Any]],
) -> None:
  fixture_payload = payloads.get('synthetic_refinement_fixtures')
  if not isinstance(fixture_payload, dict):
    return
  fixture_scenarios = fixture_payload.get('fixture_scenarios') if isinstance(fixture_payload.get('fixture_scenarios'), list) else []
  if not fixture_scenarios:
    return

  runtime_entry = _inventory_entry_by_family(inventory, 'runtime_state')
  if runtime_entry is None or int(runtime_entry.get('row_count') or 0) <= 0:
    return

  analytical_table = _table_payload_for_family(payloads, family_id='analytical_state', table_name='analytical_snapshots')
  candidate_runs_table = _table_payload_for_family(payloads, family_id='candidate_review_history', table_name='candidate_review_runs')
  candidate_rows_table = _table_payload_for_family(payloads, family_id='candidate_review_history', table_name='candidate_review_candidates')
  saved_sets_table = _table_payload_for_family(payloads, family_id='candidate_review_history', table_name='candidate_saved_sets')
  saved_members_table = _table_payload_for_family(payloads, family_id='candidate_review_history', table_name='candidate_saved_set_members')
  saved_eval_table = _table_payload_for_family(payloads, family_id='candidate_review_history', table_name='candidate_saved_set_evaluations')
  runtime_events_table = _table_payload_for_family(payloads, family_id='runtime_state', table_name='runtime_events')
  service_heartbeats_table = _table_payload_for_family(payloads, family_id='runtime_state', table_name='service_heartbeats')
  operator_actions_table = _table_payload_for_family(payloads, family_id='runtime_state', table_name='operator_actions')

  if any(table is None for table in (
    analytical_table,
    candidate_runs_table,
    candidate_rows_table,
    saved_sets_table,
    saved_members_table,
    saved_eval_table,
    runtime_events_table,
    service_heartbeats_table,
    operator_actions_table,
  )):
    return

  candidate_rows = _build_synthetic_candidate_rows_from_fixtures([dict(item) for item in fixture_scenarios if isinstance(item, dict)])
  if not candidate_rows:
    return

  run_rows: list[dict[str, Any]] = []
  review_candidate_rows: list[dict[str, Any]] = []
  saved_set_rows: list[dict[str, Any]] = []
  saved_member_rows: list[dict[str, Any]] = []
  saved_eval_rows: list[dict[str, Any]] = []
  runtime_rows: list[dict[str, Any]] = []
  heartbeat_rows: list[dict[str, Any]] = []
  operator_rows: list[dict[str, Any]] = []
  analytical_rows: list[dict[str, Any]] = []

  member_row_id = 1
  for scenario_index, scenario in enumerate(fixture_scenarios, start=1):
    scenario_id = str((scenario if isinstance(scenario, dict) else {}).get('scenario_id') or f'scenario-{scenario_index}')
    run_id = 'synthetic-refinement:{lane}:{scenario_id}'.format(lane=lane, scenario_id=scenario_id)
    lane_session_id = 'synthetic-refinement-{scenario:02d}'.format(scenario=scenario_index)
    recorded_at_utc = _retimestamp_iso(created_at, minute_offset=scenario_index * 9)
    scenario_candidate_rows = [row for row in candidate_rows if str(row.get('scenario_id') or '') == scenario_id]
    selected_rows = [
      row for row in scenario_candidate_rows
      if bool((row.get('threshold_outcome') or {}).get('selected_by_current_policy'))
    ]
    candidate_signature = hashlib.sha256(
      json.dumps([row.get('candidate_uid') for row in scenario_candidate_rows], sort_keys=True, default=str).encode('utf-8')
    ).hexdigest()
    analytical_outputs = _synthetic_analytical_outputs(
      lane=lane,
      recorded_at_utc=recorded_at_utc,
      lane_session_id=lane_session_id,
      candidate_rows=scenario_candidate_rows,
    )

    run_rows.append(
      {
        'run_id': run_id,
        'operation_lane': lane,
        'lane_session_id': lane_session_id,
        'candidate_signature': candidate_signature,
        'candidate_count': len(scenario_candidate_rows),
        'source_action': 'synthetic_refinement_seed',
        'detail_json': _json_detail({'scenario_id': scenario_id, 'source': 'synthetic_refinement_timeline'}),
        'recorded_at_utc': recorded_at_utc,
      }
    )
    for row in scenario_candidate_rows:
      review_candidate_rows.append(
        {
          'run_id': run_id,
          'candidate_uid': row.get('candidate_uid'),
          'candidate_key': row.get('candidate_key'),
          'ticker': row.get('ticker'),
          'qualifier_tier': row.get('qualifier_tier'),
          'review_row_origin': row.get('review_row_origin') or 'current',
          'detail_json': _json_detail(
            {
              'feature_vector': row.get('feature_vector'),
              'score_components': row.get('score_components'),
              'composite_score': row.get('composite_score'),
              'threshold_outcome': row.get('threshold_outcome'),
              'density_components': row.get('density_components'),
            }
          ),
          'recorded_at_utc': recorded_at_utc,
        }
      )

    saved_set_id = 'saved-set:{scenario_id}'.format(scenario_id=scenario_id)
    selected_keys = [str(row.get('candidate_key') or '') for row in selected_rows if str(row.get('candidate_key') or '').strip()]
    saved_set_rows.append(
      {
        'saved_set_id': saved_set_id,
        'run_id': run_id,
        'operation_lane': lane,
        'lane_session_id': lane_session_id,
        'saved_key_count': len(selected_keys),
        'state_id': 'synthetic_snapshot',
        'source_action': 'synthetic_refinement_seed',
        'detail_json': _json_detail({'scenario_id': scenario_id, 'member_keys': selected_keys}),
        'recorded_at_utc': recorded_at_utc,
      }
    )
    for member_order, row in enumerate(selected_rows, start=1):
      saved_member_rows.append(
        {
          'id': member_row_id,
          'saved_set_id': saved_set_id,
          'candidate_uid': row.get('candidate_uid'),
          'candidate_key': row.get('candidate_key'),
          'member_order': member_order,
          'detail_json': _json_detail({'scenario_id': scenario_id, 'ticker': row.get('ticker')}),
          'recorded_at_utc': recorded_at_utc,
        }
      )
      member_row_id += 1
    saved_eval_rows.append(
      {
        'saved_set_id': saved_set_id,
        'evaluation_status': 'evaluated',
        'actionability_status': 'revalidation_required' if scenario_index % 2 else 'offline_limited',
        'visibility_status': 'visible',
        'offline_verifiable': 1,
        'online_revalidation_required': 1,
        'detail_json': _json_detail({'scenario_id': scenario_id, 'selected_count': len(selected_rows)}),
        'recorded_at_utc': recorded_at_utc,
      }
    )

    runtime_rows.append(
      {
        'level': 'INFO',
        'event_type': 'scan_complete',
        'pair_id': None,
        'operation_lane': lane,
        'lane_session_id': lane_session_id,
        'detail_json': _json_detail(
          {
            'candidate_count': len(scenario_candidate_rows),
            'orderbook_enrichment_count': len(scenario_candidate_rows),
            'effective_density': '0.58',
            'dynamic_pair_notional_pct': '0.16',
            'analytical_outputs': analytical_outputs,
          }
        ),
        'recorded_at_utc': recorded_at_utc,
      }
    )
    heartbeat_rows.append(
      {
        'component': 'runtime-loop',
        'status': 'cycle-complete',
        'operation_lane': lane,
        'lane_session_id': lane_session_id,
        'detail_json': _json_detail(
          {
            'candidate_count': len(scenario_candidate_rows),
            'planned_pair_count': 1,
            'effective_density': '0.58',
            'dynamic_pair_notional_pct': '0.16',
            'binding_limiter': 'dynamic_notional_cap',
            'analytical_outputs': analytical_outputs,
          }
        ),
        'recorded_at_utc': recorded_at_utc,
      }
    )
    operator_rows.append(
      {
        'action': 'scan-once',
        'pair_id': None,
        'operation_lane': lane,
        'lane_session_id': lane_session_id,
        'detail_json': _json_detail({'scenario_id': scenario_id, 'candidate_count': len(scenario_candidate_rows)}),
        'recorded_at_utc': recorded_at_utc,
      }
    )
    analytical_rows.append(
      {
        'operation_lane': lane,
        'lane_session_id': lane_session_id,
        'snapshot_type': 'candidate_math_contract',
        'evidence_class': 'synthetic_refinement',
        'detail_json': _json_detail(analytical_outputs),
        'recorded_at_utc': recorded_at_utc,
      }
    )

  _set_family_table_rows(payloads, family_id='candidate_review_history', table_name='candidate_review_runs', rows=run_rows)
  _set_family_table_rows(payloads, family_id='candidate_review_history', table_name='candidate_review_candidates', rows=review_candidate_rows)
  _set_family_table_rows(payloads, family_id='candidate_review_history', table_name='candidate_saved_sets', rows=saved_set_rows)
  _set_family_table_rows(payloads, family_id='candidate_review_history', table_name='candidate_saved_set_members', rows=saved_member_rows)
  _set_family_table_rows(payloads, family_id='candidate_review_history', table_name='candidate_saved_set_evaluations', rows=saved_eval_rows)
  _set_family_table_rows(payloads, family_id='runtime_state', table_name='runtime_events', rows=runtime_rows)
  _set_family_table_rows(payloads, family_id='runtime_state', table_name='service_heartbeats', rows=heartbeat_rows)
  _set_family_table_rows(payloads, family_id='runtime_state', table_name='operator_actions', rows=operator_rows)
  _set_family_table_rows(payloads, family_id='analytical_state', table_name='analytical_snapshots', rows=analytical_rows)

  for entry in inventory:
    family_id = str(entry.get('family_id') or '').strip()
    family_payload = payloads.get(family_id)
    if not isinstance(family_payload, dict):
      continue
    tables = family_payload.get('tables')
    if not isinstance(tables, dict):
      continue
    row_count = 0
    for table_payload in tables.values():
      if not isinstance(table_payload, dict):
        continue
      rows = table_payload.get('rows') if isinstance(table_payload.get('rows'), list) else []
      row_count += len(rows)
    entry['row_count'] = row_count


def build_datapack_bundle(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  datapack_type: str,
  api_key_hash: str,
  profile_token: str | None = None,
  created_at_utc: str | None = None,
  state_db_path_tail: str | None = None,
  source_label: str = 'local_state_db',
  include_synthetic_refinement: bool = False,
) -> dict[str, Any]:
  lane = _normalize_operation_lane(operation_lane)
  created_at = str(created_at_utc or _utc_now_iso())
  normalized_profile_token = str(profile_token or '').strip() or None
  payloads: dict[str, Any] = {}
  inventory: list[dict[str, Any]] = []

  for spec in DATAPACK_FAMILY_SPECS:
    family_id = str(spec['family_id'])
    tables = tuple(spec.get('tables', ()))
    include_family = bool(tables) or (family_id == 'synthetic_refinement_fixtures' and include_synthetic_refinement)
    inventory_entry = {
      'family_id': family_id,
      'classification': spec['classification'],
      'packaging_mode': spec['packaging_mode'],
      'restore_mode': spec['restore_mode'],
      'purge_eligible': bool(spec['purge_eligible']),
      'revalidation_required': bool(spec['revalidation_required']),
      'included': include_family,
      'payload_path': f'payloads/{family_id}.json' if include_family else None,
      'tables': list(tables),
      'row_count': 0,
    }
    if family_id == 'synthetic_refinement_fixtures' and include_synthetic_refinement:
      payload = synthetic_refinement_fixture_family(operation_lane=lane)
      inventory_entry['row_count'] = len(payload.get('fixture_scenarios', []))
      payloads[family_id] = payload
    elif tables:
      table_payloads: dict[str, Any] = {}
      row_count = 0
      for table in tables:
        rows = _rows_for_table(connection, table=table, operation_lane=lane)
        row_count += len(rows)
        table_payloads[table] = {
          'columns': _table_columns(connection, table),
          'rows': rows,
        }
      inventory_entry['row_count'] = row_count
      payloads[family_id] = {
        'family_id': family_id,
        'operation_lane': lane,
        'tables': table_payloads,
      }
    else:
      inventory_entry['reason'] = 'family_is_reference_only_or_excluded'
    inventory.append(inventory_entry)

  if str(datapack_type).strip() == 'synthetic_refinement' and include_synthetic_refinement:
    _inject_synthetic_mature_timeline(
      lane=lane,
      created_at=created_at,
      payloads=payloads,
      inventory=inventory,
    )

  manifest = {
    'schema_version': DATAPACK_SCHEMA_VERSION,
    'datapack_type': str(datapack_type).strip() or 'session_snapshot',
    'created_at_utc': created_at,
    'provenance': {
      'source_label': source_label,
      'state_db_path_tail': str(state_db_path_tail or '').strip() or None,
      'extract_first_reset_boundary': True,
      'synthetic_proof_supported': True,
    },
    'operation_lane': lane,
    'api_key_hash': str(api_key_hash).strip(),
    'profile_token': normalized_profile_token,
    'inventory': inventory,
    'checksums': {},
    'cross_key_import_default': 'fail_closed',
    'gui_force_rebind_allowed': False,
    'fresh_post_reset_accumulation_route': 'fresh_post_reset_runtime_accumulation',
  }
  restore_policy = {
    'schema_version': DATAPACK_SCHEMA_VERSION,
    'operation_lane': lane,
    'api_key_hash': str(api_key_hash).strip(),
    'profile_token': normalized_profile_token,
    'default_import_policy': {
      'cross_key_behavior': 'fail_closed',
      'force_rebind_flag': '--force-rebind-api-key-hash',
      'gui_force_rebind_allowed': False,
      'revalidation_required_after_force_rebind': True,
    },
    'family_policies': [
      {
        'family_id': str(item['family_id']),
        'classification': item['classification'],
        'restore_mode': item['restore_mode'],
        'purge_eligible': bool(item['purge_eligible']),
        'revalidation_required': bool(item['revalidation_required']),
        'included': bool(item['included']),
      }
      for item in inventory
    ],
  }
  return {
    'manifest': manifest,
    'restore_policy': restore_policy,
    'payloads': payloads,
  }


def serialize_datapack_json(payload: Any) -> str:
  return json.dumps(payload, indent=2, default=str)


def datapack_payload_checksum(payload: Any) -> str:
  return hashlib.sha256(serialize_datapack_json(payload).encode('utf-8')).hexdigest()


def datapack_manifest_checksum_payload(manifest: dict[str, Any]) -> dict[str, Any]:
  manifest_payload = json.loads(json.dumps(manifest))
  # The manifest self-checksum and the manifest signature are both derived/appended integrity
  # artifacts; exclude them so the manifest checksum is stable whether or not the manifest has been
  # signed (Lane L5c: signing is applied after the checksum is computed).
  manifest_payload.pop('signature', None)
  checksums = manifest_payload.get('checksums')
  if isinstance(checksums, dict):
    next_checksums = dict(checksums)
    next_checksums.pop('manifest.json', None)
    manifest_payload['checksums'] = next_checksums
  return manifest_payload


def datapack_manifest_checksum(manifest: dict[str, Any]) -> str:
  return datapack_payload_checksum(datapack_manifest_checksum_payload(manifest))


def _datapack_manifest_signature_payload(manifest: dict[str, Any]) -> dict[str, Any]:
  """Deterministic manifest payload bound by the signature: the checksum payload minus the signature.

  Reuses the checksum normalization (drops the manifest self-checksum) so the signature covers the
  full manifest content (lane, inventory, every payload checksum) but not its own signature block.
  """
  payload = datapack_manifest_checksum_payload(manifest)
  payload.pop('signature', None)
  return payload


def sign_datapack_manifest(manifest: dict[str, Any]) -> dict[str, Any]:
  """Attach an Ed25519 signature block over the manifest (Lane L5c; Class-2).

  The bundle payloads stay checksum-only; signing the manifest authenticates the whole datapack via
  its integrity root. Fail closed: with no signing key the block is an explicit ``unsigned`` marker.
  """
  manifest['signature'] = signed_evidence.sign_evidence_record(_datapack_manifest_signature_payload(manifest))
  return manifest


def verify_datapack_manifest_signature(manifest: dict[str, Any]) -> tuple[str, str | None]:
  """Verify the manifest signature. Returns ``(status, failure_code)``.

  status: ``verified`` | ``invalid`` | ``absent``. A present-but-invalid signature is an integrity
  failure (tamper/forge); an absent signature is reported for legacy datapacks but is not a hard
  failure here, so pre-signing bundles still load. Tighten to require-signed once all datapacks are
  re-minted.
  """
  signature_block = manifest.get('signature')
  if not isinstance(signature_block, dict) or not signature_block:
    return 'absent', None
  valid, code = signed_evidence.verify_evidence_record(
    _datapack_manifest_signature_payload(manifest),
    signature_block,
  )
  return ('verified', None) if valid else ('invalid', code)


def _load_datapack_json_file(path: Path) -> dict[str, Any] | None:
  try:
    payload = json.loads(path.read_text(encoding='utf-8'))
  except (OSError, json.JSONDecodeError):
    return None
  return payload if isinstance(payload, dict) else None


def validate_datapack_artifacts(
  root: str | Path,
  manifest: dict[str, Any],
  restore_policy: dict[str, Any],
) -> list[str]:
  issues: list[str] = []
  root_path = Path(root)

  checksums = manifest.get('checksums')
  inventory = manifest.get('inventory')
  family_policies = restore_policy.get('family_policies')
  if not isinstance(checksums, dict):
    return ['manifest_invalid:checksums']
  if not isinstance(inventory, list):
    return ['manifest_invalid:inventory']
  if not isinstance(family_policies, list):
    return ['restore_policy_invalid:family_policies']

  expected_control_checksums = {
    'manifest.json': datapack_manifest_checksum(manifest),
    'restore_policy.json': datapack_payload_checksum(restore_policy),
  }
  for relative_path, expected_checksum in expected_control_checksums.items():
    actual_checksum = str(checksums.get(relative_path) or '').strip()
    if not actual_checksum:
      issues.append(f'checksum_missing:{relative_path}')
    elif actual_checksum != expected_checksum:
      issues.append(f'checksum_mismatch:{relative_path}')
    if not (root_path / Path(relative_path)).exists():
      issues.append(f'artifact_missing:{relative_path}')

  # Lane L5c: a present manifest signature must verify (tamper/forge detection); an absent signature
  # is tolerated so legacy unsigned datapacks still load.
  signature_status, signature_code = verify_datapack_manifest_signature(manifest)
  if signature_status == 'invalid':
    issues.append(f'signature_invalid:manifest.json:{signature_code}')

  policy_by_family: dict[str, dict[str, Any]] = {}
  for family_policy in family_policies:
    if not isinstance(family_policy, dict):
      issues.append('restore_policy_invalid:family_policy_entry')
      continue
    family_id = str(family_policy.get('family_id') or '').strip()
    if not family_id:
      issues.append('restore_policy_invalid:family_policy_missing_id')
      continue
    policy_by_family[family_id] = family_policy

  seen_families: set[str] = set()
  manifest_lane = str(manifest.get('operation_lane') or '').strip().lower()
  for inventory_entry in inventory:
    if not isinstance(inventory_entry, dict):
      issues.append('manifest_invalid:inventory_entry')
      continue
    family_id = str(inventory_entry.get('family_id') or '').strip()
    if not family_id:
      issues.append('manifest_invalid:inventory_family_missing_id')
      continue
    if family_id in seen_families:
      issues.append(f'manifest_duplicate_family:{family_id}')
      continue
    seen_families.add(family_id)

    try:
      spec = _family_spec(family_id)
    except KeyError:
      issues.append(f'manifest_unknown_family:{family_id}')
      continue

    if inventory_entry.get('classification') != spec['classification']:
      issues.append(f'inventory_classification_mismatch:{family_id}')
    if inventory_entry.get('packaging_mode') != spec['packaging_mode']:
      issues.append(f'inventory_packaging_mode_mismatch:{family_id}')
    if inventory_entry.get('restore_mode') != spec['restore_mode']:
      issues.append(f'inventory_restore_mode_mismatch:{family_id}')
    if bool(inventory_entry.get('purge_eligible')) != bool(spec['purge_eligible']):
      issues.append(f'inventory_purge_flag_mismatch:{family_id}')
    if bool(inventory_entry.get('revalidation_required')) != bool(spec['revalidation_required']):
      issues.append(f'inventory_revalidation_flag_mismatch:{family_id}')

    family_policy = policy_by_family.get(family_id)
    if family_policy is None:
      issues.append(f'restore_policy_missing_family:{family_id}')
    else:
      if family_policy.get('classification') != inventory_entry.get('classification'):
        issues.append(f'restore_policy_classification_mismatch:{family_id}')
      if family_policy.get('restore_mode') != inventory_entry.get('restore_mode'):
        issues.append(f'restore_policy_restore_mode_mismatch:{family_id}')
      if bool(family_policy.get('purge_eligible')) != bool(inventory_entry.get('purge_eligible')):
        issues.append(f'restore_policy_purge_flag_mismatch:{family_id}')
      if bool(family_policy.get('revalidation_required')) != bool(inventory_entry.get('revalidation_required')):
        issues.append(f'restore_policy_revalidation_flag_mismatch:{family_id}')
      if bool(family_policy.get('included')) != bool(inventory_entry.get('included')):
        issues.append(f'restore_policy_included_flag_mismatch:{family_id}')

    if not bool(inventory_entry.get('included')):
      continue

    payload_path_value = str(inventory_entry.get('payload_path') or '').strip()
    if not payload_path_value:
      issues.append(f'inventory_missing_payload_path:{family_id}')
      continue
    payload_path = root_path / Path(payload_path_value)
    expected_checksum = str(checksums.get(payload_path_value) or '').strip()
    if not expected_checksum:
      issues.append(f'checksum_missing:{payload_path_value}')
    if not payload_path.exists():
      issues.append(f'artifact_missing:{payload_path_value}')
      continue
    payload = _load_datapack_json_file(payload_path)
    if payload is None:
      issues.append(f'payload_decode_failed:{payload_path_value}')
      continue

    actual_payload_checksum = datapack_payload_checksum(payload)
    if expected_checksum and actual_payload_checksum != expected_checksum:
      issues.append(f'checksum_mismatch:{payload_path_value}')

    if str(payload.get('family_id') or '').strip() != family_id:
      issues.append(f'payload_family_id_mismatch:{family_id}')
    if str(payload.get('operation_lane') or '').strip().lower() != manifest_lane:
      issues.append(f'payload_operation_lane_mismatch:{family_id}')

    row_count = inventory_entry.get('row_count')
    if not isinstance(row_count, int):
      issues.append(f'inventory_row_count_invalid:{family_id}')
      continue

    if family_id == 'synthetic_refinement_fixtures':
      fixture_scenarios = payload.get('fixture_scenarios')
      if not isinstance(fixture_scenarios, list):
        issues.append(f'payload_fixture_scenarios_invalid:{family_id}')
        continue
      if len(fixture_scenarios) != row_count:
        issues.append(f'inventory_row_count_mismatch:{family_id}')
      continue

    tables = payload.get('tables')
    if not isinstance(tables, dict):
      issues.append(f'payload_tables_invalid:{family_id}')
      continue

    expected_tables = tuple(spec.get('tables', ()))
    total_rows = 0
    for table in expected_tables:
      if table not in tables:
        issues.append(f'payload_missing_table:{family_id}:{table}')
        continue
      table_payload = tables.get(table)
      if not isinstance(table_payload, dict):
        issues.append(f'payload_table_invalid:{family_id}:{table}')
        continue
      columns = table_payload.get('columns')
      rows = table_payload.get('rows')
      if not isinstance(columns, list):
        issues.append(f'payload_columns_invalid:{family_id}:{table}')
      if not isinstance(rows, list):
        issues.append(f'payload_rows_invalid:{family_id}:{table}')
        continue
      total_rows += len(rows)
    for table in tables:
      if table not in expected_tables:
        issues.append(f'payload_unexpected_table:{family_id}:{table}')
    if total_rows != row_count:
      issues.append(f'inventory_row_count_mismatch:{family_id}')

  for family_id in policy_by_family:
    if family_id not in seen_families:
      issues.append(f'restore_policy_orphan_family:{family_id}')
  return issues


def evaluate_datapack_convergence(
  manifest: dict[str, Any],
  restore_policy: dict[str, Any],
) -> dict[str, Any]:
  inventory = manifest.get('inventory') if isinstance(manifest.get('inventory'), list) else []
  family_policies = restore_policy.get('family_policies') if isinstance(restore_policy.get('family_policies'), list) else []

  manifest_family_ids: list[str] = []
  included_table_row_count = 0
  included_family_ids: list[str] = []
  for inventory_entry in inventory:
    if not isinstance(inventory_entry, dict):
      continue
    family_id = str(inventory_entry.get('family_id') or '').strip()
    if not family_id:
      continue
    manifest_family_ids.append(family_id)
    if bool(inventory_entry.get('included')):
      included_family_ids.append(family_id)
      if inventory_entry.get('tables'):
        try:
          included_table_row_count += int(inventory_entry.get('row_count') or 0)
        except Exception:
          continue

  policy_family_ids: list[str] = []
  for family_policy in family_policies:
    if not isinstance(family_policy, dict):
      continue
    family_id = str(family_policy.get('family_id') or '').strip()
    if family_id:
      policy_family_ids.append(family_id)

  expected_family_ids = [str(spec['family_id']) for spec in DATAPACK_FAMILY_SPECS]
  baseline_inventory_match = manifest_family_ids == expected_family_ids
  baseline_policy_match = policy_family_ids == expected_family_ids
  baseline_family_coverage = baseline_inventory_match and baseline_policy_match
  if not baseline_family_coverage:
    convergence_class = 'non_convergent_no_go'
  elif included_table_row_count <= 0:
    convergence_class = 'proof_only_non_loadable'
  else:
    convergence_class = 'baseline_convergent'

  return {
    'convergence_class': convergence_class,
    'baseline_family_coverage': baseline_family_coverage,
    'included_family_ids': included_family_ids,
    'expected_family_ids': expected_family_ids,
    'manifest_family_ids': manifest_family_ids,
    'policy_family_ids': policy_family_ids,
    'included_table_row_count': included_table_row_count,
  }


def validate_datapack_controls(
  manifest: dict[str, Any],
  restore_policy: dict[str, Any],
) -> list[str]:
  issues: list[str] = []
  required_manifest_fields = (
    'schema_version',
    'datapack_type',
    'created_at_utc',
    'provenance',
    'operation_lane',
    'api_key_hash',
    'inventory',
    'checksums',
  )
  for field in required_manifest_fields:
    if field not in manifest:
      issues.append(f'manifest_missing:{field}')
  if 'default_import_policy' not in restore_policy:
    issues.append('restore_policy_missing:default_import_policy')
  if 'family_policies' not in restore_policy:
    issues.append('restore_policy_missing:family_policies')
  if str(manifest.get('api_key_hash') or '').strip() != str(restore_policy.get('api_key_hash') or '').strip():
    issues.append('identity_mismatch:api_key_hash')
  if str(manifest.get('operation_lane') or '').strip() != str(restore_policy.get('operation_lane') or '').strip():
    issues.append('identity_mismatch:operation_lane')
  return issues


def evaluate_datapack_identity(
  manifest: dict[str, Any],
  *,
  active_operation_lane: str,
  active_api_key_hash: str,
) -> dict[str, Any]:
  active_lane = _normalize_operation_lane(active_operation_lane)
  manifest_lane = _normalize_operation_lane(str(manifest.get('operation_lane') or active_lane))
  manifest_hash = str(manifest.get('api_key_hash') or '').strip()
  active_hash = str(active_api_key_hash or '').strip()
  lane_match = manifest_lane == active_lane
  api_key_hash_match = manifest_hash == active_hash
  allowed = lane_match and api_key_hash_match
  reasons: list[str] = []
  if not lane_match:
    reasons.append('operation_lane_mismatch')
  if not api_key_hash_match:
    reasons.append('api_key_hash_mismatch')
  return {
    'allowed': allowed,
    'operation_lane_match': lane_match,
    'api_key_hash_match': api_key_hash_match,
    'reasons': reasons,
  }


def rebind_datapack_controls(
  manifest: dict[str, Any],
  restore_policy: dict[str, Any],
  *,
  new_api_key_hash: str,
  rebound_at_utc: str | None = None,
  profile_token: str | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
  rebound_at = str(rebound_at_utc or _utc_now_iso())
  rebound_hash = str(new_api_key_hash or '').strip()
  if not rebound_hash:
    raise ValueError('new_api_key_hash is required for CLI-only datapack rebind.')

  next_manifest = json.loads(json.dumps(manifest))
  next_restore_policy = json.loads(json.dumps(restore_policy))
  prior_hash = str(next_manifest.get('api_key_hash') or '').strip()
  next_manifest['api_key_hash'] = rebound_hash
  next_manifest['restored_under_key_hash'] = rebound_hash
  next_manifest['revalidation_required'] = True
  next_manifest['profile_token'] = str(profile_token or next_manifest.get('profile_token') or '').strip() or None
  next_manifest.setdefault('rebind_audit', []).append(
    {
      'prior_api_key_hash': prior_hash,
      'restored_under_key_hash': rebound_hash,
      'rebound_at_utc': rebound_at,
      'mode': 'cli_force_rebind',
    }
  )

  next_restore_policy['api_key_hash'] = rebound_hash
  default_import_policy = next_restore_policy.setdefault('default_import_policy', {})
  default_import_policy['last_force_rebind_at_utc'] = rebound_at
  default_import_policy['revalidation_required_after_force_rebind'] = True
  for family_policy in next_restore_policy.get('family_policies', []):
    restore_mode = str(family_policy.get('restore_mode') or '')
    if restore_mode not in {'proof_only', 'retain_in_place', 'never_package'}:
      family_policy['revalidation_required'] = True
  return next_manifest, next_restore_policy


def record_market_seen(
  connection: sqlite3.Connection,
  *,
  ticker: str,
  status: str,
  close_time_utc: str | None,
  last_seen_at_utc: str,
) -> None:
  with connection:
    connection.execute(
      '''
      INSERT INTO markets_seen (ticker, status, close_time_utc, last_seen_at_utc)
      VALUES (?, ?, ?, ?)
      ON CONFLICT(ticker) DO UPDATE SET
        status=excluded.status,
        close_time_utc=excluded.close_time_utc,
        last_seen_at_utc=excluded.last_seen_at_utc
      ''',
      (ticker, status, close_time_utc, last_seen_at_utc),
    )


def persist_pair_plan(
  connection: sqlite3.Connection,
  plan: PairOrderPlan,
  *,
  created_at_utc: str,
  operation_lane: str,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT OR REPLACE INTO pair_plans (
        pair_id,
        ticker,
        yes_price_dollars,
        no_price_dollars,
        contract_count,
        yes_client_order_id,
        no_client_order_id,
        time_in_force,
        post_only,
        cancel_order_on_pause,
        subaccount,
        operation_lane,
        created_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        plan.pair_id,
        plan.ticker,
        str(plan.yes_price),
        str(plan.no_price),
        str(plan.contract_count),
        plan.yes_client_order_id,
        plan.no_client_order_id,
        plan.time_in_force,
        int(plan.post_only),
        int(plan.cancel_order_on_pause),
        plan.subaccount,
        lane,
        created_at_utc,
      ),
    )
    connection.executemany(
      '''
      INSERT OR REPLACE INTO orders (
        order_id,
        pair_id,
        client_order_id,
        side,
        price_dollars,
        contract_count,
        status,
        operation_lane,
        created_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        (
          '{pair_id}:yes'.format(pair_id=plan.pair_id),
          plan.pair_id,
          plan.yes_client_order_id,
          'yes',
          str(plan.yes_price),
          str(plan.contract_count),
          'planned',
          lane,
          created_at_utc,
        ),
        (
          '{pair_id}:no'.format(pair_id=plan.pair_id),
          plan.pair_id,
          plan.no_client_order_id,
          'no',
          str(plan.no_price),
          str(plan.contract_count),
          'planned',
          lane,
          created_at_utc,
        ),
      ),
    )


def persist_order_statuses(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  statuses: list[dict[str, Any]],
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  rows = [
    (
      str(item.get('status') or '').strip().lower(),
      str(item.get('order_id') or '').strip(),
      lane,
    )
    for item in statuses
    if str(item.get('order_id') or '').strip() and str(item.get('status') or '').strip()
  ]
  if not rows:
    return
  with connection:
    connection.executemany(
      '''
      UPDATE orders
      SET status = ?
      WHERE order_id = ? AND operation_lane = ?
      ''',
      rows,
    )


def promote_order_id(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  pair_id: str,
  client_order_id: str,
  side: str,
  remote_order_id: str,
  status: str | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  clean_remote_order_id = str(remote_order_id or '').strip()
  clean_status = str(status or '').strip().lower()
  if not clean_remote_order_id:
    return
  with connection:
    connection.execute(
      '''
      UPDATE orders
      SET order_id = ?,
          status = CASE WHEN ? != '' THEN ? ELSE status END
      WHERE pair_id = ?
        AND client_order_id = ?
        AND side = ?
        AND operation_lane = ?
      ''',
      (
        clean_remote_order_id,
        clean_status,
        clean_status,
        pair_id,
        client_order_id,
        side,
        lane,
      ),
    )


def persist_fill(connection: sqlite3.Connection, fill: FillEvent, *, operation_lane: str) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT OR REPLACE INTO fills (
        fill_id,
        pair_id,
        order_id,
        client_order_id,
        side,
        price_dollars,
        contract_count,
        fee_dollars,
        operation_lane,
        created_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        fill.fill_id,
        fill.pair_id,
        fill.order_id,
        fill.client_order_id,
        fill.side,
        str(fill.price_dollars),
        str(fill.contract_count),
        str(fill.fee_dollars),
        lane,
        fill.created_at.isoformat(),
      ),
    )


def persist_pair_state_transition(
  connection: sqlite3.Connection,
  *,
  pair_id: str,
  state: str,
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str | None = None,
  detail: dict[str, Any] | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT INTO pair_states (pair_id, state, operation_lane, lane_session_id, detail_json, recorded_at_utc)
      VALUES (?, ?, ?, ?, ?, ?)
      ''',
      (pair_id, state, lane, lane_session_id, _json_detail(detail), recorded_at_utc),
    )


def persist_pnl_snapshot(
  connection: sqlite3.Connection,
  snapshot: PairPnlSnapshot,
  *,
  operation_lane: str,
  lane_session_id: str | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT INTO pair_pnl_snapshots (
        pair_id,
        locked_contracts,
        gross_dollars,
        net_projected_dollars,
        net_realized_dollars,
        operation_lane,
        lane_session_id,
        recorded_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        snapshot.pair_id,
        str(snapshot.locked_contracts),
        str(snapshot.gross_dollars),
        str(snapshot.net_projected_dollars),
        str(snapshot.net_realized_dollars),
        lane,
        lane_session_id,
        snapshot.recorded_at.isoformat(),
      ),
    )


def persist_account_limits(
  connection: sqlite3.Connection,
  limits: AccountLimits,
  *,
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT INTO account_api_limits (
        usage_tier,
        read_refill_rate,
        read_bucket_capacity,
        write_refill_rate,
        write_bucket_capacity,
        operation_lane,
        lane_session_id,
        recorded_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        limits.usage_tier,
        limits.read.refill_rate,
        limits.read.bucket_capacity,
        limits.write.refill_rate,
        limits.write.bucket_capacity,
        lane,
        lane_session_id,
        recorded_at_utc,
      ),
    )


def persist_service_heartbeat(
  connection: sqlite3.Connection,
  *,
  component: str,
  status: str,
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str | None = None,
  detail: dict[str, Any] | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT INTO service_heartbeats (component, status, operation_lane, lane_session_id, detail_json, recorded_at_utc)
      VALUES (?, ?, ?, ?, ?, ?)
      ''',
      (component, status, lane, lane_session_id, _json_detail(detail), recorded_at_utc),
    )


def persist_operator_action(
  connection: sqlite3.Connection,
  *,
  action: str,
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str | None = None,
  pair_id: str | None = None,
  detail: dict[str, Any] | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT INTO operator_actions (action, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
      VALUES (?, ?, ?, ?, ?, ?)
      ''',
      (action, pair_id, lane, lane_session_id, _json_detail(detail), recorded_at_utc),
    )


def persist_runtime_event(
  connection: sqlite3.Connection,
  *,
  level: str,
  event_type: str,
  recorded_at_utc: str,
  operation_lane: str,
  lane_session_id: str | None = None,
  pair_id: str | None = None,
  detail: dict[str, Any] | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
      VALUES (?, ?, ?, ?, ?, ?, ?)
      ''',
      (level, event_type, pair_id, lane, lane_session_id, _json_detail(detail), recorded_at_utc),
    )


def _opt_text(value: Any) -> str | None:
  return None if value is None else str(value)


def persist_pair_liquidity_observation(
  connection: sqlite3.Connection,
  *,
  pair_id: str,
  ticker: str,
  phase: str,
  operation_lane: str,
  recorded_at_utc: str,
  observation: dict[str, Any],
  lane_session_id: str | None = None,
) -> None:
  """Write one Lane A coverability observation row. Explicit lane (no default).

  ``observation`` carries already-computed values; missing keys store as NULL except
  the two ladders and ``readback_status``, which are NOT NULL. A fail-soft capture
  passes empty ladders (``'[]'``) and ``readback_status='readback_failed'``."""
  lane = _normalize_operation_lane(operation_lane)
  if str(phase or '').strip() not in {'submit', 'shelter', 'resolution'}:
    raise ValueError('phase must be submit, shelter, or resolution.')
  with connection:
    connection.execute(
      '''
      INSERT INTO pair_liquidity_observations (
        pair_id, ticker, phase, operation_lane, recorded_at_utc, readback_status,
        yes_bid_depth_json, no_bid_depth_json, best_yes_bid, best_no_bid,
        yes_depth_within_band, no_depth_within_band, yes_flow_window_fp, no_flow_window_fp,
        flow_window_sec, divergence, volume_24h_fp, volume_fp, open_interest_fp,
        intended_yes_price, intended_no_price, intended_contract_count, lane_session_id
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        pair_id, ticker, phase, lane, recorded_at_utc,
        str(observation.get('readback_status') or 'ok'),
        str(observation.get('yes_bid_depth_json') or '[]'),
        str(observation.get('no_bid_depth_json') or '[]'),
        _opt_text(observation.get('best_yes_bid')),
        _opt_text(observation.get('best_no_bid')),
        _opt_text(observation.get('yes_depth_within_band')),
        _opt_text(observation.get('no_depth_within_band')),
        _opt_text(observation.get('yes_flow_window_fp')),
        _opt_text(observation.get('no_flow_window_fp')),
        _opt_text(observation.get('flow_window_sec')),
        _opt_text(observation.get('divergence')),
        _opt_text(observation.get('volume_24h_fp')),
        _opt_text(observation.get('volume_fp')),
        _opt_text(observation.get('open_interest_fp')),
        _opt_text(observation.get('intended_yes_price')),
        _opt_text(observation.get('intended_no_price')),
        _opt_text(observation.get('intended_contract_count')),
        lane_session_id,
      ),
    )


def persist_analytical_snapshot(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  lane_session_id: str | None = None,
  snapshot_type: str,
  evidence_class: str,
  recorded_at_utc: str,
  detail: dict[str, Any] | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT INTO analytical_snapshots (operation_lane, lane_session_id, snapshot_type, evidence_class, detail_json, recorded_at_utc)
      VALUES (?, ?, ?, ?, ?, ?)
      ''',
      (lane, lane_session_id, snapshot_type, evidence_class, _json_detail(detail), recorded_at_utc),
    )


def persist_candidate_review_run(
  connection: sqlite3.Connection,
  *,
  run_id: str,
  recorded_at_utc: str,
  operation_lane: str,
  candidate_signature: str,
  candidate_count: int,
  source_action: str,
  lane_session_id: str | None = None,
  detail: dict[str, Any] | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT OR IGNORE INTO candidate_review_runs (
        run_id,
        operation_lane,
        lane_session_id,
        candidate_signature,
        candidate_count,
        source_action,
        detail_json,
        recorded_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        run_id,
        lane,
        lane_session_id,
        candidate_signature,
        int(candidate_count),
        source_action,
        _json_detail(detail),
        recorded_at_utc,
      ),
    )


def persist_candidate_review_candidates(
  connection: sqlite3.Connection,
  *,
  run_id: str,
  recorded_at_utc: str,
  operation_lane: str,
  candidates: list[dict[str, Any]],
  effective_buffers: dict[str, Any] | None = None,
) -> None:
  # operation_lane is required and explicit -- no schema DEFAULT, no fallback.
  # _normalize_operation_lane fails closed on empty/None/unknown.
  lane = _normalize_operation_lane(operation_lane)
  if not candidates:
    return
  # Lane A (candidate-expiry clock): compute the three discovery-time deadlines from
  # the candidate's already-present close time + the effective buffers (seed when not
  # supplied; Lane S supplies the self-calibrated snapshot). Single shared derivation
  # for BOTH writers; fail closed to NULL deadlines when close time is absent.
  params: list[tuple[Any, ...]] = []
  for candidate in candidates:
    candidate_uid = str(candidate.get('candidate_uid') or candidate.get('candidate_key') or '')
    if not candidate_uid.strip():
      continue
    deadlines = compute_candidate_deadlines(
      candidate.get('close_time_utc') or candidate.get('market_close_time_utc'),
      effective_buffers,
    ) or {}
    params.append((
      run_id,
      candidate_uid,
      str(candidate.get('candidate_key') or candidate.get('candidate_uid') or ''),
      str(candidate.get('ticker') or ''),
      str(candidate.get('qualifier_tier') or ''),
      str(candidate.get('review_row_origin') or 'current'),
      _json_detail(candidate),
      recorded_at_utc,
      lane,
      deadlines.get('market_close_at_utc'),
      deadlines.get('view_expires_at_utc'),
      deadlines.get('submit_expires_at_utc'),
    ))
  if not params:
    return
  with connection:
    connection.executemany(
      '''
      INSERT INTO candidate_review_candidates (
        run_id,
        candidate_uid,
        candidate_key,
        ticker,
        qualifier_tier,
        review_row_origin,
        detail_json,
        recorded_at_utc,
        operation_lane,
        market_close_at_utc,
        view_expires_at_utc,
        submit_expires_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(run_id, candidate_uid) DO UPDATE SET
        candidate_key = excluded.candidate_key,
        ticker = excluded.ticker,
        qualifier_tier = excluded.qualifier_tier,
        review_row_origin = excluded.review_row_origin,
        detail_json = excluded.detail_json,
        recorded_at_utc = excluded.recorded_at_utc,
        operation_lane = excluded.operation_lane
        -- market_close_at_utc / view_expires_at_utc / submit_expires_at_utc are
        -- intentionally PRESERVED on conflict: deadlines are stamped once at first
        -- discovery (same posture as the lifecycle/terminal columns). Lane E updates
        -- market_close_at_utc explicitly on a Kalshi close_date_updated event.
      ''',
      params,
    )


def persist_known_non_binary_market(
  connection: sqlite3.Connection,
  *,
  recorded_at_utc: str,
  operation_lane: str,
  classification_reason: str,
  actionability: str,
  market_ticker: str = '',
  event_ticker: str = '',
  series_ticker: str = '',
  shape_signature: str = '',
  market_count: int = 0,
  mutually_exclusive: bool | None = None,
  sample_sibling_tickers: list[str] | tuple[str, ...] | None = None,
  source_run_id: str | None = None,
  source_runtime_event_id: str | None = None,
  lane_session_id: str | None = None,
  detail: dict[str, Any] | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  reason = str(classification_reason or '').strip() or 'unknown_fail_closed'
  action = str(actionability or '').strip() or 'unknown_fail_closed'
  clean_market = str(market_ticker or '').strip()
  clean_event = str(event_ticker or '').strip()
  clean_series = str(series_ticker or '').strip()
  clean_shape = str(shape_signature or '').strip()
  if not clean_shape:
    clean_shape = '{series}|{reason}|{count}'.format(
      series=clean_series or clean_event or clean_market or 'unknown',
      reason=reason,
      count=int(market_count or 0),
    )
  ledger_key = '|'.join(
    (
      clean_series or 'series:unknown',
      clean_event or 'event:unknown',
      clean_shape,
    )
  )
  siblings = [str(item) for item in (sample_sibling_tickers or ()) if str(item).strip()]
  mutually_text = '' if mutually_exclusive is None else ('true' if mutually_exclusive else 'false')
  with connection:
    connection.execute(
      '''
      INSERT INTO known_non_binary_markets (
        ledger_key,
        series_ticker,
        event_ticker,
        market_ticker,
        shape_signature,
        classification_reason,
        actionability,
        market_count,
        mutually_exclusive,
        sample_sibling_tickers_json,
        first_seen_utc,
        last_seen_utc,
        seen_count,
        source_run_id,
        source_runtime_event_id,
        operation_lane,
        lane_session_id,
        detail_json
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, ?)
      ON CONFLICT(ledger_key) DO UPDATE SET
        market_ticker = excluded.market_ticker,
        classification_reason = excluded.classification_reason,
        actionability = excluded.actionability,
        market_count = excluded.market_count,
        mutually_exclusive = excluded.mutually_exclusive,
        sample_sibling_tickers_json = excluded.sample_sibling_tickers_json,
        last_seen_utc = excluded.last_seen_utc,
        seen_count = known_non_binary_markets.seen_count + 1,
        source_run_id = excluded.source_run_id,
        source_runtime_event_id = excluded.source_runtime_event_id,
        operation_lane = excluded.operation_lane,
        lane_session_id = excluded.lane_session_id,
        detail_json = excluded.detail_json
      ''',
      (
        ledger_key,
        clean_series,
        clean_event,
        clean_market,
        clean_shape,
        reason,
        action,
        int(market_count or 0),
        mutually_text,
        json.dumps(siblings, sort_keys=True),
        recorded_at_utc,
        recorded_at_utc,
        source_run_id,
        source_runtime_event_id,
        lane,
        lane_session_id,
        _json_detail(detail),
      ),
    )


def _notification_level_rank(level: str | None) -> int:
  normalized = str(level or '').strip().lower()
  if normalized not in NOTIFICATION_LEVELS:
    raise ValueError(f'level must be one of {NOTIFICATION_LEVELS}')
  return NOTIFICATION_LEVELS.index(normalized)


def _notification_visibility_expires_at(level: str, created_at_utc: str) -> str | None:
  normalized = str(level or '').strip().lower()
  if normalized == 'error':
    return None
  created_at = datetime.fromisoformat(str(created_at_utc).replace('Z', '+00:00'))
  return (created_at + timedelta(days=7)).astimezone(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def persist_operator_notification(
  connection: sqlite3.Connection,
  *,
  created_at_utc: str,
  operation_lane: str,
  profile_token: str,
  level: str,
  title: str,
  body: str,
  source: str,
  notification_id: str | None = None,
  related_candidate_id: str | None = None,
  dismissed_at_utc: str | None = None,
  dismissed_by: str | None = None,
  visibility_expires_at_utc: str | None = None,
) -> str:
  lane = _normalize_operation_lane(operation_lane)
  normalized_profile_token = str(profile_token or '').strip()
  if not normalized_profile_token:
    raise ValueError('profile_token is required for operator notifications.')
  normalized_level = str(level or '').strip().lower()
  if normalized_level not in NOTIFICATION_LEVELS:
    raise ValueError(f'level must be one of {NOTIFICATION_LEVELS}')
  normalized_source = str(source or '').strip()
  if not normalized_source:
    raise ValueError('source is required for operator notifications.')
  normalized_notification_id = str(notification_id or '').strip() or f'notif-{uuid4().hex}'
  expiry = visibility_expires_at_utc if visibility_expires_at_utc is not None else _notification_visibility_expires_at(normalized_level, created_at_utc)
  with connection:
    connection.execute(
      '''
      INSERT OR REPLACE INTO operator_notifications (
        notification_id,
        created_at_utc,
        operation_lane,
        profile_token,
        level,
        title,
        body,
        source,
        related_candidate_id,
        dismissed_at_utc,
        dismissed_by,
        visibility_expires_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        normalized_notification_id,
        created_at_utc,
        lane,
        normalized_profile_token,
        normalized_level,
        title,
        body,
        normalized_source,
        related_candidate_id,
        dismissed_at_utc,
        dismissed_by,
        expiry,
      ),
    )
  return normalized_notification_id


def fetch_operator_notifications(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  include_dismissed: bool = False,
  include_expired: bool = False,
  minimum_level: str | None = None,
  limit: int = 100,
  now_utc: str | None = None,
) -> list[dict[str, Any]]:
  lane = _normalize_operation_lane(operation_lane)
  normalized_profile_token = str(profile_token or '').strip()
  if not normalized_profile_token:
    return []
  normalized_limit = max(0, int(limit or 0))
  minimum_rank = 0 if minimum_level is None else _notification_level_rank(minimum_level)
  now_value = now_utc or _utc_now_iso()
  rows = connection.execute(
    '''
    SELECT
      notification_id,
      created_at_utc,
      operation_lane,
      profile_token,
      level,
      title,
      body,
      source,
      related_candidate_id,
      dismissed_at_utc,
      dismissed_by,
      visibility_expires_at_utc
    FROM operator_notifications
    WHERE operation_lane = ?
      AND profile_token = ?
      AND (? = 1 OR dismissed_at_utc IS NULL)
      AND (? = 1 OR visibility_expires_at_utc IS NULL OR visibility_expires_at_utc > ?)
    ORDER BY created_at_utc DESC, notification_id DESC
    LIMIT ?
    ''',
    (lane, normalized_profile_token, int(include_dismissed), int(include_expired), now_value, normalized_limit),
  ).fetchall()
  notifications: list[dict[str, Any]] = []
  for row in rows:
    level = str(row['level'] or '').strip().lower()
    if NOTIFICATION_LEVELS.index(level) < minimum_rank:
      continue
    notifications.append(
      {
        'notification_id': str(row['notification_id']),
        'created_at_utc': str(row['created_at_utc']),
        'operation_lane': str(row['operation_lane']),
        'profile_token': str(row['profile_token']),
        'level': level,
        'title': str(row['title'] or ''),
        'body': str(row['body'] or ''),
        'source': str(row['source'] or ''),
        'related_candidate_id': str(row['related_candidate_id'] or '') or None,
        'dismissed_at_utc': str(row['dismissed_at_utc'] or '') or None,
        'dismissed_by': str(row['dismissed_by'] or '') or None,
        'visibility_expires_at_utc': str(row['visibility_expires_at_utc'] or '') or None,
        'is_expired': bool(row['visibility_expires_at_utc'] and str(row['visibility_expires_at_utc']) <= now_value),
        'is_dismissed': bool(row['dismissed_at_utc']),
      }
    )
  return notifications


def dismiss_operator_notification(
  connection: sqlite3.Connection,
  *,
  notification_id: str,
  dismissed_at_utc: str,
  dismissed_by: str,
) -> bool:
  normalized_notification_id = str(notification_id or '').strip()
  if not normalized_notification_id:
    return False
  with connection:
    cursor = connection.execute(
      '''
      UPDATE operator_notifications
      SET dismissed_at_utc = ?, dismissed_by = ?
      WHERE notification_id = ?
      ''',
      (dismissed_at_utc, dismissed_by, normalized_notification_id),
    )
  return cursor.rowcount > 0


def dismiss_all_operator_notifications(
  connection: sqlite3.Connection,
  *,
  operation_lane: str,
  profile_token: str,
  dismissed_at_utc: str,
  dismissed_by: str,
) -> int:
  lane = _normalize_operation_lane(operation_lane)
  normalized_profile_token = str(profile_token or '').strip()
  if not normalized_profile_token:
    return 0
  with connection:
    cursor = connection.execute(
      '''
      UPDATE operator_notifications
      SET dismissed_at_utc = ?, dismissed_by = ?
      WHERE operation_lane = ?
        AND profile_token = ?
        AND dismissed_at_utc IS NULL
      ''',
      (dismissed_at_utc, dismissed_by, lane, normalized_profile_token),
    )
  return int(cursor.rowcount or 0)


def persist_candidate_saved_set(
  connection: sqlite3.Connection,
  *,
  saved_set_id: str,
  run_id: str | None,
  recorded_at_utc: str,
  operation_lane: str,
  saved_key_count: int,
  state_id: str,
  source_action: str,
  members: list[dict[str, Any]],
  lane_session_id: str | None = None,
  detail: dict[str, Any] | None = None,
) -> None:
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT OR REPLACE INTO candidate_saved_sets (
        saved_set_id,
        run_id,
        operation_lane,
        lane_session_id,
        saved_key_count,
        state_id,
        source_action,
        detail_json,
        recorded_at_utc
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        saved_set_id,
        run_id,
        lane,
        lane_session_id,
        int(saved_key_count),
        state_id,
        source_action,
        _json_detail(detail),
        recorded_at_utc,
      ),
    )
    connection.execute(
      'DELETE FROM candidate_saved_set_members WHERE saved_set_id = ?',
      (saved_set_id,),
    )
    connection.executemany(
      '''
      INSERT INTO candidate_saved_set_members (
        saved_set_id,
        candidate_uid,
        candidate_key,
        member_order,
        detail_json,
        recorded_at_utc,
        operation_lane
      ) VALUES (?, ?, ?, ?, ?, ?, ?)
      ''',
      [
        (
          saved_set_id,
          str(member.get('candidate_uid') or member.get('candidate_key') or ''),
          str(member.get('candidate_key') or member.get('candidate_uid') or ''),
          int(index),
          _json_detail(member),
          recorded_at_utc,
          lane,
        )
        for index, member in enumerate(members)
        if str(member.get('candidate_uid') or member.get('candidate_key') or '').strip()
      ],
    )


def persist_candidate_saved_set_evaluation(
  connection: sqlite3.Connection,
  *,
  saved_set_id: str,
  recorded_at_utc: str,
  operation_lane: str,
  evaluation_status: str,
  actionability_status: str,
  visibility_status: str,
  offline_verifiable: bool,
  online_revalidation_required: bool,
  detail: dict[str, Any] | None = None,
) -> None:
  # operation_lane is required and explicit -- no schema DEFAULT, no fallback.
  lane = _normalize_operation_lane(operation_lane)
  with connection:
    connection.execute(
      '''
      INSERT INTO candidate_saved_set_evaluations (
        saved_set_id,
        evaluation_status,
        actionability_status,
        visibility_status,
        offline_verifiable,
        online_revalidation_required,
        detail_json,
        recorded_at_utc,
        operation_lane
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      (
        saved_set_id,
        evaluation_status,
        actionability_status,
        visibility_status,
        int(bool(offline_verifiable)),
        int(bool(online_revalidation_required)),
        _json_detail(detail),
        recorded_at_utc,
        lane,
      ),
    )


def fetch_latest_candidate_saved_set(
  connection: sqlite3.Connection,
  *,
  operation_lane: str | None = None,
) -> dict[str, Any] | None:
  lane = _normalize_operation_lane(operation_lane) if operation_lane is not None else None
  if lane is None:
    saved_set_row = connection.execute(
      '''
      SELECT saved_set_id, run_id, operation_lane, lane_session_id, saved_key_count, state_id, source_action, detail_json, recorded_at_utc
      FROM candidate_saved_sets
      ORDER BY recorded_at_utc DESC, rowid DESC
      LIMIT 1
      '''
    ).fetchone()
  else:
    saved_set_row = connection.execute(
      '''
      SELECT saved_set_id, run_id, operation_lane, lane_session_id, saved_key_count, state_id, source_action, detail_json, recorded_at_utc
      FROM candidate_saved_sets
      WHERE operation_lane = ?
      ORDER BY recorded_at_utc DESC, rowid DESC
      LIMIT 1
      ''',
      (lane,),
    ).fetchone()
  if saved_set_row is None:
    return None

  member_rows = connection.execute(
    '''
    SELECT candidate_uid, candidate_key, member_order, detail_json, recorded_at_utc
    FROM candidate_saved_set_members
    WHERE saved_set_id = ?
    ORDER BY member_order ASC, id ASC
    ''',
    (saved_set_row['saved_set_id'],),
  ).fetchall()
  evaluation_row = connection.execute(
    '''
    SELECT evaluation_status, actionability_status, visibility_status, offline_verifiable, online_revalidation_required, detail_json, recorded_at_utc
    FROM candidate_saved_set_evaluations
    WHERE saved_set_id = ?
    ORDER BY recorded_at_utc DESC, id DESC
    LIMIT 1
    ''',
    (saved_set_row['saved_set_id'],),
  ).fetchone()

  return {
    'saved_set_id': saved_set_row['saved_set_id'],
    'run_id': saved_set_row['run_id'],
    'operation_lane': saved_set_row['operation_lane'],
    'lane_session_id': saved_set_row['lane_session_id'],
    'saved_key_count': int(saved_set_row['saved_key_count'] or 0),
    'state_id': saved_set_row['state_id'],
    'source_action': saved_set_row['source_action'],
    'detail': _json_load(saved_set_row['detail_json']),
    'recorded_at_utc': saved_set_row['recorded_at_utc'],
    'members': [
      {
        'candidate_uid': row['candidate_uid'],
        'candidate_key': row['candidate_key'],
        'member_order': int(row['member_order'] or 0),
        'detail': _json_load(row['detail_json']),
        'recorded_at_utc': row['recorded_at_utc'],
      }
      for row in member_rows
    ],
    'latest_evaluation': (
      {
        'evaluation_status': evaluation_row['evaluation_status'],
        'actionability_status': evaluation_row['actionability_status'],
        'visibility_status': evaluation_row['visibility_status'],
        'offline_verifiable': bool(evaluation_row['offline_verifiable']),
        'online_revalidation_required': bool(evaluation_row['online_revalidation_required']),
        'detail': _json_load(evaluation_row['detail_json']),
        'recorded_at_utc': evaluation_row['recorded_at_utc'],
      }
      if evaluation_row is not None
      else None
    ),
  }


def fetch_candidate_saved_set_for_handoff(
  connection: sqlite3.Connection,
  *,
  saved_set_id: str,
  operation_lane: str,
  lane_session_id: str,
  run_id: str,
) -> dict[str, Any] | None:
  lane = _normalize_operation_lane(operation_lane)
  saved_set_row = connection.execute(
    '''
    SELECT saved_set_id, run_id, operation_lane, lane_session_id, saved_key_count, state_id, source_action, detail_json, recorded_at_utc
    FROM candidate_saved_sets
    WHERE saved_set_id = ?
      AND operation_lane = ?
      AND lane_session_id = ?
      AND run_id = ?
    LIMIT 1
    ''',
    (saved_set_id, lane, lane_session_id, run_id),
  ).fetchone()
  if saved_set_row is None:
    return None

  member_rows = connection.execute(
    '''
    SELECT candidate_uid, candidate_key, member_order, detail_json, recorded_at_utc
    FROM candidate_saved_set_members
    WHERE saved_set_id = ?
    ORDER BY member_order ASC, id ASC
    ''',
    (saved_set_row['saved_set_id'],),
  ).fetchall()
  evaluation_row = connection.execute(
    '''
    SELECT evaluation_status, actionability_status, visibility_status, offline_verifiable, online_revalidation_required, detail_json, recorded_at_utc
    FROM candidate_saved_set_evaluations
    WHERE saved_set_id = ?
    ORDER BY recorded_at_utc DESC, id DESC
    LIMIT 1
    ''',
    (saved_set_row['saved_set_id'],),
  ).fetchone()

  return {
    'saved_set_id': saved_set_row['saved_set_id'],
    'run_id': saved_set_row['run_id'],
    'operation_lane': saved_set_row['operation_lane'],
    'lane_session_id': saved_set_row['lane_session_id'],
    'saved_key_count': int(saved_set_row['saved_key_count'] or 0),
    'state_id': saved_set_row['state_id'],
    'source_action': saved_set_row['source_action'],
    'detail': _json_load(saved_set_row['detail_json']),
    'recorded_at_utc': saved_set_row['recorded_at_utc'],
    'members': [
      {
        'candidate_uid': row['candidate_uid'],
        'candidate_key': row['candidate_key'],
        'member_order': int(row['member_order'] or 0),
        'detail': _json_load(row['detail_json']),
        'recorded_at_utc': row['recorded_at_utc'],
      }
      for row in member_rows
    ],
    'latest_evaluation': (
      {
        'evaluation_status': evaluation_row['evaluation_status'],
        'actionability_status': evaluation_row['actionability_status'],
        'visibility_status': evaluation_row['visibility_status'],
        'offline_verifiable': bool(evaluation_row['offline_verifiable']),
        'online_revalidation_required': bool(evaluation_row['online_revalidation_required']),
        'detail': _json_load(evaluation_row['detail_json']),
        'recorded_at_utc': evaluation_row['recorded_at_utc'],
      }
      if evaluation_row is not None
      else None
    ),
  }


def fetch_saved_set_history(
  connection: sqlite3.Connection,
  *,
  operation_lane: str | None = None,
  limit: int = 20,
) -> list[dict[str, Any]]:
  lane = _normalize_operation_lane(operation_lane) if operation_lane is not None else None
  if lane is None:
    set_rows = connection.execute(
      '''
      SELECT saved_set_id, operation_lane, saved_key_count, recorded_at_utc
      FROM candidate_saved_sets
      ORDER BY recorded_at_utc ASC, rowid ASC
      LIMIT ?
      ''',
      (int(limit),),
    ).fetchall()
  else:
    set_rows = connection.execute(
      '''
      SELECT saved_set_id, operation_lane, saved_key_count, recorded_at_utc
      FROM candidate_saved_sets
      WHERE operation_lane = ?
      ORDER BY recorded_at_utc ASC, rowid ASC
      LIMIT ?
      ''',
      (lane, int(limit)),
    ).fetchall()
  if not set_rows:
    return []
  results: list[dict[str, Any]] = []
  for row in set_rows:
    member_rows = connection.execute(
      '''
      SELECT candidate_key
      FROM candidate_saved_set_members
      WHERE saved_set_id = ?
      ORDER BY member_order ASC, id ASC
      ''',
      (row['saved_set_id'],),
    ).fetchall()
    results.append(
      {
        'saved_set_id': row['saved_set_id'],
        'operation_lane': row['operation_lane'],
        'saved_key_count': int(row['saved_key_count'] or 0),
        'recorded_at_utc': row['recorded_at_utc'],
        'member_keys': [str(mr['candidate_key']) for mr in member_rows],
      }
    )
  return results


def fetch_saved_set_evaluation_history(
  connection: sqlite3.Connection,
  *,
  operation_lane: str | None = None,
) -> list[dict[str, Any]]:
  lane = _normalize_operation_lane(operation_lane) if operation_lane is not None else None
  if lane is None:
    rows = connection.execute(
      '''
      SELECT e.saved_set_id, s.operation_lane, e.actionability_status, e.visibility_status,
             e.offline_verifiable, e.online_revalidation_required, e.recorded_at_utc
      FROM candidate_saved_set_evaluations e
      JOIN candidate_saved_sets s ON s.saved_set_id = e.saved_set_id
      ORDER BY e.recorded_at_utc ASC, e.id ASC
      '''
    ).fetchall()
  else:
    rows = connection.execute(
      '''
      SELECT e.saved_set_id, s.operation_lane, e.actionability_status, e.visibility_status,
             e.offline_verifiable, e.online_revalidation_required, e.recorded_at_utc
      FROM candidate_saved_set_evaluations e
      JOIN candidate_saved_sets s ON s.saved_set_id = e.saved_set_id
      WHERE s.operation_lane = ?
      ORDER BY e.recorded_at_utc ASC, e.id ASC
      ''',
      (lane,),
    ).fetchall()
  return [
    {
      'saved_set_id': row['saved_set_id'],
      'operation_lane': row['operation_lane'],
      'actionability_status': row['actionability_status'],
      'visibility_status': row['visibility_status'],
      'offline_verifiable': bool(row['offline_verifiable']),
      'online_revalidation_required': bool(row['online_revalidation_required']),
      'recorded_at_utc': row['recorded_at_utc'],
    }
    for row in rows
  ]


def fetch_pair_state_history(
  connection: sqlite3.Connection,
  *,
  pair_id: str,
  operation_lane: str | None = None,
) -> tuple[dict[str, Any], ...]:
  lane = _normalize_operation_lane(operation_lane) if operation_lane is not None else None
  if lane is None:
    rows = connection.execute(
      '''
      SELECT state, operation_lane, lane_session_id, detail_json, recorded_at_utc
      FROM pair_states
      WHERE pair_id = ?
      ORDER BY id ASC
      ''',
      (pair_id,),
    ).fetchall()
  else:
    rows = connection.execute(
      '''
      SELECT state, operation_lane, lane_session_id, detail_json, recorded_at_utc
      FROM pair_states
      WHERE pair_id = ? AND operation_lane = ?
      ORDER BY id ASC
      ''',
      (pair_id, lane),
    ).fetchall()
  return tuple(
    {
      'state': row['state'],
      'operation_lane': row['operation_lane'],
      'lane_session_id': row['lane_session_id'],
      'detail': json.loads(row['detail_json']),
      'recorded_at_utc': row['recorded_at_utc'],
    }
    for row in rows
  )


def persist_lane_defaults(
  connection: sqlite3.Connection,
  operation_lane: str,
  defaults_delta: dict[str, Any],
  *,
  sources: dict[str, str] | None = None,
) -> None:
  """Full-replace a lane's working defaults. `sources` maps field_id -> provenance
  ('operator' or 'optimizer:<id>'); fields absent from `sources` default to 'operator'.
  """
  recorded_at = datetime.now(timezone.utc).isoformat()
  source_map = sources or {}
  with connection:
    connection.execute(
      'DELETE FROM operator_lane_defaults WHERE operation_lane = ?',
      (_normalize_operation_lane(operation_lane),),
    )
    for field_id, value in defaults_delta.items():
      connection.execute(
        'INSERT INTO operator_lane_defaults (operation_lane, field_id, value, source, recorded_at_utc) '
        'VALUES (?, ?, ?, ?, ?)',
        (
          _normalize_operation_lane(operation_lane),
          str(field_id),
          str(value),
          str(source_map.get(field_id, 'operator')),
          recorded_at,
        ),
      )


def load_lane_defaults(
  connection: sqlite3.Connection,
  operation_lane: str,
) -> dict[str, str]:
  rows = connection.execute(
    'SELECT field_id, value FROM operator_lane_defaults WHERE operation_lane = ?',
    (_normalize_operation_lane(operation_lane),),
  ).fetchall()
  return {str(row[0]): str(row[1]) for row in rows}


def load_lane_default_sources(
  connection: sqlite3.Connection,
  operation_lane: str,
) -> dict[str, str]:
  """Return field_id -> provenance ('operator' | 'optimizer:<id>') for the lane."""
  rows = connection.execute(
    'SELECT field_id, source FROM operator_lane_defaults WHERE operation_lane = ?',
    (_normalize_operation_lane(operation_lane),),
  ).fetchall()
  return {str(row[0]): str(row[1]) for row in rows}


# --- C1: dynamic-sizing last-ready persistence -------------------------------
# Derived/computed sizing values are persisted so the panel does not cold-start at
# "needs more data" every session. They live in analytical_snapshots (a purpose-built
# derived-state store), NOT operator_lane_defaults: that working-default store
# full-replaces a lane's rows on every write and would clobber operator-set defaults.
# See PARAMETER_OPTIMIZATION_PERSISTENCE_AND_AUTOAPPLY_BMAP C1 / 5.1 / 10.0.
DYNAMIC_SIZING_SNAPSHOT_TYPE = 'dynamic_sizing_last_ready'
_DYNAMIC_SIZING_RETENTION = 5


def persist_dynamic_sizing_snapshot(
  connection: sqlite3.Connection,
  operation_lane: str,
  sizing_values: dict[str, Any],
  *,
  lane_session_id: str | None = None,
) -> None:
  """Persist the last ready dynamic-sizing computation for the lane (keep-latest-N).

  Writes to analytical_snapshots only; never touches operator_lane_defaults.
  """
  lane = _normalize_operation_lane(operation_lane)
  recorded_at = datetime.now(timezone.utc).isoformat()
  # Insert + prune in ONE transaction (one commit/fsync). Deliberately does NOT delegate
  # to persist_analytical_snapshot: that commits internally, so reusing it would make this
  # two fsyncs on the scan cycle -- measured ~+5ms, over the §7.1 VL1 budget (the timing
  # test caught exactly that regression). Insert body is identical to that writer.
  with connection:
    connection.execute(
      '''
      INSERT INTO analytical_snapshots (operation_lane, lane_session_id, snapshot_type, evidence_class, detail_json, recorded_at_utc)
      VALUES (?, ?, ?, ?, ?, ?)
      ''',
      (lane, lane_session_id, DYNAMIC_SIZING_SNAPSHOT_TYPE, 'computed', _json_detail(dict(sizing_values)), recorded_at),
    )
    # Retention: keep only the newest _DYNAMIC_SIZING_RETENTION rows for this lane/type.
    connection.execute(
      'DELETE FROM analytical_snapshots WHERE id IN ('
      '  SELECT id FROM analytical_snapshots'
      '  WHERE operation_lane = ? AND snapshot_type = ?'
      '  ORDER BY recorded_at_utc DESC, id DESC'
      '  LIMIT -1 OFFSET ?'
      ')',
      (lane, DYNAMIC_SIZING_SNAPSHOT_TYPE, _DYNAMIC_SIZING_RETENTION),
    )


def load_latest_dynamic_sizing_snapshot(
  connection: sqlite3.Connection,
  operation_lane: str,
) -> dict[str, Any] | None:
  """Return the newest persisted dynamic-sizing values for the lane, or None.

  Used to rehydrate the sizing display at session start / when the current scan has
  insufficient candidates, replacing the 'needs more data' cold-start.
  """
  row = connection.execute(
    'SELECT detail_json, recorded_at_utc FROM analytical_snapshots '
    'WHERE operation_lane = ? AND snapshot_type = ? '
    'ORDER BY recorded_at_utc DESC, id DESC LIMIT 1',
    (_normalize_operation_lane(operation_lane), DYNAMIC_SIZING_SNAPSHOT_TYPE),
  ).fetchone()
  if row is None:
    return None
  try:
    values = json.loads(row[0])
  except (TypeError, ValueError):
    return None
  return {
    'values': values,
    'recorded_at_utc': row[1],
    'source': 'computed:sizing',
    'carried_over': True,
  }


def _parse_iso_ts(value: Any) -> datetime | None:
  text = str(value or '').strip()
  if not text:
    return None
  try:
    dt = datetime.fromisoformat(text.replace('Z', '+00:00'))
  except (TypeError, ValueError):
    return None
  return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


def load_entry_window_evidence(
  connection: sqlite3.Connection,
  operation_lane: str,
) -> list[dict[str, Any]]:
  """Read-only per-pair (entry timing, outcome label) evidence for the C2 optimizer.

  Entry timing = seconds from pair creation to market close (same derivation as the
  offline flow-balance tool). Outcome label uses the shared labeler (VL3 no-drift).
  Only pairs with a resolvable close time are returned; non-authoritative labels are
  kept and filtered by the optimizer's evidence meter.
  """
  lane = _normalize_operation_lane(operation_lane)
  pairs: dict[str, dict[str, Any]] = {}
  for row in connection.execute(
    'SELECT pair_id, ticker, created_at_utc FROM pair_plans WHERE operation_lane = ?', (lane,)
  ):
    pairs[row['pair_id']] = {
      'ticker': row['ticker'],
      'created_at_utc': row['created_at_utc'],
      'yes_filled': Decimal('0'),
      'no_filled': Decimal('0'),
      'terminal': '',
      'order_statuses': set(),
    }
  if not pairs:
    return []
  for row in connection.execute(
    'SELECT pair_id, side, contract_count FROM fills WHERE operation_lane = ?', (lane,)
  ):
    pair = pairs.get(row['pair_id'])
    if pair is None:
      continue
    try:
      count = Decimal(str(row['contract_count']))
    except (InvalidOperation, TypeError, ValueError):
      count = Decimal('0')
    side = str(row['side'] or '').lower()
    if side == 'yes':
      pair['yes_filled'] += count
    elif side == 'no':
      pair['no_filled'] += count
  for row in connection.execute(
    'SELECT pair_id, state FROM pair_states WHERE operation_lane = ? ORDER BY recorded_at_utc, id', (lane,)
  ):
    pair = pairs.get(row['pair_id'])
    if pair is not None:
      pair['terminal'] = row['state']
  for row in connection.execute(
    'SELECT pair_id, order_id, status FROM orders WHERE operation_lane = ?', (lane,)
  ):
    pair = pairs.get(row['pair_id'])
    if pair is not None:
      pair['order_statuses'].add('{0}:{1}'.format(row['order_id'], row['status']))
  close_times: dict[str, Any] = {}
  for row in connection.execute('SELECT ticker, close_time_utc FROM markets_seen'):
    close_times[row['ticker']] = row['close_time_utc']

  evidence: list[dict[str, Any]] = []
  for pair in pairs.values():
    created = _parse_iso_ts(pair['created_at_utc'])
    close = _parse_iso_ts(close_times.get(pair['ticker']))
    if created is None or close is None:
      continue
    seconds_to_close = int((close - created).total_seconds())
    label = label_pair_outcome(
      yes_filled=pair['yes_filled'],
      no_filled=pair['no_filled'],
      raw_terminal_state=pair['terminal'],
      order_statuses=pair['order_statuses'],
      csv_settlement_confirmed=False,
    )
    evidence.append({'seconds_to_close': seconds_to_close, 'outcome_label': label})
  return evidence


def summarize_persistence(connection: sqlite3.Connection, *, operation_lane: str | None = None) -> dict[str, Any]:
  lane = _normalize_operation_lane(operation_lane) if operation_lane is not None else None
  counts: dict[str, int] = {}
  for table in REQUIRED_TABLES:
    if lane is not None and table in LANE_BEARING_TABLES:
      counts[table] = connection.execute(
        'SELECT COUNT(*) AS count FROM {table} WHERE operation_lane = ?'.format(table=table),
        (lane,),
      ).fetchone()['count']
    else:
      counts[table] = connection.execute(
        'SELECT COUNT(*) AS count FROM {table}'.format(table=table)
      ).fetchone()['count']

  if lane is None:
    latest_state_rows = connection.execute(
      '''
      SELECT pair_id, state, operation_lane, lane_session_id, recorded_at_utc
      FROM pair_states
      ORDER BY id ASC
      '''
    ).fetchall()
  else:
    latest_state_rows = connection.execute(
      '''
      SELECT pair_id, state, operation_lane, lane_session_id, recorded_at_utc
      FROM pair_states
      WHERE operation_lane = ?
      ORDER BY id ASC
      ''',
      (lane,),
    ).fetchall()
  state_history: dict[str, list[str]] = {}
  lane_session_history: dict[str, list[str]] = {}
  for row in latest_state_rows:
    state_history.setdefault(row['pair_id'], []).append(row['state'])
    lane_session_id = row['lane_session_id']
    if lane_session_id:
      lane_session_history.setdefault(row['pair_id'], []).append(str(lane_session_id))
  return {
    'table_counts': counts,
    'pair_state_history': state_history,
    'pair_lane_session_history': lane_session_history,
    'operation_lane': lane,
  }
