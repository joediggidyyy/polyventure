from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
import json
from pathlib import Path

from polyventure.persistence import (
  api_key_hash_for_id,
  build_datapack_bundle,
  datapack_manifest_checksum,
  datapack_payload_checksum,
  evaluate_datapack_convergence,
  evaluate_datapack_identity,
  dismiss_all_operator_notifications,
  dismiss_operator_notification,
  fetch_operator_notifications,
  load_lane_defaults,
  REQUIRED_TABLES,
  fetch_pair_state_history,
  open_database,
  persist_lane_defaults,
  persist_account_limits,
  persist_analytical_snapshot,
  persist_candidate_review_candidates,
  persist_candidate_review_run,
  persist_candidate_saved_set,
  persist_candidate_saved_set_evaluation,
  persist_fill,
  persist_operator_action,
  persist_operator_notification,
  persist_order_statuses,
  persist_pair_plan,
  persist_pair_state_transition,
  persist_pnl_snapshot,
  promote_order_id,
  profile_token_for_key_path,
  rebind_datapack_controls,
  validate_datapack_artifacts,
  persist_runtime_event,
  persist_service_heartbeat,
  record_market_seen,
  summarize_persistence,
)
from polyventure.types import (
  AccountBucketLimit,
  AccountLimits,
  FillEvent,
  PairOrderPlan,
  PairPnlSnapshot,
)


def _plan() -> PairOrderPlan:
  return PairOrderPlan(
    pair_id='pair-001',
    ticker='KALSHI-PAIR-001',
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='yes-client-001',
    no_client_order_id='no-client-001',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )


def test_initialize_database_creates_required_tables(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  rows = connection.execute(
    "SELECT name FROM sqlite_master WHERE type = 'table'"
  ).fetchall()
  names = {row['name'] for row in rows}

  for table in REQUIRED_TABLES:
    assert table in names


def test_persist_pair_plan_creates_pair_and_order_rows(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  plan = _plan()

  persist_pair_plan(
    connection,
    plan,
    created_at_utc='2026-05-05T05:00:00Z',
    operation_lane='sandbox',
  )

  pair_row = connection.execute(
    'SELECT * FROM pair_plans WHERE pair_id = ?',
    (plan.pair_id,),
  ).fetchone()
  order_rows = connection.execute(
    'SELECT side, client_order_id, operation_lane FROM orders WHERE pair_id = ? ORDER BY side ASC',
    (plan.pair_id,),
  ).fetchall()

  assert pair_row['ticker'] == 'KALSHI-PAIR-001'
  assert pair_row['contract_count'] == '5'
  assert pair_row['operation_lane'] == 'sandbox'
  assert [(row['side'], row['client_order_id'], row['operation_lane']) for row in order_rows] == [
    ('no', 'no-client-001', 'sandbox'),
    ('yes', 'yes-client-001', 'sandbox'),
  ]


def test_persist_order_statuses_updates_existing_order_rows(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  plan = _plan()

  persist_pair_plan(
    connection,
    plan,
    created_at_utc='2026-05-05T05:00:00Z',
    operation_lane='sandbox',
  )
  persist_order_statuses(
    connection,
    operation_lane='sandbox',
    statuses=[
      {'order_id': f'{plan.pair_id}:yes', 'status': 'resting'},
      {'order_id': f'{plan.pair_id}:no', 'status': 'canceled'},
    ],
  )

  rows = connection.execute(
    'SELECT order_id, status FROM orders WHERE pair_id = ? ORDER BY side ASC',
    (plan.pair_id,),
  ).fetchall()

  assert [(row['order_id'], row['status']) for row in rows] == [
    (f'{plan.pair_id}:no', 'canceled'),
    (f'{plan.pair_id}:yes', 'resting'),
  ]


def test_promote_order_id_replaces_synthetic_id_by_pair_client_and_side(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  plan = _plan()
  persist_pair_plan(
    connection,
    plan,
    created_at_utc='2026-05-05T05:00:00Z',
    operation_lane='live',
  )

  promote_order_id(
    connection,
    operation_lane='live',
    pair_id=plan.pair_id,
    client_order_id=plan.yes_client_order_id,
    side='yes',
    remote_order_id='remote-yes-001',
    status='resting',
  )
  persist_order_statuses(
    connection,
    operation_lane='live',
    statuses=[{'order_id': 'remote-yes-001', 'status': 'executed'}],
  )

  rows = connection.execute(
    'SELECT order_id, side, status FROM orders WHERE pair_id = ? ORDER BY side ASC',
    (plan.pair_id,),
  ).fetchall()
  assert [(row['order_id'], row['side'], row['status']) for row in rows] == [
    (f'{plan.pair_id}:no', 'no', 'planned'),
    ('remote-yes-001', 'yes', 'executed'),
  ]


def test_persistence_tracks_fill_state_and_summary(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  plan = _plan()
  lane_session_id = 'sandbox-session-001'
  persist_pair_plan(
    connection,
    plan,
    created_at_utc='2026-05-05T05:00:00Z',
    operation_lane='sandbox',
  )

  persist_account_limits(
    connection,
    AccountLimits(
      usage_tier='demo-tier',
      read=AccountBucketLimit(refill_rate=30, bucket_capacity=60),
      write=AccountBucketLimit(refill_rate=10, bucket_capacity=20),
    ),
    recorded_at_utc='2026-05-05T05:00:01Z',
    operation_lane='sandbox',
    lane_session_id=lane_session_id,
  )
  record_market_seen(
    connection,
    ticker='KALSHI-PAIR-001',
    status='open',
    close_time_utc='2026-05-05T05:10:00Z',
    last_seen_at_utc='2026-05-05T05:00:02Z',
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='PLANNED',
    recorded_at_utc='2026-05-05T05:00:03Z',
    operation_lane='sandbox',
    lane_session_id=lane_session_id,
    detail={'source': 'unit-test'},
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='LOCKED',
    recorded_at_utc='2026-05-05T05:00:04Z',
    operation_lane='sandbox',
    lane_session_id=lane_session_id,
    detail={'locked_contracts': '5'},
  )
  persist_fill(
    connection,
    FillEvent(
      fill_id='fill-001',
      pair_id=plan.pair_id,
      order_id='pair-001:yes',
      client_order_id='yes-client-001',
      side='yes',
      price_dollars=Decimal('0.34'),
      contract_count=Decimal('5'),
      fee_dollars=Decimal('0.02'),
      created_at=datetime(2026, 5, 5, 5, 0, 5, tzinfo=UTC),
    ),
    operation_lane='sandbox',
  )
  persist_fill(
    connection,
    FillEvent(
      fill_id='fill-002',
      pair_id=plan.pair_id,
      order_id='pair-001:no',
      client_order_id='no-client-001',
      side='no',
      price_dollars=Decimal('0.39'),
      contract_count=Decimal('5'),
      fee_dollars=Decimal('0.02'),
      created_at=datetime(2026, 5, 5, 5, 0, 6, tzinfo=UTC),
    ),
    operation_lane='sandbox',
  )
  persist_pnl_snapshot(
    connection,
    PairPnlSnapshot(
      pair_id=plan.pair_id,
      locked_contracts=Decimal('5'),
      gross_dollars=Decimal('1.35'),
      net_projected_dollars=Decimal('1.25'),
      net_realized_dollars=Decimal('1.21'),
      recorded_at=datetime(2026, 5, 5, 5, 0, 7, tzinfo=UTC),
    ),
    operation_lane='sandbox',
    lane_session_id=lane_session_id,
  )
  persist_service_heartbeat(
    connection,
    component='persistence-slice',
    status='ok',
    recorded_at_utc='2026-05-05T05:00:08Z',
    operation_lane='sandbox',
    lane_session_id=lane_session_id,
    detail={'db': 'ready'},
  )
  persist_operator_action(
    connection,
    action='review-pair',
    pair_id=plan.pair_id,
    recorded_at_utc='2026-05-05T05:00:09Z',
    operation_lane='sandbox',
    lane_session_id=lane_session_id,
    detail={'mode': 'sandbox'},
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='pair_locked',
    pair_id=plan.pair_id,
    recorded_at_utc='2026-05-05T05:00:10Z',
    operation_lane='sandbox',
    lane_session_id=lane_session_id,
    detail={'locked_contracts': '5'},
  )
  persist_service_heartbeat(
    connection,
    component='persistence-slice',
    status='live-ok',
    recorded_at_utc='2026-05-05T05:00:11Z',
    operation_lane='live',
    lane_session_id='live-session-001',
    detail={'db': 'ready'},
  )

  history = fetch_pair_state_history(connection, pair_id=plan.pair_id, operation_lane='sandbox')
  summary = summarize_persistence(connection, operation_lane='sandbox')
  all_summary = summarize_persistence(connection)

  assert [item['state'] for item in history] == ['PLANNED', 'LOCKED']
  assert all(item['operation_lane'] == 'sandbox' for item in history)
  assert all(item['lane_session_id'] == lane_session_id for item in history)
  assert summary['table_counts']['pair_plans'] == 1
  assert summary['table_counts']['orders'] == 2
  assert summary['table_counts']['fills'] == 2
  assert summary['table_counts']['pair_states'] == 2
  assert summary['table_counts']['service_heartbeats'] == 1
  assert summary['pair_state_history'][plan.pair_id] == ['PLANNED', 'LOCKED']
  assert summary['pair_lane_session_history'][plan.pair_id] == [lane_session_id, lane_session_id]
  assert all_summary['table_counts']['service_heartbeats'] == 2


# ---------------------------------------------------------------------------
# Z6 — Variant E analytical_snapshots retention
# ---------------------------------------------------------------------------


def test_analytical_snapshots_in_required_tables() -> None:
  """Z6: analytical_snapshots must be part of the REQUIRED_TABLES set."""
  assert 'analytical_snapshots' in REQUIRED_TABLES


def test_initialize_database_creates_analytical_snapshots_table(tmp_path: Path) -> None:
  """Z6: open_database must create the analytical_snapshots table."""
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  rows = connection.execute(
    "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'analytical_snapshots'"
  ).fetchall()
  assert len(rows) == 1


def test_persist_analytical_snapshot_round_trip(tmp_path: Path) -> None:
  """Z6: persist_analytical_snapshot must write a row readable from the DB."""
  import json

  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_analytical_snapshot(
    connection,
    operation_lane='sandbox',
    lane_session_id='sandbox-test-z6-001',
    snapshot_type='near_miss_frontier',
    evidence_class='near_miss',
    recorded_at_utc='2026-05-20T10:00:00Z',
    detail={'near_miss_count': 2, 'evidence_factor': '0.50'},
  )

  rows = connection.execute(
    'SELECT * FROM analytical_snapshots WHERE snapshot_type = ?',
    ('near_miss_frontier',),
  ).fetchall()

  assert len(rows) == 1
  row = rows[0]
  assert row['operation_lane'] == 'sandbox'
  assert row['lane_session_id'] == 'sandbox-test-z6-001'
  assert row['evidence_class'] == 'near_miss'
  assert row['recorded_at_utc'] == '2026-05-20T10:00:00Z'
  detail = json.loads(row['detail_json'])
  assert detail['near_miss_count'] == 2
  assert detail['evidence_factor'] == '0.50'


def test_persist_analytical_snapshot_normalizes_operation_lane(tmp_path: Path) -> None:
  """Z6: operation_lane normalization applies to analytical_snapshots rows."""
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_analytical_snapshot(
    connection,
    operation_lane='  SANDBOX  ',
    lane_session_id=None,
    snapshot_type='density_sizing',
    evidence_class='live_qualifying',
    recorded_at_utc='2026-05-20T11:00:00Z',
  )

  rows = connection.execute(
    'SELECT operation_lane FROM analytical_snapshots'
  ).fetchall()

  assert len(rows) == 1
  assert rows[0]['operation_lane'] == 'sandbox'


def test_persist_analytical_snapshot_multiple_rows_different_lanes(tmp_path: Path) -> None:
  """Z6: Multiple snapshots across different operation lanes are stored independently."""
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  for lane, evidence_class in [('sandbox', 'near_miss'), ('live', 'live_qualifying')]:
    persist_analytical_snapshot(
      connection,
      operation_lane=lane,
      lane_session_id=f'{lane}-session',
      snapshot_type='near_miss_frontier',
      evidence_class=evidence_class,
      recorded_at_utc='2026-05-20T12:00:00Z',
    )

  sandbox_rows = connection.execute(
    "SELECT * FROM analytical_snapshots WHERE operation_lane = 'sandbox'"
  ).fetchall()
  live_rows = connection.execute(
    "SELECT * FROM analytical_snapshots WHERE operation_lane = 'live'"
  ).fetchall()

  assert len(sandbox_rows) == 1
  assert len(live_rows) == 1
  assert sandbox_rows[0]['evidence_class'] == 'near_miss'
  assert live_rows[0]['evidence_class'] == 'live_qualifying'


def test_operator_notifications_round_trip_and_dismissal(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  info_created_at = '2026-06-04T12:00:00Z'
  warn_created_at = '2026-06-04T12:01:00Z'
  live_created_at = '2026-06-04T12:02:00Z'
  sandbox_token = 'profile-token-sandbox'
  live_token = 'profile-token-live'

  info_id = persist_operator_notification(
    connection,
    created_at_utc=info_created_at,
    operation_lane='sandbox',
    profile_token=sandbox_token,
    level='info',
    title='Info title',
    body='Info body',
    source='eligibility',
  )
  warn_id = persist_operator_notification(
    connection,
    created_at_utc=warn_created_at,
    operation_lane='sandbox',
    profile_token=sandbox_token,
    level='warn',
    title='Warn title',
    body='Warn body',
    source='datapack',
  )
  persist_operator_notification(
    connection,
    created_at_utc=live_created_at,
    operation_lane='live',
    profile_token=live_token,
    level='error',
    title='Live title',
    body='Live body',
    source='system',
  )

  visible = fetch_operator_notifications(
    connection,
    operation_lane='sandbox',
    profile_token=sandbox_token,
    now_utc='2026-06-04T15:00:00Z',
  )
  warn_and_above = fetch_operator_notifications(
    connection,
    operation_lane='sandbox',
    profile_token=sandbox_token,
    minimum_level='warn',
    now_utc='2026-06-04T15:00:00Z',
  )

  assert [row['notification_id'] for row in visible] == [warn_id, info_id]
  assert [row['level'] for row in warn_and_above] == ['warn']

  assert dismiss_operator_notification(
    connection,
    notification_id=info_id,
    dismissed_at_utc='2026-06-04T12:05:00Z',
    dismissed_by='operator',
  ) is True

  after_dismiss = fetch_operator_notifications(
    connection,
    operation_lane='sandbox',
    profile_token=sandbox_token,
    now_utc='2026-06-04T15:00:00Z',
  )
  assert [row['notification_id'] for row in after_dismiss] == [warn_id]

  dismissed_count = dismiss_all_operator_notifications(
    connection,
    operation_lane='sandbox',
    profile_token=sandbox_token,
    dismissed_at_utc='2026-06-04T12:06:00Z',
    dismissed_by='operator',
  )
  assert dismissed_count == 1

  assert fetch_operator_notifications(
    connection,
    operation_lane='sandbox',
    profile_token=sandbox_token,
    now_utc='2026-06-04T15:00:00Z',
  ) == []


def test_build_datapack_bundle_includes_manifest_restore_policy_and_payloads(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  plan = _plan()
  persist_pair_plan(
    connection,
    plan,
    created_at_utc='2026-05-21T12:00:00Z',
    operation_lane='sandbox',
  )
  persist_analytical_snapshot(
    connection,
    operation_lane='sandbox',
    lane_session_id='sandbox-stage2-001',
    snapshot_type='candidate_threshold_profile',
    evidence_class='analysis',
    recorded_at_utc='2026-05-21T12:00:01Z',
    detail={'threshold_rank': 3},
  )
  persist_candidate_review_run(
    connection,
    run_id='run-001',
    recorded_at_utc='2026-05-21T12:00:02Z',
    operation_lane='sandbox',
    candidate_signature='sig-001',
    candidate_count=2,
    source_action='find_candidates',
    lane_session_id='sandbox-stage2-001',
  )
  persist_candidate_review_candidates(
    connection,
    run_id='run-001',
    recorded_at_utc='2026-05-21T12:00:03Z',
    operation_lane='sandbox',
    candidates=[
      {'candidate_uid': 'cand-001', 'candidate_key': 'cand-001', 'ticker': 'KALSHI-PAIR-001'},
      {'candidate_uid': 'cand-002', 'candidate_key': 'cand-002', 'ticker': 'KALSHI-PAIR-002'},
    ],
  )
  persist_candidate_saved_set(
    connection,
    saved_set_id='saved-001',
    run_id='run-001',
    recorded_at_utc='2026-05-21T12:00:04Z',
    operation_lane='sandbox',
    saved_key_count=1,
    state_id='review_hold',
    source_action='save_candidates',
    members=[{'candidate_uid': 'cand-001', 'candidate_key': 'cand-001'}],
    lane_session_id='sandbox-stage2-001',
  )
  persist_candidate_saved_set_evaluation(
    connection,
    saved_set_id='saved-001',
    recorded_at_utc='2026-05-21T12:00:05Z',
    operation_lane='sandbox',
    evaluation_status='complete',
    actionability_status='needs_revalidation',
    visibility_status='visible',
    offline_verifiable=True,
    online_revalidation_required=True,
  )

  bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id('sandbox-key-001'),
    profile_token=profile_token_for_key_path(str(tmp_path / 'demo.pem')),
    state_db_path_tail='kalshi.sqlite3',
    include_synthetic_refinement=True,
  )

  manifest = bundle['manifest']
  restore_policy = bundle['restore_policy']
  payloads = bundle['payloads']

  assert manifest['schema_version'] == '2026-05-21.stage2'
  assert manifest['operation_lane'] == 'sandbox'
  assert manifest['api_key_hash'] == api_key_hash_for_id('sandbox-key-001')
  assert manifest['profile_token'].startswith('kalshi-')
  assert manifest['cross_key_import_default'] == 'fail_closed'
  assert restore_policy['default_import_policy']['force_rebind_flag'] == '--force-rebind-api-key-hash'
  assert 'runtime_state' in payloads
  assert 'analytical_state' in payloads
  assert 'candidate_review_history' in payloads
  assert 'synthetic_refinement_fixtures' in payloads
  assert payloads['runtime_state']['tables']['pair_plans']['rows'][0]['operation_lane'] == 'sandbox'


def test_build_datapack_bundle_filters_child_candidate_tables_by_parent_lane(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_candidate_review_run(
    connection,
    run_id='sandbox-run',
    recorded_at_utc='2026-05-21T12:10:00Z',
    operation_lane='sandbox',
    candidate_signature='sandbox-sig',
    candidate_count=1,
    source_action='find_candidates',
  )
  persist_candidate_review_run(
    connection,
    run_id='live-run',
    recorded_at_utc='2026-05-21T12:10:01Z',
    operation_lane='live',
    candidate_signature='live-sig',
    candidate_count=1,
    source_action='find_candidates',
  )
  persist_candidate_review_candidates(
    connection,
    run_id='sandbox-run',
    recorded_at_utc='2026-05-21T12:10:02Z',
    operation_lane='sandbox',
    candidates=[{'candidate_uid': 'sandbox-cand', 'candidate_key': 'sandbox-cand'}],
  )
  persist_candidate_review_candidates(
    connection,
    run_id='live-run',
    recorded_at_utc='2026-05-21T12:10:03Z',
    operation_lane='live',
    candidates=[{'candidate_uid': 'live-cand', 'candidate_key': 'live-cand'}],
  )

  bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id('sandbox-key-001'),
  )

  candidate_rows = bundle['payloads']['candidate_review_history']['tables']['candidate_review_candidates']['rows']
  assert [row['candidate_uid'] for row in candidate_rows] == ['sandbox-cand']


def test_evaluate_datapack_identity_and_rebind_controls(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id('sandbox-key-001'),
  )

  identity_result = evaluate_datapack_identity(
    bundle['manifest'],
    active_operation_lane='sandbox',
    active_api_key_hash=api_key_hash_for_id('other-key-001'),
  )
  rebound_manifest, rebound_restore_policy = rebind_datapack_controls(
    bundle['manifest'],
    bundle['restore_policy'],
    new_api_key_hash=api_key_hash_for_id('other-key-001'),
  )

  assert identity_result['allowed'] is False
  assert identity_result['api_key_hash_match'] is False
  assert 'api_key_hash_mismatch' in identity_result['reasons']
  assert rebound_manifest['restored_under_key_hash'] == api_key_hash_for_id('other-key-001')
  assert rebound_manifest['revalidation_required'] is True
  assert rebound_restore_policy['default_import_policy']['revalidation_required_after_force_rebind'] is True
  assert any(item['revalidation_required'] for item in rebound_restore_policy['family_policies'] if item['restore_mode'] == 'lane_partition_replace')


def test_validate_datapack_artifacts_passes_for_current_schema_bundle(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_pair_plan(
    connection,
    _plan(),
    created_at_utc='2026-05-21T12:20:00Z',
    operation_lane='sandbox',
  )
  bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id('sandbox-key-001'),
  )
  manifest = bundle['manifest']
  restore_policy = bundle['restore_policy']
  payloads = bundle['payloads']

  payload_root = tmp_path / 'datapack' / 'payloads'
  for family_id, payload in payloads.items():
    target = payload_root / f'{family_id}.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, default=str) + '\n', encoding='utf-8')
    manifest['checksums'][f'payloads/{family_id}.json'] = datapack_payload_checksum(payload)
  restore_policy_path = tmp_path / 'datapack' / 'restore_policy.json'
  restore_policy_path.parent.mkdir(parents=True, exist_ok=True)
  restore_policy_path.write_text(json.dumps(restore_policy, indent=2, default=str) + '\n', encoding='utf-8')
  manifest['checksums']['restore_policy.json'] = datapack_payload_checksum(restore_policy)
  manifest['checksums']['manifest.json'] = datapack_manifest_checksum(manifest)
  (tmp_path / 'datapack' / 'manifest.json').write_text(json.dumps(manifest, indent=2, default=str) + '\n', encoding='utf-8')

  issues = validate_datapack_artifacts(tmp_path / 'datapack', manifest, restore_policy)

  assert issues == []


def test_validate_datapack_artifacts_detects_tamper_and_missing_payload(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_pair_plan(
    connection,
    _plan(),
    created_at_utc='2026-05-21T12:21:00Z',
    operation_lane='sandbox',
  )
  bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id('sandbox-key-001'),
  )
  manifest = bundle['manifest']
  restore_policy = bundle['restore_policy']
  payloads = bundle['payloads']

  datapack_root = tmp_path / 'tampered-datapack'
  payload_root = datapack_root / 'payloads'
  for family_id, payload in payloads.items():
    target = payload_root / f'{family_id}.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, default=str) + '\n', encoding='utf-8')
    manifest['checksums'][f'payloads/{family_id}.json'] = datapack_payload_checksum(payload)
  (datapack_root / 'restore_policy.json').write_text(json.dumps(restore_policy, indent=2, default=str) + '\n', encoding='utf-8')
  manifest['checksums']['restore_policy.json'] = datapack_payload_checksum(restore_policy)
  manifest['checksums']['manifest.json'] = datapack_manifest_checksum(manifest)
  (datapack_root / 'manifest.json').write_text(json.dumps(manifest, indent=2, default=str) + '\n', encoding='utf-8')

  runtime_state_path = datapack_root / 'payloads' / 'runtime_state.json'
  runtime_state_payload = json.loads(runtime_state_path.read_text(encoding='utf-8'))
  runtime_state_payload['tables']['pair_plans']['rows'].append({'pair_id': 'tampered'})
  runtime_state_path.write_text(json.dumps(runtime_state_payload, indent=2, default=str) + '\n', encoding='utf-8')
  (datapack_root / 'payloads' / 'candidate_review_history.json').unlink()

  issues = validate_datapack_artifacts(datapack_root, manifest, restore_policy)

  assert 'checksum_mismatch:payloads/runtime_state.json' in issues
  assert 'artifact_missing:payloads/candidate_review_history.json' in issues


def test_validate_datapack_artifacts_detects_policy_family_incoherence(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='synthetic_refinement',
    api_key_hash=api_key_hash_for_id('sandbox-key-001'),
    include_synthetic_refinement=True,
  )
  manifest = bundle['manifest']
  restore_policy = bundle['restore_policy']
  restore_policy['family_policies'][0]['restore_mode'] = 'tampered_mode'

  datapack_root = tmp_path / 'policy-mismatch-datapack'
  payload_root = datapack_root / 'payloads'
  for family_id, payload in bundle['payloads'].items():
    target = payload_root / f'{family_id}.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, default=str) + '\n', encoding='utf-8')
    manifest['checksums'][f'payloads/{family_id}.json'] = datapack_payload_checksum(payload)
  (datapack_root / 'restore_policy.json').write_text(json.dumps(restore_policy, indent=2, default=str) + '\n', encoding='utf-8')
  manifest['checksums']['restore_policy.json'] = datapack_payload_checksum(restore_policy)
  manifest['checksums']['manifest.json'] = datapack_manifest_checksum(manifest)
  (datapack_root / 'manifest.json').write_text(json.dumps(manifest, indent=2, default=str) + '\n', encoding='utf-8')

  issues = validate_datapack_artifacts(datapack_root, manifest, restore_policy)

  assert 'restore_policy_restore_mode_mismatch:runtime_state' in issues


def test_evaluate_datapack_convergence_baseline_convergent_when_loadable_rows_exist(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_pair_plan(
    connection,
    _plan(),
    created_at_utc='2026-05-25T12:21:00Z',
    operation_lane='sandbox',
  )
  bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='synthetic_refinement',
    api_key_hash=api_key_hash_for_id('sandbox-key-001'),
    include_synthetic_refinement=True,
  )

  convergence = evaluate_datapack_convergence(bundle['manifest'], bundle['restore_policy'])

  assert convergence['baseline_family_coverage'] is True
  assert convergence['included_table_row_count'] > 0
  assert convergence['convergence_class'] == 'baseline_convergent'


def test_evaluate_datapack_convergence_proof_only_when_no_loadable_rows_exist(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='synthetic_refinement',
    api_key_hash=api_key_hash_for_id('sandbox-key-001'),
    include_synthetic_refinement=True,
  )

  convergence = evaluate_datapack_convergence(bundle['manifest'], bundle['restore_policy'])

  assert convergence['baseline_family_coverage'] is True
  assert convergence['included_table_row_count'] == 0
  assert convergence['convergence_class'] == 'proof_only_non_loadable'


def test_operator_lane_defaults_table_in_schema(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  tables = {
    row[0]
    for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
  }
  assert 'operator_lane_defaults' in tables, (
    'WD-1: operator_lane_defaults table absent from schema — durable default persistence not initialized'
  )


def test_persist_and_load_lane_defaults_round_trip(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  defaults = {'scan_interval_ms': '4000', 'min_edge_dollars': '0.05'}

  persist_lane_defaults(connection, 'sandbox', defaults)
  loaded = load_lane_defaults(connection, 'sandbox')

  assert loaded['scan_interval_ms'] == '4000', 'WD-2: scan_interval_ms not persisted correctly'
  assert loaded['min_edge_dollars'] == '0.05', 'WD-2: min_edge_dollars not persisted correctly'


def test_persist_lane_defaults_is_idempotent_full_replace(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')

  persist_lane_defaults(connection, 'sandbox', {'scan_interval_ms': '3000', 'min_edge_dollars': '0.04'})
  persist_lane_defaults(connection, 'sandbox', {'scan_interval_ms': '5000'})
  loaded = load_lane_defaults(connection, 'sandbox')

  assert loaded['scan_interval_ms'] == '5000', 'WD-2: second persist should overwrite first'
  assert 'min_edge_dollars' not in loaded, 'WD-2: full replace must clear fields absent from new delta'


def test_persist_lane_defaults_empty_delta_clears_rows(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')

  persist_lane_defaults(connection, 'sandbox', {'scan_interval_ms': '3000'})
  persist_lane_defaults(connection, 'sandbox', {})
  loaded = load_lane_defaults(connection, 'sandbox')

  assert loaded == {}, 'WD-4: persisting empty delta should clear all defaults for the lane'


def test_lane_defaults_are_isolated_by_lane(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')

  persist_lane_defaults(connection, 'sandbox', {'scan_interval_ms': '2000'})
  persist_lane_defaults(connection, 'live', {'scan_interval_ms': '9000'})

  sandbox = load_lane_defaults(connection, 'sandbox')
  live = load_lane_defaults(connection, 'live')

  assert sandbox['scan_interval_ms'] == '2000', 'WD-2: sandbox lane isolation broken'
  assert live['scan_interval_ms'] == '9000', 'WD-2: live lane isolation broken'


def test_persist_candidate_saved_set_members_write_live_operation_lane(tmp_path: Path) -> None:
  """FIX-E3: member rows must carry operation_lane='live' when the saved set is live-lane."""
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_candidate_saved_set(
    connection,
    saved_set_id='saved-live-001',
    run_id=None,
    recorded_at_utc='2026-06-14T12:00:00Z',
    operation_lane='live',
    saved_key_count=2,
    state_id='review_hold_saved_selection_locked',
    source_action='save_candidates',
    members=[
      {'candidate_uid': 'cand-001', 'candidate_key': 'cand-001'},
      {'candidate_uid': 'cand-002', 'candidate_key': 'cand-002'},
    ],
  )
  rows = connection.execute(
    "SELECT operation_lane FROM candidate_saved_set_members WHERE saved_set_id = 'saved-live-001'"
  ).fetchall()
  assert len(rows) == 2, 'FIX-E3: expected two member rows'
  for row in rows:
    assert row['operation_lane'] == 'live', 'FIX-E3: member operation_lane must match saved set lane'


def test_persist_candidate_saved_set_members_write_sandbox_operation_lane(tmp_path: Path) -> None:
  """FIX-E3: member rows must carry operation_lane='sandbox' when the saved set is sandbox-lane."""
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_candidate_saved_set(
    connection,
    saved_set_id='saved-sandbox-001',
    run_id=None,
    recorded_at_utc='2026-06-14T12:01:00Z',
    operation_lane='sandbox',
    saved_key_count=1,
    state_id='review_hold_saved_selection_locked',
    source_action='save_candidates',
    members=[
      {'candidate_uid': 'cand-003', 'candidate_key': 'cand-003'},
    ],
  )
  rows = connection.execute(
    "SELECT operation_lane FROM candidate_saved_set_members WHERE saved_set_id = 'saved-sandbox-001'"
  ).fetchall()
  assert len(rows) == 1, 'FIX-E3: expected one member row'
  assert rows[0]['operation_lane'] == 'sandbox', 'FIX-E3: member operation_lane must be sandbox'


def test_persist_candidate_saved_set_members_lane_overwritten_on_replace(tmp_path: Path) -> None:
  """FIX-E3: re-persisting a saved_set_id overwrites members; new lane must apply to all rows."""
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_candidate_saved_set(
    connection,
    saved_set_id='saved-rewrite-001',
    run_id=None,
    recorded_at_utc='2026-06-14T12:02:00Z',
    operation_lane='live',
    saved_key_count=1,
    state_id='review_hold_saved_selection_locked',
    source_action='save_candidates',
    members=[{'candidate_uid': 'cand-A', 'candidate_key': 'cand-A'}],
  )
  persist_candidate_saved_set(
    connection,
    saved_set_id='saved-rewrite-001',
    run_id=None,
    recorded_at_utc='2026-06-14T12:03:00Z',
    operation_lane='live',
    saved_key_count=2,
    state_id='review_hold_saved_selection_locked',
    source_action='save_candidates',
    members=[
      {'candidate_uid': 'cand-A', 'candidate_key': 'cand-A'},
      {'candidate_uid': 'cand-B', 'candidate_key': 'cand-B'},
    ],
  )
  rows = connection.execute(
    "SELECT operation_lane FROM candidate_saved_set_members WHERE saved_set_id = 'saved-rewrite-001'"
  ).fetchall()
  assert len(rows) == 2, 'FIX-E3: replace must produce exactly the new member count'
  for row in rows:
    assert row['operation_lane'] == 'live', 'FIX-E3: replaced member rows must carry live lane'


def test_open_database_enables_wal_and_concurrency_pragmas(tmp_path: Path) -> None:
  # DB concurrency BMAP R1-R3: WAL + busy_timeout + synchronous=NORMAL set on open.
  connection = open_database(tmp_path / 'concurrency.sqlite3')
  try:
    assert str(connection.execute('PRAGMA journal_mode').fetchone()[0]).lower() == 'wal'
    assert int(connection.execute('PRAGMA busy_timeout').fetchone()[0]) == 5000
    # synchronous NORMAL == 1
    assert int(connection.execute('PRAGMA synchronous').fetchone()[0]) == 1
    # foreign_keys preserved
    assert int(connection.execute('PRAGMA foreign_keys').fetchone()[0]) == 1
  finally:
    connection.close()


def test_wal_reader_not_blocked_by_open_writer(tmp_path: Path) -> None:
  # DB concurrency BMAP R4: under WAL, an uncommitted write transaction on one
  # connection must NOT block a read on a second connection. In the prior rollback-
  # journal mode this read would block until busy_timeout and then raise.
  db_path = tmp_path / 'concurrency.sqlite3'
  writer = open_database(db_path)
  reader = open_database(db_path)
  try:
    # Open a write transaction and leave it uncommitted.
    writer.execute('BEGIN IMMEDIATE')
    writer.execute(
      "INSERT INTO markets_seen (ticker, status, close_time_utc, last_seen_at_utc) "
      "VALUES ('WAL-TEST', 'open', '2026-06-17T00:00:00Z', '2026-06-17T00:00:00Z')"
    )
    # Reader sees the last committed snapshot immediately (no rows yet), without blocking.
    rows = reader.execute("SELECT ticker FROM markets_seen WHERE ticker = 'WAL-TEST'").fetchall()
    assert rows == []
    writer.commit()
    # After commit, a fresh read on the reader sees the row.
    rows_after = reader.execute("SELECT ticker FROM markets_seen WHERE ticker = 'WAL-TEST'").fetchall()
    assert len(rows_after) == 1
  finally:
    writer.close()
    reader.close()


# --- Lane L5c: datapack manifest Ed25519 signature ---

def _l5c_ed25519_keypair():
  import base64 as _b64
  from cryptography.hazmat.primitives import serialization as _ser
  from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
  private_key = Ed25519PrivateKey.generate()
  public_der = private_key.public_key().public_bytes(
    _ser.Encoding.DER, _ser.PublicFormat.SubjectPublicKeyInfo,
  )
  return private_key, _b64.b64encode(public_der).decode('ascii')


def test_l5c_manifest_signature_round_trip_and_tamper(monkeypatch) -> None:
  from polyventure import persistence as _p
  from polyventure import signed_evidence as _se
  private_key, public_b64 = _l5c_ed25519_keypair()
  monkeypatch.setattr(_se, 'trusted_verification_keys', lambda: {'k1': public_b64})
  monkeypatch.setattr(_se, 'load_signing_key', lambda: (private_key, 'k1'))

  manifest = {
    'operation_lane': 'live',
    'inventory': [{'family_id': 'a'}],
    'checksums': {'manifest.json': 'selfsum', 'payloads/a.json': 'hashA'},
  }
  _p.sign_datapack_manifest(manifest)
  assert manifest['signature']['signature_status'] == 'signed'
  assert _p.verify_datapack_manifest_signature(manifest) == ('verified', None)

  # Tampering any signed manifest field (here the lane) breaks verification.
  manifest['operation_lane'] = 'sandbox'
  status, code = _p.verify_datapack_manifest_signature(manifest)
  assert status == 'invalid'
  assert code is not None


def test_l5c_legacy_unsigned_manifest_is_absent_not_invalid() -> None:
  from polyventure import persistence as _p
  assert _p.verify_datapack_manifest_signature({'checksums': {}}) == ('absent', None)


def test_l5c_unsigned_when_no_signing_key(monkeypatch) -> None:
  from polyventure import persistence as _p
  from polyventure import signed_evidence as _se
  monkeypatch.setattr(_se, 'load_signing_key', lambda: None)
  manifest = {'operation_lane': 'live', 'checksums': {}}
  _p.sign_datapack_manifest(manifest)
  assert manifest['signature']['signature_status'] == 'unsigned'


def test_l5c_signed_manifest_passes_own_checksum_and_signature_validation(tmp_path: Path, monkeypatch) -> None:
  # End-to-end: a manifest signed AFTER its checksum is computed must still pass both the manifest
  # checksum check and the signature check at validate time (checksum excludes the signature).
  from polyventure import persistence as _p
  from polyventure import signed_evidence as _se
  private_key, public_b64 = _l5c_ed25519_keypair()
  monkeypatch.setattr(_se, 'trusted_verification_keys', lambda: {'k1': public_b64})
  monkeypatch.setattr(_se, 'load_signing_key', lambda: (private_key, 'k1'))

  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_pair_plan(connection, _plan(), created_at_utc='2026-05-21T12:20:00Z', operation_lane='sandbox')
  bundle = build_datapack_bundle(
    connection, operation_lane='sandbox', datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id('sandbox-key-001'),
  )
  manifest = bundle['manifest']
  restore_policy = bundle['restore_policy']
  payloads = bundle['payloads']

  payload_root = tmp_path / 'datapack' / 'payloads'
  for family_id, payload in payloads.items():
    target = payload_root / f'{family_id}.json'
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(payload, indent=2, default=str) + '\n', encoding='utf-8')
    manifest['checksums'][f'payloads/{family_id}.json'] = datapack_payload_checksum(payload)
  restore_policy_path = tmp_path / 'datapack' / 'restore_policy.json'
  restore_policy_path.parent.mkdir(parents=True, exist_ok=True)
  restore_policy_path.write_text(json.dumps(restore_policy, indent=2, default=str) + '\n', encoding='utf-8')
  manifest['checksums']['restore_policy.json'] = datapack_payload_checksum(restore_policy)
  manifest['checksums']['manifest.json'] = datapack_manifest_checksum(manifest)
  _p.sign_datapack_manifest(manifest)  # sign AFTER checksum, as the writers do
  (tmp_path / 'datapack' / 'manifest.json').write_text(json.dumps(manifest, indent=2, default=str) + '\n', encoding='utf-8')

  issues = validate_datapack_artifacts(tmp_path / 'datapack', manifest, restore_policy)
  assert issues == []
  assert _p.verify_datapack_manifest_signature(manifest) == ('verified', None)
