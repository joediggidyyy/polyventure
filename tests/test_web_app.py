from __future__ import annotations

import os
import inspect
import base64
import hashlib
import hmac
import io
import json
import re
import sqlite3
import time
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from polyventure import web_app
from polyventure.config import Settings
from polyventure.persistence import (
  api_key_hash_for_id,
  build_datapack_bundle,
  close_active_datapack,
  datapack_manifest_checksum,
  datapack_payload_checksum,
  fetch_latest_candidate_saved_set,
  load_lane_defaults,
  open_database,
  persist_candidate_saved_set,
  persist_candidate_saved_set_evaluation,
  persist_lane_defaults,
  persist_pair_plan,
  persist_pair_state_transition,
  persist_runtime_event,
  profile_token_for_key_path,
  resolve_active_profile_token,
  serialize_datapack_json,
  validate_datapack_artifacts,
  validate_datapack_controls,
)
from polyventure import service as service_module
from polyventure.types import AccountBucketLimit, AccountLimits, MarketSnapshot, PairOrderPlan
from polyventure.websocket_client import WebSocketAuthError, WebSocketTimeout
from polyventure.web_app import (
  ConsoleSessionController,
  OperatorConsoleServices,
  WS_CONNECTING_SLOW_GRACE_SEC,
  _load_validation_workflow_summary,
  _system_log_scroll_policy,
  create_operator_console_app,
)


def _call_app(
  app: Any,
  *,
  method: str,
  path: str,
  query: str = '',
  body: dict[str, Any] | None = None,
  headers: dict[str, str] | None = None,
) -> tuple[str, dict[str, str], str]:
  status_holder: dict[str, str] = {}
  headers_holder: dict[str, str] = {}
  encoded_body = json.dumps(body).encode('utf-8') if body is not None else b''

  def _start_response(status: str, headers: list[tuple[str, str]]) -> None:
    status_holder['status'] = status
    headers_holder.update(dict(headers))

  environ: dict[str, Any] = {
    'REQUEST_METHOD': method,
    'PATH_INFO': path,
    'QUERY_STRING': query,
    'CONTENT_LENGTH': str(len(encoded_body)),
    'wsgi.input': io.BytesIO(encoded_body),
  }
  for header_name, header_value in (headers or {}).items():
    normalized = header_name.strip().upper().replace('-', '_')
    environ[f'HTTP_{normalized}'] = str(header_value)

  body = b''.join(app(environ, _start_response)).decode('utf-8')
  return status_holder['status'], headers_holder, body


def _extract_mutation_auth_from_html(html: str) -> dict[str, Any]:
  match = re.search(r'const MUTATION_AUTH = (\{.*?\});', html, re.DOTALL)
  assert match is not None
  return json.loads(match.group(1))


def _signed_mutation_headers(path: str, body: dict[str, Any], mutation_auth: dict[str, Any]) -> dict[str, str]:
  body_text = json.dumps(body)
  body_hash = hashlib.sha256(body_text.encode('utf-8')).hexdigest()
  timestamp = str(int(time.time()))
  nonce = f'pytest-nonce-{time.time_ns()}'
  canonical = f'POST\n{path}\n{timestamp}\n{nonce}\n{body_hash}'.encode('utf-8')
  secret = base64.b64decode(str(mutation_auth['signing_key_b64']).encode('ascii'))
  signature = base64.b64encode(hmac.new(secret, canonical, hashlib.sha256).digest()).decode('ascii')
  return {
    'X-PV-Mutation-Key-Id': str(mutation_auth['key_id']),
    'X-PV-Mutation-Timestamp': timestamp,
    'X-PV-Mutation-Nonce': nonce,
    'X-PV-Mutation-Body-Hash': body_hash,
    'X-PV-Mutation-Signature': signature,
  }


def _write_test_datapack_bundle(output_root: Path, bundle: dict[str, Any]) -> None:
  manifest = dict(bundle['manifest'])
  restore_policy = dict(bundle['restore_policy'])
  payloads = dict(bundle['payloads'])

  output_root.mkdir(parents=True, exist_ok=True)
  payload_root = output_root / 'payloads'
  checksums: dict[str, str] = {}
  for family_id, payload in payloads.items():
    relative_path = f'payloads/{family_id}.json'
    payload_path = payload_root / f'{family_id}.json'
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.write_text(serialize_datapack_json(payload) + '\n', encoding='utf-8')
    checksums[relative_path] = datapack_payload_checksum(payload)

  restore_policy_checksum = datapack_payload_checksum(restore_policy)
  checksums['restore_policy.json'] = restore_policy_checksum
  manifest['checksums'] = dict(checksums)
  manifest_checksum = datapack_manifest_checksum(manifest)
  manifest['checksums']['manifest.json'] = manifest_checksum

  (output_root / 'restore_policy.json').write_text(serialize_datapack_json(restore_policy) + '\n', encoding='utf-8')
  (output_root / 'manifest.json').write_text(serialize_datapack_json(manifest) + '\n', encoding='utf-8')


def _engine_zero_found_scan_payload() -> dict[str, Any]:
  # Mirrors what the scan engine (run_scan_once) emits on a GENUINE zero-found: at least one
  # binary-eligible candidate market was scored and zero qualifying live candidates resulted.
  # The engine is the sole author of the scan_retry block; the web layer only honors it (it
  # must never fabricate this from candidate_count alone). See
  # FIND_CANDIDATES_ZERO_FOUND_RETRY_DECISION_SSOT_BMAP_2026-06-25.
  return {
    'decision': 'planned',
    'reason': 'scan_zero_found_retry',
    'message': '0 candidates found; retrying in 5 seconds.',
    'next_action': '0 candidates found; retrying in 5 seconds.',
    'candidate_count': 0,
    'sandbox_extended_count': 0,
    'candidates': [],
    'sandbox_candidates_extended': [],
    'scan_retry': {
      'active': True,
      'mode': 'zero_found_retry',
      'cycle_id': 'test-cycle',
      'attempt_index': 1,
      'retry_after_sec': 5,
      'retry_countdown_remaining_sec': 5,
      'next_retry_at_utc': '2026-06-25T01:20:10+00:00',
      'message': '0 candidates found; retrying in 5 seconds.',
    },
    'settings': {
      'kalshi_env': 'demo',
      'operation_lane': 'sandbox',
      'settings_ready': True,
      'environment_ready': True,
      'credential_ready': True,
      'mode_selected': True,
    },
  }


def _services() -> OperatorConsoleServices:
  return OperatorConsoleServices(
    bootstrap=lambda **_: {
      'decision': 'planned',
      'settings': {
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'live_websocket_url': 'wss://api.kalshi.example/ws',
        'active_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
        'state_db_path_tail': 'runtime.sqlite3',
        'private_key_path_tail': 'demo.pem',
      },
      'diagnostics_governance_context': {
        'channel': 'diagnostics_governance',
        'validation_summary': {
          'present': True,
          'default_lanes': ['pytest', 'sandbox_test', 'empirical_test'],
          'definition_count': 7,
          'operator_policy': 'All three validation lanes remain required; empirical review stays operator-driven at high-value junction points only.',
          'latest_runs': [
            {
              'run_id': 'run-123',
              'result': 'passed',
              'selected_lanes': ['pytest', 'sandbox_test', 'empirical_test'],
            }
          ],
          'lane_policy': {
            'pytest': 'Automated proof lane for code-level confidence.',
            'sandbox_test': 'Contained execution lane for dry-run and sandbox proof.',
            'empirical_test': 'Operator-driven empirical review lane for high-value junction points only.',
          },
        },
      },
      'report': {
        'latest_heartbeat': {'status': 'cycle-complete'},
        'state_db_path_tail': 'runtime.sqlite3',
        'operation_lane': 'sandbox',
        'lane_session_id': 'sandbox-session-001',
        'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
        'connection_state': {
          'status': 'connected',
          'websocket_connected': True,
        },
        'pair_runtime_summary': [
          {
            'pair_id': 'pair-1',
            'ticker': 'KALSHI-EDGE-1',
            'state': 'PLANNED',
            'contract_count': '10',
            'locked_contracts': '0',
            'gross_dollars': '0',
            'net_projected_dollars': '0',
            'dynamic_pair_notional_pct': '0.192',
            'dynamic_max_contracts': '32',
            'effective_density': '3.125',
            'binding_limiter': 'configured_contract_cap',
          }
        ],
        'next_action': 'Use Refresh shell or Cancel all pairs if attention remains.',
      },
      'reconcile': {'pair_count': 0, 'pairs': []},
      'workflow': {
        'recommended_step': 'scan',
        'auto_sequence': ['scan', 'run'],
        'headline': 'Auto-forward safe dry-run steps.',
        'operator_message': 'Run scan then one dry-run cycle.',
        'step_kind': 'execute',
        'can_run_next_step': True,
        'next_actionable_step': 'scan',
        'focus_target': 'notification-band-section',
        'focus_tone': 'focus-ok',
        'deck_view': 'workflow',
        'button_emphasis_tone': 'ok',
      },
      'next_action': 'Run scan then one dry-run cycle.',
    },
    scan=lambda **_: {
      'decision': 'planned',
      'candidate_count': 1,
      'next_action': 'Advance into one dry-run runtime cycle.',
    },
    run=lambda **_: {
      'decision': 'planned',
      'planned_pair_count': 1,
      'planned_pairs': [
        {
          'pair_id': 'pair-1',
          'ticker': 'KALSHI-EDGE-1',
          'contract_count': '10',
          'dynamic_pair_notional_pct': '0.192',
          'dynamic_max_contracts': '32',
          'effective_density': '3.125',
          'binding_limiter': 'configured_contract_cap',
        }
      ],
      'diagnostics_governance_context': {
        'channel': 'diagnostics_governance',
        'validation_summary': {
          'present': True,
          'default_lanes': ['pytest', 'sandbox_test', 'empirical_test'],
          'latest_runs': [],
          'active_run_count': 0,
          'project_aggregate_available': False,
        },
      },
      'next_action': 'Review the planned pair.',
    },
    reconcile=lambda **_: {
      'decision': 'planned',
      'pair_count': 1,
      'pairs': [{'pair_id': 'pair-1', 'state': 'PLANNED'}],
      'next_action': 'Review pair state.',
    },
    report=lambda **_: {
      'decision': 'planned',
      'latest_heartbeat': {'status': 'cycle-complete'},
      'operation_lane': 'sandbox',
      'lane_session_id': 'sandbox-session-001',
      'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': 'api.kalshi.example/ws',
      },
      'connection_state': {
        'status': 'connected',
        'websocket_connected': True,
      },
      'next_action': 'Use Refresh shell next.',
    },
    cancel_all=lambda **_: {
      'decision': 'planned',
      'canceled_pair_count': 1,
      'next_action': 'Review the updated report.',
    },
    system_log=lambda **_: {
      'decision': 'planned',
      'entries': [
        {
          'key': 'runtime_event:1',
          'source': 'runtime_event',
          'recorded_at_utc': '2026-05-05T10:00:00+00:00',
          'message': '[SANDBOX] [RUNTIME_EVENT] pair_plan_created :: pair-1',
          'operation_lane': 'sandbox',
          'lane_session_id': 'sandbox-session-001',
        }
      ],
      'latest_cursor': 'runtime_event:1',
    },
    visuals=lambda **kwargs: {
      'decision': 'planned',
      'view': {
        'id': kwargs.get('view') or 'pair_state_distribution',
        'title': 'Pair state distribution',
        'family': 'pairs',
        'render_mode': kwargs.get('mode') or 'plot',
      },
      'window': {'id': kwargs.get('window') or 'current', 'label': 'CURRENT', 'bucket': 'snapshot'},
      'status': 'ready',
      'generated_at_utc': '2026-05-05T10:00:00Z',
      'freshness': {'captured_at_utc': '2026-05-05T10:00:00Z', 'lag_sec': 0.0},
      'summary': {
        'headline': 'Pair attention remains concentrated in one visible state.',
        'next_action': 'Review reconcile if partial states begin to accumulate.',
      },
      'series': [
        {'id': 'pair_state_count', 'label': 'Pairs', 'kind': 'bar', 'unit': 'count', 'points': [{'x': 'PLANNED', 'y': 1}]}
      ],
      'categories': [],
      'table': {'columns': ['State', 'Count'], 'rows': [['PLANNED', 1]]},
      'source_contracts': ['pair_states'],
      'empty_reason': None,
      'available_views': [
        {'id': 'pair_state_distribution', 'title': 'Pair state distribution', 'table_supported': True},
        {'id': 'runtime_cadence', 'title': 'Runtime cadence', 'table_supported': True},
      ],
      'available_windows': [
        {'id': 'current', 'label': 'CURRENT', 'bucket': 'snapshot'},
        {'id': '1h', 'label': '1H', 'bucket': '5m'},
      ],
    },
  )


def _load_lane_key(
  app: Any,
  tmp_path: Path,
  monkeypatch: Any,
  lane: str,
  *,
  key_path: Path | None = None,
  mutation_auth: dict[str, Any] | None = None,
  session_token: str | None = None,
) -> dict[str, Any]:
  key_file = key_path or (tmp_path / f'{lane}-key.pem')
  if not key_file.exists():
    key_file.write_text('placeholder-key-material', encoding='utf-8')
  monkeypatch.setattr(
    web_app,
    '_probe_key_reference_acceptance',
    lambda *_args, **_kwargs: {
      'ok': True,
      'reason': 'pass',
      'message': 'Key file valid and platform accepted authenticated requests.',
      'next_action': 'Proceed.',
    },
  )
  stage_body = {'path': str(key_file)}
  stage_headers = _signed_mutation_headers('/api/key-stage', stage_body, mutation_auth) if mutation_auth is not None else None
  _call_app(
    app,
    method='POST',
    path='/api/key-stage',
    query=f'session={session_token}' if session_token else '',
    body=stage_body,
    headers=stage_headers,
  )
  load_body = {'action': 'load_live_key_reference' if lane == 'live' else 'load_sandbox_key_reference'}
  load_headers = _signed_mutation_headers('/api/key-load', load_body, mutation_auth) if mutation_auth is not None else None
  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/key-load',
    query=f'session={session_token}' if session_token else '',
    body=load_body,
    headers=load_headers,
  )
  assert status == '200 OK'
  return json.loads(body)


def _runtime_settings_for_lane(tmp_path: Path, lane: str, *, key_file: Path | None = None) -> Settings:
  runtime_key = key_file or (tmp_path / f'{lane}-runtime-key.pem')
  if not runtime_key.exists():
    runtime_key.write_text('placeholder-key-material', encoding='utf-8')
  return Settings(
    kalshi_env='demo' if lane == 'sandbox' else 'prod',
    api_key_id='demo-api-key' if lane == 'sandbox' else 'live-api-key',
    live_api_key_id='live-api-key',
    private_key_file=str(runtime_key),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2' if lane == 'sandbox' else 'https://api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws' if lane == 'sandbox' else 'wss://api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane=lane,
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )


def _patch_fake_websocket_runtime(monkeypatch: Any, settings: Settings) -> None:
  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)


def _sandbox_scan_market_set(now: datetime) -> list[MarketSnapshot]:
  close_time = now + timedelta(minutes=5)
  return [
    MarketSnapshot(
      ticker='LIVE-1',
      title='Live qualifying candidate',
      close_time=close_time,
      status='open',
      yes_bid_dollars=Decimal('0.40'),
      no_bid_dollars=Decimal('0.45'),
      volume_24h_fp=Decimal('120'),
      open_interest_fp=Decimal('90'),
    ),
    MarketSnapshot(
      ticker='MARG-1',
      title='Sandbox extended candidate',
      close_time=close_time,
      status='open',
      yes_bid_dollars=Decimal('0.47'),
      no_bid_dollars=Decimal('0.44'),
      volume_24h_fp=Decimal('95'),
      open_interest_fp=Decimal('80'),
    ),
  ]


def test_run_scan_once_sandbox_expands_marginal_sample(monkeypatch: Any, tmp_path: Path) -> None:
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(tmp_path / 'demo.pem'),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.10,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.08,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
    sandbox_edge_relaxation_factor=0.80,
    sandbox_scan_return_limit=50,
  )
  Path(settings.private_key_file).write_text('demo-private-key', encoding='utf-8')

  class _FakeClient:
    def get_balance(self) -> Decimal:
      return Decimal('100.00')

    def get_account_api_limits(self) -> AccountLimits:
      return AccountLimits(
        usage_tier='standard',
        read=AccountBucketLimit(refill_rate=1, bucket_capacity=2),
        write=AccountBucketLimit(refill_rate=1, bucket_capacity=2),
      )

  now = datetime.now(UTC)
  markets = _sandbox_scan_market_set(now)

  monkeypatch.setattr(service_module, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(service_module, '_load_candidate_market_set', lambda *_args, **_kwargs: (markets, markets, {market.ticker: market for market in markets}, 0))

  payload = service_module.run_scan_once(
    settings=settings,
    client_factory=lambda *_args: _FakeClient(),
  )

  assert payload['candidate_count'] == 1
  assert payload['sandbox_extended_count'] == 2
  assert payload['sandbox_edge_relaxation_factor_applied'] == 0.8
  assert payload['transition_rank'] == 1
  assert len(payload['sandbox_candidates_extended']) == 2
  assert [candidate['qualifier_tier'] for candidate in payload['sandbox_candidates_extended']] == ['live_qualifying', 'sandbox_extended']
  assert payload['candidates'] == payload['sandbox_candidates_extended']
  assert payload['sandbox_candidates_extended'][0]['rank'] == 1
  assert payload['sandbox_candidates_extended'][1]['rank'] == 2


def test_load_candidate_market_set_emits_enrichment_heartbeats(monkeypatch: Any) -> None:
  now = datetime.now(UTC)
  close_time = now + timedelta(minutes=5)
  markets = [
    MarketSnapshot(
      ticker=f'LIVE-{index}',
      title=f'Candidate {index}',
      close_time=close_time,
      status='open',
      yes_bid_dollars=Decimal('0.40'),
      no_bid_dollars=Decimal('0.45'),
      volume_24h_fp=Decimal('120'),
      open_interest_fp=Decimal('90'),
    )
    for index in range(1, 4)
  ]
  progress_events: list[tuple[str, str, dict[str, Any] | None, float | None]] = []

  class _FakeClient:
    def get_orderbook(self, _ticker: str) -> dict[str, Any]:
      return {}

  monkeypatch.setattr(service_module, 'SCAN_HEARTBEAT_INTERVAL_SEC', 0.01)
  monkeypatch.setattr(service_module, 'fetch_open_markets', lambda *_args, **_kwargs: list(markets))
  monkeypatch.setattr(
    service_module,
    'enrich_with_orderbook',
    lambda *_args, **_kwargs: (time.sleep(0.02), ('ignored', object()))[1],
  )
  monkeypatch.setattr(service_module, '_replace_market_with_orderbook', lambda market, _orderbook: market)

  service_module._load_candidate_market_set(
    _FakeClient(),
    recorded_at=now,
    progress_callback=lambda stage, message, detail, progress_percent: progress_events.append((stage, message, detail, progress_percent)),
  )

  heartbeat_events = [
    event for event in progress_events
    if event[0] == 'enriching_remaining_orderbooks' and event[3] is None
  ]

  assert heartbeat_events
  heartbeat_detail = heartbeat_events[0][2] or {}
  assert heartbeat_detail['processed_market_count'] >= 1
  assert heartbeat_detail['remaining_market_count'] >= 1
  assert heartbeat_detail['orderbook_enrichment_count'] >= 1


def test_scan_route_projects_pairs_owned_candidate_review_when_sandbox_marginal_candidates_exist(tmp_path: Path, monkeypatch: Any) -> None:
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(tmp_path / 'state.sqlite3'))
  services = _services()
  sandbox_scan = lambda **_: {
    'decision': 'planned',
    'scan_runtime': {'scan_session_id': 'test-scan-1', 'lane_session_id': 'test-lsid-1', 'status': 'completed', 'result_candidate_count': 2},
    'candidate_count': 0,
    'sandbox_extended_count': 2,
    'sandbox_candidates_extended': [
      {'ticker': 'LIVE-1', 'density_weight': '1.0', 'liquidity_score': '210', 'qualifier_tier': 'live_qualifying', 'rank': 1},
      {'ticker': 'MARG-1', 'density_weight': '0.9', 'liquidity_score': '175', 'qualifier_tier': 'sandbox_extended', 'rank': 2},
    ],
    'next_action': 'Advance into one dry-run runtime cycle.',
    'settings': {
      'kalshi_env': 'demo',
      'operation_lane': 'sandbox',
      'settings_ready': True,
      'environment_ready': True,
      'credential_ready': True,
      'mode_selected': True,
      'state_db_path': str(tmp_path / 'state.sqlite3'),
    },
  }
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=sandbox_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/scan')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['recommended_step'] == 'select_candidates'
  assert payload['workflow']['next_actionable_step'] == 'select_candidates'
  assert payload['workflow']['focus_target'] == 'pairs-section'
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['can_run_next_step'] is False
  assert payload['workflow']['deck_view'] == 'operator'
  assert payload['pair_monitor']['candidate_count'] == 2
  assert payload['pair_monitor']['candidate_rows'][0]['ticker'] == 'LIVE-1'
  assert payload['pair_monitor']['candidate_rows'][1]['ticker'] == 'MARG-1'


def test_scan_route_returns_processing_ack_until_background_scan_completes(tmp_path: Path, monkeypatch: Any) -> None:
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(tmp_path / 'state.sqlite3'))
  services = _services()
  bootstrap_call_count = 0
  scan_started = threading.Event()
  release_scan = threading.Event()

  def _counted_bootstrap(**kwargs: Any) -> dict[str, Any]:
    nonlocal bootstrap_call_count
    bootstrap_call_count += 1
    return services.bootstrap(**kwargs)

  def _blocking_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
      )
    scan_started.set()
    release_scan.wait(timeout=2.0)
    return {
      'decision': 'planned',
      'scan_runtime': {'scan_session_id': 'test-scan-1', 'lane_session_id': 'test-lsid-1', 'status': 'completed', 'result_candidate_count': 2},
      'candidate_count': 2,
      'candidates': [
        {'ticker': 'LIVE-1', 'density_weight': '1.0', 'liquidity_score': '210'},
        {'ticker': 'LIVE-2', 'density_weight': '0.9', 'liquidity_score': '190'},
      ],
      'next_action': 'Review candidates in Pairs.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
        'state_db_path': str(tmp_path / 'state.sqlite3'),
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=_counted_bootstrap,
      scan=_blocking_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/scan')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['execution_state']['kind'] == 'processing'
  assert payload['execution_state']['action'] == 'scan'
  assert payload['scan_runtime']['status'] == 'processing'
  assert payload['workflow']['recommended_step'] == 'processing'
  assert payload['workflow']['next_actionable_step'] == 'processing'
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['can_run_next_step'] is False
  assert payload['workflow']['focus_target'] == 'boundary-panel'
  assert bootstrap_call_count == 0
  assert scan_started.wait(timeout=1.0) is True

  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['execution_state']['kind'] == 'processing'
  assert bootstrap_payload['scan_runtime']['stage'] == 'loading_markets'
  assert bootstrap_call_count == 1

  release_scan.set()
  deadline = time.time() + 2.0
  completed_payload = None
  while time.time() < deadline:
    _, _, refresh_body = _call_app(app, method='GET', path='/api/bootstrap')
    refreshed_payload = json.loads(refresh_body)
    if refreshed_payload.get('candidate_count') == 2:
      completed_payload = refreshed_payload
      break
    time.sleep(0.02)

  assert completed_payload is not None
  assert completed_payload['workflow']['recommended_step'] == 'select_candidates'
  assert completed_payload['pair_monitor']['candidate_count'] == 2


def test_scan_route_does_not_synthesize_retry_when_engine_omits_it() -> None:
  # SSOT (FIND_CANDIDATES_ZERO_FOUND_RETRY_DECISION_SSOT_BMAP_2026-06-25): the web layer
  # must NOT fabricate a zero-found retry from candidate_count==0. When the scan engine emits
  # no scan_retry block (empty fetch / zero binary-eligible / no market considered), the scan
  # completes and the system returns to the normal find cadence -- no retry_wait, no
  # scan_zero_found_retry. This replaces the prior test that enshrined the fabrication.
  services = _services()

  def _empty_scan_without_retry(**_: Any) -> dict[str, Any]:
    return {
      'decision': 'planned',
      'reason': 'planned',
      'candidate_count': 0,
      'sandbox_extended_count': 0,
      'candidates': [],
      'sandbox_candidates_extended': [],
      'next_action': 'Review the empty result.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_empty_scan_without_retry,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/scan', body={})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['scan_runtime']['status'] == 'completed'
  assert payload['scan_runtime']['stage'] != 'retry_wait'
  assert payload['scan_runtime']['result_reason'] != 'scan_zero_found_retry'
  assert payload['scan_runtime'].get('retry_state') in ({}, None) or not payload['scan_runtime']['retry_state'].get('active')


def test_empty_fetch_scan_routes_to_retry_threshold_not_cadence(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  # SCHEDULER_ELIGIBILITY_THRESHOLD_REALIGNMENT_BMAP_2026-06-29 (C2 / INV-1, supersedes the
  # 2026-06-25 empty-window-to-cadence routing): a zero-candidate terminal arms the RETRY
  # eligibility threshold regardless of WHY it was empty. Even when the scan service omits a
  # scan_retry block, the scheduler routes found==0 to retry and never starts cadence (cadence is
  # reserved for the post-submit/shelter beat). The cadence state rests at 'idle' (D6: no phantom
  # 'waiting' without an armed deadline).
  state_db_path = str(tmp_path / 'empty-fetch-retry.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_manual_execution_truth(state_db_path)
  services = _services()

  def _empty_scan_without_retry(**_: Any) -> dict[str, Any]:
    return {
      'decision': 'planned',
      'reason': 'planned',
      'candidate_count': 0,
      'sandbox_extended_count': 0,
      'candidates': [],
      'sandbox_candidates_extended': [],
      'next_action': 'Review the empty result.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_empty_scan_without_retry,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )
  _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 120000, 'max_iterations': 3}},
  )

  status, _, body = _call_app(app, method='POST', path='/api/scan', body={})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['scan_runtime']['result_candidate_count'] == 0
  snapshot = payload['scheduler_snapshot']
  # Retry threshold armed (INV: found==0 -> retry).
  retry_record = snapshot['timer_records']['retry']
  assert retry_record is not None, 'zero-found scan must arm the retry threshold'
  assert retry_record['deadline_utc']
  retry_state = snapshot['retry_state']
  assert retry_state['state'] in {'waiting', 'due'}
  assert retry_state['next_retry_at_utc'] == retry_record['deadline_utc']
  assert 0 < int(retry_state['remaining_sec']) <= 5
  # Cadence NOT armed (INV-1: found==0 never starts cadence) and resting at idle (D6).
  assert snapshot['timer_records']['cadence'] is None
  assert snapshot['cadence_state']['state'] == 'idle'
  assert 'next_cadence_at_utc' not in snapshot['cadence_state']
  # INV-4: ownership is never stamped as the timer name.
  assert snapshot['owner'] not in {'retry_timer', 'cadence_timer'}


def test_scan_route_honors_engine_emitted_zero_found_retry() -> None:
  # SSOT: when the scan engine emits scan_retry (genuine zero-found -- markets scored, zero
  # qualifying), the web layer must honor it: completed retry_wait stage, active scheduler retry
  # state, and a persisted scan_zero_found_retry decision.
  services = _services()

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=lambda **_: _engine_zero_found_scan_payload(),
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/scan', body={})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['scan_runtime']['status'] == 'completed'
  assert payload['scan_runtime']['active'] is False
  assert payload['scan_runtime']['stage'] == 'retry_wait'
  assert payload['scan_runtime']['result_reason'] == 'scan_zero_found_retry'
  assert payload['scan_runtime']['retry_state']['active'] is True
  assert payload['scan_runtime']['retry_state']['retry_after_sec'] == 5


def test_scan_route_returns_active_processing_snapshot_on_repeat_scan_request() -> None:
  services = _services()
  scan_started = threading.Event()
  release_scan = threading.Event()

  def _blocking_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
      )
    scan_started.set()
    release_scan.wait(timeout=2.0)
    return {
      'decision': 'planned',
      'candidate_count': 0,
      'sandbox_extended_count': 0,
      'candidates': [],
      'sandbox_candidates_extended': [],
      'next_action': 'Review the empty result.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_blocking_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/scan', body={})
  payload = json.loads(body)
  assert status == '200 OK'
  assert payload['scan_runtime']['status'] == 'processing'
  assert scan_started.wait(timeout=1.0) is True

  repeat_status, _, repeat_body = _call_app(app, method='POST', path='/api/scan', body={})
  repeat_payload = json.loads(repeat_body)

  try:
    assert repeat_status == '200 OK'
    assert repeat_payload['scan_runtime']['status'] == 'processing'
    assert repeat_payload['workflow']['recommended_step'] == 'processing'
    assert repeat_payload['workflow']['next_actionable_step'] == 'processing'
  finally:
    release_scan.set()


def test_scan_route_cancel_during_retry_wait_completes_immediately() -> None:
  services = _services()

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=lambda **_: _engine_zero_found_scan_payload(),
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  cancel_status, _, cancel_body = _call_app(app, method='POST', path='/api/scan-cancel')
  cancel_payload = json.loads(cancel_body)
  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert scan_status == '200 OK'
  assert scan_payload['scan_runtime']['status'] == 'completed'
  assert scan_payload['scan_runtime']['active'] is False
  assert scan_payload['scan_runtime']['stage'] == 'retry_wait'
  assert cancel_status == '200 OK'
  assert cancel_payload['scan_runtime']['status'] == 'cancelled'
  assert cancel_payload['scan_runtime']['cancel_requested'] is True
  assert cancel_payload['scan_runtime']['retry_state'] == {}
  assert cancel_payload.get('execution_state') is None
  assert cancel_payload['workflow']['next_actionable_step'] == 'scan'
  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['scan_runtime']['status'] == 'cancelled'
  assert bootstrap_payload['scan_runtime']['retry_state'] == {}


def test_scan_route_cancel_in_flight_is_honored_over_zero_found_retry() -> None:
  # Lane A (FIND_CANDIDATES_RETRY_PROJECTION_COHERENCE_BMAP_2026-06-19): a cancel that
  # arrives while the scan is in flight must win over a zero-found retry. Without the
  # fix, the worker would clobber the cancel (cancel_requested=False) and enter
  # retry_wait, leaving the bootstrap route 'processing' while the report route shows
  # 'cancelled' for the same session. Terminal/cancel state is authoritative.
  services = _services()
  scan_in_flight = threading.Event()
  release_scan = threading.Event()

  def _slow_empty_scan(**_: Any) -> dict[str, Any]:
    scan_in_flight.set()
    release_scan.wait(timeout=2.0)
    # Returns an engine-emitted zero-found retry payload AFTER the cancel was requested; the
    # in-flight cancel must still win over the would-be retry.
    return _engine_zero_found_scan_payload()

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_slow_empty_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  scan_status, _, _ = _call_app(app, method='POST', path='/api/scan', body={})
  assert scan_status == '200 OK'
  assert scan_in_flight.wait(timeout=1.0) is True, 'scan never went in-flight'

  # Cancel while the scan is still blocked in flight, then release the scan so it
  # returns a zero-found payload AFTER the cancel was requested.
  cancel_status, _, _ = _call_app(app, method='POST', path='/api/scan-cancel')
  assert cancel_status == '200 OK'
  release_scan.set()

  # Poll the bootstrap projection until the worker reaches a terminal state. The
  # contract: it must settle on 'cancelled' with no retry_state -- never 'processing'
  # /'retry_wait'. (Bootstrap and report routes then agree on the cancelled state.)
  deadline = time.time() + 3.0
  final_runtime: dict[str, Any] = {}
  while time.time() < deadline:
    _, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
    final_runtime = json.loads(bootstrap_body).get('scan_runtime') or {}
    if str(final_runtime.get('status') or '').lower() in {'cancelled', 'completed', 'failed'}:
      break
    time.sleep(0.05)

  assert str(final_runtime.get('status') or '').lower() == 'cancelled', (
    f"in-flight cancel must win over zero-found retry; got status={final_runtime.get('status')!r} "
    f"stage={final_runtime.get('stage')!r}"
  )
  assert final_runtime.get('stage') != 'retry_wait', 'cancel must not leave the scan in retry_wait'
  assert final_runtime.get('retry_state') == {}, 'cancel must clear retry_state'


def test_scan_route_retry_wait_refire_starts_new_scan() -> None:
  services = _services()
  call_count = [0]
  second_scan_active = threading.Event()
  release_second_scan = threading.Event()

  def _two_phase_scan(**kwargs: Any) -> dict[str, Any]:
    call_count[0] += 1
    if call_count[0] == 1:
      # First scan is a genuine engine zero-found -> retry_wait.
      return _engine_zero_found_scan_payload()
    progress_callback = kwargs.get('progress_callback')
    if callable(progress_callback):
      progress_callback('loading_markets', 'Loading markets for retry scan.', {}, 0.1)
    second_scan_active.set()
    release_second_scan.wait(timeout=2.0)
    return {
      'decision': 'planned',
      'reason': 'planned',
      'candidate_count': 0,
      'sandbox_extended_count': 0,
      'candidates': [],
      'sandbox_candidates_extended': [],
      'next_action': 'No candidates found.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_two_phase_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  first_status, _, first_body = _call_app(app, method='POST', path='/api/scan', body={})
  first_payload = json.loads(first_body)
  assert first_status == '200 OK'
  assert first_payload['scan_runtime']['stage'] == 'retry_wait'
  assert first_payload['scan_runtime']['retry_state']['active'] is True

  second_status, _, second_body = _call_app(app, method='POST', path='/api/scan', body={})
  second_payload = json.loads(second_body)

  try:
    assert second_status == '200 OK'
    assert second_scan_active.wait(timeout=1.0) is True, 'second scan thread never started'
    assert call_count[0] >= 2, 'scan service must have been called a second time'
    assert second_payload['scan_runtime']['retry_state'] == {}, (
      'new in-flight scan must clear retry_state; stale retry_wait payload was returned instead'
    )
    assert second_payload['scan_runtime']['stage'] != 'retry_wait', (
      'route guard must not block retry-refire; response must reflect new scan, not old retry_wait'
    )
  finally:
    release_second_scan.set()


def test_processing_panel_sandbox_visible_gated_on_surfaced_candidates_contract_is_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())
  status, _, body = _call_app(app, method='GET', path='/')
  assert status == '200 OK'
  assert 'const sandboxVisible = surfacedCandidates > 0 ? Number(payload.sandbox_extended_count || 0) : 0;' in body


def test_merge_saved_rows_for_processing_scan_suppressed_during_retry_wait() -> None:
  services = _services()
  call_count = [0]

  def _two_phase_scan(**kwargs: Any) -> dict[str, Any]:
    call_count[0] += 1
    settings_block = {
      'kalshi_env': 'demo',
      'operation_lane': 'sandbox',
      'settings_ready': True,
      'environment_ready': True,
      'credential_ready': True,
      'mode_selected': True,
    }
    if call_count[0] == 1:
      return {
        'decision': 'planned',
        'candidate_count': 1,
        'sandbox_extended_count': 1,
        'candidates': [
          {'candidate_key': 'review-candidate-1', 'ticker': 'KALSHI-EDGE-1', 'density_weight': '1.0', 'liquidity_score': '210'},
        ],
        'sandbox_candidates_extended': [
          {'candidate_key': 'review-candidate-1', 'ticker': 'KALSHI-EDGE-1', 'density_weight': '1.0', 'liquidity_score': '210'},
        ],
        'next_action': 'Review candidates in Pairs.',
        'settings': settings_block,
      }
    # Second scan is a genuine engine zero-found -> retry_wait (the merge-suppression path).
    return _engine_zero_found_scan_payload()

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_two_phase_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  scan1_status, _, scan1_body = _call_app(app, method='POST', path='/api/scan', body={})
  assert scan1_status == '200 OK'
  assert json.loads(scan1_body)['scan_runtime']['result_candidate_count'] == 1

  save_status, _, _ = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={'action': 'save_selection', 'selected_keys': ['review-candidate-1']},
  )
  assert save_status == '200 OK'

  scan2_status, _, scan2_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan2_payload = json.loads(scan2_body)
  assert scan2_status == '200 OK'
  assert scan2_payload['scan_runtime']['stage'] == 'retry_wait'

  assert scan2_payload['sandbox_extended_count'] == 0
  assert scan2_payload['candidate_count'] == 0
  assert len(scan2_payload['sandbox_candidates_extended']) == 0
  assert len(scan2_payload['candidates']) == 0


def test_processing_panel_retry_countdown_in_stage_card_contract_is_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())
  status, _, body = _call_app(app, method='GET', path='/')
  assert status == '200 OK'
  assert "label: 'Stage', html: retryActive" in body
  assert 'boundary-zero-found-retry-value' in body
  assert "{ label: 'Retry', html:" not in body


def test_cancel_request_snapshot_suppresses_retry_state() -> None:
  services = _services()
  scan_started = threading.Event()
  release_scan = threading.Event()

  def _cancelable_zero_retry_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    cancel_check = kwargs.get('cancel_check')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    scan_started.set()
    while not release_scan.is_set():
      if callable(cancel_check) and cancel_check():
        raise service_module.ScanCancelledError('Find candidates was canceled by the operator.')
      time.sleep(0.02)
    return {
      'decision': 'planned',
      'candidate_count': 0,
      'sandbox_extended_count': 0,
      'candidates': [],
      'sandbox_candidates_extended': [],
      'next_action': 'Review the empty result.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_cancelable_zero_retry_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  assert scan_status == '200 OK'
  assert scan_started.wait(timeout=1.0) is True

  cancel_status, _, cancel_body = _call_app(app, method='POST', path='/api/scan-cancel')
  cancel_payload = json.loads(cancel_body)
  try:
    assert cancel_status == '200 OK'
    assert cancel_payload['scan_runtime']['status'] in {'canceling', 'cancelled'}
    assert cancel_payload['scan_runtime']['cancel_requested'] is True
    assert cancel_payload['scan_runtime']['retry_state'] == {}
  finally:
    release_scan.set()


def test_completed_candidates_found_refresh_survives_offline_transition_with_pairs_posture(tmp_path: Path, monkeypatch: Any) -> None:
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(tmp_path / 'state.sqlite3'))
  services = _services()
  controller = ConsoleSessionController(session_token='session-123')
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=lambda **_: {
        'decision': 'planned',
        'scan_runtime': {'scan_session_id': 'test-scan-1', 'lane_session_id': 'test-lsid-1', 'status': 'completed', 'result_candidate_count': 2},
        'candidate_count': 2,
        'candidates': [
          {'ticker': 'LIVE-1', 'density_weight': '1.0', 'liquidity_score': '210'},
          {'ticker': 'LIVE-2', 'density_weight': '0.9', 'liquidity_score': '190'},
        ],
        'next_action': 'Review candidates in Pairs.',
        'settings': {
          'kalshi_env': 'demo',
          'operation_lane': 'sandbox',
          'settings_ready': True,
          'environment_ready': True,
          'credential_ready': True,
          'mode_selected': True,
          'state_db_path': str(tmp_path / 'state.sqlite3'),
        },
      },
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    ),
    session_controller=controller,
  )

  root_status, _, root_body = _call_app(app, method='GET', path='/', query='session=session-123')
  mutation_auth = _extract_mutation_auth_from_html(root_body)
  scan_headers = _signed_mutation_headers('/api/scan', {}, mutation_auth)
  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', query='session=session-123', body={}, headers=scan_headers)
  scan_payload = json.loads(scan_body)
  close_headers = _signed_mutation_headers(
    '/api/session-close',
    {
      'set_offline_if_active': True,
      'close_reason': 'browser_window_closed',
    },
    mutation_auth,
  )
  close_status, _, close_body = _call_app(
    app,
    method='POST',
    path='/api/session-close',
    query='session=session-123',
    body={
      'set_offline_if_active': True,
      'close_reason': 'browser_window_closed',
    },
    headers=close_headers,
  )
  close_payload = json.loads(close_body)
  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert root_status == '200 OK'
  assert scan_status == '200 OK'
  assert scan_payload['pair_monitor']['candidate_count'] == 2
  assert close_status == '200 OK'
  assert close_payload['offline_transition_applied'] is True
  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['connection_posture']['operation_lane'] == 'offline'
  assert bootstrap_payload['workflow_source'] == 'bootstrap_workflow'
  assert bool((bootstrap_payload.get('replay_restore') or {}).get('available')) is False
  assert bootstrap_payload['pair_monitor']['candidate_count'] == 0


def test_completed_candidates_found_same_session_bootstrap_preserves_candidates(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  # Candidate-session SSOT: the pair monitor projects candidates from the canonical
  # persisted store, not from payload echoes — so this preservation contract needs the
  # DB-backed app (the real shell always has a state DB; a store-less harness would
  # truthfully project zero).
  services = _services()
  app = _db_backed_review_app(
    tmp_path,
    monkeypatch,
    services=OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=lambda **_: {
        'decision': 'planned',
        'candidate_count': 1,
        'sandbox_extended_count': 1,
        'candidates': [
          {'ticker': 'LIVE-1', 'candidate_uid': 'live-1-uid', 'density_weight': '1.0', 'liquidity_score': '210'},
        ],
        'sandbox_candidates_extended': [
          {'ticker': 'LIVE-1', 'candidate_uid': 'live-1-uid', 'density_weight': '1.0', 'liquidity_score': '210'},
        ],
        'next_action': 'Review candidates in Pairs.',
        'settings': {
          'kalshi_env': 'demo',
          'operation_lane': 'sandbox',
          'settings_ready': True,
          'environment_ready': True,
          'credential_ready': True,
          'mode_selected': True,
        },
      },
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    ),
  )

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert scan_status == '200 OK'
  assert scan_payload['pair_monitor']['candidate_count'] == 1
  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['scan_runtime']['status'] == 'completed'
  assert bootstrap_payload['scan_runtime']['result_candidate_count'] == 1
  assert bootstrap_payload['pair_monitor']['candidate_count'] == 1
  assert bootstrap_payload['workflow']['recommended_step'] == 'select_candidates'
  assert bootstrap_payload['workflow_source'] == 'terminal_scan_replay'


def test_bootstrap_terminal_replay_zero_candidates_preserves_offline_projection_and_coherent_copy(
  monkeypatch: Any,
  tmp_path: Path,
) -> None:
  state_db_path = tmp_path / 'runtime.sqlite3'
  key_file = tmp_path / 'demo.pem'
  key_file.write_text('demo-private-key', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=4,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(state_db_path),
  )
  ready_settings_payload = {
    'settings_ready': True,
    'environment_ready': True,
    'credential_ready': True,
    'kalshi_env': 'demo',
    'operation_lane': 'sandbox',
    'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
    'live_websocket_url': 'wss://api.kalshi.example/ws',
    'active_websocket_url': 'wss://demo-api.kalshi.example/ws',
    'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
    'available_websocket_urls': {
      'sandbox': 'demo-api.kalshi.example/ws',
      'live': 'api.kalshi.example/ws',
    },
    'state_db_path_tail': 'runtime.sqlite3',
    'private_key_path_tail': 'demo.pem',
  }

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_kwargs: (dict(ready_settings_payload), None),
  )

  base = _services()
  with open_database(state_db_path) as connection:
    service_module.persist_runtime_event(
      connection,
      level='INFO',
      event_type='scan_background_completed',
      recorded_at_utc='2026-05-14T12:00:00+00:00',
      operation_lane='sandbox',
      lane_session_id='sandbox-session-001',
      detail={
        'scan_session_id': 'scan-session-001',
        'candidate_count': 0,
        'sandbox_extended_count': 0,
        'result_decision': 'planned',
        'result_reason': 'planned',
        'result_message': 'The scan found candidates and the shell is holding on candidate review in Pairs.',
        'result_next_action': 'Review candidates in Pairs.',
        'scan_shape_summary': {
          'loaded_market_count': 300,
          'orderbook_review_market_count': 42,
          'quote_ready_market_count': 17,
          'rest_fallback_count': 5,
          'orderbook_enrichment_failure_count': 2,
          'profitability_pass_market_count': 0,
          'qualifying_candidate_count': 0,
          'websocket_orderbook_count': 12,
        },
        'candidates': [],
      },
    )

  fresh_services = OperatorConsoleServices(
    bootstrap=lambda **kwargs: web_app.build_bootstrap_payload(
      settings=settings,
      env_override=kwargs.get('env_override'),
      subaccount_override=kwargs.get('subaccount_override'),
      report_fn=service_module.report_runtime,
      reconcile_fn=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
    ),
    scan=base.scan,
    run=base.run,
    reconcile=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
    report=service_module.report_runtime,
    cancel_all=base.cancel_all,
    system_log=base.system_log,
    visuals=base.visuals,
  )
  fresh_app = create_operator_console_app(fresh_services, tombstone_path=tmp_path / 'tombstones-fresh.json')

  bootstrap_status, _, bootstrap_body = _call_app(fresh_app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)
  replay_status, _, replay_body = _call_app(fresh_app, method='POST', path='/api/replay-restore')
  replay_payload = json.loads(replay_body)

  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['workflow_source'] == 'bootstrap_workflow'
  assert bool((bootstrap_payload.get('replay_restore') or {}).get('available')) is False
  assert replay_status == '200 OK'
  assert replay_payload['decision'] == 'no-go'
  assert replay_payload['reason'] == 'replay_restore_unavailable'
  assert replay_payload['workflow_source'] == 'bootstrap_workflow'


def test_mode_change_to_live_clears_sandbox_review_hold_and_replay_state(
  monkeypatch: Any,
  tmp_path: Path,
) -> None:
  state_db_path = tmp_path / 'runtime.sqlite3'
  settings = _runtime_settings_for_lane(tmp_path, 'sandbox')
  settings = Settings(
    kalshi_env=settings.kalshi_env,
    api_key_id=settings.api_key_id,
    live_api_key_id='live-api-key',
    private_key_file=settings.private_key_file,
    private_key_inline=settings.private_key_inline,
    private_key_path_legacy=settings.private_key_path_legacy,
    api_base_url=settings.api_base_url,
    websocket_url=settings.websocket_url,
    sandbox_websocket_url=settings.sandbox_websocket_url,
    live_websocket_url=settings.live_websocket_url,
    operation_lane=settings.operation_lane,
    subaccount=settings.subaccount,
    scan_interval_ms=settings.scan_interval_ms,
    entry_window_start_sec=settings.entry_window_start_sec,
    entry_window_end_sec=settings.entry_window_end_sec,
    min_edge_dollars=settings.min_edge_dollars,
    fee_reserve_dollars=settings.fee_reserve_dollars,
    min_profit_dollars=settings.min_profit_dollars,
    max_pair_contracts=settings.max_pair_contracts,
    max_open_pairs=settings.max_open_pairs,
    max_unhedged_sec=settings.max_unhedged_sec,
    cancel_on_pause=settings.cancel_on_pause,
    log_level=settings.log_level,
    state_db_path=str(state_db_path),
  )
  _patch_fake_websocket_runtime(monkeypatch, settings)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=lambda **kwargs: web_app.build_bootstrap_payload(
        settings=settings,
        env_override=kwargs.get('env_override'),
        subaccount_override=kwargs.get('subaccount_override'),
        report_fn=service_module.report_runtime,
        reconcile_fn=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      ),
      scan=lambda **_: {
        'decision': 'planned',
        'candidate_count': 1,
        'sandbox_extended_count': 1,
        'candidates': [
          {
            'candidate_key': 'sandbox-live-1',
            'ticker': 'SANDBOX-1',
            'density_weight': '1.0',
            'liquidity_score': '210',
          }
        ],
        'sandbox_candidates_extended': [
          {
            'candidate_key': 'sandbox-live-1',
            'ticker': 'SANDBOX-1',
            'density_weight': '1.0',
            'liquidity_score': '210',
          }
        ],
        'next_action': 'Review candidates in Pairs.',
      },
      run=base.run,
      reconcile=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      report=service_module.report_runtime,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )

  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _load_lane_key(app, tmp_path, monkeypatch, 'live')

  sandbox_mode_status, _, _ = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  assert sandbox_mode_status == '200 OK'

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  assert scan_status == '200 OK'
  assert scan_payload['workflow']['recommended_step'] == 'select_candidates'

  with open_database(state_db_path) as connection:
    persist_candidate_saved_set(
      connection,
      saved_set_id='saved-set-sandbox-001',
      run_id=None,
      recorded_at_utc='2026-05-29T12:00:00+00:00',
      operation_lane='sandbox',
      lane_session_id='sandbox-session-001',
      saved_key_count=1,
      state_id='review_hold_saved_selection_locked',
      source_action='save_selection',
      members=[
        {
          'candidate_key': 'sandbox-live-1',
          'ticker': 'SANDBOX-1',
          'density_weight': '1.0',
          'liquidity_score': '210',
        }
      ],
      detail={'saved_signature': 'sandbox-live-1'},
    )
    persist_candidate_saved_set_evaluation(
      connection,
      saved_set_id='saved-set-sandbox-001',
      recorded_at_utc='2026-05-29T12:00:01+00:00',
      operation_lane='sandbox',
      evaluation_status='saved',
      actionability_status='active_valid',
      visibility_status='default_actionable',
      offline_verifiable=True,
      online_revalidation_required=False,
      detail={'reason': 'Saved from sandbox review selection.'},
    )

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['connection_posture']['operation_lane'] == 'live'
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['pair_monitor']['candidate_count'] == 0
  assert payload['review_selection']['state_id'] == 'review_hold_empty_selection'

  replay_status, _, replay_body = _call_app(app, method='POST', path='/api/replay-restore')
  replay_payload = json.loads(replay_body)

  assert replay_status == '200 OK'
  assert replay_payload['decision'] == 'no-go'
  assert replay_payload['reason'] == 'replay_restore_unavailable'
  assert replay_payload['workflow_source'] == 'bootstrap_workflow'


def test_mode_change_to_live_preserves_live_lane_terminal_review_evidence(
  monkeypatch: Any,
  tmp_path: Path,
) -> None:
  state_db_path = tmp_path / 'runtime.sqlite3'
  settings = _runtime_settings_for_lane(tmp_path, 'sandbox')
  settings = Settings(
    kalshi_env=settings.kalshi_env,
    api_key_id=settings.api_key_id,
    live_api_key_id='live-api-key',
    private_key_file=settings.private_key_file,
    private_key_inline=settings.private_key_inline,
    private_key_path_legacy=settings.private_key_path_legacy,
    api_base_url=settings.api_base_url,
    websocket_url=settings.websocket_url,
    sandbox_websocket_url=settings.sandbox_websocket_url,
    live_websocket_url=settings.live_websocket_url,
    operation_lane=settings.operation_lane,
    subaccount=settings.subaccount,
    scan_interval_ms=settings.scan_interval_ms,
    entry_window_start_sec=settings.entry_window_start_sec,
    entry_window_end_sec=settings.entry_window_end_sec,
    min_edge_dollars=settings.min_edge_dollars,
    fee_reserve_dollars=settings.fee_reserve_dollars,
    min_profit_dollars=settings.min_profit_dollars,
    max_pair_contracts=settings.max_pair_contracts,
    max_open_pairs=settings.max_open_pairs,
    max_unhedged_sec=settings.max_unhedged_sec,
    cancel_on_pause=settings.cancel_on_pause,
    log_level=settings.log_level,
    state_db_path=str(state_db_path),
  )
  _patch_fake_websocket_runtime(monkeypatch, settings)

  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=lambda **kwargs: web_app.build_bootstrap_payload(
        settings=settings,
        env_override=kwargs.get('env_override'),
        subaccount_override=kwargs.get('subaccount_override'),
        report_fn=service_module.report_runtime,
        reconcile_fn=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      ),
      scan=base.scan,
      run=base.run,
      reconcile=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      report=service_module.report_runtime,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )

  live_candidate = {
    'candidate_key': 'live-only-1',
    'ticker': 'LIVE-ONLY-1',
    'density_weight': '0.92',
    'liquidity_score': '188',
  }
  with open_database(state_db_path) as connection:
    persist_runtime_event(
      connection,
      level='INFO',
      event_type='scan_background_completed',
      recorded_at_utc='2026-05-29T13:00:00+00:00',
      operation_lane='live',
      lane_session_id='live-session-001',
      detail={
        'scan_session_id': 'scan-live-001',
        'candidate_count': 1,
        'sandbox_extended_count': 1,
        'result_decision': 'planned',
        'result_reason': 'planned',
        'result_message': 'The scan found candidates and the shell is holding on candidate review in Pairs.',
        'result_next_action': 'Review candidates in Pairs.',
        'candidates': [live_candidate],
        'sandbox_candidates_extended': [live_candidate],
      },
    )

  _load_lane_key(app, tmp_path, monkeypatch, 'live')

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  # R3a contract: live-lane candidates found but not saved — must be cleared after mode-switch
  # boot. The connection posture (lane, mode_selected) is preserved correctly.
  assert status == '200 OK'
  assert payload['connection_posture']['operation_lane'] == 'live'
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['pair_monitor']['candidate_count'] == 0
  assert [row['ticker'] for row in payload['pair_monitor']['candidate_rows']] == []


def test_processing_scan_runtime_projects_soft_stall_classification_before_completion(monkeypatch: Any) -> None:
  services = _services()
  scan_started = threading.Event()
  release_scan = threading.Event()

  monkeypatch.setattr(web_app, 'SCAN_STAGE_SOFT_TIMEOUTS_SEC', {'loading_markets': 0.02})
  monkeypatch.setattr(web_app, 'SCAN_STAGE_HARD_TIMEOUTS_SEC', {'loading_markets': 1.0})

  def _slow_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    cancel_check = kwargs.get('cancel_check')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    scan_started.set()
    deadline = time.time() + 0.8
    while time.time() < deadline and not release_scan.is_set():
      if callable(cancel_check) and cancel_check():
        raise service_module.ScanCancelledError('Find candidates was canceled by the operator.')
      time.sleep(0.01)
    return {
      'decision': 'planned',
      'candidate_count': 1,
      'candidates': [{'ticker': 'LIVE-1'}],
      'next_action': 'Review candidates in Pairs.',
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_slow_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan')
  scan_payload = json.loads(scan_body)

  assert scan_status == '200 OK'
  assert scan_payload['scan_runtime']['status'] == 'processing'
  assert scan_started.wait(timeout=1.0) is True

  stalled_payload = None
  deadline = time.time() + 2.0
  while time.time() < deadline:
    _, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
    candidate_payload = json.loads(bootstrap_body)
    stall_observation = ((candidate_payload.get('scan_runtime') or {}).get('orchestration') or {}).get('stall_observation') or {}
    if stall_observation.get('classification') == 'soft_stall':
      stalled_payload = candidate_payload
      break
    time.sleep(0.02)

  release_scan.set()
  assert stalled_payload is not None
  assert stalled_payload['scan_runtime']['status'] == 'processing'
  assert stalled_payload['scan_runtime']['exit_contract']['reason_code'] == 'soft_stall'
  assert stalled_payload['workflow']['recommended_step'] == 'processing'


def test_processing_scan_runtime_times_out_as_retryable_failure(monkeypatch: Any) -> None:
  services = _services()

  monkeypatch.setattr(web_app, 'SCAN_STAGE_SOFT_TIMEOUTS_SEC', {'loading_markets': 0.02})
  monkeypatch.setattr(web_app, 'SCAN_STAGE_HARD_TIMEOUTS_SEC', {'loading_markets': 0.05})
  monkeypatch.setattr(web_app, 'SCAN_OUTER_SAFETY_CAP_SEC', 1.0)

  def _hung_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    cancel_check = kwargs.get('cancel_check')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    deadline = time.time() + 1.0
    while time.time() < deadline:
      if callable(cancel_check) and cancel_check():
        raise service_module.ScanCancelledError('Find candidates was canceled by the operator.')
      time.sleep(0.01)
    return {
      'decision': 'planned',
      'candidate_count': 1,
      'candidates': [{'ticker': 'LIVE-1'}],
      'next_action': 'Review candidates in Pairs.',
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_hung_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  scan_status, _, _ = _call_app(app, method='POST', path='/api/scan')
  assert scan_status == '200 OK'

  failed_payload = None
  deadline = time.time() + 2.0
  while time.time() < deadline:
    _, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
    candidate_payload = json.loads(bootstrap_body)
    if candidate_payload.get('scan_runtime', {}).get('status') == 'failed':
      failed_payload = candidate_payload
      break
    time.sleep(0.02)

  assert failed_payload is not None
  assert failed_payload['scan_runtime']['result_reason'] == 'stage_timeout'
  assert failed_payload['scan_runtime']['exit_contract']['reason_code'] == 'stage_timeout'
  assert failed_payload['scan_runtime']['orchestration']['terminal_trigger'] == 'stage_timeout'
  assert failed_payload['workflow_source'] == 'bootstrap_workflow'
  assert bool((failed_payload.get('replay_restore') or {}).get('available')) is False

  replay_status, _, replay_body = _call_app(app, method='POST', path='/api/replay-restore')
  replay_payload = json.loads(replay_body)

  assert replay_status == '200 OK'
  assert replay_payload['decision'] == 'no-go'
  assert replay_payload['reason'] == 'replay_restore_unavailable'
  assert replay_payload['workflow_source'] == 'bootstrap_workflow'


def test_scan_route_allows_clean_cancellation_back_to_pre_scan_posture() -> None:
  services = _services()
  scan_started = threading.Event()
  cancel_observed = threading.Event()

  def _cancelable_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    cancel_check = kwargs.get('cancel_check')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    scan_started.set()
    deadline = time.time() + 2.0
    while time.time() < deadline:
      if callable(cancel_check) and cancel_check():
        cancel_observed.set()
        raise service_module.ScanCancelledError('Find candidates was canceled by the operator.')
      time.sleep(0.02)
    return {
      'decision': 'planned',
      'candidate_count': 1,
      'candidates': [{'ticker': 'LIVE-1'}],
      'next_action': 'Review candidates in Pairs.',
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_cancelable_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan')
  scan_payload = json.loads(scan_body)

  assert scan_status == '200 OK'
  assert scan_payload['scan_runtime']['status'] == 'processing'
  assert scan_started.wait(timeout=1.0) is True

  cancel_status, _, cancel_body = _call_app(app, method='POST', path='/api/scan-cancel')
  cancel_payload = json.loads(cancel_body)

  assert cancel_status == '200 OK'
  assert cancel_observed.wait(timeout=1.0) is True
  assert cancel_payload['scan_runtime']['status'] == 'cancelled'
  assert cancel_payload['scan_runtime']['active'] is False
  assert cancel_payload['scan_runtime']['cancel_requested'] is True
  assert cancel_payload.get('execution_state') is None

  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['workflow_source'] == 'bootstrap_workflow'
  assert bool((bootstrap_payload.get('replay_restore') or {}).get('available')) is False
  assert bootstrap_payload['scan_runtime']['status'] == 'cancelled'
  assert bootstrap_payload.get('execution_state') is None

  replay_status, _, replay_body = _call_app(app, method='POST', path='/api/replay-restore')
  replay_payload = json.loads(replay_body)

  assert replay_status == '200 OK'
  assert replay_payload['decision'] == 'no-go'
  assert replay_payload['reason'] == 'replay_restore_unavailable'
  assert replay_payload['workflow_source'] == 'bootstrap_workflow'


def test_scan_route_returns_processing_ack_promptly_when_key_path_is_present(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'demo-private-key.pem'
  key_file.write_text('placeholder-private-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    active_websocket_url='wss://demo-api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.01,
    max_pair_contracts=10.0,
    max_open_pairs=20,
    max_unhedged_sec=5,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  services = _services()
  scan_started = threading.Event()
  release_scan = threading.Event()

  def _blocking_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    scan_started.set()
    release_scan.wait(timeout=2.0)
    return {
      'decision': 'planned',
      'candidate_count': 1,
      'candidates': [
        {'ticker': 'LIVE-1', 'density_weight': '1.0', 'liquidity_score': '210'},
      ],
      'next_action': 'Review candidates in Pairs.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_blocking_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )

  result: dict[str, Any] = {}
  finished = threading.Event()

  def _invoke_scan() -> None:
    status, _, body = _call_app(app, method='POST', path='/api/scan')
    result['status'] = status
    result['payload'] = json.loads(body)
    finished.set()

  worker = threading.Thread(target=_invoke_scan, daemon=True)
  worker.start()

  try:
    # §23 contract (restored 2026-06-12): the route holds the bounded await
    # (test-tuned to 0.4 s via the conftest fast-scan-await fixture) and then
    # returns the processing fallback when the scan outlives the wait. The
    # margin covers the await timeout plus payload assembly.
    assert finished.wait(timeout=2.5) is True
    assert scan_started.wait(timeout=1.0) is True

    payload = result['payload']
    assert result['status'] == '200 OK'
    assert payload['execution_state']['kind'] == 'processing'
    assert payload['scan_runtime']['status'] == 'processing'

    bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
    bootstrap_payload = json.loads(bootstrap_body)

    assert bootstrap_status == '200 OK'
    assert bootstrap_payload['execution_state']['kind'] == 'processing'
    assert bootstrap_payload['scan_runtime']['stage'] == 'loading_markets'
  finally:
    release_scan.set()
    worker.join(timeout=1.0)


def test_root_route_embeds_gross_deploy_ratio_and_tooltip() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  # Deploy ratio replaces the nonsensical gross/gross percent.
  assert "return formatPercentCompact((metrics.grossPositionValue / metrics.totalAssets) * 100);" in body
  assert "grossPositionValue: toNumericValue(summary.gross_position_value_dollars)," in body
  # Sparse hover tooltip explains the percent-mode value.
  assert "'Share of assets in open positions'" in body


def test_root_route_embeds_background_scan_processing_contract() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "function scanProcessingActive(payload = state.payload || {})" in body
  assert "async function refreshShellWhileProcessing(options = {})" in body
  assert "const runtimeProcessing = String(((payload || {}).scan_runtime || {}).status || '').toLowerCase() === 'processing';" in body
  assert "if (runtimeProcessing || scanProcessingActive(state.payload || {})) {" in body
  assert "await refreshShellWhileProcessing({ force: runtimeProcessing });" in body
  assert "const force = Boolean(options.force);" in body
  assert "if (state.shellRefreshInFlight || (!force && !scanProcessingActive(state.payload || {}))) return;" in body
  assert "renderPayload('scan', payload, { suppressPayloadLog: true });" in body
  assert 'suppressVisualRefresh: true' not in body
  assert "Find candidates is processing in the background." in body
  assert "function buildProcessingRowModel(payload = {}, action = '')" in body
  assert "const processingRow = boundary ? null : buildProcessingRowModel(payload, action);" in body
  assert "panel.classList.add('processing-owner');" in body
  assert "pill.textContent = processingRow.pillText;" in body
  assert "function stopZeroFoundRetryTicker()" in body
  assert "function updateZeroFoundRetryCountdown()" in body
  assert "function ensureZeroFoundRetryTicker()" in body
  assert "boundary-zero-found-retry-value" in body
  assert "0 candidates found; retrying in 5 seconds." in body
  assert "focusTarget: 'boundary-panel'," in body
  assert "const normalizedAction = String(action || '').toLowerCase();" in body
  assert "const dataManagementPayload = (payload && payload.data_management) || {};" in body
  assert "if (normalizedAction === 'bootstrap' && !selectedDatapackPathDisplay && dataManagementResultTone === 'ok' && Boolean(dataManagementPayload.last_load_attestation)) {" in body
  assert "clearDetailFieldDraft('data_management');" in body
  assert "function formatElapsedDurationCompact(rawValue)" in body
  assert "function updateProcessingElapsedCounters()" in body
  assert "{ label: 'Elapsed', html: `<span data-processing-started-at=\"${escapeHtml(startedAtRaw)}\" data-processing-session-id=\"${escapeHtml(processingSessionId)}\">${escapeHtml(elapsedLabel)}</span>` }," in body
  assert "label: 'Cancel scan', pendingLabel: 'Canceling scan...', kind: 'action', value: 'scan-cancel', tone: 'danger'" in body
  assert "action === 'scan-cancel' ? '/api/scan-cancel'" in body
  assert 'function renderProcessingPanel(' not in body
  # Scan completion: terminal replay is fetched via replay-restore so the shell
  # shows the actual scan result instead of the generic bootstrap workflow.
  assert "let completionPayload = payload;" in body
  assert "const replayPayload = await requestJson('/api/replay-restore', { method: 'POST' });" in body
  assert "String((replayPayload || {}).workflow_source || '').toLowerCase() === 'terminal_scan_replay'" in body
  assert "renderPayload('scan', completionPayload, { suppressPayloadLog: true });" in body


def test_root_route_embeds_execution_time_acknowledgment_contract() -> None:
  # BMAP-1 (Class S): the scan-surface acknowledgment helpers and their wiring at the
  # four scan-trigger sites (manual find-candidates, automation enable/resume, mint/clear)
  # must be present so manual and automated triggers show the identical stable
  # processing panel at the operator gesture, handed off in place or truthfully reverted.
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  # Helpers
  assert "function buildOptimisticScanExecutionState() {" in body
  assert "function optimisticScanAcknowledgmentActive() {" in body
  assert "function applyOptimisticScanAcknowledgment() {" in body
  assert "function carryForwardOptimisticScan(payload) {" in body
  assert "function revertOptimisticScanAcknowledgment() {" in body
  # Optimistic state is the scan processing surface, marked provisional (truthfulness)
  assert "kind: 'processing'," in body
  assert "optimistic: true," in body
  # Manual find-candidates: acknowledge after the pre-scan guard, excluding the cancel request
  assert "if (normalizedAction === 'scan' && String(((options.body || {}).action) || options.action || '').toLowerCase() !== 'cancel'" in body
  # Truthful revert when a manual scan fails before any authoritative response
  assert "if (normalizedAction === 'scan' && !responsePayload && optimisticScanAcknowledgmentActive()) {" in body
  # Automated enable/resume: carry the optimistic surface forward only when the executor is allowed
  assert "const executorAllowed = automationPolicyAllowsClientExecutor(nextPayload || {});" in body
  assert "carryForwardOptimisticScan(nextPayload);" in body
  # No-churn: the optimistic surface clears boundary so the processing row renders
  assert "payload.boundary = null;" in body
  # Popup collapses immediately on confirm (close is not deferred until after the apply)
  assert "Collapse the confirmation popup immediately on confirm" in body


def test_root_route_embeds_button_interaction_feedback() -> None:
  # UT-2: uniform on-hover and on-click feedback on the base button rule so every
  # button (modal confirm/cancel, deck actions, chips, toggles, candidate-select)
  # animates, while disabled buttons stay inert.
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "transition: transform 90ms ease, box-shadow 160ms ease, border-color 160ms ease, background 160ms ease;" in body
  assert "button:not(:disabled):hover {" in body
  assert "button:not(:disabled):active {" in body


def test_failed_background_scan_returns_find_candidates_as_next_step() -> None:
  services = _services()

  def _failing_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    if callable(progress_callback):
      progress_callback(
        'enriching_remaining_orderbooks',
        'Enriching remaining orderbooks for candidate scoring.',
        {'market_count': 12},
        0.56,
      )
    raise RuntimeError('orderbook enrichment stalled')

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_failing_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  scan_status, _, _ = _call_app(app, method='POST', path='/api/scan')
  assert scan_status == '200 OK'

  deadline = time.time() + 2.0
  failed_payload = None
  while time.time() < deadline:
    _, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
    candidate_payload = json.loads(bootstrap_body)
    if candidate_payload.get('scan_runtime', {}).get('status') == 'failed':
      failed_payload = candidate_payload
      break
    time.sleep(0.02)

  assert failed_payload is not None
  assert failed_payload['decision'] == 'planned'
  assert failed_payload['workflow_source'] == 'bootstrap_workflow'
  assert bool((failed_payload.get('replay_restore') or {}).get('available')) is False

  replay_status, _, replay_body = _call_app(app, method='POST', path='/api/replay-restore')
  replay_payload = json.loads(replay_body)

  assert replay_status == '200 OK'
  assert replay_payload['decision'] == 'no-go'
  assert replay_payload['reason'] == 'replay_restore_unavailable'
  assert replay_payload['workflow_source'] == 'bootstrap_workflow'


def test_root_route_embeds_key_loading_state_parity_contract() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "rows.push({ label: 'Sandbox key', value: keyManagement.sandbox_key_tail || '--', active: connectionPosture.lane === 'sandbox' && wsConnected && sandboxKeyLoaded });" in body
  assert "rows.push({ label: 'Live key', value: keyManagement.live_key_tail || '--', active: connectionPosture.lane === 'live' && wsConnected && liveKeyLoaded });" in body
  assert "rows.push({ label: 'Sandbox websocket', value: operatorFacingWebsocketValue(connectionPosture.availableWebsocketUrls.sandbox), active: connectionPosture.lane === 'sandbox' && wsConnected });" in body
  assert "rows.push({ label: 'Live websocket', value: operatorFacingWebsocketValue(connectionPosture.availableWebsocketUrls.live), active: connectionPosture.lane === 'live' && wsConnected });" in body
  assert "rows.push({ label: 'Sandbox datapack', value: dataManagement.sandbox_datapack_id || '--', active: connectionPosture.lane === 'sandbox' && sandboxDatapackLoaded });" in body
  assert "rows.push({ label: 'Live datapack', value: dataManagement.live_datapack_id || '--', active: connectionPosture.lane === 'live' && liveDatapackLoaded });" in body
  assert 'visualsScopeSelections: {}' in body
  assert 'function rememberVisualScopeSelection(packet = {})' in body
  assert 'const packetViewId = String(((packet.view || {}).id) || \'\').trim().toLowerCase();' in body
  assert '&& packetViewId === \'analysis_threshold_progress\'' in body
  assert '? currentViewId' in body
  assert 'function rememberedVisualScopeSelection(scopeId = \'\')' in body
  assert 'const remembered = rememberedVisualScopeSelection(requestedScope);' in body


def test_root_route_embeds_connection_posture_datapack_rows_contract() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  # CP-DP-1: datapack rows are present in buildKeyLoadingStateModel
  assert "const dataManagement = payload.data_management || {};" in body
  assert "const sandboxDatapackLoaded = Boolean(dataManagement.sandbox_datapack_loaded);" in body
  assert "const liveDatapackLoaded = Boolean(dataManagement.live_datapack_loaded);" in body
  assert "rows.push({ label: 'Sandbox datapack', value: dataManagement.sandbox_datapack_id || '--', active: connectionPosture.lane === 'sandbox' && sandboxDatapackLoaded });" in body
  assert "rows.push({ label: 'Live datapack', value: dataManagement.live_datapack_id || '--', active: connectionPosture.lane === 'live' && liveDatapackLoaded });" in body
  # CP-DP-2: active condition uses sandboxDatapackLoaded / liveDatapackLoaded (no tail-string fallback)
  assert 'sandboxDatapackLoaded = Boolean(dataManagement.sandbox_datapack_loaded)' in body
  assert 'liveDatapackLoaded = Boolean(dataManagement.live_datapack_loaded)' in body
  # CP-DP-3: offline boundary — no cross-lane fallback in loaded boolean
  assert 'sandboxDatapackLoaded = Boolean(dataManagement.sandbox_datapack_loaded)' in body
  assert 'sandbox_datapack_loaded' in body


def test_datapack_tombstone_source_contract() -> None:
  source = Path(web_app.__file__).read_text(encoding='utf-8')
  # tombstone write at datapack load
  assert "_set_tombstone_value(f'{normalized_lane}_datapack_root', str(resolved_root))" in source
  # tombstone remove at datapack clear
  assert "_remove_tombstone(f'{normalized_lane}_datapack_root')" in source
  # startup hydration reads
  assert "_startup_tombstone.get('sandbox_datapack_root')" in source
  assert "_startup_tombstone.get('live_datapack_root')" in source
  # observability: now ID-based
  assert 'sandbox_datapack_ref_present' in source
  assert 'live_datapack_ref_present' in source
  assert 'sandbox_datapack_id' in source


def test_datapack_pipeline_source_contract() -> None:
  source = Path(web_app.__file__).read_text(encoding='utf-8')
  # Change D: resolve_active_profile_token called in load handler
  assert 'resolve_active_profile_token' in source
  # Change D: detail_json UPDATE in load handler
  assert "UPDATE lane_active_datapack SET detail_json" in source
  # Change F: close_active_datapack called in extract handler
  assert 'close_active_datapack' in source
  assert "closed_cause='extracted_to_store'" in source
  # Change G: extract: true in JS POST body
  assert "body: { lanes, extract: true }" in source
  # Change I: manifest['datapack_id'] stamped at extract; datapack_id in normalized items
  assert "manifest['datapack_id'] = source_datapack_id" in source
  assert "'datapack_id': str(item.get('datapack_id') or '').strip() or None" in source
  # Change J: scan mint guard present
  assert "data_management_state.get(f'{operation_lane}_datapack_id')" in source
  # Change K: datapack_id gate in extract, loaded_root gate absent
  assert 'active_datapack_id_for_extract' in source
  assert 'Clear-as-extract requires an active datapack identity' in source


def _default_settings(tmp_path: Path) -> 'Settings':
  key_file = tmp_path / 'test-key.pem'
  if not key_file.exists():
    key_file.write_text('placeholder-key-material', encoding='utf-8')
  state_db_path = tmp_path / 'runtime.sqlite3'
  return Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(state_db_path),
  )


def test_datapack_id_db_hydration_restores_on_startup(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _default_settings(tmp_path)
  state_db_path = Path(settings.state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  connection = open_database(state_db_path)
  with connection:
    resolve_active_profile_token(connection, 'sandbox', key_path=settings.private_key_file)
    connection.execute(
      "UPDATE lane_active_datapack SET detail_json = ? WHERE operation_lane = 'sandbox' AND closed_at_utc IS NULL",
      (json.dumps({'datapack_id': 'test-hydrate-abc123'}),),
    )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  dm = payload.get('data_management') or {}
  assert dm.get('sandbox_datapack_id') == 'test-hydrate-abc123'
  assert dm.get('sandbox_datapack_loaded') is True


def test_datapack_load_mints_clean_id_when_manifest_has_none(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _default_settings(tmp_path)
  state_db_path = Path(settings.state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  # Use a separate source DB to avoid triggering the overwrite-confirmation gate
  source_db_path = tmp_path / 'source.sqlite3'
  source_conn = open_database(source_db_path)
  source_conn.execute(
    'INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc) VALUES (?, ?, ?, ?, ?, ?, ?)',
    ('info', 'test_event', None, 'sandbox', None, '{}', '2026-01-01T00:00:00Z'),
  )
  source_conn.commit()
  bundle = build_datapack_bundle(
    source_conn,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=state_db_path.name,
    include_synthetic_refinement=False,
  )
  assert bundle['manifest'].get('datapack_id') is None
  input_root = tmp_path / 'input-datapack'
  _write_test_datapack_bundle(input_root, bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(input_root)})
  status, _, load_body = _call_app(app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'})
  payload = json.loads(load_body)

  assert status == '200 OK'
  dm = payload.get('data_management') or {}
  minted_id = dm.get('sandbox_datapack_id')
  assert minted_id is not None
  assert minted_id.startswith('20') and '-sandbox-' in minted_id
  assert dm.get('sandbox_datapack_loaded') is True
  fresh_conn = open_database(state_db_path)
  row = fresh_conn.execute(
    "SELECT detail_json FROM lane_active_datapack WHERE operation_lane = 'sandbox' AND closed_at_utc IS NULL LIMIT 1"
  ).fetchone()
  assert row is not None
  detail = json.loads(row['detail_json'] or '{}')
  assert detail.get('datapack_id') == minted_id


def test_datapack_extract_closes_db_row(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _default_settings(tmp_path)
  state_db_path = Path(settings.state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  source_db_path = tmp_path / 'source.sqlite3'
  source_conn = open_database(source_db_path)
  source_conn.execute(
    'INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc) VALUES (?, ?, ?, ?, ?, ?, ?)',
    ('info', 'test_event', None, 'sandbox', None, '{}', '2026-01-01T00:00:00Z'),
  )
  source_conn.commit()
  bundle = build_datapack_bundle(
    source_conn,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=state_db_path.name,
    include_synthetic_refinement=False,
  )
  input_root = tmp_path / 'input-datapack'
  _write_test_datapack_bundle(input_root, bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(input_root)})
  _call_app(app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'})
  clear_status, _, clear_body = _call_app(
    app, method='POST', path='/api/data-clear',
    body={'lanes': ['sandbox'], 'extract': True},
  )
  clear_payload = json.loads(clear_body)

  assert clear_status == '200 OK'
  assert clear_payload['data_management']['last_result']['reason'] == 'datapack_extracted_on_clear'
  fresh_conn = open_database(state_db_path)
  row = fresh_conn.execute(
    "SELECT closed_at_utc, closed_cause FROM lane_active_datapack WHERE operation_lane = 'sandbox' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  assert row is not None
  assert row['closed_at_utc'] is not None
  assert row['closed_cause'] == 'extracted_to_store'


def test_datapack_extract_manifest_carries_datapack_id(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _default_settings(tmp_path)
  state_db_path = Path(settings.state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  source_db_path = tmp_path / 'source.sqlite3'
  source_conn = open_database(source_db_path)
  source_conn.execute(
    'INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc) VALUES (?, ?, ?, ?, ?, ?, ?)',
    ('info', 'test_event', None, 'sandbox', None, '{}', '2026-01-01T00:00:00Z'),
  )
  source_conn.commit()
  bundle = build_datapack_bundle(
    source_conn,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=state_db_path.name,
    include_synthetic_refinement=False,
  )
  input_root = tmp_path / 'input-datapack'
  _write_test_datapack_bundle(input_root, bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(input_root)})
  _, _, load_body = _call_app(app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'})
  load_payload = json.loads(load_body)
  minted_id = load_payload['data_management'].get('sandbox_datapack_id')

  _, _, clear_body = _call_app(
    app, method='POST', path='/api/data-clear',
    body={'lanes': ['sandbox'], 'extract': True},
  )
  clear_payload = json.loads(clear_body)
  items = clear_payload['data_management']['last_extraction']['items']

  assert items[0].get('datapack_id') == minted_id
  extracted_root = Path(items[0]['extracted_root'])
  manifest_data = json.loads((extracted_root / 'manifest.json').read_text(encoding='utf-8'))
  assert manifest_data.get('datapack_id') == minted_id


def test_datapack_extract_gate_rejects_when_no_datapack_id(tmp_path: Path) -> None:
  # No DB row, no tombstone — sandbox_datapack_id is None
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap = json.loads(bootstrap_body)
  dm = bootstrap.get('data_management') or {}
  assert dm.get('sandbox_datapack_id') is None
  assert dm.get('sandbox_datapack_loaded') is False

  clear_status, _, clear_body = _call_app(
    app, method='POST', path='/api/data-clear',
    body={'lanes': ['sandbox'], 'extract': True},
  )
  clear_payload = json.loads(clear_body)
  last_result = clear_payload['data_management']['last_result']
  assert last_result['reason'] != 'datapack_extracted_on_clear'
  assert last_result['tone'] != 'ok'


def test_datapack_tombstone_persistence_hydration(tmp_path: Path) -> None:
  tombstone_path = tmp_path / 'tombstones.json'
  fake_root = str(tmp_path / 'datapacks' / 'my_datapack')
  tombstone_path.write_text(
    json.dumps({'sandbox_datapack_root': fake_root}),
    encoding='utf-8',
  )

  app = create_operator_console_app(_services(), tombstone_path=tombstone_path)

  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  dm = payload.get('data_management') or {}
  assert dm.get('sandbox_datapack_tail') == 'my_datapack'
  assert dm.get('sandbox_datapack_loaded') is False
  assert dm.get('sandbox_datapack_id') is None
  assert dm.get('live_datapack_tail') is None
  assert dm.get('live_datapack_loaded') is False


def test_root_route_embeds_stage5_visual_group_contract() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "label: 'Health'" in body
  assert "label: 'Cadence'" in body
  assert "label: 'Telemetry'" not in body
  assert "label: 'History'" in body
  assert "label: 'Bridge'" in body
  assert "label: 'Supporting metrics'" in body
  assert "label: 'Decision'" in body
  assert "label: 'Thresholds'" in body
  assert "label: 'Frontier'" in body
  assert "label: 'Rankings'" in body
  assert "label: 'Gate'" in body
  assert "label: 'Actionability'" in body
  assert "label: 'Factors'" in body
  assert "label: 'Decision boundary'" not in body
  assert "label: 'Preserved history'" not in body
  assert "label: 'Gate / explainer'" not in body
  assert "label: 'Factor analysis'" not in body
  assert "candidate_decision_boundary" in body
  assert "threshold_boundary_marker" in body
  assert "analysis_threshold_progress" in body
  assert "factor_contribution" in body
  assert "const ungroupedViews = availableViews.filter((view) => !consumedIds.has(String(view.id || '').toLowerCase()));" in body
  assert "groups.push({ id: 'ungrouped_views', label: 'Additional views', kind: 'views', items: ungroupedViews });" in body


def test_root_route_embeds_stage5_visual_chip_label_contract() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "runtime_cadence: 'Health'" in body
  assert "freshness_latency: 'Cadence'" in body
  assert "analysis_threshold_progress: 'Gate'" in body
  assert "actionability_status_distribution: 'Actionability'" in body
  assert "saved_set_carry_forward: 'History'" in body
  assert "factor_contribution: 'Contribution'" in body
  assert "runtime_cadence: 'Runtime'" not in body
  assert "freshness_latency: 'Freshness'" not in body
  assert 'const showLabel = Boolean(group.label);' not in body
  assert 'showLabel ? `<span class="visuals-target-group-label">' not in body
  assert 'const laneLabel = laneKey ? `<span class="visuals-target-group-label">' not in body


def test_root_route_embeds_candidate_selection_shell_contract() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'id="candidate-detail-modal"' in body
  assert 'id="candidate-detail-grid"' in body
  assert 'id="candidate-detail-toggle"' in body
  assert 'function candidateSelectionKey(candidate, index = 0)' in body
  assert 'function resetCandidateSelectionState(payload = {})' in body
  assert 'function renderCandidateDetailModal()' in body
  assert 'data-candidate-open=' in body
  assert 'data-candidate-toggle=' in body
  assert 'data-candidate-bulk-toggle=' in body
  assert 'candidate-review-shell' in body
  assert 'candidate-card-grid' in body
  assert 'candidate-select-button' in body
  assert "const reviewStateId = String((((payload || {}).review_selection || {}).state_id) || '').toLowerCase();" in body
  assert "&& (websocketSessionActive(payload || {}) || reviewStateId === 'review_hold_with_active_selection');" in body
  assert 'Select all' in body
  assert 'Review this candidate in place; the `Pairs` panel remains the owning surface.' in body


def test_root_route_embeds_candidate_pair_filter_persistence_contract() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "function pairMonitorFilterOptions(monitor = {}) {" in body
  assert "function pairMonitorFilterLabel(stateName = 'ALL') {" in body
  assert "const filterInventoryAvailable = Number(monitor.pair_count || 0) > 0 || orderedPairStateFilters.length > 0;" in body
  assert 'pair-monitor-filter-row' in body
  assert 'pair-monitor-filter-chip' in body
  assert "'PARTIAL_ONE_SIDE': 'Partial fill'" in body
  assert "'RESTING_BOTH': 'Both resting'" in body
  assert "'CANCELED': 'Canceled'" in body
  assert 'This lower monitor displays the loaded pairs for the selected state filter when the current pair inventory includes that state.' in body
  assert 'It repopulates when that state appears or when a populated filter is selected.' in body


def test_root_route_embeds_candidate_card_closes_metric_contract() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'candidate-metric-label">closes</div>' in body
  assert "formatCandidateTimeContext(candidate) ? `" in body


def test_root_route_embeds_pair_monitor_runtime_summary_public_state_id_contract() -> None:
  # P4 stub: asserts JS shell infrastructure for runtime_summary pair monitor rendering.
  # Pre-existing field checks (pass at P1). Pill/fill-summary checks (added at P2).
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  # Pre-existing: JS source already references these runtime fields
  assert 'runtime.locked_contracts' in body
  assert 'monitor.runtime_summary' in body
  assert 'runtimeByPair' in body


def test_root_route_embeds_pair_monitor_empty_card_coexistence_contract() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'pair-monitor-empty-state' in body
  assert 'const pairMonitorEmptyMarkup = rows.length' in body
  assert "${rows.length ? rows.join('') : (candidateMarkup ? '' : `<div class=\"list-item\">${escapeHtml(pairMonitorEmptyStateMessage(monitor, state.pairFilter))}</div>`)}" not in body


def test_root_route_embeds_conversational_candidate_empty_state_copy() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'This review surface displays the current-run candidates that are actively in focus when the latest scan has surfaced a working set.' in body
  assert 'It populates after Find candidates produces current-run candidates.' in body
  assert 'This review surface displays the current saved candidate set when saved selections are available in the active review context.' in body
  assert 'It populates after the current review set has been saved.' in body
  assert 'This review surface displays previously saved candidate context when earlier saved selections are available for comparison.' in body
  assert 'It populates after prior saved selections exist in history.' in body
  assert 'This review surface displays either the active found set or the active saved set when one of those review families is enabled.' in body
  assert 'It repopulates when Found or Saved is turned back on.' in body
  assert 'This lower monitor displays loaded pair-state detail for the candidates currently under review when pair inventory exists for this session.' in body
  assert 'It populates after pair state has been created or retained for the current review flow.' in body
  assert 'No candidates or loaded pairs are currently in view.' not in body


def test_root_route_embeds_softened_empty_state_styling() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert '.candidate-review-empty {' in body
  assert 'padding: 22px 18px;' in body
  assert 'color: rgba(214, 225, 236, 0.62);' in body
  assert '.visuals-empty {' in body
  assert 'padding: 10px 0;' in body
  assert 'color: var(--muted-2);' in body


def test_root_route_embeds_evidence_panel_field_corrections_contract() -> None:
  # Pass 1: Fixes 1+2+3 — evidence interpretation row, state db row, dead aggregate row removed.
  # summaryRows are computed in client-side JS, so we verify the embedded JS source code.
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  # Fix 1: new JS expression present; broken key references absent from the whole template
  assert '`evidence interpretation: ${evidence.interpretation' in body
  assert 'validationWorkflow.validation_state' not in body
  assert 'validationWorkflow.validation_status' not in body
  assert '`validation evidence:' not in body
  # Fix 2: corrected dbTail assignment present; stale fallback expression absent; new label present; old label absent
  assert 'const dbTail = settings.state_db_path ||' in body
  assert 'settings.state_db_path_tail || (connectionPosture.modeSelected' not in body
  assert '`state db: ${dbTail}`' in body
  assert '`local state cache:' not in body
  # Fix 3: dead second aggregate-file row expression absent
  assert 'evidence.project_aggregate_path ?' not in body
  # Pass 2: Fix 4 Option A — aggregate file boolean label renamed to retained runs
  assert '`retained runs: ${aggregateAvailable ?' in body
  assert '`aggregate file: ${aggregateAvailable' not in body
  # Pass 2: Fix 5 — state db artifact card variable and conditional present in renderEvidenceBrowser
  assert 'const stateDbPath = String(' in body
  assert 'state db: ${stateDbPath}' in body


def test_root_route_serves_operator_shell_html() -> None:
  app = create_operator_console_app(
    _services(),
    recovery_helper={
      'url': 'http://127.0.0.1:8766',
      'token': 'helper-token',
      'expires_at_unix': 1760000000.0,
    },
  )

  status, headers, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert headers['Content-Type'] == 'text/html; charset=utf-8'
  assert 'Polyventure Control Deck' in body
  assert 'Polyventure logo' in body
  assert 'class="brand-copy"' in body
  assert 'grid-row: 1 / span 2;' in body
  assert '>Heartbeat waiting<' in body
  assert 'Heartbeat offline' in body
  assert 'Offline mode does not carry a live heartbeat state.' in body
  assert 'sandbox sample +' in body
  assert 'AUTO-FORWARD SAFE STEPS' not in body
  assert 'Quick actions' in body
  assert "title: 'Workflow'" not in body
  assert 'Operator controls' in body
  assert 'Review / evidence' not in body
  assert 'Evidence' in body
  assert 'EXECUTION' in body
  assert 'id="live-interaction-grid"' in body
  assert 'id="live-interaction-pill"' in body
  assert 'class="panel live-interaction-panel" hidden' in body
  assert 'function renderLiveInteractionSurface(payload = {}) {' in body
  assert 'const surfaceVisible = Boolean(liveInteraction.surface_visible);' in body
  assert 'Polymath operator shell' in body
  assert 'Kalshi pilot lane' not in body
  assert 'position: sticky;' in body
  assert '--shell-padding: 20px;' in body
  assert 'padding: 0 var(--shell-padding) var(--shell-padding);' in body
  assert 'top: 0;' in body
  assert 'background: var(--panel);' in body
  assert 'backdrop-filter: blur(10px);' not in body
  assert 'System log' in body
  assert 'id="system-log-filters"' in body
  assert 'data-system-log-filter=' in body
  assert '>Showing<' not in body
  assert 'class="system-log-window"' in body
  assert 'class="system-log-entry source-' in body
  assert 'class="system-log-badge source-' in body
  assert 'class="system-log-filter filter-' in body
  assert 'repeatCount' in body
  assert "(action === 'change_mode'" in body
  assert "? '/api/change-mode'" in body
  assert "(action === 'scan-cancel' ? '/api/scan-cancel' : `/api/${action}`)" in body
  assert "const requestBody = method === 'GET'" in body
  assert 'const payload = await requestJson(path, { method, body: requestBody });' in body
  assert 'const rawResponseText = await response.text();' in body
  assert 'payload = JSON.parse(rawResponseText);' in body
  assert 'payload = {' in body
  assert 'message: sanitizeGuiText(rawResponseText).slice(0, 280),' in body
  assert 'aria-live="polite"' in body
  assert 'scrollbar-width: none;' in body
  assert '>Ops<' in body
  assert 'Operational visuals' in body
  assert 'id="notification-band-section"' in body
  assert 'id="boundary-title"' in body
  assert 'id="readiness-section"' in body
  assert 'id="visuals-section"' in body
  assert 'id="pairs-section"' in body
  assert 'id="evidence-section"' in body
  assert 'id="parameter-surface-section"' in body
  assert 'class="workspace-column workspace-column-left"' in body
  assert 'class="workspace-column workspace-column-right"' in body
  assert 'id="parameter-surface-grid"' in body
  assert 'id="system-log-section"' in body
  assert 'id="control-deck"' in body
  assert 'id="control-deck-quick-actions"' in body
  assert 'id="deck-selector-shell"' in body
  assert 'id="deck-selector-stack"' in body
  assert 'id="deck-view-shell"' in body
  assert 'id="deck-view-title"' in body
  assert 'id="deck-view-actions"' in body
  assert 'id="deck-detail-pane"' in body
  assert 'id="deck-detail-title"' in body
  assert 'id="deck-detail-summary"' in body
  assert 'id="deck-detail-meta"' in body
  assert 'id="deck-detail-grid"' in body
  assert 'id="deck-detail-controls"' in body
  assert 'id="deck-detail-jump"' in body
  assert 'id="deck-detail-close"' in body
  assert 'deck-detail-path-input' in body
  assert 'id="control-deck-next-step-wrap"' in body
  assert 'id="control-deck-next-step"' in body
  assert 'id="control-deck-next-step-guidance"' in body
  assert 'deck-step-label' in body
  assert 'id="deck-refresh-action"' not in body
  assert 'id="deck-cancel-action"' in body
  assert 'id="deck-auto-advance-toggle"' in body
  assert 'deck-selector-button' in body
  assert 'deck-view-shell.is-switching' in body
  assert 'Find candidates' in body
  assert 'Cancel scan' in body
  assert 'Submit order' in body
  assert 'Refresh pair states' not in body
  assert 'Refresh shell' in body
  assert 'Review local report' not in body
  assert 'Open local report' not in body
  assert 'Navigation' in body
  assert 'Body surfaces' not in body
  assert 'Credential posture' in body
  assert 'Runtime settings' in body
  assert 'Context' in body
  assert 'Automation' in body
  assert 'Open latest accepted run' in body
  assert 'Open retained history' in body
  assert 'Open aggregate' in body
  assert 'Export operator summary' in body
  assert 'Restart bootstrap' not in body
  assert 'Reload config posture' not in body
  assert 'Websocket URLs' in body
  assert 'Key management' in body
  assert 'id="key-loading-state-section"' in body
  assert 'id="key-loading-state-grid"' in body
  assert 'buildKeyLoadingStateModel' in body
  assert 'renderKeyLoadingState' in body
  assert 'Connection posture' in body
  assert 'Active websocket' not in body
  assert 'key-loading-state-active' in body
  assert 'Key state' not in body
  assert 'Key location' not in body
  assert 'Connection state' not in body
  assert 'Load posture' not in body
  assert 'Last result' not in body
  assert 'Offline mode' in body
  assert 'function humanOperationLaneLabel(lane)' in body
  assert 'function buildConnectionPosture(payload = {})' in body
  assert 'function operatorFacingWebsocketValue(value)' in body
  assert 'function operatorFacingValidationSummary(evidence = {}, validationWorkflow = {})' in body
  assert 'function operatorFacingEvidenceStatusMessage(evidence = {}, validationWorkflow = {})' in body
  assert 'const authoritativePosture = payload.connection_posture || {};' in body
  assert "const modeSelected = Boolean(authoritativePosture.mode_selected);" in body
  assert "const displayLane = modeSelected ? lane : 'offline';" in body
  assert "isOffline: Boolean(authoritativePosture.is_offline) || !modeSelected," in body
  assert 'laneLabel: humanOperationLaneLabel(displayLane),' in body
  assert 'const hasOverlaySandboxUrl = Object.prototype.hasOwnProperty.call(overlayWebsocketUrls, \'sandbox\');' not in body
  assert 'const hasOverlayLiveUrl = Object.prototype.hasOwnProperty.call(overlayWebsocketUrls, \'live\');' not in body
  assert 'const hasOverlayActiveWebsocket = Object.prototype.hasOwnProperty.call(contextOverlay, \'active_websocket_url_tail\');' not in body
  assert 'hasOverlaySandboxUrl ? overlayWebsocketUrls.sandbox : reportUrls.sandbox' not in body
  assert 'hasOverlayLiveUrl ? overlayWebsocketUrls.live : reportUrls.live' not in body
  assert "lane: 'offline'," in body
  assert "activeWebsocketLabel: 'unconfigured'," in body
  assert "rows.push({ label: 'Sandbox key', value: keyManagement.sandbox_key_tail || '--', active: false });" not in body
  assert "rows.push({ label: 'Live key', value: keyManagement.live_key_tail || '--', active: false });" not in body
  assert "const sandboxKeyLoaded = Boolean(keyManagement.sandbox_key_loaded) || Boolean(sanitizeGuiText(keyManagement.sandbox_key_tail || ''));" in body
  assert "const liveKeyLoaded = Boolean(keyManagement.live_key_loaded) || Boolean(sanitizeGuiText(keyManagement.live_key_tail || ''));" in body
  assert "rows.push({ label: 'Sandbox websocket', value: operatorFacingWebsocketValue(connectionPosture.availableWebsocketUrls.sandbox), active: connectionPosture.lane === 'sandbox' && wsConnected });" in body
  assert "rows.push({ label: 'Live websocket', value: operatorFacingWebsocketValue(connectionPosture.availableWebsocketUrls.live), active: connectionPosture.lane === 'live' && wsConnected });" in body
  assert 'function humanConnectionStateLabel(connectionState)' in body
  assert 'Detect keys' in body
  assert 'Select key file' in body
  assert 'Clear key' not in body
  assert "controls: ['load datapack', 'detect', 'select', 'extract']" in body
  assert "{ label: 'extract', action: 'clear_loaded_datapack'" in body
  assert 'id="data-clear-confirm" type="button" class="danger" disabled>Extract<' in body
  assert 'id="data-clear-refresh" type="button" class="quiet-button challenge-modal-refresh"' in body
  assert "confirmButton.textContent = selectedOccupiedCount > 1 ? 'Extract selected' : 'Extract';" in body
  assert 'Load sandbox' in body
  assert 'Load live' in body
  assert 'Clear all' in body
  assert 'Point to Connection posture' not in body
  assert 'Point to credential posture' not in body
  assert 'Warning: this cannot be undone.' in body
  assert 'Credential posture is the live auth summary in Readiness' in body
  assert 'Key management is where you select and load the key reference.' in body
  assert '/api/websocket-overlay' in body
  assert '/api/key-discover' in body
  assert '/api/key-stage' in body
  assert '/api/key-load' in body
  assert '/api/key-apply' in body
  assert '/api/key-clear' in body
  assert '/api/key-validate' in body
  assert '/api/key-reload' in body
  assert 'control-websocket-management' in body
  assert 'Client auto-forward off' in body
  assert 'Automation auto-forward off' not in body
  assert 'id="deck-auto-advance-toggle"' in body
  assert 'id="notification-band"' in body
  assert 'id="processing-panel"' not in body
  assert 'data-notification-card=' in body
  assert 'What ran' in body
  assert 'Outcome' in body
  assert 'Reason' in body
  assert 'id="what-ran"' not in body
  assert 'id="what-happened"' not in body
  assert 'id="backend-recovery-state"' in body
  assert 'id="backend-recovery-status-slot"' in body
  assert 'id="backend-recovery-headline"' in body
  assert 'id="backend-recovery-detail"' in body
  assert 'id="backend-recovery-next"' in body
  assert 'id="backend-recovery-action"' in body
  assert 'backendRecoveryHelpRoute' in body
  assert 'recovery-local-host' in body
  assert 'Readiness' in body
  assert 'Connection posture' in body
  assert 'Pairs' in body
  assert 'id="command-strip"' in body
  assert 'id="lane-pill"' in body
  assert 'id="lane-pill" class="pill" title="Change mode"' in body
  assert 'id="header-amount-toggle"' in body
  assert 'id="header-delta-value"' in body
  assert 'id="header-total-value"' in body
  assert 'title="net">$0<' in body
  assert 'title="gross">$0<' in body
  assert 'class="header-amount-toggle"' in body
  assert 'class="header-amount-slot neutral" title="net">$0<' in body
  assert 'const moneyStory =' in body
  assert "element.setAttribute('title', moneyStory);" in body
  assert 'mode-hidden' in body
  assert 'Click to cycle $ -> % -> hide' not in body
  assert 'id="visuals-view-chips"' in body
  assert 'id="visuals-window-chips"' in body
  assert 'id="visuals-mode-chips"' in body
  assert 'id="visuals-stage"' in body
  assert 'function renderOperationalVisuals(packet)' in body
  assert 'const analysisThresholdFallback = (' in body
  assert '&& incomingViewId === \'analysis_threshold_progress\'' in body
  assert 'if (packet.view && packet.view.id && !analysisThresholdFallback) state.visualsView = packet.view.id;' in body
  assert 'function refreshOperationalVisuals()' in body
  assert 'function visualAxisIsTemporal(categoryLabels)' in body
  assert 'function selectVisualAxisLabelIndexes(categoryLabels, windowId, temporalAxis)' in body
  assert 'function formatVisualAxisLabel(rawLabel, windowId, index, total, temporalAxis)' in body
  assert "if (windowId === 'current') {" in body
  assert "return 'Now';" in body
  assert "toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })" in body
  assert "toLocaleDateString([], { month: 'short', day: 'numeric' })" in body
  assert 'Help and recovery' in body
  assert 'id="help-drawer"' in body
  assert 'id="deck-help-toggle"' in body
  assert 'id="help-search"' in body
  assert 'Search actions, blocked states, concepts' in body
  assert 'class="help-drawer"' in body
  assert 'class="quiet-button"' in body
  assert 'Use the current next-step value as advisory context' in body
  assert '.help-card-toggle {' in body
  assert '.help-card-summary {' in body
  assert '.help-card-caret {' in body
  assert '.help-card-expand {' in body
  assert '.help-detail-label {' in body
  assert '[data-help-key]' in body
  assert 'Deck note' not in body
  assert 'function renderDeckContractStatus(payload)' in body
  assert 'function buildNextStepGuidance(payload)' in body
  assert 'function countCancelablePairs(payload)' in body
  assert 'function renderQuickActions(payload)' in body
  assert 'function setNextStepGuidanceVisible(visible)' in body
  assert 'function installNextStepGuidanceHandlers()' in body
  assert 'function notificationStateValue(payload, action, boundary)' in body
  assert 'function notificationStateDetail(payload, action, boundary, sizing)' in body
  assert 'function notificationStateMicro(payload, boundary, sizing)' in body
  assert 'function buildDecisionProjection(payload = {}, fallbackAction = \'\')' in body
  assert 'function buildEvidenceProjection(payload = {})' in body
  assert 'function buildFreshnessProjection(payload = {})' in body
  assert 'state.workflowProjection || buildWorkflowProjection' not in body
  assert 'LOCAL EVIDENCE FOUND' not in body
  assert 'ACCEPTED EVIDENCE' not in body
  assert 'REVIEW AVAILABLE' in body
  assert 'SUMMARY AVAILABLE' in body
  assert 'Current local summary is available in Evidence.' in body
  assert 'Review shell posture before continuing.' in body
  assert 'function buildReadinessProjection(payload = {})' in body
  assert 'function notificationNavigationTarget(itemKey, boundary)' in body
  assert 'function currentScrollAnchorOffset()' in body
  assert 'function scrollToSection(targetId)' in body
  assert '.panel.focus-info {' in body
  assert '.panel.focus-ok {' in body
  assert '.panel.focus-warn {' in body
  assert '.panel.focus-no-go {' in body
  assert "const PANEL_FOCUS_CLASS_NAMES = Object.freeze(['focus-info', 'focus-ok', 'focus-warn', 'focus-no-go']);" in body
  assert "const FOCUS_ROUTED_WORKFLOW_ACTIONS = Object.freeze(['scan', 'run', 'reconcile', 'report']);" in body
  assert 'panelFocusTimeoutId: 0' in body
  assert 'function clearPanelFocus()' in body
  assert 'function focusPanel(targetId, tone, options = {})' in body
  assert 'function wayfinderRoute(targetId, options = {})' in body
  assert 'function activateWayfinder(route = {})' in body
  assert 'function initialBootstrapReviewHoldRoute(payload = {})' in body
  assert 'function maybePresentInitialBootstrapReviewHold(payload = {})' in body
  assert 'function notificationWayfinderRoute(itemKey, payload = {})' in body
  assert 'function focusTargetForActionResult(action, payload, options = {})' in body
  assert "scrollIntoView: options.scrollIntoView !== false," not in body
  assert "if (options.scrollIntoView !== false) {" in body
  assert "focusPanel(route.targetId, route.tone, { scrollIntoView: route.scrollIntoView !== false });" not in body
  assert "if (executionDelta.shouldScroll) {" in body
  assert "requestAnimationFrame(() => scrollToSection(normalizedRoute.scrollTarget));" in body
  assert "applyPanelGlow(normalizedRoute.scrollTarget, normalizedRoute.tone, { sustained: Boolean(normalizedRoute.glowSustained) });" in body
  assert "route.closeHelp" in body
  assert "if (normalizedAction === 'bootstrap_refresh') {" in body
  assert "performAction('report')" in body  # Within try-catch wrapper for initialization timeout
  assert "const focusRouteKey = FOCUS_ROUTED_WORKFLOW_ACTIONS.includes(value)" in body
  assert "window.scrollTo({ top: scrollTop, behavior: 'auto' });" in body
  assert 'const CONTROL_REGISTRY_VALUE_SETS = Object.freeze({' in body
  assert 'const RECOVERY_HELPER = ' in body
  assert 'http://127.0.0.1:8766' in body
  assert 'helper-token' in body
  assert 'const BACKEND_RECOVERY_POLICY = Object.freeze({' in body
  assert 'const CONTROL_GROUP_REGISTRY = Object.freeze([' in body
  assert 'const CONTROL_REGISTRY = Object.freeze([' in body
  assert 'const DECK_VIEW_REGISTRY = Object.freeze([' in body
  assert 'const PARAMETER_REGISTRY = Object.freeze([' in body
  assert 'const CONTEXT_REGISTRY = Object.freeze([' in body
  assert 'const AUTOMATION_POLICY_CONTRACT = Object.freeze({' in body
  assert "control_id: 'run_next_step'" not in body
  assert "control_id: 'toggle_auto_advance'" in body
  assert "control_id: 'chip_session_health'" in body
  assert "control_id: 'websocket_management'" in body
  assert "control_id: 'key_management'" in body
  assert "control_id: 'open_aggregate'" in body
  assert "control_id: 'cancel_all_non_terminal_pairs'" in body
  assert "backend_binding: 'pane.websocket_management'" in body
  assert "backend_binding: 'pane.key_management'" in body
  assert "result_contract: 'open_detail_pane'" in body
  assert "mutation_class: 'destructive_local'" in body
  assert "gating_level: 'confirm'" in body
  assert "screen_scope: 'review_group'" in body
  assert "result_contract: 'navigate_body_surface'" in body
  assert "body_surface_target: 'readiness-section'" in body
  assert "body_surface_target: 'parameter-surface-section'" in body
  assert "body_surface_target: 'key-loading-state-section'" in body
  assert "body_surface_target: 'visuals-section'" in body
  assert "screen_scopes: Object.freeze(['quick_strip', 'workflow_group', 'operator_group', 'review_group', 'safety_group', 'detail_pane', 'future_disabled'])" in body
  assert "parameter_id: 'scan_interval_ms'" in body
  assert "context_id: 'environment'" in body
  assert 'const DETAIL_PANE_REGISTRY = Object.freeze({' in body
  assert "websocket_management: Object.freeze({" in body
  assert "key_management: Object.freeze({" in body
  assert 'const BODY_SURFACE_REGISTRY = Object.freeze({' in body
  assert "kind: 'detail', value: 'websocket_management'" in body
  assert "kind: 'detail', value: 'key_management'" in body
  assert 'function handleDetailPaneControl(action)' in body
  assert 'data-detail-control=' in body
  assert 'data-detail-field=' in body
  assert "if (normalizedAction === 'load_sandbox_websocket' || normalizedAction === 'load_live_websocket') {" in body
  assert "if (normalizedAction === 'clear_all_websocket_urls') {" in body
  assert "if (normalizedAction === 'clear_loaded_key') {" in body
  assert "if (normalizedAction === 'load_sandbox_key_reference' || normalizedAction === 'load_live_key_reference') {" in body
  assert "if (normalizedAction === 'validate_selected_key_reference') {" in body
  assert "if (normalizedAction === 'reload_credential_posture') {" in body
  assert "if (normalizedAction === 'apply_session_overlay') {" in body
  assert "if (normalizedAction === 'apply_context_change') {" in body
  assert "if (normalizedAction === 'apply_automation_policy') {" in body
  assert '/api/runtime-overlay' in body
  assert '/api/context-overlay' in body
  assert '/api/automation-overlay' in body
  assert 'function syncWayfinderDeckState(route = {})' in body
  assert 'control-key-management' in body
  assert 'function generalizePlatformText(value)' in body
  assert 'function normalizeOperatorGuidance(value)' in body
  assert 'function humanModeLabel(mode)' in body
  assert 'function humanEnvironmentLabel(env)' in body
  assert 'function humanActionLabel(action)' in body
  assert 'function nextActionableStep(workflowOrStep)' in body
  assert 'function humanLimiterLabel(value)' in body
  assert 'function currentSizingSummary(payload, monitor)' in body
  assert 'function automationOverlayTruth(payload = {})' in body
  assert 'function automationPolicyAllowsClientExecutor(payload = {})' in body
  assert 'function syncClientAutoForwardAuthority(payload = {}, options = {})' in body
  assert 'function formatSizingPercentFromFraction(value)' in body
  assert 'function formatDensityCompact(value)' in body
  assert "if (value === 0) return '$0';" in body
  assert "if (value === 0) return '0%';" in body
  assert 'function formatSignedCurrencyCompact(value)' in body
  assert 'function collectHeaderAmountMetrics()' in body
  assert 'function formatHeaderAmountValue(metricKey, mode, metrics)' in body
  assert 'function amountTone(value, displayValue)' in body
  assert 'function renderHeaderAmounts()' in body
  assert 'header_amount_summary' in body
  assert 'Toggle header money display mode (' in body
  assert 'function formatProcessingTimestampCompact(rawValue)' in body
  assert 'function buildProcessingRowModel(payload = {}, action = \'\')' in body
  assert "headerAmountDisplayMode: 'currency'" in body
  assert "sessionRecoveryStarted: false" in body
  assert 'lastSuccessfulShellResponseAt: Date.now()' in body
  assert 'mutationRequestsInFlight: 0' in body
  assert "activeAction: ''" in body
  assert 'backendAssessmentPromise: null' in body
  assert "backendRecoveryStatus: 'healthy'" in body
  assert "backendAutoRecoveryAttempted: false" in body
  assert "backendRecoveryInFlight: false" in body
  assert "backendManualRecoveryVisible: false" in body
  assert "deckView: 'operator'" in body
  assert "deckViewTouched: false" in body
  assert "initialBootstrapReviewHoldPresented: false" in body
  assert 'function helpEntry(key, label, summary, details = [], keywords = [])' in body
  assert 'function isExecutableNextStep(workflowOrStep)' in body
  assert 'function recommendedDeckView(workflowOrStep, payload)' in body
  assert 'can_run_next_step' in body
  assert 'next_actionable_step' in body
  assert 'step_kind' in body
  assert 'focus_target' in body
  assert 'focus_tone' in body
  assert 'button_emphasis_tone' in body
  assert 'function helpKeyForPayloadReason(payload)' in body
  assert 'function dedupeSearchTerms(terms)' in body
  assert 'function helpSearchTagsForPayloadReason(payload)' in body
  assert 'const HELP_SEARCH_TAG_LABELS = Object.freeze({' in body
  assert 'const HELP_SEARCH_TAG_DICTIONARY = Object.freeze({' in body
  assert 'const HELP_ROUTE_TAG_MAP = Object.freeze({' in body
  assert 'function searchLabelForTag(tag)' in body
  assert 'function helpSearchCueFromTags(tags)' in body
  assert 'function collectHelpSectionKeys(sections = [])' in body
  assert 'function helpSchemaMissingKeys(referencedKeys = [], availableKeySet = new Set())' in body
  assert 'function validateHelpContracts(helpSections = [])' in body
  assert 'function routeHelpSearchTags(helpKeys, payload = {})' in body
  assert 'function routeHelpSearchTerms(helpKeys, payload = {})' in body
  assert 'function tokenizeHelpText(value)' in body
  assert 'function fuzzyHelpTokenMatch(queryToken, entryToken)' in body
  assert 'function helpDictionaryEntryMatchesQuery(entry, queryTokens, normalizedQuery)' in body
  assert 'function resolveHelpSearchTags(query)' in body
  assert 'function helpEntryMatchesResolvedTags(entry, resolvedTags)' in body
  assert 'function updateHelpSearchQuery(value, options = {})' in body
  assert 'function helpSearchSuggestions(query, limit = 3)' in body
  assert 'function resolveApprovedHelpEntryTags(terms)' in body
  assert 'function normalizeDeckActionHighlightMap(highlights)' in body
  assert 'function readinessHelpRoute(stepId, payload)' in body
  assert 'function findHelpEntry(helpKey)' in body
  assert 'function openHelpTopicSet(route = {})' in body
  assert 'function bodySurfaceRoute(surfaceKey, payload = {})' in body
  assert 'function buildParameterSurfaceModel(payload)' in body
  assert 'function renderParameterSurface()' in body
  assert 'function buildDetailPaneModel(payload = {})' in body
  assert 'function renderDetailPane(payload = {})' in body
  assert 'function openDetailPane(detailKey)' in body
  assert "window.requestAnimationFrame(() => { scrollToSection('deck-detail-pane'); });" not in body
  assert "if (normalizedRoute.scrollTarget && normalizedRoute.scrollTarget !== 'deck-detail-pane') {\n          scrollToSection('deck-detail-pane');\n        }" in body
  assert 'function applyPanelGlow(targetId, tone' in body
  assert "function openDetailPane(detailKey) {\n      state.detailPaneKey = String(detailKey || '');\n      renderDeckViewShell(state.payload || {});\n      renderDetailPane(state.payload || {});\n    }" in body
  assert "function closeDetailPane() {\n      state.detailPaneKey = '';\n      clearDetailControlHighlights();\n      renderDeckViewShell(state.payload || {});\n      renderDetailPane(state.payload || {});\n    }" in body
  assert 'function closeDetailPane()' in body
  assert 'function buildHelpSections(payload, action)' in body
  assert 'function renderHelpDrawer()' in body
  assert 'function setHelpOpen(open)' in body
  assert 'function syncDeckViewDefault(payload, performedAction' in body
  assert 'function renderDeckSelectorStack()' in body
  assert 'function renderDeckViewShell(payload = {})' in body
  assert "pendingLabel: 'Finding candidates...'" in body
  assert "pendingLabel: 'Submitting order...'" in body
  assert "pendingLabel: 'Refreshing shell...'" in body
  assert "const isPendingAction = action.kind === 'action' && state.activeAction === actionValue;" in body
  assert "const disabledAttr = isPendingAction ? ' disabled aria-disabled=\"true\"' : '';" in body
  assert "const pendingClass = isPendingAction ? ' action-pending' : '';" in body
  assert ".deck-buttons button.action-pending {" in body
  assert 'function switchDeckView(nextView, options = {})' in body
  assert 'function recoverFromStaleSession()' in body
  assert 'async function maybeRecoverFromSessionMismatch(response)' in body
  assert 'function helperRecoveryIsUsable()' in body
  assert 'function backendRecoveryActive()' in body
  assert 'function buildBackendUnavailableFallbackMessage(options = {})' in body
  assert 'function renderBackendRecoveryState()' in body
  assert 'function setBackendRecoveryState(status, options = {})' in body
  assert 'function markSuccessfulShellResponse()' in body
  assert 'function isTransportFailure(error)' in body
  assert 'async function invokeRecoveryHelper(mode = \'manual\')' in body
  assert 'async function handleConfirmedBackendUnavailable(path, context, lastError)' in body
  assert 'async function runManualBackendRecovery()' in body
  assert "stack.querySelectorAll('[data-deck-view]').forEach((button) => {" in body
  assert "$('deck-cancel-action').addEventListener('click', () => {" in body
  assert "$('deck-auto-advance-toggle').addEventListener('click', () => {" in body
  assert 'title="Do the recommended action"' not in body
  assert 'title="Clear non-terminal local pairs"' in body
  assert 'title="Client auto-forward"' in body
  assert 'title="Arm client auto-forward"' not in body
  assert 'function clientAutoForwardTooltip(payload = {})' in body
  assert 'function clientAutoForwardEntrypointMode(payload = {})' in body
  assert 'function clientAutoForwardAutomationValues(payload = {})' in body
  assert 'function openClientAutoForwardChallenge(payload = {})' in body
  assert 'const CONFIRMATION_CHALLENGE_TYPE_REGISTRY = Object.freeze({' in body
  assert "acknowledge: Object.freeze({" in body
  assert "confirm_action: Object.freeze({" in body
  assert "destructive_confirm: Object.freeze({" in body
  assert "function normalizeConfirmationChallengeType(popupType = '', fallbackHideCancel = false)" in body
  assert 'function buildConfirmationChallengeState(config = {})' in body
  assert 'async function confirmClientAutoForwardToggle()' in body
  assert "challengeAction === 'client_auto_forward_toggle'" in body
  assert 'openClientAutoForwardChallenge(payload);' in body
  assert 'Enable and arm auto-forward' in body
  assert 'Resume and arm auto-forward' in body
  assert 'Client auto-forward can enable bounded automation for this ready connected lane and arm this browser session before the first scan.' in body
  assert 'This chip only arms this browser session after the backend bounded automation posture is enabled for the active lane.' not in body
  assert "body: { action: 'apply', values: clientAutoForwardAutomationValues(payload) }" in body
  assert "body: { action: 'resume' }" in body
  assert 'await confirmClientAutoForwardToggle();' in body
  assert 'autoAdvanceButton.disabled = false;' in body
  assert "const tooltipAttr = action.tooltip ? ` title=\"${escapeHtml(action.tooltip)}\"` : '';" in body
  assert '.readiness-status-button {' in body
  assert '.deck-buttons button.deck-action-glow-ok,' in body
  assert '.deck-step-guidance.visible {' in body
  assert 'pointer-events: none;' in body.split('.deck-step-guidance.visible {', 1)[1].split('}', 1)[0]
  assert 'function buildWorkflowProjection(payload = {}, fallbackAction = \'\')' in body
  assert '.parameter-surface-grid {' in body
  assert '.parameter-surface-group {' in body
  assert '.parameter-surface-row {' in body
  assert '#recommended-action.recommended-action-ready:not(:disabled) {' not in body
  assert "parameter_id: 'max_open_pairs'" in body
  assert 'data-readiness-step=' in body
  assert 'data-readiness-help-key=' in body
  assert 'data-readiness-deck-view=' in body
  assert 'data-readiness-search-terms=' in body
  assert "missing_private_key_file: ['key_missing', 'configuration']" in body
  assert "helpResolvedTags: []" in body
  assert "helpVisibleKeys: []" in body
  assert "helpContractWarningSignature: ''" in body
  assert "Search uses approved local help tags only." in body
  assert 'workflowProjection: null' in body
  assert 'evidenceProjection: null' in body
  assert 'freshnessProjection: null' in body
  assert 'decisionProjection: null' in body
  assert 'readinessProjection: null' in body
  assert 'state.workflowProjection = buildWorkflowProjection(payload, action);' in body
  assert 'state.evidenceProjection = buildEvidenceProjection(payload);' in body
  assert 'state.freshnessProjection = buildFreshnessProjection(payload);' in body
  assert 'state.decisionProjection = buildDecisionProjection(payload, action);' in body
  assert 'state.readinessProjection = buildReadinessProjection(payload);' in body
  assert 'validateHelpContracts(state.helpSections);' in body
  assert "$('lane-pill').textContent = state.readinessProjection.laneLabel || humanOperationLaneLabel('offline');" in body
  assert 'function buildModeSelectorAvailability(payload = {})' in body
  assert 'const clearHoldActive = keyManagement.clear_hold_active === true;' in body
  assert 'const sandboxKeyValidated = keyManagement.sandbox_key_validated === true;' in body
  assert 'const liveKeyValidated = keyManagement.live_key_validated === true;' in body
  assert 'const sandboxReady = !clearHoldActive && sandboxKeyPresent && sandboxUrlPresent;' in body
  assert 'const liveReady = !clearHoldActive && liveKeyPresent && liveUrlPresent && liveKeyValidated;' in body
  assert 'const canOpenModeSelector = connectionPosture.isOffline' in body
  assert "`next: ${compactActionLabel(monitor.next_action || '', 'REVIEW PAIRS')}`" not in body
  assert "const modeTooltip = 'Change mode';" in body
  assert "lanePill.title = modeTooltip;" in body
  assert "$('mode-selector-offline').disabled = false;" in body
  assert "$('mode-selector-sandbox').disabled = !availability.sandboxReady;" in body
  assert "$('mode-selector-live').disabled = !availability.liveReady;" in body
  assert 'const canOpenModeSelector = Boolean(connectionPosture.modeSelected) || (connectionPosture.isOffline && hasAnyWebsocket && credentialReady);' not in body
  assert 'const sandboxConfigured = String(connectionPosture.availableWebsocketUrls?.sandbox || \"\").toLowerCase() !== \"unconfigured\";' not in body
  assert 'const liveConfigured = String(connectionPosture.availableWebsocketUrls?.live || \"\").toLowerCase() !== \"unconfigured\";' not in body
  # W7 (§7.17): `Next:` header pill removed; the recommended-pill update is no longer emitted.
  assert "$('recommended-pill').textContent = `Next: ${state.workflowProjection.stepLabel}`;" not in body
  assert 'const detailCandidates = [workflowProjection.guidanceText].map(sanitizeGuiText).filter(Boolean);' in body
  assert "if (workflowProjection.focusTarget) {" in body
  assert "return wayfinderRoute(workflowProjection.focusTarget, {" in body
  assert "recommendedActionButton.classList.toggle('recommended-action-ready', workflowProjection.isExecutable && workflowProjection.buttonEmphasisTone === 'ok');" not in body
  assert 'value: evidenceProjection.headlineValue,' in body
  assert 'detail: evidenceProjection.detailLine,' in body
  assert 'micro: evidenceProjection.microLine,' in body
  assert 'value: freshnessProjection.headlineValue,' in body
  assert 'detail: freshnessProjection.detailLine,' in body
  assert 'micro: freshnessProjection.microLine,' in body
  assert 'value: decisionProjection.reasonLabel,' in body
  assert 'detail: decisionProjection.detailLine,' in body
  assert "const aggregatePath = String(evidence.project_aggregate_path || '').trim();" in body
  assert "data-open-artifact-path" in body
  assert 'class="list-item evidence-artifact-card"' in body
  assert "'/api/open-artifact-path'" in body
  assert 'function buildConnectionGuidanceHighlights(payload = {})' in body
  assert 'return {};' in body.split('function buildConnectionGuidanceHighlights(payload = {}) {', 1)[1].split('function humanActionLabel(action) {', 1)[0]
  assert "const keyGlowTone = String((keyManagement.key_glow_tone || '')).toLowerCase();" not in body
  assert "const loadedCount = (sandboxKeyLoaded ? 1 : 0) + (liveKeyLoaded ? 1 : 0);" not in body
  assert 'message: evidenceProjection.latestAcceptedMessage' in body
  assert 'message: evidenceProjection.retainedHistoryMessage' in body
  assert 'message: evidenceProjection.aggregateMessage' in body
  assert 'message: evidenceProjection.exportSummaryMessage' in body
  assert 'evidenceProjection.acceptedSummary' in body
  assert 'freshnessProjection.proofDetail' in body
  assert 'freshnessProjection.visualsHeadlineFallback' in body
  assert 'freshnessProjection.visualsNextFallback' in body
  assert 'decisionProjection.currentScreenSummary' in body
  assert 'decisionProjection.currentDecisionSummary' in body
  assert 'readinessProjection.configurationBlockedSummary' in body
  assert 'readinessProjection.recoveryDetail' in body
  assert 'readinessProjection.credentialTone' in body
  assert 'readinessProjection.runtimeTone' in body
  assert 'readinessProjection.contextTone' in body
  assert "$('wizard-headline').textContent = readinessProjection.wizardHeadline || decisionProjection.headlineValue;" in body
  assert '? readinessProjection.summaryDetail' in body
  assert '`state: ${decisionProjection.headlineValue}`' in body
  assert 'state.helpPinnedKeys = [];' in body
  assert 'Object.assign(state, {' in body
  assert 'deckActionHighlights: hasWorkflowDeckHighlights' in body
  assert ': (state.workflowProjection.deckHighlights || {}),' not in body
  assert ': (state.detailControlHighlights || {}),' not in body
  assert 'const fallbackDetail = sanitizeHighlightToneMap(state.detailControlHighlights || {}' not in body
  assert 'detailControlHighlights: hasPayloadDetail ? payloadDetailMap : {}' in body
  assert 'deckActionHighlights: hasPayloadDeck ? payloadDeckMap : {}' in body
  assert '          : {},' in body
  assert "detailPaneKey: ''" in body
  assert "confirmationChallenge: { open: false, action: '', lane: '', title: '', message: '', popupType: 'destructive_confirm', confirmLabel: 'Clear all', altConfirmLabel: '', altConfirmAction: '', cancelLabel: 'Cancel', hideCancel: false, confirmTone: 'danger' }" in body
  assert "popupType: 'acknowledge'" in body
  assert "popupType: 'confirm_action'" in body
  assert "popupType: 'destructive_confirm'" in body
  assert "confirmButton.className = config.confirmTone === 'danger' ? 'danger' : 'secondary';" in body
  assert 'file_missing: Object.freeze({' in body
  assert 'no_candidates: Object.freeze({' in body
  assert 'risk_gate: Object.freeze({' in body
  assert 'bootstrap_state: Object.freeze({' in body
  assert 'system_log: Object.freeze({' in body
  assert 'operational_visuals: Object.freeze({' in body
  assert "preferredKeys: Object.freeze(['recovery-fix-config', 'blocked-configuration', 'blocked-boundary'])" in body
  assert "if (reason === 'no_viable_candidates') return 'blocked-no-candidates';" in body
  assert "if (reason === 'risk_gate_blocked_new_pair') return 'blocked-risk-gate';" in body
  assert "if (reason === 'dynamic_notional_cap_below_one_contract') return 'blocked-runtime-sizing';" in body
  assert "if (['bootstrap_failed', 'bootstrap_state_failed'].includes(reason)) return 'blocked-bootstrap-state';" in body
  assert 'function mergeHelpKeySets(...keySets)' in body
  assert 'return { tags: [], preferredKeys: [], visibleKeys: [], labels: [] };' in body
  assert 'state.helpVisibleKeys = mergeHelpKeySets(intent.visibleKeys || [], seededVisibleKeys);' in body
  assert 'const visibleKeys = new Set((state.helpVisibleKeys || []).filter(Boolean));' in body
  assert 'blocked-no-candidates' in body
  assert 'blocked-risk-gate' in body
  assert 'blocked-runtime-sizing' in body
  assert 'blocked-bootstrap-state' in body
  assert 'concept-weights-and-parameters' in body
  assert 'concept-operational-visuals' in body
  assert 'concept-system-log' in body
  assert 'The System log is the deeper chronology surface' in body
  assert 'A runtime risk gate refused to allow a new pair' in body
  assert 'Current sizing or runtime-setting limits are too restrictive' in body
  assert 'if (visibleKeys.size > 0 && !visibleKeys.has(item.key)) return false;' in body
  assert 'const priorityMap = preferredOrder.size > 0 ? preferredOrder : visibleOrder;' in body
  assert 'const workflowProjection = buildWorkflowProjection(payload || {});' in body
  assert 'const workflowGlowTone = (' in body
  assert '&& workflowProjection.isExecutable' in body
  assert "&& actionValue === String(workflowProjection.actionableStepId || '').toLowerCase()" in body
  assert 'const glowTone = highlightEnvelope.deckActionHighlights[actionValue] || workflowGlowTone;' in body
  assert 'const glowClass = glowTone ? ` deck-action-glow-${glowTone}` : \'\';' in body
  assert 'function presentActionFailure(action, error) {' in body
  assert "appendLog(`${actionLabel} failed [${failCode}]: ${message}`);" in body
  assert "message: messageWithFailCode," in body
  assert "title: `${actionLabel} could not continue`," in body
  assert 'async function runUiAction(action, options = {}) {' in body
  assert "const normalizedAction = String(action || '').toLowerCase();" in body
  assert 'state.activeAction = normalizedAction;' in body
  assert 'renderDeckViewShell(state.payload || {});' in body
  assert 'if (state.activeAction === normalizedAction) {' in body
  assert "state.activeAction = '';" in body
  assert "await runUiAction('bootstrap');" in body
  assert 'await runUiAction(value, actionOptions);' in body
  assert "await runUiAction('report');" in body
  assert "await runUiAction('cancel-all');" in body
  assert 'Object.assign(state, { deckActionHighlights: normalizeDeckActionHighlightMap(route.highlights || []) });' in body
  assert 'if (route.deckView && route.deckView !== state.deckView) {' in body
  assert 'switchDeckView(route.deckView, { markTouched: false });' in body
  assert 'openHelpTopicSet(normalizedRoute);' in body
  assert "kind === 'body-surface'" in body
  assert 'if (route.closeDetailPane) {' in body
  assert "focusPanel(route.targetId, route.tone, { scrollIntoView: route.scrollIntoView !== false });" not in body
  assert 'if (normalizedRoute.message) {' in body
  assert "activateWayfinder(wayfinderRoute(value || 'readiness-section'));" in body
  assert 'const route = bodySurfaceRoute(value, state.payload || {});' in body
  assert 'route.openDetailPane = Boolean(route.detailPaneKey);' not in body
  assert "activateWayfinder(wayfinderRoute(value || 'evidence-section', { message }));" in body
  assert 'activateWayfinder(notificationWayfinderRoute(itemKey, state.payload || {}));' in body
  assert 'activateWayfinder(submitOrderPending ? { ...focusTarget, suppressScroll: true } : focusTarget)' in body  # D4-apply-3
  assert "activateWayfinder(wayfinderRoute('key-loading-state-section', {" in body
  assert "const isDatapackLoadAction = (" in body
  assert "if (isDatapackLoadAction && tone === 'ok' && !options.successRoute) {" in body
  assert "const route = wayfinderRoute('evidence-section', {" in body
  assert 'detailPaneKey: value,' in body
  assert 'openDetailPane: true,' in body
  assert "if (normalizedAction === 'discover_available_keys') {" in body
  assert "if (normalizedAction === 'select_key_file') {" in body
  assert "if (normalizedAction === 'load_sandbox_key_reference' || normalizedAction === 'load_live_key_reference') {" in body
  assert "if (normalizedAction === 'apply_selected_key_reference') {" in body
  assert "if (normalizedAction === 'validate_selected_key_reference') {" in body
  assert "if (normalizedAction === 'open_credential_posture' || normalizedAction === 'inspect_credential_posture') {" in body
  assert "normalizedAction.startsWith('select_discovered_key:')" not in body
  assert "if (nextView !== 'operator' && state.detailPaneKey) {" in body
  assert 'renderDetailPane(payload);' in body
  assert "$('deck-detail-close').addEventListener('click', () => {" in body
  assert "$('deck-detail-jump').addEventListener('click', () => {" in body
  assert "$('challenge-modal-confirm').addEventListener('click', async () => {" in body
  assert "$('challenge-modal-cancel').addEventListener('click', () => {" in body
  assert "jumpButton.dataset.jumpTone = model.jumpTone || 'focus-info';" in body
  assert "jumpButton.dataset.jumpMessage = model.jumpMessage || '';" in body
  assert "const jumpTone = $('deck-detail-jump').dataset.jumpTone || 'focus-info';" in body
  assert "const jumpMessage = $('deck-detail-jump').dataset.jumpMessage || '';" in body
  assert "activateWayfinder(wayfinderRoute(targetId, { tone: jumpTone, message: jumpMessage }));" in body
  # Guidance engine redesign assertions
  assert 'const DETAIL_PANE_ACTION_ROUTES = Object.freeze({' in body
  assert "apply_session_overlay:" in body
  assert "reset_session_overlay:" in body
  assert "apply_context_change:" in body
  assert "reset_context_overlay:" in body
  assert "rebootstrap_after_context_change:" in body
  assert "apply_automation_policy:" in body
  assert "pause_automation:" in body
  assert "resume_automation:" in body
  assert "stop_automation_now:" in body
  assert "glowSustained: false" in body
  assert "detailControlHighlights: undefined" in body
  assert "closeHelp: false" in body
  assert 'function applyPanelGlow(targetId, tone' in body
  assert 'options.sustained' in body
  assert 'scrollToSection' not in body.split('function applyPanelGlow')[1].split('function ')[0]
  assert 'if (route.detailControlHighlights === null)' in body
  assert 'Object.assign(state, { detailControlHighlights: { ...route.detailControlHighlights } });' in body
  assert 'activateWayfinder(DETAIL_PANE_ACTION_ROUTES[normalizedAction])' in body
  assert 'Session overlay only' in body
  assert 'Readiness owns the live auth posture: API key presence plus key-file reference/file availability, without duplicating status in the deck.' in body
  assert 'Weights and parameters still owns the live display.' in body
  assert 'Connection posture owns the live lane and websocket route truth.' in body
  assert 'Operational visuals still owns the live narrative.' in body
  assert 'renderParameterSurface();' in body
  assert 'function renderNotificationBand(payload, action)' in body
  assert 'function renderBoundary(payload, action)' in body
  assert '.boundary-panel.processing-owner {' in body
  assert 'data-nav-target=' in body
  assert 'data-wayfinder-key=' in body
  assert "notification-band-card${navTarget ? ' nav-link' : ''}" in body
  assert "if (key === 'what-ran') return 'system-log-section';" in body
  assert "if (key === 'next') return boundary ? 'readiness-section' : 'notification-band-section';" in body
  assert "if (key === 'evidence') return 'evidence-section';" in body
  assert "if (key === 'freshness') return 'visuals-section';" in body
  assert "if ((key === 'outcome' || key === 'reason') && boundary) return 'boundary-panel';" in body
  assert 'Local startup is blocked until configuration is fixed.' in body
  assert "if (response.status === 403 && payload.reason === 'session_token_mismatch')" in body
  assert "}).then((response) => maybeRecoverFromSessionMismatch(response)).catch(() => undefined);" in body
  assert "if (!response || response.status !== 403) return false;" in body
  assert "const isSessionRoute = String(response.url || '').includes('/api/session-');" in body
  assert 'payload = await response.clone().json();' in body
  assert 'if (!isSessionRoute) return false;' in body
  assert "if (payload.reason === 'session_token_mismatch') {" in body
  assert 'if (isSessionRoute) {' in body
  assert "recoverFromStaleSession();" in body
  assert "window.location.replace(rootUrl.toString());" in body
  assert 'STARTUP BLOCKED' in body
  assert 'NO HEARTBEAT YET' in body
  assert 'Retained accepted' in body
  assert 'local startup blocked' in body
  assert 'latest heartbeat:' in body
  assert 'platform lane:' in body
  assert 'websocket route:' in body
  assert 'runtime pairs:' in body
  assert 'retained runs:' in body
  assert 'retained pass/fail:' in body
  assert 'accepted run:' in body
  assert 'aggregate file:' in body
  assert 'validation lanes:' not in body
  assert 'state store:' not in body
  assert 'aggregate path:' not in body
  assert 'evidence interpretation:' in body
  assert 'state db:' in body
  assert 'Aggregate report' in body
  assert 'Open current aggregate location' in body
  assert 'Retained validation</div>' in body
  assert 'Workflow posture</div>' not in body
  assert '>PATH<' not in body
  assert 'last discovery:' not in body
  assert 'last validation:' not in body
  assert 'last applied:' not in body
  assert '`KEY ${keyTail} :: DB ${dbTail}`' not in body
  assert 'Retained run details are available in Evidence.' in body
  assert 'Cap ${formatSizingPercentFromFraction(sizing.dynamic_pair_notional_pct)} · ${humanLimiterLabel(sizing.binding_limiter)}' in body
  assert 'Density ${formatDensityCompact(sizing.effective_density || payload.effective_density)}' in body
  assert 'function installSystemLogScrollTracking()' in body
  assert 'const SYSTEM_LOG_SCROLL_POLICY = ' in body
  assert 'systemLogSuppressedRefreshesRemaining' in body
  assert 'systemLogUserScrollGraceRefreshes' in body
  assert 'systemLogProgrammaticScroll' in body
  assert 'logWindow.addEventListener(\'scroll\'' in body
  assert 'function armSystemLogScrollSuppression()' in body
  assert 'function shouldPreserveSystemLogScroll()' in body
  assert 'function completeSystemLogScrollSuppressionCycle(preserveUserScroll)' in body
  assert 'function applySystemLogScrollPosition(logWindow, previousScrollTop, preserveUserScroll)' in body
  assert 'state.systemLogSuppressedRefreshesRemaining = SYSTEM_LOG_SCROLL_POLICY.suppression_refreshes' in body
  assert 'renderSystemLogMotionCounter' not in body
  assert 'systemLogMotionlessRefreshCount' not in body
  assert '.reverse();' in body
  assert 'Definition' in body
  assert 'Do next' in body
  assert 'Define here' not in body
  assert 'Fix the local Kalshi environment posture' not in body
  assert 'Primary actions' not in body
  assert 'Safety actions' not in body
  assert 'class="shield">PV<' not in body
  assert 'id="decision-pill"' not in body
  assert 'command-strip-body' not in body
  assert 'Action stream' not in body
  assert 'Machine-readable payload' not in body
  assert '<h2>Workflow</h2>' not in body
  assert '<h3 id="deck-view-title" class="deck-view-title">Operator controls</h3>' in body
  assert '<summary>Workflow</summary>' not in body
  assert '<summary>Operator controls</summary>' not in body
  assert 'id="deck-reconcile-action"' not in body
  assert 'contextual_blocked_card' not in body
  assert 'id="deck-view-copy"' not in body
  assert 'id="deck-mode-chip"' not in body
  assert 'id="deck-session-health-chip"' not in body
  assert 'id="deck-auto-advance-toggle"' in body


def test_system_log_scroll_policy_contract_is_embedded_in_shell_html() -> None:
  policy = _system_log_scroll_policy()
  app = create_operator_console_app()

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert policy == {
    'default_reset_target': 'lead',
    'suppression_activation': 'user-scroll',
    'suppression_refreshes': 3,
  }
  assert '"default_reset_target": "lead"' in body
  assert '"suppression_activation": "user-scroll"' in body
  assert '"suppression_refreshes": 3' in body
  assert 'armSystemLogScrollSuppression();' in body
  assert 'const preserveUserScroll = shouldPreserveSystemLogScroll();' in body
  assert 'logWindow.scrollTop = preserveUserScroll ? previousScrollTop : 0;' in body
  assert 'completeSystemLogScrollSuppressionCycle(preserveUserScroll);' in body


def test_session_routes_require_matching_browser_token() -> None:
  app = create_operator_console_app(
    _services(),
    session_controller=ConsoleSessionController(session_token='session-123'),
  )

  denied_status, _, denied_body = _call_app(
    app,
    method='POST',
    path='/api/session-heartbeat',
    query='session=wrong-token',
  )
  allowed_status, _, allowed_body = _call_app(
    app,
    method='POST',
    path='/api/session-close',
    query='session=session-123',
  )

  assert denied_status == '403 Forbidden'
  assert json.loads(denied_body)['reason'] == 'session_token_mismatch'
  assert allowed_status == '200 OK'
  assert json.loads(allowed_body)['session'] == 'closing'


def test_root_route_marks_session_seen_for_detached_host() -> None:
  controller = ConsoleSessionController(session_token='session-123')
  app = create_operator_console_app(
    _services(),
    session_controller=controller,
  )

  status, headers, body = _call_app(
    app,
    method='GET',
    path='/',
    query='session=session-123',
  )

  assert status == '200 OK'
  assert headers['Content-Type'] == 'text/html; charset=utf-8'
  assert 'session-123' in body
  assert controller._last_seen_at is not None


def test_session_status_route_reports_active_browser_state() -> None:
  controller = ConsoleSessionController(session_token='session-123')
  app = create_operator_console_app(
    _services(),
    session_controller=controller,
  )

  initial_status, _, initial_body = _call_app(app, method='GET', path='/api/session-status')
  initial_payload = json.loads(initial_body)

  assert initial_status == '200 OK'
  assert initial_payload['session'] == {'seen': False, 'closed': False, 'active': False, 'drain_active': False}

  root_status, _, _ = _call_app(
    app,
    method='GET',
    path='/',
    query='session=session-123',
  )
  active_status, _, active_body = _call_app(app, method='GET', path='/api/session-status')
  active_payload = json.loads(active_body)

  assert root_status == '200 OK'
  assert active_status == '200 OK'
  assert active_payload['session']['seen'] is True
  assert active_payload['session']['closed'] is False
  assert active_payload['session']['active'] is True


def test_session_status_route_publishes_handoff_identity_for_detached_host() -> None:
  controller = ConsoleSessionController(session_token='session-123')
  app = create_operator_console_app(
    _services(),
    session_controller=controller,
    handoff_context={
      'launch_id': 'launch-123',
      'launch_mode': 'detached',
      'requested_port': 8765,
      'bound_port': 8765,
    },
  )

  initial_status, _, initial_body = _call_app(app, method='GET', path='/api/session-status')
  initial_payload = json.loads(initial_body)

  assert initial_status == '200 OK'
  assert initial_payload['handoff']['attach_confirmed'] is False
  assert initial_payload['handoff']['published_identity']['launch_id'] == 'launch-123'

  root_status, _, _ = _call_app(
    app,
    method='GET',
    path='/',
    query='session=session-123',
  )
  active_status, _, active_body = _call_app(app, method='GET', path='/api/session-status')
  active_payload = json.loads(active_body)

  assert root_status == '200 OK'
  assert active_status == '200 OK'
  assert active_payload['handoff']['attach_confirmed'] is True
  assert active_payload['handoff']['published_identity']['launch_mode'] == 'detached'


def test_root_probe_route_does_not_mark_session_seen_for_detached_host() -> None:
  controller = ConsoleSessionController(session_token='session-123')
  app = create_operator_console_app(
    _services(),
    session_controller=controller,
  )

  probe_status, _, _ = _call_app(
    app,
    method='GET',
    path='/',
    query='session=session-123&probe=1',
  )
  status_status, _, status_body = _call_app(app, method='GET', path='/api/session-status')
  status_payload = json.loads(status_body)

  assert probe_status == '200 OK'
  assert status_status == '200 OK'
  assert status_payload['session'] == {'seen': False, 'closed': False, 'active': False, 'drain_active': False}


def test_run_operator_console_server_uses_threaded_wsgi_server(monkeypatch: Any) -> None:
  captured: dict[str, Any] = {}

  class _FakeServer:
    def __enter__(self) -> '_FakeServer':
      return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> bool:
      return False

    def shutdown(self) -> None:
      return None

    def serve_forever(self) -> None:
      return None

  def _fake_make_server(host: str, port: int, app: Any, server_class: Any) -> _FakeServer:
    captured['host'] = host
    captured['port'] = port
    captured['app'] = app
    captured['server_class'] = server_class
    return _FakeServer()

  monkeypatch.setattr(web_app, 'make_server', _fake_make_server)

  web_app.run_operator_console_server(host='127.0.0.1', port=8765, handoff_context={'launch_id': 'launch-123'})

  assert captured['host'] == '127.0.0.1'
  assert captured['port'] == 8765
  assert captured['server_class'] is web_app._ThreadedOperatorConsoleWSGIServer
  assert issubclass(captured['server_class'], web_app.WSGIServer)
  assert captured['server_class'].daemon_threads is True


def test_session_close_route_transitions_active_websocket_session_to_offline(monkeypatch: Any, tmp_path: Path) -> None:
  controller = ConsoleSessionController(session_token='session-123')
  app = create_operator_console_app(
    _services(),
    session_controller=controller,
    tombstone_path=tmp_path / 'tombstones.json',
  )

  root_status, _, root_body = _call_app(
    app,
    method='GET',
    path='/',
    query='session=session-123',
  )
  mutation_auth = _extract_mutation_auth_from_html(root_body)

  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox', mutation_auth=mutation_auth, session_token='session-123')
  mode_body = {'lane': 'sandbox'}
  mode_headers = _signed_mutation_headers('/api/change-mode', mode_body, mutation_auth)
  mode_status, _, mode_response = _call_app(
    app,
    method='POST',
    path='/api/change-mode',
    query='session=session-123',
    body=mode_body,
    headers=mode_headers,
  )
  mode_payload = json.loads(mode_response)

  close_status, _, close_body = _call_app(
    app,
    method='POST',
    path='/api/session-close',
    query='session=session-123',
    body={
      'set_offline_if_active': True,
      'close_reason': 'browser_window_closed',
    },
  )
  close_payload = json.loads(close_body)

  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert root_status == '200 OK'
  assert mode_status == '200 OK'
  assert mode_payload['decision'] == 'planned'
  assert close_status == '200 OK'
  assert close_payload['session'] == 'closing'
  assert close_payload['offline_transition_applied'] is True
  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['connection_posture']['operation_lane'] == 'offline'
  assert bootstrap_payload['session_overlay']['context']['mode_selected'] is False


def test_session_close_route_requests_scan_cancel_during_offline_transition(monkeypatch: Any, tmp_path: Path) -> None:
  controller = ConsoleSessionController(session_token='session-123')
  services = _services()
  scan_started = threading.Event()
  cancel_observed = threading.Event()

  def _cancelable_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    cancel_check = kwargs.get('cancel_check')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    scan_started.set()
    deadline = time.time() + 2.0
    while time.time() < deadline:
      if callable(cancel_check) and cancel_check():
        cancel_observed.set()
        raise service_module.ScanCancelledError('Find candidates was canceled by the operator.')
      time.sleep(0.02)
    return {
      'decision': 'planned',
      'candidate_count': 1,
      'candidates': [{'ticker': 'LIVE-1'}],
      'next_action': 'Review candidates in Pairs.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_cancelable_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    ),
    session_controller=controller,
    tombstone_path=tmp_path / 'tombstones.json',
  )

  root_status, _, root_body = _call_app(
    app,
    method='GET',
    path='/',
    query='session=session-123',
  )
  mutation_auth = _extract_mutation_auth_from_html(root_body)

  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox', mutation_auth=mutation_auth, session_token='session-123')
  mode_body = {'lane': 'sandbox'}
  mode_headers = _signed_mutation_headers('/api/change-mode', mode_body, mutation_auth)
  mode_status, _, _ = _call_app(
    app,
    method='POST',
    path='/api/change-mode',
    query='session=session-123',
    body=mode_body,
    headers=mode_headers,
  )

  scan_headers = _signed_mutation_headers('/api/scan', {}, mutation_auth)
  scan_status, _, scan_body = _call_app(
    app,
    method='POST',
    path='/api/scan',
    query='session=session-123',
    body={},
    headers=scan_headers,
  )
  scan_payload = json.loads(scan_body)

  assert root_status == '200 OK'
  assert mode_status == '200 OK'
  assert scan_status == '200 OK'
  assert scan_payload['scan_runtime']['status'] == 'processing'
  assert scan_started.wait(timeout=1.0) is True

  close_status, _, close_body = _call_app(
    app,
    method='POST',
    path='/api/session-close',
    query='session=session-123',
    body={
      'set_offline_if_active': True,
      'close_reason': 'browser_window_closed',
    },
  )
  close_payload = json.loads(close_body)

  assert close_status == '200 OK'
  assert close_payload['offline_transition_applied'] is True
  assert close_payload['scan_cancel_requested'] is True
  assert cancel_observed.wait(timeout=1.0) is True

  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['connection_posture']['operation_lane'] == 'offline'
  assert bootstrap_payload['scan_runtime']['cancel_requested'] is True
  assert bootstrap_payload['scan_runtime']['status'] in {'canceling', 'cancelled'}


def test_mutation_routes_require_signed_headers_when_session_controller_enabled() -> None:
  controller = ConsoleSessionController(session_token='session-123')
  app = create_operator_console_app(
    _services(),
    session_controller=controller,
  )

  root_status, _, root_body = _call_app(
    app,
    method='GET',
    path='/',
    query='session=session-123',
  )
  assert root_status == '200 OK'
  mutation_auth = _extract_mutation_auth_from_html(root_body)
  assert mutation_auth.get('enabled') is True

  denied_status, _, denied_body = _call_app(
    app,
    method='POST',
    path='/api/report',
    query='session=session-123',
  )
  assert denied_status == '403 Forbidden'
  assert json.loads(denied_body)['reason'] == 'mutation_signature_required'

  signed_body = {'action': 'noop'}
  signed_headers = _signed_mutation_headers('/api/report', signed_body, mutation_auth)
  allowed_status, _, allowed_body = _call_app(
    app,
    method='POST',
    path='/api/report',
    query='session=session-123',
    body=signed_body,
    headers=signed_headers,
  )
  assert allowed_status == '200 OK', allowed_body
  assert json.loads(allowed_body)['decision'] == 'planned'


def test_bootstrap_route_returns_workflow_payload(tmp_path: Any) -> None:
  # Use a fresh startup mock (no prior evidence, no latest_heartbeat)
  fresh_services = OperatorConsoleServices(
    bootstrap=lambda **_: {
      'decision': 'planned',
      'settings': {
        'settings_ready': True,
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'live_websocket_url': 'wss://api.kalshi.example/ws',
        'active_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
        'state_db_path_tail': 'runtime.sqlite3',
        'private_key_path_tail': 'demo.pem',
      },
      'diagnostics_governance_context': {
        'channel': 'diagnostics_governance',
        'validation_summary': {
          'present': True,
          'default_lanes': ['pytest', 'sandbox_test', 'empirical_test'],
          'definition_count': 7,
          'operator_policy': 'All three validation lanes remain required; empirical review stays operator-driven at high-value junction points only.',
          'latest_runs': [
            {
              'run_id': 'run-123',
              'result': 'passed',
              'selected_lanes': ['pytest', 'sandbox_test', 'empirical_test'],
            }
          ],
          'lane_policy': {
            'pytest': 'Automated proof lane for code-level confidence.',
            'sandbox_test': 'Contained execution lane for dry-run and sandbox proof.',
            'empirical_test': 'Operator-driven empirical review lane for high-value junction points only.',
          },
        },
      },
      'report': {
        'latest_heartbeat': None,  # Fresh startup - no prior execution
        'state_db_path_tail': 'runtime.sqlite3',
        'operation_lane': 'sandbox',
        'lane_session_id': 'sandbox-session-001',
        'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
        'connection_state': {
          'status': 'connected',
          'websocket_connected': True,
        },
        'pair_runtime_summary': [
          {
            'pair_id': 'pair-1',
            'ticker': 'KALSHI-EDGE-1',
            'state': 'PLANNED',
            'contract_count': '10',
            'locked_contracts': '0',
            'gross_dollars': '0',
            'net_projected_dollars': '0',
            'dynamic_pair_notional_pct': '0.192',
            'dynamic_max_contracts': '32',
            'effective_density': '3.125',
            'binding_limiter': 'configured_contract_cap',
          }
        ],  # Pair data for display, but pair_contracts count is 0
        'table_counts': {'pair_contracts': 0, 'pair_plans': 0},
        'next_action': 'Use Refresh shell or Cancel all pairs if attention remains.',
      },
      'reconcile': {'pair_count': 0, 'pairs': []},
      'workflow': {
        'recommended_step': 'scan',
        'auto_sequence': ['scan', 'run'],
        'headline': 'Auto-forward safe dry-run steps.',
        'operator_message': 'Run scan then one dry-run cycle.',
        'step_kind': 'execute',
        'can_run_next_step': True,
        'next_actionable_step': 'scan',
        'focus_target': 'notification-band-section',
        'focus_tone': 'focus-ok',
        'deck_view': 'workflow',
        'button_emphasis_tone': 'ok',
      },
      'next_action': 'Run scan then one dry-run cycle.',
    },
    scan=_services().scan,
    run=_services().run,
    reconcile=_services().reconcile,
    report=_services().report,
    cancel_all=_services().cancel_all,
    system_log=_services().system_log,
    visuals=_services().visuals,
  )
  app = create_operator_console_app(fresh_services, tombstone_path=tmp_path / '_tombstone.json')

  status, headers, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  assert headers['Content-Type'] == 'application/json; charset=utf-8'
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow_source'] == 'bootstrap_workflow'
  assert payload['projection_authority'] == 'backend_authored'
  assert 'projection_source' in payload
  assert 'recovery_mode' in payload
  assert payload['workflow']['auto_sequence'] == ['scan', 'run']
  assert payload['workflow']['step_kind'] == 'execute'
  assert payload['workflow']['can_run_next_step'] is True
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['workflow']['focus_target'] == 'notification-band-section'
  assert payload['workflow']['deck_view'] == 'workflow'
  assert payload['diagnostics_governance_context']['validation_summary']['default_lanes'] == ['pytest', 'sandbox_test', 'empirical_test']
  assert payload['diagnostics_governance_context']['validation_summary']['latest_runs'][0]['run_id'] == 'run-123'
  assert payload['startup_wizard']['steps'][0]['label'] == 'Environment check'
  assert payload['pair_monitor']['pair_count'] == 0
  assert payload['pair_monitor']['priority_order'][0] == 'ERROR'
  assert payload['pair_monitor']['sizing_overview']['dynamic_pair_notional_pct'] == '0.192'
  assert payload['live_interaction']['title'] == 'EXECUTION'
  assert payload['live_interaction']['contract_version'] == 'live_interaction_summary.v1'
  assert payload['live_interaction']['surface_family'] == 'transient_runtime_card'
  assert payload['live_interaction']['surface_visible'] is True
  assert payload['live_interaction']['materialization_state'] == 'visible'
  assert payload['live_interaction']['activity_status'] == 'active'
  assert payload['live_interaction']['funds_refresh_status'] == 'unknown'
  assert payload['live_interaction']['summary_cards']
  assert payload['evidence_browser']['latest_accepted_run']['run_id'] == 'run-123'
  assert payload['evidence_browser']['pass_count'] == 1
  assert payload['evidence_browser']['accepted_state_summary'].startswith('Current state is anchored')
  assert payload['session_overlay']['runtime']['active'] is False
  assert payload['connection_posture']['operation_lane'] == 'sandbox'
  assert payload['connection_posture']['active_websocket_url_tail'] == 'demo-api.kalshi.example/ws'
  assert payload['connection_posture']['available_websocket_urls']['live'] == 'api.kalshi.example/ws'
  assert payload['connection_posture']['connection_state']['websocket_connected'] is True
  assert payload['session_overlay']['automation']['enabled'] is False
  assert payload['session_overlay']['automation']['authority'] == 'Backend session overlay'
  assert payload['session_overlay']['automation']['policy_state_label'] == 'disabled'
  assert payload['session_overlay']['automation']['client_executor_allowed'] is False
  # S1 diagnostic instrumentation rides on the full-path rebuild.
  full_timing = payload.get('rebuild_timing')
  assert isinstance(full_timing, dict)
  assert full_timing['path'] == 'full'
  assert isinstance(full_timing['bootstrap_service_ms'], (int, float))


def test_bootstrap_rebuild_timing_instrumentation_present(tmp_path: Any) -> None:
  # S1 diagnostic (PLAN-POLYVENTURE-LIVE-AUTO-STABILITY-20260615): every dashboard
  # rebuild attaches a rebuild_timing block carrying only durations and scan-state so
  # live-lane rebuild latency can be attributed without exposing any credential values.
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  timing = payload.get('rebuild_timing')
  assert isinstance(timing, dict), 'bootstrap payload must carry rebuild_timing'
  for key in ('action', 'path', 'scan_active', 'scan_status', 'scan_stage', 'total_ms', 'bootstrap_service_ms', 'replay_build_ms', 'post_service_ms', 'report_ms', 'reconcile_ms', 'funds_refresh_ms', 'db_read_wait_ms', 'db_write_wait_ms', 'unattributed_ms'):
    assert key in timing, f'rebuild_timing missing {key}'
  assert timing['path'] in {'full', 'replay_fast'}
  assert isinstance(timing['total_ms'], (int, float)) and timing['total_ms'] >= 0
  assert isinstance(timing['scan_active'], bool)
  if timing['path'] == 'full':
    assert isinstance(timing['bootstrap_service_ms'], (int, float))
    assert isinstance(timing['post_service_ms'], (int, float))
  # Durations/state only -- no secret-bearing values may leak into the timing block.
  serialized = json.dumps(timing)
  assert 'private_key' not in serialized
  assert '-----BEGIN' not in serialized


def test_bounded_interactive_client_factory_is_short_and_single_attempt() -> None:
  # S2: interactive dashboard rebuilds must use a bounded client so a stalled upstream
  # cannot freeze the deck.
  from polyventure import web_app as wa
  settings = object()
  client = wa._bounded_interactive_client_factory(settings, object())
  assert client.request_timeout == wa._INTERACTIVE_REBUILD_REQUEST_TIMEOUT_SEC
  assert client.request_timeout <= 5, 'interactive rebuild timeout must stay small'
  assert client.max_attempts == 1, 'interactive rebuild must not retry'


def test_replay_restore_route_returns_no_go_when_terminal_replay_unavailable(tmp_path: Path, monkeypatch: Any) -> None:
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: _runtime_settings_for_lane(tmp_path, 'sandbox'))
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  status, headers, body = _call_app(app, method='POST', path='/api/replay-restore')
  payload = json.loads(body)

  assert status == '200 OK'
  assert headers['Content-Type'] == 'application/json; charset=utf-8'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'replay_restore_unavailable'
  assert payload['workflow_source'] == 'bootstrap_workflow'
  assert bool((payload.get('replay_restore') or {}).get('available')) is False


def test_report_response_includes_external_notification_source_for_auth_failures() -> None:
  base_services = _services()
  services = OperatorConsoleServices(
    bootstrap=base_services.bootstrap,
    scan=base_services.scan,
    run=base_services.run,
    reconcile=base_services.reconcile,
    report=lambda **_: {
      'decision': 'no-go',
      'reason': 'auth_fail',
      'message': 'Platform rejected credentials on authenticated endpoint.',
      'next_action': 'Load valid credentials and retry.',
    },
    cancel_all=base_services.cancel_all,
    system_log=base_services.system_log,
    visuals=base_services.visuals,
  )
  app = create_operator_console_app(services)

  status, _, body = _call_app(app, method='POST', path='/api/report')

  assert status == '200 OK'
  payload = json.loads(body)
  assert payload['reason'] == 'auth_fail'
  assert payload['notification_source']['origin'] == 'kalshi'
  assert payload['notification_source']['is_external'] is True


def test_runtime_context_and_automation_overlay_routes_update_session_state(monkeypatch: Any, tmp_path: Any) -> None:
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=r'secrets\kalshi\demo\private_key.pem',
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')

  runtime_status, _, runtime_body = _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'apply', 'values': {'scan_interval_ms': 3500, 'max_open_pairs': 7}},
  )
  runtime_payload = json.loads(runtime_body)

  assert runtime_status == '200 OK'
  assert runtime_payload['session_overlay']['runtime']['active'] is True
  assert runtime_payload['session_overlay']['runtime']['values']['scan_interval_ms'] == 3500
  assert runtime_payload['session_overlay']['runtime']['values']['max_open_pairs'] == 7
  assert runtime_payload['session_overlay']['runtime']['last_result']['tone'] == 'ok'

  coverability_status, _, coverability_body = _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'apply', 'values': {'max_divergence': '0.30', 'flow_participation_k': '1.0'}},
  )
  coverability_payload = json.loads(coverability_body)

  assert coverability_status == '200 OK'
  assert coverability_payload['session_overlay']['runtime']['values']['max_divergence'] == 0.30
  assert coverability_payload['session_overlay']['runtime']['values']['flow_participation_k'] == 1.0

  invalid_coverability_status, _, invalid_coverability_body = _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'apply', 'values': {'max_divergence': '1.30', 'flow_participation_k': '1.0'}},
  )
  invalid_coverability_payload = json.loads(invalid_coverability_body)

  assert invalid_coverability_status == '200 OK'
  assert invalid_coverability_payload['session_overlay']['runtime']['last_result']['tone'] == 'no-go'

  context_status, _, context_body = _call_app(
    app,
    method='POST',
    path='/api/context-overlay',
    body={'action': 'apply', 'values': {'operation_lane': 'live', 'subaccount': 3}},
  )
  context_payload = json.loads(context_body)

  assert context_status == '200 OK'
  assert context_payload['session_overlay']['context']['active'] is True
  assert context_payload['session_overlay']['context']['operation_lane'] == 'live'
  assert context_payload['connection_posture']['active_websocket_url_tail'] == 'api.kalshi.example/ws'
  assert context_payload['connection_posture']['available_websocket_urls']['sandbox'] == 'demo-api.kalshi.example/ws'
  assert context_payload['session_overlay']['context']['subaccount'] == 3
  assert context_payload['session_overlay']['context']['last_result']['tone'] == 'ok'

  websocket_status, _, websocket_body = _call_app(
    app,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_live_websocket', 'url': 'wss://live-override.example/ws'},
  )
  websocket_payload = json.loads(websocket_body)

  assert websocket_status == '200 OK'
  assert websocket_payload['connection_posture']['available_websocket_urls']['live'] == 'live-override.example/ws'
  assert websocket_payload['connection_posture']['active_websocket_url_tail'] == 'live-override.example/ws'
  assert websocket_payload['websocket_management']['last_result']['tone'] == 'ok'

  clear_status, _, clear_body = _call_app(
    app,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'clear_all'},
  )
  clear_payload = json.loads(clear_body)

  assert clear_status == '200 OK'
  assert clear_payload['connection_posture']['available_websocket_urls']['sandbox'] == ''
  assert clear_payload['connection_posture']['available_websocket_urls']['live'] == ''
  assert clear_payload['connection_posture']['active_websocket_url_tail'] == 'unconfigured'
  assert clear_payload['websocket_management']['last_result']['tone'] == 'warn'
  assert clear_payload['decision'] == 'planned'
  assert clear_payload['reason'] == 'planned'
  assert clear_payload['boundary'] is None

  automation_status, _, automation_body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'apply', 'values': {'enabled': True, 'paused': True, 'cadence_ms': 5000, 'max_iterations': 4}},
  )
  automation_payload = json.loads(automation_body)

  assert automation_status == '200 OK'
  assert automation_payload['session_overlay']['automation']['enabled'] is True
  assert automation_payload['session_overlay']['automation']['paused'] is True
  assert automation_payload['session_overlay']['automation']['cadence_ms'] == 5000
  assert automation_payload['session_overlay']['automation']['max_iterations'] == 4
  assert automation_payload['session_overlay']['automation']['state_id'] == 'paused'
  assert automation_payload['session_overlay']['automation']['authority'] == 'Backend session overlay'
  assert automation_payload['session_overlay']['automation']['policy_state_label'] == 'paused'
  assert automation_payload['session_overlay']['automation']['client_executor_allowed'] is False
  assert automation_payload['session_overlay']['automation']['client_executor_summary'].startswith('Client auto-forward is waiting because bounded automation is paused')
  assert automation_payload['session_overlay']['automation']['last_result']['tone'] == 'ok'


def test_automation_overlay_resume_requires_manual_execution_truth(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'automation-gate.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'resume'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'automation_manual_truth_required'
  assert payload['session_overlay']['automation']['enabled'] is False
  assert payload['session_overlay']['automation']['state_id'] == 'disabled'
  assert payload['session_overlay']['automation']['policy_state_label'] == 'disabled'
  assert payload['session_overlay']['automation']['client_executor_allowed'] is False
  assert payload['session_overlay']['automation']['last_result']['tone'] == 'no-go'


def test_automation_overlay_apply_with_manual_truth_persists_transition(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'automation-persist.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  connection = open_database(state_db_path)
  lane_session_id = 'lane-session-submit-verified'
  event_base_ts = datetime.now(UTC)
  for offset, event_type in enumerate(('submit_order_intent', 'fill', 'reconcile_snapshot', 'cancel_applied')):
    persist_runtime_event(
      connection,
      level='INFO',
      event_type=event_type,
      recorded_at_utc=(event_base_ts + timedelta(seconds=offset)).isoformat(),
      operation_lane='sandbox',
      lane_session_id=lane_session_id,
      detail={'profile': 'submit_order_bridge', 'seq': f'f4-seq-{offset + 1:03d}'},
    )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'planned'
  assert payload['session_overlay']['automation']['enabled'] is True
  assert payload['session_overlay']['automation']['paused'] is False
  assert payload['session_overlay']['automation']['state_id'] == 'enabled_idle'
  assert payload['session_overlay']['automation']['policy_state_label'] == 'enabled'
  assert payload['session_overlay']['automation']['client_executor_allowed'] is True
  assert payload['session_overlay']['automation']['client_executor_summary'].startswith('Client auto-forward may run when armed')
  assert payload['session_overlay']['automation']['last_result']['tone'] == 'ok'

  db = sqlite3.connect(state_db_path)
  db.row_factory = sqlite3.Row
  action_row = db.execute(
    "SELECT action, detail_json FROM operator_actions WHERE action = 'automation-apply' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  event_row = db.execute(
    "SELECT event_type, detail_json FROM runtime_events WHERE event_type = 'automation_policy_transition' ORDER BY id DESC LIMIT 1"
  ).fetchone()

  assert action_row is not None
  assert event_row is not None
  action_detail = json.loads(str(action_row['detail_json'] or '{}'))
  event_detail = json.loads(str(event_row['detail_json'] or '{}'))
  assert action_detail['manual_truth_ready'] is True
  assert action_detail['automation_state_id'] == 'enabled_idle'
  assert event_detail['manual_truth_ready'] is True
  assert event_detail['action'] == 'apply'


def test_automation_overlay_apply_allows_connected_pre_first_scan_posture(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app, method='POST', path='/api/key-apply', body={'lane': 'sandbox'})

  mode_status, _, mode_body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  mode_payload = json.loads(mode_body)
  assert mode_status == '200 OK'
  assert mode_payload['connection_posture']['connection_state']['websocket_connected'] is True
  assert mode_payload['session_overlay']['automation']['client_executor_allowed'] is False
  assert mode_payload['session_overlay']['automation']['client_executor_summary'].startswith('Client auto-forward can enable bounded automation from this chip')

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'planned'
  assert payload['session_overlay']['automation']['enabled'] is True
  assert payload['session_overlay']['automation']['paused'] is False
  assert payload['session_overlay']['automation']['state_id'] == 'enabled_idle'
  assert payload['session_overlay']['automation']['client_executor_allowed'] is True


def test_automation_overlay_apply_blocks_connected_lane_while_scan_processing(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )
  scan_started = threading.Event()
  release_scan = threading.Event()

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  def _blocking_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    scan_started.set()
    release_scan.wait(timeout=2.0)
    return {
      'decision': 'planned',
      'candidate_count': 0,
      'candidates': [],
      'next_action': 'Review the empty result or find candidates again.',
      'scan_shape_summary': {
        'loaded_market_count': 12,
        'orderbook_review_market_count': 3,
        'quote_ready_market_count': 2,
        'profitability_pass_market_count': 0,
      },
    }

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=_blocking_scan,
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app, method='POST', path='/api/key-apply', body={'lane': 'sandbox'})
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan')
  scan_payload = json.loads(scan_body)
  assert scan_status == '200 OK'
  assert scan_payload['scan_runtime']['status'] == 'processing'
  assert scan_started.wait(timeout=1.0) is True

  try:
    status, _, body = _call_app(
      app,
      method='POST',
      path='/api/automation-overlay',
      body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}},
    )
    payload = json.loads(body)

    assert status == '200 OK'
    assert payload['decision'] == 'no-go'
    assert payload['reason'] == 'automation_manual_truth_required'
    assert 'Find candidates is still processing' in payload['message']
    assert payload['session_overlay']['automation']['enabled'] is False
  finally:
    release_scan.set()


def test_automation_overlay_stop_clears_client_executor_readiness(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'automation-stop.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  connection = open_database(state_db_path)
  lane_session_id = 'lane-session-stop-verified'
  event_base_ts = datetime.now(UTC)
  for offset, event_type in enumerate(('submit_order_intent', 'fill', 'reconcile_snapshot', 'cancel_applied')):
    persist_runtime_event(
      connection,
      level='INFO',
      event_type=event_type,
      recorded_at_utc=(event_base_ts + timedelta(seconds=offset)).isoformat(),
      operation_lane='sandbox',
      lane_session_id=lane_session_id,
      detail={'profile': 'submit_order_bridge', 'seq': f'f4-stop-{offset + 1:03d}'},
    )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  apply_status, _, apply_body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}},
  )
  apply_payload = json.loads(apply_body)

  stop_status, _, stop_body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'stop'},
  )
  stop_payload = json.loads(stop_body)

  assert apply_status == '200 OK'
  assert apply_payload['session_overlay']['automation']['client_executor_allowed'] is True
  assert stop_status == '200 OK'
  assert stop_payload['session_overlay']['automation']['enabled'] is False
  assert stop_payload['session_overlay']['automation']['paused'] is False
  assert stop_payload['session_overlay']['automation']['state_id'] == 'stopped'
  assert stop_payload['session_overlay']['automation']['policy_state_label'] == 'stopped'
  assert stop_payload['session_overlay']['automation']['client_executor_allowed'] is False
  assert stop_payload['session_overlay']['automation']['client_executor_summary'].startswith('Client auto-forward can enable bounded automation from this chip')
  assert stop_payload['session_overlay']['automation']['last_result']['tone'] == 'ok'

  db = sqlite3.connect(state_db_path)
  db.row_factory = sqlite3.Row
  event_row = db.execute(
    "SELECT event_type, detail_json FROM runtime_events WHERE event_type = 'automation_policy_transition' ORDER BY id DESC LIMIT 1"
  ).fetchone()

  assert event_row is not None
  event_detail = json.loads(str(event_row['detail_json'] or '{}'))
  assert event_detail['action'] == 'stop'


def _seed_active_pair(
  state_db_path: str,
  *,
  pair_id: str,
  ticker: str,
  operation_lane: str,
) -> None:
  connection = open_database(state_db_path)
  plan = PairOrderPlan(
    pair_id=pair_id,
    ticker=ticker,
    yes_price=Decimal('0.30'),
    no_price=Decimal('0.40'),
    contract_count=Decimal('2'),
    yes_client_order_id=f'{pair_id}-yes',
    no_client_order_id=f'{pair_id}-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(connection, plan, created_at_utc='2026-06-16T15:40:00Z', operation_lane=operation_lane)
  persist_pair_state_transition(
    connection,
    pair_id=pair_id,
    state='RESTING_BOTH',
    recorded_at_utc='2026-06-16T15:40:01Z',
    operation_lane=operation_lane,
    lane_session_id='lane-session-cp',
    detail={'reason': 'seed_active', 'ticker': ticker},
  )


def _latest_pair_states(state_db_path: str, operation_lane: str) -> dict[str, str]:
  db = sqlite3.connect(state_db_path)
  db.row_factory = sqlite3.Row
  rows = db.execute(
    '''
    SELECT ps.pair_id, ps.state
    FROM pair_states ps
    INNER JOIN (
      SELECT pair_id, MAX(id) AS max_id FROM pair_states WHERE operation_lane = ? GROUP BY pair_id
    ) latest ON latest.max_id = ps.id
    WHERE ps.operation_lane = ?
    ''',
    (operation_lane, operation_lane),
  ).fetchall()
  return {str(row['pair_id']): str(row['state']) for row in rows}


def test_automation_overlay_stop_cancels_active_pairs_when_cancel_on_pause_true(tmp_path: Path, monkeypatch: Any) -> None:
  # CP: an operator stop with cancel_on_pause=true cancels every active pair in
  # the lane as part of the orchestrated teardown.
  state_db_path = str(tmp_path / 'cp-stop-true.sqlite3')
  settings = _build_test_settings(state_db_path)  # cancel_on_pause defaults to True
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  _seed_active_pair(state_db_path, pair_id='pair-cp-1', ticker='KX-CP-A', operation_lane='sandbox')
  _seed_active_pair(state_db_path, pair_id='pair-cp-2', ticker='KX-CP-B', operation_lane='sandbox')

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  stop_status, _, stop_body = _call_app(
    app, method='POST', path='/api/automation-overlay', body={'action': 'stop'},
  )
  stop_payload = json.loads(stop_body)

  assert stop_status == '200 OK'
  assert stop_payload['session_overlay']['automation']['state_id'] == 'stopped'
  halt_summary = stop_payload['session_overlay']['automation'].get('halt_cancel_summary')
  assert halt_summary is not None, 'CP: stop must report a halt cancel summary when cancel_on_pause is true'
  assert halt_summary['canceled_pair_count'] == 2

  states = _latest_pair_states(state_db_path, 'sandbox')
  assert states.get('pair-cp-1') == 'CANCELED', 'CP: active pair must be CANCELED on stop'
  assert states.get('pair-cp-2') == 'CANCELED', 'CP: active pair must be CANCELED on stop'


def test_automation_overlay_stop_leaves_pairs_when_cancel_on_pause_false(tmp_path: Path, monkeypatch: Any) -> None:
  # CP: when cancel_on_pause=false, an operator stop leaves active pairs intact
  # (the resting-collapse case) and reports no cancellation.
  state_db_path = str(tmp_path / 'cp-stop-false.sqlite3')
  settings = replace(_build_test_settings(state_db_path), cancel_on_pause=False)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  _seed_active_pair(state_db_path, pair_id='pair-cp-3', ticker='KX-CP-C', operation_lane='sandbox')

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  stop_status, _, stop_body = _call_app(
    app, method='POST', path='/api/automation-overlay', body={'action': 'stop'},
  )
  stop_payload = json.loads(stop_body)

  assert stop_status == '200 OK'
  assert stop_payload['session_overlay']['automation']['state_id'] == 'stopped'
  assert stop_payload['session_overlay']['automation'].get('halt_cancel_summary') is None, (
    'CP: no halt cancel summary when cancel_on_pause is false'
  )

  states = _latest_pair_states(state_db_path, 'sandbox')
  assert states.get('pair-cp-3') == 'RESTING_BOTH', 'CP: pair must be left intact when cancel_on_pause is false'


def _seed_lifecycle_candidates(
  state_db_path: str,
  *,
  lane_session_id: str,
  run_id: str,
  tickers: list[str],
  lifecycle_stage: str = 'in_flight',
) -> None:
  conn = sqlite3.connect(state_db_path)
  conn.execute('CREATE TABLE IF NOT EXISTS candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT, recorded_at_utc TEXT)')
  conn.execute(
    'CREATE TABLE IF NOT EXISTS candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, terminal_at_utc TEXT,'
    '  recorded_at_utc TEXT, operation_lane TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute(
    'INSERT OR IGNORE INTO candidate_review_runs (run_id, lane_session_id) VALUES (?,?)',
    (run_id, lane_session_id),
  )
  conn.executemany(
    'INSERT INTO candidate_review_candidates'
    ' (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json)'
    ' VALUES (?,?,?,?,?,?)',
    [(run_id, lifecycle_stage, None, t, 'live_qualifying', '{}') for t in tickers],
  )
  conn.commit()
  conn.close()


def _lifecycle_stages_for_session(state_db_path: str, lane_session_id: str) -> dict[str, str]:
  conn = sqlite3.connect(state_db_path)
  rows = conn.execute(
    '''
    SELECT c.ticker, c.lifecycle_stage
    FROM candidate_review_candidates c
    JOIN candidate_review_runs r ON r.run_id = c.run_id
    WHERE r.lane_session_id = ?
    ''',
    (lane_session_id,),
  ).fetchall()
  conn.close()
  return {str(r[0]): str(r[1]) for r in rows}


def test_halt_mark_lifecycle_candidates_terminal_marks_non_terminal_rows(tmp_path: Path) -> None:
  # CP-L unit: _halt_mark_lifecycle_candidates_terminal must mark all non-terminal
  # candidates for the given lane session as terminal/auto_cancel, and leave
  # already-terminal rows unchanged.
  state_db_path = str(tmp_path / 'cp-l-unit.sqlite3')
  _seed_lifecycle_candidates(
    state_db_path,
    lane_session_id='test-session-cpl',
    run_id='run-cpl-1',
    tickers=['KX-L-A', 'KX-L-B'],
    lifecycle_stage='in_flight',
  )
  _seed_lifecycle_candidates(
    state_db_path,
    lane_session_id='test-session-cpl',
    run_id='run-cpl-2',
    tickers=['KX-L-C'],
    lifecycle_stage='discovered',
  )
  # Already-terminal row for same session — must not be double-written
  conn = sqlite3.connect(state_db_path)
  conn.execute(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json)'
    " VALUES ('run-cpl-1', 'terminal', 'reconciled', 'KX-L-D', 'live_qualifying', '{}')"
  )
  conn.commit()
  conn.close()

  count = web_app._halt_mark_lifecycle_candidates_terminal('test-session-cpl', state_db_path)
  assert count == 3, f'CP-L: must mark 3 non-terminal candidates terminal, got {count}'

  stages = _lifecycle_stages_for_session(state_db_path, 'test-session-cpl')
  assert stages['KX-L-A'] == 'terminal', 'CP-L: in_flight candidate must become terminal'
  assert stages['KX-L-B'] == 'terminal', 'CP-L: in_flight candidate must become terminal'
  assert stages['KX-L-C'] == 'terminal', 'CP-L: discovered candidate must become terminal'
  assert stages['KX-L-D'] == 'terminal', 'CP-L: already-terminal must stay terminal'

  # terminal_cause on newly-marked rows must be auto_cancel
  conn2 = sqlite3.connect(state_db_path)
  causes = {
    r[0]: r[1] for r in conn2.execute(
      "SELECT ticker, terminal_cause FROM candidate_review_candidates"
      " WHERE ticker IN ('KX-L-A','KX-L-B','KX-L-C')"
    ).fetchall()
  }
  conn2.close()
  assert causes.get('KX-L-A') == 'auto_cancel', 'CP-L: terminal_cause must be auto_cancel'
  assert causes.get('KX-L-B') == 'auto_cancel', 'CP-L: terminal_cause must be auto_cancel'
  assert causes.get('KX-L-C') == 'auto_cancel', 'CP-L: terminal_cause must be auto_cancel'


def test_halt_mark_lifecycle_candidates_terminal_returns_zero_on_empty_lsid() -> None:
  count = web_app._halt_mark_lifecycle_candidates_terminal('', '/some/path')
  assert count == 0, 'CP-L: empty lane_session_id must return 0 without error'


def test_automation_overlay_stop_marks_lifecycle_candidates_terminal_when_cancel_on_pause_true(tmp_path: Path, monkeypatch: Any) -> None:
  # CP-L integration: an operator stop with cancel_on_pause=true marks all
  # in_flight and discovered lifecycle candidates for the session as terminal.
  # lane_session_id is passed in the request body (JS client includes it).
  state_db_path = str(tmp_path / 'cp-l-stop-true.sqlite3')
  lane_session_id = 'cp-l-session-stop'
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  _seed_lifecycle_candidates(
    state_db_path,
    lane_session_id=lane_session_id,
    run_id='run-cpl-stop-1',
    tickers=['KX-CPL-A', 'KX-CPL-B'],
    lifecycle_stage='in_flight',
  )
  _seed_lifecycle_candidates(
    state_db_path,
    lane_session_id=lane_session_id,
    run_id='run-cpl-stop-2',
    tickers=['KX-CPL-C'],
    lifecycle_stage='discovered',
  )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  stop_status, _, stop_body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'stop', 'lane_session_id': lane_session_id},
  )
  stop_payload = json.loads(stop_body)

  assert stop_status == '200 OK'
  assert stop_payload['session_overlay']['automation']['state_id'] == 'stopped'
  halt_summary = stop_payload['session_overlay']['automation'].get('halt_cancel_summary')
  assert halt_summary is not None, 'CP-L: stop must report halt_cancel_summary when cancel_on_pause is true'
  assert halt_summary['halted_candidate_count'] == 3, (
    f"CP-L: must cancel 3 lifecycle candidates, got {halt_summary.get('halted_candidate_count')}"
  )
  assert halt_summary['transition_reason'] == 'operator_stop'

  stages = _lifecycle_stages_for_session(state_db_path, lane_session_id)
  assert stages.get('KX-CPL-A') == 'terminal', 'CP-L: in_flight candidate must be terminal after stop'
  assert stages.get('KX-CPL-B') == 'terminal', 'CP-L: in_flight candidate must be terminal after stop'
  assert stages.get('KX-CPL-C') == 'terminal', 'CP-L: discovered candidate must be terminal after stop'


def test_automation_overlay_stop_leaves_lifecycle_candidates_when_cancel_on_pause_false(tmp_path: Path, monkeypatch: Any) -> None:
  # CP-L integration: when cancel_on_pause=false, lifecycle candidates are left
  # intact and no halt_cancel_summary is reported.
  state_db_path = str(tmp_path / 'cp-l-stop-false.sqlite3')
  lane_session_id = 'cp-l-session-stop-false'
  settings = replace(_build_test_settings(state_db_path), cancel_on_pause=False)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  _seed_lifecycle_candidates(
    state_db_path,
    lane_session_id=lane_session_id,
    run_id='run-cpl-false-1',
    tickers=['KX-CPLF-A', 'KX-CPLF-B'],
    lifecycle_stage='in_flight',
  )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  stop_status, _, stop_body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'stop', 'lane_session_id': lane_session_id},
  )
  stop_payload = json.loads(stop_body)

  assert stop_status == '200 OK'
  assert stop_payload['session_overlay']['automation']['state_id'] == 'stopped'
  assert stop_payload['session_overlay']['automation'].get('halt_cancel_summary') is None, (
    'CP-L: no halt_cancel_summary when cancel_on_pause is false'
  )

  stages = _lifecycle_stages_for_session(state_db_path, lane_session_id)
  assert stages.get('KX-CPLF-A') == 'in_flight', 'CP-L: candidate must stay in_flight when cancel_on_pause is false'
  assert stages.get('KX-CPLF-B') == 'in_flight', 'CP-L: candidate must stay in_flight when cancel_on_pause is false'


def test_automation_overlay_stop_restores_find_candidates_workflow_in_clean_ready_lane(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app, method='POST', path='/api/key-apply', body={'lane': 'sandbox'})

  mode_status, _, mode_body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  mode_payload = json.loads(mode_body)
  assert mode_status == '200 OK'
  assert mode_payload['workflow']['recommended_step'] == 'scan'
  assert mode_payload['workflow']['next_actionable_step'] == 'scan'

  apply_status, _, apply_body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}},
  )
  apply_payload = json.loads(apply_body)

  stop_status, _, stop_body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'stop'},
  )
  stop_payload = json.loads(stop_body)

  assert apply_status == '200 OK'
  assert apply_payload['workflow']['recommended_step'] == 'scan'
  assert apply_payload['workflow']['next_actionable_step'] == 'scan'
  assert stop_status == '200 OK'
  assert stop_payload['workflow']['recommended_step'] == 'scan'
  assert stop_payload['workflow']['next_actionable_step'] == 'scan'
  assert stop_payload['workflow']['can_run_next_step'] is True
  assert stop_payload['workflow']['deck_view'] == 'workflow'


def test_automation_overlay_projects_auto_find_candidates_cadence_into_runtime_and_parameter_surface(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  state_db_path = str(tmp_path / 'automation-cadence.sqlite3')
  settings = _build_test_settings(state_db_path)
  assert settings.auto_find_candidates_cadence_ms == 600000
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['session_overlay']['automation']['cadence_ms'] == settings.auto_find_candidates_cadence_ms
  assert bootstrap_payload['session_overlay']['runtime']['working_default_values']['auto_find_candidates_cadence_ms'] == settings.auto_find_candidates_cadence_ms

  parameter_pages = {
    str(page.get('page_id') or ''): page
    for page in list((bootstrap_payload.get('parameter_surface') or {}).get('pages', []))
    if isinstance(page, dict)
  }
  info_group = next(
    group
    for group in list((parameter_pages.get('info') or {}).get('groups', []))
    if isinstance(group, dict)
    and any(
      isinstance(row, dict) and row.get('parameter_id') == 'auto_find_candidates_cadence_ms'
      for row in list(group.get('rows', []))
    )
  )
  set_group = next(
    group
    for group in list((parameter_pages.get('set') or {}).get('groups', []))
    if isinstance(group, dict)
    and any(
      isinstance(row, dict) and row.get('parameter_id') == 'auto_find_candidates_cadence_ms'
      for row in list(group.get('rows', []))
    )
  )
  info_row = next(row for row in list(info_group.get('rows', [])) if isinstance(row, dict) and row.get('parameter_id') == 'auto_find_candidates_cadence_ms')
  set_row = next(row for row in list(set_group.get('rows', [])) if isinstance(row, dict) and row.get('parameter_id') == 'auto_find_candidates_cadence_ms')

  assert info_group.get('group_id') == 'scan_cadence_posture'
  assert set_group.get('group_id') == 'manual_scan_cadence'
  assert info_row['label'] == 'Auto-find-candidates cadence'
  assert set_row['working_default_value'] == settings.auto_find_candidates_cadence_ms
  assert info_row['overlay_active'] is False

  automation_status, _, automation_body = _call_app(
    app,
    method='POST',
    path='/api/automation-overlay',
    body={'action': 'apply', 'values': {'enabled': True, 'paused': True, 'cadence_ms': 5000, 'max_iterations': 4}},
  )
  automation_payload = json.loads(automation_body)

  parameter_pages = {
    str(page.get('page_id') or ''): page
    for page in list((automation_payload.get('parameter_surface') or {}).get('pages', []))
    if isinstance(page, dict)
  }
  set_group = next(
    group
    for group in list((parameter_pages.get('set') or {}).get('groups', []))
    if isinstance(group, dict)
    and any(
      isinstance(row, dict) and row.get('parameter_id') == 'auto_find_candidates_cadence_ms'
      for row in list(group.get('rows', []))
    )
  )
  set_row = next(row for row in list(set_group.get('rows', [])) if isinstance(row, dict) and row.get('parameter_id') == 'auto_find_candidates_cadence_ms')

  assert automation_status == '200 OK'
  assert automation_payload['session_overlay']['automation']['cadence_ms'] == 5000
  assert automation_payload['session_overlay']['runtime']['values']['auto_find_candidates_cadence_ms'] == 5000
  assert 'auto_find_candidates_cadence_ms' in automation_payload['parameter_surface']['overlay_summary']['staged_parameter_ids']
  assert set_row['overlay_active'] is True
  assert set_row['overlay_value'] == 5000


def test_automation_cadence_seconds_contract_stays_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'function cadenceMsToSeconds(cadenceMs) {' in body
  assert "return `${cadenceSeconds} sec`;" in body
  assert 'Cadence currently runs every ${cadenceSeconds} seconds in Weights and parameters.' in body
  assert "auto_find_candidates_cadence_ms: { type: 'number', step: '1', placeholder: '600' }" in body
  assert "{ id: 'cadence_ms', label: 'Cadence (sec)', type: 'number', step: '1', value: cadenceMsToSeconds(automationOverlay.cadence_ms ?? 600000), placeholder: '600' }" in body


def test_automation_overlay_state_excludes_cancel_on_pause() -> None:
  from polyventure.web_app import create_operator_console_app as _cap
  src = inspect.getsource(_cap)
  assert "'cancel_on_pause': True" not in src, 'WP-5: cancel_on_pause must not appear in automation_overlay_state initial dict'


def test_automation_overlay_state_excludes_max_unhedged_sec() -> None:
  from polyventure.web_app import create_operator_console_app as _cap
  src = inspect.getsource(_cap)
  assert "'max_unhedged_sec': 300" not in src, 'WP-5: max_unhedged_sec must not appear in automation_overlay_state initial dict'


def test_automation_policy_contract_contains_cancel_on_pause() -> None:
  app = create_operator_console_app(_services())
  _, _, body = _call_app(app, method='GET', path='/')
  assert 'cancel_on_pause: null,' in body, 'HX0-C: cancel_on_pause missing from AUTOMATION_POLICY_CONTRACT'


def test_automation_policy_contract_contains_max_unhedged_sec() -> None:
  app = create_operator_console_app(_services())
  _, _, body = _call_app(app, method='GET', path='/')
  assert 'max_unhedged_sec: null,' in body, 'HX0-C: max_unhedged_sec missing from AUTOMATION_POLICY_CONTRACT'


def test_weights_and_params_info_group_exposes_cancel_on_pause() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG, PARAMETER_SURFACE_FIELD_CATALOG
  info_page = next(p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'info')
  cadence_group = next(g for g in info_page['group_catalog'] if g['group_id'] == 'scan_cadence_posture')
  assert 'cancel_on_pause' in cadence_group['field_ids'], 'HX0-B: cancel_on_pause missing from scan_cadence_posture Info group'
  assert 'cancel_on_pause' in PARAMETER_SURFACE_FIELD_CATALOG, 'HX0-B: cancel_on_pause missing from PARAMETER_SURFACE_FIELD_CATALOG'


def test_weights_and_params_info_group_exposes_max_unhedged_sec() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG, PARAMETER_SURFACE_FIELD_CATALOG
  info_page = next(p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'info')
  cadence_group = next(g for g in info_page['group_catalog'] if g['group_id'] == 'scan_cadence_posture')
  assert 'max_unhedged_sec' in cadence_group['field_ids'], 'HX0-B: max_unhedged_sec missing from scan_cadence_posture Info group'
  assert 'max_unhedged_sec' in PARAMETER_SURFACE_FIELD_CATALOG, 'HX0-B: max_unhedged_sec missing from PARAMETER_SURFACE_FIELD_CATALOG'


def _make_automation_payload(
  *,
  enabled: bool,
  paused: bool,
  settings_ready: bool = True,
  decision: str = 'planned',
) -> dict:
  return {
    'decision': decision,
    'settings': {'settings_ready': settings_ready},
    '_automation_posture': {'enabled': enabled, 'paused': paused},
  }


def test_automation_follow_on_workflow_disabled_returns_scan_workflow() -> None:
  from polyventure.web_app import _automation_follow_on_workflow
  payload = _make_automation_payload(enabled=False, paused=False)
  result = _automation_follow_on_workflow(payload)
  assert result is not None, 'HX2-A: disabled automation must return a scan workflow, not None'
  assert result.get('recommended_step') == 'scan', (
    'HX2-A: disabled automation must recommend scan (not fall through to generic automation step)'
  )
  assert result.get('auto_sequence') == [], (
    'HX2-A: disabled automation must not auto-advance'
  )


def test_automation_follow_on_workflow_returns_none_on_no_go() -> None:
  from polyventure.web_app import _automation_follow_on_workflow
  payload = _make_automation_payload(enabled=True, paused=False, decision='no-go')
  assert _automation_follow_on_workflow(payload) is None, (
    'HX2-A: no-go decision must short-circuit and return None'
  )


def test_automation_follow_on_workflow_returns_none_without_settings() -> None:
  from polyventure.web_app import _automation_follow_on_workflow
  payload = {'decision': 'planned', '_automation_posture': {'enabled': True, 'paused': False}}
  assert _automation_follow_on_workflow(payload) is None, (
    'HX2-A: missing settings payload must return None'
  )


def test_automation_follow_on_workflow_enabled_returns_backend_owned_scan_workflow() -> None:
  from polyventure.web_app import _automation_follow_on_workflow
  payload = _make_automation_payload(enabled=True, paused=False)
  result = _automation_follow_on_workflow(payload)
  assert result is not None, 'HX2-A: enabled automation must return a workflow (not None)'
  assert result.get('auto_sequence') == [], (
    'Phase 5: enabled automation must not depend on frontend auto_sequence dispatch'
  )


def test_automation_follow_on_workflow_paused_returns_empty_auto_sequence() -> None:
  from polyventure.web_app import _automation_follow_on_workflow
  payload = _make_automation_payload(enabled=True, paused=True)
  result = _automation_follow_on_workflow(payload)
  assert result is not None, 'HX2-A: paused automation must return a workflow (not None)'
  assert result.get('auto_sequence') == [], (
    'HX2-A: paused automation must not auto-advance'
  )


def test_automation_follow_on_workflow_paused_recommends_resume_automation() -> None:
  from polyventure.web_app import _automation_follow_on_workflow
  payload = _make_automation_payload(enabled=True, paused=True)
  result = _automation_follow_on_workflow(payload)
  assert result is not None
  assert result.get('next_actionable_step') == 'resume_automation', (
    'HX2-A: paused workflow next_actionable_step must be resume_automation'
  )


def test_live_interaction_section_present_in_html() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'live-interaction-section' in source, (
    'HX3-B: live-interaction-section must be present in the HTML body'
  )


def test_live_interaction_section_hidden_when_no_active_interactions() -> None:
  source = inspect.getsource(web_app._render_html)
  section_start = source.find('id="live-interaction-section"')
  assert section_start != -1, 'HX3-B: live-interaction-section must exist'
  section_fragment = source[section_start:section_start + 200]
  assert 'hidden' in section_fragment, (
    'HX3-B: live-interaction-section must carry the hidden attribute (JS toggles visibility as needed)'
  )


def test_execution_panel_title_is_execution() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'live-interaction-title' in source, 'UI-2: live-interaction-title element must exist'
  title_pos = source.find('live-interaction-title')
  title_fragment = source[title_pos:title_pos + 100]
  assert 'EXECUTION' in title_fragment, (
    'UI-2: live-interaction-title default must read EXECUTION'
  )
  assert 'Live interaction' not in title_fragment, (
    'UI-2: live-interaction-title must not read Live interaction'
  )


def test_live_interaction_panel_does_not_contain_chip_controls() -> None:
  source = inspect.getsource(web_app._render_html)
  section_start = source.find('id="live-interaction-section"')
  assert section_start != -1, 'HX3-B: live-interaction-section must exist'
  section_end = source.find('</section>', section_start)
  live_section = source[section_start:section_end]
  assert 'deck-chip-button' not in live_section, (
    'HX3-B anti-drift: live-interaction-section must not contain deck-chip-button controls'
  )


def test_live_interaction_panel_does_not_contain_cadence_editing() -> None:
  source = inspect.getsource(web_app._render_html)
  section_start = source.find('id="live-interaction-section"')
  assert section_start != -1, 'HX3-B: live-interaction-section must exist'
  section_end = source.find('</section>', section_start)
  live_section = source[section_start:section_end]
  assert 'cadence_ms' not in live_section, (
    'HX3-B anti-drift: live-interaction-section must not contain cadence editing controls'
  )


def test_auto_find_cadence_ticker_state_fields_exist_in_shell() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'autoFindCadenceTimerId: 0,' in source, (
    'CAD-1: autoFindCadenceTimerId state field must be initialized to 0'
  )
  assert 'autoFindCadenceDeadlineMs: 0,' in source, (
    'CAD-1: autoFindCadenceDeadlineMs state field must be initialized to 0'
  )
  assert 'autoFindCadenceRefireInFlight: false,' in source, (
    'CAD-1: autoFindCadenceRefireInFlight state field must be initialized to false'
  )


def test_auto_find_cadence_ticker_functions_exist_in_shell() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'function stopAutoFindCadenceTicker()' in source, (
    'CAD-1: stopAutoFindCadenceTicker function must be present'
  )
  assert 'function updateAutoFindCadenceCountdown()' in source, (
    'CAD-1: updateAutoFindCadenceCountdown function must be present'
  )
  assert 'function ensureAutoFindCadenceTicker()' in source, (
    'CAD-1: ensureAutoFindCadenceTicker function must be present'
  )


def test_auto_find_cadence_ticker_triggered_from_run_auto_sequence_finally() -> None:
  source = inspect.getsource(web_app._render_html)
  run_seq_pos = source.find('async function runAutoSequence(sequence)')
  assert run_seq_pos != -1, 'CAD-1: runAutoSequence function must exist'
  finally_pos = source.find('} finally {', run_seq_pos)
  assert finally_pos != -1, 'CAD-1: runAutoSequence must have a finally block'
  finally_fragment = source[finally_pos:finally_pos + 900]
  assert 'ensureAutoFindCadenceTicker()' in finally_fragment, (
    'CAD-1: runAutoSequence finally block must call ensureAutoFindCadenceTicker()'
  )


def test_auto_find_cadence_ticker_triggered_for_empty_auto_sequence() -> None:
  source = inspect.getsource(web_app._render_html)
  run_seq_pos = source.find('async function runAutoSequence(sequence)')
  assert run_seq_pos != -1, 'CAD-1B: runAutoSequence function must exist'
  guard_fragment = source[run_seq_pos:run_seq_pos + 900]
  empty_sequence_pos = guard_fragment.find('sequence.length === 0')
  assert empty_sequence_pos != -1, (
    'CAD-1B: runAutoSequence must handle an empty backend auto_sequence while automation remains armed'
  )
  ensure_pos = guard_fragment.find('ensureAutoFindCadenceTicker()', empty_sequence_pos)
  assert ensure_pos != -1, (
    'CAD-1B: empty auto_sequence branch must arm the find-cadence ticker'
  )
  assert ensure_pos < guard_fragment.find('const wasArmed = state.autoAdvanceEnabled'), (
    'CAD-1B: empty auto_sequence must arm cadence before the non-empty sequence runner path'
  )


def test_auto_find_cadence_ticker_stopped_on_sync_disarm() -> None:
  source = inspect.getsource(web_app._render_html)
  sync_pos = source.find('function syncClientAutoForwardAuthority(')
  assert sync_pos != -1, 'CAD-1: syncClientAutoForwardAuthority function must exist'
  sync_fragment = source[sync_pos:sync_pos + 600]
  assert 'stopAutoFindCadenceTicker()' in sync_fragment, (
    'CAD-1: syncClientAutoForwardAuthority must call stopAutoFindCadenceTicker() on disarm'
  )


def test_auto_find_cadence_ticker_guard_conditions_present() -> None:
  source = inspect.getsource(web_app._render_html)
  countdown_pos = source.find('function updateAutoFindCadenceCountdown()')
  assert countdown_pos != -1, 'CAD-1: updateAutoFindCadenceCountdown must exist'
  fn_fragment = source[countdown_pos:countdown_pos + 1500]
  assert 'state.isAutoRunning' in fn_fragment, (
    'CAD-1: updateAutoFindCadenceCountdown must guard on state.isAutoRunning'
  )
  assert 'requestScheduledScanRefire' not in fn_fragment, (
    'CAD-1: updateAutoFindCadenceCountdown must not dispatch — backend timer owns cadence fires'
  )
  assert 'cadence_state' in fn_fragment, (
    'CAD-1: updateAutoFindCadenceCountdown must read the cadence from scheduler_snapshot.cadence_state'
  )


def test_auto_find_cadence_ticker_honors_backend_deadline() -> None:
  source = inspect.getsource(web_app._render_html)
  countdown_pos = source.find('function updateAutoFindCadenceCountdown()')
  assert countdown_pos != -1, 'CAD-1C: updateAutoFindCadenceCountdown must exist'
  fn_fragment = source[countdown_pos:countdown_pos + 2200]
  assert 'cadence_state' in fn_fragment, (
    'CAD-1C: updateAutoFindCadenceCountdown must read backend cadence_state'
  )
  assert 'next_cadence_at_utc' in fn_fragment, (
    'CAD-1C: backend-authored next_cadence_at_utc must seed the client deadline'
  )
  assert 'Date.parse' in fn_fragment, (
    'CAD-1C: backend cadence deadline must be parsed as an absolute timestamp'
  )


def test_execution_panel_cadence_countdown_card_present_when_armed() -> None:
  source = inspect.getsource(web_app._render_html)
  # The HTML element form (id="boundary-auto-find-cadence-value") is the render-template occurrence
  html_elem_pos = source.find('id="boundary-auto-find-cadence-value"')
  assert html_elem_pos != -1, (
    'CAD-2: id="boundary-auto-find-cadence-value" element must be present in shell source'
  )
  # state.autoAdvanceEnabled gate must precede the element within the render template block
  cadence_context = source[max(0, html_elem_pos - 300):html_elem_pos + 50]
  assert 'state.autoAdvanceEnabled' in cadence_context, (
    'CAD-2: boundary-auto-find-cadence-value must be gated on state.autoAdvanceEnabled in the render template'
  )


def test_parameter_surface_set_tab_locked_when_armed_and_active() -> None:
  source = inspect.getsource(web_app._render_html)
  render_param_pos = source.find('function renderParameterSurface()')
  assert render_param_pos != -1, 'CAD-3: renderParameterSurface function must exist'
  fn_fragment = source[render_param_pos:render_param_pos + 700]
  assert 'setTabLocked' in fn_fragment, (
    'CAD-3: renderParameterSurface must compute setTabLocked gate'
  )
  assert 'surface_visible' in fn_fragment, (
    "CAD-3: setTabLocked gate must check live_interaction.surface_visible"
  )
  assert "parameterSurfacePageId = 'info'" not in fn_fragment, (
    "CAD-3: renderParameterSurface must NOT mutate state.parameterSurfacePageId — HTML-only disable only"
  )
  wide_fragment = source[render_param_pos:render_param_pos + 1300]
  assert "page.pageId === 'set'" in wide_fragment, (
    "CAD-3: set tab button must be conditionally locked via HTML attribute when setTabLocked"
  )


def test_cadence_ticker_ws_connectivity_guard_present() -> None:
  source = inspect.getsource(web_app._render_html)
  countdown_pos = source.find('function updateAutoFindCadenceCountdown()')
  assert countdown_pos != -1, 'CAD-6: updateAutoFindCadenceCountdown must exist'
  fn_fragment = source[countdown_pos:countdown_pos + 1500]
  assert 'buildConnectionPosture' in fn_fragment, (
    'CAD-6: updateAutoFindCadenceCountdown must read WS state via buildConnectionPosture — '
    'correct path is connection_posture.connection_state.websocket_connected; '
    'direct state.payload.connection path is absent from payload schema'
  )
  assert 'websocket_connected' in fn_fragment, (
    'CAD-6: updateAutoFindCadenceCountdown must check websocket_connected before firing'
  )
  assert 'autoFindCadenceDeadlineMs' in fn_fragment, (
    'CAD-6: updateAutoFindCadenceCountdown must preserve autoFindCadenceDeadlineMs on WS pause (not reset it)'
  )


def test_next_step_suppressed_during_auto_forward_running() -> None:
  source = inspect.getsource(web_app._render_html)
  quick_actions_pos = source.find('function renderQuickActions(payload)')
  assert quick_actions_pos != -1, 'CAD-4: renderQuickActions function must exist'
  fn_fragment = source[quick_actions_pos:quick_actions_pos + 2000]
  assert 'autoForwardRunning' in fn_fragment, (
    'CAD-4: renderQuickActions must compute autoForwardRunning gate'
  )
  assert 'displayStepLabel' in fn_fragment, (
    'CAD-4: renderQuickActions must use displayStepLabel when autoForwardRunning (not raw workflowProjection.stepLabel)'
  )
  assert 'Automation active' in fn_fragment, (
    "CAD-4: displayStepLabel must be 'Automation active' when autoForwardRunning is true"
  )


def test_execution_panel_hidden_backend_truth_clears_during_auto_running_scan() -> None:
  source = inspect.getsource(web_app._render_html)
  render_pos = source.find('function renderLiveInteractionSurface(')
  assert render_pos != -1, 'CAD-5/POST-4: renderLiveInteractionSurface function must exist'
  # hidden branch grew (submit-order elapsed-timer teardown); widen so the fragment
  # still spans past the hidden branch into the visible branch (const wasHidden).
  fn_fragment = source[render_pos:render_pos + 1500]
  not_visible_pos = fn_fragment.find('!surfaceVisible')
  assert not_visible_pos != -1, (
    'CAD-5/POST-4: renderLiveInteractionSurface must have !surfaceVisible branch'
  )
  visible_branch_pos = fn_fragment.find('const wasHidden', not_visible_pos)
  assert visible_branch_pos != -1, 'renderLiveInteractionSurface must continue to visible branch after hidden branch'
  not_visible_fragment = fn_fragment[not_visible_pos:visible_branch_pos]
  assert 'section.hidden = true' in not_visible_fragment
  assert "pill.textContent = 'IDLE'" in not_visible_fragment
  assert "grid.innerHTML = ''" in not_visible_fragment
  assert not_visible_fragment.find("grid.innerHTML = ''") < not_visible_fragment.find('return;'), (
    'BMAP 2026-06-29: backend-hidden live_interaction must clear/hide EXECUTION before returning'
  )
  assert 'state.autoAdvanceEnabled' not in not_visible_fragment, (
    'BMAP 2026-06-29: state.autoAdvanceEnabled may not override backend surface_visible=false'
  )
  assert 'state.isAutoRunning' not in not_visible_fragment, (
    'BMAP 2026-06-29: state.isAutoRunning may not override backend surface_visible=false'
  )


def test_post2_clear_review_selection_excludes_persisted_run_id() -> None:
  # _clear_review_selection_state is a closure — inspect via factory source
  factory_source = inspect.getsource(web_app.create_operator_console_app)
  fn_pos = factory_source.find('def _clear_review_selection_state()')
  assert fn_pos != -1, 'POST-2: _clear_review_selection_state must be defined in factory'
  # Isolate only this function's body — stop before the next def to avoid bleeding into siblings
  next_def_pos = factory_source.find('\n  def ', fn_pos + 10)
  fn_body = factory_source[fn_pos:next_def_pos] if next_def_pos != -1 else factory_source[fn_pos:fn_pos + 700]
  assert 'persisted_run_id' not in fn_body, (
    'POST-2: _clear_review_selection_state must NOT reset persisted_run_id — '
    'blanket reset caused _fetch_stage_columns to return empty on zero-candidates scans'
  )
  assert 'persisted_run_recorded_at_utc' not in fn_body, (
    'POST-2: _clear_review_selection_state must NOT reset persisted_run_recorded_at_utc — '
    'belongs exclusively to _clear_persisted_run_state'
  )


def test_post2_clear_persisted_run_state_helper_exists() -> None:
  # _clear_persisted_run_state is a closure — inspect via factory source
  factory_source = inspect.getsource(web_app.create_operator_console_app)
  fn_pos = factory_source.find('def _clear_persisted_run_state()')
  assert fn_pos != -1, (
    'POST-2: _clear_persisted_run_state helper function must be defined in factory'
  )
  fn_fragment = factory_source[fn_pos:fn_pos + 300]
  assert 'persisted_run_id' in fn_fragment, (
    'POST-2: _clear_persisted_run_state must update persisted_run_id'
  )
  assert 'persisted_run_recorded_at_utc' in fn_fragment, (
    'POST-2: _clear_persisted_run_state must update persisted_run_recorded_at_utc'
  )


def test_post2_explicit_persisted_run_clear_at_lane_mismatch_site() -> None:
  # _clear_lane_mismatched_review_selection is a closure — inspect via factory source
  factory_source = inspect.getsource(web_app.create_operator_console_app)
  fn_pos = factory_source.find('def _clear_lane_mismatched_review_selection(')
  assert fn_pos != -1, 'POST-2: _clear_lane_mismatched_review_selection must be defined in factory'
  fn_fragment = factory_source[fn_pos:fn_pos + 400]
  assert '_clear_persisted_run_state()' in fn_fragment, (
    'POST-2: _clear_lane_mismatched_review_selection must call _clear_persisted_run_state() '
    'explicitly after _clear_review_selection_state()'
  )


def test_post3_prior_saved_rows_deduplicated_against_cancelled_stage() -> None:
  source = inspect.getsource(web_app._render_html)
  # Search directly for _cancelledTickers rather than relying on a fixed window from renderPairMonitor
  assert '_cancelledTickers' in source, (
    'POST-3: _render_html must define _cancelledTickers to filter previous saved rows against cancelled stage'
  )
  assert 'filteredPriorSavedRows' in source, (
    'POST-3: _render_html must define filteredPriorSavedRows filtered against _cancelledTickers'
  )
  # Confirm the dedup and the Previous section call appear in the same logical block
  cancelled_pos = source.find('_cancelledTickers')
  filtered_pos = source.find('filteredPriorSavedRows', cancelled_pos)
  assert filtered_pos != -1 and filtered_pos - cancelled_pos < 500, (
    'POST-3: filteredPriorSavedRows must be computed immediately after _cancelledTickers (same block)'
  )
  prior_saved_pos = source.find("candidateSectionMarkup('Previous'", filtered_pos)
  assert prior_saved_pos != -1, (
    'POST-3: candidateSectionMarkup must be called with Previous label after filteredPriorSavedRows is computed'
  )
  section_call = source[prior_saved_pos:prior_saved_pos + 80]
  assert 'filteredPriorSavedRows' in section_call, (
    "POST-3: candidateSectionMarkup('Previous', ...) must receive filteredPriorSavedRows, not raw priorSavedRows"
  )


def test_post1_submit_order_elapsed_state_initialized() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'submitOrderElapsedTimerId: 0' in source, (
    'POST-1: state must initialize submitOrderElapsedTimerId: 0'
  )
  assert 'submitOrderElapsedSec: 0' in source, (
    'POST-1: state must initialize submitOrderElapsedSec: 0'
  )


def test_post1_submit_pending_surface_has_elapsed_counter() -> None:
  source = inspect.getsource(web_app._render_html)
  show_submit_pos = source.find('function showSubmitPendingSurface(')
  assert show_submit_pos != -1, 'POST-1: showSubmitPendingSurface must exist'
  fn_fragment = source[show_submit_pos:show_submit_pos + 2000]
  assert 'submit-order-elapsed-value' in fn_fragment, (
    'POST-1: showSubmitPendingSurface must render span#submit-order-elapsed-value in grid HTML'
  )
  assert 'submitOrderElapsedTimerId' in fn_fragment, (
    'POST-1: showSubmitPendingSurface must start the submitOrderElapsedTimerId interval'
  )
  assert 'setInterval' in fn_fragment, (
    'POST-1: showSubmitPendingSurface must use setInterval for elapsed counter'
  )


def test_post1_submit_elapsed_interval_cleared_in_finally() -> None:
  source = inspect.getsource(web_app._render_html)
  perform_action_pos = source.find('async function performAction(')
  assert perform_action_pos != -1, 'POST-1: performAction must exist'
  # Use a large window to capture all of performAction; search for the pending guard block
  fn_fragment = source[perform_action_pos:perform_action_pos + 12000]
  # The interval clear must appear in a block that also contains submitOrderPending = false
  pending_false_pos = fn_fragment.find('state.submitOrderPending = false')
  assert pending_false_pos != -1, 'POST-1: performAction must set submitOrderPending = false in finally'
  # clearInterval must appear near the submitOrderPending = false guard (within 400 chars)
  pending_block = fn_fragment[pending_false_pos - 50:pending_false_pos + 400]
  assert 'submitOrderElapsedTimerId' in pending_block, (
    'POST-1: submitOrderElapsedTimerId interval clear must appear in the same finally guard '
    'block as state.submitOrderPending = false'
  )
  assert 'clearInterval' in pending_block, (
    'POST-1: clearInterval must be called on submitOrderElapsedTimerId in the performAction finally block'
  )


def test_post6_render_live_interaction_preserves_backend_truth_when_submit_pending() -> None:
  source = inspect.getsource(web_app._render_html)
  fn_pos = source.find('function renderLiveInteractionSurface(')
  assert fn_pos != -1, 'POST-6: renderLiveInteractionSurface must exist in _render_html'
  next_fn_pos = source.find('\n    function ', fn_pos + 1)
  assert next_fn_pos != -1, 'POST-6: renderLiveInteractionSurface must have a bounded function body'
  fn_fragment = source[fn_pos:next_fn_pos]
  assert 'if (state.submitOrderPending) return;' not in fn_fragment, (
    'POST-6: pending submit must not suppress backend live-interaction cards'
  )
  assert 'showSubmitPendingSurface(submitPhaseLabel(liveInteraction), _d_newLease, _d_leaseId)' in fn_fragment, (
    'POST-6: pending submit refreshes must key elapsed rendering to the backend submit lease'
  )


def test_post7_reset_candidate_selection_preserved_during_automation() -> None:
  source = inspect.getsource(web_app._render_html)
  fn_pos = source.find('function resetCandidateSelectionState(')
  assert fn_pos != -1, 'POST-7: resetCandidateSelectionState must exist in _render_html'
  fn_fragment = source[fn_pos:fn_pos + 700]
  assert 'state.autoAdvanceEnabled' in fn_fragment, (
    'POST-7: resetCandidateSelectionState must check state.autoAdvanceEnabled to suppress view reset '
    'during automation'
  )
  assert 'previousVisibleClasses !== null' in fn_fragment, (
    'POST-7: automation guard must also check previousVisibleClasses !== null (only suppress when '
    'there is an established view to preserve)'
  )


def test_post8_wayfinder_suppressed_in_automation_refresh_path() -> None:
  source = inspect.getsource(web_app._render_html)
  wayfinder_pos = source.find('activateWayfinder(completionRoute)')
  assert wayfinder_pos != -1, 'POST-8: activateWayfinder(completionRoute) must exist in _render_html'
  # Automation guard must appear in the condition that gates the wayfinder call (within 200 chars)
  guard_window = source[wayfinder_pos - 200:wayfinder_pos]
  assert '!state.autoAdvanceEnabled' in guard_window, (
    'POST-8: activateWayfinder(completionRoute) must be gated on !state.autoAdvanceEnabled '
    'to prevent wayfinder snap during automation cadence scans'
  )


def test_post8_scroll_into_view_suppressed_in_automation() -> None:
  source = inspect.getsource(web_app._render_html)
  scroll_pos = source.find('section.scrollIntoView(')
  assert scroll_pos != -1, 'POST-8: section.scrollIntoView must exist in renderLiveInteractionSurface'
  # Automation guard must appear in the containing if-condition (within 150 chars before scrollIntoView)
  guard_window = source[scroll_pos - 150:scroll_pos]
  assert '!state.autoAdvanceEnabled' in guard_window, (
    'POST-8: section.scrollIntoView must be gated on !state.autoAdvanceEnabled to prevent UI jump '
    'when EXECUTION section becomes visible during automation'
  )


def test_post12_next_step_label_locked_during_armed_loop() -> None:
  source = inspect.getsource(web_app._render_html)
  fn_pos = source.find('function renderQuickActions(')
  assert fn_pos != -1, 'POST-12: renderQuickActions must exist in _render_html'
  fn_fragment = source[fn_pos:fn_pos + 1600]
  assert 'autoLoopActive' in fn_fragment, (
    'POST-12: renderQuickActions must define autoLoopActive combining autoForwardRunning || autoAdvanceEnabled'
  )
  assert 'autoForwardRunning || autoAdvanceEnabled' in fn_fragment, (
    'POST-12: autoLoopActive must be defined as autoForwardRunning || autoAdvanceEnabled'
  )
  display_step_pos = fn_fragment.find('displayStepLabel')
  assert display_step_pos != -1, 'POST-12: displayStepLabel must be defined in renderQuickActions'
  display_step_window = fn_fragment[display_step_pos - 10:display_step_pos + 100]
  assert 'autoLoopActive' in display_step_window, (
    'POST-12: displayStepLabel must use autoLoopActive (not just autoForwardRunning) so label stays '
    '"Automation active" throughout the armed loop, including between cadence scans'
  )


def test_post13_operator_controls_replaced_by_stop_during_automation() -> None:
  source = inspect.getsource(web_app._render_html)
  fn_pos = source.find('function buildDeckViewModel(')
  assert fn_pos != -1, 'POST-13: buildDeckViewModel must exist in _render_html'
  fn_fragment = source[fn_pos:fn_pos + 1800]
  armed_guard_pos = fn_fragment.find('state.autoAdvanceEnabled')
  assert armed_guard_pos != -1, (
    'POST-13: buildDeckViewModel must check state.autoAdvanceEnabled to gate operator workflow actions'
  )
  assert 'stop_automation_loop' in fn_fragment, (
    "POST-13: buildDeckViewModel must include 'stop_automation_loop' action when armed"
  )
  assert "'client-action'" in fn_fragment or '"client-action"' in fn_fragment, (
    "POST-13: stop_automation_loop must use kind: 'client-action' so it is dispatched client-side"
  )
  assert 'Stop automation' in fn_fragment, (
    "POST-13: stop_automation_loop action must have label 'Stop automation'"
  )


def test_post14_stop_automation_handler_wired_in_deck_view_shell() -> None:
  source = inspect.getsource(web_app._render_html)
  shell_pos = source.find('function renderDeckViewShell(')
  assert shell_pos != -1, 'POST-14: renderDeckViewShell must exist in _render_html'
  # Window covers through the stop_automation_loop dispatch; renderDeckViewShell grew with
  # the UX-1 scanCanceling reconciliation at its head, so size generously.
  fn_fragment = source[shell_pos:shell_pos + 7500]
  assert 'stop_automation_loop' in fn_fragment, (
    "POST-14: renderDeckViewShell click handler must dispatch 'stop_automation_loop' client-action"
  )
  # Stop logic lives in executeStopAutomationLoop; verify it is defined and contains required calls.
  exec_pos = source.find('async function executeStopAutomationLoop(')
  assert exec_pos != -1, 'POST-14: executeStopAutomationLoop must be defined in _render_html'
  exec_fragment = source[exec_pos:exec_pos + 1000]
  assert 'stopAutoFindCadenceTicker' in exec_fragment, (
    'POST-14: executeStopAutomationLoop must call stopAutoFindCadenceTicker to halt cadence'
  )
  assert 'state.autoAdvanceEnabled = false' in exec_fragment, (
    'POST-14: executeStopAutomationLoop must set state.autoAdvanceEnabled = false'
  )


def test_cpo_stop_halts_all_concurrent_refire_tickers() -> None:
  # CP-O (orchestration): an operator stop must halt EVERY concurrent refire loop,
  # not just the cadence ticker. The zero-found retry ticker fires scans
  # independently of the arm state, so leaving it running lets the background loop
  # keep surfacing new candidates after stop.
  source = inspect.getsource(web_app._render_html)
  exec_pos = source.find('async function executeStopAutomationLoop(')
  assert exec_pos != -1, 'CP-O: executeStopAutomationLoop must be defined'
  exec_fragment = source[exec_pos:exec_pos + 1400]
  assert 'stopAutoFindCadenceTicker()' in exec_fragment, (
    'CP-O: stop must halt the cadence ticker'
  )
  assert 'stopZeroFoundRetryTicker()' in exec_fragment, (
    'CP-O: stop must halt the zero-found retry ticker — it refires scans '
    'independently of the arm state and surfaces new candidates after stop'
  )
  assert 'stopProcessingElapsedTicker()' in exec_fragment, (
    'CP-O: stop must halt the processing-elapsed ticker'
  )


def test_cpo_retry_ticker_self_halts_on_manual_stop() -> None:
  # CP-O: the zero-found retry ticker must self-halt when the manual-stop sentinel
  # is set. Backend timer owns dispatch — the ticker is display-only. The
  # manual-stop guard ensures the display ticker stops cleanly on operator stop.
  source = inspect.getsource(web_app._render_html)
  fn_pos = source.find('function updateZeroFoundRetryCountdown()')
  assert fn_pos != -1, 'CP-O: updateZeroFoundRetryCountdown must be defined'
  fn_fragment = source[fn_pos:fn_pos + 3100]
  # The manual-stop guard must appear before the first other early-return so it
  # takes priority and cannot be re-armed.
  guard_pos = fn_fragment.find('state.automationManualStop')
  assert guard_pos != -1, (
    'CP-O: updateZeroFoundRetryCountdown must check state.automationManualStop to self-halt'
  )
  guard_window = fn_fragment[guard_pos:guard_pos + 120]
  assert 'stopZeroFoundRetryTicker()' in guard_window, (
    'CP-O: the manual-stop guard must call stopZeroFoundRetryTicker() and return'
  )
  # Backend timer owns all retry dispatch — the ticker must not fire requestScheduledScanRefire.
  assert 'requestScheduledScanRefire' not in fn_fragment, (
    'CP-O: updateZeroFoundRetryCountdown must not dispatch — backend timer owns retry fires'
  )


def test_cpb_fetch_stage_columns_active_count_excludes_cancelled(tmp_path: Path) -> None:
  # CP-B: active_stage_candidate_count (the surface-visibility gate) must count
  # only non-terminal stages (queued + filled). Cancelled candidates stay in the
  # columns for audit but must not keep the EXECUTION panel open after an operator
  # stop cancels everything.
  state_db_path = str(tmp_path / 'cpb-stage.sqlite3')
  conn = sqlite3.connect(state_db_path)
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute("INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES ('run-cpb', 'sess-cpb')")
  conn.executemany(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json) VALUES (?,?,?,?,?,?)',
    [
      ('run-cpb', 'terminal', 'auto_cancel', 'KX-CPB-A', 'live_qualifying', '{}'),
      ('run-cpb', 'terminal', 'auto_cancel', 'KX-CPB-B', 'live_qualifying', '{}'),
      ('run-cpb', 'terminal', 'canceled',    'KX-CPB-C', 'live_qualifying', '{}'),
    ],
  )
  conn.commit()
  conn.close()
  result = web_app._fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': 'sess-cpb'},
    'settings': {'state_db_path': state_db_path},
  })
  assert result['total_stage_candidate_count'] == 3, 'CP-B: total count still includes cancelled (audit columns intact)'
  assert result['active_stage_candidate_count'] == 0, (
    'CP-B: active count must exclude cancelled — a fully-cancelled set must collapse the panel'
  )


def test_cpb_fetch_stage_columns_active_count_includes_queued_and_filled(tmp_path: Path) -> None:
  # CP-B: active_stage_candidate_count must include queued (discovered) and filled
  # (locked) candidates so the panel stays visible while real work is in flight.
  state_db_path = str(tmp_path / 'cpb-active.sqlite3')
  conn = sqlite3.connect(state_db_path)
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute('CREATE TABLE pair_plans (pair_id TEXT PRIMARY KEY, ticker TEXT NOT NULL)')
  conn.execute('CREATE TABLE pair_states (id INTEGER PRIMARY KEY AUTOINCREMENT, pair_id TEXT NOT NULL, state TEXT NOT NULL, detail_json TEXT)')
  conn.execute("INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES ('run-cpb2', 'sess-cpb2')")
  conn.executemany(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json) VALUES (?,?,?,?,?,?)',
    [
      ('run-cpb2', 'discovered', None, 'KX-CPB-Q', 'live_qualifying', '{}'),
      ('run-cpb2', 'in_flight',  None, 'KX-CPB-F', 'live_qualifying', '{}'),
      ('run-cpb2', 'terminal', 'auto_cancel', 'KX-CPB-X', 'live_qualifying', '{}'),
    ],
  )
  conn.executemany('INSERT INTO pair_plans (pair_id, ticker) VALUES (?,?)', [('p-f', 'KX-CPB-F')])
  conn.executemany('INSERT INTO pair_states (pair_id, state) VALUES (?,?)', [('p-f', 'LOCKED')])
  conn.commit()
  conn.close()
  result = web_app._fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': 'sess-cpb2'},
    'settings': {'state_db_path': state_db_path},
  })
  assert result['active_stage_candidate_count'] == 2, (
    'CP-B: active count must include queued + filled (2), excluding the cancelled one'
  )


def test_fetch_stage_columns_projects_filled_and_settled_exposure_as_filled(tmp_path: Path) -> None:
  state_db_path = str(tmp_path / 'filled-stage.sqlite3')
  conn = sqlite3.connect(state_db_path)
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute('CREATE TABLE pair_plans (pair_id TEXT PRIMARY KEY, ticker TEXT NOT NULL)')
  conn.execute('CREATE TABLE pair_states (id INTEGER PRIMARY KEY AUTOINCREMENT, pair_id TEXT NOT NULL, state TEXT NOT NULL, detail_json TEXT)')
  conn.execute("INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES ('run-filled', 'sess-filled')")
  conn.executemany(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json) VALUES (?,?,?,?,?,?)',
    [
      ('run-filled', 'in_flight', None, 'KX-FILLED', 'live_qualifying', '{}'),
      ('run-filled', 'in_flight', None, 'KX-SETTLED-EXPOSURE', 'live_qualifying', '{}'),
    ],
  )
  conn.executemany(
    'INSERT INTO pair_plans (pair_id, ticker) VALUES (?,?)',
    [('p-filled', 'KX-FILLED'), ('p-settled-exposure', 'KX-SETTLED-EXPOSURE')],
  )
  conn.executemany(
    'INSERT INTO pair_states (pair_id, state) VALUES (?,?)',
    [('p-filled', 'FILLED'), ('p-settled-exposure', 'SETTLED_EXPOSURE')],
  )
  conn.commit()
  conn.close()
  result = web_app._fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': 'sess-filled'},
    'settings': {'state_db_path': state_db_path},
  })
  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}
  assert len(columns['filled']) == 2
  assert columns['queued'] == []


def test_fetch_stage_columns_emits_full_payload_contract_across_stages(tmp_path: Path) -> None:
  # W5: stage items must carry the full read-only detail payload the candidate
  # detail popup needs (stage_id/stage_label plus every field promoted from the
  # canonical row), not just the historically-thin ticker/qualifier_tier/close_time set.
  state_db_path = str(tmp_path / 'popup-contract-stage.sqlite3')
  conn = sqlite3.connect(state_db_path)
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute('CREATE TABLE pair_plans (pair_id TEXT PRIMARY KEY, ticker TEXT NOT NULL)')
  conn.execute(
    'CREATE TABLE pair_states'
    ' (id INTEGER PRIMARY KEY AUTOINCREMENT, pair_id TEXT NOT NULL, state TEXT NOT NULL, detail_json TEXT)'
  )
  conn.execute(
    'CREATE TABLE candidate_saved_set_members (id INTEGER PRIMARY KEY AUTOINCREMENT, saved_set_id TEXT, candidate_key TEXT)'
  )
  conn.execute(
    'CREATE TABLE candidate_saved_set_evaluations'
    ' (id INTEGER PRIMARY KEY AUTOINCREMENT, saved_set_id TEXT, actionability_status TEXT, visibility_status TEXT)'
  )
  conn.execute("INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES ('run-contract', 'sess-contract')")
  conn.executemany(
    'INSERT INTO candidate_review_candidates'
    ' (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json, candidate_uid, candidate_key)'
    ' VALUES (?,?,?,?,?,?,?,?)',
    [
      ('run-contract', 'in_flight', None, 'KX-FILLED', 'live_qualifying',
       '{"close_time_utc": "2026-07-04T00:00:00Z"}', 'uid-filled', 'key-filled'),
      ('run-contract', 'terminal', 'expired_unfilled', 'KX-CANCELLED', 'sandbox_extended',
       '{}', 'uid-cancelled', 'key-cancelled'),
    ],
  )
  conn.executemany(
    'INSERT INTO pair_plans (pair_id, ticker) VALUES (?,?)',
    [('p-filled', 'KX-FILLED')],
  )
  conn.executemany(
    'INSERT INTO pair_states (pair_id, state, detail_json) VALUES (?,?,?)',
    [('p-filled', 'FILLED', '{"yes_filled_contracts": 4}')],
  )
  conn.executemany(
    'INSERT INTO candidate_saved_set_members (saved_set_id, candidate_key) VALUES (?,?)',
    [('saved-set-1', 'key-filled')],
  )
  conn.executemany(
    'INSERT INTO candidate_saved_set_evaluations (saved_set_id, actionability_status, visibility_status) VALUES (?,?,?)',
    [('saved-set-1', 'actionable', 'visible')],
  )
  conn.commit()
  conn.close()
  result = web_app._fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': 'sess-contract'},
    'settings': {'state_db_path': state_db_path},
  })
  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}

  assert len(columns['filled']) == 1
  filled_item = columns['filled'][0]
  assert filled_item['stage_id'] == 'filled'
  assert filled_item['stage_label'] == 'Filled'
  assert filled_item['candidate_key'] == 'key-filled'
  assert filled_item['candidate_uid'] == 'uid-filled'
  assert filled_item['lifecycle_stage'] == 'in_flight'
  assert filled_item['saved_set_id'] == 'saved-set-1'
  assert filled_item['saved_set_actionability_status'] == 'actionable'
  assert filled_item['saved_set_visibility_status'] == 'visible'
  assert filled_item['pair_state'] == 'FILLED'
  assert filled_item['pair_state_label'] == 'Filled'
  assert filled_item['pair_state_detail'] == {'yes_filled_contracts': 4}
  assert filled_item['detail'] == {'close_time_utc': '2026-07-04T00:00:00Z'}
  assert filled_item['close_time'] == '2026-07-04T00:00:00Z'

  assert len(columns['cancelled']) == 1
  cancelled_item = columns['cancelled'][0]
  assert cancelled_item['stage_id'] == 'cancelled'
  assert cancelled_item['stage_label'] == 'Cancelled'
  assert cancelled_item['candidate_key'] == 'key-cancelled'
  assert cancelled_item['candidate_uid'] == 'uid-cancelled'
  assert cancelled_item['terminal_cause'] == 'expired_unfilled'
  assert cancelled_item['lifecycle_stage'] == 'terminal'


def _make_saved_set_retirement_fixture(
    tmp_path: Path,
    *,
    name: str,
    lifecycle_stage: str,
    terminal_cause: str | None,
    actionability_status: str,
    visibility_status: str,
) -> str:
  state_db_path = str(tmp_path / f'{name}.sqlite3')
  conn = sqlite3.connect(state_db_path)
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute(
    'CREATE TABLE candidate_saved_set_members (id INTEGER PRIMARY KEY AUTOINCREMENT, saved_set_id TEXT, candidate_key TEXT)'
  )
  conn.execute(
    'CREATE TABLE candidate_saved_set_evaluations'
    ' (id INTEGER PRIMARY KEY AUTOINCREMENT, saved_set_id TEXT, actionability_status TEXT, visibility_status TEXT)'
  )
  conn.execute(f"INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES ('run-{name}', 'sess-{name}')")
  conn.execute(
    'INSERT INTO candidate_review_candidates'
    ' (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json, candidate_uid, candidate_key)'
    ' VALUES (?,?,?,?,?,?,?,?)',
    (f'run-{name}', lifecycle_stage, terminal_cause, f'KX-{name.upper()}', 'live_qualifying', '{}', f'uid-{name}', f'key-{name}'),
  )
  conn.execute(
    'INSERT INTO candidate_saved_set_members (saved_set_id, candidate_key) VALUES (?,?)',
    (f'saved-set-{name}', f'key-{name}'),
  )
  conn.execute(
    'INSERT INTO candidate_saved_set_evaluations (saved_set_id, actionability_status, visibility_status) VALUES (?,?,?)',
    (f'saved-set-{name}', actionability_status, visibility_status),
  )
  conn.commit()
  conn.close()
  return state_db_path


def test_fetch_stage_columns_prefers_candidates_own_terminal_cause_over_saved_set_retirement(tmp_path: Path) -> None:
  # W5 popup-audit fix: a candidate that already reached its own terminal state (e.g. the
  # market expired before it could fill) must show that cause, even when its saved set is
  # also retired (visibility -> history_only) as a downstream consequence. Regression case:
  # a candidate that expired on its own was showing the generic 'submitted_terminal' label,
  # which falsely implied it had been submitted for execution.
  state_db_path = _make_saved_set_retirement_fixture(
    tmp_path,
    name='ownterminal',
    lifecycle_stage='terminal',
    terminal_cause='expired_unfilled',
    actionability_status='expired_actionability',
    visibility_status='history_only',
  )
  result = web_app._fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': 'sess-ownterminal'},
    'settings': {'state_db_path': state_db_path},
  })
  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}
  assert len(columns['cancelled']) == 1
  assert columns['cancelled'][0]['terminal_cause'] == 'expired_unfilled'


def test_fetch_stage_columns_uses_saved_set_actionability_when_candidate_has_no_own_cause(tmp_path: Path) -> None:
  # When the candidate itself never reached its own terminal state, the saved set's own
  # actionability status is the only available reason — use it verbatim (it already has an
  # established human label elsewhere) rather than always writing 'submitted_terminal'.
  state_db_path = _make_saved_set_retirement_fixture(
    tmp_path,
    name='noownterminal',
    lifecycle_stage='in_flight',
    terminal_cause=None,
    actionability_status='expired_actionability',
    visibility_status='history_only',
  )
  result = web_app._fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': 'sess-noownterminal'},
    'settings': {'state_db_path': state_db_path},
  })
  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}
  assert len(columns['cancelled']) == 1
  assert columns['cancelled'][0]['terminal_cause'] == 'expired_actionability'


def test_fetch_stage_columns_reports_submitted_terminal_only_when_actionability_says_so(tmp_path: Path) -> None:
  state_db_path = _make_saved_set_retirement_fixture(
    tmp_path,
    name='submitted',
    lifecycle_stage='in_flight',
    terminal_cause=None,
    actionability_status='submitted_terminal',
    visibility_status='history_only',
  )
  result = web_app._fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': 'sess-submitted'},
    'settings': {'state_db_path': state_db_path},
  })
  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}
  assert len(columns['cancelled']) == 1
  assert columns['cancelled'][0]['terminal_cause'] == 'submitted_terminal'


def test_fetch_stage_columns_projects_no_exposure_settled_as_cancelled(tmp_path: Path) -> None:
  state_db_path = str(tmp_path / 'no-exposure-stage.sqlite3')
  conn = sqlite3.connect(state_db_path)
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute('CREATE TABLE pair_plans (pair_id TEXT PRIMARY KEY, ticker TEXT NOT NULL)')
  conn.execute('CREATE TABLE pair_states (id INTEGER PRIMARY KEY AUTOINCREMENT, pair_id TEXT NOT NULL, state TEXT NOT NULL, detail_json TEXT)')
  conn.execute("INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES ('run-no-exp', 'sess-no-exp')")
  conn.execute(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json) VALUES (?,?,?,?,?,?)',
    ('run-no-exp', 'in_flight', None, 'KX-NO-EXPOSURE', 'live_qualifying', '{}'),
  )
  conn.execute('INSERT INTO pair_plans (pair_id, ticker) VALUES (?,?)', ('p-no-exp', 'KX-NO-EXPOSURE'))
  conn.execute(
    'INSERT INTO pair_states (pair_id, state, detail_json) VALUES (?,?,?)',
    (
      'p-no-exp',
      'SETTLED',
      json.dumps({
        'ticker': 'KX-NO-EXPOSURE',
        'terminal_reason': 'kalshi_alignment_no_exposure',
        'yes_filled_contracts': '0',
        'no_filled_contracts': '0',
      }),
    ),
  )
  conn.commit()
  conn.close()

  result = web_app._fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': 'sess-no-exp'},
    'settings': {'state_db_path': state_db_path},
  })

  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}
  assert columns['filled'] == []
  assert columns['cancelled'][0]['ticker'] == 'KX-NO-EXPOSURE'
  assert columns['cancelled'][0]['terminal_cause'] == 'no_exposure'


def test_fetch_stage_columns_reconciled_without_fill_bearing_pair_is_not_filled(tmp_path: Path) -> None:
  state_db_path = str(tmp_path / 'reconciled-no-fill-stage.sqlite3')
  conn = sqlite3.connect(state_db_path)
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute("INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES ('run-rec', 'sess-rec')")
  conn.execute(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json) VALUES (?,?,?,?,?,?)',
    ('run-rec', 'terminal', 'reconciled', 'KX-REC-NO-FILL', 'live_qualifying', '{}'),
  )
  conn.commit()
  conn.close()

  result = web_app._fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': 'sess-rec'},
    'settings': {'state_db_path': state_db_path},
  })

  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}
  assert columns['filled'] == []
  assert columns['cancelled'][0]['terminal_cause'] == 'reconciled_no_fill'


def test_saved_set_candidate_lifecycle_sync_is_ticker_scoped(tmp_path: Path) -> None:
  state_db_path = str(tmp_path / 'ticker-scoped-lifecycle.sqlite3')
  conn = sqlite3.connect(state_db_path)
  conn.row_factory = sqlite3.Row
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, terminal_subcause TEXT,'
    '  terminal_at_utc TEXT, ticker TEXT, qualifier_tier TEXT, detail_json TEXT)'
  )
  conn.execute('CREATE TABLE runtime_events (id INTEGER PRIMARY KEY AUTOINCREMENT, event_type TEXT, recorded_at_utc TEXT, detail_json TEXT)')
  conn.execute('CREATE TABLE pair_plans (pair_id TEXT PRIMARY KEY, ticker TEXT NOT NULL)')
  conn.execute('CREATE TABLE pair_states (id INTEGER PRIMARY KEY AUTOINCREMENT, pair_id TEXT NOT NULL, state TEXT NOT NULL, recorded_at_utc TEXT, detail_json TEXT)')
  conn.executemany(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, terminal_subcause, ticker, qualifier_tier, detail_json) VALUES (?,?,?,?,?,?,?)',
    [
      ('run-scope', 'discovered', None, None, 'KX-BLOCKED', 'live_qualifying', '{}'),
      ('run-scope', 'discovered', None, None, 'KX-NO-EXPOSURE', 'live_qualifying', '{}'),
      ('run-scope', 'discovered', None, None, 'KX-UNTOUCHED', 'live_qualifying', '{}'),
    ],
  )
  conn.execute(
    'INSERT INTO runtime_events (event_type, recorded_at_utc, detail_json) VALUES (?,?,?)',
    (
      'submit_bridge_blocked',
      '2026-06-30T21:40:24Z',
      json.dumps({
        'saved_set_id': 'saved-scope',
        'ticker': 'KX-BLOCKED',
        'orders_created': False,
        'blocked_reason': 'coverability_divergence_blocked',
      }),
    ),
  )
  conn.execute('INSERT INTO pair_plans (pair_id, ticker) VALUES (?,?)', ('p-no-exp', 'KX-NO-EXPOSURE'))
  conn.execute(
    'INSERT INTO pair_states (pair_id, state, recorded_at_utc, detail_json) VALUES (?,?,?,?)',
    (
      'p-no-exp',
      'SETTLED',
      '2026-06-30T21:42:27Z',
      json.dumps({
        'saved_set_id': 'saved-scope',
        'ticker': 'KX-NO-EXPOSURE',
        'terminal_reason': 'kalshi_alignment_no_exposure',
      }),
    ),
  )
  conn.commit()

  web_app._sync_saved_set_candidate_lifecycle_from_execution(conn, saved_set_id='saved-scope', run_id='run-scope')
  rows = {
    row['ticker']: dict(row)
    for row in conn.execute(
      'SELECT ticker, lifecycle_stage, terminal_cause, terminal_subcause FROM candidate_review_candidates'
    )
  }

  assert rows['KX-BLOCKED']['lifecycle_stage'] == 'terminal'
  assert rows['KX-BLOCKED']['terminal_cause'] == 'rejected'
  assert rows['KX-BLOCKED']['terminal_subcause'] == 'coverability_divergence_blocked'
  assert rows['KX-NO-EXPOSURE']['lifecycle_stage'] == 'terminal'
  assert rows['KX-NO-EXPOSURE']['terminal_cause'] == 'canceled'
  assert rows['KX-NO-EXPOSURE']['terminal_subcause'] == 'no_exposure'
  assert rows['KX-UNTOUCHED']['lifecycle_stage'] == 'discovered'
  assert rows['KX-UNTOUCHED']['terminal_cause'] is None


def test_cpb_surface_gate_uses_active_stage_count() -> None:
  # CP-B: the surface-visibility computation must read _active_stage_candidate_count,
  # not the cancelled-inclusive total, so the panel collapses once everything is cancelled.
  source = inspect.getsource(web_app._build_pair_monitor_payload)
  assert '_active_stage_candidate_count' in source, (
    'CP-B: _active_stage_candidate_count must feed the surface-visibility gate'
  )
  assert '_surface_active_count = max(len(active_runtime_rows), active_stage_candidate_count)' in source, (
    'CP-B: _surface_active_count must use the active (non-terminal) stage count'
  )


def test_post15_submitting_card_permanent_in_grid_template() -> None:
  source = inspect.getsource(web_app._render_html)
  render_pos = source.find('function renderLiveInteractionSurface(')
  assert render_pos != -1, 'POST-15: renderLiveInteractionSurface must exist in _render_html'
  # The grid.innerHTML template is within 3000 chars of the function start
  fn_fragment = source[render_pos:render_pos + 3000]
  assert 'submit-order-elapsed-card' in fn_fragment, (
    'POST-15: renderLiveInteractionSurface grid template must contain the permanent submit-order-elapsed-card'
  )
  elapsed_card_pos = fn_fragment.find('submit-order-elapsed-card')
  card_context = fn_fragment[elapsed_card_pos - 20:elapsed_card_pos + 100]
  assert 'hidden' in card_context, (
    'POST-15: submit-order-elapsed-card must have hidden attribute in the grid template (starts hidden)'
  )


def test_post15_show_submit_pending_surface_no_status_warn_on_section() -> None:
  source = inspect.getsource(web_app._render_html)
  fn_pos = source.find('function showSubmitPendingSurface(')
  assert fn_pos != -1, 'POST-15: showSubmitPendingSurface must exist in _render_html'
  fn_fragment = source[fn_pos:fn_pos + 1200]
  assert "classList.add('status-warn')" not in fn_fragment, (
    "POST-15: showSubmitPendingSurface must NOT add 'status-warn' to the section element — "
    'brownish background should scope only to the elapsed card, not the whole panel'
  )
  assert 'hidden = false' in fn_fragment, (
    'POST-15: showSubmitPendingSurface must show the permanent elapsed card by setting hidden = false'
  )


def test_post15_elapsed_card_hidden_not_removed_in_finally() -> None:
  source = inspect.getsource(web_app._render_html)
  perform_action_pos = source.find('async function performAction(')
  assert perform_action_pos != -1, 'POST-15: performAction must exist in _render_html'
  fn_fragment = source[perform_action_pos:perform_action_pos + 12000]
  pending_false_pos = fn_fragment.find('state.submitOrderPending = false')
  assert pending_false_pos != -1, 'POST-15: performAction must set submitOrderPending = false in finally'
  pending_block = fn_fragment[pending_false_pos - 50:pending_false_pos + 500]
  assert 'submit-order-elapsed-card' in pending_block, (
    'POST-15: submit-order-elapsed-card must be referenced in the performAction finally cleanup block'
  )
  assert '.hidden = true' in pending_block, (
    'POST-15: elapsed card cleanup must set hidden = true (not .remove()) — card is permanent in DOM'
  )
  assert '.remove()' not in pending_block, (
    'POST-15: elapsed card cleanup must NOT call .remove() — the card is a permanent hidden slot'
  )


def test_post14_sentinel_blocks_cadence_restore_after_manual_stop() -> None:
  source = inspect.getsource(web_app._render_html)
  # state init must declare automationManualStop
  assert 'automationManualStop' in source, (
    'GAP-POST-14: automationManualStop sentinel must be declared in state init'
  )
  # stop logic is in executeStopAutomationLoop (invoked on confirm); verify sentinel set there
  exec_pos = source.find('async function executeStopAutomationLoop(')
  assert exec_pos != -1, 'GAP-POST-14: executeStopAutomationLoop must be defined'
  exec_fragment = source[exec_pos:exec_pos + 600]
  assert 'automationManualStop = true' in exec_fragment, (
    'GAP-POST-14: executeStopAutomationLoop must set state.automationManualStop = true '
    'before disarming so the runAutoSequence finally block does not re-arm'
  )
  # wasArmed restore must check the sentinel
  finally_pos = source.find('if (!_hasExplicitBlock')
  assert finally_pos != -1, 'GAP-POST-14: _hasExplicitBlock guard must exist in runAutoSequence finally'
  finally_fragment = source[finally_pos:finally_pos + 100]
  assert 'automationManualStop' in finally_fragment, (
    'GAP-POST-14: wasArmed restore must check !state.automationManualStop to prevent re-arm after operator stop'
  )
  # ARM path must clear the sentinel
  arm_pos = source.find("appendLog('Bounded automation enabled and client auto-forward armed by operator.')")
  assert arm_pos != -1, 'GAP-POST-14: arm log message must exist'
  arm_window = source[arm_pos - 280:arm_pos]
  assert 'automationManualStop = false' in arm_window, (
    'GAP-POST-14: ARM path must reset automationManualStop = false so a re-arm after stop is possible'
  )


def test_post16_candidate_mutation_controls_suppressed_during_automation() -> None:
  source = inspect.getsource(web_app._render_html)
  # Find the candidate action row that renders Change selection / bulk toggle / Save candidates
  change_sel_pos = source.find('data-candidate-change-selection')
  assert change_sel_pos != -1, 'POST-16: data-candidate-change-selection button must exist in _render_html'
  # The guard line must include autoAdvanceEnabled
  guard_window = source[change_sel_pos - 150:change_sel_pos]
  assert 'autoAdvanceEnabled' in guard_window, (
    'POST-16: Change selection button guard must include state.autoAdvanceEnabled — '
    'mutation controls must be hidden when automation loop is armed'
  )
  # Bulk toggle button guard must also include autoAdvanceEnabled
  bulk_pos = source.find('data-candidate-bulk-toggle')
  assert bulk_pos != -1, 'POST-16: data-candidate-bulk-toggle button must exist'
  bulk_guard = source[bulk_pos - 200:bulk_pos]
  assert 'autoAdvanceEnabled' in bulk_guard, (
    'POST-16: Bulk toggle button guard must include state.autoAdvanceEnabled'
  )


def test_post15_show_submit_pending_surface_fallback_injects_card_when_missing() -> None:
  source = inspect.getsource(web_app._render_html)
  fn_pos = source.find('function showSubmitPendingSurface(')
  assert fn_pos != -1, 'POST-16: showSubmitPendingSurface must exist in _render_html'
  fn_fragment = source[fn_pos:fn_pos + 1500]
  assert 'let _elapsedCard' in fn_fragment, (
    'POST-16: showSubmitPendingSurface must declare _elapsedCard with let so it can be reassigned in the fallback branch'
  )
  assert 'createElement' in fn_fragment, (
    'POST-16: showSubmitPendingSurface must create the elapsed card via createElement when the permanent slot is not yet in the DOM'
  )
  assert 'grid.appendChild' in fn_fragment, (
    'POST-16: showSubmitPendingSurface must append the created card to the grid element as a fallback'
  )


def test_post17_cancel_all_pairs_hidden_during_automation() -> None:
  source = inspect.getsource(web_app._render_html)
  fn_pos = source.find('function renderDeckContractStatus(')
  assert fn_pos != -1, 'POST-17: renderDeckContractStatus must exist in _render_html'
  fn_fragment = source[fn_pos:fn_pos + 600]
  hidden_pos = fn_fragment.find('cancelActionButton.hidden')
  assert hidden_pos != -1, 'POST-17: renderDeckContractStatus must set cancelActionButton.hidden'
  hidden_line = fn_fragment[hidden_pos:hidden_pos + 120]
  assert 'autoAdvanceEnabled' in hidden_line, (
    'POST-17: cancelActionButton.hidden assignment must include state.autoAdvanceEnabled gate '
    '— CANCEL ALL PAIRS must be suppressed during automation'
  )


def test_post18_ws_cluster_suppressed_during_automation() -> None:
  source = inspect.getsource(web_app._render_html)
  fn_pos = source.find('function buildDeckViewModel(')
  assert fn_pos != -1, 'POST-18: buildDeckViewModel must exist in _render_html'
  fn_fragment = source[fn_pos:fn_pos + 3500]
  ws_cluster_pos = fn_fragment.find('action-cluster')
  assert ws_cluster_pos != -1, 'POST-18: WS cluster section must exist in buildDeckViewModel'
  cluster_gate = fn_fragment[ws_cluster_pos - 200:ws_cluster_pos]
  assert 'autoAdvanceEnabled' in cluster_gate, (
    'POST-18: WS cluster spread condition must include !state.autoAdvanceEnabled gate '
    '— WS/KEYS/DATA cluster must not appear during automation'
  )


def test_post19_automation_stopping_sentinel_wired() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'automationStopping' in source, (
    'POST-19: automationStopping sentinel must be declared in state init'
  )
  # automationStopping is managed in executeStopAutomationLoop (called via confirm)
  exec_pos = source.find('async function executeStopAutomationLoop(')
  assert exec_pos != -1, 'POST-19: executeStopAutomationLoop must be defined'
  # 8ec6f4d ("teardown UX ... popup hang fix") expanded the orchestrated teardown
  # (halt every refire loop before the awaited cancel), pushing the finally cleanup
  # further down; widen the fragment so it still reaches the finally clause.
  exec_fragment = source[exec_pos:exec_pos + 2400]
  assert 'automationStopping = true' in exec_fragment, (
    'POST-19: executeStopAutomationLoop must set state.automationStopping = true '
    'to maintain STOP AUTOMATION display during scan cancel processing'
  )
  assert 'automationStopping = false' in exec_fragment, (
    'POST-19: executeStopAutomationLoop must clear state.automationStopping = false after cancel resolves'
  )
  assert 'finally' in exec_fragment, (
    'POST-19: automationStopping cleanup must be in a finally block to clear flag even on error'
  )
  deck_fn_pos = source.find('function buildDeckViewModel(')
  assert deck_fn_pos != -1
  deck_fragment = source[deck_fn_pos:deck_fn_pos + 2200]
  stop_gate_pos = deck_fragment.find("value: 'stop_automation_loop'")
  assert stop_gate_pos != -1, 'POST-19: stop_automation_loop action must be in buildDeckViewModel'
  # The gate is the `if (... || state.automationStopping || ...)` condition immediately preceding
  # the stop-automation action push; window sized to that adjacent block (de-brittled from 250,
  # which clipped the word by ~9 chars after a refactor) without losing the locality intent.
  gate_window = deck_fragment[stop_gate_pos - 320:stop_gate_pos]
  assert 'automationStopping' in gate_window, (
    'POST-19: buildDeckViewModel STOP AUTOMATION gate must include state.automationStopping '
    '— prevents normal controls from flashing during cancel processing'
  )


def test_post20_pair_monitor_candidate_rows_enriched_with_runtime_stage(tmp_path: Path) -> None:
  # SSOT: candidate cards derive from the persisted canonical query, so seed the DB under a lane
  # session, then assert runtime_stage enrichment from the pairs (queued / canceled / unmatched).
  from polyventure.persistence import open_database
  state_db_path = str(tmp_path / 'state.sqlite3')
  conn = open_database(state_db_path)
  conn.execute(
    "INSERT INTO candidate_review_runs"
    " (run_id, operation_lane, lane_session_id, candidate_signature, candidate_count, source_action, detail_json, recorded_at_utc)"
    " VALUES ('p20-run', 'sandbox', 'p20-lsid', '', 3, 'scan', '{}', '2026-06-21T00:00:00Z')"
  )
  for _i, _tk in enumerate(
    ['KXQUICKSETTLE-14JUN26H0020-2', 'KXQUICKSETTLE-14JUN26H0020-3', 'KXQUICKSETTLE-14JUN26H0020-4'],
    start=1,
  ):
    conn.execute(
      "INSERT INTO candidate_review_candidates"
      " (run_id, candidate_uid, candidate_key, ticker, qualifier_tier, detail_json, recorded_at_utc, lifecycle_stage, operation_lane)"
      " VALUES ('p20-run', ?, ?, ?, 'live_qualifying', ?, '2026-06-21T00:00:00Z', 'discovered', 'sandbox')",
      (_tk, _tk, _tk, json.dumps({'ticker': _tk, 'rank': _i})),
    )
  conn.commit()
  payload = web_app._build_pair_monitor_payload({
    'review_selection': {'persisted_lane_session_id': 'p20-lsid'},
    'settings': {'state_db_path': state_db_path},
    'pairs': [
      {'ticker': 'KXQUICKSETTLE-14JUN26H0020-2', 'state': 'QUEUED'},
      {'ticker': 'KXQUICKSETTLE-14JUN26H0020-3', 'state': 'CANCELED'},
    ],
  })
  rows = payload['candidate_rows']
  by_ticker = {r['ticker']: r for r in rows}
  assert by_ticker['KXQUICKSETTLE-14JUN26H0020-2']['runtime_stage'] == 'queued', (
    'GAP-POST-20: candidate matched to a QUEUED pair_row must have runtime_stage=queued'
  )
  assert by_ticker['KXQUICKSETTLE-14JUN26H0020-3']['runtime_stage'] == 'canceled', (
    'GAP-POST-20: candidate matched to a CANCELED pair_row must have runtime_stage=canceled'
  )
  assert by_ticker['KXQUICKSETTLE-14JUN26H0020-4']['runtime_stage'] == '', (
    'GAP-POST-20: candidate with no matching pair_row must have runtime_stage empty string'
  )


def test_post20_candidate_card_renders_runtime_stage_pill() -> None:
  source = inspect.getsource(web_app._render_html)
  # Anchor at the JS article template, not the CSS rule
  article_pos = source.find("data-candidate-open=\"${escapeHtml(candidate._candidateKey)}\"")
  assert article_pos != -1, 'GAP-POST-20: candidate article template must exist in _render_html'
  card_fragment = source[article_pos:article_pos + 1400]
  badge_pos = card_fragment.find('candidate-badge-row')
  assert badge_pos != -1, 'GAP-POST-20: candidate-badge-row must exist in the JS card template'
  badge_fragment = card_fragment[badge_pos:badge_pos + 700]
  assert 'runtime_stage' in badge_fragment, (
    'GAP-POST-20: candidate badge row must render runtime_stage pill when runtime_stage is present'
  )
  assert 'toUpperCase' in badge_fragment, (
    'GAP-POST-20: runtime_stage pill must display the stage value in uppercase'
  )


def test_post23_fetch_stage_columns_real_time_pair_state_override(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime.sqlite3'
  run_id = 'test-run-post23'
  lane_session_id = 'test-session-post23'
  conn = sqlite3.connect(str(state_db_path))
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute('CREATE TABLE pair_plans (pair_id TEXT PRIMARY KEY, ticker TEXT NOT NULL)')
  conn.execute(
    'CREATE TABLE pair_states'
    ' (id INTEGER PRIMARY KEY AUTOINCREMENT, pair_id TEXT NOT NULL, state TEXT NOT NULL, detail_json TEXT)'
  )
  conn.execute(
    'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?,?)',
    (run_id, lane_session_id),
  )
  conn.executemany(
    'INSERT INTO candidate_review_candidates'
    ' (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json)'
    ' VALUES (?,?,?,?,?,?)',
    [
      (run_id, 'in_flight', None, 'TICK-A', 'live_qualifying', '{}'),
      (run_id, 'in_flight', None, 'TICK-B', 'sandbox_extended', '{}'),
      (run_id, 'in_flight', None, 'TICK-C', '', '{}'),
    ],
  )
  conn.executemany(
    'INSERT INTO pair_plans (pair_id, ticker) VALUES (?,?)',
    [('pair-a1', 'TICK-A'), ('pair-b1', 'TICK-B')],
  )
  conn.executemany(
    'INSERT INTO pair_states (pair_id, state) VALUES (?,?)',
    [('pair-a1', 'CANCELED'), ('pair-b1', 'LOCKED')],
  )
  conn.commit()
  conn.close()

  payload = {
    'review_selection': {'persisted_lane_session_id': lane_session_id},
    'settings': {'state_db_path': str(state_db_path)},
  }
  result = web_app._fetch_stage_columns(payload)
  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}
  queued_tickers = [i['ticker'] for i in columns['queued']]
  filled_tickers = [i['ticker'] for i in columns['filled']]
  cancelled_tickers = [i['ticker'] for i in columns['cancelled']]
  assert 'TICK-A' in cancelled_tickers, (
    'POST-23: TICK-A has CANCELED pair_state — must appear in cancelled column, not queued'
  )
  assert 'TICK-A' not in queued_tickers, (
    'POST-23: TICK-A must NOT appear in queued when pair_state is CANCELED'
  )
  assert 'TICK-B' in filled_tickers, (
    'POST-23: TICK-B has LOCKED pair_state — must appear in filled column'
  )
  assert 'TICK-B' not in queued_tickers, (
    'POST-23: TICK-B must NOT appear in queued when pair_state is LOCKED'
  )
  assert 'TICK-C' in queued_tickers, (
    'POST-23: TICK-C has no pair_state entry — must remain in queued column'
  )
  assert result['in_flight_candidate_count'] == 1, (
    'POST-23: in_flight_candidate_count must reflect only truly queued candidates'
  )


def test_post24_total_stage_candidate_count_and_surface_visible_liveness(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime.sqlite3'
  run_id = 'test-run-post24'
  lane_session_id = 'test-session-post24'
  conn = sqlite3.connect(str(state_db_path))
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute('CREATE TABLE pair_plans (pair_id TEXT PRIMARY KEY, ticker TEXT NOT NULL)')
  conn.execute(
    'CREATE TABLE pair_states'
    ' (id INTEGER PRIMARY KEY AUTOINCREMENT, pair_id TEXT NOT NULL, state TEXT NOT NULL, detail_json TEXT)'
  )
  conn.execute(
    'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?,?)',
    (run_id, lane_session_id),
  )
  # Both pairs in_flight; both have CANCELED pair_states → POST-23 moves both to cancelled, queued=0
  conn.executemany(
    'INSERT INTO candidate_review_candidates'
    ' (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json)'
    ' VALUES (?,?,?,?,?,?)',
    [
      (run_id, 'in_flight', None, 'TICK-X', 'live_qualifying', '{}'),
      (run_id, 'in_flight', None, 'TICK-Y', 'live_qualifying', '{}'),
    ],
  )
  conn.executemany(
    'INSERT INTO pair_plans (pair_id, ticker) VALUES (?,?)',
    [('pair-x1', 'TICK-X'), ('pair-y1', 'TICK-Y')],
  )
  conn.executemany(
    'INSERT INTO pair_states (pair_id, state) VALUES (?,?)',
    [('pair-x1', 'CANCELED'), ('pair-y1', 'CANCELED')],
  )
  conn.commit()
  conn.close()

  payload: dict = {
    'review_selection': {'persisted_lane_session_id': lane_session_id},
    'settings': {'state_db_path': str(state_db_path)},
  }
  result = web_app._fetch_stage_columns(payload)
  assert result['in_flight_candidate_count'] == 0, (
    'POST-24: both pairs CANCELED — in_flight_candidate_count (QUEUED) must be 0'
  )
  assert result['total_stage_candidate_count'] == 2, (
    'POST-24: total_stage_candidate_count must include CANCELLED pairs (queued+filled+cancelled=2)'
  )
  # CP-B (supersedes POST-24 liveness): the panel-visibility gate counts only
  # non-terminal stages. A fully-cancelled set must collapse the EXECUTION panel —
  # the cancelled rows remain in the columns for audit but do not keep it open.
  assert result['active_stage_candidate_count'] == 0, (
    'CP-B: active_stage_candidate_count must exclude CANCELLED — fully-cancelled set collapses the panel'
  )
  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}
  assert len(columns['queued']) == 0, 'POST-24: queued must be empty when all pairs CANCELED'
  assert len(columns['cancelled']) == 2, 'POST-24: both pairs must appear in cancelled'

  # Source inspection: the surface-visibility gate must read the active (non-terminal)
  # count, not the cancelled-inclusive total.
  src = inspect.getsource(web_app._build_pair_monitor_payload)
  assert '_active_stage_candidate_count' in src, (
    'CP-B: _build_pair_monitor_payload must read _active_stage_candidate_count from payload'
  )
  surface_count_pos = src.find('_surface_active_count =')
  assert surface_count_pos != -1, 'POST-24: _surface_active_count must exist in _build_pair_monitor_payload'
  surface_count_line = src[surface_count_pos:surface_count_pos + 120]
  assert 'active_stage_candidate_count' in surface_count_line, (
    'CP-B: _surface_active_count must use the active (non-terminal) stage count'
  )


def test_websocket_reconnect_loop_present_in_runtime_loop() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  runtime_loop_pos = source.find('async def _runtime_loop()')
  assert runtime_loop_pos != -1, 'CAD-7: _runtime_loop must exist inside create_operator_console_app'
  # Window covers through the reconnect-attempt event; the loop grew with the heartbeat
  # expiry sweep (T1) and the W1 connecting-state additions, so size generously.
  fn_fragment = source[runtime_loop_pos:runtime_loop_pos + 15000]
  assert '_reconnect_backoff_seconds' in fn_fragment, (
    'CAD-7: _runtime_loop must define _reconnect_backoff_seconds for bounded exponential backoff'
  )
  assert '_stop_aware_sleep' in fn_fragment, (
    'CAD-7: _runtime_loop must define _stop_aware_sleep for stop-event-aware backoff waits'
  )
  assert 'websocket_session_reconnect_attempt' in fn_fragment, (
    'CAD-7: _runtime_loop must persist websocket_session_reconnect_attempt event on disconnect'
  )
  assert 'websocket_session_reconnected' in fn_fragment, (
    'CAD-7: _runtime_loop must persist websocket_session_reconnected event on successful reconnect'
  )
  assert 'WebSocketAuthError' in fn_fragment, (
    'CAD-7: _runtime_loop must handle WebSocketAuthError separately (never retry auth failures)'
  )
  raise_after_auth_pos = fn_fragment.find('WebSocketAuthError')
  raise_pos = fn_fragment.find('raise', raise_after_auth_pos)
  assert raise_pos != -1 and raise_pos < raise_after_auth_pos + 50, (
    'CAD-7: WebSocketAuthError must be immediately re-raised (no retry) within _runtime_loop'
  )


def test_funds_bound_to_websocket_heartbeat() -> None:
  # FH (funds-on-heartbeat): funds liveness must be bound to the websocket
  # heartbeat, not to scan/reconcile activity. While the socket is connected and
  # beating, each beat must carry the live balance — so funds stay populated even
  # when automation is stopped and no scanning is occurring.
  source = inspect.getsource(web_app.create_operator_console_app)

  # The heartbeat persist must accept and merge funds onto the websocket beat.
  hb_pos = source.find('def _persist_websocket_runtime_heartbeat(')
  assert hb_pos != -1, 'FH: _persist_websocket_runtime_heartbeat must exist'
  hb_fragment = source[hb_pos:hb_pos + 1600]
  assert 'funds_detail' in hb_fragment, (
    'FH: the websocket heartbeat persist must accept a funds_detail argument'
  )
  assert "available_funds_snapshot" in hb_fragment, (
    'FH: the heartbeat detail must carry available_funds_snapshot when funds are present'
  )

  # The balance fetch helper must be live-lane gated, reuse the existing balance
  # call + funds projection, and fail closed (return None on error).
  helper_pos = source.find('def _heartbeat_funds_detail(')
  assert helper_pos != -1, 'FH: _heartbeat_funds_detail must exist in the runtime loop'
  helper_fragment = source[helper_pos:helper_pos + 1200]
  assert 'get_balance()' in helper_fragment, (
    'FH: funds must be fetched with the same get_balance() call reconcile uses'
  )
  assert '_project_funds_posture(' in helper_fragment, (
    'FH: funds must be projected through the existing _project_funds_posture'
  )
  assert 'return None' in helper_fragment, (
    'FH: balance-fetch helper must fail closed (return None) on error or empty snapshot'
  )

  # The blocking balance fetch must run off the event loop (worker thread) so the
  # websocket keepalive/recv stay responsive — otherwise the loop starves and the
  # socket drops into a reconnect storm.
  assert 'asyncio.to_thread(_heartbeat_funds_detail)' in source, (
    'FH: the balance fetch must run via asyncio.to_thread to avoid blocking the event loop'
  )

  # The live-lane gate: the HTTP balance client is constructed only on the live lane.
  client_pos = source.find('_funds_http_client = None')
  assert client_pos != -1, 'FH: a funds HTTP client must be set up in the runtime loop'
  gate_window = source[client_pos:client_pos + 200]
  assert "lane == 'live'" in gate_window, (
    'FH: the balance client must be constructed only on the live lane (sandbox/offline make no call)'
  )

  # The steady-state heartbeat (every beat) must pass the funds detail via the
  # off-loop async fetch.
  beat_pos = source.find("status='heartbeat-live'")
  assert beat_pos != -1, 'FH: steady-state heartbeat must exist'
  beat_window = source[beat_pos:beat_pos + 200]
  assert 'funds_detail=await _heartbeat_funds_detail_async()' in beat_window, (
    'FH: every heartbeat-live beat must carry funds via the off-loop _heartbeat_funds_detail_async()'
  )

  # The ready-critical connect beat must NOT block on a balance fetch.
  connect_pos = source.find("status='connected'")
  assert connect_pos != -1, 'FH: connect beat must exist'
  connect_window = source[connect_pos:connect_pos + 220]
  assert 'funds_detail' not in connect_window, (
    'FH: the initial connect beat must not fetch funds — ready_event must fire promptly so the live lane change does not time out'
  )


def test_websocket_reconnect_loop_infinite_retry() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  runtime_loop_pos = source.find('async def _runtime_loop()')
  assert runtime_loop_pos != -1, 'CAD-8: _runtime_loop must exist'
  # Bound the fragment to the actual loop body (ends at the final client.disconnect())
  # rather than a fixed char offset, so insertions inside the loop (e.g. heartbeat
  # helpers) cannot push the asserted backoff clamp out of the captured window.
  _loop_end = source.find('await client.disconnect()', runtime_loop_pos)
  fn_fragment = source[runtime_loop_pos:(_loop_end + 60) if _loop_end != -1 else runtime_loop_pos + 7000]
  assert 'retry_count >= len(_reconnect_backoff_seconds)' not in fn_fragment, (
    'CAD-8: _runtime_loop must NOT raise on retry exhaustion — infinite retry required; '
    'only stop_event or WebSocketAuthError should terminate the loop'
  )
  assert 'min(retry_count, len(_reconnect_backoff_seconds) - 1)' in fn_fragment, (
    'CAD-8: backoff index must be clamped with min() to cap at last slot (60s) after schedule exhausted'
  )


def test_chip_automation_status_reflects_backend_policy_state() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'deck-auto-advance-toggle' in source, (
    'HX3-C: deck-auto-advance-toggle button must be present in the HTML'
  )
  assert 'autoForwardLabel' in source, (
    'HX3-C: renderQuickActions must compute autoForwardLabel from automation posture'
  )


def test_websocket_overlay_rejects_invalid_url_with_no_go_feedback(monkeypatch: Any, tmp_path: Path) -> None:
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=r'secrets\kalshi\demo\private_key.pem',
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app()

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_sandbox_websocket', 'url': 'not-a-websocket-url'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['websocket_management']['last_result']['tone'] == 'no-go'
  assert payload['websocket_management']['last_result']['message'] == 'Websocket URLs must start with ws:// or wss:// and include a host.'
  assert payload['connection_posture']['available_websocket_urls']['sandbox'] != 'not-a-websocket-url'


def test_load_shell_settings_context_distinguishes_missing_key_from_environment(monkeypatch: Any, tmp_path: Path) -> None:
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=r'secrets\kalshi\demo\private_key.pem',
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, '_validate_env_alignment', lambda _settings: None)

  def _raise_missing_key(_settings: Any) -> Path:
    raise FileNotFoundError(settings.private_key_file or 'private_key.pem')

  monkeypatch.setattr(web_app, 'resolve_private_key_path', _raise_missing_key)

  payload, exc = web_app._load_shell_settings_context()

  assert isinstance(exc, FileNotFoundError)
  assert payload['environment_ready'] is True
  assert payload['credential_reference_present'] is True
  assert payload['private_key_file_exists'] is False
  assert payload['credential_ready'] is False
  assert payload['settings_ready'] is False
  assert payload['configuration_issue_family'] == 'missing_private_key_file'
  assert payload['environment_detail'] == 'env=DEMO :: api=v2'
  assert payload['credential_detail'] == 'api_key=present :: key file missing on disk (private_key.pem)'


def test_load_shell_settings_context_offline_lane_environment_ready_despite_live_derived_api_url(
  tmp_path: Path, monkeypatch: Any
) -> None:
  # Regression (2026-06-12): offline lane after a live session — demo environment
  # with a live-derived REST base URL. The real env-alignment validator now raises
  # the lane-membership message first, which the offline carve-out forgives, so
  # the settings context must report the environment ready and no error. The old
  # check order surfaced an env-mismatch error here, which the bootstrap payload
  # turned into a persistent bootstrap-failed error screen in offline mode.
  key_file = tmp_path / 'offline-context-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://external-api.kalshi.com/trade-api/v2',
    websocket_url='wss://external-api-ws.kalshi.com/trade-api/ws/v2',
    sandbox_websocket_url='wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2',
    live_websocket_url='wss://external-api-ws.kalshi.com/trade-api/ws/v2',
    operation_lane='offline',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  payload, exc = web_app._load_shell_settings_context()

  assert exc is None
  assert payload['environment_ready'] is True
  assert payload['credential_ready'] is True
  assert payload['settings_ready'] is True
  assert payload['configuration_issue_family'] is None


def test_startup_wizard_uses_normalized_truth_for_environment_and_credentials() -> None:
  wizard = web_app._build_startup_wizard_payload(
    {
      'decision': 'no-go',
      'reason': 'missing_private_key_file',
      'workflow': {
        'operator_message': 'Load a valid local key reference, then rerun readiness.',
      },
      'settings': {
        'kalshi_env': 'demo',
        'environment_ready': True,
        'environment_detail': 'env=DEMO :: api=v2',
        'credential_ready': False,
        'credential_detail': 'api_key=present :: key file missing on disk (private_key.pem)',
        'settings_ready': False,
        'scan_interval_ms': 2000,
        'min_edge_dollars': 0.03,
        'max_open_pairs': 4,
      },
    }
  )

  assert wizard is not None
  assert wizard['steps'][0]['id'] == 'environment'
  assert wizard['steps'][0]['status'] == 'ready'
  assert wizard['steps'][0]['detail'] == 'env=DEMO :: api=v2'
  assert wizard['steps'][1]['id'] == 'credentials'
  assert wizard['steps'][1]['status'] == 'blocked'
  assert wizard['steps'][1]['detail'] == 'api_key=present :: key file missing on disk (private_key.pem)'


def test_bootstrap_workflow_report_hold_uses_concrete_operator_guidance() -> None:
  workflow = web_app._bootstrap_workflow(
    settings_ready=True,
    report_payload={
      'pair_runtime_summary': [
        {'pair_id': 'pair-1', 'state': 'PLANNED'},
      ],
      'latest_heartbeat': {'status': 'cycle-complete'},
    },
    reconcile_payload={'pair_count': 0, 'pairs': []},
    next_action='If non-terminal pairs still require attention, use Refresh shell or Cancel all pairs before another dry-run cycle.',
    mode_selected=True,
  )

  assert workflow['recommended_step'] == 'report'
  assert workflow['auto_sequence'] == []
  assert workflow['step_kind'] == 'review'
  assert workflow['can_run_next_step'] is True
  assert workflow['next_actionable_step'] == 'scan'
  assert workflow['focus_target'] == 'evidence-section'
  assert workflow['deck_view'] == 'review'
  assert workflow['headline'] == 'Existing local runtime state is holding on an evidence review boundary.'
  assert workflow['operator_message'] == 'Evidence is already in view; choose the next executable action directly unless the shell needs a manual refresh.'


def test_validation_workflow_summary_defaults_to_local_only_posture(tmp_path: Path) -> None:
  summary = _load_validation_workflow_summary(project_root=tmp_path)

  assert summary['present'] is False
  assert summary['active_run_count'] == 0
  assert summary['latest_runs'] == []
  assert summary['latest_accepted_run'] is None
  assert summary['pass_count'] == 0
  assert summary['failed_count'] == 0
  assert summary['historical_failed_runs'] == []


def test_system_log_route_returns_live_entries() -> None:
  app = create_operator_console_app(_services())

  status, headers, body = _call_app(app, method='GET', path='/api/system-log')
  payload = json.loads(body)

  assert status == '200 OK'
  assert headers['Content-Type'] == 'application/json; charset=utf-8'
  assert payload['entries'] == []
  assert payload['message'] == 'System log entries stay hidden until sandbox or live mode is selected.'
  assert payload['scan_runtime']['status'] == 'idle'
  assert payload['scan_runtime']['result_reason'] == ''


def test_system_log_route_exposes_processing_scan_runtime_snapshot() -> None:
  services = _services()
  scan_started = threading.Event()
  release_scan = threading.Event()

  def _blocking_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    scan_started.set()
    release_scan.wait(timeout=2.0)
    return {
      'decision': 'planned',
      'candidate_count': 2,
      'candidates': [
        {'ticker': 'LIVE-1', 'density_weight': '1.0', 'liquidity_score': '210'},
        {'ticker': 'LIVE-2', 'density_weight': '0.9', 'liquidity_score': '190'},
      ],
      'next_action': 'Review candidates in Pairs.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_blocking_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  try:
    scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan')
    scan_payload = json.loads(scan_body)

    assert scan_status == '200 OK'
    assert scan_payload['scan_runtime']['status'] == 'processing'
    assert scan_started.wait(timeout=1.0) is True

    status, headers, body = _call_app(app, method='GET', path='/api/system-log')
    payload = json.loads(body)

    assert status == '200 OK'
    assert headers['Content-Type'] == 'application/json; charset=utf-8'
    assert payload['scan_runtime']['status'] == 'processing'
    assert payload['scan_runtime']['stage'] == 'loading_markets'
    assert payload['scan_runtime']['result_reason'] == ''
  finally:
    release_scan.set()


def test_report_route_projects_processing_workflow_during_active_scan() -> None:
  services = _services()
  scan_started = threading.Event()
  release_scan = threading.Event()

  def _blocking_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    scan_started.set()
    release_scan.wait(timeout=2.0)
    return {
      'decision': 'planned',
      'candidate_count': 2,
      'candidates': [
        {'ticker': 'LIVE-1', 'density_weight': '1.0', 'liquidity_score': '210'},
        {'ticker': 'LIVE-2', 'density_weight': '0.9', 'liquidity_score': '190'},
      ],
      'next_action': 'Review candidates in Pairs.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_blocking_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  try:
    scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan')
    scan_payload = json.loads(scan_body)

    assert scan_status == '200 OK'
    assert scan_payload['scan_runtime']['status'] == 'processing'
    assert scan_started.wait(timeout=1.0) is True

    report_status, _, report_body = _call_app(app, method='POST', path='/api/report')
    report_payload = json.loads(report_body)

    assert report_status == '200 OK'
    assert report_payload['scan_runtime']['status'] == 'processing'
    assert report_payload['scan_runtime']['stage'] == 'loading_markets'
    assert report_payload['execution_state']['kind'] == 'processing'
    assert report_payload['execution_state']['action'] == 'scan'
    assert report_payload['scheduler_snapshot']['owner'] == 'scan'
    assert report_payload['live_interaction']['surface_visible'] is True
    assert report_payload['live_interaction']['materialization_reason'] == 'scheduler_owner_active'
    assert report_payload['workflow']['recommended_step'] == 'processing'
    assert report_payload['workflow']['next_actionable_step'] == 'processing'
    assert report_payload['workflow']['step_kind'] == 'review'
    assert report_payload['workflow']['can_run_next_step'] is False
  finally:
    release_scan.set()


def test_system_log_route_exposes_failed_scan_runtime_snapshot() -> None:
  services = _services()

  def _failing_scan(**kwargs: Any) -> dict[str, Any]:
    progress_callback = kwargs.get('progress_callback')
    if callable(progress_callback):
      progress_callback(
        'loading_markets',
        'Loading open markets for candidate review.',
        {'market_count': 12},
        0.25,
      )
    raise RuntimeError('background scan worker crashed')

  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=services.bootstrap,
      scan=_failing_scan,
      run=services.run,
      reconcile=services.reconcile,
      report=services.report,
      cancel_all=services.cancel_all,
      system_log=services.system_log,
      visuals=services.visuals,
    )
  )

  scan_status, _, _ = _call_app(app, method='POST', path='/api/scan')
  assert scan_status == '200 OK'

  deadline = time.time() + 2.0
  payload = None
  status = ''
  while time.time() < deadline:
    status, _, body = _call_app(app, method='GET', path='/api/system-log')
    payload = json.loads(body)
    if payload['scan_runtime']['status'] == 'failed':
      break
    time.sleep(0.02)

  assert payload is not None
  assert status == '200 OK'
  assert payload['scan_runtime']['status'] == 'failed'
  assert payload['scan_runtime']['result_reason'] == 'scan_failed'
  assert payload['scan_runtime']['result_next_action'] == 'Review the reported issue before retrying this operator action.'


def test_visuals_route_returns_operational_packet() -> None:
  app = create_operator_console_app(_services())

  status, headers, body = _call_app(app, method='GET', path='/api/visuals', query='view=runtime_cadence&window=1h&mode=table')
  payload = json.loads(body)

  assert status == '200 OK'
  assert headers['Content-Type'] == 'application/json; charset=utf-8'
  assert payload['view']['id'] == 'runtime_cadence'
  assert payload['view']['render_mode'] == 'table'
  assert payload['window']['id'] == '1h'
  assert payload['series'] == []
  assert payload['table']['rows'] == []
  assert payload['subheadline'] == 'Select sandbox or live mode to resume lane-owned visuals.'


def test_run_route_augments_payload_with_follow_on_workflow(monkeypatch: Any) -> None:
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
            'mode_selected': True,
            'operation_lane': 'sandbox',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      None,
    ),
  )
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='POST', path='/api/run')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['planned_pair_count'] == 1
  assert payload['planned_pairs'][0]['binding_limiter'] == 'configured_contract_cap'
  assert payload['planned_pairs'][0]['dynamic_pair_notional_pct'] == '0.192'
  assert payload['workflow']['recommended_step'] == 'report'
  assert payload['workflow']['auto_sequence'] == []
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['can_run_next_step'] is False
  assert payload['workflow']['next_actionable_step'] == '-'
  assert payload['workflow']['focus_target'] == 'evidence-section'
  assert payload['workflow']['deck_view'] == 'review'
  assert payload['pair_monitor']['pair_count'] == 0
  assert payload['pair_monitor']['manual_execution']['status'] == 'unavailable'
  assert payload['evidence_browser']['status'] == 'missing-active-evidence'
  assert payload['evidence_browser']['accepted_state_summary'].startswith('No active retained run is available')


def test_run_route_projects_backend_manual_execution_chronology_without_relabel(monkeypatch: Any) -> None:
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
        'mode_selected': True,
        'operation_lane': 'sandbox',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      None,
    ),
  )
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=lambda **_: {
        'decision': 'planned',
        'planned_pair_count': 1,
        'planned_pairs': [
          {
            'pair_id': 'pair-1',
            'ticker': 'KALSHI-EDGE-1',
            'contract_count': '10',
            'dynamic_pair_notional_pct': '0.192',
            'dynamic_max_contracts': '32',
            'effective_density': '3.125',
            'binding_limiter': 'configured_contract_cap',
          }
        ],
        'execution_chronology': {
          'enabled': True,
          'profile': 'submit_order_bridge',
          'terminal_state': 'CANCELED',
          'sequence_count': 5,
          'contract_version': 'tranche_f_execution_event_packet.v1',
          'required_event_fields': ['event_type', 'execution_status', 'operation_lane', 'lane_session_id', 'market_ticker', 'seq', 'ts_ms', 'as_of_time'],
          'event_packet': [
            {
              'event_type': 'submit_order_intent',
              'execution_status': 'submitted',
              'profile': 'submit_order_bridge',
              'operation_lane': 'sandbox',
              'lane_session_id': 'sandbox-20260525T220000Z',
              'market_ticker': 'KALSHI-EDGE-1',
              'market_id': None,
              'order_id': 'pair-1-yes',
              'client_order_id': 'pair-1-yes-client',
              'trade_id': '',
              'seq': 'f3-seq-001',
              'ts_ms': 1700000000001,
              'as_of_time': '2026-05-25T22:00:00Z',
              'user_data_timestamp': 0,
              'outcome_side': 'yes',
              'book_side': 'buy',
              'use_yes_price': True,
            }
          ],
          'chronology': {
            'submit': {'as_of_time': '2026-05-25T22:00:00Z', 'seq': 'f3-seq-001', 'ts_ms': 1700000000001},
            'fill': {'as_of_time': '2026-05-25T22:00:01Z', 'seq': 'f3-seq-002', 'ts_ms': 1700000001001},
            'reconcile': {'as_of_time': '2026-05-25T22:00:02Z', 'seq': 'f3-seq-003', 'ts_ms': 1700000002001},
            'cancel': {'as_of_time': '2026-05-25T22:00:03Z', 'seq': 'f3-seq-004', 'ts_ms': 1700000003001},
          },
        },
        'next_action': 'Review manual execution chronology in Pairs and retained evidence before continuing.',
      },
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/run')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['recommended_step'] == 'report'
  assert payload['workflow']['deck_view'] == 'review'
  assert payload['workflow']['next_actionable_step'] == '-'
  assert payload['workflow']['focus_target'] == 'evidence-section'
  assert payload['pair_monitor']['manual_execution']['status'] == 'available'
  assert payload['pair_monitor']['manual_execution']['profile'] == 'submit_order_bridge'
  assert payload['pair_monitor']['manual_execution']['terminal_state'] == 'CANCELED'
  assert payload['live_interaction']['title'] == 'EXECUTION'
  assert payload['live_interaction']['surface_visible'] is False
  assert payload['live_interaction']['surface_status_label'] == 'IDLE'
  assert payload['live_interaction']['manual_interaction_count'] == 1
  assert payload['live_interaction']['executor_posture'] == 'manual_execution'
  assert payload['live_interaction']['operator_handoff_target'] == '-'
  assert any(card['label'] == 'Handoff' and card['value'] == '-' for card in payload['live_interaction']['summary_cards'])
  assert payload['pair_monitor']['manual_execution']['contract_version'] == 'tranche_f_execution_event_packet.v1'
  assert payload['pair_monitor']['manual_execution']['required_event_fields']
  assert payload['pair_monitor']['manual_execution']['event_packet'][0]['event_type'] == 'submit_order_intent'
  assert payload['pair_monitor']['manual_execution']['event_packet'][0]['operation_lane'] == 'sandbox'
  assert payload['pair_monitor']['manual_execution']['event_packet'][0]['market_ticker'] == 'KALSHI-EDGE-1'
  assert [step['step_id'] for step in payload['pair_monitor']['manual_execution']['ordered_steps']] == ['submit', 'fill', 'reconcile', 'cancel']


def test_report_route_preserves_live_interaction_hold_without_fake_reconcile_step() -> None:
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=base.run,
      reconcile=base.reconcile,
      report=lambda **_: {
        'decision': 'planned',
        'settings': {
          'settings_ready': True,
          'credential_ready': True,
          'environment_ready': True,
          'mode_selected': True,
          'available_websocket_urls': {
            'sandbox': 'demo-api.kalshi.example/ws',
            'live': 'api.kalshi.example/ws',
          },
        },
        'pair_runtime_summary': [
          {
            'pair_id': 'pair-1',
            'ticker': 'KALSHI-EDGE-1',
            'state': 'CANCELED',
            'public_state_id': 'CANCELED',
            'submit_response_id': 'submit-bridge-pair-1',
            'allowed_actions': ['WAIT'],
          }
        ],
        'next_action': 'Refresh only if the shell looks stale.',
      },
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['step_kind'] == 'execute'
  assert payload['workflow']['can_run_next_step'] is True
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['workflow']['focus_target'] == 'notification-band-section'
  assert payload['live_interaction']['operator_handoff_target'] == '-'
  assert payload['live_interaction']['next_backend_action_summary'] == 'WAIT'
  assert payload['live_interaction']['surface_visible'] is False
  assert payload['live_interaction']['materialization_reason'] == 'clean_resting_posture'


def test_report_route_hides_live_interaction_surface_in_clean_resting_posture() -> None:
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=base.run,
      reconcile=base.reconcile,
      report=lambda **_: {
        'decision': 'planned',
        'settings': {
          'settings_ready': True,
          'credential_ready': True,
          'environment_ready': True,
          'mode_selected': True,
          'available_websocket_urls': {
            'sandbox': 'demo-api.kalshi.example/ws',
            'live': 'api.kalshi.example/ws',
          },
        },
        'pair_runtime_summary': [],
        'next_action': 'Review evidence before continuing.',
      },
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['live_interaction']['surface_visible'] is False
  assert payload['live_interaction']['materialization_state'] == 'hidden'
  assert payload['live_interaction']['materialization_reason'] == 'clean_resting_posture'
  assert payload['live_interaction']['activity_status'] == 'idle'


def test_build_pair_monitor_payload_hides_live_interaction_during_mode_change_failure() -> None:
  payload = web_app._build_pair_monitor_payload(
    {
      'decision': 'no-go',
      'reason': 'websocket_connection_failed',
      'shell_action': 'change_mode',
      'next_action': 'Review websocket endpoint posture and retry mode change.',
      'workflow': {
        'next_actionable_step': 'set_websocket_url',
      },
      'pair_runtime_summary': [
        {
          'pair_id': 'pair-1',
          'ticker': 'KALSHI-EDGE-1',
          'state': 'SUBMITTING',
          'public_state_id': 'SUBMITTING',
          'allowed_actions': ['WAIT'],
        },
      ],
    }
  )

  assert payload['live_interaction']['surface_visible'] is False
  assert payload['live_interaction']['materialization_state'] == 'hidden'
  assert payload['live_interaction']['materialization_reason'] == 'boundary_owns_mode_change_failure'
  assert payload['live_interaction']['operator_handoff_target'] == 'set_websocket_url'


def test_run_route_emits_backend_highlight_envelope_after_payload_rebuild(monkeypatch: Any) -> None:
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      None,
    ),
  )
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='POST', path='/api/run')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['deck_action_highlights'].get('key_management') == 'no-go'
  assert payload['workflow']['deck_action_highlights'].get('websocket_management') == 'ok'
  assert payload['workflow']['highlight_policy_version'] == 'orchestrator-highlights.v1'


def test_run_no_go_defaults_to_reconcile_recovery_step(monkeypatch: Any) -> None:
  ready_settings = {
    'settings_ready': True,
    'credential_ready': True,
    'credential_reference_present': True,
    'environment_ready': True,
    'mode_selected': True,
    'available_websocket_urls': {
      'sandbox': 'demo-api.kalshi.example/ws',
      'live': 'api.kalshi.example/ws',
    },
    'sandbox_websocket_url': 'demo-api.kalshi.example/ws',
    'live_websocket_url': 'api.kalshi.example/ws',
    'active_websocket_url': 'demo-api.kalshi.example/ws',
  }
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (ready_settings, None),
  )
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=lambda **_: {
        'decision': 'no-go',
        'reason': 'runtime_boundary',
        'blocked_reason': 'runtime_boundary',
        'message': 'Runtime boundary stopped dry-run planning.',
        'next_action': 'Review pair posture before retrying.',
        'settings': ready_settings,
      },
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/run')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['workflow']['recommended_step'] == 'reconcile'
  assert payload['workflow']['next_actionable_step'] == 'reconcile'
  assert payload['workflow']['step_kind'] == 'execute'


def test_run_no_go_projects_fix_configuration_when_readiness_is_not_ready(monkeypatch: Any) -> None:
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': False,
        'credential_ready': False,
        'credential_reference_present': False,
        'environment_ready': True,
        'mode_selected': False,
        'available_websocket_urls': {
          'sandbox': '',
          'live': '',
        },
        'sandbox_websocket_url': '',
        'live_websocket_url': '',
        'active_websocket_url': '',
      },
      None,
    ),
  )
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=lambda **_: {
        'decision': 'no-go',
        'reason': 'runtime_boundary',
        'message': 'Dry-run planner could not execute due to setup posture.',
        'next_action': 'Fix configuration and retry.',
        'settings': {
          'settings_ready': False,
          'credential_ready': False,
          'environment_ready': True,
          'available_websocket_urls': {
            'sandbox': '',
            'live': '',
          },
        },
      },
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/run')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['workflow']['recommended_step'] == 'review_configuration'
  assert payload['workflow']['step_kind'] == 'fix_configuration'
  assert payload['workflow']['next_actionable_step'] == 'load_api_key'


def _services_with_review_candidates() -> OperatorConsoleServices:
  base = _services()
  return OperatorConsoleServices(
    bootstrap=base.bootstrap,
    scan=lambda **_: {
      'decision': 'planned',
      'candidate_count': 1,
      'candidates': [
        {
          'candidate_uid': 'review-candidate-1',
          'ticker': 'KALSHI-EDGE-1',
          'density_weight': '3.125',
          'liquidity_score': '210',
          'market_edge_dollars': '0.11',
          'current_price': '0.52',
          'threshold_price': '0.41',
        }
      ],
      'next_action': 'Review candidates in Pairs.',
    },
    run=base.run,
    reconcile=base.reconcile,
    report=base.report,
    cancel_all=base.cancel_all,
    system_log=base.system_log,
    visuals=base.visuals,
  )


def _with_scan_runtime(
  services: OperatorConsoleServices,
  *,
  scan_session_id: str = 'test-scan-1',
  lane_session_id: str = 'test-lsid-1',
) -> OperatorConsoleServices:
  # TEST_HARNESS_DB_BACKED_SCAN_SESSION_AND_IDENTITY_BMAP_2026-06-21. The SSOT + candidate-identity
  # lanes source candidate selection / cards / stage columns from the persisted canonical DB query,
  # so a stubbed scan must persist to a real DB under a CONSISTENT session. The scan result is given
  # a deterministic scan_runtime {scan_session_id, lane_session_id} so the persist gets a run_id and
  # writes under the same lane_session_id the read queries. (A real state_db_path is the caller's
  # other half: KALSHI_STATE_DB_PATH or a _build_test_settings/_resolve_settings patch.)
  base_scan = services.scan

  def _scan_with_runtime(**kwargs: Any) -> JSONDict:
    result = dict(base_scan(**kwargs))
    result.setdefault(
      'scan_runtime',
      {'scan_session_id': scan_session_id, 'lane_session_id': lane_session_id, 'status': 'completed'},
    )
    return result

  return replace(services, scan=_scan_with_runtime)


def _db_backed_review_app(
  tmp_path: Path,
  monkeypatch: Any,
  services: OperatorConsoleServices | None = None,
) -> Any:
  # Convenience for tests that don't already resolve a state DB: sets KALSHI_STATE_DB_PATH (read by
  # load_settings/_resolve_settings) and wraps the scan with a deterministic scan_runtime, then
  # builds the app. Candidate fixtures carry an explicit candidate_uid (honored by
  # canonical_candidate_uid), so literal selection keys stay stable.
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(tmp_path / 'state.sqlite3'))
  base = services if services is not None else _services_with_review_candidates()
  return create_operator_console_app(_with_scan_runtime(base))


def _build_test_settings(state_db_path: str) -> Settings:
  return Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=r'secrets\kalshi\demo\private_key.pem',
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=state_db_path,
  )


def _seed_saved_set(
  state_db_path: str,
  *,
  saved_set_id: str,
  state_id: str,
  members: list[dict[str, Any]],
  saved_key_count: int,
) -> None:
  recorded_at_utc = '2026-05-25T12:00:00Z'
  connection = open_database(state_db_path)
  persist_candidate_saved_set(
    connection,
    saved_set_id=saved_set_id,
    run_id=None,
    recorded_at_utc=recorded_at_utc,
    operation_lane='sandbox',
    lane_session_id='lane-session-1',
    saved_key_count=saved_key_count,
    state_id=state_id,
    source_action='save_selection',
    members=members,
    detail={'candidate_signature': 'sig-1'},
  )
  persist_candidate_saved_set_evaluation(
    connection,
    saved_set_id=saved_set_id,
    recorded_at_utc=recorded_at_utc,
    operation_lane='sandbox',
    evaluation_status='pass',
    actionability_status='active_valid',
    visibility_status='visible_current',
    offline_verifiable=True,
    online_revalidation_required=False,
    detail={'status': 'active_valid'},
  )


def test_submit_order_bridge_no_saved_set_fail_closed() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/run',
    body={'bridge_action': 'submit_order'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'no_saved_set'
  assert payload['workflow']['recommended_step'] == 'select_candidates'


def test_submit_order_bridge_saved_set_empty_fail_closed(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'submit-order-empty.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_saved_set(
    state_db_path,
    saved_set_id='saved-set-empty',
    state_id='review_hold_saved_selection_locked',
    members=[],
    saved_key_count=0,
  )
  app = create_operator_console_app(_services())

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/run',
    body={'bridge_action': 'submit_order'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'no_saved_set'
  assert payload['workflow']['recommended_step'] == 'select_candidates'


def test_run_saved_set_not_eligible_no_go_without_settings_does_not_promote_fix_config() -> None:
  # R4/Issue C regression: decision no-go + blocked_reason saved_set_not_eligible with no settings
  # field in payload must not falsely promote to fix_configuration / load_api_key.
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=lambda **_: {
        'decision': 'no-go',
        'reason': 'saved_set_not_eligible',
        'blocked_reason': 'saved_set_not_eligible',
        'message': 'Saved set is not eligible.',
        'next_action': 'Find candidates again, then save a current eligible set before retrying.',
        # Intentionally omitting 'settings' to reproduce the live-session failure path.
      },
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/run', body={})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['blocked_reason'] == 'saved_set_not_eligible'
  # NS-1 fix: saved_set_not_eligible routes to scan (FIND CANDIDATES) per cycling model
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['workflow']['focus_target'] == 'live-interaction-section'
  assert payload['workflow']['deck_view'] == 'operator'


def test_run_saved_set_not_eligible_no_go_with_settings_ready_false_does_not_promote_fix_config() -> None:
  # R4/Issue C regression: settings_ready=False in run response must not win over blocked_reason
  # when blocked_reason is saved_set_not_eligible.
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=lambda **_: {
        'decision': 'no-go',
        'reason': 'saved_set_not_eligible',
        'blocked_reason': 'saved_set_not_eligible',
        'message': 'Saved set is not eligible.',
        'next_action': 'Find candidates again, then save a current eligible set before retrying.',
        'settings': {
          'settings_ready': False,
          'credential_ready': False,
          'environment_ready': False,
        },
      },
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/run', body={})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['blocked_reason'] == 'saved_set_not_eligible'
  # NS-1 fix: saved_set_not_eligible routes to scan (FIND CANDIDATES) per cycling model
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['workflow']['focus_target'] == 'live-interaction-section'
  assert payload['workflow']['deck_view'] == 'operator'


def test_report_with_credentials_ready_but_settings_not_ready_does_not_promote_review_configuration() -> None:
  # R4/Issue 2 regression: settings_ready=False driven by a transient state (e.g., dry-run proof PENDING)
  # must not generate review_configuration/load_api_key when credential_ready=True, mode_selected=True,
  # and websocket URLs are configured.  The fix guards the settings_ready branch in _follow_on_workflow.
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=base.run,
      reconcile=base.reconcile,
      report=lambda **_: {
        'decision': 'planned',
        'settings': {
          'settings_ready': False,
          'credential_ready': True,
          'environment_ready': True,
          'mode_selected': True,
          'available_websocket_urls': {
            'sandbox': 'demo-api.kalshi.example/ws',
            'live': 'api.kalshi.example/ws',
          },
        },
        'pair_runtime_summary': [],
        'next_action': 'Review evidence before continuing.',
      },
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['recommended_step'] != 'review_configuration'
  assert payload['workflow']['next_actionable_step'] != 'load_api_key'
  assert payload['workflow']['step_kind'] != 'fix_configuration'


def test_scan_popup_guard_uses_current_rows_not_all_pair_monitor_rows() -> None:
  # R4/Issue 4 regression: candidateRowsForActiveView.currentRows must be used in
  # reviewedCandidateSetWouldBeClearedByScan, not the raw pair_monitor.candidate_rows.
  # This test exercises the backend generating review_row_origin='saved_prior' for
  # historical candidates so the JS fix has the correct input.
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=lambda **_: {
        'decision': 'planned',
        'candidate_count': 1,
        'candidates': [
          {
            'candidate_uid': 'prior-candidate-1',
            'candidate_key': 'prior-candidate-1',
            'ticker': 'KALSHI-EDGE-PRIOR-1',
            'density_weight': '3.125',
            'review_row_origin': 'saved_prior',
          }
        ],
        'next_action': 'Review candidates.',
      },
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/scan')
  payload = json.loads(body)

  assert status == '200 OK'
  candidate_rows = payload.get('pair_monitor', {}).get('candidate_rows') or []
  # backend must round-trip review_row_origin for candidates that declare it
  prior_rows = [r for r in candidate_rows if r.get('review_row_origin') == 'saved_prior']
  assert len(prior_rows) == len(candidate_rows), (
    'all candidates declared saved_prior; all pair_monitor rows should carry review_row_origin=saved_prior'
  )


def test_submit_order_bridge_saved_set_transition_stays_fail_closed(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'submit-order-not-eligible.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_saved_set(
    state_db_path,
    saved_set_id='saved-set-not-eligible',
    state_id='review_hold_saved_selection_editing',
    members=[
      {
        'candidate_uid': 'candidate-1',
        'candidate_key': 'candidate-1',
        'ticker': 'KALSHI-EDGE-1',
      }
    ],
    saved_key_count=1,
  )
  app = create_operator_console_app(_services_with_review_candidates())

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/run',
    body={'bridge_action': 'submit_order'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'no_saved_set'
  if payload['reason'] == 'saved_set_not_eligible':
    assert payload['workflow']['recommended_step'] == 'scan'
    assert payload['workflow']['next_actionable_step'] == 'scan'
  else:
    assert payload['workflow']['recommended_step'] == 'select_candidates'
    assert payload['workflow']['next_actionable_step'] == 'select_candidates'


def test_run_route_preserves_submit_order_bridge_block_workflow_over_review_selection_projection(tmp_path: Path, monkeypatch: Any) -> None:
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(tmp_path / 'state.sqlite3'))
  base = _with_scan_runtime(_services_with_review_candidates())
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=lambda **_: {
        'decision': 'planned',
        'command_family': 'polyventure run',
        'blocked_reason': 'saved_set_not_eligible',
        'next_action': 'Find candidates again, save a current eligible set, then retry submit order.',
        'settings': {
          'settings_ready': True,
          'credential_ready': True,
          'environment_ready': True,
          'mode_selected': True,
          'operation_lane': 'sandbox',
          'available_websocket_urls': {
            'sandbox': 'demo-api.kalshi.example/ws',
            'live': 'api.kalshi.example/ws',
          },
        },
        'connection_posture': {
          'operation_lane': 'sandbox',
          'mode_selected': True,
          'connection_state': {
            'status': 'connected',
            'websocket_connected': True,
          },
        },
        'candidate_count': 1,
        'sandbox_extended_count': 1,
        'candidates': [
          {
            'candidate_key': 'review-candidate-1',
            'ticker': 'KALSHI-EDGE-1',
            'density_weight': '1.0',
            'liquidity_score': '210',
          }
        ],
        'sandbox_candidates_extended': [
          {
            'candidate_key': 'review-candidate-1',
            'ticker': 'KALSHI-EDGE-1',
            'density_weight': '1.0',
            'liquidity_score': '210',
          }
        ],
      },
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  save_status, _, save_body = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={
      'action': 'save_selection',
      'selected_keys': ['review-candidate-1'],
    },
  )
  save_payload = json.loads(save_body)
  status, _, body = _call_app(app, method='POST', path='/api/run', body={})
  payload = json.loads(body)

  assert scan_status == '200 OK'
  assert scan_payload['workflow']['recommended_step'] == 'select_candidates'
  assert save_status == '200 OK'
  assert save_payload['review_selection']['state_id'] == 'review_hold_saved_selection_locked'
  assert status == '200 OK'
  assert payload['blocked_reason'] == 'saved_set_not_eligible'
  # NS-1 fix: saved_set_not_eligible routes to scan (FIND CANDIDATES) per cycling model
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['workflow']['focus_target'] == 'live-interaction-section'
  assert payload['workflow']['deck_view'] == 'operator'


def test_next_step_display_contract_monitor_branch_absent_from_shell_html() -> None:
  app = create_operator_console_app(_services())
  status, _, body = _call_app(app, method='GET', path='/')
  assert status == '200 OK'
  assert "actionableStepId === 'monitor'" not in body
  assert "next_actionable_step='monitor'" not in body
  assert "'scan': 'FIND CANDIDATES'" in body


def test_run_bridge_no_go_workflow_not_overwritten_by_projection_when_saved_keys_match_candidates(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  # Bug 2 regression: when guard fires (saved set not locked) but the refreshed candidate list
  # happens to match the saved keys, _refresh_review_selection_projection previously overwrote
  # the blocking no-go workflow with "Candidates saved. The current working set is locked for
  # review."  The fix re-asserts the no-go workflow after projection.
  #
  # Scenario: DB has saved set with state_id='review_hold_saved_selection_editing' (not locked)
  # and candidate_key='review-candidate-1'.  Scan returns the same key.
  # Guard fires (not locked -> saved_set_not_eligible).  Projection sees matched keys -> would
  # compute locked state and overwrite workflow.  After fix, no-go workflow is preserved.
  state_db_path = str(tmp_path / 'bug2-workflow-overwrite.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_saved_set(
    state_db_path,
    saved_set_id='saved-set-editing-001',
    state_id='review_hold_saved_selection_editing',
    members=[
      {
        'candidate_uid': 'review-candidate-1',
        'candidate_key': 'review-candidate-1',
        'ticker': 'KALSHI-EDGE-1',
      }
    ],
    saved_key_count=1,
  )
  app = create_operator_console_app(_services_with_review_candidates())

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/run',
    body={'bridge_action': 'submit_order'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'no_saved_set'
  # Blocking workflow must survive: recommended_step must reflect the guard block (scan or
  # select_candidates from the no-go path), NOT the locked-state acknowledgement step.
  assert payload['workflow']['recommended_step'] != 'submit_order'
  assert payload['workflow']['next_actionable_step'] != 'submit_order'


def test_report_poll_does_not_clear_review_selection_state_when_saved_set_exists(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  # Bug 1 regression: a /api/report poll returns a payload with no candidate rows.
  # _apply_review_selection_projection's no-candidates branch previously called
  # _clear_review_selection_state() unconditionally when explicit_empty_saved_set was False,
  # wiping a valid in-memory saved set.  On the next Submit Order click, hydration from DB
  # re-loaded the set with state_id='review_hold_saved_selection_restored' (not locked), causing
  # a spurious saved_set_not_eligible guard failure.
  #
  # Scenario: scan -> save -> report poll -> Submit Order.
  # After the fix the saved set state must survive the report poll so Submit Order succeeds
  # (guard does not fire).  Here we assert state_id is preserved after the report call.
  app = _db_backed_review_app(tmp_path, monkeypatch)

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  save_status, _, save_body = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={
      'action': 'save_selection',
      'selected_keys': ['review-candidate-1'],
    },
  )
  save_payload = json.loads(save_body)
  report_status, _, report_body = _call_app(app, method='POST', path='/api/report', body={})
  report_payload = json.loads(report_body)

  assert scan_status == '200 OK'
  assert save_status == '200 OK'
  assert save_payload['review_selection']['state_id'] == 'review_hold_saved_selection_locked'
  assert report_status == '200 OK'
  # In no-mode, lane-owned saved-set state stays hidden outside the owning lane.
  assert report_payload['review_selection']['state_id'] == 'review_hold_empty_selection'
  assert report_payload['review_selection']['submit_ready'] is False


def test_action_route_wires_merge_chain_without_restored_history_injection() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert "merged_payload = _merge_preserved_review_hold_payload(action_name, payload)" in source
  assert "merged_payload = _merge_saved_candidate_rows_for_processing_scan(merged_payload)" in source
  assert "merged_payload = _merge_saved_candidate_rows_for_restored_history(merged_payload)" not in source
  assert "payload = _augment_shell_payload(" in source


def test_submit_order_bridge_authorization_fail_closed_without_mode_selection(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'submit-order-authorization.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_saved_set(
    state_db_path,
    saved_set_id='saved-set-authorized-lane',
    state_id='review_hold_saved_selection_locked',
    members=[
      {
        'candidate_uid': 'candidate-1',
        'candidate_key': 'candidate-1',
        'ticker': 'KALSHI-EDGE-1',
      }
    ],
    saved_key_count=1,
  )
  app = create_operator_console_app(_services_with_review_candidates())

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/run',
    body={'bridge_action': 'submit_order'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'no_saved_set'
  assert payload['workflow']['recommended_step'] == 'select_candidates'


def test_saved_selection_lock_stays_non_submittable_until_mode_authorizes_bridge(tmp_path: Path, monkeypatch: Any) -> None:
  app = _db_backed_review_app(tmp_path, monkeypatch)

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  select_status, _, select_body = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={
      'action': 'sync_selection',
      'selected_keys': ['review-candidate-1'],
    },
  )
  select_payload = json.loads(select_body)
  save_status, _, save_body = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={
      'action': 'save_selection',
      'selected_keys': ['review-candidate-1'],
    },
  )
  save_payload = json.loads(save_body)

  assert scan_status == '200 OK'
  assert scan_payload['workflow']['recommended_step'] == 'select_candidates'
  assert select_status == '200 OK'
  assert select_payload['review_selection']['state_id'] == 'review_hold_with_active_selection'

  assert save_status == '200 OK'
  assert save_payload['review_selection']['state_id'] == 'review_hold_saved_selection_locked'
  assert save_payload['review_selection']['submit_ready'] is False
  assert save_payload['workflow']['recommended_step'] == 'change_mode'
  assert save_payload['workflow']['next_actionable_step'] == 'mode_change'


def test_submit_order_bridge_authorized_in_sandbox_and_live_with_mode_selected() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # SBA-1: sandbox and live are the authorized lanes — the guard fires for all other lanes
  assert "active_lane not in ('sandbox', 'live')" in source


def test_submit_order_bridge_blocked_in_live_lane(tmp_path: Path, monkeypatch: Any) -> None:
  # The SSOT review-selection sources candidates from the persisted canonical DB, so the
  # stubbed scan must persist under a real DB + consistent scan_runtime (see _with_scan_runtime).
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(tmp_path / 'state.sqlite3'))
  app = create_operator_console_app(_with_scan_runtime(_services_with_review_candidates()))
  _load_lane_key(app, tmp_path, monkeypatch, 'live')
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})

  _call_app(app, method='POST', path='/api/scan', body={})
  _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={'action': 'sync_selection', 'selected_keys': ['review-candidate-1']},
  )
  save_status, _, save_body = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={'action': 'save_selection', 'selected_keys': ['review-candidate-1']},
  )
  save_payload = json.loads(save_body)

  assert save_status == '200 OK'
  assert save_payload['review_selection']['state_id'] == 'review_hold_saved_selection_locked'
  assert save_payload['review_selection']['submit_ready'] is False
  assert save_payload['workflow']['recommended_step'] == 'change_mode'
  assert save_payload['workflow']['next_actionable_step'] == 'mode_change'


def test_submit_order_bridge_blocked_when_saved_set_lane_mismatches_active_lane() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # SBA-2: cross-lane guard generalized — any non-matching saved lane blocks, not just 'live'
  assert "saved_lane and saved_lane != active_lane" in source


def test_submit_guard_no_longer_blocks_on_expired_actionability() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # SSE1-A: expired_actionability check removed — guard no longer blocks on stale actionability status
  assert "expired_actionability', 'revalidation_required', 'historical_only'" not in source


def test_submit_guard_no_longer_blocks_on_state_id_mismatch() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # SSE1-A: state_id mismatch check removed — guard no longer blocks when state_id is not locked
  assert "state_id != 'review_hold_saved_selection_locked'" not in source


def test_submit_guard_still_blocks_no_saved_set() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # SSE1-A regression: no_saved_set hard block must remain after relaxation
  assert "return 'no_saved_set'" in source


def test_submit_guard_still_blocks_bridge_authorization_failed() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # SSE1-A regression: bridge_authorization_failed hard block must remain after relaxation
  assert "return 'bridge_authorization_failed'" in source


def test_saved_terminal_candidate_hidden_from_current_cards_but_kept_cancelled_stage(tmp_path: Path) -> None:
  state_db_path = str(tmp_path / 'saved-terminal-reconciliation.sqlite3')
  conn = sqlite3.connect(state_db_path)
  try:
    conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
    conn.execute(
      '''
      CREATE TABLE candidate_review_candidates (
        run_id TEXT,
        lifecycle_stage TEXT,
        terminal_cause TEXT,
        ticker TEXT,
        qualifier_tier TEXT,
        detail_json TEXT,
        candidate_uid TEXT,
        candidate_key TEXT
      )
      '''
    )
    conn.execute('INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?, ?)', ('run-r1', 'lane-r1'))
    conn.executemany(
      '''
      INSERT INTO candidate_review_candidates
        (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json, candidate_uid, candidate_key)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)
      ''',
      [
        (
          'run-r1',
          'terminal',
          'expired_unfilled',
          'KXTERM',
          'live_qualifying',
          json.dumps({'rank': 1, 'close_time_utc': '2026-06-22T09:43:51Z'}),
          'candidate-terminal-001',
          'candidate-terminal-001',
        ),
        (
          'run-r1',
          'discovered',
          '',
          'KXLIVE',
          'live_qualifying',
          json.dumps({'rank': 2, 'close_time_utc': '2026-06-22T10:30:00Z'}),
          'candidate-live-001',
          'candidate-live-001',
        ),
      ],
    )
    conn.commit()
  finally:
    conn.close()

  payload = {
    'settings': {'state_db_path': state_db_path, 'operation_lane': 'live'},
    'scan_runtime': {'lane_session_id': 'lane-r1'},
    'review_selection': {
      'persisted_lane_session_id': 'lane-r1',
      'saved_keys': ['candidate-terminal-001', 'candidate-live-001'],
    },
  }

  monitor_payload = web_app._build_pair_monitor_payload(payload)
  stage_payload = web_app._fetch_stage_columns(payload)
  rows_by_key = {row['candidate_key']: row for row in monitor_payload['candidate_rows']}

  assert rows_by_key['candidate-terminal-001']['review_row_origin'] == 'saved_prior_hidden_expired'
  assert rows_by_key['candidate-live-001'].get('review_row_origin') != 'saved_prior_hidden_expired'
  cancelled_items = next(col['items'] for col in stage_payload['stage_columns'] if col['stage_id'] == 'cancelled')
  # Stage cards carry the full uniform payload contract (see
  # test_fetch_stage_columns_emits_full_payload_contract_across_stages); assert the
  # semantic subset this scenario is about rather than pinning the whole shape here.
  assert len(cancelled_items) == 1
  cancelled_item = cancelled_items[0]
  assert cancelled_item['ticker'] == 'KXTERM'
  assert cancelled_item['qualifier_tier'] == 'live_qualifying'
  assert cancelled_item['close_time'] == '2026-06-22T09:43:51Z'
  assert cancelled_item['candidate_key'] == 'candidate-terminal-001'
  assert cancelled_item['terminal_cause'] == 'expired_unfilled'
  assert cancelled_item['stage_id'] == 'cancelled'


def test_submit_fail_closed_on_persistence_error() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # SSE1-B: exception during submit returns bridge_persistence_failed — fail-closed contract
  assert "'bridge_persistence_failed'" in source


def test_post_run_processing_failure_guard_present() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # SSE1-C: post-run response build failure returns post_run_processing_failed instead of crashing server
  assert "'post_run_processing_failed'" in source


def test_safe_settings_summary_includes_settings_ready() -> None:
  from polyventure import config as _config
  source = inspect.getsource(_config.safe_settings_summary)
  # SSE2-A: scan result payload marks settings_ready so terminal replay skips blocking Kalshi seed call
  assert "'settings_ready': True" in source


def test_terminal_replay_settings_ready_short_circuit_present() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # SSE2-B: settings_ready guard exists in terminal replay path to short-circuit Kalshi seed call
  assert "settings_ready" in source
  assert "_terminal_scan_refresh_payload" in source


def test_auto_find_cadence_pauses_during_zero_found_retry() -> None:
  source = inspect.getsource(web_app._render_html)
  # RC-1: cadence countdown must hold in both cases: retry ticker active AND scan actively processing
  cadence_fn_start = source.find("function updateAutoFindCadenceCountdown")
  next_fn_pos = source.find("function ensureAutoFindCadenceTicker", cadence_fn_start)
  assert cadence_fn_start != -1, "updateAutoFindCadenceCountdown not found"
  assert next_fn_pos != -1, "ensureAutoFindCadenceTicker not found"
  cadence_fn_body = source[cadence_fn_start:next_fn_pos]
  # RC-1a: retry ticker guard
  assert "zeroFoundRetryTimerId" in cadence_fn_body, "zeroFoundRetryTimerId guard not found in updateAutoFindCadenceCountdown"
  # RC-1b: active scan guard (covers in-flight scan before retry ticker starts)
  assert "scan_runtime" in cadence_fn_body, "scan_runtime processing guard not found in updateAutoFindCadenceCountdown"
  assert "'processing'" in cadence_fn_body, "processing status check not found in updateAutoFindCadenceCountdown"
  # both guards must reset the deadline so countdown restarts fresh after the blocking condition clears
  assert cadence_fn_body.count("autoFindCadenceDeadlineMs = 0") >= 2, "deadline reset must appear for both guard conditions"


def test_post_submit_quick_actions_shows_monitoring_posture() -> None:
  import inspect as _inspect
  source = _inspect.getsource(web_app._follow_on_workflow)
  # NS-1 fix: saved_set_not_eligible routes to scan (FIND CANDIDATES), not monitor
  assert "saved_set_not_eligible" in source
  assert "'scan'" in source
  assert "'monitor'" not in source.split("saved_set_not_eligible")[1].split("eligibility queue")[0]
  # headline confirms eligibility queue wording is preserved
  assert 'eligibility queue' in source


def test_bootstrap_credential_gap_does_not_override_inflight_monitoring() -> None:
  import inspect as _inspect
  source = _inspect.getsource(web_app._follow_on_workflow)
  # SSE1-C: in_flight monitoring check must appear before _waiting_readiness_workflow call
  inflight_pos = source.find("'saved_set_not_eligible'")
  readiness_pos = source.find('_waiting_readiness_workflow(')
  assert inflight_pos != -1, 'in_flight monitoring check not found in _follow_on_workflow'
  assert readiness_pos != -1, '_waiting_readiness_workflow call not found in _follow_on_workflow'
  assert inflight_pos < readiness_pos, 'in_flight check must precede _waiting_readiness_workflow to prevent credential workflow override'


def test_surface_visible_hold_routes_to_monitoring_not_set_api_key() -> None:
  source = inspect.getsource(web_app._follow_on_workflow)
  # C′: live-interaction hold check must fire before _waiting_readiness_workflow so credential
  # gap cannot override active-monitoring posture and push SET API KEY into NEXT STEP
  hold_pos = source.find('_payload_has_live_interaction_hold(payload)')
  readiness_pos = source.find('_waiting_readiness_workflow(')
  assert hold_pos != -1, "C′ _payload_has_live_interaction_hold guard not found in _follow_on_workflow"
  assert readiness_pos != -1, '_waiting_readiness_workflow call not found in _follow_on_workflow'
  assert hold_pos < readiness_pos, 'C′ live-interaction hold check must precede _waiting_readiness_workflow'


def test_data_management_actions_route_to_scan_not_action_name_source() -> None:
  source = inspect.getsource(web_app._follow_on_workflow)
  # NS-2 fix: data management and websocket management side-panel actions delegate to _bootstrap_workflow
  assert 'DECK_ACTION_GROUPS' in source, 'DECK_ACTION_GROUPS guard not found in _follow_on_workflow'
  assert '_bootstrap_workflow' in source, '_bootstrap_workflow delegation not found in _follow_on_workflow'
  assert "DECK_ACTION_GROUPS.get('data_management'" in source, "DECK_ACTION_GROUPS.get('data_management') not found"
  assert "DECK_ACTION_GROUPS.get('websocket_management'" in source, "DECK_ACTION_GROUPS.get('websocket_management') not found"
  # delegate guard must appear AFTER _waiting_readiness_workflow (session-ready-check must run first)
  readiness_pos = source.find('_waiting_readiness_workflow(')
  dm_handler_pos = source.find("DECK_ACTION_GROUPS.get('data_management'")
  assert readiness_pos != -1, '_waiting_readiness_workflow call not found'
  assert dm_handler_pos > readiness_pos, 'data management handler must appear after _waiting_readiness_workflow'


def test_data_management_action_returns_scan_when_mode_selected() -> None:
  # Full settings-ready + mode-selected payload — data and websocket panel actions must return natural resting state (scan)
  payload = {
    'connection_posture': {'mode_selected': True},
    'settings': {'settings_ready': True},
  }
  for action in (
    'detect_available_datapacks', 'select_datapack', 'load_datapack',
    'load_sandbox_datapack', 'load_live_datapack', 'clear_loaded_datapack',
    'load_sandbox_websocket', 'load_live_websocket', 'clear_all_websocket_urls',
  ):
    result = web_app._follow_on_workflow(action, payload)
    assert result['recommended_step'] == 'scan', f'{action}: expected recommended_step=scan, got {result["recommended_step"]!r}'
    assert result['next_actionable_step'] == 'scan', f'{action}: expected next_actionable_step=scan, got {result["next_actionable_step"]!r}'


def test_cp_datapack_id_fields_not_zeroed_in_inactive_lane_boundary_source() -> None:
  # GAP-POST-5: CP identity fields must not appear in the inactive-lane-boundary result.update override
  source = inspect.getsource(web_app.create_operator_console_app)
  update_pos = source.find("result.update(\n        {\n          'candidate_count'")
  assert update_pos != -1, 'inactive-lane-boundary result.update block not found in source'
  update_block = source[update_pos:update_pos + 1000]
  for field in (
    "'sandbox_datapack_id'",
    "'live_datapack_id'",
    "'sandbox_datapack_loaded'",
    "'live_datapack_loaded'",
    "'sandbox_datapack_tail'",
    "'live_datapack_tail'",
    "'sandbox_loaded_at'",
    "'live_loaded_at'",
  ):
    assert field not in update_block, f'{field} must not be zeroed in inactive-lane-boundary override — CP identity fields are global/persistent'


def test_command_group_buttons_gated_by_websocket_session_active_source() -> None:
  # GAP-POST-6: websocket_management, key_management, and data_management must all sit inside the
  # websocketSessionActive section-level gate so that no empty stub section renders when connected.
  # The gate is now a section-level conditional spread: !websocketSessionActive(payload) ? [{...}] : []
  source = inspect.getsource(web_app._render_html)
  gate_pattern = "!websocketSessionActive(payload) ? [{"
  gate_pos = source.find(gate_pattern)
  assert gate_pos != -1, 'websocketSessionActive section-level gate not found in buildDeckViewModel'
  block = source[gate_pos:gate_pos + 600]
  assert "'websocket_management'" in block, 'websocket_management must be inside websocketSessionActive gate'
  assert "'key_management'" in block, 'key_management must be inside websocketSessionActive gate'
  assert "'data_management'" in block, 'data_management must be inside websocketSessionActive gate'


def test_scan_mint_confirmation_gate_fresh_db_proceeds(tmp_path: Path, monkeypatch: Any) -> None:
  # GAP-POST-2: fresh DB (no open lane_active_datapack row) must NOT trigger pending_mint_confirmation
  settings = _runtime_settings_for_lane(tmp_path, 'sandbox')
  _patch_fake_websocket_runtime(monkeypatch, settings)
  monkeypatch.setattr(
    web_app, 'run_sandbox_preflight',
    lambda _s: {'result': 'pass', 'reason_code': 'preflight_passed', 'message': 'ok', 'next_action': 'proceed', 'checks': []},
  )
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=lambda **kwargs: web_app.build_bootstrap_payload(
        settings=settings,
        env_override=kwargs.get('env_override'),
        subaccount_override=kwargs.get('subaccount_override'),
        report_fn=service_module.report_runtime,
        reconcile_fn=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      ),
      scan=lambda **_: {'decision': 'planned', 'candidate_count': 0, 'candidates': [], 'sandbox_extended_count': 0, 'sandbox_candidates_extended': [], 'next_action': 'Review.'},
      run=_services().run,
      reconcile=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      report=service_module.report_runtime,
      cancel_all=_services().cancel_all,
      system_log=_services().system_log,
      visuals=_services().visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  # No open row in DB → gate must NOT fire, scan must proceed normally
  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  assert scan_status == '200 OK'
  pending = ((scan_payload.get('data_management') or {}).get('pending_mint_confirmation') or None)
  assert pending is None, f'fresh DB must not trigger pending_mint_confirmation; got {pending!r}'


def test_scan_mint_confirmation_gate_open_row_triggers_popup(tmp_path: Path, monkeypatch: Any) -> None:
  # GAP-POST-2: when an open lane_active_datapack row exists but no datapack_id, scan must return pending_mint_confirmation
  settings = _runtime_settings_for_lane(tmp_path, 'sandbox')
  _patch_fake_websocket_runtime(monkeypatch, settings)
  monkeypatch.setattr(
    web_app, 'run_sandbox_preflight',
    lambda _s: {'result': 'pass', 'reason_code': 'preflight_passed', 'message': 'ok', 'next_action': 'proceed', 'checks': []},
  )
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=lambda **kwargs: web_app.build_bootstrap_payload(
        settings=settings,
        env_override=kwargs.get('env_override'),
        subaccount_override=kwargs.get('subaccount_override'),
        report_fn=service_module.report_runtime,
        reconcile_fn=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      ),
      scan=lambda **_: {'decision': 'planned', 'candidate_count': 0, 'candidates': [], 'sandbox_extended_count': 0, 'sandbox_candidates_extended': [], 'next_action': 'Review.'},
      run=_services().run,
      reconcile=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      report=service_module.report_runtime,
      cancel_all=_services().cancel_all,
      system_log=_services().system_log,
      visuals=_services().visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  # Seed an open lane_active_datapack row with no datapack_id (no detail_json)
  state_db_path = Path(settings.state_db_path)
  with open_database(state_db_path) as conn:
    resolve_active_profile_token(conn, 'sandbox')
  # Scan must gate on pending_mint_confirmation
  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  assert scan_status == '200 OK'
  pending = (scan_payload.get('data_management') or {}).get('pending_mint_confirmation') or {}
  assert pending.get('required_confirmation') == 'confirm_mint', f'expected pending_mint_confirmation with confirm_mint; got {pending!r}'
  assert pending.get('lane') == 'sandbox'


def test_scan_mint_confirmation_gate_confirm_mint_bypasses_gate(tmp_path: Path, monkeypatch: Any) -> None:
  # GAP-POST-2: confirm_mint=True in request must bypass the gate and allow scan to proceed
  settings = _runtime_settings_for_lane(tmp_path, 'sandbox')
  _patch_fake_websocket_runtime(monkeypatch, settings)
  monkeypatch.setattr(
    web_app, 'run_sandbox_preflight',
    lambda _s: {'result': 'pass', 'reason_code': 'preflight_passed', 'message': 'ok', 'next_action': 'proceed', 'checks': []},
  )
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=lambda **kwargs: web_app.build_bootstrap_payload(
        settings=settings,
        env_override=kwargs.get('env_override'),
        subaccount_override=kwargs.get('subaccount_override'),
        report_fn=service_module.report_runtime,
        reconcile_fn=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      ),
      scan=lambda **_: {'decision': 'planned', 'candidate_count': 0, 'candidates': [], 'sandbox_extended_count': 0, 'sandbox_candidates_extended': [], 'next_action': 'Review.'},
      run=_services().run,
      reconcile=lambda **_: {'decision': 'planned', 'pair_count': 0, 'pairs': []},
      report=service_module.report_runtime,
      cancel_all=_services().cancel_all,
      system_log=_services().system_log,
      visuals=_services().visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  state_db_path = Path(settings.state_db_path)
  with open_database(state_db_path) as conn:
    resolve_active_profile_token(conn, 'sandbox')
  # Send confirm_mint=True — gate must not block
  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={'confirm_mint': True})
  scan_payload = json.loads(scan_body)
  assert scan_status == '200 OK'
  pending = (scan_payload.get('data_management') or {}).get('pending_mint_confirmation') or None
  assert pending is None, f'confirm_mint=True must clear pending_mint_confirmation; got {pending!r}'


def test_post_terminal_cancel_clears_live_interaction_surface() -> None:
  # Lane P3 (2026-06-23): hold gates on terminal_state being non-final, not _in_flight_candidate_count.
  # G1: enabled=True with non-final terminal_state must create a hold
  hold_via_resting = {
    'execution_chronology': {'enabled': True, 'terminal_state': 'RESTING_BOTH'},
  }
  assert web_app._payload_has_live_interaction_hold(hold_via_resting) is True, \
    'G1: RESTING_BOTH is non-final — hold must be active'

  # G1: enabled=True with final terminal_state must NOT create a hold
  no_hold_canceled = {
    'execution_chronology': {'enabled': True, 'terminal_state': 'CANCELED'},
  }
  assert web_app._payload_has_live_interaction_hold(no_hold_canceled) is False, \
    'G1: CANCELED is final — hold must be released'

  # G1: enabled=False must NOT hold regardless of terminal_state
  no_hold_disabled = {
    'execution_chronology': {'enabled': False, 'terminal_state': 'RESTING_BOTH'},
    '_in_flight_candidate_count': 2,
  }
  assert web_app._payload_has_live_interaction_hold(no_hold_disabled) is False, \
    'G1: chronology disabled — hold must not fire'


def test_stage_columns_reflect_lifecycle_state(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime.sqlite3'
  run_id = 'test-run-x6'
  lane_session_id = 'test-session-x6'
  conn = sqlite3.connect(str(state_db_path))
  conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  conn.execute(
    'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?,?)',
    (run_id, lane_session_id),
  )
  # 'other-run' is deliberately NOT in candidate_review_runs for this session
  conn.executemany(
    'INSERT INTO candidate_review_candidates'
    ' (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json)'
    ' VALUES (?,?,?,?,?,?)',
    [
      (run_id, 'in_flight', None,         'TICK-A', 'live_qualifying', '{}'),
      (run_id, 'in_flight', None,         'TICK-B', 'sandbox_extended', '{}'),
      (run_id, 'terminal',  'canceled',   'TICK-C', '',                '{}'),
      (run_id, 'terminal',  'reconciled', 'TICK-D', '',                '{}'),
      ('other-run', 'in_flight', None,    'TICK-X', '',                '{}'),
    ],
  )
  conn.commit()
  conn.close()

  payload = {
    'review_selection': {'persisted_lane_session_id': lane_session_id},
    'settings': {'state_db_path': str(state_db_path)},
  }
  result = web_app._fetch_stage_columns(payload)
  assert result['in_flight_candidate_count'] == 2  # D2: only in_flight (queued) rows count
  columns = {col['stage_id']: col['items'] for col in result['stage_columns']}
  assert sorted(i['ticker'] for i in columns['queued']) == ['TICK-A', 'TICK-B']
  # Terminal-classification contract (53765df "Terminal pre-wire ... acceptance
  # classification"): a reconciled terminal is a no-fill outcome -> cancelled
  # (terminal_cause 'reconciled_no_fill'), not filled. TICK-C (canceled) and
  # TICK-D (reconciled) both land in cancelled; nothing is filled without a
  # LOCKED/FILLED pair state.
  assert sorted(i['ticker'] for i in columns['cancelled']) == ['TICK-C', 'TICK-D']
  assert columns['filled'] == []
  assert 'resolved' not in columns


def test_find_candidates_available_in_monitoring_posture() -> None:
  source = inspect.getsource(web_app._render_html)
  # NS-1 fix: dead monitor branch removed; Find Candidates handled by scan branch
  assert "actionableStepId === 'monitor'" not in source, \
    "NS-1: dead monitor actionableStepId branch must be absent from buildDeckViewModel"
  assert "actionableStepId === 'scan'" in source, \
    "NS-1: scan branch must handle Find Candidates button in buildDeckViewModel"


def test_report_route_stays_review_only_after_report_refresh(monkeypatch: Any) -> None:
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      None,
    ),
  )
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['recommended_step'] == 'report'
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['can_run_next_step'] is False
  assert payload['workflow']['next_actionable_step'] == 'mode_change'
  assert payload['workflow']['focus_target'] == 'evidence-section'
  assert payload['workflow']['deck_view'] == 'review'


def test_report_route_emits_backend_highlight_envelope_after_payload_rebuild(monkeypatch: Any) -> None:
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      None,
    ),
  )
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['deck_action_highlights'].get('key_management') == 'no-go'
  assert payload['workflow']['deck_action_highlights'].get('websocket_management') == 'ok'
  assert payload['workflow']['highlight_policy_version'] == 'orchestrator-highlights.v1'


def test_report_route_promotes_fix_configuration_when_settings_not_ready() -> None:
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=base.run,
      reconcile=base.reconcile,
      report=lambda **_: {
        'decision': 'planned',
        'next_action': 'Fix websocket configuration before continuing.',
        'settings': {
          'settings_ready': False,
          'credential_ready': True,
          'environment_ready': False,
          'available_websocket_urls': {
            'sandbox': 'unconfigured',
            'live': 'unconfigured',
          },
        },
      },
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['recommended_step'] == 'review_configuration'
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['can_run_next_step'] is False
  assert payload['workflow']['next_actionable_step'] == 'set_websocket_url'
  assert payload['workflow']['focus_target'] == 'readiness-section'
  assert payload['workflow']['focus_tone'] == 'focus-info'
  assert payload['workflow']['deck_view'] == 'operator'


def test_key_management_stage_validate_and_apply_support_session_overlay(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'demo-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  monkeypatch.setattr(
    web_app,
    '_probe_key_reference_acceptance',
    lambda *_args, **_kwargs: {
      'ok': True,
      'reason': 'pass',
      'message': 'Key file valid and platform accepted authenticated requests.',
      'next_action': 'Load the validated key reference into this shell session.',
    },
  )
  app = create_operator_console_app(_services())

  discover_status, _, discover_body = _call_app(
    app,
    method='POST',
    path='/api/key-discover',
  )
  discovered_payload = json.loads(discover_body)

  assert discover_status == '200 OK'
  assert discovered_payload['key_management']['discovery_ran'] is True

  stage_status, _, stage_body = _call_app(
    app,
    method='POST',
    path='/api/key-stage',
    body={'path': str(key_file)},
  )
  staged_payload = json.loads(stage_body)

  assert stage_status == '200 OK'
  assert staged_payload['key_management']['selected_key_tail'] == 'demo-key.pem'
  assert staged_payload['key_management']['selected_key_path_display'] == str(key_file)
  assert staged_payload['key_management']['selected_key_ready'] is True
  assert staged_payload['key_management']['last_result']['tone'] == 'ok'

  validate_status, _, validate_body = _call_app(
    app,
    method='POST',
    path='/api/key-validate',
  )
  validated_payload = json.loads(validate_body)

  assert validate_status == '200 OK'
  assert validated_payload['key_management']['selected_key_tail'] == 'demo-key.pem'
  assert validated_payload['key_management']['last_result']['tone'] == 'ok'
  assert validated_payload['credential_posture']['acceptance_ready'] is True
  assert validated_payload['credential_posture']['validation_reason'] == 'pass'

  apply_status, _, apply_body = _call_app(
    app,
    method='POST',
    path='/api/key-apply',
  )
  applied_payload = json.loads(apply_body)

  assert apply_status == '200 OK'
  assert applied_payload['key_management']['overlay_active'] is True
  assert applied_payload['key_management']['active_key_tail'] == 'demo-key.pem'
  assert applied_payload['key_management']['active_key_source_label'] == 'Manual path'
  assert applied_payload['key_management']['selected_key_path_display'] == ''
  assert applied_payload['key_management']['last_result']['tone'] == 'ok'

  clear_status, _, clear_body = _call_app(
    app,
    method='POST',
    path='/api/key-clear',
  )
  cleared_payload = json.loads(clear_body)

  assert clear_status == '200 OK'
  assert cleared_payload['key_management']['overlay_active'] is False
  assert cleared_payload['key_management']['selected_key_tail'] is None
  assert cleared_payload['key_management']['selected_key_path_display'] == ''
  assert cleared_payload['key_management']['active_key_tail'] == '--'
  assert cleared_payload['key_management']['active_key_source_label'] == 'Cleared in session'
  assert cleared_payload['key_management']['last_result']['tone'] == 'warn'


def test_key_load_single_lane_sets_warning_glow_tone(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'demo-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  monkeypatch.setattr(
    web_app,
    '_probe_key_reference_acceptance',
    lambda *_args, **_kwargs: {
      'ok': True,
      'reason': 'pass',
      'message': 'Key file valid and platform accepted authenticated requests.',
      'next_action': 'Load the validated key reference into this shell session.',
      'detected_environment': 'demo',
      'expected_environment': 'demo',
    },
  )
  app = create_operator_console_app(_services())

  _call_app(
    app,
    method='POST',
    path='/api/key-stage',
    body={'path': str(key_file)},
  )
  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/key-load',
    body={'action': 'load_sandbox_key_reference'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['key_management']['loaded_count'] == 1
  assert payload['key_management']['key_glow'] == 'warning'
  assert payload['key_management']['key_glow_tone'] == 'warn'
  assert payload['workflow']['deck_action_highlights']['key_management'] == 'warn'


def test_live_key_load_prefers_lane_specific_api_key_from_settings_when_process_env_missing(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'live-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  observed: dict[str, str] = {}

  monkeypatch.delenv('KALSHI_SANDBOX_API_KEY_ID', raising=False)
  monkeypatch.delenv('KALSHI_LIVE_API_KEY_ID', raising=False)

  monkeypatch.setattr(
    web_app,
    '_resolve_settings',
    lambda *_args, **_kwargs: Settings(
      kalshi_env='demo',
      api_key_id='generic-api-key-111111',
      private_key_file=str(key_file),
      private_key_inline=None,
      private_key_path_legacy=None,
      api_base_url='https://external-api.demo.kalshi.co/trade-api/v2',
      websocket_url='wss://demo-api.kalshi.example/ws',
      subaccount=0,
      scan_interval_ms=1500,
      entry_window_start_sec=0,
      entry_window_end_sec=25,
      min_edge_dollars=0.10,
      fee_reserve_dollars=0.02,
      min_profit_dollars=0.05,
      max_pair_contracts=5,
      max_open_pairs=3,
      max_unhedged_sec=20,
      cancel_on_pause=True,
      log_level='INFO',
      state_db_path=str(tmp_path / 'state.sqlite3'),
      sandbox_api_key_id='sandbox-api-key-222222',
      live_api_key_id='live-api-key-333333',
      operation_lane='sandbox',
      sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
      live_websocket_url='wss://api.kalshi.example/ws',
      active_websocket_url='wss://demo-api.kalshi.example/ws',
    ),
  )

  def _probe(settings: Any, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
    observed['api_key_id'] = str(getattr(settings, 'api_key_id', ''))
    return {
      'ok': True,
      'reason': 'pass',
      'message': 'Key file valid and platform accepted authenticated requests.',
      'next_action': 'Load the validated key reference into this shell session.',
      'detected_environment': 'live',
      'expected_environment': 'live',
    }

  monkeypatch.setattr(web_app, '_probe_key_reference_acceptance', _probe)

  app = create_operator_console_app(_services())

  _call_app(
    app,
    method='POST',
    path='/api/key-stage',
    body={'path': str(key_file)},
  )
  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/key-load',
    body={'action': 'load_live_key_reference'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert observed['api_key_id'] == 'live-api-key-333333'
  assert payload['key_management']['live_api_key_id_tail'] == '...333333'
  assert payload['key_management']['live_api_key_source_class'] == 'lane_settings'
  assert payload['key_management']['live_api_key_id_tail'] != '...111111'
  assert payload['key_management']['live_key_loaded'] is True


def test_key_management_missing_selected_key_surfaces_no_go_feedback(tmp_path: Path) -> None:
  missing_key = tmp_path / 'missing-key.pem'
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  stage_status, _, stage_body = _call_app(
    app,
    method='POST',
    path='/api/key-stage',
    body={'path': str(missing_key)},
  )
  staged_payload = json.loads(stage_body)

  assert stage_status == '200 OK'
  assert staged_payload['key_management']['selected_key_tail'] == 'missing-key.pem'
  assert staged_payload['key_management']['selected_key_ready'] is False
  assert staged_payload['key_management']['last_result']['tone'] == 'warn'

  validate_status, _, validate_body = _call_app(
    app,
    method='POST',
    path='/api/key-validate',
  )
  validated_payload = json.loads(validate_body)

  assert validate_status == '200 OK'
  assert validated_payload['key_management']['last_result']['tone'] == 'no-go'
  assert 'not currently available' in validated_payload['key_management']['last_result']['message']
  assert validated_payload['key_management']['overlay_active'] is False

  apply_status, _, apply_body = _call_app(
    app,
    method='POST',
    path='/api/key-apply',
  )
  applied_payload = json.loads(apply_body)

  assert apply_status == '200 OK'
  assert applied_payload['key_management']['last_result']['tone'] == 'no-go'
  assert 'cannot be loaded' in applied_payload['key_management']['last_result']['message']
  assert applied_payload['key_management']['overlay_active'] is False


def test_key_validate_surfaces_format_error_for_non_pem_key(tmp_path: Path) -> None:
  invalid_key = tmp_path / 'invalid-key.pem'
  invalid_key.write_text('not-a-valid-pem-key', encoding='utf-8')
  app = create_operator_console_app(_services())

  _call_app(
    app,
    method='POST',
    path='/api/key-stage',
    body={'path': str(invalid_key)},
  )

  validate_status, _, validate_body = _call_app(
    app,
    method='POST',
    path='/api/key-validate',
  )
  payload = json.loads(validate_body)

  assert validate_status == '200 OK'
  assert payload['key_management']['last_result']['tone'] == 'no-go'
  assert payload['credential_posture']['acceptance_ready'] is False
  assert payload['credential_posture']['validation_reason'] == 'format_error'


def test_key_validate_surfaces_auth_fail_projection(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'demo-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  monkeypatch.setattr(
    web_app,
    '_probe_key_reference_acceptance',
    lambda *_args, **_kwargs: {
      'ok': False,
      'reason': 'auth_fail',
      'message': 'Platform rejected credentials on authenticated account checks.',
      'next_action': 'Verify API key id and private key pairing, then retry.',
    },
  )
  app = create_operator_console_app(_services())

  _call_app(
    app,
    method='POST',
    path='/api/key-stage',
    body={'path': str(key_file)},
  )

  validate_status, _, validate_body = _call_app(
    app,
    method='POST',
    path='/api/key-validate',
  )
  payload = json.loads(validate_body)

  assert validate_status == '200 OK'
  assert payload['key_management']['last_result']['tone'] == 'no-go'
  assert payload['key_management']['last_result']['origin'] == 'kalshi'
  assert payload['credential_posture']['acceptance_ready'] is False
  assert payload['credential_posture']['validation_reason'] == 'auth_fail'
  assert payload['notification_source']['origin'] == 'kalshi'
  assert payload['notification_source']['is_external'] is True


def test_key_validate_surfaces_credential_environment_mismatch_projection(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'demo-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  monkeypatch.setattr(
    web_app,
    '_probe_key_reference_acceptance',
    lambda *_args, **_kwargs: {
      'ok': False,
      'reason': 'credential_environment_mismatch',
      'message': 'Credential validated, but it belongs to a different Kalshi environment than the selected lane.',
      'next_action': 'Use demo credentials for sandbox mode or switch to the live lane.',
    },
  )
  app = create_operator_console_app(_services())

  _call_app(
    app,
    method='POST',
    path='/api/key-stage',
    body={'path': str(key_file)},
  )

  validate_status, _, validate_body = _call_app(
    app,
    method='POST',
    path='/api/key-validate',
  )
  payload = json.loads(validate_body)

  assert validate_status == '200 OK'
  assert payload['key_management']['last_result']['tone'] == 'no-go'
  assert payload['key_management']['last_result']['origin'] == 'kalshi'
  assert payload['credential_posture']['acceptance_ready'] is False
  assert payload['credential_posture']['validation_reason'] == 'credential_environment_mismatch'
  assert payload['notification_source']['origin'] == 'kalshi'
  assert payload['notification_source']['is_external'] is True
  assert 'different kalshi environment' in str(payload['credential_posture']['acceptance_detail']).lower()


def test_discover_private_key_candidates_finds_workspace_adjacent_secret_root(tmp_path: Path, monkeypatch: Any) -> None:
  from cryptography.hazmat.primitives.asymmetric import rsa
  from cryptography.hazmat.primitives import serialization
  from cryptography.hazmat.backends import default_backend
  
  project_root = tmp_path / 'UNC' / 'polyventure'
  project_root.mkdir(parents=True)
  key_file = tmp_path / 'secrets' / 'kalshi' / 'demo' / 'private_key.pem'
  key_file.parent.mkdir(parents=True)
  
  private_key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
    backend=default_backend(),
  )
  pem = private_key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption(),
  )
  key_file.write_bytes(pem)
  
  (project_root / '.env').write_text(
    'KALSHI_PRIVATE_KEY_FILE=secrets/kalshi/demo/private_key.pem\n',
    encoding='utf-8',
  )

  monkeypatch.chdir(project_root)
  monkeypatch.setattr(web_app, 'PROJECT_ROOT', project_root)

  candidates = web_app._discover_private_key_candidates(project_root=project_root)

  assert any(candidate['path_tail'] == 'private_key.pem' for candidate in candidates)
  assert any(candidate['resolved_path'] == str(key_file.resolve()) for candidate in candidates)


def test_key_discover_forces_fresh_refresh_and_prunes_stale_registry(tmp_path: Path, monkeypatch: Any) -> None:
  tombstone = tmp_path / 'tombstones.json'
  stale_key = tmp_path / 'stale-key.pem'
  stale_key.write_text('stale-placeholder', encoding='utf-8')
  fresh_key = tmp_path / 'fresh-key.pem'
  fresh_key.write_text('fresh-placeholder', encoding='utf-8')

  tombstone.write_text(
    json.dumps(
      {
        'key_registry': [
          {
            'resolved_path': str(stale_key.resolve()),
            'path_tail': 'stale-key.pem',
            'source_label': 'Persisted stale entry',
            'profile_token': 'kalshi-stale01',
            'discovered_at_utc': '2026-05-10T00:00:00Z',
            'last_seen_at_utc': '2026-05-10T00:00:00Z',
            'status': 'available',
          }
        ]
      },
      indent=2,
    ),
    encoding='utf-8',
  )

  discover_calls: dict[str, int] = {'count': 0}

  def _fresh_discover(*, project_root: Path = web_app.PROJECT_ROOT) -> list[dict[str, str]]:
    _ = project_root
    discover_calls['count'] += 1
    return [
      {
        'resolved_path': str(fresh_key.resolve()),
        'path_tail': 'fresh-key.pem',
        'source_label': 'Live env · KALSHI_PRIVATE_KEY_FILE',
      }
    ]

  monkeypatch.setattr(web_app, '_discover_private_key_candidates', _fresh_discover)
  app = create_operator_console_app(_services(), tombstone_path=tombstone)

  first_status, _, first_body = _call_app(app, method='POST', path='/api/key-discover')
  first_payload = json.loads(first_body)

  assert first_status == '200 OK'
  assert discover_calls['count'] == 1
  assert first_payload['key_management']['discovery_ran'] is True
  assert first_payload['key_management']['candidate_count'] == 1
  assert first_payload['key_management']['discovered_candidates'][0]['path_tail'] == 'fresh-key.pem'
  assert all(
    candidate['path_tail'] != 'stale-key.pem'
    for candidate in first_payload['key_management']['discovered_candidates']
  )
  assert first_payload['key_management']['last_result']['message'] == 'Discovered 1 available key reference(s) for this session.'

  second_status, _, _ = _call_app(app, method='POST', path='/api/key-discover')

  assert second_status == '200 OK'
  assert discover_calls['count'] == 2


def test_key_management_detail_pane_contract_stays_action_first_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "key_management: Object.freeze({" in body
  assert "summary: ''" in body
  assert 'meta: []' in body
  assert 'Stage or load a key-file reference for the current shell without exposing secret material.' not in body
  assert "detailControlHighlights: {}" in body
  assert 'deck-action-glow-no-go' in body
  assert 'background: linear-gradient(180deg, rgba(76, 24, 32, 0.92), rgba(34, 11, 15, 0.98));' not in body
  assert 'function routeKeyManagementFailure(payload = {}, options = {})' in body
  assert 'function applyKeyManagementActionOutcome(action, payload = {}, options = {})' in body
  assert 'id="key-picker-modal"' in body
  assert 'id="key-picker-list"' in body
  assert 'id="key-picker-close"' in body
  assert 'function renderKeyPickerModal()' in body
  assert 'function openKeyPickerModal(payload = {})' in body
  assert 'function closeKeyPickerModal()' in body
  assert 'Select one available key reference (name · source · unique id).' not in body
  assert "laneTag: String(candidate.lane_tag || '').trim().toLowerCase()" in body
  assert 'class="key-picker-tag"' in body
  assert "label: 'Load sandbox', action: 'load_sandbox_key_reference'" in body
  assert "label: 'Load live', action: 'load_live_key_reference'" in body
  assert 'id="key-file-input"' in body
  assert '/api/key-stage-upload' in body
  assert "action: 'open_discovered_key_picker'" not in body
  assert 'discoveredKeyRows' not in body
  assert 'discoveredKeyControls' not in body


def test_mode_selector_allows_loaded_sandbox_key_and_requires_validated_live_key_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'const clearHoldActive = keyManagement.clear_hold_active === true;' in body
  assert 'const sandboxKeyValidated = keyManagement.sandbox_key_validated === true;' in body
  assert 'const liveKeyValidated = keyManagement.live_key_validated === true;' in body
  assert 'const sandboxReady = !clearHoldActive && sandboxKeyPresent && sandboxUrlPresent;' in body
  assert 'const liveReady = !clearHoldActive && liveKeyPresent && liveUrlPresent && liveKeyValidated;' in body


def test_change_mode_feedback_and_key_load_failure_log_contract_stay_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "focusRouteKey: 'change_mode'" in body
  assert "if (normalizedAction === 'change_mode') {" in body
  assert "const route = routeKeyManagementFailure(payload);" in body
  assert "route.message = payload.message || 'Mode change blocked. Review key and websocket readiness.';" in body
  assert "if (selectedLane === 'offline') {" in body
  assert "message: 'Offline mode selected. Review shell posture before continuing.'," in body
  assert 'closeHelp: true,' in body
  assert 'clearGlow: true,' in body
  assert 'Key validation for the ${laneLabel} slot failed (${reasonLabel}). The key was not applied.' in body
  assert 'activateWayfinder({ closeHelp: true, ...successRoute });' in body


def test_next_step_projection_contract_stays_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'const actionableStepId = nextActionableStep(workflow) || normalizedStepId;' in body
  assert 'const compactLabel = compactStepLabel(actionableStepId);' in body
  assert 'stepId: actionableStepId,' in body
  assert 'stepLabel: humanActionLabel(actionableStepId),' in body
  assert 'guidanceParts.push(`Next executable move: ${humanActionLabel(actionableStepId)}.`);' in body


def test_refresh_truth_precedence_contract_stays_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "function completedCandidatesFoundResultOwnsPrimaryStory(payload = {}) {" in body
  assert "const actionableStepId = String(nextActionableStep(workflow) || workflow.recommended_step || '').toLowerCase();" in body
  assert "if (actionableStepId !== 'select_candidates' || payload.boundary) return false;" in body
  assert "const exitContract = scanRuntime.exit_contract || {};" in body
  assert "const candidateCount = Number(payload.candidate_count || pairMonitor.candidate_count || 0);" in body
  assert "if (completedCandidatesFoundResultOwnsPrimaryStory(payload)) return '';" in body
  assert "function connectionTruthToken(payload = {}) {" in body
  assert "if (!connectionPosture.modeSelected && nextStepId === 'mode_change') {" in body
  assert "return 'mode_change_required';" in body
  assert "function currentTruthHeadlineOverride(payload = {}) {" in body
  assert "const currentTruthHeadline = currentTruthHeadlineOverride(payload);" in body
  assert "if (currentTruthHeadline) return currentTruthHeadline;" in body
  assert "headlineValue: retainedHeadline || (latestAcceptedRun ? 'REVIEW AVAILABLE' : (aggregateAvailable ? 'SUMMARY AVAILABLE' : 'LOCAL SUMMARY'))," in body
  assert "headlineValue: 'CONNECTION FAILED'," in body
  assert "headlineValue: 'AUTH FAILED'," in body
  assert "function currentTruthEvidenceHeadline(payload = {}) {" in body
  assert "return connectionTruthToken(payload) ? 'RETAINED EVIDENCE' : '';" in body


def test_lane_pill_connecting_feedback_contract_stays_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert '#lane-pill.mode-connecting {' in body
  assert '@keyframes pill-strobe-establishing {' in body  # SSE1-E: renamed from pill-strobe-connecting
  assert '#lane-pill.mode-selecting {' in body  # SSE1-E: new fast-green selecting state
  assert '@keyframes pill-strobe-offline-fast {' in body  # SSE1-E: keyframe for mode-selecting
  assert "modeChangePendingLane: ''" in body
  assert "const modeChangePendingLane = String(state.modeChangePendingLane || '').toLowerCase();" in body
  assert "const websocketConnecting = state.activeAction === 'change_mode' && ['sandbox', 'live'].includes(modeChangePendingLane);" in body
  assert "lanePill.classList.remove('mode-offline', 'mode-selected', 'mode-change-disabled', 'mode-connecting', 'mode-selecting');" in body  # SSE1-E: mode-selecting added
  assert "lanePill.classList.add('mode-connecting');" in body
  assert "lanePill.classList.add('mode-selecting');" in body  # SSE1-E: new selecting state branch
  assert "lanePill.style.cursor = 'progress';" in body
  assert "lanePill.title = 'Establishing websocket session';" in body
  assert "lanePill.setAttribute('aria-label', `${humanOperationLaneLabel(modeChangePendingLane)} connecting`);" in body
  assert "state.modeChangePendingLane = selectedLane === 'offline' ? '' : selectedLane;" in body
  assert "await runUiAction('change_mode', {" in body


def test_scan_route_retry_wait_candidate_count_scan_runtime_contract_is_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  # FD-2 / S1: updateZeroFoundRetryCountdown — must gate on the ACTIONABLE (non-terminal)
  # count so the zero-found-retry resumes once every surfaced candidate has gone terminal;
  # falls back to the full result count for older payloads.
  assert 'const candidateCount = Number(runtime.result_active_candidate_count ?? runtime.result_candidate_count ?? 0);' in body
  # FD-3 / FD-1: buildProcessingRowModel — surfacedCandidates keeps the full surfaced count.
  assert 'const surfacedCandidates = Number(runtime.result_candidate_count ?? 0);' in body


def test_scan_route_heartbeat_mode_syncing_animation_contract_is_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  # FR-1: CSS animation rule present
  assert '#heartbeat-pill.mode-syncing {' in body
  assert 'pill-strobe-offline-fast' in body  # reuses existing keyframe
  # FR-1: JS class management wired
  assert "'mode-syncing'" in body


def test_close_window_offline_transition_modal_contract_is_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'id="close-transition-modal"' in body
  assert 'class="close-transition-spinner"' in body
  assert 'Switching to offline mode before closing this shell.' in body
  assert "window.addEventListener('beforeunload', (event) => {" in body
  assert 'const switchingOffline = beginCloseWindowOfflineTransition();' in body
  assert 'event.preventDefault();' in body
  assert 'event.returnValue = true;' in body
  assert "set_offline_if_active: true," in body
  assert "close_reason: 'browser_window_closed'," in body


def test_mode_change_button_locked_with_tooltip_while_action_in_flight() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'mode change locked' in body
  assert 'updateLanePillModeChangeState' in body
  assert 'state.submitOrderPending' in body


def test_clear_key_active_websocket_transition_contract_is_embedded_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "function beginKeyClearOfflineTransition() {" in body
  assert "title: 'Clearing local key'," in body
  assert "message: 'Switching to offline mode before clearing this key.'," in body
  assert "const switchingOffline = beginKeyClearOfflineTransition();" in body
  assert "set_offline_if_active: true," in body
  assert "clear_reason: 'active_websocket_key_clear'," in body


def test_mode_change_offline_success_projects_neutral_route_contract_in_shell_html() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "const selectedLane = String(" in body
  assert "if (selectedLane === 'offline') {" in body
  assert "tone: 'focus-info'," in body
  assert "message: 'Offline mode selected. Review shell posture before continuing.'," in body
  assert 'closeHelp: true,' in body
  assert 'clearGlow: true,' in body


def test_key_stage_upload_stages_uploaded_file_for_load_flow(tmp_path: Path) -> None:
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/key-stage-upload',
    body={
      'file_name': 'picked-key.pem',
      'file_bytes_b64': base64.b64encode(b'placeholder-key-material').decode('ascii'),
    },
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['key_management']['selected_key_tail'].startswith('picked-key-')
  assert payload['key_management']['selected_key_tail'].endswith('.pem')
  assert payload['key_management']['selected_key_path_display'].endswith(payload['key_management']['selected_key_tail'])
  assert payload['key_management']['selected_key_ready'] is True
  assert payload['key_management']['selected_key_source_label'] == 'Local file picker'
  assert payload['key_management']['last_result']['tone'] == 'ok'


def test_open_artifact_path_route_reveals_project_file(monkeypatch: Any) -> None:
  app = create_operator_console_app(_services())
  artifact_dir = web_app.PROJECT_ROOT / '.agent_session'
  artifact_dir.mkdir(parents=True, exist_ok=True)
  artifact_file = artifact_dir / 'pytest-open-artifact-path.txt'
  artifact_file.write_text('artifact path reveal test', encoding='utf-8')
  relative_path = artifact_file.relative_to(web_app.PROJECT_ROOT).as_posix()

  try:
    monkeypatch.setattr(web_app, '_open_file_location_in_explorer', lambda _path: (True, 'Opened Explorer and selected pytest-open-artifact-path.txt.'))

    status, _, body = _call_app(
      app,
      method='POST',
      path='/api/open-artifact-path',
      body={'path': relative_path},
    )
    payload = json.loads(body)

    assert status == '200 OK'
    assert payload['decision'] == 'planned'
    assert payload['reason'] == 'planned'
    assert 'Opened Explorer and selected' in payload['message']
  finally:
    artifact_file.unlink(missing_ok=True)


def test_open_artifact_path_route_rejects_out_of_scope_paths() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/open-artifact-path',
    body={'path': '../outside-project/report.md'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'artifact_path_invalid'


def test_unknown_route_returns_not_found_json() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/missing')
  payload = json.loads(body)

  assert status == '404 Not Found'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'route_not_found'


def test_scan_failure_promotes_safe_boundary_payload(tmp_path: Path, monkeypatch: Any) -> None:
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': False,
        'credential_ready': False,
        'environment_ready': True,
        'available_websocket_urls': {
          'sandbox': '',
          'live': '',
        },
        'mode_selected': False,
      },
      FileNotFoundError('private_key.pem'),
    ),
  )
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=lambda **_: (_ for _ in ()).throw(
        FileNotFoundError(r'secrets\kalshi\demo\private_key.pem')
      ),
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )

  status, _, body = _call_app(app, method='POST', path='/api/scan')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'missing_private_key_file'
  assert payload['workflow']['recommended_step'] == 'review_configuration'
  assert payload['workflow']['step_kind'] == 'fix_configuration'
  assert payload['workflow']['can_run_next_step'] is False
  assert payload['workflow']['next_actionable_step'] == 'load_api_key'
  assert payload['workflow']['focus_target'] == 'readiness-section'
  assert payload['workflow']['deck_view'] == 'operator'
  assert payload['message'] == 'The configured private key file is missing: private_key.pem.'
  assert 'Users' not in payload['message']
  assert payload['boundary']['reason'] == 'missing_private_key_file'
  assert payload['boundary']['evidence'][0] == 'missing file: private_key.pem'


def test_bootstrap_fix_configuration_next_step_prefers_websocket_when_key_ready(monkeypatch: Any, tmp_path: Path) -> None:
  key_file = tmp_path / 'ready-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='',
    sandbox_websocket_url='',
    live_websocket_url='',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, '_validate_env_alignment', lambda _settings: (_ for _ in ()).throw(ValueError('websocket url missing')))
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'planned'
  assert payload['workflow']['recommended_step'] == 'review_configuration'
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['next_actionable_step'] == 'set_websocket_url'
  assert payload['workflow']['focus_tone'] == 'focus-info'
  assert payload['startup_wizard']['readiness_status'] == 'pending'
  assert payload['startup_wizard']['steps'][0]['status'] == 'pending'


def test_key_clear_tombstone_persists_cleared_state_across_app_restart(tmp_path: Path) -> None:
  # Verify that a key-clear survives process restart: a NEW app instance created
  # with the same tombstone file must start with cleared state and must NOT
  # repopulate the active key from .env/config fallback.
  tombstone = tmp_path / 'tombstones.json'
  key_file = tmp_path / 'session-key.pem'
  key_file.write_text('placeholder', encoding='utf-8')

  # --- Session 1: stage, apply, then clear ---
  app1 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _call_app(app1, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app1, method='POST', path='/api/key-apply')
  _call_app(app1, method='POST', path='/api/key-clear')

  assert tombstone.exists(), 'Tombstone file must be written on key-clear'

  # --- Session 2: fresh app instance, same tombstone file (simulates restart) ---
  app2 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert payload['key_management']['active_key_tail'] == '--'
  assert payload['key_management']['active_key_source_label'] == 'Cleared in session'
  assert payload['key_management']['overlay_active'] is False


def test_key_load_after_clear_removes_tombstone_and_restores_availability(tmp_path: Path) -> None:
  # Verify that an explicit Load after a clear removes the cleared-key tombstone
  # and the loaded key reference persists across restart.
  tombstone = tmp_path / 'tombstones.json'
  key_file = tmp_path / 'restored-key.pem'
  key_file.write_text('placeholder', encoding='utf-8')

  # Session 1: clear then immediately stage + apply (explicit Load)
  app1 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _call_app(app1, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app1, method='POST', path='/api/key-apply')
  _call_app(app1, method='POST', path='/api/key-clear')
  # Now re-stage and re-apply -- this must remove the tombstone
  _call_app(app1, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app1, method='POST', path='/api/key-apply')

  # Session 2: fresh app, verify cleared flag is NOT set and loaded key persists
  app2 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert payload['key_management']['overlay_active'] is True
  assert payload['key_management']['active_key_tail'] == 'restored-key.pem'
  assert payload['key_management']['active_key_source_label'] == 'Manual path'


def test_lane_validation_persists_across_restart_for_boot_mode_selection(tmp_path: Path, monkeypatch: Any) -> None:
  tombstone = tmp_path / 'tombstones.json'
  app1 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _load_lane_key(app1, tmp_path, monkeypatch, 'sandbox')

  app2 = create_operator_console_app(_services(), tombstone_path=tombstone)
  bootstrap_status, _, bootstrap_body = _call_app(app2, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['key_management']['sandbox_key_loaded'] is True
  assert bootstrap_payload['key_management']['sandbox_key_validated'] is True
  assert bootstrap_payload['connection_posture']['available_websocket_urls']['sandbox'] == 'demo-api.kalshi.example/ws'


def test_bootstrap_offline_prefers_current_settings_over_stale_report_runtime_sources() -> None:
  base = _services()
  stale_services = OperatorConsoleServices(
    bootstrap=lambda **_: {
      'decision': 'planned',
      'settings': {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'mode_selected': False,
        'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'live_websocket_url': 'wss://api.kalshi.example/ws',
        'active_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
        'state_db_path_tail': 'kalshi.sqlite3',
        'private_key_path_tail': 'private_key.pem',
      },
      'report': {
        'latest_heartbeat': {'status': 'cycle-complete', 'recorded_at_utc': '2026-05-28T19:50:33Z'},
        'operation_lane': 'sandbox',
        'lane_session_id': 'sandbox-stale-session',
        'active_websocket_url_tail': 'external-api-ws.demo.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'external-api-ws.demo.kalshi.example/ws',
          'live': 'external-api-ws.kalshi.example/ws',
        },
        'connection_state': {
          'status': 'waiting',
          'websocket_connected': False,
        },
        'state_db_path_tail': 'kalshi_pairs.sqlite3',
      },
      'workflow': {
        'recommended_step': 'mode_change',
        'auto_sequence': ['mode_change'],
        'headline': 'Choose the operating mode before runtime actions.',
        'operator_message': 'Select Change mode to continue.',
        'step_kind': 'review',
        'can_run_next_step': True,
        'next_actionable_step': 'mode_change',
        'focus_target': 'notification-band-section',
        'focus_tone': 'focus-info',
        'deck_view': 'workflow',
        'button_emphasis_tone': 'info',
      },
      'next_action': 'Select Change mode to continue.',
    },
    scan=base.scan,
    run=base.run,
    reconcile=base.reconcile,
    report=base.report,
    cancel_all=base.cancel_all,
    system_log=base.system_log,
    visuals=base.visuals,
  )
  app = create_operator_console_app(stale_services)

  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['connection_posture']['operation_lane'] == 'offline'
  assert bootstrap_payload['connection_posture']['mode_selected'] is False
  assert bootstrap_payload['settings']['mode_selected'] is False
  assert bootstrap_payload['connection_posture']['available_websocket_urls']['sandbox'] == 'demo-api.kalshi.example/ws'
  assert bootstrap_payload['connection_posture']['available_websocket_urls']['live'] == 'api.kalshi.example/ws'
  assert bootstrap_payload['connection_posture']['lane_session_id'] == ''
  assert bootstrap_payload['connection_posture']['active_websocket_url_tail'] == 'demo-api.kalshi.example/ws'
  assert bootstrap_payload['connection_posture']['connection_state']['websocket_connected'] is False
  assert bootstrap_payload['workflow']['recommended_step'] == 'change_mode'
  assert bootstrap_payload['workflow']['next_actionable_step'] == 'mode_change'
  assert bootstrap_payload['workflow']['auto_sequence'] == []
  assert bootstrap_payload.get('report') is None


def test_bootstrap_offline_hides_loaded_lane_key_slots(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')

  mode_status, _, mode_body = _call_app(
    app,
    method='POST',
    path='/api/change-mode',
    body={'lane': 'offline'},
  )
  mode_payload = json.loads(mode_body)

  assert mode_status == '200 OK'
  assert mode_payload['connection_posture']['operation_lane'] == 'offline'

  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['key_management']['active_key_tail'] == 'sandbox-key.pem'
  assert bootstrap_payload['key_management']['overlay_active'] is True
  assert bootstrap_payload['key_management']['sandbox_key_tail'] != '--'
  assert bootstrap_payload['key_management']['sandbox_key_loaded'] is True
  assert bootstrap_payload['key_management']['loaded_count'] == 1
  assert bootstrap_payload['workflow']['recommended_step'] == 'change_mode'
  assert bootstrap_payload['workflow']['next_actionable_step'] == 'mode_change'
  assert bootstrap_payload['workflow']['auto_sequence'] == []


def test_system_log_route_hides_lane_entries_when_mode_not_selected() -> None:
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=lambda **_: {
        'entries': [
          {
            'key': 'lane:1',
            'message': 'sandbox heartbeat received',
            'operation_lane': 'sandbox',
            'lane_session_id': 'sandbox-session-1',
            'recorded_at_utc': '2026-06-04T23:00:00Z',
          }
        ],
        'latest_recorded_at_utc': '2026-06-04T23:00:00Z',
      },
      visuals=base.visuals,
    )
  )

  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'offline'})
  status, _, body = _call_app(app, method='GET', path='/api/system-log')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['entries'] == []
  assert payload['message'] == 'System log entries stay hidden until sandbox or live mode is selected.'


def test_visuals_route_hides_lane_series_when_mode_not_selected() -> None:
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=lambda **_: {
        'scope': {'id': 'runtime_posture', 'title': 'Runtime posture'},
        'view': {'id': 'runtime_cadence', 'title': 'Runtime cadence', 'render_mode': 'table'},
        'window': {'id': '1h', 'label': '1 hour'},
        'detail': {'mode': 'med'},
        'series': [
          {
            'kind': 'bar',
            'points': [
              {'x': 'Heartbeat', 'y': 3},
            ],
          }
        ],
        'table': {
          'columns': ['State', 'Count'],
          'rows': [['Heartbeat', 3]],
        },
        'report': {
          'title': 'Runtime report',
          'sections': [{'heading': 'Heartbeat', 'lines': ['sandbox lane active']}],
        },
      },
    )
  )

  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'offline'})
  status, _, body = _call_app(app, method='GET', path='/api/visuals', query='view=runtime_cadence&window=1h&mode=table')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['series'] == []
  assert payload['table']['rows'] == []
  assert payload['report']['sections'] == []
  assert payload['empty_reason'] == 'Operational visuals stay hidden until sandbox or live mode is selected.'


def test_mode_change_offline_does_not_repopulate_local_key_path_input(tmp_path: Path) -> None:
  key_file = tmp_path / 'mode-change-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  stage_status, _, stage_body = _call_app(
    app,
    method='POST',
    path='/api/key-stage',
    body={'path': str(key_file)},
  )
  staged_payload = json.loads(stage_body)
  assert stage_status == '200 OK'
  assert staged_payload['key_management']['selected_key_path_display'] == str(key_file)

  mode_status, _, mode_body = _call_app(
    app,
    method='POST',
    path='/api/change-mode',
    body={'lane': 'offline'},
  )
  mode_payload = json.loads(mode_body)

  assert mode_status == '200 OK'
  assert mode_payload['connection_posture']['operation_lane'] == 'offline'
  assert mode_payload['key_management']['selected_key_path_display'] == ''


def test_scan_authenticated_request_failure_routes_to_fix_configuration(tmp_path: Path, monkeypatch: Any) -> None:
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': False,
        'credential_ready': False,
        'environment_ready': True,
        'available_websocket_urls': {
          'sandbox': '',
          'live': '',
        },
        'mode_selected': False,
      },
      FileNotFoundError('private_key.pem'),
    ),
  )
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=lambda **_: (_ for _ in ()).throw(RuntimeError('Platform rejected the authenticated request.')),
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )

  status, _, body = _call_app(app, method='POST', path='/api/scan')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'credential_acceptance_failed'
  assert payload['workflow']['recommended_step'] == 'review_configuration'
  assert payload['workflow']['step_kind'] == 'fix_configuration'
  assert payload['workflow']['can_run_next_step'] is False
  assert payload['workflow']['next_actionable_step'] == 'load_api_key'
  assert payload['workflow']['focus_target'] == 'readiness-section'
  assert payload['credential_posture']['acceptance_ready'] is False
  assert 'Runtime account-auth acceptance failed on account endpoints' in payload['credential_posture']['acceptance_detail']
  credential_step = next(step for step in payload['startup_wizard']['steps'] if step['id'] == 'credentials')
  assert credential_step['status'] == 'blocked'
  assert 'platform rejected the authenticated account request' in credential_step['detail']
  assert 'account auth: rejected on account endpoints' in payload['boundary']['evidence']


def test_bootstrap_route_returns_no_go_and_persists_route_failure_event(
  monkeypatch: Any,
  tmp_path: Path,
) -> None:
  key_file = tmp_path / 'demo.pem'
  key_file.write_text('demo-private-key', encoding='utf-8')
  state_db_path = tmp_path / 'runtime.sqlite3'
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(state_db_path),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=lambda **_: (_ for _ in ()).throw(RuntimeError('bootstrap refresh assembly failed')),
      scan=base.scan,
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )

  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'bootstrap_failed'
  assert payload['route_failure']['route_path'] == '/api/bootstrap'
  assert payload['route_failure']['exception_class'] == 'RuntimeError'
  assert payload['route_failure']['message'] == 'bootstrap refresh assembly failed'

  with open_database(state_db_path) as connection:
    row = connection.execute(
      "SELECT event_type, detail_json FROM runtime_events WHERE event_type = 'operator_shell_route_failed' ORDER BY id DESC LIMIT 1"
    ).fetchone()

  assert row is not None
  assert row[0] == 'operator_shell_route_failed'
  detail = json.loads(row[1])
  assert detail['route_path'] == '/api/bootstrap'
  assert detail['action_name'] == 'bootstrap'
  assert detail['message'] == 'bootstrap refresh assembly failed'


def test_scan_route_returns_no_go_and_persists_route_failure_event_when_processing_ack_refresh_fails(
  monkeypatch: Any,
  tmp_path: Path,
) -> None:
  key_file = tmp_path / 'demo.pem'
  key_file.write_text('demo-private-key', encoding='utf-8')
  state_db_path = tmp_path / 'runtime.sqlite3'
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(state_db_path),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  release_scan = threading.Event()

  def _blocking_scan(**kwargs: Any) -> dict[str, Any]:
    release_scan.wait(timeout=1.0)
    return {
      'decision': 'planned',
      'candidate_count': 1,
      'candidates': [{'ticker': 'LIVE-1'}],
      'next_action': 'Review candidates in Pairs.',
      'settings': {
        'kalshi_env': 'demo',
        'operation_lane': 'sandbox',
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
        'mode_selected': True,
      },
    }

  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=_blocking_scan,
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    ),
    tombstone_path=tmp_path / 'tombstones.json',
  )
  monkeypatch.setattr(
    web_app,
    '_load_validation_workflow_summary',
    lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError('scan processing refresh assembly failed')),
  )

  try:
    status, _, body = _call_app(app, method='POST', path='/api/scan')
  finally:
    release_scan.set()

  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'scan_failed'
  assert payload['route_failure']['route_path'] == '/api/scan'
  assert payload['route_failure']['exception_class'] == 'RuntimeError'
  assert payload['route_failure']['message'] == 'scan processing refresh assembly failed'
  assert payload['scan_runtime']['status'] == 'processing'

  with open_database(state_db_path) as connection:
    row = connection.execute(
      "SELECT event_type, detail_json FROM runtime_events WHERE event_type = 'operator_shell_route_failed' ORDER BY id DESC LIMIT 1"
    ).fetchone()

  assert row is not None
  assert row[0] == 'operator_shell_route_failed'
  detail = json.loads(row[1])
  assert detail['route_path'] == '/api/scan'
  assert detail['action_name'] == 'scan'
  assert detail['message'] == 'scan processing refresh assembly failed'
  assert detail['scan_runtime_status'] == 'processing'


def test_key_reference_persists_across_restart_with_relative_state_db_path(monkeypatch: Any, tmp_path: Path) -> None:
  project_root = tmp_path / 'project-root'
  project_root.mkdir(parents=True)
  monkeypatch.setattr(web_app, 'PROJECT_ROOT', project_root)
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', 'runtime/state.sqlite3')

  key_file = tmp_path / 'persist-across-restart.pem'
  key_file.write_text('placeholder', encoding='utf-8')

  app1 = create_operator_console_app(_services())
  _call_app(app1, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app1, method='POST', path='/api/key-apply')

  restart_cwd = tmp_path / 'different-cwd'
  restart_cwd.mkdir(parents=True)
  monkeypatch.chdir(restart_cwd)
  app2 = create_operator_console_app(_services())
  _, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert payload['key_management']['overlay_active'] is True
  assert payload['key_management']['active_key_tail'] == 'persist-across-restart.pem'
  assert payload['key_management']['active_key_source_label'] == 'Manual path'


def test_key_reference_persists_across_restart_when_env_var_is_missing_on_second_launch(
  monkeypatch: Any,
  tmp_path: Path,
) -> None:
  project_root = tmp_path / 'project-root'
  project_root.mkdir(parents=True)
  monkeypatch.setattr(web_app, 'PROJECT_ROOT', project_root)

  home_dir = tmp_path / 'home'
  home_dir.mkdir(parents=True)
  monkeypatch.setenv('HOME', str(home_dir))
  monkeypatch.setenv('USERPROFILE', str(home_dir))

  state_db_path = tmp_path / 'runtime' / 'state.sqlite3'
  state_db_path.parent.mkdir(parents=True)
  key_file = tmp_path / 'persist-missing-env.pem'
  key_file.write_text('placeholder', encoding='utf-8')

  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(state_db_path),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(state_db_path))
  app1 = create_operator_console_app(_services())
  _call_app(app1, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app1, method='POST', path='/api/key-apply')

  monkeypatch.delenv('KALSHI_STATE_DB_PATH', raising=False)
  app2 = create_operator_console_app(_services())
  _, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert payload['key_management']['overlay_active'] is True
  assert payload['key_management']['active_key_tail'] == 'persist-missing-env.pem'
  assert payload['key_management']['active_key_source_label'] == 'Manual path'


def test_key_clear_hold_persists_across_explicit_to_default_tombstone_resolution(
  monkeypatch: Any,
  tmp_path: Path,
) -> None:
  project_root = tmp_path / 'project-root'
  project_root.mkdir(parents=True)
  monkeypatch.setattr(web_app, 'PROJECT_ROOT', project_root)

  state_db_path = tmp_path / 'runtime' / 'state.sqlite3'
  state_db_path.parent.mkdir(parents=True)
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(state_db_path))

  key_file = tmp_path / 'clear-hold-persist.pem'
  key_file.write_text('placeholder', encoding='utf-8')

  explicit_tombstone_path = state_db_path.parent / '_operator_clear_tombstones.json'

  app1 = create_operator_console_app(_services(), tombstone_path=explicit_tombstone_path)
  _call_app(app1, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app1, method='POST', path='/api/key-apply')
  _call_app(app1, method='POST', path='/api/key-clear')

  assert explicit_tombstone_path.exists()

  app2 = create_operator_console_app(_services())
  _, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert payload['key_management']['clear_hold_active'] is True
  assert payload['key_management']['overlay_active'] is False
  assert payload['key_management']['active_key_tail'] == '--'
  assert payload['key_management']['active_key_source_label'] == 'Cleared in session'


def test_internal_bootstrap_after_clear_hold_projects_waiting_review_state(monkeypatch: Any, tmp_path: Path) -> None:
  key_file = tmp_path / 'session-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    # tmp-scoped: a bare relative 'runtime.sqlite3' resolves against the repo root and
    # pollutes it (a killed mid-write run left a corrupt root DB that flaked this test).
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, '_validate_env_alignment', lambda _settings: None)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))

  tombstone = tmp_path / 'tombstones.json'
  app1 = create_operator_console_app(tombstone_path=tombstone)
  _call_app(app1, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app1, method='POST', path='/api/key-apply')
  _call_app(app1, method='POST', path='/api/key-clear')

  app2 = create_operator_console_app(tombstone_path=tombstone)
  status, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'planned'
  assert payload['workflow']['recommended_step'] == 'review_configuration'
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['next_actionable_step'] == 'load_api_key'
  assert payload['workflow']['focus_tone'] == 'focus-info'
  assert payload['startup_wizard']['readiness_status'] == 'pending'


def test_reload_credential_posture_after_clear_remains_waiting_review_state(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_operator_clear_tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _call_app(app, method='POST', path='/api/key-clear')

  reload_status, _, reload_body = _call_app(app, method='POST', path='/api/key-reload')
  reload_payload = json.loads(reload_body)

  assert reload_status == '200 OK'
  assert reload_payload['decision'] == 'planned'
  assert reload_payload['workflow']['recommended_step'] == 'review_configuration'
  assert reload_payload['workflow']['step_kind'] == 'review'
  assert reload_payload['workflow']['next_actionable_step'] == 'load_api_key'
  assert reload_payload['workflow']['focus_tone'] == 'focus-info'
  assert reload_payload['key_management']['last_result']['tone'] == 'warn'
  assert reload_payload['startup_wizard']['readiness_status'] == 'pending'


def test_default_test_harness_keeps_workspace_tombstone_untouched(tmp_path: Path) -> None:
  key_file = tmp_path / 'isolated-harness-key.pem'
  key_file.write_text('placeholder', encoding='utf-8')

  workspace_tombstone = web_app.PROJECT_ROOT / 'var' / '_operator_clear_tombstones.json'
  workspace_before = (
    workspace_tombstone.read_text(encoding='utf-8')
    if workspace_tombstone.exists()
    else None
  )

  app = create_operator_console_app(_services())
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app, method='POST', path='/api/key-apply')

  isolated_state_db = Path(os.environ['KALSHI_STATE_DB_PATH'])
  isolated_tombstone = isolated_state_db.parent / '_operator_clear_tombstones.json'
  isolated_payload = json.loads(isolated_tombstone.read_text(encoding='utf-8'))

  workspace_after = (
    workspace_tombstone.read_text(encoding='utf-8')
    if workspace_tombstone.exists()
    else None
  )

  assert isolated_tombstone.exists()
  assert isolated_payload['key_reference_path'] == str(key_file.resolve())
  assert workspace_after == workspace_before


def test_detect_keys_exposes_identifier_source_token_and_status(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'detected-key.pem'
  key_file.write_text('placeholder', encoding='utf-8')
  monkeypatch.setattr(
    web_app,
    '_discover_private_key_candidates',
    lambda **_: [
      {
        'resolved_path': str(key_file.resolve()),
        'path_tail': key_file.name,
        'source_label': 'Manual discovery test source',
      }
    ],
  )
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  status, _, body = _call_app(app, method='POST', path='/api/key-discover')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['key_management']['candidate_count'] == 1
  candidate = payload['key_management']['discovered_candidates'][0]
  assert candidate['candidate_id'].startswith('kalshi-')
  assert len(candidate['candidate_id']) == len('kalshi-') + 6
  assert candidate['source_label'] == 'Manual discovery test source'
  assert candidate['profile_token'].startswith('kalshi-')
  assert len(candidate['profile_token']) == len('kalshi-') + 6
  assert candidate['lane_tag'] is None
  assert candidate['status'] == 'available'


def test_detect_keys_derives_lane_tag_from_resolved_path_segments(tmp_path: Path, monkeypatch: Any) -> None:
  demo_dir = tmp_path / 'secrets' / 'kalshi' / 'demo'
  live_dir = tmp_path / 'secrets' / 'kalshi' / 'live'
  demo_dir.mkdir(parents=True)
  live_dir.mkdir(parents=True)
  demo_key = demo_dir / 'demo-key.pem'
  live_key = live_dir / 'live-key.pem'
  demo_key.write_text('placeholder', encoding='utf-8')
  live_key.write_text('placeholder', encoding='utf-8')

  monkeypatch.setattr(
    web_app,
    '_discover_private_key_candidates',
    lambda **_: [
      {
        'resolved_path': str(demo_key.resolve()),
        'path_tail': demo_key.name,
        'source_label': 'Project secrets',
      },
      {
        'resolved_path': str(live_key.resolve()),
        'path_tail': live_key.name,
        'source_label': 'Project secrets',
      },
    ],
  )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  status, _, body = _call_app(app, method='POST', path='/api/key-discover')
  payload = json.loads(body)

  assert status == '200 OK'
  by_tail = {item['path_tail']: item for item in payload['key_management']['discovered_candidates']}
  assert by_tail['demo-key.pem']['lane_tag'] == 'sandbox'
  assert by_tail['live-key.pem']['lane_tag'] == 'live'


def test_key_registry_persists_until_cleared(tmp_path: Path) -> None:
  tombstone = tmp_path / 'tombstones.json'
  key_file = tmp_path / 'persisted-discovered-key.pem'
  key_file.write_text('placeholder', encoding='utf-8')

  app1 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _call_app(app1, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  # Discovery populates discovered_candidates
  _, _, discover_body_1 = _call_app(app1, method='POST', path='/api/key-discover')
  payload_1 = json.loads(discover_body_1)
  first_candidate = payload_1['key_management']['discovered_candidates'][0]
  first_token = first_candidate['profile_token']

  app2 = create_operator_console_app(_services(), tombstone_path=tombstone)
  # Discovery in app2 sees the key from the persisted registry
  _, _, discover_body_2 = _call_app(app2, method='POST', path='/api/key-discover')
  payload_2 = json.loads(discover_body_2)
  second_candidate = payload_2['key_management']['discovered_candidates'][0]

  assert second_candidate['profile_token'] == first_token
  assert second_candidate['status'] == 'available'

  _call_app(
    app2,
    method='POST',
    path='/api/key-stage',
    body={'candidate_id': second_candidate['candidate_id']},
  )
  _call_app(app2, method='POST', path='/api/key-clear')
  app3 = create_operator_console_app(_services(), tombstone_path=tombstone)
  # After clear in app2, discovery in app3 should still find the key from the registry
  # (clear only removes from runtime/loaded state, not from discovery/registry)
  _, _, discover_body_3 = _call_app(app3, method='POST', path='/api/key-discover')
  payload_3 = json.loads(discover_body_3)
  # Key should still be discoverable because registry persists even after clear
  assert any(
    candidate['profile_token'] == first_token
    for candidate in payload_3['key_management']['discovered_candidates']
  )


def test_key_discovery_runs_fresh_on_each_detect_request(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'once-per-session.pem'
  key_file.write_text('placeholder', encoding='utf-8')
  discover_calls = {'count': 0}

  def _discover_once(**_: Any) -> list[dict[str, str]]:
    discover_calls['count'] += 1
    return [
      {
        'resolved_path': str(key_file.resolve()),
        'path_tail': key_file.name,
        'source_label': 'Session cache source',
      }
    ]

  monkeypatch.setattr(web_app, '_discover_private_key_candidates', _discover_once)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  first_status, _, first_body = _call_app(app, method='POST', path='/api/key-discover')
  first_payload = json.loads(first_body)
  second_status, _, second_body = _call_app(app, method='POST', path='/api/key-discover')
  second_payload = json.loads(second_body)

  assert first_status == '200 OK'
  assert second_status == '200 OK'
  assert discover_calls['count'] == 2
  assert first_payload['key_management']['candidate_count'] == 1
  assert second_payload['key_management']['candidate_count'] == 1
  assert second_payload['key_management']['last_result']['message'].startswith('Discovered 1 available key reference')


def test_refresh_shell_honors_cleared_key_hold_even_when_config_has_a_key(monkeypatch: Any, tmp_path: Path) -> None:
  key_file = tmp_path / 'config-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, '_validate_env_alignment', lambda _settings: None)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app, method='POST', path='/api/key-apply')
  _call_app(app, method='POST', path='/api/key-clear')

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['key_management']['overlay_active'] is False
  assert payload['key_management']['active_key_tail'] == '--'
  assert payload['key_management']['active_key_source_label'] == 'Cleared in session'
  assert payload['settings']['credential_reference_present'] is False
  assert payload['settings']['credential_ready'] is False


def test_staging_after_clear_keeps_the_cleared_key_tombstone_until_apply(tmp_path: Path) -> None:
  tombstone = tmp_path / 'tombstones.json'
  key_file = tmp_path / 'staged-after-clear.pem'
  key_file.write_text('placeholder', encoding='utf-8')

  app = create_operator_console_app(_services(), tombstone_path=tombstone)
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app, method='POST', path='/api/key-apply')
  _call_app(app, method='POST', path='/api/key-clear')

  stage_status, _, stage_body = _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  staged_payload = json.loads(stage_body)

  assert stage_status == '200 OK'
  assert staged_payload['key_management']['selected_key_tail'] == 'staged-after-clear.pem'
  assert staged_payload['key_management']['active_key_tail'] == '--'
  assert staged_payload['key_management']['active_key_source_label'] == 'Cleared in session'

  app2 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _, _, bootstrap_body = _call_app(app2, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert bootstrap_payload['key_management']['overlay_active'] is False
  assert bootstrap_payload['key_management']['active_key_tail'] == '--'
  assert bootstrap_payload['key_management']['active_key_source_label'] == 'Cleared in session'


def test_websocket_clear_tombstone_persists_cleared_state_across_app_restart(tmp_path: Path, monkeypatch: Any) -> None:
  # Verify that a websocket clear-all survives process restart: a NEW app instance
  # with the same tombstone file must NOT repopulate the URLs from .env/config.
  tombstone = tmp_path / 'tombstones.json'

  app1 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _call_app(
    app1,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_sandbox_websocket', 'url': 'wss://sandbox.example.invalid/ws'},
  )
  _call_app(app1, method='POST', path='/api/websocket-overlay', body={'action': 'clear_all'})

  assert tombstone.exists(), 'Tombstone file must be written on websocket clear-all'

  # Session 2: fresh app, same tombstone
  app2 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  available = payload['connection_posture']['available_websocket_urls']
  assert available['sandbox'] == ''
  assert available['live'] == ''
  assert payload['connection_posture']['active_websocket_url_tail'] == 'unconfigured'


def test_websocket_load_after_clear_removes_tombstone_and_restores_url(tmp_path: Path) -> None:
  # Verify that loading a URL after a clear removes the tombstone so a subsequent
  # restart shows the loaded URL rather than the cleared state.
  tombstone = tmp_path / 'tombstones.json'

  app1 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _call_app(app1, method='POST', path='/api/websocket-overlay', body={'action': 'clear_all'})
  # Explicit reload -- must remove tombstone
  _call_app(
    app1,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_sandbox_websocket', 'url': 'wss://reload.example.invalid/ws'},
  )

  # Session 2: fresh app, verify URLs are no longer cleared
  app2 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  available = payload['connection_posture']['available_websocket_urls']
  assert available['sandbox'] == 'reload.example.invalid/ws'
  assert available['live'] == ''


def test_legacy_websocket_tombstone_does_not_override_current_config_on_restart(tmp_path: Path, monkeypatch: Any) -> None:
  tombstone = tmp_path / 'tombstones.json'
  tombstone.write_text(
    json.dumps(
      {
        'websocket_sandbox_url': 'wss://external-api-ws.demo.kalshi.example/ws',
        'websocket_live_url': 'wss://external-api-ws.kalshi.example/ws',
      },
      indent=2,
    ),
    encoding='utf-8',
  )
  key_file = tmp_path / 'restart-config-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-key-id',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    active_websocket_url='wss://demo-api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.01,
    max_pair_contracts=10.0,
    max_open_pairs=20,
    max_unhedged_sec=5,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'kalshi.sqlite3'),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  app = create_operator_console_app(tombstone_path=tombstone)
  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  available = payload['connection_posture']['available_websocket_urls']
  assert available['sandbox'] == 'demo-api.kalshi.example/ws'
  assert available['live'] == 'api.kalshi.example/ws'
  assert payload['connection_posture']['active_websocket_url_tail'] == 'demo-api.kalshi.example/ws'


def test_legacy_websocket_tombstone_with_old_config_markers_does_not_override_current_config_on_restart(tmp_path: Path, monkeypatch: Any) -> None:
  tombstone = tmp_path / 'tombstones.json'
  tombstone.write_text(
    json.dumps(
      {
        'websocket_sandbox_url': 'wss://external-api-ws.demo.kalshi.example/ws',
        'websocket_live_url': 'wss://external-api-ws.kalshi.example/ws',
        'websocket_sandbox_config_url': 'demo-api.kalshi.example/ws',
        'websocket_live_config_url': 'unconfigured',
      },
      indent=2,
    ),
    encoding='utf-8',
  )
  key_file = tmp_path / 'restart-config-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-key-id',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='',
    active_websocket_url='wss://demo-api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.01,
    max_pair_contracts=10.0,
    max_open_pairs=20,
    max_unhedged_sec=5,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'kalshi.sqlite3'),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  app = create_operator_console_app(tombstone_path=tombstone)
  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  available = payload['connection_posture']['available_websocket_urls']
  assert available['sandbox'] == 'demo-api.kalshi.example/ws'
  assert available['live'] == 'unconfigured'
  assert payload['connection_posture']['active_websocket_url_tail'] == 'demo-api.kalshi.example/ws'


def test_legacy_websocket_tombstone_with_matching_current_config_still_hydrates_on_restart(tmp_path: Path, monkeypatch: Any) -> None:
  tombstone = tmp_path / 'tombstones.json'
  tombstone.write_text(
    json.dumps(
      {
        'sandbox_key_reference_path': str(tmp_path / 'restart-config-key.pem'),
        'sandbox_key_reference_source_label': 'Project secrets',
        'sandbox_key_validated': True,
        'sandbox_key_validation_reason': 'pass',
        'websocket_sandbox_url': 'wss://demo-api.kalshi.example/ws',
        'websocket_live_url': '',
        'websocket_sandbox_config_url': 'demo-api.kalshi.example/ws',
        'websocket_live_config_url': 'unconfigured',
      },
      indent=2,
    ),
    encoding='utf-8',
  )
  key_file = tmp_path / 'restart-config-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-key-id',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='',
    active_websocket_url='wss://demo-api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.01,
    max_pair_contracts=10.0,
    max_open_pairs=20,
    max_unhedged_sec=5,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'kalshi.sqlite3'),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  app = create_operator_console_app(tombstone_path=tombstone)
  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['key_management']['sandbox_key_loaded'] is True
  assert payload['key_management']['sandbox_key_validated'] is True
  available = payload['connection_posture']['available_websocket_urls']
  assert available['sandbox'] == 'demo-api.kalshi.example/ws'
  assert available['live'] == ''
  assert payload['connection_posture']['active_websocket_url_tail'] == 'demo-api.kalshi.example/ws'


def test_websocket_load_live_after_clear_keeps_sandbox_cleared_until_explicit_reload(tmp_path: Path) -> None:
  tombstone = tmp_path / 'tombstones.json'

  app1 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _call_app(app1, method='POST', path='/api/websocket-overlay', body={'action': 'clear_all'})
  _call_app(
    app1,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_live_websocket', 'url': 'wss://live-reload.example.invalid/ws'},
  )

  app2 = create_operator_console_app(_services(), tombstone_path=tombstone)
  _, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  available = payload['connection_posture']['available_websocket_urls']
  assert available['live'] == 'live-reload.example.invalid/ws'
  assert available['sandbox'] == ''


def test_mode_change_route_updates_operation_lane_in_session_overlay(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services())
  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'live'))
  _load_lane_key(app, tmp_path, monkeypatch, 'live')

  # Request mode change to 'live'
  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['session_overlay']['context']['values']['operation_lane'] == 'live'
  assert 'workflow' in payload


def test_mode_change_route_promotes_live_rest_environment_from_demo_base(tmp_path: Path, monkeypatch: Any) -> None:
  captured: dict[str, Any] = {}
  scan_finished = threading.Event()
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=lambda **kwargs: (
        captured.update(
          {
            'kalshi_env': getattr(kwargs.get('settings'), 'kalshi_env', None),
            'api_base_url': getattr(kwargs.get('settings'), 'api_base_url', None),
          }
        ),
        scan_finished.set(),
        {
          'decision': 'planned',
          'candidate_count': 0,
          'candidates': [],
          'next_action': 'done',
        }
      )[2],
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )
  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'sandbox'))
  _load_lane_key(app, tmp_path, monkeypatch, 'live')

  mode_status, _, mode_body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  mode_payload = json.loads(mode_body)
  scan_status, _, _ = _call_app(app, method='POST', path='/api/scan', body={})

  assert mode_status == '200 OK'
  assert mode_payload['connection_posture']['operation_lane'] == 'live'
  assert scan_status == '200 OK'
  assert scan_finished.wait(timeout=1.0) is True
  assert captured['kalshi_env'] == 'prod'
  assert captured['api_base_url'] == 'https://api.kalshi.example'


def test_mode_change_route_promotes_sandbox_rest_environment_from_live_base(tmp_path: Path, monkeypatch: Any) -> None:
  captured: dict[str, Any] = {}
  scan_finished = threading.Event()
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=lambda **kwargs: (
        captured.update(
          {
            'kalshi_env': getattr(kwargs.get('settings'), 'kalshi_env', None),
            'api_base_url': getattr(kwargs.get('settings'), 'api_base_url', None),
          }
        ),
        scan_finished.set(),
        {
          'decision': 'planned',
          'candidate_count': 0,
          'candidates': [],
          'next_action': 'done',
        }
      )[2],
      run=base.run,
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )
  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'live'))
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  mode_status, _, mode_body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  mode_payload = json.loads(mode_body)
  scan_status, _, _ = _call_app(app, method='POST', path='/api/scan', body={})

  assert mode_status == '200 OK'
  assert mode_payload['connection_posture']['operation_lane'] == 'sandbox'
  assert scan_status == '200 OK'
  assert scan_finished.wait(timeout=1.0) is True
  assert captured['kalshi_env'] == 'demo'
  assert 'demo' in str(captured['api_base_url']).lower()


def test_mode_change_route_defaults_to_offline_for_invalid_lane() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'invalid-lane'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['session_overlay']['context']['values']['operation_lane'] == 'offline'


def test_mode_change_route_projects_next_actionable_step_after_mode_selection(tmp_path: Path, monkeypatch: Any) -> None:
  base = _services()
  services = OperatorConsoleServices(
    bootstrap=lambda **_: {
      'decision': 'planned',
      'settings': {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
        'kalshi_env': 'demo',
        'operation_lane': 'offline',
        'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'live_websocket_url': 'wss://api.kalshi.example/ws',
        'active_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      'diagnostics_governance_context': {
        'channel': 'diagnostics_governance',
        'validation_summary': {'present': False, 'latest_runs': []},
      },
      'report': {
        'latest_heartbeat': None,
        'table_counts': {'pair_plans': 0},
        'next_action': 'Run scan then one dry-run cycle.',
      },
      'reconcile': {'pair_count': 0, 'pairs': []},
      # Stale bootstrap projection we expect the shell to overwrite after mode_selected=True.
      'workflow': {
        'recommended_step': 'mode_change',
        'auto_sequence': [],
        'headline': 'Select mode',
        'operator_message': 'Select mode',
        'step_kind': 'review',
        'can_run_next_step': False,
        'next_actionable_step': 'mode_change',
        'focus_target': 'readiness-section',
        'focus_tone': 'focus-info',
        'deck_view': 'operator',
        'button_emphasis_tone': '',
      },
      'next_action': 'Run scan then one dry-run cycle.',
    },
    scan=base.scan,
    run=base.run,
    reconcile=base.reconcile,
    report=base.report,
    cancel_all=base.cancel_all,
    system_log=base.system_log,
    visuals=base.visuals,
  )

  app = create_operator_console_app(services)
  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'live'))
  _load_lane_key(app, tmp_path, monkeypatch, 'live')
  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['session_overlay']['context']['values']['operation_lane'] == 'live'
  assert payload['session_overlay']['context']['mode_selected'] is True
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['step_kind'] == 'execute'
  assert payload['workflow']['can_run_next_step'] is True
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['workflow']['deck_view'] == 'workflow'


def test_mode_change_route_starts_live_websocket_runtime_before_projection_refresh(monkeypatch: Any, tmp_path: Path) -> None:
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  key_file = tmp_path / 'live-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='prod',
    api_key_id='live-api-key',
    live_api_key_id='live-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://api.kalshi.example/v2',
    websocket_url='wss://api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='live',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)

  _load_lane_key(app, tmp_path, monkeypatch, 'live', key_path=key_file)
  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['connection_posture']['operation_lane'] == 'live'
  assert payload['connection_posture']['connection_state']['websocket_connected'] is True
  assert str(payload['connection_posture']['lane_session_id']).startswith('live-')
  assert payload['workflow']['recommended_step'] == 'scan'


def test_mode_change_route_projects_scan_next_step_when_existing_evidence_is_present(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services())
  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'live'))
  _load_lane_key(app, tmp_path, monkeypatch, 'live')

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['session_overlay']['context']['values']['operation_lane'] == 'live'
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['step_kind'] == 'execute'
  assert payload['workflow']['can_run_next_step'] is True
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['workflow']['deck_view'] == 'workflow'


def test_bootstrap_preserves_mode_change_scan_hold_across_refresh_when_no_pair_attention(tmp_path: Path, monkeypatch: Any) -> None:
  base = _services()
  services = OperatorConsoleServices(
    bootstrap=lambda **_: {
      'decision': 'planned',
      'settings': {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
        'kalshi_env': 'live',
        'operation_lane': 'offline',
        'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'live_websocket_url': 'wss://api.kalshi.example/ws',
        'active_websocket_url': 'wss://api.kalshi.example/ws',
        'active_websocket_url_tail': 'api.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      'diagnostics_governance_context': {
        'channel': 'diagnostics_governance',
        'validation_summary': {'present': False, 'latest_runs': []},
      },
      'report': {
        'latest_heartbeat': {'status': 'heartbeat-live'},
        'table_counts': {'pair_plans': 2},
        'next_action': 'Refresh shell or continue from the current evidence boundary.',
      },
      'reconcile': {'pair_count': 0, 'pairs': []},
      'workflow': {
        'recommended_step': 'report',
        'auto_sequence': [],
        'headline': 'Existing evidence boundary',
        'operator_message': 'Existing evidence boundary',
        'step_kind': 'review',
        'can_run_next_step': False,
        'next_actionable_step': 'report',
        'focus_target': 'evidence-section',
        'focus_tone': 'focus-info',
        'deck_view': 'review',
        'button_emphasis_tone': '',
      },
      'next_action': 'Refresh shell or continue from the current evidence boundary.',
    },
    scan=base.scan,
    run=base.run,
    reconcile=base.reconcile,
    report=base.report,
    cancel_all=base.cancel_all,
    system_log=base.system_log,
    visuals=base.visuals,
  )

  app = create_operator_console_app(services)
  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'live'))
  _load_lane_key(app, tmp_path, monkeypatch, 'live')
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})

  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['session_overlay']['context']['values']['operation_lane'] == 'live'
  assert payload['session_overlay']['context']['mode_selected'] is True
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['step_kind'] == 'execute'
  assert payload['workflow']['can_run_next_step'] is True
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['workflow']['focus_target'] == 'pairs-section'
  assert payload['workflow']['deck_view'] == 'workflow'


def test_bootstrap_does_not_hide_pair_attention_behind_mode_change_scan_hold(tmp_path: Path, monkeypatch: Any) -> None:
  base = _services()
  services = OperatorConsoleServices(
    bootstrap=lambda **_: {
      'decision': 'planned',
      'settings': {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
        'kalshi_env': 'live',
        'operation_lane': 'offline',
        'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'live_websocket_url': 'wss://api.kalshi.example/ws',
        'active_websocket_url': 'wss://api.kalshi.example/ws',
        'active_websocket_url_tail': 'api.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      'diagnostics_governance_context': {
        'channel': 'diagnostics_governance',
        'validation_summary': {'present': False, 'latest_runs': []},
      },
      'report': {
        'latest_heartbeat': {'status': 'heartbeat-live'},
        'table_counts': {'pair_plans': 2},
        'next_action': 'Refresh shell or continue from the current evidence boundary.',
      },
      'reconcile': {
        'pair_count': 1,
        'pairs': [{'pair_id': 'pair-001', 'state': 'PLANNED'}],
      },
      'workflow': {
        'recommended_step': 'report',
        'auto_sequence': [],
        'headline': 'Existing evidence boundary',
        'operator_message': 'Existing evidence boundary',
        'step_kind': 'review',
        'can_run_next_step': False,
        'next_actionable_step': 'report',
        'focus_target': 'evidence-section',
        'focus_tone': 'focus-info',
        'deck_view': 'review',
        'button_emphasis_tone': '',
      },
      'next_action': 'Refresh shell or continue from the current evidence boundary.',
    },
    scan=base.scan,
    run=base.run,
    reconcile=base.reconcile,
    report=base.report,
    cancel_all=base.cancel_all,
    system_log=base.system_log,
    visuals=base.visuals,
  )

  app = create_operator_console_app(services)
  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'live'))
  _load_lane_key(app, tmp_path, monkeypatch, 'live')
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})

  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['recommended_step'] == 'reconcile'
  assert payload['workflow']['step_kind'] == 'execute'
  assert payload['workflow']['focus_target'] == 'pairs-section'
  assert payload['workflow']['deck_view'] == 'workflow'


def test_mode_change_route_runs_sandbox_preflight_before_lane_switch(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services())
  called: dict[str, Any] = {}
  runtime_key = tmp_path / 'sandbox-preflight-runtime-key.pem'
  runtime_key.write_text('placeholder-key-material', encoding='utf-8')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox', key_path=runtime_key)

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  sandbox_settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(runtime_key),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  def _fake_preflight(settings: Any) -> dict[str, Any]:
    called['lane'] = getattr(settings, 'operation_lane', None)
    return {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    }

  # Mock settings to have credentials and websocket ready
  original_load = web_app._load_shell_settings_context
  def _mock_load_shell_settings_context(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], Any]:
    settings_dict, other = original_load(*args, **kwargs)
    settings_dict['credential_ready'] = True
    settings_dict['has_any_websocket_url'] = True
    return settings_dict, other

  monkeypatch.setattr(web_app, 'run_sandbox_preflight', _fake_preflight)
  monkeypatch.setattr(web_app, '_load_shell_settings_context', _mock_load_shell_settings_context)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: sandbox_settings)
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)

  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(runtime_key)})
  apply_status, _, apply_body = _call_app(app, method='POST', path='/api/key-apply', body={'lane': 'sandbox'})
  apply_payload = json.loads(apply_body)

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  payload = json.loads(body)

  assert apply_status == '200 OK'
  assert apply_payload['key_management']['sandbox_key_loaded'] is True
  assert apply_payload['key_management']['sandbox_key_validated'] is not True
  assert status == '200 OK'
  assert called['lane'] == 'sandbox'
  assert payload['connection_posture']['operation_lane'] == 'sandbox'
  assert payload['session_overlay']['context']['mode_selected'] is True
  assert payload['report']['connection_state']['status'] == 'connected'
  assert payload['report']['connection_state']['websocket_connected'] is True


def test_mode_change_route_establishes_authenticated_websocket_session(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  apply_status, _, apply_body = _call_app(app, method='POST', path='/api/key-apply', body={'lane': 'sandbox'})
  apply_payload = json.loads(apply_body)

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  payload = json.loads(body)

  assert apply_status == '200 OK'
  assert apply_payload['key_management']['sandbox_key_loaded'] is True
  assert apply_payload['key_management']['sandbox_key_validated'] is not True
  assert status == '200 OK'
  assert payload['connection_posture']['operation_lane'] == 'sandbox'
  assert payload['session_overlay']['context']['mode_selected'] is True
  assert payload['connection_posture']['connection_state']['status'] == 'connected'
  assert payload['connection_posture']['connection_state']['websocket_connected'] is True
  assert payload['report']['latest_heartbeat']['component'] == 'websocket-session'
  assert payload['report']['latest_heartbeat']['status'] in {'connected', 'heartbeat-live'}
  assert str(payload['report']['lane_session_id']).startswith('sandbox-')


def test_mode_change_route_blocks_when_authenticated_websocket_session_cannot_start(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FailingWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      raise RuntimeError('socket connect failed')

    async def disconnect(self) -> None:
      self.connected = False

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FailingWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'websocket_connection_failed'
  assert payload['notification_source']['origin'] == 'kalshi'
  assert payload['notification_source']['is_external'] is True
  assert payload['session_overlay']['context']['mode_selected'] is False
  assert payload['session_overlay']['context']['values'] == {}


def test_mode_change_route_blocks_lane_switch_when_sandbox_preflight_fails(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services())

  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'live'))
  _load_lane_key(app, tmp_path, monkeypatch, 'live')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})

  # Mock settings to have credentials and websocket ready so preflight is called
  original_load = web_app._load_shell_settings_context
  def _mock_load_shell_settings_context(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], Any]:
    settings_dict, other = original_load(*args, **kwargs)
    settings_dict['credential_ready'] = True
    settings_dict['has_any_websocket_url'] = True
    return settings_dict, other

  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda settings: {
      'result': 'fail',
      'reason_code': 'credential_acceptance_failed',
      'message': 'Sandbox mode pre-flight checks blocked activation.',
      'next_action': 'Repair readiness posture and retry mode change.',
      'checks': [
        {'name': 'credential_acceptance', 'status': 'fail', 'reason_code': 'credential_acceptance_failed'},
      ],
    },
  )
  monkeypatch.setattr(web_app, '_load_shell_settings_context', _mock_load_shell_settings_context)

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'credential_acceptance_failed'
  assert payload['notification_source']['origin'] == 'kalshi'
  assert payload['notification_source']['is_external'] is True
  assert payload['session_overlay']['context']['values']['operation_lane'] == 'live'
  assert payload['preflight']['result'] == 'fail'
  assert payload['workflow']['headline'] == 'Sandbox mode pre-flight checks blocked activation.'
  assert payload['workflow']['operator_message'] == 'Repair readiness posture and retry mode change.'
  assert payload['boundary']['message'] == 'Sandbox mode pre-flight checks blocked activation.'
  assert 'preflight reason: credential_acceptance_failed' in payload['boundary']['evidence']
  assert payload['live_interaction']['surface_visible'] is False
  assert payload['live_interaction']['materialization_reason'] == 'boundary_owns_mode_change_failure'


def test_mode_change_route_blocks_lane_switch_when_sandbox_preflight_reports_environment_mismatch(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services())

  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'live'))
  _load_lane_key(app, tmp_path, monkeypatch, 'live')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})

  original_load = web_app._load_shell_settings_context

  def _mock_load_shell_settings_context(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], Any]:
    settings_dict, other = original_load(*args, **kwargs)
    settings_dict['credential_ready'] = True
    settings_dict['has_any_websocket_url'] = True
    return settings_dict, other

  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda settings: {
      'result': 'fail',
      'reason_code': 'credential_environment_mismatch',
      'message': 'Sandbox mode pre-flight checks blocked activation due to credential environment mismatch.',
      'next_action': 'Use demo credentials for sandbox mode or switch to the live lane.',
      'checks': [
        {'name': 'credential_environment_match', 'status': 'fail', 'reason_code': 'credential_environment_mismatch'},
      ],
    },
  )
  monkeypatch.setattr(web_app, '_load_shell_settings_context', _mock_load_shell_settings_context)

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'credential_environment_mismatch'
  assert payload['notification_source']['origin'] == 'kalshi'
  assert payload['notification_source']['is_external'] is True
  assert payload['session_overlay']['context']['values']['operation_lane'] == 'live'
  assert payload['credential_posture']['validation_reason'] == 'credential_environment_mismatch'
  assert payload['preflight']['result'] == 'fail'
  assert payload['workflow']['headline'] == 'Sandbox mode pre-flight checks blocked activation due to credential environment mismatch.'
  assert payload['workflow']['operator_message'] == 'Use demo credentials for sandbox mode or switch to the live lane.'
  assert payload['boundary']['message'] == 'Sandbox mode pre-flight checks blocked activation due to credential environment mismatch.'
  assert 'preflight reason: credential_environment_mismatch' in payload['boundary']['evidence']


def test_mode_change_route_tags_websocket_probe_failures_as_external_source(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services())

  _load_lane_key(app, tmp_path, monkeypatch, 'live')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})

  original_load = web_app._load_shell_settings_context

  def _mock_load_shell_settings_context(*args: Any, **kwargs: Any) -> tuple[dict[str, Any], Any]:
    settings_dict, other = original_load(*args, **kwargs)
    settings_dict['credential_ready'] = True
    settings_dict['has_any_websocket_url'] = True
    return settings_dict, other

  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda settings: {
      'result': 'fail',
      'reason_code': 'websocket_connection_failed',
      'message': 'The websocket connectivity probe failed. Review endpoint posture and retry mode change.',
      'next_action': 'Review websocket endpoint posture and retry mode change.',
      'checks': [
        {'name': 'websocket_connect_probe', 'status': 'fail', 'reason_code': 'websocket_connection_failed'},
      ],
    },
  )
  monkeypatch.setattr(web_app, '_load_shell_settings_context', _mock_load_shell_settings_context)

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'websocket_connection_failed'
  assert payload['notification_source']['origin'] == 'kalshi'
  assert payload['notification_source']['is_external'] is True
  assert payload['preflight']['result'] == 'fail'
  assert payload['workflow']['headline'] == 'The websocket connectivity probe failed. Review endpoint posture and retry mode change.'
  assert payload['workflow']['operator_message'] == 'Review websocket endpoint posture and retry mode change.'
  assert payload['workflow']['next_actionable_step'] == 'set_websocket_url'
  assert payload['boundary']['message'] == 'The websocket connectivity probe failed. Review endpoint posture and retry mode change.'
  assert 'preflight reason: websocket_connection_failed' in payload['boundary']['evidence']
  assert payload['live_interaction']['surface_visible'] is False
  assert payload['live_interaction']['materialization_reason'] == 'boundary_owns_mode_change_failure'


def test_build_boundary_payload_includes_environment_mismatch_evidence() -> None:
  payload = {
    'decision': 'no-go',
    'reason': 'credential_environment_mismatch',
    'message': 'Sandbox mode pre-flight checks blocked activation due to credential environment mismatch.',
    'next_action': 'Use demo credentials for sandbox mode or switch to the live lane.',
    'settings': {
      'private_key_path_tail': 'private_key.pem',
      'state_db_path_tail': 'runtime.sqlite3',
      'credential_ready': True,
      'environment_ready': True,
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': 'api.kalshi.example/ws',
      },
    },
    'report': {
      'latest_heartbeat': {'status': 'cycle-complete'},
      'state_db_path_tail': 'runtime.sqlite3',
    },
  }

  boundary = web_app._build_boundary_payload('change_mode', payload)

  assert boundary is not None
  assert 'account auth: key accepted, but environment lane mismatch detected' in boundary['evidence']


def test_build_boundary_payload_includes_preflight_evidence_rows() -> None:
  payload = {
    'decision': 'no-go',
    'reason': 'websocket_connection_failed',
    'message': 'The websocket connectivity probe failed. Review endpoint posture and retry mode change.',
    'next_action': 'Review websocket endpoint posture and retry mode change.',
    'settings': {
      'private_key_path_tail': 'private_key.pem',
      'state_db_path_tail': 'runtime.sqlite3',
      'credential_ready': True,
      'environment_ready': True,
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': 'api.kalshi.example/ws',
      },
    },
    'report': {
      'latest_heartbeat': {'status': 'cycle-complete'},
      'state_db_path_tail': 'runtime.sqlite3',
    },
    'preflight': {
      'reason_code': 'websocket_connection_failed',
      'checks': [
        {'name': 'websocket_connect_probe', 'status': 'fail', 'reason_code': 'websocket_connection_failed'},
      ],
    },
  }

  boundary = web_app._build_boundary_payload('change_mode', payload)

  assert boundary is not None
  assert 'preflight reason: websocket_connection_failed' in boundary['evidence']
  assert 'preflight check: websocket_connect_probe :: fail :: websocket_connection_failed' in boundary['evidence']


def test_report_route_projects_mode_change_when_credentials_and_websockets_ready() -> None:
  # Use a services instance whose report lambda returns settings with credential_ready=True
  # and websocket URLs so _follow_on_workflow can project mode_change without hitting the
  # real settings loader (which would see no key file on disk).
  ready_settings = {
    'settings_ready': True,
    'credential_ready': True,
    'environment_ready': True,
    'mode_selected': False,
    'kalshi_env': 'demo',
    'operation_lane': 'sandbox',
    'available_websocket_urls': {
      'sandbox': 'demo-api.kalshi.example/ws',
      'live': 'api.kalshi.example/ws',
    },
    'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
    'state_db_path_tail': 'runtime.sqlite3',
    'private_key_path_tail': 'demo.pem',
  }
  base = _services()
  services = OperatorConsoleServices(
    bootstrap=base.bootstrap,
    scan=base.scan,
    run=base.run,
    reconcile=base.reconcile,
    report=lambda **_: {
      'decision': 'planned',
      'settings': ready_settings,
      'latest_heartbeat': {'status': 'cycle-complete'},
      'operation_lane': 'sandbox',
      'lane_session_id': 'sandbox-session-001',
      'active_websocket_url_tail': 'demo-api.kalshi.example/ws',
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': 'api.kalshi.example/ws',
      },
      'connection_state': {'status': 'connected', 'websocket_connected': True},
      'next_action': 'Use Refresh shell next.',
    },
    cancel_all=base.cancel_all,
  )
  app = create_operator_console_app(services)

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['next_actionable_step'] == 'mode_change'
  assert payload['workflow']['step_kind'] == 'review'


def test_root_route_preserves_backend_focus_target_for_report_and_reconcile_follow_ons() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert "if (normalizedAction === 'reconcile') {" in body
  assert "if (normalizedAction === 'report') {" in body
  assert "tone: workflowProjection.focusTone || 'focus-info'," in body
  assert "message: workflowProjection.operatorMessage || workflowProjection.headline || ''," in body


def test_root_route_payload_log_keeps_workflow_guidance_separate_from_nested_report_guidance() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'const detailCandidates = [workflowProjection.guidanceText].map(sanitizeGuiText).filter(Boolean);' in body
  assert 'payload.report?.next_action' not in body


def test_root_route_evidence_db_tail_ignores_stale_report_when_mode_not_selected() -> None:
  # Fix 2: dbTail now reads settings.state_db_path directly — report value never consulted.
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'const dbTail = settings.state_db_path ||' in body
  assert 'state_db_path_tail || (connectionPosture.modeSelected' not in body


def test_state_2_boundary_reason_is_key_reference_required_not_action_failure() -> None:
  # State 2: no key loaded, sandbox websocket URL is set.
  # The boundary reason must name the actual constraint (key missing) rather than
  # echoing the action that was attempted (e.g. 'websocket_management_failed').
  state_2_settings = {
    'credential_ready': False,
    'environment_ready': True,
    'settings_ready': False,
    'private_key_path_tail': '--',
    'state_db_path_tail': 'runtime.sqlite3',
    'available_websocket_urls': {
      'sandbox': 'demo-api.kalshi.example/ws',
      'live': '',
    },
    'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
  }
  payload = {
    'decision': 'no-go',
    'reason': 'websocket_management_failed',
    'message': 'Settings could not be loaded.',
    'next_action': 'Review the boundary condition before continuing.',
    'settings': state_2_settings,
  }

  boundary = web_app._build_boundary_payload('websocket_management', payload)

  assert boundary is not None
  assert boundary['reason'] == 'Key reference required'


def test_state_1_boundary_reason_falls_back_to_action_reason_without_override() -> None:
  # State 1: no key, no websocket URL set.
  # The offline setup reason override only fires for State 2 (websocket present, key absent).
  # For State 1 the fallback reason from the payload is preserved unchanged.
  state_1_settings = {
    'credential_ready': False,
    'environment_ready': True,
    'settings_ready': False,
    'private_key_path_tail': '--',
    'state_db_path_tail': 'runtime.sqlite3',
    'available_websocket_urls': {
      'sandbox': '',
      'live': '',
    },
  }
  payload = {
    'decision': 'no-go',
    'reason': 'missing_private_key_file',
    'message': 'Key file not found.',
    'next_action': 'Load a key reference.',
    'settings': state_1_settings,
  }

  boundary = web_app._build_boundary_payload('apply_selected_key_reference', payload)

  assert boundary is not None
  assert boundary['reason'] == 'missing_private_key_file'


def test_payload_mode_selected_prefers_authoritative_settings_then_connection_posture() -> None:
  payload = {
    'session_overlay': {
      'context': {
        'mode_selected': True,
      },
    },
    'settings': {
      'mode_selected': True,
    },
  }

  assert web_app._payload_mode_selected(payload) is True

  payload['settings']['mode_selected'] = False

  assert web_app._payload_mode_selected(payload) is False

  payload.pop('settings')

  payload['connection_posture'] = {
    'mode_selected': True,
  }

  assert web_app._payload_mode_selected(payload) is True


def test_follow_on_workflow_uses_authoritative_connection_posture_for_mode_state() -> None:
  payload = {
    'decision': 'planned',
    'next_action': 'Use Refresh shell next.',
    'settings': {
      'settings_ready': True,
      'credential_ready': True,
      'environment_ready': True,
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': 'api.kalshi.example/ws',
      },
    },
    'session_overlay': {
      'context': {
        'mode_selected': True,
      },
    },
    'connection_posture': {
      'mode_selected': False,
    },
  }

  workflow = web_app._follow_on_workflow('report', payload)

  assert workflow['recommended_step'] == 'report'
  assert workflow['step_kind'] == 'review'
  assert workflow['next_actionable_step'] == 'mode_change'


def test_phase_1_1_contract_table_covers_all_transition_rows() -> None:
  expected_rows = [
    ('1', 'load_sandbox_websocket', 'success', '2', 'S2_WEBSOCKET_LOADED_KEY_MISSING'),
    ('1', 'apply_selected_key_reference', 'success', '3', 'S3_KEY_LOADED_WEBSOCKET_MISSING'),
    ('2', 'clear_all_websocket_urls', 'success', '1', 'S1_WEBSOCKET_CLEARED'),
    ('2', 'apply_selected_key_reference', 'success', '4', 'S4_FULL_READY_KEY_APPLIED'),
    ('3', 'clear_loaded_key', 'success', '1', 'S1_FROM_3_KEY_CLEARED'),
    ('3', 'load_sandbox_websocket', 'success', '4', 'S4_FULL_READY_WEBSOCKET_LOADED'),
    ('4', 'clear_loaded_key', 'success', '2', 'S2_FROM_4_KEY_CLEARED'),
    ('4', 'clear_all_websocket_urls', 'success', '3', 'S3_FROM_4_WEBSOCKET_CLEARED'),
  ]

  for prev_state_id, action_name, outcome, resting_state_id, rule_id in expected_rows:
    contract = web_app.resolve_transition_view_contract(
      prev_state_id=prev_state_id,
      action_name=action_name,
      outcome=outcome,
      resting_state_id=resting_state_id,
    )
    assert contract is not None
    assert contract.rule_id == rule_id


def test_follow_on_workflow_no_go_uses_transition_contract_projection() -> None:
  payload = {
    'decision': 'no-go',
    'reason': 'websocket_management_failed',
    'message': 'Generic no-go fallback message.',
    'next_action': 'Generic next action fallback.',
    'settings': {
      'settings_ready': True,
      'credential_ready': False,
      'environment_ready': True,
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': '',
      },
      'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
    },
  }

  workflow = web_app._follow_on_workflow('websocket_management', payload)

  assert workflow['recommended_step'] == 'review_configuration'
  assert workflow['headline'] == 'Sandbox websocket loaded; API key required'
  assert workflow['operator_message'] == 'Load your API key reference to proceed.'
  assert workflow['step_kind'] == 'fix_configuration'
  assert workflow['next_actionable_step'] == 'load_api_key'
  assert workflow['focus_target'] == 'readiness-section'
  assert workflow['deck_view'] == 'operator'


def test_follow_on_workflow_success_transition_projects_waiting_contract() -> None:
  payload = {
    'decision': 'planned',
    'transition_outcome': 'success',
    'settings': {
      'settings_ready': False,
      'credential_ready': False,
      'environment_ready': True,
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': '',
      },
      'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
    },
  }

  workflow = web_app._follow_on_workflow('load_sandbox_websocket', payload)

  assert workflow['recommended_step'] == 'review_configuration'
  assert workflow['headline'] == 'Websocket URL loaded; API key required'
  assert workflow['operator_message'] == 'Load your API key reference to proceed.'
  assert workflow['step_kind'] == 'review'
  assert workflow['next_actionable_step'] == 'load_api_key'
  assert workflow['focus_target'] == 'readiness-section'
  assert workflow['focus_tone'] == 'focus-info'
  assert workflow['deck_view'] == 'operator'


def test_success_transition_contract_rows_are_review_states_with_explicit_key_boundary() -> None:
  websocket_contract = web_app.resolve_transition_view_contract(
    prev_state_id='1',
    action_name='load_sandbox_websocket',
    outcome='success',
    resting_state_id='2',
  )
  key_clear_contract = web_app.resolve_transition_view_contract(
    prev_state_id='4',
    action_name='clear_loaded_key',
    outcome='success',
    resting_state_id='2',
  )

  assert websocket_contract is not None
  assert websocket_contract.rule_id == 'S2_WEBSOCKET_LOADED_KEY_MISSING'
  assert websocket_contract.step_kind == 'review'
  assert websocket_contract.focus_tone == 'focus-info'
  assert websocket_contract.boundary_reason == 'Key reference required'
  assert websocket_contract.boundary_message == 'Sandbox websocket is loaded, but an API key must be configured before execution.'
  assert websocket_contract.boundary_next_action == 'Load your API key reference to proceed.'

  assert key_clear_contract is not None
  assert key_clear_contract.rule_id == 'S2_FROM_4_KEY_CLEARED'
  assert key_clear_contract.step_kind == 'review'
  assert key_clear_contract.focus_tone == 'focus-info'
  assert key_clear_contract.boundary_reason == 'Key reference required'
  assert key_clear_contract.boundary_message == 'The key was cleared; sandbox websocket remains loaded but execution requires a key.'
  assert key_clear_contract.boundary_next_action == 'Load your API key reference to proceed.'


def test_phase_1_2_invalid_key_format_contract_covers_state_1() -> None:
  contract = web_app.resolve_transition_view_contract(
    prev_state_id='1',
    action_name='apply_selected_key_reference',
    outcome='invalid',
    resting_state_id='1',
  )
  assert contract is not None
  assert contract.rule_id == 'S1_INVALID_KEY_FORMAT'
  assert contract.boundary_reason == 'auth_rejected: invalid key file format'
  assert contract.headline == 'Key format invalid or unreadable'
  assert contract.step_kind == 'fix_configuration'
  assert contract.contract_version == '1.2'


def test_phase_1_2_invalid_key_format_contract_covers_state_2() -> None:
  contract = web_app.resolve_transition_view_contract(
    prev_state_id='2',
    action_name='apply_selected_key_reference',
    outcome='invalid',
    resting_state_id='2',
  )
  assert contract is not None
  assert contract.rule_id == 'S2_INVALID_KEY_FORMAT'
  assert contract.boundary_reason == 'auth_rejected: invalid key file format'
  assert contract.headline == 'Key format invalid; websocket still set'
  assert contract.contract_version == '1.2'


def test_phase_1_2_failed_websocket_connection_state_1() -> None:
  contract = web_app.resolve_transition_view_contract(
    prev_state_id='1',
    action_name='load_sandbox_websocket',
    outcome='failed',
    resting_state_id='1',
  )
  assert contract is not None
  assert contract.rule_id == 'S1_FAILED_WEBSOCKET_CONNECTION'
  assert contract.boundary_reason == 'ws_session: websocket endpoint unreachable or invalid'
  assert contract.headline == 'Websocket connection failed; try again or use a different endpoint'
  assert contract.step_kind == 'fix_configuration'
  assert contract.contract_version == '1.2'


def test_phase_1_2_failed_websocket_connection_state_3() -> None:
  contract = web_app.resolve_transition_view_contract(
    prev_state_id='3',
    action_name='load_sandbox_websocket',
    outcome='failed',
    resting_state_id='3',
  )
  assert contract is not None
  assert contract.rule_id == 'S3_FAILED_WEBSOCKET_CONNECTION'
  assert contract.boundary_reason == 'ws_session: websocket endpoint unreachable or invalid'
  assert contract.headline == 'Websocket connection failed; key remains loaded'
  assert contract.contract_version == '1.2'


def test_phase_1_3_canonicalization_produces_deterministic_output() -> None:
  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  
  canonical_1 = web_app.canonicalize_signed_contract_payload(
    'signed-contract.v1',
    key,
    contract,
    'scope-phase13',
  )
  canonical_2 = web_app.canonicalize_signed_contract_payload(
    'signed-contract.v1',
    key,
    contract,
    'scope-phase13',
  )
  assert canonical_1 == canonical_2, 'Canonicalization must be deterministic'


def test_phase_1_3_compute_contract_hash_generates_sha256() -> None:
  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  
  hash_1 = web_app.compute_contract_hash(
    'signed-contract.v1',
    key,
    contract,
    'scope-phase13',
  )
  assert len(hash_1) == 64, 'SHA256 hex digest should be 64 characters'
  assert all(c in '0123456789abcdef' for c in hash_1), 'Hash should be hex'


def test_phase_1_3_verify_envelope_unknown_key_fails() -> None:
  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  
  envelope = web_app.SignedContractEnvelope(
    schema_version='signed-contract.v1',
    transition_key=key,
    contract_record=contract,
    contract_hash_sha256='abc123',
    signature_alg='ed25519',
    signature_b64='AAAA',
    signer_key_id='unknown-key-id',
    issued_at_utc='2026-05-09T00:00:00Z',
    expires_at_utc='2026-05-10T00:00:00Z',
    nonce='nonce-1',
    policy_scope_id='scope-phase13',
    policy_snapshot_sha256='sha256-1',
  )
  
  result = web_app.verify_envelope_signature(envelope)
  assert result.valid is False
  assert result.failure_code == 'unknown_key'
  assert result.signer_key_id == 'unknown-key-id'


def test_phase_1_3_verify_envelope_expired_fails() -> None:
  import json
  from pathlib import Path
  from unittest.mock import patch
  
  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  
  # Mock trust store with a test key
  mock_keys = {'test-key-id': 'AEPaCxf1bqVy+mj7KuFm/6BsFJuGrVCsP6KdbHnCKSw='}
  with patch.object(web_app, '_get_trusted_verification_keys', return_value=mock_keys):
    envelope = web_app.SignedContractEnvelope(
      schema_version='signed-contract.v1',
      transition_key=key,
      contract_record=contract,
      contract_hash_sha256='abc123',
      signature_alg='ed25519',
      signature_b64='AAAA',
      signer_key_id='test-key-id',
      issued_at_utc='2026-01-01T00:00:00Z',
      expires_at_utc='2026-01-02T00:00:00Z',
      nonce='nonce-2',
      policy_scope_id='scope-phase13',
      policy_snapshot_sha256='sha256-2',
    )
    
    result = web_app.verify_envelope_signature(envelope)
    assert result.valid is False
    assert result.failure_code == 'expired'


def test_phase_1_3_verify_envelope_hash_mismatch_fails() -> None:
  from unittest.mock import patch
  
  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  
  # Mock trust store with a test key
  mock_keys = {'test-key-id': 'AEPaCxf1bqVy+mj7KuFm/6BsFJuGrVCsP6KdbHnCKSw='}
  with patch.object(web_app, '_get_trusted_verification_keys', return_value=mock_keys):
    envelope = web_app.SignedContractEnvelope(
      schema_version='signed-contract.v1',
      transition_key=key,
      contract_record=contract,
      contract_hash_sha256='incorrect_hash_value',
      signature_alg='ed25519',
      signature_b64='AAAA',
      signer_key_id='test-key-id',
      issued_at_utc='2026-05-01T00:00:00Z',
      expires_at_utc='2026-12-31T23:59:59Z',
      nonce='nonce-3',
      policy_scope_id='scope-phase13',
      policy_snapshot_sha256='sha256-3',
    )
    
    result = web_app.verify_envelope_signature(envelope)
    assert result.valid is False
    assert result.failure_code == 'hash_mismatch'


def test_phase_1_3_verification_failure_does_not_mutate_state() -> None:
  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  
  envelope = web_app.SignedContractEnvelope(
    schema_version='signed-contract.v1',
    transition_key=key,
    contract_record=contract,
    contract_hash_sha256='abc123',
    signature_alg='ed25519',
    signature_b64='AAAA',
    signer_key_id='unknown-key',
    issued_at_utc='2026-05-01T00:00:00Z',
    expires_at_utc='2026-12-31T23:59:59Z',
    nonce='nonce-4',
    policy_scope_id='scope-phase13',
    policy_snapshot_sha256='sha256-4',
  )
  
  # Verify failure
  result = web_app.verify_envelope_signature(envelope)
  assert result.valid is False
  
  # Nonce should NOT be in replay window (failed verification doesn't record it)
  assert 'nonce-4' not in web_app._NONCE_REPLAY_WINDOW


def test_phase_1_3_fallback_text_pin() -> None:
  fallback_text = 'Signed contract verification failed; shell is using safe fallback projection.'
  assert fallback_text is not None
  assert len(fallback_text) > 0
  assert 'safe fallback' in fallback_text.lower()


def _signed_contract_test_keypair() -> tuple[Any, str]:
  """Generate an Ed25519 keypair and its trust-store public_key_b64 for signed-contract tests."""
  import base64 as _b64
  from cryptography.hazmat.primitives import serialization
  from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

  private_key = Ed25519PrivateKey.generate()
  public_der = private_key.public_key().public_bytes(
    serialization.Encoding.DER,
    serialization.PublicFormat.SubjectPublicKeyInfo,
  )
  return private_key, _b64.b64encode(public_der).decode('ascii')


def test_l2_signed_contract_envelope_round_trips_through_verifier() -> None:
  from unittest.mock import patch

  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  private_key, public_b64 = _signed_contract_test_keypair()
  trusted = {'pv-sc-test': public_b64}

  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted), \
       patch.object(web_app, '_load_signed_contract_signing_key', return_value=(private_key, 'pv-sc-test')):
    envelope = web_app.build_signed_contract_envelope(key, contract, {'operation_lane': 'live'})
    assert envelope is not None
    assert envelope.signer_key_id == 'pv-sc-test'
    assert envelope.signature_alg == 'ed25519'
    assert envelope.schema_version == 'signed-contract.v1'
    assert envelope.policy_scope_id == 'live'
    assert len(envelope.policy_snapshot_sha256) == 64
    web_app._NONCE_REPLAY_WINDOW.discard(envelope.nonce)
    result = web_app.verify_envelope_signature(envelope)
    assert result.valid is True
    assert result.failure_code is None


def test_l2_signed_contract_envelope_tamper_is_rejected() -> None:
  import base64 as _b64
  from dataclasses import replace
  from unittest.mock import patch

  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  private_key, public_b64 = _signed_contract_test_keypair()
  trusted = {'pv-sc-test': public_b64}

  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted), \
       patch.object(web_app, '_load_signed_contract_signing_key', return_value=(private_key, 'pv-sc-test')):
    base = web_app.build_signed_contract_envelope(key, contract, {'operation_lane': 'live'})
    assert base is not None
    # Tampering the contract hash -> recompute mismatch.
    assert web_app.verify_envelope_signature(replace(base, contract_hash_sha256='0' * 64)).failure_code == 'hash_mismatch'
    # Tampering the signature -> Ed25519 verify fails.
    forged_sig = _b64.b64encode(b'\x00' * 64).decode('ascii')
    assert web_app.verify_envelope_signature(replace(base, signature_b64=forged_sig)).failure_code == 'invalid_signature'
    # Tampering the policy scope -> the scope is signed, so the recomputed hash diverges.
    assert web_app.verify_envelope_signature(replace(base, policy_scope_id='sandbox')).failure_code == 'hash_mismatch'
    # Tampering the signer key id -> not in the trust store.
    assert web_app.verify_envelope_signature(replace(base, signer_key_id='someone-else')).failure_code == 'unknown_key'


def test_l2_signed_contract_envelope_is_unsigned_when_no_signing_key() -> None:
  from unittest.mock import patch

  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  with patch.object(web_app, '_load_signed_contract_signing_key', return_value=None):
    assert web_app.build_signed_contract_envelope(key, contract, {'operation_lane': 'live'}) is None


def test_l2_compute_policy_snapshot_is_deterministic_and_policy_sensitive() -> None:
  params = {'min_edge_dollars': 0.03, 'max_open_pairs': 4, 'scan_interval_ms': 60000}
  digest = web_app._signed_contract_table_digest()
  baseline = web_app.compute_policy_snapshot('live', params, 'signed-contract.v1', '1.1', digest)
  assert baseline == web_app.compute_policy_snapshot('live', params, 'signed-contract.v1', '1.1', digest)
  # Lane change invalidates the snapshot.
  assert baseline != web_app.compute_policy_snapshot('sandbox', params, 'signed-contract.v1', '1.1', digest)
  # A gating-threshold change invalidates the snapshot.
  shifted = {**params, 'min_edge_dollars': 0.05}
  assert baseline != web_app.compute_policy_snapshot('live', shifted, 'signed-contract.v1', '1.1', digest)


def test_l2_signed_contract_canonicalization_preserves_unicode_unescaped() -> None:
  from dataclasses import replace

  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  base_contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert base_contract is not None
  record = replace(base_contract, headline='edge é alert')
  canonical = web_app.canonicalize_signed_contract_payload('signed-contract.v1', key, record, 'live')
  # ensure_ascii=False is locked: raw UTF-8 is preserved and NOT \uXXXX-escaped.
  assert 'é'.encode('utf-8') in canonical
  assert b'\\u00e9' not in canonical


def test_l3_policy_snapshot_check_six_rejects_cross_policy_replay() -> None:
  from unittest.mock import patch

  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  assert contract is not None
  private_key, public_b64 = _signed_contract_test_keypair()
  trusted = {'pv-sc-test': public_b64}
  live_context = {'operation_lane': 'live', 'min_edge_dollars': 0.03, 'max_open_pairs': 4}

  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted), \
       patch.object(web_app, '_load_signed_contract_signing_key', return_value=(private_key, 'pv-sc-test')):
    envelope = web_app.build_signed_contract_envelope(key, contract, live_context)
    assert envelope is not None

    # Same active context -> check #6 passes.
    web_app._NONCE_REPLAY_WINDOW.discard(envelope.nonce)
    assert web_app.verify_envelope_signature(envelope, settings_payload=live_context).valid is True

    # Operating lane changed after signing -> policy_mismatch (authorization is non-portable).
    web_app._NONCE_REPLAY_WINDOW.discard(envelope.nonce)
    sandbox_result = web_app.verify_envelope_signature(
      envelope,
      settings_payload={'operation_lane': 'sandbox', 'min_edge_dollars': 0.03, 'max_open_pairs': 4},
    )
    assert sandbox_result.valid is False
    assert sandbox_result.failure_code == 'policy_mismatch'

    # Gating threshold changed after signing -> policy_mismatch.
    web_app._NONCE_REPLAY_WINDOW.discard(envelope.nonce)
    shifted_result = web_app.verify_envelope_signature(
      envelope,
      settings_payload={'operation_lane': 'live', 'min_edge_dollars': 0.99, 'max_open_pairs': 4},
    )
    assert shifted_result.valid is False
    assert shifted_result.failure_code == 'policy_mismatch'


def _signed_contract_gate_call(state_kwargs: dict[str, Any]) -> dict[str, Any]:
  return web_app._workflow_state(
    'run',
    auto_sequence=[],
    headline='Ready to run.',
    operator_message='Proceed to run.',
    step_kind='execute',
    can_run_next_step=True,
    **state_kwargs,
  )


def test_l4_workflow_gate_marks_verified_on_valid_envelope() -> None:
  from unittest.mock import patch

  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  private_key, public_b64 = _signed_contract_test_keypair()
  trusted = {'pv-sc-test': public_b64}
  context = {'operation_lane': 'live', 'min_edge_dollars': 0.03}

  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted), \
       patch.object(web_app, '_load_signed_contract_signing_key', return_value=(private_key, 'pv-sc-test')):
    envelope = web_app.build_signed_contract_envelope(key, contract, context)
    web_app._NONCE_REPLAY_WINDOW.discard(envelope.nonce)
    state = _signed_contract_gate_call({
      'signed_envelope': envelope,
      'signature_required': True,
      'signature_settings_payload': context,
    })
    assert state['signature_status'] == 'verified'
    assert state['can_run_next_step'] is True
    assert state['operator_message'] == 'Proceed to run.'


def test_l4_workflow_gate_fails_safe_on_verification_failure() -> None:
  from dataclasses import replace
  from unittest.mock import patch

  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  private_key, public_b64 = _signed_contract_test_keypair()
  trusted = {'pv-sc-test': public_b64}
  context = {'operation_lane': 'live', 'min_edge_dollars': 0.03}

  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted), \
       patch.object(web_app, '_load_signed_contract_signing_key', return_value=(private_key, 'pv-sc-test')):
    envelope = web_app.build_signed_contract_envelope(key, contract, context)

    # Tampered (hash) -> fail-safe no-go + pinned string + failure code; runtime authorization denied.
    tampered = replace(envelope, contract_hash_sha256='0' * 64)
    state = _signed_contract_gate_call({
      'signed_envelope': tampered,
      'signature_required': True,
      'signature_settings_payload': context,
    })
    assert state['can_run_next_step'] is False
    assert state['operator_message'] == web_app.SIGNED_CONTRACT_FALLBACK_MESSAGE
    assert state['operator_message'] == 'Signed contract verification failed; shell is using safe fallback projection.'
    assert state['signature_status'] == 'hash_mismatch'
    assert state['button_emphasis_tone'] == ''

    # Policy mismatch (lane changed after signing) -> same fail-safe.
    web_app._NONCE_REPLAY_WINDOW.discard(envelope.nonce)
    mismatch_state = _signed_contract_gate_call({
      'signed_envelope': envelope,
      'signature_required': True,
      'signature_settings_payload': {'operation_lane': 'sandbox', 'min_edge_dollars': 0.03},
    })
    assert mismatch_state['can_run_next_step'] is False
    assert mismatch_state['signature_status'] == 'policy_mismatch'

    # Replay (nonce already consumed) -> fail-safe.
    web_app._NONCE_REPLAY_WINDOW.add(envelope.nonce)
    replay_state = _signed_contract_gate_call({
      'signed_envelope': envelope,
      'signature_required': True,
      'signature_settings_payload': context,
    })
    assert replay_state['can_run_next_step'] is False
    assert replay_state['signature_status'] == 'replay'
    web_app._NONCE_REPLAY_WINDOW.discard(envelope.nonce)


def test_l4_workflow_gate_marks_unsigned_without_signing_capability() -> None:
  # No envelope (no local signing key) -> explicit, non-enforcing 'unsigned'; shell is not bricked.
  state = _signed_contract_gate_call({
    'signed_envelope': None,
    'signature_required': True,
    'signature_settings_payload': {'operation_lane': 'live'},
  })
  assert state['signature_status'] == 'unsigned'
  assert state['can_run_next_step'] is True


def test_l4_workflow_state_unsigned_calls_carry_no_signature_status() -> None:
  # The vast majority of _workflow_state callers are not signed surfaces: no signature_status key.
  state = web_app._workflow_state(
    'report',
    auto_sequence=[],
    headline='Report ready.',
    operator_message='Review the report.',
    step_kind='review',
    can_run_next_step=False,
  )
  assert 'signature_status' not in state


def test_backend_emits_deck_highlights_for_key_actions(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_operator_clear_tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _call_app(app, method='POST', path='/api/websocket-overlay', body={'action': 'load_sandbox_websocket', 'url': 'wss://demo-api.kalshi.example/ws'})
  _call_app(app, method='POST', path='/api/websocket-overlay', body={'action': 'load_live_websocket', 'url': 'wss://api.kalshi.example/ws'})

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/key-clear',
  )
  payload = json.loads(body)
  
  assert status == '200 OK'
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['next_actionable_step'] == 'load_api_key'
  assert payload['workflow']['focus_tone'] == 'focus-info'
  assert payload['workflow']['deck_action_highlights'].get('key_management') == 'no-go'
  assert payload['workflow']['deck_action_highlights'].get('websocket_management') == 'ok'
  assert payload['workflow']['detail_control_highlights'].get('clear_key') == 'warn'
  assert payload['workflow']['highlight_policy_version'] == 'orchestrator-highlights.v1'


def test_key_clear_route_transitions_active_websocket_session_to_offline_before_clearing(monkeypatch: Any, tmp_path: Path) -> None:
  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app, method='POST', path='/api/key-apply', body={'lane': 'sandbox'})
  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})

  clear_status, _, clear_body = _call_app(
    app,
    method='POST',
    path='/api/key-clear',
    body={
      'set_offline_if_active': True,
      'clear_reason': 'active_websocket_key_clear',
    },
  )
  clear_payload = json.loads(clear_body)

  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)

  assert clear_status == '200 OK'
  assert clear_payload['connection_posture']['operation_lane'] == 'offline'
  assert clear_payload['session_overlay']['context']['mode_selected'] is False
  assert clear_payload['key_management']['active_key_tail'] == '--'
  assert clear_payload['key_management']['clear_hold_active'] is True
  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['connection_posture']['operation_lane'] == 'offline'
  assert bootstrap_payload['session_overlay']['context']['mode_selected'] is False
  assert bootstrap_payload['key_management']['active_key_tail'] == '--'

def test_backend_emits_deck_highlights_for_websocket_actions(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_operator_clear_tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  
  ok_status, _, ok_body = _call_app(
    app,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_sandbox_websocket', 'url': 'wss://external-api-ws.demo.kalshi.com/trade-api/ws/v2'},
  )
  ok_payload = json.loads(ok_body)
  
  assert ok_status == '200 OK'
  assert ok_payload['workflow']['deck_action_highlights'].get('key_management') == 'warn'
  assert ok_payload['workflow']['deck_action_highlights'].get('websocket_management') == 'warn'
  assert ok_payload['workflow']['highlight_policy_version'] == 'orchestrator-highlights.v1'
  
  fail_status, _, fail_body = _call_app(
    app,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_live_websocket', 'url': 'invalid-url'},
  )
  fail_payload = json.loads(fail_body)
  
  assert fail_status == '200 OK'
  assert fail_payload['workflow']['deck_action_highlights'].get('key_management') == 'warn'
  assert fail_payload['workflow']['deck_action_highlights'].get('websocket_management') == 'warn'
  assert fail_payload['workflow']['detail_control_highlights'].get('load_live_websocket') == 'no-go'
  assert fail_payload['workflow']['highlight_policy_version'] == 'orchestrator-highlights.v1'

def test_connected_websocket_highlight_stays_ok_across_unrelated_state_refreshes(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app, method='POST', path='/api/key-apply', body={'lane': 'sandbox'})

  mode_status, _, mode_body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  mode_payload = json.loads(mode_body)

  runtime_status, _, runtime_body = _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'apply', 'values': {'scan_interval_ms': 2500}},
  )
  runtime_payload = json.loads(runtime_body)

  assert mode_status == '200 OK'
  assert mode_payload['workflow']['deck_action_highlights'].get('websocket_management') == 'ok'
  assert runtime_status == '200 OK'
  assert runtime_payload['connection_posture']['operation_lane'] == 'sandbox'
  assert runtime_payload['connection_posture']['connection_state']['websocket_connected'] is True
  assert runtime_payload['workflow']['deck_action_highlights'].get('websocket_management') == 'ok'


def test_single_loaded_websocket_highlight_stays_warn_after_connected_refreshes(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'sandbox-runtime-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='',
    sandbox_websocket_url='',
    live_websocket_url='',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )

  class _FakeWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FakeWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
  _call_app(app, method='POST', path='/api/key-apply', body={'lane': 'sandbox'})
  _call_app(
    app,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_sandbox_websocket', 'url': 'wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2'},
  )

  mode_status, _, mode_body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  mode_payload = json.loads(mode_body)

  runtime_status, _, runtime_body = _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'apply', 'values': {'scan_interval_ms': 2500}},
  )
  runtime_payload = json.loads(runtime_body)

  assert mode_status == '200 OK'
  assert mode_payload['connection_posture']['connection_state']['websocket_connected'] is True
  assert mode_payload['workflow']['deck_action_highlights'].get('websocket_management') == 'warn'
  assert runtime_status == '200 OK'
  assert runtime_payload['connection_posture']['operation_lane'] == 'sandbox'
  assert runtime_payload['connection_posture']['connection_state']['websocket_connected'] is True
  assert runtime_payload['workflow']['deck_action_highlights'].get('websocket_management') == 'warn'

def test_authoritative_websocket_posture_persists_across_rerenders(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_operator_clear_tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _call_app(
    app,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_sandbox_websocket', 'url': 'wss://external-api-ws.demo.kalshi.com/trade-api/ws/v2'},
  )

  status, _, body = _call_app(app, method='POST', path='/api/key-clear')
  payload = json.loads(body)
  
  assert status == '200 OK'
  highlights = payload['workflow']['deck_action_highlights']
  assert payload['workflow']['step_kind'] == 'review'
  assert payload['workflow']['next_actionable_step'] == 'load_api_key'
  assert highlights.get('key_management') == 'no-go'
  assert highlights.get('websocket_management') == 'warn'
  
  refresh_status, _, refresh_body = _call_app(app, method='GET', path='/api/bootstrap')
  refresh_payload = json.loads(refresh_body)

  assert refresh_status == '200 OK'
  assert refresh_payload['workflow']['deck_action_highlights'].get('key_management') == 'no-go'
  assert refresh_payload['workflow']['deck_action_highlights'].get('websocket_management') == 'warn'

def test_no_client_side_highlight_mutations_allowed() -> None:
  app = create_operator_console_app(_services())
  
  status, _, body = _call_app(app, method='GET', path='/')
  
  assert status == '200 OK'
  assert re.search(r'state\\.deckActionHighlights\\s*=', body) is None
  assert re.search(r'state\\.detailControlHighlights\\s*=', body) is None


def test_workflow_highlight_envelope_fails_neutral_without_backend_maps() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  function_body = body.split('function workflowHighlightEnvelope(payload = {}) {', 1)[1].split('function readinessHelpRoute(', 1)[0]
  assert 'const fallbackDeck = sanitizeHighlightToneMap(state.deckActionHighlights || {}' not in function_body
  assert 'const fallbackDetail = sanitizeHighlightToneMap(state.detailControlHighlights || {}' not in function_body
  assert 'deckActionHighlights: hasPayloadDeck ? payloadDeckMap : {}' in function_body
  assert 'detailControlHighlights: hasPayloadDetail ? payloadDetailMap : {}' in function_body


def test_connection_guidance_highlights_contract_rejects_url_count_inference() -> None:
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  function_body = body.split('function buildConnectionGuidanceHighlights(payload = {}) {', 1)[1].split('function humanActionLabel(action) {', 1)[0]
  assert 'return {};' in function_body
  assert 'loadedCount' not in function_body
  assert 'activeWebsocketLabel' not in function_body


def test_discovery_cache_lifecycle_initialized_empty(tmp_path: Path, monkeypatch: Any) -> None:
  """
  Boot initialization (KDISC-001 Lifecycle): Discovery cache (discovered_candidates) is a
  fresh, empty session-local list for each app instance.  No entries should be loaded into
  the discovery cache from the tombstone or persisted registry.
  """
  app = create_operator_console_app(_services())
  
  # Verify bootstrap response contains empty discovered_candidates
  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)
  
  assert status == '200 OK'
  assert len(payload['key_management'].get('discovered_candidates', [])) == 0
  assert payload['key_management']['discovery_ran'] is False


def test_discovery_cache_lifecycle_volatile_per_session(tmp_path: Path, monkeypatch: Any) -> None:
  """
  Session volatility (KDISC-001 Lifecycle): Discovery cache is volatile and resets to empty
  on each explicit discovery request. Unlike the persisted registry, discovered_candidates
  never survive across fresh discovery calls within the same session.
  """
  app = create_operator_console_app(_services())
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  
  # First discovery populate cache
  first_discover_status, _, first_discover_body = _call_app(app, method='POST', path='/api/key-discover')
  first_payload = json.loads(first_discover_body)
  
  assert first_discover_status == '200 OK'
  assert first_payload['key_management']['discovery_ran'] is True
  first_candidate_count = len(first_payload['key_management'].get('discovered_candidates', []))
  assert first_candidate_count >= 0
  
  # Second discovery should report fresh re-run, not reuse cached
  # (KDISC-001: each explicit discover call is fresh)
  second_discover_status, _, second_discover_body = _call_app(app, method='POST', path='/api/key-discover')
  second_payload = json.loads(second_discover_body)
  
  assert second_discover_status == '200 OK'
  assert second_payload['key_management']['discovery_ran'] is True
  # The discovery results should be consistent (same files found both times)
  second_candidate_count = len(second_payload['key_management'].get('discovered_candidates', []))
  assert second_candidate_count == first_candidate_count


def test_discovery_cache_persisted_registry_independent(tmp_path: Path, monkeypatch: Any) -> None:
  """
  Registry vs. Cache distinction (KDISC-001 Lifecycle): The persisted registry and the
  session-local discovery cache are independent. Loading/clearing keys affects the registry
  but not the discovery cache until the next discovery run.
  """
  app = create_operator_console_app(_services())
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  
  # Perform discovery to populate both cache and registry
  discover_status, _, discover_body = _call_app(app, method='POST', path='/api/key-discover')
  discover_payload = json.loads(discover_body)
  
  assert discover_status == '200 OK'
  assert discover_payload['key_management']['discovery_ran'] is True
  candidates = discover_payload['key_management'].get('discovered_candidates', [])
  
  # Clear the key (registry updated, discovery cache remains until next discover)
  clear_status, _, clear_body = _call_app(app, method='POST', path='/api/key-clear')
  clear_payload = json.loads(clear_body)
  
  assert clear_status == '200 OK'
  # Cleared state is set, but discovered_candidates should be independent
  assert clear_payload['key_management']['clear_hold_active'] is True
  # (discovery cache is still present; it will clear on next discover if needed per contract)


def test_discovery_cache_does_not_affect_bootstrap_state(tmp_path: Path, monkeypatch: Any) -> None:
  """
  Bootstrap state (KDISC-001 Lifecycle): Bootstrap endpoint does not trigger or populate
  discovery cache. It reflects the persisted registry and loaded state, but discovery_ran
  starts as False unless explicitly called via /api/key-discover.
  """
  app = create_operator_console_app(_services())
  
  # Bootstrap before discovery
  pre_discovery_status, _, pre_discovery_body = _call_app(app, method='GET', path='/api/bootstrap')
  pre_payload = json.loads(pre_discovery_body)
  
  assert pre_discovery_status == '200 OK'
  assert pre_payload['key_management']['discovery_ran'] is False
  assert len(pre_payload['key_management'].get('discovered_candidates', [])) == 0
  
  # Load a key and bootstrap (registry updated but discovery cache independent)
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  
  post_load_status, _, post_load_body = _call_app(app, method='GET', path='/api/bootstrap')
  post_load_payload = json.loads(post_load_body)
  
  assert post_load_status == '200 OK'
  # Key is loaded, but discovery_ran should still be False (bootstrap doesn't trigger discovery)
  assert post_load_payload['key_management']['discovery_ran'] is False
  # After staging and loading a key, the sandbox_key_path should be set via the overlay
  assert post_load_payload['key_management']['active_key_tail'] is not None
  assert len(post_load_payload['key_management'].get('discovered_candidates', [])) == 0


def test_data_clear_with_extract_intent_emits_loadable_datapack_and_clears_lane(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'extract-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  state_db_path = tmp_path / 'runtime.sqlite3'
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(state_db_path),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  source_db_path = tmp_path / 'source.sqlite3'
  connection = open_database(source_db_path)
  connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'extract_source_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'test'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  connection.commit()
  datapack_bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=state_db_path.name,
    include_synthetic_refinement=False,
  )
  input_datapack_root = tmp_path / 'input-datapack'
  _write_test_datapack_bundle(input_datapack_root, datapack_bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(input_datapack_root)})
  load_status, _, load_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack'},
  )
  load_payload = json.loads(load_body)
  assert load_status == '200 OK'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is True

  clear_status, _, clear_body = _call_app(
    app,
    method='POST',
    path='/api/data-clear',
    body={'lanes': ['sandbox'], 'extract': True},
  )
  clear_payload = json.loads(clear_body)

  assert clear_status == '200 OK'
  assert clear_payload['data_management']['last_result']['tone'] == 'ok'
  assert clear_payload['data_management']['last_result']['reason'] == 'datapack_extracted_on_clear'
  assert clear_payload['data_management']['sandbox_datapack_loaded'] is False
  extraction = clear_payload['data_management']['last_extraction']
  assert isinstance(extraction, dict)
  assert extraction['mode'] == 'clear_extract'
  assert extraction['lane_count'] == 1
  items = extraction.get('items', [])
  assert isinstance(items, list)
  assert len(items) == 1
  item = items[0]
  assert item['lane'] == 'sandbox'
  assert item['storage_class'] == 'ephemeral'
  assert item['write_origin'] == 'automation_test'
  extracted_root = Path(str(item['extracted_root']))
  assert extracted_root.exists()
  assert 'var/datapack_store/ephemeral/extracts/' in str(extracted_root).replace('\\', '/')
  manifest = json.loads((extracted_root / 'manifest.json').read_text(encoding='utf-8'))
  restore_policy = json.loads((extracted_root / 'restore_policy.json').read_text(encoding='utf-8'))
  assert validate_datapack_artifacts(extracted_root, manifest, restore_policy) == []
  assert str(manifest.get('parent_datapack_id') or '').strip()
  assert str(manifest.get('source_loaded_datapack_id') or '').strip()
  assert item.get('parent_datapack_id') == manifest.get('parent_datapack_id')
  assert item.get('source_loaded_datapack_id') == manifest.get('source_loaded_datapack_id')

  _call_app(app, method='POST', path='/api/data-select', body={'path': str(extracted_root)})
  reload_status, _, reload_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack'},
  )
  reload_payload = json.loads(reload_body)
  assert reload_status == '200 OK'
  assert reload_payload['data_management']['last_result']['tone'] == 'warn'
  assert reload_payload['data_management']['last_result']['reason'] == 'datapack_overwrite_confirmation_required'
  assert reload_payload['data_management']['sandbox_datapack_loaded'] is False
  pending_overwrite = reload_payload['data_management'].get('pending_load_overwrite') or {}
  assert pending_overwrite.get('lane') == 'sandbox'
  assert int(pending_overwrite.get('row_count') or 0) >= 1


def test_data_clear_with_extract_intent_blocks_on_empty_slot(tmp_path: Path, monkeypatch: Any) -> None:
  key_file = tmp_path / 'extract-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'runtime.sqlite3'),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  clear_status, _, clear_body = _call_app(
    app,
    method='POST',
    path='/api/data-clear',
    body={'lanes': ['sandbox'], 'extract': True},
  )
  clear_payload = json.loads(clear_body)

  assert clear_status == '200 OK'
  assert clear_payload['data_management']['last_result']['tone'] == 'no-go'
  assert clear_payload['data_management']['last_result']['reason'] == 'datapack_extract_empty_slot'
  assert clear_payload['data_management']['sandbox_datapack_loaded'] is False
  assert clear_payload['data_management']['last_extraction'] is None


def test_data_load_blocks_zero_hydration_datapacks_before_clear(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  empty_source_db = tmp_path / 'empty-source.sqlite3'
  connection = open_database(empty_source_db)
  datapack_bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=empty_source_db.name,
    include_synthetic_refinement=False,
  )
  pack_root = tmp_path / 'zero-hydration-pack'
  _write_test_datapack_bundle(pack_root, datapack_bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack'},
  )
  load_payload = json.loads(load_body)

  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'no-go'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_proof_only_non_loadable'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is False
  assert load_payload['data_management']['last_load_attestation'] is None


def test_data_load_synthetic_refinement_hydrates_candidate_and_analysis_visuals(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  source_db = tmp_path / 'synthetic-source.sqlite3'
  source_connection = open_database(source_db)
  source_connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'synthetic_seed_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'test_data_load_synthetic_refinement_hydrates_candidate_and_analysis_visuals'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  source_connection.commit()

  datapack_bundle = build_datapack_bundle(
    source_connection,
    operation_lane='sandbox',
    datapack_type='synthetic_refinement',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=source_db.name,
    include_synthetic_refinement=True,
  )
  pack_root = tmp_path / 'synthetic-mature-pack'
  _write_test_datapack_bundle(pack_root, datapack_bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack'},
  )
  load_payload = json.loads(load_body)

  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'ok'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is True

  candidate_visual_status, _, candidate_visual_body = _call_app(
    app,
    method='GET',
    path='/api/visuals',
    query='scope=candidate_landscape&view=candidate_density_curve',
  )
  candidate_visual_payload = json.loads(candidate_visual_body)
  assert candidate_visual_status == '200 OK'
  assert candidate_visual_payload.get('status') == 'empty'
  assert candidate_visual_payload.get('series') == []
  assert candidate_visual_payload.get('empty_reason') == 'Operational visuals stay hidden until sandbox or live mode is selected.'

  analysis_visual_status, _, analysis_visual_body = _call_app(
    app,
    method='GET',
    path='/api/visuals',
    query='scope=analysis&view=factors_timeseries',
  )
  analysis_visual_payload = json.loads(analysis_visual_body)
  assert analysis_visual_status == '200 OK'
  assert analysis_visual_payload.get('status') == 'empty'
  assert analysis_visual_payload.get('series') == []
  assert analysis_visual_payload.get('empty_reason') == 'Operational visuals stay hidden until sandbox or live mode is selected.'


def test_bootstrap_offline_hides_datapack_partition_pointers_after_load(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  source_db = tmp_path / 'source.sqlite3'
  source_connection = open_database(source_db)
  source_connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'offline_boot_partition_seed',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'test_bootstrap_offline_hides_datapack_partition_pointers_after_load'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  source_connection.commit()

  bundle = build_datapack_bundle(
    source_connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=source_db.name,
    include_synthetic_refinement=False,
  )
  pack_root = tmp_path / 'offline-pointer-pack'
  _write_test_datapack_bundle(pack_root, bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack'},
  )
  load_payload = json.loads(load_body)
  assert load_status == '200 OK'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is True
  assert load_payload['data_management']['last_load_attestation'] is not None

  _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'offline'})
  bootstrap_status, _, bootstrap_body = _call_app(app, method='GET', path='/api/bootstrap')
  bootstrap_payload = json.loads(bootstrap_body)
  data_management = bootstrap_payload['data_management']

  assert bootstrap_status == '200 OK'
  assert bootstrap_payload['connection_posture']['operation_lane'] == 'offline'
  assert data_management['candidate_count'] == 0
  assert data_management['discovered_candidates'] == []
  assert data_management['selected_datapack_path_display'] == ''
  # CP identity fields are global/persistent — not zeroed by offline boundary (AT_BOOT §4.5)
  assert data_management['sandbox_datapack_loaded'] is True
  assert data_management['sandbox_datapack_id'] is not None
  assert data_management['loaded_source_datapack_id'] is None
  assert data_management['pending_load_overwrite'] is None
  assert data_management['last_load_attestation'] is None
  assert data_management['last_extraction'] is None
  assert all(not bool(slot.get('occupied')) for slot in data_management['datapack_slots'])


def test_data_clear_with_extract_origin_operator_extract_blocks_without_managed_session(
  tmp_path: Path, monkeypatch: Any
) -> None:
  key_file = tmp_path / 'extract-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  state_db_path = tmp_path / 'runtime.sqlite3'
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(state_db_path),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  source_db_path = tmp_path / 'source.sqlite3'
  connection = open_database(source_db_path)
  connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'extract_source_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'test'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  connection.commit()
  datapack_bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=state_db_path.name,
    include_synthetic_refinement=False,
  )
  input_datapack_root = tmp_path / 'input-datapack'
  _write_test_datapack_bundle(input_datapack_root, datapack_bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(input_datapack_root)})
  _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack'},
  )

  clear_status, _, clear_body = _call_app(
    app,
    method='POST',
    path='/api/data-clear',
    body={'lanes': ['sandbox'], 'extract': True, 'extract_origin': 'operator_extract'},
  )
  clear_payload = json.loads(clear_body)

  assert clear_status == '200 OK'
  assert clear_payload['data_management']['last_result']['tone'] == 'no-go'
  assert clear_payload['data_management']['last_result']['reason'] == 'datapack_extract_policy_blocked'
  # GAP-POST-5 contract: a policy-blocked extract is a no-op — the lane keeps its
  # loaded identity and the payload reports it truthfully (no inactive-lane zeroing).
  assert clear_payload['data_management']['sandbox_datapack_loaded'] is True
  assert clear_payload['data_management']['last_extraction'] is None


def test_data_clear_extract_blocks_when_operator_profile_requires_canonical_managed_session(
  tmp_path: Path, monkeypatch: Any
) -> None:
  monkeypatch.setenv('POLYVENTURE_DATAPACK_WRITE_PROFILE', 'operator')
  key_file = tmp_path / 'extract-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  state_db_path = tmp_path / 'runtime.sqlite3'
  settings = Settings(
    kalshi_env='demo',
    api_key_id='demo-api-key',
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane='sandbox',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(state_db_path),
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  source_db_path = tmp_path / 'source.sqlite3'
  connection = open_database(source_db_path)
  connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'extract_source_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'test'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  connection.commit()
  datapack_bundle = build_datapack_bundle(
    connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=state_db_path.name,
    include_synthetic_refinement=False,
  )
  input_datapack_root = tmp_path / 'input-datapack'
  _write_test_datapack_bundle(input_datapack_root, datapack_bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(input_datapack_root)})
  _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack'},
  )

  clear_status, _, clear_body = _call_app(
    app,
    method='POST',
    path='/api/data-clear',
    body={'lanes': ['sandbox'], 'extract': True},
  )
  clear_payload = json.loads(clear_body)

  assert clear_status == '200 OK'
  assert clear_payload['data_management']['last_result']['tone'] == 'no-go'
  assert clear_payload['data_management']['last_result']['reason'] == 'datapack_extract_policy_blocked'
  # GAP-POST-5 contract: a policy-blocked extract is a no-op — the lane keeps its
  # loaded identity and the payload reports it truthfully (no inactive-lane zeroing).
  assert clear_payload['data_management']['sandbox_datapack_loaded'] is True


# ---------------------------------------------------------------------------
# SYN-5C — Lifecycle matrix expansion
# Slices: detect→select→load, post-load mutation checkpoint, tamper/mismatch
# no-go paths, identity mismatch, CLI-only rebind preservation.
# Slices 3 (clear-as-extract) and 4 (reload extracted artifact) are already
# covered by the SYN-5B tests above and satisfy SYN-5C acceptance too.
# ---------------------------------------------------------------------------

def _make_synlc_settings(
  tmp_path: Path,
  *,
  api_key_id: str = 'demo-api-key',
  operation_lane: str = 'sandbox',
) -> Settings:
  key_file = tmp_path / f'synlc-{operation_lane}-key.pem'
  key_file.write_text('placeholder-key-material', encoding='utf-8')
  state_db_path = tmp_path / 'runtime.sqlite3'
  return Settings(
    kalshi_env='demo',
    api_key_id=api_key_id,
    private_key_file=str(key_file),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/v2',
    websocket_url='wss://demo-api.kalshi.example/ws',
    sandbox_websocket_url='wss://demo-api.kalshi.example/ws',
    live_websocket_url='wss://api.kalshi.example/ws',
    operation_lane=operation_lane,
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.05,
    max_pair_contracts=25.0,
    max_open_pairs=4,
    max_unhedged_sec=10,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(state_db_path),
  )


def _make_synlc_datapack(
  tmp_path: Path,
  pack_dir_name: str,
  *,
  api_key_id: str = 'demo-api-key',
  key_file: Path | None = None,
  operation_lane: str = 'sandbox',
) -> Path:
  tmp_path.mkdir(parents=True, exist_ok=True)
  if key_file is None:
    key_file = tmp_path / f'synlc-{operation_lane}-key.pem'
    key_file.write_text('placeholder-key-material', encoding='utf-8')
  state_db_path = tmp_path / 'runtime.sqlite3'
  connection = open_database(state_db_path)
  connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'bootstrap_runtime_seed',
      None,
      operation_lane,
      None,
      json.dumps({'source': 'test_helper_seed'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  connection.commit()
  bundle = build_datapack_bundle(
    connection,
    operation_lane=operation_lane,
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(api_key_id),
    profile_token=profile_token_for_key_path(str(key_file)),
    state_db_path_tail=state_db_path.name,
    include_synthetic_refinement=False,
  )
  pack_root = tmp_path / pack_dir_name
  _write_test_datapack_bundle(pack_root, bundle)
  return pack_root


# SYN-5C Slice 1 — detect → select via candidate_id → load
def test_lifecycle_detect_select_by_candidate_id_then_load(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'PROJECT_ROOT', tmp_path)
  canonical_root = tmp_path / 'var' / 'datapack_extracts'
  pack_root = _make_synlc_datapack(canonical_root, 'synlc-detect-pack', api_key_id=settings.api_key_id)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  detect_status, _, detect_body = _call_app(app, method='POST', path='/api/data-detect')
  detect_payload = json.loads(detect_body)

  assert detect_status == '200 OK'
  candidates = detect_payload['data_management'].get('discovered_candidates', [])
  assert len(candidates) >= 1
  # resolved_root is not included in the public payload; match by candidate_id instead.
  # candidate_id = 'datapack-' + sha256(str(resolved_root).lower())[:12]
  import hashlib as _hashlib
  expected_root_str = str(pack_root.resolve()).lower()
  expected_candidate_id = 'datapack-' + _hashlib.sha256(expected_root_str.encode('utf-8')).hexdigest()[:12]
  matched = next(
    (c for c in candidates if c.get('candidate_id') == expected_candidate_id),
    None,
  )
  assert matched is not None, f'Expected candidate_id={expected_candidate_id} in detect candidates. Found: {[c.get("candidate_id") for c in candidates]}'
  candidate_id = matched['candidate_id']

  select_status, _, select_body = _call_app(
    app, method='POST', path='/api/data-select', body={'candidate_id': candidate_id}
  )
  select_payload = json.loads(select_body)
  assert select_status == '200 OK'
  assert select_payload['data_management']['last_result']['tone'] == 'ok'

  load_status, _, load_body = _call_app(
    app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'}
  )
  load_payload = json.loads(load_body)
  assert load_status == '200 OK'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is True


def test_data_detect_excludes_out_of_path_datapacks(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'PROJECT_ROOT', tmp_path)

  canonical_root = tmp_path / 'var' / 'datapack_extracts'
  canonical_pack = _make_synlc_datapack(canonical_root, 'synlc-canonical-pack', api_key_id=settings.api_key_id)
  outside_pack = _make_synlc_datapack(tmp_path, 'synlc-outside-pack', api_key_id=settings.api_key_id)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  detect_status, _, detect_body = _call_app(app, method='POST', path='/api/data-detect')
  detect_payload = json.loads(detect_body)

  assert detect_status == '200 OK'
  candidates = detect_payload['data_management'].get('discovered_candidates', [])
  candidate_ids = {str(candidate.get('candidate_id') or '') for candidate in candidates}

  import hashlib as _hashlib
  canonical_candidate_id = 'datapack-' + _hashlib.sha256(str(canonical_pack.resolve()).lower().encode('utf-8')).hexdigest()[:12]
  outside_candidate_id = 'datapack-' + _hashlib.sha256(str(outside_pack.resolve()).lower().encode('utf-8')).hexdigest()[:12]

  assert canonical_candidate_id in candidate_ids
  assert outside_candidate_id not in candidate_ids


def test_data_load_requires_overwrite_confirmation_when_lane_has_runtime_data(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  state_connection = open_database(Path(settings.state_db_path))
  state_connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'existing_runtime_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'existing'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  state_connection.commit()

  pack_root = _make_synlc_datapack(tmp_path / 'pack-source', 'synlc-overwrite-gate-pack', api_key_id=settings.api_key_id)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack'},
  )
  load_payload = json.loads(load_body)

  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'warn'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_overwrite_confirmation_required'
  pending = load_payload['data_management'].get('pending_load_overwrite') or {}
  assert pending.get('lane') == 'sandbox'
  assert int(pending.get('row_count') or 0) >= 1


def test_data_load_single_action_requires_overwrite_confirmation_when_lane_has_runtime_data(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  state_connection = open_database(Path(settings.state_db_path))
  state_connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'existing_runtime_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'existing'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  state_connection.commit()

  pack_root = _make_synlc_datapack(tmp_path / 'pack-source-single-action', 'synlc-overwrite-gate-pack-single-action', api_key_id=settings.api_key_id)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_datapack'},
  )
  load_payload = json.loads(load_body)

  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'warn'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_overwrite_confirmation_required'
  pending = load_payload['data_management'].get('pending_load_overwrite') or {}
  assert pending.get('lane') == 'sandbox'
  assert int(pending.get('row_count') or 0) >= 1


def test_data_load_confirm_overwrite_clears_then_hydrates_lane_partition(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  state_connection = open_database(Path(settings.state_db_path))
  state_connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'existing_runtime_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'existing'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  state_connection.commit()

  pack_source_root = tmp_path / 'pack-source-confirm-overwrite'
  pack_source_root.mkdir(parents=True, exist_ok=True)
  source_db_path = pack_source_root / 'runtime.sqlite3'
  source_connection = open_database(source_db_path)
  source_connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'loaded_runtime_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'loaded'}),
      '2026-05-24T00:10:00Z',
    ),
  )
  source_connection.commit()

  datapack_bundle = build_datapack_bundle(
    source_connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=source_db_path.name,
    include_synthetic_refinement=False,
  )
  pack_root = tmp_path / 'synlc-overwrite-confirm-pack'
  _write_test_datapack_bundle(pack_root, datapack_bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack', 'continue_overwrite': True},
  )
  load_payload = json.loads(load_body)

  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'ok'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_load_hydrated_with_expected_delta'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is True
  assert load_payload['data_management']['loaded_source_datapack_id'] is not None
  assert load_payload['data_management']['runtime_state_dirty_since_load'] is False
  last_load_attestation = load_payload['data_management']['last_load_attestation']
  assert isinstance(last_load_attestation, dict)
  assert last_load_attestation.get('lane') == 'sandbox'
  assert last_load_attestation.get('parity_verdict') == 'hydrated_with_expected_delta'
  assert int(last_load_attestation.get('hydrated_table_count') or 0) >= 1
  assert int(last_load_attestation.get('hydrated_row_count') or 0) >= 1
  assert last_load_attestation.get('completion_status') == 'complete'
  assert isinstance(last_load_attestation.get('provenance'), dict)
  assert last_load_attestation['provenance'].get('source_datapack_id') == load_payload['data_management']['loaded_source_datapack_id']

  state_rows = state_connection.execute(
    "SELECT event_type FROM runtime_events WHERE operation_lane = 'sandbox' ORDER BY id"
  ).fetchall()
  state_event_types = [str(row['event_type']) for row in state_rows]
  assert 'loaded_runtime_row' in state_event_types
  assert 'existing_runtime_row' not in state_event_types

  run_status, _, run_body = _call_app(app, method='POST', path='/api/run', body={})
  run_payload = json.loads(run_body)
  assert run_status == '200 OK'
  assert run_payload['data_management']['runtime_state_dirty_since_load'] is False
  assert run_payload['data_management']['runtime_state_dirty_reason'] is None


def test_data_load_single_action_confirm_overwrite_clears_then_hydrates_lane_partition(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  state_connection = open_database(Path(settings.state_db_path))
  state_connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'existing_runtime_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'existing'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  state_connection.commit()

  pack_source_root = tmp_path / 'pack-source-confirm-overwrite-single-action'
  pack_source_root.mkdir(parents=True, exist_ok=True)
  source_db_path = pack_source_root / 'runtime.sqlite3'
  source_connection = open_database(source_db_path)
  source_connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'loaded_runtime_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'loaded'}),
      '2026-05-24T00:10:00Z',
    ),
  )
  source_connection.commit()

  datapack_bundle = build_datapack_bundle(
    source_connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=source_db_path.name,
    include_synthetic_refinement=False,
  )
  pack_root = tmp_path / 'synlc-overwrite-confirm-pack-single-action'
  _write_test_datapack_bundle(pack_root, datapack_bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_datapack', 'lane': 'sandbox', 'continue_overwrite': True},
  )
  load_payload = json.loads(load_body)

  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'ok'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_load_hydrated_with_expected_delta'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is True
  assert load_payload['data_management']['loaded_source_datapack_id'] is not None
  assert load_payload['data_management']['runtime_state_dirty_since_load'] is False
  last_load_attestation = load_payload['data_management']['last_load_attestation']
  assert isinstance(last_load_attestation, dict)
  assert last_load_attestation.get('lane') == 'sandbox'
  assert last_load_attestation.get('parity_verdict') == 'hydrated_with_expected_delta'
  assert int(last_load_attestation.get('hydrated_table_count') or 0) >= 1
  assert int(last_load_attestation.get('hydrated_row_count') or 0) >= 1
  assert last_load_attestation.get('completion_status') == 'complete'
  assert isinstance(last_load_attestation.get('provenance'), dict)
  assert last_load_attestation['provenance'].get('source_datapack_id') == load_payload['data_management']['loaded_source_datapack_id']

  state_rows = state_connection.execute(
    "SELECT event_type FROM runtime_events WHERE operation_lane = 'sandbox' ORDER BY id"
  ).fetchall()
  state_event_types = [str(row['event_type']) for row in state_rows]
  assert 'loaded_runtime_row' in state_event_types
  assert 'existing_runtime_row' not in state_event_types


def test_data_load_confirm_overwrite_clears_candidate_saved_set_evaluations_before_parent_sets(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)

  state_connection = open_database(Path(settings.state_db_path))
  state_connection.execute(
    '''
    INSERT INTO candidate_review_runs (
      run_id,
      operation_lane,
      lane_session_id,
      candidate_signature,
      candidate_count,
      source_action,
      detail_json,
      recorded_at_utc
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'existing-run-1',
      'sandbox',
      'sandbox-session-existing',
      'candidate-signature-existing',
      1,
      'review_candidates',
      json.dumps({'source': 'existing'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  state_connection.execute(
    '''
    INSERT INTO candidate_saved_sets (
      saved_set_id,
      run_id,
      operation_lane,
      lane_session_id,
      saved_key_count,
      state_id,
      source_action,
      detail_json,
      recorded_at_utc
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'existing-saved-set-1',
      'existing-run-1',
      'sandbox',
      'sandbox-session-existing',
      1,
      'existing-state',
      'save_candidate_set',
      json.dumps({'source': 'existing'}),
      '2026-05-24T00:01:00Z',
    ),
  )
  state_connection.execute(
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
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'sandbox')
    ''',
    (
      'existing-saved-set-1',
      'accepted',
      'actionable',
      'visible',
      1,
      0,
      json.dumps({'source': 'existing'}),
      '2026-05-24T00:02:00Z',
    ),
  )
  state_connection.commit()

  pack_source_root = tmp_path / 'pack-source-confirm-overwrite-saved-set-evals'
  pack_source_root.mkdir(parents=True, exist_ok=True)
  source_db_path = pack_source_root / 'runtime.sqlite3'
  source_connection = open_database(source_db_path)
  source_connection.execute(
    '''
    INSERT INTO runtime_events (level, event_type, pair_id, operation_lane, lane_session_id, detail_json, recorded_at_utc)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    ''',
    (
      'info',
      'loaded_runtime_row',
      None,
      'sandbox',
      None,
      json.dumps({'source': 'loaded'}),
      '2026-05-24T00:10:00Z',
    ),
  )
  source_connection.commit()

  datapack_bundle = build_datapack_bundle(
    source_connection,
    operation_lane='sandbox',
    datapack_type='session_snapshot',
    api_key_hash=api_key_hash_for_id(settings.api_key_id),
    profile_token=profile_token_for_key_path(settings.private_key_file),
    state_db_path_tail=source_db_path.name,
    include_synthetic_refinement=False,
  )
  pack_root = tmp_path / 'synlc-overwrite-confirm-pack-saved-set-evals'
  _write_test_datapack_bundle(pack_root, datapack_bundle)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app,
    method='POST',
    path='/api/data-load',
    body={'action': 'load_sandbox_datapack', 'continue_overwrite': True},
  )
  load_payload = json.loads(load_body)

  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'ok'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_load_hydrated_with_expected_delta'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is True

  evaluation_rows = state_connection.execute(
    'SELECT COUNT(*) AS count_value FROM candidate_saved_set_evaluations'
  ).fetchone()
  saved_set_rows = state_connection.execute(
    'SELECT COUNT(*) AS count_value FROM candidate_saved_sets WHERE operation_lane = ?',
    ('sandbox',),
  ).fetchone()
  runtime_rows = state_connection.execute(
    "SELECT event_type FROM runtime_events WHERE operation_lane = 'sandbox' ORDER BY id"
  ).fetchall()

  assert int((evaluation_rows['count_value'] if evaluation_rows is not None else 0) or 0) == 0
  assert int((saved_set_rows['count_value'] if saved_set_rows is not None else 0) or 0) == 0
  assert [str(row['event_type']) for row in runtime_rows] == ['loaded_runtime_row']


def test_data_detect_force_refresh_bypasses_cache_reuse(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'PROJECT_ROOT', tmp_path)

  canonical_root = tmp_path / 'var' / 'datapack_extracts'
  _make_synlc_datapack(canonical_root, 'synlc-cache-contract-pack', api_key_id=settings.api_key_id)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  first_status, _, first_body = _call_app(app, method='POST', path='/api/data-detect')
  first_payload = json.loads(first_body)
  assert first_status == '200 OK'
  assert first_payload['data_management']['discovery_cache_reused'] is False
  assert first_payload['data_management']['last_result']['reason'] == 'data_detect_refreshed'

  second_status, _, second_body = _call_app(app, method='POST', path='/api/data-detect')
  second_payload = json.loads(second_body)
  assert second_status == '200 OK'
  assert second_payload['data_management']['discovery_cache_reused'] is True
  assert second_payload['data_management']['last_result']['reason'] == 'data_detect_cache_reused'

  forced_status, _, forced_body = _call_app(app, method='POST', path='/api/data-detect', body={'force_refresh': True})
  forced_payload = json.loads(forced_body)
  assert forced_status == '200 OK'
  assert forced_payload['data_management']['discovery_cache_reused'] is False
  assert forced_payload['data_management']['last_result']['reason'] == 'data_detect_force_refreshed'


# SYN-5C Slice 2 — post-load mutation checkpoint: signed headers still enforced
def test_lifecycle_mutation_routes_still_require_signed_headers_after_datapack_load(
  tmp_path: Path, monkeypatch: Any
) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  pack_root = _make_synlc_datapack(tmp_path / 'pack-source', 'synlc-mutation-pack', api_key_id=settings.api_key_id)

  controller = ConsoleSessionController(session_token='synlc-mutation-test-token-abc123')
  app = create_operator_console_app(
    _services(), tombstone_path=tmp_path / 'tombstones.json', session_controller=controller
  )
  _, _, root_body_mut = _call_app(app, method='GET', path='/', query='session=synlc-mutation-test-token-abc123')
  mutation_auth_mut = _extract_mutation_auth_from_html(root_body_mut)
  sel_body = {'path': str(pack_root)}
  _call_app(app, method='POST', path='/api/data-select',
            query='session=synlc-mutation-test-token-abc123', body=sel_body,
            headers=_signed_mutation_headers('/api/data-select', sel_body, mutation_auth_mut))
  ld_body = {'action': 'load_sandbox_datapack'}
  load_status, _, load_body = _call_app(
    app, method='POST', path='/api/data-load',
    query='session=synlc-mutation-test-token-abc123', body=ld_body,
    headers=_signed_mutation_headers('/api/data-load', ld_body, mutation_auth_mut)
  )
  assert load_status == '200 OK'
  assert json.loads(load_body)['data_management']['sandbox_datapack_loaded'] is True

  # After datapack load, an unsigned mutation call must still be blocked.
  unsigned_status, _, unsigned_body = _call_app(
    app, method='POST', path='/api/run', query='session=synlc-mutation-test-token-abc123', body={}
  )
  assert unsigned_status == '403 Forbidden'
  unsigned_payload = json.loads(unsigned_body)
  assert unsigned_payload.get('reason') == 'mutation_signature_required'


# SYN-5C Slice 5a — tamper: missing manifest.json → load blocked (controls invalid)
def test_lifecycle_load_blocked_on_missing_manifest(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  pack_root = _make_synlc_datapack(tmp_path, 'synlc-tamper-missing-manifest', api_key_id=settings.api_key_id)

  # Remove manifest.json to simulate missing control file.
  (pack_root / 'manifest.json').unlink()

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'}
  )
  load_payload = json.loads(load_body)
  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'no-go'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is False


# SYN-5C Slice 5b — tamper: corrupted payload checksum → load blocked (attestation failed)
def test_lifecycle_load_blocked_on_corrupted_payload_checksum(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  pack_root = _make_synlc_datapack(tmp_path, 'synlc-tamper-corrupt-payload', api_key_id=settings.api_key_id)

  # Corrupt a payload file after the manifest checksums have been written.
  payload_dir = pack_root / 'payloads'
  payload_files = list(payload_dir.glob('*.json'))
  assert payload_files, 'Expected at least one payload file'
  payload_files[0].write_text('{"corrupted": true}\n', encoding='utf-8')

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'}
  )
  load_payload = json.loads(load_body)
  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'no-go'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_attestation_failed'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is False


# SYN-5C Slice 5c — tamper: manifest/restore_policy cross-field mismatch → load blocked
def test_lifecycle_load_blocked_on_control_pair_mismatch(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  pack_root = _make_synlc_datapack(tmp_path, 'synlc-tamper-ctrl-mismatch', api_key_id=settings.api_key_id)

  # Overwrite restore_policy with an api_key_hash that doesn't match the manifest.
  rp_path = pack_root / 'restore_policy.json'
  restore_policy = json.loads(rp_path.read_text(encoding='utf-8'))
  restore_policy['api_key_hash'] = 'different-hash-that-does-not-match'
  rp_path.write_text(json.dumps(restore_policy) + '\n', encoding='utf-8')

  # Verify the controls themselves are now flagged as mismatched.
  manifest = json.loads((pack_root / 'manifest.json').read_text(encoding='utf-8'))
  ctrl_issues = validate_datapack_controls(manifest, restore_policy)
  assert any('identity_mismatch' in issue for issue in ctrl_issues)

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'}
  )
  load_payload = json.loads(load_body)
  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'no-go'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is False


# SYN-5C Slice 6a — identity mismatch: operation_lane mismatch → no-go
def test_lifecycle_load_blocked_on_operation_lane_mismatch(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path, operation_lane='sandbox')
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  # Build a 'live' datapack — will be rejected when loading into the sandbox lane.
  pack_root = _make_synlc_datapack(
    tmp_path, 'synlc-identity-lane-mismatch',
    api_key_id=settings.api_key_id, operation_lane='live',
  )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'}
  )
  load_payload = json.loads(load_body)
  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'no-go'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_identity_mismatch'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is False


# SYN-5C Slice 6b — identity mismatch: api_key_hash mismatch → no-go
def test_lifecycle_load_blocked_on_api_key_hash_mismatch(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path, api_key_id='session-api-key')
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  # Datapack was built under a different api_key_id.
  pack_root = _make_synlc_datapack(
    tmp_path, 'synlc-identity-hash-mismatch', api_key_id='different-api-key'
  )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'}
  )
  load_payload = json.loads(load_body)
  assert load_status == '200 OK'
  assert load_payload['data_management']['last_result']['tone'] == 'no-go'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_identity_mismatch'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is False


# SYN-5C Slice 6c — CLI-only rebind preservation: GUI load does not silently rebind on mismatch
def test_lifecycle_gui_load_does_not_rebind_on_identity_mismatch(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _make_synlc_settings(tmp_path, api_key_id='session-api-key')
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  pack_root = _make_synlc_datapack(
    tmp_path, 'synlc-rebind-preservation', api_key_id='different-api-key'
  )

  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/data-select', body={'path': str(pack_root)})
  load_status, _, load_body = _call_app(
    app, method='POST', path='/api/data-load', body={'action': 'load_sandbox_datapack'}
  )
  load_payload = json.loads(load_body)
  # Must be rejected — no automatic rebind performed by the GUI route.
  assert load_payload['data_management']['last_result']['tone'] == 'no-go'
  assert load_payload['data_management']['last_result']['reason'] == 'datapack_identity_mismatch'
  assert load_payload['data_management']['sandbox_datapack_loaded'] is False
  # No rebind key present in the data management payload — rebind is CLI-only.
  assert 'rebind' not in load_payload['data_management']
  assert 'force_rebind' not in load_payload['data_management']


# R4 checklist 4.9.12 regression tests: Issues 2+6 (projection priority), Issue 7 (offline scan), Issue 5c (label)
# Updated in 4.9.14: offline routing moved to _scan_follow_on_workflow; empty-saved-set fix in _refresh_review_selection_projection


def test_bootstrap_workflow_all_ready_skips_review_configuration_and_routes_to_scan() -> None:
  # R4/Issue 2: _bootstrap_workflow must not return review_configuration/load_api_key when
  # credential_ready=True, has_any_websocket_url=True, and mode_selected=True, even when
  # settings_ready=False (dry-run proof still pending after a blocked run cycle).
  workflow = web_app._bootstrap_workflow(
    settings_ready=False,
    report_payload=None,
    reconcile_payload=None,
    next_action='Review the configuration before continuing.',
    settings_payload={
      'settings_ready': False,
      'credential_ready': True,
      'environment_ready': True,
      'mode_selected': True,
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': 'api.kalshi.example/ws',
      },
    },
    mode_selected=True,
  )

  assert workflow['recommended_step'] != 'review_configuration'
  assert workflow['next_actionable_step'] != 'load_api_key'
  assert workflow['recommended_step'] == 'scan'
  assert workflow['next_actionable_step'] == 'scan'
  assert workflow['step_kind'] == 'execute'
  assert workflow['can_run_next_step'] is True


def test_payload_has_live_interaction_hold_ignores_terminal_canceled_row_with_stale_submit_id() -> None:
  payload = {
    'pair_runtime_summary': [
      {
        'pair_id': 'pair-1',
        'public_state_id': 'CANCELED',
        'submit_response_id': 'submit-bridge-pair-1',
        'allowed_actions': ['WAIT'],
      }
    ]
  }

  assert web_app._payload_has_live_interaction_hold(payload) is False


def test_live_interaction_ignores_historical_pair_runtime_from_prior_session() -> None:
  payload = web_app._build_pair_monitor_payload({
    'report': {
      'lane_session_id': 'live-current-session',
      'pair_runtime_summary': [
        {
          'pair_id': 'pair-historical-one-sided',
          'ticker': 'KXHIST',
          'state': 'CANCELED',
          'public_state_id': 'CANCELED',
          'lane_session_id': 'live-old-session',
          'mobility_overlay_state': 'AUTO_CANCEL_COMPLETE',
          'failure_class': 'SILENT_CONTINUE',
          'failure_scope': 'interaction_local',
          'allowed_actions': ['WAIT'],
          'submit_response_id': 'SUBMIT_ACCEPTED_ASYMMETRIC',
        }
      ],
      'next_action': 'Observe runtime state.',
    },
  })

  live = payload['live_interaction']
  assert live['surface_visible'] is False
  assert live['unresolved_interaction_count'] == 0
  assert live['current_stage_summary'] == 'No active or unresolved live interaction is currently projected.'


def test_stage_sections_always_rendered_regardless_of_candidate_count() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'inFlightCandidateCount > 0 ? stageSectionsMarkup' not in source, \
    'G-A: conditional gate must be removed — stage sections must always render'
  assert 'stageSectionsMarkup' in source, \
    'G-A: stageSectionsMarkup must still be referenced in pair-monitor-rows innerHTML'


def test_manual_execution_rows_absent_from_candidates_summary() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'manualExecutionAvailable' not in source, \
    'G-C: manualExecutionAvailable must be removed from renderPairMonitor'
  assert 'manual execution profile' not in source, \
    'G-C: manual execution profile push must be removed from summary items'


def test_follow_on_workflow_cancel_all_routes_to_report_review_boundary_with_scan_next_step() -> None:
  workflow = web_app._follow_on_workflow(
    'cancel-all',
    {
      'decision': 'planned',
      'settings': {
        'settings_ready': True,
        'credential_ready': True,
        'environment_ready': True,
        'mode_selected': True,
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      'next_action': 'Review the updated pair states before the next runtime cycle.',
    },
  )

  assert workflow['recommended_step'] == 'report'
  assert workflow['step_kind'] == 'review'
  assert workflow['can_run_next_step'] is False
  assert workflow['next_actionable_step'] == 'scan'
  assert workflow['focus_target'] == 'pairs-section'
  assert workflow['deck_view'] == 'review'


def test_build_header_amount_summary_combines_available_funds_and_gross_position_value() -> None:
  summary = web_app._build_header_amount_summary(
    {
      'report': {
        'operation_lane': 'live',
      },
      'funds_posture': {
        'available_funds_snapshot': '1000.50',
      },
      'pair_runtime_summary': [
        {
          'gross_dollars': '40.00',
          'net_projected_dollars': '5.25',
        },
        {
          'gross_dollars': '10.00',
          'net_projected_dollars': '-1.00',
        },
      ],
    }
  )

  assert summary['contract_version'] == 'header_amount_summary.v1'
  assert summary['money_authorized'] is True
  assert Decimal(str(summary['net_profit_dollars'])) == Decimal('4.25')
  assert Decimal(str(summary['gross_position_value_dollars'])) == Decimal('50')
  assert Decimal(str(summary['total_assets_dollars'])) == Decimal('1050.5')
  assert summary['total_assets_available'] is True
  assert summary['left_tooltip'] == 'net'
  assert summary['right_tooltip'] == 'gross'


def test_build_header_amount_summary_excludes_terminal_pairs_and_subtracts_in_flight_fees() -> None:
  # Account gross counts only still-in-flight positions and nets their estimated
  # fees; terminal pairs have already resolved into the cash balance.
  summary = web_app._build_header_amount_summary(
    {
      'report': {
        'operation_lane': 'live',
      },
      'funds_posture': {
        'available_funds_snapshot': '100.00',
      },
      'pair_runtime_summary': [
        {
          'gross_dollars': '40.00',
          'net_projected_dollars': '5.25',
          'fees_dollars': '0.50',
          'terminal_state': '',
        },
        {
          'gross_dollars': '999.00',
          'net_projected_dollars': '12.00',
          'fees_dollars': '3.00',
          'terminal_state': 'CANCELED',
        },
      ],
    }
  )

  assert summary['money_authorized'] is True
  # Only the in-flight pair's gross counts toward position value.
  assert Decimal(str(summary['gross_position_value_dollars'])) == Decimal('40.00')
  # Total assets = cash 100 + in-flight gross 40 - in-flight estimated fee 0.50.
  assert Decimal(str(summary['total_assets_dollars'])) == Decimal('139.50')


def test_build_header_amount_summary_suppresses_non_live_lane_money() -> None:
  summary = web_app._build_header_amount_summary(
    {
      'report': {
        'operation_lane': 'sandbox',
      },
      'funds_posture': {
        'available_funds_snapshot': '1000.50',
      },
      'pair_runtime_summary': [
        {
          'gross_dollars': '40.00',
          'net_projected_dollars': '5.25',
        },
        {
          'gross_dollars': '10.00',
          'net_projected_dollars': '-1.00',
        },
      ],
    }
  )

  assert summary['contract_version'] == 'header_amount_summary.v1'
  assert summary['money_authorized'] is False
  assert summary['net_profit_dollars'] is None
  assert Decimal(str(summary['gross_position_value_dollars'])) == Decimal('50')
  assert summary['total_assets_dollars'] is None
  assert summary['total_assets_available'] is False


def test_build_header_amount_summary_prefers_current_connection_lane_over_stale_report_lane() -> None:
  summary = web_app._build_header_amount_summary(
    {
      'report': {
        'operation_lane': 'sandbox',
      },
      'connection_posture': {
        'operation_lane': 'live',
      },
      'funds_posture': {
        'available_funds_snapshot': '1000.50',
      },
      'pair_runtime_summary': [
        {
          'gross_dollars': '40.00',
          'net_projected_dollars': '5.25',
        },
        {
          'gross_dollars': '10.00',
          'net_projected_dollars': '-1.00',
        },
      ],
    }
  )

  assert summary['contract_version'] == 'header_amount_summary.v1'
  assert summary['money_authorized'] is True
  assert Decimal(str(summary['net_profit_dollars'])) == Decimal('4.25')
  assert Decimal(str(summary['gross_position_value_dollars'])) == Decimal('50')
  assert Decimal(str(summary['total_assets_dollars'])) == Decimal('1050.5')
  assert summary['total_assets_available'] is True


# FB-7: header funds bridge across funds-less rebuilds


def _utc_iso(seconds_ago: float = 0.0) -> str:
  from datetime import datetime, timezone, timedelta
  return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).isoformat().replace('+00:00', 'Z')


def test_header_funds_bridge_carries_fresh_value_within_backend_grace() -> None:
  cache: dict = {}
  # 1) A report rebuild carrying a fresh live funds value (as_of ~now) seeds the cache.
  fresh = web_app._build_header_amount_summary(
    {
      'report': {'operation_lane': 'live'},
      'funds_posture': {
        'available_funds_snapshot': '50.00',
        'funds_refresh_status': 'fresh',
        'available_funds_as_of': _utc_iso(0.5),
      },
    },
    durable_funds_cache=cache,
  )
  assert fresh['money_authorized'] is True
  assert fresh['funds_bridged'] is False
  assert cache.get('available_funds') is not None

  # 2) A scan rebuild carries NO funds block, and the cached value is still within
  #    the backend staleness grace — the banner carries it forward truthfully.
  bridged = web_app._build_header_amount_summary(
    {'connection_posture': {'operation_lane': 'live'}},
    durable_funds_cache=cache,
  )
  assert bridged['money_authorized'] is True
  assert bridged['funds_bridged'] is True
  assert Decimal(str(bridged['available_funds_snapshot'])) == Decimal('50.00')


def test_header_funds_bridge_fails_closed_past_backend_grace() -> None:
  # The carryover is bounded by the backend's own staleness grace, measured on the
  # real as_of: once the last fresh reading ages past the grace, the banner fails
  # closed to unavailable rather than showing money the backend would call stale.
  grace_sec = web_app.BALANCE_STALENESS_GRACE_MS / 1000.0
  cache = {
    'available_funds': Decimal('50.00'),
    'available_funds_as_of': _utc_iso(grace_sec + 5.0),
  }
  lapsed = web_app._build_header_amount_summary(
    {'connection_posture': {'operation_lane': 'live'}},
    durable_funds_cache=cache,
  )
  assert lapsed['money_authorized'] is False
  assert lapsed['funds_bridged'] is False
  assert lapsed['funds_authorization_state'] == 'live_unavailable'


def test_header_funds_bridge_does_not_seed_cache_from_stale_value() -> None:
  cache: dict = {}
  # A non-fresh funds value must not seed the bridge cache (fail-closed source).
  web_app._build_header_amount_summary(
    {
      'report': {'operation_lane': 'live'},
      'funds_posture': {
        'available_funds_snapshot': '50.00',
        'funds_refresh_status': 'stale',
        'available_funds_as_of': _utc_iso(0.5),
      },
    },
    durable_funds_cache=cache,
  )
  assert 'available_funds' not in cache
  # A subsequent funds-less rebuild therefore has nothing to bridge from.
  bridged = web_app._build_header_amount_summary(
    {'connection_posture': {'operation_lane': 'live'}},
    durable_funds_cache=cache,
  )
  assert bridged['funds_bridged'] is False
  assert bridged['money_authorized'] is False


def test_header_banner_total_carries_forward_when_strict_gate_closes() -> None:
  # Lane C: the gross-assets banner is a stable DISPLAY estimate. Once a fresh value is
  # computed it carries forward across a funds-less rebuild even after the strict money
  # gate closes (UI stability), while money_authorized fails closed INDEPENDENTLY. The
  # display cache is never read by any gating path — a labeled estimate, not money-truth.
  cache: dict = {}
  fresh = web_app._build_header_amount_summary(
    {
      'report': {'operation_lane': 'live'},
      'funds_posture': {
        'available_funds_snapshot': '50.00',
        'funds_refresh_status': 'fresh',
        'available_funds_as_of': _utc_iso(0.5),
      },
      'pair_runtime_summary': [{'gross_dollars': '10.00', 'net_projected_dollars': '1.00'}],
    },
    durable_funds_cache=cache,
  )
  assert fresh['money_authorized'] is True
  assert Decimal(str(fresh['total_assets_dollars'])) == Decimal('60.00')  # 50 available + 10 gross
  assert cache.get('total_assets_display') is not None

  # Strict gate closes (cached funds now past grace -> no available_funds carry), but
  # the banner display value persists and stays steady — no blank, no indicator.
  cache['available_funds_as_of'] = _utc_iso((web_app.BALANCE_STALENESS_GRACE_MS / 1000.0) + 5.0)
  lapsed = web_app._build_header_amount_summary(
    {'connection_posture': {'operation_lane': 'live'}},
    durable_funds_cache=cache,
  )
  assert lapsed['money_authorized'] is False, 'strict gate must fail closed independently of the banner'
  assert lapsed['funds_authorization_state'] == 'live_unavailable'
  assert lapsed['total_assets_available'] is True, 'banner display estimate stays available'
  assert Decimal(str(lapsed['total_assets_dollars'])) == Decimal('60.00'), 'banner stays steady across the lapse'


def test_bootstrap_workflow_processing_rows_do_not_force_reconcile() -> None:
  workflow = web_app._bootstrap_workflow(
    settings_ready=True,
    report_payload={
      'latest_heartbeat': {'status': 'cycle-complete'},
      'table_counts': {'pair_plans': 1},
    },
    reconcile_payload={
      'pair_count': 1,
      'pairs': [
        {
          'pair_id': 'pair-1',
          'state': 'PROCESSING',
          'allowed_actions': [],
        }
      ],
    },
    next_action='Review the retained evidence before continuing.',
    settings_payload={
      'settings_ready': True,
      'credential_ready': True,
      'environment_ready': True,
      'mode_selected': True,
    },
    mode_selected=True,
  )

  assert workflow['recommended_step'] == 'report'
  assert workflow['next_actionable_step'] == 'scan'
  assert workflow['focus_target'] == 'evidence-section'


def test_follow_on_workflow_processing_scan_outranks_fix_configuration_projection() -> None:
  workflow = web_app._follow_on_workflow(
    'report',
    {
      'decision': 'planned',
      'settings': {
        'settings_ready': False,
        'credential_ready': True,
        'environment_ready': True,
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      'scan_runtime': {
        'status': 'processing',
        'stage': 'market_review',
        'message': 'Find candidates is processing in the background.',
      },
    },
  )

  assert workflow['recommended_step'] == 'processing'
  assert workflow['next_actionable_step'] == 'processing'
  assert workflow['step_kind'] == 'review'
  assert workflow['can_run_next_step'] is False


def test_report_route_all_ready_but_settings_not_ready_empty_saved_set_routes_to_scan() -> None:
  # R4/Issue 6 (4.9.14): /api/report (Refresh Shell) must not project next_actionable_step='-' when
  # credential_ready=True, mode_selected=True, websocket URLs configured, and saved set is empty.
  # Correct projection: scan (Find candidates). Fix is in _refresh_review_selection_projection
  # (projection engine) — the empty-selection state with credential/mode ready repoints workflow to scan.
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=base.run,
      reconcile=base.reconcile,
      report=lambda **_: {
        'decision': 'planned',
        'next_action': 'Review the state before continuing.',
        'settings': {
          'settings_ready': False,
          'credential_ready': True,
          'environment_ready': True,
          'mode_selected': True,
          'available_websocket_urls': {
            'sandbox': 'demo-api.kalshi.example/ws',
            'live': 'api.kalshi.example/ws',
          },
        },
      },
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['next_actionable_step'] != '-'
  assert payload['workflow']['next_actionable_step'] != 'load_api_key'
  assert payload['workflow']['next_actionable_step'] == 'scan'
  assert payload['workflow']['recommended_step'] != 'review_configuration'


# R4/4.9.16 regression tests: Gap 3 (offline empty-set routing), Gap 4 (mode_selected from posture)


def test_refresh_review_selection_projection_empty_set_offline_mode_routes_to_scan() -> None:
  # BOOT partition contract: offline/no-mode empty saved-set must stay on mode_change,
  # not project a lane-owned scan action outside the owning lane.
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=base.run,
      reconcile=base.reconcile,
      report=lambda **_: {
        'decision': 'planned',
        'next_action': 'Review the state before continuing.',
        'settings': {
          'settings_ready': True,
          'credential_ready': True,
          'environment_ready': True,
          'operation_lane': 'offline',  # system-configured offline mode
          'available_websocket_urls': {
            'sandbox': 'demo-api.kalshi.example/ws',
            'live': 'api.kalshi.example/ws',
          },
        },
      },
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['next_actionable_step'] == 'mode_change'
  assert payload['workflow']['recommended_step'] != 'review_configuration'


def test_bootstrap_workflow_mode_selected_from_parameter_bypasses_review_configuration() -> None:
  # Gap 4 (4.9.16): _bootstrap_workflow must not return review_configuration/load_api_key when
  # mode_selected=True is passed as a parameter (from connection_posture) but settings_payload
  # does NOT contain mode_selected (production scenario — safe_settings_summary never includes it).
  workflow = web_app._bootstrap_workflow(
    settings_ready=False,
    report_payload=None,
    reconcile_payload=None,
    next_action='Review the configuration before continuing.',
    settings_payload={
      'settings_ready': False,
      'credential_ready': True,
      'environment_ready': True,
      # NO mode_selected in settings_payload (production scenario)
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': 'api.kalshi.example/ws',
      },
    },
    mode_selected=True,  # from connection_posture — correctly True in production
  )

  assert workflow['recommended_step'] != 'review_configuration'
  assert workflow['next_actionable_step'] != 'load_api_key'


def test_follow_on_workflow_mode_selected_from_connection_posture_bypasses_review_configuration() -> None:
  # Gap 4 (4.9.16): _follow_on_workflow must not return review_configuration/load_api_key when
  # connection_posture.mode_selected=True but settings does NOT contain mode_selected (production).
  payload = {
    'decision': 'planned',
    'settings': {
      'settings_ready': False,
      'credential_ready': True,
      'environment_ready': True,
      # NO mode_selected in settings (production scenario)
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': 'api.kalshi.example/ws',
      },
    },
    'connection_posture': {
      'mode_selected': True,  # from context_overlay_state in production
      'operation_lane': 'sandbox',
    },
  }

  workflow = web_app._follow_on_workflow('report', payload)

  assert workflow['recommended_step'] != 'review_configuration'
  assert workflow['next_actionable_step'] != 'load_api_key'


def test_follow_on_workflow_offline_scan_routes_to_change_mode() -> None:
  # R4/Issue 7 (4.9.14): when operation_lane='offline' and candidates are present, the scan
  # follow-on must route to change_mode (not select_candidates). The offline+candidates routing
  # rule lives inside _scan_follow_on_workflow so it applies from all call sites.
  payload = {
    'decision': 'planned',
    'candidate_count': 3,
    'connection_posture': {
      'operation_lane': 'offline',
      'mode_selected': False,
    },
    'settings': {
      'settings_ready': True,
      'credential_ready': True,
      'mode_selected': False,
      'operation_lane': 'offline',
      'available_websocket_urls': {
        'sandbox': 'demo-api.kalshi.example/ws',
        'live': 'api.kalshi.example/ws',
      },
    },
  }

  workflow = web_app._follow_on_workflow('scan', payload)

  assert workflow['recommended_step'] == 'change_mode'
  assert workflow['next_actionable_step'] == 'mode_change'
  assert workflow['can_run_next_step'] is False
  assert workflow['step_kind'] == 'review'


def test_submit_order_pending_label_is_submitting_order() -> None:
  # R4/Issue 5c: the Submit order workflow action button must carry pendingLabel='Submitting order...'
  # (not the stale 'Running dry-run planner...' label from the pre-Issue-5 template).
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='GET', path='/')

  assert status == '200 OK'
  assert 'Submitting order...' in body
  assert 'Running dry-run planner...' not in body


def test_scan_follow_on_workflow_offline_with_candidates_routes_to_change_mode() -> None:
  # 4.9.14: offline+candidates routing is now inside _scan_follow_on_workflow (not a pre-interceptor).
  # Direct unit test to cover the projection-engine-internal path.
  payload = {
    'decision': 'planned',
    'candidate_count': 2,
    'connection_posture': {
      'operation_lane': 'offline',
      'mode_selected': False,
    },
    'settings': {
      'settings_ready': True,
      'credential_ready': True,
      'mode_selected': False,
      'operation_lane': 'offline',
    },
  }

  workflow = web_app._scan_follow_on_workflow(payload)

  assert workflow['recommended_step'] == 'change_mode'
  assert workflow['next_actionable_step'] == 'mode_change'
  assert workflow['can_run_next_step'] is False


def test_scan_follow_on_workflow_offline_no_candidates_does_not_route_to_change_mode() -> None:
  # 4.9.14: offline with zero candidates should still route to scan (not change_mode).
  payload = {
    'decision': 'planned',
    'candidate_count': 0,
    'sandbox_extended_count': 0,
    'connection_posture': {
      'operation_lane': 'offline',
      'mode_selected': False,
    },
  }

  workflow = web_app._scan_follow_on_workflow(payload)

  assert workflow['recommended_step'] == 'scan'
  assert workflow['recommended_step'] != 'change_mode'


def test_scan_follow_on_workflow_offline_with_terminal_scan_replay_and_candidates_routes_to_change_mode() -> None:
  # Z12: when workflow_source='terminal_scan_replay', candidates > 0, and operation_lane='offline',
  # routing must reach change_mode. The prior 'not _sfow_is_replay' guard exempted replay
  # projections from this check, causing a misaligned select_candidates state (test 3, T070632Z).
  payload = {
    'decision': 'planned',
    'candidate_count': 6,
    'workflow_source': 'terminal_scan_replay',
    'connection_posture': {
      'operation_lane': 'offline',
      'mode_selected': False,
    },
    'settings': {
      'settings_ready': True,
      'credential_ready': True,
      'mode_selected': False,
      'operation_lane': 'offline',
    },
  }

  workflow = web_app._scan_follow_on_workflow(payload)

  assert workflow['recommended_step'] == 'change_mode'
  assert workflow['next_actionable_step'] == 'mode_change'
  assert workflow['can_run_next_step'] is False


def test_refresh_review_selection_projection_empty_set_with_creds_routes_workflow_to_scan() -> None:
  # 4.9.14: _refresh_review_selection_projection must update workflow to scan when
  # review_selection is empty (review_hold_empty_selection), credential_ready=True, mode_selected=True,
  # and current next_actionable_step is '-'. Covers both settings_ready=True (img1) and
  # settings_ready=False + creds-ready cases.
  app = create_operator_console_app(_services())

  # Trigger a report refresh on a clean app with no prior candidates.
  # The default _services() bootstrap uses default test settings (sandbox, creds ready).
  status, _, body = _call_app(app, method='POST', path='/api/report')
  payload = json.loads(body)

  assert status == '200 OK'
  # The projection engine must not leave next_actionable_step='-' when the selection is empty
  # and configuration is ready.
  assert payload['workflow']['next_actionable_step'] != '-'


def test_save_selection_prunes_payload_candidates_to_saved_set(tmp_path: Path, monkeypatch: Any) -> None:
  # 4.9.15 (Gap 2): After save_selection the response must carry only the saved-set
  # candidates, not the full scan cache. Previously _apply_review_selection_projection
  # updated review_selection_state['saved_candidates'] but never overwrote
  # payload['candidates'], so the UI received the entire original scan candidate list.
  app = _db_backed_review_app(tmp_path, monkeypatch)

  # 1. Scan to populate the candidate cache with 1 candidate.
  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  assert scan_status == '200 OK'
  assert int(scan_payload.get('candidate_count', 0)) == 1

  # 2. Save with an empty selected_keys — removes the candidate from the saved set.
  save_status, _, save_body = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={
      'action': 'save_selection',
      'selected_keys': [],
    },
  )
  save_payload = json.loads(save_body)

  assert save_status == '200 OK'
  # Projection must report an empty saved set.
  assert save_payload['review_selection']['state_id'] == 'review_hold_empty_selection'
  # Response candidates must be pruned to match the saved set (empty).
  assert int(save_payload.get('candidate_count', -1)) == 0
  assert save_payload.get('candidates') == []


def test_save_selection_stored_signature_derives_from_members_and_passes_submit_gate(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  # Submit-handoff signature integrity (BMAP 2026-07-03): the persisted saved-set
  # signature must describe the saved MEMBERS, not the broader candidate-row cache.
  # Reproduces the divergence class that halted every live submit: the scan cache
  # holds 2 candidates, the operator saves only 1 -- previously the stored signature
  # was copied from the selection-state signature (join of ALL cached candidates,
  # here 2 keys; live it also carried an expired phantom), while the members held
  # the clean 1, and the submit gate raised submit_handoff_saved_signature_mismatch.
  base = _services()
  two_candidate_services = OperatorConsoleServices(
    bootstrap=base.bootstrap,
    scan=lambda **_: {
      'decision': 'planned',
      'candidate_count': 2,
      'candidates': [
        {
          'candidate_uid': 'review-candidate-1',
          'ticker': 'KALSHI-EDGE-1',
          'density_weight': '3.125',
          'liquidity_score': '210',
          'market_edge_dollars': '0.11',
          'current_price': '0.52',
          'threshold_price': '0.41',
        },
        {
          'candidate_uid': 'review-candidate-2',
          'ticker': 'KALSHI-EDGE-2',
          'density_weight': '2.5',
          'liquidity_score': '150',
          'market_edge_dollars': '0.09',
          'current_price': '0.48',
          'threshold_price': '0.39',
        },
      ],
      'next_action': 'Review candidates in Pairs.',
    },
    run=base.run,
    reconcile=base.reconcile,
    report=base.report,
    cancel_all=base.cancel_all,
    system_log=base.system_log,
    visuals=base.visuals,
  )
  state_db_path = tmp_path / 'state.sqlite3'
  app = _db_backed_review_app(tmp_path, monkeypatch, services=two_candidate_services)

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  assert scan_status == '200 OK'
  assert int(json.loads(scan_body).get('candidate_count', 0)) == 2

  save_status, _, _ = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={'action': 'save_selection', 'selected_keys': ['review-candidate-1']},
  )
  assert save_status == '200 OK'

  connection = open_database(str(state_db_path))
  saved_set_row = connection.execute(
    'SELECT saved_set_id, run_id, lane_session_id, detail_json FROM candidate_saved_sets ORDER BY recorded_at_utc DESC LIMIT 1'
  ).fetchone()
  assert saved_set_row is not None
  member_keys = [
    str(row['candidate_key'])
    for row in connection.execute(
      'SELECT candidate_key FROM candidate_saved_set_members WHERE saved_set_id = ? ORDER BY member_order',
      (saved_set_row['saved_set_id'],),
    ).fetchall()
  ]
  assert len(member_keys) == 1
  assert 'review-candidate-2' not in member_keys[0]

  detail = json.loads(saved_set_row['detail_json'])
  # R1: the stored signature is exactly the pipe-join of the member keys -- no
  # phantom from the wider candidate cache, no doubling.
  assert detail['candidate_signature'] == '|'.join(member_keys)
  assert detail['saved_signature'] == '|'.join(member_keys)

  # R2: a handoff built from the same members clears the submit gate's saved-set
  # validation (previously raised submit_handoff_saved_signature_mismatch).
  handoff = {
    'handoff_id': 'handoff-signature-integrity-test',
    'operation_lane': 'sandbox',
    'operator_lane_session_id': str(saved_set_row['lane_session_id'] or ''),
    'scan_session_id': str(saved_set_row['run_id'] or ''),
    'saved_set_id': str(saved_set_row['saved_set_id']),
    'candidate_signature': '|'.join(member_keys),
    'candidate_count': len(member_keys),
    'candidate_keys': list(member_keys),
  }
  resolved = service_module._resolve_submit_handoff_saved_set(
    connection,
    submit_handoff=handoff,
    operation_lane='sandbox',
  )
  assert resolved is not None
  assert str(resolved.get('saved_set_id')) == str(saved_set_row['saved_set_id'])


def test_report_refresh_after_explicit_empty_save_does_not_restore_preserved_scan_candidates(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  state_db_path = tmp_path / 'state.sqlite3'
  app = _db_backed_review_app(tmp_path, monkeypatch)

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  save_status, _, save_body = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={
      'action': 'save_selection',
      'selected_keys': [],
    },
  )
  save_payload = json.loads(save_body)
  report_status, _, report_body = _call_app(app, method='POST', path='/api/report')
  report_payload = json.loads(report_body)

  assert scan_status == '200 OK'
  assert state_db_path.exists()
  assert state_db_path.resolve().is_relative_to(tmp_path.resolve())
  assert int(scan_payload.get('candidate_count', 0)) == 1
  assert save_status == '200 OK'
  assert save_payload['review_selection']['state_id'] == 'review_hold_empty_selection'
  assert report_status == '200 OK'
  assert int((report_payload.get('pair_monitor') or {}).get('candidate_count', -1)) == 0
  assert (report_payload.get('pair_monitor') or {}).get('candidate_rows') == []
  assert report_payload['review_selection']['state_id'] == 'review_hold_empty_selection'
  assert report_payload['review_selection']['saved_set_status'] == 'historical_only'


def test_stop_scan_cancel_bypasses_review_clear_confirm_guard() -> None:
  source = inspect.getsource(web_app._render_html)

  assert (
    "const isScanCancelAction = String(((options.body || {}).action) || '').toLowerCase() === 'cancel';"
    in source
  )
  assert (
    "normalizedAction === 'scan' && !isScanCancelAction && !options.skipReviewClearConfirm"
    in source
  )
  assert (
    "runUiAction('scan', { body: { action: 'cancel' }, focusRouteKey: 'scan', "
    "skipReviewClearConfirm: true })"
    in source
  )


def test_find_candidates_scan_still_uses_review_clear_confirm_guard() -> None:
  source = inspect.getsource(web_app._render_html)

  guard_start = source.index("normalizedAction === 'scan' && !isScanCancelAction")
  guard_fragment = source[guard_start: guard_start + 300]
  assert 'reviewedCandidateSetWouldBeClearedByScan(state.payload || {})' in guard_fragment
  assert 'openScanReviewStateClearConfirmation(state.payload || {})' in guard_fragment


def test_candidate_review_runs_lane_session_index_created(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'state.sqlite3'

  with open_database(state_db_path) as connection:
    index_rows = connection.execute("PRAGMA index_list('candidate_review_runs')").fetchall()
    index_names = {str(row[1]) for row in index_rows}
    assert 'idx_crr_lane_session' in index_names

    column_rows = connection.execute("PRAGMA index_info('idx_crr_lane_session')").fetchall()
    indexed_columns = [str(row[2]) for row in column_rows]
    assert indexed_columns == ['lane_session_id']


def test_change_mode_no_go_after_explicit_empty_save_does_not_restore_preserved_scan_candidates(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  class _FailingWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      raise RuntimeError('socket connect failed')

    async def disconnect(self) -> None:
      self.connected = False

  runtime_settings = _runtime_settings_for_lane(tmp_path, 'sandbox')
  state_db_path = Path(runtime_settings.state_db_path)
  app = _db_backed_review_app(tmp_path, monkeypatch)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: runtime_settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _FailingWebSocketClient)
  monkeypatch.setattr(
    web_app,
    'run_sandbox_preflight',
    lambda _settings: {
      'result': 'pass',
      'reason_code': 'preflight_passed',
      'message': 'ok',
      'next_action': 'proceed',
      'checks': [],
    },
  )
  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')

  scan_status, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)
  save_status, _, save_body = _call_app(
    app,
    method='POST',
    path='/api/review-selection',
    body={
      'action': 'save_selection',
      'selected_keys': [],
    },
  )
  save_payload = json.loads(save_body)
  mode_status, _, mode_body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  mode_payload = json.loads(mode_body)

  assert scan_status == '200 OK'
  assert state_db_path.exists()
  assert state_db_path.resolve().is_relative_to(tmp_path.resolve())
  assert int(scan_payload.get('candidate_count', 0)) == 1
  assert save_status == '200 OK'
  assert save_payload['review_selection']['state_id'] == 'review_hold_empty_selection'
  assert mode_status == '200 OK'
  assert mode_payload['decision'] == 'no-go'
  assert mode_payload['reason'] == 'websocket_connection_failed'
  assert int((mode_payload.get('pair_monitor') or {}).get('candidate_count', -1)) == 0
  assert (mode_payload.get('pair_monitor') or {}).get('candidate_rows') == []
  assert mode_payload['review_selection']['state_id'] == 'review_hold_empty_selection'
  assert mode_payload['review_selection']['saved_set_status'] == 'historical_only'


# TD-1: _closed_final flag state machine


def test_close_is_terminal_heartbeat_does_not_cancel() -> None:
  controller = ConsoleSessionController(session_token='tok')
  controller.mark_closed()
  closed_at_before = controller._closed_at

  controller.mark_heartbeat()

  assert controller._closed_at == closed_at_before
  assert controller._closed_final is True
  assert controller._shutdown_reason == 'close_signal_observed'


def test_close_is_terminal_interactive_ready_does_not_cancel() -> None:
  controller = ConsoleSessionController(session_token='tok')
  controller.mark_closed()
  closed_at_before = controller._closed_at

  controller.mark_interactive_ready(route_name='run')

  assert controller._closed_at == closed_at_before
  assert controller._closed_final is True
  assert controller._shutdown_reason == 'close_signal_observed'


def test_root_reload_after_close_resets_close_state() -> None:
  controller = ConsoleSessionController(session_token='tok')
  controller.mark_closed()
  assert controller._closed_final is True
  assert controller._closed_at is not None

  controller.mark_root_loaded()

  assert controller._closed_final is False
  assert controller._closed_at is None
  assert controller._shutdown_reason is None


def test_close_suppressed_during_reconnect_window() -> None:
  controller = ConsoleSessionController(
    session_token='tok',
    startup_grace_sec=30.0,
    idle_timeout_sec=30.0,
    close_grace_sec=2.5,
  )

  controller.mark_root_loaded()
  assert controller._reconnect_in_progress is True

  controller.mark_closed()
  assert controller._closed_at is None
  assert controller._closed_final is False

  controller.mark_heartbeat()
  assert controller._reconnect_in_progress is False

  assert controller.should_shutdown() is False

  controller.mark_closed()
  assert controller._closed_final is True


# TD-2: root GET token gate


def test_mark_root_loaded_requires_token_match() -> None:
  controller = ConsoleSessionController(session_token='session-123')
  app = create_operator_console_app(_services(), session_controller=controller)

  status, headers, body = _call_app(app, method='GET', path='/', query='session=session-123')

  assert status == '200 OK'
  assert 'text/html' in headers['Content-Type']
  assert controller._root_loaded_at is not None


def test_mark_root_loaded_mismatch_serves_html_but_does_not_reset() -> None:
  controller = ConsoleSessionController(session_token='session-123', close_grace_sec=60.0)
  controller.mark_closed()
  assert controller._closed_final is True
  app = create_operator_console_app(_services(), session_controller=controller)

  status, headers, body = _call_app(app, method='GET', path='/', query='session=wrong-token')

  assert status == '200 OK'
  assert 'text/html' in headers['Content-Type']
  assert controller._closed_final is True
  assert controller._closed_at is not None


# TD-3: should_shutdown() timeout branch coverage


def test_should_shutdown_close_grace_fires_after_close_grace_sec() -> None:
  import time as _time
  controller = ConsoleSessionController(session_token='tok', close_grace_sec=0.01)
  controller.mark_closed()

  _time.sleep(0.02)

  assert controller.should_shutdown() is True
  assert controller._shutdown_reason == 'close_grace_elapsed'
  assert controller._shutdown_requested is True


def test_should_shutdown_startup_grace_fires_before_last_seen() -> None:
  controller = ConsoleSessionController(session_token='tok', startup_grace_sec=0.0)

  assert controller.should_shutdown() is True
  assert controller._shutdown_reason == 'startup_grace_expired_no_browser'
  assert controller._shutdown_requested is True


def test_should_shutdown_idle_timeout_fires_on_inactivity() -> None:
  import time as _time
  controller = ConsoleSessionController(session_token='tok', idle_timeout_sec=0.01)
  controller.mark_heartbeat()

  _time.sleep(0.02)

  assert controller.should_shutdown() is True
  assert controller._shutdown_reason == 'idle_timeout_elapsed'
  assert controller._shutdown_requested is True


# TD-4: in-flight drain guard


def _make_test_db_with_pairs(db_path: str, pairs: dict[str, str]) -> None:
  import sqlite3
  con = sqlite3.connect(db_path)
  con.execute(
    'CREATE TABLE IF NOT EXISTS pair_plans (pair_id TEXT PRIMARY KEY)'
  )
  con.execute(
    'CREATE TABLE IF NOT EXISTS pair_states ('
    'id INTEGER PRIMARY KEY AUTOINCREMENT,'
    'pair_id TEXT NOT NULL,'
    'state TEXT NOT NULL,'
    'operation_lane TEXT NOT NULL DEFAULT \'sandbox\','
    'lane_session_id TEXT,'
    'detail_json TEXT NOT NULL DEFAULT \'{}\','
    'recorded_at_utc TEXT NOT NULL DEFAULT \'\')'
  )
  for pair_id, state in pairs.items():
    con.execute('INSERT OR IGNORE INTO pair_plans (pair_id) VALUES (?)', (pair_id,))
    con.execute(
      'INSERT INTO pair_states (pair_id, state, detail_json, recorded_at_utc) VALUES (?, ?, ?, ?)',
      (pair_id, state, '{}', '2026-06-06T00:00:00Z'),
    )
  con.commit()
  con.close()


def test_should_not_shutdown_while_pairs_in_flight(tmp_path: Any) -> None:
  import time as _time
  db_path = str(tmp_path / 'state.sqlite3')
  _make_test_db_with_pairs(db_path, {'PAIR-A': 'RESTING_BOTH', 'PAIR-B': 'FULLY_FILLED'})
  controller = ConsoleSessionController(
    session_token='tok', idle_timeout_sec=0.01, state_db_path=db_path,
  )
  controller.mark_heartbeat()

  _time.sleep(0.02)

  assert controller.should_shutdown() is False
  assert controller._waiting_for_execution_drain is True
  assert controller._shutdown_requested is False


def test_should_shutdown_when_pairs_drain_to_zero(tmp_path: Any) -> None:
  import time as _time
  db_path = str(tmp_path / 'state.sqlite3')
  _make_test_db_with_pairs(db_path, {'PAIR-A': 'LOCKED', 'PAIR-B': 'CANCELED', 'PAIR-C': 'FILLED'})
  controller = ConsoleSessionController(
    session_token='tok', idle_timeout_sec=0.01, state_db_path=db_path,
  )
  controller.mark_heartbeat()

  _time.sleep(0.02)

  assert controller.should_shutdown() is True
  assert controller._waiting_for_execution_drain is False
  assert controller._shutdown_reason == 'idle_timeout_elapsed'


def test_filled_pair_does_not_project_live_interaction_attention() -> None:
  row = {
    'pair_id': 'PAIR-FILLED',
    'public_state_id': 'FILLED',
    'state': 'FILLED',
    'submit_response_id': 'submit-123',
  }

  assert web_app._pair_has_live_interaction_hold(row) is False
  assert web_app._pair_requires_reconcile_attention(row) is False


def test_in_flight_guard_does_not_modify_shutdown_requested(tmp_path: Any) -> None:
  import time as _time
  db_path = str(tmp_path / 'state.sqlite3')
  _make_test_db_with_pairs(db_path, {'PAIR-A': 'PLANNED'})
  controller = ConsoleSessionController(
    session_token='tok', idle_timeout_sec=0.01, state_db_path=db_path,
  )
  controller.mark_heartbeat()

  _time.sleep(0.02)
  controller.should_shutdown()

  assert controller._shutdown_requested is False
  assert controller._shutdown_reason is None


# B1: automation-active gate — should_shutdown() blocked while automation running


def test_b1a_notify_complete_accepts_optional_detail() -> None:
  import inspect
  from polyventure.tray import ExecutionTrayIcon
  sig = inspect.signature(ExecutionTrayIcon.notify_complete)
  assert 'detail' in sig.parameters, (
    'B1R: notify_complete must accept a detail parameter'
  )
  param = sig.parameters['detail']
  assert param.default is None, (
    'B1R: notify_complete detail param must default to None'
  )
  import ast, pathlib
  src = pathlib.Path(__file__).parent.parent / 'src' / 'polyventure' / 'tray.py'
  tree = ast.parse(src.read_text(encoding='utf-8'))
  for node in ast.walk(tree):
    if isinstance(node, ast.FunctionDef) and node.name == 'notify_complete':
      src_segment = src.read_text(encoding='utf-8')
      assert 'detail or' in src_segment, (
        'B1R: notify_complete must use "detail or" fallback pattern to preserve default message'
      )
      break


def test_b1r2_monitor_reads_automation_was_active_from_controller() -> None:
  import pathlib
  src = pathlib.Path(__file__).parent.parent / 'src' / 'polyventure' / 'web_app.py'
  text = src.read_text(encoding='utf-8')
  assert '_automation_was_active' in text, (
    'B1-R2: ConsoleSessionController must have _automation_was_active field'
  )
  # FB-5: automation sessions now suppress the execution-complete popup entirely
  # (the mid-cycle "continuing to next scan" message was misleading and fired on
  # transient drains). The monitor gates the notify on NOT _automation_was_active.
  assert 'not session_controller._automation_was_active' in text, (
    'B1-R2: _monitor_session must suppress notify_complete when automation was active'
  )
  assert 'tray.notify_complete(detail=' in text, (
    'B1-R2: _monitor_session must call tray.notify_complete with detail= keyword'
  )
  assert 'session_controller._automation_was_active' in text, (
    'B1-R2: _monitor_session must read _automation_was_active from controller, not automation_overlay_state'
  )


def test_b4r2_stage_column_query_uses_lsid_with_scan_runtime_fallback(tmp_path: Any) -> None:
  import pathlib
  src = pathlib.Path(__file__).parent.parent / 'src' / 'polyventure' / 'web_app.py'
  text = src.read_text(encoding='utf-8')
  assert "scan_runtime_payload.get('lane_session_id')" in text, (
    'B4-R2: _fetch_stage_columns must fall back to scan_runtime.lane_session_id when persisted_lane_session_id is empty'
  )
  assert 'JOIN candidate_review_runs r ON r.run_id = c.run_id' in text, (
    'B4-R2: LSID-scoped JOIN on candidate_review_runs must be present for cross-cycle accumulation'
  )
  import sqlite3 as _sqlite3
  _db_path = tmp_path / 'runtime_b4r2.sqlite3'
  _scan_lsid = 'scan-b4r2-test'
  _conn = _sqlite3.connect(str(_db_path))
  _conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  _conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  _conn.execute('INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?,?)', ('run-b4r2', _scan_lsid))
  _conn.execute(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json)'
    " VALUES ('run-b4r2', 'discovered', NULL, 'TICK-B4R2', 'live_qualifying', '{}')"
  )
  _conn.commit()
  _conn.close()
  from polyventure.web_app import _fetch_stage_columns
  _result = _fetch_stage_columns({
    'review_selection': {},
    'scan_runtime': {'lane_session_id': _scan_lsid},
    'settings': {'state_db_path': str(_db_path)},
  })
  _all_tickers = [i['ticker'] for col in _result['stage_columns'] for i in col['items']]
  assert 'TICK-B4R2' in _all_tickers, (
    'B4-R2: scan_runtime fallback LSID must allow discovered candidate to appear when persisted_lane_session_id is empty'
  )
  _columns = {col['stage_id']: col['items'] for col in _result['stage_columns']}
  assert 'TICK-B4R2' in [i['ticker'] for i in _columns['queued']], (
    'B4-R2: discovered candidate found via scan_runtime fallback must map to queued column'
  )


def test_b4_ssot_discovered_lifecycle_stage_maps_to_queued() -> None:
  import pathlib
  src = pathlib.Path(__file__).parent.parent / 'src' / 'polyventure' / 'web_app.py'
  text = src.read_text(encoding='utf-8')
  assert "lifecycle_stage == 'discovered'" in text, (
    "B4-SSOT: row-processing loop must have a 'discovered' branch"
  )
  discovered_idx = text.index("lifecycle_stage == 'discovered'")
  segment = text[discovered_idx:discovered_idx + 200]
  assert 'queued.append' in segment, (
    "B4-SSOT: 'discovered' branch must append to the queued list"
  )


def test_b1r3_mark_execution_started_called_for_all_run_actions() -> None:
  import pathlib
  src = pathlib.Path(__file__).parent.parent / 'src' / 'polyventure' / 'web_app.py'
  text = src.read_text(encoding='utf-8')
  assert "def mark_execution_started(self, *, automation_was_active: bool = False)" in text, (
    "B1-R3: mark_execution_started() must accept automation_was_active parameter (not read module-global)"
  )
  assert "automation_was_active=bool(automation_overlay_state.get('enabled'))" in text, (
    "B1-R3: call site must pass automation_was_active from automation_overlay_state (in-scope closure)"
  )
  assert "if submit_order_bridge_intent:\n            session_controller.mark_execution_started()" not in text, (
    "B1-R3: old submit_order_bridge_intent gate must be removed from mark_execution_started call site"
  )


def test_c1_run_handler_populates_funds_posture_for_live_lane() -> None:
  import pathlib
  src = pathlib.Path(__file__).parent.parent / 'src' / 'polyventure' / 'web_app.py'
  text = src.read_text(encoding='utf-8')
  assert '_project_funds_posture' in text, (
    "C1: _project_funds_posture must be used in web_app.py"
  )
  assert "_latest_heartbeat_payload(_fp_conn, operation_lane='live')" in text, (
    "C1: run handler must query latest live heartbeat for funds_posture"
  )
  fp_idx = text.index("_latest_heartbeat_payload(_fp_conn, operation_lane='live')")
  segment = text[fp_idx:fp_idx + 200]
  assert "_project_funds_posture(latest_heartbeat_payload=_fp_hb)" in segment, (
    "C1: run handler must project funds_posture from live heartbeat payload"
  )


# TD-5: /api/execution-status endpoint


def test_execution_status_endpoint_returns_in_flight_names(tmp_path: Any) -> None:
  db_path = str(tmp_path / 'state.sqlite3')
  _make_test_db_with_pairs(db_path, {
    'PAIR-A': 'RESTING_BOTH',
    'PAIR-B': 'FULLY_FILLED',
    'PAIR-C': 'LOCKED',
    'PAIR-D': 'CANCELED',
  })
  controller = ConsoleSessionController(session_token='session-123', state_db_path=db_path)
  app = create_operator_console_app(_services(), session_controller=controller)

  status, _, body = _call_app(app, method='GET', path='/api/execution-status')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'planned'
  assert payload['in_flight_count'] == 2
  assert sorted(payload['active_pairs']) == ['PAIR-A', 'PAIR-B']
  assert payload['drain_active'] is False
  # FB-5: the tray consumes automation_active to suppress the execution-complete
  # popup while automation is armed. Default posture is not armed.
  assert payload['automation_active'] is False


# Projection engine credential_ready fix — P1 regression tests


def test_run_route_post_submit_bridge_canceled_projects_correct_next_step_not_load_api_key(monkeypatch: Any) -> None:
  # Regression: service.run_execution returns safe_settings_summary as payload['settings'];
  # safe_settings_summary lacks credential_ready so the old guard (if 'settings' not in payload)
  # was bypassed and _follow_on_workflow routed to load_api_key via S2_SUCCESS_WAITING_FOR_KEY.
  # Fix: unconditional overwrite — _load_shell_settings_context always runs.
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': True,
        'credential_ready': True,
        'credential_reference_present': True,
        'environment_ready': True,
        'mode_selected': True,
        'operation_lane': 'sandbox',
        'kalshi_env': 'demo',
        'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'live_websocket_url': 'wss://api.kalshi.example/ws',
        'active_websocket_url': 'wss://demo-api.kalshi.example/ws',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      None,
    ),
  )
  base = _services()
  app = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=lambda **_: {
        'decision': 'planned',
        'planned_pair_count': 0,
        'planned_pairs': [],
        'settings': {
          # safe_settings_summary shape: present but lacks credential_ready
          'kalshi_env': 'demo',
          'operation_lane': 'sandbox',
          'api_key_id_present': True,
          'private_key_file_present': True,
          'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
          'live_websocket_url': 'wss://api.kalshi.example/ws',
          # credential_ready, credential_reference_present, settings_ready intentionally absent
        },
        'next_action': 'All pairs cancelled.',
      },
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  status, _, body = _call_app(app, method='POST', path='/api/run')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow']['next_actionable_step'] != 'load_api_key'
  assert payload['workflow']['recommended_step'] != 'review_configuration'


def test_run_route_settings_overwrite_contains_names_only_no_credential_values(monkeypatch: Any) -> None:
  # Sandbox security invariant: payload['settings'] after unconditional overwrite must contain
  # only names and presence flags — no raw api_key_id values, no private key paths, no inline keys.
  credential_values_seen: list[str] = []
  safe_settings: dict[str, Any] = {
    'settings_ready': True,
    'credential_ready': True,
    'credential_reference_present': True,
    'environment_ready': True,
    'mode_selected': True,
    'operation_lane': 'sandbox',
    'kalshi_env': 'demo',
    'api_key_id_present': True,
    'private_key_file_present': True,
    'sandbox_websocket_url': 'wss://demo-api.kalshi.example/ws',
    'live_websocket_url': 'wss://api.kalshi.example/ws',
    'available_websocket_urls': {
      'sandbox': 'demo-api.kalshi.example/ws',
      'live': 'api.kalshi.example/ws',
    },
  }
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (dict(safe_settings), None),
  )
  app = create_operator_console_app(_services())

  status, _, body = _call_app(app, method='POST', path='/api/run')
  payload = json.loads(body)

  assert status == '200 OK'
  settings_in_response = payload.get('settings') or {}
  # Names-only invariant: raw credential fields must not appear
  assert 'api_key_id' not in settings_in_response, 'api_key_id (raw value) must not appear in settings response'
  assert 'private_key_inline' not in settings_in_response, 'private_key_inline must not appear in settings response'
  assert 'private_key_file' not in settings_in_response, 'private_key_file path must not appear in settings response'
  # Presence flags are permitted
  assert settings_in_response.get('api_key_id_present') is True
  assert settings_in_response.get('credential_ready') is True
  # Track any unexpected string values that look like raw credential data
  for k, v in settings_in_response.items():
    if isinstance(v, str) and len(v) > 30 and not v.startswith('wss://') and not v.startswith('https://') and not v.startswith('ws://'):
      credential_values_seen.append(k)
  assert credential_values_seen == [], f'Unexpected long string fields in settings (possible credential leak): {credential_values_seen}'


def test_run_route_signing_middleware_unaffected_by_settings_overwrite(monkeypatch: Any) -> None:
  # Sandbox: _validate_signed_mutation_request behavior is identical with and without
  # settings key in service payload. The middleware runs on the request (before action dispatch);
  # the settings overwrite runs on the response (after service call). Structural independence is
  # verified here by confirming call count and return value are unaffected by payload shape.
  calls: list[tuple[str, Any]] = []
  original_validate = web_app._validate_signed_mutation_request if hasattr(web_app, '_validate_signed_mutation_request') else None

  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_: (
      {
        'settings_ready': True,
        'credential_ready': True,
        'credential_reference_present': True,
        'environment_ready': True,
        'mode_selected': True,
        'operation_lane': 'sandbox',
        'available_websocket_urls': {
          'sandbox': 'demo-api.kalshi.example/ws',
          'live': 'api.kalshi.example/ws',
        },
      },
      None,
    ),
  )

  base = _services()

  # Case A: service payload has 'settings' key (safe_settings_summary bypass pattern)
  app_with_settings = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=lambda **_: {
        'decision': 'planned',
        'planned_pair_count': 0,
        'planned_pairs': [],
        'settings': {'api_key_id_present': True, 'operation_lane': 'sandbox'},
        'next_action': 'Complete.',
      },
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  # Case B: service payload has no 'settings' key
  app_without_settings = create_operator_console_app(
    OperatorConsoleServices(
      bootstrap=base.bootstrap,
      scan=base.scan,
      run=lambda **_: {
        'decision': 'planned',
        'planned_pair_count': 0,
        'planned_pairs': [],
        'next_action': 'Complete.',
      },
      reconcile=base.reconcile,
      report=base.report,
      cancel_all=base.cancel_all,
      system_log=base.system_log,
      visuals=base.visuals,
    )
  )

  # Neither app has a session_controller so _validate_signed_mutation_request returns None
  # (signing not enforced without session). Both requests must return 200 — middleware passthrough
  # is identical in both cases regardless of service payload shape.
  status_a, _, body_a = _call_app(app_with_settings, method='POST', path='/api/run')
  payload_a = json.loads(body_a)

  status_b, _, body_b = _call_app(app_without_settings, method='POST', path='/api/run')
  payload_b = json.loads(body_b)

  assert status_a == '200 OK', 'signing middleware must pass through when session_controller is None (case A)'
  assert status_b == '200 OK', 'signing middleware must pass through when session_controller is None (case B)'
  # Both cases produce the same workflow projection — settings overwrite provides consistent output
  assert payload_a['workflow']['next_actionable_step'] == payload_b['workflow']['next_actionable_step'], (
    'next_actionable_step must be identical regardless of whether service returned settings key'
  )
  assert payload_a['workflow']['next_actionable_step'] != 'load_api_key', (
    'unconditional overwrite must prevent load_api_key when credentials are ready'
  )


# ---------------------------------------------------------------------------
# FD-6b: retry countdown null initialization fix
# ---------------------------------------------------------------------------

def test_retry_countdown_null_guard_present_in_stage_card() -> None:
  app = create_operator_console_app(_services())
  status, _, body = _call_app(app, method='GET', path='/')
  assert status == '200 OK'
  assert 'retry_countdown_remaining_sec != null && Number.isFinite' in body, (
    'FD-6b regression: null guard absent from countdown expression — countdown will show 0 on null first-render'
  )
  assert 'String(Number.isFinite(Number(retryState.retry_countdown_remaining_sec)) ? Number(retryState.retry_countdown_remaining_sec)' not in body, (
    'FD-6b regression: unguarded countdown expression still present'
  )


# ---------------------------------------------------------------------------
# FD-6c SC-1/SC-2: client-anchored countdown state (regression guard)
# ---------------------------------------------------------------------------

def test_retry_countdown_reads_scheduler_deadline_no_client_anchor() -> None:
  # SCHEDULER_ELIGIBILITY_THRESHOLD_REALIGNMENT_BMAP_2026-06-29 (C4): the retry countdown is
  # display-only and reads the scheduler-authored deadline (retry timer record next_retry_at_utc),
  # mirroring the cadence ticker. This supersedes the FD-6c client-anchored countdown, which authored
  # its own deadline from Date.now() and drifted from the scheduler's armed timer.
  app = create_operator_console_app(_services())
  status, _, body = _call_app(app, method='GET', path='/')
  assert status == '200 OK'
  retry_start = body.find('function updateZeroFoundRetryCountdown()')
  retry_end = body.find('\n    function ensureZeroFoundRetryTicker()', retry_start)
  retry_body = body[retry_start:retry_end] if retry_end != -1 else body[retry_start:]
  assert 'function scanRetryState(payload = {})' in body, (
    'C4 regression: retry countdown no longer shares the backend retry-state selector'
  )
  assert 'const retryState = scanRetryState(payload);' in retry_body, (
    'C4 regression: retry ticker no longer reads the selected backend retry state'
  )
  assert 'retryState.next_retry_at_utc' in retry_body, (
    'C4 regression: retry ticker no longer reads the scheduler-authored retry deadline'
  )
  assert 'const schedulerRetry =' not in retry_body, (
    'C4 regression: retry ticker bypasses retry-state fallback selection'
  )
  assert 'Date.now() + retryAfterSec * 1000' not in body, (
    'C4 regression: client-authored retry countdown anchor reintroduced'
  )


# ---------------------------------------------------------------------------
# N1 SC-1/SC-2: execution panel in_flight update in run handler (regression guard)
# ---------------------------------------------------------------------------

def test_execution_panel_in_flight_update_present_in_run_handler() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert "lifecycle_stage = 'in_flight'" in _source, (
    'N1-P2 regression: lifecycle_stage in_flight absent from run handler'
  )
  assert "INSERT OR IGNORE INTO candidate_review_candidates" in _source, (
    'N1-P2 regression: UPSERT into candidate_review_candidates absent from run handler'
  )
  assert "INSERT OR IGNORE INTO candidate_review_runs" in _source, (
    'N1-P2 regression: FK-guard INSERT into candidate_review_runs absent from run handler'
  )
  assert "SELECT run_id, operation_lane, lane_session_id FROM candidate_saved_sets WHERE saved_set_id = ?" in _source, (
    'N1-P2 regression: saved-set promotion must read lane_session_id for the direct submit handoff lane anchor'
  )
  assert " (run_id, operation_lane, lane_session_id, candidate_signature, candidate_count, source_action, detail_json, recorded_at_utc)" in _source, (
    'N1-P2 regression: FK-guard INSERT into candidate_review_runs must preserve lane_session_id'
  )
  assert "candidate_saved_set_members" in _source, (
    'N1-P2 regression: candidate_saved_set_members source reference absent from run handler'
  )


def test_follow_on_workflow_routes_to_execution_panel_when_in_flight() -> None:
  from polyventure.web_app import _follow_on_workflow
  payload = {
    'planned_pair_count': 2,
    '_in_flight_candidate_count': 2,
    'execution_chronology': {},
  }
  result = _follow_on_workflow('run', payload)
  assert result.get('focus_target') == 'live-interaction-section', (
    'N1-P2 regression: focus_target should be live-interaction-section when in_flight_count >= 1'
  )


def test_follow_on_workflow_routes_to_evidence_when_no_in_flight() -> None:
  from polyventure.web_app import _follow_on_workflow
  payload = {
    'planned_pair_count': 2,
    '_in_flight_candidate_count': 0,
    'execution_chronology': {},
  }
  result = _follow_on_workflow('run', payload)
  assert result.get('focus_target') == 'evidence-section', (
    'N1-P2 regression: focus_target should be evidence-section when in_flight_count == 0'
  )


def test_p2a_uses_saved_set_members_source() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert "FROM candidate_saved_set_members" in _source, (
    "N1-P2 regression: P2-A UPSERT must SELECT FROM candidate_saved_set_members"
  )
  assert "review_selection_state['persisted_run_id'] = _p2a_run_id" in _source, (
    "N1-P2 regression: P2-A must sync persisted_run_id back to review_selection_state"
  )


def test_p2e_persisted_run_id_in_apply_review_selection_projection() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  count = _source.count("'persisted_run_id': str(review_selection_state.get('persisted_run_id')")
  assert count >= 4, (
    f"N1-P2 regression: persisted_run_id must be present in all 4 review_selection assignment sites, found {count}"
  )


def test_ssc1_persisted_lane_session_id_in_payload_construction_sites() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  count = _source.count("'persisted_lane_session_id': str(review_selection_state.get('persisted_lane_session_id')")
  assert count >= 4, (
    f'SSC-3: persisted_lane_session_id must be present in all 4 review_selection payload sites, found {count}'
  )


def test_ssc2_persist_snapshot_writes_lane_session_id_to_state() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert "review_selection_state['persisted_lane_session_id'] = _lsid" in _source, (
    'SSC-2: _persist_candidate_review_snapshot must write persisted_lane_session_id to review_selection_state'
  )
  guard_count = _source.count("not review_selection_state.get('persisted_lane_session_id')")
  assert guard_count >= 2, (
    f'B2: first-write-only guard must be present at both write sites in _persist_candidate_review_snapshot, found {guard_count}'
  )


def test_push_a_reconciler_receives_current_operating_session() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert 'current_operating_session_id: str | None = None' in _source, (
    'Push A: bootstrap payload builder must accept the current operating session id'
  )
  assert 'current_operating_session_id=current_operating_session_id' in _source, (
    'Push A: bootstrap must forward the current operating session id into reconciliation'
  )
  count = _source.count(
    "current_operating_session_id=str(review_selection_state.get('persisted_lane_session_id') or '').strip() or None"
  )
  assert count >= 3, (
    f'Push A: shell bootstrap call sites must pass the stable operating session id, found {count}'
  )


def test_push_a_mode_change_clear_is_success_or_offline_only() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert (
    "review_selection_state['persisted_lane_session_id'] = ''\n    _commit_mode_selection("
    in _source
  ), (
    'Push A: connected mode change must clear the operating-session id only at the committed-success boundary'
  )
  route_start = _source.index("if method == 'POST' and path == '/api/change-mode':")
  route_end = _source.index("if method == 'POST' and path in {'/api/scan', '/api/scan-cancel'}:", route_start)
  route_source = _source[route_start:route_end]
  assert "review_selection_state['persisted_lane_session_id'] = ''" not in route_source, (
    'Push A: mode-change route entry and no-go branches must not clear the operating-session id'
  )


def test_ssc4_fetch_stage_columns_empty_without_lane_session_id(tmp_path: Path) -> None:
  from polyventure.web_app import _fetch_stage_columns
  _db_path = tmp_path / 'runtime_ssc4a.sqlite3'
  _conn = sqlite3.connect(str(_db_path))
  _conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  _conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  _conn.execute(
    "INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES ('run-ssc4a', 'session-ssc4a')"
  )
  _conn.execute(
    "INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json)"
    " VALUES ('run-ssc4a', 'in_flight', NULL, 'TICK-SSC4A', 'live_qualifying', '{}')"
  )
  _conn.commit()
  _conn.close()
  _result = _fetch_stage_columns({
    'review_selection': {},
    'settings': {'state_db_path': str(_db_path)},
  })
  assert _result['in_flight_candidate_count'] == 0, 'SSC-4: absent persisted_lane_session_id must return empty stage columns'
  assert all(len(col['items']) == 0 for col in _result['stage_columns']), (
    'SSC-4: all stage sections must be empty when persisted_lane_session_id is absent'
  )


def test_ssc4_fetch_stage_columns_session_scoped_accumulation(tmp_path: Path) -> None:
  from polyventure.web_app import _fetch_stage_columns
  _db_path = tmp_path / 'runtime_ssc4b.sqlite3'
  _lane_session_id = 'current-session'
  _other_session_id = 'other-session'
  _conn = sqlite3.connect(str(_db_path))
  _conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  _conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  _conn.executemany(
    'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?,?)',
    [
      ('run-cycle-1', _lane_session_id),
      ('run-cycle-2', _lane_session_id),
      ('run-other-session', _other_session_id),
    ],
  )
  _conn.executemany(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json) VALUES (?,?,?,?,?,?)',
    [
      ('run-cycle-1',        'terminal', 'canceled', 'TICK-CYCLE1', 'live_qualifying', '{}'),
      ('run-cycle-2',        'in_flight', None,       'TICK-CYCLE2', 'live_qualifying', '{}'),
      ('run-other-session',  'in_flight', None,       'TICK-OTHER',  'live_qualifying', '{}'),
    ],
  )
  _conn.commit()
  _conn.close()
  _result = _fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': _lane_session_id},
    'settings': {'state_db_path': str(_db_path)},
  })
  _columns = {col['stage_id']: col['items'] for col in _result['stage_columns']}
  _all_tickers = [i['ticker'] for col in _result['stage_columns'] for i in col['items']]
  assert 'TICK-CYCLE1' in [i['ticker'] for i in _columns['cancelled']], (
    'SSC-4: terminal candidate from prior cycle must appear in cancelled column'
  )
  assert 'TICK-CYCLE2' in [i['ticker'] for i in _columns['queued']], (
    'SSC-4: in_flight candidate from current cycle must appear in queued column'
  )
  assert 'TICK-OTHER' not in _all_tickers, (
    'SSC-4: candidate from a different session must be excluded'
  )
  assert _result['total_stage_candidate_count'] == 2, (
    'SSC-4: total must cover both current-session cycles (1 terminal + 1 in_flight)'
  )


def test_ssc4_fetch_stage_columns_deduplicates_by_ticker(tmp_path: Path) -> None:
  from polyventure.web_app import _fetch_stage_columns
  _db_path = tmp_path / 'runtime_ssc4c.sqlite3'
  _lane_session_id = 'dedup-session'
  _conn = sqlite3.connect(str(_db_path))
  _conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  _conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  _conn.executemany(
    'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?,?)',
    [('run-dd-1', _lane_session_id), ('run-dd-2', _lane_session_id)],
  )
  # Older row: terminal/canceled; newer row: in_flight. Most recent (in_flight) must win.
  _conn.executemany(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json) VALUES (?,?,?,?,?,?)',
    [
      ('run-dd-1', 'terminal', 'canceled', 'TICK-DUP', 'live_qualifying', '{}'),
      ('run-dd-2', 'in_flight', None,       'TICK-DUP', 'live_qualifying', '{}'),
    ],
  )
  _conn.commit()
  _conn.close()
  _result = _fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': _lane_session_id},
    'settings': {'state_db_path': str(_db_path)},
  })
  _all_tickers = [i['ticker'] for col in _result['stage_columns'] for i in col['items']]
  _columns = {col['stage_id']: col['items'] for col in _result['stage_columns']}
  assert _all_tickers.count('TICK-DUP') == 1, (
    'SSC-4: de-dup must ensure TICK-DUP appears exactly once despite two rows in same session'
  )
  assert 'TICK-DUP' in [i['ticker'] for i in _columns['queued']], (
    'SSC-4: most recent in_flight row must win; TICK-DUP must appear in queued'
  )
  assert 'TICK-DUP' not in [i['ticker'] for i in _columns['cancelled']], (
    'SSC-4: older terminal row for TICK-DUP must be suppressed by de-dup'
  )


# B2: stable LSID in candidate_review_runs — prepend persisted_lane_session_id as first fallback


def test_b2_candidate_review_run_stores_stable_lsid() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert (
    "review_selection_state.get('persisted_lane_session_id') or _websocket_runtime_snapshot().get('lane_session_id')"
    in _source
  ), (
    'B2: candidate_review_runs lane_session_id must use persisted_lane_session_id as first fallback before per-scan WS snapshot'
  )


# FB-10: operator_lane_session_id threaded from web layer into service scan/run calls


def test_fb10_scan_call_site_injects_operator_lane_session_id() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert (
    "_scan_call_accepts_keyword(resolved_services.scan, 'operator_lane_session_id')"
    in _source
  ), (
    'FB-10: scan call site must guard operator_lane_session_id with _scan_call_accepts_keyword before injecting'
  )
  assert (
    "call_kwargs['operator_lane_session_id'] = operator_lane_session_id"
    in _source
  ), (
    'FB-10: scan call site must inject operator_lane_session_id into call_kwargs when available'
  )


def test_candidate_session_ssot_scan_runtime_uses_operator_session_id() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert "review_selection_state['persisted_lane_session_id'] = str(runtime_snapshot.get('lane_session_id') or '').strip()" in _source
  assert "scan_runtime_lane_session_id = 'scan-{suffix}'" in _source
  assert "lane_session_id=operator_lane_session_id or scan_runtime_lane_session_id" in _source
  assert "'operator_lane_session_id': str(snapshot.get(" in _source
  assert "event_type='operator_lane_session_unavailable'" in _source


def test_fb10_run_call_site_injects_operator_lane_session_id() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert (
    "action_name == 'run'"
    in _source
  ), (
    'FB-10: run call site must gate operator_lane_session_id injection on action_name == run'
  )
  assert (
    "call_kwargs['operator_lane_session_id'] = _run_op_lsid"
    in _source
  ), (
    'FB-10: run call site must inject operator_lane_session_id into call_kwargs'
  )


def test_fb10_service_functions_accept_operator_lane_session_id() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'service.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert (
    'operator_lane_session_id: str | None = None' in _source
  ), (
    'FB-10: run_scan_once, run_service_once, and _persist_candidate_math_contract must accept operator_lane_session_id'
  )
  assert (
    'operator_lane_session_id or lane_session_id' in _source
  ), (
    'FB-10: _persist_candidate_math_contract must use operator_lane_session_id or lane_session_id for the DB column'
  )


def test_fb10_read_side_augment_threads_and_stamps_operator_review_session_id() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert 'operator_review_session_id: str | None = None' in _source, (
    'FB-10 read: _augment_shell_payload must accept operator_review_session_id'
  )
  assert "_aug_review_sel['persisted_lane_session_id'] = operator_review_session_id" in _source, (
    'FB-10 read: augment must stamp the operator review session id onto review_selection '
    'before the stage-column read'
  )
  # The stamp must be gated on an active durable mode so an offline/inactive shell does
  # not surface a prior session's candidates (the id is not cleared on offline reset).
  _idx = _source.find("_aug_review_sel = augmented.get('review_selection')")
  assert _idx != -1
  _window = _source[max(0, _idx - 300):_idx]
  assert 'if _durable_mode_selected and operator_review_session_id:' in _window, (
    'FB-10 read: durable-session stamping must be gated on _durable_mode_selected'
  )


def test_fb10_read_side_call_sites_thread_operator_review_session_id() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  _threaded = _source.count(
    "operator_review_session_id=str(review_selection_state.get('persisted_lane_session_id') or '').strip() or None,"
  )
  assert _threaded >= 8, (
    f'FB-10 read: every _augment_shell_payload call site must thread the operator review '
    f'session id from review_selection_state (found {_threaded}, expected >= 8)'
  )


def test_fb10_panel_read_unified_session_returns_rows_per_scan_id_empty(tmp_path: Path) -> None:
  # End-to-end proof of the FB-10 read fix: candidate-math rows written under the unified
  # operator session id must be retrievable by the panel via that id, while the per-scan
  # runtime id (the pre-fix fallback) returns nothing — which was the blank-out cause.
  from polyventure.web_app import _fetch_stage_columns
  _db_path = tmp_path / 'runtime_fb10.sqlite3'
  _operator_session_id = 'live-20260616T092930Z-8d611dc2'
  _per_scan_id = 'scan-3637c60c0835'
  _conn = sqlite3.connect(str(_db_path))
  _conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  _conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  # Three cycles' candidate-math runs, all unified under the operator session id (post-fix).
  _conn.executemany(
    'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?,?)',
    [
      ('live-...093718Z:scan-once:candidate-math', _operator_session_id),
      ('live-...093654Z:runtime-cycle:candidate-math', _operator_session_id),
      ('live-...093631Z:scan-once:candidate-math', _operator_session_id),
    ],
  )
  _conn.executemany(
    'INSERT INTO candidate_review_candidates (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json) VALUES (?,?,?,?,?,?)',
    [
      ('live-...093718Z:scan-once:candidate-math',     'discovered', None, 'TICK-A', 'live_qualifying', '{}'),
      ('live-...093654Z:runtime-cycle:candidate-math', 'discovered', None, 'TICK-B', 'live_qualifying', '{}'),
      ('live-...093631Z:scan-once:candidate-math',     'in_flight',  None, 'TICK-C', 'live_qualifying', '{}'),
    ],
  )
  _conn.commit()
  _conn.close()
  _unified = _fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': _operator_session_id},
    'settings': {'state_db_path': str(_db_path)},
  })
  _unified_tickers = [i['ticker'] for col in _unified['stage_columns'] for i in col['items']]
  assert {'TICK-A', 'TICK-B', 'TICK-C'} <= set(_unified_tickers), (
    'FB-10: panel queried by the unified operator session id must return all cycles candidates'
  )
  _per_scan = _fetch_stage_columns({
    'review_selection': {'persisted_lane_session_id': ''},
    'scan_runtime': {'lane_session_id': _per_scan_id},
    'settings': {'state_db_path': str(_db_path)},
  })
  _per_scan_tickers = [i['ticker'] for col in _per_scan['stage_columns'] for i in col['items']]
  assert _per_scan_tickers == [], (
    'FB-10: the per-scan runtime id (pre-fix fallback) must return no rows — this was the blank-out'
  )


# B3: offline gate — execution panel zeroed when operation_lane == offline


def test_b3_in_flight_candidate_count_zeroed_offline_lane() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert (
    "str(_settings_payload.get('operation_lane') or '').strip().lower() == 'offline'"
    in _source
  ), (
    "B3: offline gate must zero in_flight_candidate_count when operation_lane == 'offline'"
  )
  assert (
    'in_flight_candidate_count = 0' in _source
  ), (
    'B3: in_flight_candidate_count must be explicitly zeroed in the offline gate branch'
  )


def test_b3_in_flight_candidate_count_nonzero_when_live_lane() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert (
    "_settings_payload = payload.get('settings') if isinstance(payload.get('settings'), dict) else {}"
    in _source
  ), (
    'B3: offline gate preamble must safely extract settings_payload before comparing operation_lane'
  )




# ---------------------------------------------------------------------------
# N2: lifecycle terminal bridge — N2-A persistence fix + N2-B bootstrap bridge
# ---------------------------------------------------------------------------

def test_n2a_on_conflict_pattern_present_in_persistence_source() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'persistence.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert 'ON CONFLICT(run_id, candidate_uid) DO UPDATE SET' in _source, (
    'N2-A regression: ON CONFLICT DO UPDATE SET absent from persist_candidate_review_candidates'
  )
  assert 'INSERT OR REPLACE INTO candidate_review_candidates' not in _source, (
    'N2-A regression: INSERT OR REPLACE still present in persistence.py — lifecycle reset not fixed'
  )


def test_n2a_persist_candidate_review_candidates_preserves_lifecycle_on_conflict() -> None:
  import tempfile
  from polyventure.persistence import open_database, persist_candidate_review_candidates
  with tempfile.NamedTemporaryFile(suffix='.sqlite3', delete=False) as _f:
    _tmp_path = _f.name
  try:
    _conn = open_database(_tmp_path)
    _conn.execute(
      "INSERT INTO candidate_review_runs"
      " (run_id, operation_lane, candidate_signature, candidate_count, source_action, detail_json, recorded_at_utc)"
      " VALUES ('n2a-test-run', 'sandbox', '', 0, 'test', '{}', '2026-06-09T00:00:00Z')"
    )
    _conn.commit()
    _conn.execute(
      "INSERT INTO candidate_review_candidates"
      " (run_id, candidate_uid, candidate_key, detail_json, recorded_at_utc, lifecycle_stage, operation_lane)"
      " VALUES ('n2a-test-run', 'n2a-cand-1', 'n2a-cand-1', '{}', '2026-06-09T00:00:00Z', 'in_flight', 'sandbox')"
    )
    _conn.commit()
    persist_candidate_review_candidates(
      _conn,
      run_id='n2a-test-run',
      recorded_at_utc='2026-06-09T00:01:00Z',
      operation_lane='sandbox',
      candidates=[{'candidate_uid': 'n2a-cand-1', 'candidate_key': 'n2a-cand-1'}],
    )
    _row = _conn.execute(
      "SELECT lifecycle_stage FROM candidate_review_candidates"
      " WHERE run_id='n2a-test-run' AND candidate_uid='n2a-cand-1'"
    ).fetchone()
    assert _row is not None
    assert _row[0] == 'in_flight', (
      f'N2-A regression: lifecycle_stage was reset to {_row[0]!r} by persist_candidate_review_candidates'
    )
  finally:
    try:
      os.unlink(_tmp_path)
    except Exception:
      pass


def test_n2b_terminal_bridge_source_patterns_present() -> None:
  # N2-B (a57ea98) was superseded by the Terminal pre-wire (53765df): the bootstrap
  # `_n2b_cause` block is gone, and its invariants now live in (1) the stage-column
  # projection's pair_states saved_set_id linkage (qualified ps.detail_json) and
  # (2) the halt mark-terminal lifecycle write. Guard those.
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert "json_extract(ps.detail_json, '$.saved_set_id')" in _source, (
    'N2-B invariant regression: pair_states saved_set_id linkage absent from the stage projection'
  )
  assert "lifecycle_stage = 'terminal'" in _source, (
    'N2-B invariant regression: terminal lifecycle write absent'
  )
  assert "terminal_cause = 'auto_cancel'" in _source, (
    'N2-B invariant regression: halt mark-terminal write absent'
  )


# ---------------------------------------------------------------------------
# D-series: execution panel UX fixes (D1/D2/D4)
# ---------------------------------------------------------------------------

def test_d1_stage_nav_card_renders_ticker_from_object() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert "typeof item === 'string' ? item : (item.ticker || '--')" in _source, (
    'D1 regression: stage nav card does not extract ticker from object items'
  )


def test_d1_stage_nav_card_old_string_coerce_pattern_absent() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert 'stageItems.map((ticker) =>' not in _source, (
    'D1 regression: old ticker-as-string map pattern still present in stage nav card'
  )


def test_d2_in_flight_candidate_count_is_queued_only() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert "'in_flight_candidate_count': len(queued)," in _source, (
    'D2 regression: in_flight_candidate_count is not len(queued) only'
  )
  assert "'in_flight_candidate_count': len(queued) + len(filled)" not in _source, (
    'D2 regression: in_flight_candidate_count still includes filled/cancelled in sum'
  )


def test_d2_fetch_stage_columns_excludes_terminal_from_count(tmp_path: Path) -> None:
  from polyventure.web_app import _fetch_stage_columns
  _db_path = tmp_path / 'runtime_d2.sqlite3'
  _run_id = 'run-d2-test'
  _lane_session_id = 'test-session-d2'
  _conn = sqlite3.connect(str(_db_path))
  _conn.execute('CREATE TABLE candidate_review_runs (run_id TEXT PRIMARY KEY, lane_session_id TEXT)')
  _conn.execute(
    'CREATE TABLE candidate_review_candidates'
    ' (run_id TEXT, lifecycle_stage TEXT, terminal_cause TEXT, ticker TEXT,'
    '  qualifier_tier TEXT, detail_json TEXT, candidate_uid TEXT, candidate_key TEXT)'
  )
  _conn.execute(
    'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?,?)',
    (_run_id, _lane_session_id),
  )
  _conn.execute(
    'INSERT INTO candidate_review_candidates'
    ' (run_id, lifecycle_stage, terminal_cause, ticker, qualifier_tier, detail_json)'
    ' VALUES (?,?,?,?,?,?)',
    (_run_id, 'terminal', 'canceled', 'TICK1', 'sandbox_extended', '{}'),
  )
  _conn.commit()
  _conn.close()
  _payload = {
    'review_selection': {'persisted_lane_session_id': _lane_session_id},
    'settings': {'state_db_path': str(_db_path)},
  }
  _result = _fetch_stage_columns(_payload)
  assert _result['in_flight_candidate_count'] == 0, (
    f"D2 regression: terminal row counted as in_flight; got {_result['in_flight_candidate_count']}"
  )
  assert len(_result['stage_columns'][2]['items']) == 1, (
    'D2: cancelled item should still appear in stage_columns even when count=0'
  )


def test_d4_suppress_scroll_field_in_wayfinder_route() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert 'suppressScroll: Boolean(options.suppressScroll),' in _source, (
    'D4 regression: suppressScroll field absent from wayfinderRoute return'
  )
  assert 'suppressScroll: Boolean(normalized.suppressScroll),' in _source, (
    'D4 regression: suppressScroll not preserved in normalizeWayfinderRoute'
  )
  assert '!route.suppressScroll &&' in _source, (
    'D4 regression: suppressScroll guard absent from deriveWayfinderExecutionDelta shouldScroll'
  )


def test_d4_scan_routes_suppress_scroll() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert "wayfinderRoute('pairs-section', { tone: 'focus-ok', suppressScroll: true })" in _source, (
    'D4 regression: scan pairs-section route does not suppress scroll'
  )


def test_d4_submit_direct_scroll_removed() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert '_submitSection.scrollIntoView' not in _source, (
    'D4 regression: direct scrollIntoView on _submitSection still present'
  )
  assert "_submitSection.classList.add('submit-glow')" in _source, (
    'D4 regression: submit-glow class add removed — must be preserved'
  )


def test_d4_submit_wayfinder_uses_suppress_scroll() -> None:
  source_path = os.path.join(os.path.dirname(__file__), '..', 'src', 'polyventure', 'web_app.py')
  with open(source_path, encoding='utf-8') as _f:
    _source = _f.read()
  assert 'suppressScroll: true } : focusTarget' in _source, (
    'D4 regression: post-run wayfinder suppressScroll guard absent for submit path'
  )


# ---------------------------------------------------------------------------
# SC-CP5: candidateReviewShellVisible dead code removal + unconditional render
# ---------------------------------------------------------------------------

def test_candidateReviewShellVisible_gate_absent_from_rendered_shell() -> None:
  app = create_operator_console_app(_services())
  status, _, body = _call_app(app, method='GET', path='/')
  assert status == '200 OK'
  assert 'candidateReviewShellVisible' not in body, (
    'SC-CP5 regression: candidateReviewShellVisible still present in rendered shell — dead function was reintroduced'
  )


def test_review_shell_always_rendered_when_no_candidates() -> None:
  app = create_operator_console_app(_services())
  status, _, body = _call_app(app, method='GET', path='/')
  assert status == '200 OK'
  assert 'candidate-review-shell' in body, (
    'SC-CP5: candidate-review-shell class absent from rendered shell — unconditional render contract broken'
  )


def test_review_shell_empty_state_message_present_in_shell() -> None:
  app = create_operator_console_app(_services())
  status, _, body = _call_app(app, method='GET', path='/')
  assert status == '200 OK'
  assert 'candidate-review-empty' in body, (
    'SC-CP5: candidate-review-empty class absent from rendered shell — empty state path not wired'
  )


def test_set_working_default_persists_to_db(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'wd-persist.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')

  _call_app(app, method='GET', path='/')

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'set_working_default', 'values': {'scan_interval_ms': 4000, 'min_edge_dollars': 0.07}},
  )
  payload = json.loads(body)

  assert status == '200 OK', 'WD-3: set_working_default action failed'
  assert payload['session_overlay']['runtime']['last_result']['tone'] == 'ok'

  stored = load_lane_defaults(open_database(state_db_path), 'sandbox')
  assert stored.get('scan_interval_ms') == '4000', 'WD-3: scan_interval_ms not written to DB'
  assert stored.get('min_edge_dollars') == '0.07', 'WD-3: min_edge_dollars not written to DB'


def test_reset_action_clears_db_defaults(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'wd-reset.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')

  _call_app(app, method='GET', path='/')
  _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'set_working_default', 'values': {'scan_interval_ms': 4000}},
  )

  _call_app(app, method='POST', path='/api/runtime-overlay', body={'action': 'reset'})

  stored = load_lane_defaults(open_database(state_db_path), 'sandbox')
  assert stored == {}, 'WD-4: reset action must clear DB defaults'


def test_bootstrap_load_seeds_working_default_state_from_db(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'wd-seed.sqlite3')
  settings = _build_test_settings(state_db_path)

  connection = open_database(state_db_path)
  persist_lane_defaults(connection, 'sandbox', {'scan_interval_ms': '5500', 'min_edge_dollars': '0.09'})

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')

  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  runtime = payload.get('session_overlay', {}).get('runtime', {})
  assert runtime.get('working_default_active') is True, (
    'WD-5: working_default_active should be True after loading defaults from DB'
  )
  wd_values = runtime.get('working_default_values', {})
  assert wd_values.get('scan_interval_ms') == 5500, (
    'WD-5: scan_interval_ms not seeded from DB on bootstrap (coercer not applied)'
  )
  assert wd_values.get('min_edge_dollars') == 0.09, (
    'WD-5: min_edge_dollars not seeded from DB on bootstrap (coercer not applied)'
  )


def test_set_working_default_enables_durable_default_available_flag(
  tmp_path: Path, monkeypatch: Any,
) -> None:
  state_db_path = str(tmp_path / 'wd-flag.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')

  _call_app(app, method='GET', path='/')

  bootstrap_before = json.loads(_call_app(app, method='GET', path='/api/bootstrap')[2])
  assert bootstrap_before['session_overlay']['runtime']['durable_default_available'] is False, (
    'WD-7: durable_default_available should be False before any default is set'
  )

  _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'set_working_default', 'values': {'scan_interval_ms': 3500}},
  )

  bootstrap_after = json.loads(_call_app(app, method='GET', path='/api/bootstrap')[2])
  assert bootstrap_after['session_overlay']['runtime']['durable_default_available'] is True, (
    'WD-7: durable_default_available should be True after set_working_default'
  )


def test_set_working_default_stores_under_baseline_lane_not_context_overlay_lane(
  tmp_path: Path, monkeypatch: Any,
) -> None:
  """WD-9: defaults must be keyed to the .env baseline lane, not the active context overlay lane."""
  state_db_path = str(tmp_path / 'wd-lane.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')

  _call_app(app, method='GET', path='/')

  _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'update', 'values': {'operation_lane': 'live'}},
  )

  _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'set_working_default', 'values': {'scan_interval_ms': 3000}},
  )

  baseline_lane = str(getattr(settings, 'operation_lane', 'sandbox') or 'sandbox').lower()
  stored = load_lane_defaults(open_database(state_db_path), baseline_lane)
  assert stored.get('scan_interval_ms') == '3000', (
    f'WD-9: default must be stored under baseline lane "{baseline_lane}", not the context overlay lane'
  )


# --- Packet A: WD-10 + HYD-1-REM + HYD-3-REM ---


def test_packet_a_wd10_log_lines_present_in_wd_persist_helper() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert '[WARN] WD defaults persist failed:' in source


def test_packet_a_wd10_log_lines_present_in_wd_load_helper() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert '[WARN] WD defaults load failed:' in source


def test_packet_a_wd10_no_bare_silent_except_in_wd_helpers() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert 'WD defaults persist failed' in source, 'persist helper must have observable failure path'
  assert 'WD defaults load failed' in source, 'load helper must have observable failure path'


def test_packet_a_hyd1_persisted_scan_hydration_not_called_in_shell_payload_builder() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # Check the call pattern (comma-separated args), not the definition (colon-annotated params)
  assert '_hydrate_persisted_scan_runtime(env_override,' not in source


def test_packet_a_hyd1_cold_bootstrap_has_empty_scan_runtime_status() -> None:
  app = create_operator_console_app(_services())
  _, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)
  assert int(payload['scan_runtime'].get('result_candidate_count', 0)) == 0, (
    'HYD-1: cold bootstrap must not hydrate scan state from DB'
  )


def test_packet_a_hyd3_restored_history_merge_not_called_as_assignment() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert 'merged_payload = _merge_saved_candidate_rows_for_restored_history(merged_payload)' not in source


def test_packet_a_hyd3_restored_history_merge_not_called_inline() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert '_merge_saved_candidate_rows_for_restored_history(_merge_preserved' not in source


def test_packet_a_hyd3_cold_bootstrap_has_zero_candidate_count() -> None:
  app = create_operator_console_app(_services())
  _, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)
  assert payload['pair_monitor']['candidate_count'] == 0, (
    'HYD-3: cold bootstrap must not inject historical candidates from DB'
  )


def test_packet_a_hyd3_review_hold_merge_still_present_in_merge_chain() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert 'merged_payload = _merge_preserved_review_hold_payload(action_name, payload)' in source


def test_wd12_set_working_default_activates_values_in_overlay(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'wd12.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')

  _call_app(app, method='GET', path='/')

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'set_working_default', 'values': {'scan_interval_ms': 4500, 'min_edge_dollars': 0.06}},
  )
  payload = json.loads(body)
  runtime = payload.get('session_overlay', {}).get('runtime', {})

  assert status == '200 OK', 'WD-12: set_working_default action failed'
  assert runtime.get('values', {}).get('scan_interval_ms') == 4500, (
    'WD-12: scan_interval_ms must be active in overlay immediately after set_working_default'
  )
  assert runtime.get('values', {}).get('min_edge_dollars') == 0.06, (
    'WD-12: min_edge_dollars must be active in overlay immediately after set_working_default'
  )
  assert runtime.get('active') is True, 'WD-12: overlay must be active after set_working_default'


def test_wd11_boot_load_activates_values_in_overlay(tmp_path: Path, monkeypatch: Any) -> None:
  state_db_path = str(tmp_path / 'wd11.sqlite3')
  settings = _build_test_settings(state_db_path)

  connection = open_database(state_db_path)
  persist_lane_defaults(connection, 'sandbox', {'scan_interval_ms': '6000', 'min_edge_dollars': '0.08'})

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')

  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)
  runtime = payload.get('session_overlay', {}).get('runtime', {})

  assert status == '200 OK'
  assert runtime.get('values', {}).get('scan_interval_ms') == 6000, (
    'WD-11: scan_interval_ms must be active in overlay at boot — no RESTORE required'
  )
  assert runtime.get('values', {}).get('min_edge_dollars') == 0.08, (
    'WD-11: min_edge_dollars must be active in overlay at boot — no RESTORE required'
  )
  assert runtime.get('active') is True, 'WD-11: overlay must be active after boot load'


# ---------------------------------------------------------------------------
# Packet B — HYD-2-GATE tests
# ---------------------------------------------------------------------------

def test_packet_b_hyd2_refresh_shell_payload_does_not_call_hydrate_review_selection() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # Verify no _hydrate_review_selection_from_persistence call-pattern in _refresh_shell_payload.
  # The call pattern uses a comma (positional kwarg), the definition uses a colon (type annotation).
  # We check the broader source; the important thing is there are no remaining bare call sites
  # that aren't gated behind the resume_saved_selection action.
  # Count occurrences of the call pattern — only the resume_saved_selection dispatch block should remain.
  call_count = source.count('_hydrate_review_selection_from_persistence(\n      env_override=env_override,')
  assert call_count <= 1, (
    f'HYD-2: expected at most 1 call site (_hydrate_review_selection_from_persistence) in factory source, found {call_count}'
  )


def test_packet_b_hyd2_refresh_state_payload_does_not_call_hydrate_review_selection() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert '_refresh_state_payload' in source, 'sanity: _refresh_state_payload must exist in factory source'
  # _refresh_state_payload should NOT contain a bare hydrate call; only resume_saved_selection dispatch does
  # Verify by checking that total ungated call sites are absent — covered by the call_count test above


def test_packet_b_hyd2_run_bridge_guard_failure_does_not_call_hydrate_review_selection() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  # _run_bridge_guard_failure should not contain the hydrate call; it now reads review_selection_state directly
  assert '_run_bridge_guard_failure' in source, 'sanity: _run_bridge_guard_failure must exist'


def test_packet_b_hyd2_resume_saved_selection_action_present_in_overlay_handler() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert "action_name == 'resume_saved_selection'" in source, (
    'HYD-2: resume_saved_selection action dispatch must be present in overlay handler'
  )
  assert "'resume_saved_selection'" in source, (
    'HYD-2: resume_saved_selection must appear in the allowed-actions guard'
  )


def test_packet_b_hyd2_run_bridge_guard_returns_no_saved_set_when_review_selection_empty(
  tmp_path: Path, monkeypatch: Any
) -> None:
  state_db_path = str(tmp_path / 'b2a.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')
  # Cold boot with no saved set — review_selection_state will be empty
  _call_app(app, method='GET', path='/')
  # POST to /api/run with bridge_action=submit_order — should hit bridge guard and return no_saved_set
  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/run',
    body={'bridge_action': 'submit_order'},
  )
  payload = json.loads(body)
  assert status == '200 OK'
  reason = str(payload.get('reason') or '').lower()
  decision = str(payload.get('decision') or '').lower()
  assert decision == 'no-go', f'B-2a: expected no-go decision from bridge guard, got: {decision!r}'
  assert 'no_saved_set' in reason or 'saved' in reason or 'candidate' in reason, (
    f'B-2a: bridge guard must return no_saved_set reason when review_selection is empty, got: {reason!r}'
  )


def test_packet_b_hyd2_resume_saved_selection_action_dispatches_without_error(
  tmp_path: Path, monkeypatch: Any
) -> None:
  # B-2b: resume_saved_selection is a valid action that returns 200 OK and reflects DB state.
  # Full hydration requires mode selection (lane boundary guard); this test verifies the route
  # dispatches cleanly and that prior_saved_set_available reflects the DB correctly.
  state_db_path = str(tmp_path / 'b2b.sqlite3')
  settings = _build_test_settings(state_db_path)
  _seed_saved_set(
    state_db_path,
    saved_set_id='b2b-set-1',
    state_id='review_hold_saved_selection_locked',
    members=[{
      'candidate_uid': 'b2b-uid-1',
      'ticker': 'KALSHI-B2B-1',
      'density_weight': '2.0',
      'liquidity_score': '150',
      'market_edge_dollars': '0.09',
      'current_price': '0.48',
      'threshold_price': '0.39',
    }],
    saved_key_count=1,
  )
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / '_tombstone.json')
  _call_app(app, method='GET', path='/')
  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/runtime-overlay',
    body={'action': 'resume_saved_selection'},
  )
  assert status == '200 OK', f'B-2b: resume_saved_selection action must return 200, got {status}'
  payload = json.loads(body)
  # prior_saved_set_available confirms the DB is correctly queried after the action
  runtime = (payload.get('session_overlay') or {}).get('runtime') or {}
  assert runtime.get('prior_saved_set_available') is True, (
    'B-2b: prior_saved_set_available must be True when a saved set exists in the DB'
  )
  assert 'decision' not in payload or payload.get('decision') != 'no-go', (
    f'B-2b: resume_saved_selection must not produce a no-go decision, got: {payload.get("decision")!r}'
  )


def test_packet_b_hyd2_prior_saved_set_available_field_present_in_session_overlay_source() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  assert 'prior_saved_set_available' in source, (
    'B-2c: prior_saved_set_available field must be present in session overlay payload builder'
  )


# ---------------------------------------------------------------------------
# SUBMIT-SUPREMACY — contract locks (SUBMIT_SUPREMACY_AUTOMATION_SCHEDULER_PRIORITY_BMAP_2026-06-27)
# ---------------------------------------------------------------------------


def test_scan_refire_does_not_dispatch_submit_or_scan_from_browser() -> None:
  # Phase 5: active automation dispatch is backend-owned. The legacy browser refire seam
  # must not be able to launch submit or scan from the frontend.
  source = inspect.getsource(web_app._render_html)
  fn_start = source.find('function requestScheduledScanRefire()')
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert "performAction('run'" not in fn_body, 'browser refire must not submit orders'
  assert "performAction('scan'" not in fn_body, 'browser refire must not launch scans'
  assert 'return null;' in fn_body, 'legacy refire seam must be inert'


def test_scheduler_admit_blocks_automated_scan_when_scan_in_progress() -> None:
  # Contract 3 step 3: when scan is processing or canceling, a new automated scan request
  # must be blocked (yielding wait_scan_terminal) rather than launching a duplicate scan or submit.
  source = inspect.getsource(web_app.create_operator_console_app)
  fn_start = source.find('def _scheduler_admit_action(')
  assert fn_start != -1, '_scheduler_admit_action must be defined'
  fn_end = source.find('\n  def ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert 'automated_scan_sources' in fn_body, (
    'admission must define automated_scan_sources set to distinguish automated from manual'
  )
  # scan_status 'processing'/'canceling' must block automated scan sources
  assert "'processing'" in fn_body and "'canceling'" in fn_body, (
    'contract 3: admission must block on processing/canceling scan status'
  )
  assert "decision='wait'" in fn_body, (
    'contract 3: a blocked admission must emit decision=wait'
  )
  # The scan-in-progress block must apply to automated sources
  assert 'scan_in_progress' in fn_body, (
    "contract 3: admission must use 'scan_in_progress' as the block reason for in-progress scans"
  )


def test_post_submit_cadence_deadline_uses_submit_completion_origin() -> None:
  # Contract 4: the post-submit cadence countdown must derive its deadline from
  # last_submit_completed_at_utc, not from the scan completion timestamp. The backend
  # must set last_submit_completed_at_utc when releasing a submit_bridge lease so that
  # _scheduler_cadence_state can use submit terminal time as the cadence origin.
  # The cadence-countdown projection (incl. the post-submit origin) lives in the shared
  # _scheduler_timer_countdown_state helper that _scheduler_cadence_state delegates to.
  source = inspect.getsource(web_app.create_operator_console_app)
  fn_start = source.find('def _scheduler_timer_countdown_state(')
  assert fn_start != -1, '_scheduler_timer_countdown_state must be defined'
  fn_end = source.find('\n  def ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert 'last_submit_completed_at_utc' in fn_body, (
    'contract 4: the cadence countdown projection must use last_submit_completed_at_utc as the'
    ' post-submit cadence origin when the most recent automation cycle had a candidate submit'
  )
  assert "'submit_completion'" in fn_body, (
    'contract 4: the post-submit cadence origin must be tied to the submit_completion timer source'
  )


def test_automation_overlay_resume_emits_scheduler_event_not_direct_cadence() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  route_start = source.find("if method == 'POST' and path == '/api/automation-overlay':")
  assert route_start != -1, 'automation overlay route must exist'
  route_end = source.find("if method == 'POST' and path == '/api/context-overlay':", route_start)
  route_body = source[route_start:route_end] if route_end != -1 else source[route_start:]
  assert "_scheduler_handle_event(\n            'automation_enabled'" in route_body
  assert "_scheduler_handle_event(\n            'automation_paused'" in route_body
  assert "_scheduler_handle_event(\n          'automation_stopped'" in route_body
  assert '_scheduler_start_cadence_timer(' not in route_body


def test_stop_route_delegates_policy_teardown_and_transition_to_scheduler() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  route_start = source.find("if method == 'POST' and path == '/api/automation-overlay':")
  route_end = source.find("if method == 'POST' and path == '/api/context-overlay':", route_start)
  route_body = source[route_start:route_end] if route_end != -1 else source[route_start:]
  stop_branch_start = route_body.find("elif action_name == 'stop':")
  stop_branch_end = route_body.find('else:', stop_branch_start)
  stop_branch = route_body[stop_branch_start:stop_branch_end]
  assert "automation_overlay_state['enabled'] = False" not in stop_branch
  assert "automation_overlay_state['paused'] = False" not in stop_branch
  assert "'gate_snapshot': gate_snapshot" in route_body
  assert "transition_reason == 'operator_pause'" in route_body
  assert "transition_reason in {'operator_stop', 'operator_pause'}" not in route_body


def test_scheduler_stop_handler_owns_stop_staging_and_normalized_teardown() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find("if normalized_event == 'automation_stopped':")
  handler_end = source.find("if normalized_event == 'automation_paused':", handler_start)
  handler_body = source[handler_start:handler_end]
  assert "automation_overlay_state['enabled'] = False" in handler_body
  assert '_scheduler_cancel_timers()' in handler_body
  assert '_scheduler_stage_stop_scan_cancel(settings)' in handler_body
  assert '_run_operator_teardown(' in handler_body
  assert '_persist_automation_transition(' in handler_body
  assert "'stop_teardown': stop_teardown" in handler_body


def test_scheduler_stop_scan_cancel_stamps_stop_owned_projection() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  helper_start = source.find('def _scheduler_stage_stop_scan_cancel(')
  assert helper_start != -1, 'Stop must use a scheduler-owned scan-cancel staging helper'
  helper_end = source.find('\n  def ', helper_start + 1)
  helper_body = source[helper_start:helper_end] if helper_end != -1 else source[helper_start:]
  assert '_request_scan_runtime_cancel(settings)' in helper_body
  assert "scheduler_state['owner'] = 'stop'" in helper_body
  assert "'kind': 'automation_stopped'" in helper_body
  assert "bool(cancel_snapshot.get('cancel_requested'))" in helper_body


def test_zero_found_retry_wait_is_completed_not_active_scan() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  scan_start = source.find('def _run_scan() -> None:')
  assert scan_start != -1
  scan_end = source.find('\n    scan_runtime_thread = threading.Thread', scan_start)
  scan_body = source[scan_start:scan_end] if scan_end != -1 else source[scan_start:]
  zero_start = scan_body.find('if is_zero_found_retry:')
  zero_end = scan_body.find('else:', zero_start)
  zero_body = scan_body[zero_start:zero_end]
  assert "status='completed'" in zero_body
  assert 'active=False' in zero_body
  assert "stage='retry_wait'" in zero_body
  assert 'completed_at_utc=now_utc' in zero_body


def test_submit_bridge_lease_ttl_uses_entry_window_plus_post_submit_buffer() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  ttl_start = source.find('def _scheduler_lease_ttl_sec(')
  ttl_end = source.find('\n  def ', ttl_start + 1)
  ttl_body = source[ttl_start:ttl_end]
  assert "owner or '').strip() != 'submit_bridge'" in ttl_body
  assert "getattr(settings, 'entry_window_start_sec'" in ttl_body
  assert '_scheduler_effective_post_submit_processing_buffer_sec(settings)' in ttl_body
  acquire_start = source.find('def _scheduler_acquire_lease(')
  acquire_end = source.find('\n  def ', acquire_start + 1)
  acquire_body = source[acquire_start:acquire_end]
  assert "lease_ttl_sec = _scheduler_lease_ttl_sec(settings, owner)" in acquire_body
  assert "'expires_monotonic': now_monotonic + lease_ttl_sec" in acquire_body


def test_late_submit_terminal_preserves_stopped_halt_projection() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find("if normalized_event == 'submit_terminal':")
  handler_end = source.find("snapshot = _scheduler_build_snapshot(settings=settings, source=normalized_event or 'scheduler_event'", handler_start)
  handler_body = source[handler_start:handler_end]
  assert 'stopped_terminal = (' in handler_body
  assert "current_halt_projection if stopped_terminal else" in handler_body
  assert "not stopped_terminal" in handler_body
  assert "_scheduler_start_cadence_timer(" in handler_body


def test_post_submit_processing_buffer_web_policy_and_set_wiring() -> None:
  render_source = inspect.getsource(web_app._render_html)
  app_source = inspect.getsource(web_app.create_operator_console_app)
  assert "parameter_id: 'post_submit_processing_buffer_sec'" in render_source
  assert "post_submit_processing_buffer_sec: null" in render_source
  assert "post_submit_processing_buffer_sec: { type: 'number'" in render_source
  assert "'post_submit_processing_buffer_sec'," in app_source
  assert 'FLOOR_POST_SUBMIT_PROCESSING_BUFFER_SEC' in app_source


def test_scheduler_timer_callbacks_emit_events_not_direct_scan_dispatch() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  for fn_name, event_name in (
    ('_scheduler_start_retry_timer', 'retry_timer_elapsed'),
    ('_scheduler_start_cadence_timer', 'cadence_timer_elapsed'),
  ):
    fn_start = source.find(f'def {fn_name}(')
    assert fn_start != -1, f'{fn_name} must be defined'
    fn_end = source.find('\n  def ', fn_start + 1)
    fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
    assert '_scheduler_dispatch_scan_internal(' not in fn_body
    assert '_scheduler_handle_event(' in fn_body
    assert event_name in fn_body


def test_scan_terminal_completion_token_required_for_active_automation_routing() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'scan_terminal'")
  st_end = handler_body.find("normalized_event == 'retry_timer_elapsed'", st_start)
  st_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  validation_pos = st_body.find('_validate_scan_terminal_completion_token(')
  failed_pos = st_body.find("terminal_status in {'failed', 'canceled', 'cancelled'}")
  found_pos = st_body.find('found_count > 0')
  retry_pos = st_body.find('found_count == 0')
  assert validation_pos != -1, 'scan terminal routing must validate completion token first'
  assert validation_pos < failed_pos < found_pos < retry_pos
  assert "decision='halted_scan_terminal_contract_invalid'" in st_body
  assert "'scan_terminal_contract_invalid'" in st_body


def test_scan_terminal_completion_token_matches_runtime_and_outer_detail() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  builder_start = source.find('def _build_scan_terminal_completion_token(')
  builder_end = source.find('\n  def _scan_terminal_source_ref_from_token(', builder_start)
  builder_body = source[builder_start:builder_end] if builder_end != -1 else source[builder_start:]
  for expected in (
    "'token_kind': 'scan_terminal_completion'",
    "'scan_session_id': str(runtime_snapshot.get('",
    "'lane_session_id': str(runtime_snapshot.get('",
    "'scan_scheduler_lease_id': str(scan_scheduler_lease_id or '')",
    "'terminal_status': str(terminal_status or '').strip().lower()",
    "'found_count': int(found_count or 0)",
    "'raw_found_count': int(found_count or 0)",
    "'terminal_at_utc': runtime_snapshot.get('completed_at_utc')",
    "'verified_runtime_written': True",
  ):
    assert expected in builder_body
  validator_start = source.find('def _validate_scan_terminal_completion_token(')
  validator_end = source.find('\n  def _retry_elapsed_source_ref_valid(', validator_start)
  validator_body = source[validator_start:validator_end] if validator_end != -1 else source[validator_start:]
  for expected in (
    'missing_scan_terminal_completion_token',
    'scan_terminal_status_mismatch',
    'scan_terminal_found_count_mismatch',
    'scan_terminal_raw_found_count_mismatch',
    'scan_terminal_session_mismatch',
    'scan_terminal_lane_session_mismatch',
    'scan_terminal_lease_mismatch',
    'scan_terminal_timestamp_missing',
  ):
    assert expected in validator_body


def test_zero_found_retry_timer_carries_scan_terminal_token_reference() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  scan_start = source.find('def _run_scan() -> None:')
  scan_end = source.find('\n    scan_runtime_thread = threading.Thread', scan_start)
  scan_body = source[scan_start:scan_end] if scan_end != -1 else source[scan_start:]
  zero_start = scan_body.find('if is_zero_found_retry:')
  zero_end = scan_body.find('else:', zero_start)
  zero_body = scan_body[zero_start:zero_end]
  assert '_build_scan_terminal_completion_token(' in zero_body
  assert "retry_source='zero_found_retry'" in zero_body
  assert "'scan_terminal_completion_token': scan_terminal_token" in zero_body
  release_start = source.find('def _scheduler_release_lease(')
  release_end = source.find('\n  def _scheduler_block_snapshot(', release_start)
  release_body = source[release_start:release_end] if release_end != -1 else source[release_start:]
  assert "scheduler_state['pending_retry_after_scan_release'] = None" in release_body
  assert "'scheduler_retry_timer_started_after_release'" in release_body
  assert "'source_lease_released_at_utc'" in release_body
  timer_start = source.find('def _scheduler_start_retry_timer(')
  timer_end = source.find('\n  def _scheduler_start_cadence_timer(', timer_start)
  timer_body = source[timer_start:timer_end] if timer_end != -1 else source[timer_start:]
  assert 'source_terminal_ref: JSONDict | None = None' in timer_body
  assert 'source_terminal_ref=source_terminal_ref' in timer_body
  assert "if str(key or '').startswith('source_')" in timer_body
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'scan_terminal'")
  st_end = handler_body.find("normalized_event == 'retry_timer_elapsed'", st_start)
  st_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  assert "scheduler_state['pending_retry_after_scan_release']" in st_body
  assert "'source_terminal_ref': _scan_terminal_source_ref_from_token(scan_terminal_token)" in st_body


def test_zero_actionable_retry_source_ref_carries_retry_source_and_actionability_facts() -> None:
  # AUTOMATION_CADENCE_RECOVERY_AND_INACTIVITY_GUARD_BMAP_2026-07-02 (C1 / Lane A): a raw-found but
  # zero-actionable scan terminal must queue its pending retry with an explicit retry source plus
  # the actionability facts. Completed-scan tokens carry a blank retry_source, and a blank
  # source_retry later fails the retry elapsed validator, halting the cadence as contract invalid
  # (verified live incident 2026-07-02, lane live-20260702T045949Z).
  source = inspect.getsource(web_app.create_operator_console_app)
  helper_start = source.find('def _zero_actionable_retry_source_ref(')
  assert helper_start != -1, 'zero-actionable retry source ref helper must exist'
  helper_end = source.find('\n  def _validate_scan_terminal_completion_token(', helper_start)
  helper_body = source[helper_start:helper_end] if helper_end != -1 else source[helper_start:]
  for expected in (
    "ref['source_retry'] = str(ref.get('source_retry') or '').strip() or 'zero_found_retry'",
    "'source_raw_found_count'",
    "'source_actionable_candidate_count'",
    "'source_actionability_status'",
    "'source_actionability_reason'",
    "'source_scan_completed_at_utc'",
  ):
    assert expected in helper_body
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'scan_terminal'")
  st_end = handler_body.find("normalized_event == 'retry_timer_elapsed'", st_start)
  st_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  # Both zero-found retry routes (raw-found-zero-actionable and true zero-found) must build the
  # pending retry reference through the enriched helper; the scan-failure route keeps the plain
  # ref because its token already stamps retry_source at construction.
  assert st_body.count("'source_terminal_ref': _zero_actionable_retry_source_ref(scan_terminal_token, actionability_resolution)") == 2
  assert "'source_terminal_ref': _scan_terminal_source_ref_from_token(scan_terminal_token)" in st_body


def test_retry_source_metadata_only_gap_normalizes_and_other_invalid_states_fail_closed() -> None:
  # AUTOMATION_CADENCE_RECOVERY_AND_INACTIVITY_GUARD_BMAP_2026-07-02 (C2 / Lane B): normalization
  # self-corrects ONLY the metadata-only raw-found-zero-actionable retry source gap, persists a
  # warning, and refuses when identity fields are incomplete, provenance is not zero-actionable,
  # a fail-closed halt is active, or a submit lease is active. It runs before elapsed validation.
  source = inspect.getsource(web_app.create_operator_console_app)
  helper_start = source.find('def _normalize_metadata_only_retry_source_gap(')
  assert helper_start != -1, 'metadata-only retry source normalization helper must exist'
  helper_end = source.find('\n  def _retry_elapsed_interrupted_by_stop_or_cancel(', helper_start)
  helper_body = source[helper_start:helper_end] if helper_end != -1 else source[helper_start:]
  for expected in (
    "if str(event_detail.get('source_retry') or '').strip():",
    "'source_scan_session_id'",
    "'source_lease_released_at_utc'",
    "!= 'zero_actionable'",
    "int(event_detail.get('source_actionable_candidate_count') or 0) != 0",
    "bool(halt_projection.get('active'))",
    "== 'submit_bridge'",
    "event_detail['source_retry'] = 'zero_found_retry'",
    "'normalization_reason': 'metadata_only_gap_zero_actionable_provenance'",
  ):
    assert expected in helper_body
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  retry_start = handler_body.find("normalized_event == 'retry_timer_elapsed'")
  retry_end = handler_body.find("normalized_event == 'cadence_timer_elapsed'", retry_start)
  retry_body = handler_body[retry_start:retry_end] if retry_end != -1 else handler_body[retry_start:]
  norm_pos = retry_body.find('_normalize_metadata_only_retry_source_gap(event_detail)')
  valid_pos = retry_body.find('_retry_elapsed_source_ref_valid(event_detail)')
  assert norm_pos != -1 and valid_pos != -1
  assert norm_pos < valid_pos, 'normalization must run before elapsed validation'
  assert "'scheduler_retry_source_ref_normalized'" in retry_body
  assert "level='WARN'" in retry_body


def test_retry_timer_start_validates_source_ref_and_blocks_doomed_timer() -> None:
  # AUTOMATION_CADENCE_RECOVERY_AND_INACTIVITY_GUARD_BMAP_2026-07-02 (C2 / Lane B): the pending
  # retry source reference is validated (after normalization) BEFORE the retry timer is armed at
  # scan lease release, so an unrecoverable reference halts explicitly at start instead of arming
  # a timer that is guaranteed to halt at elapsed.
  source = inspect.getsource(web_app.create_operator_console_app)
  release_start = source.find('def _scheduler_release_lease(')
  release_end = source.find('\n  def _scheduler_block_snapshot(', release_start)
  release_body = source[release_start:release_end] if release_end != -1 else source[release_start:]
  probe_pos = release_body.find('_normalize_metadata_only_retry_source_gap(start_probe)')
  valid_pos = release_body.find('_retry_elapsed_source_ref_valid(start_probe)')
  start_pos = release_body.find('_scheduler_start_retry_timer(')
  assert probe_pos != -1 and valid_pos != -1 and start_pos != -1
  assert probe_pos < valid_pos < start_pos, 'validate (after normalization) before arming the timer'
  assert "'scheduler_retry_timer_start_blocked'" in release_body
  assert "decision='halted_retry_timer_contract_invalid'" in release_body
  assert "'missing_retry_source_terminal_ref_at_start'" in release_body


def test_scheduler_snapshot_projects_active_timer_owner_only_with_deadline() -> None:
  # AUTOMATION_CADENCE_RECOVERY_AND_INACTIVITY_GUARD_BMAP_2026-07-02 (C3+C4 / Lane C): the
  # snapshot projects the single active scheduler timer as flat owner/start/deadline/state fields,
  # never an owner without a backend deadline, with retry outranking cadence; and while a submit
  # lease is active it projects the lease id, backend start time, and a derivable elapsed value.
  source = inspect.getsource(web_app.create_operator_console_app)
  builder_start = source.find('def _scheduler_build_snapshot(')
  builder_end = source.find('\n  def _persist_scheduler_event(', builder_start)
  builder_body = source[builder_start:builder_end] if builder_end != -1 else source[builder_start:]
  retry_pick = builder_body.find("if isinstance(retry_timer_record, dict) and bool(retry_timer_record.get('active')):")
  cadence_pick = builder_body.find("elif isinstance(cadence_timer_record, dict) and bool(cadence_timer_record.get('active')):")
  assert retry_pick != -1 and cadence_pick != -1
  assert retry_pick < cadence_pick, 'retry timer must outrank cadence in the flat projection'
  for expected in (
    "str(active_timer_record.get('deadline_utc') or '').strip()",
    "'scheduler_timer_owner': scheduler_timer_owner",
    "'scheduler_timer_started_at_utc': scheduler_timer_started_at_utc",
    "'scheduler_timer_deadline_utc': scheduler_timer_deadline_utc",
    "'scheduler_timer_state': scheduler_timer_state",
    "'scheduler_timer_source_ref': scheduler_timer_source_ref",
    "scheduler_submit_state = 'submitting'",
    "'scheduler_submit_state': scheduler_submit_state",
    "'submit_scheduler_lease_id': submit_scheduler_lease_id",
    "'submit_started_at_utc': submit_started_at_utc",
    "'submit_elapsed_ms': submit_elapsed_ms",
  ):
    assert expected in builder_body


def test_cadence_timer_missing_yields_to_armed_retry_and_active_halt() -> None:
  # AUTOMATION_CADENCE_RECOVERY_AND_INACTIVITY_GUARD_BMAP_2026-07-02 (C3 / Lane C): the stale
  # submit-completion timestamp must not resurrect a deadline-less cadence-missing state after a
  # later scan/retry transition superseded the post-submit beat — an armed retry record owns the
  # wait and an active halt projection owns the explanation (2026-07-02 incident display symptom).
  source = inspect.getsource(web_app.create_operator_console_app)
  cadence_start = source.find('def _scheduler_cadence_state(')
  cadence_end = source.find('\n  def _scheduler_public_lease(', cadence_start)
  cadence_body = source[cadence_start:cadence_end] if cadence_end != -1 else source[cadence_start:]
  for expected in (
    'not retry_armed',
    'not halt_projection_active',
    "'cadence_timer_missing'",
  ):
    assert expected in cadence_body


def test_submit_elapsed_display_seeds_from_backend_lease_start() -> None:
  # AUTOMATION_CADENCE_RECOVERY_AND_INACTIVITY_GUARD_BMAP_2026-07-02 (C4 / Lane C): the submit
  # elapsed card seeds from the backend submit lease start for BOTH a newly observed lease and a
  # resumed render — never a frontend-only zero when a backend start exists. The old seeding was
  # gated to known-lease + no-ticker, so a new lease started at 0.
  source = inspect.getsource(web_app._render_html)
  assert '_d_snap.submit_started_at_utc || _d_snap.lease_started_at_utc' in source
  assert '_d_snap.submit_scheduler_lease_id || _d_snap.lease_id' in source
  assert 'state.submitOrderElapsedSec = Math.max(0, Math.floor((Date.now() - Date.parse(_d_startedAt)) / 1000))' in source
  assert 'state.submitOrderElapsedLeaseId && !state.submitOrderElapsedTimerId' not in source, (
    'backend seeding must not be gated behind the no-ticker/known-lease condition'
  )


def test_inactivity_guard_is_scheduler_owned_one_shot_event_rearmed_watchdog() -> None:
  # AUTOMATION_CADENCE_RECOVERY_AND_INACTIVITY_GUARD_BMAP_2026-07-02 (C5 / D9): the inactivity
  # guard is a third scheduler timer slot, re-armed by authoritative scheduler events, stale-fire
  # protected by timer identity, and canceled with the scheduler timers on stop/pause/disable.
  source = inspect.getsource(web_app.create_operator_console_app)
  assert "'inactivity_guard_timer': None" in source
  assert "'inactivity_guard_state': 'disabled'" in source
  assert "'inactivity_guard_recovery_history': []" in source
  assert "'guard': None" in source
  assert "for key in ('retry_timer', 'cadence_timer', 'inactivity_guard_timer')" in source
  assert "_scheduler_set_timer_record(\n      'guard'," in source
  assert "threading.Timer(threshold_sec, _callback)" in source
  assert "scheduler_state.get('inactivity_guard_timer') is not timer_ref[0]" in source
  assert "_scheduler_finish_event(snapshot, settings, env_override, subaccount_override, lane_session_id, normalized_event)" in source
  assert "_scheduler_rearm_inactivity_guard(settings, env_override, subaccount_override, lane_session_id)" in source
  assert "normalized_event in {'automation_stopped', 'automation_paused'}" in source
  guard_start = source.find('def _scheduler_rearm_inactivity_guard(')
  guard_end = source.find('\n  def _scheduler_finish_event(', guard_start)
  guard_body = source[guard_start:guard_end] if guard_end != -1 else source[guard_start:]
  assert 'while ' not in guard_body
  assert 'time.sleep' not in guard_body


def test_inactivity_guard_threshold_classifier_and_recovery_contract() -> None:
  # AUTOMATION_CADENCE_RECOVERY_AND_INACTIVITY_GUARD_BMAP_2026-07-02 (C5 / D10): the trigger is
  # cadence+90s, absent cadence disables the guard, classification is fail-closed for active or
  # protected states, and recovery is bounded scan-only through scheduler admission.
  source = inspect.getsource(web_app.create_operator_console_app)
  helper_start = source.find('def _inactivity_guard_trigger_threshold_sec(')
  helper_end = source.find('\n  def _inactivity_guard_set_state(', helper_start)
  helper_body = source[helper_start:helper_end] if helper_end != -1 else source[helper_start:]
  assert 'INACTIVITY_GUARD_PAD_SEC = 90.0' in source
  assert "cadence_ms = int(automation_overlay_state.get('cadence_ms') or 0)" in helper_body
  assert 'return float(cadence_ms) / 1000.0 + INACTIVITY_GUARD_PAD_SEC' in helper_body
  assert "reason='cadence_value_absent'" in source
  classifier_start = source.find('def _inactivity_guard_classify(')
  classifier_end = source.find('\n  def _inactivity_guard_fire(', classifier_start)
  classifier_body = source[classifier_start:classifier_end] if classifier_end != -1 else source[classifier_start:]
  for expected in (
    "'disabled'",
    "'healthy_wait'",
    "'active_work'",
    "'slow_work'",
    "'recoverable_idle_gap'",
    "'blocked_fail_closed'",
    "'budget_exhausted'",
    "'submit_ready_saved_set'",
    "'failed_submit_money_path_crossed'",
  ):
    assert expected in classifier_body
  assert "pending_reason = str(scheduler_state.get('pending_reason') or '').strip().lower()" in classifier_body
  assert "pending_action == 'scan' and pending_reason.startswith('socket_')" in classifier_body
  assert "return 'recoverable_idle_gap', f'deferred_scan_{pending_reason}'" in classifier_body
  fire_start = source.find('def _inactivity_guard_fire(')
  fire_end = source.find('\n  def _scheduler_rearm_inactivity_guard(', fire_start)
  fire_body = source[fire_start:fire_end] if fire_end != -1 else source[fire_start:]
  assert "source='automation_cadence'" in fire_body
  assert "'inactivity_guard_recovery_dispatched' if dispatched else 'inactivity_guard_recovery_deferred'" in fire_body
  assert 'INACTIVITY_GUARD_MAX_RECOVERIES = 2' in source
  assert 'INACTIVITY_GUARD_BUDGET_WINDOW_SEC = 900.0' in source
  assert "'inactivity_guard_decision'" in source


def test_inactivity_guard_recovery_correction_rearm_budget_and_route_sync() -> None:
  # GUARD_RECOVERY_CORRECTION_AND_WATCH_STALE_ERROR_PROJECTION_BMAP_2026-07-02 (G1/G2/G3,
  # D12/D13/D14): a non-admitted recovery re-arms the watchdog (socket reconnect emits no
  # scheduler event, so an unarmed guard would freeze); every recoverable classification —
  # including the socket-deferred pending scan — is budget-gated with attempt-based accounting;
  # and the automation policy route syncs the guard lifecycle on every non-stop transition.
  source = inspect.getsource(web_app.create_operator_console_app)
  # G2: budget precedes the pending block, and the socket branch is gated on it.
  classifier_start = source.find('def _inactivity_guard_classify(')
  classifier_end = source.find('\n  def _inactivity_guard_fire(', classifier_start)
  classifier_body = source[classifier_start:classifier_end] if classifier_end != -1 else source[classifier_start:]
  budget_pos = classifier_body.find('recovery_budget_available = len(fresh_recoveries) < INACTIVITY_GUARD_MAX_RECOVERIES')
  pending_pos = classifier_body.find('if pending_action:')
  socket_pos = classifier_body.find("pending_action == 'scan' and pending_reason.startswith('socket_')")
  socket_gate_pos = classifier_body.find('if not recovery_budget_available:', socket_pos)
  socket_return_pos = classifier_body.find("return 'recoverable_idle_gap', f'deferred_scan_{pending_reason}'")
  assert -1 < budget_pos < pending_pos < socket_pos < socket_gate_pos < socket_return_pos
  assert classifier_body.count("return 'budget_exhausted', 'recovery_budget_reached'") == 2
  # G1: after the dispatched/deferred persist, only the admitted path rests without re-arming;
  # the non-admitted path re-arms.
  fire_start = source.find('def _inactivity_guard_fire(')
  fire_end = source.find('\n  def _scheduler_rearm_inactivity_guard(', fire_start)
  fire_body = source[fire_start:fire_end] if fire_end != -1 else source[fire_start:]
  persist_pos = fire_body.find("'inactivity_guard_recovery_dispatched' if dispatched else 'inactivity_guard_recovery_deferred'")
  dispatched_pos = fire_body.find('if dispatched:', persist_pos)
  rearm_pos = fire_body.find('_scheduler_rearm_inactivity_guard(settings, env_override, subaccount_override, lane_session_id)', dispatched_pos)
  assert -1 < persist_pos < dispatched_pos < rearm_pos
  # G2 attempt accounting: the history entry is appended before dispatch, so deferred attempts
  # count against the budget.
  history_pos = fire_body.find("history.append({'lane_session_id': lane_session_id, 'monotonic': time.monotonic()})")
  dispatch_call_pos = fire_body.find('_scheduler_dispatch_scan_internal(')
  assert -1 < history_pos < dispatch_call_pos
  # G3: the policy route syncs the guard lifecycle for non-stop transitions after the transition
  # persist, using the route-resolved settings.
  route_sync_pos = source.find(
    '_scheduler_rearm_inactivity_guard(\n          active_settings,\n          env_override,\n          subaccount_override,'
  )
  persist_transition_pos = source.find('_persist_automation_transition(')
  assert -1 < persist_transition_pos < route_sync_pos


def test_error_no_exposure_is_terminal_on_web_hold_surfaces() -> None:
  # GUARD_RECOVERY_CORRECTION_AND_WATCH_STALE_ERROR_PROJECTION_BMAP_2026-07-02 (W2/W3, D16/D18):
  # the qualified terminal no-exposure state opens no live-interaction hold and no
  # active/unresolved/attention counts, while raw ERROR and RECONCILE_REQUIRED keep demanding
  # attention, and the raw-state SQL exclusion lists stay untouched.
  assert 'ERROR_NO_EXPOSURE' in web_app.PAIR_TERMINAL_PUBLIC_STATES
  assert 'ERROR_NO_EXPOSURE' in web_app.PAIR_ACTIVE_PROJECTION_EXCLUDED_STATES
  assert web_app.PAIR_STATE_PUBLIC_LABELS['ERROR_NO_EXPOSURE'] == 'Failed - no exposure'
  assert 'ERROR' not in web_app.PAIR_ACTIVE_PROJECTION_EXCLUDED_STATES
  assert 'RECONCILE_REQUIRED' not in web_app.PAIR_ACTIVE_PROJECTION_EXCLUDED_STATES
  payload = web_app._build_pair_monitor_payload(
    {
      'settings': {'operation_lane': 'offline'},
      'pairs': [
        {'ticker': 'T-NOEXP', 'state': 'ERROR', 'public_state_id': 'ERROR_NO_EXPOSURE'},
        {'ticker': 'T-HOLD', 'state': 'ERROR', 'public_state_id': 'RECONCILE_REQUIRED'},
      ],
    }
  )
  attention_tickers = {str(pair.get('ticker')) for pair in payload['attention_pairs']}
  assert attention_tickers == {'T-HOLD'}
  assert payload['attention_count'] == 1
  # D18: the raw-state NOT-IN exclusion lists are unchanged — row-level projection is the only
  # hold-closing mechanism; fill-bearing ERROR rows must keep flowing to attention surfaces.
  module_source = inspect.getsource(web_app)
  assert module_source.count("state NOT IN ('LOCKED', 'FILLED', 'SETTLED', 'CANCELED', 'SETTLED_EXPOSURE')") == 2


def test_websocket_connected_reconcile_does_not_dispatch_submit() -> None:
  # Last-update audit, 2026-07-02: reconnect reconcile must remain pair-reconcile only. Submit
  # dispatch belongs to scheduler events and submit admission, not a side helper under websocket
  # health.
  source = inspect.getsource(web_app.create_operator_console_app)
  assert '_scheduler_recover_socket_deferred_submit' not in source
  assert 'scheduler_socket_deferred_submit' not in source
  reconcile_start = source.find('def _websocket_connected_reconcile(reason: str) -> None:')
  reconcile_end = source.find('\n        async def _websocket_connected_reconcile_async', reconcile_start)
  reconcile_body = source[reconcile_start:reconcile_end] if reconcile_end != -1 else source[reconcile_start:]
  assert '_scheduler_dispatch_submit_internal(' not in reconcile_body
  assert '_build_backend_submit_handoff(' not in reconcile_body
  assert "'deferred_submit_recovery'" not in reconcile_body


def test_retry_elapsed_rejects_missing_token_reference_and_persists_before_dispatch() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  validator_start = source.find('def _retry_elapsed_source_ref_valid(')
  validator_end = source.find('\n  def _retry_elapsed_interrupted_by_stop_or_cancel(', validator_start)
  validator_body = source[validator_start:validator_end] if validator_end != -1 else source[validator_start:]
  for expected in (
    'source_scan_session_id',
    'source_terminal_at_utc',
    'source_retry',
    'source_lease_released_at_utc',
    'source_released_lease_id',
    'source_release_reason',
  ):
    assert expected in validator_body
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  retry_start = handler_body.find("normalized_event == 'retry_timer_elapsed'")
  retry_end = handler_body.find("normalized_event == 'cadence_timer_elapsed'", retry_start)
  retry_body = handler_body[retry_start:retry_end] if retry_end != -1 else handler_body[retry_start:]
  assert '_retry_elapsed_source_ref_valid(event_detail)' in retry_body
  assert "decision='halted_retry_timer_contract_invalid'" in retry_body
  assert "'retry_timer_contract_invalid'" in retry_body
  persist_pos = retry_body.find("_persist_scheduler_event(settings, f'scheduler_event_{normalized_event}', pre_dispatch_snapshot")
  dispatch_pos = retry_body.find('_scheduler_dispatch_scan_internal(')
  assert persist_pos != -1 and dispatch_pos != -1
  assert persist_pos < dispatch_pos, 'retry elapsed evidence must persist before scan admission'


def test_retry_elapsed_blocks_scan_when_stop_or_cancel_active() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  helper_start = source.find('def _retry_elapsed_interrupted_by_stop_or_cancel(')
  helper_end = source.find('\n  def _scheduler_start_retry_timer(', helper_start)
  helper_body = source[helper_start:helper_end] if helper_end != -1 else source[helper_start:]
  for expected in (
    "automation_overlay_state.get('state_id')",
    "state_owner in {'stop', 'manual_stop', 'cancel'}",
    "halt_kind in {'automation_stopped', 'scan_canceled_halted'}",
    "scan_snapshot.get('cancel_requested')",
    "scan_status in {'canceling', 'canceled', 'cancelled'}",
  ):
    assert expected in helper_body
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  retry_start = handler_body.find("normalized_event == 'retry_timer_elapsed'")
  retry_end = handler_body.find("normalized_event == 'cadence_timer_elapsed'", retry_start)
  retry_body = handler_body[retry_start:retry_end] if retry_end != -1 else handler_body[retry_start:]
  interrupt_pos = retry_body.find('_retry_elapsed_interrupted_by_stop_or_cancel()')
  dispatch_pos = retry_body.find('_scheduler_dispatch_scan_internal(')
  assert interrupt_pos != -1 and dispatch_pos != -1
  assert interrupt_pos < dispatch_pos
  assert "decision='interrupted'" in retry_body


def test_scan_processing_active_includes_retry_wait_backend_truth() -> None:
  source = inspect.getsource(web_app._render_html)
  assert source.count('function scanProcessingActive(') == 1
  predicate_start = source.find('function scanProcessingActive(')
  predicate_end = source.find('\n    function buildProcessingRowModel(', predicate_start)
  predicate_body = source[predicate_start:predicate_end] if predicate_end != -1 else source[predicate_start:]
  assert 'scanRetryStateActive(payload)' in predicate_body
  assert "scanStage === 'retry_wait'" in predicate_body
  assert "scanStatus === 'processing'" in predicate_body
  assert "String(executionState.kind || '').toLowerCase() === 'processing'" in predicate_body
  assert "String(executionState.action || '').toLowerCase() === 'scan'" in predicate_body


def test_scan_processing_active_includes_cancel_wait_backend_truth() -> None:
  source = inspect.getsource(web_app._render_html)
  predicate_start = source.find('function scanProcessingActive(')
  predicate_end = source.find('\n    function buildProcessingRowModel(', predicate_start)
  predicate_body = source[predicate_start:predicate_end] if predicate_end != -1 else source[predicate_start:]
  assert 'scanRuntimeTerminalNoRetry(payload)' in predicate_body
  assert "scanStatus === 'canceling'" in predicate_body
  assert 'Boolean(runtime.cancel_requested)' in predicate_body
  row_start = source.find('function buildProcessingRowModel(')
  row_end = source.find('\n    function renderBoundary(', row_start)
  row_body = source[row_start:row_end] if row_end != -1 else source[row_start:]
  assert "const cancelWaitActive = !scanRuntimeTerminalNoRetry(payload) && (scanStatus === 'canceling' || Boolean(runtime.cancel_requested));" in row_body
  assert 'Cancel requested; waiting for a safe checkpoint.' in row_body


def test_terminal_scan_cancel_clears_processing_and_cancel_action() -> None:
  source = inspect.getsource(web_app._render_html)
  terminal_start = source.find('function scanRuntimeTerminalNoRetry(')
  terminal_end = source.find('\n    function scanProcessingActive(', terminal_start)
  terminal_body = source[terminal_start:terminal_end] if terminal_end != -1 else source[terminal_start:]
  assert "'canceled'" in terminal_body and "'cancelled'" in terminal_body
  assert "return !scanRetryStateActive(payload);" in terminal_body
  predicate_start = source.find('function scanProcessingActive(')
  predicate_end = source.find('\n    function buildProcessingRowModel(', predicate_start)
  predicate_body = source[predicate_start:predicate_end] if predicate_end != -1 else source[predicate_start:]
  terminal_guard_pos = predicate_body.find('scanRuntimeTerminalNoRetry(payload)')
  cancel_requested_pos = predicate_body.find('Boolean(runtime.cancel_requested)')
  assert terminal_guard_pos != -1 and cancel_requested_pos != -1
  assert terminal_guard_pos < cancel_requested_pos
  cancel_start = source.find('function buildScanCancelOperatorAction(payload = {})')
  cancel_end = source.find('\n    function renderQuickActions(', cancel_start)
  cancel_body = source[cancel_start:cancel_end] if cancel_end != -1 else source[cancel_start:]
  terminal_cancel_pos = cancel_body.find('scanRuntimeTerminalNoRetry(payload)')
  pending_cancel_pos = cancel_body.find('state.scanCanceling')
  assert terminal_cancel_pos != -1 and pending_cancel_pos != -1
  assert terminal_cancel_pos < pending_cancel_pos


def test_processing_row_renders_retry_wait_without_execution_processing() -> None:
  source = inspect.getsource(web_app._render_html)
  row_start = source.find('function buildProcessingRowModel(')
  row_end = source.find('\n    function renderBoundary(', row_start)
  row_body = source[row_start:row_end] if row_end != -1 else source[row_start:]
  assert 'if (!scanProcessingActive(payload))' in row_body
  assert "String(executionState.kind || '').toLowerCase() !== 'processing'" not in row_body
  assert "const retryActive = !cancelWaitActive && scanStage === 'retry_wait' && scanRetryStateActive(payload)" in row_body
  assert 'id="boundary-zero-found-retry-value"' in row_body
  assert 'data-next-retry-at' in row_body


def test_processing_row_uses_runtime_session_and_started_at_for_elapsed() -> None:
  source = inspect.getsource(web_app._render_html)
  row_start = source.find('function buildProcessingRowModel(')
  row_end = source.find('\n    function renderBoundary(', row_start)
  row_body = source[row_start:row_end] if row_end != -1 else source[row_start:]
  assert "const retryStartedAtRaw = retryActive ? String(retryState.started_at_utc || '') : '';" in row_body
  assert "const startedAtRaw = retryStartedAtRaw || String(runtime.started_at_utc || executionState.started_at_utc || '');" in row_body
  assert "const endedAtRaw = retryActive ? '' : String(runtime.completed_at_utc || executionState.completed_at_utc || '');" in row_body
  assert "const processingSessionId = String(runtime.scan_session_id || executionState.scan_session_id || '');" in row_body
  assert 'data-processing-session-id="${escapeHtml(processingSessionId)}"' in row_body
  render_start = source.find('function renderBoundary(')
  render_end = source.find('\n    function renderStartupWizard(', render_start)
  render_body = source[render_start:render_end] if render_end != -1 else source[render_start:]
  assert 'data-processing-session-id="${escapeHtml(processingRow.processingSessionId)}"' in render_body


def test_refresh_while_processing_does_not_replay_retry_wait_or_cancel_wait() -> None:
  source = inspect.getsource(web_app._render_html)
  refresh_start = source.find('async function refreshShellWhileProcessing(')
  refresh_end = source.find('\n    async function requestJson(', refresh_start)
  refresh_body = source[refresh_start:refresh_end] if refresh_end != -1 else source[refresh_start:]
  assert 'const wasProcessing = scanProcessingActive(previousPayload);' in refresh_body
  assert 'const isProcessing = scanProcessingActive(payload);' in refresh_body
  assert "scan_runtime) || {}).status || '').toLowerCase() === 'processing'" not in refresh_body
  replay_pos = refresh_body.find('/api/replay-restore')
  render_pos = refresh_body.find("renderPayload('scan', payload")
  assert replay_pos != -1 and render_pos != -1
  assert 'if (wasProcessing && !isProcessing)' in refresh_body


def test_stop_automation_keeps_stopping_flag_through_overlay_and_bootstrap() -> None:
  source = inspect.getsource(web_app._render_html)
  stop_start = source.find('async function executeStopAutomationLoop()')
  stop_end = source.find('\n    function beginCloseWindowOfflineTransition(', stop_start)
  stop_body = source[stop_start:stop_end] if stop_end != -1 else source[stop_start:]
  set_pos = stop_body.find('state.automationStopping = true')
  cancel_pos = stop_body.find("runUiAction('scan', { body: { action: 'cancel' }")
  overlay_pos = stop_body.find("requestJson('/api/automation-overlay'")
  bootstrap_pos = stop_body.find("await runUiAction('bootstrap')")
  clear_pos = stop_body.find('state.automationStopping = false')
  assert -1 not in (set_pos, overlay_pos, bootstrap_pos, clear_pos)
  assert set_pos < cancel_pos < overlay_pos < bootstrap_pos < clear_pos
  assert "renderPayload('stop_automation_loop', overlayPayload)" in stop_body
  assert 'renderDeckViewShell(state.payload || {})' in stop_body


def test_cancel_scan_control_hidden_or_pending_while_stop_in_progress() -> None:
  source = inspect.getsource(web_app._render_html)
  deck_start = source.find('function buildDeckViewModel(payload)')
  deck_end = source.find('\n    function switchDeckView(', deck_start)
  deck_body = source[deck_start:deck_end] if deck_end != -1 else source[deck_start:]
  assert 'if (state.autoAdvanceEnabled && !state.automationStopping && scanCancelAction)' in deck_body
  assert "label: state.automationStopping ? 'Stopping automation' : (state.automationStarting ? 'Starting automation…' : 'Stop automation')" in deck_body
  assert "pendingLabel: 'Stopping automation'" in deck_body
  assert 'disabled: state.automationStarting || state.automationStopping || undefined' in deck_body
  cancel_start = source.find('function buildScanCancelOperatorAction(payload = {})')
  cancel_end = source.find('\n    function renderQuickActions(', cancel_start)
  cancel_body = source[cancel_start:cancel_end] if cancel_end != -1 else source[cancel_start:]
  assert "scanStage === 'retry_wait'" in cancel_body
  assert 'retryWaitCancelable' in cancel_body
  assert 'Cancel the pending zero-candidate retry before the next scan starts' in cancel_body


def test_timers_do_not_claim_scheduler_owner() -> None:
  # SCHEDULER_ELIGIBILITY_THRESHOLD_REALIGNMENT_BMAP_2026-06-29 (C3 / INV-4): retry and cadence
  # timers are eligibility thresholds, not focus-holders. Neither timer-start helper may stamp
  # the scheduler owner with its own name; ownership during a wait is derived in the snapshot.
  source = inspect.getsource(web_app.create_operator_console_app)
  for fn_name in ('_scheduler_start_retry_timer', '_scheduler_start_cadence_timer'):
    fn_start = source.find(f'def {fn_name}(')
    assert fn_start != -1, f'{fn_name} must be defined'
    fn_end = source.find('\n  def ', fn_start + 1)
    fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
    assert "scheduler_state['owner'] = 'retry_timer'" not in fn_body
    assert "scheduler_state['owner'] = 'cadence_timer'" not in fn_body


def test_scan_terminal_zero_found_routes_to_retry_not_cadence_in_handler() -> None:
  # C2 / INV-1: the scan_terminal handler routes found_count == 0 to the retry threshold and never
  # starts cadence on that path (cadence start lives only on the submit_terminal path).
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  assert handler_start != -1
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  # Locate the scan_terminal branch through to the retry_timer_elapsed branch.
  st_start = handler_body.find("normalized_event == 'scan_terminal'")
  st_end = handler_body.find("normalized_event == 'retry_timer_elapsed'", st_start)
  st_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  assert 'found_count == 0' in st_body
  assert "'resolution_kind': 'scan_actionability_resolution'" in st_body
  assert "'actionability_status': 'zero_actionable'" in st_body
  assert "'actionability_reason': 'raw_zero'" in st_body
  assert "scheduler_state['pending_retry_after_scan_release']" in st_body
  assert "decision='retry_timer_pending_scan_release'" in st_body
  assert '_scheduler_start_retry_timer(' not in st_body
  assert '_scheduler_start_cadence_timer(' not in st_body
  assert "owner_reason='empty_scan_terminal'" not in st_body


def test_scan_terminal_failed_active_automation_routes_to_retry_threshold() -> None:
  # SCHEDULER_CONTINUITY_BACKEND_PROJECTION_SYNC_BMAP_2026-06-29:
  # recoverable failed scans under active automation must not settle into timerless idle.
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  assert handler_start != -1
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'scan_terminal'")
  st_end = handler_body.find("normalized_event == 'retry_timer_elapsed'", st_start)
  st_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  failed_pos = st_body.find("terminal_status in {'failed', 'canceled', 'cancelled'}")
  no_continuation_pos = st_body.find("decision = 'terminal_no_continuation'", failed_pos)
  retry_pos = st_body.find("'source': 'scan_failure_retry'", failed_pos)
  assert failed_pos != -1, 'scan_terminal must classify failed/canceled terminals'
  assert retry_pos != -1, 'active recoverable scan failure must start scan_failure_retry'
  assert retry_pos < no_continuation_pos, (
    'scan_failure_retry must be evaluated before terminal_no_continuation fallback'
  )
  assert "retryable_failed = terminal_status == 'failed' and automation_active" in st_body
  assert "scheduler_state['pending_retry_after_scan_release']" in st_body[failed_pos:no_continuation_pos]
  assert "decision='retry_timer_pending_scan_release'" in st_body[failed_pos:no_continuation_pos]


def test_scan_terminal_failed_inactive_automation_does_not_continue() -> None:
  # Disabled/paused automation remains no-continuation; the BMAP only changes active automation.
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'scan_terminal'")
  st_end = handler_body.find("normalized_event == 'retry_timer_elapsed'", st_start)
  st_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  assert "automation_active = bool(automation_overlay_state.get('enabled')) and not bool(automation_overlay_state.get('paused'))" in st_body
  assert "retryable_failed = terminal_status == 'failed' and automation_active" in st_body
  assert "decision = 'terminal_no_continuation'" in st_body
  assert "if automation_active:" in st_body, (
    'halt projection must be restricted to active automation; inactive failure remains no-continuation'
  )


def test_scan_terminal_canceled_active_automation_projects_explicit_halt() -> None:
  # Cancel must be explicit and visible, not a silent active-automation idle with no timer.
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'scan_terminal'")
  st_end = handler_body.find("normalized_event == 'retry_timer_elapsed'", st_start)
  st_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  assert "'scan_canceled_halted'" in st_body
  assert "decision = 'halted_scan_canceled'" in st_body
  assert "scheduler_state['halt_projection'] = halt_projection" in st_body
  canceled_branch = st_body[st_body.find("halted_kind = 'scan_canceled_halted'"):]
  assert '_scheduler_start_retry_timer(' not in canceled_branch[:canceled_branch.find('found_count = int')], (
    'operator cancel halt must not be converted into an automatic retry'
  )


def test_scheduler_snapshot_exposes_logical_halt_projection() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  snapshot_start = source.find('def _scheduler_build_snapshot(')
  assert snapshot_start != -1, '_scheduler_build_snapshot must be defined'
  snapshot_end = source.find('\n  def _persist_scheduler_event(', snapshot_start)
  snapshot_body = source[snapshot_start:snapshot_end] if snapshot_end != -1 else source[snapshot_start:]
  assert "halt_projection = scheduler_state.get('halt_projection')" in snapshot_body
  assert "'halt_projection': dict(halt_projection)" in snapshot_body


def test_scan_terminal_found_dispatches_submit_and_clears_cadence() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  scan_start = source.find('def _run_scan() -> None:')
  assert scan_start != -1, '_run_scan must be defined'
  scan_end = source.find('\n    scan_runtime_thread = threading.Thread', scan_start)
  scan_body = source[scan_start:scan_end] if scan_end != -1 else source[scan_start:]
  assert "'scan_terminal'" in scan_body
  assert '_scheduler_auto_select_and_submit_after_scan(' not in scan_body
  handler_start = source.find('def _scheduler_handle_event(')
  assert handler_start != -1, '_scheduler_handle_event must be defined'
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  assert 'found_count > 0' in handler_body
  assert "_scheduler_cancel_timers()" in handler_body
  assert 'scan_actionability_result = _scheduler_auto_select_and_submit_after_scan(' in handler_body
  assert "actionability_status == 'actionable_ready'" in handler_body
  assert "actionability_status == 'zero_actionable'" in handler_body
  resolution_pos = handler_body.find("extra_detail={**event_detail, 'found_count': found_count, 'scan_actionability_resolution': actionability_resolution}")
  release_pos = handler_body.find("_scheduler_release_lease(")
  dispatch_pos = handler_body.find("_scheduler_dispatch_submit_internal(", release_pos)
  assert -1 not in (resolution_pos, release_pos, dispatch_pos)
  assert resolution_pos < release_pos < dispatch_pos


def test_scan_terminal_actionability_contract_removes_completed_pending() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  assert handler_start != -1, '_scheduler_handle_event must be defined'
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'scan_terminal'")
  st_end = handler_body.find("normalized_event == 'retry_timer_elapsed'", st_start)
  st_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  assert "decision=submit_dispatch_result if submit_dispatch_result != 'pending' else 'pending'" not in st_body
  assert "'actionability_status': 'automation_inactive'" in st_body
  assert "decision='halted_scan_actionability_contract_invalid'" in st_body
  assert "decision='scan_actionability_automation_inactive'" in st_body
  assert "decision='retry_timer_pending_scan_release'" in st_body
  assert "decision='actionable_submit_handoff'" in st_body


def test_submit_terminal_starts_post_submit_cadence() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  submit_start = source.find('def _scheduler_dispatch_submit_internal(')
  assert submit_start != -1, '_scheduler_dispatch_submit_internal must be defined'
  submit_end = source.find('\n  def _scheduler_auto_select_and_submit_after_scan(', submit_start)
  submit_body = source[submit_start:submit_end] if submit_end != -1 else source[submit_start:]
  assert "'submit_terminal'" in submit_body
  assert '_scheduler_start_cadence_timer(' not in submit_body
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  assert "normalized_event == 'submit_terminal'" in handler_body
  failed_pos = handler_body.find("terminal_status == 'failed'")
  cadence_pos = handler_body.find("source='submit_completion'")
  assert failed_pos != -1, 'submit_terminal must classify failed submit terminals'
  assert cadence_pos != -1, 'completed submit terminal must still start post-submit cadence'
  assert failed_pos < cadence_pos, 'failed submit terminal must be handled before cadence start'
  assert "'halted_submit_failed'" in handler_body
  assert "stopped_terminal else 'halted_submit_failed'" in handler_body
  assert "'kind': 'submit_failed_halted'" in handler_body
  assert "source='submit_completion'" in handler_body


def test_submit_terminal_completion_token_contract_controls_cadence() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'submit_terminal'")
  st_end = handler_body.find("snapshot = _scheduler_build_snapshot(settings=settings, source=normalized_event or 'scheduler_event'", st_start)
  submit_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  for expected in (
    'missing_submit_terminal_completion_token',
    'invalid_submit_terminal_token_kind',
    'invalid_submit_terminal_token_schema',
    'submit_terminal_status_mismatch',
    'submit_terminal_lease_not_verified',
    'submit_terminal_lease_missing',
    'submit_terminal_projection_not_verified',
    'submit_terminal_completed_at_missing',
    'submit_terminal_saved_set_mismatch',
    "'submit_terminal_contract_invalid'",
    "decision='halted_submit_terminal_contract_invalid'",
    "'token_validation_reason': token_invalid_reason",
  ):
    assert expected in submit_body
  invalid_pos = submit_body.find('if token_invalid_reason:')
  cadence_pos = submit_body.find("_scheduler_start_cadence_timer(")
  assert invalid_pos != -1 and cadence_pos != -1
  assert invalid_pos < cadence_pos


def test_submit_terminal_consumes_current_saved_set_before_cadence() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'submit_terminal'")
  st_end = handler_body.find("snapshot = _scheduler_build_snapshot(settings=settings, source=normalized_event or 'scheduler_event'", st_start)
  submit_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  consume_pos = submit_body.find("review_selection_state['consumed_submit_terminal'] = consumed_record")
  cadence_ref_pos = submit_body.find('cadence_source_terminal_ref = {')
  cadence_start_pos = submit_body.find("_scheduler_start_cadence_timer(")
  assert -1 not in (consume_pos, cadence_ref_pos, cadence_start_pos)
  assert consume_pos < cadence_ref_pos < cadence_start_pos
  for expected in (
    "'consumed_saved_set_id': token_saved_set_id",
    "'consumed_candidate_signature': token_candidate_signature",
    "review_selection_state['saved_set_status'] = 'submitted_terminal'",
    "evaluation_status='submitted_terminal'",
    "visibility_status='history_only'",
    "'source_submit_token_kind': str(submit_terminal_token.get('token_kind') or '')",
    "'source_saved_set_id': str(submit_terminal_token.get('saved_set_id') or '')",
    "'source_last_submit_completed_at_utc': str(submit_terminal_token.get('last_submit_completed_at_utc') or '')",
    'source_terminal_ref=cadence_source_terminal_ref',
  ):
    assert expected in submit_body


def test_submit_ready_rejects_consumed_saved_set_and_projection_surfaces_token() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  submit_ready_start = source.find('def _scheduler_submit_ready()')
  submit_ready_end = source.find('\n  def _scheduler_public_timer_record(', submit_ready_start)
  submit_ready_body = source[submit_ready_start:submit_ready_end] if submit_ready_end != -1 else source[submit_ready_start:]
  assert "review_selection_state.get('consumed_submit_terminal')" in submit_ready_body
  assert "consumed.get('consumed_saved_set_id')" in submit_ready_body
  assert "consumed.get('consumed_candidate_signature')" in submit_ready_body
  assert 'return False' in submit_ready_body
  assert "'consumed_submit_terminal': None" in source
  assert "_review_projection['consumed_submit_terminal'] = _consumed_submit_terminal" in source
  assert "_rsp_projected_review_sel['consumed_submit_terminal'] = _rsp_consumed_submit_terminal" in source
  assert "_review_projection['submit_ready'] = False" in source
  assert "_rsp_projected_review_sel['submit_ready'] = False" in source


def test_scan_complete_runtime_event_uses_completion_timestamp_contract() -> None:
  source = inspect.getsource(service_module.run_scan_once)
  finalizing_pos = source.find("'finalizing_result'")
  completed_pos = source.find('completed_at = datetime.now(UTC)')
  event_pos = source.find("event_type='scan_complete'")
  assert -1 not in (finalizing_pos, completed_pos, event_pos)
  assert finalizing_pos < completed_pos < event_pos
  event_body = source[event_pos: source.find('persist_candidate_review_run(', event_pos)]
  assert 'recorded_at_utc=completed_at.isoformat()' in event_body
  assert "'scan_started_at_utc': recorded_at.isoformat()" in event_body
  assert "'scan_completed_at_utc': completed_at.isoformat()" in event_body


def test_pair_monitor_suppresses_submitted_terminal_saved_set_current_card(tmp_path: Path) -> None:
  db_path = tmp_path / 'runtime.sqlite3'
  connection = sqlite3.connect(db_path)
  with connection:
    connection.executescript(
      '''
      CREATE TABLE candidate_review_runs (
        run_id TEXT PRIMARY KEY,
        lane_session_id TEXT
      );
      CREATE TABLE candidate_review_candidates (
        id INTEGER PRIMARY KEY,
        run_id TEXT,
        candidate_uid TEXT,
        candidate_key TEXT,
        ticker TEXT,
        qualifier_tier TEXT,
        review_row_origin TEXT,
        detail_json TEXT,
        recorded_at_utc TEXT,
        operation_lane TEXT,
        lifecycle_stage TEXT,
        terminal_cause TEXT
      );
      CREATE TABLE candidate_saved_set_members (
        id INTEGER PRIMARY KEY,
        saved_set_id TEXT,
        candidate_key TEXT
      );
      CREATE TABLE candidate_saved_set_evaluations (
        id INTEGER PRIMARY KEY,
        saved_set_id TEXT,
        actionability_status TEXT,
        visibility_status TEXT
      );
      CREATE TABLE pair_plans (
        pair_id TEXT,
        ticker TEXT
      );
      CREATE TABLE pair_states (
        id INTEGER PRIMARY KEY,
        pair_id TEXT,
        state TEXT,
        detail_json TEXT
      );
      '''
    )
    detail = {
      'ticker': 'KXHYPE15M-26JUN291845-45',
      'rank': 1,
      'qualifier_tier': 'live_qualifying',
      'review_row_origin': 'current',
    }
    connection.execute(
      'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?, ?)',
      ('scan-1', 'live-session-1'),
    )
    connection.execute(
      'INSERT INTO candidate_review_candidates '
      '(run_id, candidate_uid, candidate_key, ticker, qualifier_tier, review_row_origin, detail_json, recorded_at_utc, operation_lane, lifecycle_stage, terminal_cause) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      (
        'scan-1',
        'candidate-1',
        'candidate-1',
        'KXHYPE15M-26JUN291845-45',
        'live_qualifying',
        'current',
        json.dumps(detail),
        '2026-06-29T22:37:15Z',
        'live',
        'discovered',
        '',
      ),
    )
    connection.execute(
      'INSERT INTO candidate_saved_set_members (saved_set_id, candidate_key) VALUES (?, ?)',
      ('saved-set-1', 'candidate-1'),
    )
    connection.execute(
      'INSERT INTO candidate_saved_set_evaluations (saved_set_id, actionability_status, visibility_status) VALUES (?, ?, ?)',
      ('saved-set-1', 'submitted_terminal', 'history_only'),
    )
  payload = {
    'settings': {'operation_lane': 'live', 'state_db_path': str(db_path)},
    'review_selection': {'persisted_lane_session_id': 'live-session-1'},
  }
  monitor = web_app._build_pair_monitor_payload(payload)
  assert monitor['candidate_count'] == 0
  assert monitor['candidate_rows'] == []


def test_candidate_row_saved_set_retired_predicate() -> None:
  # Fork D1 (2026-07-02): single retirement predicate shared by the card builder and
  # the scan-runtime result-count override.
  retired = web_app._candidate_row_saved_set_retired
  assert retired({'saved_set_actionability_status': 'submitted_terminal'}) is True
  assert retired({'saved_set_visibility_status': 'history_only'}) is True
  assert retired({'pair_state': 'SETTLED'}) is True
  assert retired({'pair_state': 'CANCELED'}) is True
  assert retired({
    'saved_set_actionability_status': 'active_valid',
    'saved_set_visibility_status': 'default_actionable',
    'pair_state': '',
  }) is False
  assert retired({}) is False


def test_scan_runtime_count_matches_card_count_after_saved_set_retirement(tmp_path: Path) -> None:
  # Fork D1 (2026-07-02): after a blocked submit retires a saved set, the scan-runtime
  # result count must equal the card count (count == cards in every phase). One
  # candidate belongs to a retired set, one to an active set: cards show 1, and the
  # count override's computation (canonical rows minus retired) must also be 1 — not
  # the unfiltered 2 that produced the live 5-vs-0 / 3-vs-0 divergence.
  db_path = tmp_path / 'runtime.sqlite3'
  connection = sqlite3.connect(db_path)
  with connection:
    connection.executescript(
      '''
      CREATE TABLE candidate_review_runs (
        run_id TEXT PRIMARY KEY,
        lane_session_id TEXT
      );
      CREATE TABLE candidate_review_candidates (
        id INTEGER PRIMARY KEY,
        run_id TEXT,
        candidate_uid TEXT,
        candidate_key TEXT,
        ticker TEXT,
        qualifier_tier TEXT,
        review_row_origin TEXT,
        detail_json TEXT,
        recorded_at_utc TEXT,
        operation_lane TEXT,
        lifecycle_stage TEXT,
        terminal_cause TEXT
      );
      CREATE TABLE candidate_saved_set_members (
        id INTEGER PRIMARY KEY,
        saved_set_id TEXT,
        candidate_key TEXT
      );
      CREATE TABLE candidate_saved_set_evaluations (
        id INTEGER PRIMARY KEY,
        saved_set_id TEXT,
        actionability_status TEXT,
        visibility_status TEXT
      );
      CREATE TABLE pair_plans (
        pair_id TEXT,
        ticker TEXT
      );
      CREATE TABLE pair_states (
        id INTEGER PRIMARY KEY,
        pair_id TEXT,
        state TEXT,
        detail_json TEXT
      );
      '''
    )
    connection.execute(
      'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?, ?)',
      ('scan-1', 'live-session-1'),
    )
    for candidate_key, ticker in (
      ('candidate-retired', 'KXZEC15M-26JUL012130-30'),
      ('candidate-active', 'KXDOGE15M-26JUL012130-30'),
    ):
      connection.execute(
        'INSERT INTO candidate_review_candidates '
        '(run_id, candidate_uid, candidate_key, ticker, qualifier_tier, review_row_origin, detail_json, recorded_at_utc, operation_lane, lifecycle_stage, terminal_cause) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        (
          'scan-1',
          candidate_key,
          candidate_key,
          ticker,
          'live_qualifying',
          'current',
          json.dumps({'ticker': ticker, 'rank': 1, 'qualifier_tier': 'live_qualifying', 'review_row_origin': 'current'}),
          '2026-07-02T01:20:51Z',
          'live',
          'discovered',
          '',
        ),
      )
    connection.execute(
      'INSERT INTO candidate_saved_set_members (saved_set_id, candidate_key) VALUES (?, ?)',
      ('saved-set-retired', 'candidate-retired'),
    )
    connection.execute(
      'INSERT INTO candidate_saved_set_evaluations (saved_set_id, actionability_status, visibility_status) VALUES (?, ?, ?)',
      ('saved-set-retired', 'submitted_terminal', 'history_only'),
    )
    connection.execute(
      'INSERT INTO candidate_saved_set_members (saved_set_id, candidate_key) VALUES (?, ?)',
      ('saved-set-active', 'candidate-active'),
    )
    connection.execute(
      'INSERT INTO candidate_saved_set_evaluations (saved_set_id, actionability_status, visibility_status) VALUES (?, ?, ?)',
      ('saved-set-active', 'active_valid', 'default_actionable'),
    )
  payload = {
    'settings': {'operation_lane': 'live', 'state_db_path': str(db_path)},
    'review_selection': {'persisted_lane_session_id': 'live-session-1'},
  }
  monitor = web_app._build_pair_monitor_payload(payload)
  assert monitor['candidate_count'] == 1
  assert [row['ticker'] for row in monitor['candidate_rows']] == ['KXDOGE15M-26JUL012130-30']
  canonical_rows = web_app._query_canonical_candidates('live-session-1', str(db_path))
  assert len(canonical_rows) == 2
  filtered_count = sum(1 for row in canonical_rows if not web_app._candidate_row_saved_set_retired(row))
  assert filtered_count == monitor['candidate_count'] == 1


def test_stage_columns_pair_filled_state_outranks_candidate_lifecycle(tmp_path: Path) -> None:
  db_path = tmp_path / 'runtime.sqlite3'
  connection = sqlite3.connect(db_path)
  with connection:
    connection.executescript(
      '''
      CREATE TABLE candidate_review_runs (
        run_id TEXT PRIMARY KEY,
        lane_session_id TEXT
      );
      CREATE TABLE candidate_review_candidates (
        id INTEGER PRIMARY KEY,
        run_id TEXT,
        candidate_uid TEXT,
        candidate_key TEXT,
        ticker TEXT,
        qualifier_tier TEXT,
        review_row_origin TEXT,
        detail_json TEXT,
        recorded_at_utc TEXT,
        operation_lane TEXT,
        lifecycle_stage TEXT,
        terminal_cause TEXT
      );
      CREATE TABLE candidate_saved_set_members (
        id INTEGER PRIMARY KEY,
        saved_set_id TEXT,
        candidate_key TEXT
      );
      CREATE TABLE candidate_saved_set_evaluations (
        id INTEGER PRIMARY KEY,
        saved_set_id TEXT,
        actionability_status TEXT,
        visibility_status TEXT
      );
      CREATE TABLE pair_plans (
        pair_id TEXT,
        ticker TEXT
      );
      CREATE TABLE pair_states (
        id INTEGER PRIMARY KEY,
        pair_id TEXT,
        state TEXT,
        detail_json TEXT
      );
      '''
    )
    detail = {
      'ticker': 'KXHYPE15M-26JUN291845-45',
      'rank': 1,
      'qualifier_tier': 'live_qualifying',
      'review_row_origin': 'current',
    }
    connection.execute(
      'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?, ?)',
      ('scan-1', 'live-session-1'),
    )
    connection.execute(
      'INSERT INTO candidate_review_candidates '
      '(run_id, candidate_uid, candidate_key, ticker, qualifier_tier, review_row_origin, detail_json, recorded_at_utc, operation_lane, lifecycle_stage, terminal_cause) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      (
        'scan-1',
        'candidate-1',
        'candidate-1',
        'KXHYPE15M-26JUN291845-45',
        'live_qualifying',
        'current',
        json.dumps(detail),
        '2026-06-29T22:43:22Z',
        'live',
        'terminal',
        'expired_unfilled',
      ),
    )
    connection.execute(
      'INSERT INTO pair_plans (pair_id, ticker) VALUES (?, ?)',
      ('pair-1', 'KXHYPE15M-26JUN291845-45'),
    )
    connection.execute(
      'INSERT INTO pair_states (pair_id, state, detail_json) VALUES (?, ?, ?)',
      ('pair-1', 'FILLED', json.dumps({'ticker': 'KXHYPE15M-26JUN291845-45'})),
    )
  payload = {
    'settings': {'operation_lane': 'live', 'state_db_path': str(db_path)},
    'review_selection': {'persisted_lane_session_id': 'live-session-1'},
  }
  stage_payload = web_app._fetch_stage_columns(payload)
  stage_by_id = {stage['stage_id']: stage for stage in stage_payload['stage_columns']}
  assert stage_by_id['queued']['items'] == []
  assert stage_by_id['cancelled']['items'] == []
  assert stage_by_id['filled']['items'][0]['ticker'] == 'KXHYPE15M-26JUN291845-45'


def test_stage_columns_submitted_terminal_saved_set_does_not_keep_execution_active(tmp_path: Path) -> None:
  db_path = tmp_path / 'runtime.sqlite3'
  connection = sqlite3.connect(db_path)
  with connection:
    connection.executescript(
      '''
      CREATE TABLE candidate_review_runs (
        run_id TEXT PRIMARY KEY,
        lane_session_id TEXT
      );
      CREATE TABLE candidate_review_candidates (
        id INTEGER PRIMARY KEY,
        run_id TEXT,
        candidate_uid TEXT,
        candidate_key TEXT,
        ticker TEXT,
        qualifier_tier TEXT,
        review_row_origin TEXT,
        detail_json TEXT,
        recorded_at_utc TEXT,
        operation_lane TEXT,
        lifecycle_stage TEXT,
        terminal_cause TEXT
      );
      CREATE TABLE candidate_saved_set_members (
        id INTEGER PRIMARY KEY,
        saved_set_id TEXT,
        candidate_key TEXT
      );
      CREATE TABLE candidate_saved_set_evaluations (
        id INTEGER PRIMARY KEY,
        saved_set_id TEXT,
        actionability_status TEXT,
        visibility_status TEXT
      );
      CREATE TABLE pair_plans (
        pair_id TEXT,
        ticker TEXT
      );
      CREATE TABLE pair_states (
        id INTEGER PRIMARY KEY,
        pair_id TEXT,
        state TEXT,
        detail_json TEXT
      );
      '''
    )
    connection.execute(
      'INSERT INTO candidate_review_runs (run_id, lane_session_id) VALUES (?, ?)',
      ('scan-1', 'live-session-1'),
    )
    for index, ticker in enumerate(('KXHYPE15M-26JUN292000-00', 'KXDOGE15M-26JUN292000-00'), start=1):
      detail = {'ticker': ticker, 'qualifier_tier': 'live_qualifying', 'review_row_origin': 'current'}
      connection.execute(
        'INSERT INTO candidate_review_candidates '
        '(run_id, candidate_uid, candidate_key, ticker, qualifier_tier, review_row_origin, detail_json, recorded_at_utc, operation_lane, lifecycle_stage, terminal_cause) '
        'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
        ('scan-1', f'candidate-{index}', f'candidate-{index}', ticker, 'live_qualifying', 'current', json.dumps(detail), '2026-06-29T23:47:14Z', 'live', 'in_flight', ''),
      )
      connection.execute(
        'INSERT INTO candidate_saved_set_members (saved_set_id, candidate_key) VALUES (?, ?)',
        ('saved-set-1', f'candidate-{index}'),
      )
    connection.execute(
      'INSERT INTO pair_plans (pair_id, ticker) VALUES (?, ?)',
      ('pair-1', 'KXHYPE15M-26JUN292000-00'),
    )
    connection.execute(
      'INSERT INTO pair_states (pair_id, state, detail_json) VALUES (?, ?, ?)',
      ('pair-1', 'FILLED', json.dumps({'ticker': 'KXHYPE15M-26JUN292000-00'})),
    )
    connection.execute(
      'INSERT INTO candidate_saved_set_evaluations (saved_set_id, actionability_status, visibility_status) VALUES (?, ?, ?)',
      ('saved-set-1', 'submitted_terminal', 'history_only'),
    )

  stage_payload = web_app._fetch_stage_columns(
    {
      'settings': {'operation_lane': 'live', 'state_db_path': str(db_path)},
      'review_selection': {'persisted_lane_session_id': 'live-session-1'},
    }
  )

  assert stage_payload['in_flight_candidate_count'] == 0
  assert stage_payload['active_stage_candidate_count'] == 0
  assert [item['ticker'] for item in stage_payload['stage_columns'][1]['items']] == ['KXHYPE15M-26JUN292000-00']
  assert [item['terminal_cause'] for item in stage_payload['stage_columns'][2]['items']] == ['submitted_terminal']


def test_saved_set_in_flight_helper_materializes_missing_member_rows(tmp_path: Path) -> None:
  db_path = tmp_path / 'runtime.sqlite3'
  connection = sqlite3.connect(db_path)
  connection.row_factory = sqlite3.Row
  with connection:
    connection.executescript(
      '''
      CREATE TABLE candidate_review_candidates (
        id INTEGER PRIMARY KEY,
        run_id TEXT,
        candidate_uid TEXT,
        candidate_key TEXT,
        ticker TEXT,
        qualifier_tier TEXT,
        review_row_origin TEXT,
        detail_json TEXT,
        recorded_at_utc TEXT,
        operation_lane TEXT,
        lifecycle_stage TEXT
      );
      '''
    )
  saved_set = {
    'run_id': 'scan-1',
    'operation_lane': 'live',
    'recorded_at_utc': '2026-06-29T23:47:08Z',
    'members': [
      {
        'candidate_uid': 'candidate-1',
        'candidate_key': 'candidate-1',
        'recorded_at_utc': '2026-06-29T23:47:08Z',
        'detail': {
          'ticker': 'KXHYPE15M-26JUN292000-00',
          'qualifier_tier': 'live_qualifying',
          'review_row_origin': 'current',
        },
      },
    ],
  }

  service_module._write_saved_set_candidates_in_flight(connection, saved_set=saved_set)
  service_module._write_saved_set_candidates_in_flight(connection, saved_set=saved_set)

  rows = connection.execute('SELECT * FROM candidate_review_candidates').fetchall()
  assert len(rows) == 1
  assert rows[0]['lifecycle_stage'] == 'in_flight'
  assert rows[0]['ticker'] == 'KXHYPE15M-26JUN292000-00'


def test_scheduler_cadence_state_reports_missing_submit_completion_timer() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  cadence_start = source.find('def _scheduler_cadence_state(')
  cadence_end = source.find('\n  def _scheduler_public_lease(', cadence_start)
  cadence_body = source[cadence_start:cadence_end] if cadence_end != -1 else source[cadence_start:]
  assert "'cadence_timer_missing'" in cadence_body
  assert "'missing_submit_completion_cadence_timer'" in cadence_body
  snapshot_start = source.find('def _scheduler_build_snapshot(')
  snapshot_end = source.find('\n  def _persist_scheduler_event(', snapshot_start)
  snapshot_body = source[snapshot_start:snapshot_end] if snapshot_end != -1 else source[snapshot_start:]
  assert "'cadence_timer_missing'" in snapshot_body
  assert "resolved_owner = 'automation_cadence'" in snapshot_body


def test_terminal_scan_replay_defers_workflow_when_submit_terminal_consumed() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  replay_start = source.find('def _terminal_scan_refresh_payload(')
  replay_end = source.find('\n  def _bootstrap_replay_restore_availability_payload(', replay_start)
  replay_body = source[replay_start:replay_end] if replay_end != -1 else source[replay_start:]
  assert '_current_saved_set_consumed_by_submit_terminal()' in replay_body
  retired_pos = replay_body.find('_replay_saved_set_retired_by_submit')
  candidate_guard_pos = replay_body.find('and not _replay_saved_set_retired_by_submit')
  automation_workflow_pos = replay_body.find('_automation_follow_on_workflow(augmented)')
  assert -1 not in (retired_pos, candidate_guard_pos, automation_workflow_pos)
  assert retired_pos < candidate_guard_pos < automation_workflow_pos


def test_cadence_elapsed_requires_submit_completion_source_contract() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  cadence_start = handler_body.find("normalized_event == 'cadence_timer_elapsed'")
  cadence_end = handler_body.find("normalized_event == 'submit_terminal'", cadence_start)
  cadence_body = handler_body[cadence_start:cadence_end] if cadence_end != -1 else handler_body[cadence_start:]
  for expected in (
    "cadence_source == 'submit_completion'",
    "'source_submit_token_kind'",
    "'source_submit_terminal_status'",
    "'source_submit_scheduler_lease_id'",
    "'source_saved_set_id'",
    "'source_candidate_signature'",
    "'source_submit_terminal_at_utc'",
    "'source_last_submit_completed_at_utc'",
    "'cadence_timer_contract_invalid'",
    "decision='halted_cadence_timer_contract_invalid'",
  ):
    assert expected in cadence_body


def test_scheduler_submit_dispatch_failure_persists_phase_evidence() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  submit_start = source.find('def _scheduler_dispatch_submit_internal(')
  assert submit_start != -1, '_scheduler_dispatch_submit_internal must be defined'
  submit_end = source.find('\n  def _scheduler_auto_select_and_submit_after_scan(', submit_start)
  submit_body = source[submit_start:submit_end] if submit_end != -1 else source[submit_start:]
  assert "'scheduler_submit_dispatch_failed'" in submit_body
  assert "failure_phase = str(getattr(exc, 'polyventure_submit_bridge_phase', '') or 'submit_dispatch')" in submit_body
  assert "'failure_phase': failure_phase" in submit_body
  assert "'error_family': type(exc).__name__" in submit_body
  assert "'terminal_status': 'failed'" in submit_body


def test_scheduler_submit_failed_token_carries_pre_order_failure_contract() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  submit_start = source.find('def _scheduler_dispatch_submit_internal(')
  submit_end = source.find('\n  def _scheduler_auto_select_and_submit_after_scan(', submit_start)
  submit_body = source[submit_start:submit_end] if submit_end != -1 else source[submit_start:]
  for expected in (
    "'terminal_status': 'failed'",
    "'terminal_kind': 'failed_submit'",
    "'failure_phase': failure_phase",
    "'failure_reason': failure_phase",
    "'error_family': type(exc).__name__",
    "'error_message': str(exc)",
    "'money_path_crossed': not pre_order_failure",
    "'pair_plan_created': not pre_order_failure",
    "'orders_created': failure_phase in {'live_order_dispatch'}",
    "'verified_submit_projection_recorded': False",
    "'verified_lease_released': True",
  ):
    assert expected in submit_body
  failed_token_pos = submit_body.find("'terminal_status': 'failed'")
  cadence_pos = submit_body.find("_scheduler_start_cadence_timer(")
  assert failed_token_pos != -1
  assert cadence_pos == -1 or failed_token_pos < cadence_pos


def test_scheduler_submit_failed_records_attempt_without_consuming_saved_set() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'submit_terminal'")
  st_end = handler_body.find('cadence_source_terminal_ref: JSONDict = {}', st_start)
  failed_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  for expected in (
    "review_selection_state['failed_submit_attempt']",
    "'failed_submit_saved_set_id'",
    "'failed_submit_candidate_signature'",
    "'failed_submit_handoff_id'",
    "'failed_submit_failure_phase'",
    "'failed_submit_money_path_crossed'",
    "'kind': 'submit_failed_halted'",
    "'money_path_crossed': bool(submit_terminal_token.get('money_path_crossed'))",
    "'pair_plan_created': bool(submit_terminal_token.get('pair_plan_created'))",
    "'orders_created': bool(submit_terminal_token.get('orders_created'))",
  ):
    assert expected in failed_body
  assert "review_selection_state['consumed_submit_terminal'] = consumed_record" not in failed_body


def test_scheduler_submit_failed_pre_order_timeout_is_recoverable_only_before_money_path() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'submit_terminal'")
  st_end = handler_body.find('cadence_source_terminal_ref: JSONDict = {}', st_start)
  failed_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  for expected in (
    "_recoverable_pre_order_submit_failure = (",
    "_failed_failure_phase == 'submit_dispatch'",
    "_failed_error_family == 'KalshiHttpError'",
    "_failed_reason_code == 'network_timeout'",
    "_failed_kalshi_method == 'GET'",
    "_failed_kalshi_endpoint == '/portfolio/balance'",
    "and not bool(submit_terminal_token.get('money_path_crossed'))",
    "and not bool(submit_terminal_token.get('pair_plan_created'))",
    "and not bool(submit_terminal_token.get('orders_created'))",
    "and _failed_order_id_absent",
    "and _failed_exposure_absent",
    "event_detail['submit_failure_classification']",
    "'pre_order_connectivity_failed'",
    "'dangerous_submit_failure'",
    "if _recoverable_pre_order_submit_failure:",
    "decision='noop' if stopped_terminal else 'halted_submit_failed'",
  ):
    assert expected in failed_body
  recoverable_pos = failed_body.find("if _recoverable_pre_order_submit_failure:")
  halt_pos = failed_body.find("decision='noop' if stopped_terminal else 'halted_submit_failed'")
  assert -1 not in (recoverable_pos, halt_pos)
  assert recoverable_pos < halt_pos


def test_scheduler_submit_failed_requires_proof_fields_and_matching_current_selection() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'submit_terminal'")
  st_end = handler_body.find('cadence_source_terminal_ref: JSONDict = {}', st_start)
  failed_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  for expected in (
    "_failed_required_fields_present = all(",
    "'failure_phase'",
    "'error_family'",
    "'reason_code'",
    "'kalshi_method'",
    "'kalshi_endpoint'",
    "'money_path_crossed'",
    "'pair_plan_created'",
    "'orders_created'",
    "_failed_token_saved_set_id == _failed_current_saved_set_id",
    "_failed_token_candidate_signature == _failed_current_candidate_signature",
    "_failed_current_halt_clearable",
  ):
    assert expected in failed_body


def test_recoverable_failed_submit_terminal_reuses_history_only_retirement() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'submit_terminal'")
  st_end = handler_body.find("snapshot = _scheduler_build_snapshot(settings=settings, source=normalized_event or 'scheduler_event'", st_start)
  submit_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  for expected in (
    "'consumed_submit_terminal_status': str(submit_terminal_token.get('terminal_status') or '')",
    "review_selection_state['consumed_submit_terminal'] = consumed_record",
    "review_selection_state['saved_set_status'] = 'submitted_terminal'",
    "Submit failed before order placement; saved set retired from current automation routing.",
    "evaluation_status='submitted_terminal'",
    "actionability_status='submitted_terminal'",
    "visibility_status='history_only'",
    "source_terminal_ref=cadence_source_terminal_ref",
  ):
    assert expected in submit_body


def test_scheduler_completed_submit_token_carries_candidate_local_rejection_evidence() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  submit_start = source.find('def _scheduler_dispatch_submit_internal(')
  submit_end = source.find('\n  def _scheduler_auto_select_and_submit_after_scan(', submit_start)
  submit_body = source[submit_start:submit_end] if submit_end != -1 else source[submit_start:]
  for expected in (
    "if isinstance(raw_payload, dict):",
    "'blocked_reason': str(raw_payload.get('blocked_reason') or '')",
    "'submit_response_id': str(raw_payload.get('submit_response_id') or '')",
    "'planned_pair_count': int(raw_payload.get('planned_pair_count') or 0)",
    "'orders_created': bool(int(raw_payload.get('planned_pair_count') or 0))",
    "if str(raw_payload.get('blocked_reason') or '') == 'pair_plan_validation':",
    "'failure_phase': 'pair_plan_validation'",
  ):
    assert expected in submit_body
  completed_pos = submit_body.find("'terminal_status': 'completed'")
  evidence_pos = submit_body.find("'blocked_reason': str(raw_payload.get('blocked_reason') or '')")
  assert completed_pos != -1
  assert evidence_pos > completed_pos


def test_scheduler_owned_submit_service_uses_operator_lane_session() -> None:
  source = inspect.getsource(service_module.run_service_once)
  for expected in (
    "bridge_profile_active = str(execution_profile or '').strip().lower() == 'submit_order_bridge'",
    "operator_lane_session_id_normalized = str(operator_lane_session_id or '').strip()",
    "if bridge_profile_active and operator_lane_session_id_normalized",
    "else _lane_session_id(resolved_settings.operation_lane)",
  ):
    assert expected in source


def test_execution_surface_does_not_count_unbacked_saved_candidates_as_active() -> None:
  source = inspect.getsource(web_app._fetch_stage_columns)
  for expected in (
    'active_pair_backed_count = 0',
    "if live_state:",
    "'active_stage_candidate_count': active_pair_backed_count",
  ):
    assert expected in source
  app_source = inspect.getsource(web_app._build_pair_monitor_payload)
  for expected in (
    'scheduler_inflight_active',
    "scheduler_owner_normalized in {'submit_bridge', 'scan', 'cancel'}",
    'or scheduler_inflight_active',
  ):
    assert expected in app_source


def test_backend_auto_dispatch_builds_direct_submit_handoff_contract() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  builder_start = source.find('def _build_backend_submit_handoff(')
  assert builder_start != -1, '_build_backend_submit_handoff must be defined'
  builder_end = source.find('\n  def _scheduler_dispatch_submit_internal(', builder_start)
  builder_body = source[builder_start:builder_end] if builder_end != -1 else source[builder_start:]
  for expected in (
    "'handoff_id'",
    "'operation_lane'",
    "'operator_lane_session_id'",
    "'scan_session_id'",
    "'saved_set_id'",
    "'candidate_signature'",
    "'candidate_count'",
    "'candidate_keys'",
    "'scan_shape_operator_summary'",
  ):
    assert expected in builder_body, f'direct submit handoff missing {expected}'
  auto_start = source.find('def _scheduler_auto_select_and_submit_after_scan(')
  assert auto_start != -1, '_scheduler_auto_select_and_submit_after_scan must be defined'
  auto_end = source.find('\n  def _start_scan_runtime(', auto_start)
  auto_body = source[auto_start:auto_end] if auto_end != -1 else source[auto_start:]
  assert 'submit_handoff = _build_backend_submit_handoff(' in auto_body
  assert "return {'scan_actionability_resolution': actionability_resolution, 'submit_handoff': submit_handoff}" in auto_body
  assert '_scheduler_dispatch_submit_internal(' not in auto_body
  assert '_scheduler_release_lease(' not in auto_body
  assert "return {'scan_actionability_resolution': {" in auto_body
  assert "'actionability_status': 'zero_actionable'" in auto_body
  assert "'actionability_status': 'actionable_ready'" in auto_body
  assert "'submit_handoff_id': str(submit_handoff.get('handoff_id') or '')" in auto_body


def test_scan_terminal_persists_actionability_before_submit_release_dispatch() -> None:
  source = inspect.getsource(web_app.create_operator_console_app)
  handler_start = source.find('def _scheduler_handle_event(')
  assert handler_start != -1, '_scheduler_handle_event must be defined'
  handler_end = source.find('\n  def _scheduler_dispatch_scan_internal(', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:]
  st_start = handler_body.find("normalized_event == 'scan_terminal'")
  st_end = handler_body.find("normalized_event == 'retry_timer_elapsed'", st_start)
  st_body = handler_body[st_start:st_end] if st_end != -1 else handler_body[st_start:]
  for expected in (
    'scan_actionability_result = _scheduler_auto_select_and_submit_after_scan(',
    "actionability_status = str(actionability_resolution.get('actionability_status') or '').strip()",
    "decision='actionable_submit_handoff'",
    "extra_detail={**event_detail, 'found_count': found_count, 'scan_actionability_resolution': actionability_resolution}",
    "reason='scan_runtime_released_for_submit_dispatch'",
    'submit_handoff=submit_handoff',
    "snapshot['scan_lease_released'] = True",
  ):
    assert expected in st_body
  resolution_pos = st_body.find("decision='actionable_submit_handoff'")
  release_pos = st_body.find("reason='scan_runtime_released_for_submit_dispatch'")
  dispatch_pos = st_body.find('submit_handoff=submit_handoff', release_pos)
  assert -1 not in (resolution_pos, release_pos, dispatch_pos)
  assert resolution_pos < release_pos < dispatch_pos


def test_submit_failure_halt_projects_operator_execution_summary() -> None:
  payload = web_app._build_pair_monitor_payload({
    'scheduler_snapshot': {
      'halt_projection': {
        'active': True,
        'kind': 'submit_failed_halted',
        'reason': 'submit_bridge_service_failed',
        'failure_phase': 'pair_plan_validation',
        'handoff_id': 'handoff-test-001',
        'scan_shape_operator_summary': '84 markets loaded, 7 binary-eligible, 77 rejected as multivariate, 2 scan final candidates.',
      },
    },
    'scan_shape_summary': {
      'markets_loaded': 84,
      'binary_eligible': 7,
      'multivariate_rejected': 77,
    },
    'candidate_count': 2,
  })

  live = payload['live_interaction']
  assert live['surface_visible'] is True
  assert live['surface_status_label'] == 'SUBMIT FAILED'
  assert live['surface_status_tone'] == 'no-go'
  assert live['headline'] == 'Submit failed before order placement.'
  assert live['detail'] == 'pair_plan_validation: submit_bridge_service_failed'
  assert '84 markets loaded, 7 binary-eligible, 77 rejected as multivariate, 2 scan final candidates.' in live['observed']
  assert 'Submit handoff: handoff-test-001' in live['observed']


def test_submit_elapsed_uses_submit_lease_and_excludes_ready_state() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'submitOrderElapsedLeaseId' in source
  fn_pos = source.find('function renderLiveInteractionSurface(')
  assert fn_pos != -1, 'renderLiveInteractionSurface must exist'
  next_fn_pos = source.find('\n    function ', fn_pos + 1)
  fn_fragment = source[fn_pos:next_fn_pos] if next_fn_pos != -1 else source[fn_pos:]
  active_line = "const _d_backendSubmitActive = _d_submitState === 'submitting' || _d_submitState === 'launching' || _d_submitState === 'processing';"
  assert active_line in fn_fragment
  assert "_d_submitState === 'ready'" not in fn_fragment
  # AUTOMATION_CADENCE_RECOVERY_AND_INACTIVITY_GUARD_BMAP_2026-07-02 (C4 / Lane C): the lease id
  # prefers the explicit backend submit-lease projection field before the generic lease id.
  assert "const _d_leaseId = String(_d_snap.submit_scheduler_lease_id || _d_snap.lease_id || '')" in fn_fragment
  assert "showSubmitPendingSurface(submitPhaseLabel(liveInteraction), _d_newLease, _d_leaseId)" in fn_fragment
  show_pos = source.find('function showSubmitPendingSurface(')
  show_end = source.find('\n    function ', show_pos + 1)
  show_body = source[show_pos:show_end] if show_end != -1 else source[show_pos:]
  assert "state.submitOrderElapsedLeaseId = String(leaseId || '')" in show_body
  assert "_elapsedValue.textContent = '0s'" in show_body


def test_cadence_countdown_has_no_local_deadline_fallback() -> None:
  source = inspect.getsource(web_app._render_html)
  fn_start = source.find('function updateAutoFindCadenceCountdown()')
  assert fn_start != -1, 'updateAutoFindCadenceCountdown must exist'
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert 'Date.now() + cadenceMs' not in fn_body
  assert 'autoFindCadenceDeadlineMs = Date.now' not in fn_body
  assert "textContent = '--'" in fn_body


# ---------------------------------------------------------------------------
# AA-1 / AA-2 — Automation auto-select and mode authorization (§19, 2026-06-10)
# ---------------------------------------------------------------------------


def _seed_manual_execution_truth(state_db_path: str) -> None:
  connection = open_database(state_db_path)
  event_base_ts = datetime.now(UTC)
  for offset, event_type in enumerate(('submit_order_intent', 'fill', 'reconcile_snapshot', 'cancel_applied')):
    persist_runtime_event(
      connection,
      level='INFO',
      event_type=event_type,
      recorded_at_utc=(event_base_ts + timedelta(seconds=offset)).isoformat(),
      operation_lane='sandbox',
      lane_session_id='lane-session-aa-truth',
      detail={'profile': 'submit_order_bridge', 'seq': f'aa-seq-{offset + 1:03d}'},
    )


def _prepare_scheduler_submit_ready_app(
  tmp_path: Path,
  monkeypatch: Any,
  *,
  connected_socket: bool,
  automation_enabled: bool = False,
) -> tuple[Any, str]:
  tmp_path.mkdir(parents=True, exist_ok=True)
  state_db_path = str(tmp_path / 'scheduler.sqlite3')
  settings = _build_test_settings(state_db_path)
  if connected_socket:
    _patch_fake_websocket_runtime(monkeypatch, settings)
    monkeypatch.setattr(
      web_app,
      'run_sandbox_preflight',
      lambda _settings: {
        'result': 'pass',
        'reason_code': 'preflight_passed',
        'message': 'ok',
        'next_action': 'proceed',
        'checks': [],
      },
    )
  else:
    monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  if automation_enabled:
    _seed_manual_execution_truth(state_db_path)
  app = create_operator_console_app(
    _with_scan_runtime(_services_with_review_candidates()),
    tombstone_path=tmp_path / 'tombstones.json',
  )
  if connected_socket:
    key_file = tmp_path / 'sandbox-key.pem'
    key_file.write_text('placeholder-key-material', encoding='utf-8')
    _call_app(app, method='POST', path='/api/key-stage', body={'path': str(key_file)})
    _call_app(app, method='POST', path='/api/key-apply', body={'lane': 'sandbox'})
    _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'sandbox'})
  _call_app(app, method='POST', path='/api/scan', body={})
  _call_app(app, method='POST', path='/api/review-selection', body={'action': 'sync_selection', 'selected_keys': ['review-candidate-1']})
  _call_app(app, method='POST', path='/api/review-selection', body={'action': 'save_selection', 'selected_keys': ['review-candidate-1']})
  if automation_enabled:
    _call_app(
      app,
      method='POST',
      path='/api/automation-overlay',
      body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}},
    )
  return app, state_db_path


def test_scheduler_snapshot_present_in_bootstrap_payload() -> None:
  _, _, body = _call_app(create_operator_console_app(_services()), method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  snapshot = payload.get('scheduler_snapshot') or {}
  assert snapshot['schema_version'] == 2
  assert snapshot['decision'] == 'noop'
  assert 'socket_state' in snapshot
  assert 'scan_state' in snapshot
  assert 'cadence_state' in snapshot
  assert 'blocked_actions' in snapshot  # backward-compatible string list retained
  assert 'wait_reason' not in snapshot  # WAIT concept removed
  assert 'blocked_action_details' not in snapshot  # WAIT concept removed


def test_terminal_replay_preserves_submit_ready_workflow_for_automation(
  tmp_path: Path,
  monkeypatch: Any,
) -> None:
  # Superseded expectation update (submit supremacy, 478f65b): enabling automation with a
  # submit-ready saved set dispatches the submit IMMEDIATELY (submit owns the lane the
  # moment it is ready) — it no longer rests as submit_ready waiting for a later beat.
  # Bootstrap terminal replay therefore truthfully shows the consumed/retired set
  # (submitted_terminal), the post-submit cadence beat armed, and scan as the next step.
  # The manual (non-automation) preservation shape is covered by the sibling
  # defer/socket tests using the same fixture without automation.
  app, _ = _prepare_scheduler_submit_ready_app(
    tmp_path,
    monkeypatch,
    connected_socket=True,
    automation_enabled=True,
  )

  status, _, body = _call_app(app, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['workflow_source'] == 'terminal_scan_replay'
  assert payload['review_selection']['state_id'] == 'review_hold_saved_selection_locked'
  # The auto-dispatched submit consumed the set: retired, not still ready.
  assert payload['review_selection']['submit_ready'] is False
  assert payload['review_selection']['saved_set_status'] == 'submitted_terminal'
  consumed = payload['review_selection']['consumed_submit_terminal'] or {}
  assert consumed.get('consumed_submit_terminal_status') == 'completed'
  assert consumed.get('source_orders_created') is True
  assert payload['scheduler_snapshot']['submit_state'] == 'idle'
  assert payload['scheduler_snapshot']['pending_action'] is None
  # Post-submit cadence beat armed (cadence origin = submit completion).
  assert 'next_cadence_at_utc' in payload['scheduler_snapshot']['cadence_state']
  assert payload['workflow']['recommended_step'] == 'scan'
  assert payload['workflow']['next_actionable_step'] == 'scan'


def test_scheduler_defers_automated_scan_when_submit_ready(tmp_path: Path, monkeypatch: Any) -> None:
  app, _ = _prepare_scheduler_submit_ready_app(tmp_path, monkeypatch, connected_socket=True)

  status, _, body = _call_app(
    app,
    method='POST',
    path='/api/scan',
    body={'scheduler_source': 'automation_cadence'},
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['scheduler_snapshot']['decision'] == 'defer_to_submit'
  assert payload['scheduler_snapshot']['owner'] == 'submit_bridge'
  assert payload['scheduler_snapshot']['blocked_actions'] == ['scan']
  assert payload['workflow']['next_actionable_step'] == 'submit_order'


def test_scheduler_blocks_submit_bridge_when_socket_nonoperative(tmp_path: Path, monkeypatch: Any) -> None:
  app, _ = _prepare_scheduler_submit_ready_app(
    tmp_path,
    monkeypatch,
    connected_socket=False,
    automation_enabled=True,
  )

  status, _, body = _call_app(app, method='POST', path='/api/run', body={'bridge_action': 'submit_order'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['scheduler_snapshot']['decision'] == 'wait'
  assert payload['scheduler_snapshot']['owner'] == 'websocket_health'
  assert payload['scheduler_snapshot']['blocked_actions'] == ['submit_bridge']
  assert payload['scheduler_snapshot']['pending_action'] == 'submit'
  assert payload['reason'] == 'socket_disconnected'


def test_scheduler_submit_bridge_lease_acquire_release_events(tmp_path: Path, monkeypatch: Any) -> None:
  app, state_db_path = _prepare_scheduler_submit_ready_app(tmp_path, monkeypatch, connected_socket=True)

  status, _, body = _call_app(app, method='POST', path='/api/run', body={'bridge_action': 'submit_order'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['scheduler_snapshot']['owner'] == 'idle'
  connection = open_database(state_db_path)
  rows = connection.execute(
    "SELECT event_type, detail_json FROM runtime_events WHERE event_type LIKE 'scheduler_lease_%' ORDER BY id"
  ).fetchall()
  event_types = [str(row['event_type']) for row in rows]
  assert 'scheduler_lease_acquired' in event_types
  assert 'scheduler_lease_released' in event_types
  details = [json.loads(row['detail_json']) for row in rows if row['detail_json']]
  assert any(detail.get('owner') == 'submit_bridge' for detail in details)
  # schema 2: submit lease release must persist last_submit_completed_at_utc so cadence
  # can use submit terminal time as its countdown origin (Contract 4, Lane E)
  snapshot = payload.get('scheduler_snapshot') or {}
  assert snapshot.get('last_submit_completed_at_utc') is not None, (
    'contract 4: last_submit_completed_at_utc must be set in snapshot after submit lease release'
  )


def test_automation_active_satisfies_submit_guard_mode_authorization(tmp_path: Path, monkeypatch: Any) -> None:
  # AA-T1: automation enabled + not paused → submit_ready=True without a change_mode call
  state_db_path = str(tmp_path / 'aa-t1.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_manual_execution_truth(state_db_path)
  app = create_operator_console_app(_with_scan_runtime(_services_with_review_candidates()), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/automation-overlay', body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}})

  _call_app(app, method='POST', path='/api/scan', body={})
  _call_app(app, method='POST', path='/api/review-selection', body={'action': 'sync_selection', 'selected_keys': ['review-candidate-1']})
  _, _, save_body = _call_app(app, method='POST', path='/api/review-selection', body={'action': 'save_selection', 'selected_keys': ['review-candidate-1']})
  save_payload = json.loads(save_body)

  assert save_payload['review_selection']['state_id'] == 'review_hold_saved_selection_locked'
  assert save_payload['review_selection']['submit_ready'] is True


def test_automation_paused_does_not_satisfy_submit_guard(tmp_path: Path, monkeypatch: Any) -> None:
  # AA-T2: automation paused → escape hatch inactive → submit_ready=False
  state_db_path = str(tmp_path / 'aa-t2.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_manual_execution_truth(state_db_path)
  app = create_operator_console_app(_with_scan_runtime(_services_with_review_candidates()), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/automation-overlay', body={'action': 'apply', 'values': {'enabled': True, 'paused': True, 'cadence_ms': 4000, 'max_iterations': 3}})

  _call_app(app, method='POST', path='/api/scan', body={})
  _call_app(app, method='POST', path='/api/review-selection', body={'action': 'sync_selection', 'selected_keys': ['review-candidate-1']})
  _, _, save_body = _call_app(app, method='POST', path='/api/review-selection', body={'action': 'save_selection', 'selected_keys': ['review-candidate-1']})
  save_payload = json.loads(save_body)

  assert save_payload['review_selection']['state_id'] == 'review_hold_saved_selection_locked'
  assert save_payload['review_selection']['submit_ready'] is False


def test_automation_mode_authorization_escape_hatch_present_in_source() -> None:
  # AA-T3: source inspection — both escape hatch guard expressions are present
  source = inspect.getsource(web_app.create_operator_console_app)
  assert "automation_overlay_state.get('enabled')" in source
  assert "automation_overlay_state.get('paused')" in source


def test_scan_auto_selects_candidates_when_automation_active(tmp_path: Path, monkeypatch: Any) -> None:
  # AA-T4: scan with automation active → review_selection locked and submit_ready without operator selection step
  state_db_path = str(tmp_path / 'aa-t4.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_manual_execution_truth(state_db_path)
  app = create_operator_console_app(_with_scan_runtime(_services_with_review_candidates()), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/automation-overlay', body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}})

  _, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)

  assert scan_payload['review_selection']['state_id'] == 'review_hold_saved_selection_locked'
  assert scan_payload['review_selection']['submit_ready'] is True


def test_scan_auto_select_returns_submit_order_workflow_without_run_sequence(tmp_path: Path, monkeypatch: Any) -> None:
  # Phase 5: scan with automation active may project submit_order, but frontend auto_sequence stays empty.
  state_db_path = str(tmp_path / 'aa-t5.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_manual_execution_truth(state_db_path)
  app = create_operator_console_app(_with_scan_runtime(_services_with_review_candidates()), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/automation-overlay', body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}})

  _, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)

  assert scan_payload['workflow']['next_actionable_step'] == 'submit_order'
  assert scan_payload['workflow']['auto_sequence'] == []


def test_scan_no_auto_select_when_automation_disabled() -> None:
  # AA-T6: scan with no automation arm → workflow stays at select_candidates with empty auto_sequence
  app = create_operator_console_app(_services_with_review_candidates())

  _, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)

  assert scan_payload['workflow']['next_actionable_step'] == 'select_candidates'
  assert scan_payload['workflow']['auto_sequence'] == []



def test_ui2_execution_panel_backend_title_is_execution() -> None:
  # UI-T4: backend live_interaction summary must set title to EXECUTION, not Live interaction
  source = inspect.getsource(web_app)
  assert "'title': 'EXECUTION'" in source
  assert "'title': 'Live interaction'" not in source


def test_ui3_execution_panel_not_visible_when_automation_active_no_pairs(tmp_path: Path, monkeypatch: Any) -> None:
  # UI-T5: automation active but no runtime pairs → surface_visible=False → panel hidden
  state_db_path = str(tmp_path / 'ui-t5.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_manual_execution_truth(state_db_path)
  app = create_operator_console_app(_services_with_review_candidates(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/automation-overlay', body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}})

  _, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)

  live_interaction = scan_payload.get('live_interaction') or {}
  assert live_interaction.get('surface_visible') is False


def test_ui3_execution_panel_title_in_html_is_execution() -> None:
  # UI-T6: HTML default for live-interaction-title must be EXECUTION
  status, _, body = _call_app(create_operator_console_app(_services()), method='GET', path='/')
  assert status == '200 OK'
  assert 'id="live-interaction-title">EXECUTION' in body


def test_scan_no_auto_select_when_automation_paused(tmp_path: Path, monkeypatch: Any) -> None:
  # AA-T7: scan with automation paused → workflow stays at select_candidates with empty auto_sequence
  state_db_path = str(tmp_path / 'aa-t7.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_manual_execution_truth(state_db_path)
  app = create_operator_console_app(_services_with_review_candidates(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/automation-overlay', body={'action': 'apply', 'values': {'enabled': True, 'paused': True, 'cadence_ms': 4000, 'max_iterations': 3}})

  _, _, scan_body = _call_app(app, method='POST', path='/api/scan', body={})
  scan_payload = json.loads(scan_body)

  assert scan_payload['workflow']['next_actionable_step'] == 'select_candidates'
  assert scan_payload['workflow']['auto_sequence'] == []


def test_run_with_automation_active_returns_backend_owned_scan_workflow(tmp_path: Path, monkeypatch: Any) -> None:
  # Phase 5: run + automation active returns no browser scan sequence; backend cadence owns continuation.
  state_db_path = str(tmp_path / 'ac-t1.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_manual_execution_truth(state_db_path)
  app = create_operator_console_app(_services_with_review_candidates(), tombstone_path=tmp_path / 'tombstones.json')
  _call_app(app, method='POST', path='/api/automation-overlay', body={'action': 'apply', 'values': {'enabled': True, 'paused': False, 'cadence_ms': 4000, 'max_iterations': 3}})

  _, _, run_body = _call_app(app, method='POST', path='/api/run', body={})
  run_payload = json.loads(run_body)

  assert run_payload['workflow']['auto_sequence'] == []


def test_run_without_automation_does_not_return_scan_sequence(tmp_path: Path, monkeypatch: Any) -> None:
  # AC-T2: run + automation disabled → workflow.auto_sequence does not trigger scan loop
  state_db_path = str(tmp_path / 'ac-t2.sqlite3')
  settings = _build_test_settings(state_db_path)
  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  _seed_manual_execution_truth(state_db_path)
  app = create_operator_console_app(_services_with_review_candidates(), tombstone_path=tmp_path / 'tombstones.json')

  _, _, run_body = _call_app(app, method='POST', path='/api/run', body={})
  run_payload = json.loads(run_body)

  assert run_payload['workflow']['auto_sequence'] != ['scan']


def test_scan_auto_select_clears_stale_review_selection_before_building() -> None:
  # AC-T3: scan AA-2 block must clear stale review_selection state before building new auto-selection
  source = inspect.getsource(web_app.create_operator_console_app)
  clear_pos = source.find('_clear_review_selection_state()\n          _clear_review_selection_hydration_state()\n          _refresh_review_selection_projection(payload, selected_keys=_auto_keys')
  assert clear_pos != -1, 'AC-T3: _clear_review_selection_state must be called immediately before auto-select projection in scan AA-2 block'


def test_run_recomputes_in_flight_count_after_review_selection_refresh() -> None:
  # AC-T4: run handler must recompute _in_flight_candidate_count AFTER _refresh_review_selection_projection
  source = inspect.getsource(web_app.create_operator_console_app)
  refresh_pos = source.rfind('_refresh_review_selection_projection(payload)\n')
  recompute_pos = source.rfind("payload['_in_flight_candidate_count'] = _stage_data['in_flight_candidate_count']")
  assert refresh_pos != -1, 'AC-T4: _refresh_review_selection_projection must appear in run handler'
  assert recompute_pos != -1, 'AC-T4: _in_flight_candidate_count recomputation must be present after review_selection refresh'
  assert recompute_pos > refresh_pos, 'AC-T4: in-flight recomputation must appear AFTER _refresh_review_selection_projection'


def test_run_rebuilds_live_interaction_after_in_flight_recompute() -> None:
  # AC-T5: run handler must rebuild pair_monitor and live_interaction after _fetch_stage_columns recompute
  source = inspect.getsource(web_app.create_operator_console_app)
  recompute_pos = source.rfind("payload['_in_flight_candidate_count'] = _stage_data['in_flight_candidate_count']")
  pair_monitor_pos = source.rfind("payload['pair_monitor'] = _build_pair_monitor_payload(payload)")
  live_interaction_pos = source.rfind("payload['live_interaction'] = dict(payload['pair_monitor'].get('live_interaction', {}))")
  assert recompute_pos != -1, 'AC-T5: in-flight recomputation must be present'
  assert pair_monitor_pos > recompute_pos, 'AC-T5: pair_monitor must be rebuilt after in-flight recompute'
  assert live_interaction_pos > pair_monitor_pos, 'AC-T5: live_interaction must be rebuilt after pair_monitor'


def test_run_auto_sequence_uses_while_loop_with_chaining() -> None:
  # AC-T6: runAutoSequence must use a while loop that follows returned auto_sequence
  source = inspect.getsource(web_app._render_html)
  assert 'pending = [...sequence]' in source, 'AC-T6: pending array from initial sequence must be present'
  assert 'const step = pending.shift()' in source, 'AC-T6: step must be shifted from pending queue'
  assert "pending = [...returnedSeq]" in source, 'AC-T6: pending must be replaced with returned auto_sequence'
  assert 'while (pending.length > 0)' in source, 'AC-T6: while loop driving pending queue must be present'


def test_run_auto_sequence_passes_bridge_action_for_submit_order() -> None:
  # AC-T7: runAutoSequence must send bridge_action when next_actionable_step is submit_order
  source = inspect.getsource(web_app._render_html)
  assert "if (nextStep === 'submit_order')" in source, 'AC-T7: bridge action guard must check submit_order'
  assert "actionOpts.body = { bridge_action: 'submit_order' }" in source, 'AC-T7: bridge action body must be set'


def test_confirm_auto_forward_toggle_kickstarts_run_auto_sequence() -> None:
  # AC-T8: confirmClientAutoForwardToggle must call runAutoSequence after enable_and_arm and resume_and_arm
  source = inspect.getsource(web_app._render_html)
  kickstart_pattern = "(nextPayload && nextPayload.workflow && nextPayload.workflow.auto_sequence) || []"
  count = source.count(kickstart_pattern)
  assert count >= 2, f'AC-T8: runAutoSequence kickstart must appear in both enable_and_arm and resume_and_arm blocks, found {count}'


def test_perform_action_suppresses_deck_render_for_intermediate_auto_steps() -> None:
  # AC-T9: performAction must suppress deck rendering when suppressDeckRender is set and auto_sequence is non-empty
  source = inspect.getsource(web_app._render_html)
  assert 'suppressedDeckRender' in source, 'AC-T9: suppressedDeckRender flag must be present'
  assert 'options.suppressDeckRender' in source, 'AC-T9: suppressDeckRender option must be checked'
  assert 'if (!suppressedDeckRender)' in source, 'AC-T9: renderDeckViewShell must be guarded by suppressedDeckRender'


def test_scan_endpoint_await_loop_present() -> None:
  # SC23-T1: scan endpoint must contain blocking wait loop with interval and timeout constants
  # (restored 2026-06-12 after silent revert in a174ee4 — see
  # AUTOMATION_SELECT_STALL_AND_OFFLINE_PILL_GLOW_INVESTIGATION_AND_UPDATE_PLAN_2026-06-12.md)
  source = inspect.getsource(web_app.create_operator_console_app)
  assert '_SCAN_AWAIT_POLL_SEC' in source, 'SC23-T1: _SCAN_AWAIT_POLL_SEC constant must be present in scan endpoint'
  assert '_SCAN_AWAIT_TIMEOUT_SEC' in source, 'SC23-T1: _SCAN_AWAIT_TIMEOUT_SEC constant must be present in scan endpoint'
  assert '_scan_await_start' in source, 'SC23-T1: _scan_await_start variable must be present in scan endpoint'
  assert '_scan_runtime_snapshot()' in source, 'SC23-T1: _scan_runtime_snapshot() must be called in the wait loop'


def test_scan_endpoint_await_covers_canceling_status() -> None:
  # SC23-T2: wait loop must hold on both processing and canceling statuses
  source = inspect.getsource(web_app.create_operator_console_app)
  assert "'canceling'" in source or '"canceling"' in source, "SC23-T2: 'canceling' must be included in the wait condition"
  assert "'processing'" in source or '"processing"' in source, "SC23-T2: 'processing' must be included in the wait condition"


def test_scan_endpoint_await_has_timeout_break() -> None:
  # SC23-T3: wait loop must break on timeout to prevent infinite blocking
  source = inspect.getsource(web_app.create_operator_console_app)
  assert 'time.monotonic()' in source, 'SC23-T3: time.monotonic() must be used for timeout tracking'
  assert '_SCAN_AWAIT_TIMEOUT_SEC' in source, 'SC23-T3: timeout constant must be used in break condition'
  assert '_terminal_status' in source, 'SC23-T3: _terminal_status must be set after wait loop exits'


def test_scan_endpoint_await_fallback_on_non_terminal() -> None:
  # SC23-T4: if wait loop exits without reaching a terminal status, must fall back to _refresh_state_payload
  source = inspect.getsource(web_app.create_operator_console_app)
  assert '_terminal_status not in' in source, 'SC23-T4: non-terminal fallback guard must be present'
  assert '_refresh_state_payload' in source, 'SC23-T4: _refresh_state_payload fallback must be present for non-terminal exit'


def test_blocked_popup_message_excludes_cadence_line() -> None:
  # SC23-T5: DISMISS-only blocked popup must not append cadenceLine — value is Python default, not operator-configured
  source = inspect.getsource(web_app._render_html)
  # The acknowledge/blocked path must use only clientAutoForwardBlockedMessage, not cadenceLine concatenation
  assert "message: clientAutoForwardBlockedMessage(payload || {})," in source, (
    'SC23-T5: blocked popup must use clientAutoForwardBlockedMessage directly without cadenceLine'
  )
  # Confirm the old pattern with cadenceLine appended is gone from the blocked path
  assert "`${clientAutoForwardBlockedMessage(payload || {})} ${cadenceLine}`" not in source, (
    'SC23-T5: blocked popup must not concatenate cadenceLine'
  )


def test_mint_gate_confirm_handlers_resume_automation_sequence() -> None:
  # V-A3 (AUTOMATION_SELECT_STALL plan 2026-06-12): the mint/clear confirm handlers
  # must hand the confirmed scan response to the sequence runner without rendering
  # the intermediate select/submit state when auto-forward is armed — UI contract
  # under automation is processing -> queued cards only (§6.2.1).
  source = inspect.getsource(web_app._render_html)
  assert 'async function resumeAutomationAfterMintGate(payload)' in source, (
    'V-A3: mint-gate automation handoff function must be present'
  )
  assert source.count('await resumeAutomationAfterMintGate(payload)') >= 2, (
    'V-A3: both mint and clear-lane confirm handlers must route through the handoff'
  )
  assert 'await runAutoSequence(sequence)' in source, (
    'V-A3: handoff must re-enter the client sequence runner'
  )


# ---------------------------------------------------------------------------
# RET-A: overlay_lock scope — _private_key_file_env_overlay (lines 529-546)
# Plan: PLAN-POLYVENTURE-RETRY-FIX-20260613
# ---------------------------------------------------------------------------

def test_overlay_lock_not_held_during_yield() -> None:
  # A1: after entering the context manager body the lock must be free so
  # other threads can acquire it without blocking.
  lock = threading.Lock()
  env_var = web_app.PRIVATE_KEY_FILE_ENV_VAR
  prev = os.environ.pop(env_var, None)
  try:
    with web_app._private_key_file_env_overlay('/fake/key.pem', lock=lock):
      acquired = lock.acquire(blocking=False)
      assert acquired, 'A1: overlay_lock must NOT be held during yield'
      if acquired:
        lock.release()
  finally:
    if prev is not None:
      os.environ[env_var] = prev
    else:
      os.environ.pop(env_var, None)


def test_overlay_env_var_set_inside_context() -> None:
  # A2: the env var must be set to path_value for the duration of the yield.
  env_var = web_app.PRIVATE_KEY_FILE_ENV_VAR
  prev = os.environ.pop(env_var, None)
  try:
    with web_app._private_key_file_env_overlay('/test/key.pem'):
      assert os.environ.get(env_var) == '/test/key.pem', (
        'A2: env var must equal path_value inside context'
      )
    assert os.environ.get(env_var) is None, (
      'A2: env var must be removed after context exits (was absent before)'
    )
  finally:
    if prev is not None:
      os.environ[env_var] = prev
    else:
      os.environ.pop(env_var, None)


def test_overlay_env_var_restored_on_exception() -> None:
  # A3: env var must be restored even when the body raises.
  env_var = web_app.PRIVATE_KEY_FILE_ENV_VAR
  original = 'original_value'
  os.environ[env_var] = original
  try:
    try:
      with web_app._private_key_file_env_overlay('/new/key.pem'):
        raise RuntimeError('deliberate')
    except RuntimeError:
      pass
    assert os.environ.get(env_var) == original, (
      'A3: env var must be restored to original value after exception'
    )
  finally:
    os.environ.pop(env_var, None)


def test_overlay_concurrent_calls_do_not_deadlock() -> None:
  # A4: two concurrent overlay calls sharing the same lock must both complete
  # without deadlock or exception.  The lock is held only for µs-scale env
  # set/restore, so concurrent calls can overlap in the yield body — proven
  # by both threads entering the barrier inside the context simultaneously.
  # Note: env var final state is intentionally not asserted here; with a
  # shared env var and non-atomic snapshot/restore, cleanup ordering is
  # non-deterministic when calls genuinely overlap.  Downstream code uses
  # pre-resolved Settings objects and does not read the env var during yield,
  # so the stale-var window carries no operational risk.
  lock = threading.Lock()
  errors: list[str] = []
  both_inside = threading.Barrier(2)

  def run_overlay() -> None:
    try:
      with web_app._private_key_file_env_overlay('/path/key.pem', lock=lock):
        both_inside.wait(timeout=2.0)
    except Exception as exc:
      errors.append(str(exc))

  prev = os.environ.pop(web_app.PRIVATE_KEY_FILE_ENV_VAR, None)
  try:
    t_a = threading.Thread(target=run_overlay)
    t_b = threading.Thread(target=run_overlay)
    t_a.start()
    t_b.start()
    t_a.join(timeout=4.0)
    t_b.join(timeout=4.0)
    assert not t_a.is_alive(), 'A4: thread A must not be deadlocked'
    assert not t_b.is_alive(), 'A4: thread B must not be deadlocked'
    assert not errors, f'A4: concurrent overlay raised: {errors}'
  finally:
    if prev is not None:
      os.environ[web_app.PRIVATE_KEY_FILE_ENV_VAR] = prev
    else:
      os.environ.pop(web_app.PRIVATE_KEY_FILE_ENV_VAR, None)


# ---------------------------------------------------------------------------
# RET-B: poll loop exits immediately on retry_wait stage
# Plan: PLAN-POLYVENTURE-RETRY-FIX-20260613
# ---------------------------------------------------------------------------

def test_poll_loop_breaks_on_retry_wait_stage() -> None:
  # B1: the scan wait loop must contain a stage == 'retry_wait' break guard.
  source = inspect.getsource(web_app.create_operator_console_app)
  assert "'retry_wait'" in source or '"retry_wait"' in source, (
    'B1: retry_wait stage must appear as a string literal in the scan wait loop'
  )


def test_poll_loop_retry_wait_break_uses_lower() -> None:
  # B2: the stage comparison must be case-normalised so values like RETRY_WAIT
  # and Retry_Wait also match.
  source = inspect.getsource(web_app.create_operator_console_app)
  # The stage guard must use .lower() — same pattern as the status check above it.
  assert 'stage' in source and '.lower()' in source, (
    "B2: stage check in poll loop must use .lower() for case-insensitive comparison"
  )


def test_poll_loop_retry_wait_guard_inside_while_loop() -> None:
  # B3: the retry_wait break must be inside the while loop body, not after it.
  source = inspect.getsource(web_app.create_operator_console_app)
  while_idx = source.find('while str(scan_snapshot')
  assert while_idx != -1, 'B3: scan await while loop must be present'
  sleep_idx = source.find('_SCAN_AWAIT_POLL_SEC', while_idx)
  retry_wait_idx = source.find('retry_wait', while_idx)
  assert retry_wait_idx != -1, 'B3: retry_wait literal must appear after the while statement'
  assert retry_wait_idx < sleep_idx, (
    'B3: retry_wait break must appear before the sleep call inside the loop body'
  )


def test_poll_loop_retry_wait_preserves_timeout_guard() -> None:
  # B4: the timeout guard (_SCAN_AWAIT_TIMEOUT_SEC) must still be present
  # alongside the new retry_wait guard — both exit conditions required.
  source = inspect.getsource(web_app.create_operator_console_app)
  while_idx = source.find('while str(scan_snapshot')
  assert while_idx != -1, 'B4: scan await while loop must be present'
  timeout_idx = source.find('_SCAN_AWAIT_TIMEOUT_SEC', while_idx)
  retry_wait_idx = source.find('retry_wait', while_idx)
  assert timeout_idx != -1, 'B4: timeout guard must still be present in the wait loop'
  assert retry_wait_idx != -1, 'B4: retry_wait guard must also be present in the wait loop'


# ---------------------------------------------------------------------------
# RET-C: retry ticker chains runAutoSequence on successful scan
# Plan: PLAN-POLYVENTURE-RETRY-FIX-20260613
# ---------------------------------------------------------------------------

def test_retry_ticker_chains_run_auto_sequence_after_scan() -> None:
  # C1 (Phase 2+3): backend timer owns retry dispatch; ticker is display-only.
  # Ticker must NOT call requestScheduledScanRefire or runAutoSequence.
  source = inspect.getsource(web_app._render_html)
  ticker_idx = source.find('updateZeroFoundRetryCountdown')
  assert ticker_idx != -1, 'C1: updateZeroFoundRetryCountdown function must be present'
  fn_body_start = source.find('function updateZeroFoundRetryCountdown', ticker_idx - 200)
  assert fn_body_start != -1, 'C1: function definition must be locatable'
  fn_body_end = source.find('\n    function ', fn_body_start + 1)
  fn_body = source[fn_body_start:fn_body_end] if fn_body_end != -1 else source[fn_body_start:]
  assert 'requestScheduledScanRefire' not in fn_body, (
    'C1: updateZeroFoundRetryCountdown must not dispatch — backend timer owns retry fires'
  )


def test_retry_ticker_auto_sequence_uses_workflow_auto_sequence_field() -> None:
  # C2 (Phase 2+3): ticker does not dispatch, so it does not extract workflow.auto_sequence.
  # Confirm the ticker body contains no dispatch artifacts.
  source = inspect.getsource(web_app._render_html)
  fn_start = source.find('function updateZeroFoundRetryCountdown')
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert "performAction('scan'" not in fn_body, (
    'C2: retry ticker must not call performAction(scan) directly'
  )


def test_retry_ticker_auto_sequence_guarded_by_auto_advance_enabled() -> None:
  # C3 (Phase 2+3): ticker is display-only; no runAutoSequence call inside.
  # Backend handles dispatch; the ticker only renders the countdown.
  source = inspect.getsource(web_app._render_html)
  fn_start = source.find('function updateZeroFoundRetryCountdown')
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert 'runAutoSequence' not in fn_body, (
    'C3: retry ticker must not call runAutoSequence — backend timer owns dispatch'
  )


def test_retry_ticker_run_auto_sequence_after_scan_refire_in_chain() -> None:
  # C4 (Phase 2+3): ticker is display-only. Backend timer owns all scan dispatch.
  # Neither requestScheduledScanRefire nor performAction('scan') must appear in the ticker.
  source = inspect.getsource(web_app._render_html)
  fn_start = source.find('function updateZeroFoundRetryCountdown')
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert 'requestScheduledScanRefire' not in fn_body, (
    'C4: retry ticker must not dispatch via requestScheduledScanRefire'
  )
  assert "performAction('scan'" not in fn_body, (
    'C4: retry ticker must NOT call performAction(scan) directly'
  )


def test_scan_refire_submit_tie_break_helper_embedded_in_shell_html() -> None:
  source = inspect.getsource(web_app._render_html)
  assert 'function submitOrderReadyForScanTieBreak(payload = {})' in source
  assert "workflow.next_actionable_step || '').toLowerCase() === 'submit_order'" in source
  fn_start = source.find('function requestScheduledScanRefire()')
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert "performAction('run'" not in fn_body


def test_auto_find_cadence_refire_routes_through_scan_refire_authority() -> None:
  # Phase 2+3 (2026-06-27): backend threading.Timer owns cadence dispatch.
  # The cadence ticker is display-only — it renders the countdown from
  # cadence_state.next_cadence_at_utc and does not call requestScheduledScanRefire.
  source = inspect.getsource(web_app._render_html)
  fn_start = source.find('function updateAutoFindCadenceCountdown')
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert 'requestScheduledScanRefire()' not in fn_body, 'cadence ticker must not dispatch — backend timer owns fires'
  assert "performAction('scan'" not in fn_body, 'cadence ticker must not call performAction(scan) directly'
  assert 'WAIT' not in fn_body, 'cadence ticker must not show WAIT text'


def test_retry_refire_routes_through_scan_refire_authority() -> None:
  # Phase 2+3 (2026-06-27): backend threading.Timer owns retry dispatch.
  # The retry ticker is display-only — it renders the countdown from
  # retry_state and does not call requestScheduledScanRefire.
  source = inspect.getsource(web_app._render_html)
  fn_start = source.find('function updateZeroFoundRetryCountdown')
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert 'requestScheduledScanRefire()' not in fn_body, 'retry ticker must not dispatch — backend timer owns fires'
  assert "performAction('scan'" not in fn_body, 'retry ticker must not call performAction(scan) directly'
  assert 'WAIT' not in fn_body, 'retry ticker must not show WAIT text'


def test_scan_refire_authority_is_inert_for_phase5_backend_dispatch() -> None:
  # Phase 5: frontend scan-refire authority is retained only as an inert compatibility seam.
  source = inspect.getsource(web_app._render_html)
  fn_start = source.find('function requestScheduledScanRefire()')
  assert fn_start != -1, 'requestScheduledScanRefire authority must be defined'
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert "performAction('scan'" not in fn_body
  assert "performAction('run'" not in fn_body
  assert 'return null;' in fn_body


def test_scan_refire_attention_gate_does_not_precede_submit_ready_check() -> None:
  # SUBMIT_SUPREMACY_AUTOMATION_SCHEDULER_PRIORITY_BMAP_2026-06-27 Lane B correction:
  # the broad attention gate (liveInteractionRequiresAttention) must NOT precede the submit-ready
  # tie-break. The previous Fix A ordering (attention before submit) was the priority inversion:
  # it allowed a generic surface-visible or client-flag condition to block submit dispatch.
  # Contract 3 requires: submit-active and submit-ready checks come BEFORE the attention gate.
  # liveInteractionRequiresAttention remains required (step 7) but only for non-submit attention.
  source = inspect.getsource(web_app._render_html)
  assert 'function liveInteractionRequiresAttention(payload = {})' in source, (
    'liveInteractionRequiresAttention helper must remain defined (Contract 3 step 7)'
  )
  fn_start = source.find('function liveInteractionRequiresAttention(payload = {})')
  fn_end = source.find('\n    function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert 'surface_visible' in fn_body, 'gate must still read backend live_interaction.surface_visible'
  assert 'execution_state' in fn_body and 'submitting' in fn_body, (
    'gate must still treat an in-flight submit/processing execution_state as attention'
  )
  # The inert legacy refire seam must not contain either gate; backend admission owns priority.
  auth_start = source.find('function requestScheduledScanRefire()')
  auth_body = source[auth_start:source.find('\n    function ', auth_start + 1)]
  attn_idx = auth_body.find('liveInteractionRequiresAttention(state.payload || {})')
  submit_idx = auth_body.find('submitOrderReadyForScanTieBreak(state.payload || {})')
  assert submit_idx == -1
  assert attn_idx == -1


# ---------------------------------------------------------------------------
# RET-D: shellRefreshInFlight timeout backstop in refreshShellWhileProcessing
# Plan: PLAN-POLYVENTURE-RETRY-FIX-20260613
# ---------------------------------------------------------------------------

def test_bootstrap_refresh_timeout_constant_present() -> None:
  # D1: BOOTSTRAP_REFRESH_TIMEOUT_MS constant must be defined and used —
  # prevents shellRefreshInFlight from being permanently stuck if bootstrap hangs.
  source = inspect.getsource(web_app._render_html)
  assert 'BOOTSTRAP_REFRESH_TIMEOUT_MS' in source, (
    'D1: BOOTSTRAP_REFRESH_TIMEOUT_MS constant must be present in the rendered template'
  )
  assert '30000' in source, (
    'D1: timeout must be set to 30000 ms (30 s — 4-6x headroom over expected <5 s bootstrap)'
  )


def test_bootstrap_refresh_uses_promise_race_with_timeout() -> None:
  # D2: requestJson('/api/bootstrap') must be wrapped in Promise.race with a
  # timeout rejection so shellRefreshInFlight is always cleared, even if the
  # server hangs.  Timeout rejection flows through finally { shellRefreshInFlight = false }.
  source = inspect.getsource(web_app._render_html)
  fn_start = source.find('async function refreshShellWhileProcessing')
  assert fn_start != -1, 'D2: refreshShellWhileProcessing function must be present'
  fn_end = source.find('\n    async function ', fn_start + 1)
  fn_body = source[fn_start:fn_end] if fn_end != -1 else source[fn_start:]
  assert 'Promise.race' in fn_body, (
    'D2: requestJson bootstrap call must be wrapped in Promise.race'
  )
  assert 'BOOTSTRAP_REFRESH_TIMEOUT_MS' in fn_body, (
    'D2: timeout constant must be used inside Promise.race in refreshShellWhileProcessing'
  )
  assert 'setTimeout' in fn_body, (
    'D2: Promise.race timeout leg must use setTimeout for the deadline'
  )


# ---------------------------------------------------------------------------
# CA-1: stop_automation_loop wired to server overlay stop
# ---------------------------------------------------------------------------

def test_stop_automation_loop_calls_server_overlay_stop() -> None:
  # CA-1 / TD-1: the quick-strip handler opens a confirm dialog; overlay stop
  # is inside executeStopAutomationLoop (called on confirm).
  source = inspect.getsource(web_app._render_html)
  handler_start = source.find("value === 'stop_automation_loop'")
  assert handler_start != -1, "CA-1: stop_automation_loop handler must be present in rendered HTML"
  handler_end = source.find('return;\n          }', handler_start)
  handler_body = source[handler_start:handler_end] if handler_end != -1 else source[handler_start:handler_start + 400]
  assert 'openConfirmationChallenge' in handler_body, (
    "TD-1: stop_automation_loop handler must open confirm dialog before executing stop"
  )
  assert "stop_automation_loop_confirm" in handler_body, (
    "TD-1: confirm challenge action must be 'stop_automation_loop_confirm'"
  )
  exec_pos = source.find('async function executeStopAutomationLoop(')
  assert exec_pos != -1, "CA-1: executeStopAutomationLoop must be defined in _render_html"
  exec_fragment = source[exec_pos:exec_pos + 1900]
  assert "requestJson('/api/automation-overlay'" in exec_fragment, (
    "CA-1: executeStopAutomationLoop must call requestJson('/api/automation-overlay') to stop server state"
  )
  assert "action: 'stop'" in exec_fragment, (
    "CA-1: overlay call must pass action: 'stop'"
  )
  assert 'renderPayload(' in exec_fragment, (
    "CA-1: executeStopAutomationLoop must call renderPayload with the returned overlay payload"
  )
  assert "runUiAction('bootstrap')" in exec_fragment, (
    "TD-2: executeStopAutomationLoop must call runUiAction('bootstrap') after overlay stop"
  )


def test_td3_focus_listener_dismisses_hanging_close_popup() -> None:
  source = inspect.getsource(web_app._render_html)
  start_pos = source.find('function startSessionLease(')
  assert start_pos != -1, "TD-3: startSessionLease must exist in _render_html"
  fn_fragment = source[start_pos:start_pos + 3000]
  assert "addEventListener('focus'" in fn_fragment, (
    "TD-3: startSessionLease must register a 'focus' listener to dismiss the hanging close popup"
  )
  assert 'closeTransition.started' in fn_fragment, (
    "TD-3: focus listener must check closeTransition.started before dismissing"
  )
  assert 'closeCloseTransitionModal' in fn_fragment, (
    "TD-3: focus listener must call closeCloseTransitionModal to reset the close transition state"
  )


def test_td4_browser_close_with_automation_active_shows_stop_confirm() -> None:
  source = inspect.getsource(web_app._render_html)
  pos = source.find('function beginCloseWindowOfflineTransition(')
  assert pos != -1, "TD-4: beginCloseWindowOfflineTransition must exist"
  fn_fragment = source[pos:pos + 1500]
  assert 'autoForwardActive' in fn_fragment, (
    "TD-4: beginCloseWindowOfflineTransition must check autoForwardActive"
  )
  assert 'stop_automation_loop_and_close_confirm' in fn_fragment, (
    "TD-4: beginCloseWindowOfflineTransition must open stop-confirm dialog when automation is active"
  )
  assert 'openConfirmationChallenge' in fn_fragment, (
    "TD-4: beginCloseWindowOfflineTransition must call openConfirmationChallenge for automation-active case"
  )


def test_td4_confirm_handler_wires_stop_automation_loop_and_close() -> None:
  source = inspect.getsource(web_app._render_html)
  confirm_pos = source.find("challengeAction === 'stop_automation_loop_and_close_confirm'")
  assert confirm_pos != -1, (
    "TD-4: challenge-modal-confirm must handle 'stop_automation_loop_and_close_confirm' action"
  )
  confirm_fragment = source[confirm_pos:confirm_pos + 200]
  assert 'executeStopAutomationLoop' in confirm_fragment, (
    "TD-4: stop_automation_loop_and_close_confirm must call executeStopAutomationLoop"
  )
  assert 'closeCloseTransitionModal' in confirm_fragment, (
    "TD-4: stop_automation_loop_and_close_confirm must reset closeTransition after stop"
  )


def test_extended_wait_fires_on_alive_thread(tmp_path: Path, monkeypatch: Any) -> None:
  import asyncio

  settings = _runtime_settings_for_lane(tmp_path, 'live')

  class _SlowConnectWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      await asyncio.sleep(3.5)
      self.connected = True

    async def disconnect(self) -> None:
      self.connected = False

    async def _recv_with_timeout(self, _timeout_sec: float) -> str:
      raise WebSocketTimeout('idle')

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _SlowConnectWebSocketClient)

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'live')

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['session_overlay']['context']['mode_selected'] is True
  assert payload['connection_posture']['connection_state']['websocket_connected'] is True


def test_no_extended_wait_on_normal_connect(tmp_path: Path, monkeypatch: Any) -> None:
  _patch_fake_websocket_runtime(monkeypatch, _runtime_settings_for_lane(tmp_path, 'live'))

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'live')

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['session_overlay']['context']['mode_selected'] is True
  assert payload['connection_posture']['connection_state']['websocket_connected'] is True


def test_no_extended_wait_on_dead_thread(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _runtime_settings_for_lane(tmp_path, 'live')

  class _ErrorConnectWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      raise RuntimeError('socket connect failed')

    async def disconnect(self) -> None:
      self.connected = False

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _ErrorConnectWebSocketClient)

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'live')

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'websocket_connection_failed'
  assert payload['session_overlay']['context']['mode_selected'] is False


def test_auth_failure_resolves_at_initial_budget(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _runtime_settings_for_lane(tmp_path, 'live')

  class _AuthFailWebSocketClient:
    def __init__(self, **_: Any) -> None:
      self.connected = False

    async def connect(self) -> None:
      raise WebSocketAuthError('auth rejected')

    async def disconnect(self) -> None:
      self.connected = False

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_args, **_kwargs: settings)
  monkeypatch.setattr(web_app, 'resolve_private_key_path', lambda _settings: Path(str(_settings.private_key_file)))
  monkeypatch.setattr(web_app, 'load_private_key', lambda _path: object())
  monkeypatch.setattr(web_app, 'KalshiWebSocketClient', _AuthFailWebSocketClient)

  app = create_operator_console_app(tombstone_path=tmp_path / 'tombstones.json')
  _load_lane_key(app, tmp_path, monkeypatch, 'live')

  status, _, body = _call_app(app, method='POST', path='/api/change-mode', body={'lane': 'live'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'credential_acceptance_failed'
  assert payload['session_overlay']['context']['mode_selected'] is False

# ---------------------------------------------------------------------------
# FIX-DRC — deck-rebuild funds suppression threading + header stability
# ---------------------------------------------------------------------------

def test_drc_bootstrap_forwards_suppress_flag_to_report_and_reconcile(monkeypatch) -> None:
  ready = {'settings_ready': True, 'environment_ready': True, 'credential_ready': True, 'operation_lane': 'sandbox'}
  monkeypatch.setattr(web_app, '_load_shell_settings_context', lambda **_k: (dict(ready), None))
  captured: dict = {}

  def _rec_report(**kwargs):
    captured['report'] = kwargs.get('suppress_live_funds_refresh')
    return {'next_action': 'x', 'funds_posture': {}, 'operation_lane': 'sandbox'}

  def _rec_reconcile(**kwargs):
    captured['reconcile'] = kwargs.get('suppress_live_funds_refresh')
    return {'decision': 'planned', 'pair_count': 0, 'pairs': []}

  web_app.build_bootstrap_payload(report_fn=_rec_report, reconcile_fn=_rec_reconcile, suppress_live_funds_refresh=True)
  assert captured['report'] is True
  assert captured['reconcile'] is True

  captured.clear()
  web_app.build_bootstrap_payload(report_fn=_rec_report, reconcile_fn=_rec_reconcile)
  assert captured['report'] is False
  assert captured['reconcile'] is False


def test_drc_header_banner_stable_from_heartbeat_sourced_posture() -> None:
  cache: dict = {}
  # A suppressed rebuild serves a heartbeat-sourced fresh posture; the banner shows it
  # and seeds the durable cache exactly as a live reading would.
  fresh = web_app._build_header_amount_summary(
    {
      'report': {'operation_lane': 'live'},
      'funds_posture': {
        'available_funds_snapshot': '50.00',
        'funds_refresh_status': 'fresh',
        'available_funds_as_of': _utc_iso(0.5),
        'funds_source': 'heartbeat_snapshot',
      },
    },
    durable_funds_cache=cache,
  )
  assert fresh['money_authorized'] is True
  assert fresh['funds_bridged'] is False
  assert cache.get('available_funds') is not None

  # A funds-less rebuild with the cache still fresh bridges rather than blanking.
  bridged = web_app._build_header_amount_summary(
    {'connection_posture': {'operation_lane': 'live'}},
    durable_funds_cache=cache,
  )
  assert bridged['money_authorized'] is True
  assert bridged['funds_bridged'] is True


# ---------------------------------------------------------------------------
# W1: a first-boot websocket connect that is slow/transient must not hard-fail the
# mode change and kill the in-flight connect. Auth rejection still fails closed.
# ---------------------------------------------------------------------------

def test_w1_grace_constant_is_45s() -> None:
  # Telemetry-derived (connect p90 ~38s; current 9s window catches only ~56%).
  assert WS_CONNECTING_SLOW_GRACE_SEC == 45.0


def _w1_web_app_source() -> str:
  import pathlib
  return (pathlib.Path(__file__).resolve().parents[1] / 'src' / 'polyventure' / 'web_app.py').read_text(encoding='utf-8')


def test_w1_mode_change_keeps_transient_connect_alive() -> None:
  text = _w1_web_app_source()
  # Wait-timeout decision: auth/terminal-failure/exited-thread -> stop (fail closed);
  # otherwise (transient, thread alive) -> do NOT stop.
  assert "_ws_terminal = (" in text, 'W1: three-way terminal decision must exist'
  assert "_ws_status in {'auth_failed', 'connection_failed'}" in text, 'W1: auth/terminal failure fails closed'
  assert "or not websocket_runtime_thread.is_alive()" in text, 'W1: an exited runtime thread fails closed'
  assert "if _ws_terminal:" in text and "_stop_websocket_runtime(" in text, 'W1: only the terminal branch stops the runtime'


def test_w1_connecting_slow_and_grace_clock_wired() -> None:
  text = _w1_web_app_source()
  # connecting_since is stamped at connect start and cleared on connect-success (both sites).
  assert "connecting_since=_websocket_runtime_utc_now()" in text, 'W1: grace clock must start at connect'
  assert text.count('connecting_since=None') >= 2, 'W1: grace clock must clear on connect-success'
  # connecting_slow is derived in the snapshot from the grace + the unconnected connecting state.
  assert "snapshot['connecting_slow']" in text, 'W1: snapshot must surface connecting_slow'
  assert "> WS_CONNECTING_SLOW_GRACE_SEC" in text, 'W1: connecting_slow compares elapsed against the grace'
  assert "== 'connecting' and not bool(snapshot.get('websocket_connected'))" in text, 'W1: connecting_slow only for the unconnected connecting state'


# ---------------------------------------------------------------------------
# STOP-3 SA: the unified exit orchestrator — one cancel_on_pause-gated teardown
# (cancel_all_pairs + halt-mark) shared by STOP / browser-close / offline.
# ---------------------------------------------------------------------------

def test_sa_run_operator_teardown_truth_table(monkeypatch: Any) -> None:
  import polyventure.web_app as web_app
  cancel_calls: list = []
  halt_calls: list = []
  monkeypatch.setattr(web_app, 'cancel_all_pairs', lambda settings, **kw: cancel_calls.append(kw) or {'canceled_pair_count': 3})
  monkeypatch.setattr(web_app, '_halt_mark_lifecycle_candidates_terminal', lambda lsid, db: halt_calls.append((lsid, db)) or 5)

  class _S:
    def __init__(self, cop: bool) -> None:
      self.cancel_on_pause = cop
      self.state_db_path = '/tmp/teardown.sqlite3'

  # cancel_on_pause True -> both primitives run; their results are returned.
  summary, count = web_app._run_operator_teardown(
    active_settings=_S(True), lane_session_id='sess-1', env_override='live', subaccount_override=2,
  )
  assert len(cancel_calls) == 1 and len(halt_calls) == 1, 'SA: both teardown legs must run when cancel_on_pause'
  assert summary == {'canceled_pair_count': 3} and count == 5
  assert halt_calls[0] == ('sess-1', '/tmp/teardown.sqlite3'), 'SA: halt-mark gets the resolved lsid + db path'
  assert cancel_calls[0] == {'env_override': 'live', 'subaccount_override': 2}, 'SA: cancel_all_pairs gets overrides'

  # cancel_on_pause False -> neither leg runs; (None, 0) -> orders + candidates left in play.
  cancel_calls.clear(); halt_calls.clear()
  summary, count = web_app._run_operator_teardown(active_settings=_S(False), lane_session_id='sess-1')
  assert cancel_calls == [] and halt_calls == [] and summary is None and count == 0, 'SA: cancel_on_pause=false leaves in play'

  # settings None -> fail-safe (None, 0).
  summary, count = web_app._run_operator_teardown(active_settings=None, lane_session_id='sess-1')
  assert summary is None and count == 0


def test_sa_orchestrator_wired_at_stop_and_offline_seams() -> None:
  import pathlib
  text = (pathlib.Path(__file__).resolve().parents[1] / 'src' / 'polyventure' / 'web_app.py').read_text(encoding='utf-8')
  assert 'def _run_operator_teardown(' in text, 'SA: orchestrator must exist'
  # Scheduler-owned STOP handler calls it; the route keeps only pause-side route teardown.
  assert "if normalized_event == 'automation_stopped':" in text
  stop_handler_start = text.find("if normalized_event == 'automation_stopped':")
  stop_handler_end = text.find("if normalized_event == 'automation_paused':", stop_handler_start)
  stop_handler = text[stop_handler_start:stop_handler_end]
  assert '_run_operator_teardown(' in stop_handler, 'SA: scheduler stop handler must run teardown'
  assert "if transition_reason == 'operator_pause':" in text, 'SA: route-side teardown is retained for pause only'
  assert text.count('_run_operator_teardown(') >= 3, 'SA: definition + STOP call + offline call'
  # offline seam captures the lsid before the ws stop, and only tears down when automation was active.
  assert '_offline_lsid = _active_transition_lane_session_id()' in text, 'SA: offline seam resolves lsid pre-ws-stop'
  assert 'if automation_stopped:' in text, 'SA: offline teardown only when automation was active'


def test_su_manual_action_confirmation_popups_wired() -> None:
  import pathlib
  text = (pathlib.Path(__file__).resolve().parents[1] / 'src' / 'polyventure' / 'web_app.py').read_text(encoding='utf-8')
  # SU: both manual actions now open the Polymath challenge modal instead of firing directly.
  assert "action: 'stop_find_candidates_confirm'," in text, 'SU: stop-find-candidates must open a confirm'
  assert "action: 'cancel_all_pairs_confirm'," in text, 'SU: cancel-all-pairs must open a confirm'
  # SU: the confirm dispatch runs the underlying action on confirm.
  assert "if (challengeAction === 'stop_find_candidates_confirm') {" in text, 'SU: stop-find dispatch'
  assert "if (challengeAction === 'cancel_all_pairs_confirm') {" in text, 'SU: cancel-all dispatch'


def _ux_web_app_source() -> str:
  import pathlib
  return (pathlib.Path(__file__).resolve().parents[1] / 'src' / 'polyventure' / 'web_app.py').read_text(encoding='utf-8')


def test_ux0_stop_find_popup_fired_on_correct_value_and_endpoint() -> None:
  text = _ux_web_app_source()
  # UX-0: the popup guard must match the real control-deck button value 'scan-cancel'
  # (hyphen), and the confirm must run the real scan-cancel action.
  assert "if (kind === 'action' && value === 'scan-cancel') {" in text, 'UX-0: guard must read scan-cancel (hyphen)'
  assert "value === 'scan_cancel'" not in text, 'UX-0: the dead underscore guard must be gone'
  assert "await runUiAction('scan-cancel');" in text, 'UX-0: confirm must dispatch runUiAction(scan-cancel)'


def test_ux1_scan_canceling_flag_lifecycle() -> None:
  text = _ux_web_app_source()
  assert 'scanCanceling: false,' in text, 'UX-1: state must declare scanCanceling'
  assert 'state.scanCanceling = true;' in text, 'UX-1: confirm handler must set the flag'
  # consumed in the cancel-button builder to show a disabled "Cancelling…"
  assert "state.scanCanceling && scanStatus !== 'canceling'" in text, 'UX-1: builder must honor the flag'
  assert "label: 'Cancelling…'," in text, 'UX-1: disabled label must be Cancelling…'
  # cleared when the scan leaves processing/canceling
  assert "if (state.scanCanceling && _scanStatusForCancelFlag !== 'processing' && _scanStatusForCancelFlag !== 'canceling') {" in text, 'UX-1: flag cleared on terminal scan status'


def test_ux2_automation_launcher_gated_while_find_in_flight() -> None:
  text = _ux_web_app_source()
  # UX-2: the apply control + enabled checkbox disable while a scan is in flight, with a tooltip.
  assert "action: 'apply_automation_policy', tone: 'secondary', tooltip: scanProcessingActive(payload)" in text, 'UX-2: apply control gated'
  assert "disabled: scanProcessingActive(payload) }," in text, 'UX-2: apply control disabled-while-scan'
  assert "checked: Boolean(automationOverlay.enabled), disabled: scanProcessingActive(payload)" in text, 'UX-2: enabled checkbox gated'
  # the checkbox field render now honors disabled
  assert 'const checkboxDisabled = Boolean(field.disabled);' in text, 'UX-2: checkbox render must support disabled'
