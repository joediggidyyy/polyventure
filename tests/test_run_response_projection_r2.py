"""Focused harness for charter row R2 — /api/run response projection persistence.

Covers:
  - the names-only / counts-only whitelist contract of
    `_build_run_response_projection`
  - the persistence side effect of the nested handler helper that runs
    immediately before every /api/run JSON response is returned
"""

from __future__ import annotations

import io
import json
import sqlite3
from pathlib import Path
from typing import Any

from polyventure import web_app
from polyventure.config import Settings
from polyventure.persistence import open_database
from polyventure.web_app import (
  OperatorConsoleServices,
  _build_run_response_projection,
  create_operator_console_app,
)


def _call_app_post(app: Any, path: str, *, query: str = '') -> tuple[str, str]:
  status_holder: dict[str, str] = {}

  def _start_response(status: str, _headers: list[tuple[str, str]]) -> None:
    status_holder['status'] = status

  environ: dict[str, Any] = {
    'REQUEST_METHOD': 'POST',
    'PATH_INFO': path,
    'QUERY_STRING': query,
    'CONTENT_LENGTH': '0',
    'wsgi.input': io.BytesIO(b''),
  }
  body = b''.join(app(environ, _start_response)).decode('utf-8')
  return status_holder['status'], body


def _minimal_services(run_payload: dict[str, Any]) -> OperatorConsoleServices:
  return OperatorConsoleServices(
    bootstrap=lambda **_: {'decision': 'planned'},
    scan=lambda **_: {'decision': 'planned'},
    run=lambda **_: run_payload,
    reconcile=lambda **_: {'pair_count': 0, 'pairs': []},
    report=lambda **_: {'decision': 'noop'},
    cancel_all=lambda **_: {'decision': 'noop'},
  )


def _settings_for(state_db_path: Path, key_file: Path) -> Settings:
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


def test_build_run_response_projection_returns_whitelist_defaults_for_empty_payload() -> None:
  projection = _build_run_response_projection({})
  assert projection == {
    'decision': '',
    'command_family': '',
    'mode': '',
    'dry_run': True,
    'market_count': None,
    'candidate_count': None,
    'planned_pair_count': None,
    'failure_class': None,
    'allowed_action_count': None,
    'next_action_present': False,
    'execution_chronology_enabled': False,
    'execution_terminal_state': None,
    'execution_profile': None,
  }


def test_build_run_response_projection_returns_whitelist_defaults_for_none_payload() -> None:
  projection = _build_run_response_projection(None)
  assert projection['decision'] == ''
  assert projection['dry_run'] is True
  assert projection['market_count'] is None


def test_build_run_response_projection_extracts_scalar_and_count_fields() -> None:
  payload = {
    'decision': 'planned',
    'command_family': 'polyventure run',
    'mode': 'ab_guarded',
    'dry_run': True,
    'market_count': 42,
    'candidate_count': 7,
    'planned_pair_count': 3,
    'failure_class': None,
    'allowed_actions': ['report', 'reconcile', 'cancel_all'],
    'next_action': 'Review candidates in Pairs.',
  }
  projection = _build_run_response_projection(payload)
  assert projection['decision'] == 'planned'
  assert projection['command_family'] == 'polyventure run'
  assert projection['mode'] == 'ab_guarded'
  assert projection['dry_run'] is True
  assert projection['market_count'] == 42
  assert projection['candidate_count'] == 7
  assert projection['planned_pair_count'] == 3
  assert projection['failure_class'] is None
  assert projection['allowed_action_count'] == 3
  assert projection['next_action_present'] is True


def test_build_run_response_projection_drops_unlisted_keys_including_secret_like_fields() -> None:
  payload = {
    'decision': 'planned',
    'settings': {'api_key_id': 'should-not-leak', 'private_key_inline': 'PEM-DATA'},
    'private_key_path_tail': 'demo.pem',
    'account_limits': {'balance': '1000'},
    'analytical_outputs': {'foo': 'bar'},
    'execution_chronology': {'event_packet': [{'sig': 'abc'}]},
    'planned_pairs': [{'pair_id': 'pair-1', 'ticker': 'KALSHI-EDGE-1'}],
  }
  projection = _build_run_response_projection(payload)
  # Whitelist enforcement: no secret-bearing or richly-structured fields appear.
  forbidden = {
    'settings',
    'private_key_path_tail',
    'account_limits',
    'analytical_outputs',
    'execution_chronology',
    'planned_pairs',
  }
  assert forbidden.isdisjoint(projection.keys())
  serialized = json.dumps(projection)
  assert 'PEM-DATA' not in serialized
  assert 'should-not-leak' not in serialized
  assert 'pair-1' not in serialized


def test_build_run_response_projection_coerces_non_int_counts_to_none() -> None:
  payload = {
    'decision': 'planned',
    'market_count': '42',
    'candidate_count': None,
    'planned_pair_count': 1.5,
  }
  projection = _build_run_response_projection(payload)
  assert projection['market_count'] is None
  assert projection['candidate_count'] is None
  assert projection['planned_pair_count'] is None


def test_build_run_response_projection_exposes_chronology_enabled_when_set() -> None:
  payload = {
    'execution_chronology': {
      'enabled': True,
      'terminal_state': 'RESTING_BOTH',
      'profile': 'submit_order_bridge',
    },
  }
  projection = _build_run_response_projection(payload)
  assert projection['execution_chronology_enabled'] is True
  assert projection['execution_terminal_state'] == 'RESTING_BOTH'
  assert projection['execution_profile'] == 'submit_order_bridge'


def test_build_run_response_projection_chronology_disabled_when_absent() -> None:
  projection = _build_run_response_projection({'decision': 'planned'})
  assert projection['execution_chronology_enabled'] is False
  assert projection['execution_terminal_state'] is None
  assert projection['execution_profile'] is None


def test_build_run_response_projection_chronology_disabled_when_not_enabled() -> None:
  payload = {'execution_chronology': {'enabled': False, 'profile': ''}}
  projection = _build_run_response_projection(payload)
  assert projection['execution_chronology_enabled'] is False


def test_build_run_response_projection_does_not_leak_chronology_event_packet() -> None:
  payload = {
    'execution_chronology': {
      'enabled': True,
      'terminal_state': 'RESTING_BOTH',
      'profile': 'submit_order_bridge',
      'event_packet': [{'order_id': 'secret-order-id', 'price': '0.54'}],
      'signed_evidence': {'signature': 'abc123'},
    }
  }
  projection = _build_run_response_projection(payload)
  serialized = json.dumps(projection)
  assert 'secret-order-id' not in serialized
  assert 'event_packet' not in serialized
  assert 'signed_evidence' not in serialized
  assert 'abc123' not in serialized


def test_build_run_response_projection_records_failure_class_as_string() -> None:
  projection = _build_run_response_projection({'failure_class': 'bridge_persistence_failed'})
  assert projection['failure_class'] == 'bridge_persistence_failed'


def test_run_route_persists_run_response_projection_event(monkeypatch: Any, tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime.sqlite3'
  key_file = tmp_path / 'demo.pem'
  key_file.write_text('demo-private-key', encoding='utf-8')
  settings = _settings_for(state_db_path, key_file)

  monkeypatch.setattr(web_app, '_resolve_settings', lambda *_a, **_kw: settings)
  monkeypatch.setattr(
    web_app,
    '_load_shell_settings_context',
    lambda **_kw: (
      {
        'settings_ready': True,
        'environment_ready': True,
        'credential_ready': True,
      },
      None,
    ),
  )

  app = create_operator_console_app(
    _minimal_services(
      {
        'decision': 'planned',
        'command_family': 'polyventure run',
        'mode': 'ab_guarded',
        'dry_run': True,
        'market_count': 42,
        'candidate_count': 7,
        'planned_pair_count': 3,
        'failure_class': None,
        'allowed_actions': ['report', 'reconcile'],
        'next_action': 'Review candidates in Pairs.',
      }
    )
  )

  # env query forces _active_settings to resolve a real Settings via _resolve_settings
  status, _body = _call_app_post(app, '/api/run', query='env=demo')
  assert status == '200 OK'

  with open_database(state_db_path) as connection:
    rows = connection.execute(
      'SELECT level, event_type, operation_lane, lane_session_id, detail_json'
      ' FROM runtime_events WHERE event_type = ?',
      ('run_response_projection',),
    ).fetchall()
  assert len(rows) == 1
  row = rows[0]
  assert row[0] == 'INFO'
  assert row[1] == 'run_response_projection'
  assert row[2] == 'sandbox'
  assert str(row[3] or '').startswith('run-')
  detail = json.loads(row[4])
  assert detail['decision'] == 'planned'
  assert detail['command_family'] == 'polyventure run'
  assert detail['market_count'] == 42
  assert detail['candidate_count'] == 7
  assert detail['planned_pair_count'] == 3
  assert detail['allowed_action_count'] == 2
  assert detail['next_action_present'] is True


def test_run_route_persistence_is_fail_closed_when_state_db_path_missing(
  monkeypatch: Any,
) -> None:
  # No state_db_path is wired; helper must swallow and the response must still succeed.
  app = create_operator_console_app(
    _minimal_services({'decision': 'planned'})
  )
  status, body = _call_app_post(app, '/api/run')
  assert status == '200 OK'
  payload = json.loads(body)
  assert payload['decision'] == 'planned'
