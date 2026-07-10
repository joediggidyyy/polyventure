from __future__ import annotations

import os
from pathlib import Path

import pytest

from polyventure.config import load_settings, resolve_private_key_path


REQUIRED_VALUES = {
  'KALSHI_ENV': 'demo',
  'KALSHI_API_KEY_ID': 'key-id',
  'KALSHI_API_BASE_URL': 'https://demo-api.kalshi.co/trade-api/v2',
  'KALSHI_OPERATION_LANE': 'sandbox',
  'KALSHI_SANDBOX_WEBSOCKET_URL': 'wss://demo-api.kalshi.co/trade-api/ws/v2',
  'KALSHI_LIVE_WEBSOCKET_URL': 'wss://api.elections.kalshi.com/trade-api/ws/v2',
  'KALSHI_SCAN_INTERVAL_MS': '2000',
  'KALSHI_ENTRY_WINDOW_START_SEC': '900',
  'KALSHI_ENTRY_WINDOW_END_SEC': '60',
  'KALSHI_MIN_EDGE_DOLLARS': '0.03',
  'KALSHI_FEE_RESERVE_DOLLARS': '0.02',
  'KALSHI_MIN_PROFIT_DOLLARS': '0.01',
  'KALSHI_MAX_PAIR_CONTRACTS': '10',
  'KALSHI_MAX_OPEN_PAIRS': '20',
  'KALSHI_MAX_UNHEDGED_SEC': '5',
  'KALSHI_POST_SUBMIT_PROCESSING_BUFFER_SEC': '180',
  'KALSHI_CANCEL_ON_PAUSE': 'true',
  'KALSHI_LOG_LEVEL': 'INFO',
  'KALSHI_STATE_DB_PATH': 'var/kalshi.sqlite3',
}


@pytest.fixture(autouse=True)
def _clear_force_local_dotenv(monkeypatch) -> None:
  monkeypatch.delenv('KALSHI_FORCE_LOCAL_DOTENV', raising=False)


def test_load_settings_from_environment(monkeypatch) -> None:
  for key, value in REQUIRED_VALUES.items():
    monkeypatch.setenv(key, value)
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_FILE', 'secrets/demo.pem')

  settings = load_settings()

  assert settings.kalshi_env == 'demo'
  assert settings.operation_lane == 'sandbox'
  assert settings.private_key_file == 'secrets/demo.pem'
  assert settings.sandbox_websocket_url == 'wss://demo-api.kalshi.co/trade-api/ws/v2'
  assert settings.live_websocket_url == 'wss://api.elections.kalshi.com/trade-api/ws/v2'
  assert settings.active_websocket_url == 'wss://demo-api.kalshi.co/trade-api/ws/v2'
  assert settings.websocket_url == settings.active_websocket_url
  assert settings.max_open_pairs == 20
  assert settings.post_submit_processing_buffer_sec == 180
  assert settings.min_pair_notional_pct == 0.05
  assert settings.max_pair_notional_pct == 0.20
  assert settings.target_deployment_pct == 0.60


def test_load_settings_uses_legacy_websocket_alias_for_selected_lane(monkeypatch) -> None:
  for key, value in REQUIRED_VALUES.items():
    monkeypatch.setenv(key, value)
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_FILE', 'secrets/demo.pem')
  monkeypatch.setenv('KALSHI_OPERATION_LANE', 'live')
  monkeypatch.setenv('KALSHI_SANDBOX_WEBSOCKET_URL', '')
  monkeypatch.setenv('KALSHI_LIVE_WEBSOCKET_URL', '')
  monkeypatch.setenv('KALSHI_WEBSOCKET_URL', 'wss://api.elections.kalshi.com/trade-api/ws/v2')

  settings = load_settings()

  assert settings.operation_lane == 'live'
  assert settings.live_websocket_url == 'wss://api.elections.kalshi.com/trade-api/ws/v2'
  assert settings.sandbox_websocket_url == ''
  assert settings.active_websocket_url == 'wss://api.elections.kalshi.com/trade-api/ws/v2'
  assert settings.websocket_url == 'wss://api.elections.kalshi.com/trade-api/ws/v2'


def test_settings_for_lane_resolves_lane_scoped_credentials_and_env(monkeypatch) -> None:
  from polyventure.config import settings_for_lane

  for key, value in REQUIRED_VALUES.items():
    monkeypatch.setenv(key, value)
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_FILE', 'secrets/demo.pem')
  monkeypatch.setenv('KALSHI_SANDBOX_API_KEY_ID', 'sandbox-key-id')
  monkeypatch.setenv('KALSHI_SANDBOX_PRIVATE_KEY_FILE', 'secrets/sandbox.pem')
  monkeypatch.setenv('KALSHI_LIVE_API_KEY_ID', 'live-key-id')
  monkeypatch.setenv('KALSHI_LIVE_PRIVATE_KEY_FILE', 'secrets/live.pem')

  base = load_settings()

  live = settings_for_lane(base, 'live')
  assert live.operation_lane == 'live'
  assert live.kalshi_env == 'prod'
  assert live.api_key_id == 'live-key-id'
  assert live.private_key_file == 'secrets/live.pem'
  assert live.active_websocket_url == REQUIRED_VALUES['KALSHI_LIVE_WEBSOCKET_URL']

  sandbox = settings_for_lane(base, 'sandbox')
  assert sandbox.operation_lane == 'sandbox'
  assert sandbox.kalshi_env == 'demo'
  assert sandbox.api_key_id == 'sandbox-key-id'
  assert sandbox.private_key_file == 'secrets/sandbox.pem'
  assert sandbox.active_websocket_url == REQUIRED_VALUES['KALSHI_SANDBOX_WEBSOCKET_URL']


def test_settings_for_lane_live_does_not_fall_back_to_generic_credentials(monkeypatch) -> None:
  from dataclasses import replace

  from polyventure.config import settings_for_lane

  for key, value in REQUIRED_VALUES.items():
    monkeypatch.setenv(key, value)
  monkeypatch.setenv('KALSHI_API_KEY_ID', 'generic-sandbox-key-id')
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_FILE', 'secrets/demo.pem')

  # Build a base with a generic credential present but the live-scoped fields
  # explicitly empty (the dotenv search would otherwise supply them).
  base = replace(load_settings(), live_api_key_id='', live_private_key_file='')

  live = settings_for_lane(base, 'live')

  # Fail closed: the live lane must NOT borrow the generic/sandbox credential.
  assert live.api_key_id == ''
  assert live.private_key_file is None


def test_settings_for_lane_requires_explicit_operative_lane(monkeypatch) -> None:
  from polyventure.config import settings_for_lane

  for key, value in REQUIRED_VALUES.items():
    monkeypatch.setenv(key, value)
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_FILE', 'secrets/demo.pem')
  base = load_settings()

  for bad_lane in ('offline', '', 'sandbox-ish'):
    with pytest.raises(ValueError):
      settings_for_lane(base, bad_lane)


def test_load_settings_uses_dynamic_sizing_defaults(monkeypatch) -> None:
  for key, value in REQUIRED_VALUES.items():
    monkeypatch.setenv(key, value)
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_FILE', 'secrets/demo.pem')
  monkeypatch.delenv('KALSHI_MIN_PAIR_NOTIONAL_PCT', raising=False)
  monkeypatch.delenv('KALSHI_MAX_PAIR_NOTIONAL_PCT', raising=False)
  monkeypatch.delenv('KALSHI_TARGET_DEPLOYMENT_PCT', raising=False)
  monkeypatch.delenv('KALSHI_DENSITY_ALPHA', raising=False)
  monkeypatch.delenv('KALSHI_DENSITY_EDGE_REF', raising=False)
  monkeypatch.delenv('KALSHI_DENSITY_LIQUIDITY_REF', raising=False)

  settings = load_settings()

  assert settings.min_pair_notional_pct == 0.05
  assert settings.max_pair_notional_pct == 0.20
  assert settings.target_deployment_pct == 0.60
  assert settings.density_alpha == 0.20
  assert settings.density_edge_ref == 0.05
  assert settings.density_liquidity_ref == 100.0


def test_resolve_private_key_path_prefers_file(monkeypatch, tmp_path: Path) -> None:
  pem_path = tmp_path / 'demo.pem'
  pem_path.write_text('placeholder', encoding='utf-8')

  for key, value in REQUIRED_VALUES.items():
    monkeypatch.setenv(key, value)
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_FILE', str(pem_path))
  monkeypatch.delenv('KALSHI_PRIVATE_KEY_PATH', raising=False)
  monkeypatch.delenv('KALSHI_PRIVATE_KEY_INLINE', raising=False)

  settings = load_settings()
  resolved = resolve_private_key_path(settings)

  assert resolved == pem_path.resolve()


def test_load_settings_prefers_nearest_dotenv_tuple_when_legacy_inline_env_present(monkeypatch, tmp_path: Path) -> None:
  local_env = tmp_path / '.env'
  local_env.write_text(
    '\n'.join([
      'KALSHI_ENV=demo',
      'KALSHI_API_KEY_ID=local-key-id',
      'KALSHI_PRIVATE_KEY_FILE=secrets/local.pem',
      'KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2',
      'KALSHI_WEBSOCKET_URL=wss://demo-api.kalshi.co/trade-api/ws/v2',
      'KALSHI_SCAN_INTERVAL_MS=2000',
      'KALSHI_ENTRY_WINDOW_START_SEC=900',
      'KALSHI_ENTRY_WINDOW_END_SEC=60',
      'KALSHI_MIN_EDGE_DOLLARS=0.03',
      'KALSHI_FEE_RESERVE_DOLLARS=0.02',
      'KALSHI_MIN_PROFIT_DOLLARS=0.01',
      'KALSHI_MAX_PAIR_CONTRACTS=10',
      'KALSHI_MAX_OPEN_PAIRS=20',
      'KALSHI_MAX_UNHEDGED_SEC=5',
      'KALSHI_CANCEL_ON_PAUSE=true',
      'KALSHI_LOG_LEVEL=INFO',
      'KALSHI_STATE_DB_PATH=var/kalshi.sqlite3',
    ]),
    encoding='utf-8',
  )

  monkeypatch.chdir(tmp_path)
  monkeypatch.setenv('KALSHI_API_KEY_ID', 'inherited-key-id')
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_PATH', '-----BEGIN RSA PRIVATE KEY-----')

  settings = load_settings()

  assert settings.api_key_id == 'local-key-id'
  assert settings.private_key_file == 'secrets/local.pem'


def test_load_settings_can_force_nearest_dotenv_tuple_without_inline_legacy(monkeypatch, tmp_path: Path) -> None:
  local_env = tmp_path / '.env'
  local_env.write_text(
    '\n'.join([
      'KALSHI_ENV=demo',
      'KALSHI_API_KEY_ID=local-key-id',
      'KALSHI_PRIVATE_KEY_FILE=secrets/local.pem',
      'KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2',
      'KALSHI_WEBSOCKET_URL=wss://demo-api.kalshi.co/trade-api/ws/v2',
      'KALSHI_SCAN_INTERVAL_MS=2000',
      'KALSHI_ENTRY_WINDOW_START_SEC=900',
      'KALSHI_ENTRY_WINDOW_END_SEC=60',
      'KALSHI_MIN_EDGE_DOLLARS=0.03',
      'KALSHI_FEE_RESERVE_DOLLARS=0.02',
      'KALSHI_MIN_PROFIT_DOLLARS=0.01',
      'KALSHI_MAX_PAIR_CONTRACTS=10',
      'KALSHI_MAX_OPEN_PAIRS=20',
      'KALSHI_MAX_UNHEDGED_SEC=5',
      'KALSHI_CANCEL_ON_PAUSE=true',
      'KALSHI_LOG_LEVEL=INFO',
      'KALSHI_STATE_DB_PATH=var/kalshi.sqlite3',
    ]),
    encoding='utf-8',
  )

  monkeypatch.chdir(tmp_path)
  monkeypatch.setenv('KALSHI_API_KEY_ID', 'inherited-key-id')
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_FILE', 'secrets/inherited.pem')
  monkeypatch.setenv('KALSHI_FORCE_LOCAL_DOTENV', 'true')

  settings = load_settings()

  assert settings.api_key_id == 'local-key-id'
  assert settings.private_key_file == 'secrets/local.pem'
  assert settings.state_db_path == 'var/kalshi.sqlite3'
  assert settings.sandbox_websocket_url == 'wss://demo-api.kalshi.co/trade-api/ws/v2'
  assert settings.active_websocket_url == 'wss://demo-api.kalshi.co/trade-api/ws/v2'


def test_load_settings_force_local_dotenv_overrides_inherited_websocket_env(monkeypatch, tmp_path: Path) -> None:
  local_env = tmp_path / '.env'
  local_env.write_text(
    '\n'.join([
      'KALSHI_ENV=demo',
      'KALSHI_API_KEY_ID=local-key-id',
      'KALSHI_PRIVATE_KEY_FILE=secrets/local.pem',
      'KALSHI_API_BASE_URL=https://demo-api.kalshi.co/trade-api/v2',
      'KALSHI_OPERATION_LANE=sandbox',
      'KALSHI_WEBSOCKET_URL=wss://demo-api.kalshi.co/trade-api/ws/v2',
      'KALSHI_SCAN_INTERVAL_MS=2000',
      'KALSHI_ENTRY_WINDOW_START_SEC=900',
      'KALSHI_ENTRY_WINDOW_END_SEC=60',
      'KALSHI_MIN_EDGE_DOLLARS=0.03',
      'KALSHI_FEE_RESERVE_DOLLARS=0.02',
      'KALSHI_MIN_PROFIT_DOLLARS=0.01',
      'KALSHI_MAX_PAIR_CONTRACTS=10',
      'KALSHI_MAX_OPEN_PAIRS=20',
      'KALSHI_MAX_UNHEDGED_SEC=5',
      'KALSHI_CANCEL_ON_PAUSE=true',
      'KALSHI_LOG_LEVEL=INFO',
      'KALSHI_STATE_DB_PATH=var/kalshi.sqlite3',
    ]),
    encoding='utf-8',
  )

  monkeypatch.chdir(tmp_path)
  monkeypatch.setenv('KALSHI_API_KEY_ID', 'inherited-key-id')
  monkeypatch.setenv('KALSHI_PRIVATE_KEY_FILE', 'secrets/inherited.pem')
  monkeypatch.setenv('KALSHI_SANDBOX_WEBSOCKET_URL', 'wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2')
  monkeypatch.setenv('KALSHI_LIVE_WEBSOCKET_URL', 'wss://external-api-ws.kalshi.com/trade-api/ws/v2')
  monkeypatch.setenv('KALSHI_FORCE_LOCAL_DOTENV', 'true')

  settings = load_settings()

  assert settings.api_key_id == 'local-key-id'
  assert settings.private_key_file == 'secrets/local.pem'
  assert settings.sandbox_websocket_url == 'wss://demo-api.kalshi.co/trade-api/ws/v2'
  assert settings.live_websocket_url == ''
  assert settings.active_websocket_url == 'wss://demo-api.kalshi.co/trade-api/ws/v2'
