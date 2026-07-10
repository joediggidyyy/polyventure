from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from polyventure.config import Settings
from polyventure.http_client import KalshiHttpError
from polyventure.sandbox_preflight import run_sandbox_preflight


def _settings(*, state_db_path: str, active_websocket_url: str = 'wss://demo-api.kalshi.example/trade-api/ws/v2') -> Settings:
  return Settings(
    kalshi_env='demo',
    api_key_id='key-id',
    private_key_file='secrets/demo.pem',
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.example/trade-api/v2',
    websocket_url=active_websocket_url,
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
    state_db_path=state_db_path,
    operation_lane='sandbox',
    sandbox_websocket_url=active_websocket_url,
    live_websocket_url='wss://api.kalshi.example/trade-api/ws/v2',
    active_websocket_url=active_websocket_url,
  )


def test_run_sandbox_preflight_returns_pass_and_writes_log(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _settings(state_db_path=str(tmp_path / 'state.sqlite3'))

  monkeypatch.setattr('polyventure.sandbox_preflight.resolve_private_key_path', lambda _settings: tmp_path / 'demo.pem')
  monkeypatch.setattr('polyventure.sandbox_preflight.load_private_key', lambda _path: object())

  class _OkClient:
    def __init__(self, settings: Any, private_key: object) -> None:
      _ = settings
      _ = private_key

    def get_markets(self, status: str = 'open', limit: int = 1) -> tuple[list[Any], None]:
      _ = status
      _ = limit
      return ([], None)

    def get_account_api_limits(self) -> dict[str, Any]:
      return {
        'usage_tier': 'standard',
      }

  result = run_sandbox_preflight(settings, project_root=tmp_path, client_factory=_OkClient)

  assert result['result'] == 'pass'
  assert result['reason_code'] == 'preflight_passed'
  log_path = tmp_path / 'logs' / 'sandbox_mode_change_preflight.jsonl'
  assert log_path.exists()
  record = json.loads(log_path.read_text(encoding='utf-8').splitlines()[-1])
  assert record['result'] == 'pass'
  assert record['operation_lane'] == 'sandbox'


def test_run_sandbox_preflight_fails_for_invalid_websocket_url(tmp_path: Path) -> None:
  settings = _settings(
    state_db_path=str(tmp_path / 'state.sqlite3'),
    active_websocket_url='https://not-a-websocket.example/endpoint',
  )

  result = run_sandbox_preflight(settings, project_root=tmp_path)

  assert result['result'] == 'fail'
  assert result['reason_code'] == 'websocket_endpoint_validation_failed'


def test_run_sandbox_preflight_fails_credential_acceptance(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _settings(state_db_path=str(tmp_path / 'state.sqlite3'))

  monkeypatch.setattr('polyventure.sandbox_preflight.resolve_private_key_path', lambda _settings: tmp_path / 'demo.pem')
  monkeypatch.setattr('polyventure.sandbox_preflight.load_private_key', lambda _path: object())

  class _FailClient:
    def __init__(self, settings: Any, private_key: object) -> None:
      _ = settings
      _ = private_key

    def get_markets(self, status: str = 'open', limit: int = 1) -> tuple[list[Any], None]:
      _ = status
      _ = limit
      raise RuntimeError('auth rejected')

  result = run_sandbox_preflight(settings, project_root=tmp_path, client_factory=_FailClient)

  assert result['result'] == 'fail'
  assert result['reason_code'] == 'credential_acceptance_failed'
  log_path = tmp_path / 'logs' / 'sandbox_mode_change_preflight.jsonl'
  record = json.loads(log_path.read_text(encoding='utf-8').splitlines()[-1])
  assert record['reason_code'] == 'credential_acceptance_failed'


def test_run_sandbox_preflight_fails_for_credential_environment_mismatch(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _settings(state_db_path=str(tmp_path / 'state.sqlite3'))

  monkeypatch.setattr('polyventure.sandbox_preflight.resolve_private_key_path', lambda _settings: tmp_path / 'demo.pem')
  monkeypatch.setattr('polyventure.sandbox_preflight.load_private_key', lambda _path: object())

  class _ProdOnlyClient:
    __module__ = 'polyventure.http_client'

    def __init__(self, settings: Any, private_key: object) -> None:
      self.settings = settings
      _ = private_key

    def get_markets(self, status: str = 'open', limit: int = 1) -> tuple[list[Any], None]:
      _ = status
      _ = limit
      return ([], None)

    def get_account_api_limits(self) -> dict[str, Any]:
      if '//api.kalshi.example' in str(self.settings.api_base_url):
        return {'usage_tier': 'standard'}
      raise KalshiHttpError(
        'auth_failed',
        'Kalshi rejected the authenticated request.',
        'Verify credentials.',
      )

  result = run_sandbox_preflight(settings, project_root=tmp_path, client_factory=_ProdOnlyClient)

  assert result['result'] == 'fail'
  assert result['reason_code'] == 'credential_environment_mismatch'


def test_run_sandbox_preflight_records_distinct_websocket_validation_and_probe_checks(tmp_path: Path, monkeypatch: Any) -> None:
  settings = _settings(state_db_path=str(tmp_path / 'state.sqlite3'))

  monkeypatch.setattr('polyventure.sandbox_preflight.resolve_private_key_path', lambda _settings: tmp_path / 'demo.pem')
  monkeypatch.setattr('polyventure.sandbox_preflight.load_private_key', lambda _path: object())

  observed_probe_modes: list[bool] = []

  def _fake_check_websocket_endpoint(
    _settings: Any,
    *,
    private_key: Any | None = None,
    perform_connect_probe: bool = True,
  ) -> tuple[bool, str | None, str | None]:
    _ = private_key
    observed_probe_modes.append(perform_connect_probe)
    return (True, None, None)

  monkeypatch.setattr('polyventure.sandbox_preflight._check_websocket_endpoint', _fake_check_websocket_endpoint)

  class _DemoClient:
    __module__ = 'polyventure.http_client'

    def __init__(self, settings: Any, private_key: object) -> None:
      self.settings = settings
      _ = private_key

    def get_markets(self, status: str = 'open', limit: int = 1) -> tuple[list[Any], None]:
      _ = status
      _ = limit
      return ([], None)

    def get_account_api_limits(self) -> dict[str, Any]:
      if '//demo-api.kalshi.example' in str(self.settings.api_base_url):
        return {'usage_tier': 'standard'}
      raise KalshiHttpError(
        'auth_failed',
        'Kalshi rejected the authenticated request.',
        'Verify credentials.',
      )

  result = run_sandbox_preflight(settings, project_root=tmp_path, client_factory=_DemoClient)

  assert result['result'] == 'pass'
  assert [check['name'] for check in result['checks']] == [
    'websocket_endpoint_validation',
    'credential_acceptance',
    'websocket_connect_probe',
    'persistence_readability',
  ]
  assert observed_probe_modes == [False, True]
