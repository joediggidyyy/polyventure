from __future__ import annotations

import asyncio
import json
import time
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable

from .auth import load_private_key
from .config import Settings, derive_rest_api_base_url, resolve_private_key_path, websocket_url_is_valid
from .http_client import KalshiHttpClient, KalshiHttpError
from .persistence import open_database, summarize_persistence
from .websocket_client import KalshiWebSocketClient, WebSocketAuthError, WebSocketError, WebSocketServiceUnavailableError


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PreflightClientFactory = Callable[[Settings, object], Any]


def _utc_now_iso() -> str:
  return datetime.now(UTC).isoformat().replace('+00:00', 'Z')


def _check_websocket_endpoint(
  settings: Settings,
  *,
  private_key: Any | None = None,
  perform_connect_probe: bool = True,
) -> tuple[bool, str | None, str | None]:
  websocket_url = str(settings.active_websocket_url or settings.sandbox_websocket_url or '').strip()
  if not websocket_url:
    return (False, 'sandbox_websocket_unconfigured', 'Configure a sandbox websocket URL before switching lanes.')
  if not websocket_url_is_valid(websocket_url):
    return (False, 'websocket_endpoint_validation_failed', 'The sandbox websocket URL is invalid. Load a valid websocket endpoint and retry mode change.')

  if not perform_connect_probe:
    return (True, None, None)
  if private_key is None:
    return (
      False,
      'credential_acceptance_failed',
      'A valid private key is required for websocket authentication checks.',
    )

  client = KalshiWebSocketClient(
    ws_url=websocket_url,
    api_key_id=str(settings.api_key_id or '').strip(),
    private_key=private_key,
    on_message=None,
  )

  async def _probe() -> None:
    await client.connect()
    await client.disconnect()

  try:
    asyncio.run(_probe())
  except WebSocketAuthError:
    return (
      False,
      'credential_acceptance_failed',
      'Credential acceptance checks failed on authenticated account endpoints. Verify API key and private key pairing before retrying mode change.',
    )
  except WebSocketServiceUnavailableError:
    return (
      False,
      'websocket_service_unavailable',
      'The websocket endpoint is temporarily unavailable. Retry mode change when the service recovers.',
    )
  except WebSocketError:
    return (False, 'websocket_connection_failed', 'The websocket connectivity probe failed. Review endpoint posture and retry mode change.')
  except RuntimeError:
    return (False, 'websocket_connection_failed', 'The websocket connectivity probe could not execute in the current runtime context.')
  return (True, None, None)


def _detect_key_environment(
  settings: Settings,
  private_key: Any,
  *,
  client_factory: PreflightClientFactory,
) -> tuple[str | None, str | None, str | None]:
  """
  Auto-detect which environment (demo or live) the key belongs to.
  Tries demo endpoint first, then live. Returns (environment_name, api_base_url, error_message_if_failed).
  """
  environments = [
    (env_name, url)
    for env_name, url in (
      ('demo', derive_rest_api_base_url(str(getattr(settings, 'sandbox_websocket_url', '') or ''))),
      ('live', derive_rest_api_base_url(str(getattr(settings, 'live_websocket_url', '') or ''))),
    )
    if url
  ]
  
  for env_name, api_base_url in environments:
    try:
      env_settings = replace(settings, api_base_url=api_base_url)
      client = client_factory(env_settings, private_key)
      client.get_markets(status='open', limit=1)
      client.get_account_api_limits()
      # Both calls succeeded; return detected environment
      return (env_name, api_base_url, None)
    except KalshiHttpError as exc:
      reason_code = str(getattr(exc, 'reason_code', '')).lower()
      if reason_code == 'auth_failed':
        # Auth failed on this environment; try next environment
        continue
      if reason_code == 'network_timeout':
        return (None, None, f'{env_name} environment: network timeout')
      # Other HTTP errors; try next environment
      continue
    except Exception:
      # Connection or parsing errors; try next environment
      continue
  
  return (None, None, 'Key does not validate against demo or production environments. Verify API key and private key pairing.')


def _check_credential_acceptance(
  settings: Settings,
  *,
  client_factory: PreflightClientFactory,
  private_key: Any | None = None,
) -> tuple[bool, str | None, str | None, str | None, str | None]:
  """
  Validate credentials and detect environment.
  Returns (success, reason_code, next_action, detected_api_base_url, detected_env_name).
  """
  if not str(settings.api_key_id or '').strip():
    return (False, 'credential_acceptance_failed', 'An API key id is required before sandbox mode can be activated.', None, None)
  try:
    resolved_private_key = private_key
    if resolved_private_key is None:
      private_key_path = resolve_private_key_path(settings)
      resolved_private_key = load_private_key(private_key_path)
    
    # Auto-detect which environment (demo or live) the key belongs to
    detected_env, detected_url, detection_error = _detect_key_environment(settings, resolved_private_key, client_factory=client_factory)
    
    if detected_env is None:
      return (
        False,
        'credential_acceptance_failed',
        detection_error or 'Credential acceptance checks failed on authenticated account endpoints. Verify API key and private key pairing before retrying mode change.',
        None,
        None,
      )
    
    # Key validated successfully against detected environment
    return (True, None, None, detected_url, detected_env)
    
  except KalshiHttpError as exc:
    reason_code = str(getattr(exc, 'reason_code', '')).lower()
    if reason_code == 'auth_failed':
      return (
        False,
        'credential_acceptance_failed',
        'Credential acceptance checks failed on authenticated account endpoints. Verify API key and private key pairing before retrying mode change.',
        None,
        None,
      )
    if reason_code == 'network_timeout':
      return (
        False,
        'credential_acceptance_timeout',
        'Credential acceptance checks timed out. Verify network connectivity and retry mode change.',
        None,
        None,
      )
    return (
      False,
      'credential_acceptance_failed',
      str(exc) or 'Credential acceptance checks failed before sandbox mode activation.',
      None,
      None,
    )
  except Exception:
    return (
      False,
      'credential_acceptance_failed',
      'Credential acceptance checks did not pass. Review API key and key-file posture before retrying mode change.',
      None,
      None,
    )


def _check_persistence_readability(settings: Settings) -> tuple[bool, str | None, str | None]:
  try:
    connection = open_database(settings.state_db_path)
    summary = summarize_persistence(connection, operation_lane=settings.operation_lane)
    if not isinstance(summary, dict) or 'table_counts' not in summary:
      return (
        False,
        'persistence_readability_failed',
        'Local persistence checks did not return expected evidence. Validate state DB posture before retrying mode change.',
      )
  except Exception:
    return (
      False,
      'persistence_readability_failed',
      'Local persistence checks failed. Validate state DB posture before retrying mode change.',
    )
  return (True, None, None)


def _append_preflight_log(project_root: Path, record: dict[str, Any]) -> None:
  log_dir = project_root / 'logs'
  log_dir.mkdir(parents=True, exist_ok=True)
  log_path = log_dir / 'sandbox_mode_change_preflight.jsonl'
  with log_path.open('a', encoding='utf-8') as handle:
    handle.write(json.dumps(record, sort_keys=True) + '\n')


def run_sandbox_preflight(
  settings: Settings,
  *,
  project_root: Path = PROJECT_ROOT,
  client_factory: PreflightClientFactory = KalshiHttpClient,
) -> dict[str, Any]:
  started = time.perf_counter()
  sandbox_websocket_url = str(settings.sandbox_websocket_url or settings.active_websocket_url or settings.websocket_url or '').strip()
  preflight_settings = replace(
    settings,
    operation_lane='sandbox',
    active_websocket_url=sandbox_websocket_url,
    websocket_url=sandbox_websocket_url,
    sandbox_websocket_url=sandbox_websocket_url,
  )
  checks: list[dict[str, Any]] = []
  detected_api_base_url: str | None = None
  detected_env_name: str | None = None
  supports_live_probe = str(getattr(client_factory, '__module__', '')) == 'polyventure.http_client'

  private_key: Any | None = None

  # Websocket check
  check_name = 'websocket_endpoint_validation'
  ok, reason_code, next_action = _check_websocket_endpoint(
    preflight_settings,
    perform_connect_probe=False,
  )
  checks.append({'name': check_name, 'status': 'pass' if ok else 'fail', 'reason_code': reason_code})
  if not ok:
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    result = {
      'result': 'fail',
      'reason_code': reason_code or 'mode_change_preflight_failed',
      'message': 'Sandbox mode pre-flight checks blocked activation.',
      'next_action': next_action or 'Resolve readiness posture before retrying mode change.',
      'elapsed_ms': elapsed_ms,
      'checks': checks,
    }
    _append_preflight_log(
      project_root,
      {
        'recorded_at_utc': _utc_now_iso(),
        'operation_lane': 'sandbox',
        'result': result['result'],
        'reason_code': result['reason_code'],
        'elapsed_ms': elapsed_ms,
        'checks': checks,
      },
    )
    return result

  if supports_live_probe:
    try:
      private_key_path = resolve_private_key_path(preflight_settings)
      private_key = load_private_key(private_key_path)
    except Exception:
      elapsed_ms = int((time.perf_counter() - started) * 1000)
      result = {
        'result': 'fail',
        'reason_code': 'credential_acceptance_failed',
        'message': 'Sandbox mode pre-flight checks blocked activation.',
        'next_action': 'Credential key material could not be loaded. Verify key-file posture before retrying mode change.',
        'elapsed_ms': elapsed_ms,
        'checks': checks,
      }
      _append_preflight_log(
        project_root,
        {
          'recorded_at_utc': _utc_now_iso(),
          'operation_lane': 'sandbox',
          'result': result['result'],
          'reason_code': result['reason_code'],
          'elapsed_ms': elapsed_ms,
          'checks': checks,
        },
      )
      return result

    # Credential check (returns detected_api_base_url on success)
    check_name = 'credential_acceptance'
    ok, reason_code, next_action, detected_api_base_url, detected_env_name = _check_credential_acceptance(
      preflight_settings,
      client_factory=client_factory,
      private_key=private_key,
    )
    checks.append({'name': check_name, 'status': 'pass' if ok else 'fail', 'reason_code': reason_code})
    if not ok:
      elapsed_ms = int((time.perf_counter() - started) * 1000)
      result = {
        'result': 'fail',
        'reason_code': reason_code or 'mode_change_preflight_failed',
        'message': 'Sandbox mode pre-flight checks blocked activation.',
        'next_action': next_action or 'Resolve readiness posture before retrying mode change.',
        'elapsed_ms': elapsed_ms,
        'checks': checks,
      }
      _append_preflight_log(
        project_root,
        {
          'recorded_at_utc': _utc_now_iso(),
          'operation_lane': 'sandbox',
          'result': result['result'],
          'reason_code': result['reason_code'],
          'elapsed_ms': elapsed_ms,
          'checks': checks,
        },
      )
      return result

    # Sandbox lane requires demo credentials.
    if str(detected_env_name or '').lower() != 'demo':
      elapsed_ms = int((time.perf_counter() - started) * 1000)
      result = {
        'result': 'fail',
        'reason_code': 'credential_environment_mismatch',
        'message': 'Sandbox mode pre-flight checks blocked activation.',
        'next_action': (
          'Detected non-demo API credentials while sandbox mode is selected. '
          'Load demo API key credentials or switch to the live lane before retrying mode change.'
        ),
        'elapsed_ms': elapsed_ms,
        'checks': checks,
      }
      _append_preflight_log(
        project_root,
        {
          'recorded_at_utc': _utc_now_iso(),
          'operation_lane': 'sandbox',
          'result': result['result'],
          'reason_code': result['reason_code'],
          'elapsed_ms': elapsed_ms,
          'checks': checks,
        },
      )
      return result

    check_name = 'websocket_connect_probe'
    ok, reason_code, next_action = _check_websocket_endpoint(
      preflight_settings,
      private_key=private_key,
      perform_connect_probe=True,
    )
  else:
    ok, reason_code, next_action = (True, None, None)

  checks.append({'name': check_name, 'status': 'pass' if ok else 'fail', 'reason_code': reason_code})
  if not ok:
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    result = {
      'result': 'fail',
      'reason_code': reason_code or 'mode_change_preflight_failed',
      'message': 'Sandbox mode pre-flight checks blocked activation.',
      'next_action': next_action or 'Resolve readiness posture before retrying mode change.',
      'elapsed_ms': elapsed_ms,
      'checks': checks,
    }
    _append_preflight_log(
      project_root,
      {
        'recorded_at_utc': _utc_now_iso(),
        'operation_lane': 'sandbox',
        'result': result['result'],
        'reason_code': result['reason_code'],
        'elapsed_ms': elapsed_ms,
        'checks': checks,
      },
    )
    return result

  if not supports_live_probe:
    # Credential check (returns detected_api_base_url on success)
    check_name = 'credential_acceptance'
    ok, reason_code, next_action, detected_api_base_url, detected_env_name = _check_credential_acceptance(
      preflight_settings,
      client_factory=client_factory,
      private_key=private_key,
    )
    checks.append({'name': check_name, 'status': 'pass' if ok else 'fail', 'reason_code': reason_code})
    if not ok:
      elapsed_ms = int((time.perf_counter() - started) * 1000)
      result = {
        'result': 'fail',
        'reason_code': reason_code or 'mode_change_preflight_failed',
        'message': 'Sandbox mode pre-flight checks blocked activation.',
        'next_action': next_action or 'Resolve readiness posture before retrying mode change.',
        'elapsed_ms': elapsed_ms,
        'checks': checks,
      }
      _append_preflight_log(
        project_root,
        {
          'recorded_at_utc': _utc_now_iso(),
          'operation_lane': 'sandbox',
          'result': result['result'],
          'reason_code': result['reason_code'],
          'elapsed_ms': elapsed_ms,
          'checks': checks,
        },
      )
      return result

  # Persistence check
  check_name = 'persistence_readability'
  ok, reason_code, next_action = _check_persistence_readability(preflight_settings)
  checks.append({'name': check_name, 'status': 'pass' if ok else 'fail', 'reason_code': reason_code})
  if not ok:
    elapsed_ms = int((time.perf_counter() - started) * 1000)
    result = {
      'result': 'fail',
      'reason_code': reason_code or 'mode_change_preflight_failed',
      'message': 'Sandbox mode pre-flight checks blocked activation.',
      'next_action': next_action or 'Resolve readiness posture before retrying mode change.',
      'elapsed_ms': elapsed_ms,
      'checks': checks,
    }
    _append_preflight_log(
      project_root,
      {
        'recorded_at_utc': _utc_now_iso(),
        'operation_lane': 'sandbox',
        'result': result['result'],
        'reason_code': result['reason_code'],
        'elapsed_ms': elapsed_ms,
        'checks': checks,
      },
    )
    return result

  elapsed_ms = int((time.perf_counter() - started) * 1000)
  result = {
    'result': 'pass',
    'reason_code': 'preflight_passed',
    'message': 'Sandbox mode pre-flight checks passed.',
    'next_action': 'Mode change may proceed.',
    'elapsed_ms': elapsed_ms,
    'checks': checks,
    'detected_api_base_url': detected_api_base_url,
  }
  _append_preflight_log(
    project_root,
    {
      'recorded_at_utc': _utc_now_iso(),
      'operation_lane': 'sandbox',
      'result': result['result'],
      'reason_code': result['reason_code'],
      'elapsed_ms': elapsed_ms,
      'checks': checks,
    },
  )
  return result