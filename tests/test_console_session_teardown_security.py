"""
Security sandbox proof tests — SB-1 through SB-5.

Policy flag: containment, security-surface
Calamum lane: sandbox_test
Catalog: polyventure-console-session-teardown / polyventure-console-session-teardown-sandbox-security-proof

All tests are fully mocked at the boundary — no live server, no network, no
real subprocess.  Each test proves a specific security invariant:

  SB-1  mark_root_loaded token gate (TD-2) — _closed_final is not reset on mismatch
  SB-2a /api/execution-status response contains only whitelisted fields
  SB-2b /api/execution-status active_pairs contains names only (no detail_json)
  SB-2c /api/execution-status returns safe defaults when DB is unavailable
  SB-3a launch block probe unreachable does not silently proceed
  SB-3b launch block fires on positive in_flight_count
  SB-4  tray _open_console uses explicit venv path (not PATH)
  SB-5  tray _abort_execution POST includes X-PV-Mutation-* signed headers
"""

from __future__ import annotations

import base64
import io
import json
import os
import sqlite3
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

from polyventure.web_app import (
    ConsoleSessionController,
    OperatorConsoleServices,
    create_operator_console_app,
)
from polyventure.tray import ExecutionTrayIcon, _sign_mutation_request


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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
                    'operator_policy': '',
                    'latest_runs': [],
                    'lane_policy': {},
                },
            },
            'report': {'latest_heartbeat': {'status': 'cycle-complete'}},
        },
        scan=lambda **_: {'decision': 'planned', 'candidate_count': 0},
        run=lambda **_: {'decision': 'planned'},
        reconcile=lambda **_: {'pair_count': 0, 'pairs': []},
        cancel_all=lambda **_: {'decision': 'planned'},
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

    def start_response(status: str, response_headers: list[tuple[str, str]]) -> None:
        status_holder['status'] = status
        for name, value in response_headers:
            headers_holder[name] = value

    environ: dict[str, Any] = {
        'REQUEST_METHOD': method,
        'PATH_INFO': path,
        'QUERY_STRING': query,
        'CONTENT_LENGTH': str(len(encoded_body)),
        'wsgi.input': io.BytesIO(encoded_body),
        'wsgi.errors': io.BytesIO(),
    }
    if headers:
        for k, v in headers.items():
            environ[f'HTTP_{k.upper().replace("-", "_")}'] = v
    response_parts = app(environ, start_response)
    body_str = b''.join(response_parts).decode('utf-8', errors='replace')
    return status_holder.get('status', ''), headers_holder, body_str


def _make_test_db(db_path: str, pairs: dict[str, str]) -> None:
    con = sqlite3.connect(db_path)
    con.execute('CREATE TABLE IF NOT EXISTS pair_plans (pair_id TEXT PRIMARY KEY)')
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
            (pair_id, state, json.dumps({'ticker': pair_id, 'secret': 'REDACTED'}), '2026-06-06T00:00:00Z'),
        )
    con.commit()
    con.close()


# ---------------------------------------------------------------------------
# SB-1: mark_root_loaded token gate — _closed_final must not reset on mismatch
# ---------------------------------------------------------------------------


def test_mark_root_loaded_token_mismatch_does_not_reset_closed_final() -> None:
    """
    SB-1: A root GET with a mismatched session token must not reset _closed_final.
    Fail-closed invariant: unauthenticated browser cannot cancel a shutdown countdown.
    """
    controller = ConsoleSessionController(session_token='session-abc', close_grace_sec=60.0)
    controller.mark_closed()
    assert controller._closed_final is True
    closed_at_before = controller._closed_at

    app = create_operator_console_app(_services(), session_controller=controller)
    status, headers, body = _call_app(app, method='GET', path='/', query='session=wrong-token')

    assert status == '200 OK', 'HTML must still be served on token mismatch'
    assert 'text/html' in headers.get('Content-Type', '')
    assert controller._closed_final is True, '_closed_final must remain True on token mismatch'
    assert controller._closed_at == closed_at_before, '_closed_at must not be cleared on token mismatch'
    assert controller._root_loaded_at is None, 'mark_root_loaded must not be called on token mismatch'


# ---------------------------------------------------------------------------
# SB-2: /api/execution-status data exposure surface
# ---------------------------------------------------------------------------


def test_execution_status_response_contains_only_whitelisted_fields(tmp_path: Any) -> None:
    """
    SB-2a: Response must contain only {decision, in_flight_count, active_pairs, drain_active}.
    No raw DB rows, no detail_json, no credentials.
    """
    db_path = str(tmp_path / 'state.sqlite3')
    _make_test_db(db_path, {'PAIR-X': 'RESTING_BOTH'})
    controller = ConsoleSessionController(session_token='tok', state_db_path=db_path)
    app = create_operator_console_app(_services(), session_controller=controller)

    status, _, body = _call_app(app, method='GET', path='/api/execution-status')
    payload = json.loads(body)

    assert status == '200 OK'
    # notification_source is framework metadata added to all JSON responses — non-sensitive.
    # automation_active is a non-sensitive boolean (FB-5) the tray reads to suppress the
    # execution-complete popup while automation is armed.
    allowed_keys = {'decision', 'in_flight_count', 'active_pairs', 'drain_active', 'notification_source', 'automation_active'}
    extra_keys = set(payload.keys()) - allowed_keys
    assert not extra_keys, f'Response contains non-whitelisted keys: {extra_keys}'


def test_execution_status_active_pairs_names_only(tmp_path: Any) -> None:
    """
    SB-2b: active_pairs must be a list of pair_id strings (names only).
    detail_json (which may contain sensitive trade data) must never appear.
    """
    db_path = str(tmp_path / 'state.sqlite3')
    _make_test_db(db_path, {'PAIR-A': 'RESTING_BOTH', 'PAIR-B': 'FULLY_FILLED'})
    controller = ConsoleSessionController(session_token='tok', state_db_path=db_path)
    app = create_operator_console_app(_services(), session_controller=controller)

    status, _, body = _call_app(app, method='GET', path='/api/execution-status')
    payload = json.loads(body)

    assert status == '200 OK'
    active_pairs = payload['active_pairs']
    assert isinstance(active_pairs, list)
    for item in active_pairs:
        assert isinstance(item, str), f'active_pairs must contain strings, got {type(item)}'
    assert sorted(active_pairs) == ['PAIR-A', 'PAIR-B']
    assert 'REDACTED' not in body, 'detail_json content must not appear in response'
    assert 'detail_json' not in body, 'detail_json key must not appear in response'


def test_execution_status_returns_safe_defaults_when_db_unavailable() -> None:
    """
    SB-2c: When state_db_path is None or DB does not exist, endpoint returns
    {in_flight_count: 0, active_pairs: [], drain_active: false} — no error surfaced.
    """
    controller = ConsoleSessionController(session_token='tok', state_db_path=None)
    app = create_operator_console_app(_services(), session_controller=controller)

    status, _, body = _call_app(app, method='GET', path='/api/execution-status')
    payload = json.loads(body)

    assert status == '200 OK'
    assert payload['in_flight_count'] == 0
    assert payload['active_pairs'] == []
    assert payload['drain_active'] is False


# ---------------------------------------------------------------------------
# SB-3: Launch block fail-closed behaviour (TD-6)
# ---------------------------------------------------------------------------


def test_launch_block_probe_unreachable_does_not_silently_proceed() -> None:
    """
    SB-3a: When the execution-status probe fails (server unreachable), the launch
    must be blocked — reason must be execution_status_probe_failed, not silently pass.
    """
    from polyventure.cli import _probe_execution_status

    with patch('polyventure.cli._probe_execution_status', return_value=None) as mock_probe:
        from polyventure.cli import launch_detached_operator_console
        with patch('polyventure.cli._collect_console_reuse_health_basis') as mock_basis:
            mock_basis.return_value = {
                'reusable': True,
                'root_probe_ok': True,
                'code_signature_match': True,
                'registry_entry_present': True,
                'listener_pid': 12345,
                'prune_registry_pid': None,
            }
            with patch('polyventure.cli._append_launcher_telemetry_event'), \
                 patch('polyventure.cli._console_browser_session_active', return_value=False), \
                 patch('polyventure.cli._console_code_signature', return_value='sig'), \
                 patch('polyventure.cli._console_reuse_reason_code', return_value='reuse_healthy'), \
                 patch('polyventure.cli._resolve_console_workspace_root', return_value=Path('/tmp')):
                result = launch_detached_operator_console(host='127.0.0.1', port=8765)

    assert result['decision'] == 'no-go'
    assert result['reason'] == 'execution_status_probe_failed'
    assert 'reattach_url' in result


def test_launch_block_fires_on_positive_in_flight_count() -> None:
    """
    SB-3b: When the execution-status probe returns in_flight_count > 0, the launch
    must be blocked — reason must be execution_in_progress with active pair names.
    """
    exec_status_response = {
        'decision': 'planned',
        'in_flight_count': 2,
        'active_pairs': ['PAIR-A', 'PAIR-B'],
        'drain_active': True,
    }
    with patch('polyventure.cli._probe_execution_status', return_value=exec_status_response):
        from polyventure.cli import launch_detached_operator_console
        with patch('polyventure.cli._collect_console_reuse_health_basis') as mock_basis:
            mock_basis.return_value = {
                'reusable': True,
                'root_probe_ok': True,
                'code_signature_match': True,
                'registry_entry_present': True,
                'listener_pid': 12345,
                'prune_registry_pid': None,
            }
            with patch('polyventure.cli._append_launcher_telemetry_event'), \
                 patch('polyventure.cli._console_browser_session_active', return_value=False), \
                 patch('polyventure.cli._console_code_signature', return_value='sig'), \
                 patch('polyventure.cli._console_reuse_reason_code', return_value='reuse_healthy'), \
                 patch('polyventure.cli._resolve_console_workspace_root', return_value=Path('/tmp')):
                result = launch_detached_operator_console(host='127.0.0.1', port=8765)

    assert result['decision'] == 'no-go'
    assert result['reason'] == 'execution_in_progress'
    assert result['in_flight_count'] == 2
    assert result['active_pairs'] == ['PAIR-A', 'PAIR-B']
    assert 'reattach_url' in result


def test_launch_block_fires_when_instance_already_running() -> None:
    """
    SB-3c: When the execution-status probe succeeds with in_flight_count=0, the
    launch must still be blocked — reason must be instance_already_running.
    A healthy reusable host is never a green light to open a second browser session.
    """
    exec_status_response = {
        'decision': 'planned',
        'in_flight_count': 0,
        'active_pairs': [],
        'drain_active': False,
    }
    with patch('polyventure.cli._probe_execution_status', return_value=exec_status_response):
        from polyventure.cli import launch_detached_operator_console
        with patch('polyventure.cli._collect_console_reuse_health_basis') as mock_basis:
            mock_basis.return_value = {
                'reusable': True,
                'root_probe_ok': True,
                'code_signature_match': True,
                'registry_entry_present': True,
                'listener_pid': 12345,
                'prune_registry_pid': None,
            }
            with patch('polyventure.cli._append_launcher_telemetry_event'), \
                 patch('polyventure.cli._console_browser_session_active', return_value=False), \
                 patch('polyventure.cli._console_code_signature', return_value='sig'), \
                 patch('polyventure.cli._console_reuse_reason_code', return_value='reuse_healthy'), \
                 patch('polyventure.cli._resolve_console_workspace_root', return_value=Path('/tmp')):
                result = launch_detached_operator_console(host='127.0.0.1', port=8765)

    assert result['decision'] == 'no-go'
    assert result['reason'] == 'instance_already_running'
    assert result['in_flight_count'] == 0
    assert 'reattach_url' in result


# ---------------------------------------------------------------------------
# SB-4: Tray _open_console path containment
# ---------------------------------------------------------------------------


def test_tray_open_console_uses_explicit_venv_path() -> None:
    """
    SB-4: _open_console must use Path(sys.executable).parent / 'polyventure',
    NOT a bare 'polyventure' string that would resolve via shell PATH.
    """
    import sys

    captured_calls: list[list[str]] = []

    def fake_popen(args: list[str], **kwargs: Any) -> MagicMock:
        captured_calls.append(list(args))
        return MagicMock()

    tray = ExecutionTrayIcon(
        host='127.0.0.1',
        port=8765,
        session_token='tok-abc',
        signing_key_b64=base64.b64encode(b'k' * 32).decode('ascii'),
        key_id='ui-session-tok-abc',
    )

    with patch('polyventure.tray.subprocess.Popen', side_effect=fake_popen):
        tray._open_console(None, None)

    assert len(captured_calls) == 1
    called_script = captured_calls[0][0]
    expected_dir = str(Path(sys.executable).parent)
    assert called_script.startswith(expected_dir), (
        f'_open_console must use the venv-local polyventure script; '
        f'called {called_script!r}, expected prefix {expected_dir!r}'
    )
    assert 'polyventure' in Path(called_script).name
    assert called_script != 'polyventure', '_open_console must not use bare PATH-resolved polyventure'


# ---------------------------------------------------------------------------
# SB-5: Tray _abort_execution signed mutation headers
# ---------------------------------------------------------------------------


def test_tray_abort_post_requires_session_token() -> None:
    """
    SB-5: _abort_execution must include all five X-PV-Mutation-* signed headers
    and the key_id must incorporate the session token suffix.
    """
    captured_requests: list[Any] = []

    def fake_urlopen(req: Any, timeout: float = 3.0) -> Any:
        captured_requests.append(req)
        cm = MagicMock()
        cm.__enter__ = MagicMock(return_value=MagicMock())
        cm.__exit__ = MagicMock(return_value=False)
        return cm

    signing_key_b64 = base64.b64encode(os.urandom(32)).decode('ascii')
    session_token = 'session-tok-xyz'
    key_id = f'ui-session-{session_token[-6:]}'

    tray = ExecutionTrayIcon(
        host='127.0.0.1',
        port=8765,
        session_token=session_token,
        signing_key_b64=signing_key_b64,
        key_id=key_id,
    )

    with patch('polyventure.tray.urllib_request.urlopen', side_effect=fake_urlopen):
        tray._abort_execution(None, None)

    assert len(captured_requests) == 1
    req = captured_requests[0]
    req_headers = {k.lower(): v for k, v in req.headers.items()}

    assert 'x-pv-mutation-signature' in req_headers, 'Abort POST must include X-PV-Mutation-Signature'
    assert 'x-pv-mutation-key-id' in req_headers, 'Abort POST must include X-PV-Mutation-Key-Id'
    assert 'x-pv-mutation-timestamp' in req_headers, 'Abort POST must include X-PV-Mutation-Timestamp'
    assert 'x-pv-mutation-nonce' in req_headers, 'Abort POST must include X-PV-Mutation-Nonce'
    assert 'x-pv-mutation-body-hash' in req_headers, 'Abort POST must include X-PV-Mutation-Body-Hash'

    key_id_value = req_headers['x-pv-mutation-key-id']
    assert session_token[-6:] in key_id_value, (
        f'key_id must incorporate session token suffix; got {key_id_value!r}'
    )
