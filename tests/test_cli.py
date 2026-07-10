from __future__ import annotations

import importlib.util
import io
import json
import time
from pathlib import Path
from types import SimpleNamespace
from urllib import error as urllib_error
from typing import Any

import pytest

from polyventure import cli


@pytest.fixture(autouse=True)
def _suppress_launch_splash(monkeypatch: Any) -> None:
  # The launch splash is default-on, but CLI tests exercise the console launch
  # path headlessly and must not spawn a tkinter window. Splash default-on
  # behavior is covered in tests/test_popup.py.
  monkeypatch.setenv('POLYVENTURE_POPUP', '0')


def test_helper_ready_budget_is_cold_start_aware() -> None:
  # Phase-1 finding: a 5s helper health budget killed healthy-but-slow helpers on
  # cold launches. The budget must be cold-start-aware and below the host's window.
  assert cli.DETACHED_CONSOLE_HELPER_READY_WAIT_SEC >= 10.0
  assert cli.DETACHED_CONSOLE_HELPER_READY_WAIT_SEC <= cli.DETACHED_CONSOLE_READY_WAIT_SEC


def test_wait_for_console_ready_exits_early_when_process_dead() -> None:
  # A crashed helper (process already exited) must not burn the full budget.
  class _DeadProcess:
    def poll(self) -> int:
      return 1

  started = time.time()
  result = cli._wait_for_console_ready(
    'http://127.0.0.1:0/health',
    timeout_sec=10.0,
    process=_DeadProcess(),  # type: ignore[arg-type]
  )
  elapsed = time.time() - started
  assert result is False
  assert elapsed < 2.0, f'crash-aware wait should return immediately, took {elapsed:.2f}s'


def _load_tool_module(script_name: str) -> Any:
  script_path = Path(__file__).resolve().parents[1] / 'tools' / script_name
  if not script_path.exists():
    import pytest

    pytest.skip(f'developer tool not shipped in this package: {script_name}')
  module_name = f'test_tool_{script_path.stem}_{time.time_ns()}'
  spec = importlib.util.spec_from_file_location(module_name, script_path)
  if spec is None or spec.loader is None:
    raise AssertionError(f'Unable to load tool module: {script_path}')
  module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(module)
  return module


def _call_helper_app(
  app: Any,
  *,
  method: str,
  path: str,
  query: str = '',
) -> tuple[str, dict[str, str], str]:
  status_holder: dict[str, str] = {}
  headers_holder: dict[str, str] = {}

  def _start_response(status: str, headers: list[tuple[str, str]]) -> None:
    status_holder['status'] = status
    headers_holder.update(dict(headers))

  body = b''.join(
    app(
      {
        'REQUEST_METHOD': method,
        'PATH_INFO': path,
        'QUERY_STRING': query,
        'CONTENT_LENGTH': '0',
        'wsgi.input': io.BytesIO(b''),
      },
      _start_response,
    )
  ).decode('utf-8')
  return status_holder['status'], headers_holder, body


def _scan_payload() -> dict[str, Any]:
  return {
    'decision': 'planned',
    'command_family': 'polyventure scan-once',
    'mode': 'ab_guarded',
    'dry_run': True,
    'balance_dollars': '321.00',
    'market_count': 3,
    'candidate_count': 2,
    'candidates': [
      {
        'ticker': 'KALSHI-MARKET-HIGH',
        'edge_net_per_contract': '0.27',
        'target_yes_bid': '0.32',
        'target_no_bid': '0.39',
      },
      {
        'ticker': 'KALSHI-MARKET-LOW',
        'edge_net_per_contract': '0.17',
        'target_yes_bid': '0.40',
        'target_no_bid': '0.41',
      },
    ],
    'account_limits': {
      'usage_tier': 'demo-tier',
      'read': {'refill_rate': 30, 'bucket_capacity': 60},
      'write': {'refill_rate': 10, 'bucket_capacity': 20},
    },
    'settings': {
      'kalshi_env': 'demo',
      'api_key_id_present': True,
      'private_key_file_present': True,
      'legacy_private_key_path_present': False,
      'inline_private_key_present': False,
      'api_base_url': 'https://demo-api.kalshi.co/trade-api/v2',
      'websocket_url': 'wss://demo-api.kalshi.co/trade-api/ws/v2',
      'subaccount': 0,
      'scan_interval_ms': 2000,
      'entry_window_start_sec': 900,
      'entry_window_end_sec': 60,
      'min_edge_dollars': 0.03,
      'fee_reserve_dollars': 0.02,
      'min_profit_dollars': 0.01,
      'max_pair_contracts': 10.0,
      'max_open_pairs': 20,
      'max_unhedged_sec': 5,
      'cancel_on_pause': True,
      'log_level': 'INFO',
      'state_db_path': 'var/kalshi.sqlite3',
    },
    'private_key_path_tail': 'demo_private_key.pem',
  }


def _read_launcher_events(workspace_root) -> list[dict[str, Any]]:
  telemetry_path = cli._launcher_telemetry_path(workspace_root=workspace_root)
  if not telemetry_path.exists():
    return []
  return [json.loads(line) for line in telemetry_path.read_text(encoding='utf-8').splitlines() if line.strip()]


def test_scan_once_rejects_targeted_mode_json(capsys) -> None:
  exit_code = cli.main(['--json', 'scan-once', '--mode', 'a_targeted'])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 2
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'mode_not_enabled_in_first_milestone'


def test_scan_once_rejects_allow_orders(capsys) -> None:
  exit_code = cli.main(['scan-once', '--allow-orders'])
  captured = capsys.readouterr()

  assert exit_code == 2
  assert 'order_capable_mode_not_enabled' in captured.out


def test_scan_once_human_output_shows_ranked_candidates(
  monkeypatch,
  capsys,
) -> None:
  monkeypatch.setattr(cli, 'run_scan_once', lambda **_: _scan_payload())

  exit_code = cli.main(['scan-once', '--env', 'demo'])
  captured = capsys.readouterr()

  assert exit_code == 0
  assert 'Top candidates' in captured.out
  assert captured.out.index('KALSHI-MARKET-HIGH') < captured.out.index('KALSHI-MARKET-LOW')
  assert 'candidate_count:   2' in captured.out


def test_scan_once_json_output_preserves_candidate_order(
  monkeypatch,
  capsys,
) -> None:
  monkeypatch.setattr(cli, 'run_scan_once', lambda **_: _scan_payload())

  exit_code = cli.main(['--json', 'scan-once', '--env', 'demo'])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 0
  assert payload['candidate_count'] == 2
  assert [candidate['ticker'] for candidate in payload['candidates']] == [
    'KALSHI-MARKET-HIGH',
    'KALSHI-MARKET-LOW',
  ]


def test_bootstrap_scan_sandbox_uses_temp_owned_state_db() -> None:
  module = _load_tool_module('run_bootstrap_scan_sandbox.py')
  root = module._create_sandbox_root()
  env_updates = module._build_env_updates(root, str(root / 'demo_private_key.pem'))

  project_var_db = Path(__file__).resolve().parents[1] / 'var' / 'kalshi.sqlite3'

  assert Path(env_updates['KALSHI_STATE_DB_PATH']) == root / 'kalshi.sqlite3'
  assert Path(env_updates['KALSHI_STATE_DB_PATH']) != project_var_db
  assert Path(env_updates['HOME']) == root
  assert Path(env_updates['USERPROFILE']) == root


def test_market_discovery_sandbox_uses_temp_owned_state_db() -> None:
  module = _load_tool_module('run_market_discovery_sandbox.py')
  root = module._create_sandbox_root()
  env_updates = module._build_env_updates(root, str(root / 'demo_private_key.pem'))

  project_var_db = Path(__file__).resolve().parents[1] / 'var' / 'kalshi.sqlite3'

  assert Path(env_updates['KALSHI_STATE_DB_PATH']) == root / 'kalshi.sqlite3'
  assert Path(env_updates['KALSHI_STATE_DB_PATH']) != project_var_db
  assert Path(env_updates['HOME']) == root
  assert Path(env_updates['USERPROFILE']) == root


def test_ws_reconciliation_risk_sandbox_uses_temp_owned_settings(tmp_path: Path) -> None:
  module = _load_tool_module('run_ws_reconciliation_risk_sandbox.py')
  root = tmp_path / 'ws-proof-root'
  root.mkdir()
  settings = module._settings(root)

  assert Path(settings.state_db_path) == root / 'kalshi.sqlite3'
  assert Path(settings.private_key_file) == root / 'demo.pem'


def test_order_sandbox_uses_temp_owned_settings(tmp_path: Path) -> None:
  module = _load_tool_module('run_order_sandbox.py')
  root = tmp_path / 'order-proof-root'
  root.mkdir()
  settings = module._settings(root)

  assert Path(settings.state_db_path) == root / 'kalshi.sqlite3'
  assert Path(settings.private_key_file) == root / 'demo.pem'


def test_batch_command_without_env_fails_closed(monkeypatch, capsys) -> None:
  # Fail closed; no silent default lane. A bare batch command must refuse and
  # must not invoke the service.
  called = {'scan': False}

  def _should_not_run(**_):
    called['scan'] = True
    return _scan_payload()

  monkeypatch.setattr(cli, 'run_scan_once', _should_not_run)

  exit_code = cli.main(['scan-once'])
  captured = capsys.readouterr()

  assert exit_code != 0
  assert called['scan'] is False
  assert 'lane is required' in captured.out.lower()


def test_run_requires_explicit_targeted_confirmation(capsys) -> None:
  exit_code = cli.main(['run', '--mode', 'a_targeted'])
  captured = capsys.readouterr()

  assert exit_code == 2
  assert 'targeted_mode_requires_explicit_confirmation' in captured.out


def test_run_rejects_allow_orders_until_acceptance_gates(capsys) -> None:
  exit_code = cli.main(['run', '--allow-orders'])
  captured = capsys.readouterr()

  assert exit_code == 2
  assert 'order_enabled_runtime_not_available' in captured.out
  assert 'sandbox-enable acceptance gates are satisfied' in captured.out


def test_run_json_output_includes_planned_pair(monkeypatch, capsys) -> None:
  monkeypatch.setattr(
    cli,
    'run_service_once',
    lambda **_: {
      'decision': 'planned',
      'command_family': 'polyventure run',
      'mode': 'ab_guarded',
      'dry_run': True,
      'balance_dollars': '123.45',
      'market_count': 3,
      'candidate_count': 2,
      'planned_pair_count': 1,
      'planned_pairs': [
        {
          'pair_id': 'pair-123',
          'ticker': 'KALSHI-RUNNER',
          'contract_count': '5',
          'yes_price': '0.34',
          'no_price': '0.39',
        }
      ],
      'blocked_reason': None,
      'account_limits': {
        'usage_tier': 'demo-tier',
        'read': {'refill_rate': 30, 'bucket_capacity': 60},
        'write': {'refill_rate': 10, 'bucket_capacity': 20},
      },
      'settings': _scan_payload()['settings'],
      'private_key_path_tail': 'demo_private_key.pem',
      'state_db_path_tail': 'kalshi.sqlite3',
      'next_action': 'Review the planned pair.',
    },
  )

  exit_code = cli.main(['--json', 'run', '--env', 'demo'])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 0
  assert payload['planned_pair_count'] == 1
  assert payload['planned_pairs'][0]['ticker'] == 'KALSHI-RUNNER'


def test_report_human_output_shows_table_counts(monkeypatch, capsys) -> None:
  monkeypatch.setattr(
    cli,
    'report_runtime',
    lambda **_: {
      'decision': 'planned',
      'command_family': 'polyventure report',
      'state_db_path_tail': 'kalshi.sqlite3',
      'table_counts': {'pair_plans': 1, 'orders': 2},
      'pair_state_history': {'pair-123': ['PLANNED']},
      'latest_heartbeat': {
        'component': 'runtime-loop',
        'status': 'cycle-complete',
        'recorded_at_utc': '2026-05-05T05:55:00Z',
      },
      'next_action': 'Use reconcile next.',
    },
  )

  exit_code = cli.main(['report', '--env', 'demo'])
  captured = capsys.readouterr()

  assert exit_code == 0
  assert 'latest_heartbeat' in captured.out
  assert 'pair_plans: 1' in captured.out


def _stub_settings(tmp_path: Path) -> SimpleNamespace:
  key_path = tmp_path / 'demo.pem'
  key_path.write_text('demo-key\n', encoding='utf-8')
  return SimpleNamespace(
    operation_lane='sandbox',
    api_key_id='sandbox-key-001',
    private_key_file=str(key_path),
    state_db_path=str(tmp_path / 'kalshi.sqlite3'),
  )


def test_datapack_export_writes_manifest_restore_policy_and_payloads(monkeypatch, tmp_path: Path, capsys) -> None:
  monkeypatch.setattr(cli, 'load_settings', lambda: _stub_settings(tmp_path))

  output_root = tmp_path / 'datapack-export'
  exit_code = cli.main(['--json', 'datapack', 'export', '--output', str(output_root), '--include-synthetic-refinement'])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)
  manifest = json.loads((output_root / 'manifest.json').read_text(encoding='utf-8'))
  restore_policy = json.loads((output_root / 'restore_policy.json').read_text(encoding='utf-8'))

  assert exit_code == 0
  assert payload['command_family'] == 'polyventure datapack export'
  assert manifest['operation_lane'] == 'sandbox'
  assert manifest['cross_key_import_default'] == 'fail_closed'
  assert 'runtime_state' in payload['included_families']
  assert 'synthetic_refinement_fixtures' in payload['included_families']
  assert restore_policy['default_import_policy']['force_rebind_flag'] == '--force-rebind-api-key-hash'
  assert (output_root / 'payloads' / 'runtime_state.json').exists()


def test_datapack_validate_fails_closed_on_api_key_hash_mismatch(monkeypatch, tmp_path: Path, capsys) -> None:
  export_settings = _stub_settings(tmp_path)
  monkeypatch.setattr(cli, 'load_settings', lambda: export_settings)
  output_root = tmp_path / 'datapack-validate'
  export_exit = cli.main(['datapack', 'export', '--output', str(output_root)])
  assert export_exit == 0
  capsys.readouterr()

  mismatch_settings = SimpleNamespace(
    operation_lane='sandbox',
    api_key_id='different-key-002',
    private_key_file=export_settings.private_key_file,
    state_db_path=export_settings.state_db_path,
  )
  monkeypatch.setattr(cli, 'load_settings', lambda: mismatch_settings)

  exit_code = cli.main(['--json', 'datapack', 'validate', '--input', str(output_root)])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 0
  assert payload['decision'] == 'no-go'
  assert payload['allowed'] is False
  assert 'api_key_hash_mismatch' in payload['reasons']


def test_datapack_validate_fails_closed_on_checksum_tamper(monkeypatch, tmp_path: Path, capsys) -> None:
  monkeypatch.setattr(cli, 'load_settings', lambda: _stub_settings(tmp_path))
  output_root = tmp_path / 'datapack-checksum-tamper'

  export_exit = cli.main(['datapack', 'export', '--output', str(output_root)])
  assert export_exit == 0
  capsys.readouterr()

  runtime_state_path = output_root / 'payloads' / 'runtime_state.json'
  runtime_state = json.loads(runtime_state_path.read_text(encoding='utf-8'))
  runtime_state['tables']['pair_plans']['rows'].append({'pair_id': 'tampered-row'})
  runtime_state_path.write_text(json.dumps(runtime_state, indent=2, default=str) + '\n', encoding='utf-8')

  exit_code = cli.main(['--json', 'datapack', 'validate', '--input', str(output_root)])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 0
  assert payload['decision'] == 'no-go'
  assert 'checksum_mismatch:payloads/runtime_state.json' in payload['issues']


def test_datapack_validate_fails_closed_on_missing_payload(monkeypatch, tmp_path: Path, capsys) -> None:
  monkeypatch.setattr(cli, 'load_settings', lambda: _stub_settings(tmp_path))
  output_root = tmp_path / 'datapack-missing-payload'

  export_exit = cli.main(['datapack', 'export', '--output', str(output_root)])
  assert export_exit == 0
  capsys.readouterr()

  (output_root / 'payloads' / 'candidate_review_history.json').unlink()

  exit_code = cli.main(['--json', 'datapack', 'validate', '--input', str(output_root)])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 0
  assert payload['decision'] == 'no-go'
  assert 'artifact_missing:payloads/candidate_review_history.json' in payload['issues']


def test_datapack_validate_fails_closed_on_restore_policy_incoherence(monkeypatch, tmp_path: Path, capsys) -> None:
  monkeypatch.setattr(cli, 'load_settings', lambda: _stub_settings(tmp_path))
  output_root = tmp_path / 'datapack-policy-mismatch'

  export_exit = cli.main(['datapack', 'export', '--output', str(output_root)])
  assert export_exit == 0
  capsys.readouterr()

  restore_policy_path = output_root / 'restore_policy.json'
  restore_policy = json.loads(restore_policy_path.read_text(encoding='utf-8'))
  restore_policy['family_policies'][0]['restore_mode'] = 'tampered_mode'
  restore_policy_path.write_text(json.dumps(restore_policy, indent=2, default=str) + '\n', encoding='utf-8')

  exit_code = cli.main(['--json', 'datapack', 'validate', '--input', str(output_root)])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 0
  assert payload['decision'] == 'no-go'
  assert 'checksum_mismatch:restore_policy.json' in payload['issues']
  assert 'restore_policy_restore_mode_mismatch:runtime_state' in payload['issues']


def test_datapack_rebind_requires_explicit_force_flag(monkeypatch, tmp_path: Path, capsys) -> None:
  monkeypatch.setattr(cli, 'load_settings', lambda: _stub_settings(tmp_path))
  output_root = tmp_path / 'datapack-rebind-input'
  export_exit = cli.main(['datapack', 'export', '--output', str(output_root)])
  assert export_exit == 0
  capsys.readouterr()

  exit_code = cli.main(['datapack', 'rebind', '--input', str(output_root), '--output', str(tmp_path / 'rebound')])
  captured = capsys.readouterr()

  assert exit_code == 2
  assert 'force_rebind_flag_required' in captured.out


def test_datapack_rebind_writes_cli_only_audit_metadata(monkeypatch, tmp_path: Path, capsys) -> None:
  export_settings = _stub_settings(tmp_path)
  monkeypatch.setattr(cli, 'load_settings', lambda: export_settings)
  input_root = tmp_path / 'datapack-rebind-source'
  export_exit = cli.main(['datapack', 'export', '--output', str(input_root)])
  assert export_exit == 0
  capsys.readouterr()

  rebound_settings = SimpleNamespace(
    operation_lane='sandbox',
    api_key_id='different-key-002',
    private_key_file=export_settings.private_key_file,
    state_db_path=export_settings.state_db_path,
  )
  monkeypatch.setattr(cli, 'load_settings', lambda: rebound_settings)

  output_root = tmp_path / 'datapack-rebind-output'
  exit_code = cli.main([
    '--json',
    'datapack',
    'rebind',
    '--input',
    str(input_root),
    '--output',
    str(output_root),
    '--force-rebind-api-key-hash',
  ])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)
  rebound_manifest = json.loads((output_root / 'manifest.json').read_text(encoding='utf-8'))

  assert exit_code == 0
  assert payload['decision'] == 'planned'
  assert rebound_manifest['restored_under_key_hash'] == payload['api_key_hash']
  assert rebound_manifest['revalidation_required'] is True
  assert rebound_manifest['rebind_audit'][0]['mode'] == 'cli_force_rebind'


def test_datapack_rebind_blocks_tampered_input(monkeypatch, tmp_path: Path, capsys) -> None:
  export_settings = _stub_settings(tmp_path)
  monkeypatch.setattr(cli, 'load_settings', lambda: export_settings)
  input_root = tmp_path / 'datapack-rebind-tampered-source'
  export_exit = cli.main(['datapack', 'export', '--output', str(input_root)])
  assert export_exit == 0
  capsys.readouterr()

  runtime_state_path = input_root / 'payloads' / 'runtime_state.json'
  runtime_state = json.loads(runtime_state_path.read_text(encoding='utf-8'))
  runtime_state['tables']['pair_plans']['rows'].append({'pair_id': 'tampered'})
  runtime_state_path.write_text(json.dumps(runtime_state, indent=2, default=str) + '\n', encoding='utf-8')

  rebound_settings = SimpleNamespace(
    operation_lane='sandbox',
    api_key_id='different-key-002',
    private_key_file=export_settings.private_key_file,
    state_db_path=export_settings.state_db_path,
  )
  monkeypatch.setattr(cli, 'load_settings', lambda: rebound_settings)

  exit_code = cli.main([
    '--json',
    'datapack',
    'rebind',
    '--input',
    str(input_root),
    '--output',
    str(tmp_path / 'rebound-tampered'),
    '--force-rebind-api-key-hash',
  ])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 1
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'datapack_command_failed'
  assert 'failed attestation' in payload['message']


def test_datapack_synthetic_refinement_writes_first_proof_family(monkeypatch, tmp_path: Path, capsys) -> None:
  settings = _stub_settings(tmp_path)
  monkeypatch.setattr(cli, 'load_settings', lambda: settings)

  connection = cli.open_database(Path(settings.state_db_path))
  connection.execute(
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
      json.dumps({'source': 'test_datapack_synthetic_refinement_writes_first_proof_family'}),
      '2026-05-24T00:00:00Z',
    ),
  )
  connection.commit()

  output_root = tmp_path / 'synthetic-datapack'
  exit_code = cli.main(['--json', 'datapack', 'synthetic-refinement', '--output', str(output_root)])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)
  manifest = json.loads((output_root / 'manifest.json').read_text(encoding='utf-8'))
  fixture_payload = json.loads((output_root / 'payloads' / 'synthetic_refinement_fixtures.json').read_text(encoding='utf-8'))
  runtime_payload = json.loads((output_root / 'payloads' / 'runtime_state.json').read_text(encoding='utf-8'))
  analytical_payload = json.loads((output_root / 'payloads' / 'analytical_state.json').read_text(encoding='utf-8'))
  candidate_payload = json.loads((output_root / 'payloads' / 'candidate_review_history.json').read_text(encoding='utf-8'))

  assert exit_code == 0
  assert set(payload['included_families']) == {
    'runtime_state',
    'analytical_state',
    'candidate_review_history',
    'synthetic_refinement_fixtures',
  }
  assert payload['convergence']['convergence_class'] == 'baseline_convergent'
  assert manifest['datapack_type'] == 'synthetic_refinement'
  assert fixture_payload['provenance'] == 'synthetic_refinement'
  assert len(fixture_payload['fixture_scenarios']) == 3
  runtime_inventory = {
    str(item['family_id']): int(item.get('row_count') or 0)
    for item in manifest['inventory']
    if isinstance(item, dict)
  }
  assert runtime_inventory['runtime_state'] > 0
  assert runtime_inventory['analytical_state'] > 0
  assert runtime_inventory['candidate_review_history'] > 0
  runtime_events_rows = runtime_payload['tables']['runtime_events']['rows']
  assert any(str(row.get('event_type') or '') == 'scan_complete' for row in runtime_events_rows)
  scan_complete_rows = [row for row in runtime_events_rows if str(row.get('event_type') or '') == 'scan_complete']
  assert any(
    isinstance(json.loads(str(row.get('detail_json') or '{}')).get('analytical_outputs'), dict)
    for row in scan_complete_rows
  )
  assert len(analytical_payload['tables']['analytical_snapshots']['rows']) > 0
  assert len(candidate_payload['tables']['candidate_review_runs']['rows']) > 0
  assert len(candidate_payload['tables']['candidate_review_candidates']['rows']) > 0
  assert len(candidate_payload['tables']['candidate_saved_sets']['rows']) > 0


def test_datapack_synthetic_refinement_blocks_non_convergent_payload(monkeypatch, tmp_path: Path, capsys) -> None:
  monkeypatch.setattr(cli, 'load_settings', lambda: _stub_settings(tmp_path))

  output_root = tmp_path / 'synthetic-datapack-non-convergent'
  exit_code = cli.main(['--json', 'datapack', 'synthetic-refinement', '--output', str(output_root)])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 0
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'synthetic_datapack_non_convergent_no_go'
  assert payload['convergence']['convergence_class'] in {'proof_only_non_loadable', 'non_convergent_no_go'}
  assert not (output_root / 'manifest.json').exists()


def test_datapack_canonical_add_writes_under_canonical_root_and_ledger(monkeypatch, tmp_path: Path, capsys) -> None:
  monkeypatch.setattr(cli, '_resolve_console_workspace_root', lambda **_: tmp_path)
  monkeypatch.setattr(cli, 'load_settings', lambda: _stub_settings(tmp_path))

  input_root = tmp_path / 'canonical-add-input'
  export_exit = cli.main(['datapack', 'export', '--output', str(input_root)])
  assert export_exit == 0
  capsys.readouterr()

  exit_code = cli.main([
    '--json',
    'datapack',
    'canonical-add',
    '--input',
    str(input_root),
    '--reason',
    'seed canonical inventory',
    '--reference',
    'DS-CLI-CANONICAL-1',
  ])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 0
  assert payload['decision'] == 'go'
  assert payload['reason'] == 'canonical_add_succeeded'
  target_path = Path(payload['target_path'])
  assert target_path.exists()
  assert 'var\\datapack_extracts' in str(target_path)
  ledger_path = Path(payload['ledger_path'])
  assert ledger_path.exists()
  ledger_rows = [json.loads(line) for line in ledger_path.read_text(encoding='utf-8').splitlines() if line.strip()]
  assert any(row.get('action') == 'canonical_add' and row.get('reason') == 'canonical_add_succeeded' for row in ledger_rows)


def test_datapack_canonical_add_fails_closed_on_identity_mismatch(monkeypatch, tmp_path: Path, capsys) -> None:
  export_settings = _stub_settings(tmp_path)
  monkeypatch.setattr(cli, '_resolve_console_workspace_root', lambda **_: tmp_path)
  monkeypatch.setattr(cli, 'load_settings', lambda: export_settings)

  input_root = tmp_path / 'canonical-add-mismatch-input'
  export_exit = cli.main(['datapack', 'export', '--output', str(input_root)])
  assert export_exit == 0
  capsys.readouterr()

  mismatch_settings = SimpleNamespace(
    operation_lane='sandbox',
    api_key_id='different-key-002',
    private_key_file=export_settings.private_key_file,
    state_db_path=export_settings.state_db_path,
  )
  monkeypatch.setattr(cli, 'load_settings', lambda: mismatch_settings)

  exit_code = cli.main([
    '--json',
    'datapack',
    'canonical-add',
    '--input',
    str(input_root),
    '--reason',
    'seed canonical inventory',
    '--reference',
    'DS-CLI-CANONICAL-1',
  ])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 0
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'canonical_add_identity_mismatch'
  assert 'api_key_hash_mismatch' in payload['reasons']


def test_datapack_canonical_remove_archives_and_ledgers(monkeypatch, tmp_path: Path, capsys) -> None:
  monkeypatch.setattr(cli, '_resolve_console_workspace_root', lambda **_: tmp_path)
  monkeypatch.setattr(cli, 'load_settings', lambda: _stub_settings(tmp_path))

  input_root = tmp_path / 'canonical-remove-input'
  export_exit = cli.main(['datapack', 'export', '--output', str(input_root)])
  assert export_exit == 0
  capsys.readouterr()

  add_exit = cli.main([
    '--json',
    'datapack',
    'canonical-add',
    '--input',
    str(input_root),
    '--reason',
    'seed canonical inventory',
    '--reference',
    'DS-CLI-CANONICAL-1',
  ])
  assert add_exit == 0
  add_payload = json.loads(capsys.readouterr().out)
  selector_path = str(add_payload['target_path'])

  remove_exit = cli.main([
    '--json',
    'datapack',
    'canonical-remove',
    '--path',
    selector_path,
    '--reason',
    'retire canonical pack',
    '--reference',
    'DS-CLI-CANONICAL-1-RM1',
  ])
  remove_payload = json.loads(capsys.readouterr().out)

  assert remove_exit == 0
  assert remove_payload['decision'] == 'go'
  assert remove_payload['reason'] == 'canonical_remove_archived'
  assert not Path(remove_payload['removed_from']).exists()
  assert Path(remove_payload['archived_to']).exists()
  ledger_rows = [json.loads(line) for line in Path(remove_payload['ledger_path']).read_text(encoding='utf-8').splitlines() if line.strip()]
  assert any(row.get('action') == 'canonical_remove' and row.get('reason') == 'canonical_remove_archived' for row in ledger_rows)


def test_help_includes_examples(capsys) -> None:
  try:
    cli.main(['--help'])
  except SystemExit as exc:
    assert exc.code == 0

  captured = capsys.readouterr()

  assert 'Examples:' in captured.out
  assert 'polyventure scan-once --dry-run' in captured.out


def test_contract_audit_review_dispatches_to_v1_runner(monkeypatch) -> None:
  captured: dict[str, Any] = {}

  def _fake_runner(argv, *, force_json=False):
    captured['argv'] = list(argv)
    captured['force_json'] = force_json
    return 0

  monkeypatch.setattr(cli, 'run_contract_audit_cli', _fake_runner)

  exit_code = cli.main(['--json', 'contract-audit', 'review', '--format', 'json'])

  assert exit_code == 0
  assert captured['argv'] == ['--json', 'review', '--format', 'json']
  assert captured['force_json'] is False


def test_contract_audit_rejects_non_v1_status_verb() -> None:
  try:
    cli.main(['contract-audit', 'status'])
  except SystemExit as exc:
    assert exc.code == 2
  else:
    raise AssertionError('Expected parser rejection for non-V1 contract-audit verb.')


def test_console_command_starts_operator_shell(monkeypatch) -> None:
  captured: dict[str, object] = {}

  def _fake_console_server(
    *,
    host: str,
    port: int,
    session_token: str | None = None,
    startup_grace_sec: float = 15.0,
    idle_timeout_sec: float = 20.0,
    state_db_path: str | None = None,
    handoff_context: dict[str, Any] | None = None,
    recovery_helper_url: str | None = None,
    recovery_helper_token: str | None = None,
    recovery_helper_expiry_unix: float = 0.0,
  ) -> None:
    captured['host'] = host
    captured['port'] = port
    captured['session_token'] = session_token
    captured['startup_grace_sec'] = startup_grace_sec
    captured['idle_timeout_sec'] = idle_timeout_sec
    captured['state_db_path'] = state_db_path
    captured['handoff_context'] = handoff_context
    captured['recovery_helper_url'] = recovery_helper_url
    captured['recovery_helper_token'] = recovery_helper_token
    captured['recovery_helper_expiry_unix'] = recovery_helper_expiry_unix

  monkeypatch.setattr(cli, 'run_operator_console_server', _fake_console_server)

  exit_code = cli.main(['console', '--foreground', '--host', '127.0.0.1', '--port', '9001'])

  assert exit_code == 0
  state_db_path_captured = captured.pop('state_db_path', None)
  assert isinstance(state_db_path_captured, (str, type(None)))
  assert captured == {
    'host': '127.0.0.1',
    'port': 9001,
    'session_token': None,
    'startup_grace_sec': 15.0,
    'idle_timeout_sec': 20.0,
    'handoff_context': None,
    'recovery_helper_url': None,
    'recovery_helper_token': None,
    'recovery_helper_expiry_unix': 0.0,
  }


def _make_instance_already_running_payload() -> dict[str, Any]:
  return {
    'decision': 'no-go',
    'command_family': 'polyventure console',
    'reason': 'instance_already_running',
    'message': 'A console instance is already running.',
    'in_flight_count': 0,
    'active_pairs': [],
    'reattach_url': 'http://127.0.0.1:8765/',
    'next_action': 'Navigate to http://127.0.0.1:8765/ in your existing browser tab — paste the URL in the address bar; do not open a new window or tab.',
  }


def test_console_instance_already_running_no_browser_opened(monkeypatch, capsys) -> None:
  opened: list[str] = []
  monkeypatch.setattr(cli, 'launch_detached_operator_console', lambda **_: _make_instance_already_running_payload())
  monkeypatch.setattr(cli, '_open_console_browser', lambda url: opened.append(url) or True)

  exit_code = cli.main(['console'])

  assert exit_code == 2
  assert opened == []
  out = capsys.readouterr().out
  assert 'http://127.0.0.1:8765/' in out
  assert 'instance_already_running' in out
  assert 'existing browser tab' in out


def test_console_instance_already_running_emits_no_go(monkeypatch, capsys) -> None:
  monkeypatch.setattr(cli, 'launch_detached_operator_console', lambda **_: _make_instance_already_running_payload())

  exit_code = cli.main(['console'])

  assert exit_code == 2
  out = capsys.readouterr().out
  assert 'instance_already_running' in out
  assert 'existing browser tab' in out


def test_console_browser_session_active_prefers_backend_handoff_attach_confirmation(monkeypatch) -> None:
  class _Response:
    status = 200

    def __enter__(self) -> '_Response':
      return self

    def __exit__(self, exc_type, exc, tb) -> None:
      return None

    def read(self) -> bytes:
      return json.dumps(
        {
          'decision': 'planned',
          'session': {
            'seen': True,
            'closed': False,
            'active': False,
          },
          'handoff': {
            'attach_confirmed': True,
            'usability': {
              'state': 'interactive_shell_ready',
            },
          },
        }
      ).encode('utf-8')

  monkeypatch.setattr(cli.urllib_request, 'urlopen', lambda *_args, **_kwargs: _Response())

  assert cli._console_browser_session_active('127.0.0.1', 8765) is True


def test_console_command_launches_detached_host_by_default(monkeypatch, capsys) -> None:
  captured: dict[str, object] = {}

  def _fake_launch(*, host: str, port: int, open_browser: bool, explicit_port: bool, **_: Any) -> dict[str, object]:
    captured['host'] = host
    captured['port'] = port
    captured['open_browser'] = open_browser
    captured['explicit_port'] = explicit_port
    return {
      'decision': 'planned',
      'command_family': 'polyventure console',
      'launch_mode': 'detached',
      'url': 'http://127.0.0.1:9001',
      'requested_port': 9001,
      'bound_port': 9001,
      'reaped_pid_count': 0,
      'browser_opened': True,
      'next_action': 'Detached host launched.',
    }

  monkeypatch.setattr(cli, 'launch_detached_operator_console', _fake_launch)

  exit_code = cli.main(['console', '--host', '127.0.0.1', '--port', '9001'])
  captured_output = capsys.readouterr()

  assert exit_code == 0
  assert captured == {
    'host': '127.0.0.1',
    'port': 9001,
    'open_browser': True,
    'explicit_port': True,
  }
  assert 'detached' in captured_output.out
  assert 'http://127.0.0.1:9001' in captured_output.out


def test_console_command_treats_equals_style_port_as_explicit(monkeypatch, capsys) -> None:
  captured: dict[str, object] = {}

  def _fake_launch(*, host: str, port: int, open_browser: bool, explicit_port: bool, **_: Any) -> dict[str, object]:
    captured['host'] = host
    captured['port'] = port
    captured['open_browser'] = open_browser
    captured['explicit_port'] = explicit_port
    return {
      'decision': 'planned',
      'command_family': 'polyventure console',
      'launch_mode': 'detached',
      'url': 'http://127.0.0.1:9001',
      'requested_port': 9001,
      'bound_port': 9001,
      'reaped_pid_count': 0,
      'browser_opened': False,
      'next_action': 'Detached host launched.',
    }

  monkeypatch.setattr(cli, 'launch_detached_operator_console', _fake_launch)

  exit_code = cli.main(['console', '--host', '127.0.0.1', '--port=9001', '--no-open'])
  captured_output = capsys.readouterr()

  assert exit_code == 0
  assert captured['explicit_port'] is True
  assert captured['port'] == 9001
  assert '--port=9001' not in captured_output.out


def test_open_console_browser_prefers_startfile_on_windows(monkeypatch) -> None:
  captured: dict[str, Any] = {}

  monkeypatch.setattr(cli.os, 'name', 'nt')
  monkeypatch.setattr(cli.os, 'startfile', lambda url: captured.setdefault('url', url), raising=False)
  monkeypatch.setattr(cli.webbrowser, 'open', lambda *_args, **_kwargs: captured.setdefault('webbrowser_called', True))

  assert cli._open_console_browser('http://127.0.0.1:8765/') is True
  assert captured == {
    'url': 'http://127.0.0.1:8765/',
  }


def test_open_console_browser_falls_back_to_webbrowser_when_startfile_fails(monkeypatch) -> None:
  captured: dict[str, Any] = {}

  def _failing_startfile(_url: str) -> None:
    raise OSError('startfile failed')

  monkeypatch.setattr(cli.os, 'name', 'nt')
  monkeypatch.setattr(cli.os, 'startfile', _failing_startfile, raising=False)
  monkeypatch.setattr(
    cli.webbrowser,
    'open',
    lambda url, new=0: captured.setdefault('call', {'url': url, 'new': new}) or True,
  )

  assert cli._open_console_browser('http://127.0.0.1:8765/') is True
  assert captured == {
    'call': {
      'url': 'http://127.0.0.1:8765/',
      'new': 2,
    }
  }


def test_acquire_console_launch_lock_reclaims_stale_entry(monkeypatch, tmp_path) -> None:
  workspace_root = tmp_path / 'polyventure'
  workspace_root.mkdir()
  monkeypatch.setattr(cli.tempfile, 'gettempdir', lambda: str(tmp_path))
  lock_path = cli._console_launch_lock_path(workspace_root=workspace_root)
  lock_path.write_text(
    json.dumps(
      {
        'pid': 999999,
        'workspace_root': str(workspace_root),
        'acquired_at_unix': time.time() - 10.0,
      }
    ),
    encoding='utf-8',
  )

  acquired = cli._acquire_console_launch_lock(workspace_root=workspace_root)
  payload = json.loads(acquired.read_text(encoding='utf-8'))

  assert acquired == lock_path
  assert int(payload['pid']) == cli.os.getpid()
  cli._release_console_launch_lock(acquired)
  assert not lock_path.exists()


def test_detached_python_executable_prefers_workspace_venv_core_on_windows(monkeypatch, tmp_path) -> None:
  workspace_root = tmp_path / 'UNC' / 'polyventure'
  workspace_root.mkdir(parents=True)
  governed_python = tmp_path / 'UNC' / '.venv-core' / 'Scripts' / 'python.exe'
  governed_pythonw = tmp_path / 'UNC' / '.venv-core' / 'Scripts' / 'pythonw.exe'
  governed_python.parent.mkdir(parents=True)
  governed_python.write_text('', encoding='utf-8')
  governed_pythonw.write_text('', encoding='utf-8')

  global_python = tmp_path / 'Python314' / 'python.exe'
  global_pythonw = tmp_path / 'Python314' / 'pythonw.exe'
  global_python.parent.mkdir(parents=True)
  global_python.write_text('', encoding='utf-8')
  global_pythonw.write_text('', encoding='utf-8')

  monkeypatch.setattr(cli.os, 'name', 'nt')
  monkeypatch.setattr(cli.sys, 'executable', str(global_python))

  assert cli._detached_python_executable(workspace_root=workspace_root) == str(governed_pythonw)


def test_detached_python_executable_falls_back_to_sys_executable_sibling_when_workspace_venv_missing(monkeypatch, tmp_path) -> None:
  workspace_root = tmp_path / 'polyventure'
  workspace_root.mkdir()
  fake_python = tmp_path / 'python.exe'
  fake_pythonw = tmp_path / 'pythonw.exe'
  fake_python.write_text('', encoding='utf-8')
  fake_pythonw.write_text('', encoding='utf-8')

  monkeypatch.setattr(cli.os, 'name', 'nt')
  monkeypatch.setattr(cli.sys, 'executable', str(fake_python))

  assert cli._detached_python_executable(workspace_root=workspace_root) == str(fake_pythonw)


def test_detached_console_env_prefers_workspace_dotenv_runtime_authority(monkeypatch, tmp_path) -> None:
  (tmp_path / '.env').write_text(
    '\n'.join([
      'KALSHI_WEBSOCKET_URL=wss://demo-api.kalshi.co/trade-api/ws/v2',
      'KALSHI_STATE_DB_PATH=var/kalshi.sqlite3',
    ]),
    encoding='utf-8',
  )
  monkeypatch.setenv('KALSHI_WEBSOCKET_URL', 'wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2')
  monkeypatch.setenv('KALSHI_SANDBOX_WEBSOCKET_URL', 'wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2')
  monkeypatch.setenv('KALSHI_LIVE_WEBSOCKET_URL', 'wss://external-api-ws.kalshi.com/trade-api/ws/v2')
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(tmp_path / 'var' / 'kalshi_pairs.sqlite3'))

  env = cli._detached_console_env(workspace_root=tmp_path)

  assert env['KALSHI_FORCE_LOCAL_DOTENV'] == 'true'
  assert env['KALSHI_WEBSOCKET_URL'] == 'wss://demo-api.kalshi.co/trade-api/ws/v2'
  assert 'KALSHI_SANDBOX_WEBSOCKET_URL' not in env
  assert 'KALSHI_LIVE_WEBSOCKET_URL' not in env
  assert env['KALSHI_STATE_DB_PATH'] == 'var/kalshi.sqlite3'


def test_launch_detached_operator_console_prefers_workspace_source_tree(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  (tmp_path / 'src' / 'polyventure' / '__main__.py').write_text('from polyventure.cli import main\nmain()\n', encoding='utf-8')
  governed_pythonw = tmp_path.parent / '.venv-core' / 'Scripts' / 'pythonw.exe'
  governed_pythonw.parent.mkdir(parents=True)
  governed_pythonw.write_text('', encoding='utf-8')
  captured: dict[str, object] = {}

  class _FakeProcess:
    pid = 4242

  def _fake_popen(command, **kwargs):
    captured['command'] = command
    captured['kwargs'] = kwargs
    return _FakeProcess()

  def _fake_wait_for_console_ready(url, **kwargs):
    captured.setdefault('ready_probe_calls', []).append((url, kwargs))
    return True

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli.sys, 'executable', str(tmp_path / 'global-python' / 'python.exe'))
  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(tmp_path / 'var' / 'kalshi_pairs.sqlite3'))
  monkeypatch.setenv('KALSHI_WEBSOCKET_URL', 'wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2')
  monkeypatch.setenv('KALSHI_SANDBOX_WEBSOCKET_URL', 'wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2')
  monkeypatch.setenv('KALSHI_LIVE_WEBSOCKET_URL', 'wss://external-api-ws.kalshi.com/trade-api/ws/v2')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_listener_pid_for_port', lambda *_args, **_kwargs: 9898)
  monkeypatch.setattr(cli, '_record_console_host', lambda **kwargs: captured.setdefault('recorded', kwargs))
  monkeypatch.setattr(cli, '_wait_for_console_ready', _fake_wait_for_console_ready)
  monkeypatch.setattr(
    cli,
    '_launch_console_recovery_helper',
    lambda **_: {
      'url': 'http://127.0.0.1:8766',
      'token': 'helper-token',
      'expires_at_unix': 123.0,
    },
  )
  monkeypatch.setattr(cli.webbrowser, 'open', lambda *_: False)
  monkeypatch.setattr(cli.subprocess, 'Popen', _fake_popen)

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=False)

  assert payload['bound_port'] == 8765
  assert payload['decision'] == 'manual_attach_required'
  assert payload['url'] == 'http://127.0.0.1:8765/'
  assert payload['attach_window_sec'] == cli.DETACHED_CONSOLE_STARTUP_GRACE_SEC
  assert 'within 45 seconds' in payload['next_action']
  assert captured['kwargs']['cwd'] == str(tmp_path)
  assert captured['kwargs']['env']['KALSHI_FORCE_LOCAL_DOTENV'] == 'true'
  assert captured['command'][0] == str(governed_pythonw)
  assert 'KALSHI_STATE_DB_PATH' not in captured['kwargs']['env']
  assert 'KALSHI_WEBSOCKET_URL' not in captured['kwargs']['env']
  assert 'KALSHI_SANDBOX_WEBSOCKET_URL' not in captured['kwargs']['env']
  assert 'KALSHI_LIVE_WEBSOCKET_URL' not in captured['kwargs']['env']
  assert str(tmp_path / 'src') in captured['kwargs']['env']['PYTHONPATH']
  assert captured['command'][1:3] == ['-m', 'polyventure.cli']
  assert '--startup-grace-sec' in captured['command']
  assert str(cli.DETACHED_CONSOLE_STARTUP_GRACE_SEC) in captured['command']
  assert '--idle-timeout-sec' in captured['command']
  assert str(cli.DETACHED_CONSOLE_IDLE_TIMEOUT_SEC) in captured['command']
  assert '--recovery-helper-url' in captured['command']
  assert 'http://127.0.0.1:8766' in captured['command']
  assert '--recovery-helper-token' in captured['command']
  assert 'helper-token' in captured['command']
  assert '--recovery-helper-expiry-unix' in captured['command']
  assert '123.0' in captured['command']
  assert captured['recorded']['pid'] == 9898
  assert captured['recorded']['port'] == 8765
  ready_probe_urls = [url for url, _kwargs in captured['ready_probe_calls']]
  assert any(url.startswith('http://127.0.0.1:8765/?session=') and '&launch=' in url and '&probe=1' in url for url in ready_probe_urls)
  assert any(url.startswith('http://127.0.0.1:8765/api/session-status?session=') and '&launch=' in url for url in ready_probe_urls)
  events = _read_launcher_events(tmp_path)
  assert [event['event'] for event in events] == [
    'discover_started',
    'discover_completed',
    'port_selected',
    'helper_launch_started',
    'helper_ready',
    'host_spawn_started',
    'ready_pass',
    'registry_commit',
    'launch_succeeded',
  ]
  assert events[-1]['state'] == 'COMMIT_REGISTRY'
  assert events[-1]['decision'] == 'manual_attach_required'


def test_launch_detached_operator_console_finds_nested_polyventure_project(monkeypatch, tmp_path) -> None:
  workspace_root = tmp_path / 'UNC'
  project_root = workspace_root / 'polyventure'
  (project_root / 'src' / 'polyventure').mkdir(parents=True)
  (project_root / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  (project_root / 'src' / 'polyventure' / '__main__.py').write_text('from polyventure.cli import main\nmain()\n', encoding='utf-8')
  captured: dict[str, object] = {}

  class _FakeProcess:
    pid = 5252

  def _fake_popen(command, **kwargs):
    captured['command'] = command
    captured['kwargs'] = kwargs
    return _FakeProcess()

  def _fake_wait_for_console_ready(url, **kwargs):
    captured.setdefault('ready_probe_calls', []).append((url, kwargs))
    return True

  def _fake_open_browser(url: str) -> bool:
    captured['browser_url'] = url
    return True

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: workspace_root))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port', lambda *_args, **_kwargs: None)
  monkeypatch.setattr(cli, '_wait_for_console_ready', _fake_wait_for_console_ready)
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(
    cli,
    '_launch_console_recovery_helper',
    lambda **_: {
      'url': 'http://127.0.0.1:8766',
      'token': 'helper-token',
      'expires_at_unix': 123.0,
    },
  )
  monkeypatch.setattr(cli, '_open_console_browser', _fake_open_browser)
  monkeypatch.setattr(cli.subprocess, 'Popen', _fake_popen)

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert payload['bound_port'] == 8765
  assert payload['decision'] == 'launched_fresh_host'
  assert payload['url'] == 'http://127.0.0.1:8765/'
  assert captured['kwargs']['cwd'] == str(project_root)
  assert str(project_root / 'src') in captured['kwargs']['env']['PYTHONPATH']
  assert captured['command'][1:3] == ['-m', 'polyventure.cli']
  assert '--recovery-helper-url' in captured['command']
  assert 'http://127.0.0.1:8766' in captured['command']
  assert captured['browser_url'].startswith('http://127.0.0.1:8765/?session=')
  assert '&launch=' in captured['browser_url']
  assert '&probe=1' not in captured['browser_url']
  ready_probe_urls = [url for url, _kwargs in captured['ready_probe_calls']]
  assert any(url.startswith('http://127.0.0.1:8765/?session=') and '&launch=' in url and '&probe=1' in url for url in ready_probe_urls)
  assert any(url.startswith('http://127.0.0.1:8765/api/session-status?session=') and '&launch=' in url for url in ready_probe_urls)
  events = _read_launcher_events(project_root)
  assert [event['event'] for event in events] == [
    'discover_started',
    'discover_completed',
    'port_selected',
    'helper_launch_started',
    'helper_ready',
    'host_spawn_started',
    'ready_pass',
    'registry_commit',
    'browser_open_attempted',
    'browser_open_result',
    'attach_confirmed',
    'launch_succeeded',
  ]
  assert events[6]['reason_code'] == 'ready_probe_plus_session_status'
  assert events[6]['notes'] == 'ready_probe_ok=true;session_status_ok=true'
  assert events[-1]['session_attach_confirmed'] is True
  assert events[-1]['decision'] == 'launched_fresh_host'


def test_launch_detached_operator_console_replacement_skips_browser_open(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {'open_calls': 0}

  class _FakeProcess:
    pid = 5253

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_port_serves_polyventure_console', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [1111])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5253)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(
    cli,
    '_launch_console_recovery_helper',
    lambda **_: {
      'url': 'http://127.0.0.1:8766',
      'host': '127.0.0.1',
      'port': 8766,
      'pid': 6262,
      'token': 'helper-token',
      'expires_at_unix': 123.0,
    },
  )
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: captured.__setitem__('open_calls', int(captured['open_calls']) + 1) or True)

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert payload['decision'] == 'replaced_existing_host'
  assert payload['reaped_pid_count'] == 1
  assert payload['browser_opened'] is False
  assert captured['open_calls'] == 0
  events = _read_launcher_events(tmp_path)
  assert 'self_heal_applied' in [event['event'] for event in events]
  assert events[-1]['event'] == 'launch_succeeded'
  assert events[-1]['decision'] == 'replaced_existing_host'


def test_launch_detached_operator_console_reuses_healthy_active_host_without_relaunch(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {'open_calls': 0, 'recorded': []}

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_console_code_signature', lambda **_: 'sig-current')
  monkeypatch.setattr(
    cli,
    '_collect_console_reuse_health_basis',
    lambda **_: {
      'registry_entry_present': True,
      'registry_pid': 5253,
      'registry_pid_alive': True,
      'listener_pid': 5253,
      'listener_pid_matches_registry_pid': True,
      'root_probe_ok': True,
      'session_status_ok': True,
      'code_signature_match': True,
      'registry_fresh': True,
      'reusable': True,
      'prune_registry_pid': None,
      'prune_reason_code': None,
    },
  )
  monkeypatch.setattr(cli, '_probe_execution_status', lambda *_a, **_k: {'in_flight_count': 0, 'active_pairs': [], 'drain_active': False})
  monkeypatch.setattr(cli, '_record_console_host', lambda **kwargs: captured['recorded'].append(kwargs))
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: (_ for _ in ()).throw(AssertionError('tracked hosts should not be reaped')))
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: (_ for _ in ()).throw(AssertionError('preferred host should not be reaped')))
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError('detached relaunch should not execute')))
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: captured.__setitem__('open_calls', int(captured['open_calls']) + 1) or True)

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'instance_already_running'
  assert payload['reattach_url'] == 'http://127.0.0.1:8765/'
  assert captured['open_calls'] == 0, 'browser must not be opened when instance is already running'


def test_launch_detached_operator_console_prunes_mismatched_registry_pid_before_relaunch(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {'pruned_pids': [], 'open_calls': 0}

  class _FakeProcess:
    pid = 6266

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_console_code_signature', lambda **_: 'sig-current')
  monkeypatch.setattr(
    cli,
    '_collect_console_reuse_health_basis',
    lambda **_: {
      'registry_entry_present': True,
      'registry_pid': 5151,
      'registry_pid_alive': True,
      'listener_pid': 6266,
      'listener_pid_matches_registry_pid': False,
      'root_probe_ok': True,
      'session_status_ok': True,
      'code_signature_match': True,
      'registry_fresh': True,
      'reusable': False,
      'prune_registry_pid': 5151,
      'prune_reason_code': 'stale_registry_listener_pid_mismatch',
    },
  )
  monkeypatch.setattr(cli, '_remove_console_registry_entries', lambda *, pids: captured['pruned_pids'].append(set(pids)))
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8766)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 6266)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: captured.__setitem__('open_calls', int(captured['open_calls']) + 1) or True)

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert payload['decision'] == 'replaced_existing_host'
  assert payload['bound_port'] == 8766
  assert captured['pruned_pids'] == [{5151}]
  events = _read_launcher_events(tmp_path)
  assert [event['event'] for event in events[:5]] == [
    'discover_started',
    'self_heal_applied',
    'stale_registry_pruned',
    'discover_completed',
    'port_selected',
  ]
  assert events[1]['reason_code'] == 'stale_registry_listener_pid_mismatch'
  assert events[2]['reason_code'] == 'stale_registry_listener_pid_mismatch'
  assert events[3]['reason_code'] == 'stale_registry_listener_pid_mismatch'


def test_launch_detached_operator_console_succeeds_after_bounded_same_host_attach_retry(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {'open_calls': 0}

  class _FakeProcess:
    pid = 5258

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5258)
  monkeypatch.setattr(cli, '_process_is_alive', lambda pid: pid == 5258)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  attach_checks = iter([False, True])
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: next(attach_checks))
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: captured.__setitem__('open_calls', int(captured['open_calls']) + 1) or True)

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert payload['decision'] == 'launched_fresh_host'
  assert payload['browser_opened'] is True
  assert captured['open_calls'] == 2
  events = _read_launcher_events(tmp_path)
  assert [event['event'] for event in events] == [
    'discover_started',
    'discover_completed',
    'port_selected',
    'helper_launch_started',
    'helper_ready',
    'host_spawn_started',
    'ready_pass',
    'registry_commit',
    'browser_open_attempted',
    'browser_open_result',
    'attach_timeout',
    'attach_retry_started',
    'attach_retry_result',
    'attach_confirmed',
    'launch_succeeded',
  ]
  assert [event['event'] for event in events if event['event'] == 'launch_succeeded'] == ['launch_succeeded']
  assert events[6]['reason_code'] == 'ready_probe_plus_session_status'
  assert events[10]['notes'] == 'ready_probe_ok=true;session_status_ok=true;host_pid_alive=true'
  assert events[-3]['reason_code'] == 'attach_retry_confirmed'
  assert events[-4]['notes'] == 'ready_probe_ok=true;session_status_ok=true'
  assert events[-1]['decision'] == 'launched_fresh_host'


def test_launch_detached_operator_console_emits_stale_lock_self_heal(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli.tempfile, 'gettempdir', lambda: str(tmp_path))
  lock_path = cli._console_launch_lock_path(workspace_root=tmp_path)
  lock_path.write_text(
    json.dumps(
      {
        'pid': 999999,
        'workspace_root': str(tmp_path),
        'acquired_at_unix': time.time() - 10.0,
      }
    ),
    encoding='utf-8',
  )

  class _FakeProcess:
    pid = 6264

  monkeypatch.setattr(cli, '_process_is_alive', lambda _pid: False)
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 6264)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=False)

  assert payload['decision'] == 'manual_attach_required'
  events = _read_launcher_events(tmp_path)
  assert [event['event'] for event in events[:6]] == [
    'discover_started',
    'discover_completed',
    'self_heal_applied',
    'stale_lock_reclaimed',
    'port_selected',
    'helper_launch_started',
  ]
  assert events[2]['reason_code'] == 'stale_lock_dead_owner'
  assert events[3]['reason_code'] == 'stale_lock_dead_owner'
  assert events[-1]['event'] == 'launch_succeeded'
  assert events[-1]['decision'] == 'manual_attach_required'


def test_launch_detached_operator_console_replaces_healthy_host_when_code_signature_changed(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {'open_calls': 0}

  class _FakeProcess:
    pid = 5256

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_port_serves_polyventure_console', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_console_code_signature', lambda **_: 'sig-current')
  monkeypatch.setattr(cli, '_active_console_host_signature', lambda **_: 'sig-old')
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [1111])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5256)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  attach_checks = iter([False, True])
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: next(attach_checks))
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: captured.__setitem__('open_calls', int(captured['open_calls']) + 1) or True)

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert payload['reaped_pid_count'] == 1
  assert payload['browser_opened'] is True
  assert captured['open_calls'] == 1


def test_launch_detached_operator_console_replacement_opens_browser_when_reattach_fails(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {'open_calls': 0}

  class _FakeProcess:
    pid = 5255

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_port_serves_polyventure_console', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [1111])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5255)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  attach_checks = iter([False, True])
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: next(attach_checks))
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: captured.__setitem__('open_calls', int(captured['open_calls']) + 1) or True)

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert payload['reaped_pid_count'] == 1
  assert payload['browser_opened'] is True
  assert captured['open_calls'] == 1


def test_launch_detached_operator_console_opens_browser_when_reaping_stale_host_without_active_session(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {'open_calls': 0}

  class _FakeProcess:
    pid = 5254

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [1111])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5254)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: captured.__setitem__('open_calls', int(captured['open_calls']) + 1) or True)

  payload = cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert payload['reaped_pid_count'] == 1
  assert payload['browser_opened'] is True
  assert captured['open_calls'] == 1


def test_launch_detached_operator_console_blocks_when_fresh_browser_hydration_missing(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {}

  class _FakeProcess:
    pid = 5257

  health_basis_calls = iter(
    [
      {'ready_probe_ok': True, 'session_status_ok': True},
      {'ready_probe_ok': False, 'session_status_ok': False},
      {'ready_probe_ok': False, 'session_status_ok': False},
    ]
  )

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5257)
  monkeypatch.setattr(cli, '_process_is_alive', lambda pid: pid == 5257)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_collect_console_health_basis', lambda **_kwargs: next(health_basis_calls))
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli, '_cleanup_failed_detached_console_launch', lambda **kwargs: captured.setdefault('cleanup', kwargs))
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: True)

  with pytest.raises(cli.ConsoleLaunchBlockedError, match='bootstrap hydration'):
    cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert 'cleanup' in captured
  events = _read_launcher_events(tmp_path)
  assert [event['event'] for event in events] == [
    'discover_started',
    'discover_completed',
    'port_selected',
    'helper_launch_started',
    'helper_ready',
    'host_spawn_started',
    'ready_pass',
    'registry_commit',
    'browser_open_attempted',
    'browser_open_result',
    'attach_timeout',
    'attach_retry_result',
    'attach_unconfirmed_launch_blocked',
    'cleanup_started',
    'cleanup_complete',
    'launch_blocked',
  ]
  assert events[11]['reason_code'] == 'attach_retry_skipped_precondition'
  assert events[10]['notes'] == 'ready_probe_ok=true;session_status_ok=true;host_pid_alive=true'
  assert events[11]['notes'] == 'host_not_ready_for_retry;ready_probe_ok=false;session_status_ok=false;host_pid_alive=true'
  assert events[12]['reason_code'] == 'attach_hydration_timeout'
  assert events[12]['notes'] == 'ready_probe_ok=false;session_status_ok=false'
  assert events[-1]['decision'] == 'launch_blocked'
  assert events[-1]['session_attach_confirmed'] is False


def test_launch_detached_operator_console_blocks_even_when_host_remains_healthy_without_hydration(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')

  class _FakeProcess:
    pid = 5261

  health_basis_calls = iter(
    [
      {'ready_probe_ok': True, 'session_status_ok': True},
      {'ready_probe_ok': False, 'session_status_ok': False},
      {'ready_probe_ok': True, 'session_status_ok': True},
    ]
  )

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5261)
  monkeypatch.setattr(cli, '_process_is_alive', lambda pid: pid == 5261)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_collect_console_health_basis', lambda **_kwargs: next(health_basis_calls))
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  captured: dict[str, Any] = {}
  monkeypatch.setattr(cli, '_cleanup_failed_detached_console_launch', lambda **kwargs: captured.setdefault('cleanup', kwargs))
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: True)

  with pytest.raises(cli.ConsoleLaunchBlockedError, match='bootstrap hydration'):
    cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert 'cleanup' in captured
  events = _read_launcher_events(tmp_path)
  assert [event['event'] for event in events] == [
    'discover_started',
    'discover_completed',
    'port_selected',
    'helper_launch_started',
    'helper_ready',
    'host_spawn_started',
    'ready_pass',
    'registry_commit',
    'browser_open_attempted',
    'browser_open_result',
    'attach_timeout',
    'attach_retry_result',
    'attach_unconfirmed_launch_blocked',
    'cleanup_started',
    'cleanup_complete',
    'launch_blocked',
  ]
  assert events[10]['notes'] == 'ready_probe_ok=true;session_status_ok=true;host_pid_alive=true'
  assert events[11]['notes'] == 'host_not_ready_for_retry;ready_probe_ok=false;session_status_ok=false;host_pid_alive=true'
  assert events[12]['reason_code'] == 'attach_hydration_timeout'
  assert events[12]['notes'] == 'ready_probe_ok=true;session_status_ok=true'
  assert events[-1]['decision'] == 'launch_blocked'
  assert events[-1]['session_attach_confirmed'] is False


def test_launch_detached_operator_console_skips_retry_when_precondition_fails(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {}

  class _FakeProcess:
    pid = 5259

  health_basis_calls = iter(
    [
      {'ready_probe_ok': True, 'session_status_ok': True},
      {'ready_probe_ok': False, 'session_status_ok': False},
      {'ready_probe_ok': False, 'session_status_ok': False},
    ]
  )

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5259)
  monkeypatch.setattr(cli, '_process_is_alive', lambda pid: False)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_collect_console_health_basis', lambda **_kwargs: next(health_basis_calls))
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli, '_cleanup_failed_detached_console_launch', lambda **kwargs: captured.setdefault('cleanup', kwargs))
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: True)

  with pytest.raises(cli.ConsoleLaunchBlockedError, match='bootstrap hydration'):
    cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  events = _read_launcher_events(tmp_path)
  assert 'attach_retry_started' not in [event['event'] for event in events]
  assert events[10]['event'] == 'attach_timeout'
  assert events[10]['notes'] == 'ready_probe_ok=true;session_status_ok=true;host_pid_alive=false'
  assert events[11]['event'] == 'attach_retry_result'
  assert events[11]['reason_code'] == 'attach_retry_skipped_precondition'
  assert events[11]['notes'] == 'host_not_ready_for_retry;ready_probe_ok=false;session_status_ok=false;host_pid_alive=false'
  assert events[12]['event'] == 'attach_unconfirmed_launch_blocked'
  assert events[-1]['event'] == 'launch_blocked'
  assert events[-1]['decision'] == 'launch_blocked'
  assert 'cleanup' in captured


def test_attach_timeout_event_notes_include_host_pid_alive_status(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')

  class _FakeProcess:
    pid = 5359

  health_basis_calls = iter(
    [
      {'ready_probe_ok': True, 'session_status_ok': True},
      {'ready_probe_ok': False, 'session_status_ok': False},
      {'ready_probe_ok': False, 'session_status_ok': False},
    ]
  )

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5359)
  monkeypatch.setattr(cli, '_process_is_alive', lambda pid: pid == 5359)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_collect_console_health_basis', lambda **_kwargs: next(health_basis_calls))
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli, '_cleanup_failed_detached_console_launch', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: True)

  with pytest.raises(cli.ConsoleLaunchBlockedError, match='bootstrap hydration'):
    cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  events = _read_launcher_events(tmp_path)
  assert events[10]['event'] == 'attach_timeout'
  assert events[10]['notes'].endswith('host_pid_alive=true')


def test_attach_retry_skipped_notes_include_host_pid_alive_status(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')

  class _FakeProcess:
    pid = 5360

  health_basis_calls = iter(
    [
      {'ready_probe_ok': True, 'session_status_ok': True},
      {'ready_probe_ok': False, 'session_status_ok': False},
      {'ready_probe_ok': False, 'session_status_ok': False},
    ]
  )

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5360)
  monkeypatch.setattr(cli, '_process_is_alive', lambda pid: False)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_collect_console_health_basis', lambda **_kwargs: next(health_basis_calls))
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli, '_cleanup_failed_detached_console_launch', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: True)

  with pytest.raises(cli.ConsoleLaunchBlockedError, match='bootstrap hydration'):
    cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  events = _read_launcher_events(tmp_path)
  assert events[11]['event'] == 'attach_retry_result'
  assert 'host_pid_alive=false' in str(events[11]['notes'])


def test_host_pid_alive_note_value_returns_unknown_when_pid_missing() -> None:
  assert cli._host_pid_alive_note_value(None) == 'unknown'


def test_attach_unconfirmed_host_retained_event_is_not_emittable() -> None:
  cli_source = Path(cli.__file__).read_text(encoding='utf-8')

  assert 'attach_unconfirmed_host_retained' not in cli_source


def test_render_console_launch_human_surfaces_manual_attach_deadline(capsys) -> None:
  cli._render_console_launch_human(
    {
      'decision': 'manual_attach_required',
      'launch_mode': 'detached',
      'url': 'http://127.0.0.1:8765/',
      'requested_port': 8765,
      'bound_port': 8765,
      'reaped_pid_count': 0,
      'browser_opened': False,
      'attach_window_sec': cli.DETACHED_CONSOLE_STARTUP_GRACE_SEC,
      'next_action': 'Open the local shell URL manually within 45 seconds or the detached host will self-close before the first browser session attaches.',
    }
  )

  captured = capsys.readouterr()

  assert 'manual_attach_required' in captured.out
  assert 'within 45 seconds' in captured.out


def test_launch_detached_operator_console_retry_does_not_respawn_host(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  captured: dict[str, Any] = {'popen_calls': 0}

  class _FakeProcess:
    pid = 5260

  health_basis_calls = iter(
    [
      {'ready_probe_ok': True, 'session_status_ok': True},
      {'ready_probe_ok': False, 'session_status_ok': False},
      {'ready_probe_ok': False, 'session_status_ok': False},
    ]
  )

  def _fake_popen(*_args, **_kwargs):
    captured['popen_calls'] = int(captured['popen_calls']) + 1
    return _FakeProcess()

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: [])
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 5260)
  monkeypatch.setattr(cli, '_process_is_alive', lambda pid: pid == 5260)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_collect_console_health_basis', lambda **_kwargs: next(health_basis_calls))
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli, '_cleanup_failed_detached_console_launch', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', _fake_popen)
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: True)

  with pytest.raises(cli.ConsoleLaunchBlockedError, match='bootstrap hydration'):
    cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  assert captured['popen_calls'] == 1


def test_launch_detached_operator_console_rejects_overlapping_launch(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')
  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli.tempfile, 'gettempdir', lambda: str(tmp_path))
  lock_path = cli._console_launch_lock_path(workspace_root=tmp_path)
  lock_path.write_text(
    json.dumps(
      {
        'pid': cli.os.getpid(),
        'workspace_root': str(tmp_path),
        'acquired_at_unix': time.time(),
      }
    ),
    encoding='utf-8',
  )

  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, '_process_is_alive', lambda _pid: True)
  monkeypatch.setattr(cli, '_reap_tracked_console_hosts', lambda **_: (_ for _ in ()).throw(AssertionError('overlap should fail before reaping')))
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: (_ for _ in ()).throw(AssertionError('overlap should fail before preferred-port cleanup')))

  try:
    with pytest.raises(RuntimeError, match='already in progress'):
      cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)
  finally:
    cli._release_console_launch_lock(lock_path)

  events = _read_launcher_events(tmp_path)
  assert [event['event'] for event in events] == [
    'discover_started',
    'discover_completed',
    'launch_blocked',
  ]
  assert events[-1]['decision'] == 'launch_blocked'
  assert events[-1]['state'] == 'FAIL_CLOSED'


def test_console_command_json_reports_launch_blocked(monkeypatch, capsys) -> None:
  monkeypatch.setattr(
    cli,
    'launch_detached_operator_console',
    lambda **_: (_ for _ in ()).throw(RuntimeError('already in progress')),
  )

  exit_code = cli.main(['--json', 'console'])
  captured = capsys.readouterr()
  payload = json.loads(captured.out)

  assert exit_code == 1
  assert payload['decision'] == 'launch_blocked'
  assert payload['reason'] == 'console_failed'


def test_console_recovery_helper_requests_detached_relaunch(monkeypatch) -> None:
  captured: dict[str, Any] = {}
  app = cli._create_console_recovery_helper_app(
    helper_token='helper-123',
    helper_expires_at_unix=time.time() + 60.0,
    target_host='127.0.0.1',
    target_port=8765,
  )

  def _fake_launch(*, host: str, port: int, open_browser: bool, explicit_port: bool, helper_recovery: bool, **_: Any) -> dict[str, Any]:
    captured['host'] = host
    captured['port'] = port
    captured['open_browser'] = open_browser
    captured['explicit_port'] = explicit_port
    captured['helper_recovery'] = helper_recovery
    return {
      'decision': 'recovered_via_helper',
      'url': 'http://127.0.0.1:8765/',
      'bound_port': 8765,
      'launch_id': 'launch-123',
    }

  monkeypatch.setattr(cli, '_port_serves_polyventure_console', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(cli, 'launch_detached_operator_console', _fake_launch)

  status, headers, body = _call_helper_app(
    app,
    method='GET',
    path='/recover',
    query='token=helper-123&mode=auto',
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert headers['Access-Control-Allow-Origin'] == '*'
  assert payload['decision'] == 'recovered_via_helper'
  assert payload['recovery'] == 'launched'
  assert payload['mode'] == 'auto'
  assert payload['url'] == 'http://127.0.0.1:8765/'
  assert payload['launch_id'] == 'launch-123'
  assert captured == {
    'host': '127.0.0.1',
    'port': 8765,
    'open_browser': False,
    'explicit_port': False,
    'helper_recovery': True,
  }


def test_console_recovery_helper_skips_relaunch_when_target_already_healthy(monkeypatch) -> None:
  app = cli._create_console_recovery_helper_app(
    helper_token='helper-123',
    helper_expires_at_unix=time.time() + 60.0,
    target_host='127.0.0.1',
    target_port=8765,
  )

  monkeypatch.setattr(cli, '_port_serves_polyventure_console', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(
    cli,
    'launch_detached_operator_console',
    lambda **_: (_ for _ in ()).throw(AssertionError('relaunch should be skipped when host is already healthy')),
  )

  status, _, body = _call_helper_app(
    app,
    method='GET',
    path='/recover',
    query='token=helper-123&mode=auto',
  )
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['decision'] == 'reused_existing_host'
  assert payload['recovery'] == 'already-healthy'
  assert payload['url'] == 'http://127.0.0.1:8765/'


def test_console_recovery_helper_rejects_bad_token_with_launch_blocked() -> None:
  app = cli._create_console_recovery_helper_app(
    helper_token='helper-123',
    helper_expires_at_unix=time.time() + 60.0,
    target_host='127.0.0.1',
    target_port=8765,
  )

  status, _, body = _call_helper_app(
    app,
    method='GET',
    path='/recover',
    query='token=wrong-token&mode=auto',
  )
  payload = json.loads(body)

  assert status == '403 Forbidden'
  assert payload['decision'] == 'launch_blocked'
  assert payload['reason'] == 'recovery_helper_token_mismatch'


def test_launch_detached_operator_console_emits_helper_orphan_self_heal(monkeypatch, tmp_path) -> None:
  (tmp_path / 'src' / 'polyventure').mkdir(parents=True)
  (tmp_path / 'pyproject.toml').write_text('[project]\nname = "polyventure"\n', encoding='utf-8')

  class _FakeProcess:
    pid = 6263

  monkeypatch.setattr(cli.Path, 'cwd', staticmethod(lambda: tmp_path))
  monkeypatch.setattr(cli, '_detached_python_executable', lambda **_: 'pythonw.exe')
  monkeypatch.setattr(cli, '_console_browser_session_active', lambda *_args, **_kwargs: False)
  monkeypatch.setattr(
    cli,
    '_reap_tracked_console_hosts',
    lambda **_: {
      'terminated_pids': [7171],
      'host_pids': [],
      'helper_pids': [7171],
    },
  )
  monkeypatch.setattr(cli, '_reap_preferred_console_host', lambda **_: [])
  monkeypatch.setattr(cli, '_select_console_port', lambda **_: 8765)
  monkeypatch.setattr(cli, '_record_console_host', lambda **_: None)
  monkeypatch.setattr(cli, '_listener_pid_for_port_with_retry', lambda *_args, **_kwargs: 6263)
  monkeypatch.setattr(cli, '_wait_for_console_ready', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_wait_for_console_browser_session_attach', lambda *_args, **_kwargs: True)
  monkeypatch.setattr(cli, '_launch_console_recovery_helper', lambda **_: None)
  monkeypatch.setattr(cli.subprocess, 'Popen', lambda *_args, **_kwargs: _FakeProcess())
  monkeypatch.setattr(cli, '_open_console_browser', lambda _url: True)

  cli.launch_detached_operator_console(host='127.0.0.1', port=8765, open_browser=True)

  events = _read_launcher_events(tmp_path)
  assert [event['event'] for event in events[:6]] == [
    'discover_started',
    'discover_completed',
    'self_heal_applied',
    'helper_orphan_pruned',
    'stale_pruned',
    'port_selected',
  ]
  assert events[2]['state'] == 'SELF_HEAL'
  assert events[3]['reason_code'] == 'helper_orphan'


def test_reap_excludes_calling_process(monkeypatch, tmp_path) -> None:
  import os
  self_pid = os.getpid()
  other_pid = self_pid + 1000

  registry = [
    {
      'pid': self_pid,
      'workspace_root': str(tmp_path),
      'host': '127.0.0.1',
      'port': 8765,
      'role': 'recovery-helper',
    },
    {
      'pid': other_pid,
      'workspace_root': str(tmp_path),
      'host': '127.0.0.1',
      'port': 8766,
      'role': 'host',
    },
  ]

  monkeypatch.setattr(cli, '_load_console_registry', lambda: registry)
  saved: list[dict] = []
  monkeypatch.setattr(cli, '_save_console_registry', lambda entries: saved.extend(entries))
  terminated_pids: list[int] = []
  monkeypatch.setattr(cli, '_terminate_process', lambda pid: terminated_pids.append(pid))
  monkeypatch.setattr(cli, '_process_is_alive', lambda pid: True)
  monkeypatch.setattr(cli, '_wait_for_port_availability', lambda host, port: True)

  result = cli._reap_tracked_console_hosts(workspace_root=tmp_path)

  assert self_pid not in terminated_pids
  assert other_pid in result['terminated_pids']
  self_entries = [e for e in saved if e['pid'] == self_pid]
  assert self_entries

