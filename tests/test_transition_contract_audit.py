from __future__ import annotations

import json

from polyventure.transition_contract_audit import (
  SNAPSHOT_SCHEMA_VERSION,
  build_audit_snapshot,
  build_projection_rows,
  compare_snapshots,
  find_projection_anomalies,
  run_contract_audit_validation,
  run_contract_audit_cli,
  update_contract_audit,
)
from polyventure.web_app import CONTRACT_TABLE


def test_projection_rows_cover_entire_contract_table() -> None:
  rows = build_projection_rows()

  assert len(rows) == len(CONTRACT_TABLE)
  transition_ids = {row['transition_id'] for row in rows}
  assert len(transition_ids) == len(rows)


def test_audit_snapshot_groups_rows_by_rest_state() -> None:
  snapshot = build_audit_snapshot()

  assert snapshot['schema_version'] == SNAPSHOT_SCHEMA_VERSION
  assert snapshot['row_count'] == len(CONTRACT_TABLE)
  grouped = snapshot['rows_by_rest_state']
  assert set(grouped.keys()) == {'1', '2', '3', '4'}


def test_audit_anomaly_detector_rejects_legacy_management_failed_reason() -> None:
  synthetic_row = {
    'transition_id': 'x:websocket_management:no-go:2',
    'transition_key': {
      'prev_state_id': 'unknown',
      'action_name': 'websocket_management',
      'outcome': 'no-go',
      'resting_state_id': '2',
    },
    'resting_state_label': 'No key, websocket configured',
    'projection': {
      'rule_id': 'S2_BAD_REASON',
      'contract_version': '1.9',
      'headline': 'Bad reason leak',
      'operator_message': 'Should not pass audit',
      'recommended_step': 'review_configuration',
      'step_kind': 'fix_configuration',
      'next_actionable_step': 'load_api_key',
      'focus_target': 'readiness-section',
      'focus_tone': 'focus-no-go',
      'deck_view': 'operator',
      'boundary_reason': 'WEBSOCKET MANAGEMENT FAILED',
      'boundary_message': 'Generic fallback leak',
      'boundary_next_action': '',
    },
  }

  anomalies = find_projection_anomalies([synthetic_row])
  issues = {item['issue'] for item in anomalies}

  assert 'legacy_reason_leak' in issues
  assert 'missing_boundary_next_action' in issues


def test_compare_snapshots_marks_changed_projection() -> None:
  baseline = build_audit_snapshot()
  current = json.loads(json.dumps(baseline))

  first_state = sorted(current['rows_by_rest_state'].keys())[0]
  first_row = current['rows_by_rest_state'][first_state][0]
  first_row['projection']['headline'] = first_row['projection']['headline'] + ' (updated)'

  diff = compare_snapshots(current, baseline)

  assert len(diff['changed']) == 1
  assert diff['added'] == []
  assert diff['removed'] == []


def test_state_2_success_rows_expose_complete_key_required_boundary_triad() -> None:
  rows = build_projection_rows()
  target_rule_ids = {
    'S2_WEBSOCKET_LOADED_KEY_MISSING',
    'S2_FROM_4_KEY_CLEARED',
    'S2_SUCCESS_WAITING_FOR_KEY',
  }

  targeted_rows = [
    row for row in rows
    if str(row['projection'].get('rule_id') or '') in target_rule_ids
  ]
  anomalies = find_projection_anomalies(rows)

  assert {str(row['projection'].get('rule_id') or '') for row in targeted_rows} == target_rule_ids
  for row in targeted_rows:
    projection = row['projection']
    assert projection['boundary_reason'] == 'Key reference required'
    assert projection['boundary_message']
    assert projection['boundary_next_action'] == 'Load your API key reference to proceed.'

  targeted_transition_ids = {row['transition_id'] for row in targeted_rows}
  assert not [
    anomaly for anomaly in anomalies
    if anomaly.get('issue') == 'state2_missing_boundary_reason'
    and anomaly.get('transition_id') in targeted_transition_ids
  ]


def test_update_contract_audit_rejects_missing_signed_auth(tmp_path) -> None:
  spec_path = tmp_path / 'spec.json'
  spec_path.write_text(
    json.dumps(
      {
        'selector': {'rule_id': 'S2_GENERIC_NOGO_KEY_REQUIRED'},
        'updates': {'headline': 'Updated headline'},
      }
    ),
    encoding='utf-8',
  )

  payload, exit_code = update_contract_audit(spec_path=spec_path)

  assert exit_code == 2
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'signed_auth_missing_required_fields'


def test_update_contract_audit_rejects_out_of_scope_fields(tmp_path) -> None:
  spec_path = tmp_path / 'spec.json'
  spec_path.write_text(
    json.dumps(
      {
        'selector': {'rule_id': 'S2_GENERIC_NOGO_KEY_REQUIRED'},
        'updates': {'rule_id': 'ILLEGAL_CHANGE'},
        'auth': {
          'signer_key_id': 'test-signer',
          'signature_b64': 'stub-signature',
          'nonce': 'nonce-1',
          'policy_scope_id': 'policy-v1',
        },
      }
    ),
    encoding='utf-8',
  )

  payload, exit_code = update_contract_audit(spec_path=spec_path)

  assert exit_code == 2
  assert payload['decision'] == 'no-go'
  assert payload['reason'] == 'out_of_scope_mutation_fields'


def test_run_contract_audit_cli_rejects_non_v1_status_verb() -> None:
  try:
    run_contract_audit_cli(['status'])
  except SystemExit as exc:
    assert exc.code == 2
  else:
    raise AssertionError('Expected parser rejection for non-V1 status verb.')


def test_highlight_validator_rejects_invalid_tone() -> None:
  payload = {
    'workflow': {
      'highlight_policy_version': 'orchestrator-highlights.v1',
      'deck_action_highlights': {'key_management': 'bad-tone'},
      'detail_control_highlights': {},
    }
  }

  result = run_contract_audit_validation(payload)

  assert result['decision'] == 'no-go'
  assert 'INVALID_HIGHLIGHT_TONE' in result['error_codes']


def test_highlight_validator_rejects_invalid_key() -> None:
  payload = {
    'workflow': {
      'highlight_policy_version': 'orchestrator-highlights.v1',
      'deck_action_highlights': {'unknown_action': 'no-go'},
      'detail_control_highlights': {},
    }
  }

  result = run_contract_audit_validation(payload)

  assert result['decision'] == 'no-go'
  assert 'INVALID_HIGHLIGHT_KEY' in result['error_codes']


def test_highlight_validator_requires_policy_version_when_highlights_present() -> None:
  payload = {
    'workflow': {
      'deck_action_highlights': {'key_management': 'no-go'},
      'detail_control_highlights': {},
    }
  }

  result = run_contract_audit_validation(payload)

  assert result['decision'] == 'no-go'
  assert 'MISSING_HIGHLIGHT_POLICY_VERSION' in result['error_codes']