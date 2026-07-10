from __future__ import annotations

import argparse
import copy
import difflib
import importlib
import json
import re
from datetime import datetime, timezone
from collections import defaultdict
from dataclasses import asdict
from pathlib import Path
from typing import Any
import uuid

from . import web_app
from .web_app import TransitionKey


REST_STATE_LABELS = {
  '1': 'No key, no websocket',
  '2': 'No key, websocket configured',
  '3': 'Key loaded, no websocket',
  '4': 'Key loaded, websocket configured',
}

SNAPSHOT_SCHEMA_VERSION = 'rest-state-message-audit.v1'
ALLOWED_MUTATION_FIELDS = {
  'headline',
  'operator_message',
  'recommended_step',
  'step_kind',
  'next_actionable_step',
  'focus_target',
  'focus_tone',
  'deck_view',
  'boundary_reason',
  'boundary_message',
  'boundary_next_action',
}
HIGHLIGHT_POLICY_VERSION = 'orchestrator-highlights.v1'
HIGHLIGHT_TONES = {'ok', 'warn', 'no-go'}
HIGHLIGHT_ERROR_CODES = {
  'invalid_tone': 'INVALID_HIGHLIGHT_TONE',
  'invalid_key': 'INVALID_HIGHLIGHT_KEY',
  'missing_policy_version': 'MISSING_HIGHLIGHT_POLICY_VERSION',
}


def validate_highlight_envelope(payload: dict[str, Any]) -> dict[str, Any]:
  workflow = payload.get('workflow') if isinstance(payload, dict) else None
  workflow = workflow if isinstance(workflow, dict) else {}

  deck_highlights = workflow.get('deck_action_highlights')
  detail_highlights = workflow.get('detail_control_highlights')
  has_deck = isinstance(deck_highlights, dict) and bool(deck_highlights)
  has_detail = isinstance(detail_highlights, dict) and bool(detail_highlights)

  error_codes: set[str] = set()

  if has_deck and str(workflow.get('highlight_policy_version') or '').strip() != HIGHLIGHT_POLICY_VERSION:
    error_codes.add(HIGHLIGHT_ERROR_CODES['missing_policy_version'])
  if has_detail and str(workflow.get('highlight_policy_version') or '').strip() != HIGHLIGHT_POLICY_VERSION:
    error_codes.add(HIGHLIGHT_ERROR_CODES['missing_policy_version'])

  if isinstance(deck_highlights, dict):
    for key, tone in deck_highlights.items():
      if str(key) not in web_app.DECK_ACTION_KEYS:
        error_codes.add(HIGHLIGHT_ERROR_CODES['invalid_key'])
      if str(tone).lower() not in HIGHLIGHT_TONES:
        error_codes.add(HIGHLIGHT_ERROR_CODES['invalid_tone'])

  if isinstance(detail_highlights, dict):
    for key, tone in detail_highlights.items():
      if str(key) not in web_app.DETAIL_CONTROL_KEYS:
        error_codes.add(HIGHLIGHT_ERROR_CODES['invalid_key'])
      if str(tone).lower() not in HIGHLIGHT_TONES:
        error_codes.add(HIGHLIGHT_ERROR_CODES['invalid_tone'])

  return {
    'decision': 'no-go' if error_codes else 'go',
    'error_codes': sorted(error_codes),
  }


def run_contract_audit_validation(payload: dict[str, Any]) -> dict[str, Any]:
  return validate_highlight_envelope(payload)


def _contract_table() -> dict[TransitionKey, Any]:
  return dict(web_app.CONTRACT_TABLE)


def _transition_id(key: TransitionKey) -> str:
  return f'{key.prev_state_id}:{key.action_name}:{key.outcome}:{key.resting_state_id}'


def build_projection_rows() -> list[dict[str, Any]]:
  rows: list[dict[str, Any]] = []
  for key, record in _contract_table().items():
    rows.append(
      {
        'transition_id': _transition_id(key),
        'transition_key': asdict(key),
        'resting_state_label': REST_STATE_LABELS.get(key.resting_state_id, 'Unknown state'),
        'projection': {
          'rule_id': record.rule_id,
          'contract_version': record.contract_version,
          'headline': record.headline,
          'operator_message': record.operator_message,
          'recommended_step': record.recommended_step,
          'step_kind': record.step_kind,
          'next_actionable_step': record.next_actionable_step,
          'focus_target': record.focus_target,
          'focus_tone': record.focus_tone,
          'deck_view': record.deck_view,
          'boundary_reason': record.boundary_reason,
          'boundary_message': record.boundary_message,
          'boundary_next_action': record.boundary_next_action,
        },
      }
    )
  return sorted(rows, key=lambda item: item['transition_id'])


def group_rows_by_rest_state(rows: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
  grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
  for row in rows:
    grouped[row['transition_key']['resting_state_id']].append(row)
  for state_rows in grouped.values():
    state_rows.sort(key=lambda item: item['transition_id'])
  return dict(sorted(grouped.items(), key=lambda item: item[0]))


def find_projection_anomalies(rows: list[dict[str, Any]]) -> list[dict[str, str]]:
  anomalies: list[dict[str, str]] = []
  for row in rows:
    transition_id = row['transition_id']
    key = row['transition_key']
    projection = row['projection']
    boundary_reason = str(projection.get('boundary_reason') or '')

    if boundary_reason:
      lowered = boundary_reason.lower()
      if lowered.endswith('_failed') or 'management failed' in lowered:
        anomalies.append(
          {
            'transition_id': transition_id,
            'issue': 'legacy_reason_leak',
            'detail': f"Boundary reason uses generic failure wording: '{boundary_reason}'",
          }
        )
      if not projection.get('boundary_next_action'):
        anomalies.append(
          {
            'transition_id': transition_id,
            'issue': 'missing_boundary_next_action',
            'detail': 'Boundary reason exists without boundary_next_action guidance.',
          }
        )

    if (
      key['resting_state_id'] == '2'
      and key['outcome'] in {'success', 'no-go'}
      and key['prev_state_id'] in {'1', '3', '4', 'unknown'}
    ):
      if not boundary_reason:
        anomalies.append(
          {
            'transition_id': transition_id,
            'issue': 'state2_missing_boundary_reason',
            'detail': 'Rest state 2 should expose explicit boundary_reason for key-required no-go posture.',
          }
        )

  return anomalies


def build_audit_snapshot() -> dict[str, Any]:
  rows = build_projection_rows()
  grouped = group_rows_by_rest_state(rows)
  anomalies = find_projection_anomalies(rows)
  return {
    'schema_version': SNAPSHOT_SCHEMA_VERSION,
    'row_count': len(rows),
    'rows_by_rest_state': grouped,
    'anomalies': anomalies,
  }


def _serialize_now_utc() -> str:
  return datetime.now(timezone.utc).isoformat()


def _decision_payload(*, command: str, decision: str, reason: str, message: str, next_action: str, run_id: str | None = None) -> dict[str, Any]:
  payload: dict[str, Any] = {
    'decision': decision,
    'command_family': command,
    'reason': reason,
    'message': message,
    'next_action': next_action,
    'verified_at_utc': _serialize_now_utc(),
  }
  if run_id:
    payload['run_id'] = run_id
  return payload


def _load_json_file(path: Path) -> dict[str, Any]:
  return json.loads(path.read_text(encoding='utf-8'))


def _python_literal(value: Any) -> str:
  if value is None:
    return 'None'
  if isinstance(value, bool):
    return 'True' if value else 'False'
  if isinstance(value, (int, float)):
    return repr(value)
  return repr(str(value))


def _find_rule_block(lines: list[str], rule_id: str) -> tuple[int, int]:
  rule_pattern = re.compile(rf"\brule_id\s*=\s*['\"]{re.escape(rule_id)}['\"]")
  rule_index = -1
  for idx, line in enumerate(lines):
    if rule_pattern.search(line):
      if rule_index != -1:
        raise ValueError(f"Ambiguous selector: rule_id '{rule_id}' appears more than once in source.")
      rule_index = idx
  if rule_index == -1:
    raise ValueError(f"Rule id '{rule_id}' was not found in contract table source.")

  start = -1
  for idx in range(rule_index, -1, -1):
    if 'ViewContractRecord(' in lines[idx]:
      start = idx
      break
  if start == -1:
    raise ValueError(f"Could not locate ViewContractRecord block start for rule_id '{rule_id}'.")

  end = -1
  for idx in range(rule_index, len(lines)):
    if lines[idx].strip() == '),':
      end = idx
      break
  if end == -1:
    raise ValueError(f"Could not locate ViewContractRecord block end for rule_id '{rule_id}'.")

  return start, end


def _apply_field_updates_to_block(block: str, updates: dict[str, Any]) -> str:
  updated_block = block
  for field, value in updates.items():
    assignment_pattern = re.compile(
      rf"(^\s*{re.escape(field)}\s*=\s*)([^,\n]+)(,\s*$)",
      re.MULTILINE,
    )
    replacement = rf"\1{_python_literal(value)}\3"
    updated_block, count = assignment_pattern.subn(replacement, updated_block, count=1)
    if count != 1:
      raise ValueError(f"Mutation failed: field '{field}' was not found exactly once in target contract block.")
  return updated_block


def _resolve_target_row(snapshot: dict[str, Any], selector: dict[str, Any]) -> dict[str, Any]:
  grouped = snapshot.get('rows_by_rest_state', {})
  rows: list[dict[str, Any]] = []
  for state_rows in grouped.values():
    rows.extend(state_rows)

  transition_id = str(selector.get('transition_id') or '').strip()
  rule_id = str(selector.get('rule_id') or '').strip()

  if transition_id:
    matches = [row for row in rows if row['transition_id'] == transition_id]
  elif rule_id:
    matches = [row for row in rows if str(row['projection'].get('rule_id') or '') == rule_id]
  else:
    raise ValueError('Mutation selector must provide transition_id or rule_id.')

  if not matches:
    raise ValueError('Mutation selector did not match any transition contract row.')
  if len(matches) > 1:
    raise ValueError('Mutation selector was ambiguous; expected exactly one matching transition row.')
  return matches[0]


def _validate_auth_envelope(auth: dict[str, Any]) -> None:
  required = ('signer_key_id', 'signature_b64', 'nonce', 'policy_scope_id')
  missing = [name for name in required if not str(auth.get(name) or '').strip()]
  if missing:
    raise ValueError(f"Signed mutation auth envelope is missing required field(s): {', '.join(missing)}")


def _project_root_from_web_app_file(web_app_file: Path) -> Path:
  candidate = web_app_file.resolve()
  if candidate.parent.name == 'polyventure' and candidate.parent.parent.name == 'src':
    return candidate.parent.parent.parent
  return candidate.parents[2]


def _write_update_ledger(
  *,
  project_root: Path,
  run_id: str,
  request_payload: dict[str, Any],
  pre_snapshot: dict[str, Any],
  post_snapshot: dict[str, Any],
  diff: dict[str, list[str]],
  validator_matrix: list[dict[str, Any]],
  applied_patch: str,
  decision_payload: dict[str, Any],
) -> Path:
  ledger_root = project_root / '.polyventure' / 'generated' / 'contract_audit' / 'ledger'
  run_dir = ledger_root / run_id
  run_dir.mkdir(parents=True, exist_ok=True)

  (run_dir / 'request.json').write_text(
    json.dumps(request_payload, indent=2, ensure_ascii=False, sort_keys=True) + '\n',
    encoding='utf-8',
  )
  (run_dir / 'pre_snapshot.json').write_text(
    json.dumps(pre_snapshot, indent=2, ensure_ascii=False, sort_keys=True) + '\n',
    encoding='utf-8',
  )
  (run_dir / 'post_snapshot.json').write_text(
    json.dumps(post_snapshot, indent=2, ensure_ascii=False, sort_keys=True) + '\n',
    encoding='utf-8',
  )
  (run_dir / 'diff.json').write_text(
    json.dumps(diff, indent=2, ensure_ascii=False, sort_keys=True) + '\n',
    encoding='utf-8',
  )
  (run_dir / 'validator_matrix.json').write_text(
    json.dumps({'checks': validator_matrix}, indent=2, ensure_ascii=False, sort_keys=True) + '\n',
    encoding='utf-8',
  )
  (run_dir / 'apply.patch').write_text(applied_patch, encoding='utf-8')
  (run_dir / 'decision.json').write_text(
    json.dumps(decision_payload, indent=2, ensure_ascii=False, sort_keys=True) + '\n',
    encoding='utf-8',
  )

  index_path = ledger_root / 'index.jsonl'
  index_path.parent.mkdir(parents=True, exist_ok=True)
  index_row = {
    'run_id': run_id,
    'timestamp_utc': _serialize_now_utc(),
    'command': 'polyventure contract-audit update',
    'decision': decision_payload.get('decision'),
    'reason_family': decision_payload.get('reason'),
    'mutated': True,
    'pre_snapshot_sha256': None,
    'post_snapshot_sha256': None,
    'policy_scope_id': request_payload.get('auth', {}).get('policy_scope_id'),
    'signature_status': 'provided',
  }
  with index_path.open('a', encoding='utf-8') as handle:
    handle.write(json.dumps(index_row, ensure_ascii=False, sort_keys=True) + '\n')

  return run_dir


def review_contract_audit(
  *,
  output_format: str,
  baseline: Path | None,
  output: Path | None,
  write_baseline: bool,
  fail_on_drift: bool,
) -> tuple[dict[str, Any], int]:
  snapshot = build_audit_snapshot()
  diff: dict[str, list[str]] | None = None

  if baseline is not None and baseline.exists():
    baseline_payload = _load_json_file(baseline)
    diff = compare_snapshots(snapshot, baseline_payload)

  if write_baseline:
    if baseline is None:
      raise ValueError('--write-baseline requires --baseline <path>.')
    baseline.parent.mkdir(parents=True, exist_ok=True)
    baseline.write_text(
      json.dumps(snapshot, indent=2, ensure_ascii=False, sort_keys=True),
      encoding='utf-8',
    )

  rendered: str
  if output_format == 'json':
    rendered = json.dumps({'snapshot': snapshot, 'diff': diff}, indent=2, ensure_ascii=False, sort_keys=True)
  else:
    rendered = render_markdown(snapshot, diff)

  if output is None:
    print(rendered)
  else:
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered + '\n', encoding='utf-8')

  has_anomalies = bool(snapshot.get('anomalies'))
  has_drift = bool(diff and (diff.get('added') or diff.get('removed') or diff.get('changed')))
  should_fail = bool(fail_on_drift and (has_anomalies or has_drift))
  decision = 'no-go' if should_fail else 'go'
  payload = {
    'decision': decision,
    'command_family': 'polyventure contract-audit review',
    'reason': 'audit_anomalies_or_drift_detected' if should_fail else 'audit_passed_or_info_only',
    'row_count': snapshot.get('row_count', 0),
    'anomaly_count': len(snapshot.get('anomalies', [])),
    'drift': diff,
    'next_action': (
      'Resolve anomalies/drift before applying transition-contract updates.'
      if should_fail
      else 'Use contract-audit update with a signed mutation envelope when targeted changes are required.'
    ),
  }
  return payload, (1 if should_fail else 0)


def update_contract_audit(*, spec_path: Path, web_app_file: Path | None = None) -> tuple[dict[str, Any], int]:
  if not spec_path.exists():
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='mutation_spec_missing',
      message=f'Mutation spec file was not found: {spec_path}',
      next_action='Provide --spec with an existing JSON file and retry.',
    )
    return payload, 2

  spec = _load_json_file(spec_path)
  selector = spec.get('selector', {})
  updates = spec.get('updates', {})
  expected_before = spec.get('expected_before', {})
  auth = spec.get('auth', {})

  run_id = f'contract-audit-{datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")}-{uuid.uuid4().hex[:8]}'

  if not isinstance(selector, dict) or not isinstance(updates, dict) or not isinstance(expected_before, dict) or not isinstance(auth, dict):
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='mutation_spec_invalid_shape',
      message='Mutation spec requires object fields: selector, updates, expected_before, auth.',
      next_action='Fix the mutation spec shape and retry update.',
      run_id=run_id,
    )
    return payload, 2

  try:
    _validate_auth_envelope(auth)
  except ValueError as exc:
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='signed_auth_missing_required_fields',
      message=str(exc),
      next_action='Provide a complete signed auth envelope and retry update.',
      run_id=run_id,
    )
    return payload, 2

  unknown_fields = sorted(set(str(name) for name in updates.keys()) - ALLOWED_MUTATION_FIELDS)
  if unknown_fields:
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='out_of_scope_mutation_fields',
      message=f'Mutation touched non-allowlisted field(s): {", ".join(unknown_fields)}',
      next_action='Restrict updates to allowlisted projection fields only.',
      run_id=run_id,
    )
    return payload, 2

  pre_snapshot = build_audit_snapshot()
  try:
    target_row = _resolve_target_row(pre_snapshot, selector)
  except ValueError as exc:
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='target_selector_resolution_failed',
      message=str(exc),
      next_action='Correct selector.transition_id or selector.rule_id and retry update.',
      run_id=run_id,
    )
    return payload, 2

  target_transition_id = str(target_row['transition_id'])
  target_rule_id = str(target_row['projection'].get('rule_id') or '')

  for field, expected_value in expected_before.items():
    current = target_row['projection'].get(field)
    if current != expected_value:
      payload = _decision_payload(
        command='polyventure contract-audit update',
        decision='no-go',
        reason='optimistic_lock_mismatch',
        message=(
          f"expected_before mismatch for '{field}': expected {expected_value!r}, current {current!r}."
        ),
        next_action='Refresh review output, update expected_before values, and retry update.',
        run_id=run_id,
      )
      return payload, 2

  source_file = web_app_file or Path(web_app.__file__).resolve()
  if not source_file.exists():
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='contract_source_missing',
      message=f'Contract source file not found: {source_file}',
      next_action='Restore the contract source file and retry update.',
      run_id=run_id,
    )
    return payload, 2

  original_text = source_file.read_text(encoding='utf-8')
  original_lines = original_text.splitlines(keepends=True)

  try:
    start, end = _find_rule_block(original_lines, target_rule_id)
    target_block = ''.join(original_lines[start:end + 1])
    updated_block = _apply_field_updates_to_block(target_block, updates)
  except ValueError as exc:
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='source_patch_generation_failed',
      message=str(exc),
      next_action='Correct the selector/updates and retry update.',
      run_id=run_id,
    )
    return payload, 2

  mutated_lines = copy.copy(original_lines)
  mutated_lines[start:end + 1] = [updated_block]
  mutated_text = ''.join(mutated_lines)

  if mutated_text == original_text:
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='no_effective_change',
      message='Mutation did not change any contract projection fields.',
      next_action='Adjust updates values or selector and retry update.',
      run_id=run_id,
    )
    return payload, 2

  source_file.write_text(mutated_text, encoding='utf-8')

  try:
    importlib.reload(web_app)
    post_snapshot = build_audit_snapshot()
  except Exception as exc:
    source_file.write_text(original_text, encoding='utf-8')
    importlib.reload(web_app)
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='post_apply_reload_failed',
      message=f'Updated contract source could not be loaded safely: {exc}',
      next_action='Fix mutation payload or contract syntax and retry update.',
      run_id=run_id,
    )
    return payload, 1

  diff = compare_snapshots(post_snapshot, pre_snapshot)
  blocker_anomalies = [
    item for item in post_snapshot.get('anomalies', [])
    if item.get('issue') in {'legacy_reason_leak', 'missing_boundary_next_action', 'state2_missing_boundary_reason'}
  ]
  out_of_scope_drift = bool(
    diff.get('added')
    or diff.get('removed')
    or any(item != target_transition_id for item in diff.get('changed', []))
  )

  validator_matrix = [
    {
      'check': 'target_selector_resolved',
      'severity': 'blocker',
      'status': 'pass',
      'detail': target_transition_id,
    },
    {
      'check': 'mutation_fields_allowlisted',
      'severity': 'blocker',
      'status': 'pass',
      'detail': sorted(list(updates.keys())),
    },
    {
      'check': 'post_apply_anomaly_blockers',
      'severity': 'blocker',
      'status': 'fail' if blocker_anomalies else 'pass',
      'detail': blocker_anomalies,
    },
    {
      'check': 'out_of_scope_drift',
      'severity': 'blocker',
      'status': 'fail' if out_of_scope_drift else 'pass',
      'detail': diff,
    },
  ]

  if blocker_anomalies or out_of_scope_drift:
    source_file.write_text(original_text, encoding='utf-8')
    importlib.reload(web_app)
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='post_apply_validator_blocked',
      message='Post-apply validation blocked the mutation; source was restored automatically.',
      next_action='Review validator failures and submit a corrected mutation.',
      run_id=run_id,
    )
    payload['validator_matrix'] = validator_matrix
    return payload, 1

  patch_text = ''.join(
    difflib.unified_diff(
      original_text.splitlines(keepends=True),
      mutated_text.splitlines(keepends=True),
      fromfile=str(source_file),
      tofile=str(source_file),
    )
  )

  decision_payload = _decision_payload(
    command='polyventure contract-audit update',
    decision='go',
    reason='mutation_applied_and_validated',
    message='Transition contract mutation applied and validator checks passed.',
    next_action='Run contract-audit review to confirm the current projection posture.',
    run_id=run_id,
  )
  decision_payload['validator_matrix'] = validator_matrix
  decision_payload['diff'] = diff

  project_root = _project_root_from_web_app_file(source_file)
  run_dir = _write_update_ledger(
    project_root=project_root,
    run_id=run_id,
    request_payload=spec,
    pre_snapshot=pre_snapshot,
    post_snapshot=post_snapshot,
    diff=diff,
    validator_matrix=validator_matrix,
    applied_patch=patch_text,
    decision_payload=decision_payload,
  )
  decision_payload['evidence_dir'] = str(run_dir)
  return decision_payload, 0


def run_contract_audit_cli(argv: list[str] | None = None, *, force_json: bool = False) -> int:
  parser = argparse.ArgumentParser(
    description='V1 transition-contract audit surface (review/update only).'
  )
  parser.add_argument('--json', action='store_true', help='Emit JSON output only.')
  subparsers = parser.add_subparsers(dest='command', required=True)

  review = subparsers.add_parser('review', help='Review transition-contract projections grouped by rest state.')
  review.add_argument('--format', choices=('markdown', 'json'), default='markdown')
  review.add_argument('--output', type=Path, default=None)
  review.add_argument('--baseline', type=Path, default=None)
  review.add_argument('--write-baseline', action='store_true')
  review.add_argument('--fail-on-drift', action='store_true')

  update = subparsers.add_parser('update', help='Apply a targeted contract mutation from a signed spec.')
  update.add_argument('--spec', type=Path, required=True)
  update.add_argument('--web-app-file', type=Path, default=None, help=argparse.SUPPRESS)

  args = parser.parse_args(argv)
  as_json = bool(force_json or args.json)

  if args.command == 'review':
    try:
      payload, exit_code = review_contract_audit(
        output_format=args.format,
        baseline=args.baseline,
        output=args.output,
        write_baseline=args.write_baseline,
        fail_on_drift=args.fail_on_drift,
      )
    except Exception as exc:
      payload = _decision_payload(
        command='polyventure contract-audit review',
        decision='no-go',
        reason='review_execution_failed',
        message=str(exc),
        next_action='Fix the reported review input issue and retry.',
      )
      exit_code = 1
    if as_json:
      print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return exit_code

  try:
    payload, exit_code = update_contract_audit(
      spec_path=args.spec,
      web_app_file=args.web_app_file,
    )
  except Exception as exc:
    payload = _decision_payload(
      command='polyventure contract-audit update',
      decision='no-go',
      reason='update_execution_failed',
      message=str(exc),
      next_action='Fix the mutation input and retry update.',
    )
    exit_code = 1

  if as_json:
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
  else:
    print(f"Polyventure :: contract-audit {args.command}")
    print(f"decision: {payload.get('decision', 'no-go')}")
    print()
    print('Summary')
    print(f"  reason:            {payload.get('reason', '--')}")
    print(f"  message:           {payload.get('message', '--')}")
    if payload.get('run_id'):
      print(f"  run_id:            {payload['run_id']}")
    if payload.get('evidence_dir'):
      print(f"  evidence_dir:      {payload['evidence_dir']}")
    print()
    print('Next action')
    print(f"  {payload.get('next_action', 'Review retained evidence and proceed safely.')}")
  return exit_code


def compare_snapshots(current: dict[str, Any], baseline: dict[str, Any]) -> dict[str, list[str]]:
  def _flatten(snapshot: dict[str, Any]) -> dict[str, str]:
    flat: dict[str, str] = {}
    grouped = snapshot.get('rows_by_rest_state', {})
    for state_rows in grouped.values():
      for row in state_rows:
        transition_id = str(row['transition_id'])
        flat[transition_id] = json.dumps(row['projection'], sort_keys=True, ensure_ascii=False)
    return flat

  current_flat = _flatten(current)
  baseline_flat = _flatten(baseline)

  added = sorted(set(current_flat.keys()) - set(baseline_flat.keys()))
  removed = sorted(set(baseline_flat.keys()) - set(current_flat.keys()))
  changed = sorted(
    transition_id
    for transition_id in set(current_flat.keys()) & set(baseline_flat.keys())
    if current_flat[transition_id] != baseline_flat[transition_id]
  )
  return {
    'added': added,
    'removed': removed,
    'changed': changed,
  }


def render_markdown(snapshot: dict[str, Any], diff: dict[str, list[str]] | None = None) -> str:
  lines: list[str] = []
  lines.append('# Resting-state projection audit')
  lines.append('')
  lines.append(f"- Schema: `{snapshot.get('schema_version')}`")
  lines.append(f"- Rows: `{snapshot.get('row_count')}`")
  lines.append(f"- Anomalies: `{len(snapshot.get('anomalies', []))}`")
  lines.append('')

  grouped = snapshot.get('rows_by_rest_state', {})
  for state_id, rows in grouped.items():
    label = REST_STATE_LABELS.get(state_id, 'Unknown state')
    lines.append(f"## Rest state {state_id} — {label}")
    lines.append('')
    lines.append('| transition | rule_id | outcome | headline | boundary_reason | next_actionable_step |')
    lines.append('| --- | --- | --- | --- | --- | --- |')
    for row in rows:
      key = row['transition_key']
      projection = row['projection']
      lines.append(
        '| {transition} | {rule_id} | {outcome} | {headline} | {boundary_reason} | {next_step} |'.format(
          transition=row['transition_id'],
          rule_id=projection.get('rule_id', '--'),
          outcome=key.get('outcome', '--'),
          headline=str(projection.get('headline') or '--').replace('|', '/'),
          boundary_reason=str(projection.get('boundary_reason') or '--').replace('|', '/'),
          next_step=str(projection.get('next_actionable_step') or '--').replace('|', '/'),
        )
      )
    lines.append('')

  anomalies = snapshot.get('anomalies', [])
  lines.append('## Anomalies')
  lines.append('')
  if not anomalies:
    lines.append('- None detected.')
  else:
    for anomaly in anomalies:
      lines.append(
        f"- `{anomaly['transition_id']}` · `{anomaly['issue']}` · {anomaly['detail']}"
      )
  lines.append('')

  if diff is not None:
    lines.append('## Drift vs baseline')
    lines.append('')
    lines.append(f"- Added: `{len(diff.get('added', []))}`")
    lines.append(f"- Removed: `{len(diff.get('removed', []))}`")
    lines.append(f"- Changed: `{len(diff.get('changed', []))}`")
    lines.append('')
    for label in ('added', 'removed', 'changed'):
      values = diff.get(label, [])
      if values:
        lines.append(f"### {label.title()}")
        lines.append('')
        for value in values:
          lines.append(f'- `{value}`')
        lines.append('')

  return '\n'.join(lines)


def run_cli(argv: list[str] | None = None) -> int:
  if argv and any(arg in {'review', 'update'} for arg in argv):
    # Legacy bridge: if explicit subcommands are passed to this module script, route to V1 runner.
    return run_contract_audit_cli(argv)

  parser = argparse.ArgumentParser(
    description='Audit and diff Polyventure resting-state transition-contract message projections.'
  )
  parser.add_argument('--format', choices=('markdown', 'json'), default='markdown')
  parser.add_argument('--output', type=Path, default=None)
  parser.add_argument('--baseline', type=Path, default=None)
  parser.add_argument('--write-baseline', action='store_true')
  parser.add_argument('--fail-on-drift', action='store_true')
  args = parser.parse_args(argv)

  snapshot = build_audit_snapshot()
  diff: dict[str, list[str]] | None = None

  if args.baseline is not None and args.baseline.exists():
    baseline_payload = json.loads(args.baseline.read_text(encoding='utf-8'))
    diff = compare_snapshots(snapshot, baseline_payload)

  if args.write_baseline:
    if args.baseline is None:
      raise SystemExit('--write-baseline requires --baseline <path>.')
    args.baseline.parent.mkdir(parents=True, exist_ok=True)
    args.baseline.write_text(
      json.dumps(snapshot, indent=2, ensure_ascii=False, sort_keys=True),
      encoding='utf-8',
    )

  if args.format == 'json':
    rendered = json.dumps(
      {
        'snapshot': snapshot,
        'diff': diff,
      },
      indent=2,
      ensure_ascii=False,
      sort_keys=True,
    )
  else:
    rendered = render_markdown(snapshot, diff)

  if args.output is None:
    print(rendered)
  else:
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered + '\n', encoding='utf-8')
    print(f'Audit output written: {args.output}')

  has_anomalies = bool(snapshot.get('anomalies'))
  has_drift = bool(diff and (diff.get('added') or diff.get('removed') or diff.get('changed')))
  if args.fail_on_drift and (has_anomalies or has_drift):
    return 1
  return 0


if __name__ == '__main__':
  raise SystemExit(run_cli())