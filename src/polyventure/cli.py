from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from typing import Any, Callable
from urllib import error as urllib_error
from urllib import request as urllib_request
from urllib.parse import parse_qs
import uuid
import webbrowser
from wsgiref.simple_server import make_server

from .config import _read_dotenv_file, load_settings, settings_for_lane
from .persistence import (
  api_key_hash_for_id,
  build_datapack_bundle,
  datapack_manifest_checksum,
  datapack_payload_checksum,
  sign_datapack_manifest,
  evaluate_datapack_convergence,
  evaluate_datapack_identity,
  open_database,
  profile_token_for_key_path,
  rebind_datapack_controls,
  serialize_datapack_json,
  synthetic_refinement_fixture_family,
  validate_datapack_artifacts,
  validate_datapack_controls,
)
from .service import cancel_all_pairs, reconcile_pairs, report_runtime, run_scan_once, run_service_once
from .transition_contract_audit import run_contract_audit_cli
from .web_app import run_operator_console_server


CONSOLE_HOST_REGISTRY = Path(tempfile.gettempdir()) / 'polyventure_console_hosts.json'
LAUNCHER_AUDIT_DIRNAME = 'polyventure_launcher_audit'
DETACHED_CONSOLE_STARTUP_GRACE_SEC = 45.0
DETACHED_CONSOLE_IDLE_TIMEOUT_SEC = 90.0
DETACHED_CONSOLE_RECOVERY_HELPER_LIFETIME_SEC = 180.0
DETACHED_CONSOLE_READY_WAIT_SEC = 20.0
# The recovery helper is a second cold-start Python process; a 5s budget killed
# healthy-but-slow helpers on cold launches (Phase-1 finding), forcing a degraded
# launch / extra cycle. Give it a cold-start-aware budget below the host's window,
# and the wait now exits early if the helper process actually crashes.
DETACHED_CONSOLE_HELPER_READY_WAIT_SEC = 15.0
DETACHED_CONSOLE_REATTACH_WAIT_SEC = 3.0
DETACHED_CONSOLE_FIRST_ATTACH_WAIT_SEC = 15.0
DETACHED_CONSOLE_ATTACH_RETRY_WAIT_SEC = 5.0
DETACHED_CONSOLE_RETRY_PRECONDITION_WAIT_SEC = 3.0
DETACHED_CONSOLE_LAUNCH_LOCK_TTL_SEC = 90.0
DETACHED_CONSOLE_REGISTRY_FRESHNESS_SEC = 86400.0
CANONICAL_DATAPACK_ROOT_RELATIVE = Path('var') / 'datapack_extracts'
CANONICAL_DATAPACK_ARCHIVE_ROOT_RELATIVE = Path('var') / 'datapack_extracts_archive'
CANONICAL_DATAPACK_LEDGER_RELATIVE = Path('var') / 'datapack_store' / 'canonical_mutation_ledger.jsonl'


class ConsoleLaunchBlockedError(RuntimeError):
  def __init__(self, message: str, *, reason: str = 'console_failed', next_action: str | None = None) -> None:
    super().__init__(message)
    self.reason = reason
    self.next_action = next_action or 'Fix the local shell startup issue and retry the console command.'


def _utc_now_iso() -> str:
  return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace('+00:00', 'Z')


def _workspace_digest(workspace_root: Path) -> str:
  return hashlib.sha256(str(workspace_root).encode('utf-8')).hexdigest()[:16]


def _launcher_telemetry_path(*, workspace_root: Path, at_utc: datetime | None = None) -> Path:
  timestamp = at_utc or datetime.now(timezone.utc)
  date_stamp = timestamp.strftime('%Y%m%d')
  return workspace_root / 'temp' / LAUNCHER_AUDIT_DIRNAME / f'launcher_events_{date_stamp}.jsonl'


def _append_launcher_telemetry_event(
  *,
  workspace_root: Path,
  launch_id: str,
  event: str,
  state: str,
  requested_port: int | None,
  bound_port: int | None = None,
  host_pid: int | None = None,
  helper_pid: int | None = None,
  code_signature_match: bool | None = None,
  session_attach_required: bool = True,
  session_attach_confirmed: bool | None = None,
  cleanup_actions: list[str] | None = None,
  decision: str | None = None,
  reason_code: str | None = None,
  notes: str | None = None,
) -> None:
  telemetry_path = _launcher_telemetry_path(workspace_root=workspace_root)
  telemetry_path.parent.mkdir(parents=True, exist_ok=True)
  payload = {
    'timestamp_utc': _utc_now_iso(),
    'launch_id': launch_id,
    'workspace_digest': _workspace_digest(workspace_root),
    'event': event,
    'decision': decision,
    'state': state,
    'requested_port': requested_port,
    'bound_port': bound_port,
    'host_pid': host_pid,
    'helper_pid': helper_pid,
    'code_signature_match': code_signature_match,
    'session_attach_required': session_attach_required,
    'session_attach_confirmed': session_attach_confirmed,
    'cleanup_actions': list(cleanup_actions or []),
    'reason_code': reason_code,
    'notes': notes,
  }
  with telemetry_path.open('a', encoding='utf-8') as handle:
    handle.write(json.dumps(payload, default=str))
    handle.write('\n')


def _launcher_cleanup_actions(*, helper_host: str, helper_port: int) -> list[str]:
  actions = [
    'terminate_host_processes',
    'remove_registry_entries',
    'wait_for_host_port_release',
  ]
  if helper_host and helper_port > 0:
    actions.extend(
      [
        'terminate_helper_processes',
        'wait_for_helper_port_release',
      ]
    )
  return actions


def _append_launcher_self_heal_event(
  *,
  workspace_root: Path,
  launch_id: str,
  requested_port: int,
  bound_port: int | None,
  session_attach_required: bool,
  reason_code: str,
  cleanup_actions: list[str] | None = None,
  host_pid: int | None = None,
  helper_pid: int | None = None,
  notes: str | None = None,
) -> None:
  _append_launcher_telemetry_event(
    workspace_root=workspace_root,
    launch_id=launch_id,
    event='self_heal_applied',
    state='SELF_HEAL',
    requested_port=requested_port,
    bound_port=bound_port,
    host_pid=host_pid,
    helper_pid=helper_pid,
    session_attach_required=session_attach_required,
    cleanup_actions=cleanup_actions,
    reason_code=reason_code,
    notes=notes,
  )


def _append_launcher_terminal_success_event(
  *,
  workspace_root: Path,
  launch_id: str,
  decision: str,
  state: str,
  requested_port: int,
  bound_port: int,
  session_attach_required: bool,
  session_attach_confirmed: bool | None,
  host_pid: int | None = None,
  helper_pid: int | None = None,
  cleanup_actions: list[str] | None = None,
  reason_code: str | None = None,
) -> None:
  _append_launcher_telemetry_event(
    workspace_root=workspace_root,
    launch_id=launch_id,
    event='launch_succeeded',
    state=state,
    requested_port=requested_port,
    bound_port=bound_port,
    host_pid=host_pid,
    helper_pid=helper_pid,
    session_attach_required=session_attach_required,
    session_attach_confirmed=session_attach_confirmed,
    cleanup_actions=cleanup_actions,
    decision=decision,
    reason_code=reason_code,
  )


def _normalize_reaped_console_hosts(result: Any) -> dict[str, list[int]]:
  if isinstance(result, dict):
    terminated_pids = [int(pid) for pid in result.get('terminated_pids', []) if int(pid or 0) > 0]
    host_pids = [int(pid) for pid in result.get('host_pids', []) if int(pid or 0) > 0]
    helper_pids = [int(pid) for pid in result.get('helper_pids', []) if int(pid or 0) > 0]
    return {
      'terminated_pids': terminated_pids,
      'host_pids': host_pids,
      'helper_pids': helper_pids,
    }
  terminated_pids = [int(pid) for pid in (result or []) if int(pid or 0) > 0]
  return {
    'terminated_pids': terminated_pids,
    'host_pids': list(terminated_pids),
    'helper_pids': [],
  }


def _launcher_success_decision(
  *,
  replacement_launch: bool,
  reaped_pid_count: int,
  browser_opened: bool,
  session_attach_required: bool,
  session_attach_confirmed: bool | None,
  helper_recovery: bool = False,
) -> str:
  if helper_recovery:
    return 'recovered_via_helper'
  if session_attach_required and session_attach_confirmed is False:
    return 'manual_attach_required'
  if not session_attach_required:
    return 'manual_attach_required'
  if replacement_launch and session_attach_confirmed:
    return 'replaced_existing_host'
  if replacement_launch or reaped_pid_count > 0:
    return 'replaced_existing_host'
  return 'launched_fresh_host'


def _acquire_console_launch_lock_details(*, workspace_root: Path) -> dict[str, Any]:
  lock_path = _console_launch_lock_path(workspace_root=workspace_root)
  payload = {
    'pid': os.getpid(),
    'workspace_root': str(workspace_root),
    'acquired_at_unix': time.time(),
  }
  reclaimed_stale_lock = False
  reclaimed_pid = 0
  reclaimed_reason = ''
  while True:
    try:
      with lock_path.open('x', encoding='utf-8') as handle:
        json.dump(payload, handle, indent=2)
        handle.write('\n')
      return {
        'path': lock_path,
        'reclaimed_stale_lock': reclaimed_stale_lock,
        'reclaimed_pid': reclaimed_pid,
        'reclaimed_reason': reclaimed_reason,
      }
    except FileExistsError:
      existing = _load_console_launch_lock(lock_path)
      existing_pid = int((existing or {}).get('pid', 0) or 0)
      acquired_at_unix = float((existing or {}).get('acquired_at_unix', 0.0) or 0.0)
      stale_reason = ''
      if existing is None:
        stale_reason = 'stale_lock_unreadable'
      elif not _process_is_alive(existing_pid):
        stale_reason = 'stale_lock_dead_owner'
      elif (time.time() - acquired_at_unix) > DETACHED_CONSOLE_LAUNCH_LOCK_TTL_SEC:
        stale_reason = 'stale_lock_expired'
      if stale_reason:
        reclaimed_stale_lock = True
        reclaimed_pid = existing_pid
        reclaimed_reason = stale_reason
        try:
          lock_path.unlink()
        except OSError:
          pass
        continue
      raise RuntimeError(
        'A detached Polyventure console launch is already in progress for this project. Wait for the earlier attempt to finish before retrying.'
      )


def _console_code_signature(*, workspace_root: Path) -> str:
  signature_parts: list[str] = []
  for relative_path in (
    Path('src/polyventure/cli.py'),
    Path('src/polyventure/web_app.py'),
  ):
    candidate = workspace_root / relative_path
    try:
      stat_result = candidate.stat()
    except OSError:
      signature_parts.append(f'{relative_path}:missing')
      continue
    signature_parts.append(
      f'{relative_path}:{int(stat_result.st_mtime_ns)}:{int(stat_result.st_size)}'
    )
  return '|'.join(signature_parts)


def _is_polyventure_project_root(path: Path) -> bool:
  return (path / 'pyproject.toml').exists() and (path / 'src' / 'polyventure').exists()


def _resolve_console_workspace_root(*, cwd: Path | None = None) -> Path:
  current = (cwd or Path.cwd()).resolve()
  for candidate in (current, *current.parents):
    if _is_polyventure_project_root(candidate):
      return candidate
    nested_project = candidate / 'polyventure'
    if _is_polyventure_project_root(nested_project):
      return nested_project.resolve()

  module_root = Path(__file__).resolve().parents[2]
  if _is_polyventure_project_root(module_root):
    return module_root
  return current


def _workspace_governed_python_candidates(*, workspace_root: Path) -> list[Path]:
  candidates: list[Path] = []
  for candidate_root in (workspace_root, *workspace_root.parents):
    venv_root = candidate_root / '.venv-core'
    if os.name == 'nt':
      candidates.extend(
        [
          venv_root / 'Scripts' / 'pythonw.exe',
          venv_root / 'Scripts' / 'python.exe',
        ]
      )
    else:
      candidates.append(venv_root / 'bin' / 'python')
  return candidates


def _detached_python_executable(*, workspace_root: Path) -> str:
  for candidate in _workspace_governed_python_candidates(workspace_root=workspace_root):
    if candidate.exists():
      return str(candidate)
  if os.name != 'nt':
    return sys.executable
  current = Path(sys.executable)
  pythonw = current.with_name('pythonw.exe')
  if pythonw.exists():
    return str(pythonw)
  return str(current)


def _open_console_browser(url: str) -> bool:
  if os.name == 'nt':
    startfile = getattr(os, 'startfile', None)
    if callable(startfile):
      try:
        startfile(url)
        return True
      except OSError:
        pass
  try:
    return bool(webbrowser.open(url, new=2))
  except webbrowser.Error:
    return False


def _load_console_registry() -> list[dict[str, Any]]:
  if not CONSOLE_HOST_REGISTRY.exists():
    return []
  try:
    payload = json.loads(CONSOLE_HOST_REGISTRY.read_text(encoding='utf-8'))
  except Exception:
    return []
  return payload if isinstance(payload, list) else []


def _console_launch_lock_path(*, workspace_root: Path) -> Path:
  digest = hashlib.sha256(str(workspace_root).encode('utf-8')).hexdigest()[:16]
  return Path(tempfile.gettempdir()) / f'polyventure_console_launch_{digest}.json'


def _load_console_launch_lock(lock_path: Path) -> dict[str, Any] | None:
  if not lock_path.exists():
    return None
  try:
    payload = json.loads(lock_path.read_text(encoding='utf-8'))
  except Exception:
    return None
  return payload if isinstance(payload, dict) else None


def _acquire_console_launch_lock(*, workspace_root: Path) -> Path:
  return Path(_acquire_console_launch_lock_details(workspace_root=workspace_root)['path'])


def _release_console_launch_lock(lock_path: Path | None) -> None:
  if lock_path is None:
    return
  try:
    lock_path.unlink()
  except OSError:
    return


def _save_console_registry(entries: list[dict[str, Any]]) -> None:
  CONSOLE_HOST_REGISTRY.write_text(json.dumps(entries, indent=2), encoding='utf-8')


def _process_is_alive(pid: int) -> bool:
  if pid <= 0:
    return False
  if os.name == 'nt':
    try:
      result = subprocess.run(
        ['tasklist', '/FI', f'PID eq {pid}', '/NH'],
        check=False,
        capture_output=True,
        text=True,
      )
    except OSError:
      return False
    line = (result.stdout or '').strip()
    if not line:
      return False
    if 'No tasks are running' in line:
      return False
    return str(pid) in line
  try:
    os.kill(pid, 0)
  except OSError:
    return False
  return True


def _terminate_process(pid: int) -> None:
  if pid <= 0:
    return
  if os.name == 'nt':
    subprocess.run(
      ['taskkill', '/PID', str(pid), '/T', '/F'],
      check=False,
      stdout=subprocess.DEVNULL,
      stderr=subprocess.DEVNULL,
    )
    return
  try:
    os.kill(pid, 15)
  except OSError:
    return


def _listener_pid_for_port(host: str, port: int) -> int | None:
  if os.name == 'nt':
    try:
      result = subprocess.run(
        ['netstat', '-ano', '-p', 'tcp'],
        check=False,
        capture_output=True,
        text=True,
      )
    except OSError:
      return None
    for line in result.stdout.splitlines():
      parts = line.split()
      if len(parts) < 5 or parts[0].upper() != 'TCP':
        continue
      local_address = parts[1]
      state = parts[3].upper()
      if state != 'LISTENING' or not local_address.endswith(f':{port}'):
        continue
      try:
        return int(parts[-1])
      except ValueError:
        continue
    return None

  try:
    result = subprocess.run(
      ['lsof', '-nP', f'-iTCP:{port}', '-sTCP:LISTEN', '-t'],
      check=False,
      capture_output=True,
      text=True,
    )
  except OSError:
    return None
  for line in result.stdout.splitlines():
    candidate = line.strip()
    if not candidate:
      continue
    try:
      return int(candidate)
    except ValueError:
      continue
  return None


def _listener_pid_for_port_with_retry(host: str, port: int, *, timeout_sec: float = 8.0) -> int | None:
  deadline = time.time() + max(timeout_sec, 0.1)
  while time.time() < deadline:
    pid = _listener_pid_for_port(host, port)
    if pid is not None:
      return pid
    time.sleep(0.1)
  return _listener_pid_for_port(host, port)


def _wait_for_port_availability(host: str, port: int, *, timeout_sec: float = 3.0) -> bool:
  deadline = time.time() + timeout_sec
  while time.time() < deadline:
    if _port_is_available(host, port):
      return True
    time.sleep(0.1)
  return _port_is_available(host, port)


def _probe_execution_status(host: str, port: int) -> dict[str, Any] | None:
  url = f'http://{host}:{port}/api/execution-status'
  try:
    request = urllib_request.Request(url, headers={'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
    with urllib_request.urlopen(request, timeout=2.0) as response:
      body = response.read().decode('utf-8', errors='ignore')
      return json.loads(body)
  except Exception:
    return None


def _port_serves_polyventure_console(host: str, port: int) -> bool:
  url = f'http://{host}:{port}/'
  try:
    request = urllib_request.Request(url, headers={'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
    with urllib_request.urlopen(request, timeout=1.5) as response:
      if int(getattr(response, 'status', 200)) >= 500:
        return False
      body = response.read().decode('utf-8', errors='ignore')
  except (UnicodeDecodeError, urllib_error.URLError, TimeoutError, OSError):
    return False
  return 'Polyventure Control Deck' in body


def _reap_preferred_console_host(*, host: str, port: int, explicit_port: bool) -> list[int]:
  if explicit_port or _port_is_available(host, port):
    return []
  if not _port_serves_polyventure_console(host, port):
    return []
  pid = _listener_pid_for_port(host, port)
  if pid is None:
    return []
  _terminate_process(pid)
  if _wait_for_port_availability(host, port):
    return [pid]
  return []


def _reap_tracked_console_hosts(*, workspace_root: Path) -> dict[str, list[int]]:
  survivors: list[dict[str, Any]] = []
  terminated: list[int] = []
  terminated_host_pids: list[int] = []
  terminated_helper_pids: list[int] = []
  terminated_listeners: list[tuple[str, int]] = []
  for entry in _load_console_registry():
    pid = int(entry.get('pid', 0) or 0)
    entry_workspace = Path(str(entry.get('workspace_root', workspace_root)))
    if not _process_is_alive(pid):
      continue
    if entry_workspace == workspace_root:
      if pid == os.getpid():
        survivors.append(entry)
        continue
      host = str(entry.get('host', '127.0.0.1') or '127.0.0.1')
      entry_role = str(entry.get('role', 'host') or 'host')
      try:
        port = int(entry.get('port', 0) or 0)
      except (TypeError, ValueError):
        port = 0
      _terminate_process(pid)
      terminated.append(pid)
      if entry_role == 'recovery-helper':
        terminated_helper_pids.append(pid)
      else:
        terminated_host_pids.append(pid)
      if port > 0:
        terminated_listeners.append((host, port))
      continue
    survivors.append(entry)
  _save_console_registry(survivors)
  for host, port in terminated_listeners:
    _wait_for_port_availability(host, port)
  return {
    'terminated_pids': terminated,
    'host_pids': terminated_host_pids,
    'helper_pids': terminated_helper_pids,
  }


def _record_console_host(*, pid: int, workspace_root: Path, host: str, port: int, role: str = 'host') -> None:
  entries: list[dict[str, Any]] = []
  for entry in _load_console_registry():
    entry_pid = int(entry.get('pid', 0) or 0)
    if not _process_is_alive(entry_pid):
      continue
    entry_workspace = Path(str(entry.get('workspace_root', workspace_root)))
    entry_host = str(entry.get('host', host) or host)
    try:
      entry_port = int(entry.get('port', 0) or 0)
    except (TypeError, ValueError):
      entry_port = 0
    entry_role = str(entry.get('role', 'host') or 'host')
    if entry_pid == pid:
      continue
    if entry_workspace == workspace_root and entry_host == host and entry_port == port and entry_role == role:
      continue
    entries.append(entry)
  entries.append(
    {
      'pid': pid,
      'workspace_root': str(workspace_root),
      'host': host,
      'port': port,
      'role': role,
      'code_signature': _console_code_signature(workspace_root=workspace_root) if role == 'host' else None,
      'recorded_at_unix': time.time(),
    }
  )
  _save_console_registry(entries)


def _remove_console_registry_entries(*, pids: set[int]) -> None:
  if not pids:
    return
  survivors: list[dict[str, Any]] = []
  for entry in _load_console_registry():
    entry_pid = int(entry.get('pid', 0) or 0)
    if entry_pid in pids or not _process_is_alive(entry_pid):
      continue
    survivors.append(entry)
  _save_console_registry(survivors)


def _find_console_registry_entry(
  *,
  workspace_root: Path,
  host: str,
  port: int,
  role: str = 'host',
  require_live_pid: bool = False,
) -> dict[str, Any] | None:
  for entry in _load_console_registry():
    entry_pid = int(entry.get('pid', 0) or 0)
    entry_workspace = Path(str(entry.get('workspace_root', workspace_root)))
    entry_host = str(entry.get('host', host) or host)
    try:
      entry_port = int(entry.get('port', 0) or 0)
    except (TypeError, ValueError):
      entry_port = 0
    entry_role = str(entry.get('role', role) or role)
    if entry_workspace != workspace_root or entry_host != host or entry_port != port or entry_role != role:
      continue
    if require_live_pid and not _process_is_alive(entry_pid):
      continue
    return entry
  return None


def _console_registry_entry_is_fresh(recorded_at_unix: Any, *, now_unix: float | None = None) -> bool:
  try:
    recorded_value = float(recorded_at_unix)
  except (TypeError, ValueError):
    return False
  if recorded_value <= 0.0:
    return False
  current_unix = time.time() if now_unix is None else float(now_unix)
  if recorded_value > current_unix + 60.0:
    return False
  return (current_unix - recorded_value) <= DETACHED_CONSOLE_REGISTRY_FRESHNESS_SEC


def _collect_console_reuse_health_basis(
  *,
  workspace_root: Path,
  host: str,
  port: int,
  current_code_signature: str,
  session_status_ok: bool,
) -> dict[str, Any]:
  registry_entry = _find_console_registry_entry(workspace_root=workspace_root, host=host, port=port)
  registry_pid = int((registry_entry or {}).get('pid', 0) or 0)
  registry_pid_alive = registry_pid > 0 and _process_is_alive(registry_pid)
  listener_pid = _listener_pid_for_port_with_retry(host, port, timeout_sec=0.5)
  listener_pid_matches_registry_pid = bool(registry_pid_alive and listener_pid is not None and listener_pid == registry_pid)
  root_probe_ok = _port_serves_polyventure_console(host, port)
  recorded_at_unix = (registry_entry or {}).get('recorded_at_unix')
  registry_fresh = _console_registry_entry_is_fresh(recorded_at_unix)
  active_host_signature = str((registry_entry or {}).get('code_signature', '') or '').strip()
  code_signature_match = bool(registry_pid_alive and active_host_signature and active_host_signature == current_code_signature)
  prune_registry_pid = 0
  prune_reason_code: str | None = None
  if registry_entry and not registry_pid_alive:
    prune_registry_pid = registry_pid
    prune_reason_code = 'stale_registry_pid_dead'
  elif registry_entry and registry_pid_alive and listener_pid is not None and listener_pid != registry_pid:
    prune_registry_pid = registry_pid
    prune_reason_code = 'stale_registry_listener_pid_mismatch'
  reusable = bool(
    session_status_ok
    and root_probe_ok
    and registry_pid_alive
    and listener_pid_matches_registry_pid
    and code_signature_match
    and registry_fresh
  )
  return {
    'registry_entry_present': bool(registry_entry),
    'registry_pid': registry_pid or None,
    'registry_pid_alive': registry_pid_alive,
    'listener_pid': listener_pid,
    'listener_pid_matches_registry_pid': listener_pid_matches_registry_pid,
    'root_probe_ok': root_probe_ok,
    'session_status_ok': session_status_ok,
    'code_signature_match': code_signature_match,
    'registry_fresh': registry_fresh,
    'reusable': reusable,
    'prune_registry_pid': prune_registry_pid or None,
    'prune_reason_code': prune_reason_code,
  }


def _console_reuse_health_basis_notes(basis: dict[str, Any]) -> str:
  return ';'.join(
    [
      f"registry_entry_present={'true' if bool(basis.get('registry_entry_present')) else 'false'}",
      f"registry_pid_alive={'true' if bool(basis.get('registry_pid_alive')) else 'false'}",
      f"listener_pid_matches_registry_pid={'true' if bool(basis.get('listener_pid_matches_registry_pid')) else 'false'}",
      f"root_probe_ok={'true' if bool(basis.get('root_probe_ok')) else 'false'}",
      f"session_status_ok={'true' if bool(basis.get('session_status_ok')) else 'false'}",
      f"code_signature_match={'true' if bool(basis.get('code_signature_match')) else 'false'}",
      f"registry_fresh={'true' if bool(basis.get('registry_fresh')) else 'false'}",
    ]
  )


def _console_reuse_reason_code(*, replacement_launch: bool, basis: dict[str, Any]) -> str:
  if bool(basis.get('reusable')):
    return 'healthy_match'
  if bool(basis.get('prune_reason_code')):
    return str(basis.get('prune_reason_code'))
  if bool(basis.get('registry_entry_present')) and not bool(basis.get('registry_fresh')):
    return 'stale_registry_entry_expired'
  if bool(basis.get('registry_entry_present')) and not bool(basis.get('code_signature_match')):
    return 'code_signature_mismatch'
  if bool(basis.get('registry_entry_present')) and not bool(basis.get('root_probe_ok')):
    return 'root_probe_unreachable'
  if replacement_launch and not bool(basis.get('listener_pid_matches_registry_pid')):
    return 'reuse_listener_verification_failed'
  if replacement_launch and not bool(basis.get('session_status_ok')):
    return 'session_status_unreachable'
  if replacement_launch:
    return 'needs_replace'
  if bool(basis.get('registry_entry_present')) or bool(basis.get('root_probe_ok')):
    return 'needs_replace'
  return 'no_reusable_host'


def _cleanup_failed_detached_console_launch(
  *,
  host: str,
  port: int,
  host_pids: list[int],
  helper_host: str,
  helper_port: int,
  helper_pids: list[int],
) -> None:
  cleanup_pids = {pid for pid in [*host_pids, *helper_pids] if pid > 0}
  for pid in cleanup_pids:
    _terminate_process(pid)
  _remove_console_registry_entries(pids=cleanup_pids)
  if port > 0:
    _wait_for_port_availability(host, port)
  if helper_host and helper_port > 0:
    _wait_for_port_availability(helper_host, helper_port)


def _emit_launcher_cleanup_events(
  *,
  workspace_root: Path,
  launch_id: str,
  requested_port: int,
  bound_port: int,
  host_pid: int | None,
  helper_pid: int | None,
  session_attach_required: bool,
  cleanup_actions: list[str],
  reason_code: str,
  notes: str | None = None,
) -> None:
  _append_launcher_telemetry_event(
    workspace_root=workspace_root,
    launch_id=launch_id,
    event='cleanup_started',
    state='CLEANUP',
    requested_port=requested_port,
    bound_port=bound_port,
    host_pid=host_pid,
    helper_pid=helper_pid,
    session_attach_required=session_attach_required,
    cleanup_actions=cleanup_actions,
    reason_code=reason_code,
    notes=notes,
  )
  _append_launcher_telemetry_event(
    workspace_root=workspace_root,
    launch_id=launch_id,
    event='cleanup_complete',
    state='CLEANUP',
    requested_port=requested_port,
    bound_port=bound_port,
    host_pid=host_pid,
    helper_pid=helper_pid,
    session_attach_required=session_attach_required,
    cleanup_actions=cleanup_actions,
    reason_code='cleanup_complete',
    notes=notes,
  )


def _active_console_host_signature(*, workspace_root: Path, host: str, port: int) -> str | None:
  entry = _find_console_registry_entry(
    workspace_root=workspace_root,
    host=host,
    port=port,
    role='host',
    require_live_pid=True,
  )
  if entry is None:
    return None
  signature = str(entry.get('code_signature', '') or '').strip()
  if signature:
    return signature or None
  return None


def _port_is_available(host: str, port: int) -> bool:
  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
    probe.settimeout(0.2)
    if probe.connect_ex((host, port)) == 0:
      return False

  with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
    if os.name != 'nt':
      probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
      probe.bind((host, port))
    except OSError:
      return False
  return True


def _select_console_port(*, host: str, preferred_port: int, explicit_port: bool) -> int:
  if _port_is_available(host, preferred_port):
    return preferred_port
  if explicit_port:
    raise RuntimeError(f'Port {preferred_port} is already in use. Close the existing host or omit --port to auto-pick a free port.')
  for candidate in range(preferred_port + 1, preferred_port + 25):
    if _port_is_available(host, candidate):
      return candidate
  raise RuntimeError(f'No free Polyventure console port was found near {preferred_port}.')


def _wait_for_console_ready(
  url: str,
  *,
  expected_session_token: str | None = None,
  timeout_sec: float = 8.0,
  process: subprocess.Popen[Any] | None = None,
) -> bool:
  deadline = time.time() + timeout_sec
  while time.time() < deadline:
    # If we are waiting on a specific child process and it has already exited,
    # there is nothing left to become ready -- stop waiting immediately rather
    # than burning the full timeout on a dead process.
    if process is not None and process.poll() is not None:
      return False
    try:
      request = urllib_request.Request(url, headers={'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
      with urllib_request.urlopen(request, timeout=1.5) as response:
        if int(getattr(response, 'status', 200)) < 500:
          if expected_session_token is None:
            return True
          body = response.read().decode('utf-8', errors='ignore')
          if expected_session_token in body:
            return True
    except UnicodeDecodeError:
      pass
    except (urllib_error.URLError, TimeoutError, OSError):
      pass
    time.sleep(0.2)
  return False


def _console_session_status_url(
  host: str,
  port: int,
  *,
  expected_session_token: str | None = None,
  expected_launch_id: str | None = None,
) -> str:
  url = f'http://{host}:{port}/api/session-status'
  query_parts: list[str] = []
  if expected_session_token:
    query_parts.append(f'session={expected_session_token}')
  if expected_launch_id:
    query_parts.append(f'launch={expected_launch_id}')
  if query_parts:
    return f"{url}?{'&'.join(query_parts)}"
  return url


def _console_browser_session_active(
  host: str,
  port: int,
  *,
  expected_session_token: str | None = None,
  expected_launch_id: str | None = None,
) -> bool:
  url = _console_session_status_url(
    host,
    port,
    expected_session_token=expected_session_token,
    expected_launch_id=expected_launch_id,
  )
  try:
    request = urllib_request.Request(url, headers={'Cache-Control': 'no-cache', 'Pragma': 'no-cache'})
    with urllib_request.urlopen(request, timeout=1.5) as response:
      if int(getattr(response, 'status', 200)) >= 500:
        return False
      payload = json.loads(response.read().decode('utf-8', errors='ignore'))
  except (json.JSONDecodeError, UnicodeDecodeError, urllib_error.URLError, TimeoutError, OSError, ValueError):
    return False
  handoff_payload = payload.get('handoff') if isinstance(payload, dict) else {}
  if isinstance(handoff_payload, dict):
    published_identity = handoff_payload.get('published_identity') if isinstance(handoff_payload.get('published_identity'), dict) else {}
    published_launch_id = str((published_identity or {}).get('launch_id', '') or '')
    if expected_launch_id and published_launch_id and published_launch_id != expected_launch_id:
      return False
    if bool(handoff_payload.get('attach_confirmed')):
      return True
  session_payload = payload.get('session') if isinstance(payload, dict) else {}
  return bool((session_payload or {}).get('active'))


def _wait_for_console_browser_session_attach(
  host: str,
  port: int,
  *,
  expected_session_token: str | None = None,
  expected_launch_id: str | None = None,
  timeout_sec: float = DETACHED_CONSOLE_REATTACH_WAIT_SEC,
) -> bool:
  deadline = time.time() + max(timeout_sec, 0.1)
  while time.time() < deadline:
    if _console_browser_session_active(
      host,
      port,
      expected_session_token=expected_session_token,
      expected_launch_id=expected_launch_id,
    ):
      return True
    time.sleep(0.2)
  return _console_browser_session_active(
    host,
    port,
    expected_session_token=expected_session_token,
    expected_launch_id=expected_launch_id,
  )


def _collect_console_health_basis(
  *,
  host: str,
  port: int,
  probe_url: str,
  expected_session_token: str,
  expected_launch_id: str | None,
  ready_timeout_sec: float,
  session_status_timeout_sec: float = 1.5,
) -> dict[str, Any]:
  ready_probe_ok = _wait_for_console_ready(
    probe_url,
    expected_session_token=expected_session_token,
    timeout_sec=ready_timeout_sec,
  )
  session_status_ok = _wait_for_console_ready(
    _console_session_status_url(
      host,
      port,
      expected_session_token=expected_session_token,
      expected_launch_id=expected_launch_id,
    ),
    timeout_sec=max(0.5, session_status_timeout_sec),
  )
  return {
    'ready_probe_ok': ready_probe_ok,
    'session_status_ok': session_status_ok,
  }


def _console_health_basis_notes(basis: dict[str, Any]) -> str:
  return ';'.join(
    [
      f"ready_probe_ok={'true' if bool(basis.get('ready_probe_ok')) else 'false'}",
      f"session_status_ok={'true' if bool(basis.get('session_status_ok')) else 'false'}",
    ]
  )


def _console_ready_reason_code(basis: dict[str, Any]) -> str:
  if bool(basis.get('ready_probe_ok')) and bool(basis.get('session_status_ok')):
    return 'ready_probe_plus_session_status'
  if bool(basis.get('ready_probe_ok')):
    return 'session_status_unreachable'
  return 'ready_probe_unreachable'


def _console_retry_precondition_ready(basis: dict[str, Any]) -> bool:
  return bool(basis.get('ready_probe_ok')) or bool(basis.get('session_status_ok'))


def _host_pid_alive_note_value(pid: int | None) -> str:
  if pid is None:
    return 'unknown'
  return 'true' if _process_is_alive(pid) else 'false'


def _append_note_suffix(notes: str | None, suffix: str) -> str:
  base = str(notes or '').strip()
  return f'{base};{suffix}' if base else suffix


def _manual_attach_deadline_label(timeout_sec: float) -> str:
  if float(timeout_sec).is_integer():
    return f'{int(timeout_sec)} seconds'
  return f'{timeout_sec:.1f} seconds'


def _collect_post_attach_timeout_retention_basis(
  *,
  host: str,
  port: int,
  probe_url: str,
  expected_session_token: str,
  expected_launch_id: str | None,
) -> dict[str, Any]:
  return _collect_console_health_basis(
    host=host,
    port=port,
    probe_url=probe_url,
    expected_session_token=expected_session_token,
    expected_launch_id=expected_launch_id,
    ready_timeout_sec=DETACHED_CONSOLE_RETRY_PRECONDITION_WAIT_SEC,
    session_status_timeout_sec=1.0,
  )


def _launch_lock_owned_by_current_process(lock_path: Path | None) -> bool:
  if lock_path is None or not lock_path.exists():
    return False
  payload = _load_console_launch_lock(lock_path)
  return int((payload or {}).get('pid', 0) or 0) == os.getpid()


def _perform_bounded_same_host_attach_retry(
  *,
  workspace_root: Path,
  launch_id: str,
  lock_path: Path | None,
  host: str,
  port: int,
  requested_port: int,
  browser_url: str,
  probe_url: str,
  session_token: str,
  tracked_pid: int | None,
  helper_pid: int | None,
) -> dict[str, Any]:
  if not _launch_lock_owned_by_current_process(lock_path):
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='attach_retry_result',
      state='CONFIRM_ATTACH',
      requested_port=requested_port,
      bound_port=port,
      host_pid=tracked_pid,
      helper_pid=helper_pid,
      session_attach_required=True,
      session_attach_confirmed=False,
      reason_code='attach_retry_skipped_precondition',
      notes='launch_lock_not_owned',
    )
    return {
      'status': 'skipped',
      'session_attach_confirmed': False,
      'browser_opened': False,
    }

  retry_basis = _collect_console_health_basis(
    host=host,
    port=port,
    probe_url=probe_url,
    expected_session_token=session_token,
    expected_launch_id=launch_id,
    ready_timeout_sec=DETACHED_CONSOLE_RETRY_PRECONDITION_WAIT_SEC,
    session_status_timeout_sec=1.0,
  )
  if not _console_retry_precondition_ready(retry_basis):
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='attach_retry_result',
      state='CONFIRM_ATTACH',
      requested_port=requested_port,
      bound_port=port,
      host_pid=tracked_pid,
      helper_pid=helper_pid,
      session_attach_required=True,
      session_attach_confirmed=False,
      reason_code='attach_retry_skipped_precondition',
      notes=_append_note_suffix(
        'host_not_ready_for_retry;' + _console_health_basis_notes(retry_basis),
        f'host_pid_alive={_host_pid_alive_note_value(tracked_pid)}',
      ),
    )
    return {
      'status': 'skipped',
      'session_attach_confirmed': False,
      'browser_opened': False,
    }

  _append_launcher_telemetry_event(
    workspace_root=workspace_root,
    launch_id=launch_id,
    event='attach_retry_started',
    state='CONFIRM_ATTACH',
    requested_port=requested_port,
    bound_port=port,
    host_pid=tracked_pid,
    helper_pid=helper_pid,
    session_attach_required=True,
    reason_code='attach_retry_after_browser_open',
    notes=_console_health_basis_notes(retry_basis),
  )

  retry_browser_opened = _open_console_browser(browser_url)
  retry_attached = retry_browser_opened and _wait_for_console_browser_session_attach(
    host,
    port,
    expected_session_token=session_token,
    expected_launch_id=launch_id,
    timeout_sec=DETACHED_CONSOLE_ATTACH_RETRY_WAIT_SEC,
  )
  retry_reason = 'attach_retry_confirmed' if retry_attached else 'attach_retry_exhausted'
  retry_notes = None if retry_browser_opened else 'browser_open_failed_on_retry'
  _append_launcher_telemetry_event(
    workspace_root=workspace_root,
    launch_id=launch_id,
    event='attach_retry_result',
    state='CONFIRM_ATTACH',
    requested_port=requested_port,
    bound_port=port,
    host_pid=tracked_pid,
    helper_pid=helper_pid,
    session_attach_required=True,
    session_attach_confirmed=retry_attached,
    reason_code=retry_reason,
    notes=retry_notes,
  )
  if retry_attached:
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='attach_confirmed',
      state='CONFIRM_ATTACH',
      requested_port=requested_port,
      bound_port=port,
      host_pid=tracked_pid,
      helper_pid=helper_pid,
      session_attach_required=True,
      session_attach_confirmed=True,
      reason_code='attach_retry_confirmed',
    )
  return {
    'status': 'confirmed' if retry_attached else 'exhausted',
    'session_attach_confirmed': retry_attached,
    'browser_opened': retry_browser_opened,
  }


def _has_explicit_port_argument(raw_argv: list[str]) -> bool:
  for argument in raw_argv:
    if argument == '--port' or argument.startswith('--port='):
      return True
  return False


def _detached_console_env(*, workspace_root: Path) -> dict[str, str]:
  env = dict(os.environ)
  env['KALSHI_FORCE_LOCAL_DOTENV'] = 'true'
  local_runtime_authority: dict[str, str] = {}
  local_dotenv_path = workspace_root / '.env'
  if local_dotenv_path.exists():
    try:
      local_runtime_authority = _read_dotenv_file(local_dotenv_path)
    except OSError:
      local_runtime_authority = {}
  for name in (
    'KALSHI_STATE_DB_PATH',
    'KALSHI_WEBSOCKET_URL',
    'KALSHI_SANDBOX_WEBSOCKET_URL',
    'KALSHI_LIVE_WEBSOCKET_URL',
  ):
    local_value = str(local_runtime_authority.get(name) or '').strip()
    if local_value:
      env[name] = local_value
    else:
      env.pop(name, None)
  src_path = workspace_root / 'src'
  if src_path.exists():
    existing = env.get('PYTHONPATH', '')
    env['PYTHONPATH'] = str(src_path) if not existing else f"{src_path}{os.pathsep}{existing}"
  return env


def _detached_console_command(
  *,
  workspace_root: Path,
  host: str,
  port: int,
  session_token: str,
  handoff_context: dict[str, Any] | None = None,
  recovery_helper: dict[str, Any] | None = None,
) -> list[str]:
  package_root = workspace_root / 'src' / 'polyventure'
  command = [
    _detached_python_executable(workspace_root=workspace_root),
    '-m',
    'polyventure.cli',
    'console',
    '--host',
    host,
    '--port',
    str(port),
    '--console-host-process',
    '--session-token',
    session_token,
    '--startup-grace-sec',
    str(DETACHED_CONSOLE_STARTUP_GRACE_SEC),
    '--idle-timeout-sec',
    str(DETACHED_CONSOLE_IDLE_TIMEOUT_SEC),
  ]
  if handoff_context:
    command.extend(
      [
        '--handoff-context-json',
        json.dumps(handoff_context, separators=(',', ':')),
      ]
    )
  if recovery_helper:
    command.extend(
      [
        '--recovery-helper-url',
        str(recovery_helper.get('url', '')),
        '--recovery-helper-token',
        str(recovery_helper.get('token', '')),
        '--recovery-helper-expiry-unix',
        str(recovery_helper.get('expires_at_unix', 0.0)),
      ]
    )
  if package_root.exists():
    return command
  return command


def _detached_recovery_helper_command(
  *,
  workspace_root: Path,
  helper_host: str,
  helper_port: int,
  helper_token: str,
  target_host: str,
  target_port: int,
  lifetime_sec: float,
) -> list[str]:
  return [
    _detached_python_executable(workspace_root=workspace_root),
    '-m',
    'polyventure.cli',
    'console',
    '--host',
    helper_host,
    '--port',
    str(helper_port),
    '--console-recovery-helper-process',
    '--recovery-helper-token',
    helper_token,
    '--recovery-target-host',
    target_host,
    '--recovery-target-port',
    str(target_port),
    '--recovery-helper-lifetime-sec',
    str(lifetime_sec),
  ]


def _helper_json_response(
  start_response: Any,
  payload: dict[str, Any],
  *,
  status: str = '200 OK',
) -> list[bytes]:
  body = json.dumps(payload, indent=2, default=str).encode('utf-8')
  start_response(
    status,
    [
      ('Content-Type', 'application/json; charset=utf-8'),
      ('Content-Length', str(len(body))),
      ('Cache-Control', 'no-store'),
      ('Access-Control-Allow-Origin', '*'),
    ],
  )
  return [body]


def _create_console_recovery_helper_app(
  *,
  helper_token: str,
  helper_expires_at_unix: float,
  target_host: str,
  target_port: int,
) -> Any:
  def app(environ: dict[str, Any], start_response: Any) -> list[bytes]:
    method = str(environ.get('REQUEST_METHOD', 'GET')).upper()
    path = str(environ.get('PATH_INFO', '/'))
    query = parse_qs(str(environ.get('QUERY_STRING', '')))

    if method == 'GET' and path == '/health':
      return _helper_json_response(
        start_response,
        {
          'decision': 'planned',
          'helper': 'ready',
          'expires_at_unix': helper_expires_at_unix,
          'target_url': f'http://{target_host}:{target_port}/',
        },
      )

    if method == 'GET' and path == '/recover':
      provided_token = str(query.get('token', [''])[0] or '')
      if provided_token != helper_token:
        return _helper_json_response(
          start_response,
          {
            'decision': 'launch_blocked',
            'reason': 'recovery_helper_token_mismatch',
            'message': 'The local recovery helper rejected the current browser token.',
            'next_action': 'Relaunch the console from the current project to start a fresh host.',
          },
          status='403 Forbidden',
        )
      if time.time() >= helper_expires_at_unix:
        return _helper_json_response(
          start_response,
          {
            'decision': 'launch_blocked',
            'reason': 'recovery_helper_expired',
            'message': 'The local recovery helper expired before recovery was requested.',
            'next_action': 'Relaunch the console from the current project to start a fresh host.',
          },
          status='410 Gone',
        )
      recovery_mode = str(query.get('mode', ['manual'])[0] or 'manual')
      if _port_serves_polyventure_console(target_host, target_port):
        return _helper_json_response(
          start_response,
          {
            'decision': 'reused_existing_host',
            'recovery': 'already-healthy',
            'mode': recovery_mode,
            'url': f'http://{target_host}:{target_port}/',
            'bound_port': target_port,
            'expires_at_unix': helper_expires_at_unix,
            'next_action': 'Reuse the current local shell URL; helper relaunch was skipped because the target host is already healthy.',
          },
        )
      try:
        payload = launch_detached_operator_console(
          host=target_host,
          port=target_port,
          open_browser=False,
          explicit_port=False,
          helper_recovery=True,
        )
      except Exception as exc:
        return _helper_json_response(
          start_response,
          {
            'decision': 'launch_blocked',
            'reason': 'helper_recovery_failed',
            'message': str(exc),
            'next_action': 'Relaunch the console from the current project to start a fresh host.',
          },
        )
      return _helper_json_response(
        start_response,
        {
          'decision': str(payload.get('decision') or 'recovered_via_helper'),
          'recovery': 'launched',
          'mode': recovery_mode,
          'url': payload.get('url'),
          'bound_port': payload.get('bound_port'),
          'launch_id': payload.get('launch_id'),
          'expires_at_unix': helper_expires_at_unix,
          'next_action': 'Redirect the browser to the fresh local shell URL.',
        },
      )

    return _helper_json_response(
      start_response,
      {
        'decision': 'no-go',
        'reason': 'recovery_helper_route_not_found',
        'message': f'The recovery helper does not expose {path}.',
        'next_action': 'Use the documented recovery helper routes only.',
      },
      status='404 Not Found',
    )

  return app


def run_console_recovery_helper_server(
  *,
  host: str,
  port: int,
  helper_token: str,
  target_host: str,
  target_port: int,
  lifetime_sec: float = DETACHED_CONSOLE_RECOVERY_HELPER_LIFETIME_SEC,
) -> None:
  expires_at_unix = time.time() + max(lifetime_sec, 5.0)
  app = _create_console_recovery_helper_app(
    helper_token=helper_token,
    helper_expires_at_unix=expires_at_unix,
    target_host=target_host,
    target_port=target_port,
  )
  with make_server(host, port, app) as server:
    stop_event = threading.Event()

    def _monitor_lifetime() -> None:
      while not stop_event.wait(1.0):
        if time.time() >= expires_at_unix:
          threading.Thread(target=server.shutdown, daemon=True).start()
          return

    threading.Thread(target=_monitor_lifetime, daemon=True).start()
    try:
      server.serve_forever()
    finally:
      stop_event.set()


def _launch_console_recovery_helper(
  *,
  workspace_root: Path,
  helper_host: str,
  preferred_port: int,
  target_host: str,
  target_port: int,
) -> dict[str, Any] | None:
  helper_port = _select_console_port(host=helper_host, preferred_port=preferred_port, explicit_port=False)
  helper_token = uuid.uuid4().hex
  helper_expires_at_unix = time.time() + DETACHED_CONSOLE_RECOVERY_HELPER_LIFETIME_SEC
  command = _detached_recovery_helper_command(
    workspace_root=workspace_root,
    helper_host=helper_host,
    helper_port=helper_port,
    helper_token=helper_token,
    target_host=target_host,
    target_port=target_port,
    lifetime_sec=DETACHED_CONSOLE_RECOVERY_HELPER_LIFETIME_SEC,
  )
  popen_kwargs: dict[str, Any] = {
    'stdin': subprocess.DEVNULL,
    'stdout': subprocess.DEVNULL,
    'stderr': subprocess.DEVNULL,
    'close_fds': True,
    'cwd': str(workspace_root),
    'env': _detached_console_env(workspace_root=workspace_root),
  }
  process: subprocess.Popen[Any]
  if os.name == 'nt':
    creationflags = (
      int(getattr(subprocess, 'DETACHED_PROCESS', 0))
      | int(getattr(subprocess, 'CREATE_NEW_PROCESS_GROUP', 0))
      | int(getattr(subprocess, 'CREATE_NO_WINDOW', 0))
    )
    process = subprocess.Popen(command, creationflags=creationflags, **popen_kwargs)
  else:
    process = subprocess.Popen(command, start_new_session=True, **popen_kwargs)
  helper_url = f'http://{helper_host}:{helper_port}'
  if not _wait_for_console_ready(
    f'{helper_url}/health',
    timeout_sec=DETACHED_CONSOLE_HELPER_READY_WAIT_SEC,
    process=process,
  ):
    # Phase-1 attribution: distinguish a crash (process already exited) from a
    # slow-but-alive cold start that simply exceeded the health budget.
    _helper_exit_code = process.poll()
    _helper_failure_cause = (
      f'helper_process_exited_before_ready(code={_helper_exit_code})'
      if _helper_exit_code is not None
      else 'helper_health_timeout_process_alive'
    )
    _terminate_process(int(process.pid))
    _helper_exc = RuntimeError(
      f'The detached recovery helper did not become ready before launch ({_helper_failure_cause}).'
    )
    setattr(_helper_exc, 'helper_failure_cause', _helper_failure_cause)
    raise _helper_exc
  return {
    'url': helper_url,
    'host': helper_host,
    'port': helper_port,
    'pid': int(process.pid),
    'token': helper_token,
    'expires_at_unix': helper_expires_at_unix,
  }


def _popup_mode_active(args: Any) -> bool:
  # Splash/popup is default-on for interactive console launches. Opt out with
  # POLYVENTURE_POPUP=0 (e.g. headless/scripted use); machine-readable (--json)
  # runs never show a popup.
  return os.environ.get('POLYVENTURE_POPUP', '1') != '0' and (not getattr(args, 'json', False))


def launch_detached_operator_console(
  *,
  host: str,
  port: int,
  open_browser: bool = True,
  explicit_port: bool = False,
  helper_recovery: bool = False,
  pre_browser_hook: Callable[[], None] | None = None,
) -> dict[str, Any]:
  workspace_root = _resolve_console_workspace_root()
  launch_id = uuid.uuid4().hex
  helper_pid = 0
  helper_host = ''
  helper_port = 0
  helper_listener_pid = 0
  host_pid = 0
  bound_port = port
  launch_lock_path: Path | None = None
  launch_block_reason = 'console_failed'
  launch_block_next_action = 'Fix the local shell startup issue and retry the console command.'
  try:
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='discover_started',
      state='DISCOVER_EXISTING',
      requested_port=port,
      session_attach_required=open_browser,
    )
    replacement_launch = _console_browser_session_active(host, port)
    current_code_signature = _console_code_signature(workspace_root=workspace_root)
    reuse_basis = _collect_console_reuse_health_basis(
      workspace_root=workspace_root,
      host=host,
      port=port,
      current_code_signature=current_code_signature,
      session_status_ok=replacement_launch,
    )
    existing_host_healthy = bool(reuse_basis.get('root_probe_ok'))
    code_signature_match = bool(reuse_basis.get('code_signature_match'))
    discover_reason = _console_reuse_reason_code(replacement_launch=replacement_launch, basis=reuse_basis)
    if reuse_basis.get('prune_registry_pid'):
      stale_registry_pid = int(reuse_basis.get('prune_registry_pid', 0) or 0)
      _remove_console_registry_entries(pids={stale_registry_pid})
      _append_launcher_self_heal_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        requested_port=port,
        bound_port=port,
        session_attach_required=open_browser,
        host_pid=stale_registry_pid or None,
        cleanup_actions=['remove_registry_entries'],
        reason_code=str(reuse_basis.get('prune_reason_code') or 'stale_registry_pruned'),
        notes=_console_reuse_health_basis_notes(reuse_basis),
      )
      _append_launcher_telemetry_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        event='stale_registry_pruned',
        state='DISCOVER_EXISTING',
        requested_port=port,
        bound_port=port,
        host_pid=stale_registry_pid or None,
        session_attach_required=open_browser,
        cleanup_actions=['remove_registry_entries'],
        reason_code=str(reuse_basis.get('prune_reason_code') or 'stale_registry_pruned'),
        notes=_console_reuse_health_basis_notes(reuse_basis),
      )
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='discover_completed',
      state='DISCOVER_EXISTING',
      requested_port=port,
      bound_port=port,
      code_signature_match=code_signature_match if bool(reuse_basis.get('registry_entry_present')) else None,
      session_attach_required=open_browser,
      reason_code=discover_reason,
      notes=_console_reuse_health_basis_notes(reuse_basis),
    )
    if bool(reuse_basis.get('reusable')):
      reattach_url = f'http://{host}:{port}/'
      exec_status = _probe_execution_status(host, port)
      exec_in_flight = int((exec_status or {}).get('in_flight_count') or 0)
      if exec_status is None:
        blocked_reason = 'execution_status_probe_failed'
        blocked_message = 'The running console did not respond to the execution-status probe. Reattach manually to inspect its state.'
      elif exec_in_flight > 0:
        blocked_reason = 'execution_in_progress'
        blocked_message = f'Execution is in progress ({exec_in_flight} pair(s) in-flight). Reattach to the running console instead of launching a new instance.'
      else:
        blocked_reason = 'instance_already_running'
        blocked_message = 'A console instance is already running. Open the existing console instead of launching a new one.'
      _append_launcher_telemetry_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        event='launch_blocked',
        state='FAIL_CLOSED',
        requested_port=port,
        bound_port=port,
        host_pid=int(reuse_basis.get('listener_pid', 0) or 0) or None,
        session_attach_required=False,
        session_attach_confirmed=False,
        decision='no-go',
        reason_code=blocked_reason,
      )
      return {
        'decision': 'no-go',
        'command_family': 'polyventure console',
        'reason': blocked_reason,
        'message': blocked_message,
        'in_flight_count': exec_in_flight,
        'active_pairs': list((exec_status or {}).get('active_pairs') or []),
        'reattach_url': reattach_url,
        'next_action': (
          f'Navigate to {reattach_url} in your existing browser tab — paste the URL in the address bar; do not open a new window or tab.'
          if blocked_reason in {'execution_in_progress', 'instance_already_running'}
          else f'Reattach manually to the running console at {reattach_url}.'
        ),
      }
    lock_details = _acquire_console_launch_lock_details(workspace_root=workspace_root)
    launch_lock_path = Path(lock_details['path'])
    if bool(lock_details.get('reclaimed_stale_lock')):
      reclaimed_reason = str(lock_details.get('reclaimed_reason', '') or 'stale_lock_reclaimed')
      reclaimed_pid = int(lock_details.get('reclaimed_pid', 0) or 0)
      _append_launcher_self_heal_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        requested_port=port,
        bound_port=port,
        session_attach_required=open_browser,
        host_pid=reclaimed_pid or None,
        cleanup_actions=['remove_stale_launch_lock'],
        reason_code=reclaimed_reason,
        notes='launch_lock_reclaimed_before_reap',
      )
      _append_launcher_telemetry_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        event='stale_lock_reclaimed',
        state='SELF_HEAL',
        requested_port=port,
        bound_port=port,
        host_pid=reclaimed_pid or None,
        session_attach_required=open_browser,
        cleanup_actions=['remove_stale_launch_lock'],
        reason_code=reclaimed_reason,
      )
    reaped_result = _normalize_reaped_console_hosts(_reap_tracked_console_hosts(workspace_root=workspace_root))
    reaped_pids = list(reaped_result['terminated_pids'])
    preferred_reaped_pids = _reap_preferred_console_host(host=host, port=port, explicit_port=explicit_port)
    reaped_pids.extend(preferred_reaped_pids)
    reaped_host_pids = [*reaped_result['host_pids'], *preferred_reaped_pids]
    reaped_helper_pids = list(reaped_result['helper_pids'])
    if reaped_pids:
      if reaped_helper_pids and not reaped_host_pids:
        _append_launcher_self_heal_event(
          workspace_root=workspace_root,
          launch_id=launch_id,
          requested_port=port,
          bound_port=port,
          session_attach_required=open_browser,
          helper_pid=reaped_helper_pids[0],
          cleanup_actions=['terminate_helper_processes', 'wait_for_helper_port_release'],
          reason_code='helper_orphan_pruned',
          notes=f'reaped_helper_pid_count={len(set(reaped_helper_pids))}',
        )
        _append_launcher_telemetry_event(
          workspace_root=workspace_root,
          launch_id=launch_id,
          event='helper_orphan_pruned',
          state='REAP_STALE',
          requested_port=port,
          bound_port=port,
          helper_pid=reaped_helper_pids[0],
          cleanup_actions=['terminate_helper_processes', 'wait_for_helper_port_release'],
          session_attach_required=open_browser,
          reason_code='helper_orphan',
          notes=f'reaped_helper_pid_count={len(set(reaped_helper_pids))}',
        )
      else:
        cleanup_actions = ['terminate_stale_workspace_hosts', 'wait_for_port_release']
        if reaped_helper_pids:
          cleanup_actions.extend(['terminate_helper_processes', 'wait_for_helper_port_release'])
        _append_launcher_self_heal_event(
          workspace_root=workspace_root,
          launch_id=launch_id,
          requested_port=port,
          bound_port=port,
          session_attach_required=open_browser,
          host_pid=reaped_host_pids[0] if reaped_host_pids else None,
          helper_pid=reaped_helper_pids[0] if reaped_helper_pids else None,
          cleanup_actions=cleanup_actions,
          reason_code='workspace_conflict_pruned',
          notes=f'reaped_pid_count={len(set(reaped_pids))}',
        )
      _append_launcher_telemetry_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        event='stale_pruned',
        state='REAP_STALE',
        requested_port=port,
        bound_port=port,
        host_pid=reaped_host_pids[0] if reaped_host_pids else None,
        helper_pid=reaped_helper_pids[0] if reaped_helper_pids else None,
        cleanup_actions=(
          ['terminate_stale_workspace_hosts', 'wait_for_port_release']
          + (['terminate_helper_processes', 'wait_for_helper_port_release'] if reaped_helper_pids else [])
        ),
        session_attach_required=open_browser,
        reason_code='workspace_conflict_pruned',
        notes=f'reaped_pid_count={len(set(reaped_pids))}',
      )
    bound_port = _select_console_port(host=host, preferred_port=port, explicit_port=explicit_port)
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='port_selected',
      state='SELECT_PORT',
      requested_port=port,
      bound_port=bound_port,
      session_attach_required=open_browser,
      reason_code='preferred_port' if bound_port == port else 'fallback_nearby_port',
    )
    recovery_helper: dict[str, Any] | None = None
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='helper_launch_started',
      state='START_HELPER',
      requested_port=port,
      bound_port=bound_port,
      session_attach_required=open_browser,
    )
    try:
      recovery_helper = _launch_console_recovery_helper(
        workspace_root=workspace_root,
        helper_host=host,
        preferred_port=bound_port + 1,
        target_host=host,
        target_port=bound_port,
      )
    except Exception as _helper_exc:
      recovery_helper = None
      _helper_failure_cause = str(getattr(_helper_exc, 'helper_failure_cause', '') or 'helper_start_failed_unknown')
      _append_launcher_self_heal_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        requested_port=port,
        bound_port=bound_port,
        session_attach_required=open_browser,
        cleanup_actions=['continue_without_helper'],
        reason_code='helper_start_failed_continue_without_helper',
        notes=f'launch_continues_without_helper;cause={_helper_failure_cause}',
      )
      _append_launcher_telemetry_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        event='helper_unavailable_nonfatal',
        state='START_HELPER',
        requested_port=port,
        bound_port=bound_port,
        session_attach_required=open_browser,
        reason_code='helper_start_failed',
        notes=f'cause={_helper_failure_cause}',
      )
    else:
      helper_pid = int((recovery_helper or {}).get('pid', 0) or 0)
      _append_launcher_telemetry_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        event='helper_ready',
        state='START_HELPER',
        requested_port=port,
        bound_port=bound_port,
        helper_pid=helper_pid or None,
        session_attach_required=open_browser,
        reason_code='helper_ready',
      )
    session_token = uuid.uuid4().hex
    probe_url = f'http://{host}:{bound_port}/?session={session_token}&launch={launch_id}&probe=1'
    ready_url = f'http://{host}:{bound_port}/?session={session_token}&launch={launch_id}'
    browser_url = f'http://{host}:{bound_port}/'
    command = _detached_console_command(
      workspace_root=workspace_root,
      host=host,
      port=bound_port,
      session_token=session_token,
      handoff_context={
        'launch_id': launch_id,
        'launch_mode': 'detached',
        'requested_port': port,
        'bound_port': bound_port,
        'session_attach_required': open_browser,
      },
      recovery_helper=recovery_helper,
    )
    popen_kwargs: dict[str, Any] = {
      'stdin': subprocess.DEVNULL,
      'stdout': subprocess.DEVNULL,
      'stderr': subprocess.DEVNULL,
      'close_fds': True,
      'cwd': str(workspace_root),
      'env': _detached_console_env(workspace_root=workspace_root),
    }
    process: subprocess.Popen[Any]
    if os.name == 'nt':
      creationflags = (
        int(getattr(subprocess, 'DETACHED_PROCESS', 0))
        | int(getattr(subprocess, 'CREATE_NEW_PROCESS_GROUP', 0))
        | int(getattr(subprocess, 'CREATE_NO_WINDOW', 0))
      )
      process = subprocess.Popen(command, creationflags=creationflags, **popen_kwargs)
    else:
      process = subprocess.Popen(command, start_new_session=True, **popen_kwargs)
    host_pid = int(process.pid)
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='host_spawn_started',
      state='SPAWN_HOST',
      requested_port=port,
      bound_port=bound_port,
      host_pid=host_pid,
      helper_pid=helper_pid or None,
      session_attach_required=open_browser,
    )

    ready_basis = _collect_console_health_basis(
      host=host,
      port=bound_port,
      probe_url=probe_url,
      expected_session_token=session_token,
      expected_launch_id=launch_id,
      ready_timeout_sec=DETACHED_CONSOLE_READY_WAIT_SEC,
      session_status_timeout_sec=1.5,
    )
    if not (bool(ready_basis.get('ready_probe_ok')) and bool(ready_basis.get('session_status_ok'))):
      _append_launcher_telemetry_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        event='ready_fail',
        state='PROBE_READY',
        requested_port=port,
        bound_port=bound_port,
        host_pid=host_pid,
        helper_pid=helper_pid or None,
        session_attach_required=open_browser,
        reason_code='ready_timeout',
        notes=_console_health_basis_notes(ready_basis),
      )
      cleanup_actions = _launcher_cleanup_actions(helper_host=helper_host, helper_port=helper_port)
      _emit_launcher_cleanup_events(
        workspace_root=workspace_root,
        launch_id=launch_id,
        requested_port=port,
        bound_port=bound_port,
        host_pid=host_pid,
        helper_pid=helper_pid or None,
        session_attach_required=open_browser,
        cleanup_actions=cleanup_actions,
        reason_code='ready_timeout',
      )
      _cleanup_failed_detached_console_launch(
        host=host,
        port=bound_port,
        host_pids=[host_pid],
        helper_host=helper_host,
        helper_port=helper_port,
        helper_pids=[helper_pid] if helper_pid > 0 else [],
      )
      launch_block_reason = 'ready_timeout'
      launch_block_next_action = 'Retry the console launch after the detached host readiness issue is resolved.'
      raise ConsoleLaunchBlockedError(
        'The detached console host did not become ready before the browser launch window expired.',
        reason=launch_block_reason,
        next_action=launch_block_next_action,
      )

    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='ready_pass',
      state='PROBE_READY',
      requested_port=port,
      bound_port=bound_port,
      host_pid=host_pid,
      helper_pid=helper_pid or None,
      session_attach_required=open_browser,
      reason_code=_console_ready_reason_code(ready_basis),
      notes=_console_health_basis_notes(ready_basis),
    )
    tracked_pid = _listener_pid_for_port_with_retry(host, bound_port) or host_pid
    _record_console_host(pid=tracked_pid, workspace_root=workspace_root, host=host, port=bound_port)
    helper_host = str((recovery_helper or {}).get('host', '') or '').strip()
    helper_port = int((recovery_helper or {}).get('port', 0) or 0)
    helper_listener_pid = int(_listener_pid_for_port_with_retry(helper_host, helper_port) or 0) if (helper_host and helper_port > 0) else 0
    if helper_pid > 0 and helper_host and helper_port > 0:
      _record_console_host(
        pid=helper_listener_pid or helper_pid,
        workspace_root=workspace_root,
        host=helper_host,
        port=helper_port,
        role='recovery-helper',
      )
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='registry_commit',
      state='COMMIT_REGISTRY',
      requested_port=port,
      bound_port=bound_port,
      host_pid=tracked_pid,
      helper_pid=(helper_listener_pid or helper_pid) or None,
      session_attach_required=open_browser,
    )

    replacement_attached = False
    if open_browser and replacement_launch:
      replacement_attached = _wait_for_console_browser_session_attach(
        host,
        bound_port,
        expected_session_token=session_token,
        expected_launch_id=launch_id,
      )

    should_open_browser = bool(open_browser and (not replacement_launch or not replacement_attached))
    browser_opened = False
    final_session_attach_confirmed: bool | None = True if (open_browser and replacement_attached) else None
    if should_open_browser:
      _append_launcher_telemetry_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        event='browser_open_attempted',
        state='OPEN_BROWSER',
        requested_port=port,
        bound_port=bound_port,
        host_pid=tracked_pid,
        helper_pid=(helper_listener_pid or helper_pid) or None,
        session_attach_required=True,
        reason_code=_console_ready_reason_code(ready_basis),
        notes=_console_health_basis_notes(ready_basis),
      )
      if pre_browser_hook is not None:
        pre_browser_hook()
      browser_opened = _open_console_browser(ready_url)
      _append_launcher_telemetry_event(
        workspace_root=workspace_root,
        launch_id=launch_id,
        event='browser_open_result',
        state='OPEN_BROWSER',
        requested_port=port,
        bound_port=bound_port,
        host_pid=tracked_pid,
        helper_pid=(helper_listener_pid or helper_pid) or None,
        session_attach_required=True,
        reason_code='browser_opened' if browser_opened else 'browser_open_failed',
      )
      attach_timeout_sec = DETACHED_CONSOLE_REATTACH_WAIT_SEC if replacement_launch else DETACHED_CONSOLE_FIRST_ATTACH_WAIT_SEC
      browser_attached = browser_opened and _wait_for_console_browser_session_attach(
        host,
        bound_port,
        expected_session_token=session_token,
        expected_launch_id=launch_id,
        timeout_sec=attach_timeout_sec,
      )
      if browser_attached:
        final_session_attach_confirmed = True
        _append_launcher_telemetry_event(
          workspace_root=workspace_root,
          launch_id=launch_id,
          event='attach_confirmed',
          state='CONFIRM_ATTACH',
          requested_port=port,
          bound_port=bound_port,
          host_pid=tracked_pid,
          helper_pid=(helper_listener_pid or helper_pid) or None,
          session_attach_required=True,
          session_attach_confirmed=True,
        )
      if not browser_attached:
        final_session_attach_confirmed = False
        _append_launcher_telemetry_event(
          workspace_root=workspace_root,
          launch_id=launch_id,
          event='attach_timeout',
          state='CONFIRM_ATTACH',
          requested_port=port,
          bound_port=bound_port,
          host_pid=tracked_pid,
          helper_pid=(helper_listener_pid or helper_pid) or None,
          session_attach_required=True,
          session_attach_confirmed=False,
          reason_code='attach_timeout',
          notes=_append_note_suffix(
            _console_health_basis_notes(ready_basis),
            f'host_pid_alive={_host_pid_alive_note_value(tracked_pid)}',
          ),
        )
        retry_outcome = _perform_bounded_same_host_attach_retry(
          workspace_root=workspace_root,
          launch_id=launch_id,
          lock_path=launch_lock_path,
          host=host,
          port=bound_port,
          requested_port=port,
          browser_url=ready_url,
          probe_url=probe_url,
          session_token=session_token,
          tracked_pid=tracked_pid,
          helper_pid=(helper_listener_pid or helper_pid) or None,
        )
        browser_opened = browser_opened or bool(retry_outcome.get('browser_opened'))
        if bool(retry_outcome.get('session_attach_confirmed')):
          final_session_attach_confirmed = True
          browser_attached = True
        if browser_attached:
          pass
        else:
          hydration_basis = _collect_post_attach_timeout_retention_basis(
            host=host,
            port=bound_port,
            probe_url=probe_url,
            expected_session_token=session_token,
            expected_launch_id=launch_id,
          )
          _append_launcher_telemetry_event(
            workspace_root=workspace_root,
            launch_id=launch_id,
            event='attach_unconfirmed_launch_blocked',
            state='FAIL_CLOSED',
            requested_port=port,
            bound_port=bound_port,
            host_pid=tracked_pid,
            helper_pid=(helper_listener_pid or helper_pid) or None,
            session_attach_required=True,
            session_attach_confirmed=False,
            decision='launch_blocked',
            reason_code='attach_hydration_timeout',
            notes=_console_health_basis_notes(hydration_basis),
          )
          cleanup_actions = _launcher_cleanup_actions(helper_host=helper_host, helper_port=helper_port)
          _emit_launcher_cleanup_events(
            workspace_root=workspace_root,
            launch_id=launch_id,
            requested_port=port,
            bound_port=bound_port,
            host_pid=tracked_pid,
            helper_pid=(helper_listener_pid or helper_pid) or None,
            session_attach_required=True,
            cleanup_actions=cleanup_actions,
            reason_code='attach_hydration_timeout',
          )
          _cleanup_failed_detached_console_launch(
            host=host,
            port=bound_port,
            host_pids=[tracked_pid] if tracked_pid else [],
            helper_host=helper_host,
            helper_port=helper_port,
            helper_pids=[helper_pid] if helper_pid > 0 else [],
          )
          launch_block_reason = 'attach_hydration_timeout'
          launch_block_next_action = 'Retry the console launch so the current browser run can complete bootstrap hydration for this launch.'
          raise ConsoleLaunchBlockedError(
            'The detached console host became ready, but the current browser launch did not complete bootstrap hydration before the attach window expired.',
            reason=launch_block_reason,
            next_action=launch_block_next_action,
          )

    decision = _launcher_success_decision(
      replacement_launch=replacement_launch,
      reaped_pid_count=len(set(reaped_pids)),
      browser_opened=browser_opened,
      session_attach_required=open_browser,
      session_attach_confirmed=final_session_attach_confirmed,
      helper_recovery=helper_recovery,
    )
    terminal_state = 'CONFIRM_ATTACH' if open_browser else 'COMMIT_REGISTRY'
    terminal_reason = (
      'helper_recovery_success'
      if helper_recovery
      else 'manual_attach_required_after_attach_timeout'
      if decision == 'manual_attach_required'
      else 'healthy_replacement_attached'
      if replacement_launch and final_session_attach_confirmed
      else 'manual_attach_required'
      if not open_browser
      else 'fresh_launch_attached'
      if decision == 'launched_fresh_host'
      else 'replacement_launch_attached'
      if decision == 'replaced_existing_host'
      else 'launcher_success'
    )
    _append_launcher_terminal_success_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      decision=decision,
      state=terminal_state,
      requested_port=port,
      bound_port=bound_port,
      host_pid=tracked_pid,
      helper_pid=(helper_listener_pid or helper_pid) or None,
      session_attach_required=open_browser,
      session_attach_confirmed=final_session_attach_confirmed,
      cleanup_actions=[],
      reason_code=terminal_reason,
    )
    return {
      'decision': decision,
      'command_family': 'polyventure console',
      'launch_mode': 'detached',
      'launch_id': launch_id,
      'url': browser_url,
      'requested_port': port,
      'bound_port': bound_port,
      'reaped_pid_count': len(set(reaped_pids)),
      'browser_opened': browser_opened,
      'attach_window_sec': DETACHED_CONSOLE_STARTUP_GRACE_SEC if not open_browser else None,
      'next_action': (
        'The browser did not confirm attachment before the launch window expired. Open or refresh the local shell URL manually; the detached host remains available and will self-tear down if no browser session attaches.'
        if open_browser and final_session_attach_confirmed is False
        else (
          'Open the local shell URL manually within '
          f'{_manual_attach_deadline_label(DETACHED_CONSOLE_STARTUP_GRACE_SEC)} '
          'or the detached host will self-close before the first browser session attaches.'
        )
        if not open_browser
        else 'Close the browser tab or window when finished; the detached host now self-tears down after the browser session ends.'
      ),
    }
  except Exception as exc:
    reason_code = launch_block_reason
    next_action = launch_block_next_action
    if isinstance(exc, ConsoleLaunchBlockedError):
      reason_code = str(exc.reason or reason_code)
      next_action = str(exc.next_action or next_action)
    _append_launcher_telemetry_event(
      workspace_root=workspace_root,
      launch_id=launch_id,
      event='launch_blocked',
      state='FAIL_CLOSED',
      requested_port=port,
      bound_port=bound_port,
      host_pid=host_pid or None,
      helper_pid=(helper_listener_pid or helper_pid) or None,
      session_attach_required=open_browser,
      session_attach_confirmed=(False if isinstance(exc, ConsoleLaunchBlockedError) else None),
      decision='launch_blocked',
      reason_code=reason_code,
      notes=str(exc),
    )
    raise
  finally:
    _release_console_launch_lock(launch_lock_path)


def _add_runtime_args(parser: argparse.ArgumentParser) -> None:
  parser.add_argument(
    '--mode',
    choices=['ab_guarded', 'a_targeted', 'b_targeted'],
    default='ab_guarded',
    help='Strategy mode. Dry-run integration supports ab_guarded fully and targeted modes only with explicit confirmation.',
  )
  parser.add_argument(
    '--dry-run',
    action='store_true',
    default=True,
    help='Plan-only execution. No orders are submitted.',
  )
  parser.add_argument(
    '--allow-orders',
    action='store_true',
    help='Reserved for a later sandbox-enable stage after the documented acceptance gates pass. Not enabled yet.',
  )
  parser.add_argument(
    '--env',
    choices=['demo', 'prod'],
    help='Override the configured Kalshi environment for this invocation.',
  )
  parser.add_argument(
    '--subaccount',
    type=int,
    help='Override the configured Kalshi subaccount for this invocation.',
  )
  parser.add_argument(
    '--confirm-targeted',
    action='store_true',
    help='Explicitly confirm a targeted mode selection when not using ab_guarded.',
  )


def _add_context_args(parser: argparse.ArgumentParser) -> None:
  parser.add_argument(
    '--env',
    choices=['demo', 'prod'],
    help='Override the configured Kalshi environment for this invocation.',
  )
  parser.add_argument(
    '--subaccount',
    type=int,
    help='Override the configured Kalshi subaccount for this invocation.',
  )


def _datapack_output_path(path_value: str | Path) -> Path:
  path = Path(path_value).expanduser()
  if not path.is_absolute():
    path = (Path.cwd() / path).resolve()
  return path


def _canonical_datapack_root(*, workspace_root: Path) -> Path:
  root = (workspace_root / CANONICAL_DATAPACK_ROOT_RELATIVE).resolve()
  root.mkdir(parents=True, exist_ok=True)
  return root


def _canonical_datapack_archive_root(*, workspace_root: Path) -> Path:
  root = (workspace_root / CANONICAL_DATAPACK_ARCHIVE_ROOT_RELATIVE).resolve()
  root.mkdir(parents=True, exist_ok=True)
  return root


def _canonical_datapack_ledger_path(*, workspace_root: Path) -> Path:
  ledger_path = (workspace_root / CANONICAL_DATAPACK_LEDGER_RELATIVE).resolve()
  ledger_path.parent.mkdir(parents=True, exist_ok=True)
  return ledger_path


def _path_within_root(path: Path, root: Path) -> bool:
  try:
    path.resolve().relative_to(root.resolve())
    return True
  except ValueError:
    return False


def _safe_datapack_directory_name(raw_value: str) -> str:
  normalized = re.sub(r'[^A-Za-z0-9._-]+', '-', str(raw_value or '').strip()).strip('-._')
  return normalized or f'datapack-{hashlib.sha256(str(raw_value).encode("utf-8")).hexdigest()[:12]}'


def _append_canonical_mutation_ledger(
  *,
  workspace_root: Path,
  action: str,
  decision: str,
  reason: str,
  operator_reason: str,
  reference: str | None,
  datapack_id: str | None,
  source_path: str | None,
  target_path: str | None,
  archive_path: str | None,
  checksum_count: int | None,
) -> str:
  ledger_path = _canonical_datapack_ledger_path(workspace_root=workspace_root)
  entry = {
    'recorded_at_utc': _utc_now_iso(),
    'action': str(action or '').strip().lower(),
    'decision': str(decision or '').strip().lower(),
    'reason': str(reason or '').strip().lower(),
    'operator_reason': str(operator_reason or '').strip(),
    'reference': str(reference or '').strip() or None,
    'datapack_id': str(datapack_id or '').strip() or None,
    'source_path': str(source_path or '').strip() or None,
    'target_path': str(target_path or '').strip() or None,
    'archive_path': str(archive_path or '').strip() or None,
    'checksum_count': int(checksum_count or 0) if checksum_count is not None else None,
    'actor': str(os.environ.get('USERNAME') or os.environ.get('USER') or 'unknown').strip() or 'unknown',
  }
  with ledger_path.open('a', encoding='utf-8') as handle:
    handle.write(json.dumps(entry, default=str))
    handle.write('\n')
  return str(ledger_path)


def _resolve_canonical_datapack_by_id(
  *,
  canonical_root: Path,
  datapack_id: str,
) -> Path | None:
  requested = str(datapack_id or '').strip()
  if not requested:
    return None
  for child in sorted(canonical_root.iterdir() if canonical_root.exists() else []):
    if not child.is_dir():
      continue
    if child.name == requested:
      return child.resolve()
    manifest_path = child / 'manifest.json'
    if not manifest_path.exists():
      continue
    try:
      manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
      continue
    if str((manifest or {}).get('datapack_id') or '').strip() == requested:
      return child.resolve()
  return None


def _build_datapack_canonical_add_payload(
  *,
  input_root: Path,
  env_override: str | None,
  subaccount_override: int | None,
  operator_reason: str,
  reference: str | None,
) -> dict[str, Any]:
  workspace_root = _resolve_console_workspace_root()
  canonical_root = _canonical_datapack_root(workspace_root=workspace_root)
  manifest, restore_policy = _read_datapack_control_pair(input_root)
  issues = validate_datapack_controls(manifest, restore_policy)
  issues.extend(validate_datapack_artifacts(input_root, manifest, restore_policy))
  if issues:
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack canonical-add',
      'reason': 'canonical_add_attestation_failed',
      'message': 'Canonical add is blocked because the input datapack failed attestation checks.',
      'issues': issues,
      'next_action': 'Fix manifest/restore policy or payload checksums and retry canonical add.',
    }

  identity = _active_datapack_identity(env_override=env_override, subaccount_override=subaccount_override)
  identity_result = evaluate_datapack_identity(
    manifest,
    active_operation_lane=str(env_override or identity['operation_lane'] or 'sandbox'),
    active_api_key_hash=str(identity['api_key_hash'] or ''),
  )
  if not bool(identity_result.get('allowed')):
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack canonical-add',
      'reason': 'canonical_add_identity_mismatch',
      'message': 'Canonical add is blocked because lane/key identity did not match the active profile.',
      'reasons': identity_result.get('reasons', []),
      'next_action': 'Use datapack validate/rebind to reconcile identity before canonical add.',
    }

  datapack_id = str(manifest.get('datapack_id') or '').strip()
  target_name = _safe_datapack_directory_name(datapack_id or input_root.name)
  target_root = (canonical_root / target_name).resolve()
  if not _path_within_root(target_root, canonical_root):
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack canonical-add',
      'reason': 'canonical_add_path_escape_blocked',
      'message': 'Canonical add target escaped canonical datastore containment.',
      'next_action': 'Retry with a valid datapack identifier/path that resolves inside canonical root.',
    }
  if target_root.exists():
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack canonical-add',
      'reason': 'canonical_add_target_exists',
      'message': 'Canonical add is blocked because the target datapack id/path already exists.',
      'target_path': str(target_root),
      'next_action': 'Remove/archive the existing canonical datapack first or choose a distinct datapack id.',
    }

  shutil.copytree(input_root, target_root)
  post_issues = validate_datapack_artifacts(target_root, manifest, restore_policy)
  if post_issues:
    shutil.rmtree(target_root, ignore_errors=True)
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack canonical-add',
      'reason': 'canonical_add_postcopy_validation_failed',
      'message': 'Canonical add copy verification failed after write.',
      'issues': post_issues,
      'next_action': 'Inspect source datapack and retry canonical add after attestation issues are resolved.',
    }

  checksums = manifest.get('checksums') if isinstance(manifest.get('checksums'), dict) else {}
  ledger_path = _append_canonical_mutation_ledger(
    workspace_root=workspace_root,
    action='canonical_add',
    decision='go',
    reason='canonical_add_succeeded',
    operator_reason=operator_reason,
    reference=reference,
    datapack_id=datapack_id or target_name,
    source_path=str(input_root),
    target_path=str(target_root),
    archive_path=None,
    checksum_count=len(checksums),
  )
  return {
    'decision': 'go',
    'command_family': 'polyventure datapack canonical-add',
    'reason': 'canonical_add_succeeded',
    'input_path': str(input_root),
    'target_path': str(target_root),
    'canonical_root': str(canonical_root),
    'datapack_id': datapack_id or target_name,
    'checksum_count': len(checksums),
    'ledger_path': ledger_path,
    'next_action': 'Use datapack canonical-remove with archive evidence if this canonical entry must be retired.',
  }


def _build_datapack_canonical_remove_payload(
  *,
  selector_id: str | None,
  selector_path: str | None,
  operator_reason: str,
  reference: str | None,
) -> dict[str, Any]:
  workspace_root = _resolve_console_workspace_root()
  canonical_root = _canonical_datapack_root(workspace_root=workspace_root)
  archive_root = _canonical_datapack_archive_root(workspace_root=workspace_root)

  selected_path: Path | None = None
  if selector_id:
    selected_path = _resolve_canonical_datapack_by_id(canonical_root=canonical_root, datapack_id=selector_id)
  elif selector_path:
    candidate_path = _datapack_output_path(selector_path)
    if not _path_within_root(candidate_path, canonical_root):
      return {
        'decision': 'no-go',
        'command_family': 'polyventure datapack canonical-remove',
        'reason': 'canonical_remove_path_escape_blocked',
        'message': 'Canonical remove path must stay inside canonical datastore root.',
        'next_action': 'Provide --path inside canonical root or select by --id.',
      }
    selected_path = candidate_path.resolve()

  if selected_path is None or not selected_path.exists() or not selected_path.is_dir():
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack canonical-remove',
      'reason': 'canonical_remove_target_not_found',
      'message': 'Canonical remove target was not found in canonical datastore inventory.',
      'next_action': 'Use datapack id/path from canonical inventory and retry remove.',
    }

  manifest_path = selected_path / 'manifest.json'
  if not manifest_path.exists():
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack canonical-remove',
      'reason': 'canonical_remove_invalid_target_shape',
      'message': 'Canonical remove target is missing manifest.json and is not a valid datapack root.',
      'next_action': 'Select a valid canonical datapack root and retry remove.',
    }

  manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
  datapack_id = str((manifest or {}).get('datapack_id') or selected_path.name).strip() or selected_path.name
  archive_name = '{timestamp}-{name}'.format(
    timestamp=datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ'),
    name=_safe_datapack_directory_name(selected_path.name),
  )
  archive_path = (archive_root / archive_name).resolve()
  if not _path_within_root(archive_path, archive_root):
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack canonical-remove',
      'reason': 'canonical_remove_archive_escape_blocked',
      'message': 'Canonical remove archive path escaped archive containment root.',
      'next_action': 'Retry canonical remove after archive path policy is corrected.',
    }

  shutil.move(str(selected_path), str(archive_path))
  if selected_path.exists() or not archive_path.exists():
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack canonical-remove',
      'reason': 'canonical_remove_archive_move_failed',
      'message': 'Canonical remove archive move did not complete deterministically.',
      'next_action': 'Inspect filesystem permissions/state and retry canonical remove.',
    }

  checksums = manifest.get('checksums') if isinstance(manifest.get('checksums'), dict) else {}
  ledger_path = _append_canonical_mutation_ledger(
    workspace_root=workspace_root,
    action='canonical_remove',
    decision='go',
    reason='canonical_remove_archived',
    operator_reason=operator_reason,
    reference=reference,
    datapack_id=datapack_id,
    source_path=str(selected_path),
    target_path=None,
    archive_path=str(archive_path),
    checksum_count=len(checksums),
  )
  return {
    'decision': 'go',
    'command_family': 'polyventure datapack canonical-remove',
    'reason': 'canonical_remove_archived',
    'datapack_id': datapack_id,
    'canonical_root': str(canonical_root),
    'removed_from': str(selected_path),
    'archived_to': str(archive_path),
    'checksum_count': len(checksums),
    'ledger_path': ledger_path,
    'next_action': 'Use archive evidence path for reconciliation and retain ledger mapping for governed closeout.',
  }


def build_parser() -> argparse.ArgumentParser:
  parser = argparse.ArgumentParser(
    prog='polyventure',
    description='Dry-run-first Polyventure operator shell for the current Kalshi pilot.',
    epilog=(
      'Examples:\n'
      '  polyventure scan-once --dry-run\n'
      '  polyventure run --mode ab_guarded --dry-run\n'
      '  polyventure reconcile\n'
      '  polyventure report --json\n'
      '  polyventure console --host 127.0.0.1 --port 8765'
    ),
    formatter_class=argparse.RawDescriptionHelpFormatter,
  )
  parser.add_argument('--json', action='store_true', help='Emit JSON output only.')

  subparsers = parser.add_subparsers(dest='command', required=True)

  scan_once = subparsers.add_parser(
    'scan-once',
    help='Run one dry-run scan and print candidate pairs.',
  )
  _add_runtime_args(scan_once)

  run = subparsers.add_parser(
    'run',
    help='Run one integrated dry-run runtime cycle and persist planned work to the local state database.',
  )
  _add_runtime_args(run)

  reconcile = subparsers.add_parser(
    'reconcile',
    help='Summarize the latest persisted pair states from the local state database.',
  )
  _add_context_args(reconcile)

  cancel_all = subparsers.add_parser(
    'cancel-all',
    help='Mark all non-terminal local pair states as canceled in the local state database.',
  )
  _add_context_args(cancel_all)

  report = subparsers.add_parser(
    'report',
    help='Show a local persistence summary and latest heartbeat for the dry-run runtime state.',
  )
  _add_context_args(report)

  datapack = subparsers.add_parser(
    'datapack',
    help='Freeze, validate, and rebind the Stage 2 datapack contract without enabling restore execution.',
  )
  datapack_subparsers = datapack.add_subparsers(dest='datapack_command', required=True)

  datapack_export = datapack_subparsers.add_parser(
    'export',
    help='Export the current lane partition into a datapack bundle with manifest and restore policy.',
  )
  _add_context_args(datapack_export)
  datapack_export.add_argument('--output', type=Path, required=True)
  datapack_export.add_argument(
    '--datapack-type',
    choices=('session_snapshot', 'profile_seed', 'forensic_archive'),
    default='session_snapshot',
  )
  datapack_export.add_argument(
    '--include-synthetic-refinement',
    action='store_true',
    help='Include the deterministic synthetic proof family in the exported bundle.',
  )

  datapack_validate = datapack_subparsers.add_parser(
    'validate',
    help='Validate a datapack manifest/restore policy pair against the active lane and key identity.',
  )
  _add_context_args(datapack_validate)
  datapack_validate.add_argument('--input', type=Path, required=True)

  datapack_rebind = datapack_subparsers.add_parser(
    'rebind',
    help='Rewrite datapack identity under an explicit CLI-only force-rebind audit path.',
  )
  _add_context_args(datapack_rebind)
  datapack_rebind.add_argument('--input', type=Path, required=True)
  datapack_rebind.add_argument('--output', type=Path, required=True)
  datapack_rebind.add_argument(
    '--force-rebind-api-key-hash',
    action='store_true',
    help='Explicitly approve cross-key datapack rebind from the CLI only.',
  )

  datapack_synthetic = datapack_subparsers.add_parser(
    'synthetic-refinement',
    help='Create the first bounded synthetic refinement datapack proof family.',
  )
  _add_context_args(datapack_synthetic)
  datapack_synthetic.add_argument('--output', type=Path, required=True)
  datapack_synthetic.add_argument(
    '--datapack-type',
    choices=('synthetic_refinement',),
    default='synthetic_refinement',
  )

  datapack_canonical_add = datapack_subparsers.add_parser(
    'canonical-add',
    help='Add a validated datapack into canonical datastore inventory via governed CLI mutation path.',
  )
  _add_context_args(datapack_canonical_add)
  datapack_canonical_add.add_argument('--input', type=Path, required=True)
  datapack_canonical_add.add_argument('--reason', required=True, help='Operator reason for canonical add ledger record.')
  datapack_canonical_add.add_argument('--reference', default='', help='Optional ticket/reference id for governance traceability.')

  datapack_canonical_remove = datapack_subparsers.add_parser(
    'canonical-remove',
    help='Archive-and-remove a canonical datapack entry through governed CLI mutation path.',
  )
  datapack_canonical_remove.add_argument('--id', default='', help='Canonical datapack id (or directory name) to remove.')
  datapack_canonical_remove.add_argument('--path', default='', help='Canonical datapack directory path to remove.')
  datapack_canonical_remove.add_argument('--reason', required=True, help='Operator reason for canonical remove ledger record.')
  datapack_canonical_remove.add_argument('--reference', default='', help='Required governance reference (ticket/change id).')

  console = subparsers.add_parser(
    'console',
    help='Serve the local HTML Polyventure operator console for the current dry-run surfaces.',
  )
  console.add_argument(
    '--host',
    default='127.0.0.1',
    help='Bind host for the local operator console. Default: 127.0.0.1',
  )
  console.add_argument(
    '--port',
    type=int,
    default=8765,
    help='Bind port for the local operator console. Default: 8765',
  )
  console.add_argument(
    '--foreground',
    action='store_true',
    help='Keep the local operator console server attached to the current terminal for debugging.',
  )
  console.add_argument(
    '--no-open',
    action='store_true',
    help='Start the detached console host without opening a browser tab automatically.',
  )
  console.add_argument(
    '--console-host-process',
    action='store_true',
    help=argparse.SUPPRESS,
  )
  console.add_argument(
    '--console-recovery-helper-process',
    action='store_true',
    help=argparse.SUPPRESS,
  )
  console.add_argument('--session-token', help=argparse.SUPPRESS)
  console.add_argument('--startup-grace-sec', type=float, default=15.0, help=argparse.SUPPRESS)
  console.add_argument('--idle-timeout-sec', type=float, default=20.0, help=argparse.SUPPRESS)
  console.add_argument('--handoff-context-json', default='', help=argparse.SUPPRESS)
  console.add_argument('--recovery-helper-url', help=argparse.SUPPRESS)
  console.add_argument('--recovery-helper-token', help=argparse.SUPPRESS)
  console.add_argument('--recovery-helper-expiry-unix', type=float, default=0.0, help=argparse.SUPPRESS)
  console.add_argument('--recovery-target-host', help=argparse.SUPPRESS)
  console.add_argument('--recovery-target-port', type=int, default=0, help=argparse.SUPPRESS)
  console.add_argument('--recovery-helper-lifetime-sec', type=float, default=DETACHED_CONSOLE_RECOVERY_HELPER_LIFETIME_SEC, help=argparse.SUPPRESS)

  contract_audit = subparsers.add_parser(
    'contract-audit',
    help='Audit and update transition-contract projection rows (V1: review/update only).',
  )
  contract_audit_subparsers = contract_audit.add_subparsers(dest='contract_audit_command', required=True)

  contract_audit_review = contract_audit_subparsers.add_parser(
    'review',
    help='Review current transition-contract projections grouped by rest state.',
  )
  contract_audit_review.add_argument('--format', choices=('markdown', 'json'), default='markdown')
  contract_audit_review.add_argument('--output', type=Path, default=None)
  contract_audit_review.add_argument('--baseline', type=Path, default=None)
  contract_audit_review.add_argument('--write-baseline', action='store_true')
  contract_audit_review.add_argument('--fail-on-drift', action='store_true')

  contract_audit_update = contract_audit_subparsers.add_parser(
    'update',
    help='Apply a targeted transition-contract mutation from a signed spec file.',
  )
  contract_audit_update.add_argument('--spec', type=Path, required=True)

  return parser


def _emit_json(payload: dict[str, Any], exit_code: int = 0) -> int:
  print(json.dumps(payload, indent=2, default=str))
  return exit_code


def _render_human(payload: dict[str, Any]) -> None:
  print('Polyventure :: scan-once')
  print(f"decision: {payload['decision']} :: dry-run")
  print()
  print('Summary')
  print(f"  mode:              {payload['mode']}")
  print(f"  balance_dollars:   {payload['balance_dollars']}")
  print(f"  market_count:      {payload['market_count']}")
  print(f"  candidate_count:   {payload['candidate_count']}")
  print()
  print('Details')
  print(f"  key_file:          {payload['private_key_path_tail']}")
  print(
    '  account_limits:    '
    f"{payload['account_limits']['usage_tier']} "
    f"(read {payload['account_limits']['read']['refill_rate']}/s, "
    f"write {payload['account_limits']['write']['refill_rate']}/s)"
  )
  print()
  print('Evidence')
  print('  command:           polyventure scan-once --dry-run')
  print('  scope:             authenticated balance + open-market scan')
  print()
  print('Next action')
  print('  review the candidate list and fix credential posture before any later live milestone.')
  if payload['candidates']:
    print()
    print('Top candidates')
    for candidate in payload['candidates'][:5]:
      print(
        '  - '
        f"{candidate['ticker']}: "
        f"edge_net={candidate['edge_net_per_contract']} "
        f"yes={candidate['target_yes_bid']} "
        f"no={candidate['target_no_bid']}"
      )


def _render_runtime_human(payload: dict[str, Any]) -> None:
  print('Polyventure :: run')
  print(f"decision: {payload['decision']} :: dry-run")
  print()
  print('Summary')
  print(f"  mode:              {payload['mode']}")
  print(f"  balance_dollars:   {payload['balance_dollars']}")
  print(f"  market_count:      {payload['market_count']}")
  print(f"  candidate_count:   {payload['candidate_count']}")
  print(f"  planned_pair_count:{' ' if payload['planned_pair_count'] < 10 else ''} {payload['planned_pair_count']}")
  print()
  print('Details')
  print(f"  key_file:          {payload['private_key_path_tail']}")
  print(f"  state_db:          {payload['state_db_path_tail']}")
  if payload.get('blocked_reason'):
    print(f"  blocked_reason:    {payload['blocked_reason']}")
  print(
    '  account_limits:    '
    f"{payload['account_limits']['usage_tier']} "
    f"(read {payload['account_limits']['read']['refill_rate']}/s, "
    f"write {payload['account_limits']['write']['refill_rate']}/s)"
  )
  print()
  print('Evidence')
  print('  command:           polyventure run --dry-run')
  print(f"  state_db:          {payload['state_db_path_tail']}")
  print()
  print('Next action')
  print(f"  {payload['next_action']}")
  if payload['planned_pairs']:
    print()
    print('Planned pairs')
    for pair in payload['planned_pairs']:
      print(
        '  - '
        f"{pair['pair_id']} :: {pair['ticker']} "
        f"contracts={pair['contract_count']} "
        f"yes={pair['yes_price']} no={pair['no_price']}"
      )


def _render_reconcile_human(payload: dict[str, Any]) -> None:
  print('Polyventure :: reconcile')
  print(f"decision: {payload['decision']}")
  print()
  print('Summary')
  print(f"  pair_count:        {payload['pair_count']}")
  print(f"  state_db:          {payload['state_db_path_tail']}")
  print()
  print('Next action')
  print(f"  {payload['next_action']}")
  if payload['pairs']:
    print()
    print('Pairs')
    for pair in payload['pairs']:
      print(f"  - {pair['pair_id']} :: {pair['ticker']} :: {pair['state']}")


def _render_cancel_all_human(payload: dict[str, Any]) -> None:
  print('Polyventure :: cancel-all')
  print(f"decision: {payload['decision']} :: local-only")
  print()
  print('Summary')
  print(f"  canceled_pair_count: {payload['canceled_pair_count']}")
  print(f"  state_db:            {payload['state_db_path_tail']}")
  print()
  print('Next action')
  print(f"  {payload['next_action']}")
  if payload['canceled_pairs']:
    print()
    print('Canceled pairs')
    for pair in payload['canceled_pairs']:
      print(f"  - {pair['pair_id']} :: {pair['ticker']} :: was {pair['previous_state']}")


def _render_report_human(payload: dict[str, Any]) -> None:
  print('Polyventure :: report')
  print(f"decision: {payload['decision']}")
  print()
  print('Summary')
  print(f"  state_db:          {payload['state_db_path_tail']}")
  if payload['latest_heartbeat'] is not None:
    print(
      '  latest_heartbeat:  '
      f"{payload['latest_heartbeat']['component']} "
      f"({payload['latest_heartbeat']['status']})"
    )
  print()
  print('Details')
  for table_name, count in payload['table_counts'].items():
    print(f"  {table_name}: {count}")
  print()
  print('Next action')
  print(f"  {payload['next_action']}")


def _render_console_launch_human(payload: dict[str, Any]) -> None:
  print('Polyventure :: console')
  print(f"decision: {payload['decision']} :: {payload['launch_mode']}")
  print()
  print('Summary')
  print(f"  url:               {payload['url']}")
  if payload.get('requested_port') != payload.get('bound_port'):
    print(f"  port:              requested {payload['requested_port']} -> bound {payload['bound_port']}")
  if payload.get('reaped_pid_count'):
    print(f"  stale_hosts:       reaped {payload['reaped_pid_count']}")
  print(f"  browser_opened:    {'yes' if payload['browser_opened'] else 'no'}")
  if payload.get('attach_window_sec'):
    print(f"  attach_window:     {_manual_attach_deadline_label(float(payload['attach_window_sec']))}")
  print()
  print('Next action')
  print(f"  {payload['next_action']}")


def _write_json_file(path: Path, payload: dict[str, Any], *, checksum: str | None = None) -> str:
  text = serialize_datapack_json(payload)
  path.parent.mkdir(parents=True, exist_ok=True)
  path.write_text(text + '\n', encoding='utf-8')
  return str(checksum or datapack_payload_checksum(payload))


def _write_datapack_files(
  *,
  output_root: Path,
  manifest: dict[str, Any],
  restore_policy: dict[str, Any],
  payloads: dict[str, Any],
) -> dict[str, str]:
  checksums: dict[str, str] = {}
  payload_root = output_root / 'payloads'
  for family_id, payload in payloads.items():
    relative_path = f'payloads/{family_id}.json'
    checksum = datapack_payload_checksum(payload)
    checksums[relative_path] = _write_json_file(payload_root / f'{family_id}.json', payload, checksum=checksum)

  restore_policy_checksum = datapack_payload_checksum(restore_policy)
  checksums['restore_policy.json'] = restore_policy_checksum
  manifest['checksums'] = dict(checksums)
  manifest_checksum = datapack_manifest_checksum(manifest)
  manifest['checksums']['manifest.json'] = manifest_checksum
  # Lane L5c (Class-2): sign the manifest integrity root; bundle stays checksum-only.
  sign_datapack_manifest(manifest)

  _write_json_file(output_root / 'restore_policy.json', restore_policy, checksum=restore_policy_checksum)
  _write_json_file(output_root / 'manifest.json', manifest, checksum=manifest_checksum)
  return dict(manifest['checksums'])


def _load_datapack_payloads(input_root: Path, manifest: dict[str, Any]) -> dict[str, Any]:
  payloads: dict[str, Any] = {}
  for inventory_entry in manifest.get('inventory', []):
    if not isinstance(inventory_entry, dict) or not bool(inventory_entry.get('included')):
      continue
    family_id = str(inventory_entry.get('family_id') or '').strip()
    payload_path_value = str(inventory_entry.get('payload_path') or '').strip()
    if not family_id or not payload_path_value:
      raise ValueError('Included datapack families must declare family_id and payload_path.')
    payload_path = input_root / Path(payload_path_value)
    payload = json.loads(payload_path.read_text(encoding='utf-8'))
    if not isinstance(payload, dict):
      raise ValueError(f'Datapack payload {payload_path_value} must decode to a JSON object.')
    payloads[family_id] = payload
  return payloads


def _read_datapack_control_pair(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
  manifest_path = root / 'manifest.json'
  restore_policy_path = root / 'restore_policy.json'
  manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
  restore_policy = json.loads(restore_policy_path.read_text(encoding='utf-8'))
  if not isinstance(manifest, dict) or not isinstance(restore_policy, dict):
    raise ValueError('Datapack control files must both decode to JSON objects.')
  return manifest, restore_policy


_CLI_ENV_TO_LANE = {'demo': 'sandbox', 'prod': 'live'}


def _require_cli_lane_settings(env_value: str | None):
  """Resolve lane-specific settings for a CLI batch command, fail-closed.

  The lane must be stated explicitly via --env {demo,prod} (demo = sandbox lane,
  prod = live lane). There is no default: a bare batch command refuses rather
  than inferring a lane. The resolved Settings carries the correct lane, Kalshi
  environment, endpoint, and lane-scoped credentials.
  """
  lane = _CLI_ENV_TO_LANE.get(str(env_value or '').strip().lower())
  if lane is None:
    raise ValueError(
      'A lane is required for this command: pass --env demo (sandbox lane) '
      'or --env prod (live lane). There is no default lane.'
    )
  return settings_for_lane(load_settings(), lane)


def _active_datapack_identity(*, env_override: str | None, subaccount_override: int | None) -> dict[str, str | None]:
  _ = subaccount_override
  settings = load_settings()
  operation_lane = str(env_override or settings.operation_lane or 'sandbox').strip().lower() or 'sandbox'
  api_key_hash = api_key_hash_for_id(settings.api_key_id)
  profile_token = profile_token_for_key_path(settings.private_key_file)
  return {
    'operation_lane': operation_lane,
    'api_key_hash': api_key_hash,
    'profile_token': profile_token,
    'state_db_path': settings.state_db_path,
  }


def _build_datapack_export_payload(
  *,
  output_root: Path,
  operation_lane: str,
  datapack_type: str,
  include_synthetic_refinement: bool,
  env_override: str | None,
  subaccount_override: int | None,
) -> dict[str, Any]:
  identity = _active_datapack_identity(env_override=env_override, subaccount_override=subaccount_override)
  state_db_path = Path(str(identity['state_db_path'] or '')).expanduser()
  if not state_db_path.is_absolute():
    state_db_path = (Path.cwd() / state_db_path).resolve()
  connection = open_database(state_db_path)
  bundle = build_datapack_bundle(
    connection,
    operation_lane=operation_lane,
    datapack_type=datapack_type,
    api_key_hash=str(identity['api_key_hash'] or ''),
    profile_token=str(identity['profile_token'] or '').strip() or None,
    state_db_path_tail=state_db_path.name,
    include_synthetic_refinement=include_synthetic_refinement,
  )
  manifest = bundle['manifest']
  restore_policy = bundle['restore_policy']
  payloads = bundle['payloads']
  checksums = _write_datapack_files(
    output_root=output_root,
    manifest=manifest,
    restore_policy=restore_policy,
    payloads=payloads,
  )

  included_families = [item['family_id'] for item in manifest['inventory'] if item.get('included')]
  return {
    'decision': 'planned',
    'command_family': 'polyventure datapack export',
    'datapack_type': datapack_type,
    'operation_lane': operation_lane,
    'api_key_hash': identity['api_key_hash'],
    'profile_token': identity['profile_token'],
    'output_path': str(output_root),
    'included_families': included_families,
    'checksums': checksums,
    'next_action': 'Use polyventure datapack validate to confirm the lane-plus-key lock before any future import or purge work.',
  }


def _build_synthetic_datapack_payload(
  *,
  output_root: Path,
  operation_lane: str,
  datapack_type: str,
  env_override: str | None,
  subaccount_override: int | None,
) -> dict[str, Any]:
  identity = _active_datapack_identity(env_override=env_override, subaccount_override=subaccount_override)
  connection = open_database(Path(str(identity['state_db_path'] or '')).expanduser())
  bundle = build_datapack_bundle(
    connection,
    operation_lane=operation_lane,
    datapack_type=datapack_type,
    api_key_hash=str(identity['api_key_hash'] or ''),
    profile_token=str(identity['profile_token'] or '').strip() or None,
    state_db_path_tail=Path(str(identity['state_db_path'] or '')).expanduser().name,
    source_label='synthetic_refinement_seed',
    include_synthetic_refinement=True,
  )
  manifest = bundle['manifest']
  restore_policy = bundle['restore_policy']
  payloads = bundle['payloads']
  convergence = evaluate_datapack_convergence(manifest, restore_policy)
  convergence_class = str(convergence.get('convergence_class') or 'non_convergent_no_go')
  if convergence_class != 'baseline_convergent':
    return {
      'decision': 'no-go',
      'command_family': 'polyventure datapack synthetic-refinement',
      'reason': 'synthetic_datapack_non_convergent_no_go',
      'message': (
        'Synthetic refinement datapack generation is blocked because the bundle is not baseline-convergent '
        f'({convergence_class}).'
      ),
      'datapack_type': datapack_type,
      'operation_lane': operation_lane,
      'api_key_hash': identity['api_key_hash'],
      'profile_token': identity['profile_token'],
      'output_path': str(output_root),
      'convergence': convergence,
      'next_action': (
        'Seed at least one loadable runtime family row in the selected lane and re-run synthetic-refinement '
        'so the generated artifact remains loadable and promotion-safe.'
      ),
    }
  checksums = _write_datapack_files(
    output_root=output_root,
    manifest=manifest,
    restore_policy=restore_policy,
    payloads=payloads,
  )
  return {
    'decision': 'planned',
    'command_family': 'polyventure datapack synthetic-refinement',
    'datapack_type': datapack_type,
    'operation_lane': operation_lane,
    'api_key_hash': identity['api_key_hash'],
    'profile_token': identity['profile_token'],
    'output_path': str(output_root),
    'included_families': [item['family_id'] for item in manifest['inventory'] if item.get('included')],
    'checksums': checksums,
    'convergence': convergence,
    'next_action': 'Use this datapack as the baseline-shaped synthetic carrier for Stage 2 schema, provenance, and convergence validation.',
  }


def _render_datapack_human(payload: dict[str, Any]) -> None:
  print(f"Polyventure :: {payload['command_family'].split(' ', 1)[1]}")
  print(f"decision: {payload['decision']}")
  print()
  print('Summary')
  if payload.get('datapack_type'):
    print(f"  datapack_type:     {payload['datapack_type']}")
  if payload.get('operation_lane'):
    print(f"  operation_lane:    {payload['operation_lane']}")
  if payload.get('api_key_hash'):
    print(f"  api_key_hash:      {payload['api_key_hash']}")
  if payload.get('profile_token'):
    print(f"  profile_token:     {payload['profile_token']}")
  if payload.get('output_path'):
    print(f"  output_path:       {payload['output_path']}")
  if payload.get('input_path'):
    print(f"  input_path:        {payload['input_path']}")
  if payload.get('allowed') is not None:
    print(f"  identity_lock:     {'pass' if payload['allowed'] else 'fail_closed'}")
  print()
  if payload.get('included_families'):
    print('Families')
    for family_id in payload['included_families']:
      print(f'  - {family_id}')
    print()
  if payload.get('issues'):
    print('Issues')
    for issue in payload['issues']:
      print(f'  - {issue}')
    print()
  if payload.get('reasons'):
    print('Identity checks')
    for reason in payload['reasons']:
      print(f'  - {reason}')
    print()
  print('Next action')
  print(f"  {payload['next_action']}")


def _planned_not_ready(command_name: str, as_json: bool) -> int:
  payload = {
    'decision': 'planned',
    'command_family': f'polyventure {command_name}',
    'reason': 'first_milestone_not_implemented',
    'message': (
      'This command is planned in the implementation sequence but is not '
      'enabled in the first dry-run milestone.'
    ),
    'next_action': 'Use polyventure scan-once to validate auth and market discovery first.',
  }
  if as_json:
    return _emit_json(payload)
  print(f"Polyventure :: {command_name}")
  print('decision: planned')
  print()
  print('Summary')
  print(f"  reason:            {payload['reason']}")
  print()
  print('Next action')
  print(f"  {payload['next_action']}")
  return 0


def _emit_no_go(payload: dict[str, Any], as_json: bool, exit_code: int) -> int:
  if as_json:
    return _emit_json(payload, exit_code=exit_code)
  print(f"Polyventure :: {payload['command_family'].split()[-1]}")
  print(f"decision: {payload['decision']}")
  print()
  print('Summary')
  print(f"  reason:            {payload['reason']}")
  print(f"  message:           {payload['message']}")
  if payload.get('reattach_url'):
    print()
    print('Reattach')
    print(f"  {payload['reattach_url']}")
  print()
  print('Next action')
  print(f"  {payload['next_action']}")
  return exit_code


def main(argv: list[str] | None = None) -> int:
  raw_argv = list(argv) if argv is not None else sys.argv[1:]
  parser = build_parser()
  args = parser.parse_args(raw_argv)

  if args.command == 'scan-once':
    if args.mode != 'ab_guarded':
      payload = {
        'decision': 'no-go',
        'command_family': 'polyventure scan-once',
        'reason': 'mode_not_enabled_in_first_milestone',
        'message': 'The first implementation slice supports ab_guarded only.',
        'next_action': 'Re-run with --mode ab_guarded.',
      }
      return _emit_no_go(payload, args.json, 2)

    if args.allow_orders:
      payload = {
        'decision': 'no-go',
        'command_family': 'polyventure scan-once',
        'reason': 'order_capable_mode_not_enabled',
        'message': 'The first implementation slice is dry-run only.',
        'next_action': 'Remove --allow-orders and re-run in dry-run mode.',
      }
      return _emit_no_go(payload, args.json, 2)

    try:
      payload = run_scan_once(settings=_require_cli_lane_settings(args.env), subaccount_override=args.subaccount)
    except Exception as exc:
      error_payload = {
        'decision': 'no-go',
        'command_family': 'polyventure scan-once',
        'reason': 'scan_once_failed',
        'message': str(exc),
        'next_action': 'Fix the reported configuration or authentication issue and re-run scan-once.',
      }
      return _emit_no_go(error_payload, args.json, 1)

    if args.json:
      return _emit_json(payload)
    _render_human(payload)
    return 0

  if args.command == 'run':
    if args.mode != 'ab_guarded' and not args.confirm_targeted:
      payload = {
        'decision': 'no-go',
        'command_family': 'polyventure run',
        'reason': 'targeted_mode_requires_explicit_confirmation',
        'message': 'Targeted runtime modes require explicit operator confirmation.',
        'next_action': 'Re-run with --confirm-targeted or use --mode ab_guarded.',
      }
      return _emit_no_go(payload, args.json, 2)
    if args.allow_orders:
      payload = {
        'decision': 'no-go',
        'command_family': 'polyventure run',
        'reason': 'order_enabled_runtime_not_available',
        'message': 'Order-enabled runtime remains blocked until the sandbox-enable acceptance gates are satisfied.',
        'next_action': 'Remove --allow-orders and continue validating the documented demo and soak gates first.',
      }
      return _emit_no_go(payload, args.json, 2)
    try:
      payload = run_service_once(
        settings=_require_cli_lane_settings(args.env),
        mode=args.mode,
        allow_orders=args.allow_orders,
        confirm_targeted=args.confirm_targeted,
        subaccount_override=args.subaccount,
      )
    except Exception as exc:
      error_payload = {
        'decision': 'no-go',
        'command_family': 'polyventure run',
        'reason': 'runtime_cycle_failed',
        'message': str(exc),
        'next_action': 'Fix the reported configuration, persistence, or authentication issue and re-run the dry-run runtime cycle.',
      }
      return _emit_no_go(error_payload, args.json, 1)
    if args.json:
      return _emit_json(payload)
    _render_runtime_human(payload)
    return 0

  if args.command == 'reconcile':
    try:
      payload = reconcile_pairs(settings=_require_cli_lane_settings(args.env), subaccount_override=args.subaccount)
    except Exception as exc:
      error_payload = {
        'decision': 'no-go',
        'command_family': 'polyventure reconcile',
        'reason': 'reconcile_failed',
        'message': str(exc),
        'next_action': 'Fix the local state database issue and re-run reconcile.',
      }
      return _emit_no_go(error_payload, args.json, 1)
    if args.json:
      return _emit_json(payload)
    _render_reconcile_human(payload)
    return 0

  if args.command == 'cancel-all':
    try:
      payload = cancel_all_pairs(settings=_require_cli_lane_settings(args.env), subaccount_override=args.subaccount)
    except Exception as exc:
      error_payload = {
        'decision': 'no-go',
        'command_family': 'polyventure cancel-all',
        'reason': 'cancel_all_failed',
        'message': str(exc),
        'next_action': 'Fix the local state database issue and re-run cancel-all.',
      }
      return _emit_no_go(error_payload, args.json, 1)
    if args.json:
      return _emit_json(payload)
    _render_cancel_all_human(payload)
    return 0

  if args.command == 'report':
    try:
      payload = report_runtime(settings=_require_cli_lane_settings(args.env), subaccount_override=args.subaccount)
    except Exception as exc:
      error_payload = {
        'decision': 'no-go',
        'command_family': 'polyventure report',
        'reason': 'report_failed',
        'message': str(exc),
        'next_action': 'Fix the local state database issue and re-run report.',
      }
      return _emit_no_go(error_payload, args.json, 1)
    if args.json:
      return _emit_json(payload)
    _render_report_human(payload)
    return 0

  if args.command == 'datapack':
    try:
      if args.datapack_command == 'export':
        output_root = _datapack_output_path(args.output)
        payload = _build_datapack_export_payload(
          output_root=output_root,
          operation_lane=str(args.env or _active_datapack_identity(env_override=args.env, subaccount_override=args.subaccount)['operation_lane'] or 'sandbox'),
          datapack_type=args.datapack_type,
          include_synthetic_refinement=bool(args.include_synthetic_refinement),
          env_override=args.env,
          subaccount_override=args.subaccount,
        )
      elif args.datapack_command == 'synthetic-refinement':
        output_root = _datapack_output_path(args.output)
        payload = _build_synthetic_datapack_payload(
          output_root=output_root,
          operation_lane=str(args.env or _active_datapack_identity(env_override=args.env, subaccount_override=args.subaccount)['operation_lane'] or 'sandbox'),
          datapack_type=args.datapack_type,
          env_override=args.env,
          subaccount_override=args.subaccount,
        )
      elif args.datapack_command == 'validate':
        input_root = _datapack_output_path(args.input)
        manifest, restore_policy = _read_datapack_control_pair(input_root)
        issues = validate_datapack_controls(manifest, restore_policy)
        issues.extend(validate_datapack_artifacts(input_root, manifest, restore_policy))
        convergence = evaluate_datapack_convergence(manifest, restore_policy)
        identity = _active_datapack_identity(env_override=args.env, subaccount_override=args.subaccount)
        identity_result = evaluate_datapack_identity(
          manifest,
          active_operation_lane=str(args.env or identity['operation_lane'] or 'sandbox'),
          active_api_key_hash=str(identity['api_key_hash'] or ''),
        )
        convergence_class = str(convergence.get('convergence_class') or 'non_convergent_no_go')
        convergence_allowed = convergence_class == 'baseline_convergent'
        payload = {
          'decision': 'planned' if not issues and identity_result['allowed'] and convergence_allowed else 'no-go',
          'command_family': 'polyventure datapack validate',
          'input_path': str(input_root),
          'operation_lane': manifest.get('operation_lane'),
          'api_key_hash': manifest.get('api_key_hash'),
          'profile_token': manifest.get('profile_token'),
          'issues': issues,
          'allowed': bool(identity_result['allowed']),
          'reasons': identity_result['reasons'],
          'convergence_class': convergence_class,
          'convergence': convergence,
          'next_action': (
            'Identity lock and convergence checks passed. The datapack remains lane-plus-key compatible for future restore planning.'
            if not issues and identity_result['allowed'] and convergence_allowed
            else (
              'This datapack is proof-only or otherwise non-convergent; do not import it into a destructive load path.'
              if convergence_class == 'proof_only_non_loadable'
              else 'Do not import this datapack as-is. Use the explicit CLI-only rebind path only if joediggidyyy intends to override the key lock.'
            )
          ),
        }
      elif args.datapack_command == 'rebind':
        if not args.force_rebind_api_key_hash:
          payload = {
            'decision': 'no-go',
            'command_family': 'polyventure datapack rebind',
            'reason': 'force_rebind_flag_required',
            'message': 'Cross-key datapack rebind is blocked until the explicit CLI-only force flag is supplied.',
            'next_action': 'Re-run with --force-rebind-api-key-hash if joediggidyyy intends to override the default fail-closed key lock.',
          }
          return _emit_no_go(payload, args.json, 2)
        input_root = _datapack_output_path(args.input)
        output_root = _datapack_output_path(args.output)
        manifest, restore_policy = _read_datapack_control_pair(input_root)
        input_issues = validate_datapack_controls(manifest, restore_policy)
        input_issues.extend(validate_datapack_artifacts(input_root, manifest, restore_policy))
        if input_issues:
          raise ValueError('Datapack rebind is blocked because the input datapack failed attestation: {issues}'.format(issues=', '.join(input_issues)))
        identity = _active_datapack_identity(env_override=args.env, subaccount_override=args.subaccount)
        rebound_manifest, rebound_restore_policy = rebind_datapack_controls(
          manifest,
          restore_policy,
          new_api_key_hash=str(identity['api_key_hash'] or ''),
          profile_token=str(identity['profile_token'] or '').strip() or None,
        )
        payloads = _load_datapack_payloads(input_root, manifest)
        checksums = _write_datapack_files(
          output_root=output_root,
          manifest=rebound_manifest,
          restore_policy=rebound_restore_policy,
          payloads=payloads,
        )
        payload = {
          'decision': 'planned',
          'command_family': 'polyventure datapack rebind',
          'input_path': str(input_root),
          'output_path': str(output_root),
          'operation_lane': rebound_manifest.get('operation_lane'),
          'api_key_hash': rebound_manifest.get('api_key_hash'),
          'profile_token': rebound_manifest.get('profile_token'),
          'included_families': [item['family_id'] for item in rebound_manifest.get('inventory', []) if item.get('included')],
          'checksums': checksums,
          'next_action': 'Treat the rebound datapack as non-actionable until the future restore lane completes explicit revalidation under the new key identity.',
        }
      elif args.datapack_command == 'canonical-add':
        input_root = _datapack_output_path(args.input)
        payload = _build_datapack_canonical_add_payload(
          input_root=input_root,
          env_override=args.env,
          subaccount_override=args.subaccount,
          operator_reason=str(args.reason or '').strip(),
          reference=str(args.reference or '').strip() or None,
        )
      elif args.datapack_command == 'canonical-remove':
        selector_id = str(args.id or '').strip()
        selector_path = str(args.path or '').strip()
        if bool(selector_id) == bool(selector_path):
          payload = {
            'decision': 'no-go',
            'command_family': 'polyventure datapack canonical-remove',
            'reason': 'canonical_remove_selector_required',
            'message': 'Canonical remove requires exactly one selector: --id or --path.',
            'next_action': 'Provide exactly one selector and retry canonical-remove.',
          }
          return _emit_no_go(payload, args.json, 2)
        if not str(args.reference or '').strip():
          payload = {
            'decision': 'no-go',
            'command_family': 'polyventure datapack canonical-remove',
            'reason': 'canonical_remove_reference_required',
            'message': 'Canonical remove requires --reference for governance traceability.',
            'next_action': 'Provide --reference (ticket/change id) and retry canonical-remove.',
          }
          return _emit_no_go(payload, args.json, 2)
        payload = _build_datapack_canonical_remove_payload(
          selector_id=selector_id or None,
          selector_path=selector_path or None,
          operator_reason=str(args.reason or '').strip(),
          reference=str(args.reference or '').strip(),
        )
      else:
        return _planned_not_ready('datapack', args.json)
    except Exception as exc:
      error_payload = {
        'decision': 'no-go',
        'command_family': f'polyventure datapack {getattr(args, "datapack_command", "")}'.strip(),
        'reason': 'datapack_command_failed',
        'message': str(exc),
        'next_action': 'Fix the reported datapack contract, file-system, or identity issue and re-run the datapack command.',
      }
      return _emit_no_go(error_payload, args.json, 1)
    if args.json:
      return _emit_json(payload)
    _render_datapack_human(payload)
    return 0

  if args.command == 'console':
    try:
      if args.console_recovery_helper_process:
        run_console_recovery_helper_server(
          host=args.host,
          port=args.port,
          helper_token=str(args.recovery_helper_token or ''),
          target_host=str(args.recovery_target_host or args.host),
          target_port=int(args.recovery_target_port or args.port),
          lifetime_sec=args.recovery_helper_lifetime_sec,
        )
        return 0
      if args.console_host_process or args.foreground:
        handoff_context: dict[str, Any] | None = None
        if str(args.handoff_context_json or '').strip():
          try:
            parsed_handoff_context = json.loads(str(args.handoff_context_json))
          except json.JSONDecodeError:
            parsed_handoff_context = None
          if isinstance(parsed_handoff_context, dict):
            handoff_context = parsed_handoff_context
        try:
          _console_state_db_path: str | None = str(load_settings().state_db_path or '').strip() or None
        except Exception:
          _console_state_db_path = None
        run_operator_console_server(
          host=args.host,
          port=args.port,
          session_token=args.session_token,
          startup_grace_sec=args.startup_grace_sec,
          idle_timeout_sec=args.idle_timeout_sec,
          state_db_path=_console_state_db_path,
          handoff_context=handoff_context,
          recovery_helper_url=args.recovery_helper_url,
          recovery_helper_token=args.recovery_helper_token,
          recovery_helper_expiry_unix=args.recovery_helper_expiry_unix,
        )
        return 0
      _close_splash: Callable[[], None] | None = None
      if _popup_mode_active(args):
        # Splash is best-effort: a display/tk failure (e.g. headless) must never
        # block the launch now that the splash is default-on.
        try:
          from polyventure.popup import show_launch_splash
          _close_splash = show_launch_splash()
        except Exception:
          _close_splash = None
      try:
        payload = launch_detached_operator_console(
          host=args.host,
          port=args.port,
          open_browser=not args.no_open,
          explicit_port=_has_explicit_port_argument(raw_argv),
          **({'pre_browser_hook': _close_splash} if _close_splash is not None else {}),
        )
      finally:
        if _close_splash is not None:
          _close_splash()
    except Exception as exc:
      error_payload = {
        'decision': 'launch_blocked',
        'command_family': 'polyventure console',
        'reason': str(getattr(exc, 'reason', 'console_failed') or 'console_failed'),
        'message': str(exc),
        'next_action': str(getattr(exc, 'next_action', 'Fix the local shell startup issue and retry the console command.') or 'Fix the local shell startup issue and retry the console command.'),
      }
      return _emit_no_go(error_payload, args.json, 1)
    if payload.get('decision') == 'no-go':
      _blocked_reason = str(payload.get('reason') or '')
      if _blocked_reason == 'instance_already_running' and not args.json:
        reattach_url = str(payload.get('reattach_url') or '')
        print('Polyventure :: console')
        print('decision: no-go')
        print()
        print('Summary')
        print(f"  reason:            instance_already_running")
        print(f"  message:           {payload.get('message') or 'A console session is already running.'}")
        print()
        print('Session')
        print(f'  {reattach_url}')
        print()
        print('Next action')
        print(f'  Navigate to {reattach_url} in your existing browser tab')
        print(f'  Paste the URL in the address bar of the open console window')
        print(f'  Do not open a new window or tab')
        if _popup_mode_active(args):
          from polyventure.popup import show_blocked_launch_popup
          show_blocked_launch_popup(
            reason=_blocked_reason,
            reattach_url=reattach_url,
            message=str(payload.get('message') or ''),
          )
        return 2
      if _blocked_reason == 'execution_in_progress' and _popup_mode_active(args):
        from polyventure.popup import show_blocked_launch_popup
        show_blocked_launch_popup(
          reason=_blocked_reason,
          reattach_url=str(payload.get('reattach_url') or ''),
          message=str(payload.get('message') or ''),
          in_flight_count=int(payload.get('in_flight_count') or 0) or None,
        )
        return 2
      return _emit_no_go(payload, args.json, 2)
    if args.json:
      return _emit_json(payload)
    _render_console_launch_human(payload)
    return 0

  if args.command == 'contract-audit':
    passthrough: list[str] = ['--json'] if args.json else []
    try:
      command_index = raw_argv.index('contract-audit')
    except ValueError:
      command_index = 0
    passthrough.extend(raw_argv[command_index + 1:])
    return run_contract_audit_cli(passthrough)

  return _planned_not_ready(args.command, args.json)


if __name__ == '__main__':
  raise SystemExit(main(sys.argv[1:]))
