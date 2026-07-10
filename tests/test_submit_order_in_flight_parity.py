"""Focused harness for submit-order in-flight panel parity (gaps G4/G5).

Validates the bounded plan
`docs/development/SUBMIT_ORDER_IN_FLIGHT_PARITY_G4_G5_BOUNDED_MATURE_ACTIONABLE_PLAN_2026-06-04.md`.

Contract under test:
  - On Submit order click, the live-interaction (#live-interaction-section)
    panel appears IMMEDIATELY in a truthful PENDING ("SUBMITTING") state,
    mirroring how the scan lane reveals its processing panel on click.
  - The pending surface is never a fabricated "IN FLIGHT" claim; it reconciles
    to the authoritative backend payload on response and fails closed on error
    or no-go (Polymath surgical-posture truthfulness; security invariants
    #3 names-only, #5 fail-closed).

Frontend behavior lives in the served HTML/JS template, so it is asserted
against the rendered console body (the established project pattern). Backend
truthfulness is asserted against the /api/run submit-order response.
"""

from __future__ import annotations

import io
import json
from typing import Any

from polyventure.web_app import (
  OperatorConsoleServices,
  create_operator_console_app,
)


def _call_app(
  app: Any,
  *,
  method: str,
  path: str,
  body: dict[str, Any] | None = None,
) -> tuple[str, str]:
  status_holder: dict[str, str] = {}
  encoded_body = json.dumps(body).encode('utf-8') if body is not None else b''

  def _start_response(status: str, _headers: list[tuple[str, str]]) -> None:
    status_holder['status'] = status

  environ: dict[str, Any] = {
    'REQUEST_METHOD': method,
    'PATH_INFO': path,
    'QUERY_STRING': '',
    'CONTENT_LENGTH': str(len(encoded_body)),
    'wsgi.input': io.BytesIO(encoded_body),
  }
  rendered = b''.join(app(environ, _start_response)).decode('utf-8')
  return status_holder['status'], rendered


def _services() -> OperatorConsoleServices:
  return OperatorConsoleServices(
    bootstrap=lambda **_: {'decision': 'planned'},
    scan=lambda **_: {'decision': 'planned'},
    run=lambda **_: {'decision': 'planned'},
    reconcile=lambda **_: {'pair_count': 0, 'pairs': []},
    report=lambda **_: {'decision': 'noop'},
    cancel_all=lambda **_: {'decision': 'noop'},
  )


def _console_body() -> str:
  app = create_operator_console_app(_services())
  status, body = _call_app(app, method='GET', path='/')
  assert status == '200 OK'
  return body


# --- Frontend: immediate truthful on-click pending surface -------------------


def test_submit_pending_surface_function_present() -> None:
  body = _console_body()
  # signature gained params (phaseLabel/resetElapsed/leaseId) in later teardown work;
  # match param-agnostically so the presence check is not brittle to the signature.
  assert 'function showSubmitPendingSurface(' in body
  # Truthful pending label, never a fabricated IN FLIGHT claim on click.
  assert "pill.textContent = 'SUBMITTING';" in body
  assert "title.textContent = 'EXECUTION';" in body


def test_submit_pending_wired_into_perform_action() -> None:
  body = _console_body()
  # Submit-order intent is detected and the pending surface is shown before the
  # /api/run round trip resolves (mirrors scan's on-click reveal).
  assert "String(options.body.bridge_action || '').toLowerCase() === 'submit_order'" in body
  assert 'state.submitOrderPending = true;' in body
  assert 'showSubmitPendingSurface();' in body


def test_submit_pending_forces_run_wayfinder_emphasis_after_response() -> None:
  body = _console_body()
  assert 'function focusTargetForActionResult(action, payload, options = {}) {' in body
  assert 'const forceEmphasis = Boolean(options.forceEmphasis);' in body
  assert 'forceEmphasis: submitOrderPending && String(focusRouteKey || \'\').toLowerCase() === \'run\'' in body
  assert 'if (Number(payload.planned_pair_count || 0) > 0) return wayfinderRoute(\'evidence-section\', { tone: \'focus-ok\', forceEmphasis });' in body


def test_submit_pending_fails_closed_on_error() -> None:
  body = _console_body()
  # On the error path renderPayload never runs, so the pending surface must snap
  # back to the last authoritative payload (fail closed), not linger as a fake
  # pending state.
  assert 'if (state.submitOrderPending) {' in body
  assert 'state.submitOrderPending = false;' in body
  assert 'if (!responsePayload) {' in body
  assert 'renderLiveInteractionSurface(state.payload || {});' in body


def test_submit_pending_state_field_declared() -> None:
  body = _console_body()
  assert 'submitOrderPending: false,' in body


# --- Backend: truthful reconciliation / fail closed --------------------------


def test_submit_no_go_does_not_fabricate_in_flight_surface() -> None:
  app = create_operator_console_app(_services())
  status, body = _call_app(
    app,
    method='POST',
    path='/api/run',
    body={'bridge_action': 'submit_order'},
  )
  payload = json.loads(body)
  assert status == '200 OK'
  assert payload['decision'] == 'no-go'
  # A no-go submit must NOT leave a surface claiming an in-flight interaction.
  live_interaction = payload.get('live_interaction') or {}
  assert bool(live_interaction.get('surface_visible')) is False


# --- Regression guard: scan precedent untouched ------------------------------


def test_scan_lane_render_unaffected() -> None:
  body = _console_body()
  # The sealed scan precedent and the reconciling render function are preserved.
  assert 'function scanProcessingActive(payload = state.payload || {}) {' in body
  assert 'function renderLiveInteractionSurface(payload = {}) {' in body
  assert 'const surfaceVisible = Boolean(liveInteraction.surface_visible);' in body
