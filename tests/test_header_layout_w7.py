"""W7 (§7.17) header layout tests.

Asserts that the rendered console page exposes the locked Z1/Z2/Z3/Z4 zones,
that the legacy `Next:` pill is absent from the header, that the money-slot
container is fixed-width, that the notification icon socket exists right-anchored,
and that the pill cluster contains exactly two pills with their visual style
classes preserved.
"""

from __future__ import annotations

import io
import re
from typing import Any

from polyventure.web_app import OperatorConsoleServices, create_operator_console_app


def _bootstrap(**_: Any) -> dict[str, Any]:
  return {
    'settings': {
      'kalshi_env': 'demo',
      'operation_lane': 'sandbox',
      'settings_ready': True,
      'environment_ready': True,
      'credential_ready': True,
      'mode_selected': True,
    },
    'next_action': '',
  }


def _services() -> OperatorConsoleServices:
  return OperatorConsoleServices(
    bootstrap=_bootstrap,
    scan=lambda **_: {'decision': 'planned', 'candidate_count': 0, 'settings': _bootstrap()['settings']},
    run=lambda **_: {'decision': 'planned', 'settings': _bootstrap()['settings']},
    reconcile=lambda **_: {'decision': 'planned', 'settings': _bootstrap()['settings']},
    report=lambda **_: {'decision': 'planned', 'settings': _bootstrap()['settings']},
    cancel_all=lambda **_: {'decision': 'planned', 'settings': _bootstrap()['settings']},
    system_log=lambda **_: {'entries': []},
    visuals=lambda **_: {'packet': None},
  )


def _body() -> str:
  app = create_operator_console_app(_services())
  status_holder: dict[str, str] = {}

  def _start_response(status: str, _headers: list[tuple[str, str]]) -> None:
    status_holder['status'] = status

  environ: dict[str, Any] = {
    'REQUEST_METHOD': 'GET',
    'PATH_INFO': '/',
    'QUERY_STRING': '',
    'CONTENT_LENGTH': '0',
    'wsgi.input': io.BytesIO(b''),
  }
  body_bytes = b''.join(app(environ, _start_response))
  assert status_holder['status'].startswith('200'), status_holder['status']
  return body_bytes.decode('utf-8')


def test_header_zones_present() -> None:
  body = _body()
  for zone_id in ('header-zone-z1', 'header-zone-z2', 'header-zone-z3', 'header-zone-z4'):
    assert f'id="{zone_id}"' in body, f'missing zone {zone_id}'


def test_next_pill_absent_from_header() -> None:
  body = _body()
  assert 'id="recommended-pill"' not in body
  assert 'class="pill ok">Next: review startup' not in body


def test_z2_pill_cluster_has_exactly_two_pills() -> None:
  body = _body()
  match = re.search(
    r'<div\s+id="header-zone-z2"[^>]*>(.*?)</div>\s*<div\s+id="header-zone-z3"',
    body,
    re.DOTALL,
  )
  assert match is not None, 'header-zone-z2 cluster block not found'
  cluster_html = match.group(1)
  pill_count = len(re.findall(r'class="pill[^"]*"', cluster_html))
  assert pill_count == 2, f'expected exactly two pills in Z2 cluster, got {pill_count}'
  assert 'id="lane-pill"' in cluster_html
  assert 'id="heartbeat-pill"' in cluster_html


def test_z3_money_slots_have_fixed_width_container() -> None:
  body = _body()
  assert '.header-zone-z3 {' in body
  z3_block = body.split('.header-zone-z3 {', 1)[1].split('}', 1)[0]
  assert 'width: 200px;' in z3_block
  assert 'min-width: 200px;' in z3_block
  assert 'max-width: 200px;' in z3_block


def test_z4_notification_icon_right_anchored() -> None:
  body = _body()
  assert 'id="header-notification-icon"' in body
  assert 'class="header-notification-svg"' in body
  assert 'header-notification-glyph' not in body
  assert 'id="header-notification-unread-dot"' in body
  assert '.header-zone-z4 {' in body
  z4_block = body.split('.header-zone-z4 {', 1)[1].split('}', 1)[0]
  assert 'margin-left: auto;' in z4_block
  assert 'class="header-notification-icon" type="button" aria-label="Open notifications"' in body
  assert 'class="header-notification-icon" type="button" aria-label="Open notifications" hidden' not in body


def test_pill_visual_style_classes_preserved() -> None:
  body = _body()
  assert '.pill {' in body
  assert '.pill.ok {' in body
  assert '.pill.warn {' in body
  assert '.pill.no-go {' in body
  assert '#lane-pill.mode-connecting {' in body


def test_only_z2_contains_pills_inside_meta_block() -> None:
  body = _body()
  meta_match = re.search(
    r'<div\s+class="command-strip-meta">(.*?)</div>\s*</div>\s*</section>',
    body,
    re.DOTALL,
  )
  assert meta_match is not None, 'command-strip-meta block not found'
  meta_html = meta_match.group(1)
  z2_match = re.search(
    r'<div\s+id="header-zone-z2"[^>]*>(.*?)</div>\s*<div\s+id="header-zone-z3"',
    meta_html,
    re.DOTALL,
  )
  assert z2_match is not None
  z2_pill_count = len(re.findall(r'class="pill[^"]*"', z2_match.group(1)))
  total_pill_count = len(re.findall(r'class="pill[^"]*"', meta_html))
  assert z2_pill_count == total_pill_count == 2
