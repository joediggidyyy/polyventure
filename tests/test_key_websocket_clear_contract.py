from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from polyventure import web_app
from polyventure.web_app import create_operator_console_app
from test_web_app import _call_app, _load_lane_key, _services


@pytest.mark.parametrize('lane,url', [
  ('sandbox', 'wss://demo-api.kalshi.example/ws'),
  ('live', 'wss://api.kalshi.example/ws'),
])
def test_key_clear_does_not_clear_websocket_urls(tmp_path: Path, monkeypatch: Any, lane: str, url: str) -> None:
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _load_lane_key(app, tmp_path, monkeypatch, 'live')
  _call_app(app, method='POST', path='/api/websocket-overlay', body={'action': 'load_sandbox_websocket', 'url': 'wss://demo-api.kalshi.example/ws'})
  _call_app(app, method='POST', path='/api/websocket-overlay', body={'action': 'load_live_websocket', 'url': 'wss://api.kalshi.example/ws'})

  status, _, body = _call_app(app, method='POST', path='/api/key-clear')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['key_management']['loaded_count'] == 0
  assert payload['session_overlay']['context']['websocket_values'][lane] == url.replace('wss://', '')


def test_websocket_clear_does_not_clear_loaded_keys(tmp_path: Path, monkeypatch: Any) -> None:
  app = create_operator_console_app(_services(), tombstone_path=tmp_path / 'tombstones.json')

  _load_lane_key(app, tmp_path, monkeypatch, 'sandbox')
  _load_lane_key(app, tmp_path, monkeypatch, 'live')
  _call_app(app, method='POST', path='/api/websocket-overlay', body={'action': 'load_sandbox_websocket', 'url': 'wss://demo-api.kalshi.example/ws'})
  _call_app(app, method='POST', path='/api/websocket-overlay', body={'action': 'load_live_websocket', 'url': 'wss://api.kalshi.example/ws'})

  status, _, body = _call_app(app, method='POST', path='/api/websocket-overlay', body={'action': 'clear_all'})
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['session_overlay']['context']['websocket_values']['sandbox'] == 'unconfigured'
  assert payload['session_overlay']['context']['websocket_values']['live'] == 'unconfigured'
  assert payload['key_management']['sandbox_key_loaded'] is True
  assert payload['key_management']['live_key_loaded'] is True


def test_lane_websocket_clear_hold_persists_after_restart(tmp_path: Path) -> None:
  tombstone = tmp_path / 'tombstones.json'
  app1 = create_operator_console_app(_services(), tombstone_path=tombstone)

  _call_app(app1, method='POST', path='/api/websocket-overlay', body={'action': 'clear_all'})
  _call_app(
    app1,
    method='POST',
    path='/api/websocket-overlay',
    body={'action': 'load_live_websocket', 'url': 'wss://api.kalshi.example/ws'},
  )

  app2 = create_operator_console_app(_services(), tombstone_path=tombstone)
  status, _, body = _call_app(app2, method='GET', path='/api/bootstrap')
  payload = json.loads(body)

  assert status == '200 OK'
  assert payload['session_overlay']['context']['websocket_values']['live'] == 'api.kalshi.example/ws'
  assert payload['session_overlay']['context']['websocket_values']['sandbox'] == 'unconfigured'
