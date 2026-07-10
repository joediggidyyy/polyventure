from __future__ import annotations

import asyncio
from collections import deque
import json

from cryptography.hazmat.primitives.asymmetric import rsa
import pytest

from polyventure.websocket_client import (
  KalshiWebSocketClient,
  WebSocketAuthError,
)


class _FakeSocket:
  def __init__(self, messages: list[str] | None = None) -> None:
    self.sent_messages: list[str] = []
    self._messages: deque[str] = deque(messages or [])
    self.closed = False

  async def send(self, message: str) -> None:
    self.sent_messages.append(message)

  async def recv(self) -> str:
    if not self._messages:
      raise TimeoutError('no message available')
    return self._messages.popleft()

  async def close(self) -> None:
    self.closed = True


def _private_key() -> object:
  return rsa.generate_private_key(public_exponent=65537, key_size=2048)


def test_websocket_headers_include_required_fields() -> None:
  client = KalshiWebSocketClient(
    ws_url='wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2',
    api_key_id='key-id',
    private_key=_private_key(),
  )

  headers = client._generate_websocket_headers(timestamp_ms='1700000000000')

  assert headers['KALSHI-ACCESS-KEY'] == 'key-id'
  assert headers['KALSHI-ACCESS-TIMESTAMP'] == '1700000000000'
  assert headers['KALSHI-ACCESS-SIGNATURE']


def test_connect_maps_auth_error_to_websocket_auth_error() -> None:
  client = KalshiWebSocketClient(
    ws_url='wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2',
    api_key_id='key-id',
    private_key=_private_key(),
  )

  class _AuthFailure(Exception):
    status_code = 401

  async def _raise_auth_error(headers: dict[str, str]):
    del headers
    raise _AuthFailure('401 unauthorized')

  client._connect_socket = _raise_auth_error  # type: ignore[method-assign]

  with pytest.raises(WebSocketAuthError):
    asyncio.run(client.connect())


def test_subscribe_sends_expected_payload() -> None:
  client = KalshiWebSocketClient(
    ws_url='wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2',
    api_key_id='key-id',
    private_key=_private_key(),
  )
  socket = _FakeSocket()

  async def _connect(headers: dict[str, str]):
    del headers
    return socket

  client._connect_socket = _connect  # type: ignore[method-assign]

  asyncio.run(client.connect())
  subscription_ids = asyncio.run(client.subscribe(['ticker', 'orderbook_delta'], ['KALSHI-TEST-001']))

  assert len(subscription_ids) == 2
  assert len(socket.sent_messages) == 1
  payload = json.loads(socket.sent_messages[0])
  assert payload['cmd'] == 'subscribe'
  assert payload['params']['channels'] == ['ticker', 'orderbook_delta']
  assert payload['params']['market_tickers'] == ['KALSHI-TEST-001']


def test_listen_dispatches_messages_to_callback() -> None:
  messages = [
    json.dumps({'type': 'ticker', 'msg': {'market_ticker': 'KALSHI-TEST-001', 'yes_bid_dollars': '0.41', 'no_bid_dollars': '0.53'}}),
    json.dumps({'type': 'orderbook_snapshot', 'msg': {'ticker': 'KALSHI-TEST-001', 'yes_dollars': [['0.41', '10']], 'no_dollars': [['0.53', '8']], 'seq': 1}}),
  ]
  socket = _FakeSocket(messages)
  received: list[dict[str, object]] = []
  client = KalshiWebSocketClient(
    ws_url='wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2',
    api_key_id='key-id',
    private_key=_private_key(),
    on_message=received.append,
  )

  async def _connect(headers: dict[str, str]):
    del headers
    return socket

  client._connect_socket = _connect  # type: ignore[method-assign]

  asyncio.run(client.connect())
  processed = asyncio.run(client.listen(timeout_sec=0.05, max_events=5))

  assert processed == 2
  assert len(received) == 2
  assert received[0]['type'] == 'ticker'


def test_reconnect_and_resubscribe_restores_subscription_requests() -> None:
  first_socket = _FakeSocket()
  second_socket = _FakeSocket()
  sockets = deque([first_socket, second_socket])
  client = KalshiWebSocketClient(
    ws_url='wss://external-api-ws.demo.kalshi.co/trade-api/ws/v2',
    api_key_id='key-id',
    private_key=_private_key(),
  )

  async def _connect(headers: dict[str, str]):
    del headers
    return sockets.popleft()

  client._connect_socket = _connect  # type: ignore[method-assign]

  asyncio.run(client.connect())
  asyncio.run(client.subscribe(['ticker'], ['KALSHI-TEST-001']))
  resubscribed_ids = asyncio.run(client.reconnect_and_resubscribe())

  assert first_socket.closed is True
  assert len(resubscribed_ids) == 1
  assert len(second_socket.sent_messages) == 1
  payload = json.loads(second_socket.sent_messages[0])
  assert payload['cmd'] == 'subscribe'
  assert payload['params']['channels'] == ['ticker']
