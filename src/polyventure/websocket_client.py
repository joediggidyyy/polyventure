from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
import json
import logging
import time
from typing import Any
from uuid import uuid4

from .auth import RSAPrivateKey, create_signature
from .types import OrderbookSnapshot


def _to_decimal(value: object) -> Decimal:
  return Decimal(str(value))


def _normalize_levels(raw_levels: list[list[object]] | tuple[tuple[object, object], ...]) -> tuple[tuple[Decimal, Decimal], ...]:
  levels = tuple((
    _to_decimal(price),
    _to_decimal(size),
  ) for price, size in raw_levels)
  return tuple(sorted(levels, key=lambda item: item[0]))


def _best_bid(levels: tuple[tuple[Decimal, Decimal], ...]) -> Decimal | None:
  if not levels:
    return None
  return levels[-1][0]


def _implied_ask(other_best_bid: Decimal | None) -> Decimal | None:
  if other_best_bid is None:
    return None
  return Decimal('1') - other_best_bid


def normalize_orderbook_snapshot(payload: dict[str, Any]) -> OrderbookSnapshot:
  yes_levels = payload.get('yes_bids') or payload.get('yes_dollars') or []
  no_levels = payload.get('no_bids') or payload.get('no_dollars') or []
  yes_bids = _normalize_levels(yes_levels)
  no_bids = _normalize_levels(no_levels)
  best_yes_bid = _best_bid(yes_bids)
  best_no_bid = _best_bid(no_bids)
  captured_at = payload.get('captured_at')
  if isinstance(captured_at, datetime):
    timestamp = captured_at.astimezone(UTC)
  else:
    timestamp = datetime.now(UTC)
  return OrderbookSnapshot(
    ticker=str(payload.get('ticker', '')),
    yes_bids=yes_bids,
    no_bids=no_bids,
    best_yes_bid=best_yes_bid,
    best_no_bid=best_no_bid,
    best_yes_ask_implied=_implied_ask(best_no_bid),
    best_no_ask_implied=_implied_ask(best_yes_bid),
    captured_at=timestamp,
    last_seq=payload.get('seq'),
  )


def apply_orderbook_delta(
  snapshot: OrderbookSnapshot,
  delta: dict[str, Any],
) -> OrderbookSnapshot:
  next_seq = delta.get('seq')
  if snapshot.last_seq is not None and next_seq != snapshot.last_seq + 1:
    raise ValueError('Orderbook sequence gap detected; rebuild the snapshot before continuing.')

  side = str(delta.get('side', '')).lower()
  price = _to_decimal(delta['price'])
  size = _to_decimal(delta['size'])

  if side not in {'yes', 'no'}:
    raise ValueError('Delta side must be yes or no.')

  target_levels = list(snapshot.yes_bids if side == 'yes' else snapshot.no_bids)
  target_levels = [level for level in target_levels if level[0] != price]
  if size > 0:
    target_levels.append((price, size))
  target_levels = list(sorted(target_levels, key=lambda item: item[0]))

  updated = replace(
    snapshot,
    yes_bids=tuple(target_levels) if side == 'yes' else snapshot.yes_bids,
    no_bids=tuple(target_levels) if side == 'no' else snapshot.no_bids,
    captured_at=delta.get('captured_at', snapshot.captured_at),
    last_seq=next_seq,
  )
  best_yes_bid = _best_bid(updated.yes_bids)
  best_no_bid = _best_bid(updated.no_bids)
  return replace(
    updated,
    best_yes_bid=best_yes_bid,
    best_no_bid=best_no_bid,
    best_yes_ask_implied=_implied_ask(best_no_bid),
    best_no_ask_implied=_implied_ask(best_yes_bid),
  )


class WebSocketError(RuntimeError):
  """Base websocket error for Polyventure websocket operations."""


class WebSocketAuthError(WebSocketError):
  """Authentication failed for websocket connection."""


class WebSocketConnectionError(WebSocketError):
  """Connection establishment or lifecycle failure."""


class WebSocketTimeout(WebSocketError):
  """Websocket operation timed out."""


class WebSocketServiceUnavailableError(WebSocketError):
  """WebSocket endpoint returned an HTTP 5xx service error."""


class KalshiWebSocketClient:
  def __init__(
    self,
    *,
    ws_url: str,
    api_key_id: str,
    private_key: RSAPrivateKey,
    logger: logging.Logger | None = None,
    on_message: Any | None = None,
  ) -> None:
    self.ws_url = str(ws_url or '').strip()
    self.api_key_id = str(api_key_id or '').strip()
    self.private_key = private_key
    self.logger = logger or logging.getLogger(__name__)
    self._on_message = on_message
    self._socket: Any | None = None
    self._next_subscription_id = 1
    self._subscription_requests: list[dict[str, Any]] = []
    self._subscriptions: dict[int, dict[str, Any]] = {}

  @property
  def connected(self) -> bool:
    return self._socket is not None

  def _generate_websocket_headers(self, *, timestamp_ms: str | None = None) -> dict[str, str]:
    ts = timestamp_ms or str(int(time.time() * 1000))
    signature = create_signature(self.private_key, ts, 'GET', '/trade-api/ws/v2')
    return {
      'KALSHI-ACCESS-KEY': self.api_key_id,
      'KALSHI-ACCESS-SIGNATURE': signature,
      'KALSHI-ACCESS-TIMESTAMP': ts,
    }

  def _map_connection_error(self, exc: Exception) -> WebSocketError:
    response = getattr(exc, 'response', None)
    status_code = (
      getattr(exc, 'status_code', None)
      or getattr(exc, 'status', None)
      or (getattr(response, 'status_code', None) if response is not None else None)
    )
    message = str(exc)
    if status_code in {401, 403} or '401' in message or '403' in message:
      return WebSocketAuthError('WebSocket authentication failed.')
    if isinstance(status_code, int) and status_code >= 500:
      return WebSocketServiceUnavailableError('WebSocket endpoint returned a service error.')
    return WebSocketConnectionError('WebSocket connection failed.')

  async def _connect_socket(self, headers: dict[str, str]) -> Any:
    try:
      import websockets  # type: ignore
    except ImportError as exc:
      raise WebSocketConnectionError(
        'Python package "websockets" is required for websocket connectivity.'
      ) from exc
    return await websockets.connect(self.ws_url, additional_headers=headers)

  async def connect(self) -> None:
    if self.connected:
      return
    headers = self._generate_websocket_headers()
    try:
      self._socket = await self._connect_socket(headers)
      self.logger.info('websocket_connected endpoint_shape=%s', self.ws_url)
    except Exception as exc:  # pragma: no cover - mapping tested directly
      mapped = self._map_connection_error(exc)
      self.logger.warning('websocket_connect_failed code=%s', mapped.__class__.__name__)
      raise mapped from exc

  async def disconnect(self) -> None:
    if self._socket is None:
      return
    socket = self._socket
    self._socket = None
    close = getattr(socket, 'close', None)
    if callable(close):
      maybe_coro = close()
      if asyncio.iscoroutine(maybe_coro):
        await maybe_coro

  async def subscribe(self, channels: list[str], market_tickers: list[str] | None = None) -> list[int]:
    if not self.connected:
      raise WebSocketConnectionError('WebSocket is not connected.')
    tickers = list(market_tickers or [])
    payload = {
      'id': self._next_subscription_id,
      'cmd': 'subscribe',
      'params': {
        'channels': list(channels),
        'market_tickers': tickers,
      },
    }
    self._next_subscription_id += 1
    await self._socket.send(json.dumps(payload))
    self._subscription_requests.append({'channels': list(channels), 'market_tickers': tickers})
    subscription_ids: list[int] = []
    for channel in channels:
      sid = self._next_subscription_id
      self._next_subscription_id += 1
      self._subscriptions[sid] = {'channel': channel, 'market_tickers': tuple(tickers)}
      subscription_ids.append(sid)
    self.logger.info('websocket_subscribe channels=%s market_count=%d', channels, len(tickers))
    return subscription_ids

  async def unsubscribe(self, subscription_ids: list[int]) -> None:
    if not self.connected:
      raise WebSocketConnectionError('WebSocket is not connected.')
    for sid in subscription_ids:
      self._subscriptions.pop(sid, None)
    if subscription_ids:
      payload = {
        'id': self._next_subscription_id,
        'cmd': 'unsubscribe',
        'params': {'sids': subscription_ids},
      }
      self._next_subscription_id += 1
      await self._socket.send(json.dumps(payload))

  async def _recv_with_timeout(self, timeout_sec: float) -> str:
    if not self.connected:
      raise WebSocketConnectionError('WebSocket is not connected.')
    try:
      return await asyncio.wait_for(self._socket.recv(), timeout=timeout_sec)
    except TimeoutError as exc:
      raise WebSocketTimeout('Timed out waiting for websocket message.') from exc

  async def listen(self, timeout_sec: float = 300.0, *, max_events: int = 200) -> int:
    if not self.connected:
      raise WebSocketConnectionError('WebSocket is not connected.')
    started = time.monotonic()
    processed = 0
    while processed < max_events:
      remaining = timeout_sec - (time.monotonic() - started)
      if remaining <= 0:
        break
      try:
        raw_message = await self._recv_with_timeout(remaining)
      except WebSocketTimeout:
        break
      try:
        payload = json.loads(raw_message)
      except json.JSONDecodeError:
        self.logger.warning('websocket_message_decode_failed code=invalid_json')
        continue
      if isinstance(payload, dict) and str(payload.get('type', '')).lower() == 'error':
        error_code = str((payload.get('msg') or {}).get('code', 'unknown'))
        self.logger.warning('websocket_message_error code=%s', error_code)
        if error_code == '9':
          raise WebSocketAuthError('WebSocket authentication required or expired.')
      if callable(self._on_message):
        try:
          self._on_message(payload)
        except Exception:
          self.logger.warning('websocket_handler_failed code=on_message_exception')
      processed += 1
    return processed

  async def reconnect_and_resubscribe(self) -> list[int]:
    previous_requests = list(self._subscription_requests)
    await self.disconnect()
    await self.connect()
    self._subscriptions.clear()
    self._subscription_requests.clear()
    resubscribed_ids: list[int] = []
    for request in previous_requests:
      resubscribed_ids.extend(
        await self.subscribe(
          list(request['channels']),
          list(request['market_tickers']),
        )
      )
    return resubscribed_ids

  async def hydrate_orderbooks(
    self,
    *,
    channels: list[str],
    market_tickers: list[str],
    timeout_sec: float = 2.0,
    max_events: int = 200,
  ) -> int:
    await self.connect()
    await self.subscribe(channels, market_tickers)
    try:
      return await self.listen(timeout_sec=timeout_sec, max_events=max_events)
    finally:
      await self.disconnect()


class SimulatedWebSocketClient:
  def __init__(
    self,
    *,
    operation_lane: str = 'sandbox',
    active_websocket_url: str | None = None,
  ) -> None:
    self.connected = False
    self._next_subscription_id = 1
    self._subscriptions: dict[int, dict[str, Any]] = {}
    self._events: deque[dict[str, Any]] = deque()
    self.operation_lane = str(operation_lane or 'sandbox').strip().lower() or 'sandbox'
    self.active_websocket_url = str(active_websocket_url or '').strip() or None
    self.lane_session_id = self._new_lane_session_id()

  def _new_lane_session_id(self) -> str:
    timestamp = datetime.now(UTC).strftime('%Y%m%dT%H%M%SZ')
    return '{lane}-{timestamp}-{suffix}'.format(
      lane=self.operation_lane,
      timestamp=timestamp,
      suffix=uuid4().hex[:8],
    )

  def connect(self) -> None:
    self.connected = True

  def disconnect(self) -> None:
    self.connected = False

  def subscribe(self, channels: list[str], market_tickers: list[str]) -> list[int]:
    subscription_ids: list[int] = []
    for channel in channels:
      sid = self._next_subscription_id
      self._next_subscription_id += 1
      self._subscriptions[sid] = {
        'channel': channel,
        'market_tickers': tuple(market_tickers),
        'operation_lane': self.operation_lane,
        'lane_session_id': self.lane_session_id,
        'active_websocket_url': self.active_websocket_url,
      }
      subscription_ids.append(sid)
    return subscription_ids

  def unsubscribe(self, subscription_ids: list[int]) -> None:
    for sid in subscription_ids:
      self._subscriptions.pop(sid, None)

  def reconnect_and_resubscribe(self) -> list[int]:
    previous = list(self._subscriptions.values())
    self.disconnect()
    self.connect()
    self.lane_session_id = self._new_lane_session_id()
    self._subscriptions.clear()
    subscription_ids: list[int] = []
    for subscription in previous:
      subscription_ids.extend(
        self.subscribe(
          [str(subscription['channel'])],
          list(subscription['market_tickers']),
        )
      )
    return subscription_ids

  def switch_lane(self, operation_lane: str, *, active_websocket_url: str | None = None) -> None:
    self.operation_lane = str(operation_lane or 'sandbox').strip().lower() or 'sandbox'
    self.active_websocket_url = str(active_websocket_url or '').strip() or None
    self.lane_session_id = self._new_lane_session_id()
    self._subscriptions.clear()

  def subscription_snapshot(self) -> tuple[dict[str, Any], ...]:
    return tuple(self._subscriptions[sid] for sid in sorted(self._subscriptions))

  def queue_event(self, event: dict[str, Any]) -> None:
    self._events.append(event)

  def next_event(self, timeout_sec: float) -> dict[str, Any]:
    if not self._events:
      raise TimeoutError(
        'No websocket event was available within {timeout_sec:.2f}s.'.format(
          timeout_sec=timeout_sec,
        )
      )
    return self._events.popleft()
