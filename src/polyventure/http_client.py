from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import logging
import random
import time
from urllib.parse import urljoin, urlsplit

import requests

from .auth import build_auth_headers, normalize_signing_path
from .config import Settings
from .kalshi_units import (
  SELF_TRADE_PREVENTION_TYPE,
  KalshiUnitError,
  count_contracts_to_wire,
  group_limit_to_wire,
  leg_to_v2_side,
  outbound_leg_price_dollars,
  price_dollars_to_fp4,
  restore_leg_price_dollars,
  validate_time_in_force,
)
from .types import AccountBucketLimit, AccountLimits, EventSnapshot, MarketSnapshot, OrderbookSnapshot, PairPosition, SubmittedOrder
from .websocket_client import normalize_orderbook_snapshot


LOGGER = logging.getLogger(__name__)


def _parse_trade_time(value: object) -> datetime | None:
  text = str(value or '').strip()
  if not text:
    return None
  try:
    parsed = datetime.fromisoformat(text.replace('Z', '+00:00'))
  except ValueError:
    return None
  return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _trade_count_fp(trade: dict) -> Decimal:
  for key in ('count_fp', 'count'):
    raw = trade.get(key)
    if raw is None:
      continue
    try:
      return Decimal(str(raw))
    except (ArithmeticError, ValueError):
      continue
  return Decimal('0')
AUTH_HEADER_NAMES = frozenset({
  'KALSHI-ACCESS-KEY',
  'KALSHI-ACCESS-SIGNATURE',
  'KALSHI-ACCESS-TIMESTAMP',
})


class KalshiHttpError(RuntimeError):
  def __init__(
    self,
    reason_code: str,
    message: str,
    next_action: str,
    *,
    method: str | None = None,
    endpoint: str | None = None,
    status_code: int | None = None,
  ):
    super().__init__(message)
    self.reason_code = reason_code
    self.next_action = next_action
    self.method = method
    self.endpoint = endpoint
    self.status_code = status_code


def kalshi_error_safe_detail(exc: BaseException) -> dict[str, object]:
  detail: dict[str, object] = {}
  reason_code = str(getattr(exc, 'reason_code', '') or '').strip()
  method = str(getattr(exc, 'method', '') or '').strip().upper()
  endpoint = str(getattr(exc, 'endpoint', '') or '').strip()
  status_code = getattr(exc, 'status_code', None)
  if reason_code:
    detail['reason_code'] = reason_code
  if method:
    detail['kalshi_method'] = method
  if endpoint:
    detail['kalshi_endpoint'] = endpoint
  if status_code is not None:
    try:
      detail['kalshi_status_code'] = int(status_code)
    except (TypeError, ValueError):
      detail['kalshi_status_code'] = str(status_code)
  return detail


def _redact_headers(headers: dict[str, object]) -> dict[str, object]:
  safe_headers = {
    key: value
    for key, value in headers.items()
    if key.upper() not in AUTH_HEADER_NAMES
  }
  if any(key.upper() in AUTH_HEADER_NAMES for key in headers):
    safe_headers['auth_headers'] = '<redacted>'
  return safe_headers


def _backoff_delay(attempt: int, *, include_jitter: bool) -> float:
  delay = 0.25 * (2 ** attempt)
  if include_jitter:
    delay += random.uniform(0, 0.25)
  return delay


def _response_json(response: requests.Response) -> dict:
  try:
    return response.json()
  except ValueError:
    if not getattr(response, 'content', b'') and not getattr(response, 'text', ''):
      return {}
    raise


def _http_error_for_status(
  status_code: int | None,
  *,
  method: str | None = None,
  endpoint: str | None = None,
) -> KalshiHttpError:
  if status_code == 400:
    return KalshiHttpError(
      'bad_request',
      'Kalshi rejected the request shape before execution.',
      'Review the request payload against the expected Kalshi API schema and retry the dry-run path.',
      method=method,
      endpoint=endpoint,
      status_code=status_code,
    )
  if status_code == 401:
    return KalshiHttpError(
      'auth_failed',
      'Kalshi rejected the authenticated request.',
      'Verify the API key id, signing key file, and environment alignment before retrying.',
      method=method,
      endpoint=endpoint,
      status_code=status_code,
    )
  if status_code == 409:
    return KalshiHttpError(
      'conflict',
      'Kalshi reported a request conflict for the current resource state.',
      'Review the order or pair state and retry only after reconciliation.',
      method=method,
      endpoint=endpoint,
      status_code=status_code,
    )
  if status_code == 429:
    return KalshiHttpError(
      'rate_limited',
      'Kalshi rate-limited the request after bounded retries.',
      'Wait for the live account bucket to recover, then retry the dry-run request.',
      method=method,
      endpoint=endpoint,
      status_code=status_code,
    )
  return KalshiHttpError(
    'http_request_failed',
    'Kalshi returned an unexpected HTTP failure.',
    'Review the logged status code and retry only after the remote failure is understood.',
    method=method,
    endpoint=endpoint,
    status_code=status_code,
  )


@dataclass
class KalshiHttpClient:
  settings: Settings
  private_key: object
  session: requests.Session | None = None
  # Per-client request policy. Defaults preserve the historical behaviour
  # (20s timeout, up to 4 network attempts). Interactive callers that must not
  # block the operator deck construct a bounded client (short timeout, single
  # attempt) so a stalled upstream degrades quickly instead of freezing the UI.
  request_timeout: int | None = None
  max_attempts: int = 4

  def __post_init__(self) -> None:
    if self.session is None:
      self.session = requests.Session()

  def _request(self, method: str, endpoint: str, **kwargs: object) -> dict:
    if self.session is None:
      raise RuntimeError('HTTP session is not available.')
    base_path = urlsplit(self.settings.api_base_url).path.rstrip('/')
    signing_path = normalize_signing_path(f'{base_path}{endpoint}')
    auth_headers = build_auth_headers(
      self.private_key,
      self.settings.api_key_id,
      method,
      signing_path,
    )
    extra_headers = kwargs.pop('headers', None) or {}
    headers = {**auth_headers, **extra_headers}
    url = urljoin(f'{self.settings.api_base_url.rstrip('/')}/', endpoint.lstrip('/'))
    default_timeout = self.request_timeout if self.request_timeout is not None else 20
    timeout = int(kwargs.pop('timeout', default_timeout))
    max_attempts = max(1, int(kwargs.pop('max_attempts', self.max_attempts)))

    timeout_attempt = 0
    rate_limit_attempt = 0
    while True:
      started_at = time.perf_counter()
      try:
        response = self.session.request(
          method=method.upper(),
          url=url,
          headers=headers,
          timeout=timeout,
          **kwargs,
        )
        latency_ms = (time.perf_counter() - started_at) * 1000
        status_code = getattr(response, 'status_code', 'unknown')
        LOGGER.info(
          'kalshi_http_request method=%s path=%s status=%s latency_ms=%.2f headers=%s',
          method.upper(),
          endpoint,
          status_code,
          latency_ms,
          _redact_headers(headers),
        )
        response.raise_for_status()
        return _response_json(response)
      except requests.exceptions.SSLError as exc:
        latency_ms = (time.perf_counter() - started_at) * 1000
        LOGGER.warning(
          'kalshi_http_request method=%s path=%s status=trust-failure latency_ms=%.2f headers=%s',
          method.upper(),
          endpoint,
          latency_ms,
          _redact_headers(headers),
        )
        raise KalshiHttpError(
          'trust_failure',
          'TLS trust validation failed before the Kalshi request could complete.',
          'Review the endpoint, local trust store, and environment selection before retrying.',
          method=method.upper(),
          endpoint=endpoint,
        ) from exc
      except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
        latency_ms = (time.perf_counter() - started_at) * 1000
        LOGGER.warning(
          'kalshi_http_request method=%s path=%s status=network-failure latency_ms=%.2f headers=%s',
          method.upper(),
          endpoint,
          latency_ms,
          _redact_headers(headers),
        )
        if timeout_attempt >= max_attempts - 1:
          raise KalshiHttpError(
            'network_timeout',
            'Network connectivity failed after bounded Kalshi retries.',
            'Verify connectivity to the Kalshi endpoint and retry the dry-run request.',
            method=method.upper(),
            endpoint=endpoint,
          ) from exc
        time.sleep(_backoff_delay(timeout_attempt, include_jitter=False))
        timeout_attempt += 1
      except requests.exceptions.HTTPError as exc:
        response = exc.response
        status_code = getattr(response, 'status_code', None)
        latency_ms = (time.perf_counter() - started_at) * 1000
        LOGGER.warning(
          'kalshi_http_request method=%s path=%s status=%s latency_ms=%.2f headers=%s',
          method.upper(),
          endpoint,
          status_code if status_code is not None else 'http-error',
          latency_ms,
          _redact_headers(headers),
        )
        if status_code == 429 and rate_limit_attempt < max_attempts - 1:
          time.sleep(_backoff_delay(rate_limit_attempt, include_jitter=True))
          rate_limit_attempt += 1
          continue
        raise _http_error_for_status(status_code, method=method.upper(), endpoint=endpoint) from exc

  def get_balance(self) -> Decimal:
    payload = self._request('GET', '/portfolio/balance')
    balance_cents = Decimal(str(payload['balance']))
    return balance_cents / Decimal('100')

  def get_account_api_limits(self) -> AccountLimits:
    payload = self._request('GET', '/account/limits')
    return AccountLimits(
      usage_tier=str(payload['usage_tier']),
      read=AccountBucketLimit(
        refill_rate=int(payload['read']['refill_rate']),
        bucket_capacity=int(payload['read']['bucket_capacity']),
      ),
      write=AccountBucketLimit(
        refill_rate=int(payload['write']['refill_rate']),
        bucket_capacity=int(payload['write']['bucket_capacity']),
      ),
    )

  def get_markets(
    self,
    status: str = 'open',
    limit: int = 100,
    cursor: str | None = None,
    min_close_ts: int | None = None,
    max_close_ts: int | None = None,
  ) -> tuple[list[MarketSnapshot], str | None]:
    params: dict[str, object] = {'status': status, 'limit': limit}
    if cursor:
      params['cursor'] = cursor
    if min_close_ts is not None:
      params['min_close_ts'] = min_close_ts
    if max_close_ts is not None:
      params['max_close_ts'] = max_close_ts
    payload = self._request('GET', '/markets', params=params)
    raw_markets = payload.get('markets', [])
    markets = [market_from_payload(item) for item in raw_markets]
    next_cursor = payload.get('cursor') or payload.get('next_cursor')
    return markets, next_cursor if isinstance(next_cursor, str) else None

  def get_market(self, ticker: str) -> MarketSnapshot:
    payload = self._request('GET', f'/markets/{ticker}')
    raw_market = payload.get('market') or payload
    return market_from_payload(raw_market)

  def get_market_readback(self, ticker: str) -> dict:
    payload = self._request('GET', f'/markets/{ticker}')
    raw_market = payload.get('market') or payload
    return raw_market if isinstance(raw_market, dict) else {}

  def get_event(self, event_ticker: str) -> EventSnapshot:
    payload = self._request('GET', f'/events/{event_ticker}')
    return event_from_payload(payload)

  def get_orderbook(self, ticker: str, depth: int = 0) -> OrderbookSnapshot:
    payload = self._request('GET', f'/markets/{ticker}/orderbook', params={'depth': depth})
    raw_orderbook = payload.get('orderbook_fp') or payload.get('orderbook') or payload
    if 'ticker' not in raw_orderbook:
      raw_orderbook = {**raw_orderbook, 'ticker': ticker}
    return normalize_orderbook_snapshot(raw_orderbook)

  def get_recent_trades(
    self,
    ticker: str,
    *,
    window_sec: int,
    now: datetime | None = None,
    max_pages: int = 10,
  ) -> dict[str, object]:
    """Read-only per-side traded flow over the trailing window (authoritative).

    Sums ``count_fp`` by ``taker_outcome_side`` for trades whose ``created_time``
    falls within the trailing ``window_sec``, paginating the ``cursor`` until a
    trade older than the window is seen or the page budget is exhausted. Returns
    ``yes_flow_fp`` / ``no_flow_fp`` (Decimal) and ``trade_count``. The REST
    ``ts_ms`` field is null for this endpoint, so the window filters on
    ``created_time``."""
    cutoff = (now or datetime.now(UTC)) - timedelta(seconds=max(0, int(window_sec)))
    yes_flow = Decimal('0')
    no_flow = Decimal('0')
    trade_count = 0
    cursor: str | None = None
    for _ in range(max(1, int(max_pages))):
      params: dict[str, object] = {'ticker': ticker, 'limit': 100}
      if cursor:
        params['cursor'] = cursor
      payload = self._request('GET', '/markets/trades', params=params)
      trades = payload.get('trades') if isinstance(payload, dict) else None
      if not trades:
        break
      window_passed = False
      for trade in trades:
        created = _parse_trade_time(trade.get('created_time'))
        if created is not None and created < cutoff:
          window_passed = True
          continue
        side = str(trade.get('taker_outcome_side') or '').strip().lower()
        size = _trade_count_fp(trade)
        if side == 'yes':
          yes_flow += size
        elif side == 'no':
          no_flow += size
        trade_count += 1
      next_cursor = payload.get('cursor') or payload.get('next_cursor')
      if window_passed or not isinstance(next_cursor, str) or not next_cursor:
        break
      cursor = next_cursor
    return {'yes_flow_fp': yes_flow, 'no_flow_fp': no_flow, 'trade_count': trade_count}

  def create_order_v2(self, **payload: object) -> SubmittedOrder:
    response = self._request('POST', '/portfolio/events/orders', json=_order_payload_to_v2_wire(payload))
    raw_order = response.get('order') or response
    return submitted_order_from_payload(raw_order)

  def create_orders_v2_batch(self, order_payloads: list[dict[str, object]]) -> list[SubmittedOrder]:
    wire_orders = [_order_payload_to_v2_wire(payload) for payload in order_payloads]
    response = self._request('POST', '/portfolio/events/orders/batched', json={'orders': wire_orders})
    raw_orders: object
    if isinstance(response.get('orders'), list):
      raw_orders = response['orders']
    elif isinstance(response.get('order'), dict):
      raw_orders = [response['order']]
    elif isinstance(response, list):
      raw_orders = response
    else:
      raw_orders = []
    wire_by_client_order_id = {
      str(order.get('client_order_id') or ''): order
      for order in wire_orders
      if order.get('client_order_id')
    }
    return [
      submitted_order_from_payload(_enrich_batch_submitted_order_payload(item, wire_by_client_order_id))
      for item in raw_orders
      if isinstance(item, dict)
    ]

  def cancel_order_v2(self, order_id: str) -> dict[str, object]:
    response = self._request('DELETE', f'/portfolio/events/orders/{order_id}')
    return response.get('cancel') or response

  def cancel_orders_v2_batch(self, order_ids: list[str], *, subaccount: int = 0) -> list[dict[str, object]]:
    response = self._request(
      'DELETE',
      '/portfolio/events/orders/batched',
      json={
        'orders': [
          {'order_id': str(order_id), 'subaccount': int(subaccount), 'exchange_index': 0}
          for order_id in order_ids
          if str(order_id or '').strip()
        ],
      },
    )
    raw_cancels = response.get('orders') or response.get('cancels') or response.get('cancel') or []
    if isinstance(raw_cancels, dict):
      return [raw_cancels]
    return [item for item in raw_cancels if isinstance(item, dict)]

  def get_order(self, order_id: str) -> SubmittedOrder:
    response = self._request('GET', f'/portfolio/orders/{order_id}')
    raw_order = response.get('order') or response
    return submitted_order_from_payload(raw_order)

  def get_positions(
    self,
    *,
    ticker: str | None = None,
    event_ticker: str | None = None,
    count_filter: str | None = None,
  ) -> list[PairPosition]:
    params = {
      key: value
      for key, value in {
        'ticker': ticker,
        'event_ticker': event_ticker,
        'count_filter': count_filter,
      }.items()
      if value not in (None, '')
    }
    response = self._request('GET', '/portfolio/positions', params=params or None)
    raw_positions = response.get('positions') or []
    return [pair_position_from_payload(item) for item in raw_positions]

  def list_orders(
    self,
    *,
    ticker: str,
    status: str = 'resting',
    limit: int = 100,
  ) -> list[SubmittedOrder]:
    orders: list[SubmittedOrder] = []
    cursor: str | None = None
    while True:
      params: dict[str, object] = {'ticker': ticker, 'status': status, 'limit': limit}
      if cursor:
        params['cursor'] = cursor
      response = self._request('GET', '/portfolio/orders', params=params)
      raw_orders: object
      if isinstance(response.get('orders'), list):
        raw_orders = response.get('orders')
      elif isinstance(response.get('order'), dict):
        raw_orders = [response.get('order')]
      elif isinstance(response, list):
        raw_orders = response
      else:
        raw_orders = []
      orders.extend(
        submitted_order_from_payload(item)
        for item in raw_orders
        if isinstance(item, dict)
      )
      next_cursor = response.get('cursor') or response.get('next_cursor')
      if not isinstance(next_cursor, str) or not next_cursor:
        break
      cursor = next_cursor
    return orders

  def list_orders_for_batch_readback(
    self,
    *,
    ticker: str | None = None,
    status: str | None = None,
    limit: int = 100,
    min_ts: int | None = None,
    max_ts: int | None = None,
    subaccount: int | None = None,
    max_pages: int = 3,
  ) -> list[dict[str, object]]:
    orders: list[dict[str, object]] = []
    cursor: str | None = None
    page_count = 0
    while page_count < max(1, max_pages):
      params: dict[str, object] = {'limit': limit}
      if ticker:
        params['ticker'] = ticker
      if status:
        params['status'] = status
      if min_ts is not None:
        params['min_ts'] = int(min_ts)
      if max_ts is not None:
        params['max_ts'] = int(max_ts)
      if subaccount is not None:
        params['subaccount'] = int(subaccount)
      if cursor:
        params['cursor'] = cursor
      response = self._request('GET', '/portfolio/orders', params=params)
      raw_orders: object
      if isinstance(response.get('orders'), list):
        raw_orders = response.get('orders')
      elif isinstance(response.get('order'), dict):
        raw_orders = [response.get('order')]
      elif isinstance(response, list):
        raw_orders = response
      else:
        raw_orders = []
      orders.extend(item for item in raw_orders if isinstance(item, dict))
      page_count += 1
      next_cursor = response.get('cursor') or response.get('next_cursor')
      if not isinstance(next_cursor, str) or not next_cursor:
        break
      cursor = next_cursor
    return orders

  def get_fills(self, **params: object) -> list[dict]:
    clean_params = {key: value for key, value in params.items() if value not in (None, '')}
    response = self._request('GET', '/portfolio/fills', params=clean_params or None)
    raw_fills = response.get('fills') or response.get('trades') or []
    return [item for item in raw_fills if isinstance(item, dict)]

  def create_order_group(self, contracts_limit_fp: Decimal, subaccount: int = 0) -> str:
    response = self._request(
      'POST',
      '/portfolio/order_groups/create',
      json={
        'contracts_limit_fp': group_limit_to_wire(contracts_limit_fp),
        'subaccount': subaccount,
      },
    )
    group = response.get('order_group') or response
    return str(group.get('order_group_id') or group.get('id') or '')


def _order_payload_to_v2_wire(payload: dict[str, object]) -> dict[str, object]:
  """Build the complete Kalshi V2 order body from the domain-natural caller kwargs.

  Caller passes ``side='yes'``/``'no'``, ``yes_price``/``no_price`` dollar Decimals,
  and a Decimal ``count``. This boundary: maps the leg to the YES-book ``bid``/``ask``,
  converts the NO price to its YES-book complement, formats price as a 4-decimal dollar
  string and count as Count-FP, adds the required ``self_trade_prevention_type``, and
  drops legacy ``action``/``type`` fields. Fails closed (``KalshiUnitError``) on any
  value not exactly representable in Kalshi V2 units."""
  leg = str(payload['side'])
  leg_price_dollars = (
    payload.get('yes_price') if leg == 'yes' else payload.get('no_price')
  )
  wire: dict[str, object] = {
    'ticker': payload['ticker'],
    'side': leg_to_v2_side(leg),
    'price': price_dollars_to_fp4(outbound_leg_price_dollars(leg, leg_price_dollars)),
    'count': count_contracts_to_wire(payload['count']),
    'time_in_force': validate_time_in_force(str(payload['time_in_force'])),
    'self_trade_prevention_type': SELF_TRADE_PREVENTION_TYPE,
    'client_order_id': str(payload['client_order_id']),
    'post_only': bool(payload.get('post_only', False)),
    'subaccount': int(payload.get('subaccount', 0)),
  }
  if payload.get('cancel_order_on_pause') is not None:
    wire['cancel_order_on_pause'] = bool(payload['cancel_order_on_pause'])
  if payload.get('order_group_id') is not None:
    wire['order_group_id'] = str(payload['order_group_id'])
  return wire


def _enrich_batch_submitted_order_payload(
  raw_order: dict[str, object],
  wire_by_client_order_id: dict[str, dict[str, object]],
) -> dict[str, object]:
  enriched = dict(raw_order)
  client_order_id = str(enriched.get('client_order_id') or '')
  wire = wire_by_client_order_id.get(client_order_id)
  if not wire:
    return enriched
  for key in ('ticker', 'side', 'price', 'count', 'cancel_order_on_pause', 'subaccount'):
    if enriched.get(key) in (None, '') and wire.get(key) not in (None, ''):
      enriched[key] = wire[key]
  if enriched.get('initial_count_fp') in (None, '') and wire.get('count') not in (None, ''):
    enriched['initial_count_fp'] = wire['count']
  if enriched.get('remaining_count_fp') in (None, '') and wire.get('count') not in (None, ''):
    status = str(enriched.get('status') or '').strip().lower()
    enriched['remaining_count_fp'] = '0.00' if status in {'executed', 'filled'} else wire['count']
  if enriched.get('fill_count_fp') in (None, '') and wire.get('count') not in (None, ''):
    status = str(enriched.get('status') or '').strip().lower()
    if status in {'executed', 'filled'}:
      enriched['fill_count_fp'] = wire['count']
  return enriched


def _to_decimal(value: object) -> Decimal:
  if value in (None, ''):
    return Decimal('0')
  return Decimal(str(value))


def _optional_decimal(value: object) -> Decimal | None:
  if value in (None, ''):
    return None
  return Decimal(str(value))


def _first_present(payload: dict, *keys: str) -> object:
  for key in keys:
    value = payload.get(key)
    if value not in (None, ''):
      return value
  return None


def _parse_close_time(value: object) -> datetime | None:
  if not value:
    return None
  text = str(value)
  if text.endswith('Z'):
    text = text[:-1] + '+00:00'
  return datetime.fromisoformat(text).astimezone(UTC)


def market_from_payload(payload: dict) -> MarketSnapshot:
  return MarketSnapshot(
    ticker=str(payload.get('ticker', '')),
    title=payload.get('title'),
    close_time=_parse_close_time(
      payload.get('close_time') or payload.get('expiration_time')
    ),
    status=str(payload.get('status', '')),
    yes_bid_dollars=_to_decimal(payload.get('yes_bid_dollars') or payload.get('yes_bid')),
    no_bid_dollars=_to_decimal(payload.get('no_bid_dollars') or payload.get('no_bid')),
    volume_24h_fp=_to_decimal(payload.get('volume_24h_fp') or payload.get('volume_24h')),
    open_interest_fp=_to_decimal(
      payload.get('open_interest_fp') or payload.get('open_interest')
    ),
    event_ticker=str(payload.get('event_ticker') or ''),
    series_ticker=str(payload.get('series_ticker') or ''),
    category=str(payload.get('category') or ''),
    yes_sub_title=str(payload.get('yes_sub_title') or ''),
    no_sub_title=str(payload.get('no_sub_title') or ''),
    open_time=_parse_close_time(payload.get('open_time')),
    latest_expiration_time=_parse_close_time(payload.get('latest_expiration_time')),
    yes_ask_dollars=_to_decimal(payload.get('yes_ask_dollars') or payload.get('yes_ask')),
    no_ask_dollars=_to_decimal(payload.get('no_ask_dollars') or payload.get('no_ask')),
    yes_bid_size_fp=_to_decimal(payload.get('yes_bid_size_fp')),
    yes_ask_size_fp=_to_decimal(payload.get('yes_ask_size_fp')),
    volume_fp=_to_decimal(payload.get('volume_fp') or payload.get('volume')),
    can_close_early=bool(payload.get('can_close_early', False)),
    rules_primary=str(payload.get('rules_primary') or ''),
    rules_secondary=str(payload.get('rules_secondary') or ''),
    price_level_structure=str(payload.get('price_level_structure') or ''),
    floor_strike=str(payload.get('floor_strike') or ''),
    cap_strike=str(payload.get('cap_strike') or ''),
    mve_collection_ticker=str(payload.get('mve_collection_ticker') or ''),
    mve_selected_legs=tuple(str(item) for item in payload.get('mve_selected_legs') or ()),
    price_ranges=tuple(
      (
        _to_decimal(item.get('min_price') or item.get('min')),
        _to_decimal(item.get('max_price') or item.get('max')),
      )
      for item in payload.get('price_ranges', [])
    ),
  )


def event_from_payload(payload: dict) -> EventSnapshot:
  event_payload = payload.get('event') if isinstance(payload.get('event'), dict) else payload
  raw_markets = payload.get('markets')
  if raw_markets is None and isinstance(event_payload, dict):
    raw_markets = event_payload.get('markets')
  markets = tuple(
    market_from_payload(item)
    for item in (raw_markets or ())
    if isinstance(item, dict)
  )
  mutually_exclusive = event_payload.get('mutually_exclusive') if isinstance(event_payload, dict) else None
  return EventSnapshot(
    event_ticker=str(event_payload.get('event_ticker') or event_payload.get('ticker') or ''),
    series_ticker=str(event_payload.get('series_ticker') or ''),
    category=str(event_payload.get('category') or ''),
    title=str(event_payload.get('title') or ''),
    mutually_exclusive=bool(mutually_exclusive) if mutually_exclusive is not None else None,
    markets=markets,
  )


def submitted_order_from_payload(payload: dict) -> SubmittedOrder:
  contract_count_raw = _first_present(payload, 'initial_count_fp', 'contract_count', 'count')
  remaining_count_raw = _first_present(payload, 'remaining_count_fp', 'remaining_count', 'remaining')
  fill_count_raw = _first_present(payload, 'fill_count_fp', 'fill_count')
  contract_count = _to_decimal(contract_count_raw if contract_count_raw is not None else '0')
  remaining_count = _to_decimal(remaining_count_raw if remaining_count_raw is not None else '0')
  if fill_count_raw is not None:
    fill_count = _to_decimal(fill_count_raw)
  elif contract_count_raw is not None and remaining_count_raw is not None:
    fill_count = max(Decimal('0'), contract_count - remaining_count)
  else:
    fill_count = Decimal('0')
  explicit_status = payload.get('status')
  if explicit_status:
    status = str(explicit_status)
  elif contract_count > 0 and remaining_count == 0:
    status = 'filled'
  else:
    status = 'resting'
  created_at_raw = payload.get('created_at')
  if not created_at_raw and payload.get('ts_ms'):
    from datetime import timezone
    created_at_raw = datetime.fromtimestamp(int(payload['ts_ms']) / 1000, tz=timezone.utc).isoformat()
  side = str(payload.get('side') or '')
  reported_price = _to_decimal(
    _first_present(
      payload,
      'yes_price_dollars',
      'no_price_dollars',
      'average_fill_price',
      'price_dollars',
      'price',
    ) or '0'
  )
  price_keys = {
    key for key in ('yes_price_dollars', 'no_price_dollars')
    if payload.get(key) not in (None, '')
  }
  if side in {'no', 'ask'} and 'no_price_dollars' not in price_keys and reported_price:
    reported_price = restore_leg_price_dollars('no', reported_price)
    if side == 'ask':
      side = 'no'
  elif side == 'ask':
    side = 'no'
  elif side == 'bid':
    side = 'yes'
  return SubmittedOrder(
    order_id=str(payload.get('order_id') or payload.get('id') or ''),
    client_order_id=str(payload.get('client_order_id') or ''),
    ticker=str(payload.get('ticker') or ''),
    side=side,
    price_dollars=reported_price,
    contract_count=contract_count,
    remaining_count=remaining_count,
    fill_count=fill_count,
    status=status,
    created_at=_parse_close_time(created_at_raw) or datetime.now(UTC),
    cancel_order_on_pause=bool(payload.get('cancel_order_on_pause', False)),
    subaccount=int(payload.get('subaccount') or 0),
    reduced_by=_to_decimal(payload.get('reduced_by') or '0'),
  )


def pair_position_from_payload(payload: dict) -> PairPosition:
  position_fp = _to_decimal(payload.get('position_fp') or '0')
  side = str(payload.get('side') or '')
  contract_count_raw = _first_present(payload, 'contract_count_fp', 'contract_count', 'count')
  contract_count = _to_decimal(contract_count_raw if contract_count_raw is not None else abs(position_fp))
  if not side and position_fp:
    side = 'yes' if position_fp > 0 else 'no'
  return PairPosition(
    ticker=str(payload.get('ticker') or ''),
    side=side,
    contract_count=contract_count,
    average_price_dollars=_to_decimal(
      _first_present(payload, 'average_price_dollars', 'average_price', 'avg_price_dollars', 'avg_price') or '0'
    ),
    realized_pnl_dollars=_to_decimal(payload.get('realized_pnl_dollars') or payload.get('realized_pnl') or '0'),
    fees_dollars=_to_decimal(payload.get('fees_dollars') or payload.get('fees') or '0'),
    market_exposure_dollars=_to_decimal(payload.get('market_exposure_dollars') or payload.get('market_exposure') or '0'),
    position_fp=position_fp,
  )
