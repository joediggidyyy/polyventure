from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import logging

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
import pytest
import requests

from polyventure.config import Settings
from polyventure.execution import simulate_cancel_pair, simulate_partial_fill, simulate_submit_pair
from polyventure.http_client import KalshiHttpClient, KalshiHttpError
from polyventure.market_data import compute_seconds_to_close, derive_implied_asks, enrich_with_orderbook, fetch_open_markets
from polyventure.service import run_scan_once, run_service_once
from polyventure.strategy import build_pair_order_plan
from polyventure.types import AccountBucketLimit, AccountLimits, CandidatePair, MarketSnapshot
from polyventure.websocket_client import SimulatedWebSocketClient


class FakeDemoClient:
  def __init__(self, settings: Settings, private_key: object):
    self.settings = settings
    self.private_key = private_key
    now = datetime.now(UTC)
    self.markets = {
      'KALSHI-DEMO-INTEGRATION-001': MarketSnapshot(
        ticker='KALSHI-DEMO-INTEGRATION-001',
        title='Demo integration market',
        close_time=now + timedelta(seconds=240),
        status='open',
        yes_bid_dollars=Decimal('0.33'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('120.00'),
        open_interest_fp=Decimal('75.00'),
      )
    }

  def get_balance(self) -> Decimal:
    return Decimal('250.00')

  def get_account_api_limits(self) -> AccountLimits:
    return AccountLimits(
      usage_tier='demo-tier',
      read=AccountBucketLimit(refill_rate=30, bucket_capacity=60),
      write=AccountBucketLimit(refill_rate=10, bucket_capacity=20),
    )

  def get_markets(
    self,
    status: str = 'open',
    limit: int = 100,
    cursor: str | None = None,
    min_close_ts: int | None = None,
    max_close_ts: int | None = None,
  ) -> tuple[list[MarketSnapshot], str | None]:
    del status, limit, cursor, min_close_ts, max_close_ts
    return list(self.markets.values()), None

  def get_market(self, ticker: str) -> MarketSnapshot:
    return self.markets[ticker]

  def get_orderbook(self, ticker: str, depth: int = 0):
    del depth
    from polyventure.websocket_client import normalize_orderbook_snapshot

    market = self.markets[ticker]
    return normalize_orderbook_snapshot(
      {
        'ticker': ticker,
        'yes_dollars': [[str(market.yes_bid_dollars - Decimal('0.02')), '8'], [str(market.yes_bid_dollars), '13']],
        'no_dollars': [[str(market.no_bid_dollars - Decimal('0.02')), '7'], [str(market.no_bid_dollars), '12']],
        'seq': 4,
      }
    )


class PaginatedMarketClient:
  def __init__(self) -> None:
    now = datetime.now(UTC)
    self.calls: list[dict[str, object]] = []
    self.pages: dict[str | None, tuple[list[MarketSnapshot], str | None]] = {
      None: (
        [
          MarketSnapshot(
            ticker='KALSHI-PAGED-001',
            title='Paged market 1',
            close_time=now + timedelta(seconds=240),
            status='open',
            yes_bid_dollars=Decimal('0.31'),
            no_bid_dollars=Decimal('0.39'),
            volume_24h_fp=Decimal('100.00'),
            open_interest_fp=Decimal('50.00'),
          ),
          MarketSnapshot(
            ticker='KALSHI-PAGED-002',
            title='Paged market 2',
            close_time=now + timedelta(seconds=240),
            status='open',
            yes_bid_dollars=Decimal('0.32'),
            no_bid_dollars=Decimal('0.39'),
            volume_24h_fp=Decimal('100.00'),
            open_interest_fp=Decimal('50.00'),
          ),
        ],
        'page-2',
      ),
      'page-2': (
        [
          MarketSnapshot(
            ticker='KALSHI-PAGED-003',
            title='Paged market 3',
            close_time=now + timedelta(seconds=240),
            status='open',
            yes_bid_dollars=Decimal('0.33'),
            no_bid_dollars=Decimal('0.39'),
            volume_24h_fp=Decimal('100.00'),
            open_interest_fp=Decimal('50.00'),
          ),
          MarketSnapshot(
            ticker='KALSHI-PAGED-004',
            title='Paged market 4',
            close_time=now + timedelta(seconds=240),
            status='open',
            yes_bid_dollars=Decimal('0.34'),
            no_bid_dollars=Decimal('0.39'),
            volume_24h_fp=Decimal('100.00'),
            open_interest_fp=Decimal('50.00'),
          ),
        ],
        'page-3',
      ),
      'page-3': (
        [
          MarketSnapshot(
            ticker='KALSHI-PAGED-005',
            title='Paged market 5',
            close_time=now + timedelta(seconds=240),
            status='open',
            yes_bid_dollars=Decimal('0.35'),
            no_bid_dollars=Decimal('0.39'),
            volume_24h_fp=Decimal('100.00'),
            open_interest_fp=Decimal('50.00'),
          )
        ],
        None,
      ),
    }

  def get_markets(
    self,
    status: str = 'open',
    limit: int = 100,
    cursor: str | None = None,
    min_close_ts: int | None = None,
    max_close_ts: int | None = None,
  ) -> tuple[list[MarketSnapshot], str | None]:
    del min_close_ts, max_close_ts
    self.calls.append({'status': status, 'limit': limit, 'cursor': cursor})
    return self.pages[cursor]


def _write_private_key(path: Path) -> None:
  key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
  pem = key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption(),
  )
  path.write_bytes(pem)


def _settings(tmp_path: Path) -> Settings:
  private_key_path = tmp_path / 'demo_integration.pem'
  _write_private_key(private_key_path)
  return Settings(
    kalshi_env='demo',
    api_key_id='demo-key-id',
    private_key_file=str(private_key_path),
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.co/trade-api/v2',
    websocket_url='wss://demo-api.kalshi.co/trade-api/ws/v2',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=0.03,
    fee_reserve_dollars=0.02,
    min_profit_dollars=0.01,
    max_pair_contracts=10.0,
    max_open_pairs=20,
    max_unhedged_sec=5,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path=str(tmp_path / 'demo_integration.sqlite3'),
  )


def _candidate() -> CandidatePair:
  return CandidatePair(
    ticker='KALSHI-DEMO-INTEGRATION-PAIR',
    seconds_to_close=240,
    target_yes_bid=Decimal('0.33'),
    target_no_bid=Decimal('0.39'),
    edge_gross_per_contract=Decimal('0.28'),
    fee_reserve_per_contract=Decimal('0.02'),
    edge_net_per_contract=Decimal('0.26'),
    asymmetry=Decimal('0.06'),
    max_size_contracts=Decimal('5'),
    ranking_key=(Decimal('0.26'), Decimal('0.06'), Decimal('120'), Decimal('75'), -240),
  )


def test_demo_authenticated_balance_and_market_scan(monkeypatch, tmp_path: Path) -> None:
  settings = _settings(tmp_path)
  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeDemoClient)

  payload = run_scan_once(settings=settings)

  assert payload['settings']['kalshi_env'] == 'demo'
  assert payload['balance_dollars'] == '250.00'
  assert payload['market_count'] == 1
  assert payload['candidate_count'] == 1


def test_demo_runtime_cycle_persists_limits_and_plan(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  settings = _settings(tmp_path)
  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeDemoClient)

  payload = run_service_once(settings=settings)

  assert payload['planned_pair_count'] == 1
  assert payload['account_limits']['usage_tier'] == 'demo-tier'
  assert payload['state_db_path_tail'] == 'demo_integration.sqlite3'
  assert payload['dry_run_explanation'] == 'No order was submitted.'
  assert payload['orderbook_enrichment_count'] == 1


def test_demo_pair_submission_uses_unique_client_order_ids(tmp_path: Path) -> None:
  settings = _settings(tmp_path)
  plan = build_pair_order_plan(_candidate(), Decimal('50.00'), settings)
  yes_order, no_order = simulate_submit_pair(
    plan,
    submitted_at=datetime(2026, 5, 5, 7, 0, tzinfo=UTC),
  )

  assert plan.yes_client_order_id != plan.no_client_order_id
  assert yes_order.order_id != no_order.order_id
  assert yes_order.client_order_id == plan.yes_client_order_id
  assert no_order.client_order_id == plan.no_client_order_id


def test_demo_order_cancellation_flow_reports_reduced_remaining_quantity(tmp_path: Path) -> None:
  settings = _settings(tmp_path)
  plan = build_pair_order_plan(_candidate(), Decimal('50.00'), settings)
  orders = simulate_submit_pair(
    plan,
    submitted_at=datetime(2026, 5, 5, 7, 1, tzinfo=UTC),
  )
  canceled_yes, canceled_no = simulate_cancel_pair(
    orders,
    canceled_at=datetime(2026, 5, 5, 7, 2, tzinfo=UTC),
  )

  assert canceled_yes.status == 'canceled'
  assert canceled_no.status == 'canceled'
  assert canceled_yes.reduced_by == plan.contract_count
  assert canceled_no.remaining_count == Decimal('0')


def test_demo_websocket_subscribe_reconnect_and_resubscribe() -> None:
  client = SimulatedWebSocketClient(
    operation_lane='sandbox',
    active_websocket_url='wss://demo-api.kalshi.co/trade-api/ws/v2',
  )
  client.connect()
  first_lane_session_id = client.lane_session_id
  initial_ids = client.subscribe(
    ['orderbook_delta', 'fill', 'user_orders'],
    ['KALSHI-DEMO-INTEGRATION-PAIR'],
  )

  assert client.connected is True
  assert len(initial_ids) == 3
  assert len(client.subscription_snapshot()) == 3

  resubscribed_ids = client.reconnect_and_resubscribe()

  assert client.connected is True
  assert len(resubscribed_ids) == 3
  assert client.lane_session_id != first_lane_session_id
  snapshot = client.subscription_snapshot()
  assert len(snapshot) == 3
  assert {item['channel'] for item in snapshot} == {'orderbook_delta', 'fill', 'user_orders'}
  assert all(item['operation_lane'] == 'sandbox' for item in snapshot)
  assert all(item['lane_session_id'] == client.lane_session_id for item in snapshot)


def test_demo_reconciliation_after_partial_fill_events_locks_only_after_both_sides_match(tmp_path: Path) -> None:
  settings = _settings(tmp_path)
  plan = build_pair_order_plan(_candidate(), Decimal('50.00'), settings)

  partial = simulate_partial_fill(
    plan,
    yes_filled=Decimal('5'),
    no_filled=Decimal('3'),
    as_of=datetime(2026, 5, 5, 7, 3, tzinfo=UTC),
    realized_fees_dollars=Decimal('0.03'),
  )
  locked = simulate_partial_fill(
    plan,
    yes_filled=Decimal('5'),
    no_filled=Decimal('5'),
    as_of=datetime(2026, 5, 5, 7, 4, tzinfo=UTC),
    realized_fees_dollars=Decimal('0.04'),
  )

  assert partial.state == 'PARTIAL_BOTH'
  assert locked.state == 'LOCKED'


class FakeResponse:
  def __init__(self, payload: dict, *, status_code: int = 200):
    self._payload = payload
    self.status_code = status_code

  def raise_for_status(self) -> None:
    if self.status_code >= 400:
      error = requests.HTTPError(f'HTTP {self.status_code}')
      error.response = self
      raise error
    return None

  def json(self) -> dict:
    return self._payload


class FakeSession:
  def __init__(self) -> None:
    self.requests: list[dict[str, object]] = []

  def request(self, method: str, url: str, headers: dict[str, str], timeout: int, **kwargs: object) -> FakeResponse:
    self.requests.append({'method': method, 'url': url, 'headers': headers, 'kwargs': kwargs})
    if url.endswith('/markets'):
      return FakeResponse(
        {
          'markets': [
            {
              'ticker': 'KALSHI-DEMO-INTEGRATION-001',
              'title': 'Demo integration market',
              'close_time': '2026-05-05T07:10:00Z',
              'status': 'open',
              'yes_bid_dollars': '0.33',
              'no_bid_dollars': '0.39',
              'volume_24h_fp': '120.00',
              'open_interest_fp': '75.00',
            }
          ],
          'cursor': None,
        }
      )
    if '/markets/' in url and url.endswith('/orderbook'):
      return FakeResponse(
        {
          'orderbook': {
            'ticker': 'KALSHI-DEMO-INTEGRATION-001',
            'yes_dollars': [[0.31, 10], [0.33, 12]],
            'no_dollars': [[0.39, 9], [0.41, 11]],
            'seq': 4,
          }
        }
      )
    if '/markets/' in url:
      return FakeResponse(
        {
          'market': {
            'ticker': 'KALSHI-DEMO-INTEGRATION-001',
            'title': 'Demo integration market',
            'close_time': '2026-05-05T07:10:00Z',
            'status': 'open',
            'yes_bid_dollars': '0.33',
            'no_bid_dollars': '0.39',
            'volume_24h_fp': '120.00',
            'open_interest_fp': '75.00',
          }
        }
      )
    raise AssertionError(f'Unexpected URL requested: {url}')


class FakeOrderbookFpSession(FakeSession):
  def request(self, method: str, url: str, headers: dict[str, str], timeout: int, **kwargs: object) -> FakeResponse:
    self.requests.append({'method': method, 'url': url, 'headers': headers, 'kwargs': kwargs})
    if '/markets/' in url and url.endswith('/orderbook'):
      return FakeResponse(
        {
          'orderbook_fp': {
            'yes_dollars': [['0.3100', '10.00'], ['0.3300', '12.00']],
            'no_dollars': [['0.3900', '9.00'], ['0.4100', '11.00']],
          }
        }
      )
    return super().request(method, url, headers, timeout, **kwargs)


class SequencedSession:
  def __init__(self, responses: list[FakeResponse]):
    self.responses = responses
    self.requests: list[dict[str, object]] = []

  def request(self, method: str, url: str, headers: dict[str, str], timeout: int, **kwargs: object) -> FakeResponse:
    self.requests.append({'method': method, 'url': url, 'headers': headers, 'kwargs': kwargs})
    return self.responses.pop(0)


class SSLFailingSession:
  def request(self, method: str, url: str, headers: dict[str, str], timeout: int, **kwargs: object) -> FakeResponse:
    del method, url, headers, timeout, kwargs
    raise requests.exceptions.SSLError('certificate verify failed')


class FakePrivateKey:
  def sign(self, *args: object, **kwargs: object) -> bytes:
    del args, kwargs
    return b'fake-signature'


def test_demo_market_data_helpers_cover_paginated_market_fetch_and_orderbook_views(tmp_path: Path) -> None:
  settings = _settings(tmp_path)
  client = KalshiHttpClient(settings=settings, private_key=FakePrivateKey(), session=FakeSession())

  markets = fetch_open_markets(client, limit=1000)
  market, orderbook = enrich_with_orderbook(client, 'KALSHI-DEMO-INTEGRATION-001')

  assert len(markets) == 1
  assert market.ticker == 'KALSHI-DEMO-INTEGRATION-001'
  assert orderbook.best_yes_bid == Decimal('0.33')
  assert derive_implied_asks(orderbook) == (Decimal('0.59'), Decimal('0.67'))
  assert compute_seconds_to_close(
    market,
    datetime(2026, 5, 5, 7, 6, tzinfo=UTC),
  ) == 240


def test_http_client_get_orderbook_unwraps_orderbook_fp(tmp_path: Path) -> None:
  settings = _settings(tmp_path)
  client = KalshiHttpClient(settings=settings, private_key=FakePrivateKey(), session=FakeOrderbookFpSession())

  orderbook = client.get_orderbook('KALSHI-DEMO-INTEGRATION-001')

  assert orderbook.ticker == 'KALSHI-DEMO-INTEGRATION-001'
  assert orderbook.best_yes_bid == Decimal('0.3300')
  assert orderbook.best_no_bid == Decimal('0.4100')
  assert orderbook.best_yes_ask_implied == Decimal('0.5900')
  assert orderbook.best_no_ask_implied == Decimal('0.6700')


def test_fetch_open_markets_honors_total_limit_across_pagination() -> None:
  client = PaginatedMarketClient()

  markets = fetch_open_markets(client, limit=3)

  assert [market.ticker for market in markets] == [
    'KALSHI-PAGED-001',
    'KALSHI-PAGED-002',
    'KALSHI-PAGED-003',
  ]
  assert client.calls == [
    {'status': 'open', 'limit': 3, 'cursor': None},
    {'status': 'open', 'limit': 1, 'cursor': 'page-2'},
  ]


def test_http_client_retries_rate_limit_and_redacts_auth_headers(monkeypatch, tmp_path: Path, caplog) -> None:
  settings = _settings(tmp_path)
  session = SequencedSession(
    [
      FakeResponse({'message': 'too many requests'}, status_code=429),
      FakeResponse({'balance': '25000'}),
    ]
  )
  client = KalshiHttpClient(settings=settings, private_key=FakePrivateKey(), session=session)

  sleeps: list[float] = []
  monkeypatch.setattr('polyventure.http_client.time.sleep', lambda delay: sleeps.append(delay))
  monkeypatch.setattr('polyventure.http_client.random.uniform', lambda start, end: 0.0)

  with caplog.at_level(logging.INFO):
    balance = client.get_balance()

  assert balance == Decimal('250')
  assert len(session.requests) == 2
  assert sleeps == [0.25]
  assert 'KALSHI-ACCESS-SIGNATURE' not in caplog.text
  assert '<redacted>' in caplog.text


def test_http_client_reports_secret_safe_trust_failure(tmp_path: Path) -> None:
  settings = _settings(tmp_path)
  client = KalshiHttpClient(settings=settings, private_key=FakePrivateKey(), session=SSLFailingSession())

  with pytest.raises(KalshiHttpError) as exc_info:
    client.get_balance()

  assert exc_info.value.reason_code == 'trust_failure'
  assert 'TLS trust validation failed' in str(exc_info.value)
