from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from polyventure.config import Settings
from polyventure.service import cancel_all_pairs, reconcile_pairs, report_runtime, run_service_once
from polyventure.types import AccountBucketLimit, AccountLimits, MarketSnapshot


class FakeSoakClient:
  def __init__(self, settings: Settings, private_key: object):
    self.settings = settings
    self.private_key = private_key
    now = datetime.now(UTC)
    self.markets = {
      'KALSHI-DEMO-SOAK-001': MarketSnapshot(
        ticker='KALSHI-DEMO-SOAK-001',
        title='Bounded demo soak market',
        close_time=now + timedelta(seconds=240),
        status='open',
        yes_bid_dollars=Decimal('0.33'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('180.00'),
        open_interest_fp=Decimal('95.00'),
      )
    }

  def get_balance(self) -> Decimal:
    return Decimal('500.00')

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
        'yes_dollars': [[str(market.yes_bid_dollars - Decimal('0.01')), '9'], [str(market.yes_bid_dollars), '15']],
        'no_dollars': [[str(market.no_bid_dollars - Decimal('0.01')), '8'], [str(market.no_bid_dollars), '14']],
        'seq': 2,
      }
    )


def _write_private_key(path: Path) -> None:
  key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
  pem = key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption(),
  )
  path.write_bytes(pem)


def _settings(tmp_path: Path) -> Settings:
  private_key_path = tmp_path / 'demo_soak.pem'
  _write_private_key(private_key_path)
  return Settings(
    kalshi_env='demo',
    api_key_id='demo-soak-key-id',
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
    state_db_path=str(tmp_path / 'demo_soak.sqlite3'),
  )


def test_bounded_demo_soak_runs_repeated_cycles_without_unhandled_exceptions(
  monkeypatch,
  tmp_path: Path,
) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  settings = _settings(tmp_path)
  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeSoakClient)

  payloads = [run_service_once(settings=settings) for _ in range(5)]
  report = report_runtime(settings=settings)

  assert all(payload['decision'] == 'planned' for payload in payloads)
  assert all(payload['mode'] == 'ab_guarded' for payload in payloads)
  assert report['table_counts']['service_heartbeats'] == 10
  assert report['table_counts']['pair_plans'] == 5
  assert report['latest_heartbeat']['status'] == 'cycle-complete'


def test_bounded_demo_soak_cleanup_leaves_pairs_in_terminal_state(
  monkeypatch,
  tmp_path: Path,
) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  settings = _settings(tmp_path)
  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeSoakClient)

  for _ in range(3):
    run_service_once(settings=settings)

  cancel_payload = cancel_all_pairs(settings=settings)
  reconcile_payload = reconcile_pairs(settings=settings)

  assert cancel_payload['canceled_pair_count'] == 3
  assert reconcile_payload['pair_count'] == 3
  assert all(pair['state'] == 'CANCELED' for pair in reconcile_payload['pairs'])