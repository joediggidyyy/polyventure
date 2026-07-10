from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from polyventure.config import Settings
from polyventure.soak import SoakConfig, run_demo_soak
from polyventure.types import AccountBucketLimit, AccountLimits, MarketSnapshot


class FakeSoakClient:
  def __init__(self, settings: Settings, private_key: object):
    self.settings = settings
    self.private_key = private_key
    now = datetime.now(UTC)
    self.markets = {
      'KALSHI-SOAK-HARNESS-001': MarketSnapshot(
        ticker='KALSHI-SOAK-HARNESS-001',
        title='Soak harness market',
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
  private_key_path = tmp_path / 'demo_soak_harness.pem'
  _write_private_key(private_key_path)
  return Settings(
    kalshi_env='demo',
    api_key_id='demo-soak-harness-key-id',
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
    state_db_path=str(tmp_path / 'demo_soak_harness.sqlite3'),
  )


def test_run_demo_soak_completes_requested_cycles_and_cleans_up(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  result = run_demo_soak(
    settings=_settings(tmp_path),
    config=SoakConfig(cycles=3, interval_seconds=0),
    client_factory=FakeSoakClient,
  )

  assert result['decision'] == 'pass'
  assert result['cycles_completed'] == 3
  assert result['cycle_error_count'] == 0
  assert result['table_counts']['pair_plans'] == 3
  assert result['canceled_pair_count'] == 3
  assert result['terminal_states_after_cleanup'] == ['CANCELED', 'CANCELED', 'CANCELED']


def test_run_demo_soak_requires_cycle_or_duration(tmp_path: Path) -> None:
  with pytest.raises(ValueError, match='requires cycles or max_duration_seconds'):
    run_demo_soak(settings=_settings(tmp_path), config=SoakConfig())