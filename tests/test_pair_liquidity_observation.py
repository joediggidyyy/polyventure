"""Lane A coverability instrumentation: band-sum math, per-side flow aggregation,
explicit-lane persistence, and fail-soft capture.

Additive, read-only instrumentation -- these tests assert the new observation seam
without touching candidate selection, ranking, submit, or shelter behavior.
"""
from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest

from polyventure.http_client import KalshiHttpClient
from polyventure.persistence import SCHEMA_STATEMENTS, persist_pair_liquidity_observation
from polyventure.service import capture_pair_liquidity_observation
from polyventure.strategy import summarize_depth_within_band
from polyventure.types import OrderbookSnapshot


def _book(yes_bids, no_bids) -> OrderbookSnapshot:
  return OrderbookSnapshot(
    ticker='KXTEST',
    yes_bids=tuple((Decimal(str(p)), Decimal(str(s))) for p, s in yes_bids),
    no_bids=tuple((Decimal(str(p)), Decimal(str(s))) for p, s in no_bids),
    best_yes_bid=Decimal(str(yes_bids[0][0])) if yes_bids else None,
    best_no_bid=Decimal(str(no_bids[0][0])) if no_bids else None,
    best_yes_ask_implied=None,
    best_no_ask_implied=None,
    captured_at=datetime.now(UTC),
  )


def _db() -> sqlite3.Connection:
  conn = sqlite3.connect(':memory:')
  conn.row_factory = sqlite3.Row
  for statement in SCHEMA_STATEMENTS:
    conn.execute(statement)
  return conn


class _FakeTradesClient:
  """Minimal client exposing only what the capture helper calls."""

  def __init__(self, flow=None, raises: bool = False):
    self._flow = flow or {'yes_flow_fp': Decimal('0'), 'no_flow_fp': Decimal('0'), 'trade_count': 0}
    self._raises = raises

  def get_recent_trades(self, ticker, *, window_sec):  # noqa: ARG002 - signature parity
    if self._raises:
      raise RuntimeError('trades read failed')
    return self._flow


def test_band_sum_counts_only_levels_at_or_above_intended_price() -> None:
  book = _book(yes_bids=[(0.20, 30), (0.18, 25), (0.10, 100)], no_bids=[(0.80, 50), (0.70, 10)])
  summary = summarize_depth_within_band(book, Decimal('0.18'), Decimal('0.75'))
  # YES levels >= 0.18: 30 + 25 = 55 (the 0.10 level is below the maker limit).
  assert summary['yes_depth_within_band'] == Decimal('55')
  # NO levels >= 0.75: 50 only.
  assert summary['no_depth_within_band'] == Decimal('50')
  # Decimal canonicalizes (0.20 -> '0.2', 0.10 -> '0.1'); values, not formatting, are authoritative.
  assert summary['yes_bid_depth_json'] == [['0.2', '30'], ['0.18', '25'], ['0.1', '100']]
  assert summary['best_yes_bid'] == Decimal('0.20')


def test_recent_trades_sum_per_side_flow_inside_window_and_stop_past_it() -> None:
  now = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)
  page = {
    'trades': [
      {'created_time': '2026-01-01T11:59:00Z', 'taker_outcome_side': 'yes', 'count_fp': '5'},
      {'created_time': '2026-01-01T11:58:00Z', 'taker_outcome_side': 'no', 'count_fp': '3'},
      {'created_time': '2026-01-01T11:50:00Z', 'taker_outcome_side': 'yes', 'count_fp': '9'},
    ],
  }
  client = SimpleNamespace(_request=lambda method, path, params=None: page)
  flow = KalshiHttpClient.get_recent_trades(client, 'KXTEST', window_sec=300, now=now)
  # The 11:50 trade is older than the 300s window -> excluded, and pagination stops.
  assert flow['yes_flow_fp'] == Decimal('5')
  assert flow['no_flow_fp'] == Decimal('3')
  assert flow['trade_count'] == 2


def test_persist_requires_explicit_lane() -> None:
  conn = _db()
  with pytest.raises(ValueError):
    persist_pair_liquidity_observation(
      conn,
      pair_id='pair-1',
      ticker='KXTEST',
      phase='submit',
      operation_lane='',
      recorded_at_utc='2026-01-01T12:00:00+00:00',
      observation={'readback_status': 'ok', 'yes_bid_depth_json': '[]', 'no_bid_depth_json': '[]'},
    )


def test_capture_writes_ok_row_with_band_and_flow() -> None:
  conn = _db()
  book = _book(yes_bids=[(0.20, 30), (0.18, 25)], no_bids=[(0.80, 50)])
  client = _FakeTradesClient(flow={'yes_flow_fp': Decimal('5'), 'no_flow_fp': Decimal('3'), 'trade_count': 2})
  market = SimpleNamespace(volume_24h_fp=Decimal('790'), volume_fp=None, open_interest_fp=Decimal('6118'))
  settings = SimpleNamespace(flow_window_sec=300, operation_lane='live')

  capture_pair_liquidity_observation(
    client,
    conn,
    pair_id='pair-1',
    ticker='KXTEST',
    phase='submit',
    orderbook=book,
    intended_yes_price=Decimal('0.18'),
    intended_no_price=Decimal('0.75'),
    intended_contract_count=Decimal('40'),
    market=market,
    settings=settings,
    recorded_at_utc='2026-01-01T12:00:00+00:00',
    lane_session_id='sess-1',
  )

  row = conn.execute('SELECT * FROM pair_liquidity_observations').fetchone()
  assert row['readback_status'] == 'ok'
  assert row['operation_lane'] == 'live'
  assert row['yes_depth_within_band'] == '55'
  assert row['yes_flow_window_fp'] == '5'
  assert row['no_flow_window_fp'] == '3'
  assert row['divergence'] == str(abs(Decimal('0.18') - Decimal('0.75')))
  assert row['open_interest_fp'] == '6118'


def test_capture_is_fail_soft_on_trades_read_error() -> None:
  conn = _db()
  book = _book(yes_bids=[(0.20, 30)], no_bids=[(0.80, 50)])
  client = _FakeTradesClient(raises=True)
  settings = SimpleNamespace(flow_window_sec=300, operation_lane='live')

  # Must NOT raise into the order path.
  capture_pair_liquidity_observation(
    client,
    conn,
    pair_id='pair-2',
    ticker='KXTEST',
    phase='submit',
    orderbook=book,
    intended_yes_price=Decimal('0.18'),
    intended_no_price=Decimal('0.75'),
    intended_contract_count=Decimal('40'),
    market=None,
    settings=settings,
    recorded_at_utc='2026-01-01T12:00:00+00:00',
    lane_session_id='sess-1',
  )

  row = conn.execute('SELECT * FROM pair_liquidity_observations').fetchone()
  assert row['readback_status'] == 'readback_failed'
  assert row['yes_bid_depth_json'] == '[]'
  assert row['yes_flow_window_fp'] is None
