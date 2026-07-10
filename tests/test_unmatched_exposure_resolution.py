"""Canonical shelter resolution: cap the ahead leg, preserve the repair order.

Exercises the post-shelter resolution path with a self-contained fake client. Proves
the canonical one-sided-fill response: cancel only the over-filled (ahead) leg, leave
the deficient (repair) leg's resting order open to fill, and project REPAIR_LIVE while
the repair order is live / EXPOSURE_CAPPED once it is not. No market-crossing catch-up,
no order-age freeze to ERROR, no operator alert on this path.
"""
from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from polyventure.config import Settings
from polyventure.persistence import open_database, persist_pair_plan
from polyventure.service import _place_live_pair_orders
from polyventure.types import (
  AccountBucketLimit,
  AccountLimits,
  MarketSnapshot,
  PairOrderPlan,
  PairPosition,
  SubmittedOrder,
)
from polyventure.websocket_client import normalize_orderbook_snapshot

from tests.test_service import _settings, _write_private_key

TICKER = 'KXTEST-RESOLVE'


def _order(order_id, side, price, count, status, fill_count) -> SubmittedOrder:
  return SubmittedOrder(
    order_id=order_id,
    client_order_id=order_id,
    ticker=TICKER,
    side=side,
    price_dollars=Decimal(str(price)),
    contract_count=Decimal(str(count)),
    remaining_count=Decimal(str(count)) - Decimal(str(fill_count)),
    fill_count=Decimal(str(fill_count)),
    status=status,
    created_at=datetime.now(UTC),
    cancel_order_on_pause=False,
    subaccount=0,
  )


class _ResolveFake:
  """One-sided fill: YES fills fully (ahead leg), NO never fills (repair leg).
  ``repair_resting`` drives whether the preserved repair order is still live at
  readback (REPAIR_LIVE) or already closed (EXPOSURE_CAPPED)."""

  def __init__(self, settings, key, *, repair_resting=True, book_yes_bid='0.95'):
    self.settings = settings
    self.private_key = key
    self.repair_resting = bool(repair_resting)
    self.book_yes_bid = book_yes_bid
    self.create_order_v2_calls: list[dict] = []
    self.cancel_calls: list[str] = []
    self._orders: dict[str, SubmittedOrder] = {}

  def create_order_group(self, contracts_limit_fp, subaccount=0):
    return 'grp-1'

  def create_order_v2(self, **payload):
    side = str(payload['side'])
    coid = str(payload.get('client_order_id', ''))
    self.create_order_v2_calls.append({'side': side, 'client_order_id': coid, 'post_only': payload.get('post_only')})
    price = Decimal(str(payload.get('yes_price') or payload.get('no_price') or '0'))
    count = Decimal(str(payload['count']))
    oid = f'kalshi-{side}-001'
    self._orders[oid] = _order(oid, side, price, count, 'resting', 0)
    return self._orders[oid]

  def cancel_order_v2(self, order_id):
    self.cancel_calls.append(order_id)
    return {'order_id': order_id, 'status': 'canceled', 'reduced_by': '1'}

  def get_order(self, order_id):
    # YES filled fully (executed, not cancelable). NO is the repair leg: left resting
    # (open to fill) when repair_resting, else reported closed to drive EXPOSURE_CAPPED.
    if order_id == 'kalshi-yes-001':
      return _order(order_id, 'yes', '0.40', 10, 'executed', 10)
    if order_id in self.cancel_calls or not self.repair_resting:
      return _order(order_id, 'no', '0.41', 10, 'canceled', 0)
    return _order(order_id, 'no', '0.41', 10, 'resting', 0)

  def get_market(self, ticker):
    return MarketSnapshot(
      ticker=ticker, title='t', close_time=datetime.now(UTC) + timedelta(seconds=2),
      status='open', yes_bid_dollars=Decimal('0.40'), no_bid_dollars=Decimal('0.41'),
      volume_24h_fp=Decimal('100'), open_interest_fp=Decimal('100'),
    )

  def get_orderbook(self, ticker, depth=0):
    return normalize_orderbook_snapshot({
      'ticker': ticker,
      'yes_dollars': [[self.book_yes_bid, '50']],
      'no_dollars': [['0.50', '50']],
      'seq': 1,
    })

  def get_positions(self):
    return []

  def get_account_api_limits(self):
    return AccountLimits('t', AccountBucketLimit(30, 60), AccountBucketLimit(10, 20))


def _live_settings(tmp_path: Path) -> Settings:
  key = tmp_path / 'k.pem'
  _write_private_key(key)
  db = tmp_path / 'resolve.sqlite3'
  base = _settings(str(key)).__dict__
  return Settings(**{**base, 'operation_lane': 'live', 'kalshi_env': 'prod', 'max_unhedged_sec': 5, 'state_db_path': str(db)})


def _plan() -> PairOrderPlan:
  return PairOrderPlan(
    pair_id='pair-resolve', ticker=TICKER, yes_price=Decimal('0.40'), no_price=Decimal('0.41'),
    contract_count=Decimal('10'), yes_client_order_id='pair-resolve-yes', no_client_order_id='pair-resolve-no',
    time_in_force='good_till_canceled', post_only=True, cancel_order_on_pause=False, subaccount=0,
  )


_SIZING = {
  'effective_density': '1', 'dynamic_pair_notional_pct': '0.1',
  'dynamic_pair_notional_cap_dollars': '100', 'dynamic_max_contracts': '10', 'binding_limiter': 'test',
}


def _run(tmp_path, monkeypatch, **fake_kwargs):
  monkeypatch.setattr('time.sleep', lambda _x: None)
  settings = _live_settings(tmp_path)
  connection = open_database(Path(settings.state_db_path))
  plan = _plan()
  persist_pair_plan(connection, plan, created_at_utc=datetime.now(UTC).isoformat(), operation_lane='live')
  client = _ResolveFake(settings, object(), **fake_kwargs)
  result = _place_live_pair_orders(
    client, connection, plan=plan, settings=settings, lane_session_id='sess-1',
    recorded_at=datetime.now(UTC), sizing_summary=_SIZING, saved_set_snapshot=None,
  )
  return result, client, connection


def test_one_sided_fill_caps_ahead_and_preserves_repair_order(tmp_path, monkeypatch) -> None:
  # Canonical shelter: YES filled fully (ahead leg), NO unfilled (repair leg). The
  # ahead leg is capped (already executed -> not_cancelable, no new cancel) and the
  # repair order is left resting -> REPAIR_LIVE. No catch-up, no freeze, no alert.
  result, client, connection = _run(tmp_path, monkeypatch, repair_resting=True)
  assert result['terminal_state'] == 'REPAIR_LIVE'
  # The deficient NO leg was NOT cancelled (left open to fill); the executed YES leg
  # was not re-cancelled either.
  assert client.cancel_calls == []
  # No market-crossing catch-up order was placed.
  assert not any('catchup' in c['client_order_id'] for c in client.create_order_v2_calls)
  # No operator alert: the canonical path does not freeze to ERROR here.
  sources = [r[0] for r in connection.execute('SELECT source FROM operator_notifications').fetchall()]
  assert 'unmatched_exposure_resolution' not in sources


def test_one_sided_fill_repair_order_closed_caps_exposure(tmp_path, monkeypatch) -> None:
  # Repair order is no longer live at readback (closed) -> EXPOSURE_CAPPED, still with
  # no catch-up and no freeze to ERROR.
  result, client, connection = _run(tmp_path, monkeypatch, repair_resting=False)
  assert result['terminal_state'] == 'EXPOSURE_CAPPED'
  assert not any('catchup' in c['client_order_id'] for c in client.create_order_v2_calls)
  sources = [r[0] for r in connection.execute('SELECT source FROM operator_notifications').fetchall()]
  assert 'unmatched_exposure_resolution' not in sources
