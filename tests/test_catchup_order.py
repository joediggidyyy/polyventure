from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from polyventure.strategy import compute_catchup_order
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


def test_catchup_submits_when_edge_holds_with_depth() -> None:
  # YES filled at 0.16; need NO. Deficient=no crosses YES bids.
  # max_no_price = 1 - 0.16 - 0.02 = 0.82 -> min qualifying yes bid = 0.18.
  book = _book(yes_bids=[(0.20, 30), (0.18, 25), (0.10, 100)], no_bids=[])
  plan = compute_catchup_order('no', Decimal('40'), Decimal('0.16'), book, Decimal('0.02'))
  assert plan.submit is True
  assert plan.limit_price_dollars == Decimal('0.82')
  # qualifying yes bids: 0.20 (30) + 0.18 (25) = 55, capped at unmatched 40.
  assert plan.fillable_contracts == Decimal('40')
  assert plan.reason == 'catchup_available'


def test_catchup_fillable_capped_by_available_depth() -> None:
  book = _book(yes_bids=[(0.20, 10), (0.18, 5), (0.10, 100)], no_bids=[])
  plan = compute_catchup_order('no', Decimal('40'), Decimal('0.16'), book, Decimal('0.02'))
  assert plan.submit is True
  assert plan.fillable_contracts == Decimal('15')  # 10 + 5; 0.10 bid excluded (< 0.18)


def test_catchup_blocks_when_no_depth_within_edge() -> None:
  # All YES bids below the 0.18 floor -> nothing fillable within the edge.
  book = _book(yes_bids=[(0.10, 100), (0.05, 100)], no_bids=[])
  plan = compute_catchup_order('no', Decimal('40'), Decimal('0.16'), book, Decimal('0.02'))
  assert plan.submit is False
  assert plan.reason == 'no_fillable_depth_within_edge'


def test_catchup_blocks_when_edge_inequality_fails() -> None:
  # YES filled at 0.97; max_no_price = 1 - 0.97 - 0.02 = 0.01 ... still positive,
  # but use a case where it goes non-positive: filled 0.99 + fee 0.02 -> -0.01.
  book = _book(yes_bids=[(0.50, 100)], no_bids=[])
  plan = compute_catchup_order('no', Decimal('10'), Decimal('0.99'), book, Decimal('0.02'))
  assert plan.submit is False
  assert plan.reason == 'edge_inequality_failed'


def test_catchup_deficient_yes_crosses_no_bids() -> None:
  # NO filled at 0.30; need YES. Deficient=yes crosses NO bids.
  # max_yes_price = 1 - 0.30 - 0.02 = 0.68 -> min qualifying no bid = 0.32.
  book = _book(yes_bids=[], no_bids=[(0.40, 20), (0.32, 10), (0.20, 50)])
  plan = compute_catchup_order('yes', Decimal('25'), Decimal('0.30'), book, Decimal('0.02'))
  assert plan.submit is True
  assert plan.limit_price_dollars == Decimal('0.68')
  assert plan.fillable_contracts == Decimal('25')  # 20 + 10 = 30, capped at 25


def test_catchup_no_unmatched_quantity() -> None:
  book = _book(yes_bids=[(0.50, 100)], no_bids=[(0.50, 100)])
  plan = compute_catchup_order('no', Decimal('0'), Decimal('0.16'), book, Decimal('0.02'))
  assert plan.submit is False
  assert plan.reason == 'no_unmatched_quantity'
