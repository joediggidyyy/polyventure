from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from polyventure.config import Settings
from polyventure.execution import simulate_cancel_pair, simulate_partial_fill, simulate_submit_pair
from polyventure.strategy import build_pair_order_plan
from polyventure.types import CandidatePair


def _settings() -> Settings:
  return Settings(
    kalshi_env='demo',
    api_key_id='key-id',
    private_key_file='secrets/demo.pem',
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
    state_db_path='var/kalshi.sqlite3',
  )


def _candidate() -> CandidatePair:
  return CandidatePair(
    ticker='KALSHI-ROUND5',
    seconds_to_close=240,
    target_yes_bid=Decimal('0.34'),
    target_no_bid=Decimal('0.39'),
    edge_gross_per_contract=Decimal('0.27'),
    fee_reserve_per_contract=Decimal('0.02'),
    edge_net_per_contract=Decimal('0.25'),
    asymmetry=Decimal('0.05'),
    max_size_contracts=Decimal('5'),
    ranking_key=(Decimal('0.25'), Decimal('0.05'), Decimal('100'), Decimal('50'), -240),
  )


def test_build_pair_order_plan_sizes_to_available_balance() -> None:
  plan = build_pair_order_plan(_candidate(), Decimal('10.00'), _settings())

  assert plan.contract_count == Decimal('5')
  assert plan.post_only is True
  assert plan.cancel_order_on_pause is True


def test_simulate_submit_pair_returns_resting_orders() -> None:
  plan = build_pair_order_plan(_candidate(), Decimal('10.00'), _settings())
  yes_order, no_order = simulate_submit_pair(
    plan,
    submitted_at=datetime(2026, 5, 5, 6, 30, tzinfo=UTC),
  )

  assert yes_order.status == 'resting'
  assert no_order.status == 'resting'
  assert yes_order.remaining_count == plan.contract_count
  assert no_order.remaining_count == plan.contract_count


def test_simulate_cancel_pair_zeroes_remaining_quantity() -> None:
  plan = build_pair_order_plan(_candidate(), Decimal('10.00'), _settings())
  orders = simulate_submit_pair(
    plan,
    submitted_at=datetime(2026, 5, 5, 6, 30, tzinfo=UTC),
  )
  canceled_yes, canceled_no = simulate_cancel_pair(
    orders,
    canceled_at=datetime(2026, 5, 5, 6, 31, tzinfo=UTC),
  )

  assert canceled_yes.status == 'canceled'
  assert canceled_no.status == 'canceled'
  assert canceled_yes.remaining_count == Decimal('0')
  assert canceled_no.reduced_by == plan.contract_count


def test_simulate_partial_fill_marks_partial_then_locked() -> None:
  plan = build_pair_order_plan(_candidate(), Decimal('10.00'), _settings())

  partial = simulate_partial_fill(
    plan,
    yes_filled=Decimal('5'),
    no_filled=Decimal('3'),
    as_of=datetime(2026, 5, 5, 6, 32, tzinfo=UTC),
    realized_fees_dollars=Decimal('0.03'),
  )
  locked = simulate_partial_fill(
    plan,
    yes_filled=Decimal('5'),
    no_filled=Decimal('5'),
    as_of=datetime(2026, 5, 5, 6, 33, tzinfo=UTC),
    realized_fees_dollars=Decimal('0.04'),
  )

  assert partial.state == 'PARTIAL_BOTH'
  assert locked.state == 'LOCKED'