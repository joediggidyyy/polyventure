from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polyventure.config import Settings
from polyventure.execution import compute_locked_pnl, reconcile_pair_runtime_state
from polyventure.risk import can_open_new_pair, validate_pair_plan, validate_post_fill
from polyventure.types import CandidatePair, PairOrderPlan, PairRuntimeState
from polyventure.websocket_client import apply_orderbook_delta, normalize_orderbook_snapshot


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


def test_normalize_orderbook_snapshot_and_apply_delta() -> None:
  snapshot = normalize_orderbook_snapshot(
    {
      'ticker': 'KALSHI-OB-001',
      'yes_dollars': [[0.30, 10], [0.35, 12]],
      'no_dollars': [[0.40, 9], [0.42, 11]],
      'seq': 10,
      'captured_at': datetime(2026, 5, 5, 5, 20, tzinfo=UTC),
    }
  )

  updated = apply_orderbook_delta(
    snapshot,
    {
      'side': 'yes',
      'price': '0.36',
      'size': '15',
      'seq': 11,
      'captured_at': datetime(2026, 5, 5, 5, 20, 1, tzinfo=UTC),
    },
  )

  assert snapshot.best_yes_bid == Decimal('0.35')
  assert snapshot.best_no_bid == Decimal('0.42')
  assert snapshot.best_yes_ask_implied == Decimal('0.58')
  assert updated.best_yes_bid == Decimal('0.36')
  assert updated.best_no_ask_implied == Decimal('0.64')


def test_apply_orderbook_delta_rejects_sequence_gap() -> None:
  snapshot = normalize_orderbook_snapshot(
    {
      'ticker': 'KALSHI-OB-002',
      'yes_dollars': [[0.30, 10]],
      'no_dollars': [[0.40, 9]],
      'seq': 5,
    }
  )

  with pytest.raises(ValueError, match='sequence gap'):
    apply_orderbook_delta(snapshot, {'side': 'no', 'price': '0.41', 'size': '12', 'seq': 7})


def test_reconcile_pair_runtime_state_and_locked_pnl() -> None:
  pair = PairRuntimeState(
    pair_id='pair-runtime-001',
    state='RESTING_BOTH',
    yes_filled_contracts=Decimal('5'),
    no_filled_contracts=Decimal('5'),
    average_yes_price=Decimal('0.34'),
    average_no_price=Decimal('0.39'),
    realized_fees_dollars=Decimal('0.04'),
    last_update_at=datetime(2026, 5, 5, 5, 21, tzinfo=UTC),
  )

  reconciled = reconcile_pair_runtime_state(
    pair,
    as_of=datetime(2026, 5, 5, 5, 21, 2, tzinfo=UTC),
  )
  pnl = compute_locked_pnl(reconciled)

  assert reconciled.state == 'LOCKED'
  assert pnl['locked_contracts'] == '5'
  assert pnl['gross_dollars'] == '1.35'
  assert pnl['net_realized_dollars'] == '1.31'


def test_validate_post_fill_blocks_stale_unmatched_exposure() -> None:
  pair = PairRuntimeState(
    pair_id='pair-runtime-002',
    state='PARTIAL_ONE_SIDE',
    yes_filled_contracts=Decimal('5'),
    no_filled_contracts=Decimal('0'),
    average_yes_price=Decimal('0.34'),
    average_no_price=Decimal('0.00'),
    realized_fees_dollars=Decimal('0.02'),
    last_update_at=datetime.now(UTC) - timedelta(seconds=8),
    websocket_connected=False,
  )

  with pytest.raises(ValueError, match='Unmatched exposure exceeded'):
    validate_post_fill(pair, _settings(), as_of=datetime.now(UTC))


def test_can_open_new_pair_blocks_disconnected_unmatched_exposure() -> None:
  pair = PairRuntimeState(
    pair_id='pair-runtime-003',
    state='PARTIAL_BOTH',
    yes_filled_contracts=Decimal('5'),
    no_filled_contracts=Decimal('4'),
    average_yes_price=Decimal('0.34'),
    average_no_price=Decimal('0.39'),
    realized_fees_dollars=Decimal('0.04'),
    last_update_at=datetime.now(UTC),
    websocket_connected=False,
  )

  assert can_open_new_pair([pair], Decimal('100.00'), _settings()) is False


def test_validate_pair_plan_accepts_viable_candidate() -> None:
  candidate = CandidatePair(
    ticker='KALSHI-CANDIDATE-ROUND4',
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
  plan = PairOrderPlan(
    pair_id='pair-plan-004',
    ticker='KALSHI-CANDIDATE-ROUND4',
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='yes-plan-004',
    no_client_order_id='no-plan-004',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )

  validate_pair_plan(plan, candidate, _settings(), as_of=datetime(2026, 5, 5, 7, 30, tzinfo=UTC))