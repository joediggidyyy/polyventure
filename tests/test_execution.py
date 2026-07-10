from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

import pytest

from polyventure.execution import cancel_pair, compute_locked_pnl, ensure_order_group, hedge_unmatched, reconcile_pair, submit_pair
from polyventure.types import PairOrderPlan, PairRuntimeState


def _plan() -> PairOrderPlan:
  return PairOrderPlan(
    pair_id='pair-execution-001',
    ticker='KALSHI-EXECUTION-001',
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='pair-execution-001-yes',
    no_client_order_id='pair-execution-001-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )


def test_submit_reconcile_cancel_and_group_contracts_align_to_local_sandbox_state() -> None:
  submitted_at = datetime(2026, 5, 5, 7, 30, tzinfo=UTC)
  resting = submit_pair(_plan(), submitted_at=submitted_at)
  locked = reconcile_pair(
    PairRuntimeState(
      pair_id=resting.pair_id,
      state=resting.state,
      yes_filled_contracts=Decimal('5'),
      no_filled_contracts=Decimal('5'),
      average_yes_price=Decimal('0.34'),
      average_no_price=Decimal('0.39'),
      realized_fees_dollars=Decimal('0.04'),
      last_update_at=submitted_at,
      websocket_connected=True,
    ),
    as_of=datetime(2026, 5, 5, 7, 31, tzinfo=UTC),
  )
  canceled = cancel_pair(locked, canceled_at=datetime(2026, 5, 5, 7, 32, tzinfo=UTC))

  assert resting.state == 'RESTING_BOTH'
  assert locked.state == 'LOCKED'
  assert canceled.state == 'CANCELED'
  assert ensure_order_group(_plan().pair_id, Decimal('5')) == 'pair-execution-001-group-5'


def test_hedge_unmatched_freezes_pair_into_error_state_without_claiming_locked_residual() -> None:
  pair = PairRuntimeState(
    pair_id='pair-execution-002',
    state='PARTIAL_BOTH',
    yes_filled_contracts=Decimal('5'),
    no_filled_contracts=Decimal('3'),
    average_yes_price=Decimal('0.34'),
    average_no_price=Decimal('0.39'),
    realized_fees_dollars=Decimal('0.04'),
    last_update_at=datetime(2026, 5, 5, 7, 33, tzinfo=UTC),
    websocket_connected=True,
  )

  hedged = hedge_unmatched(pair, as_of=datetime(2026, 5, 5, 7, 34, tzinfo=UTC))

  assert hedged.state == 'ERROR'
  assert hedged.yes_filled_contracts == Decimal('3')
  assert hedged.no_filled_contracts == Decimal('3')


def test_compute_locked_pnl_reports_projected_and_realized_values_on_locked_quantity() -> None:
  pair = PairRuntimeState(
    pair_id='pair-execution-003',
    state='PARTIAL_BOTH',
    yes_filled_contracts=Decimal('5'),
    no_filled_contracts=Decimal('3'),
    average_yes_price=Decimal('0.34'),
    average_no_price=Decimal('0.39'),
    realized_fees_dollars=Decimal('0.04'),
    last_update_at=datetime(2026, 5, 5, 7, 35, tzinfo=UTC),
    websocket_connected=True,
  )

  pnl = compute_locked_pnl(pair, fee_reserve_dollars=Decimal('0.02'))

  assert pnl['locked_contracts'] == '3'
  assert pnl['unmatched_contracts'] == '2'
  assert pnl['gross_dollars'] == '0.81'
  assert pnl['net_projected_dollars'] == '0.75'
  assert pnl['net_realized_dollars'] == '0.77'


def test_pair_runtime_state_rejects_invalid_state() -> None:
  with pytest.raises(ValueError, match='Unsupported pair state'):
    PairRuntimeState(
      pair_id='pair-invalid-001',
      state='NOT_A_REAL_STATE',
      yes_filled_contracts=Decimal('0'),
      no_filled_contracts=Decimal('0'),
      average_yes_price=Decimal('0'),
      average_no_price=Decimal('0'),
      realized_fees_dollars=Decimal('0'),
      last_update_at=datetime(2026, 5, 5, 7, 36, tzinfo=UTC),
      websocket_connected=False,
    )