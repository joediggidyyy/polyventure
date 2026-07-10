from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

from .types import PairOrderPlan, PairRuntimeState, SubmittedOrder


def reconcile_pair_runtime_state(pair: PairRuntimeState, *, as_of: datetime) -> PairRuntimeState:
  yes_filled = pair.yes_filled_contracts
  no_filled = pair.no_filled_contracts

  if yes_filled == 0 and no_filled == 0:
    state = 'RESTING_BOTH'
  elif yes_filled == no_filled:
    state = 'LOCKED'
  elif yes_filled == 0 or no_filled == 0:
    state = 'PARTIAL_ONE_SIDE'
  else:
    state = 'PARTIAL_BOTH'

  return replace(pair, state=state, last_update_at=as_of)


def compute_locked_pnl(
  pair: PairRuntimeState,
  *,
  fee_reserve_dollars: Decimal = Decimal('0'),
) -> dict[str, str]:
  locked_contracts = pair.locked_contracts
  gross = locked_contracts * (
    Decimal('1') - pair.average_yes_price - pair.average_no_price
  )
  net_projected = locked_contracts * (
    Decimal('1') - pair.average_yes_price - pair.average_no_price - fee_reserve_dollars
  )
  net_realized = gross - pair.realized_fees_dollars
  return {
    'locked_contracts': str(locked_contracts),
    'unmatched_contracts': str(pair.unmatched_contracts),
    'gross_dollars': str(gross),
    'net_projected_dollars': str(net_projected),
    'net_realized_dollars': str(net_realized),
  }


def simulate_submit_pair(
  plan: PairOrderPlan,
  *,
  submitted_at: datetime,
) -> tuple[SubmittedOrder, SubmittedOrder]:
  yes_order = SubmittedOrder(
    order_id='{pair_id}:yes'.format(pair_id=plan.pair_id),
    client_order_id=plan.yes_client_order_id,
    ticker=plan.ticker,
    side='yes',
    price_dollars=plan.yes_price,
    contract_count=plan.contract_count,
    remaining_count=plan.contract_count,
    fill_count=Decimal('0'),
    status='resting',
    created_at=submitted_at,
    cancel_order_on_pause=plan.cancel_order_on_pause,
    subaccount=plan.subaccount,
  )
  no_order = SubmittedOrder(
    order_id='{pair_id}:no'.format(pair_id=plan.pair_id),
    client_order_id=plan.no_client_order_id,
    ticker=plan.ticker,
    side='no',
    price_dollars=plan.no_price,
    contract_count=plan.contract_count,
    remaining_count=plan.contract_count,
    fill_count=Decimal('0'),
    status='resting',
    created_at=submitted_at,
    cancel_order_on_pause=plan.cancel_order_on_pause,
    subaccount=plan.subaccount,
  )
  return yes_order, no_order


def simulate_cancel_pair(
  orders: tuple[SubmittedOrder, SubmittedOrder],
  *,
  canceled_at: datetime,
) -> tuple[SubmittedOrder, SubmittedOrder]:
  del canceled_at
  return tuple(
    replace(
      order,
      status='canceled',
      reduced_by=order.remaining_count,
      remaining_count=Decimal('0'),
    )
    for order in orders
  )


def simulate_partial_fill(
  plan: PairOrderPlan,
  *,
  yes_filled: Decimal,
  no_filled: Decimal,
  as_of: datetime,
  realized_fees_dollars: Decimal,
) -> PairRuntimeState:
  pair = PairRuntimeState(
    pair_id=plan.pair_id,
    state='RESTING_BOTH',
    yes_filled_contracts=yes_filled,
    no_filled_contracts=no_filled,
    average_yes_price=plan.yes_price,
    average_no_price=plan.no_price,
    realized_fees_dollars=realized_fees_dollars,
    last_update_at=as_of,
    websocket_connected=True,
  )
  return reconcile_pair_runtime_state(pair, as_of=as_of)


def ensure_order_group(pair_id: str, contract_limit_fp: Decimal) -> str:
  normalized_limit = format(contract_limit_fp.normalize(), 'f').rstrip('0').rstrip('.') or '0'
  return '{pair_id}-group-{limit}'.format(
    pair_id=pair_id,
    limit=normalized_limit.replace('.', '_'),
  )


def submit_pair(plan: PairOrderPlan, *, submitted_at: datetime | None = None) -> PairRuntimeState:
  effective_submitted_at = submitted_at or datetime.now(UTC)
  try:
    _ = simulate_submit_pair(plan, submitted_at=effective_submitted_at)
  except Exception:
    return PairRuntimeState(
      pair_id=plan.pair_id,
      state='ERROR',
      yes_filled_contracts=Decimal('0'),
      no_filled_contracts=Decimal('0'),
      average_yes_price=plan.yes_price,
      average_no_price=plan.no_price,
      realized_fees_dollars=Decimal('0'),
      last_update_at=effective_submitted_at,
      websocket_connected=False,
    )
  return PairRuntimeState(
    pair_id=plan.pair_id,
    state='RESTING_BOTH',
    yes_filled_contracts=Decimal('0'),
    no_filled_contracts=Decimal('0'),
    average_yes_price=plan.yes_price,
    average_no_price=plan.no_price,
    realized_fees_dollars=Decimal('0'),
    last_update_at=effective_submitted_at,
    websocket_connected=True,
  )


def reconcile_pair(pair: PairRuntimeState, *, as_of: datetime) -> PairRuntimeState:
  return reconcile_pair_runtime_state(pair, as_of=as_of)


def cancel_pair(pair: PairRuntimeState, *, canceled_at: datetime) -> PairRuntimeState:
  detail = replace(
    pair,
    state='CANCELED',
    yes_filled_contracts=pair.locked_contracts,
    no_filled_contracts=pair.locked_contracts,
    last_update_at=canceled_at,
  )
  return detail


def hedge_unmatched(pair: PairRuntimeState, *, as_of: datetime) -> PairRuntimeState:
  if pair.yes_filled_contracts == pair.no_filled_contracts:
    return reconcile_pair_runtime_state(pair, as_of=as_of)

  locked_contracts = min(pair.yes_filled_contracts, pair.no_filled_contracts)
  return PairRuntimeState(
    pair_id=pair.pair_id or 'pair-{suffix}'.format(suffix=uuid4().hex[:12]),
    state='ERROR',
    yes_filled_contracts=locked_contracts,
    no_filled_contracts=locked_contracts,
    average_yes_price=pair.average_yes_price,
    average_no_price=pair.average_no_price,
    realized_fees_dollars=pair.realized_fees_dollars,
    last_update_at=as_of,
    websocket_connected=pair.websocket_connected,
  )
