from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from .config import Settings
from .types import CandidatePair, PairOrderPlan, PairRuntimeState


@dataclass(frozen=True)
class CoverabilityGuardResult:
  ok: bool
  reason: str | None = None
  message: str = ''
  detail: dict[str, str] | None = None


def _required_decimal_setting(settings: Settings, field_name: str) -> tuple[Decimal | None, CoverabilityGuardResult | None]:
  raw_value = getattr(settings, field_name, None)
  if raw_value is None or str(raw_value).strip() == '':
    return None, CoverabilityGuardResult(
      ok=False,
      reason='coverability_threshold_unset',
      message=f'One-leg exposure guard threshold {field_name} is unset.',
      detail={'threshold': field_name},
    )
  try:
    value = Decimal(str(raw_value))
  except (InvalidOperation, ValueError):
    return None, CoverabilityGuardResult(
      ok=False,
      reason='coverability_threshold_invalid',
      message=f'One-leg exposure guard threshold {field_name} is invalid.',
      detail={'threshold': field_name, 'value': str(raw_value)},
    )
  return value, None


def _required_positive_decimal_setting(settings: Settings, field_name: str) -> tuple[Decimal | None, CoverabilityGuardResult | None]:
  value, error = _required_decimal_setting(settings, field_name)
  if error is not None or value is None:
    return value, error
  if value <= 0:
    return None, CoverabilityGuardResult(
      ok=False,
      reason='coverability_threshold_invalid',
      message=f'One-leg exposure guard threshold {field_name} must be positive.',
      detail={'threshold': field_name, 'value': str(value)},
    )
  return value, None


def evaluate_pre_submit_coverability_static_prices(
  *,
  yes_price: Decimal,
  no_price: Decimal,
  settings: Settings,
  best_yes_bid: Decimal | None,
  best_no_bid: Decimal | None,
) -> CoverabilityGuardResult:
  """Cheap pre-submit one-leg exposure checks before the trades read."""
  flow_k, error = _required_positive_decimal_setting(settings, 'flow_participation_k')
  if error is not None:
    return error
  del flow_k
  max_divergence, error = _required_positive_decimal_setting(settings, 'max_divergence')
  if error is not None:
    return error
  if max_divergence is None or max_divergence > Decimal('1'):
    return CoverabilityGuardResult(
      ok=False,
      reason='coverability_threshold_invalid',
      message='One-leg exposure guard threshold max_divergence must be in (0, 1].',
      detail={'threshold': 'max_divergence', 'value': str(max_divergence)},
    )
  if best_yes_bid is None or best_no_bid is None or best_yes_bid <= 0 or best_no_bid <= 0:
    return CoverabilityGuardResult(
      ok=False,
      reason='live_price_unavailable',
      message='Live book price is unavailable for coverability validation.',
    )
  divergence = abs(Decimal(str(yes_price)) - Decimal(str(no_price)))
  if divergence > max_divergence:
    return CoverabilityGuardResult(
      ok=False,
      reason='coverability_divergence_blocked',
      message='Candidate divergence exceeds max_divergence.',
      detail={
        'divergence': str(divergence),
        'max_divergence': str(max_divergence),
      },
    )
  yes_price = Decimal(str(yes_price))
  no_price = Decimal(str(no_price))
  if yes_price < Decimal(str(best_yes_bid)) or no_price < Decimal(str(best_no_bid)):
    return CoverabilityGuardResult(
      ok=False,
      reason='coverability_maker_price_blocked',
      message='Maker price is no longer competitive at the live book.',
      detail={
        'yes_price': str(yes_price),
        'no_price': str(no_price),
        'best_yes_bid': str(best_yes_bid),
        'best_no_bid': str(best_no_bid),
      },
    )
  return CoverabilityGuardResult(ok=True)


def evaluate_pre_submit_coverability_static(
  plan: PairOrderPlan,
  settings: Settings,
  *,
  best_yes_bid: Decimal | None,
  best_no_bid: Decimal | None,
) -> CoverabilityGuardResult:
  return evaluate_pre_submit_coverability_static_prices(
    yes_price=plan.yes_price,
    no_price=plan.no_price,
    settings=settings,
    best_yes_bid=best_yes_bid,
    best_no_bid=best_no_bid,
  )


def evaluate_flow_coverability(
  yes_flow_window_fp: Decimal | str | int | float | None,
  no_flow_window_fp: Decimal | str | int | float | None,
  intended_contract_count: Decimal,
  settings: Settings,
) -> CoverabilityGuardResult:
  flow_k, error = _required_positive_decimal_setting(settings, 'flow_participation_k')
  if error is not None or flow_k is None:
    return error or CoverabilityGuardResult(
      ok=False,
      reason='coverability_threshold_unset',
      message='One-leg exposure guard threshold flow_participation_k is unset.',
      detail={'threshold': 'flow_participation_k'},
    )
  try:
    yes_flow = Decimal(str(yes_flow_window_fp))
    no_flow = Decimal(str(no_flow_window_fp))
  except (InvalidOperation, ValueError):
    return CoverabilityGuardResult(
      ok=False,
      reason='coverability_flow_unavailable',
      message='Recent per-side flow is unavailable for coverability validation.',
    )
  required_flow = flow_k * Decimal(str(intended_contract_count))
  if yes_flow < required_flow or no_flow < required_flow:
    return CoverabilityGuardResult(
      ok=False,
      reason='coverability_flow_blocked',
      message='Recent per-side flow is below the participation floor.',
      detail={
        'yes_flow_window_fp': str(yes_flow),
        'no_flow_window_fp': str(no_flow),
        'required_flow_window_fp': str(required_flow),
        'flow_participation_k': str(flow_k),
        'intended_contract_count': str(intended_contract_count),
      },
    )
  return CoverabilityGuardResult(ok=True)


def _nth_weekday_of_month(year: int, month: int, weekday: int, occurrence: int) -> int:
  first_of_month = datetime(year, month, 1)
  offset = (weekday - first_of_month.weekday()) % 7
  return 1 + offset + (occurrence - 1) * 7


def _as_eastern(as_of: datetime) -> datetime:
  current_utc = as_of.astimezone(UTC)
  year = current_utc.year

  dst_start_day = _nth_weekday_of_month(year, 3, 6, 2)
  dst_end_day = _nth_weekday_of_month(year, 11, 6, 1)
  dst_start_utc = datetime(year, 3, dst_start_day, 7, 0, tzinfo=UTC)
  dst_end_utc = datetime(year, 11, dst_end_day, 6, 0, tzinfo=UTC)

  offset_hours = -4 if dst_start_utc <= current_utc < dst_end_utc else -5
  offset = timedelta(hours=offset_hours)
  return (current_utc + offset).replace(tzinfo=timezone(offset))


def _minimum_required_balance(settings: Settings) -> Decimal:
  max_pair_contracts = Decimal(str(settings.max_pair_contracts))
  fee_reserve = Decimal(str(settings.fee_reserve_dollars))
  return max_pair_contracts * (Decimal('1') + fee_reserve)


def _is_maintenance_window(as_of: datetime) -> bool:
  eastern = _as_eastern(as_of)
  if eastern.weekday() != 3:
    return False
  return 3 <= eastern.hour < 5


_FILL_BEARING_STATES = frozenset({
  'PARTIAL_ONE_SIDE',
  'PARTIAL_BOTH',
  'ASYMMETRIC_EXPOSURE',
  'REPAIR_LIVE',
  'EXPOSURE_CAPPED',
  'RECONCILE_REQUIRED',
  'LOCKED',
})


def can_open_new_pair(
  current_pairs: list[PairRuntimeState],
  balance: Decimal,
  settings: Settings,
  *,
  as_of: datetime | None = None,
  account_limits_loaded: bool = True,
  mode: str = 'ab_guarded',
  confirm_targeted: bool = True,
) -> bool:
  active_filled_pairs = [p for p in current_pairs if p.state in _FILL_BEARING_STATES]
  if len(active_filled_pairs) >= settings.max_open_pairs:
    return False
  if balance < _minimum_required_balance(settings):
    return False
  if not account_limits_loaded:
    return False
  if mode != 'ab_guarded' and not confirm_targeted:
    return False
  if _is_maintenance_window(as_of or datetime.now(UTC)):
    return False
  return not any(
    pair.state in {'PARTIAL_ONE_SIDE', 'PARTIAL_BOTH', 'ASYMMETRIC_EXPOSURE', 'REPAIR_LIVE', 'EXPOSURE_CAPPED', 'RECONCILE_REQUIRED'} and not pair.websocket_connected
    for pair in current_pairs
  )


def validate_pair_plan(
  plan: PairOrderPlan,
  candidate: CandidatePair,
  settings: Settings,
  *,
  market_status: str = 'active',
  exchange_paused: bool = False,
  trading_paused: bool = False,
  account_limits_loaded: bool = True,
  mode: str = 'ab_guarded',
  confirm_targeted: bool = True,
  as_of: datetime | None = None,
) -> None:
  if market_status.lower() not in {'active', 'open'}:
    raise ValueError('Market status is not eligible for a new paired entry.')
  if _is_maintenance_window(as_of or datetime.now(UTC)):
    raise ValueError('Thursday maintenance window blocks new paired entries.')
  if exchange_paused or trading_paused:
    raise ValueError('Exchange or trading pause blocks new paired entries.')
  if not account_limits_loaded:
    raise ValueError('Live account API limits must be loaded before planning a new pair.')
  if mode != 'ab_guarded' and not confirm_targeted:
    raise ValueError('Targeted mode requires explicit operator confirmation.')
  if candidate.seconds_to_close < settings.entry_window_end_sec:
    raise ValueError('Candidate is too close to close time for a new entry.')
  if candidate.seconds_to_close > settings.entry_window_start_sec:
    raise ValueError('Candidate is too early for the configured entry window.')
  if candidate.edge_gross_per_contract < Decimal(str(settings.min_edge_dollars)):
    raise ValueError('Candidate gross edge is below the configured minimum.')
  if candidate.edge_net_per_contract < Decimal(str(settings.min_profit_dollars)):
    raise ValueError('Candidate net edge is below the configured profit floor.')
  if plan.contract_count > Decimal(str(settings.max_pair_contracts)):
    raise ValueError('Plan contract count exceeds the configured pair cap.')
  if plan.contract_count > candidate.max_size_contracts:
    raise ValueError('Plan contract count exceeds the candidate size cap.')
  if plan.contract_count <= 0:
    raise ValueError('Plan contract count must be positive.')


def validate_post_fill(
  pair: PairRuntimeState,
  settings: Settings,
  *,
  as_of: datetime | None = None,
) -> None:
  """Raise when a one-sided (unmatched) fill has persisted past max_unhedged_sec
  (SSOT WAGER_HEDGE_MODELS.md unmatched-exposure rule). Matched or unfilled pairs
  never raise."""
  unmatched = abs(pair.yes_filled_contracts - pair.no_filled_contracts)
  if unmatched <= 0:
    return
  current = (as_of or datetime.now(UTC)).astimezone(UTC)
  last_update = pair.last_update_at
  if last_update.tzinfo is None:
    last_update = last_update.replace(tzinfo=UTC)
  elapsed_sec = (current - last_update.astimezone(UTC)).total_seconds()
  if elapsed_sec > settings.max_unhedged_sec:
    raise ValueError(
      'Unmatched exposure exceeded max_unhedged_sec '
      '({elapsed:.0f}s > {limit}s).'.format(elapsed=elapsed_sec, limit=settings.max_unhedged_sec)
    )
