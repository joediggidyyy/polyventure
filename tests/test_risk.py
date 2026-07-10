from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from polyventure.config import Settings
from polyventure.risk import (
  can_open_new_pair,
  evaluate_flow_coverability,
  evaluate_pre_submit_coverability_static,
  validate_pair_plan,
  validate_post_fill,
)
from polyventure.types import CandidatePair, PairOrderPlan, PairRuntimeState


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


def test_validate_pair_plan_rejects_candidates_outside_window_or_profit_floor() -> None:
  settings = _settings()
  plan = PairOrderPlan(
    pair_id='pair-risk-001',
    ticker='KALSHI-RISK-001',
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='pair-risk-001-yes',
    no_client_order_id='pair-risk-001-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  candidate = CandidatePair(
    ticker='KALSHI-RISK-001',
    seconds_to_close=30,
    target_yes_bid=Decimal('0.34'),
    target_no_bid=Decimal('0.39'),
    edge_gross_per_contract=Decimal('0.27'),
    fee_reserve_per_contract=Decimal('0.02'),
    edge_net_per_contract=Decimal('0.25'),
    asymmetry=Decimal('0.05'),
    max_size_contracts=Decimal('5'),
    ranking_key=(Decimal('0.25'), Decimal('0.05'), Decimal('100'), Decimal('50'), -30),
  )

  with pytest.raises(ValueError, match='too close to close time'):
    validate_pair_plan(plan, candidate, settings, as_of=datetime(2026, 5, 5, 7, 30, tzinfo=UTC))


def test_can_open_new_pair_and_validate_post_fill_preserve_unmatched_exposure_guard() -> None:
  settings = _settings()
  pair = PairRuntimeState(
    pair_id='pair-risk-002',
    state='PARTIAL_ONE_SIDE',
    yes_filled_contracts=Decimal('5'),
    no_filled_contracts=Decimal('0'),
    average_yes_price=Decimal('0.34'),
    average_no_price=Decimal('0'),
    realized_fees_dollars=Decimal('0.02'),
    last_update_at=datetime.now(UTC) - timedelta(seconds=8),
    websocket_connected=False,
  )

  assert can_open_new_pair([pair], Decimal('100.00'), settings) is False
  # The pair is stale past max_unhedged_sec, so the post-fill guard must block it.
  with pytest.raises(ValueError, match='Unmatched exposure exceeded'):
    validate_post_fill(pair, settings, as_of=datetime.now(UTC))


def test_validate_pair_plan_blocks_maintenance_window_and_missing_limits() -> None:
  settings = _settings()
  plan = PairOrderPlan(
    pair_id='pair-risk-003',
    ticker='KALSHI-RISK-003',
    yes_price=Decimal('0.33'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='pair-risk-003-yes',
    no_client_order_id='pair-risk-003-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  candidate = CandidatePair(
    ticker='KALSHI-RISK-003',
    seconds_to_close=300,
    target_yes_bid=Decimal('0.33'),
    target_no_bid=Decimal('0.39'),
    edge_gross_per_contract=Decimal('0.28'),
    fee_reserve_per_contract=Decimal('0.02'),
    edge_net_per_contract=Decimal('0.26'),
    asymmetry=Decimal('0.06'),
    max_size_contracts=Decimal('5'),
    ranking_key=(Decimal('0.26'), Decimal('0.06'), Decimal('120'), Decimal('75'), -300),
  )

  with pytest.raises(ValueError, match='maintenance window'):
    validate_pair_plan(
      plan,
      candidate,
      settings,
      as_of=datetime(2026, 5, 7, 7, 30, tzinfo=UTC),
    )

  with pytest.raises(ValueError, match='API limits must be loaded'):
    validate_pair_plan(
      plan,
      candidate,
      settings,
      account_limits_loaded=False,
      as_of=datetime(2026, 5, 5, 7, 30, tzinfo=UTC),
    )


def test_can_open_new_pair_requires_limits_and_targeted_confirmation() -> None:
  settings = _settings()

  assert can_open_new_pair(
    [],
    Decimal('500.00'),
    settings,
    account_limits_loaded=False,
  ) is False
  assert can_open_new_pair(
    [],
    Decimal('500.00'),
    settings,
    mode='a_targeted',
    confirm_targeted=False,
  ) is False


def _make_pair(pair_id: str, state: str) -> PairRuntimeState:
  return PairRuntimeState(
    pair_id=pair_id,
    state=state,
    yes_filled_contracts=Decimal('0'),
    no_filled_contracts=Decimal('0'),
    average_yes_price=Decimal('0'),
    average_no_price=Decimal('0'),
    realized_fees_dollars=Decimal('0'),
    last_update_at=datetime.now(UTC),
    websocket_connected=True,
  )


def test_can_open_new_pair_submitting_state_does_not_count_as_active() -> None:
  settings = _settings()
  submitted_pair = _make_pair('pair-hx1-submitting', 'SUBMITTING')
  assert can_open_new_pair([submitted_pair], Decimal('500.00'), settings) is True, (
    'HX1-A regression: SUBMITTING pair should not consume an active slot'
  )


def test_can_open_new_pair_partial_one_side_counts_as_active() -> None:
  from polyventure.risk import _FILL_BEARING_STATES
  assert 'PARTIAL_ONE_SIDE' in _FILL_BEARING_STATES
  assert 'REPAIR_LIVE' in _FILL_BEARING_STATES
  assert 'EXPOSURE_CAPPED' in _FILL_BEARING_STATES
  assert 'RECONCILE_REQUIRED' in _FILL_BEARING_STATES
  settings = _settings()
  filled_pairs = [_make_pair(f'pair-hx1-p1s-{i}', 'PARTIAL_ONE_SIDE') for i in range(settings.max_open_pairs)]
  assert can_open_new_pair(filled_pairs, Decimal('500.00'), settings) is False, (
    'HX1-A: max_open_pairs fill-bearing pairs should block new pair'
  )


def test_can_open_new_pair_locked_counts_as_active_and_submitting_does_not() -> None:
  settings = _settings()
  locked_pairs = [_make_pair(f'pair-hx1-locked-{i}', 'LOCKED') for i in range(settings.max_open_pairs)]
  submitting_pair = _make_pair('pair-hx1-submitting-extra', 'SUBMITTING')
  all_pairs = locked_pairs + [submitting_pair]
  assert can_open_new_pair(all_pairs, Decimal('500.00'), settings) is False, (
    'HX1-A: max_open_pairs LOCKED pairs should block regardless of extra SUBMITTING pair'
  )


def _guard_plan(*, yes_price: Decimal = Decimal('0.40'), no_price: Decimal = Decimal('0.41')) -> PairOrderPlan:
  return PairOrderPlan(
    pair_id='pair-coverability-risk',
    ticker='KALSHI-COVERABILITY',
    yes_price=yes_price,
    no_price=no_price,
    contract_count=Decimal('10'),
    yes_client_order_id='pair-coverability-risk-yes',
    no_client_order_id='pair-coverability-risk-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )


def test_pre_submit_coverability_static_fails_closed_until_thresholds_set() -> None:
  settings = _settings()

  result = evaluate_pre_submit_coverability_static(
    _guard_plan(),
    settings,
    best_yes_bid=Decimal('0.40'),
    best_no_bid=Decimal('0.41'),
  )

  assert result.ok is False
  assert result.reason == 'coverability_threshold_unset'
  assert result.detail == {'threshold': 'flow_participation_k'}


def test_pre_submit_coverability_static_rejects_divergence_and_stale_maker_price() -> None:
  settings = Settings(**{**_settings().__dict__, 'flow_participation_k': 1.0, 'max_divergence': 0.30})

  divergent = evaluate_pre_submit_coverability_static(
    _guard_plan(yes_price=Decimal('0.10'), no_price=Decimal('0.55')),
    settings,
    best_yes_bid=Decimal('0.10'),
    best_no_bid=Decimal('0.55'),
  )
  assert divergent.ok is False
  assert divergent.reason == 'coverability_divergence_blocked'

  stale_maker = evaluate_pre_submit_coverability_static(
    _guard_plan(yes_price=Decimal('0.39'), no_price=Decimal('0.41')),
    settings,
    best_yes_bid=Decimal('0.40'),
    best_no_bid=Decimal('0.41'),
  )
  assert stale_maker.ok is False
  assert stale_maker.reason == 'coverability_maker_price_blocked'


def test_flow_coverability_requires_each_side_to_clear_participation_floor() -> None:
  settings = Settings(**{**_settings().__dict__, 'flow_participation_k': 1.0, 'max_divergence': 0.30})

  rejected = evaluate_flow_coverability(
    Decimal('10'),
    Decimal('9'),
    Decimal('10'),
    settings,
  )
  assert rejected.ok is False
  assert rejected.reason == 'coverability_flow_blocked'

  accepted = evaluate_flow_coverability(
    Decimal('10'),
    Decimal('10'),
    Decimal('10'),
    settings,
  )
  assert accepted.ok is True
