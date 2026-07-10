from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from polyventure.config import Settings
from polyventure.strategy import (
  build_pair_order_plan,
  classify_binary_suitability,
  compute_candidate_density_weight,
  compute_dynamic_max_contracts,
  compute_dynamic_pair_notional_pct,
  compute_effective_qualifying_density,
  compute_instantaneous_qualifying_density,
  find_candidates,
  reprice_candidate,
)
from polyventure.types import EventSnapshot, MarketSnapshot


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


def _market(
  ticker: str,
  *,
  title: str = 'Will this binary event happen?',
  event_ticker: str = 'EVT-BINARY',
  yes_sub_title: str = '',
) -> MarketSnapshot:
  return MarketSnapshot(
    ticker=ticker,
    title=title,
    close_time=datetime.now(UTC) + timedelta(seconds=300),
    status='open',
    yes_bid_dollars=Decimal('0.35'),
    no_bid_dollars=Decimal('0.40'),
    volume_24h_fp=Decimal('125.00'),
    open_interest_fp=Decimal('80.00'),
    event_ticker=event_ticker,
    yes_sub_title=yes_sub_title,
  )


def test_binary_suitability_accepts_single_market_event_family() -> None:
  market = _market('KX-BINARY-YESNO')
  event = EventSnapshot(event_ticker='EVT-BINARY', mutually_exclusive=False, markets=(market,))

  suitability = classify_binary_suitability(market, event)

  assert suitability.status == 'eligible'
  assert suitability.reason == 'binary_event_family'


def test_binary_suitability_rejects_multi_lane_range_event_family() -> None:
  first = _market(
    'KX-MIA-LOW-79-80',
    title='Lowest temperature in Miami on Jun 23, 2026?',
    event_ticker='EVT-MIA-RANGE',
    yes_sub_title='79 to 80',
  )
  second = _market(
    'KX-MIA-ABOVE-83',
    title='Lowest temperature in Miami on Jun 23, 2026?',
    event_ticker='EVT-MIA-RANGE',
    yes_sub_title='83 or above',
  )
  event = EventSnapshot(event_ticker='EVT-MIA-RANGE', mutually_exclusive=True, markets=(first, second))

  suitability = classify_binary_suitability(first, event)

  assert suitability.status == 'rejected'
  assert suitability.reason == 'multi_lane_range_event'


def test_binary_suitability_unknown_without_event_readback() -> None:
  market = _market('KX-BINARY-NO-EVENT')

  suitability = classify_binary_suitability(market, None)

  assert suitability.status == 'unknown'
  assert suitability.reason == 'event_readback_missing'


def test_find_candidates_accepts_positive_edge_market() -> None:
  now = datetime.now(UTC)
  markets = [
    MarketSnapshot(
      ticker='KALSHI-EDGE-1',
      title='Demo candidate',
      close_time=now + timedelta(seconds=300),
      status='open',
      yes_bid_dollars=Decimal('0.35'),
      no_bid_dollars=Decimal('0.40'),
      volume_24h_fp=Decimal('125.00'),
      open_interest_fp=Decimal('80.00'),
    )
  ]

  candidates = find_candidates(markets, now, _settings())

  assert len(candidates) == 1
  assert candidates[0].ticker == 'KALSHI-EDGE-1'
  assert candidates[0].edge_gross_per_contract == Decimal('0.25')
  assert candidates[0].edge_net_per_contract == Decimal('0.23')


def test_find_candidates_orders_by_edge_then_asymmetry() -> None:
  now = datetime.now(UTC)
  markets = [
    MarketSnapshot(
      ticker='KALSHI-EDGE-LOW',
      title='Lower edge',
      close_time=now + timedelta(seconds=200),
      status='open',
      yes_bid_dollars=Decimal('0.42'),
      no_bid_dollars=Decimal('0.42'),
      volume_24h_fp=Decimal('400.00'),
      open_interest_fp=Decimal('100.00'),
    ),
    MarketSnapshot(
      ticker='KALSHI-EDGE-HIGH',
      title='Higher edge',
      close_time=now + timedelta(seconds=200),
      status='open',
      yes_bid_dollars=Decimal('0.33'),
      no_bid_dollars=Decimal('0.39'),
      volume_24h_fp=Decimal('50.00'),
      open_interest_fp=Decimal('20.00'),
    ),
  ]

  candidates = find_candidates(markets, now, _settings())

  assert [candidate.ticker for candidate in candidates] == [
    'KALSHI-EDGE-HIGH',
    'KALSHI-EDGE-LOW',
  ]


def test_candidate_density_weight_favors_stronger_edge_and_liquidity() -> None:
  now = datetime.now(UTC)
  markets = [
    MarketSnapshot(
      ticker='KALSHI-DENSITY-HIGH',
      title='High density weight',
      close_time=now + timedelta(seconds=300),
      status='open',
      yes_bid_dollars=Decimal('0.33'),
      no_bid_dollars=Decimal('0.39'),
      volume_24h_fp=Decimal('200.00'),
      open_interest_fp=Decimal('125.00'),
    ),
    MarketSnapshot(
      ticker='KALSHI-DENSITY-LOW',
      title='Low density weight',
      close_time=now + timedelta(seconds=300),
      status='open',
      yes_bid_dollars=Decimal('0.42'),
      no_bid_dollars=Decimal('0.42'),
      volume_24h_fp=Decimal('20.00'),
      open_interest_fp=Decimal('10.00'),
    ),
  ]

  candidates = find_candidates(markets, now, _settings())
  weight_by_ticker = {
    candidate.ticker: compute_candidate_density_weight(candidate, _settings())
    for candidate in candidates
  }

  assert weight_by_ticker['KALSHI-DENSITY-HIGH'] > weight_by_ticker['KALSHI-DENSITY-LOW']


def test_dynamic_pair_notional_pct_shrinks_as_density_rises() -> None:
  settings = _settings()

  low_density_pct = compute_dynamic_pair_notional_pct(Decimal('2'), settings)
  high_density_pct = compute_dynamic_pair_notional_pct(Decimal('8'), settings)

  assert low_density_pct == Decimal('0.20')
  assert high_density_pct == Decimal('0.075')
  assert low_density_pct > high_density_pct


def test_dynamic_pair_notional_pct_respects_floor_and_ceiling() -> None:
  settings = _settings()

  capped_high = compute_dynamic_pair_notional_pct(Decimal('1'), settings)
  floored_low = compute_dynamic_pair_notional_pct(Decimal('50'), settings)

  assert capped_high == Decimal('0.20')
  assert floored_low == Decimal('0.05')


def test_effective_density_applies_smoothing_against_previous_value() -> None:
  settings = _settings()

  effective_density = compute_effective_qualifying_density(
    Decimal('6'),
    settings,
    previous_density=Decimal('2'),
  )

  assert effective_density == Decimal('2.8')


def test_instantaneous_density_accumulates_candidate_weights() -> None:
  now = datetime.now(UTC)
  markets = [
    MarketSnapshot(
      ticker='KALSHI-INSTANT-1',
      title='Instant density one',
      close_time=now + timedelta(seconds=300),
      status='open',
      yes_bid_dollars=Decimal('0.35'),
      no_bid_dollars=Decimal('0.39'),
      volume_24h_fp=Decimal('100.00'),
      open_interest_fp=Decimal('80.00'),
    ),
    MarketSnapshot(
      ticker='KALSHI-INSTANT-2',
      title='Instant density two',
      close_time=now + timedelta(seconds=300),
      status='open',
      yes_bid_dollars=Decimal('0.36'),
      no_bid_dollars=Decimal('0.39'),
      volume_24h_fp=Decimal('80.00'),
      open_interest_fp=Decimal('40.00'),
    ),
  ]

  candidates = find_candidates(markets, now, _settings())

  density = compute_instantaneous_qualifying_density(candidates, _settings())

  assert density > Decimal('0')
  assert density == sum(
    (compute_candidate_density_weight(candidate, _settings()) for candidate in candidates),
    start=Decimal('0'),
  )


def test_dynamic_max_contracts_respects_density_regime() -> None:
  now = datetime.now(UTC)
  markets = [
    MarketSnapshot(
      ticker='KALSHI-CAP-TEST',
      title='Cap test market',
      close_time=now + timedelta(seconds=300),
      status='open',
      yes_bid_dollars=Decimal('0.33'),
      no_bid_dollars=Decimal('0.39'),
      volume_24h_fp=Decimal('120.00'),
      open_interest_fp=Decimal('75.00'),
    )
  ]
  candidate = find_candidates(markets, now, _settings())[0]

  low_density_contracts = compute_dynamic_max_contracts(
    candidate,
    Decimal('1000.00'),
    Decimal('2'),
    _settings(),
  )
  high_density_contracts = compute_dynamic_max_contracts(
    candidate,
    Decimal('1000.00'),
    Decimal('12'),
    _settings(),
  )

  assert low_density_contracts > high_density_contracts
  assert low_density_contracts == Decimal('270')
  assert high_density_contracts == Decimal('67')


def test_reprice_candidate_recomputes_price_derived_math() -> None:
  now = datetime.now(UTC)
  candidate = find_candidates(
    [
      MarketSnapshot(
        ticker='KALSHI-REPRICE',
        title='Reprice candidate',
        close_time=now + timedelta(seconds=300),
        status='open',
        yes_bid_dollars=Decimal('0.34'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('120.00'),
        open_interest_fp=Decimal('75.00'),
      )
    ],
    now,
    _settings(),
  )[0]

  repriced = reprice_candidate(candidate, Decimal('0.41'), Decimal('0.42'), _settings())

  assert repriced.ticker == candidate.ticker
  assert repriced.seconds_to_close == candidate.seconds_to_close
  assert repriced.max_size_contracts == candidate.max_size_contracts
  assert repriced.target_yes_bid == Decimal('0.41')
  assert repriced.target_no_bid == Decimal('0.42')
  assert repriced.edge_gross_per_contract == Decimal('0.17')
  assert repriced.fee_reserve_per_contract == Decimal('0.02')
  assert repriced.edge_net_per_contract == Decimal('0.15')
  assert repriced.asymmetry == Decimal('0.01')
  assert repriced.ranking_key == (
    Decimal('0.15'),
    Decimal('-0.01'),
    candidate.ranking_key[2],
    candidate.ranking_key[3],
    candidate.ranking_key[4],
  )


def _screen_market(ticker: str, yes_bid: str, no_bid: str, now: datetime) -> MarketSnapshot:
  return MarketSnapshot(
    ticker=ticker,
    title='Divergence screen market',
    close_time=now + timedelta(seconds=300),
    status='open',
    yes_bid_dollars=Decimal(yes_bid),
    no_bid_dollars=Decimal(no_bid),
    volume_24h_fp=Decimal('100.00'),
    open_interest_fp=Decimal('50.00'),
  )


def test_divergence_screen_rejects_above_threshold_and_passes_equality() -> None:
  now = datetime.now(UTC)
  settings = replace(_settings(), max_divergence=0.3)
  markets = [
    _screen_market('KALSHI-DIV-EQUAL', '0.60', '0.30', now),   # divergence 0.30 == threshold: passes
    _screen_market('KALSHI-DIV-OVER', '0.61', '0.30', now),    # divergence 0.31 > threshold: screened
    _screen_market('KALSHI-DIV-BALANCED', '0.45', '0.45', now),
  ]
  stats: dict[str, object] = {}

  candidates = find_candidates(markets, now, settings, screen_stats=stats)

  tickers = [candidate.ticker for candidate in candidates]
  assert 'KALSHI-DIV-OVER' not in tickers
  assert 'KALSHI-DIV-EQUAL' in tickers
  assert 'KALSHI-DIV-BALANCED' in tickers
  assert stats['divergence_screen_applied'] is True
  assert stats['divergence_screen_threshold'] == '0.3'
  assert stats['divergence_screened_count'] == 1


def test_divergence_screen_inert_when_threshold_unset() -> None:
  now = datetime.now(UTC)
  settings = _settings()
  assert settings.max_divergence is None
  markets = [_screen_market('KALSHI-DIV-WIDE', '0.75', '0.10', now)]
  stats: dict[str, object] = {}

  candidates = find_candidates(markets, now, settings, screen_stats=stats)

  assert [candidate.ticker for candidate in candidates] == ['KALSHI-DIV-WIDE']
  assert stats['divergence_screen_applied'] is False
  assert stats['divergence_screen_threshold'] is None
  assert stats['divergence_screened_count'] == 0


def test_equal_edge_ranking_prefers_balanced_book() -> None:
  now = datetime.now(UTC)
  settings = replace(_settings(), max_divergence=0.5)
  markets = [
    # Same edge_gross (0.25) and edge_net; only divergence differs.
    _screen_market('KALSHI-RANK-LOPSIDED', '0.55', '0.20', now),
    _screen_market('KALSHI-RANK-BALANCED', '0.38', '0.37', now),
  ]

  candidates = find_candidates(markets, now, settings)

  assert [candidate.ticker for candidate in candidates] == [
    'KALSHI-RANK-BALANCED',
    'KALSHI-RANK-LOPSIDED',
  ]


def test_build_pair_order_plan_floors_cash_limited_contracts() -> None:
  now = datetime.now(UTC)
  candidate = find_candidates(
    [
      MarketSnapshot(
        ticker='KALSHI-FLOOR',
        title='Floor candidate',
        close_time=now + timedelta(seconds=300),
        status='open',
        yes_bid_dollars=Decimal('0.30'),
        no_bid_dollars=Decimal('0.40'),
        volume_24h_fp=Decimal('120.00'),
        open_interest_fp=Decimal('75.00'),
      )
    ],
    now,
    _settings(),
  )[0]

  plan = build_pair_order_plan(candidate, Decimal('1.82'), _settings())

  assert plan.contract_count == Decimal('2')


def _zero_spread_market(now: datetime) -> MarketSnapshot:
  return MarketSnapshot(
    ticker='KALSHI-ZERO-SPREAD-SIM',
    title='Simulation zero-spread market',
    close_time=now + timedelta(seconds=300),
    status='open',
    yes_bid_dollars=Decimal('0.50'),
    no_bid_dollars=Decimal('0.50'),
    volume_24h_fp=Decimal('100.00'),
    open_interest_fp=Decimal('50.00'),
  )


def test_simulation_inject_find_candidates_accepts_zero_spread_market_with_inject_settings() -> None:
  now = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
  from dataclasses import replace
  inject_settings = replace(_settings(), min_edge_dollars=-1.0, min_profit_dollars=-1.0)
  result = find_candidates([_zero_spread_market(now)], now, inject_settings)
  assert len(result) == 1
  assert result[0].ticker == 'KALSHI-ZERO-SPREAD-SIM'


def test_simulation_inject_find_candidates_rejects_zero_spread_market_with_normal_settings() -> None:
  now = datetime(2026, 6, 10, 12, 0, tzinfo=UTC)
  result = find_candidates([_zero_spread_market(now)], now, _settings())
  assert len(result) == 0
