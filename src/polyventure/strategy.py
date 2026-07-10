from __future__ import annotations

from datetime import UTC, datetime
from decimal import ROUND_FLOOR, Decimal
from uuid import uuid4

from .config import Settings
from .types import (
  BinarySuitability,
  CandidatePair,
  CatchupOrderPlan,
  EventSnapshot,
  MarketSnapshot,
  OrderbookSnapshot,
  PairOrderPlan,
)


def _clamp_decimal(value: Decimal, lower: Decimal, upper: Decimal) -> Decimal:
  return min(upper, max(lower, value))


def _safe_decimal_ratio(numerator: Decimal, denominator: Decimal, *, fallback: Decimal) -> Decimal:
  if denominator <= 0:
    return fallback
  return numerator / denominator


def compute_candidate_liquidity_score(candidate: CandidatePair) -> Decimal:
  volume_24h, open_interest, _seconds_to_close = candidate.ranking_key[2:]
  return Decimal(str(volume_24h)) + Decimal(str(open_interest))


def compute_candidate_density_weight(candidate: CandidatePair, settings: Settings) -> Decimal:
  edge_ref = Decimal(str(settings.density_edge_ref))
  liquidity_ref = Decimal(str(settings.density_liquidity_ref))
  edge_ratio = _safe_decimal_ratio(
    candidate.edge_net_per_contract,
    edge_ref,
    fallback=Decimal('1'),
  )
  liquidity_ratio = _safe_decimal_ratio(
    compute_candidate_liquidity_score(candidate),
    liquidity_ref,
    fallback=Decimal('1'),
  )
  edge_weight = _clamp_decimal(edge_ratio, Decimal('0.75'), Decimal('1.25'))
  liquidity_weight = _clamp_decimal(liquidity_ratio, Decimal('0.75'), Decimal('1.25'))
  return edge_weight * liquidity_weight


def compute_instantaneous_qualifying_density(
  candidates: list[CandidatePair],
  settings: Settings,
) -> Decimal:
  if not candidates:
    return Decimal('0')
  return sum(
    (compute_candidate_density_weight(candidate, settings) for candidate in candidates),
    start=Decimal('0'),
  )


def compute_effective_qualifying_density(
  instantaneous_density: Decimal,
  settings: Settings,
  *,
  previous_density: Decimal | None = None,
) -> Decimal:
  alpha = _clamp_decimal(
    Decimal(str(settings.density_alpha)),
    Decimal('0'),
    Decimal('1'),
  )
  baseline = instantaneous_density if previous_density is None else previous_density
  return (alpha * instantaneous_density) + ((Decimal('1') - alpha) * baseline)


def compute_dynamic_pair_notional_pct(
  effective_density: Decimal,
  settings: Settings,
) -> Decimal:
  min_pct = Decimal(str(settings.min_pair_notional_pct))
  max_pct = Decimal(str(settings.max_pair_notional_pct))
  deployable_pct = Decimal(str(settings.target_deployment_pct))
  divisor = max(Decimal('1'), effective_density)
  raw_pct = _safe_decimal_ratio(deployable_pct, divisor, fallback=max_pct)
  return _clamp_decimal(raw_pct, min_pct, max_pct)


def compute_dynamic_pair_notional_cap_dollars(
  equity_dollars: Decimal,
  effective_density: Decimal,
  settings: Settings,
) -> Decimal:
  return equity_dollars * compute_dynamic_pair_notional_pct(effective_density, settings)


def compute_dynamic_max_contracts(
  candidate: CandidatePair,
  equity_dollars: Decimal,
  effective_density: Decimal,
  settings: Settings,
) -> Decimal:
  fee_reserve = Decimal(str(settings.fee_reserve_dollars))
  total_per_contract = candidate.target_yes_bid + candidate.target_no_bid + fee_reserve
  if total_per_contract <= 0:
    raise ValueError('Per-contract spend must be positive.')
  dynamic_cap_dollars = compute_dynamic_pair_notional_cap_dollars(
    equity_dollars,
    effective_density,
    settings,
  )
  return (dynamic_cap_dollars / total_per_contract).to_integral_value(rounding=ROUND_FLOOR)


def _market_has_range_shape(market: MarketSnapshot) -> bool:
  text_fields = (
    market.title or '',
    market.yes_sub_title,
    market.no_sub_title,
    market.rules_primary,
    market.rules_secondary,
  )
  has_structured_range = bool(
    market.price_ranges
    or market.price_level_structure
    or market.floor_strike
    or market.cap_strike
  )
  if has_structured_range:
    return True
  normalized = ' '.join(text_fields).lower()
  return any(token in normalized for token in (' to ', ' or above', ' or below', 'between '))


def _market_has_multivariate_shape(market: MarketSnapshot) -> bool:
  return bool(market.mve_collection_ticker or market.mve_selected_legs)


def classify_binary_suitability(
  market: MarketSnapshot,
  event: EventSnapshot | None,
) -> BinarySuitability:
  event_ticker = market.event_ticker or (event.event_ticker if event is not None else '')
  if not event_ticker:
    return BinarySuitability(status='unknown', reason='binary_suitability_unknown')
  if event is None:
    return BinarySuitability(
      status='unknown',
      reason='event_readback_missing',
      event_ticker=event_ticker,
    )
  active_siblings = tuple(
    sibling for sibling in event.markets
    if str(sibling.status or '').strip().lower() in {'open', 'active'}
  )
  sibling_tickers = tuple(str(sibling.ticker) for sibling in active_siblings if sibling.ticker)
  if _market_has_multivariate_shape(market) or any(_market_has_multivariate_shape(sibling) for sibling in active_siblings):
    return BinarySuitability(
      status='rejected',
      reason='multivariate_event',
      event_ticker=event_ticker,
      series_ticker=event.series_ticker,
      category=event.category,
      market_count=len(active_siblings),
      sibling_tickers=sibling_tickers,
    )
  if len(active_siblings) > 1:
    if event.mutually_exclusive is True:
      reason = 'multi_lane_range_event' if any(_market_has_range_shape(sibling) for sibling in active_siblings) else 'non_binary_event_family'
    elif any(_market_has_range_shape(sibling) for sibling in active_siblings):
      reason = 'multi_lane_range_event'
    else:
      reason = 'binary_suitability_unknown'
    return BinarySuitability(
      status='rejected' if reason != 'binary_suitability_unknown' else 'unknown',
      reason=reason,
      event_ticker=event_ticker,
      series_ticker=event.series_ticker,
      category=event.category,
      market_count=len(active_siblings),
      sibling_tickers=sibling_tickers,
    )
  if _market_has_range_shape(market) and event.mutually_exclusive is True:
    return BinarySuitability(
      status='rejected',
      reason='rules_indicate_ladder',
      event_ticker=event_ticker,
      series_ticker=event.series_ticker,
      category=event.category,
      market_count=len(active_siblings),
      sibling_tickers=sibling_tickers,
    )
  return BinarySuitability(
    status='eligible',
    reason='binary_event_family',
    event_ticker=event_ticker,
    series_ticker=event.series_ticker,
    category=event.category,
    market_count=len(active_siblings),
    sibling_tickers=sibling_tickers,
  )


def reprice_candidate(
  candidate: CandidatePair,
  target_yes_bid: Decimal,
  target_no_bid: Decimal,
  settings: Settings,
) -> CandidatePair:
  fee_reserve = Decimal(str(settings.fee_reserve_dollars))
  edge_gross = Decimal('1') - target_yes_bid - target_no_bid
  edge_net = edge_gross - fee_reserve
  asymmetry = abs(target_yes_bid - target_no_bid)
  ranking_key = (
    edge_net,
    -asymmetry,
    candidate.ranking_key[2],
    candidate.ranking_key[3],
    candidate.ranking_key[4],
  )
  return CandidatePair(
    ticker=candidate.ticker,
    seconds_to_close=candidate.seconds_to_close,
    target_yes_bid=target_yes_bid,
    target_no_bid=target_no_bid,
    edge_gross_per_contract=edge_gross,
    fee_reserve_per_contract=fee_reserve,
    edge_net_per_contract=edge_net,
    asymmetry=asymmetry,
    max_size_contracts=candidate.max_size_contracts,
    ranking_key=ranking_key,
    binary_suitability_status=candidate.binary_suitability_status,
    binary_suitability_reason=candidate.binary_suitability_reason,
    binary_suitability_event_ticker=candidate.binary_suitability_event_ticker,
    binary_suitability_series_ticker=candidate.binary_suitability_series_ticker,
    binary_suitability_category=candidate.binary_suitability_category,
    binary_suitability_market_count=candidate.binary_suitability_market_count,
    binary_suitability_sibling_tickers=candidate.binary_suitability_sibling_tickers,
  )


def resolve_divergence_screen_threshold(settings: Settings) -> Decimal | None:
  """Resolve the scan-side divergence screen from the SAME operator setting the
  pre-submit coverability guard enforces (`max_divergence`).

  Returns None when the setting is unset or invalid: the screen rests inert and
  the authoritative fail-closed block at the pre-submit boundary remains the
  sole money-path authority (selection-alignment BMAP 2026-07-02, decision D-C).
  """
  raw = getattr(settings, 'max_divergence', None)
  if raw is None or str(raw).strip() == '':
    return None
  try:
    resolved = Decimal(str(raw))
  except (ArithmeticError, ValueError):
    return None
  if resolved <= 0 or resolved > Decimal('1'):
    return None
  return resolved


def find_candidates(
  markets: list[MarketSnapshot],
  now: datetime,
  settings: Settings,
  *,
  screen_stats: dict[str, object] | None = None,
) -> list[CandidatePair]:
  accepted: list[CandidatePair] = []
  current = now.astimezone(UTC)
  fee_reserve = Decimal(str(settings.fee_reserve_dollars))
  min_edge = Decimal(str(settings.min_edge_dollars))
  min_profit = Decimal(str(settings.min_profit_dollars))
  max_pair_contracts = Decimal(str(settings.max_pair_contracts))
  divergence_screen = resolve_divergence_screen_threshold(settings)
  divergence_screened_count = 0

  for market in markets:
    if market.status.lower() != 'open' and market.status.lower() != 'active':
      continue
    if market.close_time is None:
      continue
    seconds_to_close = int((market.close_time - current).total_seconds())
    if seconds_to_close < settings.entry_window_end_sec:
      continue
    if seconds_to_close > settings.entry_window_start_sec:
      continue
    if not market.yes_bid_dollars or not market.no_bid_dollars:
      continue

    edge_gross = Decimal('1') - market.yes_bid_dollars - market.no_bid_dollars
    edge_net = edge_gross - fee_reserve
    if edge_gross < min_edge or edge_net < min_profit:
      continue

    asymmetry = abs(market.yes_bid_dollars - market.no_bid_dollars)
    if divergence_screen is not None and asymmetry > divergence_screen:
      # Selection alignment: reject at scan what the pre-submit coverability
      # guard would reject at the money boundary (same threshold, equality
      # passes on both surfaces). The pre-submit guard still runs unchanged.
      divergence_screened_count += 1
      continue
    ranking_key = (
      edge_net,
      -asymmetry,
      market.volume_24h_fp,
      market.open_interest_fp,
      Decimal(str(-seconds_to_close)),
    )
    accepted.append(
      CandidatePair(
        ticker=market.ticker,
        seconds_to_close=seconds_to_close,
        target_yes_bid=market.yes_bid_dollars,
        target_no_bid=market.no_bid_dollars,
        edge_gross_per_contract=edge_gross,
        fee_reserve_per_contract=fee_reserve,
        edge_net_per_contract=edge_net,
        asymmetry=asymmetry,
        max_size_contracts=max_pair_contracts,
        ranking_key=ranking_key,
        binary_suitability_status=market.binary_suitability_status,
        binary_suitability_reason=market.binary_suitability_reason,
        binary_suitability_event_ticker=market.binary_suitability_event_ticker,
        binary_suitability_series_ticker=market.binary_suitability_series_ticker,
        binary_suitability_category=market.binary_suitability_category,
        binary_suitability_market_count=market.binary_suitability_market_count,
        binary_suitability_sibling_tickers=market.binary_suitability_sibling_tickers,
      )
    )

  if screen_stats is not None:
    screen_stats['divergence_screen_applied'] = divergence_screen is not None
    screen_stats['divergence_screen_threshold'] = (
      str(divergence_screen) if divergence_screen is not None else None
    )
    screen_stats['divergence_screened_count'] = divergence_screened_count
  return sorted(accepted, key=lambda item: item.ranking_key, reverse=True)


def build_pair_order_plan(
  candidate: CandidatePair,
  available_balance: Decimal,
  settings: Settings,
) -> PairOrderPlan:
  per_contract_spend = candidate.target_yes_bid + candidate.target_no_bid
  fee_reserve = Decimal(str(settings.fee_reserve_dollars))
  total_per_contract = per_contract_spend + fee_reserve
  if total_per_contract <= 0:
    raise ValueError('Per-contract spend must be positive.')
  cash_limited_contracts = (available_balance / total_per_contract).to_integral_value(rounding=ROUND_FLOOR)
  contract_count = min(
    cash_limited_contracts,
    Decimal(str(settings.max_pair_contracts)),
    candidate.max_size_contracts,
  )
  if contract_count < 1:
    raise ValueError('Available balance is insufficient for one paired contract.')

  pair_id = 'pair-{suffix}'.format(suffix=uuid4().hex[:12])
  return PairOrderPlan(
    pair_id=pair_id,
    ticker=candidate.ticker,
    yes_price=candidate.target_yes_bid,
    no_price=candidate.target_no_bid,
    contract_count=contract_count,
    yes_client_order_id='{pair_id}-yes'.format(pair_id=pair_id),
    no_client_order_id='{pair_id}-no'.format(pair_id=pair_id),
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=settings.cancel_on_pause,
    subaccount=settings.subaccount,
  )


def compute_catchup_order(
  deficient_side: str,
  unmatched_count: Decimal,
  filled_price_dollars: Decimal,
  orderbook: OrderbookSnapshot,
  fee_reserve_dollars: Decimal,
) -> CatchupOrderPlan:
  """SSOT unmatched-exposure catch-up evaluation (WAGER_HEDGE_MODELS.md ~1317-1331).

  After a one-sided fill, decide whether the deficient leg can be completed by a
  crossing order whose price still preserves the edge:

      p_filled + p_catchup + fee_reserve <= 1

  Authoritative price comes only from the live orderbook (PD-1). To buy the
  deficient side we cross the opposite bid stack; lifting a contra bid at price
  ``b`` costs ``1 - b`` for the deficient contract, so the edge inequality permits
  any contra bid with ``b >= filled_price + fee_reserve``. Fillable depth is the
  summed size of qualifying contra levels, capped at the unmatched quantity.

  Returns ``submit=True`` only when the inequality holds and there is fillable
  depth within the edge; otherwise ``submit=False`` with a names-only reason and
  the residual is frozen to ``ERROR`` by the caller (V1 never crosses past the
  edge)."""
  if deficient_side not in {'yes', 'no'}:
    raise ValueError('deficient_side must be yes or no.')
  if unmatched_count <= 0:
    return CatchupOrderPlan(False, deficient_side, Decimal('0'), Decimal('0'), 'no_unmatched_quantity')

  max_price = Decimal('1') - filled_price_dollars - fee_reserve_dollars
  if max_price <= 0:
    return CatchupOrderPlan(False, deficient_side, Decimal('0'), Decimal('0'), 'edge_inequality_failed')

  # Buying NO crosses the YES-bid stack; buying YES crosses the NO-bid stack.
  contra_levels = orderbook.yes_bids if deficient_side == 'no' else orderbook.no_bids
  min_contra_bid = Decimal('1') - max_price  # == filled_price + fee_reserve
  fillable = Decimal('0')
  for level in contra_levels:
    price = Decimal(str(level[0]))
    size = Decimal(str(level[1]))
    if price >= min_contra_bid:
      fillable += size
  fillable = min(fillable, unmatched_count)
  if fillable <= 0:
    return CatchupOrderPlan(False, deficient_side, max_price, Decimal('0'), 'no_fillable_depth_within_edge')

  return CatchupOrderPlan(True, deficient_side, max_price, fillable, 'catchup_available')


def summarize_depth_within_band(
  orderbook: OrderbookSnapshot,
  intended_yes_price: Decimal,
  intended_no_price: Decimal,
) -> dict[str, object]:
  """Pure: summarize authoritative resting depth for a coverability observation.

  "Within band" = resting contracts that would fill a maker order at our intended
  price, i.e. YES-bid levels priced >= ``intended_yes_price`` (symmetric for NO).
  This is a structural definition, not a tuned threshold. Returns the band sums,
  the full ladders (as ``[price, size]`` strings for storage), and the tops."""
  def _band(levels: tuple, threshold: Decimal) -> tuple[Decimal, list[list[str]]]:
    total = Decimal('0')
    ladder: list[list[str]] = []
    for level in levels:
      price = Decimal(str(level[0]))
      size = Decimal(str(level[1]))
      ladder.append([str(price), str(size)])
      if price >= threshold:
        total += size
    return total, ladder

  yes_within, yes_ladder = _band(orderbook.yes_bids, intended_yes_price)
  no_within, no_ladder = _band(orderbook.no_bids, intended_no_price)
  return {
    'yes_depth_within_band': yes_within,
    'no_depth_within_band': no_within,
    'yes_bid_depth_json': yes_ladder,
    'no_bid_depth_json': no_ladder,
    'best_yes_bid': orderbook.best_yes_bid,
    'best_no_bid': orderbook.best_no_bid,
  }
