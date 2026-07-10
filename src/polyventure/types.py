from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal


PriceRange = tuple[Decimal, Decimal]


ALLOWED_PAIR_STATES = frozenset({
  'DISCOVERED',
  'PLANNED',
  'SUBMITTING',
  'RESTING_BOTH',
  'PARTIAL_ONE_SIDE',
  'PARTIAL_BOTH',
  'ASYMMETRIC_EXPOSURE',
  'REPAIR_LIVE',
  'EXPOSURE_CAPPED',
  'RECONCILE_REQUIRED',
  'SETTLED',
  'SETTLED_EXPOSURE',
  'LOCKED',
  'FILLED',
  'CANCELING',
  'CANCELED',
  'ERROR',
})


@dataclass(frozen=True)
class AccountBucketLimit:
  refill_rate: int
  bucket_capacity: int


@dataclass(frozen=True)
class AccountLimits:
  usage_tier: str
  read: AccountBucketLimit
  write: AccountBucketLimit


@dataclass(frozen=True)
class MarketSnapshot:
  ticker: str
  title: str | None
  close_time: datetime | None
  status: str
  yes_bid_dollars: Decimal
  no_bid_dollars: Decimal
  volume_24h_fp: Decimal
  open_interest_fp: Decimal
  event_ticker: str = ''
  yes_sub_title: str = ''
  no_sub_title: str = ''
  open_time: datetime | None = None
  latest_expiration_time: datetime | None = None
  yes_ask_dollars: Decimal = Decimal('0')
  no_ask_dollars: Decimal = Decimal('0')
  yes_bid_size_fp: Decimal = Decimal('0')
  yes_ask_size_fp: Decimal = Decimal('0')
  volume_fp: Decimal = Decimal('0')
  can_close_early: bool = False
  rules_primary: str = ''
  rules_secondary: str = ''
  price_ranges: tuple[PriceRange, ...] = ()
  series_ticker: str = ''
  category: str = ''
  price_level_structure: str = ''
  floor_strike: str = ''
  cap_strike: str = ''
  mve_collection_ticker: str = ''
  mve_selected_legs: tuple[str, ...] = ()
  binary_suitability_status: str = ''
  binary_suitability_reason: str = ''
  binary_suitability_event_ticker: str = ''
  binary_suitability_series_ticker: str = ''
  binary_suitability_category: str = ''
  binary_suitability_market_count: int = 0
  binary_suitability_sibling_tickers: tuple[str, ...] = ()


@dataclass(frozen=True)
class EventSnapshot:
  event_ticker: str
  series_ticker: str = ''
  category: str = ''
  title: str = ''
  mutually_exclusive: bool | None = None
  markets: tuple[MarketSnapshot, ...] = ()


@dataclass(frozen=True)
class BinarySuitability:
  status: str
  reason: str
  event_ticker: str = ''
  series_ticker: str = ''
  category: str = ''
  market_count: int = 0
  sibling_tickers: tuple[str, ...] = ()


@dataclass(frozen=True)
class CandidatePair:
  ticker: str
  seconds_to_close: int
  target_yes_bid: Decimal
  target_no_bid: Decimal
  edge_gross_per_contract: Decimal
  fee_reserve_per_contract: Decimal
  edge_net_per_contract: Decimal
  asymmetry: Decimal
  max_size_contracts: Decimal
  ranking_key: tuple[Decimal, Decimal, Decimal, Decimal, int]
  binary_suitability_status: str = ''
  binary_suitability_reason: str = ''
  binary_suitability_event_ticker: str = ''
  binary_suitability_series_ticker: str = ''
  binary_suitability_category: str = ''
  binary_suitability_market_count: int = 0
  binary_suitability_sibling_tickers: tuple[str, ...] = ()


@dataclass(frozen=True)
class PairOrderPlan:
  pair_id: str
  ticker: str
  yes_price: Decimal
  no_price: Decimal
  contract_count: Decimal
  yes_client_order_id: str
  no_client_order_id: str
  time_in_force: str
  post_only: bool
  cancel_order_on_pause: bool
  subaccount: int


@dataclass(frozen=True)
class FillEvent:
  fill_id: str
  pair_id: str
  order_id: str
  client_order_id: str
  side: str
  price_dollars: Decimal
  contract_count: Decimal
  fee_dollars: Decimal
  created_at: datetime


@dataclass(frozen=True)
class PairPnlSnapshot:
  pair_id: str
  locked_contracts: Decimal
  gross_dollars: Decimal
  net_projected_dollars: Decimal
  net_realized_dollars: Decimal
  recorded_at: datetime


@dataclass(frozen=True)
class OrderbookSnapshot:
  ticker: str
  yes_bids: tuple[PriceRange, ...]
  no_bids: tuple[PriceRange, ...]
  best_yes_bid: Decimal | None
  best_no_bid: Decimal | None
  best_yes_ask_implied: Decimal | None
  best_no_ask_implied: Decimal | None
  captured_at: datetime
  last_seq: int | None = None


@dataclass(frozen=True)
class CatchupOrderPlan:
  """Result of the SSOT unmatched-exposure catch-up evaluation (Lane C).

  `submit` is True only when the edge-preservation inequality holds AND there is
  fillable contra depth within that edge; `limit_price_dollars` is the domain
  price for the deficient leg (the most aggressive price still satisfying the
  edge inequality)."""
  submit: bool
  deficient_side: str
  limit_price_dollars: Decimal
  fillable_contracts: Decimal
  reason: str


@dataclass(frozen=True)
class PairRuntimeState:
  pair_id: str
  state: str
  yes_filled_contracts: Decimal
  no_filled_contracts: Decimal
  average_yes_price: Decimal
  average_no_price: Decimal
  realized_fees_dollars: Decimal
  last_update_at: datetime
  websocket_connected: bool = True

  def __post_init__(self) -> None:
    if self.state not in ALLOWED_PAIR_STATES:
      allowed = ', '.join(sorted(ALLOWED_PAIR_STATES))
      raise ValueError(f'Unsupported pair state {self.state!r}. Allowed states: {allowed}')

  @property
  def locked_contracts(self) -> Decimal:
    return min(self.yes_filled_contracts, self.no_filled_contracts)

  @property
  def unmatched_contracts(self) -> Decimal:
    return abs(self.yes_filled_contracts - self.no_filled_contracts)


@dataclass(frozen=True)
class SubmittedOrder:
  order_id: str
  client_order_id: str
  ticker: str
  side: str
  price_dollars: Decimal
  contract_count: Decimal
  remaining_count: Decimal
  fill_count: Decimal
  status: str
  created_at: datetime
  cancel_order_on_pause: bool
  subaccount: int
  reduced_by: Decimal = Decimal('0')


PairState = PairRuntimeState


@dataclass(frozen=True)
class PairPosition:
  ticker: str
  side: str
  contract_count: Decimal
  average_price_dollars: Decimal
  realized_pnl_dollars: Decimal = Decimal('0')
  fees_dollars: Decimal = Decimal('0')
  market_exposure_dollars: Decimal = Decimal('0')
  position_fp: Decimal = Decimal('0')


@dataclass(frozen=True)
class PairPnl:
  pair_id: str
  locked_contracts: Decimal
  gross_dollars: Decimal
  net_projected_dollars: Decimal
  net_realized_dollars: Decimal
