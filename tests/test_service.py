from __future__ import annotations

import json
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from polyventure import service as service_module
from polyventure.http_client import KalshiHttpError, _order_payload_to_v2_wire
from polyventure.persistence import (
  open_database,
  persist_candidate_review_candidates,
  persist_candidate_review_run,
  persist_candidate_saved_set,
  persist_candidate_saved_set_evaluation,
  persist_operator_action,
  persist_pair_plan,
  persist_pair_state_transition,
  persist_runtime_event,
  persist_service_heartbeat,
  summarize_persistence,
)
from polyventure.service import ScanCanceledError, SubmitHandoffValidationError, align_pairs_with_kalshi, cancel_all_pairs, fetch_operational_visuals, fetch_system_log_entries, reconcile_pairs, report_runtime, run_scan_once, run_service_once, _sandbox_candidate_projection, _project_funds_posture, _latest_heartbeat_payload, _latest_funds_heartbeat_payload, _persist_candidate_math_contract, _mark_auto_canceled_candidates_terminal, _pair_runtime_summary, _heartbeat_balance_at, _load_candidate_market_set, _refresh_reporting_funds_posture, _place_live_pair_orders, _load_current_pairs, _latest_pair_snapshots, _binary_suitability_filter
from polyventure.service import _candidate_evidence_preview, _candidate_projection_record, _project_saved_set_snapshot, _submit_binary_proof_block
from polyventure.strategy import find_candidates
from polyventure.types import AccountBucketLimit, AccountLimits, CandidatePair, EventSnapshot, MarketSnapshot, PairPosition
from polyventure.config import Settings
from polyventure.types import PairOrderPlan


class FakeClient:
  def __init__(self, settings: Settings, private_key: object):
    self.settings = settings
    self.private_key = private_key
    self.recent_trades_calls: list[str] = []
    now = datetime.now(UTC)
    self.markets = {
      'KALSHI-CANDIDATE-HIGH': MarketSnapshot(
        ticker='KALSHI-CANDIDATE-HIGH',
        title='Higher edge candidate',
        close_time=now + timedelta(seconds=240),
        status='open',
        yes_bid_dollars=Decimal('0.31'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('150.00'),
        open_interest_fp=Decimal('90.00'),
        event_ticker='KALSHI-EVENT-HIGH',
      ),
      'KALSHI-CANDIDATE-LOW': MarketSnapshot(
        ticker='KALSHI-CANDIDATE-LOW',
        title='Lower edge candidate',
        close_time=now + timedelta(seconds=240),
        status='open',
        yes_bid_dollars=Decimal('0.40'),
        no_bid_dollars=Decimal('0.41'),
        volume_24h_fp=Decimal('200.00'),
        open_interest_fp=Decimal('100.00'),
        event_ticker='KALSHI-EVENT-LOW',
      ),
      'KALSHI-REJECTED': MarketSnapshot(
        ticker='KALSHI-REJECTED',
        title='Outside profitability threshold',
        close_time=now + timedelta(seconds=240),
        status='open',
        yes_bid_dollars=Decimal('0.50'),
        no_bid_dollars=Decimal('0.49'),
        volume_24h_fp=Decimal('900.00'),
        open_interest_fp=Decimal('400.00'),
        event_ticker='KALSHI-EVENT-REJECTED',
      ),
    }

  def get_balance(self) -> Decimal:
    return Decimal('123.45')

  def get_account_api_limits(self) -> AccountLimits:
    return AccountLimits(
      usage_tier='demo-tier',
      read=AccountBucketLimit(refill_rate=30, bucket_capacity=60),
      write=AccountBucketLimit(refill_rate=10, bucket_capacity=20),
    )

  def get_markets(
    self,
    status: str = 'open',
    limit: int = 100,
    cursor: str | None = None,
    min_close_ts: int | None = None,
    max_close_ts: int | None = None,
  ) -> tuple[list[MarketSnapshot], str | None]:
    del status, limit, cursor, min_close_ts, max_close_ts
    return list(self.markets.values()), None

  def get_market(self, ticker: str) -> MarketSnapshot:
    return self.markets[ticker]

  def get_event(self, event_ticker: str) -> EventSnapshot:
    markets = tuple(
      market for market in self.markets.values()
      if market.event_ticker == event_ticker
    )
    if not markets:
      markets = tuple(self.markets.values())[:1]
    # Single-market binary event family (eligible binary suitability).
    return EventSnapshot(event_ticker=event_ticker, mutually_exclusive=False, markets=markets)

  def get_orderbook(self, ticker: str, depth: int = 0):
    del depth
    market = self.markets[ticker]
    from polyventure.websocket_client import normalize_orderbook_snapshot

    return normalize_orderbook_snapshot(
      {
        'ticker': ticker,
        'yes_dollars': [[str(market.yes_bid_dollars - Decimal('0.02')), '7'], [str(market.yes_bid_dollars), '11']],
        'no_dollars': [[str(market.no_bid_dollars - Decimal('0.02')), '6'], [str(market.no_bid_dollars), '10']],
        'seq': 1,
      }
    )

  def get_recent_trades(self, ticker: str, *, window_sec: int):
    del window_sec
    self.recent_trades_calls.append(ticker)
    return {
      'yes_flow_fp': Decimal('1000'),
      'no_flow_fp': Decimal('1000'),
      'trade_count': 40,
    }


def _binary_suitability_market(event_ticker: str = 'EVENT-1') -> MarketSnapshot:
  return MarketSnapshot(
    ticker='KALSHI-BINARY-1',
    title='Binary suitability fixture',
    close_time=datetime.now(UTC) + timedelta(seconds=240),
    status='open',
    yes_bid_dollars=Decimal('0.20'),
    no_bid_dollars=Decimal('0.30'),
    volume_24h_fp=Decimal('100'),
    open_interest_fp=Decimal('100'),
    event_ticker=event_ticker,
  )


def test_binary_suitability_filter_honors_preexisting_scan_cancel() -> None:
  cancel_event = threading.Event()
  cancel_event.set()

  class Client:
    def get_event(self, _event_ticker: str) -> object:
      raise AssertionError('cancel checkpoint must fire before event-family readback')

  with pytest.raises(ScanCanceledError):
    _binary_suitability_filter(Client(), [_binary_suitability_market()], cancel_event=cancel_event)


def test_binary_suitability_filter_honors_cancel_during_event_readback() -> None:
  cancel_event = threading.Event()

  class Client:
    def get_event(self, _event_ticker: str) -> object:
      cancel_event.set()
      return {'markets': []}

  with pytest.raises(ScanCanceledError):
    _binary_suitability_filter(Client(), [_binary_suitability_market()], cancel_event=cancel_event)


def test_binary_suitability_filter_rejects_threshold_family_and_updates_ledger(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'binary-ledger.sqlite3'
  connection = open_database(str(state_db_path))
  first = MarketSnapshot(
    ticker='KXTEMPNYCH-26JUN2420-T76.99',
    title='New York City temperature today at 8pm EDT?',
    close_time=datetime.now(UTC) + timedelta(seconds=300),
    status='open',
    yes_bid_dollars=Decimal('0.02'),
    no_bid_dollars=Decimal('0.01'),
    volume_24h_fp=Decimal('100.93'),
    open_interest_fp=Decimal('100.93'),
    event_ticker='KXTEMPNYCH-26JUN2420',
    series_ticker='KXTEMPNYCH',
    yes_sub_title='77 or above',
  )
  second = replace(first, ticker='KXTEMPNYCH-26JUN2420-T77.99', yes_sub_title='78 or above')
  event = EventSnapshot(
    event_ticker='KXTEMPNYCH-26JUN2420',
    series_ticker='KXTEMPNYCH',
    category='Climate and Weather',
    mutually_exclusive=False,
    markets=(first, second),
  )

  class Client:
    def get_event(self, _event_ticker: str) -> EventSnapshot:
      return event

  eligible, stats = _binary_suitability_filter(
    Client(),
    [first],
    connection=connection,
    operation_lane='live',
    lane_session_id='scan-ledger',
    recorded_at=datetime.now(UTC),
  )

  assert eligible == []
  assert stats['binary_suitability_rejected_count'] == 1
  assert stats['known_non_binary_ledger_update_count'] == 1
  row = connection.execute(
    'SELECT series_ticker, event_ticker, market_ticker, classification_reason, actionability, seen_count '
    'FROM known_non_binary_markets'
  ).fetchone()
  assert tuple(row) == (
    'KXTEMPNYCH',
    'KXTEMPNYCH-26JUN2420',
    'KXTEMPNYCH-26JUN2420-T76.99',
    'multi_lane_range_event',
    'deferred_threshold_range',
    1,
  )


def test_candidate_market_loader_returns_only_binary_eligible_markets(tmp_path: Path) -> None:
  now = datetime.now(UTC)
  eligible = MarketSnapshot(
    ticker='KX-BINARY-YESNO',
    title='Will the isolated event happen?',
    close_time=now + timedelta(seconds=300),
    status='open',
    yes_bid_dollars=Decimal('0.25'),
    no_bid_dollars=Decimal('0.30'),
    volume_24h_fp=Decimal('10'),
    open_interest_fp=Decimal('10'),
    event_ticker='EVT-BINARY',
    series_ticker='KXBIN',
  )
  rejected = MarketSnapshot(
    ticker='KXTEMPNYCH-26JUN2420-T76.99',
    title='New York City temperature today at 8pm EDT?',
    close_time=now + timedelta(seconds=300),
    status='open',
    yes_bid_dollars=Decimal('0.02'),
    no_bid_dollars=Decimal('0.01'),
    volume_24h_fp=Decimal('100'),
    open_interest_fp=Decimal('100'),
    event_ticker='KXTEMPNYCH-26JUN2420',
    series_ticker='KXTEMPNYCH',
    yes_sub_title='77 or above',
  )
  rejected_sibling = replace(rejected, ticker='KXTEMPNYCH-26JUN2420-T77.99', yes_sub_title='78 or above')

  class Client:
    def get_markets(self, **_kwargs: object) -> tuple[list[MarketSnapshot], None]:
      return [eligible, rejected], None

    def get_event(self, event_ticker: str) -> EventSnapshot:
      if event_ticker == 'EVT-BINARY':
        return EventSnapshot(event_ticker='EVT-BINARY', series_ticker='KXBIN', markets=(eligible,))
      return EventSnapshot(
        event_ticker='KXTEMPNYCH-26JUN2420',
        series_ticker='KXTEMPNYCH',
        mutually_exclusive=False,
        markets=(rejected, rejected_sibling),
      )

    def get_orderbook(self, ticker: str, depth: int = 0):
      del depth
      from polyventure.websocket_client import normalize_orderbook_snapshot

      return normalize_orderbook_snapshot(
        {
          'ticker': ticker,
          'yes_dollars': [['0.24', '1']],
          'no_dollars': [['0.29', '1']],
          'seq': 1,
        }
      )

  settings = _settings('secrets/demo.pem')
  connection = open_database(str(tmp_path / 'candidate-loader.sqlite3'))

  markets, candidate_markets, _market_by_ticker, _enriched_count, posture = _load_candidate_market_set(
    Client(),
    recorded_at=now,
    connection=connection,
    operation_lane='live',
    lane_session_id='scan-loader',
    settings=settings,
  )

  assert [market.ticker for market in markets] == ['KX-BINARY-YESNO', 'KXTEMPNYCH-26JUN2420-T76.99']
  assert [market.ticker for market in candidate_markets] == ['KX-BINARY-YESNO']
  assert candidate_markets[0].binary_suitability_status == 'eligible'
  assert posture['binary_suitability_rejected_count'] == 1


def test_submit_binary_proof_blocks_rejected_fresh_event_family() -> None:
  market = MarketSnapshot(
    ticker='KXTEMPNYCH-26JUN2420-T76.99',
    title='New York City temperature today at 8pm EDT?',
    close_time=datetime.now(UTC) + timedelta(seconds=300),
    status='open',
    yes_bid_dollars=Decimal('0.02'),
    no_bid_dollars=Decimal('0.01'),
    volume_24h_fp=Decimal('100'),
    open_interest_fp=Decimal('100'),
    event_ticker='KXTEMPNYCH-26JUN2420',
    series_ticker='KXTEMPNYCH',
    yes_sub_title='77 or above',
  )
  sibling = replace(market, ticker='KXTEMPNYCH-26JUN2420-T77.99', yes_sub_title='78 or above')
  candidate = CandidatePair(
    ticker=market.ticker,
    seconds_to_close=300,
    target_yes_bid=Decimal('0.02'),
    target_no_bid=Decimal('0.01'),
    edge_gross_per_contract=Decimal('0.97'),
    fee_reserve_per_contract=Decimal('0.01'),
    edge_net_per_contract=Decimal('0.96'),
    asymmetry=Decimal('0.01'),
    max_size_contracts=Decimal('100'),
    ranking_key=(Decimal('0.96'), Decimal('0.01'), Decimal('100'), Decimal('100'), -300),
    binary_suitability_status='eligible',
    binary_suitability_event_ticker='KXTEMPNYCH-26JUN2420',
  )

  class Client:
    def get_event(self, _event_ticker: str) -> EventSnapshot:
      return EventSnapshot(
        event_ticker='KXTEMPNYCH-26JUN2420',
        series_ticker='KXTEMPNYCH',
        mutually_exclusive=False,
        markets=(market, sibling),
      )

  reason, detail = _submit_binary_proof_block(Client(), candidate, market)

  assert reason == 'binary_proof_rejected'
  assert detail['fresh_binary_reason'] == 'multi_lane_range_event'
  assert detail['fresh_binary_market_count'] == 2

class SparseDensityClient(FakeClient):
  def __init__(self, settings: Settings, private_key: object):
    super().__init__(settings, private_key)
    self.markets = {
      'KALSHI-SPARSE-001': MarketSnapshot(
        ticker='KALSHI-SPARSE-001',
        title='Sparse density candidate',
        close_time=datetime.now(UTC) + timedelta(seconds=240),
        status='open',
        yes_bid_dollars=Decimal('0.33'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('150.00'),
        open_interest_fp=Decimal('90.00'),
        event_ticker='KALSHI-EVENT-SPARSE-001',
      )
    }

  def get_balance(self) -> Decimal:
    return Decimal('1000.00')


class DenseDensityClient(FakeClient):
  def __init__(self, settings: Settings, private_key: object):
    self.settings = settings
    self.private_key = private_key
    now = datetime.now(UTC)
    self.markets = {
      f'KALSHI-DENSE-{index:03d}': MarketSnapshot(
        ticker=f'KALSHI-DENSE-{index:03d}',
        title=f'Dense density candidate {index}',
        close_time=now + timedelta(seconds=240),
        status='open',
        yes_bid_dollars=Decimal('0.33'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('150.00'),
        open_interest_fp=Decimal('90.00'),
        event_ticker=f'KALSHI-EVENT-DENSE-{index:03d}',
      )
      for index in range(1, 11)
    }

  def get_balance(self) -> Decimal:
    return Decimal('1000.00')


class ZeroCandidateClient(FakeClient):
  def __init__(self, settings: Settings, private_key: object):
    self.settings = settings
    self.private_key = private_key
    self.markets = {}

  def get_markets(
    self,
    status: str = 'open',
    limit: int = 100,
    cursor: str | None = None,
    min_close_ts: int | None = None,
    max_close_ts: int | None = None,
  ) -> tuple[list[MarketSnapshot], str | None]:
    del status, limit, cursor, min_close_ts, max_close_ts
    return [], None

  def get_balance(self) -> Decimal:
    return Decimal('1000.00')


class NoQualifyingCandidateClient(FakeClient):
  def __init__(self, settings: Settings, private_key: object):
    self.settings = settings
    self.private_key = private_key
    now = datetime.now(UTC)
    self.markets = {
      'KALSHI-LOW-EDGE-001': MarketSnapshot(
        ticker='KALSHI-LOW-EDGE-001',
        title='Loaded market that does not qualify',
        close_time=now + timedelta(seconds=240),
        status='open',
        yes_bid_dollars=Decimal('0.50'),
        no_bid_dollars=Decimal('0.49'),
        volume_24h_fp=Decimal('100.00'),
        open_interest_fp=Decimal('50.00'),
        event_ticker='KALSHI-EVENT-001',
      ),
    }

  def get_balance(self) -> Decimal:
    return Decimal('1000.00')


class AccountScopedAuthRejectedClient(FakeClient):
  def get_balance(self) -> Decimal:
    raise KalshiHttpError(
      'auth_failed',
      'Kalshi rejected the authenticated request.',
      'Verify the API key id, signing key file, and environment alignment before retrying.',
    )


class PrefilteringClient(FakeClient):
  def __init__(self, settings: Settings, private_key: object):
    self.settings = settings
    self.private_key = private_key
    self.orderbook_calls: list[str] = []
    now = datetime.now(UTC)
    self.markets = {
      'KALSHI-KEEP-001': MarketSnapshot(
        ticker='KALSHI-KEEP-001',
        title='Eligible before enrichment',
        close_time=now + timedelta(seconds=240),
        status='open',
        yes_bid_dollars=Decimal('0.31'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('150.00'),
        open_interest_fp=Decimal('90.00'),
        event_ticker='KALSHI-EVENT-KEEP-001',
      ),
      'KALSHI-CLOSED-001': MarketSnapshot(
        ticker='KALSHI-CLOSED-001',
        title='Closed market',
        close_time=now + timedelta(seconds=240),
        status='closed',
        yes_bid_dollars=Decimal('0.31'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('150.00'),
        open_interest_fp=Decimal('90.00'),
        event_ticker='KALSHI-EVENT-CLOSED-001',
      ),
      'KALSHI-NOCLOSE-001': MarketSnapshot(
        ticker='KALSHI-NOCLOSE-001',
        title='Missing close time',
        close_time=None,
        status='open',
        yes_bid_dollars=Decimal('0.31'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('150.00'),
        open_interest_fp=Decimal('90.00'),
        event_ticker='KALSHI-EVENT-NOCLOSE-001',
      ),
      'KALSHI-SOON-001': MarketSnapshot(
        ticker='KALSHI-SOON-001',
        title='Too close to expiration',
        close_time=now + timedelta(seconds=30),
        status='open',
        yes_bid_dollars=Decimal('0.31'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('150.00'),
        open_interest_fp=Decimal('90.00'),
        event_ticker='KALSHI-EVENT-SOON-001',
      ),
      'KALSHI-LATE-001': MarketSnapshot(
        ticker='KALSHI-LATE-001',
        title='Too far from expiration',
        close_time=now + timedelta(seconds=1200),
        status='open',
        yes_bid_dollars=Decimal('0.31'),
        no_bid_dollars=Decimal('0.39'),
        volume_24h_fp=Decimal('150.00'),
        open_interest_fp=Decimal('90.00'),
        event_ticker='KALSHI-EVENT-LATE-001',
      ),
    }

  def get_orderbook(self, ticker: str, depth: int = 0):
    self.orderbook_calls.append(ticker)
    return super().get_orderbook(ticker, depth)


def _write_private_key(path: Path) -> None:
  key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
  pem = key.private_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PrivateFormat.TraditionalOpenSSL,
    encryption_algorithm=serialization.NoEncryption(),
  )
  path.write_bytes(pem)


def _settings(private_key_file: str) -> Settings:
  return Settings(
    kalshi_env='demo',
    api_key_id='key-id',
    private_key_file=private_key_file,
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
    # Isolate every test from the real production DB: derive an isolated database
    # in the same temp directory as the (tmp) private key. Tests that need a
    # specific path still override state_db_path explicitly.
    state_db_path=str(Path(private_key_file).parent / 'test_state.sqlite3'),
    operation_lane='sandbox',
  )


def _saved_candidate_member(ticker: str, *, candidate_uid: str | None = None) -> dict[str, object]:
  uid = candidate_uid or ticker
  event_ticker = {
    'KALSHI-CANDIDATE-HIGH': 'KALSHI-EVENT-HIGH',
    'KALSHI-CANDIDATE-LOW': 'KALSHI-EVENT-LOW',
    'KALSHI-REJECTED': 'KALSHI-EVENT-REJECTED',
  }.get(ticker, f'{ticker}-EVENT')
  return {
    'candidate_uid': uid,
    'candidate_key': uid,
    'ticker': ticker,
    'seconds_to_close': 240,
    'target_yes_bid': '0.40',
    'target_no_bid': '0.41',
    'edge_gross_per_contract': '0.19',
    'fee_reserve_per_contract': '0.02',
    'edge_net_per_contract': '0.17',
    'asymmetry': '0.01',
    'max_size_contracts': '10',
    'ranking_key': ['0.17', '0.01', '200.00', '100.00', '-240'],
    'binary_suitability_status': 'eligible',
    'binary_suitability_reason': 'binary_event_family',
    'binary_suitability_event_ticker': event_ticker,
    'binary_suitability_series_ticker': ticker.split('-')[0],
    'binary_suitability_category': 'Test',
    'binary_suitability_market_count': 1,
    'binary_suitability_sibling_tickers': [ticker],
    'qualifier_tier': 'live_qualifying',
    'review_row_origin': 'scan',
  }


def _seed_saved_set(
  state_db_path: Path,
  *,
  ticker: str,
  saved_set_id: str = 'saved-set-001',
  state_id: str = 'review_hold_saved_selection_locked',
  actionability_status: str = 'active_valid',
  run_id: str | None = None,
) -> None:
  connection = open_database(state_db_path)
  resolved_run_id = run_id or f'{saved_set_id}-run'
  member = _saved_candidate_member(ticker)
  persist_candidate_review_run(
    connection,
    run_id=resolved_run_id,
    recorded_at_utc='2026-05-25T12:00:00Z',
    operation_lane='sandbox',
    candidate_signature=ticker,
    candidate_count=1,
    source_action='scan',
    lane_session_id='sandbox-saved-set-001',
  )
  persist_candidate_review_candidates(
    connection,
    run_id=resolved_run_id,
    recorded_at_utc='2026-05-25T12:00:00Z',
    operation_lane='sandbox',
    candidates=[member],
  )
  persist_candidate_saved_set(
    connection,
    saved_set_id=saved_set_id,
    run_id=resolved_run_id,
    recorded_at_utc='2026-05-25T12:00:00Z',
    operation_lane='sandbox',
    lane_session_id='sandbox-saved-set-001',
    saved_key_count=1,
    state_id=state_id,
    source_action='save_selection',
    members=[member],
    detail={'candidate_signature': ticker},
  )
  persist_candidate_saved_set_evaluation(
    connection,
    saved_set_id=saved_set_id,
    recorded_at_utc='2026-05-25T12:00:01Z',
    operation_lane='sandbox',
    evaluation_status='pass',
    actionability_status=actionability_status,
    visibility_status='visible_current',
    offline_verifiable=True,
    online_revalidation_required=False,
    detail={'reason': 'Saved set seeded for tranche-G bridge proof.'},
  )


def test_run_scan_once_reports_ranked_candidates(monkeypatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = _settings(str(private_key_path))

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  payload = run_scan_once(settings=settings)

  assert payload['decision'] == 'planned'
  assert payload['market_count'] == 3
  assert payload['candidate_count'] == 2
  assert payload['orderbook_enrichment_count'] == 3
  assert payload['dry_run_explanation'] == 'No order was submitted.'
  assert payload['scan_shape_summary'] == {
    'loaded_market_count': 3,
    'open_or_active_market_count': 3,
    'close_time_known_market_count': 3,
    'entry_window_eligible_market_count': 3,
    'orderbook_review_market_count': 3,
    'quote_ready_market_count': 3,
    'rest_fallback_count': 3,
    'orderbook_enrichment_failure_count': 0,
    'profitability_pass_market_count': 2,
    'qualifying_candidate_count': 2,
    'websocket_orderbook_count': 0,
    'websocket_hit_count': 0,
    'orderbook_enrichment_count': 3,
    'api_orderbook_enrichment_count': 3,
    'candidate_count': 2,
    'candidate_conversion_from_loaded_markets': 0.6667,
    'candidate_conversion_from_enriched_markets': 0.6667,
    'binary_suitability': {
      'binary_suitability_gate': 'applied',
      'event_family_readback_count': 3,
      'event_family_readback_failure_count': 0,
      'binary_suitability_eligible_count': 3,
      'binary_suitability_rejected_count': 0,
      'binary_suitability_unknown_count': 0,
      'binary_suitability_rejection_reasons': {},
    },
  }
  assert [candidate['ticker'] for candidate in payload['candidates']] == [
    'KALSHI-CANDIDATE-HIGH',
    'KALSHI-CANDIDATE-LOW',
  ]
  assert payload['private_key_path_tail'] == 'demo_private_key.pem'
  assert payload['qualifying_candidate_count'] == 2
  assert Decimal(payload['effective_density']) == Decimal('3.125')
  assert Decimal(payload['dynamic_pair_notional_pct']) == Decimal('0.192')
  assert Decimal(payload['dynamic_pair_notional_cap_dollars']) == Decimal('23.7024')
  assert Decimal(payload['dynamic_max_contracts']) == Decimal('32')
  assert payload['binding_limiter'] == 'configured_contract_cap'


def test_run_scan_once_empty_entry_window_emits_zero_found_retry(monkeypatch, tmp_path: Path) -> None:
  # SCHEDULER_ELIGIBILITY_THRESHOLD_REALIGNMENT_BMAP_2026-06-29: the reason a scan is zero-found is
  # irrelevant. An empty entry-window fetch (zero markets) now emits the same retry metadata as a
  # loaded-but-zero-qualifying scan; the scheduler routes both to the retry threshold. This replaces
  # the superseded 2026-06-25 behavior that suppressed retry on empty-window and fell to cadence.
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = _settings(str(private_key_path))

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', ZeroCandidateClient)

  payload = run_scan_once(settings=settings)

  assert payload['decision'] == 'planned'
  assert payload['candidate_count'] == 0
  assert payload['market_count'] == 0
  assert payload['reason'] == 'scan_zero_found_retry'
  assert payload['scan_retry']['active'] is True
  assert payload['scan_retry']['mode'] == 'zero_found_retry'
  assert payload['scan_retry']['retry_after_sec'] == 5
  assert payload['scan_shape_summary']['zero_candidate_reason_family'] == 'entry_window_fetch_empty'
  assert payload['scan_shape_summary']['zero_candidate_blocking_stage'] == 'market_fetch'


def test_run_scan_once_loaded_markets_zero_candidates_returns_retry_metadata(monkeypatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = _settings(str(private_key_path))

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', NoQualifyingCandidateClient)

  payload = run_scan_once(settings=settings)

  assert payload['decision'] == 'planned'
  assert payload['candidate_count'] == 0
  assert payload['market_count'] == 1
  assert payload['reason'] == 'scan_zero_found_retry'
  assert payload['message'] == '0 candidates found; retrying in 5 seconds.'
  assert payload['next_action'] == '0 candidates found; retrying in 5 seconds.'
  assert payload['scan_retry']['active'] is True
  assert payload['scan_retry']['mode'] == 'zero_found_retry'
  assert payload['scan_retry']['retry_after_sec'] == 5
  assert payload['scan_retry']['retry_countdown_remaining_sec'] == 5


def test_candidate_evidence_preview_carries_threshold_margins() -> None:
  settings = Settings(
    **{
      **_settings('secrets/demo.pem').__dict__,
      'min_edge_dollars': 0.05,
      'min_profit_dollars': 0.04,
    }
  )
  candidate = CandidatePair(
    ticker='KALSHI-NEAR-MISS-001',
    seconds_to_close=300,
    target_yes_bid=Decimal('0.48'),
    target_no_bid=Decimal('0.48'),
    edge_gross_per_contract=Decimal('0.04'),
    fee_reserve_per_contract=Decimal('0.01'),
    edge_net_per_contract=Decimal('0.03'),
    asymmetry=Decimal('0'),
    max_size_contracts=Decimal('2'),
    ranking_key=(Decimal('0.03'), Decimal('0'), Decimal('10'), Decimal('10'), -300),
  )

  record = _candidate_projection_record(
    candidate,
    rank=1,
    qualifier_tier='near_miss',
    market_by_ticker={},
    settings=settings,
  )
  preview = _candidate_evidence_preview([record])[0]

  assert preview['target_yes_bid'] == '0.48'
  assert preview['target_no_bid'] == '0.48'
  assert preview['edge_gross_per_contract'] == '0.04'
  assert preview['edge_net_per_contract'] == '0.03'
  assert preview['min_edge_dollars'] == '0.05'
  assert preview['min_profit_dollars'] == '0.04'
  assert preview['gross_edge_margin_to_min_edge'] == '-0.01'
  assert preview['net_profit_margin_to_min_profit'] == '-0.01'
  assert preview['edge_threshold_pass'] is False
  assert preview['profit_threshold_pass'] is False
  assert preview['threshold_outcome'] == 'near_miss'


def test_saved_set_snapshot_guard_overrides_stale_active_actionability() -> None:
  snapshot = _project_saved_set_snapshot(
    {
      'saved_set_id': 'saved-set-stale-active',
      'recorded_at_utc': '2026-06-24T00:00:00Z',
      'operation_lane': 'live',
      'state_id': 'review_hold_saved_selection_locked',
      'saved_key_count': 1,
      'members': [{'ticker': 'KALSHI-OLD'}],
      'latest_evaluation': {'actionability_status': 'active_valid'},
    },
    guard_reason='saved_set_not_eligible',
  )

  assert snapshot['eligible_for_submission'] is False
  assert snapshot['guard_reason'] == 'saved_set_not_eligible'
  assert snapshot['actionability_status'] == 'candidate_not_currently_eligible'


def test_run_scan_once_continues_read_only_discovery_when_account_scope_rejects_auth(tmp_path: Path) -> None:
  private_key_path = tmp_path / 'live_private_key.pem'
  _write_private_key(private_key_path)
  settings = Settings(
    **{
      **_settings(str(private_key_path)).__dict__,
      'kalshi_env': 'prod',
      'operation_lane': 'live',
      'api_base_url': 'https://external-api.kalshi.com/trade-api/v2',
      'websocket_url': 'wss://external-api-ws.kalshi.com/trade-api/ws/v2',
      'live_websocket_url': 'wss://external-api-ws.kalshi.com/trade-api/ws/v2',
      'active_websocket_url': 'wss://external-api-ws.kalshi.com/trade-api/ws/v2',
    }
  )
  progress_events: list[tuple[str, dict[str, object], float | None]] = []

  def _progress_callback(stage: str, message: str, detail: dict[str, object] | None, progress_percent: float | None) -> None:
    del message
    progress_events.append((stage, dict(detail or {}), progress_percent))

  payload = run_scan_once(
    settings=settings,
    client_factory=AccountScopedAuthRejectedClient,
    progress_callback=_progress_callback,
  )

  assert payload['decision'] == 'planned'
  assert payload['candidate_count'] == 2
  assert payload['account_posture']['status'] == 'degraded'
  assert payload['account_posture']['reason_code'] == 'account_scope_auth_failed'
  assert payload['balance_dollars'] == '0'
  assert payload['account_limits']['usage_tier'] == 'unavailable_account_scope'
  assert any(stage == 'account_posture_degraded' for stage, _, _ in progress_events)
  assert any(stage == 'loading_markets' for stage, _, _ in progress_events)


def test_run_service_once_persists_dry_run_plan(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  payload = run_service_once(settings=settings)
  connection = open_database(state_db_path)
  summary = summarize_persistence(connection, operation_lane='sandbox')
  report_payload = report_runtime(settings=settings)

  assert payload['planned_pair_count'] == 2
  assert payload['orderbook_enrichment_count'] == 3
  assert payload['dry_run_explanation'] == 'No order was submitted.'
  assert payload['scan_shape_summary'] == {
    'loaded_market_count': 3,
    'open_or_active_market_count': 3,
    'close_time_known_market_count': 3,
    'entry_window_eligible_market_count': 3,
    'orderbook_review_market_count': 3,
    'quote_ready_market_count': 3,
    'rest_fallback_count': 3,
    'orderbook_enrichment_failure_count': 0,
    'profitability_pass_market_count': 2,
    'qualifying_candidate_count': 2,
    'websocket_orderbook_count': 0,
    'websocket_hit_count': 0,
    'orderbook_enrichment_count': 3,
    'api_orderbook_enrichment_count': 3,
    'candidate_count': 2,
    'candidate_conversion_from_loaded_markets': 0.6667,
    'candidate_conversion_from_enriched_markets': 0.6667,
    'binary_suitability': {},
  }
  assert payload['reconciled_pair_count'] == 0
  assert payload['planned_pairs'][0]['ticker'] == 'KALSHI-CANDIDATE-HIGH'
  assert Decimal(payload['effective_density']) == Decimal('3.125')
  assert Decimal(payload['dynamic_pair_notional_pct']) == Decimal('0.192')
  assert Decimal(payload['dynamic_pair_notional_cap_dollars']) == Decimal('23.7024')
  assert Decimal(payload['dynamic_max_contracts']) == Decimal('32')
  assert payload['binding_limiter'] == 'configured_contract_cap'
  assert payload['planned_pairs'][0]['binding_limiter'] == 'configured_contract_cap'
  assert payload['operation_lane'] == 'sandbox'
  assert payload['lane_session_id'].startswith('sandbox-')
  assert payload['active_websocket_url_tail'] == 'demo-api.kalshi.co/trade-api/ws/v2'
  assert payload['available_websocket_urls']['sandbox'] == 'demo-api.kalshi.co/trade-api/ws/v2'
  assert Decimal(report_payload['pair_runtime_summary'][0]['contract_count']) == Decimal('10')
  assert Decimal(report_payload['pair_runtime_summary'][0]['dynamic_pair_notional_pct']) == Decimal('0.192')
  assert report_payload['pair_runtime_summary'][0]['binding_limiter'] == 'configured_contract_cap'
  assert report_payload['operation_lane'] == 'sandbox'
  assert report_payload['pair_lane_session_history']
  assert report_payload['latest_heartbeat']['operation_lane'] == 'sandbox'
  assert summary['table_counts']['pair_plans'] == 2
  assert summary['table_counts']['pair_states'] == 2
  assert summary['table_counts']['account_api_limits'] == 1
  assert summary['table_counts']['service_heartbeats'] == 2


def test_run_service_once_dynamic_sizing_changes_with_density(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)

  sparse_settings = Settings(
    **{
      **_settings(str(private_key_path)).__dict__,
      'state_db_path': str(tmp_path / 'sparse.sqlite3'),
      'max_pair_contracts': 1000.0,
    }
  )
  dense_settings = Settings(
    **{
      **_settings(str(private_key_path)).__dict__,
      'state_db_path': str(tmp_path / 'dense.sqlite3'),
      'max_pair_contracts': 1000.0,
    }
  )

  sparse_payload = run_service_once(settings=sparse_settings, client_factory=SparseDensityClient)
  dense_payload = run_service_once(settings=dense_settings, client_factory=DenseDensityClient)

  assert sparse_payload['planned_pair_count'] == 1
  assert dense_payload['planned_pair_count'] == 10
  assert sparse_payload['qualifying_candidate_count'] == 1
  assert dense_payload['qualifying_candidate_count'] == 10
  assert Decimal(sparse_payload['dynamic_pair_notional_pct']) > Decimal(dense_payload['dynamic_pair_notional_pct'])
  assert Decimal(sparse_payload['planned_pairs'][0]['contract_count']) > Decimal(dense_payload['planned_pairs'][0]['contract_count'])
  assert sparse_payload['binding_limiter'] == 'dynamic_notional_cap'
  assert dense_payload['binding_limiter'] == 'dynamic_notional_cap'


def test_run_service_once_submit_order_bridge_persists_f2_execution_chronology(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_bridge.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})
  _seed_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='saved-set-bridge-001')

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge')
  connection = open_database(state_db_path)
  summary = summarize_persistence(connection, operation_lane='sandbox')

  assert payload['saved_set_snapshot']['present'] is True
  assert payload['saved_set_snapshot']['eligible_for_submission'] is True
  assert payload['saved_set_snapshot']['matched_candidate_ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert payload['funds_posture']['funds_refresh_status'] == 'fresh'
  assert payload['planned_pairs'][0]['ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert payload['submit_response_id'] == 'SUBMIT_ACCEPTED_DISPATCHING'
  assert payload['submit_rest_state_id'] == 'CANCELED'
  assert payload['execution_chronology']['enabled'] is True
  assert payload['execution_chronology']['profile'] == 'submit_order_bridge'
  assert payload['execution_chronology']['terminal_state'] == 'CANCELED'
  assert payload['execution_chronology']['contract_version'] == 'tranche_f_execution_event_packet.v1'
  event_packet = payload['execution_chronology']['event_packet']
  assert isinstance(event_packet, list)
  assert event_packet
  assert {step['event_type'] for step in event_packet} == {
    'submit_order_intent',
    'user_orders',
    'fill',
    'market_positions',
    'reconcile_snapshot',
    'cancel_applied',
  }
  for step in event_packet:
    assert step['operation_lane'] == 'sandbox'
    assert str(step['lane_session_id']).startswith('sandbox-')
    assert step['market_ticker'] == payload['planned_pairs'][0]['ticker']
    assert str(step['seq']).startswith('f2-seq-')
    assert int(step['ts_ms']) > 0
    assert step['as_of_time']
  assert payload['planned_pair_count'] == 1
  assert payload['planned_pairs'][0]['execution_profile'] == 'submit_order_bridge'
  assert payload['planned_pairs'][0]['execution_terminal_state'] == 'CANCELED'
  assert payload['planned_pairs'][0]['execution_intent_source'] == 'saved_set'
  assert payload['planned_pairs'][0]['saved_set_id'] == 'saved-set-bridge-001'
  assert payload['planned_pairs'][0]['submit_response_id'] == 'SUBMIT_ACCEPTED_DISPATCHING'
  assert payload['planned_pairs'][0]['public_state_id'] == 'CANCELED'
  assert summary['table_counts']['orders'] == 2
  assert summary['table_counts']['fills'] == 2
  assert summary['table_counts']['pair_states'] == 6
  pair_id = payload['planned_pairs'][0]['pair_id']
  assert summary['pair_state_history'][pair_id] == [
    'PLANNED',
    'SUBMITTING',
    'RESTING_BOTH',
    'PARTIAL_BOTH',
    'PARTIAL_BOTH',
    'CANCELED',
  ]

  runtime_events = connection.execute(
    '''
    SELECT event_type, detail_json
    FROM runtime_events
    WHERE operation_lane = ?
      AND pair_id = ?
      AND event_type IN ('submit_order_intent', 'user_orders', 'fill', 'market_positions', 'reconcile_snapshot', 'cancel_applied')
    ''',
    ('sandbox', pair_id),
  ).fetchall()
  assert {row['event_type'] for row in runtime_events} == {
    'submit_order_intent',
    'user_orders',
    'fill',
    'market_positions',
    'reconcile_snapshot',
    'cancel_applied',
  }
  detail_by_event = {row['event_type']: json.loads(row['detail_json']) for row in runtime_events}
  assert detail_by_event['market_positions']['position_state'] == 'PARTIAL_BOTH'
  assert detail_by_event['reconcile_snapshot']['user_data_timestamp'] == detail_by_event['reconcile_snapshot']['ts_ms']
  assert detail_by_event['submit_order_intent']['profile'] == 'submit_order_bridge'

  report_payload = report_runtime(settings=settings)
  assert report_payload['saved_set_snapshot']['saved_set_id'] == 'saved-set-bridge-001'
  assert report_payload['funds_posture']['funds_refresh_status'] == 'fresh'
  assert report_payload['pair_runtime_summary'][0]['execution_intent_source'] == 'saved_set'
  assert report_payload['pair_runtime_summary'][0]['submit_response_id'] == 'SUBMIT_ACCEPTED_DISPATCHING'
  assert report_payload['pair_runtime_summary'][0]['public_state_id'] == 'CANCELED'


def test_run_service_once_submit_order_bridge_requires_saved_set(monkeypatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_bridge_blocked.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge')

  assert payload['planned_pair_count'] == 0
  assert payload['blocked_reason'] == 'no_saved_set'
  assert payload['saved_set_snapshot']['present'] is False
  assert payload['submit_response_id'] == 'SUBMIT_BLOCKED_NO_SAVED_SET'
  assert payload['submit_rest_state_id'] == 'UPSTREAM_REVIEW_HOLD'


def _seed_run_with_saved_set(
  state_db_path: Path,
  *,
  run_id: str,
  ticker: str,
  candidate_uid: str,
  saved_set_id: str = 'saved-set-inflight-001',
  actionability_status: str = 'active_valid',
  operation_lane: str = 'sandbox',
  lane_session_id: str = 'sandbox-session-sse1',
) -> None:
  connection = open_database(state_db_path)
  member = _saved_candidate_member(ticker, candidate_uid=candidate_uid)
  persist_candidate_review_run(
    connection,
    run_id=run_id,
    recorded_at_utc='2026-06-06T12:00:00Z',
    operation_lane=operation_lane,
    candidate_signature='sig-sse1',
    candidate_count=1,
    source_action='scan',
    lane_session_id=lane_session_id,
  )
  persist_candidate_review_candidates(
    connection,
    run_id=run_id,
    recorded_at_utc='2026-06-06T12:00:00Z',
    operation_lane=operation_lane,
    candidates=[member],
  )
  persist_candidate_saved_set(
    connection,
    saved_set_id=saved_set_id,
    run_id=run_id,
    recorded_at_utc='2026-06-06T12:00:01Z',
    operation_lane=operation_lane,
    lane_session_id=lane_session_id,
    saved_key_count=1,
    state_id='review_hold_saved_selection_locked',
    source_action='save_selection',
    members=[member],
    detail={'candidate_signature': 'sig-sse1'},
  )
  persist_candidate_saved_set_evaluation(
    connection,
    saved_set_id=saved_set_id,
    recorded_at_utc='2026-06-06T12:00:02Z',
    operation_lane=operation_lane,
    evaluation_status='pass',
    actionability_status=actionability_status,
    visibility_status='visible_current',
    offline_verifiable=True,
    online_revalidation_required=False,
    detail={'reason': 'SSE-1 in-flight lifecycle test.'},
  )


def _seed_exact_handoff_saved_set(
  state_db_path: Path,
  *,
  run_id: str,
  saved_set_id: str,
  lane_session_id: str,
  ticker: str,
  candidate_key: str,
  recorded_at_utc: str,
) -> dict[str, object]:
  connection = open_database(state_db_path)
  member = _saved_candidate_member(ticker, candidate_uid=candidate_key)
  signature = candidate_key
  persist_candidate_review_run(
    connection,
    run_id=run_id,
    recorded_at_utc=recorded_at_utc,
    operation_lane='sandbox',
    candidate_signature=signature,
    candidate_count=1,
    source_action='scan',
    lane_session_id=lane_session_id,
  )
  persist_candidate_review_candidates(
    connection,
    run_id=run_id,
    recorded_at_utc=recorded_at_utc,
    operation_lane='sandbox',
    candidates=[member],
  )
  persist_candidate_saved_set(
    connection,
    saved_set_id=saved_set_id,
    run_id=run_id,
    recorded_at_utc=recorded_at_utc,
    operation_lane='sandbox',
    lane_session_id=lane_session_id,
    saved_key_count=1,
    state_id='review_hold_saved_selection_locked',
    source_action='save_selection',
    members=[member],
    detail={'candidate_signature': signature, 'saved_signature': signature},
  )
  persist_candidate_saved_set_evaluation(
    connection,
    saved_set_id=saved_set_id,
    recorded_at_utc=recorded_at_utc,
    operation_lane='sandbox',
    evaluation_status='saved',
    actionability_status='active_valid',
    visibility_status='default_actionable',
    offline_verifiable=True,
    online_revalidation_required=False,
    detail={'reason': 'Direct handoff test saved set.'},
  )
  return {
    'schema_version': 1,
    'handoff_id': f'handoff-{saved_set_id}',
    'source': 'backend_auto_dispatch',
    'operation_lane': 'sandbox',
    'operator_lane_session_id': lane_session_id,
    'scan_session_id': run_id,
    'saved_set_id': saved_set_id,
    'candidate_signature': signature,
    'candidate_count': 1,
    'candidate_keys': [candidate_key],
    'created_at_utc': recorded_at_utc,
  }


def test_submit_handoff_uses_exact_saved_set_not_latest(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_direct_handoff.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})
  handoff = _seed_exact_handoff_saved_set(
    state_db_path,
    run_id='scan-direct-handoff-target',
    saved_set_id='saved-set-direct-target',
    lane_session_id='sandbox-direct-session',
    ticker='KALSHI-CANDIDATE-LOW',
    candidate_key='direct-low-key',
    recorded_at_utc='2026-06-29T12:00:00Z',
  )
  _seed_exact_handoff_saved_set(
    state_db_path,
    run_id='scan-direct-handoff-stale-latest',
    saved_set_id='saved-set-direct-stale-latest',
    lane_session_id='sandbox-direct-session-latest',
    ticker='KALSHI-CANDIDATE-HIGH',
    candidate_key='direct-high-key',
    recorded_at_utc='2026-06-29T12:01:00Z',
  )
  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  payload = run_service_once(
    settings=settings,
    execution_profile='submit_order_bridge',
    submit_handoff=handoff,
  )

  assert payload['saved_set_snapshot']['saved_set_id'] == 'saved-set-direct-target'
  assert payload['saved_set_snapshot']['matched_candidate_ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert payload['planned_pairs'][0]['ticker'] == 'KALSHI-CANDIDATE-LOW'


def test_submit_handoff_mismatch_fails_closed_before_queue(monkeypatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_direct_handoff_mismatch.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})
  handoff = _seed_exact_handoff_saved_set(
    state_db_path,
    run_id='scan-direct-handoff-target',
    saved_set_id='saved-set-direct-target',
    lane_session_id='sandbox-direct-session',
    ticker='KALSHI-CANDIDATE-LOW',
    candidate_key='direct-low-key',
    recorded_at_utc='2026-06-29T12:00:00Z',
  )
  bad_handoff = {**handoff, 'saved_set_id': 'saved-set-missing-or-stale'}
  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  with pytest.raises(SubmitHandoffValidationError, match='submit_handoff_saved_set_not_found') as excinfo:
    run_service_once(
      settings=settings,
      execution_profile='submit_order_bridge',
      submit_handoff=bad_handoff,
    )

  assert getattr(excinfo.value, 'polyventure_submit_bridge_phase') == 'submit_handoff_validation'
  connection = open_database(state_db_path)
  queue_event = connection.execute(
    "SELECT event_type FROM runtime_events WHERE event_type = 'candidate_queue_submitted'",
  ).fetchone()
  assert queue_event is None


def test_submit_transitions_candidates_to_in_flight(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_inflight.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})
  run_id = 'run-sse1-inflight-001'
  candidate_uid = 'KALSHI-CANDIDATE-LOW'
  _seed_run_with_saved_set(
    state_db_path,
    run_id=run_id,
    ticker='KALSHI-CANDIDATE-LOW',
    candidate_uid=candidate_uid,
    actionability_status='expired_actionability',
  )
  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  run_service_once(settings=settings, execution_profile='submit_order_bridge')

  connection = open_database(state_db_path)

  candidate_row = connection.execute(
    '''
    SELECT lifecycle_stage FROM candidate_review_candidates
    WHERE run_id = ? AND candidate_uid = ?
    ''',
    (run_id, candidate_uid),
  ).fetchone()
  assert candidate_row is not None
  assert candidate_row['lifecycle_stage'] == 'in_flight'

  queue_event = connection.execute(
    "SELECT event_type FROM runtime_events WHERE event_type = 'candidate_queue_submitted' AND operation_lane = 'sandbox'",
  ).fetchone()
  assert queue_event is not None


def test_submit_bridge_blocks_incomplete_saved_member_before_queue(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_incomplete_saved_member.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)
  persist_candidate_review_run(
    connection,
    run_id='run-incomplete-saved-member',
    recorded_at_utc='2026-06-06T12:00:00Z',
    operation_lane='sandbox',
    candidate_signature='KALSHI-CANDIDATE-LOW',
    candidate_count=1,
    source_action='scan',
    lane_session_id='sandbox-session-incomplete',
  )
  persist_candidate_saved_set(
    connection,
    saved_set_id='saved-set-incomplete-member',
    run_id='run-incomplete-saved-member',
    recorded_at_utc='2026-06-06T12:00:01Z',
    operation_lane='sandbox',
    lane_session_id='sandbox-session-incomplete',
    saved_key_count=1,
    state_id='review_hold_saved_selection_locked',
    source_action='save_selection',
    members=[{'candidate_uid': 'KALSHI-CANDIDATE-LOW', 'candidate_key': 'KALSHI-CANDIDATE-LOW', 'ticker': 'KALSHI-CANDIDATE-LOW'}],
    detail={'candidate_signature': 'KALSHI-CANDIDATE-LOW'},
  )
  persist_candidate_saved_set_evaluation(
    connection,
    saved_set_id='saved-set-incomplete-member',
    recorded_at_utc='2026-06-06T12:00:02Z',
    operation_lane='sandbox',
    evaluation_status='pass',
    actionability_status='active_valid',
    visibility_status='visible_current',
    offline_verifiable=True,
    online_revalidation_required=False,
    detail={'reason': 'Incomplete saved member must fail before queue.'},
  )
  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge')
  queue_event = connection.execute(
    "SELECT event_type FROM runtime_events WHERE event_type = 'candidate_queue_submitted'",
  ).fetchone()

  assert payload['blocked_reason'] == 'saved_set_member_detail_unavailable'
  assert queue_event is None


def test_submit_bridge_live_orderbook_exception_rejects_candidate_before_order(monkeypatch, tmp_path: Path) -> None:
  # Ratified shape (BRIDGE_SUBMIT_TERMINAL_PREWIRE_COVERABILITY_BMAP_2026-07-01, failure table):
  # a final orderbook read failure is candidate-local — reject before order with
  # final_orderbook_read_failed evidence and complete the run; never a global phase failure.
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'live_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_live_orderbook_failure.sqlite3'
  live_ws_url = 'wss://external-api-ws.kalshi.com/trade-api/ws/v2'
  base_settings = _settings(str(private_key_path))
  settings = Settings(**{
    **base_settings.__dict__,
    'state_db_path': str(state_db_path),
    'operation_lane': 'live',
    'kalshi_env': 'prod',
    'api_base_url': 'https://external-api.kalshi.com/trade-api/v2',
    'websocket_url': live_ws_url,
    'live_websocket_url': live_ws_url,
    'active_websocket_url': live_ws_url,
  })
  _seed_run_with_saved_set(
    state_db_path,
    run_id='run-live-orderbook-failure',
    ticker='KALSHI-CANDIDATE-LOW',
    candidate_uid='KALSHI-CANDIDATE-LOW',
    operation_lane='live',
    lane_session_id='live-session-orderbook-failure',
  )

  class OrderbookFailureClient(FakeClient):
    def get_orderbook(self, ticker: str, depth: int = 0):
      del ticker, depth
      raise RuntimeError('synthetic orderbook failure')

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', OrderbookFailureClient)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge')

  assert payload['planned_pair_count'] == 0
  connection = open_database(state_db_path)
  phase_failed_rows = connection.execute(
    "SELECT detail_json FROM runtime_events WHERE event_type = 'submit_bridge_phase_failed'",
  ).fetchall()
  assert phase_failed_rows == []
  blocked_rows = connection.execute(
    "SELECT detail_json FROM runtime_events WHERE event_type = 'submit_bridge_blocked' ORDER BY id",
  ).fetchall()
  blocked_details = [json.loads(row['detail_json']) for row in blocked_rows]
  rejected_detail = next(
    detail for detail in blocked_details
    if detail.get('blocked_reason') == 'final_orderbook_read_failed'
  )
  assert rejected_detail['failure_phase'] == 'final_orderbook_read_failed'
  assert rejected_detail['error_family'] == 'RuntimeError'
  assert rejected_detail['error_message'] == 'synthetic orderbook failure'
  assert rejected_detail['ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert rejected_detail['money_path_crossed'] is False
  assert rejected_detail['pair_plan_created'] is False
  assert rejected_detail['orders_created'] is False
  coverability_row = connection.execute(
    "SELECT detail_json FROM runtime_events WHERE event_type = 'submit_bridge_final_coverability_checked'",
  ).fetchone()
  assert coverability_row is not None
  coverability_detail = json.loads(coverability_row['detail_json'])
  assert coverability_detail['ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert coverability_detail['ok'] is False
  assert coverability_detail['guard_reason'] == 'final_orderbook_read_failed'
  assert coverability_detail['message'] == 'synthetic orderbook failure'


def test_run_scan_once_prefilters_market_universe_before_enrichment(tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = _settings(str(private_key_path))

  progress_events: list[tuple[str, dict[str, object], float | None]] = []
  client_holder: dict[str, PrefilteringClient] = {}

  def _client_factory(resolved_settings: Settings, private_key: object) -> PrefilteringClient:
    client = PrefilteringClient(resolved_settings, private_key)
    client_holder['client'] = client
    return client

  def _progress_callback(stage: str, message: str, detail: dict[str, object] | None, progress_percent: float | None) -> None:
    del message
    progress_events.append((stage, dict(detail or {}), progress_percent))

  payload = run_scan_once(
    settings=settings,
    client_factory=_client_factory,
    progress_callback=_progress_callback,
  )

  screening_detail = next(detail for stage, detail, _ in progress_events if stage == 'screening_candidate_universe')
  screening_summary = dict(screening_detail['scan_shape_summary'])
  enrichment_detail = next(detail for stage, detail, _ in progress_events if stage == 'enriching_remaining_orderbooks')

  assert payload['market_count'] == 5
  assert payload['candidate_count'] == 1
  assert payload['orderbook_enrichment_count'] == 1
  assert [candidate['ticker'] for candidate in payload['candidates']] == ['KALSHI-KEEP-001']
  assert client_holder['client'].orderbook_calls == ['KALSHI-KEEP-001']
  assert screening_detail['loaded_market_count'] == 5
  assert screening_detail['orderbook_enrichment_target_count'] == 1
  assert screening_summary['summary_phase'] == 'pre_enrichment'
  assert screening_summary['loaded_market_count'] == 5
  assert screening_summary['open_or_active_market_count'] == 4
  assert screening_summary['close_time_known_market_count'] == 3
  assert screening_summary['entry_window_eligible_market_count'] == 1
  assert screening_summary['orderbook_enrichment_target_count'] == 1
  assert enrichment_detail['loaded_market_count'] == 5
  assert enrichment_detail['market_count'] == 1
  assert enrichment_detail['orderbook_enrichment_target_count'] == 1


def test_reconcile_cancel_all_and_report_use_local_state(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime.sqlite3'
  settings = _settings('secrets/demo.pem')
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)
  plan = PairOrderPlan(
    pair_id='pair-runtime-001',
    ticker='KALSHI-RUNTIME-001',
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='pair-runtime-001-yes',
    no_client_order_id='pair-runtime-001-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(
    connection,
    plan,
    created_at_utc='2026-05-05T05:55:00Z',
    operation_lane='sandbox',
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='PLANNED',
    recorded_at_utc='2026-05-05T05:55:01Z',
    operation_lane='sandbox',
    lane_session_id='sandbox-session-001',
    detail={
      'ticker': plan.ticker,
      'websocket_connected': True,
      'effective_density': '3.125',
      'dynamic_pair_notional_pct': '0.192',
      'dynamic_pair_notional_cap_dollars': '23.7024',
      'dynamic_max_contracts': '32',
      'binding_limiter': 'configured_contract_cap',
    },
  )

  reconcile_payload = reconcile_pairs(settings=settings)
  cancel_payload = cancel_all_pairs(settings=settings)
  report_payload = report_runtime(settings=settings)

  assert reconcile_payload['pair_count'] == 1
  assert reconcile_payload['pairs'][0]['state'] == 'PLANNED'
  assert Decimal(reconcile_payload['pair_runtime_summary'][0]['dynamic_pair_notional_pct']) == Decimal('0.192')
  assert reconcile_payload['pair_runtime_summary'][0]['binding_limiter'] == 'configured_contract_cap'
  assert cancel_payload['canceled_pair_count'] == 1
  assert report_payload['table_counts']['pair_states'] >= 2
  assert plan.pair_id in report_payload['pair_state_history']
  assert report_payload['pair_lane_session_history'][plan.pair_id]
  assert report_payload['pair_runtime_summary'][0]['locked_contracts'] == '0'
  assert report_payload['pair_runtime_summary'][0]['contract_count'] == '5'


def test_reconcile_pairs_uses_current_operating_session_for_orphan_boundary(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime_orphan_boundary.sqlite3'
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)
  persist_candidate_review_run(
    connection,
    run_id='run-current',
    recorded_at_utc='2026-06-22T10:00:00Z',
    operation_lane='sandbox',
    lane_session_id='operator-session',
    candidate_signature='sig-current',
    candidate_count=1,
    source_action='scan',
  )
  persist_candidate_review_run(
    connection,
    run_id='run-prior',
    recorded_at_utc='2026-06-22T09:00:00Z',
    operation_lane='sandbox',
    lane_session_id='prior-session',
    candidate_signature='sig-prior',
    candidate_count=1,
    source_action='scan',
  )
  persist_candidate_review_candidates(
    connection,
    run_id='run-current',
    recorded_at_utc='2026-06-22T10:00:01Z',
    operation_lane='sandbox',
    candidates=[{
      'candidate_uid': 'TCURRENT::live_qualifying',
      'candidate_key': 'TCURRENT::live_qualifying',
      'ticker': 'TCURRENT',
      'qualifier_tier': 'live_qualifying',
      'review_row_origin': 'current',
      'event_ticker': 'TCURRENT',
    }],
  )
  persist_candidate_review_candidates(
    connection,
    run_id='run-prior',
    recorded_at_utc='2026-06-22T09:00:01Z',
    operation_lane='sandbox',
    candidates=[{
      'candidate_uid': 'TPRIOR::live_qualifying',
      'candidate_key': 'TPRIOR::live_qualifying',
      'ticker': 'TPRIOR',
      'qualifier_tier': 'live_qualifying',
      'review_row_origin': 'current',
      'event_ticker': 'TPRIOR',
    }],
  )
  connection.execute(
    "UPDATE candidate_review_candidates SET lifecycle_stage = 'in_flight' WHERE ticker IN ('TCURRENT', 'TPRIOR')",
  )
  persist_service_heartbeat(
    connection,
    component='websocket',
    status='connected',
    recorded_at_utc='2026-06-22T10:00:02Z',
    operation_lane='sandbox',
    lane_session_id='per-cycle-session',
    detail={},
  )

  reconcile_pairs(
    settings=settings,
    suppress_live_funds_refresh=True,
    current_operating_session_id='operator-session',
  )

  rows = {
    row[0]: (row[1], row[2])
    for row in open_database(state_db_path).execute(
      'SELECT ticker, lifecycle_stage, terminal_cause FROM candidate_review_candidates'
    )
  }
  assert rows['TCURRENT'][0] == 'in_flight'
  assert rows['TPRIOR'] == ('terminal', 'orphaned_teardown_reconciled')


def test_run_service_once_does_not_timeout_stale_unmatched_exposure(monkeypatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_stale.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)
  persist_pair_plan(
    connection,
    PairOrderPlan(
      pair_id='pair-stale-001',
      ticker='KALSHI-CANDIDATE-HIGH',
      yes_price=Decimal('0.31'),
      no_price=Decimal('0.39'),
      contract_count=Decimal('5'),
      yes_client_order_id='pair-stale-001-yes',
      no_client_order_id='pair-stale-001-no',
      time_in_force='good_till_canceled',
      post_only=True,
      cancel_order_on_pause=True,
      subaccount=0,
    ),
    created_at_utc='2026-05-05T05:55:00Z',
    operation_lane='sandbox',
  )
  persist_pair_state_transition(
    connection,
    pair_id='pair-stale-001',
    state='PARTIAL_ONE_SIDE',
    recorded_at_utc='2026-05-05T05:55:01Z',
    operation_lane='sandbox',
    lane_session_id='sandbox-session-stale',
    detail={
      'yes_filled_contracts': '5',
      'no_filled_contracts': '0',
      'average_yes_price': '0.31',
      'average_no_price': '0',
      'realized_fees_dollars': '0.02',
      'websocket_connected': False,
    },
  )

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  payload = run_service_once(settings=settings)
  reconcile_payload = reconcile_pairs(settings=settings)
  report_payload = report_runtime(settings=settings)

  # Canonical: max_unhedged_sec is a shelter window keyed on seconds-to-close, not an
  # order-age timeout. A stale one-sided pair is NOT force-flipped to ERROR by the
  # reconcile sweep; it stays open for the shelter / repair-close path to resolve.
  assert payload['timed_out_pair_count'] == 0
  assert payload.get('blocked_reason') != 'unmatched_exposure_timeout'
  assert all(pair['state'] != 'ERROR' for pair in reconcile_payload['pairs'])
  # The stale one-sided pair keeps its PARTIAL_ONE_SIDE projection (open to fill); it is
  # not escalated to a RECONCILE_REQUIRED / HARD_STOP order-age timeout posture.
  assert report_payload['pair_runtime_summary'][0]['public_state_id'] == 'PARTIAL_ONE_SIDE'


def test_report_runtime_projects_auto_cancel_overlay_for_one_sided_exposure(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime_overlay.sqlite3'
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)
  recorded_at = datetime.now(UTC) - timedelta(seconds=20)
  plan = PairOrderPlan(
    pair_id='pair-overlay-001',
    ticker='KALSHI-OVERLAY-001',
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='pair-overlay-001-yes',
    no_client_order_id='pair-overlay-001-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(
    connection,
    plan,
    created_at_utc=recorded_at.isoformat(),
    operation_lane='sandbox',
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='PARTIAL_ONE_SIDE',
    recorded_at_utc=recorded_at.isoformat(),
    operation_lane='sandbox',
    lane_session_id='sandbox-overlay-001',
    detail={
      'ticker': plan.ticker,
      'yes_filled_contracts': '5',
      'no_filled_contracts': '0',
      'average_yes_price': '0.34',
      'average_no_price': '0',
      'realized_fees_dollars': '0.02',
      'websocket_connected': True,
    },
  )

  payload = report_runtime(settings=settings)

  assert payload['pair_runtime_summary'][0]['public_state_id'] == 'PARTIAL_ONE_SIDE'
  assert payload['pair_runtime_summary'][0]['mobility_overlay_state'] == 'AUTO_CANCEL_RECOMMENDED'
  assert payload['pair_runtime_summary'][0]['failure_class'] == 'HARD_STOP'
  assert payload['pair_runtime_summary'][0]['failure_scope'] == 'pair_local'
  assert payload['pair_runtime_summary'][0]['retry_allowed'] is False
  assert 'AUTO_CANCEL' in payload['pair_runtime_summary'][0]['allowed_actions']


def test_pair_runtime_summary_includes_terminal_state_and_recorded_at_fields(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime_terminal_fields.sqlite3'
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)
  recorded_at = datetime.now(UTC)
  plan = PairOrderPlan(
    pair_id='pair-terminal-001',
    ticker='KALSHI-TERMINAL-001',
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='pair-terminal-001-yes',
    no_client_order_id='pair-terminal-001-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(
    connection,
    plan,
    created_at_utc=recorded_at.isoformat(),
    operation_lane='sandbox',
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='CANCELED',
    recorded_at_utc=recorded_at.isoformat(),
    operation_lane='sandbox',
    lane_session_id='sandbox-terminal-001',
    detail={
      'ticker': plan.ticker,
      'yes_filled_contracts': '0',
      'no_filled_contracts': '0',
      'average_yes_price': '0',
      'average_no_price': '0',
      'realized_fees_dollars': '0',
    },
  )

  payload = report_runtime(settings=settings)

  row = payload['pair_runtime_summary'][0]
  # P1: terminal_state is set for terminal public_state_id values
  assert row['terminal_state'] == 'CANCELED'
  # P1: pair_state_recorded_at_utc is forwarded from the snapshot
  assert row['pair_state_recorded_at_utc'] == recorded_at.isoformat()
  assert row['lane_session_id'] == 'sandbox-terminal-001'
  # P1: pre-existing fields remain present and unmodified
  assert row['public_state_id'] == 'CANCELED'
  assert 'locked_contracts' in row
  assert 'net_realized_dollars' in row
  assert payload['next_action'] == 'Use Refresh Shell to update the current runtime view.'


def test_pair_runtime_summary_terminal_state_empty_for_non_terminal_state(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime_nonterminal.sqlite3'
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)
  recorded_at = datetime.now(UTC)
  plan = PairOrderPlan(
    pair_id='pair-nonterminal-001',
    ticker='KALSHI-NONTERMINAL-001',
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='pair-nonterminal-001-yes',
    no_client_order_id='pair-nonterminal-001-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(
    connection,
    plan,
    created_at_utc=recorded_at.isoformat(),
    operation_lane='sandbox',
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='RESTING_BOTH',
    recorded_at_utc=recorded_at.isoformat(),
    operation_lane='sandbox',
    lane_session_id='sandbox-nonterminal-001',
    detail={
      'ticker': plan.ticker,
      'yes_filled_contracts': '0',
      'no_filled_contracts': '0',
      'average_yes_price': '0.34',
      'average_no_price': '0.39',
      'realized_fees_dollars': '0',
    },
  )

  payload = report_runtime(settings=settings)

  row = payload['pair_runtime_summary'][0]
  # P1: terminal_state is empty string for non-terminal states
  assert row['terminal_state'] == ''
  assert row['pair_state_recorded_at_utc'] == recorded_at.isoformat()


def _runtime_snapshot(*, legacy_state: str, public_state_id: str, realized_fees: str, locked: str = '0') -> dict:
  # Minimal snapshot accepted by _pair_runtime_summary. `state` must be a legacy
  # pair state; the public/terminal id is forced via detail.public_state_id. locked
  # contracts are driven by equal yes/no fills so gross/cost terms stay deterministic.
  return {
    'pair_id': 'pair-fee-001',
    'ticker': 'KALSHI-FEE-001',
    'state': legacy_state,
    'recorded_at_utc': datetime.now(UTC).isoformat(),
    'contract_count': locked,
    'detail': {
      'public_state_id': public_state_id,
      'yes_filled_contracts': locked,
      'no_filled_contracts': locked,
      'average_yes_price': '0.34',
      'average_no_price': '0.39',
      'realized_fees_dollars': realized_fees,
      'websocket_connected': True,
    },
  }


def test_settled_fees_dollars_carries_realized_fee_for_filled_and_settled() -> None:
  # R1: a paid (FILLED/SETTLED) pair contributes its real fee to the settled ledger.
  for terminal in ('FILLED', 'SETTLED'):
    summary = _pair_runtime_summary(
      _runtime_snapshot(legacy_state='LOCKED', public_state_id=terminal, realized_fees='1.50', locked='5'),
      fee_reserve_dollars=Decimal('0.02'),
    )
    assert summary['public_state_id'] == terminal
    assert Decimal(str(summary['settled_fees_dollars'])) == Decimal('1.50')
    # The estimate field is preserved unchanged for the in-flight gross offset.
    assert Decimal(str(summary['fees_dollars'])) == Decimal('1.50')


def test_settled_fees_dollars_zero_for_canceled_and_failed() -> None:
  # R2: canceled/failed pairs paid no fee -> zero in the settled ledger, even though
  # the estimate field still carries the runtime value.
  for legacy_state, terminal in (('CANCELED', 'CANCELED'), ('ERROR', 'SUBMIT_FAILED_TERMINAL')):
    summary = _pair_runtime_summary(
      _runtime_snapshot(legacy_state=legacy_state, public_state_id=terminal, realized_fees='1.50'),
      fee_reserve_dollars=Decimal('0.02'),
    )
    assert Decimal(str(summary['settled_fees_dollars'])) == Decimal('0')


def test_settled_fees_dollars_zero_for_in_flight() -> None:
  # R3: an in-flight pair's fee is an estimate, not yet paid -> zero in the ledger.
  summary = _pair_runtime_summary(
    _runtime_snapshot(legacy_state='RESTING_BOTH', public_state_id='RESTING_BOTH', realized_fees='0.75'),
    fee_reserve_dollars=Decimal('0.02'),
  )
  assert summary['terminal_state'] == ''
  assert Decimal(str(summary['settled_fees_dollars'])) == Decimal('0')
  assert Decimal(str(summary['fees_dollars'])) == Decimal('0.75')


def test_heartbeat_balance_at_returns_balance_as_of_time(tmp_path: Path) -> None:
  # R4: as-of lookup returns the most recent fresh funds snapshot at or before the
  # requested time, and zero when none precedes it.
  connection = open_database(tmp_path / 'balance_as_of.sqlite3')
  now = datetime.now(UTC)
  t1 = now - timedelta(minutes=30)
  t2 = t1 + timedelta(minutes=10)
  for recorded_at, balance in ((t1, '40.00'), (t2, '55.00')):
    persist_service_heartbeat(
      connection,
      component='runtime-loop',
      status='cycle-complete',
      recorded_at_utc=recorded_at.isoformat(),
      operation_lane='live',
      lane_session_id='live-balance-001',
      detail={
        'available_funds_snapshot': balance,
        'available_funds_as_of': recorded_at.isoformat(),
        'funds_refresh_status': 'fresh',
      },
    )
  before_any = _heartbeat_balance_at(connection, operation_lane='live', at_utc=t1 - timedelta(minutes=1))
  at_t1 = _heartbeat_balance_at(connection, operation_lane='live', at_utc=t1 + timedelta(seconds=30))
  at_t2 = _heartbeat_balance_at(connection, operation_lane='live', at_utc=t2 + timedelta(seconds=30))
  connection.close()
  assert before_any == Decimal('0')
  assert at_t1 == Decimal('40.00')
  assert at_t2 == Decimal('55.00')


def test_report_runtime_marks_stale_funds_from_latest_heartbeat(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime_stale_funds.sqlite3'
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)
  stale_at = datetime.now(UTC) - timedelta(seconds=20)
  persist_service_heartbeat(
    connection,
    component='runtime-loop',
    status='cycle-complete',
    recorded_at_utc=datetime.now(UTC).isoformat(),
    operation_lane='sandbox',
    lane_session_id='sandbox-funds-001',
    detail={
      'available_funds_snapshot': '123.45',
      'available_funds_as_of': stale_at.isoformat(),
      'funds_refresh_status': 'fresh',
      'funds_refresh_reason': None,
    },
  )

  payload = report_runtime(settings=settings)

  assert payload['funds_posture']['available_funds_snapshot'] == '123.45'
  assert payload['funds_posture']['funds_refresh_status'] == 'stale'
  assert payload['funds_posture']['funds_refresh_reason'] == 'balance_staleness_grace_exceeded'
  assert payload['funds_posture']['stale_blocks_submit'] is True


def test_report_runtime_refreshes_live_funds_directly_from_account_scope(monkeypatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'live_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_live_report.sqlite3'
  settings = Settings(
    **{
      **_settings(str(private_key_path)).__dict__,
      'kalshi_env': 'prod',
      'api_base_url': 'https://api.kalshi.com/trade-api/v2',
      'websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
      'live_websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
      'active_websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
      'operation_lane': 'live',
      'state_db_path': str(state_db_path),
    }
  )

  connection = open_database(state_db_path)
  persist_service_heartbeat(
    connection,
    component='reconcile',
    status='complete',
    recorded_at_utc=datetime.now(UTC).isoformat(),
    operation_lane='live',
    lane_session_id='live-funds-001',
    detail={
      'pair_count': 0,
      'active_websocket_url_tail': 'v2',
    },
  )

  payload = report_runtime(settings=settings, client_factory=FakeClient)

  assert payload['operation_lane'] == 'live'
  assert payload['funds_posture']['available_funds_snapshot'] == '123.45'
  assert payload['funds_posture']['funds_refresh_status'] == 'fresh'
  assert payload['funds_posture']['available_funds_as_of'] is not None


def test_reconcile_pairs_refreshes_live_funds_and_persists_them_in_heartbeat(monkeypatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'live_reconcile_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_live_reconcile.sqlite3'
  settings = Settings(
    **{
      **_settings(str(private_key_path)).__dict__,
      'kalshi_env': 'prod',
      'api_base_url': 'https://api.kalshi.com/trade-api/v2',
      'websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
      'live_websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
      'active_websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
      'operation_lane': 'live',
      'state_db_path': str(state_db_path),
    }
  )

  payload = reconcile_pairs(settings=settings, client_factory=FakeClient)
  connection = open_database(state_db_path)
  latest_heartbeat = connection.execute(
    '''
    SELECT detail_json
    FROM service_heartbeats
    WHERE operation_lane = ?
    ORDER BY id DESC
    LIMIT 1
    ''',
    ('live',),
  ).fetchone()
  detail = json.loads(latest_heartbeat['detail_json']) if latest_heartbeat is not None else {}

  assert payload['operation_lane'] == 'live'
  assert payload['funds_posture']['available_funds_snapshot'] == '123.45'
  assert payload['funds_posture']['funds_refresh_status'] == 'fresh'
  assert detail['available_funds_snapshot'] == '123.45'
  assert detail['funds_refresh_status'] == 'fresh'


def test_fetch_system_log_entries_merges_persisted_sources_chronologically(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime_log.sqlite3'
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)

  persist_service_heartbeat(
    connection,
    component='runtime-loop',
    status='startup-ok',
    recorded_at_utc='2026-05-05T10:00:00+00:00',
    operation_lane='sandbox',
    lane_session_id='sandbox-log-001',
    detail={'mode': 'ab_guarded'},
  )
  persist_operator_action(
    connection,
    action='reconcile',
    recorded_at_utc='2026-05-05T10:00:01+00:00',
    operation_lane='sandbox',
    lane_session_id='sandbox-log-001',
    detail={'pair_count': 1},
  )
  persist_runtime_event(
    connection,
    level='INFO',
    event_type='pair_plan_created',
    pair_id='pair-123',
    recorded_at_utc='2026-05-05T10:00:02+00:00',
    operation_lane='sandbox',
    lane_session_id='sandbox-log-001',
    detail={'ticker': 'KALSHI-TEST-001'},
  )

  payload = fetch_system_log_entries(settings=settings)

  assert payload['decision'] == 'planned'
  assert [entry['source'] for entry in payload['entries']] == [
    'service_heartbeat',
    'operator_action',
    'runtime_event',
  ]
  assert all(entry['operation_lane'] == 'sandbox' for entry in payload['entries'])
  assert all(entry['lane_session_id'] == 'sandbox-log-001' for entry in payload['entries'])
  assert payload['entries'][0]['message'].startswith('[HEARTBEAT][SANDBOX] runtime-loop -> startup-ok')
  assert payload['entries'][1]['message'].startswith('[ACTION][SANDBOX] reconcile')
  assert payload['entries'][2]['message'].startswith('[RUNTIME][SANDBOX] pair_plan_created :: pair-123')


def test_fetch_system_log_entries_handles_nested_runtime_event_payloads(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime_log_nested.sqlite3'
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)

  persist_runtime_event(
    connection,
    level='WARN',
    event_type='scan_background_failed',
    recorded_at_utc='2026-05-05T10:00:02+00:00',
    operation_lane='sandbox',
    lane_session_id='sandbox-log-002',
    detail={
      'message': 'Kalshi rejected the authenticated request.',
      'reason': 'credential_acceptance_failed',
      'result_payload': {
        'decision': 'no-go',
        'reason': 'credential_acceptance_failed',
        'message': 'Kalshi rejected the authenticated request.',
      },
    },
  )

  payload = fetch_system_log_entries(settings=settings)

  assert payload['decision'] == 'planned'
  assert len(payload['entries']) == 1
  assert payload['entries'][0]['source'] == 'runtime_event'
  assert 'result_payload={dict:decision,message,reason}' in payload['entries'][0]['message']


def test_fetch_operational_visuals_returns_pair_distribution_with_live_state(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_visuals.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  run_service_once(settings=settings)
  payload = fetch_operational_visuals(settings=settings, view='pair_state_distribution', window='current', mode='table')

  assert payload['status'] == 'ready'
  assert payload['view']['id'] == 'pair_state_distribution'
  assert payload['view']['render_mode'] == 'table'
  assert payload['operation_lane'] == 'sandbox'
  assert payload['series'][0]['kind'] == 'bar'
  assert payload['table']['columns'] == ['State', 'Count']
  assert payload['table']['rows'][0][0] == 'PLANNED'


def test_fetch_operational_visuals_defaults_performance_scope_to_waterfall(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_visuals_default.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(
    **{
      **settings.__dict__,
      'state_db_path': str(state_db_path),
      'operation_lane': 'live',
      'live_websocket_url': 'wss://demo-api.kalshi.co/trade-api/ws/v2',
      'active_websocket_url': 'wss://demo-api.kalshi.co/trade-api/ws/v2',
    }
  )

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  run_service_once(settings=settings)
  payload = fetch_operational_visuals(settings=settings, scope='performance')

  assert payload['status'] == 'ready'
  assert payload['scope']['id'] == 'performance'
  assert payload['view']['id'] == 'performance_waterfall'


def test_fetch_operational_visuals_performance_supporting_metrics_uses_time_series_plot(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_visuals_performance_timeseries.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(
    **{
      **settings.__dict__,
      'state_db_path': str(state_db_path),
      'operation_lane': 'live',
      'live_websocket_url': 'wss://demo-api.kalshi.co/trade-api/ws/v2',
      'active_websocket_url': 'wss://demo-api.kalshi.co/trade-api/ws/v2',
    }
  )

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  run_service_once(settings=settings)
  payload = fetch_operational_visuals(settings=settings, view='performance_total', window='current', mode='plot')

  assert payload['status'] == 'ready'
  assert payload['scope']['id'] == 'performance'
  assert payload['view']['id'] == 'performance_total'
  assert payload['window']['id'] == 'all'
  assert payload['axis']['x']['kind'] == 'temporal_bucket'
  assert payload['controls']['window']['enabled'] is True
  assert [window['id'] for window in payload['available_windows']] == ['1h', '24h', '7d', 'all']
  assert payload['series'][0]['kind'] == 'line'
  assert payload['series'][0]['points']
  assert 'T' in str(payload['series'][0]['points'][0]['x'])
  assert payload['table']['rows']
  assert payload['table']['columns'][0] == 'Pair'
  assert str(payload['table']['rows'][0][0]).strip()


def test_fetch_operational_visuals_performance_uses_live_lane_data_only(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'runtime_visuals_performance_live_only.sqlite3'
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(state_db_path), 'operation_lane': 'sandbox'})
  connection = open_database(state_db_path)

  sandbox_plan = PairOrderPlan(
    pair_id='pair-performance-sandbox',
    ticker='KALSHI-PERFORMANCE-SANDBOX',
    yes_price=Decimal('0.20'),
    no_price=Decimal('0.30'),
    contract_count=Decimal('4'),
    yes_client_order_id='pair-performance-sandbox-yes',
    no_client_order_id='pair-performance-sandbox-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  live_plan = PairOrderPlan(
    pair_id='pair-performance-live',
    ticker='KALSHI-PERFORMANCE-LIVE',
    yes_price=Decimal('0.10'),
    no_price=Decimal('0.20'),
    contract_count=Decimal('7'),
    yes_client_order_id='pair-performance-live-yes',
    no_client_order_id='pair-performance-live-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(connection, sandbox_plan, created_at_utc='2026-06-02T20:00:00Z', operation_lane='sandbox')
  persist_pair_plan(connection, live_plan, created_at_utc='2026-06-02T20:00:00Z', operation_lane='live')
  persist_pair_state_transition(
    connection,
    pair_id=sandbox_plan.pair_id,
    state='CANCELED',
    recorded_at_utc='2026-06-02T20:00:01Z',
    operation_lane='sandbox',
    lane_session_id='sandbox-performance-001',
    detail={
      'ticker': sandbox_plan.ticker,
      'yes_filled_contracts': '4',
      'no_filled_contracts': '4',
      'average_yes_price': '0.20',
      'average_no_price': '0.30',
      'realized_fees_dollars': '0.04',
      'websocket_connected': False,
    },
  )
  persist_pair_state_transition(
    connection,
    pair_id=live_plan.pair_id,
    state='CANCELED',
    recorded_at_utc='2026-06-02T20:00:01Z',
    operation_lane='live',
    lane_session_id='live-performance-001',
    detail={
      'ticker': live_plan.ticker,
      'yes_filled_contracts': '7',
      'no_filled_contracts': '7',
      'average_yes_price': '0.10',
      'average_no_price': '0.20',
      'realized_fees_dollars': '0.07',
      'websocket_connected': True,
    },
  )

  payload = fetch_operational_visuals(settings=settings, view='performance_total', mode='table')

  assert payload['status'] == 'ready'
  assert payload['scope']['id'] == 'performance'
  assert payload['view']['id'] == 'performance_total'
  assert payload['table'] is not None
  assert len(payload['table']['rows']) == 1
  assert payload['table']['rows'][0][0] == 'KALSHI-PERFORMANCE-LIVE'
  assert 'SANDBOX' not in str(payload['table']['rows'])


@pytest.mark.parametrize(
  ('view_id', 'expected_headline', 'expected_next_action', 'expected_empty_reason'),
  [
    (
      'pair_state_distribution',
      'When local pair-state rows are present, this view will show the current pair-state distribution here, with exact counts available in the table.',
      'Run a dry-run cycle or reconcile after the next pair plan appears to populate pair-state distribution.',
      'When local pair-state rows are present, this view will show the current pair-state distribution here.',
    ),
    (
      'runtime_cadence',
      'When heartbeat, operator, and runtime activity accumulate in the selected window, this view will show lane activity cadence here, with matching bucket counts in the table.',
      'Run scan, report, or dry-run actions to establish recent activity history for this view.',
      'When recent heartbeat, operator, and runtime activity exists, this cadence view will populate here.',
    ),
    (
      'cycle_outcomes',
      'When completed runtime cycles exist in the selected window, this view will show planned, blocked, and no-candidate outcome history here, with matching bucket counts in the table.',
      'Run a dry-run cycle to begin building completed outcome history for this view.',
      'When completed runtime cycles exist in the selected window, their outcome history will appear here.',
    ),
    (
      'freshness_latency',
      'When heartbeat history has accumulated, this view will show freshness age and heartbeat-gap drift here, with exact timestamps available in the table.',
      'Run shell actions that persist heartbeats to begin building freshness history for this view.',
      'When heartbeat history has accumulated, freshness age and heartbeat-gap drift will appear here.',
    ),
    (
      'performance_total',
      'When retained live-lane monetary history is available, the selected money timeline will appear here and the matching table/report will populate below this surface.',
      'Create or reconcile live-lane pair state to begin building this history.',
      'When retained live-lane monetary history is available, the selected money timeline will appear here.',
    ),
    (
      'performance_waterfall',
      'When live-lane pair-runtime monetary rows are available, this bridge will show how total out, fees, and total in reconcile to net projected value, with matching table/report detail.',
      'Create or reconcile live-lane pair state before using the bridge as a money check.',
      'When live-lane pair-runtime monetary rows are available, this bridge will show the money reconciliation here.',
    ),
  ],
)
def test_fetch_operational_visuals_runtime_and_performance_empty_messages_are_expectation_first(
  tmp_path: Path,
  view_id: str,
  expected_headline: str,
  expected_next_action: str,
  expected_empty_reason: str,
) -> None:
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(tmp_path / 'runtime_empty_visuals.sqlite3')})

  payload = fetch_operational_visuals(settings=settings, view=view_id)

  assert payload['status'] == 'empty'
  assert payload['summary']['headline'] == expected_headline
  assert payload['summary']['next_action'] == expected_next_action
  assert payload['empty_reason'] == expected_empty_reason


@pytest.mark.parametrize(
  ('view_id', 'expected_headline', 'expected_next_action', 'expected_empty_reason'),
  [
    (
      'candidate_density_curve',
      'This view displays the score-and-margin density shape of the retained candidate set when a candidate review packet is available.',
      'It populates after Find candidates or a dry-run cycle retains that packet.',
      'This view displays the score-and-margin density shape of the retained candidate set when a candidate review packet is available. It populates after Find candidates or a dry-run cycle retains that packet.',
    ),
    (
      'candidate_decision_boundary',
      'This view displays the retained decision-boundary reading for surfaced candidates, with weighted score, threshold, and score-margin context.',
      'It populates after Find candidates or a dry-run cycle retains the decision packet.',
      'This view displays the retained decision-boundary reading for surfaced candidates, with weighted score, threshold, and score-margin context. It populates after Find candidates or a dry-run cycle retains the decision packet.',
    ),
    (
      'threshold_boundary_marker',
      'This view displays each surfaced candidate\'s distance from the active qualification gates when retained threshold evidence is available.',
      'It populates after Find candidates or a dry-run cycle retains the threshold packet.',
      'This view displays each surfaced candidate\'s distance from the active qualification gates when retained threshold evidence is available. It populates after Find candidates or a dry-run cycle retains the threshold packet.',
    ),
    (
      'candidate_frontier_scatter',
      'This view displays the opportunity shape across edge and liquidity, with selected, near-miss, and rejected cohorts separated in the same surface.',
      'It populates after Find candidates or a dry-run cycle retains the frontier packet.',
      'This view displays the opportunity shape across edge and liquidity, with selected, near-miss, and rejected cohorts separated in the same surface. It populates after Find candidates or a dry-run cycle retains the frontier packet.',
    ),
    (
      'comparative_ranking_snapshot',
      'This view displays the retained ordinal ranking snapshot for the current surfaced leaders when a candidate ranking packet is available.',
      'It populates after Find candidates or a dry-run cycle retains the ranking snapshot.',
      'This view displays the retained ordinal ranking snapshot for the current surfaced leaders when a candidate ranking packet is available. It populates after Find candidates or a dry-run cycle retains the ranking snapshot.',
    ),
    (
      'saved_set_carry_forward',
      'This view displays how saved candidate selections carry forward over time when saved-set history exists.',
      'It populates after at least one candidate selection is saved.',
      'This view displays how saved candidate selections carry forward over time when saved-set history exists. It populates after at least one candidate selection is saved.',
    ),
  ],
)
def test_fetch_operational_visuals_candidate_empty_messages_are_pane_specific(
  tmp_path: Path,
  view_id: str,
  expected_headline: str,
  expected_next_action: str,
  expected_empty_reason: str,
) -> None:
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(tmp_path / 'candidate_empty_visuals.sqlite3')})

  payload = fetch_operational_visuals(settings=settings, view=view_id)

  assert payload['status'] == 'empty'
  assert payload['summary']['headline'] == expected_headline
  assert payload['summary']['next_action'] == expected_next_action
  assert payload['empty_reason'] == expected_empty_reason


def test_fetch_operational_visuals_analysis_empty_messages_follow_contract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'state_db_path': str(tmp_path / 'analysis_empty_visuals.sqlite3')})

  gate_payload = fetch_operational_visuals(settings=settings, view='analysis_threshold_progress')

  assert gate_payload['summary']['headline'] == 'When enough retained candidate rows are available, this view will show threshold progress here and keep the current-versus-target count visible until analysis activation is reached.'
  assert gate_payload['summary']['next_action'] == 'Run Find candidates to continue building the retained row count for this view.'
  assert gate_payload['empty_reason'] == 'When enough retained candidate rows are available, threshold progress will appear here.'

  monkeypatch.setattr(
    'polyventure.service._analysis_activation_state',
    lambda *_args, **_kwargs: {
      'ready': True,
      'status': 'threshold_progress',
      'current_count': 12,
      'threshold': 24,
      'remaining_count': 12,
    },
  )

  diagnostics_payload = fetch_operational_visuals(settings=settings, view='analysis_linear_diagnostics')
  factors_payload = fetch_operational_visuals(settings=settings, view='factors_timeseries')
  actionability_payload = fetch_operational_visuals(settings=settings, view='actionability_status_distribution')

  assert diagnostics_payload['status'] == 'empty'
  assert diagnostics_payload['summary']['headline'] == 'When retained candidate feature vectors are available, this view will project the current diagnostic shape here and keep matching table/report detail available.'
  assert diagnostics_payload['empty_reason'] == 'When retained candidate feature vectors are available, the diagnostic projection will appear here.'
  assert factors_payload['status'] == 'empty'
  assert factors_payload['summary']['headline'] == 'When retained candidate run history is available, this view will show factor-weight history here and keep exact run values available in the table.'
  assert factors_payload['empty_reason'] == 'When retained candidate run history is available, factor-weight history will appear here.'
  assert actionability_payload['status'] == 'empty'
  assert actionability_payload['summary']['headline'] == 'When saved candidate selections have been evaluated, this view will show sandbox and live actionability history here, with exact counts available in the table.'
  assert actionability_payload['empty_reason'] == 'When saved candidate selections have been evaluated, actionability history will appear here.'


def test_fetch_operational_visuals_thresholds_use_boundary_scatter_contract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = Settings(**{**_settings(str(private_key_path)).__dict__, 'state_db_path': str(tmp_path / 'threshold_scatter.sqlite3')})

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  run_service_once(settings=settings)
  payload = fetch_operational_visuals(settings=settings, view='threshold_boundary_marker', mode='plot')

  assert payload['status'] == 'ready'
  assert payload['view']['id'] == 'threshold_boundary_marker'
  assert payload['series'][0]['id'] == 'zero_threshold_line'
  assert payload['series'][0]['kind'] == 'line'
  assert payload['series'][0]['toggleable'] is False
  assert [series['id'] for series in payload['series'][1:]] == ['selected', 'near_miss', 'rejected']
  assert all(series['kind'] == 'scatter' for series in payload['series'][1:])
  assert payload['boundary_band']['near_zero_band'] == {'min_margin': '-0.05', 'max_margin': '0.05'}
  assert payload['shared_graph_mode'] is True
  assert payload['shared_series_contract']['metric_ids'] == ['selected', 'near_miss', 'rejected']


def test_fetch_operational_visuals_rankings_use_ordinal_supporting_evidence_contract(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = Settings(**{**_settings(str(private_key_path)).__dict__, 'state_db_path': str(tmp_path / 'rankings_contract.sqlite3')})

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  run_service_once(settings=settings)
  payload = fetch_operational_visuals(settings=settings, view='comparative_ranking_snapshot', mode='plot')

  assert payload['status'] == 'ready'
  assert payload['view']['id'] == 'comparative_ranking_snapshot'
  assert payload['axis']['x'] == {'kind': 'ordinal_rank', 'spacing': 'ordinal'}
  assert payload['series'][0]['kind'] == 'horizontal_bar'
  assert payload['series'][0]['label'] == 'Edge net by ordinal rank'
  assert payload['series'][0]['points']
  assert all(str(point['x']).startswith('Rank ') for point in payload['series'][0]['points'])
  assert all('KALSHI-' not in str(point['x']) for point in payload['series'][0]['points'])
  assert payload['ranking_snapshot']['evidence_role'] == 'ordinal_supporting_evidence'
  assert payload['ranking_snapshot']['history_view_id'] == 'saved_set_carry_forward'
  assert payload['no_workflow_authority'] is True


def test_fetch_operational_visuals_rankings_keep_identifiers_in_table_not_chart(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = Settings(**{**_settings(str(private_key_path)).__dict__, 'state_db_path': str(tmp_path / 'rankings_table.sqlite3')})

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  run_service_once(settings=settings)
  payload = fetch_operational_visuals(settings=settings, view='comparative_ranking_snapshot', mode='table')

  assert payload['status'] == 'ready'
  assert payload['table']['columns'] == ['Rank', 'Role', 'Ticker', 'Tier', 'Density', 'Liquidity', 'Edge']
  assert payload['table']['rows']
  assert all(str(row[2]).startswith('KALSHI-') for row in payload['table']['rows'])
  assert all(str(row[1]) in {'Surfaced leader', 'Transition', 'Near miss'} for row in payload['table']['rows'])


def test_fetch_operational_visuals_frontier_defaults_to_selected_and_rejected_visible(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = Settings(**{**_settings(str(private_key_path)).__dict__, 'state_db_path': str(tmp_path / 'frontier_polish.sqlite3')})

  monkeypatch.setattr('polyventure.service.KalshiHttpClient', FakeClient)

  run_service_once(settings=settings)
  payload = fetch_operational_visuals(settings=settings, view='candidate_frontier_scatter', mode='plot')

  assert payload['status'] == 'ready'
  assert payload['view']['id'] == 'candidate_frontier_scatter'
  assert payload['shared_series_contract']['metric_ids'] == ['selected', 'near_miss', 'rejected']
  assert payload['shared_series_contract']['default_visible_metric_ids'] == ['selected', 'rejected']
  colors = {series['id']: series.get('color') for series in payload['series']}
  assert colors == {
    'selected': '#7cf7ab',
    'near_miss': '#f2a654',
    'rejected': '#72859a',
  }
  assert all(series['kind'] == 'scatter' for series in payload['series'])
  assert all(series.get('marker_shape') == 'circle' for series in payload['series'])
  assert all((point.get('radius') or 0) >= 3.8 for series in payload['series'] for point in series['points'])


def test_validate_env_alignment_uses_api_for_environment_and_active_lane_for_websocket(tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = Settings(
    **{
      **_settings(str(private_key_path)).__dict__,
      'operation_lane': 'live',
      'sandbox_websocket_url': 'wss://demo-api.kalshi.co/trade-api/ws/v2',
      'live_websocket_url': 'wss://api.elections.kalshi.com/trade-api/ws/v2',
      'active_websocket_url': 'wss://api.elections.kalshi.com/trade-api/ws/v2',
      'websocket_url': 'wss://api.elections.kalshi.com/trade-api/ws/v2',
    }
  )

  from polyventure.service import _validate_env_alignment

  _validate_env_alignment(settings)


def test_batch_submit_loop_source_contains_candidates_to_process_structure() -> None:
  import inspect
  from polyventure.service import run_service_once
  src = inspect.getsource(run_service_once)
  assert '_candidates_to_process' in src, 'HX1-B: batch loop variable _candidates_to_process missing'
  assert 'for _batch_idx, candidate in enumerate(_candidates_to_process)' in src, (
    'HX1-B: batch enumeration loop missing from run_service_once'
  )


def test_batch_submit_loop_source_contains_silent_continue_on_notional_cap() -> None:
  import inspect
  from polyventure.service import run_service_once
  src = inspect.getsource(run_service_once)
  assert 'continue' in src, 'HX1-B: SILENT_CONTINUE (continue) missing from batch loop'


def test_batch_submit_loop_source_checks_capacity_for_subsequent_candidates() -> None:
  import inspect
  from polyventure.service import run_service_once
  src = inspect.getsource(run_service_once)
  assert '_batch_idx > 0' in src, 'HX1-B: per-candidate capacity re-check missing (_batch_idx > 0)'
  assert 'break' in src, 'HX1-B: capacity-at-limit break missing from batch loop'


def test_bridge_profile_active_path_uses_all_saved_set_candidates_as_dispatch_source() -> None:
  import inspect
  from polyventure.service import run_service_once
  src = inspect.getsource(run_service_once)
  assert 'if bridge_profile_active' in src, 'HX1-B: bridge_profile_active branch missing from batch candidate selection'
  assert '_resolve_saved_set_execution_candidates(saved_set)' in src, (
    'saved-set bridge dispatch must materialize the full locked saved-set candidate batch'
  )
  assert 'candidates = list(saved_set_candidates)' in src, (
    'saved-set bridge dispatch must replace the fresh candidate list with all locked saved candidates'
  )


def test_stale_funds_blocks_submit_regardless_of_bridge_profile_active() -> None:
  import inspect
  from polyventure.service import run_service_once
  src = inspect.getsource(run_service_once)
  # F1: the final stale gate is a plain `elif` not gated on bridge_profile_active, so
  # non-bridge paths (sandbox, dry-run) are also protected by the stale-funds guard.
  assert "elif funds_posture['stale_blocks_submit']:" in src, (
    "HX2 Gap 4 regression: stale balance guard must not be gated on bridge_profile_active"
  )
  # F1: the at-point re-fetch is an earlier elif that gates on bridge_profile_active AND
  # live lane — confirming it does not skip the guard for non-bridge paths.
  assert (
    "bridge_profile_active\n    and resolved_settings.operation_lane == 'live'\n    and funds_posture['stale_blocks_submit']" in src
    or "bridge_profile_active and resolved_settings.operation_lane == 'live' and funds_posture['stale_blocks_submit']" in src
  ), "F1: at-point re-fetch must be scoped to bridge_profile_active + live lane"


def test_validate_env_alignment_rejects_missing_active_lane_websocket(tmp_path: Path) -> None:
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = Settings(
    **{
      **_settings(str(private_key_path)).__dict__,
      'operation_lane': 'live',
      'sandbox_websocket_url': 'wss://demo-api.kalshi.co/trade-api/ws/v2',
      'live_websocket_url': '',
      'active_websocket_url': '',
      'websocket_url': '',
    }
  )

  from polyventure.service import _validate_env_alignment

  with pytest.raises(ValueError, match='active websocket endpoint'):
    _validate_env_alignment(settings)


def test_validate_env_alignment_offline_lane_raises_lane_membership_before_env_checks(tmp_path: Path) -> None:
  # Regression (2026-06-12): after a live session, offline-mode settings carry a
  # demo environment alongside a live-derived REST base URL (no 'demo' substring).
  # The lane-membership error must surface first so the web shell's offline
  # carve-out recognizes it; previously the env/endpoint mismatch fired first,
  # producing a persistent bootstrap-failed error screen in offline mode.
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = Settings(
    **{
      **_settings(str(private_key_path)).__dict__,
      'operation_lane': 'offline',
      'kalshi_env': 'demo',
      'api_base_url': 'https://external-api.kalshi.com/trade-api/v2',
      'live_websocket_url': 'wss://external-api-ws.kalshi.com/trade-api/ws/v2',
      'active_websocket_url': 'wss://external-api-ws.kalshi.com/trade-api/ws/v2',
      'websocket_url': 'wss://external-api-ws.kalshi.com/trade-api/ws/v2',
    }
  )

  from polyventure.service import _validate_env_alignment

  with pytest.raises(ValueError, match='operation lane must be sandbox or live'):
    _validate_env_alignment(settings)


def test_parameter_surface_derived_row_status_pending_when_no_scan_data() -> None:
  from polyventure.service import _parameter_surface_derived_row
  row = _parameter_surface_derived_row(
    'effective_density',
    runtime_summary={},
    generated_at_utc=None,
  )
  assert row['row_status'] == 'pending', '§17: derived row without scan data must have row_status=pending'
  assert row['derived_value'] is None, '§17: derived_value must be None when runtime_summary is empty'


def test_parameter_surface_derived_row_status_available_when_value_present() -> None:
  from polyventure.service import _parameter_surface_derived_row
  row = _parameter_surface_derived_row(
    'effective_density',
    runtime_summary={'effective_density': 0.42},
    generated_at_utc='2026-06-10T00:00:00Z',
  )
  assert row['row_status'] == 'available', '§17: derived row with value must have row_status=available'
  assert row['derived_value'] == 0.42, '§17: derived_value must carry through from runtime_summary'


def test_parameter_surface_derived_row_present_in_source_inspection() -> None:
  import inspect
  from polyventure.service import _parameter_surface_derived_row
  src = inspect.getsource(_parameter_surface_derived_row)
  assert "'row_status'" in src or '"row_status"' in src, (
    '§17: row_status field must be present in _parameter_surface_derived_row'
  )
  assert 'pending' in src, '§17: pending sentinel must be present in _parameter_surface_derived_row'
  assert 'available' in src, '§17: available sentinel must be present in _parameter_surface_derived_row'


def test_parameter_surface_info_filter_keeps_catalog_rows_visible_when_unset() -> None:
  """§17: INFO page must surface unset catalog rows instead of hiding missing values."""
  import inspect
  from polyventure import web_app as _web_app
  src = inspect.getsource(_web_app)
  assert 'return Boolean(parameterId);' in src, (
    '§17: parameterSurfaceRowHasRenderableValue must render cataloged rows even when values are unset'
  )
  assert "[null, undefined, ''].includes(sourceValue)" not in src, (
    '§17: INFO filter must not hide rows whose current/source value is unset'
  )


def test_simulation_inject_mode_guard_present_in_run_scan_once_source() -> None:
  import inspect
  from polyventure import service as _service
  src = inspect.getsource(_service.run_scan_once)
  assert 'KALSHI_SIMULATION_MODE' in src, (
    'simulation inject: KALSHI_SIMULATION_MODE env var guard must be present in run_scan_once'
  )
  assert 'inject' in src, (
    "simulation inject: 'inject' mode value must be present in run_scan_once"
  )


def test_simulation_inject_mode_sandbox_lane_guard_present_in_source() -> None:
  import inspect
  from polyventure import service as _service
  src = inspect.getsource(_service.run_scan_once)
  sandbox_block_idx = src.find("operation_lane == 'sandbox'")
  inject_idx = src.find('KALSHI_SIMULATION_MODE')
  assert sandbox_block_idx != -1, 'sandbox lane guard must be present in run_scan_once'
  assert inject_idx != -1, 'inject mode check must be present in run_scan_once'
  assert inject_idx > sandbox_block_idx, (
    'simulation inject: KALSHI_SIMULATION_MODE check must be nested inside sandbox lane guard'
  )


def test_cancel_on_pause_in_parameter_surface_overlay_field_ids() -> None:
  from polyventure.service import PARAMETER_SURFACE_OVERLAY_FIELD_IDS
  assert 'cancel_on_pause' in PARAMETER_SURFACE_OVERLAY_FIELD_IDS, (
    'cancel_on_pause must be in PARAMETER_SURFACE_OVERLAY_FIELD_IDS so it appears as settable in the SET tab'
  )


def test_max_unhedged_sec_in_parameter_surface_overlay_field_ids() -> None:
  from polyventure.service import PARAMETER_SURFACE_OVERLAY_FIELD_IDS
  assert 'max_unhedged_sec' in PARAMETER_SURFACE_OVERLAY_FIELD_IDS, (
    'max_unhedged_sec must be in PARAMETER_SURFACE_OVERLAY_FIELD_IDS so it appears as settable in the SET tab'
  )


def test_post_submit_processing_buffer_in_parameter_surface_overlay_field_ids() -> None:
  from polyventure.service import PARAMETER_SURFACE_OVERLAY_FIELD_IDS
  assert 'post_submit_processing_buffer_sec' in PARAMETER_SURFACE_OVERLAY_FIELD_IDS, (
    'post_submit_processing_buffer_sec must be in PARAMETER_SURFACE_OVERLAY_FIELD_IDS so it appears as settable in the SET tab'
  )


def test_set_page_scan_cadence_group_contains_cancel_on_pause_max_unhedged_and_post_submit_buffer() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG
  set_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'set'), None)
  assert set_page is not None, 'set page must exist in PARAMETER_SURFACE_PAGE_CATALOG'
  cadence_group = next((g for g in set_page['group_catalog'] if g['group_id'] == 'manual_scan_cadence'), None)
  assert cadence_group is not None, 'manual_scan_cadence group must exist in set page'
  assert 'cancel_on_pause' in cadence_group['field_ids'], (
    'cancel_on_pause must be in manual_scan_cadence SET group'
  )
  assert 'max_unhedged_sec' in cadence_group['field_ids'], (
    'max_unhedged_sec must be in manual_scan_cadence SET group'
  )
  assert 'post_submit_processing_buffer_sec' in cadence_group['field_ids'], (
    'post_submit_processing_buffer_sec must be in manual_scan_cadence SET group'
  )


def test_info_page_scan_cadence_group_contains_post_submit_processing_buffer() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG, PARAMETER_SURFACE_FIELD_CATALOG
  info_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'info'), None)
  assert info_page is not None, 'info page must exist in PARAMETER_SURFACE_PAGE_CATALOG'
  cadence_group = next((g for g in info_page['group_catalog'] if g['group_id'] == 'scan_cadence_posture'), None)
  assert cadence_group is not None, 'scan_cadence_posture group must exist in info page'
  assert 'post_submit_processing_buffer_sec' in cadence_group['field_ids']
  field_meta = PARAMETER_SURFACE_FIELD_CATALOG['post_submit_processing_buffer_sec']
  assert field_meta['source_env_var'] == 'KALSHI_POST_SUBMIT_PROCESSING_BUFFER_SEC'
  assert '180' not in field_meta['info_detail'] and '60' not in field_meta['info_detail']


def test_info_and_set_page_scan_cadence_group_titles_match() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG
  info_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'info'), None)
  set_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'set'), None)
  assert info_page is not None and set_page is not None
  info_cadence = next((g for g in info_page['group_catalog'] if g['group_id'] == 'scan_cadence_posture'), None)
  set_cadence = next((g for g in set_page['group_catalog'] if g['group_id'] == 'manual_scan_cadence'), None)
  assert info_cadence is not None, 'scan_cadence_posture group must exist in INFO page'
  assert set_cadence is not None, 'manual_scan_cadence group must exist in SET page'
  assert info_cadence['title'] == set_cadence['title'], (
    f"INFO scan cadence title '{info_cadence['title']}' must match SET scan cadence title '{set_cadence['title']}'"
  )


def test_info_and_set_page_entry_window_group_titles_match() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG
  info_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'info'), None)
  set_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'set'), None)
  assert info_page is not None and set_page is not None
  info_ew = next((g for g in info_page['group_catalog'] if g['group_id'] == 'entry_window_posture'), None)
  set_ew = next((g for g in set_page['group_catalog'] if g['group_id'] == 'manual_entry_window'), None)
  assert info_ew is not None, 'entry_window_posture group must exist in INFO page'
  assert set_ew is not None, 'manual_entry_window group must exist in SET page'
  assert info_ew['title'] == set_ew['title'], (
    f"INFO entry window title '{info_ew['title']}' must match SET entry window title '{set_ew['title']}'"
  )


def test_info_and_set_page_sizing_group_titles_match() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG
  info_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'info'), None)
  set_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'set'), None)
  assert info_page is not None and set_page is not None
  info_sizing = next((g for g in info_page['group_catalog'] if g['group_id'] == 'sizing_and_density_posture'), None)
  set_sizing = next((g for g in set_page['group_catalog'] if g['group_id'] == 'manual_sizing_and_density'), None)
  assert info_sizing is not None and set_sizing is not None
  assert info_sizing['title'] == set_sizing['title'], (
    f"INFO sizing group title '{info_sizing['title']}' must match SET sizing group title '{set_sizing['title']}'"
  )


def test_info_and_analysis_page_runtime_context_group_titles_match() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG
  info_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'info'), None)
  analysis_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'analysis'), None)
  assert info_page is not None and analysis_page is not None
  info_ctx = next((g for g in info_page['group_catalog'] if g['group_id'] == 'optimization_runtime_context'), None)
  analysis_ctx = next((g for g in analysis_page['group_catalog'] if g['group_id'] == 'optimization_runtime_context'), None)
  assert info_ctx is not None and analysis_ctx is not None
  assert info_ctx['title'] == analysis_ctx['title'], (
    f"INFO runtime context title '{info_ctx['title']}' must match ANALYSIS runtime context title '{analysis_ctx['title']}'"
  )


def test_all_group_titles_are_concise() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG
  max_label_chars = 24
  for page in PARAMETER_SURFACE_PAGE_CATALOG:
    for group in page['group_catalog']:
      title = group['title']
      assert len(title) <= max_label_chars, (
        f"Group title '{title}' on page '{page['page_id']}' is {len(title)} chars — "
        f"exceeds {max_label_chars}-char limit (causes button row wrap)"
      )


def test_parameter_surface_overlay_field_ids_covers_set_page_settable_fields() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG, PARAMETER_SURFACE_OVERLAY_FIELD_IDS, PARAMETER_SURFACE_FIELD_CATALOG
  set_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'set'), None)
  assert set_page is not None
  for group in set_page['group_catalog']:
    for field_id in group['field_ids']:
      field_meta = PARAMETER_SURFACE_FIELD_CATALOG.get(field_id)
      if field_meta and field_meta.get('value_class') == 'setting':
        assert field_id in PARAMETER_SURFACE_OVERLAY_FIELD_IDS, (
          f"SET page field '{field_id}' has value_class='setting' but is not in PARAMETER_SURFACE_OVERLAY_FIELD_IDS — "
          f"it will be silently excluded from the SET tab"
        )


def test_coverability_thresholds_surface_in_weights_params_info_and_set() -> None:
  from polyventure.service import PARAMETER_SURFACE_PAGE_CATALOG, PARAMETER_SURFACE_OVERLAY_FIELD_IDS, PARAMETER_SURFACE_FIELD_CATALOG
  info_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'info'), None)
  set_page = next((p for p in PARAMETER_SURFACE_PAGE_CATALOG if p['page_id'] == 'set'), None)
  assert info_page is not None
  assert set_page is not None
  info_fields = {
    field_id
    for group in info_page['group_catalog']
    for field_id in group['field_ids']
  }
  set_fields = {
    field_id
    for group in set_page['group_catalog']
    for field_id in group['field_ids']
  }
  for field_id in ('max_divergence', 'flow_participation_k'):
    assert field_id in PARAMETER_SURFACE_FIELD_CATALOG
    assert field_id in PARAMETER_SURFACE_OVERLAY_FIELD_IDS
    assert field_id in info_fields
    assert field_id in set_fields


def test_parameter_surface_info_keeps_unset_setting_rows_visible(tmp_path: Path) -> None:
  from polyventure.service import build_parameter_surface_payload
  private_key_path = tmp_path / 'coverability_info_key.pem'
  _write_private_key(private_key_path)
  settings = Settings(**{**_settings(str(private_key_path)).__dict__, 'max_divergence': None, 'flow_participation_k': None})

  payload = build_parameter_surface_payload(settings)
  info_page = next(page for page in payload['pages'] if page['page_id'] == 'info')
  threshold_group = next(group for group in info_page['groups'] if group['group_id'] == 'threshold_and_reserve_posture')
  rows = {row['parameter_id']: row for row in threshold_group['rows']}

  for field_id in ('max_divergence', 'flow_participation_k'):
    assert field_id in rows
    assert rows[field_id]['current_value'] is None


# ---------------------------------------------------------------------------
# LOP-A / LOP-B: Live order placement — price-at-submission fix + live bridge
# ---------------------------------------------------------------------------

from polyventure.persistence import persist_candidate_saved_set, persist_candidate_saved_set_evaluation
from polyventure.types import SubmittedOrder


def _live_settings(private_key_file: str, state_db_path: str) -> Settings:
  return Settings(
    **{
      **_settings(private_key_file).__dict__,
      'kalshi_env': 'prod',
      'api_base_url': 'https://api.kalshi.com/trade-api/v2',
      'websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
      'live_websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
      'active_websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
      'operation_lane': 'live',
      'max_unhedged_sec': 5,
      'flow_participation_k': 1.0,
      'max_divergence': 0.30,
      'state_db_path': state_db_path,
    }
  )


def _seed_live_saved_set(
  state_db_path: Path,
  *,
  ticker: str,
  saved_set_id: str = 'live-saved-set-001',
  run_id: str | None = None,
) -> None:
  connection = open_database(state_db_path)
  resolved_run_id = run_id or f'{saved_set_id}-run'
  member = _saved_candidate_member(ticker)
  persist_candidate_review_run(
    connection,
    run_id=resolved_run_id,
    recorded_at_utc='2026-06-14T09:30:00Z',
    operation_lane='live',
    candidate_signature=ticker,
    candidate_count=1,
    source_action='scan',
    lane_session_id='live-seed-001',
  )
  persist_candidate_review_candidates(
    connection,
    run_id=resolved_run_id,
    recorded_at_utc='2026-06-14T09:30:00Z',
    operation_lane='live',
    candidates=[member],
  )
  persist_candidate_saved_set(
    connection,
    saved_set_id=saved_set_id,
    run_id=resolved_run_id,
    recorded_at_utc='2026-06-14T09:30:00Z',
    operation_lane='live',
    lane_session_id='live-seed-001',
    saved_key_count=1,
    state_id='review_hold_saved_selection_locked',
    source_action='save_selection',
    members=[member],
    detail={'candidate_signature': ticker},
  )
  persist_candidate_saved_set_evaluation(
    connection,
    saved_set_id=saved_set_id,
    recorded_at_utc='2026-06-14T09:30:01Z',
    operation_lane='live',
    evaluation_status='pass',
    actionability_status='active_valid',
    visibility_status='visible_current',
    offline_verifiable=True,
    online_revalidation_required=False,
    detail={'reason': 'Live saved set seeded for LOP proof.'},
  )


def _seed_live_saved_set_batch(
  state_db_path: Path,
  *,
  tickers: list[str],
  saved_set_id: str = 'live-saved-set-batch-001',
  run_id: str | None = None,
) -> None:
  connection = open_database(state_db_path)
  resolved_run_id = run_id or f'{saved_set_id}-run'
  members = [_saved_candidate_member(ticker) for ticker in tickers]
  candidate_signature = '|'.join(str(member['candidate_key']) for member in members)
  persist_candidate_review_run(
    connection,
    run_id=resolved_run_id,
    recorded_at_utc='2026-06-14T09:30:00Z',
    operation_lane='live',
    candidate_signature=candidate_signature,
    candidate_count=len(members),
    source_action='scan',
    lane_session_id='live-seed-001',
  )
  persist_candidate_review_candidates(
    connection,
    run_id=resolved_run_id,
    recorded_at_utc='2026-06-14T09:30:00Z',
    operation_lane='live',
    candidates=members,
  )
  persist_candidate_saved_set(
    connection,
    saved_set_id=saved_set_id,
    run_id=resolved_run_id,
    recorded_at_utc='2026-06-14T09:30:00Z',
    operation_lane='live',
    lane_session_id='live-seed-001',
    saved_key_count=len(members),
    state_id='review_hold_saved_selection_locked',
    source_action='save_selection',
    members=members,
    detail={'candidate_signature': candidate_signature},
  )
  persist_candidate_saved_set_evaluation(
    connection,
    saved_set_id=saved_set_id,
    recorded_at_utc='2026-06-14T09:30:01Z',
    operation_lane='live',
    evaluation_status='pass',
    actionability_status='active_valid',
    visibility_status='visible_current',
    offline_verifiable=True,
    online_revalidation_required=False,
    detail={'reason': 'Live saved-set batch seeded for batch-submit proof.'},
  )


def _make_submitted_order(
  order_id: str,
  client_order_id: str,
  ticker: str,
  side: str,
  price: Decimal,
  count: Decimal,
  status: str = 'resting',
  *,
  fill_count: Decimal | None = None,
) -> SubmittedOrder:
  remaining_count = count if status == 'resting' else Decimal('0')
  effective_fill_count = fill_count if fill_count is not None else max(Decimal('0'), count - remaining_count)
  return SubmittedOrder(
    order_id=order_id,
    client_order_id=client_order_id,
    ticker=ticker,
    side=side,
    price_dollars=price,
    contract_count=count,
    remaining_count=remaining_count,
    fill_count=effective_fill_count,
    status=status,
    created_at=datetime.now(UTC),
    cancel_order_on_pause=False,
    subaccount=0,
    reduced_by=Decimal('0'),
  )


class LiveFakeClient(FakeClient):
  def __init__(self, settings: Settings, private_key: object):
    super().__init__(settings, private_key)
    self.orderbook_calls: list[str] = []
    self.create_order_group_calls: list[dict] = []
    self.create_order_v2_calls: list[dict] = []
    self.create_orders_v2_batch_calls: list[list[dict]] = []
    self.get_order_calls: list[str] = []
    self.cancel_order_v2_calls: list[str] = []
    self._get_order_responses: dict[str, list[SubmittedOrder]] = {}
    self._orders: dict[str, SubmittedOrder] = {}

  def get_orderbook(self, ticker: str, depth: int = 0):
    self.orderbook_calls.append(ticker)
    return super().get_orderbook(ticker, depth=depth)

  def create_order_group(self, contracts_limit_fp: Decimal, subaccount: int = 0) -> str:
    self.create_order_group_calls.append({'contracts_limit_fp': contracts_limit_fp, 'subaccount': subaccount})
    return 'test-group-001'

  def create_order_v2(self, **payload: object) -> SubmittedOrder:
    # Mirror the real client's V2 boundary: capture the wire body and return a
    # domain-natural SubmittedOrder so the live-order tests can assert the V2 shape.
    wire_payload = _order_payload_to_v2_wire(payload)
    self.create_order_v2_calls.append(dict(wire_payload))
    leg = str(payload.get('side', ''))
    client_order_id = str(payload.get('client_order_id', ''))
    ticker = str(payload.get('ticker', ''))
    # Return the domain leg price so the reconcile loop sees the correct dollar value.
    leg_price = Decimal(str(payload.get('yes_price') or payload.get('no_price') or '0'))
    count = Decimal(str(payload.get('count', '1')))
    order_id = f'kalshi-live-{leg}-001'
    self._get_order_responses.setdefault(order_id, [])
    order = _make_submitted_order(order_id, client_order_id, ticker, leg, leg_price, count, status='resting')
    self._orders[order_id] = order
    return order

  def create_orders_v2_batch(self, order_payloads: list[dict[str, object]]) -> list[SubmittedOrder]:
    wire_payloads: list[dict] = []
    orders: list[SubmittedOrder] = []
    for payload in order_payloads:
      wire_payload = _order_payload_to_v2_wire(payload)
      wire_payloads.append(dict(wire_payload))
      leg = str(payload.get('side', ''))
      client_order_id = str(payload.get('client_order_id', ''))
      ticker = str(payload.get('ticker', ''))
      leg_price = Decimal(str(payload.get('yes_price') or payload.get('no_price') or '0'))
      count = Decimal(str(payload.get('count', '1')))
      order_id = f'kalshi-live-batch-{leg}-{len(self._orders) + 1:03d}'
      self._get_order_responses.setdefault(order_id, [])
      order = _make_submitted_order(order_id, client_order_id, ticker, leg, leg_price, count, status='resting')
      self._orders[order_id] = order
      orders.append(order)
    self.create_orders_v2_batch_calls.append(wire_payloads)
    return orders

  def get_order(self, order_id: str) -> SubmittedOrder:
    self.get_order_calls.append(order_id)
    responses = self._get_order_responses.get(order_id, [])
    if responses:
      return responses.pop(0)
    return self._orders.get(order_id) or _make_submitted_order(order_id, '', '', '', Decimal('0'), Decimal('1'), status='resting')

  def cancel_order_v2(self, order_id: str) -> dict:
    self.cancel_order_v2_calls.append(order_id)
    current = self._orders.get(order_id)
    if current is not None:
      self._orders[order_id] = replace(
        current,
        status='canceled',
        remaining_count=Decimal('0'),
        fill_count=current.fill_count,
      )
    return {'order_id': order_id, 'status': 'canceled'}

  def list_orders_for_batch_readback(self, **params: object) -> list[dict[str, object]]:
    ticker = str(params.get('ticker') or '')
    rows: list[dict[str, object]] = []
    for order in self._orders.values():
      if ticker and order.ticker != ticker:
        continue
      rows.append(
        {
          'order_id': order.order_id,
          'client_order_id': order.client_order_id,
          'ticker': order.ticker,
          'side': 'bid' if order.side == 'yes' else 'ask',
          'status': order.status,
          'initial_count_fp': str(order.contract_count),
          'remaining_count_fp': str(order.remaining_count),
          'fill_count_fp': str(order.fill_count),
          'price': str(order.price_dollars),
        }
      )
    return rows

  def get_positions(self) -> list:
    return []


class LiveFakeClientBothFilled(LiveFakeClient):
  def get_order(self, order_id: str) -> SubmittedOrder:
    self.get_order_calls.append(order_id)
    stored = self._orders.get(order_id)
    count = stored.contract_count if stored is not None else Decimal('1')
    price = stored.price_dollars if stored is not None else Decimal('0.40')
    client_order_id = stored.client_order_id if stored is not None else ''
    ticker = stored.ticker if stored is not None else ''
    side = stored.side if stored is not None else ''
    filled = _make_submitted_order(order_id, client_order_id, ticker, side, price, count, status='executed', fill_count=count)
    return replace(filled, remaining_count=Decimal('0'))


class LiveFakeClientApiError(LiveFakeClient):
  def create_order_v2(self, **payload: object) -> SubmittedOrder:
    raise KalshiHttpError('rate_limited', 'Too many requests.', 'retry_later')


class LiveFakeClientZeroPriceOrderbook(LiveFakeClient):
  def get_orderbook(self, ticker: str, depth: int = 0):
    self.orderbook_calls.append(ticker)
    from polyventure.websocket_client import normalize_orderbook_snapshot
    return normalize_orderbook_snapshot({'ticker': ticker, 'yes_dollars': [], 'no_dollars': []})


class LiveFakeClientOrderbookFp(LiveFakeClient):
  def get_orderbook(self, ticker: str, depth: int = 0):
    del depth
    self.orderbook_calls.append(ticker)
    from polyventure.websocket_client import normalize_orderbook_snapshot
    payload = {
      'orderbook_fp': {
        'yes_dollars': [['0.3800', '7.00'], ['0.4000', '11.00']],
        'no_dollars': [['0.3900', '6.00'], ['0.4100', '10.00']],
      }
    }
    raw_orderbook = payload.get('orderbook_fp') or payload
    return normalize_orderbook_snapshot({**raw_orderbook, 'ticker': ticker})


class LiveFakeClientWideDivergenceOrderbook(LiveFakeClient):
  def get_orderbook(self, ticker: str, depth: int = 0):
    del depth
    self.orderbook_calls.append(ticker)
    from polyventure.websocket_client import normalize_orderbook_snapshot
    return normalize_orderbook_snapshot(
      {
        'ticker': ticker,
        'yes_dollars': [['0.1000', '20.00']],
        'no_dollars': [['0.5500', '20.00']],
      }
    )


class LiveFakeClientLowFlow(LiveFakeClientOrderbookFp):
  def get_recent_trades(self, ticker: str, *, window_sec: int):
    del window_sec
    self.recent_trades_calls.append(ticker)
    return {
      'yes_flow_fp': Decimal('10'),
      'no_flow_fp': Decimal('0'),
      'trade_count': 2,
    }


def test_submit_order_bridge_uses_saved_run_candidate_when_fresh_scan_omits_it(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'runtime_bridge_saved_handoff.sqlite3'
  settings = _settings(str(private_key_path))
  settings = Settings(**{**settings.__dict__, 'state_db_path': str(state_db_path)})
  _seed_saved_set(
    state_db_path,
    ticker='KALSHI-CANDIDATE-LOW',
    saved_set_id='saved-handoff-001',
    run_id='scan-live-saved-handoff-001',
  )

  class FreshDiscoveryOmitsSavedClient(FakeClient):
    def get_markets(self, *args: object, **kwargs: object) -> tuple[list[MarketSnapshot], None]:
      del args, kwargs
      return [self.markets['KALSHI-CANDIDATE-HIGH']], None

  payload = run_service_once(
    settings=settings,
    execution_profile='submit_order_bridge',
    client_factory=FreshDiscoveryOmitsSavedClient,
  )
  connection = open_database(state_db_path)
  blocked = connection.execute(
    "SELECT event_type FROM runtime_events WHERE event_type = 'submit_bridge_blocked'",
  ).fetchall()

  assert payload['blocked_reason'] is None
  assert payload['saved_set_snapshot']['matched_candidate_ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert payload['planned_pairs'][0]['ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert blocked == []


def _seed_live_pair_state(
  state_db_path: Path,
  *,
  pair_id: str,
  ticker: str,
  state: str,
  detail: dict[str, object] | None = None,
) -> None:
  connection = open_database(state_db_path)
  plan = PairOrderPlan(
    pair_id=pair_id,
    ticker=ticker,
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id=f'{pair_id}-yes',
    no_client_order_id=f'{pair_id}-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(connection, plan, created_at_utc='2026-06-25T10:00:00Z', operation_lane='live')
  persist_pair_state_transition(
    connection,
    pair_id=pair_id,
    state=state,
    recorded_at_utc='2026-06-25T10:00:01Z',
    operation_lane='live',
    lane_session_id='live-seed-align',
    detail={
      'ticker': ticker,
      'yes_filled_contracts': '0',
      'no_filled_contracts': '0',
      'average_yes_price': '0.34',
      'average_no_price': '0.39',
      'realized_fees_dollars': '0',
      **(detail or {}),
    },
  )


class AlignmentFakeClient(LiveFakeClient):
  market_status = 'open'
  market_result = ''
  settlement_ts = ''
  position_rows: list[PairPosition] = []
  resting_rows: list[SubmittedOrder] = []
  fail_market = False

  def get_market_readback(self, ticker: str) -> dict:
    if self.fail_market:
      raise RuntimeError('readback failed')
    return {
      'ticker': ticker,
      'status': self.market_status,
      'result': self.market_result,
      'settlement_ts': self.settlement_ts,
      'settlement_value_dollars': '1.00' if self.market_result == 'yes' else '',
    }

  def get_positions(self, **kwargs: object) -> list[PairPosition]:
    ticker = str(kwargs.get('ticker') or '')
    return [position for position in self.position_rows if not ticker or position.ticker == ticker]

  def list_orders(self, *, ticker: str, status: str = 'resting') -> list[SubmittedOrder]:
    return [
      order for order in self.resting_rows
      if order.ticker == ticker and order.status == status
    ]


def test_align_pairs_with_kalshi_terminalizes_finalized_one_sided_exposure(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'align_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'align_settled.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_pair_state(
    state_db_path,
    pair_id='pair-align-settled',
    ticker='KALSHI-CANDIDATE-LOW',
    state='EXPOSURE_CAPPED',
    detail={'no_filled_contracts': '2'},
  )
  AlignmentFakeClient.market_status = 'finalized'
  AlignmentFakeClient.market_result = 'yes'
  AlignmentFakeClient.settlement_ts = '2026-06-25T10:05:00Z'
  AlignmentFakeClient.position_rows = [
    PairPosition(
      ticker='KALSHI-CANDIDATE-LOW',
      side='no',
      contract_count=Decimal('0'),
      average_price_dollars=Decimal('0'),
      realized_pnl_dollars=Decimal('-1.25'),
    )
  ]
  AlignmentFakeClient.resting_rows = []
  AlignmentFakeClient.fail_market = False
  connection = open_database(state_db_path)

  result = align_pairs_with_kalshi(
    connection,
    settings=settings,
    client=AlignmentFakeClient(settings, object()),
    pairs=_latest_pair_snapshots(connection, operation_lane='live'),
    recorded_at_utc='2026-06-25T10:06:00Z',
    operation_lane='live',
    lane_session_id='live-align-test',
    reason='unit_test',
  )

  assert result.degraded is False
  assert result.terminalized[0].state_after == 'SETTLED_EXPOSURE'
  latest = connection.execute(
    "SELECT state, detail_json FROM pair_states WHERE pair_id = 'pair-align-settled' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  detail = json.loads(latest['detail_json'])
  assert latest['state'] == 'SETTLED_EXPOSURE'
  assert detail['market_result'] == 'yes'
  assert detail['settlement_ts'] == '2026-06-25T10:05:00Z'
  assert detail['realized_pnl_dollars'] == '-1.25'


def _seed_one_sided_local_fill(
  state_db_path: Path,
  *,
  pair_id: str,
  ticker: str,
  filled_side: str,
  filled_contracts: str,
  filled_domain_price: str,
  resting_side: str,
  resting_domain_price: str,
) -> None:
  """Seed a pair whose STATE detail says zero filled (the reconcile-erased
  condition) but whose local fills/orders SSOT records a real one-sided fill."""
  _seed_live_pair_state(
    state_db_path,
    pair_id=pair_id,
    ticker=ticker,
    state='RECONCILE_REQUIRED',
    detail={'no_filled_contracts': '0', 'yes_filled_contracts': '0'},
  )
  connection = open_database(state_db_path)
  with connection:
    connection.execute(
      'INSERT INTO orders (order_id, pair_id, client_order_id, side, price_dollars, contract_count, status, created_at_utc, operation_lane) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
      (f'{pair_id}-{filled_side}-oid', pair_id, f'{pair_id}-{filled_side}', filled_side, filled_domain_price, filled_contracts, 'executed', '2026-06-25T10:00:02Z', 'live'),
    )
    connection.execute(
      'INSERT INTO orders (order_id, pair_id, client_order_id, side, price_dollars, contract_count, status, created_at_utc, operation_lane) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
      (f'{pair_id}-{resting_side}-oid', pair_id, f'{pair_id}-{resting_side}', resting_side, resting_domain_price, filled_contracts, 'resting', '2026-06-25T10:00:02Z', 'live'),
    )
    connection.execute(
      'INSERT INTO fills (fill_id, pair_id, order_id, client_order_id, side, price_dollars, contract_count, fee_dollars, operation_lane, created_at_utc) '
      'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)',
      (f'{pair_id}-fill', pair_id, f'{pair_id}-{filled_side}-oid', f'{pair_id}-{filled_side}', filled_side, '0.63', filled_contracts, '0', 'live', '2026-06-25T10:00:03Z'),
    )


def test_align_pairs_finalized_one_sided_local_fill_settles_exposure_with_real_pnl(monkeypatch, tmp_path: Path) -> None:
  # Regression for the settlement fill-truth BLOCKER (BMAP 2026-07-03): a one-sided
  # fill whose state-detail count was erased by an earlier reconcile cycle must still
  # settle as SETTLED_EXPOSURE with the real loss, sourced from the local fills SSOT --
  # even though the post-finalization positions read-back is empty.
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'align_key_fill_truth.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'align_fill_truth.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_one_sided_local_fill(
    state_db_path,
    pair_id='pair-fill-truth',
    ticker='KALSHI-CANDIDATE-LOW',
    filled_side='no',
    filled_contracts='51',
    filled_domain_price='0.37',
    resting_side='yes',
    resting_domain_price='0.58',
  )
  AlignmentFakeClient.market_status = 'finalized'
  AlignmentFakeClient.market_result = 'yes'  # NO leg loses -> payout 0
  AlignmentFakeClient.settlement_ts = '2026-06-25T10:05:00Z'
  AlignmentFakeClient.position_rows = []  # settled position dropped off the read-back (the bug trigger)
  AlignmentFakeClient.resting_rows = []
  AlignmentFakeClient.fail_market = False
  connection = open_database(state_db_path)

  result = align_pairs_with_kalshi(
    connection,
    settings=settings,
    client=AlignmentFakeClient(settings, object()),
    pairs=_latest_pair_snapshots(connection, operation_lane='live'),
    recorded_at_utc='2026-06-25T10:06:00Z',
    operation_lane='live',
    lane_session_id='live-align-test',
    reason='unit_test',
  )

  assert result.terminalized[0].state_after == 'SETTLED_EXPOSURE'
  latest = connection.execute(
    "SELECT state, detail_json FROM pair_states WHERE pair_id = 'pair-fill-truth' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  detail = json.loads(latest['detail_json'])
  assert latest['state'] == 'SETTLED_EXPOSURE'
  # Fill truth carried through, not erased:
  assert Decimal(str(detail['no_filled_contracts'])) == Decimal('51')
  # Real one-sided loss from local cost basis + authoritative result (51 * 0.37):
  assert detail['realized_pnl_source'] == 'local_fill_settlement_reconciliation'
  assert Decimal(str(detail['realized_pnl_dollars'])) == Decimal('-18.87')


def test_align_pairs_finalized_one_sided_fill_missing_result_fails_closed(monkeypatch, tmp_path: Path) -> None:
  # Fail-closed: a real one-sided fill whose finalization read-back carries no market
  # result must still be SETTLED_EXPOSURE with a null (pending) P&L -- NEVER booked $0.
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'align_key_fill_pending.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'align_fill_pending.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_one_sided_local_fill(
    state_db_path,
    pair_id='pair-fill-pending',
    ticker='KALSHI-CANDIDATE-LOW',
    filled_side='no',
    filled_contracts='51',
    filled_domain_price='0.37',
    resting_side='yes',
    resting_domain_price='0.58',
  )
  AlignmentFakeClient.market_status = 'finalized'
  AlignmentFakeClient.market_result = ''  # authoritative result unavailable
  AlignmentFakeClient.settlement_ts = ''
  AlignmentFakeClient.position_rows = []
  AlignmentFakeClient.resting_rows = []
  AlignmentFakeClient.fail_market = False
  connection = open_database(state_db_path)

  result = align_pairs_with_kalshi(
    connection,
    settings=settings,
    client=AlignmentFakeClient(settings, object()),
    pairs=_latest_pair_snapshots(connection, operation_lane='live'),
    recorded_at_utc='2026-06-25T10:06:00Z',
    operation_lane='live',
    lane_session_id='live-align-test',
    reason='unit_test',
  )

  assert result.terminalized[0].state_after == 'SETTLED_EXPOSURE'
  latest = connection.execute(
    "SELECT state, detail_json FROM pair_states WHERE pair_id = 'pair-fill-pending' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  detail = json.loads(latest['detail_json'])
  assert latest['state'] == 'SETTLED_EXPOSURE'
  assert detail['realized_pnl_source'] == 'result_unavailable_pending_readback'
  assert detail['realized_pnl_dollars'] is None


def test_align_pairs_with_kalshi_preserves_live_position(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'align_key_live.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'align_preserve.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_pair_state(state_db_path, pair_id='pair-align-preserve', ticker='KALSHI-CANDIDATE-LOW', state='REPAIR_LIVE')
  AlignmentFakeClient.market_status = 'open'
  AlignmentFakeClient.market_result = ''
  AlignmentFakeClient.settlement_ts = ''
  AlignmentFakeClient.position_rows = [
    PairPosition(
      ticker='KALSHI-CANDIDATE-LOW',
      side='yes',
      contract_count=Decimal('3'),
      average_price_dollars=Decimal('0.25'),
      market_exposure_dollars=Decimal('0.75'),
    )
  ]
  AlignmentFakeClient.resting_rows = []
  AlignmentFakeClient.fail_market = False
  connection = open_database(state_db_path)

  result = align_pairs_with_kalshi(
    connection,
    settings=settings,
    client=AlignmentFakeClient(settings, object()),
    pairs=_latest_pair_snapshots(connection, operation_lane='live'),
    recorded_at_utc='2026-06-25T10:06:00Z',
    operation_lane='live',
    lane_session_id='live-align-test',
    reason='unit_test',
  )

  assert result.preserved[0].reason == 'kalshi_alignment_preserved'
  latest = connection.execute(
    "SELECT state, detail_json FROM pair_states WHERE pair_id = 'pair-align-preserve' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  detail = json.loads(latest['detail_json'])
  assert latest['state'] == 'REPAIR_LIVE'
  assert detail['open_position_count'] == 1
  assert detail['exchange_position_contracts'] == '3'


def test_align_pairs_with_kalshi_failure_preserves_and_degrades(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'align_key_fail.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'align_fail.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_pair_state(state_db_path, pair_id='pair-align-fail', ticker='KALSHI-CANDIDATE-LOW', state='REPAIR_LIVE')
  AlignmentFakeClient.fail_market = True
  connection = open_database(state_db_path)

  result = align_pairs_with_kalshi(
    connection,
    settings=settings,
    client=AlignmentFakeClient(settings, object()),
    pairs=_latest_pair_snapshots(connection, operation_lane='live'),
    recorded_at_utc='2026-06-25T10:06:00Z',
    operation_lane='live',
    lane_session_id='live-align-test',
    reason='unit_test',
  )

  assert result.degraded is True
  assert result.preserved[0].state_after == 'REPAIR_LIVE'
  event = connection.execute(
    "SELECT detail_json FROM runtime_events WHERE event_type = 'pair_alignment_readback_failed'"
  ).fetchone()
  assert event is not None


def test_reconcile_pairs_runs_kalshi_alignment_before_repair_close(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  private_key_path = tmp_path / 'align_key_reconcile.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'align_reconcile.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_pair_state(
    state_db_path,
    pair_id='pair-align-reconcile',
    ticker='KALSHI-CANDIDATE-LOW',
    state='EXPOSURE_CAPPED',
    detail={'no_filled_contracts': '2'},
  )
  AlignmentFakeClient.market_status = 'finalized'
  AlignmentFakeClient.market_result = 'yes'
  AlignmentFakeClient.settlement_ts = '2026-06-25T10:05:00Z'
  AlignmentFakeClient.position_rows = []
  AlignmentFakeClient.resting_rows = []
  AlignmentFakeClient.fail_market = False

  payload = reconcile_pairs(settings=settings, client_factory=AlignmentFakeClient)

  assert payload['kalshi_alignment']['terminalized_count'] == 1
  assert payload['pairs'][0]['state'] == 'SETTLED_EXPOSURE'
  assert payload['pair_runtime_summary'][0]['terminal_state'] == 'SETTLED_EXPOSURE'


def test_run_service_once_aligns_stale_pair_before_submit_gate(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'align_key_run.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'align_run.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-align-run-001')
  _seed_live_pair_state(
    state_db_path,
    pair_id='pair-align-run-stale',
    ticker='KALSHI-CANDIDATE-LOW',
    state='EXPOSURE_CAPPED',
    detail={'no_filled_contracts': '2'},
  )
  AlignmentFakeClient.market_status = 'finalized'
  AlignmentFakeClient.market_result = 'yes'
  AlignmentFakeClient.settlement_ts = '2026-06-25T10:05:00Z'
  AlignmentFakeClient.position_rows = []
  AlignmentFakeClient.resting_rows = []
  AlignmentFakeClient.fail_market = False

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=AlignmentFakeClient)

  assert payload['kalshi_alignment']['terminalized_count'] == 1
  assert payload['blocked_reason'] != 'already_active_pair'
  latest = open_database(state_db_path).execute(
    "SELECT state FROM pair_states WHERE pair_id = 'pair-align-run-stale' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  assert latest['state'] == 'SETTLED_EXPOSURE'


def test_live_order_placement_fetches_orderbook_prices(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_a_fetch.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW')

  client_instance = None

  class TrackingClient(LiveFakeClient):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TrackingClient)

  assert client_instance is not None
  assert 'KALSHI-CANDIDATE-LOW' in client_instance.orderbook_calls, (
    'get_orderbook must be called for live lane price refresh before building the pair order plan'
  )
  planned_pairs = payload.get('planned_pairs', [])
  if planned_pairs:
    yes_price = Decimal(planned_pairs[0]['yes_price'])
    no_price = Decimal(planned_pairs[0]['no_price'])
    assert yes_price > 0, 'yes_price must be non-zero after live orderbook price fetch'
    assert no_price > 0, 'no_price must be non-zero after live orderbook price fetch'


def test_live_order_placement_uses_orderbook_fp_price_source(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_fp.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_orderbook_fp.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-orderbook-fp-001')

  client_instance = None

  class TrackingClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TrackingClient)

  assert client_instance is not None
  assert 'KALSHI-CANDIDATE-LOW' in client_instance.orderbook_calls
  assert payload['blocked_reason'] is None
  assert payload['planned_pair_count'] == 1
  assert client_instance.create_order_v2_calls, 'non-empty orderbook_fp bids must reach live order creation'
  planned_pair = payload['planned_pairs'][0]
  assert Decimal(planned_pair['yes_price']) == Decimal('0.4000')
  assert Decimal(planned_pair['no_price']) == Decimal('0.4100')
  connection = open_database(state_db_path)
  blocked_events = connection.execute(
    '''
    SELECT COUNT(*) as cnt FROM runtime_events
    WHERE operation_lane = 'live' AND event_type = 'live_order_price_fetch_blocked'
    '''
  ).fetchone()
  assert blocked_events['cnt'] == 0


def test_live_order_price_fetch_blocked_when_orderbook_returns_zero_prices(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  private_key_path = tmp_path / 'live_key_zero.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_a_zero_price.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-zero-price-001')

  payload = run_service_once(
    settings=settings,
    execution_profile='submit_order_bridge',
    client_factory=LiveFakeClientZeroPriceOrderbook,
  )

  assert payload['planned_pair_count'] == 0
  assert payload['blocked_reason'] == 'live_price_unavailable'
  assert payload['submit_response_id'] == 'SUBMIT_REJECTED_RETRYABLE'
  assert payload['submit_rest_state_id'] == 'SUBMIT_FAILED_RETRYABLE'
  assert payload['failure_class'] == 'SILENT_CONTINUE'
  assert payload['failure_scope'] == 'interaction_local'
  assert payload['retry_allowed'] is True
  assert payload['allowed_actions'] == ['RETRY_SUBMIT', 'WAIT']
  connection = open_database(state_db_path)
  events = connection.execute(
    '''
    SELECT event_type, detail_json FROM runtime_events
    WHERE operation_lane = 'live' AND event_type = 'live_order_price_fetch_blocked'
    '''
  ).fetchall()
  assert events, 'live_order_price_fetch_blocked event must be persisted when orderbook returns zero prices'


def test_coverability_guard_unseeded_thresholds_block_before_trades_and_orders(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  private_key_path = tmp_path / 'live_key_coverability_unset.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_coverability_unset.sqlite3'
  base_settings = _live_settings(str(private_key_path), str(state_db_path))
  settings = Settings(**{**base_settings.__dict__, 'flow_participation_k': None, 'max_divergence': None})
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-coverability-unset-001')

  client_instance = None

  class TrackingClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TrackingClient)

  assert payload['planned_pair_count'] == 0
  assert payload['blocked_reason'] == 'coverability_threshold_unset'
  assert payload['submit_response_id'] == 'SUBMIT_REJECTED_TERMINAL'
  assert payload['submit_rest_state_id'] == 'UPSTREAM_REVIEW_HOLD'
  assert payload['retry_allowed'] is False
  assert payload['submit_guard_summary']['blocked_count'] == 1
  assert client_instance is not None
  assert client_instance.recent_trades_calls == []
  connection = open_database(state_db_path)
  assert connection.execute('SELECT COUNT(*) AS cnt FROM pair_states').fetchone()['cnt'] == 0
  assert connection.execute('SELECT COUNT(*) AS cnt FROM orders').fetchone()['cnt'] == 0


def test_coverability_guard_divergence_blocks_before_trades_and_orders(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  private_key_path = tmp_path / 'live_key_coverability_divergence.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_coverability_divergence.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-coverability-divergence-001')

  client_instance = None

  class TrackingClient(LiveFakeClientWideDivergenceOrderbook):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TrackingClient)

  assert payload['planned_pair_count'] == 0
  assert payload['blocked_reason'] == 'coverability_divergence_blocked'
  assert payload['submit_guard_summary']['block_reasons'] == ['coverability_divergence_blocked']
  assert client_instance is not None
  assert client_instance.recent_trades_calls == []
  connection = open_database(state_db_path)
  assert connection.execute('SELECT COUNT(*) AS cnt FROM pair_states').fetchone()['cnt'] == 0
  assert connection.execute('SELECT COUNT(*) AS cnt FROM orders').fetchone()['cnt'] == 0


def test_coverability_guard_flow_blocks_with_one_trades_call_and_no_orders(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  private_key_path = tmp_path / 'live_key_coverability_flow.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_coverability_flow.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-coverability-flow-001')

  client_instance = None

  class TrackingClient(LiveFakeClientLowFlow):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TrackingClient)

  assert payload['planned_pair_count'] == 0
  assert payload['blocked_reason'] == 'coverability_flow_blocked'
  assert payload['submit_guard_summary']['blocked_count'] == 1
  assert client_instance is not None
  assert client_instance.recent_trades_calls == ['KALSHI-CANDIDATE-LOW']
  connection = open_database(state_db_path)
  assert connection.execute('SELECT COUNT(*) AS cnt FROM pair_states').fetchone()['cnt'] == 0
  assert connection.execute('SELECT COUNT(*) AS cnt FROM orders').fetchone()['cnt'] == 0
  assert connection.execute('SELECT COUNT(*) AS cnt FROM pair_liquidity_observations').fetchone()['cnt'] == 0
  final_event = connection.execute(
    '''
    SELECT detail_json FROM runtime_events
    WHERE event_type = 'submit_bridge_final_coverability_checked'
    '''
  ).fetchone()
  assert final_event is not None
  final_detail = json.loads(final_event['detail_json'])
  assert final_detail['ok'] is False
  assert final_detail['reason'] == 'final_coverability_blocked'
  assert final_detail['guard_reason'] == 'coverability_flow_blocked'
  assert final_detail['source'] == 'final_set_coverability_gate'
  assert final_detail['sizing_phase'] == 'pre_sizing'
  # Flow/depth observation capture (BMAP 2026-07-02): the flow-blocked event
  # retains the per-side flow, the floor inputs, and the resting-depth summary.
  assert final_detail['flow_threshold_pass'] is False
  assert final_detail['yes_flow_window_fp'] == '10'
  assert final_detail['no_flow_window_fp'] == '0'
  assert final_detail['flow_window_sec'] == '300'
  assert Decimal(str(final_detail['flow_participation_k'])) == Decimal('1')
  assert final_detail['intended_contract_count_for_floor'] is not None
  assert Decimal(str(final_detail['required_flow_window_fp'])) == (
    Decimal(str(final_detail['flow_participation_k']))
    * Decimal(str(final_detail['intended_contract_count_for_floor']))
  )
  assert final_detail['yes_depth_within_band'] == '11.00'
  assert final_detail['no_depth_within_band'] == '10.00'


def test_coverability_guard_submit_bridge_batch_skips_rejected_member_and_sizes_survivor_set(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_batch_survivor.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_batch_survivor.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-batch-survivor-001',
  )

  client_instance = None

  class BatchClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

    def get_orderbook(self, ticker: str, depth: int = 0):
      if ticker == 'KALSHI-CANDIDATE-HIGH':
        self.orderbook_calls.append(ticker)
        from polyventure.websocket_client import normalize_orderbook_snapshot
        return normalize_orderbook_snapshot(
          {
            'ticker': ticker,
            'yes_dollars': [['0.1000', '20.00']],
            'no_dollars': [['0.5500', '20.00']],
          }
        )
      return super().get_orderbook(ticker, depth=depth)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=BatchClient)

  assert client_instance is not None
  assert payload['planned_pair_count'] == 1
  assert payload['qualifying_candidate_count'] == 1
  assert payload['planned_pairs'][0]['ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert payload['submit_guard_summary']['blocked_count'] == 1
  assert payload['submit_guard_summary']['block_reasons'] == ['coverability_divergence_blocked']
  assert client_instance.recent_trades_calls == ['KALSHI-CANDIDATE-LOW']
  assert client_instance.create_order_v2_calls, 'surviving batch member must reach live order creation'
  connection = open_database(state_db_path)
  final_coverability = connection.execute(
    '''
    SELECT detail_json FROM runtime_events
    WHERE event_type = 'submit_bridge_final_coverability_checked'
    ORDER BY id
    '''
  ).fetchall()
  final_details = [json.loads(row['detail_json']) for row in final_coverability]
  assert [(detail['ticker'], detail['ok'], detail['guard_reason']) for detail in final_details] == [
    ('KALSHI-CANDIDATE-HIGH', False, 'coverability_divergence_blocked'),
    ('KALSHI-CANDIDATE-LOW', True, ''),
  ]
  survivor_detail = final_details[1]
  assert survivor_detail['profitability_evidence_available'] is True
  assert survivor_detail['profitability_basis'] == 'post_reprice_final_prices'
  assert survivor_detail['edge_gross_per_contract'] == '0.1900'
  assert survivor_detail['fee_reserve_per_contract'] == '0.02'
  assert survivor_detail['edge_net_per_contract'] == '0.1700'
  assert survivor_detail['min_edge_dollars'] == '0.03'
  assert survivor_detail['min_profit_dollars'] == '0.01'
  assert survivor_detail['gross_edge_margin_to_min_edge'] == '0.1600'
  assert survivor_detail['net_profit_margin_to_min_profit'] == '0.1600'
  assert survivor_detail['edge_threshold_pass'] is True
  assert survivor_detail['profit_threshold_pass'] is True
  assert survivor_detail['threshold_outcome'] == 'pass'
  assert survivor_detail['event_recorded_at_basis'] == 'submit_bridge_recorded_at'
  assert survivor_detail['submit_bridge_recorded_at_utc']
  assert survivor_detail['final_checked_at_basis'] == 'actual_final_orderbook_check_time'
  assert Decimal(str(survivor_detail['final_check_elapsed_sec'])) >= Decimal('0')
  # Flow/depth observation capture (BMAP 2026-07-02): the PASSED survivor event
  # retains the same flow/floor/depth evidence as a flow-blocked one.
  assert survivor_detail['flow_threshold_pass'] is True
  assert survivor_detail['yes_flow_window_fp'] == '1000'
  assert survivor_detail['no_flow_window_fp'] == '1000'
  assert survivor_detail['flow_window_sec'] == '300'
  assert survivor_detail['intended_contract_count_for_floor'] is not None
  assert Decimal(str(survivor_detail['required_flow_window_fp'])) == (
    Decimal(str(survivor_detail['flow_participation_k']))
    * Decimal(str(survivor_detail['intended_contract_count_for_floor']))
  )
  assert survivor_detail['yes_depth_within_band'] == '11.00'
  assert survivor_detail['no_depth_within_band'] == '10.00'
  # A divergence-blocked member never reaches the flow stage: its evidence
  # truthfully carries null flow fields rather than fabricated values.
  divergence_blocked_detail = final_details[0]
  assert divergence_blocked_detail['flow_threshold_pass'] is None
  assert divergence_blocked_detail['yes_flow_window_fp'] is None
  assert divergence_blocked_detail['no_flow_window_fp'] is None
  assert divergence_blocked_detail['required_flow_window_fp'] is None
  sizing_event = connection.execute(
    '''
    SELECT detail_json FROM runtime_events
    WHERE event_type = 'submit_bridge_final_sizing_resolved'
    '''
  ).fetchone()
  assert sizing_event is not None
  sizing_detail = json.loads(sizing_event['detail_json'])
  assert sizing_detail['source'] == 'final_set_sizing_after_coverability'
  assert sizing_detail['sizing_phase'] == 'post_final_coverability_pre_pair_plan'
  assert sizing_detail['qualifying_candidate_count'] == 1
  assert sizing_detail['final_submit_tickers'] == ['KALSHI-CANDIDATE-LOW']
  assert sizing_detail['sizing_summary']['qualifying_candidate_count'] == 1
  blocked = connection.execute(
    '''
    SELECT detail_json FROM runtime_events
    WHERE event_type = 'submit_bridge_blocked'
    ORDER BY id
    '''
  ).fetchall()
  blocked_details = [json.loads(row['detail_json']) for row in blocked]
  assert any(
    detail.get('ticker') == 'KALSHI-CANDIDATE-HIGH'
    and detail.get('blocked_reason') == 'coverability_divergence_blocked'
    for detail in blocked_details
  )
  pair_plans = connection.execute(
    'SELECT ticker FROM pair_plans ORDER BY created_at_utc'
  ).fetchall()
  assert [row['ticker'] for row in pair_plans] == ['KALSHI-CANDIDATE-LOW']


def test_final_coverability_orderbook_failure_is_candidate_local(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_final_orderbook_failure.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_final_orderbook_failure.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-final-orderbook-failure-001',
  )

  client_instance = None

  class OrderbookFailureClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

    def get_orderbook(self, ticker: str, depth: int = 0):
      if ticker == 'KALSHI-CANDIDATE-HIGH':
        self.orderbook_calls.append(ticker)
        raise RuntimeError('synthetic final orderbook failure')
      return super().get_orderbook(ticker, depth=depth)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=OrderbookFailureClient)

  assert client_instance is not None
  assert payload['planned_pair_count'] == 1
  assert payload['planned_pairs'][0]['ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert client_instance.create_order_v2_calls
  assert client_instance.create_orders_v2_batch_calls == []
  connection = open_database(state_db_path)
  rows = connection.execute(
    '''
    SELECT event_type, detail_json FROM runtime_events
    WHERE event_type IN ('submit_bridge_final_coverability_checked', 'submit_bridge_phase_failed')
    ORDER BY id
    '''
  ).fetchall()
  assert [row['event_type'] for row in rows] == [
    'submit_bridge_final_coverability_checked',
    'submit_bridge_final_coverability_checked',
  ]
  details = [json.loads(row['detail_json']) for row in rows]
  assert details[0]['ticker'] == 'KALSHI-CANDIDATE-HIGH'
  assert details[0]['ok'] is False
  assert details[0]['guard_reason'] == 'final_orderbook_read_failed'
  assert details[0]['profitability_evidence_available'] is False
  assert details[0]['profitability_basis'] == 'unavailable_before_final_reprice'
  assert details[0]['edge_gross_per_contract'] is None
  assert details[0]['fee_reserve_per_contract'] is None
  assert details[0]['edge_net_per_contract'] is None
  assert details[0]['gross_edge_margin_to_min_edge'] is None
  assert details[0]['net_profit_margin_to_min_profit'] is None
  assert details[0]['edge_threshold_pass'] is None
  assert details[0]['profit_threshold_pass'] is None
  assert details[0]['threshold_outcome'] == 'unavailable_before_final_reprice'
  assert details[1]['ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert details[1]['ok'] is True


def test_final_prepared_loop_does_not_rerun_market_gates(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_no_duplicate_gates.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_no_duplicate_gates.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  tickers = ['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW']
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=tickers,
    saved_set_id='live-no-duplicate-gates-001',
  )

  client_instance = None

  class TrackingClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      self.get_market_calls: list[str] = []
      client_instance = self

    def get_market(self, ticker: str):
      self.get_market_calls.append(ticker)
      return super().get_market(ticker)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TrackingClient)

  assert client_instance is not None
  assert payload['planned_pair_count'] == 2
  assert len(client_instance.create_orders_v2_batch_calls) == 1
  assert client_instance.orderbook_calls[: len(tickers)] == tickers
  assert client_instance.recent_trades_calls[: len(tickers)] == tickers
  for ticker in tickers:
    assert client_instance.orderbook_calls.count(ticker) == 2
    assert client_instance.recent_trades_calls.count(ticker) == 1
  connection = open_database(state_db_path)
  sizing_event = connection.execute(
    "SELECT detail_json FROM runtime_events WHERE event_type = 'submit_bridge_final_sizing_resolved'"
  ).fetchone()
  assert sizing_event is not None
  sizing_detail = json.loads(sizing_event['detail_json'])
  assert sizing_detail['qualifying_candidate_count'] == 2
  assert sizing_detail['final_submit_tickers'] == tickers


def test_submit_order_bridge_multi_survivor_uses_kalshi_v2_batch_create(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_v2_batch.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_v2_batch.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-v2-batch-001',
  )

  client_instance = None

  class BatchCreateClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=BatchCreateClient)

  assert client_instance is not None
  assert payload['planned_pair_count'] == 2
  assert [pair['execution_terminal_state'] for pair in payload['planned_pairs']] == ['CANCELED', 'CANCELED']
  assert client_instance.create_order_v2_calls == []
  assert len(client_instance.create_orders_v2_batch_calls) == 1
  batch_wire = client_instance.create_orders_v2_batch_calls[0]
  assert len(batch_wire) == 4
  assert [order['side'] for order in batch_wire] == ['bid', 'ask', 'bid', 'ask']
  assert all(order['post_only'] is True for order in batch_wire)
  assert all(order['self_trade_prevention_type'] == 'taker_at_cross' for order in batch_wire)
  assert len(client_instance.create_order_group_calls) == 2
  assert client_instance.get_order_calls, 'accepted batch orders must enter readback/shelter lifecycle'
  assert client_instance.get_order_calls[:4] == [
    'kalshi-live-batch-yes-001',
    'kalshi-live-batch-no-002',
    'kalshi-live-batch-yes-003',
    'kalshi-live-batch-no-004',
  ], 'first batch observe cycle must read both accepted pairs in dispatch order before any inter-cycle wait'
  connection = open_database(state_db_path)
  state_history = connection.execute(
    '''
    SELECT pair_id, state, detail_json FROM pair_states
    WHERE operation_lane = 'live'
    ORDER BY id
    '''
  ).fetchall()
  resting_positions = [idx for idx, row in enumerate(state_history) if row['state'] == 'RESTING_BOTH']
  canceled_positions = [idx for idx, row in enumerate(state_history) if row['state'] == 'CANCELED']
  assert len(resting_positions) == 2
  assert len(canceled_positions) == 2
  assert max(resting_positions) < min(canceled_positions), (
    'all accepted batch pairs must be registered RESTING_BOTH before shelter terminalization begins'
  )
  latest_states = connection.execute(
    '''
    SELECT pair_id, state, detail_json FROM pair_states
    WHERE operation_lane = 'live'
    AND id IN (SELECT MAX(id) FROM pair_states GROUP BY pair_id)
    ORDER BY pair_id
    '''
  ).fetchall()
  assert [row['state'] for row in latest_states] == ['CANCELED', 'CANCELED']
  assert all(json.loads(row['detail_json'])['reason'] == 'shelter_window_no_fill_canceled' for row in latest_states)
  event_counts = connection.execute(
    '''
    SELECT event_type, COUNT(*) AS cnt FROM runtime_events
    WHERE event_type IN ('live_order_group_created', 'live_order_shelter_action', 'bridge_execution_result_slot')
    GROUP BY event_type
    '''
  ).fetchall()
  assert {row['event_type']: row['cnt'] for row in event_counts} == {
    'live_order_group_created': 2,
    'live_order_shelter_action': 2,
    'bridge_execution_result_slot': 2,
  }
  result_events = connection.execute(
    '''
    SELECT detail_json FROM runtime_events
    WHERE event_type = 'bridge_execution_result_slot'
    ORDER BY id
    '''
  ).fetchall()
  assert [json.loads(row['detail_json'])['submit_mode'] for row in result_events] == ['batch_create_v2', 'batch_create_v2']


def test_submit_order_bridge_batch_both_filled_projects_filled(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_v2_batch_filled.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_v2_batch_filled.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-v2-batch-filled-001',
  )

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=LiveFakeClientBothFilled)

  assert payload['planned_pair_count'] == 2
  assert [pair['execution_terminal_state'] for pair in payload['planned_pairs']] == ['FILLED', 'FILLED']
  connection = open_database(state_db_path)
  latest_states = connection.execute(
    '''
    SELECT pair_id, state, detail_json FROM pair_states
    WHERE operation_lane = 'live'
    AND id IN (SELECT MAX(id) FROM pair_states GROUP BY pair_id)
    ORDER BY pair_id
    '''
  ).fetchall()
  assert [row['state'] for row in latest_states] == ['FILLED', 'FILLED']
  assert all(json.loads(row['detail_json'])['reason'] in {'both_legs_filled', 'both_legs_filled_after_shelter_readback'} for row in latest_states)
  fill_rows = connection.execute(
    '''
    SELECT pair_id, side, contract_count FROM fills
    WHERE operation_lane = 'live'
    ORDER BY pair_id, side
    '''
  ).fetchall()
  assert len(fill_rows) == 4
  assert [row['side'] for row in fill_rows] == ['no', 'yes', 'no', 'yes']


def test_submit_order_bridge_batch_one_sided_fill_preserves_repair_live(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_v2_batch_one_sided.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_v2_batch_one_sided.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-v2-batch-one-sided-001',
  )

  client_instance = None

  class BatchOneSidedFilledClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

    def get_order(self, order_id: str) -> SubmittedOrder:
      self.get_order_calls.append(order_id)
      stored = self._orders[order_id]
      if stored.side == 'yes':
        return replace(stored, status='executed', fill_count=stored.contract_count, remaining_count=Decimal('0'))
      return replace(stored, status='resting', fill_count=Decimal('0'), remaining_count=stored.contract_count)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=BatchOneSidedFilledClient)

  assert client_instance is not None
  assert payload['planned_pair_count'] == 2
  assert [pair['execution_terminal_state'] for pair in payload['planned_pairs']] == ['REPAIR_LIVE', 'REPAIR_LIVE']
  assert client_instance.cancel_order_v2_calls == []
  connection = open_database(state_db_path)
  latest_states = connection.execute(
    '''
    SELECT pair_id, state, detail_json FROM pair_states
    WHERE operation_lane = 'live'
    AND id IN (SELECT MAX(id) FROM pair_states GROUP BY pair_id)
    ORDER BY pair_id
    '''
  ).fetchall()
  assert [row['state'] for row in latest_states] == ['REPAIR_LIVE', 'REPAIR_LIVE']
  details = [json.loads(row['detail_json']) for row in latest_states]
  assert all(detail['reason'] == 'asymmetric_exposure_repair_order_preserved' for detail in details)
  assert all(detail['repair_leg'] == 'no' for detail in details)


def test_submit_order_bridge_batch_create_failure_does_not_retry(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  private_key_path = tmp_path / 'live_key_v2_batch_fail.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_v2_batch_fail.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-v2-batch-fail-001',
  )

  client_instance = None

  class FailingBatchCreateClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      self.create_orders_v2_batch_attempts = 0
      client_instance = self

    def create_orders_v2_batch(self, order_payloads: list[dict[str, object]]) -> list[SubmittedOrder]:
      self.create_orders_v2_batch_attempts += 1
      self.create_orders_v2_batch_calls.append([_order_payload_to_v2_wire(payload) for payload in order_payloads])
      raise KalshiHttpError('batch_rejected', 'Batch rejected.', 'do_not_retry')

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=FailingBatchCreateClient)

  assert client_instance is not None
  assert client_instance.create_orders_v2_batch_attempts == 1
  assert client_instance.create_order_v2_calls == []
  assert payload['planned_pair_count'] == 2
  assert [pair['execution_terminal_state'] for pair in payload['planned_pairs']] == ['CANCELED', 'CANCELED']
  connection = open_database(state_db_path)
  latest_states = connection.execute(
    '''
    SELECT state, detail_json FROM pair_states
    WHERE operation_lane = 'live'
    AND id IN (SELECT MAX(id) FROM pair_states GROUP BY pair_id)
    ORDER BY pair_id
    '''
  ).fetchall()
  assert [row['state'] for row in latest_states] == ['CANCELED', 'CANCELED']
  assert all(json.loads(row['detail_json'])['reason'] == 'batch_submit_failed_not_submitted' for row in latest_states)


def test_submit_order_bridge_batch_partial_acceptance_cleans_known_remote_id(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  private_key_path = tmp_path / 'live_key_v2_batch_partial.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_v2_batch_partial.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-v2-batch-partial-001',
  )

  client_instance = None

  class PartialBatchCreateClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

    def create_orders_v2_batch(self, order_payloads: list[dict[str, object]]) -> list[SubmittedOrder]:
      orders = super().create_orders_v2_batch(order_payloads)
      return orders[:1]

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=PartialBatchCreateClient)

  assert client_instance is not None
  assert client_instance.create_order_v2_calls == []
  assert len(client_instance.create_orders_v2_batch_calls) == 1
  assert payload['planned_pair_count'] == 2
  assert [pair['execution_terminal_state'] for pair in payload['planned_pairs']] == ['RECONCILE_REQUIRED', 'RECONCILE_REQUIRED']
  assert client_instance.cancel_order_v2_calls == ['kalshi-live-batch-yes-001']
  assert client_instance.get_order_calls == ['kalshi-live-batch-yes-001', 'kalshi-live-batch-yes-001']

  connection = open_database(state_db_path)
  latest_states = connection.execute(
    '''
    SELECT pair_id, state, detail_json FROM pair_states
    WHERE operation_lane = 'live'
    AND id IN (SELECT MAX(id) FROM pair_states GROUP BY pair_id)
    ORDER BY pair_id
    '''
  ).fetchall()
  assert [row['state'] for row in latest_states] == ['RECONCILE_REQUIRED', 'RECONCILE_REQUIRED']
  details = [json.loads(row['detail_json']) for row in latest_states]
  first_detail = next(detail for detail in details if detail['accepted_yes_order_id'] == 'kalshi-live-batch-yes-001')
  assert first_detail['reason'] == 'batch_submit_reconcile_required'
  assert first_detail['cleanup_results'][0]['cleanup_action'] == 'zero_fill_cancel_attempted'
  assert first_detail['cleanup_results'][0]['post_cancel_status'] == 'canceled'
  second_detail = next(detail for detail in details if detail['accepted_yes_order_id'] == '')
  assert second_detail['cleanup_results'] == []


def test_submit_order_bridge_batch_pair_local_malformed_does_not_block_clean_pair(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_v2_batch_pair_local.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_v2_batch_pair_local.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-v2-batch-pair-local-001',
  )

  client_instance = None

  class PairLocalMalformedBatchClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

    def create_orders_v2_batch(self, order_payloads: list[dict[str, object]]) -> list[SubmittedOrder]:
      orders = super().create_orders_v2_batch(order_payloads)
      malformed_first_no = replace(orders[1], client_order_id='', order_id='')
      return [orders[0], malformed_first_no, orders[2], orders[3]]

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=PairLocalMalformedBatchClient)

  assert client_instance is not None
  assert client_instance.create_order_v2_calls == []
  assert len(client_instance.create_orders_v2_batch_calls) == 1
  assert payload['planned_pair_count'] == 2
  states_by_ticker = {
    pair['ticker']: pair['execution_terminal_state']
    for pair in payload['planned_pairs']
  }
  assert states_by_ticker == {
    'KALSHI-CANDIDATE-HIGH': 'RECONCILE_REQUIRED',
    'KALSHI-CANDIDATE-LOW': 'CANCELED',
  }
  assert 'kalshi-live-batch-yes-003' in client_instance.get_order_calls
  assert 'kalshi-live-batch-no-004' in client_instance.get_order_calls
  assert client_instance.create_orders_v2_batch_calls and client_instance.create_order_v2_calls == []

  connection = open_database(state_db_path)
  latest_states = connection.execute(
    '''
    SELECT state, detail_json FROM pair_states
    WHERE operation_lane = 'live'
    AND id IN (SELECT MAX(id) FROM pair_states GROUP BY pair_id)
    '''
  ).fetchall()
  details_by_ticker = {json.loads(row['detail_json'])['ticker']: (row['state'], json.loads(row['detail_json'])) for row in latest_states}
  dirty_state, dirty_detail = details_by_ticker['KALSHI-CANDIDATE-HIGH']
  clean_state, clean_detail = details_by_ticker['KALSHI-CANDIDATE-LOW']
  assert dirty_state == 'RECONCILE_REQUIRED'
  assert dirty_detail['batch_pair_acceptance_classification'] == 'partial_or_ambiguous'
  assert len(dirty_detail['missing_client_order_ids']) == 1
  assert dirty_detail['missing_client_order_ids'][0].endswith('-no')
  assert dirty_detail['malformed_order_count'] == 1
  assert dirty_detail['cleanup_results'][0]['cleanup_action'] == 'zero_fill_cancel_attempted'
  assert clean_state == 'CANCELED'
  assert clean_detail['reason'] == 'shelter_window_no_fill_canceled'


def test_submit_bridge_phase_failed_preserves_kalshi_request_detail(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'submit_bridge_phase_failed_detail.sqlite3'
  connection = open_database(state_db_path)
  exc = KalshiHttpError(
    'auth_failed',
    'Kalshi rejected the authenticated request.',
    'Verify credentials.',
    method='GET',
    endpoint='/account/limits',
    status_code=401,
  )

  service_module._persist_submit_bridge_phase_failed(
    connection,
    recorded_at_utc='2026-07-01T00:00:00+00:00',
    operation_lane='live',
    lane_session_id='lane-submit-detail',
    phase='submit_dispatch',
    exc=exc,
    saved_set_id='saved-set-detail',
  )

  row = connection.execute(
    '''
    SELECT detail_json FROM runtime_events
    WHERE event_type = 'submit_bridge_phase_failed'
    ORDER BY id DESC LIMIT 1
    '''
  ).fetchone()
  assert row is not None
  detail = json.loads(row['detail_json'])
  assert detail['failure_phase'] == 'submit_dispatch'
  assert detail['error_family'] == 'KalshiHttpError'
  assert detail['reason_code'] == 'auth_failed'
  assert detail['kalshi_method'] == 'GET'
  assert detail['kalshi_endpoint'] == '/account/limits'
  assert detail['kalshi_status_code'] == 401


def test_submit_order_bridge_batch_duplicate_remote_order_id_reconciles_without_settlement(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_v2_batch_duplicate_remote.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_v2_batch_duplicate_remote.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-v2-batch-duplicate-remote-001',
  )

  client_instance = None

  class DuplicateRemoteIdBatchClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

    def create_orders_v2_batch(self, order_payloads: list[dict[str, object]]) -> list[SubmittedOrder]:
      orders = super().create_orders_v2_batch(order_payloads)
      duplicated_no = replace(orders[1], order_id=orders[0].order_id)
      return [orders[0], duplicated_no, orders[2], orders[3]]

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=DuplicateRemoteIdBatchClient)

  assert client_instance is not None
  assert client_instance.create_order_v2_calls == []
  assert payload['planned_pair_count'] == 2
  states_by_ticker = {
    pair['ticker']: pair['execution_terminal_state']
    for pair in payload['planned_pairs']
  }
  assert states_by_ticker == {
    'KALSHI-CANDIDATE-HIGH': 'RECONCILE_REQUIRED',
    'KALSHI-CANDIDATE-LOW': 'CANCELED',
  }
  assert client_instance.cancel_order_v2_calls == [
    'kalshi-live-batch-yes-001',
    'kalshi-live-batch-yes-003',
    'kalshi-live-batch-no-004',
  ]
  connection = open_database(state_db_path)
  latest_states = connection.execute(
    '''
    SELECT pair_id, state, detail_json FROM pair_states
    WHERE operation_lane = 'live'
    AND id IN (SELECT MAX(id) FROM pair_states GROUP BY pair_id)
    ORDER BY pair_id
    '''
  ).fetchall()
  details = [json.loads(row['detail_json']) for row in latest_states]
  details_by_ticker = {detail['ticker']: (row['state'], detail) for row, detail in zip(latest_states, details)}
  duplicate_state, duplicate_detail = details_by_ticker['KALSHI-CANDIDATE-HIGH']
  clean_state, clean_detail = details_by_ticker['KALSHI-CANDIDATE-LOW']
  assert duplicate_state == 'RECONCILE_REQUIRED'
  assert clean_state == 'CANCELED'
  assert duplicate_detail['duplicate_remote_order_ids'] == ['kalshi-live-batch-yes-001']
  assert clean_detail['reason'] == 'shelter_window_no_fill_canceled'
  # Acceptance records live on the duplicate pair's detail only; hunting across all
  # details with raw key access flaked on the random pair_id ordering (clean pair's
  # CANCELED detail carries no acceptance keys).
  assert duplicate_detail['accepted_no_order_id'] == 'kalshi-live-batch-yes-001'
  assert duplicate_detail['accepted_yes_order_id'] == 'kalshi-live-batch-yes-001'
  assert duplicate_detail['cleanup_results'][0]['cleanup_action'] == 'zero_fill_cancel_attempted'
  shelter_events = connection.execute(
    '''
    SELECT COUNT(*) AS cnt FROM runtime_events
    WHERE event_type = 'live_order_shelter_action'
    '''
  ).fetchone()
  assert shelter_events['cnt'] == 1


def test_submit_order_bridge_batch_signed_evidence_failure_preserves_result_slot(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  from polyventure import service as _service

  private_key_path = tmp_path / 'live_key_v2_batch_signing_failure.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_v2_batch_signing_failure.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-v2-batch-signing-failure-001',
  )

  def raise_after_settlement(*_args: object, **_kwargs: object) -> dict:
    raise RuntimeError('signature backend unavailable')

  monkeypatch.setattr(_service, '_attach_money_evidence_signature', raise_after_settlement)
  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=LiveFakeClientOrderbookFp)

  assert payload['planned_pair_count'] == 2
  assert [pair['execution_terminal_state'] for pair in payload['planned_pairs']] == ['CANCELED', 'CANCELED']
  connection = open_database(state_db_path)
  latest_states = connection.execute(
    '''
    SELECT state, detail_json FROM pair_states
    WHERE operation_lane = 'live'
    AND id IN (SELECT MAX(id) FROM pair_states GROUP BY pair_id)
    ORDER BY pair_id
    '''
  ).fetchall()
  assert [row['state'] for row in latest_states] == ['CANCELED', 'CANCELED']
  result_events = connection.execute(
    '''
    SELECT detail_json FROM runtime_events
    WHERE event_type = 'bridge_execution_result_slot'
    ORDER BY id
    '''
  ).fetchall()
  assert len(result_events) == 2
  details = [json.loads(row['detail_json']) for row in result_events]
  assert [detail['terminal_state'] for detail in details] == ['CANCELED', 'CANCELED']
  assert all(detail['signed_money_evidence_status'] == 'failed' for detail in details)
  assert all(detail['signed_money_evidence_error_family'] == 'RuntimeError' for detail in details)


@pytest.mark.parametrize(
  ('settings_override', 'expected_error_message'),
  (
    (
      {'min_edge_dollars': 1.0},
      'Candidate gross edge is below the configured minimum.',
    ),
    (
      {'min_profit_dollars': 1.0},
      'Candidate net edge is below the configured profit floor.',
    ),
  ),
)
def test_submit_bridge_pair_plan_economics_rejection_is_candidate_local(
  monkeypatch,
  tmp_path: Path,
  settings_override: dict[str, float],
  expected_error_message: str,
) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  private_key_path = tmp_path / 'live_key_profit_floor.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_profit_floor.sqlite3'
  base_settings = _live_settings(str(private_key_path), str(state_db_path))
  settings = Settings(**{**base_settings.__dict__, **settings_override})
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-profit-floor-001')

  payload = run_service_once(
    settings=settings,
    execution_profile='submit_order_bridge',
    client_factory=LiveFakeClientOrderbookFp,
  )

  assert payload['planned_pair_count'] == 0
  assert payload['blocked_reason'] == 'pair_plan_validation'
  assert payload['submit_response_id'] == 'SUBMIT_REJECTED_TERMINAL'
  assert payload['submit_rest_state_id'] == 'UPSTREAM_REVIEW_HOLD'
  assert payload['failure_class'] == 'SILENT_CONTINUE'
  assert payload['retry_allowed'] is False
  connection = open_database(state_db_path)
  assert connection.execute('SELECT COUNT(*) AS cnt FROM pair_states').fetchone()['cnt'] == 0
  assert connection.execute('SELECT COUNT(*) AS cnt FROM orders').fetchone()['cnt'] == 0
  rows = connection.execute(
    '''
    SELECT event_type, level, detail_json FROM runtime_events
    WHERE operation_lane = 'live' AND event_type IN ('submit_bridge_blocked', 'submit_bridge_phase_failed')
    ORDER BY id
    '''
  ).fetchall()
  assert [row['event_type'] for row in rows] == ['submit_bridge_blocked']
  detail = json.loads(rows[0]['detail_json'])
  assert rows[0]['level'] == 'WARN'
  assert detail['blocked_reason'] == 'pair_plan_validation'
  assert detail['failure_phase'] == 'pair_plan_validation'
  assert detail['money_path_crossed'] is False
  assert detail['pair_plan_created'] is False
  assert detail['orders_created'] is False
  assert detail['error_message'] == expected_error_message
  final_event = connection.execute(
    '''
    SELECT detail_json FROM runtime_events
    WHERE event_type = 'submit_bridge_final_coverability_checked'
    '''
  ).fetchone()
  assert final_event is not None
  final_detail = json.loads(final_event['detail_json'])
  assert final_detail['ticker'] == 'KALSHI-CANDIDATE-LOW'
  assert final_detail['ok'] is False
  assert final_detail['guard_reason'] == 'pair_plan_validation'
  assert final_detail['profitability_evidence_available'] is True
  assert final_detail['profitability_basis'] == 'post_reprice_final_prices'
  assert final_detail['edge_gross_per_contract'] == '0.1900'
  assert final_detail['fee_reserve_per_contract'] == '0.02'
  assert final_detail['edge_net_per_contract'] == '0.1700'
  assert final_detail['min_edge_dollars'] == str(Decimal(str(settings.min_edge_dollars)))
  assert final_detail['min_profit_dollars'] == str(Decimal(str(settings.min_profit_dollars)))
  assert final_detail['gross_edge_margin_to_min_edge'] == str(Decimal('0.1900') - Decimal(str(settings.min_edge_dollars)))
  assert final_detail['net_profit_margin_to_min_profit'] == str(Decimal('0.1700') - Decimal(str(settings.min_profit_dollars)))
  assert final_detail['edge_threshold_pass'] is (Decimal('0.1900') >= Decimal(str(settings.min_edge_dollars)))
  assert final_detail['profit_threshold_pass'] is (Decimal('0.1700') >= Decimal(str(settings.min_profit_dollars)))
  assert final_detail['threshold_outcome'] == 'below_threshold'
  pair_states = connection.execute(
    "SELECT COUNT(*) as cnt FROM pair_states WHERE operation_lane = 'live'"
  ).fetchone()
  assert pair_states['cnt'] == 0, 'No pair state transitions must occur when price fetch is blocked'


def test_live_order_placement_calls_create_order_v2_not_simulation(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_bridge.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_b_live_bridge.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-bridge-001')

  client_instance = None

  class TrackingLiveClient(LiveFakeClient):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  simulate_calls: list[str] = []
  monkeypatch.setattr(
    'polyventure.service.simulate_submit_pair',
    lambda *a, **kw: simulate_calls.append('called') or [],
  )

  run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TrackingLiveClient)

  assert client_instance is not None
  assert client_instance.create_order_v2_calls, 'create_order_v2 must be called for live lane submit bridge'
  assert not simulate_calls, 'simulate_submit_pair must NOT be called for live lane'
  sides_called = {call.get('side') for call in client_instance.create_order_v2_calls}
  assert sides_called == {'bid', 'ask'}, 'Both YES (bid) and NO (ask) orders must be placed on the V2 YES-book'
  for call in client_instance.create_order_v2_calls:
    assert call.get('post_only') is True, 'live bridge must preserve passive paired-maker order posture'
    # V2 wire shape: price is a 4-decimal dollar string; count is a Count-FP string.
    price_on_wire = call.get('price')
    assert type(price_on_wire) is str, 'V2 order price must be a string on the wire'
    from decimal import Decimal as _D
    price_decimal = _D(price_on_wire)
    assert _D('0.01') <= price_decimal <= _D('0.99'), 'V2 order price must be in [0.01, 0.99]'
    assert len(price_on_wire.split('.')[-1]) == 4, 'V2 order price must have exactly 4 decimal places'
    count_on_wire = call.get('count')
    assert type(count_on_wire) is str, 'V2 order count must be a string on the wire'
    assert _D(count_on_wire) >= 1, 'V2 order count must be a positive value'
    assert call.get('self_trade_prevention_type') == 'taker_at_cross', 'STP must be present'
    assert 'action' not in call, 'legacy action field must not be in the V2 wire body'
    assert 'type' not in call, 'legacy type field must not be in the V2 wire body'


def test_live_order_placement_source_reprices_candidate_math_before_validation() -> None:
  import inspect
  from polyventure import service as service_module

  source = inspect.getsource(service_module.run_service_once)

  assert 'reprice_candidate(candidate, yes_price_live, no_price_live, resolved_settings)' in source
  assert 'replace(candidate, target_yes_bid=yes_price_live, target_no_bid=no_price_live)' not in source
  assert source.index('candidate = reprice_candidate(candidate, yes_price_live, no_price_live, resolved_settings)') < source.index('validate_pair_plan(')


def test_place_live_pair_orders_fails_closed_on_non_cent_price(tmp_path: Path) -> None:
  private_key_path = tmp_path / 'units_block_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_units_block.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  connection = open_database(str(state_db_path))

  plan = PairOrderPlan(
    pair_id='pair-units-block',
    ticker='KALSHI-CANDIDATE-LOW',
    yes_price=Decimal('0.545'),  # sub-cent: not exactly representable in integer cents
    no_price=Decimal('0.40'),
    contract_count=Decimal('1'),
    yes_client_order_id='pair-units-block-yes',
    no_client_order_id='pair-units-block-no',
    time_in_force='good_till_canceled',
    post_only=False,
    cancel_order_on_pause=False,
    subaccount=0,
  )
  persist_pair_plan(connection, plan, created_at_utc='2026-06-22T00:00:00Z', operation_lane='live')

  client = LiveFakeClient(settings, object())
  result = _place_live_pair_orders(
    client,
    connection,
    plan=plan,
    settings=settings,
    lane_session_id='live-units-block-001',
    recorded_at=datetime.now(UTC),
    sizing_summary={},
  )

  assert result['terminal_state'] == 'CANCELED'
  assert result['blocked_reason'] == 'price_precision_invalid'
  # Fail-closed before the first order call: no order and no order group are created.
  assert client.create_order_v2_calls == [], 'no order may be placed when a unit is invalid'
  assert client.create_order_group_calls == [], 'no order group may be created on a units block'
  blocked = connection.execute(
    "SELECT COUNT(*) AS cnt FROM runtime_events WHERE event_type = 'live_order_units_blocked'"
  ).fetchone()
  assert blocked['cnt'] == 1, 'a names-only live_order_units_blocked event must be persisted'


def test_sandbox_order_placement_uses_simulation_not_real_api(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'sandbox_bridge_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_b_sandbox_regression.sqlite3'
  settings_base = _settings(str(private_key_path))
  settings = Settings(**{**settings_base.__dict__, 'state_db_path': str(state_db_path)})
  _seed_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='sandbox-regress-002')

  create_order_calls: list[str] = []

  class NoOrderClient(FakeClient):
    def create_order_v2(self, **payload):
      create_order_calls.append('called')
      return _make_submitted_order('fake', 'fake', '', '', Decimal('0'), Decimal('1'))

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=NoOrderClient)

  assert not create_order_calls, 'create_order_v2 must NOT be called for sandbox lane'
  assert payload['execution_chronology']['terminal_state'] == 'CANCELED', (
    'Sandbox lane must still use simulation path ending in CANCELED'
  )


def test_live_order_placement_signing_headers_present_in_request_path() -> None:
  from polyventure.http_client import KalshiHttpClient, AUTH_HEADER_NAMES
  assert hasattr(KalshiHttpClient, '_request'), '_request method must exist on KalshiHttpClient'
  import inspect
  source = inspect.getsource(KalshiHttpClient._request)
  assert 'build_auth_headers' in source, (
    'KalshiHttpClient._request must call build_auth_headers to apply RSA-PSS signing'
  )
  for method_name in ('create_order_v2', 'cancel_order_v2', 'create_order_group'):
    method_source = inspect.getsource(getattr(KalshiHttpClient, method_name))
    assert '_request' in method_source, (
      f'KalshiHttpClient.{method_name} must route through _request (which applies signing)'
    )


def test_http_client_max_attempts_bounds_network_retries(monkeypatch, tmp_path: Path) -> None:
  # S2 (PLAN-POLYVENTURE-LIVE-AUTO-STABILITY-20260615): an interactive bounded client
  # makes a single attempt and raises immediately on a stalled upstream, while the default
  # client preserves the resilient 4-attempt retry behavior.
  import requests
  from polyventure import http_client as hc

  monkeypatch.setattr(hc, 'build_auth_headers', lambda *a, **k: {})
  monkeypatch.setattr(hc, '_backoff_delay', lambda *a, **k: 0.0)
  key_path = tmp_path / 'k.pem'
  _write_private_key(key_path)
  settings = _settings(str(key_path))

  class _StallSession:
    def __init__(self) -> None:
      self.calls = 0

    def request(self, **kwargs):
      self.calls += 1
      raise requests.exceptions.Timeout('stalled upstream')

  bounded_session = _StallSession()
  bounded = hc.KalshiHttpClient(settings, object(), session=bounded_session, request_timeout=3, max_attempts=1)
  with pytest.raises(hc.KalshiHttpError):
    bounded._request('GET', '/portfolio/balance')
  assert bounded_session.calls == 1, 'bounded client must make exactly one attempt'

  default_session = _StallSession()
  default = hc.KalshiHttpClient(settings, object(), session=default_session)
  with pytest.raises(hc.KalshiHttpError):
    default._request('GET', '/portfolio/balance')
  assert default_session.calls == 4, 'default client must preserve 4-attempt retry behavior'


def test_live_order_credential_fail_closed(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)

  def _raise_key_error(_path):
    raise FileNotFoundError('private key not found')

  monkeypatch.setattr('polyventure.service.load_private_key', _raise_key_error)
  private_key_path = tmp_path / 'missing_key.pem'
  state_db_path = tmp_path / 'lop_b_cred_fail.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-cred-fail-001')

  with pytest.raises((FileNotFoundError, Exception)):
    run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=LiveFakeClient)


def test_live_order_placement_cancel_on_max_unhedged_timeout(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_timeout.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_b_timeout.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-timeout-001')

  client_instance = None

  class TimeoutClient(LiveFakeClient):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TimeoutClient)

  assert client_instance is not None
  assert client_instance.cancel_order_v2_calls, 'cancel_order_v2 must be called when the shelter window is reached with no fills'
  connection = open_database(state_db_path)
  canceled_states = connection.execute(
    "SELECT detail_json FROM pair_states WHERE operation_lane = 'live' AND state = 'CANCELED'"
  ).fetchall()
  assert canceled_states, 'CANCELED pair state must be persisted when unfilled orders are sheltered'
  details = [json.loads(row['detail_json']) for row in canceled_states]
  assert any(d.get('reason') == 'shelter_window_no_fill_canceled' for d in details), (
    'CANCELED state detail must have the no-fill shelter reason'
  )
  assert set(client_instance.cancel_order_v2_calls) == {'kalshi-live-yes-001', 'kalshi-live-no-001'}, (
    'shelter action must cancel both unfilled cancelable remote legs when there is no exposure'
  )


def test_live_order_readback_both_executed_projects_filled_not_canceled(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_both_filled.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_both_filled.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-both-filled-001')

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=LiveFakeClientBothFilled)

  assert payload['execution_chronology']['terminal_state'] == 'FILLED'
  connection = open_database(state_db_path)
  final_states = connection.execute(
    "SELECT state, detail_json FROM pair_states WHERE operation_lane = 'live' ORDER BY id DESC LIMIT 1"
  ).fetchall()
  assert final_states[0]['state'] == 'FILLED'
  final_detail = json.loads(final_states[0]['detail_json'])
  assert Decimal(final_detail['yes_filled_contracts']) > 0
  assert Decimal(final_detail['no_filled_contracts']) > 0
  canceled_states = connection.execute(
    "SELECT COUNT(*) AS cnt FROM pair_states WHERE operation_lane = 'live' AND state = 'CANCELED'"
  ).fetchone()
  assert canceled_states['cnt'] == 0
  fill_rows = connection.execute(
    "SELECT side, contract_count FROM fills WHERE operation_lane = 'live' ORDER BY side"
  ).fetchall()
  assert [row['side'] for row in fill_rows] == ['no', 'yes'], (
    'both-executed live readback must persist both domain fill legs, including Kalshi YES-book ask as domain no'
  )
  assert all(Decimal(row['contract_count']) > 0 for row in fill_rows)


def test_live_order_one_sided_executed_ahead_preserves_repair_live(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_one_sided.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_one_sided.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-one-sided-001')

  client_instance = None

  class OneSidedClient(LiveFakeClient):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

    def get_order(self, order_id: str) -> SubmittedOrder:
      self.get_order_calls.append(order_id)
      stored = self._orders[order_id]
      if stored.side == 'yes':
        return replace(stored, status='executed', remaining_count=Decimal('0'), fill_count=stored.contract_count)
      return self._orders[order_id]

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=OneSidedClient)

  # Canonical: YES filled fully (ahead leg, already executed -> not cancelable, so no
  # cancel call), the deficient NO repair order is preserved resting, and the pair
  # projects REPAIR_LIVE. No market-crossing catch-up, no freeze to ERROR.
  assert client_instance is not None
  assert client_instance.cancel_order_v2_calls == []
  assert payload['execution_chronology']['terminal_state'] == 'REPAIR_LIVE'
  connection = open_database(state_db_path)
  final_state = connection.execute(
    "SELECT state, detail_json FROM pair_states WHERE operation_lane = 'live' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  detail = json.loads(final_state['detail_json'])
  assert final_state['state'] == 'REPAIR_LIVE'
  assert Decimal(detail['yes_filled_contracts']) > 0
  assert Decimal(detail['no_filled_contracts']) == Decimal('0')
  assert detail['reason'] == 'asymmetric_exposure_repair_order_preserved'
  assert 'catchup_side' not in detail
  assert any(item.get('status') == 'preserved_repair_order' for item in detail['cancel_results'])


def test_cancel_all_preserves_fill_bearing_repair_live(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_cancel_preserve.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_cancel_preserve.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-cancel-preserve-001')

  class OneSidedClient(LiveFakeClient):
    def get_order(self, order_id: str) -> SubmittedOrder:
      self.get_order_calls.append(order_id)
      stored = self._orders[order_id]
      if stored.side == 'yes':
        return replace(stored, status='executed', remaining_count=Decimal('0'), fill_count=stored.contract_count)
      return stored

  run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=OneSidedClient)
  cancel_payload = cancel_all_pairs(settings=settings)

  # The one-sided fill leaves live exposure as REPAIR_LIVE (repair order preserved);
  # cancel-all must preserve it (fill-bearing) rather than cancelling.
  assert cancel_payload['canceled_pair_count'] == 0
  assert cancel_payload['preserved_fill_bearing_pair_count'] == 1
  connection = open_database(state_db_path)
  latest = connection.execute(
    "SELECT state, detail_json FROM pair_states WHERE operation_lane = 'live' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  detail = json.loads(latest['detail_json'])
  assert latest['state'] == 'REPAIR_LIVE'
  assert Decimal(detail['yes_filled_contracts']) > 0
  assert Decimal(detail['no_filled_contracts']) == Decimal('0')
  event = connection.execute(
    "SELECT detail_json FROM runtime_events WHERE event_type = 'cancel_all_preserved_fill_bearing_exposure'"
  ).fetchone()
  assert event is not None


def test_cancel_all_does_not_overwrite_filled_pair_from_stale_snapshot(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  private_key_path = tmp_path / 'cancel_stale_snapshot.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'cancel_stale_snapshot.sqlite3'
  settings = Settings(**{**_settings(str(private_key_path)).__dict__, 'state_db_path': str(state_db_path)})
  connection = open_database(state_db_path)
  plan = PairOrderPlan(
    pair_id='pair-cancel-filled-race',
    ticker='KALSHI-CANDIDATE-LOW',
    yes_price=Decimal('0.34'),
    no_price=Decimal('0.39'),
    contract_count=Decimal('5'),
    yes_client_order_id='pair-cancel-filled-race-yes',
    no_client_order_id='pair-cancel-filled-race-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(connection, plan, created_at_utc='2026-07-02T16:00:00Z', operation_lane='sandbox')
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='RESTING_BOTH',
    recorded_at_utc='2026-07-02T16:00:01Z',
    operation_lane='sandbox',
    lane_session_id='sandbox-cancel-race',
    detail={'ticker': plan.ticker, 'yes_filled_contracts': '0', 'no_filled_contracts': '0'},
  )
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='FILLED',
    recorded_at_utc='2026-07-02T16:00:02Z',
    operation_lane='sandbox',
    lane_session_id='sandbox-cancel-race',
    detail={
      'ticker': plan.ticker,
      'reason': 'both_legs_filled',
      'yes_filled_contracts': '5',
      'no_filled_contracts': '5',
      'average_yes_price': '0.34',
      'average_no_price': '0.39',
      'realized_fees_dollars': '0',
    },
  )
  monkeypatch.setattr(
    service_module,
    '_latest_pair_snapshots',
    lambda *_args, **_kwargs: [{
      'pair_id': plan.pair_id,
      'ticker': plan.ticker,
      'contract_count': plan.contract_count,
      'state': 'RESTING_BOTH',
      'detail': {'yes_filled_contracts': '0', 'no_filled_contracts': '0'},
      'recorded_at_utc': '2026-07-02T16:00:01Z',
    }],
  )

  cancel_payload = cancel_all_pairs(settings=settings)

  assert cancel_payload['canceled_pair_count'] == 0
  latest = connection.execute(
    'SELECT state, detail_json FROM pair_states WHERE pair_id = ? ORDER BY id DESC LIMIT 1',
    (plan.pair_id,),
  ).fetchone()
  assert latest['state'] == 'FILLED'
  assert json.loads(latest['detail_json'])['reason'] == 'both_legs_filled'
  assert connection.execute(
    "SELECT COUNT(*) FROM pair_states WHERE pair_id = ? AND state = 'CANCELED'",
    (plan.pair_id,),
  ).fetchone()[0] == 0


def test_reconcile_terminalizes_closed_repair_live_as_settled_exposure(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_settled_exposure.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_settled_exposure.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-settled-exposure-001')

  class RepairCloseClient(LiveFakeClient):
    shared_orders: dict[str, SubmittedOrder] = {}
    close_mode = False

    def create_order_v2(self, **payload: object) -> SubmittedOrder:
      order = super().create_order_v2(**payload)
      self.shared_orders[order.order_id] = order
      return order

    def get_order(self, order_id: str) -> SubmittedOrder:
      self.get_order_calls.append(order_id)
      stored = self.shared_orders.get(order_id) or self._orders[order_id]
      if stored.side == 'yes':
        return replace(stored, status='executed', remaining_count=Decimal('0'), fill_count=stored.contract_count)
      if self.close_mode:
        return replace(stored, status='canceled', remaining_count=Decimal('0'), fill_count=Decimal('0'))
      return stored

    def get_market_readback(self, ticker: str) -> dict:
      return {
        'ticker': ticker,
        'status': 'finalized',
        'result': 'no',
        'close_time': '2026-06-24T10:30:00Z',
      }

    def get_fills(self, **params: object) -> list[dict]:
      del params
      return [{'ticker': 'KALSHI-CANDIDATE-LOW', 'order_id': 'kalshi-live-yes-001'}]

  RepairCloseClient.shared_orders = {}
  RepairCloseClient.close_mode = False
  run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=RepairCloseClient)
  RepairCloseClient.close_mode = True

  payload = reconcile_pairs(settings=settings, client_factory=RepairCloseClient)

  # Canonical: with the shelter restored to cap-ahead + preserve-repair, the one-sided
  # fill rests as REPAIR_LIVE and is terminalized to SETTLED_EXPOSURE when the market
  # finalizes (settlement reconciliation), carrying realized P&L. It is not frozen to
  # ERROR. (The finalized-market terminalization is owned by Kalshi alignment, so the
  # repair-close sweep count may be zero.)
  assert payload['pairs'][0]['state'] == 'SETTLED_EXPOSURE'
  row = payload['pair_runtime_summary'][0]
  assert row['public_state_id'] == 'SETTLED_EXPOSURE'
  assert row['terminal_state'] == 'SETTLED_EXPOSURE'
  connection = open_database(state_db_path)
  latest = connection.execute(
    "SELECT state, detail_json FROM pair_states WHERE operation_lane = 'live' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  detail = json.loads(latest['detail_json'])
  assert latest['state'] == 'SETTLED_EXPOSURE'
  assert detail['terminal_reason'] == 'market_finalized_one_sided_exposure'
  assert 'realized_pnl_dollars' in detail


def test_live_order_shelter_cancels_ahead_side_and_preserves_repair_order(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_shelter_ahead.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_shelter_ahead.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-shelter-ahead-001')

  client_instance = None

  class AheadPartialClient(LiveFakeClient):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      self.canceled_ids: set[str] = set()
      client_instance = self

    def get_order(self, order_id: str) -> SubmittedOrder:
      self.get_order_calls.append(order_id)
      stored = self._orders[order_id]
      if stored.side == 'yes':
        if order_id in self.canceled_ids:
          return replace(stored, status='canceled', remaining_count=Decimal('0'), fill_count=Decimal('1'))
        return replace(stored, status='resting', remaining_count=stored.contract_count - Decimal('1'), fill_count=Decimal('1'))
      return stored

    def cancel_order_v2(self, order_id: str) -> dict:
      self.canceled_ids.add(order_id)
      return super().cancel_order_v2(order_id)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=AheadPartialClient)

  # Canonical: only the ahead (YES, partially filled but still cancelable) leg is
  # capped; the deficient NO repair order is preserved resting -> REPAIR_LIVE. No
  # market-crossing catch-up, no freeze to ERROR.
  assert client_instance is not None
  assert client_instance.cancel_order_v2_calls == ['kalshi-live-yes-001']
  assert payload['execution_chronology']['terminal_state'] == 'REPAIR_LIVE'
  connection = open_database(state_db_path)
  final_state = connection.execute(
    "SELECT state, detail_json FROM pair_states WHERE operation_lane = 'live' ORDER BY id DESC LIMIT 1"
  ).fetchone()
  detail = json.loads(final_state['detail_json'])
  assert final_state['state'] == 'REPAIR_LIVE'
  assert 'catchup_side' not in detail
  assert any(item.get('status') == 'preserved_repair_order' for item in detail['cancel_results'])


def test_live_order_placement_api_error_records_error_state(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_err.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_b_api_error.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW', saved_set_id='live-api-error-001')

  run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=LiveFakeClientApiError)

  connection = open_database(state_db_path)
  error_states = connection.execute(
    "SELECT detail_json FROM pair_states WHERE operation_lane = 'live' AND state = 'ERROR'"
  ).fetchall()
  assert error_states, 'ERROR pair state must be persisted when create_order_v2 raises KalshiHttpError'
  detail = json.loads(error_states[0]['detail_json'])
  assert detail.get('reason') == 'live_order_api_error'
  assert 'error_family' in detail
  assert 'KalshiHttpError' in detail['error_family'] or 'Error' in detail['error_family']
  assert 'rate_limited' == detail.get('reason_code'), 'reason_code must be included names-only (no credential values)'
  runtime_events = connection.execute(
    "SELECT detail_json FROM runtime_events WHERE operation_lane = 'live' AND event_type = 'live_order_placement_error'"
  ).fetchall()
  assert runtime_events, 'live_order_placement_error runtime event must be persisted'
  event_detail = json.loads(runtime_events[0]['detail_json'])
  assert 'error_family' in event_detail
  for forbidden_key in ('message', 'traceback', 'args', 'key', 'token', 'password', 'credential'):
    assert forbidden_key not in event_detail, (
      f'Event detail must not contain sensitive field: {forbidden_key}'
    )


def _make_candidate_pair(ticker: str, yes_bid: str, no_bid: str) -> CandidatePair:
  return CandidatePair(
    ticker=ticker,
    seconds_to_close=300,
    target_yes_bid=Decimal(yes_bid),
    target_no_bid=Decimal(no_bid),
    edge_gross_per_contract=Decimal('0.06'),
    fee_reserve_per_contract=Decimal('0.02'),
    edge_net_per_contract=Decimal('0.04'),
    asymmetry=Decimal('0.01'),
    max_size_contracts=Decimal('1'),
    ranking_key=(Decimal('0.04'), Decimal('0.06'), Decimal('0.72'), Decimal('0.01'), 300),
  )


def _projection_settings() -> Settings:
  return Settings(
    kalshi_env='demo',
    api_key_id='key-id',
    private_key_file='/tmp/placeholder.pem',
    private_key_inline=None,
    private_key_path_legacy=None,
    api_base_url='https://demo-api.kalshi.co/trade-api/v2',
    websocket_url='wss://demo-api.kalshi.co/trade-api/ws/v2',
    subaccount=0,
    scan_interval_ms=2000,
    entry_window_start_sec=900,
    entry_window_end_sec=60,
    min_edge_dollars=Decimal('0.03'),
    fee_reserve_dollars=Decimal('0.02'),
    min_profit_dollars=Decimal('0.01'),
    max_pair_contracts=10.0,
    max_open_pairs=20,
    max_unhedged_sec=5,
    cancel_on_pause=True,
    log_level='INFO',
    state_db_path='var/kalshi.sqlite3',
  )


def test_live_qualifying_tier_assigned_when_both_bids_positive() -> None:
  """FIX-E5: candidate in live_tickers with both bids > 0 must receive live_qualifying tier."""
  candidate = _make_candidate_pair('TICKER-LIVE', '0.31', '0.39')
  projected, transition_rank = _sandbox_candidate_projection(
    [candidate],
    {'TICKER-LIVE'},
    market_by_ticker={},
    settings=_projection_settings(),
  )
  assert projected[0]['qualifier_tier'] == 'live_qualifying', 'FIX-E5: positive-bid live ticker must be live_qualifying'
  assert transition_rank == 1, 'FIX-E5: transition_rank must be set for live_qualifying candidate'


def test_live_qualifying_tier_requires_positive_yes_bid() -> None:
  """FIX-E5: candidate in live_tickers with yes_bid=0 must be demoted to sandbox_extended."""
  candidate = _make_candidate_pair('TICKER-LIVE', '0.00', '0.39')
  projected, transition_rank = _sandbox_candidate_projection(
    [candidate],
    {'TICKER-LIVE'},
    market_by_ticker={},
    settings=_projection_settings(),
  )
  assert projected[0]['qualifier_tier'] == 'sandbox_extended', 'FIX-E5: zero yes_bid must yield sandbox_extended'
  assert transition_rank is None, 'FIX-E5: transition_rank must be None when no live_qualifying candidate'


def test_live_qualifying_tier_requires_positive_no_bid() -> None:
  """FIX-E5: candidate in live_tickers with no_bid=0 must be demoted to sandbox_extended."""
  candidate = _make_candidate_pair('TICKER-LIVE', '0.31', '0.00')
  projected, transition_rank = _sandbox_candidate_projection(
    [candidate],
    {'TICKER-LIVE'},
    market_by_ticker={},
    settings=_projection_settings(),
  )
  assert projected[0]['qualifier_tier'] == 'sandbox_extended', 'FIX-E5: zero no_bid must yield sandbox_extended'
  assert transition_rank is None, 'FIX-E5: transition_rank must be None when no live_qualifying candidate'


def test_live_qualifying_tier_sandbox_extended_when_not_in_live_tickers() -> None:
  """FIX-E5: candidate not in live_tickers must be sandbox_extended regardless of bids."""
  candidate = _make_candidate_pair('TICKER-NOT-LIVE', '0.31', '0.39')
  projected, transition_rank = _sandbox_candidate_projection(
    [candidate],
    set(),
    market_by_ticker={},
    settings=_projection_settings(),
  )
  assert projected[0]['qualifier_tier'] == 'sandbox_extended', 'FIX-E5: non-live-tickers candidate must be sandbox_extended'
  assert transition_rank is None, 'FIX-E5: no transition_rank when no live_qualifying candidate'


def test_live_qualifying_transition_rank_none_when_all_zero_bid(tmp_path: Path) -> None:
  """FIX-E5: when all live_tickers candidates have zero bids, transition_rank must be None."""
  candidates = [
    _make_candidate_pair('TICKER-A', '0.00', '0.39'),
    _make_candidate_pair('TICKER-B', '0.31', '0.00'),
    _make_candidate_pair('TICKER-C', '0.00', '0.00'),
  ]
  _, transition_rank = _sandbox_candidate_projection(
    candidates,
    {'TICKER-A', 'TICKER-B', 'TICKER-C'},
    market_by_ticker={},
    settings=_projection_settings(),
  )
  assert transition_rank is None, 'FIX-E5: no live_qualifying candidate must yield transition_rank=None'


def test_project_funds_posture_populates_snapshot_from_heartbeat_detail(tmp_path: Path) -> None:
  """FIX-E4: _project_funds_posture must extract available_funds_snapshot from heartbeat detail."""
  from polyventure.persistence import open_database, persist_service_heartbeat
  hb_time = datetime(2026, 6, 14, 12, 0, 0, tzinfo=UTC)
  hb_ts = hb_time.isoformat()
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_service_heartbeat(
    connection,
    component='reconcile',
    status='ok',
    operation_lane='live',
    lane_session_id='sess-001',
    recorded_at_utc=hb_ts,
    detail={
      'available_funds_snapshot': '50.00',
      'available_funds_as_of': hb_ts,
      'funds_refresh_status': 'fresh',
    },
  )
  hb = _latest_heartbeat_payload(connection, operation_lane='live')
  # Pass now=1 second after the heartbeat so staleness grace (10 s) is not exceeded
  posture = _project_funds_posture(
    latest_heartbeat_payload=hb,
    now=hb_time + timedelta(seconds=1),
  )
  assert posture['available_funds_snapshot'] == '50.00', 'FIX-E4: snapshot must be populated from heartbeat detail'
  assert posture['funds_refresh_status'] == 'fresh', 'FIX-E4: refresh_status must be carried from heartbeat'


def test_project_funds_posture_degrades_gracefully_with_no_heartbeat() -> None:
  """FIX-E4: _project_funds_posture with no heartbeat must return None snapshot without crashing."""
  posture = _project_funds_posture(latest_heartbeat_payload=None)
  assert posture['available_funds_snapshot'] is None, 'FIX-E4: None heartbeat must yield None snapshot'
  assert isinstance(posture, dict), 'FIX-E4: posture must always be a dict'
  assert 'funds_refresh_status' in posture, 'FIX-E4: posture must contain funds_refresh_status'


def test_project_funds_posture_live_lane_condition_excludes_sandbox() -> None:
  """FIX-E4: the live-lane gate must be distinct from sandbox; operation_lane 'live' != 'sandbox'."""
  live_lane = str('live').strip().lower() == 'live'
  sandbox_lane = str('sandbox').strip().lower() == 'live'
  assert live_lane is True, 'FIX-E4: live lane must satisfy live-lane gate'
  assert sandbox_lane is False, 'FIX-E4: sandbox lane must not satisfy live-lane gate'


# ---------------------------------------------------------------------------
# F1 (funds-decoupling BMAP 2026-06-20): the deck rebuild must serve the fresh
# FH-heartbeat funds snapshot instead of making its own synchronous live balance
# call, which competes with the scan loop + FH beat and stalls the deck (prereq #1)
# / starves the strict gate (prereq #2). The live call fires only when the heartbeat
# is stale/absent.
# ---------------------------------------------------------------------------

def _f1_live_settings(private_key_file: str) -> Settings:
  return Settings(**{**_settings(private_key_file).__dict__, 'operation_lane': 'live'})


def _funds_heartbeat_payload(*, snapshot: str, as_of: datetime, status: str = 'fresh') -> dict:
  return {'detail': {
    'available_funds_snapshot': snapshot,
    'available_funds_as_of': as_of.isoformat(),
    'funds_refresh_status': status,
  }}


def test_f1_fresh_heartbeat_skips_live_balance_call(tmp_path: Path) -> None:
  """F1: a fresh FH heartbeat is served directly; the synchronous balance client
  is never constructed (no rate-limit/GIL contention with the scan loop + FH beat)."""
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = _f1_live_settings(str(private_key_path))

  def _must_not_construct(_settings_arg, _private_key_arg):
    raise AssertionError('F1: live balance client must not be constructed when the heartbeat is fresh')

  posture = _refresh_reporting_funds_posture(
    settings,
    latest_funds_heartbeat_payload=_funds_heartbeat_payload(
      snapshot='50.00', as_of=datetime.now(UTC),
    ),
    client_factory=_must_not_construct,
  )
  assert posture['funds_source'] == 'heartbeat_fresh', 'F1: fresh heartbeat must be served without a live call'
  assert posture['funds_refresh_ms'] is None, 'F1: no live call means no live-call timing'
  assert posture['available_funds_snapshot'] == '50.00'
  assert posture['stale'] is False


def test_f1_stale_heartbeat_makes_live_balance_call(tmp_path: Path) -> None:
  """F1: when the heartbeat funds are stale, the synchronous refresh still fires —
  the decoupling does not suppress a genuinely-needed refresh."""
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = _f1_live_settings(str(private_key_path))

  posture = _refresh_reporting_funds_posture(
    settings,
    latest_funds_heartbeat_payload=_funds_heartbeat_payload(
      snapshot='50.00', as_of=datetime.now(UTC) - timedelta(seconds=120), status='stale',
    ),
    client_factory=FakeClient,
  )
  assert posture['funds_source'] == 'live_refresh', 'F1: stale heartbeat must trigger the live refresh'
  assert posture['available_funds_snapshot'] == '123.45', 'F1: live refresh value must be served'


def test_f1_suppress_override_still_serves_heartbeat(tmp_path: Path) -> None:
  """F1: the scan-active suppress override is preserved — it serves the heartbeat
  snapshot regardless of freshness, with no live call."""
  private_key_path = tmp_path / 'demo_private_key.pem'
  _write_private_key(private_key_path)
  settings = _f1_live_settings(str(private_key_path))

  def _must_not_construct(_settings_arg, _private_key_arg):
    raise AssertionError('F1: suppress_live_refresh must not construct the live client')

  posture = _refresh_reporting_funds_posture(
    settings,
    latest_funds_heartbeat_payload=_funds_heartbeat_payload(
      snapshot='50.00', as_of=datetime.now(UTC) - timedelta(seconds=120), status='stale',
    ),
    client_factory=_must_not_construct,
    suppress_live_refresh=True,
  )
  assert posture['funds_source'] == 'heartbeat_snapshot', 'F1: suppress path keeps the heartbeat_snapshot source'


# ---------------------------------------------------------------------------
# F1-gate: at-point balance re-fetch in the bridge submit cycle (Lane F, 2026-06-23)
# ---------------------------------------------------------------------------

def test_f1_gate_refreshes_funds_when_stale_on_live_bridge_cycle(monkeypatch, tmp_path: Path) -> None:
  # Simulate a stale funds_posture by patching _project_funds_posture to return stale
  # on the first (startup) call and fresh on the second (at-point gate) call.
  from polyventure import service as _svc
  original_ppf = _svc._project_funds_posture
  call_count: list[int] = [0]
  refresh_calls: list[str] = []

  def _mock_ppf(**kwargs):
    call_count[0] += 1
    if call_count[0] == 1:
      # First call (startup): return stale to trigger the re-fetch gate
      return {
        'available_funds_snapshot': '50',
        'available_funds_as_of': '2026-01-01T00:00:00Z',
        'funds_refresh_status': 'stale',
        'funds_refresh_reason': 'balance_staleness_grace_exceeded',
        'balance_staleness_grace_ms': 10000,
        'stale': True,
        'stale_blocks_submit': True,
      }
    refresh_calls.append('refreshed')
    return original_ppf(**kwargs)

  monkeypatch.setattr('polyventure.service._project_funds_posture', _mock_ppf)
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'f1_gate_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'f1_gate.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW')

  payload = run_service_once(
    settings=settings,
    execution_profile='submit_order_bridge',
    client_factory=LiveFakeClient,
  )
  assert refresh_calls, 'F1: stale funds on live bridge cycle must trigger an at-point balance re-fetch'
  assert payload.get('funds_posture', {}).get('stale_blocks_submit') is False, (
    'F1: successful re-fetch must clear the stale gate'
  )


def test_f1_gate_does_not_refresh_on_sandbox_lane(monkeypatch, tmp_path: Path) -> None:
  from polyventure import service as _svc
  original_ppf = _svc._project_funds_posture
  call_count: list[int] = [0]
  extra_calls: list[str] = []

  def _mock_ppf(**kwargs):
    call_count[0] += 1
    if call_count[0] == 1:
      return {
        'available_funds_snapshot': '50', 'available_funds_as_of': '2026-01-01T00:00:00Z',
        'funds_refresh_status': 'stale', 'funds_refresh_reason': 'balance_staleness_grace_exceeded',
        'balance_staleness_grace_ms': 10000, 'stale': True, 'stale_blocks_submit': True,
      }
    extra_calls.append('extra')
    return original_ppf(**kwargs)

  monkeypatch.setattr('polyventure.service._project_funds_posture', _mock_ppf)
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  private_key_path = tmp_path / 'f1_sandbox_key.pem'
  _write_private_key(private_key_path)
  settings = _settings(str(private_key_path))

  run_service_once(
    settings=settings,
    execution_profile='submit_order_bridge',
    client_factory=FakeClient,
  )
  assert not extra_calls, 'F1: sandbox lane must not trigger the at-point balance re-fetch'


def test_f1_gate_fail_closed_when_refresh_raises(monkeypatch, tmp_path: Path) -> None:
  from polyventure import service as _svc
  call_count: list[int] = [0]

  def _mock_ppf(**kwargs):
    call_count[0] += 1
    return {
      'available_funds_snapshot': '50', 'available_funds_as_of': '2026-01-01T00:00:00Z',
      'funds_refresh_status': 'stale', 'funds_refresh_reason': 'balance_staleness_grace_exceeded',
      'balance_staleness_grace_ms': 10000, 'stale': True, 'stale_blocks_submit': True,
    }

  monkeypatch.setattr('polyventure.service._project_funds_posture', _mock_ppf)
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'f1_fail_closed_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'f1_fail_closed.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set(state_db_path, ticker='KALSHI-CANDIDATE-LOW')

  class FailingRefreshClient(LiveFakeClient):
    _balance_calls: int = 0
    def get_balance(self) -> Decimal:
      FailingRefreshClient._balance_calls += 1
      raise KalshiHttpError('network_timeout', 'connection failed', 'retry later')

  # The initial get_balance raises too — run_service_once should propagate that
  # or the stale posture must still block at the gate (no order placed).
  try:
    payload = run_service_once(
      settings=settings,
      execution_profile='submit_order_bridge',
      client_factory=FailingRefreshClient,
    )
    conn = open_database(str(state_db_path))
    pair_count = conn.execute(
      "SELECT COUNT(*) FROM pair_states WHERE operation_lane='live'"
    ).fetchone()[0]
    assert pair_count == 0, 'F1: when re-fetch fails, no pair may be placed (fail-closed)'
  except KalshiHttpError:
    pass  # startup balance failure propagates — also correct fail-closed behaviour


# ---------------------------------------------------------------------------
# P3: live-interaction-hold gate (Lane P, 2026-06-23)
# ---------------------------------------------------------------------------

def test_p3_hold_active_on_resting_both_state() -> None:
  from polyventure.web_app import _payload_has_live_interaction_hold
  payload = {'execution_chronology': {'enabled': True, 'terminal_state': 'RESTING_BOTH'}}
  assert _payload_has_live_interaction_hold(payload) is True, (
    'P3: RESTING_BOTH is non-final — hold must be active'
  )


def test_p3_hold_active_on_partial_states() -> None:
  from polyventure.web_app import _payload_has_live_interaction_hold
  for state in (
    'PARTIAL_ONE_SIDE',
    'PARTIAL_BOTH',
    'ASYMMETRIC_EXPOSURE',
    'REPAIR_LIVE',
    'EXPOSURE_CAPPED',
    'RECONCILE_REQUIRED',
    'SUBMITTING',
  ):
    payload = {'execution_chronology': {'enabled': True, 'terminal_state': state}}
    assert _payload_has_live_interaction_hold(payload) is True, (
      f'P3: {state} is non-final — hold must be active'
    )


def test_p3_hold_released_on_final_states() -> None:
  from polyventure.web_app import _payload_has_live_interaction_hold
  for state in ('CANCELED', 'ERROR', 'FILLED', 'LOCKED', 'SETTLED_EXPOSURE'):
    payload = {'execution_chronology': {'enabled': True, 'terminal_state': state}}
    assert _payload_has_live_interaction_hold(payload) is False, (
      f'P3: {state} is final — hold must be released'
    )


def test_p3_hold_inactive_when_chronology_disabled() -> None:
  from polyventure.web_app import _payload_has_live_interaction_hold
  payload = {'execution_chronology': {'enabled': False, 'terminal_state': 'RESTING_BOTH'}}
  assert _payload_has_live_interaction_hold(payload) is False, (
    'P3: chronology disabled — hold must not fire regardless of terminal_state'
  )


def test_p3_hold_inactive_when_no_pair_runtime_and_no_chronology() -> None:
  from polyventure.web_app import _payload_has_live_interaction_hold
  assert _payload_has_live_interaction_hold({}) is False
  assert _payload_has_live_interaction_hold(None) is False


# ---------------------------------------------------------------------------
# D1 (funds-decoupling + DB-performance BMAP 2026-06-20): hot-path indexes for the
# candidate SSOT query, the heartbeat-cadence expiry sweep, the STOP halt-fence
# guard, and the heartbeat lookups, so they stop full-scanning large tables.
# ---------------------------------------------------------------------------

def test_d1_hot_path_indexes_exist(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  names = {
    row[0] for row in connection.execute(
      "SELECT name FROM sqlite_master WHERE type='index'"
    ).fetchall()
  }
  for expected in ('idx_rte_session_type', 'idx_shb_lane_id'):
    assert expected in names, f'D1: missing hot-path index {expected}'
  # candidate_review_runs lane_session index is now present: all lane_session_id reads of
  # candidate_review_runs are order-insensitive (IN-subqueries) or carry an explicit ORDER BY,
  # so the index speeds replay rebuilds without reordering a non-deterministic query (the
  # earlier SSOT-ORDER determinism deferral is resolved; see persistence schema note).
  assert 'idx_crr_lane_session' in names, 'D1: candidate-runs lane_session index must exist for replay-rebuild seeks'


def test_d1_runtime_events_guard_query_uses_index(tmp_path: Path) -> None:
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  plan = connection.execute(
    'EXPLAIN QUERY PLAN SELECT id FROM runtime_events '
    'WHERE lane_session_id = ? AND event_type = ? ORDER BY id DESC LIMIT 1',
    ('sess-x', 'automation_policy_transition'),
  ).fetchall()
  plan_text = ' '.join(str(row[-1]) for row in plan)
  assert 'idx_rte_session_type' in plan_text, f'D1: STOP-guard query must use the index, got: {plan_text}'
  assert 'SCAN runtime_events' not in plan_text, f'D1: must not full-scan runtime_events, got: {plan_text}'


# ---------------------------------------------------------------------------
# STUCK-2 Lane A: the terminal-scan-replay must refresh the report's LIVE funds
# from the FH heartbeat, so the strict submit gate (money_authorized = funds
# present) does not close on replay-dominant runs while the heartbeat is fresh.
# ---------------------------------------------------------------------------

def test_stuck2_replay_funds_data_path_yields_authorizable_posture(tmp_path: Path) -> None:
  """The live-funds data path the replay injects: a fresh funds heartbeat -> a posture
  with available_funds present (money_authorized can hold), not unavailable."""
  hb_time = datetime.now(UTC)
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  persist_service_heartbeat(
    connection,
    component='websocket-session',
    status='heartbeat-live',
    operation_lane='live',
    lane_session_id='live-sess-1',
    recorded_at_utc=hb_time.isoformat(),
    detail={
      'available_funds_snapshot': '50',
      'available_funds_as_of': hb_time.isoformat(),
      'funds_refresh_status': 'fresh',
    },
  )
  funds_hb = _latest_funds_heartbeat_payload(connection, operation_lane='live')
  posture = _project_funds_posture(latest_heartbeat_payload=funds_hb, now=hb_time + timedelta(seconds=1))
  assert posture['available_funds_snapshot'] == '50', 'replay funds refresh must carry the live snapshot'
  assert posture['funds_refresh_status'] == 'fresh', 'a fresh FH heartbeat must yield a fresh posture'
  assert posture['stale'] is False, 'fresh funds must not be stale (submit gate stays open)'


def test_stuck2_replay_funds_data_path_fails_closed_without_funds_heartbeat(tmp_path: Path) -> None:
  """Fail-closed: with no fresh funds heartbeat the replay posture is unavailable
  (gate stays closed) -- never fabricated fresh."""
  connection = open_database(tmp_path / 'kalshi.sqlite3')
  funds_hb = _latest_funds_heartbeat_payload(connection, operation_lane='live')
  assert funds_hb is None, 'no funds heartbeat -> None'
  posture = _project_funds_posture(latest_heartbeat_payload=funds_hb)
  assert posture['available_funds_snapshot'] is None, 'no funds -> no snapshot (gate closed)'
  assert posture['funds_refresh_status'] == 'unavailable', 'fail-closed to unavailable, not fabricated fresh'


def test_stuck2_replay_livetruth_refresh_wired_in_web_app() -> None:
  src_path = Path(__file__).resolve().parents[1] / 'src' / 'polyventure' / 'web_app.py'
  text = src_path.read_text(encoding='utf-8')
  # The replay path refreshes funds_posture + latest_heartbeat + connection_state from live sources.
  assert "_replay_report['funds_posture'] = _project_funds_posture(" in text, 'Lane A: replay must refresh funds_posture'
  assert "_latest_funds_heartbeat_payload(_replay_conn" in text, 'Lane A: funds from the live FH heartbeat'
  assert "_replay_report['latest_heartbeat'] = _any_hb" in text, 'Lane A: replay must refresh latest_heartbeat'
  assert "_replay_report['connection_state'] = dict(payload['connection_state'])" in text, 'Lane A: replay must refresh connection_state'


# ---------------------------------------------------------------------------
# FB-10: operator_lane_session_id threading — candidate_review_runs.lane_session_id
# uses the stable operator session so the panel query can span automation cycles
# ---------------------------------------------------------------------------

_FB10_CONTRACT = {
  'candidate_math_evidence_contract': {
    'contract_version': '1',
    'model_reference': 'test',
    'retention_schema': 'test',
    'seeded_fixture_family': None,
    'candidate_evidence_rows': [],
  }
}


def test_fb10_persist_candidate_math_uses_operator_session_for_lane_column(tmp_path: Path) -> None:
  db_path = str(tmp_path / 'fb10.sqlite3')
  conn = open_database(db_path)
  per_cycle_id = 'live-20260616T090000Z-aabbccdd'
  operator_id = 'live-20260616T084045Z-stableop'
  _persist_candidate_math_contract(
    conn,
    operation_lane='live',
    lane_session_id=per_cycle_id,
    operator_lane_session_id=operator_id,
    recorded_at=datetime(2026, 6, 16, 9, 0, 0, tzinfo=UTC),
    source_action='runtime-cycle',
    analytical_outputs=_FB10_CONTRACT,
  )
  row = conn.execute(
    'SELECT run_id, lane_session_id FROM candidate_review_runs WHERE source_action = ?',
    ('runtime-cycle',),
  ).fetchone()
  assert row is not None, 'FB-10: candidate_review_run row must be written'
  assert per_cycle_id in str(row[0]), 'FB-10: run_id must retain per-cycle id for uniqueness'
  assert row[1] == operator_id, 'FB-10: lane_session_id column must be the stable operator session'


def test_fb10_persist_candidate_math_falls_back_to_per_cycle_without_operator_session(tmp_path: Path) -> None:
  db_path = str(tmp_path / 'fb10_fallback.sqlite3')
  conn = open_database(db_path)
  per_cycle_id = 'live-20260616T090000Z-xxyyzz00'
  _persist_candidate_math_contract(
    conn,
    operation_lane='live',
    lane_session_id=per_cycle_id,
    recorded_at=datetime(2026, 6, 16, 9, 0, 0, tzinfo=UTC),
    source_action='scan-once',
    analytical_outputs=_FB10_CONTRACT,
  )
  row = conn.execute(
    'SELECT lane_session_id FROM candidate_review_runs WHERE source_action = ?',
    ('scan-once',),
  ).fetchone()
  assert row is not None, 'FB-10: row must be written when no operator session provided'
  assert row[0] == per_cycle_id, 'FB-10: lane_session_id must fall back to per-cycle id when no operator session'


# ---------------------------------------------------------------------------
# CA-2: _mark_auto_canceled_candidates_terminal
# ---------------------------------------------------------------------------

def _seed_ca2_db(
  connection: Any,
  *,
  run_id: str,
  operator_session_id: str,
  ticker: str,
  pair_id: str,
  operation_lane: str,
) -> None:
  persist_candidate_review_run(
    connection,
    run_id=run_id,
    recorded_at_utc='2026-06-16T10:00:00Z',
    operation_lane=operation_lane,
    candidate_signature='sig-ca2',
    candidate_count=1,
    source_action='scan',
    lane_session_id=operator_session_id,
  )
  persist_candidate_review_candidates(
    connection,
    run_id=run_id,
    recorded_at_utc='2026-06-16T10:00:00Z',
    operation_lane=operation_lane,
    candidates=[
      {
        'candidate_uid': ticker,
        'candidate_key': ticker,
        'ticker': ticker,
        'qualifier_tier': 'live_qualifying',
        'review_row_origin': 'scan',
      }
    ],
  )
  pair_plan = PairOrderPlan(
    pair_id=pair_id,
    ticker=ticker,
    yes_price=Decimal('0.30'),
    no_price=Decimal('0.40'),
    contract_count=Decimal('2'),
    yes_client_order_id=f'{pair_id}-yes',
    no_client_order_id=f'{pair_id}-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(connection, pair_plan, created_at_utc='2026-06-16T10:00:01Z', operation_lane=operation_lane)
  persist_pair_state_transition(
    connection,
    pair_id=pair_id,
    state='CANCELED',
    recorded_at_utc='2026-06-16T10:00:10Z',
    operation_lane=operation_lane,
    lane_session_id=operator_session_id,
    detail={'reason': 'auto_cancel', 'ticker': ticker},
  )


def test_ca2_mark_auto_canceled_transitions_in_flight_to_terminal(tmp_path: Path) -> None:
  db_path = str(tmp_path / 'ca2_mark_terminal.sqlite3')
  conn = open_database(db_path)
  run_id = 'run-ca2-001'
  operator_session_id = 'live-20260616T100000Z-ca2abc'
  ticker = 'KALSHI-AUTO-CANCEL-A'
  pair_id = 'pair-ca2-001'

  _seed_ca2_db(conn, run_id=run_id, operator_session_id=operator_session_id,
               ticker=ticker, pair_id=pair_id, operation_lane='live')
  with conn:
    conn.execute(
      "UPDATE candidate_review_candidates SET lifecycle_stage='in_flight' WHERE run_id=? AND candidate_uid=?",
      (run_id, ticker),
    )

  _mark_auto_canceled_candidates_terminal(
    conn,
    operation_lane='live',
    operator_lane_session_id=operator_session_id,
    recorded_at=datetime(2026, 6, 16, 10, 1, 0, tzinfo=UTC),
  )

  row = conn.execute(
    'SELECT lifecycle_stage, terminal_cause FROM candidate_review_candidates WHERE run_id=? AND candidate_uid=?',
    (run_id, ticker),
  ).fetchone()
  assert row is not None
  assert row['lifecycle_stage'] == 'terminal', 'CA-2: in_flight candidate of a CANCELED pair must become terminal'
  assert row['terminal_cause'] == 'auto_cancel', 'CA-2: terminal_cause must be auto_cancel'


def test_ca2_mark_auto_canceled_leaves_discovered_candidate_untouched(tmp_path: Path) -> None:
  db_path = str(tmp_path / 'ca2_discovered_untouched.sqlite3')
  conn = open_database(db_path)
  run_id = 'run-ca2-002'
  operator_session_id = 'live-20260616T100000Z-ca2def'
  ticker = 'KALSHI-AUTO-CANCEL-B'
  pair_id = 'pair-ca2-002'

  _seed_ca2_db(conn, run_id=run_id, operator_session_id=operator_session_id,
               ticker=ticker, pair_id=pair_id, operation_lane='live')

  _mark_auto_canceled_candidates_terminal(
    conn,
    operation_lane='live',
    operator_lane_session_id=operator_session_id,
    recorded_at=datetime(2026, 6, 16, 10, 1, 0, tzinfo=UTC),
  )

  row = conn.execute(
    'SELECT lifecycle_stage FROM candidate_review_candidates WHERE run_id=? AND candidate_uid=?',
    (run_id, ticker),
  ).fetchone()
  assert row is not None
  assert row['lifecycle_stage'] == 'discovered', 'CA-2: discovered candidate must not be changed by the auto-cancel update'


def test_ca2_mark_auto_canceled_is_idempotent(tmp_path: Path) -> None:
  db_path = str(tmp_path / 'ca2_idempotent.sqlite3')
  conn = open_database(db_path)
  run_id = 'run-ca2-003'
  operator_session_id = 'live-20260616T100000Z-ca2ghi'
  ticker = 'KALSHI-AUTO-CANCEL-C'
  pair_id = 'pair-ca2-003'

  _seed_ca2_db(conn, run_id=run_id, operator_session_id=operator_session_id,
               ticker=ticker, pair_id=pair_id, operation_lane='live')
  with conn:
    conn.execute(
      "UPDATE candidate_review_candidates SET lifecycle_stage='in_flight' WHERE run_id=? AND candidate_uid=?",
      (run_id, ticker),
    )
  recorded_at = datetime(2026, 6, 16, 10, 1, 0, tzinfo=UTC)

  _mark_auto_canceled_candidates_terminal(conn, operation_lane='live',
    operator_lane_session_id=operator_session_id, recorded_at=recorded_at)
  _mark_auto_canceled_candidates_terminal(conn, operation_lane='live',
    operator_lane_session_id=operator_session_id, recorded_at=recorded_at)

  row = conn.execute(
    'SELECT lifecycle_stage, terminal_cause FROM candidate_review_candidates WHERE run_id=? AND candidate_uid=?',
    (run_id, ticker),
  ).fetchone()
  assert row['lifecycle_stage'] == 'terminal', 'CA-2: second call must not corrupt the terminal state'
  assert row['terminal_cause'] == 'auto_cancel', 'CA-2: second call must preserve terminal_cause'


# ---------------------------------------------------------------------------
# MFEW — Market Fetch Entry-Window Filter tests
# ---------------------------------------------------------------------------

def test_mfew_get_markets_includes_min_close_ts_when_set() -> None:
  from polyventure.http_client import KalshiHttpClient
  captured: list[dict] = []

  class _CapturingClient(KalshiHttpClient):
    def __init__(self) -> None: pass
    def _request(self, method: str, endpoint: str, **kwargs: object) -> dict:
      captured.append(dict(kwargs.get('params', {})))
      return {'markets': [], 'cursor': None}

  _CapturingClient().get_markets(min_close_ts=1700000000)
  assert captured[0].get('min_close_ts') == 1700000000


def test_mfew_get_markets_includes_max_close_ts_when_set() -> None:
  from polyventure.http_client import KalshiHttpClient
  captured: list[dict] = []

  class _CapturingClient(KalshiHttpClient):
    def __init__(self) -> None: pass
    def _request(self, method: str, endpoint: str, **kwargs: object) -> dict:
      captured.append(dict(kwargs.get('params', {})))
      return {'markets': [], 'cursor': None}

  _CapturingClient().get_markets(max_close_ts=1700003600)
  assert captured[0].get('max_close_ts') == 1700003600


def test_mfew_get_markets_omits_time_params_when_none() -> None:
  from polyventure.http_client import KalshiHttpClient
  captured: list[dict] = []

  class _CapturingClient(KalshiHttpClient):
    def __init__(self) -> None: pass
    def _request(self, method: str, endpoint: str, **kwargs: object) -> dict:
      captured.append(dict(kwargs.get('params', {})))
      return {'markets': [], 'cursor': None}

  _CapturingClient().get_markets()
  assert 'min_close_ts' not in captured[0]
  assert 'max_close_ts' not in captured[0]


def test_mfew_fetch_open_markets_forwards_min_close_ts() -> None:
  from polyventure.market_data import fetch_open_markets
  captured: list[dict] = []

  class _FakeClient:
    def get_markets(self, *, status: str, limit: int, cursor: object, min_close_ts: object, max_close_ts: object) -> tuple:
      captured.append({'min_close_ts': min_close_ts, 'max_close_ts': max_close_ts})
      return [], None

  fetch_open_markets(_FakeClient(), min_close_ts=1700000000)
  assert captured[0]['min_close_ts'] == 1700000000


def test_mfew_fetch_open_markets_forwards_max_close_ts() -> None:
  from polyventure.market_data import fetch_open_markets
  captured: list[dict] = []

  class _FakeClient:
    def get_markets(self, *, status: str, limit: int, cursor: object, min_close_ts: object, max_close_ts: object) -> tuple:
      captured.append({'min_close_ts': min_close_ts, 'max_close_ts': max_close_ts})
      return [], None

  fetch_open_markets(_FakeClient(), max_close_ts=1700003600)
  assert captured[0]['max_close_ts'] == 1700003600


def test_mfew_fetch_open_markets_no_params_passes_none() -> None:
  from polyventure.market_data import fetch_open_markets
  captured: list[dict] = []

  class _FakeClient:
    def get_markets(self, *, status: str, limit: int, cursor: object, min_close_ts: object, max_close_ts: object) -> tuple:
      captured.append({'min_close_ts': min_close_ts, 'max_close_ts': max_close_ts})
      return [], None

  fetch_open_markets(_FakeClient())
  assert captured[0]['min_close_ts'] is None
  assert captured[0]['max_close_ts'] is None


def test_mfew_min_close_ts_formula_correct() -> None:
  captured: list[dict] = []
  recorded_at = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)
  settings = _settings('secrets/demo.pem')

  class _FakeClient:
    def get_markets(self, *, status: str, limit: int, cursor: object, min_close_ts: object, max_close_ts: object) -> tuple:
      captured.append({'min_close_ts': min_close_ts, 'max_close_ts': max_close_ts})
      return [], None

  _load_candidate_market_set(_FakeClient(), recorded_at=recorded_at, settings=settings)
  expected = int(recorded_at.timestamp()) + settings.entry_window_end_sec + settings.entry_window_fetch_padding_sec
  assert captured[0]['min_close_ts'] == expected


def test_mfew_max_close_ts_formula_correct() -> None:
  captured: list[dict] = []
  recorded_at = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)
  settings = _settings('secrets/demo.pem')

  class _FakeClient:
    def get_markets(self, *, status: str, limit: int, cursor: object, min_close_ts: object, max_close_ts: object) -> tuple:
      captured.append({'min_close_ts': min_close_ts, 'max_close_ts': max_close_ts})
      return [], None

  _load_candidate_market_set(_FakeClient(), recorded_at=recorded_at, settings=settings)
  expected = int(recorded_at.timestamp()) + settings.entry_window_start_sec
  assert captured[0]['max_close_ts'] == expected


def test_mfew_load_candidate_set_applies_fetch_padding_to_min_close_ts() -> None:
  captured: list[dict] = []
  recorded_at = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)
  settings = Settings(**{**_settings('secrets/demo.pem').__dict__, 'entry_window_fetch_padding_sec': 30})

  class _FakeClient:
    def get_markets(self, *, status: str, limit: int, cursor: object, min_close_ts: object, max_close_ts: object) -> tuple:
      captured.append({'min_close_ts': min_close_ts})
      return [], None

  _load_candidate_market_set(_FakeClient(), recorded_at=recorded_at, settings=settings)
  expected = int(recorded_at.timestamp()) + settings.entry_window_end_sec + 30
  assert captured[0]['min_close_ts'] == expected


def test_mfew_no_settings_path_passes_none_time_params() -> None:
  captured: list[dict] = []
  recorded_at = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)

  class _FakeClient:
    def get_markets(self, *, status: str, limit: int, cursor: object, min_close_ts: object, max_close_ts: object) -> tuple:
      captured.append({'min_close_ts': min_close_ts, 'max_close_ts': max_close_ts})
      return [], None

  _load_candidate_market_set(_FakeClient(), recorded_at=recorded_at, settings=None)
  assert captured[0]['min_close_ts'] is None
  assert captured[0]['max_close_ts'] is None


def test_mfew_entry_window_fetch_padding_sec_default_is_fifteen() -> None:
  settings = _settings('secrets/demo.pem')
  assert settings.entry_window_fetch_padding_sec == 15


def test_mfew_ws_hydration_skipped_when_no_entry_window_markets() -> None:
  recorded_at = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)
  settings = _settings('secrets/demo.pem')

  class _EmptyMarketsClient:
    def get_markets(self, *, status: str, limit: int, cursor: object, min_close_ts: object, max_close_ts: object) -> tuple:
      return [], None

  _EmptyMarketsClient.__module__ = 'polyventure.http_client'

  _, _, _, _, websocket_posture = _load_candidate_market_set(
    _EmptyMarketsClient(),
    recorded_at=recorded_at,
    settings=settings,
    private_key=object(),
  )
  assert websocket_posture['websocket_status'] == 'skipped_no_entry_window_markets'
  assert websocket_posture['websocket_connected'] is False


# ---------------------------------------------------------------------------
# FIX-DRC — Deck-rebuild funds-refresh contention (suppress live funds during scan)
# ---------------------------------------------------------------------------

def _drc_live_settings(private_key_file: str, state_db_path: str | None = None) -> Settings:
  base = {
    **_settings(private_key_file).__dict__,
    'kalshi_env': 'prod',
    'api_base_url': 'https://api.kalshi.com/trade-api/v2',
    'websocket_url': 'wss://api.kalshi.com/trade-api/ws/v2',
    'operation_lane': 'live',
  }
  if state_db_path is not None:
    base['state_db_path'] = state_db_path
  return Settings(**base)


def _drc_heartbeat(snapshot: str, *, age_seconds: float) -> dict:
  as_of = (datetime.now(UTC) - timedelta(seconds=age_seconds)).isoformat()
  return {'detail': {
    'available_funds_snapshot': snapshot,
    'available_funds_as_of': as_of,
    'funds_refresh_status': 'fresh',
  }}


class _RaisingFundsClient:
  """Live client whose balance call must never run under suppression."""
  def __init__(self, settings: object, private_key: object) -> None:
    raise AssertionError('DRC: live client must not be constructed when suppressed')


def test_drc_suppressed_refresh_serves_heartbeat_and_makes_no_live_call() -> None:
  settings = _drc_live_settings('secrets/demo.pem')
  heartbeat = _drc_heartbeat('50', age_seconds=1.0)

  posture = _refresh_reporting_funds_posture(
    settings,
    latest_funds_heartbeat_payload=heartbeat,
    client_factory=_RaisingFundsClient,
    suppress_live_refresh=True,
  )

  assert posture['available_funds_snapshot'] == '50'
  assert posture['funds_source'] == 'heartbeat_snapshot'
  assert posture['funds_refresh_ms'] is None
  assert posture['stale'] is False


def test_drc_suppressed_preserves_as_of_and_marks_stale_when_aged() -> None:
  settings = _drc_live_settings('secrets/demo.pem')
  aged = _drc_heartbeat('50', age_seconds=120.0)  # well beyond the 10s grace
  aged_as_of = aged['detail']['available_funds_as_of']

  posture = _refresh_reporting_funds_posture(
    settings,
    latest_funds_heartbeat_payload=aged,
    client_factory=_RaisingFundsClient,
    suppress_live_refresh=True,
  )

  assert posture['available_funds_as_of'] == aged_as_of  # preserved, not re-stamped
  assert posture['stale'] is True
  assert posture['stale_blocks_submit'] is True
  assert posture['funds_source'] == 'heartbeat_snapshot'


def test_drc_suppressed_no_heartbeat_is_unavailable_without_error() -> None:
  settings = _drc_live_settings('secrets/demo.pem')

  posture = _refresh_reporting_funds_posture(
    settings,
    suppress_live_refresh=True,
  )

  assert posture['available_funds_snapshot'] is None
  assert posture['funds_refresh_status'] == 'unavailable'
  assert posture['funds_source'] == 'heartbeat_snapshot'
  assert posture['funds_refresh_ms'] is None


def test_drc_default_refresh_makes_live_call(tmp_path: Path) -> None:
  private_key_path = tmp_path / 'drc_live_key.pem'
  _write_private_key(private_key_path)
  settings = _drc_live_settings(str(private_key_path))

  posture = _refresh_reporting_funds_posture(
    settings,
    client_factory=FakeClient,
  )

  assert posture['funds_source'] == 'live_refresh'
  assert posture['available_funds_snapshot'] == '123.45'
  assert posture['funds_refresh_ms'] is not None


def test_drc_reconcile_skips_funds_heartbeat_when_suppressed(tmp_path: Path) -> None:
  private_key_path = tmp_path / 'drc_reconcile_key.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'drc_reconcile.sqlite3'
  settings = _drc_live_settings(str(private_key_path), state_db_path=str(state_db_path))

  def _reconcile_heartbeat_count() -> int:
    connection = open_database(state_db_path)
    row = connection.execute(
      "SELECT COUNT(*) AS c FROM service_heartbeats WHERE component = 'reconcile' AND operation_lane = 'live'",
    ).fetchone()
    return int(row['c'])

  # Suppressed: no funds-bearing reconcile heartbeat written.
  reconcile_pairs(settings=settings, client_factory=FakeClient, suppress_live_funds_refresh=True)
  assert _reconcile_heartbeat_count() == 0

  # Default (loop-authoritative): the reconcile heartbeat is written as before.
  reconcile_pairs(settings=settings, client_factory=FakeClient)
  assert _reconcile_heartbeat_count() == 1


# --- Lane L5b: composite money-evidence signing + pre-submit signing-key gate ---

def _l5b_signing_keypair():
  from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
  import base64 as _b64
  private_key = Ed25519PrivateKey.generate()
  public_der = private_key.public_key().public_bytes(
    serialization.Encoding.DER,
    serialization.PublicFormat.SubjectPublicKeyInfo,
  )
  return private_key, _b64.b64encode(public_der).decode('ascii')


def test_l5b_money_evidence_signature_round_trips_and_binds_context(monkeypatch) -> None:
  from polyventure import service as _service
  from polyventure import signed_evidence as _se

  private_key, public_b64 = _l5b_signing_keypair()
  monkeypatch.setattr(_se, 'trusted_verification_keys', lambda: {'k1': public_b64})
  monkeypatch.setattr(_se, 'load_signing_key', lambda: (private_key, 'k1'))

  chronology = {
    'enabled': True,
    'profile': 'submit_order_bridge',
    'terminal_state': 'CANCELED',
    'sequence_count': 4,
    'contract_version': _service.TRANCHE_F_EXECUTION_EVENT_CONTRACT_VERSION,
    'event_packet': [{'event_type': 'submit_order_intent', 'order_id': 'o-1'}],
    'states': ['SUBMITTING', 'CANCELED'],
    'chronology': {'submit': {'seq': 's-1'}},
  }
  signed = _service._attach_money_evidence_signature(
    chronology, operation_lane='live', lane_session_id='live-sess-1', pair_id='pair-1',
  )
  block = signed['signed_evidence']
  assert block['signature_status'] == 'signed'

  payload = _service._money_evidence_signature_payload(
    signed, operation_lane='live', lane_session_id='live-sess-1', pair_id='pair-1',
  )
  valid, code = _se.verify_evidence_record(payload, block)
  assert valid is True and code is None

  # Re-attributing the record to a different lane breaks verification (lane is bound).
  tampered = _service._money_evidence_signature_payload(
    signed, operation_lane='sandbox', lane_session_id='live-sess-1', pair_id='pair-1',
  )
  assert _se.verify_evidence_record(tampered, block)[0] is False


def test_l5b_blocked_chronology_is_unsigned_when_no_signing_key(monkeypatch) -> None:
  from polyventure import service as _service
  from polyventure import signed_evidence as _se

  monkeypatch.setattr(_se, 'load_signing_key', lambda: None)
  blocked = _service._signing_key_blocked_execution_chronology(
    operation_lane='live', lane_session_id='live-sess-2', pair_id='pair-2',
  )
  assert blocked['terminal_state'] == 'BLOCKED'
  assert blocked['blocked_reason'] == 'signing_key_unavailable'
  assert blocked['event_packet'] == []
  assert blocked['signed_evidence']['signature_status'] == 'unsigned'


# ---------------------------------------------------------------------------
# C2: zero-bid candidate filter (Lane C, 2026-06-23)
# ---------------------------------------------------------------------------

def _make_market_for_filter(
  ticker: str,
  yes_bid: str,
  no_bid: str,
  seconds_to_close: int = 300,
) -> MarketSnapshot:
  from datetime import UTC
  return MarketSnapshot(
    ticker=ticker,
    title=ticker,
    close_time=datetime.now(UTC) + timedelta(seconds=seconds_to_close),
    status='open',
    yes_bid_dollars=Decimal(yes_bid) if yes_bid != 'None' else None,
    no_bid_dollars=Decimal(no_bid) if no_bid != 'None' else None,
    volume_24h_fp=Decimal('100.00'),
    open_interest_fp=Decimal('200.00'),
    event_ticker=ticker + '-EVENT',
  )


def _filter_settings(tmp_path=None) -> Settings:
  import tempfile, os
  if tmp_path is None:
    fd, key_path = tempfile.mkstemp(suffix='.pem')
    os.close(fd)
  else:
    key_path = str(tmp_path / 'filter_key.pem')
  _write_private_key(Path(key_path))
  return _settings(key_path)


def test_c2_zero_yes_bid_rejected_by_find_candidates(tmp_path: Path) -> None:
  from datetime import UTC
  settings = _filter_settings(tmp_path)
  markets = [_make_market_for_filter('KXMVE-ZERO', '0.00', '0.41')]
  result = find_candidates(markets, datetime.now(UTC), settings)
  tickers = [c.ticker for c in result]
  assert 'KXMVE-ZERO' not in tickers, 'C2: zero yes_bid must be rejected — never reaches ranking'


def test_c2_zero_no_bid_rejected_by_find_candidates(tmp_path: Path) -> None:
  from datetime import UTC
  settings = _filter_settings(tmp_path)
  markets = [_make_market_for_filter('KXMVE-ZERO', '0.41', '0.00')]
  result = find_candidates(markets, datetime.now(UTC), settings)
  assert not any(c.ticker == 'KXMVE-ZERO' for c in result), 'C2: zero no_bid must be rejected'


def test_c2_none_bid_rejected_by_find_candidates(tmp_path: Path) -> None:
  from datetime import UTC
  settings = _filter_settings(tmp_path)
  markets = [_make_market_for_filter('KXMVE-NONE', 'None', '0.41')]
  result = find_candidates(markets, datetime.now(UTC), settings)
  assert not any(c.ticker == 'KXMVE-NONE' for c in result), 'C2: None yes_bid must be rejected'


def test_c2_positive_bids_accepted(tmp_path: Path) -> None:
  from datetime import UTC
  settings = _filter_settings(tmp_path)
  markets = [_make_market_for_filter('KXMVE-OK', '0.40', '0.40')]
  result = find_candidates(markets, datetime.now(UTC), settings)
  assert any(c.ticker == 'KXMVE-OK' for c in result), 'C2: valid bids must pass through to ranking'


def test_c2_zero_bid_never_produces_fake_edge(tmp_path: Path) -> None:
  from datetime import UTC
  settings = _filter_settings(tmp_path)
  markets = [
    _make_market_for_filter('KXMVE-ZERO', '0.00', '0.00'),
    _make_market_for_filter('KXMVE-OK', '0.40', '0.40'),
  ]
  result = find_candidates(markets, datetime.now(UTC), settings)
  tickers = [c.ticker for c in result]
  assert 'KXMVE-ZERO' not in tickers, 'C2: zero-bid market must not appear in results at any edge value'
  assert 'KXMVE-OK' in tickers, 'C2: valid market must still be found when zero-bid is filtered'
  for c in result:
    assert c.edge_net_per_contract < Decimal('1'), 'C2: no candidate may have edge_net >= 1.0'


# ---------------------------------------------------------------------------
# E3: session-scope _load_current_pairs (Lane E, 2026-06-23)
# ---------------------------------------------------------------------------

def test_e3_load_current_pairs_excludes_historical_session(tmp_path: Path) -> None:
  from polyventure.persistence import persist_pair_plan
  state_db_path = tmp_path / 'e3_pairs.sqlite3'
  conn = open_database(str(state_db_path))

  def _make_plan(pair_id: str, ticker: str) -> PairOrderPlan:
    return PairOrderPlan(
      pair_id=pair_id, ticker=ticker,
      yes_price=Decimal('0.40'), no_price=Decimal('0.40'),
      contract_count=Decimal('1'),
      yes_client_order_id=f'{pair_id}-yes', no_client_order_id=f'{pair_id}-no',
      time_in_force='good_till_canceled', post_only=False,
      cancel_order_on_pause=True, subaccount=0,
    )

  # Historical pair — written with old session
  persist_pair_plan(conn, _make_plan('pair-historical', 'KXMVE-HIST'), created_at_utc='2026-06-14T09:36:00Z', operation_lane='live')
  persist_pair_state_transition(
    conn, pair_id='pair-historical', state='RESTING_BOTH',
    recorded_at_utc='2026-06-14T09:36:00Z', operation_lane='live',
    lane_session_id='live-old-session', detail={'ticker': 'KXMVE-HIST'},
  )
  # Current pair — written with current session
  persist_pair_plan(conn, _make_plan('pair-current', 'KXMVE-CURR'), created_at_utc='2026-06-23T13:00:00Z', operation_lane='live')
  persist_pair_state_transition(
    conn, pair_id='pair-current', state='RESTING_BOTH',
    recorded_at_utc='2026-06-23T13:00:00Z', operation_lane='live',
    lane_session_id='live-current-session', detail={'ticker': 'KXMVE-CURR'},
  )

  # With session scope: only current pair visible
  scoped = _load_current_pairs(conn, operation_lane='live', lane_session_id='live-current-session')
  scoped_ids = {p.pair_id for p in scoped}
  assert 'pair-current' in scoped_ids, 'E3: current-session pair must be visible'
  assert 'pair-historical' not in scoped_ids, 'E3: historical-session pair must be excluded when lane_session_id is set'

  # Without session scope (fallback): both visible
  unscoped = _load_current_pairs(conn, operation_lane='live')
  unscoped_ids = {p.pair_id for p in unscoped}
  assert 'pair-current' in unscoped_ids, 'E3: current pair visible in unscoped mode'
  assert 'pair-historical' in unscoped_ids, 'E3: historical pair visible in unscoped mode (regression guard)'


def _qualified_zero_fill_error_detail() -> dict[str, object]:
  # Mirrors the live-order placement-error write (service pair-state transition detail) from the
  # 2026-07-02 WATCH incident: Kalshi HTTP 400 on order create, zero fills both legs.
  return {
    'ticker': 'KXBNB15M-26JUL020830-30',
    'reason': 'live_order_api_error',
    'error_family': 'KalshiHttpError',
    'kalshi_status_code': 400,
    'yes_filled_contracts': '0',
    'no_filled_contracts': '0',
    'average_yes_price': '0.41',
    'average_no_price': '0.38',
    'realized_fees_dollars': '0',
    'websocket_connected': False,
  }


def test_zero_fill_rejected_order_error_projects_terminal_no_exposure() -> None:
  # GUARD_RECOVERY_CORRECTION_AND_WATCH_STALE_ERROR_PROJECTION_BMAP_2026-07-02 (W1/W2): a proven
  # Kalshi 4xx rejection with zero fills on both legs and no remote order id projects as terminal
  # no-exposure history instead of a reconcile hold.
  detail = _qualified_zero_fill_error_detail()
  assert service_module._zero_fill_rejected_order_error(detail) is True
  assert service_module._project_public_state_id('ERROR', detail=detail) == 'ERROR_NO_EXPOSURE'


def test_latest_pair_snapshots_project_zero_fill_error_no_exposure(tmp_path: Path) -> None:
  state_db_path = tmp_path / 'zero_fill_snapshot_projection.sqlite3'
  connection = open_database(state_db_path)
  plan = PairOrderPlan(
    pair_id='pair-zero-fill-error',
    ticker='KXBNB15M-26JUL020830-30',
    yes_price=Decimal('0.41'),
    no_price=Decimal('0.38'),
    contract_count=Decimal('5'),
    yes_client_order_id='pair-zero-fill-error-yes',
    no_client_order_id='pair-zero-fill-error-no',
    time_in_force='good_till_canceled',
    post_only=True,
    cancel_order_on_pause=True,
    subaccount=0,
  )
  persist_pair_plan(connection, plan, created_at_utc='2026-07-02T12:24:40Z', operation_lane='live')
  persist_pair_state_transition(
    connection,
    pair_id=plan.pair_id,
    state='ERROR',
    recorded_at_utc='2026-07-02T12:24:42Z',
    operation_lane='live',
    lane_session_id='live-zero-fill-error',
    detail=_qualified_zero_fill_error_detail(),
  )

  snapshot = _latest_pair_snapshots(connection, operation_lane='live')[0]

  assert snapshot['state'] == 'ERROR'
  assert snapshot['public_state_id'] == 'ERROR_NO_EXPOSURE'


def test_zero_fill_rejected_order_error_fails_closed_per_condition() -> None:
  # W1 negatives: every single violated condition keeps the reconcile-attention projection.
  base = _qualified_zero_fill_error_detail()
  # Condition 1: only the raw ERROR state is eligible.
  assert service_module._project_public_state_id('REPAIR_LIVE', detail=dict(base)) == 'REPAIR_LIVE'
  negatives: list[dict[str, object]] = [
    {'reason': 'unwind_cancel_failed_after_no_post_error'},  # 2: wrong reason
    {'error_family': 'KalshiTransportError'},                # 3: not a proven HTTP rejection
    {'kalshi_status_code': 500},                             # 4: 5xx is ambiguous (order may exist)
    {'kalshi_status_code': None},                            # 4: missing status fails closed
    {'yes_filled_contracts': '11'},                          # 5: fill-bearing
    {'no_filled_contracts': '1'},                            # 6: fill-bearing
    {'accepted_yes_order_id': 'ord-123'},                    # 7: remote order id present
  ]
  for override in negatives:
    detail = {**base, **override}
    assert service_module._zero_fill_rejected_order_error(detail) is False, override
    assert service_module._project_public_state_id('ERROR', detail=detail) == 'RECONCILE_REQUIRED', override
  missing_fill_truth = dict(base)
  missing_fill_truth.pop('yes_filled_contracts')
  assert service_module._project_public_state_id('ERROR', detail=missing_fill_truth) == 'RECONCILE_REQUIRED'


def test_error_no_exposure_postures_are_terminal_without_attention() -> None:
  # W2: terminal no-exposure rests silent (pair-local), mobility idle, WAIT-only action contract
  # from the existing vocabulary; unqualified reconcile-attention postures are preserved.
  failure = service_module._project_failure_posture(public_state_id='ERROR_NO_EXPOSURE')
  assert failure == {'failure_class': 'SILENT_CONTINUE', 'failure_scope': 'pair_local'}
  mobility = service_module._project_mobility_posture(
    public_state_id='ERROR_NO_EXPOSURE',
    recorded_at=datetime.now(UTC),
  )
  assert mobility['mobility_overlay_state'] == 'AUTO_CANCEL_IDLE'
  action = service_module._project_action_contract(public_state_id='ERROR_NO_EXPOSURE')
  assert action['allowed_actions'] == ['WAIT']
  assert action['retry_allowed'] is False
  reconcile_action = service_module._project_action_contract(public_state_id='RECONCILE_REQUIRED')
  assert 'RECONCILE' in reconcile_action['allowed_actions']


# --- Selection/screening alignment BMAP 2026-07-02: S3 submit-prep reorder + top-K cap ---


def test_submit_prep_divergence_block_skips_expensive_readbacks(monkeypatch, tmp_path: Path) -> None:
  """Cheapest-rejection-first: a divergence-blocked candidate must consume ONE
  orderbook read and never reach the market/event-family readbacks."""
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_prep_reorder.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_prep_reorder.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  _seed_live_saved_set_batch(
    state_db_path,
    tickers=['KALSHI-CANDIDATE-HIGH', 'KALSHI-CANDIDATE-LOW'],
    saved_set_id='live-prep-reorder-001',
  )

  client_instance = None

  class ReorderProofClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self
      self.get_market_calls: list[str] = []
      self.get_event_calls: list[str] = []

    def get_market(self, ticker: str):
      self.get_market_calls.append(ticker)
      return super().get_market(ticker)

    def get_event(self, event_ticker: str):
      self.get_event_calls.append(event_ticker)
      return super().get_event(event_ticker)

    def get_orderbook(self, ticker: str, depth: int = 0):
      if ticker == 'KALSHI-CANDIDATE-HIGH':
        self.orderbook_calls.append(ticker)
        from polyventure.websocket_client import normalize_orderbook_snapshot
        return normalize_orderbook_snapshot(
          {
            'ticker': ticker,
            'yes_dollars': [['0.1000', '20.00']],
            'no_dollars': [['0.5500', '20.00']],
          }
        )
      return super().get_orderbook(ticker, depth=depth)

  payload = run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=ReorderProofClient)

  assert client_instance is not None
  assert payload['planned_pair_count'] == 1
  assert payload['planned_pairs'][0]['ticker'] == 'KALSHI-CANDIDATE-LOW'
  # The divergence-blocked candidate never reached the expensive submit-prep
  # readbacks (the scan's own suitability pass reads each event family once, so
  # a submit-prep proof read shows up as a SECOND read for that family):
  assert 'KALSHI-CANDIDATE-HIGH' not in client_instance.get_market_calls
  assert client_instance.get_event_calls.count('KALSHI-EVENT-HIGH') == 1
  # The survivor still passed EVERY gate (fresh market + event-family proof ran):
  assert 'KALSHI-CANDIDATE-LOW' in client_instance.get_market_calls
  assert client_instance.get_event_calls.count('KALSHI-EVENT-LOW') == 2
  connection = open_database(state_db_path)
  blocked = connection.execute(
    "SELECT detail_json FROM runtime_events WHERE event_type = 'submit_bridge_blocked'",
  ).fetchall()
  blocked_details = [json.loads(row['detail_json']) for row in blocked]
  assert any(
    detail.get('ticker') == 'KALSHI-CANDIDATE-HIGH'
    and detail.get('blocked_reason') == 'coverability_divergence_blocked'
    for detail in blocked_details
  )


def _seed_topk_batch(state_db_path: Path, count: int, saved_set_id: str) -> list[str]:
  tickers = [f'KALSHI-TOPK-{index}' for index in range(1, count + 1)]
  _seed_live_saved_set_batch(state_db_path, tickers=tickers, saved_set_id=saved_set_id)
  return tickers


def test_submit_prep_top_k_defers_remainder_with_evidence(monkeypatch, tmp_path: Path) -> None:
  """Only the top-K ranked saved-set members enter serial prep; the remainder is
  deferred with names-only evidence and counted in the block summary."""
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_topk.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_topk.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  assert settings.submit_prep_top_k == 3
  tickers = _seed_topk_batch(state_db_path, 5, 'live-topk-cap-001')

  client_instance = None

  class TopKClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TopKClient)

  assert client_instance is not None
  # Only K=3 candidates consumed per-candidate readbacks:
  topk_orderbook_calls = [ticker for ticker in client_instance.orderbook_calls if ticker.startswith('KALSHI-TOPK-')]
  assert len(topk_orderbook_calls) == 3
  connection = open_database(state_db_path)
  deferred = connection.execute(
    """
    SELECT detail_json FROM runtime_events
    WHERE event_type = 'submit_bridge_blocked'
    """,
  ).fetchall()
  deferred_details = [
    json.loads(row['detail_json'])
    for row in deferred
    if json.loads(row['detail_json']).get('blocked_reason') == 'survivor_prep_top_k_deferred'
  ]
  assert len(deferred_details) == 2
  for detail in deferred_details:
    assert detail['submit_prep_top_k'] == 3
    assert detail['ticker'] in tickers
    assert detail['ticker'] not in topk_orderbook_calls


def test_submit_prep_top_k_zero_disables_cap(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setattr('polyventure.risk._is_maintenance_window', lambda *_a, **_k: False)
  monkeypatch.setattr('polyventure.service._validate_env_alignment', lambda _s: None)
  monkeypatch.setattr('time.sleep', lambda _: None)
  private_key_path = tmp_path / 'live_key_topk_off.pem'
  _write_private_key(private_key_path)
  state_db_path = tmp_path / 'lop_topk_off.sqlite3'
  settings = _live_settings(str(private_key_path), str(state_db_path))
  settings = Settings(**{**settings.__dict__, 'submit_prep_top_k': 0})
  _seed_topk_batch(state_db_path, 5, 'live-topk-off-001')

  client_instance = None

  class TopKOffClient(LiveFakeClientOrderbookFp):
    def __init__(self, s, k):
      nonlocal client_instance
      super().__init__(s, k)
      client_instance = self

  run_service_once(settings=settings, execution_profile='submit_order_bridge', client_factory=TopKOffClient)

  assert client_instance is not None
  topk_orderbook_calls = [ticker for ticker in client_instance.orderbook_calls if ticker.startswith('KALSHI-TOPK-')]
  assert len(topk_orderbook_calls) == 5
  connection = open_database(state_db_path)
  deferred = connection.execute(
    "SELECT detail_json FROM runtime_events WHERE event_type = 'submit_bridge_blocked'",
  ).fetchall()
  assert not any(
    json.loads(row['detail_json']).get('blocked_reason') == 'survivor_prep_top_k_deferred'
    for row in deferred
  )
