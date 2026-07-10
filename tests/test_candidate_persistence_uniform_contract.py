"""Lane 0 — uniform candidate-persistence contract.

Plan: CANDIDATE_PERSISTENCE_UNIFORM_CONTRACT_BMAP_2026-06-19.

Proves the two structural preconditions for the candidate single-source-of-truth:
  0.1  one canonical, display-stable identity — the same candidate yields the same
       candidate_uid/key through BOTH writer paths (candidate-math in service.py and
       the card/save_selection path in web_app.py); identity is stable across
       tier/rank changes and unique across distinct selections.
  0.2  every persisted candidate-math evidence row is card-renderable — it carries the
       full display fields AND the scoring fields, with the canonical identity.
"""
from __future__ import annotations

from polyventure import web_app
from polyventure.candidate_identity import canonical_candidate_key, canonical_candidate_uid
from polyventure.config import Settings
from polyventure.service import (
  _build_candidate_math_evidence_contract,
  _candidate_key,
  _candidate_uid,
)


def _candidate(**overrides):
  base = {
    'ticker': 'KXBTC15M-26JUN1930-30',
    'event_ticker': 'KXBTC15M-26JUN1930',
    'yes_sub_title': 'Target Price: $1.1338',
    'qualifier_tier': 'live_qualifying',
    'rank': 1,
    'edge_net_per_contract': '0.04',
    'edge_gross_per_contract': '0.05',
    'liquidity_score': '210',
    'density_weight': '1.0',
    'seconds_to_close': 600,
    'title': 'BTC above target',
  }
  base.update(overrides)
  return base


def _settings() -> Settings:
  return Settings(
    kalshi_env='demo',
    api_key_id='key-id',
    private_key_file='dummy.pem',
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
    operation_lane='sandbox',
  )


# --- 0.1 identity ---------------------------------------------------------------

def test_canonical_identity_is_display_stable_composite() -> None:
  cand = _candidate()
  assert canonical_candidate_uid(cand) == 'KXBTC15M-26JUN1930-30::KXBTC15M-26JUN1930::Target Price: $1.1338'
  assert canonical_candidate_key(cand) == canonical_candidate_uid(cand)


def test_both_writer_paths_produce_same_identity() -> None:
  cand = _candidate()
  # candidate-math path (service) and card/save_selection path (web_app) must agree.
  assert _candidate_uid(cand) == web_app._candidate_review_uid(cand, 0)
  assert _candidate_key(cand) == web_app._candidate_review_key(cand, 0)
  assert _candidate_uid(cand) == canonical_candidate_uid(cand)


def test_identity_stable_across_tier_and_rank_changes() -> None:
  base = _candidate(qualifier_tier='live_qualifying', rank=1)
  reranked = _candidate(qualifier_tier='near_miss', rank=9)
  # tier/rank are NOT part of identity — same market+selection -> same id.
  assert _candidate_uid(base) == _candidate_uid(reranked)


def test_distinct_selections_have_distinct_identity() -> None:
  yes = _candidate(yes_sub_title='Target Price: $1.1338', no_sub_title='')
  other = _candidate(yes_sub_title='Target Price: $1,707.31', no_sub_title='')
  assert _candidate_uid(yes) != _candidate_uid(other)


# --- 0.2 uniform display-bearing rows ------------------------------------------

def test_candidate_math_rows_are_card_renderable_with_scoring() -> None:
  cand = _candidate()
  contract = _build_candidate_math_evidence_contract([cand], context={}, settings=_settings())
  rows = contract['candidate_evidence_rows']
  assert len(rows) == 1
  row = rows[0]
  # display fields present (card-renderable)
  for field in ('event_ticker', 'yes_sub_title', 'title'):
    assert row.get(field) == cand[field], f'display field {field!r} missing from candidate-math row'
  # scoring present
  assert 'feature_vector' in row and 'composite_score' in row
  # canonical identity, shared with the card path
  assert row['candidate_uid'] == canonical_candidate_uid(cand)
  assert row['candidate_uid'] == web_app._candidate_review_uid(cand, 0)
  assert row['qualifier_tier'] == 'live_qualifying'
  assert row['review_row_origin'] == 'current'


def test_candidate_math_and_card_rows_share_identity_for_same_candidate() -> None:
  cand = _candidate()
  contract = _build_candidate_math_evidence_contract([cand], context={}, settings=_settings())
  math_uid = contract['candidate_evidence_rows'][0]['candidate_uid']
  card_uid = web_app._candidate_review_uid(cand, 0)
  assert math_uid == card_uid
