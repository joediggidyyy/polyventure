"""Market-data normalization helpers for later milestones."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from .types import MarketSnapshot, OrderbookSnapshot
from .websocket_client import normalize_orderbook_snapshot as normalize_orderbook_payload


def fetch_open_markets(
  client: Any,
  *,
  limit: int = 1000,
  min_close_ts: int | None = None,
  max_close_ts: int | None = None,
) -> list[MarketSnapshot]:
  if limit <= 0:
    return []
  cursor: str | None = None
  markets: list[MarketSnapshot] = []
  while True:
    remaining = max(limit - len(markets), 0)
    if remaining <= 0:
      break
    page, cursor = client.get_markets(
      status='open', limit=remaining, cursor=cursor,
      min_close_ts=min_close_ts, max_close_ts=max_close_ts,
    )
    if not page:
      if not cursor:
        break
      continue
    markets.extend(page[:remaining])
    if len(markets) >= limit or not cursor:
      break
  return markets


def enrich_with_orderbook(client: Any, ticker: str, *, depth: int = 0) -> tuple[MarketSnapshot, OrderbookSnapshot]:
  return client.get_market(ticker), client.get_orderbook(ticker, depth=depth)


def compute_seconds_to_close(market: MarketSnapshot, now: datetime) -> int:
  if market.close_time is None:
    raise ValueError('Market close_time is required to compute seconds_to_close.')
  return int((market.close_time.astimezone(UTC) - now.astimezone(UTC)).total_seconds())


def derive_implied_asks(orderbook: OrderbookSnapshot) -> tuple[object, object]:
  return orderbook.best_yes_ask_implied, orderbook.best_no_ask_implied


def normalize_market(raw_market: dict) -> MarketSnapshot:
  """Normalize a Kalshi market payload into the local snapshot type."""
  from .http_client import market_from_payload

  return market_from_payload(raw_market)


def normalize_orderbook_snapshot(ws_or_rest_payload: dict) -> OrderbookSnapshot:
  return normalize_orderbook_payload(ws_or_rest_payload)
