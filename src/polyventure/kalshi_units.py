"""Outbound order money-units conversion boundary (dollars -> Kalshi V2 wire units).

The strategy/edge layer represents money as dollar ``Decimal`` values. The Kalshi V2
order endpoint expects a fixed-point dollar **string** for price (4 decimals, cent-granular,
e.g. ``"0.5400"``), a Count-FP **string** for count (e.g. ``"1.00"``), and a YES-book
``bid``/``ask`` side (buy-NO is placed as ``ask`` at ``1 - no_price``). The order-group limit
uses the same Count-FP string.

Governance (load-bearing): no rounding and no silent substitution on a money surface. A
value that is not exactly representable in Kalshi units raises ``KalshiUnitError`` so the
caller fails closed (the order is not placed) rather than altering an operator money value.
"""

from __future__ import annotations

from decimal import Decimal


class KalshiUnitError(ValueError):
  """Raised when an outbound order money value cannot be represented exactly in Kalshi
  wire units. Fail-closed signal: on this error the order must not be placed."""

  def __init__(self, reason: str) -> None:
    super().__init__(reason)
    self.reason = reason


SELF_TRADE_PREVENTION_TYPE = 'taker_at_cross'

_VALID_TIME_IN_FORCE = frozenset({'fill_or_kill', 'good_till_canceled', 'immediate_or_cancel'})
_MIN_PRICE = Decimal('0.01')
_MAX_PRICE = Decimal('0.99')


def price_dollars_to_fp4(price_dollars: Decimal) -> str:
  """Convert a cent-granular dollar price to a V2 fixed-point string (4 decimals,
  e.g. ``Decimal('0.54')`` -> ``'0.5400'``). Fails closed on a non-decimal input, an
  out-of-range value, a non-cent-exact value, or any round-trip drift — never rounds."""
  if not isinstance(price_dollars, Decimal):
    raise KalshiUnitError('price_not_decimal')
  if not (_MIN_PRICE <= price_dollars <= _MAX_PRICE):
    raise KalshiUnitError('price_out_of_range')
  cents = price_dollars * Decimal('100')
  if cents != cents.to_integral_value():
    raise KalshiUnitError('price_not_cent_exact')
  wire = format(price_dollars, '.4f')
  if Decimal(wire) != price_dollars:
    raise KalshiUnitError('price_roundtrip_violation')
  return wire


def count_contracts_to_int(count: Decimal) -> int:
  """Convert a contract count to a positive integer. Fails closed on a fractional or
  non-positive count."""
  if not isinstance(count, Decimal):
    raise KalshiUnitError('count_not_decimal')
  if count != count.to_integral_value():
    raise KalshiUnitError('count_not_integral')
  count_int = int(count)
  if count_int <= 0:
    raise KalshiUnitError('count_not_positive')
  return count_int


def count_contracts_to_wire(count: Decimal) -> str:
  """Convert a contract count to Kalshi's Count-FP wire form (e.g. ``"1.00"``)."""
  return f'{count_contracts_to_int(count)}.00'


def group_limit_to_wire(contracts_limit_fp: Decimal) -> str:
  """Convert an order-group contract limit to Kalshi's Count-FP form (e.g. ``"2.00"``)."""
  return f'{count_contracts_to_int(contracts_limit_fp)}.00'


def leg_to_v2_side(leg: str) -> str:
  """Map a domain leg name to the Kalshi V2 YES-book side.
  ``'yes'`` -> ``'bid'`` (buy YES); ``'no'`` -> ``'ask'`` (sell YES = buy NO)."""
  if leg == 'yes':
    return 'bid'
  if leg == 'no':
    return 'ask'
  raise KalshiUnitError('side_unknown')


def outbound_leg_price_dollars(leg: str, price_dollars: Decimal) -> Decimal:
  """Return the YES-book price for a leg. YES leg passes through; NO leg maps to
  ``1 - no_price`` (the equivalent YES ask price)."""
  if leg == 'yes':
    return price_dollars
  if leg == 'no':
    return Decimal('1') - price_dollars
  raise KalshiUnitError('side_unknown')


def restore_leg_price_dollars(leg: str, yes_book_price_dollars: Decimal) -> Decimal:
  """Invert the YES-book mapping back to the domain leg price. Symmetric inverse of
  ``outbound_leg_price_dollars``: YES passes through; NO maps to ``1 - reported``."""
  if leg == 'yes':
    return yes_book_price_dollars
  if leg == 'no':
    return Decimal('1') - yes_book_price_dollars
  raise KalshiUnitError('side_unknown')


def validate_time_in_force(tif: str) -> str:
  """Pass through a valid time-in-force value; fail closed on an unrecognised value."""
  if tif not in _VALID_TIME_IN_FORCE:
    raise KalshiUnitError('time_in_force_invalid')
  return tif
