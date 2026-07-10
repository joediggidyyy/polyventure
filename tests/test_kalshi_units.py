from __future__ import annotations

from decimal import Decimal

import pytest

from polyventure.http_client import KalshiHttpClient, pair_position_from_payload, submitted_order_from_payload
from polyventure.kalshi_units import (
  KalshiUnitError,
  SELF_TRADE_PREVENTION_TYPE,
  count_contracts_to_int,
  count_contracts_to_wire,
  group_limit_to_wire,
  leg_to_v2_side,
  outbound_leg_price_dollars,
  price_dollars_to_fp4,
  restore_leg_price_dollars,
  validate_time_in_force,
)


# ---------------------------------------------------------------------------
# price_dollars_to_fp4
# ---------------------------------------------------------------------------

def test_price_fp4_exact() -> None:
  assert price_dollars_to_fp4(Decimal('0.54')) == '0.5400'
  assert price_dollars_to_fp4(Decimal('0.01')) == '0.0100'
  assert price_dollars_to_fp4(Decimal('0.99')) == '0.9900'
  assert price_dollars_to_fp4(Decimal('0.40')) == '0.4000'


def test_price_fp4_round_trip_no_drift() -> None:
  for dollars in (Decimal('0.05'), Decimal('0.50'), Decimal('0.73')):
    wire = price_dollars_to_fp4(dollars)
    assert Decimal(wire) == dollars


def test_price_fp4_non_cent_exact_fails_closed() -> None:
  with pytest.raises(KalshiUnitError) as exc:
    price_dollars_to_fp4(Decimal('0.545'))
  assert exc.value.reason == 'price_not_cent_exact'


def test_price_fp4_out_of_range_fails_closed() -> None:
  with pytest.raises(KalshiUnitError) as low:
    price_dollars_to_fp4(Decimal('0'))
  assert low.value.reason == 'price_out_of_range'
  with pytest.raises(KalshiUnitError) as high:
    price_dollars_to_fp4(Decimal('1.00'))
  assert high.value.reason == 'price_out_of_range'


def test_price_fp4_rejects_non_decimal() -> None:
  with pytest.raises(KalshiUnitError) as exc:
    price_dollars_to_fp4(0.54)  # type: ignore[arg-type]
  assert exc.value.reason == 'price_not_decimal'


# ---------------------------------------------------------------------------
# count helpers
# ---------------------------------------------------------------------------

def test_count_contracts_to_int_valid() -> None:
  assert count_contracts_to_int(Decimal('1')) == 1
  assert count_contracts_to_int(Decimal('10.00')) == 10


def test_count_contracts_to_int_fractional_fails_closed() -> None:
  with pytest.raises(KalshiUnitError) as exc:
    count_contracts_to_int(Decimal('1.5'))
  assert exc.value.reason == 'count_not_integral'


def test_count_contracts_to_int_non_positive_fails_closed() -> None:
  with pytest.raises(KalshiUnitError) as exc:
    count_contracts_to_int(Decimal('0'))
  assert exc.value.reason == 'count_not_positive'


def test_count_contracts_to_wire() -> None:
  assert count_contracts_to_wire(Decimal('1')) == '1.00'
  assert count_contracts_to_wire(Decimal('10')) == '10.00'


def test_group_limit_to_wire() -> None:
  assert group_limit_to_wire(Decimal('1')) == '1.00'
  assert group_limit_to_wire(Decimal('2')) == '2.00'
  assert group_limit_to_wire(Decimal('10.00')) == '10.00'


def test_group_limit_to_wire_fractional_fails_closed() -> None:
  with pytest.raises(KalshiUnitError):
    group_limit_to_wire(Decimal('1.5'))


# ---------------------------------------------------------------------------
# side + leg mapping
# ---------------------------------------------------------------------------

def test_leg_to_v2_side() -> None:
  assert leg_to_v2_side('yes') == 'bid'
  assert leg_to_v2_side('no') == 'ask'


def test_leg_to_v2_side_unknown_fails_closed() -> None:
  with pytest.raises(KalshiUnitError) as exc:
    leg_to_v2_side('maybe')
  assert exc.value.reason == 'side_unknown'


def test_outbound_leg_price_yes_passthrough() -> None:
  assert outbound_leg_price_dollars('yes', Decimal('0.54')) == Decimal('0.54')


def test_outbound_leg_price_no_complement() -> None:
  assert outbound_leg_price_dollars('no', Decimal('0.41')) == Decimal('0.59')


def test_restore_leg_price_no_round_trip() -> None:
  no_price = Decimal('0.41')
  outbound = outbound_leg_price_dollars('no', no_price)
  restored = restore_leg_price_dollars('no', outbound)
  assert restored == no_price


def test_restore_leg_price_yes_passthrough() -> None:
  assert restore_leg_price_dollars('yes', Decimal('0.54')) == Decimal('0.54')


# ---------------------------------------------------------------------------
# validate_time_in_force
# ---------------------------------------------------------------------------

def test_validate_time_in_force_valid() -> None:
  assert validate_time_in_force('good_till_canceled') == 'good_till_canceled'
  assert validate_time_in_force('fill_or_kill') == 'fill_or_kill'
  assert validate_time_in_force('immediate_or_cancel') == 'immediate_or_cancel'


def test_validate_time_in_force_invalid_fails_closed() -> None:
  with pytest.raises(KalshiUnitError) as exc:
    validate_time_in_force('day')
  assert exc.value.reason == 'time_in_force_invalid'


# ---------------------------------------------------------------------------
# Integration: real HTTP boundary emits V2 wire shape
# ---------------------------------------------------------------------------

class _CapturingHttpClient(KalshiHttpClient):
  def __post_init__(self) -> None:
    self.captured: list[tuple[str, str, object]] = []

  def _request(self, method: str, endpoint: str, **kwargs: object) -> dict:
    self.captured.append((method, endpoint, kwargs.get('json')))
    if endpoint == '/portfolio/order_groups/create':
      return {'order_group': {'order_group_id': 'grp-1'}}
    return {}


class _ReadCapturingHttpClient(KalshiHttpClient):
  def __post_init__(self) -> None:
    self.calls: list[tuple[str, str, object]] = []
    self.responses: list[dict] = []

  def _request(self, method: str, endpoint: str, **kwargs: object) -> dict:
    self.calls.append((method, endpoint, kwargs.get('params')))
    if self.responses:
      return self.responses.pop(0)
    return {}


def test_get_positions_filter_forwards_params_and_no_arg_unchanged() -> None:
  client = _ReadCapturingHttpClient(settings=None, private_key=None)  # type: ignore[arg-type]
  client.responses = [
    {'positions': []},
    {'positions': [{'ticker': 'KXTEST', 'position_fp': '-2.00', 'average_price_dollars': '0.25'}]},
  ]

  assert client.get_positions() == []
  filtered = client.get_positions(ticker='KXTEST', event_ticker='EVENT', count_filter='nonzero')

  assert client.calls[0] == ('GET', '/portfolio/positions', None)
  assert client.calls[1] == (
    'GET',
    '/portfolio/positions',
    {'ticker': 'KXTEST', 'event_ticker': 'EVENT', 'count_filter': 'nonzero'},
  )
  assert filtered[0].ticker == 'KXTEST'
  assert filtered[0].side == 'no'
  assert filtered[0].contract_count == Decimal('2.00')


def test_list_orders_resting_pagination_uses_portfolio_orders() -> None:
  client = _ReadCapturingHttpClient(settings=None, private_key=None)  # type: ignore[arg-type]
  client.responses = [
    {
      'orders': [
        {
          'order_id': 'order-1',
          'ticker': 'KXTEST',
          'side': 'bid',
          'status': 'resting',
          'initial_count_fp': '3.00',
          'remaining_count_fp': '2.00',
          'fill_count_fp': '1.00',
          'yes_price_dollars': '0.4000',
        }
      ],
      'cursor': 'next-page',
    },
    {
      'orders': [
        {
          'order_id': 'order-2',
          'ticker': 'KXTEST',
          'side': 'ask',
          'status': 'resting',
          'initial_count_fp': '4.00',
          'remaining_count_fp': '4.00',
          'fill_count_fp': '0.00',
          'no_price_dollars': '0.3000',
        }
      ],
    },
  ]

  orders = client.list_orders(ticker='KXTEST', status='resting', limit=1)

  assert [order.order_id for order in orders] == ['order-1', 'order-2']
  assert client.calls[0] == ('GET', '/portfolio/orders', {'ticker': 'KXTEST', 'status': 'resting', 'limit': 1})
  assert client.calls[1] == (
    'GET',
    '/portfolio/orders',
    {'ticker': 'KXTEST', 'status': 'resting', 'limit': 1, 'cursor': 'next-page'},
  )
  assert orders[0].remaining_count == Decimal('2.00')
  assert orders[1].side == 'no'


def test_real_client_order_create_emits_v2_wire_yes_leg() -> None:
  client = _CapturingHttpClient(settings=None, private_key=None)  # type: ignore[arg-type]

  group_id = client.create_order_group(contracts_limit_fp=Decimal('2'))
  client.create_order_v2(
    ticker='KALSHI-CANDIDATE-LOW',
    side='yes',
    yes_price=Decimal('0.54'),
    count=Decimal('1'),
    client_order_id='c-yes',
    time_in_force='good_till_canceled',
    post_only=False,
    cancel_order_on_pause=True,
    subaccount=0,
    order_group_id=group_id,
  )

  group_method, group_endpoint, group_body = client.captured[0]
  assert group_method == 'POST'
  assert group_endpoint == '/portfolio/order_groups/create'
  assert group_body == {'contracts_limit_fp': '2.00', 'subaccount': 0}

  order_method, order_endpoint, order_body = client.captured[1]
  assert order_method == 'POST'
  assert order_endpoint == '/portfolio/events/orders'
  assert order_body['side'] == 'bid'
  assert order_body['price'] == '0.5400'
  assert type(order_body['price']) is str
  assert order_body['count'] == '1.00'
  assert type(order_body['count']) is str
  assert order_body['self_trade_prevention_type'] == SELF_TRADE_PREVENTION_TYPE
  assert 'yes_price' not in order_body
  assert 'action' not in order_body
  assert 'type' not in order_body
  assert order_body['cancel_order_on_pause'] is True


def test_real_client_order_create_emits_v2_wire_no_leg() -> None:
  client = _CapturingHttpClient(settings=None, private_key=None)  # type: ignore[arg-type]

  client.create_order_v2(
    ticker='KALSHI-CANDIDATE-LOW',
    side='no',
    no_price=Decimal('0.41'),
    count=Decimal('1'),
    client_order_id='c-no',
    time_in_force='good_till_canceled',
    post_only=False,
    cancel_order_on_pause=True,
    subaccount=0,
    order_group_id='grp-1',
  )

  _, order_endpoint, order_body = client.captured[0]
  assert order_endpoint == '/portfolio/events/orders'
  assert order_body['side'] == 'ask'
  assert order_body['price'] == '0.5900'
  assert 'no_price' not in order_body


def test_real_client_batch_order_create_uses_batched_events_endpoint() -> None:
  client = _CapturingHttpClient(settings=None, private_key=None)  # type: ignore[arg-type]

  orders = client.create_orders_v2_batch(
    [
      {
        'ticker': 'KALSHI-CANDIDATE-LOW',
        'side': 'yes',
        'yes_price': Decimal('0.54'),
        'count': Decimal('2'),
        'client_order_id': 'batch-yes',
        'time_in_force': 'good_till_canceled',
        'post_only': True,
        'cancel_order_on_pause': True,
        'subaccount': 0,
        'order_group_id': 'grp-batch-1',
      },
      {
        'ticker': 'KALSHI-CANDIDATE-LOW',
        'side': 'no',
        'no_price': Decimal('0.41'),
        'count': Decimal('2'),
        'client_order_id': 'batch-no',
        'time_in_force': 'good_till_canceled',
        'post_only': True,
        'cancel_order_on_pause': True,
        'subaccount': 0,
        'order_group_id': 'grp-batch-1',
      },
    ]
  )

  method, endpoint, body = client.captured[0]
  assert method == 'POST'
  assert endpoint == '/portfolio/events/orders/batched'
  assert set(body) == {'orders'}
  assert [order['side'] for order in body['orders']] == ['bid', 'ask']
  assert [order['price'] for order in body['orders']] == ['0.5400', '0.5900']
  assert [order['client_order_id'] for order in body['orders']] == ['batch-yes', 'batch-no']
  assert all(order['self_trade_prevention_type'] == SELF_TRADE_PREVENTION_TYPE for order in body['orders'])
  assert orders == []


def test_real_client_batch_cancel_uses_batched_delete_endpoint() -> None:
  client = _CapturingHttpClient(settings=None, private_key=None)  # type: ignore[arg-type]

  client.cancel_orders_v2_batch(['order-1', 'order-2'], subaccount=3)

  method, endpoint, body = client.captured[0]
  assert method == 'DELETE'
  assert endpoint == '/portfolio/events/orders/batched'
  assert body == {
    'orders': [
      {'order_id': 'order-1', 'subaccount': 3, 'exchange_index': 0},
      {'order_id': 'order-2', 'subaccount': 3, 'exchange_index': 0},
    ]
  }


def test_submitted_order_from_payload_restores_ask_to_domain_no_price() -> None:
  order = submitted_order_from_payload(
    {
      'order_id': 'order-no',
      'client_order_id': 'pair-no',
      'ticker': 'KXTEMPNYCH-26JUN2420-T76.99',
      'side': 'ask',
      'price': '0.9900',
      'initial_count_fp': '100.00',
      'remaining_count_fp': '2.07',
      'fill_count_fp': '97.93',
      'status': 'resting',
      'created_at': '2026-06-24T23:48:15Z',
    }
  )

  assert order.side == 'no'
  assert order.price_dollars == Decimal('0.0100')
  assert order.fill_count == Decimal('97.93')


def test_real_client_order_create_fails_closed_on_non_cent_price() -> None:
  client = _CapturingHttpClient(settings=None, private_key=None)  # type: ignore[arg-type]
  with pytest.raises(KalshiUnitError) as exc:
    client.create_order_v2(
      ticker='KALSHI-CANDIDATE-LOW',
      side='yes',
      yes_price=Decimal('0.545'),
      count=Decimal('1'),
      client_order_id='c-yes',
      time_in_force='good_till_canceled',
      post_only=False,
      cancel_order_on_pause=True,
      subaccount=0,
      order_group_id='grp-1',
    )
  assert exc.value.reason == 'price_not_cent_exact'
  assert client.captured == []


def test_real_client_cancel_uses_delete_on_events_path() -> None:
  client = _CapturingHttpClient(settings=None, private_key=None)  # type: ignore[arg-type]
  client.cancel_order_v2('order-abc-123')
  method, endpoint, _ = client.captured[0]
  assert method == 'DELETE'
  assert endpoint == '/portfolio/events/orders/order-abc-123'


def test_submitted_order_parser_preserves_kalshi_fixed_point_fill_truth() -> None:
  order = submitted_order_from_payload({
    'order_id': 'remote-yes-001',
    'client_order_id': 'client-yes-001',
    'ticker': 'KXTEST',
    'side': 'bid',
    'status': 'executed',
    'initial_count_fp': '10.00',
    'fill_count_fp': '10.00',
    'remaining_count_fp': '0.00',
    'yes_price_dollars': '0.5500',
  })

  assert order.contract_count == Decimal('10.00')
  assert order.fill_count == Decimal('10.00')
  assert order.remaining_count == Decimal('0.00')
  assert order.price_dollars == Decimal('0.5500')
  assert order.status == 'executed'


def test_submitted_order_parser_uses_no_leg_domain_price() -> None:
  order = submitted_order_from_payload({
    'order_id': 'remote-no-001',
    'client_order_id': 'client-no-001',
    'ticker': 'KXTEST',
    'side': 'ask',
    'status': 'executed',
    'initial_count_fp': '10.00',
    'fill_count_fp': '10.00',
    'remaining_count_fp': '0.00',
    'no_price_dollars': '0.0200',
  })

  assert order.contract_count == Decimal('10.00')
  assert order.fill_count == Decimal('10.00')
  assert order.remaining_count == Decimal('0.00')
  assert order.price_dollars == Decimal('0.0200')


def test_position_parser_preserves_fixed_point_contract_count() -> None:
  position = pair_position_from_payload({
    'ticker': 'KXTEST',
    'side': 'no',
    'contract_count_fp': '2.00',
    'average_price_dollars': '0.2500',
  })

  assert position.contract_count == Decimal('2.00')
  assert position.average_price_dollars == Decimal('0.2500')
