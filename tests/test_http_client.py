from __future__ import annotations

from polyventure.http_client import KalshiHttpError, _http_error_for_status, kalshi_error_safe_detail


def test_http_error_for_status_preserves_safe_kalshi_request_detail() -> None:
  exc = _http_error_for_status(401, method='GET', endpoint='/account/limits')

  assert exc.reason_code == 'auth_failed'
  assert exc.method == 'GET'
  assert exc.endpoint == '/account/limits'
  assert exc.status_code == 401
  assert kalshi_error_safe_detail(exc) == {
    'reason_code': 'auth_failed',
    'kalshi_method': 'GET',
    'kalshi_endpoint': '/account/limits',
    'kalshi_status_code': 401,
  }


def test_kalshi_error_safe_detail_omits_unset_values() -> None:
  exc = KalshiHttpError(
    'network_timeout',
    'Network connectivity failed.',
    'Retry after connectivity is restored.',
    method='GET',
    endpoint='/portfolio/balance',
  )

  assert kalshi_error_safe_detail(exc) == {
    'reason_code': 'network_timeout',
    'kalshi_method': 'GET',
    'kalshi_endpoint': '/portfolio/balance',
  }
