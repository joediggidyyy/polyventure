from __future__ import annotations

from cryptography.hazmat.primitives.asymmetric import rsa

from polyventure.auth import create_signature, normalize_signing_path


def test_normalize_signing_path_strips_query() -> None:
  assert normalize_signing_path('/trade-api/v2/markets?status=open') == '/trade-api/v2/markets'


def test_create_signature_returns_base64_text() -> None:
  private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
  signature = create_signature(
    private_key,
    '1714867200000',
    'GET',
    '/trade-api/v2/portfolio/balance?cursor=abc',
  )

  assert isinstance(signature, str)
  assert len(signature) > 20
