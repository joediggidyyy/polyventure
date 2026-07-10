from __future__ import annotations

import base64
import time
from pathlib import Path
from urllib.parse import urlsplit

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa


RSAPrivateKey = rsa.RSAPrivateKey


def load_private_key(path: str | Path) -> RSAPrivateKey:
  pem_bytes = Path(path).read_bytes()
  key = serialization.load_pem_private_key(pem_bytes, password=None)
  if not isinstance(key, rsa.RSAPrivateKey):
    raise TypeError('Loaded key is not an RSA private key.')
  return key


def normalize_signing_path(path: str) -> str:
  parsed = urlsplit(path)
  if parsed.scheme and parsed.path:
    return parsed.path
  if '?' in path:
    return path.split('?', 1)[0]
  return path


def create_signature(
  private_key: RSAPrivateKey,
  timestamp_ms: str,
  method: str,
  path: str,
) -> str:
  normalized_path = normalize_signing_path(path)
  message = f'{timestamp_ms}{method.upper()}{normalized_path}'.encode('utf-8')
  signature = private_key.sign(
    message,
    padding.PSS(
      mgf=padding.MGF1(hashes.SHA256()),
      salt_length=padding.PSS.DIGEST_LENGTH,
    ),
    hashes.SHA256(),
  )
  return base64.b64encode(signature).decode('utf-8')


def build_auth_headers(
  private_key: RSAPrivateKey,
  api_key_id: str,
  method: str,
  path: str,
) -> dict[str, str]:
  timestamp_ms = str(int(time.time() * 1000))
  return {
    'Content-Type': 'application/json',
    'KALSHI-ACCESS-KEY': api_key_id,
    'KALSHI-ACCESS-SIGNATURE': create_signature(
      private_key,
      timestamp_ms,
      method,
      path,
    ),
    'KALSHI-ACCESS-TIMESTAMP': timestamp_ms,
  }
