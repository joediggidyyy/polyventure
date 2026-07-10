"""Shared native Ed25519 signing/verification primitives for retained evidence (Lane L5).

This module is the single home for the signed-contract key material and the generic
"sign / verify a retained record" primitives, so that surfaces which cannot import ``web_app``
(notably ``service.py``, which ``web_app`` imports) can still produce signed money/datapack
evidence without a circular dependency.

Native ``cryptography`` Ed25519 only on the v1 crypto path (no external signing harness). Keys come
from the gitignored ``.secrets/signing/<key_id>.pem`` and the tracked
``.secrets/signed_contract_trust_store.json`` (or their env overrides), matching the verifier in
``web_app``. Fail closed everywhere: a missing/ambiguous key yields an explicitly UNSIGNED record,
never a forged-trust one.
"""
from __future__ import annotations

import base64
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

PROJECT_ROOT = Path(__file__).resolve().parents[2]

SIGNED_EVIDENCE_SCHEMA_VERSION = 'signed-evidence.v1'

_TRUST_STORE_ENV = 'POLYVENTURE_SIGNED_CONTRACT_TRUST_STORE'
_SIGNING_KEY_ENV = 'POLYVENTURE_SIGNED_CONTRACT_SIGNING_KEY'


def canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
  """Deterministic canonical serialization (locked params: sorted keys, compact, ensure_ascii=False)."""
  return json.dumps(payload, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')


def read_trust_store() -> dict[str, Any] | None:
  """Parse the signed-contract trust store (env override, then the canonical .secrets path)."""
  env_override = os.environ.get(_TRUST_STORE_ENV, '').strip()
  store_paths = []
  if env_override:
    store_paths.append(Path(env_override))
  store_paths.append(PROJECT_ROOT / '.secrets' / 'signed_contract_trust_store.json')
  for path in store_paths:
    if path.exists():
      try:
        loaded = json.loads(path.read_text(encoding='utf-8'))
      except json.JSONDecodeError:
        return None
      return loaded if isinstance(loaded, dict) else None
  return None


def trusted_verification_keys() -> dict[str, str]:
  """Return ``key_id -> public_key_b64`` for the active, non-revoked trust-store entries.

  A key_id present in ``revoked_keys`` is excluded even if it also appears in ``active_keys`` (rotation
  policy: a revoked key must fail verification regardless of signature validity).
  """
  store = read_trust_store()
  if not store:
    return {}
  active_keys = store.get('active_keys', [])
  if not isinstance(active_keys, list):
    return {}
  revoked = store.get('revoked_keys', [])
  revoked_ids = {
    str(entry.get('key_id'))
    for entry in revoked
    if isinstance(entry, dict) and entry.get('key_id')
  } if isinstance(revoked, list) else set()
  return {
    key['key_id']: key['public_key_b64']
    for key in active_keys
    if isinstance(key, dict)
    and 'key_id' in key
    and 'public_key_b64' in key
    and key['key_id'] not in revoked_ids
  }


def resolve_active_signer_key_id(trusted_keys: Mapping[str, str]) -> str | None:
  """Resolve which active key signs new evidence. Fail closed when ambiguous.

  The trust store's ``last_activated_key_id`` wins when it is still active (rotation overlap);
  otherwise a single active key is used; multiple active keys with no resolvable last-activated id
  is ambiguous and refuses to sign.
  """
  if not trusted_keys:
    return None
  store = read_trust_store() or {}
  last_activated = str((store.get('metadata') or {}).get('last_activated_key_id') or '').strip()
  if last_activated and last_activated in trusted_keys:
    return last_activated
  if len(trusted_keys) == 1:
    return next(iter(trusted_keys))
  return None


def load_signing_key() -> tuple[Any, str] | None:
  """Resolve the active Ed25519 signing key and its key_id, or None (fail closed).

  WHICH key = the active trust-store entry. WHERE the private key lives =
  ``POLYVENTURE_SIGNED_CONTRACT_SIGNING_KEY`` (explicit path override) else the canonical gitignored
  location ``.secrets/signing/<key_id>.pem``. No silent fallback to a different key.
  """
  from cryptography.hazmat.primitives import serialization
  from cryptography.hazmat.primitives.asymmetric import ed25519

  signer_key_id = resolve_active_signer_key_id(trusted_verification_keys())
  if signer_key_id is None:
    return None
  env_path = os.environ.get(_SIGNING_KEY_ENV, '').strip()
  key_path = Path(env_path) if env_path else (PROJECT_ROOT / '.secrets' / 'signing' / f'{signer_key_id}.pem')
  if not key_path.is_file():
    return None
  try:
    private_key = serialization.load_pem_private_key(key_path.read_bytes(), password=None)
  except (ValueError, TypeError, OSError):
    return None
  if not isinstance(private_key, ed25519.Ed25519PrivateKey):
    return None
  return private_key, signer_key_id


def sign_evidence_record(payload: Mapping[str, Any]) -> dict[str, Any]:
  """Produce a signature block over a retained record's canonical bytes.

  Always returns a checksum; returns a signature when a signing key is provisioned, else an explicit
  ``signature_status='unsigned'`` (fail closed: the artifact is marked UNSIGNED, never forged-trust).
  """
  canonical = canonical_json_bytes(payload)
  checksum_sha256 = hashlib.sha256(canonical).hexdigest()
  block: dict[str, Any] = {
    'schema_version': SIGNED_EVIDENCE_SCHEMA_VERSION,
    'checksum_sha256': checksum_sha256,
  }
  signer = load_signing_key()
  if signer is None:
    block['signature_status'] = 'unsigned'
    return block
  private_key, signer_key_id = signer
  block['signature_alg'] = 'ed25519'
  block['signature_b64'] = base64.b64encode(private_key.sign(canonical)).decode('ascii')
  block['signer_key_id'] = signer_key_id
  block['signed_at_utc'] = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')
  block['signature_status'] = 'signed'
  return block


def verify_evidence_record(payload: Mapping[str, Any], signature_block: Mapping[str, Any]) -> tuple[bool, str | None]:
  """Verify a signed evidence record against the trust store.

  Returns ``(valid, failure_code)``. Fail closed: a missing/unsigned block, checksum mismatch,
  unknown/revoked key, algorithm downgrade, or bad signature all return ``(False, code)``.
  """
  from cryptography.exceptions import InvalidSignature
  from cryptography.hazmat.primitives import serialization

  if not isinstance(signature_block, Mapping):
    return False, 'missing_signature_block'
  if signature_block.get('signature_status') != 'signed':
    return False, 'unsigned'
  if signature_block.get('signature_alg') != 'ed25519':
    return False, 'algorithm_downgrade'

  canonical = canonical_json_bytes(payload)
  if hashlib.sha256(canonical).hexdigest() != str(signature_block.get('checksum_sha256') or ''):
    return False, 'checksum_mismatch'

  signer_key_id = str(signature_block.get('signer_key_id') or '')
  trusted_keys = trusted_verification_keys()
  if signer_key_id not in trusted_keys:
    return False, 'unknown_key'

  try:
    public_key = serialization.load_der_public_key(base64.b64decode(trusted_keys[signer_key_id]))
    public_key.verify(base64.b64decode(str(signature_block.get('signature_b64') or '')), canonical)
  except (InvalidSignature, ValueError, TypeError):
    return False, 'invalid_signature'
  except Exception:
    return False, 'invalid_signature'
  return True, None
