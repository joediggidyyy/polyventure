"""Unit tests for the shared native Ed25519 evidence primitives (Lane L5a)."""
from __future__ import annotations

import base64
from typing import Any
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from polyventure import signed_evidence


def _keypair() -> tuple[Any, str]:
  private_key = Ed25519PrivateKey.generate()
  public_der = private_key.public_key().public_bytes(
    serialization.Encoding.DER,
    serialization.PublicFormat.SubjectPublicKeyInfo,
  )
  return private_key, base64.b64encode(public_der).decode('ascii')


def test_sign_then_verify_round_trips() -> None:
  private_key, public_b64 = _keypair()
  payload = {'terminal_state': 'CANCELED', 'operation_lane': 'live', 'pair_id': 'p-1'}
  with patch.object(signed_evidence, 'trusted_verification_keys', return_value={'k1': public_b64}), \
       patch.object(signed_evidence, 'load_signing_key', return_value=(private_key, 'k1')):
    block = signed_evidence.sign_evidence_record(payload)
    assert block['signature_status'] == 'signed'
    assert block['signer_key_id'] == 'k1'
    assert len(block['checksum_sha256']) == 64
    valid, code = signed_evidence.verify_evidence_record(payload, block)
    assert valid is True
    assert code is None


def test_unsigned_when_no_signing_key() -> None:
  payload = {'terminal_state': 'RESTING_BOTH'}
  with patch.object(signed_evidence, 'load_signing_key', return_value=None):
    block = signed_evidence.sign_evidence_record(payload)
    assert block['signature_status'] == 'unsigned'
    assert 'signature_b64' not in block
    assert len(block['checksum_sha256']) == 64
    # An unsigned block never verifies as valid (fail closed).
    valid, code = signed_evidence.verify_evidence_record(payload, block)
    assert valid is False
    assert code == 'unsigned'


def test_verify_rejects_tamper_and_downgrade() -> None:
  private_key, public_b64 = _keypair()
  payload = {'terminal_state': 'CANCELED', 'operation_lane': 'live'}
  with patch.object(signed_evidence, 'trusted_verification_keys', return_value={'k1': public_b64}), \
       patch.object(signed_evidence, 'load_signing_key', return_value=(private_key, 'k1')):
    block = signed_evidence.sign_evidence_record(payload)

    # Payload tamper -> checksum mismatch.
    assert signed_evidence.verify_evidence_record({**payload, 'operation_lane': 'sandbox'}, block)[1] == 'checksum_mismatch'
    # Signature tamper -> invalid signature.
    forged = {**block, 'signature_b64': base64.b64encode(b'\x00' * 64).decode('ascii')}
    assert signed_evidence.verify_evidence_record(payload, forged)[1] == 'invalid_signature'
    # Algorithm downgrade -> rejected before any verify.
    downgraded = {**block, 'signature_alg': 'sha256-raw'}
    assert signed_evidence.verify_evidence_record(payload, downgraded)[1] == 'algorithm_downgrade'

  # Unknown signer (key not in trust store) -> rejected.
  with patch.object(signed_evidence, 'trusted_verification_keys', return_value={}):
    assert signed_evidence.verify_evidence_record(payload, block)[1] == 'unknown_key'


def test_resolve_active_signer_key_id_fails_closed_on_ambiguity() -> None:
  # Single active key -> used.
  with patch.object(signed_evidence, 'read_trust_store', return_value={'metadata': {}}):
    assert signed_evidence.resolve_active_signer_key_id({'only': 'b64'}) == 'only'
  # Multiple active keys, last_activated resolvable -> that one.
  with patch.object(signed_evidence, 'read_trust_store', return_value={'metadata': {'last_activated_key_id': 'kB'}}):
    assert signed_evidence.resolve_active_signer_key_id({'kA': 'b64', 'kB': 'b64'}) == 'kB'
  # Multiple active keys, no resolvable last_activated -> ambiguous, refuse.
  with patch.object(signed_evidence, 'read_trust_store', return_value={'metadata': {}}):
    assert signed_evidence.resolve_active_signer_key_id({'kA': 'b64', 'kB': 'b64'}) is None
  # No keys -> None.
  assert signed_evidence.resolve_active_signer_key_id({}) is None
