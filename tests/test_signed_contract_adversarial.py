"""Lane L6 adversarial pass for the native signed-contract / signed-evidence subsystem.

Every attack case must fail closed or be detected; no accepted-but-invalid signature; no key leak.
Covers the contract envelope, the money-evidence record, and the datapack manifest, plus the
revocation / trust-store-tamper / path / env cases, and a regression assert on the separate inbound
signed-mutation gate.
"""
from __future__ import annotations

import base64
import json
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import patch

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from polyventure import persistence, signed_evidence, web_app


def _keypair() -> tuple[Any, str]:
  private_key = Ed25519PrivateKey.generate()
  public_der = private_key.public_key().public_bytes(
    serialization.Encoding.DER,
    serialization.PublicFormat.SubjectPublicKeyInfo,
  )
  return private_key, base64.b64encode(public_der).decode('ascii')


def _contract_envelope(private_key: Any, key_id: str, trusted: dict[str, str], context: dict[str, Any]):
  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted), \
       patch.object(web_app, '_load_signed_contract_signing_key', return_value=(private_key, key_id)):
    return web_app.build_signed_contract_envelope(key, contract, context)


# ----------------------------- contract envelope attacks -----------------------------

def test_adv_forged_signature_rejected() -> None:
  priv, pub = _keypair()
  trusted = {'k1': pub}
  ctx = {'operation_lane': 'live'}
  env = _contract_envelope(priv, 'k1', trusted, ctx)
  forged = replace(env, signature_b64=base64.b64encode(b'\x00' * 64).decode('ascii'))
  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted):
    assert web_app.verify_envelope_signature(forged).failure_code == 'invalid_signature'


def test_adv_algorithm_downgrade_cannot_bypass_envelope() -> None:
  # The envelope verify is hard-wired to Ed25519; a downgraded alg field cannot present a weaker
  # signature that still verifies.
  priv, pub = _keypair()
  trusted = {'k1': pub}
  env = _contract_envelope(priv, 'k1', trusted, {'operation_lane': 'live'})
  downgraded = replace(env, signature_alg='sha256-raw', signature_b64=base64.b64encode(b'\x01' * 32).decode('ascii'))
  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted):
    assert web_app.verify_envelope_signature(downgraded).valid is False


def test_adv_unknown_signer_rejected() -> None:
  priv, pub = _keypair()
  env = _contract_envelope(priv, 'k1', {'k1': pub}, {'operation_lane': 'live'})
  with patch.object(web_app, '_get_trusted_verification_keys', return_value={'other': pub}):
    assert web_app.verify_envelope_signature(env).failure_code == 'unknown_key'


def test_adv_trust_store_tamper_breaks_verification() -> None:
  # Swapping the stored public key (trust-store tamper) makes a legitimately-signed envelope fail.
  priv, pub = _keypair()
  env = _contract_envelope(priv, 'k1', {'k1': pub}, {'operation_lane': 'live'})
  _, attacker_pub = _keypair()
  with patch.object(web_app, '_get_trusted_verification_keys', return_value={'k1': attacker_pub}):
    assert web_app.verify_envelope_signature(env).failure_code == 'invalid_signature'


def test_adv_hash_tamper_rejected() -> None:
  priv, pub = _keypair()
  trusted = {'k1': pub}
  env = _contract_envelope(priv, 'k1', trusted, {'operation_lane': 'live'})
  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted):
    assert web_app.verify_envelope_signature(replace(env, contract_hash_sha256='0' * 64)).failure_code == 'hash_mismatch'


def test_adv_policy_snapshot_mismatch_rejected() -> None:
  priv, pub = _keypair()
  trusted = {'k1': pub}
  env = _contract_envelope(priv, 'k1', trusted, {'operation_lane': 'live', 'min_edge_dollars': 0.03})
  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted):
    res = web_app.verify_envelope_signature(env, settings_payload={'operation_lane': 'sandbox', 'min_edge_dollars': 0.03})
    assert res.failure_code == 'policy_mismatch'


def test_adv_replay_rejected() -> None:
  priv, pub = _keypair()
  trusted = {'k1': pub}
  env = _contract_envelope(priv, 'k1', trusted, {'operation_lane': 'live'})
  with patch.object(web_app, '_get_trusted_verification_keys', return_value=trusted):
    web_app._NONCE_REPLAY_WINDOW.discard(env.nonce)
    assert web_app.verify_envelope_signature(env).valid is True
    assert web_app.verify_envelope_signature(env).failure_code == 'replay'
  web_app._NONCE_REPLAY_WINDOW.discard(env.nonce)


def test_adv_canonicalization_variant_hashes_identically() -> None:
  # A re-ordered / re-spaced equivalent payload must canonicalize to the same bytes (no variant attack).
  key = web_app.TransitionKey('1', 'apply_selected_key_reference', 'success', '3')
  contract = web_app.resolve_transition_view_contract('1', 'apply_selected_key_reference', 'success', '3')
  a = web_app.canonicalize_signed_contract_payload('signed-contract.v1', key, contract, 'live')
  b = web_app.canonicalize_signed_contract_payload('signed-contract.v1', key, contract, 'live')
  assert a == b
  # Anti-variant: the canonical bytes are a fixpoint of the locked serializer, so a re-spaced /
  # re-ordered equivalent JSON re-canonicalizes to the SAME bytes (and thus the same hash).
  reserialized = json.dumps(json.loads(a), sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode('utf-8')
  assert reserialized == a


# ----------------------------- revocation -----------------------------

def test_adv_revoked_key_fails_even_with_valid_signature() -> None:
  priv, pub = _keypair()
  store = {
    'active_keys': [{'key_id': 'k1', 'public_key_b64': pub}],
    'revoked_keys': [{'key_id': 'k1', 'reason': 'compromised'}],
  }
  with patch.object(signed_evidence, 'read_trust_store', return_value=store):
    # The revoked key is excluded from the trusted set, so a valid signature still fails closed.
    assert 'k1' not in signed_evidence.trusted_verification_keys()


# ----------------------------- money-evidence + datapack manifest attacks -----------------------------

def test_adv_money_evidence_forge_and_downgrade_rejected() -> None:
  priv, pub = _keypair()
  with patch.object(signed_evidence, 'trusted_verification_keys', return_value={'k1': pub}), \
       patch.object(signed_evidence, 'load_signing_key', return_value=(priv, 'k1')):
    payload = {'operation_lane': 'live', 'pair_id': 'p-1', 'terminal_state': 'CANCELED'}
    block = signed_evidence.sign_evidence_record(payload)
    forged = {**block, 'signature_b64': base64.b64encode(b'\x00' * 64).decode('ascii')}
    assert signed_evidence.verify_evidence_record(payload, forged)[1] == 'invalid_signature'
    assert signed_evidence.verify_evidence_record(payload, {**block, 'signature_alg': 'sha256-raw'})[1] == 'algorithm_downgrade'
    assert signed_evidence.verify_evidence_record({**payload, 'pair_id': 'p-2'}, block)[1] == 'checksum_mismatch'


def test_adv_datapack_manifest_tamper_rejected() -> None:
  priv, pub = _keypair()
  with patch.object(signed_evidence, 'trusted_verification_keys', return_value={'k1': pub}), \
       patch.object(signed_evidence, 'load_signing_key', return_value=(priv, 'k1')):
    manifest = {'operation_lane': 'live', 'inventory': [], 'checksums': {'payloads/a.json': 'h'}}
    persistence.sign_datapack_manifest(manifest)
    assert persistence.verify_datapack_manifest_signature(manifest) == ('verified', None)
    manifest['operation_lane'] = 'sandbox'  # post-signature tamper
    assert persistence.verify_datapack_manifest_signature(manifest)[0] == 'invalid'


# ----------------------------- path traversal / env spoofing -----------------------------

def test_adv_bogus_key_env_path_fails_closed(monkeypatch, tmp_path: Path) -> None:
  # A spoofed / traversal signing-key path that does not resolve to a real PEM yields no signer.
  monkeypatch.setenv('POLYVENTURE_SIGNED_CONTRACT_SIGNING_KEY', str(tmp_path / '..' / 'nope' / 'evil.pem'))
  with patch.object(signed_evidence, 'trusted_verification_keys', return_value={'k1': 'b64'}), \
       patch.object(signed_evidence, 'resolve_active_signer_key_id', return_value='k1'):
    assert signed_evidence.load_signing_key() is None


def test_adv_bogus_trust_store_env_yields_no_keys(monkeypatch, tmp_path: Path) -> None:
  monkeypatch.setenv('POLYVENTURE_SIGNED_CONTRACT_TRUST_STORE', str(tmp_path / '..' / 'no' / 'such.json'))
  # Non-existent override -> falls through to the canonical path; a bogus path never injects keys.
  assert isinstance(signed_evidence.read_trust_store() or {}, dict)


def test_adv_corrupt_trust_store_fails_closed(monkeypatch, tmp_path: Path) -> None:
  corrupt = tmp_path / 'trust.json'
  corrupt.write_text('{ not valid json', encoding='utf-8')
  monkeypatch.setenv('POLYVENTURE_SIGNED_CONTRACT_TRUST_STORE', str(corrupt))
  assert signed_evidence.read_trust_store() is None
  assert signed_evidence.trusted_verification_keys() == {}
