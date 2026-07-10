from __future__ import annotations

from pathlib import Path
import sys

import pytest

TESTS_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = TESTS_ROOT.parent
for candidate in (PROJECT_ROOT, TESTS_ROOT):
  candidate_text = str(candidate)
  if candidate_text not in sys.path:
    sys.path.insert(0, candidate_text)


@pytest.fixture(autouse=True)
def fast_scan_await(monkeypatch: pytest.MonkeyPatch) -> None:
  # The scan endpoint's §23 blocking wait runs 120 s in production. Tests that
  # hold a scan in 'processing' exercise the timeout-fallback path; the real
  # cadence would add ~2 minutes per such test (the original FD-3 conflict).
  # Tuning the module constants preserves identical code paths at test speed.
  from polyventure import web_app

  monkeypatch.setattr(web_app, '_SCAN_AWAIT_POLL_SEC', 0.05)
  monkeypatch.setattr(web_app, '_SCAN_AWAIT_TIMEOUT_SEC', 0.4)


@pytest.fixture(autouse=True)
def isolate_operator_persistence(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
  state_root = tmp_path / 'isolated-operator-state'
  state_root.mkdir(parents=True, exist_ok=True)
  home_root = tmp_path / 'isolated-home'
  home_root.mkdir(parents=True, exist_ok=True)

  monkeypatch.setenv('KALSHI_STATE_DB_PATH', str(state_root / 'runtime.sqlite3'))
  monkeypatch.setenv('KALSHI_POST_SUBMIT_PROCESSING_BUFFER_SEC', '180')
  monkeypatch.setenv('HOME', str(home_root))
  monkeypatch.setenv('USERPROFILE', str(home_root))


@pytest.fixture(autouse=True)
def ephemeral_signing_key(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
  # The real operator signing key lives in the gitignored .secrets/signing/ and is deliberately not
  # shipped in this public package. Without a provisioned key the signed-evidence surface fails closed
  # (decision 'no-go'), which would fail signing-dependent tests on a fresh clone. This mints a throwaway
  # Ed25519 keypair + matching trust store and points the signer and verifier at them via the documented
  # env overrides, so the signed-evidence surface is reproducible anywhere without shipping a secret. A
  # test that needs the no-key fail-closed path can delete these env vars in its own body.
  import base64
  import json

  from cryptography.hazmat.primitives import serialization
  from cryptography.hazmat.primitives.asymmetric import ed25519

  key_id = 'pv-sc-20260621T043303Z'
  signing_root = tmp_path / 'ephemeral-signing'
  signing_root.mkdir(parents=True, exist_ok=True)
  private_key = ed25519.Ed25519PrivateKey.generate()
  key_path = signing_root / f'{key_id}.pem'
  key_path.write_bytes(
    private_key.private_bytes(
      encoding=serialization.Encoding.PEM,
      format=serialization.PrivateFormat.PKCS8,
      encryption_algorithm=serialization.NoEncryption(),
    )
  )
  public_key_b64 = base64.b64encode(
    private_key.public_key().public_bytes(
      encoding=serialization.Encoding.DER,
      format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )
  ).decode('ascii')
  trust_store_path = signing_root / 'trust_store.json'
  trust_store_path.write_text(
    json.dumps(
      {
        'schema_version': 'signed-contract-trust-store.v1',
        'status': 'active',
        'default_algorithm': 'ed25519',
        'active_keys': [
          {
            'key_id': key_id,
            'public_key_b64': public_key_b64,
            'activated_at_utc': '2026-06-21T04:33:03Z',
          }
        ],
        'revoked_keys': [],
      }
    ),
    encoding='utf-8',
  )
  monkeypatch.setenv('POLYVENTURE_SIGNED_CONTRACT_SIGNING_KEY', str(key_path))
  monkeypatch.setenv('POLYVENTURE_SIGNED_CONTRACT_TRUST_STORE', str(trust_store_path))
