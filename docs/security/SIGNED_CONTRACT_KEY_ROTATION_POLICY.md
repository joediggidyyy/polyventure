# Signed Contract Key Rotation Policy (Stub)

**Status**: Minimal precondition stub (Phase 1.3 gate support)
**Scope**: `polyventure` signed contract verification key lifecycle governance
**Last Updated**: 2026-05-08

---

## 1) Purpose

This stub establishes the minimum governance structure required before Phase 1.3 signed-contract enforcement begins.

---

## 2) Key Lifecycle

- **Generate**: Create signing/verification keypairs using approved cryptographic standards.
- **Activate**: Introduce new trusted verification key(s) with explicit `key_id` and activation timestamp.
- **Deprecate**: Mark previous key(s) as deprecated while overlap remains active.
- **Retire**: Remove deprecated key(s) from active trust set once overlap window closes.

---

## 3) Overlap Window

- Rotation requires a defined overlap window where both old and new key IDs are accepted.
- Default overlap window (stub default): **14 days**.
- During overlap, all produced envelopes must include a valid `signer_key_id` matching active trust-store entries.

---

## 4) Revocation Handling

- Any compromised or invalid key must be marked revoked immediately.
- Revoked key IDs must fail verification regardless of signature validity.
- Revocation events must be logged with timestamp, key ID, reason, and operator reference.

---

## 5) Rollback Behavior

- If rotation causes verification instability, rollback is permitted to the previously active key set.
- Rollback must restore prior trust-store snapshot and log event metadata.
- Rollback must not disable signature verification requirements.

---

## 6) Trust Store Binding

- Default local trust store path: `.secrets/signed_contract_trust_store.json`
- Optional override: `POLYVENTURE_SIGNED_CONTRACT_TRUST_STORE`
- Trust-store updates must be auditable and reviewed.

---

## 7) Enforcement Notes (Stub)

This is a precondition artifact for planning-gate closure. Replace stub defaults with production-approved values before Phase 1.3 implementation starts.
